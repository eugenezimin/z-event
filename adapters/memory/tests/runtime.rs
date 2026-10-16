//! Core runtime behaviour on the memory adapter and the in-process
//! transport. The exhaustive contract lives in the conformance suites.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use z_event::{
    AllowAll, Backoff, CursorStore, Event, FeedConfig, Handler, HandlerError, InProcess, NewEvent,
    ParkedFilter, Producer, ProducerConfig, RetryPolicy, Seq, Store, Subscriber, SubscriberStatus,
};
use z_event_memory::{MemoryStore, MemoryTx};

fn fast_producer(store: MemoryStore, name: &str) -> Producer<MemoryStore> {
    Producer::new(
        store,
        ProducerConfig::new(name)
            .safety_interval(Duration::from_millis(50))
            .lead_retry_interval(Duration::from_millis(50))
            .feed(FeedConfig {
                backlog_batch: 7,
                heartbeat: Duration::from_millis(200),
            }),
    )
}

fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        backoff: Backoff::new(Duration::from_millis(1), Duration::from_millis(5)),
    }
}

/// Writes `effect/<seq>` in the consumer's transaction; fails for poisoned
/// aggregates.
#[derive(Clone, Default)]
struct Record {
    poisoned: Arc<Mutex<HashSet<String>>>,
}

impl Handler<MemoryStore> for Record {
    async fn handle(&self, tx: &mut MemoryTx, ev: &Event) -> Result<(), HandlerError> {
        if self.poisoned.lock().unwrap().contains(&ev.aggregate) {
            return Err(HandlerError::permanent(format!(
                "poisoned {}",
                ev.aggregate
            )));
        }
        let key = format!("effect/{:06}", ev.seq.get());
        assert!(tx.get(&key).is_none(), "effect {key} applied twice");
        tx.put(key, ev.aggregate.clone());
        Ok(())
    }
}

async fn append(producer: &Producer<MemoryStore>, event_type: &str, aggregate: &str) {
    let store = producer.store();
    let mut tx = store.begin().await.unwrap();
    producer
        .append(&mut tx, NewEvent::new(event_type, aggregate, 1, "{}"))
        .await
        .unwrap();
    store.commit(tx).await.unwrap();
}

async fn wait_cursor(store: &MemoryStore, consumer: &str, producer: &str, seq: u64) {
    let key = z_event::CursorKey::new(consumer, producer);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if store.cursor(&key).await.unwrap().seq >= Seq(seq) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("cursor of {consumer} did not reach {seq}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_backlog_then_live_exactly_once() {
    let orders = MemoryStore::new();
    let billing = MemoryStore::new();
    let producer = fast_producer(orders.clone(), "orders");
    let running = producer.start();
    let transport = InProcess::new();
    transport.register("orders", producer.feed_source(AllowAll));

    for i in 0..20 {
        append(&producer, "OrderConfirmed", &format!("orders/o{i}")).await;
    }
    let sub = Subscriber::builder(billing.clone(), transport.clone(), Record::default())
        .consumer("billing")
        .producer("orders")
        .ack_interval(Duration::from_millis(20))
        .spawn();
    wait_cursor(&billing, "billing", "orders", 20).await;
    for i in 20..30 {
        append(&producer, "OrderConfirmed", &format!("orders/o{i}")).await;
    }
    wait_cursor(&billing, "billing", "orders", 30).await;
    assert_eq!(billing.scan_prefix("effect/").len(), 30);
    assert!(matches!(sub.status(), SubscriberStatus::Streaming { .. }));

    // Acks reach the producer.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let consumers = z_event::OutboxStore::consumers(&orders).await.unwrap();
            if consumers.iter().any(|c| c.acked_seq == Seq(30)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("ack recorded");

    sub.shutdown().await.unwrap();
    running.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_consumer_reaches_head_through_checkpoints() {
    let orders = MemoryStore::new();
    let search = MemoryStore::new();
    let producer = fast_producer(orders.clone(), "orders");
    let running = producer.start();
    let transport = InProcess::new();
    transport.register("orders", producer.feed_source(AllowAll));

    append(&producer, "OrderConfirmed", "orders/o1").await;
    for i in 0..15 {
        append(&producer, "OrderNoted", &format!("orders/n{i}")).await;
    }
    let sub = Subscriber::builder(search.clone(), transport, Record::default())
        .consumer("search")
        .producer("orders")
        .types(["OrderConfirmed"])
        .spawn();
    wait_cursor(&search, "search", "orders", 16).await;
    assert_eq!(search.scan_prefix("effect/").len(), 1);
    sub.shutdown().await.unwrap();
    running.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poison_parks_the_aggregate_and_replay_applies_in_order() {
    let orders = MemoryStore::new();
    let billing = MemoryStore::new();
    let producer = fast_producer(orders, "orders");
    let running = producer.start();
    let transport = InProcess::new();
    transport.register("orders", producer.feed_source(AllowAll));

    let handler = Record::default();
    handler.poisoned.lock().unwrap().insert("orders/a".into());
    let sub = Subscriber::builder(billing.clone(), transport, handler.clone())
        .consumer("billing")
        .producer("orders")
        .retry(fast_retry())
        .build();
    let running_sub = sub.spawn();

    for agg in ["orders/a", "orders/b", "orders/a", "orders/b", "orders/a"] {
        append(&producer, "T", agg).await;
    }
    wait_cursor(&billing, "billing", "orders", 5).await;
    let effects: Vec<_> = billing
        .scan_prefix("effect/")
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(effects, ["effect/000002", "effect/000004"]);
    let parked = sub.parked(ParkedFilter::unresolved()).await.unwrap();
    let seqs: Vec<u64> = parked.iter().map(|l| l.seq().get()).collect();
    assert_eq!(seqs, [1, 3, 5]);
    assert_eq!(parked[0].attempts, 1, "permanent errors are not retried");
    assert_eq!(parked[1].attempts, 0, "parked behind");

    // Still failing: nothing overtakes the first event.
    let report = sub.replay_parked(ParkedFilter::unresolved()).await.unwrap();
    assert_eq!(report.replayed, 0);
    assert_eq!(report.blocked_aggregates, ["orders/a"]);

    handler.poisoned.lock().unwrap().clear();
    let report = sub.replay_parked(ParkedFilter::unresolved()).await.unwrap();
    assert_eq!(report.replayed, 3);
    assert!(
        sub.parked(ParkedFilter::unresolved())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(billing.scan_prefix("effect/").len(), 5);

    // The aggregate flows again.
    append(&producer, "T", "orders/a").await;
    wait_cursor(&billing, "billing", "orders", 6).await;
    assert_eq!(billing.scan_prefix("effect/").len(), 6);

    running_sub.shutdown().await.unwrap();
    running.shutdown().await;
}
