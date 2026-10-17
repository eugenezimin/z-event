//! The runtime emits the documented metrics with the documented labels.
//!
//! This binary holds a single test, because it installs a process-global
//! recorder.

#![allow(clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::registry::{AtomicStorage, Registry};
use z_event::telemetry::*;
use z_event::{
    AllowAll, Backoff, Cursor, CursorKey, CursorStore, Event, FeedConfig, Handler, HandlerError,
    InProcess, NewEvent, OutboxStore, Producer, ProducerConfig, RetryPolicy, Seq, Store,
    Subscriber,
};
use z_event_memory::{MemoryStore, MemoryTx};

#[derive(Clone, Default)]
struct Apply {
    poisoned: Arc<Mutex<HashSet<String>>>,
}

impl Handler<MemoryStore> for Apply {
    async fn handle(&self, tx: &mut MemoryTx, ev: &Event) -> Result<(), HandlerError> {
        if self.poisoned.lock().unwrap().contains(&ev.aggregate) {
            return Err(HandlerError::permanent("poisoned"));
        }
        tx.put(format!("effect/{}", ev.seq), "");
        Ok(())
    }
}

/// A recorder whose values can be read without resetting them.
#[derive(Clone)]
struct Metrics(Arc<Registry<Key, AtomicStorage>>);

impl Recorder for Metrics {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        self.0
            .get_or_create_counter(key, |c| Counter::from_arc(c.clone()))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        self.0
            .get_or_create_gauge(key, |g| Gauge::from_arc(g.clone()))
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.0
            .get_or_create_histogram(key, |h| Histogram::from_arc(h.clone()))
    }
}

fn matches_key(key: &Key, name: &str, labels: &[(&str, &str)]) -> bool {
    let mut got: Vec<(&str, &str)> = key.labels().map(|l| (l.key(), l.value())).collect();
    got.sort_unstable();
    let mut want = labels.to_vec();
    want.sort_unstable();
    key.name() == name && got == want
}

impl Metrics {
    fn counter(&self, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
        self.0
            .get_counter_handles()
            .into_iter()
            .find(|(k, _)| matches_key(k, name, labels))
            .map(|(_, c)| c.load(std::sync::atomic::Ordering::SeqCst))
    }

    fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        self.0
            .get_gauge_handles()
            .into_iter()
            .find(|(k, _)| matches_key(k, name, labels))
            .map(|(_, g)| f64::from_bits(g.load(std::sync::atomic::Ordering::SeqCst)))
    }

    fn histogram_exists(&self, name: &str, labels: &[(&str, &str)]) -> bool {
        self.0
            .get_histogram_handles()
            .keys()
            .any(|k| matches_key(k, name, labels))
    }
}

async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

async fn append(producer: &Producer<MemoryStore>, aggregate: &str) {
    let store = producer.store();
    let mut tx = store.begin().await.unwrap();
    producer
        .append(&mut tx, NewEvent::new("T", aggregate, 1, "{}"))
        .await
        .unwrap();
    store.commit(tx).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_emits_documented_metrics() {
    let m = Metrics(Arc::new(Registry::atomic()));
    metrics::set_global_recorder(m.clone()).unwrap();

    const P: &[(&str, &str)] = &[("producer", "orders")];
    const PC: &[(&str, &str)] = &[("producer", "orders"), ("consumer", "billing")];

    let orders = MemoryStore::new();
    let billing = MemoryStore::new();
    let producer = Producer::new(
        orders.clone(),
        ProducerConfig::new("orders")
            .safety_interval(Duration::from_millis(50))
            .lead_retry_interval(Duration::from_millis(20))
            .retention(Duration::ZERO)
            .retention_interval(Duration::from_millis(30))
            .feed(FeedConfig {
                backlog_batch: 50,
                heartbeat: Duration::from_millis(100),
            }),
    );
    let running = producer.start();
    let transport = InProcess::new();
    transport.register("orders", producer.feed_source(AllowAll));

    let handler = Apply::default();
    handler.poisoned.lock().unwrap().insert("bad/1".into());
    let subscriber = |handler: Apply| {
        Subscriber::builder(billing.clone(), transport.clone(), handler)
            .consumer("billing")
            .producer("orders")
            .ack_interval(Duration::from_millis(20))
            .retry(RetryPolicy {
                max_attempts: 2,
                backoff: Backoff::new(Duration::from_millis(1), Duration::from_millis(2)),
            })
            .spawn()
    };
    let sub = subscriber(handler.clone());

    for i in 0..5 {
        append(&producer, &format!("ok/{i}")).await;
    }
    append(&producer, "bad/1").await;
    let key = CursorKey::new("billing", "orders");
    tokio::time::timeout(Duration::from_secs(10), async {
        while billing.cursor(&key).await.unwrap().seq < Seq(6) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // Producer side.
    assert_eq!(m.counter(APPENDED_TOTAL, P), Some(6));
    assert_eq!(m.gauge(HEAD_SEQ, P), Some(6.0));
    assert_eq!(m.gauge(SEQUENCER_LEADER, P), Some(1.0));
    assert_eq!(m.gauge(STREAMS_OPEN, P), Some(1.0));
    eventually("acked gauge and purge", || {
        m.gauge(CONSUMER_ACKED_SEQ, PC) == Some(6.0) && m.counter(PURGED_TOTAL, P) >= Some(5)
    })
    .await;
    assert_eq!(m.gauge(UNSEQUENCED_ROWS, P), Some(0.0));

    // Consumer side.
    assert_eq!(m.counter(EVENTS_APPLIED_TOTAL, PC), Some(5));
    assert_eq!(m.gauge(DEAD_LETTERS, PC), Some(1.0));
    let permanent = [PC[0], PC[1], ("kind", "permanent")];
    assert!(m.counter(HANDLER_ERRORS_TOTAL, &permanent) >= Some(1));
    assert!(m.histogram_exists(APPLY_SECONDS, PC));
    assert!(m.gauge(CONSUMER_LAG_SECONDS, PC) >= Some(0.0));
    assert!(m.gauge(CONSUMER_LAG_EVENTS, PC).is_some());

    // A rotated epoch stops the subscriber.
    orders.rotate_epoch().await.unwrap();
    assert!(sub.join().await.is_err());
    assert_eq!(m.counter(EPOCH_MISMATCH_TOTAL, PC), Some(1));

    // A cursor ahead of the head in the current epoch: the producer rotates.
    let head = orders.head().await.unwrap();
    billing
        .reset_cursor(
            &key,
            Cursor::start()
                .with_epoch(head.epoch)
                .next(Seq(head.seq.get() + 10)),
        )
        .await
        .unwrap();
    assert!(subscriber(handler).join().await.is_err());
    assert_eq!(m.counter(EPOCH_ROTATIONS_TOTAL, P), Some(1));
    assert_eq!(m.counter(EPOCH_MISMATCH_TOTAL, PC), Some(2));

    // Streams close and leadership ends on shutdown.
    eventually("streams closed", || m.gauge(STREAMS_OPEN, P) == Some(0.0)).await;
    running.shutdown().await;
    assert_eq!(m.gauge(SEQUENCER_LEADER, P), Some(0.0));
}
