//! Port-level cases: the sequencing contract, reads, leases, cursor locks,
//! parking visibility, retention and notifications, called directly on the
//! store.

use std::collections::BTreeSet;
use std::time::Duration;

use futures::StreamExt;
use time::OffsetDateTime;
use z_event::{
    Cursor, CursorKey, CursorStore, DeadLetter, NewEvent, Notification, OutboxStore, ParkedFilter,
    Seq, Store, TypeFilter,
};

use crate::factory::StoreFactory;
use crate::util::{drain_sequencer, read_all};

fn ev(event_type: &str, aggregate: &str) -> NewEvent {
    NewEvent::new(event_type, aggregate, 1, Vec::new())
}

async fn commit_one<S: OutboxStore>(store: &S, event: NewEvent) {
    let mut tx = store.begin().await.unwrap();
    store.append(&mut tx, &[event]).await.unwrap();
    store.commit(tx).await.unwrap();
}

/// Only committed events get a `seq`; rolled-back ones never do; the
/// sequencer stamps in commit order, not insert order.
pub async fn only_committed_sequenced<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    let lease = store.try_lead().await.unwrap().expect("no other leader");

    let mut first = store.begin().await.unwrap();
    store.append(&mut first, &[ev("T", "a")]).await.unwrap();
    let mut second = store.begin().await.unwrap();
    store.append(&mut second, &[ev("T", "b")]).await.unwrap();
    let mut doomed = store.begin().await.unwrap();
    store.append(&mut doomed, &[ev("T", "c")]).await.unwrap();

    assert_eq!(store.sequence_batch(&lease, 10).await.unwrap(), None);
    assert_eq!(store.unsequenced().await.unwrap(), 0);
    store.commit(second).await.unwrap();
    assert_eq!(
        store.sequence_batch(&lease, 10).await.unwrap(),
        Some(Seq(1))
    );
    store.commit(first).await.unwrap();
    store.rollback(doomed).await.unwrap();
    assert_eq!(store.unsequenced().await.unwrap(), 1);
    assert_eq!(
        store.sequence_batch(&lease, 10).await.unwrap(),
        Some(Seq(2))
    );
    assert_eq!(store.sequence_batch(&lease, 10).await.unwrap(), None);

    let feed = read_all(&store, Seq::ZERO, &TypeFilter::All).await;
    let order: Vec<_> = feed
        .iter()
        .map(|e| (e.seq.get(), e.aggregate.as_str()))
        .collect();
    assert_eq!(order, [(1, "b"), (2, "a")]);
    store.release(lease).await.unwrap();
}

/// `read` honours `after`, the type filter and `limit`, ascending.
pub async fn read_contract<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    for i in 0..10 {
        let t = if i % 2 == 0 { "A" } else { "B" };
        commit_one(&store, ev(t, &format!("x/{i}"))).await;
    }
    drain_sequencer(&store).await;

    let only_a = TypeFilter::only(["A"]);
    let batch = store.read(Seq(3), &only_a, 2).await.unwrap();
    let seqs: Vec<u64> = batch.iter().map(|e| e.seq.get()).collect();
    assert_eq!(seqs, [5, 7]);
    assert!(batch.iter().all(|e| e.event_type == "A"));
    assert_eq!(
        store.read(Seq(10), &TypeFilter::All, 10).await.unwrap(),
        vec![]
    );
    let all = store.read(Seq::ZERO, &TypeFilter::All, 100).await.unwrap();
    assert_eq!(all.len(), 10);
    assert!(all.windows(2).all(|w| w[0].seq < w[1].seq));
    let head = store.head().await.unwrap();
    assert_eq!(head.seq, Seq(10));
    assert_eq!(head.purged_through, Seq::ZERO);
}

/// Envelope fields survive the round trip through the store.
pub async fn envelope_round_trip<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    let at = OffsetDateTime::now_utc()
        .replace_nanosecond(123_000)
        .unwrap();
    let original = NewEvent::new("OrderConfirmed", "orders/o17", 4, b"{\"k\":1}".to_vec())
        .version(2)
        .correlation("req-1")
        .header("tenant", "acme")
        .header("actor", "u1")
        .occurred_at(at);
    commit_one(&store, original.clone()).await;
    drain_sequencer(&store).await;
    let got = store.read(Seq::ZERO, &TypeFilter::All, 1).await.unwrap();
    assert_eq!(got, vec![original.into_event(Seq(1))]);
}

/// Only one lease holder at a time; release frees it.
pub async fn lease_exclusive<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    let lease = store.try_lead().await.unwrap().expect("first lease");
    assert!(store.try_lead().await.unwrap().is_none(), "second lease");
    store.release(lease).await.unwrap();
    let again = store
        .try_lead()
        .await
        .unwrap()
        .expect("lease after release");
    store.release(again).await.unwrap();
}

/// `lock_cursor` is exclusive until the holder's transaction ends, and the
/// waiter sees the holder's committed advance.
pub async fn cursor_lock_exclusive<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    let key = CursorKey::new("billing", "orders");
    let mut t1 = store.begin().await.unwrap();
    let c = store.lock_cursor(&mut t1, &key).await.unwrap();
    assert_eq!(c, Cursor::start());

    let other = store.clone();
    let k = key.clone();
    let waiter = tokio::spawn(async move {
        let mut t2 = other.begin().await.unwrap();
        other.lock_cursor(&mut t2, &k).await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiter.is_finished(), "a second lock_cursor must wait");
    store.advance(&mut t1, &key, c.next(Seq(9))).await.unwrap();
    store.commit(t1).await.unwrap();
    let seen = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("waiter proceeds after commit")
        .unwrap();
    assert_eq!(seen.seq, Seq(9));

    // Different keys do not block each other.
    let mut a = store.begin().await.unwrap();
    store.lock_cursor(&mut a, &key).await.unwrap();
    let mut b = store.begin().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        store.lock_cursor(&mut b, &CursorKey::new("search", "orders")),
    )
    .await
    .expect("independent keys")
    .unwrap();
}

/// Cursors round-trip; `reset_cursor` overwrites; keys are independent.
pub async fn cursor_round_trip<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    let key = CursorKey::new("billing", "orders");
    assert_eq!(store.cursor(&key).await.unwrap(), Cursor::start());
    let epoch = store.head().await.unwrap().epoch;
    let mut tx = store.begin().await.unwrap();
    store.lock_cursor(&mut tx, &key).await.unwrap();
    let to = Cursor::start().with_epoch(epoch.clone()).next(Seq(5));
    store.advance(&mut tx, &key, to.clone()).await.unwrap();
    assert_eq!(
        store.cursor(&key).await.unwrap(),
        Cursor::start(),
        "not yet committed"
    );
    store.commit(tx).await.unwrap();
    assert_eq!(store.cursor(&key).await.unwrap(), to);

    store
        .reset_cursor(&key, Cursor::start().next(Seq(2)))
        .await
        .unwrap();
    assert_eq!(store.cursor(&key).await.unwrap().seq, Seq(2));
    assert_eq!(
        store
            .cursor(&CursorKey::new("search", "orders"))
            .await
            .unwrap(),
        Cursor::start()
    );
}

/// `is_parked` sees the transaction's own writes; `parked` lists in `seq`
/// order and honours the filter; `resolve` closes a letter.
pub async fn parking_visibility<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    let key = CursorKey::new("billing", "orders");
    let e3 = ev("T", "orders/a").into_event(Seq(3));
    let e5 = ev("T", "orders/a").into_event(Seq(5));
    let e4 = ev("T", "orders/b").into_event(Seq(4));

    let mut tx = store.begin().await.unwrap();
    assert!(!store.is_parked(&mut tx, &key, "orders/a").await.unwrap());
    let failed = DeadLetter::failed(&key, &e3, &z_event::HandlerError::permanent("boom"), 2);
    store.park(&mut tx, &failed).await.unwrap();
    assert!(store.is_parked(&mut tx, &key, "orders/a").await.unwrap());
    assert!(!store.is_parked(&mut tx, &key, "orders/b").await.unwrap());
    store
        .park(&mut tx, &DeadLetter::behind(&key, &e5))
        .await
        .unwrap();
    store
        .park(&mut tx, &DeadLetter::behind(&key, &e4))
        .await
        .unwrap();
    store.commit(tx).await.unwrap();

    let open = store
        .parked(&key, &ParkedFilter::unresolved())
        .await
        .unwrap();
    let seqs: Vec<u64> = open.iter().map(|l| l.seq().get()).collect();
    assert_eq!(seqs, [3, 4, 5]);
    assert_eq!(open[0].attempts, 2);
    assert_eq!(open[0].event, e3);
    let only_a = store
        .parked(&key, &ParkedFilter::unresolved().aggregate("orders/a"))
        .await
        .unwrap();
    assert_eq!(only_a.len(), 2);
    let other_key = CursorKey::new("search", "orders");
    assert!(
        store
            .parked(&other_key, &ParkedFilter::unresolved())
            .await
            .unwrap()
            .is_empty()
    );

    let mut tx = store.begin().await.unwrap();
    store.resolve(&mut tx, &key, Seq(3)).await.unwrap();
    store.resolve(&mut tx, &key, Seq(5)).await.unwrap();
    assert!(!store.is_parked(&mut tx, &key, "orders/a").await.unwrap());
    store.commit(tx).await.unwrap();
    let open = store
        .parked(&key, &ParkedFilter::unresolved())
        .await
        .unwrap();
    assert_eq!(open.iter().map(|l| l.seq().get()).collect::<Vec<_>>(), [4]);
    let all = ParkedFilter {
        include_resolved: true,
        ..ParkedFilter::default()
    };
    let every = store.parked(&key, &all).await.unwrap();
    assert_eq!(every.len(), 3);
    assert!(every[0].is_resolved());

    // Re-parking a resolved letter reopens it.
    let mut tx = store.begin().await.unwrap();
    store.park(&mut tx, &failed).await.unwrap();
    store.commit(tx).await.unwrap();
    assert_eq!(
        store
            .parked(&key, &ParkedFilter::unresolved())
            .await
            .unwrap()
            .len(),
        2
    );
}

/// Purge deletes only acked, old events, never the head; acks are
/// monotonic; removing a consumer releases retention.
pub async fn purge_respects_acks<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    for i in 0..10 {
        commit_one(&store, ev("T", &format!("x/{i}"))).await;
    }
    drain_sequencer(&store).await;
    store.register_consumer("c1", Seq(8)).await.unwrap();
    store.register_consumer("c2", Seq(5)).await.unwrap();
    let later = OffsetDateTime::now_utc() + time::Duration::minutes(5);
    let earlier = OffsetDateTime::now_utc() - time::Duration::hours(1);

    assert_eq!(
        store.purge(earlier).await.unwrap(),
        0,
        "events are too young"
    );
    assert_eq!(store.purge(later).await.unwrap(), 5);
    assert_eq!(store.head().await.unwrap().purged_through, Seq(5));
    store.ack("c2", Seq(3)).await.unwrap();
    let c2 = store.consumers().await.unwrap();
    assert_eq!(
        c2.iter().find(|c| c.consumer == "c2").map(|c| c.acked_seq),
        Some(Seq(5)),
        "acks never go back"
    );
    assert_eq!(store.purge(later).await.unwrap(), 0);

    store.remove_consumer("c2").await.unwrap();
    assert_eq!(store.purge(later).await.unwrap(), 3);
    store.ack("c1", Seq(10)).await.unwrap();
    assert_eq!(
        store.purge(later).await.unwrap(),
        1,
        "the head event is kept"
    );
    let head = store.head().await.unwrap();
    assert_eq!((head.seq, head.purged_through), (Seq(10), Seq(9)));
    let left = read_all(&store, Seq::ZERO, &TypeFilter::All).await;
    assert_eq!(left.iter().map(|e| e.seq.get()).collect::<Vec<_>>(), [10]);

    // Re-registering sets the position (a consumer that reset its cursor).
    store.register_consumer("c1", Seq(2)).await.unwrap();
    let c1 = store.consumers().await.unwrap();
    assert_eq!(c1[0].acked_seq, Seq(2));
    store.ack("nobody", Seq(4)).await.unwrap();
    assert_eq!(
        store.consumers().await.unwrap().len(),
        1,
        "ack does not register"
    );
}

/// `rotate_epoch` changes the epoch reported by `head`.
pub async fn epoch_rotation<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    let before = store.head().await.unwrap().epoch;
    assert_eq!(store.head().await.unwrap().epoch, before, "epoch is stable");
    let rotated = store.rotate_epoch().await.unwrap();
    assert_ne!(rotated, before);
    assert_eq!(store.head().await.unwrap().epoch, rotated);
}

async fn next_hint(
    hints: &mut futures::stream::BoxStream<'static, Notification>,
    want: fn(&Notification) -> bool,
) -> Notification {
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(n) = hints.next().await {
            if want(&n) || n == Notification::Lagged {
                return n;
            }
        }
        panic!("notification stream ended");
    })
    .await
    .expect("notification within 2 s")
}

/// `Appended` follows a commit with events and `Sequenced(head)` follows a
/// sequencing batch, both promptly.
pub async fn notifications_wake<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    let mut hints = store.notifications();
    // Give listener-based adapters a moment to start listening.
    tokio::time::sleep(Duration::from_millis(100)).await;
    commit_one(&store, ev("T", "x/1")).await;
    let n = next_hint(&mut hints, |n| *n == Notification::Appended).await;
    assert!(matches!(n, Notification::Appended | Notification::Lagged));
    let lease = store.try_lead().await.unwrap().unwrap();
    store.sequence_batch(&lease, 10).await.unwrap();
    let n = next_hint(&mut hints, |n| matches!(n, Notification::Sequenced(_))).await;
    assert!(matches!(
        n,
        Notification::Sequenced(Seq(1)) | Notification::Lagged
    ));
    store.release(lease).await.unwrap();
}

/// 50 concurrent transactions with random commit delays, some held open
/// and some rolled back, while a sequencer and a reader run: the reader
/// receives exactly the committed events, gapless, nothing skipped.
pub async fn commit_order_gap<F: StoreFactory>(f: &F) {
    use z_event::{AllowAll, InProcess, Producer, Start, Transport};

    let store = f.fresh().await;
    let producer = Producer::new(store.clone(), crate::util::producer_config("orders"));
    let running = producer.start();
    let transport = InProcess::new();
    transport.register("orders", producer.feed_source(AllowAll));
    let mut sub = transport
        .subscribe(
            "orders",
            Start {
                consumer: "reader".into(),
                from_seq: Seq::ZERO,
                epoch: None,
                types: vec![],
            },
        )
        .await
        .unwrap();

    let mut rng = crate::util::rng(1);
    let mut writers = Vec::new();
    let mut committed = BTreeSet::new();
    for i in 0..50u32 {
        let event = ev("T", &format!("gap/{i}"));
        let (delay_ms, commit) = match i % 10 {
            0 => (rng.u64(300..600), true), // held open
            1 => (rng.u64(0..50), false),   // rolled back
            _ => (rng.u64(0..50), true),
        };
        if commit {
            committed.insert(event.event_id());
        }
        let store = store.clone();
        let producer = producer.clone();
        writers.push(tokio::spawn(async move {
            let mut tx = store.begin().await.unwrap();
            producer.append(&mut tx, event).await.unwrap();
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            if commit {
                store.commit(tx).await.unwrap();
            } else {
                store.rollback(tx).await.unwrap();
            }
        }));
    }
    for w in writers {
        w.await.unwrap();
    }

    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while received.len() < committed.len() {
            match sub.messages.next().await {
                Some(Ok(z_event::FeedMessage::Event(e))) => received.push(e),
                Some(Ok(_)) => {}
                other => panic!("reader stream ended: {other:?}"),
            }
        }
    })
    .await
    .expect("reader received every committed event");

    let seqs: Vec<u64> = received.iter().map(|e| e.seq.get()).collect();
    assert_eq!(
        seqs,
        (1..=committed.len() as u64).collect::<Vec<_>>(),
        "gapless, in order"
    );
    let ids: BTreeSet<_> = received.iter().map(|e| e.id).collect();
    assert_eq!(ids, committed, "exactly the committed events");
    running.shutdown().await;
}

/// Hundreds of events from concurrent writers in random-size transactions:
/// `seq` is gapless and immutable, ids are unique, and events of one
/// aggregate (written in sequence) are in version order.
pub async fn gapless_and_immutable<F: StoreFactory>(f: &F) {
    use z_event::{Producer, Store as _};

    let store = f.fresh().await;
    let producer = Producer::new(store.clone(), crate::util::producer_config("orders"));
    let running = producer.start();
    let writers = 8u64;
    let per_writer = 40u64 * u64::from(crate::util::scale());
    let mut tasks = Vec::new();
    for w in 0..writers {
        let store = store.clone();
        let producer = producer.clone();
        let mut rng = crate::util::rng(100 + w);
        tasks.push(tokio::spawn(async move {
            let mut version = 0;
            while version < per_writer {
                let n = rng.u64(1..=4).min(per_writer - version);
                let mut tx = store.begin().await.unwrap();
                for _ in 0..n {
                    version += 1;
                    let e = NewEvent::new("T", format!("agg/{w}"), version, Vec::new());
                    producer.append(&mut tx, e).await.unwrap();
                }
                if rng.u8(0..4) == 0 {
                    tokio::time::sleep(Duration::from_millis(rng.u64(0..5))).await;
                }
                store.commit(tx).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let total = writers * per_writer;
    let orders = store.clone();
    crate::util::wait_until("everything sequenced", Duration::from_secs(20), || {
        let orders = orders.clone();
        async move { orders.head().await.unwrap().seq == Seq(total) }
    })
    .await;

    let feed = read_all(&store, Seq::ZERO, &TypeFilter::All).await;
    let seqs: Vec<u64> = feed.iter().map(|e| e.seq.get()).collect();
    assert_eq!(seqs, (1..=total).collect::<Vec<_>>());
    let ids: BTreeSet<_> = feed.iter().map(|e| e.id).collect();
    assert_eq!(ids.len() as u64, total, "unique ids");
    for w in 0..writers {
        let versions: Vec<u64> = feed
            .iter()
            .filter(|e| e.aggregate == format!("agg/{w}"))
            .map(|e| e.aggregate_version)
            .collect();
        assert_eq!(
            versions,
            (1..=per_writer).collect::<Vec<_>>(),
            "aggregate order"
        );
    }
    assert_eq!(
        read_all(&store, Seq::ZERO, &TypeFilter::All).await,
        feed,
        "immutable"
    );
    assert_eq!(store.unsequenced().await.unwrap(), 0);
    running.shutdown().await;
}
