//! Behaviour specific to the in-memory adapter (the shared contract is
//! covered by the conformance suites).

use std::time::Duration;

use time::OffsetDateTime;
use z_event::{
    Cursor, CursorKey, CursorStore, DeadLetter, NewEvent, OutboxStore, ParkedFilter, Seq, Store,
    TypeFilter,
};
use z_event_memory::{MemoryError, MemoryStore};

fn ev(agg: &str) -> NewEvent {
    NewEvent::new("T", agg, 1, "{}")
}

#[tokio::test]
async fn sequencer_sees_only_committed_events_in_commit_order() {
    let store = MemoryStore::new();
    let lease = store.try_lead().await.unwrap().unwrap();

    let mut a = store.begin().await.unwrap();
    store.append(&mut a, &[ev("a")]).await.unwrap(); // lower insert id
    let mut b = store.begin().await.unwrap();
    store.append(&mut b, &[ev("b")]).await.unwrap();
    let mut c = store.begin().await.unwrap();
    store.append(&mut c, &[ev("c")]).await.unwrap();

    assert_eq!(store.sequence_batch(&lease, 10).await.unwrap(), None);
    store.commit(b).await.unwrap();
    assert_eq!(
        store.sequence_batch(&lease, 10).await.unwrap(),
        Some(Seq(1))
    );
    store.commit(a).await.unwrap();
    drop(c); // rolled back
    assert_eq!(
        store.sequence_batch(&lease, 10).await.unwrap(),
        Some(Seq(2))
    );
    assert_eq!(store.sequence_batch(&lease, 10).await.unwrap(), None);

    let feed = store.read(Seq::ZERO, &TypeFilter::All, 10).await.unwrap();
    let aggs: Vec<_> = feed.iter().map(|e| e.aggregate.as_str()).collect();
    assert_eq!(aggs, ["b", "a"]);
    assert_eq!(store.unsequenced().await.unwrap(), 0);
}

#[tokio::test]
async fn lease_is_exclusive_and_fenced() {
    let store = MemoryStore::new();
    let first = store.try_lead().await.unwrap().unwrap();
    assert!(store.try_lead().await.unwrap().is_none());

    store.expire_lease();
    let second = store.try_lead().await.unwrap().unwrap();
    assert_eq!(
        store.sequence_batch(&first, 1).await,
        Err(MemoryError::LeaseLost)
    );
    // Releasing a stale lease must not free the new holder's lease.
    store.release(first).await.unwrap();
    assert!(store.try_lead().await.unwrap().is_none());

    store.release(second).await.unwrap();
    assert!(store.try_lead().await.unwrap().is_some());
}

#[tokio::test]
async fn lease_expires_without_renewal() {
    let store = MemoryStore::builder()
        .lease_ttl(Duration::from_millis(50))
        .build();
    let lease = store.try_lead().await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(store.try_lead().await.unwrap().is_some());
    assert_eq!(
        store.sequence_batch(&lease, 1).await,
        Err(MemoryError::LeaseLost)
    );
}

#[tokio::test]
async fn cursor_lock_is_exclusive_until_commit() {
    let store = MemoryStore::new();
    let key = CursorKey::new("billing", "orders");

    let mut t1 = store.begin().await.unwrap();
    let c = store.lock_cursor(&mut t1, &key).await.unwrap();
    assert_eq!(c, Cursor::start());

    let s2 = store.clone();
    let k2 = key.clone();
    let waiter = tokio::spawn(async move {
        let mut t2 = s2.begin().await.unwrap();
        s2.lock_cursor(&mut t2, &k2).await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished(), "second lock must wait");

    store.advance(&mut t1, &key, c.next(Seq(7))).await.unwrap();
    store.commit(t1).await.unwrap();
    let seen = waiter.await.unwrap();
    assert_eq!(seen.seq, Seq(7), "waiter sees the committed value");
}

#[tokio::test]
async fn dropped_transaction_rolls_back_and_releases_locks() {
    let store = MemoryStore::new();
    let key = CursorKey::new("c", "p");
    let mut t1 = store.begin().await.unwrap();
    store.lock_cursor(&mut t1, &key).await.unwrap();
    store
        .advance(&mut t1, &key, Cursor::start().next(Seq(3)))
        .await
        .unwrap();
    t1.put("k", "v");
    drop(t1);

    let mut t2 = store.begin().await.unwrap();
    let c = tokio::time::timeout(Duration::from_secs(1), store.lock_cursor(&mut t2, &key))
        .await
        .expect("lock released on drop")
        .unwrap();
    assert_eq!(c.seq, Seq::ZERO);
    assert_eq!(store.get("k"), None);
}

#[tokio::test]
async fn host_values_read_your_writes() {
    let store = MemoryStore::new();
    let mut tx = store.begin().await.unwrap();
    tx.put("orders/o1", "new");
    assert_eq!(tx.get("orders/o1").as_deref(), Some(&b"new"[..]));
    assert_eq!(store.get("orders/o1"), None);
    store.commit(tx).await.unwrap();
    assert_eq!(store.get("orders/o1").as_deref(), Some(&b"new"[..]));

    let mut tx = store.begin().await.unwrap();
    tx.delete("orders/o1");
    assert_eq!(tx.get("orders/o1"), None);
    store.commit(tx).await.unwrap();
    assert!(store.scan_prefix("orders/").is_empty());
}

#[tokio::test]
async fn is_parked_sees_own_writes() {
    let store = MemoryStore::new();
    let key = CursorKey::new("c", "p");
    let event = ev("orders/o1").into_event(Seq(4));

    let mut tx = store.begin().await.unwrap();
    assert!(!store.is_parked(&mut tx, &key, "orders/o1").await.unwrap());
    store
        .park(&mut tx, &DeadLetter::behind(&key, &event))
        .await
        .unwrap();
    assert!(store.is_parked(&mut tx, &key, "orders/o1").await.unwrap());
    assert!(!store.is_parked(&mut tx, &key, "orders/o2").await.unwrap());
    store.commit(tx).await.unwrap();

    let mut tx = store.begin().await.unwrap();
    store.resolve(&mut tx, &key, Seq(4)).await.unwrap();
    assert!(!store.is_parked(&mut tx, &key, "orders/o1").await.unwrap());
    store.commit(tx).await.unwrap();

    assert!(
        store
            .parked(&key, &ParkedFilter::unresolved())
            .await
            .unwrap()
            .is_empty()
    );
    let all = ParkedFilter {
        include_resolved: true,
        ..ParkedFilter::default()
    };
    let letters = store.parked(&key, &all).await.unwrap();
    assert_eq!(letters.len(), 1);
    assert!(letters[0].is_resolved());
}

#[tokio::test]
async fn purge_keeps_head_and_unacked() {
    let store = MemoryStore::new();
    let lease = store.try_lead().await.unwrap().unwrap();
    for i in 0..10 {
        let mut tx = store.begin().await.unwrap();
        store
            .append(&mut tx, &[ev(&format!("a/{i}"))])
            .await
            .unwrap();
        store.commit(tx).await.unwrap();
    }
    store.sequence_batch(&lease, 100).await.unwrap();
    store.register_consumer("c1", Seq(8)).await.unwrap();
    store.register_consumer("c2", Seq(5)).await.unwrap();
    let later = OffsetDateTime::now_utc() + time::Duration::seconds(1);
    let earlier = OffsetDateTime::now_utc() - time::Duration::hours(1);

    assert_eq!(store.purge(earlier).await.unwrap(), 0, "too young");
    assert_eq!(store.purge(later).await.unwrap(), 5);
    assert_eq!(store.head().await.unwrap().purged_through, Seq(5));

    store.ack("c2", Seq(3)).await.unwrap(); // acks never go back
    store.remove_consumer("c2").await.unwrap();
    assert_eq!(store.purge(later).await.unwrap(), 3);

    store.ack("c1", Seq(10)).await.unwrap();
    assert_eq!(store.purge(later).await.unwrap(), 1, "head row is kept");
    let head = store.head().await.unwrap();
    assert_eq!((head.seq, head.purged_through), (Seq(10), Seq(9)));
    assert_eq!(
        store
            .read(Seq::ZERO, &TypeFilter::All, 100)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn unavailable_fails_every_operation() {
    let store = MemoryStore::new();
    let mut tx = store.begin().await.unwrap();
    store.append(&mut tx, &[ev("a")]).await.unwrap();
    store.set_unavailable(true);
    assert_eq!(store.commit(tx).await, Err(MemoryError::Unavailable));
    assert!(store.begin().await.is_err());
    store.set_unavailable(false);
    assert_eq!(
        store.unsequenced().await.unwrap(),
        0,
        "failed commit wrote nothing"
    );
}

#[tokio::test]
async fn foreign_transactions_are_rejected() {
    let a = MemoryStore::new();
    let b = MemoryStore::new();
    let mut tx = a.begin().await.unwrap();
    assert_eq!(
        b.append(&mut tx, &[ev("x")]).await,
        Err(MemoryError::ForeignTransaction)
    );
}
