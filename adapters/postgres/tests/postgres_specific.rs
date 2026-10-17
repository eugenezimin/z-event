//! Behaviour specific to the PostgreSQL adapter (the shared contract is
//! covered by the conformance suites).

#![allow(clippy::unwrap_used)]

mod support;

use std::time::Duration;

use futures::StreamExt;
use z_event::{NewEvent, Notification, OutboxStore, Producer, ProducerConfig, Seq, Store};
use z_event_postgres::{PgError, PgStore};

fn fast(name: &str) -> ProducerConfig {
    ProducerConfig::new(name)
        .safety_interval(Duration::from_millis(50))
        .lead_retry_interval(Duration::from_millis(50))
}

async fn wait_head(store: &PgStore, seq: u64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while store.head().await.unwrap().seq < Seq(seq) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("head {seq} in {}", store.schema()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemas_in_one_database_are_independent() {
    let db = support::db("16-alpine").await;
    let orders = db.store().await;
    let billing = db.store().await;

    // Independent leases: both schemas can have a leader at once.
    let a = orders.try_lead().await.unwrap().expect("orders lease");
    let b = billing.try_lead().await.unwrap().expect("billing lease");
    orders.release(a).await.unwrap();
    billing.release(b).await.unwrap();

    let mut billing_hints = billing.notifications();
    let orders_producer = Producer::new(orders.clone(), fast("orders"));
    let billing_producer = Producer::new(billing.clone(), fast("billing"));
    let ra = orders_producer.start();
    let rb = billing_producer.start();
    tokio::time::sleep(Duration::from_millis(200)).await;

    for i in 0..3 {
        let mut tx = orders.begin().await.unwrap();
        orders_producer
            .append(&mut tx, NewEvent::new("T", format!("o/{i}"), 1, "{}"))
            .await
            .unwrap();
        orders.commit(tx).await.unwrap();
    }
    wait_head(&orders, 3).await;
    assert_eq!(billing.head().await.unwrap().seq, Seq::ZERO);
    assert_ne!(
        orders.head().await.unwrap().epoch,
        billing.head().await.unwrap().epoch
    );

    // No cross-talk: billing's channel saw nothing from orders.
    let heard = tokio::time::timeout(Duration::from_millis(300), billing_hints.next()).await;
    assert!(heard.is_err(), "billing heard {heard:?}");

    ra.shutdown().await;
    rb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn migrate_is_idempotent_and_safe_to_run_concurrently() {
    let db = support::db("16-alpine").await;
    let store = db.store().await; // migrated once
    let epoch = store.head().await.unwrap().epoch;
    store.migrate().await.unwrap();

    let mut runs = Vec::new();
    for _ in 0..4 {
        let s = store.clone();
        runs.push(tokio::spawn(async move { s.migrate().await }));
    }
    for r in runs {
        r.await.unwrap().unwrap();
    }
    assert_eq!(
        store.head().await.unwrap().epoch,
        epoch,
        "re-migrating keeps the feed"
    );

    let applied: Vec<i32> = sqlx::query_scalar(&format!(
        "SELECT version FROM {}.zevent_migrations ORDER BY version",
        store.schema()
    ))
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(applied, [1]);

    // A brand-new schema migrated by several replicas at once.
    let fresh = PgStore::new(store.pool().clone(), format!("{}_fresh", store.schema())).unwrap();
    let mut runs = Vec::new();
    for _ in 0..4 {
        let s = fresh.clone();
        runs.push(tokio::spawn(async move { s.migrate().await }));
    }
    for r in runs {
        r.await.unwrap().unwrap();
    }
    assert_eq!(fresh.head().await.unwrap().seq, Seq::ZERO);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_writes_and_events_share_the_transaction() {
    let db = support::db("16-alpine").await;
    let store = db.store().await;
    let table = format!("{}.orders", store.schema());
    sqlx::query(&format!(
        "CREATE TABLE {table} (id text PRIMARY KEY, status text NOT NULL)"
    ))
    .execute(store.pool())
    .await
    .unwrap();
    let insert = format!("INSERT INTO {table} VALUES ($1, 'confirmed')");

    // Rolled back: neither the order nor the event exists.
    let mut tx = store.begin().await.unwrap();
    sqlx::query(&insert)
        .bind("o1")
        .execute(&mut *tx)
        .await
        .unwrap();
    store
        .append(
            &mut tx,
            &[NewEvent::new("OrderConfirmed", "orders/o1", 1, "{}")],
        )
        .await
        .unwrap();
    store.rollback(tx).await.unwrap();
    let count = format!("SELECT count(*) FROM {table}");
    let orders: i64 = sqlx::query_scalar(&count)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!((orders, store.unsequenced().await.unwrap()), (0, 0));

    // Committed: both exist.
    let mut tx = store.begin().await.unwrap();
    sqlx::query(&insert)
        .bind("o2")
        .execute(&mut *tx)
        .await
        .unwrap();
    store
        .append(
            &mut tx,
            &[NewEvent::new("OrderConfirmed", "orders/o2", 1, "{}")],
        )
        .await
        .unwrap();
    store.commit(tx).await.unwrap();
    let orders: i64 = sqlx::query_scalar(&count)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!((orders, store.unsequenced().await.unwrap()), (1, 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_lease_frees_leadership() {
    let db = support::db("16-alpine").await;
    let store = db.store().await;
    let lease = store.try_lead().await.unwrap().expect("lease");
    assert!(store.try_lead().await.unwrap().is_none());

    // Dropping the lease closes its connection, like a crashed process.
    drop(lease);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(lease) = store.try_lead().await.unwrap() {
                return lease;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("another holder takes over");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn notifications_carry_the_new_head() {
    let db = support::db("16-alpine").await;
    let store = db.store().await;
    let mut hints = store.notifications();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut tx = store.begin().await.unwrap();
    store
        .append(
            &mut tx,
            &[
                NewEvent::new("T", "a/1", 1, "{}"),
                NewEvent::new("T", "a/2", 1, "{}"),
            ],
        )
        .await
        .unwrap();
    store.commit(tx).await.unwrap();
    let lease = store.try_lead().await.unwrap().unwrap();
    assert_eq!(
        store.sequence_batch(&lease, 10).await.unwrap(),
        Some(Seq(2))
    );

    let got: Vec<Notification> = tokio::time::timeout(Duration::from_secs(5), async {
        let mut got = Vec::new();
        while let Some(n) = hints.next().await {
            got.push(n);
            if matches!(n, Notification::Sequenced(_)) {
                return got;
            }
        }
        got
    })
    .await
    .unwrap();
    // One Appended per committed transaction (NOTIFY folds duplicates).
    assert_eq!(
        got,
        [Notification::Appended, Notification::Sequenced(Seq(2))]
    );
    store.release(lease).await.unwrap();
}

#[tokio::test]
async fn invalid_schema_names_are_rejected() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/none")
        .unwrap();
    assert!(matches!(
        PgStore::new(pool, "orders; DROP TABLE x"),
        Err(PgError::InvalidSchema(_))
    ));
}
