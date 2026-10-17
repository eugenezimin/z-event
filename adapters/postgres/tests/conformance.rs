//! The PostgreSQL adapter must pass the conformance suites, on the oldest
//! and the newest supported PostgreSQL.

mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_suite_postgres_16() {
    let db = support::db("16-alpine").await;
    z_event_testkit::storage::run_all(db.factory()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_suite_postgres_13() {
    let db = support::db("13-alpine").await;
    z_event_testkit::storage::run_all(db.factory()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transport_suite_on_postgres_16() {
    let db = support::db("16-alpine").await;
    z_event_testkit::transport::run_all(db.factory(), z_event_testkit::InProcessHarness).await;
}
