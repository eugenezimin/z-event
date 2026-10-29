//! Runs the example as two real processes against one PostgreSQL server,
//! with one database per service as in production.

#![allow(clippy::unwrap_used)]

use std::process::Stdio;
use std::time::Duration;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_z-event-example-split");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orders_and_billing_as_two_processes() {
    // An existing server (ZEVENT_PG_URL) may not allow CREATE DATABASE, so
    // there both services share its database, in separate schemas.
    let (orders_url, billing_url, _container) = match std::env::var("ZEVENT_PG_URL") {
        Ok(url) => (url.clone(), url, None),
        Err(_) => {
            let c = Postgres::default()
                .with_tag("16-alpine")
                .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
                .start()
                .await
                .expect("start a PostgreSQL container (is Docker running?)");
            let port = c.get_host_port_ipv4(5432).await.unwrap();
            let base = format!("postgres://postgres:postgres@127.0.0.1:{port}");
            let admin = sqlx::PgPool::connect(&format!("{base}/postgres"))
                .await
                .unwrap();
            for db in ["orders", "billing"] {
                sqlx::query(&format!("CREATE DATABASE {db}"))
                    .execute(&admin)
                    .await
                    .unwrap();
            }
            (format!("{base}/orders"), format!("{base}/billing"), Some(c))
        }
    };
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let mut orders = Command::new(BIN)
        .args([
            "orders",
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--count",
            "30",
        ])
        .env("DATABASE_URL", &orders_url)
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut billing = Command::new(BIN)
        .args([
            "billing",
            "--orders",
            &format!("http://127.0.0.1:{port}"),
            "--until",
            "30",
        ])
        .env("DATABASE_URL", &billing_url)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    let status = tokio::time::timeout(Duration::from_secs(90), billing.wait())
        .await
        .expect("billing finishes")
        .unwrap();
    let mut out = String::new();
    billing
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut out)
        .await
        .unwrap();
    assert!(status.success(), "billing failed: {out}");
    assert!(out.contains("billing: 30 invoices"), "{out}");
    orders.kill().await.unwrap();

    // Exactly one invoice per order, no parked events.
    let pool = sqlx::PgPool::connect(&billing_url).await.unwrap();
    let (invoices, distinct): (i64, i64) =
        sqlx::query_as("SELECT count(*), count(DISTINCT event_id) FROM billing.invoices")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((invoices, distinct), (30, 30));
    let parked: i64 = sqlx::query_scalar("SELECT count(*) FROM billing_zevent.dead_letters")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(parked, 0);
}
