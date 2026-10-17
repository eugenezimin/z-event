//! Test databases: a PostgreSQL container per test (testcontainers), or an
//! existing server when `ZEVENT_PG_URL` is set. Every store gets a fresh
//! schema.

#![allow(dead_code, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use z_event_postgres::PgStore;
use z_event_testkit::StoreFactory;

/// A database for one test. The container (if any) stops when this drops.
pub struct Db {
    pub pool: PgPool,
    prefix: String,
    counter: Arc<AtomicU32>,
    _container: Option<ContainerAsync<Postgres>>,
}

/// Starts `postgres:<tag>` (or connects to `ZEVENT_PG_URL`).
pub async fn db(tag: &str) -> Db {
    let (url, container) = match std::env::var("ZEVENT_PG_URL") {
        Ok(url) => (url, None),
        Err(_) => {
            let container = Postgres::default()
                .with_tag(tag)
                .with_cmd(["postgres", "-c", "max_connections=500"])
                // The crash test reconnects about a thousand times; SCRAM
                // (the default since PostgreSQL 14) is slow in debug builds.
                .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
                .start()
                .await
                .expect("start a PostgreSQL container (is Docker running?)");
            let port = container.get_host_port_ipv4(5432).await.unwrap();
            (
                format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
                Some(container),
            )
        }
    };
    let pool = PgPoolOptions::new()
        .max_connections(40)
        .connect(&url)
        .await
        .unwrap();
    Db {
        pool,
        // Unique per run, so a shared server is never polluted across runs.
        prefix: format!("ze{:08x}", fastrand_u32()),
        counter: Arc::default(),
        _container: container,
    }
}

fn fastrand_u32() -> u32 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    nanos ^ std::process::id().rotate_left(16)
}

impl Db {
    /// A migrated store in a new schema.
    pub async fn store(&self) -> PgStore {
        new_store(&self.pool, &self.prefix, &self.counter).await
    }

    /// A factory of migrated stores in new schemas, for the suites.
    pub fn factory(&self) -> impl StoreFactory<Store = PgStore> + use<> {
        let pool = self.pool.clone();
        let prefix = self.prefix.clone();
        let counter = self.counter.clone();
        move || {
            let pool = pool.clone();
            let prefix = prefix.clone();
            let counter = counter.clone();
            async move { new_store(&pool, &prefix, &counter).await }
        }
    }
}

async fn new_store(pool: &PgPool, prefix: &str, counter: &AtomicU32) -> PgStore {
    let n = counter.fetch_add(1, Ordering::SeqCst);
    let store = PgStore::new(pool.clone(), format!("{prefix}_{n}")).unwrap();
    store.migrate().await.unwrap();
    store
}
