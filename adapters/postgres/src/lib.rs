//! # z-event-postgres
//!
//! PostgreSQL storage for z-event: the [`Store`], [`OutboxStore`] and
//! [`CursorStore`] ports on [sqlx](https://docs.rs/sqlx) 0.8.
//!
//! [`OutboxStore`]: z_event::OutboxStore
//! [`CursorStore`]: z_event::CursorStore
//!
//! - **Transactions.** `Store::Tx` is `sqlx::Transaction<'static, Postgres>`.
//!   The host runs its own business SQL on the same transaction, which makes
//!   "change + event" and "effect + cursor" atomic.
//! - **A schema of its own, in the service's database.** A [`PgStore`] owns
//!   the z-event tables in one schema (`outbox`, `feed_meta`,
//!   `feed_consumers`, `feed_cursors`, `dead_letters`, plus fixed index,
//!   function and trigger names). Use a dedicated schema such as
//!   `orders_zevent` next to the business schema `orders`, so these names
//!   never collide with the service's own objects. It must be the same
//!   database, because the outbox row and the cursor commit in the
//!   business transaction. Queries are schema-qualified, so they do not
//!   depend on the connection's `search_path`.
//! - **Pools.** Any transaction on the same database can carry
//!   [`append`](z_event::Producer::append), so the service may begin it on
//!   its own pool and give the store a small pool for z-event's background
//!   work (sequencer, feed reads, acks, retention). The sequencer lease and
//!   the `LISTEN` connection are dedicated connections on top of the pool.
//! - **Migrations** are embedded: [`PgStore::migrate`] creates or upgrades
//!   the schema, with its own history table (`zevent_migrations`), so it
//!   never clashes with the host's migrations. Safe to run from several
//!   replicas at once.
//! - **Sequencer.** One leader per schema holds a session advisory lock on a
//!   dedicated connection. It stamps committed rows with the next `seq`
//!   values in one statement (uncommitted rows are invisible to it, which is
//!   how the commit-order gap is closed) and moves `feed_meta.head` in the
//!   same transaction. If the leader's connection dies, the lock goes with
//!   it and another replica takes over.
//! - **Notifications.** An `AFTER INSERT` statement trigger calls
//!   `pg_notify` on commit; the sequencer notifies the new head. A
//!   `LISTEN` connection turns both into z-event notifications.
//! - **Runtime-checked queries only**: no `query!` macros, which would
//!   compile-check against the host's `DATABASE_URL`.
//!
//! ```no_run
//! use sqlx::postgres::PgPoolOptions;
//! use z_event::{NewEvent, OutboxStore, Producer, ProducerConfig, Store};
//! use z_event_postgres::PgStore;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = PgPoolOptions::new().connect("postgres://localhost/orders").await?;
//! // Business tables live in schema `orders`, z-event's in `orders_zevent`.
//! let store = PgStore::new(pool, "orders_zevent")?;
//! store.migrate().await?;
//!
//! let producer = Producer::new(store.clone(), ProducerConfig::new("orders"));
//! let _running = producer.start();
//!
//! let mut tx = store.begin().await?;
//! sqlx::query("UPDATE orders.orders SET status = 'confirmed' WHERE id = $1")
//!     .bind("o17")
//!     .execute(&mut *tx)
//!     .await?;
//! producer
//!     .append(&mut tx, NewEvent::new("OrderConfirmed", "orders/o17", 2, "{}"))
//!     .await?;
//! store.commit(tx).await?;
//! # Ok(())
//! # }
//! ```

mod cursor;
mod migrate;
mod notify;
mod outbox;
mod sql;

use std::future::Future;
use std::sync::Arc;

use sqlx::{PgPool, Postgres};
use z_event::Store;

pub use crate::outbox::PgLease;
use crate::sql::Sql;

/// Errors of the PostgreSQL store.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PgError {
    /// A database error.
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
    /// A stored envelope could not be (de)serialised.
    #[error("envelope: {0}")]
    Envelope(#[from] serde_json::Error),
    /// The schema name is not a plain lower-case identifier.
    #[error("invalid schema name {0:?}: use [a-z_][a-z0-9_]*, at most 40 characters")]
    InvalidSchema(String),
    /// A stored value does not fit the envelope (e.g. a negative `seq`).
    #[error("corrupt row: {0}")]
    Corrupt(String),
    /// A value does not fit a PostgreSQL column (e.g. `u64` above `i64::MAX`).
    #[error("out of range: {0}")]
    OutOfRange(&'static str),
}

pub(crate) struct Inner {
    pub(crate) pool: PgPool,
    pub(crate) schema: String,
    pub(crate) sql: Sql,
    pub(crate) lock_key: i64,
    pub(crate) appended_channel: String,
    pub(crate) sequenced_channel: String,
}

/// z-event storage in one PostgreSQL schema. Cheap to clone.
///
/// ```no_run
/// use sqlx::PgPool;
/// use z_event_postgres::PgStore;
///
/// # async fn run(pool: PgPool) -> Result<(), z_event_postgres::PgError> {
/// // One store per service, each in the service's z-event schema. Services
/// // normally have a database each; for tests they may share one.
/// let orders = PgStore::new(pool.clone(), "orders_zevent")?;
/// let billing = PgStore::new(pool, "billing_zevent")?;
/// orders.migrate().await?;
/// billing.migrate().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct PgStore {
    pub(crate) inner: Arc<Inner>,
}

impl std::fmt::Debug for PgStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgStore")
            .field("schema", &self.inner.schema)
            .finish_non_exhaustive()
    }
}

/// Whether `schema` is a plain identifier we can embed in SQL and in
/// notification channel names (which are limited to 63 bytes).
fn valid_schema(schema: &str) -> bool {
    let mut chars = schema.chars();
    matches!(chars.next(), Some('a'..='z' | '_'))
        && chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_'))
        && schema.len() <= 40
}

/// FNV-1a: a stable 64-bit key for the schema's advisory locks.
fn lock_key(schema: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in "z-event:".bytes().chain(schema.bytes()) {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    i64::from_ne_bytes(hash.to_ne_bytes())
}

impl PgStore {
    /// A store for the z-event tables in `schema`. Run
    /// [`migrate`](Self::migrate) before use.
    ///
    /// The schema name must match `[a-z_][a-z0-9_]*` and be at most 40
    /// characters long.
    pub fn new(pool: PgPool, schema: impl Into<String>) -> Result<Self, PgError> {
        let schema = schema.into();
        if !valid_schema(&schema) {
            return Err(PgError::InvalidSchema(schema));
        }
        Ok(PgStore {
            inner: Arc::new(Inner {
                sql: Sql::new(&schema),
                lock_key: lock_key(&schema),
                appended_channel: format!("zevent_{schema}_appended"),
                sequenced_channel: format!("zevent_{schema}_sequenced"),
                pool,
                schema,
            }),
        })
    }

    /// Creates or upgrades the z-event tables in the store's schema.
    // Declared `+ Send` so callers can spawn it: rustc cannot always prove
    // sqlx futures `Send` through a plain `async fn` in generic contexts.
    pub fn migrate(&self) -> impl Future<Output = Result<(), PgError>> + Send + '_ {
        migrate::run(&self.inner)
    }

    /// The connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    /// The schema holding the z-event tables.
    pub fn schema(&self) -> &str {
        &self.inner.schema
    }
}

impl Store for PgStore {
    type Tx = sqlx::Transaction<'static, Postgres>;
    type Error = PgError;

    async fn begin(&self) -> Result<Self::Tx, PgError> {
        Ok(self.inner.pool.begin().await?)
    }

    async fn commit(&self, tx: Self::Tx) -> Result<(), PgError> {
        Ok(tx.commit().await?)
    }

    async fn rollback(&self, tx: Self::Tx) -> Result<(), PgError> {
        Ok(tx.rollback().await?)
    }

    async fn begin_snapshot(&self) -> Result<Self::Tx, PgError> {
        let mut tx = self.inner.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }
}

/// Creates a [`PgStore`] for `schema` and migrates it: the one call a
/// service makes at startup.
///
/// ```no_run
/// # async fn run(pool: sqlx::PgPool) -> Result<(), z_event_postgres::PgError> {
/// let store = z_event_postgres::migrate(&pool, "orders_zevent").await?;
/// # let _ = store;
/// # Ok(())
/// # }
/// ```
pub async fn migrate(pool: &PgPool, schema: &str) -> Result<PgStore, PgError> {
    let store = PgStore::new(pool.clone(), schema)?;
    store.migrate().await?;
    Ok(store)
}

pub(crate) fn to_i64(v: u64, what: &'static str) -> Result<i64, PgError> {
    i64::try_from(v).map_err(|_| PgError::OutOfRange(what))
}

pub(crate) fn to_u64(v: i64, what: &str) -> Result<u64, PgError> {
    u64::try_from(v).map_err(|_| PgError::Corrupt(format!("negative {what}: {v}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_names() {
        assert!(valid_schema("orders"));
        assert!(valid_schema("_svc_2"));
        assert!(!valid_schema("Orders"));
        assert!(!valid_schema("2orders"));
        assert!(!valid_schema("a;drop"));
        assert!(!valid_schema(""));
        assert!(!valid_schema(&"a".repeat(41)));
    }

    #[test]
    fn lock_keys_are_stable_and_distinct() {
        assert_eq!(lock_key("orders"), lock_key("orders"));
        assert_ne!(lock_key("orders"), lock_key("billing"));
    }
}
