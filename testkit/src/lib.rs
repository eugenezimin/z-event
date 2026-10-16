//! # z-event-testkit
//!
//! Conformance suites every z-event storage or transport adapter must pass.
//!
//! - [`storage`]: the sequencing contract (commit-order gap, gapless and
//!   immutable `seq`), crash safety of effect + cursor, sequencer failover,
//!   parking and replay, outages, duplicates, retention, epochs.
//! - [`transport`]: delivery through a real transport: network drops,
//!   identity and ACL, backpressure, heartbeats, reconnect backoff, acks,
//!   ordering, head and snapshot.
//!
//! A storage adapter runs the storage suite (one `#[tokio::test]` per case):
//!
//! ```ignore
//! z_event_testkit::storage_suite!(|| async { MyStore::fresh().await });
//! ```
//!
//! or everything in one test:
//!
//! ```ignore
//! #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//! async fn conformance() {
//!     z_event_testkit::storage::run_all(|| async { MyStore::fresh().await }).await;
//! }
//! ```
//!
//! The store under test must implement both [`OutboxStore`] and
//! [`CursorStore`], because test handlers record their effects by
//! appending to the consumer store's own outbox in the handler's
//! transaction. The factory must return a **new, empty** storage on every
//! call (for SQL: a new schema or database).
//!
//! Environment: `ZEVENT_TESTKIT_SEED` fixes the random seed (printed on
//! every run), `ZEVENT_TESTKIT_SCALE` multiplies iteration counts.
//!
//! [`OutboxStore`]: z_event::OutboxStore
//! [`CursorStore`]: z_event::CursorStore

#![allow(clippy::unwrap_used)]

mod factory;
pub mod faults;
pub mod handlers;
mod macros;
pub mod storage;
pub mod transport;
pub mod util;

pub use factory::StoreFactory;
pub use transport::{InProcessHarness, TransportHarness};
pub use util::World;
