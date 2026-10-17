//! Ports: the traits adapters and hosts implement.
//!
//! | Port | Implemented by |
//! |---|---|
//! | [`Store`] + [`OutboxStore`] | a storage adapter, producer side |
//! | [`Store`] + [`CursorStore`] | a storage adapter, consumer side |
//! | [`Transport`] | a transport adapter, client side |
//! | [`Handler`] | the host: business logic of a subscription |
//! | [`Acl`], [`Snapshotter`] | the host (or the defaults) |
//!
//! Async methods are declared as `fn …(…) -> impl Future<Output = …> + Send`
//! so the runtime can spawn them on a multi-threaded executor. Implementors
//! simply write `async fn`:
//!
//! ```
//! use std::convert::Infallible;
//! use z_event::ports::*;
//! use z_event::*;
//!
//! /// A store that stores nothing: proves the ports are implementable with
//! /// plain `async fn` and that the resulting futures are `Send`.
//! struct NoopStore;
//! struct NoopTx;
//! struct NoopLease;
//!
//! impl Store for NoopStore {
//!     type Tx = NoopTx;
//!     type Error = Infallible;
//!     async fn begin(&self) -> Result<NoopTx, Infallible> { Ok(NoopTx) }
//!     async fn commit(&self, _tx: NoopTx) -> Result<(), Infallible> { Ok(()) }
//! }
//!
//! impl OutboxStore for NoopStore {
//!     type Lease = NoopLease;
//!     async fn append(&self, _tx: &mut NoopTx, _events: &[NewEvent]) -> Result<(), Infallible> { Ok(()) }
//!     async fn try_lead(&self) -> Result<Option<NoopLease>, Infallible> { Ok(Some(NoopLease)) }
//!     async fn release(&self, _lease: NoopLease) -> Result<(), Infallible> { Ok(()) }
//!     async fn sequence_batch(&self, _l: &NoopLease, _limit: u32) -> Result<Option<Seq>, Infallible> { Ok(None) }
//!     async fn read(&self, _after: Seq, _t: &TypeFilter, _limit: u32) -> Result<Vec<Event>, Infallible> { Ok(vec![]) }
//!     async fn head(&self) -> Result<FeedHead, Infallible> {
//!         Ok(FeedHead { seq: Seq::ZERO, epoch: Epoch::new("e").unwrap(), purged_through: Seq::ZERO })
//!     }
//!     async fn rotate_epoch(&self) -> Result<Epoch, Infallible> { Ok(Epoch::generate()) }
//!     fn notifications(&self) -> BoxStream<'static, Notification> { Box::pin(futures::stream::empty()) }
//!     async fn unsequenced(&self) -> Result<u64, Infallible> { Ok(0) }
//!     async fn register_consumer(&self, _c: &str, _at: Seq) -> Result<(), Infallible> { Ok(()) }
//!     async fn ack(&self, _c: &str, _seq: Seq) -> Result<(), Infallible> { Ok(()) }
//!     async fn consumers(&self) -> Result<Vec<ConsumerState>, Infallible> { Ok(vec![]) }
//!     async fn remove_consumer(&self, _c: &str) -> Result<(), Infallible> { Ok(()) }
//!     async fn purge(&self, _before: OffsetDateTime) -> Result<u64, Infallible> { Ok(0) }
//! }
//!
//! impl CursorStore for NoopStore {
//!     async fn lock_cursor(&self, _tx: &mut NoopTx, _k: &CursorKey) -> Result<Cursor, Infallible> { Ok(Cursor::start()) }
//!     async fn advance(&self, _tx: &mut NoopTx, _k: &CursorKey, _to: Cursor) -> Result<(), Infallible> { Ok(()) }
//!     async fn cursor(&self, _k: &CursorKey) -> Result<Cursor, Infallible> { Ok(Cursor::start()) }
//!     async fn reset_cursor(&self, _k: &CursorKey, _to: Cursor) -> Result<(), Infallible> { Ok(()) }
//!     async fn park(&self, _tx: &mut NoopTx, _l: &DeadLetter) -> Result<(), Infallible> { Ok(()) }
//!     async fn is_parked(&self, _tx: &mut NoopTx, _k: &CursorKey, _a: &str) -> Result<bool, Infallible> { Ok(false) }
//!     async fn parked(&self, _k: &CursorKey, _f: &ParkedFilter) -> Result<Vec<DeadLetter>, Infallible> { Ok(vec![]) }
//!     async fn resolve(&self, _tx: &mut NoopTx, _k: &CursorKey, _s: Seq) -> Result<(), Infallible> { Ok(()) }
//! }
//!
//! struct Ignore;
//! impl Handler<NoopStore> for Ignore {
//!     async fn handle(&self, _tx: &mut NoopTx, _e: &Event) -> Result<(), HandlerError> { Ok(()) }
//! }
//!
//! // Generic code over the ports produces Send futures.
//! async fn apply_one<S: CursorStore, H: Handler<S>>(s: &S, h: &H, e: &Event) {
//!     let key = CursorKey::new("billing", "orders");
//!     let mut tx = s.begin().await.ok().unwrap();
//!     let cursor = s.lock_cursor(&mut tx, &key).await.ok().unwrap();
//!     h.handle(&mut tx, e).await.unwrap();
//!     s.advance(&mut tx, &key, cursor.next(e.seq)).await.ok().unwrap();
//!     s.commit(tx).await.ok().unwrap();
//! }
//! fn assert_send<T: Send>(_: T) {}
//! let ev = NewEvent::new("A", "x/1", 1, Bytes::new()).into_event(Seq(1));
//! assert_send(apply_one(&NoopStore, &Ignore, &ev));
//! assert_send(NoopStore.sequence_batch(&NoopLease, 10));
//! ```

mod acl;
mod handler;
mod snapshot;
mod store;
mod transport;

pub use acl::{Acl, AllowAll, TypeAcl};
pub use futures::stream::BoxStream;
pub use handler::Handler;
pub use snapshot::Snapshotter;
pub use store::{CursorStore, OutboxStore, Store};
pub use transport::{AckReceiver, AckSender, MessageStream, Subscription, Transport};
