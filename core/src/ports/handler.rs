//! [`Handler`]: the host's business logic for one subscription.

use std::future::Future;

use super::store::Store;
use crate::error::HandlerError;
use crate::types::Event;

/// Applies one event inside the consumer's transaction.
///
/// The subscriber commits the handler's writes together with the cursor, so
/// a database effect happens exactly once. For external side effects, write
/// a job row here and let a worker execute it with the event `id` as the
/// idempotency key.
///
/// ```
/// use z_event::{Event, Handler, HandlerError, Store};
///
/// struct CountOrders;
///
/// impl<S: Store> Handler<S> for CountOrders {
///     async fn handle(&self, _tx: &mut S::Tx, event: &Event) -> Result<(), HandlerError> {
///         if event.payload.is_empty() {
///             return Err(HandlerError::permanent("empty payload"));
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait Handler<S: Store>: Send + Sync + 'static {
    /// Applies `event` using `tx`. Errors roll the transaction back.
    fn handle(
        &self,
        tx: &mut S::Tx,
        event: &Event,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;
}
