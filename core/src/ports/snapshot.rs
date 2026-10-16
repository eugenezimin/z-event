//! [`Snapshotter`]: the host's current aggregate state, for bootstrapping
//! consumers. Provisional: finalised together with snapshot bootstrap.

use std::future::Future;

use super::store::Store;
use crate::error::BoxError;
use crate::types::{NewEvent, SnapshotRequest};

/// Produces the current state of the requested aggregates as `*Snapshot`
/// events, read through `tx` (one consistent read).
///
/// The feed reads its head `S` before calling this, then ends the snapshot
/// with `Checkpoint(S)`. Changes committed in between are delivered again
/// after `S`, which handlers tolerate by checking `aggregate_version`.
pub trait Snapshotter<S: Store>: Send + Sync + 'static {
    /// The snapshot events.
    fn snapshot(
        &self,
        tx: &mut S::Tx,
        request: &SnapshotRequest,
    ) -> impl Future<Output = Result<Vec<NewEvent>, BoxError>> + Send;
}
