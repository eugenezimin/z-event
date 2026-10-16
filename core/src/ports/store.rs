//! Storage ports: [`Store`], [`OutboxStore`] (producer side) and
//! [`CursorStore`] (consumer side).

use std::future::Future;

use futures::stream::BoxStream;
use time::OffsetDateTime;

use crate::types::{
    ConsumerState, Cursor, CursorKey, DeadLetter, Epoch, Event, FeedHead, NewEvent, Notification,
    ParkedFilter, Seq, TypeFilter,
};

/// A storage backend with native transactions.
///
/// The host uses the **same** transaction for its own business writes. That
/// is what makes "change + event" (producer) and "effect + cursor"
/// (consumer) atomic.
pub trait Store: Send + Sync + 'static {
    /// The adapter's native transaction. Dropping it MUST roll it back.
    type Tx: Send + 'static;
    /// The adapter's error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Begins a transaction.
    fn begin(&self) -> impl Future<Output = Result<Self::Tx, Self::Error>> + Send;

    /// Commits a transaction.
    fn commit(&self, tx: Self::Tx) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Rolls a transaction back explicitly. The default drops it.
    #[allow(clippy::manual_async_fn)]
    fn rollback(&self, tx: Self::Tx) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            drop(tx);
            Ok(())
        }
    }
}

/// The producer side: the outbox, the sequencer and the feed.
///
/// # The sequencing contract
///
/// Every implementation must guarantee, and the conformance suite checks:
/// 1. `seq` values are assigned only to committed events.
/// 2. `seq` is strictly increasing and gapless (1, 2, 3, …) within an epoch.
/// 3. Once an event has a `seq`, it never changes, and no event is later
///    inserted below the head.
/// 4. Two events for the same aggregate get `seq` values in commit order.
pub trait OutboxStore: Store {
    /// Proof of sequencer leadership. Dropping it without
    /// [`release`](OutboxStore::release) is a crash: the adapter frees it on
    /// connection loss or expiry.
    type Lease: Send + Sync + 'static;

    /// Appends events inside the host's transaction, unsequenced.
    fn append(
        &self,
        tx: &mut Self::Tx,
        events: &[NewEvent],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Tries to become the sequencer leader. `None` if another holder leads.
    fn try_lead(&self) -> impl Future<Output = Result<Option<Self::Lease>, Self::Error>> + Send;

    /// Gives leadership up (graceful shutdown).
    fn release(&self, lease: Self::Lease) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Stamps up to `limit` committed, unsequenced events with the next
    /// `seq` values, honouring the sequencing contract. Returns the new head
    /// if anything was stamped.
    ///
    /// MUST fail if the lease was lost (fencing) and MUST renew it: the
    /// producer calls this at least once per safety interval while leading.
    /// Publishes [`Notification::Sequenced`] when it stamped anything.
    fn sequence_batch(
        &self,
        lease: &Self::Lease,
        limit: u32,
    ) -> impl Future<Output = Result<Option<Seq>, Self::Error>> + Send;

    /// Sequenced events with `seq > after`, ascending, matching `types`, at
    /// most `limit`.
    fn read(
        &self,
        after: Seq,
        types: &TypeFilter,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<Event>, Self::Error>> + Send;

    /// The current head, epoch and retention boundary.
    fn head(&self) -> impl Future<Output = Result<FeedHead, Self::Error>> + Send;

    /// Starts a new epoch (after a restore from backup). Returns it.
    fn rotate_epoch(&self) -> impl Future<Output = Result<Epoch, Self::Error>> + Send;

    /// Wake-up hints: [`Notification::Appended`] after a commit that appended
    /// events, [`Notification::Sequenced`] after a batch was stamped. Hints
    /// may be coalesced; on internal overflow yield
    /// [`Notification::Lagged`] rather than dropping silently. The producer
    /// opens one such stream and fans it out.
    fn notifications(&self) -> BoxStream<'static, Notification>;

    /// Number of committed, unsequenced events (a stuck-sequencer signal).
    fn unsequenced(&self) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Registers (or re-registers) a consumer at position `at`.
    fn register_consumer(
        &self,
        consumer: &str,
        at: Seq,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Records an ack: `acked_seq = max(acked_seq, seq)`. No-op for an
    /// unregistered consumer.
    fn ack(&self, consumer: &str, seq: Seq)
    -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// The registered consumers.
    fn consumers(&self) -> impl Future<Output = Result<Vec<ConsumerState>, Self::Error>> + Send;

    /// Unregisters a decommissioned consumer, so it no longer holds back
    /// retention.
    fn remove_consumer(
        &self,
        consumer: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Deletes the longest prefix of the feed whose events were sequenced
    /// before `before` **and** are acked by every registered consumer,
    /// never the head event. Updates `purged_through`. Returns the count.
    fn purge(
        &self,
        before: OffsetDateTime,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;
}

/// The consumer side: cursors and parked events.
pub trait CursorStore: Store {
    /// Locks the cursor for `key` until the transaction ends and returns it,
    /// creating it on first use. Exclusive across concurrent transactions;
    /// returns the latest committed value after waiting (`SELECT … FOR
    /// UPDATE` semantics).
    fn lock_cursor(
        &self,
        tx: &mut Self::Tx,
        key: &CursorKey,
    ) -> impl Future<Output = Result<Cursor, Self::Error>> + Send;

    /// Moves the cursor (inside the transaction that holds its lock).
    fn advance(
        &self,
        tx: &mut Self::Tx,
        key: &CursorKey,
        to: Cursor,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reads the committed cursor without locking. Missing means
    /// [`Cursor::start`].
    fn cursor(&self, key: &CursorKey) -> impl Future<Output = Result<Cursor, Self::Error>> + Send;

    /// Overwrites the cursor (replay from a position, or after a rebuild).
    fn reset_cursor(
        &self,
        key: &CursorKey,
        to: Cursor,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Parks an event (upsert by key and `seq`; clears `resolved_at`).
    fn park(
        &self,
        tx: &mut Self::Tx,
        letter: &DeadLetter,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Whether `aggregate` has an unresolved parked event. MUST see this
    /// transaction's own parks and resolves.
    fn is_parked(
        &self,
        tx: &mut Self::Tx,
        key: &CursorKey,
        aggregate: &str,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Parked events matching `filter`, in ascending `seq` order.
    fn parked(
        &self,
        key: &CursorKey,
        filter: &ParkedFilter,
    ) -> impl Future<Output = Result<Vec<DeadLetter>, Self::Error>> + Send;

    /// Marks a parked event as applied (by a replay).
    fn resolve(
        &self,
        tx: &mut Self::Tx,
        key: &CursorKey,
        seq: Seq,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
