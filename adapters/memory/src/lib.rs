//! # z-event-memory
//!
//! In-memory storage for z-event: the reference implementation of the
//! [`Store`], [`OutboxStore`] and [`CursorStore`] ports, used by tests,
//! examples and single-process setups that need no durability.
//!
//! One [`MemoryStore`] stands for **one service's storage** (one database
//! schema): its outbox and feed metadata (producer side), its cursors and
//! dead letters (consumer side), and a small key-value space for the host's
//! own business data. Replicas of one service share one `MemoryStore`, the
//! way two processes share one database.
//!
//! How it meets the sequencing contract: insert ids are taken when an event
//! is appended, but events become visible only when their transaction
//! commits, so committed events can appear out of insert order (the
//! commit-order gap). The sequencer stamps only committed events, under a
//! lease with fencing, from the head upwards. Commit is a single synchronous
//! critical section, so it is atomic even if the committing task is aborted.
//!
//! ```
//! use z_event::{NewEvent, OutboxStore, Store, TypeFilter, Seq};
//! use z_event_memory::MemoryStore;
//!
//! # tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(async {
//! let store = MemoryStore::new();
//! let mut tx = store.begin().await?;
//! tx.put("orders/o1", "confirmed");
//! store.append(&mut tx, &[NewEvent::new("OrderConfirmed", "orders/o1", 1, "{}")]).await?;
//! store.commit(tx).await?;
//!
//! let lease = store.try_lead().await?.expect("no other leader");
//! assert_eq!(store.sequence_batch(&lease, 100).await?, Some(Seq(1)));
//! assert_eq!(store.read(Seq::ZERO, &TypeFilter::All, 10).await?.len(), 1);
//! # Ok::<(), z_event_memory::MemoryError>(())
//! # }).unwrap();
//! ```

mod state;
mod tx;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{self, BoxStream};
use time::OffsetDateTime;
use tokio::sync::broadcast::error::RecvError;
use z_event::{
    ConsumerState, Cursor, CursorKey, CursorStore, DeadLetter, Epoch, Event, FeedHead, NewEvent,
    Notification, OutboxStore, ParkedFilter, Seq, Store, TypeFilter,
};

use crate::state::{Inner, LeaseRow, Sequenced};
use crate::tx::LetterOp;
pub use crate::tx::MemoryTx;

/// Errors of the in-memory store.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MemoryError {
    /// The sequencer lease expired or was taken over (fencing).
    #[error("sequencer lease lost")]
    LeaseLost,
    /// The store was switched to unavailable (fault injection).
    #[error("store unavailable")]
    Unavailable,
    /// A transaction was used with another store.
    #[error("transaction belongs to another store")]
    ForeignTransaction,
}

/// Proof of sequencer leadership on a [`MemoryStore`]. Dropping it without
/// `release` simulates a crash: it frees itself when it expires.
#[derive(Debug)]
pub struct MemoryLease {
    token: u64,
}

/// In-memory storage for one service. Cheap to clone; clones share state.
#[derive(Clone)]
pub struct MemoryStore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore").finish_non_exhaustive()
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Configures a [`MemoryStore`].
#[derive(Debug, Clone)]
pub struct MemoryStoreBuilder {
    lease_ttl: Duration,
    notify_capacity: usize,
}

impl MemoryStoreBuilder {
    /// How long a sequencer lease lives without renewal (default 3 s). Keep
    /// it at least three times the producer's safety interval.
    pub fn lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }

    /// Capacity of the notification channel (default 1024).
    pub fn notify_capacity(mut self, capacity: usize) -> Self {
        self.notify_capacity = capacity.max(1);
        self
    }

    /// Builds the store.
    pub fn build(self) -> MemoryStore {
        MemoryStore {
            inner: Arc::new(Inner::new(self.lease_ttl, self.notify_capacity)),
        }
    }
}

impl MemoryStore {
    /// An empty store with default settings.
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// A builder.
    pub fn builder() -> MemoryStoreBuilder {
        MemoryStoreBuilder {
            lease_ttl: Duration::from_secs(3),
            notify_capacity: 1024,
        }
    }

    /// Reads a committed host value.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.inner.peek(|s| s.kv.get(key).cloned())
    }

    /// Committed host values whose key starts with `prefix`, in key order.
    pub fn scan_prefix(&self, prefix: &str) -> Vec<(String, Bytes)> {
        self.inner.peek(|s| {
            s.kv.range(prefix.to_owned()..)
                .take_while(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
    }

    /// Test hook: expires the current sequencer lease now, so another
    /// replica can take over immediately.
    pub fn expire_lease(&self) {
        self.inner.peek(|s| {
            if let Some(lease) = s.lease.as_mut() {
                lease.expires_at = tokio::time::Instant::now();
            }
        });
    }

    /// Fault hook: while on, every operation fails with
    /// [`MemoryError::Unavailable`].
    pub fn set_unavailable(&self, on: bool) {
        self.inner.set_unavailable(on);
    }

    fn own(&self, tx: &MemoryTx) -> Result<(), MemoryError> {
        if Arc::ptr_eq(&self.inner, &tx.inner) {
            Ok(())
        } else {
            Err(MemoryError::ForeignTransaction)
        }
    }
}

impl Store for MemoryStore {
    type Tx = MemoryTx;
    type Error = MemoryError;

    async fn begin(&self) -> Result<MemoryTx, MemoryError> {
        self.inner.check()?;
        Ok(MemoryTx::new(self.inner.clone()))
    }

    async fn commit(&self, mut tx: MemoryTx) -> Result<(), MemoryError> {
        self.own(&tx)?;
        let appended = self.inner.with_state(|state| tx.apply(state))?;
        tx.notify_committed(appended);
        // Row locks are released here, after the state is updated.
        drop(tx);
        Ok(())
    }
}

impl OutboxStore for MemoryStore {
    type Lease = MemoryLease;

    async fn append(&self, tx: &mut MemoryTx, events: &[NewEvent]) -> Result<(), MemoryError> {
        self.own(tx)?;
        self.inner.check()?;
        for ev in events {
            tx.appended.push((self.inner.next_insert_id(), ev.clone()));
        }
        Ok(())
    }

    async fn try_lead(&self) -> Result<Option<MemoryLease>, MemoryError> {
        let ttl = self.inner.lease_ttl;
        self.inner.with_state(|s| {
            let now = tokio::time::Instant::now();
            if s.lease.as_ref().is_some_and(|l| l.expires_at > now) {
                return None;
            }
            s.lease_counter += 1;
            let token = s.lease_counter;
            s.lease = Some(LeaseRow {
                token,
                expires_at: now + ttl,
            });
            Some(MemoryLease { token })
        })
    }

    async fn release(&self, lease: MemoryLease) -> Result<(), MemoryError> {
        self.inner.with_state(|s| {
            if s.lease.as_ref().is_some_and(|l| l.token == lease.token) {
                s.lease = None;
            }
        })
    }

    async fn sequence_batch(
        &self,
        lease: &MemoryLease,
        limit: u32,
    ) -> Result<Option<Seq>, MemoryError> {
        let ttl = self.inner.lease_ttl;
        let head = self.inner.with_state(|s| {
            let now = tokio::time::Instant::now();
            match s.lease.as_mut() {
                Some(l) if l.token == lease.token && l.expires_at > now => {
                    l.expires_at = now + ttl;
                }
                _ => return Err(MemoryError::LeaseLost),
            }
            let ids: Vec<u64> = s.unsequenced.keys().take(limit as usize).copied().collect();
            if ids.is_empty() {
                return Ok(None);
            }
            let sequenced_at = OffsetDateTime::now_utc();
            for id in ids {
                if let Some(ev) = s.unsequenced.remove(&id) {
                    s.head = s.head.next();
                    let event = ev.into_event(s.head);
                    s.feed.insert(
                        s.head,
                        Sequenced {
                            event,
                            sequenced_at,
                        },
                    );
                }
            }
            Ok(Some(s.head))
        })??;
        if let Some(h) = head {
            self.inner.publish(Notification::Sequenced(h));
        }
        Ok(head)
    }

    async fn read(
        &self,
        after: Seq,
        types: &TypeFilter,
        limit: u32,
    ) -> Result<Vec<Event>, MemoryError> {
        self.inner.with_state(|s| {
            s.feed
                .range(after.next()..)
                .map(|(_, row)| &row.event)
                .filter(|ev| types.matches(&ev.event_type))
                .take(limit as usize)
                .cloned()
                .collect()
        })
    }

    async fn head(&self) -> Result<FeedHead, MemoryError> {
        self.inner.with_state(|s| FeedHead {
            seq: s.head,
            epoch: s.epoch.clone(),
            purged_through: s.purged_through,
        })
    }

    async fn rotate_epoch(&self) -> Result<Epoch, MemoryError> {
        self.inner.with_state(|s| {
            s.epoch = Epoch::generate();
            s.epoch.clone()
        })
    }

    fn notifications(&self) -> BoxStream<'static, Notification> {
        let rx = self.inner.notify.subscribe();
        Box::pin(stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(n) => Some((n, rx)),
                Err(RecvError::Lagged(_)) => Some((Notification::Lagged, rx)),
                Err(RecvError::Closed) => None,
            }
        }))
    }

    async fn unsequenced(&self) -> Result<u64, MemoryError> {
        self.inner.with_state(|s| s.unsequenced.len() as u64)
    }

    async fn register_consumer(&self, consumer: &str, at: Seq) -> Result<(), MemoryError> {
        self.inner.with_state(|s| {
            s.consumers.insert(
                consumer.to_owned(),
                ConsumerState {
                    consumer: consumer.to_owned(),
                    acked_seq: at,
                    acked_at: Some(OffsetDateTime::now_utc()),
                },
            );
        })
    }

    async fn ack(&self, consumer: &str, seq: Seq) -> Result<(), MemoryError> {
        self.inner.with_state(|s| {
            if let Some(c) = s.consumers.get_mut(consumer) {
                if seq > c.acked_seq {
                    c.acked_seq = seq;
                }
                c.acked_at = Some(OffsetDateTime::now_utc());
            }
        })
    }

    async fn consumers(&self) -> Result<Vec<ConsumerState>, MemoryError> {
        self.inner
            .with_state(|s| s.consumers.values().cloned().collect())
    }

    async fn remove_consumer(&self, consumer: &str) -> Result<(), MemoryError> {
        self.inner.with_state(|s| {
            s.consumers.remove(consumer);
        })
    }

    async fn purge(&self, before: OffsetDateTime) -> Result<u64, MemoryError> {
        self.inner.with_state(|s| {
            let min_acked = s.consumers.values().map(|c| c.acked_seq).min();
            let mut purged = 0;
            while let Some((&seq, row)) = s.feed.first_key_value() {
                let acked = min_acked.is_none_or(|m| seq <= m);
                if seq >= s.head || row.sequenced_at >= before || !acked {
                    break;
                }
                s.feed.remove(&seq);
                s.purged_through = seq;
                purged += 1;
            }
            purged
        })
    }
}

impl CursorStore for MemoryStore {
    async fn lock_cursor(&self, tx: &mut MemoryTx, key: &CursorKey) -> Result<Cursor, MemoryError> {
        self.own(tx)?;
        self.inner.check()?;
        tx.ensure_cursor_lock(key).await;
        if let Some(staged) = tx.cursor_writes.get(key) {
            return Ok(staged.clone());
        }
        self.inner
            .with_state(|s| s.cursors.get(key).cloned().unwrap_or_default())
    }

    async fn advance(
        &self,
        tx: &mut MemoryTx,
        key: &CursorKey,
        to: Cursor,
    ) -> Result<(), MemoryError> {
        self.own(tx)?;
        self.inner.check()?;
        tx.ensure_cursor_lock(key).await;
        tx.cursor_writes.insert(key.clone(), to);
        Ok(())
    }

    async fn cursor(&self, key: &CursorKey) -> Result<Cursor, MemoryError> {
        self.inner
            .with_state(|s| s.cursors.get(key).cloned().unwrap_or_default())
    }

    async fn reset_cursor(&self, key: &CursorKey, to: Cursor) -> Result<(), MemoryError> {
        self.inner.check()?;
        // Waits for an in-flight apply, like an UPDATE waits for a row lock.
        let _row = self.inner.lock_cursor_row(key).await;
        self.inner.with_state(|s| {
            s.cursors.insert(key.clone(), to);
        })
    }

    async fn park(&self, tx: &mut MemoryTx, letter: &DeadLetter) -> Result<(), MemoryError> {
        self.own(tx)?;
        self.inner.check()?;
        tx.letters.push(LetterOp::Park(Box::new(letter.clone())));
        Ok(())
    }

    async fn is_parked(
        &self,
        tx: &mut MemoryTx,
        key: &CursorKey,
        aggregate: &str,
    ) -> Result<bool, MemoryError> {
        self.own(tx)?;
        let tx = &*tx;
        self.inner
            .with_state(|s| tx.is_parked_view(s, key, aggregate))
    }

    async fn parked(
        &self,
        key: &CursorKey,
        filter: &ParkedFilter,
    ) -> Result<Vec<DeadLetter>, MemoryError> {
        self.inner.with_state(|s| {
            s.dead_letters
                .range((key.clone(), Seq::ZERO)..=(key.clone(), Seq(u64::MAX)))
                .map(|(_, l)| l)
                .filter(|l| filter.matches(l))
                .take(filter.limit.map_or(usize::MAX, |l| l as usize))
                .cloned()
                .collect()
        })
    }

    async fn resolve(
        &self,
        tx: &mut MemoryTx,
        key: &CursorKey,
        seq: Seq,
    ) -> Result<(), MemoryError> {
        self.own(tx)?;
        self.inner.check()?;
        tx.letters.push(LetterOp::Resolve(key.clone(), seq));
        Ok(())
    }
}
