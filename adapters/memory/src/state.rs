//! The committed state of one in-memory storage, and the shared internals.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use time::OffsetDateTime;
use tokio::sync::{OwnedMutexGuard, broadcast};
use z_event::{
    ConsumerState, Cursor, CursorKey, DeadLetter, Epoch, Event, NewEvent, Notification, Seq,
};

use crate::MemoryError;

/// A committed, sequenced event.
pub(crate) struct Sequenced {
    pub(crate) event: Event,
    pub(crate) sequenced_at: OffsetDateTime,
}

pub(crate) struct LeaseRow {
    pub(crate) token: u64,
    pub(crate) expires_at: tokio::time::Instant,
}

/// Everything that is committed. Only ever touched inside a synchronous
/// critical section, so a commit is atomic even if its task is aborted.
pub(crate) struct State {
    /// Committed events waiting for the sequencer, by insert id. Insert ids
    /// are taken at append time, so they are NOT in commit order: this is
    /// what makes the commit-order gap real in memory too.
    pub(crate) unsequenced: BTreeMap<u64, NewEvent>,
    pub(crate) feed: BTreeMap<Seq, Sequenced>,
    pub(crate) head: Seq,
    pub(crate) epoch: Epoch,
    pub(crate) purged_through: Seq,
    pub(crate) consumers: BTreeMap<String, ConsumerState>,
    pub(crate) cursors: HashMap<CursorKey, Cursor>,
    pub(crate) dead_letters: BTreeMap<(CursorKey, Seq), DeadLetter>,
    pub(crate) lease: Option<LeaseRow>,
    pub(crate) lease_counter: u64,
    pub(crate) kv: BTreeMap<String, Bytes>,
}

impl State {
    fn new() -> Self {
        State {
            unsequenced: BTreeMap::new(),
            feed: BTreeMap::new(),
            head: Seq::ZERO,
            epoch: Epoch::generate(),
            purged_through: Seq::ZERO,
            consumers: BTreeMap::new(),
            cursors: HashMap::new(),
            dead_letters: BTreeMap::new(),
            lease: None,
            lease_counter: 0,
            kv: BTreeMap::new(),
        }
    }
}

type LockMap<K> = Mutex<HashMap<K, Arc<tokio::sync::Mutex<()>>>>;

pub(crate) struct Inner {
    state: Mutex<State>,
    next_insert_id: AtomicU64,
    pub(crate) notify: broadcast::Sender<Notification>,
    cursor_locks: LockMap<CursorKey>,
    key_locks: LockMap<String>,
    pub(crate) lease_ttl: Duration,
    unavailable: AtomicBool,
}

impl Inner {
    pub(crate) fn new(lease_ttl: Duration, notify_capacity: usize) -> Self {
        let (notify, _) = broadcast::channel(notify_capacity);
        Inner {
            state: Mutex::new(State::new()),
            next_insert_id: AtomicU64::new(1),
            notify,
            cursor_locks: Mutex::new(HashMap::new()),
            key_locks: Mutex::new(HashMap::new()),
            lease_ttl,
            unavailable: AtomicBool::new(false),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs `f` on the committed state in one critical section. Never call
    /// it across an `.await`.
    pub(crate) fn with_state<R>(&self, f: impl FnOnce(&mut State) -> R) -> Result<R, MemoryError> {
        self.check()?;
        Ok(f(&mut self.lock_state()))
    }

    /// Like [`with_state`](Self::with_state), ignoring the unavailable switch
    /// (test inspection).
    pub(crate) fn peek<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        f(&mut self.lock_state())
    }

    pub(crate) fn check(&self) -> Result<(), MemoryError> {
        if self.unavailable.load(Ordering::SeqCst) {
            Err(MemoryError::Unavailable)
        } else {
            Ok(())
        }
    }

    pub(crate) fn set_unavailable(&self, on: bool) {
        self.unavailable.store(on, Ordering::SeqCst);
    }

    pub(crate) fn next_insert_id(&self) -> u64 {
        self.next_insert_id.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn publish(&self, n: Notification) {
        // No receivers is fine: hints are best effort.
        let _ = self.notify.send(n);
    }

    fn lock_of<K: std::hash::Hash + Eq + Clone>(
        map: &LockMap<K>,
        key: &K,
    ) -> Arc<tokio::sync::Mutex<()>> {
        map.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key.clone())
            .or_default()
            .clone()
    }

    pub(crate) async fn lock_cursor_row(&self, key: &CursorKey) -> OwnedMutexGuard<()> {
        Self::lock_of(&self.cursor_locks, key).lock_owned().await
    }

    pub(crate) async fn lock_key_row(&self, key: &str) -> OwnedMutexGuard<()> {
        Self::lock_of(&self.key_locks, &key.to_owned())
            .lock_owned()
            .await
    }
}
