//! [`MemoryTx`]: a staged change set, applied atomically on commit.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use time::OffsetDateTime;
use tokio::sync::OwnedMutexGuard;
use z_event::{Cursor, CursorKey, DeadLetter, NewEvent, Notification, Seq};

use crate::state::{Inner, State};

pub(crate) enum LetterOp {
    Park(Box<DeadLetter>),
    Resolve(CursorKey, Seq),
}

/// A transaction on a [`MemoryStore`](crate::MemoryStore).
///
/// Writes are staged and become visible to others only on commit; dropping
/// the transaction rolls it back. Reads see the transaction's own writes.
/// Row locks (cursors, and host keys via [`lock`](MemoryTx::lock)) are held
/// until the transaction ends.
///
/// ```
/// # tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(async {
/// use z_event::Store;
/// use z_event_memory::MemoryStore;
/// let store = MemoryStore::new();
/// let mut tx = store.begin().await?;
/// tx.lock("orders/o1").await; // serialise changes to this order
/// tx.put("orders/o1", "confirmed");
/// assert_eq!(tx.get("orders/o1").as_deref(), Some(&b"confirmed"[..]));
/// assert_eq!(store.get("orders/o1"), None); // not committed yet
/// store.commit(tx).await?;
/// assert!(store.get("orders/o1").is_some());
/// # Ok::<(), z_event_memory::MemoryError>(())
/// # }).unwrap();
/// ```
pub struct MemoryTx {
    pub(crate) inner: Arc<Inner>,
    pub(crate) appended: Vec<(u64, NewEvent)>,
    pub(crate) cursor_writes: HashMap<CursorKey, Cursor>,
    pub(crate) letters: Vec<LetterOp>,
    pub(crate) kv_writes: BTreeMap<String, Option<Bytes>>,
    pub(crate) cursor_guards: HashMap<CursorKey, OwnedMutexGuard<()>>,
    pub(crate) key_guards: HashMap<String, OwnedMutexGuard<()>>,
}

impl std::fmt::Debug for MemoryTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryTx")
            .field("appended", &self.appended.len())
            .field("cursor_writes", &self.cursor_writes.len())
            .field("letters", &self.letters.len())
            .field("kv_writes", &self.kv_writes.len())
            .finish_non_exhaustive()
    }
}

impl MemoryTx {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        MemoryTx {
            inner,
            appended: Vec::new(),
            cursor_writes: HashMap::new(),
            letters: Vec::new(),
            kv_writes: BTreeMap::new(),
            cursor_guards: HashMap::new(),
            key_guards: HashMap::new(),
        }
    }

    /// Reads a host value, seeing this transaction's own writes.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        match self.kv_writes.get(key) {
            Some(staged) => staged.clone(),
            None => self.inner.peek(|s| s.kv.get(key).cloned()),
        }
    }

    /// Writes a host value.
    pub fn put(&mut self, key: impl Into<String>, value: impl Into<Bytes>) {
        self.kv_writes.insert(key.into(), Some(value.into()));
    }

    /// Deletes a host value.
    pub fn delete(&mut self, key: impl Into<String>) {
        self.kv_writes.insert(key.into(), None);
    }

    /// Locks a host row until the transaction ends (like `SELECT … FOR
    /// UPDATE`). Serialises changes to one aggregate.
    pub async fn lock(&mut self, key: &str) {
        if !self.key_guards.contains_key(key) {
            let guard = self.inner.lock_key_row(key).await;
            self.key_guards.insert(key.to_owned(), guard);
        }
    }

    pub(crate) async fn ensure_cursor_lock(&mut self, key: &CursorKey) {
        if !self.cursor_guards.contains_key(key) {
            let guard = self.inner.lock_cursor_row(key).await;
            self.cursor_guards.insert(key.clone(), guard);
        }
    }

    /// Whether `aggregate` has an unresolved parked event, as this
    /// transaction sees it.
    pub(crate) fn is_parked_view(&self, state: &State, key: &CursorKey, aggregate: &str) -> bool {
        let mut view: BTreeMap<Seq, bool> = state
            .dead_letters
            .range((key.clone(), Seq::ZERO)..=(key.clone(), Seq(u64::MAX)))
            .filter(|(_, l)| l.aggregate() == aggregate)
            .map(|((_, seq), l)| (*seq, !l.is_resolved()))
            .collect();
        for op in &self.letters {
            match op {
                LetterOp::Park(l) if &l.key == key && l.aggregate() == aggregate => {
                    view.insert(l.seq(), true);
                }
                LetterOp::Resolve(k, seq) if k == key => {
                    if let Some(open) = view.get_mut(seq) {
                        *open = false;
                    }
                }
                _ => {}
            }
        }
        view.values().any(|open| *open)
    }

    /// Applies the staged changes to the committed state. Synchronous: the
    /// caller holds the state lock, so this is atomic.
    pub(crate) fn apply(&mut self, state: &mut State) -> bool {
        let appended = !self.appended.is_empty();
        for (id, ev) in self.appended.drain(..) {
            state.unsequenced.insert(id, ev);
        }
        for (key, cursor) in self.cursor_writes.drain() {
            state.cursors.insert(key, cursor);
        }
        let now = OffsetDateTime::now_utc();
        for op in self.letters.drain(..) {
            match op {
                LetterOp::Park(letter) => {
                    let mut letter = *letter;
                    letter.resolved_at = None;
                    state
                        .dead_letters
                        .insert((letter.key.clone(), letter.seq()), letter);
                }
                LetterOp::Resolve(key, seq) => {
                    if let Some(l) = state.dead_letters.get_mut(&(key, seq)) {
                        l.resolved_at = Some(now);
                    }
                }
            }
        }
        for (key, value) in std::mem::take(&mut self.kv_writes) {
            match value {
                Some(v) => state.kv.insert(key, v),
                None => state.kv.remove(&key),
            };
        }
        appended
    }

    pub(crate) fn notify_committed(&self, appended: bool) {
        if appended {
            self.inner.publish(Notification::Appended);
        }
    }
}
