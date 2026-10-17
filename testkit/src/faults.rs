//! Fault injectors: a handler that "crashes" at a planned point, and
//! transport wrappers that cut streams, resend history or refuse to
//! connect.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use futures::StreamExt;
use tokio::sync::Notify;
use tokio::time::Instant;
use z_event::{
    Event, FeedHead, Handler, HandlerError, MessageStream, SnapshotRequest, Start, Store,
    Subscription, Transport, TransportError,
};

/// Where a [`KillingHandler`] stops.
///
/// ```
/// use z_event_testkit::faults::KillPoint;
/// // AfterEffect: the handler's writes are in the transaction, the commit never happens.
/// assert_ne!(KillPoint::BeforeHandle, KillPoint::AfterEffect);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillPoint {
    /// Before the inner handler runs.
    BeforeHandle,
    /// After the inner handler wrote its effect, before the commit.
    AfterEffect,
}

/// Delegates until a planned event, then signals [`killed`](Self::killed)
/// and hangs forever, so the driver can abort the subscriber task right
/// there: the open transaction is dropped, exactly as in a crash.
///
/// ```
/// use z_event::{Handler, Store};
/// use z_event_testkit::faults::{KillPoint, KillingHandler};
///
/// fn crash_on_third<S: Store, H: Handler<S>>(handler: H) -> KillingHandler<H> {
///     // Writes the third event's effect, then hangs before the commit.
///     KillingHandler::new(handler, 2, KillPoint::AfterEffect)
/// }
/// ```
#[derive(Clone, Debug)]
pub struct KillingHandler<H> {
    inner: H,
    countdown: Arc<AtomicU32>,
    point: KillPoint,
    killed: Arc<Notify>,
}

impl<H> KillingHandler<H> {
    /// Kills at the `after`-th event from now (0 = the next one).
    pub fn new(inner: H, after: u32, point: KillPoint) -> Self {
        KillingHandler {
            inner,
            countdown: Arc::new(AtomicU32::new(after)),
            point,
            killed: Arc::new(Notify::new()),
        }
    }

    /// Notified when the kill point is reached.
    pub fn killed(&self) -> Arc<Notify> {
        self.killed.clone()
    }

    fn due(&self) -> bool {
        self.countdown
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_err()
    }
}

impl<S: Store, H: Handler<S>> Handler<S> for KillingHandler<H> {
    async fn handle(&self, tx: &mut S::Tx, ev: &Event) -> Result<(), HandlerError> {
        if !self.due() {
            return self.inner.handle(tx, ev).await;
        }
        if self.point == KillPoint::AfterEffect {
            self.inner.handle(tx, ev).await?;
        }
        self.killed.notify_one();
        std::future::pending().await
    }
}

/// Ends every subscription with [`TransportError::Closed`] after a random
/// number of messages in `[1, max_messages]`.
///
/// ```
/// use z_event::Transport;
/// use z_event_testkit::faults::StreamCutter;
///
/// fn flaky_network<T: Transport>(transport: T) -> StreamCutter<T> {
///     StreamCutter::new(transport, 20, 42) // cut after 1-20 messages, seed 42
/// }
/// ```
#[derive(Clone, Debug)]
pub struct StreamCutter<T> {
    inner: T,
    max_messages: u32,
    cuts: Arc<AtomicU64>,
    seed: u64,
}

impl<T> StreamCutter<T> {
    /// Wraps `inner`.
    pub fn new(inner: T, max_messages: u32, seed: u64) -> Self {
        StreamCutter {
            inner,
            max_messages: max_messages.max(1),
            cuts: Arc::default(),
            seed,
        }
    }

    /// How many streams were cut.
    pub fn cuts(&self) -> u64 {
        self.cuts.load(Ordering::SeqCst)
    }
}

impl<T: Transport> Transport for StreamCutter<T> {
    async fn subscribe(
        &self,
        producer: &str,
        start: Start,
    ) -> Result<Subscription, TransportError> {
        let sub = self.inner.subscribe(producer, start).await?;
        let n = self.cuts.fetch_add(0, Ordering::SeqCst);
        let mut rng = fastrand::Rng::with_seed(self.seed.wrapping_add(n.wrapping_mul(7919)));
        let limit = rng.u32(1..=self.max_messages) as usize;
        let cuts = self.cuts.clone();
        let cut: MessageStream = Box::pin(sub.messages.take(limit).chain(futures::stream::once(
            async move {
                cuts.fetch_add(1, Ordering::SeqCst);
                Err(TransportError::Closed)
            },
        )));
        Ok(Subscription {
            messages: cut,
            acks: sub.acks,
        })
    }

    async fn snapshot(
        &self,
        producer: &str,
        request: SnapshotRequest,
    ) -> Result<MessageStream, TransportError> {
        self.inner.snapshot(producer, request).await
    }

    async fn head(&self, producer: &str) -> Result<FeedHead, TransportError> {
        self.inner.head(producer).await
    }
}

/// Subscribes `rewind` events before the requested position, so the
/// producer resends events the consumer already applied.
///
/// ```
/// use z_event::Transport;
/// use z_event_testkit::faults::ReplayingTransport;
///
/// fn with_resends<T: Transport>(transport: T) -> ReplayingTransport<T> {
///     ReplayingTransport::new(transport, 10) // resend the last 10 events on every subscribe
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ReplayingTransport<T> {
    inner: T,
    rewind: u64,
}

impl<T> ReplayingTransport<T> {
    /// Wraps `inner`.
    pub fn new(inner: T, rewind: u64) -> Self {
        ReplayingTransport { inner, rewind }
    }
}

impl<T: Transport> Transport for ReplayingTransport<T> {
    async fn subscribe(
        &self,
        producer: &str,
        mut start: Start,
    ) -> Result<Subscription, TransportError> {
        start.from_seq = z_event::Seq(start.from_seq.get().saturating_sub(self.rewind));
        self.inner.subscribe(producer, start).await
    }

    async fn snapshot(
        &self,
        producer: &str,
        request: SnapshotRequest,
    ) -> Result<MessageStream, TransportError> {
        self.inner.snapshot(producer, request).await
    }

    async fn head(&self, producer: &str) -> Result<FeedHead, TransportError> {
        self.inner.head(producer).await
    }
}

/// Fails the first `failures` subscribe attempts with
/// [`TransportError::Unavailable`] and records when every attempt happened.
///
/// ```
/// use z_event::Transport;
/// use z_event_testkit::faults::FlakyTransport;
///
/// fn producer_down_at_first<T: Transport>(transport: T) -> FlakyTransport<T> {
///     FlakyTransport::new(transport, 4) // the first 4 subscribes fail
/// }
/// ```
#[derive(Clone, Debug)]
pub struct FlakyTransport<T> {
    inner: T,
    failures: u32,
    attempts: Arc<Mutex<Vec<Instant>>>,
}

impl<T> FlakyTransport<T> {
    /// Wraps `inner`.
    pub fn new(inner: T, failures: u32) -> Self {
        FlakyTransport {
            inner,
            failures,
            attempts: Arc::default(),
        }
    }

    /// When each subscribe attempt happened.
    pub fn attempts(&self) -> Vec<Instant> {
        self.attempts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl<T: Transport> Transport for FlakyTransport<T> {
    async fn subscribe(
        &self,
        producer: &str,
        start: Start,
    ) -> Result<Subscription, TransportError> {
        let n = {
            let mut attempts = self.attempts.lock().unwrap_or_else(PoisonError::into_inner);
            attempts.push(Instant::now());
            attempts.len() as u32
        };
        if n <= self.failures {
            return Err(TransportError::Unavailable("injected failure".into()));
        }
        self.inner.subscribe(producer, start).await
    }

    async fn snapshot(
        &self,
        producer: &str,
        request: SnapshotRequest,
    ) -> Result<MessageStream, TransportError> {
        self.inner.snapshot(producer, request).await
    }

    async fn head(&self, producer: &str) -> Result<FeedHead, TransportError> {
        self.inner.head(producer).await
    }
}

/// When a [`CrashingStore`] crashes relative to the planned operation.
///
/// ```
/// use z_event_testkit::faults::CrashTiming;
/// // After: the operation (e.g. a commit) completed, nothing after it runs.
/// assert_ne!(CrashTiming::Before, CrashTiming::After);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashTiming {
    /// Before the operation reaches the store.
    Before,
    /// After the operation completed (e.g. right after a commit).
    After,
}

/// Wraps a store and "crashes" at its N-th operation: it signals
/// [`crashed`](Self::crashed) and hangs, so the driver can abort the task
/// using it at exactly that point. With [`CrashTiming::After`] on a
/// commit, the commit is durable but nothing after it runs — the point
/// where splitting effect and cursor into two transactions would show.
///
/// ```
/// use z_event::{CursorStore, OutboxStore};
/// use z_event_testkit::faults::{CrashTiming, CrashingStore};
///
/// fn crash_after_fifth_operation<S: OutboxStore + CursorStore + Clone>(store: S) -> CrashingStore<S> {
///     CrashingStore::new(store, 4, CrashTiming::After)
/// }
/// ```
pub struct CrashingStore<S> {
    inner: S,
    countdown: Arc<AtomicU32>,
    timing: CrashTiming,
    crashed: Arc<Notify>,
}

impl<S: Clone> Clone for CrashingStore<S> {
    fn clone(&self) -> Self {
        CrashingStore {
            inner: self.inner.clone(),
            countdown: self.countdown.clone(),
            timing: self.timing,
            crashed: self.crashed.clone(),
        }
    }
}

impl<S> std::fmt::Debug for CrashingStore<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrashingStore")
            .field("timing", &self.timing)
            .finish_non_exhaustive()
    }
}

impl<S> CrashingStore<S> {
    /// Crashes at operation number `after` (0 = the next one), `timing`
    /// relative to it.
    pub fn new(inner: S, after: u32, timing: CrashTiming) -> Self {
        CrashingStore {
            inner,
            countdown: Arc::new(AtomicU32::new(after)),
            timing,
            crashed: Arc::new(Notify::new()),
        }
    }

    /// Notified when the crash point is reached.
    pub fn crashed(&self) -> Arc<Notify> {
        self.crashed.clone()
    }

    async fn op<R>(&self, call: impl std::future::Future<Output = R>) -> R {
        // Counts down to 0, crashes once, then stays disarmed (u32::MAX).
        let due = self
            .countdown
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| match n {
                u32::MAX => None,
                0 => Some(u32::MAX),
                n => Some(n - 1),
            })
            == Ok(0);
        if due && self.timing == CrashTiming::Before {
            self.crashed.notify_one();
            std::future::pending::<()>().await;
        }
        let result = call.await;
        if due {
            self.crashed.notify_one();
            std::future::pending::<()>().await;
        }
        result
    }
}

impl<S: Store> Store for CrashingStore<S> {
    type Tx = S::Tx;
    type Error = S::Error;

    async fn begin(&self) -> Result<S::Tx, S::Error> {
        self.op(self.inner.begin()).await
    }

    async fn commit(&self, tx: S::Tx) -> Result<(), S::Error> {
        self.op(self.inner.commit(tx)).await
    }

    async fn rollback(&self, tx: S::Tx) -> Result<(), S::Error> {
        self.op(self.inner.rollback(tx)).await
    }
}

impl<S: z_event::OutboxStore> z_event::OutboxStore for CrashingStore<S> {
    type Lease = S::Lease;

    async fn append(&self, tx: &mut S::Tx, events: &[z_event::NewEvent]) -> Result<(), S::Error> {
        self.op(self.inner.append(tx, events)).await
    }
    async fn try_lead(&self) -> Result<Option<S::Lease>, S::Error> {
        self.inner.try_lead().await
    }
    async fn release(&self, lease: S::Lease) -> Result<(), S::Error> {
        self.inner.release(lease).await
    }
    async fn sequence_batch(
        &self,
        lease: &S::Lease,
        limit: u32,
    ) -> Result<Option<z_event::Seq>, S::Error> {
        self.inner.sequence_batch(lease, limit).await
    }
    async fn read(
        &self,
        after: z_event::Seq,
        types: &z_event::TypeFilter,
        limit: u32,
    ) -> Result<Vec<Event>, S::Error> {
        self.inner.read(after, types, limit).await
    }
    async fn head(&self) -> Result<FeedHead, S::Error> {
        self.inner.head().await
    }
    async fn rotate_epoch(&self) -> Result<z_event::Epoch, S::Error> {
        self.inner.rotate_epoch().await
    }
    fn notifications(&self) -> futures::stream::BoxStream<'static, z_event::Notification> {
        self.inner.notifications()
    }
    async fn unsequenced(&self) -> Result<u64, S::Error> {
        self.inner.unsequenced().await
    }
    async fn register_consumer(&self, consumer: &str, at: z_event::Seq) -> Result<(), S::Error> {
        self.inner.register_consumer(consumer, at).await
    }
    async fn ack(&self, consumer: &str, seq: z_event::Seq) -> Result<(), S::Error> {
        self.inner.ack(consumer, seq).await
    }
    async fn consumers(&self) -> Result<Vec<z_event::ConsumerState>, S::Error> {
        self.inner.consumers().await
    }
    async fn remove_consumer(&self, consumer: &str) -> Result<(), S::Error> {
        self.inner.remove_consumer(consumer).await
    }
    async fn purge(&self, before: time::OffsetDateTime) -> Result<u64, S::Error> {
        self.inner.purge(before).await
    }
}

impl<S: z_event::CursorStore> z_event::CursorStore for CrashingStore<S> {
    async fn lock_cursor(
        &self,
        tx: &mut S::Tx,
        key: &z_event::CursorKey,
    ) -> Result<z_event::Cursor, S::Error> {
        self.op(self.inner.lock_cursor(tx, key)).await
    }
    async fn advance(
        &self,
        tx: &mut S::Tx,
        key: &z_event::CursorKey,
        to: z_event::Cursor,
    ) -> Result<(), S::Error> {
        self.op(self.inner.advance(tx, key, to)).await
    }
    async fn cursor(&self, key: &z_event::CursorKey) -> Result<z_event::Cursor, S::Error> {
        self.op(self.inner.cursor(key)).await
    }
    async fn reset_cursor(
        &self,
        key: &z_event::CursorKey,
        to: z_event::Cursor,
    ) -> Result<(), S::Error> {
        self.inner.reset_cursor(key, to).await
    }
    async fn park(&self, tx: &mut S::Tx, letter: &z_event::DeadLetter) -> Result<(), S::Error> {
        self.op(self.inner.park(tx, letter)).await
    }
    async fn is_parked(
        &self,
        tx: &mut S::Tx,
        key: &z_event::CursorKey,
        aggregate: &str,
    ) -> Result<bool, S::Error> {
        self.op(self.inner.is_parked(tx, key, aggregate)).await
    }
    async fn parked(
        &self,
        key: &z_event::CursorKey,
        filter: &z_event::ParkedFilter,
    ) -> Result<Vec<z_event::DeadLetter>, S::Error> {
        self.inner.parked(key, filter).await
    }
    async fn resolve(
        &self,
        tx: &mut S::Tx,
        key: &z_event::CursorKey,
        seq: z_event::Seq,
    ) -> Result<(), S::Error> {
        self.op(self.inner.resolve(tx, key, seq)).await
    }
}
