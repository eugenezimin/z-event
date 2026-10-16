//! The consumer side: [`Subscriber`]. Subscribes to one producer from the
//! stored cursor, applies each event in the same transaction as the cursor
//! update, retries and parks failing events per aggregate, and reconnects
//! with backoff.

mod apply;
mod policy;

use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

pub use self::apply::ReplayReport;
pub use self::policy::{Backoff, RetryPolicy};
use crate::error::{SubscriberError, TransportError};
use crate::ports::{CursorStore, Handler, Subscription, Transport};
use crate::telemetry;
use crate::types::{Cursor, CursorKey, DeadLetter, Epoch, FeedMessage, ParkedFilter, Seq, Start};

/// What a running subscriber is doing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriberStatus {
    /// Opening a subscription.
    Connecting,
    /// Receiving events. `cursor` is the last committed position, `head`
    /// the producer's head at the last checkpoint.
    Streaming {
        /// Last committed position.
        cursor: Seq,
        /// Producer head at the last checkpoint.
        head: Seq,
    },
    /// Waiting before reconnect number `attempt`.
    Backoff {
        /// Consecutive failed attempts.
        attempt: u32,
    },
    /// Stopped for good.
    Stopped(StopReason),
}

/// Why a subscriber stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopReason {
    /// Shut down on request.
    Shutdown,
    /// The producer's epoch differs from the cursor's (restore from backup).
    EpochMismatch,
    /// The cursor is below the producer's retention window.
    BeyondRetention,
    /// The producer refused this consumer.
    PermissionDenied,
    /// The producer rejected the subscription for another reason.
    Rejected,
    /// The task ended abnormally.
    Failed,
}

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) key: CursorKey,
    pub(crate) types: Vec<String>,
    pub(crate) retry: RetryPolicy,
    pub(crate) reconnect: Backoff,
    pub(crate) ack_interval: Duration,
    pub(crate) batch_size: usize,
}

pub(crate) struct Inner<S, T, H> {
    pub(crate) store: S,
    pub(crate) transport: T,
    pub(crate) handler: H,
    pub(crate) cfg: Config,
    pub(crate) reset: Notify,
    pub(crate) labels: Vec<metrics::Label>,
}

/// Builds a [`Subscriber`]. `consumer` and `producer` are required.
pub struct SubscriberBuilder<S, T, H> {
    store: S,
    transport: T,
    handler: H,
    consumer: Option<String>,
    producer: Option<String>,
    types: Vec<String>,
    retry: RetryPolicy,
    reconnect: Backoff,
    ack_interval: Duration,
    batch_size: usize,
}

impl<S, T, H> SubscriberBuilder<S, T, H>
where
    S: CursorStore,
    T: Transport,
    H: Handler<S>,
{
    /// This consumer's name (must match its transport identity).
    pub fn consumer(mut self, consumer: impl Into<String>) -> Self {
        self.consumer = Some(consumer.into());
        self
    }

    /// The producer to subscribe to.
    pub fn producer(mut self, producer: impl Into<String>) -> Self {
        self.producer = Some(producer.into());
        self
    }

    /// Event types to receive. Default: every type the producer's ACL
    /// allows.
    pub fn types<I, X>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = X>,
        X: Into<String>,
    {
        self.types = types.into_iter().map(Into::into).collect();
        self
    }

    /// Handler retries before parking (default 5 attempts, 0.1–30 s).
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Reconnect backoff (default 0.1–30 s with jitter).
    pub fn reconnect(mut self, backoff: Backoff) -> Self {
        self.reconnect = backoff;
        self
    }

    /// How often acks are sent (default 5 s).
    pub fn ack_interval(mut self, interval: Duration) -> Self {
        self.ack_interval = interval;
        self
    }

    /// Events applied per transaction when they are already buffered
    /// (default 1). Larger batches speed up projections; on any failure the
    /// batch is re-applied one event at a time.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n.max(1);
        self
    }

    /// Builds the subscriber.
    ///
    /// # Panics
    ///
    /// If `consumer` or `producer` was not set.
    pub fn build(self) -> Subscriber<S, T, H> {
        let (Some(consumer), Some(producer)) = (self.consumer, self.producer) else {
            panic!("Subscriber needs .consumer(..) and .producer(..)");
        };
        let labels = telemetry::pair_labels(&producer, &consumer);
        Subscriber {
            inner: Arc::new(Inner {
                store: self.store,
                transport: self.transport,
                handler: self.handler,
                cfg: Config {
                    key: CursorKey::new(consumer, producer),
                    types: self.types,
                    retry: self.retry,
                    reconnect: self.reconnect,
                    ack_interval: self.ack_interval,
                    batch_size: self.batch_size,
                },
                reset: Notify::new(),
                labels,
            }),
        }
    }

    /// Builds and spawns the subscriber.
    ///
    /// # Panics
    ///
    /// If `consumer` or `producer` was not set.
    pub fn spawn(self) -> SubscriberHandle {
        self.build().spawn()
    }
}

/// Consumes one producer's feed with one [`Handler`].
///
/// ```
/// use z_event::{CursorStore, Event, Handler, HandlerError, Subscriber, Transport};
///
/// struct CreateInvoice;
/// impl<S: CursorStore> Handler<S> for CreateInvoice {
///     async fn handle(&self, _tx: &mut S::Tx, _ev: &Event) -> Result<(), HandlerError> {
///         Ok(()) // business writes on the same transaction
///     }
/// }
///
/// fn run<S: CursorStore, T: Transport>(store: S, transport: T) {
///     let _handle = Subscriber::builder(store, transport, CreateInvoice)
///         .consumer("billing")
///         .producer("orders")
///         .types(["OrderConfirmed"])
///         .spawn();
/// }
/// ```
pub struct Subscriber<S, T, H> {
    inner: Arc<Inner<S, T, H>>,
}

impl<S, T, H> Clone for Subscriber<S, T, H> {
    fn clone(&self) -> Self {
        Subscriber {
            inner: self.inner.clone(),
        }
    }
}

impl<S, T, H> std::fmt::Debug for Subscriber<S, T, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscriber")
            .field("key", &self.inner.cfg.key)
            .finish_non_exhaustive()
    }
}

impl<S, T, H> Subscriber<S, T, H>
where
    S: CursorStore,
    T: Transport,
    H: Handler<S>,
{
    /// Starts building a subscriber.
    pub fn builder(store: S, transport: T, handler: H) -> SubscriberBuilder<S, T, H> {
        SubscriberBuilder {
            store,
            transport,
            handler,
            consumer: None,
            producer: None,
            types: Vec::new(),
            retry: RetryPolicy::default(),
            reconnect: Backoff::default(),
            ack_interval: Duration::from_secs(5),
            batch_size: 1,
        }
    }

    /// Which consumer reads which producer.
    pub fn key(&self) -> &CursorKey {
        &self.inner.cfg.key
    }

    /// Spawns the subscription loop. Dropping the handle leaves it running;
    /// use [`SubscriberHandle::shutdown`] to stop it. Running the same
    /// consumer twice is safe (cursor lock and dedup) but redundant.
    pub fn spawn(&self) -> SubscriberHandle {
        let cancel = CancellationToken::new();
        let (status_tx, status_rx) = watch::channel(SubscriberStatus::Connecting);
        let key = &self.inner.cfg.key;
        let span = tracing::info_span!(
            "z_event.subscriber",
            producer = %key.producer,
            consumer = %key.consumer
        );
        let join = tokio::spawn(
            self.inner
                .clone()
                .run(cancel.clone(), status_tx)
                .instrument(span),
        );
        SubscriberHandle {
            cancel,
            join,
            status: status_rx,
        }
    }

    /// The committed cursor.
    pub async fn cursor(&self) -> Result<Cursor, SubscriberError> {
        self.inner
            .store
            .cursor(&self.inner.cfg.key)
            .await
            .map_err(store_error)
    }

    /// Moves the cursor (replay from a position, or after a rebuild). A
    /// running loop reconnects from the new position.
    pub async fn reset_cursor(&self, to: Cursor) -> Result<(), SubscriberError> {
        self.inner
            .store
            .reset_cursor(&self.inner.cfg.key, to)
            .await
            .map_err(store_error)?;
        self.inner.reset.notify_one();
        Ok(())
    }

    /// Parked events matching `filter`.
    pub async fn parked(&self, filter: ParkedFilter) -> Result<Vec<DeadLetter>, SubscriberError> {
        self.inner
            .store
            .parked(&self.inner.cfg.key, &filter)
            .await
            .map_err(store_error)
    }

    /// Re-runs the handler for unresolved parked events matching `filter`,
    /// in `seq` order, each in its own transaction. If an event fails
    /// again, it stays parked and the rest of its aggregate is not replayed
    /// (it must not overtake it).
    pub async fn replay_parked(
        &self,
        filter: ParkedFilter,
    ) -> Result<ReplayReport, SubscriberError> {
        self.inner.replay_parked(filter).await
    }
}

/// Controls a spawned [`Subscriber`].
#[derive(Debug)]
pub struct SubscriberHandle {
    cancel: CancellationToken,
    join: JoinHandle<Result<(), SubscriberError>>,
    status: watch::Receiver<SubscriberStatus>,
}

impl SubscriberHandle {
    /// The current status.
    pub fn status(&self) -> SubscriberStatus {
        self.status.borrow().clone()
    }

    /// Watches status changes.
    pub fn status_watch(&self) -> watch::Receiver<SubscriberStatus> {
        self.status.clone()
    }

    /// Whether the loop has ended.
    pub fn is_finished(&self) -> bool {
        self.join.is_finished()
    }

    /// Stops after the event in progress and sends a final ack.
    pub async fn shutdown(self) -> Result<(), SubscriberError> {
        self.cancel.cancel();
        self.join().await
    }

    /// Waits for the loop to end by itself (a fatal error) or after
    /// [`shutdown`](Self::shutdown) from another handle.
    pub async fn join(self) -> Result<(), SubscriberError> {
        match self.join.await {
            Ok(result) => result,
            Err(e) => Err(SubscriberError::Task(e.to_string())),
        }
    }

    /// Kills the loop at its current await point, like a crash. The open
    /// transaction is dropped and rolls back.
    pub fn abort(self) {
        self.join.abort();
    }

    /// Like [`abort`](Self::abort), and waits until the task is gone (its
    /// transaction dropped).
    pub async fn abort_and_wait(self) {
        self.join.abort();
        let _ = self.join.await;
    }
}

fn store_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> SubscriberError {
    SubscriberError::Store(Box::new(e))
}

/// How one connection ended.
pub(crate) enum Outcome {
    Shutdown,
    /// Reconnect after backoff.
    Retry(String),
    /// Reconnect now (the cursor was reset).
    Reconnect,
    Fatal(SubscriberError),
}

impl<S, T, H> Inner<S, T, H>
where
    S: CursorStore,
    T: Transport,
    H: Handler<S>,
{
    async fn run(
        self: Arc<Self>,
        cancel: CancellationToken,
        status: watch::Sender<SubscriberStatus>,
    ) -> Result<(), SubscriberError> {
        let mut attempt = 0u32;
        loop {
            if cancel.is_cancelled() {
                status.send_replace(SubscriberStatus::Stopped(StopReason::Shutdown));
                return Ok(());
            }
            status.send_replace(SubscriberStatus::Connecting);
            let outcome = self.connect(&cancel, &status, &mut attempt).await;
            match outcome {
                Outcome::Shutdown => {
                    status.send_replace(SubscriberStatus::Stopped(StopReason::Shutdown));
                    return Ok(());
                }
                Outcome::Fatal(err) => {
                    let reason = match &err {
                        SubscriberError::EpochMismatch { .. } => {
                            metrics::counter!(telemetry::EPOCH_MISMATCH_TOTAL, self.labels.clone())
                                .increment(1);
                            StopReason::EpochMismatch
                        }
                        SubscriberError::BeyondRetention { .. } => StopReason::BeyondRetention,
                        SubscriberError::PermissionDenied(_) => StopReason::PermissionDenied,
                        SubscriberError::Rejected(_) => StopReason::Rejected,
                        _ => StopReason::Failed,
                    };
                    tracing::error!(error = %err, "subscriber stopped");
                    status.send_replace(SubscriberStatus::Stopped(reason));
                    return Err(err);
                }
                Outcome::Reconnect => {}
                Outcome::Retry(reason) => {
                    attempt = attempt.saturating_add(1);
                    metrics::counter!(telemetry::STREAM_RECONNECTS_TOTAL, self.labels.clone())
                        .increment(1);
                    let delay = self.cfg.reconnect.delay(attempt);
                    tracing::debug!(%reason, attempt, ?delay, "reconnecting");
                    status.send_replace(SubscriberStatus::Backoff { attempt });
                    tokio::select! {
                        _ = cancel.cancelled() => {}
                        _ = tokio::time::sleep(delay) => {}
                        _ = self.reset.notified() => {}
                    }
                }
            }
        }
    }

    async fn connect(
        &self,
        cancel: &CancellationToken,
        status: &watch::Sender<SubscriberStatus>,
        attempt: &mut u32,
    ) -> Outcome {
        let cursor = match self.store.cursor(&self.cfg.key).await {
            Ok(c) => c,
            Err(e) => return Outcome::Retry(format!("reading cursor: {e}")),
        };
        let start = Start {
            consumer: self.cfg.key.consumer.clone(),
            from_seq: cursor.seq,
            epoch: cursor.epoch.clone(),
            types: self.cfg.types.clone(),
        };
        let subscribed = tokio::select! {
            _ = cancel.cancelled() => return Outcome::Shutdown,
            r = self.transport.subscribe(&self.cfg.key.producer, start) => r,
        };
        match subscribed {
            Ok(sub) => self.stream(sub, cursor, cancel, status, attempt).await,
            Err(e) => self.transport_failure(e, cursor.epoch),
        }
    }

    fn transport_failure(&self, e: TransportError, cursor_epoch: Option<Epoch>) -> Outcome {
        if e.is_retryable() {
            return Outcome::Retry(e.to_string());
        }
        Outcome::Fatal(match e {
            TransportError::EpochMismatch { producer_epoch, .. } => {
                SubscriberError::EpochMismatch {
                    producer_epoch,
                    cursor_epoch,
                }
            }
            TransportError::BeyondRetention {
                from_seq,
                purged_through,
            } => SubscriberError::BeyondRetention {
                from_seq,
                purged_through,
            },
            TransportError::PermissionDenied(s) => SubscriberError::PermissionDenied(s),
            other => SubscriberError::Rejected(other.to_string()),
        })
    }

    async fn stream(
        &self,
        sub: Subscription,
        cursor: Cursor,
        cancel: &CancellationToken,
        status: &watch::Sender<SubscriberStatus>,
        attempt: &mut u32,
    ) -> Outcome {
        let Subscription { messages, acks } = sub;
        let mut messages = messages.peekable();
        let mut committed = cursor.seq;
        let mut epoch = cursor.epoch;
        let mut acked = Seq::ZERO;
        let mut ack_tick = tokio::time::interval(self.cfg.ack_interval);
        ack_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut head = Seq::ZERO;

        loop {
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    acks.send(committed);
                    return Outcome::Shutdown;
                }
                _ = self.reset.notified() => return Outcome::Reconnect,
                _ = ack_tick.tick() => {
                    if committed > acked {
                        acks.send(committed);
                        acked = committed;
                    }
                    continue;
                }
                next = messages.next() => next,
            };
            let msg = match next {
                None => return Outcome::Retry("stream closed".into()),
                Some(Err(e)) => return self.transport_failure(e, epoch),
                Some(Ok(msg)) => msg,
            };
            *attempt = 0;

            match msg {
                FeedMessage::Checkpoint(cp) => {
                    if let Some(mine) = &epoch
                        && *mine != cp.epoch
                    {
                        return Outcome::Fatal(SubscriberError::EpochMismatch {
                            producer_epoch: Some(cp.epoch),
                            cursor_epoch: epoch,
                        });
                    }
                    if cp.seq > committed || epoch.is_none() {
                        if let Err(o) = self.advance_to(cp.seq, &cp.epoch, &mut committed).await {
                            return o;
                        }
                        epoch = Some(cp.epoch.clone());
                    }
                    head = head.max(cp.head);
                    metrics::gauge!(telemetry::CONSUMER_LAG_EVENTS, self.labels.clone())
                        .set(head.get().saturating_sub(committed.get()) as f64);
                    if committed >= head {
                        metrics::gauge!(telemetry::CONSUMER_LAG_SECONDS, self.labels.clone())
                            .set(0.0);
                    }
                }
                FeedMessage::Event(first) => {
                    if epoch.is_none() {
                        return Outcome::Retry("event before the opening checkpoint".into());
                    }
                    let mut batch = vec![first];
                    while batch.len() < self.cfg.batch_size {
                        let more = std::pin::Pin::new(&mut messages)
                            .next_if(|m| matches!(m, Ok(FeedMessage::Event(_))))
                            .now_or_never()
                            .flatten();
                        match more {
                            Some(Ok(FeedMessage::Event(e))) => batch.push(e),
                            _ => break,
                        }
                    }
                    if let Some(last) = batch.last() {
                        head = head.max(last.seq);
                    }
                    if let Err(o) = self.apply_batch(&batch, &mut committed, cancel).await {
                        return o;
                    }
                }
            }
            status.send_replace(SubscriberStatus::Streaming {
                cursor: committed,
                head,
            });
        }
    }

    /// Moves the cursor to a checkpoint (and records the epoch).
    async fn advance_to(
        &self,
        seq: Seq,
        epoch: &Epoch,
        committed: &mut Seq,
    ) -> Result<(), Outcome> {
        let retry = |e: S::Error| Outcome::Retry(format!("store: {e}"));
        let key = &self.cfg.key;
        let mut tx = self.store.begin().await.map_err(retry)?;
        let current = self.store.lock_cursor(&mut tx, key).await.map_err(retry)?;
        if current.seq < *committed {
            return Err(Outcome::Reconnect);
        }
        let to = Cursor {
            seq: current.seq.max(seq),
            epoch: Some(epoch.clone()),
        };
        let seq = to.seq;
        self.store.advance(&mut tx, key, to).await.map_err(retry)?;
        self.store.commit(tx).await.map_err(retry)?;
        *committed = seq;
        Ok(())
    }
}
