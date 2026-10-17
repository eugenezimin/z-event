//! The producer side: [`Producer`] (append, sequencer, retention) and its
//! configuration.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use time::OffsetDateTime;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::feed::FeedSource;
use crate::ports::{Acl, OutboxStore};
use crate::telemetry;
use crate::types::{NewEvent, Notification};

/// Feed stream settings.
#[derive(Clone, Debug)]
pub struct FeedConfig {
    /// Events per backlog read (default 500).
    pub backlog_batch: u32,
    /// Checkpoint interval while a stream is idle (default 15 s).
    pub heartbeat: Duration,
}

impl Default for FeedConfig {
    fn default() -> Self {
        FeedConfig {
            backlog_batch: 500,
            heartbeat: Duration::from_secs(15),
        }
    }
}

/// Producer settings. Build with [`ProducerConfig::new`] and the setters.
#[derive(Clone, Debug)]
pub struct ProducerConfig {
    pub(crate) name: String,
    pub(crate) sequencer_batch: u32,
    pub(crate) safety_interval: Duration,
    pub(crate) lead_retry_interval: Duration,
    pub(crate) retention: Duration,
    pub(crate) retention_interval: Duration,
    pub(crate) feed: FeedConfig,
}

impl ProducerConfig {
    /// Defaults for a producer called `name` (the metrics label and the
    /// name consumers subscribe to).
    pub fn new(name: impl Into<String>) -> Self {
        ProducerConfig {
            name: name.into(),
            sequencer_batch: 1000,
            safety_interval: Duration::from_secs(1),
            lead_retry_interval: Duration::from_secs(1),
            retention: Duration::from_secs(90 * 24 * 3600),
            retention_interval: Duration::from_secs(300),
            feed: FeedConfig::default(),
        }
    }

    /// Events stamped per sequencer statement (default 1000).
    pub fn sequencer_batch(mut self, n: u32) -> Self {
        self.sequencer_batch = n.max(1);
        self
    }

    /// The sequencer's safety timer, covering a missed notification
    /// (default 1 s). The storage adapter's lease must live at least three
    /// times as long, because the sequencer renews it on this timer.
    pub fn safety_interval(mut self, d: Duration) -> Self {
        self.safety_interval = d;
        self
    }

    /// How often a non-leader replica tries to take the lease (default 1 s).
    pub fn lead_retry_interval(mut self, d: Duration) -> Self {
        self.lead_retry_interval = d;
        self
    }

    /// How long acked events are kept (default 90 days).
    pub fn retention(mut self, d: Duration) -> Self {
        self.retention = d;
        self
    }

    /// How often retention runs and gauges refresh (default 5 min).
    pub fn retention_interval(mut self, d: Duration) -> Self {
        self.retention_interval = d;
        self
    }

    /// Feed stream settings.
    pub fn feed(mut self, feed: FeedConfig) -> Self {
        self.feed = feed;
        self
    }

    /// The producer name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

pub(crate) struct ProducerInner<S> {
    pub(crate) store: S,
    pub(crate) cfg: ProducerConfig,
    pub(crate) hub: broadcast::Sender<Notification>,
}

/// The producer side of one service's feed.
///
/// - [`append`](Producer::append) writes events in the host's transaction;
/// - [`start`](Producer::start) runs the sequencer (one leader across
///   replicas) and retention;
/// - [`feed_source`](Producer::feed_source) serves the feed to a transport.
///
/// ```
/// use z_event::{NewEvent, OutboxStore, Producer, ProducerConfig};
///
/// async fn confirm<S: OutboxStore>(producer: &Producer<S>, order: &str) -> Result<(), S::Error> {
///     let store = producer.store();
///     let mut tx = store.begin().await?;
///     // … the host's business writes on `tx` …
///     producer
///         .append(&mut tx, NewEvent::new("OrderConfirmed", format!("orders/{order}"), 1, "{}"))
///         .await?;
///     store.commit(tx).await
/// }
/// ```
pub struct Producer<S: OutboxStore> {
    inner: Arc<ProducerInner<S>>,
}

impl<S: OutboxStore> Clone for Producer<S> {
    fn clone(&self) -> Self {
        Producer {
            inner: self.inner.clone(),
        }
    }
}

impl<S: OutboxStore> std::fmt::Debug for Producer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("name", &self.inner.cfg.name)
            .finish_non_exhaustive()
    }
}

impl<S: OutboxStore> Producer<S> {
    /// A producer on `store`. Starts no tasks; call
    /// [`start`](Producer::start).
    pub fn new(store: S, config: ProducerConfig) -> Self {
        let (hub, _) = broadcast::channel(1024);
        Producer {
            inner: Arc::new(ProducerInner {
                store,
                cfg: config,
                hub,
            }),
        }
    }

    /// The storage.
    pub fn store(&self) -> &S {
        &self.inner.store
    }

    /// The producer name.
    pub fn name(&self) -> &str {
        &self.inner.cfg.name
    }

    /// Appends one event in the host's transaction.
    pub async fn append(&self, tx: &mut S::Tx, event: NewEvent) -> Result<(), S::Error> {
        self.append_all(tx, [event]).await
    }

    /// Appends events in the host's transaction.
    pub async fn append_all(
        &self,
        tx: &mut S::Tx,
        events: impl IntoIterator<Item = NewEvent> + Send,
    ) -> Result<(), S::Error> {
        let events: Vec<NewEvent> = events.into_iter().collect();
        let span = match events.first() {
            Some(first) => tracing::debug_span!(
                "z_event.append",
                producer = %self.inner.cfg.name,
                count = events.len(),
                event_type = %first.event_type(),
                aggregate = %first.aggregate(),
                correlation_id = first.correlation_id().unwrap_or_default(),
            ),
            None => return Ok(()),
        };
        self.inner
            .store
            .append(tx, &events)
            .instrument(span)
            .await?;
        metrics::counter!(
            telemetry::APPENDED_TOTAL,
            telemetry::producer_labels(&self.inner.cfg.name)
        )
        .increment(events.len() as u64);
        Ok(())
    }

    /// A feed source enforcing `acl`, to mount on a transport.
    pub fn feed_source(&self, acl: impl Acl) -> FeedSource<S> {
        FeedSource::new(self.inner.clone(), Arc::new(acl))
    }

    /// Starts the background tasks: the notification hub, the sequencer
    /// (which takes the lease if no other replica holds it) and retention.
    /// Call it once per process. Dropping the handle leaves them running;
    /// use [`ProducerHandle::shutdown`] to stop them.
    pub fn start(&self) -> ProducerHandle {
        let cancel = CancellationToken::new();
        let (leader_tx, leader_rx) = watch::channel(false);
        let span = tracing::info_span!("z_event.producer", producer = %self.inner.cfg.name);
        let tasks = vec![
            tokio::spawn(pump(self.inner.clone(), cancel.clone()).instrument(span.clone())),
            tokio::spawn(
                sequencer(self.inner.clone(), cancel.clone(), leader_tx).instrument(span.clone()),
            ),
            tokio::spawn(
                retention(self.inner.clone(), cancel.clone(), leader_rx.clone()).instrument(span),
            ),
        ];
        ProducerHandle {
            cancel,
            tasks,
            leader: leader_rx,
        }
    }
}

/// Controls a started [`Producer`].
#[derive(Debug)]
pub struct ProducerHandle {
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    leader: watch::Receiver<bool>,
}

impl ProducerHandle {
    /// Stops the tasks and releases the sequencer lease.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }

    /// Kills the tasks without releasing the lease, like a crash. Another
    /// replica takes over when the lease expires.
    pub fn abort(mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }

    /// Like [`abort`](Self::abort), and waits until the tasks are gone.
    pub async fn abort_and_wait(mut self) {
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }

    /// Whether this replica currently leads the sequencer.
    pub fn is_leader(&self) -> bool {
        *self.leader.borrow()
    }

    /// Watches leadership changes.
    pub fn leader_watch(&self) -> watch::Receiver<bool> {
        self.leader.clone()
    }
}

/// Forwards the store's notifications to the hub, re-opening the stream if
/// it ends.
async fn pump<S: OutboxStore>(inner: Arc<ProducerInner<S>>, cancel: CancellationToken) {
    loop {
        let mut stream = inner.store.notifications();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                n = stream.next() => match n {
                    Some(n) => { let _ = inner.hub.send(n); }
                    None => break,
                },
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(inner.cfg.safety_interval) => {}
        }
    }
}

async fn sequencer<S: OutboxStore>(
    inner: Arc<ProducerInner<S>>,
    cancel: CancellationToken,
    leader: watch::Sender<bool>,
) {
    let labels = telemetry::producer_labels(&inner.cfg.name);
    loop {
        let attempt = tokio::select! {
            _ = cancel.cancelled() => return,
            r = inner.store.try_lead() => r,
        };
        match attempt {
            Ok(Some(lease)) => {
                tracing::info!("sequencer lease acquired");
                leader.send_replace(true);
                metrics::gauge!(telemetry::SEQUENCER_LEADER, labels.clone()).set(1.0);
                lead(&inner, &cancel, lease).await;
                leader.send_replace(false);
                metrics::gauge!(telemetry::SEQUENCER_LEADER, labels.clone()).set(0.0);
                if cancel.is_cancelled() {
                    return;
                }
                tracing::info!("sequencer stepped down");
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, "sequencer could not try the lease"),
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(inner.cfg.lead_retry_interval) => {}
        }
    }
}

/// Runs while this replica holds the lease. Returns after releasing it
/// (shutdown) or after losing it.
async fn lead<S: OutboxStore>(
    inner: &ProducerInner<S>,
    cancel: &CancellationToken,
    lease: S::Lease,
) {
    let labels = telemetry::producer_labels(&inner.cfg.name);
    let mut rx = inner.hub.subscribe();
    loop {
        // Drain everything committed so far. Also renews the lease.
        loop {
            match inner
                .store
                .sequence_batch(&lease, inner.cfg.sequencer_batch)
                .await
            {
                Ok(Some(head)) => {
                    metrics::gauge!(telemetry::HEAD_SEQ, labels.clone()).set(head.get() as f64);
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "sequencing failed; giving the lease up");
                    let _ = inner.store.release(lease).await;
                    return;
                }
            }
        }
        // Wait for a commit, a lost hint, or the safety timer.
        let deadline = tokio::time::sleep(inner.cfg.safety_interval);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = inner.store.release(lease).await;
                    return;
                }
                n = rx.recv() => match n {
                    Ok(Notification::Appended | Notification::Lagged)
                    | Err(broadcast::error::RecvError::Lagged(_)) => break,
                    // Our own output: keep waiting.
                    Ok(Notification::Sequenced(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {
                        (&mut deadline).await;
                        break;
                    }
                },
                _ = &mut deadline => break,
            }
        }
    }
}

async fn retention<S: OutboxStore>(
    inner: Arc<ProducerInner<S>>,
    cancel: CancellationToken,
    leader: watch::Receiver<bool>,
) {
    let name = inner.cfg.name.clone();
    let labels = telemetry::producer_labels(&name);
    let mut tick = tokio::time::interval(inner.cfg.retention_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => {}
        }
        if let Ok(n) = inner.store.unsequenced().await {
            metrics::gauge!(telemetry::UNSEQUENCED_ROWS, labels.clone()).set(n as f64);
        }
        if let Ok(consumers) = inner.store.consumers().await {
            for c in consumers {
                metrics::gauge!(
                    telemetry::CONSUMER_ACKED_SEQ,
                    telemetry::pair_labels(&name, &c.consumer)
                )
                .set(c.acked_seq.get() as f64);
            }
        }
        if !*leader.borrow() {
            continue;
        }
        let before = OffsetDateTime::now_utc()
            .checked_sub(
                time::Duration::try_from(inner.cfg.retention).unwrap_or(time::Duration::MAX),
            )
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
        match inner.store.purge(before).await {
            Ok(0) => {}
            Ok(n) => {
                tracing::debug!(purged = n, "retention purged events");
                metrics::counter!(telemetry::PURGED_TOTAL, labels.clone()).increment(n);
            }
            Err(e) => tracing::warn!(error = %e, "retention failed"),
        }
    }
}
