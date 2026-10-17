//! Helpers shared by the suites: configuration tuned for fast tests,
//! waiting, effect inspection, and [`World`] (a producer service and a
//! consumer service wired in process).

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use z_event::{
    AllowAll, Backoff, CursorKey, CursorStore, FeedConfig, Handler, InProcess, NewEvent,
    OutboxStore, Producer, ProducerConfig, ProducerHandle, RetryPolicy, Seq, StopReason,
    SubscriberBuilder, SubscriberError, SubscriberHandle, SubscriberStatus, Transport, TypeFilter,
};

use crate::factory::StoreFactory;
use crate::handlers::{EFFECT, EffectHandler};

/// Name of the producer service in [`World`].
pub const PRODUCER: &str = "orders";
/// Name of the consumer service in [`World`].
pub const CONSUMER: &str = "billing";

/// The random seed (`ZEVENT_TESTKIT_SEED`, or random), printed once.
pub fn seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| {
        let seed = std::env::var("ZEVENT_TESTKIT_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| fastrand::u64(..));
        eprintln!("z-event-testkit: ZEVENT_TESTKIT_SEED={seed}");
        seed
    })
}

/// A random generator seeded from [`seed`] and `salt`.
pub fn rng(salt: u64) -> fastrand::Rng {
    fastrand::Rng::with_seed(seed() ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// Iteration multiplier (`ZEVENT_TESTKIT_SCALE`, default 1).
pub fn scale() -> u32 {
    std::env::var("ZEVENT_TESTKIT_SCALE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1)
}

/// Producer settings for tests: 50 ms safety timer and lead retry, 200 ms
/// heartbeat, small backlog batches.
pub fn producer_config(name: &str) -> ProducerConfig {
    ProducerConfig::new(name)
        .safety_interval(Duration::from_millis(50))
        .lead_retry_interval(Duration::from_millis(50))
        .retention_interval(Duration::from_millis(200))
        .feed(heartbeat_config())
}

/// Feed settings for tests.
pub fn heartbeat_config() -> FeedConfig {
    FeedConfig {
        backlog_batch: 50,
        heartbeat: Duration::from_millis(200),
    }
}

/// Handler retries for tests: 3 attempts, 1–10 ms.
pub fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        backoff: Backoff::new(Duration::from_millis(1), Duration::from_millis(10)),
    }
}

/// Reconnect backoff for tests: 5–100 ms.
pub fn reconnect() -> Backoff {
    Backoff::new(Duration::from_millis(5), Duration::from_millis(100))
}

/// Waits until `cond` holds, panicking with `what` after `timeout`.
pub async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out after {timeout:?} waiting for {what} (seed {})",
                seed()
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Sequences everything committed in `store` (when no producer runs on it).
pub async fn drain_sequencer<S: OutboxStore>(store: &S) {
    loop {
        match store.try_lead().await {
            Ok(Some(lease)) => {
                while let Ok(Some(_)) = store.sequence_batch(&lease, 1000).await {}
                let _ = store.release(lease).await;
                return;
            }
            _ => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
}

/// Every sequenced event of `store` after `after`, in feed order.
pub async fn read_all<S: OutboxStore>(
    store: &S,
    after: Seq,
    types: &TypeFilter,
) -> Vec<z_event::Event> {
    let mut out = Vec::new();
    let mut pos = after;
    loop {
        let batch = store.read(pos, types, 500).await.unwrap();
        let Some(last) = batch.last() else {
            return out;
        };
        pos = last.seq;
        out.extend(batch);
    }
}

/// Source positions of the effects recorded by [`EffectHandler`] in
/// `store`, in the order they were committed.
pub async fn effects<S: OutboxStore>(store: &S) -> Vec<u64> {
    drain_sequencer(store).await;
    read_all(store, Seq::ZERO, &TypeFilter::only([EFFECT]))
        .await
        .iter()
        .map(|e| {
            e.aggregate
                .strip_prefix("effect/")
                .and_then(|s| s.parse().ok())
                .unwrap()
        })
        .collect()
}

/// Asserts that `effects` contains each of `expected` exactly once.
pub fn assert_exactly_once(effects: &[u64], expected: impl IntoIterator<Item = u64>) {
    let expected: BTreeSet<u64> = expected.into_iter().collect();
    let unique: BTreeSet<u64> = effects.iter().copied().collect();
    assert_eq!(
        unique.len(),
        effects.len(),
        "duplicate effects (seed {}): {effects:?}",
        seed()
    );
    let missing: Vec<_> = expected.difference(&unique).collect();
    let extra: Vec<_> = unique.difference(&expected).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "effects differ (seed {}): missing {missing:?}, unexpected {extra:?}",
        seed()
    );
}

/// Waits (up to 5 s) for a subscriber to stop by itself with `reason` and
/// returns its error.
pub async fn expect_stop(handle: SubscriberHandle, reason: StopReason) -> SubscriberError {
    let mut status = handle.status_watch();
    let result = tokio::time::timeout(Duration::from_secs(5), handle.join())
        .await
        .expect("subscriber stops by itself");
    assert_eq!(
        *status.borrow_and_update(),
        SubscriberStatus::Stopped(reason)
    );
    result.expect_err("subscriber stops with an error")
}

/// A producer service (`orders`) and a consumer service (`billing`), each
/// with a fresh store, wired through [`InProcess`].
///
/// ```
/// use z_event_testkit::{StoreFactory, World};
///
/// async fn smoke<F: StoreFactory>(factory: &F) {
///     let w = World::new(factory).await;
///     w.append_n(10, "OrderConfirmed", "orders").await;
///     let sub = w.subscriber(w.transport.clone(), w.effect_handler()).spawn();
///     w.wait_cursor(10).await;
///     sub.shutdown().await.unwrap();
///     z_event_testkit::util::assert_exactly_once(&w.effects().await, 1..=10);
///     w.shutdown().await;
/// }
/// ```
pub struct World<S: OutboxStore + CursorStore + Clone> {
    /// The producer's storage.
    pub orders: S,
    /// The consumer's storage.
    pub billing: S,
    /// The producer.
    pub producer: Producer<S>,
    /// The running producer tasks.
    pub running: Option<ProducerHandle>,
    /// The in-process transport serving `orders` with [`AllowAll`].
    pub transport: InProcess,
}

impl<S: OutboxStore + CursorStore + Clone> World<S> {
    /// Fresh stores, a started producer, `orders` registered.
    pub async fn new<F: StoreFactory<Store = S>>(factory: &F) -> Self {
        let orders = factory.fresh().await;
        let billing = factory.fresh().await;
        let producer = Producer::new(orders.clone(), producer_config(PRODUCER));
        let running = Some(producer.start());
        let transport = InProcess::new();
        transport.register(PRODUCER, producer.feed_source(AllowAll));
        World {
            orders,
            billing,
            producer,
            running,
            transport,
        }
    }

    /// The consumer's cursor key.
    pub fn key(&self) -> CursorKey {
        CursorKey::new(CONSUMER, PRODUCER)
    }

    /// Appends one event in its own transaction.
    pub async fn append(&self, event_type: &str, aggregate: &str) {
        let mut tx = self.orders.begin().await.unwrap();
        self.producer
            .append(&mut tx, NewEvent::new(event_type, aggregate, 1, Vec::new()))
            .await
            .ok()
            .unwrap();
        self.orders.commit(tx).await.unwrap();
    }

    /// Appends `n` events of `event_type` on aggregates `<prefix>/<i>`.
    pub async fn append_n(&self, n: u64, event_type: &str, prefix: &str) {
        for i in 0..n {
            self.append(event_type, &format!("{prefix}/{i}")).await;
        }
    }

    /// The effect recorder for the consumer.
    pub fn effect_handler(&self) -> EffectHandler<S> {
        EffectHandler::new(self.billing.clone())
    }

    /// A subscriber builder for `billing` on `orders` with fast settings.
    pub fn subscriber<T: Transport, H: Handler<S>>(
        &self,
        transport: T,
        handler: H,
    ) -> SubscriberBuilder<S, T, H> {
        z_event::Subscriber::builder(self.billing.clone(), transport, handler)
            .consumer(CONSUMER)
            .producer(PRODUCER)
            .retry(retry())
            .reconnect(reconnect())
            .ack_interval(Duration::from_millis(50))
    }

    /// The consumer's committed cursor position.
    pub async fn cursor(&self) -> Seq {
        self.billing.cursor(&self.key()).await.unwrap().seq
    }

    /// Waits until the consumer's cursor reaches `seq`.
    pub async fn wait_cursor(&self, seq: u64) {
        let key = self.key();
        let billing = self.billing.clone();
        wait_until(&format!("cursor {seq}"), Duration::from_secs(20), || {
            let billing = billing.clone();
            let key = key.clone();
            async move { billing.cursor(&key).await.unwrap().seq >= Seq(seq) }
        })
        .await;
    }

    /// Waits until the producer's head reaches `seq`.
    pub async fn wait_head(&self, seq: u64) {
        let orders = self.orders.clone();
        wait_until(&format!("head {seq}"), Duration::from_secs(20), || {
            let orders = orders.clone();
            async move { orders.head().await.unwrap().seq >= Seq(seq) }
        })
        .await;
    }

    /// The consumer's recorded effects.
    pub async fn effects(&self) -> Vec<u64> {
        effects(&self.billing).await
    }

    /// Stops the producer.
    pub async fn shutdown(mut self) {
        if let Some(running) = self.running.take() {
            running.shutdown().await;
        }
    }
}
