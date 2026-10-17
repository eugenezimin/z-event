//! The transport conformance suite.
//!
//! A transport adapter implements [`TransportHarness`] (serve a
//! [`FeedSource`] and return a client authenticating as a given peer) and
//! runs [`run_all`] or [`transport_suite!`](crate::transport_suite). The
//! core's in-process transport runs it through [`InProcessHarness`].

use std::any::Any;
use std::future::Future;
use std::time::Duration;

use futures::StreamExt;
use z_event::{
    Acl, AllowAll, Backoff, FeedMessage, FeedSource, InProcess, NewEvent, OutboxStore, PeerId, Seq,
    SnapshotRequest, Snapshotter, Start, StopReason, Store, SubscriberError, Transport,
    TransportError, TypeAcl,
};

use crate::factory::StoreFactory;
use crate::faults::{FlakyTransport, StreamCutter};
use crate::handlers::SlowHandler;
use crate::util::{CONSUMER, PRODUCER, World, assert_exactly_once, expect_stop, rng, wait_until};

/// Serves feed sources over a transport, for the suite.
pub trait TransportHarness: Send + Sync + 'static {
    /// The transport's client.
    type Client: Transport + Clone;

    /// Serves `source` as `producer` and returns a client that
    /// authenticates as `peer`, plus a guard that stops serving when
    /// dropped.
    fn serve<S: OutboxStore>(
        &self,
        producer: &str,
        source: FeedSource<S>,
        peer: PeerId,
    ) -> impl Future<Output = (Self::Client, Box<dyn Any + Send>)> + Send;
}

/// Runs the suite over the core's [`InProcess`] transport.
#[derive(Clone, Copy, Debug, Default)]
pub struct InProcessHarness;

impl TransportHarness for InProcessHarness {
    type Client = InProcess;

    async fn serve<S: OutboxStore>(
        &self,
        producer: &str,
        source: FeedSource<S>,
        peer: PeerId,
    ) -> (InProcess, Box<dyn Any + Send>) {
        let transport = InProcess::new();
        transport.register(producer, source);
        (transport.as_peer(peer), Box::new(()))
    }
}

async fn served<F: StoreFactory, T: TransportHarness>(
    f: &F,
    t: &T,
    acl: impl Acl,
    peer: &str,
) -> (World<F::Store>, T::Client, Box<dyn Any + Send>) {
    let w = World::new(f).await;
    let (client, guard) = t
        .serve(PRODUCER, w.producer.feed_source(acl), PeerId::from(peer))
        .await;
    (w, client, guard)
}

fn start(from: u64) -> Start {
    Start {
        consumer: CONSUMER.into(),
        from_seq: Seq(from),
        epoch: None,
        types: vec![],
    }
}

/// Streams are cut every few messages while events are appended
/// concurrently: every event is applied exactly once.
pub async fn network_drop_loop<F: StoreFactory, T: TransportHarness>(f: &F, t: &T) {
    let (w, client, _guard) = served(f, t, AllowAll, CONSUMER).await;
    let cutter = StreamCutter::new(client, 20, rng(10).u64(..));
    let sub = w.subscriber(cutter.clone(), w.effect_handler()).spawn();
    let n = 300;
    w.append_n(n, "OrderConfirmed", "orders").await;
    w.wait_cursor(n).await;
    sub.shutdown().await.unwrap();
    assert!(cutter.cuts() > 0, "streams were cut");
    assert_exactly_once(&w.effects().await, 1..=n);
    w.shutdown().await;
}

/// A peer may only subscribe under its own name.
pub async fn identity_enforced<F: StoreFactory, T: TransportHarness>(f: &F, t: &T) {
    let (w, client, _guard) = served(f, t, AllowAll, "mallory").await;
    let sub = w.subscriber(client, w.effect_handler()).spawn();
    let err = expect_stop(sub, StopReason::PermissionDenied).await;
    assert!(matches!(err, SubscriberError::PermissionDenied(_)), "{err}");
    w.shutdown().await;
}

/// Explicitly requested types must all be allowed; without explicit types
/// the consumer receives only what the ACL allows, and still reaches the
/// head.
pub async fn acl_enforced<F: StoreFactory, T: TransportHarness>(f: &F, t: &T) {
    let acl = TypeAcl::new().allow(CONSUMER, ["OrderConfirmed"]);
    let (w, client, _guard) = served(f, t, acl, CONSUMER).await;
    let denied = w
        .subscriber(client.clone(), w.effect_handler())
        .types(["OrderConfirmed", "OrderCancelled"])
        .spawn();
    let err = expect_stop(denied, StopReason::PermissionDenied).await;
    assert!(matches!(err, SubscriberError::PermissionDenied(_)), "{err}");

    w.append("OrderConfirmed", "orders/1").await;
    w.append("OrderCancelled", "orders/1").await;
    w.append("OrderConfirmed", "orders/2").await;
    let sub = w.subscriber(client, w.effect_handler()).spawn();
    w.wait_cursor(3).await;
    sub.shutdown().await.unwrap();
    assert_exactly_once(&w.effects().await, [1, 3]);
    w.shutdown().await;
}

/// A slow consumer falls behind without slowing the producer down, and
/// catches up exactly once.
pub async fn backpressure_slow_consumer<F: StoreFactory, T: TransportHarness>(f: &F, t: &T) {
    let (w, client, _guard) = served(f, t, AllowAll, CONSUMER).await;
    let handler = SlowHandler::new(w.effect_handler(), Duration::from_millis(10));
    let sub = w.subscriber(client, handler).spawn();
    let n = 200;
    let started = tokio::time::Instant::now();
    w.append_n(n, "OrderConfirmed", "orders").await;
    let append_time = started.elapsed();
    let lag = n - w.cursor().await.get();
    assert!(
        lag > 20,
        "the consumer should lag behind (lag {lag}, appends took {append_time:?})"
    );
    w.wait_cursor(n).await;
    sub.shutdown().await.unwrap();
    assert_exactly_once(&w.effects().await, 1..=n);
    w.shutdown().await;
}

/// An idle stream sends heartbeat checkpoints.
pub async fn heartbeats<F: StoreFactory, T: TransportHarness>(f: &F, t: &T) {
    let (w, client, _guard) = served(f, t, AllowAll, CONSUMER).await;
    let mut sub = client.subscribe(PRODUCER, start(0)).await.unwrap();
    let mut checkpoints = Vec::new();
    let _ = tokio::time::timeout(Duration::from_millis(1100), async {
        while let Some(m) = sub.messages.next().await {
            if let Ok(FeedMessage::Checkpoint(cp)) = m {
                checkpoints.push(cp.seq);
            }
        }
    })
    .await;
    assert!(
        checkpoints.len() >= 4,
        "expected an opening checkpoint and heartbeats every 200 ms, got {}",
        checkpoints.len()
    );
    assert!(checkpoints.windows(2).all(|p| p[0] <= p[1]));
    w.shutdown().await;
}

/// Failed connection attempts back off exponentially with jitter, then the
/// subscriber catches up.
pub async fn reconnect_with_backoff<F: StoreFactory, T: TransportHarness>(f: &F, t: &T) {
    let (w, client, _guard) = served(f, t, AllowAll, CONSUMER).await;
    let flaky = FlakyTransport::new(client, 4);
    w.append_n(5, "T", "x").await;
    let backoff = Backoff::new(Duration::from_millis(20), Duration::from_millis(200));
    let sub = w
        .subscriber(flaky.clone(), w.effect_handler())
        .reconnect(backoff)
        .spawn();
    w.wait_cursor(5).await;
    sub.shutdown().await.unwrap();
    let attempts = flaky.attempts();
    assert!(attempts.len() >= 5, "4 failures then a success");
    let gaps: Vec<Duration> = attempts.windows(2).map(|p| p[1] - p[0]).take(4).collect();
    assert!(
        gaps[0] >= Duration::from_millis(10),
        "jittered first delay: {gaps:?}"
    );
    assert!(
        gaps.iter().all(|g| *g < Duration::from_millis(400)),
        "capped: {gaps:?}"
    );
    assert!(gaps[3] > gaps[0], "exponential growth: {gaps:?}");
    assert_exactly_once(&w.effects().await, 1..=5);
    w.shutdown().await;
}

/// Acks reach the producer's consumer registry.
pub async fn acks_reach_producer<F: StoreFactory, T: TransportHarness>(f: &F, t: &T) {
    let (w, client, _guard) = served(f, t, AllowAll, CONSUMER).await;
    let sub = w.subscriber(client, w.effect_handler()).spawn();
    w.append_n(20, "T", "x").await;
    w.wait_cursor(20).await;
    let orders = w.orders.clone();
    wait_until("ack 20 recorded", Duration::from_secs(5), || {
        let orders = orders.clone();
        async move {
            orders
                .consumers()
                .await
                .unwrap()
                .iter()
                .any(|c| c.consumer == CONSUMER && c.acked_seq == Seq(20))
        }
    })
    .await;
    sub.shutdown().await.unwrap();
    w.shutdown().await;
}

/// Events arrive in strictly increasing `seq`; checkpoints never go back
/// and never precede an event they claim to cover.
pub async fn ordered_delivery<F: StoreFactory, T: TransportHarness>(f: &F, t: &T) {
    let (w, client, _guard) = served(f, t, AllowAll, CONSUMER).await;
    let mut sub = client.subscribe(PRODUCER, start(0)).await.unwrap();
    let writer = {
        let orders = w.orders.clone();
        let producer = w.producer.clone();
        tokio::spawn(async move {
            for i in 0..100 {
                let mut tx = orders.begin().await.unwrap();
                producer
                    .append(&mut tx, NewEvent::new("T", format!("x/{i}"), 1, Vec::new()))
                    .await
                    .unwrap();
                orders.commit(tx).await.unwrap();
            }
        })
    };
    let mut events = Vec::new();
    let mut covered = Seq::ZERO;
    tokio::time::timeout(Duration::from_secs(20), async {
        while events.len() < 100 {
            match sub.messages.next().await {
                Some(Ok(FeedMessage::Event(e))) => {
                    assert!(
                        e.seq > covered,
                        "event {} after checkpoint {covered}",
                        e.seq
                    );
                    events.push(e.seq.get());
                }
                Some(Ok(FeedMessage::Checkpoint(cp))) => {
                    assert!(cp.seq >= covered, "checkpoint went back");
                    covered = cp.seq;
                }
                other => panic!("stream ended: {other:?}"),
            }
        }
    })
    .await
    .expect("100 events");
    writer.await.unwrap();
    assert_eq!(events, (1..=100).collect::<Vec<_>>());
    w.shutdown().await;
}

struct StubSnapshotter;

impl<S: Store> Snapshotter<S> for StubSnapshotter {
    async fn snapshot(
        &self,
        _tx: &mut S::Tx,
        request: &SnapshotRequest,
    ) -> Result<Vec<NewEvent>, z_event::BoxError> {
        Ok(request
            .aggregate_types
            .iter()
            .map(|t| NewEvent::new(format!("{t}Snapshot"), format!("{t}/1"), 7, Vec::new()))
            .collect())
    }
}

/// `head` matches the producer; `snapshot` is refused without a
/// snapshotter and otherwise ends with a checkpoint at the head.
pub async fn head_and_snapshot<F: StoreFactory, T: TransportHarness>(f: &F, t: &T) {
    let (w, client, _guard) = served(f, t, AllowAll, CONSUMER).await;
    w.append_n(3, "T", "x").await;
    w.wait_head(3).await;
    let head = client.head(PRODUCER).await.unwrap();
    assert_eq!(head, w.orders.head().await.unwrap());

    let request = SnapshotRequest {
        consumer: CONSUMER.into(),
        aggregate_types: vec!["orders".into(), "customers".into()],
    };
    match client.snapshot(PRODUCER, request.clone()).await {
        Err(TransportError::Unsupported(_)) => {}
        Err(e) => panic!("expected Unsupported, got {e}"),
        Ok(_) => panic!("expected Unsupported, got a stream"),
    }

    let source = w
        .producer
        .feed_source(AllowAll)
        .with_snapshotter(StubSnapshotter);
    let (client, _guard2) = t.serve(PRODUCER, source, PeerId::from(CONSUMER)).await;
    let messages: Vec<_> = client
        .snapshot(PRODUCER, request)
        .await
        .unwrap()
        .map(|m| m.unwrap())
        .collect()
        .await;
    assert_eq!(messages.len(), 3);
    let types: Vec<_> = messages[..2]
        .iter()
        .map(|m| match m {
            FeedMessage::Event(e) => e.event_type.clone(),
            other => panic!("expected an event, got {other:?}"),
        })
        .collect();
    assert_eq!(types, ["ordersSnapshot", "customersSnapshot"]);
    match &messages[2] {
        FeedMessage::Checkpoint(cp) => assert_eq!(cp.seq, Seq(3)),
        other => panic!("expected the closing checkpoint, got {other:?}"),
    }
    w.shutdown().await;
}

/// Names of every transport case, in the order [`run_all`] runs them.
pub const CASES: &[&str] = &[
    "network_drop_loop",
    "identity_enforced",
    "acl_enforced",
    "backpressure_slow_consumer",
    "heartbeats",
    "reconnect_with_backoff",
    "acks_reach_producer",
    "ordered_delivery",
    "head_and_snapshot",
];

/// Runs every transport case. Needs a multi-threaded tokio runtime.
pub async fn run_all<F: StoreFactory, T: TransportHarness>(stores: F, harness: T) {
    let (f, t) = (&stores, &harness);
    network_drop_loop(f, t).await;
    identity_enforced(f, t).await;
    acl_enforced(f, t).await;
    backpressure_slow_consumer(f, t).await;
    heartbeats(f, t).await;
    reconnect_with_backoff(f, t).await;
    acks_reach_producer(f, t).await;
    ordered_delivery(f, t).await;
    head_and_snapshot(f, t).await;
}
