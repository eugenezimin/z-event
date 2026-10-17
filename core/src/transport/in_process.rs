//! [`InProcess`]: a transport over tokio channels, for running several
//! services in one process and for tests.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream;
use tokio::sync::mpsc;

use crate::error::{FeedError, TransportError};
use crate::feed::{FeedSource, FeedStream};
use crate::ports::{AckSender, MessageStream, OutboxStore, Subscription, Transport};
use crate::types::{Ack, FeedHead, PeerId, SnapshotRequest, Start};

/// Object-safe view of a [`FeedSource`], so sources over different stores
/// can share one registry.
trait ErasedFeed: Send + Sync {
    fn open(&self, peer: PeerId, start: Start)
    -> BoxFuture<'static, Result<FeedStream, FeedError>>;
    fn on_ack(&self, peer: PeerId, ack: Ack) -> BoxFuture<'static, Result<(), FeedError>>;
    fn head(&self) -> BoxFuture<'static, Result<FeedHead, FeedError>>;
    fn snapshot(
        &self,
        peer: PeerId,
        request: SnapshotRequest,
    ) -> BoxFuture<'static, Result<FeedStream, FeedError>>;
}

impl<S: OutboxStore> ErasedFeed for FeedSource<S> {
    fn open(
        &self,
        peer: PeerId,
        start: Start,
    ) -> BoxFuture<'static, Result<FeedStream, FeedError>> {
        let source = self.clone();
        Box::pin(async move { source.open(&peer, start).await })
    }

    fn on_ack(&self, peer: PeerId, ack: Ack) -> BoxFuture<'static, Result<(), FeedError>> {
        let source = self.clone();
        Box::pin(async move { source.on_ack(&peer, ack).await })
    }

    fn head(&self) -> BoxFuture<'static, Result<FeedHead, FeedError>> {
        let source = self.clone();
        Box::pin(async move { source.head().await })
    }

    fn snapshot(
        &self,
        peer: PeerId,
        request: SnapshotRequest,
    ) -> BoxFuture<'static, Result<FeedStream, FeedError>> {
        let source = self.clone();
        Box::pin(async move { source.snapshot(&peer, request).await })
    }
}

/// An in-process [`Transport`]: a registry of feed sources by producer name.
///
/// Each subscription runs its feed stream on its own task, connected to the
/// subscriber by a bounded channel, so a slow consumer applies backpressure
/// exactly as it would over a network.
///
/// The peer identity is the subscriber's consumer name (trusted, since both
/// sides run in one process), unless pinned with [`as_peer`](InProcess::as_peer).
///
/// ```
/// use z_event::{InProcess, Producer, ProducerConfig, AllowAll, OutboxStore};
///
/// fn wire<S: OutboxStore>(orders: &Producer<S>) -> InProcess {
///     let transport = InProcess::new();
///     transport.register("orders", orders.feed_source(AllowAll));
///     transport
/// }
/// ```
#[derive(Clone)]
pub struct InProcess {
    registry: Arc<RwLock<HashMap<String, Arc<dyn ErasedFeed>>>>,
    peer: Option<PeerId>,
    capacity: usize,
}

impl Default for InProcess {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for InProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcess")
            .field("peer", &self.peer)
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

impl InProcess {
    /// An empty registry with a channel capacity of 64 messages.
    pub fn new() -> Self {
        InProcess {
            registry: Arc::default(),
            peer: None,
            capacity: 64,
        }
    }

    /// Serves `source` under the name `producer`. Replaces an earlier one.
    pub fn register<S: OutboxStore>(&self, producer: impl Into<String>, source: FeedSource<S>) {
        self.registry
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(producer.into(), Arc::new(source));
    }

    /// Stops serving `producer` (new subscriptions fail with
    /// [`TransportError::UnknownProducer`]).
    pub fn unregister(&self, producer: &str) {
        self.registry
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(producer);
    }

    /// A handle on the same registry that authenticates as `peer`.
    pub fn as_peer(&self, peer: impl Into<PeerId>) -> Self {
        InProcess {
            peer: Some(peer.into()),
            ..self.clone()
        }
    }

    /// Channel capacity per subscription (default 64).
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity.max(1);
        self
    }

    fn lookup(&self, producer: &str) -> Result<Arc<dyn ErasedFeed>, TransportError> {
        self.registry
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(producer)
            .cloned()
            .ok_or_else(|| TransportError::UnknownProducer(producer.to_owned()))
    }

    fn peer_or(&self, consumer: &str) -> PeerId {
        self.peer.clone().unwrap_or_else(|| PeerId::from(consumer))
    }
}

fn channel_stream<T: Send + 'static>(rx: mpsc::Receiver<T>) -> stream::BoxStream<'static, T> {
    Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

impl Transport for InProcess {
    async fn subscribe(
        &self,
        producer: &str,
        start: Start,
    ) -> Result<Subscription, TransportError> {
        let feed = self.lookup(producer)?;
        let peer = self.peer_or(&start.consumer);
        let mut source = feed.open(peer.clone(), start).await?;

        let (tx, rx) = mpsc::channel(self.capacity);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tx.closed() => break,
                    item = source.next() => match item {
                        Some(item) => {
                            if tx.send(item.map_err(TransportError::from)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    },
                }
            }
        });

        let (acks, mut ack_rx) = AckSender::channel();
        tokio::spawn(async move {
            while let Some(applied_seq) = ack_rx.changed().await {
                if let Err(e) = feed.on_ack(peer.clone(), Ack { applied_seq }).await {
                    tracing::debug!(error = %e, "ack not recorded");
                }
            }
        });

        let messages: MessageStream = channel_stream(rx);
        Ok(Subscription { messages, acks })
    }

    async fn snapshot(
        &self,
        producer: &str,
        request: SnapshotRequest,
    ) -> Result<MessageStream, TransportError> {
        let feed = self.lookup(producer)?;
        let peer = self.peer_or(&request.consumer);
        let source = feed.snapshot(peer, request).await?;
        Ok(Box::pin(source.map(|m| m.map_err(TransportError::from))))
    }

    async fn head(&self, producer: &str) -> Result<FeedHead, TransportError> {
        Ok(self.lookup(producer)?.head().await?)
    }
}
