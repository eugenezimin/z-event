//! The server side of the protocol: [`FeedSource`]. A transport adapter
//! maps its wire format to [`open`](FeedSource::open),
//! [`on_ack`](FeedSource::on_ack), [`head`](FeedSource::head) and
//! [`snapshot`](FeedSource::snapshot).

mod state;

use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};

use crate::error::{BoxError, FeedError};
use crate::ports::{Acl, OutboxStore, Snapshotter, Store};
use crate::producer::ProducerInner;
use crate::telemetry;
use crate::types::{
    Ack, Checkpoint, FeedHead, FeedMessage, NewEvent, PeerId, SnapshotRequest, Start,
};

/// A stream of feed messages, as produced by a [`FeedSource`].
///
/// ```
/// use futures::StreamExt;
/// use z_event::{FeedMessage, FeedStream};
///
/// async fn count_events(mut stream: FeedStream) -> usize {
///     let mut n = 0;
///     while let Some(Ok(FeedMessage::Event(_))) = stream.next().await {
///         n += 1;
///     }
///     n
/// }
/// ```
pub type FeedStream = BoxStream<'static, Result<FeedMessage, FeedError>>;

/// Serves one producer's feed: identity, ACL and epoch checks, then the
/// backlog, then live push. Cheap to clone.
///
/// ```
/// use futures::StreamExt;
/// use z_event::{Ack, FeedError, FeedSource, OutboxStore, PeerId, Seq, Start};
///
/// /// The core of a transport's server side: authenticate, open, forward.
/// async fn serve<S: OutboxStore>(source: FeedSource<S>, peer: PeerId, from: u64) -> Result<(), FeedError> {
///     let start = Start {
///         consumer: peer.as_str().to_owned(),
///         from_seq: Seq(from),
///         epoch: None,
///         types: vec![],
///     };
///     let mut stream = source.open(&peer, start).await?;
///     while let Some(message) = stream.next().await {
///         let _message = message?; // … encode onto the wire …
///     }
///     source.on_ack(&peer, Ack { applied_seq: Seq(from) }).await
/// }
/// ```
pub struct FeedSource<S: OutboxStore> {
    producer: Arc<ProducerInner<S>>,
    acl: Arc<dyn Acl>,
    snapshotter: Option<Arc<dyn ErasedSnapshotter<S>>>,
}

impl<S: OutboxStore> Clone for FeedSource<S> {
    fn clone(&self) -> Self {
        FeedSource {
            producer: self.producer.clone(),
            acl: self.acl.clone(),
            snapshotter: self.snapshotter.clone(),
        }
    }
}

impl<S: OutboxStore> std::fmt::Debug for FeedSource<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedSource")
            .field("producer", &self.producer.cfg.name)
            .finish_non_exhaustive()
    }
}

fn unavailable<E: std::error::Error + Send + Sync + 'static>(e: E) -> FeedError {
    FeedError::Unavailable(Box::new(e))
}

impl<S: OutboxStore> FeedSource<S> {
    pub(crate) fn new(producer: Arc<ProducerInner<S>>, acl: Arc<dyn Acl>) -> Self {
        FeedSource {
            producer,
            acl,
            snapshotter: None,
        }
    }

    /// Serves snapshots from `snapshotter`.
    pub fn with_snapshotter(mut self, snapshotter: impl Snapshotter<S>) -> Self {
        self.snapshotter = Some(Arc::new(snapshotter));
        self
    }

    /// The producer name.
    pub fn producer(&self) -> &str {
        &self.producer.cfg.name
    }

    /// Opens a stream for `peer`.
    ///
    /// Checks, in order: the consumer name equals the peer; the ACL allows
    /// every explicitly requested type; the epoch matches (a consumer
    /// ahead of the head in the current epoch means the producer went back
    /// in time: the epoch is rotated and the stream refused); the start is
    /// not below retention. Then the consumer is registered and the stream
    /// starts with a [`Checkpoint`] at `from_seq` carrying the epoch.
    pub async fn open(&self, peer: &PeerId, start: Start) -> Result<FeedStream, FeedError> {
        let store = &self.producer.store;
        if start.consumer.is_empty() {
            return Err(FeedError::InvalidRequest("empty consumer name".into()));
        }
        if start.consumer != peer.as_str() {
            return Err(FeedError::PermissionDenied {
                peer: peer.clone(),
                reason: format!("may not subscribe as {}", start.consumer),
            });
        }
        if let Some(denied) = start.types.iter().find(|t| !self.acl.allows(peer, t)) {
            return Err(FeedError::PermissionDenied {
                peer: peer.clone(),
                reason: format!("may not read {denied}"),
            });
        }

        // Subscribe to wake-ups before reading the head, so nothing
        // sequenced in between is missed.
        let rx = self.producer.hub.subscribe();
        let head = store.head().await.map_err(unavailable)?;

        match &start.epoch {
            Some(epoch) if *epoch != head.epoch => {
                return Err(FeedError::EpochMismatch {
                    producer_epoch: head.epoch,
                    consumer_epoch: start.epoch,
                });
            }
            Some(_) if start.from_seq > head.seq => {
                let rotated = store.rotate_epoch().await.map_err(unavailable)?;
                tracing::warn!(
                    consumer = %start.consumer,
                    from_seq = %start.from_seq,
                    head = %head.seq,
                    epoch = %rotated,
                    "consumer is ahead of the head: storage went back in time; epoch rotated"
                );
                metrics::counter!(
                    telemetry::EPOCH_ROTATIONS_TOTAL,
                    telemetry::producer_labels(self.producer())
                )
                .increment(1);
                return Err(FeedError::EpochMismatch {
                    producer_epoch: rotated,
                    consumer_epoch: start.epoch,
                });
            }
            None if start.from_seq > head.seq => {
                return Err(FeedError::EpochMismatch {
                    producer_epoch: head.epoch,
                    consumer_epoch: None,
                });
            }
            _ => {}
        }
        if start.from_seq < head.purged_through {
            return Err(FeedError::BeyondRetention {
                from_seq: start.from_seq,
                purged_through: head.purged_through,
            });
        }
        store
            .register_consumer(&start.consumer, start.from_seq)
            .await
            .map_err(unavailable)?;

        Ok(state::FeedState::start(self.clone(), peer.clone(), start, head, rx).into_stream())
    }

    /// Records an ack from `peer`'s stream.
    pub async fn on_ack(&self, peer: &PeerId, ack: Ack) -> Result<(), FeedError> {
        self.producer
            .store
            .ack(peer.as_str(), ack.applied_seq)
            .await
            .map_err(unavailable)
    }

    /// The feed's head.
    pub async fn head(&self) -> Result<FeedHead, FeedError> {
        self.producer.store.head().await.map_err(unavailable)
    }

    /// Streams the current state of the requested aggregates (from the
    /// configured [`Snapshotter`]) and ends with `Checkpoint(S)`, where `S`
    /// is the head read before the state. The snapshot events carry
    /// `seq = S`. Subscribe from `S` afterwards.
    pub async fn snapshot(
        &self,
        peer: &PeerId,
        request: SnapshotRequest,
    ) -> Result<FeedStream, FeedError> {
        if request.consumer != peer.as_str() {
            return Err(FeedError::PermissionDenied {
                peer: peer.clone(),
                reason: format!("may not request a snapshot as {}", request.consumer),
            });
        }
        let Some(snapshotter) = self.snapshotter.clone() else {
            return Err(FeedError::Unsupported("snapshots are not configured"));
        };
        let store = &self.producer.store;
        let head = store.head().await.map_err(unavailable)?;
        let mut tx = store.begin().await.map_err(unavailable)?;
        let events = snapshotter
            .snapshot_erased(&mut tx, &request)
            .await
            .map_err(FeedError::Unavailable)?;
        store.rollback(tx).await.map_err(unavailable)?;

        let peer = peer.clone();
        let acl = self.acl.clone();
        let seq = head.seq;
        let mut messages: Vec<Result<FeedMessage, FeedError>> = events
            .into_iter()
            .filter(|e| acl.allows(&peer, e.event_type()))
            .map(|e| Ok(FeedMessage::Event(e.into_event(seq))))
            .collect();
        messages.push(Ok(FeedMessage::Checkpoint(Checkpoint {
            seq,
            head: seq,
            epoch: head.epoch,
        })));
        Ok(Box::pin(stream::iter(messages)))
    }
}

/// Object-safe wrapper over [`Snapshotter`].
trait ErasedSnapshotter<S: Store>: Send + Sync {
    fn snapshot_erased<'a>(
        &'a self,
        tx: &'a mut S::Tx,
        request: &'a SnapshotRequest,
    ) -> BoxFuture<'a, Result<Vec<NewEvent>, BoxError>>;
}

impl<S: Store, T: Snapshotter<S>> ErasedSnapshotter<S> for T {
    fn snapshot_erased<'a>(
        &'a self,
        tx: &'a mut S::Tx,
        request: &'a SnapshotRequest,
    ) -> BoxFuture<'a, Result<Vec<NewEvent>, BoxError>> {
        Box::pin(self.snapshot(tx, request))
    }
}
