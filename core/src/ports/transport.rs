//! The transport port: [`Transport`] (client side) and [`Subscription`].

use std::future::Future;
use std::sync::Arc;

use futures::stream::BoxStream;
use tokio::sync::watch;

use crate::error::TransportError;
use crate::types::{FeedHead, FeedMessage, Seq, SnapshotRequest, Start};

/// Opens subscriptions to producers. The server side of a transport exposes
/// the core [`FeedSource`](crate::FeedSource); a transport only maps its wire
/// format to the protocol messages.
///
/// ```
/// use futures::StreamExt;
/// use z_event::{FeedMessage, Seq, Start, Transport, TransportError};
///
/// /// Opens a subscription and reads the feed's epoch from the opening checkpoint.
/// async fn epoch_of<T: Transport>(transport: &T) -> Result<Option<String>, TransportError> {
///     let start = Start { consumer: "billing".into(), from_seq: Seq::ZERO, epoch: None, types: vec![] };
///     let mut sub = transport.subscribe("orders", start).await?;
///     Ok(match sub.messages.next().await {
///         Some(Ok(FeedMessage::Checkpoint(cp))) => Some(cp.epoch.to_string()),
///         _ => None,
///     })
/// }
/// ```
pub trait Transport: Send + Sync + 'static {
    /// Opens a subscription to `producer`.
    fn subscribe(
        &self,
        producer: &str,
        start: Start,
    ) -> impl Future<Output = Result<Subscription, TransportError>> + Send;

    /// Streams the producer's snapshot, ending with a checkpoint.
    fn snapshot(
        &self,
        producer: &str,
        request: SnapshotRequest,
    ) -> impl Future<Output = Result<MessageStream, TransportError>> + Send;

    /// The producer's head.
    fn head(&self, producer: &str)
    -> impl Future<Output = Result<FeedHead, TransportError>> + Send;
}

/// A stream of feed messages.
///
/// ```
/// use futures::StreamExt;
/// use z_event::{FeedMessage, MessageStream};
///
/// async fn first_event_seq(mut messages: MessageStream) -> Option<u64> {
///     while let Some(Ok(msg)) = messages.next().await {
///         if let FeedMessage::Event(e) = msg {
///             return Some(e.seq.get());
///         }
///     }
///     None
/// }
/// ```
pub type MessageStream = BoxStream<'static, Result<FeedMessage, TransportError>>;

/// An open subscription: messages from the producer and the ack channel back.
///
/// ```
/// use z_event::{Seq, Subscription};
///
/// fn applied_up_to(sub: &Subscription, seq: u64) {
///     sub.acks.send(Seq(seq)); // never blocks
/// }
/// ```
pub struct Subscription {
    /// Events and checkpoints, in order. Ends or errors when the stream
    /// breaks.
    pub messages: MessageStream,
    /// Sends acks to the producer.
    pub acks: AckSender,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription").finish_non_exhaustive()
    }
}

/// Sends acks. Latest value wins and sending never blocks: only the highest
/// applied position matters.
///
/// ```
/// use z_event::{AckSender, Seq};
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// let (acks, mut received) = AckSender::channel();
/// acks.send(Seq(5));
/// acks.send(Seq(3)); // ignored: acks never go back
/// assert_eq!(received.changed().await, Some(Seq(5)));
/// # });
/// ```
#[derive(Clone, Debug)]
pub struct AckSender(Arc<watch::Sender<Seq>>);

impl AckSender {
    /// A connected sender and receiver.
    pub fn channel() -> (AckSender, AckReceiver) {
        let (tx, rx) = watch::channel(Seq::ZERO);
        (AckSender(Arc::new(tx)), AckReceiver(rx))
    }

    /// Acks everything up to `applied`.
    pub fn send(&self, applied: Seq) {
        self.0.send_if_modified(|current| {
            if applied > *current {
                *current = applied;
                true
            } else {
                false
            }
        });
    }
}

/// Receives acks (transport side).
///
/// ```
/// use z_event::{AckReceiver, Seq};
///
/// /// A transport forwards acks until the subscriber goes away.
/// async fn forward(mut acks: AckReceiver, mut send: impl FnMut(Seq)) {
///     while let Some(applied) = acks.changed().await {
///         send(applied);
///     }
/// }
/// ```
#[derive(Debug)]
pub struct AckReceiver(watch::Receiver<Seq>);

impl AckReceiver {
    /// Waits for a new ack. `None` once every sender is gone.
    pub async fn changed(&mut self) -> Option<Seq> {
        self.0.changed().await.ok()?;
        Some(*self.0.borrow_and_update())
    }
}
