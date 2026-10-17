//! The client side: [`Client`], a [`Transport`] on a tonic channel.

use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, stream};
use tokio::sync::mpsc;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use z_event::{
    AckSender, FeedHead, FeedMessage, MessageStream, SnapshotRequest, Start, Subscription,
    Transport, TransportError,
};

use crate::ClientTls;
use crate::pb;
use crate::pb::event_feed_client::EventFeedClient;
use crate::status::from_status;

/// A gRPC [`Transport`] to one producer's feed service.
///
/// The `producer` argument of the [`Transport`] methods is not sent: a
/// client talks to one producer, the one its channel points at.
///
/// ```no_run
/// use tonic::transport::Endpoint;
/// use z_event_grpc::Client;
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// // On the host's own channel (its TLS, timeouts, load balancing):
/// let channel = Endpoint::from_static("https://orders:50051").connect().await?;
/// let orders = Client::new(channel).bearer_token("billing-token")?;
///
/// // Or turnkey:
/// let orders = Client::connect("http://orders:50051", None).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    inner: EventFeedClient<Channel>,
    metadata: Arc<Vec<(AsciiMetadataKey, AsciiMetadataValue)>>,
}

/// An invalid metadata key or value.
#[derive(Debug, thiserror::Error)]
#[error("invalid metadata: {0}")]
pub struct InvalidMetadata(String);

impl Client {
    /// A client on the host's channel.
    pub fn new(channel: Channel) -> Self {
        Client {
            inner: EventFeedClient::new(channel),
            metadata: Arc::default(),
        }
    }

    /// Connects to `url` (`http://…`, or `https://…` with `tls`), with HTTP/2
    /// keep-alive.
    pub async fn connect(
        url: impl Into<String>,
        tls: Option<ClientTls>,
    ) -> Result<Self, tonic::transport::Error> {
        #[allow(unused_mut)]
        let mut endpoint = Endpoint::from_shared(url.into())?
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .connect_timeout(Duration::from_secs(10));
        #[cfg(feature = "tls")]
        if let Some(tls) = tls {
            endpoint = endpoint.tls_config(tls)?;
        }
        #[cfg(not(feature = "tls"))]
        if let Some(never) = tls {
            match never {}
        }
        Ok(Self::new(endpoint.connect().await?))
    }

    /// Sends `key: value` with every request (e.g. a token for the server's
    /// [`PeerIdentity`](crate::PeerIdentity)).
    pub fn with_metadata(mut self, key: &str, value: &str) -> Result<Self, InvalidMetadata> {
        let key: AsciiMetadataKey = key.parse().map_err(|_| InvalidMetadata(key.to_owned()))?;
        let value: AsciiMetadataValue = value
            .parse()
            .map_err(|_| InvalidMetadata(key.to_string()))?;
        Arc::make_mut(&mut self.metadata).push((key, value));
        Ok(self)
    }

    /// Sends `authorization: Bearer <token>` (see
    /// [`TokenIdentity`](crate::TokenIdentity)).
    pub fn bearer_token(self, token: &str) -> Result<Self, InvalidMetadata> {
        self.with_metadata("authorization", &format!("Bearer {token}"))
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        for (key, value) in self.metadata.iter() {
            request.metadata_mut().insert(key.clone(), value.clone());
        }
        request
    }
}

fn convert(m: Result<pb::FeedMessage, Status>) -> Result<FeedMessage, TransportError> {
    let m = m.map_err(from_status)?;
    FeedMessage::try_from(m).map_err(TransportError::Protocol)
}

impl Transport for Client {
    async fn subscribe(
        &self,
        _producer: &str,
        start: Start,
    ) -> Result<Subscription, TransportError> {
        let (outbound, rx) = mpsc::channel::<pb::SubscribeMessage>(16);
        outbound
            .send(pb::SubscribeMessage {
                msg: Some(pb::subscribe_message::Msg::Start(start.into())),
            })
            .await
            .map_err(|_| TransportError::Closed)?;
        let requests = stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|m| (m, rx)) });

        let response = self
            .inner
            .clone()
            .subscribe(self.request(requests))
            .await
            .map_err(from_status)?;

        // Forward acks on the same call until the subscription is dropped.
        let (acks, mut applied) = AckSender::channel();
        tokio::spawn(async move {
            while let Some(seq) = applied.changed().await {
                let ack = pb::SubscribeMessage {
                    msg: Some(pb::subscribe_message::Msg::Ack(pb::Ack {
                        applied_seq: seq.get(),
                    })),
                };
                if outbound.send(ack).await.is_err() {
                    return;
                }
            }
        });

        let messages: MessageStream = Box::pin(response.into_inner().map(convert));
        Ok(Subscription { messages, acks })
    }

    async fn snapshot(
        &self,
        _producer: &str,
        request: SnapshotRequest,
    ) -> Result<MessageStream, TransportError> {
        let response = self
            .inner
            .clone()
            .snapshot(self.request(pb::SnapshotRequest::from(request)))
            .await
            .map_err(from_status)?;
        Ok(Box::pin(response.into_inner().map(convert)))
    }

    async fn head(&self, _producer: &str) -> Result<FeedHead, TransportError> {
        let response = self
            .inner
            .clone()
            .head(self.request(pb::HeadRequest {}))
            .await
            .map_err(from_status)?;
        FeedHead::try_from(response.into_inner()).map_err(TransportError::Protocol)
    }
}
