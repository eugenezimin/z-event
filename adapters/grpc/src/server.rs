//! The server side: [`FeedService`], mountable on any tonic server.

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use z_event::{FeedSource, OutboxStore};

use crate::identity::PeerIdentity;
use crate::pb;
use crate::pb::event_feed_server::{EventFeed, EventFeedServer};
use crate::status::to_status;

type MessageStream = Pin<Box<dyn Stream<Item = Result<pb::FeedMessage, Status>> + Send + 'static>>;

/// Serves a producer's [`FeedSource`] over gRPC. Build it with
/// [`feed_service`].
pub struct FeedService<S: OutboxStore, I> {
    source: FeedSource<S>,
    identity: Arc<I>,
}

/// The feed as a tonic service, to add to the host's server next to its
/// other services (sharing its port, TLS and interceptors).
///
/// ```no_run
/// use tonic::transport::Server;
/// use z_event::{AllowAll, OutboxStore, Producer};
/// use z_event_grpc::{feed_service, TokenIdentity};
///
/// async fn serve<S: OutboxStore>(producer: Producer<S>) -> Result<(), tonic::transport::Error> {
///     let identity = TokenIdentity::new().token("billing-token", "billing");
///     Server::builder()
///         .add_service(feed_service(producer.feed_source(AllowAll), identity))
///         .serve("0.0.0.0:50051".parse().unwrap())
///         .await
/// }
/// ```
pub fn feed_service<S: OutboxStore, I: PeerIdentity>(
    source: FeedSource<S>,
    identity: I,
) -> EventFeedServer<FeedService<S, I>> {
    EventFeedServer::new(FeedService {
        source,
        identity: Arc::new(identity),
    })
}

fn map_stream(stream: z_event::FeedStream) -> MessageStream {
    Box::pin(stream.map(|m| m.map(Into::into).map_err(to_status)))
}

#[tonic::async_trait]
impl<S: OutboxStore, I: PeerIdentity> EventFeed for FeedService<S, I> {
    type SubscribeStream = MessageStream;
    type SnapshotStream = MessageStream;

    async fn subscribe(
        &self,
        request: Request<Streaming<pb::SubscribeMessage>>,
    ) -> Result<Response<MessageStream>, Status> {
        let peer = self.identity.identify(&request)?;
        let mut inbound = request.into_inner();
        let start = match inbound.message().await? {
            Some(pb::SubscribeMessage {
                msg: Some(pb::subscribe_message::Msg::Start(start)),
            }) => start,
            _ => return Err(Status::invalid_argument("the first message must be Start")),
        };
        let stream = self
            .source
            .open(&peer, start.into())
            .await
            .map_err(to_status)?;

        // Acks arrive on the same call; delivery never waits for them.
        let source = self.source.clone();
        tokio::spawn(async move {
            while let Ok(Some(message)) = inbound.message().await {
                if let Some(pb::subscribe_message::Msg::Ack(ack)) = message.msg
                    && let Err(e) = source.on_ack(&peer, ack.into()).await
                {
                    tracing::debug!(error = %e, "ack not recorded");
                }
            }
        });
        Ok(Response::new(map_stream(stream)))
    }

    async fn snapshot(
        &self,
        request: Request<pb::SnapshotRequest>,
    ) -> Result<Response<MessageStream>, Status> {
        let peer = self.identity.identify(&request)?;
        let stream = self
            .source
            .snapshot(&peer, request.into_inner().into())
            .await
            .map_err(to_status)?;
        Ok(Response::new(map_stream(stream)))
    }

    async fn head(
        &self,
        request: Request<pb::HeadRequest>,
    ) -> Result<Response<pb::HeadResponse>, Status> {
        self.identity.identify(&request)?;
        let head = self.source.head().await.map_err(to_status)?;
        Ok(Response::new(head.into()))
    }
}
