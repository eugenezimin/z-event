//! Turnkey serving for hosts without their own tonic server.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use z_event::{FeedSource, OutboxStore};

use crate::ServerTls;
use crate::identity::PeerIdentity;
use crate::server::feed_service;

fn builder(tls: Option<ServerTls>) -> Result<Server, tonic::transport::Error> {
    #[allow(unused_mut)]
    let mut server = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)));
    #[cfg(feature = "tls")]
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }
    #[cfg(not(feature = "tls"))]
    if let Some(never) = tls {
        match never {}
    }
    Ok(server)
}

/// Serves the feed on `addr` until the process ends, with HTTP/2
/// keep-alive and optional TLS (mutual TLS when the config requires client
/// certificates).
///
/// ```no_run
/// use z_event::{AllowAll, OutboxStore, Producer};
/// use z_event_grpc::{MtlsIdentity, ServerTls, serve_standalone};
///
/// async fn run<S: OutboxStore>(producer: Producer<S>, tls: ServerTls) -> Result<(), tonic::transport::Error> {
///     let addr = "0.0.0.0:50051".parse().unwrap();
///     serve_standalone(producer.feed_source(AllowAll), MtlsIdentity::default(), addr, Some(tls)).await
/// }
/// ```
pub async fn serve_standalone<S: OutboxStore, I: PeerIdentity>(
    source: FeedSource<S>,
    identity: I,
    addr: SocketAddr,
    tls: Option<ServerTls>,
) -> Result<(), tonic::transport::Error> {
    builder(tls)?
        .add_service(feed_service(source, identity))
        .serve(addr)
        .await
}

/// Like [`serve_standalone`], on an already bound listener (e.g. port 0),
/// until `shutdown` completes.
///
/// ```no_run
/// use tokio::net::TcpListener;
/// use z_event::{AllowAll, OutboxStore, Producer};
/// use z_event_grpc::{TokenIdentity, serve_standalone_with_shutdown};
///
/// async fn run<S: OutboxStore>(
///     producer: Producer<S>,
///     stop: tokio::sync::oneshot::Receiver<()>,
/// ) -> Result<(), Box<dyn std::error::Error>> {
///     let listener = TcpListener::bind("127.0.0.1:0").await?;
///     println!("feed on {}", listener.local_addr()?);
///     let identity = TokenIdentity::new().token("t", "billing");
///     serve_standalone_with_shutdown(producer.feed_source(AllowAll), identity, listener, None, async {
///         let _ = stop.await;
///     })
///     .await?;
///     Ok(())
/// }
/// ```
pub async fn serve_standalone_with_shutdown<S, I, F>(
    source: FeedSource<S>,
    identity: I,
    listener: TcpListener,
    tls: Option<ServerTls>,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    S: OutboxStore,
    I: PeerIdentity,
    F: Future<Output = ()> + Send,
{
    builder(tls)?
        .add_service(feed_service(source, identity))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}
