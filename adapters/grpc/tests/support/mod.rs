//! A real gRPC server on localhost for the suites, and a TCP proxy that
//! cuts connections at random.

#![allow(dead_code, clippy::unwrap_used)]

use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use z_event::{FeedSource, OutboxStore, PeerId};
use z_event_grpc::{Client, MetadataIdentity, serve_standalone_with_shutdown};
use z_event_testkit::TransportHarness;

pub const PEER_HEADER: &str = "x-zevent-peer";

/// Stops the server (and the proxy) when dropped.
pub struct Guard {
    _stop: oneshot::Sender<()>,
    _proxy: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(proxy) = &self._proxy {
            proxy.abort();
        }
    }
}

/// Serves a feed source over gRPC on 127.0.0.1 and returns its address.
pub async fn serve<S: OutboxStore>(source: FeedSource<S>) -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(serve_standalone_with_shutdown(
        source,
        MetadataIdentity::new(PEER_HEADER),
        listener,
        None,
        async {
            let _ = stopped.await;
        },
    ));
    (addr, stop)
}

/// The transport suite over real gRPC; the peer travels in a header.
#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcHarness;

impl TransportHarness for GrpcHarness {
    type Client = Client;

    async fn serve<S: OutboxStore>(
        &self,
        _producer: &str,
        source: FeedSource<S>,
        peer: PeerId,
    ) -> (Client, Box<dyn Any + Send>) {
        let (addr, stop) = serve(source).await;
        let client = Client::connect(format!("http://{addr}"), None)
            .await
            .unwrap()
            .with_metadata(PEER_HEADER, peer.as_str())
            .unwrap();
        (
            client,
            Box::new(Guard {
                _stop: stop,
                _proxy: None,
            }),
        )
    }
}

/// A TCP proxy to `upstream` that cuts every connection after a random
/// 50–300 ms. Returns its address and a counter of cut connections.
pub async fn cutting_proxy(
    upstream: SocketAddr,
    seed: u64,
) -> (SocketAddr, Arc<AtomicU64>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cuts = Arc::new(AtomicU64::new(0));
    let counter = cuts.clone();
    let task = tokio::spawn(async move {
        let mut rng = fastrand::Rng::with_seed(seed);
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                return;
            };
            let life = Duration::from_millis(rng.u64(50..300));
            let counter = counter.clone();
            tokio::spawn(async move {
                let Ok(mut outbound) = TcpStream::connect(upstream).await else {
                    return;
                };
                let piped = tokio::io::copy_bidirectional(&mut inbound, &mut outbound);
                if tokio::time::timeout(life, piped).await.is_err() {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                // Dropping both sockets cuts the connection.
            });
        }
    });
    (addr, cuts, task)
}
