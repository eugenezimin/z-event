//! Behaviour specific to the gRPC transport: real network drops, mounting
//! on the host's server, bearer tokens, mutual TLS, and latency.

#![allow(clippy::unwrap_used)]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use z_event::{
    AllowAll, Backoff, CursorKey, CursorStore, Event, Handler, HandlerError, NewEvent, Producer,
    ProducerConfig, Seq, StopReason, Store, Subscriber, SubscriberError, SubscriberStatus,
};
use z_event_grpc::{Client, TokenIdentity, feed_service};
use z_event_memory::{MemoryStore, MemoryTx};

fn producer(store: MemoryStore) -> Producer<MemoryStore> {
    Producer::new(
        store,
        ProducerConfig::new("orders")
            .safety_interval(Duration::from_millis(50))
            .lead_retry_interval(Duration::from_millis(50)),
    )
}

/// Writes `effect/<seq>` (asserting it is new) and records the commit →
/// apply latency.
#[derive(Clone, Default)]
struct Apply {
    latencies: Arc<Mutex<Vec<Duration>>>,
}

impl Handler<MemoryStore> for Apply {
    async fn handle(&self, tx: &mut MemoryTx, ev: &Event) -> Result<(), HandlerError> {
        let key = format!("effect/{:06}", ev.seq.get());
        assert!(tx.get(&key).is_none(), "effect {key} applied twice");
        tx.put(key, "");
        let lag = (time::OffsetDateTime::now_utc() - ev.occurred_at).unsigned_abs();
        self.latencies.lock().unwrap().push(lag);
        Ok(())
    }
}

async fn append(producer: &Producer<MemoryStore>, n: u64, prefix: &str, gap: Duration) {
    for i in 0..n {
        let store = producer.store();
        let mut tx = store.begin().await.unwrap();
        producer
            .append(
                &mut tx,
                NewEvent::new("OrderConfirmed", format!("{prefix}/{i}"), 1, "{}"),
            )
            .await
            .unwrap();
        store.commit(tx).await.unwrap();
        if !gap.is_zero() {
            tokio::time::sleep(gap).await;
        }
    }
}

async fn wait_cursor(store: &MemoryStore, seq: u64) {
    let key = CursorKey::new("billing", "orders");
    tokio::time::timeout(Duration::from_secs(30), async {
        while store.cursor(&key).await.unwrap().seq < Seq(seq) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("cursor did not reach {seq}"));
}

fn subscriber<T: z_event::Transport>(
    billing: &MemoryStore,
    transport: T,
    handler: Apply,
) -> z_event::SubscriberHandle {
    Subscriber::builder(billing.clone(), transport, handler)
        .consumer("billing")
        .producer("orders")
        .reconnect(Backoff::new(
            Duration::from_millis(10),
            Duration::from_millis(200),
        ))
        .ack_interval(Duration::from_millis(50))
        .spawn()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivery_survives_tcp_connection_cuts() {
    let orders = MemoryStore::new();
    let billing = MemoryStore::new();
    let producer = producer(orders);
    let running = producer.start();
    let (server, _stop) = support::serve(producer.feed_source(AllowAll)).await;
    let (proxy, cuts, proxy_task) = support::cutting_proxy(server, 7).await;

    let client = Client::connect(format!("http://{proxy}"), None)
        .await
        .unwrap()
        .with_metadata(support::PEER_HEADER, "billing")
        .unwrap();
    let sub = subscriber(&billing, client, Apply::default());
    append(&producer, 300, "orders", Duration::from_millis(5)).await;
    wait_cursor(&billing, 300).await;
    assert_eq!(
        billing.scan_prefix("effect/").len(),
        300,
        "every event exactly once"
    );
    assert!(
        cuts.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "connections were cut"
    );

    sub.shutdown().await.unwrap();
    proxy_task.abort();
    running.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mounted_on_the_hosts_server_with_bearer_tokens() {
    let orders = MemoryStore::new();
    let billing = MemoryStore::new();
    let producer = producer(orders);
    let running = producer.start();

    // The host's own server: its settings, the feed added as one service.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let identity = TokenIdentity::new().token("billing-token", "billing");
    tokio::spawn(
        Server::builder()
            .timeout(Duration::from_secs(60))
            .add_service(feed_service(producer.feed_source(AllowAll), identity))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            }),
    );

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let good = Client::new(channel.clone())
        .bearer_token("billing-token")
        .unwrap();
    let sub = subscriber(&billing, good, Apply::default());
    append(&producer, 20, "orders", Duration::ZERO).await;
    wait_cursor(&billing, 20).await;
    sub.shutdown().await.unwrap();

    // An unknown token is refused for good.
    let bad = Client::new(channel).bearer_token("stolen").unwrap();
    let refused = subscriber(&MemoryStore::new(), bad, Apply::default());
    let mut status = refused.status_watch();
    let result = tokio::time::timeout(Duration::from_secs(5), refused.join())
        .await
        .unwrap();
    assert!(
        matches!(result, Err(SubscriberError::PermissionDenied(_))),
        "{result:?}"
    );
    assert_eq!(
        *status.borrow_and_update(),
        SubscriberStatus::Stopped(StopReason::PermissionDenied)
    );

    let _ = stop.send(());
    running.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_to_applied_latency_p95_under_100ms() {
    let orders = MemoryStore::new();
    let billing = MemoryStore::new();
    let producer = producer(orders);
    let running = producer.start();
    let (server, _stop) = support::serve(producer.feed_source(AllowAll)).await;
    let client = Client::connect(format!("http://{server}"), None)
        .await
        .unwrap()
        .with_metadata(support::PEER_HEADER, "billing")
        .unwrap();
    let handler = Apply::default();
    let sub = subscriber(&billing, client, handler.clone());
    tokio::time::sleep(Duration::from_millis(200)).await; // caught up, live

    append(&producer, 200, "orders", Duration::from_millis(5)).await;
    wait_cursor(&billing, 200).await;
    let mut latencies = handler.latencies.lock().unwrap().clone();
    latencies.sort();
    let p95 = latencies[latencies.len() * 95 / 100];
    eprintln!(
        "commit -> applied over gRPC: p50 {:?}, p95 {:?}, max {:?}",
        latencies[latencies.len() / 2],
        p95,
        latencies.last().unwrap()
    );
    assert!(p95 < Duration::from_millis(100), "p95 {p95:?}");

    sub.shutdown().await.unwrap();
    running.shutdown().await;
}

#[cfg(feature = "tls")]
mod mtls {
    use super::*;
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
    use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
    use z_event_grpc::{MtlsIdentity, serve_standalone_with_shutdown};

    struct Pki {
        ca_pem: String,
        issuer: Issuer<'static, KeyPair>,
    }

    impl Pki {
        fn new() -> Self {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params
                .distinguished_name
                .push(DnType::CommonName, "z-event test CA");
            let ca = params.self_signed(&key).unwrap();
            Pki {
                ca_pem: ca.pem(),
                issuer: Issuer::new(params, key),
            }
        }

        /// A certificate with this CN (and DNS SAN), as an identity.
        fn leaf(&self, name: &str) -> Identity {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(vec![name.to_owned()]).unwrap();
            params.distinguished_name.push(DnType::CommonName, name);
            let cert = params.signed_by(&key, &self.issuer).unwrap();
            Identity::from_pem(cert.pem(), key.serialize_pem())
        }

        fn ca(&self) -> Certificate {
            Certificate::from_pem(&self.ca_pem)
        }
    }

    async fn client(
        pki: &Pki,
        addr: std::net::SocketAddr,
        cert: Option<&str>,
    ) -> Result<Client, tonic::transport::Error> {
        let mut tls = ClientTlsConfig::new()
            .ca_certificate(pki.ca())
            .domain_name("localhost");
        if let Some(name) = cert {
            tls = tls.identity(pki.leaf(name));
        }
        Client::connect(format!("https://{addr}"), Some(tls)).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn client_certificates_identify_consumers() {
        let pki = Pki::new();
        let orders = MemoryStore::new();
        let billing = MemoryStore::new();
        let producer = producer(orders);
        let running = producer.start();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tls = ServerTlsConfig::new()
            .identity(pki.leaf("localhost"))
            .client_ca_root(pki.ca());
        let (stop, stopped) = oneshot::channel::<()>();
        tokio::spawn(serve_standalone_with_shutdown(
            producer.feed_source(AllowAll),
            MtlsIdentity::default(),
            listener,
            Some(tls),
            async {
                let _ = stopped.await;
            },
        ));

        // billing's certificate: delivered.
        let sub = subscriber(
            &billing,
            client(&pki, addr, Some("billing")).await.unwrap(),
            Apply::default(),
        );
        append(&producer, 10, "orders", Duration::ZERO).await;
        wait_cursor(&billing, 10).await;
        sub.shutdown().await.unwrap();

        // mallory's certificate cannot subscribe as billing.
        let mallory = client(&pki, addr, Some("mallory")).await.unwrap();
        let refused = subscriber(&MemoryStore::new(), mallory, Apply::default());
        let result = tokio::time::timeout(Duration::from_secs(5), refused.join())
            .await
            .unwrap();
        assert!(
            matches!(result, Err(SubscriberError::PermissionDenied(_))),
            "{result:?}"
        );

        // No client certificate: the TLS handshake fails.
        let anonymous = client(&pki, addr, None).await;
        let rejected = match anonymous {
            Err(_) => true,
            Ok(c) => z_event::Transport::head(&c, "orders").await.is_err(),
        };
        assert!(rejected, "a client without a certificate must be rejected");

        let _ = stop.send(());
        running.shutdown().await;
    }
}
