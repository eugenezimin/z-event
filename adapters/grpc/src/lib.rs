//! # z-event-grpc
//!
//! The gRPC transport for z-event (tonic). A producer serves its feed; a
//! consumer's [`Subscriber`](z_event::Subscriber) subscribes through a
//! [`Client`].
//!
//! | Use case | API |
//! |---|---|
//! | The host already runs a tonic server (its TLS, interceptors, port) | [`feed_service`] returns a service for `Server::builder().add_service(..)` |
//! | The host already has a `Channel` | [`Client::new`] |
//! | No gRPC setup yet | [`serve_standalone`] and [`Client::connect`] |
//! | A non-Rust consumer | generate a client from `proto/zevent/v1/feed.proto` |
//!
//! Identity comes from a [`PeerIdentity`]: [`MtlsIdentity`] (client
//! certificates), [`TokenIdentity`] (bearer tokens), or your own. Errors map
//! to status codes as described in [`status`]. The protobuf code is
//! generated at build time with `protox`, so no `protoc` is needed.
//!
//! ```no_run
//! use z_event::{CursorStore, Event, Handler, HandlerError, Subscriber};
//! use z_event_grpc::Client;
//!
//! struct CreateInvoice;
//! impl<S: CursorStore> Handler<S> for CreateInvoice {
//!     async fn handle(&self, _tx: &mut S::Tx, _ev: &Event) -> Result<(), HandlerError> {
//!         Ok(())
//!     }
//! }
//!
//! async fn consume<S: CursorStore>(store: S) -> Result<(), Box<dyn std::error::Error>> {
//!     let orders = Client::connect("http://orders:50051", None).await?.bearer_token("billing-token")?;
//!     let _running = Subscriber::builder(store, orders, CreateInvoice)
//!         .consumer("billing")
//!         .producer("orders")
//!         .spawn();
//!     Ok(())
//! }
//! ```

mod client;
mod convert;
mod identity;
mod server;
mod standalone;
pub mod status;

/// The generated protobuf messages and tonic stubs (`zevent.v1`).
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod pb {
    tonic::include_proto!("zevent.v1");
}

pub use client::{Client, InvalidMetadata};
#[cfg(feature = "tls")]
pub use identity::MtlsIdentity;
pub use identity::{MetadataIdentity, PeerIdentity, TokenIdentity};
pub use server::{FeedService, feed_service};
pub use standalone::{serve_standalone, serve_standalone_with_shutdown};

/// Client TLS settings (with the `tls` feature: tonic's `ClientTlsConfig`).
#[cfg(feature = "tls")]
pub type ClientTls = tonic::transport::ClientTlsConfig;
/// Client TLS settings (uninhabited without the `tls` feature).
#[cfg(not(feature = "tls"))]
pub type ClientTls = std::convert::Infallible;

/// Server TLS settings (with the `tls` feature: tonic's `ServerTlsConfig`).
#[cfg(feature = "tls")]
pub type ServerTls = tonic::transport::ServerTlsConfig;
/// Server TLS settings (uninhabited without the `tls` feature).
#[cfg(not(feature = "tls"))]
pub type ServerTls = std::convert::Infallible;
