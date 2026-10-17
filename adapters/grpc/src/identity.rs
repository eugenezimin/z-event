//! Who is calling: [`PeerIdentity`] and its implementations.

use std::collections::HashMap;

use tonic::metadata::MetadataMap;
use tonic::{Request, Status};
use z_event::PeerId;

/// Establishes the identity of the caller of a feed request. The consumer
/// name in `Start` must equal it.
///
/// ```
/// use tonic::{Request, Status};
/// use z_event::PeerId;
/// use z_event_grpc::PeerIdentity;
///
/// /// Trusts an `x-service` header set by an authenticating gateway.
/// struct FromGateway;
/// impl PeerIdentity for FromGateway {
///     fn identify<T>(&self, request: &Request<T>) -> Result<PeerId, Status> {
///         request
///             .metadata()
///             .get("x-service")
///             .and_then(|v| v.to_str().ok())
///             .map(PeerId::from)
///             .ok_or_else(|| Status::unauthenticated("no x-service header"))
///     }
/// }
/// ```
pub trait PeerIdentity: Send + Sync + 'static {
    /// The caller's identity, or a status (usually `UNAUTHENTICATED`).
    fn identify<T>(&self, request: &Request<T>) -> Result<PeerId, Status>;
}

/// Bearer tokens mapped to peers: `authorization: Bearer <token>`.
///
/// ```
/// use z_event_grpc::TokenIdentity;
/// let identity = TokenIdentity::new()
///     .token("s3cr3t-billing", "billing")
///     .token("s3cr3t-search", "search");
/// # let _ = identity;
/// ```
#[derive(Clone, Debug, Default)]
pub struct TokenIdentity {
    tokens: HashMap<String, PeerId>,
}

impl TokenIdentity {
    /// No tokens yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Accepts `token` as `peer`.
    pub fn token(mut self, token: impl Into<String>, peer: impl Into<PeerId>) -> Self {
        self.tokens.insert(token.into(), peer.into());
        self
    }

    fn lookup(&self, metadata: &MetadataMap) -> Option<PeerId> {
        let header = metadata.get("authorization")?.to_str().ok()?;
        let token = header.strip_prefix("Bearer ")?;
        self.tokens.get(token).cloned()
    }
}

impl PeerIdentity for TokenIdentity {
    fn identify<T>(&self, request: &Request<T>) -> Result<PeerId, Status> {
        self.lookup(request.metadata())
            .ok_or_else(|| Status::unauthenticated("missing or unknown bearer token"))
    }
}

/// Takes the identity from a metadata header **as is**. Only for tests, or
/// behind a proxy that authenticates callers and sets the header itself.
///
/// ```
/// use z_event_grpc::MetadataIdentity;
/// let identity = MetadataIdentity::new("x-zevent-peer");
/// # let _ = identity;
/// ```
#[derive(Clone, Debug)]
pub struct MetadataIdentity {
    header: &'static str,
}

impl MetadataIdentity {
    /// Reads the peer from `header`.
    pub fn new(header: &'static str) -> Self {
        MetadataIdentity { header }
    }
}

impl PeerIdentity for MetadataIdentity {
    fn identify<T>(&self, request: &Request<T>) -> Result<PeerId, Status> {
        request
            .metadata()
            .get(self.header)
            .and_then(|v| v.to_str().ok())
            .filter(|v| !v.is_empty())
            .map(PeerId::from)
            .ok_or_else(|| Status::unauthenticated(format!("missing {} header", self.header)))
    }
}

/// The identity is the client certificate (mutual TLS): its subject common
/// name by default, or its first DNS subject alternative name. The server
/// must require client certificates (`ServerTlsConfig::client_ca_root`).
///
/// ```
/// use z_event_grpc::MtlsIdentity;
/// let by_cn = MtlsIdentity::default();
/// let by_san = MtlsIdentity::dns_san();
/// # let _ = (by_cn, by_san);
/// ```
#[cfg(feature = "tls")]
#[derive(Clone, Copy, Debug, Default)]
pub struct MtlsIdentity {
    use_san: bool,
}

#[cfg(feature = "tls")]
impl MtlsIdentity {
    /// Uses the first DNS subject alternative name instead of the CN.
    pub fn dns_san() -> Self {
        MtlsIdentity { use_san: true }
    }

    fn name_in(&self, der: &[u8]) -> Option<String> {
        use x509_parser::prelude::*;
        let (_, cert) = X509Certificate::from_der(der).ok()?;
        if self.use_san {
            let san = cert.subject_alternative_name().ok()??;
            san.value.general_names.iter().find_map(|name| match name {
                GeneralName::DNSName(dns) => Some((*dns).to_owned()),
                _ => None,
            })
        } else {
            let cn = cert.subject().iter_common_name().next()?;
            cn.as_str().ok().map(str::to_owned)
        }
    }
}

#[cfg(feature = "tls")]
impl PeerIdentity for MtlsIdentity {
    fn identify<T>(&self, request: &Request<T>) -> Result<PeerId, Status> {
        let certs = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("no client certificate"))?;
        let leaf = certs
            .first()
            .ok_or_else(|| Status::unauthenticated("no client certificate"))?;
        self.name_in(leaf.as_ref())
            .map(PeerId::from)
            .ok_or_else(|| Status::unauthenticated("client certificate has no usable name"))
    }
}
