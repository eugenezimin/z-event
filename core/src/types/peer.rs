//! [`PeerId`]: the authenticated identity of a remote consumer.

use std::fmt;

/// The identity of the caller, as established by the transport (for gRPC,
/// the mTLS client certificate). A consumer may only subscribe under its
/// own name: `Start::consumer` must equal the peer id.
///
/// ```
/// use z_event::PeerId;
/// let peer = PeerId::from("billing");
/// assert_eq!(peer.as_str(), "billing");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(String);

impl PeerId {
    /// A peer id.
    pub fn new(id: impl Into<String>) -> Self {
        PeerId(id.into())
    }

    /// The peer id as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for PeerId {
    fn from(s: &str) -> Self {
        PeerId(s.to_owned())
    }
}

impl From<String> for PeerId {
    fn from(s: String) -> Self {
        PeerId(s)
    }
}
