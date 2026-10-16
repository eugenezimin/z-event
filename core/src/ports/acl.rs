//! [`Acl`]: which consumer may read which event types.

use std::collections::{BTreeMap, BTreeSet};

use crate::types::PeerId;

/// Answers "may this peer read events of this type?". The host decides the
/// policy; the feed enforces it.
pub trait Acl: Send + Sync + 'static {
    /// Whether `peer` may read `event_type`.
    fn allows(&self, peer: &PeerId, event_type: &str) -> bool;
}

impl<F> Acl for F
where
    F: Fn(&PeerId, &str) -> bool + Send + Sync + 'static,
{
    fn allows(&self, peer: &PeerId, event_type: &str) -> bool {
        self(peer, event_type)
    }
}

/// Allows everything. For development and tests only.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAll;

impl Acl for AllowAll {
    fn allows(&self, _peer: &PeerId, _event_type: &str) -> bool {
        true
    }
}

/// A static map from consumer to readable types; `"*"` grants every type.
/// Unknown consumers may read nothing.
///
/// ```
/// use z_event::{Acl, PeerId, TypeAcl};
///
/// let acl = TypeAcl::new()
///     .allow("billing", ["OrderConfirmed", "OrderCancelled"])
///     .allow("audit", ["*"]);
/// assert!(acl.allows(&PeerId::from("billing"), "OrderConfirmed"));
/// assert!(!acl.allows(&PeerId::from("billing"), "CustomerCreated"));
/// assert!(acl.allows(&PeerId::from("audit"), "CustomerCreated"));
/// assert!(!acl.allows(&PeerId::from("search"), "OrderConfirmed"));
/// ```
#[derive(Clone, Debug, Default)]
pub struct TypeAcl {
    grants: BTreeMap<String, BTreeSet<String>>,
}

impl TypeAcl {
    /// An ACL that allows nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lets `consumer` read `types` (`"*"` for every type).
    pub fn allow<I, S>(mut self, consumer: impl Into<String>, types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.grants
            .entry(consumer.into())
            .or_default()
            .extend(types.into_iter().map(Into::into));
        self
    }
}

impl Acl for TypeAcl {
    fn allows(&self, peer: &PeerId, event_type: &str) -> bool {
        self.grants
            .get(peer.as_str())
            .is_some_and(|types| types.contains("*") || types.contains(event_type))
    }
}
