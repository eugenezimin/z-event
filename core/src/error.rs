//! Error types: [`HandlerError`] (host handlers), [`FeedError`] (producer
//! side), [`TransportError`] (consumer side) and [`SubscriberError`].

use crate::types::{Epoch, PeerId, Seq};

/// A boxed, thread-safe error.
///
/// ```
/// use z_event::BoxError;
/// let e: BoxError = "something failed".into();
/// assert_eq!(e.to_string(), "something failed");
/// ```
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// What a [`Handler`](crate::Handler) returns when it cannot apply an event.
///
/// ```
/// use z_event::{Event, HandlerError};
///
/// fn parse(ev: &Event) -> Result<serde_json::Value, HandlerError> {
///     // A payload that does not parse never will: park it at once.
///     ev.json().map_err(HandlerError::permanent)
/// }
///
/// let timeout = HandlerError::transient(std::io::Error::other("db timeout"));
/// assert!(!timeout.is_permanent());
/// ```
#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    /// Might succeed on retry (timeout, lock conflict, a dependency is down).
    #[error("transient: {0}")]
    Transient(#[source] BoxError),
    /// Will not succeed on retry (a bug, an unexpected payload). The event is
    /// parked right away.
    #[error("permanent: {0}")]
    Permanent(#[source] BoxError),
}

impl HandlerError {
    /// A transient error. Usable as `.map_err(HandlerError::transient)`.
    pub fn transient(e: impl Into<BoxError>) -> Self {
        HandlerError::Transient(e.into())
    }

    /// A permanent error. Usable as `.map_err(HandlerError::permanent)`.
    pub fn permanent(e: impl Into<BoxError>) -> Self {
        HandlerError::Permanent(e.into())
    }

    /// Whether retrying is pointless.
    pub fn is_permanent(&self) -> bool {
        matches!(self, HandlerError::Permanent(_))
    }
}

/// Why a producer's [`FeedSource`](crate::FeedSource) refused or ended a
/// stream.
///
/// ```
/// use z_event::{FeedError, TransportError};
/// // Transports carry feed errors to the client, which classifies them.
/// let e: TransportError = FeedError::Unsupported("snapshots are not configured").into();
/// assert!(!e.is_retryable());
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FeedError {
    /// The peer may not subscribe under that name, or may not read a type.
    #[error("permission denied for {peer}: {reason}")]
    PermissionDenied {
        /// The caller.
        peer: PeerId,
        /// Why.
        reason: String,
    },
    /// The consumer's cursor belongs to another incarnation of the feed.
    #[error("epoch mismatch: producer {producer_epoch}, consumer {consumer_epoch:?}")]
    EpochMismatch {
        /// The producer's current epoch.
        producer_epoch: Epoch,
        /// The epoch the consumer presented.
        consumer_epoch: Option<Epoch>,
    },
    /// The consumer's position was already deleted by retention.
    #[error("position {from_seq} is beyond retention (purged through {purged_through})")]
    BeyondRetention {
        /// The requested start.
        from_seq: Seq,
        /// Everything up to here is gone.
        purged_through: Seq,
    },
    /// The request is malformed.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// The producer does not support the request.
    #[error("unsupported: {0}")]
    Unsupported(&'static str),
    /// The producer's storage failed.
    #[error("unavailable: {0}")]
    Unavailable(#[source] BoxError),
}

/// A client-side transport failure. The single error type of the
/// [`Transport`](crate::Transport) port, so the subscriber can tell errors
/// worth a reconnect from errors that must stop it.
///
/// ```
/// use z_event::TransportError;
/// assert!(TransportError::Closed.is_retryable());
/// assert!(!TransportError::PermissionDenied("mallory".into()).is_retryable());
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// Identity or ACL refused. Not retried.
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    /// The cursor's epoch does not match the producer's. Not retried.
    #[error("epoch mismatch: producer {producer_epoch:?}, consumer {consumer_epoch:?}")]
    EpochMismatch {
        /// The producer's current epoch, if known.
        producer_epoch: Option<Epoch>,
        /// The epoch the consumer presented.
        consumer_epoch: Option<Epoch>,
    },
    /// The cursor is below the producer's retention window. Not retried.
    #[error("position {from_seq} is beyond retention (purged through {purged_through})")]
    BeyondRetention {
        /// The requested start.
        from_seq: Seq,
        /// Everything up to here is gone.
        purged_through: Seq,
    },
    /// The producer is not known (yet). Retried.
    #[error("unknown producer {0}")]
    UnknownProducer(String),
    /// The producer does not support the request. Not retried.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The request was rejected as malformed. Not retried.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// The stream ended. Retried.
    #[error("stream closed")]
    Closed,
    /// The peer violated the protocol. Retried.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// The network or the producer's storage failed. Retried.
    #[error("unavailable: {0}")]
    Unavailable(#[source] BoxError),
}

impl TransportError {
    /// Whether reconnecting may help.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            TransportError::UnknownProducer(_)
                | TransportError::Closed
                | TransportError::Protocol(_)
                | TransportError::Unavailable(_)
        )
    }
}

impl From<FeedError> for TransportError {
    fn from(e: FeedError) -> Self {
        match e {
            FeedError::PermissionDenied { peer, reason } => {
                TransportError::PermissionDenied(format!("{peer}: {reason}"))
            }
            FeedError::EpochMismatch {
                producer_epoch,
                consumer_epoch,
            } => TransportError::EpochMismatch {
                producer_epoch: Some(producer_epoch),
                consumer_epoch,
            },
            FeedError::BeyondRetention {
                from_seq,
                purged_through,
            } => TransportError::BeyondRetention {
                from_seq,
                purged_through,
            },
            FeedError::InvalidRequest(s) => TransportError::InvalidRequest(s),
            FeedError::Unsupported(s) => TransportError::Unsupported(s.to_owned()),
            FeedError::Unavailable(e) => TransportError::Unavailable(e),
        }
    }
}

/// Why a [`Subscriber`](crate::Subscriber) stopped, or why one of its
/// administrative calls failed.
///
/// ```
/// use z_event::{SubscriberError, SubscriberHandle};
///
/// async fn supervise(handle: SubscriberHandle) {
///     match handle.join().await {
///         Ok(()) => {}
///         Err(SubscriberError::EpochMismatch { .. }) => {
///             // The producer was restored from a backup: rebuild from a snapshot.
///         }
///         Err(e) => eprintln!("subscriber stopped: {e}"),
///     }
/// }
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubscriberError {
    /// The producer's feed is another incarnation than the cursor's: the
    /// producer was restored from a backup. Rebuild from a snapshot.
    #[error("epoch mismatch: producer {producer_epoch:?}, cursor {cursor_epoch:?}")]
    EpochMismatch {
        /// The producer's epoch, if known.
        producer_epoch: Option<Epoch>,
        /// The cursor's epoch.
        cursor_epoch: Option<Epoch>,
    },
    /// The cursor is below the producer's retention window.
    #[error("position {from_seq} is beyond retention (purged through {purged_through})")]
    BeyondRetention {
        /// The cursor position.
        from_seq: Seq,
        /// Everything up to here is gone.
        purged_through: Seq,
    },
    /// The producer refused this consumer.
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    /// A request the producer rejected for good.
    #[error("rejected: {0}")]
    Rejected(String),
    /// The consumer's storage failed.
    #[error("store: {0}")]
    Store(#[source] BoxError),
    /// The subscriber task panicked or was aborted.
    #[error("subscriber task failed: {0}")]
    Task(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classification() {
        assert!(TransportError::Closed.is_retryable());
        assert!(TransportError::Unavailable("x".into()).is_retryable());
        assert!(!TransportError::PermissionDenied("x".into()).is_retryable());
        let e: TransportError = FeedError::EpochMismatch {
            producer_epoch: Epoch::new("a").unwrap(),
            consumer_epoch: None,
        }
        .into();
        assert!(!e.is_retryable());
    }

    #[test]
    fn handler_error_kinds() {
        assert!(HandlerError::permanent("bad").is_permanent());
        assert!(!HandlerError::transient("later").is_permanent());
    }
}
