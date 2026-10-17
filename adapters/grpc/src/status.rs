//! Errors ↔ gRPC status codes.
//!
//! | Error | Code | Retried by the subscriber |
//! |---|---|---|
//! | identity mismatch, ACL refusal | `PERMISSION_DENIED` | no |
//! | epoch mismatch | `FAILED_PRECONDITION` | no |
//! | start below retention | `OUT_OF_RANGE` | no |
//! | malformed request | `INVALID_ARGUMENT` | no |
//! | snapshots not configured | `UNIMPLEMENTED` | no |
//! | storage failure | `UNAVAILABLE` | yes |
//!
//! Details travel in metadata (`zevent-producer-epoch`,
//! `zevent-purged-through`), so the client can rebuild the exact error.

use tonic::{Code, Status};
use z_event::{Epoch, FeedError, Seq, TransportError};

const PRODUCER_EPOCH: &str = "zevent-producer-epoch";
const CONSUMER_EPOCH: &str = "zevent-consumer-epoch";
const FROM_SEQ: &str = "zevent-from-seq";
const PURGED_THROUGH: &str = "zevent-purged-through";

fn with_meta(mut status: Status, pairs: &[(&'static str, String)]) -> Status {
    for (key, value) in pairs {
        if let Ok(value) = value.parse() {
            status.metadata_mut().insert(*key, value);
        }
    }
    status
}

/// Maps a feed error to the status sent to the client.
pub fn to_status(e: FeedError) -> Status {
    match e {
        FeedError::PermissionDenied { peer, reason } => {
            Status::permission_denied(format!("{peer}: {reason}"))
        }
        FeedError::EpochMismatch {
            producer_epoch,
            consumer_epoch,
        } => with_meta(
            Status::failed_precondition("epoch mismatch: the producer's feed was reset"),
            &[
                (PRODUCER_EPOCH, producer_epoch.to_string()),
                (
                    CONSUMER_EPOCH,
                    consumer_epoch.map(|e| e.to_string()).unwrap_or_default(),
                ),
            ],
        ),
        FeedError::BeyondRetention {
            from_seq,
            purged_through,
        } => with_meta(
            Status::out_of_range("position is beyond retention"),
            &[
                (FROM_SEQ, from_seq.to_string()),
                (PURGED_THROUGH, purged_through.to_string()),
            ],
        ),
        FeedError::InvalidRequest(s) => Status::invalid_argument(s),
        FeedError::Unsupported(s) => Status::unimplemented(s),
        FeedError::Unavailable(e) => Status::unavailable(e.to_string()),
        other => Status::internal(other.to_string()),
    }
}

fn meta(status: &Status, key: &str) -> Option<String> {
    status
        .metadata()
        .get(key)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

fn meta_seq(status: &Status, key: &str) -> Seq {
    Seq(meta(status, key).and_then(|v| v.parse().ok()).unwrap_or(0))
}

/// Maps a status received by the client to a transport error.
pub fn from_status(status: Status) -> TransportError {
    match status.code() {
        Code::PermissionDenied | Code::Unauthenticated => {
            TransportError::PermissionDenied(status.message().to_owned())
        }
        Code::FailedPrecondition => TransportError::EpochMismatch {
            producer_epoch: meta(&status, PRODUCER_EPOCH).and_then(Epoch::new),
            consumer_epoch: meta(&status, CONSUMER_EPOCH).and_then(Epoch::new),
        },
        Code::OutOfRange => TransportError::BeyondRetention {
            from_seq: meta_seq(&status, FROM_SEQ),
            purged_through: meta_seq(&status, PURGED_THROUGH),
        },
        Code::InvalidArgument => TransportError::InvalidRequest(status.message().to_owned()),
        Code::Unimplemented => TransportError::Unsupported(status.message().to_owned()),
        _ => TransportError::Unavailable(Box::new(status)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_survive_the_round_trip() {
        let epoch = Epoch::new("e2").unwrap();
        let e = from_status(to_status(FeedError::EpochMismatch {
            producer_epoch: epoch.clone(),
            consumer_epoch: None,
        }));
        assert!(
            matches!(e, TransportError::EpochMismatch { producer_epoch: Some(p), consumer_epoch: None } if p == epoch)
        );
        let e = from_status(to_status(FeedError::BeyondRetention {
            from_seq: Seq(3),
            purged_through: Seq(9),
        }));
        assert!(matches!(
            e,
            TransportError::BeyondRetention {
                from_seq: Seq(3),
                purged_through: Seq(9)
            }
        ));
        assert!(from_status(to_status(FeedError::Unavailable("db".into()))).is_retryable());
        assert!(!from_status(Status::permission_denied("x")).is_retryable());
        assert!(from_status(Status::cancelled("x")).is_retryable());
    }
}
