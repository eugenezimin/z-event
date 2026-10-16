//! # z-event
//!
//! Reliable, push-based events between services, without a message broker.
//!
//! A producer writes events into its own storage in the **same transaction**
//! as the business change (a transactional outbox). A sequencer stamps
//! committed events with a gapless, commit-ordered `seq`, and the producer
//! serves them as a feed. Consumers subscribe once and get events pushed;
//! each consumer stores its cursor in the same transaction as the effect of
//! the event, so nothing is lost and nothing is applied twice.
//!
//! This crate is the database- and transport-agnostic core. Storage and
//! network transport are adapters (see the repository's `DESIGN.md`).

pub mod error;
pub mod types;

pub use error::{BoxError, FeedError, HandlerError, SubscriberError, TransportError};
pub use types::*;

pub use bytes::Bytes;
pub use time::OffsetDateTime;
pub use ulid::Ulid;
