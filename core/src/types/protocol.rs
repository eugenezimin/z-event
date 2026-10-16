//! The feed protocol, independent of any transport: [`Start`], [`Ack`],
//! [`Checkpoint`], [`FeedMessage`], [`SnapshotRequest`], plus the storage
//! wake-up [`Notification`].

use serde::{Deserialize, Serialize};

use super::event::Event;
use super::filter::TypeFilter;
use super::position::{Epoch, Seq};

/// Opens a subscription: "I have applied up to `from_seq` in `epoch`; send me
/// these types".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Start {
    /// The consumer name; must match the caller's identity.
    pub consumer: String,
    /// Last applied position (exclusive start).
    pub from_seq: Seq,
    /// Epoch of that position; `None` on the first subscribe.
    pub epoch: Option<Epoch>,
    /// Requested types. Empty means every type the ACL lets this consumer read.
    pub types: Vec<String>,
}

impl Start {
    /// The requested types as a filter.
    pub fn filter(&self) -> TypeFilter {
        TypeFilter::only(self.types.iter().cloned())
    }
}

/// "I have durably applied everything up to `applied_seq`." Sent every few
/// seconds; feeds lag metrics and retention. Delivery never waits for acks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    /// Highest durably applied position.
    pub applied_seq: Seq,
}

/// Heartbeat and skip marker.
///
/// Promise: every event with `seq <= self.seq` that matches the stream's
/// filter was sent before this message. The consumer may therefore move its
/// cursor to `seq` even if it received no event there (filtered-out ranges).
/// A stream always opens with a checkpoint at the consumer's `from_seq`; it
/// tells a new consumer the feed's epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Everything matching up to here was sent.
    pub seq: Seq,
    /// The producer's head when the checkpoint was made.
    pub head: Seq,
    /// The feed's epoch.
    pub epoch: Epoch,
}

/// A message from producer to consumer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeedMessage {
    /// One event, in `seq` order.
    Event(Event),
    /// A heartbeat or skip marker.
    Checkpoint(Checkpoint),
}

/// Asks the producer for the current state of aggregates (bootstrap of a new
/// consumer beyond retention).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRequest {
    /// The consumer name; must match the caller's identity.
    pub consumer: String,
    /// Aggregate types to snapshot, as understood by the host's snapshotter.
    pub aggregate_types: Vec<String>,
}

/// Wake-up hints from a storage adapter. They may be coalesced or lost; a
/// lost hint only costs latency, because the producer re-checks on timers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Notification {
    /// Events were committed to the outbox (wake the sequencer).
    Appended,
    /// Events were sequenced up to this head (wake feed streams).
    Sequenced(Seq),
    /// Hints were dropped; re-check everything.
    Lagged,
}
