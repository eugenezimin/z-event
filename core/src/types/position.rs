//! Positions in a feed: [`Seq`], [`Epoch`], [`Cursor`], [`CursorKey`] and [`FeedHead`].

use std::fmt;

use serde::{Deserialize, Serialize};

/// Position of an event in a producer's feed.
///
/// Assigned by the sequencer, never by the writer: gapless (1, 2, 3, …) and in
/// commit order within one [`Epoch`]. `Seq(0)` means "before the first event".
///
/// ```
/// use z_event::Seq;
/// let s = Seq(41);
/// assert_eq!(s.next(), Seq(42));
/// assert!(Seq::ZERO < s);
/// ```
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Seq(pub u64);

impl Seq {
    /// The position before the first event.
    pub const ZERO: Seq = Seq(0);

    /// The raw value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next position.
    pub const fn next(self) -> Seq {
        Seq(self.0 + 1)
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u64> for Seq {
    fn from(v: u64) -> Self {
        Seq(v)
    }
}

/// Identity of one incarnation of a producer's feed.
///
/// Stores generate it as a ULID string, but any non-empty string is valid.
/// The epoch changes when the producer's storage is restored from a backup
/// (explicitly, or when a consumer turns out to be ahead of the head), so a
/// consumer never silently continues on a feed that went back in time.
///
/// ```
/// use z_event::Epoch;
/// let e = Epoch::generate();
/// assert_eq!(Epoch::new(e.as_str()), Some(e.clone()));
/// assert_eq!(Epoch::new(""), None); // "no epoch yet" on the wire
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Epoch(String);

impl Epoch {
    /// A fresh, unique epoch.
    pub fn generate() -> Self {
        Epoch(ulid::Ulid::new().to_string())
    }

    /// Wraps an existing epoch value. Returns `None` for an empty string,
    /// which on the wire means "no epoch yet".
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (!value.is_empty()).then_some(Epoch(value))
    }

    /// The epoch as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A consumer's position in one producer's feed: the last applied `seq` and
/// the epoch it belongs to (`None` before the first subscribe).
///
/// ```
/// use z_event::{Cursor, Epoch, Seq};
/// let c = Cursor::start().with_epoch(Epoch::new("e1").unwrap()).next(Seq(7));
/// assert_eq!(c.seq, Seq(7));
/// assert_eq!(c.epoch.unwrap().as_str(), "e1");
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Cursor {
    /// Last applied (or skipped) position.
    pub seq: Seq,
    /// Epoch of the feed this position belongs to.
    pub epoch: Option<Epoch>,
}

impl Cursor {
    /// The initial cursor: nothing applied, epoch unknown.
    pub const fn start() -> Self {
        Cursor {
            seq: Seq::ZERO,
            epoch: None,
        }
    }

    /// The same cursor moved to `seq`.
    pub fn next(&self, seq: Seq) -> Cursor {
        Cursor {
            seq,
            epoch: self.epoch.clone(),
        }
    }

    /// The same cursor with `epoch`.
    pub fn with_epoch(self, epoch: Epoch) -> Cursor {
        Cursor {
            seq: self.seq,
            epoch: Some(epoch),
        }
    }
}

/// Identifies one subscription's cursor and dead letters: which consumer
/// reads which producer. One service may run several consumers against the
/// same producer, each with its own position.
///
/// ```
/// use z_event::CursorKey;
/// let key = CursorKey::new("billing", "orders");
/// assert_eq!(key.to_string(), "billing<-orders");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CursorKey {
    /// The consumer name (as authenticated by the transport).
    pub consumer: String,
    /// The producer name.
    pub producer: String,
}

impl CursorKey {
    /// A key for `consumer` reading `producer`.
    pub fn new(consumer: impl Into<String>, producer: impl Into<String>) -> Self {
        CursorKey {
            consumer: consumer.into(),
            producer: producer.into(),
        }
    }
}

impl fmt::Display for CursorKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}<-{}", self.consumer, self.producer)
    }
}

/// The current end of a producer's feed.
///
/// ```
/// use z_event::{Epoch, FeedHead, Seq};
/// let head = FeedHead { seq: Seq(10), epoch: Epoch::generate(), purged_through: Seq(4) };
/// // A consumer at seq 3 can no longer continue from the feed.
/// assert!(Seq(3) < head.purged_through);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedHead {
    /// The highest sequenced position (`Seq::ZERO` for an empty feed).
    pub seq: Seq,
    /// The feed's current epoch.
    pub epoch: Epoch,
    /// Every event with `seq <= purged_through` was deleted by retention.
    /// A consumer whose cursor is below it cannot continue from the feed.
    pub purged_through: Seq,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_orders_and_steps() {
        assert!(Seq(1) < Seq(2));
        assert_eq!(Seq::ZERO.next(), Seq(1));
        assert_eq!(Seq(7).to_string(), "7");
    }

    #[test]
    fn empty_epoch_is_none() {
        assert_eq!(Epoch::new(""), None);
        assert_eq!(Epoch::new("e1").map(|e| e.to_string()), Some("e1".into()));
        assert_ne!(Epoch::generate(), Epoch::generate());
    }

    #[test]
    fn cursor_next_keeps_epoch() {
        let c = Cursor::start().with_epoch(Epoch::new("e").unwrap());
        let n = c.next(Seq(5));
        assert_eq!(n.seq, Seq(5));
        assert_eq!(n.epoch, c.epoch);
    }
}
