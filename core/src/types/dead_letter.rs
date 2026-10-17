//! Parked events: [`DeadLetter`] and [`ParkedFilter`].

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::event::Event;
use super::position::{CursorKey, Seq};
use crate::error::HandlerError;

/// An event the consumer could not apply (or that waits behind one that
/// could not be applied), stored in the consumer's storage.
///
/// ```
/// use z_event::{CursorKey, DeadLetter, HandlerError, NewEvent, Seq};
/// let key = CursorKey::new("billing", "orders");
/// let first = NewEvent::new("T", "orders/o1", 1, "{}").into_event(Seq(3));
/// let later = NewEvent::new("T", "orders/o1", 2, "{}").into_event(Seq(5));
/// let failed = DeadLetter::failed(&key, &first, &HandlerError::permanent("bad payload"), 1);
/// let behind = DeadLetter::behind(&key, &later);
/// assert_eq!((failed.seq(), failed.attempts), (Seq(3), 1));
/// assert_eq!(behind.attempts, 0);
/// assert!(!behind.is_resolved());
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    /// Which subscription parked it.
    pub key: CursorKey,
    /// The full envelope.
    pub event: Event,
    /// The last error, or why it was parked behind another event.
    pub error: String,
    /// Handler attempts made (0 for an event parked behind another one).
    pub attempts: u32,
    /// When it was (last) parked.
    #[serde(with = "time::serde::rfc3339")]
    pub parked_at: OffsetDateTime,
    /// When it was applied by a replay, if it was.
    #[serde(with = "time::serde::rfc3339::option")]
    pub resolved_at: Option<OffsetDateTime>,
}

impl DeadLetter {
    /// An event whose handler failed `attempts` times with `error`.
    pub fn failed(key: &CursorKey, event: &Event, error: &HandlerError, attempts: u32) -> Self {
        DeadLetter {
            key: key.clone(),
            event: event.clone(),
            error: error.to_string(),
            attempts,
            parked_at: OffsetDateTime::now_utc(),
            resolved_at: None,
        }
    }

    /// An event parked because an earlier event of the same aggregate is
    /// parked, so it must not overtake it.
    pub fn behind(key: &CursorKey, event: &Event) -> Self {
        DeadLetter {
            key: key.clone(),
            event: event.clone(),
            error: format!("parked behind an earlier event of {}", event.aggregate),
            attempts: 0,
            parked_at: OffsetDateTime::now_utc(),
            resolved_at: None,
        }
    }

    /// The parked event's position.
    pub fn seq(&self) -> Seq {
        self.event.seq
    }

    /// The parked event's aggregate.
    pub fn aggregate(&self) -> &str {
        &self.event.aggregate
    }

    /// Whether a replay applied it.
    pub fn is_resolved(&self) -> bool {
        self.resolved_at.is_some()
    }
}

/// Selects parked events.
///
/// ```
/// use z_event::{ParkedFilter, Seq};
/// let f = ParkedFilter {
///     from_seq: Some(Seq(100)),
///     limit: Some(50),
///     ..ParkedFilter::unresolved().aggregate("orders/o1")
/// };
/// assert!(!f.include_resolved);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParkedFilter {
    /// Only this aggregate.
    pub aggregate: Option<String>,
    /// Only `seq >= from_seq`.
    pub from_seq: Option<Seq>,
    /// Only `seq <= to_seq`.
    pub to_seq: Option<Seq>,
    /// Include events already resolved by a replay.
    pub include_resolved: bool,
    /// At most this many.
    pub limit: Option<u32>,
}

impl ParkedFilter {
    /// Unresolved events only, no other restriction.
    pub fn unresolved() -> Self {
        ParkedFilter::default()
    }

    /// Only this aggregate.
    pub fn aggregate(mut self, aggregate: impl Into<String>) -> Self {
        self.aggregate = Some(aggregate.into());
        self
    }

    /// Whether `letter` passes the filter (ignoring `limit`).
    pub fn matches(&self, letter: &DeadLetter) -> bool {
        (self.include_resolved || !letter.is_resolved())
            && self
                .aggregate
                .as_deref()
                .is_none_or(|a| a == letter.aggregate())
            && self.from_seq.is_none_or(|s| letter.seq() >= s)
            && self.to_seq.is_none_or(|s| letter.seq() <= s)
    }
}
