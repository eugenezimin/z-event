//! [`ConsumerState`]: what a producer knows about a consumer.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::position::Seq;

/// A consumer registered with a producer (on its first `Start`).
/// Retention never deletes events a registered consumer has not acked.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerState {
    /// The consumer name.
    pub consumer: String,
    /// The highest acked position.
    pub acked_seq: Seq,
    /// When it last acked (or registered).
    #[serde(with = "time::serde::rfc3339::option")]
    pub acked_at: Option<OffsetDateTime>,
}
