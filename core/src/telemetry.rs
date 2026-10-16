//! Metric names (emitted through the [`metrics`] facade) and tracing span
//! names. Install any `metrics` recorder (e.g. a Prometheus exporter) in
//! the host to collect them.
//!
//! | Metric | Kind | Labels | Suggested alert |
//! |---|---|---|---|
//! | `feed_head_seq` | gauge | producer | |
//! | `feed_unsequenced_rows` | gauge | producer | > 0 for more than 10 s (sequencer stuck) |
//! | `feed_sequencer_leader` | gauge | producer | no replica at 1 |
//! | `feed_appended_total` | counter | producer | |
//! | `feed_purged_total` | counter | producer | |
//! | `feed_epoch_rotations_total` | counter | producer | any |
//! | `feed_streams_open` | gauge | producer | |
//! | `feed_consumer_acked_seq` | gauge | producer, consumer | stops moving |
//! | `feed_consumer_lag_events` | gauge | producer, consumer | |
//! | `feed_consumer_lag_seconds` | gauge | producer, consumer | > 60 s for 5 min |
//! | `feed_dead_letters` | gauge | producer, consumer | > 0 |
//! | `feed_stream_reconnects_total` | counter | producer, consumer | reconnect storms |
//! | `feed_epoch_mismatch_total` | counter | producer, consumer | any |
//! | `feed_events_applied_total` | counter | producer, consumer | |
//! | `feed_handler_errors_total` | counter | producer, consumer, kind | |
//! | `feed_apply_seconds` | histogram | producer, consumer | |

/// Highest sequenced position.
pub const HEAD_SEQ: &str = "feed_head_seq";
/// Committed but not yet sequenced events.
pub const UNSEQUENCED_ROWS: &str = "feed_unsequenced_rows";
/// 1 while this replica leads the sequencer.
pub const SEQUENCER_LEADER: &str = "feed_sequencer_leader";
/// Events appended through the producer.
pub const APPENDED_TOTAL: &str = "feed_appended_total";
/// Events deleted by retention.
pub const PURGED_TOTAL: &str = "feed_purged_total";
/// Automatic epoch rotations (a consumer was ahead of the head).
pub const EPOCH_ROTATIONS_TOTAL: &str = "feed_epoch_rotations_total";
/// Open feed streams.
pub const STREAMS_OPEN: &str = "feed_streams_open";
/// A consumer's acked position, as seen by the producer.
pub const CONSUMER_ACKED_SEQ: &str = "feed_consumer_acked_seq";
/// Events between the head and the consumer's cursor.
pub const CONSUMER_LAG_EVENTS: &str = "feed_consumer_lag_events";
/// Now minus `occurred_at` of the last applied event.
pub const CONSUMER_LAG_SECONDS: &str = "feed_consumer_lag_seconds";
/// Unresolved parked events.
pub const DEAD_LETTERS: &str = "feed_dead_letters";
/// Subscriber reconnects.
pub const STREAM_RECONNECTS_TOTAL: &str = "feed_stream_reconnects_total";
/// Subscribers stopped by an epoch mismatch.
pub const EPOCH_MISMATCH_TOTAL: &str = "feed_epoch_mismatch_total";
/// Events applied by handlers.
pub const EVENTS_APPLIED_TOTAL: &str = "feed_events_applied_total";
/// Handler failures (label `kind`: transient or permanent).
pub const HANDLER_ERRORS_TOTAL: &str = "feed_handler_errors_total";
/// Time to apply one event (handler + cursor commit).
pub const APPLY_SECONDS: &str = "feed_apply_seconds";

pub(crate) fn producer_labels(producer: &str) -> Vec<metrics::Label> {
    vec![metrics::Label::new("producer", producer.to_owned())]
}

pub(crate) fn pair_labels(producer: &str, consumer: &str) -> Vec<metrics::Label> {
    vec![
        metrics::Label::new("producer", producer.to_owned()),
        metrics::Label::new("consumer", consumer.to_owned()),
    ]
}
