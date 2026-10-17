//! The storage conformance suite.
//!
//! Every case is a public function taking the [`StoreFactory`], so an
//! adapter can run them individually; [`run_all`] runs them in sequence,
//! and [`storage_suite!`](crate::storage_suite) generates one test per case.

mod delivery;
mod ports;

pub use delivery::{
    batched_apply_exactly_once, batched_apply_poison_falls_back, bootstrap_beyond_retention,
    bootstrap_new_consumer, checkpoint_skips_filtered, concurrent_subscribers_same_consumer,
    consumer_outage_exactly_once, crash_mid_apply, duplicate_after_reconnect_skipped,
    epoch_learned_on_first_subscribe, epoch_mismatch_detected, poison_park_replay,
    reset_cursor_replays, sequencer_failover, start_below_retention_rejected,
    transient_retry_then_park,
};
pub use ports::{
    commit_order_gap, cursor_lock_exclusive, cursor_round_trip, envelope_round_trip,
    epoch_rotation, gapless_and_immutable, lease_exclusive, notifications_wake,
    only_committed_sequenced, parking_visibility, purge_respects_acks, read_contract,
};

use crate::factory::StoreFactory;

/// Names of every storage case, in the order [`run_all`] runs them.
pub const CASES: &[&str] = &[
    "only_committed_sequenced",
    "read_contract",
    "envelope_round_trip",
    "lease_exclusive",
    "cursor_lock_exclusive",
    "cursor_round_trip",
    "parking_visibility",
    "purge_respects_acks",
    "epoch_rotation",
    "notifications_wake",
    "commit_order_gap",
    "gapless_and_immutable",
    "consumer_outage_exactly_once",
    "duplicate_after_reconnect_skipped",
    "concurrent_subscribers_same_consumer",
    "batched_apply_exactly_once",
    "batched_apply_poison_falls_back",
    "crash_mid_apply",
    "sequencer_failover",
    "poison_park_replay",
    "transient_retry_then_park",
    "checkpoint_skips_filtered",
    "epoch_learned_on_first_subscribe",
    "epoch_mismatch_detected",
    "start_below_retention_rejected",
    "reset_cursor_replays",
    "bootstrap_beyond_retention",
    "bootstrap_new_consumer",
];

/// Runs every storage case against stores from `factory`. Needs a
/// multi-threaded tokio runtime.
pub async fn run_all<F: StoreFactory>(factory: F) {
    let f = &factory;
    only_committed_sequenced(f).await;
    read_contract(f).await;
    envelope_round_trip(f).await;
    lease_exclusive(f).await;
    cursor_lock_exclusive(f).await;
    cursor_round_trip(f).await;
    parking_visibility(f).await;
    purge_respects_acks(f).await;
    epoch_rotation(f).await;
    notifications_wake(f).await;
    commit_order_gap(f).await;
    gapless_and_immutable(f).await;
    consumer_outage_exactly_once(f).await;
    duplicate_after_reconnect_skipped(f).await;
    concurrent_subscribers_same_consumer(f).await;
    batched_apply_exactly_once(f).await;
    batched_apply_poison_falls_back(f).await;
    crash_mid_apply(f).await;
    sequencer_failover(f).await;
    poison_park_replay(f).await;
    transient_retry_then_park(f).await;
    checkpoint_skips_filtered(f).await;
    epoch_learned_on_first_subscribe(f).await;
    epoch_mismatch_detected(f).await;
    start_below_retention_rejected(f).await;
    reset_cursor_replays(f).await;
    bootstrap_beyond_retention(f).await;
    bootstrap_new_consumer(f).await;
}
