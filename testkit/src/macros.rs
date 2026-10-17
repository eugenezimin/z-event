//! Macros generating one `#[tokio::test]` per conformance case. The calling
//! crate needs `tokio` (with the `macros` and `rt-multi-thread` features)
//! as a dev-dependency.

/// Generates a `storage_conformance` module with one multi-threaded
/// `#[tokio::test]` per storage case, each calling the case with
/// `$factory` (a [`StoreFactory`](crate::StoreFactory), typically a
/// closure `|| async { … }`).
///
/// ```ignore
/// z_event_testkit::storage_suite!(|| async { z_event_memory::MemoryStore::new() });
/// ```
#[macro_export]
macro_rules! storage_suite {
    ($factory:expr) => {
        $crate::__cases!(storage_conformance, storage, (&$factory); [
            only_committed_sequenced, read_contract, envelope_round_trip, lease_exclusive,
            cursor_lock_exclusive, cursor_round_trip, parking_visibility, purge_respects_acks,
            epoch_rotation, notifications_wake, commit_order_gap, gapless_and_immutable,
            consumer_outage_exactly_once, duplicate_after_reconnect_skipped,
            concurrent_subscribers_same_consumer, batched_apply_exactly_once,
            batched_apply_poison_falls_back, crash_mid_apply, sequencer_failover,
            poison_park_replay, transient_retry_then_park, checkpoint_skips_filtered,
            epoch_learned_on_first_subscribe, epoch_mismatch_detected,
            start_below_retention_rejected, reset_cursor_replays
        ]);
    };
}

/// Generates a `transport_conformance` module with one multi-threaded
/// `#[tokio::test]` per transport case, each calling the case with
/// `$factory` (stores) and `$harness` (a
/// [`TransportHarness`](crate::TransportHarness)).
///
/// ```ignore
/// z_event_testkit::transport_suite!(
///     || async { z_event_memory::MemoryStore::new() },
///     z_event_testkit::InProcessHarness
/// );
/// ```
#[macro_export]
macro_rules! transport_suite {
    ($factory:expr, $harness:expr) => {
        $crate::__cases!(transport_conformance, transport, (&$factory, &$harness); [
            network_drop_loop, identity_enforced, acl_enforced, backpressure_slow_consumer,
            heartbeats, reconnect_with_backoff, acks_reach_producer, ordered_delivery,
            head_and_snapshot
        ]);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __cases {
    ($module:ident, $suite:ident, $args:tt; [$($case:ident),* $(,)?]) => {
        mod $module {
            #[allow(unused_imports)]
            use super::*;
            $(
                #[::tokio::test(flavor = "multi_thread", worker_threads = 4)]
                async fn $case() {
                    $crate::$suite::$case $args .await;
                }
            )*
        }
    };
}
