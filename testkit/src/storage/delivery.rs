//! Delivery cases: the full runtime (producer, sequencer, feed, subscriber)
//! on the store under test, over the in-process transport.

use std::time::Duration;

use z_event::{
    Cursor, CursorStore, OutboxStore, ParkedFilter, Producer, Seq, StopReason, Store, Subscriber,
    SubscriberError,
};

use crate::factory::StoreFactory;
use crate::faults::{
    CrashTiming, CrashingStore, KillPoint, KillingHandler, ReplayingTransport, StreamCutter,
};
use crate::handlers::{EffectHandler, PoisonHandler};
use crate::util::{
    CONSUMER, PRODUCER, World, assert_exactly_once, expect_stop, producer_config, rng, scale,
    wait_until,
};

/// A consumer that was down while 100 events were appended applies each
/// exactly once on restart, then keeps up live; a restart adds nothing.
pub async fn consumer_outage_exactly_once<F: StoreFactory>(f: &F) {
    let w = World::new(f).await;
    w.append_n(100, "OrderConfirmed", "orders").await;
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .spawn();
    w.wait_cursor(100).await;
    w.append_n(50, "OrderConfirmed", "more").await;
    w.wait_cursor(150).await;
    sub.shutdown().await.unwrap();

    let again = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .spawn();
    tokio::time::sleep(Duration::from_millis(300)).await;
    again.shutdown().await.unwrap();
    assert_exactly_once(&w.effects().await, 1..=150);
    w.shutdown().await;
}

/// Resent events after reconnects (the stream is rewound on every
/// reconnect and cut often) are skipped: no duplicate effects.
pub async fn duplicate_after_reconnect_skipped<F: StoreFactory>(f: &F) {
    let w = World::new(f).await;
    let n = 120;
    w.append_n(n, "OrderConfirmed", "orders").await;
    let transport = StreamCutter::new(
        ReplayingTransport::new(w.transport.clone(), 10),
        15,
        rng(2).u64(..),
    );
    // Batches: resent events must also be skipped inside a batch.
    let sub = w
        .subscriber(transport.clone(), w.effect_handler())
        .batch_size(8)
        .spawn();
    w.wait_cursor(n).await;
    sub.shutdown().await.unwrap();
    assert!(transport.cuts() > 0, "streams were cut");
    assert_exactly_once(&w.effects().await, 1..=n);
    w.shutdown().await;
}

/// Two subscribers with the same consumer name (one applying in batches)
/// never apply an event twice.
pub async fn concurrent_subscribers_same_consumer<F: StoreFactory>(f: &F) {
    let w = World::new(f).await;
    let a = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .batch_size(8)
        .spawn();
    let b = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .spawn();
    w.append_n(150, "OrderConfirmed", "orders").await;
    w.wait_cursor(150).await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    assert_exactly_once(&w.effects().await, 1..=150);
    w.shutdown().await;
}

/// Events that arrive together are applied in batches (one transaction per
/// batch) exactly once, and a restart adds nothing.
pub async fn batched_apply_exactly_once<F: StoreFactory>(f: &F) {
    let w = World::new(f).await;
    w.append_n(200, "OrderConfirmed", "orders").await;
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .batch_size(16)
        .spawn();
    w.wait_cursor(200).await;
    sub.shutdown().await.unwrap();
    let again = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .batch_size(16)
        .spawn();
    w.append_n(40, "OrderConfirmed", "live").await;
    w.wait_cursor(240).await;
    again.shutdown().await.unwrap();
    assert_exactly_once(&w.effects().await, 1..=240);
    w.shutdown().await;
}

/// A failing event inside a batch rolls the batch back; the events are
/// re-applied one by one, the failing aggregate is parked (and later
/// events of it behind it), everything else is applied exactly once, and
/// replay resolves the parked events in order.
pub async fn batched_apply_poison_falls_back<F: StoreFactory>(f: &F) {
    let w = World::new(f).await;
    let poisoned = [5u64, 12, 20];
    for seq in 1..=30u64 {
        let aggregate = if poisoned.contains(&seq) {
            "orders/p".to_owned()
        } else {
            format!("orders/{seq}")
        };
        w.append("T", &aggregate).await;
    }
    let handler = PoisonHandler::permanent(w.effect_handler());
    handler.poison("orders/p");
    let sub = w
        .subscriber(w.transport.clone(), handler.clone())
        .batch_size(16)
        .build();
    let running = sub.spawn();
    w.wait_cursor(30).await;
    assert_exactly_once(
        &w.effects().await,
        (1..=30).filter(|s| !poisoned.contains(s)),
    );
    let parked = sub.parked(ParkedFilter::unresolved()).await.unwrap();
    assert_eq!(
        parked.iter().map(|l| l.seq().get()).collect::<Vec<_>>(),
        poisoned
    );
    assert_eq!(parked[0].attempts, 1);
    assert_eq!(parked[1].attempts, 0, "parked behind the first");

    handler.heal();
    let report = sub.replay_parked(ParkedFilter::unresolved()).await.unwrap();
    assert_eq!(report.replayed, 3);
    let effects = w.effects().await;
    assert_eq!(
        &effects[effects.len() - 3..],
        poisoned,
        "replayed in seq order"
    );
    assert_exactly_once(&effects, 1..=30);
    running.shutdown().await.unwrap();
    w.shutdown().await;
}

/// Kills the subscriber at random points (before the handler, after the
/// effect but before the commit, or at a random await point) 1000 times
/// (`ZEVENT_TESTKIT_SCALE` multiplies): effect and cursor never diverge.
pub async fn crash_mid_apply<F: StoreFactory>(f: &F) {
    let w = World::new(f).await;
    let iterations = 1000 * scale();
    let mut rng = rng(3);
    let mut head = 0u64;
    let mut verified_cursor = 0u64;
    let mut verified_effects = 0usize;
    for i in 0..iterations {
        if verified_cursor + 5 >= head {
            w.append_n(20, "OrderConfirmed", &format!("batch{i}")).await;
            head += 20;
        }
        // Kill at a planned handler point, right before or after a random
        // store operation (e.g. just after a commit), or at a random time;
        // half of the runs apply buffered events in batches.
        let batch = if rng.bool() { 1 } else { 8 };
        let (sub, killed) = if rng.bool() {
            let point = if rng.bool() {
                KillPoint::BeforeHandle
            } else {
                KillPoint::AfterEffect
            };
            let handler = KillingHandler::new(w.effect_handler(), rng.u32(0..4), point);
            let killed = handler.killed();
            let sub = w
                .subscriber(w.transport.clone(), handler)
                .batch_size(batch)
                .spawn();
            (sub, killed)
        } else {
            let timing = if rng.bool() {
                CrashTiming::Before
            } else {
                CrashTiming::After
            };
            let store = CrashingStore::new(w.billing.clone(), rng.u32(0..24), timing);
            let killed = store.crashed();
            let sub = Subscriber::builder(
                store.clone(),
                w.transport.clone(),
                EffectHandler::new(store.clone()),
            )
            .consumer(CONSUMER)
            .producer(PRODUCER)
            .retry(crate::util::retry())
            .reconnect(crate::util::reconnect())
            .batch_size(batch)
            .spawn();
            (sub, killed)
        };
        let random_point = Duration::from_micros(rng.u64(0..4000));
        tokio::select! {
            _ = killed.notified() => {}
            _ = tokio::time::sleep(random_point) => {}
        }
        sub.abort_and_wait().await;

        // Effects and cursor must agree exactly.
        let cursor = w.cursor().await.get();
        let effects = w.effects().await;
        let new: Vec<u64> = effects[verified_effects..].to_vec();
        assert_eq!(
            new,
            ((verified_cursor + 1)..=cursor).collect::<Vec<_>>(),
            "iteration {i}: effects after a crash must match the cursor (seed {})",
            crate::util::seed()
        );
        verified_cursor = cursor;
        verified_effects = effects.len();
    }
    // Finally run to completion.
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .spawn();
    w.wait_cursor(head).await;
    sub.shutdown().await.unwrap();
    assert_exactly_once(&w.effects().await, 1..=head);
    w.shutdown().await;
}

/// Two producer replicas on one store: crashing the leader hands the lease
/// over within 5 s, a graceful shutdown within 2 s, and the feed stays
/// gapless.
pub async fn sequencer_failover<F: StoreFactory>(f: &F) {
    let store = f.fresh().await;
    let a = Producer::new(store.clone(), producer_config("orders"));
    let b = Producer::new(store.clone(), producer_config("orders"));
    let mut running_a = Some(a.start());
    let mut running_b = Some(b.start());

    let writer_store = store.clone();
    let writer_producer = a.clone();
    let stop = stop_flag();
    let stop_writer = stop.clone();
    let writer = tokio::spawn(async move {
        let mut n = 0u64;
        while !stop_writer.load(std::sync::atomic::Ordering::SeqCst) {
            let mut tx = writer_store.begin().await.unwrap();
            writer_producer
                .append(
                    &mut tx,
                    z_event::NewEvent::new("T", format!("w/{n}"), 1, Vec::new()),
                )
                .await
                .unwrap();
            writer_store.commit(tx).await.unwrap();
            n += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        n
    });

    let leader_known = || {
        let (a, b) = (running_a.as_ref(), running_b.as_ref());
        a.is_some_and(|h| h.is_leader()) || b.is_some_and(|h| h.is_leader())
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !leader_known() {
        assert!(tokio::time::Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for (round, graceful) in [(0, false), (1, true)] {
        let a_leads = running_a.as_ref().is_some_and(|h| h.is_leader());
        let (dead, survivor) = if a_leads {
            (running_a.take(), &running_b)
        } else {
            (running_b.take(), &running_a)
        };
        let dead = dead.expect("a leader exists");
        let started = tokio::time::Instant::now();
        if graceful {
            dead.shutdown().await;
        } else {
            dead.abort_and_wait().await;
        }
        let head_at_death = store.head().await.unwrap().seq;
        let limit = if graceful {
            Duration::from_secs(2)
        } else {
            Duration::from_secs(5)
        };
        let survivor = survivor.as_ref().expect("a survivor exists");
        loop {
            let head = store.head().await.unwrap().seq;
            if survivor.is_leader() && head > head_at_death {
                break;
            }
            assert!(
                started.elapsed() < limit,
                "round {round}: takeover took longer than {limit:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if round == 0 {
            // Bring a fresh replica back for the graceful round.
            let replica = Producer::new(store.clone(), producer_config("orders"));
            if running_a.is_none() {
                running_a = Some(replica.start());
            } else {
                running_b = Some(replica.start());
            }
        }
    }

    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let written = writer.await.unwrap();
    let orders = store.clone();
    wait_until("all events sequenced", Duration::from_secs(10), || {
        let orders = orders.clone();
        async move { orders.head().await.unwrap().seq == Seq(written) }
    })
    .await;
    let feed = crate::util::read_all(&store, Seq::ZERO, &z_event::TypeFilter::All).await;
    assert_eq!(
        feed.iter().map(|e| e.seq.get()).collect::<Vec<_>>(),
        (1..=written).collect::<Vec<_>>(),
        "gapless after failovers"
    );
    for h in [running_a, running_b].into_iter().flatten() {
        h.shutdown().await;
    }
}

fn stop_flag() -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false))
}

async fn poison_case<F: StoreFactory>(f: &F, permanent: bool) {
    let w = World::new(f).await;
    let handler = if permanent {
        PoisonHandler::permanent(w.effect_handler())
    } else {
        PoisonHandler::transient(w.effect_handler())
    };
    handler.poison("orders/a");
    let sub = w.subscriber(w.transport.clone(), handler.clone()).build();
    let running = sub.spawn();
    for agg in ["orders/a", "orders/b", "orders/a", "orders/b", "orders/a"] {
        w.append("T", agg).await;
    }
    w.wait_cursor(5).await;
    assert_exactly_once(&w.effects().await, [2, 4]);

    let parked = sub.parked(ParkedFilter::unresolved()).await.unwrap();
    assert_eq!(
        parked.iter().map(|l| l.seq().get()).collect::<Vec<_>>(),
        [1, 3, 5]
    );
    let expected_attempts = if permanent {
        1
    } else {
        crate::util::retry().max_attempts
    };
    assert_eq!(parked[0].attempts, expected_attempts);
    assert_eq!(parked[1].attempts, 0, "parked behind the first");

    // A replay that fails again blocks the aggregate.
    let report = sub.replay_parked(ParkedFilter::unresolved()).await.unwrap();
    assert_eq!(report.replayed, 0);
    assert_eq!(report.blocked_aggregates, ["orders/a".to_owned()]);
    assert_eq!(report.failed.len(), 1);

    handler.heal();
    let report = sub.replay_parked(ParkedFilter::unresolved()).await.unwrap();
    assert_eq!(report.replayed, 3);
    assert!(
        sub.parked(ParkedFilter::unresolved())
            .await
            .unwrap()
            .is_empty()
    );
    let effects = w.effects().await;
    assert_eq!(
        effects,
        [2, 4, 1, 3, 5],
        "replay applies the aggregate in seq order"
    );

    w.append("T", "orders/a").await;
    w.wait_cursor(6).await;
    assert_exactly_once(&w.effects().await, 1..=6);
    running.shutdown().await.unwrap();
    w.shutdown().await;
}

/// A permanently failing event is parked at once; later events of the
/// same aggregate park behind it while other aggregates flow; replay
/// applies them in order once the cause is fixed.
pub async fn poison_park_replay<F: StoreFactory>(f: &F) {
    poison_case(f, true).await;
}

/// A transiently failing event is retried up to the budget, then parked.
pub async fn transient_retry_then_park<F: StoreFactory>(f: &F) {
    poison_case(f, false).await;
}

/// A consumer filtering on one type reaches the head through checkpoints
/// even when the tail holds only other types.
pub async fn checkpoint_skips_filtered<F: StoreFactory>(f: &F) {
    let w = World::new(f).await;
    w.append("OrderConfirmed", "orders/1").await;
    w.append_n(120, "OrderNoted", "noise").await;
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .types(["OrderConfirmed"])
        .spawn();
    w.wait_cursor(121).await;
    sub.shutdown().await.unwrap();
    assert_exactly_once(&w.effects().await, [1]);
    w.shutdown().await;
}

/// A fresh consumer learns the feed's epoch from the opening checkpoint,
/// even when the feed is empty.
pub async fn epoch_learned_on_first_subscribe<F: StoreFactory>(f: &F) {
    let w = World::new(f).await;
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .spawn();
    let epoch = w.orders.head().await.unwrap().epoch;
    let billing = w.billing.clone();
    let key = w.key();
    wait_until("cursor epoch recorded", Duration::from_secs(5), || {
        let billing = billing.clone();
        let key = key.clone();
        let epoch = epoch.clone();
        async move { billing.cursor(&key).await.unwrap().epoch == Some(epoch) }
    })
    .await;
    sub.shutdown().await.unwrap();
    w.shutdown().await;
}

/// Epoch mismatches stop the subscriber: a rotation while streaming; a
/// cursor ahead of the head in the current epoch (the producer rotates);
/// a cursor ahead of the head with no epoch (rejected, no rotation).
pub async fn epoch_mismatch_detected<F: StoreFactory>(f: &F) {
    // (a) rotated while streaming.
    let w = World::new(f).await;
    w.append_n(3, "T", "x").await;
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .spawn();
    w.wait_cursor(3).await;
    w.orders.rotate_epoch().await.unwrap();
    let err = expect_stop(sub, StopReason::EpochMismatch).await;
    assert!(
        matches!(err, SubscriberError::EpochMismatch { .. }),
        "{err}"
    );

    // (b) cursor ahead of the head, same epoch: the producer rotates.
    let head = w.orders.head().await.unwrap();
    w.billing
        .reset_cursor(
            &w.key(),
            Cursor::start()
                .with_epoch(head.epoch.clone())
                .next(Seq(head.seq.get() + 10)),
        )
        .await
        .unwrap();
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .spawn();
    let err = expect_stop(sub, StopReason::EpochMismatch).await;
    assert!(
        matches!(err, SubscriberError::EpochMismatch { .. }),
        "{err}"
    );
    assert_ne!(
        w.orders.head().await.unwrap().epoch,
        head.epoch,
        "epoch rotated"
    );

    // (c) cursor ahead of the head without an epoch: rejected, not rotated.
    let head = w.orders.head().await.unwrap();
    w.billing
        .reset_cursor(&w.key(), Cursor::start().next(Seq(head.seq.get() + 10)))
        .await
        .unwrap();
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .spawn();
    expect_stop(sub, StopReason::EpochMismatch).await;
    assert_eq!(
        w.orders.head().await.unwrap().epoch,
        head.epoch,
        "not rotated"
    );
    w.shutdown().await;
}

/// A consumer whose cursor is below the retention window is stopped
/// instead of silently skipping purged events.
pub async fn start_below_retention_rejected<F: StoreFactory>(f: &F) {
    let w = World::new(f).await;
    w.append_n(10, "T", "x").await;
    w.wait_head(10).await;
    let later = time::OffsetDateTime::now_utc() + time::Duration::minutes(5);
    assert_eq!(w.orders.purge(later).await.unwrap(), 9);
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .spawn();
    let err = expect_stop(sub, StopReason::BeyondRetention).await;
    assert!(
        matches!(err, SubscriberError::BeyondRetention { .. }),
        "{err}"
    );
    assert!(w.effects().await.is_empty());

    // From the retention boundary it works.
    let epoch = w.orders.head().await.unwrap().epoch;
    w.billing
        .reset_cursor(&w.key(), Cursor::start().with_epoch(epoch).next(Seq(9)))
        .await
        .unwrap();
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .spawn();
    w.wait_cursor(10).await;
    sub.shutdown().await.unwrap();
    assert_exactly_once(&w.effects().await, [10]);
    w.shutdown().await;
}

/// `reset_cursor` on a running subscriber replays from the new position.
pub async fn reset_cursor_replays<F: StoreFactory>(f: &F) {
    let w = World::new(f).await;
    w.append_n(10, "T", "x").await;
    let sub = w
        .subscriber(w.transport.clone(), w.effect_handler())
        .build();
    let running = sub.spawn();
    w.wait_cursor(10).await;
    let epoch = w.orders.head().await.unwrap().epoch;
    sub.reset_cursor(Cursor::start().with_epoch(epoch).next(Seq(7)))
        .await
        .unwrap();
    let effects_target = 13;
    let billing = w.billing.clone();
    wait_until("replayed effects", Duration::from_secs(10), || {
        let billing = billing.clone();
        async move { crate::util::effects(&billing).await.len() >= effects_target }
    })
    .await;
    running.shutdown().await.unwrap();
    let effects = w.effects().await;
    assert_eq!(
        &effects[10..],
        [8, 9, 10],
        "events after the reset position are replayed"
    );
    w.shutdown().await;
}
