//! Applying events: dedup, effect + cursor in one transaction, retries,
//! parking per aggregate, and replay of parked events.

use std::collections::BTreeSet;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::{Inner, Outcome, store_error};
use crate::error::{HandlerError, SubscriberError};
use crate::ports::{CursorStore, Handler, Transport};
use crate::telemetry;
use crate::types::{DeadLetter, Event, ParkedFilter, Seq};

/// Result of [`Subscriber::replay_parked`](crate::Subscriber::replay_parked).
///
/// ```
/// use z_event::{CursorStore, Handler, ParkedFilter, Subscriber, SubscriberError, Transport};
///
/// async fn replay<S: CursorStore, T: Transport, H: Handler<S>>(
///     sub: &Subscriber<S, T, H>,
/// ) -> Result<(), SubscriberError> {
///     let report = sub.replay_parked(ParkedFilter::unresolved().aggregate("orders/o17")).await?;
///     println!("replayed {}, still blocked: {:?}", report.replayed, report.blocked_aggregates);
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplayReport {
    /// Events applied and resolved.
    pub replayed: u32,
    /// Events that failed again, with the error.
    pub failed: Vec<(Seq, String)>,
    /// Aggregates whose remaining events stay parked.
    pub blocked_aggregates: Vec<String>,
}

impl<S, T, H> Inner<S, T, H>
where
    S: CursorStore,
    T: Transport,
    H: Handler<S>,
{
    /// Applies events that arrived together. With one event, or after any
    /// failure, falls back to applying them one at a time.
    pub(super) async fn apply_batch(
        &self,
        batch: &[Event],
        committed: &mut Seq,
        cancel: &CancellationToken,
    ) -> Result<(), Outcome> {
        if batch.len() > 1 {
            match self.try_batch(batch, committed).await {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(o) => return Err(o),
            }
        }
        for ev in batch {
            self.apply_one(ev, committed, cancel).await?;
        }
        Ok(())
    }

    /// One transaction for the whole batch. `Ok(false)` means "roll back and
    /// go one by one".
    async fn try_batch(&self, batch: &[Event], committed: &mut Seq) -> Result<bool, Outcome> {
        let retry = |e: S::Error| Outcome::Retry(format!("store: {e}"));
        let key = &self.cfg.key;
        let started = Instant::now();
        let mut tx = self.store.begin().await.map_err(retry)?;
        let cursor = self.store.lock_cursor(&mut tx, key).await.map_err(retry)?;
        if cursor.seq < *committed {
            return Err(Outcome::Reconnect);
        }
        let mut applied = 0u64;
        let mut parked_any = false;
        let mut last = cursor.seq;
        for ev in batch.iter().filter(|e| e.seq > cursor.seq) {
            if self
                .store
                .is_parked(&mut tx, key, &ev.aggregate)
                .await
                .map_err(retry)?
            {
                self.store
                    .park(&mut tx, &DeadLetter::behind(key, ev))
                    .await
                    .map_err(retry)?;
                parked_any = true;
            } else if self.handler.handle(&mut tx, ev).await.is_err() {
                return Ok(false);
            } else {
                applied += 1;
            }
            last = ev.seq;
        }
        if last == cursor.seq {
            *committed = cursor.seq;
            return Ok(true);
        }
        self.store
            .advance(&mut tx, key, cursor.next(last))
            .await
            .map_err(retry)?;
        if self.store.commit(tx).await.is_err() {
            return Ok(false);
        }
        *committed = last;
        metrics::counter!(telemetry::EVENTS_APPLIED_TOTAL, self.labels.clone()).increment(applied);
        metrics::histogram!(telemetry::APPLY_SECONDS, self.labels.clone())
            .record(started.elapsed().as_secs_f64());
        if let Some(ev) = batch.last() {
            self.record_lag(ev);
        }
        if parked_any {
            self.refresh_dead_letters().await;
        }
        Ok(true)
    }

    /// Applies one event exactly once: retry in place, then park.
    pub(super) async fn apply_one(
        &self,
        ev: &Event,
        committed: &mut Seq,
        cancel: &CancellationToken,
    ) -> Result<(), Outcome> {
        let span = tracing::debug_span!(
            "z_event.apply",
            seq = ev.seq.get(),
            event_id = %ev.id,
            event_type = %ev.event_type,
            aggregate = %ev.aggregate,
            correlation_id = ev.correlation_id.as_deref().unwrap_or_default(),
        );
        self.apply_with_retries(ev, committed, cancel)
            .instrument(span)
            .await
    }

    async fn apply_with_retries(
        &self,
        ev: &Event,
        committed: &mut Seq,
        cancel: &CancellationToken,
    ) -> Result<(), Outcome> {
        let retry = |e: S::Error| Outcome::Retry(format!("store: {e}"));
        let key = &self.cfg.key;
        let mut attempts = 1u32;
        loop {
            let started = Instant::now();
            let mut tx = self.store.begin().await.map_err(retry)?;
            let cursor = self.store.lock_cursor(&mut tx, key).await.map_err(retry)?;
            if cursor.seq < *committed {
                // Moved back underneath us (reset elsewhere): start over.
                return Err(Outcome::Reconnect);
            }
            if ev.seq <= cursor.seq {
                // Already applied (a resend after reconnect, or another
                // replica): skip. Dropping the transaction rolls it back.
                *committed = cursor.seq;
                return Ok(());
            }
            let parked = self
                .store
                .is_parked(&mut tx, key, &ev.aggregate)
                .await
                .map_err(retry)?;
            let outcome = if parked {
                self.store
                    .park(&mut tx, &DeadLetter::behind(key, ev))
                    .await
                    .map_err(retry)?;
                Ok(())
            } else {
                self.handler.handle(&mut tx, ev).await
            };
            let failure = match outcome {
                Ok(()) => {
                    self.store
                        .advance(&mut tx, key, cursor.next(ev.seq))
                        .await
                        .map_err(retry)?;
                    match self.store.commit(tx).await {
                        Ok(()) => {
                            *committed = ev.seq;
                            if parked {
                                tracing::debug!("parked behind an earlier event of the aggregate");
                                self.refresh_dead_letters().await;
                            } else {
                                metrics::counter!(
                                    telemetry::EVENTS_APPLIED_TOTAL,
                                    self.labels.clone()
                                )
                                .increment(1);
                                metrics::histogram!(telemetry::APPLY_SECONDS, self.labels.clone())
                                    .record(started.elapsed().as_secs_f64());
                                self.record_lag(ev);
                            }
                            return Ok(());
                        }
                        Err(e) => HandlerError::transient(e),
                    }
                }
                Err(e) => {
                    drop(tx);
                    e
                }
            };

            let kind = if failure.is_permanent() {
                "permanent"
            } else {
                "transient"
            };
            let mut labels = self.labels.clone();
            labels.push(metrics::Label::new("kind", kind));
            metrics::counter!(telemetry::HANDLER_ERRORS_TOTAL, labels).increment(1);

            if failure.is_permanent() || attempts >= self.cfg.retry.max_attempts {
                return self.park_failed(ev, &failure, attempts, committed).await;
            }
            let delay = self.cfg.retry.backoff.delay(attempts);
            tracing::warn!(error = %failure, attempts, ?delay, "handler failed; retrying");
            tokio::select! {
                _ = cancel.cancelled() => return Err(Outcome::Shutdown),
                _ = tokio::time::sleep(delay) => {}
            }
            attempts += 1;
        }
    }

    /// Parks a failed event and moves the cursor past it, so other
    /// aggregates keep flowing.
    async fn park_failed(
        &self,
        ev: &Event,
        failure: &HandlerError,
        attempts: u32,
        committed: &mut Seq,
    ) -> Result<(), Outcome> {
        let retry = |e: S::Error| Outcome::Retry(format!("store: {e}"));
        let key = &self.cfg.key;
        let mut tx = self.store.begin().await.map_err(retry)?;
        let cursor = self.store.lock_cursor(&mut tx, key).await.map_err(retry)?;
        if cursor.seq < *committed {
            return Err(Outcome::Reconnect);
        }
        if ev.seq <= cursor.seq {
            *committed = cursor.seq;
            return Ok(());
        }
        self.store
            .park(&mut tx, &DeadLetter::failed(key, ev, failure, attempts))
            .await
            .map_err(retry)?;
        self.store
            .advance(&mut tx, key, cursor.next(ev.seq))
            .await
            .map_err(retry)?;
        self.store.commit(tx).await.map_err(retry)?;
        *committed = ev.seq;
        tracing::error!(error = %failure, attempts, "event parked");
        self.refresh_dead_letters().await;
        Ok(())
    }

    fn record_lag(&self, ev: &Event) {
        let lag = (time::OffsetDateTime::now_utc() - ev.occurred_at).as_seconds_f64();
        metrics::gauge!(telemetry::CONSUMER_LAG_SECONDS, self.labels.clone()).set(lag.max(0.0));
    }

    async fn refresh_dead_letters(&self) {
        if let Ok(open) = self
            .store
            .parked(&self.cfg.key, &ParkedFilter::unresolved())
            .await
        {
            metrics::gauge!(telemetry::DEAD_LETTERS, self.labels.clone()).set(open.len() as f64);
        }
    }

    pub(super) async fn replay_parked(
        &self,
        filter: ParkedFilter,
    ) -> Result<ReplayReport, SubscriberError> {
        let key = &self.cfg.key;
        let filter = ParkedFilter {
            include_resolved: false,
            ..filter
        };
        let mut report = ReplayReport::default();
        let mut blocked: BTreeSet<String> = BTreeSet::new();
        // Live delivery may park more events behind an aggregate while we
        // replay it; repeat until a round finds nothing new.
        for _round in 0..10 {
            let letters = self.store.parked(key, &filter).await.map_err(store_error)?;
            let mut progressed = false;
            for letter in letters {
                if blocked.contains(letter.aggregate()) {
                    continue;
                }
                let mut tx = self.store.begin().await.map_err(store_error)?;
                // Serialises with live delivery of this subscription.
                self.store
                    .lock_cursor(&mut tx, key)
                    .await
                    .map_err(store_error)?;
                match self.handler.handle(&mut tx, &letter.event).await {
                    Ok(()) => {
                        self.store
                            .resolve(&mut tx, key, letter.seq())
                            .await
                            .map_err(store_error)?;
                        self.store.commit(tx).await.map_err(store_error)?;
                        report.replayed += 1;
                        progressed = true;
                    }
                    Err(e) => {
                        drop(tx);
                        let mut tx = self.store.begin().await.map_err(store_error)?;
                        let mut again = DeadLetter::failed(
                            key,
                            &letter.event,
                            &e,
                            letter.attempts.saturating_add(1),
                        );
                        again.parked_at = letter.parked_at;
                        self.store
                            .park(&mut tx, &again)
                            .await
                            .map_err(store_error)?;
                        self.store.commit(tx).await.map_err(store_error)?;
                        tracing::warn!(seq = %letter.seq(), error = %e, "replay failed");
                        report.failed.push((letter.seq(), e.to_string()));
                        blocked.insert(letter.aggregate().to_owned());
                    }
                }
            }
            if !progressed {
                break;
            }
        }
        report.blocked_aggregates = blocked.into_iter().collect();
        self.refresh_dead_letters().await;
        Ok(report)
    }
}
