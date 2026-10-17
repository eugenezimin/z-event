//! Bootstrapping a consumer from the producer's snapshot: for a new
//! consumer, or one whose cursor fell below the producer's retention window.

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::{Inner, Outcome};
use crate::error::SubscriberError;
use crate::ports::{CursorStore, Handler, Transport};
use crate::types::{Cursor, Event, FeedMessage, Seq, SnapshotRequest};

impl<S, T, H> Inner<S, T, H>
where
    S: CursorStore,
    T: Transport,
    H: Handler<S>,
{
    /// Applies the snapshot's events (each in its own transaction), then
    /// moves the cursor to the snapshot's checkpoint. Snapshot events are
    /// upserts of current state, so a bootstrap that is interrupted and run
    /// again is harmless if the handler honours `aggregate_version`.
    pub(super) async fn bootstrap_from(
        &self,
        aggregate_types: Vec<String>,
        cancel: &CancellationToken,
    ) -> Result<Seq, Outcome> {
        let key = &self.cfg.key;
        let span = tracing::info_span!("z_event.bootstrap", producer = %key.producer, consumer = %key.consumer);
        async move {
            let request = SnapshotRequest {
                consumer: key.consumer.clone(),
                aggregate_types,
            };
            let mut messages = tokio::select! {
                _ = cancel.cancelled() => return Err(Outcome::Shutdown),
                r = self.transport.snapshot(&key.producer, request) => {
                    r.map_err(|e| self.transport_failure(e, None))?
                }
            };
            let mut applied = 0u64;
            while let Some(message) = messages.next().await {
                match message.map_err(|e| self.transport_failure(e, None))? {
                    FeedMessage::Event(ev) => {
                        self.apply_snapshot_event(&ev, cancel).await?;
                        applied += 1;
                    }
                    FeedMessage::Checkpoint(cp) => {
                        let to = Cursor {
                            seq: cp.seq,
                            epoch: Some(cp.epoch),
                        };
                        self.store
                            .reset_cursor(key, to)
                            .await
                            .map_err(|e| Outcome::Retry(format!("store: {e}")))?;
                        tracing::info!(events = applied, seq = %cp.seq, "bootstrapped from the producer's snapshot");
                        return Ok(cp.seq);
                    }
                }
            }
            Err(Outcome::Retry("the snapshot ended without a checkpoint".into()))
        }
        .instrument(span)
        .await
    }

    async fn apply_snapshot_event(
        &self,
        ev: &Event,
        cancel: &CancellationToken,
    ) -> Result<(), Outcome> {
        let retry = |e: S::Error| Outcome::Retry(format!("store: {e}"));
        let mut attempts = 1u32;
        loop {
            let mut tx = self.store.begin().await.map_err(retry)?;
            let failure = match self.handler.handle(&mut tx, ev).await {
                Ok(()) => match self.store.commit(tx).await {
                    Ok(()) => return Ok(()),
                    Err(e) => crate::HandlerError::transient(e),
                },
                Err(e) => e,
            };
            if failure.is_permanent() || attempts >= self.cfg.retry.max_attempts {
                // Snapshot events share one position, so they cannot be
                // parked; stop and let an operator fix the cause.
                return Err(Outcome::Fatal(SubscriberError::Rejected(format!(
                    "snapshot event {} of {} failed: {failure}",
                    ev.event_type, ev.aggregate
                ))));
            }
            tokio::select! {
                _ = cancel.cancelled() => return Err(Outcome::Shutdown),
                _ = tokio::time::sleep(self.cfg.retry.backoff.delay(attempts)) => {}
            }
            attempts += 1;
        }
    }
}
