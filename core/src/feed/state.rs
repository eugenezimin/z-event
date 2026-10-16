//! The per-stream state machine: initial checkpoint, backlog in batches,
//! skip checkpoints over filtered ranges, live push, heartbeats.
//!
//! The stream is pull-based: nothing runs unless the transport polls it, so
//! the transport's flow control is the backpressure, and dropping the
//! stream cancels it.

use std::collections::VecDeque;

use futures::stream;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::time::Instant;

use super::{FeedSource, FeedStream, unavailable};
use crate::error::FeedError;
use crate::ports::OutboxStore;
use crate::telemetry;
use crate::types::{
    Checkpoint, Epoch, Event, FeedHead, FeedMessage, Notification, PeerId, Seq, Start, TypeFilter,
};

/// Keeps `feed_streams_open` accurate, including when a stream is dropped.
struct OpenGauge(Vec<metrics::Label>);

impl OpenGauge {
    fn new(producer: &str) -> Self {
        let labels = telemetry::producer_labels(producer);
        metrics::gauge!(telemetry::STREAMS_OPEN, labels.clone()).increment(1.0);
        OpenGauge(labels)
    }
}

impl Drop for OpenGauge {
    fn drop(&mut self) {
        metrics::gauge!(telemetry::STREAMS_OPEN, self.0.clone()).decrement(1.0);
    }
}

pub(super) struct FeedState<S: OutboxStore> {
    source: FeedSource<S>,
    peer: PeerId,
    filter: TypeFilter,
    /// No explicit types were requested: filter every event through the ACL.
    acl_per_event: bool,
    epoch: Epoch,
    /// Every matching event `<= pos` was emitted or is in `buf`.
    pos: Seq,
    /// Highest head we know of.
    known_head: Seq,
    buf: VecDeque<Event>,
    /// `seq` of the last emitted event or checkpoint.
    last_emitted: Seq,
    last_sent: Instant,
    rx: broadcast::Receiver<Notification>,
    opened: bool,
    done: bool,
    _gauge: OpenGauge,
}

type Step<S> = Option<(Result<FeedMessage, FeedError>, FeedState<S>)>;

impl<S: OutboxStore> FeedState<S> {
    pub(super) fn start(
        source: FeedSource<S>,
        peer: PeerId,
        start: Start,
        head: FeedHead,
        rx: broadcast::Receiver<Notification>,
    ) -> Self {
        let gauge = OpenGauge::new(source.producer());
        FeedState {
            filter: start.filter(),
            acl_per_event: start.types.is_empty(),
            epoch: head.epoch,
            pos: start.from_seq,
            known_head: head.seq,
            buf: VecDeque::new(),
            last_emitted: start.from_seq,
            last_sent: Instant::now(),
            rx,
            opened: false,
            done: false,
            _gauge: gauge,
            source,
            peer,
        }
    }

    pub(super) fn into_stream(self) -> FeedStream {
        Box::pin(stream::unfold(self, Self::step))
    }

    fn checkpoint(&mut self, seq: Seq, head: Seq) -> FeedMessage {
        self.last_emitted = seq;
        self.last_sent = Instant::now();
        FeedMessage::Checkpoint(Checkpoint {
            seq,
            head,
            epoch: self.epoch.clone(),
        })
    }

    fn fail(mut self, e: FeedError) -> Step<S> {
        self.done = true;
        Some((Err(e), self))
    }

    /// Reads the head and ends the stream if the epoch changed under it.
    async fn refresh_head(&mut self) -> Result<FeedHead, FeedError> {
        let head = self
            .source
            .producer
            .store
            .head()
            .await
            .map_err(unavailable)?;
        if head.epoch != self.epoch {
            return Err(FeedError::EpochMismatch {
                producer_epoch: head.epoch,
                consumer_epoch: Some(self.epoch.clone()),
            });
        }
        if head.seq > self.known_head {
            self.known_head = head.seq;
        }
        Ok(head)
    }

    async fn step(mut self) -> Step<S> {
        if self.done {
            return None;
        }
        if !self.opened {
            self.opened = true;
            let msg = self.checkpoint(self.pos, self.known_head);
            return Some((Ok(msg), self));
        }
        loop {
            if let Some(ev) = self.buf.pop_front() {
                self.last_emitted = ev.seq;
                self.last_sent = Instant::now();
                return Some((Ok(FeedMessage::Event(ev)), self));
            }
            if self.pos > self.last_emitted {
                // Skip marker: the range up to `pos` had no (more) matches.
                let msg = self.checkpoint(self.pos, self.known_head);
                return Some((Ok(msg), self));
            }
            if self.pos < self.known_head {
                if let Err(e) = self.read_batch().await {
                    return self.fail(e);
                }
                continue;
            }

            // Caught up: wait for a sequenced batch or the heartbeat.
            let heartbeat_at = self.last_sent + self.source.producer.cfg.feed.heartbeat;
            tokio::select! {
                n = self.rx.recv() => match n {
                    Ok(Notification::Sequenced(head)) => {
                        if head > self.known_head {
                            self.known_head = head;
                        }
                    }
                    Ok(Notification::Appended) => {}
                    Ok(Notification::Lagged) | Err(RecvError::Lagged(_)) => {
                        if let Err(e) = self.refresh_head().await {
                            return self.fail(e);
                        }
                    }
                    Err(RecvError::Closed) => {
                        return self.fail(FeedError::Unavailable("producer stopped".into()));
                    }
                },
                _ = tokio::time::sleep_until(heartbeat_at) => {
                    match self.refresh_head().await {
                        Err(e) => return self.fail(e),
                        Ok(head) if head.seq > self.pos => {}
                        Ok(head) => {
                            let msg = self.checkpoint(self.pos, head.seq);
                            return Some((Ok(msg), self));
                        }
                    }
                }
            }
        }
    }

    /// Reads the next batch after `pos` into `buf` and moves `pos` to the
    /// end of the scanned range.
    async fn read_batch(&mut self) -> Result<(), FeedError> {
        let head = self.refresh_head().await?;
        let limit = self.source.producer.cfg.feed.backlog_batch.max(1);
        let batch = self
            .source
            .producer
            .store
            .read(self.pos, &self.filter, limit)
            .await
            .map_err(unavailable)?;
        let last = batch.last().map(|e| e.seq);
        // A short batch contains every match up to the head read before it,
        // because everything <= head was already visible.
        let scanned_to = if batch.len() < limit as usize {
            last.map_or(head.seq, |l| l.max(head.seq))
        } else {
            last.unwrap_or(head.seq)
        };
        let acl = self.source.acl.clone();
        self.buf.extend(
            batch
                .into_iter()
                .filter(|e| !self.acl_per_event || acl.allows(&self.peer, &e.event_type)),
        );
        if scanned_to > self.pos {
            self.pos = scanned_to;
        }
        // The head just read is authoritative (a stale, higher hint must
        // not make the stream spin).
        self.known_head = head.seq.max(scanned_to);
        Ok(())
    }
}
