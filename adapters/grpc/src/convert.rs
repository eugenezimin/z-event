//! Conversions between the protocol types and the generated messages.
//! On the wire an empty string means "none" (correlation id, epoch).

use z_event::{Ack, Checkpoint, Epoch, Event, FeedHead, FeedMessage, Seq, SnapshotRequest, Start};

use crate::pb;

fn none_if_empty(s: String) -> Option<String> {
    (!s.is_empty()).then_some(s)
}

pub(crate) fn timestamp(t: time::OffsetDateTime) -> prost_types::Timestamp {
    let nanos = t.unix_timestamp_nanos();
    prost_types::Timestamp {
        seconds: i64::try_from(nanos.div_euclid(1_000_000_000)).unwrap_or(i64::MAX),
        nanos: i32::try_from(nanos.rem_euclid(1_000_000_000)).unwrap_or(0),
    }
}

pub(crate) fn from_timestamp(
    t: Option<prost_types::Timestamp>,
) -> Result<time::OffsetDateTime, String> {
    let t = t.ok_or("missing occurred_at")?;
    let nanos = i128::from(t.seconds) * 1_000_000_000 + i128::from(t.nanos);
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(|e| e.to_string())
}

impl From<Event> for pb::Event {
    fn from(e: Event) -> Self {
        pb::Event {
            seq: e.seq.get(),
            id: e.id.to_string(),
            r#type: e.event_type,
            version: e.version,
            aggregate: e.aggregate,
            aggregate_version: e.aggregate_version,
            correlation_id: e.correlation_id.unwrap_or_default(),
            occurred_at: Some(timestamp(e.occurred_at)),
            headers: e.headers.into_iter().collect(),
            payload: e.payload.to_vec(),
        }
    }
}

impl TryFrom<pb::Event> for Event {
    type Error = String;

    fn try_from(e: pb::Event) -> Result<Self, String> {
        Ok(Event {
            seq: Seq(e.seq),
            id: e
                .id
                .parse()
                .map_err(|_| format!("event id {:?} is not a ULID", e.id))?,
            event_type: e.r#type,
            version: e.version,
            aggregate: e.aggregate,
            aggregate_version: e.aggregate_version,
            correlation_id: none_if_empty(e.correlation_id),
            occurred_at: from_timestamp(e.occurred_at)?,
            headers: e.headers.into_iter().collect(),
            payload: e.payload.into(),
        })
    }
}

impl From<FeedMessage> for pb::FeedMessage {
    fn from(m: FeedMessage) -> Self {
        let msg = match m {
            FeedMessage::Event(e) => pb::feed_message::Msg::Event(e.into()),
            FeedMessage::Checkpoint(c) => pb::feed_message::Msg::Checkpoint(pb::Checkpoint {
                seq: c.seq.get(),
                head: c.head.get(),
                epoch: c.epoch.to_string(),
            }),
        };
        pb::FeedMessage { msg: Some(msg) }
    }
}

impl TryFrom<pb::FeedMessage> for FeedMessage {
    type Error = String;

    fn try_from(m: pb::FeedMessage) -> Result<Self, String> {
        match m.msg.ok_or("empty feed message")? {
            pb::feed_message::Msg::Event(e) => Ok(FeedMessage::Event(e.try_into()?)),
            pb::feed_message::Msg::Checkpoint(c) => Ok(FeedMessage::Checkpoint(Checkpoint {
                seq: Seq(c.seq),
                head: Seq(c.head),
                epoch: Epoch::new(c.epoch).ok_or("checkpoint without an epoch")?,
            })),
        }
    }
}

impl From<Start> for pb::Start {
    fn from(s: Start) -> Self {
        pb::Start {
            consumer: s.consumer,
            from_seq: s.from_seq.get(),
            epoch: s.epoch.map(|e| e.to_string()).unwrap_or_default(),
            types: s.types,
        }
    }
}

impl From<pb::Start> for Start {
    fn from(s: pb::Start) -> Self {
        Start {
            consumer: s.consumer,
            from_seq: Seq(s.from_seq),
            epoch: Epoch::new(s.epoch),
            types: s.types,
        }
    }
}

impl From<pb::Ack> for Ack {
    fn from(a: pb::Ack) -> Self {
        Ack {
            applied_seq: Seq(a.applied_seq),
        }
    }
}

impl From<SnapshotRequest> for pb::SnapshotRequest {
    fn from(r: SnapshotRequest) -> Self {
        pb::SnapshotRequest {
            consumer: r.consumer,
            aggregate_types: r.aggregate_types,
        }
    }
}

impl From<pb::SnapshotRequest> for SnapshotRequest {
    fn from(r: pb::SnapshotRequest) -> Self {
        SnapshotRequest {
            consumer: r.consumer,
            aggregate_types: r.aggregate_types,
        }
    }
}

impl From<FeedHead> for pb::HeadResponse {
    fn from(h: FeedHead) -> Self {
        pb::HeadResponse {
            head: h.seq.get(),
            epoch: h.epoch.to_string(),
            purged_through: h.purged_through.get(),
        }
    }
}

impl TryFrom<pb::HeadResponse> for FeedHead {
    type Error = String;

    fn try_from(h: pb::HeadResponse) -> Result<Self, String> {
        Ok(FeedHead {
            seq: Seq(h.head),
            epoch: Epoch::new(h.epoch).ok_or("head without an epoch")?,
            purged_through: Seq(h.purged_through),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use z_event::NewEvent;

    #[test]
    fn event_round_trip() {
        let ev = NewEvent::new("T", "a/1", 7, &b"{}"[..])
            .correlation("c")
            .header("tenant", "acme")
            .into_event(Seq(3));
        let back: Event = pb::Event::from(ev.clone()).try_into().unwrap();
        assert_eq!(back, ev);
        let none = NewEvent::new("T", "a/1", 1, &b""[..]).into_event(Seq(1));
        let back: Event = pb::Event::from(none.clone()).try_into().unwrap();
        assert_eq!(back.correlation_id, None);
    }

    #[test]
    fn timestamps_before_1970() {
        let t = time::macros::datetime!(1969-12-31 23:59:59.25 UTC);
        assert_eq!(from_timestamp(Some(timestamp(t))).unwrap(), t);
    }
}
