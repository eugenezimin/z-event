//! The event envelope: [`Event`] (sequenced, as delivered) and [`NewEvent`]
//! (as appended by the producer's business code).

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use ulid::Ulid;

use super::position::Seq;

/// A sequenced event, as read from the feed and delivered to handlers.
///
/// z-event never interprets `headers` or `payload`; they belong to the host.
///
/// ```
/// use z_event::{NewEvent, Seq};
///
/// #[derive(serde::Deserialize)]
/// struct OrderConfirmed { order_id: String }
///
/// let ev = NewEvent::new("OrderConfirmed", "orders/o1", 1, r#"{"order_id":"o1"}"#)
///     .header("tenant", "acme")
///     .into_event(Seq(1));
/// let payload: OrderConfirmed = ev.json()?;
/// assert_eq!(payload.order_id, "o1");
/// assert_eq!(ev.header("tenant"), Some("acme"));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Position in the producer's feed: gapless, commit order.
    pub seq: Seq,
    /// Globally unique id; the idempotency key for external side effects.
    pub id: Ulid,
    /// Event type, e.g. `OrderConfirmed`. Used by filters and ACLs.
    #[serde(rename = "type")]
    pub event_type: String,
    /// Payload schema version.
    pub version: u32,
    /// The record the event is about, e.g. `orders/o17`. The unit of
    /// ordering and parking.
    pub aggregate: String,
    /// Version of that record after the change.
    pub aggregate_version: u64,
    /// Request id of the user action, for tracing across services.
    pub correlation_id: Option<String>,
    /// When the business change happened.
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
    /// Host-defined metadata, e.g. `tenant` or `actor`.
    pub headers: BTreeMap<String, String>,
    /// Host-defined payload, usually JSON.
    pub payload: Bytes,
}

impl Event {
    /// Deserialises a JSON payload.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.payload)
    }

    /// A header value.
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(String::as_str)
    }
}

/// An event to append to the outbox, inside the business transaction.
///
/// ```
/// use z_event::NewEvent;
///
/// #[derive(serde::Serialize)]
/// struct OrderConfirmed { order_id: String }
///
/// let ev = NewEvent::json("OrderConfirmed", "orders/o17", 3, &OrderConfirmed { order_id: "o17".into() })?
///     .correlation("req-42")
///     .header("tenant", "acme");
/// assert_eq!(ev.event_type(), "OrderConfirmed");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewEvent {
    id: Ulid,
    event_type: String,
    version: u32,
    aggregate: String,
    aggregate_version: u64,
    correlation_id: Option<String>,
    occurred_at: OffsetDateTime,
    headers: BTreeMap<String, String>,
    payload: Bytes,
}

impl NewEvent {
    /// An event with a raw payload. The id is a fresh ULID, the version is 1
    /// and `occurred_at` is now.
    pub fn new(
        event_type: impl Into<String>,
        aggregate: impl Into<String>,
        aggregate_version: u64,
        payload: impl Into<Bytes>,
    ) -> Self {
        NewEvent {
            id: Ulid::new(),
            event_type: event_type.into(),
            version: 1,
            aggregate: aggregate.into(),
            aggregate_version,
            correlation_id: None,
            occurred_at: OffsetDateTime::now_utc(),
            headers: BTreeMap::new(),
            payload: payload.into(),
        }
    }

    /// An event whose payload is `payload` serialised as JSON.
    pub fn json<T: Serialize + ?Sized>(
        event_type: impl Into<String>,
        aggregate: impl Into<String>,
        aggregate_version: u64,
        payload: &T,
    ) -> Result<Self, serde_json::Error> {
        let bytes = serde_json::to_vec(payload)?;
        Ok(Self::new(event_type, aggregate, aggregate_version, bytes))
    }

    /// Sets the correlation id (the request id of the user action).
    pub fn correlation(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Adds a host-defined header.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Sets the payload schema version (default 1).
    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// Sets when the business change happened (default: now).
    pub fn occurred_at(mut self, at: OffsetDateTime) -> Self {
        self.occurred_at = at;
        self
    }

    /// Sets the event id (default: a fresh ULID). Useful for deterministic tests.
    pub fn id(mut self, id: Ulid) -> Self {
        self.id = id;
        self
    }

    /// The event id.
    pub fn event_id(&self) -> Ulid {
        self.id
    }

    /// The event type.
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// The aggregate.
    pub fn aggregate(&self) -> &str {
        &self.aggregate
    }

    /// The aggregate version.
    pub fn aggregate_version(&self) -> u64 {
        self.aggregate_version
    }

    /// The correlation id, if set.
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    /// When the business change happened.
    pub fn occurred(&self) -> OffsetDateTime {
        self.occurred_at
    }

    /// The headers.
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }

    /// The payload.
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// The payload schema version.
    pub fn schema_version(&self) -> u32 {
        self.version
    }

    /// Turns the event into a sequenced [`Event`]. For storage adapters.
    pub fn into_event(self, seq: Seq) -> Event {
        Event {
            seq,
            id: self.id,
            event_type: self.event_type,
            version: self.version,
            aggregate: self.aggregate,
            aggregate_version: self.aggregate_version,
            correlation_id: self.correlation_id,
            occurred_at: self.occurred_at,
            headers: self.headers,
            payload: self.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_and_into_event() {
        let id = Ulid::new();
        let ev = NewEvent::json("A", "x/1", 2, &serde_json::json!({"k": 1}))
            .unwrap()
            .id(id)
            .version(3)
            .correlation("c")
            .header("tenant", "t")
            .into_event(Seq(9));
        assert_eq!(ev.seq, Seq(9));
        assert_eq!(ev.id, id);
        assert_eq!(ev.version, 3);
        assert_eq!(ev.aggregate_version, 2);
        assert_eq!(ev.correlation_id.as_deref(), Some("c"));
        assert_eq!(ev.header("tenant"), Some("t"));
        let v: serde_json::Value = ev.json().unwrap();
        assert_eq!(v["k"], 1);
    }

    #[test]
    fn serde_round_trip_uses_type_field() {
        let ev = NewEvent::new("OrderConfirmed", "orders/o1", 1, &b"{}"[..]).into_event(Seq(1));
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "OrderConfirmed");
        let back: Event = serde_json::from_value(json).unwrap();
        assert_eq!(back, ev);
    }
}
