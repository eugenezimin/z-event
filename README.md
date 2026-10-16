# z-event

**Reliable, push-based events between services, without a message broker.** A Rust library.

> Status: **design draft**, no code yet. Read [`DESIGN.md`](DESIGN.md) for the full design and [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) for the build order.

## What it does

- **Transactional outbox.** A service writes its events in the **same database transaction** as its business change. If the change rolls back, the event never existed. If it commits, the event will be delivered.
- **Ordered feed, pushed.** Each producer serves its outbox as a gapless, commit-ordered feed. Consumers subscribe once; new events are **pushed** within milliseconds. Nobody polls.
- **Effectively exactly-once.** Each consumer stores its cursor **in the same transaction** as the effect of the event. Crashes, restarts and network drops never lose or double-apply an event.
- **Operational basics included:**
  - parking of poison events per aggregate, with replay;
  - replay from any position, snapshots for new consumers;
  - retention gated on acks;
  - epoch detection after a backup restore;
  - lag metrics and tracing.

## Why no broker

At modest volume (up to hundreds of events per second per service), the database you already run is a perfectly good durable log. z-event gives you broker-like guarantees with **no extra clustered system** to install, secure, back up and monitor. If you outgrow it, a broker can be added as another transport adapter without touching your handlers ([DESIGN §19](DESIGN.md#19-costs-limits-and-when-to-add-a-broker)).

## Crates

| Crate | Folder | What it is | Optional? |
|---|---|---|---|
| `z-event` | `core/` | Protocol, `Producer`, `Subscriber`, storage and transport **ports** (traits), in-process transport. No database or network dependencies | required |
| `z-event-postgres` | `adapters/postgres/` | Storage adapter for PostgreSQL (sqlx): outbox, sequencer, LISTEN/NOTIFY, cursors, dead letters, embedded migrations | use it, or write your own (e.g. MongoDB) |
| `z-event-grpc` | `adapters/grpc/` | Transport adapter for gRPC (tonic): a feed service you **mount on your own server**, a client on **your own channel**, or turnkey standalone helpers | use it, or write your own (e.g. HTTP/SSE) |
| `z-event-memory` | `adapters/memory/` | In-memory storage: the reference implementation, for tests and examples | dev/test |
| `z-event-testkit` | `testkit/` | Conformance suites that every storage or transport adapter must pass | dev/test |

## Usage sketch

```rust
// Producer: event and business change in one transaction
let mut tx = store.begin().await?;
orders::confirm(&mut tx, id).await?;
producer.append(&mut tx, NewEvent::json("OrderConfirmed", format!("orders/{id}"), ver, &payload)?).await?;
store.commit(tx).await?;

// Producer: expose the feed on your existing tonic server
Server::builder()
    .add_service(z_event_grpc::feed_service(producer.feed_source(), MtlsIdentity::default()))
    .serve(addr).await?;

// Consumer: handler runs in the same transaction as the cursor update
struct CreateInvoice;
impl Handler<PgStore> for CreateInvoice {
    async fn handle(&self, tx: &mut PgTx, ev: &Event) -> Result<(), HandlerError> {
        billing::create_invoice(tx, ev).await.map_err(HandlerError::transient)
    }
}
Subscriber::builder(store, z_event_grpc::Client::new(channel), CreateInvoice)
    .consumer("billing").producer("orders").types(["OrderConfirmed"])
    .spawn();
```

## Guarantees in one table

| Scope | Guarantee |
|---|---|
| Event vs business change | Atomic: both or neither |
| Delivery | At-least-once on the wire, **exactly-once effect** for database effects (effect + cursor in one transaction) |
| Order | Strict per aggregate; total per producer; none across producers |
| Outages | Consumer down for any time within retention: no loss. Producer storage restored from backup: detected, never silent |
