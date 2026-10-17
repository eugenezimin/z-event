# z-event

**Reliable, push-based events between services, without a message broker.** A Rust library.

> Status: **0.1.0, unreleased.** All crates below are implemented and tested. Read [`DESIGN.md`](DESIGN.md) for the full design and [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) for the build order.

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
| `z-event-postgres` | `adapters/postgres/` | Storage adapter for PostgreSQL (sqlx): outbox, sequencer, LISTEN/NOTIFY, cursors, dead letters, embedded migrations | use it, or write your own |
| `z-event-grpc` | `adapters/grpc/` | Transport adapter for gRPC (tonic): a feed service you **mount on your own server**, a client on **your own channel**, or turnkey standalone helpers; mTLS or bearer-token identity | use it, or write your own (e.g. HTTP/SSE) |
| `z-event-memory` | `adapters/memory/` | In-memory storage: the reference implementation, for tests and examples | dev/test |
| `z-event-cli` | `cli/` | Operations CLI on PostgreSQL: head, consumers, parked events, cursors, epochs, migrations. Its commands are a library, so a service can add them to its own binary (with `parked replay`) | optional |
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
    .add_service(z_event_grpc::feed_service(producer.feed_source(acl), MtlsIdentity::default()))
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

Runnable examples:
- [`examples/in-process`](examples/in-process/src/main.rs): both services in one process on the in-memory adapter. `cargo run -p z-event-example-in-process`.
- [`examples/split`](examples/split/src/main.rs): orders and billing as two processes, on PostgreSQL and gRPC. `DATABASE_URL=… cargo run -p z-event-example-split -- orders` in one terminal and `… -- billing` in another.

## Guides

- [`docs/operating.md`](docs/operating.md): deployment, settings, monitoring and runbooks (stuck sequencer, parked events, restoring from a backup, new consumers).
- [`docs/writing-an-adapter.md`](docs/writing-an-adapter.md): the port contracts and how to prove a storage or transport adapter with the conformance suites.

## Development

```sh
cargo test --workspace --all-features      # unit tests, doctests, conformance suites (memory and PostgreSQL)
cargo clippy --workspace --all-targets --all-features -- -D warnings
bash scripts/check-core-deps.sh            # the core must not depend on a database, gRPC or an adapter
ZEVENT_TESTKIT_SCALE=10 cargo test --release -p z-event-memory --test conformance   # stress
```

`ZEVENT_TESTKIT_SEED` reproduces a randomised conformance run (the seed is printed on every run).

The PostgreSQL tests start `postgres:13-alpine` and `postgres:16-alpine` containers through testcontainers, so they need a running Docker daemon (set `DOCKER_HOST` if its socket is not `/var/run/docker.sock`, e.g. with OrbStack or Colima). Set `ZEVENT_PG_URL=postgres://…` to run them against an existing server instead; every store gets its own fresh schema.

## Guarantees in one table

| Scope | Guarantee |
|---|---|
| Event vs business change | Atomic: both or neither |
| Delivery | At-least-once on the wire, **exactly-once effect** for database effects (effect + cursor in one transaction) |
| Order | Strict per aggregate; total per producer; none across producers |
| Outages | Consumer down for any time within retention: no loss. Producer storage restored from backup: detected, never silent |

## Licence

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT licence](LICENSE-MIT) at your option.
