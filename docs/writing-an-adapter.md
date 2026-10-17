# Writing a z-event adapter

z-event's core knows nothing about databases or networks. Storage and transport are **adapters** that implement the core's ports. This guide explains what each port promises and how to prove an adapter keeps those promises. The reference implementations are `z-event-memory` (storage, small and readable), `z-event-postgres` (storage) and `z-event-grpc` (transport).

## 1. Storage adapters

Implement [`Store`], [`OutboxStore`] (producer side) and [`CursorStore`] (consumer side). Async methods are declared as `fn …(…) -> impl Future<Output = …> + Send`; write them as `async fn`.

### The transaction

`Store::Tx` is the database's own transaction, and the host writes its business data through it too. That shared transaction is the whole point: "business change + event" and "effect + cursor" commit or roll back together. Dropping a `Tx` must roll it back.

`begin_snapshot` returns a read-only transaction that sees one consistent state across all its reads (used for snapshots). The default is `begin`; override it if your default isolation is weaker (PostgreSQL uses `REPEATABLE READ READ ONLY`).

### The sequencing contract

This is what the outbox must guarantee, and the conformance suite checks it hard:

1. `seq` is assigned only to committed events.
2. `seq` is strictly increasing and gapless (1, 2, 3, …) within an epoch.
3. Once an event has a `seq`, it never changes, and nothing is later inserted below the head.
4. Two events for the same aggregate get `seq` values in commit order.

The usual trap is that insert-time ids are not in commit order, so "read everything after id N" skips events. z-event solves it with a single sequencer: writers insert events without a `seq`, and the leader stamps only committed events. Your adapter decides how:

- **Leadership:** `try_lead` returns a lease when nobody else holds one. `sequence_batch` must fail when the lease was lost (fencing) and must renew it; the producer calls it at least once per safety interval. Dropping a lease without `release` is a crash: the lease must free itself (connection loss, expiry). PostgreSQL uses a session advisory lock on a dedicated connection and runs the stamping statement on that connection.
- **Stamping:** one atomic step that takes committed, unsequenced events in a stable order and gives them `head + 1, head + 2, …`, moving the head with them.

### Other promises

| Method | Promise |
|---|---|
| `read(after, types, limit)` | Sequenced events with `seq > after`, ascending, matching `types`, at most `limit` |
| `head` | Current head, epoch and `purged_through` |
| `notifications` | `Appended` after a commit that appended events, `Sequenced(head)` after stamping. Hints only: they may be coalesced or lost, but on overflow yield `Lagged` rather than dropping silently. Start listening when the method is called, not when the stream is first polled |
| `register_consumer` / `ack` | `ack` is monotonic (`max`) and ignores unregistered consumers |
| `purge(before)` | Delete the longest prefix that was sequenced before `before`, is acked by every registered consumer, and is below the head. Never delete the head event. Record the boundary as `purged_through` |
| `lock_cursor` | Exclusive per `(consumer, producer)` until the transaction ends; a second locker waits and then sees the committed value (`SELECT … FOR UPDATE`). Create the cursor on first use |
| `is_parked` | Must see the transaction's own `park` and `resolve` |
| `parked` | Ascending `seq`, honouring the filter |

### Practical advice (learned building the PostgreSQL adapter)

- **Runtime-checked queries.** Never use compile-time checked query macros in an adapter: they would check against the *host's* database at build time.
- **`Send` futures.** Some database libraries produce futures that rustc cannot prove `Send` in generic contexts ("implementation of `Executor` is not general enough"). Keep temporaries such as `&format!(…)` out of `.await` expressions, bind values rather than references, and give public `async` helpers an explicit `-> impl Future<Output = …> + Send` return type so the check happens where the types are concrete.
- **Commits after a crash.** With a networked database, a task that is killed can still have its `COMMIT` land on the server afterwards. That is fine for atomicity, but tests must not compare two separate reads of state that one transaction changes. The testkit's crash test reads the cursor under `lock_cursor` for that reason.

### Proving it: the conformance suites

Add `z-event-testkit` as a dev-dependency and generate one test per case:

```rust
// tests/conformance.rs
z_event_testkit::storage_suite!(|| async { MyStore::fresh().await });
z_event_testkit::transport_suite!(|| async { MyStore::fresh().await }, z_event_testkit::InProcessHarness);
```

or run everything in one test (useful when a database container should live for the whole run):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance() {
    let db = start_database().await;
    z_event_testkit::storage::run_all(db.factory()).await;
}
```

- The factory returns a **new, empty** storage on every call (for SQL, a new schema).
- The store must implement both `OutboxStore` and `CursorStore`: test handlers record their effects by appending to the consumer store's own outbox, in the handler's transaction.
- `ZEVENT_TESTKIT_SEED` reproduces a random run (the seed is printed); `ZEVENT_TESTKIT_SCALE=10` runs ten times the iterations (e.g. 10,000 crashes).

The storage suite covers the sequencing contract (including 50 concurrent transactions with random commit delays, some held open and some rolled back), crashes at a thousand random points (inside the handler, right before and after any store operation, and at random times), sequencer failover, parking and replay, retention, epochs and snapshot bootstrap.

## 2. Transport adapters

A transport carries the protocol (`Start`, `Ack`, `Event`, `Checkpoint`) between processes. It has two halves.

**Server side:** map your wire format to the core's `FeedSource`:

- `open(peer, start)` for a new subscription: forward the returned stream's messages in order, and forward acks to `on_ack`. Establish `peer` from the connection (a certificate, a token); the feed checks that the consumer name matches it.
- `snapshot(peer, request)` and `head()`.

**Client side:** implement [`Transport`]. `subscribe` returns the message stream and an `AckSender`. Map failures to one `TransportError` so the subscriber can tell what to retry: `Closed`, `Unavailable`, `UnknownProducer` and `Protocol` are retried; `PermissionDenied`, `EpochMismatch`, `BeyondRetention`, `Unsupported` and `InvalidRequest` stop the subscriber. Carry enough detail across the wire to rebuild them (the gRPC adapter uses status codes plus metadata).

**Proving it:** implement `TransportHarness` (serve a `FeedSource` and return a client that authenticates as a given peer, plus a guard that stops serving when dropped) and run:

```rust
z_event_testkit::transport_suite!(|| async { z_event_memory::MemoryStore::new() }, MyHarness);
```

The suite checks delivery through repeated stream cuts, identity and ACL enforcement, backpressure, heartbeats, reconnect backoff, acks, ordering, head, snapshot and bootstrap. For a network transport, also test real connection loss: the gRPC adapter runs 300 events through a TCP proxy that kills every connection after 50–300 ms and checks that each event is applied exactly once.

[`Store`]: https://docs.rs/z-event/latest/z_event/trait.Store.html
[`OutboxStore`]: https://docs.rs/z-event/latest/z_event/trait.OutboxStore.html
[`CursorStore`]: https://docs.rs/z-event/latest/z_event/trait.CursorStore.html
[`Transport`]: https://docs.rs/z-event/latest/z_event/trait.Transport.html
