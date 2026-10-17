# z-event: implementation plan

> The design is in [`DESIGN.md`](DESIGN.md). This document is the build order.
>
> Principle: **prove the ports are database-agnostic before touching a real database.** The core and the in-memory adapter must pass the conformance suite first (ZE2). Only then do Postgres (ZE3) and gRPC (ZE4) arrive, and they must pass the same suite.

## 0. Definition of Done (every milestone)

1. `cargo fmt`, `clippy -D warnings`, `cargo deny` and `cargo nextest` are green in CI.
2. `core` has no dependency on any adapter, database driver or gRPC crate. A CI check fails if `cargo tree -p z-event` contains `sqlx`, `tonic`, `prost` or any `z-event-*` crate.
3. Public APIs have rustdoc with an example. Breaking changes are listed in `CHANGELOG.md`.
4. Every storage or transport adapter runs the full `z-event-testkit` suite in its own tests.

## 1. Dependency graph

```
ZE1 skeleton + core types/ports ──> ZE2 core runtime + memory adapter + testkit ──┬─> ZE3 postgres adapter ──┬─> ZE5 snapshot, epochs, retention, CLI
                                                                                   └─> ZE4 grpc adapter ──────┘
```

ZE3 and ZE4 run in parallel once ZE2 is done. A host project needs **ZE1–ZE4** to run split services. ZE5 completes operations features.

---

### ZE1. Workspace skeleton, core types and ports. Size: S (3–4 days). ✅ Done

**Tasks:**
1. Run `git init`. Create a Cargo workspace with members `core`, `adapters/memory`, `testkit` and `examples`; `adapters/postgres` and `adapters/grpc` are created in ZE3 and ZE4. Add `rust-toolchain.toml`, `rustfmt.toml`, `clippy.toml`, `deny.toml`, `CHANGELOG.md`, and choose a licence (MIT OR Apache-2.0).
2. CI: fmt, clippy, deny, nextest, plus the "core has no adapter dependencies" check.
3. `core` types:
   - `Event`, `NewEvent` (builder: `json(type, aggregate, aggregate_version, &payload)`, `.correlation()`, `.header()`), `Seq`, `Epoch`, `Cursor`, `FeedHead`, `TypeFilter`, `DeadLetter`;
   - the protocol messages `Start`, `Ack`, `Checkpoint`, `FeedMessage`, `SnapshotRequest`.
4. `core` ports: `Store`, `OutboxStore`, `CursorStore`, `Handler`, `HandlerError`, `Transport`, `Subscription`, `Acl` (+ `AllowAll`), `PeerIdentity`, `Snapshotter` (DESIGN §13–§14).

**Done when:** the workspace builds and CI is green. The port traits compile with a dummy implementation in a doc test.

### ZE2. Core runtime, in-memory adapter and the conformance testkit. Size: M (1.5–2 weeks). ★ The critical milestone. ✅ Done

**Tasks:**
1. **`Producer`:**
   - `append` (delegates to the store);
   - the sequencer task: acquire the lease → loop `sequence_batch` on `Appended` notifications plus a 1-second safety timer → release on shutdown;
   - the retention job;
   - `feed_source()`.
2. **`FeedSource`:** identity, ACL and epoch checks; backlog in batches of 500; live push on `Sequenced` notifications; `Checkpoint` after filtered ranges and every 15 s while idle; `on_ack` → `store.ack`; automatic epoch rotation when `from_seq > head`.
3. **`Subscriber`:**
   - builder (`consumer`, `producer`, `types`, retry policy, batch size);
   - reconnect with exponential backoff + jitter;
   - `seq <= cursor` dedup; effect + cursor in one transaction; periodic `Ack`;
   - transient retry → park per aggregate; events behind a parked aggregate are parked too;
   - `replay_parked(filter)`, `reset_cursor`;
   - epoch mismatch → stop + alert metric.
4. **`InProcess` transport** in core.
5. **Metrics and tracing** (DESIGN §18).
6. **`z-event-memory`:** `Tx` = a staged change set applied atomically on commit; true concurrent transactions, so the commit-order-gap test means something; a lease with expiry.
7. **`z-event-testkit`:**
   - `storage::run_all(factory)` and `transport::run_all(factory)`, with every case from DESIGN §17;
   - fault injectors: a `Handler` wrapper that kills before or after commit at random points, and a stream cutter.
8. `examples/in-process`: orders → billing in one process with the memory adapter.

**Done when:** `z-event-memory` + `InProcess` pass both conformance suites in CI, 0 lost and 0 duplicated effects.

**Outcome of ZE1–ZE2:**
- Port signatures are final (DESIGN §13–§14). Decisions made while building them are recorded in DESIGN: cursors keyed by (consumer, producer), a single `TransportError`, the opening checkpoint that carries the epoch, lease fencing and renewal, `purged_through` with `BeyondRetention`, notifications as hints.
- `z-event-memory` with the in-process transport passes both suites: 26 storage cases and 9 transport cases. The suites were checked against seeded bugs (no dedup, effect and cursor in separate transactions, skipped backlog events, sequencing uncommitted appends, and three bugs in batched apply); each makes at least one case fail.
- Gaps closed at the end of ZE2:
  - batched apply is tested, including inside the crash test;
  - the retention task and `InProcess` re-registration are tested;
  - a metrics test checks every documented `feed_*` metric and its labels;
  - every public type and trait has a rustdoc example;
  - `cargo nextest` (CI profile), `cargo deny` and `actionlint` pass locally.
- `PeerIdentity` moved to the transport adapters (core keeps `PeerId`); `z-event-grpc` defines it in ZE4.

### ZE3. Postgres adapter (`z-event-postgres`). Size: M (1–1.5 weeks). ✅ Done

**Tasks:**
1. Embedded migrations with their own history table; `migrate(&pool)`; unqualified table names (works per schema via `search_path`).
2. The `append` and `AFTER INSERT` statement trigger → `pg_notify(zevent_<schema>_appended)`.
3. The sequencer statement (DESIGN §15) and `pg_notify(zevent_<schema>_sequenced, head)`.
4. Advisory-lock lease on a dedicated connection; `PgListener` notifications.
5. Cursors (`SELECT … FOR UPDATE`), dead letters, acks, purge (never deletes the head row), epoch in `feed_meta`.
6. **Runtime-checked queries only** (no `query!` macros). Add a lint or grep check in CI.

**Done when:**
- the conformance suite passes against Postgres in testcontainers (13 and 16 at least);
- two schemas in one database run independent feeds without cross-talk;
- the sequencer failover test passes with two processes.

**Outcome of ZE3:**
- `z-event-postgres` on sqlx 0.8 (the same as z-auth). Statements are schema-qualified, migrations are embedded (`z_event_postgres::migrate(&pool, schema)`), and the advisory-lock lease runs on a dedicated connection that also carries the sequencer statement (fencing).
- The storage suite (26 cases) passes on PostgreSQL 13 and 16 in testcontainers, and the transport suite passes on PostgreSQL 16. Adapter tests: two schemas in one database without cross-talk; idempotent and concurrent migrations; host writes sharing the transaction; a dropped lease freeing leadership; the notification payloads.
- The sequencer failover case runs two producers with separate database sessions in one test process. From PostgreSQL's point of view that is the same as two processes.
- Finding: with a networked database, a killed task's `COMMIT` can still land after the task is gone. The crash test now reads the cursor under `lock_cursor` (`World::settled`), so it compares effects and cursor only after in-flight transactions finish.

### ZE4. gRPC adapter (`z-event-grpc`). Size: M (1–1.5 weeks). ✅ Done

**Tasks:**
1. `proto/zevent/v1/feed.proto` (DESIGN §16); codegen via `tonic-build` + `protox` in `build.rs` at build time (decided in ZE1: no system `protoc`, nothing generated is committed).
2. `feed_service(source, identity)`, a tonic service mountable on the host's server.
3. `Client::new(channel)`, implementing `Transport`.
4. Turnkey helpers: `serve_standalone(source, addr, ServerTls)` and `Client::connect(url, ClientTls)`.
5. `MtlsIdentity` (client-cert subject/SAN) plus a metadata-token example; error → status mapping; HTTP/2 keep-alive.
6. `examples/split`: orders and billing as two processes (postgres + grpc, mTLS with test certs).

**Done when:**
- the transport conformance suite passes over real gRPC, including the network-drop loop through a TCP fault proxy;
- identity and ACL denial are tested;
- the service works both mounted on an existing tonic server and standalone;
- commit → applied latency is p95 < 100 ms in the split example.

**Outcome of ZE4:**
- `z-event-grpc` on tonic 0.14. Code is generated with `tonic-prost-build` + `protox` (no `protoc`). It provides `feed_service` for host servers, `Client` (a `Transport`) with metadata and bearer tokens, `serve_standalone` / `serve_standalone_with_shutdown`, and `MtlsIdentity`, `TokenIdentity` and `MetadataIdentity`.
- Errors map to status codes, with details in metadata. A start below retention uses `OUT_OF_RANGE`, so it can be told apart from an epoch mismatch.
- The transport suite passes over real HTTP/2. Delivery is exactly once through a TCP proxy that cuts connections. Mutual TLS (generated CA and certificates) and bearer tokens on a host-built server are tested. Commit → applied p95 is under 1 ms locally, against the 100 ms target.
- `examples/split` runs orders and billing as two OS processes on PostgreSQL. Its test spawns both binaries against a container.

### ZE5. Snapshots, epochs, retention operations, CLI. Size: M (1 week). ✅ Done

**Tasks:**
1. `Snapshot` end-to-end: the host's `Snapshotter` runs in a consistent read (`REPEATABLE READ` in Postgres) → `*Snapshot` events → `Checkpoint(S)`; the Subscriber's bootstrap mode is "snapshot then subscribe from S".
2. Epochs: `rotate_epoch`; a restore runbook in the docs; tests for both automatic detection and explicit rotation.
3. `z-event-cli` (Postgres first): `head`, `consumers list|remove`, `parked list|replay|resolve`, `cursor reset`, `epoch rotate`.
4. Docs: an "operating z-event" guide (alerts, runbooks) and a "writing an adapter" guide.

**Done when:** a new consumer bootstraps beyond the retention window, a restored producer is detected, and every CLI command has an integration test.

**Outcome of ZE5:**
- Snapshot bootstrap in the subscriber: `bootstrap_from_snapshot` for new consumers and for cursors below retention, plus `Subscriber::bootstrap`. Snapshots read through `Store::begin_snapshot` (PostgreSQL: `REPEATABLE READ READ ONLY`). New conformance cases cover bootstrap below retention, for a new consumer, and through a transport, on memory, PostgreSQL and gRPC.
- Epochs: explicit rotation (`epoch rotate`) and automatic detection were already covered by `epoch_mismatch_detected`. The restore runbook is in `docs/operating.md`.
- `z-event-cli`: `migrate`, `head`, `consumers list|remove`, `parked list|resolve|replay`, `cursor show|reset`, `epoch show|rotate`. The commands are a library over the ports. `parked replay` runs the service's handlers, so a service mounts the commands in its own binary with its `Subscriber`; the generic binary explains this. Every command has an integration test against PostgreSQL, plus a smoke test of the binary.
- Guides: `docs/operating.md` and `docs/writing-an-adapter.md`.

---

## 2. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Port design leaks Postgres assumptions | Another storage adapter is hard to write later | The memory adapter was built first and passes the full suite, so the ports carry no Postgres-only features |
| Subtle delivery bugs: gaps, duplicates, stalls | Lost or doubled effects in host systems | Sequencer by design; conformance suite with concurrency and chaos tests on every CI build; lag and unsequenced alerts |
| Generic traits make the API hard to use | Hosts wrap it inconsistently | Examples first; builders with defaults; one-call turnkey helpers in the gRPC adapter |
| Async-trait ergonomics (`Send` bounds on generic futures) | Compile errors in hosts | Decided in ZE1: native async methods declared as `impl Future + Send` (implementors write `async fn`), no `async-trait`; the example and the suites run on a multi-threaded runtime |
