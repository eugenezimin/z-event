# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Cargo workspace, tooling configuration, CI and licences (MIT OR Apache-2.0).
- `z-event` (core): the envelope and protocol types; the storage, handler,
  transport, ACL and snapshot ports; `Producer` (append, sequencer with lease
  fencing, retention), `FeedSource` (identity, ACL, epoch and retention
  checks; backlog, checkpoints, live push, heartbeats), `Subscriber` (effect
  and cursor in one transaction, retries, parking per aggregate, replay,
  reconnect with backoff, acks), the `InProcess` transport, metrics and
  tracing.
- `z-event-memory`: the in-memory reference storage adapter.
- `z-event-postgres`: the PostgreSQL storage adapter (sqlx 0.8). Embedded
  migrations per schema, a sequencer under an advisory-lock lease on a
  dedicated connection, LISTEN/NOTIFY wake-ups, locked cursors and dead
  letters. Tested on PostgreSQL 13 and 16.
- `z-event-testkit`: storage (26 cases) and transport (9 cases) conformance
  suites, fault injectors, and the `storage_suite!` / `transport_suite!`
  macros.
- `examples/in-process`: orders to billing in one process.
- Tests for batched apply, the retention task, `InProcess` re-registration,
  and every documented metric; rustdoc examples on all public types and
  traits.
