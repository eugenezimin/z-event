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
- `z-event-testkit`: storage (24 cases) and transport (9 cases) conformance
  suites, fault injectors, and the `storage_suite!` / `transport_suite!`
  macros.
- `examples/in-process`: orders to billing in one process.
