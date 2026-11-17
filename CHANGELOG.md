# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0] - 2026-11-17

### Changed

- `z-event-postgres` moves to sqlx 0.9, the version z-auth 0.7 uses, so a host still shares one transaction type between both libraries. Hosts must move to sqlx 0.9 together with this release.
- `Ulid`, which `z-event` re-exports, now comes from ulid 3, where `Ulid::new` is called `Ulid::generate`. Event ids and epochs keep their format.
- The workspace asks for tokio 1.54 and fastrand 2.5 at least, and the lockfile is refreshed to the newest compatible releases.

### Security

- cargo-deny ignores RUSTSEC-2026-0440 (astral-tokio-tar is unmaintained), which reaches only the tests through testcontainers.

## [0.1.0] - 2026-10-29

### Added

- The Cargo workspace, tooling configuration, CI and licences, MIT or Apache-2.0 at the user's option.
- The core crate `z-event` with the envelope and protocol types and the traits for storage, handlers, transport, access control and snapshots.
- `Producer` with appending in the host's transaction, a sequencer under a fenced lease and ack-gated retention.
- `FeedSource` with identity, access control, epoch and retention checks, backlog reading, skip checkpoints, live push and heartbeats.
- `Subscriber` with effect and cursor in one transaction, retries, parking per aggregate, replay, batching, reconnects with backoff and acknowledgements.
- The `InProcess` transport, metrics through the `metrics` facade and spans through `tracing`.
- Snapshot bootstrap with `SubscriberBuilder::bootstrap_from_snapshot` and `Subscriber::bootstrap`, and `Store::begin_snapshot` for consistent snapshot reads.
- `z-event-memory`, the in-memory reference storage adapter.
- `z-event-postgres`, the PostgreSQL storage adapter on sqlx 0.8, with embedded migrations per schema, an advisory-lock sequencer lease, notifications on commit, locked cursors and parked events, tested on PostgreSQL 13 and 16.
- `z-event-grpc`, the gRPC transport on tonic 0.14 with protox code generation, a feed service for existing servers, a client for existing channels, standalone helpers and identities based on client certificates, bearer tokens or headers.
- `z-event-cli`, operations commands for feeds, consumers, parked events, cursors, epochs and migrations, usable as a binary or as a library inside a service.
- `z-event-testkit`, with 28 storage and 10 transport conformance cases, fault injectors and the `storage_suite!` and `transport_suite!` macros.
- `examples/in-process`, with two services in one process, and `examples/split`, with two processes on PostgreSQL and gRPC.
- Documentation covering the architecture in `DESIGN.md`, a usage guide, an operations guide and a guide for adapter authors.
- `docs/scenarios.md`, which walks through deployment topologies with draw.io component diagrams in `docs/diagrams` and sequence diagrams, and `scripts/export-diagrams.sh`, which regenerates the diagram images.

### Changed

- The z-event tables are recommended to live in a schema of their own inside the service's database, such as `orders_zevent` next to the business schema `orders`, with business transactions begun on the service's own pool. `examples/split`, the doc comments of `z-event-postgres` and `z-event-cli`, and the documentation follow this layout, and a PostgreSQL test covers it.

[Unreleased]: https://github.com/eugenezimin/z-event/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/eugenezimin/z-event/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/eugenezimin/z-event/releases/tag/v0.1.0
