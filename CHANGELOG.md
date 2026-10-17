# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
