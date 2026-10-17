# Operating z-event

This guide is for the people who deploy and run services that use z-event: what to configure, what to watch, and what to do when something goes wrong. The design behind it is in [`DESIGN.md`](../DESIGN.md).

## 1. Deployment

**One schema per service.** Each service keeps the z-event tables in its own PostgreSQL schema, next to its business tables. Services may share a database and a connection pool.

```rust
let store = z_event_postgres::migrate(&pool, "orders").await?;   // at startup, on every replica
```

`migrate` creates or upgrades the tables (`outbox`, `feed_meta`, `feed_consumers`, `feed_cursors`, `dead_letters`) and records what it did in `zevent_migrations`. Several replicas may run it at the same time.

**Producers.** Every replica of a producing service calls `Producer::start()`. Only one replica at a time stamps events (the sequencer leader, chosen with a PostgreSQL advisory lock); the others take over within about a second if the leader's connection goes away. Every replica can serve the feed.

**Consumers.** One `Subscriber` per consumed producer. Running the same consumer on several replicas is safe (the cursor lock and the duplicate check prevent double effects) but buys nothing: only one of them makes progress at a time.

**Transport.** Mount `z_event_grpc::feed_service` on the service's existing gRPC server, or use `serve_standalone`. Give every consumer an identity: client certificates (`MtlsIdentity`) or bearer tokens (`TokenIdentity`). Give every feed an ACL (`TypeAcl` or your own `Acl`); `AllowAll` is for development only.

### Settings that matter

| Setting | Default | When to change |
|---|---|---|
| `ProducerConfig::retention` | 90 days | How long acked events are kept for replays. Longer means more storage; shorter means consumers that fall further behind must bootstrap from a snapshot |
| `ProducerConfig::safety_interval` | 1 s | Upper bound on the commit → feed delay if a notification is lost |
| `FeedConfig::heartbeat` | 15 s | How often idle streams send a checkpoint; also bounds how quickly a rotated epoch stops streams |
| `SubscriberBuilder::retry` | 5 attempts, 0.1–30 s | How long a failing event is retried before it is parked |
| `SubscriberBuilder::batch_size` | 1 | Raise for projections that apply many small events (e.g. 100) |
| `SubscriberBuilder::bootstrap_from_snapshot` | off | Turn on for consumers that should start from the producer's current state instead of the whole history, and recover from falling behind retention by themselves. Needs a `Snapshotter` on the producer |

## 2. Monitoring

z-event reports through the `metrics` facade (install the exporter you use, e.g. Prometheus) and `tracing`. The names are constants in `z_event::telemetry`.

| Metric | Alert when | Meaning |
|---|---|---|
| `feed_unsequenced_rows{producer}` | > 0 for more than 10 s | Committed events are not being stamped: the sequencer is stuck |
| `feed_sequencer_leader{producer}` | no replica at 1 for more than 10 s | No replica holds the sequencer lease |
| `feed_consumer_lag_seconds{producer,consumer}` | > 60 s for 5 min | A consumer is behind |
| `feed_dead_letters{producer,consumer}` | > 0 | Events are parked and need attention |
| `feed_epoch_mismatch_total`, `feed_epoch_rotations_total` | any increase | A producer's feed was reset (restore from backup) |
| `feed_stream_reconnects_total` | sustained high rate | Network trouble or a crashing producer |
| `feed_consumer_acked_seq{producer,consumer}` | not moving for a day | A consumer is gone and holds back retention |

Every applied event runs in a `z_event.apply` span with the event's `correlation_id`, so one trace connects the user's request with the effects in every consumer.

The `z-event-cli` binary gives a quick look at a schema:

```text
z-event-cli --database-url $DATABASE_URL --schema orders head
z-event-cli --schema orders consumers list
z-event-cli --schema billing cursor show --consumer billing --producer orders
z-event-cli --schema billing parked list --consumer billing --producer orders
```

## 3. Runbooks

### The sequencer is stuck (`feed_unsequenced_rows` stays above zero)

1. Check `feed_sequencer_leader`. If no replica leads, the replicas cannot take the advisory lock: look for a connection that still holds it (`SELECT * FROM pg_locks WHERE locktype = 'advisory'`) and for database connectivity errors in the logs (`sequencer could not try the lease`).
2. If a replica leads but nothing is stamped, look for `sequencing failed; giving the lease up` in its logs; the replica steps down and another takes over.
3. Restarting the producing service is always safe: committed events are never lost, they are stamped by the next leader.

### A consumer is behind

1. `z-event-cli --schema <consumer> cursor show …` against `head` on the producer shows the distance.
2. A slow handler is the usual cause. Raise `batch_size` for projections, or scale the work the handler starts (job rows) rather than the handler itself.
3. Lag never affects the producer: it keeps appending, and the consumer catches up at full speed.

### Events are parked (`feed_dead_letters > 0`)

1. List them: `z-event-cli --schema billing parked list --consumer billing --producer orders`. Later events of the same aggregate are parked behind the first failure (`attempts 0`), so the aggregate's order is preserved while other aggregates keep flowing.
2. Fix the cause (a bug, bad data, a missing dependency) and deploy.
3. Replay. Replay runs the service's handlers, so it runs inside the service: call `Subscriber::replay_parked` from an admin endpoint, or add the z-event commands to the service's own binary with `z_event_cli::execute(store, command, out, Some(&subscriber))` and run `parked replay`. Events that fail again stay parked, and the rest of their aggregate stays behind them.
4. To drop an event deliberately: `parked resolve --seq N`. Its handler never runs. Do this only when the effect is known to be unwanted.

### Restoring a producer from a backup

A restored producer's feed goes back in time. Events after the backup point are gone, and new events will reuse their positions. Consumers must not continue as if nothing happened.

1. Restore the producer's database.
2. Rotate the epoch before restarting the service:
   ```text
   z-event-cli --schema orders epoch rotate
   ```
   (If you forget, z-event rotates it by itself as soon as a consumer turns out to be ahead of the head, but a consumer that is not ahead would continue on the reused positions until then.)
3. Every consumer of this producer stops with `EpochMismatch` and `feed_epoch_mismatch_total` increases. For each consumer:
   1. Decide what happens to the data it derived from the lost events. Usually: rebuild its projection.
   2. Clear its cursor: `z-event-cli --schema billing cursor reset --consumer billing --producer orders --seq 0 --clear-epoch`.
   3. Restart it. With `bootstrap_from_snapshot` it starts from the producer's current state; without it, it replays the feed that is still retained.

### Restoring a consumer from a backup

Nothing special is needed: the consumer's cursor is in the same database as its data, so they are restored together. It resubscribes from the restored cursor and the producer sends everything since. If that position is already below the producer's retention window, the subscriber stops with `BeyondRetention` (or bootstraps from the snapshot, if configured).

### A new consumer joins a producer with a long history

Either let it replay (it subscribes from 0 and catches up at full speed, within retention), or configure `bootstrap_from_snapshot(["orders", …])`: it applies the producer's snapshot and continues from there. The producer needs a `Snapshotter` (`FeedSource::with_snapshotter`).

### A consumer is decommissioned

Registered consumers hold back retention until they ack. Remove it so the producer can purge:

```text
z-event-cli --schema orders consumers remove search
```

### Replaying a consumer from a position

Reset its cursor while it runs (or before it starts); it resubscribes from there:

```text
z-event-cli --schema search cursor reset --consumer search --producer orders --seq 1200
```

Handlers that check `aggregate_version` make this safe over data they already applied.

## 4. Security checklist

- Every feed has an ACL; types carrying personal data are readable only by the consumers that need them.
- Consumers authenticate with client certificates or tokens; the consumer name in `Start` must match the identity, so one service cannot read as another.
- Producers keep personal data out of events unless the consumer is allowed to see it. z-event never looks at payloads or headers.
- The database user of a service needs rights only in its own schema.
