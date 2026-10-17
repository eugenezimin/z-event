//! # z-event-cli
//!
//! Operations commands for z-event: the feed head and epoch, registered
//! consumers, parked events, cursors and migrations.
//!
//! The commands use only the storage ports, so [`execute`] works with any
//! storage adapter. The `z-event-cli` binary runs them against PostgreSQL:
//!
//! ```text
//! z-event-cli --database-url postgres://… --schema orders head
//! z-event-cli --schema billing parked list --consumer billing --producer orders
//! z-event-cli --schema orders epoch rotate        # after restoring from a backup
//! ```
//!
//! **Replaying parked events runs the service's own handlers**, which a
//! generic binary does not have. A service adds the commands to its own
//! binary and passes its [`Subscriber`] to
//! [`execute`] as the [`Replay`]:
//!
//! ```no_run
//! use clap::Parser;
//! use z_event::{CursorStore, Handler, Transport, Subscriber};
//! use z_event_cli::{Cli, execute};
//! use z_event_postgres::PgStore;
//!
//! async fn admin<T: Transport, H: Handler<PgStore>>(
//!     store: PgStore,
//!     subscriber: Subscriber<PgStore, T, H>,
//! ) -> Result<(), z_event_cli::CliError> {
//!     let cli = Cli::parse();
//!     execute(&store, cli.command, &mut std::io::stdout(), Some(&subscriber)).await
//! }
//! ```

use std::future::Future;
use std::io::Write;

use clap::{Parser, Subcommand};
use z_event::{
    Cursor, CursorKey, CursorStore, Epoch, Handler, OutboxStore, ParkedFilter, ReplayReport, Seq,
    Subscriber, SubscriberError, Transport,
};

/// Command-line arguments of the `z-event-cli` binary.
#[derive(Debug, Parser)]
#[command(
    name = "z-event-cli",
    version,
    about = "Operate z-event feeds and consumers"
)]
pub struct Cli {
    /// PostgreSQL connection URL.
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    pub database_url: String,
    /// The schema holding the z-event tables (one per service).
    #[arg(long, env = "ZEVENT_SCHEMA")]
    pub schema: String,
    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// A command. Producer-side commands (`head`, `consumers`, `epoch`) act on
/// the producer's schema; consumer-side commands (`parked`, `cursor`) on
/// the consumer's.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Create or upgrade the z-event tables in the schema.
    Migrate,
    /// Show the feed's head, epoch and retention boundary.
    Head,
    /// Consumers registered with this producer.
    #[command(subcommand)]
    Consumers(ConsumersCommand),
    /// Parked events of a consumer.
    #[command(subcommand)]
    Parked(ParkedCommand),
    /// A consumer's position in a producer's feed.
    #[command(subcommand)]
    Cursor(CursorCommand),
    /// The feed's epoch.
    #[command(subcommand)]
    Epoch(EpochCommand),
}

/// `consumers …`
#[derive(Debug, Subcommand)]
pub enum ConsumersCommand {
    /// List registered consumers and their acked positions.
    List,
    /// Unregister a decommissioned consumer, so it no longer holds back retention.
    Remove {
        /// The consumer name.
        consumer: String,
    },
}

/// Which subscription.
#[derive(Debug, Clone, clap::Args)]
pub struct Subscription {
    /// The consumer name.
    #[arg(long)]
    pub consumer: String,
    /// The producer name.
    #[arg(long)]
    pub producer: String,
}

impl Subscription {
    fn key(&self) -> CursorKey {
        CursorKey::new(&self.consumer, &self.producer)
    }
}

/// `parked …`
#[derive(Debug, Subcommand)]
pub enum ParkedCommand {
    /// List parked events, oldest first.
    List {
        /// Which subscription.
        #[command(flatten)]
        subscription: Subscription,
        /// Only this aggregate.
        #[arg(long)]
        aggregate: Option<String>,
        /// Include events already resolved.
        #[arg(long)]
        all: bool,
        /// At most this many.
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Mark a parked event as resolved without applying it (discard it).
    Resolve {
        /// Which subscription.
        #[command(flatten)]
        subscription: Subscription,
        /// The event's position.
        #[arg(long)]
        seq: u64,
    },
    /// Re-run the handler for parked events, in order. Needs the service's
    /// handlers: run it from the service's binary (see the crate docs).
    Replay {
        /// Which subscription.
        #[command(flatten)]
        subscription: Subscription,
        /// Only this aggregate.
        #[arg(long)]
        aggregate: Option<String>,
    },
}

/// `cursor …`
#[derive(Debug, Subcommand)]
pub enum CursorCommand {
    /// Show the position and epoch.
    Show {
        /// Which subscription.
        #[command(flatten)]
        subscription: Subscription,
    },
    /// Move the cursor (replay from a position, or start over after a
    /// rebuild with `--seq 0 --clear-epoch`).
    Reset {
        /// Which subscription.
        #[command(flatten)]
        subscription: Subscription,
        /// The new position (the last applied seq).
        #[arg(long)]
        seq: u64,
        /// The epoch to record (default: keep the current one).
        #[arg(long, conflicts_with = "clear_epoch")]
        epoch: Option<String>,
        /// Forget the epoch: the next subscribe learns it again.
        #[arg(long)]
        clear_epoch: bool,
    },
}

/// `epoch …`
#[derive(Debug, Subcommand)]
pub enum EpochCommand {
    /// Show the current epoch.
    Show,
    /// Start a new epoch. Run it after restoring the producer's storage from
    /// a backup: consumers on the old epoch stop instead of diverging.
    Rotate,
}

/// Why a command failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CliError {
    /// The storage failed.
    #[error("storage: {0}")]
    Store(#[source] z_event::BoxError),
    /// Writing the output failed.
    #[error("output: {0}")]
    Output(#[from] std::io::Error),
    /// Replay is not available here.
    #[error(
        "replay runs the service's handlers: run it from the service's own binary \
         (z_event_cli::execute with its Subscriber) or call Subscriber::replay_parked"
    )]
    ReplayUnavailable,
    /// The replay failed.
    #[error("replay: {0}")]
    Replay(#[from] SubscriberError),
    /// The command is not valid for this storage.
    #[error("{0}")]
    Invalid(String),
}

fn store_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> CliError {
    CliError::Store(Box::new(e))
}

/// Re-runs parked events with the service's handlers.
pub trait Replay: Sync {
    /// Replays the parked events of `key` matching `filter`.
    fn replay(
        &self,
        key: &CursorKey,
        filter: ParkedFilter,
    ) -> impl Future<Output = Result<ReplayReport, CliError>> + Send;
}

impl<S, T, H> Replay for Subscriber<S, T, H>
where
    S: CursorStore,
    T: Transport,
    H: Handler<S>,
{
    async fn replay(
        &self,
        key: &CursorKey,
        filter: ParkedFilter,
    ) -> Result<ReplayReport, CliError> {
        if key != self.key() {
            return Err(CliError::Invalid(format!(
                "this service replays {}, not {key}",
                self.key()
            )));
        }
        Ok(self.replay_parked(filter).await?)
    }
}

/// No replay available (the generic binary).
#[derive(Debug, Clone, Copy)]
pub struct NoReplay;

impl Replay for NoReplay {
    async fn replay(
        &self,
        _key: &CursorKey,
        _filter: ParkedFilter,
    ) -> Result<ReplayReport, CliError> {
        Err(CliError::ReplayUnavailable)
    }
}

fn fmt_time(t: Option<time::OffsetDateTime>) -> String {
    t.and_then(|t| {
        t.format(&time::format_description::well_known::Rfc3339)
            .ok()
    })
    .unwrap_or_else(|| "-".into())
}

/// Runs `command` against `store`, writing human-readable output to `out`.
/// `Migrate` is handled by the binary (it is adapter-specific) and is a
/// no-op here.
pub async fn execute<S, R>(
    store: &S,
    command: Command,
    out: &mut impl Write,
    replay: Option<&R>,
) -> Result<(), CliError>
where
    S: OutboxStore + CursorStore,
    R: Replay,
{
    match command {
        Command::Migrate => {}
        Command::Head | Command::Epoch(EpochCommand::Show) => {
            let head = store.head().await.map_err(store_err)?;
            writeln!(out, "head            {}", head.seq)?;
            writeln!(out, "epoch           {}", head.epoch)?;
            writeln!(out, "purged_through  {}", head.purged_through)?;
            writeln!(
                out,
                "unsequenced     {}",
                store.unsequenced().await.map_err(store_err)?
            )?;
        }
        Command::Epoch(EpochCommand::Rotate) => {
            let epoch = store.rotate_epoch().await.map_err(store_err)?;
            writeln!(out, "new epoch {epoch}")?;
        }
        Command::Consumers(ConsumersCommand::List) => {
            let consumers = store.consumers().await.map_err(store_err)?;
            if consumers.is_empty() {
                writeln!(out, "no registered consumers")?;
            }
            for c in consumers {
                writeln!(
                    out,
                    "{}\tacked {}\tat {}",
                    c.consumer,
                    c.acked_seq,
                    fmt_time(c.acked_at)
                )?;
            }
        }
        Command::Consumers(ConsumersCommand::Remove { consumer }) => {
            store.remove_consumer(&consumer).await.map_err(store_err)?;
            writeln!(out, "removed consumer {consumer}")?;
        }
        Command::Parked(ParkedCommand::List {
            subscription,
            aggregate,
            all,
            limit,
        }) => {
            let filter = ParkedFilter {
                aggregate,
                include_resolved: all,
                limit,
                ..ParkedFilter::default()
            };
            let letters = store
                .parked(&subscription.key(), &filter)
                .await
                .map_err(store_err)?;
            if letters.is_empty() {
                writeln!(out, "no parked events")?;
            }
            for l in letters {
                writeln!(
                    out,
                    "{}\t{}\t{}\tattempts {}\t{}\t{}",
                    l.seq(),
                    l.aggregate(),
                    l.event.event_type,
                    l.attempts,
                    if l.is_resolved() { "resolved" } else { "open" },
                    l.error
                )?;
            }
        }
        Command::Parked(ParkedCommand::Resolve { subscription, seq }) => {
            let mut tx = store.begin().await.map_err(store_err)?;
            store
                .resolve(&mut tx, &subscription.key(), Seq(seq))
                .await
                .map_err(store_err)?;
            store.commit(tx).await.map_err(store_err)?;
            writeln!(out, "resolved {seq} without applying it")?;
        }
        Command::Parked(ParkedCommand::Replay {
            subscription,
            aggregate,
        }) => {
            let replay = replay.ok_or(CliError::ReplayUnavailable)?;
            let filter = ParkedFilter {
                aggregate,
                ..ParkedFilter::default()
            };
            let report = replay.replay(&subscription.key(), filter).await?;
            writeln!(out, "replayed {}", report.replayed)?;
            for (seq, error) in &report.failed {
                writeln!(out, "failed {seq}: {error}")?;
            }
            if !report.blocked_aggregates.is_empty() {
                writeln!(
                    out,
                    "still blocked: {}",
                    report.blocked_aggregates.join(", ")
                )?;
            }
        }
        Command::Cursor(CursorCommand::Show { subscription }) => {
            let cursor = store.cursor(&subscription.key()).await.map_err(store_err)?;
            writeln!(out, "seq    {}", cursor.seq)?;
            writeln!(
                out,
                "epoch  {}",
                cursor.epoch.as_ref().map_or("-", Epoch::as_str)
            )?;
        }
        Command::Cursor(CursorCommand::Reset {
            subscription,
            seq,
            epoch,
            clear_epoch,
        }) => {
            let key = subscription.key();
            let current = store.cursor(&key).await.map_err(store_err)?;
            let epoch = match (epoch, clear_epoch) {
                (Some(e), _) => {
                    Some(Epoch::new(e).ok_or_else(|| CliError::Invalid("empty --epoch".into()))?)
                }
                (None, true) => None,
                (None, false) => current.epoch,
            };
            store
                .reset_cursor(
                    &key,
                    Cursor {
                        seq: Seq(seq),
                        epoch,
                    },
                )
                .await
                .map_err(store_err)?;
            writeln!(out, "cursor of {key} reset to {seq}")?;
        }
    }
    Ok(())
}
