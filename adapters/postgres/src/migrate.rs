//! Embedded, versioned migrations with their own history table.

use sqlx::Executor;
use z_event::Epoch;

use crate::{Inner, PgError};

/// Each migration is applied once, in order, inside one transaction.
/// `{s}` is the schema; `{appended}` the appended-notification channel.
const MIGRATIONS: &[(i32, &str)] = &[(
    1,
    r#"
CREATE TABLE {s}.outbox (
    insert_id         bigserial   PRIMARY KEY,
    seq               bigint      UNIQUE,
    id                text        NOT NULL UNIQUE,
    type              text        NOT NULL,
    version           integer     NOT NULL,
    aggregate         text        NOT NULL,
    aggregate_version bigint      NOT NULL,
    correlation_id    text,
    occurred_at       timestamptz NOT NULL,
    sequenced_at      timestamptz,
    headers           jsonb       NOT NULL DEFAULT '{}',
    payload           bytea       NOT NULL
);
CREATE INDEX outbox_unsequenced ON {s}.outbox (insert_id) WHERE seq IS NULL;
CREATE INDEX outbox_aggregate ON {s}.outbox (aggregate);

CREATE TABLE {s}.feed_meta (
    id             boolean PRIMARY KEY DEFAULT true CHECK (id),
    epoch          text    NOT NULL,
    head           bigint  NOT NULL DEFAULT 0,
    purged_through bigint  NOT NULL DEFAULT 0
);

CREATE TABLE {s}.feed_consumers (
    consumer  text        PRIMARY KEY,
    acked_seq bigint      NOT NULL,
    acked_at  timestamptz
);

CREATE TABLE {s}.feed_cursors (
    consumer   text        NOT NULL,
    producer   text        NOT NULL,
    epoch      text,
    seq        bigint      NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, producer)
);

CREATE TABLE {s}.dead_letters (
    consumer    text        NOT NULL,
    producer    text        NOT NULL,
    seq         bigint      NOT NULL,
    aggregate   text        NOT NULL,
    type        text        NOT NULL,
    envelope    jsonb       NOT NULL,
    error       text        NOT NULL,
    attempts    integer     NOT NULL,
    parked_at   timestamptz NOT NULL,
    resolved_at timestamptz,
    PRIMARY KEY (consumer, producer, seq)
);
CREATE INDEX dead_letters_open ON {s}.dead_letters (consumer, producer, aggregate)
    WHERE resolved_at IS NULL;

-- NOTIFY is delivered on commit, so the sequencer wakes only for committed rows.
CREATE FUNCTION {s}.zevent_notify_appended() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('{appended}', '');
    RETURN NULL;
END
$$;
CREATE TRIGGER zevent_outbox_appended AFTER INSERT ON {s}.outbox
    FOR EACH STATEMENT EXECUTE FUNCTION {s}.zevent_notify_appended();
"#,
)];

pub(crate) async fn run(inner: &Inner) -> Result<(), PgError> {
    let s = inner.schema.as_str();
    let bootstrap = format!(
        "CREATE SCHEMA IF NOT EXISTS {s}; \
         CREATE TABLE IF NOT EXISTS {s}.zevent_migrations ( \
             version    integer     PRIMARY KEY, \
             applied_at timestamptz NOT NULL DEFAULT now())"
    );
    let current = format!("SELECT max(version) FROM {s}.zevent_migrations");
    let record = format!("INSERT INTO {s}.zevent_migrations (version) VALUES ($1)");
    let seed_meta =
        format!("INSERT INTO {s}.feed_meta (id, epoch) VALUES (true, $1) ON CONFLICT DO NOTHING");

    let mut tx = inner.pool.begin().await?;
    // Serialise concurrent migrators of this schema (several replicas start).
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(inner.lock_key ^ 0x5a5a_5a5a)
        .execute(&mut *tx)
        .await?;
    (&mut *tx).execute(bootstrap.as_str()).await?;
    let applied: Option<i32> = sqlx::query_scalar(&current).fetch_one(&mut *tx).await?;
    for (version, body) in MIGRATIONS {
        if applied.is_some_and(|a| a >= *version) {
            continue;
        }
        let body = body
            .replace("{s}", s)
            .replace("{appended}", &inner.appended_channel);
        (&mut *tx).execute(body.as_str()).await?;
        sqlx::query(&record)
            .bind(*version)
            .execute(&mut *tx)
            .await?;
        if *version == 1 {
            let epoch = Epoch::generate();
            sqlx::query(&seed_meta)
                .bind(epoch.as_str())
                .execute(&mut *tx)
                .await?;
        }
        tracing::info!(schema = %s, version, "z-event migration applied");
    }
    tx.commit().await?;
    Ok(())
}
