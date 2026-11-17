//! SQL statements, prepared once per store with the schema name embedded.
//! The schema is validated as a plain identifier, so formatting is safe.

use std::sync::Arc;

use sqlx::{AssertSqlSafe, SqlSafeStr, SqlStr};

/// `format!` for a statement whose only interpolations are the validated
/// schema name and constants. The text is shared, so each use is a cheap clone.
macro_rules! sql {
    ($($arg:tt)*) => {
        AssertSqlSafe(Arc::<str>::from(format!($($arg)*))).into_sql_str()
    };
}

pub(crate) struct Sql {
    pub(crate) append: SqlStr,
    pub(crate) stamp: SqlStr,
    pub(crate) set_head: SqlStr,
    pub(crate) read: SqlStr,
    pub(crate) head: SqlStr,
    pub(crate) rotate_epoch: SqlStr,
    pub(crate) unsequenced: SqlStr,
    pub(crate) register_consumer: SqlStr,
    pub(crate) ack: SqlStr,
    pub(crate) consumers: SqlStr,
    pub(crate) remove_consumer: SqlStr,
    pub(crate) purge_boundary: SqlStr,
    pub(crate) purge_delete: SqlStr,
    pub(crate) purge_meta: SqlStr,
    pub(crate) cursor_insert: SqlStr,
    pub(crate) cursor_lock: SqlStr,
    pub(crate) cursor_advance: SqlStr,
    pub(crate) cursor_read: SqlStr,
    pub(crate) cursor_reset: SqlStr,
    pub(crate) park: SqlStr,
    pub(crate) is_parked: SqlStr,
    pub(crate) parked: SqlStr,
    pub(crate) resolve: SqlStr,
}

pub(crate) const EVENT_COLUMNS: &str = "seq, id, type, version, aggregate, aggregate_version, \
     correlation_id, occurred_at, headers, payload";

impl Sql {
    pub(crate) fn new(s: &str) -> Self {
        Sql {
            append: sql!(
                "INSERT INTO {s}.outbox (id, type, version, aggregate, aggregate_version, \
                 correlation_id, occurred_at, headers, payload) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
            ),
            // One leader runs this (advisory lock), so `head + row_number` is
            // safe. Uncommitted rows are invisible to the statement, so only
            // committed events are stamped.
            stamp: sql!(
                "WITH meta AS (SELECT head FROM {s}.feed_meta FOR UPDATE), \
                 batch AS ( \
                     SELECT insert_id, row_number() OVER (ORDER BY insert_id) AS rn \
                     FROM (SELECT insert_id FROM {s}.outbox WHERE seq IS NULL \
                           ORDER BY insert_id LIMIT $1) pending \
                 ) \
                 UPDATE {s}.outbox o \
                 SET seq = meta.head + batch.rn, sequenced_at = now() \
                 FROM batch, meta \
                 WHERE o.insert_id = batch.insert_id \
                 RETURNING o.seq"
            ),
            set_head: sql!("UPDATE {s}.feed_meta SET head = $1"),
            read: sql!(
                "SELECT {EVENT_COLUMNS} FROM {s}.outbox \
                 WHERE seq > $1 AND ($2::text[] IS NULL OR type = ANY($2)) \
                 ORDER BY seq LIMIT $3"
            ),
            head: sql!("SELECT head, epoch, purged_through FROM {s}.feed_meta"),
            rotate_epoch: sql!("UPDATE {s}.feed_meta SET epoch = $1"),
            unsequenced: sql!("SELECT count(*) FROM {s}.outbox WHERE seq IS NULL"),
            register_consumer: sql!(
                "INSERT INTO {s}.feed_consumers (consumer, acked_seq, acked_at) \
                 VALUES ($1, $2, now()) \
                 ON CONFLICT (consumer) DO UPDATE SET acked_seq = $2, acked_at = now()"
            ),
            ack: sql!(
                "UPDATE {s}.feed_consumers \
                 SET acked_seq = GREATEST(acked_seq, $2), acked_at = now() \
                 WHERE consumer = $1"
            ),
            consumers: sql!(
                "SELECT consumer, acked_seq, acked_at FROM {s}.feed_consumers ORDER BY consumer"
            ),
            remove_consumer: sql!("DELETE FROM {s}.feed_consumers WHERE consumer = $1"),
            // The longest prefix that is below the head, acked by every
            // registered consumer, and sequenced before $1.
            purge_boundary: sql!(
                "SELECT LEAST( \
                     (SELECT head - 1 FROM {s}.feed_meta), \
                     COALESCE((SELECT min(acked_seq) FROM {s}.feed_consumers), 9223372036854775807), \
                     COALESCE((SELECT min(seq) - 1 FROM {s}.outbox \
                               WHERE seq IS NOT NULL AND sequenced_at >= $1), 9223372036854775807) \
                 ), (SELECT purged_through FROM {s}.feed_meta)"
            ),
            purge_delete: sql!("DELETE FROM {s}.outbox WHERE seq IS NOT NULL AND seq <= $1"),
            purge_meta: sql!(
                "UPDATE {s}.feed_meta SET purged_through = GREATEST(purged_through, $1)"
            ),
            cursor_insert: sql!(
                "INSERT INTO {s}.feed_cursors (consumer, producer) VALUES ($1, $2) \
                 ON CONFLICT DO NOTHING"
            ),
            cursor_lock: sql!(
                "SELECT seq, epoch FROM {s}.feed_cursors \
                 WHERE consumer = $1 AND producer = $2 FOR UPDATE"
            ),
            cursor_advance: sql!(
                "UPDATE {s}.feed_cursors SET seq = $3, epoch = $4, updated_at = now() \
                 WHERE consumer = $1 AND producer = $2"
            ),
            cursor_read: sql!(
                "SELECT seq, epoch FROM {s}.feed_cursors WHERE consumer = $1 AND producer = $2"
            ),
            cursor_reset: sql!(
                "INSERT INTO {s}.feed_cursors (consumer, producer, seq, epoch) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (consumer, producer) \
                 DO UPDATE SET seq = $3, epoch = $4, updated_at = now()"
            ),
            park: sql!(
                "INSERT INTO {s}.dead_letters \
                 (consumer, producer, seq, aggregate, type, envelope, error, attempts, parked_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                 ON CONFLICT (consumer, producer, seq) DO UPDATE SET \
                 envelope = $6, error = $7, attempts = $8, parked_at = $9, resolved_at = NULL"
            ),
            is_parked: sql!(
                "SELECT EXISTS (SELECT 1 FROM {s}.dead_letters \
                 WHERE consumer = $1 AND producer = $2 AND aggregate = $3 \
                 AND resolved_at IS NULL)"
            ),
            parked: sql!(
                "SELECT envelope, error, attempts, parked_at, resolved_at FROM {s}.dead_letters \
                 WHERE consumer = $1 AND producer = $2 \
                 AND ($3::text IS NULL OR aggregate = $3) \
                 AND ($4::bigint IS NULL OR seq >= $4) \
                 AND ($5::bigint IS NULL OR seq <= $5) \
                 AND ($6 OR resolved_at IS NULL) \
                 ORDER BY seq LIMIT $7"
            ),
            resolve: sql!(
                "UPDATE {s}.dead_letters SET resolved_at = now() \
                 WHERE consumer = $1 AND producer = $2 AND seq = $3"
            ),
        }
    }
}
