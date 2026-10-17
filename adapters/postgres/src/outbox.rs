//! The producer side: outbox, sequencer lease, feed reads, consumers,
//! retention.

use std::collections::BTreeMap;

use bytes::Bytes;
use futures::stream::BoxStream;
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::{Connection, PgConnection, Row};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use z_event::{
    ConsumerState, Epoch, Event, FeedHead, NewEvent, Notification, OutboxStore, Seq, TypeFilter,
};

use crate::{PgError, PgStore, to_i64, to_u64};

/// Sequencer leadership: a session advisory lock held by a dedicated
/// connection. Dropping the lease closes the connection, which releases the
/// lock (a crash looks the same to PostgreSQL).
pub struct PgLease {
    conn: Mutex<PgConnection>,
}

impl std::fmt::Debug for PgLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgLease").finish_non_exhaustive()
    }
}

pub(crate) fn decode_event(row: &PgRow) -> Result<Event, PgError> {
    let id: String = row.try_get("id")?;
    let version: i32 = row.try_get("version")?;
    let headers: Json<BTreeMap<String, String>> = row.try_get("headers")?;
    let payload: Vec<u8> = row.try_get("payload")?;
    Ok(Event {
        seq: Seq(to_u64(row.try_get("seq")?, "seq")?),
        id: id
            .parse()
            .map_err(|_| PgError::Corrupt(format!("event id {id:?} is not a ULID")))?,
        event_type: row.try_get("type")?,
        version: u32::try_from(version)
            .map_err(|_| PgError::Corrupt(format!("negative version {version}")))?,
        aggregate: row.try_get("aggregate")?,
        aggregate_version: to_u64(row.try_get("aggregate_version")?, "aggregate_version")?,
        correlation_id: row.try_get("correlation_id")?,
        occurred_at: row.try_get("occurred_at")?,
        headers: headers.0,
        payload: Bytes::from(payload),
    })
}

impl OutboxStore for PgStore {
    type Lease = PgLease;

    async fn append(&self, tx: &mut Self::Tx, events: &[NewEvent]) -> Result<(), PgError> {
        for ev in events {
            sqlx::query(&self.inner.sql.append)
                .bind(ev.event_id().to_string())
                .bind(ev.event_type())
                .bind(
                    i32::try_from(ev.schema_version())
                        .map_err(|_| PgError::OutOfRange("version"))?,
                )
                .bind(ev.aggregate())
                .bind(to_i64(ev.aggregate_version(), "aggregate_version")?)
                .bind(ev.correlation_id())
                .bind(ev.occurred())
                .bind(Json(ev.headers()))
                .bind(ev.payload().as_ref())
                .execute(&mut **tx)
                .await?;
        }
        Ok(())
    }

    async fn try_lead(&self) -> Result<Option<PgLease>, PgError> {
        let options = self.inner.pool.connect_options();
        let mut conn = PgConnection::connect_with(&options).await?;
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(self.inner.lock_key)
            .fetch_one(&mut conn)
            .await?;
        if locked {
            Ok(Some(PgLease {
                conn: Mutex::new(conn),
            }))
        } else {
            let _ = conn.close().await;
            Ok(None)
        }
    }

    async fn release(&self, lease: PgLease) -> Result<(), PgError> {
        let mut conn = lease.conn.into_inner();
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(self.inner.lock_key)
            .execute(&mut conn)
            .await?;
        conn.close().await?;
        Ok(())
    }

    async fn sequence_batch(&self, lease: &PgLease, limit: u32) -> Result<Option<Seq>, PgError> {
        // Runs on the lease's own connection: if that connection is gone, so
        // is the lock, and this fails (fencing). Session locks need no renewal.
        let mut conn = lease.conn.lock().await;
        let mut tx = conn.begin().await?;
        let stamped: Vec<i64> = sqlx::query_scalar(&self.inner.sql.stamp)
            .bind(i64::from(limit))
            .fetch_all(&mut *tx)
            .await?;
        let Some(&head) = stamped.iter().max() else {
            tx.commit().await?;
            return Ok(None);
        };
        sqlx::query(&self.inner.sql.set_head)
            .bind(head)
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&self.inner.sequenced_channel)
            .bind(head.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Some(Seq(to_u64(head, "head")?)))
    }

    async fn read(
        &self,
        after: Seq,
        types: &TypeFilter,
        limit: u32,
    ) -> Result<Vec<Event>, PgError> {
        let types: Option<Vec<String>> = match types {
            TypeFilter::All => None,
            TypeFilter::Only(set) => Some(set.iter().cloned().collect()),
        };
        let rows = sqlx::query(&self.inner.sql.read)
            .bind(to_i64(after.get(), "seq")?)
            .bind(types)
            .bind(i64::from(limit))
            .fetch_all(&self.inner.pool)
            .await?;
        rows.iter().map(decode_event).collect()
    }

    async fn head(&self) -> Result<FeedHead, PgError> {
        let row = sqlx::query(&self.inner.sql.head)
            .fetch_one(&self.inner.pool)
            .await?;
        let epoch: String = row.try_get("epoch")?;
        Ok(FeedHead {
            seq: Seq(to_u64(row.try_get("head")?, "head")?),
            epoch: Epoch::new(epoch).ok_or_else(|| PgError::Corrupt("empty epoch".into()))?,
            purged_through: Seq(to_u64(row.try_get("purged_through")?, "purged_through")?),
        })
    }

    async fn rotate_epoch(&self) -> Result<Epoch, PgError> {
        let epoch = Epoch::generate();
        sqlx::query(&self.inner.sql.rotate_epoch)
            .bind(epoch.as_str())
            .execute(&self.inner.pool)
            .await?;
        Ok(epoch)
    }

    fn notifications(&self) -> BoxStream<'static, Notification> {
        crate::notify::stream(self)
    }

    async fn unsequenced(&self) -> Result<u64, PgError> {
        let n: i64 = sqlx::query_scalar(&self.inner.sql.unsequenced)
            .fetch_one(&self.inner.pool)
            .await?;
        to_u64(n, "count")
    }

    async fn register_consumer(&self, consumer: &str, at: Seq) -> Result<(), PgError> {
        sqlx::query(&self.inner.sql.register_consumer)
            .bind(consumer)
            .bind(to_i64(at.get(), "seq")?)
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    async fn ack(&self, consumer: &str, seq: Seq) -> Result<(), PgError> {
        sqlx::query(&self.inner.sql.ack)
            .bind(consumer)
            .bind(to_i64(seq.get(), "seq")?)
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    async fn consumers(&self) -> Result<Vec<ConsumerState>, PgError> {
        let rows = sqlx::query(&self.inner.sql.consumers)
            .fetch_all(&self.inner.pool)
            .await?;
        rows.iter()
            .map(|row| {
                Ok(ConsumerState {
                    consumer: row.try_get("consumer")?,
                    acked_seq: Seq(to_u64(row.try_get("acked_seq")?, "acked_seq")?),
                    acked_at: row.try_get("acked_at")?,
                })
            })
            .collect()
    }

    async fn remove_consumer(&self, consumer: &str) -> Result<(), PgError> {
        sqlx::query(&self.inner.sql.remove_consumer)
            .bind(consumer)
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    async fn purge(&self, before: OffsetDateTime) -> Result<u64, PgError> {
        let mut tx = self.inner.pool.begin().await?;
        let (boundary, purged_through): (Option<i64>, i64) =
            sqlx::query_as(&self.inner.sql.purge_boundary)
                .bind(before)
                .fetch_one(&mut *tx)
                .await?;
        let Some(boundary) = boundary.filter(|b| *b > purged_through) else {
            return Ok(0);
        };
        let deleted = sqlx::query(&self.inner.sql.purge_delete)
            .bind(boundary)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if deleted > 0 {
            sqlx::query(&self.inner.sql.purge_meta)
                .bind(boundary)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(deleted)
    }
}
