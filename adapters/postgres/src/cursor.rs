//! The consumer side: locked cursors and parked events.

use sqlx::Row;
use sqlx::types::Json;
use z_event::{Cursor, CursorKey, CursorStore, DeadLetter, Epoch, Event, ParkedFilter, Seq};

use crate::{PgError, PgStore, to_i64, to_u64};

fn cursor_from(seq: i64, epoch: Option<String>) -> Result<Cursor, PgError> {
    Ok(Cursor {
        seq: Seq(to_u64(seq, "cursor seq")?),
        epoch: epoch.and_then(Epoch::new),
    })
}

impl CursorStore for PgStore {
    async fn lock_cursor(&self, tx: &mut Self::Tx, key: &CursorKey) -> Result<Cursor, PgError> {
        sqlx::query(&self.inner.sql.cursor_insert)
            .bind(&key.consumer)
            .bind(&key.producer)
            .execute(&mut **tx)
            .await?;
        let (seq, epoch): (i64, Option<String>) = sqlx::query_as(&self.inner.sql.cursor_lock)
            .bind(&key.consumer)
            .bind(&key.producer)
            .fetch_one(&mut **tx)
            .await?;
        cursor_from(seq, epoch)
    }

    async fn advance(&self, tx: &mut Self::Tx, key: &CursorKey, to: Cursor) -> Result<(), PgError> {
        sqlx::query(&self.inner.sql.cursor_advance)
            .bind(&key.consumer)
            .bind(&key.producer)
            .bind(to_i64(to.seq.get(), "seq")?)
            .bind(to.epoch.as_ref().map(Epoch::as_str))
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    async fn cursor(&self, key: &CursorKey) -> Result<Cursor, PgError> {
        let row: Option<(i64, Option<String>)> = sqlx::query_as(&self.inner.sql.cursor_read)
            .bind(&key.consumer)
            .bind(&key.producer)
            .fetch_optional(&self.inner.pool)
            .await?;
        match row {
            Some((seq, epoch)) => cursor_from(seq, epoch),
            None => Ok(Cursor::start()),
        }
    }

    async fn reset_cursor(&self, key: &CursorKey, to: Cursor) -> Result<(), PgError> {
        sqlx::query(&self.inner.sql.cursor_reset)
            .bind(&key.consumer)
            .bind(&key.producer)
            .bind(to_i64(to.seq.get(), "seq")?)
            .bind(to.epoch.as_ref().map(Epoch::as_str))
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    async fn park(&self, tx: &mut Self::Tx, letter: &DeadLetter) -> Result<(), PgError> {
        sqlx::query(&self.inner.sql.park)
            .bind(&letter.key.consumer)
            .bind(&letter.key.producer)
            .bind(to_i64(letter.seq().get(), "seq")?)
            .bind(letter.aggregate())
            .bind(&letter.event.event_type)
            .bind(Json(&letter.event))
            .bind(&letter.error)
            .bind(i32::try_from(letter.attempts).map_err(|_| PgError::OutOfRange("attempts"))?)
            .bind(letter.parked_at)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    async fn is_parked(
        &self,
        tx: &mut Self::Tx,
        key: &CursorKey,
        aggregate: &str,
    ) -> Result<bool, PgError> {
        Ok(sqlx::query_scalar(&self.inner.sql.is_parked)
            .bind(&key.consumer)
            .bind(&key.producer)
            .bind(aggregate)
            .fetch_one(&mut **tx)
            .await?)
    }

    async fn parked(
        &self,
        key: &CursorKey,
        filter: &ParkedFilter,
    ) -> Result<Vec<DeadLetter>, PgError> {
        let rows = sqlx::query(&self.inner.sql.parked)
            .bind(&key.consumer)
            .bind(&key.producer)
            .bind(filter.aggregate.as_deref())
            .bind(
                filter
                    .from_seq
                    .map(|s| to_i64(s.get(), "seq"))
                    .transpose()?,
            )
            .bind(filter.to_seq.map(|s| to_i64(s.get(), "seq")).transpose()?)
            .bind(filter.include_resolved)
            .bind(filter.limit.map(i64::from))
            .fetch_all(&self.inner.pool)
            .await?;
        rows.iter()
            .map(|row| {
                let event: Json<Event> = row.try_get("envelope")?;
                let attempts: i32 = row.try_get("attempts")?;
                Ok(DeadLetter {
                    key: key.clone(),
                    event: event.0,
                    error: row.try_get("error")?,
                    attempts: u32::try_from(attempts)
                        .map_err(|_| PgError::Corrupt(format!("negative attempts {attempts}")))?,
                    parked_at: row.try_get("parked_at")?,
                    resolved_at: row.try_get("resolved_at")?,
                })
            })
            .collect()
    }

    async fn resolve(&self, tx: &mut Self::Tx, key: &CursorKey, seq: Seq) -> Result<(), PgError> {
        sqlx::query(&self.inner.sql.resolve)
            .bind(&key.consumer)
            .bind(&key.producer)
            .bind(to_i64(seq.get(), "seq")?)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}
