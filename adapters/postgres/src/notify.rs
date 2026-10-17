//! LISTEN/NOTIFY → z-event notifications.

use futures::stream::{self, BoxStream};
use sqlx::postgres::PgListener;
use tokio::sync::mpsc;
use z_event::{Notification, Seq};

use crate::PgStore;

/// Starts listening right away (on a task), so hints sent after this call
/// are not lost while the stream is not yet polled. The task ends when the
/// stream is dropped or the listener fails; the producer then re-opens the
/// stream. Must be called inside a tokio runtime.
pub(crate) fn stream(store: &PgStore) -> BoxStream<'static, Notification> {
    let (tx, rx) = mpsc::channel(256);
    let pool = store.inner.pool.clone();
    let appended = store.inner.appended_channel.clone();
    let sequenced = store.inner.sequenced_channel.clone();
    tokio::spawn(async move {
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(listener) => listener,
            Err(e) => {
                tracing::warn!(error = %e, "z-event LISTEN connection failed");
                return;
            }
        };
        if let Err(e) = listener
            .listen_all([appended.as_str(), sequenced.as_str()])
            .await
        {
            tracing::warn!(error = %e, "z-event LISTEN failed");
            return;
        }
        loop {
            let hint = tokio::select! {
                _ = tx.closed() => return,
                received = listener.try_recv() => match received {
                    Ok(Some(n)) if n.channel() == appended => Notification::Appended,
                    Ok(Some(n)) => match n.payload().parse() {
                        Ok(head) => Notification::Sequenced(Seq(head)),
                        Err(_) => Notification::Lagged,
                    },
                    // The connection was lost and re-established: hints in
                    // between are gone.
                    Ok(None) => Notification::Lagged,
                    Err(e) => {
                        tracing::warn!(error = %e, "z-event LISTEN connection lost");
                        return;
                    }
                },
            };
            if tx.send(hint).await.is_err() {
                return;
            }
        }
    });
    Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|n| (n, rx))
    }))
}
