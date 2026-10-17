//! Handlers used by the suites.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use z_event::{Event, Handler, HandlerError, NewEvent, OutboxStore, Store};

/// Event type of recorded effects.
pub const EFFECT: &str = "Effect";

/// Records each applied event as an `Effect` event appended to the
/// **consumer store's own outbox**, in the handler's transaction. The
/// effect is therefore atomic with the cursor, and the suite can count
/// effects through the ports alone. The effect's aggregate is
/// `effect/<source seq>`, or `snapshot/<aggregate>` for `*Snapshot` events
/// (which all carry the snapshot's position).
///
/// ```
/// use z_event::OutboxStore;
/// use z_event_testkit::handlers::EffectHandler;
///
/// fn recorder<S: OutboxStore + Clone>(consumer_store: &S) -> EffectHandler<S> {
///     EffectHandler::new(consumer_store.clone())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct EffectHandler<S> {
    store: S,
}

impl<S> EffectHandler<S> {
    /// Records effects in `store` (the consumer's store).
    pub fn new(store: S) -> Self {
        EffectHandler { store }
    }
}

impl<S: OutboxStore> Handler<S> for EffectHandler<S> {
    async fn handle(&self, tx: &mut S::Tx, ev: &Event) -> Result<(), HandlerError> {
        let aggregate = if ev.event_type.ends_with("Snapshot") {
            format!("snapshot/{}", ev.aggregate)
        } else {
            format!("effect/{}", ev.seq.get())
        };
        let effect = NewEvent::new(EFFECT, aggregate, 1, Vec::new())
            .header("source_id", ev.id.to_string())
            .header("source_aggregate", ev.aggregate.clone());
        self.store
            .append(tx, &[effect])
            .await
            .map_err(HandlerError::transient)
    }
}

/// Answers snapshot requests with one `<type>Snapshot` event per listed
/// aggregate of each requested type (`orders` → `ordersSnapshot`).
#[derive(Clone, Debug, Default)]
pub struct FixedSnapshot {
    aggregates: Vec<String>,
}

impl FixedSnapshot {
    /// Snapshots these aggregates.
    pub fn new<I, X>(aggregates: I) -> Self
    where
        I: IntoIterator<Item = X>,
        X: Into<String>,
    {
        FixedSnapshot {
            aggregates: aggregates.into_iter().map(Into::into).collect(),
        }
    }
}

impl<S: Store> z_event::Snapshotter<S> for FixedSnapshot {
    async fn snapshot(
        &self,
        _tx: &mut S::Tx,
        request: &z_event::SnapshotRequest,
    ) -> Result<Vec<NewEvent>, z_event::BoxError> {
        Ok(request
            .aggregate_types
            .iter()
            .flat_map(|t| {
                self.aggregates
                    .iter()
                    .map(move |a| NewEvent::new(format!("{t}Snapshot"), a.clone(), 1, Vec::new()))
            })
            .collect())
    }
}

/// Fails (permanently or transiently) for the aggregates in a shared set,
/// otherwise delegates.
///
/// ```
/// use z_event::OutboxStore;
/// use z_event_testkit::handlers::{EffectHandler, PoisonHandler};
///
/// fn poisoned<S: OutboxStore + Clone>(store: S) -> PoisonHandler<EffectHandler<S>> {
///     let handler = PoisonHandler::permanent(EffectHandler::new(store));
///     handler.poison("orders/o1"); // heal() later, then replay
///     handler
/// }
/// ```
#[derive(Clone, Debug)]
pub struct PoisonHandler<H> {
    inner: H,
    poisoned: Arc<Mutex<HashSet<String>>>,
    permanent: bool,
}

impl<H> PoisonHandler<H> {
    /// Fails with permanent errors for poisoned aggregates.
    pub fn permanent(inner: H) -> Self {
        PoisonHandler {
            inner,
            poisoned: Arc::default(),
            permanent: true,
        }
    }

    /// Fails with transient errors for poisoned aggregates.
    pub fn transient(inner: H) -> Self {
        PoisonHandler {
            permanent: false,
            ..Self::permanent(inner)
        }
    }

    /// Poisons an aggregate.
    pub fn poison(&self, aggregate: &str) {
        self.set().insert(aggregate.to_owned());
    }

    /// Heals every aggregate.
    pub fn heal(&self) {
        self.set().clear();
    }

    fn set(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.poisoned.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<S: Store, H: Handler<S>> Handler<S> for PoisonHandler<H> {
    async fn handle(&self, tx: &mut S::Tx, ev: &Event) -> Result<(), HandlerError> {
        let poisoned = self.set().contains(&ev.aggregate);
        if poisoned {
            let msg = format!("{} is poisoned", ev.aggregate);
            return Err(if self.permanent {
                HandlerError::permanent(msg)
            } else {
                HandlerError::transient(msg)
            });
        }
        self.inner.handle(tx, ev).await
    }
}

/// Sleeps before delegating: a slow consumer.
///
/// ```
/// use std::time::Duration;
/// use z_event::{Handler, Store};
/// use z_event_testkit::handlers::SlowHandler;
///
/// fn slow<S: Store, H: Handler<S>>(handler: H) -> SlowHandler<H> {
///     SlowHandler::new(handler, Duration::from_millis(10))
/// }
/// ```
#[derive(Clone, Debug)]
pub struct SlowHandler<H> {
    inner: H,
    delay: Duration,
}

impl<H> SlowHandler<H> {
    /// Wraps `inner`, sleeping `delay` per event.
    pub fn new(inner: H, delay: Duration) -> Self {
        SlowHandler { inner, delay }
    }
}

impl<S: Store, H: Handler<S>> Handler<S> for SlowHandler<H> {
    async fn handle(&self, tx: &mut S::Tx, ev: &Event) -> Result<(), HandlerError> {
        tokio::time::sleep(self.delay).await;
        self.inner.handle(tx, ev).await
    }
}
