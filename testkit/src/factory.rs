use std::future::Future;

use z_event::{CursorStore, OutboxStore};

/// Creates fresh, empty storages for the suites.
///
/// Implemented for every closure `|| async { … }` returning a store.
///
/// ```
/// use z_event::{CursorStore, OutboxStore};
/// use z_event_testkit::StoreFactory;
///
/// // Any closure returning a future of a fresh store is a factory.
/// fn factory<S: OutboxStore + CursorStore + Clone + Default>() -> impl StoreFactory<Store = S> {
///     || async { S::default() }
/// }
/// ```
pub trait StoreFactory: Send + Sync + 'static {
    /// The store under test.
    type Store: OutboxStore + CursorStore + Clone;

    /// A new, empty storage (for SQL: a new schema or database).
    fn fresh(&self) -> impl Future<Output = Self::Store> + Send;
}

impl<F, Fut, S> StoreFactory for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = S> + Send,
    S: OutboxStore + CursorStore + Clone,
{
    type Store = S;

    fn fresh(&self) -> impl Future<Output = S> + Send {
        self()
    }
}
