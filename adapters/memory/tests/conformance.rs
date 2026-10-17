//! The memory adapter (with the core's in-process transport) must pass
//! both conformance suites.

use z_event_memory::MemoryStore;

z_event_testkit::storage_suite!(|| async { MemoryStore::new() });

z_event_testkit::transport_suite!(
    || async { MemoryStore::new() },
    z_event_testkit::InProcessHarness
);
