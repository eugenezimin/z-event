//! The gRPC transport must pass the transport conformance suite over a real
//! HTTP/2 connection.

mod support;

z_event_testkit::transport_suite!(
    || async { z_event_memory::MemoryStore::new() },
    support::GrpcHarness
);
