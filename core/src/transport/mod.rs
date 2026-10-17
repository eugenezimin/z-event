//! Transports built into the core. Network transports are adapters
//! (e.g. `z-event-grpc`).

mod in_process;

pub use in_process::InProcess;
