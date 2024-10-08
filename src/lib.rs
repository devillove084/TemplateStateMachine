#![feature(async_closure)]

mod consensus;
pub use consensus::*;

mod state_machine;
pub use state_machine::*;

mod wal;
pub use wal::*;

mod error;
pub use error::*;

mod server;
pub use server::*;

mod consensus_service {
    tonic::include_proto!("consensus");
}
pub use consensus_service::*;

use std::sync::Once;
static INIT: Once = Once::new();

// This function ensures that the logger is initialized only once
pub fn init_tracing(level: tracing::Level) {
    INIT.call_once(|| {
        tracing_subscriber::fmt::Subscriber::builder()
            .with_max_level(level) // Adjust the log level if necessary
            .with_test_writer() // Ensures logs are output in test mode
            .init();
    });
}
