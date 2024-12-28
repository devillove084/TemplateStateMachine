#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::arithmetic_side_effects,
    clippy::must_use_candidate
)]
#![warn(clippy::todo, clippy::dbg_macro)]

mod rpc;
pub use rpc::*;

mod client_call_server;
pub use client_call_server::*;

mod net;

mod config;
pub use config::*;

mod serve;
pub use serve::*;