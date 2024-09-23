use snafu::prelude::*;

/// Result type used in paimon.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error type for paimon.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(
        visibility(pub),
        display("Consensus execute command error {}", message)
    )]
    ExecuteCommandError { message: String },

    #[snafu(visibility(pub), display("Consensus io error {}", message))]
    ExecuteIOError { message: String },

    #[snafu(visibility(pub), display("Rpc io error {}", message))]
    RpcIOError { message: String },
}
