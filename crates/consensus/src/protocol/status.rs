use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Status {
    PreAccepted = 1,
    Accepted = 2,
    Committed = 3,
    Issued = 5,
    Executed = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExecStatus {
    Committed = 3,
    Issuing = 4,
    Issued = 5,
    Executed = 6,
}
