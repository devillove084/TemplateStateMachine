use super::acc::Acc;
use super::deps::Deps;
use super::id::{Ballot, Seq};
use super::status::Status;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance<C> {
    pub propose_ballot: Ballot,
    pub cmd: C,
    pub seq: Seq,
    pub deps: Deps,
    pub accepted_ballot: Ballot,
    pub status: Status,
    pub acc: Acc,
}
