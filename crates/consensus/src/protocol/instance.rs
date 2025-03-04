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

impl<C: Default> Default for Instance<C> {
    fn default() -> Self {
        Self {
            propose_ballot: Ballot::default(),
            cmd: C::default(),
            seq: Seq::default(),
            deps: Deps::default(),
            accepted_ballot: Ballot::default(),
            status: Status::PreAccepted,
            acc: Acc::default(),
        }
    }
}
