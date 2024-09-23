use pro_macro::FromInner;
use serde::{Deserialize, Serialize};

use super::config::Configure;

#[derive(
    Default,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    FromInner,
    Hash,
)]
pub struct ReplicaID(usize);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, FromInner, Hash,
)]
pub struct LocalInstanceID(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, FromInner)]
pub struct Seq(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub struct Ballot {
    epoch: usize,
    base: usize,
    replica: ReplicaID,
}

impl Ballot {
    pub fn new(replica: ReplicaID, conf: &Configure) -> Self {
        Ballot {
            epoch: conf.epoch,
            base: 0,
            replica,
        }
    }

    pub fn is_init(&self) -> bool {
        self.base == 0
    }
}

#[derive(Debug, FromInner, Serialize, Deserialize)]
pub struct CommandLeaderID(usize);

#[derive(Debug, FromInner, Serialize, Deserialize)]
pub struct AcceptorID(usize);

impl From<ReplicaID> for CommandLeaderID {
    fn from(r: ReplicaID) -> Self {
        Self(*r)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InstanceID {
    pub replica: ReplicaID,
    pub local: LocalInstanceID,
}

impl InstanceID {
    pub fn new(replica: ReplicaID, local: LocalInstanceID) -> Self {
        Self { replica, local }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub enum InstanceStatus {
    PreAccepted,
    PreAcceptedEq,
    Accepted,
    Committed,
    Executed,
}

#[derive(Debug, Clone, Default)]
pub struct LeaderBook {
    pub accept_ok: usize,
    pub preaccept_ok: usize,
    pub nack: usize,
    pub max_ballot: Ballot,
    pub all_equal: bool,
}

impl LeaderBook {
    pub fn new(replica: ReplicaID, conf: &Configure) -> Self {
        LeaderBook {
            accept_ok: 0,
            preaccept_ok: 0,
            nack: 0,
            max_ballot: Ballot::new(replica, conf),
            all_equal: true,
        }
    }
}
