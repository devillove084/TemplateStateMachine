use serde::{Deserialize, Serialize};

use super::{
    types::{Ballot, CommandLeaderID, InstanceID, LocalInstanceID, Seq},
    Command,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct PreAccept {
    pub command_leader_id: CommandLeaderID,
    pub instance_id: InstanceID,
    pub seq: Seq,
    pub ballot: Ballot,
    pub cmds: Vec<Command>,
    pub deps: Vec<Option<LocalInstanceID>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PreAcceptReply {
    pub instance_id: InstanceID,
    pub seq: Seq,
    pub ballot: Ballot,
    pub ok: bool,
    pub deps: Vec<Option<LocalInstanceID>>,
    pub committed_deps: Vec<Option<LocalInstanceID>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PreAcceptOk {
    pub instance_id: InstanceID,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Accept {
    pub leader_id: CommandLeaderID,
    pub instance_id: InstanceID,
    pub ballot: Ballot,
    pub seq: Seq,
    pub cmd_cnt: usize,
    pub deps: Vec<Option<LocalInstanceID>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AcceptReply {
    pub instance_id: InstanceID,
    pub ok: bool,
    pub ballot: Ballot,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Commit {
    pub command_leader_id: CommandLeaderID,
    pub instance_id: InstanceID,
    pub seq: Seq,
    pub cmds: Vec<Command>,
    pub deps: Vec<Option<LocalInstanceID>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitShort {
    leader_id: CommandLeaderID,
    instance_id: InstanceID,
    seq: Seq,
    cmd_cnt: usize,
    deps: Vec<InstanceID>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Propose {
    pub cmds: Vec<Command>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    PreAccept(PreAccept),
    PreAcceptReply(PreAcceptReply),
    PreAcceptOk(PreAcceptOk),
    Accept(Accept),
    AcceptReply(AcceptReply),
    Commit(Commit),
    CommitShort(CommitShort),
    Propose(Propose),
}
