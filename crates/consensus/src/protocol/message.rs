use super::acc::Acc;
use super::deps::Deps;
use super::id::*;
use super::instance::Instance;
use super::status::Status;

use crate::utils::time::LocalInstant;

use std::net::SocketAddr;

use ordered_vecmap::VecMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreAccept<C> {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub propose_ballot: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub acc: Acc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreAcceptOk {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub propose_ballot: Ballot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreAcceptDiff {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub propose_ballot: Ballot,
    pub seq: Seq,
    pub deps: Deps,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Accept<C> {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub propose_ballot: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub acc: Acc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptOk {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub propose_ballot: Ballot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit<C> {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub propose_ballot: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub acc: Acc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prepare {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub propose_ballot: Ballot,
    pub known: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareNack {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub propose_ballot: Ballot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareUnchosen {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareOk<C> {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub propose_ballot: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub accepted_ballot: Ballot,
    pub status: Status,
    pub acc: Acc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinOk {
    pub sender: ReplicaId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Leave {
    pub sender: ReplicaId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeRtt {
    pub sender: ReplicaId,
    pub time: LocalInstant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeRttOk {
    pub sender: ReplicaId,
    pub time: LocalInstant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AskLog {
    pub sender: ReplicaId,
    pub addr: SocketAddr,
    pub known_up_to: VecMap<ReplicaId, LocalInstanceId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncLog<C> {
    pub sender: ReplicaId,
    pub sync_id: SyncId,
    pub instances: Vec<(InstanceId, Instance<C>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncLogOk {
    pub sender: ReplicaId,
    pub sync_id: SyncId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerBounds {
    pub sender: ReplicaId,
    pub committed_up_to: Option<VecMap<ReplicaId, LocalInstanceId>>,
    pub executed_up_to: Option<VecMap<ReplicaId, LocalInstanceId>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message<C> {
    PreAccept(PreAccept<C>),
    PreAcceptOk(PreAcceptOk),
    PreAcceptDiff(PreAcceptDiff),
    Accept(Accept<C>),
    AcceptOk(AcceptOk),
    Commit(Commit<C>),
    Prepare(Prepare),
    PrepareOk(PrepareOk<C>),
    PrepareNack(PrepareNack),
    PrepareUnchosen(PrepareUnchosen),
    Join(Join),
    JoinOk(JoinOk),
    Leave(Leave),
    ProbeRtt(ProbeRtt),
    ProbeRttOk(ProbeRttOk),
    AskLog(AskLog),
    SyncLog(SyncLog<C>),
    SyncLogOk(SyncLogOk),
    PeerBounds(PeerBounds),
}

pub enum PreAcceptReply {
    Ok(PreAcceptOk),
    Diff(PreAcceptDiff),
}

pub enum AcceptReply {
    Ok(AcceptOk),
}

pub enum PrepareReply<C> {
    Ok(PrepareOk<C>),
    Nack(PrepareNack),
    Unchosen(PrepareUnchosen),
}

impl PreAcceptReply {
    pub fn convert<C>(msg: Message<C>) -> Option<Self> {
        match msg {
            Message::PreAcceptOk(msg) => Some(Self::Ok(msg)),
            Message::PreAcceptDiff(msg) => Some(Self::Diff(msg)),
            _ => None,
        }
    }
}

impl AcceptReply {
    pub fn convert<C>(msg: Message<C>) -> Option<Self> {
        match msg {
            Message::AcceptOk(msg) => Some(Self::Ok(msg)),
            _ => None,
        }
    }
}

impl<C> PrepareReply<C> {
    pub fn convert(msg: Message<C>) -> Option<Self> {
        match msg {
            Message::PrepareOk(msg) => Some(Self::Ok(msg)),
            Message::PrepareNack(msg) => Some(Self::Nack(msg)),
            Message::PrepareUnchosen(msg) => Some(Self::Unchosen(msg)),
            _ => None,
        }
    }
}

impl<C> Message<C> {
    pub fn variant_name(&self) -> &'static str {
        match self {
            Message::PreAccept(_) => "PreAccept",
            Message::PreAcceptOk(_) => "PreAcceptOk",
            Message::PreAcceptDiff(_) => "PreAcceptDiff",
            Message::Accept(_) => "Accept",
            Message::AcceptOk(_) => "AcceptOk",
            Message::Commit(_) => "Commit",
            Message::Prepare(_) => "Prepare",
            Message::PrepareOk(_) => "PrepareOk",
            Message::PrepareNack(_) => "PrepareNack",
            Message::PrepareUnchosen(_) => "PrepareUnchosen",
            Message::Join(_) => "Join",
            Message::JoinOk(_) => "JoinOk",
            Message::Leave(_) => "Leave",
            Message::ProbeRtt(_) => "ProbeRtt",
            Message::ProbeRttOk(_) => "ProbeRttOk",
            Message::AskLog(_) => "AskLog",
            Message::SyncLog(_) => "SyncLog",
            Message::SyncLogOk(_) => "SyncLogOk",
            Message::PeerBounds(_) => "PeerBounds",
        }
    }
}
