use std::net::SocketAddr;
use std::ops::Not;
use std::sync::Arc;

use ordered_vecmap::VecSet;

use crate::{Accept, CommandLike, Commit, Message, PreAccept, ReplicaId};

pub trait MemberNetwork<C: CommandLike>: Send + Sync + 'static {
    fn broadcast(&self, targets: VecSet<ReplicaId>, msg: Message<C>);
    fn send_one(&self, target: ReplicaId, msg: Message<C>);
    fn join(&self, replica_id: ReplicaId, addr: SocketAddr) -> Option<ReplicaId>;
    fn leave(&self, replica_id: ReplicaId);
}

pub fn broadcast_preaccept<C: CommandLike>(
    network: Arc<dyn MemberNetwork<C>>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: PreAccept<C>,
) {
    if acc.is_empty().not() {
        network.broadcast(
            acc,
            Message::PreAccept(PreAccept {
                sender: msg.sender,
                epoch: msg.epoch,
                id: msg.id,
                propose_ballot: msg.propose_ballot,
                cmd: None,
                seq: msg.seq,
                deps: msg.deps.clone(),
                acc: msg.acc.clone(),
            }),
        );
    }
    if others.is_empty().not() {
        assert!(msg.cmd.is_some());
        network.broadcast(others, Message::PreAccept(msg));
    }
}

pub fn broadcast_accept<C: CommandLike>(
    network: Arc<dyn MemberNetwork<C>>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: Accept<C>,
) {
    if acc.is_empty().not() {
        network.broadcast(
            acc,
            Message::Accept(Accept {
                sender: msg.sender,
                epoch: msg.epoch,
                id: msg.id,
                propose_ballot: msg.propose_ballot,
                cmd: None,
                seq: msg.seq,
                deps: msg.deps.clone(),
                acc: msg.acc.clone(),
            }),
        );
    }
    if others.is_empty().not() {
        assert!(msg.cmd.is_some());
        network.broadcast(others, Message::Accept(msg));
    }
}

pub fn broadcast_commit<C: CommandLike>(
    network: Arc<dyn MemberNetwork<C>>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: Commit<C>,
) {
    if acc.is_empty().not() {
        network.broadcast(
            acc,
            Message::Commit(Commit {
                sender: msg.sender,
                epoch: msg.epoch,
                id: msg.id,
                propose_ballot: msg.propose_ballot,
                cmd: None,
                seq: msg.seq,
                deps: msg.deps.clone(),
                acc: msg.acc.clone(),
            }),
        );
    }
    if others.is_empty().not() {
        assert!(msg.cmd.is_some());
        network.broadcast(others, Message::Commit(msg));
    }
}
