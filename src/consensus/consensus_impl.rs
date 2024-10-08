use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::error;
use tracing::info;

use super::instance_exist;
use super::replica::Replica;
use super::InstanceStatus;
use super::LeaderBook;
use super::PeerCommunication;
use crate::consensus_message::Message;
use crate::consensus_message::Message::*;
use crate::Accept;
use crate::AcceptReply;
use crate::Ballot;
use crate::CommandLeaderId;
use crate::Commit;
use crate::CommitShort;
use crate::ConsensusMessage;
use crate::InstanceId;
use crate::PreAccept;
use crate::PreAcceptOk;
use crate::PreAcceptReply;
use crate::Propose;
use crate::ReplicaId;
use crate::{
    consensus::instance::{Instance, SharedInstance},
    consensus_service_server::ConsensusService,
};
#[derive(Clone)]
pub struct ConsensusImpl {
    // conf: Configure,
    replica: Arc<Mutex<Replica>>,
    peer_communication: Arc<PeerCommunication>,

    #[cfg(test)]
    tick_receiver: Option<Arc<Mutex<mpsc::Receiver<()>>>>,

    #[cfg(test)]
    tick_sender: Option<Arc<Mutex<mpsc::Sender<()>>>>,
}

#[tonic::async_trait]
impl ConsensusService for ConsensusImpl {
    type StreamMessagesStream =
        Pin<Box<dyn Stream<Item = Result<ConsensusMessage, Status>> + Send + Sync + 'static>>;

    async fn stream_messages(
        &self,
        request: Request<tonic::Streaming<ConsensusMessage>>,
    ) -> Result<Response<Self::StreamMessagesStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(1024);
        let self_clone = self.clone();

        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(message) => {
                        self_clone.handle_message(message, tx.clone()).await;
                    }
                    Err(e) => {
                        error!("Error receiving message: {:?}", e);
                        break;
                    }
                }
            }
        });

        let outbound = ReceiverStream::new(rx);

        Ok(Response::new(
            Box::pin(outbound) as Self::StreamMessagesStream
        ))
    }
}

impl ConsensusImpl {
    pub fn new(
        replica: Arc<Mutex<Replica>>,
        peer_communication: Arc<PeerCommunication>,
    ) -> Arc<Self> {
        #[cfg(test)]
        {
            let (tx, rx) = mpsc::channel(1);
            Arc::new(Self {
                replica,
                peer_communication,
                tick_sender: Some(Arc::new(Mutex::new(tx))),
                tick_receiver: Some(Arc::new(Mutex::new(rx))),
            })
        }
        #[cfg(not(test))]
        {
            Arc::new(Self {
                replica,
                peer_communication,
            })
        }
    }

    #[cfg(test)]
    pub async fn tick(&self) -> crate::Result<()> {
        if let Some(ref tx) = self.tick_sender {
            let tx_lock = tx.lock().await;
            tx_lock.send(()).await.expect("tick error");
            info!("tick: Tick sent");
        }
        Ok(())
    }

    #[cfg(test)]
    async fn wait_for_tick(&self) -> crate::Result<()> {
        if let Some(ref rx) = self.tick_receiver {
            let mut rx_lock = rx.lock().await;
            rx_lock.recv().await.expect("wait for tick");
            info!("wait_for_tick: Tick received");
        }
        Ok(())
    }

    pub async fn handle_message<T>(&self, message: ConsensusMessage, _tx: Sender<T>) {
        if let Some(message) = message.message {
            let res = match message {
                Propose(propose) => {
                    info!("handle_message: Handling Propose message");
                    self.handle_propose(propose).await
                }
                PreAccept(pre_accept) => {
                    info!("handle_message: Handling PreAccept message");
                    self.handle_preaccept(pre_accept).await
                }
                PreAcceptReply(pre_accept_reply) => {
                    info!("handle_message: Handling PreAcceptReply message");
                    self.handle_preaccept_reply(pre_accept_reply).await
                }
                PreAcceptOk(pre_accept_ok) => {
                    info!("handle_message: Handling PreAcceptOk message");
                    self.handle_preaccept_ok(pre_accept_ok).await
                }
                Accept(accept) => {
                    info!("handle_message: Handling Accept message");
                    self.handle_accept(accept).await
                }
                AcceptReply(accept_reply) => {
                    info!("handle_message: Handling AcceptReply message");
                    self.handle_accept_reply(accept_reply).await
                }
                Commit(commit) => {
                    info!("handle_message: Handling Commit message");
                    self.handle_commit(commit).await
                }
                CommitShort(commit_short) => {
                    info!("handle_message: Handling CommitShort message");
                    self.handle_commit_short(commit_short).await
                }
                Empty(_) => unreachable!(),
            };
            if let Err(e) = res {
                error!(
                    "handle_message: Error occurred while handling message: {:?}",
                    e
                );
            }

            #[cfg(test)]
            {
                if let Err(e) = self.wait_for_tick().await {
                    error!("handle_message: Failed to wait for tick: {:?}", e);
                }
            }
        }
    }

    async fn handle_propose(&self, propose: Propose) -> crate::Result<()> {
        info!("handle_propose: Starting handle propose");
        let mut replica = self.replica.lock().await;

        let instance_id = *replica.local_cur_instance();
        info!(
            "handle_propose: Current local instance ID: {:?}",
            instance_id
        );

        replica.inc_local_cur_instance();
        info!("handle_propose: Incremented local instance ID");

        let (mut seq, deps) = replica.get_seq_deps(&propose.cmds).await;
        info!("handle_propose: Obtained seq: {:?}, deps: {:?}", seq, deps);

        let new_shared_instance = SharedInstance::new(
            Some(Instance {
                id: InstanceId {
                    replica_id: Some(replica.id),
                    local_instance_id: Some(instance_id),
                },
                seq,
                ballot: self.new_ballot(replica.id).await,
                cmds: propose.cmds.clone(),
                deps: deps.clone(),
                status: InstanceStatus::PreAccepted,
                leaderbook: self.new_leaderbook(replica.id).await,
            }),
            None,
        );
        info!("handle_propose: Created new shared instance");

        let new_shared_instance_read = new_shared_instance.get_instance_read().await;
        let new_instance_read_inner = new_shared_instance_read.as_ref().unwrap();
        info!(
            "handle_propose: Read new instance, commands: {:?}",
            new_instance_read_inner.cmds
        );

        let replica_id = replica.id;
        info!(
            "handle_propose: Updating conflicts for replica: {:?}",
            replica_id
        );
        replica
            .update_conflicts(
                &replica_id,
                &new_instance_read_inner.cmds,
                new_shared_instance.clone(),
            )
            .await;
        info!("handle_propose: Conflicts updated");

        info!("handle_propose: Inserting new instance into instance space");
        replica
            .instance_space
            .insert_instance(&replica_id, &instance_id, new_shared_instance.clone())
            .await;
        info!("handle_propose: New instance inserted into instance space");

        if seq > replica.max_seq {
            seq.value += 1;
            replica.max_seq = seq;
            info!("handle_propose: Updated max seq to {:?}", replica.max_seq);
        }

        #[cfg(test)]
        let _ = self.wait_for_tick().await;

        info!("handle_propose: Broadcasting PreAccept message");
        self.broadcast_message(PreAccept(PreAccept {
            command_leader_id: Some(CommandLeaderId {
                value: replica_id.value,
            }),
            instance_id: Some(InstanceId {
                replica_id: Some(replica.id),
                local_instance_id: Some(instance_id),
            }),
            seq: Some(seq),
            ballot: Some(self.new_ballot(replica_id).await),
            cmds: new_instance_read_inner.cmds.clone(),
            deps: new_instance_read_inner.deps.clone(),
        }))
        .await
    }

    async fn handle_preaccept(&self, preaccept: PreAccept) -> crate::Result<()> {
        info!(
            "handle_preaccept: Handling PreAccept message: {:?}",
            preaccept
        );
        let mut replica = self.replica.lock().await;
        let instance = replica
            .instance_space
            .get_instance(
                &preaccept.instance_id.unwrap().replica_id.unwrap(),
                &preaccept.instance_id.unwrap().local_instance_id.unwrap(),
            )
            .await;

        if instance_exist(&instance).await {
            // TODO: abstract to a macro
            info!("handle_preaccept: Instance already exists, checking status");
            let instance = instance.unwrap();
            let instance_read = instance.get_instance_read().await;
            let instance_read_inner = instance_read.as_ref().unwrap();

            // We have got accept or commit before, do not reply
            if matches!(
                instance_read_inner.status,
                InstanceStatus::Committed | InstanceStatus::Accepted
            ) {
                // Later message may not contain commands, we should fill it here
                if instance_read_inner.cmds.is_empty() {
                    drop(instance_read);
                    // TODO: abstract to a macro
                    let mut instance_write = instance.get_instance_write().await;
                    let instance_write_inner = instance_write.as_mut().unwrap();
                    instance_write_inner.cmds = preaccept.cmds;
                }
                info!("handle_preaccept: Instance already committed or accepted, skipping");
                return Ok(());
            }

            // smaller ballot number
            if preaccept.ballot.unwrap() < instance_read_inner.ballot {
                info!("handle_preaccept: Received smaller ballot number, sending PreAcceptReply with ok=false");
                return self
                    .reply(
                        &preaccept.command_leader_id.unwrap(),
                        Message::PreAcceptReply(PreAcceptReply {
                            instance_id: preaccept.instance_id,
                            seq: Some(instance_read_inner.seq),
                            ballot: Some(instance_read_inner.ballot),
                            ok: false,
                            deps: instance_read_inner.deps.clone(),
                            committed_deps: replica.committed_upto.clone(),
                        }),
                    )
                    .await;
            }
        }

        if preaccept.instance_id.unwrap().local_instance_id.unwrap()
            > replica.cur_instance(&preaccept.instance_id.unwrap().replica_id.unwrap())
        {
            replica.set_cur_instance(&preaccept.instance_id.unwrap());
        }

        // TODO: We have better not copy dep vec.
        let (seq, deps, changed) = replica
            .update_seq_deps(
                preaccept.seq.unwrap(),
                preaccept.deps.clone(),
                &preaccept.cmds,
            )
            .await;

        let status = if changed {
            InstanceStatus::PreAccepted
        } else {
            InstanceStatus::PreAcceptedEq
        };

        let uncommitted_deps = replica
            .committed_upto
            .iter()
            .enumerate()
            .map(|cu| {
                {
                    let cu_id = cu.1;
                    // 1 -> local instance id
                    // if deps[cu.0].value != u64::MAX {
                    //     if cu_id < &deps[cu.0] {
                    //         return true;
                    //     }
                    // }
                    if cu_id < &deps[cu.0] {
                        return true;
                    }
                }
                false
            })
            .filter(|a| *a)
            .count()
            > 0;

        let new_instance = SharedInstance::new(
            Some(Instance {
                id: preaccept.instance_id.unwrap(),
                seq,
                ballot: preaccept.ballot.unwrap(),
                // TODO: cmds and deps should not copy
                cmds: preaccept.cmds.clone(),
                deps: deps.clone(),
                status,
                leaderbook: self.new_leaderbook(replica.id).await,
            }),
            None,
        );

        replica
            .instance_space
            .insert_instance(
                &preaccept.instance_id.unwrap().replica_id.unwrap(),
                &preaccept.instance_id.unwrap().local_instance_id.unwrap(),
                new_instance.clone(),
            )
            .await;
        replica
            .update_conflicts(
                &preaccept.instance_id.unwrap().replica_id.unwrap(),
                &preaccept.cmds,
                new_instance,
            )
            .await;

        // TODO: sync to disk

        if changed
            || uncommitted_deps
            || preaccept
                .instance_id
                .as_ref()
                .unwrap()
                .replica_id
                .as_ref()
                .unwrap()
                .value
                != preaccept.command_leader_id.as_ref().unwrap().value
            || !self.is_init_ballot(preaccept.ballot.as_ref().unwrap())
        {
            self.reply(
                preaccept.command_leader_id.as_ref().unwrap(),
                Message::PreAcceptReply(PreAcceptReply {
                    instance_id: preaccept.instance_id,
                    seq: Some(seq),
                    ballot: preaccept.ballot,
                    ok: true,
                    deps: deps.clone(),
                    committed_deps: replica.committed_upto.clone(),
                }),
            )
            .await?;
        } else {
            self.reply(
                preaccept.command_leader_id.as_ref().unwrap(),
                Message::PreAcceptOk(PreAcceptOk {
                    instance_id: preaccept.instance_id,
                }),
            )
            .await?;
        }

        Ok(())
    }

    async fn handle_preaccept_reply(&self, pre_accept_reply: PreAcceptReply) -> crate::Result<()> {
        let replica = self.replica.lock().await;
        let replica_id = pre_accept_reply.instance_id.unwrap().replica_id.unwrap();
        let local_instance_id = pre_accept_reply
            .instance_id
            .unwrap()
            .local_instance_id
            .unwrap();

        let instance = replica
            .instance_space
            .get_instance(&replica_id, &local_instance_id)
            .await;

        // TODO: Error process
        assert!(
            (instance_exist(&instance).await),
            "this instance should already in the space"
        );

        // we have checked the existence
        let orig = instance.unwrap();
        let mut instance_w = orig.get_instance_write().await;
        let instance_w_inner = instance_w.as_mut().unwrap();

        if !matches!(instance_w_inner.status, InstanceStatus::PreAccepted) {
            // we have translated to the later states
            return Ok(());
        }

        if instance_w_inner.ballot != pre_accept_reply.ballot.unwrap() {
            // other advanced (large ballot) command leader is handling
            return Ok(());
        }

        if !pre_accept_reply.ok {
            instance_w_inner.leaderbook.nack += 1;
            if pre_accept_reply.ballot.unwrap() > instance_w_inner.leaderbook.max_ballot {
                instance_w_inner.leaderbook.max_ballot = pre_accept_reply.ballot.unwrap();
            }
            return Ok(());
        }
        instance_w_inner.leaderbook.preaccept_ok += 1;

        let equal = replica.merge_seq_deps(
            instance_w_inner,
            &pre_accept_reply.seq.unwrap(),
            &pre_accept_reply.deps,
        );
        if instance_w_inner.leaderbook.preaccept_ok > 1 {
            instance_w_inner.leaderbook.all_equal = instance_w_inner.leaderbook.all_equal && equal;
        }

        if instance_w_inner.leaderbook.preaccept_ok >= replica.peer_cnt / 2
            && instance_w_inner.leaderbook.all_equal
            && self.is_init_ballot(&instance_w_inner.ballot)
        {
            instance_w_inner.status = InstanceStatus::Committed;
            // TODO: sync to disk
            let _ = self
                .broadcast_message(
                    // replica.id,
                    Message::Commit(Commit {
                        command_leader_id: Some(CommandLeaderId {
                            value: replica_id.value,
                        }),
                        instance_id: pre_accept_reply.instance_id,
                        seq: Some(instance_w_inner.seq),
                        cmds: instance_w_inner.cmds.clone(),
                        deps: instance_w_inner.deps.clone(),
                    }),
                )
                .await;
            drop(instance_w);
            let _ = replica.exec_send.send(orig.clone()).await;
            orig.notify_commit().await;
        } else if instance_w_inner.leaderbook.preaccept_ok >= replica.peer_cnt / 2 {
            instance_w_inner.status = InstanceStatus::Accepted;
            let _ = self
                .broadcast_message(Message::Accept(Accept {
                    command_leader_id: Some(CommandLeaderId {
                        value: replica_id.value,
                    }),
                    instance_id: pre_accept_reply.instance_id,
                    ballot: Some(instance_w_inner.ballot),
                    seq: Some(instance_w_inner.seq),
                    cmd_cnt: instance_w_inner.cmds.len() as u64,
                    deps: instance_w_inner.deps.clone(),
                }))
                .await;
        }
        Ok(())
    }

    async fn handle_preaccept_ok(&self, pre_accept_ok: PreAcceptOk) -> crate::Result<()> {
        let replica = self.replica.lock().await;
        let replica_id = pre_accept_ok.instance_id.unwrap().replica_id.unwrap();
        let local_instance_id = pre_accept_ok
            .instance_id
            .unwrap()
            .local_instance_id
            .unwrap();

        let instance = replica
            .instance_space
            .get_instance(&replica_id, &local_instance_id)
            .await;

        assert!(
            (instance_exist(&instance).await),
            "This instance should already in the space"
        );

        let instance = instance.unwrap();
        let mut instance_write = instance.get_instance_write().await;
        let instance_write_inner = instance_write.as_mut().unwrap();

        if !matches!(instance_write_inner.status, InstanceStatus::PreAccepted) {
            // We have translated to the later states
            return Ok(());
        }

        if !self.is_init_ballot(&instance_write_inner.ballot) {
            // only the first leader can send ok
            return Ok(());
        }

        instance_write_inner.leaderbook.preaccept_ok += 1;

        // TODO: remove duplicate code
        if instance_write_inner.leaderbook.preaccept_ok >= replica.peer_cnt / 2
            && instance_write_inner.leaderbook.all_equal
            && self.is_init_ballot(&instance_write_inner.ballot)
        {
            instance_write_inner.status = InstanceStatus::Committed;
            // TODO: sync to disk
            let _ = self
                .broadcast_message(Message::Commit(Commit {
                    command_leader_id: Some(CommandLeaderId {
                        value: replica.id.value,
                    }),
                    instance_id: pre_accept_ok.instance_id,
                    seq: Some(instance_write_inner.seq),
                    cmds: instance_write_inner.cmds.clone(),
                    deps: instance_write_inner.deps.clone(),
                }))
                .await;
            drop(instance_write);
            let _ = replica.exec_send.send(instance.clone()).await;
            instance.notify_commit().await;
        } else if instance_write_inner.leaderbook.preaccept_ok >= replica.peer_cnt / 2 {
            instance_write_inner.status = InstanceStatus::Accepted;
            let _ = self
                .broadcast_message(Message::Accept(Accept {
                    command_leader_id: Some(CommandLeaderId {
                        value: replica.id.value,
                    }),
                    instance_id: pre_accept_ok.instance_id,
                    ballot: Some(instance_write_inner.ballot),
                    seq: Some(instance_write_inner.seq),
                    cmd_cnt: instance_write_inner.cmds.len() as u64,
                    deps: instance_write_inner.deps.clone(),
                }))
                .await;
        }
        Ok(())
    }

    async fn handle_accept(&self, accept: Accept) -> crate::Result<()> {
        let mut replica = self.replica.lock().await;
        let replica_id = accept.instance_id.unwrap().replica_id.unwrap();
        let mut local_instance_id = accept.instance_id.unwrap().local_instance_id.unwrap();

        if local_instance_id >= replica.cur_instance(&replica_id) {
            local_instance_id.value += 1;
            replica.set_cur_instance(&InstanceId {
                replica_id: Some(replica_id),
                local_instance_id: Some(local_instance_id),
            });
        }

        let instance = replica
            .instance_space
            .get_instance(&replica_id, &local_instance_id)
            .await;

        let exist = instance_exist(&instance).await;
        if exist {
            let instance = instance.unwrap();
            let instance_read = instance.get_instance_read().await;
            let instance_read_inner = instance_read.as_ref().unwrap();
            if matches!(
                instance_read_inner.status,
                InstanceStatus::Committed | InstanceStatus::Executed
            ) {
                // We' ve translate to the later states
                return Ok(());
            }

            let instance_ballot = instance_read_inner.ballot;
            if accept.ballot.unwrap() < instance_ballot {
                return self
                    .reply(
                        &accept.command_leader_id.unwrap(),
                        Message::AcceptReply(AcceptReply {
                            instance_id: accept.instance_id,
                            ok: false,
                            ballot: Some(instance_ballot),
                        }),
                    )
                    .await;
            }

            drop(instance_read);

            let mut instance_write = instance.get_instance_write().await;
            let instance_write_inner = instance_write.as_mut().unwrap();

            instance_write_inner.status = InstanceStatus::Accepted;
            instance_write_inner.seq = accept.seq.unwrap();
            instance_write_inner.deps = accept.deps;
        } else {
            // FIXME: Message reordering?
            let new_instance = SharedInstance::new(
                Some(Instance {
                    id: accept.instance_id.unwrap(),
                    seq: accept.seq.unwrap(),
                    ballot: accept.ballot.unwrap(),
                    cmds: vec![],
                    deps: accept.deps,
                    status: InstanceStatus::Accepted,
                    leaderbook: self.new_leaderbook(replica_id).await,
                }),
                None,
            );
            replica
                .instance_space
                .insert_instance(&replica_id, &local_instance_id, new_instance)
                .await;
        }

        // TODO: sync to disk

        let _ = self
            .reply(
                &accept.command_leader_id.unwrap(),
                Message::AcceptReply(AcceptReply {
                    instance_id: accept.instance_id,
                    ok: true,
                    ballot: accept.ballot,
                }),
            )
            .await;
        Ok(())
    }

    async fn handle_accept_reply(&self, accept_reply: AcceptReply) -> crate::Result<()> {
        let replica = self.replica.lock().await;
        let replica_id = accept_reply.instance_id.unwrap().replica_id.unwrap();
        let local_instance_id = accept_reply.instance_id.unwrap().local_instance_id.unwrap();

        let instance = replica
            .instance_space
            .get_instance(&replica_id, &local_instance_id)
            .await;

        // TODO: Error processing
        assert!(
            (instance_exist(&instance).await),
            "The instance {:?} should exist",
            accept_reply.instance_id
        );

        let instance = instance.unwrap();
        let mut instance_write = instance.get_instance_write().await;
        let instance_write_inner = instance_write.as_mut().unwrap();

        if !accept_reply.ok {
            instance_write_inner.leaderbook.nack += 1;
            if accept_reply.ballot.unwrap() > instance_write_inner.leaderbook.max_ballot {
                instance_write_inner.leaderbook.max_ballot = accept_reply.ballot.unwrap();
            }
            return Ok(());
        }

        instance_write_inner.leaderbook.accept_ok += 1;

        if instance_write_inner.leaderbook.accept_ok >= replica.peer_cnt / 2 {
            instance_write_inner.status = InstanceStatus::Committed;
            // TODO: sync to disk

            let _ = self
                .broadcast_message(Message::Commit(Commit {
                    command_leader_id: Some(CommandLeaderId {
                        value: replica.id.value,
                    }),
                    instance_id: accept_reply.instance_id,
                    seq: Some(instance_write_inner.seq),
                    cmds: instance_write_inner.cmds.clone(),
                    deps: instance_write_inner.deps.clone(),
                }))
                .await;

            drop(instance_write);
            // TODO: sync to disk
            let _ = replica.exec_send.send(instance.clone()).await;
            instance.notify_commit().await;
        }
        Ok(())
    }

    async fn handle_commit(&self, commit: Commit) -> crate::Result<()> {
        let mut replica = self.replica.lock().await;
        let replica_id = commit.instance_id.unwrap().replica_id.unwrap();
        let mut local_instance_id = commit.instance_id.unwrap().local_instance_id.unwrap();

        if local_instance_id >= replica.cur_instance(&replica_id) {
            local_instance_id.value += 1;
            replica.set_cur_instance(&InstanceId {
                replica_id: Some(replica_id),
                local_instance_id: Some(local_instance_id),
            });
        }

        let instance = replica
            .instance_space
            .get_instance(&replica_id, &local_instance_id)
            .await;
        let exist = instance_exist(&instance).await;

        let instance = if exist {
            let instance = instance.unwrap();
            let mut instance_write = instance.get_instance_write().await;
            let instance_write_inner = instance_write.as_mut().unwrap();
            instance_write_inner.seq = commit.seq.unwrap();
            instance_write_inner.deps = commit.deps;
            instance_write_inner.status = InstanceStatus::Committed;
            drop(instance_write);
            instance
        } else {
            let new_instance = SharedInstance::new(
                Some(Instance {
                    id: commit.instance_id.unwrap(),
                    seq: commit.seq.unwrap(),
                    ballot: self.new_ballot(replica_id).await,
                    cmds: commit.cmds.clone(),
                    deps: commit.deps,
                    status: InstanceStatus::Committed,
                    leaderbook: self.new_leaderbook(replica_id).await,
                }),
                None,
            );
            replica
                .update_conflicts(&replica_id, &commit.cmds, new_instance.clone())
                .await;
            new_instance
        };

        // TODO: sync to disk
        // TODO: handle errors

        let _ = replica.exec_send.send(instance.clone()).await;
        instance.notify_commit().await;
        Ok(())
    }

    async fn handle_commit_short(&self, _commit_short: CommitShort) -> crate::Result<()> {
        unreachable!()
    }

    async fn broadcast_message(&self, message: Message) -> crate::Result<()> {
        let consensus_message = ConsensusMessage {
            message: Some(message),
        };
        self.peer_communication
            .broadcast_message(consensus_message)
            .await
    }

    async fn reply(
        &self,
        target_replica_id: &CommandLeaderId,
        message: Message,
    ) -> crate::Result<()> {
        let consensus_message = ConsensusMessage {
            message: Some(message),
        };
        let target_replica_id = ReplicaId {
            value: target_replica_id.value,
        };
        self.peer_communication
            .send_message(&target_replica_id, consensus_message)
            .await
    }

    pub async fn new_ballot(&self, replica_id: ReplicaId) -> Ballot {
        Ballot {
            epoch: 0,
            base: 0,
            replica_id: Some(replica_id),
        }
    }

    pub async fn new_leaderbook(&self, replica_id: ReplicaId) -> LeaderBook {
        LeaderBook::new(replica_id)
    }

    fn is_init_ballot(&self, ballot: &Ballot) -> bool {
        ballot.base == 0
    }

    pub async fn get_replica_id(&self) -> ReplicaId {
        self.replica.lock().await.id
    }
}

#[cfg(test)]
mod consensus_unit_test {
    use crate::consensus::PeerCommunication;
    use crate::init_tracing;
    use crate::run_server;
    use crate::CommandType;
    use crate::ConsensusImpl;
    use crate::ConsensusMessage;
    use crate::InstanceStatus;
    use crate::Propose;
    use crate::Replica;
    use crate::{consensus_message::Message, Command, LocalInstanceId, ReplicaId, StateMachine};
    use bytes::Bytes;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tracing::info;

    #[ctor::ctor]
    fn setup_tracing_for_tests() {
        init_tracing(tracing::Level::INFO);
    }

    struct MockStateMachine {
        executed_commands: Mutex<Vec<Command>>,
    }

    impl MockStateMachine {
        pub fn new() -> Self {
            Self {
                executed_commands: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateMachine for MockStateMachine {
        async fn apply(&self, command: &Command) -> crate::Result<()> {
            self.executed_commands.lock().await.push(command.clone());
            Ok(())
        }
    }

    // Helper function to set up a Replica with PeerCommunication
    async fn setup_replica(
        replica_id: u64,
        peer_count: usize,
        port: u16,
    ) -> (Arc<ConsensusImpl>, Arc<PeerCommunication>) {
        let state_machine = Arc::new(MockStateMachine::new());
        let replica = Arc::new(Mutex::new(Replica::new(
            replica_id as usize,
            peer_count,
            state_machine.clone(),
        )));
        let peer_comm = Arc::new(PeerCommunication::new());
        let consensus_impl = ConsensusImpl::new(replica.clone(), peer_comm.clone());
        let consensus_clone = consensus_impl.clone();
        tokio::spawn(async move {
            if let Err(e) = run_server(port, consensus_impl.clone()).await {
                info!("Server failed to run on port {}: {:?}", port, e);
            }
        });
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        (consensus_clone, peer_comm)
    }

    async fn connect_all_replicas(
        peer_comm1: Arc<PeerCommunication>,
        peer_comm2: Arc<PeerCommunication>,
        peer_comm3: Arc<PeerCommunication>,
        consensus1: Arc<ConsensusImpl>,
        consensus2: Arc<ConsensusImpl>,
        consensus3: Arc<ConsensusImpl>,
    ) {
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8001);
        let addr3 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8002);

        let peer_comm1_clone = peer_comm1.clone();
        let consensus_impl_2 = consensus2.clone();
        let consensus_impl_3 = consensus3.clone();

        let _ = peer_comm1_clone
            .clone()
            .connect_to_peer(ReplicaId { value: 1 }, addr2, consensus_impl_2.clone())
            .await;
        let _ = peer_comm1_clone
            .connect_to_peer(ReplicaId { value: 1 }, addr3, consensus_impl_3.clone())
            .await;

        let peer_comm2_clone = peer_comm2.clone();
        let consensus_impl_1 = consensus1.clone();
        let consensus_impl_3 = consensus3.clone();

        let _ = peer_comm2_clone
            .clone()
            .connect_to_peer(ReplicaId { value: 2 }, addr1, consensus_impl_1.clone())
            .await;
        let _ = peer_comm2_clone
            .connect_to_peer(ReplicaId { value: 2 }, addr3, consensus_impl_3.clone())
            .await;

        let peer_comm3_clone = peer_comm3.clone();
        let consensus_impl_1 = consensus1.clone();
        let consensus_impl_2 = consensus2.clone();

        let _ = peer_comm3_clone
            .clone()
            .connect_to_peer(ReplicaId { value: 3 }, addr1, consensus_impl_1.clone())
            .await;
        let _ = peer_comm3_clone
            .connect_to_peer(ReplicaId { value: 3 }, addr2, consensus_impl_2.clone())
            .await;

        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        assert!(peer_comm1.peers.contains_key(&ReplicaId { value: 2 }));
        assert!(peer_comm1.peers.contains_key(&ReplicaId { value: 3 }));
        assert!(peer_comm2.peers.contains_key(&ReplicaId { value: 1 }));
        assert!(peer_comm2.peers.contains_key(&ReplicaId { value: 3 }));
        assert!(peer_comm3.peers.contains_key(&ReplicaId { value: 1 }));
        assert!(peer_comm3.peers.contains_key(&ReplicaId { value: 2 }));
    }

    async fn validate_instance_state(
        replica: &Replica,
        instance_id: LocalInstanceId,
        expected_status: InstanceStatus,
    ) {
        let instance = replica
            .instance_space
            .get_instance(&replica.id, &instance_id)
            .await;
        assert!(instance.is_some(), "Instance should exist, but not");
        let instance_read = instance.unwrap().get_instance_read().await.clone().unwrap();

        // let all_instances = replica.instance_space.get_all_instance().await;
        // assert!(all_instances.is_some(), "Instance should exist, but not");
        // let all_instances = all_instances.unwrap();
        // let len = all_instances.len();
        // info!("len is ----> {}", len);
        // // assert!(len == 3, "Instance should exist, but not");
        // let instance = &all_instances[0];
        // let instance_read = instance.get_instance_read().await.clone().unwrap();
        assert_eq!(
            instance_read.status, expected_status,
            "Instance status should match"
        );
    }

    #[tokio::test]
    async fn test_propose_phase() -> crate::Result<()> {
        // Set up and connect the replicas
        let (replica1, replica1_peer_comm) = setup_replica(1, 3, 8000).await;
        let (replica2, replica2_peer_comm) = setup_replica(2, 3, 8001).await;
        let (replica3, replica3_peer_comm) = setup_replica(3, 3, 8002).await;
        connect_all_replicas(
            replica1_peer_comm.clone(),
            replica2_peer_comm.clone(),
            replica3_peer_comm.clone(),
            replica1.clone(),
            replica2.clone(),
            replica3.clone(),
        )
        .await;

        assert!(replica1_peer_comm
            .peers
            .contains_key(&ReplicaId { value: 2 }));
        assert!(replica1_peer_comm
            .peers
            .contains_key(&ReplicaId { value: 3 }));

        // Send Propose message to replica1
        let propose_command = Command {
            command_type: CommandType::Add.into(),
            key: Bytes::from_static(b"key1").to_vec(),
            value: Some(Bytes::from_static(b"value1").to_vec()),
        };
        let propose_message = ConsensusMessage {
            message: Some(Message::Propose(Propose {
                cmds: vec![propose_command],
            })),
        };
        // let replica1_r = (replica1.replica.lock().await).clone();
        let replica2_r = (replica2.replica.lock().await).clone();
        // let replica3_r = (replica3.replica.lock().await).clone();
        let replica2_id = replica2_r.id;
        replica1_peer_comm
            .send_message(&replica2_id, propose_message)
            .await?;

        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Validate that the instance is in PreAccepted state
        // validate_instance_state(
        //     &replica1_r,
        //     LocalInstanceId { value: 0 },
        //     InstanceStatus::Executed,
        // )
        // .await;
        validate_instance_state(
            &replica2_r,
            LocalInstanceId { value: 0 },
            InstanceStatus::PreAccepted,
        )
        .await;

        let _ = replica2.tick().await;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // validate_instance_state(
        //     &replica1_r,
        //     LocalInstanceId { value: 0 },
        //     InstanceStatus::PreAccepted,
        // )
        // .await;
        // validate_instance_state(
        //     &replica3_r,
        //     LocalInstanceId { value: 0 },
        //     InstanceStatus::PreAccepted,
        // )
        // .await;

        Ok(())
    }
}
