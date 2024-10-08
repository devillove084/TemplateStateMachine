use std::{collections::HashMap, sync::Arc, u64};

use tokio::sync::mpsc;
use tracing::info;

use crate::{Command, InstanceId, LocalInstanceId, ReplicaId, Seq, StateMachine};

use super::{
    execute::Executor,
    instance::{Instance, SharedInstance},
    InstanceSpace,
};

type Conflict = Vec<HashMap<Command, SharedInstance>>;

#[derive(Clone)]
pub struct Replica {
    pub id: ReplicaId,
    pub peer_cnt: usize,
    pub instance_space: Arc<InstanceSpace>,
    pub cur_max_instances: Vec<LocalInstanceId>,
    pub committed_upto: Vec<LocalInstanceId>,
    pub conflicts: Conflict,
    pub max_seq: Seq,
    pub exec_send: mpsc::Sender<SharedInstance>,
}

impl Replica {
    pub fn new(id: usize, peer_cnt: usize, stm: Arc<dyn StateMachine>) -> Self {
        let instance_space = Arc::new(InstanceSpace::new(peer_cnt));
        let mut cur_max_instances = Vec::with_capacity(peer_cnt);
        let mut committed_upto = Vec::with_capacity(peer_cnt);
        let mut conflicts = Vec::with_capacity(peer_cnt);

        for _ in 0..peer_cnt {
            cur_max_instances.push(LocalInstanceId::default());
            committed_upto.push(LocalInstanceId::default());
            conflicts.push(HashMap::new());
        }

        let (sender, receiver) = mpsc::channel(1024);
        let space_clone = instance_space.clone();
        tokio::spawn(async move {
            let executor = Executor::new(space_clone, stm.clone());
            executor.execute(receiver).await;
        });

        Self {
            id: ReplicaId { value: id as u64 },
            peer_cnt,
            instance_space,
            cur_max_instances,
            committed_upto,
            conflicts,
            max_seq: Seq { value: 0 },
            exec_send: sender,
        }
    }

    pub fn cur_instance(&self, r: &ReplicaId) -> LocalInstanceId {
        let replica_id = r.value as usize;
        self.cur_max_instances[replica_id]
    }

    pub fn set_cur_instance(&mut self, instance: &InstanceId) {
        let instance_replica_id = instance.replica_id.unwrap().value;
        self.cur_max_instances[instance_replica_id as usize] = instance.local_instance_id.unwrap();
    }

    pub fn local_cur_instance(&self) -> &LocalInstanceId {
        let replica_id = self.id.value as usize;
        self.cur_max_instances.get(replica_id).unwrap()
    }

    pub fn inc_local_cur_instance(&mut self) -> &LocalInstanceId {
        let replica_id = self.id.value as usize;
        let instance_id = self.cur_max_instances.get_mut(replica_id).unwrap();
        instance_id.value += 1;
        instance_id
    }

    pub fn merge_seq_deps(
        &self,
        instance: &mut Instance,
        new_seq: &Seq,
        new_deps: &[LocalInstanceId],
    ) -> bool {
        let mut equal = true;

        if &instance.seq != new_seq {
            equal = false;
            if new_seq > &instance.seq {
                instance.seq = *new_seq;
            }
        }

        instance
            .deps
            .iter_mut()
            .zip(new_deps.iter())
            .for_each(|(o, n)| {
                if o != n {
                    equal = false;
                    if o.value == 0 || (n.value != 0 && o.value < n.value) {
                        *o = *n;
                    }
                }
            });
        equal
    }

    #[allow(clippy::needless_range_loop)]
    pub async fn get_seq_deps(&self, cmds: &[Command]) -> (Seq, Vec<LocalInstanceId>) {
        info!("get_seq_deps: Starting to compute seq and deps...");
        let mut new_seq = Seq { value: 0 };
        let mut deps = vec![LocalInstanceId { value: u64::MAX }; self.peer_cnt];
        for r_id in 0..self.peer_cnt {
            info!("get_seq_deps: Iterating over peer {}", r_id);
            if r_id != self.id.value as usize {
                for command in cmds.iter() {
                    info!("get_seq_deps: Processing command for peer {}", r_id);
                    if let Some(instance) = self.conflicts[r_id].get(command) {
                        let conflict_instance = instance.get_instance_read().await;
                        if conflict_instance.is_none() {
                            info!("get_seq_deps: No conflict instance found for peer {}", r_id);
                            continue;
                        }
                        let conflict_instance = conflict_instance.as_ref().unwrap();
                        if deps[r_id].value == u64::MAX
                            || conflict_instance.local_id().value > deps[r_id].value
                        {
                            deps[r_id] = conflict_instance.local_id();
                        }
                        let s = &conflict_instance.seq;
                        if s.value >= new_seq.value {
                            new_seq.value = s.value + 1;
                        }
                    }
                }
            }
        }
        info!("get_seq_deps: Completed computing seq and deps");
        (new_seq, deps)
    }

    // TODO: merge with get_seq_deps
    #[allow(clippy::needless_range_loop)]
    pub async fn update_seq_deps(
        &self,
        mut seq: Seq,
        mut deps: Vec<LocalInstanceId>,
        cmds: &[Command],
    ) -> (Seq, Vec<LocalInstanceId>, bool) {
        let mut changed = false;
        for r_id in 0..self.peer_cnt {
            if r_id != self.id.value as usize {
                for command in cmds.iter() {
                    if let Some(instance) = self.conflicts[r_id].get(command) {
                        let conflict_instance = instance.get_instance_read().await;
                        if conflict_instance.is_none() {
                            continue;
                        }
                        let conflict_instance = conflict_instance.as_ref().unwrap();
                        if deps[r_id].value != 0
                            && deps[r_id].value < conflict_instance.local_id().value
                        {
                            changed = true;

                            deps[r_id] = conflict_instance.local_id();

                            let conflict_seq = &conflict_instance.seq;
                            if conflict_seq.value >= seq.value {
                                seq.value = conflict_seq.value + 1;
                            }
                        }
                    }
                }
            }
        }
        (seq, deps, changed)
    }

    pub async fn update_conflicts(
        &mut self,
        replica: &ReplicaId,
        cmds: &[Command],
        new_inst: SharedInstance,
    ) {
        let replica_idx = replica.value as usize;
        for c in cmds {
            let update_inst = match self.conflicts[replica_idx].get(c) {
                None => Some(new_inst.clone()),
                Some(ins) => {
                    let ins_read = ins.get_instance_read().await;
                    let new_inst_read = new_inst.get_instance_read().await;
                    if ins_read.is_some()
                        && ins_read
                            .as_ref()
                            .unwrap()
                            .id
                            .local_instance_id
                            .as_ref()
                            .unwrap()
                            .value
                            >= new_inst_read
                                .as_ref()
                                .unwrap()
                                .id
                                .local_instance_id
                                .as_ref()
                                .unwrap()
                                .value
                    {
                        None
                    } else {
                        Some(new_inst.clone())
                    }
                }
            };

            if let Some(ninst) = update_inst {
                self.conflicts[replica_idx].insert(c.clone(), ninst);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandType, Instance, LocalInstanceId, Seq};

    struct MockStateMachine {}

    impl MockStateMachine {
        pub fn new() -> Self {
            Self {}
        }
    }

    #[async_trait::async_trait]
    impl StateMachine for MockStateMachine {
        async fn apply(&self, _command: &Command) -> crate::Result<()> {
            todo!()
        }
    }

    #[test]
    fn test_merge_seq_deps() {
        let mut instance = Instance {
            id: InstanceId {
                replica_id: Some(ReplicaId { value: 1 }),
                local_instance_id: Some(LocalInstanceId { value: 1 }),
            },
            seq: Seq { value: 1 },
            ballot: Default::default(),
            cmds: vec![],
            deps: vec![LocalInstanceId { value: 1 }, LocalInstanceId { value: 2 }],
            status: crate::InstanceStatus::Accepted,
            leaderbook: Default::default(),
        };

        let new_seq = Seq { value: 2 };
        let new_deps = vec![LocalInstanceId { value: 2 }, LocalInstanceId { value: 2 }];

        let replica = Replica::new(0, 2, Arc::new(MockStateMachine::new()));

        let equal = replica.merge_seq_deps(&mut instance, &new_seq, &new_deps);

        assert!(!equal);
        assert_eq!(instance.seq.value, 2);
        assert_eq!(instance.deps, new_deps);
    }

    #[tokio::test]
    async fn test_get_seq_deps() {
        let mut replica = Replica::new(0, 2, Arc::new(MockStateMachine::new()));
        let command = Command {
            command_type: CommandType::Add.into(),
            key: Vec::new(),
            value: Some(Vec::new()),
        };
        let cmds = vec![command.clone()];

        // 模拟冲突
        let instance = SharedInstance::new(
            Some(Instance {
                id: InstanceId {
                    replica_id: Some(ReplicaId { value: 1 }),
                    local_instance_id: Some(LocalInstanceId { value: 1 }),
                },
                seq: Seq { value: 3 },
                ballot: Default::default(),
                cmds: vec![command.clone()],
                deps: vec![],
                status: crate::InstanceStatus::Committed,
                leaderbook: Default::default(),
            }),
            None,
        );

        replica.conflicts[1].insert(command.clone(), instance);

        let (seq, deps) = replica.get_seq_deps(&cmds).await;

        assert_eq!(seq.value, 4);
        assert_eq!(deps[1].value, 1);
    }

    // #[tokio::test]
    // async fn test_update_seq_deps() {
    //     let mut replica = Replica::new(0, 2);
    //     let command = Command { /* 初始化命令 */ };
    //     let cmds = vec![command.clone()];
    //     let seq = Seq { value: 2 };
    //     let deps = vec![LocalInstanceId { value: 1 }, LocalInstanceId { value: 1 }];

    //     // 模拟冲突
    //     let instance = SharedInstance::new(
    //         Some(Instance {
    //             id: InstanceId {
    //                 replica_id: Some(ReplicaId { value: 1 }),
    //                 local_instance_id: Some(LocalInstanceId { value: 2 }),
    //             },
    //             seq: Seq { value: 3 },
    //             ballot: Default::default(),
    //             cmds: vec![command.clone()],
    //             deps: vec![],
    //             status: Default::default(),
    //             leaderbook: Default::default(),
    //         }),
    //         None,
    //     );

    //     replica.conflicts[1].insert(command.clone(), instance);

    //     let (new_seq, new_deps, changed) = replica.update_seq_deps(seq, deps, &cmds).await;

    //     assert_eq!(changed, true);
    //     assert_eq!(new_seq.value, 4);
    //     assert_eq!(new_deps[1].value, 2);
    // }

    // #[tokio::test]
    // async fn test_update_conflicts() {
    //     let mut replica = Replica::new(0, 2);
    //     let command = Command { /* 初始化命令 */ };
    //     let cmds = vec![command.clone()];

    //     // 新的实例
    //     let new_instance = SharedInstance::new(
    //         Some(Instance {
    //             id: InstanceId {
    //                 replica_id: Some(ReplicaId { value: 1 }),
    //                 local_instance_id: Some(LocalInstanceId { value: 2 }),
    //             },
    //             seq: Seq { value: 3 },
    //             ballot: Default::default(),
    //             cmds: vec![command.clone()],
    //             deps: vec![],
    //             status: Default::default(),
    //             leaderbook: Default::default(),
    //         }),
    //         None,
    //     );

    //     // 旧的实例
    //     let old_instance = SharedInstance::new(
    //         Some(Instance {
    //             id: InstanceId {
    //                 replica_id: Some(ReplicaId { value: 1 }),
    //                 local_instance_id: Some(LocalInstanceId { value: 1 }),
    //             },
    //             seq: Seq { value: 2 },
    //             ballot: Default::default(),
    //             cmds: vec![command.clone()],
    //             deps: vec![],
    //             status: Default::default(),
    //             leaderbook: Default::default(),
    //         }),
    //         None,
    //     );

    //     replica.conflicts[1].insert(command.clone(), old_instance);

    //     // 更新冲突
    //     replica
    //         .update_conflicts(&ReplicaId { value: 1 }, &cmds, new_instance.clone())
    //         .await;

    //     let stored_instance = replica.conflicts[1].get(&command).unwrap();

    //     let stored_instance_read = stored_instance.get_instance_read().await;
    //     let stored_instance_read = stored_instance_read.as_ref().unwrap();

    //     assert_eq!(
    //         stored_instance_read
    //             .id
    //             .local_instance_id
    //             .as_ref()
    //             .unwrap()
    //             .value,
    //         2
    //     );
    // }
}
