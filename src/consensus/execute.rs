use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use petgraph::{
    algo::tarjan_scc,
    graph::{DiGraph, NodeIndex},
};
use tokio::sync::mpsc;
use tracing::info;

use crate::{Command, ReplicaId, StateMachine};

use super::{
    instance::{InstanceSpace, SharedInstance},
    InstanceStatus,
};

#[derive(Clone)]
pub struct Executor {
    space: Arc<InstanceSpace>,
    state_machine: Arc<dyn StateMachine>,
}

impl Executor {
    pub fn new(space: Arc<InstanceSpace>, state_machine: Arc<dyn StateMachine>) -> Self {
        Self {
            space,
            state_machine,
        }
    }

    pub async fn execute(&self, mut recv: mpsc::Receiver<SharedInstance>) {
        // a infinite loop to poll instance to execute
        loop {
            let instance = recv.recv().await;
            if instance.is_none() {
                // channel has been closed, stop.
                break;
            }
            let self_clone = self.clone();
            let mut inner = InnerExecutor::new(self.space.clone(), instance.unwrap());
            tokio::spawn(async move {
                let scc = inner.build_scc().await;
                if let Some(scc) = scc {
                    inner
                        .build_execute_task(scc, self_clone.state_machine.clone())
                        .await;
                }
            });
        }
    }
}

struct InnerExecutor {
    space: Arc<InstanceSpace>,
    start_instance: SharedInstance,
    map: Option<HashMap<SharedInstance, NodeIndex>>,
    graph: Option<DiGraph<SharedInstance, ()>>,
}

impl InnerExecutor {
    fn new(space: Arc<InstanceSpace>, start_instance: SharedInstance) -> Self {
        Self {
            space,
            start_instance,
            map: None,
            graph: None,
        }
    }

    async fn generate_scc(&self) -> Vec<Vec<NodeIndex>> {
        let g = self.graph.as_ref().unwrap();
        tarjan_scc(g)
    }

    /// Get the graph index for the instance, if the index is missing we
    /// insert the instance into graph and return the index, otherwise
    /// return the index in the map directly.
    #[allow(clippy::mutable_key_type)]
    fn get_or_insert_index(&mut self, instance: &SharedInstance) -> NodeIndex {
        let map = self.map.as_mut().unwrap();
        let g = self.graph.as_mut().unwrap();
        if !map.contains_key(instance) {
            let index = g.add_node(instance.clone());
            map.insert(instance.clone(), index);
            index
        } else {
            *map.get(instance).unwrap()
        }
    }

    /// Tell whether we have visited the instance while building the dep graph
    #[allow(clippy::mutable_key_type)]
    fn has_visited(&self, ins: &SharedInstance) -> bool {
        let map = self.map.as_ref().unwrap();
        map.contains_key(ins)
    }

    fn add_edge(&mut self, src: NodeIndex, dst: NodeIndex) {
        let g = self.graph.as_mut().unwrap();
        g.add_edge(src, dst, ());
    }

    /// Build the scc and generate the result vec from an instance. We'll stop inserting instance to
    /// the graph in the following condition:
    /// - the instance's status is EXECUTED, which means every following step is EXECUTED.
    ///
    /// We'll also wait for one instance in the following conditions:
    /// - the instance's status is NOT COMMITTED and NOT EXECUTED.
    /// - the instance is empty.
    ///
    /// The return value is None if there's no instance to execute.
    /// The return value is Some(Vec<...>), which is the scc vec, if there are instances to execute.
    async fn build_scc(&mut self) -> Option<Vec<Vec<NodeIndex>>> {
        // the start_instance is at least in the stage of COMMITTED
        if self
            .start_instance
            .match_status(&[InstanceStatus::Executed])
            .await
        {
            return None;
        }

        let mut queue = VecDeque::new();
        queue.push_back(self.start_instance.clone());

        // init for map and graph fields
        self.map = Some(HashMap::<SharedInstance, NodeIndex>::new());
        self.graph = Some(DiGraph::<SharedInstance, ()>::new());

        loop {
            let cur = queue.pop_front();

            // if queue is empty
            if cur.is_none() {
                break;
            }
            let cur = cur.unwrap();

            // get node index
            let cur_index = self.get_or_insert_index(&cur);
            let cur_read = cur.get_instance_read().await;
            let cur_read_inner = cur_read.as_ref().unwrap();

            for (r, d) in cur_read_inner.deps.iter().enumerate() {
                if d.value == u64::MAX {
                    continue;
                }

                let r = ReplicaId { value: r as u64 };
                // let d = d.value;

                let (d_ins, notify) = self.space.get_instance_or_notify(&r, d).await;

                let d_ins = if let Some(n) = notify {
                    n.notified().await;
                    self.space.get_instance(&r, d).await
                } else {
                    d_ins
                };

                assert!(
                    d_ins.is_some(),
                    "instance should not be none after notification"
                );

                let d_ins = d_ins.unwrap();

                if d_ins.match_status(&[InstanceStatus::Committed]).await {
                    // there might be cycle
                    if !self.has_visited(&d_ins) {
                        queue.push_back(d_ins.clone());
                    }
                    let d_index = self.get_or_insert_index(&d_ins);
                    self.add_edge(cur_index, d_index);
                }
            }
        }

        Some(self.generate_scc().await)
    }

    async fn build_execute_task(&self, scc: Vec<Vec<NodeIndex>>, stm: Arc<dyn StateMachine>) {
        info!("build execute task");
        let g = self.graph.as_ref().unwrap();
        for each_scc in scc {
            info!("each scc");
            let ins_vec = each_scc.iter().map(|index| &g[*index]);

            let mut sort_vec = Vec::with_capacity(each_scc.len());
            for (id, ins) in ins_vec.enumerate() {
                let ins_read = ins.get_instance_read().await;
                let ins_read_inner = ins_read.as_ref().unwrap();
                sort_vec.push((id, (ins_read_inner.id.replica_id, ins_read_inner.seq)));
            }

            sort_vec.sort_by(|a, b| {
                // Compare seq
                match a.1 .1.partial_cmp(&b.1 .1) {
                    Some(std::cmp::Ordering::Greater) => std::cmp::Ordering::Greater,
                    Some(std::cmp::Ordering::Less) => std::cmp::Ordering::Less,
                    _ => std::cmp::Ordering::Equal,
                };

                // Compare replica id
                match a.1 .0.partial_cmp(&b.1 .0) {
                    Some(std::cmp::Ordering::Greater) => std::cmp::Ordering::Greater,
                    Some(std::cmp::Ordering::Less) => std::cmp::Ordering::Less,
                    _ => std::cmp::Ordering::Equal,
                }
            });

            for (id, _) in sort_vec {
                info!("run in sort vec");
                let ins = &g[each_scc[id]];
                let mut instance_write = ins.get_instance_write().await;
                let instance_write_inner = instance_write.as_mut().unwrap();

                // TODO: It may be executed by other execution tasks
                if instance_write_inner.status == InstanceStatus::Committed {
                    for c in &instance_write_inner.cmds {
                        // FIXME: handle execute error
                        let _ = self.execute(c, &stm).await;
                    }
                    instance_write_inner.status = InstanceStatus::Executed;
                }
            }
        }
    }

    async fn execute(&self, command: &Command, stm: &Arc<dyn StateMachine>) -> crate::Result<()> {
        info!("execute under stm");
        stm.apply(command).await
    }
}

#[cfg(test)]
mod executor_unit_test {
    use super::*;
    use crate::{
        Ballot, Command, CommandType, Instance, InstanceId, LeaderBook, LocalInstanceId, ReplicaId,
        Seq, StateMachine,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;
    struct MockStateMachine {
        executed_commands: Mutex<Vec<Command>>,
    }

    #[async_trait]
    impl StateMachine for MockStateMachine {
        async fn apply(&self, command: &Command) -> crate::Result<()> {
            self.executed_commands.lock().unwrap().push(command.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_executor_executes_single_instance() {
        let space = Arc::new(InstanceSpace::new(1));
        let state_machine = Arc::new(MockStateMachine {
            executed_commands: Mutex::new(Vec::new()),
        });

        let instance_id = 1;
        let replica_id = ReplicaId { value: 0 };
        let command = Command::default();
        let shared_instance = SharedInstance::new(
            Some(Instance {
                id: InstanceId {
                    replica_id: Some(replica_id),
                    local_instance_id: Some(crate::LocalInstanceId { value: instance_id }),
                },
                seq: Seq::default(),
                ballot: Ballot::default(),
                cmds: vec![Command::default()],
                deps: Vec::new(),
                status: InstanceStatus::PreAccepted,
                leaderbook: LeaderBook::default(),
            }),
            None,
        );

        {
            let mut instance_write = shared_instance.get_instance_write().await;
            instance_write.as_mut().unwrap().status = InstanceStatus::Committed;
        }

        space
            .insert_instance(
                &replica_id.clone(),
                &crate::LocalInstanceId { value: instance_id },
                shared_instance.clone(),
            )
            .await;

        let (tx, rx) = mpsc::channel(10);
        let executor = Executor::new(space.clone(), state_machine.clone());
        tokio::spawn(async move {
            executor.execute(rx).await;
        });

        tx.send(shared_instance.clone()).await.unwrap();
        drop(tx);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let executed_commands = state_machine.executed_commands.lock().unwrap();
        assert_eq!(executed_commands.len(), 1);
        assert_eq!(executed_commands[0], command);
    }

    #[tokio::test]
    async fn test_executor_executes_instances_with_dependencies() {
        let space = Arc::new(InstanceSpace::new(1));
        let state_machine = Arc::new(MockStateMachine {
            executed_commands: Mutex::new(Vec::new()),
        });

        let command1 = Command {
            command_type: CommandType::Add.into(),
            key: Bytes::from_static(b"key1").to_vec(),
            value: Some(Bytes::from_static(b"value1").to_vec()),
        };
        let command2 = Command {
            command_type: CommandType::Delete.into(),
            key: Bytes::from_static(b"key1").to_vec(),
            value: Some(Bytes::from_static(b"value1").to_vec()),
        };

        // 创建实例1
        let instance_id1 = 1;
        let replica_id1 = ReplicaId { value: 0 };
        let shared_instance1 = SharedInstance::new(
            Some(Instance {
                id: InstanceId {
                    replica_id: Some(replica_id1),
                    local_instance_id: Some(crate::LocalInstanceId {
                        value: instance_id1,
                    }),
                },
                seq: Seq::default(),
                ballot: Ballot::default(),
                cmds: vec![command1.clone()],
                deps: Vec::new(),
                status: InstanceStatus::PreAccepted,
                leaderbook: LeaderBook::default(),
            }),
            None,
        );
        {
            let mut instance_write = shared_instance1.get_instance_write().await;
            instance_write.as_mut().unwrap().status = InstanceStatus::Committed;
            instance_write.as_mut().unwrap().deps = vec![LocalInstanceId { value: u64::MAX }; 2];
        }
        space
            .insert_instance(
                &replica_id1.clone(),
                &LocalInstanceId {
                    value: instance_id1,
                },
                shared_instance1.clone(),
            )
            .await;

        let instance_id2 = 2;
        let replica_id2 = ReplicaId { value: 1 };
        let shared_instance2 = SharedInstance::new(
            Some(Instance {
                id: InstanceId {
                    replica_id: Some(replica_id2),
                    local_instance_id: Some(crate::LocalInstanceId {
                        value: instance_id2,
                    }),
                },
                seq: Seq::default(),
                ballot: Ballot::default(),
                cmds: vec![command2.clone()],
                deps: vec![LocalInstanceId {
                    value: instance_id1,
                }],
                status: InstanceStatus::PreAccepted,
                leaderbook: LeaderBook::default(),
            }),
            None,
        );
        {
            let mut instance_write = shared_instance2.get_instance_write().await;
            instance_write.as_mut().unwrap().status = InstanceStatus::Committed;
            let mut deps = vec![LocalInstanceId { value: u64::MAX }; 2];
            deps[replica_id1.value as usize] = LocalInstanceId {
                value: instance_id1,
            };
            instance_write.as_mut().unwrap().deps = deps;
        }
        space
            .insert_instance(
                &replica_id2.clone(),
                &LocalInstanceId {
                    value: instance_id2,
                },
                shared_instance2.clone(),
            )
            .await;

        let (tx, rx) = mpsc::channel(10);
        let executor = Executor::new(space.clone(), state_machine.clone());
        tokio::spawn(async move {
            executor.execute(rx).await;
        });

        tx.send(shared_instance2.clone()).await.unwrap();
        drop(tx);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let executed_commands = state_machine.executed_commands.lock().unwrap();
        assert_eq!(executed_commands.len(), 2);
        assert_eq!(executed_commands[0], command1);
        assert_eq!(executed_commands[1], command2);
    }

    // #[tokio::test]
    // async fn test_executor_handles_cyclic_dependencies() {
    //     // 创建实例空间和状态机
    //     let space = Arc::new(InstanceSpace::new());
    //     let state_machine = Arc::new(MockStateMachine {
    //         executed_commands: Mutex::new(Vec::new()),
    //     });

    //     // 创建命令
    //     let command1 = Command::new("command1");
    //     let command2 = Command::new("command2");

    //     // 创建实例1，依赖于实例2
    //     let instance_id1 = 1;
    //     let replica_id1 = ReplicaId { value: 0 };
    //     let shared_instance1 =
    //         SharedInstance::new(replica_id1.clone(), instance_id1, vec![command1.clone()]);

    //     // 创建实例2，依赖于实例1
    //     let instance_id2 = 2;
    //     let replica_id2 = ReplicaId { value: 1 };
    //     let shared_instance2 =
    //         SharedInstance::new(replica_id2.clone(), instance_id2, vec![command2.clone()]);

    //     // 设置实例1的依赖
    //     {
    //         let mut instance_write = shared_instance1.get_instance_write().await;
    //         instance_write.as_mut().unwrap().status = InstanceStatus::Committed;
    //         let mut deps = vec![InstanceId::new(u64::MAX); NUM_REPLICAS];
    //         deps[replica_id2.value as usize] = InstanceId::new(instance_id2);
    //         instance_write.as_mut().unwrap().deps = deps;
    //     }
    //     space
    //         .insert_instance(replica_id1.clone(), instance_id1, shared_instance1.clone())
    //         .await;

    //     // 设置实例2的依赖
    //     {
    //         let mut instance_write = shared_instance2.get_instance_write().await;
    //         instance_write.as_mut().unwrap().status = InstanceStatus::Committed;
    //         let mut deps = vec![InstanceId::new(u64::MAX); NUM_REPLICAS];
    //         deps[replica_id1.value as usize] = InstanceId::new(instance_id1);
    //         instance_write.as_mut().unwrap().deps = deps;
    //     }
    //     space
    //         .insert_instance(replica_id2.clone(), instance_id2, shared_instance2.clone())
    //         .await;

    //     // 创建 mpsc 通道并启动执行器
    //     let (tx, rx) = mpsc::channel(10);
    //     let executor = Executor::new(space.clone(), state_machine.clone());
    //     tokio::spawn(async move {
    //         executor.execute(rx).await;
    //     });

    //     // 发送实例1到执行器
    //     tx.send(shared_instance1.clone()).await.unwrap();
    //     drop(tx); // 关闭发送端

    //     // 等待执行完成
    //     tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    //     // 验证命令已被执行，且处理了循环依赖
    //     let executed_commands = state_machine.executed_commands.lock().unwrap();
    //     assert_eq!(executed_commands.len(), 2);
    //     // 因为循环依赖，需要按照顺序排序
    //     // 可以根据具体的实现决定执行顺序，这里假设按实例ID排序
    //     assert_eq!(executed_commands[0], command1);
    //     assert_eq!(executed_commands[1], command2);
    // }
}
