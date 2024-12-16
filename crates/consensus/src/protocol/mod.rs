#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::arithmetic_side_effects,
    clippy::must_use_candidate
)]
#![warn(clippy::todo, clippy::dbg_macro)]

mod acc;
pub use acc::*;

mod id;
pub use id::*;

mod addr_map;
pub use addr_map::*;

mod bounds;
pub use bounds::*;

mod cmd;
pub use cmd::*;

mod deps;
pub use deps::*;

mod exec;
pub use exec::*;

mod instance;
pub use instance::*;

mod message;
pub use message::*;

mod membership;
pub use membership::*;

mod status;
pub use status::*;

mod cache;
pub use cache::*;

mod config;
pub use config::*;

mod graph;
pub use graph::*;

mod log;
pub use log::*;

mod peers;
pub use peers::*;

mod replica;
pub use replica::*;

#[allow(dead_code)]
#[cfg(test)]
mod consensus_unit_test {
    use std::{
        collections::{HashMap, HashSet},
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, Mutex},
    };

    use asc::Asc;
    use ordered_vecmap::{VecMap, VecSet};
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{DataStore, LogStore, UpdateMode, onemap::OneMap};
    use anyhow::Result;

    fn create_test_socket_addr(replica_id: usize) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9500 + replica_id as u16)
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct TestKeys {
        keys: HashSet<String>,
    }

    impl Keys for TestKeys {
        type Key = String;

        fn is_unbounded(&self) -> bool {
            self.keys.is_empty()
        }

        fn for_each(&self, mut f: impl FnMut(&Self::Key)) {
            for key in &self.keys {
                f(key);
            }
        }
    }

    impl TestKeys {
        pub fn new<I: IntoIterator<Item = String>>(iter: I) -> Self {
            Self {
                keys: iter.into_iter().collect(),
            }
        }

        pub fn add(&mut self, key: String) {
            self.keys.insert(key);
        }

        pub fn remove(&mut self, key: &String) {
            self.keys.remove(key);
        }
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct TestCommand {
        pub command: String,
        pub keys: HashSet<String>,
    }

    impl TestCommand {
        pub fn new(command: String, keys: HashSet<String>) -> Self {
            Self { command, keys }
        }
    }

    impl CommandLike for TestCommand {
        type Key = String;
        type Keys = TestKeys;

        fn keys(&self) -> Self::Keys {
            TestKeys {
                keys: self.keys.clone(),
            }
        }

        fn is_nop(&self) -> bool {
            self.command == "NOP"
        }

        fn create_nop() -> Self {
            Self {
                command: "NOP".to_string(),
                keys: HashSet::new(),
            }
        }

        fn create_fence() -> Self {
            Self {
                command: "FENCE".to_string(),
                keys: HashSet::new(),
            }
        }

        fn notify_committed(&self) {
            println!("Command {:?} has been committed.", self);
        }

        fn notify_executed(&self) {
            println!("Command {:?} has been executed.", self);
        }
    }

    async fn setup_test_replica<C, L, D, N>(num: usize) -> Arc<Replica<C, L, D, N>>
    where
        C: CommandLike + Default,
        L: LogStore<C> + Default,
        D: DataStore<C> + Default,
        N: MembershipChange<C> + Default,
    {
        let mut peers_map = VecMap::new();
        for r in 1..=num {
            peers_map.insert(ReplicaId::from(r as u64), create_test_socket_addr(r));
        }
        let replica_meta = ReplicaMeta {
            rid: ReplicaId::ZERO,
            peers: peers_map,
            ..Default::default()
        };

        let log_store = Arc::new(L::default());
        let data_store = Arc::new(D::default());
        let member_net = N::default();

        Replica::new(replica_meta, log_store, data_store, member_net)
            .await
            .expect("Failed to initialize replica")
    }

    fn create_test_command() -> TestCommand {
        let mut keys = HashSet::new();
        keys.insert("key1".to_string());
        keys.insert("key2".to_string());
        TestCommand {
            command: "Write".to_string(),
            keys,
        }
    }

    #[derive(Default)]
    struct TestLogStore<C: CommandLike> {
        storage: Mutex<HashMap<InstanceId, Instance<C>>>,
        pbal_store: Mutex<HashMap<InstanceId, Ballot>>,
        bounds: Mutex<HashMap<String, Vec<u8>>>,
    }

    const ATTR_BOUNDS_KEY: &str = "attr_bounds";
    const STATUS_BOUNDS_KEY: &str = "status_bounds";

    #[async_trait::async_trait]
    impl<C: CommandLike> LogStore<C> for TestLogStore<C> {
        async fn save(
            self: &Arc<Self>,
            id: InstanceId,
            ins: Instance<C>,
            _mode: UpdateMode,
        ) -> Result<()> {
            let mut storage = self.storage.lock().unwrap();
            storage.insert(id, ins);
            // Handle UpdateMode if necessary
            Ok(())
        }

        async fn load(self: &Arc<Self>, id: InstanceId) -> Result<Option<Instance<C>>> {
            let storage = self.storage.lock().unwrap();
            Ok(storage.get(&id).cloned())
        }

        async fn save_pbal(self: &Arc<Self>, id: InstanceId, pbal: Ballot) -> Result<()> {
            let mut pbal_store = self.pbal_store.lock().unwrap();
            pbal_store.insert(id, pbal);
            Ok(())
        }

        async fn load_pbal(self: &Arc<Self>, id: InstanceId) -> Result<Option<Ballot>> {
            let pbal_store = self.pbal_store.lock().unwrap();
            Ok(pbal_store.get(&id).cloned())
        }

        async fn save_bounds(
            self: &Arc<Self>,
            attr_bounds: AttrBounds,
            status_bounds: SavedStatusBounds,
        ) -> Result<()> {
            let mut bounds_map = self.bounds.lock().unwrap();

            let attr_bytes = serde_json::to_vec(&attr_bounds)?;
            let status_bytes = serde_json::to_vec(&status_bounds)?;

            bounds_map.insert(ATTR_BOUNDS_KEY.to_string(), attr_bytes);
            bounds_map.insert(STATUS_BOUNDS_KEY.to_string(), status_bytes);

            Ok(())
        }

        async fn load_bounds(self: &Arc<Self>) -> Result<(AttrBounds, StatusBounds)> {
            let bounds_map = self.bounds.lock().unwrap();

            let attr_bounds: AttrBounds = if let Some(attr_bytes) = bounds_map.get(ATTR_BOUNDS_KEY)
            {
                serde_json::from_slice(attr_bytes)?
            } else {
                AttrBounds {
                    max_seq: Seq::ZERO,
                    max_lids: VecMap::new(),
                }
            };

            let saved_status_bounds: SavedStatusBounds =
                if let Some(status_bytes) = bounds_map.get(STATUS_BOUNDS_KEY) {
                    serde_json::from_slice(status_bytes)?
                } else {
                    SavedStatusBounds::default()
                };

            let mut status_bounds = {
                let mut maps: VecMap<ReplicaId, StatusMap> = VecMap::new();

                let create_default = || StatusMap {
                    known: OneMap::new(0),
                    committed: OneMap::new(0),
                    executed: OneMap::new(0),
                };

                let mut merge =
                    |map: &VecMap<ReplicaId, LocalInstanceId>,
                     project: fn(&mut StatusMap) -> &mut OneMap| {
                        for (rid, lid) in map {
                            let m = maps.entry(*rid).or_insert_with(create_default);
                            project(m).set_bound(lid.raw_value());
                        }
                    };

                merge(&saved_status_bounds.known_up_to, |m| &mut m.known);
                merge(&saved_status_bounds.committed_up_to, |m| &mut m.committed);
                merge(&saved_status_bounds.executed_up_to, |m| &mut m.executed);

                StatusBounds::from_maps(maps)
            };

            status_bounds.update_bounds();

            println!("AttrBounds: {:?}", attr_bounds);
            // println!("StatusBounds: {:?}", status_bounds);

            Ok((attr_bounds, status_bounds))
        }

        async fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> Result<()> {
            let mut storage = self.storage.lock().unwrap();
            if let Some(instance) = storage.get_mut(&id) {
                instance.status = status;
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestDataStore<C> {
        issued_commands: Mutex<HashMap<InstanceId, (C, Asc<ExecNotify>)>>,
    }

    #[async_trait::async_trait]
    impl<C: CommandLike> DataStore<C> for TestDataStore<C> {
        async fn issue(
            self: &Arc<Self>,
            id: InstanceId,
            cmd: C,
            notify: Asc<ExecNotify>,
        ) -> Result<()> {
            let mut issued = self.issued_commands.lock().unwrap();
            issued.insert(id, (cmd.clone(), notify));
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestMemberNet<C: CommandLike> {
        sent_messages: Mutex<Vec<(ReplicaId, Message<C>)>>,
    }

    impl<C: CommandLike> MembershipChange<C> for TestMemberNet<C> {
        fn broadcast(&self, targets: VecSet<ReplicaId>, msg: Message<C>) {
            let mut sent = self.sent_messages.lock().unwrap();
            for target in targets {
                sent.push((target, msg.clone()));
            }
        }

        fn send_one(&self, target: ReplicaId, msg: Message<C>) {
            let mut sent = self.sent_messages.lock().unwrap();
            sent.push((target, msg));
        }

        fn join(&self, rid: ReplicaId, _addr: SocketAddr) -> Option<ReplicaId> {
            // For testing, simply return the joined ReplicaId
            Some(rid)
        }

        fn leave(&self, _rid: ReplicaId) {
            // For testing, do nothing
        }
    }

    #[tokio::test]
    async fn test_preaccept_fast_path_success() {
        let replica = setup_test_replica::<
            TestCommand,
            TestLogStore<TestCommand>,
            TestDataStore<TestCommand>,
            TestMemberNet<TestCommand>,
        >(5)
        .await;
        let cmds = create_test_command();
        let result = replica.run_propose(cmds).await;
        assert!(result.is_ok());
    }
}
