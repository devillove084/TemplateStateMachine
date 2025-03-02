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

mod status;
pub use status::*;

mod cache;
pub use cache::*;

mod graph;
pub use graph::*;

mod log;
pub use log::*;

mod peers;
pub use peers::*;

mod replica;
pub use replica::*;

mod batch_command;
pub use batch_command::*;

// #[allow(dead_code)]
// #[cfg(test)]
// mod consensus_unit_test {
//     use std::{
//         collections::{HashMap, HashSet},
//         net::{IpAddr, Ipv4Addr, SocketAddr},
//         sync::{Arc, atomic::AtomicBool},
//         time::Duration,
//     };

//     use asc::Asc;
//     use dashmap::DashMap;
//     use ordered_vecmap::{VecMap, VecSet};
//     use serde::{Deserialize, Serialize};
//     use tokio::sync::{
//         Notify,
//         mpsc::{self},
//     };
//     use tokio::{sync::Mutex as AsyncMutex, time::sleep};
//     use tracing::debug;

//     use super::*;
//     use crate::{DataStore, LogStore, UpdateMode, hook::TestHooks, onemap::OneMap};
//     use anyhow::Result;

//     fn create_test_socket_addr(replica_id: usize) -> SocketAddr {
//         SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9500 + replica_id as u16)
//     }

//     #[derive(Serialize, Deserialize, Clone, Debug, Default)]
//     pub struct TestKeys {
//         keys: HashSet<String>,
//     }

//     impl Keys for TestKeys {
//         type Key = String;

//         fn is_unbounded(&self) -> bool {
//             self.keys.is_empty()
//         }

//         fn for_each(&self, mut f: impl FnMut(&Self::Key)) {
//             for key in &self.keys {
//                 f(key);
//             }
//         }
//     }

//     impl TestKeys {
//         pub fn new<I: IntoIterator<Item = String>>(iter: I) -> Self {
//             Self {
//                 keys: iter.into_iter().collect(),
//             }
//         }

//         pub fn add(&mut self, key: String) {
//             self.keys.insert(key);
//         }

//         pub fn remove(&mut self, key: &String) {
//             self.keys.remove(key);
//         }
//     }

//     #[derive(Serialize, Deserialize, Clone, Debug, Default)]
//     pub struct TestCommand {
//         pub command: String,
//         pub keys: HashSet<String>,
//     }

//     impl TestCommand {
//         pub fn new(command: String, keys: HashSet<String>) -> Self {
//             Self { command, keys }
//         }
//     }

//     impl CommandLike for TestCommand {
//         type Key = String;
//         type Keys = TestKeys;

//         fn keys(&self) -> Self::Keys {
//             TestKeys {
//                 keys: self.keys.clone(),
//             }
//         }

//         fn is_nop(&self) -> bool {
//             self.command == "NOP"
//         }

//         fn create_nop() -> Self {
//             Self {
//                 command: "NOP".to_string(),
//                 keys: HashSet::new(),
//             }
//         }

//         fn create_fence() -> Self {
//             Self {
//                 command: "FENCE".to_string(),
//                 keys: HashSet::new(),
//             }
//         }

//         fn notify_committed(&self) {
//             println!("Command {:?} has been committed.", self);
//         }

//         fn notify_executed(&self) {
//             println!("Command {:?} has been executed.", self);
//         }
//     }

//     async fn setup_test_replica<C, L, D, N>(num: usize) -> Arc<Replica<C, L, D, N>>
//     where
//         C: CommandLike + Default,
//         L: LogStore<C> + Default,
//         D: DataStore<C> + Default,
//         N: MemberNetwork<C> + Default,
//     {
//         let mut peers_map = VecMap::new();
//         for r in 1..=num {
//             peers_map.insert(ReplicaId::from(r as u64), create_test_socket_addr(r));
//         }
//         let replica_meta = ReplicaMeta {
//             replica_id: ReplicaId::ONE,
//             peers: peers_map,
//             ..Default::default()
//         };

//         let log_store = Arc::new(L::default());
//         let data_store = Arc::new(D::default());
//         let member_net = N::default();

//         Replica::new(replica_meta, log_store, data_store, member_net)
//             .await
//             .expect("Failed to initialize replica")
//     }

//     async fn setup_test_replica_with_member_net<C, L, D>(
//         num: usize,
//     ) -> (Arc<Replica<C, L, D, TestMemberNet<C>>>, TestMemberNet<C>)
//     where
//         C: CommandLike + Default + Clone + Send + Sync + 'static,
//         L: LogStore<C> + Default + Send + Sync + 'static,
//         D: DataStore<C> + Default + Send + Sync + 'static,
//     {
//         let replica_meta = ReplicaMeta {
//             replica_id: ReplicaId::ONE,
//             peers: (2..=num as u64)
//                 .map(|i| (ReplicaId::from(i), create_test_socket_addr(i as usize)))
//                 .collect(),
//             ..Default::default()
//         };

//         let log_store = Arc::new(L::default());
//         let data_store = Arc::new(D::default());
//         let member_net = TestMemberNet::<C>::default();

//         let replica = Replica::new(
//             replica_meta,
//             log_store.clone(),
//             data_store.clone(),
//             member_net.clone(),
//         )
//         .await
//         .expect("Failed to initialize replica");

//         let replica_clone = replica.clone();
//         let rx = member_net.register_replica(ReplicaId::ONE).await;
//         tokio::spawn(async move {
//             replica_clone.handle_messages(rx).await;
//         });

//         (replica, member_net)
//     }

//     fn create_test_command() -> TestCommand {
//         let mut keys = HashSet::new();
//         keys.insert("key1".to_string());
//         keys.insert("key2".to_string());
//         TestCommand {
//             command: "Write".to_string(),
//             keys,
//         }
//     }

//     #[derive(Default)]
//     struct TestLogStore<C: CommandLike> {
//         storage: AsyncMutex<HashMap<InstanceId, Instance<C>>>,
//         propose_ballot_store: AsyncMutex<HashMap<InstanceId, Ballot>>,
//         bounds: AsyncMutex<HashMap<String, Vec<u8>>>,
//     }

//     const ATTR_BOUNDS_KEY: &str = "attr_bounds";
//     const STATUS_BOUNDS_KEY: &str = "status_bounds";

//     #[async_trait::async_trait]
//     impl<C: CommandLike> LogStore<C> for TestLogStore<C> {
//         async fn save(
//             self: &Arc<Self>,
//             id: InstanceId,
//             ins: Instance<C>,
//             _mode: UpdateMode,
//         ) -> Result<()> {
//             let mut storage = self.storage.lock().await;
//             storage.insert(id, ins);
//             // Handle UpdateMode if necessary
//             Ok(())
//         }

//         async fn load(self: &Arc<Self>, id: InstanceId) -> Result<Option<Instance<C>>> {
//             let storage = self.storage.lock().await;
//             Ok(storage.get(&id).cloned())
//         }

//         async fn save_propose_ballot(
//             self: &Arc<Self>,
//             id: InstanceId,
//             propose_ballot: Ballot,
//         ) -> Result<()> {
//             let mut propose_ballot_store = self.propose_ballot_store.lock().await;
//             propose_ballot_store.insert(id, propose_ballot);
//             Ok(())
//         }

//         async fn load_propose_ballot(self: &Arc<Self>, id: InstanceId) -> Result<Option<Ballot>> {
//             let propose_ballot_store = self.propose_ballot_store.lock().await;
//             Ok(propose_ballot_store.get(&id).cloned())
//         }

//         async fn save_bounds(
//             self: &Arc<Self>,
//             attr_bounds: AttrBounds,
//             status_bounds: SavedStatusBounds,
//         ) -> Result<()> {
//             let mut bounds_map = self.bounds.lock().await;

//             let attr_bytes = serde_json::to_vec(&attr_bounds)?;
//             let status_bytes = serde_json::to_vec(&status_bounds)?;

//             bounds_map.insert(ATTR_BOUNDS_KEY.to_string(), attr_bytes);
//             bounds_map.insert(STATUS_BOUNDS_KEY.to_string(), status_bytes);

//             Ok(())
//         }

//         async fn load_bounds(self: &Arc<Self>) -> Result<(AttrBounds, StatusBounds)> {
//             let bounds_map = self.bounds.lock().await;

//             let attr_bounds: AttrBounds = if let Some(attr_bytes) = bounds_map.get(ATTR_BOUNDS_KEY)
//             {
//                 serde_json::from_slice(attr_bytes)?
//             } else {
//                 AttrBounds {
//                     max_seq: Seq::ZERO,
//                     max_lids: VecMap::new(),
//                 }
//             };

//             let saved_status_bounds: SavedStatusBounds =
//                 if let Some(status_bytes) = bounds_map.get(STATUS_BOUNDS_KEY) {
//                     serde_json::from_slice(status_bytes)?
//                 } else {
//                     SavedStatusBounds::default()
//                 };

//             let mut status_bounds = {
//                 let mut maps: VecMap<ReplicaId, StatusMap> = VecMap::new();

//                 let create_default = || StatusMap {
//                     known: OneMap::new(0),
//                     committed: OneMap::new(0),
//                     executed: OneMap::new(0),
//                 };

//                 let mut merge =
//                     |map: &VecMap<ReplicaId, LocalInstanceId>,
//                      project: fn(&mut StatusMap) -> &mut OneMap| {
//                         for (replica_id, local_instance_id) in map {
//                             let m = maps.entry(*replica_id).or_insert_with(create_default);
//                             project(m).set_bound(local_instance_id.raw_value());
//                         }
//                     };

//                 merge(&saved_status_bounds.known_up_to, |m| &mut m.known);
//                 merge(&saved_status_bounds.committed_up_to, |m| &mut m.committed);
//                 merge(&saved_status_bounds.executed_up_to, |m| &mut m.executed);

//                 StatusBounds::from_maps(maps)
//             };

//             status_bounds.update_bounds();

//             println!("AttrBounds: {:?}", attr_bounds);
//             // println!("StatusBounds: {:?}", status_bounds);

//             Ok((attr_bounds, status_bounds))
//         }

//         async fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> Result<()> {
//             let mut storage = self.storage.lock().await;
//             if let Some(instance) = storage.get_mut(&id) {
//                 instance.status = status;
//             }
//             Ok(())
//         }
//     }

//     #[derive(Default)]
//     struct TestDataStore<C> {
//         issued_commands: AsyncMutex<HashMap<InstanceId, (C, Asc<ExecNotify>)>>,
//     }

//     #[async_trait::async_trait]
//     impl<C: CommandLike> DataStore<C> for TestDataStore<C> {
//         async fn issue(
//             self: &Arc<Self>,
//             id: InstanceId,
//             cmd: C,
//             notify: Asc<ExecNotify>,
//         ) -> Result<()> {
//             let mut issued = self.issued_commands.lock().await;
//             issued.insert(id, (cmd.clone(), notify));
//             Ok(())
//         }
//     }

//     #[derive(Clone, Default)]
//     struct TestMemberNet<C> {
//         senders: Arc<DashMap<ReplicaId, mpsc::Sender<Message<C>>>>,
//         notifiers: Arc<DashMap<ReplicaId, Arc<Notify>>>,
//     }

//     impl<C> TestMemberNet<C>
//     where
//         C: CommandLike + Send + Sync + 'static,
//     {
//         pub fn new() -> Self {
//             Self {
//                 senders: Arc::new(DashMap::new()),
//                 notifiers: Arc::new(DashMap::new()),
//             }
//         }

//         pub async fn register_replica(&self, replica_id: ReplicaId) -> mpsc::Receiver<Message<C>> {
//             let (tx, rx) = mpsc::channel::<Message<C>>(100);
//             let notify = Arc::new(Notify::new());
//             self.senders.insert(replica_id.clone(), tx);
//             self.notifiers.insert(replica_id.clone(), notify.clone());
//             rx
//         }

//         pub fn send_one(&self, target: ReplicaId, msg: Message<C>) {
//             if let Some(sender) = self.senders.get(&target) {
//                 let sender = sender.clone();
//                 let notifiers = self.notifiers.clone();
//                 let target_clone = target.clone();
//                 let msg_clone = msg.clone();
//                 tokio::spawn(async move {
//                     let _ = sender.send(msg_clone).await;
//                     // 通知 Replica 有新消息
//                     if let Some(notify) = notifiers.get(&target_clone) {
//                         notify.notify_one();
//                     }
//                 });
//             }
//         }

//         pub fn broadcast(&self, targets: VecSet<ReplicaId>, msg: Message<C>) {
//             for target in targets {
//                 self.send_one(target.clone(), msg.clone());
//             }
//         }

//         fn join_replica(&self, _replica_id: ReplicaId, _addr: std::net::SocketAddr) -> Option<ReplicaId> {
//             None
//         }

//         fn leave_replica(&self, replica_id: ReplicaId) {
//             self.senders.remove(&replica_id);
//             self.notifiers.remove(&replica_id);
//         }
//     }

//     #[async_trait::async_trait]
//     impl<C> MemberNetwork<C> for TestMemberNet<C>
//     where
//         C: CommandLike + Send + Sync + 'static,
//     {
//         fn broadcast(&self, targets: VecSet<ReplicaId>, msg: Message<C>) {
//             self.broadcast(targets, msg);
//         }

//         fn send_one(&self, target: ReplicaId, msg: Message<C>) {
//             self.send_one(target, msg);
//         }

//         fn join(&self, replica_id: ReplicaId, addr: std::net::SocketAddr) -> Option<ReplicaId> {
//             self.join_replica(replica_id, addr)
//         }

//         fn leave(&self, replica_id: ReplicaId) {
//             self.leave_replica(replica_id);
//         }
//     }

//     #[tokio::test]
//     async fn test_preaccept_fast_path_success() {
//         let _ = tracing_subscriber::fmt()
//             .with_max_level(tracing::Level::DEBUG)
//             .try_init();
//         let (replicas, _) = setup_test_replica_with_member_net::<
//             TestCommand,
//             TestLogStore<TestCommand>,
//             TestDataStore<TestCommand>,
//         >(5)
//         .await;
//         let cmd = create_test_command();

//         let preaccept_trigger = Arc::new(Notify::new());
//         let test_hooks = TestHooks {
//             skip: Arc::new(AtomicBool::new(true)),
//             phase_preaccept_barrier: preaccept_trigger.clone(),
//         };
//         replicas.test_hooks.lock().await.replace(test_hooks);

//         let propose_handle = tokio::spawn({
//             let replica = replicas.clone();
//             async move { replica.run_propose(cmd).await }
//         });

//         debug!("Start propose");

//         tokio::spawn({
//             let propose_handle = propose_handle;
//             async move {
//                 let _ = propose_handle.await;
//             }
//         });

//         tokio::time::sleep(Duration::from_secs(2)).await;

//         let instance_id = InstanceId(ReplicaId::from(1), LocalInstanceId::from(1));

//         let real_sender = replicas.get_propose_instance_sender(instance_id);

//         let responses = vec![
//             Message::PreAcceptOk(PreAcceptOk {
//                 sender: ReplicaId::from(2),
//                 epoch: Epoch::ZERO,
//                 id: instance_id.clone(),
//                 propose_ballot: Ballot(Round::ZERO, ReplicaId::ONE),
//             }),
//             Message::PreAcceptOk(PreAcceptOk {
//                 sender: ReplicaId::from(3),
//                 epoch: Epoch::ZERO,
//                 id: instance_id.clone(),
//                 propose_ballot: Ballot(Round::ZERO, ReplicaId::ONE),
//             }),
//             Message::PreAcceptOk(PreAcceptOk {
//                 sender: ReplicaId::from(4),
//                 epoch: Epoch::ZERO,
//                 id: instance_id.clone(),
//                 propose_ballot: Ballot(Round::ZERO, ReplicaId::ONE),
//             }),
//         ];

//         for msg in responses {
//             let _ = real_sender.send(msg).await;
//         }

//         debug!("notify one!");
//         preaccept_trigger.notify_one();

//         sleep(Duration::from_secs(1)).await;

//         let final_metrics = replicas.metrics().await;
//         println!(
//             "Final metrics: preaccept_slow_path = {}, preaccept_fast_path = {}",
//             final_metrics.preaccept_slow_path, final_metrics.preaccept_fast_path
//         );

//         assert_eq!(
//             final_metrics.preaccept_fast_path, 1,
//             "Expected to enter slow path once (due to timeout)"
//         );
//     }

//     #[tokio::test]
//     async fn test_preaccept_slow_path_due_to_timeout() {
//         let _ = tracing_subscriber::fmt()
//             .with_max_level(tracing::Level::DEBUG)
//             .try_init();
//         let (replicas, _) = setup_test_replica_with_member_net::<
//             TestCommand,
//             TestLogStore<TestCommand>,
//             TestDataStore<TestCommand>,
//         >(5)
//         .await;
//         let cmd = create_test_command();

//         let preaccept_trigger = Arc::new(Notify::new());
//         let test_hooks = TestHooks {
//             skip: Arc::new(AtomicBool::new(true)),
//             phase_preaccept_barrier: preaccept_trigger.clone(),
//         };
//         replicas.test_hooks.lock().await.replace(test_hooks);

//         let propose_handle = tokio::spawn({
//             let replica = replicas.clone();
//             async move { replica.run_propose(cmd).await }
//         });

//         debug!("Start propose");

//         tokio::spawn({
//             let propose_handle = propose_handle;
//             async move {
//                 let _ = propose_handle.await;
//             }
//         });

//         tokio::time::sleep(Duration::from_secs(2)).await;

//         let instance_id = InstanceId(ReplicaId::from(1), LocalInstanceId::from(1));

//         let real_sender = replicas.get_propose_instance_sender(instance_id);

//         let responses = vec![
//             Message::PreAcceptOk(PreAcceptOk {
//                 sender: ReplicaId::from(2),
//                 epoch: Epoch::ZERO,
//                 id: instance_id.clone(),
//                 propose_ballot: Ballot(Round::ZERO, ReplicaId::ONE),
//             }),
//             Message::PreAcceptDiff(PreAcceptDiff {
//                 sender: ReplicaId::from(5),
//                 epoch: Epoch::ZERO,
//                 id: instance_id.clone(),
//                 propose_ballot: Ballot(Round::ZERO, ReplicaId::ONE),
//                 seq: Seq::from(1),
//                 deps: Deps::default(),
//             }),
//         ];

//         for msg in responses {
//             let _ = real_sender.send(msg).await;
//         }

//         debug!("notify one!");
//         preaccept_trigger.notify_one();

//         sleep(Duration::from_secs(1)).await;

//         let final_metrics = replicas.metrics().await;
//         println!(
//             "Final metrics: preaccept_slow_path = {}, preaccept_fast_path = {}",
//             final_metrics.preaccept_slow_path, final_metrics.preaccept_fast_path
//         );

//         assert_eq!(
//             final_metrics.preaccept_slow_path, 1,
//             "Expected to enter slow path once (due to timeout)"
//         );
//     }
// }

// #[cfg(test)]
// mod replica_test {
//     use std::sync::Arc;

//     use super::{CommandLike, Replica};

//     struct Cluster<C: CommandLike> {
//         replicas: Vec<Arc<Replica<C>>>,
//         network: InProcNetwork<C>,
//     }
// }
