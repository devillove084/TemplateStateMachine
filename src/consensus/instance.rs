use std::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Notify, RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard};

use crate::{Ballot, Command, InstanceId, LocalInstanceId, ReplicaId, Seq};

#[derive(Debug, Clone)]
pub struct Instance {
    pub id: InstanceId,
    pub seq: Seq,
    pub ballot: Ballot,
    pub cmds: Vec<Command>,
    pub deps: Vec<LocalInstanceId>,
    pub status: InstanceStatus,
    pub leaderbook: LeaderBook,
}

impl Instance {
    pub fn local_id(&self) -> LocalInstanceId {
        self.id.local_instance_id.unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct SharedInstanceSpace {
    instance: Option<Instance>,
    notify: Option<Vec<Arc<Notify>>>,
}

#[derive(Debug, Clone)]
pub struct SharedInstance {
    space: Arc<RwLock<SharedInstanceSpace>>,
}

impl PartialEq for SharedInstance {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.space, &other.space)
    }
}

impl Eq for SharedInstance {}

impl Hash for SharedInstance {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.space).hash(state);
    }
}

impl SharedInstance {
    pub fn none() -> Self {
        Self::new(None, None)
    }

    pub fn new(instance: Option<Instance>, notify: Option<Vec<Arc<Notify>>>) -> Self {
        Self {
            space: Arc::new(RwLock::new(SharedInstanceSpace { instance, notify })),
        }
    }

    pub async fn match_status(&self, status: &[InstanceStatus]) -> bool {
        let d_ins_read = self.get_instance_read().await;
        if d_ins_read.is_none() {
            return false;
        }
        let d_ins_read_space = d_ins_read.as_ref().unwrap();
        status.contains(&d_ins_read_space.status)
    }

    pub async fn get_instance_read(&self) -> RwLockReadGuard<Option<Instance>> {
        RwLockReadGuard::map(self.space.read().await, |i| &i.instance)
    }

    pub async fn get_instance_write(&self) -> RwLockMappedWriteGuard<Option<Instance>> {
        RwLockWriteGuard::map(self.space.write().await, |i| &mut i.instance)
    }

    pub fn get_raw_read(
        option_instance: RwLockReadGuard<'_, Option<Instance>>,
    ) -> RwLockReadGuard<'_, Instance> {
        RwLockReadGuard::<Option<Instance>>::map(option_instance, |f| f.as_ref().unwrap())
    }

    pub async fn get_notify_read(&self) -> RwLockReadGuard<Option<Vec<Arc<Notify>>>> {
        RwLockReadGuard::map(self.space.read().await, |i| &i.notify)
    }

    pub async fn clear_notify(&self) {
        let mut space = self.space.write().await;
        space.notify = None;
    }

    pub async fn add_notify(&self, notify: Arc<Notify>) {
        let mut space = self.space.write().await;
        if space.notify.is_none() {
            space.notify = Some(vec![notify]);
        } else if let Some(v) = space.notify.as_mut() {
            v.push(notify);
        }
    }

    pub async fn notify_commit(&self) {
        let notify_vec = self.get_notify_read().await;
        if let Some(vec) = notify_vec.as_ref() {
            vec.iter().for_each(|notify| notify.notify_one());
        }

        drop(notify_vec);
        self.clear_notify().await;
    }
}

pub struct InstanceSpace {
    space: DashMap<(ReplicaId, LocalInstanceId), SharedInstance>,
}

impl InstanceSpace {
    pub fn new(_peer_cnt: usize) -> Self {
        Self {
            space: DashMap::new(),
        }
    }

    pub async fn get_instance_or_notify(
        &self,
        replica: &ReplicaId,
        instance_id: &LocalInstanceId,
    ) -> (Option<SharedInstance>, Option<Arc<Notify>>) {
        let key = (*replica, *instance_id);
        if let Some(instance) = self.space.get(&key) {
            let instance = instance.clone();
            if Self::need_notify(&Some(instance.clone())).await {
                let notify = Arc::new(Notify::new());
                instance.add_notify(notify.clone()).await;
                (Some(instance), Some(notify))
            } else {
                (Some(instance), None)
            }
        } else {
            let notify = Arc::new(Notify::new());
            let instance = SharedInstance::none();
            instance.add_notify(notify.clone()).await;
            self.space.insert(key, instance);
            (None, Some(notify))
        }
    }

    pub async fn get_instance(
        &self,
        replica: &ReplicaId,
        instance_id: &LocalInstanceId,
    ) -> Option<SharedInstance> {
        let key = (*replica, *instance_id);
        self.space.get(&key).map(|instance| instance.clone())
    }

    pub async fn get_all_instance(&self) -> Option<Vec<SharedInstance>> {
        if self.space.is_empty() {
            return None;
        }

        let instances: Vec<SharedInstance> = self
            .space
            .iter()
            .map(|entry| entry.value().clone()) // 获取每个实例的副本
            .collect();

        Some(instances)
    }

    pub async fn insert_instance(
        &self,
        replica: &ReplicaId,
        instance_id: &LocalInstanceId,
        instance: SharedInstance,
    ) {
        let key = (*replica, *instance_id);
        self.space.insert(key, instance);
    }

    pub async fn need_notify(instance: &Option<SharedInstance>) -> bool {
        if !instance_exist(instance).await {
            true
        } else {
            let d_ins = instance.as_ref().unwrap();
            let d_ins_read = d_ins.get_instance_read().await;
            let d_ins_read_space = d_ins_read.as_ref().unwrap();

            !matches! {
                d_ins_read_space.status,
                InstanceStatus::Committed | InstanceStatus::Executed
            }
        }
    }
}

pub async fn instance_exist(instance: &Option<SharedInstance>) -> bool {
    if instance.is_some() {
        let ins = instance.as_ref().unwrap();
        let ins_read = ins.get_instance_read().await;
        ins_read.is_some()
    } else {
        false
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
    pub fn new(replica_id: ReplicaId) -> Self {
        // let mut ballot = Ballot::default();
        // ballot.replica_id = Some(replica_id);
        LeaderBook {
            accept_ok: 0,
            preaccept_ok: 0,
            nack: 0,
            max_ballot: Ballot {
                replica_id: Some(replica_id),
                ..Default::default()
            },
            all_equal: true,
        }
    }
}
