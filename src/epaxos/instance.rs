use std::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Notify, RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard};

use super::types::{
    Ballot, InstanceID, InstanceStatus, LeaderBook, LocalInstanceID, ReplicaID, Seq,
};
use super::util::instance_exist;
use super::Command;

#[derive(Debug, Clone)]
pub struct Instance {
    pub id: InstanceID,
    pub seq: Seq,
    pub ballot: Ballot,
    pub cmds: Vec<Command>,
    pub deps: Vec<Option<LocalInstanceID>>,
    pub status: InstanceStatus,
    pub leaderbook: LeaderBook,
}

impl Instance {
    pub fn local_id(&self) -> LocalInstanceID {
        self.id.local
    }
}

#[derive(Debug, Clone)]
pub struct SharedInstancespace {
    instance: Option<Instance>,
    notify: Option<Vec<Arc<Notify>>>,
}

#[derive(Debug, Clone)]
pub struct SharedInstance {
    space: Arc<RwLock<SharedInstancespace>>,
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
            space: Arc::new(RwLock::new(SharedInstancespace { instance, notify })),
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
    space: DashMap<(ReplicaID, LocalInstanceID), SharedInstance>,
}

impl InstanceSpace {
    pub fn new(_peer_cnt: usize) -> Self {
        Self {
            space: DashMap::new(),
        }
    }

    pub async fn get_instance_or_notify(
        &self,
        replica: &ReplicaID,
        instance_id: &LocalInstanceID,
    ) -> (Option<SharedInstance>, Option<Arc<Notify>>) {
        let key = (replica.clone(), instance_id.clone());
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
        replica: &ReplicaID,
        instance_id: &LocalInstanceID,
    ) -> Option<SharedInstance> {
        let key = (replica.clone(), instance_id.clone());
        self.space.get(&key).map(|instance| instance.clone())
    }

    pub async fn insert_instance(
        &self,
        replica: &ReplicaID,
        instance_id: &LocalInstanceID,
        instance: SharedInstance,
    ) {
        let key = (replica.clone(), instance_id.clone());
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
