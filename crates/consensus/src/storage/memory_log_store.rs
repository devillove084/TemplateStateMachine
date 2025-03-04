use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use ordered_vecmap::VecMap;

use crate::{
    AttrBounds, Ballot, CommandLike, Instance, InstanceId, SavedStatusBounds, Seq, Status,
    StatusBounds,
};

use super::{LogStore, UpdateMode};

use anyhow::Result;

#[derive(Debug, Default)]
struct InstanceData<C> {
    instance: Instance<C>,
    ballot: Option<Ballot>,
}

pub struct MemoryLogStore<C: CommandLike> {
    instances: RwLock<HashMap<InstanceId, InstanceData<C>>>,

    bounds: Mutex<(AttrBounds, SavedStatusBounds)>,
}

impl<C: CommandLike> MemoryLogStore<C> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            instances: RwLock::new(HashMap::new()),
            bounds: Mutex::new((
                AttrBounds {
                    max_seq: Seq::ZERO,
                    max_local_instance_ids: VecMap::new(),
                },
                SavedStatusBounds::default(),
            )),
        })
    }
}

#[async_trait::async_trait]
impl<C: CommandLike> LogStore<C> for MemoryLogStore<C> {
    async fn save(&self, id: InstanceId, ins: Instance<C>, mode: UpdateMode) -> Result<()> {
        let mut instances = self.instances.write().unwrap();

        let data = instances.entry(id).or_insert_with(|| InstanceData {
            instance: Instance::default(),
            ballot: None,
        });

        data.instance.status = ins.status;
        data.instance.seq = ins.seq;
        data.instance.deps = ins.deps;
        data.instance.accepted_ballot = ins.accepted_ballot;
        data.instance.acc = ins.acc;

        if matches!(mode, UpdateMode::Full) {
            data.instance.cmd = ins.cmd;
        }

        // ! should wrap
        let mut bounds = self.bounds.lock().unwrap();
        bounds.0.max_seq = bounds.0.max_seq.max(ins.seq);
        bounds
            .0
            .max_local_instance_ids
            .entry(id.0)
            .and_modify(|e| *e = (*e).max(id.1))
            .or_insert(id.1);
        Ok(())
    }

    async fn load(&self, id: InstanceId) -> Result<Option<Instance<C>>> {
        let instances = self.instances.read().unwrap();
        Ok(instances.get(&id).map(|d| d.instance.clone()))
    }

    async fn save_propose_ballot(&self, id: InstanceId, propose_ballot: Ballot) -> Result<()> {
        let mut instances = self.instances.write().unwrap();
        let data = instances.entry(id).or_default();
        data.ballot = Some(propose_ballot);
        Ok(())
    }

    async fn load_propose_ballot(&self, id: InstanceId) -> Result<Option<Ballot>> {
        let instances = self.instances.read().unwrap();
        Ok(instances.get(&id).and_then(|d| d.ballot))
    }

    async fn save_bounds(
        &self,
        attr_bounds: AttrBounds,
        status_bounds: SavedStatusBounds,
    ) -> Result<()> {
        let mut bounds = self.bounds.lock().unwrap();
        *bounds = (attr_bounds, status_bounds);
        Ok(())
    }

    async fn load_bounds(&self) -> Result<(AttrBounds, StatusBounds)> {
        let bounds = self.bounds.lock().unwrap();
        let status_bounds = StatusBounds::from_saved(&bounds.1);
        Ok((bounds.0.clone(), status_bounds))
    }

    async fn update_status(&self, id: InstanceId, status: Status) -> Result<()> {
        let mut instances = self.instances.write().unwrap();
        if let Some(data) = instances.get_mut(&id) {
            data.instance.status = status;
        }
        Ok(())
    }
}
