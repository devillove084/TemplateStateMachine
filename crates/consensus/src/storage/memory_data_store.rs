use std::{
    collections::VecDeque,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use asc::Asc;
use dashmap::DashMap;
use tracing::debug;

use crate::{CommandLike, ExecNotify, InstanceId};

use super::DataStore;

pub struct MemoryDataStore<C: CommandLike> {
    kv_store: DashMap<InstanceId, Vec<u8>>,

    exec_order: Mutex<VecDeque<InstanceId>>,

    _p: PhantomData<C>,
}

impl<C> MemoryDataStore<C>
where
    C: CommandLike,
{
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            kv_store: DashMap::new(),
            exec_order: Mutex::new(VecDeque::new()),
            _p: PhantomData,
        })
    }

    pub fn get_execution_order(&self) -> Vec<InstanceId> {
        (*self.exec_order.lock().unwrap())
            .iter()
            .map(|ins| *ins)
            .collect::<Vec<InstanceId>>()
    }
}

#[async_trait::async_trait]
impl<C: CommandLike> DataStore<C> for MemoryDataStore<C> {
    async fn issue(&self, id: InstanceId, cmd: C, notify: Asc<ExecNotify>) -> Result<()> {
        debug!(?id, "Executing command in memory");

        let cmd_vec = serde_json::to_vec(&cmd)?;
        self.kv_store.insert(id, cmd_vec);

        {
            let mut order = self.exec_order.lock().unwrap();
            order.push_back(id);
        }

        notify.notify_executed();
        Ok(())
    }
}
