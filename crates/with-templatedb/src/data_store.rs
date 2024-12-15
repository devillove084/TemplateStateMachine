use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use asc::Asc;
use consensus::{DataStore, ExecNotify, InstanceId};

pub struct MemoryDataStore {
    store: HashMap<String, String>,
}

impl MemoryDataStore {
    pub fn new(store: HashMap<String, String>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl<C: Send + Sync + 'static> DataStore<C> for MemoryDataStore {
    async fn issue(
        self: &Arc<Self>,
        id: InstanceId,
        cmd: C,
        notify: Asc<ExecNotify>,
    ) -> Result<()> {
        todo!()
    }
}
