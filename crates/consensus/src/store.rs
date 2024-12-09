use crate::bounds::{AttrBounds, SavedStatusBounds, StatusBounds};
use crate::exec::ExecNotify;
use crate::file_io::{FileIO, FileIOBuilder};
use crate::id::{Ballot, InstanceId};
use crate::ins::Instance;
use crate::status::Status;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use asc::Asc;
use tokio::sync::oneshot;

pub trait LogStore<C>: Send + Sync + 'static {
    fn save(
        self: &Arc<Self>,
        id: InstanceId,
        ins: Instance<C>,
        mode: UpdateMode,
    ) -> oneshot::Receiver<Result<()>>;

    fn load(self: &Arc<Self>, id: InstanceId) -> oneshot::Receiver<Result<Option<Instance<C>>>>;

    fn save_pbal(self: &Arc<Self>, id: InstanceId, pbal: Ballot) -> oneshot::Receiver<Result<()>>;

    fn load_pbal(self: &Arc<Self>, id: InstanceId) -> oneshot::Receiver<Result<Option<Ballot>>>;

    fn save_bounds(
        self: &Arc<Self>,
        attr_bounds: AttrBounds,
        status_bounds: SavedStatusBounds,
    ) -> oneshot::Receiver<Result<()>>;

    fn load_bounds(self: &Arc<Self>) -> oneshot::Receiver<Result<(AttrBounds, StatusBounds)>>;

    fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> oneshot::Receiver<Result<()>>;
}

#[derive(Debug, Clone, Copy)]
pub enum UpdateMode {
    Full,
    Partial,
}

#[async_trait::async_trait]
pub trait DataStore<C>: Send + Sync + 'static {
    async fn issue(
        self: &Arc<Self>,
        id: InstanceId,
        cmd: C,
        notify: Asc<ExecNotify>,
    ) -> Result<()>;
}


pub struct MemoryLogStore {
    file_io: FileIO,
    // cmds: Vec<C>,
}

impl MemoryLogStore {
    pub fn new(path: &str) -> Self {
        let file_io = FileIOBuilder::new(path).build().expect("build file io failed");
        Self { file_io }
    }
}

impl<C: Send + Sync + 'static> LogStore<C> for MemoryLogStore {
    fn save(
        self: &Arc<Self>,
        id: InstanceId,
        ins: Instance<C>,
        mode: UpdateMode,
    ) -> oneshot::Receiver<Result<()>> {
        todo!()
    }

    fn load(self: &Arc<Self>, id: InstanceId) -> oneshot::Receiver<Result<Option<Instance<C>>>> {
        todo!()
    }

    fn save_pbal(self: &Arc<Self>, id: InstanceId, pbal: Ballot) -> oneshot::Receiver<Result<()>> {
        todo!()
    }

    fn load_pbal(self: &Arc<Self>, id: InstanceId) -> oneshot::Receiver<Result<Option<Ballot>>> {
        todo!()
    }

    fn save_bounds(
        self: &Arc<Self>,
        attr_bounds: AttrBounds,
        status_bounds: SavedStatusBounds,
    ) -> oneshot::Receiver<Result<()>> {
        todo!()
    }

    fn load_bounds(self: &Arc<Self>) -> oneshot::Receiver<Result<(AttrBounds, StatusBounds)>> {
        todo!()
    }

    fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> oneshot::Receiver<Result<()>> {
        todo!()
    }
}

pub struct MemoryDataStore {
    store: HashMap<String, String>,
}

impl MemoryDataStore {
    pub fn new(store: HashMap<String, String>) -> Self {
        Self {
            store
        }
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