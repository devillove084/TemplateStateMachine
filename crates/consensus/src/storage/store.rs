use crate::protocol::CommandLike;
use crate::protocol::ExecNotify;
use crate::protocol::Instance;
use crate::protocol::Status;
use crate::protocol::{AttrBounds, SavedStatusBounds, StatusBounds};
use crate::protocol::{Ballot, InstanceId};

use std::sync::Arc;

use anyhow::Result;
use asc::Asc;

#[async_trait::async_trait]
pub trait LogStore<C: CommandLike>: Send + Sync + 'static {
    async fn save(
        self: &Arc<Self>,
        id: InstanceId,
        ins: Instance<C>,
        mode: UpdateMode,
    ) -> Result<()>;

    async fn load(self: &Arc<Self>, id: InstanceId) -> Result<Option<Instance<C>>>;

    async fn save_pbal(self: &Arc<Self>, id: InstanceId, pbal: Ballot) -> Result<()>;

    async fn load_pbal(self: &Arc<Self>, id: InstanceId) -> Result<Option<Ballot>>;

    async fn save_bounds(
        self: &Arc<Self>,
        attr_bounds: AttrBounds,
        status_bounds: SavedStatusBounds,
    ) -> Result<()>;

    async fn load_bounds(self: &Arc<Self>) -> Result<(AttrBounds, StatusBounds)>;

    async fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub enum UpdateMode {
    Full,
    Partial,
}

#[async_trait::async_trait]
pub trait DataStore<C>: Send + Sync + 'static {
    async fn issue(self: &Arc<Self>, id: InstanceId, cmd: C, notify: Asc<ExecNotify>)
    -> Result<()>;
}
