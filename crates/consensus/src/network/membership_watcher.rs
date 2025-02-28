use std::ops::Not;
use std::sync::Arc;
use std::time::Duration;
use std::{fs, net::SocketAddr};

use anyhow::Result;
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::Mutex;
use tracing::{debug, error};
use wgp::WaitGroup;

use crate::RpcServerConfig;
use crate::{AddrMap, Epoch, NextGenerator, ReplicaId, Service, atomic_flag::AtomicFlag, serve};

use super::{MemberRegisterArgs, MemberRegisterOutput, RegisterArgs, RegisterOutput};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipWatcherConfig {
    pub state_path: Utf8PathBuf,
    pub listen_rpc_addr: SocketAddr,
    pub save_state_interval_us: u64,
    pub rpc_server: RpcServerConfig,
}

pub struct MembershipWatcher {
    state: Mutex<MembershipState>,
    dirty: AtomicFlag,
    config: MembershipWatcherConfig,
    is_waiting_shutdown: AtomicFlag,
    waitgroup: WaitGroup,
}

#[derive(Debug, Serialize, Deserialize)]
struct MembershipState {
    replica_id_generator: NextGenerator<ReplicaId>,
    epoch_generator: NextGenerator<Epoch>,
    addr_map: AddrMap,
}

impl Default for MembershipState {
    fn default() -> Self {
        Self {
            replica_id_generator: NextGenerator::new(ReplicaId::ZERO),
            epoch_generator: NextGenerator::new(Epoch::ZERO),
            addr_map: AddrMap::new(),
        }
    }
}

impl MembershipState {
    fn load_or_create(path: &Utf8Path) -> Result<Self> {
        if path.exists() {
            let content = fs::read(path)?;
            Ok(serde_json::from_slice(&content)?)
        } else {
            let state = Self::default();
            state.save(path)?;
            Ok(state)
        }
    }

    fn save(&self, path: &Utf8Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let content = serde_json::to_vec(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}

impl MembershipWatcher {
    pub async fn run(config: MembershipWatcherConfig) -> Result<()> {
        let state = {
            let path = config.state_path.as_ref();
            MembershipState::load_or_create(path)?
        };
        debug!(?state);

        let watcher = {
            let state = Mutex::new(state);
            let dirty = AtomicFlag::new(false);

            let is_waiting_shutdown = AtomicFlag::new(false);
            let waitgroup = WaitGroup::new();

            Arc::new(MembershipWatcher {
                state,
                dirty,
                config,
                is_waiting_shutdown,
                waitgroup,
            })
        };

        let mut bg_tasks = Vec::new();

        {
            let listener = TcpListener::bind(watcher.config.listen_rpc_addr).await?;
            let this = Arc::clone(&watcher);
            bg_tasks.push(spawn(this.serve_rpc(listener)));
        };

        {
            let this = Arc::clone(&watcher);
            bg_tasks.push(spawn(this.interval_save_state()));
        }

        {
            tokio::signal::ctrl_c().await?;
        }

        {
            watcher.is_waiting_shutdown.set(true);
            for task in &bg_tasks {
                task.abort();
            }
            drop(bg_tasks);

            let task_count = watcher.waitgroup.count();
            debug!(?task_count, "waiting running tasks");
            watcher.waitgroup.wait().await;
        }

        watcher.shutdown().await?;

        Ok(())
    }

    async fn serve_rpc(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        let config = self.config.rpc_server.clone();
        let working = self.waitgroup.working();
        serve(self, listener, config, working).await
    }

    async fn interval_save_state(self: Arc<Self>) -> Result<()> {
        let mut interval =
            tokio::time::interval(Duration::from_micros(self.config.save_state_interval_us));

        loop {
            interval.tick().await;

            if self.is_waiting_shutdown.get() {
                break;
            }

            if self.dirty.get().not() {
                continue;
            }

            let this = Arc::clone(&self);
            let working = self.waitgroup.working();
            spawn(async move {
                if let Err(err) = this.run_save_state().await {
                    error!(?err, "interval save state");
                }
                drop(working);
            });
        }

        Ok(())
    }

    async fn shutdown(self: &Arc<Self>) -> Result<()> {
        self.run_save_state().await
    }
}

#[async_trait::async_trait]
impl Service<RegisterArgs> for MembershipWatcher {
    type Output = RegisterOutput;

    async fn call(self: &Arc<Self>, args: RegisterArgs) -> Result<RegisterOutput> {
        self.handle_rpc(args).await
    }

    fn needs_stop(&self) -> bool {
        self.is_waiting_shutdown.get()
    }
}

impl MembershipWatcher {
    async fn handle_rpc(self: &Arc<Self>, args: RegisterArgs) -> Result<RegisterOutput> {
        match args {
            RegisterArgs::Register(args) => {
                self.rpc_register(args).await.map(RegisterOutput::Register)
            }
        }
    }

    async fn rpc_register(
        self: &Arc<Self>,
        args: MemberRegisterArgs,
    ) -> Result<MemberRegisterOutput> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let replica_id = s.replica_id_generator.gen_next();
        let epoch = s.epoch_generator.gen_next();

        let mut peers = s.addr_map.map().clone();

        let prev_replica_id = s.addr_map.update(replica_id, args.public_peer_addr);

        if let Some(prev_replica_id) = prev_replica_id {
            let _ = peers.remove(&prev_replica_id);
        }

        self.dirty.set(true);

        drop(guard);

        let output = MemberRegisterOutput {
            replica_id,
            epoch,
            peers,
            prev_replica_id,
        };
        Ok(output)
    }

    async fn run_save_state(&self) -> Result<()> {
        let guard = self.state.lock().await;
        let s = &*guard;
        if self.dirty.get() {
            s.save(&self.config.state_path)?;
            debug!(state=?s);
            self.dirty.set(false);
        }
        drop(guard);
        Ok(())
    }
}
