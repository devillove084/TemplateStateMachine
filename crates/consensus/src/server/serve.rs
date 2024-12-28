use std::future::Future;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use asc::Asc;
use numeric_cast::NumericCast;
use parking_lot::Mutex as SyncMutex;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::{Semaphore, mpsc};
use tracing::debug;
use tracing::error;
use wgp::WaitGroup;

use crate::Args::Set;
use crate::atomic_flag::AtomicFlag;
use crate::lock::with_mutex;
use crate::{CommandLike, DataStore, LogStore, MembershipChange, Replica, ReplicaMeta, chan};

use super::net::Listener;
use super::{
    Args, Config, DelOutput, GetArgs, GetMetricsArgs, GetMetricsOutput, GetOutput, Output, Service,
    SetArgs, SetOutput, serve,
};

pub struct Server<C, L, D, N>
where
    C: CommandLike,
    L: LogStore<C>,
    D: DataStore<C>,
    N: MembershipChange<C>,
{
    replica: Arc<Replica<C, L, D, N>>,
    config: Config,
    cmd_tx: mpsc::Sender<C>,
    metrics: SyncMutex<Metrics>,
    propose_limit: Arc<Semaphore>,
    is_waiting_shutdown: AtomicFlag,
    waitgroup: WaitGroup,
}

#[derive(Clone)]
struct Metrics {
    proposed_single_cmd_count: u64,
    proposed_batched_cmd_count: u64,
}

impl<C, L, D, N> Server<C, L, D, N>
where
    C: CommandLike,
    L: LogStore<C>,
    D: DataStore<C>,
    N: MembershipChange<C>,
{
    pub async fn run(config: Config) -> Result<()> {
        let replica = {
            let log_store = LogDb::new(&config.log_db.path)?;
            let data_store = DataDb::new(&config.data_db.path)?;
            let net = TcpNetwork::new(&config.network);

            let (rid, epoch, peers) = {
                let remote_addr = config.server.monitor_addr;
                let monitor = Monitor::connect(remote_addr, &config.rpc_client).await?;

                let public_peer_addr = config.server.public_peer_addr;
                let output = monitor
                    .register(sm::RegisterArgs { public_peer_addr })
                    .await?;

                debug!(?output, "monitor register");

                (output.rid, output.epoch, output.peers)
            };

            let public_peer_addr = config.server.public_peer_addr;

            let meta = ReplicaMeta {
                rid,
                epoch,
                peers,
                public_peer_addr,
                config: config.replica.clone(),
            };

            Replica::new(meta, log_store, data_store, net).await?
        };

        let (cmd_tx, cmd_rx) = mpsc::channel(config.batching.chan_size);

        let server = {
            let metrics = SyncMutex::new(Metrics {
                proposed_single_cmd_count: 0,
                proposed_batched_cmd_count: 0,
            });

            let propose_limit =
                Arc::new(Semaphore::new(config.server.propose_limit.numeric_cast()));

            let is_waiting_shutdown = AtomicFlag::new(false);
            let waitgroup = WaitGroup::new();

            Arc::new(Server {
                replica,
                config,
                cmd_tx,
                metrics,
                propose_limit,
                is_waiting_shutdown,
                waitgroup,
            })
        };

        let mut bg_tasks = Vec::new();

        {
            let this = Arc::clone(&server);

            let listener = {
                let addr = this.config.server.listen_peer_addr;
                TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("failed to bind to {addr}"))?
            };

            let listener = TcpNetwork::spawn_listener(listener, &this.config.network);
            bg_tasks.push(spawn(this.serve_peer(listener)));
        }

        {
            let this = Arc::clone(&server);
            let listener = TcpListener::bind(this.config.server.listen_client_addr).await?;
            bg_tasks.push(spawn(this.serve_client(listener)));
        }

        {
            let this = Arc::clone(&server);
            bg_tasks.push(spawn(this.cmd_batcher(cmd_rx)))
        }

        {
            let this = Arc::clone(&server);
            bg_tasks.push(spawn(this.interval_probe_rtt()));
        }

        {
            let this = Arc::clone(&server);
            bg_tasks.push(spawn(this.interval_clear_key_map()));
        }

        {
            let this = Arc::clone(&server);
            bg_tasks.push(spawn(this.interval_save_bounds()));
        }

        {
            let this = Arc::clone(&server);
            bg_tasks.push(spawn(this.interval_broadcast_bounds()));
        }

        {
            tokio::signal::ctrl_c().await?;
        }

        {
            server.is_waiting_shutdown.set(true);
            for task in &bg_tasks {
                task.abort();
            }
            drop(bg_tasks);

            let task_count = server.waitgroup.count();
            debug!(?task_count, "waiting running tasks");
            server.waitgroup.wait().await;
        }

        server.shutdown().await?;

        Ok(())
    }

    async fn serve_peer(self: Arc<Self>, mut listener: Listener<BatchedCommand>) -> Result<()> {
        {
            let this = Arc::clone(&self);
            spawn(async move {
                if let Err(err) = this.replica.run_sync_known().await {
                    error!(?err, "run_sync_known");
                }
            });
        }

        {
            let this = Arc::clone(&self);
            spawn(async move {
                if let Err(err) = this.replica.run_join().await {
                    error!(?err, "run_join")
                }
            });
        }

        while let Some(result) = listener.recv().await {
            if self.is_waiting_shutdown.get() {
                break;
            }
            match result {
                Ok(msg) => {
                    let this = Arc::clone(&self);
                    let working = self.waitgroup.working();
                    spawn(async move {
                        if let Err(err) = this.replica.handle_message(msg).await {
                            error!(?err, "handle_message");
                        }
                        drop(working);
                    });
                }
                Err(err) => {
                    error!(?err, "listener recv");
                    continue;
                }
            }
        }

        Ok(())
    }

    async fn serve_client(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        let config = self.config.rpc_server.clone();
        let working = self.waitgroup.working();
        serve(self, listener, config, working).await
    }

    async fn shutdown(self: Arc<Self>) -> Result<()> {
        // do what?
        Ok(())
    }
}

impl Service<Args> for Server {
    type Output = super::Output;

    fn call(self: &Arc<Self>, args: Args) -> impl Future<Output = Result<Self::Output>> + Send {
        self.handle_client_rpc(args)
    }

    fn needs_stop(&self) -> bool {
        self.is_waiting_shutdown.get()
    }
}

impl Server {
    async fn handle_client_rpc(self: &Arc<Self>, args: Args) -> Result<Output> {
        match args {
            Args::Get(args) => self.client_rpc_get(args).await.map(Output::Get),
            Args::Set(args) => self.client_rpc_set(args).await.map(Output::Set),
            Args::Del(args) => self.client_rpc_del(args).await.map(Output::Del),
            Args::GetMetrics(args) => self
                .client_rpc_get_metrics(args)
                .await
                .map(Output::GetMetrics),
        }
    }

    async fn client_rpc_get(self: &Arc<Self>, args: GetArgs) -> Result<GetOutput> {
        let (tx, mut rx) = mpsc::channel(1);
        let cmd = Command::from_mutable(MutableCommand {
            kind: CommandKind::Get(Get {
                key: args.key,
                tx: Some(tx),
            }),
            notify: None,
        });
        chan::send(&self.cmd_tx, cmd)
            .await
            .map_err(|_| anyhow!("failed to send command"))?;
        match rx.recv().await {
            Some(value) => Ok(GetOutput { value }),
            None => Err(anyhow!("failed to receive command output")),
        }
    }

    async fn client_rpc_set(self: &Arc<Self>, args: SetArgs) -> Result<SetOutput> {
        let notify = Asc::new(CommandNotify::new());
        let cmd = Command::from_mutable(MutableCommand {
            kind: CommandKind::Set(Set {
                key: args.key,
                value: args.value,
            }),
            notify: Some(Asc::clone(&notify)),
        });
        chan::send(&self.cmd_tx, cmd)
            .await
            .map_err(|_| anyhow!("failed to send command"))?;
        notify.wait_committed().await;
        Ok(SetOutput {})
    }

    async fn client_rpc_del(self: &Arc<Self>, args: DelArgs) -> Result<DelOutput> {
        let notify = Asc::new(CommandNotify::new());
        let cmd = Command::from_mutable(MutableCommand {
            kind: CommandKind::Del(Del { key: args.key }),
            notify: Some(Asc::clone(&notify)),
        });
        chan::send(&self.cmd_tx, cmd)
            .await
            .map_err(|_| anyhow!("failed to send command"))?;
        notify.wait_committed().await;
        Ok(DelOutput {})
    }

    async fn client_rpc_get_metrics(
        self: &Arc<Self>,
        _: GetMetricsArgs,
    ) -> Result<GetMetricsOutput> {
        let network = self.replica.network().metrics();
        let server = with_mutex(&self.metrics, |m| m.clone());
        let replica = self.replica.metrics();
        let data_db = self.replica.data_store().metrics();
        let status_bounds = self.replica.dump_saved_status_bounds();
        let replica_rid = self.replica.rid();

        Ok(GetMetricsOutput {
            network_msg_total_size: network.msg_total_size,
            network_msg_count: network.msg_count,
            proposed_single_cmd_count: server.proposed_single_cmd_count,
            proposed_batched_cmd_count: server.proposed_batched_cmd_count,
            replica_rid,
            replica_preaccept_fast_path: replica.preaccept_fast_path,
            replica_preaccept_slow_path: replica.preaccept_slow_path,
            replica_recover_nop_count: replica.recover_nop_count,
            replica_recover_success_count: replica.recover_success_count,
            replica_status_bounds: status_bounds,
            executed_single_cmd_count: data_db.executed_single_cmd_count,
            executed_batched_cmd_count: data_db.executed_batched_cmd_count,
        })
    }

    async fn cmd_batcher(self: Arc<Self>, mut rx: mpsc::Receiver<Command>) -> Result<()> {
        let initial_capacity = self.config.batching.batch_initial_capacity;
        let max_size = self.config.batching.batch_max_size;
        let mut interval = tokio::time::interval(Duration::from_micros(
            self.config.batching.batch_interval_us,
        ));

        let mut batch = Vec::with_capacity(initial_capacity);

        'interval: loop {
            interval.tick().await;

            loop {
                if self.is_waiting_shutdown.get() {
                    break 'interval;
                }

                loop {
                    match rx.try_recv() {
                        Ok(cmd) => batch.push(cmd.into_mutable()),
                        Err(_) => break,
                    }

                    if batch.len() >= max_size {
                        break;
                    }
                }

                if batch.is_empty() {
                    continue 'interval;
                }

                let batch = mem::replace(&mut batch, Vec::with_capacity(initial_capacity));

                let this = Arc::clone(&self);
                let permit = self.propose_limit.clone().acquire_owned().await.unwrap();
                let working = self.waitgroup.working();
                spawn(async move {
                    let cmd = BatchedCommand::from_vec(batch);
                    if let Err(err) = this.handle_batched_command(cmd).await {
                        error!(?err, "handle batched command")
                    }
                    drop(working);
                    drop(permit);
                });
            }
        }
        Ok(())
    }

    async fn handle_batched_command(self: &Arc<Self>, cmd: BatchedCommand) -> Result<()> {
        let cnt: u64 = cmd.as_slice().len().numeric_cast();
        with_mutex(&self.metrics, |m| {
            m.proposed_single_cmd_count = m.proposed_single_cmd_count.wrapping_add(cnt);
            m.proposed_batched_cmd_count = m.proposed_batched_cmd_count.wrapping_add(1);
        });
        debug!("batch len: {:?}", cnt);
        self.replica.run_propose(cmd).await
    }

    async fn interval_probe_rtt(self: Arc<Self>) -> Result<()> {
        let mut interval = {
            let duration = Duration::from_micros(self.config.interval.probe_rtt_interval_us);
            tokio::time::interval(duration)
        };

        loop {
            interval.tick().await;

            if self.is_waiting_shutdown.get() {
                break;
            }

            let this = Arc::clone(&self);
            spawn(async move {
                if let Err(err) = this.replica.run_probe_rtt().await {
                    error!(?err, "interval probe rtt")
                }
            });
        }

        Ok(())
    }

    async fn interval_clear_key_map(self: Arc<Self>) -> Result<()> {
        let mut interval = {
            let duration = Duration::from_micros(self.config.interval.clear_key_map_interval_us);
            tokio::time::interval(duration)
        };

        loop {
            interval.tick().await;

            if self.is_waiting_shutdown.get() {
                break;
            }

            let this = Arc::clone(&self);
            spawn(async move {
                this.replica.run_clear_key_map().await;
            });
        }

        Ok(())
    }

    async fn interval_save_bounds(self: Arc<Self>) -> Result<()> {
        let mut interval = {
            let duration = Duration::from_micros(self.config.interval.save_bounds_interval_us);
            tokio::time::interval(duration)
        };

        loop {
            interval.tick().await;

            if self.is_waiting_shutdown.get() {
                break;
            }

            let this = Arc::clone(&self);
            spawn(async move {
                if let Err(err) = this.replica.run_save_bounds().await {
                    error!(?err, "interval save bounds")
                }
            });
        }

        Ok(())
    }

    async fn interval_broadcast_bounds(self: Arc<Self>) -> Result<()> {
        let mut interval = {
            let duration = Duration::from_micros(self.config.interval.broadcast_bounds_interval_us);
            tokio::time::interval(duration)
        };

        loop {
            interval.tick().await;

            if self.is_waiting_shutdown.get() {
                break;
            }

            let this = Arc::clone(&self);
            spawn(async move {
                if let Err(err) = this.replica.run_broadcast_bounds().await {
                    error!(?err, "interval broadcast bounds")
                }
            });
        }

        Ok(())
    }
}
