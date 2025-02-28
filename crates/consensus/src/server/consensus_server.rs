use anyhow::anyhow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use asc::Asc;
use numeric_cast::NumericCast;
use parking_lot::Mutex as SyncMutex;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::{Semaphore, mpsc};
use tracing::debug;
use tracing::error;
use wgp::WaitGroup;

use crate::atomic_flag::AtomicFlag;
use crate::lock::with_mutex;
use crate::{
    BatchedCommand, ClientArgs, ClientDeleteArgs, ClientDeleteOutput, ClientGetArgs,
    ClientGetOutput, ClientOutput, ClientSetArgs, ClientSetOutput, Command, CommandKind,
    CommandNotify, ConsensusConfig, DataStore, Del, Get, Listener, LogStore, MemberNetwork,
    MemberRegister, MemberRegisterArgs, MutableCommand, Replica, ReplicaMeta, Service, Set,
    TcpNetwork, chan,
};

pub struct ConsensusServer {
    replica: Arc<Replica<BatchedCommand>>,
    config: ConsensusConfig,
    cmd_tx: mpsc::Sender<Command>,
    metrics: SyncMutex<ConsensusMetrics>,
    propose_limit: Arc<Semaphore>,
    is_waiting_shutdown: AtomicFlag,
    waitgroup: WaitGroup,
}

#[derive(Clone)]
struct ConsensusMetrics {
    proposed_single_cmd_count: u64,
    proposed_batched_cmd_count: u64,
}

impl ConsensusServer {
    pub async fn run(
        config: ConsensusConfig,
        log_store: Arc<dyn LogStore<BatchedCommand>>,
        data_store: Arc<dyn DataStore<BatchedCommand>>,
        net: Arc<dyn MemberNetwork<BatchedCommand>>,
    ) -> Result<()> {
        let replica = {
            let (replica_id, epoch, peers) = {
                let remote_addr = config.server.member_watcher_addr;
                let membership_register =
                    MemberRegister::connect(remote_addr, &config.rpc_client).await?;

                let public_peer_addr = config.server.public_peer_addr;
                let output = membership_register
                    .register(MemberRegisterArgs { public_peer_addr })
                    .await?;

                debug!(?output, "replica member register");

                (output.replica_id, output.epoch, output.peers)
            };

            let public_peer_addr = config.server.public_peer_addr;

            let meta = ReplicaMeta {
                replica_id,
                epoch,
                peers,
                public_peer_addr,
                config: config.replica.clone(),
            };

            Replica::new(meta, log_store, data_store, net).await?
        };

        let (cmd_tx, cmd_rx) = mpsc::channel(config.batching.chan_size);

        let server = {
            let metrics = SyncMutex::new(ConsensusMetrics {
                proposed_single_cmd_count: 0,
                proposed_batched_cmd_count: 0,
            });

            let propose_limit =
                Arc::new(Semaphore::new(config.server.propose_limit.numeric_cast()));

            let is_waiting_shutdown = AtomicFlag::new(false);
            let waitgroup = WaitGroup::new();

            Arc::new(ConsensusServer {
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
        crate::serve(self, listener, config, working).await
    }

    async fn shutdown(self: Arc<Self>) -> Result<()> {
        // do what?
        Ok(())
    }
}

#[async_trait::async_trait]
impl Service<ClientArgs> for ConsensusServer {
    type Output = ClientOutput;

    async fn call(self: &Arc<Self>, args: ClientArgs) -> Result<Self::Output> {
        self.handle_client_rpc(args).await
    }

    fn needs_stop(&self) -> bool {
        self.is_waiting_shutdown.get()
    }
}

impl ConsensusServer {
    async fn handle_client_rpc(self: &Arc<Self>, args: ClientArgs) -> Result<ClientOutput> {
        match args {
            ClientArgs::Get(args) => self.client_rpc_get(args).await.map(ClientOutput::Get),
            ClientArgs::Set(args) => self.client_rpc_set(args).await.map(ClientOutput::Set),
            ClientArgs::Del(args) => self.client_rpc_del(args).await.map(ClientOutput::Del),
            ClientArgs::GetMetrics(_) => unimplemented!(), //     .client_rpc_get_metrics(args)
                                                           //     .await
                                                           //     .map(Output::GetMetrics),
        }
    }

    async fn client_rpc_get(self: &Arc<Self>, args: ClientGetArgs) -> Result<ClientGetOutput> {
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
            Some(value) => Ok(ClientGetOutput { value }),
            None => Err(anyhow!("failed to receive command output")),
        }
    }

    async fn client_rpc_set(self: &Arc<Self>, args: ClientSetArgs) -> Result<ClientSetOutput> {
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
        Ok(ClientSetOutput {})
    }

    async fn client_rpc_del(
        self: &Arc<Self>,
        args: ClientDeleteArgs,
    ) -> Result<ClientDeleteOutput> {
        let notify = Asc::new(CommandNotify::new());
        let cmd = Command::from_mutable(MutableCommand {
            kind: CommandKind::Del(Del { key: args.key }),
            notify: Some(Asc::clone(&notify)),
        });
        chan::send(&self.cmd_tx, cmd)
            .await
            .map_err(|_| anyhow!("failed to send command"))?;
        notify.wait_committed().await;
        Ok(ClientDeleteOutput {})
    }

    // async fn client_rpc_get_metrics(
    //     self: &Arc<Self>,
    //     _: GetMetricsArgs,
    // ) -> Result<GetMetricsOutput> {
    //     let network = self.replica.network().metrics();
    //     let server = with_mutex(&self.metrics, |m| m.clone());
    //     let replica = self.replica.metrics();
    //     let data_db = self.replica.data_store().metrics();
    //     let status_bounds = self.replica.dump_saved_status_bounds();
    //     let replica_replica_id = self.replica.replica_id();

    //     Ok(GetMetricsOutput {
    //         network_msg_total_size: network.msg_total_size,
    //         network_msg_count: network.msg_count,
    //         proposed_single_cmd_count: server.proposed_single_cmd_count,
    //         proposed_batched_cmd_count: server.proposed_batched_cmd_count,
    //         replica_replica_id,
    //         replica_preaccept_fast_path: replica.preaccept_fast_path,
    //         replica_preaccept_slow_path: replica.preaccept_slow_path,
    //         replica_recover_nop_count: replica.recover_nop_count,
    //         replica_recover_success_count: replica.recover_success_count,
    //         replica_status_bounds: status_bounds,
    //         executed_single_cmd_count: data_db.executed_single_cmd_count,
    //         executed_batched_cmd_count: data_db.executed_batched_cmd_count,
    //     })
    // }

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

                let batch = std::mem::replace(&mut batch, Vec::with_capacity(initial_capacity));

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
        let _ = self
            .replica
            .run_propose(cmd)
            .await
            .map_err(|_| anyhow!("propose failed"));
        Ok(())
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
