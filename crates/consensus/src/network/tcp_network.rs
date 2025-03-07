use crate::AddrMap;
use crate::CommandLike;
use crate::MemberNetwork;
use crate::Message;
use crate::NetworkConfig;
use crate::ReplicaId;

use crate::utils::chan;
use crate::utils::codec::{self, bytes_sink, bytes_stream};
use crate::utils::lock::{with_mutex, with_read_lock, with_write_lock};

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::{SinkExt, StreamExt};
use numeric_cast::NumericCast;
use ordered_vecmap::VecMap;
use ordered_vecmap::VecSet;
use parking_lot::Mutex as SyncMutex;
use parking_lot::RwLock as SyncRwLock;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error};

pub struct Connection {
    tx: mpsc::Sender<Bytes>,
    task: Option<JoinHandle<()>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(ref task) = self.task {
            task.abort();
        }
    }
}

impl<C> Listener<C>
where
    C: DeserializeOwned,
{
    pub async fn recv(&mut self) -> Option<Result<Message<C>>> {
        let bytes = self.rx.recv().await?;
        Some(codec::deserialize_owned(&bytes))
    }
}

pub struct Listener<C> {
    rx: mpsc::Receiver<Bytes>,
    task: Option<JoinHandle<()>>,
    _marker: PhantomData<fn() -> C>,
}

impl<C> Drop for Listener<C> {
    fn drop(&mut self) {
        if let Some(ref task) = self.task {
            task.abort();
        }
    }
}

struct State {
    conns: VecMap<ReplicaId, Connection>,
    addr_map: AddrMap,
}

pub struct TcpNetwork<C: CommandLike> {
    state: SyncRwLock<State>,
    config: NetworkConfig,

    metrics: SyncMutex<NetworkMetrics>,

    _marker: PhantomData<fn(C) -> C>,
}

#[derive(Debug, Clone)]
pub struct NetworkMetrics {
    pub msg_total_size: u64,
    pub msg_count: u64,
}

impl<C> MemberNetwork<C> for TcpNetwork<C>
where
    C: CommandLike + Serialize + 'static,
{
    fn broadcast(&self, targets: VecSet<ReplicaId>, msg: Message<C>) {
        if targets.is_empty() {
            return;
        }
        if targets.len() == 1 {
            return self.send_one(targets.as_slice()[0], msg);
        }

        let msg_bytes = codec::serialize(&msg).expect("message should be able to be serialized");

        let mut txs = Vec::with_capacity(targets.len());
        with_read_lock(&self.state, |s| {
            s.conns.apply(&targets, |conn| txs.push(conn.tx.clone()));
        });
        {
            let cnt: u64 = targets.len().numeric_cast();
            let single_size: u64 = msg_bytes.len().numeric_cast();
            let total_size = single_size.wrapping_mul(cnt);
            with_mutex(&self.metrics, |metrics| {
                metrics.msg_total_size = metrics.msg_total_size.wrapping_add(total_size);
                metrics.msg_count = metrics.msg_count.wrapping_add(cnt);
            });
        }
        spawn(async move {
            let futures = txs.iter().map(|tx| chan::send(tx, msg_bytes.clone()));
            let _ = join_all(futures).await;
        });
    }

    fn send_one(&self, target: ReplicaId, msg: Message<C>) {
        let msg_bytes = codec::serialize(&msg).expect("message should be able to be serialized");
        let tx = with_read_lock(&self.state, |s| {
            s.conns.get(&target).map(|conn| conn.tx.clone())
        });
        if let Some(tx) = tx {
            {
                let single_size: u64 = msg_bytes.len().numeric_cast();
                with_mutex(&self.metrics, |metrics| {
                    metrics.msg_total_size = metrics.msg_total_size.wrapping_add(single_size);
                    metrics.msg_count = metrics.msg_count.wrapping_add(1);
                });
            }
            spawn(async move {
                let _ = chan::send(&tx, msg_bytes).await;
            });
        }
    }

    fn join(&self, replica_id: ReplicaId, addr: SocketAddr) -> Option<ReplicaId> {
        with_write_lock(&self.state, |s| {
            let prev_replica_id = s.addr_map.update(replica_id, addr);

            if let Some(prev) = prev_replica_id {
                let _ = s.conns.remove(&prev);
            }

            let _ = s
                .conns
                .insert(replica_id, Self::spawn_connector(addr, &self.config));

            prev_replica_id
        })
    }

    fn leave(&self, replica_id: ReplicaId) {
        let conn = with_write_lock(&self.state, |s| {
            s.addr_map.remove(replica_id);
            s.conns.remove(&replica_id)
        });
        drop(conn);
    }
}

impl<C: CommandLike> TcpNetwork<C> {
    #[must_use]
    pub fn new(config: &NetworkConfig) -> Self {
        Self {
            state: SyncRwLock::new(State {
                conns: VecMap::new(),
                addr_map: AddrMap::new(),
            }),
            config: config.clone(),
            metrics: SyncMutex::new(NetworkMetrics {
                msg_total_size: 0,
                msg_count: 0,
            }),
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub fn spawn_connector(addr: SocketAddr, config: &NetworkConfig) -> Connection {
        let chan_size = config.outbound_chan_size;
        let max_frame_length = config.max_frame_length;

        let initial_reconnect_timeout = config.initial_reconnect_timeout_us;
        let max_reconnect_timeout = config.max_reconnect_timeout_us;

        let (tx, rx) = mpsc::channel::<Bytes>(chan_size);

        let task = spawn(async move {
            let mut rx = rx;

            'drive: loop {
                let mut sink = {
                    let mut spin_weight: u64 = 1;
                    loop {
                        // FIXME: check rx.is_closed()
                        // https://github.com/tokio-rs/tokio/issues/4638
                        match TcpStream::connect(addr).await {
                            Ok(tcp) => {
                                break bytes_sink(tcp, max_frame_length);
                            }
                            Err(err) => {
                                spin_weight = spin_weight.wrapping_mul(2);

                                let timeout = Duration::from_micros(
                                    initial_reconnect_timeout
                                        .saturating_mul(spin_weight)
                                        .min(max_reconnect_timeout),
                                );

                                error!(?err, ?addr, ?timeout, "failed to reconnect");

                                sleep(timeout).await;
                            }
                        }
                    }
                };
                debug!(?addr, "tcp connection established");

                'forward: loop {
                    let mut item = match rx.recv().await {
                        Some(x) => x,
                        None => break 'drive,
                    };

                    loop {
                        match sink.feed(item).await {
                            Ok(()) => {}
                            Err(err) => {
                                error!(?err, "tcp connection error");
                                break 'forward;
                            }
                        }

                        match rx.try_recv() {
                            Ok(x) => item = x,
                            Err(_) => break,
                        }
                    }

                    match sink.flush().await {
                        Ok(()) => {}
                        Err(err) => {
                            error!(?err, "tcp connection error");
                            break 'forward;
                        }
                    }
                }
            }
        });

        Connection {
            tx,
            task: Some(task),
        }
    }

    pub fn spawn_listener(listener: TcpListener, config: &NetworkConfig) -> Listener<C> {
        let chan_size = config.inbound_chan_size;
        let max_frame_length = config.max_frame_length;

        let (tx, rx) = mpsc::channel::<Bytes>(chan_size);

        let task = spawn(async move {
            loop {
                let (tcp, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(err) => {
                        error!(?err, "tcp listener error");
                        break;
                    }
                };
                if tx.is_closed() {
                    break;
                }
                let mut stream = bytes_stream(tcp, max_frame_length);
                let tx = tx.clone();
                spawn(async move {
                    while let Some(result) = stream.next().await {
                        let item = match result {
                            Ok(x) => x,
                            Err(err) => {
                                error!(?err, "tcp stream error");
                                break;
                            }
                        };
                        match chan::send(&tx, item).await {
                            Ok(()) => {}
                            Err(_) => break,
                        }
                    }
                });
            }
        });

        Listener {
            rx,
            task: Some(task),
            _marker: PhantomData,
        }
    }

    pub fn metrics(&self) -> NetworkMetrics {
        with_mutex(&self.metrics, |metrics| metrics.clone())
    }
}
