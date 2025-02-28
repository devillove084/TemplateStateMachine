use std::{net::SocketAddr, time::Duration};

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

use std::fs;

use anyhow::{Result, anyhow};
use camino::Utf8Path;
use serde::de::DeserializeOwned;

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ConsensusConfig {
    pub server: ServerConfig,
    pub replica: ReplicaConfig,
    pub network: NetworkConfig,
    pub log_db: LogDbConfig,
    pub data_db: DataDbConfig,
    pub rpc_client: RpcClientConfig,
    pub rpc_server: RpcServerConfig,
    pub batching: BatchingConfig,
    pub interval: IntervalConfig,
}

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ServerConfig {
    pub listen_peer_addr: SocketAddr,
    pub listen_client_addr: SocketAddr,
    pub member_watcher_addr: SocketAddr,
    pub public_peer_addr: SocketAddr,
    pub propose_limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub inbound_chan_size: usize,
    pub outbound_chan_size: usize,
    pub initial_reconnect_timeout_us: u64,
    pub max_reconnect_timeout_us: u64,
    pub max_frame_length: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            inbound_chan_size: 1024,
            outbound_chan_size: 1024,
            initial_reconnect_timeout_us: 1000,
            max_reconnect_timeout_us: 1000,
            max_frame_length: 1024,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogDbConfig {
    pub path: Utf8PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataDbConfig {
    pub path: Utf8PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchingConfig {
    pub chan_size: usize,
    pub batch_initial_capacity: usize,
    pub batch_max_size: usize,
    pub batch_interval_us: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IntervalConfig {
    pub probe_rtt_interval_us: u64,
    pub clear_key_map_interval_us: u64,
    pub save_bounds_interval_us: u64,
    pub broadcast_bounds_interval_us: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ReplicaConfig {
    pub preaccept_timeout: PreAcceptTimeout,
    pub accept_timeout: AcceptTimeout,
    pub recover_timeout: RecoverTimeout,
    pub sync_limits: SyncLimits,
    pub join_timeout: JoinTimeout,
    pub optimization: Optimization,
    pub execution_limits: ExecutionLimits,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PreAcceptTimeout {
    /// default timeout, in microseconds
    pub default_us: u64,
    pub enable_adaptive: bool,
}

impl PreAcceptTimeout {
    pub fn with(
        &self,
        avg_rtt: Option<Duration>,
        f: impl FnOnce(Duration) -> Duration,
    ) -> Duration {
        let default = Duration::from_micros(self.default_us);
        if self.enable_adaptive {
            avg_rtt.map_or(default, f)
        } else {
            default
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AcceptTimeout {
    pub default_us: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RecoverTimeout {
    /// default timeout, in microseconds
    pub default_us: u64,
    pub enable_adaptive: bool,
}

impl RecoverTimeout {
    pub fn with(
        &self,
        avg_rtt: Option<Duration>,
        f: impl FnOnce(Duration) -> Duration,
    ) -> Duration {
        let default = Duration::from_micros(self.default_us);
        if self.enable_adaptive {
            avg_rtt.map_or(default, f)
        } else {
            default
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SyncLimits {
    pub max_instance_num: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct JoinTimeout {
    pub default_us: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Optimization {
    pub enable_acc: bool,
    pub probe_rtt_per_msg_count: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ExecutionLimits {
    pub max_task_num: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcClientConfig {
    pub max_frame_length: usize,
    pub op_chan_size: usize,
    pub forward_chan_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcServerConfig {
    pub max_frame_length: usize,
    pub max_task_num: usize,
}

#[inline]
pub fn read_config_file<T>(path: &Utf8Path) -> Result<T>
where
    T: DeserializeOwned,
{
    match path.extension() {
        Some("toml") => {
            let content = fs::read_to_string(path)?;
            let config: T = toml::from_str(&content)?;
            Ok(config)
        }
        Some("json") => {
            let content = fs::read(path)?;
            let config: T = serde_json::from_slice(&content)?;
            Ok(config)
        }
        _ => Err(anyhow!("unknown config file type")),
    }
}
