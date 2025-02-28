use crate::{ReplicaId, RpcClientConfig, RpcConnection, SavedStatusBounds};

use std::net::SocketAddr;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum ClientArgs {
    Get(ClientGetArgs),
    Set(ClientSetArgs),
    Del(ClientDeleteArgs),
    GetMetrics(GetMetricsArgs),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ClientOutput {
    Get(ClientGetOutput),
    Set(ClientSetOutput),
    Del(ClientDeleteOutput),
    GetMetrics(GetMetricsOutput),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientGetArgs {
    pub key: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientGetOutput {
    pub value: Option<Bytes>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientSetArgs {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientSetOutput {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientDeleteArgs {
    pub key: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientDeleteOutput {}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetMetricsArgs {}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetMetricsOutput {
    pub network_msg_total_size: u64,
    pub network_msg_count: u64,
    pub proposed_single_cmd_count: u64,
    pub proposed_batched_cmd_count: u64,
    pub replica_replica_id: ReplicaId,
    pub replica_preaccept_fast_path: u64,
    pub replica_preaccept_slow_path: u64,
    pub replica_recover_nop_count: u64,
    pub replica_recover_success_count: u64,
    pub replica_status_bounds: SavedStatusBounds,
    pub executed_single_cmd_count: u64,
    pub executed_batched_cmd_count: u64,
}

pub struct ClientAsk {
    conn: RpcConnection<ClientArgs, ClientOutput>,
}

macro_rules! declare_rpc {
    ($method: ident, $kind: ident, $args: ident, $output: ident) => {
        pub async fn $method(&self, args: $args) -> Result<$output> {
            let output = self.conn.call(ClientArgs::$kind(args)).await?;
            match output {
                ClientOutput::$kind(output) => Ok(output),
                _ => Err(anyhow!("unexpected rpc output type")),
            }
        }
    };
}

impl ClientAsk {
    pub async fn connect(remote_addr: SocketAddr, config: &RpcClientConfig) -> Result<Self> {
        let conn = RpcConnection::connect(remote_addr, config).await?;
        Ok(Self { conn })
    }

    declare_rpc!(get, Get, ClientGetArgs, ClientGetOutput);
    declare_rpc!(set, Set, ClientSetArgs, ClientSetOutput);
    declare_rpc!(del, Del, ClientDeleteArgs, ClientDeleteOutput);
    declare_rpc!(get_metrics, GetMetrics, GetMetricsArgs, GetMetricsOutput);
}
