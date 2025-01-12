//! client calls server

use crate::{ReplicaId, SavedStatusBounds};

use super::rpc::{RpcClientConfig, RpcConnection};

use std::net::SocketAddr;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Args {
    Get(GetArgs),
    Set(SetArgs),
    Del(DelArgs),
    GetMetrics(GetMetricsArgs),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Output {
    Get(GetOutput),
    Set(SetOutput),
    Del(DelOutput),
    GetMetrics(GetMetricsOutput),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetArgs {
    pub key: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetOutput {
    pub value: Option<Bytes>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetArgs {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetOutput {}

#[derive(Debug, Serialize, Deserialize)]
pub struct DelArgs {
    pub key: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DelOutput {}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetMetricsArgs {}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetMetricsOutput {
    pub network_msg_total_size: u64,
    pub network_msg_count: u64,
    pub proposed_single_cmd_count: u64,
    pub proposed_batched_cmd_count: u64,
    pub replica_rid: ReplicaId,
    pub replica_preaccept_fast_path: u64,
    pub replica_preaccept_slow_path: u64,
    pub replica_recover_nop_count: u64,
    pub replica_recover_success_count: u64,
    pub replica_status_bounds: SavedStatusBounds,
    pub executed_single_cmd_count: u64,
    pub executed_batched_cmd_count: u64,
}

pub struct ServerForClient {
    conn: RpcConnection<Args, Output>,
}

macro_rules! declare_rpc {
    ($method: ident, $kind: ident, $args: ident, $output: ident) => {
        pub async fn $method(&self, args: $args) -> Result<$output> {
            let output = self.conn.call(Args::$kind(args)).await?;
            match output {
                Output::$kind(output) => Ok(output),
                _ => Err(anyhow!("unexpected rpc output type")),
            }
        }
    };
}

impl ServerForClient {
    pub async fn connect(remote_addr: SocketAddr, config: &RpcClientConfig) -> Result<Self> {
        let conn = RpcConnection::connect(remote_addr, config).await?;
        Ok(Self { conn })
    }

    declare_rpc!(get, Get, GetArgs, GetOutput);
    declare_rpc!(set, Set, SetArgs, SetOutput);
    declare_rpc!(del, Del, DelArgs, DelOutput);
    declare_rpc!(get_metrics, GetMetrics, GetMetricsArgs, GetMetricsOutput);
}
