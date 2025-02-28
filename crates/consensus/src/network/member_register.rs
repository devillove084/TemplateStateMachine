use std::net::SocketAddr;

use anyhow::Result;
use ordered_vecmap::VecMap;
use serde::{Deserialize, Serialize};

use crate::{Epoch, ReplicaId, RpcClientConfig, RpcConnection};

#[derive(Debug, Serialize, Deserialize)]
pub enum RegisterArgs {
    Register(MemberRegisterArgs),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RegisterOutput {
    Register(MemberRegisterOutput),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemberRegisterArgs {
    pub public_peer_addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemberRegisterOutput {
    pub replica_id: ReplicaId,
    pub epoch: Epoch,
    pub peers: VecMap<ReplicaId, SocketAddr>,
    pub prev_replica_id: Option<ReplicaId>,
}

pub struct MemberRegister {
    conn: RpcConnection<RegisterArgs, RegisterOutput>,
}

impl MemberRegister {
    pub async fn connect(remote_addr: SocketAddr, config: &RpcClientConfig) -> Result<Self> {
        let conn = RpcConnection::connect(remote_addr, config).await?;
        Ok(Self { conn })
    }

    pub async fn register(&self, args: MemberRegisterArgs) -> Result<MemberRegisterOutput> {
        let output = self.conn.call(RegisterArgs::Register(args)).await?;
        match output {
            RegisterOutput::Register(ans) => Ok(ans),
            // _ => Err(anyhow!("unexpected rpc output type")),
        }
    }
}
