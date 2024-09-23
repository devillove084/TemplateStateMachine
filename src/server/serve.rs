use super::RpcServer;
use crate::{Configure, ConsensusImpl};

use std::sync::Arc;

pub struct Server {
    rpc_server: RpcServer,
}

impl Server {
    pub async fn new(conf: Configure) -> Self {
        let inner = Arc::new(ConsensusImpl::new(conf).await);
        let rpc_server = RpcServer::new(inner.conf(), inner.clone()).await;
        Self { rpc_server }
    }

    pub async fn run(&self) {
        let _ = self.rpc_server.serve().await;
    }
}

pub struct DefaultServer {
    inner: Server,
}

impl DefaultServer {
    pub async fn new(conf: Configure) -> Self {
        Self {
            inner: Server::new(conf).await,
        }
    }

    pub async fn run(&self) {
        self.inner.run().await;
    }
}
