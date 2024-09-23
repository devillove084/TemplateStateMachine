use std::sync::Arc;
use tokio::net::TcpListener;

use crate::{recv_message, Configure, ConsensusImpl};

pub struct RpcServer {
    server: Arc<ConsensusImpl>,
    listener: TcpListener,
}

impl RpcServer {
    // TODO: only need conf
    pub async fn new(conf: &Configure, server: Arc<ConsensusImpl>) -> Self {
        let listener = TcpListener::bind(conf.peer.get(conf.index).unwrap())
            .await
            .map_err(|e| panic!("bind server address error, {e}"))
            .unwrap();
        Self { server, listener }
    }

    pub async fn serve(&self) -> crate::Result<()> {
        loop {
            let (mut stream, _) = self.listener.accept().await.expect("serve error");
            let server = self.server.clone();
            tokio::spawn(async move {
                loop {
                    let message = recv_message(&mut stream).await;
                    server.handle_message(message).await;
                }
            });
        }
    }
}
