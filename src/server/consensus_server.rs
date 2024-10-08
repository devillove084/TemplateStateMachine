use std::sync::Arc;

use tonic::transport::Server;
use tracing::info;

use crate::{consensus_service_server::ConsensusServiceServer, ConsensusImpl};

pub async fn run_server(port: u16, service: Arc<ConsensusImpl>) -> crate::Result<()> {
    let addr = ("127.0.0.1:".to_string() + &port.to_string())
        .parse()
        .unwrap();

    info!("ConsensusService listening on {}", addr);

    Server::builder()
        .add_service(ConsensusServiceServer::new((*service).clone()))
        .serve(addr)
        .await
        .expect("build server failed");
    Ok(())
}
