use crate::{consensus_service_client::ConsensusServiceClient, ConsensusMessage, ReplicaId};
use dashmap::DashMap;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::info;

use super::ConsensusImpl;

pub struct PeerCommunication {
    pub peers: DashMap<ReplicaId, PeerConnection>,
}

pub struct PeerConnection {
    sender: mpsc::Sender<ConsensusMessage>,
}

impl Default for PeerCommunication {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerCommunication {
    pub fn new() -> Self {
        Self {
            peers: DashMap::new(),
        }
    }

    pub async fn add_peer(&self, replica_id: ReplicaId, connection: PeerConnection) {
        self.peers.insert(replica_id, connection);
    }

    pub async fn remove_peer(&self, replica_id: &ReplicaId) {
        self.peers.remove(replica_id);
    }

    pub async fn send_message(
        &self,
        target_replica_id: &ReplicaId,
        message: ConsensusMessage,
    ) -> crate::Result<()> {
        if let Some(peer) = self.peers.get(target_replica_id) {
            let sender = &peer.sender;
            sender.send(message).await.expect("send error")
        } else {
            info!(
                "No connection found for replica {}",
                target_replica_id.value
            );
        }
        Ok(())
    }

    pub async fn broadcast_message(&self, message: ConsensusMessage) -> crate::Result<()> {
        for entry in self.peers.iter() {
            let replica_id = entry.key();
            let peer = entry.value();
            if let Err(e) = peer.sender.send(message.clone()).await {
                info!(
                    "Failed to send message to replica {:?}: {:?}",
                    replica_id, e
                );
            }
        }
        Ok(())
    }

    pub async fn connect_to_peer(
        self: Arc<Self>,
        replica_id: ReplicaId,
        address: SocketAddr,
        consensus_impl: Arc<ConsensusImpl>,
    ) -> crate::Result<()> {
        let consensus_replica_id = consensus_impl.get_replica_id().await;
        if replica_id == consensus_replica_id {
            info!("Node is trying to connect to itself, skipping...");
            return Ok(());
        }

        info!(
            "Attempting to connect to peer with ID: {}",
            consensus_replica_id.value
        );

        let (sender, receiver) = establish_connection(address).await?;
        let connection = PeerConnection {
            sender: sender.clone(),
        };

        self.add_peer(consensus_replica_id, connection).await;
        info!("Added peer with ID: {} to peer map", replica_id.value);

        let consensus_impl_clone = consensus_impl.clone();
        tokio::spawn(async move {
            let mut receiver = receiver;
            while let Some(message) = receiver.recv().await {
                consensus_impl_clone
                    .handle_message(message, sender.clone())
                    .await;
            }
        });

        Ok(())
    }
}

async fn establish_connection(
    address: SocketAddr,
) -> crate::Result<(
    mpsc::Sender<ConsensusMessage>,
    mpsc::Receiver<ConsensusMessage>,
)> {
    let endpoint = format!("http://{}", address);
    let channel = Channel::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .expect("create channel failed");
    info!("Successfully established connection to: {}", address);

    let mut client = ConsensusServiceClient::new(channel);

    let (tx_to_peer, rx_to_peer) = mpsc::channel(1024);
    let (tx_from_peer, rx_from_peer) = mpsc::channel(1024);

    let outbound = ReceiverStream::new(rx_to_peer);
    let response = client
        .stream_messages(outbound)
        .await
        .expect("create stream failed");
    let mut inbound = response.into_inner();

    info!("Successfully established streaming with peer: {}", address);

    tokio::spawn(async move {
        while let Some(result) = inbound.next().await {
            match result {
                Ok(message) => {
                    if let Err(e) = tx_from_peer.send(message).await {
                        info!("Failed to send message to local receiver: {:?}", e);
                        break;
                    }
                }
                Err(e) => {
                    info!("Error receiving message from peer: {:?}", e);
                    break;
                }
            }
        }
    });

    Ok((tx_to_peer, rx_from_peer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus_service_server::ConsensusService;
    use crate::{ConsensusMessage, ReplicaId};
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::Stream;
    use tokio_stream::StreamExt;
    use tonic::{Request, Response, Status};

    // Mock ConsensusImpl for testing
    #[derive(Clone)]
    struct MockConsensusImpl {
        // For simplicity, we'll store received messages in a vector
        received_messages: Arc<tokio::sync::Mutex<Vec<ConsensusMessage>>>,
    }

    impl MockConsensusImpl {
        async fn handle_message(
            &self,
            message: ConsensusMessage,
            _sender: mpsc::Sender<ConsensusMessage>,
        ) {
            self.received_messages.lock().await.push(message);
        }
    }

    // Implement ConsensusService for MockConsensusImpl
    #[tonic::async_trait]
    impl ConsensusService for MockConsensusImpl {
        type StreamMessagesStream =
            Pin<Box<dyn Stream<Item = Result<ConsensusMessage, Status>> + Send + Sync + 'static>>;

        async fn stream_messages(
            &self,
            request: Request<tonic::Streaming<ConsensusMessage>>,
        ) -> Result<Response<Self::StreamMessagesStream>, Status> {
            let mut inbound = request.into_inner();
            let self_clone = self.clone();

            tokio::spawn(async move {
                while let Some(result) = inbound.next().await {
                    match result {
                        Ok(message) => {
                            // We don't need to send messages back in this mock
                            let _ = self_clone.handle_message(message, mpsc::channel(1).0).await;
                        }
                        Err(e) => {
                            info!("Error receiving message: {:?}", e);
                            break;
                        }
                    }
                }
            });

            // Return an empty stream for outbound messages
            let (_tx, rx) = mpsc::channel(1);
            let outbound = ReceiverStream::new(rx);
            Ok(Response::new(
                Box::pin(outbound) as Self::StreamMessagesStream
            ))
        }
    }

    #[tokio::test]
    async fn test_add_peer() {
        // Create a PeerCommunication instance
        let peer_comm = PeerCommunication::new();

        // Create a dummy PeerConnection
        let (tx, _rx) = mpsc::channel(1024);
        let peer_connection = PeerConnection { sender: tx };

        // Create a ReplicaId
        let replica_id = ReplicaId { value: 1 };

        // Add the peer
        peer_comm.add_peer(replica_id, peer_connection).await;

        // Check that the peer exists in the peers map
        assert!(peer_comm.peers.contains_key(&replica_id));
    }

    #[tokio::test]
    async fn test_remove_peer() {
        // Create a PeerCommunication instance
        let peer_comm = PeerCommunication::new();

        // Create a dummy PeerConnection
        let (tx, _rx) = mpsc::channel(1024);
        let peer_connection = PeerConnection { sender: tx };

        // Create a ReplicaId
        let replica_id = ReplicaId { value: 1 };

        // Add the peer
        peer_comm.add_peer(replica_id, peer_connection).await;

        // Remove the peer
        peer_comm.remove_peer(&replica_id).await;

        // Check that the peer no longer exists
        assert!(!peer_comm.peers.contains_key(&replica_id));
    }

    #[tokio::test]
    async fn test_send_message() {
        // Create a PeerCommunication instance
        let peer_comm = PeerCommunication::new();

        // Create a message channel for testing
        let (tx, mut rx) = mpsc::channel(1024);
        let peer_connection = PeerConnection { sender: tx };

        // Create a ReplicaId
        let replica_id = ReplicaId { value: 1 };

        // Add the peer
        peer_comm.add_peer(replica_id, peer_connection).await;

        // Create a test message
        let test_message = ConsensusMessage {
            message: Some(crate::consensus_message::Message::Empty(crate::Empty {})),
        };

        // Send the message
        peer_comm
            .send_message(&replica_id, test_message.clone())
            .await
            .unwrap();

        // Receive the message
        if let Some(received_message) = rx.recv().await {
            // Check that the message is as expected
            assert_eq!(received_message, test_message);
        } else {
            panic!("Did not receive message");
        }
    }

    #[tokio::test]
    async fn test_broadcast_message() {
        // Create a PeerCommunication instance
        let peer_comm = PeerCommunication::new();

        // Create message channels for two peers
        let (tx1, mut rx1) = mpsc::channel(1024);
        let peer_connection1 = PeerConnection { sender: tx1 };
        let (tx2, mut rx2) = mpsc::channel(1024);
        let peer_connection2 = PeerConnection { sender: tx2 };

        // Create ReplicaIds
        let replica_id1 = ReplicaId { value: 1 };
        let replica_id2 = ReplicaId { value: 2 };

        // Add the peers
        peer_comm.add_peer(replica_id1, peer_connection1).await;
        peer_comm.add_peer(replica_id2, peer_connection2).await;

        // Create a test message
        let test_message = ConsensusMessage {
            message: Some(crate::consensus_message::Message::Empty(crate::Empty {})),
        };

        // Broadcast the message
        peer_comm
            .broadcast_message(test_message.clone())
            .await
            .unwrap();

        // Receive the messages from both peers
        if let Some(received_message1) = rx1.recv().await {
            assert_eq!(received_message1, test_message);
        } else {
            panic!("Peer 1 did not receive the message");
        }

        if let Some(received_message2) = rx2.recv().await {
            assert_eq!(received_message2, test_message);
        } else {
            panic!("Peer 2 did not receive the message");
        }
    }
}
