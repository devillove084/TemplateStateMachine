
#[cfg(test)]
mod consensus_test {
    use std::{collections::HashMap, sync::Arc};

    use anyhow::Result;
    use consensus::{acc::Acc, deps::Deps, id::{Ballot, Epoch, InstanceId, LocalInstanceId, ReplicaId, Seq}, msg::{Message, PreAccept, Prepare}, replica::{Replica, ReplicaMeta}, store::{MemoryDataStore, MemoryLogStore}};
    use server::{config::NetworkConfig, net::TcpNetwork};

    fn GenerateConsensusEnv() {}

    #[tokio::test]
    async fn handle_propose_test() -> Result<()> {
        // 1. start one replica instance
        let memory_store_path = "memory";

        let replica_meta = ReplicaMeta::default();
        let inner_store = HashMap::new();
        let memory_log_store = Arc::new(MemoryLogStore::new(memory_store_path));
        let memory_data_store = Arc::new(MemoryDataStore::new(inner_store));
        let net_config = NetworkConfig::default();
        let net: TcpNetwork<_>= TcpNetwork::<String>::new(&net_config);
        let replica = Replica::new(replica_meta, memory_log_store, memory_data_store, net).await.unwrap();
        // 2. build propose msg
        let replica_id = ReplicaId::default();
        let epoch = Epoch::default();
        let id = InstanceId::default();
        let seq = Seq::default();
        let pbal = Ballot::default();
        let deps = Deps::default();
        let acc = Acc::default();
        // let pre_accept_msg = PreAccept{ sender: replica_id, epoch, id, pbal, cmd: None, seq, deps, acc };
        
        // let prepare_msg = Prepare { sender: todo!(), epoch, id, pbal, known: todo!() };
            
        
        // let propose = Message::Prepare();

        // // 3. call it
        // let res = replica.handle_message(propose).await;
        // println!("Result is: {:?}", res);
        Ok(())
    }
}
