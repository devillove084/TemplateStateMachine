use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use ordered_vecmap::VecSet;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, error};

use crate::{CommandLike, Message, ReplicaId};

use super::MemberNetwork;

type MessageSender<C> = Sender<Message<C>>;
type MessageReceiver<C> = Receiver<Message<C>>;

pub struct LocalNetworkState<C: CommandLike> {
    addr_map: HashMap<ReplicaId, SocketAddr>,
    senders: HashMap<ReplicaId, MessageSender<C>>,
    receivers: HashMap<ReplicaId, MessageReceiver<C>>,
}

pub struct LocalNetwork<C: CommandLike> {
    net_state: Mutex<LocalNetworkState<C>>,

    filter: Mutex<Option<Box<dyn Fn(&Message<C>) -> bool + Send + Sync>>>,
}

impl<C: CommandLike> LocalNetwork<C> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            net_state: Mutex::new(LocalNetworkState {
                addr_map: HashMap::new(),
                senders: HashMap::new(),
                receivers: HashMap::new(),
            }),
            filter: Mutex::new(None),
        })
    }

    pub fn get_receiver(&self, replica_id: ReplicaId) -> Option<MessageReceiver<C>> {
        self.net_state.lock().unwrap().receivers.remove(&replica_id)
    }

    pub fn set_filter<F>(&mut self, filter: F)
    where
        F: Fn(&Message<C>) -> bool + Send + Sync + 'static,
    {
        *self.filter.lock().unwrap() = Some(Box::new(filter));
    }
}

#[async_trait::async_trait]
impl<C: CommandLike> MemberNetwork<C> for LocalNetwork<C> {
    fn broadcast(&self, targets: VecSet<ReplicaId>, msg: Message<C>) {
        if let Some(filter) = &*self.filter.lock().unwrap() {
            if !filter(&msg) {
                return;
            }
        }

        for replica_id in targets {
            self.send_one(replica_id, msg.clone());
        }
    }

    fn send_one(&self, target: ReplicaId, msg: Message<C>) {
        let state = self.net_state.lock().unwrap();

        if let Some(tx) = state.senders.get(&target) {
            match tx.try_send(msg) {
                Ok(()) => debug!("Sent message to {:?}", target),
                Err(e) => {
                    error!("Send message failed {:?}", e)
                }
            }
        }
    }

    fn join(&self, replica_id: ReplicaId, addr: SocketAddr) -> Option<ReplicaId> {
        let mut state = self.net_state.lock().unwrap();

        let prev_replica_id = state
            .addr_map
            .iter()
            .find(|&(_, &a)| a == addr)
            .map(|(&replica_id, _)| replica_id);

        let (tx, rx) = mpsc::channel(1024);
        state.senders.insert(replica_id, tx);
        state.receivers.insert(replica_id, rx);
        state.addr_map.insert(replica_id, addr);

        prev_replica_id
    }

    fn leave(&self, replica_id: ReplicaId) {
        let mut state = self.net_state.lock().unwrap();
        state.senders.remove(&replica_id);
        state.receivers.remove(&replica_id);
        state.addr_map.remove(&replica_id);
    }
}
