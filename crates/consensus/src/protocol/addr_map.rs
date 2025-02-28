use std::net::SocketAddr;

use fnv::FnvHashMap;
use ordered_vecmap::VecMap;
use serde::{Deserialize, Serialize};

use super::ReplicaId;

#[derive(Debug, Serialize, Deserialize)]
pub struct AddrMap {
    map: VecMap<ReplicaId, SocketAddr>,
    rev: FnvHashMap<SocketAddr, ReplicaId>,
}

impl AddrMap {
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: VecMap::new(),
            rev: FnvHashMap::default(),
        }
    }

    pub fn update(&mut self, replica_id: ReplicaId, addr: SocketAddr) -> Option<ReplicaId> {
        if let Some(prev_addr) = self.map.insert(replica_id, addr) {
            self.rev.remove(&prev_addr);
        }
        let prev_replica_id = self.rev.insert(addr, replica_id);
        if let Some(prev_replica_id) = prev_replica_id {
            let _ = self.map.remove(&prev_replica_id);
        }
        prev_replica_id
    }

    pub fn remove(&mut self, replica_id: ReplicaId) {
        if let Some(addr) = self.map.remove(&replica_id) {
            self.rev.remove(&addr);
        }
    }

    #[must_use]
    pub fn map(&self) -> &VecMap<ReplicaId, SocketAddr> {
        &self.map
    }
}

impl Default for AddrMap {
    fn default() -> Self {
        Self::new()
    }
}
