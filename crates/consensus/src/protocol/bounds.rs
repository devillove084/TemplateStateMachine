use super::id::{InstanceId, LocalInstanceId, ReplicaId, Seq};
use super::status::Status;

use crate::utils::iter::filter_map_collect;
use crate::utils::onemap::OneMap;

use ordered_vecmap::VecMap;
use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct AttrBounds {
    pub max_seq: Seq,
    pub max_local_instance_ids: VecMap<ReplicaId, LocalInstanceId>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SavedStatusBounds {
    pub known_up_to: VecMap<ReplicaId, LocalInstanceId>,
    pub committed_up_to: VecMap<ReplicaId, LocalInstanceId>,
    pub executed_up_to: VecMap<ReplicaId, LocalInstanceId>,
}

#[derive(Default)]
pub struct StatusBounds(VecMap<ReplicaId, StatusMap>);

#[derive(Default)]
pub struct StatusMap {
    pub known: OneMap,
    pub committed: OneMap,
    pub executed: OneMap,
}

impl AsRef<VecMap<ReplicaId, StatusMap>> for StatusBounds {
    fn as_ref(&self) -> &VecMap<ReplicaId, StatusMap> {
        &self.0
    }
}

impl AsMut<VecMap<ReplicaId, StatusMap>> for StatusBounds {
    fn as_mut(&mut self) -> &mut VecMap<ReplicaId, StatusMap> {
        &mut self.0
    }
}

impl StatusBounds {
    #[must_use]
    pub fn from_maps(maps: VecMap<ReplicaId, StatusMap>) -> Self {
        Self(maps)
    }

    pub fn from_saved(saved: &SavedStatusBounds) -> Self {
        let mut maps = VecMap::new();

        let default_status_map = || StatusMap {
            known: OneMap::new(0),
            committed: OneMap::new(0),
            executed: OneMap::new(0),
        };

        for (rid, lid) in &saved.known_up_to {
            maps.entry(*rid)
                .or_insert_with(default_status_map)
                .known
                .set_bound(lid.raw_value());
        }

        for (rid, lid) in &saved.committed_up_to {
            maps.entry(*rid)
                .or_insert_with(default_status_map)
                .committed
                .set_bound(lid.raw_value());
        }

        for (rid, lid) in &saved.executed_up_to {
            maps.entry(*rid)
                .or_insert_with(default_status_map)
                .executed
                .set_bound(lid.raw_value());
        }

        let mut status_bounds = StatusBounds(maps);
        status_bounds.update_bounds();
        status_bounds
    }

    pub fn set(&mut self, id: InstanceId, status: Status) {
        debug!(?id, ?status, "set status");
        let InstanceId(replica_id, local_instance_id) = id;
        let m = self.0.entry(replica_id).or_default();
        m.known.set(local_instance_id.raw_value());
        if status >= Status::Committed {
            m.committed.set(local_instance_id.raw_value());
        }
        if status >= Status::Executed {
            m.executed.set(local_instance_id.raw_value());
        }
    }

    pub fn update_bounds(&mut self) {
        self.0.iter_mut().for_each(|(_, m)| {
            m.known.update_bound();
            m.committed.update_bound();
            m.executed.update_bound();
        })
    }

    #[must_use]
    pub fn known_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        filter_map_collect(self.0.iter(), |&(r, ref m)| {
            let bound = m.known.bound();
            (bound > 0).then(|| (r, LocalInstanceId::from(bound)))
        })
    }

    #[must_use]
    pub fn committed_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        filter_map_collect(self.0.iter(), |&(r, ref m)| {
            let bound = m.committed.bound();
            (bound > 0).then(|| (r, LocalInstanceId::from(bound)))
        })
    }

    #[must_use]
    pub fn executed_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        filter_map_collect(self.0.iter(), |&(r, ref m)| {
            let bound = m.executed.bound();
            (bound > 0).then(|| (r, LocalInstanceId::from(bound)))
        })
    }
}

#[derive(Default)]
pub struct PeerStatusBounds {
    committed: VecMap<ReplicaId, VecMap<ReplicaId, LocalInstanceId>>,
    executed: VecMap<ReplicaId, VecMap<ReplicaId, LocalInstanceId>>,
}

impl PeerStatusBounds {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            committed: VecMap::new(),
            executed: VecMap::new(),
        }
    }

    pub fn set_committed(
        &mut self,
        replica_id: ReplicaId,
        bounds: VecMap<ReplicaId, LocalInstanceId>,
    ) {
        let _ = self.committed.insert(replica_id, bounds);
    }

    pub fn set_executed(
        &mut self,
        replica_id: ReplicaId,
        bounds: VecMap<ReplicaId, LocalInstanceId>,
    ) {
        let _ = self.executed.insert(replica_id, bounds);
    }

    #[must_use]
    pub fn committed_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        let mut ans = VecMap::new();
        for (_, m) in self.committed.iter() {
            ans.merge_copied_with(m, |lhs, rhs| lhs.min(rhs))
        }
        ans
    }
}
