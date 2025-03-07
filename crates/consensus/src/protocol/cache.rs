use super::bounds::AttrBounds;
use super::cmd::{CommandLike, Keys};
use super::deps::MutableDeps;
use super::id::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Seq};
use super::instance::Instance;

use crate::utils::cmp::max_assign;
use crate::utils::iter::{copied_map_collect, map_collect};

use std::collections::{HashMap, hash_map};
use std::mem;

use fnv::FnvHashMap;
use ordered_vecmap::VecMap;

pub struct LogCache<C>
where
    C: CommandLike,
{
    ins_cache: VecMap<ReplicaId, FnvHashMap<LocalInstanceId, Instance<C>>>,
    propose_ballot_cache: FnvHashMap<InstanceId, Ballot>,

    max_key_map: HashMap<C::Key, MaxKey>,
    max_lid_map: VecMap<ReplicaId, MaxLid>,
    max_seq: MaxSeq,
}

pub struct MaxKey {
    seq: Seq,
    local_instance_ids: VecMap<ReplicaId, LocalInstanceId>,
}

struct MaxLid {
    checkpoint: LocalInstanceId,
    any: LocalInstanceId,
}

struct MaxSeq {
    checkpoint: Seq,
    any: Seq,
}

impl<C> LogCache<C>
where
    C: CommandLike,
{
    #[must_use]
    pub fn new(attr_bounds: AttrBounds) -> Self {
        let max_key_map = HashMap::new();

        let max_lid_map = copied_map_collect(
            attr_bounds.max_local_instance_ids.iter(),
            |(replica_id, local_instance_id)| {
                let max_lid = MaxLid {
                    checkpoint: local_instance_id,
                    any: local_instance_id,
                };
                (replica_id, max_lid)
            },
        );

        let max_seq = MaxSeq {
            checkpoint: attr_bounds.max_seq,
            any: attr_bounds.max_seq,
        };

        let ins_cache = VecMap::new();
        let propose_ballot_cache = FnvHashMap::default();

        Self {
            ins_cache,
            propose_ballot_cache,
            max_key_map,
            max_lid_map,
            max_seq,
        }
    }

    pub fn calc_attributes(&self, id: InstanceId, keys: &C::Keys) -> (Seq, MutableDeps) {
        let mut deps = MutableDeps::with_capacity(self.max_lid_map.len());
        let mut seq = Seq::ZERO;
        let InstanceId(replica_id, local_instance_id) = id;

        if keys.is_unbounded() {
            let others = self.max_lid_map.iter().filter(|(r, _)| *r != replica_id);
            for &(r, ref m) in others {
                deps.insert(InstanceId(r, m.any));
            }
            max_assign(&mut seq, self.max_seq.any);
        } else {
            keys.for_each(|k| {
                if let Some(m) = self.max_key_map.get(k) {
                    let others = m
                        .local_instance_ids
                        .iter()
                        .filter(|(r, _)| *r != replica_id);
                    for &(r, l) in others {
                        deps.insert(InstanceId(r, l));
                    }
                    max_assign(&mut seq, m.seq);
                }
            });
            let others = self.max_lid_map.iter().filter(|(r, _)| *r != replica_id);
            for &(r, ref m) in others {
                if m.checkpoint > LocalInstanceId::ZERO {
                    deps.insert(InstanceId(r, m.checkpoint));
                }
            }
            max_assign(&mut seq, self.max_seq.checkpoint);
        }
        if local_instance_id > LocalInstanceId::ONE {
            deps.insert(InstanceId(replica_id, local_instance_id.sub_one()));
        }
        seq = seq.add_one();
        (seq, deps)
    }

    pub fn update_attrs(&mut self, id: InstanceId, keys: C::Keys, seq: Seq) {
        let InstanceId(replica_id, local_instance_id) = id;

        if keys.is_unbounded() {
            self.max_lid_map
                .entry(replica_id)
                .and_modify(|m| {
                    max_assign(&mut m.checkpoint, local_instance_id);
                    max_assign(&mut m.any, local_instance_id);
                })
                .or_insert_with(|| MaxLid {
                    checkpoint: local_instance_id,
                    any: local_instance_id,
                });

            max_assign(&mut self.max_seq.checkpoint, seq);
            max_assign(&mut self.max_seq.any, seq);
        } else {
            keys.for_each(|k| match self.max_key_map.entry(k.clone()) {
                hash_map::Entry::Occupied(mut e) => {
                    let m = e.get_mut();
                    max_assign(&mut m.seq, seq);
                    m.local_instance_ids
                        .entry(replica_id)
                        .and_modify(|l| max_assign(l, local_instance_id))
                        .or_insert(local_instance_id);
                }
                hash_map::Entry::Vacant(e) => {
                    let local_instance_ids = VecMap::from_single(replica_id, local_instance_id);
                    e.insert(MaxKey {
                        seq,
                        local_instance_ids,
                    });
                }
            });

            self.max_lid_map
                .entry(replica_id)
                .and_modify(|m| max_assign(&mut m.any, local_instance_id))
                .or_insert_with(|| MaxLid {
                    checkpoint: LocalInstanceId::ZERO,
                    any: local_instance_id,
                });

            max_assign(&mut self.max_seq.any, seq);
        }
    }

    pub fn clear_key_map(&mut self) -> HashMap<<C as CommandLike>::Key, MaxKey> {
        let cap = self.max_key_map.capacity() / 2;
        let new_key_map = HashMap::with_capacity(cap);

        let garbage = mem::replace(&mut self.max_key_map, new_key_map);

        for (_, m) in &mut self.max_lid_map {
            m.checkpoint = m.any;
        }
        {
            let m = &mut self.max_seq;
            m.checkpoint = m.any;
        }

        garbage
    }

    #[must_use]
    pub fn get_instance(&self, id: InstanceId) -> Option<&Instance<C>> {
        let row = self.ins_cache.get(&id.0)?;
        row.get(&id.1)
    }

    #[must_use]
    pub fn get_mut_instance(&mut self, id: InstanceId) -> Option<&mut Instance<C>> {
        let row = self.ins_cache.get_mut(&id.0)?;
        row.get_mut(&id.1)
    }

    #[must_use]
    pub fn contains_instance(&self, id: InstanceId) -> bool {
        match self.ins_cache.get(&id.0) {
            Some(row) => row.contains_key(&id.1),
            None => false,
        }
    }

    pub fn insert_instance(&mut self, id: InstanceId, ins: Instance<C>) {
        let row = self.ins_cache.entry(id.0).or_default();
        if row.insert(id.1, ins).is_none() {
            self.propose_ballot_cache.remove(&id);
        }
    }

    pub fn insert_orphan_propose_ballot(&mut self, id: InstanceId, propose_ballot: Ballot) {
        self.propose_ballot_cache.insert(id, propose_ballot);
    }

    #[must_use]
    pub fn contains_orphan_propose_ballot(&self, id: InstanceId) -> bool {
        self.propose_ballot_cache.contains_key(&id)
    }

    #[must_use]
    pub fn get_propose_ballot(&self, id: InstanceId) -> Option<Ballot> {
        if let Some(ins) = self.get_instance(id) {
            return Some(ins.propose_ballot);
        }
        self.propose_ballot_cache.get(&id).copied()
    }

    pub fn remove_instance(&mut self, id: InstanceId) {
        if let Some(row) = self.ins_cache.get_mut(&id.0) {
            if row.remove(&id.1).is_none() {
                self.propose_ballot_cache.remove(&id);
            }
        }
    }

    #[must_use]
    pub fn calc_attr_bounds(&self) -> AttrBounds {
        AttrBounds {
            max_seq: self.max_seq.any,
            max_local_instance_ids: map_collect(&self.max_lid_map, |&(replica_id, ref m)| {
                (replica_id, m.any)
            }),
        }
    }
}
