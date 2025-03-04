use crate::cmd::BatchedCommand;
use crate::db_utils::{get_value, put_small_value, put_value};
use crate::log_key::{GlobalFieldKey, InstanceFieldKey};

use consensus::Acc;
use consensus::Deps;
use consensus::Instance;
use consensus::Status;
use consensus::{AttrBounds, SavedStatusBounds, StatusBounds, StatusMap};
use consensus::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Seq};
use consensus::{LogStore, UpdateMode};

use consensus::cmp::max_assign;
use consensus::codec;
use consensus::onemap::OneMap;

use std::ops::Not;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Result, ensure};
use bytemuck::bytes_of;
use bytemuck::checked::{from_bytes, try_from_bytes};
use camino::Utf8Path;
use ordered_vecmap::VecMap;
use rocksdb::{DB, DBRawIterator, WriteBatch};
use tracing::debug;

pub struct LogDb {
    db: DB,
}

impl LogDb {
    pub fn new(path: &Utf8Path) -> Result<Arc<Self>> {
        let db = DB::open_default(path)?;
        Ok(Arc::new(Self { db }))
    }

    pub async fn save(
        self: &Arc<Self>,
        id: InstanceId,
        ins: Instance<BatchedCommand>,
        mode: UpdateMode,
    ) -> Result<()> {
        let needs_save_cmd = match mode {
            UpdateMode::Full => true,
            UpdateMode::Partial => false,
        };

        let mut wb = WriteBatch::default();

        let mut log_key = InstanceFieldKey::new(id, 0);
        let mut buf = Vec::new();

        // status
        {
            log_key.set_field(InstanceFieldKey::FIELD_STATUS);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.status)?;
        }

        // seq
        {
            log_key.set_field(InstanceFieldKey::FIELD_SEQ);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.seq)?;
        }

        // propose_ballot
        {
            log_key.set_field(InstanceFieldKey::FIELD_propose_ballot);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.propose_ballot)?;
        }

        if needs_save_cmd {
            log_key.set_field(InstanceFieldKey::FIELD_CMD);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.cmd)?;
        }

        // (deps, accepted_ballot, acc)
        {
            log_key.set_field(InstanceFieldKey::FIELD_OTHERS);
            let value = (&ins.deps, ins.accepted_ballot, &ins.acc);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &value)?;
        }

        self.db.write(wb)?;
        Ok(())
    }

    pub async fn load(
        self: &Arc<Self>,
        id: InstanceId,
    ) -> Result<Option<Instance<BatchedCommand>>> {
        // <https://github.com/facebook/rocksdb/wiki/Basic-Operations#iteration>
        // <https://github.com/facebook/rocksdb/wiki/Iterator>

        let mut iter = self.db.raw_iterator();

        let status: Status = {
            let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_STATUS);

            iter.seek(bytes_of(&log_key));

            if iter.valid().not() {
                iter.status()?;
                return Ok(None);
            }

            let log_key: &InstanceFieldKey = match try_from_bytes(iter.key().unwrap()) {
                Ok(k) => k,
                Err(_) => return Ok(None),
            };

            if log_key.id() != id {
                return Ok(None);
            }
            if log_key.field() != InstanceFieldKey::FIELD_STATUS {
                return Ok(None);
            }

            codec::deserialize_owned(iter.value().unwrap())?
        };

        macro_rules! next_field {
            ($field:tt) => {{
                iter.next();
                iter.status()?;
                ensure!(iter.valid());
                let log_key: &InstanceFieldKey = from_bytes(iter.key().unwrap());
                assert_eq!(log_key.id(), id);
                assert_eq!(log_key.field(), InstanceFieldKey::$field);
                codec::deserialize_owned(iter.value().unwrap())?
            }};
        }

        let seq: Seq = next_field!(FIELD_SEQ);
        let propose_ballot: Ballot = next_field!(FIELD_propose_ballot);
        let cmd: BatchedCommand = next_field!(FIELD_CMD);
        let others: (Deps, Ballot, Acc) = next_field!(FIELD_OTHERS);
        let (deps, accepted_ballot, acc) = others;

        let ins = Instance {
            propose_ballot,
            cmd,
            seq,
            deps,
            accepted_ballot,
            status,
            acc,
        };

        Ok(Some(ins))
    }

    pub async fn save_propose_ballot(
        self: &Arc<Self>,
        id: InstanceId,
        propose_ballot: Ballot,
    ) -> Result<()> {
        let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_propose_ballot);
        put_small_value(&mut &self.db, bytes_of(&log_key), &propose_ballot)
    }

    pub async fn load_propose_ballot(self: &Arc<Self>, id: InstanceId) -> Result<Option<Ballot>> {
        let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_propose_ballot);
        get_value(&self.db, bytes_of(&log_key))
    }

    #[tracing::instrument(skip(self))]
    pub async fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> Result<()> {
        let t0 = Instant::now();
        let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_STATUS);
        let result = put_small_value(&mut &self.db, bytes_of(&log_key), &status);
        debug!(elapsed_us = ?t0.elapsed().as_micros(), "updated status");
        result
    }

    pub async fn save_bounds(
        self: &Arc<Self>,
        attr: AttrBounds,
        status: SavedStatusBounds,
    ) -> Result<()> {
        let mut buf = Vec::new();
        let mut wb = WriteBatch::default();
        {
            let log_key = GlobalFieldKey::new(GlobalFieldKey::FIELD_ATTR_BOUNDS);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &attr)?;
        }
        {
            let log_key = GlobalFieldKey::new(GlobalFieldKey::FIELD_STATUS_BOUNDS);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &status)?;
        }
        self.db.write(wb)?;
        Ok(())
    }

    fn load_bounds_optional(
        &self,
        iter: &mut DBRawIterator<'_>,
    ) -> Result<Option<(AttrBounds, SavedStatusBounds)>> {
        let log_key = GlobalFieldKey::new(GlobalFieldKey::FIELD_ATTR_BOUNDS);
        iter.seek(bytes_of(&log_key));

        if iter.valid().not() {
            iter.status()?;
            return Ok(None);
        }

        if bytes_of(&log_key) != iter.key().unwrap() {
            return Ok(None);
        }

        let attr_bounds: AttrBounds = codec::deserialize_owned(iter.value().unwrap())?;

        let log_key = GlobalFieldKey::new(GlobalFieldKey::FIELD_STATUS_BOUNDS);
        iter.seek(bytes_of(&log_key));
        iter.status()?;
        ensure!(iter.valid());
        assert_eq!(bytes_of(&log_key), iter.key().unwrap());
        let saved_status_bounds: SavedStatusBounds =
            codec::deserialize_owned(iter.value().unwrap())?;

        Ok(Some((attr_bounds, saved_status_bounds)))
    }

    pub async fn load_bounds(self: &Arc<Self>) -> Result<(AttrBounds, StatusBounds)> {
        let mut iter = self.db.raw_iterator();

        let (mut attr_bounds, saved_status_bounds) =
            self.load_bounds_optional(&mut iter)?.unwrap_or_else(|| {
                let attr_bounds = AttrBounds {
                    max_seq: Seq::ZERO,
                    max_local_instance_ids: VecMap::new(),
                };
                let saved_status_bounds = SavedStatusBounds::default();
                (attr_bounds, saved_status_bounds)
            });

        let mut status_bounds = {
            let mut maps: VecMap<ReplicaId, StatusMap> = VecMap::new();

            let create_default = || StatusMap {
                known: OneMap::new(0),
                committed: OneMap::new(0),
                executed: OneMap::new(0),
            };

            let mut merge =
                |map: &VecMap<ReplicaId, LocalInstanceId>,
                 project: fn(&mut StatusMap) -> &mut OneMap| {
                    for &(replica_id, local_instance_id) in map {
                        let m = maps.entry(replica_id).or_insert_with(create_default);
                        ((project)(m)).set_bound(local_instance_id.raw_value());
                    }
                };

            merge(&saved_status_bounds.known_up_to, |m| &mut m.known);
            merge(&saved_status_bounds.committed_up_to, |m| &mut m.committed);
            merge(&saved_status_bounds.executed_up_to, |m| &mut m.executed);

            StatusBounds::from_maps(maps)
        };

        {
            let field_status = InstanceFieldKey::FIELD_STATUS;

            let mut replica_id = ReplicaId::ONE;
            let mut local_instance_id = match saved_status_bounds.executed_up_to.get(&replica_id) {
                Some(jump_to) => jump_to.add_one(),
                None => LocalInstanceId::ONE,
            };

            loop {
                let log_key = InstanceFieldKey::new(InstanceId(replica_id, local_instance_id), field_status);

                iter.seek(bytes_of(&log_key));
                if iter.valid().not() {
                    break;
                }

                let log_key: &InstanceFieldKey = match try_from_bytes(iter.key().unwrap()) {
                    Ok(k) => k,
                    Err(_) => break,
                };

                let id = log_key.id();
                let is_replica_id_changed = replica_id != id.0;
                InstanceId(replica_id, local_instance_id) = id;

                if is_replica_id_changed {
                    if let Some(&jump_to) = saved_status_bounds.executed_up_to.get(&id.0) {
                        if local_instance_id < jump_to {
                            local_instance_id = jump_to.add_one();
                            continue;
                        }
                    }
                }

                if log_key.field() != field_status {
                    local_instance_id = local_instance_id.add_one();
                    continue;
                }

                let status: Status = codec::deserialize_owned(iter.value().unwrap())?;

                iter.next();
                if iter.valid().not() {
                    break;
                }

                let seq: Seq = codec::deserialize_owned(iter.value().unwrap())?;

                max_assign(&mut attr_bounds.max_seq, seq);
                attr_bounds
                    .max_local_instance_ids
                    .entry(replica_id)
                    .and_modify(|l| max_assign(l, local_instance_id))
                    .or_insert(local_instance_id);
                status_bounds.set(InstanceId(replica_id, local_instance_id), status);

                local_instance_id = local_instance_id.add_one();
            }

            iter.status()?;
        }

        status_bounds.update_bounds();

        debug!(?attr_bounds);

        Ok((attr_bounds, status_bounds))
    }
}

#[async_trait::async_trait]
impl LogStore<BatchedCommand> for LogDb {
    async fn save(
        self: &Arc<Self>,
        id: InstanceId,
        ins: Instance<BatchedCommand>,
        mode: UpdateMode,
    ) -> Result<()> {
        let this = Arc::clone(self);
        LogDb::save(&this, id, ins, mode).await
    }

    async fn load(self: &Arc<Self>, id: InstanceId) -> Result<Option<Instance<BatchedCommand>>> {
        let this = Arc::clone(self);
        LogDb::load(&this, id).await
    }

    async fn save_propose_ballot(
        self: &Arc<Self>,
        id: InstanceId,
        propose_ballot: Ballot,
    ) -> Result<()> {
        let this = Arc::clone(self);
        LogDb::save_propose_ballot(&this, id, propose_ballot).await
    }

    async fn load_propose_ballot(self: &Arc<Self>, id: InstanceId) -> Result<Option<Ballot>> {
        let this = Arc::clone(self);
        LogDb::load_propose_ballot(&this, id).await
    }

    async fn save_bounds(
        self: &Arc<Self>,
        attr_bounds: AttrBounds,
        status_bounds: SavedStatusBounds,
    ) -> Result<()> {
        let this = Arc::clone(self);
        LogDb::save_bounds(&this, attr_bounds, status_bounds).await
    }

    async fn load_bounds(self: &Arc<Self>) -> Result<(AttrBounds, StatusBounds)> {
        let this = Arc::clone(self);
        LogDb::load_bounds(&this).await
    }

    async fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> Result<()> {
        let this = Arc::clone(self);
        LogDb::update_status(&this, id, status).await
    }
}

#[cfg(test)]
mod tests {
    use consensus::Acc;
    use consensus::Instance;
    use consensus::Status;
    use consensus::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Round, Seq};
    use consensus::{Deps, MutableDeps};

    use consensus::UpdateMode;
    use consensus::codec;
    use ordered_vecmap::VecSet;

    use std::io;

    use anyhow::Result;
    use numeric_cast::NumericCast;
    use tempdir::TempDir;

    use crate::cmd::{BatchedCommand, CommandKind, Get, MutableCommand};
    use crate::log_db::LogDb;

    #[test]
    fn tuple_ref_serde() {
        let deps = {
            let mut deps = MutableDeps::with_capacity(1);
            deps.insert(InstanceId(2022.into(), 422.into()));
            Deps::from_mutable(deps)
        };

        let status = Status::Committed;
        let acc = VecSet::from_single(ReplicaId::from(2022));

        let input_tuple = &(&deps, status, &acc);

        let bytes = codec::serialize(input_tuple).unwrap();

        let output_tuple: (Deps, Status, VecSet<ReplicaId>) =
            codec::deserialize_owned(&bytes).unwrap();

        assert_eq!(input_tuple.0, &output_tuple.0);
        assert_eq!(input_tuple.1, output_tuple.1);
        assert_eq!(input_tuple.2, &output_tuple.2);
    }

    #[test]
    fn cursor_serde() {
        let input_propose_ballot = Ballot(Round::ONE, ReplicaId::ONE);

        let mut buf = [0u8; 64];
        let pos: usize = {
            let mut value_buf = io::Cursor::new(buf.as_mut_slice());
            codec::serialize_into(&mut value_buf, &input_propose_ballot).unwrap();
            value_buf.position().numeric_cast()
        };
        let value = &buf[..pos];

        let output_propose_ballot: Ballot = codec::deserialize_owned(value).unwrap();

        assert_eq!(input_propose_ballot, output_propose_ballot);
    }

    #[tokio::test]
    async fn test_rocksdb_log_store() -> Result<()> {
        let temp_dir = TempDir::new("rocksdb_test")?;
        let db_path = temp_dir.path().to_str().unwrap();

        let rocksdb_log_store = LogDb::new(db_path.into()).unwrap();

        let instance_id = InstanceId::new(ReplicaId::from(10), LocalInstanceId::from(11));
        let cmd = BatchedCommand::from_vec(vec![MutableCommand {
            kind: CommandKind::Get(Get {
                key: "hello".into(),
                tx: None,
            }),
            notify: None,
        }]);

        let instance = Instance {
            propose_ballot: Ballot::new(Round::ONE, ReplicaId::from(10)),
            cmd: cmd,
            seq: Seq::from(1),
            deps: Deps::default(),
            accepted_ballot: Ballot::new(Round::ONE, ReplicaId::from(10)),
            status: Status::PreAccepted,
            acc: Acc::default(),
        };

        rocksdb_log_store
            .save(instance_id, instance.clone(), UpdateMode::Full)
            .await?;

        let loaded_instance = rocksdb_log_store.load(instance_id).await?;
        assert!(loaded_instance.is_some());
        assert_eq!(
            loaded_instance.unwrap().propose_ballot,
            instance.propose_ballot
        );

        Ok(())
    }
}
