use bytes::Bytes;
use consensus::Acc;
use consensus::CommandLike;
use consensus::Deps;
use consensus::FileIO;
use consensus::Instance;
use consensus::Status;
use consensus::cmp::max_assign;
use consensus::onemap::OneMap;
use consensus::{AttrBounds, SavedStatusBounds, StatusBounds, StatusMap};
use consensus::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Seq};
use consensus::{LogStore, UpdateMode};
use ordered_vecmap::VecMap;
use serde::de::DeserializeOwned;
use tracing::debug;

use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Result;

fn serialize<T: serde::Serialize>(val: &T) -> Result<Vec<u8>> {
    Ok(bincode::serialize(val)?)
}

fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(bincode::deserialize(bytes)?)
}

pub struct PathBuilder {
    scheme: String,
    prefix: String,
}

impl PathBuilder {
    pub fn new(scheme: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            scheme: scheme.into(),
            prefix: prefix.into(),
        }
    }

    pub fn instance_field_path(&self, id: InstanceId, field: u8) -> String {
        format!(
            "{}/instances/{}/{}/{}",
            self.prefix,
            format!("{:016x}", id.0.raw_value()),
            format!("{:016x}", id.1.raw_value()),
            self.field_to_str(field)
        )
    }

    pub fn global_field_path(&self, field: u8) -> String {
        format!("{}/global/{}", self.prefix, self.field_to_str(field))
    }

    fn field_to_str(&self, field: u8) -> &'static str {
        match field {
            1 => "status",
            2 => "seq",
            3 => "propose_ballot",
            4 => "cmd",
            5 => "others",
            6 => "attr_bounds",
            7 => "status_bounds",
            _ => "unknown",
        }
    }
}

impl std::fmt::Debug for PathBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PathBuilder")
            .field("scheme", &self.scheme)
            .field("prefix", &self.prefix)
            .finish()
    }
}

pub struct OpenDalLogStore<C: CommandLike> {
    file_io: FileIO,
    path_builder: PathBuilder,
    _ph: PhantomData<C>,
}

impl<C: CommandLike> OpenDalLogStore<C> {
    pub fn new(file_io: FileIO, scheme: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            file_io,
            path_builder: PathBuilder::new(scheme, prefix),
            _ph: PhantomData::default(),
        }
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        let output = self.file_io.new_output(path)?;
        output.write(Bytes::from(data.to_vec())).await
    }

    async fn read_file<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let input = self.file_io.new_input(path)?;
        let bytes = input.read().await?;
        deserialize(&bytes)
    }

    async fn file_exists(&self, path: &str) -> Result<bool> {
        self.file_io.exists(path).await
    }

    async fn list_instance_status_paths(&self) -> Result<Vec<String>> {
        let instances_prefix = format!("{}/instances/", self.path_builder.prefix);
        let all_instance_files = self.file_io.list(&instances_prefix).await?;
        let status_files: Vec<String> = all_instance_files
            .into_iter()
            .filter(|path| path.ends_with("/status"))
            .collect();
        Ok(status_files)
    }

    pub async fn save_action(
        self: &Arc<Self>,
        id: InstanceId,
        ins: Instance<C>,
        mode: UpdateMode,
    ) -> Result<()> {
        self.save(id, ins, mode).await
    }

    pub async fn load_action(self: &Arc<Self>, id: InstanceId) -> Result<Option<Instance<C>>> {
        self.load(id).await
    }
}

#[async_trait::async_trait]
impl<C: CommandLike> LogStore<C> for OpenDalLogStore<C> {
    async fn save(
        self: &Arc<Self>,
        id: InstanceId,
        ins: Instance<C>,
        mode: UpdateMode,
    ) -> Result<()> {
        let needs_save_cmd = matches!(mode, UpdateMode::Full);

        let status_path = self.path_builder.instance_field_path(id, 1); // FIELD_STATUS
        let seq_path = self.path_builder.instance_field_path(id, 2); // FIELD_SEQ
        let propose_ballot_path = self.path_builder.instance_field_path(id, 3); // FIELD_propose_ballot
        let others_path = self.path_builder.instance_field_path(id, 5); // FIELD_OTHERS

        self.write_file(&status_path, &serialize(&ins.status)?)
            .await?;
        self.write_file(&seq_path, &serialize(&ins.seq)?).await?;
        self.write_file(&propose_ballot_path, &serialize(&ins.propose_ballot)?)
            .await?;

        if needs_save_cmd {
            let cmd_path = self.path_builder.instance_field_path(id, 4); // FIELD_CMD
            self.write_file(&cmd_path, &serialize(&ins.cmd)?).await?;
        }

        let others_val = (&ins.deps, ins.accepted_ballot, &ins.acc);
        self.write_file(&others_path, &serialize(&others_val)?)
            .await?;

        Ok(())
    }

    async fn load(self: &Arc<Self>, id: InstanceId) -> Result<Option<Instance<C>>> {
        let status_path = self.path_builder.instance_field_path(id, 1); // FIELD_STATUS

        if !self.file_exists(&status_path).await? {
            return Ok(None);
        }

        let status: Status = self.read_file(&status_path).await?;

        let seq: Seq = self
            .read_file(&self.path_builder.instance_field_path(id, 2))
            .await?;
        let propose_ballot: Ballot = self
            .read_file(&self.path_builder.instance_field_path(id, 3))
            .await?;

        let cmd_path = self.path_builder.instance_field_path(id, 4); // FIELD_CMD
        let cmd = if self.file_exists(&cmd_path).await? {
            self.read_file(&cmd_path).await?
        } else {
            // 如果 Partial 模式下 cmd 可能不存在，根据业务逻辑决定
            // 这里假设必须有 cmd，如果不存在则返回 None
            return Ok(None);
        };

        let (deps, accepted_ballot, acc): (Deps, Ballot, Acc) = self
            .read_file(&self.path_builder.instance_field_path(id, 5))
            .await?;

        Ok(Some(Instance {
            propose_ballot,
            cmd,
            seq,
            deps,
            accepted_ballot,
            status,
            acc,
        }))
    }

    async fn save_propose_ballot(
        self: &Arc<Self>,
        id: InstanceId,
        propose_ballot: Ballot,
    ) -> Result<()> {
        let path = self.path_builder.instance_field_path(id, 3); // FIELD_propose_ballot
        self.write_file(&path, &serialize(&propose_ballot)?).await
    }

    async fn load_propose_ballot(self: &Arc<Self>, id: InstanceId) -> Result<Option<Ballot>> {
        let path = self.path_builder.instance_field_path(id, 3); // FIELD_propose_ballot
        if !self.file_exists(&path).await? {
            return Ok(None);
        }
        let propose_ballot: Ballot = self.read_file(&path).await?;
        Ok(Some(propose_ballot))
    }

    async fn save_bounds(
        self: &Arc<Self>,
        attr_bounds: AttrBounds,
        status_bounds: SavedStatusBounds,
    ) -> Result<()> {
        let attr_path = self.path_builder.global_field_path(6); // FIELD_ATTR_BOUNDS
        let status_path = self.path_builder.global_field_path(7); // FIELD_STATUS_BOUNDS

        self.write_file(&attr_path, &serialize(&attr_bounds)?)
            .await?;
        self.write_file(&status_path, &serialize(&status_bounds)?)
            .await?;

        Ok(())
    }

    async fn load_bounds(self: &Arc<Self>) -> Result<(AttrBounds, StatusBounds)> {
        // 读取 attr_bounds 和 saved_status_bounds
        let attr_path = self.path_builder.global_field_path(6); // FIELD_ATTR_BOUNDS
        let status_path = self.path_builder.global_field_path(7); // FIELD_STATUS_BOUNDS

        let mut attr_bounds = if self.file_exists(&attr_path).await? {
            self.read_file(&attr_path).await?
        } else {
            AttrBounds {
                max_seq: Seq::ZERO,
                max_local_instance_ids: VecMap::new(),
            }
        };

        let saved_status_bounds = if self.file_exists(&status_path).await? {
            self.read_file(&status_path).await?
        } else {
            SavedStatusBounds::default()
        };

        // 从 saved_status_bounds 中构建 StatusBounds
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

        // 遍历所有实例状态文件，更新 attr_bounds 和 status_bounds
        let status_files = self.list_instance_status_paths().await?;
        for status_path in status_files {
            // 读取 status 文件内容
            let status: Status = self.read_file(&status_path).await?;

            // 构造相应的 seq 文件路径
            // 假设路径格式为 "{prefix}/instances/{replica_id}/{local_instance_id}/status"
            // 则 seq 文件路径为 "{prefix}/instances/{replica_id}/{local_instance_id}/seq"
            let parts: Vec<&str> = status_path.split('/').collect();
            // parts = ["{prefix}", "instances", "{replica_id}", "{local_instance_id}", "status"]
            if parts.len() != 5 {
                // 路径格式不正确，跳过
                continue;
            }
            let rid_str = parts[2];
            let lid_str = parts[3];
            let seq_path = format!(
                "{}/instances/{}/{}/seq",
                self.path_builder.prefix, rid_str, lid_str
            );

            // 读取 seq 文件内容
            let seq: Seq = if self.file_exists(&seq_path).await? {
                self.read_file(&seq_path).await?
            } else {
                // seq 文件不存在，跳过
                continue;
            };

            // 更新 attr_bounds
            max_assign(&mut attr_bounds.max_seq, seq);
            let replica_id = ReplicaId::from(u64::from_str_radix(rid_str, 16)?);
            let local_instance_id = LocalInstanceId::from(u64::from_str_radix(lid_str, 16)?);
            attr_bounds
                .max_local_instance_ids
                .entry(replica_id)
                .and_modify(|l| max_assign(l, local_instance_id))
                .or_insert(local_instance_id);

            // 更新 status_bounds
            let instance_id = InstanceId(replica_id, local_instance_id);
            status_bounds.set(instance_id, status);
        }

        // 更新 bounds 的内部状态
        status_bounds.update_bounds();

        debug!(?attr_bounds);

        Ok((attr_bounds, status_bounds))
    }

    async fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> Result<()> {
        let path = self.path_builder.instance_field_path(id, 1); // FIELD_STATUS
        self.write_file(&path, &serialize(&status)?).await
    }
}

#[cfg(test)]
mod log_store_test {
    use std::str::FromStr;
    use std::sync::Arc;

    use anyhow::Result;
    use consensus::Acc;
    use consensus::Deps;
    use consensus::FileIOBuilder;
    use consensus::Instance;
    use consensus::Status;
    use consensus::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Round, Seq};

    use crate::log_store::OpenDalLogStore;
    use consensus::UpdateMode;

    #[tokio::test]
    async fn test_opendal_log_store() -> Result<()> {
        let file_io = FileIOBuilder::new("memory").build()?;

        let opendal_log_store = Arc::new(OpenDalLogStore::new(file_io, "memory", "test_log_store"));

        let instance_id = InstanceId::new(ReplicaId::from(12), LocalInstanceId::from(13));

        let cmd = String::from_str("test").unwrap();
        let instance = Instance {
            propose_ballot: Ballot::new(Round::ONE, ReplicaId::from(10)),
            cmd: cmd,
            seq: Seq::from(1),
            deps: Deps::default(),
            accepted_ballot: Ballot::new(Round::ONE, ReplicaId::from(10)),
            status: Status::PreAccepted,
            acc: Acc::default(),
        };

        opendal_log_store
            .save_action(instance_id, instance.clone(), UpdateMode::Full)
            .await?;

        // 测试 load
        let loaded_instance = opendal_log_store.load_action(instance_id).await?;
        assert!(loaded_instance.is_some());
        assert_eq!(
            loaded_instance.unwrap().accepted_ballot,
            instance.accepted_ballot
        );

        // // 测试 save_propose_ballot
        // opendal_log_store.save_propose_ballot(instance_id, Ballot(2)).await?;

        // // 测试 load_propose_ballot
        // let loaded_propose_ballot = opendal_log_store.load_propose_ballot(instance_id).await?;
        // assert!(loaded_propose_ballot.is_some());
        // assert_eq!(loaded_propose_ballot.unwrap(), Ballot(2));

        // // 测试 update_status
        // opendal_log_store
        //     .update_status(instance_id, Status::Committed)
        //     .await?;
        // let updated_instance = opendal_log_store.load(instance_id).await?.unwrap();
        // assert_eq!(updated_instance.status, Status::Committed);

        // // 测试 save_bounds
        // let attr_bounds = AttrBounds::default();
        // let status_bounds = SavedStatusBounds::default();
        // opendal_log_store
        //     .save_bounds(attr_bounds.clone(), status_bounds.clone())
        //     .await?;

        // // 测试 load_bounds
        // let (loaded_attr_bounds, loaded_status_bounds) = opendal_log_store.load_bounds().await?;
        // assert_eq!(loaded_attr_bounds, attr_bounds);
        // assert_eq!(loaded_status_bounds, status_bounds);

        Ok(())
    }
}
