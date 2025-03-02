use super::bounds::{AttrBounds, SavedStatusBounds, StatusBounds};
use super::cache::LogCache;
use super::cmd::CommandLike;
use super::deps::Deps;
use super::id::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Seq};
use super::instance::Instance;
use super::status::Status;

use crate::storage::LogStore;
use crate::utils::lock::with_mutex;
use crate::{UpdateMode, clone};

use std::ops::Not;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use asc::Asc;
use dashmap::DashMap;
use ordered_vecmap::VecMap;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::OwnedMutexGuard as OwnedAsyncMutexGuard;
use tracing::debug;

pub struct Log<C>
where
    C: CommandLike,
{
    log_store: Arc<dyn LogStore<C>>,
    status_bounds: Asc<SyncMutex<StatusBounds>>,
    cache: AsyncMutex<LogCache<C>>,
    ins_locks: DashMap<InstanceId, Arc<AsyncMutex<()>>>,
}

pub struct InsGuard {
    _guard: OwnedAsyncMutexGuard<()>,
    t1: Instant,
}

impl Drop for InsGuard {
    fn drop(&mut self) {
        debug!(elapsed_us = ?self.t1.elapsed().as_micros(), "unlock instance");
    }
}

impl<C> Log<C>
where
    C: CommandLike,
{
    #[must_use]
    pub fn new(
        log_store: Arc<dyn LogStore<C>>,
        attr_bounds: AttrBounds,
        status_bounds: Asc<SyncMutex<StatusBounds>>,
    ) -> Self {
        let cache = AsyncMutex::new(LogCache::new(attr_bounds));
        let ins_locks = DashMap::new();
        Self {
            log_store,
            status_bounds,
            cache,
            ins_locks,
        }
    }

    pub async fn lock_instance(&self, id: InstanceId) -> InsGuard {
        let mutex = self
            .ins_locks
            .entry(id)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        debug!("start to lock instance {:?}", id);
        let t0 = Instant::now();
        let guard = mutex.lock_owned().await;
        debug!(elapsed_us = ?t0.elapsed().as_micros(), "locked instance");
        let t1 = Instant::now();
        InsGuard { _guard: guard, t1 }
    }

    pub async fn calc_and_update_attributes(
        &self,
        id: InstanceId,
        keys: C::Keys,
        prev_seq: Seq,
        prev_deps: &Deps,
    ) -> (Seq, Deps) {
        let mut guard = self.cache.lock().await;
        let cache = &mut *guard;

        let (seq, mut deps) = cache.calc_attributes(id, &keys);
        let seq = seq.max(prev_seq);
        deps.merge(prev_deps.as_ref());
        let deps = Deps::from_mutable(deps);

        let needs_update_attrs = if let Some(saved) = cache.get_instance(id) {
            saved.seq != seq || saved.deps != deps
        } else {
            true
        };

        if needs_update_attrs {
            let t0 = Instant::now();
            cache.update_attrs(id, keys, seq);
            debug!(elapsed_us = ?t0.elapsed().as_micros(), "updated attrs id: {:?}", id);
        }

        (seq, deps)
    }

    pub async fn update_attrs(&self, id: InstanceId, keys: C::Keys, seq: Seq) {
        let mut guard = self.cache.lock().await;
        let cache = &mut *guard;
        cache.update_attrs(id, keys, seq)
    }

    pub async fn clear_key_map(&self) -> impl Send + Sync + 'static {
        let mut guard = self.cache.lock().await;
        let cache = &mut *guard;
        cache.clear_key_map()
    }

    async fn with<R>(&self, f: impl FnOnce(&mut LogCache<C>) -> R) -> R {
        let mut guard = self.cache.lock().await;
        let cache = &mut *guard;
        f(&mut *cache)
    }

    pub async fn save(
        &self,
        id: InstanceId,
        ins: Instance<C>,
        mode: UpdateMode,
        needs_update_attrs: Option<bool>,
    ) -> Result<()> {
        {
            clone!(ins);
            let t0 = Instant::now();
            self.log_store.save(id, ins, mode).await?;
            debug!(elapsed_us = ?t0.elapsed().as_micros(), "saved instance id: {:?}, mode: {:?}", id, mode);
        }

        let ins_status = ins.status;

        self.with(|cache| {
            let needs_update_attrs = needs_update_attrs.unwrap_or_else(|| {
                if let Some(saved) = cache.get_instance(id) {
                    saved.seq != ins.seq || saved.deps != ins.deps
                } else {
                    true
                }
            });

            if needs_update_attrs {
                let t0 = Instant::now();
                cache.update_attrs(id, ins.cmd.keys(), ins.seq);
                debug!(elapsed_us = ?t0.elapsed().as_micros(), "updated attrs id: {:?}", id);
            }
            cache.insert_instance(id, ins);
        })
        .await;

        self.status_bounds.lock().set(id, ins_status);

        Ok(())
    }

    pub async fn load(&self, id: InstanceId) -> Result<()> {
        let needs_load = self.with(|cache| cache.contains_instance(id).not()).await;

        if needs_load {
            let t0 = Instant::now();
            let result = self.log_store.load(id).await?;
            debug!(elapsed_us = ?t0.elapsed().as_micros(), "loaded instance id: {:?}", id);

            match result {
                Some(ins) => {
                    self.status_bounds.lock().set(id, ins.status);
                    self.with(|cache| cache.insert_instance(id, ins)).await;
                }
                None => {
                    let needs_load_propose_ballot = self
                        .with(|cache| cache.contains_orphan_propose_ballot(id).not())
                        .await;
                    if needs_load_propose_ballot {
                        if let Some(propose_ballot) = self.log_store.load_propose_ballot(id).await?
                        {
                            self.with(|cache| {
                                cache.insert_orphan_propose_ballot(id, propose_ballot)
                            })
                            .await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn save_propose_ballot(&self, id: InstanceId, propose_ballot: Ballot) -> Result<()> {
        self.log_store
            .save_propose_ballot(id, propose_ballot)
            .await?;

        self.with(|cache| match cache.get_mut_instance(id) {
            Some(ins) => ins.propose_ballot = propose_ballot,
            None => cache.insert_orphan_propose_ballot(id, propose_ballot),
        })
        .await;

        Ok(())
    }

    pub async fn update_status(&self, id: InstanceId, status: Status) -> Result<()> {
        self.log_store.update_status(id, status).await?;
        self.with(|cache| {
            if let Some(ins) = cache.get_mut_instance(id) {
                if ins.status >= Status::Committed && status < Status::Committed {
                    debug!(?id, ins_status=?ins.status, new_status=?status, "consistency incorrect");
                    panic!("consistency incorrect")
                }
                ins.status = status;
            }
        })
        .await;
        self.status_bounds.lock().set(id, status);
        Ok(())
    }

    pub async fn get_cached_propose_ballot(&self, id: InstanceId) -> Option<Ballot> {
        self.with(|cache| cache.get_propose_ballot(id)).await
    }

    pub async fn with_cached_instance<R>(
        &self,
        id: InstanceId,
        f: impl FnOnce(Option<&Instance<C>>) -> R,
    ) -> R {
        self.with(|cache| f(cache.get_instance(id))).await
    }

    pub async fn should_ignore_propose_ballot(
        &self,
        id: InstanceId,
        propose_ballot: Ballot,
    ) -> bool {
        if let Some(saved_propose_ballot) = self.get_cached_propose_ballot(id).await {
            if saved_propose_ballot != propose_ballot {
                return true;
            }
        }
        false
    }

    pub async fn should_ignore_status(
        &self,
        id: InstanceId,
        propose_ballot: Ballot,
        next_status: Status,
    ) -> bool {
        self.with_cached_instance(id, |ins| {
            if let Some(ins) = ins {
                let accepted_ballot = ins.accepted_ballot;
                let status = ins.status;

                if (propose_ballot, next_status) <= (accepted_ballot, status) {
                    return true;
                }
            }
            false
        })
        .await
    }

    pub fn update_bounds(&self) {
        self.status_bounds.lock().update_bounds();
    }

    #[must_use]
    pub fn known_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        self.status_bounds.lock().known_up_to()
    }

    #[must_use]
    pub fn committed_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        self.status_bounds.lock().committed_up_to()
    }

    #[must_use]
    pub fn executed_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        self.status_bounds.lock().executed_up_to()
    }

    #[must_use]
    pub fn saved_status_bounds(&self) -> SavedStatusBounds {
        with_mutex(&self.status_bounds, |status_bounds| {
            status_bounds.update_bounds();
            SavedStatusBounds {
                known_up_to: status_bounds.known_up_to(),
                committed_up_to: status_bounds.committed_up_to(),
                executed_up_to: status_bounds.executed_up_to(),
            }
        })
    }

    pub async fn save_bounds(&self) -> Result<()> {
        let saved_status_bounds = self.saved_status_bounds();
        let attr_bounds = self.with(|cache| cache.calc_attr_bounds()).await;
        self.log_store
            .save_bounds(attr_bounds, saved_status_bounds)
            .await?;
        Ok(())
    }

    pub async fn retire_instance(&self, id: InstanceId) {
        self.with(|cache| cache.remove_instance(id)).await
    }
}
