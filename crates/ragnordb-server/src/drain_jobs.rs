//! Durable, node-local checkpoints for node-drain orchestration.
//!
//! Metadata remains the cluster-wide placement authority and Raft `ConfState`
//! remains the membership authority. This registry only remembers the
//! coordinator's stable replacement choice and the last observed workflow
//! stage. It is written before an asynchronous metadata proposal so restart
//! can reproduce the same replacement `(RaftGroupId, ReplicaId)` lifetime.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use ragnordb_common::{
    Error, Result,
    ids::{NodeId, RaftGroupId, ReplicaId, TabletId},
    metadata_codec::DesiredReplicaPlacement,
};
use serde::{Deserialize, Serialize};

pub const DRAIN_JOB_REGISTRY_VERSION: u32 = 1;

/// Restart-resumable stage of one source-replica replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainReplicaJobStage {
    Planned,
    ReplacementDesired,
    SourceRemovalDesired,
    Retired,
    Complete,
}

impl DrainReplicaJobStage {
    fn rank(self) -> u8 {
        match self {
            Self::Planned => 0,
            Self::ReplacementDesired => 1,
            Self::SourceRemovalDesired => 2,
            Self::Retired => 3,
            Self::Complete => 4,
        }
    }
}

/// Durable identity and desired-placement checkpoints for one drain action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainReplicaJobRecord {
    pub cluster_id: String,
    pub node_id: NodeId,
    pub raft_group_id: RaftGroupId,
    pub tablet_id: TabletId,
    pub source_replica_id: ReplicaId,
    pub replacement_node_id: Option<NodeId>,
    pub replacement_replica_id: Option<ReplicaId>,
    /// Transitional desired placement that keeps the source voter while the
    /// replacement learner is added and promoted.
    pub replacement_placement: Option<DesiredReplicaPlacement>,
    /// Placement to publish after the replacement has been promoted. Keeping
    /// this full image makes the final policy deterministic after restart,
    /// including removal of a draining node from leader preferences.
    pub final_placement: DesiredReplicaPlacement,
    pub stage: DrainReplicaJobStage,
}

impl DrainReplicaJobRecord {
    pub fn key(&self) -> (RaftGroupId, ReplicaId) {
        (self.raft_group_id, self.source_replica_id)
    }

    fn validate(&self, cluster_id: &str) -> Result<()> {
        if self.cluster_id != cluster_id {
            return Err(Error::Configuration(format!(
                "drain job belongs to cluster {}, configured cluster is {}",
                self.cluster_id, cluster_id
            )));
        }
        if self.node_id.0 == 0
            || self.raft_group_id.0 == 0
            || self.tablet_id.0 == 0
            || self.source_replica_id.0 == 0
        {
            return Err(Error::CorruptData(
                "drain job contains a reserved or zero identity".to_string(),
            ));
        }
        match (self.replacement_node_id, self.replacement_replica_id) {
            (Some(node_id), Some(replica_id)) if node_id.0 != 0 && replica_id.0 != 0 => {}
            (None, None) => {}
            _ => {
                return Err(Error::CorruptData(
                    "drain job contains only half of a replacement identity".to_string(),
                ));
            }
        }
        if self.replacement_replica_id.is_some() != self.replacement_placement.is_some() {
            return Err(Error::CorruptData(
                "drain job replacement identity and placement disagree".to_string(),
            ));
        }
        if let (Some(replacement_id), Some(replacement_placement)) =
            (self.replacement_replica_id, &self.replacement_placement)
            && !replacement_placement
                .replicas
                .iter()
                .any(|replica| replica.replica_id == replacement_id)
        {
            return Err(Error::CorruptData(
                "drain job replacement placement omits its replacement lifetime".to_string(),
            ));
        }
        self.final_placement.validate().map_err(|error| {
            Error::CorruptData(format!("invalid drain job final placement: {error}"))
        })?;
        if self.final_placement.tablet_id != self.tablet_id
            || self
                .final_placement
                .replicas
                .iter()
                .any(|replica| replica.replica_id == self.source_replica_id)
        {
            return Err(Error::CorruptData(
                "drain job final placement still contains its source lifetime".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedDrainJobs {
    format_version: u32,
    cluster_id: String,
    records: Vec<DrainReplicaJobRecord>,
}

/// Single-owner atomic registry for drain workflow checkpoints.
#[derive(Debug)]
pub struct DrainJobRegistry {
    path: PathBuf,
    state: PersistedDrainJobs,
    _lock: File,
    poisoned: bool,
}

impl DrainJobRegistry {
    pub fn open(path: impl AsRef<Path>, cluster_id: &str) -> Result<Self> {
        if cluster_id.is_empty() {
            return Err(Error::InvalidArgument(
                "drain job registry requires a non-empty cluster ID".to_string(),
            ));
        }
        let path = path.as_ref().to_path_buf();
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| io_error(&path, "create directory", error))?;
        let lock_path = path.with_extension("json.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| io_error(&lock_path, "open lock", error))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(Error::Configuration(format!(
                    "drain job registry {} is already open",
                    path.display()
                )));
            }
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(io_error(&lock_path, "acquire lock", error));
            }
        }

        let state = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| {
                Error::CorruptData(format!("decode {}: {error}", path.display()))
            })?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => PersistedDrainJobs {
                format_version: DRAIN_JOB_REGISTRY_VERSION,
                cluster_id: cluster_id.to_string(),
                records: Vec::new(),
            },
            Err(error) => return Err(io_error(&path, "read", error)),
        };
        Self::validate_loaded(&state, cluster_id, &path)?;
        Ok(Self {
            path,
            state,
            _lock: lock,
            poisoned: false,
        })
    }

    pub fn records(&self) -> Result<Vec<DrainReplicaJobRecord>> {
        self.ensure_healthy()?;
        Ok(self.state.records.clone())
    }

    pub fn record(
        &self,
        raft_group_id: RaftGroupId,
        source_replica_id: ReplicaId,
    ) -> Result<Option<DrainReplicaJobRecord>> {
        self.ensure_healthy()?;
        Ok(self
            .state
            .records
            .binary_search_by_key(
                &(raft_group_id, source_replica_id),
                DrainReplicaJobRecord::key,
            )
            .ok()
            .map(|index| self.state.records[index].clone()))
    }

    /// Persist the selected replacement before publishing any placement edit.
    pub fn ensure(&mut self, record: DrainReplicaJobRecord) -> Result<()> {
        self.ensure_healthy()?;
        record.validate(&self.state.cluster_id)?;
        match self
            .state
            .records
            .binary_search_by_key(&record.key(), DrainReplicaJobRecord::key)
        {
            Ok(index) => {
                let existing = &self.state.records[index];
                if existing.cluster_id != record.cluster_id
                    || existing.node_id != record.node_id
                    || existing.tablet_id != record.tablet_id
                    || existing.replacement_node_id != record.replacement_node_id
                    || existing.replacement_replica_id != record.replacement_replica_id
                    || existing.replacement_placement != record.replacement_placement
                    || existing.final_placement != record.final_placement
                {
                    return Err(Error::InvalidArgument(format!(
                        "drain job {:?} conflicts with its existing durable plan",
                        record.key()
                    )));
                }
                Ok(())
            }
            Err(index) => {
                let mut next = self.state.clone();
                next.records.insert(index, record);
                self.persist(&next)?;
                self.state = next;
                Ok(())
            }
        }
    }

    /// Replace only a not-yet-published plan after a concurrent metadata
    /// placement edit made its original epoch stale.
    pub fn replace_planned(&mut self, record: DrainReplicaJobRecord) -> Result<()> {
        self.ensure_healthy()?;
        record.validate(&self.state.cluster_id)?;
        let index = self.index_of(record.key())?;
        if self.state.records[index].stage != DrainReplicaJobStage::Planned {
            return Err(Error::InvalidArgument(
                "only a planned drain job may be replanned".to_string(),
            ));
        }
        let mut next = self.state.clone();
        next.records[index] = record;
        self.persist(&next)?;
        self.state = next;
        Ok(())
    }

    /// Advance the checkpoint monotonically; delayed acknowledgements cannot
    /// resurrect an already retired source lifetime.
    pub fn advance(
        &mut self,
        key: (RaftGroupId, ReplicaId),
        stage: DrainReplicaJobStage,
    ) -> Result<()> {
        self.ensure_healthy()?;
        let index = self.index_of(key)?;
        let current = self.state.records[index].stage;
        if stage.rank() <= current.rank() {
            return Ok(());
        }
        let mut next = self.state.clone();
        next.records[index].stage = stage;
        self.persist(&next)?;
        self.state = next;
        Ok(())
    }

    fn index_of(&self, key: (RaftGroupId, ReplicaId)) -> Result<usize> {
        self.state
            .records
            .binary_search_by_key(&key, DrainReplicaJobRecord::key)
            .map_err(|_| Error::InvalidArgument(format!("drain job {key:?} is not registered")))
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired {
                reason: format!(
                    "drain job registry {} has an uncertain publication; reopen it",
                    self.path.display()
                ),
            });
        }
        Ok(())
    }

    fn validate_loaded(state: &PersistedDrainJobs, cluster_id: &str, path: &Path) -> Result<()> {
        if state.format_version != DRAIN_JOB_REGISTRY_VERSION {
            return Err(Error::CorruptData(format!(
                "drain job registry {} has unsupported format version {}",
                path.display(),
                state.format_version
            )));
        }
        if state.cluster_id != cluster_id {
            return Err(Error::Configuration(format!(
                "drain job registry belongs to cluster {}, configured cluster is {}",
                state.cluster_id, cluster_id
            )));
        }
        let mut previous = None;
        for record in &state.records {
            record.validate(cluster_id)?;
            if previous.is_some_and(|previous| previous >= record.key()) {
                return Err(Error::CorruptData(format!(
                    "drain job registry {} is not strictly sorted",
                    path.display()
                )));
            }
            previous = Some(record.key());
        }
        Ok(())
    }

    fn persist(&mut self, state: &PersistedDrainJobs) -> Result<()> {
        self.ensure_healthy()?;
        let parent = self.path.parent().ok_or_else(|| Error::RecoveryFailed {
            reason: format!("drain job path {} has no parent", self.path.display()),
        })?;
        let bytes = serde_json::to_vec_pretty(state)
            .map_err(|error| Error::CorruptData(format!("encode drain jobs: {error}")))?;
        let temporary = self.path.with_extension("json.tmp");
        if temporary.exists() {
            fs::remove_file(&temporary).map_err(|error| {
                self.poisoned = true;
                io_error(&temporary, "remove temporary registry", error)
            })?;
        }
        let result = (|| -> io::Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, &self.path)?;
            File::open(parent)?.sync_all()
        })();
        if let Err(error) = result {
            self.poisoned = true;
            let _ = fs::remove_file(&temporary);
            return Err(io_error(&self.path, "publish drain jobs", error));
        }
        Ok(())
    }
}

fn io_error(path: &Path, operation: &str, source: io::Error) -> Error {
    Error::RecoveryFailed {
        reason: format!(
            "{operation} drain job registry {}: {source}",
            path.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::metadata_codec::{DesiredReplica, DesiredReplicaRole, PlacementPolicy};

    fn record() -> DrainReplicaJobRecord {
        DrainReplicaJobRecord {
            cluster_id: "cluster-a".to_string(),
            node_id: NodeId(11),
            raft_group_id: RaftGroupId(23),
            tablet_id: TabletId(17),
            source_replica_id: ReplicaId(31),
            replacement_node_id: Some(NodeId(12)),
            replacement_replica_id: Some(ReplicaId(33)),
            replacement_placement: Some(DesiredReplicaPlacement {
                tablet_id: TabletId(17),
                configuration_epoch: 2,
                placement_policy: PlacementPolicy::for_replica_count(2),
                replicas: vec![
                    DesiredReplica {
                        replica_id: ReplicaId(31),
                        node_id: NodeId(11),
                        role: DesiredReplicaRole::Voter,
                    },
                    DesiredReplica {
                        replica_id: ReplicaId(33),
                        node_id: NodeId(12),
                        role: DesiredReplicaRole::Voter,
                    },
                ],
            }),
            final_placement: DesiredReplicaPlacement {
                tablet_id: TabletId(17),
                configuration_epoch: 3,
                placement_policy: PlacementPolicy::for_replica_count(1),
                replicas: vec![DesiredReplica {
                    replica_id: ReplicaId(33),
                    node_id: NodeId(12),
                    role: DesiredReplicaRole::Voter,
                }],
            },
            stage: DrainReplicaJobStage::Planned,
        }
    }

    /// Realistic bug caught: a crash after the replacement plan is persisted
    /// must reopen the same source/replacement identity and resume its stage.
    #[test]
    fn drain_plan_reopens_and_ignores_stale_stage_acknowledgements() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("drain-jobs.json");
        let expected = record();
        let mut registry = DrainJobRegistry::open(&path, "cluster-a").unwrap();
        registry.ensure(expected.clone()).unwrap();
        registry
            .advance(expected.key(), DrainReplicaJobStage::ReplacementDesired)
            .unwrap();
        drop(registry);

        let mut reopened = DrainJobRegistry::open(&path, "cluster-a").unwrap();
        reopened
            .advance(expected.key(), DrainReplicaJobStage::Planned)
            .unwrap();
        assert_eq!(
            reopened
                .record(expected.raft_group_id, expected.source_replica_id)
                .unwrap()
                .unwrap()
                .stage,
            DrainReplicaJobStage::ReplacementDesired
        );
    }
}
