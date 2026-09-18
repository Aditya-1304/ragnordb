//! Durable records for post-bootstrap replica lifetimes.
//!
//! `RaftGroupBootstrap` is immutable initial authority. This file owns the
//! separate, versioned record used while a later replica is being admitted,
//! caught up, promoted, removed, and finally retired. Keeping the two records
//! separate prevents a placement edit from rewriting the initial membership
//! needed to replay historical WAL entries.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use ragnordb_common::{
    Error, Result,
    ids::{NodeId, RaftGroupId, ReplicaId, TabletId},
};
use serde::{Deserialize, Serialize};

pub const REPLICA_JOIN_REGISTRY_VERSION: u32 = 1;

/// Restart-resumable lifecycle of one post-bootstrap replica lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoiningReplicaLifecycle {
    Creating,
    RouteRegistered,
    AddLearnerCommitted,
    CatchingUp,
    ReadyToPromote,
    Removed,
    Retired,
}

impl JoiningReplicaLifecycle {
    fn rank(self) -> u8 {
        match self {
            Self::Creating => 0,
            Self::RouteRegistered => 1,
            Self::AddLearnerCommitted => 2,
            Self::CatchingUp => 3,
            Self::ReadyToPromote => 4,
            Self::Removed => 5,
            Self::Retired => 6,
        }
    }
}

/// Immutable witness used to initialize a passive joiner before the first
/// committed AddLearner entry reaches it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoiningMembershipWitness {
    pub version: u64,
    pub voters: BTreeSet<ReplicaId>,
    pub learners: BTreeSet<ReplicaId>,
    #[serde(default)]
    pub outgoing_voters: BTreeSet<ReplicaId>,
}

impl JoiningMembershipWitness {
    pub fn from_core(conf_state: &raft::types::ConfState) -> Result<Self> {
        let witness = Self {
            version: conf_state.version,
            voters: conf_state
                .voters
                .iter()
                .map(|id| ReplicaId::from_raft(*id))
                .collect(),
            learners: conf_state
                .learners
                .iter()
                .map(|id| ReplicaId::from_raft(*id))
                .collect(),
            outgoing_voters: conf_state
                .outgoing_voters
                .iter()
                .map(|id| ReplicaId::from_raft(*id))
                .collect(),
        };
        witness.validate()?;
        Ok(witness)
    }

    pub fn to_core(&self) -> Result<raft::types::ConfState> {
        self.validate()?;
        let voters = self
            .voters
            .iter()
            .map(|id| {
                id.to_raft()
                    .map_err(|error| Error::CorruptData(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let learners = self
            .learners
            .iter()
            .map(|id| {
                id.to_raft()
                    .map_err(|error| Error::CorruptData(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let outgoing_voters = self
            .outgoing_voters
            .iter()
            .map(|id| {
                id.to_raft()
                    .map_err(|error| Error::CorruptData(error.to_string()))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let mut state = raft::types::ConfState::new(self.version, voters, learners)
            .map_err(|error| Error::CorruptData(format!("invalid joining witness: {error:?}")))?;
        state.outgoing_voters = outgoing_voters;
        state
            .validate()
            .map_err(|error| Error::CorruptData(format!("invalid joining witness: {error:?}")))?;
        Ok(state)
    }

    fn validate(&self) -> Result<()> {
        if self.version == 0 || self.voters.is_empty() {
            return Err(Error::CorruptData(
                "joining membership witness must contain a version and voter".to_string(),
            ));
        }
        if self.voters.iter().any(|id| id.0 == 0)
            || self.learners.iter().any(|id| id.0 == 0)
            || self.outgoing_voters.iter().any(|id| id.0 == 0)
            || self.voters.iter().any(|id| self.learners.contains(id))
            || self
                .voters
                .iter()
                .any(|id| self.outgoing_voters.contains(id))
            || self
                .learners
                .iter()
                .any(|id| self.outgoing_voters.contains(id))
        {
            return Err(Error::CorruptData(
                "joining membership witness contains overlapping identities".to_string(),
            ));
        }
        Ok(())
    }
}

/// Durable identity and progress record for one dynamically added replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoiningReplicaRecord {
    pub cluster_id: String,
    pub raft_group_id: RaftGroupId,
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
    pub replica_id: ReplicaId,
    pub physical_node_id: NodeId,
    pub expected_current_conf_state_version: u64,
    pub committed_membership_witness: JoiningMembershipWitness,
    pub lifecycle: JoiningReplicaLifecycle,
}

impl JoiningReplicaRecord {
    pub fn key(&self) -> (RaftGroupId, ReplicaId) {
        (self.raft_group_id, self.replica_id)
    }

    fn validate(&self, cluster_id: &str) -> Result<()> {
        if self.cluster_id != cluster_id {
            return Err(Error::Configuration(format!(
                "joining replica record belongs to cluster {}, configured cluster is {}",
                self.cluster_id, cluster_id
            )));
        }
        if self.raft_group_id.0 == 0
            || self.tablet_id.0 == 0
            || self.tablet_epoch == 0
            || self.replica_id.0 == 0
            || self.physical_node_id.0 == 0
            || self.expected_current_conf_state_version == 0
        {
            return Err(Error::CorruptData(
                "joining replica record contains a reserved or zero identity".to_string(),
            ));
        }
        self.committed_membership_witness.validate()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedJoiningRegistry {
    format_version: u32,
    cluster_id: String,
    records: Vec<JoiningReplicaRecord>,
}

/// Single-owner durable store for dynamic join records.
#[derive(Debug)]
pub struct JoiningReplicaRegistry {
    path: PathBuf,
    state: PersistedJoiningRegistry,
    _lock: File,
    poisoned: bool,
}

impl JoiningReplicaRegistry {
    pub fn open(path: impl AsRef<Path>, cluster_id: &str) -> Result<Self> {
        if cluster_id.is_empty() {
            return Err(Error::InvalidArgument(
                "joining replica registry requires a non-empty cluster ID".to_string(),
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
                    "joining replica registry {} is already open",
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
            Err(error) if error.kind() == io::ErrorKind::NotFound => PersistedJoiningRegistry {
                format_version: REPLICA_JOIN_REGISTRY_VERSION,
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

    pub fn records(&self) -> Result<Vec<JoiningReplicaRecord>> {
        self.ensure_healthy()?;
        Ok(self.state.records.clone())
    }

    pub fn record(
        &self,
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    ) -> Result<Option<JoiningReplicaRecord>> {
        self.ensure_healthy()?;
        Ok(self
            .state
            .records
            .binary_search_by_key(&(raft_group_id, replica_id), JoiningReplicaRecord::key)
            .ok()
            .map(|index| self.state.records[index].clone()))
    }

    /// Persist `Creating` before a route or Raft message can be admitted.
    pub fn ensure(&mut self, record: JoiningReplicaRecord) -> Result<()> {
        self.ensure_healthy()?;
        record.validate(&self.state.cluster_id)?;
        match self
            .state
            .records
            .binary_search_by_key(&record.key(), JoiningReplicaRecord::key)
        {
            Ok(index) => {
                let existing = &self.state.records[index];
                if existing != &record
                    && (existing.cluster_id != record.cluster_id
                        || existing.tablet_id != record.tablet_id
                        || existing.tablet_epoch != record.tablet_epoch
                        || existing.physical_node_id != record.physical_node_id
                        || existing.expected_current_conf_state_version
                            != record.expected_current_conf_state_version
                        || existing.committed_membership_witness
                            != record.committed_membership_witness)
                {
                    return Err(Error::InvalidArgument(format!(
                        "joining replica {:?} conflicts with its existing lifetime",
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

    /// Advance the durable lifecycle monotonically. Replaying the same
    /// acknowledgement is safe; skipping a state would hide a crash boundary.
    pub fn advance(
        &mut self,
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
        lifecycle: JoiningReplicaLifecycle,
    ) -> Result<()> {
        self.ensure_healthy()?;
        let key = (raft_group_id, replica_id);
        let index = self
            .state
            .records
            .binary_search_by_key(&key, JoiningReplicaRecord::key)
            .map_err(|_| {
                Error::InvalidArgument(format!("joining replica {:?} is not registered", key))
            })?;
        let current = self.state.records[index].lifecycle;
        if lifecycle.rank() < current.rank() {
            // A delayed acknowledgement may describe an older stage than the
            // one already durable. Treat it as an idempotent no-op; accepting
            // the stale acknowledgement must never rewrite the record or
            // resurrect a Removed/Retired lifetime.
            return Ok(());
        }
        if lifecycle == current {
            return Ok(());
        }
        let mut next = self.state.clone();
        next.records[index].lifecycle = lifecycle;
        self.persist(&next)?;
        self.state = next;
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired {
                reason: format!(
                    "joining replica registry {} has an uncertain publication; reopen it",
                    self.path.display()
                ),
            });
        }
        Ok(())
    }

    fn validate_loaded(
        state: &PersistedJoiningRegistry,
        cluster_id: &str,
        path: &Path,
    ) -> Result<()> {
        if state.format_version != REPLICA_JOIN_REGISTRY_VERSION {
            return Err(Error::CorruptData(format!(
                "joining replica registry {} has unsupported format version {}",
                path.display(),
                state.format_version
            )));
        }
        if state.cluster_id != cluster_id {
            return Err(Error::Configuration(format!(
                "joining replica registry belongs to cluster {}, configured cluster is {}",
                state.cluster_id, cluster_id
            )));
        }
        let mut previous = None;
        for record in &state.records {
            record.validate(cluster_id)?;
            if previous.is_some_and(|previous| previous >= record.key()) {
                return Err(Error::CorruptData(format!(
                    "joining replica registry {} is not strictly sorted",
                    path.display()
                )));
            }
            previous = Some(record.key());
        }
        Ok(())
    }

    fn persist(&mut self, state: &PersistedJoiningRegistry) -> Result<()> {
        self.ensure_healthy()?;
        let parent = self.path.parent().ok_or_else(|| Error::RecoveryFailed {
            reason: format!(
                "joining registry path {} has no parent",
                self.path.display()
            ),
        })?;
        let bytes = serde_json::to_vec_pretty(state)
            .map_err(|error| Error::CorruptData(format!("encode joining registry: {error}")))?;
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
            return Err(io_error(&self.path, "publish joining registry", error));
        }
        Ok(())
    }
}

fn io_error(path: &Path, operation: &str, source: io::Error) -> Error {
    Error::RecoveryFailed {
        reason: format!(
            "{operation} joining replica registry {}: {source}",
            path.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> JoiningReplicaRecord {
        JoiningReplicaRecord {
            cluster_id: "cluster-a".to_string(),
            raft_group_id: RaftGroupId(17),
            tablet_id: TabletId(9),
            tablet_epoch: 4,
            replica_id: ReplicaId(3),
            physical_node_id: NodeId(12),
            expected_current_conf_state_version: 7,
            committed_membership_witness: JoiningMembershipWitness {
                version: 7,
                voters: [ReplicaId(1)].into_iter().collect(),
                learners: BTreeSet::new(),
                outgoing_voters: BTreeSet::new(),
            },
            lifecycle: JoiningReplicaLifecycle::Creating,
        }
    }

    /// Realistic bug caught: a crash after persisting join intent must reopen
    /// with the same lifetime and resume at the last durable lifecycle stage.
    #[test]
    fn joining_lifetime_reopens_and_advances_monotonically() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("joining-registry.json");
        let expected = record();

        let mut registry = JoiningReplicaRegistry::open(&path, "cluster-a").unwrap();
        registry.ensure(expected.clone()).unwrap();
        registry
            .advance(
                RaftGroupId(17),
                ReplicaId(3),
                JoiningReplicaLifecycle::RouteRegistered,
            )
            .unwrap();
        registry
            .advance(
                RaftGroupId(17),
                ReplicaId(3),
                JoiningReplicaLifecycle::AddLearnerCommitted,
            )
            .unwrap();
        registry
            .advance(
                RaftGroupId(17),
                ReplicaId(3),
                JoiningReplicaLifecycle::CatchingUp,
            )
            .unwrap();
        registry
            .advance(
                RaftGroupId(17),
                ReplicaId(3),
                JoiningReplicaLifecycle::ReadyToPromote,
            )
            .unwrap();
        registry
            .advance(
                RaftGroupId(17),
                ReplicaId(3),
                JoiningReplicaLifecycle::Removed,
            )
            .unwrap();
        drop(registry);

        let mut reopened = JoiningReplicaRegistry::open(&path, "cluster-a").unwrap();
        assert_eq!(
            reopened.record(RaftGroupId(17), ReplicaId(3)).unwrap(),
            Some(JoiningReplicaRecord {
                lifecycle: JoiningReplicaLifecycle::Removed,
                ..expected.clone()
            })
        );
        reopened
            .advance(
                RaftGroupId(17),
                ReplicaId(3),
                JoiningReplicaLifecycle::ReadyToPromote,
            )
            .unwrap();
        assert_eq!(
            reopened
                .record(RaftGroupId(17), ReplicaId(3))
                .unwrap()
                .unwrap()
                .lifecycle,
            JoiningReplicaLifecycle::Removed
        );
        reopened
            .advance(
                RaftGroupId(17),
                ReplicaId(3),
                JoiningReplicaLifecycle::Retired,
            )
            .unwrap();
    }

    /// Realistic bug caught: a stale or conflicting admission request must not
    /// silently attach new configuration to an existing replica lifetime.
    #[test]
    fn joining_lifetime_rejects_conflicting_reuse() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("joining-registry.json");
        let expected = record();

        let mut registry = JoiningReplicaRegistry::open(&path, "cluster-a").unwrap();
        registry.ensure(expected.clone()).unwrap();

        let conflicting = JoiningReplicaRecord {
            expected_current_conf_state_version: 8,
            ..expected
        };
        let error = registry.ensure(conflicting).unwrap_err();
        assert!(error.to_string().contains("conflicts"));
    }
}
