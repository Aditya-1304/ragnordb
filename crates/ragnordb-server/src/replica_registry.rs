//! Durable local authority for tablet-replica lifetimes.
//!
//! Metadata describes where a tablet replica should run, while this registry
//! records the local work that has actually begun.  The registry is therefore
//! intentionally node-local and keyed by `(raft_group_id, replica_id)`:
//! `ReplicaId` is a lifetime scoped to one Raft group and must never be
//! confused with a physical node identity.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Write},
    path::{Path, PathBuf},
};

use ragnordb_common::{
    Error, Result,
    ids::{RaftGroupId, ReplicaId, TableId, TabletId},
};
use serde::{Deserialize, Serialize};

/// Current on-disk registry format.
pub const REPLICA_REGISTRY_VERSION: u32 = 1;

/// Durable point in a Raft replica's state.
///
/// A zero frontier is represented by `None` at the registry API.  Keeping the
/// pair together prevents callers from recording an index without the term
/// needed to validate a recovered snapshot or applied boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableFrontier {
    pub index: u64,
    pub term: u64,
}

impl DurableFrontier {
    /// Construct an exact `(index, term)` durability boundary.
    pub const fn new(index: u64, term: u64) -> Self {
        Self { index, term }
    }

    fn validate(self, field: &str) -> Result<()> {
        if (self.index == 0) != (self.term == 0) {
            return Err(Error::CorruptData(format!(
                "{field} frontier must contain either index=0, term=0 or two non-zero values"
            )));
        }
        Ok(())
    }
}

/// Durable lifecycle state for one local replica lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaLifecycle {
    Creating,
    Active,
    Destroying,
    Tombstoned,
}

/// Stable key for one local replica lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalReplicaKey {
    pub raft_group_id: RaftGroupId,
    pub replica_id: ReplicaId,
}

/// One registry record.  Identity and tablet epoch are immutable after the
/// record is first persisted; lifecycle and frontiers advance independently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalReplicaRecord {
    pub raft_group_id: RaftGroupId,
    pub replica_id: ReplicaId,
    pub tablet_id: TabletId,
    pub table_id: TableId,
    pub tablet_epoch: u64,
    pub lifecycle: ReplicaLifecycle,
    pub snapshot_frontier: Option<DurableFrontier>,
    pub apply_frontier: Option<DurableFrontier>,
}

impl LocalReplicaRecord {
    /// Construct a new record before local creation work begins.
    pub fn new(
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
        tablet_id: TabletId,
        table_id: TableId,
        tablet_epoch: u64,
        lifecycle: ReplicaLifecycle,
    ) -> Self {
        Self {
            raft_group_id,
            replica_id,
            tablet_id,
            table_id,
            tablet_epoch,
            lifecycle,
            snapshot_frontier: None,
            apply_frontier: None,
        }
    }

    /// Attach the last durable snapshot and applied boundaries to a record.
    pub fn with_frontiers(
        mut self,
        snapshot_frontier: Option<DurableFrontier>,
        apply_frontier: Option<DurableFrontier>,
    ) -> Self {
        self.snapshot_frontier = snapshot_frontier;
        self.apply_frontier = apply_frontier;
        self
    }

    /// Return the stable key used for registry lookup and ordering.
    pub const fn key(&self) -> LocalReplicaKey {
        LocalReplicaKey {
            raft_group_id: self.raft_group_id,
            replica_id: self.replica_id,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.raft_group_id.0 == 0 {
            return Err(Error::CorruptData(
                "local replica registry contains reserved Raft group 0".to_string(),
            ));
        }
        if self.replica_id.0 == 0 {
            return Err(Error::CorruptData(
                "local replica registry contains replica ID 0".to_string(),
            ));
        }
        if self.tablet_id.0 == 0 || self.table_id.0 == 0 {
            return Err(Error::CorruptData(
                "local replica registry contains a reserved tablet or table ID".to_string(),
            ));
        }
        if self.tablet_epoch == 0 {
            return Err(Error::CorruptData(
                "local replica registry contains tablet epoch 0".to_string(),
            ));
        }
        if let Some(frontier) = self.snapshot_frontier {
            frontier.validate("snapshot")?;
        }
        if let Some(frontier) = self.apply_frontier {
            frontier.validate("apply")?;
        }
        if let (Some(snapshot), Some(apply)) = (self.snapshot_frontier, self.apply_frontier)
            && frontier_is_ahead(snapshot, apply)
        {
            return Err(Error::CorruptData(
                "snapshot frontier is ahead of apply frontier".to_string(),
            ));
        }
        Ok(())
    }

    fn same_immutable_identity(&self, other: &Self) -> bool {
        self.key() == other.key()
            && self.tablet_id == other.tablet_id
            && self.table_id == other.table_id
            && self.tablet_epoch == other.tablet_epoch
    }
}

/// Result of attempting to create a local registry record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryMutation {
    Created,
    AlreadyPresent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedRegistry {
    format_version: u32,
    cluster_id: String,
    /// A sorted vector gives the JSON image one canonical representation and
    /// avoids relying on implementation-specific map-key serialization.
    replicas: Vec<LocalReplicaRecord>,
}

/// Open registry handle. Mutations are written through a durable replacement
/// image before the in-memory state is changed, so a failed publication cannot
/// make this process observe state that a restart would not recover.
///
/// The registry is a single-owner resource: the server must hold its
/// [`DataDirectoryLock`](crate::data_directory_lock::DataDirectoryLock) and
/// keep one handle for the complete lifecycle-controller lifetime. Concurrent
/// independent handles would race full-image replacements and are unsupported.
#[derive(Debug)]
pub struct LocalReplicaRegistry {
    path: PathBuf,
    state: PersistedRegistry,
    /// OS-held sidecar lock preventing two in-process or cross-process
    /// handles from replacing the same full registry image concurrently.
    _lock: File,
    /// A failed publication may have renamed the new image before directory
    /// sync reported an error.  The handle must be reopened from disk before
    /// it can observe or publish another state transition.
    poisoned: bool,
}

impl LocalReplicaRegistry {
    /// Load an existing registry or open an empty registry for a fresh node.
    pub fn open(path: impl AsRef<Path>, cluster_id: &str) -> Result<Self> {
        if cluster_id.is_empty() {
            return Err(Error::InvalidArgument(
                "local replica registry requires a non-empty cluster ID".to_string(),
            ));
        }

        let path = path.as_ref().to_path_buf();
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)
            .map_err(|error| io_error(parent, "create registry directory", error))?;

        let lock_path = path.with_extension("json.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| io_error(&lock_path, "open registry lock", error))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(Error::Configuration(format!(
                    "local replica registry {} is already open",
                    path.display()
                )));
            }
            Err(TryLockError::Error(error)) => {
                return Err(io_error(&lock_path, "acquire registry lock", error));
            }
        }

        let state = match fs::read(&path) {
            Ok(bytes) => {
                let state: PersistedRegistry =
                    serde_json::from_slice(&bytes).map_err(|source| {
                        Error::CorruptData(format!(
                            "decode local replica registry {}: {source}",
                            path.display()
                        ))
                    })?;
                Self::validate_loaded(&state, cluster_id, &path)?;
                state
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => PersistedRegistry {
                format_version: REPLICA_REGISTRY_VERSION,
                cluster_id: cluster_id.to_string(),
                replicas: Vec::new(),
            },
            Err(error) => return Err(io_error(&path, "read", error)),
        };

        Ok(Self {
            path,
            state,
            _lock: lock,
            poisoned: false,
        })
    }

    /// Return the cluster identity bound to this registry.
    pub fn cluster_id(&self) -> &str {
        &self.state.cluster_id
    }

    /// Return records in canonical `(group, replica)` order.
    pub fn records(&self) -> Result<Vec<LocalReplicaRecord>> {
        self.ensure_healthy()?;
        Ok(self.state.replicas.clone())
    }

    /// Look up one local replica lifetime by its stable identity.
    pub fn record(&self, key: LocalReplicaKey) -> Result<Option<LocalReplicaRecord>> {
        self.ensure_healthy()?;
        Ok(self
            .state
            .replicas
            .binary_search_by_key(&key, LocalReplicaRecord::key)
            .ok()
            .map(|index| self.state.replicas[index].clone()))
    }

    /// Verify that every locally active lifetime has a corresponding shared
    /// WAL recovery identity.  New `Creating` records are allowed to have no
    /// WAL yet because Slice 2 may resume their interrupted bootstrap.
    pub fn validate_recovered_lifetimes(
        &self,
        recovered: impl IntoIterator<Item = LocalReplicaKey>,
    ) -> Result<()> {
        self.ensure_healthy()?;
        let recovered = recovered.into_iter().collect::<BTreeSet<_>>();
        for record in &self.state.replicas {
            if record.lifecycle == ReplicaLifecycle::Active && !recovered.contains(&record.key()) {
                return Err(Error::RecoveryFailed {
                    reason: format!(
                        "active local replica {} of group {} is missing from shared-WAL recovery",
                        record.replica_id.0, record.raft_group_id.0
                    ),
                });
            }
        }
        Ok(())
    }

    /// Add one `Creating` record exactly once. Same immutable identity is
    /// idempotent; a changed tablet identity or epoch is rejected rather than
    /// silently reusing a replica lifetime.
    pub fn ensure_replica(&mut self, record: LocalReplicaRecord) -> Result<RegistryMutation> {
        self.ensure_healthy()?;
        record.validate()?;
        let key = record.key();

        match self
            .state
            .replicas
            .binary_search_by_key(&key, LocalReplicaRecord::key)
        {
            Ok(index) => {
                let existing = &self.state.replicas[index];
                if existing.lifecycle == ReplicaLifecycle::Tombstoned {
                    return Err(Error::InvalidArgument(format!(
                        "local replica {} of group {} is tombstoned and cannot be recreated",
                        record.replica_id.0, record.raft_group_id.0
                    )));
                }
                if !existing.same_immutable_identity(&record) {
                    return Err(Error::InvalidArgument(format!(
                        "local replica {} of group {} conflicts with the existing tablet lifetime",
                        record.replica_id.0, record.raft_group_id.0
                    )));
                }
                Ok(RegistryMutation::AlreadyPresent)
            }
            Err(index) => {
                if record.lifecycle != ReplicaLifecycle::Creating {
                    return Err(Error::InvalidArgument(
                        "a new local replica must first be registered as creating".to_string(),
                    ));
                }
                let mut next = self.state.clone();
                next.replicas.insert(index, record);
                self.persist(&next)?;
                self.state = next;
                Ok(RegistryMutation::Created)
            }
        }
    }

    /// Advance a replica from creation intent to active runtime.  Repeating
    /// the activation acknowledgement is deliberately harmless.
    pub fn mark_active(&mut self, key: LocalReplicaKey) -> Result<()> {
        self.ensure_healthy()?;
        let index = self.index_of(key)?;
        if self.state.replicas[index].lifecycle == ReplicaLifecycle::Active {
            return Ok(());
        }
        if self.state.replicas[index].lifecycle != ReplicaLifecycle::Creating {
            return Err(Error::InvalidArgument(format!(
                "cannot activate local replica {} of group {} from lifecycle {:?}",
                key.replica_id.0, key.raft_group_id.0, self.state.replicas[index].lifecycle
            )));
        }

        let mut next = self.state.clone();
        next.replicas[index].lifecycle = ReplicaLifecycle::Active;
        self.persist(&next)?;
        self.state = next;
        Ok(())
    }

    /// Advance durable snapshot and apply frontiers without allowing either
    /// boundary to regress after a crash/restart checkpoint has been recorded.
    pub fn update_frontiers(
        &mut self,
        key: LocalReplicaKey,
        snapshot_frontier: Option<DurableFrontier>,
        apply_frontier: Option<DurableFrontier>,
    ) -> Result<()> {
        self.ensure_healthy()?;
        if let Some(frontier) = snapshot_frontier {
            frontier.validate("snapshot")?;
        }
        if let Some(frontier) = apply_frontier {
            frontier.validate("apply")?;
        }
        if let (Some(snapshot), Some(apply)) = (snapshot_frontier, apply_frontier)
            && frontier_is_ahead(snapshot, apply)
        {
            return Err(Error::InvalidArgument(
                "snapshot frontier cannot be ahead of apply frontier".to_string(),
            ));
        }

        let index = self.index_of(key)?;
        let existing = &self.state.replicas[index];
        if regresses(existing.snapshot_frontier, snapshot_frontier)
            || regresses(existing.apply_frontier, apply_frontier)
        {
            return Err(Error::InvalidArgument(format!(
                "frontier update for replica {} of group {} regresses durable state",
                key.replica_id.0, key.raft_group_id.0
            )));
        }

        let mut next = self.state.clone();
        next.replicas[index].snapshot_frontier = snapshot_frontier;
        next.replicas[index].apply_frontier = apply_frontier;
        next.replicas[index].validate()?;
        self.persist(&next)?;
        self.state = next;
        Ok(())
    }

    fn index_of(&self, key: LocalReplicaKey) -> Result<usize> {
        self.state
            .replicas
            .binary_search_by_key(&key, LocalReplicaRecord::key)
            .map_err(|_| {
                Error::InvalidArgument(format!(
                    "local replica {} of group {} is not registered",
                    key.replica_id.0, key.raft_group_id.0
                ))
            })
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired {
                reason: format!(
                    "local replica registry {} has an uncertain publication; reopen it before reuse",
                    self.path.display()
                ),
            });
        }
        Ok(())
    }

    fn validate_loaded(state: &PersistedRegistry, cluster_id: &str, path: &Path) -> Result<()> {
        if state.format_version != REPLICA_REGISTRY_VERSION {
            return Err(Error::CorruptData(format!(
                "local replica registry {} has unsupported format version {}",
                path.display(),
                state.format_version
            )));
        }
        if state.cluster_id != cluster_id {
            return Err(Error::Configuration(format!(
                "local replica registry belongs to cluster {}, configured cluster is {}",
                state.cluster_id, cluster_id
            )));
        }

        let mut previous = None;
        for record in &state.replicas {
            record.validate()?;
            if previous.is_some_and(|previous| previous >= record.key()) {
                return Err(Error::CorruptData(format!(
                    "local replica registry {} is not strictly sorted by replica identity",
                    path.display()
                )));
            }
            previous = Some(record.key());
        }
        Ok(())
    }

    fn persist(&mut self, state: &PersistedRegistry) -> Result<()> {
        self.ensure_healthy()?;
        let parent = self.path.parent().ok_or_else(|| Error::RecoveryFailed {
            reason: format!(
                "registry path {} has no parent directory",
                self.path.display()
            ),
        })?;
        if let Err(error) = fs::create_dir_all(parent) {
            self.poisoned = true;
            return Err(io_error(parent, "create registry directory", error));
        }

        let bytes = serde_json::to_vec_pretty(state).map_err(|source| {
            Error::CorruptData(format!(
                "encode local replica registry {}: {source}",
                self.path.display()
            ))
        })?;
        let temporary = self.path.with_extension("json.tmp");
        if temporary.exists()
            && let Err(error) = fs::remove_file(&temporary)
        {
            self.poisoned = true;
            return Err(io_error(
                &temporary,
                "remove stale temporary registry",
                error,
            ));
        }

        let write_result = (|| -> io::Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, &self.path)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();

        if write_result.is_err() {
            self.poisoned = true;
            let _ = fs::remove_file(&temporary);
        }
        write_result.map_err(|error| io_error(&self.path, "durably publish", error))
    }
}

fn regresses(previous: Option<DurableFrontier>, next: Option<DurableFrontier>) -> bool {
    match (previous, next) {
        (Some(previous), Some(next)) => {
            next.index < previous.index
                || next.term < previous.term
                || (next.index == previous.index && next.term != previous.term)
        }
        (Some(_), None) => true,
        _ => false,
    }
}

fn frontier_is_ahead(snapshot: DurableFrontier, apply: DurableFrontier) -> bool {
    snapshot.index > apply.index
        || snapshot.term > apply.term
        || (snapshot.index == apply.index && snapshot.term != apply.term)
}

fn io_error(path: &Path, operation: &str, source: io::Error) -> Error {
    Error::RecoveryFailed {
        reason: format!(
            "{operation} local replica registry {}: {source}",
            path.display()
        ),
    }
}
