//! single group Raft Ready persistence and application runtime
//!
//! the runtime owns the host-side ordering contract around `RaftNode::ready`.
//! The Raft core remains responsible for consensus transitions, while this
//! module owns durable persistence, snapshot publication, state-machine apply,
//! and quarantine decisions
//!
//! Durable ordering:
//!
//! ```text
//! Ready
//!   -> publish and verify snapshot
//!   -> ordered A-WAL append and sync
//!   -> advance_persisted
//!   -> restore snapshot
//!   -> apply committed entries in order
//!   -> advance_applied
//!   -> release Ready output
//! ```
//!
//! an uncertain WAL result permanently fences the runtime. The caller must
//! restart and reconstruct the group from the recovered durable prefix

use std::{
    fmt::Display,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::PathBuf,
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use raft::{
    core::{
        node::{ProposeError, RaftError, RaftNode, SnapshotInstallError, StepError},
        ready::{AdvanceError, Ready},
    },
    entry::EntryPayload,
    message::Envelope,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{HardState, LogIndex, Snapshot, SnapshotMetadata, Term},
};

use crate::storage::{
    codec::{RAFT_SNAPSHOT_POINTER_RECORD_VERSION, RaftReplicaIdentity, RaftSnapshotPointerRecord},
    persistence::{
        RaftPersistedBatch, RaftPersistenceBatch, RaftPersistenceError, RaftWal, RaftWalStorage,
    },
};

type ReadyGeneration = Ready<Vec<u8>, Vec<u8>>;
type ReadyLoopResult = Result<Option<ReadyGeneration>, ReadyLoopError>;
type ReadyApplyResult = Result<Option<ReadyGeneration>, ReadyApplyError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeState {
    Active,
    GroupQuarantined,
    RecoveryRequired,
}

/// errors raised by the single group Ready runtime
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReadyLoopError {
    #[error("a previous Ready generation is still awaiting persistence")]
    PendingReady,

    #[error("the Raft group requires restart and durable recovery")]
    RecoveryRequired,

    #[error("the Raft group is quarantined")]
    GroupQuarantined,

    #[error("a Ready snapshot requires a previously published snapshot pointer")]
    SnapshotPointerRequired,

    #[error("a snapshot pointer was supplied without a corresponding Ready snapshot")]
    UnexpectedSnapshotPointer,

    #[error("the supplied snapshot boundary is not the durable boundary owned by this Ready loop")]
    SnapshotBoundaryNotPersisted,

    #[error("the durable snapshot boundary did not contain pointer plus HardState")]
    SnapshotPersistenceShape,

    #[error("Raft WAL retention release failed: {0}")]
    Retention(String),

    #[error("incoming snapshot installation failed: {0:?}")]
    SnapshotInstall(SnapshotInstallError),

    #[error("Ready snapshot metadata does not match the published snapshot pointer")]
    SnapshotMetadataMismatch {
        expected: Box<SnapshotMetadata>,
        received: Box<SnapshotMetadata>,
    },

    #[error("Raft WAL persistence can be retried: {0}")]
    RetryablePersistence(RaftPersistenceError),

    #[error("Raft WAL persistence was rejected and the group was quarantined: {0}")]
    PersistenceRejected(RaftPersistenceError),

    #[error("Raft persistence acknowledgement failed: {0:?}")]
    Advance(AdvanceError),

    #[error("Raft tick failed: {0:?}")]
    Tick(RaftError),

    #[error("Raft message processing failed: {0:?}")]
    Step(StepError),

    #[error("Raft proposal failed: {0:?}")]
    Proposal(ProposeError),

    #[error("invalid applied Raft frontier: index {index}, term {term}")]
    InvalidAppliedFrontier { index: LogIndex, term: Term },

    #[error(
        "applied Raft frontier conflicts with the recorded boundary: current={current:?}, attempted={attempted:?}"
    )]
    AppliedFrontierConflict {
        current: AppliedRaftFrontier,
        attempted: AppliedRaftFrontier,
    },
}

/// exact Raft log boundary already applied to the host state machine
///
/// this value is recorded from the committed Ready entries or from a verified
/// snapshot. It is not reconstructed from the current term or commit index,
/// because neither one proves which term belongs to the applied index
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedRaftFrontier {
    pub index: LogIndex,
    pub term: Term,
}

impl AppliedRaftFrontier {
    pub const fn new(index: LogIndex, term: Term) -> Self {
        Self { index, term }
    }
}

/// Host owned snapshot file boundary used by the Ready runtime
///
/// Implementations must publish a complete file before returning its pointer,
/// and `load_verified` must validate the pointer identity, exact length,
/// checksum, and snapshot metadata before returning the image
pub trait RaftSnapshotStore {
    type Error: Display;

    fn publish(
        &mut self,
        identity: RaftReplicaIdentity,
        snapshot: &Snapshot<Vec<u8>>,
    ) -> Result<RaftSnapshotPointerRecord, Self::Error>;

    fn load_verified(
        &mut self,
        pointer: &RaftSnapshotPointerRecord,
    ) -> Result<Snapshot<Vec<u8>>, Self::Error>;
}

/// application state-machine boundary for ordered committed-entry execution
pub trait RaftReadyStateMachine {
    type Error: Display;

    fn restore_snapshot(&mut self, snapshot: &Snapshot<Vec<u8>>) -> Result<(), Self::Error>;

    fn apply(&mut self, index: LogIndex, command: &[u8]) -> Result<(), Self::Error>;
}

/// failure while restoring or applying one already durable Ready generation
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReadyApplyError {
    #[error(transparent)]
    Ready(#[from] ReadyLoopError),

    #[error("snapshot store operation failed: {0}")]
    SnapshotStore(String),

    #[error("verified snapshot metadata does not match the Ready snapshot")]
    SnapshotMetadataMismatch {
        expected: Box<SnapshotMetadata>,
        received: Box<SnapshotMetadata>,
    },

    #[error("state-machine snapshot restore failed: {0}")]
    SnapshotRestore(String),

    #[error("state-machine apply failed at index {index}: {reason}")]
    Application { index: LogIndex, reason: String },

    #[error(
        "committed Ready entries are not contiguous: previous index {previous}, received {received}"
    )]
    ApplyOrder {
        previous: LogIndex,
        received: LogIndex,
    },
}

/// Filesystem-backed snapshot store for one node's Raft snapshot directory.
pub struct FileRaftSnapshotStore {
    root: PathBuf,
}

static NEXT_TEMP_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

impl FileRaftSnapshotStore {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn path_for(&self, file_name: &str) -> PathBuf {
        self.root.join(file_name)
    }

    fn file_name(identity: RaftReplicaIdentity, snapshot_id: u64) -> String {
        format!(
            "raft-{}-{}-{}.snapshot",
            identity.raft_group_id.0, identity.replica_id.0, snapshot_id
        )
    }

    fn temporary_path(&self, file_name: &str) -> PathBuf {
        let sequence = NEXT_TEMP_SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);

        self.root
            .join(format!(".{file_name}.{}.{}.tmp", process::id(), sequence))
    }
}

impl RaftSnapshotStore for FileRaftSnapshotStore {
    type Error = io::Error;

    fn publish(
        &mut self,
        identity: RaftReplicaIdentity,
        snapshot: &Snapshot<Vec<u8>>,
    ) -> Result<RaftSnapshotPointerRecord, Self::Error> {
        let data_length = u64::try_from(snapshot.data.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "snapshot data length overflow")
        })?;

        if snapshot.size_bytes != data_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot size metadata does not match encoded snapshot bytes",
            ));
        }

        if snapshot.checksum != *blake3::hash(&snapshot.data).as_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot checksum does not match encoded snapshot bytes",
            ));
        }

        let pointer = RaftSnapshotPointerRecord {
            format_version: RAFT_SNAPSHOT_POINTER_RECORD_VERSION,
            identity,
            snapshot_id: snapshot.snapshot_id,
            last_included_index: snapshot.last_included_index,
            last_included_term: snapshot.last_included_term,
            applied_index: snapshot.last_included_index,
            conf_state: snapshot.conf_state.clone(),
            size_bytes: snapshot.size_bytes,
            checksum: snapshot.checksum,
            file_name: Self::file_name(identity, snapshot.snapshot_id),
        };

        pointer
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;

        let final_path = self.path_for(&pointer.file_name);

        if final_path.exists() {
            self.load_verified(&pointer)?;
            return Ok(pointer);
        }

        let temporary_path = self.temporary_path(&pointer.file_name);

        let write_result = (|| -> io::Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary_path)?;

            file.write_all(&snapshot.data)?;
            file.sync_all()?;
            drop(file);

            match fs::hard_link(&temporary_path, &final_path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    self.load_verified(&pointer)?;
                }
                Err(error) => return Err(error),
            }

            fs::remove_file(&temporary_path)?;
            File::open(&self.root)?.sync_all()?;

            Ok(())
        })();

        if write_result.is_err() {
            let _ = fs::remove_file(&temporary_path);
        }

        write_result?;

        self.load_verified(&pointer)?;
        Ok(pointer)
    }

    fn load_verified(
        &mut self,
        pointer: &RaftSnapshotPointerRecord,
    ) -> Result<Snapshot<Vec<u8>>, Self::Error> {
        pointer
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;

        let expected_name = Self::file_name(pointer.identity, pointer.snapshot_id);

        if pointer.file_name != expected_name {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot file name is not bound to its Raft identity",
            ));
        }

        let mut file = File::open(self.path_for(&pointer.file_name))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        if data.len() as u64 != pointer.size_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot file length does not match its pointer",
            ));
        }

        if *blake3::hash(&data).as_bytes() != pointer.checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot file checksum does not match its pointer",
            ));
        }

        Ok(Snapshot {
            snapshot_id: pointer.snapshot_id,
            last_included_index: pointer.last_included_index,
            last_included_term: pointer.last_included_term,
            conf_state: pointer.conf_state.clone(),
            size_bytes: pointer.size_bytes,
            checksum: pointer.checksum,
            data,
        })
    }
}

/// Host-side runtime for one Raft group and replica lifetime.
pub struct RaftReadyLoop<W, LS, SS>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    raft: RaftNode<Vec<u8>, Vec<u8>, LS, SS>,
    persistence: RaftWalStorage<W>,
    state: RuntimeState,
    applied_frontier: Option<AppliedRaftFrontier>,
}

impl<W, LS, SS> RaftReadyLoop<W, LS, SS>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    /// creates a runtime around a recovered or newly bootstrapped Raft core
    ///
    /// restarted nodes must initialize the persistence writer with
    /// `RaftWalStorage::from_recovered` before entering this runtime
    pub fn new(raft: RaftNode<Vec<u8>, Vec<u8>, LS, SS>, persistence: RaftWalStorage<W>) -> Self {
        Self {
            raft,
            persistence,
            state: RuntimeState::Active,
            applied_frontier: None,
        }
    }

    pub fn raft(&self) -> &RaftNode<Vec<u8>, Vec<u8>, LS, SS> {
        &self.raft
    }

    pub fn persistence(&self) -> &RaftWalStorage<W> {
        &self.persistence
    }

    /// returns the exact applied boundary observed by this ready loop
    ///
    /// `None` is intentional for a newly created or restarted loop until the
    /// host applies a Ready generation or seeds the recovered frontier. A
    /// caller must not substitute the commit index for this value
    pub fn applied_frontier(&self) -> Option<AppliedRaftFrontier> {
        self.applied_frontier
    }

    /// Release storage retention through the group lifecycle owner.
    pub(crate) fn release_retention(
        &mut self,
        floor: wal::lsn::Lsn,
    ) -> Result<usize, ReadyLoopError> {
        self.ensure_active()?;
        self.persistence.release_retention(floor).map_err(|error| {
            self.quarantine();
            ReadyLoopError::Retention(error)
        })
    }

    /// advances the Raft clock only when no previous Ready is pending
    pub fn tick(&mut self, ticks: u64) -> Result<(), ReadyLoopError> {
        self.ensure_active()?;
        self.ensure_no_pending_ready()?;

        self.raft.tick_checked(ticks).map_err(ReadyLoopError::Tick)
    }

    /// processes one inbound Raft message only after the previous Ready has
    /// been durably acknowledged
    pub fn step(&mut self, message: Envelope<Vec<u8>, Vec<u8>>) -> Result<(), ReadyLoopError> {
        self.ensure_active()?;
        self.ensure_no_pending_ready()?;

        self.raft
            .step_checked(message)
            .map_err(ReadyLoopError::Step)
    }

    /// admits one application proposal into the logical Raft overlay
    pub fn propose(
        &mut self,
        command: Vec<u8>,
        encoded_len: usize,
    ) -> Result<LogIndex, ReadyLoopError> {
        self.ensure_active()?;
        self.ensure_no_pending_ready()?;

        self.raft
            .propose_with_size(command, encoded_len)
            .map_err(ReadyLoopError::Proposal)
    }

    /// persists and acknowledges the next exact Ready generation
    ///
    /// snapshot pointers must reference already-published and synchronized
    /// snapshot files. The persistence layer places the pointer first, entries
    /// next, and HardState last
    pub fn persist_next_ready(
        &mut self,
        snapshot_pointer: Option<RaftSnapshotPointerRecord>,
    ) -> ReadyLoopResult {
        self.ensure_active()?;

        let Some(ready) = self.raft.ready() else {
            if snapshot_pointer.is_some() {
                return Err(ReadyLoopError::UnexpectedSnapshotPointer);
            }

            return Ok(None);
        };

        match (ready.snapshot.as_ref(), snapshot_pointer.as_ref()) {
            (Some(snapshot), Some(pointer)) => {
                validate_snapshot_pointer(pointer, snapshot)?;
            }
            (Some(_), None) => {
                return Err(ReadyLoopError::SnapshotPointerRequired);
            }
            (None, Some(_)) => {
                return Err(ReadyLoopError::UnexpectedSnapshotPointer);
            }
            (None, None) => {}
        }

        let batch = RaftPersistenceBatch {
            snapshot: snapshot_pointer,
            entries: ready.entries_to_persist.clone(),
            hard_state: ready.hard_state.clone(),
        };

        match self.persistence.persist(batch) {
            Ok(_) => {}
            Err(RaftPersistenceError::OutcomeUnknown { .. }) => {
                let report_result = self.raft.report_persistence_outcome_unknown(ready.id);

                self.state = RuntimeState::RecoveryRequired;

                if let Err(error) = report_result {
                    return Err(ReadyLoopError::Advance(error));
                }

                return Err(ReadyLoopError::RecoveryRequired);
            }
            Err(RaftPersistenceError::RecoveryRequired)
            | Err(RaftPersistenceError::NotStaged {
                recovery_required: true,
                ..
            })
            | Err(RaftPersistenceError::PostSyncInvariant(_))
            | Err(RaftPersistenceError::InternalInvariant(_)) => {
                self.state = RuntimeState::RecoveryRequired;
                return Err(ReadyLoopError::RecoveryRequired);
            }
            Err(
                error @ RaftPersistenceError::NotStaged {
                    recovery_required: false,
                    ..
                },
            ) => {
                return Err(ReadyLoopError::RetryablePersistence(error));
            }
            Err(error) => {
                self.state = RuntimeState::GroupQuarantined;
                return Err(ReadyLoopError::PersistenceRejected(error));
            }
        }

        if let Err(error) = self.raft.advance_persisted(ready.id) {
            self.state = RuntimeState::RecoveryRequired;
            return Err(ReadyLoopError::Advance(error));
        }

        Ok(Some(ready))
    }

    /// completes an externally transferred snapshot after its image has been
    /// verified by the caller
    pub fn complete_snapshot_install(
        &mut self,
        snapshot: Snapshot<Vec<u8>>,
    ) -> Result<(), ReadyLoopError> {
        self.ensure_active()?;
        self.ensure_no_pending_ready()?;

        self.raft
            .complete_snapshot_install(snapshot)
            .map_err(|error| {
                self.quarantine();
                ReadyLoopError::SnapshotInstall(error)
            })
    }

    /// persist an externally published tablet snapshot boundary through the
    /// Ready loop's failure state owner
    ///
    /// this operation intentionally permits an incoming snapshot Ready to be
    /// pending: the external image must become durable before the core can
    /// acknowledge that Ready. Every persistence outcome is classified here so
    /// high-level snapshot code cannot leave an uncertain WAL result active
    pub(crate) fn persist_external_snapshot_boundary(
        &mut self,
        pointer: RaftSnapshotPointerRecord,
        hard_state: HardState,
    ) -> Result<RaftPersistedBatch, ReadyLoopError> {
        self.ensure_active()?;

        let retention_floor = self
            .persistence
            .log_view()
            .first_retained_lsn()
            .unwrap_or(wal::lsn::Lsn::ZERO);
        let _retention_pin = self
            .persistence
            .acquire_retention_pin("tablet-snapshot-boundary", retention_floor)
            .map_err(|_| {
                self.quarantine();
                ReadyLoopError::GroupQuarantined
            })?;

        let batch = RaftPersistenceBatch {
            snapshot: Some(pointer),
            entries: Vec::new(),
            hard_state: Some(hard_state),
        };

        let persisted = match self.persistence.persist(batch) {
            Ok(persisted) => persisted,
            Err(RaftPersistenceError::OutcomeUnknown { .. })
            | Err(RaftPersistenceError::RecoveryRequired)
            | Err(RaftPersistenceError::NotStaged {
                recovery_required: true,
                ..
            })
            | Err(RaftPersistenceError::PostSyncInvariant(_))
            | Err(RaftPersistenceError::InternalInvariant(_)) => {
                self.state = RuntimeState::RecoveryRequired;
                return Err(ReadyLoopError::RecoveryRequired);
            }
            Err(
                error @ RaftPersistenceError::NotStaged {
                    recovery_required: false,
                    ..
                },
            ) => return Err(ReadyLoopError::RetryablePersistence(error)),
            Err(error) => {
                self.quarantine();
                return Err(ReadyLoopError::PersistenceRejected(error));
            }
        };

        if persisted.record_count != 2 || persisted.end_lsn.is_none() {
            self.state = RuntimeState::RecoveryRequired;
            return Err(ReadyLoopError::SnapshotPersistenceShape);
        }

        Ok(persisted)
    }

    /// install a locally generated snapshot after its pointer and stable
    /// boundary have already been synchronized through this loop's WAL
    ///
    /// The core snapshot is only made authoritative after the durable pointer
    /// exists. This prevents a leader from advertising a compacted log range
    /// that cannot be reconstructed after restart
    pub(crate) fn restore_persisted_snapshot(
        &mut self,
        pointer: &RaftSnapshotPointerRecord,
        snapshot: Snapshot<Vec<u8>>,
    ) -> Result<(), ReadyLoopError> {
        self.ensure_active()?;
        self.ensure_no_pending_ready()?;

        if self.persistence.snapshot() != Some(pointer) {
            self.quarantine();
            return Err(ReadyLoopError::SnapshotBoundaryNotPersisted);
        }

        if let Err(error) = validate_snapshot_pointer(pointer, &snapshot) {
            self.quarantine();
            return Err(error);
        }

        let frontier =
            AppliedRaftFrontier::new(snapshot.last_included_index, snapshot.last_included_term);
        if frontier.index == 0 || frontier.term == 0 {
            self.quarantine();
            return Err(ReadyLoopError::InvalidAppliedFrontier {
                index: frontier.index,
                term: frontier.term,
            });
        }

        self.raft.restore_snapshot(snapshot);
        self.raft.advance_applied(frontier.index).map_err(|error| {
            self.quarantine();
            ReadyLoopError::Advance(error)
        })?;
        self.applied_frontier = Some(frontier);
        Ok(())
    }

    /// persist the entries and HardState from a Ready whose snapshot pointer
    /// was already durably published by the incoming tablet installer
    ///
    /// incoming tablet installation must restore and publish the database
    /// image before the core acknowledges the external snapshot transfer. The
    /// pointer is therefore intentionally omitted from this second batch, but
    /// all post-snapshot entries and the final HardState still cross the normal
    /// exact A-WAL acknowledgement boundary
    pub(crate) fn persist_ready_after_snapshot_boundary(
        &mut self,
        pointer: &RaftSnapshotPointerRecord,
    ) -> ReadyLoopResult {
        self.ensure_active()?;

        let Some(ready) = self.raft.ready() else {
            return Ok(None);
        };

        let Some(snapshot) = ready.snapshot.as_ref() else {
            return Err(ReadyLoopError::UnexpectedSnapshotPointer);
        };

        if self.persistence.snapshot() != Some(pointer) {
            return Err(ReadyLoopError::SnapshotBoundaryNotPersisted);
        }

        validate_snapshot_pointer(pointer, snapshot)?;

        let batch = RaftPersistenceBatch {
            snapshot: None,
            entries: ready.entries_to_persist.clone(),
            hard_state: ready.hard_state.clone(),
        };

        match self.persistence.persist(batch) {
            Ok(_) => {}
            Err(RaftPersistenceError::OutcomeUnknown { .. }) => {
                let report_result = self.raft.report_persistence_outcome_unknown(ready.id);

                self.state = RuntimeState::RecoveryRequired;

                if let Err(error) = report_result {
                    return Err(ReadyLoopError::Advance(error));
                }

                return Err(ReadyLoopError::RecoveryRequired);
            }
            Err(RaftPersistenceError::RecoveryRequired)
            | Err(RaftPersistenceError::NotStaged {
                recovery_required: true,
                ..
            })
            | Err(RaftPersistenceError::PostSyncInvariant(_))
            | Err(RaftPersistenceError::InternalInvariant(_)) => {
                self.state = RuntimeState::RecoveryRequired;
                return Err(ReadyLoopError::RecoveryRequired);
            }
            Err(
                error @ RaftPersistenceError::NotStaged {
                    recovery_required: false,
                    ..
                },
            ) => return Err(ReadyLoopError::RetryablePersistence(error)),
            Err(error) => {
                self.state = RuntimeState::GroupQuarantined;
                return Err(ReadyLoopError::PersistenceRejected(error));
            }
        }

        self.raft.advance_persisted(ready.id).map_err(|error| {
            self.state = RuntimeState::RecoveryRequired;
            ReadyLoopError::Advance(error)
        })?;

        Ok(Some(ready))
    }

    /// marks the state machine frontier recovered from an already durable
    /// state-machine snapshot before live Ready processing begins
    pub fn advance_applied(&mut self, applied_through: LogIndex) -> Result<(), ReadyLoopError> {
        self.ensure_active()?;

        self.raft
            .advance_applied(applied_through)
            .map_err(ReadyLoopError::Advance)
    }

    /// seeds the applied frontier during restart recovery when the recovery
    /// layer has already restored the corresponding state machine image
    ///
    /// the ordinary live path records this value from Ready entries. Recovery
    /// must provide both index and term explicitly because the core public API
    /// exposes no safe way to infer a historical term from an index
    pub fn advance_applied_frontier(
        &mut self,
        frontier: AppliedRaftFrontier,
    ) -> Result<(), ReadyLoopError> {
        self.ensure_active()?;

        if frontier.index == 0 || frontier.term == 0 {
            return Err(ReadyLoopError::InvalidAppliedFrontier {
                index: frontier.index,
                term: frontier.term,
            });
        }

        if let Some(current) = self.applied_frontier
            && (frontier.index < current.index
                || (frontier.index == current.index && frontier.term != current.term)
                || (frontier.index > current.index && frontier.term < current.term))
        {
            return Err(ReadyLoopError::AppliedFrontierConflict {
                current,
                attempted: frontier,
            });
        }

        self.raft
            .advance_applied(frontier.index)
            .map_err(ReadyLoopError::Advance)?;
        self.applied_frontier = Some(frontier);
        Ok(())
    }

    /// persists the next Ready, restores any verified snapshot, applies
    /// committed commands in order, and acknowledges the applied frontier
    pub fn persist_and_apply_next_ready<SM, SF>(
        &mut self,
        snapshot_store: &mut SF,
        state_machine: &mut SM,
    ) -> ReadyApplyResult
    where
        SM: RaftReadyStateMachine,
        SF: RaftSnapshotStore,
    {
        self.ensure_active().map_err(ReadyApplyError::Ready)?;

        let Some(pending) = self.raft.ready() else {
            return Ok(None);
        };

        let verified_snapshot = if let Some(snapshot) = pending.snapshot.as_ref() {
            let identity = self.persistence.log_view().identity();

            let pointer = match snapshot_store.publish(identity, snapshot) {
                Ok(pointer) => pointer,
                Err(error) => {
                    self.quarantine();
                    return Err(ReadyApplyError::SnapshotStore(error.to_string()));
                }
            };

            let verified = match snapshot_store.load_verified(&pointer) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.quarantine();
                    return Err(ReadyApplyError::SnapshotStore(error.to_string()));
                }
            };

            if verified.metadata() != snapshot.metadata() {
                self.quarantine();
                return Err(ReadyApplyError::SnapshotMetadataMismatch {
                    expected: Box::new(snapshot.metadata()),
                    received: Box::new(verified.metadata()),
                });
            }

            Some((pointer, verified))
        } else {
            None
        };

        let pointer = verified_snapshot
            .as_ref()
            .map(|(pointer, _)| pointer.clone());

        let ready = self
            .persist_next_ready(pointer)
            .map_err(ReadyApplyError::Ready)?
            .expect("the pending Ready was observed immediately before persistence");

        let mut applied_through = self.raft.last_applied();
        let mut applied_frontier = self.applied_frontier;

        if let Some((_, snapshot)) = verified_snapshot {
            if let Err(error) = state_machine.restore_snapshot(&snapshot) {
                self.quarantine();
                return Err(ReadyApplyError::SnapshotRestore(error.to_string()));
            }

            self.raft.restore_snapshot(snapshot);

            applied_through = ready
                .snapshot
                .as_ref()
                .expect("snapshot persistence must retain the Ready snapshot")
                .last_included_index;

            let snapshot = ready
                .snapshot
                .as_ref()
                .expect("snapshot persistence must retain the Ready snapshot");
            applied_frontier = Some(AppliedRaftFrontier::new(
                snapshot.last_included_index,
                snapshot.last_included_term,
            ));
        }

        for entry in &ready.committed_entries {
            let expected_index = applied_through.saturating_add(1);

            if entry.index != expected_index {
                self.quarantine();
                return Err(ReadyApplyError::ApplyOrder {
                    previous: applied_through,
                    received: entry.index,
                });
            }

            if let EntryPayload::Normal(command) = &entry.payload
                && let Err(error) = state_machine.apply(entry.index, command)
            {
                self.quarantine();
                return Err(ReadyApplyError::Application {
                    index: entry.index,
                    reason: error.to_string(),
                });
            }

            applied_through = entry.index;
            applied_frontier = Some(AppliedRaftFrontier::new(entry.index, entry.term));
        }

        if applied_through > self.raft.last_applied()
            && let Err(error) = self.raft.advance_applied(applied_through)
        {
            self.quarantine();
            return Err(ReadyApplyError::Ready(ReadyLoopError::Advance(error)));
        }

        self.applied_frontier = applied_frontier;

        Ok(Some(ready))
    }

    fn ensure_active(&self) -> Result<(), ReadyLoopError> {
        match self.state {
            RuntimeState::Active => Ok(()),
            RuntimeState::GroupQuarantined => Err(ReadyLoopError::GroupQuarantined),
            RuntimeState::RecoveryRequired => Err(ReadyLoopError::RecoveryRequired),
        }
    }

    fn ensure_no_pending_ready(&self) -> Result<(), ReadyLoopError> {
        if self.raft.has_ready() {
            return Err(ReadyLoopError::PendingReady);
        }

        Ok(())
    }

    /// Permanently quarantine this Raft group after a state-machine or runtime
    /// invariant failure. The group must not accept more work in this lifetime.
    pub fn quarantine(&mut self) {
        // Node-wide uncertain durability is strictly stronger than a local
        // quarantine and must never be downgraded by outer error decoration.
        if self.state != RuntimeState::RecoveryRequired {
            self.state = RuntimeState::GroupQuarantined;
        }
    }
}

fn validate_snapshot_pointer(
    pointer: &RaftSnapshotPointerRecord,
    snapshot: &Snapshot<Vec<u8>>,
) -> Result<(), ReadyLoopError> {
    let expected = snapshot.metadata();

    let received = SnapshotMetadata {
        snapshot_id: pointer.snapshot_id,
        last_included_index: pointer.last_included_index,
        last_included_term: pointer.last_included_term,
        conf_state: pointer.conf_state.clone(),
        size_bytes: pointer.size_bytes,
        checksum: pointer.checksum,
    };

    if expected != received {
        return Err(ReadyLoopError::SnapshotMetadataMismatch {
            expected: Box::new(expected),
            received: Box::new(received),
        });
    }

    Ok(())
}
