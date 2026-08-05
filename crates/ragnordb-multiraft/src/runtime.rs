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
    types::{LogIndex, Snapshot, SnapshotMetadata},
};

use crate::storage::{
    codec::{RAFT_SNAPSHOT_POINTER_RECORD_VERSION, RaftReplicaIdentity, RaftSnapshotPointerRecord},
    persistence::{RaftPersistenceBatch, RaftPersistenceError, RaftWal, RaftWalStorage},
};

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

    #[error("incoming snapshot installation failed: {0:?}")]
    SnapshotInstall(SnapshotInstallError),

    #[error("Ready snapshot metadata does not match the published snapshot pointer")]
    SnapshotMetadataMismatch {
        expected: SnapshotMetadata,
        received: SnapshotMetadata,
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
        expected: SnapshotMetadata,
        received: SnapshotMetadata,
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
        }
    }

    pub fn raft(&self) -> &RaftNode<Vec<u8>, Vec<u8>, LS, SS> {
        &self.raft
    }

    pub fn persistence(&self) -> &RaftWalStorage<W> {
        &self.persistence
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
    ) -> Result<Option<Ready<Vec<u8>, Vec<u8>>>, ReadyLoopError> {
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
            .map_err(ReadyLoopError::SnapshotInstall)
    }

    /// marks the state machine frontier recovered from an already durable
    /// state-machine snapshot before live Ready processing begins
    pub fn advance_applied(&mut self, applied_through: LogIndex) -> Result<(), ReadyLoopError> {
        self.ensure_active()?;

        self.raft
            .advance_applied(applied_through)
            .map_err(ReadyLoopError::Advance)
    }

    /// persists the next Ready, restores any verified snapshot, applies
    /// committed commands in order, and acknowledges the applied frontier
    pub fn persist_and_apply_next_ready<SM, SF>(
        &mut self,
        snapshot_store: &mut SF,
        state_machine: &mut SM,
    ) -> Result<Option<Ready<Vec<u8>, Vec<u8>>>, ReadyApplyError>
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
                    expected: snapshot.metadata(),
                    received: verified.metadata(),
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
        }

        if applied_through > self.raft.last_applied()
            && let Err(error) = self.raft.advance_applied(applied_through)
        {
            self.quarantine();
            return Err(ReadyApplyError::Ready(ReadyLoopError::Advance(error)));
        }

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

    fn quarantine(&mut self) {
        self.state = RuntimeState::GroupQuarantined;
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
        return Err(ReadyLoopError::SnapshotMetadataMismatch { expected, received });
    }

    Ok(())
}
