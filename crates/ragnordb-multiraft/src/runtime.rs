//! single group Raft Ready persistence boundary
//!
//! this module owns the host-side ordering contract around `RaftNode::ready`.
//! The Raft core remains responsible for consensus state transitions, while
//! this runtime is responsible for durable WAL admission and acknowledgement
//!
//! The required order is:
//!
//! ```text
//! Ready
//!   -> ordered A-WAL append and sync
//!   -> advance_persisted
//!   -> release messages and committed entries
//! ```
//!
//! an uncertain shared-WAL result permanently fences the runtime. The caller
//! must restart and reconstruct the group from the recovered durable prefix

use raft::{
    core::{
        node::{ProposeError, RaftError, RaftNode, StepError},
        ready::{AdvanceError, Ready},
    },
    message::Envelope,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{LogIndex, Snapshot, SnapshotMetadata},
};

use crate::storage::{
    codec::{RaftReplicaIdentity, RaftSnapshotPointerRecord},
    persistence::{RaftPersistenceBatch, RaftPersistenceError, RaftWal, RaftWalStorage},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeState {
    Active,
    GroupQuarantined,
    RecoveryRequired,
}

/// errors raised by the single-group Ready runtime
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

    #[error("incoming snapshot installation is implemented by the next runtime slice")]
    SnapshotInstallRequiresSlice3,

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

/// host side runtime for one Raft group and replica lifetime
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
    /// `persistence` must already be initialized from the same recovered
    /// replica lifetime as `raft`. A restarted node must use
    /// `RaftWalStorage::from_recovered` before entering this runtime
    pub fn new(raft: RaftNode<Vec<u8>, Vec<u8>, LS, SS>, persistence: RaftWalStorage<W>) -> Self {
        Self {
            raft,
            persistence,
            state: RuntimeState::Active,
        }
    }

    /// returns a read only view of the consensus core
    pub fn raft(&self) -> &RaftNode<Vec<u8>, Vec<u8>, LS, SS> {
        &self.raft
    }

    /// returns a read only view of the durable Raft storage owner
    pub fn persistence(&self) -> &RaftWalStorage<W> {
        &self.persistence
    }

    /// advances the Raft clock only when no previous Ready is pending
    pub fn tick(&mut self, ticks: u64) -> Result<(), ReadyLoopError> {
        self.ensure_active()?;
        self.ensure_no_pending_ready()?;

        self.raft.tick_checked(ticks).map_err(ReadyLoopError::Tick)
    }

    /// processes one inbound Raft message only when the previous Ready is
    /// already durable and acknowledged
    pub fn step(&mut self, message: Envelope<Vec<u8>, Vec<u8>>) -> Result<(), ReadyLoopError> {
        self.ensure_active()?;
        self.ensure_no_pending_ready()?;

        self.raft
            .step_checked(message)
            .map_err(ReadyLoopError::Step)
    }

    /// admits one application proposal into the logical Raft overlay
    ///
    /// The proposal is not considered durable until the caller successfully
    /// invokes `persist_next_ready`
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
    /// the caller must provide a snapshot pointer only after the external
    /// snapshot file has been durably written and published. The pointer is
    /// placed before entries, and HardState is placed last by
    /// `RaftWalStorage::persist`
    ///
    /// messages and committed entries are returned only after the exact Ready
    /// identifier has been acknowledged through `advance_persisted`
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

        // snapshot installation requires file verification, restore, and a
        // separate applied acknowledgement. Those responsibilities belong to
        // the next runtime slice and must not be silently skipped here
        if ready.snapshot_install.is_some() {
            return Err(ReadyLoopError::SnapshotInstallRequiresSlice3);
        }

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
                // The exact Ready remains pending in the Raft core and can be
                // retried after the caller handles the transient failure
                return Err(ReadyLoopError::RetryablePersistence(error));
            }
            Err(error) => {
                self.state = RuntimeState::GroupQuarantined;
                return Err(ReadyLoopError::PersistenceRejected(error));
            }
        }

        if let Err(error) = self.raft.advance_persisted(ready.id) {
            // A successful WAL sync followed by a failed Ready acknowledgement
            // leaves the host/core contract inconsistent. Restart is required
            self.state = RuntimeState::RecoveryRequired;
            return Err(ReadyLoopError::Advance(error));
        }

        // Returning the Ready value is safe only after advance_persisted has
        // completed. This is the release boundary for messages and applies
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
