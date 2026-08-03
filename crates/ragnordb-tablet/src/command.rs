//! deterministic apply boundary for replicated tablet commands
//!
//! raft decides command order, while this module validates that each committed
//! envelope targets this tablet generation before dispatching its payload. The
//! state machine will also own replicated request deduplication in later slices

use std::collections::BTreeMap;

use ragnordb_common::{
    Error,
    codec::{TxnStatus, WriteKind},
    command_codec::{
        CachedTabletCommandResult, ClientDeduplicationSnapshot, CommitCommand, PrewriteCommand,
        ResolveIntentCommand, RollbackCommand, SingleShardCommitCommand, TabletCommand,
        TabletCommandEnvelope, TabletCommandEnvelopeError, TabletStateMachineSnapshot,
        TabletStateMachineSnapshotError,
    },
    encoding::encode_row,
    ids::TabletId,
};
use ragnordb_storage::{
    key::decode_row_key,
    mvcc::{InMemoryMvcc, Mutation, MvccStorage},
};

use crate::Tablet;

/// replicated state machine owner for one tablet generation
///
/// wrapping `Tablet` keeps replication metadata out of the existing local
/// execution API. Every command must pass this boundary before a payload is
/// allowed to inspect or mutate MVCC state
///
/// each instance belomgs to one raft group. its client sequence
/// map is naturally scode to that group and cannot consume sequence
/// applied by a different tablet state machine
#[derive(Debug)]
pub struct TabletStateMachine<S = InMemoryMvcc> {
    tablet: Tablet<S>,
    epoch: u64,
    client_deduplication: BTreeMap<u128, ClientDeduplicationState>,
}

impl<S: MvccStorage> TabletStateMachine<S> {
    /// bind a tablet to the non-zero descriptor epoch represented by this
    /// state-machine instance
    pub fn new(tablet: Tablet<S>, epoch: u64) -> Result<Self, TabletCommandApplyError> {
        if epoch == 0 {
            return Err(TabletCommandApplyError::ZeroTabletEpoch);
        }

        Ok(Self {
            tablet,
            epoch,
            client_deduplication: BTreeMap::new(),
        })
    }

    /// Borrow the tablet state owned by this replicated state machine.
    pub fn tablet(&self) -> &Tablet<S> {
        &self.tablet
    }

    /// nncode replicated command metadata that must accompany a tablet snapshot
    ///
    /// MVCC data is intentionally owned by the surrounding tablet snapshot. This
    /// image contains only tablet-generation and retry-deduplication state
    pub fn encode_snapshot_state(&self) -> Result<Vec<u8>, TabletStateMachineSnapshotError> {
        let clients = self
            .client_deduplication
            .iter()
            .map(|(client_id, state)| {
                (
                    *client_id,
                    ClientDeduplicationSnapshot {
                        last_sequence_applied: state.last_sequence_applied,
                        cached_result: state.cached_result.into(),
                    },
                )
            })
            .collect();

        TabletStateMachineSnapshot::new(self.tablet.id(), self.epoch, clients)?.encode()
    }

    /// restore replicated command metadata before applying any post-snapshot Raft
    /// entry
    pub fn restore_from_snapshot(
        tablet: Tablet<S>,
        bytes: &[u8],
    ) -> Result<Self, TabletStateMachineRestoreError> {
        let snapshot = TabletStateMachineSnapshot::decode(bytes)?;
        let local_tablet_id = tablet.id();

        if snapshot.tablet_id != local_tablet_id {
            return Err(TabletStateMachineRestoreError::TabletIdMismatch {
                local_tablet_id,
                snapshot_tablet_id: snapshot.tablet_id,
            });
        }

        let client_deduplication = snapshot
            .clients
            .into_iter()
            .map(|(client_id, state)| {
                (
                    client_id,
                    ClientDeduplicationState {
                        last_sequence_applied: state.last_sequence_applied,
                        cached_result: state.cached_result.into(),
                    },
                )
            })
            .collect();

        Ok(Self {
            tablet,
            epoch: snapshot.tablet_epoch,
            client_deduplication,
        })
    }

    /// apply one committed command after deterministic target validation
    ///
    /// target validation runs before request deduplication so a command sent to
    /// the wrong tablet generation cannot consume a client sequence. Successful
    /// payload results are cached only after dispatch completes
    pub fn apply(
        &mut self,
        envelope: TabletCommandEnvelope,
    ) -> Result<TabletCommandApplyOutcome, TabletCommandApplyError> {
        envelope.validate()?;

        let local_tablet_id = self.tablet.id();

        if envelope.tablet_id != local_tablet_id {
            return Err(TabletCommandApplyError::TabletIdMismatch {
                local_tablet_id,
                requested_tablet_id: envelope.tablet_id,
            });
        }

        if envelope.expected_epoch != self.epoch {
            return Err(TabletCommandApplyError::TabletEpochMismatch {
                current_epoch: self.epoch,
                expected_epoch: envelope.expected_epoch,
            });
        }

        let client_id = envelope.request_id.client_id;
        let sequence = envelope.request_id.sequence;

        if let Some(deduplication) = self.client_deduplication.get(&client_id) {
            if sequence == deduplication.last_sequence_applied {
                return Ok(TabletCommandApplyOutcome::deduplicated(
                    deduplication.cached_result,
                ));
            }

            if sequence < deduplication.last_sequence_applied {
                return Err(TabletCommandApplyError::StaleRequestSequence {
                    last_sequence_applied: deduplication.last_sequence_applied,
                    received_sequence: sequence,
                });
            }

            let expected_sequence = deduplication
                .last_sequence_applied
                .checked_add(1)
                .ok_or(TabletCommandApplyError::RequestSequenceExhausted { client_id })?;

            if sequence != expected_sequence {
                return Err(TabletCommandApplyError::RequestSequenceGap {
                    last_sequence_applied: deduplication.last_sequence_applied,
                    expected_sequence,
                    received_sequence: sequence,
                });
            }
        } else if sequence != 1 {
            return Err(TabletCommandApplyError::RequestSequenceGap {
                last_sequence_applied: 0,
                expected_sequence: 1,
                received_sequence: sequence,
            });
        }

        let result = match envelope.command {
            TabletCommand::Noop(_) => TabletCommandApplyResult::Noop,
            TabletCommand::SingleShardCommit(command) => self.apply_single_shard_commit(command)?,
            TabletCommand::Prewrite(command) => self.apply_prewrite(command)?,
            TabletCommand::Commit(command) => self.apply_commit(command)?,
            TabletCommand::Rollback(command) => self.apply_rollback(command)?,
            TabletCommand::ResolveIntent(command) => self.apply_resolve_intent(command)?,
            command => {
                return Err(TabletCommandApplyError::UnsupportedCommand {
                    command: command_name(&command),
                });
            }
        };

        self.client_deduplication.insert(
            client_id,
            ClientDeduplicationState {
                last_sequence_applied: sequence,
                cached_result: result,
            },
        );

        Ok(TabletCommandApplyOutcome::applied(result))
    }

    fn apply_single_shard_commit(
        &mut self,
        command: SingleShardCommitCommand,
    ) -> Result<TabletCommandApplyResult, TabletCommandApplyError> {
        if command.writes.is_empty() {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: "single-shard commit requires at least one write".to_string(),
            });
        }

        let mut mutations = BTreeMap::new();

        for write in command.writes {
            self.validate_owned_key(&write.key)?;

            let mutation = mutation_from_command(write.op, write.row)?;

            if mutations.insert(write.key, mutation).is_some() {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "single-shard commit contains a duplicate row key".to_string(),
                });
            }
        }

        self.tablet
            .storage
            .commit_batch(
                command.txn_id,
                command.start_timestamp,
                command.commit_timestamp,
                &mutations,
            )
            .map_err(map_database_error)?;

        Ok(TabletCommandApplyResult::SingleShardCommit)
    }

    fn apply_prewrite(
        &mut self,
        command: PrewriteCommand,
    ) -> Result<TabletCommandApplyResult, TabletCommandApplyError> {
        self.validate_owned_key(&command.key)?;

        let mutation = mutation_from_command(command.op, command.row)?;

        self.tablet
            .storage
            .prewrite(
                command.txn_id,
                command.start_timestamp,
                &command.key,
                &mutation,
                &command.primary_key,
                command.ttl_ms,
            )
            .map_err(map_database_error)?;

        Ok(TabletCommandApplyResult::Prewrite)
    }

    fn validate_owned_key(&self, key: &[u8]) -> Result<(), TabletCommandApplyError> {
        let row_key = decode_row_key(key).map_err(map_database_error)?;

        if row_key.table_id != self.tablet.table_id() {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: format!(
                    "row belongs to table {}, but tablet {} owns table {}",
                    row_key.table_id.0,
                    self.tablet.id().0,
                    self.tablet.table_id().0
                ),
            });
        }

        Ok(())
    }

    fn apply_commit(
        &mut self,
        command: CommitCommand,
    ) -> Result<TabletCommandApplyResult, TabletCommandApplyError> {
        self.validate_owned_key(&command.key)?;

        self.tablet
            .storage
            .commit_intent(
                command.txn_id,
                command.start_timestamp,
                command.commit_timestamp,
                &command.key,
            )
            .map_err(map_database_error)?;

        Ok(TabletCommandApplyResult::Commit)
    }

    fn apply_rollback(
        &mut self,
        command: RollbackCommand,
    ) -> Result<TabletCommandApplyResult, TabletCommandApplyError> {
        self.validate_owned_key(&command.key)?;

        self.tablet
            .storage
            .rollback_intent(command.txn_id, command.start_timestamp, &command.key)
            .map_err(map_database_error)?;

        Ok(TabletCommandApplyResult::Rollback)
    }

    fn apply_resolve_intent(
        &mut self,
        command: ResolveIntentCommand,
    ) -> Result<TabletCommandApplyResult, TabletCommandApplyError> {
        self.validate_owned_key(&command.key)?;

        match (command.resolved_status, command.commit_timestamp) {
            (TxnStatus::Committed, Some(commit_timestamp)) => self
                .tablet
                .storage
                .commit_intent(
                    command.txn_id,
                    command.start_timestamp,
                    commit_timestamp,
                    &command.key,
                )
                .map_err(map_database_error)?,

            (TxnStatus::Aborted, None) => self
                .tablet
                .storage
                .rollback_intent(command.txn_id, command.start_timestamp, &command.key)
                .map_err(map_database_error)?,

            (TxnStatus::Pending, _) => {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "pending transaction cannot be resolved".to_string(),
                });
            }

            (TxnStatus::Committed, None) => {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "committed intent resolution requires commit_timestamp".to_string(),
                });
            }

            (TxnStatus::Aborted, Some(_)) => {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "aborted intent resolution must not contain commit_timestamp"
                        .to_string(),
                });
            }
        }

        Ok(TabletCommandApplyResult::ResolveIntent)
    }
}

fn mutation_from_command(
    op: WriteKind,
    row: Option<ragnordb_common::codec::Row>,
) -> Result<Mutation, TabletCommandApplyError> {
    match (op, row) {
        (WriteKind::Put, Some(row)) => encode_row(&row)
            .map(Mutation::Put)
            .map_err(map_database_error),

        (WriteKind::Delete, None) => Ok(Mutation::Delete),

        (WriteKind::Put, None) => Err(TabletCommandApplyError::InvalidCommand {
            reason: "Put command requires a complete row".to_string(),
        }),

        (WriteKind::Delete, Some(_)) => Err(TabletCommandApplyError::InvalidCommand {
            reason: "Delete command must not contain a row".to_string(),
        }),

        (WriteKind::Rollback, _) => Err(TabletCommandApplyError::InvalidCommand {
            reason: "Rollback is not a valid write mutation payload".to_string(),
        }),
    }
}

fn map_database_error(error: Error) -> TabletCommandApplyError {
    match error {
        Error::InvalidArgument(reason) => TabletCommandApplyError::InvalidCommand { reason },

        Error::WriteConflict(reason) => TabletCommandApplyError::WriteConflict { reason },

        Error::CorruptData(reason) => TabletCommandApplyError::CorruptState { reason },

        error => TabletCommandApplyError::StorageFailure {
            reason: error.to_string(),
        },
    }
}

/// last successfully applied request and result for one client in this Raft
/// group’s sequence namespace
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClientDeduplicationState {
    last_sequence_applied: u64,
    cached_result: TabletCommandApplyResult,
}

/// result and provenance for one state machine apply attempt
///
/// callers return `result` to the client in both cases. The `deduplicated` flag
/// lets proposal tracking and diagnostics distinguish a fresh transition from
/// an exact retry served by replicated state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TabletCommandApplyOutcome {
    pub result: TabletCommandApplyResult,
    pub deduplicated: bool,
}

impl TabletCommandApplyOutcome {
    fn applied(result: TabletCommandApplyResult) -> Self {
        Self {
            result,
            deduplicated: false,
        }
    }

    fn deduplicated(result: TabletCommandApplyResult) -> Self {
        Self {
            result,
            deduplicated: true,
        }
    }
}

/// deterministic result produced by applying a replicated tablet command
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TabletCommandApplyResult {
    ///  no-op passed target validation and intentionally changed no MVCC
    /// state. Later phases use this result for replicated barriers
    Noop,

    /// A complete single-tablet write batch committed atomically.
    SingleShardCommit,

    /// One distributed transaction intent was installed atomically.
    Prewrite,

    /// One distributed intent was committed into a visible MVCC write.
    Commit,

    /// One distributed intent was removed and protected by a rollback marker.
    Rollback,

    /// One intent was resolved according to the durable transaction status.
    ResolveIntent,
}

impl From<TabletCommandApplyResult> for CachedTabletCommandResult {
    fn from(result: TabletCommandApplyResult) -> Self {
        match result {
            TabletCommandApplyResult::Noop => Self::Noop,
            TabletCommandApplyResult::SingleShardCommit => Self::SingleShardCommit,
            TabletCommandApplyResult::Prewrite => Self::Prewrite,
            TabletCommandApplyResult::Commit => Self::Commit,
            TabletCommandApplyResult::Rollback => Self::Rollback,
            TabletCommandApplyResult::ResolveIntent => Self::ResolveIntent,
        }
    }
}

impl From<CachedTabletCommandResult> for TabletCommandApplyResult {
    fn from(result: CachedTabletCommandResult) -> Self {
        match result {
            CachedTabletCommandResult::Noop => Self::Noop,
            CachedTabletCommandResult::SingleShardCommit => Self::SingleShardCommit,
            CachedTabletCommandResult::Prewrite => Self::Prewrite,
            CachedTabletCommandResult::Commit => Self::Commit,
            CachedTabletCommandResult::Rollback => Self::Rollback,
            CachedTabletCommandResult::ResolveIntent => Self::ResolveIntent,
        }
    }
}

/// failure while restoring command metadata into a tablet state machine
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletStateMachineRestoreError {
    #[error(
        "tablet snapshot belongs to tablet {snapshot_tablet_id:?}, but the supplied tablet is {local_tablet_id:?}"
    )]
    TabletIdMismatch {
        local_tablet_id: TabletId,
        snapshot_tablet_id: TabletId,
    },

    #[error("invalid tablet state-machine snapshot: {0}")]
    InvalidSnapshot(#[from] TabletStateMachineSnapshotError),
}

/// deterministic rejection returned by the tablet apply boundary
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletCommandApplyError {
    #[error("tablet state-machine epoch must be non-zero")]
    ZeroTabletEpoch,

    #[error(
        "tablet command targets tablet {requested_tablet_id:?}, but this state machine owns {local_tablet_id:?}"
    )]
    TabletIdMismatch {
        local_tablet_id: TabletId,
        requested_tablet_id: TabletId,
    },

    #[error(
        "tablet command expects epoch {expected_epoch}, but the current tablet epoch is {current_epoch}"
    )]
    TabletEpochMismatch {
        current_epoch: u64,
        expected_epoch: u64,
    },

    #[error(
        "request sequence {received_sequence} is stale; client state has already applied sequence {last_sequence_applied}"
    )]
    StaleRequestSequence {
        last_sequence_applied: u64,
        received_sequence: u64,
    },

    #[error(
        "request sequence gap after {last_sequence_applied}: expected {expected_sequence}, received {received_sequence}"
    )]
    RequestSequenceGap {
        last_sequence_applied: u64,
        expected_sequence: u64,
        received_sequence: u64,
    },

    #[error("request sequence space is exhausted for client {client_id:#034x}")]
    RequestSequenceExhausted { client_id: u128 },

    #[error("invalid tablet command: {reason}")]
    InvalidCommand { reason: String },

    #[error("tablet command encountered a write conflict: {reason}")]
    WriteConflict { reason: String },

    #[error("tablet command detected corrupt MVCC state: {reason}")]
    CorruptState { reason: String },

    #[error("tablet storage could not execute the command: {reason}")]
    StorageFailure { reason: String },

    #[error("tablet command payload {command} is not implemented by this state machine")]
    UnsupportedCommand { command: &'static str },

    #[error("invalid tablet command envelope: {0}")]
    InvalidEnvelope(#[from] TabletCommandEnvelopeError),
}

fn command_name(command: &TabletCommand) -> &'static str {
    match command {
        TabletCommand::Prewrite(_) => "prewrite",
        TabletCommand::Commit(_) => "commit",
        TabletCommand::Rollback(_) => "rollback",
        TabletCommand::SingleShardCommit(_) => "single_shard_commit",
        TabletCommand::ResolveIntent(_) => "resolve_intent",
        TabletCommand::Catalog(_) => "catalog",
        TabletCommand::Noop(_) => "noop",
    }
}

#[cfg(test)]
mod tests {
    use ragnordb_common::{
        Error,
        codec::{Row, TxnStatus, Value, WriteKind},
        command_codec::{
            CommitCommand, NoopCommand, PrewriteCommand, ResolveIntentCommand, RollbackCommand,
            SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope, WriteEntry,
        },
        ids::{RequestId, TableId, TabletId, Timestamp, TxnId},
    };
    use ragnordb_storage::key::{encode_row_key, make_row_key};
    use ragnordb_txn::Transaction;

    use super::{
        TabletCommandApplyError, TabletCommandApplyOutcome, TabletCommandApplyResult,
        TabletStateMachine, TabletStateMachineRestoreError,
    };
    use crate::Tablet;

    const LOCAL_TABLET_ID: TabletId = TabletId(41);
    const LOCAL_TABLET_EPOCH: u64 = 7;

    fn state_machine() -> TabletStateMachine {
        state_machine_for(LOCAL_TABLET_ID)
    }

    fn state_machine_for(tablet_id: TabletId) -> TabletStateMachine {
        let tablet = Tablet::new(tablet_id, TableId(9)).unwrap();
        TabletStateMachine::new(tablet, LOCAL_TABLET_EPOCH).unwrap()
    }

    fn noop_envelope(tablet_id: TabletId, expected_epoch: u64) -> TabletCommandEnvelope {
        noop_envelope_for_sequence(tablet_id, expected_epoch, 1)
    }

    fn noop_envelope_for_sequence(
        tablet_id: TabletId,
        expected_epoch: u64,
        sequence: u64,
    ) -> TabletCommandEnvelope {
        command_envelope_for(
            tablet_id,
            expected_epoch,
            sequence,
            TabletCommand::Noop(NoopCommand),
        )
    }

    fn command_envelope(sequence: u64, command: TabletCommand) -> TabletCommandEnvelope {
        command_envelope_for(LOCAL_TABLET_ID, LOCAL_TABLET_EPOCH, sequence, command)
    }

    fn command_envelope_for(
        tablet_id: TabletId,
        expected_epoch: u64,
        sequence: u64,
        command: TabletCommand,
    ) -> TabletCommandEnvelope {
        TabletCommandEnvelope::new(
            RequestId {
                client_id: 0xf5b4_81ab_9b67_4418_ba82_b49c_e371_007d,
                sequence,
            },
            tablet_id,
            expected_epoch,
            command,
        )
        .unwrap()
    }

    fn test_row(id: i64, name: &str) -> Row {
        Row {
            values: vec![Value::Int(id), Value::Text(name.to_string())],
        }
    }

    #[test]
    fn apply_rejects_stale_tablet_epoch_before_payload_dispatch() {
        let mut state_machine = state_machine();

        let error = state_machine
            .apply(noop_envelope(LOCAL_TABLET_ID, LOCAL_TABLET_EPOCH - 1))
            .unwrap_err();

        assert_eq!(
            error,
            TabletCommandApplyError::TabletEpochMismatch {
                current_epoch: LOCAL_TABLET_EPOCH,
                expected_epoch: LOCAL_TABLET_EPOCH - 1,
            }
        );

        // Rejection must not consume the request sequence. The same request is
        // still fresh when routed using the current tablet epoch.
        let result = state_machine
            .apply(noop_envelope(LOCAL_TABLET_ID, LOCAL_TABLET_EPOCH))
            .unwrap();

        assert_eq!(
            result,
            TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: false,
            }
        );
    }

    #[test]
    fn apply_rejects_command_for_another_tablet() {
        let mut state_machine = state_machine();
        let requested_tablet_id = TabletId(LOCAL_TABLET_ID.0 + 1);

        let error = state_machine
            .apply(noop_envelope(requested_tablet_id, LOCAL_TABLET_EPOCH))
            .unwrap_err();

        assert_eq!(
            error,
            TabletCommandApplyError::TabletIdMismatch {
                local_tablet_id: LOCAL_TABLET_ID,
                requested_tablet_id,
            }
        );
    }

    #[test]
    fn exact_request_retry_returns_cached_result_without_reapplying() {
        let mut state_machine = state_machine();

        let first = state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        let retry = state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        assert_eq!(
            first,
            TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: false,
            }
        );

        assert_eq!(
            retry,
            TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: true,
            }
        );
    }

    #[test]
    fn request_sequence_gap_is_rejected_without_consuming_missing_sequence() {
        let mut state_machine = state_machine();

        state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        let error = state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                3,
            ))
            .unwrap_err();

        assert_eq!(
            error,
            TabletCommandApplyError::RequestSequenceGap {
                last_sequence_applied: 1,
                expected_sequence: 2,
                received_sequence: 3,
            }
        );

        let missing = state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                2,
            ))
            .unwrap();

        assert_eq!(
            missing,
            TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: false,
            }
        );
    }

    #[test]
    fn client_request_sequences_are_scoped_to_each_state_machine() {
        let second_tablet_id = TabletId(LOCAL_TABLET_ID.0 + 1);
        let mut first_group = state_machine_for(LOCAL_TABLET_ID);
        let mut second_group = state_machine_for(second_tablet_id);

        let first_result = first_group
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        let second_result = second_group
            .apply(noop_envelope_for_sequence(
                second_tablet_id,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        assert!(!first_result.deduplicated);
        assert!(!second_result.deduplicated);
    }

    #[test]
    fn snapshot_restore_preserves_cached_request_result() {
        let mut original = state_machine();

        original
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        let snapshot = original.encode_snapshot_state().unwrap();

        let tablet = Tablet::new(LOCAL_TABLET_ID, TableId(9)).unwrap();
        let mut restored = TabletStateMachine::restore_from_snapshot(tablet, &snapshot).unwrap();

        // The last acknowledged request must be served from restored deduplication
        // state instead of being dispatched as a fresh state transition.
        let retry = restored
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        assert_eq!(
            retry,
            TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: true,
            }
        );

        // Restoration must also preserve the next expected sequence.
        let next = restored
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                2,
            ))
            .unwrap();

        assert!(!next.deduplicated);
    }

    #[test]
    fn snapshot_restore_rejects_state_for_another_tablet() {
        let original = state_machine();
        let snapshot = original.encode_snapshot_state().unwrap();

        let other_tablet_id = TabletId(LOCAL_TABLET_ID.0 + 1);
        let other_tablet = Tablet::new(other_tablet_id, TableId(9)).unwrap();

        let error = TabletStateMachine::restore_from_snapshot(other_tablet, &snapshot).unwrap_err();

        assert_eq!(
            error,
            TabletStateMachineRestoreError::TabletIdMismatch {
                local_tablet_id: other_tablet_id,
                snapshot_tablet_id: LOCAL_TABLET_ID,
            }
        );
    }

    #[test]
    fn single_shard_commit_applies_complete_row_batch() {
        let mut state_machine = state_machine();
        let first_key = make_row_key(TableId(9), &[Value::Int(1)]).unwrap();
        let second_key = make_row_key(TableId(9), &[Value::Int(2)]).unwrap();
        let first_row = test_row(1, "Ada");
        let second_row = test_row(2, "Grace");

        let command = SingleShardCommitCommand {
            txn_id: TxnId(11),
            start_timestamp: Timestamp(20),
            commit_timestamp: Timestamp(30),
            writes: vec![
                WriteEntry {
                    key: encode_row_key(&first_key).unwrap(),
                    row: Some(first_row.clone()),
                    op: WriteKind::Put,
                },
                WriteEntry {
                    key: encode_row_key(&second_key).unwrap(),
                    row: Some(second_row.clone()),
                    op: WriteKind::Put,
                },
            ],
        };

        let outcome = state_machine
            .apply(command_envelope(
                1,
                TabletCommand::SingleShardCommit(command),
            ))
            .unwrap();

        assert_eq!(outcome.result, TabletCommandApplyResult::SingleShardCommit);
        assert!(!outcome.deduplicated);

        let reader = Transaction::new(TxnId(99), Timestamp(31)).unwrap();

        assert_eq!(
            state_machine.tablet().get(&reader, &first_key).unwrap(),
            Some(first_row)
        );
        assert_eq!(
            state_machine.tablet().get(&reader, &second_key).unwrap(),
            Some(second_row)
        );
    }

    #[test]
    fn prewrite_atomically_installs_default_value_and_lock() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(7)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();

        let command = PrewriteCommand {
            txn_id: TxnId(12),
            start_timestamp: Timestamp(40),
            key: encoded_key.clone(),
            row: Some(test_row(7, "Lin")),
            primary_key: encoded_key,
            op: WriteKind::Put,
            ttl_ms: 30_000,
        };

        let outcome = state_machine
            .apply(command_envelope(1, TabletCommand::Prewrite(command)))
            .unwrap();

        assert_eq!(outcome.result, TabletCommandApplyResult::Prewrite);
        assert_eq!(state_machine.tablet().stats().default_versions, 1);
        assert_eq!(state_machine.tablet().stats().locks, 1);

        let reader = Transaction::new(TxnId(100), Timestamp(50)).unwrap();

        assert!(matches!(
            state_machine.tablet().get(&reader, &key),
            Err(Error::WriteConflict(_))
        ));
    }

    #[test]
    fn commit_resolves_prewrite_and_is_safe_to_replay() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(8)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();
        let row = test_row(8, "Edsger");

        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(PrewriteCommand {
                    txn_id: TxnId(13),
                    start_timestamp: Timestamp(60),
                    key: encoded_key.clone(),
                    row: Some(row.clone()),
                    primary_key: encoded_key.clone(),
                    op: WriteKind::Put,
                    ttl_ms: 30_000,
                }),
            ))
            .unwrap();

        let commit = CommitCommand {
            txn_id: TxnId(13),
            start_timestamp: Timestamp(60),
            commit_timestamp: Timestamp(70),
            key: encoded_key,
        };

        let outcome = state_machine
            .apply(command_envelope(2, TabletCommand::Commit(commit.clone())))
            .unwrap();

        assert_eq!(outcome.result, TabletCommandApplyResult::Commit);
        assert_eq!(state_machine.tablet().stats().locks, 0);
        assert_eq!(state_machine.tablet().stats().write_records, 1);

        // A semantically identical command with a different request sequence
        // still must not create a duplicate physical MVCC version.
        let replay = state_machine
            .apply(command_envelope(3, TabletCommand::Commit(commit)))
            .unwrap();

        assert_eq!(replay.result, TabletCommandApplyResult::Commit);
        assert_eq!(state_machine.tablet().stats().write_records, 1);

        let reader = Transaction::new(TxnId(101), Timestamp(71)).unwrap();

        assert_eq!(
            state_machine.tablet().get(&reader, &key).unwrap(),
            Some(row)
        );
    }

    #[test]
    fn rollback_removes_intent_and_blocks_a_delayed_prewrite() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(9)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();

        let prewrite = PrewriteCommand {
            txn_id: TxnId(14),
            start_timestamp: Timestamp(80),
            key: encoded_key.clone(),
            row: Some(test_row(9, "Barbara")),
            primary_key: encoded_key.clone(),
            op: WriteKind::Put,
            ttl_ms: 30_000,
        };

        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(prewrite.clone()),
            ))
            .unwrap();

        let outcome = state_machine
            .apply(command_envelope(
                2,
                TabletCommand::Rollback(RollbackCommand {
                    txn_id: TxnId(14),
                    start_timestamp: Timestamp(80),
                    key: encoded_key,
                }),
            ))
            .unwrap();

        assert_eq!(outcome.result, TabletCommandApplyResult::Rollback);
        assert_eq!(state_machine.tablet().stats().default_versions, 0);
        assert_eq!(state_machine.tablet().stats().locks, 0);
        assert_eq!(state_machine.tablet().stats().write_records, 1);

        let error = state_machine
            .apply(command_envelope(3, TabletCommand::Prewrite(prewrite)))
            .unwrap_err();

        assert!(matches!(
            error,
            TabletCommandApplyError::WriteConflict { .. }
        ));
    }

    #[test]
    fn resolve_intent_applies_the_recorded_abort_outcome() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(10)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();

        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(PrewriteCommand {
                    txn_id: TxnId(15),
                    start_timestamp: Timestamp(90),
                    key: encoded_key.clone(),
                    row: Some(test_row(10, "Margaret")),
                    primary_key: encoded_key.clone(),
                    op: WriteKind::Put,
                    ttl_ms: 30_000,
                }),
            ))
            .unwrap();

        let outcome = state_machine
            .apply(command_envelope(
                2,
                TabletCommand::ResolveIntent(ResolveIntentCommand {
                    txn_id: TxnId(15),
                    start_timestamp: Timestamp(90),
                    key: encoded_key,
                    resolved_status: TxnStatus::Aborted,
                    commit_timestamp: None,
                }),
            ))
            .unwrap();

        assert_eq!(outcome.result, TabletCommandApplyResult::ResolveIntent);
        assert_eq!(state_machine.tablet().stats().default_versions, 0);
        assert_eq!(state_machine.tablet().stats().locks, 0);
        assert_eq!(state_machine.tablet().stats().write_records, 1);

        let reader = Transaction::new(TxnId(102), Timestamp(100)).unwrap();
        assert_eq!(state_machine.tablet().get(&reader, &key).unwrap(), None);
    }
}
