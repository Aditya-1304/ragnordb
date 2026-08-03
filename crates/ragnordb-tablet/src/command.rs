//! deterministic apply boundary for replicated tablet commands
//!
//! raft decides command order, while this module validates that each committed
//! envelope targets this tablet generation before dispatching its payload. The
//! state machine will also own replicated request deduplication in later slices

use std::collections::BTreeMap;

use ragnordb_common::{
    command_codec::{
        CachedTabletCommandResult, ClientDeduplicationSnapshot, TabletCommand,
        TabletCommandEnvelope, TabletCommandEnvelopeError, TabletStateMachineSnapshot,
        TabletStateMachineSnapshotError,
    },
    ids::TabletId,
};
use ragnordb_storage::mvcc::{InMemoryMvcc, MvccStorage};

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
}

impl From<TabletCommandApplyResult> for CachedTabletCommandResult {
    fn from(result: TabletCommandApplyResult) -> Self {
        match result {
            TabletCommandApplyResult::Noop => Self::Noop,
        }
    }
}

impl From<CachedTabletCommandResult> for TabletCommandApplyResult {
    fn from(result: CachedTabletCommandResult) -> Self {
        match result {
            CachedTabletCommandResult::Noop => Self::Noop,
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
        command_codec::{NoopCommand, TabletCommand, TabletCommandEnvelope},
        ids::{RequestId, TableId, TabletId},
    };

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
        TabletCommandEnvelope::new(
            RequestId {
                client_id: 0xf5b4_81ab_9b67_4418_ba82_b49c_e371_007d,
                sequence,
            },
            tablet_id,
            expected_epoch,
            TabletCommand::Noop(NoopCommand),
        )
        .unwrap()
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
}
