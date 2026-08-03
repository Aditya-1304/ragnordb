//! deterministic apply boundary for replicated tablet commands
//!
//! raft decides command order, while this module validates that each committed
//! envelope targets this tablet generation before dispatching its payload. The
//! state machine will also own replicated request deduplication in later slices

use ragnordb_common::{
    command_codec::{TabletCommand, TabletCommandEnvelope, TabletCommandEnvelopeError},
    ids::TabletId,
};
use ragnordb_storage::mvcc::{InMemoryMvcc, MvccStorage};

use crate::Tablet;

/// replicated state machine owner for one tablet generation
///
/// wrapping `Tablet` keeps replication metadata out of the existing local
/// execution API. Every command must pass this boundary before a payload is
/// allowed to inspect or mutate MVCC state
#[derive(Debug)]
pub struct TabletStateMachine<S = InMemoryMvcc> {
    tablet: Tablet<S>,
    epoch: u64,
}

impl<S: MvccStorage> TabletStateMachine<S> {
    /// bind a tablet to the non-zero descriptor epoch represented by this
    /// state-machine instance
    pub fn new(tablet: Tablet<S>, epoch: u64) -> Result<Self, TabletCommandApplyError> {
        if epoch == 0 {
            return Err(TabletCommandApplyError::ZeroTabletEpoch);
        }

        Ok(Self { tablet, epoch })
    }

    /// apply one committed command after deterministic target validation
    ///
    /// Target checks intentionally precede payload dispatch. A command routed
    /// with an obsolete descriptor therefore cannot mutate the current tablet
    /// generation, even when the command itself is otherwise valid
    pub fn apply(
        &mut self,
        envelope: TabletCommandEnvelope,
    ) -> Result<TabletCommandApplyResult, TabletCommandApplyError> {
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

        match envelope.command {
            TabletCommand::Noop(_) => Ok(TabletCommandApplyResult::Noop),

            command => Err(TabletCommandApplyError::UnsupportedCommand {
                command: command_name(&command),
            }),
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

    use super::{TabletCommandApplyError, TabletCommandApplyResult, TabletStateMachine};
    use crate::Tablet;

    const LOCAL_TABLET_ID: TabletId = TabletId(41);
    const LOCAL_TABLET_EPOCH: u64 = 7;

    fn state_machine() -> TabletStateMachine {
        let tablet = Tablet::new(LOCAL_TABLET_ID, TableId(9)).unwrap();
        TabletStateMachine::new(tablet, LOCAL_TABLET_EPOCH).unwrap()
    }

    fn noop_envelope(tablet_id: TabletId, expected_epoch: u64) -> TabletCommandEnvelope {
        TabletCommandEnvelope::new(
            RequestId {
                client_id: 0xf5b4_81ab_9b67_4418_ba82_b49c_e371_007d,
                sequence: 1,
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

        // A stale rejection must not poison the state machine. The same logical
        // request remains valid when routed with the current tablet epoch
        let result = state_machine
            .apply(noop_envelope(LOCAL_TABLET_ID, LOCAL_TABLET_EPOCH))
            .unwrap();

        assert_eq!(result, TabletCommandApplyResult::Noop);
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
}
