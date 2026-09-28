use ragnordb_common::command_codec::TabletCommandEnvelope;
use ragnordb_storage::{lsm::RecoveryFrontier, mvcc::MvccStorage};
use ragnordb_tablet::command::{
    TabletCommandApplyError, TabletCommandApplyOutcome, TabletStateMachine,
};

/// Lets integration tests exercise replicated commands without exposing a
/// position-free mutation method from the production tablet API.
pub(crate) trait ApplyCommittedTestCommand {
    fn apply(
        &mut self,
        envelope: TabletCommandEnvelope,
    ) -> Result<TabletCommandApplyOutcome, TabletCommandApplyError>;
}

impl<S: MvccStorage> ApplyCommittedTestCommand for TabletStateMachine<S> {
    fn apply(
        &mut self,
        envelope: TabletCommandEnvelope,
    ) -> Result<TabletCommandApplyOutcome, TabletCommandApplyError> {
        let (index, term) = match self.recovery_frontier() {
            Some(RecoveryFrontier::ReplicatedTablet {
                applied_index,
                applied_term,
                ..
            }) => (applied_index.saturating_add(1), applied_term),
            _ => (1, 1),
        };
        self.apply_committed_at(envelope, index, term)
    }
}
