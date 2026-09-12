//! This file contains shared error types used by RagnorDB's internal layers
//!
//! Error variants preserve semantic information so protocol handlers can map
//! failures to stable client error codes without inspecting human-readable
//! messages
//!  Additional errors will be introduced later alongside
//! the transaction, routing, and Raft milestones

/// Canonical error type shared across RagnorDB crates.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The requested subsystem or operation has not been implemented yet
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),

    /// A caller supplied an invalid non-SQL argument or malformed value
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Stored, recovered, or received internal bytes violate their canonical
    /// encoding contract
    #[error("corrupt data: {0}")]
    CorruptData(String),

    /// A transaction encountered a comitted version, rollback marker, or unresolved
    /// lock that conflicts with its snapshot
    #[error("write conflict: {0}")]
    WriteConflict(String),

    /// The SQL parser could not construct an AST from the client input
    #[error("SQL parse error: {0}")]
    SqlParse(String),

    /// The statement is syntactically valid but outside the supported SQL subset
    #[error("unsupported SQL: {0}")]
    UnsupportedSql(String),

    /// A referenced table, column, or value type does not match the catalog
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    /// A schema or data constraint would be violated
    #[error("constraint violation: {0}")]
    ConstraintViolation(String),

    /// Node or cluster configuration could not be loaded or validated
    #[error("configuration error: {0}")]
    Configuration(String),

    /// statement expired before it acquired the serialized database owner
    ///
    /// once execution begins, RagnorDB waits for the authoritative durability
    /// outcome instead of converting an in flight commit into a cancellation
    #[error("statement admission timed out after {timeout_ms} ms")]
    StatementTimeout { timeout_ms: u64 },

    /// The contacted replica is not the current tablet leader.
    #[error("node is not the tablet leader; current leader is {leader_id:?}")]
    NotLeader { leader_id: Option<u64> },

    /// The route was valid when issued, but the tablet generation changed
    /// before admission. The caller must refresh metadata before retrying.
    #[error("tablet epoch is stale; expected {expected_epoch}, current epoch is {current_epoch}")]
    StaleTabletEpoch {
        current_epoch: u64,
        expected_epoch: u64,
    },

    /// No authoritative leader was published for the requested group.
    #[error("tablet leader is currently unknown")]
    LeaderUnknown,

    /// The group exists in metadata but cannot currently admit work.
    #[error("tablet is temporarily unavailable: {reason}")]
    TabletUnavailable { reason: String },

    /// The server cannot prove whether the logical operation was applied.
    /// Retries are safe only after the retained identity outcome is queried.
    #[error("logical request outcome is unknown: {identity}")]
    RequestOutcomeUnknown { identity: String },

    /// The request identity has fallen outside the retained retry horizon.
    #[error("request identity has expired: {identity}")]
    RequestIdExpired { identity: String },

    /// A session epoch older than the durable client session floor was used.
    #[error("client session epoch has expired: {session_epoch}")]
    ClientSessionExpired { session_epoch: u64 },

    /// A proposal lost leadership or exceeded its deadline before apply.
    #[error("replicated proposal did not reach a known apply result: {reason}")]
    ProposalUnavailable { reason: String },

    /// A-WAL rejected the database record before assigning it a logical extent
    ///
    /// `recovery_required` distinguishes an ordinary admission rejection from a
    /// record that was not staged because the shared writer is sticky-fatal
    #[error("WAL append was definitely not staged: {reason}")]
    WalAppendNotStaged {
        reason: String,

        /// whether the shared WAL writer requires reopen and recovery despite
        /// the user record itself being definitely absent
        recovery_required: bool,
    },

    /// A commit record acquired a WAL extent, but its durable outcome cannot be
    /// determined without reopen and recovery.
    ///
    /// This error is deliberately non-retryable until RagnorDB has durable
    /// request-identity deduplication. Retrying immediately could apply the same
    /// logical transaction twice if recovery retains the original record.
    #[error("commit outcome is unknown for WAL extent [{start_lsn}, {end_lsn}): {reason}")]
    CommitOutcomeUnknown {
        /// First logical WAL position occupied by the staged commit record
        start_lsn: u64,

        /// First logical WAL position after the complete staged commit record
        end_lsn: u64,

        /// Diagnostic description of the durability failure
        reason: String,
    },

    /// the local write path has stopped and cannot safely accept another write
    /// until authorative durable state is replayed
    ///
    /// this includes a durable commit that failed during MVCC application and
    /// coordinator whose preceding commit outcome requires recovery
    #[error("local write path requires recovery: {reason}")]
    RecoveryRequired { reason: String },

    /// catalog record acquired a WAL extent, but recovery is required to
    /// determine whether that metadata operation became durable
    #[error(
        "catalog outcome is unknown for WAL extent \
         [{start_lsn}, {end_lsn}): {reason}"
    )]
    CatalogOutcomeUnknown {
        /// First logical WAL position assigned to the catalog record
        start_lsn: u64,

        /// First logical WAL position after the complete catalog record
        end_lsn: u64,

        /// Diagnostic description of the durability failure
        reason: String,
    },

    /// startup recovery could not read or traverse the physical WAL stream
    ///
    /// semantic payload violations remain `CorruptData`. this variant represents
    /// failures reported by A-WAL itself, including invalid replay boundaries
    /// segment read failures and physical iterator failures
    #[error("WAL recovery failed: {reason}")]
    RecoveryFailed {
        /// diagnostic description of physical recovery failure
        reason: String,
    },

    /// a database snapshot could not be durably published to its final path
    ///
    /// The snapshot has not been referenced by WAL yet. Callers must not append
    /// a `SnapshotPointer` or `CheckpointMarker` after this error
    #[error("snapshot publication failed: {reason}")]
    SnapshotPublicationFailed {
        /// filesystem operation and path that prevented durable publication
        reason: String,
    },

    /// a checkpoint WAL record acquired an extent, but its durable outcome
    /// cannot be determined without reopening and recovering A-WAL
    ///
    /// The live process must not expose a retention-safe checkpoint after this
    /// error. Retrying could duplicate a record that recovery ultimately keeps
    #[error(
        "checkpoint {stage} outcome is unknown for WAL extent \
         [{start_lsn}, {end_lsn}): {reason}"
    )]
    CheckpointOutcomeUnknown {
        /// checkpoint publication stage whose durability became uncertain
        stage: &'static str,

        /// first logical WAL position assigned to the staged record
        start_lsn: u64,

        /// first logical WAL position after the complete staged record
        end_lsn: u64,

        /// diagnostic description of the durability failure
        reason: String,
    },
}

/// Canonical action associated with a client-visible distributed error.
///
/// This is deliberately more precise than the legacy boolean `retryable`: an
/// unknown mutation must be queried with its original identity, never replayed
/// as a fresh transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryAction {
    None,
    RetrySameRequest,
    RestartTransaction,
    QueryOriginalOutcome,
}

impl Error {
    pub const fn retry_action(&self) -> RetryAction {
        match self {
            Self::NotLeader { .. }
            | Self::StaleTabletEpoch { .. }
            | Self::LeaderUnknown
            | Self::TabletUnavailable { .. }
            | Self::ProposalUnavailable { .. }
            | Self::StatementTimeout { .. } => RetryAction::RetrySameRequest,
            Self::RequestOutcomeUnknown { .. }
            | Self::CommitOutcomeUnknown { .. }
            | Self::CatalogOutcomeUnknown { .. }
            | Self::CheckpointOutcomeUnknown { .. } => RetryAction::QueryOriginalOutcome,
            Self::WriteConflict(_) => RetryAction::RestartTransaction,
            Self::RequestIdExpired { .. }
            | Self::ClientSessionExpired { .. }
            | Self::ConstraintViolation(_)
            | Self::InvalidArgument(_)
            | Self::CorruptData(_)
            | Self::NotImplemented(_)
            | Self::SqlParse(_)
            | Self::UnsupportedSql(_)
            | Self::SchemaMismatch(_)
            | Self::Configuration(_)
            | Self::WalAppendNotStaged { .. }
            | Self::RecoveryRequired { .. }
            | Self::RecoveryFailed { .. }
            | Self::SnapshotPublicationFailed { .. } => RetryAction::None,
        }
    }
}

/// Standard result type used throughout RagnorDB
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::{Error, RetryAction};

    #[test]
    fn unknown_mutation_outcome_requires_lookup_not_fresh_retry() {
        assert_eq!(
            Error::RequestOutcomeUnknown {
                identity: "client=1/epoch=2/sequence=3".to_string(),
            }
            .retry_action(),
            RetryAction::QueryOriginalOutcome
        );
        assert_eq!(
            Error::NotLeader { leader_id: None }.retry_action(),
            RetryAction::RetrySameRequest
        );
    }
}
