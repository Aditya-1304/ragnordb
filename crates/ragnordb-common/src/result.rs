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

/// Standard result type used throughout RagnorDB
pub type Result<T> = std::result::Result<T, Error>;
