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

    /// A-WAL rejected the database record before assigning it a logical extent.
    ///
    /// The logical operation was definitely not staged. The underlying WAL may
    /// still be fail-stopped when rollover metadata or another mutating internal
    /// operation failed before the database record itself was assigned space.
    #[error("WAL append was definitely not staged: {reason}")]
    WalAppendNotStaged { reason: String },

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
}

/// Standard result type used throughout RagnorDB
pub type Result<T> = std::result::Result<T, Error>;
