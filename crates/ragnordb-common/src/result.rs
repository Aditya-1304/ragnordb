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
}

/// Standard result type used throughout RagnorDB
pub type Result<T> = std::result::Result<T, Error>;
