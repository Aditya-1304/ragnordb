//! Typed results returned by the local SQL executor
//!
//! These structures represent execution output before it is converted into the
//! client-facing JSON wire response. Keeping the executor result independent
//! from JSON allows future protocol implementations to reuse the same execution
//! layer

use ragnordb_common::{catalog_codec::DataType, codec::Row, ids::TableId};

/// Metadata for one returned result column
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumn {
    /// Client-visible column name
    pub name: String,

    /// Logical SQL type of the returned values
    pub data_type: DataType,

    /// Whether rows in this column may contain SQL NULL
    pub nullable: bool,
}

/// Materialized result of a query statement
///
/// currently it materializes rows because the current client protocol returns one
/// complete response frame. The internal executor still consumes source rows
/// one at a time so a streaming protocol can be introduced later
#[derive(Debug, Clone, PartialEq)]
pub struct ResultSet {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Row>,
}

/// Supported data-mutation operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlOperation {
    Insert,
    Update,
    Delete,
}

/// Result of executing one logical plan
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    /// A table was published in the local catalog and assigned one tablet
    CreatedTable { table_id: TableId },

    /// A DML statement matched or inserted a number of rows
    Mutation {
        operation: DmlOperation,
        affected_rows: usize,
    },

    /// A SELECT or SHOW statement returned rows
    Query(ResultSet),
}
