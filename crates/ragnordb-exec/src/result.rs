//! Typed results returned by the local SQL executor.
//!
//! These structures represent execution output before conversion into the
//! client-facing JSON wire response. Keeping executor results independent from
//! JSON allows future protocol implementations to reuse the execution layer.

use ragnordb_common::{
    Result,
    catalog_codec::DataType,
    codec::Row,
    ids::{TableId, Timestamp, TxnId},
};

/// Metadata for one returned result column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumn {
    /// Client-visible column name.
    pub name: String,

    /// Logical SQL type of returned values.
    pub data_type: DataType,

    /// Whether rows in this column may contain SQL NULL.
    pub nullable: bool,
}

/// Materialized result of a query statement.
///
/// Milestone 2 materializes complete result sets because the current client
/// protocol returns one response frame. Streaming can be introduced later
/// without exposing JSON types inside the executor.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultSet {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Row>,
}

/// Bounded producer interface used by the V2 streaming protocol.
///
/// Implementations must block or reject `push_batch` when their bounded
/// transport queue is full. That backpressure deliberately runs on the SQL
/// blocking worker, stopping additional tablet pages from being requested
/// while the client is slow. `cancelled` lets a disconnected client or an
/// expired statement release the database owner without waiting for another
/// batch to fill.
pub trait QueryResultSink {
    fn start(&mut self, columns: Vec<ResultColumn>, read_ts: Timestamp) -> Result<()>;

    fn push_batch(&mut self, rows: Vec<Row>) -> Result<()>;

    fn cancelled(&self) -> bool;
}

/// Successful completion metadata returned only after every required scan
/// span has finished. The server emits `ResultEnd` from this value; errors
/// after earlier batches therefore cannot be mistaken for a complete result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryStreamSummary {
    pub read_ts: Timestamp,
    pub rows_read: u64,
}

/// Supported data-mutation operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlOperation {
    Insert,
    Update,
    Delete,
}

/// Result of executing one logical plan.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    /// A table was published in the local catalog and assigned one tablet.
    CreatedTable { table_id: TableId },

    /// A DML statement matched or inserted a number of rows.
    Mutation {
        operation: DmlOperation,
        affected_rows: usize,
    },

    /// A SELECT or SHOW statement returned rows.
    Query(ResultSet),

    /// BEGIN attached a new transaction to the SQL session.
    TransactionStarted {
        transaction_id: TxnId,
        start_ts: Timestamp,
    },

    /// COMMIT cleared the SQL session and committed its buffered mutations.
    ///
    /// Read-only transactions do not allocate a commit timestamp.
    TransactionCommitted {
        transaction_id: TxnId,
        commit_ts: Option<Timestamp>,
        committed_writes: usize,
    },

    /// ROLLBACK cleared the SQL session and discarded buffered mutations.
    TransactionRolledBack {
        transaction_id: TxnId,
        discarded_writes: usize,
    },
}
