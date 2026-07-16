//! server owned local database runtime
//!
//! a running local node owns exactly onw executor and one transaction
//! manager. every client connection shares this runtime, while each connection
//! retains its own `SqlSession` and therefore its own explicit transaction state
//!
//! the runtime us protected by one asynchronous mutex. this serializes
//! physical statement execution while still allowing explicit transactions from
//! different connections to interleave between statements

use std::sync::Arc;

use ragnordb_common::Result;
use ragnordb_exec::{ExecutionResult, LocalExecutor, SqlSession};
use ragnordb_txn::LocalTransactionManager;
use tokio::sync::Mutex;

/// Shared server wide local database runtime
pub type SharedLocalDatabase = Arc<Mutex<LocalDatabase>>;

/// in memory database state owned by one running server node.
///
/// The executor and transaction manager must remain paired. Constructing either
/// one per connection would isolate catalog/tablet state or reuse transaction
/// identities and MVCC timestamps.
#[derive(Debug, Default)]
pub struct LocalDatabase {
    executor: LocalExecutor,
    transaction_manager: LocalTransactionManager,
}

impl LocalDatabase {
    /// an empty local database runtime
    pub fn new() -> Self {
        Self::default()
    }

    /// a shard runtime suitable for cloning into connection tasks
    pub fn shared() -> SharedLocalDatabase {
        Arc::new(Mutex::new(Self::new()))
    }

    /// execute one sql statement through the connection's SQL session
    pub fn execute_sql(&mut self, session: &mut SqlSession, sql: &str) -> Result<ExecutionResult> {
        let Self {
            executor,
            transaction_manager,
        } = self;

        session.execute_sql(sql, executor, transaction_manager)
    }
}
