//! the file handles the autocommit and transaction session behaviour
//!
//! a session owns the complete active `Transaction` and not merly its identifier
//! this keeps the transaction's snapshot timestamp and pending write set
//! attached to the same connection level state
//!
//! new session will use autocommit. standalone DML and SELECT statements recieve
//! \and implicit transaction, while BEGIN attaches an explicit transaction that
//! remains active until COMMIT or ROLLBACK

use std::sync::atomic::{AtomicU64, Ordering};

use ragnordb_common::{
    Error, Result,
    ids::{Timestamp, TxnId},
};
use ragnordb_sql::{Plan, analyze, parse_one, plan};
use ragnordb_txn::{Transaction, TransactionManager};

use crate::{ExecutionResult, LocalExecutor};

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// process-local identifier for one client sesssion
///
/// session IDs are diagnostic identities and are not durable transactions IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub u64);

/// conncetion level SQL transaction state
#[derive(Debug)]
pub struct Session {
    session_id: SessionId,
    current_transaction: Option<Transaction>,
    autocommit: bool,
    statement_timeout_ms: u64,
}

impl Session {
    /// construct session using v1 defaults
    ///
    /// autocommit begins enabled, no explicit transaction is attached,
    /// and the default statement timeout is thirty seconds
    pub fn new() -> Self {
        let session_id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);

        assert_ne!(
            session_id, 0,
            "process local session ID allocator exhausted and wrapped to zero"
        );

        Self {
            session_id: SessionId(session_id),
            current_transaction: None,
            autocommit: true,
            statement_timeout_ms: 30_000,
        }
    }

    /// Return the process-local session identifier.
    pub fn id(&self) -> SessionId {
        self.session_id
    }

    /// Return whether standalone data statements use implicit transactions.
    pub fn autocommit(&self) -> bool {
        self.autocommit
    }

    /// Return the configured statement timeout in milliseconds.
    pub fn statement_timeout_ms(&self) -> u64 {
        self.statement_timeout_ms
    }

    /// Return whether BEGIN has attached an explicit transaction.
    pub fn has_active_transaction(&self) -> bool {
        self.current_transaction.is_some()
    }

    /// Return the active transaction identifier, if one is attached.
    pub fn current_transaction_id(&self) -> Option<TxnId> {
        self.current_transaction.as_ref().map(Transaction::id)
    }

    /// Parse, analyze, plan, and execute one SQL statement.
    ///
    /// Parse and analysis failures occur before an implicit transaction is
    /// created. When an explicit transaction is active, such failures leave the
    /// transaction attached because no execution-side state was changed.
    pub fn execute_sql<M: TransactionManager>(
        &mut self,
        sql: &str,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult> {
        let parsed = parse_one(sql)?;
        let bound = analyze(&parsed, executor.catalog())?;
        let plan = plan(bound);

        self.execute_plan(plan, executor, transaction_manager)
    }

    /// Execute one parser-independent logical plan.
    pub fn execute_plan<M: TransactionManager>(
        &mut self,
        plan: Plan,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult> {
        match plan {
            Plan::Begin => self.begin(transaction_manager),

            Plan::Commit => self.commit(executor, transaction_manager),

            Plan::Rollback => self.rollback(executor),

            // CREATE TABLE remains autocommit-only. Passing the attached
            // transaction preserves the executor's existing DDL boundary and
            // produces a clear error without changing session state.
            plan @ Plan::CreateTable(_) => {
                executor.execute(plan, self.current_transaction.as_mut())
            }

            // SHOW TABLES reads catalog metadata and does not require an MVCC
            // transaction. An existing explicit transaction remains attached.
            Plan::ShowTables => executor.execute(Plan::ShowTables, None),

            plan @ (Plan::Insert(_) | Plan::Select(_) | Plan::Update(_) | Plan::Delete(_)) => {
                self.execute_data_plan(plan, executor, transaction_manager)
            }
        }
    }

    fn begin<M: TransactionManager>(
        &mut self,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult> {
        if self.current_transaction.is_some() {
            return Err(Error::InvalidArgument(
                "BEGIN cannot start a nested transaction; the session already has an active transaction"
                    .to_string(),
            ));
        }

        let transaction = transaction_manager.begin_transaction()?;
        let transaction_id = transaction.id();
        let start_ts = transaction.start_ts();

        self.current_transaction = Some(transaction);

        Ok(ExecutionResult::TransactionStarted {
            transaction_id,
            start_ts,
        })
    }

    fn commit<M: TransactionManager>(
        &mut self,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult> {
        // Taking the transaction first guarantees that COMMIT clears session
        // state on both success and failure.
        let transaction = self.current_transaction.take().ok_or_else(|| {
            Error::InvalidArgument(
                "COMMIT requires an active transaction; execute BEGIN first".to_string(),
            )
        })?;

        let transaction_id = transaction.id();

        let commit_ts = if transaction.is_empty() {
            None
        } else {
            match transaction_manager.allocate_commit_timestamp(transaction.start_ts()) {
                Ok(commit_ts) => Some(commit_ts),

                Err(error) => {
                    executor.rollback_transaction(transaction);
                    return Err(error);
                }
            }
        };

        let committed_writes =
            executor.commit_transaction(transaction, commit_ts.unwrap_or(Timestamp(0)))?;

        Ok(ExecutionResult::TransactionCommitted {
            transaction_id,
            commit_ts,
            committed_writes,
        })
    }

    fn rollback(&mut self, executor: &LocalExecutor) -> Result<ExecutionResult> {
        let transaction = self.current_transaction.take().ok_or_else(|| {
            Error::InvalidArgument(
                "ROLLBACK requires an active transaction; execute BEGIN first".to_string(),
            )
        })?;

        let transaction_id = transaction.id();
        let discarded_writes = executor.rollback_transaction(transaction);

        Ok(ExecutionResult::TransactionRolledBack {
            transaction_id,
            discarded_writes,
        })
    }

    fn execute_data_plan<M: TransactionManager>(
        &mut self,
        plan: Plan,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult> {
        if let Some(transaction) = self.current_transaction.as_mut() {
            // Explicit transactions keep their successfully buffered writes
            // after a later statement error. Phase 2.7 guarantees statement
            // preparation is atomic before changing the write set.
            return executor.execute(plan, Some(transaction));
        }

        if !self.autocommit {
            return Err(Error::InvalidArgument(
                "the session has autocommit disabled but no active transaction".to_string(),
            ));
        }

        self.execute_implicit(plan, executor, transaction_manager)
    }

    fn execute_implicit<M: TransactionManager>(
        &mut self,
        plan: Plan,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult> {
        let mut transaction = transaction_manager.begin_transaction()?;

        let result = match executor.execute(plan, Some(&mut transaction)) {
            Ok(result) => result,

            Err(error) => {
                // Implicit transaction errors discard the complete write set,
                // including any state prepared before the executor reported the
                // failure.
                executor.rollback_transaction(transaction);
                return Err(error);
            }
        };

        if transaction.is_empty() {
            // Snapshot-only statements require a start timestamp but do not
            // create an MVCC version and therefore need no commit timestamp.
            executor.commit_transaction(transaction, Timestamp(0))?;
            return Ok(result);
        }

        let commit_ts = match transaction_manager.allocate_commit_timestamp(transaction.start_ts())
        {
            Ok(commit_ts) => commit_ts,

            Err(error) => {
                executor.rollback_transaction(transaction);
                return Err(error);
            }
        };

        // Local tablet commit validates the complete write batch before applying
        // anything. A commit error therefore consumes and aborts the implicit
        // transaction without exposing a partial result.
        executor.commit_transaction(transaction, commit_ts)?;

        Ok(result)
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}
