//! Autocommit and explicit SQL transaction behavior.
//!
//! `SqlSession` owns only SQL transaction policy and the complete active
//! `Transaction`. Connection identity, statement deadlines, cancellation, and
//! client transport state remain server-layer responsibilities.
//!
//! New SQL sessions use autocommit. Standalone DML and SELECT statements receive
//! an implicit transaction. BEGIN attaches an explicit transaction that remains
//! active until COMMIT or ROLLBACK.

use ragnordb_common::{Error, Result, ids::TxnId};
use ragnordb_sql::{Plan, analyze, parse_one, plan};
use ragnordb_txn::{Transaction, TransactionManager};

use crate::{ExecutionResult, LocalExecutor};

/// SQL transaction policy and state for one client connection.
///
/// The caller must share exactly one transaction manager between every SQL
/// session operating on the same local executor. The executor owns database
/// state, while the shared manager provides database-wide transaction IDs and
/// timestamps.
#[derive(Debug)]
pub struct SqlSession {
    current_transaction: Option<Transaction>,
}

impl SqlSession {
    /// Construct a SQL session with autocommit enabled and no active explicit
    /// transaction.
    pub fn new() -> Self {
        Self {
            current_transaction: None,
        }
    }

    /// Return whether standalone data statements use implicit transactions.
    ///
    /// Until SQL `SET autocommit` support exists, autocommit state is derived
    /// entirely from whether BEGIN has attached an explicit transaction.
    pub fn autocommit(&self) -> bool {
        self.current_transaction.is_none()
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
    /// created. When an explicit transaction is active, these failures leave
    /// the transaction attached because no execution state was changed.
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

            // CREATE TABLE remains autocommit-only. Passing an attached
            // transaction preserves the executor's DDL validation boundary
            // without changing the session transaction.
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
                "BEGIN cannot start a nested transaction; the SQL session already has an active transaction"
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
        // taking the transaction before entering the coordinator guarantees
        // that success, preflight failure, outcome unknown, and fatal recovery
        // errors all terminate the explicit SQL transaction.
        let transaction = self.current_transaction.take().ok_or_else(|| {
            Error::InvalidArgument(
                "COMMIT requires an active transaction; \
                     execute BEGIN first"
                    .to_string(),
            )
        })?;

        let outcome = executor.commit_transaction_outcome(transaction, transaction_manager)?;

        Ok(ExecutionResult::TransactionCommitted {
            transaction_id: outcome.transaction_id,
            commit_ts: outcome.commit_timestamp,
            committed_writes: outcome.committed_writes,
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
            // Explicit transactions remain active after statement errors.
            // Phase 2.7 prepares complete statement batches before adding them
            // to the write set, so a failed statement contributes no partial
            // mutations while earlier successful statements remain available.
            return executor.execute(plan, Some(transaction));
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
                // Statement preparation and buffering are atomic. Consuming
                // the implicit transaction discards every pending mutation.
                executor.rollback_transaction(transaction);
                return Err(error);
            }
        };

        // The implicit statement already has its client-facing result. The
        // commit outcome is consumed here as the required durability and MVCC
        // publication gate before that statement result can be acknowledged.
        let _commit_outcome =
            executor.commit_transaction_outcome(transaction, transaction_manager)?;

        Ok(result)
    }
}

impl Default for SqlSession {
    fn default() -> Self {
        Self::new()
    }
}
