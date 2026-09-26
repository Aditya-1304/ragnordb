//! Autocommit and explicit SQL transaction behavior.
//!
//! `SqlSession` owns SQL transaction policy, the complete active `Transaction`,
//! and the request context passed to metadata-routed tablet operations.
//! Connection transport and cancellation remain server-layer responsibilities.
//!
//! New SQL sessions use autocommit. Standalone DML and SELECT statements receive
//! an implicit transaction. BEGIN attaches an explicit transaction that remains
//! active until COMMIT or ROLLBACK.

use std::time::Duration;

use ragnordb_common::{Error, Result, ids::ClientRequestId, ids::RequestId, ids::TxnId};
use ragnordb_sql::{Plan, analyze, parse_one, plan};
use ragnordb_txn::{Transaction, TransactionManager};

use crate::{
    ExecutionResult, LocalExecutor, QueryResultSink, QueryStreamSummary, TabletRequestContext,
};

/// SQL transaction policy and state for one client connection.
///
/// The caller must share exactly one transaction manager between every SQL
/// session operating on the same local executor. The executor owns database
/// state, while the shared manager provides database-wide transaction IDs and
/// timestamps.
#[derive(Debug)]
pub struct SqlSession {
    current_transaction: Option<Transaction>,
    tablet_request_context: TabletRequestContext,
}

impl SqlSession {
    /// Construct a SQL session with autocommit enabled and no active explicit
    /// transaction.
    pub fn new() -> Self {
        Self::with_client_id(1)
    }

    /// Construct a SQL session with an explicit stable client identity.
    ///
    /// Server connections use an incarnation-safe identity; tests and embedded
    /// callers can keep the default constructor when no remote tablet is used.
    pub fn with_client_id(client_id: u128) -> Self {
        Self {
            current_transaction: None,
            tablet_request_context: TabletRequestContext::new(client_id)
                .expect("SQL session client identity must be non-zero"),
        }
    }

    /// Replace the request identity used by future metadata-routed tablet RPCs.
    pub fn set_client_id(&mut self, client_id: u128) -> Result<()> {
        self.tablet_request_context = TabletRequestContext::new(client_id)?;
        Ok(())
    }

    pub fn set_client_request_identity(
        &mut self,
        client_id: u128,
        session_epoch: u64,
        request_sequence: u64,
    ) -> Result<()> {
        self.set_client_request_identity_with_ack(client_id, session_epoch, request_sequence, None)
    }

    pub fn set_client_request_identity_with_ack(
        &mut self,
        client_id: u128,
        session_epoch: u64,
        request_sequence: u64,
        acknowledged_through: Option<u64>,
    ) -> Result<()> {
        self.tablet_request_context.reset_for_root_request_with_ack(
            client_id,
            session_epoch,
            request_sequence,
            acknowledged_through,
        )
    }

    /// Update the bounded RPC deadline for this connection's next statement.
    pub fn set_tablet_request_timeout(&mut self, timeout: Duration) {
        self.tablet_request_context.set_timeout(timeout);
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

    /// Borrow the current transaction so the server can install durable
    /// transaction policies before the session is allowed to execute SQL.
    pub fn current_transaction_mut(&mut self) -> Option<&mut Transaction> {
        self.current_transaction.as_mut()
    }

    /// Begin an explicit transaction for the shared distributed SQL owner.
    ///
    /// The allocator is borrowed only for the identity allocation itself; the
    /// returned transaction remains owned by this connection while later
    /// tablet operations run outside the allocator lock.
    pub fn begin_with_transaction_manager<M: TransactionManager + ?Sized>(
        &mut self,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult> {
        self.begin(transaction_manager)
    }

    /// Execute a data plan against this session's explicit transaction without
    /// requiring mutable ownership of the shared executor.
    pub fn execute_data_plan_with_shared_executor(
        &mut self,
        plan: Plan,
        executor: &LocalExecutor,
    ) -> Result<ExecutionResult> {
        let transaction = self.current_transaction.as_mut().ok_or_else(|| {
            Error::InvalidArgument(
                "shared executor data execution requires an active transaction".to_string(),
            )
        })?;
        executor.execute_data_plan_with_request_context(
            plan,
            transaction,
            &mut self.tablet_request_context,
        )
    }

    /// Stream an explicit transaction's SELECT through a shared executor. The
    /// executor is borrowed immutably while the gateway performs remote page
    /// reads, so unrelated sessions do not wait on a node-wide mutable owner.
    pub fn execute_select_streaming_with_shared_executor(
        &mut self,
        plan: Plan,
        executor: &LocalExecutor,
        sink: &mut dyn QueryResultSink,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<QueryStreamSummary> {
        let transaction = self.current_transaction.as_mut().ok_or_else(|| {
            Error::InvalidArgument(
                "shared executor streaming requires an active transaction".to_string(),
            )
        })?;
        executor.execute_select_streaming(
            plan,
            transaction,
            &mut self.tablet_request_context,
            sink,
            max_rows,
            max_bytes,
        )
    }

    /// Remove the explicit transaction before entering its commit boundary.
    ///
    /// Taking it first preserves the existing rule that a commit failure,
    /// including an indeterminate remote outcome, cannot leave a half-usable
    /// transaction attached to the connection.
    pub fn take_transaction_for_commit(&mut self) -> Result<Transaction> {
        self.current_transaction.take().ok_or_else(|| {
            Error::InvalidArgument(
                "COMMIT requires an active transaction; execute BEGIN first".to_string(),
            )
        })
    }

    /// Discard the active transaction without touching shared executor state.
    pub fn rollback_current_transaction(&mut self) -> Result<ExecutionResult> {
        let transaction = self.current_transaction.take().ok_or_else(|| {
            Error::InvalidArgument(
                "ROLLBACK requires an active transaction; execute BEGIN first".to_string(),
            )
        })?;

        Ok(ExecutionResult::TransactionRolledBack {
            transaction_id: transaction.id(),
            discarded_writes: transaction.len(),
        })
    }

    /// Return the request context used by the distributed database service.
    /// The context remains connection-owned and is never shared between SQL
    /// statements or connections.
    pub fn request_context_mut(&mut self) -> &mut TabletRequestContext {
        &mut self.tablet_request_context
    }

    /// Parse, analyze, plan, and execute one SQL statement.
    ///
    /// Parse and analysis failures occur before an implicit transaction is
    /// created. When an explicit transaction is active, these failures leave
    /// the transaction attached because no execution state was changed.
    pub fn execute_sql<M: TransactionManager + ?Sized>(
        &mut self,
        sql: &str,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult> {
        self.execute_sql_with_metadata_request(
            sql,
            executor,
            transaction_manager,
            None,
            Duration::ZERO,
        )
    }

    /// Execute an opt-in streaming SELECT while preserving the same
    /// transaction and metadata-refresh boundaries as the materialized path.
    /// The successful stream summary is returned only after an implicit
    /// transaction has crossed its commit boundary.
    pub fn execute_sql_streaming<M: TransactionManager + ?Sized>(
        &mut self,
        sql: &str,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
        sink: &mut dyn QueryResultSink,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<QueryStreamSummary> {
        let parsed = parse_one(sql)?;
        let bound = analyze(&parsed, executor.catalog())?;
        let plan = plan(bound);
        let Plan::Select(_) = plan else {
            return Err(Error::UnsupportedSql(
                "streaming result mode currently supports SELECT only".to_string(),
            ));
        };

        if let Some(transaction) = self.current_transaction.as_mut() {
            return executor.execute_select_streaming(
                plan,
                transaction,
                &mut self.tablet_request_context,
                sink,
                max_rows,
                max_bytes,
            );
        }

        let mut transaction = transaction_manager.begin_transaction()?;
        let summary = match executor.execute_select_streaming(
            plan,
            &mut transaction,
            &mut self.tablet_request_context,
            sink,
            max_rows,
            max_bytes,
        ) {
            Ok(summary) => summary,
            Err(error) => {
                executor.rollback_transaction(transaction);
                return Err(error);
            }
        };
        let _ = executor.commit_sql_transaction_outcome_with_request_context(
            transaction,
            transaction_manager,
            &mut self.tablet_request_context,
        )?;
        Ok(summary)
    }

    /// Parse, analyze, and execute one SQL statement with an optional
    /// metadata-Raft request identity for CREATE TABLE.
    pub fn execute_sql_with_metadata_request<M: TransactionManager + ?Sized>(
        &mut self,
        sql: &str,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
        metadata_request_id: Option<RequestId>,
        metadata_timeout: Duration,
    ) -> Result<ExecutionResult> {
        self.execute_sql_with_metadata_request_and_identity(
            sql,
            executor,
            transaction_manager,
            metadata_request_id,
            None,
            metadata_timeout,
        )
    }

    pub fn execute_sql_with_metadata_request_and_identity<M: TransactionManager + ?Sized>(
        &mut self,
        sql: &str,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
        metadata_request_id: Option<RequestId>,
        logical_request_id: Option<ClientRequestId>,
        metadata_timeout: Duration,
    ) -> Result<ExecutionResult> {
        let parsed = parse_one(sql)?;
        let bound = analyze(&parsed, executor.catalog())?;
        let plan = plan(bound);

        self.execute_plan_with_metadata_request_and_identity(
            plan,
            executor,
            transaction_manager,
            metadata_request_id,
            logical_request_id,
            metadata_timeout,
        )
    }

    /// Execute one parser-independent logical plan.
    pub fn execute_plan<M: TransactionManager + ?Sized>(
        &mut self,
        plan: Plan,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult> {
        self.execute_plan_with_metadata_request(
            plan,
            executor,
            transaction_manager,
            None,
            Duration::ZERO,
        )
    }

    /// Execute one plan while preserving the optional metadata request
    /// identity across the SQL-session boundary.
    pub fn execute_plan_with_metadata_request<M: TransactionManager + ?Sized>(
        &mut self,
        plan: Plan,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
        metadata_request_id: Option<RequestId>,
        metadata_timeout: Duration,
    ) -> Result<ExecutionResult> {
        self.execute_plan_with_metadata_request_and_identity(
            plan,
            executor,
            transaction_manager,
            metadata_request_id,
            None,
            metadata_timeout,
        )
    }

    pub fn execute_plan_with_metadata_request_and_identity<M: TransactionManager + ?Sized>(
        &mut self,
        plan: Plan,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
        metadata_request_id: Option<RequestId>,
        logical_request_id: Option<ClientRequestId>,
        metadata_timeout: Duration,
    ) -> Result<ExecutionResult> {
        match plan {
            Plan::Begin => self.begin(transaction_manager),

            Plan::Commit => self.commit(executor, transaction_manager),

            Plan::Rollback => self.rollback(executor),

            // CREATE TABLE remains autocommit-only. Passing an attached
            // transaction preserves the executor's DDL validation boundary
            // without changing the session transaction.
            Plan::CreateTable(plan) => {
                if self.current_transaction.is_some() {
                    return executor
                        .execute(Plan::CreateTable(plan), self.current_transaction.as_mut());
                }

                match metadata_request_id {
                    Some(request_id) => executor.execute_create_table_with_metadata_and_identity(
                        plan,
                        request_id,
                        logical_request_id,
                        metadata_timeout,
                    ),
                    None if executor.metadata_table_creator_installed() => {
                        Err(Error::InvalidArgument(
                            "metadata-backed CREATE TABLE requires a request identity".to_string(),
                        ))
                    }
                    None => executor.execute_create_table_durable(plan, transaction_manager),
                }
            }

            // SHOW TABLES reads catalog metadata and does not require an MVCC
            // transaction. An existing explicit transaction remains attached.
            Plan::ShowTables => executor.execute(Plan::ShowTables, None),

            // Replicated servers replace this with their bounded lifecycle
            // snapshot before the executor boundary. The local compatibility
            // executor has no shared distributed lifecycle registry.
            Plan::ShowTransactions => executor.execute(Plan::ShowTransactions, None),

            plan @ (Plan::Insert(_) | Plan::Select(_) | Plan::Update(_) | Plan::Delete(_)) => {
                self.execute_data_plan(plan, executor, transaction_manager)
            }
        }
    }

    fn begin<M: TransactionManager + ?Sized>(
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

    fn commit<M: TransactionManager + ?Sized>(
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

        let outcome = executor.commit_sql_transaction_outcome_with_request_context(
            transaction,
            transaction_manager,
            &mut self.tablet_request_context,
        )?;

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

    fn execute_data_plan<M: TransactionManager + ?Sized>(
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
            return executor.execute_with_request_context(
                plan,
                Some(transaction),
                &mut self.tablet_request_context,
            );
        }

        self.execute_implicit(plan, executor, transaction_manager)
    }

    fn execute_implicit<M: TransactionManager + ?Sized>(
        &mut self,
        plan: Plan,
        executor: &mut LocalExecutor,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult> {
        let mut transaction = transaction_manager.begin_transaction()?;

        let result = match executor.execute_with_request_context(
            plan,
            Some(&mut transaction),
            &mut self.tablet_request_context,
        ) {
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
        let _commit_outcome = executor.commit_sql_transaction_outcome_with_request_context(
            transaction,
            transaction_manager,
            &mut self.tablet_request_context,
        )?;

        Ok(result)
    }
}

impl Default for SqlSession {
    fn default() -> Self {
        Self::new()
    }
}
