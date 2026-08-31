//! Local SQL plan execution.
//!
//! it consumes parser-independent logical plans and executes them through
//! the catalog, transaction, and tablet APIs implemented in earlier phases.
//!
//! The local executor owns:
//!
//! - the mutable `MemoryCatalog`,
//! - one in-memory tablet for every locally created table,
//! - physical access-path selection between point lookup and table scan.
//!
//! The `session` module owns implicit and explicit SQL transaction lifecycles.
//! `SqlSession` contains only transaction policy and active transaction state;
//! connection identity, deadlines, and transport concerns remain in the server
//! crate. The lower-level `LocalExecutor` receives an active `Transaction` and
//! remains independent from connection-level state.
//!
//! The executor never depends directly on `sqlparser`. Unsupported SQL clauses
//! remain the analyzer's responsibility and cannot reach this layer as a Plan.

mod expression;
mod result;
mod session;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use expression::evaluate;
use ragnordb_catalog::{
    Catalog, CatalogCreateOutcome, CatalogLogExtent, CatalogLogRecord, ColumnSchema,
    DurableCatalog, DurableCatalogLog, MemoryCatalog, TableSchema,
};
use ragnordb_common::{
    Error, Result,
    catalog_codec::DataType,
    codec::{Row, Value, WriteKind},
    command_codec::SingleShardCommitCommand,
    encoding::encode_row,
    ids::{ColumnId, RowKey, TableId, TabletId, Timestamp},
    metadata_codec::{CreateTableRequest, MetadataCommandCodecError},
    proto::snapshot as snapshot_proto,
};
use ragnordb_sql::{
    BoundBinaryOperator, BoundColumnRef, BoundExpr, BoundExprKind, BoundTableRef, CreateTablePlan,
    DeletePlan, ExpressionType, InsertPlan, Plan, SelectPlan, UpdateAssignmentPlan, UpdatePlan,
};

use ragnordb_storage::{
    checkpoint::CapturedMvccState,
    key::{decode_row_key, make_row_key},
    mvcc::InMemoryMvcc,
    wal::{DurableCommitLog, DurableWalExtent, SingleNodeTxnCommit},
};

use ragnordb_tablet::{RowMutation, Tablet};
use ragnordb_txn::{
    CommitTimestampAllocator, SingleNodeCommitCoordinator, SingleNodeCommitOutcome, Transaction,
    TransactionManager,
};

pub use result::{DmlOperation, ExecutionResult, ResultColumn, ResultSet};
pub use session::SqlSession;

/// Process-wide semantic commit boundary used by every local tablet.
///
/// The trait object lets server startup replace the initial A-WAL sink with the
/// replicated tablet proposal sink after Raft recovery is complete.
pub type SharedCommitLog = Arc<dyn DurableCommitLog + Send + Sync>;

type LocalTablet = SingleNodeCommitCoordinator<Tablet, SharedCommitLog>;

/// Process-wide catalog publication boundary.
pub type SharedCatalogLog = Arc<dyn DurableCatalogLog + Send + Sync>;

/// Metadata-owned CREATE TABLE authority used by replicated SQL execution.
///
/// The executor supplies only schema semantics and a request identity. The
/// implementation must submit those semantics to metadata Raft and return the
/// fully assigned definition only after the corresponding entry has applied.
pub trait MetadataTableCreator: Send + Sync {
    fn create_table(
        &self,
        request: CreateTableRequest,
        request_id: ragnordb_common::ids::RequestId,
        timeout: Duration,
    ) -> Result<ragnordb_common::catalog_codec::TableDefinition>;

    /// Return the latest committed metadata definitions for local catalog
    /// cache refresh. Definitions are authoritative but remain read-only here.
    fn list_tables(&self) -> Vec<ragnordb_common::catalog_codec::TableDefinition>;
}

/// Shared metadata CREATE TABLE client installed by the server runtime.
pub type SharedMetadataTableCreator = Arc<dyn MetadataTableCreator>;

type LocalCatalog = DurableCatalog<SharedCatalogLog>;

/// Temporary materialized-result boundary until the client protocol streams.
pub const MAX_MATERIALIZED_RESULT_ROWS: usize = 100_000;

#[derive(Default)]
struct InMemoryCatalogLog {
    next_lsn: Mutex<u64>,
}

impl DurableCatalogLog for InMemoryCatalogLog {
    fn append_catalog_update(&self, _update: &CatalogLogRecord) -> Result<CatalogLogExtent> {
        let mut next_lsn = self
            .next_lsn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let start_lsn = *next_lsn;
        let end_lsn = start_lsn.checked_add(1).ok_or_else(|| {
            Error::Configuration("in-memory catalog-log LSN space is exhausted".to_string())
        })?;

        *next_lsn = end_lsn;

        Ok(CatalogLogExtent { start_lsn, end_lsn })
    }
}

/// In-memory semantic commit log used by executor unit tests
///
/// production node construction should inject `RagnorDbWalAdapter`. This
/// implementation exists so parser, planner, and executor tests remain
/// independent from filesystem setup while still exercising the exact same
/// coordinator path
#[derive(Default)]
struct InMemoryCommitLog {
    next_lsn: Mutex<u64>,
}

impl DurableCommitLog for InMemoryCommitLog {
    fn append_single_node_commit(&self, commit: &SingleNodeTxnCommit) -> Result<DurableWalExtent> {
        commit.encode()?;

        let mut next_lsn = self
            .next_lsn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let start_lsn = *next_lsn;
        let end_lsn = start_lsn.checked_add(1).ok_or_else(|| {
            Error::Configuration("in-memory commit-log LSN space is exhausted".to_string())
        })?;

        *next_lsn = end_lsn;

        Ok(DurableWalExtent::from_raw(start_lsn, end_lsn))
    }
}

/// Local single-node executor
///
/// Every locally created table receives one dedicated tablet. Using a separate
/// tablet per table preserves the ownership boundary introduced in Phase 2.6,
/// even though distributed routing is not implemented yet.
pub struct LocalExecutor {
    catalog: LocalCatalog,
    tablets: BTreeMap<TableId, LocalTablet>,
    metadata_table_creator: Option<SharedMetadataTableCreator>,
    metadata_table_ids: BTreeSet<TableId>,
    commit_log: SharedCommitLog,
    next_local_catalog_timestamp: u64,
    replay_from_end_lsn: u64,
}

impl std::fmt::Debug for LocalExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalExecutor")
            .field(
                "catalog_table_count",
                &self.catalog.catalog().list_tables().len(),
            )
            .field("tablet_count", &self.tablets.len())
            .finish_non_exhaustive()
    }
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalExecutor {
    /// an in-memory executor for unit and local semantic tests
    pub fn new() -> Self {
        Self::with_logs(
            Arc::new(InMemoryCommitLog::default()),
            Arc::new(InMemoryCatalogLog::default()),
        )
    }

    /// an executor using the supplied durable commit log
    ///
    /// A running database node supplies one shared `RagnorDbWalAdapter`, while
    /// tests may inject deterministic success and failure implementations
    pub fn with_commit_log(commit_log: SharedCommitLog) -> Self {
        Self::with_logs(commit_log, Arc::new(InMemoryCatalogLog::default()))
    }

    pub fn with_logs(commit_log: SharedCommitLog, catalog_log: SharedCatalogLog) -> Self {
        Self {
            catalog: DurableCatalog::new(catalog_log),
            tablets: BTreeMap::new(),
            metadata_table_creator: None,
            metadata_table_ids: BTreeSet::new(),
            commit_log,
            next_local_catalog_timestamp: 0,
            replay_from_end_lsn: 0,
        }
    }

    /// Route every future table commit through a new semantic durability sink.
    ///
    /// Existing coordinators must be updated together with the template stored
    /// for later `CREATE TABLE` operations. Updating only one side would allow
    /// tables created before startup wiring to bypass replication.
    pub fn replace_commit_log(&mut self, commit_log: SharedCommitLog) {
        for tablet in self.tablets.values_mut() {
            tablet.replace_commit_log(commit_log.clone());
        }
        self.commit_log = commit_log;
    }

    /// Route future catalog publications through the replicated host.
    pub fn replace_catalog_log(&mut self, catalog_log: SharedCatalogLog) {
        self.catalog.replace_durable_log(catalog_log);
    }

    /// Install the metadata Raft client used for replicated CREATE TABLE.
    pub fn replace_metadata_table_creator(&mut self, creator: SharedMetadataTableCreator) {
        self.metadata_table_creator = Some(creator);
    }

    pub(crate) fn metadata_table_creator_installed(&self) -> bool {
        self.metadata_table_creator.is_some()
    }

    /// Refresh the local SQL catalog cache from committed metadata state.
    ///
    /// Metadata-owned tables do not receive a local MVCC tablet until the
    /// later tablet-lifecycle phase. They are therefore visible to SQL schema
    /// analysis and `SHOW TABLES`, while DML correctly remains unavailable until
    /// a routed tablet is installed.
    pub fn refresh_metadata_catalog(&mut self) -> Result<()> {
        let Some(creator) = &self.metadata_table_creator else {
            return Ok(());
        };

        for definition in creator.list_tables() {
            let schema = self
                .catalog
                .catalog()
                .table_by_id(TableId(definition.table_id));

            if let Some(existing) = schema {
                if existing.to_definition() != definition {
                    return Err(Error::CorruptData(format!(
                        "metadata catalog definition for table {} conflicts with the local SQL cache",
                        definition.table_id,
                    )));
                }
            } else {
                self.catalog
                    .install_replicated_definition(definition.clone())?;
            }

            self.metadata_table_ids.insert(TableId(definition.table_id));
        }

        Ok(())
    }

    /// Install a Raft-authoritative catalog command and its local SQL tablet.
    pub fn apply_replicated_catalog(
        &mut self,
        command: &ragnordb_common::command_codec::CatalogCommand,
    ) -> Result<()> {
        let ragnordb_common::command_codec::CatalogOperation::CreateTable(operation) =
            &command.operation;
        let schema = self
            .catalog
            .install_replicated_definition(operation.table_def.clone())?;
        if !self.tablets.contains_key(&schema.id) {
            let tablet = Tablet::new(TabletId(schema.id.0), schema.id)?;
            let coordinator =
                SingleNodeCommitCoordinator::with_participant(tablet, self.commit_log.clone())?;
            self.tablets.insert(schema.id, coordinator);
        }
        Ok(())
    }

    /// Execute a metadata-owned CREATE TABLE.
    ///
    /// No local table-ID allocator, timestamp allocator, or
    /// `DurableCatalogLog` is consulted. The returned definition is already
    /// authoritative in metadata Raft and is installed only as a local SQL
    /// cache entry.
    pub fn execute_create_table_with_metadata(
        &mut self,
        plan: CreateTablePlan,
        request_id: ragnordb_common::ids::RequestId,
        timeout: Duration,
    ) -> Result<ExecutionResult> {
        let creator = self
            .metadata_table_creator
            .clone()
            .ok_or(Error::NotImplemented(
                "metadata-backed CREATE TABLE is unavailable",
            ))?;

        let CreateTablePlan {
            table_name,
            columns,
            primary_key_column_ids,
        } = plan;

        let request = CreateTableRequest {
            table_name,
            columns: columns
                .into_iter()
                .map(|column| ragnordb_common::catalog_codec::ColumnDefinition {
                    column_id: column.id,
                    name: column.name,
                    ty: column.ty,
                    nullable: column.nullable,
                })
                .collect(),
            primary_key_column_ids,
        };

        request
            .validate()
            .map_err(|error: MetadataCommandCodecError| {
                Error::ConstraintViolation(error.to_string())
            })?;

        let expected_request = request.clone();
        let definition = creator.create_table(request, request_id, timeout)?;

        if definition.table_id <= 1 {
            return Err(Error::CorruptData(format!(
                "metadata CREATE TABLE returned reserved table ID {}",
                definition.table_id,
            )));
        }

        if definition.name != expected_request.table_name
            || definition.columns != expected_request.columns
            || definition.primary_key_column_ids != expected_request.primary_key_column_ids
            || definition.schema_version != 1
            || definition.tablet_count != 1
        {
            return Err(Error::CorruptData(
                "metadata CREATE TABLE returned a definition different from the requested schema"
                    .to_string(),
            ));
        }

        let table_id = TableId(definition.table_id);

        self.catalog.install_replicated_definition(definition)?;
        self.metadata_table_ids.insert(table_id);

        Ok(ExecutionResult::CreatedTable { table_id })
    }

    /// Apply a Raft-authoritative single-tablet commit to the SQL read mirror.
    ///
    /// The replicated tablet state machine has already validated and applied
    /// this command. This second materialization keeps follower SQL planning and
    /// reads current without creating another durability record.
    pub fn apply_replicated_commit(&mut self, command: &SingleShardCommitCommand) -> Result<usize> {
        let first_key = command.writes.first().ok_or_else(|| {
            Error::InvalidArgument("replicated commit contains no writes".to_string())
        })?;
        let table_id = decode_row_key(&first_key.key)?.table_id;
        let mut transaction = Transaction::new(command.txn_id, command.start_timestamp)?;

        for write in &command.writes {
            let write_table_id = decode_row_key(&write.key)?.table_id;
            if write_table_id != table_id {
                return Err(Error::InvalidArgument(
                    "replicated single-shard commit spans multiple tables".to_string(),
                ));
            }

            match (write.op, write.row.as_ref()) {
                (WriteKind::Put, Some(row)) => {
                    transaction.buffer_put(write.key.clone(), encode_row(row)?)?;
                }
                (WriteKind::Delete, None) => {
                    transaction.buffer_delete(write.key.clone())?;
                }
                _ => {
                    return Err(Error::InvalidArgument(
                        "replicated commit contains an invalid row mutation".to_string(),
                    ));
                }
            }
        }

        self.tablets
            .get_mut(&table_id)
            .ok_or_else(|| Error::CorruptData(format!(
                "replicated commit targets table {}, but the local catalog has no matching tablet",
                table_id.0
            )))?
            .apply_replicated_commit(&transaction, command.commit_timestamp)
    }

    /// Replace the SQL mirror for the Milestone 4 tablet with Raft-recovered
    /// state. Returns `false` when its catalog table has not been created yet.
    pub fn install_replicated_storage(
        &mut self,
        table_id: TableId,
        storage: InMemoryMvcc,
    ) -> Result<bool> {
        if self.catalog.catalog().table_by_id(table_id).is_none() {
            return Ok(false);
        }
        let tablet = Tablet::with_storage(TabletId(table_id.0), table_id, storage)?;
        let coordinator =
            SingleNodeCommitCoordinator::with_participant(tablet, self.commit_log.clone())?;
        self.tablets.insert(table_id, coordinator);
        Ok(true)
    }

    /// constructs the live executor from completely recovered database state
    ///
    /// every recovered catalog table must have exactly one corresponding MVCC
    /// store. The method creates all tablets and durable coordinators before
    /// returning, so the caller cannot publish a partially initialized
    /// executor
    pub fn from_recovered(
        catalog: MemoryCatalog,
        mvcc_by_table: BTreeMap<TableId, InMemoryMvcc>,
        commit_log: SharedCommitLog,
        catalog_log: SharedCatalogLog,
        catalog_timestamp_high_water: Timestamp,
        replay_from_end_lsn: u64,
    ) -> Result<Self> {
        let catalog_table_ids = catalog
            .list_tables()
            .into_iter()
            .map(|schema| {
                if schema.tablet_count != 1 {
                    return Err(Error::CorruptData(format!(
                        "recovered local table {} declares {} tablets; \
                         single-node recovery requires exactly one",
                        schema.id.0, schema.tablet_count
                    )));
                }

                Ok(schema.id)
            })
            .collect::<Result<BTreeSet<_>>>()?;

        let storage_table_ids = mvcc_by_table.keys().copied().collect::<BTreeSet<_>>();

        if catalog_table_ids != storage_table_ids {
            return Err(Error::CorruptData(format!(
                "recovered catalog table set {:?} does not match recovered \
                 MVCC table set {:?}",
                catalog_table_ids, storage_table_ids
            )));
        }

        let mut tablets = BTreeMap::new();

        for (table_id, storage) in mvcc_by_table {
            let tablet = Tablet::with_storage(TabletId(table_id.0), table_id, storage).map_err(
                |source| {
                    Error::CorruptData(format!(
                        "failed to construct recovered tablet for table {}: {}",
                        table_id.0, source
                    ))
                },
            )?;

            let coordinator =
                SingleNodeCommitCoordinator::with_participant(tablet, commit_log.clone()).map_err(
                    |source| {
                        Error::CorruptData(format!(
                            "failed to construct recovered commit coordinator \
                         for table {}: {}",
                            table_id.0, source
                        ))
                    },
                )?;

            tablets.insert(table_id, coordinator);
        }

        Ok(Self {
            catalog: DurableCatalog::from_recovered(catalog, catalog_log),
            tablets,
            metadata_table_creator: None,
            metadata_table_ids: BTreeSet::new(),
            commit_log,
            next_local_catalog_timestamp: catalog_timestamp_high_water.0,
            replay_from_end_lsn,
        })
    }

    /// Return the catalog snapshot used by SQL analysis.
    ///
    /// The mutable catalog remains private so table creation cannot bypass
    /// corresponding tablet creation.
    pub fn catalog(&self) -> &MemoryCatalog {
        self.catalog.catalog()
    }

    /// Freeze catalog and MVCC state into detached per-table snapshot messages.
    ///
    /// `LocalDatabase` invokes this while it exclusively owns the complete
    /// runtime, which is the same serialized boundary used by commits and
    /// catalog publication.
    pub fn capture_snapshot_tables(&self) -> Result<Vec<snapshot_proto::SnapshotTable>> {
        self.catalog
            .catalog()
            .list_tables()
            .into_iter()
            .filter_map(|schema| {
                if self.metadata_table_ids.contains(&schema.id) {
                    return None;
                }

                Some(schema)
            })
            .map(|schema| {
                let coordinator = self.tablets.get(&schema.id).ok_or_else(|| {
                    Error::CorruptData(format!(
                        "catalog table {} has no local commit coordinator",
                        schema.id.0
                    ))
                })?;

                let mvcc: CapturedMvccState =
                    coordinator.participant().storage().capture_snapshot_state();

                Ok(mvcc.into_snapshot_table(schema.to_definition()))
            })
            .collect()
    }

    /// Return the first WAL position not represented by the current state.
    pub fn replay_from_end_lsn(&self) -> u64 {
        self.replay_from_end_lsn
    }

    /// Execute one logical plan.
    ///
    /// SELECT and DML plans require a transaction supplied by the caller. CREATE
    /// TABLE is autocommit-only and therefore rejects an attached transaction.
    /// Session transitions for BEGIN, COMMIT, and ROLLBACK are introduced in
    /// Phase 2.8 and are deliberately not simulated here.
    pub fn execute(
        &mut self,
        plan: Plan,
        transaction: Option<&mut Transaction>,
    ) -> Result<ExecutionResult> {
        match plan {
            Plan::CreateTable(plan) => {
                if transaction.is_some() {
                    return Err(Error::InvalidArgument(
                        "CREATE TABLE is autocommit-only and must not \
                         receive a transaction context"
                            .to_string(),
                    ));
                }

                self.execute_create_table(plan)
            }

            Plan::Insert(plan) => {
                self.execute_insert(plan, require_transaction(transaction, "INSERT")?)
            }

            Plan::Select(plan) => {
                self.execute_select(plan, require_transaction(transaction, "SELECT")?)
            }

            Plan::Update(plan) => {
                self.execute_update(plan, require_transaction(transaction, "UPDATE")?)
            }

            Plan::Delete(plan) => {
                self.execute_delete(plan, require_transaction(transaction, "DELETE")?)
            }

            Plan::ShowTables => self.execute_show_tables(),

            Plan::Begin | Plan::Commit | Plan::Rollback => Err(Error::NotImplemented(
                "transaction-control plans are handled by the \
                     Phase 2.8 session layer",
            )),
        }
    }

    /// commit a transaction through its table's durable coordinator
    ///
    /// this compatibility method returns only the affected-row count. Session
    /// integration uses `commit_transaction_outcome` to publish the coordinator's
    /// timestamp and WAL diagnostics
    pub fn commit_transaction<A>(
        &mut self,
        transaction: Transaction,
        timestamp_allocator: A,
    ) -> Result<usize>
    where
        A: CommitTimestampAllocator,
    {
        self.commit_transaction_outcome(transaction, timestamp_allocator)
            .map(|outcome| outcome.committed_writes)
    }

    /// commit a transaction and return its complete published outcome
    pub fn commit_transaction_outcome<A>(
        &mut self,
        transaction: Transaction,
        timestamp_allocator: A,
    ) -> Result<SingleNodeCommitOutcome>
    where
        A: CommitTimestampAllocator,
    {
        if transaction.is_empty() {
            return Ok(SingleNodeCommitOutcome {
                transaction_id: transaction.id(),
                commit_timestamp: None,
                committed_writes: 0,
                wal_extent: None,
            });
        }

        let mut table_ids = BTreeSet::new();

        for encoded_key in transaction.write_set().keys() {
            let row_key = decode_row_key(encoded_key)?;

            if self
                .catalog
                .catalog()
                .table_by_id(row_key.table_id)
                .is_none()
            {
                return Err(Error::SchemaMismatch(format!(
                    "transaction references unknown table ID {}",
                    row_key.table_id.0
                )));
            }

            table_ids.insert(row_key.table_id);
        }

        if table_ids.len() != 1 {
            return Err(Error::UnsupportedSql(
                "a local transaction may write only one tablet; \
                 cross-table transactions require distributed coordination"
                    .to_string(),
            ));
        }

        let table_id = *table_ids
            .first()
            .expect("non-empty table-ID set was checked above");

        let coordinator = self.tablets.get_mut(&table_id).ok_or_else(|| {
            Error::CorruptData(format!(
                "catalog table {} has no local commit coordinator",
                table_id.0
            ))
        })?;

        let outcome = coordinator.commit(transaction, timestamp_allocator)?;

        if let Some(extent) = outcome.wal_extent {
            self.replay_from_end_lsn = self.replay_from_end_lsn.max(extent.end_lsn.as_u64());
        }

        Ok(outcome)
    }

    /// Abort an uncommitted transaction by discarding its buffered mutations.
    pub fn rollback_transaction(&self, transaction: Transaction) -> usize {
        transaction.len()
    }

    fn execute_create_table(&mut self, plan: CreateTablePlan) -> Result<ExecutionResult> {
        let CreateTablePlan {
            table_name,
            columns,
            primary_key_column_ids,
        } = plan;

        let outcome = {
            let catalog = &mut self.catalog;
            let timestamp = &mut self.next_local_catalog_timestamp;

            catalog.create_table(table_name, columns, primary_key_column_ids, || {
                let next = timestamp.checked_add(1).ok_or_else(|| {
                    Error::Configuration("local catalog timestamp space is exhausted".to_string())
                })?;

                *timestamp = next;
                Ok(Timestamp(next))
            })?
        };

        self.install_catalog_table(outcome)
    }

    /// Execute CREATE TABLE using the shared database timestamp authority.
    pub fn execute_create_table_durable<M>(
        &mut self,
        plan: CreateTablePlan,
        transaction_manager: &mut M,
    ) -> Result<ExecutionResult>
    where
        M: TransactionManager,
    {
        let CreateTablePlan {
            table_name,
            columns,
            primary_key_column_ids,
        } = plan;

        let outcome =
            self.catalog
                .create_table(table_name, columns, primary_key_column_ids, || {
                    transaction_manager.allocate_commit_timestamp(Timestamp(0))
                })?;

        self.install_catalog_table(outcome)
    }

    fn install_catalog_table(&mut self, outcome: CatalogCreateOutcome) -> Result<ExecutionResult> {
        let table_id = outcome.schema.id;
        let replay_from_end_lsn = outcome.wal_extent.end_lsn;

        if self.tablets.contains_key(&table_id) {
            let error = self.catalog.stop_for_recovery(format!(
                "durable catalog table {} already has a local tablet",
                table_id.0
            ));

            return Err(error);
        }

        let tablet = Tablet::new(TabletId(table_id.0), table_id).map_err(|source| {
            self.catalog.stop_for_recovery(format!(
                "durable catalog table {} could not create its \
                         local tablet: {}",
                table_id.0, source
            ))
        })?;

        let coordinator =
            SingleNodeCommitCoordinator::with_participant(tablet, self.commit_log.clone())
                .map_err(|source| {
                    self.catalog.stop_for_recovery(format!(
                        "durable catalog table {} could not create its \
                     commit coordinator: {}",
                        table_id.0, source
                    ))
                })?;

        self.tablets.insert(table_id, coordinator);

        self.replay_from_end_lsn = self.replay_from_end_lsn.max(replay_from_end_lsn);

        Ok(ExecutionResult::CreatedTable { table_id })
    }

    fn execute_show_tables(&self) -> Result<ExecutionResult> {
        let rows = self
            .catalog
            .catalog()
            .list_tables()
            .into_iter()
            .take(MAX_MATERIALIZED_RESULT_ROWS + 1)
            .map(|table| Row {
                values: vec![Value::Text(table.name.clone())],
            })
            .collect::<Vec<_>>();

        ensure_result_row_limit(rows.len())?;

        Ok(ExecutionResult::Query(ResultSet {
            columns: vec![ResultColumn {
                name: "table_name".to_string(),
                data_type: DataType::Text,
                nullable: false,
            }],
            rows,
        }))
    }

    fn execute_insert(
        &self,
        plan: InsertPlan,
        transaction: &mut Transaction,
    ) -> Result<ExecutionResult> {
        let InsertPlan {
            table,
            target_columns,
            rows,
        } = plan;

        let schema = self.resolve_table(&table)?;
        let tablet = self.tablet_for(schema.id)?;

        for column in &target_columns {
            validate_bound_column(schema.as_ref(), column)?;
        }

        let mut prepared = Vec::with_capacity(rows.len());
        let mut statement_keys = BTreeSet::new();

        // Construct every row and key before touching the transaction buffer.
        // This gives a multi-row INSERT statement an all-or-nothing preparation
        // boundary for malformed rows and duplicate input keys.
        for values in rows {
            let row = materialize_insert_row(schema.as_ref(), &target_columns, values)?;

            let key = row_key_for_constructed_row(schema.as_ref(), &row)?;

            if !statement_keys.insert(key.clone()) {
                return Err(Error::ConstraintViolation(
                    "INSERT statement contains duplicate primary keys".to_string(),
                ));
            }

            prepared.push((key, row));
        }

        // Check every destination before buffering any mutation. Since this
        // executor holds exclusive access during the call, a later apply pass
        // cannot observe a different local storage state.
        for (key, _) in &prepared {
            if tablet.get(transaction, key)?.is_some() {
                return Err(Error::ConstraintViolation(format!(
                    "cannot insert duplicate primary key into table {}",
                    schema.name
                )));
            }
        }

        let affected_rows = prepared.len();

        tablet.buffer_batch(
            transaction,
            prepared
                .into_iter()
                .map(|(key, row)| RowMutation::Put { key, row }),
        )?;

        Ok(ExecutionResult::Mutation {
            operation: DmlOperation::Insert,
            affected_rows,
        })
    }

    fn execute_select(
        &self,
        plan: SelectPlan,
        transaction: &Transaction,
    ) -> Result<ExecutionResult> {
        let SelectPlan {
            table,
            projection,
            filter,
        } = plan;

        let schema = self.resolve_table(&table)?;
        let tablet = self.tablet_for(schema.id)?;

        if projection.is_empty() {
            return Err(Error::SchemaMismatch(
                "SELECT plan contains an empty projection".to_string(),
            ));
        }

        for column in &projection {
            validate_bound_column(schema.as_ref(), column)?;
        }

        if let Some(filter) = &filter {
            validate_filter(schema.as_ref(), filter)?;
        }

        let matching = matching_rows(tablet, transaction, schema.as_ref(), filter.as_ref())?;

        let rows = matching
            .iter()
            .map(|row| project_row(&row.row, &projection))
            .collect::<Result<Vec<_>>>()?;

        let columns = projection
            .into_iter()
            .map(|column| ResultColumn {
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
            })
            .collect();

        Ok(ExecutionResult::Query(ResultSet { columns, rows }))
    }

    fn execute_update(
        &self,
        plan: UpdatePlan,
        transaction: &mut Transaction,
    ) -> Result<ExecutionResult> {
        let UpdatePlan {
            table,
            assignments,
            filter,
        } = plan;

        let schema = self.resolve_table(&table)?;
        let tablet = self.tablet_for(schema.id)?;

        if assignments.is_empty() {
            return Err(Error::InvalidArgument(
                "UPDATE plan contains no assignments".to_string(),
            ));
        }

        validate_filter(schema.as_ref(), &filter)?;

        for assignment in &assignments {
            validate_update_assignment(schema.as_ref(), assignment)?;
        }

        let matching = matching_rows(tablet, transaction, schema.as_ref(), Some(&filter))?;

        let mut prepared = Vec::with_capacity(matching.len());

        // Evaluate all assignments for all rows before buffering anything. All
        // right-hand expressions observe the row as it existed before this
        // UPDATE statement, matching SQL simultaneous-assignment semantics.
        for keyed_row in matching {
            let original = keyed_row.row;
            let mut updated = original.clone();

            let evaluated = assignments
                .iter()
                .map(|assignment| evaluate(&assignment.value, &original))
                .collect::<Result<Vec<_>>>()?;

            for (assignment, value) in assignments.iter().zip(evaluated) {
                let column = validate_bound_column(schema.as_ref(), &assignment.column)?;

                validate_constructed_value(column, &value)?;

                updated.values[assignment.column.ordinal] = value;
            }

            validate_constructed_row(schema.as_ref(), &updated)?;

            prepared.push((keyed_row.key, updated));
        }

        let affected_rows = prepared.len();

        tablet.buffer_batch(
            transaction,
            prepared
                .into_iter()
                .map(|(key, row)| RowMutation::Put { key, row }),
        )?;

        Ok(ExecutionResult::Mutation {
            operation: DmlOperation::Update,
            affected_rows,
        })
    }

    fn execute_delete(
        &self,
        plan: DeletePlan,
        transaction: &mut Transaction,
    ) -> Result<ExecutionResult> {
        let DeletePlan { table, filter } = plan;

        let schema = self.resolve_table(&table)?;
        let tablet = self.tablet_for(schema.id)?;

        validate_filter(schema.as_ref(), &filter)?;

        let matching = matching_rows(tablet, transaction, schema.as_ref(), Some(&filter))?;

        // Matching completes before the atomic buffer operation, so neither a
        // filter error nor a mutation-encoding error can partially apply this
        // statement to the transaction.
        let affected_rows = matching.len();

        tablet.buffer_batch(
            transaction,
            matching
                .into_iter()
                .map(|keyed_row| RowMutation::Delete { key: keyed_row.key }),
        )?;

        Ok(ExecutionResult::Mutation {
            operation: DmlOperation::Delete,
            affected_rows,
        })
    }

    fn resolve_table(&self, table: &BoundTableRef) -> Result<Arc<TableSchema>> {
        let schema = self
            .catalog
            .catalog()
            .table_by_id(table.table_id)
            .ok_or_else(|| {
                Error::SchemaMismatch(format!(
                    "plan references unknown table ID {}",
                    table.table_id.0
                ))
            })?;

        if schema.name != table.name {
            return Err(Error::SchemaMismatch(format!(
                "table ID {} is named {}, but plan expects {}",
                table.table_id.0, schema.name, table.name
            )));
        }

        if schema.schema_version != table.schema_version {
            return Err(Error::SchemaMismatch(format!(
                "table {} is at schema version {}, but plan was \
                 bound against version {}",
                schema.name, schema.schema_version, table.schema_version
            )));
        }

        if schema.tablet_count != 1 {
            return Err(Error::UnsupportedSql(format!(
                "Phase 2.7 local execution requires exactly one \
                 tablet for table {}, found {}",
                schema.name, schema.tablet_count
            )));
        }

        Ok(schema)
    }

    fn tablet_for(&self, table_id: TableId) -> Result<&Tablet> {
        self.tablets
            .get(&table_id)
            .map(SingleNodeCommitCoordinator::participant)
            .ok_or_else(|| {
                Error::CorruptData(format!("catalog table {} has no local tablet", table_id.0))
            })
    }
}

fn require_transaction<'a>(
    transaction: Option<&'a mut Transaction>,
    statement: &str,
) -> Result<&'a mut Transaction> {
    transaction.ok_or_else(|| {
        Error::InvalidArgument(format!(
            "{statement} requires an active transaction context"
        ))
    })
}

/// One row and its stable primary-key identity.
#[derive(Debug)]
struct KeyedRow {
    key: RowKey,
    row: Row,
}

/// Pull-based internal row source.
///
/// Storage currently materializes tablet scans, but execution above that layer
/// pulls one keyed row at a time. This preserves a clean path to a fully
/// streaming storage scan in a later phase.
trait KeyedRowExecutor {
    fn next(&mut self) -> Result<Option<KeyedRow>>;
}

struct MaterializedRows {
    rows: std::vec::IntoIter<(RowKey, Row)>,
}

impl MaterializedRows {
    fn new(rows: Vec<(RowKey, Row)>) -> Self {
        Self {
            rows: rows.into_iter(),
        }
    }
}

impl KeyedRowExecutor for MaterializedRows {
    fn next(&mut self) -> Result<Option<KeyedRow>> {
        Ok(self.rows.next().map(|(key, row)| KeyedRow { key, row }))
    }
}

struct FilterRows<'a, E> {
    input: E,
    schema: &'a TableSchema,
    predicate: Option<&'a BoundExpr>,
}

impl<'a, E: KeyedRowExecutor> FilterRows<'a, E> {
    fn new(input: E, schema: &'a TableSchema, predicate: Option<&'a BoundExpr>) -> Self {
        Self {
            input,
            schema,
            predicate,
        }
    }
}

impl<E: KeyedRowExecutor> KeyedRowExecutor for FilterRows<'_, E> {
    fn next(&mut self) -> Result<Option<KeyedRow>> {
        loop {
            let Some(row) = self.input.next()? else {
                return Ok(None);
            };

            validate_stored_keyed_row(self.schema, &row)?;

            let matches = match self.predicate {
                Some(predicate) => expression::evaluate_filter(predicate, &row.row)?,
                None => true,
            };

            if matches {
                return Ok(Some(row));
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum AccessPath {
    Empty,
    Point(RowKey),
    Scan,
}

fn matching_rows(
    tablet: &Tablet,
    transaction: &Transaction,
    schema: &TableSchema,
    filter: Option<&BoundExpr>,
) -> Result<Vec<KeyedRow>> {
    let candidates = match choose_access_path(schema, filter)? {
        AccessPath::Empty => Vec::new(),

        AccessPath::Point(key) => tablet
            .get(transaction, &key)?
            .map(|row| vec![(key, row)])
            .unwrap_or_default(),

        AccessPath::Scan => tablet.scan(transaction, None, None)?,
    };

    let source = MaterializedRows::new(candidates);
    let mut filtered = FilterRows::new(source, schema, filter);
    let mut rows = Vec::new();

    while let Some(row) = filtered.next()? {
        if rows.len() == MAX_MATERIALIZED_RESULT_ROWS {
            return Err(Error::InvalidArgument(format!(
                "query result exceeds the materialized row limit of \
                 {MAX_MATERIALIZED_RESULT_ROWS}; add a selective predicate"
            )));
        }

        rows.push(row);
    }

    Ok(rows)
}

fn ensure_result_row_limit(row_count: usize) -> Result<()> {
    if row_count > MAX_MATERIALIZED_RESULT_ROWS {
        return Err(Error::InvalidArgument(format!(
            "query result exceeds the materialized row limit of \
             {MAX_MATERIALIZED_RESULT_ROWS}; add a selective predicate"
        )));
    }

    Ok(())
}

/// Select point lookup only when every primary-key column is constrained by a
/// literal equality in an AND-connected predicate.
fn choose_access_path(schema: &TableSchema, filter: Option<&BoundExpr>) -> Result<AccessPath> {
    let Some(filter) = filter else {
        return Ok(AccessPath::Scan);
    };

    let primary_key_ids = schema
        .primary_key_column_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();

    let mut equalities = BTreeMap::new();

    if collect_primary_key_equalities(filter, &primary_key_ids, &mut equalities) {
        return Ok(AccessPath::Empty);
    }

    let mut values = Vec::with_capacity(schema.primary_key_column_ids.len());

    for column_id in &schema.primary_key_column_ids {
        let Some(value) = equalities.get(column_id) else {
            return Ok(AccessPath::Scan);
        };

        let column = schema.column_by_id(*column_id).ok_or_else(|| {
            Error::SchemaMismatch(format!(
                "table {} references missing primary-key column ID {}",
                schema.name, column_id.0
            ))
        })?;

        if !value_matches_type(value, column.ty) {
            return Err(Error::SchemaMismatch(format!(
                "primary-key predicate for column {} requires {}, found {}",
                column.name,
                data_type_name(column.ty),
                value_type_name(value)
            )));
        }

        values.push(value.clone());
    }

    Ok(AccessPath::Point(make_row_key(schema.id, &values)?))
}

/// Collect safe primary-key equalities.
///
/// Returns `true` when two predicates constrain the same primary-key column to
/// different literals, making the predicate unsatisfiable.
fn collect_primary_key_equalities(
    expression: &BoundExpr,
    primary_key_ids: &BTreeSet<ColumnId>,
    equalities: &mut BTreeMap<ColumnId, Value>,
) -> bool {
    let BoundExprKind::Binary {
        left,
        operator,
        right,
    } = &expression.kind
    else {
        return false;
    };

    if *operator == BoundBinaryOperator::And {
        return collect_primary_key_equalities(left, primary_key_ids, equalities)
            || collect_primary_key_equalities(right, primary_key_ids, equalities);
    }

    if *operator != BoundBinaryOperator::Equal {
        return false;
    }

    let column_and_value = match (&left.kind, &right.kind) {
        (BoundExprKind::Column(column), BoundExprKind::Literal(value))
        | (BoundExprKind::Literal(value), BoundExprKind::Column(column)) => Some((column, value)),

        _ => None,
    };

    let Some((column, value)) = column_and_value else {
        return false;
    };

    if value == &Value::Null || !primary_key_ids.contains(&column.column_id) {
        return false;
    }

    if let Some(existing) = equalities.get(&column.column_id) {
        return existing != value;
    }

    equalities.insert(column.column_id, value.clone());
    false
}

fn materialize_insert_row(
    schema: &TableSchema,
    target_columns: &[BoundColumnRef],
    source_values: Vec<Value>,
) -> Result<Row> {
    if source_values.len() != target_columns.len() {
        return Err(Error::SchemaMismatch(format!(
            "INSERT plan contains {} values for {} target columns",
            source_values.len(),
            target_columns.len()
        )));
    }

    let mut values = vec![Value::Null; schema.columns.len()];
    let mut assigned = BTreeSet::new();

    for (target, value) in target_columns.iter().zip(source_values) {
        let column = validate_bound_column(schema, target)?;

        if !assigned.insert(target.ordinal) {
            return Err(Error::SchemaMismatch(format!(
                "INSERT plan assigns column {} more than once",
                target.name
            )));
        }

        validate_constructed_value(column, &value)?;
        values[target.ordinal] = value;
    }

    let row = Row { values };
    validate_constructed_row(schema, &row)?;

    Ok(row)
}

fn row_key_for_constructed_row(schema: &TableSchema, row: &Row) -> Result<RowKey> {
    let mut values = Vec::with_capacity(schema.primary_key_column_ids.len());

    for column in schema.primary_key_columns()? {
        let ordinal = schema.column_ordinal(column.id).ok_or_else(|| {
            Error::SchemaMismatch(format!(
                "primary-key column {} has no row ordinal",
                column.name
            ))
        })?;

        let value = row.values.get(ordinal).ok_or_else(|| {
            Error::SchemaMismatch(format!(
                "constructed row does not contain primary-key \
                 column {}",
                column.name
            ))
        })?;

        if value == &Value::Null {
            return Err(Error::ConstraintViolation(format!(
                "primary-key column {} cannot be NULL",
                column.name
            )));
        }

        values.push(value.clone());
    }

    make_row_key(schema.id, &values)
}

fn project_row(row: &Row, projection: &[BoundColumnRef]) -> Result<Row> {
    let values = projection
        .iter()
        .map(|column| {
            row.values.get(column.ordinal).cloned().ok_or_else(|| {
                Error::CorruptData(format!(
                    "stored row has no ordinal {} for projected \
                         column {}",
                    column.ordinal, column.name
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Row { values })
}

fn validate_update_assignment(
    schema: &TableSchema,
    assignment: &UpdateAssignmentPlan,
) -> Result<()> {
    validate_bound_column(schema, &assignment.column)?;

    if schema
        .primary_key_column_ids
        .contains(&assignment.column.column_id)
    {
        return Err(Error::ConstraintViolation(format!(
            "updating primary-key column {} is not supported",
            assignment.column.name
        )));
    }

    validate_expression_columns(schema, &assignment.value)
}

fn validate_filter(schema: &TableSchema, filter: &BoundExpr) -> Result<()> {
    if filter.data_type != ExpressionType::Bool {
        return Err(Error::SchemaMismatch(format!(
            "WHERE expression must return BOOL, found {}",
            filter.data_type
        )));
    }

    validate_expression_columns(schema, filter)
}

fn validate_expression_columns(schema: &TableSchema, expression: &BoundExpr) -> Result<()> {
    match &expression.kind {
        BoundExprKind::Column(column) => {
            validate_bound_column(schema, column)?;
        }

        BoundExprKind::Literal(_) => {}

        BoundExprKind::Unary { expression, .. } | BoundExprKind::IsNull { expression, .. } => {
            validate_expression_columns(schema, expression)?;
        }

        BoundExprKind::Binary { left, right, .. } => {
            validate_expression_columns(schema, left)?;
            validate_expression_columns(schema, right)?;
        }
    }

    Ok(())
}

fn validate_bound_column<'a>(
    schema: &'a TableSchema,
    column: &BoundColumnRef,
) -> Result<&'a ColumnSchema> {
    if column.table_id != schema.id {
        return Err(Error::SchemaMismatch(format!(
            "column {} belongs to table {}, but plan targets table {}",
            column.name, column.table_id.0, schema.id.0
        )));
    }

    let actual = schema.columns.get(column.ordinal).ok_or_else(|| {
        Error::SchemaMismatch(format!(
            "column {} uses invalid row ordinal {}",
            column.name, column.ordinal
        ))
    })?;

    if actual.id != column.column_id
        || actual.name != column.name
        || actual.ty != column.data_type
        || actual.nullable != column.nullable
    {
        return Err(Error::SchemaMismatch(format!(
            "bound metadata for column {} no longer matches schema \
             version {}",
            column.name, schema.schema_version
        )));
    }

    Ok(actual)
}

fn validate_constructed_row(schema: &TableSchema, row: &Row) -> Result<()> {
    if row.values.len() != schema.columns.len() {
        return Err(Error::SchemaMismatch(format!(
            "constructed row for table {} has {} values, expected {}",
            schema.name,
            row.values.len(),
            schema.columns.len()
        )));
    }

    for (column, value) in schema.columns.iter().zip(&row.values) {
        validate_constructed_value(column, value)?;
    }

    Ok(())
}

fn validate_constructed_value(column: &ColumnSchema, value: &Value) -> Result<()> {
    if value == &Value::Null {
        if column.nullable {
            return Ok(());
        }

        return Err(Error::ConstraintViolation(format!(
            "column {} cannot contain NULL",
            column.name
        )));
    }

    if !value_matches_type(value, column.ty) {
        return Err(Error::SchemaMismatch(format!(
            "column {} requires {}, found {}",
            column.name,
            data_type_name(column.ty),
            value_type_name(value)
        )));
    }

    Ok(())
}

fn validate_stored_keyed_row(schema: &TableSchema, keyed_row: &KeyedRow) -> Result<()> {
    validate_stored_row(schema, &keyed_row.row)?;

    let expected_key = stored_row_key(schema, &keyed_row.row)?;

    if expected_key != keyed_row.key {
        return Err(Error::CorruptData(format!(
            "stored row primary key does not match its tablet key \
             in table {}",
            schema.name
        )));
    }

    Ok(())
}

fn validate_stored_row(schema: &TableSchema, row: &Row) -> Result<()> {
    if row.values.len() != schema.columns.len() {
        return Err(Error::CorruptData(format!(
            "stored row for table {} has {} values, expected {}",
            schema.name,
            row.values.len(),
            schema.columns.len()
        )));
    }

    for (column, value) in schema.columns.iter().zip(&row.values) {
        if value == &Value::Null {
            if !column.nullable {
                return Err(Error::CorruptData(format!(
                    "stored row contains NULL in non-nullable \
                     column {}",
                    column.name
                )));
            }

            continue;
        }

        if !value_matches_type(value, column.ty) {
            return Err(Error::CorruptData(format!(
                "stored column {} contains {}, expected {}",
                column.name,
                value_type_name(value),
                data_type_name(column.ty)
            )));
        }
    }

    Ok(())
}

fn stored_row_key(schema: &TableSchema, row: &Row) -> Result<RowKey> {
    let mut values = Vec::with_capacity(schema.primary_key_column_ids.len());

    for column in schema.primary_key_columns()? {
        let ordinal = schema.column_ordinal(column.id).ok_or_else(|| {
            Error::CorruptData(format!(
                "primary-key column {} has no row ordinal",
                column.name
            ))
        })?;

        let value = row.values.get(ordinal).ok_or_else(|| {
            Error::CorruptData(format!(
                "stored row does not contain primary-key column {}",
                column.name
            ))
        })?;

        if value == &Value::Null {
            return Err(Error::CorruptData(format!(
                "stored primary-key column {} contains NULL",
                column.name
            )));
        }

        values.push(value.clone());
    }

    make_row_key(schema.id, &values).map_err(|error| {
        Error::CorruptData(format!("stored row has an invalid primary key: {error}"))
    })
}

fn value_matches_type(value: &Value, data_type: DataType) -> bool {
    matches!(
        (value, data_type),
        (Value::Int(_), DataType::Int)
            | (Value::Text(_), DataType::Text)
            | (Value::Bool(_), DataType::Bool)
    )
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Int(_) => "INT",
        Value::Text(_) => "TEXT",
        Value::Bool(_) => "BOOL",
        Value::Null => "NULL",
    }
}

fn data_type_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Int => "INT",
        DataType::Text => "TEXT",
        DataType::Bool => "BOOL",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_sql::{analyze, parse_one, plan};

    fn build(executor: &LocalExecutor, sql: &str) -> Plan {
        let parsed = parse_one(sql).unwrap();
        let bound = analyze(&parsed, executor.catalog()).unwrap();

        plan(bound)
    }

    fn create_memberships(executor: &mut LocalExecutor) {
        let create = build(
            executor,
            "CREATE TABLE memberships (
                user_id INT,
                group_id INT,
                role TEXT NOT NULL,
                PRIMARY KEY (user_id, group_id)
            )",
        );

        executor.execute(create, None).unwrap();
    }

    fn select_access_path(executor: &LocalExecutor, sql: &str) -> AccessPath {
        let Plan::Select(select) = build(executor, sql) else {
            panic!("expected SELECT plan");
        };

        let schema = executor.resolve_table(&select.table).unwrap();

        choose_access_path(schema.as_ref(), select.filter.as_ref()).unwrap()
    }

    #[test]
    fn access_path_selection_requires_the_complete_primary_key() {
        let mut executor = LocalExecutor::new();
        create_memberships(&mut executor);

        assert_eq!(
            select_access_path(
                &executor,
                "SELECT role FROM memberships
                 WHERE group_id = 20 AND user_id = 1",
            ),
            AccessPath::Point(make_row_key(TableId(1), &[Value::Int(1), Value::Int(20)]).unwrap())
        );

        assert_eq!(
            select_access_path(&executor, "SELECT role FROM memberships WHERE user_id = 1",),
            AccessPath::Scan
        );

        assert_eq!(
            select_access_path(
                &executor,
                "SELECT role FROM memberships
                 WHERE user_id = 1 AND user_id = 2 AND group_id = 20",
            ),
            AccessPath::Empty
        );
    }
}
