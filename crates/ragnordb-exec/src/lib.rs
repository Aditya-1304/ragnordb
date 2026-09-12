//! Local SQL plan execution.
//!
//! it consumes parser-independent logical plans and executes them through
//! the catalog, transaction, and tablet APIs implemented in earlier phases.
//!
//! The local executor owns:
//!
//! - the mutable `MemoryCatalog`,
//! - one in-memory tablet for every locally materialized table,
//! - physical access-path selection between point lookup and table scan,
//! - the optional server-provided gateway for remote metadata-routed points.
//!
//! The `session` module owns implicit and explicit SQL transaction lifecycles.
//! `SqlSession` contains transaction policy, active transaction state, and the
//! request identity context needed to make remote tablet calls deterministic.
//! Transport ownership remains in the server crate.
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
    catalog_codec::TableDefinition,
    codec::{Row, Value, WriteKind},
    command_codec::{
        CachedTabletCommandOutcome, CachedTabletCommandRejectionKind, SingleShardCommitCommand,
        TabletCommand, WriteEntry,
    },
    encoding::{decode_row, encode_row},
    ids::{
        ClientRequestId, ColumnId, CommandKind, LogicalCommandId, RaftGroupId, RequestId, RowKey,
        TableId, TabletId, Timestamp,
    },
    metadata_codec::{CreateTableRequest, MetadataCommandCodecError, TabletDescriptor},
    proto::snapshot as snapshot_proto,
    rpc_codec::TabletRoute,
};
use ragnordb_sql::{
    BoundBinaryOperator, BoundColumnRef, BoundExpr, BoundExprKind, BoundTableRef, CreateTablePlan,
    DeletePlan, ExpressionType, InsertPlan, Plan, SelectPlan, UpdateAssignmentPlan, UpdatePlan,
};

use ragnordb_storage::{
    checkpoint::CapturedMvccState,
    key::{decode_row_key, make_row_key},
    mvcc::{InMemoryMvcc, Mutation},
    wal::{DurableCommitLog, DurableWalExtent, SingleNodeTxnCommit},
};

use ragnordb_tablet::command::TabletCommandApplyOutcome;
use ragnordb_tablet::{RowMutation, Tablet, TabletRouter};
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

    /// Register one durable retry session in metadata control state. The
    /// request identity is separate from SQL table allocation so reconnects
    /// cannot silently invent an executable old session epoch.
    fn register_client(
        &self,
        _request_id: ragnordb_common::ids::RequestId,
        _client_id: u128,
        _requested_session_epoch: u64,
        _timeout: Duration,
    ) -> Result<u64> {
        Err(Error::NotImplemented(
            "metadata-backed client registration is unavailable",
        ))
    }

    /// Renew a registered retry session and advance its durable acknowledgement
    /// floor without allowing that floor to regress.
    fn renew_client(
        &self,
        _request_id: ragnordb_common::ids::RequestId,
        _client_id: u128,
        _session_epoch: u64,
        _acknowledged_through: u64,
        _timeout: Duration,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "metadata-backed client renewal is unavailable",
        ))
    }

    /// Read the committed session epoch used to fence stale connections. A
    /// missing session is distinct from an expired epoch so a caller can
    /// choose registration or fail closed explicitly.
    fn active_client_session_epoch(&self, _client_id: u128) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Return the committed tablet descriptors for one metadata-owned table.
    ///
    /// A schema definition without its complete partition map is not a usable
    /// routing view. Implementations that can create tables should override
    /// this method, while the default keeps older test doubles fail-closed.
    fn table_descriptors(&self, _table_id: TableId) -> Result<Vec<TabletDescriptor>> {
        Err(Error::NotImplemented(
            "metadata tablet topology lookup is unavailable",
        ))
    }

    /// Return the schema and the topology observed for one CREATE TABLE.
    ///
    /// The default composes the legacy schema method with the topology lookup
    /// so existing metadata clients can adopt the stronger contract
    /// incrementally. The server implementation overrides this at the
    /// proposal/apply boundary to return both values from one committed view.
    fn create_table_topology(
        &self,
        request: CreateTableRequest,
        request_id: ragnordb_common::ids::RequestId,
        timeout: Duration,
    ) -> Result<MetadataTableTopology> {
        let definition = self.create_table(request, request_id, timeout)?;
        let table_id = TableId(definition.table_id);
        let tablets = self.table_descriptors(table_id)?;

        Ok(MetadataTableTopology {
            definition,
            tablets,
        })
    }

    /// Identity-aware CREATE TABLE boundary. The legacy method remains the
    /// compatibility entry point for embedded callers; replicated V2 sessions
    /// override this method so metadata deduplication includes the session
    /// epoch and root request sequence.
    fn create_table_topology_with_identity(
        &self,
        request: CreateTableRequest,
        request_id: ragnordb_common::ids::RequestId,
        logical_request_id: Option<ClientRequestId>,
        timeout: Duration,
    ) -> Result<MetadataTableTopology> {
        let _ = logical_request_id;
        self.create_table_topology(request, request_id, timeout)
    }

    /// Return the latest committed metadata definitions for local catalog
    /// cache refresh. Definitions are authoritative but remain read-only here.
    fn list_tables(&self) -> Vec<ragnordb_common::catalog_codec::TableDefinition>;
}

/// Shared metadata CREATE TABLE client installed by the server runtime.
pub type SharedMetadataTableCreator = Arc<dyn MetadataTableCreator>;

/// Gateway operations required by SQL execution for a metadata-routed tablet.
///
/// The executor owns SQL semantics but must not depend on the server crate. The
/// server therefore supplies this narrow object-safe boundary, backed by the
/// Slice 2 local/remote tablet RPC client. Implementations must preserve the
/// supplied request identity when forwarding commands so a retry cannot become
/// a second logical mutation at the tablet.
pub trait TabletGateway: Send + Sync {
    fn lookup_tablet_route(&self, table_id: TableId, key: &[u8]) -> Result<TabletRoute>;

    fn read_point(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        row_key: RowKey,
        read_timestamp: Timestamp,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>>;

    fn submit_command(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome>;

    /// Submit a command with its topology-independent V2 identity. The
    /// default keeps compatibility gateways source-compatible while ensuring
    /// new gateways can carry the identity through every retry hop.
    fn submit_command_with_identity(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        let _ = logical_command_id;
        self.submit_command(route, request_id, command, timeout)
    }

    /// Submit a V2 command together with the durable acknowledgement floor
    /// observed by the client. Compatibility gateways may ignore the optional
    /// watermark, but the production gateway persists it with the command.
    fn submit_command_with_identity_and_ack(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        acknowledged_through: Option<u64>,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        let _ = acknowledged_through;
        self.submit_command_with_identity(route, request_id, logical_command_id, command, timeout)
    }

    /// Resolve an indeterminate mutation by its stable logical identity. A
    /// missing outcome must remain unknown; implementations must not convert
    /// that result into permission to execute a fresh command.
    fn query_original_outcome(
        &self,
        _route: &TabletRoute,
        _request_id: RequestId,
        _logical_command_id: LogicalCommandId,
        _timeout: Duration,
    ) -> Result<Option<CachedTabletCommandOutcome>> {
        Err(Error::NotImplemented(
            "tablet logical outcome lookup is unavailable",
        ))
    }
}

/// Shared server-provided gateway used by metadata-backed SQL tables.
pub type SharedTabletGateway = Arc<dyn TabletGateway>;

/// Per-connection request identity used for tablet RPCs.
///
/// Request sequences are scoped to one client identity and carried into the
/// destination Raft group. Reads and commands use independent sequence
/// counters because only commands populate the tablet's contiguous
/// deduplication state; consuming a command sequence for a read would make the
/// first subsequent commit look like a gap to the Raft state machine.
#[derive(Debug, Clone)]
pub struct TabletRequestContext {
    client_id: u128,
    session_epoch: u64,
    next_read_sequence: u64,
    next_command_sequence: u64,
    root_request_sequence: u64,
    next_command_ordinal: u32,
    acknowledged_through: Option<u64>,
    timeout: Duration,
}

impl TabletRequestContext {
    pub fn new(client_id: u128) -> Result<Self> {
        Self::new_with_session_epoch(client_id, 1)
    }

    pub fn new_with_session_epoch(client_id: u128, session_epoch: u64) -> Result<Self> {
        if client_id == 0 {
            return Err(Error::InvalidArgument(
                "tablet request client ID 0 is reserved".to_string(),
            ));
        }
        if session_epoch == 0 {
            return Err(Error::InvalidArgument(
                "tablet request session epoch 0 is reserved".to_string(),
            ));
        }

        Ok(Self {
            client_id,
            session_epoch,
            next_read_sequence: 1,
            next_command_sequence: 1,
            root_request_sequence: 1,
            next_command_ordinal: 1,
            acknowledged_through: None,
            timeout: Duration::from_secs(30),
        })
    }

    pub fn client_id(&self) -> u128 {
        self.client_id
    }

    pub fn session_epoch(&self) -> u64 {
        self.session_epoch
    }

    pub fn acknowledged_through(&self) -> Option<u64> {
        self.acknowledged_through
    }

    pub fn reset_for_root_request(
        &mut self,
        client_id: u128,
        session_epoch: u64,
        request_sequence: u64,
    ) -> Result<()> {
        self.reset_for_root_request_with_ack(client_id, session_epoch, request_sequence, None)
    }

    pub fn reset_for_root_request_with_ack(
        &mut self,
        client_id: u128,
        session_epoch: u64,
        request_sequence: u64,
        acknowledged_through: Option<u64>,
    ) -> Result<()> {
        let mut context = Self::new_with_session_epoch(client_id, session_epoch)?;
        if request_sequence == 0 {
            return Err(Error::InvalidArgument(
                "tablet root request sequence 0 is reserved".to_string(),
            ));
        }
        context.next_read_sequence = request_sequence;
        context.next_command_sequence = request_sequence;
        context.root_request_sequence = request_sequence;
        context.next_command_ordinal = 1;
        context.acknowledged_through = acknowledged_through;
        *self = context;
        Ok(())
    }

    fn next_logical_command_id(&mut self, kind: CommandKind) -> Result<LogicalCommandId> {
        let command_ordinal = self.next_command_ordinal;
        self.next_command_ordinal = command_ordinal.checked_add(1).ok_or_else(|| {
            Error::Configuration("logical command ordinal space is exhausted".to_string())
        })?;

        Ok(LogicalCommandId {
            client_request_id: ClientRequestId {
                client_id: self.client_id,
                session_epoch: self.session_epoch,
                request_sequence: self.root_request_sequence,
            },
            command_ordinal,
            kind,
        })
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        if !timeout.is_zero() {
            self.timeout = timeout;
        }
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn next_request_id(
        &mut self,
        raft_group_id: RaftGroupId,
        sequence: &mut u64,
    ) -> Result<RequestId> {
        if raft_group_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "tablet request Raft group ID 0 is reserved".to_string(),
            ));
        }

        let current_sequence = *sequence;
        *sequence = current_sequence.checked_add(1).ok_or_else(|| {
            Error::Configuration("tablet request sequence space is exhausted".to_string())
        })?;

        Ok(RequestId {
            client_id: self.client_id,
            sequence: current_sequence,
            raft_group_id,
        })
    }

    fn next_read_request_id(&mut self, raft_group_id: RaftGroupId) -> Result<RequestId> {
        let mut sequence = self.next_read_sequence;
        let request_id = self.next_request_id(raft_group_id, &mut sequence)?;
        self.next_read_sequence = sequence;
        Ok(request_id)
    }

    fn next_command_request_id(&mut self, raft_group_id: RaftGroupId) -> Result<RequestId> {
        let mut sequence = self.next_command_sequence;
        let request_id = self.next_request_id(raft_group_id, &mut sequence)?;
        self.next_command_sequence = sequence;
        Ok(request_id)
    }
}

impl Default for TabletRequestContext {
    fn default() -> Self {
        Self::new(1).expect("default tablet request identity is non-zero")
    }
}

/// Authoritative schema and complete tablet partition map returned by metadata
/// after a committed CREATE TABLE apply.
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataTableTopology {
    pub definition: TableDefinition,
    pub tablets: Vec<TabletDescriptor>,
}

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
/// Every locally created table receives one dedicated compatibility tablet.
/// Metadata-backed routes are stored separately and are consulted by every
/// point lookup and scan; physical multi-tablet ownership remains a later
/// lifecycle concern and is never guessed from the table ID.
pub struct LocalExecutor {
    catalog: LocalCatalog,
    tablets: BTreeMap<TableId, LocalTablet>,
    tablet_routers: BTreeMap<TableId, TabletRouter>,
    metadata_table_creator: Option<SharedMetadataTableCreator>,
    tablet_gateway: Option<SharedTabletGateway>,
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
            tablet_routers: BTreeMap::new(),
            metadata_table_creator: None,
            tablet_gateway: None,
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

    /// Install the server-owned gateway used for metadata-routed tablet
    /// operations. Local compatibility tablets continue to use their direct
    /// coordinator path; only tables without a local materialized tablet cross
    /// this boundary.
    pub fn replace_tablet_gateway(&mut self, gateway: SharedTabletGateway) {
        self.tablet_gateway = Some(gateway);
    }

    pub(crate) fn metadata_table_creator_installed(&self) -> bool {
        self.metadata_table_creator.is_some()
    }

    /// Refresh the local SQL catalog cache from committed metadata state.
    ///
    /// Metadata-owned tables do not receive a local MVCC mirror. They remain
    /// visible to SQL schema analysis and `SHOW TABLES`, while point DML is
    /// routed to the assigned tablet through the installed gateway.
    pub fn refresh_metadata_catalog(&mut self) -> Result<()> {
        let Some(creator) = self.metadata_table_creator.clone() else {
            return Ok(());
        };

        for definition in creator.list_tables() {
            let table_id = TableId(definition.table_id);
            let descriptors = creator.table_descriptors(table_id)?;
            let router = self.build_metadata_router(&definition, &descriptors)?;

            if let Some(existing_router) = self.tablet_routers.get(&table_id)
                && existing_router != &router
            {
                return Err(Error::CorruptData(format!(
                    "metadata routing map for table {} changed in the local SQL cache",
                    table_id.0
                )));
            }

            let schema = self.catalog.catalog().table_by_id(table_id);

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

            self.tablet_routers.entry(table_id).or_insert(router);
            self.metadata_table_ids.insert(table_id);
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
        if operation.table_def.tablet_count != 1 {
            return Err(Error::CorruptData(format!(
                "replicated catalog table {} declares {} tablets without a metadata routing map",
                operation.table_def.table_id, operation.table_def.tablet_count
            )));
        }

        let table_id = TableId(operation.table_def.table_id);
        let router = TabletRouter::for_single_tablet(table_id, TabletId(table_id.0))?;
        if let Some(existing) = self.tablet_routers.get(&table_id)
            && existing != &router
        {
            return Err(Error::CorruptData(format!(
                "replicated catalog route conflicts with metadata routing for table {}",
                table_id.0
            )));
        }

        let schema = self
            .catalog
            .install_replicated_definition(operation.table_def.clone())?;

        if !self.tablets.contains_key(&schema.id) {
            let tablet = Tablet::new(TabletId(schema.id.0), schema.id)?;
            let coordinator =
                SingleNodeCommitCoordinator::with_participant(tablet, self.commit_log.clone())?;
            self.tablets.insert(schema.id, coordinator);
        }

        self.tablet_routers.entry(table_id).or_insert(router);
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
        self.execute_create_table_with_metadata_and_identity(plan, request_id, None, timeout)
    }

    pub fn execute_create_table_with_metadata_and_identity(
        &mut self,
        plan: CreateTablePlan,
        request_id: ragnordb_common::ids::RequestId,
        logical_request_id: Option<ClientRequestId>,
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
        let topology = creator.create_table_topology_with_identity(
            request,
            request_id,
            logical_request_id,
            timeout,
        )?;
        let definition = topology.definition;

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
        {
            return Err(Error::CorruptData(
                "metadata CREATE TABLE returned a definition different from the requested schema"
                    .to_string(),
            ));
        }

        let table_id = TableId(definition.table_id);
        let router = self.build_metadata_router(&definition, &topology.tablets)?;

        self.catalog.install_replicated_definition(definition)?;
        self.tablet_routers.insert(table_id, router);
        self.metadata_table_ids.insert(table_id);

        Ok(ExecutionResult::CreatedTable { table_id })
    }

    /// Apply a Raft-authoritative single-tablet commit to the SQL read mirror.
    ///
    /// Metadata-owned tablets are read through the tablet gateway and therefore
    /// intentionally have no local SQL mirror. Their follower-side publication
    /// is a successful no-op; legacy replicated tables still require their
    /// local coordinator so a missing mirror remains fail-closed.
    pub fn apply_replicated_commit(&mut self, command: &SingleShardCommitCommand) -> Result<usize> {
        let first_key = command.writes.first().ok_or_else(|| {
            Error::InvalidArgument("replicated commit contains no writes".to_string())
        })?;
        let first_row_key = decode_row_key(&first_key.key)?;
        let table_id = first_row_key.table_id;
        if self.metadata_table_ids.contains(&table_id) {
            return Ok(0);
        }

        let tablet_id = self.route_row_key(&first_row_key)?;
        let mut transaction = Transaction::new(command.txn_id, command.start_timestamp)?;

        for write in &command.writes {
            let write_row_key = decode_row_key(&write.key)?;
            let write_table_id = write_row_key.table_id;
            if write_table_id != table_id {
                return Err(Error::InvalidArgument(
                    "replicated single-shard commit spans multiple tables".to_string(),
                ));
            }

            if self.route_row_key(&write_row_key)? != tablet_id {
                return Err(Error::InvalidArgument(
                    "replicated single-shard commit spans multiple routed tablets".to_string(),
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

        if self.local_tablet_id(table_id)? != tablet_id {
            return Err(Error::UnsupportedSql(format!(
                "replicated commit targets tablet {}, but the local SQL mirror owns tablet {}",
                tablet_id.0,
                self.local_tablet_id(table_id)?.0
            )));
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
        let Some(schema) = self.catalog.catalog().table_by_id(table_id) else {
            return Ok(false);
        };

        if !self.tablet_routers.contains_key(&table_id) && schema.tablet_count != 1 {
            return Err(Error::CorruptData(format!(
                "table {} declares {} tablets but has no authoritative routing map",
                table_id.0, schema.tablet_count
            )));
        }

        let tablet = Tablet::with_storage(TabletId(table_id.0), table_id, storage)?;
        let coordinator =
            SingleNodeCommitCoordinator::with_participant(tablet, self.commit_log.clone())?;
        self.tablets.insert(table_id, coordinator);

        if !self.tablet_routers.contains_key(&table_id) {
            self.install_single_tablet_router(table_id)?;
        }

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
        let mut tablet_routers = BTreeMap::new();

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
            tablet_routers.insert(
                table_id,
                TabletRouter::for_single_tablet(table_id, TabletId(table_id.0)).map_err(
                    |source| {
                        Error::CorruptData(format!(
                            "failed to construct recovered tablet router for table {}: {}",
                            table_id.0, source
                        ))
                    },
                )?,
            );
        }

        Ok(Self {
            catalog: DurableCatalog::from_recovered(catalog, catalog_log),
            tablets,
            tablet_routers,
            metadata_table_creator: None,
            tablet_gateway: None,
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
        self.execute_inner(plan, transaction, None)
    }

    /// Execute one logical plan with the connection identity needed for
    /// metadata-routed tablet reads and single-tablet commits.
    pub fn execute_with_request_context(
        &mut self,
        plan: Plan,
        transaction: Option<&mut Transaction>,
        request_context: &mut TabletRequestContext,
    ) -> Result<ExecutionResult> {
        self.execute_inner(plan, transaction, Some(request_context))
    }

    fn execute_inner(
        &mut self,
        plan: Plan,
        transaction: Option<&mut Transaction>,
        request_context: Option<&mut TabletRequestContext>,
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

            Plan::Insert(plan) => self.execute_insert(
                plan,
                require_transaction(transaction, "INSERT")?,
                request_context,
            ),

            Plan::Select(plan) => self.execute_select(
                plan,
                require_transaction(transaction, "SELECT")?,
                request_context,
            ),

            Plan::Update(plan) => self.execute_update(
                plan,
                require_transaction(transaction, "UPDATE")?,
                request_context,
            ),

            Plan::Delete(plan) => self.execute_delete(
                plan,
                require_transaction(transaction, "DELETE")?,
                request_context,
            ),

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
        let mut request_context = TabletRequestContext::default();
        self.commit_transaction_outcome_with_request_context(
            transaction,
            timestamp_allocator,
            &mut request_context,
        )
    }

    /// Commit a transaction through either the local coordinator or the
    /// metadata-routed tablet gateway.
    pub fn commit_transaction_outcome_with_request_context<A>(
        &mut self,
        transaction: Transaction,
        timestamp_allocator: A,
        request_context: &mut TabletRequestContext,
    ) -> Result<SingleNodeCommitOutcome>
    where
        A: CommitTimestampAllocator,
    {
        self.commit_transaction_outcome_inner(transaction, timestamp_allocator, request_context)
    }

    fn commit_transaction_outcome_inner<A>(
        &mut self,
        transaction: Transaction,
        mut timestamp_allocator: A,
        request_context: &mut TabletRequestContext,
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
        let mut tablet_ids = BTreeSet::new();

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
            tablet_ids.insert(self.route_row_key(&row_key)?);
        }

        if table_ids.len() != 1 || tablet_ids.len() != 1 {
            return Err(Error::UnsupportedSql(
                "a local transaction may write only one routed tablet; \
                 cross-table or cross-tablet transactions require distributed coordination"
                    .to_string(),
            ));
        }

        let table_id = *table_ids
            .first()
            .expect("non-empty table-ID set was checked above");

        let tablet_id = *tablet_ids
            .first()
            .expect("non-empty tablet-ID set was checked above");

        if let Some(coordinator) = self.tablets.get_mut(&table_id) {
            if tablet_id != TabletId(table_id.0) {
                return Err(Error::UnsupportedSql(format!(
                    "tablet {} is not installed in the local single-tablet commit path",
                    tablet_id.0
                )));
            }

            let outcome = coordinator.commit(transaction, timestamp_allocator)?;

            if let Some(extent) = outcome.wal_extent {
                self.replay_from_end_lsn = self.replay_from_end_lsn.max(extent.end_lsn.as_u64());
            }

            return Ok(outcome);
        }

        let gateway = self.tablet_gateway.clone().ok_or_else(|| {
            Error::UnsupportedSql(format!(
                "tablet {} for table {} is not installed on this SQL gateway",
                tablet_id.0, table_id.0
            ))
        })?;
        let first_key = transaction
            .write_set()
            .keys()
            .next()
            .expect("non-empty transaction has a first write key");
        let first_row_key = decode_row_key(first_key)?;
        let route = gateway
            .lookup_tablet_route(first_row_key.table_id, &first_row_key.primary_key_bytes)?;
        route
            .validate()
            .map_err(|error| Error::CorruptData(error.to_string()))?;
        if route.tablet_id != tablet_id {
            return Err(Error::CorruptData(format!(
                "gateway route selected tablet {}, but SQL plan selected tablet {}",
                route.tablet_id.0, tablet_id.0
            )));
        }

        let mut writes = Vec::with_capacity(transaction.write_set().len());
        for (encoded_key, mutation) in transaction.write_set() {
            let row_key = decode_row_key(encoded_key)?;
            if row_key.table_id != table_id || self.route_row_key(&row_key)? != tablet_id {
                return Err(Error::UnsupportedSql(
                    "a remote single-tablet commit contains a foreign routed key".to_string(),
                ));
            }

            let (op, row) = match mutation {
                Mutation::Put(encoded_row) => (WriteKind::Put, Some(decode_row(encoded_row)?)),
                Mutation::Delete => (WriteKind::Delete, None),
            };
            writes.push(WriteEntry {
                key: encoded_key.clone(),
                row,
                op,
            });
        }

        let commit_timestamp =
            timestamp_allocator.finalize_commit_timestamp(transaction.start_ts())?;
        let command = TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: transaction.id(),
            start_timestamp: transaction.start_ts(),
            commit_timestamp,
            writes,
        });
        let request_id = request_context.next_command_request_id(route.raft_group_id)?;
        let logical_command_id =
            request_context.next_logical_command_id(CommandKind::SingleShardCommit)?;
        let outcome = match gateway.submit_command_with_identity_and_ack(
            &route,
            request_id.clone(),
            logical_command_id,
            request_context.acknowledged_through(),
            command,
            request_context.timeout(),
        ) {
            Ok(outcome) => outcome,
            Err(error @ Error::RequestOutcomeUnknown { .. }) => {
                match gateway.query_original_outcome(
                    &route,
                    request_id,
                    logical_command_id,
                    request_context.timeout(),
                )? {
                    Some(CachedTabletCommandOutcome::Applied(result)) => {
                        TabletCommandApplyOutcome {
                            result: result.into(),
                            deduplicated: true,
                        }
                    }
                    Some(CachedTabletCommandOutcome::Rejected(rejection)) => {
                        return Err(match rejection.kind {
                            CachedTabletCommandRejectionKind::WriteConflict => {
                                Error::WriteConflict(rejection.reason)
                            }
                            CachedTabletCommandRejectionKind::InvalidCommand
                            | CachedTabletCommandRejectionKind::UnsupportedCommand => {
                                Error::InvalidArgument(rejection.reason)
                            }
                        });
                    }
                    None => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        if !matches!(
            outcome.result,
            ragnordb_tablet::command::TabletCommandApplyResult::SingleShardCommit
        ) {
            return Err(Error::CorruptData(
                "remote tablet returned a non-commit outcome for a commit command".to_string(),
            ));
        }

        Ok(SingleNodeCommitOutcome {
            transaction_id: transaction.id(),
            commit_timestamp: Some(commit_timestamp),
            committed_writes: transaction.len(),
            wal_extent: None,
        })
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
        self.install_single_tablet_router(table_id)?;

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
        mut request_context: Option<&mut TabletRequestContext>,
    ) -> Result<ExecutionResult> {
        let InsertPlan {
            table,
            target_columns,
            rows,
        } = plan;

        let schema = self.resolve_table(&table)?;

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

        // Establish the complete routing destination before performing any
        // duplicate checks. A statement spanning tablets must fail at this
        // boundary even when one of its keys already exists locally.
        let tablet_id = self.single_destination(schema.id, prepared.iter().map(|(key, _)| key))?;

        // Check every destination before buffering any mutation. Since this
        // executor holds exclusive access during the call, a later apply pass
        // cannot observe a different local storage state.
        for (key, _) in &prepared {
            if self
                .point_row(transaction, key, &mut request_context)?
                .is_some()
            {
                return Err(Error::ConstraintViolation(format!(
                    "cannot insert duplicate primary key into table {}",
                    schema.name
                )));
            }
        }

        let affected_rows = prepared.len();

        if self.tablets.contains_key(&schema.id) {
            let tablet = self.local_tablet_for_route(schema.id, tablet_id)?;
            tablet.buffer_batch(
                transaction,
                prepared
                    .into_iter()
                    .map(|(key, row)| RowMutation::Put { key, row }),
            )?;
        } else {
            let mut writes = BTreeMap::new();
            for (key, row) in prepared {
                writes.insert(
                    ragnordb_storage::key::encode_row_key(&key)?,
                    Mutation::Put(encode_row(&row)?),
                );
            }
            transaction.buffer_batch(writes)?;
        }

        Ok(ExecutionResult::Mutation {
            operation: DmlOperation::Insert,
            affected_rows,
        })
    }

    fn execute_select(
        &self,
        plan: SelectPlan,
        transaction: &Transaction,
        request_context: Option<&mut TabletRequestContext>,
    ) -> Result<ExecutionResult> {
        let SelectPlan {
            table,
            projection,
            filter,
        } = plan;

        let schema = self.resolve_table(&table)?;

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

        let matching = self.matching_rows(
            transaction,
            schema.as_ref(),
            filter.as_ref(),
            request_context,
        )?;

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
        request_context: Option<&mut TabletRequestContext>,
    ) -> Result<ExecutionResult> {
        let UpdatePlan {
            table,
            assignments,
            filter,
        } = plan;

        let schema = self.resolve_table(&table)?;

        if assignments.is_empty() {
            return Err(Error::InvalidArgument(
                "UPDATE plan contains no assignments".to_string(),
            ));
        }

        validate_filter(schema.as_ref(), &filter)?;

        for assignment in &assignments {
            validate_update_assignment(schema.as_ref(), assignment)?;
        }

        let matching =
            self.matching_rows(transaction, schema.as_ref(), Some(&filter), request_context)?;

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
        if !prepared.is_empty() {
            let tablet_id =
                self.single_destination(schema.id, prepared.iter().map(|(key, _)| key))?;
            let mutations = prepared
                .into_iter()
                .map(|(key, row)| RowMutation::Put { key, row });
            if self.tablets.contains_key(&schema.id) {
                let tablet = self.local_tablet_for_route(schema.id, tablet_id)?;
                tablet.buffer_batch(transaction, mutations)?;
            } else {
                let mut writes = BTreeMap::new();
                for mutation in mutations {
                    let RowMutation::Put { key, row } = mutation else {
                        unreachable!("UPDATE only constructs put mutations")
                    };
                    writes.insert(
                        ragnordb_storage::key::encode_row_key(&key)?,
                        Mutation::Put(encode_row(&row)?),
                    );
                }
                transaction.buffer_batch(writes)?;
            }
        }

        Ok(ExecutionResult::Mutation {
            operation: DmlOperation::Update,
            affected_rows,
        })
    }

    fn execute_delete(
        &self,
        plan: DeletePlan,
        transaction: &mut Transaction,
        request_context: Option<&mut TabletRequestContext>,
    ) -> Result<ExecutionResult> {
        let DeletePlan { table, filter } = plan;

        let schema = self.resolve_table(&table)?;

        validate_filter(schema.as_ref(), &filter)?;

        let matching =
            self.matching_rows(transaction, schema.as_ref(), Some(&filter), request_context)?;

        // Matching completes before the atomic buffer operation, so neither a
        // filter error nor a mutation-encoding error can partially apply this
        // statement to the transaction.
        let affected_rows = matching.len();
        if !matching.is_empty() {
            let tablet_id =
                self.single_destination(schema.id, matching.iter().map(|row| &row.key))?;
            let mutations = matching
                .into_iter()
                .map(|keyed_row| RowMutation::Delete { key: keyed_row.key });
            if self.tablets.contains_key(&schema.id) {
                let tablet = self.local_tablet_for_route(schema.id, tablet_id)?;
                tablet.buffer_batch(transaction, mutations)?;
            } else {
                let mut writes = BTreeMap::new();
                for mutation in mutations {
                    let RowMutation::Delete { key } = mutation else {
                        unreachable!("DELETE only constructs delete mutations")
                    };
                    writes.insert(
                        ragnordb_storage::key::encode_row_key(&key)?,
                        Mutation::Delete,
                    );
                }
                transaction.buffer_batch(writes)?;
            }
        }

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

        Ok(schema)
    }

    fn build_metadata_router(
        &self,
        definition: &TableDefinition,
        descriptors: &[TabletDescriptor],
    ) -> Result<TabletRouter> {
        let table_id = TableId(definition.table_id);
        if definition.tablet_count == 0 {
            return Err(Error::CorruptData(format!(
                "metadata table {} declares zero tablets",
                definition.table_id
            )));
        }

        if descriptors.is_empty() {
            return Err(Error::CorruptData(format!(
                "metadata table {} returned no tablet descriptors",
                definition.table_id,
            )));
        }

        let router = TabletRouter::new(table_id, descriptors).map_err(|error| {
            Error::CorruptData(format!(
                "metadata table {} returned an invalid tablet routing map: {}",
                definition.table_id, error
            ))
        })?;

        Ok(router)
    }

    fn install_single_tablet_router(&mut self, table_id: TableId) -> Result<()> {
        let router = TabletRouter::for_single_tablet(table_id, TabletId(table_id.0))?;

        if let Some(existing) = self.tablet_routers.get(&table_id) {
            if existing != &router {
                return Err(Error::CorruptData(format!(
                    "single-tablet compatibility route conflicts with table {} metadata",
                    table_id.0
                )));
            }

            return Ok(());
        }

        self.tablet_routers.insert(table_id, router);
        Ok(())
    }

    fn router_for(&self, table_id: TableId) -> Result<&TabletRouter> {
        self.tablet_routers.get(&table_id).ok_or_else(|| {
            Error::CorruptData(format!(
                "catalog table {} has no authoritative tablet routing map",
                table_id.0
            ))
        })
    }

    fn route_row_key(&self, row_key: &RowKey) -> Result<TabletId> {
        self.router_for(row_key.table_id)?
            .route_point(&row_key.primary_key_bytes)
    }

    fn local_tablet_id(&self, table_id: TableId) -> Result<TabletId> {
        self.tablets
            .get(&table_id)
            .map(|_| TabletId(table_id.0))
            .ok_or_else(|| {
                Error::CorruptData(format!(
                    "catalog table {} has no local commit coordinator",
                    table_id.0
                ))
            })
    }

    fn local_tablet_for_route(&self, table_id: TableId, tablet_id: TabletId) -> Result<&Tablet> {
        let local_tablet_id = self.local_tablet_id(table_id)?;
        if tablet_id != local_tablet_id {
            return Err(Error::UnsupportedSql(format!(
                "tablet {} for table {} is not installed on this local executor",
                tablet_id.0, table_id.0
            )));
        }

        self.tablets
            .get(&table_id)
            .map(SingleNodeCommitCoordinator::participant)
            .ok_or_else(|| {
                Error::CorruptData(format!(
                    "catalog table {} has no local commit coordinator",
                    table_id.0
                ))
            })
    }

    fn single_destination<'a, I>(&self, table_id: TableId, keys: I) -> Result<TabletId>
    where
        I: IntoIterator<Item = &'a RowKey>,
    {
        let mut destinations = BTreeSet::new();
        for key in keys {
            if key.table_id != table_id {
                return Err(Error::CorruptData(format!(
                    "row key belongs to table {}, expected table {}",
                    key.table_id.0, table_id.0
                )));
            }

            destinations.insert(self.route_row_key(key)?);
        }

        match destinations.len() {
            0 => self.local_tablet_id(table_id),
            1 => Ok(*destinations
                .first()
                .expect("single destination set contains one tablet")),
            _ => Err(Error::UnsupportedSql(
                "one local statement may buffer mutations for only one routed tablet; \
                 distributed transaction coordination is required for a multi-tablet mutation"
                    .to_string(),
            )),
        }
    }

    fn matching_rows(
        &self,
        transaction: &Transaction,
        schema: &TableSchema,
        filter: Option<&BoundExpr>,
        mut request_context: Option<&mut TabletRequestContext>,
    ) -> Result<Vec<KeyedRow>> {
        let candidates = match choose_access_path(schema, filter)? {
            AccessPath::Empty => Vec::new(),

            AccessPath::Point(key) => self
                .point_row(transaction, &key, &mut request_context)?
                .map(|row| vec![(key, row)])
                .unwrap_or_default(),

            AccessPath::Scan => {
                if !self.tablets.contains_key(&schema.id) {
                    return Err(Error::UnsupportedSql(
                        "distributed scans are not available in this routing slice".to_string(),
                    ));
                }
                let tablet_ids = self.router_for(schema.id)?.route_scan();
                let mut rows = Vec::new();

                for tablet_id in tablet_ids {
                    let tablet = self.local_tablet_for_route(schema.id, tablet_id)?;

                    for (key, row) in tablet.scan(transaction, None, None)? {
                        if self.route_row_key(&key)? != tablet_id {
                            return Err(Error::CorruptData(format!(
                                "tablet {} returned row key routed to another tablet for table {}",
                                tablet_id.0, schema.id.0
                            )));
                        }

                        rows.push((key, row));
                    }
                }

                rows
            }
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

    /// Read one row from the local compatibility tablet or the server gateway,
    /// overlaying a transaction's own pending mutation before consulting
    /// committed remote state.
    fn point_row(
        &self,
        transaction: &Transaction,
        row_key: &RowKey,
        request_context: &mut Option<&mut TabletRequestContext>,
    ) -> Result<Option<Row>> {
        let encoded_key = ragnordb_storage::key::encode_row_key(row_key)?;
        if let Some(mutation) = transaction.pending_write(&encoded_key) {
            return match mutation {
                Mutation::Put(encoded_row) => decode_row(encoded_row).map(Some),
                Mutation::Delete => Ok(None),
            };
        }

        let tablet_id = self.route_row_key(row_key)?;
        if self.tablets.contains_key(&row_key.table_id) {
            return self
                .local_tablet_for_route(row_key.table_id, tablet_id)?
                .get(transaction, row_key);
        }

        let gateway = self.tablet_gateway.clone().ok_or_else(|| {
            Error::UnsupportedSql(format!(
                "tablet {} for table {} is not installed on this SQL gateway",
                tablet_id.0, row_key.table_id.0
            ))
        })?;
        let route = gateway.lookup_tablet_route(row_key.table_id, &row_key.primary_key_bytes)?;
        route
            .validate()
            .map_err(|error| Error::CorruptData(error.to_string()))?;
        if route.tablet_id != tablet_id {
            return Err(Error::CorruptData(format!(
                "gateway route selected tablet {}, but SQL plan selected tablet {}",
                route.tablet_id.0, tablet_id.0
            )));
        }

        let request_context = match request_context {
            Some(context) => &mut **context,
            None => {
                return Err(Error::InvalidArgument(
                    "metadata-routed tablet access requires a request identity".to_string(),
                ));
            }
        };
        let request_id = request_context.next_read_request_id(route.raft_group_id)?;
        gateway
            .read_point(
                &route,
                request_id,
                row_key.clone(),
                transaction.start_ts(),
                request_context.timeout(),
            )?
            .map(|encoded_row| decode_row(&encoded_row))
            .transpose()
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
    use ragnordb_common::{
        catalog_codec::{ColumnDefinition, TableDefinition},
        command_codec::TabletCommand,
        ids::{NodeId, RaftGroupId, ReplicaId, RequestId, TxnId},
        metadata_codec::PartitionSpec,
        rpc_codec::{ReplicaRoute, TabletRoute},
    };
    use ragnordb_sql::{analyze, parse_one, plan};
    use ragnordb_tablet::command::TabletCommandApplyResult;
    use ragnordb_txn::{LocalTransactionManager, TransactionManager};

    struct StaticMetadata {
        definition: TableDefinition,
        descriptor: TabletDescriptor,
    }

    impl MetadataTableCreator for StaticMetadata {
        fn create_table(
            &self,
            _request: CreateTableRequest,
            _request_id: RequestId,
            _timeout: Duration,
        ) -> Result<TableDefinition> {
            Err(Error::NotImplemented("static test metadata is read-only"))
        }

        fn table_descriptors(&self, table_id: TableId) -> Result<Vec<TabletDescriptor>> {
            if table_id != TableId(self.definition.table_id) {
                return Err(Error::SchemaMismatch("unknown test table".to_string()));
            }
            Ok(vec![self.descriptor.clone()])
        }

        fn list_tables(&self) -> Vec<TableDefinition> {
            vec![self.definition.clone()]
        }
    }

    struct RecordingGateway {
        route: TabletRoute,
        row_key: Vec<u8>,
        row: Vec<u8>,
        read_requests: Mutex<Vec<RequestId>>,
        commands: Mutex<Vec<(RequestId, TabletCommand)>>,
    }

    impl TabletGateway for RecordingGateway {
        fn lookup_tablet_route(&self, _table_id: TableId, _key: &[u8]) -> Result<TabletRoute> {
            Ok(self.route.clone())
        }

        fn read_point(
            &self,
            _route: &TabletRoute,
            request_id: RequestId,
            row_key: RowKey,
            _read_timestamp: Timestamp,
            _timeout: Duration,
        ) -> Result<Option<Vec<u8>>> {
            self.read_requests.lock().unwrap().push(request_id);
            let encoded = ragnordb_storage::key::encode_row_key(&row_key)?;
            Ok((encoded == self.row_key).then(|| self.row.clone()))
        }

        fn submit_command(
            &self,
            _route: &TabletRoute,
            request_id: RequestId,
            command: TabletCommand,
            _timeout: Duration,
        ) -> Result<TabletCommandApplyOutcome> {
            self.commands.lock().unwrap().push((request_id, command));
            Ok(TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::SingleShardCommit,
                deduplicated: false,
            })
        }
    }

    fn remote_executor() -> (LocalExecutor, Arc<RecordingGateway>, RowKey) {
        let table_id = TableId(42);
        let tablet_id = TabletId(142);
        let row_key = RowKey {
            table_id,
            primary_key_bytes: make_row_key(table_id, &[Value::Int(7)])
                .unwrap()
                .primary_key_bytes,
        };
        let descriptor = TabletDescriptor {
            tablet_id,
            table_id,
            raft_group_id: RaftGroupId(242),
            tablet_epoch: 1,
            partition: PartitionSpec::Hash {
                bucket: 0,
                bucket_count: 1,
            },
        };
        let definition = TableDefinition {
            table_id: table_id.0,
            name: "remote_users".to_string(),
            columns: vec![
                ColumnDefinition {
                    column_id: ColumnId(1),
                    name: "id".to_string(),
                    ty: DataType::Int,
                    nullable: false,
                },
                ColumnDefinition {
                    column_id: ColumnId(2),
                    name: "name".to_string(),
                    ty: DataType::Text,
                    nullable: false,
                },
            ],
            primary_key_column_ids: vec![ColumnId(1)],
            schema_version: 1,
            tablet_count: 1,
        };
        let route = TabletRoute {
            raft_group_id: descriptor.raft_group_id,
            tablet_id,
            tablet_epoch: descriptor.tablet_epoch,
            leader_replica_id: ReplicaId(1),
            replicas: vec![ReplicaRoute {
                replica_id: ReplicaId(1),
                node_id: NodeId(9),
            }],
        };
        let row = Row {
            values: vec![Value::Int(7), Value::Text("alice".to_string())],
        };
        let gateway = Arc::new(RecordingGateway {
            route,
            row_key: ragnordb_storage::key::encode_row_key(&row_key).unwrap(),
            row: encode_row(&row).unwrap(),
            read_requests: Mutex::new(Vec::new()),
            commands: Mutex::new(Vec::new()),
        });
        let mut executor = LocalExecutor::new();
        executor.replace_metadata_table_creator(Arc::new(StaticMetadata {
            definition,
            descriptor,
        }));
        executor.refresh_metadata_catalog().unwrap();
        executor.replace_tablet_gateway(gateway.clone());
        (executor, gateway, row_key)
    }

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

    /// Realistic bug caught: metadata-backed tables were visible to SQL
    /// analysis but a point read still attempted to access a missing local
    /// coordinator instead of using the gateway route.
    #[test]
    fn metadata_point_select_reads_through_tablet_gateway() {
        let (mut executor, gateway, _) = remote_executor();
        let plan = build(&executor, "SELECT name FROM remote_users WHERE id = 7");
        let mut transaction = Transaction::new(TxnId(1), Timestamp(10)).unwrap();
        let mut context = TabletRequestContext::new(77).unwrap();

        let result = executor
            .execute_with_request_context(plan, Some(&mut transaction), &mut context)
            .unwrap();

        let ExecutionResult::Query(result) = result else {
            panic!("expected a query result");
        };
        assert_eq!(
            result.rows,
            vec![Row {
                values: vec![Value::Text("alice".to_string())],
            }]
        );
        assert_eq!(gateway.read_requests.lock().unwrap().len(), 1);
        assert_eq!(gateway.read_requests.lock().unwrap()[0].client_id, 77);
    }

    /// Realistic bug caught: a single-tablet mutation on a remote metadata
    /// route could be buffered locally but had no commit path, causing the
    /// gateway to reject the transaction after SQL had accepted the write.
    #[test]
    fn metadata_single_table_insert_submits_one_replicated_command() {
        let (mut executor, gateway, _) = remote_executor();
        let plan = build(
            &executor,
            "INSERT INTO remote_users (id, name) VALUES (8, 'alice')",
        );
        let mut manager = LocalTransactionManager::new();
        let mut transaction = manager.begin_transaction().unwrap();
        let mut context = TabletRequestContext::new(88).unwrap();

        let result = executor
            .execute_with_request_context(plan, Some(&mut transaction), &mut context)
            .unwrap();
        assert_eq!(
            result,
            ExecutionResult::Mutation {
                operation: DmlOperation::Insert,
                affected_rows: 1,
            }
        );

        let outcome = executor
            .commit_transaction_outcome_with_request_context(
                transaction,
                &mut manager,
                &mut context,
            )
            .unwrap();
        assert_eq!(outcome.committed_writes, 1);
        assert_eq!(gateway.commands.lock().unwrap().len(), 1);
        let (request_id, command) = &gateway.commands.lock().unwrap()[0];
        assert_eq!(request_id.client_id, 88);
        assert_eq!(request_id.sequence, 1);
        assert_eq!(request_id.raft_group_id, RaftGroupId(242));
        assert!(matches!(command, TabletCommand::SingleShardCommit(_)));
    }

    /// Realistic bug caught: two logical tablet commands emitted by one SQL
    /// request reused the transport sequence as their durable identity,
    /// allowing a retry of the second command to collide with the first.
    #[test]
    fn logical_command_ordinals_share_one_root_request_identity() {
        let mut context = TabletRequestContext::new_with_session_epoch(91, 4).unwrap();
        context.reset_for_root_request(91, 4, 37).unwrap();

        let first = context
            .next_logical_command_id(CommandKind::SingleShardCommit)
            .unwrap();
        let second = context
            .next_logical_command_id(CommandKind::SingleShardCommit)
            .unwrap();

        assert_eq!(first.client_request_id, second.client_request_id);
        assert_eq!(first.client_request_id.request_sequence, 37);
        assert_eq!(first.command_ordinal, 1);
        assert_eq!(second.command_ordinal, 2);
    }

    /// Realistic bug caught: a metadata-owned tablet has no local SQL mirror,
    /// so follower-side derived-state publication must not reject a committed
    /// Raft command merely because there is no local coordinator to update.
    #[test]
    fn metadata_replicated_commit_does_not_require_a_local_sql_mirror() {
        let (mut executor, _, row_key) = remote_executor();
        let encoded_key = ragnordb_storage::key::encode_row_key(&row_key).unwrap();
        let command = SingleShardCommitCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(1),
            commit_timestamp: Timestamp(2),
            writes: vec![WriteEntry {
                key: encoded_key,
                row: Some(Row {
                    values: vec![Value::Int(7), Value::Text("alice".to_string())],
                }),
                op: WriteKind::Put,
            }],
        };

        assert_eq!(executor.apply_replicated_commit(&command).unwrap(), 0);
    }

    /// Realistic bug caught: a point-read sequence was previously counted as a
    /// command sequence, so implicit autocommit sent its first commit with
    /// sequence two and the tablet rejected it as a gap.
    #[test]
    fn implicit_remote_commit_reuses_connection_request_identity() {
        let (mut executor, gateway, _) = remote_executor();
        let mut session = SqlSession::with_client_id(99);
        let mut manager = LocalTransactionManager::new();

        let result = session
            .execute_sql(
                "INSERT INTO remote_users (id, name) VALUES (8, 'alice')",
                &mut executor,
                &mut manager,
            )
            .unwrap();

        assert!(matches!(
            result,
            ExecutionResult::Mutation {
                affected_rows: 1,
                ..
            }
        ));
        let reads = gateway.read_requests.lock().unwrap();
        let commands = gateway.commands.lock().unwrap();
        assert_eq!(reads.len(), 1);
        assert_eq!(commands.len(), 1);
        assert_eq!(reads[0].client_id, 99);
        assert_eq!(commands[0].0.client_id, 99);
        assert_eq!(reads[0].sequence, 1);
        assert_eq!(commands[0].0.sequence, 1);
    }
}
