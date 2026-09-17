//! server owned local database runtime
//!
//! a running node owns exactly one executor, transaction manager, WAL adapter,
//! and checkpoint publication coordinator. Every connection shares this runtime,
//! while each connection retains its own `SqlSession` and explicit transaction
//!
//! startup recovery reconstructs catalog, MVCC, allocator, and WAL-writer state
//! privately. No client-facing runtime is returned until every recovery boundary
//! has succeeded

use std::{
    collections::BTreeMap,
    fmt, fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::data_directory_lock::DataDirectoryLock;
use raft::types::ConfState;
use ragnordb_catalog::Catalog;
use ragnordb_common::{
    Error, Result,
    command_codec::SingleShardCommitCommand,
    durability::DurabilityGate,
    ids::{NodeId, RequestId},
    proto::snapshot as snapshot_proto,
};
use ragnordb_exec::{
    ExecutionResult, LocalExecutor, QueryResultSink, QueryStreamSummary, SharedCatalogLog,
    SharedCommitLog, SharedMetadataTableCreator, SharedTabletGateway, SqlSession,
};
use ragnordb_multiraft::storage::persistence::NodeRaftWal;
use ragnordb_multiraft::storage::{
    codec::RaftReplicaIdentity, recovery::RecoveredRaftStorage,
    shared_recovery::recover_shared_storage_from_state,
};
use ragnordb_sql::{Plan, analyze, parse_one, plan};
use ragnordb_storage::{
    checkpoint::{
        PublishedCheckpoint, cleanup_orphan_snapshot_files,
        publish_checkpoint as publish_checkpoint_metadata, publish_snapshot_file,
    },
    recovery::{
        RecoveryState, load_recovery_checkpoint, replay_recovery_stream_from_state,
        scan_recovery_records, select_recovery_checkpoint,
    },
    wal::{CheckpointRetentionPin, RagnorDbWalAdapter},
};
use ragnordb_txn::{LocalTransactionManager, TransactionManager};
use tokio::sync::{Mutex, Semaphore};
use wal::{
    config::WalConfig,
    io::directory::FsSegmentDirectory,
    lsn::Lsn,
    types::WalIdentity,
    wal::{WalHandle, report::RecoveryReport},
};

/// concrete local WAL adapter shared by transaction, catalog, checkpoint, and
/// recovery-owned runtime paths
type LocalWalAdapter = RagnorDbWalAdapter<FsSegmentDirectory, ()>;

/// shared server-wide local database runtime
pub type SharedLocalDatabase = Arc<Mutex<LocalDatabase>>;

/// Shared distributed SQL services. The service owns independent boundaries
/// for immutable executor/catalog reads, transaction identity allocation, and
/// bounded statement admission. It deliberately does not expose the
/// compatibility `LocalDatabase` mutex to request handlers.
pub type SharedDatabaseServices = Arc<DatabaseServices>;

/// Distributed SQL execution services split out of the serialized local
/// compatibility runtime.
pub struct DatabaseServices {
    executor: Arc<RwLock<LocalExecutor>>,
    transaction_manager: Arc<StdMutex<LocalTransactionManager>>,
    durability_gate: DurabilityGate,
    statement_permits: Arc<Semaphore>,
    published_view_generation: AtomicU64,
}

impl fmt::Debug for DatabaseServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabaseServices")
            .field(
                "statement_permits",
                &self.statement_permits.available_permits(),
            )
            .field(
                "published_view_generation",
                &self.published_view_generation.load(Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}

impl DatabaseServices {
    /// Acquire one bounded statement slot. The timeout is part of the
    /// statement deadline, so overload is reported as a normal SQL timeout
    /// instead of creating unbounded blocking-pool work.
    pub async fn acquire_statement(
        self: &Arc<Self>,
        timeout: std::time::Duration,
    ) -> Result<tokio::sync::OwnedSemaphorePermit> {
        tokio::time::timeout(timeout, self.statement_permits.clone().acquire_owned())
            .await
            .map_err(|_| Error::StatementTimeout {
                timeout_ms: timeout.as_millis().try_into().unwrap_or(u64::MAX),
            })?
            .map_err(|_| Error::Configuration("database statement service is closed".to_string()))
    }

    fn refresh_metadata_catalog(&self) -> Result<()> {
        match self.executor.try_write() {
            Ok(mut executor) => {
                if executor.refresh_metadata_catalog_changed()? {
                    self.published_view_generation
                        .fetch_add(1, Ordering::AcqRel);
                }
                Ok(())
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                // An in-flight statement holds a stable read view. Deferring a
                // metadata publication keeps unrelated tablet requests from
                // waiting behind that statement; the next request retries the
                // short refresh section.
                Ok(())
            }
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                if poisoned.into_inner().refresh_metadata_catalog_changed()? {
                    self.published_view_generation
                        .fetch_add(1, Ordering::AcqRel);
                }
                Ok(())
            }
        }
    }

    /// Return the generation pinned by the next shared executor read section.
    /// A generation changes only after a complete schema/routing publication.
    pub fn published_view_generation(&self) -> u64 {
        self.published_view_generation.load(Ordering::Acquire)
    }

    fn plan_statement(&self, sql: &str) -> Result<Plan> {
        let executor = self
            .executor
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let parsed = parse_one(sql)?;
        let bound = analyze(&parsed, executor.catalog())?;
        Ok(plan(bound))
    }

    /// Execute one distributed SQL statement after the frontend has acquired a
    /// bounded statement permit.
    ///
    /// Catalog refresh/publication uses a short write section. Planning and
    /// data execution use a shared immutable executor view; only the
    /// compatibility local-table commit path takes mutable ownership. Remote
    /// Raft/RPC waits therefore never hold the node-global `LocalDatabase`
    /// mutex.
    pub fn execute_sql(
        &self,
        session: &mut SqlSession,
        sql: &str,
        metadata_request_id: Option<RequestId>,
        logical_request_id: Option<ragnordb_common::ids::ClientRequestId>,
        metadata_timeout: std::time::Duration,
    ) -> Result<ExecutionResult> {
        self.durability_gate.ensure_healthy()?;
        self.refresh_metadata_catalog()?;
        let plan = self.plan_statement(sql)?;

        let result = match plan {
            Plan::Begin => {
                let mut transaction_manager = self
                    .transaction_manager
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                session.begin_with_transaction_manager(&mut *transaction_manager)
            }
            Plan::Rollback => session.rollback_current_transaction(),
            Plan::ShowTables => {
                let executor = self
                    .executor
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                executor.execute_show_tables_read_only()
            }
            Plan::CreateTable(create_plan) => {
                if session.has_active_transaction() {
                    Err(Error::InvalidArgument(
                        "CREATE TABLE is autocommit-only and must not receive a transaction context"
                            .to_string(),
                    ))
                } else {
                    let request_id = metadata_request_id.ok_or_else(|| {
                        Error::InvalidArgument(
                            "distributed CREATE TABLE requires a metadata request identity"
                                .to_string(),
                        )
                    })?;
                    let topology = {
                        let executor = self
                            .executor
                            .read()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        executor.prepare_create_table_with_metadata_and_identity(
                            create_plan,
                            request_id,
                            logical_request_id,
                            metadata_timeout,
                        )?
                    };
                    let mut executor = self
                        .executor
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    executor.install_metadata_table_topology(topology)
                }
            }
            Plan::Commit => {
                let transaction = session.take_transaction_for_commit()?;
                let outcome =
                    self.commit_transaction(transaction, session.request_context_mut())?;
                Ok(ExecutionResult::TransactionCommitted {
                    transaction_id: outcome.transaction_id,
                    commit_ts: outcome.commit_timestamp,
                    committed_writes: outcome.committed_writes,
                })
            }
            data_plan @ (Plan::Insert(_) | Plan::Select(_) | Plan::Update(_) | Plan::Delete(_)) => {
                if session.has_active_transaction() {
                    let executor = self
                        .executor
                        .read()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    session.execute_data_plan_with_shared_executor(data_plan, &executor)
                } else {
                    let mut transaction = {
                        let mut transaction_manager = self
                            .transaction_manager
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        transaction_manager.begin_transaction()?
                    };
                    let statement_result = {
                        let executor = self
                            .executor
                            .read()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        executor.execute_data_plan_with_request_context(
                            data_plan,
                            &mut transaction,
                            session.request_context_mut(),
                        )
                    }?;
                    let _ = self.commit_transaction(transaction, session.request_context_mut())?;
                    Ok(statement_result)
                }
            }
        };

        if let Err(error) = &result {
            self.durability_gate.observe_error(error);
        }
        result
    }

    fn commit_transaction(
        &self,
        transaction: ragnordb_txn::Transaction,
        request_context: &mut ragnordb_exec::TabletRequestContext,
    ) -> Result<ragnordb_txn::SingleNodeCommitOutcome> {
        if transaction.is_empty() {
            return Ok(ragnordb_txn::SingleNodeCommitOutcome {
                transaction_id: transaction.id(),
                commit_timestamp: None,
                committed_writes: 0,
                wal_extent: None,
            });
        }

        let commit_timestamp = {
            let mut transaction_manager = self
                .transaction_manager
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            transaction_manager.allocate_commit_timestamp(transaction.start_ts())?
        };

        let executor = self
            .executor
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if executor.transaction_targets_local_table(&transaction)? {
            drop(executor);
            let mut executor = self
                .executor
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            executor.commit_transaction_outcome(transaction, commit_timestamp)
        } else {
            executor.commit_remote_transaction_outcome_with_timestamp(
                transaction,
                commit_timestamp,
                request_context,
            )
        }
    }

    /// Execute a bounded streaming SELECT through the same split ownership
    /// contract as materialized statements.
    pub fn execute_sql_streaming(
        &self,
        session: &mut SqlSession,
        sql: &str,
        sink: &mut dyn QueryResultSink,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<QueryStreamSummary> {
        self.durability_gate.ensure_healthy()?;
        self.refresh_metadata_catalog()?;
        let plan = self.plan_statement(sql)?;
        if !matches!(plan, Plan::Select(_)) {
            return Err(Error::UnsupportedSql(
                "streaming result mode currently supports SELECT only".to_string(),
            ));
        }

        if session.has_active_transaction() {
            let executor = self
                .executor
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            session.execute_select_streaming_with_shared_executor(
                plan, &executor, sink, max_rows, max_bytes,
            )
        } else {
            let mut transaction = {
                let mut transaction_manager = self
                    .transaction_manager
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                transaction_manager.begin_transaction()?
            };
            let summary = {
                let executor = self
                    .executor
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                executor.execute_select_streaming(
                    plan,
                    &mut transaction,
                    session.request_context_mut(),
                    sink,
                    max_rows,
                    max_bytes,
                )?
            };
            let _ = self.commit_transaction(transaction, session.request_context_mut())?;
            Ok(summary)
        }
    }
}

/// Point-in-time storage state exposed through administrative health endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseStatus {
    pub durable_lsn: u64,
    pub replay_frontier: u64,
    pub latest_checkpoint_id: Option<u64>,
    pub wal_retained_bytes: u64,
    pub retention_pins_active: usize,
    pub oldest_retention_pin_lsn: Option<u64>,
    pub wal_last_append_nanos: u64,
    pub wal_last_sync_nanos: u64,
}

/// result returned only after the complete live checkpoint workflow succeeds
#[must_use = "checkpoint publication contains the durable and retention boundaries"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveCheckpointPublication {
    /// snapshot pointer and matching marker proven durable by WAL
    pub checkpoint: PublishedCheckpoint,

    /// retention floor installed after the checkpoint marker became durable
    pub retention_advanced_to: Lsn,

    /// sealed WAL segments physically removed during this advancement
    ///
    /// active readers may keep this value at zero even though the retention floor
    /// was advanced successfully
    pub pruned_segments: usize,
}

/// in memory state owned by one running local database node
///
/// the executor and transaction manager must remain paired. Constructing either
/// per connection would isolate catalog/tablet state or reuse transaction IDs and
/// MVCC timestamps
///
/// checkpoint publication has its own mutex because the database state lock is
/// deliberately released while the immutable snapshot file is written. This
/// permits later commits to proceed without allowing two checkpoints to publish
/// out of order
pub struct LocalDatabase {
    executor: Arc<RwLock<LocalExecutor>>,
    transaction_manager: Arc<StdMutex<LocalTransactionManager>>,
    next_snapshot_id: Option<u64>,
    data_dir: Option<PathBuf>,
    checkpoint_adapter: Option<Arc<LocalWalAdapter>>,
    checkpoint_publication_lock: Arc<Mutex<()>>,
    data_directory_lock: Option<DataDirectoryLock>,
    durability_gate: DurabilityGate,
    latest_checkpoint_id: Option<u64>,
    checkpoint_replay_frontier: Lsn,
    node_wal: Option<NodeRaftWal<WalHandle<FsSegmentDirectory, ()>>>,
}

impl fmt::Debug for LocalDatabase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDatabase")
            .field("executor", &self.executor)
            .field("transaction_manager", &self.transaction_manager)
            .field("next_snapshot_id", &self.next_snapshot_id)
            .field("data_dir", &self.data_dir)
            .field("has_checkpoint_adapter", &self.checkpoint_adapter.is_some())
            .field("owns_data_directory", &self.data_directory_lock.is_some())
            .finish_non_exhaustive()
    }
}

impl Default for LocalDatabase {
    fn default() -> Self {
        Self {
            executor: Arc::new(RwLock::new(LocalExecutor::default())),
            transaction_manager: Arc::new(StdMutex::new(LocalTransactionManager::default())),
            next_snapshot_id: Some(1),
            data_dir: None,
            checkpoint_adapter: None,
            checkpoint_publication_lock: Arc::new(Mutex::new(())),
            data_directory_lock: None,
            durability_gate: DurabilityGate::new(),
            latest_checkpoint_id: None,
            checkpoint_replay_frontier: Lsn::ZERO,
            node_wal: None,
        }
    }
}

impl LocalDatabase {
    /// construct an in memory runtime for tests that do not exercise durability
    pub fn new() -> Self {
        Self::default()
    }

    /// recover a complete local runtime from the node's A-WAL directory
    ///
    /// A-WAL first establishes and repairs its physically valid prefix. RagnorDB
    /// then selects and loads the newest published checkpoint, when one exists,
    /// replays its exact WAL suffix into private state, and publishes the runtime
    /// only after every recovery boundary succeeds
    pub fn recover(data_dir: impl AsRef<Path>, node_id: NodeId) -> Result<(Self, RecoveryReport)> {
        let (database, report, _) = Self::recover_internal(data_dir, node_id, None, None)?;
        Ok((database, report))
    }

    /// Recover database and Raft semantic state from one retained A-WAL cursor.
    /// Database records below a selected checkpoint are skipped while Raft
    /// records remain visible to their independent recovery owner.
    pub fn recover_shared_with_raft(
        data_dir: impl AsRef<Path>,
        node_id: NodeId,
        configurations: &BTreeMap<RaftReplicaIdentity, ConfState>,
    ) -> Result<(Self, RecoveryReport, RecoveredRaftStorage)> {
        let (database, report, raft) =
            Self::recover_internal(data_dir, node_id, Some(configurations), None)?;
        Ok((
            database,
            report,
            raft.expect("shared recovery always returns Raft state"),
        ))
    }

    /// Recover a replicated runtime while consuming an exclusive directory
    /// guard already acquired by the physical server startup path.
    ///
    /// This permits bootstrap discovery and WAL recovery to occur under one
    /// continuous process-ownership lifetime.
    pub fn recover_shared_with_raft_with_lock(
        data_dir: impl AsRef<Path>,
        node_id: NodeId,
        configurations: &BTreeMap<RaftReplicaIdentity, ConfState>,
        data_directory_lock: DataDirectoryLock,
    ) -> Result<(Self, RecoveryReport, RecoveredRaftStorage)> {
        let (database, report, raft) = Self::recover_internal(
            data_dir,
            node_id,
            Some(configurations),
            Some(data_directory_lock),
        )?;

        Ok((
            database,
            report,
            raft.expect("shared recovery always returns Raft state"),
        ))
    }

    fn recover_internal(
        data_dir: impl AsRef<Path>,
        node_id: NodeId,
        configurations: Option<&BTreeMap<RaftReplicaIdentity, ConfState>>,
        preacquired_lock: Option<DataDirectoryLock>,
    ) -> Result<(Self, RecoveryReport, Option<RecoveredRaftStorage>)> {
        if node_id.0 == 0 {
            return Err(Error::Configuration(
                "node ID 0 cannot identify a recovery WAL".to_string(),
            ));
        }

        let data_dir = data_dir.as_ref().to_path_buf();

        // the directory must exist before its persistent lock file can be opened
        // ownership is acquired before A-WAL is opened, preventing two database
        // runtimes or an offline inspector from overlapping storage lifetimes
        fs::create_dir_all(&data_dir).map_err(|source| Error::RecoveryFailed {
            reason: format!(
                "failed to create data directory {}: {}",
                data_dir.display(),
                source
            ),
        })?;

        let data_directory_lock = match preacquired_lock {
            Some(lock) => {
                if lock.data_dir() != data_dir.as_path() {
                    return Err(Error::Configuration(format!(
                        "pre-acquired data-directory lock protects {}, \
                         but recovery requested {}",
                        lock.data_dir().display(),
                        data_dir.display(),
                    )));
                }

                lock
            }

            None => DataDirectoryLock::acquire(&data_dir)?,
        };
        let wal_dir = data_dir.join("wal");

        fs::create_dir_all(&wal_dir).map_err(|source| Error::RecoveryFailed {
            reason: format!(
                "failed to create WAL directory {}: {}",
                wal_dir.display(),
                source
            ),
        })?;

        let wal_config = WalConfig {
            dir: wal_dir.clone(),
            identity: WalIdentity::new(node_id.0, 1, 1),
            ..WalConfig::default()
        };

        let directory = FsSegmentDirectory::new(wal_dir);

        let (wal, recovery_report) =
            WalHandle::open(directory, wal_config, ()).map_err(|source| Error::RecoveryFailed {
                reason: format!("failed to open and physically recover A-WAL: {source}"),
            })?;

        // recovery begins at the first retained record rather than assuming that
        // the WAL prefix still begins at zero
        let first_retained_lsn = recovery_report.first_lsn.unwrap_or(Lsn::ZERO);

        let recovery_pin = wal
            .acquire_retention_pin("ragnordb-startup-recovery", first_retained_lsn)
            .map_err(|source| Error::RecoveryFailed {
                reason: format!(
                    "failed to pin WAL retention at startup LSN {}: {source}",
                    first_retained_lsn.as_u64()
                ),
            })?;

        let checkpoint_stream = scan_recovery_records(&wal, first_retained_lsn)?;
        let checkpoint_candidate = select_recovery_checkpoint(checkpoint_stream)?;
        let selected_snapshot_path = checkpoint_candidate
            .as_ref()
            .map(|candidate| candidate.pointer.relative_path.clone());
        let selected_snapshot_id = checkpoint_candidate
            .as_ref()
            .map(|candidate| candidate.pointer.snapshot_id);

        let (recovered_state, replay_from_lsn) = match checkpoint_candidate {
            Some(candidate) => {
                // loading validates the snapshot envelope, checksum, identity,
                // metadata, allocator maxima, and replay boundary before state is
                // accepted
                let loaded = load_recovery_checkpoint(&data_dir, &candidate)?;
                (loaded.state, loaded.replay_from_lsn)
            }

            None if first_retained_lsn != Lsn::ZERO => {
                return Err(Error::RecoveryFailed {
                    reason: format!(
                        "retained WAL begins at LSN {}, but no published \
                         checkpoint can reconstruct the pruned prefix",
                        first_retained_lsn.as_u64()
                    ),
                });
            }

            None => (RecoveryState::new(), Lsn::ZERO),
        };

        let (recovered_state, recovered_raft) = match configurations {
            Some(configurations) => {
                let mut replay_stream =
                    wal.iter_from(first_retained_lsn)
                        .map_err(|source| Error::RecoveryFailed {
                            reason: format!("open shared database/Raft recovery stream: {source}"),
                        })?;
                let recovered = recover_shared_storage_from_state(
                    &mut replay_stream,
                    recovered_state,
                    replay_from_lsn,
                    configurations,
                )
                .map_err(|source| Error::RecoveryFailed {
                    reason: source.to_string(),
                })?;
                (recovered.database, Some(recovered.raft))
            }
            None => {
                let replay_stream = scan_recovery_records(&wal, replay_from_lsn)?;
                (
                    replay_recovery_stream_from_state(replay_stream, recovered_state)?,
                    None,
                )
            }
        };

        // Cleanup follows complete snapshot validation and WAL-suffix replay.
        // Until this point every final file may still be the only recoverable
        // image for a candidate discovered during startup.
        cleanup_orphan_snapshot_files(&data_dir, selected_snapshot_path.as_deref())?;

        let high_water_marks = recovered_state.high_water_marks();
        let floors = high_water_marks.checked_allocator_floors()?;

        let (mut catalog, mvcc_by_table, recovered_marks) = recovered_state.into_parts();

        catalog.restore_table_id_floor(floors.next_table_id)?;

        let transaction_manager = LocalTransactionManager::from_recovered_floors(
            floors.next_transaction_id,
            floors.next_timestamp,
        )?;

        let replay_from_end_lsn = wal.durable_lsn().as_u64();

        // every startup reader has reached the immutable durable frontier. The
        // live runtime establishes separate retention protection for checkpoint
        // publication
        drop(recovery_pin);

        let adapter = Arc::new(RagnorDbWalAdapter::new(wal));

        let executor = LocalExecutor::from_recovered(
            catalog,
            mvcc_by_table,
            adapter.clone(),
            adapter.clone(),
            recovered_marks.max_timestamp,
            replay_from_end_lsn,
        )?;

        Ok((
            Self {
                executor: Arc::new(RwLock::new(executor)),
                transaction_manager: Arc::new(StdMutex::new(transaction_manager)),
                next_snapshot_id: Some(floors.next_snapshot_id),
                data_dir: Some(data_dir),
                checkpoint_adapter: Some(adapter),
                checkpoint_publication_lock: Arc::new(Mutex::new(())),
                data_directory_lock: Some(data_directory_lock),
                durability_gate: DurabilityGate::new(),
                latest_checkpoint_id: selected_snapshot_id,
                checkpoint_replay_frontier: replay_from_lsn,
                node_wal: None,
            },
            recovery_report,
            recovered_raft,
        ))
    }

    /// construct an empty shared runtime for tests without durable storage
    pub fn shared() -> SharedLocalDatabase {
        Arc::new(Mutex::new(Self::new()))
    }

    /// publish an already recovered runtime to connection tasks
    pub fn into_shared(self) -> SharedLocalDatabase {
        Arc::new(Mutex::new(self))
    }

    /// Publish the split distributed SQL services for request handlers.
    ///
    /// The returned handles share the recovered executor and allocator with
    /// lifecycle/recovery code, but statement execution no longer needs the
    /// outer `SharedLocalDatabase` mutex. A fixed permit set bounds parsing,
    /// planning, CPU operators, and blocking tablet waits as one admission
    /// domain until dedicated query workers are introduced.
    pub fn database_services(&self) -> SharedDatabaseServices {
        let parallelism = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .max(1);

        Arc::new(DatabaseServices {
            executor: self.executor.clone(),
            transaction_manager: self.transaction_manager.clone(),
            durability_gate: self.durability_gate.clone(),
            statement_permits: Arc::new(Semaphore::new(parallelism)),
            published_view_generation: AtomicU64::new(0),
        })
    }

    /// Connect future SQL commits to the server-owned replicated tablet host.
    ///
    /// Catalog durability and checkpoint metadata remain on the shared A-WAL;
    /// only row-commit authority moves to Raft for the Milestone 4 replicated
    /// runtime.
    pub fn replace_commit_log(&mut self, commit_log: SharedCommitLog) {
        self.executor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace_commit_log(commit_log);
    }

    /// Connect future SQL catalog changes to the replicated tablet host.
    pub fn replace_catalog_log(&mut self, catalog_log: SharedCatalogLog) {
        self.executor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace_catalog_log(catalog_log);
    }

    /// Install the metadata-Raft authority used by replicated SQL CREATE TABLE.
    ///
    /// The legacy catalog log remains available for the M4 compatibility table,
    /// but metadata-owned schema creation is routed through this boundary and
    /// therefore receives its identity only from the metadata state machine.
    pub fn replace_metadata_table_creator(&mut self, creator: SharedMetadataTableCreator) {
        self.executor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace_metadata_table_creator(creator);
    }

    /// Install the node-level gateway used when a metadata-routed tablet is not
    /// materialized in this process. Existing local-table compatibility paths
    /// remain unchanged and continue to use their direct coordinators.
    pub fn replace_tablet_gateway(&mut self, gateway: SharedTabletGateway) {
        self.executor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace_tablet_gateway(gateway);
    }

    /// Clone the serialized A-WAL handle for the Raft persistence owner.
    ///
    /// `WalHandle` clones share one synchronized engine, so database records and
    /// group-owned Raft records retain a single physical ordering and recovery
    /// stream.
    pub fn wal_handle(&self) -> Result<WalHandle<FsSegmentDirectory, ()>> {
        self.checkpoint_adapter
            .as_ref()
            .map(|adapter| adapter.wal_handle())
            .ok_or_else(|| {
                Error::Configuration(
                    "the in-memory database runtime does not own an A-WAL handle".to_string(),
                )
            })
    }

    /// Install the sole physical-retention owner for a shared database/Raft
    /// WAL. Database checkpoints publish their logical floor through this
    /// coordinator instead of truncating segments independently.
    pub fn install_node_wal(
        &mut self,
        node_wal: NodeRaftWal<WalHandle<FsSegmentDirectory, ()>>,
    ) -> Result<()> {
        node_wal
            .advance_database_retention(self.checkpoint_replay_frontier)
            .map_err(|reason| Error::RecoveryRequired { reason })?;
        self.node_wal = Some(node_wal);
        Ok(())
    }

    /// Update the SQL-visible MVCC mirror from one committed Raft command.
    pub(crate) fn apply_replicated_commit(
        &mut self,
        command: &SingleShardCommitCommand,
    ) -> Result<usize> {
        self.durability_gate.ensure_healthy()?;
        let table_id = command
            .writes
            .first()
            .and_then(|write| ragnordb_storage::key::decode_row_key(&write.key).ok())
            .map(|row_key| row_key.table_id);
        let result = if table_id.is_some_and(|table_id| {
            self.executor
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_metadata_table(table_id)
        }) {
            // Metadata-owned row state is authoritative in the replicated
            // tablet. The SQL gateway has no local MVCC mirror to publish, so
            // a follower only advances its allocator high-water floor.
            Ok(0)
        } else {
            self.executor
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .apply_replicated_commit(command)
        };
        if result.is_ok() {
            self.transaction_manager
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .observe_replicated_high_water(command.txn_id, command.commit_timestamp);
        }
        if let Err(error) = &result {
            self.durability_gate.observe_error(error);
        }
        result
    }

    /// Advance transaction and timestamp allocation floors for a replicated
    /// commit whose tablet is authoritative outside the local SQL mirror.
    /// Metadata-owned tablets still contribute durable MVCC timestamps that a
    /// follower must observe before it starts a routed read or becomes leader.
    pub(crate) fn observe_replicated_commit_high_water(
        &mut self,
        command: &SingleShardCommitCommand,
    ) -> Result<()> {
        self.durability_gate.ensure_healthy()?;
        self.transaction_manager
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .observe_replicated_high_water(command.txn_id, command.commit_timestamp);
        Ok(())
    }

    /// Install the authoritative tablet image reconstructed by Raft startup.
    pub(crate) fn install_replicated_storage(
        &mut self,
        table_id: ragnordb_common::ids::TableId,
        storage: ragnordb_storage::mvcc::InMemoryMvcc,
    ) -> Result<bool> {
        let (transaction_id, timestamp) = storage.allocator_high_water_marks();
        let installed = self
            .executor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .install_replicated_storage(table_id, storage)?;
        if installed {
            self.transaction_manager
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .observe_replicated_high_water(transaction_id, timestamp);
        }
        Ok(installed)
    }

    /// Materialize a Raft-authoritative catalog update on a follower.
    pub(crate) fn apply_replicated_catalog(
        &mut self,
        command: &ragnordb_common::command_codec::CatalogCommand,
        update_timestamp: ragnordb_common::ids::Timestamp,
    ) -> Result<()> {
        let ragnordb_common::command_codec::CatalogOperation::CreateTable(operation) =
            &command.operation;
        let table_id = ragnordb_common::ids::TableId(operation.table_def.table_id);
        // The local catalog WAL is a derived recovery cache. Startup can see
        // the cached definition before replaying the still-retained Raft entry,
        // so repeated installation must be an exact no-op.
        if self
            .executor
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .catalog()
            .table_by_id(table_id)
            .is_some()
        {
            self.transaction_manager
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .observe_replicated_high_water(ragnordb_common::ids::TxnId(0), update_timestamp);
            return Ok(());
        }
        self.executor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .apply_replicated_catalog(command)?;
        self.transaction_manager
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .observe_replicated_high_water(ragnordb_common::ids::TxnId(0), update_timestamp);
        Ok(())
    }

    /// publish one complete checkpoint through the live database owner
    ///
    /// the workflow owns the complete correctness sequence:
    ///
    /// 1. serialize against another checkpoint publication,
    /// 2. pin the existing WAL recovery path,
    /// 3. capture catalog, MVCC, allocators, and replay frontier under the
    ///    database state barrier,
    /// 4. release the state barrier while writing and synchronizing the detached
    ///    snapshot file,
    /// 5. append and synchronize the snapshot pointer,
    /// 6. append and synchronize the exactly matching checkpoint marker,
    /// 7. release the old-history retention pin,
    /// 8. advance retention to the published replay boundary and prune only
    ///    complete segments allowed by remaining pins.
    ///
    /// the separate publication mutex prevents an older snapshot from publishing
    /// its marker after a newer snapshot and incorrectly becoming recovery's last
    /// checkpoint candidate
    pub async fn publish_checkpoint(
        database: &SharedLocalDatabase,
    ) -> Result<LiveCheckpointPublication> {
        let (publication_lock, durability_gate) = {
            let runtime = database.lock().await;
            runtime.durability_gate.ensure_healthy()?;

            (
                runtime.checkpoint_publication_lock.clone(),
                runtime.durability_gate.clone(),
            )
        };

        let _publication_guard = publication_lock.lock_owned().await;
        durability_gate.ensure_healthy()?;

        let (data_dir, snapshot, adapter, retention_pin, node_wal) = {
            let mut runtime = database.lock().await;
            runtime.durability_gate.ensure_healthy()?;

            let data_dir = runtime.data_dir.clone().ok_or_else(|| {
                Error::Configuration(
                    "checkpoint publication requires a recovered durable database runtime"
                        .to_string(),
                )
            })?;

            let adapter = runtime.checkpoint_adapter.clone().ok_or_else(|| {
                Error::Configuration(
                    "checkpoint publication requires the live database WAL adapter".to_string(),
                )
            })?;

            let retention_pin: CheckpointRetentionPin =
                adapter.acquire_checkpoint_retention_pin()?;
            let snapshot = runtime.capture_checkpoint_image()?;

            (
                data_dir,
                snapshot,
                adapter,
                retention_pin,
                runtime.node_wal.clone(),
            )
        };

        let worker_gate = durability_gate.clone();
        let join_gate = durability_gate.clone();

        let worker = tokio::task::spawn_blocking(move || -> Result<LiveCheckpointPublication> {
            let result = (|| {
                // snapshot file failure occurs before authoritative WAL
                // publication and is therefore retryable without fencing
                let snapshot_file = publish_snapshot_file(&data_dir, &snapshot)?;

                // a concurrent SQL durability failure may have fenced the
                // node while the immutable file was being written
                worker_gate.ensure_healthy()?;

                let checkpoint = publish_checkpoint_metadata(adapter.as_ref(), &snapshot_file)?;

                // the marker is durable, so the new snapshot is now a valid
                // recovery source. Releasing the old-history pin is safe even
                // if another operation fenced the node immediately afterward
                drop(retention_pin);

                worker_gate.ensure_healthy()?;

                let pruned_segments = match node_wal {
                    Some(owner) => owner
                        .advance_database_retention(checkpoint.replay_from_lsn)
                        .map_err(|reason| Error::RecoveryRequired { reason })?,
                    None => adapter.advance_checkpoint_retention(checkpoint.replay_from_lsn)?,
                };

                Ok(LiveCheckpointPublication {
                    checkpoint,
                    retention_advanced_to: checkpoint.replay_from_lsn,
                    pruned_segments,
                })
            })();

            if let Err(error) = &result {
                worker_gate.observe_error(error);
            }

            result
        })
        .await;

        match worker {
            Ok(Ok(publication)) => {
                let mut runtime = database.lock().await;
                runtime.latest_checkpoint_id = Some(publication.checkpoint.snapshot_id);
                runtime.checkpoint_replay_frontier = publication.checkpoint.replay_from_lsn;
                crate::metrics::counter_inc("ragnordb_checkpoint_success_total");
                crate::metrics::gauge_set(
                    "ragnordb_checkpoint_replay_frontier",
                    publication.checkpoint.replay_from_lsn.as_u64() as f64,
                );
                Ok(publication)
            }

            Ok(Err(error)) => {
                crate::metrics::counter_inc("ragnordb_checkpoint_failure_total");
                Err(error)
            }

            Err(source) => Err(join_gate.require_recovery(
                ragnordb_common::durability::DurabilityFailureKind::RecoveryRequired,
                format!("checkpoint publication worker failed: {source}"),
            )),
        }
    }

    /// execute one SQL statement through the connection's SQL session
    ///
    /// every statement, including reads and metadata inspection, is rejected
    /// after authoritative durable state becomes uncertain. The error that
    /// first crosses this boundary fences every later connection
    pub fn execute_sql(&mut self, session: &mut SqlSession, sql: &str) -> Result<ExecutionResult> {
        self.execute_sql_with_metadata_request(session, sql, None, std::time::Duration::ZERO)
    }

    /// Execute one SQL statement after refreshing committed metadata state and
    /// optionally carrying a request identity to metadata Raft.
    pub fn execute_sql_with_metadata_request(
        &mut self,
        session: &mut SqlSession,
        sql: &str,
        metadata_request_id: Option<RequestId>,
        metadata_timeout: std::time::Duration,
    ) -> Result<ExecutionResult> {
        self.execute_sql_with_metadata_request_and_identity(
            session,
            sql,
            metadata_request_id,
            None,
            metadata_timeout,
        )
    }

    pub fn execute_sql_with_metadata_request_and_identity(
        &mut self,
        session: &mut SqlSession,
        sql: &str,
        metadata_request_id: Option<RequestId>,
        logical_request_id: Option<ragnordb_common::ids::ClientRequestId>,
        metadata_timeout: std::time::Duration,
    ) -> Result<ExecutionResult> {
        self.durability_gate.ensure_healthy()?;
        self.executor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .refresh_metadata_catalog()?;

        let result = {
            let mut executor = self
                .executor
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut transaction_manager = self
                .transaction_manager
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            session.execute_sql_with_metadata_request_and_identity(
                sql,
                &mut executor,
                &mut *transaction_manager,
                metadata_request_id,
                logical_request_id,
                metadata_timeout,
            )
        };

        if let Err(error) = &result {
            self.durability_gate.observe_error(error);
        }

        result
    }

    /// Execute one SELECT through the bounded streaming executor. The caller
    /// keeps the database guard while the sink applies backpressure, so tablet
    /// pages stop advancing when the client stops consuming output.
    pub fn execute_sql_streaming(
        &mut self,
        session: &mut SqlSession,
        sql: &str,
        sink: &mut dyn QueryResultSink,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<QueryStreamSummary> {
        self.durability_gate.ensure_healthy()?;
        self.executor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .refresh_metadata_catalog()?;
        let mut executor = self
            .executor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut transaction_manager = self
            .transaction_manager
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let result = session.execute_sql_streaming(
            sql,
            &mut executor,
            &mut *transaction_manager,
            sink,
            max_rows,
            max_bytes,
        );
        if let Err(error) = &result {
            self.durability_gate.observe_error(error);
        }
        result
    }

    /// capture one immutable database image under the runtime's write barrier.
    ///
    /// this operation fixes catalog state, MVCC state, allocator maxima, and the
    /// exact replay frontier as one consistent cut. It does not perform file or
    /// WAL publication; the live `publish_checkpoint` workflow owns those later
    /// stages
    pub fn capture_checkpoint_image(&mut self) -> Result<snapshot_proto::DatabaseSnapshot> {
        self.durability_gate.ensure_healthy()?;

        let snapshot_id = self.next_snapshot_id.ok_or_else(|| {
            Error::Configuration("snapshot ID allocator is exhausted".to_string())
        })?;

        let previous_timestamp = self
            .transaction_manager
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .last_allocated_timestamp();

        let snapshot_timestamp = self
            .transaction_manager
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .allocate_commit_timestamp(previous_timestamp)?;

        let tables = self
            .executor
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .capture_snapshot_tables()?;
        // durable runtimes use A-WAL's complete physical frontier. This includes
        // previously published checkpoint pointer and marker records, allowing
        // later checkpoints to make obsolete checkpoint metadata reclaimable
        //
        // in memory executor tests have no A-WAL adapter and retain their
        // semantic state-changing frontier
        let replay_from_lsn = self
            .checkpoint_adapter
            .as_ref()
            .map(|adapter| adapter.durable_lsn().as_u64())
            .unwrap_or_else(|| {
                self.executor
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .replay_from_end_lsn()
            });
        let max_table_id = self
            .executor
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .catalog()
            .table_id_high_water_mark();
        let max_transaction_id = self
            .transaction_manager
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .last_allocated_transaction_id();

        self.next_snapshot_id = snapshot_id.checked_add(1);

        Ok(snapshot_proto::DatabaseSnapshot {
            snapshot_id,
            snapshot_timestamp: Some(snapshot_timestamp.to_proto()),
            replay_from_lsn,
            high_water_marks: Some(snapshot_proto::AllocatorHighWaterMarks {
                max_transaction_id: Some(max_transaction_id.to_proto()),
                max_timestamp: Some(snapshot_timestamp.to_proto()),
                max_table_id: Some(max_table_id.to_proto()),
                max_snapshot_id: snapshot_id,
            }),
            tables,
        })
    }

    /// return the node-wide durability gate used by SQL, checkpoint publication,
    /// retention, and administrative health reporting
    pub fn durability_gate(&self) -> DurabilityGate {
        self.durability_gate.clone()
    }

    /// return storage progress without exposing mutable database internals
    pub fn status(&self) -> DatabaseStatus {
        match &self.checkpoint_adapter {
            Some(adapter) => {
                let wal_metrics = adapter.metrics();

                DatabaseStatus {
                    durable_lsn: adapter.durable_lsn().as_u64(),
                    replay_frontier: self.checkpoint_replay_frontier.as_u64(),
                    latest_checkpoint_id: self.latest_checkpoint_id,
                    wal_retained_bytes: wal_metrics.current_wal_size,
                    retention_pins_active: wal_metrics.retention_pins_active,
                    //  only live RagnorDB-owned retention pin protects LSN
                    // zero during checkpoint publication
                    oldest_retention_pin_lsn: (wal_metrics.retention_pins_active != 0).then_some(0),
                    wal_last_append_nanos: adapter.last_append_duration_nanos(),
                    wal_last_sync_nanos: wal_metrics
                        .last_sync_duration
                        .as_nanos()
                        .try_into()
                        .unwrap_or(u64::MAX),
                }
            }
            None => DatabaseStatus {
                durable_lsn: self
                    .executor
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .replay_from_end_lsn(),
                replay_frontier: self
                    .executor
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .replay_from_end_lsn(),
                latest_checkpoint_id: self.latest_checkpoint_id,
                wal_retained_bytes: 0,
                retention_pins_active: 0,
                oldest_retention_pin_lsn: None,
                wal_last_append_nanos: 0,
                wal_last_sync_nanos: 0,
            },
        }
    }

    /// complete the durable clean shutdown protocol for the owned A-WAL
    pub fn shutdown(&mut self) -> Result<()> {
        self.durability_gate.ensure_healthy()?;

        if let Some(adapter) = &self.checkpoint_adapter {
            adapter.shutdown()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, Mutex as TestMutex};

    use ragnordb_common::{
        catalog_codec::{ColumnDefinition, DataType, TableDefinition},
        ids::{RaftGroupId, ReplicaId, TableId, TabletId},
        metadata_codec::{PartitionSpec, TabletDescriptor},
        rpc_codec::{ReplicaRoute, TabletRoute},
    };
    use ragnordb_exec::{MetadataTableCreator, TabletGateway};

    struct TestMetadata {
        definitions: Vec<TableDefinition>,
        descriptors: BTreeMap<TableId, Vec<TabletDescriptor>>,
    }

    impl MetadataTableCreator for TestMetadata {
        fn create_table(
            &self,
            _request: ragnordb_common::metadata_codec::CreateTableRequest,
            _request_id: RequestId,
            _timeout: std::time::Duration,
        ) -> Result<TableDefinition> {
            Err(Error::NotImplemented("CREATE TABLE is unused by this test"))
        }

        fn table_descriptors(&self, table_id: TableId) -> Result<Vec<TabletDescriptor>> {
            self.descriptors.get(&table_id).cloned().ok_or_else(|| {
                Error::CorruptData(format!("missing test descriptors for table {}", table_id.0))
            })
        }

        fn list_tables(&self) -> Vec<TableDefinition> {
            self.definitions.clone()
        }
    }

    struct BlockingGateway {
        slow_table: TableId,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
        reads: TestMutex<Vec<TableId>>,
    }

    impl BlockingGateway {
        fn route(table_id: TableId) -> TabletRoute {
            TabletRoute {
                raft_group_id: RaftGroupId(table_id.0 + 1_000),
                tablet_id: TabletId(table_id.0 + 100),
                tablet_epoch: 1,
                leader_replica_id: ReplicaId(1),
                replicas: vec![ReplicaRoute {
                    replica_id: ReplicaId(1),
                    node_id: ragnordb_common::ids::NodeId(1),
                }],
            }
        }
    }

    impl TabletGateway for BlockingGateway {
        fn lookup_tablet_route(&self, table_id: TableId, _key: &[u8]) -> Result<TabletRoute> {
            Ok(Self::route(table_id))
        }

        fn read_point(
            &self,
            route: &TabletRoute,
            _request_id: RequestId,
            _row_key: ragnordb_common::ids::RowKey,
            _read_timestamp: ragnordb_common::ids::Timestamp,
            _timeout: std::time::Duration,
        ) -> Result<Option<Vec<u8>>> {
            let table_id = TableId(route.tablet_id.0 - 100);
            self.reads.lock().unwrap().push(table_id);
            if table_id == self.slow_table {
                self.entered.wait();
                self.release.wait();
            }
            Ok(None)
        }

        fn submit_command(
            &self,
            _route: &TabletRoute,
            _request_id: RequestId,
            _command: ragnordb_common::command_codec::TabletCommand,
            _timeout: std::time::Duration,
        ) -> Result<ragnordb_tablet::command::TabletCommandApplyOutcome> {
            Err(Error::NotImplemented("commands are unused by this test"))
        }
    }

    fn test_table(table_id: u64, name: &str) -> (TableDefinition, TabletDescriptor) {
        let table_id = TableId(table_id);
        (
            TableDefinition {
                table_id: table_id.0,
                name: name.to_string(),
                columns: vec![ColumnDefinition {
                    column_id: ragnordb_common::ids::ColumnId(1),
                    name: "id".to_string(),
                    ty: DataType::Int,
                    nullable: false,
                }],
                primary_key_column_ids: vec![ragnordb_common::ids::ColumnId(1)],
                schema_version: 1,
                tablet_count: 1,
            },
            TabletDescriptor {
                tablet_id: TabletId(table_id.0 + 100),
                table_id,
                raft_group_id: RaftGroupId(table_id.0 + 1_000),
                tablet_epoch: 1,
                partition: PartitionSpec::Hash {
                    bucket: 0,
                    bucket_count: 1,
                },
            },
        )
    }

    /// Realistic bug caught:
    ///
    /// A slow remote read on one metadata-routed tablet used to retain the
    /// node-global `LocalDatabase` mutex, so an unrelated tablet statement
    /// could not even parse and route. This proves the distributed service
    /// keeps the session and remote tablet wait outside that mutable owner.
    #[test]
    fn unrelated_metadata_table_reads_do_not_head_of_line_block() {
        let (slow_definition, slow_descriptor) = test_table(42, "slow_table");
        let (fast_definition, fast_descriptor) = test_table(43, "fast_table");
        let definitions = vec![slow_definition, fast_definition];
        let descriptors = BTreeMap::from([
            (slow_descriptor.table_id, vec![slow_descriptor]),
            (fast_descriptor.table_id, vec![fast_descriptor]),
        ]);

        let mut database = LocalDatabase::new();
        database.replace_metadata_table_creator(Arc::new(TestMetadata {
            definitions,
            descriptors,
        }));
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        database.replace_tablet_gateway(Arc::new(BlockingGateway {
            slow_table: TableId(42),
            entered: entered.clone(),
            release: release.clone(),
            reads: TestMutex::new(Vec::new()),
        }));
        let services = database.database_services();

        let slow_services = services.clone();
        let slow = std::thread::spawn(move || {
            let mut session = SqlSession::with_client_id(101);
            slow_services.execute_sql(
                &mut session,
                "SELECT id FROM slow_table WHERE id = 1",
                None,
                None,
                std::time::Duration::from_secs(1),
            )
        });

        entered.wait();

        let fast_services = services.clone();
        let fast = std::thread::spawn(move || {
            let mut session = SqlSession::with_client_id(102);
            fast_services.execute_sql(
                &mut session,
                "SELECT id FROM fast_table WHERE id = 1",
                None,
                None,
                std::time::Duration::from_secs(1),
            )
        });

        let fast_result = fast.join().expect("fast statement thread panicked");
        assert!(
            fast_result.is_ok(),
            "fast tablet statement was blocked: {fast_result:?}"
        );

        release.wait();
        assert!(slow.join().expect("slow statement thread panicked").is_ok());
    }
}
