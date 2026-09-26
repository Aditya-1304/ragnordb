use std::{
    collections::BTreeMap,
    net::TcpListener,
    sync::{
        Arc, Barrier, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use ragnordb_common::{
    Error, Result,
    catalog_codec::{ColumnDefinition, DataType},
    codec::{Row, TxnStatus, TxnStatusRecord, Value, WriteKind},
    command_codec::{PrewriteCommand, SingleShardCommitCommand, TabletCommand, WriteEntry},
    encoding::decode_row,
    ids::{
        ClientRequestId, ColumnId, CommandKind, LogicalCommandId, NodeId, RaftGroupId, ReplicaId,
        RequestId, TableId, Timestamp, TxnId,
    },
    metadata_codec::CreateTableRequest,
    rpc_codec::{TabletPointReadInspection, TabletRoute, TabletScanBatch},
};
use ragnordb_exec::{
    ExecutionResult, QueryResultSink, ResultColumn, SqlSession, TabletGateway, TabletScanRoute,
};
use ragnordb_server::{
    config::{NodeConfig, SeedNodeConfig},
    data_directory_lock::DataDirectoryLock,
    database::{LocalDatabase, SharedLocalDatabase},
    multiraft_runtime::{MetadataTimestampReservationClient, MultiRaftRuntime},
    rpc::TabletRpcClient,
    transaction_lifecycle::{LifecycleTabletGateway, TransactionRuntime, TransactionRuntimeConfig},
};
use ragnordb_storage::key::make_row_key;
use tempfile::TempDir;

fn unused_address(reservations: &mut Vec<TcpListener>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    reservations.push(listener);
    address
}

fn endpoint_fixture_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

struct TestNode {
    database: SharedLocalDatabase,
    runtime: MultiRaftRuntime,
    _data: Arc<TempDir>,
}

/// Simulates a streaming writer failure or a writer that has already closed.
struct FailingQuerySink {
    cancelled: bool,
}

impl QueryResultSink for FailingQuerySink {
    fn start(&mut self, _columns: Vec<ResultColumn>, _read_ts: Timestamp) -> Result<()> {
        Ok(())
    }

    fn push_batch(&mut self, _rows: Vec<ragnordb_common::codec::Row>) -> Result<()> {
        Err(Error::ProposalUnavailable {
            reason: "injected streaming sink failure".to_string(),
        })
    }

    fn cancelled(&self) -> bool {
        self.cancelled
    }
}

async fn assert_gc_protection_released_after_idle(
    runtime: Arc<TransactionRuntime>,
    operation: &str,
) {
    // The durable aggregate may remain conservative during the configured
    // grace period, but must become releasable once no local owner remains.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let reconcile_runtime = Arc::clone(&runtime);
    tokio::task::spawn_blocking(move || reconcile_runtime.reconcile_gc_protection_once())
        .await
        .expect("GC protection reconciliation task must not panic")
        .expect("GC protection reconciliation must complete");

    assert_eq!(
        runtime.gc_protection_snapshot().active_protections,
        0,
        "{operation} must leave no active GC history protection"
    );
}

#[derive(Debug, Clone)]
struct CapturedCommandIdentity {
    route: TabletRoute,
    request_id: RequestId,
    logical_command_id: LogicalCommandId,
}

#[derive(Default)]
struct RuntimeFaultState {
    primary_prewrite_txn: Option<TxnId>,
    primary_prewrite_tablet: Option<ragnordb_common::ids::TabletId>,
    primary_prewrite_identity: Option<CapturedCommandIdentity>,
    primary_commit_txn: Option<TxnId>,
    primary_commit_tablet: Option<ragnordb_common::ids::TabletId>,
    primary_commit_status: Option<TxnStatusRecord>,
    primary_commit_identity: Option<CapturedCommandIdentity>,
    secondary_commit_attempts: Vec<(ragnordb_common::ids::TabletId, Timestamp)>,
    resolved_intents: Vec<(TxnId, Option<Timestamp>, LogicalCommandId)>,
    events: Vec<RuntimeCommandEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)] // Event names identify the applied command boundary under test.
enum RuntimeCommandEvent {
    PrewriteApplied {
        txn_id: TxnId,
        tablet_id: ragnordb_common::ids::TabletId,
    },
    CommitApplied {
        txn_id: TxnId,
        tablet_id: ragnordb_common::ids::TabletId,
        commit_timestamp: Timestamp,
        primary: bool,
    },
    RollbackApplied {
        txn_id: TxnId,
        tablet_id: ragnordb_common::ids::TabletId,
    },
    AbortStatusApplied {
        txn_id: TxnId,
        tablet_id: ragnordb_common::ids::TabletId,
    },
    IntentResolutionApplied {
        txn_id: TxnId,
        tablet_id: ragnordb_common::ids::TabletId,
    },
}

/// Production-gateway wrapper used by runtime tests to stop at exact
/// participant boundaries while leaving every successful operation on the
/// real tablet RPC, Raft, and A-WAL path.
struct RuntimeFaultGateway {
    inner: TabletRpcClient,
    state: Mutex<RuntimeFaultState>,
    leader_overrides: Mutex<BTreeMap<RaftGroupId, ReplicaId>>,
    fail_secondary_prewrite_once: AtomicBool,
    fail_secondary_commit_once: AtomicBool,
}

impl RuntimeFaultGateway {
    fn new(
        inner: TabletRpcClient,
        fail_secondary_prewrite_once: bool,
        fail_secondary_commit_once: bool,
    ) -> Self {
        Self {
            inner,
            state: Mutex::new(RuntimeFaultState::default()),
            leader_overrides: Mutex::new(BTreeMap::new()),
            fail_secondary_prewrite_once: AtomicBool::new(fail_secondary_prewrite_once),
            fail_secondary_commit_once: AtomicBool::new(fail_secondary_commit_once),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, RuntimeFaultState> {
        self.state.lock().unwrap()
    }
}

impl TabletGateway for RuntimeFaultGateway {
    fn lookup_tablet_route(&self, table_id: TableId, key: &[u8]) -> Result<TabletRoute> {
        let mut route = TabletGateway::lookup_tablet_route(&self.inner, table_id, key)?;
        if let Some(leader) = self
            .leader_overrides
            .lock()
            .unwrap()
            .get(&route.raft_group_id)
            .copied()
        {
            route.leader_replica_id = leader;
        }
        Ok(route)
    }

    fn inspect_point(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        row_key: ragnordb_common::ids::RowKey,
        read_timestamp: Timestamp,
        timeout: Duration,
    ) -> Result<TabletPointReadInspection> {
        TabletGateway::inspect_point(
            &self.inner,
            route,
            request_id,
            row_key,
            read_timestamp,
            timeout,
        )
    }

    fn transaction_status(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        txn_id: TxnId,
        timeout: Duration,
    ) -> Result<Option<TxnStatusRecord>> {
        TabletGateway::transaction_status(&self.inner, route, request_id, txn_id, timeout)
    }

    fn read_point(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        row_key: ragnordb_common::ids::RowKey,
        read_timestamp: Timestamp,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>> {
        TabletGateway::read_point(
            &self.inner,
            route,
            request_id,
            row_key,
            read_timestamp,
            timeout,
        )
    }

    fn lookup_scan_routes(
        &self,
        table_id: TableId,
        span: &ragnordb_tablet::ScanSpan,
    ) -> Result<Vec<TabletScanRoute>> {
        TabletGateway::lookup_scan_routes(&self.inner, table_id, span)
    }

    fn scan_page(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        span: &ragnordb_tablet::ScanSpan,
        resume_after: Option<&[u8]>,
        read_timestamp: Timestamp,
        max_rows: u32,
        max_bytes: u32,
        timeout: Duration,
    ) -> Result<TabletScanBatch> {
        TabletGateway::scan_page(
            &self.inner,
            route,
            request_id,
            span,
            resume_after,
            read_timestamp,
            max_rows,
            max_bytes,
            timeout,
        )
    }

    fn submit_command(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<ragnordb_tablet::command::TabletCommandApplyOutcome> {
        TabletGateway::submit_command(&self.inner, route, request_id, command, timeout)
    }

    fn submit_command_with_identity(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<ragnordb_tablet::command::TabletCommandApplyOutcome> {
        self.submit_command_with_identity_and_ack(
            route,
            request_id,
            logical_command_id,
            None,
            command,
            timeout,
        )
    }

    fn submit_command_with_identity_and_ack(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        acknowledged_through: Option<u64>,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<ragnordb_tablet::command::TabletCommandApplyOutcome> {
        let identity = CapturedCommandIdentity {
            route: route.clone(),
            request_id: request_id.clone(),
            logical_command_id,
        };

        let secondary_prewrite = match &command {
            TabletCommand::Prewrite(prewrite) if prewrite.pending_status.is_none() => {
                let state = self.state();
                state
                    .primary_prewrite_txn
                    .filter(|txn_id| *txn_id == prewrite.txn_id)
                    .zip(state.primary_prewrite_tablet)
                    .filter(|(_, primary_tablet)| *primary_tablet != route.tablet_id)
                    .map(|_| prewrite.txn_id)
            }
            _ => None,
        };
        if let Some(txn_id) = secondary_prewrite
            && self
                .fail_secondary_prewrite_once
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return Err(Error::WriteConflict(format!(
                "injected secondary prewrite conflict for transaction {}",
                txn_id.0
            )));
        }

        let secondary_commit = match &command {
            TabletCommand::Commit(commit) if commit.committed_status.is_none() => {
                let mut state = self.state();
                let candidate = state
                    .primary_commit_txn
                    .filter(|txn_id| *txn_id == commit.txn_id)
                    .zip(state.primary_commit_tablet)
                    .filter(|(_, primary_tablet)| *primary_tablet != route.tablet_id)
                    .map(|_| (route.tablet_id, commit.commit_timestamp));
                if let Some(candidate) = candidate {
                    state.secondary_commit_attempts.push(candidate);
                }
                candidate
            }
            _ => None,
        };
        let primary_commit_applied =
            secondary_commit.is_some_and(|_| self.state().primary_commit_status.is_some());
        if primary_commit_applied
            && self
                .fail_secondary_commit_once
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return Err(Error::TabletUnavailable {
                reason: "injected stop after primary commit and before secondary commit"
                    .to_string(),
            });
        }

        let outcome = TabletGateway::submit_command_with_identity_and_ack(
            &self.inner,
            route,
            request_id,
            logical_command_id,
            acknowledged_through,
            command.clone(),
            timeout,
        )?;

        let mut state = self.state();
        match command {
            TabletCommand::Prewrite(prewrite) if prewrite.pending_status.is_some() => {
                state.primary_prewrite_txn = Some(prewrite.txn_id);
                state.primary_prewrite_tablet = Some(route.tablet_id);
                state.primary_prewrite_identity = Some(identity);
                state.events.push(RuntimeCommandEvent::PrewriteApplied {
                    txn_id: prewrite.txn_id,
                    tablet_id: route.tablet_id,
                });
            }
            TabletCommand::Prewrite(prewrite) => {
                state.events.push(RuntimeCommandEvent::PrewriteApplied {
                    txn_id: prewrite.txn_id,
                    tablet_id: route.tablet_id,
                });
            }
            TabletCommand::Commit(commit) if commit.committed_status.is_some() => {
                state.primary_commit_txn = Some(commit.txn_id);
                state.primary_commit_tablet = Some(route.tablet_id);
                state.primary_commit_status = commit.committed_status;
                state.primary_commit_identity = Some(identity);
                state.events.push(RuntimeCommandEvent::CommitApplied {
                    txn_id: commit.txn_id,
                    tablet_id: route.tablet_id,
                    commit_timestamp: commit.commit_timestamp,
                    primary: true,
                });
            }
            TabletCommand::Commit(commit) => {
                state.events.push(RuntimeCommandEvent::CommitApplied {
                    txn_id: commit.txn_id,
                    tablet_id: route.tablet_id,
                    commit_timestamp: commit.commit_timestamp,
                    primary: false,
                });
            }
            TabletCommand::Rollback(rollback) => {
                state.events.push(RuntimeCommandEvent::RollbackApplied {
                    txn_id: rollback.txn_id,
                    tablet_id: route.tablet_id,
                });
            }
            TabletCommand::PublishAbortedTransactionStatus(abort) => {
                state.events.push(RuntimeCommandEvent::AbortStatusApplied {
                    txn_id: abort.status_record.txn_id,
                    tablet_id: route.tablet_id,
                });
            }
            TabletCommand::ResolveIntent(resolve) => {
                state.resolved_intents.push((
                    resolve.txn_id,
                    resolve.commit_timestamp,
                    logical_command_id,
                ));
                state
                    .events
                    .push(RuntimeCommandEvent::IntentResolutionApplied {
                        txn_id: resolve.txn_id,
                        tablet_id: route.tablet_id,
                    });
            }
            _ => {}
        }
        Ok(outcome)
    }

    fn query_original_outcome(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        timeout: Duration,
    ) -> Result<Option<ragnordb_common::command_codec::CachedTabletCommandOutcome>> {
        TabletGateway::query_original_outcome(
            &self.inner,
            route,
            request_id,
            logical_command_id,
            timeout,
        )
    }
}

fn start_failover_test_node(
    seed: SeedNodeConfig,
    all_seeds: Vec<SeedNodeConfig>,
    cluster_id: String,
) -> TestNode {
    start_failover_test_node_with_data(
        Arc::new(tempfile::tempdir().unwrap()),
        seed,
        all_seeds,
        cluster_id,
    )
}

fn start_failover_test_node_with_data(
    data: Arc<TempDir>,
    seed: SeedNodeConfig,
    all_seeds: Vec<SeedNodeConfig>,
    cluster_id: String,
) -> TestNode {
    let config = NodeConfig {
        node_id: seed.id,
        data_dir: data.path().to_path_buf(),
        listen_addr: seed.sql_addr,
        admin_addr: seed.admin_addr,
        max_connections: 8,
        statement_timeout_ms: 5_000,
        shutdown_grace_period_ms: 1_000,
        statement_logging: ragnordb_server::config::StatementLogging::Off,
        cluster_id: Some(cluster_id),
        bootstrap: true,
        seed_nodes: all_seeds,
        snapshot_interval_entries: 100_000,
        snapshot_interval_bytes: 256 * 1024 * 1024,
        snapshot_min_elapsed_ms: 300_000,
        max_snapshot_file_bytes: 512 * 1024 * 1024,
        snapshot_chunk_bytes: 1024 * 1024,
        reactor_count: 1,
    };
    let data_directory_lock = DataDirectoryLock::acquire(&config.data_dir).unwrap();

    let configurations =
        MultiRaftRuntime::recovery_configurations(&config, &data_directory_lock).unwrap();

    let (database, _, recovered) = LocalDatabase::recover_shared_with_raft_with_lock(
        &config.data_dir,
        config.node_id,
        &configurations,
        data_directory_lock,
    )
    .unwrap();
    let wal = database.wal_handle().unwrap();
    let database = database.into_shared();
    let runtime =
        MultiRaftRuntime::start_from_shared_recovery(&config, wal, database.clone(), recovered)
            .unwrap();
    install_test_timestamp_manager(&database, &runtime);

    TestNode {
        database,
        runtime,
        _data: data,
    }
}

/// Install the same metadata-backed timestamp allocator used by production
/// server startup so recovered SQL reads cannot reuse a timestamp below the
/// durable frontier of any replicated tablet.
fn install_test_timestamp_manager(database: &SharedLocalDatabase, runtime: &MultiRaftRuntime) {
    let durable_frontier = runtime
        .metadata_handle()
        .state_snapshot()
        .timestamp_reserved_until();
    let provider =
        MetadataTimestampReservationClient::new(runtime.metadata_control(), Duration::from_secs(5));
    database
        .blocking_lock()
        .install_reserved_timestamp_manager(provider, durable_frontier, 1_024, 256)
        .expect("metadata-backed timestamp allocation must install");
}

async fn start_failover_test_nodes() -> Vec<TestNode> {
    // Serialize port selection through all transport binds. The old helper
    // dropped each ephemeral listener immediately, allowing parallel runtime
    // tests to advertise the same address before either bootstrap bound it.
    let _fixture_guard = endpoint_fixture_lock().lock().await;
    let mut reservations = Vec::new();
    let seeds = (1..=3)
        .map(|id| SeedNodeConfig {
            id: NodeId(id),
            raft_addr: unused_address(&mut reservations),
            snapshot_addr: unused_address(&mut reservations),
            sql_addr: unused_address(&mut reservations),
            admin_addr: unused_address(&mut reservations),
            region: None,
            zone: None,
            rack: None,
            storage_class: "default".to_string(),
        })
        .collect::<Vec<_>>();
    drop(reservations);
    let cluster_id = "runtime-stale-route-failover".to_string();
    let startup_handles = seeds
        .iter()
        .cloned()
        .map(|seed| {
            let all_seeds = seeds.clone();
            let cluster_id = cluster_id.clone();
            tokio::task::spawn_blocking(move || {
                start_failover_test_node(seed, all_seeds, cluster_id)
            })
        })
        .collect::<Vec<_>>();

    let mut nodes = Vec::with_capacity(startup_handles.len());
    for startup in startup_handles {
        let node = startup.await.unwrap();
        let handle = node.runtime.handle();
        let tablet_gateway = Arc::new(node.runtime.tablet_rpc_client());

        {
            let mut database = node.database.lock().await;
            database.replace_commit_log(handle.clone());
            database.replace_catalog_log(handle);
            database.replace_tablet_gateway(tablet_gateway);
        }

        nodes.push(node);
    }
    nodes
}

/// Realistic bug caught: successful single-shard SQL commits do not pass
/// through the primary-prewrite lifecycle hook, so each can leave a staged
/// footprint behind and exhaust the configured registry after 1,024 commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_single_shard_commits_do_not_exhaust_lifecycle_registry() {
    const TRANSACTIONS: u64 = 2_048;

    let nodes = start_failover_test_nodes().await;
    let leader = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(index) = nodes
                .iter()
                .position(|node| node.runtime.handle().is_leader())
            {
                break index;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the production hosts must elect a metadata leader");

    for node in &nodes {
        let creator = node.runtime.metadata_table_creator();
        node.database
            .lock()
            .await
            .replace_metadata_table_creator(creator);
    }

    let create_database = nodes[leader].database.clone();
    let created = tokio::task::spawn_blocking(move || {
        create_database
            .blocking_lock()
            .execute_sql_with_metadata_request(
                &mut SqlSession::with_client_id(0x7A10),
                "CREATE TABLE lifecycle_stress (id INT PRIMARY KEY, value TEXT NOT NULL)",
                Some(RequestId {
                    client_id: 0x7A10,
                    sequence: 1,
                    raft_group_id: RaftGroupId(2),
                }),
                Duration::from_secs(5),
            )
    })
    .await
    .expect("metadata-backed table creation task must not panic")
    .expect("metadata-backed lifecycle stress table must be created");
    let ExecutionResult::CreatedTable { table_id } = created else {
        panic!("lifecycle stress setup must create a metadata-owned table");
    };
    let tablet_group_id = nodes[leader]
        .runtime
        .metadata_table_creator()
        .table_descriptors(table_id)
        .expect("metadata must publish the lifecycle stress table descriptor")[0]
        .raft_group_id;
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if nodes.iter().all(|node| {
                node.runtime
                    .host_status()
                    .groups
                    .iter()
                    .any(|group| group.identity.raft_group_id == tablet_group_id)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("every assigned node must materialize the lifecycle stress tablet");

    let mut lifecycle_config = TransactionRuntimeConfig::from_environment(5_000)
        .expect("production lifecycle configuration must be valid");
    lifecycle_config.max_active_transactions = 1_024;
    lifecycle_config.gc_protection_idle_grace = Duration::from_millis(1);
    assert_eq!(
        lifecycle_config.max_active_transactions, 1_024,
        "the regression must exercise the normal bounded lifecycle capacity"
    );
    let transaction_runtime = Arc::new(
        TransactionRuntime::new(
            NodeId((leader + 1) as u64),
            lifecycle_config,
            nodes[leader].runtime.metadata_handle(),
            nodes[leader].runtime.metadata_control(),
        )
        .expect("production transaction runtime must initialize"),
    );

    let database = nodes[leader].database.clone();
    let gateway = Arc::new(LifecycleTabletGateway::new(
        nodes[leader].runtime.tablet_rpc_client(),
        transaction_runtime.lifecycle.clone(),
    ));
    let services = {
        let mut database = database.lock().await;
        database.replace_tablet_gateway(gateway);
        database.database_services_with_transaction_runtime(transaction_runtime.clone())
    };

    let stress_services = services.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut session = SqlSession::with_client_id(0x7A11);
        for id in 1..=TRANSACTIONS {
            let statement = format!("INSERT INTO lifecycle_stress (id, value) VALUES ({id}, 'v')");
            stress_services.execute_sql(
                &mut session,
                &statement,
                None,
                None,
                Duration::from_secs(5),
            )?;
        }

        stress_services.execute_sql(
            &mut session,
            &format!(
                "INSERT INTO lifecycle_stress (id, value) VALUES ({}, 'v')",
                TRANSACTIONS + 1,
            ),
            None,
            None,
            Duration::from_secs(5),
        )?;
        Ok(())
    })
    .await
    .expect("single-shard lifecycle stress task must not panic")
    .expect("more than 1,024 successful SingleShardCommit transactions must remain admissible");

    assert_eq!(
        transaction_runtime
            .lifecycle
            .status_snapshot(8)
            .active_count,
        0
    );

    let duplicate_services = services.clone();
    let duplicate = tokio::task::spawn_blocking(move || {
        duplicate_services.execute_sql(
            &mut SqlSession::with_client_id(0x7A12),
            &format!(
                "INSERT INTO lifecycle_stress (id, value) VALUES ({}, 'duplicate')",
                TRANSACTIONS + 1,
            ),
            None,
            None,
            Duration::from_secs(5),
        )
    })
    .await
    .expect("duplicate statement task must not panic");
    assert!(duplicate.is_err(), "duplicate primary keys must fail");
    assert_gc_protection_released_after_idle(
        Arc::clone(&transaction_runtime),
        "autocommit execution failure",
    )
    .await;

    let streaming_services = services.clone();
    let stream_failure = tokio::task::spawn_blocking(move || {
        streaming_services.execute_sql_streaming(
            &mut SqlSession::with_client_id(0x7A13),
            "SELECT id FROM lifecycle_stress",
            &mut FailingQuerySink { cancelled: false },
            1,
            128,
        )
    })
    .await
    .expect("streaming failure task must not panic");
    assert!(
        stream_failure.is_err(),
        "the injected sink error must propagate"
    );
    assert_gc_protection_released_after_idle(Arc::clone(&transaction_runtime), "streaming failure")
        .await;

    let cancelled_services = services.clone();
    let cancelled_stream = tokio::task::spawn_blocking(move || {
        cancelled_services.execute_sql_streaming(
            &mut SqlSession::with_client_id(0x7A14),
            "SELECT id FROM lifecycle_stress",
            &mut FailingQuerySink { cancelled: true },
            1,
            128,
        )
    })
    .await
    .expect("cancelled streaming task must not panic");
    assert!(cancelled_stream.is_err(), "cancellation must stop the scan");
    assert_gc_protection_released_after_idle(
        Arc::clone(&transaction_runtime),
        "stream cancellation",
    )
    .await;

    drop(services);
    drop(transaction_runtime);
    drop(nodes);
}

/// Realistic bugs caught:
///
/// The low-level Raft and deterministic cluster tests can all pass while the
/// production TCP host remains disconnected from SQL. This test uses three
/// independent durable runtimes. It also verifies that simultaneous latest
/// reads do not reuse one internal request identity while their first barrier
/// is still awaiting apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_runtime_admits_concurrent_barriers_and_replicates_sql_commit() {
    let _fixture_guard = endpoint_fixture_lock().lock().await;
    let mut reservations = Vec::new();
    let seeds = (1..=3)
        .map(|id| SeedNodeConfig {
            id: NodeId(id),
            raft_addr: unused_address(&mut reservations),
            snapshot_addr: unused_address(&mut reservations),
            sql_addr: unused_address(&mut reservations),
            admin_addr: unused_address(&mut reservations),
            region: None,
            zone: None,
            rack: None,
            storage_class: "default".to_string(),
        })
        .collect::<Vec<_>>();
    drop(reservations);
    let cluster_id = "runtime-test".to_string();
    // Replicated startup waits for metadata initialization to commit and apply.
    // Start every configured node together so the metadata Raft group can form
    // its initial quorum before any startup call waits for completion.
    let startup_handles = seeds
        .iter()
        .cloned()
        .map(|seed| {
            let all_seeds = seeds.clone();
            let cluster_id = cluster_id.clone();

            tokio::task::spawn_blocking(move || {
                let data = Arc::new(tempfile::tempdir().unwrap());
                let config = NodeConfig {
                    node_id: seed.id,
                    data_dir: data.path().to_path_buf(),
                    listen_addr: seed.sql_addr,
                    admin_addr: seed.admin_addr,
                    max_connections: 8,
                    statement_timeout_ms: 5_000,
                    shutdown_grace_period_ms: 1_000,
                    statement_logging: ragnordb_server::config::StatementLogging::Off,
                    cluster_id: Some(cluster_id),
                    bootstrap: true,
                    seed_nodes: all_seeds,
                    snapshot_interval_entries: 100_000,
                    snapshot_interval_bytes: 256 * 1024 * 1024,
                    snapshot_min_elapsed_ms: 300_000,
                    max_snapshot_file_bytes: 512 * 1024 * 1024,
                    snapshot_chunk_bytes: 1024 * 1024,
                    reactor_count: 1,
                };
                let data_directory_lock = DataDirectoryLock::acquire(&config.data_dir).unwrap();

                let configurations =
                    MultiRaftRuntime::recovery_configurations(&config, &data_directory_lock)
                        .unwrap();

                let (database, _, recovered) = LocalDatabase::recover_shared_with_raft_with_lock(
                    &config.data_dir,
                    config.node_id,
                    &configurations,
                    data_directory_lock,
                )
                .unwrap();
                let wal = database.wal_handle().unwrap();
                let database = database.into_shared();
                let runtime = MultiRaftRuntime::start_from_shared_recovery(
                    &config,
                    wal,
                    database.clone(),
                    recovered,
                )
                .unwrap();
                install_test_timestamp_manager(&database, &runtime);

                TestNode {
                    database,
                    runtime,
                    _data: data,
                }
            })
        })
        .collect::<Vec<_>>();

    let mut nodes = Vec::with_capacity(startup_handles.len());

    for startup in startup_handles {
        let node = startup.await.unwrap();
        let handle = node.runtime.handle();
        let tablet_gateway = Arc::new(node.runtime.tablet_rpc_client());

        {
            let mut database = node.database.lock().await;
            database.replace_commit_log(handle.clone());
            database.replace_catalog_log(handle);
            database.replace_tablet_gateway(tablet_gateway);
        }

        nodes.push(node);
    }

    let leader = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(index) = nodes
                .iter()
                .position(|node| node.runtime.handle().is_leader())
            {
                break index;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the production hosts must elect a leader");

    // Metadata CREATE TABLE is the lifecycle trigger for a non-legacy tablet.
    // Materialization runs on the bounded host thread after the SQL owner
    // lock is released, so every assigned node must eventually expose the
    // new Raft group through its authoritative host status.
    let request = CreateTableRequest {
        table_name: "metadata_users".to_string(),
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
    };
    let request_id = RequestId {
        client_id: 0x55,
        sequence: 1,
        raft_group_id: RaftGroupId(2),
    };
    let deadline = Instant::now() + Duration::from_secs(8);
    let topology = 'accepted: loop {
        for node in &nodes {
            let creator = node.runtime.metadata_table_creator();
            let attempt = tokio::task::spawn_blocking({
                let request = request.clone();
                let request_id = request_id.clone();
                move || {
                    creator.create_table_topology(request, request_id, Duration::from_millis(500))
                }
            })
            .await
            .expect("metadata proposal task must not panic");
            match attempt {
                Ok(topology) => break 'accepted topology,
                Err(Error::NotLeader { .. } | Error::ProposalUnavailable { .. }) => continue,
                Err(error) => panic!("metadata CREATE TABLE failed permanently: {error}"),
            }
        }
        if Instant::now() >= deadline {
            panic!("metadata CREATE TABLE did not reach a leader before the deadline");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert!(topology.definition.table_id > 1);
    let users_table_id = TableId(topology.definition.table_id);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if nodes
                .iter()
                .all(|node| node.runtime.host_status().groups.len() >= 3)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("every assigned node must materialize the metadata tablet group");

    // Both callers must receive independently tracked barriers. A shared
    // request identity would reject one caller before Raft can commit either
    // no-op, which makes healthy concurrent latest reads unavailable.
    let start = Arc::new(Barrier::new(3));
    let first = nodes[leader].runtime.handle();
    let second = nodes[leader].runtime.handle();
    let before_barriers = first.status();
    let first_start = start.clone();
    let first_barrier = tokio::task::spawn_blocking(move || {
        first_start.wait();
        first.read_barrier(Duration::from_secs(5))
    });
    let second_start = start.clone();
    let second_barrier = tokio::task::spawn_blocking(move || {
        second_start.wait();
        second.read_barrier(Duration::from_secs(5))
    });
    start.wait();

    first_barrier
        .await
        .unwrap()
        .expect("the first concurrent latest-read barrier must apply");
    second_barrier
        .await
        .unwrap()
        .expect("the second concurrent latest-read barrier must apply");
    let after_barriers = nodes[leader].runtime.handle().status();
    assert_eq!(
        after_barriers.last_log_index, before_barriers.last_log_index,
        "ReadIndex barriers must not append one no-op per latest-read waiter"
    );

    for node in &nodes {
        let creator = node.runtime.metadata_table_creator();
        node.database
            .lock()
            .await
            .replace_metadata_table_creator(creator);
    }

    // A transaction spanning two metadata-owned tables exercises two real
    // tablet Raft groups while keeping online split/merge outside this test.
    let create_secondary_table_database = nodes[leader].database.clone();
    let secondary_table = tokio::task::spawn_blocking(move || {
        create_secondary_table_database
            .blocking_lock()
            .execute_sql_with_metadata_request(
                &mut SqlSession::with_client_id(7002),
                "CREATE TABLE metadata_orders (id INT PRIMARY KEY, description TEXT NOT NULL)",
                Some(RequestId {
                    client_id: 0x7002,
                    sequence: 1,
                    raft_group_id: RaftGroupId(2),
                }),
                Duration::from_secs(5),
            )
    })
    .await
    .unwrap()
    .expect("the second metadata-owned table must be created through metadata Raft");
    let ExecutionResult::CreatedTable {
        table_id: secondary_table_id,
    } = secondary_table
    else {
        panic!("second metadata table creation must return its assigned table ID");
    };
    let secondary_descriptors = nodes[leader]
        .runtime
        .metadata_table_creator()
        .table_descriptors(secondary_table_id)
        .expect("metadata must publish the second table's tablet descriptor");
    assert_eq!(secondary_descriptors.len(), 1);
    let secondary_group_id = secondary_descriptors[0].raft_group_id;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if nodes.iter().all(|node| {
                node.runtime
                    .host_status()
                    .groups
                    .iter()
                    .any(|group| group.identity.raft_group_id == secondary_group_id)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("every assigned node must materialize the second metadata tablet group");

    let commit_fault_gateway = Arc::new(RuntimeFaultGateway::new(
        nodes[leader].runtime.tablet_rpc_client(),
        false,
        true,
    ));
    nodes[leader]
        .database
        .lock()
        .await
        .replace_tablet_gateway(commit_fault_gateway.clone());

    let cross_table_database = nodes[leader].database.clone();
    let cross_table_result = tokio::task::spawn_blocking(move || {
        let mut database = cross_table_database.blocking_lock();
        let mut session = SqlSession::with_client_id(7003);
        database.execute_sql(&mut session, "BEGIN")?;
        database.execute_sql(
            &mut session,
            "INSERT INTO metadata_users (id, name) VALUES (8, 'txn-user')",
        )?;
        database.execute_sql(
            &mut session,
            "INSERT INTO metadata_orders (id, description) VALUES (8, 'txn-order')",
        )?;
        let commit = database.execute_sql(&mut session, "COMMIT")?;
        let user = database.execute_sql(
            &mut session,
            "SELECT id, name FROM metadata_users WHERE id = 8",
        )?;
        Ok::<_, Error>((commit, user))
    })
    .await
    .unwrap();
    let (cross_table_commit, cross_table_user) =
        cross_table_result.expect("production SQL must commit writes across two tablet groups");
    let ExecutionResult::TransactionCommitted {
        commit_ts: Some(cross_table_commit_ts),
        committed_writes: 2,
        ..
    } = cross_table_commit
    else {
        panic!("two-tablet SQL transaction must report its applied commit timestamp");
    };
    assert!(cross_table_commit_ts.0 > 0);
    assert_eq!(
        cross_table_user,
        ExecutionResult::Query(ragnordb_exec::ResultSet {
            columns: vec![
                ragnordb_exec::ResultColumn {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ragnordb_exec::ResultColumn {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                },
            ],
            rows: vec![ragnordb_common::codec::Row {
                values: vec![Value::Int(8), Value::Text("txn-user".to_string())],
            }],
        })
    );
    let (
        committed_txn_id,
        committed_status,
        primary_prewrite_identity,
        primary_commit_identity,
        secondary_commit_attempts,
    ) = {
        let state = commit_fault_gateway.state();
        (
            state
                .primary_commit_txn
                .expect("the primary commit must be observed"),
            state
                .primary_commit_status
                .clone()
                .expect("the primary status must be durable"),
            state
                .primary_prewrite_identity
                .clone()
                .expect("the primary prewrite identity must be captured"),
            state
                .primary_commit_identity
                .clone()
                .expect("the primary commit identity must be captured"),
            state.secondary_commit_attempts.clone(),
        )
    };
    assert_eq!(committed_status.txn_id, committed_txn_id);
    assert_eq!(committed_status.status, TxnStatus::Committed);
    assert_eq!(
        committed_status.commit_timestamp,
        Some(cross_table_commit_ts)
    );
    assert_eq!(
        primary_prewrite_identity.logical_command_id.kind,
        CommandKind::Prewrite
    );
    assert_eq!(
        primary_commit_identity.logical_command_id.kind,
        CommandKind::Commit
    );
    assert_eq!(
        secondary_commit_attempts,
        vec![(secondary_descriptors[0].tablet_id, cross_table_commit_ts)],
        "every participant must use the same commit timestamp"
    );

    let primary_key = make_row_key(users_table_id, &[Value::Int(8)]).unwrap();
    let secondary_key = make_row_key(secondary_table_id, &[Value::Int(8)]).unwrap();
    let secondary_route = TabletGateway::lookup_tablet_route(
        commit_fault_gateway.as_ref(),
        secondary_table_id,
        &secondary_key.primary_key_bytes,
    )
    .expect("metadata must route the secondary transaction key");
    let secondary_inspection = TabletGateway::inspect_point(
        commit_fault_gateway.as_ref(),
        &secondary_route,
        RequestId {
            client_id: 0x7007,
            sequence: 1,
            raft_group_id: secondary_route.raft_group_id,
        },
        secondary_key.clone(),
        Timestamp(u64::MAX),
        Duration::from_secs(5),
    )
    .expect("the participant must report the unresolved secondary intent");
    assert_eq!(
        secondary_inspection.intent.as_ref().map(|lock| lock.txn_id),
        Some(committed_txn_id)
    );

    let primary_group_id = topology.tablets[0].raft_group_id;
    let restart_index = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(index) = nodes.iter().position(|node| {
                node.runtime
                    .host_status()
                    .groups
                    .iter()
                    .find(|group| group.identity.raft_group_id == primary_group_id)
                    .is_some_and(|group| group.leader_replica_id == Some(group.identity.replica_id))
            }) {
                break index;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the primary status tablet must have a hosted leader");
    let restart_group_frontiers = [primary_group_id, secondary_group_id].map(|group_id| {
        nodes[restart_index]
            .runtime
            .host_status()
            .groups
            .iter()
            .find(|group| group.identity.raft_group_id == group_id)
            .map(|group| group.applied_index)
            .expect("the restarting node must host each transaction participant")
    });

    // Reopen one node from its retained A-WAL while two Raft peers remain
    // live. The client then reads the still-locked secondary through the real
    // gateway, which consults the recovered primary status and replicates its
    // terminal resolution.
    nodes[leader]
        .database
        .lock()
        .await
        .replace_tablet_gateway(Arc::new(nodes[leader].runtime.tablet_rpc_client()));
    drop(commit_fault_gateway);
    let retained_data = nodes[restart_index]._data.clone();
    let stopped_node = nodes.remove(restart_index);
    let TestNode {
        database: stopped_database,
        runtime: stopped_runtime,
        _data: stopped_data,
    } = stopped_node;
    drop(stopped_runtime);
    drop(stopped_database);
    drop(stopped_data);

    let seed = seeds[restart_index].clone();
    let all_seeds = seeds.clone();
    let recovery_cluster_id = cluster_id.clone();
    let reopened_node = tokio::task::spawn_blocking(move || {
        start_failover_test_node_with_data(retained_data, seed, all_seeds, recovery_cluster_id)
    })
    .await
    .expect("A-WAL recovery task must not panic");
    let handle = reopened_node.runtime.handle();
    let recovery_gateway = Arc::new(RuntimeFaultGateway::new(
        reopened_node.runtime.tablet_rpc_client(),
        false,
        false,
    ));
    let metadata_creator = reopened_node.runtime.metadata_table_creator();
    {
        let mut database = reopened_node.database.lock().await;
        database.replace_commit_log(handle.clone());
        database.replace_catalog_log(handle);
        database.replace_metadata_table_creator(metadata_creator);
        database.replace_tablet_gateway(recovery_gateway.clone());
    }
    nodes.insert(restart_index, reopened_node);

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let groups = nodes[restart_index].runtime.host_status().groups;
            let recovered = [primary_group_id, secondary_group_id]
                .into_iter()
                .zip(restart_group_frontiers)
                .all(|(group_id, previous_index)| {
                    groups
                        .iter()
                        .find(|group| group.identity.raft_group_id == group_id)
                        .is_some_and(|group| group.applied_index >= previous_index)
                });
            if recovered {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("restarted tablet groups must recover their applied A-WAL frontiers");

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let serving_primary_leader = nodes.iter().any(|node| {
                node.runtime
                    .tablet_rpc_client()
                    .tablet_status(primary_group_id)
                    .is_some_and(|status| status.serving_leader)
            });
            let serving_secondary_leader = nodes.iter().any(|node| {
                node.runtime
                    .tablet_rpc_client()
                    .tablet_status(secondary_group_id)
                    .is_some_and(|status| status.serving_leader)
            });
            if serving_primary_leader && serving_secondary_leader {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("recovered participant groups must cross serving-leader activation");

    let recovered_primary_route = TabletGateway::lookup_tablet_route(
        recovery_gateway.as_ref(),
        users_table_id,
        &primary_key.primary_key_bytes,
    )
    .expect("recovered metadata must route the status authority");
    let (leader_replica_id, leader_node_id) = nodes
        .iter()
        .zip(&seeds)
        .find_map(|(node, seed)| {
            node.runtime
                .tablet_rpc_client()
                .tablet_status(primary_group_id)
                .filter(|status| status.serving_leader)
                .and_then(|status| {
                    status
                        .leader_replica_id
                        .map(|replica_id| (ReplicaId(replica_id), seed.id))
                })
        })
        .expect("the primary group must have an activated leader");
    let leader_replica = *recovered_primary_route
        .replicas
        .iter()
        .find(|replica| {
            replica.replica_id == leader_replica_id && replica.node_id == leader_node_id
        })
        .expect("the active primary leader must be present in the recovered route");
    let leader_node_index = seeds
        .iter()
        .position(|seed| seed.id == leader_node_id)
        .expect("the primary leader must be one of the runtime nodes");
    let mut leader_local_route = recovered_primary_route.clone();
    leader_local_route.leader_replica_id = leader_replica_id;
    leader_local_route.replicas = Arc::from(vec![leader_replica]);
    let leader_gateway = nodes[leader_node_index].runtime.tablet_rpc_client();
    let local_request_group_id = recovered_primary_route.raft_group_id;
    let local_status = tokio::task::spawn_blocking(move || {
        TabletGateway::transaction_status(
            &leader_gateway,
            &leader_local_route,
            RequestId {
                client_id: 0x7008,
                sequence: 1,
                raft_group_id: local_request_group_id,
            },
            committed_txn_id,
            Duration::from_secs(5),
        )
    })
    .await
    .expect("leader-local status query must not panic")
    .expect("the current leader must serve its durable transaction status");
    assert_eq!(local_status, Some(committed_status.clone()));

    let recovering_node_id = seeds[restart_index].id;
    let mut status_route = recovered_primary_route.clone();
    status_route.leader_replica_id = leader_replica_id;
    if status_route.replicas.iter().any(|replica| {
        replica.replica_id == status_route.leader_replica_id
            && replica.node_id == recovering_node_id
    }) {
        // When recovery reopened the current leader, keep the route focused on
        // that local replica so an unsuccessful local ReadIndex is not hidden
        // by a later remote retry in the gateway client.
        let leader_replica_id = status_route.leader_replica_id;
        status_route.replicas = Arc::from(
            status_route
                .replicas
                .iter()
                .filter(|replica| replica.replica_id == leader_replica_id)
                .cloned()
                .collect::<Vec<_>>(),
        );
    }
    let recovered_status_result = TabletGateway::transaction_status(
        recovery_gateway.as_ref(),
        &status_route,
        RequestId {
            client_id: 0x7008,
            sequence: 1,
            raft_group_id: recovered_primary_route.raft_group_id,
        },
        committed_txn_id,
        Duration::from_secs(5),
    );
    let replica_statuses = nodes
        .iter()
        .zip(&seeds)
        .map(|(node, seed)| {
            (
                seed.id,
                node.runtime
                    .tablet_rpc_client()
                    .tablet_status(primary_group_id),
            )
        })
        .collect::<Vec<_>>();
    let recovered_status = recovered_status_result
        .unwrap_or_else(|error| {
            panic!(
                "the recovered primary status must be readable; recovering_node_id={recovering_node_id:?}, route={status_route:?}, replica_statuses={replica_statuses:?}, error={error}"
            )
        })
        .expect("the committed status must survive A-WAL reopen");
    assert_eq!(recovered_status, committed_status);
    for identity in [&primary_prewrite_identity, &primary_commit_identity] {
        assert!(matches!(
            TabletGateway::query_original_outcome(
                recovery_gateway.as_ref(),
                &recovered_primary_route,
                identity.request_id.clone(),
                identity.logical_command_id,
                Duration::from_secs(5),
            )
            .expect("recovered command identity must be queryable"),
            Some(ragnordb_common::command_codec::CachedTabletCommandOutcome::Applied(_))
        ));
        assert_eq!(identity.route.raft_group_id, primary_group_id);
    }

    let recovered_secondary_route = TabletGateway::lookup_tablet_route(
        recovery_gateway.as_ref(),
        secondary_table_id,
        &secondary_key.primary_key_bytes,
    )
    .expect("secondary metadata route must survive A-WAL recovery");
    let recovered_secondary_inspection = TabletGateway::inspect_point(
        recovery_gateway.as_ref(),
        &recovered_secondary_route,
        RequestId {
            client_id: 0x7009,
            sequence: 1,
            raft_group_id: recovered_secondary_route.raft_group_id,
        },
        secondary_key.clone(),
        Timestamp(u64::MAX),
        Duration::from_secs(5),
    )
    .expect("recovered participant state must expose its unresolved intent");
    assert_eq!(
        recovered_secondary_inspection
            .intent
            .as_ref()
            .map(|lock| lock.txn_id),
        Some(committed_txn_id),
        "the committed secondary intent must survive A-WAL reopen until a reader resolves it"
    );

    let read_timestamp_database = nodes[restart_index].database.clone();
    let recovered_read_timestamp = tokio::task::spawn_blocking(move || {
        let mut session = SqlSession::with_client_id(0x700A);
        let mut database = read_timestamp_database.blocking_lock();
        let started = database.execute_sql(&mut session, "BEGIN")?;
        let ExecutionResult::TransactionStarted { start_ts, .. } = started else {
            panic!("recovered read timestamp probe must begin a transaction");
        };
        database.execute_sql(&mut session, "ROLLBACK")?;
        Ok::<_, Error>(start_ts)
    })
    .await
    .expect("recovered timestamp probe must not panic")
    .expect("the restarted node must allocate a read timestamp");
    assert!(
        recovered_read_timestamp > cross_table_commit_ts,
        "a restarted SQL reader must allocate above committed tablet state; read_ts={recovered_read_timestamp:?}, commit_ts={cross_table_commit_ts:?}"
    );

    let read_secondary_database = nodes[restart_index].database.clone();
    let recovered_secondary = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::task::spawn_blocking(move || {
            read_secondary_database.blocking_lock().execute_sql(
                &mut SqlSession::with_client_id(7009),
                "SELECT id, description FROM metadata_orders WHERE id = 8",
            )
        }),
    )
    .await
    .expect("foreground intent recovery must complete within its bound")
    .unwrap()
    .expect("the reader must roll the committed secondary intent forward");
    assert_eq!(
        recovered_secondary,
        ExecutionResult::Query(ragnordb_exec::ResultSet {
            columns: vec![
                ragnordb_exec::ResultColumn {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ragnordb_exec::ResultColumn {
                    name: "description".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                },
            ],
            rows: vec![Row {
                values: vec![Value::Int(8), Value::Text("txn-order".to_string())],
            }],
        })
    );
    let recovered_resolution = recovery_gateway
        .state()
        .resolved_intents
        .iter()
        .find(|(txn_id, _, _)| *txn_id == committed_txn_id)
        .cloned()
        .expect("the SQL read must submit a replicated intent resolution");
    assert_eq!(recovered_resolution.1, Some(cross_table_commit_ts));
    assert_eq!(recovered_resolution.2.kind, CommandKind::ResolveIntent);

    let resolved_secondary_route = TabletGateway::lookup_tablet_route(
        recovery_gateway.as_ref(),
        secondary_table_id,
        &secondary_key.primary_key_bytes,
    )
    .expect("secondary route must remain available after resolution");
    let resolved_secondary = TabletGateway::inspect_point(
        recovery_gateway.as_ref(),
        &resolved_secondary_route,
        RequestId {
            client_id: 0x7010,
            sequence: 1,
            raft_group_id: resolved_secondary_route.raft_group_id,
        },
        secondary_key,
        Timestamp(u64::MAX),
        Duration::from_secs(5),
    )
    .expect("the resolved row must be visible without an intent");
    assert!(resolved_secondary.intent.is_none());
    assert_eq!(
        decode_row(
            &resolved_secondary
                .visible_row
                .expect("the committed secondary row must be visible")
        )
        .unwrap(),
        Row {
            values: vec![Value::Int(8), Value::Text("txn-order".to_string())],
        }
    );

    // A deterministic conflict on the second participant must roll back every
    // prewritten key before the primary publishes Aborted.
    let prewrite_fault_gateway = Arc::new(RuntimeFaultGateway::new(
        nodes[restart_index].runtime.tablet_rpc_client(),
        true,
        false,
    ));
    nodes[restart_index]
        .database
        .lock()
        .await
        .replace_tablet_gateway(prewrite_fault_gateway.clone());
    let failing_database = nodes[restart_index].database.clone();
    let failed_commit = tokio::task::spawn_blocking(move || {
        let mut database = failing_database.blocking_lock();
        let mut session = SqlSession::with_client_id(7011);
        database.execute_sql(&mut session, "BEGIN")?;
        database.execute_sql(
            &mut session,
            "INSERT INTO metadata_users (id, name) VALUES (10, 'will-abort')",
        )?;
        database.execute_sql(
            &mut session,
            "INSERT INTO metadata_orders (id, description) VALUES (10, 'will-abort')",
        )?;
        database.execute_sql(&mut session, "COMMIT")
    })
    .await
    .unwrap();
    assert!(
        matches!(failed_commit, Err(Error::WriteConflict(_))),
        "secondary prewrite conflict must fail the SQL transaction, got {failed_commit:?}"
    );
    let (aborted_txn_id, primary_tablet_id, events) = {
        let state = prewrite_fault_gateway.state();
        (
            state
                .primary_prewrite_txn
                .expect("the primary Pending prewrite must have applied"),
            state
                .primary_prewrite_tablet
                .expect("the pending status has a primary tablet"),
            state.events.clone(),
        )
    };
    let aborted_primary_key = make_row_key(users_table_id, &[Value::Int(10)]).unwrap();
    let aborted_primary_route = TabletGateway::lookup_tablet_route(
        prewrite_fault_gateway.as_ref(),
        users_table_id,
        &aborted_primary_key.primary_key_bytes,
    )
    .expect("metadata must route the aborted status authority");
    let aborted_status = TabletGateway::transaction_status(
        prewrite_fault_gateway.as_ref(),
        &aborted_primary_route,
        RequestId {
            client_id: 0x7012,
            sequence: 1,
            raft_group_id: aborted_primary_route.raft_group_id,
        },
        aborted_txn_id,
        Duration::from_secs(5),
    )
    .expect("the aborted status must be readable")
    .expect("the primary must publish a terminal abort decision");
    assert_eq!(aborted_status.status, TxnStatus::Aborted);
    let abort_event_index = events
        .iter()
        .position(|event| {
            *event
                == RuntimeCommandEvent::AbortStatusApplied {
                    txn_id: aborted_txn_id,
                    tablet_id: primary_tablet_id,
                }
        })
        .expect("the aborted status command must apply through Raft");
    let rollback_tablets_before_abort = events[..abort_event_index]
        .iter()
        .filter_map(|event| match event {
            RuntimeCommandEvent::RollbackApplied { txn_id, tablet_id }
                if *txn_id == aborted_txn_id =>
            {
                Some(*tablet_id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rollback_tablets_before_abort.len(),
        2,
        "both participant rollback commands must apply before Aborted is published"
    );
    assert!(rollback_tablets_before_abort.contains(&primary_tablet_id));
    assert!(rollback_tablets_before_abort.contains(&secondary_descriptors[0].tablet_id));

    let aborted_secondary_key = make_row_key(secondary_table_id, &[Value::Int(10)]).unwrap();
    for (client_id, key) in [
        (0x7013, aborted_primary_key),
        (0x7014, aborted_secondary_key),
    ] {
        let route = TabletGateway::lookup_tablet_route(
            prewrite_fault_gateway.as_ref(),
            key.table_id,
            &key.primary_key_bytes,
        )
        .expect("metadata must route each rolled-back participant");
        let inspection = TabletGateway::inspect_point(
            prewrite_fault_gateway.as_ref(),
            &route,
            RequestId {
                client_id,
                sequence: 1,
                raft_group_id: route.raft_group_id,
            },
            key,
            Timestamp(u64::MAX),
            Duration::from_secs(5),
        )
        .expect("rolled-back participants must not retain a live intent");
        assert!(inspection.intent.is_none());
        assert!(inspection.visible_row.is_none());
    }
    nodes[restart_index]
        .database
        .lock()
        .await
        .replace_tablet_gateway(Arc::new(nodes[restart_index].runtime.tablet_rpc_client()));

    // A pending write to a key with no committed version must still block a
    // range read. The old scan path returned other visible rows and silently
    // skipped this lock-only insertion.
    let pending_key = make_row_key(users_table_id, &[Value::Int(9)]).unwrap();
    let tablet_client = nodes[leader].runtime.tablet_rpc_client();
    let pending_route = tablet_client
        .lookup_tablet_route(users_table_id, &pending_key.primary_key_bytes)
        .expect("metadata must route the pending intent key");
    let encoded_pending_key = ragnordb_storage::key::encode_row_key(&pending_key).unwrap();
    let pending_txn_id = TxnId(0x7004);
    let pending_start_ts = Timestamp(1);
    let pending_status = TxnStatusRecord {
        txn_id: pending_txn_id,
        start_timestamp: pending_start_ts,
        commit_timestamp: None,
        status: TxnStatus::Pending,
        primary_key: encoded_pending_key.clone(),
        participant_tablet_ids: vec![pending_route.tablet_id.0],
        last_heartbeat_timestamp: None,
        lease_deadline_ms: None,
    };
    let pending_command = TabletCommand::Prewrite(PrewriteCommand {
        txn_id: pending_txn_id,
        start_timestamp: pending_start_ts,
        writes: vec![WriteEntry {
            key: encoded_pending_key.clone(),
            row: Some(Row {
                values: vec![Value::Int(9), Value::Text("pending".to_string())],
            }),
            op: WriteKind::Put,
        }],
        primary_key: encoded_pending_key,
        ttl_ms: 60_000,
        pending_status: Some(pending_status),
    });
    let pending_client_request = ClientRequestId {
        client_id: 0x7004,
        session_epoch: 1,
        request_sequence: 1,
    };
    tablet_client
        .submit_command_with_identity_and_ack(
            &pending_route,
            RequestId {
                client_id: pending_client_request.client_id,
                sequence: 1,
                raft_group_id: pending_route.raft_group_id,
            },
            Some(LogicalCommandId {
                client_request_id: pending_client_request,
                command_ordinal: 1,
                kind: CommandKind::Prewrite,
            }),
            None,
            pending_command,
            Duration::from_secs(5),
        )
        .expect("the real participant Raft group must apply the pending prewrite");

    let scan_database = nodes[leader].database.clone();
    let pending_scan = tokio::task::spawn_blocking(move || {
        scan_database.blocking_lock().execute_sql(
            &mut SqlSession::with_client_id(7005),
            "SELECT id, name FROM metadata_users",
        )
    })
    .await
    .unwrap();
    assert!(
        matches!(pending_scan, Err(Error::WriteConflict(_))),
        "a range read must surface an unresolved insert intent as retryable conflict, got {pending_scan:?}"
    );

    let leader_catalog = nodes[leader].database.clone();
    let users_table = tokio::task::spawn_blocking(move || {
        leader_catalog
            .blocking_lock()
            .execute_sql_with_metadata_request(
                &mut SqlSession::new(),
                "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)",
                Some(RequestId {
                    client_id: 7006,
                    sequence: 1,
                    raft_group_id: RaftGroupId(2),
                }),
                Duration::from_secs(5),
            )
    })
    .await
    .unwrap()
    .expect("catalog success must also be resolved from replicated apply");
    let ExecutionResult::CreatedTable {
        table_id: users_table_id,
    } = users_table
    else {
        panic!("replicated CREATE TABLE must return its assigned table ID");
    };
    let users_descriptors = nodes[leader]
        .runtime
        .metadata_table_creator()
        .table_descriptors(users_table_id)
        .expect("metadata must publish the users tablet descriptor");
    let users_group_id = users_descriptors[0].raft_group_id;
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let leaders = nodes
                .iter()
                .filter_map(|node| {
                    node.runtime
                        .host_status()
                        .groups
                        .iter()
                        .find(|group| group.identity.raft_group_id == users_group_id)
                        .and_then(|group| group.leader_replica_id)
                })
                .collect::<Vec<_>>();
            if leaders.len() == nodes.len() && leaders.windows(2).all(|pair| pair[0] == pair[1]) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the users tablet group must publish a stable leader before SQL writes");

    let leader_database = nodes[leader].database.clone();
    tokio::task::spawn_blocking(move || {
        leader_database.blocking_lock().execute_sql(
            &mut SqlSession::new(),
            "INSERT INTO users (id, name) VALUES (1, 'replicated')",
        )
    })
    .await
    .unwrap()
    .expect("SQL success must be resolved from replicated tablet apply");

    let follower = (0..nodes.len()).find(|index| *index != leader).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let result = nodes[follower]
                .database
                .lock()
                .await
                .execute_sql(&mut SqlSession::new(), "SELECT id, name FROM users");

            match result {
                Ok(ExecutionResult::Query(rows))
                    if rows.rows
                        == vec![ragnordb_common::codec::Row {
                            values: vec![Value::Int(1), Value::Text("replicated".to_string())],
                        }] =>
                {
                    break;
                }

                // The catalog mirror is published by the follower's Ready
                // owner. It may apply the durable CREATE entry after the
                // leader has acknowledged it, so an initial unknown-table
                // response is a valid propagation state rather than a test
                // failure. Any other SQL error remains fatal.
                Err(Error::SchemaMismatch(message)) if message == "unknown table: users" => {}

                Ok(_) => {}

                Err(error) => panic!("follower SQL mirror returned an unexpected error: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a follower SQL mirror must observe the applied Raft commit");

    let routed_group_id = topology.tablets[0].raft_group_id;
    let routed_leader_replica = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let leaders = nodes
                .iter()
                .filter_map(|node| {
                    node.runtime
                        .host_status()
                        .groups
                        .iter()
                        .find(|group| group.identity.raft_group_id == routed_group_id)
                        .and_then(|group| group.leader_replica_id)
                })
                .collect::<Vec<_>>();
            if leaders.len() == nodes.len() && leaders.windows(2).all(|pair| pair[0] == pair[1]) {
                break leaders[0];
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the routed tablet must publish a stable leader before SQL forwarding");
    let routed_leader_node = nodes
        .iter()
        .position(|node| {
            node.runtime
                .host_status()
                .groups
                .iter()
                .find(|group| group.identity.raft_group_id == routed_group_id)
                .is_some_and(|group| group.identity.replica_id == routed_leader_replica)
        })
        .expect("the routed leader must have a hosted replica on one test node");

    // Exercise the production gateway from a non-leader SQL owner. The
    // metadata tablet is authoritative in Raft and deliberately has no local
    // SQL mirror, so this insert must be forwarded to the assigned tablet
    // leader and completed through the request-identity path.
    let routed_gateway = (0..nodes.len())
        .find(|index| *index != routed_leader_node)
        .expect("a three-node runtime must provide a non-leader gateway");
    let routed_database = nodes[routed_gateway].database.clone();
    let (insert_result, update_result, updated_select, delete_result) =
        tokio::task::spawn_blocking(move || {
            let mut session = SqlSession::with_client_id(7001);
            let mut database = routed_database.blocking_lock();
            let insert_result = database.execute_sql(
                &mut session,
                "INSERT INTO metadata_users (id, name) VALUES (7, 'alice')",
            )?;
            let update_result = database.execute_sql(
                &mut session,
                "UPDATE metadata_users SET name = 'bob' WHERE id = 7",
            )?;
            let updated_select = database.execute_sql(
                &mut session,
                "SELECT id, name FROM metadata_users WHERE id = 7",
            )?;
            let delete_result =
                database.execute_sql(&mut session, "DELETE FROM metadata_users WHERE id = 7")?;
            Ok::<_, Error>((insert_result, update_result, updated_select, delete_result))
        })
        .await
        .unwrap()
        .expect("metadata-routed DML must be forwarded and durably applied");
    assert!(matches!(
        insert_result,
        ExecutionResult::Mutation {
            affected_rows: 1,
            ..
        }
    ));
    assert!(matches!(
        update_result,
        ExecutionResult::Mutation {
            affected_rows: 1,
            ..
        }
    ));
    assert_eq!(
        updated_select,
        ExecutionResult::Query(ragnordb_exec::ResultSet {
            columns: vec![
                ragnordb_exec::ResultColumn {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ragnordb_exec::ResultColumn {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                },
            ],
            rows: vec![ragnordb_common::codec::Row {
                values: vec![Value::Int(7), Value::Text("bob".to_string())],
            }],
        })
    );
    assert!(matches!(
        delete_result,
        ExecutionResult::Mutation {
            affected_rows: 1,
            ..
        }
    ));

    let routed_reader = nodes[(routed_gateway + 1) % nodes.len()].database.clone();
    let routed_select_deadline = Instant::now() + Duration::from_secs(5);
    let routed_select = loop {
        let database = routed_reader.clone();
        let result = tokio::task::spawn_blocking(move || {
            database.blocking_lock().execute_sql(
                &mut SqlSession::new(),
                "SELECT id, name FROM metadata_users WHERE id = 7",
            )
        })
        .await
        .unwrap();
        match result {
            Ok(ExecutionResult::Query(rows)) if rows.rows.is_empty() => {
                break ExecutionResult::Query(rows);
            }
            Ok(_) if Instant::now() < routed_select_deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(Error::NotLeader { .. } | Error::ProposalUnavailable { .. })
                if Instant::now() < routed_select_deadline =>
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => panic!("metadata-routed point SELECT failed permanently: {error}"),
            Ok(result) => break result,
        }
    };
    let ExecutionResult::Query(routed_rows) = routed_select else {
        panic!("metadata-routed point SELECT must return a query result");
    };
    assert_eq!(routed_rows.rows, Vec::<ragnordb_common::codec::Row>::new());

    // Keep all lifecycle guards alive until assertions complete. Runtime Drop
    // performs an orderly Ready-owner shutdown.
    drop(nodes);
}

/// Realistic bug caught: a gateway can retain a route to the old tablet leader
/// after that leader stops. The request must retry through the same gateway's
/// stale route cache, converge on a surviving replica, and still observe the
/// committed point row. Runtime `Drop` is used as the bounded in-process stop
/// boundary; it signals and joins the host and RPC workers before the failed
/// node's temporary directory is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_routed_tablet_leader_fails_over_after_runtime_drop() {
    let mut nodes = start_failover_test_nodes().await;

    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if nodes.iter().any(|node| node.runtime.handle().is_leader()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the production hosts must elect a metadata leader");

    let request = CreateTableRequest {
        table_name: "stale_route_users".to_string(),
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
    };
    let request_id = RequestId {
        client_id: 0x66,
        sequence: 1,
        raft_group_id: RaftGroupId(2),
    };
    let topology = 'accepted: loop {
        for node in &nodes {
            let creator = node.runtime.metadata_table_creator();
            let attempt = tokio::task::spawn_blocking({
                let request = request.clone();
                let request_id = request_id.clone();
                move || {
                    creator.create_table_topology(request, request_id, Duration::from_millis(500))
                }
            })
            .await
            .expect("metadata proposal task must not panic");
            match attempt {
                Ok(topology) => break 'accepted topology,
                Err(Error::NotLeader { .. } | Error::ProposalUnavailable { .. }) => continue,
                Err(error) => panic!("metadata CREATE TABLE failed permanently: {error}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert!(topology.definition.table_id > 1);

    let routed_group_id = topology.tablets[0].raft_group_id;
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if nodes
                .iter()
                .all(|node| node.runtime.host_status().groups.len() >= 3)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("every assigned node must materialize the routed tablet group");

    for node in &nodes {
        let creator = node.runtime.metadata_table_creator();
        node.database
            .lock()
            .await
            .replace_metadata_table_creator(creator);
    }

    let (old_leader_replica, old_leader_node) =
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let mut leader = None;
                let mut leader_node = None;
                let mut converged = true;
                for (node_index, node) in nodes.iter().enumerate() {
                    let status = node.runtime.host_status();
                    let Some(group) = status
                        .groups
                        .iter()
                        .find(|group| group.identity.raft_group_id == routed_group_id)
                    else {
                        converged = false;
                        break;
                    };
                    let Some(replica) = group.leader_replica_id else {
                        converged = false;
                        break;
                    };
                    if let Some(expected) = leader {
                        if expected != replica {
                            converged = false;
                            break;
                        }
                    } else {
                        leader = Some(replica);
                    }
                    if group.identity.replica_id == replica {
                        leader_node = Some(node_index);
                    }
                }
                if converged
                    && let (Some(leader_replica), Some(leader_node_index)) = (leader, leader_node)
                    && nodes.iter().all(|node| {
                        node.runtime
                            .host_status()
                            .groups
                            .iter()
                            .find(|group| group.identity.raft_group_id == routed_group_id)
                            .and_then(|group| group.leader_replica_id)
                            == Some(leader_replica)
                    })
                {
                    break (leader_replica, leader_node_index);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the routed tablet must publish a converged leader before the stop");

    let gateway_index = (0..nodes.len())
        .find(|index| *index != old_leader_node)
        .expect("a three-node runtime must provide a surviving gateway");
    let gateway_database = nodes[gateway_index].database.clone();
    let inserted = tokio::task::spawn_blocking({
        let gateway_database = gateway_database.clone();
        move || {
            gateway_database.blocking_lock().execute_sql(
                &mut SqlSession::with_client_id(8101),
                "INSERT INTO stale_route_users (id, name) VALUES (42, 'before-failover')",
            )
        }
    })
    .await
    .unwrap()
    .expect("the gateway must warm its route cache through the current leader");
    assert!(matches!(
        inserted,
        ExecutionResult::Mutation {
            affected_rows: 1,
            ..
        }
    ));

    // The metadata route is deliberately made stale without changing the
    // placement. A stale epoch response must re-resolve the complete route
    // instead of patching only the epoch; the same lookup is what keeps a
    // later metadata move to another tablet or Raft group safe.
    let row_key = make_row_key(
        ragnordb_common::ids::TableId(topology.definition.table_id),
        &[Value::Int(42)],
    )
    .unwrap();
    let tablet_client = nodes[gateway_index].runtime.tablet_rpc_client();
    let route = tablet_client
        .lookup_tablet_route(row_key.table_id, &row_key.primary_key_bytes)
        .unwrap();
    let follower = route
        .replicas
        .iter()
        .find(|replica| replica.replica_id != route.leader_replica_id)
        .expect("the routed tablet must have a live follower for the hint test")
        .replica_id;
    let mut follower_route = route.clone();
    follower_route.leader_replica_id = follower;
    let hinted_read = tablet_client
        .read_point(
            &follower_route,
            RequestId {
                client_id: 0x66,
                sequence: 2,
                raft_group_id: follower_route.raft_group_id,
            },
            row_key.clone(),
            Timestamp(u64::MAX),
            Duration::from_secs(5),
        )
        .expect("a live follower must return a typed leader hint and be retried");
    assert_eq!(
        decode_row(&hinted_read.expect("the committed row must remain visible")).unwrap(),
        ragnordb_common::codec::Row {
            values: vec![Value::Int(42), Value::Text("before-failover".to_string())],
        }
    );

    let mut stale_route = route;
    stale_route.tablet_epoch += 1;
    let stale_read = tablet_client
        .read_point(
            &stale_route,
            RequestId {
                client_id: 0x66,
                sequence: 3,
                raft_group_id: stale_route.raft_group_id,
            },
            row_key.clone(),
            Timestamp(u64::MAX),
            Duration::from_secs(5),
        )
        .expect("a stale tablet epoch must refresh the metadata route and retry");
    assert_eq!(
        decode_row(&stale_read.expect("the committed row must remain visible")).unwrap(),
        ragnordb_common::codec::Row {
            values: vec![Value::Int(42), Value::Text("before-failover".to_string())],
        }
    );

    let failed = nodes.swap_remove(old_leader_node);
    let TestNode {
        database,
        runtime,
        _data,
    } = failed;
    drop(runtime);
    drop(database);
    drop(_data);

    let new_leader_replica = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let leaders = nodes
                .iter()
                .filter_map(|node| {
                    node.runtime
                        .host_status()
                        .groups
                        .iter()
                        .find(|group| group.identity.raft_group_id == routed_group_id)
                        .and_then(|group| group.leader_replica_id)
                })
                .collect::<Vec<ReplicaId>>();
            if leaders.len() == nodes.len()
                && leaders.windows(2).all(|pair| pair[0] == pair[1])
                && leaders[0] != old_leader_replica
            {
                break leaders[0];
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the surviving tablet replicas must elect a new leader");
    assert_ne!(new_leader_replica, old_leader_replica);
    // Host status publishes the elected replica before its tablet worker has
    // completed the current-term serving activation used by routed reads.
    // Poll that observable lifecycle boundary instead of using a fixed sleep;
    // the test must remain deterministic on both fast and loaded hosts.
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let serving_new_leader = nodes.iter().any(|node| {
                node.runtime
                    .tablet_rpc_client()
                    .tablet_status(routed_group_id)
                    .is_some_and(|status| {
                        status.serving_leader
                            && status.leader_replica_id == Some(new_leader_replica.0)
                    })
            });
            if serving_new_leader {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the elected tablet leader must cross its serving activation boundary");

    let observed = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking({
            let gateway_database = gateway_database.clone();
            move || {
                let mut session = SqlSession::with_client_id(8102);
                session.set_tablet_request_timeout(Duration::from_secs(5));
                gateway_database.blocking_lock().execute_sql(
                    &mut session,
                    "SELECT id, name FROM stale_route_users WHERE id = 42",
                )
            }
        }),
    )
    .await
    .expect("stale-route read must finish within the bounded failover window")
    .unwrap()
    .expect("the gateway must retry the stale route against the new leader");
    assert_eq!(
        observed,
        ExecutionResult::Query(ragnordb_exec::ResultSet {
            columns: vec![
                ragnordb_exec::ResultColumn {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ragnordb_exec::ResultColumn {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                },
            ],
            rows: vec![ragnordb_common::codec::Row {
                values: vec![Value::Int(42), Value::Text("before-failover".to_string())],
            }],
        })
    );

    // Exercise the mutation-side stale-epoch route refresh after failover has
    // completed. Keeping it after the SQL acceptance prevents this additional
    // command from changing the warmed route cache used by the failover read.
    let mutation_client = nodes[0].runtime.tablet_rpc_client();
    let mut stale_mutation_route = mutation_client
        .lookup_tablet_route(row_key.table_id, &row_key.primary_key_bytes)
        .unwrap();
    stale_mutation_route.tablet_epoch += 1;
    let mutation_row_key = make_row_key(row_key.table_id, &[Value::Int(43)]).unwrap();
    let mutation = TabletCommand::SingleShardCommit(SingleShardCommitCommand {
        txn_id: TxnId(901),
        start_timestamp: Timestamp(1_000),
        commit_timestamp: Timestamp(1_001),
        writes: vec![WriteEntry {
            key: ragnordb_storage::key::encode_row_key(&mutation_row_key).unwrap(),
            row: Some(Row {
                values: vec![Value::Int(43), Value::Text("stale-epoch".to_string())],
            }),
            op: WriteKind::Put,
        }],
    });
    let logical_command_id = LogicalCommandId {
        client_request_id: ClientRequestId {
            client_id: 0x67,
            session_epoch: 1,
            request_sequence: 1,
        },
        command_ordinal: 1,
        kind: CommandKind::SingleShardCommit,
    };
    let command_request_id = RequestId {
        client_id: 0x67,
        sequence: 1,
        raft_group_id: stale_mutation_route.raft_group_id,
    };
    let first_command = mutation_client
        .submit_command_with_identity_and_ack(
            &stale_mutation_route,
            command_request_id.clone(),
            Some(logical_command_id),
            None,
            mutation.clone(),
            Duration::from_secs(5),
        )
        .expect("a stale epoch mutation must refresh its route and apply once");
    assert!(matches!(
        first_command.result,
        ragnordb_tablet::command::TabletCommandApplyResult::SingleShardCommit
    ));
    assert!(!first_command.deduplicated);

    let repeated_command = mutation_client
        .submit_command_with_identity_and_ack(
            &stale_mutation_route,
            command_request_id,
            Some(logical_command_id),
            None,
            mutation,
            Duration::from_secs(5),
        )
        .expect("repeating the same logical command must use the retained outcome");
    assert!(repeated_command.deduplicated);

    drop(gateway_database);
    drop(nodes);
}
