use std::{
    net::TcpListener,
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

use ragnordb_common::{
    Error,
    catalog_codec::{ColumnDefinition, DataType},
    codec::{Row, Value, WriteKind},
    command_codec::{SingleShardCommitCommand, TabletCommand, WriteEntry},
    encoding::decode_row,
    ids::{
        ClientRequestId, ColumnId, CommandKind, LogicalCommandId, NodeId, RaftGroupId, ReplicaId,
        RequestId, Timestamp, TxnId,
    },
    metadata_codec::CreateTableRequest,
};
use ragnordb_exec::{ExecutionResult, SqlSession};
use ragnordb_server::{
    config::{NodeConfig, SeedNodeConfig},
    data_directory_lock::DataDirectoryLock,
    database::{LocalDatabase, SharedLocalDatabase},
    multiraft_runtime::MultiRaftRuntime,
};
use ragnordb_storage::key::make_row_key;
use tempfile::TempDir;

fn unused_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

struct TestNode {
    database: SharedLocalDatabase,
    runtime: MultiRaftRuntime,
    _data: TempDir,
}

fn start_failover_test_node(
    seed: SeedNodeConfig,
    all_seeds: Vec<SeedNodeConfig>,
    cluster_id: String,
) -> TestNode {
    let data = tempfile::tempdir().unwrap();
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

    TestNode {
        database,
        runtime,
        _data: data,
    }
}

async fn start_failover_test_nodes() -> Vec<TestNode> {
    let seeds = (1..=3)
        .map(|id| SeedNodeConfig {
            id: NodeId(id),
            raft_addr: unused_address(),
            snapshot_addr: unused_address(),
            sql_addr: unused_address(),
            admin_addr: unused_address(),
            region: None,
            zone: None,
            rack: None,
            storage_class: "default".to_string(),
        })
        .collect::<Vec<_>>();
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

/// Realistic bugs caught:
///
/// The low-level Raft and deterministic cluster tests can all pass while the
/// production TCP host remains disconnected from SQL. This test uses three
/// independent durable runtimes. It also verifies that simultaneous latest
/// reads do not reuse one internal request identity while their first barrier
/// is still awaiting apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_runtime_admits_concurrent_barriers_and_replicates_sql_commit() {
    let seeds = (1..=3)
        .map(|id| SeedNodeConfig {
            id: NodeId(id),
            raft_addr: unused_address(),
            snapshot_addr: unused_address(),
            sql_addr: unused_address(),
            admin_addr: unused_address(),
            region: None,
            zone: None,
            rack: None,
            storage_class: "default".to_string(),
        })
        .collect::<Vec<_>>();
    // Replicated startup waits for metadata initialization to commit and apply.
    // Start every configured node together so the metadata Raft group can form
    // its initial quorum before any startup call waits for completion.
    let startup_handles = seeds
        .iter()
        .cloned()
        .map(|seed| {
            let all_seeds = seeds.clone();

            tokio::task::spawn_blocking(move || {
                let data = tempfile::tempdir().unwrap();
                let config = NodeConfig {
                    node_id: seed.id,
                    data_dir: data.path().to_path_buf(),
                    listen_addr: seed.sql_addr,
                    admin_addr: seed.admin_addr,
                    max_connections: 8,
                    statement_timeout_ms: 5_000,
                    shutdown_grace_period_ms: 1_000,
                    statement_logging: ragnordb_server::config::StatementLogging::Off,
                    cluster_id: Some("runtime-test".to_string()),
                    bootstrap: true,
                    seed_nodes: all_seeds,
                    snapshot_interval_entries: 100_000,
                    snapshot_interval_bytes: 256 * 1024 * 1024,
                    snapshot_min_elapsed_ms: 300_000,
                    max_snapshot_file_bytes: 512 * 1024 * 1024,
                    snapshot_chunk_bytes: 1024 * 1024,
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

    let leader_catalog = nodes[leader].database.clone();
    tokio::task::spawn_blocking(move || {
        leader_catalog.blocking_lock().execute_sql(
            &mut SqlSession::new(),
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)",
        )
    })
    .await
    .unwrap()
    .expect("catalog success must also be resolved from replicated apply");

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

    for node in &nodes {
        let creator = node.runtime.metadata_table_creator();
        node.database
            .lock()
            .await
            .replace_metadata_table_creator(creator);
    }

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
