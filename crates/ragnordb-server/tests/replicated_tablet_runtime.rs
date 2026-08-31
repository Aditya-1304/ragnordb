use std::{
    net::TcpListener,
    sync::{Arc, Barrier},
    time::Duration,
};

use ragnordb_common::{Error, codec::Value, ids::NodeId};
use ragnordb_exec::{ExecutionResult, SqlSession};
use ragnordb_server::{
    config::{NodeConfig, SeedNodeConfig},
    data_directory_lock::DataDirectoryLock,
    database::{LocalDatabase, SharedLocalDatabase},
    multiraft_runtime::MultiRaftRuntime,
};
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

        node.database
            .lock()
            .await
            .replace_commit_log(handle.clone());
        node.database.lock().await.replace_catalog_log(handle);

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

    // Both callers must receive independently tracked barriers. A shared
    // request identity would reject one caller before Raft can commit either
    // no-op, which makes healthy concurrent latest reads unavailable.
    let start = Arc::new(Barrier::new(3));
    let first = nodes[leader].runtime.handle();
    let second = nodes[leader].runtime.handle();
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

    // Keep all lifecycle guards alive until assertions complete. Runtime Drop
    // performs an orderly Ready-owner shutdown.
    drop(nodes);
}
