use std::{net::TcpListener, time::Duration};

use ragnordb_common::{codec::Value, ids::NodeId};
use ragnordb_exec::{ExecutionResult, SqlSession};
use ragnordb_server::{
    config::{NodeConfig, SeedNodeConfig},
    database::{LocalDatabase, SharedLocalDatabase},
    replicated_tablet::ReplicatedTabletRuntime,
};
use tempfile::TempDir;

fn unused_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

struct TestNode {
    database: SharedLocalDatabase,
    runtime: ReplicatedTabletRuntime,
    _data: TempDir,
}

/// Realistic bug caught:
///
/// The low-level Raft and deterministic cluster tests can all pass while the
/// production TCP host remains disconnected from SQL. This test uses three
/// independent durable runtimes and proves that a SQL commit returns through
/// Raft apply and becomes visible in a follower's SQL MVCC mirror.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_commit_crosses_three_node_tcp_raft_and_updates_followers() {
    let seeds = (1..=3)
        .map(|id| SeedNodeConfig {
            id: NodeId(id),
            raft_addr: unused_address(),
            sql_addr: unused_address(),
            admin_addr: unused_address(),
        })
        .collect::<Vec<_>>();
    let mut nodes = Vec::new();

    for seed in &seeds {
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
            seed_nodes: seeds.clone(),
        };
        let (database, _) = LocalDatabase::recover(&config.data_dir, config.node_id).unwrap();
        let wal = database.wal_handle().unwrap();
        let database = database.into_shared();
        let runtime = ReplicatedTabletRuntime::start(&config, wal, database.clone()).unwrap();
        database.lock().await.replace_commit_log(runtime.handle());
        database.lock().await.replace_catalog_log(runtime.handle());
        nodes.push(TestNode {
            database,
            runtime,
            _data: data,
        });
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
                .execute_sql(&mut SqlSession::new(), "SELECT id, name FROM users")
                .unwrap();
            if let ExecutionResult::Query(rows) = result
                && rows.rows
                    == vec![ragnordb_common::codec::Row {
                        values: vec![Value::Int(1), Value::Text("replicated".to_string())],
                    }]
            {
                break;
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
