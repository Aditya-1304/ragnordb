use std::sync::Arc;

use ragnordb_common::{Error, durability::NodeDurabilityState};
use ragnordb_exec::SqlSession;
use ragnordb_server::{
    admin::{AdminState, serve_admin},
    database::LocalDatabase,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_util::sync::CancellationToken;

/// A safe admission rejection must not stop unrelated work, while a sticky-fatal
/// writer rejection must fence every SQL path and checkpoint publication path.
#[tokio::test]
async fn sticky_fatal_wal_rejection_fences_the_complete_local_database() {
    let mut database = LocalDatabase::new();
    let gate = database.durability_gate();
    let mut session = SqlSession::new();

    gate.observe_error(&Error::WalAppendNotStaged {
        reason: "payload exceeds configured maximum".to_string(),
        recovery_required: false,
    });

    database
        .execute_sql(&mut session, "SHOW TABLES")
        .expect("safe admission rejection must not fence reads");

    gate.observe_error(&Error::WalAppendNotStaged {
        reason: "WAL is in sticky-fatal state".to_string(),
        recovery_required: true,
    });

    let sql_error = database
        .execute_sql(&mut session, "SHOW TABLES")
        .expect_err("sticky-fatal WAL state must fence unrelated reads");

    assert!(matches!(sql_error, Error::RecoveryRequired { .. }));

    let shared = database.into_shared();
    let checkpoint_error = LocalDatabase::publish_checkpoint(&shared)
        .await
        .expect_err("checkpoint publication must use the same node-wide gate");

    assert!(matches!(checkpoint_error, Error::RecoveryRequired { .. }));

    let NodeDurabilityState::RecoveryRequired(failure) = gate.state() else {
        panic!("node-wide durability gate must remain fenced");
    };

    assert_eq!(failure.kind().as_str(), "wal_writer_fatal");
}

/// An indeterminate commit can be present in durable history without being
/// reflected in live MVCC state. Both reads and unrelated table writes must
/// stop until restart resolves the authoritative prefix.
#[test]
fn unknown_commit_blocks_subsequent_reads_and_other_table_writes() {
    let mut database = LocalDatabase::new();
    let gate = database.durability_gate();
    let mut session = SqlSession::new();

    database
        .execute_sql(&mut session, "CREATE TABLE first (id INT PRIMARY KEY)")
        .unwrap();
    database
        .execute_sql(&mut session, "CREATE TABLE second (id INT PRIMARY KEY)")
        .unwrap();

    gate.observe_error(&Error::CommitOutcomeUnknown {
        start_lsn: 128,
        end_lsn: 192,
        reason: "injected synchronization uncertainty".to_string(),
    });

    assert!(matches!(
        database.execute_sql(&mut session, "SELECT id FROM first"),
        Err(Error::RecoveryRequired { .. })
    ));
    assert!(matches!(
        database.execute_sql(&mut session, "INSERT INTO second (id) VALUES (1)"),
        Err(Error::RecoveryRequired { .. })
    ));
}

/// Realistic bug caught:
///
/// A live read can discover a broken MVCC write/default relationship or a
/// malformed stored row. Returning that corruption only to the current client
/// would allow later statements to keep using storage whose integrity is no
/// longer established.
#[test]
fn live_storage_corruption_blocks_subsequent_reads_and_writes() {
    let mut database = LocalDatabase::new();
    let gate = database.durability_gate();
    let mut session = SqlSession::new();

    database
        .execute_sql(&mut session, "CREATE TABLE users (id INT PRIMARY KEY)")
        .unwrap();

    // `execute_sql` observes every error returned by the live executor through
    // this same node-wide gate. Injecting the typed error here isolates the
    // durability classification from a particular private storage layout.
    gate.observe_error(&Error::CorruptData(
        "MVCC write references a missing default value".to_string(),
    ));

    assert!(matches!(
        database.execute_sql(&mut session, "SELECT id FROM users"),
        Err(Error::RecoveryRequired { .. })
    ));
    assert!(matches!(
        database.execute_sql(&mut session, "INSERT INTO users (id) VALUES (1)"),
        Err(Error::RecoveryRequired { .. })
    ));

    let NodeDurabilityState::RecoveryRequired(failure) = gate.state() else {
        panic!("live storage corruption must fence the node");
    };
    assert_eq!(failure.kind().as_str(), "storage_corruption");
}

/// Every database-owned durability failure class must cross the same shared
/// SQL admission boundary.
#[test]
fn catalog_apply_and_retention_failures_all_fence_sql() {
    let failures = [
        Error::CatalogOutcomeUnknown {
            start_lsn: 10,
            end_lsn: 20,
            reason: "catalog synchronization is unknown".to_string(),
        },
        Error::RecoveryRequired {
            reason: "durable MVCC apply failed".to_string(),
        },
        Error::RecoveryRequired {
            reason: "checkpoint retention mutation failed".to_string(),
        },
    ];

    for failure in failures {
        let mut database = LocalDatabase::new();
        let gate = database.durability_gate();
        let mut session = SqlSession::new();

        gate.observe_error(&failure);

        assert!(matches!(
            database.execute_sql(&mut session, "SHOW TABLES"),
            Err(Error::RecoveryRequired { .. })
        ));
    }
}

/// Checkpoint uncertainty must prevent another publication attempt from
/// reaching file or retention mutation, even on an otherwise empty runtime.
#[tokio::test]
async fn checkpoint_unknown_blocks_later_checkpoint() {
    let database = LocalDatabase::new();
    let gate = database.durability_gate();
    let shared = database.into_shared();

    gate.observe_error(&Error::CheckpointOutcomeUnknown {
        stage: "CheckpointMarker",
        start_lsn: 200,
        end_lsn: 240,
        reason: "marker synchronization is unknown".to_string(),
    });

    assert!(matches!(
        LocalDatabase::publish_checkpoint(&shared).await,
        Err(Error::RecoveryRequired { .. })
    ));
}

/// Administrative health remains available after fail-stop and exposes the
/// first authoritative classification and diagnostic reason.
#[tokio::test]
async fn status_reports_recovery_required_reason() {
    let gate = ragnordb_common::durability::DurabilityGate::new();
    gate.observe_error(&Error::CommitOutcomeUnknown {
        start_lsn: 300,
        end_lsn: 360,
        reason: "status-visible synchronization uncertainty".to_string(),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let state = Arc::new(AdminState {
        started_at: 1,
        connection_semaphore: Arc::new(Semaphore::new(8)),
        max_connections: 8,
        durability_gate: gate,
        database: LocalDatabase::shared(),
        replicated_tablet: None,
        multiraft_status: None,
    });
    let server = tokio::spawn(async move {
        serve_admin(listener, state, server_shutdown).await.unwrap();
    });

    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap();
    let status: serde_json::Value = serde_json::from_str(body).unwrap();

    assert_eq!(status["durability"]["recovery_required"], true);
    assert_eq!(status["durability"]["state"], "commit_outcome_unknown");
    assert!(
        status["durability"]["reason"]
            .as_str()
            .unwrap()
            .contains("status-visible synchronization uncertainty")
    );

    shutdown.cancel();
    server.await.unwrap();
}
