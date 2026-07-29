use std::{fs, path::Path};

use ragnordb_common::{
    Error,
    codec::{Row, Value},
    ids::{NodeId, Timestamp, TxnId},
};
use ragnordb_exec::{ExecutionResult, ResultSet, SqlSession};
use ragnordb_server::database::LocalDatabase;
use ragnordb_storage::{
    checkpoint::{PublishedSnapshotFile, publish_checkpoint, publish_snapshot_file},
    wal::RagnorDbWalAdapter,
};
use wal::{
    config::WalConfig, io::directory::FsSegmentDirectory, types::WalIdentity, wal::WalHandle,
};

fn result_set(result: ExecutionResult) -> ResultSet {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query result");
    };

    result
}

fn open_checkpoint_adapter(
    data_dir: &Path,
    node_id: NodeId,
) -> RagnorDbWalAdapter<FsSegmentDirectory, ()> {
    let wal_dir = data_dir.join("wal");
    let config = WalConfig {
        dir: wal_dir.clone(),
        identity: WalIdentity::new(node_id.0, 1, 1),
        ..WalConfig::default()
    };

    let (wal, _) = WalHandle::open(FsSegmentDirectory::new(wal_dir), config, ())
        .expect("checkpoint WAL must reopen");

    RagnorDbWalAdapter::new(wal)
}

fn durably_publish_checkpoint_metadata(
    data_dir: &Path,
    node_id: NodeId,
    snapshot_file: &PublishedSnapshotFile,
) {
    let adapter = open_checkpoint_adapter(data_dir, node_id);
    let published = publish_checkpoint(&adapter, snapshot_file)
        .expect("checkpoint pointer and marker must become durable");

    assert!(published.pointer_extent.end_lsn <= published.marker_extent.start_lsn);
}

/// Realistic bug caught:
///
/// Startup could validate a checkpoint pair but still replay from `Lsn::ZERO`,
/// or restore the snapshot without applying commits at its captured suffix
/// boundary. Either error duplicates covered writes or loses commits completed
/// while the immutable snapshot file was being published.
#[test]
fn startup_restores_checkpoint_then_replays_only_its_wal_suffix() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let node_id = NodeId(17);
    let (mut database, _) =
        LocalDatabase::recover(data_dir.path(), node_id).expect("empty recovery must succeed");
    let mut session = SqlSession::new();

    database
        .execute_sql(
            &mut session,
            "CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .expect("table creation must become durable");

    database
        .execute_sql(
            &mut session,
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
        )
        .expect("checkpoint-covered row must become durable");

    let snapshot = database
        .capture_checkpoint_image()
        .expect("consistent checkpoint image must be captured");

    let snapshot_file = publish_snapshot_file(data_dir.path(), &snapshot)
        .expect("snapshot file must become durable");

    database
        .execute_sql(
            &mut session,
            "INSERT INTO users (id, name) VALUES (2, 'Grace')",
        )
        .expect("post-checkpoint row must become durable");

    drop(session);
    drop(database);

    durably_publish_checkpoint_metadata(data_dir.path(), node_id, &snapshot_file);

    let (mut recovered, _) = LocalDatabase::recover(data_dir.path(), node_id)
        .expect("checkpoint-based startup must succeed");
    let mut recovered_session = SqlSession::new();

    let started = recovered
        .execute_sql(&mut recovered_session, "BEGIN")
        .expect("post-recovery transaction allocation must succeed");

    assert!(matches!(
        started,
        ExecutionResult::TransactionStarted {
            transaction_id: TxnId(3),
            start_ts: Timestamp(7),
        }
    ));

    let rows = result_set(
        recovered
            .execute_sql(&mut recovered_session, "SELECT id, name FROM users")
            .expect("restored table and WAL suffix must be queryable"),
    );

    assert_eq!(
        rows.rows,
        vec![
            Row {
                values: vec![Value::Int(1), Value::Text("Ada".to_string())],
            },
            Row {
                values: vec![Value::Int(2), Value::Text("Grace".to_string())],
            },
        ]
    );

    recovered
        .execute_sql(&mut recovered_session, "ROLLBACK")
        .expect("recovered transaction must roll back");

    let next_snapshot = recovered
        .capture_checkpoint_image()
        .expect("snapshot allocator must resume above the recovered checkpoint");

    assert_eq!(next_snapshot.snapshot_id, 2);
}

/// Realistic bug caught:
///
/// Startup could select a durable pointer/marker pair but ignore a missing or
/// corrupt referenced file and silently fall back to full WAL replay. Once
/// retention has pruned the covered prefix, that fallback no longer exists, so
/// accepting the startup would expose incomplete or unrecoverable state.
#[test]
fn startup_fails_closed_when_selected_snapshot_is_corrupt() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let node_id = NodeId(18);
    let (mut database, _) =
        LocalDatabase::recover(data_dir.path(), node_id).expect("empty recovery must succeed");
    let mut session = SqlSession::new();

    database
        .execute_sql(
            &mut session,
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)",
        )
        .expect("table creation must become durable");

    database
        .execute_sql(
            &mut session,
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
        )
        .expect("row insertion must become durable");

    let snapshot = database
        .capture_checkpoint_image()
        .expect("consistent checkpoint image must be captured");

    let snapshot_file = publish_snapshot_file(data_dir.path(), &snapshot)
        .expect("snapshot file must become durable");

    let snapshot_path = data_dir.path().join(snapshot_file.relative_path());

    drop(session);
    drop(database);

    durably_publish_checkpoint_metadata(data_dir.path(), node_id, &snapshot_file);

    let mut bytes = fs::read(&snapshot_path).expect("published snapshot must be readable");
    let last = bytes
        .last_mut()
        .expect("published snapshot must contain a protobuf body");

    *last ^= 0x01;

    fs::write(&snapshot_path, bytes).expect("snapshot corruption must be installed");

    let error = match LocalDatabase::recover(data_dir.path(), node_id) {
        Err(error) => error,
        Ok(_) => panic!("startup must reject the corrupt selected snapshot"),
    };

    assert!(matches!(
        error,
        Error::CorruptData(message) if message.contains("checksum mismatch")
    ));
}
