use std::{path::Path, process::Command};

use ragnordb_common::ids::{NodeId, Timestamp};
use ragnordb_exec::SqlSession;
use ragnordb_server::database::LocalDatabase;
use ragnordb_storage::wal::{CheckpointMarker, RagnorDbWalRecordType};
use wal::{
    config::WalConfig, io::directory::FsSegmentDirectory, lsn::Lsn, types::WalIdentity,
    wal::WalHandle,
};

/// Open the same writable A-WAL configuration that `LocalDatabase::recover`
/// uses. The helper is intentionally limited to test setup; the CLI inspector
/// itself always opens the WAL read-only.
fn open_wal_for_write(data_dir: &Path, node_id: NodeId) -> WalHandle<FsSegmentDirectory, ()> {
    let wal_dir = data_dir.join("wal");
    let config = WalConfig {
        dir: wal_dir.clone(),
        identity: WalIdentity::new(node_id.0, 1, 1),
        ..WalConfig::default()
    };

    WalHandle::open(FsSegmentDirectory::new(wal_dir), config, ())
        .expect("test WAL must open")
        .0
}

/// Realistic bug caught:
///
/// A physical WAL record can have valid A-WAL framing and checksum while its
/// RagnorDB protobuf payload is malformed. Stopping at that record hides both
/// the useful corruption diagnostic and later records an operator needs to
/// assess the affected log. The inspector must report the malformed payload,
/// preserve its physical recovery report, continue best-effort output, and
/// return a failing process status.
#[test]
fn inspect_wal_reports_malformed_database_payload_and_continues_listing() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let node_id = NodeId(8);

    let (mut database, _) =
        LocalDatabase::recover(data_dir.path(), node_id).expect("empty database must recover");
    let mut session = SqlSession::new();

    database
        .execute_sql(
            &mut session,
            "CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .expect("catalog update must become durable");

    drop(database);

    let wal = open_wal_for_write(data_dir.path(), node_id);

    // The record is physically valid because A-WAL writes its normal framing
    // and checksum. Its one-byte payload is deliberately not a decodable
    // `SingleNodeTxnCommit` protobuf.
    let malformed_extent = wal
        .append_and_sync(
            RagnorDbWalRecordType::SingleNodeTxnCommit.as_wal_record_type(),
            &[0xff],
        )
        .expect("malformed semantic payload must become physically durable");

    let marker_payload = CheckpointMarker {
        snapshot_id: 1,
        snapshot_timestamp: Timestamp(1),
        replay_from_lsn: Lsn::ZERO,
    }
    .encode()
    .expect("valid checkpoint marker must encode");

    let _marker_extent = wal
        .append_and_sync(
            RagnorDbWalRecordType::CheckpointMarker.as_wal_record_type(),
            &marker_payload,
        )
        .expect("valid record after malformed payload must become durable");

    drop(wal);

    let output = Command::new(env!("CARGO_BIN_EXE_ragnordb"))
        .arg("inspect")
        .arg("wal")
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--node-id")
        .arg(node_id.0.to_string())
        .output()
        .expect("inspect command must start");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined_output = format!("{stdout}\n{stderr}");

    assert!(
        !output.status.success(),
        "a malformed database payload must make inspection fail\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    let expected_diagnostic = format!(
        "malformed_database_payload: lsn={} record_type={}",
        malformed_extent.start_lsn.as_u64(),
        RagnorDbWalRecordType::SingleNodeTxnCommit
            .as_wal_record_type()
            .as_u16(),
    );

    let report_index = stdout
        .find("physical_recovery:")
        .expect("A-WAL physical report must remain visible");
    let diagnostic_index = stdout
        .find(&expected_diagnostic)
        .expect("malformed RagnorDB payload must include its LSN and raw type");

    assert!(
        report_index < diagnostic_index,
        "physical recovery diagnostics must print before semantic payload errors:\n{stdout}"
    );
    assert!(
        stdout.contains("failed to decode SingleNodeTxnCommit"),
        "malformed payload output must retain the semantic decoder reason:\n{stdout}"
    );
    assert!(
        stdout[diagnostic_index..].contains("type=CheckpointMarker"),
        "inspection must continue past a malformed payload and list later valid records:\n{stdout}"
    );
    assert!(
        combined_output.contains("WAL inspection found 1 malformed RagnorDB payload(s)"),
        "the process failure must summarize the semantic corruption count:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// Realistic bug caught:
///
/// A physical WAL inspection command could report that records are readable
/// without revealing which durable RagnorDB operations they represent. That
/// leaves an operator unable to identify the table and commit contained in a
/// recovery-critical log. This test exercises the public CLI against a real
/// catalog update and transaction commit.
#[test]
fn inspect_wal_prints_physical_diagnostics_and_decoded_database_records() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let node_id = NodeId(7);

    let (mut database, _) =
        LocalDatabase::recover(data_dir.path(), node_id).expect("empty database must recover");
    let mut session = SqlSession::new();

    database
        .execute_sql(
            &mut session,
            "CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .expect("catalog update must become durable");

    database
        .execute_sql(
            &mut session,
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
        )
        .expect("transaction commit must become durable");

    drop(database);

    let output = Command::new(env!("CARGO_BIN_EXE_ragnordb"))
        .arg("inspect")
        .arg("wal")
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--node-id")
        .arg(node_id.0.to_string())
        .output()
        .expect("inspect command must start");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "inspect wal must succeed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("physical_recovery:"),
        "inspection must preserve A-WAL physical recovery diagnostics:\n{stdout}"
    );
    assert!(
        stdout.contains("records_scanned:"),
        "inspection must expose the structured A-WAL recovery report:\n{stdout}"
    );
    assert!(
        stdout.contains("ragnordb_records:"),
        "inspection must separate decoded database records from physical diagnostics:\n{stdout}"
    );
    assert!(
        stdout.contains("type=CatalogUpdate"),
        "inspection must identify durable catalog records:\n{stdout}"
    );
    assert!(
        stdout.contains("summary=\"catalog_update create_table name=users"),
        "inspection must summarize the decoded catalog operation:\n{stdout}"
    );
    assert!(
        stdout.contains("type=SingleNodeTxnCommit"),
        "inspection must identify durable transaction commits:\n{stdout}"
    );
    assert!(
        stdout.contains("commit_timestamp=") && stdout.contains("table_id=1"),
        "inspection must show the transaction visibility timestamp and table identity:\n{stdout}"
    );
    assert!(
        stdout.contains("writes=1 puts=1 deletes=0"),
        "inspection must summarize the decoded transaction mutation batch:\n{stdout}"
    );
}
