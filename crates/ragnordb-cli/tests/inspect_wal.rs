use std::process::Command;

use ragnordb_common::ids::NodeId;
use ragnordb_exec::SqlSession;
use ragnordb_server::database::LocalDatabase;

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
