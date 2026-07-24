use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use ragnordb_common::{
    codec::{Row, Value},
    ids::{NodeId, Timestamp, TxnId},
};
use ragnordb_exec::{ExecutionResult, ResultSet, SqlSession};
use ragnordb_server::database::LocalDatabase;
use wal::lsn::Lsn;

/// isolated database directory owned by one restart test
struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must follow the Unix epoch")
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "ragnordb-server-recovery-{prefix}-{}-{nanos}",
            process::id()
        ));

        fs::create_dir_all(&path).expect("failed to create recovery test directory");

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn result_set(result: ExecutionResult) -> ResultSet {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query result");
    };

    result
}

/// verifies the complete process local restart boundary
///
/// Realistic bug caught:
///
/// Startup could successfully open and repair A-WAL but then discard the
/// reconstructed catalog, MVCC state, or allocator floors by constructing the
/// server's previous empty `LocalDatabase`
#[test]
fn startup_replays_durable_state_and_restores_allocator_floors() {
    let test_dir = TestDir::new("durable-restart");
    let node_id = NodeId(7);

    let (mut first_database, first_report) = LocalDatabase::recover(test_dir.path(), node_id)
        .expect("empty database recovery must succeed");

    assert_eq!(first_report.next_lsn, Lsn::ZERO);

    let mut first_session = SqlSession::new();

    first_database
        .execute_sql(
            &mut first_session,
            "CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .expect("table creation must become durable");

    first_database
        .execute_sql(
            &mut first_session,
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
        )
        .expect("row insertion must become durable");

    // dropping the complete runtime simulates process loss after every logical
    // operation has synchronously reached A-WAL
    drop(first_session);
    drop(first_database);

    let (mut recovered_database, recovery_report) =
        LocalDatabase::recover(test_dir.path(), node_id)
            .expect("database restart recovery must succeed");

    assert!(
        recovery_report.records_scanned >= 2,
        "A-WAL recovery must report the durable catalog and commit records"
    );
    assert!(recovery_report.next_lsn > Lsn::ZERO);

    let mut recovered_session = SqlSession::new();

    let started = recovered_database
        .execute_sql(&mut recovered_session, "BEGIN")
        .expect("post-recovery transaction must begin");

    // Durable history used transaction ID 1 and timestamps through 3:
    //
    // 1 = catalog update
    // 2 = INSERT transaction start
    // 3 = INSERT transaction commit
    assert!(matches!(
        started,
        ExecutionResult::TransactionStarted {
            transaction_id: TxnId(2),
            start_ts: Timestamp(4),
        }
    ));

    let rows = result_set(
        recovered_database
            .execute_sql(&mut recovered_session, "SELECT id, name FROM users")
            .expect("recovered table must be queryable"),
    );

    assert_eq!(
        rows.rows,
        vec![Row {
            values: vec![Value::Int(1), Value::Text("Ada".to_string()),],
        }]
    );

    recovered_database
        .execute_sql(&mut recovered_session, "ROLLBACK")
        .expect("recovered transaction must roll back");
}
