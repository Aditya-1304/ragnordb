use ragnordb_common::{
    codec::{Row, Value},
    ids::NodeId,
};
use ragnordb_exec::{ExecutionResult, ResultSet, SqlSession};
use ragnordb_server::database::LocalDatabase;
use wal::lsn::Lsn;

fn result_set(result: ExecutionResult) -> ResultSet {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query result");
    };

    result
}

/// Realistic bug caught:
///
/// Checkpoint components could work independently while the live database still
/// had no operation that safely coupled:
///
/// 1. consistent-cut capture,
/// 2. durable snapshot-file publication,
/// 3. durable pointer/marker publication,
/// 4. release of the snapshot's retention pin,
/// 5. advancement of the WAL retention floor.
///
/// That missing ownership forced callers to drop the database and reopen A-WAL,
/// creating a second WAL-owner path and leaving retention advancement outside
/// the checkpoint's correctness boundary.
#[tokio::test]
async fn live_database_publishes_checkpoint_and_recovers_its_wal_suffix() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let node_id = NodeId(31);

    let (database, _) =
        LocalDatabase::recover(data_dir.path(), node_id).expect("empty recovery must succeed");
    let database = database.into_shared();
    let mut session = SqlSession::new();

    {
        let mut runtime = database.lock().await;

        runtime
            .execute_sql(
                &mut session,
                "CREATE TABLE users (
                    id INT PRIMARY KEY,
                    name TEXT NOT NULL
                )",
            )
            .expect("table creation must become durable");

        runtime
            .execute_sql(
                &mut session,
                "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            )
            .expect("checkpoint-covered row must become durable");
    }

    // RED: this associated API does not exist yet.
    let publication = LocalDatabase::publish_checkpoint(&database)
        .await
        .expect("the live database must publish its complete checkpoint");

    assert_eq!(publication.checkpoint.snapshot_id, 1);
    assert!(publication.checkpoint.replay_from_lsn > Lsn::ZERO);
    assert_eq!(
        publication.retention_advanced_to,
        publication.checkpoint.replay_from_lsn
    );
    assert!(
        publication.checkpoint.pointer_extent.end_lsn
            <= publication.checkpoint.marker_extent.start_lsn
    );

    // This commit occurs after the captured frontier and must therefore be
    // reconstructed from the WAL suffix following snapshot restoration.
    {
        let mut runtime = database.lock().await;

        runtime
            .execute_sql(
                &mut session,
                "INSERT INTO users (id, name) VALUES (2, 'Grace')",
            )
            .expect("post-checkpoint commit must become durable");
    }

    drop(session);
    drop(database);

    let (mut recovered, _) = LocalDatabase::recover(data_dir.path(), node_id)
        .expect("checkpoint-based recovery must succeed");
    let mut recovered_session = SqlSession::new();

    let rows = result_set(
        recovered
            .execute_sql(&mut recovered_session, "SELECT id, name FROM users")
            .expect("snapshot state and its WAL suffix must be queryable"),
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
}

/// A checkpoint-only workload must advance past the previous checkpoint's WAL
/// metadata so obsolete pointer and marker records eventually become prunable.
#[tokio::test]
async fn second_checkpoint_without_dml_advances_the_physical_replay_frontier() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let node_id = NodeId(32);

    let (database, _) =
        LocalDatabase::recover(data_dir.path(), node_id).expect("empty database must recover");
    let database = database.into_shared();

    let first = LocalDatabase::publish_checkpoint(&database)
        .await
        .expect("first checkpoint must publish");

    let second = LocalDatabase::publish_checkpoint(&database)
        .await
        .expect("second checkpoint must publish without intervening DML");

    assert!(
        second.checkpoint.replay_from_lsn > first.checkpoint.replay_from_lsn,
        "second checkpoint must cover the first checkpoint's WAL metadata"
    );

    assert_eq!(
        second.checkpoint.replay_from_lsn, first.checkpoint.marker_extent.end_lsn,
        "the second capture must use A-WAL's exact durable frontier"
    );
}
