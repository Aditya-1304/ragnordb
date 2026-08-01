use ragnordb_common::{Error, durability::NodeDurabilityState};
use ragnordb_exec::SqlSession;
use ragnordb_server::database::LocalDatabase;

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
