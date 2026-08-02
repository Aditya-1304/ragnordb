use ragnordb_common::ids::NodeId;
use ragnordb_server::database::LocalDatabase;

/// Realistic bug caught:
///
/// Server shutdown could merely drop the WAL handle after draining clients,
/// forcing every normal restart through crash recovery and never publishing the
/// clean-shutdown witness that the graceful-shutdown contract promises.
#[test]
fn explicit_database_shutdown_publishes_a_clean_restart_witness() {
    let data_dir = tempfile::tempdir().expect("temporary data directory must be created");
    let (mut database, initial_report) = LocalDatabase::recover(data_dir.path(), NodeId(1))
        .expect("initial database recovery must succeed");

    assert!(!initial_report.clean_shutdown);

    database
        .shutdown()
        .expect("database shutdown must synchronize A-WAL's clean witness");
    drop(database);

    let (reopened, report) = LocalDatabase::recover(data_dir.path(), NodeId(1))
        .expect("cleanly shut down database must reopen");

    assert!(report.clean_shutdown);
    drop(reopened);
}
