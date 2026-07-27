use ragnordb_common::ids::NodeId;
use ragnordb_exec::SqlSession;
use ragnordb_server::database::LocalDatabase;
use ragnordb_storage::{
    checkpoint::{decode_snapshot_file, encode_snapshot_file},
    recovery::{RecoveryPayload, scan_recovery_records},
};
use wal::{
    config::WalConfig, io::directory::FsSegmentDirectory, lsn::Lsn, types::WalIdentity,
    wal::WalHandle,
};

/// Realistic bug caught:
///
/// If MVCC state and `replay_from_lsn` are sampled outside one serialized
/// barrier, a commit can fall between them. Recovery could then load a row that
/// is already in the snapshot and replay it again, or skip a row that is in
/// neither the image nor the WAL suffix. This test also catches a snapshot
/// retaining references to live MVCC maps that later commits can mutate.
#[test]
fn checkpoint_capture_freezes_state_allocators_and_exact_wal_frontier() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let (mut database, _) =
        LocalDatabase::recover(data_dir.path(), NodeId(1)).expect("empty database must recover");
    let mut session = SqlSession::new();

    database
        .execute_sql(
            &mut session,
            "CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .expect("table creation must succeed");

    database
        .execute_sql(
            &mut session,
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
        )
        .expect("first commit must succeed");

    let first = database
        .capture_checkpoint_image()
        .expect("first consistent cut must succeed");
    let first_bytes = encode_snapshot_file(&first).expect("first image must be encodable");

    database
        .execute_sql(
            &mut session,
            "INSERT INTO users (id, name) VALUES (2, 'Grace')",
        )
        .expect("later commit must succeed");

    let second = database
        .capture_checkpoint_image()
        .expect("second consistent cut must succeed");
    let first_after_later_commit =
        decode_snapshot_file(&first_bytes).expect("first image must remain valid");

    assert_eq!(first_after_later_commit, first);
    assert_eq!(first.snapshot_id, 1);
    assert_eq!(second.snapshot_id, 2);
    assert_eq!(first.tables.len(), 1);
    assert_eq!(first.tables[0].default_values.len(), 1);
    assert_eq!(first.tables[0].writes.len(), 1);
    assert_eq!(second.tables[0].default_values.len(), 2);
    assert_eq!(second.tables[0].writes.len(), 2);
    assert!(first.replay_from_lsn > 0);
    assert!(second.replay_from_lsn > first.replay_from_lsn);

    let first_marks = first
        .high_water_marks
        .expect("first image must contain allocator maxima");
    let second_marks = second
        .high_water_marks
        .expect("second image must contain allocator maxima");

    assert_eq!(
        first_marks
            .max_transaction_id
            .expect("transaction high-water mark must exist")
            .id,
        1
    );
    assert_eq!(
        second_marks
            .max_transaction_id
            .expect("transaction high-water mark must exist")
            .id,
        2
    );
    assert_eq!(first_marks.max_snapshot_id, 1);
    assert_eq!(second_marks.max_snapshot_id, 2);

    // Reopen A-WAL after releasing the live runtime. The first captured
    // frontier must land exactly on the later transaction record, proving it is
    // the end LSN of the last record represented by the first image.
    drop(database);

    let wal_dir = data_dir.path().join("wal");
    let wal_config = WalConfig {
        dir: wal_dir.clone(),
        identity: WalIdentity::new(1, 1, 1),
        ..WalConfig::default()
    };

    let (wal, _) = WalHandle::open(FsSegmentDirectory::new(wal_dir), wal_config, ())
        .expect("WAL must reopen after releasing the live runtime");

    let mut suffix = scan_recovery_records(&wal, Lsn::new(first.replay_from_lsn))
        .expect("captured replay frontier must be a valid WAL boundary");

    let record = suffix
        .next_record()
        .expect("later WAL suffix must decode")
        .expect("later transaction must begin at the captured frontier");

    assert_eq!(record.lsn.as_u64(), first.replay_from_lsn);
    assert!(
        matches!(
            record.payload,
            RecoveryPayload::SingleNodeTxnCommit(commit) if commit.txn_id.0 == 2
        ),
        "first replay record must be the transaction committed after capture"
    );
}
