use std::fs;

use ragnordb_common::{
    ids::{TableId, Timestamp, TxnId},
    proto::snapshot as snapshot_proto,
};
use ragnordb_storage::checkpoint::{
    SNAPSHOT_DIRECTORY_NAME, decode_snapshot_file, publish_snapshot_file,
};

fn empty_snapshot(snapshot_id: u64) -> snapshot_proto::DatabaseSnapshot {
    snapshot_proto::DatabaseSnapshot {
        snapshot_id,
        snapshot_timestamp: Some(Timestamp(40).to_proto()),
        replay_from_lsn: 4096,
        high_water_marks: Some(snapshot_proto::AllocatorHighWaterMarks {
            max_transaction_id: Some(TxnId(12).to_proto()),
            max_timestamp: Some(Timestamp(40).to_proto()),
            max_table_id: Some(TableId(0).to_proto()),
            max_snapshot_id: snapshot_id,
        }),
        tables: Vec::new(),
    }
}

/// Realistic bug caught:
///
/// A crash can leave either a partial temporary file or an orphan final file
/// whose WAL pointer was never published. Retrying the same non-durable
/// snapshot identity must replace those bytes with one complete, decodable
/// file and must not expose the temporary name as the published checkpoint.
#[test]
fn snapshot_publication_replaces_crash_leftovers_with_complete_final_file() {
    let data_dir = tempfile::tempdir().expect("temporary data directory must be created");
    let snapshot_dir = data_dir.path().join(SNAPSHOT_DIRECTORY_NAME);

    fs::create_dir_all(&snapshot_dir).expect("snapshot directory must be created");
    fs::write(
        snapshot_dir.join(".snapshot-7.ragnor.tmp"),
        b"partial temporary bytes",
    )
    .expect("stale temporary file must be created");
    fs::write(
        snapshot_dir.join("snapshot-7.ragnor"),
        b"orphan final bytes",
    )
    .expect("orphan final file must be created");

    let expected = empty_snapshot(7);
    let published = publish_snapshot_file(data_dir.path(), &expected)
        .expect("snapshot publication must succeed");
    let final_path = data_dir.path().join(&published.relative_path);
    let final_bytes = fs::read(&final_path).expect("published snapshot must be readable");
    let decoded =
        decode_snapshot_file(&final_bytes).expect("published snapshot must be complete and valid");
    let remaining_names = fs::read_dir(&snapshot_dir)
        .expect("snapshot directory must remain readable")
        .map(|entry| {
            entry
                .expect("snapshot directory entry must be readable")
                .file_name()
        })
        .collect::<Vec<_>>();

    assert_eq!(published.relative_path, "snapshots/snapshot-7.ragnor");
    assert_eq!(published.file_length, final_bytes.len() as u64);
    assert_eq!(decoded, expected);
    assert_eq!(remaining_names, vec!["snapshot-7.ragnor"]);
}
