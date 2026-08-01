use std::fs;

use ragnordb_common::{
    Error,
    ids::{TableId, Timestamp, TxnId},
    proto::snapshot as snapshot_proto,
};
use ragnordb_storage::checkpoint::{
    MAX_SNAPSHOT_FILE_BYTES, SNAPSHOT_FILE_VERSION, encode_snapshot_file,
    load_snapshot_file_bounded,
};

fn empty_snapshot() -> snapshot_proto::DatabaseSnapshot {
    snapshot_proto::DatabaseSnapshot {
        snapshot_id: 1,
        snapshot_timestamp: Some(Timestamp(1).to_proto()),
        replay_from_lsn: 0,
        high_water_marks: Some(snapshot_proto::AllocatorHighWaterMarks {
            max_transaction_id: Some(TxnId(0).to_proto()),
            max_timestamp: Some(Timestamp(1).to_proto()),
            max_table_id: Some(TableId(0).to_proto()),
            max_snapshot_id: 1,
        }),
        tables: Vec::new(),
    }
}

/// Verifies that the pointer-declared size is checked before the snapshot body
/// is allocated or read.
///
/// Realistic bug caught:
///
/// A corrupt pointer could make startup reserve an attacker-controlled amount
/// of memory before the loader notices that the selected snapshot is too large.
#[test]
fn selected_snapshot_is_rejected_before_allocating_an_oversized_body() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("oversized.ragnor");
    let file = fs::File::create(&path).unwrap();

    file.set_len(MAX_SNAPSHOT_FILE_BYTES + 1).unwrap();

    let error =
        load_snapshot_file_bounded(&path, MAX_SNAPSHOT_FILE_BYTES + 1, 0, SNAPSHOT_FILE_VERSION)
            .unwrap_err();

    assert!(matches!(error, Error::CorruptData(_)));
}

/// Verifies that recovery binds the selected snapshot to the checksum carried
/// by its durable WAL pointer.
///
/// Realistic bug caught:
///
/// A snapshot file could be replaced with different bytes of the same length
/// and otherwise pass the pointer's identity and size checks during restart.
#[test]
fn selected_snapshot_must_match_the_pointer_bound_whole_file_checksum() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("snapshot.ragnor");
    let bytes = encode_snapshot_file(&empty_snapshot()).unwrap();
    let expected_checksum = crc32c::crc32c(&bytes);

    fs::write(&path, &bytes).unwrap();

    load_snapshot_file_bounded(
        &path,
        bytes.len() as u64,
        expected_checksum,
        SNAPSHOT_FILE_VERSION,
    )
    .expect("exact pointer-bound snapshot must load");

    let mut replaced = bytes;
    let last = replaced
        .last_mut()
        .expect("encoded snapshot cannot be empty");
    *last ^= 0x01;
    fs::write(&path, &replaced).unwrap();

    let error = load_snapshot_file_bounded(
        &path,
        replaced.len() as u64,
        expected_checksum,
        SNAPSHOT_FILE_VERSION,
    )
    .unwrap_err();

    assert!(matches!(error, Error::CorruptData(_)));
    assert!(error.to_string().contains("whole-file checksum mismatch"));
}
