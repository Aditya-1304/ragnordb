use std::collections::BTreeSet;

use ragnordb_catalog::Catalog;
use ragnordb_common::{
    Error,
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    codec::{Row, Value, WriteKind, WriteRecord},
    encoding::encode_row,
    ids::{ColumnId, TableId, Timestamp, TxnId},
    proto::snapshot as snapshot_proto,
};
use ragnordb_storage::{
    checkpoint::{PublishedSnapshotFile, encode_snapshot_file, publish_snapshot_file},
    key::{encode_row_key, make_row_key},
    mvcc::MvccStorage,
    recovery::{RecoveryCheckpointCandidate, load_recovery_checkpoint},
    wal::SnapshotPointer,
};
use wal::lsn::Lsn;

fn test_snapshot() -> (snapshot_proto::DatabaseSnapshot, Vec<u8>, Vec<u8>) {
    let table_id = TableId(7);
    let key = encode_row_key(&make_row_key(table_id, &[Value::Int(42)]).unwrap()).unwrap();
    let row = encode_row(&Row {
        values: vec![Value::Int(42), Value::Text("Ada".to_string())],
    })
    .unwrap();
    let write = WriteRecord {
        start_timestamp: Timestamp(80),
        commit_timestamp: Timestamp(90),
        op: WriteKind::Put,
    };
    let snapshot = snapshot_proto::DatabaseSnapshot {
        snapshot_id: 11,
        snapshot_timestamp: Some(Timestamp(90).to_proto()),
        replay_from_lsn: 4096,
        high_water_marks: Some(snapshot_proto::AllocatorHighWaterMarks {
            max_transaction_id: Some(TxnId(81).to_proto()),
            max_timestamp: Some(Timestamp(90).to_proto()),
            max_table_id: Some(table_id.to_proto()),
            max_snapshot_id: 11,
        }),
        tables: vec![snapshot_proto::SnapshotTable {
            definition: Some(
                TableDefinition {
                    table_id: table_id.0,
                    name: "users".to_string(),
                    columns: vec![
                        ColumnDefinition {
                            column_id: ColumnId(1),
                            name: "id".to_string(),
                            ty: DataType::Int,
                            nullable: false,
                        },
                        ColumnDefinition {
                            column_id: ColumnId(2),
                            name: "name".to_string(),
                            ty: DataType::Text,
                            nullable: false,
                        },
                    ],
                    primary_key_column_ids: vec![ColumnId(1)],
                    schema_version: 1,
                    tablet_count: 1,
                }
                .to_proto(),
            ),
            default_values: vec![snapshot_proto::DefaultValueEntry {
                key: key.clone(),
                start_timestamp: Some(Timestamp(80).to_proto()),
                row: row.clone(),
            }],
            locks: Vec::new(),
            writes: vec![snapshot_proto::WriteEntry {
                key: key.clone(),
                write_timestamp: Some(Timestamp(90).to_proto()),
                record: Some(write.to_proto()),
            }],
        }],
    };

    (snapshot, key, row)
}

fn candidate(
    snapshot: &snapshot_proto::DatabaseSnapshot,
    published: &PublishedSnapshotFile,
) -> RecoveryCheckpointCandidate {
    RecoveryCheckpointCandidate {
        pointer_lsn: Lsn::new(8192),
        marker_lsn: Lsn::new(8256),
        pointer: SnapshotPointer {
            snapshot_id: snapshot.snapshot_id,
            snapshot_timestamp: Timestamp::from_proto(
                snapshot
                    .snapshot_timestamp
                    .expect("test snapshot timestamp must exist"),
            ),
            replay_from_lsn: Lsn::new(snapshot.replay_from_lsn),
            relative_path: published.relative_path().to_string(),
            table_ids: BTreeSet::from([TableId(7)]),
            file_length: published.file_length(),
            file_checksum_crc32c: published.file_checksum_crc32c(),
            snapshot_format_version: published.snapshot_format_version(),
        },
    }
}

/// Realistic bug caught:
///
/// a checksum-valid snapshot could be decoded but never installed into private
/// catalog and MVCC recovery state. Startup would then replay only the WAL
/// suffix over an empty database, permanently losing every row represented
/// exclusively by the checkpoint
#[test]
fn selected_checkpoint_restores_catalog_mvcc_and_allocator_state() {
    let data_dir = tempfile::tempdir().expect("temporary data directory must be created");
    let (snapshot, key, expected_row) = test_snapshot();
    let published =
        publish_snapshot_file(data_dir.path(), &snapshot).expect("snapshot file must be durable");
    let checkpoint = candidate(&snapshot, &published);

    let loaded = load_recovery_checkpoint(data_dir.path(), &checkpoint)
        .expect("published checkpoint must restore into private state");

    assert_eq!(loaded.snapshot_id, snapshot.snapshot_id);
    assert_eq!(loaded.replay_from_lsn, Lsn::new(snapshot.replay_from_lsn));
    assert_eq!(
        loaded
            .state
            .catalog()
            .table_by_id(TableId(7))
            .expect("restored table must exist")
            .name,
        "users"
    );
    assert_eq!(
        loaded
            .state
            .table_storage(TableId(7))
            .expect("restored MVCC table must exist")
            .read(&key, Timestamp(100))
            .expect("restored row read must succeed"),
        Some(expected_row)
    );
    assert_eq!(
        loaded.state.high_water_marks().max_transaction_id,
        TxnId(81)
    );
    assert_eq!(loaded.state.high_water_marks().max_timestamp, Timestamp(90));
    assert_eq!(loaded.state.high_water_marks().max_table_id, TableId(7));
    assert_eq!(loaded.state.high_water_marks().max_snapshot_id, 11);
}

/// Realistic bug caught:
///
/// Recovery could trust the replay boundary from a matched WAL pair while the
/// referenced file belongs to a different snapshot identity. Skipping to that
/// boundary would combine unrelated state and WAL history
#[test]
fn selected_checkpoint_rejects_snapshot_identity_mismatch() {
    let data_dir = tempfile::tempdir().expect("temporary data directory must be created");
    let (snapshot, _, _) = test_snapshot();
    let published =
        publish_snapshot_file(data_dir.path(), &snapshot).expect("snapshot file must be durable");
    let mut checkpoint = candidate(&snapshot, &published);
    checkpoint.pointer.snapshot_id += 1;

    let error = load_recovery_checkpoint(data_dir.path(), &checkpoint).unwrap_err();

    assert!(matches!(
        error,
        Error::CorruptData(message)
            if message.contains("snapshot ID")
                && message.contains("12")
                && message.contains("11")
    ));
}

/// Realistic bug caught:
///
/// A snapshot may contain transaction or MVCC timestamps above its declared
/// allocator high-water marks. Accepting that image would let restart reuse a
/// durable identity or timestamp after the covered WAL prefix is pruned
#[test]
fn snapshot_rejects_allocator_high_water_marks_below_mvcc_state() {
    let (mut snapshot, _, _) = test_snapshot();
    snapshot.snapshot_timestamp = Some(Timestamp(50).to_proto());
    let high_water = snapshot
        .high_water_marks
        .as_mut()
        .expect("test high-water marks must exist");
    high_water.max_timestamp = Some(Timestamp(50).to_proto());

    let error = encode_snapshot_file(&snapshot).unwrap_err();

    assert!(matches!(
        error,
        Error::InvalidArgument(message)
            if message.contains("timestamp high-water mark 50")
                && message.contains("MVCC timestamp 90")
    ));
}
