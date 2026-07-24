use ragnordb_common::{
    Error,
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    codec::{Row, Value, WriteKind, WriteRecord},
    encoding::encode_row,
    ids::{ColumnId, TableId, Timestamp, TxnId},
    proto::snapshot as snapshot_proto,
};
use ragnordb_storage::{
    checkpoint::{decode_snapshot_file, encode_snapshot_file},
    key::{encode_row_key, make_row_key},
};

fn test_snapshot() -> snapshot_proto::DatabaseSnapshot {
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

    snapshot_proto::DatabaseSnapshot {
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
                row,
            }],
            locks: Vec::new(),
            writes: vec![snapshot_proto::WriteEntry {
                key,
                write_timestamp: Some(Timestamp(90).to_proto()),
                record: Some(write.to_proto()),
            }],
        }],
    }
}

/// Realistic bug caught:
///
/// a snapshot writer could omit catalog, MVCC history, replay-frontier, or
/// allocator metadata while still producing a syntactically valid file.
/// current WAL replay tests cannot catch this because they never round-trip a
/// standalone snapshot image.
#[test]
fn snapshot_file_round_trips_complete_recovery_state() {
    let expected = test_snapshot();

    let encoded = encode_snapshot_file(&expected).unwrap();
    let decoded = decode_snapshot_file(&encoded).unwrap();

    assert_eq!(decoded, expected);
}

/// Realistic bug caught:
///
/// a bit flip in an otherwise readable snapshot protobuf could alter durable
/// rows or allocator maxima. Without an envelope checksum, recovery might trust
/// that damaged image and then prune the WAL needed to rebuild it.
#[test]
fn snapshot_file_rejects_checksum_mismatch_before_restore() {
    let mut encoded = encode_snapshot_file(&test_snapshot()).unwrap();
    let last = encoded
        .last_mut()
        .expect("encoded snapshot must contain a protobuf body");
    *last ^= 0x01;

    let error = decode_snapshot_file(&encoded).unwrap_err();

    assert!(
        matches!(error, Error::CorruptData(ref message) if message.contains("checksum mismatch")),
        "unexpected error: {error}"
    );
}
