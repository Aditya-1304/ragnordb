use std::collections::{BTreeMap, BTreeSet};

use ragnordb_common::{
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    codec::{Row, Value},
    command_codec::{CatalogCommand, CatalogOperation, CreateTableOperation},
    encoding::encode_row,
    ids::{ColumnId, TableId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    recovery::{DecodedRecoveryRecord, RecoveryHighWaterMarks, RecoveryPayload, RecoveryState},
    wal::{CatalogUpdate, CheckpointMarker, SingleNodeTxnCommit, SnapshotPointer, WalMutation},
};
use wal::lsn::Lsn;

fn catalog_update(table_id: TableId) -> CatalogUpdate {
    CatalogUpdate {
        table_id,
        update_timestamp: Timestamp(11),
        command: CatalogCommand {
            operation: CatalogOperation::CreateTable(CreateTableOperation {
                table_def: TableDefinition {
                    table_id: table_id.0,
                    name: "users".to_string(),
                    columns: vec![ColumnDefinition {
                        column_id: ColumnId(1),
                        name: "id".to_string(),
                        ty: DataType::Int,
                        nullable: false,
                    }],
                    primary_key_column_ids: vec![ColumnId(1)],
                    schema_version: 1,
                    tablet_count: 1,
                },
            }),
        },
    }
}

fn transaction_commit(table_id: TableId) -> SingleNodeTxnCommit {
    let row_key =
        make_row_key(table_id, &[Value::Int(1)]).expect("recovery test row key must be valid");

    let encoded_key = encode_row_key(&row_key).expect("recovery test row key must encode");

    let encoded_row = encode_row(&Row {
        values: vec![Value::Int(1)],
    })
    .expect("recovery test row must encode");

    SingleNodeTxnCommit {
        table_id,
        txn_id: TxnId(41),
        start_timestamp: Timestamp(70),
        commit_timestamp: Timestamp(90),
        writes: BTreeMap::from([(encoded_key, WalMutation::Put(encoded_row))]),
    }
}

fn snapshot_pointer() -> SnapshotPointer {
    SnapshotPointer {
        snapshot_id: 8,
        snapshot_timestamp: Timestamp(85),
        replay_from_lsn: Lsn::new(256),
        relative_path: "snapshots/snapshot-8.ragnor".to_string(),
        table_ids: BTreeSet::from([TableId(9), TableId(42)]),
    }
}

fn checkpoint_marker() -> CheckpointMarker {
    CheckpointMarker {
        snapshot_id: 8,
        snapshot_timestamp: Timestamp(85),
        replay_from_lsn: Lsn::new(256),
    }
}

/// Verifies that recovery retains the maximum value from every durable
/// allocator namespace.
///
/// Realistic bug caught:
///
/// A restart could reuse a transaction ID, timestamp, table ID, or snapshot ID
/// when replay successfully reconstructs user data but fails to retain allocator
/// high-water marks from all semantic record types
#[test]
fn replay_tracks_all_durable_allocator_high_water_marks() {
    let table_id = TableId(9);

    let records = [
        DecodedRecoveryRecord {
            lsn: Lsn::new(64),
            payload: RecoveryPayload::CatalogUpdate(catalog_update(table_id)),
        },
        DecodedRecoveryRecord {
            lsn: Lsn::new(144),
            payload: RecoveryPayload::SingleNodeTxnCommit(transaction_commit(table_id)),
        },
        DecodedRecoveryRecord {
            lsn: Lsn::new(256),
            payload: RecoveryPayload::SnapshotPointer(snapshot_pointer()),
        },
        DecodedRecoveryRecord {
            lsn: Lsn::new(320),
            payload: RecoveryPayload::CheckpointMarker(checkpoint_marker()),
        },
    ];

    let mut state = RecoveryState::new();

    for record in &records {
        state
            .apply_record(record)
            .expect("valid durable record must replay");
    }

    // the transaction commit contains the largest timestamp, while snapshot
    // metadata contains the largest table ID. Recovery must take maxima across
    // all relevant fields rather than simply copying the final record
    assert_eq!(
        state.high_water_marks(),
        RecoveryHighWaterMarks {
            max_transaction_id: TxnId(41),
            max_timestamp: Timestamp(90),
            max_table_id: TableId(42),
            max_snapshot_id: 8,
        }
    );

    // identical replay must not advance or otherwise change allocator floors
    for record in &records {
        state
            .apply_record(record)
            .expect("identical replay must remain idempotent");
    }

    assert_eq!(
        state.high_water_marks(),
        RecoveryHighWaterMarks {
            max_transaction_id: TxnId(41),
            max_timestamp: Timestamp(90),
            max_table_id: TableId(42),
            max_snapshot_id: 8,
        }
    );
}
