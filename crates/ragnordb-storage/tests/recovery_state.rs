use std::collections::BTreeMap;

use ragnordb_catalog::Catalog;
use ragnordb_common::{
    Error,
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    codec::{Row, Value},
    command_codec::{CatalogCommand, CatalogOperation, CreateTableOperation},
    encoding::encode_row,
    ids::{ColumnId, TableId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    mvcc::MvccStorage,
    recovery::{DecodedRecoveryRecord, RecoveryPayload, RecoveryState},
    wal::{CatalogUpdate, SingleNodeTxnCommit, WalMutation},
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
                },
            }),
        },
    }
}

fn encoded_key(table_id: TableId, id: i64) -> Vec<u8> {
    let row_key =
        make_row_key(table_id, &[Value::Int(id)]).expect("recovery test row key must be valid");

    encode_row_key(&row_key).expect("recovery test row key must encode")
}

fn encoded_row(id: i64, name: &str) -> Vec<u8> {
    encode_row(&Row {
        values: vec![Value::Int(id), Value::Text(name.to_string())],
    })
    .expect("recovery test row must encode")
}

fn transaction_commit(table_id: TableId, key: Vec<u8>, row: Vec<u8>) -> SingleNodeTxnCommit {
    SingleNodeTxnCommit {
        table_id,
        txn_id: TxnId(7),
        start_timestamp: Timestamp(12),
        commit_timestamp: Timestamp(13),
        schema_version: 1,
        writes: BTreeMap::from([(key, WalMutation::Put(row))]),
    }
}

/// verifies that typed durable history reconstructs catalog and MVCC state
/// without duplicating versions when replayed again
///
/// Realistic bug caught:
///
/// Recovery could install the catalog but lose the transaction mutation,
/// translate Put into Delete, place the row under the wrong table, or create
/// duplicate MVCC versions when the same durable history is replayed again
#[test]
fn catalog_then_commit_rebuilds_mvcc_state_idempotently() {
    let table_id = TableId(9);
    let key = encoded_key(table_id, 1);
    let row = encoded_row(1, "Ada");

    let catalog_record = DecodedRecoveryRecord {
        lsn: Lsn::new(64),
        payload: RecoveryPayload::CatalogUpdate(catalog_update(table_id)),
    };

    let commit_record = DecodedRecoveryRecord {
        lsn: Lsn::new(144),
        payload: RecoveryPayload::SingleNodeTxnCommit(transaction_commit(
            table_id,
            key.clone(),
            row.clone(),
        )),
    };

    let mut state = RecoveryState::new();

    state
        .apply_record(&catalog_record)
        .expect("catalog update must replay");

    state
        .apply_record(&commit_record)
        .expect("transaction commit must replay");

    // replaying the same durable prefix must not duplicate catalog or MVCC
    // state. This is required when startup is retried after a failed attempt
    state
        .apply_record(&catalog_record)
        .expect("identical catalog replay must be idempotent");

    state
        .apply_record(&commit_record)
        .expect("identical transaction replay must be idempotent");

    let schema = state
        .catalog()
        .table_by_id(table_id)
        .expect("replayed table must exist");

    assert_eq!(schema.name, "users");

    let storage = state
        .table_storage(table_id)
        .expect("replayed table must have MVCC state");

    assert_eq!(
        storage
            .read(&key, Timestamp(13))
            .expect("recovered row read must succeed"),
        Some(row)
    );

    let stats = storage.stats();

    assert_eq!(stats.default_versions, 1);
    assert_eq!(stats.write_records, 1);
    assert_eq!(stats.locks, 0);
    assert_eq!(state.catalog().list_tables().len(), 1);
}

/// verifies that transaction history cannot create state for an unknown table
///
/// Realistic bug caught:
///
/// Applying a transaction before its catalog definition would create row
/// history whose schema and primary-key encoding cannot be interpreted safely
/// after startup
#[test]
fn transaction_before_catalog_is_rejected_without_orphan_state() {
    let table_id = TableId(9);
    let key = encoded_key(table_id, 1);
    let row = encoded_row(1, "Ada");

    let commit_record = DecodedRecoveryRecord {
        lsn: Lsn::new(64),
        payload: RecoveryPayload::SingleNodeTxnCommit(transaction_commit(table_id, key, row)),
    };

    let mut state = RecoveryState::new();

    let error = state.apply_record(&commit_record).unwrap_err();

    assert!(matches!(
        error,
        Error::CorruptData(message)
            if message.contains("WAL LSN 64")
                && message.contains("table 9")
                && message.contains("before its catalog definition")
    ));

    assert!(state.catalog().list_tables().is_empty());
    assert!(state.table_storage(table_id).is_none());
}

fn schema_replay_error(commit: SingleNodeTxnCommit) -> Error {
    let table_id = commit.table_id;
    let mut state = RecoveryState::new();

    state
        .apply_record(&DecodedRecoveryRecord {
            lsn: Lsn::new(64),
            payload: RecoveryPayload::CatalogUpdate(catalog_update(table_id)),
        })
        .expect("catalog schema must replay before the transaction");

    state
        .apply_record(&DecodedRecoveryRecord {
            lsn: Lsn::new(128),
            payload: RecoveryPayload::SingleNodeTxnCommit(commit),
        })
        .unwrap_err()
}

/// Verifies that canonical key and row bytes must also satisfy the recovered
/// catalog schema before they can enter private MVCC state.
///
/// Realistic bug caught:
///
/// Earlier software could durably write a canonically encoded row with the
/// wrong arity, types, nullability, primary key, or schema revision. Codec-only
/// validation would accept those bytes and publish logically impossible data
/// after restart.
#[test]
fn replay_rejects_canonical_mutations_that_violate_catalog_schema() {
    let table_id = TableId(9);
    let valid_key = encoded_key(table_id, 1);

    let mut wrong_schema = transaction_commit(table_id, valid_key.clone(), encoded_row(1, "Ada"));
    wrong_schema.schema_version = 2;
    assert!(
        schema_replay_error(wrong_schema)
            .to_string()
            .contains("schema version 2")
    );

    let wrong_key_arity = transaction_commit(
        table_id,
        encode_row_key(&make_row_key(table_id, &[Value::Int(1), Value::Int(2)]).unwrap()).unwrap(),
        encoded_row(1, "Ada"),
    );
    assert!(
        schema_replay_error(wrong_key_arity)
            .to_string()
            .contains("primary key has 2 values")
    );

    let wrong_key_type = transaction_commit(
        table_id,
        encode_row_key(&make_row_key(table_id, &[Value::Text("one".to_string())]).unwrap())
            .unwrap(),
        encoded_row(1, "Ada"),
    );
    assert!(
        schema_replay_error(wrong_key_type)
            .to_string()
            .contains("primary-key column id expects Int")
    );

    let wrong_row_arity = transaction_commit(
        table_id,
        valid_key.clone(),
        encode_row(&Row {
            values: vec![Value::Int(1)],
        })
        .unwrap(),
    );
    assert!(
        schema_replay_error(wrong_row_arity)
            .to_string()
            .contains("row has 1 values")
    );

    let wrong_row_type = transaction_commit(
        table_id,
        valid_key.clone(),
        encode_row(&Row {
            values: vec![Value::Int(1), Value::Bool(true)],
        })
        .unwrap(),
    );
    assert!(
        schema_replay_error(wrong_row_type)
            .to_string()
            .contains("column name expects Text")
    );

    let null_in_required_column = transaction_commit(
        table_id,
        valid_key.clone(),
        encode_row(&Row {
            values: vec![Value::Int(1), Value::Null],
        })
        .unwrap(),
    );
    assert!(
        schema_replay_error(null_in_required_column)
            .to_string()
            .contains("column name expects Text")
    );

    let mismatched_physical_key = transaction_commit(table_id, valid_key, encoded_row(2, "Ada"));
    assert!(
        schema_replay_error(mismatched_physical_key)
            .to_string()
            .contains("physical row key does not match")
    );
}

/// Verifies that one durable transaction identity names exactly one logical
/// commit throughout semantic replay.
///
/// Realistic bug caught:
///
/// Two individually valid WAL records could reuse a transaction ID for
/// disjoint keys. MVCC idempotence checks operate per key and would otherwise
/// apply both records as if they were one larger transaction.
#[test]
fn replay_rejects_conflicting_duplicate_transaction_identity() {
    let table_id = TableId(9);
    let first_key = encoded_key(table_id, 1);
    let second_key = encoded_key(table_id, 2);
    let first = transaction_commit(table_id, first_key.clone(), encoded_row(1, "Ada"));
    let conflicting = transaction_commit(table_id, second_key.clone(), encoded_row(2, "Grace"));
    let mut state = RecoveryState::new();

    state
        .apply_record(&DecodedRecoveryRecord {
            lsn: Lsn::new(64),
            payload: RecoveryPayload::CatalogUpdate(catalog_update(table_id)),
        })
        .unwrap();
    state
        .apply_record(&DecodedRecoveryRecord {
            lsn: Lsn::new(128),
            payload: RecoveryPayload::SingleNodeTxnCommit(first),
        })
        .unwrap();

    let error = state
        .apply_record(&DecodedRecoveryRecord {
            lsn: Lsn::new(192),
            payload: RecoveryPayload::SingleNodeTxnCommit(conflicting),
        })
        .unwrap_err();

    assert!(matches!(
        error,
        Error::CorruptData(message)
            if message.contains("transaction ID 7")
                && message.contains("different logical commit")
    ));
    assert_eq!(
        state
            .table_storage(table_id)
            .unwrap()
            .read(&second_key, Timestamp(13))
            .unwrap(),
        None
    );
}

/// Verifies that the serialized single-node history cannot assign one commit
/// timestamp to two different transactions.
///
/// Realistic bug caught:
///
/// Reused visibility timestamps make MVCC ordering ambiguous even when the
/// transactions touch different keys and their individual records are valid.
#[test]
fn replay_rejects_commit_timestamp_reuse_by_another_transaction() {
    let table_id = TableId(9);
    let first = transaction_commit(table_id, encoded_key(table_id, 1), encoded_row(1, "Ada"));
    let mut second =
        transaction_commit(table_id, encoded_key(table_id, 2), encoded_row(2, "Grace"));
    second.txn_id = TxnId(8);
    second.start_timestamp = Timestamp(11);
    let mut state = RecoveryState::new();

    for record in [
        DecodedRecoveryRecord {
            lsn: Lsn::new(64),
            payload: RecoveryPayload::CatalogUpdate(catalog_update(table_id)),
        },
        DecodedRecoveryRecord {
            lsn: Lsn::new(128),
            payload: RecoveryPayload::SingleNodeTxnCommit(first),
        },
    ] {
        state.apply_record(&record).unwrap();
    }

    let error = state
        .apply_record(&DecodedRecoveryRecord {
            lsn: Lsn::new(192),
            payload: RecoveryPayload::SingleNodeTxnCommit(second),
        })
        .unwrap_err();

    assert!(matches!(
        error,
        Error::CorruptData(message)
            if message.contains("reuses commit timestamp 13")
                && message.contains("transaction 7")
                && message.contains("transaction 8")
    ));
}
