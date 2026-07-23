use ragnordb_common::{
    Error,
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    command_codec::{CatalogCommand, CatalogOperation, CreateTableOperation},
    ids::{ColumnId, TableId, Timestamp},
};
use ragnordb_storage::{
    recovery::{DecodedRecoveryRecord, RecoveryPayload, decode_recovery_record},
    wal::{CatalogUpdate, RagnorDbWalRecordType},
};
use wal::lsn::Lsn;

fn valid_catalog_update() -> CatalogUpdate {
    let table_id = TableId(9);

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

/// Verifies that the recovery dispatcher preserves both the physical WAL
/// position and the validated semantic catalog operation.
///
/// Realistic bug caught:
///
/// Recovery could classify a catalog record correctly but invoke the wrong
/// payload decoder, discard its LSN, or construct a different operation than
/// the one contained in durable history.
#[test]
fn catalog_update_is_decoded_into_a_typed_recovery_record() {
    let update = valid_catalog_update();
    let encoded = update.encode().expect("catalog fixture must encode");
    let lsn = Lsn::new(64);

    let decoded = decode_recovery_record(
        lsn,
        RagnorDbWalRecordType::CatalogUpdate.as_wal_record_type(),
        &encoded,
    )
    .expect("valid durable catalog data must decode")
    .expect("RagnorDB catalog records must not be skipped");

    assert_eq!(
        decoded,
        DecodedRecoveryRecord {
            lsn,
            payload: RecoveryPayload::CatalogUpdate(update),
        }
    );
}

/// Verifies that physical WAL validity is not treated as proof of database
/// payload validity.
///
/// Realistic bug caught:
///
/// A checksummed record with malformed protobuf bytes could otherwise be
/// skipped or reach catalog publication, allowing startup to accept a durable
/// history that cannot be replayed deterministically.
#[test]
fn malformed_ragnordb_payload_stops_recovery_during_decoding() {
    let lsn = Lsn::new(144);

    let error = decode_recovery_record(
        lsn,
        RagnorDbWalRecordType::CatalogUpdate.as_wal_record_type(),
        &[0xff],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        Error::CorruptData(message)
            if message.contains("CatalogUpdate")
                && message.contains("WAL LSN 144")
    ));
}
