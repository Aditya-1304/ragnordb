use prost::Message;
use ragnordb_common::{
    Error,
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    command_codec::{CatalogCommand, CatalogOperation, CreateTableOperation},
    ids::{ColumnId, TableId, Timestamp},
    proto::{command as command_proto, wal as wal_proto},
};
use ragnordb_storage::wal::{CATALOG_UPDATE_VERSION, CatalogUpdate};

fn table_definition(table_id: u64, name: &str) -> TableDefinition {
    TableDefinition {
        table_id,
        name: name.to_string(),
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
                nullable: true,
            },
        ],
        primary_key_column_ids: vec![ColumnId(1)],
        schema_version: 1,
        tablet_count: 1,
    }
}

fn catalog_command(table_id: u64, name: &str) -> CatalogCommand {
    CatalogCommand {
        operation: CatalogOperation::CreateTable(CreateTableOperation {
            table_def: table_definition(table_id, name),
        }),
    }
}

fn valid_update() -> CatalogUpdate {
    CatalogUpdate {
        table_id: TableId(9),
        update_timestamp: Timestamp(5),
        command: catalog_command(9, "users"),
    }
}

fn valid_proto_update() -> wal_proto::CatalogUpdate {
    let encoded = valid_update().encode().unwrap();

    wal_proto::CatalogUpdate::decode(encoded.as_slice()).unwrap()
}

fn create_table_operation_mut(update: &mut CatalogUpdate) -> &mut CreateTableOperation {
    match &mut update.command.operation {
        CatalogOperation::CreateTable(operation) => operation,
    }
}

fn proto_create_table_operation_mut(
    update: &mut wal_proto::CatalogUpdate,
) -> &mut command_proto::CreateTableOperation {
    let command = update
        .command
        .as_mut()
        .expect("valid fixture must contain a catalog command");

    match command
        .operation
        .as_mut()
        .expect("valid fixture must contain a catalog operation")
    {
        command_proto::catalog_command::Operation::CreateTable(operation) => operation,
    }
}

fn assert_encode_rejected(mutate: impl FnOnce(&mut CatalogUpdate), expected_message: &str) {
    let mut update = valid_update();
    mutate(&mut update);

    let error = update.encode().unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(_)));
    assert!(
        error.to_string().contains(expected_message),
        "unexpected encoding error: {error}"
    );
}

fn assert_decode_rejected(
    mutate: impl FnOnce(&mut wal_proto::CatalogUpdate),
    expected_message: &str,
) {
    let mut proto = valid_proto_update();
    mutate(&mut proto);

    let error = CatalogUpdate::decode(&proto.encode_to_vec()).unwrap_err();

    assert!(matches!(error, Error::CorruptData(_)));
    assert!(
        error.to_string().contains(expected_message),
        "unexpected decoding error: {error}"
    );
}

/// Verifies that catalog metadata and its operation survive durable encoding.
///
/// Realistic bug caught:
///
/// Recovery could install a catalog definition under a different table ID or
/// timestamp, or lose the catalog operation entirely.
#[test]
fn valid_catalog_update_round_trips() {
    let original = valid_update();

    let encoded = original.encode().unwrap();
    let decoded = CatalogUpdate::decode(&encoded).unwrap();

    assert_eq!(decoded, original);
}

/// Verifies that bytes written by the V1 catalog schema remain recoverable.
///
/// Realistic bug caught:
///
/// New encoder and decoder code could agree after protobuf field-number changes
/// while existing catalog WAL records become unreadable.
#[test]
fn v1_catalog_update_golden_bytes_remain_decodable() {
    const V1_GOLDEN_BYTES: &[u8] = &[
        // CatalogUpdate.version = 1.
        0x08, 0x01, // CatalogUpdate.update_timestamp = Timestamp { id: 5 }.
        0x12, 0x02, 0x08, 0x05, // CatalogUpdate.table_id = TableId { id: 1 }.
        0x1a, 0x02, 0x08, 0x01, // CatalogUpdate.command.
        0x22, 0x1a, // CatalogCommand.create_table.
        0x0a, 0x18, // CreateTableOperation.table_definition.
        0x0a, 0x16, // TableDefinition.table_id = 1.
        0x08, 0x01, // TableDefinition.name = "t".
        0x12, 0x01, b't', // One nonnullable INT column: id=1, name="id".
        0x1a, 0x08, 0x08, 0x01, 0x12, 0x02, b'i', b'd', 0x18, 0x01,
        // Primary-key column IDs = [1].
        0x22, 0x01, 0x01, // Schema version = 1.
        0x28, 0x01, // Tablet count = 1.
        0x30, 0x01,
    ];

    let expected = CatalogUpdate {
        table_id: TableId(1),
        update_timestamp: Timestamp(5),
        command: CatalogCommand {
            operation: CatalogOperation::CreateTable(CreateTableOperation {
                table_def: TableDefinition {
                    table_id: 1,
                    name: "t".to_string(),
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
    };

    let decoded = CatalogUpdate::decode(V1_GOLDEN_BYTES).unwrap();

    assert_eq!(decoded, expected);
}

/// Ensures recovery never interprets unknown catalog payload versions.
///
/// Realistic bug caught:
///
/// An older binary could apply catalog fields using obsolete semantics.
#[test]
fn zero_and_unsupported_catalog_versions_are_rejected() {
    assert_decode_rejected(
        |proto| proto.version = 0,
        "unsupported CatalogUpdate version 0",
    );

    assert_decode_rejected(
        |proto| proto.version = CATALOG_UPDATE_VERSION + 1,
        "unsupported CatalogUpdate version 2",
    );
}

/// Ensures wrapper metadata and operation ownership remain consistent.
///
/// Realistic bug caught:
///
/// Recovery could install an operation for one table under another table's WAL
/// identity or accept reserved table and timestamp values.
#[test]
fn invalid_catalog_metadata_is_rejected_on_encode_and_decode() {
    assert_encode_rejected(
        |update| update.table_id = TableId(0),
        "table ID 0 is reserved",
    );

    assert_encode_rejected(
        |update| update.update_timestamp = Timestamp(0),
        "update timestamp 0 is reserved",
    );

    assert_encode_rejected(
        |update| {
            create_table_operation_mut(update).table_def.table_id = 0;
        },
        "catalog operation table ID 0 is reserved",
    );

    assert_encode_rejected(
        |update| {
            create_table_operation_mut(update).table_def.table_id = 10;
        },
        "does not match update table",
    );

    assert_decode_rejected(
        |proto| proto.table_id = Some(TableId(0).to_proto()),
        "table ID 0 is reserved",
    );

    assert_decode_rejected(
        |proto| {
            proto.update_timestamp = Some(Timestamp(0).to_proto());
        },
        "update timestamp 0 is reserved",
    );

    assert_decode_rejected(
        |proto| {
            proto_create_table_operation_mut(proto)
                .table_definition
                .as_mut()
                .unwrap()
                .table_id = 0;
        },
        "catalog operation table ID 0 is reserved",
    );

    assert_decode_rejected(
        |proto| {
            proto_create_table_operation_mut(proto)
                .table_definition
                .as_mut()
                .unwrap()
                .table_id = 10;
        },
        "does not match update table",
    );
}

/// Ensures incomplete catalog envelopes cannot reach replay.
///
/// Realistic bug caught:
///
/// Recovery could receive a record whose table, timestamp, command, operation,
/// or table definition is absent and then guess its intended meaning.
#[test]
fn missing_catalog_update_fields_are_rejected() {
    assert_decode_rejected(
        |proto| proto.update_timestamp = None,
        "missing its update timestamp",
    );

    assert_decode_rejected(
        |proto| proto.table_id = None,
        "missing its table identifier",
    );

    assert_decode_rejected(|proto| proto.command = None, "missing its catalog command");

    assert_decode_rejected(
        |proto| {
            proto.command.as_mut().unwrap().operation = None;
        },
        "missing catalog operation",
    );

    assert_decode_rejected(
        |proto| {
            proto_create_table_operation_mut(proto).table_definition = None;
        },
        "missing table_definition",
    );
}

/// Ensures catalog-schema validation cannot be bypassed through WAL encoding.
///
/// Realistic bug caught:
///
/// A structurally valid protobuf could contain an invalid schema and become
/// durable even though the live catalog would refuse to install it.
#[test]
fn invalid_table_schema_is_rejected_on_encode_and_decode() {
    assert_encode_rejected(
        |update| {
            create_table_operation_mut(update).table_def.name.clear();
        },
        "table name cannot be empty",
    );

    assert_decode_rejected(
        |proto| {
            proto_create_table_operation_mut(proto)
                .table_definition
                .as_mut()
                .unwrap()
                .name
                .clear();
        },
        "table name cannot be empty",
    );
}
