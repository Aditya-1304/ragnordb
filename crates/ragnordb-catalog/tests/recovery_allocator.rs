use ragnordb_catalog::{ColumnSchema, MemoryCatalog};
use ragnordb_common::{
    Error,
    catalog_codec::DataType,
    ids::{ColumnId, TableId},
};

fn columns() -> Vec<ColumnSchema> {
    vec![ColumnSchema {
        id: ColumnId(1),
        name: "id".to_string(),
        ty: DataType::Int,
        nullable: false,
    }]
}

/// Verifies that recovered table-ID history controls future catalog allocation.
///
/// Realistic bug caught:
///
/// If recovery derives the allocator only from currently visible tables, a
/// previously used but absent table ID could be assigned to a new table.
#[test]
fn recovered_table_floor_controls_next_catalog_allocation() {
    let mut catalog = MemoryCatalog::new();

    catalog
        .restore_table_id_floor(TableId(43))
        .expect("valid table-ID floor must be accepted");

    let allocated = catalog
        .add_table("events", columns(), vec![ColumnId(1)])
        .expect("table creation after recovery must succeed");

    assert_eq!(allocated, TableId(43));
}

/// Verifies that zero cannot become the next catalog table identity.
#[test]
fn zero_table_floor_is_rejected() {
    let mut catalog = MemoryCatalog::new();

    let error = catalog.restore_table_id_floor(TableId(0)).unwrap_err();

    assert!(matches!(
        error,
        Error::Configuration(message)
            if message.contains("table ID")
                && message.contains("nonzero")
    ));
}
