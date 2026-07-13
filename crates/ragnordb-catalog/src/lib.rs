//! Catalog schema snapshots and the local in-memory catalog.
//!
//! The SQL binder reads immutable `Arc<TableSchema>` snapshots. This ownership
//! model works for the current local catalog and for a future shared metadata
//! cache without introducing long-lived catalog borrows.

use ragnordb_common::catalog_codec::DataType;
use ragnordb_common::ids::{ColumnId, TableId};
use ragnordb_common::{Error, Result};

use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// this describes a single column in a table schema (in-memory representation)
///
/// Column IDs are unique and stable within a table. They are assigned by the
/// analyzer during `CREATE TABLE` processing and must never be reused after a
/// column is removed in future schema-change implementations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    /// Stable schema identity. This is not the row ordinal
    pub id: ColumnId,

    /// sql identifier
    pub name: String,

    /// logical SQL data type
    pub ty: DataType,

    /// stored values may be NULL
    pub nullable: bool,
}

/// this is the full table schema held in memory by the catalog
///
/// primary_key_column_ids: ordered list of column IDs forming the PK
///   Used by the SQL analyzer to enforce PK-in-INSERT rules and
///   by the tablet layer to construct internal keys
/// schema_version: bumped on every schema change (DDL)
/// tablet_count: number of hash partitions for this table
/// `primary_key_column_ids` preserves primary-key column order because that
/// order defines deterministic composite-key encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key_column_ids: Vec<ColumnId>,
    pub schema_version: u64,
    pub tablet_count: u32,
}

impl TableSchema {
    /// Find a column by its canonical SQL identifier.
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.name == name)
    }

    /// Find a column by its stable catalog ID.
    pub fn column_by_id(&self, id: ColumnId) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.id == id)
    }

    pub fn column_ordinal(&self, id: ColumnId) -> Option<usize> {
        self.columns.iter().position(|column| column.id == id)
    }

    /// Resolve the ordered primary-key column list.
    ///
    /// Returning an error instead of silently dropping missing IDs prevents a
    /// corrupted schema from producing a different internal key layout.
    pub fn primary_key_columns(&self) -> Result<Vec<&ColumnSchema>> {
        self.primary_key_column_ids
            .iter()
            .map(|id| {
                self.column_by_id(*id).ok_or_else(|| {
                    Error::SchemaMismatch(format!(
                        "table {} references missing primary-key column id {}",
                        self.name, id.0
                    ))
                })
            })
            .collect()
    }
}

/// Read-only catalog interface used during SQL analysis.
///
/// Returned `Arc<TableSchema>` values remain valid if the catalog publishes a
/// newer schema snapshot concurrently in a future metadata-cache design.
pub trait Catalog {
    fn table_by_name(&self, name: &str) -> Option<Arc<TableSchema>>;

    fn table_by_id(&self, id: TableId) -> Option<Arc<TableSchema>>;

    /// Return tables in deterministic table-ID order
    fn list_tables(&self) -> Vec<Arc<TableSchema>>;
}

/// In-memory catalog used by single-node mode and analyzer tests.
///
/// Table IDs begin at one. Zero remains unused so default protobuf scalar values
/// cannot accidentally identify a valid table.
#[derive(Debug)]
pub struct MemoryCatalog {
    tables_ids_by_name: HashMap<String, TableId>,
    tables_by_id: BTreeMap<TableId, Arc<TableSchema>>,
    next_table_id: u64,
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCatalog {
    /// Create an empty catalog with the first valid table ID set to one.
    pub fn new() -> Self {
        Self {
            tables_ids_by_name: HashMap::new(),
            tables_by_id: BTreeMap::new(),
            next_table_id: 1,
        }
    }

    /// Allocate a table ID and publish a validated schema snapshot.
    ///
    /// CREATE TABLE analysis validates SQL semantics. The catalog remains the
    /// final authority for stable table identity, schema version, and local
    /// tablet count.
    pub fn add_table(
        &mut self,
        name: impl Into<String>,
        columns: Vec<ColumnSchema>,
        primary_key_column_ids: Vec<ColumnId>,
    ) -> Result<TableId> {
        let name = name.into();

        validate_new_table(&name, &columns, &primary_key_column_ids)?;

        if self.tables_ids_by_name.contains_key(&name) {
            return Err(Error::ConstraintViolation(format!(
                "table already exists: {name}"
            )));
        }

        let table_id = TableId(self.next_table_id);

        self.next_table_id = self.next_table_id.checked_add(1).ok_or_else(|| {
            Error::ConstraintViolation("table ID space has been exhausted".to_string())
        })?;

        let schema = Arc::new(TableSchema {
            id: table_id,
            name: name.clone(),
            columns,
            primary_key_column_ids,
            schema_version: 1,
            tablet_count: 1,
        });

        self.tables_ids_by_name.insert(name, table_id);
        self.tables_by_id.insert(table_id, schema);

        Ok(table_id)
    }
}

impl Catalog for MemoryCatalog {
    fn table_by_name(&self, name: &str) -> Option<Arc<TableSchema>> {
        let id = self.tables_ids_by_name.get(name)?;
        self.tables_by_id.get(id).cloned()
    }

    fn table_by_id(&self, id: TableId) -> Option<Arc<TableSchema>> {
        self.tables_by_id.get(&id).cloned()
    }

    fn list_tables(&self) -> Vec<Arc<TableSchema>> {
        self.tables_by_id.values().cloned().collect()
    }
}

fn validate_new_table(
    name: &str,
    columns: &[ColumnSchema],
    primary_key_column_ids: &[ColumnId],
) -> Result<()> {
    if name.trim().is_empty() {
        return Err(Error::ConstraintViolation(
            "table name cannot be empty".to_string(),
        ));
    }

    if columns.is_empty() {
        return Err(Error::ConstraintViolation(format!(
            "table {name} must define at least one column"
        )));
    }

    if primary_key_column_ids.is_empty() {
        return Err(Error::ConstraintViolation(format!(
            "table {name} must define a primary key"
        )));
    }

    let mut names = HashSet::new();
    let mut column_ids = HashSet::new();

    for column in columns {
        if column.id.0 == 0 {
            return Err(Error::ConstraintViolation(format!(
                "column {} uses reserved column ID 0",
                column.name
            )));
        }

        if column.name.trim().is_empty() {
            return Err(Error::ConstraintViolation(format!(
                "table {name} contains an empty column name"
            )));
        }

        if !names.insert(column.name.as_str()) {
            return Err(Error::ConstraintViolation(format!(
                "duplicate column name: {}",
                column.name
            )));
        }

        if !column_ids.insert(column.id) {
            return Err(Error::ConstraintViolation(format!(
                "duplicate column ID: {}",
                column.id.0
            )));
        }
    }

    let mut primary_key_ids = HashSet::new();

    for id in primary_key_column_ids {
        if !primary_key_ids.insert(*id) {
            return Err(Error::ConstraintViolation(format!(
                "duplicate primary-key column ID: {}",
                id.0
            )));
        }

        let column = columns
            .iter()
            .find(|column| column.id == *id)
            .ok_or_else(|| {
                Error::ConstraintViolation(format!(
                    "primary-key column ID {} does not exist in table {name}",
                    id.0
                ))
            })?;

        if column.nullable {
            return Err(Error::ConstraintViolation(format!(
                "primary-key column {} cannot be nullable",
                column.name
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema {
                id: ColumnId(1),
                name: "id".into(),
                ty: DataType::Int,
                nullable: false,
            },
            ColumnSchema {
                id: ColumnId(2),
                name: "name".into(),
                ty: DataType::Text,
                nullable: true,
            },
        ]
    }

    #[test]
    fn new_and_default_allocate_id_one() {
        for mut catalog in [MemoryCatalog::new(), MemoryCatalog::default()] {
            let id = catalog
                .add_table("users", columns(), vec![ColumnId(1)])
                .unwrap();

            assert_eq!(id, TableId(1));
        }
    }

    #[test]
    fn lookup_by_name_and_id_returns_same_snapshot() {
        let mut catalog = MemoryCatalog::new();

        let id = catalog
            .add_table("users", columns(), vec![ColumnId(1)])
            .unwrap();

        let by_name = catalog.table_by_name("users").unwrap();
        let by_id = catalog.table_by_id(id).unwrap();

        assert!(Arc::ptr_eq(&by_name, &by_id));
        assert_eq!(by_name.schema_version, 1);
        assert_eq!(by_name.tablet_count, 1);
    }

    #[test]
    fn list_tables_is_ordered_by_table_id() {
        let mut catalog = MemoryCatalog::new();

        catalog
            .add_table("users", columns(), vec![ColumnId(1)])
            .unwrap();

        catalog
            .add_table("orders", columns(), vec![ColumnId(1)])
            .unwrap();

        let tables = catalog.list_tables();

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].id, TableId(1));
        assert_eq!(tables[1].id, TableId(2));
    }

    #[test]
    fn rejects_duplicate_column_ids() {
        let invalid = vec![
            ColumnSchema {
                id: ColumnId(1),
                name: "id".into(),
                ty: DataType::Int,
                nullable: false,
            },
            ColumnSchema {
                id: ColumnId(1),
                name: "name".into(),
                ty: DataType::Text,
                nullable: true,
            },
        ];

        let error = MemoryCatalog::new()
            .add_table("users", invalid, vec![ColumnId(1)])
            .unwrap_err();

        assert!(error.to_string().contains("duplicate column ID"));
    }

    #[test]
    fn rejects_zero_column_id() {
        let invalid = vec![ColumnSchema {
            id: ColumnId(0),
            name: "id".into(),
            ty: DataType::Int,
            nullable: false,
        }];

        let error = MemoryCatalog::new()
            .add_table("users", invalid, vec![ColumnId(0)])
            .unwrap_err();

        assert!(error.to_string().contains("reserved column ID 0"));
    }
}
