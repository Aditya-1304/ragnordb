use ragnordb_common::catalog_codec::DataType;
use ragnordb_common::ids::TableId;
use ragnordb_common::{Error, Result};

use std::collections::HashMap;
use std::collections::HashSet;

/// this describes a single column in a table schema (in-memory representation)
///
/// Column IDs are unique and stable within a table. They are assigned by the
/// analyzer during `CREATE TABLE` processing and must never be reused after a
/// column is removed in future schema-change implementations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub id: u64,
    pub name: String,
    pub ty: DataType,
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
    pub primary_key_column_ids: Vec<u64>,
    pub schema_version: u64,
    pub tablet_count: u32,
}

impl TableSchema {
    /// Find a column by its canonical SQL identifier.
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.name == name)
    }

    /// Find a column by its stable catalog ID.
    pub fn column_by_id(&self, id: u64) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.id == id)
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
                        "table {} references missing primary key column id {id}",
                        self.name
                    ))
                })
            })
            .collect()
    }
}

/// Read-only catalog interface used during SQL analysis.
///
/// The analyzer depends on this interface instead of `MemoryCatalog` directly,
/// allowing a metadata-backed schema cache to replace the local implementation
/// in later milestones.
pub trait Catalog {
    fn table_by_name(&self, name: &str) -> Option<&TableSchema>;
}

/// In-memory catalog used by single-node mode and analyzer tests.
///
/// Table IDs begin at one. Zero remains unused so default protobuf scalar values
/// cannot accidentally identify a valid table.
#[derive(Debug)]
pub struct MemoryCatalog {
    tables_by_name: HashMap<String, TableSchema>,
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
            tables_by_name: HashMap::new(),
            next_table_id: 1,
        }
    }

    /// Register a validated single-tablet schema.
    ///
    /// This method is the invariant boundary for schemas entering the local
    /// catalog. Callers cannot register duplicate IDs, nullable primary keys,
    /// missing primary-key columns, or otherwise ambiguous definitions.
    pub fn add_table(
        &mut self,
        name: impl Into<String>,
        columns: Vec<ColumnSchema>,
        primary_key_column_ids: Vec<u64>,
    ) -> Result<TableId> {
        let name = name.into();

        if name.trim().is_empty() {
            return Err(Error::ConstraintViolation(
                "table name cannot be empty".to_string(),
            ));
        }

        if self.tables_by_name.contains_key(&name) {
            return Err(Error::ConstraintViolation(format!(
                "table already exists: {name}"
            )));
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

        let mut seen_names = HashSet::new();
        let mut seen_ids = HashSet::new();

        for column in &columns {
            if column.id == 0 {
                return Err(Error::ConstraintViolation(format!(
                    "column {} on table {name} uses reserved column id 0",
                    column.name
                )));
            }

            if column.name.trim().is_empty() {
                return Err(Error::ConstraintViolation(format!(
                    "table {name} contains an empty column name"
                )));
            }

            if !seen_names.insert(column.name.as_str()) {
                return Err(Error::ConstraintViolation(format!(
                    "duplicate column name: {}",
                    column.name
                )));
            }

            if !seen_ids.insert(column.id) {
                return Err(Error::ConstraintViolation(format!(
                    "duplicate column id: {}",
                    column.id
                )));
            }
        }

        let mut seen_primary_key_ids = HashSet::new();

        for primary_key_id in &primary_key_column_ids {
            if !seen_primary_key_ids.insert(*primary_key_id) {
                return Err(Error::ConstraintViolation(format!(
                    "duplicate primary key column id: {primary_key_id}"
                )));
            }

            let column = columns
                .iter()
                .find(|column| column.id == *primary_key_id)
                .ok_or_else(|| {
                    Error::ConstraintViolation(format!(
                        "primary key column id {primary_key_id} does not exist in table {name}"
                    ))
                })?;

            if column.nullable {
                return Err(Error::ConstraintViolation(format!(
                    "primary key column {} on table {name} cannot be nullable",
                    column.name
                )));
            }
        }

        let table_id = TableId(self.next_table_id);

        self.next_table_id = self.next_table_id.checked_add(1).ok_or_else(|| {
            Error::ConstraintViolation("table ID space has been exhausted".to_string())
        })?;

        let table = TableSchema {
            id: table_id,
            name: name.clone(),
            columns,
            primary_key_column_ids,
            schema_version: 1,
            tablet_count: 1,
        };

        self.tables_by_name.insert(name, table);

        Ok(table_id)
    }
}

impl Catalog for MemoryCatalog {
    fn table_by_name(&self, name: &str) -> Option<&TableSchema> {
        self.tables_by_name.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_columns() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema {
                id: 1,
                name: "id".into(),
                ty: DataType::Int,
                nullable: false,
            },
            ColumnSchema {
                id: 2,
                name: "name".into(),
                ty: DataType::Text,
                nullable: true,
            },
        ]
    }

    #[test]
    fn new_and_default_allocate_the_same_first_table_id() {
        let mut new_catalog = MemoryCatalog::new();
        let mut default_catalog = MemoryCatalog::default();

        let new_id = new_catalog
            .add_table("users", valid_columns(), vec![1])
            .unwrap();

        let default_id = default_catalog
            .add_table("users", valid_columns(), vec![1])
            .unwrap();

        assert_eq!(new_id, TableId(1));
        assert_eq!(default_id, TableId(1));
    }

    #[test]
    fn add_table_registers_a_valid_schema() {
        let mut catalog = MemoryCatalog::new();

        let id = catalog
            .add_table("users", valid_columns(), vec![1])
            .unwrap();

        assert_eq!(id, TableId(1));

        let table = catalog.table_by_name("users").unwrap();
        assert_eq!(table.schema_version, 1);
        assert_eq!(table.tablet_count, 1);
        assert_eq!(table.primary_key_columns().unwrap()[0].name, "id");
    }

    #[test]
    fn rejects_duplicate_table_name() {
        let mut catalog = MemoryCatalog::new();

        catalog
            .add_table("users", valid_columns(), vec![1])
            .unwrap();

        let error = catalog
            .add_table("users", valid_columns(), vec![1])
            .unwrap_err();

        assert!(error.to_string().contains("table already exists"));
    }

    #[test]
    fn rejects_duplicate_column_name() {
        let mut columns = valid_columns();
        columns[1].name = "id".to_string();

        let error = MemoryCatalog::new()
            .add_table("users", columns, vec![1])
            .unwrap_err();

        assert!(error.to_string().contains("duplicate column name"));
    }

    #[test]
    fn rejects_duplicate_column_id() {
        let mut columns = valid_columns();
        columns[1].id = 1;

        let error = MemoryCatalog::new()
            .add_table("users", columns, vec![1])
            .unwrap_err();

        assert!(error.to_string().contains("duplicate column id"));
    }

    #[test]
    fn rejects_duplicate_primary_key_id() {
        let error = MemoryCatalog::new()
            .add_table("users", valid_columns(), vec![1, 1])
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("duplicate primary key column id")
        );
    }

    #[test]
    fn rejects_missing_primary_key_column() {
        let error = MemoryCatalog::new()
            .add_table("users", valid_columns(), vec![99])
            .unwrap_err();

        assert!(error.to_string().contains("does not exist"));
    }

    #[test]
    fn rejects_nullable_primary_key() {
        let mut columns = valid_columns();
        columns[0].nullable = true;

        let error = MemoryCatalog::new()
            .add_table("users", columns, vec![1])
            .unwrap_err();

        assert!(error.to_string().contains("cannot be nullable"));
    }

    #[test]
    fn rejects_reserved_zero_column_id() {
        let mut columns = valid_columns();
        columns[0].id = 0;

        let error = MemoryCatalog::new()
            .add_table("users", columns, vec![0])
            .unwrap_err();

        assert!(error.to_string().contains("reserved column id 0"));
    }
}
