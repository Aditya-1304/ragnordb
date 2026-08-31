//! Catalog schema snapshots and the local in-memory catalog.
//!
//! The SQL analyzer reads immutable `Arc<TableSchema>` snapshots. This
//! ownership model allows analyzed statements to retain the exact schema
//! version against which they were bound without holding catalog borrows.
//!
//! for now table creation always produces a single-tablet schema
//! Fully assigned schemas may also be installed from durable definitions,
//! providing the bootstrap boundary later required by recovery and metadata
//! replication without implementing those later phases here.

mod durable;
mod metadata;

pub use durable::{
    CatalogCreateOutcome, CatalogLogExtent, CatalogLogRecord, DurableCatalog, DurableCatalogLog,
};
pub use metadata::{MetadataApplyOutcome, MetadataRejection, MetadataState, MetadataTableCreated};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use ragnordb_common::catalog_codec::{ColumnDefinition, DataType, TableDefinition};
use ragnordb_common::ids::{ColumnId, TableId};
use ragnordb_common::{Error, Result};

/// Immutable in-memory metadata for one table column.
///
/// A column identifier is stable within its table and is distinct from the
/// column's row-layout ordinal. Future schema versions may change ordinals,
/// but must never reuse a previously assigned column identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    /// Stable, nonzero identity assigned when the column is created.
    pub id: ColumnId,

    /// Canonical SQL identifier used for name resolution.
    pub name: String,

    /// Logical SQL type used by analysis and row encoding.
    pub ty: DataType,

    /// Whether stored values may contain SQL `NULL`.
    pub nullable: bool,
}

impl ColumnSchema {
    /// Convert the in-memory column metadata into its durable representation.
    fn to_definition(&self) -> ColumnDefinition {
        ColumnDefinition {
            column_id: self.id,
            name: self.name.clone(),
            ty: self.ty,
            nullable: self.nullable,
        }
    }

    /// Construct in-memory column metadata from a durable definition.
    fn from_definition(definition: ColumnDefinition) -> Self {
        Self {
            id: definition.column_id,
            name: definition.name,
            ty: definition.ty,
            nullable: definition.nullable,
        }
    }
}

/// Immutable schema snapshot for one SQL table.
///
/// `primary_key_column_ids` preserves primary-key column order because that
/// order defines deterministic composite-key encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    /// Stable table identifier within the catalog.
    ///
    /// In distributed mode, the metadata authority will allocate identifiers
    /// uniquely across the cluster
    pub id: TableId,

    /// Canonical SQL table name.
    pub name: String,

    /// Columns in deterministic row-layout order.
    pub columns: Vec<ColumnSchema>,

    /// Ordered list of column identifiers forming the primary key.
    pub primary_key_column_ids: Vec<ColumnId>,

    /// Monotonically increasing schema revision.
    pub schema_version: u64,

    /// Number of tablets assigned to the table.
    pub tablet_count: u32,
}

impl TableSchema {
    /// Find a column by its canonical SQL identifier.
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.name == name)
    }

    /// Find a column by its stable catalog identifier.
    pub fn column_by_id(&self, id: ColumnId) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.id == id)
    }

    /// Return the row-layout ordinal for a stable column identifier.
    pub fn column_ordinal(&self, id: ColumnId) -> Option<usize> {
        self.columns.iter().position(|column| column.id == id)
    }

    /// Resolve the primary-key columns in key-encoding order.
    ///
    /// Returning an error instead of silently dropping missing identifiers
    /// prevents corrupt catalog metadata from producing incompatible row keys.
    pub fn primary_key_columns(&self) -> Result<Vec<&ColumnSchema>> {
        self.primary_key_column_ids
            .iter()
            .map(|id| {
                self.column_by_id(*id).ok_or_else(|| {
                    Error::SchemaMismatch(format!(
                        "table {} references missing primary-key column ID {}",
                        self.name, id.0
                    ))
                })
            })
            .collect()
    }

    /// Convert this schema snapshot into its durable catalog representation.
    pub fn to_definition(&self) -> TableDefinition {
        TableDefinition {
            table_id: self.id.0,
            name: self.name.clone(),
            columns: self
                .columns
                .iter()
                .map(ColumnSchema::to_definition)
                .collect(),
            primary_key_column_ids: self.primary_key_column_ids.clone(),
            schema_version: self.schema_version,
            tablet_count: self.tablet_count,
        }
    }

    /// Decode and validate a durable catalog definition.
    ///
    /// This conversion is suitable for initial bootstrap, snapshot restoration,
    /// and idempotent metadata replay. It does not implement schema evolution.
    pub fn from_definition(definition: TableDefinition) -> Result<Self> {
        let TableDefinition {
            table_id,
            name,
            columns,
            primary_key_column_ids,
            schema_version,
            tablet_count,
        } = definition;

        let schema = Self {
            id: TableId(table_id),
            name,
            columns: columns
                .into_iter()
                .map(ColumnSchema::from_definition)
                .collect(),
            primary_key_column_ids,
            schema_version,
            tablet_count,
        };

        validate_table_schema(&schema)?;

        Ok(schema)
    }
}

/// Read-only catalog interface consumed by the SQL analyzer.
///
/// Returning immutable snapshots keeps this API compatible with a future local
/// cache populated by the metadata Raft group.
pub trait Catalog: Send + Sync {
    /// Look up the current schema snapshot by canonical table name.
    fn table_by_name(&self, name: &str) -> Option<Arc<TableSchema>>;

    /// Look up the current schema snapshot by stable table identifier.
    fn table_by_id(&self, id: TableId) -> Option<Arc<TableSchema>>;

    /// Return all tables in deterministic table-ID order.
    fn list_tables(&self) -> Vec<Arc<TableSchema>>;
}

/// In-memory catalog used by single-node mode and SQL-layer tests.
///
/// Table identifiers start at one. Zero remains reserved so a default protobuf
/// scalar cannot accidentally refer to a valid table.
#[derive(Debug)]
pub struct MemoryCatalog {
    table_ids_by_name: HashMap<String, TableId>,
    tables_by_id: BTreeMap<TableId, Arc<TableSchema>>,

    /// Next locally allocatable table identifier.
    ///
    /// `None` means the monotonically increasing identifier space has been
    /// exhausted.
    next_table_id: Option<u64>,
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCatalog {
    /// Create an empty local catalog.
    pub fn new() -> Self {
        Self {
            table_ids_by_name: HashMap::new(),
            tables_by_id: BTreeMap::new(),
            next_table_id: Some(1),
        }
    }

    /// raise the next table identity to a checked recovery floor.
    ///
    /// Recovery floors include historical table IDs that may no longer appear
    /// in the visible catalog. This method never lowers an allocator already
    /// advanced by installed schema definitions
    pub fn restore_table_id_floor(&mut self, next_table_id: TableId) -> Result<()> {
        if next_table_id.0 == 0 {
            return Err(Error::Configuration(
                "recovered next table ID must be nonzero".to_string(),
            ));
        }

        let current = self.next_table_id.ok_or_else(|| {
            Error::Configuration("catalog table-ID allocator is already exhausted".to_string())
        })?;

        self.next_table_id = Some(current.max(next_table_id.0));

        Ok(())
    }

    /// return the largest table identity consumed by this catalog allocator
    ///
    /// the value includes recovered allocation history even when a future
    /// DROP TABLE removes the corresponding schema from the visible catlaog
    pub fn table_id_high_water_mark(&self) -> TableId {
        match self.next_table_id {
            Some(next_table_id) => TableId(next_table_id - 1),
            None => TableId(u64::MAX),
        }
    }

    /// Allocate, validate, and publish one in-memory table.
    ///
    /// Durable single-node creation uses `prepare_table` and
    /// `publish_prepared_table` separately so WAL synchronization can occur
    /// between validation and publication.
    pub fn add_table(
        &mut self,
        name: impl Into<String>,
        columns: Vec<ColumnSchema>,
        primary_key_column_ids: Vec<ColumnId>,
    ) -> Result<TableId> {
        let schema = self.prepare_table(name, columns, primary_key_column_ids)?;

        let table_id = schema.id;
        self.publish_prepared_table(schema)?;

        Ok(table_id)
    }

    /// Construct and validate the next local table without publishing it.
    ///
    /// This method does not advance the table-ID allocator or change catalog
    /// lookup results. The durable coordinator calls it before allocating the
    /// catalog timestamp or appending a WAL record.
    pub(crate) fn prepare_table(
        &self,
        name: impl Into<String>,
        columns: Vec<ColumnSchema>,
        primary_key_column_ids: Vec<ColumnId>,
    ) -> Result<TableSchema> {
        let name = name.into();

        if self.table_ids_by_name.contains_key(&name) {
            return Err(Error::ConstraintViolation(format!(
                "table already exists: {name}"
            )));
        }

        let table_id = TableId(self.next_table_id.ok_or_else(|| {
            Error::ConstraintViolation("table ID space has been exhausted".to_string())
        })?);

        let schema = TableSchema {
            id: table_id,
            name,
            columns,
            primary_key_column_ids,
            schema_version: 1,
            tablet_count: 1,
        };

        validate_table_schema(&schema)?;

        Ok(schema)
    }

    /// Publish the exact schema previously returned by `prepare_table`.
    ///
    /// The durable coordinator owns exclusive mutable access between these
    /// operations. Any mismatch at publication is therefore an internal
    /// invariant failure requiring recovery from the authoritative log.
    pub(crate) fn publish_prepared_table(
        &mut self,
        schema: TableSchema,
    ) -> Result<Arc<TableSchema>> {
        let expected_table_id = TableId(self.next_table_id.ok_or_else(|| {
            Error::CorruptData(
                "catalog table-ID allocator was exhausted between \
                     validation and publication"
                    .to_string(),
            )
        })?);

        if schema.id != expected_table_id {
            return Err(Error::CorruptData(format!(
                "prepared table ID {} does not match the next publishable \
                 table ID {}",
                schema.id.0, expected_table_id.0
            )));
        }

        if self.table_ids_by_name.contains_key(&schema.name)
            || self.tables_by_id.contains_key(&schema.id)
        {
            return Err(Error::CorruptData(format!(
                "prepared table {} became occupied before publication",
                schema.name
            )));
        }

        self.install_table(schema)
    }

    /// Install a fully assigned schema from an authoritative external source.
    ///
    /// This method preserves table IDs, schema versions, column IDs, and tablet
    /// counts exactly as supplied. Reinstalling an identical schema is
    /// idempotent.
    ///
    /// Installing a newer version over an existing table is intentionally not
    /// supported in Phase 2.4. Schema evolution will require a separate
    /// metadata command and stricter cross-version validation.
    pub fn install_table(&mut self, schema: TableSchema) -> Result<Arc<TableSchema>> {
        validate_table_schema(&schema)?;

        if let Some(existing) = self.tables_by_id.get(&schema.id) {
            if existing.as_ref() == &schema {
                return Ok(existing.clone());
            }

            return Err(Error::ConstraintViolation(format!(
                "table ID {} is already assigned to table {}",
                schema.id.0, existing.name
            )));
        }

        if let Some(existing_id) = self.table_ids_by_name.get(&schema.name) {
            return Err(Error::ConstraintViolation(format!(
                "table name {} is already assigned to table ID {}",
                schema.name, existing_id.0
            )));
        }

        let next_after_installed = schema.id.0.checked_add(1);

        self.next_table_id = match (self.next_table_id, next_after_installed) {
            (None, _) | (_, None) => None,
            (Some(current), Some(candidate)) => Some(current.max(candidate)),
        };

        let snapshot = Arc::new(schema);

        self.table_ids_by_name
            .insert(snapshot.name.clone(), snapshot.id);
        self.tables_by_id.insert(snapshot.id, snapshot.clone());

        Ok(snapshot)
    }

    /// Decode, validate, and install a durable catalog definition.
    pub fn install_definition(&mut self, definition: TableDefinition) -> Result<Arc<TableSchema>> {
        self.install_table(TableSchema::from_definition(definition)?)
    }
}

impl Catalog for MemoryCatalog {
    fn table_by_name(&self, name: &str) -> Option<Arc<TableSchema>> {
        let id = self.table_ids_by_name.get(name)?;
        self.tables_by_id.get(id).cloned()
    }

    fn table_by_id(&self, id: TableId) -> Option<Arc<TableSchema>> {
        self.tables_by_id.get(&id).cloned()
    }

    fn list_tables(&self) -> Vec<Arc<TableSchema>> {
        self.tables_by_id.values().cloned().collect()
    }
}

/// Validate every invariant required for a publishable table schema.
fn validate_table_schema(schema: &TableSchema) -> Result<()> {
    if schema.id.0 == 0 {
        return Err(Error::ConstraintViolation(
            "table ID 0 is reserved".to_string(),
        ));
    }

    if schema.schema_version == 0 {
        return Err(Error::ConstraintViolation(format!(
            "table {} must have a nonzero schema version",
            schema.name
        )));
    }

    if schema.tablet_count == 0 {
        return Err(Error::ConstraintViolation(format!(
            "table {} must have at least one tablet",
            schema.name
        )));
    }

    validate_table_definition(
        &schema.name,
        &schema.columns,
        &schema.primary_key_column_ids,
    )
}

/// Validate table names, column identities, and primary-key metadata.
fn validate_table_definition(
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

    let mut column_names = HashSet::new();
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

        if !column_names.insert(column.name.as_str()) {
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

    fn schema(id: u64, name: &str, schema_version: u64, tablet_count: u32) -> TableSchema {
        TableSchema {
            id: TableId(id),
            name: name.into(),
            columns: columns(),
            primary_key_column_ids: vec![ColumnId(1)],
            schema_version,
            tablet_count,
        }
    }

    #[test]
    fn new_and_default_allocate_table_id_one() {
        for mut catalog in [MemoryCatalog::new(), MemoryCatalog::default()] {
            let id = catalog
                .add_table("users", columns(), vec![ColumnId(1)])
                .unwrap();

            assert_eq!(id, TableId(1));
        }
    }

    #[test]
    fn local_table_ids_are_monotonic() {
        let mut catalog = MemoryCatalog::new();

        let first = catalog
            .add_table("users", columns(), vec![ColumnId(1)])
            .unwrap();

        let second = catalog
            .add_table("orders", columns(), vec![ColumnId(1)])
            .unwrap();

        assert_eq!(first, TableId(1));
        assert_eq!(second, TableId(2));
    }

    #[test]
    fn local_creation_uses_one_tablet_and_schema_version_one() {
        let mut catalog = MemoryCatalog::new();

        let id = catalog
            .add_table("users", columns(), vec![ColumnId(1)])
            .unwrap();

        let table = catalog.table_by_id(id).unwrap();

        assert_eq!(table.schema_version, 1);
        assert_eq!(table.tablet_count, 1);
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
    fn durable_definition_roundtrips() {
        let original = schema(42, "users", 3, 8);

        let decoded = TableSchema::from_definition(original.to_definition()).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn install_definition_populates_catalog() {
        let mut catalog = MemoryCatalog::new();
        let original = schema(12, "users", 4, 6);

        let installed = catalog
            .install_definition(original.to_definition())
            .unwrap();

        assert_eq!(installed.as_ref(), &original);
        assert_eq!(
            catalog.table_by_id(TableId(12)).unwrap().as_ref(),
            &original
        );
    }

    #[test]
    fn installing_identical_metadata_is_idempotent() {
        let mut catalog = MemoryCatalog::new();
        let descriptor = schema(7, "users", 2, 4);

        let first = catalog.install_table(descriptor.clone()).unwrap();

        let second = catalog.install_table(descriptor).unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(catalog.list_tables().len(), 1);
    }

    #[test]
    fn installed_table_id_advances_local_allocator() {
        let mut catalog = MemoryCatalog::new();

        catalog.install_table(schema(20, "imported", 3, 8)).unwrap();

        let local_id = catalog
            .add_table("local", columns(), vec![ColumnId(1)])
            .unwrap();

        assert_eq!(local_id, TableId(21));
    }

    #[test]
    fn rejects_conflicting_table_id() {
        let mut catalog = MemoryCatalog::new();

        catalog.install_table(schema(10, "users", 1, 1)).unwrap();

        let error = catalog
            .install_table(schema(10, "orders", 1, 1))
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("table ID 10 is already assigned")
        );
    }

    #[test]
    fn rejects_conflicting_table_name() {
        let mut catalog = MemoryCatalog::new();

        catalog.install_table(schema(10, "users", 1, 1)).unwrap();

        let error = catalog
            .install_table(schema(11, "users", 1, 1))
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("table name users is already assigned")
        );
    }

    #[test]
    fn rejects_zero_table_id() {
        let error = MemoryCatalog::new()
            .install_table(schema(0, "users", 1, 1))
            .unwrap_err();

        assert!(error.to_string().contains("table ID 0"));
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

    #[test]
    fn rejects_empty_table_name() {
        let error = MemoryCatalog::new()
            .add_table(" ", columns(), vec![ColumnId(1)])
            .unwrap_err();

        assert!(error.to_string().contains("table name cannot be empty"));
    }

    #[test]
    fn rejects_empty_column_name() {
        let invalid = vec![ColumnSchema {
            id: ColumnId(1),
            name: " ".into(),
            ty: DataType::Int,
            nullable: false,
        }];

        let error = MemoryCatalog::new()
            .add_table("users", invalid, vec![ColumnId(1)])
            .unwrap_err();

        assert!(error.to_string().contains("empty column name"));
    }

    #[test]
    fn rejects_empty_column_list() {
        let error = MemoryCatalog::new()
            .add_table("users", vec![], vec![ColumnId(1)])
            .unwrap_err();

        assert!(error.to_string().contains("at least one column"));
    }

    #[test]
    fn rejects_missing_primary_key() {
        let error = MemoryCatalog::new()
            .add_table("users", columns(), vec![])
            .unwrap_err();

        assert!(error.to_string().contains("primary key"));
    }

    #[test]
    fn rejects_duplicate_primary_key_ids() {
        let error = MemoryCatalog::new()
            .add_table("users", columns(), vec![ColumnId(1), ColumnId(1)])
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("duplicate primary-key column ID")
        );
    }

    #[test]
    fn rejects_missing_primary_key_column() {
        let error = MemoryCatalog::new()
            .add_table("users", columns(), vec![ColumnId(99)])
            .unwrap_err();

        assert!(error.to_string().contains("does not exist in table users"));
    }

    #[test]
    fn rejects_nullable_primary_key_column() {
        let mut invalid = columns();
        invalid[0].nullable = true;

        let error = MemoryCatalog::new()
            .add_table("users", invalid, vec![ColumnId(1)])
            .unwrap_err();

        assert!(error.to_string().contains("cannot be nullable"));
    }

    #[test]
    fn rejects_duplicate_column_names() {
        let invalid = vec![
            ColumnSchema {
                id: ColumnId(1),
                name: "id".into(),
                ty: DataType::Int,
                nullable: false,
            },
            ColumnSchema {
                id: ColumnId(2),
                name: "id".into(),
                ty: DataType::Text,
                nullable: true,
            },
        ];

        let error = MemoryCatalog::new()
            .add_table("users", invalid, vec![ColumnId(1)])
            .unwrap_err();

        assert!(error.to_string().contains("duplicate column name"));
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
    fn rejects_zero_schema_version() {
        let error = MemoryCatalog::new()
            .install_table(schema(10, "users", 0, 1))
            .unwrap_err();

        assert!(error.to_string().contains("nonzero schema version"));
    }

    #[test]
    fn rejects_zero_tablet_count() {
        let error = MemoryCatalog::new()
            .install_table(schema(10, "users", 1, 0))
            .unwrap_err();

        assert!(error.to_string().contains("at least one tablet"));
    }

    #[test]
    fn rejects_duplicate_local_table_name() {
        let mut catalog = MemoryCatalog::new();

        catalog
            .add_table("users", columns(), vec![ColumnId(1)])
            .unwrap();

        let error = catalog
            .add_table("users", columns(), vec![ColumnId(1)])
            .unwrap_err();

        assert!(error.to_string().contains("table already exists"));
    }
}
