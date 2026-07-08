use ragnordb_common::catalog_codec::DataType;
use ragnordb_common::ids::TableId;
use ragnordb_common::{Error, Result};

use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub id: u64,
    pub name: String,
    pub ty: DataType,
    pub nullable: bool,
}

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
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.name == name)
    }

    pub fn primary_key_columns(&self) -> Vec<&ColumnSchema> {
        self.primary_key_column_ids
            .iter()
            .filter_map(|id| self.columns.iter().find(|column| column.id == *id))
            .collect()
    }
}

pub trait Catalog {
    fn table_by_name(&self, name: &str) -> Option<&TableSchema>;
}

#[derive(Debug, Default)]
pub struct MemoryCatalog {
    tables_by_name: HashMap<String, TableSchema>,
    next_table_id: u64,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self {
            tables_by_name: HashMap::new(),
            next_table_id: 1,
        }
    }

    pub fn add_table(
        &mut self,
        name: impl Into<String>,
        columns: Vec<ColumnSchema>,
        primary_key_column_ids: Vec<u64>,
    ) -> Result<TableId> {
        let name = name.into();

        if self.tables_by_name.contains_key(&name) {
            return Err(Error::InvalidArgument(format!(
                "table already exists: {name}"
            )));
        }

        if primary_key_column_ids.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "table {name} must have at least one primary key column"
            )));
        }

        let mut seen_names = HashSet::new();
        for col in &columns {
            if !seen_names.insert(&col.name) {
                return Err(Error::InvalidArgument(format!(
                    "duplicate column name: {}",
                    col.name
                )));
            }
        }

        let mut seen_ids = HashSet::new();
        for col in &columns {
            if !seen_ids.insert(col.id) {
                return Err(Error::InvalidArgument(format!(
                    "duplicate column id: {}",
                    col.id
                )));
            }
        }

        let column_ids: HashSet<u64> = columns.iter().map(|c| c.id).collect();
        for pk_id in &primary_key_column_ids {
            if !column_ids.contains(pk_id) {
                return Err(Error::InvalidArgument(format!(
                    "primary key column id {pk_id} does not exist in table {name}"
                )));
            }
        }

        let table_id = TableId(self.next_table_id);
        self.next_table_id += 1;

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
