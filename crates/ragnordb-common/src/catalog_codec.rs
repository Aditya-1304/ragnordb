use crate::proto::catalog;

pub struct ColumnDefinition {
    pub column_id: u64,
    pub name: String,
    pub ty: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int,
    Text,
    Bool,
}

impl DataType {
    pub fn to_proto(&self) -> catalog::DataType {
        match self {
            DataType::Int => catalog::DataType::Int,
            DataType::Text => catalog::DataType::Text,
            DataType::Bool => catalog::DataType::Bool,
        }
    }

    pub fn from_proto(proto: catalog::DataType) -> Result<Self, &'static str> {
        match proto {
            catalog::DataType::Int => Ok(DataType::Int),
            catalog::DataType::Text => Ok(DataType::Text),
            catalog::DataType::Bool => Ok(DataType::Bool),
            catalog::DataType::Unspecified => Err("unspecified data type"),
        }
    }
}

impl ColumnDefinition {
    pub fn to_proto(&self) -> catalog::ColumnDefinition {
        catalog::ColumnDefinition {
            column_id: self.column_id,
            name: self.name.clone(),
            r#type: self.ty.to_proto() as i32, //this is required by prost, to change type to r#type and cast it to i32
            nullable: self.nullable,
        }
    }

    pub fn from_proto(proto: catalog::ColumnDefinition) -> Result<Self, &'static str> {
        Ok(ColumnDefinition {
            column_id: proto.column_id,
            name: proto.name,
            ty: DataType::from_proto(
                catalog::DataType::try_from(proto.r#type).map_err(|_| "invalid enum")?,
            )?,
            nullable: proto.nullable,
        })
    }
}

pub struct TableDefinition {
    pub table_id: u64,
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key_column_ids: Vec<u64>,
    pub schema_version: u64,
    pub tablet_count: u32,
}

impl TableDefinition {
    pub fn to_proto(&self) -> catalog::TableDefinition {
        catalog::TableDefinition {
            table_id: self.table_id,
            name: self.name.clone(),
            columns: self.columns.iter().map(|c| c.to_proto()).collect(),
            primary_key_column_ids: self.primary_key_column_ids.clone(),
            schema_version: self.schema_version,
            tablet_count: self.tablet_count,
        }
    }

    pub fn from_proto(proto: catalog::TableDefinition) -> Result<Self, &'static str> {
        let columns = proto
            .columns
            .into_iter()
            .map(ColumnDefinition::from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TableDefinition {
            table_id: proto.table_id,
            name: proto.name,
            columns,
            primary_key_column_ids: proto.primary_key_column_ids,
            schema_version: proto.schema_version,
            tablet_count: proto.tablet_count,
        })
    }
}
