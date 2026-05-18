use ragnordb_common::ids::TableId;

#[derive(Debug, Clone)]
pub struct TableDescriptor {
    pub id: TableId,
    pub name: String,
}
