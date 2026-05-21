use crate::proto::ids;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl NodeId {
    pub fn to_proto(&self) -> ids::NodeId {
        ids::NodeId { id: self.0 }
    }

    pub fn from_proto(proto: ids::NodeId) -> Self {
        NodeId(proto.id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TabletId(pub u64);

impl TabletId {
    pub fn to_proto(&self) -> ids::TabletId {
        ids::TabletId { id: self.0 }
    }

    pub fn from_proto(proto: ids::TabletId) -> Self {
        TabletId(proto.id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

impl TableId {
    pub fn to_proto(&self) -> ids::TableId {
        ids::TableId { id: self.0 }
    }

    pub fn from_proto(proto: ids::TableId) -> Self {
        TableId(proto.id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TxnId(pub u64);

impl TxnId {
    pub fn to_proto(&self) -> ids::TxnId {
        ids::TxnId { id: self.0 }
    }

    pub fn from_proto(proto: ids::TxnId) -> Self {
        TxnId(proto.id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub fn to_proto(&self) -> ids::Timestamp {
        ids::Timestamp { id: self.0 }
    }

    pub fn from_proto(proto: ids::Timestamp) -> Self {
        Timestamp(proto.id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RaftGroupId(pub u64);
impl RaftGroupId {
    pub fn to_proto(&self) -> ids::RaftGroupId {
        ids::RaftGroupId { id: self.0 }
    }
    pub fn from_proto(proto: ids::RaftGroupId) -> Self {
        RaftGroupId(proto.id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RowKey {
    pub table_id: TableId,
    pub primary_key_bytes: Vec<u8>,
}
impl RowKey {
    pub fn to_proto(&self) -> crate::proto::row::RowKey {
        crate::proto::row::RowKey {
            table_id_bytes: self.table_id.0.to_le_bytes().to_vec(),
            primary_key_bytes: self.primary_key_bytes.clone(),
        }
    }
    pub fn from_proto(proto: crate::proto::row::RowKey) -> Result<Self, &'static str> {
        if proto.table_id_bytes.len() != 8 {
            return Err("invalid table_id_bytes length");
        }
        let table_id = u64::from_le_bytes(
            proto
                .table_id_bytes
                .try_into()
                .map_err(|_| "invalid table_id_bytes")?,
        );
        Ok(RowKey {
            table_id: TableId(table_id),
            primary_key_bytes: proto.primary_key_bytes,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestId {
    pub client_id: u128,
    pub sequence: u64,
}
impl RequestId {
    pub fn to_proto(&self) -> ids::RequestId {
        ids::RequestId {
            client_id: self.client_id.to_le_bytes().to_vec(),
            sequence: self.sequence,
        }
    }
    pub fn from_proto(proto: ids::RequestId) -> Result<Self, &'static str> {
        if proto.client_id.len() != 16 {
            return Err("invalid client_id length");
        }
        let client_id = u128::from_le_bytes(
            proto
                .client_id
                .try_into()
                .map_err(|_| "invalid client_id")?,
        );
        Ok(RequestId {
            client_id,
            sequence: proto.sequence,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_roundtrip() {
        let id = NodeId(42);
        let decoded = NodeId::from_proto(id.to_proto());

        assert_eq!(id, decoded);
    }

    #[test]
    fn tablet_id_roundtrip() {
        let id = TabletId(99);
        let decoded = TabletId::from_proto(id.to_proto());

        assert_eq!(id, decoded);
    }

    #[test]
    fn raft_group_id_roundtrip() {
        let id = RaftGroupId(7);
        let decoded = RaftGroupId::from_proto(id.to_proto());

        assert_eq!(id, decoded);
    }

    #[test]
    fn request_id_roundtrip() {
        let id = RequestId {
            client_id: u128::MAX,
            sequence: 12345,
        };

        let decoded = RequestId::from_proto(id.to_proto()).unwrap();

        assert_eq!(id, decoded);
    }

    #[test]
    fn request_id_invalid_client_id_length() {
        let proto = crate::proto::ids::RequestId {
            client_id: vec![1, 2, 3],
            sequence: 0,
        };

        assert!(RequestId::from_proto(proto).is_err());
    }

    #[test]
    fn row_key_roundtrip() {
        let key = RowKey {
            table_id: TableId(5),
            primary_key_bytes: vec![0, 0, 0, 1],
        };

        let decoded = RowKey::from_proto(key.to_proto()).unwrap();

        assert_eq!(key, decoded);
    }

    #[test]
    fn row_key_invalid_table_id_bytes() {
        let proto = crate::proto::row::RowKey {
            table_id_bytes: vec![1, 2, 3],
            primary_key_bytes: vec![],
        };

        assert!(RowKey::from_proto(proto).is_err());
    }

    #[test]
    fn timestamp_ordering() {
        let a = Timestamp(100);
        let b = Timestamp(200);

        assert!(a < b);
        assert_eq!(Timestamp::from_proto(a.to_proto()), a);
    }
}
