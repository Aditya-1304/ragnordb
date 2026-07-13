//! This file contains all the IDs which other crate uses throughout the codebase
//! every ID type implements:
//!  to_proto(&self) -> proto::ids::{Type}
//!  from_proto(proto) -> Self
//!
//! The proto field is always a single uint64 { id: self.0 },
//! except for RequestId (client_id as 16 bytes + sequence as u64)
//! and RowKey (table_id_bytes + primary_key_bytes).

use crate::proto::ids;
use serde::{Deserialize, Serialize};

/// Works as an identifier for a node in the DB cluster
///
/// Node IDs are statically added in the bootstrap or generation of node and never changes during a node's lifetime.
/// It is used in:
/// - Raft group membership
/// - Internode transport
/// - Metadata group leader/tablet leader caches
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

/// This Identifies a single tablet (shard) within in the cluster
///
/// Each tablet is owned by exactly one raft group and holds
/// a partition of a table's key space.
/// Tablet IDs are assigned by the metadata of the raft group during the table creation (CREATE TABLE)
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

/// This identifies a logical SQL table across the cluster
///
/// Table IDs are assigned once by the metadata Raft group
/// and are never reused after that, even after Dropping the Table
/// they form the first component of every internal key path (/table/{table_id}/...)
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

/// this is the stable identifier for one column within a table schema
///
/// Column Ids are assinged when a table is created and are never reused within
/// that table. A column Id is distint from its ordinal: the Id remain stable
/// across the schema versions, while the ordinal identifies its position in the
/// encoded row representation
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnId(pub u64);

impl ColumnId {
    pub fn to_proto(&self) -> ids::ColumnId {
        ids::ColumnId { id: self.0 }
    }

    pub fn from_proto(proto: ids::ColumnId) -> Self {
        Self(proto.id)
    }
}

/// This identifies a distributed transaction globally.
///
/// Assigned by the timestamp oracle (also called metadata Raft group) during the start/begining of a transaction
/// this is used in lock records, write records and transaction status records
/// to associate intents with their corresponding owning transaction
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

/// A hybrid-monotonic timestamp for MVCC ordering
///
/// Timestamps are allocated from the timestamp oracle in the
/// metadata raft group. They satisfy:
///   start_ts < commit_ts for every committed transaction
///   timestamps are monotonic and never reused
///
/// Used in:
///   - MVCC version keys (default/{user_key}/{start_ts})
///   - Write records (write/{user_key}/{commit_ts})
///   - Snapshot reads (read at start_ts)
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

/// This identifies a single Raft consensus group.
///
/// Every tablet has its own Raft group. also there is only one
/// metadata raft group
/// RaftgroupID binds together :
///   - A-WAL records (each record stores group_id)
///   - Raft log stores (per-group index→LSN maps)
///   - Inter-node transport demux
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

/// This identifies a singe row by its table and primary key encoded in it
///
/// Rowkey is the internal representation of a SQL row's location:
///   /table/{table_id}/pk/{encoded_primary_key}
///
/// still it is not the full MVCC internal key - it does not include
/// timestamp or column family. Those will be added by the tablet layer
/// when constructing default/lock/write keys
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

/// This helps in uniquely identifing a client request for idempotent retry
///
/// When a client retries after a timeout, the same RequestId ensures
/// that the tablet state machine does not apply the command twice or even
/// multiple times
/// the state machine caches (client_id, last_sequence) and compares
/// incoming requests against it
///
/// client_id: 128-bit unique client identifier (e.g., UUID v4).
/// sequence:  monotonically increasing per-client counter.
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

    #[test]
    fn column_id_roundtrip() {
        let id = ColumnId(42);
        let decoded = ColumnId::from_proto(id.to_proto());

        assert_eq!(decoded, id);
    }
}
