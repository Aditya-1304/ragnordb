//! RagnorDB owned record identities stored inside the WAL and semantic payloads
//!
//! A-WAL owns physical record framing, checksums, durability,
//! internal metadata and recovery of valid byte prefix.
//! this module owns the semantic mapping between stable
//! user defined record identifiers and ragnorDB payload schema
//!
//! Numeric record identifiers are part of the durable storage contract
//! once a released database writes an identifier, it must never be reused for
//! another playload

use prost::Message;
use ragnordb_common::{
    Error, Result,
    encoding::decode_row,
    ids::{TableId, Timestamp, TxnId},
    proto::wal as wal_proto,
};
use std::collections::BTreeMap;
use wal::types::{RecordType, record_types::USER_MIN};

use crate::key::decode_row_key;

/// current durable schema version for `SingleNodeTxnCommit`
pub const SINGLE_NODE_TXN_COMMIT_VERSION: u32 = 1;

/// snapshot pointer identifier
const SNAPSHOT_POINTER_ID: u16 = USER_MIN + 3;

/// Identifier for an atomically committed single node transaction
const SINGLE_NODE_TXN_COMMIT_ID: u16 = USER_MIN + 5;

/// Identifier for durable catalog mutation
///
/// intial catalog bootstrap is represented as the furst catalog update
const CATALOG_UPDATE_ID: u16 = USER_MIN + 6;

/// Identifier for a published checkpoint boundary.
const CHECKPOINT_MARKER_ID: u16 = USER_MIN + 7;

/// Semantic RagnorDB record kinds carried by A-WAL user records
///
/// Rust enum discriminants are deliberately not used as the durable format
/// The explicit mapping in `as_wal_record_type` prevents enum reordering from
/// silently changing the on-disk representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RagnorDbWalRecordType {
    /// Points recovery to a durable database snapshot.
    SnapshotPointer,

    /// Contains every mutation committed by one local transaction.
    SingleNodeTxnCommit,

    /// Contains one durable catalog state transition.
    CatalogUpdate,

    /// Publishes a checkpoint after its referenced snapshot is durable.
    CheckpointMarker,
}

impl RagnorDbWalRecordType {
    /// return the permanent A-WAL record identitdeir for this playload
    pub const fn as_wal_record_type(self) -> RecordType {
        let record_id = match self {
            Self::SnapshotPointer => SNAPSHOT_POINTER_ID,
            Self::SingleNodeTxnCommit => SINGLE_NODE_TXN_COMMIT_ID,
            Self::CatalogUpdate => CATALOG_UPDATE_ID,
            Self::CheckpointMarker => CHECKPOINT_MARKER_ID,
        };

        RecordType::new(record_id)
    }

    /// Classify one record returned by A-WAL iteration.
    ///
    /// The result distinguishes the three cases required by recovery:
    ///
    /// - `Ok(None)` means the record belongs to A-WAL's internal namespace.
    /// - `Ok(Some(kind))` identifies a supported RagnorDB payload.
    /// - `Err(Error::CorruptData)` means the record is user-defined but this
    ///   binary cannot safely determine its payload schema.
    ///
    /// A-WAL remains responsible for validating its internal record semantics.
    /// RagnorDB must not attempt to decode those records as database commands.
    pub fn classify(record_type: RecordType) -> Result<Option<Self>> {
        if record_type.is_internal() {
            return Ok(None);
        }

        let logical_type = match record_type.as_u16() {
            SNAPSHOT_POINTER_ID => Self::SnapshotPointer,
            SINGLE_NODE_TXN_COMMIT_ID => Self::SingleNodeTxnCommit,
            CATALOG_UPDATE_ID => Self::CatalogUpdate,
            CHECKPOINT_MARKER_ID => Self::CheckpointMarker,
            unknown_id => {
                return Err(Error::CorruptData(format!(
                    "unknown RagnorDB user WAL record type {unknown_id}; \
                     this binary cannot safely select a payload decoder"
                )));
            }
        };

        Ok(Some(logical_type))
    }
}

/// complete mutation batch committed by the single node transactional engine
///
/// `writes` uses canonical encoded row keys as map keys. `BtreeMap` will
/// guarantee deterministic serialization order and prevents multiple final
/// mutations for the same encoded row
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingleNodeTxnCommit {
    /// table owned by the local tablet that produced the commit
    pub table_id: TableId,

    /// Stable transaction identity
    pub txn_id: TxnId,

    /// snapshot timestamp used for conflict validation
    pub start_timestamp: Timestamp,

    /// visibility timestamp assigned to the complete batch
    pub commit_timestamp: Timestamp,

    /// Deterministically ordered, unique mutation set
    pub writes: BTreeMap<Vec<u8>, WalMutation>,
}

/// Durable row mutation stored inside a single-node commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalMutation {
    /// Insert or replace a complete canonical encoded row.
    Put(Vec<u8>),

    /// Make the row absent at and after the commit timestamp.
    Delete,
}

impl SingleNodeTxnCommit {
    /// Validate and encode this commit as a protobuf payload for A-WAL.
    ///
    /// Invalid in-memory values are caller errors and are rejected before any
    /// bytes can be appended to durable history.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate().map_err(|message| {
            Error::InvalidArgument(format!("invalid SingleNodeTxnCommit: {message}"))
        })?;

        Ok(self.to_proto().encode_to_vec())
    }

    /// Decode and validate a commit payload read from A-WAL.
    ///
    /// Structurally malformed protobuf and violations of durable transaction
    /// invariants are corruption from the perspective of recovery.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let proto = wal_proto::SingleNodeTxnCommit::decode(bytes).map_err(|error| {
            Error::CorruptData(format!(
                "failed to decode SingleNodeTxnCommit protobuf: {error}"
            ))
        })?;

        Self::from_proto(proto)
    }

    fn to_proto(&self) -> wal_proto::SingleNodeTxnCommit {
        let writes = self
            .writes
            .iter()
            .map(|(key, mutation)| {
                let mutation = match mutation {
                    WalMutation::Put(row) => wal_proto::wal_write::Mutation::PutRow(row.clone()),
                    WalMutation::Delete => {
                        wal_proto::wal_write::Mutation::Delete(wal_proto::DeleteMarker {})
                    }
                };

                wal_proto::WalWrite {
                    key: key.clone(),
                    mutation: Some(mutation),
                }
            })
            .collect();

        wal_proto::SingleNodeTxnCommit {
            version: SINGLE_NODE_TXN_COMMIT_VERSION,
            table_id: Some(self.table_id.to_proto()),
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            commit_timestamp: Some(self.commit_timestamp.to_proto()),
            writes,
        }
    }

    fn from_proto(proto: wal_proto::SingleNodeTxnCommit) -> Result<Self> {
        if proto.version != SINGLE_NODE_TXN_COMMIT_VERSION {
            return Err(Error::CorruptData(format!(
                "unsupported SingleNodeTxnCommit version {}; expected {}",
                proto.version, SINGLE_NODE_TXN_COMMIT_VERSION
            )));
        }

        let table_id = proto.table_id.ok_or_else(|| {
            Error::CorruptData("SingleNodeTxnCommit is missing its table identifier".to_string())
        })?;

        let txn_id = proto.txn_id.ok_or_else(|| {
            Error::CorruptData(
                "SingleNodeTxnCommit is missing its transaction identifier".to_string(),
            )
        })?;

        let start_timestamp = proto.start_timestamp.ok_or_else(|| {
            Error::CorruptData("SingleNodeTxnCommit is missing its start timestamp".to_string())
        })?;

        let commit_timestamp = proto.commit_timestamp.ok_or_else(|| {
            Error::CorruptData("SingleNodeTxnCommit is missing its commit timestamp".to_string())
        })?;

        let mut writes = BTreeMap::new();

        for write in proto.writes {
            let mutation = match write.mutation {
                Some(wal_proto::wal_write::Mutation::PutRow(row)) => WalMutation::Put(row),
                Some(wal_proto::wal_write::Mutation::Delete(_)) => WalMutation::Delete,
                None => {
                    return Err(Error::CorruptData(
                        "WAL write is missing its mutation".to_string(),
                    ));
                }
            };

            if writes.contains_key(&write.key) {
                return Err(Error::CorruptData(
                    "SingleNodeTxnCommit contains a duplicate encoded row key".to_string(),
                ));
            }

            writes.insert(write.key, mutation);
        }

        let record = Self {
            table_id: TableId::from_proto(table_id),
            txn_id: TxnId::from_proto(txn_id),
            start_timestamp: Timestamp::from_proto(start_timestamp),
            commit_timestamp: Timestamp::from_proto(commit_timestamp),
            writes,
        };

        record.validate().map_err(|message| {
            Error::CorruptData(format!("invalid durable SingleNodeTxnCommit: {message}"))
        })?;

        Ok(record)
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if self.table_id.0 == 0 {
            return Err("table ID 0 is reserved".to_string());
        }

        if self.txn_id.0 == 0 {
            return Err("transaction ID 0 is reserved".to_string());
        }

        if self.start_timestamp.0 == 0 {
            return Err("start timestamp 0 is reserved".to_string());
        }

        if self.commit_timestamp.0 == 0 {
            return Err("commit timestamp 0 is reserved".to_string());
        }

        if self.commit_timestamp <= self.start_timestamp {
            return Err(format!(
                "commit timestamp {} must be greater than start timestamp {}",
                self.commit_timestamp.0, self.start_timestamp.0
            ));
        }

        if self.writes.is_empty() {
            return Err("SingleNodeTxnCommit must contain at least one write".to_string());
        }

        for (key, mutation) in &self.writes {
            let row_key = decode_row_key(key)
                .map_err(|error| format!("commit contains a noncanonical row key: {error}"))?;

            if row_key.table_id != self.table_id {
                return Err(format!(
                    "encoded row key belongs to table {}, which does not match \
                     commit table {}",
                    row_key.table_id.0, self.table_id.0
                ));
            }

            if let WalMutation::Put(row) = mutation {
                decode_row(row).map_err(|error| {
                    format!("Put mutation contains a malformed canonical row: {error}")
                })?;
            }
        }

        Ok(())
    }
}
