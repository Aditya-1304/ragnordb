//! RagnorDB owned record identities stored inside the WAL
//!
//! A-WAL owns physical record framing, checksums, durability and recovery of
//! valid byte prefix. this module owns the semantic mapping between stable
//! user defined record identifiers and ragnorDB payload schema
//!
//! Numeric record identifiers are part of the durable storage contract
//! once a released database writes an identifier, it must never be reused for
//! another playload

use ragnordb_common::{Error, Result};
use wal::types::{RecordType, record_types::USER_MIN};

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
