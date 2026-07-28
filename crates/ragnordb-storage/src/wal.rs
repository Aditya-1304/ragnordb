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
use ragnordb_catalog::{CatalogLogExtent, CatalogLogRecord, DurableCatalogLog, TableSchema};
use ragnordb_common::{
    Error, Result,
    catalog_codec::TableDefinition,
    command_codec::{CatalogCommand, CatalogOperation},
    encoding::decode_row,
    ids::{TableId, Timestamp, TxnId},
    proto::wal as wal_proto,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    format,
    sync::Arc,
};
use wal::{
    error::AppendFailure,
    io::directory::SegmentDirectory,
    lsn::Lsn,
    types::{RecordType, record_types::USER_MIN},
    wal::WalHandle,
};

use crate::key::decode_row_key;

/// current durable schema version for `SingleNodeTxnCommit`
pub const SINGLE_NODE_TXN_COMMIT_VERSION: u32 = 1;

/// Current durable schema version for `SnapshotPointer`.
pub const SNAPSHOT_POINTER_VERSION: u32 = 1;

/// current durable schema version for `CatalogUpdate`
pub const CATALOG_UPDATE_VERSION: u32 = 1;

/// current duarable schema version for `CheckpointMarker`
pub const CHECKPOINT_MARKER_VERSION: u32 = 1;

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

/// storage owned logical extent of one durably appended RagnorDB record
///
/// The adapter returns logical WAL positions for diagnostics and future
/// checkpoint accounting without exposing A-WAL headers, checksums, alignment,
/// compression, segment seals, or any other physical framing details
#[must_use = "the durable WAL extent is required for diagnostics and checkpoint accounting"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableWalExtent {
    /// logical position at which the RagnorDB record begins
    pub start_lsn: Lsn,

    /// first logical position after the complete RagnorDB record
    pub end_lsn: Lsn,
}

impl DurableWalExtent {
    /// a storage owned extent from raw logical WAL positions
    ///
    /// this constructor supports semantic commit log implementations without
    /// requiring transaction or executor crates to import A-WAL's `Lsn` type
    pub const fn from_raw(start_lsn: u64, end_lsn: u64) -> Self {
        Self {
            start_lsn: Lsn::new(start_lsn),
            end_lsn: Lsn::new(end_lsn),
        }
    }
}

/// semantic durable log boundary used by the transaction coordinator
///
/// the transaction layer supplies a validated RagnorDB commit record and
/// receives only its logical durable extent. A-WAL record identifiers, framing,
/// checksums, alignment, and synchronization mechanics remain owned by the
/// storage adapter
pub trait DurableCommitLog {
    /// append and synchronize one complete single-node transaction commit
    fn append_single_node_commit(&self, commit: &SingleNodeTxnCommit) -> Result<DurableWalExtent>;
}

impl<T> DurableCommitLog for Arc<T>
where
    T: DurableCommitLog + ?Sized,
{
    fn append_single_node_commit(&self, commit: &SingleNodeTxnCommit) -> Result<DurableWalExtent> {
        (**self).append_single_node_commit(commit)
    }
}

/// exact durable WAL extents assigned to one checkpoint publication pair
///
/// this value is returned only after both the snapshot pointer and its matching
/// checkpoint marker have reached A-WAL's durable frontier
#[must_use = "checkpoint WAL extents prove the complete publication boundary"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableCheckpointExtents {
    /// durable extent assigned to the snapshot pointer
    pub pointer_extent: DurableWalExtent,

    /// durable extent assigned to the matching checkpoint marker
    pub marker_extent: DurableWalExtent,
}

/// semantic durability boundary for publishing checkpoint WAL metadata
///
/// implementations must validate both records before WAL admission, append and
/// synchronize the pointer first, then append and synchronize the marker
pub trait DurableCheckpointLog {
    /// durably append one exactly matching pointer and marker pair
    fn append_checkpoint_records(
        &self,
        pointer: &SnapshotPointer,
        marker: &CheckpointMarker,
    ) -> Result<DurableCheckpointExtents>;
}

impl<T> DurableCheckpointLog for Arc<T>
where
    T: DurableCheckpointLog + ?Sized,
{
    fn append_checkpoint_records(
        &self,
        pointer: &SnapshotPointer,
        marker: &CheckpointMarker,
    ) -> Result<DurableCheckpointExtents> {
        (**self).append_checkpoint_records(pointer, marker)
    }
}

/// durable storage adapter between RagnorDB records and A-WAL
///
/// transaction and SQL layers provide semantic RagnorDB records. This adapter
/// owns protobuf encoding, permanent record-type selection, synchronous WAL
/// durability, and conversion into canonical database errors
pub struct RagnorDbWalAdapter<D, C>
where
    D: SegmentDirectory,
{
    wal: WalHandle<D, C>,
}

impl<D, C> RagnorDbWalAdapter<D, C>
where
    D: SegmentDirectory + Clone,
{
    /// construct an adapter around the node's serialized A-WAL writer
    pub fn new(wal: WalHandle<D, C>) -> Self {
        Self { wal }
    }

    /// encode and append one atomic single node transaction commit
    ///
    /// success is returned only after A-WAL confirms that the complete logical
    /// record extent is durable. Encoding failures occur before WAL admission
    /// and therefore retain their existing canonical validation error
    pub fn append_single_node_commit(
        &self,
        commit: &SingleNodeTxnCommit,
    ) -> Result<DurableWalExtent> {
        let payload = commit.encode()?;
        let record_type = RagnorDbWalRecordType::SingleNodeTxnCommit.as_wal_record_type();

        let extent = self
            .wal
            .append_and_sync(record_type, &payload)
            .map_err(map_commit_append_failure)?;

        Ok(DurableWalExtent {
            start_lsn: extent.start_lsn,
            end_lsn: extent.end_lsn,
        })
    }

    /// Encode and durably append one catalog state transition.
    pub fn append_catalog_update(&self, record: &CatalogLogRecord) -> Result<CatalogLogExtent> {
        let update = CatalogUpdate {
            table_id: record.table_id,
            update_timestamp: record.update_timestamp,
            command: record.command.clone(),
        };

        let payload = update.encode()?;
        let record_type = RagnorDbWalRecordType::CatalogUpdate.as_wal_record_type();

        let extent = self
            .wal
            .append_and_sync(record_type, &payload)
            .map_err(map_catalog_append_failure)?;

        Ok(CatalogLogExtent {
            start_lsn: extent.start_lsn.as_u64(),
            end_lsn: extent.end_lsn.as_u64(),
        })
    }
}

impl<D, C> DurableCommitLog for RagnorDbWalAdapter<D, C>
where
    D: SegmentDirectory + Clone,
{
    fn append_single_node_commit(&self, commit: &SingleNodeTxnCommit) -> Result<DurableWalExtent> {
        RagnorDbWalAdapter::append_single_node_commit(self, commit)
    }
}

impl<D, C> DurableCheckpointLog for RagnorDbWalAdapter<D, C>
where
    D: SegmentDirectory + Clone,
{
    fn append_checkpoint_records(
        &self,
        pointer: &SnapshotPointer,
        marker: &CheckpointMarker,
    ) -> Result<DurableCheckpointExtents> {
        if pointer.snapshot_id != marker.snapshot_id
            || pointer.snapshot_timestamp != marker.snapshot_timestamp
            || pointer.replay_from_lsn != marker.replay_from_lsn
        {
            return Err(Error::InvalidArgument(
                "checkpoint marker must exactly match its snapshot pointer identity, \
                 timestamp, and replay boundary"
                    .to_string(),
            ));
        }

        // encode and validate both records before admitting either record to
        // A-WAL. Invalid marker metadata must not leave an orphan pointer
        let pointer_payload = pointer.encode()?;
        let marker_payload = marker.encode()?;
        let pointer_extent = self
            .wal
            .append_and_sync(
                RagnorDbWalRecordType::SnapshotPointer.as_wal_record_type(),
                &pointer_payload,
            )
            .map_err(|failure| map_checkpoint_append_failure(failure, "SnapshotPointer"))?;
        let marker_extent = self
            .wal
            .append_and_sync(
                RagnorDbWalRecordType::CheckpointMarker.as_wal_record_type(),
                &marker_payload,
            )
            .map_err(|failure| map_checkpoint_append_failure(failure, "CheckpointMarker"))?;

        Ok(DurableCheckpointExtents {
            pointer_extent: DurableWalExtent {
                start_lsn: pointer_extent.start_lsn,
                end_lsn: pointer_extent.end_lsn,
            },
            marker_extent: DurableWalExtent {
                start_lsn: marker_extent.start_lsn,
                end_lsn: marker_extent.end_lsn,
            },
        })
    }
}

impl<D, C> DurableCatalogLog for RagnorDbWalAdapter<D, C>
where
    D: SegmentDirectory + Clone,
{
    fn append_catalog_update(&self, update: &CatalogLogRecord) -> Result<CatalogLogExtent> {
        RagnorDbWalAdapter::append_catalog_update(self, update)
    }
}

/// preserve A-WAL's staging boundary while converting it into canonical
/// database errors
///
/// this conversion intentionally matches `AppendFailure` directly. Converting
/// only its underlying `WalError` would discard whether the commit definitely
/// received no extent or may already exist in the recovered durable prefix
fn map_commit_append_failure(failure: AppendFailure) -> Error {
    match failure {
        AppendFailure::NotStaged(source) => Error::WalAppendNotStaged {
            reason: source.to_string(),
        },

        AppendFailure::OutcomeUnknown { extent, source } => Error::CommitOutcomeUnknown {
            start_lsn: extent.start_lsn.as_u64(),
            end_lsn: extent.end_lsn.as_u64(),
            reason: source.to_string(),
        },
    }
}

fn map_catalog_append_failure(failure: AppendFailure) -> Error {
    match failure {
        AppendFailure::NotStaged(source) => Error::WalAppendNotStaged {
            reason: source.to_string(),
        },

        AppendFailure::OutcomeUnknown { extent, source } => Error::CatalogOutcomeUnknown {
            start_lsn: extent.start_lsn.as_u64(),
            end_lsn: extent.end_lsn.as_u64(),
            reason: source.to_string(),
        },
    }
}

fn map_checkpoint_append_failure(failure: AppendFailure, stage: &'static str) -> Error {
    match failure {
        AppendFailure::NotStaged(source) => Error::WalAppendNotStaged {
            reason: format!("checkpoint {stage} append failed before staging: {source}"),
        },

        AppendFailure::OutcomeUnknown { extent, source } => Error::CheckpointOutcomeUnknown {
            stage,
            start_lsn: extent.start_lsn.as_u64(),
            end_lsn: extent.end_lsn.as_u64(),
            reason: source.to_string(),
        },
    }
}

/// versioned durable envelope for one catalog state transition
///
/// the operation reuses `CatalogCommand`, ensuring that single-node recovery and
/// the later replicated metadata path share one operation representation
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogUpdate {
    /// Stable identity of the table affected by the operation
    pub table_id: TableId,

    /// Nonzero timestamp assigned to the catalog transition
    pub update_timestamp: Timestamp,

    /// Complete catalog command applied during recovery
    pub command: CatalogCommand,
}

impl CatalogUpdate {
    /// Validate and encode this catalog update for A-WAL
    ///
    /// Invalid wrapper metadata or an invalid table definition is rejected
    /// before it can become durable history
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate().map_err(|message| {
            Error::InvalidArgument(format!("invalid CatalogUpdate: {message}"))
        })?;

        Ok(self.to_proto().encode_to_vec())
    }

    /// Decode and validate a catalog update read from A-WAL
    ///
    /// Missing fields, unsupported versions, invalid catalog commands, and
    /// schema violations are corrupt durable data during recovery
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let proto = wal_proto::CatalogUpdate::decode(bytes).map_err(|error| {
            Error::CorruptData(format!("failed to decode CatalogUpdate protobuf: {error}"))
        })?;

        Self::from_proto(proto)
    }

    fn to_proto(&self) -> wal_proto::CatalogUpdate {
        wal_proto::CatalogUpdate {
            version: CATALOG_UPDATE_VERSION,
            update_timestamp: Some(self.update_timestamp.to_proto()),
            table_id: Some(self.table_id.to_proto()),
            command: Some(self.command.to_proto()),
        }
    }

    fn from_proto(proto: wal_proto::CatalogUpdate) -> Result<Self> {
        if proto.version != CATALOG_UPDATE_VERSION {
            return Err(Error::CorruptData(format!(
                "unsupported CatalogUpdate version {}; expected {}",
                proto.version, CATALOG_UPDATE_VERSION
            )));
        }

        let update_timestamp = proto.update_timestamp.ok_or_else(|| {
            Error::CorruptData("CatalogUpdate is missing its update timestamp".to_string())
        })?;

        let table_id = proto.table_id.ok_or_else(|| {
            Error::CorruptData("CatalogUpdate is missing its table identifier".to_string())
        })?;

        let command = proto.command.ok_or_else(|| {
            Error::CorruptData("CatalogUpdate is missing its catalog command".to_string())
        })?;

        let command = CatalogCommand::from_proto(command).map_err(|message| {
            Error::CorruptData(format!("invalid CatalogUpdate command: {message}"))
        })?;

        let update = Self {
            table_id: TableId::from_proto(table_id),
            update_timestamp: Timestamp::from_proto(update_timestamp),
            command,
        };

        update.validate().map_err(|message| {
            Error::CorruptData(format!("invalid durable CatalogUpdate: {message}"))
        })?;

        Ok(update)
    }

    fn table_definition(&self) -> &TableDefinition {
        match &self.command.operation {
            CatalogOperation::CreateTable(operation) => &operation.table_def,
        }
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if self.table_id.0 == 0 {
            return Err("table ID 0 is reserved".to_string());
        }

        if self.update_timestamp.0 == 0 {
            return Err("update timestamp 0 is reserved".to_string());
        }

        let definition = self.table_definition();
        let operation_table_id = TableId(definition.table_id);

        if operation_table_id.0 == 0 {
            return Err("catalog operation table ID 0 is reserved".to_string());
        }

        if operation_table_id != self.table_id {
            return Err(format!(
                "catalog operation table {} does not match update table {}",
                operation_table_id.0, self.table_id.0
            ));
        }

        // Reuse the catalog's canonical validator instead of maintaining a
        // second, potentially divergent set of schema invariants in WAL code
        TableSchema::from_definition(definition.clone()).map_err(|error| {
            format!("catalog operation contains an invalid table definition: {error}")
        })?;

        Ok(())
    }
}

/// durable pointer to a published database snapshot
///
/// this type contains only self-contained snapshot metadata; file publication,
/// checksum verification, replay-boundary validation against an open A-WAL, and
/// retention advancement will be done later !TODO
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPointer {
    /// Stable, nonzero identity referenced by checkpoint metadata
    pub snapshot_id: u64,

    /// highest MVCC timestamp represented by the snapshot
    pub snapshot_timestamp: Timestamp,

    /// first logical WAL position not represented by the snapshot
    ///
    /// recovery must replay records at or after this position. `Lsn::ZERO`
    /// remains valid and prevents the snapshot from skipping WAL history
    pub replay_from_lsn: Lsn,

    /// portable path beneath the configured database snapshot directory
    pub relative_path: String,

    /// deterministically ordered, unique table identities in the snapshot
    ///
    /// an empty set is valid for a snapshot of an empty catalog
    pub table_ids: BTreeSet<TableId>,
}

impl SnapshotPointer {
    /// validate and encode this snapshot pointer for A-WAL
    ///
    /// invalid in-memory metadata is rejected before it can direct recovery to
    /// an unsafe path or ambiguous snapshot
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate().map_err(|message| {
            Error::InvalidArgument(format!("invalid SnapshotPointer: {message}"))
        })?;

        Ok(self.to_proto().encode_to_vec())
    }

    /// decode and validate snapshot metadata read from A-WAL
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let proto = wal_proto::SnapshotPointer::decode(bytes).map_err(|error| {
            Error::CorruptData(format!(
                "failed to decode SnapshotPointer protobuf: {error}"
            ))
        })?;

        Self::from_proto(proto)
    }

    fn to_proto(&self) -> wal_proto::SnapshotPointer {
        wal_proto::SnapshotPointer {
            version: SNAPSHOT_POINTER_VERSION,
            snapshot_id: self.snapshot_id,
            snapshot_timestamp: Some(self.snapshot_timestamp.to_proto()),
            replay_from_lsn: self.replay_from_lsn.as_u64(),
            relative_path: self.relative_path.clone(),
            table_ids: self.table_ids.iter().map(TableId::to_proto).collect(),
        }
    }

    fn from_proto(proto: wal_proto::SnapshotPointer) -> Result<Self> {
        if proto.version != SNAPSHOT_POINTER_VERSION {
            return Err(Error::CorruptData(format!(
                "unsupported SnapshotPointer version {}; expected {}",
                proto.version, SNAPSHOT_POINTER_VERSION
            )));
        }

        let snapshot_timestamp = proto.snapshot_timestamp.ok_or_else(|| {
            Error::CorruptData("SnapshotPointer is missing its snapshot timestamp".to_string())
        })?;

        let mut table_ids = BTreeSet::new();

        for proto_table_id in proto.table_ids {
            let table_id = TableId::from_proto(proto_table_id);

            if table_id.0 == 0 {
                return Err(Error::CorruptData(
                    "SnapshotPointer contains reserved table ID 0".to_string(),
                ));
            }

            if !table_ids.insert(table_id) {
                return Err(Error::CorruptData(format!(
                    "SnapshotPointer contains duplicate table ID {}",
                    table_id.0
                )));
            }
        }

        let pointer = Self {
            snapshot_id: proto.snapshot_id,
            snapshot_timestamp: Timestamp::from_proto(snapshot_timestamp),
            replay_from_lsn: Lsn::new(proto.replay_from_lsn),
            relative_path: proto.relative_path,
            table_ids,
        };

        pointer.validate().map_err(|message| {
            Error::CorruptData(format!("invalid durable SnapshotPointer: {message}"))
        })?;

        Ok(pointer)
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if self.snapshot_id == 0 {
            return Err("snapshot ID 0 is reserved".to_string());
        }

        if self.snapshot_timestamp.0 == 0 {
            return Err("snapshot timestamp 0 is reserved".to_string());
        }

        validate_relative_snapshot_path(&self.relative_path)?;

        for table_id in &self.table_ids {
            if table_id.0 == 0 {
                return Err("SnapshotPointer contains reserved table ID 0".to_string());
            }
        }

        Ok(())
    }
}

/// validates the platform-independent relative path stored in a snapshot pointer
///
/// Snapshot paths use `/` separators regardless of the host platform. Absolute
/// paths, parent traversal, empty components, Windows separators, drive-style
/// prefixes, and NUL bytes are rejected before filesystem access occurs
fn validate_relative_snapshot_path(path: &str) -> std::result::Result<(), String> {
    if path.is_empty() {
        return Err("invalid relative snapshot path: path cannot be empty".to_string());
    }

    if path.starts_with('/') || path.starts_with('\\') {
        return Err("invalid relative snapshot path: absolute paths are forbidden".to_string());
    }

    if path.contains('\\') {
        return Err(
            "invalid relative snapshot path: backslash separators are forbidden".to_string(),
        );
    }

    if path.contains('\0') {
        return Err("invalid relative snapshot path: NUL bytes are forbidden".to_string());
    }

    for component in path.split('/') {
        if component.is_empty() {
            return Err("invalid relative snapshot path: empty path component".to_string());
        }

        if component == "." || component == ".." {
            return Err(format!(
                "invalid relative snapshot path: component {component:?} is forbidden"
            ));
        }

        if component.contains(':') {
            return Err(
                "invalid relative snapshot path: drive-style path components are forbidden"
                    .to_string(),
            );
        }
    }

    Ok(())
}

/// durable publication marker for completed database snapshot
///
/// `CheckpointMarker` is deliberately smaller than `snapshotPointer`. it
/// repeats only the recovery critical metadata needed to identify the snapshot
/// and establish the wal replay boundary
///
/// this codec validates one marker in isolation. this path remains responsible for:
///
/// - the snapshot file has been safely published
/// - the corresponding `SnapshotPointer` is durable
/// - the marker fields match that pointer exactly
/// - WAL retention advances only after the marker is durable
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointMarker {
    /// stable, nonzero identity of the snapshot being published
    pub snapshot_id: u64,

    /// highest MVCC timestamp represented by the published snapshot
    pub snapshot_timestamp: Timestamp,

    /// first logical WAL position not represented by the snapshot
    ///
    /// recovery must replay records at or afyer this postion. `Lsn::ZERO` is
    /// valid and means that the checkpoint does not permit skipping any WAL
    /// history
    pub replay_from_lsn: Lsn,
}

impl CheckpointMarker {
    /// validates and encodes this checkpoint marker for storage in A-WAL
    ///
    /// invalid in memory metadata is classified as caller input rather than
    /// durable corruption because it has not yet crossed the WAL boundary
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate().map_err(|message| {
            Error::InvalidArgument(format!("invalid CheckpointMarker: {message}"))
        })?;

        Ok(self.to_proto().encode_to_vec())
    }

    /// decodes and validates checkpoint metadata read from A-WAL
    ///
    /// malformed protobuf bytes and invalid durable field values are reported
    /// as corruption because recovery obtained them from persisted storage
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let proto = wal_proto::CheckpointMarker::decode(bytes).map_err(|error| {
            Error::CorruptData(format!(
                "failed to decode CheckpointMarker protobuf: {error}"
            ))
        })?;

        Self::from_proto(proto)
    }

    fn to_proto(&self) -> wal_proto::CheckpointMarker {
        wal_proto::CheckpointMarker {
            version: CHECKPOINT_MARKER_VERSION,
            snapshot_id: self.snapshot_id,
            snapshot_timestamp: Some(self.snapshot_timestamp.to_proto()),
            replay_from_lsn: self.replay_from_lsn.as_u64(),
        }
    }

    fn from_proto(proto: wal_proto::CheckpointMarker) -> Result<Self> {
        if proto.version != CHECKPOINT_MARKER_VERSION {
            return Err(Error::CorruptData(format!(
                "unsupported CheckpointMarker version {}; expected {}",
                proto.version, CHECKPOINT_MARKER_VERSION
            )));
        }

        let snapshot_timestamp = proto.snapshot_timestamp.ok_or_else(|| {
            Error::CorruptData("CheckpointMarker is missing its snapshot timestamp".to_string())
        })?;

        let marker = Self {
            snapshot_id: proto.snapshot_id,
            snapshot_timestamp: Timestamp::from_proto(snapshot_timestamp),
            replay_from_lsn: Lsn::new(proto.replay_from_lsn),
        };

        marker.validate().map_err(|message| {
            Error::CorruptData(format!("invalid durable CheckpointMarker: {message}"))
        })?;

        Ok(marker)
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if self.snapshot_id == 0 {
            return Err("snapshot ID 0 is reserved".to_string());
        }

        if self.snapshot_timestamp.0 == 0 {
            return Err("snapshot timestamp 0 is reserved".to_string());
        }

        // Every u64 value is representable as an A-WAL LSN. In particular,
        // LSN zero is an intentional replay boundary rather than missing data.
        Ok(())
    }
}
