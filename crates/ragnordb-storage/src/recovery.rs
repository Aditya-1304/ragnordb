//! semantic decoding and streaming boundary for RagnorDB recovery
//!
//! A-WAL validates physical framing, checksums, segment ordering, and the
//! recoverable byte prefix. This module begins the database owned portion of
//! recovery by classifying each logical WAL record and decoding recognized
//! RagnorDB payloads into validated semantic values
//!
//! decoding is separate from catalog and MVCC publication
//! Later recovery slices can validate complete history before exposing any
//! reconstructed database state

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use ragnordb_catalog::{Catalog, MemoryCatalog, TableSchema};
use ragnordb_common::{
    Error, Result,
    catalog_codec::{DataType, TableDefinition},
    codec::Value,
    command_codec::CatalogOperation,
    encoding::decode_row,
    ids::{TableId, Timestamp, TxnId},
    proto::snapshot as snapshot_proto,
};
use wal::{
    error::WalError,
    io::{directory::SegmentDirectory, segment_file::SegmentFile},
    lsn::Lsn,
    types::RecordType,
    wal::{WalHandle, iterator::WalIterator},
};

use crate::{
    checkpoint::load_snapshot_file_bounded,
    key::{decode_primary_key, decode_row_key, encode_row_key, make_row_key},
    mvcc::{InMemoryMvcc, Mutation, MvccStorage},
    wal::{
        CatalogUpdate, CheckpointMarker, RagnorDbWalRecordType, SingleNodeTxnCommit,
        SnapshotPointer, WalMutation,
    },
};

/// validated database payload obtained from one logical A-WAL record
///
/// every variant has passed its version, protobuf, identity, timestamp, and
/// operation-specific validation before it can reach state-machine replay
#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryPayload {
    /// points to a candidate durable database snapshot
    SnapshotPointer(SnapshotPointer),

    /// contains one complete committed single node transaction
    SingleNodeTxnCommit(SingleNodeTxnCommit),

    /// contains one durable catalog state transition
    CatalogUpdate(CatalogUpdate),

    /// publishes a previously written snapshot pointer
    CheckpointMarker(CheckpointMarker),
}

/// one validated RagnorDB record together with its physical WAL position
///
/// retaining the source LSN allows later replay stages to produce precise
/// corruption diagnostics and verify ordering dependencies between catalog and
/// transaction records
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedRecoveryRecord {
    /// starting logical WAL position of the physical record
    pub lsn: Lsn,

    /// validated semantic payload carried by the record
    pub payload: RecoveryPayload,
}

/// durable metadata candidate formed by an exactly matched snapshot pointer and
/// checkpoint marker
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryCheckpointCandidate {
    /// WAL location of the durable snapshot pointer
    pub pointer_lsn: Lsn,

    /// WAL location of the marker that published the pointer
    pub marker_lsn: Lsn,

    /// complete snapshot metadata repeated and confirmed by the marker
    pub pointer: SnapshotPointer,
}

/// validated snapshot state and replay boundary selected for startup recovery
///
/// construction proves that the referenced file is readable, checksummed,
/// semantically valid, and exactly matches the selected WAL metadata
#[must_use = "loaded checkpoint state must seed WAL suffix replay"]
#[derive(Debug)]
pub struct LoadedRecoveryCheckpoint {
    /// stable identity shared by the file, pointer, and marker
    pub snapshot_id: u64,

    /// first WAL position not represented by the restored snapshot
    pub replay_from_lsn: Lsn,

    /// private catalog, MVCC, and allocator state reconstructed from the file
    pub state: RecoveryState,
}

/// incremental selector for published RagnorDB checkpoint metadata
///
/// pointers remain pending until a later marker repeats the same snapshot ID,
/// snapshot timestamp, and replay boundary. A pointer without a marker is an
/// orphan and cannot replace an earlier published checkpoint candidate
#[derive(Debug, Default)]
pub struct RecoveryCheckpointSelector {
    pending_pointers: BTreeMap<u64, PendingSnapshotPointer>,
    selected: Option<RecoveryCheckpointCandidate>,
    last_observed_lsn: Option<Lsn>,
}

#[derive(Debug, Clone)]
struct PendingSnapshotPointer {
    lsn: Lsn,
    pointer: SnapshotPointer,
}

impl RecoveryCheckpointSelector {
    /// an empty selector.
    pub fn new() -> Self {
        Self::default()
    }

    /// observe one decoded record in physical WAL order
    ///
    /// Non checkpoint database records participate in LSN-order validation but
    /// otherwise do not affect snapshot selection
    pub fn observe_record(&mut self, record: &DecodedRecoveryRecord) -> Result<()> {
        self.validate_record_order(record.lsn)?;

        match &record.payload {
            RecoveryPayload::SnapshotPointer(pointer) => self.observe_pointer(record.lsn, pointer),

            RecoveryPayload::CheckpointMarker(marker) => self.observe_marker(record.lsn, marker),

            RecoveryPayload::CatalogUpdate(_) | RecoveryPayload::SingleNodeTxnCommit(_) => Ok(()),
        }
    }

    /// return the newest exactly matched checkpoint candidate
    pub fn selected(&self) -> Option<&RecoveryCheckpointCandidate> {
        self.selected.as_ref()
    }

    /// consume the selector and return its selected candidate
    pub fn into_selected(self) -> Option<RecoveryCheckpointCandidate> {
        self.selected
    }

    fn validate_record_order(&mut self, lsn: Lsn) -> Result<()> {
        if let Some(previous_lsn) = self.last_observed_lsn
            && lsn <= previous_lsn
        {
            return Err(Error::CorruptData(format!(
                "recovery record at WAL LSN {} does not follow previously \
                 observed WAL LSN {}",
                lsn.as_u64(),
                previous_lsn.as_u64()
            )));
        }

        self.last_observed_lsn = Some(lsn);
        Ok(())
    }

    fn observe_pointer(&mut self, lsn: Lsn, pointer: &SnapshotPointer) -> Result<()> {
        if pointer.replay_from_lsn > lsn {
            return Err(Error::CorruptData(format!(
                "SnapshotPointer for snapshot {} at WAL LSN {} claims future \
                 replay boundary {}",
                pointer.snapshot_id,
                lsn.as_u64(),
                pointer.replay_from_lsn.as_u64()
            )));
        }

        if let Some(existing) = self.pending_pointers.get(&pointer.snapshot_id)
            && existing.pointer != *pointer
        {
            return Err(Error::CorruptData(format!(
                "snapshot ID {} has conflicting SnapshotPointer records at \
                 WAL LSNs {} and {}",
                pointer.snapshot_id,
                existing.lsn.as_u64(),
                lsn.as_u64()
            )));
        }

        // repeating identical pointer metadata is harmless. Retaining the most
        // recent preceding pointer gives the selected candidate the closest
        // physical publication context
        self.pending_pointers.insert(
            pointer.snapshot_id,
            PendingSnapshotPointer {
                lsn,
                pointer: pointer.clone(),
            },
        );

        Ok(())
    }

    fn observe_marker(&mut self, lsn: Lsn, marker: &CheckpointMarker) -> Result<()> {
        let pending = self
            .pending_pointers
            .get(&marker.snapshot_id)
            .ok_or_else(|| {
                Error::CorruptData(format!(
                    "CheckpointMarker for snapshot {} at WAL LSN {} has no \
                     preceding SnapshotPointer",
                    marker.snapshot_id,
                    lsn.as_u64()
                ))
            })?;

        if pending.pointer.snapshot_timestamp != marker.snapshot_timestamp
            || pending.pointer.replay_from_lsn != marker.replay_from_lsn
        {
            return Err(Error::CorruptData(format!(
                "CheckpointMarker for snapshot {} at WAL LSN {} does not match \
                 its SnapshotPointer timestamp or replay boundary",
                marker.snapshot_id,
                lsn.as_u64()
            )));
        }

        let pending = self
            .pending_pointers
            .remove(&marker.snapshot_id)
            .expect("pending pointer was validated above");

        self.selected = Some(RecoveryCheckpointCandidate {
            pointer_lsn: pending.lsn,
            marker_lsn: lsn,
            pointer: pending.pointer,
        });

        Ok(())
    }
}

/// maximum durable values observed while reconstructing database state
///
/// zero means no value from that allocator namespace has been observed. These
/// values are floors, not the next values to allocate. Allocator restoration
/// must ensure that every subsequent allocation is strictly greater
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryHighWaterMarks {
    /// largest durable transaction identity
    pub max_transaction_id: TxnId,

    /// largest start, commit, catalog update, or snapshot timestamp
    pub max_timestamp: Timestamp,

    /// largest table identity found in catalog or snapshot metadata
    pub max_table_id: TableId,

    /// largest durable snapshot identity found in a pointer or marker
    pub max_snapshot_id: u64,
}

impl Default for RecoveryHighWaterMarks {
    fn default() -> Self {
        Self {
            max_transaction_id: TxnId(0),
            max_timestamp: Timestamp(0),
            max_table_id: TableId(0),
            max_snapshot_id: 0,
        }
    }
}

impl RecoveryHighWaterMarks {
    /// convert durable maxima into checked next allocation values
    ///
    /// The complete result is calculated before it is returned. If any
    /// namespace is exhausted, recovery fails without publishing a partial set
    /// of allocator floors
    pub fn checked_allocator_floors(&self) -> Result<RecoveryAllocatorFloors> {
        let next_transaction_id =
            checked_recovery_successor("transaction ID", self.max_transaction_id.0)?;

        let next_timestamp = checked_recovery_successor("timestamp", self.max_timestamp.0)?;

        let next_table_id = checked_recovery_successor("table ID", self.max_table_id.0)?;

        let next_snapshot_id = checked_recovery_successor("snapshot ID", self.max_snapshot_id)?;

        Ok(RecoveryAllocatorFloors {
            next_transaction_id: TxnId(next_transaction_id),
            next_timestamp: Timestamp(next_timestamp),
            next_table_id: TableId(next_table_id),
            next_snapshot_id,
        })
    }

    fn observe_catalog_update(&mut self, update: &CatalogUpdate) {
        self.observe_table_id(update.table_id);
        self.observe_timestamp(update.update_timestamp);
    }

    fn observe_transaction_commit(&mut self, commit: &SingleNodeTxnCommit) {
        self.max_transaction_id = TxnId(self.max_transaction_id.0.max(commit.txn_id.0));

        self.observe_table_id(commit.table_id);
        self.observe_timestamp(commit.start_timestamp);
        self.observe_timestamp(commit.commit_timestamp);
    }

    fn observe_snapshot_pointer(&mut self, pointer: &SnapshotPointer) {
        self.max_snapshot_id = self.max_snapshot_id.max(pointer.snapshot_id);

        self.observe_timestamp(pointer.snapshot_timestamp);

        for table_id in &pointer.table_ids {
            self.observe_table_id(*table_id);
        }
    }

    fn observe_checkpoint_marker(&mut self, marker: &CheckpointMarker) {
        self.max_snapshot_id = self.max_snapshot_id.max(marker.snapshot_id);

        self.observe_timestamp(marker.snapshot_timestamp);
    }

    fn observe_timestamp(&mut self, timestamp: Timestamp) {
        self.max_timestamp = Timestamp(self.max_timestamp.0.max(timestamp.0));
    }

    fn observe_table_id(&mut self, table_id: TableId) {
        self.max_table_id = TableId(self.max_table_id.0.max(table_id.0));
    }
}

/// calculate the first reusable value after one durable allocator maximum
fn checked_recovery_successor(allocator_name: &str, maximum: u64) -> Result<u64> {
    maximum.checked_add(1).ok_or_else(|| Error::RecoveryFailed {
        reason: format!(
            "{allocator_name} allocator is exhausted at recovered \
                 high-water mark {maximum}"
        ),
    })
}

/// prevalidated first values available for allocation after recovery
///
/// every field is exactly one greater than the corresponding durable maximum
/// Constructing this value performs all overflow checks before any live
/// allocator is initialized
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryAllocatorFloors {
    /// first transaction identity available after recovery.
    pub next_transaction_id: TxnId,

    /// first timestamp available after recovery
    pub next_timestamp: Timestamp,

    /// first table identity available after recovery
    pub next_table_id: TableId,

    /// first snapshot identity available after recovery
    pub next_snapshot_id: u64,
}

/// private catalog and MVCC state reconstructed during recovery
///
/// this state is deliberately independent from the running executor. Startup
/// must replay and validate the complete selected WAL suffix before transferring
/// the recovered state and allocator floors to live database components
///
/// If any later record is invalid, the caller discards this entire value and no
/// partially reconstructed state becomes visible to sessions
#[derive(Debug, Default)]
pub struct RecoveryState {
    catalog: MemoryCatalog,
    mvcc_by_table: BTreeMap<TableId, InMemoryMvcc>,
    high_water_marks: RecoveryHighWaterMarks,

    /// Exact committed identity observed in the selected WAL replay stream.
    ///
    /// Storing the validated semantic record avoids relying on a digest with
    /// collision semantics. Recovery streams are bounded by retained WAL size,
    /// and the map is discarded after startup publishes reconstructed state.
    transactions_by_id: BTreeMap<TxnId, SingleNodeTxnCommit>,

    /// Single-node commit timestamps are globally unique because one allocator
    /// serializes their creation. Reuse by another transaction is corruption.
    transactions_by_commit_timestamp: BTreeMap<Timestamp, TxnId>,
}

impl RecoveryState {
    /// Construct an empty recovery staging state.
    pub fn new() -> Self {
        Self::default()
    }

    /// reconstruct private recovery state from one validated snapshot body
    ///
    /// Catalog definitions and MVCC maps are built independently from the live
    /// executor. The complete value is returned only after every table and all
    /// allocator high-water marks have been restored successfully
    pub fn from_snapshot(snapshot: &snapshot_proto::DatabaseSnapshot) -> Result<Self> {
        let high_water = snapshot.high_water_marks.as_ref().ok_or_else(|| {
            Error::CorruptData("snapshot allocator high-water marks are missing".to_string())
        })?;
        let max_transaction_id = high_water
            .max_transaction_id
            .as_ref()
            .cloned()
            .map(TxnId::from_proto)
            .ok_or_else(|| {
                Error::CorruptData("snapshot maximum transaction ID is missing".to_string())
            })?;
        let max_timestamp = high_water
            .max_timestamp
            .as_ref()
            .cloned()
            .map(Timestamp::from_proto)
            .ok_or_else(|| {
                Error::CorruptData("snapshot maximum timestamp is missing".to_string())
            })?;
        let max_table_id = high_water
            .max_table_id
            .as_ref()
            .cloned()
            .map(TableId::from_proto)
            .ok_or_else(|| {
                Error::CorruptData("snapshot maximum table ID is missing".to_string())
            })?;
        let mut catalog = MemoryCatalog::new();
        let mut mvcc_by_table = BTreeMap::new();

        for table in &snapshot.tables {
            let definition = table
                .definition
                .as_ref()
                .cloned()
                .ok_or_else(|| {
                    Error::CorruptData(
                        "snapshot table definition is missing during restore".to_string(),
                    )
                })
                .and_then(|definition| {
                    TableDefinition::from_proto(definition).map_err(|message| {
                        Error::CorruptData(format!(
                            "snapshot table definition is invalid during restore: {message}"
                        ))
                    })
                })?;
            let table_id = TableId(definition.table_id);

            catalog.install_definition(definition).map_err(|source| {
                Error::CorruptData(format!(
                    "failed to install snapshot definition for table {}: {source}",
                    table_id.0
                ))
            })?;

            let restored = InMemoryMvcc::from_snapshot_table(table_id, table)?;

            if mvcc_by_table.insert(table_id, restored.storage).is_some() {
                return Err(Error::CorruptData(format!(
                    "snapshot contains duplicate restored table {}",
                    table_id.0
                )));
            }
        }

        Ok(Self {
            catalog,
            mvcc_by_table,
            high_water_marks: RecoveryHighWaterMarks {
                max_transaction_id,
                max_timestamp,
                max_table_id,
                max_snapshot_id: high_water.max_snapshot_id,
            },
            transactions_by_id: BTreeMap::new(),
            transactions_by_commit_timestamp: BTreeMap::new(),
        })
    }

    /// Return the catalog reconstructed from durable metadata operations.
    pub fn catalog(&self) -> &MemoryCatalog {
        &self.catalog
    }

    /// return the reconstructed MVCC state for one table
    pub fn table_storage(&self, table_id: TableId) -> Option<&InMemoryMvcc> {
        self.mvcc_by_table.get(&table_id)
    }

    /// return allocator high-water marks observed during successful replay
    ///
    /// the returned value is a copy so callers cannot alter recovery state
    pub fn high_water_marks(&self) -> RecoveryHighWaterMarks {
        self.high_water_marks
    }

    /// apply one decoded record to private recovery state
    ///
    /// high water marks advance only after the corresponding semantic operation
    /// succeeds. A rejected catalog or transaction record therefore cannot
    /// influence allocator restoration
    pub fn apply_record(&mut self, record: &DecodedRecoveryRecord) -> Result<()> {
        match &record.payload {
            RecoveryPayload::CatalogUpdate(update) => {
                self.apply_catalog_update(record.lsn, update)?;
                self.high_water_marks.observe_catalog_update(update);
                Ok(())
            }

            RecoveryPayload::SingleNodeTxnCommit(commit) => {
                self.apply_transaction_commit(record.lsn, commit)?;
                self.high_water_marks.observe_transaction_commit(commit);
                Ok(())
            }

            RecoveryPayload::SnapshotPointer(pointer) => {
                // snapshot file selection and validation are handled by the
                // replay-planning slice. The metadata still contributes durable
                // allocator floors even when its pointer is later determined to
                // be orphaned and cannot be selected for state restoration
                self.high_water_marks.observe_snapshot_pointer(pointer);
                Ok(())
            }

            RecoveryPayload::CheckpointMarker(marker) => {
                self.high_water_marks.observe_checkpoint_marker(marker);
                Ok(())
            }
        }
    }

    fn apply_catalog_update(&mut self, lsn: Lsn, update: &CatalogUpdate) -> Result<()> {
        let definition = match &update.command.operation {
            CatalogOperation::CreateTable(operation) => operation.table_def.clone(),
        };

        let installed = self
            .catalog
            .install_definition(definition)
            .map_err(|source| {
                recovery_corruption(lsn, "failed to apply durable CatalogUpdate", source)
            })?;

        if installed.id != update.table_id {
            return Err(Error::CorruptData(format!(
                "CatalogUpdate at WAL LSN {} installed table {}, \
                 but the durable update identifies table {}",
                lsn.as_u64(),
                installed.id.0,
                update.table_id.0
            )));
        }

        // identical catalog metadata must preserve any MVCC state
        // already reconstructed for the table.
        self.mvcc_by_table.entry(update.table_id).or_default();

        Ok(())
    }

    fn apply_transaction_commit(&mut self, lsn: Lsn, commit: &SingleNodeTxnCommit) -> Result<()> {
        if let Some(previous) = self.transactions_by_id.get(&commit.txn_id) {
            if previous == commit {
                // Exact duplicate replay is intentionally idempotent. Returning
                // before MVCC application also avoids depending on lower-layer
                // duplicate-detection details.
                return Ok(());
            }

            return Err(Error::CorruptData(format!(
                "SingleNodeTxnCommit at WAL LSN {} reuses transaction ID {} \
                 for a different logical commit",
                lsn.as_u64(),
                commit.txn_id.0,
            )));
        }

        if let Some(previous_txn_id) = self
            .transactions_by_commit_timestamp
            .get(&commit.commit_timestamp)
        {
            return Err(Error::CorruptData(format!(
                "SingleNodeTxnCommit at WAL LSN {} reuses commit timestamp {} \
                 previously owned by transaction {} for transaction {}",
                lsn.as_u64(),
                commit.commit_timestamp.0,
                previous_txn_id.0,
                commit.txn_id.0,
            )));
        }

        let schema = self.catalog.table_by_id(commit.table_id).ok_or_else(|| {
            Error::CorruptData(format!(
                "SingleNodeTxnCommit at WAL LSN {} references table {} \
                 before its catalog definition appears in durable history",
                lsn.as_u64(),
                commit.table_id.0
            ))
        })?;

        validate_commit_against_schema(lsn, schema.as_ref(), commit)?;

        let storage = self
            .mvcc_by_table
            .get_mut(&commit.table_id)
            .ok_or_else(|| {
                Error::CorruptData(format!(
                    "recovery catalog contains table {}, but its private \
                     MVCC state is missing at WAL LSN {}",
                    commit.table_id.0,
                    lsn.as_u64()
                ))
            })?;

        let mutations = commit
            .writes
            .iter()
            .map(|(key, mutation)| {
                let mutation = match mutation {
                    WalMutation::Put(row) => Mutation::Put(row.clone()),

                    WalMutation::Delete => Mutation::Delete,
                };

                (key.clone(), mutation)
            })
            .collect::<BTreeMap<_, _>>();

        storage
            .commit_batch(
                commit.txn_id,
                commit.start_timestamp,
                commit.commit_timestamp,
                &mutations,
            )
            .map_err(|source| {
                recovery_corruption(lsn, "failed to apply durable SingleNodeTxnCommit", source)
            })?;

        self.transactions_by_id
            .insert(commit.txn_id, commit.clone());
        self.transactions_by_commit_timestamp
            .insert(commit.commit_timestamp, commit.txn_id);

        Ok(())
    }

    /// consume successfully reconstructed state for live runtime publication
    ///
    /// this is the only ownership transfer from private recovery state. Startup
    /// calls it only after the complete selected WAL stream has been decoded
    /// validated, and applied successfully
    pub fn into_parts(
        self,
    ) -> (
        MemoryCatalog,
        BTreeMap<TableId, InMemoryMvcc>,
        RecoveryHighWaterMarks,
    ) {
        (self.catalog, self.mvcc_by_table, self.high_water_marks)
    }
}

/// Validate durable transaction bytes against the exact catalog schema that
/// gives their row and primary-key encodings semantic meaning.
///
/// The WAL codec proves that keys and rows are canonically encoded. Recovery
/// must additionally prove that those values could have been produced by the
/// live SQL path for the referenced schema revision.
fn validate_commit_against_schema(
    lsn: Lsn,
    schema: &TableSchema,
    commit: &SingleNodeTxnCommit,
) -> Result<()> {
    let corrupt = |message: String| {
        Error::CorruptData(format!(
            "SingleNodeTxnCommit at WAL LSN {} violates table {} schema: {message}",
            lsn.as_u64(),
            schema.id.0,
        ))
    };

    if commit.schema_version != schema.schema_version {
        return Err(corrupt(format!(
            "commit schema version {} does not match catalog schema version {}",
            commit.schema_version, schema.schema_version,
        )));
    }

    let primary_key_columns = schema
        .primary_key_columns()
        .map_err(|source| corrupt(source.to_string()))?;

    for (encoded_key, mutation) in &commit.writes {
        let row_key = decode_row_key(encoded_key).map_err(|source| corrupt(source.to_string()))?;
        let key_values = decode_primary_key(&row_key.primary_key_bytes)
            .map_err(|source| corrupt(source.to_string()))?;

        if key_values.len() != primary_key_columns.len() {
            return Err(corrupt(format!(
                "primary key has {} values, but schema requires {}",
                key_values.len(),
                primary_key_columns.len(),
            )));
        }

        for (value, column) in key_values.iter().zip(&primary_key_columns) {
            if !value_matches_type(value, column.ty, false) {
                return Err(corrupt(format!(
                    "primary-key column {} expects {:?}, but key contains {}",
                    column.name,
                    column.ty,
                    value_kind(value),
                )));
            }
        }

        let WalMutation::Put(encoded_row) = mutation else {
            continue;
        };

        let row = decode_row(encoded_row).map_err(|source| corrupt(source.to_string()))?;

        if row.values.len() != schema.columns.len() {
            return Err(corrupt(format!(
                "row has {} values, but schema requires {}",
                row.values.len(),
                schema.columns.len(),
            )));
        }

        for (value, column) in row.values.iter().zip(&schema.columns) {
            if !value_matches_type(value, column.ty, column.nullable) {
                return Err(corrupt(format!(
                    "column {} expects {}{:?}, but row contains {}",
                    column.name,
                    if column.nullable { "nullable " } else { "" },
                    column.ty,
                    value_kind(value),
                )));
            }
        }

        let row_primary_key = schema
            .primary_key_column_ids
            .iter()
            .map(|column_id| {
                let ordinal = schema.column_ordinal(*column_id).ok_or_else(|| {
                    corrupt(format!(
                        "primary-key column ID {} has no row-layout ordinal",
                        column_id.0,
                    ))
                })?;

                Ok(row.values[ordinal].clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let expected_key = make_row_key(schema.id, &row_primary_key)
            .and_then(|key| encode_row_key(&key))
            .map_err(|source| corrupt(source.to_string()))?;

        if expected_key != *encoded_key {
            return Err(corrupt(
                "physical row key does not match the row's primary-key values".to_string(),
            ));
        }
    }

    Ok(())
}

fn value_matches_type(value: &Value, expected: DataType, nullable: bool) -> bool {
    match value {
        Value::Null => nullable,
        Value::Int(_) => expected == DataType::Int,
        Value::Text(_) => expected == DataType::Text,
        Value::Bool(_) => expected == DataType::Bool,
    }
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Int(_) => "INT",
        Value::Text(_) => "TEXT",
        Value::Bool(_) => "BOOL",
    }
}

/// load the snapshot referenced by one selected pointer/marker pair
///
/// recovery must not use the candidate replay boundary until this function has
/// validated the complete file and matched its identity, timestamp, frontier,
/// and table set against the durable pointer
pub fn load_recovery_checkpoint(
    data_dir: impl AsRef<Path>,
    candidate: &RecoveryCheckpointCandidate,
) -> Result<LoadedRecoveryCheckpoint> {
    candidate.pointer.encode().map_err(|source| {
        Error::CorruptData(format!(
            "selected SnapshotPointer is invalid during checkpoint loading: {source}"
        ))
    })?;

    if candidate.pointer.replay_from_lsn > candidate.pointer_lsn {
        return Err(Error::CorruptData(format!(
            "selected SnapshotPointer for snapshot {} claims future replay \
             boundary {} beyond pointer LSN {}",
            candidate.pointer.snapshot_id,
            candidate.pointer.replay_from_lsn.as_u64(),
            candidate.pointer_lsn.as_u64()
        )));
    }

    let snapshot_path = data_dir.as_ref().join(&candidate.pointer.relative_path);
    let snapshot = load_snapshot_file_bounded(
        &snapshot_path,
        candidate.pointer.file_length,
        candidate.pointer.file_checksum_crc32c,
        candidate.pointer.snapshot_format_version,
    )?;
    let snapshot_timestamp = snapshot
        .snapshot_timestamp
        .as_ref()
        .cloned()
        .map(Timestamp::from_proto)
        .ok_or_else(|| Error::CorruptData("selected snapshot timestamp is missing".to_string()))?;
    let snapshot_table_ids = snapshot
        .tables
        .iter()
        .map(|table| {
            table
                .definition
                .as_ref()
                .map(|definition| TableId(definition.table_id))
                .ok_or_else(|| {
                    Error::CorruptData("selected snapshot table definition is missing".to_string())
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;

    if snapshot.snapshot_id != candidate.pointer.snapshot_id {
        return Err(Error::CorruptData(format!(
            "selected SnapshotPointer identifies snapshot ID {}, but file {} \
             contains snapshot ID {}",
            candidate.pointer.snapshot_id,
            snapshot_path.display(),
            snapshot.snapshot_id
        )));
    }

    if snapshot_timestamp != candidate.pointer.snapshot_timestamp {
        return Err(Error::CorruptData(format!(
            "selected snapshot {} timestamp {} does not match pointer timestamp {}",
            snapshot.snapshot_id, snapshot_timestamp.0, candidate.pointer.snapshot_timestamp.0
        )));
    }

    if Lsn::new(snapshot.replay_from_lsn) != candidate.pointer.replay_from_lsn {
        return Err(Error::CorruptData(format!(
            "selected snapshot {} replay boundary {} does not match pointer boundary {}",
            snapshot.snapshot_id,
            snapshot.replay_from_lsn,
            candidate.pointer.replay_from_lsn.as_u64()
        )));
    }

    if snapshot_table_ids != candidate.pointer.table_ids {
        return Err(Error::CorruptData(format!(
            "selected snapshot {} table identities do not match its SnapshotPointer",
            snapshot.snapshot_id
        )));
    }

    let state = RecoveryState::from_snapshot(&snapshot)?;

    Ok(LoadedRecoveryCheckpoint {
        snapshot_id: snapshot.snapshot_id,
        replay_from_lsn: candidate.pointer.replay_from_lsn,
        state,
    })
}

/// lazy semantic reader over an immutable A-WAL iterator snapshot
///
/// the stream preserves the exact physical order supplied by A-WAL. internal
/// WAL records are consumed but not returned because they are not ragnordb
/// state machine operation
///
/// records are decoded one at a time so recovery does not need to retain
/// the complete WAL in memory
pub struct RecoveryRecordStream<F>
where
    F: SegmentFile,
{
    iterator: WalIterator<F>,
}

impl<F> RecoveryRecordStream<F>
where
    F: SegmentFile,
{
    /// decode and return the next RagnorDB record in physical WAL order
    ///
    /// `Ok(None)` means the immutable iterator snapshot has been completely
    /// consumed. A physical iterator error stops recovery as `RecoveryFailed`;
    /// a malformed database payload stops recovery as `CorruptData`
    pub fn next_record(&mut self) -> Result<Option<DecodedRecoveryRecord>> {
        loop {
            let attempted_lsn = self.iterator.current_lsn();

            let physical_record = self.iterator.next().map_err(|source| {
                wal_recovery_failure(
                    "failed while reading the WAL recovery stream",
                    attempted_lsn,
                    source,
                )
            })?;

            let Some(physical_record) = physical_record else {
                return Ok(None);
            };

            let decoded = decode_recovery_record(
                physical_record.lsn,
                physical_record.record_type,
                &physical_record.payload,
            )?;

            if let Some(decoded) = decoded {
                return Ok(Some(decoded));
            }

            // WAL internal records are already physically validated and are
            // deliberately consumed without becoming database operations
        }
    }

    /// return the physical LSN at which the next iterator read will begin
    ///
    /// After `next_record` returns `None`, this is the end of the immutable WAL
    /// snapshot represented by this stream
    pub fn current_lsn(&self) -> Lsn {
        self.iterator.current_lsn()
    }
}

/// replay one complete immutable WAL stream into private recovery state
///
/// the state is returned only after the stream reaches its validated end. If
/// physical iteration, semantic decoding, catalog installation, dependency
/// validation, or MVCC application fails, the partially reconstructed state is
/// dropped and can never be published
pub fn replay_recovery_stream<F>(stream: RecoveryRecordStream<F>) -> Result<RecoveryState>
where
    F: SegmentFile,
{
    replay_recovery_stream_from_state(stream, RecoveryState::new())
}

/// replay one complete immutable WAL suffix over private restored state
///
/// seed comes from a validated checkpoint. It remains private and
/// is dropped together with every suffix mutation if decoding or application
/// fails before the stream reaches its validated end
pub fn replay_recovery_stream_from_state<F>(
    mut stream: RecoveryRecordStream<F>,
    mut state: RecoveryState,
) -> Result<RecoveryState>
where
    F: SegmentFile,
{
    while let Some(record) = stream.next_record()? {
        state.apply_record(&record)?;
    }

    Ok(state)
}

/// scan one complete immutable WAL stream and select its newest published
/// checkpoint metadata candidate
///
/// The stream is consumed without applying catalog or MVCC state. Startup opens
/// a second stream at the selected boundary only after the referenced snapshot
/// file has been independently validated and loaded.
///
/// When no matched pair exists, this returns `Ok(None)` and startup must replay
/// from `Lsn::ZERO`
pub fn select_recovery_checkpoint<F>(
    mut stream: RecoveryRecordStream<F>,
) -> Result<Option<RecoveryCheckpointCandidate>>
where
    F: SegmentFile,
{
    let mut selector = RecoveryCheckpointSelector::new();

    while let Some(record) = stream.next_record()? {
        selector.observe_record(&record)?;
    }

    Ok(selector.into_selected())
}

/// open a lazy recovery stream at an exact WAL record boundary
///
/// The caller supplies the replay boundary selected from snapshot/checkpoint
/// metadata. WAL validates that the LSN is available and points to a complete
/// record boundary. This function never substitutes LSN zero when the supplied
/// boundary is invalid
pub fn scan_recovery_records<D, C>(
    wal: &WalHandle<D, C>,
    replay_from: Lsn,
) -> Result<RecoveryRecordStream<D::File>>
where
    D: SegmentDirectory + Clone,
{
    let iterator = wal.iter_from(replay_from).map_err(|source| {
        wal_recovery_failure(
            "failed to open the WAL recovery stream",
            replay_from,
            source,
        )
    })?;

    Ok(RecoveryRecordStream { iterator })
}

/// classify and decode one record returned by A-WAL iteration
///
/// A-WAL internal records return `Ok(None)` because their semantics are owned
/// and already validated by A-WAL. Recognized RagnorDB user records return a
/// typed payload. Unknown user records and malformed payloads fail closed as
/// durable corruption
///
/// This function performs no catalog or MVCC mutation. Recovery can therefore
/// decode and validate history before publishing reconstructed state
pub fn decode_recovery_record(
    lsn: Lsn,
    record_type: RecordType,
    bytes: &[u8],
) -> Result<Option<DecodedRecoveryRecord>> {
    let logical_type = RagnorDbWalRecordType::classify(record_type).map_err(|source| {
        recovery_corruption(lsn, "failed to classify RagnorDB WAL record", source)
    })?;

    let Some(logical_type) = logical_type else {
        // Internal A-WAL records belong to the physical log implementation and
        // must never be interpreted as RagnorDB state-machine operations.
        return Ok(None);
    };

    let payload = match logical_type {
        RagnorDbWalRecordType::SnapshotPointer => {
            SnapshotPointer::decode(bytes).map(RecoveryPayload::SnapshotPointer)
        }

        RagnorDbWalRecordType::SingleNodeTxnCommit => {
            SingleNodeTxnCommit::decode(bytes).map(RecoveryPayload::SingleNodeTxnCommit)
        }

        RagnorDbWalRecordType::CatalogUpdate => {
            CatalogUpdate::decode(bytes).map(RecoveryPayload::CatalogUpdate)
        }

        RagnorDbWalRecordType::CheckpointMarker => {
            CheckpointMarker::decode(bytes).map(RecoveryPayload::CheckpointMarker)
        }
    }
    .map_err(|source| {
        recovery_corruption(lsn, &format!("failed to decode {logical_type:?}"), source)
    })?;

    Ok(Some(DecodedRecoveryRecord { lsn, payload }))
}

/// attach the physical WAL position and recovery operation to a semantic
/// decoding failure
///
/// Recovery errors must identify the exact durable record that prevented
/// startup. The original error is retained in the message so codec-specific
/// validation details remain available to operators
fn recovery_corruption(lsn: Lsn, operation: &str, source: Error) -> Error {
    Error::CorruptData(format!("{operation} at WAL LSN {}: {source}", lsn.as_u64()))
}

/// convert an WAL iterator failure into a canonical startup recovery error
///
/// physical WAL errors are kept separate from invalid RagnorDB payloads so
/// startup diagnostics accurately identify which recovery boundary failed
fn wal_recovery_failure(operation: &str, lsn: Lsn, source: WalError) -> Error {
    Error::RecoveryFailed {
        reason: format!("{operation} at WAL LSN {}: {source}", lsn.as_u64()),
    }
}
