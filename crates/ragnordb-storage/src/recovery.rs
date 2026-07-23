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

use std::collections::BTreeMap;

use ragnordb_catalog::{Catalog, MemoryCatalog};
use ragnordb_common::{Error, Result, command_codec::CatalogOperation, ids::TableId};
use wal::{
    error::WalError,
    io::{directory::SegmentDirectory, segment_file::SegmentFile},
    lsn::Lsn,
    types::RecordType,
    wal::{WalHandle, iterator::WalIterator},
};

use crate::{
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

/// private catalog and MVCC state reconstructed during recovery
///
/// this state is delibrately independent from the running executor. startup
/// replay and validate the complete selected WAL suffix before transferring
/// the recovered state to live database components
///
/// if any later record is invalid, the caller discards this entire value and
/// partially reconstructed state becomes visible to sessions
#[derive(Debug, Default)]
pub struct RecoveryState {
    catalog: MemoryCatalog,
    mvcc_by_table: BTreeMap<TableId, InMemoryMvcc>,
}

impl RecoveryState {
    /// an empty recovery staging state
    pub fn new() -> Self {
        Self::default()
    }

    /// returns the catalog reconstructed from durable metadata operations.
    pub fn catalog(&self) -> &MemoryCatalog {
        &self.catalog
    }

    /// return the reconstructed MVCC state for one table
    ///
    /// `None` means no durable catalog definition for this table has been
    /// successfully applied to the recovery state
    pub fn table_storage(&self, table_id: TableId) -> Option<&InMemoryMvcc> {
        self.mvcc_by_table.get(&table_id)
    }

    /// apply one decoded record to private recovery state
    ///
    /// catalog updates create the schema and corresponding empty MVCC state
    /// Transaction commits require that catalog state to exist first. Snapshot
    /// pointers and checkpoint markers are recovery-planning metadata and do not
    /// directly mutate catalog or MVCC state
    pub fn apply_record(&mut self, record: &DecodedRecoveryRecord) -> Result<()> {
        match &record.payload {
            RecoveryPayload::CatalogUpdate(update) => self.apply_catalog_update(record.lsn, update),

            RecoveryPayload::SingleNodeTxnCommit(commit) => {
                self.apply_transaction_commit(record.lsn, commit)
            }

            RecoveryPayload::SnapshotPointer(_) | RecoveryPayload::CheckpointMarker(_) => {
                // snapshot selection and allocator high-water tracking are
                // handled by later recovery slices. These records carry no
                // direct catalog or MVCC state transition.
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
        if self.catalog.table_by_id(commit.table_id).is_none() {
            return Err(Error::CorruptData(format!(
                "SingleNodeTxnCommit at WAL LSN {} references table {} \
                 before its catalog definition appears in durable history",
                lsn.as_u64(),
                commit.table_id.0
            )));
        }

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

        Ok(())
    }
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
/// the state is returned only after the stream reaches its validated end
/// if phsycisal iteration, semantic decoind catalog installation, dependency
/// validation or MVCC application fails, the half or partial reconstructed state
/// dropped and cannot be published accidently
pub fn replay_recovery_stream<F>(mut stream: RecoveryRecordStream<F>) -> Result<RecoveryState>
where
    F: SegmentFile,
{
    let mut state = RecoveryState::new();

    while let Some(record) = stream.next_record()? {
        state.apply_record(&record)?;
    }

    Ok(state)
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
