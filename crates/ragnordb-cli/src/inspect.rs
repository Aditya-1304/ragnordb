//! read only operational inspection for persisted RagnorDB artifacts
//!
//! A-WAL owns physical record framing, checksums, and recoverable-prefix
//! detection. This module prints A-WAL's structured recovery report, then
//! decodes only RagnorDB-owned user records for operator diagnostics

use std::{error::Error as StdError, io, path::Path};

use ragnordb_common::{
    command_codec::{
        CatalogOperation, MAX_TABLET_COMMAND_BATCH_BYTES, TabletCommand,
        TabletCommandBatchEnvelope, TabletCommandEnvelope,
    },
    ids::NodeId,
};
use ragnordb_multiraft::storage::{
    codec::{DurableRaftEntryPayload, RaftLogEntryRecord},
    persistence::RaftWalRecordType,
};
use ragnordb_server::data_directory_lock::DataDirectoryLock;
use ragnordb_storage::recovery::{DecodedRecoveryRecord, RecoveryPayload, decode_recovery_record};
use wal::{
    config::WalConfig,
    io::directory::FsSegmentDirectory,
    lsn::Lsn,
    types::WalIdentity,
    wal::{WalHandle, report::RecoveryReport},
};

/// Bound offline transaction diagnostics so a large WAL cannot create
/// unbounded decoding work or terminal output.
const MAX_RAFT_ENTRY_DIAGNOSTICS: usize = 128;
const MAX_RAFT_ENTRY_DECODE_BYTES: usize = MAX_TABLET_COMMAND_BATCH_BYTES + 64 * 1024;
const MAX_TOTAL_RAFT_DECODE_BYTES: usize = 8 * 1024 * 1024;
const MAX_TRANSACTION_COMMAND_SUMMARIES: usize = 512;

/// inspect one local node's RagnorDB WAL without starting the SQL server
///
/// the inspector opens A-WAL read-only: it cannot append records, clear a
/// clean-shutdown witness, or repair a truncatable tail. Physical A-WAL
/// diagnostics are printed before semantic decoding begins. A malformed
/// RagnorDB payload is reported and contributes to a non-zero process exit,
/// but does not prevent inspection of later physically valid records
pub fn run_wal(data_dir: &Path, node_id: NodeId) -> Result<(), Box<dyn StdError>> {
    if node_id.0 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "node ID 0 cannot identify a RagnorDB WAL",
        )
        .into());
    }

    // offline inspection requires the same exclusive ownership used by the live
    // server. The guard remains in scope until physical iteration and semantic
    // decoding have both completed, so checkpoint retention cannot change the
    // inspected segment set during this command
    let _data_directory_lock = DataDirectoryLock::acquire(data_dir)?;

    let wal_dir = data_dir.join("wal");
    let wal_config = WalConfig {
        dir: wal_dir.clone(),
        identity: WalIdentity::new(node_id.0, 1, 1),
        read_only: true,
        truncate_tail: false,
        ..WalConfig::default()
    };

    let (wal, recovery_report) = WalHandle::open(FsSegmentDirectory::new(wal_dir), wal_config, ())?;

    print_physical_recovery_report(&recovery_report);

    let first_lsn = recovery_report.first_lsn.unwrap_or(Lsn::ZERO);

    // A-WAL intentionally rejects retention pins on read-only handles. Keep
    // this standalone CLI read-only rather than creating a second mutable WAL
    // owner. Its current operating boundary is offline inspection; a future
    // online inspector must obtain a pin from the server-owned WAL handle
    let mut records = wal.iter_from(first_lsn)?;
    let mut malformed_payloads = 0_usize;
    let mut raft_diagnostics = RaftDiagnosticBudget::default();

    println!("ragnordb_records:");
    println!("replicated_transaction_diagnostics:");

    loop {
        let attempted_lsn = records.current_lsn();
        let physical_record = records.next().map_err(|source| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "A-WAL physical iteration failed at LSN {}: {source}",
                    attempted_lsn.as_u64()
                ),
            )
        })?;

        let Some(physical_record) = physical_record else {
            break;
        };

        if physical_record.record_type == RaftWalRecordType::LogEntry.as_wal_record_type() {
            if raft_diagnostics.entries_inspected >= MAX_RAFT_ENTRY_DIAGNOSTICS {
                raft_diagnostics.entries_omitted += 1;
            } else {
                raft_diagnostics.entries_inspected += 1;
                print_raft_entry_diagnostic(
                    physical_record.lsn,
                    &physical_record.payload,
                    &mut raft_diagnostics,
                );
            }
        }

        match decode_recovery_record(
            physical_record.lsn,
            physical_record.record_type,
            &physical_record.payload,
        ) {
            Ok(Some(record)) => print_database_record(&record),

            // A-WAL internal records are physically valid but have no
            // RagnorDB semantic meaning, so the CLI deliberately omits them
            Ok(None) => {}

            Err(error) => {
                malformed_payloads += 1;

                println!(
                    "  malformed_database_payload: lsn={} record_type={} error={error}",
                    physical_record.lsn.as_u64(),
                    physical_record.record_type.as_u16(),
                );
            }
        }
    }

    if raft_diagnostics.entries_omitted > 0 {
        println!(
            "  raft_entry_diagnostics_omitted: {}",
            raft_diagnostics.entries_omitted
        );
    }
    if raft_diagnostics.command_summaries_omitted > 0 {
        println!(
            "  transaction_command_summaries_omitted: {}",
            raft_diagnostics.command_summaries_omitted
        );
    }

    if malformed_payloads == 0 {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("WAL inspection found {malformed_payloads} malformed RagnorDB payload(s)"),
        )
        .into())
    }
}

#[derive(Default)]
struct RaftDiagnosticBudget {
    entries_inspected: usize,
    entries_omitted: usize,
    total_decode_bytes: usize,
    command_summaries_printed: usize,
    command_summaries_omitted: usize,
}

/// Decode only bounded Raft log entries for operational transaction identity
/// diagnostics. The shared WAL record remains owned by MultiRaft recovery; this
/// view intentionally reports no mutation keys or row values.
fn print_raft_entry_diagnostic(lsn: Lsn, bytes: &[u8], budget: &mut RaftDiagnosticBudget) {
    if bytes.len() > MAX_RAFT_ENTRY_DECODE_BYTES {
        println!(
            "  raft_log_entry: lsn={} diagnostic=skipped reason=entry_exceeds_decode_limit bytes={} max_bytes={}",
            lsn.as_u64(),
            bytes.len(),
            MAX_RAFT_ENTRY_DECODE_BYTES,
        );
        return;
    }

    if budget.total_decode_bytes.saturating_add(bytes.len()) > MAX_TOTAL_RAFT_DECODE_BYTES {
        println!(
            "  raft_log_entry: lsn={} diagnostic=skipped reason=total_decode_budget_exhausted bytes={}",
            lsn.as_u64(),
            bytes.len(),
        );
        return;
    }
    budget.total_decode_bytes += bytes.len();

    let entry = match RaftLogEntryRecord::decode(bytes) {
        Ok(entry) => entry,
        Err(_) => {
            println!(
                "  raft_log_entry: lsn={} diagnostic=unavailable reason=invalid_raft_entry_record",
                lsn.as_u64(),
            );
            return;
        }
    };

    let identity = entry.identity;
    match entry.payload {
        DurableRaftEntryPayload::Configuration(_) => println!(
            "  raft_log_entry: lsn={} group_id={} replica_id={} index={} term={} payload=configuration_change",
            lsn.as_u64(),
            identity.raft_group_id.0,
            identity.replica_id.0,
            entry.index,
            entry.term,
        ),
        DurableRaftEntryPayload::Normal(command_bytes) => {
            let (encoding, commands) = match decode_tablet_commands(&command_bytes) {
                Some(decoded) => decoded,
                None => {
                    println!(
                        "  raft_log_entry: lsn={} group_id={} replica_id={} index={} term={} payload=unrecognized_tablet_command",
                        lsn.as_u64(),
                        identity.raft_group_id.0,
                        identity.replica_id.0,
                        entry.index,
                        entry.term,
                    );
                    return;
                }
            };

            println!(
                "  raft_log_entry: lsn={} group_id={} replica_id={} index={} term={} encoding={} commands={}",
                lsn.as_u64(),
                identity.raft_group_id.0,
                identity.replica_id.0,
                entry.index,
                entry.term,
                encoding,
                commands.len(),
            );

            for envelope in commands {
                let (kind, txn_id) = transaction_command_identity(&envelope.command);
                if budget.command_summaries_printed >= MAX_TRANSACTION_COMMAND_SUMMARIES {
                    budget.command_summaries_omitted += 1;
                    continue;
                }

                budget.command_summaries_printed += 1;
                println!(
                    "    command: tablet_id={} kind={} txn_id={}",
                    envelope.tablet_id.0,
                    kind,
                    txn_id.map_or_else(|| "-".to_string(), |id| id.to_string()),
                );
            }
        }
    }
}

/// Accept both the original one-command envelope and the bounded batch format
/// without exposing the command's mutation payload to inspection output.
fn decode_tablet_commands(bytes: &[u8]) -> Option<(&'static str, Vec<TabletCommandEnvelope>)> {
    if let Ok(batch) = TabletCommandBatchEnvelope::decode(bytes) {
        return Some(("batch", batch.commands));
    }

    TabletCommandEnvelope::decode(bytes)
        .ok()
        .map(|envelope| ("single", vec![envelope]))
}

fn transaction_command_identity(command: &TabletCommand) -> (&'static str, Option<u64>) {
    match command {
        TabletCommand::Prewrite(command) => ("prewrite", Some(command.txn_id.0)),
        TabletCommand::Commit(command) => ("commit", Some(command.txn_id.0)),
        TabletCommand::Rollback(command) => ("rollback", Some(command.txn_id.0)),
        TabletCommand::SingleShardCommit(command) => {
            ("single_shard_commit", Some(command.txn_id.0))
        }
        TabletCommand::ResolveIntent(command) => ("resolve_intent", Some(command.txn_id.0)),
        TabletCommand::PublishAbortedTransactionStatus(command) => (
            "publish_aborted_status",
            Some(command.status_record.txn_id.0),
        ),
        TabletCommand::HeartbeatTransactionStatus(command) => {
            ("heartbeat_status", Some(command.expected_status.txn_id.0))
        }
        TabletCommand::ExpirePendingTransactionStatus(command) => (
            "expire_pending_status",
            Some(command.expected_status.txn_id.0),
        ),
        TabletCommand::Catalog(_) => ("catalog", None),
        TabletCommand::Noop(_) => ("noop", None),
    }
}

/// print A-WAL-owned recovery facts without duplicating physical WAL logic
fn print_physical_recovery_report(report: &RecoveryReport) {
    println!("physical_recovery:");
    println!("  segments_scanned: {}", report.segments_scanned);
    println!("  sealed_segments: {}", report.sealed_segments);
    println!("  records_scanned: {}", report.records_scanned);
    println!("  corrupt_records_found: {}", report.corrupt_records_found);
    println!("  first_lsn: {}", optional_lsn(report.first_lsn));
    println!("  last_valid_lsn: {}", optional_lsn(report.last_valid_lsn));
    println!("  next_lsn: {}", report.next_lsn.as_u64());
    println!("  checkpoint_lsn: {}", optional_lsn(report.checkpoint_lsn));
    println!("  truncated_bytes: {}", report.truncated_bytes);
    println!("  segments_prunable: {}", report.segments_prunable);
    println!("  clean_shutdown: {}", report.clean_shutdown);
    println!("  recovery_skipped: {}", report.recovery_skipped);
    println!(
        "  recovery_duration_ms: {}",
        report.recovery_duration.as_millis()
    );
}

/// render one validated RagnorDB payload as a stable, single-line entry
fn print_database_record(record: &DecodedRecoveryRecord) {
    let (record_type, commit_timestamp, table_id, summary) = match &record.payload {
        RecoveryPayload::CatalogUpdate(update) => {
            let summary = match &update.command.operation {
                CatalogOperation::CreateTable(create) => format!(
                    "catalog_update create_table name={} schema_version={} update_timestamp={}",
                    create.table_def.name,
                    create.table_def.schema_version,
                    update.update_timestamp.0,
                ),
            };

            ("CatalogUpdate", None, Some(update.table_id.0), summary)
        }

        RecoveryPayload::SingleNodeTxnCommit(commit) => {
            let (puts, deletes) =
                commit
                    .writes
                    .values()
                    .fold(
                        (0_usize, 0_usize),
                        |(puts, deletes), mutation| match mutation {
                            ragnordb_storage::wal::WalMutation::Put(_) => (puts + 1, deletes),
                            ragnordb_storage::wal::WalMutation::Delete => (puts, deletes + 1),
                        },
                    );

            (
                "SingleNodeTxnCommit",
                Some(commit.commit_timestamp.0),
                Some(commit.table_id.0),
                format!(
                    "single_node_txn_commit txn_id={} start_timestamp={} writes={} puts={} deletes={}",
                    commit.txn_id.0,
                    commit.start_timestamp.0,
                    commit.writes.len(),
                    puts,
                    deletes,
                ),
            )
        }

        RecoveryPayload::SnapshotPointer(pointer) => (
            "SnapshotPointer",
            None,
            None,
            format!(
                "snapshot_pointer snapshot_id={} snapshot_timestamp={} replay_from_lsn={} tables={} path={}",
                pointer.snapshot_id,
                pointer.snapshot_timestamp.0,
                pointer.replay_from_lsn.as_u64(),
                pointer.table_ids.len(),
                pointer.relative_path,
            ),
        ),

        RecoveryPayload::CheckpointMarker(marker) => (
            "CheckpointMarker",
            None,
            None,
            format!(
                "checkpoint_marker snapshot_id={} snapshot_timestamp={} replay_from_lsn={}",
                marker.snapshot_id,
                marker.snapshot_timestamp.0,
                marker.replay_from_lsn.as_u64(),
            ),
        ),
    };

    println!(
        "  lsn={} type={} commit_timestamp={} table_id={} summary={summary:?}",
        record.lsn.as_u64(),
        record_type,
        optional_u64(commit_timestamp),
        optional_u64(table_id),
    );
}

/// format an optional physical WAL location consistently in operator output
fn optional_lsn(lsn: Option<Lsn>) -> String {
    lsn.map_or_else(|| "-".to_string(), |value| value.as_u64().to_string())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}
