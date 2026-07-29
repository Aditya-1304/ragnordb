//! read only operational inspection for persisted RagnorDB artifacts
//!
//! A-WAL remains responsible for physical record framing, checksum validation,
//! and recovery reporting. This module only interprets valid RagnorDB-owned
//! payloads after A-WAL has established the physically readable WAL prefix

use std::{error::Error as StdError, io, path::Path};

use ragnordb_common::{command_codec::CatalogOperation, ids::NodeId};
use ragnordb_storage::recovery::{DecodedRecoveryRecord, RecoveryPayload, scan_recovery_records};
use wal::{
    config::WalConfig,
    io::directory::FsSegmentDirectory,
    lsn::Lsn,
    types::WalIdentity,
    wal::{WalHandle, report::RecoveryReport},
};

/// inspects one local node RagnorDB WAL without starting the SQL server
///
/// The inspector opens A-WAL in read-only mode so running an operational
/// diagnostic cannot append records, clear a clean-shutdown witness, or repair
/// a truncatable tail. A-WAL's structured recovery report is printed before
/// semantic record decoding begins, preserving physical diagnostics when a
/// later RagnorDB payload is malformed
pub fn run_wal(data_dir: &Path, node_id: NodeId) -> Result<(), Box<dyn StdError>> {
    if node_id.0 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "node ID 0 cannot identify a RagnorDB WAL",
        )
        .into());
    }

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
    let mut records = scan_recovery_records(&wal, first_lsn)?;

    println!("ragnordb_records:");

    while let Some(record) = records.next_record()? {
        print_database_record(&record);
    }

    Ok(())
}

/// print A-WAL-owned recovery facts without reimplementing physical WAL logic
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

/// render one already-validated RagnorDB payload as a stable single line entry
///
/// internal A-WAL records never reach this function: `scan_recovery_records`
/// deliberately filters them before RagnorDB semantic decoding
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
