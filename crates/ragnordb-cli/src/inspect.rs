//! read only operational inspection for persisted RagnorDB artifacts
//!
//! A-WAL owns physical record framing, checksums, and recoverable-prefix
//! detection. This module prints A-WAL's structured recovery report, then
//! decodes only RagnorDB-owned user records for operator diagnostics

use std::{error::Error as StdError, io, path::Path};

use ragnordb_common::{command_codec::CatalogOperation, ids::NodeId};
use ragnordb_server::data_directory_lock::DataDirectoryLock;
use ragnordb_storage::recovery::{DecodedRecoveryRecord, RecoveryPayload, decode_recovery_record};
use wal::{
    config::WalConfig,
    io::directory::FsSegmentDirectory,
    lsn::Lsn,
    types::WalIdentity,
    wal::{WalHandle, report::RecoveryReport},
};

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

    println!("ragnordb_records:");

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
