use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use ragnordb_tablet::IntentCleanupReport;
use std::sync::OnceLock;
use tracing::warn;

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn init_metrics() {
    if PROMETHEUS_HANDLE.get().is_some() {
        return;
    }

    match PrometheusBuilder::new().install_recorder() {
        Ok(handle) => {
            let _ = PROMETHEUS_HANDLE.set(handle);
            describe_metrics();
        }
        Err(error) => {
            warn!(
                error = %error,
                "metrics recorder already installed or unavailable"
            );
        }
    }
}

fn describe_metrics() {
    metrics::describe_counter!(
        "RagnorDB_connections_accepted_total",
        "Total client connections accepted"
    );

    metrics::describe_gauge!(
        "RagnorDB_connections_active",
        "Currently active client connections"
    );

    metrics::describe_counter!(
        "RagnorDB_requests_received_total",
        "Total SQL requests received"
    );

    metrics::describe_counter!(
        "RagnorDB_requests_success_total",
        "SQL requests that completed successfully"
    );

    metrics::describe_counter!(
        "RagnorDB_requests_error_total",
        "SQL requests that returned an error"
    );

    metrics::describe_counter!(
        "RagnorDB_response_rows_read_total",
        "Rows reported as read across successful SQL responses"
    );

    metrics::describe_counter!(
        "RagnorDB_response_rows_written_total",
        "Rows reported as written across successful SQL responses"
    );

    metrics::describe_counter!("ragnordb_txn_commits_total", "Committed transactions");
    metrics::describe_counter!("ragnordb_txn_aborts_total", "Rolled-back transactions");
    metrics::describe_gauge!(
        "ragnordb_txn_active_transactions",
        "Currently active distributed transactions tracked by the gateway"
    );
    metrics::describe_counter!(
        "ragnordb_txn_heartbeat_attempts_total",
        "Durable transaction heartbeat attempts"
    );
    metrics::describe_counter!(
        "ragnordb_txn_heartbeat_failures_total",
        "Transaction heartbeat attempts that did not cross the durable boundary"
    );
    metrics::describe_counter!(
        "ragnordb_txn_expired_transactions_found_total",
        "Transactions whose authoritative lease was found expired by a cleaner"
    );
    metrics::describe_counter!(
        "ragnordb_txn_intents_resolved_by_reader_total",
        "Intents resolved by foreground readers"
    );
    metrics::describe_counter!(
        "ragnordb_txn_intents_resolved_by_cleaner_total",
        "Intents resolved by background cleaners"
    );
    metrics::describe_counter!(
        "ragnordb_txn_intent_cleaner_runs_total",
        "Background transaction intent-cleaner passes"
    );
    metrics::describe_counter!(
        "ragnordb_txn_intent_cleaner_failures_total",
        "Background transaction intent-cleaner passes that failed"
    );
    metrics::describe_counter!(
        "ragnordb_txn_commit_unknown_total",
        "Commit attempts whose durable outcome requires recovery"
    );
    metrics::describe_histogram!(
        "ragnordb_statement_execution_seconds",
        "Blocking SQL execution latency after admission"
    );
    metrics::describe_histogram!(
        "ragnordb_wal_append_latency_seconds",
        "Latest observed A-WAL append latency"
    );
    metrics::describe_histogram!(
        "ragnordb_wal_sync_latency_seconds",
        "Latest observed A-WAL synchronization latency"
    );
    metrics::describe_histogram!(
        "ragnordb_recovery_duration_seconds",
        "Physical A-WAL startup recovery duration"
    );
    metrics::describe_counter!(
        "ragnordb_recovery_records_replayed_total",
        "Physical WAL records scanned during startup recovery"
    );
    metrics::describe_counter!(
        "ragnordb_checkpoint_success_total",
        "Checkpoints fully published and retention-advanced"
    );
    metrics::describe_counter!(
        "ragnordb_checkpoint_failure_total",
        "Checkpoint publication attempts that failed"
    );
    metrics::describe_counter!(
        "ragnordb_timestamp_allocations_total",
        "MVCC timestamps allocated from committed local ranges"
    );
    metrics::describe_counter!(
        "ragnordb_timestamp_reservations_total",
        "Durable metadata timestamp-range reservations committed"
    );
    metrics::describe_histogram!(
        "ragnordb_timestamp_allocation_latency_seconds",
        "Timestamp allocation latency, including any local prefetch"
    );
    metrics::describe_histogram!(
        "ragnordb_timestamp_reservation_latency_seconds",
        "Metadata Raft timestamp reservation latency"
    );
    metrics::describe_gauge!(
        "ragnordb_timestamp_last_allocated",
        "Most recently allocated MVCC timestamp"
    );
    metrics::describe_gauge!(
        "ragnordb_timestamp_reserved_until",
        "Durable timestamp reservation frontier"
    );
    metrics::describe_gauge!(
        "ragnordb_timestamp_unused_reserved_gap",
        "Reserved timestamp values above the last allocation"
    );
    metrics::describe_gauge!("ragnordb_wal_durable_lsn", "Current durable WAL frontier");
    metrics::describe_gauge!("ragnordb_wal_retained_bytes", "Current retained WAL bytes");
    metrics::describe_gauge!(
        "ragnordb_wal_oldest_retention_pin",
        "Lowest LSN held by an active retention pin, or zero when unpinned"
    );
    metrics::describe_gauge!(
        "ragnordb_checkpoint_replay_frontier",
        "Replay frontier of the latest published checkpoint"
    );
    metrics::describe_gauge!(
        "ragnordb_node_recovery_required",
        "One when the node durability gate requires recovery"
    );
}

pub fn render_metrics() -> String {
    match PROMETHEUS_HANDLE.get() {
        Some(handle) => handle.render(),
        None => String::from("# metrics not initialized"),
    }
}

pub fn counter_inc(name: &'static str) {
    counter_add(name, 1);
}

pub fn counter_add(name: &'static str, value: u64) {
    metrics::counter!(name).increment(value);
}

pub fn gauge_set(name: &'static str, value: f64) {
    metrics::gauge!(name).set(value);
}

pub fn histogram_record(name: &'static str, value: f64) {
    metrics::histogram!(name).record(value);
}

pub fn set_active_transactions(value: usize) {
    metrics::gauge!("ragnordb_txn_active_transactions").set(value as f64);
}

pub fn record_heartbeat_attempt() {
    counter_inc("ragnordb_txn_heartbeat_attempts_total");
}

pub fn record_heartbeat_failure() {
    counter_inc("ragnordb_txn_heartbeat_failures_total");
}

pub fn record_reader_intent_resolution() {
    counter_inc("ragnordb_txn_intents_resolved_by_reader_total");
}

pub fn record_intent_cleaner_report(report: &IntentCleanupReport) {
    counter_add(
        "ragnordb_txn_expired_transactions_found_total",
        report.expired_transactions as u64,
    );
    counter_add(
        "ragnordb_txn_intents_resolved_by_cleaner_total",
        report.resolved_intents as u64,
    );
}
