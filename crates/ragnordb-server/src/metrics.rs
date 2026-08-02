use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
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
