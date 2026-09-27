use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use ragnordb_tablet::IntentCleanupReport;
use std::{sync::OnceLock, time::Instant};
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
    metrics::describe_gauge!(
        "ragnordb_txn_gc_safe_point",
        "Published metadata-managed MVCC history safe point"
    );
    metrics::describe_gauge!(
        "ragnordb_txn_gc_protections_active",
        "Live durable MVCC history protections at the latest safe-point sweep"
    );
    metrics::describe_counter!(
        "ragnordb_txn_gc_protection_release_failures_total",
        "Transaction history protections left to expire after release failures"
    );
    metrics::describe_counter!(
        "ragnordb_txn_gc_protection_register_total",
        "Ordinary transaction GC-protection registrations"
    );
    metrics::describe_counter!(
        "ragnordb_txn_gc_protection_aggregate_register_total",
        "Aggregate GC-protection lease registrations"
    );
    metrics::describe_counter!(
        "ragnordb_txn_gc_protection_aggregate_update_total",
        "Atomic aggregate GC-protection floor updates"
    );
    metrics::describe_counter!(
        "ragnordb_txn_gc_protection_aggregate_release_total",
        "Aggregate GC-protection lease releases"
    );
    metrics::describe_histogram!(
        "ragnordb_txn_gc_protection_register_seconds",
        "Metadata latency for aggregate GC-protection registration"
    );
    metrics::describe_histogram!(
        "ragnordb_txn_gc_protection_release_seconds",
        "Metadata latency for aggregate GC-protection release/update"
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
        "ragnordb_txn_prewrite_seconds",
        "Distributed transaction prewrite phase latency"
    );
    metrics::describe_histogram!(
        "ragnordb_txn_commit_seconds",
        "Distributed transaction commit phase latency"
    );
    metrics::describe_histogram!(
        "ragnordb_txn_rollback_seconds",
        "Distributed transaction rollback phase latency"
    );
    metrics::describe_counter!(
        "ragnordb_txn_participant_batches_total",
        "Participant batches dispatched by transaction phase"
    );
    metrics::describe_histogram!(
        "ragnordb_statement_execution_seconds",
        "Blocking SQL execution latency after admission"
    );
    metrics::describe_histogram!(
        "ragnordb_sql_request_to_execution_complete_seconds",
        "SQL request latency from server admission through response completion"
    );
    metrics::describe_histogram!(
        "ragnordb_txn_begin_seconds",
        "BEGIN transaction setup latency, including GC-history protection admission"
    );
    metrics::describe_histogram!(
        "ragnordb_txn_commit_service_seconds",
        "Time spent in the database service commit boundary"
    );
    metrics::describe_histogram!(
        "ragnordb_commit_timestamp_allocation_seconds",
        "Commit timestamp allocation latency, including any durable oracle refill"
    );
    metrics::describe_histogram!(
        "ragnordb_txn_update_read_seconds",
        "Time spent reading rows selected by an UPDATE before buffering mutations"
    );
    metrics::describe_histogram!(
        "ragnordb_txn_write_set_buffer_seconds",
        "Time spent adding a statement's mutations to its transaction write set"
    );
    metrics::describe_counter!(
        "ragnordb_txn_write_set_mutations_total",
        "Mutations successfully added to SQL transaction write sets"
    );
    metrics::describe_histogram!(
        "ragnordb_single_shard_command_construction_seconds",
        "Time spent constructing a replicated SingleShardCommit command"
    );
    metrics::describe_histogram!(
        "ragnordb_tablet_command_queue_admission_seconds",
        "Time spent admitting a tablet command to the bounded reactor mailbox"
    );
    metrics::describe_histogram!(
        "ragnordb_tablet_command_client_wait_seconds",
        "Time from tablet command enqueue until its apply result reaches the caller"
    );
    metrics::describe_counter!(
        "ragnordb_semantic_batches_total",
        "Mutation batches formed by the tablet semantic batcher"
    );
    metrics::describe_counter!(
        "ragnordb_semantic_batch_commands_total",
        "Mutation commands included in tablet semantic batches"
    );
    metrics::describe_histogram!(
        "ragnordb_semantic_batch_commands",
        "Number of commands in each tablet semantic batch"
    );
    metrics::describe_histogram!(
        "ragnordb_semantic_batch_bytes",
        "Encoded bytes in each tablet semantic batch"
    );
    metrics::describe_histogram!(
        "ragnordb_semantic_batch_wait_seconds",
        "Time spent forming a tablet semantic batch after its first request is dequeued"
    );
    metrics::describe_histogram!(
        "ragnordb_raft_proposal_admission_seconds",
        "Raft-core proposal admission latency"
    );
    metrics::describe_counter!(
        "ragnordb_raft_proposals_admitted_total",
        "Application proposals admitted by local Raft groups"
    );
    metrics::describe_counter!(
        "ragnordb_raft_proposal_payload_bytes_total",
        "Encoded application proposal bytes admitted by local Raft groups"
    );
    metrics::describe_counter!(
        "ragnordb_raft_ready_generations_total",
        "New Raft Ready generations produced by local groups"
    );
    metrics::describe_histogram!(
        "ragnordb_raft_proposal_to_persisted_seconds",
        "Time from local Raft proposal admission to successful A-WAL persistence"
    );
    metrics::describe_histogram!(
        "ragnordb_raft_persisted_to_quorum_seconds",
        "Time from successful local persistence until the proposal is observed committed"
    );
    metrics::describe_histogram!(
        "ragnordb_raft_quorum_to_apply_seconds",
        "Time from the committed Ready being observed until its proposal is applied"
    );
    metrics::describe_histogram!(
        "ragnordb_raft_state_machine_apply_seconds",
        "State-machine apply duration for one committed Raft entry"
    );
    metrics::describe_histogram!(
        "ragnordb_tablet_apply_to_reply_seconds",
        "Time to forward an applied tablet result to its waiting caller"
    );
    metrics::describe_counter!(
        "ragnordb_persistence_queue_admissions_total",
        "Ready persistence batches admitted to the node-wide A-WAL queue"
    );
    metrics::describe_histogram!(
        "ragnordb_persistence_queue_wait_seconds",
        "Time a Ready persistence batch waits before dispatch to the A-WAL worker"
    );
    metrics::describe_counter!(
        "ragnordb_awal_sync_calls_total",
        "Node-wide Raft A-WAL append-and-sync calls"
    );
    metrics::describe_counter!(
        "ragnordb_awal_sync_records_total",
        "Raft WAL records included in node-wide append-and-sync calls"
    );
    metrics::describe_counter!(
        "ragnordb_awal_sync_bytes_total",
        "Raft WAL payload bytes included in node-wide append-and-sync calls"
    );
    metrics::describe_counter!(
        "ragnordb_awal_sync_groups_total",
        "Raft groups represented in node-wide append-and-sync calls"
    );
    metrics::describe_histogram!(
        "ragnordb_awal_sync_latency_seconds",
        "A-WAL append-and-sync latency, one observation per physical sync"
    );
    metrics::describe_histogram!(
        "ragnordb_awal_records_per_sync",
        "Raft WAL records included in each node-wide append-and-sync call"
    );
    metrics::describe_histogram!(
        "ragnordb_awal_bytes_per_sync",
        "Raft WAL payload bytes included in each node-wide append-and-sync call"
    );
    metrics::describe_histogram!(
        "ragnordb_awal_groups_per_sync",
        "Raft groups included in each node-wide append-and-sync call"
    );
    metrics::describe_gauge!(
        "ragnordb_raft_pending_proposals",
        "Current aggregate number of admitted proposals awaiting a terminal result"
    );
    metrics::describe_gauge!(
        "ragnordb_persistence_pending_groups",
        "Current Ready persistence groups queued or in flight"
    );
    metrics::describe_gauge!(
        "ragnordb_persistence_pending_records",
        "Current Ready persistence records queued or in flight"
    );
    metrics::describe_gauge!(
        "ragnordb_persistence_pending_bytes",
        "Current Ready persistence bytes queued or in flight"
    );
    metrics::describe_gauge!(
        "ragnordb_raft_apply_backlog_entries",
        "Current aggregate committed entries waiting for state-machine apply"
    );
    metrics::describe_gauge!(
        "ragnordb_raft_apply_backlog_bytes",
        "Current aggregate committed bytes waiting for state-machine apply"
    );
    metrics::describe_gauge!(
        "ragnordb_raft_apply_backlog_oldest_age_seconds",
        "Age of the oldest committed Ready generation waiting for apply"
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

/// Records one monotonic duration sample when the guarded scope exits.
///
/// Keeping the timer as an RAII guard ensures early returns and `?` paths are
/// included in the same latency distribution as successful operations.
pub struct HistogramTimer {
    name: &'static str,
    started_at: Instant,
}

impl HistogramTimer {
    /// Start a timer that records one histogram observation when dropped.
    pub fn start(name: &'static str) -> Self {
        Self {
            name,
            started_at: Instant::now(),
        }
    }
}

impl Drop for HistogramTimer {
    fn drop(&mut self) {
        histogram_record(self.name, self.started_at.elapsed().as_secs_f64());
    }
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
