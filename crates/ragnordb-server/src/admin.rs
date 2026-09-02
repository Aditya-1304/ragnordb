use axum::{Json, Router, http::header, response::IntoResponse, routing::get};
use ragnordb_common::durability::{DurabilityGate, NodeDurabilityState};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::build_info::BUILD_INFO;
use crate::database::SharedLocalDatabase;
use crate::metrics;
use crate::replicated_tablet::ReplicatedTabletHandle;
use ragnordb_multiraft::host::SharedMultiRaftHostStatus;

/// Thread-safe error type returned by administrative server tasks.
///
/// Tokio may move spawned futures and their outputs between worker threads.
/// Therefore, errors returned from a spawned task must implement `Send`.
pub type AdminError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct AdminState {
    pub started_at: u64,
    pub connection_semaphore: Arc<Semaphore>,
    pub max_connections: u32,
    pub durability_gate: DurabilityGate,
    pub database: SharedLocalDatabase,
    pub replicated_tablet: Option<Arc<ReplicatedTabletHandle>>,
    pub multiraft_status: Option<SharedMultiRaftHostStatus>,
}

pub async fn start_admin_server(
    addr: SocketAddr,
    state: Arc<AdminState>,
    shutdown: CancellationToken,
) -> Result<(), AdminError> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_admin(listener, state, shutdown).await
}

pub async fn serve_admin(
    listener: tokio::net::TcpListener,
    state: Arc<AdminState>,
    shutdown: CancellationToken,
) -> Result<(), AdminError> {
    let addr = listener.local_addr()?;

    let app = Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/status", get(handle_status))
        .with_state(state);

    info!(admin_addr = %addr, "admin HTTP server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            info!("admin HTTP server shutting down");
        })
        .await?;

    Ok(())
}

/// Return metrics using the Prometheus text exposition content type.
async fn handle_metrics() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        metrics::render_metrics(),
    )
}

/// Return structured node status as JSON.
async fn handle_status(
    axum::extract::State(state): axum::extract::State<Arc<AdminState>>,
) -> Json<serde_json::Value> {
    let active = state.max_connections as usize - state.connection_semaphore.available_permits();
    // Health and leadership diagnostics must remain available while a SQL
    // request owns the serialized database state. Storage details are omitted
    // for that sample instead of blocking the complete status response.
    let storage = state
        .database
        .try_lock()
        .ok()
        .map(|database| database.status());

    let durability = match state.durability_gate.state() {
        NodeDurabilityState::Healthy => {
            metrics::gauge_set("ragnordb_node_recovery_required", 0.0);
            serde_json::json!({
                "state": "healthy",
                "recovery_required": false,
            })
        }

        NodeDurabilityState::RecoveryRequired(failure) => {
            metrics::gauge_set("ragnordb_node_recovery_required", 1.0);
            serde_json::json!({
                "state": failure.kind().as_str(),
                "recovery_required": true,
                "reason": failure.reason(),
            })
        }
    };
    let replication = state.replicated_tablet.as_ref().map(|runtime| {
        let status = runtime.status();
        serde_json::json!({
            "role": status.role.map(|role| role.as_str()),
            "leader_replica_id": status.leader_replica_id,
            "term": status.term,
            "commit_index": status.commit_index,
            "last_log_index": status.last_log_index,
            "applied_index": status.applied_index,
            "snapshot_index": status.snapshot_index,
            "uncommitted_bytes": status.uncommitted_bytes,
            "replication_inflight_bytes": status.replication_inflight_bytes,
            "is_leader": status.serving_leader,
            "runtime_error": status.runtime_error,
        })
    });
    let multiraft = state.multiraft_status.as_ref().map(|status_handle| {
        let status = status_handle
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let groups = status
            .groups
            .iter()
            .map(|group| {
                serde_json::json!({
                    "raft_group_id": group.identity.raft_group_id.0,
                    "replica_id": group.identity.replica_id.0,
                    "role": group.role.map(|role| role.as_str()),
                    "leader_replica_id": group.leader_replica_id.map(|replica_id| replica_id.0),
                    "term": group.term,
                    "commit_index": group.commit_index,
                    "last_log_index": group.last_log_index,
                    "applied_index": group.applied_index,
                    "snapshot_index": group.snapshot_index,
                    "uncommitted_bytes": group.uncommitted_bytes,
                    "replication_inflight_bytes": group.replication_inflight_bytes,
                    "pending_work": group.pending_work,
                    "pending_messages": group.pending_messages,
                    "pending_message_bytes": group.pending_message_bytes,
                    "quarantine_reason": group.quarantine_reason,
                })
            })
            .collect::<Vec<_>>();

        serde_json::json!({
            "node_id": status.node_id.0,
            "state": status.state.as_str(),
            "pending_message_count": status.pending_message_count,
            "pending_message_bytes": status.pending_message_bytes,
            "groups": groups,
        })
    });

    Json(serde_json::json!({
        "build": {
            "version": BUILD_INFO.ragnordb_version,
            "target": BUILD_INFO.target,
            "built_at": BUILD_INFO.built_at,
            "rust_version": BUILD_INFO.rust_version,
            "features": BUILD_INFO.feature_flags,
        },
        "infra": {
            "raft": BUILD_INFO.raft_version,
            "raft_revision": BUILD_INFO.raft_revision,
            "wal": BUILD_INFO.wal_version,
            "wal_revision": BUILD_INFO.wal_revision,
            "bloom": BUILD_INFO.bloom_version,
            "bloom_revision": BUILD_INFO.bloom_revision,
        },
        "server": {
            "started_at": state.started_at,
            "max_connections": state.max_connections,
            "active_connections": active,
        },
        "durability": durability,
        "replication": replication,
        "multiraft": multiraft,
        "storage": storage.map(|storage| serde_json::json!({
            "durable_lsn": storage.durable_lsn,
            "replay_frontier": storage.replay_frontier,
            "latest_checkpoint_id": storage.latest_checkpoint_id,
            "wal_retained_bytes": storage.wal_retained_bytes,
            "retention_pins_active": storage.retention_pins_active,
            "oldest_retention_pin_lsn": storage.oldest_retention_pin_lsn,
        })),
    }))
}
