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
    let database = state.database.lock().await;
    let storage = database.status();
    drop(database);

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
        "storage": {
            "durable_lsn": storage.durable_lsn,
            "replay_frontier": storage.replay_frontier,
            "latest_checkpoint_id": storage.latest_checkpoint_id,
            "wal_retained_bytes": storage.wal_retained_bytes,
            "retention_pins_active": storage.retention_pins_active,
            "oldest_retention_pin_lsn": storage.oldest_retention_pin_lsn,
        },
    }))
}
