use axum::{Json, Router, http::header, response::IntoResponse, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::build_info::BUILD_INFO;
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
            "wal": BUILD_INFO.wal_version,
            "bloom": BUILD_INFO.bloom_version,
        },
        "server": {
            "started_at": state.started_at,
            "max_connections": state.max_connections,
            "active_connections": active,
        }
    }))
}
