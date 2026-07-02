use axum::{Router, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::info;

use crate::build_info::BUILD_INFO;
use crate::metrics;

pub struct AdminState {
    pub started_at: u64,
    pub connection_semaphore: Arc<Semaphore>,
    pub max_connections: u32,
}

pub async fn start_admin_server(
    addr: SocketAddr,
    state: Arc<AdminState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/status", get(handle_status))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(admin_addr = %addr, "admin HTTP server listening");

    axum::serve(listener, app).await?;

    Ok(())
}

async fn handle_metrics() -> String {
    metrics::render_metrics()
}

async fn handle_status(
    axum::extract::State(state): axum::extract::State<Arc<AdminState>>,
) -> String {
    let active = state.max_connections as usize - state.connection_semaphore.available_permits();

    serde_json::json!({
        "build": {
            "version": BUILD_INFO.ragnordb_version,
            "target": BUILD_INFO.target,
            "built_at": BUILD_INFO.built_at,
            "rust_version": BUILD_INFO.rust_version,
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
    })
    .to_string()
}
