pub mod admin;
pub mod build_info;
pub mod config;
pub mod metrics;
pub mod protocol;
pub mod session;

use admin::AdminState;
use config::NodeConfig;
use protocol::{error_response, ok_response, read_frame, write_frame};
use session::Session;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

#[derive(Debug)]
pub struct Server {
    config: NodeConfig,
}

impl Server {
    pub fn new(config: NodeConfig) -> Self {
        Server { config }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let config = &self.config;

        info!(
            node_id = config.node_id.0,
            data_dir = %config.data_dir.display(),
            listen = %config.listen_addr,
            admin = %config.admin_addr,
            max_connections = config.max_connections,
            "node starting",
        );

        tokio::fs::create_dir_all(&config.data_dir).await?;
        info!("data directory ready");

        let started_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let connection_semaphore = Arc::new(Semaphore::new(config.max_connections as usize));

        let admin_state = Arc::new(AdminState {
            started_at,
            connection_semaphore: connection_semaphore.clone(),
            max_connections: config.max_connections,
        });

        let admin_addr = config.admin_addr;
        tokio::spawn(async move {
            if let Err(e) = admin::start_admin_server(admin_addr, admin_state).await {
                error!(error = %e, "admin server failed");
            }
        });

        let listener = TcpListener::bind(config.listen_addr).await?;
        info!(listen = %config.listen_addr, "listening (SQL protocol)");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            let sem = connection_semaphore.clone();
                            match sem.try_acquire() {
                                Ok(permit) => {
                                    metrics::counter_inc("RagnorDB_connections_accepted_total");
                                    let active = config.max_connections as usize - sem.available_permits();
                                    metrics::gauge_set("RagnorDB_connections_active", active as f64);

                                    info!(from = %addr, active_connections = active, "accepted connection");
                                    tokio::spawn(async move {
                                        if let Err(e) = handle_connection(stream).await {
                                            warn!(from = %addr, error = %e, "connection error");
                                        }
                                        drop(permit);
                                        let active = config.max_connections as usize - sem.available_permits();
                                        metrics::gauge_set("RagnorDB_connections_active", active as f64);
                                        info!(from = %addr, "connection closed");
                                    });
                                }
                                Err(_) => {
                                    warn!(from = %addr, max = config.max_connections, "connection limit reached, rejecting");
                                    let response = error_response(
                                        "CONNECTION_LIMIT",
                                        &format!("server has reached its configured max connection count ({})", config.max_connections),
                                        true,
                                    );
                                    if let Ok(bytes) = serde_json::to_vec(&response) {
                                        let _ = tokio::io::AsyncWriteExt::write_all(
                                            &mut tokio::io::BufWriter::new(stream),
                                            &bytes,
                                        ).await;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "accept error");
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("received SIGINT, shutting down");
                    break;
                }
            }
        }

        // admin server shuts down when the tokio runtime drops
        info!("goodbye");
        Ok(())
    }
}

/// Handle a single client connection.
///
/// Reads SQL statements line by line and returns JSON responses.
async fn handle_connection(
    stream: tokio::net::TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut reader, mut writer) = stream.into_split();
    let mut session = Session::new();

    loop {
        let sql = match read_frame(&mut reader).await {
            Ok(sql) => sql,
            Err(_) => break,
        };

        let trimmed = sql.trim().to_string();
        if trimmed.is_empty() {
            continue;
        }

        metrics::counter_inc("RagnorDB_requests_received_total");
        info!(session_id = %session.session_id.0, statement = %trimmed, "received SQL");

        let response = error_response(
            "UNSUPPORTED_SQL",
            "SQL execution not implemented yet",
            false,
        );
        metrics::counter_inc("RagnorDB_requests_error_total");

        write_frame(&mut writer, &response).await?;
    }

    Ok(())
}
