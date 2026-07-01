pub mod build_info;
pub mod config;
pub mod protocol;
pub mod service;

use config::NodeConfig;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
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

        let listener = TcpListener::bind(config.listen_addr).await?;
        info!(listen = %config.listen_addr, "listening (SQL protocol)");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            info!(from = %addr, "accepted connection");
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream).await {
                                    warn!(from = %addr, error = %e, "connection error");
                                }
                                info!(from = %addr, "connection closed");
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "accept error");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("received SIGINT, shutting down");
                    break;
                }
            }
        }

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
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        info!(statement = %trimmed, "received SQL");

        let response = serde_json::json!({
            "ok": false,
            "error": {
                "code": "UNSUPPORTED_SQL",
                "message": "SQL execution not implemented yet",
                "retryable": false
            }
        });

        let response_bytes = serde_json::to_vec(&response)?;
        writer.write_all(&response_bytes).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

    Ok(())
}
