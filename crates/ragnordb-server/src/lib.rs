pub mod config;
use config::NodeConfig;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

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

        println!("  RagnorDB Node  ");
        println!("  Node ID:       {}", config.node_id.0);
        println!("  Data directory: {}", config.data_dir.display());
        println!("  SQL listen:     {}", config.listen_addr);
        println!("  Admin HTTP:     {}", config.admin_addr);
        println!("  Max connections: {}", config.max_connections);

        tokio::fs::create_dir_all(&config.data_dir).await?;
        println!("[server] data directory ready");

        let listener = TcpListener::bind(config.listen_addr).await?;
        println!(
            "[server] listening on {} (SQL protocol)",
            config.listen_addr
        );

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            println!("[server] accepted connection from {addr}");
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream).await {
                                    eprintln!("[connection] {addr} error: {e}");
                                }
                                println!("[server] connection closed: {addr}");
                            });
                        }
                        Err(e) => {
                            eprintln!("[server] accept error: {e}");

                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    println!("\n[server] received SIGINT, shutting down...");
                    break;
                }
            }
        }

        println!("[server] goodbye");
        Ok(())
    }
}

/// this is to handle a single client connection.
///
/// reads SQL statements line by line  and returns
/// JSON responses
async fn handle_connection(
    stream: tokio::net::TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;

        // this means the client closed the connection
        if bytes_read == 0 {
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        println!("[handler] received: {trimmed}");

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
