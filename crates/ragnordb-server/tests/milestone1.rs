use ragnordb_common::protocol::read_frame;
use ragnordb_server::admin::AdminState;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

async fn write_sql_frame<W>(writer: &mut W, sql: &str) -> Result<(), Box<dyn std::error::Error>>
where
    W: AsyncWrite + Unpin,
{
    let bytes = sql.as_bytes();
    let len = bytes.len() as u32;

    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(bytes).await?;
    writer.flush().await?;

    Ok(())
}

async fn read_http_body(
    addr: SocketAddr,
    path: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");

    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut response = String::new();
    stream.read_to_string(&mut response).await?;

    let (_header, body) = response
        .split_once("\r\n\r\n")
        .ok_or("HTTP response missing header/body separator")?;

    Ok(body.to_string())
}

#[tokio::test]
async fn sql_connection_returns_framed_json_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        ragnordb_server::handle_connection(stream).await.unwrap();
    });

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();

    write_sql_frame(&mut writer, "SELECT 1").await.unwrap();

    let response = read_frame(&mut reader).await.unwrap();
    let json: serde_json::Value = serde_json::from_str(&response).unwrap();

    assert_eq!(json["ok"], false);
    assert_eq!(json["error"]["code"], "UNSUPPORTED_SQL");
    assert_eq!(json["error"]["retryable"], false);

    drop(writer);
    server_task.await.unwrap();
}

#[tokio::test]
async fn admin_status_returns_json() {
    ragnordb_server::metrics::init_metrics();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();

    let state = Arc::new(AdminState {
        started_at: 123,
        connection_semaphore: Arc::new(Semaphore::new(10)),
        max_connections: 10,
    });

    let server_task = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            ragnordb_server::admin::serve_admin(listener, state, shutdown)
                .await
                .unwrap();
        })
    };

    let body = read_http_body(addr, "/status").await.unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert_eq!(json["server"]["started_at"], 123);
    assert_eq!(json["server"]["max_connections"], 10);
    assert_eq!(json["server"]["active_connections"], 0);
    assert!(json["build"]["version"].is_string());
    assert!(json["infra"]["raft"].is_string());

    shutdown.cancel();
    server_task.await.unwrap();
}

#[tokio::test]
async fn admin_metrics_returns_prometheus_text() {
    ragnordb_server::metrics::init_metrics();
    ragnordb_server::metrics::counter_inc("RagnorDB_requests_received_total");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();

    let state = Arc::new(AdminState {
        started_at: 123,
        connection_semaphore: Arc::new(Semaphore::new(10)),
        max_connections: 10,
    });

    let server_task = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            ragnordb_server::admin::serve_admin(listener, state, shutdown)
                .await
                .unwrap();
        })
    };

    let body = read_http_body(addr, "/metrics").await.unwrap();

    assert!(body.contains("RagnorDB_requests_received_total"));

    shutdown.cancel();
    server_task.await.unwrap();
}
