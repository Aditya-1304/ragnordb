use serde_json::Value;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

const LEN_SIZE: usize = 4;

pub async fn read_frame(reader: &mut OwnedReadHalf) -> Result<String, Box<dyn std::error::Error>> {
    let mut len_buf = [0u8; LEN_SIZE];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut buffer = vec![0u8; len];
    reader.read_exact(&mut buffer).await?;

    Ok(String::from_utf8(buffer)?)
}

pub async fn write_frame(
    writer: &mut OwnedWriteHalf,
    response: &Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec(response)?;
    let len = bytes.len() as u32;

    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;

    Ok(())
}

pub fn ok_response(columns: Vec<&str>, rows: Vec<Vec<serde_json::Value>>) -> Value {
    serde_json::json!({
        "ok": true,
        "columns": columns,
        "rows": rows,
        "stats": {
            "rows_read": 0,
            "rows_written": 0
        }
    })
}

pub fn error_response(code: &str, message: &str, retryable: bool) -> Value {
    serde_json::json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message,
            "retryable": retryable
        }
    })
}
