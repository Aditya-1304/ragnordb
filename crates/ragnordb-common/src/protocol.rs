use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const LEN_SIZE: usize = 4;
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// V1 client wire protocol: length-prefixed TCP frames
///
/// Request wire format:
///   [len: u32 little-endian][UTF-8 SQL bytes]
///
/// Response wire format:
///   [len: u32 little-endian][UTF-8 JSON bytes]
///
/// MAX_FRAME_SIZE = 16 MiB - prevents memory exhaustion on
/// oversized frames from misbehaving clients
pub async fn read_frame<R>(reader: &mut R) -> Result<String, Box<dyn std::error::Error>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; LEN_SIZE];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_FRAME_SIZE {
        return Err(format!("frame size {len} exceeds maximum of {MAX_FRAME_SIZE}").into());
    }

    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;

    Ok(String::from_utf8(buf)?)
}

pub async fn write_frame<W>(
    writer: &mut W,
    response: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>>
where
    W: AsyncWrite + Unpin,
{
    let bytes = serde_json::to_vec(response)?;
    let len = bytes.len() as u32;

    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncWriteExt, duplex};

    #[tokio::test]
    async fn request_frame_round_trip() {
        let (mut client, mut server) = duplex(1024);

        let writer = tokio::spawn(async move {
            let sql = b"SELECT * FROM users";
            client
                .write_all(&(sql.len() as u32).to_le_bytes())
                .await
                .unwrap();
            client.write_all(sql).await.unwrap();
        });

        let decoded = read_frame(&mut server).await.unwrap();

        writer.await.unwrap();
        assert_eq!(decoded, "SELECT * FROM users");
    }

    #[tokio::test]
    async fn response_frame_round_trip() {
        let (mut server, mut client) = duplex(1024);

        let expected = json!({
            "ok": false,
            "error": {
                "code": "UNSUPPORTED_SQL",
                "message": "unsupported",
                "retryable": false
            }
        });

        let response = expected.clone();

        let writer = tokio::spawn(async move {
            write_frame(&mut server, &response).await.unwrap();
        });

        let decoded = read_frame(&mut client).await.unwrap();

        writer.await.unwrap();

        let decoded: serde_json::Value = serde_json::from_str(&decoded).unwrap();

        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn rejects_oversized_frame_before_allocating_payload() {
        let (mut client, mut server) = duplex(16);

        client
            .write_all(&((MAX_FRAME_SIZE as u32) + 1).to_le_bytes())
            .await
            .unwrap();

        let error = read_frame(&mut server).await.unwrap_err();

        assert!(error.to_string().contains("exceeds maximum"));
    }

    #[tokio::test]
    async fn rejects_non_utf8_request_payload() {
        let (mut client, mut server) = duplex(16);
        let invalid_utf8 = [0xff, 0xfe];

        client
            .write_all(&(invalid_utf8.len() as u32).to_le_bytes())
            .await
            .unwrap();
        client.write_all(&invalid_utf8).await.unwrap();

        assert!(read_frame(&mut server).await.is_err());
    }
}
