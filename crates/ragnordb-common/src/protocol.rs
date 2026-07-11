use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const LEN_SIZE: usize = 4;
const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

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
