use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub use crate::result::StreamingResultFrame;

const LEN_SIZE: usize = 4;
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
const V2_MAGIC: &[u8; 4] = b"RDB2";
const V2_STREAMING_MAGIC: &[u8; 4] = b"RDBS";

pub const MAX_STREAMING_BATCH_ROWS: u32 = 65_536;
pub const MAX_STREAMING_BATCH_BYTES: u32 = 8 * 1024 * 1024;

/// Builds one length-prefixed frame while enforcing the body limit before
/// serialized JSON bytes are appended to the output buffer.
struct BoundedFrameBodyWriter {
    buffer: Vec<u8>,
    max_body_bytes: usize,
}

impl BoundedFrameBodyWriter {
    fn new(max_body_bytes: usize) -> io::Result<Self> {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(LEN_SIZE)
            .map_err(|error| io::Error::other(error.to_string()))?;
        buffer.resize(LEN_SIZE, 0);

        Ok(Self {
            buffer,
            max_body_bytes,
        })
    }
}

impl Write for BoundedFrameBodyWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let current_body_len =
            self.buffer.len().checked_sub(LEN_SIZE).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "missing frame prefix")
            })?;
        let next_body_len = current_body_len
            .checked_add(bytes.len())
            .filter(|length| *length <= self.max_body_bytes)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "streaming result frame body exceeds maximum of {} bytes",
                        self.max_body_bytes
                    ),
                )
            })?;
        let required_capacity = LEN_SIZE.checked_add(next_body_len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "streaming frame size overflowed",
            )
        })?;
        let maximum_capacity = LEN_SIZE.checked_add(self.max_body_bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "streaming frame limit overflowed",
            )
        })?;

        if required_capacity > self.buffer.capacity() {
            // Grow geometrically for normal serialization, but never request
            // capacity beyond the length prefix plus the configured body cap.
            let target_capacity = self
                .buffer
                .capacity()
                .saturating_mul(2)
                .max(required_capacity)
                .min(maximum_capacity);
            let additional = target_capacity.saturating_sub(self.buffer.len());
            self.buffer
                .try_reserve_exact(additional)
                .map_err(|error| io::Error::other(error.to_string()))?;
        }

        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Client-owned V2 request envelope. The root identity is preserved across
/// gateways and topology changes; only the tablet route is allowed to change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRequestV2 {
    pub protocol_version: u16,
    pub client_id: u128,
    pub client_session_epoch: u64,
    pub request_sequence: u64,
    pub acknowledged_through: Option<u64>,
    pub statement_timeout_ms: u64,
    pub sql: String,
}

impl ClientRequestV2 {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.protocol_version != 2 {
            return Err("unsupported client request protocol version");
        }
        if self.client_id == 0 {
            return Err("client ID must be non-zero");
        }
        if self.client_session_epoch == 0 {
            return Err("client session epoch must be non-zero");
        }
        if self.request_sequence == 0 {
            return Err("client request sequence must be non-zero");
        }
        if self
            .acknowledged_through
            .is_some_and(|acknowledged| acknowledged > self.request_sequence)
        {
            return Err("acknowledged request sequence is ahead of the request");
        }
        if self.statement_timeout_ms == 0 {
            return Err("client statement timeout must be non-zero");
        }
        if self.sql.trim().is_empty() {
            return Err("client SQL must not be empty");
        }
        Ok(())
    }
}

/// Opt-in V2 request envelope for bounded result streaming. This is a
/// separate type and magic so the established ClientRequestV2 JSON bytes and
/// field set remain unchanged for existing clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRequestV2Streaming {
    pub protocol_version: u16,
    pub client_id: u128,
    pub client_session_epoch: u64,
    pub request_sequence: u64,
    pub acknowledged_through: Option<u64>,
    pub statement_timeout_ms: u64,
    pub sql: String,
    pub max_rows_per_batch: u32,
    pub max_bytes_per_batch: u32,
}

impl ClientRequestV2Streaming {
    pub fn validate(&self) -> Result<(), &'static str> {
        ClientRequestV2 {
            protocol_version: self.protocol_version,
            client_id: self.client_id,
            client_session_epoch: self.client_session_epoch,
            request_sequence: self.request_sequence,
            acknowledged_through: self.acknowledged_through,
            statement_timeout_ms: self.statement_timeout_ms,
            sql: self.sql.clone(),
        }
        .validate()?;
        if self.max_rows_per_batch == 0 || self.max_rows_per_batch > MAX_STREAMING_BATCH_ROWS {
            return Err("streaming max_rows_per_batch is outside the allowed range");
        }
        if self.max_bytes_per_batch == 0 || self.max_bytes_per_batch > MAX_STREAMING_BATCH_BYTES {
            return Err("streaming max_bytes_per_batch is outside the allowed range");
        }
        Ok(())
    }
}

/// Wire-level request accepted by the server without breaking the V1 SQL
/// framing used by the existing shell and compatibility clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRequestFrame {
    V1(String),
    V2(ClientRequestV2),
    V2Streaming(ClientRequestV2Streaming),
}

pub async fn read_client_frame<R>(
    reader: &mut R,
) -> Result<ClientRequestFrame, Box<dyn std::error::Error>>
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
    if buf.starts_with(V2_MAGIC) {
        let request: ClientRequestV2 = serde_json::from_slice(&buf[V2_MAGIC.len()..])?;
        request
            .validate()
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
        Ok(ClientRequestFrame::V2(request))
    } else if buf.starts_with(V2_STREAMING_MAGIC) {
        let request: ClientRequestV2Streaming =
            serde_json::from_slice(&buf[V2_STREAMING_MAGIC.len()..])?;
        request
            .validate()
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
        Ok(ClientRequestFrame::V2Streaming(request))
    } else {
        Ok(ClientRequestFrame::V1(String::from_utf8(buf)?))
    }
}

pub fn encode_client_request_v2(
    request: &ClientRequestV2,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    request
        .validate()
        .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
    let body = serde_json::to_vec(request)?;
    let total = V2_MAGIC.len() + body.len();
    if total > MAX_FRAME_SIZE {
        return Err(
            format!("V2 request frame size {total} exceeds maximum of {MAX_FRAME_SIZE}").into(),
        );
    }
    let mut frame = Vec::with_capacity(LEN_SIZE + total);
    frame.extend_from_slice(&u32::try_from(total)?.to_le_bytes());
    frame.extend_from_slice(V2_MAGIC);
    frame.extend_from_slice(&body);
    Ok(frame)
}

pub fn encode_client_request_v2_streaming(
    request: &ClientRequestV2Streaming,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    request
        .validate()
        .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
    let body = serde_json::to_vec(request)?;
    let total = V2_STREAMING_MAGIC.len() + body.len();
    if total > MAX_FRAME_SIZE {
        return Err(format!(
            "streaming V2 request frame size {total} exceeds maximum of {MAX_FRAME_SIZE}"
        )
        .into());
    }
    let mut frame = Vec::with_capacity(LEN_SIZE + total);
    frame.extend_from_slice(&u32::try_from(total)?.to_le_bytes());
    frame.extend_from_slice(V2_STREAMING_MAGIC);
    frame.extend_from_slice(&body);
    Ok(frame)
}

pub fn encode_streaming_result_frame(
    frame: &StreamingResultFrame,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut output = BoundedFrameBodyWriter::new(MAX_FRAME_SIZE)?;
    serde_json::to_writer(&mut output, frame)?;

    let body_len = output.buffer.len() - LEN_SIZE;
    let encoded_body_len = u32::try_from(body_len)?;
    output.buffer[..LEN_SIZE].copy_from_slice(&encoded_body_len.to_le_bytes());

    Ok(output.buffer)
}
pub fn decode_streaming_result_frame(
    frame: &[u8],
) -> Result<StreamingResultFrame, Box<dyn std::error::Error>> {
    if frame.len() < LEN_SIZE {
        return Err("streaming result frame is missing its length prefix".into());
    }
    let body_len = u32::from_le_bytes(frame[..LEN_SIZE].try_into()?) as usize;
    if body_len > MAX_FRAME_SIZE {
        return Err(format!(
            "streaming result frame size {body_len} exceeds maximum of {MAX_FRAME_SIZE}"
        )
        .into());
    }
    if frame.len() != LEN_SIZE + body_len {
        return Err("streaming result frame length does not match its payload".into());
    }
    Ok(serde_json::from_slice(&frame[LEN_SIZE..])?)
}

pub async fn read_streaming_result_frame<R>(
    reader: &mut R,
) -> Result<StreamingResultFrame, Box<dyn std::error::Error>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; LEN_SIZE];
    reader.read_exact(&mut len_buf).await?;
    let body_len = u32::from_le_bytes(len_buf) as usize;
    if body_len > MAX_FRAME_SIZE {
        return Err(format!(
            "streaming result frame size {body_len} exceeds maximum of {MAX_FRAME_SIZE}"
        )
        .into());
    }

    let mut encoded = Vec::with_capacity(LEN_SIZE + body_len);
    encoded.extend_from_slice(&len_buf);
    encoded.resize(LEN_SIZE + body_len, 0);
    reader.read_exact(&mut encoded[LEN_SIZE..]).await?;
    decode_streaming_result_frame(&encoded)
}

pub async fn write_streaming_result_frame<W>(
    writer: &mut W,
    frame: &StreamingResultFrame,
) -> Result<(), Box<dyn std::error::Error>>
where
    W: AsyncWrite + Unpin,
{
    let encoded = encode_streaming_result_frame(frame)?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

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

    if bytes.len() > MAX_FRAME_SIZE {
        return Err(format!(
            "response frame size {} exceeds maximum of {MAX_FRAME_SIZE}",
            bytes.len()
        )
        .into());
    }

    let mut frame = Vec::with_capacity(LEN_SIZE + bytes.len());
    frame.extend_from_slice(&u32::try_from(bytes.len())?.to_le_bytes());
    frame.extend_from_slice(&bytes);

    writer.write_all(&frame).await?;
    writer.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    #[derive(Default)]
    struct WriteRecorder {
        writes: Vec<Vec<u8>>,
        flushes: usize,
    }

    impl AsyncWrite for WriteRecorder {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.writes.push(bytes.to_vec());
            Poll::Ready(Ok(bytes.len()))
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            self.flushes += 1;
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

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

    /// A length prefix and a small JSON response written separately can trigger
    /// TCP delayed-ACK/Nagle latency on request-response connections. One frame
    /// must reach the socket writer as one contiguous write.
    #[tokio::test]
    async fn response_frame_is_emitted_in_one_write() {
        let response = json!({ "ok": true, "result": "ready" });
        let expected_body = serde_json::to_vec(&response).unwrap();
        let mut writer = WriteRecorder::default();

        write_frame(&mut writer, &response).await.unwrap();

        assert_eq!(writer.writes.len(), 1);
        assert_eq!(writer.flushes, 1);
        assert_eq!(
            writer.writes[0],
            [
                u32::try_from(expected_body.len())
                    .unwrap()
                    .to_le_bytes()
                    .as_slice(),
                expected_body.as_slice()
            ]
            .concat()
        );
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

    #[tokio::test]
    async fn v2_request_round_trip_preserves_logical_retry_identity() {
        let request = ClientRequestV2 {
            protocol_version: 2,
            client_id: 17,
            client_session_epoch: 4,
            request_sequence: 9,
            acknowledged_through: Some(8),
            statement_timeout_ms: 30_000,
            sql: "INSERT INTO users (id) VALUES (1)".to_string(),
        };
        let encoded = encode_client_request_v2(&request).unwrap();
        let (mut writer, mut reader) = duplex(4096);
        writer.write_all(&encoded).await.unwrap();

        assert_eq!(
            read_client_frame(&mut reader).await.unwrap(),
            ClientRequestFrame::V2(request)
        );
    }

    #[tokio::test]
    async fn v2_request_rejects_acknowledgement_beyond_root_sequence() {
        let request = ClientRequestV2 {
            protocol_version: 2,
            client_id: 1,
            client_session_epoch: 1,
            request_sequence: 2,
            acknowledged_through: Some(3),
            statement_timeout_ms: 30_000,
            sql: "SELECT 1".to_string(),
        };

        assert!(encode_client_request_v2(&request).is_err());
    }

    #[tokio::test]
    async fn opt_in_v2_streaming_request_uses_distinct_magic_and_roundtrips() {
        let request = ClientRequestV2Streaming {
            protocol_version: 2,
            client_id: 17,
            client_session_epoch: 4,
            request_sequence: 9,
            acknowledged_through: Some(8),
            statement_timeout_ms: 30_000,
            sql: "SELECT * FROM users".to_string(),
            max_rows_per_batch: 128,
            max_bytes_per_batch: 64 * 1024,
        };
        let encoded = encode_client_request_v2_streaming(&request).unwrap();
        assert_ne!(&encoded[LEN_SIZE..LEN_SIZE + V2_MAGIC.len()], V2_MAGIC);

        let (mut writer, mut reader) = duplex(4096);
        writer.write_all(&encoded).await.unwrap();

        assert_eq!(
            read_client_frame(&mut reader).await.unwrap(),
            ClientRequestFrame::V2Streaming(request)
        );
    }

    #[test]
    fn streaming_result_frames_are_tagged_and_keep_error_distinct_from_successful_end() {
        let frames = [
            StreamingResultFrame::ResultStart {
                columns: vec!["id".to_string()],
                read_ts: 100,
            },
            StreamingResultFrame::RowBatch {
                rows: vec![b"row-1".to_vec()],
                row_count: 1,
                byte_count: 5,
            },
            StreamingResultFrame::ResultError {
                code: "DISTRIBUTED_SCAN_FAILED".to_string(),
                message: "tablet 12 unavailable".to_string(),
                retryable: true,
                rows_emitted: 1,
            },
        ];

        let encoded = encode_streaming_result_frame(&frames[2]).unwrap();
        let decoded = decode_streaming_result_frame(&encoded).unwrap();

        assert_eq!(decoded, frames[2]);
        let start_json = serde_json::to_value(&frames[0]).unwrap();
        assert_eq!(start_json["type"], "result_start");
        assert_eq!(start_json["read_ts"], 100);
        assert!(matches!(&decoded, StreamingResultFrame::ResultError { .. }));
        assert!(!matches!(&decoded, StreamingResultFrame::ResultEnd { .. }));
    }

    #[test]
    fn streaming_request_rejects_zero_or_oversized_batch_caps() {
        let mut request = ClientRequestV2Streaming {
            protocol_version: 2,
            client_id: 17,
            client_session_epoch: 4,
            request_sequence: 9,
            acknowledged_through: None,
            statement_timeout_ms: 30_000,
            sql: "SELECT * FROM users".to_string(),
            max_rows_per_batch: 128,
            max_bytes_per_batch: 64 * 1024,
        };

        request.max_rows_per_batch = 0;
        assert_eq!(
            request.validate(),
            Err("streaming max_rows_per_batch is outside the allowed range")
        );

        request.max_rows_per_batch = 128;
        request.max_bytes_per_batch = MAX_STREAMING_BATCH_BYTES + 1;
        assert_eq!(
            request.validate(),
            Err("streaming max_bytes_per_batch is outside the allowed range")
        );
    }

    /// Realistic bug caught:
    ///
    /// A materialized result larger than the protocol limit could truncate its
    /// usize length into u32 and place a malformed frame on the connection.
    #[tokio::test]
    async fn rejects_oversized_response_before_writing_a_length_prefix() {
        let (mut server, mut client) = duplex(16);
        let response = json!({ "value": "x".repeat(MAX_FRAME_SIZE) });

        let error = write_frame(&mut server, &response).await.unwrap_err();

        assert!(error.to_string().contains("response frame size"));

        drop(server);
        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        assert!(received.is_empty());
    }
}
