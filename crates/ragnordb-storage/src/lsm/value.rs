//! Explicit, versioned encodings for records stored by the tablet LSM.
//!
//! V1 bytes are storage contracts. Tags and integer byte order are assigned
//! explicitly here; Rust enum layout, native struct layout, and serde formats
//! are never persisted.

use ragnordb_common::{
    Error, Result,
    codec::{LockRecord, TxnStatus, TxnStatusRecord, WriteKind, WriteRecord},
    command_codec::{
        CachedTabletCommandOutcome, CachedTabletCommandRejectionKind, CachedTabletCommandResult,
    },
    encoding::decode_row,
    ids::{Timestamp, TxnId},
};

use crate::key::decode_row_key;

const FORMAT_V1: u8 = 0x01;
const TAG_DEFAULT_ROW: u8 = 0x01;
const TAG_WRITE_PUT: u8 = 0x10;
const TAG_WRITE_DELETE: u8 = 0x11;
const TAG_WRITE_ROLLBACK: u8 = 0x12;
const TAG_LOCK: u8 = 0x20;
const TAG_TXN_PENDING: u8 = 0x30;
const TAG_TXN_COMMITTED: u8 = 0x31;
const TAG_TXN_ABORTED: u8 = 0x32;
const TAG_LOGICAL_OUTCOME_APPLIED: u8 = 0x40;
const TAG_LOGICAL_OUTCOME_REJECTED: u8 = 0x41;
const TAG_LEGACY_OUTCOME_APPLIED: u8 = 0x42;
const TAG_LEGACY_OUTCOME_REJECTED: u8 = 0x43;
const TAG_RETRY_FLOOR: u8 = 0x50;

/// Maximum complete encoded value accepted by the V1 codec.
///
/// The command admission layer uses the same ceiling for a complete delta;
/// keeping the per-value codec bounded prevents malformed lengths from
/// causing unbounded allocation during decode.
pub const MAX_VALUE_V1_BYTES: usize = 64 * 1024 * 1024;
const MAX_PARTICIPANTS: usize = 4096;
const MAX_REJECTION_REASON_BYTES: usize = 4096;

/// Logical record value encoded using the frozen V1 envelope and payloads.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueRecordV1 {
    /// Canonical encoded SQL row stored by a Default-family key.
    DefaultRow(Vec<u8>),
    /// MVCC write metadata; the envelope tag records the write operation.
    Write(WriteRecord),
    /// A validated unresolved transaction intent.
    Lock(LockRecord),
    /// A primary transaction decision and its lease/participant metadata.
    TransactionStatus(TxnStatusRecord),
    /// A retained route-independent command outcome.
    LogicalOutcome(CachedTabletCommandOutcome),
    /// A retained legacy client sequence outcome.
    LegacyOutcome {
        last_sequence_applied: u64,
        outcome: CachedTabletCommandOutcome,
    },
    /// A durable V2 retry floor. The outcome records at or below this floor
    /// may be retired because retries must remain expired after recovery.
    RetryFloor {
        client_id: u128,
        session_epoch: u64,
        acknowledged_through: u64,
    },
}

/// Encode one value into its canonical V1 byte representation.
pub fn encode_value_v1(value: &ValueRecordV1) -> Result<Vec<u8>> {
    let (tag, payload) = encode_payload(value)?;
    let mut encoder = Encoder::new();
    encoder.write_u8(FORMAT_V1)?;
    encoder.write_u8(tag)?;
    encoder.write_bytes(&payload)?;
    Ok(encoder.into_vec())
}

/// Decode exactly one canonical V1 value. Trailing bytes are rejected.
pub fn decode_value_v1(bytes: &[u8]) -> Result<ValueRecordV1> {
    if bytes.len() > MAX_VALUE_V1_BYTES {
        return Err(corrupt("encoded value exceeds the V1 maximum size"));
    }
    let mut decoder = Decoder::new(bytes);
    let version = decoder.read_u8()?;
    if version != FORMAT_V1 {
        return Err(corrupt("unsupported storage value format version"));
    }
    let tag = decoder.read_u8()?;
    let value = decode_payload(tag, &mut decoder)?;
    decoder.finish()?;
    Ok(value)
}

fn encode_payload(value: &ValueRecordV1) -> Result<(u8, Vec<u8>)> {
    let mut payload = Encoder::new();
    let tag = match value {
        ValueRecordV1::DefaultRow(row) => {
            decode_row(row).map_err(|error| {
                Error::InvalidArgument(format!("default value is not a canonical row: {error}"))
            })?;
            payload.write_framed_bytes(row)?;
            TAG_DEFAULT_ROW
        }
        ValueRecordV1::Write(write) => {
            write
                .validate()
                .map_err(|reason| Error::InvalidArgument(reason.to_string()))?;
            payload.write_u64(write.start_timestamp.0)?;
            payload.write_u64(write.commit_timestamp.0)?;
            match write.op {
                WriteKind::Put => TAG_WRITE_PUT,
                WriteKind::Delete => TAG_WRITE_DELETE,
                WriteKind::Rollback => TAG_WRITE_ROLLBACK,
            }
        }
        ValueRecordV1::Lock(lock) => {
            lock.validate()
                .map_err(|reason| Error::InvalidArgument(reason.to_string()))?;
            validate_row_key(&lock.primary_key)?;
            payload.write_u64(lock.txn_id.0)?;
            payload.write_framed_bytes(&lock.primary_key)?;
            payload.write_u64(lock.start_timestamp.0)?;
            payload.write_u64(lock.ttl_ms)?;
            payload.write_u8(encode_live_write_kind(lock.op)?)?;
            TAG_LOCK
        }
        ValueRecordV1::TransactionStatus(status) => {
            validate_status(status)?;
            payload.write_u64(status.txn_id.0)?;
            payload.write_u64(status.start_timestamp.0)?;
            payload.write_optional_u64(status.commit_timestamp.map(|value| value.0))?;
            payload.write_framed_bytes(&status.primary_key)?;
            payload.write_len(status.participant_tablet_ids.len())?;
            for participant in &status.participant_tablet_ids {
                payload.write_u64(*participant)?;
            }
            payload.write_optional_u64(status.last_heartbeat_timestamp.map(|value| value.0))?;
            payload.write_optional_u64(status.lease_deadline_ms)?;
            match status.status {
                TxnStatus::Pending => TAG_TXN_PENDING,
                TxnStatus::Committed => TAG_TXN_COMMITTED,
                TxnStatus::Aborted => TAG_TXN_ABORTED,
            }
        }
        ValueRecordV1::LogicalOutcome(outcome) => encode_outcome(outcome, false, &mut payload)?,
        ValueRecordV1::LegacyOutcome {
            last_sequence_applied,
            outcome,
        } => {
            if *last_sequence_applied == 0 {
                return Err(Error::InvalidArgument(
                    "legacy outcome sequence must be non-zero".to_string(),
                ));
            }
            payload.write_u64(*last_sequence_applied)?;
            encode_outcome(outcome, true, &mut payload)?
        }
        ValueRecordV1::RetryFloor {
            client_id,
            session_epoch,
            acknowledged_through,
        } => {
            if *client_id == 0 || *session_epoch == 0 || *acknowledged_through == 0 {
                return Err(Error::InvalidArgument(
                    "retry floor identity and acknowledged sequence must be non-zero".to_string(),
                ));
            }
            payload.write_u128(*client_id)?;
            payload.write_u64(*session_epoch)?;
            payload.write_u64(*acknowledged_through)?;
            TAG_RETRY_FLOOR
        }
    };
    Ok((tag, payload.into_vec()))
}

fn encode_outcome(
    outcome: &CachedTabletCommandOutcome,
    legacy: bool,
    payload: &mut Encoder,
) -> Result<u8> {
    match outcome {
        CachedTabletCommandOutcome::Applied(result) => {
            payload.write_u8(encode_result(*result))?;
            Ok(if legacy {
                TAG_LEGACY_OUTCOME_APPLIED
            } else {
                TAG_LOGICAL_OUTCOME_APPLIED
            })
        }
        CachedTabletCommandOutcome::Rejected(rejection) => {
            let kind = match rejection.kind {
                CachedTabletCommandRejectionKind::InvalidCommand => 1,
                CachedTabletCommandRejectionKind::WriteConflict => 2,
                CachedTabletCommandRejectionKind::UnsupportedCommand => 3,
            };
            if rejection.reason.len() > MAX_REJECTION_REASON_BYTES {
                return Err(Error::InvalidArgument(
                    "cached rejection diagnostic exceeds the V1 limit".to_string(),
                ));
            }
            payload.write_u8(kind)?;
            payload.write_framed_bytes(rejection.reason.as_bytes())?;
            Ok(if legacy {
                TAG_LEGACY_OUTCOME_REJECTED
            } else {
                TAG_LOGICAL_OUTCOME_REJECTED
            })
        }
    }
}

fn decode_payload(tag: u8, decoder: &mut Decoder<'_>) -> Result<ValueRecordV1> {
    let value = match tag {
        TAG_DEFAULT_ROW => {
            let row = decoder.read_framed_bytes()?.to_vec();
            decode_row(&row)
                .map_err(|error| corrupt(format!("default value row is malformed: {error}")))?;
            ValueRecordV1::DefaultRow(row)
        }
        TAG_WRITE_PUT | TAG_WRITE_DELETE | TAG_WRITE_ROLLBACK => {
            let start_timestamp = Timestamp(decoder.read_u64()?);
            let commit_timestamp = Timestamp(decoder.read_u64()?);
            let op = match tag {
                TAG_WRITE_PUT => WriteKind::Put,
                TAG_WRITE_DELETE => WriteKind::Delete,
                TAG_WRITE_ROLLBACK => WriteKind::Rollback,
                _ => unreachable!(),
            };
            let write = WriteRecord {
                start_timestamp,
                commit_timestamp,
                op,
            };
            write
                .validate()
                .map_err(|reason| corrupt(reason.to_string()))?;
            ValueRecordV1::Write(write)
        }
        TAG_LOCK => {
            let lock = LockRecord {
                txn_id: TxnId(decoder.read_u64()?),
                primary_key: decoder.read_framed_bytes()?.to_vec(),
                start_timestamp: Timestamp(decoder.read_u64()?),
                ttl_ms: decoder.read_u64()?,
                op: WriteKind::Put,
            };
            // V1 stores the operation in a fixed payload byte so Lock's tag
            // remains stable while preserving Put/Delete semantics.
            let op = decoder.read_u8()?;
            let lock = LockRecord {
                op: decode_live_write_kind(op)?,
                ..lock
            };
            validate_lock(&lock).map_err(|error| corrupt(error.to_string()))?;
            ValueRecordV1::Lock(lock)
        }
        TAG_TXN_PENDING | TAG_TXN_COMMITTED | TAG_TXN_ABORTED => {
            let txn_id = TxnId(decoder.read_u64()?);
            let start_timestamp = Timestamp(decoder.read_u64()?);
            let commit_timestamp = decoder.read_optional_u64()?.map(Timestamp);
            let primary_key = decoder.read_framed_bytes()?.to_vec();
            let participant_count = decoder.read_len()?;
            if participant_count == 0 || participant_count > MAX_PARTICIPANTS {
                return Err(corrupt("transaction status participant count is invalid"));
            }
            let mut participant_tablet_ids = Vec::with_capacity(participant_count);
            for _ in 0..participant_count {
                participant_tablet_ids.push(decoder.read_u64()?);
            }
            let last_heartbeat_timestamp = decoder.read_optional_u64()?.map(Timestamp);
            let lease_deadline_ms = decoder.read_optional_u64()?;
            let status = match tag {
                TAG_TXN_PENDING => TxnStatus::Pending,
                TAG_TXN_COMMITTED => TxnStatus::Committed,
                TAG_TXN_ABORTED => TxnStatus::Aborted,
                _ => unreachable!(),
            };
            let record = TxnStatusRecord {
                txn_id,
                start_timestamp,
                commit_timestamp,
                status,
                primary_key,
                participant_tablet_ids,
                last_heartbeat_timestamp,
                lease_deadline_ms,
            };
            validate_status(&record).map_err(|error| corrupt(error.to_string()))?;
            ValueRecordV1::TransactionStatus(record)
        }
        TAG_LOGICAL_OUTCOME_APPLIED | TAG_LOGICAL_OUTCOME_REJECTED => {
            ValueRecordV1::LogicalOutcome(decode_outcome(tag, decoder)?)
        }
        TAG_LEGACY_OUTCOME_APPLIED | TAG_LEGACY_OUTCOME_REJECTED => {
            let last_sequence_applied = decoder.read_u64()?;
            if last_sequence_applied == 0 {
                return Err(corrupt("legacy outcome sequence must be non-zero"));
            }
            ValueRecordV1::LegacyOutcome {
                last_sequence_applied,
                outcome: decode_outcome(tag, decoder)?,
            }
        }
        TAG_RETRY_FLOOR => {
            let client_id = decoder.read_u128()?;
            let session_epoch = decoder.read_u64()?;
            let acknowledged_through = decoder.read_u64()?;
            if client_id == 0 || session_epoch == 0 || acknowledged_through == 0 {
                return Err(corrupt("retry floor identity or sequence is zero"));
            }
            ValueRecordV1::RetryFloor {
                client_id,
                session_epoch,
                acknowledged_through,
            }
        }
        _ => return Err(corrupt("unknown or reserved storage value tag")),
    };
    Ok(value)
}

fn decode_outcome(tag: u8, decoder: &mut Decoder<'_>) -> Result<CachedTabletCommandOutcome> {
    let applied = matches!(
        tag,
        TAG_LOGICAL_OUTCOME_APPLIED | TAG_LEGACY_OUTCOME_APPLIED
    );
    if applied {
        let result = decode_result(decoder.read_u8()?)?;
        return Ok(CachedTabletCommandOutcome::Applied(result));
    }

    let kind = match decoder.read_u8()? {
        1 => CachedTabletCommandRejectionKind::InvalidCommand,
        2 => CachedTabletCommandRejectionKind::WriteConflict,
        3 => CachedTabletCommandRejectionKind::UnsupportedCommand,
        _ => return Err(corrupt("unknown or reserved cached rejection kind")),
    };
    let reason = decoder.read_framed_bytes()?;
    if reason.len() > MAX_REJECTION_REASON_BYTES {
        return Err(corrupt("cached rejection diagnostic exceeds the V1 limit"));
    }
    let reason = std::str::from_utf8(reason)
        .map_err(|_| corrupt("cached rejection diagnostic is not UTF-8"))?
        .to_owned();
    Ok(CachedTabletCommandOutcome::Rejected(
        ragnordb_common::command_codec::CachedTabletCommandRejection { kind, reason },
    ))
}

fn encode_result(result: CachedTabletCommandResult) -> u8 {
    match result {
        CachedTabletCommandResult::Noop => 1,
        CachedTabletCommandResult::SingleShardCommit => 2,
        CachedTabletCommandResult::Prewrite => 3,
        CachedTabletCommandResult::Commit => 4,
        CachedTabletCommandResult::Rollback => 5,
        CachedTabletCommandResult::ResolveIntent => 6,
        CachedTabletCommandResult::PublishAbortedTransactionStatus => 7,
        CachedTabletCommandResult::HeartbeatTransactionStatus => 8,
    }
}

fn decode_result(value: u8) -> Result<CachedTabletCommandResult> {
    match value {
        1 => Ok(CachedTabletCommandResult::Noop),
        2 => Ok(CachedTabletCommandResult::SingleShardCommit),
        3 => Ok(CachedTabletCommandResult::Prewrite),
        4 => Ok(CachedTabletCommandResult::Commit),
        5 => Ok(CachedTabletCommandResult::Rollback),
        6 => Ok(CachedTabletCommandResult::ResolveIntent),
        7 => Ok(CachedTabletCommandResult::PublishAbortedTransactionStatus),
        8 => Ok(CachedTabletCommandResult::HeartbeatTransactionStatus),
        _ => Err(corrupt("unknown or reserved cached command result")),
    }
}

fn decode_live_write_kind(tag: u8) -> Result<WriteKind> {
    match tag {
        1 => Ok(WriteKind::Put),
        2 => Ok(WriteKind::Delete),
        _ => Err(corrupt("unknown or reserved lock operation")),
    }
}

fn encode_live_write_kind(kind: WriteKind) -> Result<u8> {
    match kind {
        WriteKind::Put => Ok(1),
        WriteKind::Delete => Ok(2),
        WriteKind::Rollback => Err(Error::InvalidArgument(
            "a live lock cannot encode a rollback operation".to_string(),
        )),
    }
}

fn validate_status(status: &TxnStatusRecord) -> Result<()> {
    if status.participant_tablet_ids.len() > MAX_PARTICIPANTS {
        return Err(Error::InvalidArgument(
            "transaction status participant count exceeds the V1 limit".to_string(),
        ));
    }
    status
        .validate()
        .map_err(|reason| Error::InvalidArgument(reason.to_string()))?;
    validate_row_key(&status.primary_key)
}

fn validate_lock(lock: &LockRecord) -> Result<()> {
    lock.validate()
        .map_err(|reason| Error::InvalidArgument(reason.to_string()))?;
    validate_row_key(&lock.primary_key)
}

fn validate_row_key(key: &[u8]) -> Result<()> {
    decode_row_key(key).map_err(|error| {
        Error::InvalidArgument(format!("record primary key is malformed: {error}"))
    })?;
    Ok(())
}

fn corrupt(message: impl Into<String>) -> Error {
    Error::CorruptData(message.into())
}

struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn ensure_capacity(&self, additional: usize) -> Result<()> {
        let size = self.bytes.len().checked_add(additional).ok_or_else(|| {
            Error::InvalidArgument("storage value encoded size overflowed".to_string())
        })?;
        if size > MAX_VALUE_V1_BYTES {
            return Err(Error::InvalidArgument(
                "storage value exceeds the V1 maximum size".to_string(),
            ));
        }
        Ok(())
    }

    fn write_u8(&mut self, value: u8) -> Result<()> {
        self.ensure_capacity(1)?;
        self.bytes.push(value);
        Ok(())
    }

    fn write_u64(&mut self, value: u64) -> Result<()> {
        self.write_bytes(&value.to_be_bytes())
    }

    fn write_u128(&mut self, value: u128) -> Result<()> {
        self.write_bytes(&value.to_be_bytes())
    }

    fn write_optional_u64(&mut self, value: Option<u64>) -> Result<()> {
        match value {
            Some(value) => {
                self.write_u8(1)?;
                self.write_u64(value)
            }
            None => self.write_u8(0),
        }
    }

    fn write_len(&mut self, length: usize) -> Result<()> {
        let length = u32::try_from(length).map_err(|_| {
            Error::InvalidArgument("storage value field exceeds the V1 length limit".to_string())
        })?;
        self.write_bytes(&length.to_be_bytes())
    }

    fn write_framed_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_len(bytes.len())?;
        self.write_bytes(bytes)
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.ensure_capacity(bytes.len())?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .expect("fixed-size slice was checked");
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_u128(&mut self) -> Result<u128> {
        let bytes: [u8; 16] = self
            .read_exact(16)?
            .try_into()
            .expect("fixed-size slice was checked");
        Ok(u128::from_be_bytes(bytes))
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            _ => Err(corrupt("invalid optional integer marker")),
        }
    }

    fn read_len(&mut self) -> Result<usize> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .expect("fixed-size slice was checked");
        Ok(u32::from_be_bytes(bytes) as usize)
    }

    fn read_framed_bytes(&mut self) -> Result<&'a [u8]> {
        let length = self.read_len()?;
        self.read_exact(length)
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| corrupt("storage value field length overflowed"))?;
        if end > self.bytes.len() {
            return Err(corrupt("storage value is truncated"));
        }
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn finish(&self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(corrupt("storage value has trailing bytes"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ValueRecordV1, decode_value_v1, encode_value_v1};
    use ragnordb_common::{
        codec::{LockRecord, TxnStatus, TxnStatusRecord, Value, WriteKind, WriteRecord},
        command_codec::{
            CachedTabletCommandOutcome, CachedTabletCommandRejection,
            CachedTabletCommandRejectionKind, CachedTabletCommandResult,
        },
        ids::{TableId, Timestamp, TxnId},
    };

    use crate::key::{encode_row_key, make_row_key};

    const ROW_KEY: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x10, 0x80, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01,
    ];

    const ROW_VALUE: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
        0x00, 0x00, 0x00, 0x01, b'x',
    ];

    fn framed_row_key() -> Vec<u8> {
        let mut bytes = vec![0x00, 0x00, 0x00, 0x12];
        bytes.extend_from_slice(ROW_KEY);
        bytes
    }

    fn assert_golden(value: ValueRecordV1, expected: Vec<u8>) {
        assert_eq!(encode_value_v1(&value).unwrap(), expected);
        assert_eq!(decode_value_v1(&expected).unwrap(), value);
    }

    #[test]
    fn default_write_and_lock_values_have_stable_v1_golden_bytes() {
        assert_golden(
            ValueRecordV1::DefaultRow(ROW_VALUE.to_vec()),
            vec![
                0x01, 0x01, 0x00, 0x00, 0x00, 0x14, 0x01, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01, b'x',
            ],
        );

        for (op, tag, commit_timestamp) in [
            (WriteKind::Put, 0x10, 2),
            (WriteKind::Delete, 0x11, 2),
            (WriteKind::Rollback, 0x12, 1),
        ] {
            let value = ValueRecordV1::Write(WriteRecord {
                start_timestamp: Timestamp(1),
                commit_timestamp: Timestamp(commit_timestamp),
                op,
            });
            assert_golden(
                value,
                vec![
                    0x01,
                    tag,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x01,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    commit_timestamp as u8,
                ],
            );
        }

        let lock = ValueRecordV1::Lock(LockRecord {
            txn_id: TxnId(2),
            primary_key: ROW_KEY.to_vec(),
            start_timestamp: Timestamp(3),
            ttl_ms: 4,
            op: WriteKind::Delete,
        });
        let mut expected_lock = vec![
            0x01, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x12,
        ];
        expected_lock.extend_from_slice(ROW_KEY);
        expected_lock.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x04, 0x02,
        ]);
        assert_golden(lock, expected_lock);
    }

    #[test]
    fn transaction_status_values_have_stable_v1_golden_bytes() {
        let cases = [
            (TxnStatus::Pending, None, None, None, 0x30, vec![0x00]),
            (
                TxnStatus::Committed,
                Some(Timestamp(4)),
                Some(Timestamp(5)),
                Some(6),
                0x31,
                vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04],
            ),
            (TxnStatus::Aborted, None, None, None, 0x32, vec![0x00]),
        ];

        for (status, commit_timestamp, heartbeat, lease, tag, commit_bytes) in cases {
            let record = TxnStatusRecord {
                txn_id: TxnId(2),
                start_timestamp: Timestamp(3),
                commit_timestamp,
                status,
                primary_key: ROW_KEY.to_vec(),
                participant_tablet_ids: vec![9],
                last_heartbeat_timestamp: heartbeat,
                lease_deadline_ms: lease,
            };
            let mut expected = vec![
                0x01, tag, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x03,
            ];
            expected.extend_from_slice(&commit_bytes);
            expected.extend_from_slice(&framed_row_key());
            expected.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
            expected.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09]);
            match heartbeat {
                Some(Timestamp(5)) => {
                    expected
                        .extend_from_slice(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05]);
                }
                None => expected.push(0x00),
                _ => unreachable!("the golden fixture uses only the fields above"),
            }
            match lease {
                Some(6) => expected
                    .extend_from_slice(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06]),
                None => expected.push(0x00),
                _ => unreachable!("the golden fixture uses only the fields above"),
            }
            assert_golden(ValueRecordV1::TransactionStatus(record), expected);
        }
    }

    #[test]
    fn outcome_values_have_stable_v1_golden_bytes() {
        assert_golden(
            ValueRecordV1::LogicalOutcome(CachedTabletCommandOutcome::Applied(
                CachedTabletCommandResult::Noop,
            )),
            vec![0x01, 0x40, 0x01],
        );
        assert_golden(
            ValueRecordV1::LogicalOutcome(CachedTabletCommandOutcome::Rejected(
                CachedTabletCommandRejection {
                    kind: CachedTabletCommandRejectionKind::WriteConflict,
                    reason: "x".to_string(),
                },
            )),
            vec![0x01, 0x41, 0x02, 0x00, 0x00, 0x00, 0x01, b'x'],
        );
        assert_golden(
            ValueRecordV1::LegacyOutcome {
                last_sequence_applied: 7,
                outcome: CachedTabletCommandOutcome::Applied(CachedTabletCommandResult::Noop),
            },
            vec![
                0x01, 0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x01,
            ],
        );
        assert_golden(
            ValueRecordV1::LegacyOutcome {
                last_sequence_applied: 7,
                outcome: CachedTabletCommandOutcome::Rejected(CachedTabletCommandRejection {
                    kind: CachedTabletCommandRejectionKind::InvalidCommand,
                    reason: "x".to_string(),
                }),
            },
            vec![
                0x01, 0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x01, 0x00, 0x00, 0x00,
                0x01, b'x',
            ],
        );
    }

    #[test]
    fn retry_floor_has_stable_big_endian_v1_bytes() {
        let value = ValueRecordV1::RetryFloor {
            client_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
            session_epoch: 0x1112_1314_1516_1718,
            acknowledged_through: 0x2122_2324_2526_2728,
        };

        let encoded = encode_value_v1(&value).unwrap();

        assert_eq!(
            encoded,
            vec![
                0x01, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x21, 0x22,
                0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
            ]
        );
        assert_eq!(decode_value_v1(&encoded).unwrap(), value);
    }

    #[test]
    fn lock_value_round_trip_preserves_delete_operation() {
        let primary_key =
            encode_row_key(&make_row_key(TableId(1), &[Value::Int(1)]).unwrap()).unwrap();
        let value = ValueRecordV1::Lock(LockRecord {
            txn_id: TxnId(2),
            primary_key,
            start_timestamp: Timestamp(3),
            ttl_ms: 4,
            op: WriteKind::Delete,
        });

        let encoded = encode_value_v1(&value).unwrap();

        assert_eq!(decode_value_v1(&encoded).unwrap(), value);
    }

    #[test]
    fn decoder_fails_closed_for_unknown_truncated_and_noncanonical_values() {
        assert!(decode_value_v1(&[0x02, 0x01]).is_err());
        assert!(decode_value_v1(&[0x01, 0x7f]).is_err());
        assert!(decode_value_v1(&[0x01, 0x10, 0x00]).is_err());
        assert!(decode_value_v1(&[0x01, 0x50, 0xff, 0xff, 0xff, 0xff]).is_err());
        assert!(decode_value_v1(&[0x01, 0x40, 0x01, 0x00]).is_err());
        assert!(decode_value_v1(&[0x01, 0x41, 0x01, 0x00, 0x00, 0x00, 0x01, 0xff]).is_err());

        let mut malformed_lock = vec![
            0x01, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01,
            0x00,
        ];
        malformed_lock.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03]);
        malformed_lock.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x01]);
        assert!(matches!(
            decode_value_v1(&malformed_lock),
            Err(ragnordb_common::Error::CorruptData(_))
        ));
    }
}
