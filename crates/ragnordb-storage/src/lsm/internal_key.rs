//! Versioned ordered key identities for the tablet-local LSM.
//!
//! Candidate B keeps Default, Write, and Lock in distinct logical trees under
//! one tablet-replica storage lineage. This codec gives every logical family a
//! stable, prefix-free identity so records remain unambiguous in cursors,
//! exports, and future shared segment metadata.

use std::cmp::Ordering;

use ragnordb_common::{
    Error, Result,
    ids::{CommandKind, LogicalCommandId, TableId, Timestamp, TxnId},
};

use crate::key::decode_row_key;

/// The first durable internal-key encoding version.
pub const INTERNAL_KEY_FORMAT_V1: u8 = 0x01;

/// Stable descriptor persisted by future segment metadata for the V1 order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComparatorV1;

impl ComparatorV1 {
    /// Comparator identity stored in future SST/footer metadata.
    pub const ID: &'static [u8] = b"ragnordb.internal-key.bytewise.v1";

    /// Compare complete V1 keys using unsigned bytewise lexicographic order.
    pub fn compare(left: &[u8], right: &[u8]) -> Ordering {
        left.cmp(right)
    }
}

const COMPONENT_ESCAPE: u8 = 0x00;
const COMPONENT_ESCAPED_ZERO: u8 = 0xff;
const COMPONENT_TERMINATOR: [u8; 2] = [0x00, 0x00];

/// Stable logical record identities. These byte values are part of V1 and are
/// never derived from Rust enum layout or serialization defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecordNamespace {
    /// Row payload keyed by transaction start timestamp in the Default tree.
    Default,
    /// Commit records and rollback witnesses in the Write tree.
    Write,
    /// Active transaction intent in the Lock tree.
    Lock,
    /// Range deletion starts, colocated with the Write tree.
    RangeTombstone,
    /// Tablet-owned primary transaction status.
    TxnPrimaryStatus,
    /// Stable logical-command outcome used for retry deduplication.
    RetryOutcome,
    /// Monotonic retry-session retention floor.
    RetryFloor,
    /// Future MVCC secondary-index entries.
    SecondaryIndex,
    /// Future MVCC unique-value claims.
    UniqueClaim,
}

impl RecordNamespace {
    /// All assigned V1 namespace identifiers, in byte order.
    pub const ALL: [Self; 9] = [
        Self::Default,
        Self::Write,
        Self::Lock,
        Self::RangeTombstone,
        Self::TxnPrimaryStatus,
        Self::RetryOutcome,
        Self::RetryFloor,
        Self::SecondaryIndex,
        Self::UniqueClaim,
    ];

    const fn byte(self) -> u8 {
        match self {
            Self::Default => 0x10,
            Self::Write => 0x11,
            Self::Lock => 0x12,
            Self::RangeTombstone => 0x13,
            Self::TxnPrimaryStatus => 0x20,
            Self::RetryOutcome => 0x21,
            Self::RetryFloor => 0x22,
            Self::SecondaryIndex => 0x30,
            Self::UniqueClaim => 0x31,
        }
    }

    fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0x10 => Ok(Self::Default),
            0x11 => Ok(Self::Write),
            0x12 => Ok(Self::Lock),
            0x13 => Ok(Self::RangeTombstone),
            0x20 => Ok(Self::TxnPrimaryStatus),
            0x21 => Ok(Self::RetryOutcome),
            0x22 => Ok(Self::RetryFloor),
            0x30 => Ok(Self::SecondaryIndex),
            0x31 => Ok(Self::UniqueClaim),
            other => Err(corrupt(format!(
                "unknown or reserved internal-key namespace 0x{other:02x}"
            ))),
        }
    }

    const fn has_descending_timestamp(self) -> bool {
        matches!(
            self,
            Self::Default
                | Self::Write
                | Self::RangeTombstone
                | Self::SecondaryIndex
                | Self::UniqueClaim
        )
    }
}

/// Logical physical tree selected for a V1 record namespace.
///
/// Metadata and future index namespaces remain part of the same tablet-local
/// LSM lifetime. They do not create independent database instances or
/// publication/recovery lineages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalFamily {
    Default,
    Write,
    Lock,
    Metadata,
    Index,
}

impl RecordNamespace {
    /// Map logical record identities to the physical family in Candidate B.
    pub const fn physical_family(self) -> PhysicalFamily {
        match self {
            Self::Default => PhysicalFamily::Default,
            Self::Write | Self::RangeTombstone => PhysicalFamily::Write,
            Self::Lock => PhysicalFamily::Lock,
            Self::TxnPrimaryStatus | Self::RetryOutcome | Self::RetryFloor => {
                PhysicalFamily::Metadata
            }
            Self::SecondaryIndex | Self::UniqueClaim => PhysicalFamily::Index,
        }
    }
}

/// Decoded V1 key identity.
///
/// `logical_identity` contains the raw, canonical identity bytes before the
/// outer prefix-free component framing. Timestamped namespaces carry a
/// descending timestamp suffix; Lock and metadata identities do not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKeyV1 {
    namespace: RecordNamespace,
    logical_identity: Vec<u8>,
    timestamp: Option<Timestamp>,
}

impl InternalKeyV1 {
    /// Construct a Default-family row version identified by its start time.
    pub fn for_default(row_key: &[u8], start_ts: Timestamp) -> Result<Self> {
        Self::for_row(RecordNamespace::Default, row_key, Some(start_ts))
    }

    /// Construct a committed Write record identified by its commit time.
    pub fn for_write(row_key: &[u8], commit_ts: Timestamp) -> Result<Self> {
        Self::for_row(RecordNamespace::Write, row_key, Some(commit_ts))
    }

    /// Construct a rollback witness in the Write family, keyed by start time.
    ///
    /// The value record distinguishes this witness from a committed write.
    pub fn for_rollback(row_key: &[u8], start_ts: Timestamp) -> Result<Self> {
        Self::for_row(RecordNamespace::Write, row_key, Some(start_ts))
    }

    /// Construct a Lock-family key for one canonical row key.
    pub fn for_lock(row_key: &[u8]) -> Result<Self> {
        Self::for_row(RecordNamespace::Lock, row_key, None)
    }

    /// Construct the start key for a half-open range tombstone `[start, end)`.
    ///
    /// The exclusive end row key belongs in the versioned tombstone value;
    /// the key orders tombstones by start row and descending delete timestamp.
    pub fn for_range_tombstone(start_row_key: &[u8], delete_ts: Timestamp) -> Result<Self> {
        Self::for_row(
            RecordNamespace::RangeTombstone,
            start_row_key,
            Some(delete_ts),
        )
    }

    /// Construct the stable tablet-owned primary transaction status key.
    pub fn for_txn_primary_status(txn_id: TxnId) -> Result<Self> {
        if txn_id.0 == 0 {
            return Err(invalid("transaction ID 0 is reserved"));
        }
        Ok(Self {
            namespace: RecordNamespace::TxnPrimaryStatus,
            logical_identity: txn_id.0.to_be_bytes().to_vec(),
            timestamp: None,
        })
    }

    /// Construct the stable retry/dedup key for a logical command.
    pub fn for_retry_outcome(command_id: LogicalCommandId) -> Result<Self> {
        command_id
            .validate()
            .map_err(|message| invalid(format!("invalid logical command identity: {message}")))?;

        let mut identity = Vec::with_capacity(37);
        identity.extend_from_slice(&command_id.client_request_id.client_id.to_be_bytes());
        identity.extend_from_slice(&command_id.client_request_id.session_epoch.to_be_bytes());
        identity.extend_from_slice(&command_id.client_request_id.request_sequence.to_be_bytes());
        identity.extend_from_slice(&command_id.command_ordinal.to_be_bytes());
        identity.push(command_kind_byte(command_id.kind));

        Ok(Self {
            namespace: RecordNamespace::RetryOutcome,
            logical_identity: identity,
            timestamp: None,
        })
    }

    /// Construct the retry floor for one durable client session.
    pub fn for_retry_floor(client_id: u128, session_epoch: u64) -> Result<Self> {
        if client_id == 0 || session_epoch == 0 {
            return Err(invalid(
                "retry-floor client ID and session epoch must be non-zero",
            ));
        }

        let mut identity = Vec::with_capacity(24);
        identity.extend_from_slice(&client_id.to_be_bytes());
        identity.extend_from_slice(&session_epoch.to_be_bytes());

        Ok(Self {
            namespace: RecordNamespace::RetryFloor,
            logical_identity: identity,
            timestamp: None,
        })
    }

    /// Construct a future MVCC secondary-index entry.
    ///
    /// The index key bytes must already use the catalog-defined
    /// memcomparable encoding. Including the canonical row key makes duplicate
    /// index values distinct and keeps a stable path back to the base row.
    pub fn for_secondary_index(
        table_id: TableId,
        index_id: u64,
        index_key: &[u8],
        row_key: &[u8],
        timestamp: Timestamp,
    ) -> Result<Self> {
        let identity = encode_index_identity(table_id, index_id, index_key, Some(row_key))?;
        Ok(Self {
            namespace: RecordNamespace::SecondaryIndex,
            logical_identity: identity,
            timestamp: Some(timestamp),
        })
    }

    /// Construct a future unique-value claim ordered by its MVCC timestamp.
    ///
    /// The claiming transaction and row are stored in the value so that the
    /// unique-value identity remains the seek prefix used by validation.
    pub fn for_unique_claim(
        table_id: TableId,
        index_id: u64,
        unique_value: &[u8],
        timestamp: Timestamp,
    ) -> Result<Self> {
        let identity = encode_index_identity(table_id, index_id, unique_value, None)?;
        Ok(Self {
            namespace: RecordNamespace::UniqueClaim,
            logical_identity: identity,
            timestamp: Some(timestamp),
        })
    }

    fn for_row(
        namespace: RecordNamespace,
        row_key: &[u8],
        timestamp: Option<Timestamp>,
    ) -> Result<Self> {
        decode_row_key(row_key).map_err(|error| {
            invalid(format!(
                "internal MVCC key requires a canonical row key: {error}"
            ))
        })?;
        Ok(Self {
            namespace,
            logical_identity: row_key.to_vec(),
            timestamp,
        })
    }

    /// Return this key's stable logical namespace.
    pub const fn namespace(&self) -> RecordNamespace {
        self.namespace
    }

    /// Return the Candidate B tree that owns this logical namespace.
    pub const fn physical_family(&self) -> PhysicalFamily {
        self.namespace.physical_family()
    }

    /// Return the raw canonical identity before internal framing.
    pub fn logical_identity(&self) -> &[u8] {
        &self.logical_identity
    }

    /// Return the timestamp carried by a versioned record identity.
    pub const fn timestamp(&self) -> Option<Timestamp> {
        self.timestamp
    }

    /// Encode the exact V1 bytes used by bytewise lexicographic comparison.
    pub fn encode(&self) -> Result<Vec<u8>> {
        validate_fields(
            self.namespace,
            &self.logical_identity,
            self.timestamp,
            false,
        )?;

        let mut bytes = Vec::with_capacity(2 + self.logical_identity.len() + 10);
        bytes.push(INTERNAL_KEY_FORMAT_V1);
        bytes.push(self.namespace.byte());
        encode_component(&self.logical_identity, &mut bytes);

        if let Some(timestamp) = self.timestamp {
            bytes.extend_from_slice(&(!timestamp.0).to_be_bytes());
        }

        Ok(bytes)
    }

    /// Decode and validate persisted V1 bytes. Unsupported or reserved
    /// identities fail closed as corrupt data.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 2 {
            return Err(corrupt("internal key is truncated before its header"));
        }
        if bytes[0] != INTERNAL_KEY_FORMAT_V1 {
            return Err(corrupt(format!(
                "unsupported internal-key format version {}",
                bytes[0]
            )));
        }

        let namespace = RecordNamespace::from_byte(bytes[1])?;
        let (logical_identity, suffix_start) = decode_component(&bytes[2..])?;
        let suffix = &bytes[2 + suffix_start..];
        let timestamp = if namespace.has_descending_timestamp() {
            if suffix.len() != 8 {
                return Err(corrupt(format!(
                    "namespace 0x{:02x} requires an eight-byte timestamp suffix",
                    namespace.byte()
                )));
            }
            let mut encoded = [0_u8; 8];
            encoded.copy_from_slice(suffix);
            Some(Timestamp(!u64::from_be_bytes(encoded)))
        } else {
            if !suffix.is_empty() {
                return Err(corrupt(format!(
                    "namespace 0x{:02x} does not permit a timestamp suffix",
                    namespace.byte()
                )));
            }
            None
        };

        validate_fields(namespace, &logical_identity, timestamp, true)?;
        Ok(Self {
            namespace,
            logical_identity,
            timestamp,
        })
    }
}

fn validate_fields(
    namespace: RecordNamespace,
    identity: &[u8],
    timestamp: Option<Timestamp>,
    persisted: bool,
) -> Result<()> {
    let fail = |message: String| {
        if persisted {
            corrupt(message)
        } else {
            invalid(message)
        }
    };

    if namespace.has_descending_timestamp() != timestamp.is_some() {
        return Err(fail(format!(
            "namespace 0x{:02x} has an invalid timestamp shape",
            namespace.byte()
        )));
    }

    match namespace {
        RecordNamespace::Default
        | RecordNamespace::Write
        | RecordNamespace::Lock
        | RecordNamespace::RangeTombstone => {
            decode_row_key(identity).map_err(|error| {
                fail(format!("namespace row identity is not canonical: {error}"))
            })?;
        }
        RecordNamespace::TxnPrimaryStatus => {
            let _txn_id = decode_nonzero_u64(identity, "transaction status ID", persisted)?;
        }
        RecordNamespace::RetryOutcome => {
            if identity.len() != 37 {
                return Err(fail(
                    "retry outcome identity must contain exactly 37 bytes".to_string(),
                ));
            }
            let client_id = u128::from_be_bytes(
                identity[..16]
                    .try_into()
                    .map_err(|_| fail("retry outcome client ID is truncated".to_string()))?,
            );
            let session_epoch = read_u64(&identity[16..24]).unwrap_or(0);
            let request_sequence = read_u64(&identity[24..32]).unwrap_or(0);
            let command_ordinal = u32::from_be_bytes(
                identity[32..36]
                    .try_into()
                    .map_err(|_| fail("retry outcome ordinal is truncated".to_string()))?,
            );
            let _kind = command_kind_from_byte(identity[36]).ok_or_else(|| {
                fail(format!("unknown retry command kind 0x{:02x}", identity[36]))
            })?;
            if client_id == 0 || session_epoch == 0 || request_sequence == 0 || command_ordinal == 0
            {
                return Err(fail(
                    "retry outcome identity contains a reserved zero field".to_string(),
                ));
            }
        }
        RecordNamespace::RetryFloor => {
            if identity.len() != 24 {
                return Err(fail(
                    "retry-floor identity must contain exactly 24 bytes".to_string(),
                ));
            }
            let client_id = u128::from_be_bytes(
                identity[..16]
                    .try_into()
                    .map_err(|_| fail("retry-floor client ID is truncated".to_string()))?,
            );
            let session_epoch = read_u64(&identity[16..24]).unwrap_or(0);
            if client_id == 0 || session_epoch == 0 {
                return Err(fail(
                    "retry-floor identity contains a reserved zero field".to_string(),
                ));
            }
        }
        RecordNamespace::SecondaryIndex => {
            decode_index_identity(identity, true, persisted)?;
        }
        RecordNamespace::UniqueClaim => {
            decode_index_identity(identity, false, persisted)?;
        }
    }

    Ok(())
}

fn encode_index_identity(
    table_id: TableId,
    index_id: u64,
    index_key: &[u8],
    row_key: Option<&[u8]>,
) -> Result<Vec<u8>> {
    if table_id.0 == 0 || index_id == 0 {
        return Err(invalid("table and index IDs must be non-zero"));
    }
    if index_key.is_empty() {
        return Err(invalid("encoded index key cannot be empty"));
    }

    let mut identity = Vec::new();
    identity.extend_from_slice(&table_id.0.to_be_bytes());
    identity.extend_from_slice(&index_id.to_be_bytes());
    encode_component(index_key, &mut identity);

    if let Some(row_key) = row_key {
        let decoded = decode_row_key(row_key).map_err(|error| {
            invalid(format!("index entry requires a canonical row key: {error}"))
        })?;
        if decoded.table_id != table_id {
            return Err(invalid(
                "index entry table ID does not match its base row key",
            ));
        }
        encode_component(row_key, &mut identity);
    }

    Ok(identity)
}

fn decode_index_identity(identity: &[u8], includes_row_key: bool, persisted: bool) -> Result<()> {
    let fail = |message: String| {
        if persisted {
            corrupt(message)
        } else {
            invalid(message)
        }
    };
    if identity.len() < 16 {
        return Err(fail(
            "index identity is missing table/index IDs".to_string(),
        ));
    }
    let table_id = read_u64(&identity[..8]).unwrap_or(0);
    let index_id = read_u64(&identity[8..16]).unwrap_or(0);
    if table_id == 0 || index_id == 0 {
        return Err(fail(
            "index identity contains a reserved zero ID".to_string(),
        ));
    }

    let (index_key, mut position) = decode_component(&identity[16..]).map_err(|error| {
        fail(format!(
            "index identity has a malformed index-key component: {error}"
        ))
    })?;
    if index_key.is_empty() {
        return Err(fail("encoded index key cannot be empty".to_string()));
    }
    position += 16;

    if includes_row_key {
        let (row_key, component_bytes) =
            decode_component(&identity[position..]).map_err(|error| {
                fail(format!(
                    "index identity has a malformed row-key component: {error}"
                ))
            })?;
        if position + component_bytes != identity.len() {
            return Err(fail("index identity contains trailing bytes".to_string()));
        }
        let row = decode_row_key(&row_key)
            .map_err(|error| fail(format!("index base row key is not canonical: {error}")))?;
        if row.table_id.0 != table_id {
            return Err(fail(
                "index identity table ID does not match its base row key".to_string(),
            ));
        }
    } else if position != identity.len() {
        return Err(fail(
            "unique-claim identity contains trailing bytes".to_string(),
        ));
    }

    Ok(())
}

/// Append one prefix-free byte component while retaining lexicographic order.
fn encode_component(component: &[u8], output: &mut Vec<u8>) {
    for byte in component {
        if *byte == COMPONENT_ESCAPE {
            output.push(COMPONENT_ESCAPE);
            output.push(COMPONENT_ESCAPED_ZERO);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&COMPONENT_TERMINATOR);
}

/// Decode one canonical prefix-free component and return the byte offset after
/// its terminator within `bytes`.
fn decode_component(bytes: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut output = Vec::with_capacity(bytes.len());
    let mut position = 0;

    while position < bytes.len() {
        let byte = bytes[position];
        position += 1;
        if byte != COMPONENT_ESCAPE {
            output.push(byte);
            continue;
        }

        let escaped = *bytes
            .get(position)
            .ok_or_else(|| corrupt("internal-key component ends in an escape byte"))?;
        position += 1;
        match escaped {
            0x00 => return Ok((output, position)),
            COMPONENT_ESCAPED_ZERO => output.push(COMPONENT_ESCAPE),
            other => {
                return Err(corrupt(format!(
                    "invalid internal-key component escape 0x00 0x{other:02x}"
                )));
            }
        }
    }

    Err(corrupt("internal-key component is missing its terminator"))
}

fn decode_nonzero_u64(bytes: &[u8], context: &str, persisted: bool) -> Result<u64> {
    let fail = |message: String| {
        if persisted {
            corrupt(message)
        } else {
            invalid(message)
        }
    };
    if bytes.len() != 8 {
        return Err(fail(format!("{context} identity must contain eight bytes")));
    }
    let value = read_u64(bytes).unwrap_or(0);
    if value == 0 {
        return Err(fail(format!("{context} 0 is reserved")));
    }
    Ok(value)
}

fn read_u64(bytes: &[u8]) -> Option<u64> {
    let encoded: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_be_bytes(encoded))
}

fn command_kind_byte(kind: CommandKind) -> u8 {
    match kind {
        CommandKind::Read => 0x01,
        CommandKind::Prewrite => 0x02,
        CommandKind::Commit => 0x03,
        CommandKind::Rollback => 0x04,
        CommandKind::ResolveIntent => 0x05,
        CommandKind::SingleShardCommit => 0x06,
        CommandKind::Catalog => 0x07,
        CommandKind::Noop => 0x08,
    }
}

fn command_kind_from_byte(byte: u8) -> Option<CommandKind> {
    match byte {
        0x01 => Some(CommandKind::Read),
        0x02 => Some(CommandKind::Prewrite),
        0x03 => Some(CommandKind::Commit),
        0x04 => Some(CommandKind::Rollback),
        0x05 => Some(CommandKind::ResolveIntent),
        0x06 => Some(CommandKind::SingleShardCommit),
        0x07 => Some(CommandKind::Catalog),
        0x08 => Some(CommandKind::Noop),
        _ => None,
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidArgument(message.into())
}

fn corrupt(message: impl Into<String>) -> Error {
    Error::CorruptData(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ragnordb_common::{
        codec::Value,
        ids::{ClientRequestId, CommandKind, LogicalCommandId, TableId, Timestamp, TxnId},
    };

    use super::{
        ComparatorV1, INTERNAL_KEY_FORMAT_V1, InternalKeyV1, PhysicalFamily, RecordNamespace,
    };
    use crate::key::{decode_row_key, encode_row_key, make_row_key};

    fn row_key(table_id: u64, values: &[Value]) -> Vec<u8> {
        encode_row_key(&make_row_key(TableId(table_id), values).expect("valid row key"))
            .expect("canonical row key")
    }

    fn retry_id() -> LogicalCommandId {
        LogicalCommandId {
            client_request_id: ClientRequestId {
                client_id: u128::MAX,
                session_epoch: u64::MAX,
                request_sequence: u64::MAX,
            },
            command_ordinal: u32::MAX,
            kind: CommandKind::Noop,
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn default_key_v1_encodes_the_logical_row_and_descending_start_timestamp() {
        assert_eq!(ComparatorV1::ID, b"ragnordb.internal-key.bytewise.v1");

        let key = InternalKeyV1::for_default(&row_key(1, &[Value::Int(7)]), Timestamp(1))
            .expect("valid default key");
        let encoded = key.encode().expect("V1 key encoding");

        assert_eq!(encoded[0], INTERNAL_KEY_FORMAT_V1);
        assert_eq!(encoded[1], 0x10);
        assert!(encoded.ends_with(&(!1_u64).to_be_bytes()));
        assert_eq!(InternalKeyV1::decode(&encoded).unwrap(), key);
    }

    #[test]
    fn each_v1_namespace_has_a_golden_byte_encoding() {
        let row = row_key(u64::MAX, &[Value::Bool(true)]);
        let cases = [
            (
                InternalKeyV1::for_default(&row, Timestamp(0x0102_0304_0506_0708)).unwrap(),
                "011001ffffffffffffffff30010000fefdfcfbfaf9f8f7",
            ),
            (
                InternalKeyV1::for_write(&row, Timestamp(0x0102_0304_0506_0708)).unwrap(),
                "011101ffffffffffffffff30010000fefdfcfbfaf9f8f7",
            ),
            (
                InternalKeyV1::for_lock(&row).unwrap(),
                "011201ffffffffffffffff30010000",
            ),
            (
                InternalKeyV1::for_rollback(&row, Timestamp(0x0102_0304_0506_0708)).unwrap(),
                "011101ffffffffffffffff30010000fefdfcfbfaf9f8f7",
            ),
            (
                InternalKeyV1::for_range_tombstone(&row, Timestamp(0x0102_0304_0506_0708)).unwrap(),
                "011301ffffffffffffffff30010000fefdfcfbfaf9f8f7",
            ),
            (
                InternalKeyV1::for_txn_primary_status(TxnId(u64::MAX)).unwrap(),
                "0120ffffffffffffffff0000",
            ),
            (
                InternalKeyV1::for_retry_outcome(retry_id()).unwrap(),
                "0121ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff080000",
            ),
            (
                InternalKeyV1::for_retry_floor(u128::MAX, u64::MAX).unwrap(),
                "0122ffffffffffffffffffffffffffffffffffffffffffffffff0000",
            ),
            (
                InternalKeyV1::for_secondary_index(
                    TableId(u64::MAX),
                    u64::MAX,
                    &[0xaa],
                    &row,
                    Timestamp(0x0102_0304_0506_0708),
                )
                .unwrap(),
                "0130ffffffffffffffffffffffffffffffffaa00ff00ff01ffffffffffffffff300100ff00ff0000fefdfcfbfaf9f8f7",
            ),
            (
                InternalKeyV1::for_unique_claim(
                    TableId(u64::MAX),
                    u64::MAX,
                    &[0xaa],
                    Timestamp(0x0102_0304_0506_0708),
                )
                .unwrap(),
                "0131ffffffffffffffffffffffffffffffffaa00ff00ff0000fefdfcfbfaf9f8f7",
            ),
        ];

        let mut namespace_ids = BTreeSet::new();
        for (key, expected_hex) in cases {
            let encoded = key.encode().unwrap();
            assert_eq!(hex(&encoded), expected_hex, "{:?}", key.namespace());
            namespace_ids.insert(encoded[1]);
            assert_eq!(InternalKeyV1::decode(&encoded).unwrap(), key);
        }
        assert_eq!(namespace_ids.len(), RecordNamespace::ALL.len());
    }

    #[test]
    fn encoding_round_trips_and_uses_one_canonical_representation() {
        let key = InternalKeyV1::for_default(
            &row_key(9, &[Value::Text("zero\0byte".to_string()), Value::Int(-2)]),
            Timestamp(42),
        )
        .unwrap();
        let encoded = key.encode().unwrap();
        let decoded = InternalKeyV1::decode(&encoded).unwrap();

        assert_eq!(decoded, key);
        assert_eq!(decoded.encode().unwrap(), encoded);
        assert_eq!(
            decode_row_key(decoded.logical_identity()).unwrap().table_id,
            TableId(9)
        );
    }

    #[test]
    fn bytewise_order_preserves_row_keys_and_places_newer_versions_first() {
        let older_row = row_key(4, &[Value::Int(-1)]);
        let newer_row = row_key(4, &[Value::Int(0)]);
        let old_encoded = InternalKeyV1::for_default(&older_row, Timestamp(10))
            .unwrap()
            .encode()
            .unwrap();
        let new_row_encoded = InternalKeyV1::for_default(&newer_row, Timestamp(10))
            .unwrap()
            .encode()
            .unwrap();
        assert_eq!(
            ComparatorV1::compare(&old_encoded, &new_row_encoded),
            std::cmp::Ordering::Less
        );

        let old_version = InternalKeyV1::for_default(&older_row, Timestamp(10))
            .unwrap()
            .encode()
            .unwrap();
        let new_version = InternalKeyV1::for_default(&older_row, Timestamp(11))
            .unwrap()
            .encode()
            .unwrap();
        assert_eq!(
            ComparatorV1::compare(&new_version, &old_version),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn prefix_keys_remain_distinct_and_namespace_keys_cannot_alias() {
        let prefix_row = row_key(7, &[Value::Int(1)]);
        let extended_row = row_key(7, &[Value::Int(1), Value::Bool(false)]);
        assert!(extended_row.starts_with(&prefix_row));

        let prefix = InternalKeyV1::for_default(&prefix_row, Timestamp(5))
            .unwrap()
            .encode()
            .unwrap();
        let extended = InternalKeyV1::for_default(&extended_row, Timestamp(5))
            .unwrap()
            .encode()
            .unwrap();
        assert_ne!(prefix, extended);
        assert!(prefix < extended);

        let lock = InternalKeyV1::for_lock(&prefix_row)
            .unwrap()
            .encode()
            .unwrap();
        let write = InternalKeyV1::for_write(&prefix_row, Timestamp(5))
            .unwrap()
            .encode()
            .unwrap();
        let default = InternalKeyV1::for_default(&prefix_row, Timestamp(5))
            .unwrap()
            .encode()
            .unwrap();
        assert_ne!(lock, write);
        assert_ne!(lock, default);
        assert_ne!(write, default);
    }

    #[test]
    fn timestamp_and_identifier_boundaries_are_supported() {
        let min =
            InternalKeyV1::for_default(&row_key(1, &[Value::Int(i64::MIN)]), Timestamp(0)).unwrap();
        let max = InternalKeyV1::for_default(
            &row_key(u64::MAX, &[Value::Int(i64::MAX)]),
            Timestamp(u64::MAX),
        )
        .unwrap();

        assert_eq!(InternalKeyV1::decode(&min.encode().unwrap()).unwrap(), min);
        assert_eq!(InternalKeyV1::decode(&max.encode().unwrap()).unwrap(), max);
        assert!(min.encode().unwrap() < max.encode().unwrap());
    }

    #[test]
    fn namespace_mapping_keeps_candidate_b_under_one_storage_lineage() {
        assert_eq!(
            RecordNamespace::Default.physical_family(),
            PhysicalFamily::Default
        );
        assert_eq!(
            RecordNamespace::Write.physical_family(),
            PhysicalFamily::Write
        );
        assert_eq!(
            RecordNamespace::RangeTombstone.physical_family(),
            PhysicalFamily::Write
        );
        assert_eq!(
            RecordNamespace::Lock.physical_family(),
            PhysicalFamily::Lock
        );
        assert_eq!(
            RecordNamespace::TxnPrimaryStatus.physical_family(),
            PhysicalFamily::Metadata
        );
        assert_eq!(
            RecordNamespace::SecondaryIndex.physical_family(),
            PhysicalFamily::Index
        );
    }

    #[test]
    fn malformed_truncated_and_reserved_encodings_fail_closed() {
        for bytes in [
            &[][..],
            &[INTERNAL_KEY_FORMAT_V1][..],
            &[0x02, 0x10, 0, 0][..],
            &[INTERNAL_KEY_FORMAT_V1, 0x14, 0, 0][..],
            &[INTERNAL_KEY_FORMAT_V1, 0x10, 0][..],
            &[INTERNAL_KEY_FORMAT_V1, 0x10, 0, 0x01][..],
        ] {
            assert!(
                InternalKeyV1::decode(bytes).is_err(),
                "accepted {bytes:02x?}"
            );
        }

        let mut timestamped =
            InternalKeyV1::for_default(&row_key(3, &[Value::Int(1)]), Timestamp(9))
                .unwrap()
                .encode()
                .unwrap();
        timestamped.pop();
        assert!(InternalKeyV1::decode(&timestamped).is_err());

        let mut lock_with_suffix = InternalKeyV1::for_lock(&row_key(3, &[Value::Int(1)]))
            .unwrap()
            .encode()
            .unwrap();
        lock_with_suffix.extend_from_slice(&[0; 8]);
        assert!(InternalKeyV1::decode(&lock_with_suffix).is_err());
    }

    #[test]
    fn constructors_reject_noncanonical_rows_and_reserved_metadata_ids() {
        assert!(InternalKeyV1::for_lock(&[1, 0, 0]).is_err());
        assert!(InternalKeyV1::for_txn_primary_status(TxnId(0)).is_err());
        assert!(InternalKeyV1::for_retry_floor(0, 1).is_err());
        assert!(InternalKeyV1::for_retry_floor(1, 0).is_err());
        assert!(InternalKeyV1::for_unique_claim(TableId(0), 1, &[1], Timestamp(1)).is_err());
        assert!(
            InternalKeyV1::for_secondary_index(
                TableId(2),
                1,
                &[1],
                &row_key(1, &[Value::Int(1)]),
                Timestamp(1)
            )
            .is_err()
        );
    }
}
