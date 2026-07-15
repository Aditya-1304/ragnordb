//! Ordered primary key and row key encoding
//!
//! primary keys use memcomparable representation: lexicographic byte order
//! matches logical SQL ordering for values of the same schema defined type
//!
//! supported component formats:
//!
//! ```text
//! INT  = [tag][sign-adjusted i64 as u64 big-endian]
//! TEXT = [tag][escaped UTF-8 bytes][0x00 0x00 terminator]
//! BOOL = [tag][0 or 1]
//! ```
//!
//! Text uses binary UTF-8 collation. embedded zero bytes are encoded as
//! `0x00 0xff`; `0x00 0x00` terminates the component. this makes strings
//! prefix free while preserving their bytewise ordering
//!
//! SQL primary key columns are non nullable, so `NULL` is rejected
//!
//! A complete row key uses this representation:
//!
//! ```text
//! [row namespace: u8]
//! [table ID: u64 big-endian]
//! [encoded primary key]
//! ```
//!
//! the namespace byte leaves room for future index and metadeta key spaces
//! `RowKey::to_proto()` is message serialization and currently represents its
//! table ID with little-endian bytes. That protobuf representation is separate
//! from `encode_row_key()`, which deliberately uses big-endian table IDs to
//! preserve numeric ordering in the storage engine.

use ragnordb_common::{
    Error, Result,
    codec::Value,
    ids::{RowKey, TableId},
};

const ROW_KEY_NAMESPACE: u8 = 0x01;

const KEY_TAG_INT: u8 = 0x10;
const KEY_TAG_TEXT: u8 = 0x20;
const KEY_TAG_BOOL: u8 = 0x30;

const TEXT_ESCAPE: u8 = 0x00;
const TEXT_TERMINATOR: u8 = 0x00;
const TEXT_ESCAPED_ZERO: u8 = 0xff;

const SIGN_BIT: u64 = 1_u64 << 63;
const ROW_KEY_HEADER_LENGTH: usize = 1 + 8;

/// this encode an ordered, non-empty primary-key tuple.
///
/// Component order must match the catalog's
/// `primary_key_column_ids` ordering.
pub fn encode_primary_key(values: &[Value]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Err(invalid_key("primary key must contain at least one value"));
    }

    let mut output = Vec::new();

    for value in values {
        encode_primary_key_value(value, &mut output)?;
    }

    Ok(output)
}

/// this decide a complete primary key tuple
///
/// Unlike `encode_primary_key()`, this function consumes persisted or received
/// storage bytes. Malformed input therefore represents corrupted encoded data,
/// not invalid logical input from a caller.
pub fn decode_primary_key(bytes: &[u8]) -> Result<Vec<Value>> {
    if bytes.is_empty() {
        return Err(corrupt_key("encoded primary key cannot be empty"));
    }

    let mut decoder = KeyDecoder::new(bytes);
    let mut values = Vec::new();

    while !decoder.is_finished() {
        values.push(decode_primary_key_value(&mut decoder)?);
    }

    Ok(values)
}

/// constructs the domain row key for one table and primary key tuple.
pub fn make_row_key(table_id: TableId, primary_key_values: &[Value]) -> Result<RowKey> {
    validate_table_id(table_id)?;

    Ok(RowKey {
        table_id,
        primary_key_bytes: encode_primary_key(primary_key_values)?,
    })
}

/// Encode a complete row key into ordered storage bytes.
///
/// Because `RowKey` has public fields, this function validates externally
/// constructed values before accepting them. Invalid caller-provided `RowKey`
/// values return `InvalidArgument`, not `CorruptData`.
pub fn encode_row_key(row_key: &RowKey) -> Result<Vec<u8>> {
    validate_table_id(row_key.table_id)?;

    decode_primary_key(&row_key.primary_key_bytes).map_err(|error| {
        invalid_key(format!(
            "RowKey contains noncanonical primary-key bytes: {error}"
        ))
    })?;

    let mut output = Vec::with_capacity(ROW_KEY_HEADER_LENGTH + row_key.primary_key_bytes.len());

    output.push(ROW_KEY_NAMESPACE);
    output.extend_from_slice(&row_key.table_id.0.to_be_bytes());
    output.extend_from_slice(&row_key.primary_key_bytes);

    Ok(output)
}

/// Decode and validate a complete ordered row key.
///
/// unknown namespaces, truncated keysmm zero table IDs and malformed
/// primary key components indicate corrupt stored bytes
pub fn decode_row_key(bytes: &[u8]) -> Result<RowKey> {
    if bytes.len() <= ROW_KEY_HEADER_LENGTH {
        return Err(corrupt_key("row key is missing its encoded primary key"));
    }

    if bytes[0] != ROW_KEY_NAMESPACE {
        return Err(corrupt_key(format!(
            "unknown row-key namespace 0x{:02x}",
            bytes[0]
        )));
    }

    let mut table_id_bytes = [0_u8; 8];

    table_id_bytes.copy_from_slice(&bytes[1..ROW_KEY_HEADER_LENGTH]);

    let table_id = TableId(u64::from_be_bytes(table_id_bytes));

    if table_id.0 == 0 {
        return Err(corrupt_key("decoded row key contains reserved table ID 0"));
    }

    let primary_key_bytes = bytes[ROW_KEY_HEADER_LENGTH..].to_vec();

    decode_primary_key(&primary_key_bytes)?;

    Ok(RowKey {
        table_id,
        primary_key_bytes,
    })
}

fn encode_primary_key_value(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Int(value) => {
            output.push(KEY_TAG_INT);

            // Flipping the sign bit maps signed integer order onto unsigned
            // big-endian byte order:
            //
            // i64::MIN -> 0
            // -1       -> 2^63 - 1
            // 0        -> 2^63
            // i64::MAX -> u64::MAX
            let ordered = (*value as u64) ^ SIGN_BIT;

            output.extend_from_slice(&ordered.to_be_bytes());
        }

        Value::Text(value) => {
            output.push(KEY_TAG_TEXT);

            for byte in value.as_bytes() {
                if *byte == TEXT_ESCAPE {
                    output.push(TEXT_ESCAPE);
                    output.push(TEXT_ESCAPED_ZERO);
                } else {
                    output.push(*byte);
                }
            }

            output.push(TEXT_ESCAPE);
            output.push(TEXT_TERMINATOR);
        }

        Value::Bool(value) => {
            output.push(KEY_TAG_BOOL);
            output.push(u8::from(*value));
        }

        Value::Null => {
            return Err(invalid_key("NULL cannot be encoded as a primary-key value"));
        }
    }

    Ok(())
}

fn decode_primary_key_value(decoder: &mut KeyDecoder<'_>) -> Result<Value> {
    match decoder.read_u8()? {
        KEY_TAG_INT => {
            let bytes = decoder.read_exact(8)?;
            let mut encoded = [0_u8; 8];

            encoded.copy_from_slice(bytes);

            let ordered = u64::from_be_bytes(encoded);
            let signed_bits = ordered ^ SIGN_BIT;

            Ok(Value::Int(signed_bits as i64))
        }

        KEY_TAG_TEXT => decode_text_key(decoder),

        KEY_TAG_BOOL => {
            let encoded = decoder.read_u8()?;

            match encoded {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                other => Err(corrupt_key(format!("invalid BOOL key payload {other}"))),
            }
        }

        tag => Err(corrupt_key(format!("unknown primary-key tag 0x{tag:02x}"))),
    }
}

fn decode_text_key(decoder: &mut KeyDecoder<'_>) -> Result<Value> {
    let mut text_bytes = Vec::new();

    loop {
        let byte = decoder.read_u8()?;

        if byte != TEXT_ESCAPE {
            text_bytes.push(byte);
            continue;
        }

        let escaped = decoder.read_u8()?;

        match escaped {
            TEXT_TERMINATOR => break,
            TEXT_ESCAPED_ZERO => {
                text_bytes.push(TEXT_ESCAPE);
            }
            other => {
                return Err(corrupt_key(format!(
                    "invalid TEXT escape sequence 0x00 0x{other:02x}"
                )));
            }
        }
    }

    let text = String::from_utf8(text_bytes)
        .map_err(|error| corrupt_key(format!("TEXT primary key is not valid UTF-8: {error}")))?;

    Ok(Value::Text(text))
}

fn validate_table_id(table_id: TableId) -> Result<()> {
    if table_id.0 == 0 {
        return Err(invalid_key("table ID 0 is reserved"));
    }

    Ok(())
}

/// Bounds-checked decoder for primary key components
struct KeyDecoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> KeyDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn is_finished(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| corrupt_key("encoded key length overflow"))?;

        if end > self.bytes.len() {
            return Err(corrupt_key(format!(
                "truncated primary key at byte {}, needed {} more bytes",
                self.position,
                end - self.bytes.len()
            )));
        }

        let result = &self.bytes[self.position..end];
        self.position = end;

        Ok(result)
    }
}

fn corrupt_key(message: impl std::fmt::Display) -> Error {
    Error::CorruptData(format!("invalid storage key encoding: {message}"))
}

fn invalid_key(message: impl std::fmt::Display) -> Error {
    Error::InvalidArgument(format!("invalid storage key: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_key_roundtrips_all_supported_types() {
        let values = vec![
            Value::Int(-42),
            Value::Text("tenant\0name".to_string()),
            Value::Bool(true),
        ];

        let encoded = encode_primary_key(&values).unwrap();
        let decoded = decode_primary_key(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn signed_integer_keys_have_stable_bytes() {
        assert_eq!(
            encode_primary_key(&[Value::Int(0)]).unwrap(),
            vec![KEY_TAG_INT, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,]
        );

        assert_eq!(
            encode_primary_key(&[Value::Int(-1)]).unwrap(),
            vec![KEY_TAG_INT, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,]
        );
    }

    #[test]
    fn escaped_text_key_has_stable_bytes() {
        assert_eq!(
            encode_primary_key(&[Value::Text("a\0".to_string()),]).unwrap(),
            vec![KEY_TAG_TEXT, b'a', 0x00, 0xff, 0x00, 0x00,]
        );
    }

    #[test]
    fn complete_row_key_has_stable_bytes() {
        let row_key = make_row_key(TableId(1), &[Value::Int(0)]).unwrap();

        assert_eq!(
            encode_row_key(&row_key).unwrap(),
            vec![
                ROW_KEY_NAMESPACE,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x01,
                KEY_TAG_INT,
                0x80,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
            ]
        );
    }

    #[test]
    fn signed_integer_encoding_preserves_order() {
        let values = [i64::MIN, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX];

        let encoded = values
            .iter()
            .map(|value| encode_primary_key(&[Value::Int(*value)]).unwrap())
            .collect::<Vec<_>>();

        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn text_encoding_preserves_binary_utf8_order() {
        let values = ["", "\0", "\0a", "a", "a\0", "aa", "b", "é"];

        let encoded = values
            .iter()
            .map(|value| encode_primary_key(&[Value::Text((*value).to_string())]).unwrap())
            .collect::<Vec<_>>();

        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn boolean_encoding_preserves_order() {
        let false_key = encode_primary_key(&[Value::Bool(false)]).unwrap();

        let true_key = encode_primary_key(&[Value::Bool(true)]).unwrap();

        assert!(false_key < true_key);
    }

    #[test]
    fn composite_key_encoding_preserves_tuple_order() {
        let first = encode_primary_key(&[Value::Int(1), Value::Text("alice".to_string())]).unwrap();

        let second = encode_primary_key(&[Value::Int(1), Value::Text("bob".to_string())]).unwrap();

        let third = encode_primary_key(&[Value::Int(2), Value::Text("alice".to_string())]).unwrap();

        assert!(first < second);
        assert!(second < third);
    }

    #[test]
    fn row_key_roundtrips() {
        let row_key = make_row_key(
            TableId(7),
            &[Value::Int(42), Value::Text("Ada".to_string())],
        )
        .unwrap();

        let bytes = encode_row_key(&row_key).unwrap();
        let decoded = decode_row_key(&bytes).unwrap();

        assert_eq!(decoded, row_key);
    }

    #[test]
    fn row_key_bytes_preserve_table_order() {
        let first = make_row_key(TableId(1), &[Value::Int(100)]).unwrap();

        let second = make_row_key(TableId(2), &[Value::Int(-100)]).unwrap();

        assert!(encode_row_key(&first).unwrap() < encode_row_key(&second).unwrap());
    }

    #[test]
    fn row_key_bytes_preserve_primary_key_order() {
        let first = make_row_key(TableId(1), &[Value::Int(-1)]).unwrap();

        let second = make_row_key(TableId(1), &[Value::Int(0)]).unwrap();

        assert!(encode_row_key(&first).unwrap() < encode_row_key(&second).unwrap());
    }

    #[test]
    fn rejects_empty_primary_key_as_invalid_argument() {
        let error = encode_primary_key(&[]).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("at least one value"));
    }

    #[test]
    fn rejects_null_primary_key_as_invalid_argument() {
        let error = encode_primary_key(&[Value::Null]).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("NULL cannot be encoded"));
    }

    #[test]
    fn rejects_zero_table_id_as_invalid_argument() {
        let error = make_row_key(TableId(0), &[Value::Int(1)]).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("table ID 0"));
    }

    #[test]
    fn rejects_noncanonical_external_row_key_as_invalid_argument() {
        let row_key = RowKey {
            table_id: TableId(1),
            primary_key_bytes: vec![0xff],
        };

        let error = encode_row_key(&row_key).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("noncanonical primary-key bytes"));
    }

    #[test]
    fn rejects_unknown_primary_key_tag_as_corruption() {
        let error = decode_primary_key(&[0xff]).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
        assert!(error.to_string().contains("unknown primary-key tag"));
    }

    #[test]
    fn rejects_truncated_integer_key_as_corruption() {
        let error = decode_primary_key(&[KEY_TAG_INT, 0, 1]).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
        assert!(error.to_string().contains("truncated"));
    }

    #[test]
    fn rejects_invalid_text_escape_as_corruption() {
        let bytes = [KEY_TAG_TEXT, b'a', TEXT_ESCAPE, 0x01];

        let error = decode_primary_key(&bytes).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
        assert!(error.to_string().contains("invalid TEXT escape sequence"));
    }

    #[test]
    fn rejects_unknown_row_namespace_as_corruption() {
        let mut bytes =
            encode_row_key(&make_row_key(TableId(1), &[Value::Int(1)]).unwrap()).unwrap();

        bytes[0] = 0xff;

        let error = decode_row_key(&bytes).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
        assert!(error.to_string().contains("unknown row-key namespace"));
    }

    #[test]
    fn rejects_decoded_zero_table_id_as_corruption() {
        let mut bytes =
            encode_row_key(&make_row_key(TableId(1), &[Value::Int(1)]).unwrap()).unwrap();

        bytes[1..ROW_KEY_HEADER_LENGTH].fill(0);

        let error = decode_row_key(&bytes).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
        assert!(error.to_string().contains("table ID 0"));
    }

    #[test]
    fn rejects_empty_encoded_primary_key_as_corruption() {
        let error = decode_primary_key(&[]).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
        assert!(
            error
                .to_string()
                .contains("encoded primary key cannot be empty")
        );
    }

    #[test]
    fn rejects_invalid_boolean_payload_as_corruption() {
        let bytes = [KEY_TAG_BOOL, 2];

        let error = decode_primary_key(&bytes).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
        assert!(error.to_string().contains("invalid BOOL key payload"));
    }
}
