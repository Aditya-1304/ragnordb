//! Deterministic storage encoding for SQL rows and values
//!
//! protobuf remains the durable abd cross node message format
//! this module defines the canonical byte representation stored as
//! row payloads by the storage layer
//!
//! row format v1:
//! ```text
//! [version: u8]
//! [value_count: u32 big-endian]
//! [encoded_value...]
//! ```
//!
//! value formats:
//!
//! ```text
//! NULL  = [tag]
//! INT   = [tag][i64 big-endian]
//! TEXT  = [tag][length: u32 big-endian][UTF-8 bytes]
//! BOOL  = [tag][0 or 1]
//! ```
//!
//! Decoders reject trailing, truncated and non canonical input so corrupted
//! storage bytes cannot silently produce a different logical row

use crate::codec::{Row, Value};
use crate::{Error, Result};

const ROW_FORMAT_VERSION: u8 = 1;

const VALUE_TAG_NULL: u8 = 0x00;
const VALUE_TAG_INT: u8 = 0x01;
const VALUE_TAG_TEXT: u8 = 0x02;
const VALUE_TAG_BOOL: u8 = 0x03;

/// Encode one SQL value into its deterministic storage representation
pub fn encode_value(value: &Value) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    encode_value_into(value, &mut output)?;

    Ok(output)
}

/// this decodes one complete SQL value
///
/// trailing byte sare rejected cause accepting them would make multiple
/// byte sequences represent the same logical value
pub fn decode_value(bytes: &[u8]) -> Result<Value> {
    let mut decoder = Decoder::new(bytes, "value");
    let value = decode_value_from(&mut decoder)?;

    decoder.finish()?;
    Ok(value)
}

/// this encodes a complete SQL row
///
/// Values are encoded in their existing row-layout. the encoder does not
/// reorder cells or applu schema dependent transformation
pub fn encode_row(row: &Row) -> Result<Vec<u8>> {
    let value_count = u32::try_from(row.values.len())
        .map_err(|_| Error::InvalidArgument("row contsins too may values to encode".to_string()))?;
    let mut output = Vec::new();
    output.push(ROW_FORMAT_VERSION);
    output.extend_from_slice(&value_count.to_be_bytes());

    for value in &row.values {
        encode_value_into(value, &mut output)?;
    }

    Ok(output)
}

/// this decodes one complete row from its canonical storage representation
pub fn decode_row(bytes: &[u8]) -> Result<Row> {
    let mut decoder = Decoder::new(bytes, "row");

    let version = decoder.read_u8()?;

    if version != ROW_FORMAT_VERSION {
        return Err(invalid_encoding(
            "row",
            format!("unsupported format version {version}"),
        ));
    }
    let value_count = decoder.read_u32()? as usize;
    let mut values = Vec::with_capacity(value_count);

    for _ in 0..value_count {
        values.push(decode_value_from(&mut decoder)?);
    }

    decoder.finish()?;

    Ok(Row { values })
}

/// Append one value to an existing row or value buffer.
fn encode_value_into(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => {
            output.push(VALUE_TAG_NULL);
        }
        Value::Int(value) => {
            output.push(VALUE_TAG_INT);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Value::Text(value) => {
            let length = u32::try_from(value.len()).map_err(|_| {
                Error::InvalidArgument("TEXT value is too large to encode".to_string())
            })?;

            output.push(VALUE_TAG_TEXT);
            output.extend_from_slice(&length.to_be_bytes());
            output.extend_from_slice(value.as_bytes());
        }
        Value::Bool(value) => {
            output.push(VALUE_TAG_BOOL);
            output.push(u8::from(*value));
        }
    }

    Ok(())
}

/// Decode one value from the decoder's current position.
fn decode_value_from(decoder: &mut Decoder<'_>) -> Result<Value> {
    match decoder.read_u8()? {
        VALUE_TAG_NULL => Ok(Value::Null),

        VALUE_TAG_INT => {
            let bytes = decoder.read_exact(8)?;
            let mut encoded = [0_u8; 8];

            encoded.copy_from_slice(bytes);

            Ok(Value::Int(i64::from_be_bytes(encoded)))
        }

        VALUE_TAG_TEXT => {
            let length = decoder.read_u32()? as usize;
            let bytes = decoder.read_exact(length)?;

            let text = std::str::from_utf8(bytes).map_err(|error| {
                invalid_encoding(
                    decoder.context,
                    format!("TEXT value is not valid UTF-8: {error}"),
                )
            })?;

            Ok(Value::Text(text.to_string()))
        }

        VALUE_TAG_BOOL => {
            let encoded = decoder.read_u8()?;

            match encoded {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                other => Err(invalid_encoding(
                    decoder.context,
                    format!("invalid BOOL payload {other}"),
                )),
            }
        }

        tag => Err(invalid_encoding(
            decoder.context,
            format!("unknown value tag 0x{tag:02x}"),
        )),
    }
}

/// Bounds-checked cursor used by the storage decoders.
struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
    context: &'static str,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8], context: &'static str) -> Self {
        Self {
            bytes,
            position: 0,
            context,
        }
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_exact(4)?;
        let mut encoded = [0_u8; 4];

        encoded.copy_from_slice(bytes);

        Ok(u32::from_be_bytes(encoded))
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| invalid_encoding(self.context, "encoded length overflow"))?;

        if end > self.bytes.len() {
            return Err(invalid_encoding(
                self.context,
                format!(
                    "truncated input at byte {}, needed {} more bytes",
                    self.position,
                    end - self.bytes.len()
                ),
            ));
        }

        let result = &self.bytes[self.position..end];
        self.position = end;

        Ok(result)
    }

    fn finish(&self) -> Result<()> {
        if self.position != self.bytes.len() {
            return Err(invalid_encoding(
                self.context,
                format!(
                    "{} trailing bytes after encoded value",
                    self.bytes.len() - self.position
                ),
            ));
        }

        Ok(())
    }
}

fn invalid_encoding(context: &str, message: impl std::fmt::Display) -> Error {
    Error::InvalidArgument(format!("invalid {context} encoding: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_values() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Int(i64::MIN),
            Value::Int(-1),
            Value::Int(0),
            Value::Int(i64::MAX),
            Value::Text(String::new()),
            Value::Text("Ada Lovelace".to_string()),
            Value::Text("RagnorDB 🦀".to_string()),
            Value::Bool(false),
            Value::Bool(true),
        ]
    }

    #[test]
    fn every_value_type_roundtrips() {
        for value in sample_values() {
            let encoded = encode_value(&value).unwrap();
            let decoded = decode_value(&encoded).unwrap();

            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn value_encoding_is_deterministic() {
        let value = Value::Text("deterministic".to_string());

        assert_eq!(encode_value(&value).unwrap(), encode_value(&value).unwrap());
    }

    #[test]
    fn mixed_row_roundtrips() {
        let row = Row {
            values: vec![
                Value::Int(-42),
                Value::Text("Ada".to_string()),
                Value::Bool(true),
                Value::Null,
            ],
        };

        let encoded = encode_row(&row).unwrap();
        let decoded = decode_row(&encoded).unwrap();

        assert_eq!(decoded, row);
    }

    #[test]
    fn empty_row_roundtrips() {
        let row = Row { values: vec![] };

        let encoded = encode_row(&row).unwrap();
        let decoded = decode_row(&encoded).unwrap();

        assert_eq!(decoded, row);
    }

    #[test]
    fn row_encoding_is_deterministic() {
        let row = Row {
            values: sample_values(),
        };

        assert_eq!(encode_row(&row).unwrap(), encode_row(&row).unwrap());
    }

    #[test]
    fn different_row_orders_produce_different_bytes() {
        let first = Row {
            values: vec![Value::Int(1), Value::Int(2)],
        };

        let second = Row {
            values: vec![Value::Int(2), Value::Int(1)],
        };

        assert_ne!(encode_row(&first).unwrap(), encode_row(&second).unwrap());
    }

    #[test]
    fn rejects_unknown_row_version() {
        let bytes = [99, 0, 0, 0, 0];

        let error = decode_row(&bytes).unwrap_err();

        assert!(error.to_string().contains("unsupported format version 99"));
    }

    #[test]
    fn rejects_unknown_value_tag() {
        let error = decode_value(&[0xff]).unwrap_err();

        assert!(error.to_string().contains("unknown value tag"));
    }

    #[test]
    fn rejects_non_canonical_bool_payload() {
        let error = decode_value(&[VALUE_TAG_BOOL, 2]).unwrap_err();

        assert!(error.to_string().contains("invalid BOOL payload"));
    }

    #[test]
    fn rejects_truncated_integer() {
        let error = decode_value(&[VALUE_TAG_INT, 0, 0]).unwrap_err();

        assert!(error.to_string().contains("truncated input"));
    }

    #[test]
    fn rejects_invalid_utf8_text() {
        let bytes = [VALUE_TAG_TEXT, 0, 0, 0, 1, 0xff];

        let error = decode_value(&bytes).unwrap_err();

        assert!(error.to_string().contains("not valid UTF-8"));
    }

    #[test]
    fn rejects_value_trailing_bytes() {
        let mut bytes = encode_value(&Value::Int(1)).unwrap();
        bytes.push(0xff);

        let error = decode_value(&bytes).unwrap_err();

        assert!(error.to_string().contains("trailing bytes"));
    }

    #[test]
    fn rejects_row_trailing_bytes() {
        let mut bytes = encode_row(&Row { values: vec![] }).unwrap();
        bytes.push(0xff);

        let error = decode_row(&bytes).unwrap_err();

        assert!(error.to_string().contains("trailing bytes"));
    }
}
