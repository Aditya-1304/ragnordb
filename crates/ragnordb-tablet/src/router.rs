//! Deterministic tablet-selection primitives.
//!
//! This module deliberately contains no metadata or process-local state. It
//! maps the canonical primary-key bytes for a table to a hash bucket so that
//! every node can independently derive the same tablet assignment from the
//! same routing inputs.

use ragnordb_common::{Error, Result, ids::TableId};
use ragnordb_storage::key::decode_primary_key;

const HASH_ROUTING_DOMAIN: &[u8] = b"ragnordb/tablet-hash";

/// Version of the serialized input used by [`HashTabletPartitioner`].
///
/// This is part of the routing contract. A future incompatible hash-input
/// change must use a new version instead of silently moving existing keys.
pub const HASH_ROUTING_VERSION: u8 = 1;

/// Stateless hash partitioner for table primary keys
#[derive(Debug, Clone, Copy, Default)]
pub struct HashTabletPartitioner;

impl HashTabletPartitioner {
    /// creates a partitioner with no node local configuration
    pub const fn new() -> Self {
        Self
    }

    /// selects the hash bucket for a canonical primary key
    ///
    /// the digest input is domain-separated and length-delimited:
    /// `domain || version || table_id || key_length || primary_key_bytes`.
    /// Table IDs and lengths use big-endian encoding, and the first eight
    /// digest bytes are interpreted as a big-endian integer before reduction
    /// by `bucket_count`
    ///
    /// Validating the key bytes here prevents callers from routing a key using
    /// an encoding that the storage layer would not recognize as a canonical
    /// primary key. The method does not consult metadata, leaders, or local
    /// tablet state, so its result is identical on every node
    pub fn bucket_for(
        &self,
        table_id: TableId,
        primary_key_bytes: &[u8],
        bucket_count: u32,
    ) -> Result<u32> {
        if table_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "tablet routing requires a non-zero table ID".to_string(),
            ));
        }

        if bucket_count == 0 {
            return Err(Error::InvalidArgument(
                "tablet routing requires a non-zero bucket count".to_string(),
            ));
        }

        decode_primary_key(primary_key_bytes).map_err(|error| {
            Error::InvalidArgument(format!(
                "tablet routing requires canonical primary-key bytes: {error}"
            ))
        })?;

        let key_length = u64::try_from(primary_key_bytes.len()).map_err(|_| {
            Error::InvalidArgument("primary-key byte length exceeds the routing format".to_string())
        })?;

        let mut hasher = blake3::Hasher::new();
        hasher.update(HASH_ROUTING_DOMAIN);
        hasher.update(&[HASH_ROUTING_VERSION]);
        hasher.update(&table_id.0.to_be_bytes());
        hasher.update(&key_length.to_be_bytes());
        hasher.update(primary_key_bytes);

        let digest = hasher.finalize();
        let mut prefix = [0_u8; 8];
        prefix.copy_from_slice(&digest.as_bytes()[..8]);

        Ok((u64::from_be_bytes(prefix) % u64::from(bucket_count)) as u32)
    }
}
