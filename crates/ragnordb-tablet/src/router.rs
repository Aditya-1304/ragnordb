//! Deterministic tablet-selection primitives.
//!
//! This module deliberately contains no metadata or process-local state. It
//! maps the canonical primary-key bytes for a table to a hash bucket so that
//! every node can independently derive the same tablet assignment from the
//! same routing inputs.

use std::collections::{BTreeMap, BTreeSet};

use ragnordb_common::{
    Error, Result,
    ids::{TableId, TabletId},
    metadata_codec::{PartitionSpec, TabletDescriptor},
};
use ragnordb_storage::key::decode_primary_key;

const HASH_ROUTING_DOMAIN: &[u8] = b"ragnordb/tablet-hash";

/// Version of the serialized input used by [`HashTabletPartitioner`].
///
/// This is part of the routing contract. A future incompatible hash-input
/// change must use a new version instead of silently moving existing keys.
pub const HASH_ROUTING_VERSION: u8 = 1;

/// Stateless hash partitioner for table primary keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HashTabletPartitioner;

impl HashTabletPartitioner {
    /// Creates a partitioner with no node-local configuration.
    pub const fn new() -> Self {
        Self
    }

    /// Selects the hash bucket for a canonical primary key.
    ///
    /// the digest input is domain-separated and length-delimited:
    /// `domain || version || table_id || key_length || primary_key_bytes`.
    /// Table IDs and lengths use big-endian encoding, and the first eight
    /// digest bytes are interpreted as a big-endian integer before reduction
    /// by `bucket_count`.
    ///
    /// Validating the key bytes here prevents callers from routing a key using
    /// an encoding that the storage layer would not recognize as a canonical
    /// primary key. The method does not consult metadata, leaders, or local
    /// tablet state, so its result is identical on every node.
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

/// Immutable routing view built from one table's metadata tablet descriptors.
///
/// The constructor requires exactly one valid descriptor for every hash bucket.
/// This turns metadata validation into a routing invariant: a point operation
/// always resolves to one tablet, while a scan always has a complete and stable
/// bucket-ordered fan-out set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletRouter {
    table_id: TableId,
    bucket_count: u32,
    partitioner: HashTabletPartitioner,
    tablet_ids_by_bucket: BTreeMap<u32, TabletId>,
}

impl TabletRouter {
    /// Builds the compatibility route used by the local single-tablet engine.
    ///
    /// This constructor intentionally does not manufacture a metadata
    /// [`TabletDescriptor`]. A local compatibility tablet has no independent
    /// metadata Raft identity yet, so callers must use [`Self::new`] whenever
    /// the route comes from authoritative metadata.
    pub fn for_single_tablet(table_id: TableId, tablet_id: TabletId) -> Result<Self> {
        if table_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "tablet routing requires a non-zero table ID".to_string(),
            ));
        }

        if tablet_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "tablet routing requires a non-zero tablet ID".to_string(),
            ));
        }

        Ok(Self {
            table_id,
            bucket_count: 1,
            partitioner: HashTabletPartitioner::new(),
            tablet_ids_by_bucket: BTreeMap::from([(0, tablet_id)]),
        })
    }

    /// Builds a router from the authoritative metadata descriptors for one table.
    ///
    /// Descriptors may arrive in any order, but they must describe the same
    /// table, use one bucket count, cover every bucket exactly once, and use a
    /// distinct tablet identity for each bucket. Rejecting an incomplete view
    /// prevents a caller from treating a partial metadata snapshot as a valid
    /// routing table.
    pub fn new(table_id: TableId, descriptors: &[TabletDescriptor]) -> Result<Self> {
        if table_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "tablet routing requires a non-zero table ID".to_string(),
            ));
        }

        if descriptors.is_empty() {
            return Err(Error::InvalidArgument(
                "tablet routing requires at least one tablet descriptor".to_string(),
            ));
        }

        let mut bucket_count = None;
        let mut tablet_ids_by_bucket = BTreeMap::new();
        let mut tablet_ids = BTreeSet::new();

        for descriptor in descriptors {
            descriptor.validate().map_err(|error| {
                Error::InvalidArgument(format!(
                    "invalid tablet descriptor {}: {error}",
                    descriptor.tablet_id.0
                ))
            })?;

            if descriptor.table_id != table_id {
                return Err(Error::InvalidArgument(format!(
                    "tablet descriptor {} belongs to table {}, expected table {}",
                    descriptor.tablet_id.0, descriptor.table_id.0, table_id.0
                )));
            }

            let (bucket, descriptor_bucket_count) = match descriptor.partition {
                PartitionSpec::Hash {
                    bucket,
                    bucket_count,
                } => (bucket, bucket_count),
            };

            if let Some(expected) = bucket_count {
                if expected != descriptor_bucket_count {
                    return Err(Error::InvalidArgument(format!(
                        "tablet routing has inconsistent bucket counts: expected {}, received {}",
                        expected, descriptor_bucket_count
                    )));
                }
            } else {
                bucket_count = Some(descriptor_bucket_count);
            }

            if tablet_ids_by_bucket
                .insert(bucket, descriptor.tablet_id)
                .is_some()
            {
                return Err(Error::InvalidArgument(format!(
                    "tablet routing has duplicate bucket {} for table {}",
                    bucket, table_id.0
                )));
            }

            if !tablet_ids.insert(descriptor.tablet_id) {
                return Err(Error::InvalidArgument(format!(
                    "tablet routing reuses tablet {} across buckets",
                    descriptor.tablet_id.0
                )));
            }
        }

        let bucket_count = bucket_count.ok_or_else(|| {
            Error::InvalidArgument("tablet routing has no bucket count".to_string())
        })?;

        let expected_descriptor_count = usize::try_from(bucket_count).map_err(|_| {
            Error::InvalidArgument(
                "tablet routing bucket count exceeds addressable memory".to_string(),
            )
        })?;

        if tablet_ids_by_bucket.len() != expected_descriptor_count {
            return Err(Error::InvalidArgument(format!(
                "tablet routing requires one descriptor for each of {} buckets, received {}",
                bucket_count,
                tablet_ids_by_bucket.len()
            )));
        }

        Ok(Self {
            table_id,
            bucket_count,
            partitioner: HashTabletPartitioner::new(),
            tablet_ids_by_bucket,
        })
    }

    /// Return the table identity represented by this routing view.
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Return the number of hash buckets represented by this routing view.
    pub const fn tablet_count(&self) -> u32 {
        self.bucket_count
    }

    /// Route one canonical primary key to exactly one tablet.
    pub fn route_point(&self, primary_key_bytes: &[u8]) -> Result<TabletId> {
        let bucket =
            self.partitioner
                .bucket_for(self.table_id, primary_key_bytes, self.bucket_count)?;

        self.tablet_ids_by_bucket
            .get(&bucket)
            .copied()
            .ok_or_else(|| {
                Error::CorruptData(format!(
                    "validated tablet routing table is missing bucket {} for table {}",
                    bucket, self.table_id.0
                ))
            })
    }

    /// Route a scan to every tablet in deterministic hash-bucket order.
    pub fn route_scan(&self) -> Vec<TabletId> {
        self.tablet_ids_by_bucket.values().copied().collect()
    }
}
