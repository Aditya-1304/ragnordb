use ragnordb_common::Result;

pub mod key;
pub mod mvcc;
pub mod wal;

/// Abstract key-value storage engine.
///
/// Currently a stub. Will be implemented by:
///   - In-memory MVCC maps (Phase 2.6)
///   - A-WAL-backed single-node storage (Phase 3)
///   - Raft-backed replicated storage (Phase 4)
///   - Durable sorted segments + Bloom filters (Phase 9)
pub trait StorageEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
}
