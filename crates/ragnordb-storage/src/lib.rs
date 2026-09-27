use ragnordb_common::Result;

pub mod checkpoint;
pub mod key;
pub mod lsm;
pub mod mvcc;
pub mod recovery;
pub mod wal;

/// Abstract key-value storage engine.
///
/// This remains a compatibility boundary while the tablet-local LSM is
/// developed. The LSM modules define their own manifest, segment, and MVCC
/// contracts rather than treating this generic interface as the recovery
/// authority.
pub trait StorageEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
}
