use ragnordb_common::Result;

pub trait StorageEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
}
