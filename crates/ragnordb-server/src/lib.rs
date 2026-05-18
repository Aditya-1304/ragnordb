use ragnordb_common::Result;

#[derive(Debug, Default)]
pub struct Server;

impl Server {
    pub fn new() -> Self {
        Self
    }

    pub fn start(&self) -> Result<()> {
        Ok(())
    }
}
