pub mod catalog_codec;
pub mod codec;
pub mod command_codec;
pub mod ids;
pub mod result;

pub mod proto {
    pub mod ids {
        include!("proto/ragnordb.ids.rs");
    }
    pub mod row {
        include!("proto/ragnordb.row.rs");
    }
    pub mod catalog {
        include!("proto/ragnordb.catalog.rs");
    }
    pub mod mvcc {
        include!("proto/ragnordb.mvcc.rs");
    }
    pub mod command {
        include!("proto/ragnordb.command.rs");
    }
    pub mod rpc {
        include!("proto/ragnordb.rpc.rs");
    }
}

pub use result::{Error, Result};
