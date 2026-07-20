//! Shared domain types, durable codecs, storage encodings, and errors.
//!
//! Modules:
//!
//! - `ids`: stable domain identifier types.
//! - `codec`: row values and MVCC protobuf conversions.
//! - `encoding`: deterministic row and value storage bytes.
//! - `catalog_codec`: durable catalog definitions.
//! - `command_codec`: replicated tablet command conversions.
//! - `rpc_codec`: inter-node request and response conversions.
//! - `protocol`: V1 client frame reading and writing.

pub mod catalog_codec;
pub mod codec;
pub mod command_codec;
pub mod encoding;
pub mod ids;
pub mod protocol;
pub mod result;
pub mod rpc_codec;

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
    pub mod wal {
        include!("proto/ragnordb.wal.rs");
    }
}

pub use result::{Error, Result};
