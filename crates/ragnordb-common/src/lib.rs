//! root file of this crate, used to re-export Error and Result so other crates can use it
//!
//! also maps protos sub-module to:
//!  ids          - domain ID newtypes (NodeId, TxnId, etc.)
//!  codec        - row values + MVCC record types + proto roundtrips
//!  catalog_codec– catalog-specific codecs (ColumnDef, TableDef, DataType)
//!  command_codec– TabletCommand variants + proto roundtrips
//!  rpc_codec    - inter-node frame + metadata request/response codecs
//!  protocol     - V1 TCP frame reading/writing

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
}

pub use result::{Error, Result};
