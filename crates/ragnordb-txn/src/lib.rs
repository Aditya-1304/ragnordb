use ragnordb_common::ids::{Timestamp, TxnId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transaction {
    pub id: TxnId,
    pub start_ts: Timestamp,
}
