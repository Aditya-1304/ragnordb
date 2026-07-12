use ragnordb_common::ids::{Timestamp, TxnId};

/// A handle for a single transaction.
///
/// Currently a stub. Will be extended to track:
///   - read set (keys read at start_ts)
///   - write set (buffered mutations)
///   - participant tablets
///   - transaction status location
///   - heartbeat state
///   - retry state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transaction {
    pub id: TxnId,
    pub start_ts: Timestamp,
}
