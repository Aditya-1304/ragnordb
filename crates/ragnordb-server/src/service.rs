use ragnordb_common::ids::TxnId;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

#[derive(Debug, Clone)]
pub struct Session {
    pub session_id: SessionId,
    pub current_txn: Option<TxnId>,
    pub autocommit: bool,
}

impl Session {
    pub fn new() -> Self {
        let id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
        Session {
            session_id: SessionId(id),
            current_txn: None,
            autocommit: true,
        }
    }

    pub fn begin_txn(&mut self, txn_id: TxnId) {
        self.current_txn = Some(txn_id);
    }

    pub fn end_txn(&mut self) {
        self.current_txn = None;
    }
}
