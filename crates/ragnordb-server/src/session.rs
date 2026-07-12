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
    pub statement_timeout_ms: u64,
}

impl Session {
    pub fn new() -> Self {
        let id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
        Session {
            session_id: SessionId(id),
            current_txn: None,
            autocommit: true,
            statement_timeout_ms: 30_000,
        }
    }

    pub fn begin_txn(&mut self, txn_id: TxnId) {
        self.current_txn = Some(txn_id);
    }

    pub fn end_txn(&mut self) {
        self.current_txn = None;
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_session_uses_v1_defaults() {
        let session = Session::new();

        assert!(session.autocommit);
        assert_eq!(session.current_txn, None);
        assert_eq!(session.statement_timeout_ms, 30_000);
    }

    #[test]
    fn session_transaction_lifecycle_is_explicit() {
        let mut session = Session::new();

        session.begin_txn(TxnId(42));
        assert_eq!(session.current_txn, Some(TxnId(42)));

        session.end_txn();
        assert_eq!(session.current_txn, None);
    }

    #[test]
    fn session_ids_are_unique() {
        let first = Session::new();
        let second = Session::new();

        assert_ne!(first.session_id, second.session_id);
    }
}
