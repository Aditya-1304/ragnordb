//! Connection-level client session state.
//!
//! The server session owns connection identity and request-level configuration.
//! SQL transaction state is delegated exclusively to `SqlSession`, preventing
//! the server and executor layers from maintaining competing transaction state.

use std::{
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use ragnordb_common::{
    Error, Result,
    ids::{RequestId, TxnId},
    metadata_codec::RESERVED_METADATA_RAFT_GROUP_ID,
};
use ragnordb_exec::SqlSession;

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);
static PROCESS_CLIENT_ID: OnceLock<u128> = OnceLock::new();

fn process_client_id() -> u128 {
    *PROCESS_CLIENT_ID.get_or_init(|| {
        let clock_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let process_id = u128::from(std::process::id());

        clock_nanos ^ (process_id << 64)
    })
}

/// Process-local diagnostic identity for one client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

/// Connection-level state for one SQL client.
///
/// `statement_timeout_ms` bounds how long a request may wait for the serialized
/// database execution owner. Once admitted, execution runs to its authoritative
/// durability outcome so an in-flight commit is never mislabeled as cancelled.
#[derive(Debug)]
pub struct Session {
    pub session_id: SessionId,
    pub sql: SqlSession,
    pub statement_timeout_ms: u64,

    /// Monotonic request sequence scoped to this connection identity.
    ///
    /// The sequence is allocated for potential metadata-Raft requests. Other
    /// SQL statements may consume a sequence without creating a metadata entry;
    /// this keeps request allocation independent from SQL comment syntax.
    next_metadata_sequence: u64,
}

impl Session {
    /// Construct a connection session using the V1 defaults.
    pub fn new() -> Self {
        let session_id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);

        assert_ne!(
            session_id, 0,
            "process-local session ID allocator exhausted and wrapped to zero"
        );

        Self {
            session_id: SessionId(session_id),
            sql: SqlSession::new(),
            statement_timeout_ms: 30_000,
            next_metadata_sequence: 1,
        }
    }

    /// Allocate the next request identity for the metadata Raft group.
    pub fn next_metadata_request_id(&mut self) -> Result<RequestId> {
        let sequence = self.next_metadata_sequence;
        self.next_metadata_sequence = sequence.checked_add(1).ok_or_else(|| {
            Error::Configuration("metadata request sequence space is exhausted".to_string())
        })?;

        Ok(RequestId {
            // The process-incarnation component prevents a restarted server
            // from accidentally reusing a durable request identity. The
            // connection component keeps sessions independent within one
            // process; an explicit retry must reuse its original ID rather
            // than allocate a fresh sequence.
            client_id: process_client_id() ^ u128::from(self.session_id.0),
            sequence,
            raft_group_id: RESERVED_METADATA_RAFT_GROUP_ID,
        })
    }

    /// Return whether standalone data statements use implicit transactions.
    pub fn autocommit(&self) -> bool {
        self.sql.autocommit()
    }

    /// Return whether BEGIN has attached an explicit transaction.
    pub fn has_active_transaction(&self) -> bool {
        self.sql.has_active_transaction()
    }

    /// Return the active transaction identifier, if one exists.
    pub fn current_transaction_id(&self) -> Option<TxnId> {
        self.sql.current_transaction_id()
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
    fn new_session_uses_connection_and_sql_defaults() {
        let session = Session::new();

        assert!(session.autocommit());
        assert!(!session.has_active_transaction());
        assert_eq!(session.current_transaction_id(), None);
        assert_eq!(session.statement_timeout_ms, 30_000);
    }

    #[test]
    fn session_ids_are_unique() {
        let first = Session::new();
        let second = Session::new();

        assert_ne!(first.session_id, second.session_id);
    }
}
