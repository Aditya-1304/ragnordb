use std::sync::{Arc, Mutex};

use ragnordb_common::{
    Error, Result,
    codec::{Row, Value},
};
use ragnordb_exec::{ExecutionResult, LocalExecutor, ResultSet, SqlSession};
use ragnordb_storage::wal::{DurableCommitLog, DurableWalExtent, SingleNodeTxnCommit};
use ragnordb_txn::LocalTransactionManager;

#[derive(Debug, Clone, Copy)]
enum NextAppend {
    Succeed,
    OutcomeUnknown,
}

struct CommitLogState {
    next_lsn: u64,
    next_append: NextAppend,
    records: Vec<SingleNodeTxnCommit>,
}

struct TestCommitLog {
    state: Mutex<CommitLogState>,
}

impl TestCommitLog {
    fn new() -> Self {
        Self {
            state: Mutex::new(CommitLogState {
                next_lsn: 0,
                next_append: NextAppend::Succeed,
                records: Vec::new(),
            }),
        }
    }

    fn fail_next_with_outcome_unknown(&self) {
        self.state
            .lock()
            .expect("test commit-log mutex must not be poisoned")
            .next_append = NextAppend::OutcomeUnknown;
    }

    fn records(&self) -> Vec<SingleNodeTxnCommit> {
        self.state
            .lock()
            .expect("test commit-log mutex must not be poisoned")
            .records
            .clone()
    }
}

impl DurableCommitLog for TestCommitLog {
    fn append_single_node_commit(&self, commit: &SingleNodeTxnCommit) -> Result<DurableWalExtent> {
        // Exercise the same semantic validation performed before the real
        // adapter crosses into A-WAL.
        commit.encode()?;

        let mut state = self
            .state
            .lock()
            .expect("test commit-log mutex must not be poisoned");

        let start_lsn = state.next_lsn;
        let end_lsn = start_lsn
            .checked_add(1)
            .expect("test logical LSN space must not overflow");

        state.next_lsn = end_lsn;
        state.records.push(commit.clone());

        match std::mem::replace(&mut state.next_append, NextAppend::Succeed) {
            NextAppend::Succeed => Ok(DurableWalExtent::from_raw(start_lsn, end_lsn)),

            NextAppend::OutcomeUnknown => Err(Error::CommitOutcomeUnknown {
                start_lsn,
                end_lsn,
                reason: "injected post-staging synchronization failure".to_string(),
            }),
        }
    }
}

fn executor_with_log(log: Arc<TestCommitLog>) -> LocalExecutor {
    LocalExecutor::with_commit_log(log)
}

fn create_users(
    session: &mut SqlSession,
    executor: &mut LocalExecutor,
    manager: &mut LocalTransactionManager,
) {
    session
        .execute_sql(
            "CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
            executor,
            manager,
        )
        .expect("users table creation must succeed");
}

fn result_set(result: ExecutionResult) -> ResultSet {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query result");
    };

    result
}

/// Realistic bug caught:
///
/// Autocommit DML could still use the old direct tablet commit path, making the
/// row visible without constructing a durable transaction record.
#[test]
fn autocommit_write_uses_durable_commit_coordinator() {
    let log = Arc::new(TestCommitLog::new());
    let mut executor = executor_with_log(log.clone());
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    session
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            &mut executor,
            &mut manager,
        )
        .expect("autocommit insert must succeed");

    let records = log.records();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].writes.len(), 1);
    assert_eq!(
        records[0].commit_timestamp.0,
        records[0].start_timestamp.0 + 1
    );

    let rows = result_set(
        session
            .execute_sql("SELECT id, name FROM users", &mut executor, &mut manager)
            .expect("committed row must be readable"),
    );

    assert_eq!(
        rows.rows,
        vec![Row {
            values: vec![Value::Int(1), Value::Text("Ada".to_string()),],
        }]
    );
}

/// Realistic bug caught:
///
/// Explicit COMMIT could bypass the durable coordinator even when autocommit
/// statements use it correctly.
#[test]
fn explicit_commit_uses_durable_commit_coordinator() {
    let log = Arc::new(TestCommitLog::new());
    let mut executor = executor_with_log(log.clone());
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .expect("BEGIN must succeed");

    session
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            &mut executor,
            &mut manager,
        )
        .expect("explicit insert must buffer");

    assert!(log.records().is_empty());
    assert!(session.has_active_transaction());

    let committed = session
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .expect("explicit commit must succeed");

    assert!(matches!(
        committed,
        ExecutionResult::TransactionCommitted {
            committed_writes: 1,
            commit_ts: Some(_),
            ..
        }
    ));

    assert_eq!(log.records().len(), 1);
    assert!(!session.has_active_transaction());
}

/// Realistic bug caught:
///
/// A read-only explicit transaction could allocate a commit timestamp or append
/// an empty transaction record while being routed through the new coordinator.
#[test]
fn explicit_read_only_commit_remains_wal_free() {
    let log = Arc::new(TestCommitLog::new());
    let mut executor = executor_with_log(log.clone());
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .expect("BEGIN must succeed");

    let committed = session
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .expect("read-only commit must succeed");

    assert!(matches!(
        committed,
        ExecutionResult::TransactionCommitted {
            committed_writes: 0,
            commit_ts: None,
            ..
        }
    ));

    assert!(log.records().is_empty());
    assert!(!session.has_active_transaction());
}

/// Realistic bug caught:
///
/// A post-staging synchronization failure could leave the explicit transaction
/// attached to the session, allowing its uncertain write set to be committed or
/// retried again.
#[test]
fn explicit_outcome_unknown_clears_session_without_mvcc_visibility() {
    let log = Arc::new(TestCommitLog::new());
    let mut executor = executor_with_log(log.clone());
    let mut manager = LocalTransactionManager::new();
    let mut writer = SqlSession::new();
    let mut reader = SqlSession::new();

    create_users(&mut writer, &mut executor, &mut manager);

    writer
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .expect("BEGIN must succeed");

    writer
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            &mut executor,
            &mut manager,
        )
        .expect("insert must buffer");

    log.fail_next_with_outcome_unknown();

    let error = writer
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .unwrap_err();

    assert!(matches!(error, Error::CommitOutcomeUnknown { .. }));

    assert!(!writer.has_active_transaction());
    assert!(writer.autocommit());
    assert_eq!(log.records().len(), 1);

    let rows = result_set(
        reader
            .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
            .expect("read-only query must remain available"),
    );

    assert!(rows.rows.is_empty());
}

/// Realistic bug caught:
///
/// An implicit write whose WAL outcome becomes unknown could expose its MVCC
/// mutation or remain reusable even though no SQL transaction is attached.
#[test]
fn implicit_outcome_unknown_does_not_apply_mvcc_state() {
    let log = Arc::new(TestCommitLog::new());
    let mut executor = executor_with_log(log.clone());
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    log.fail_next_with_outcome_unknown();

    let error = session
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            &mut executor,
            &mut manager,
        )
        .unwrap_err();

    assert!(matches!(error, Error::CommitOutcomeUnknown { .. }));

    assert!(session.autocommit());
    assert!(!session.has_active_transaction());

    let rows = result_set(
        session
            .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
            .expect("read-only query must remain available"),
    );

    assert!(rows.rows.is_empty());
}

/// Realistic bug caught:
///
/// A transaction that loses MVCC preflight could still append a second durable
/// record before the conflict is discovered.
#[test]
fn explicit_preflight_conflict_appends_no_losing_record() {
    let log = Arc::new(TestCommitLog::new());
    let mut executor = executor_with_log(log.clone());
    let mut manager = LocalTransactionManager::new();
    let mut first = SqlSession::new();
    let mut second = SqlSession::new();

    create_users(&mut first, &mut executor, &mut manager);

    first
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .expect("first BEGIN must succeed");

    second
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .expect("second BEGIN must succeed");

    first
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'first')",
            &mut executor,
            &mut manager,
        )
        .expect("first insert must buffer");

    second
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'second')",
            &mut executor,
            &mut manager,
        )
        .expect("second insert must buffer");

    second
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .expect("second transaction must commit");

    assert_eq!(log.records().len(), 1);

    let error = first
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .unwrap_err();

    assert!(matches!(error, Error::WriteConflict(_)));
    assert_eq!(log.records().len(), 1);
    assert!(!first.has_active_transaction());
}
