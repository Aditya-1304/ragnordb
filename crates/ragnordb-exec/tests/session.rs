use ragnordb_common::{
    Error,
    codec::{Row, Value},
    ids::{Timestamp, TxnId},
};
use ragnordb_exec::{ExecutionResult, LocalExecutor, ResultSet, SqlSession};
use ragnordb_txn::LocalTransactionManager;

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
        .unwrap();
}

fn result_set(result: ExecutionResult) -> ResultSet {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query result");
    };

    result
}

#[test]
fn autocommit_tracks_explicit_transaction_state() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    assert!(session.autocommit());
    assert!(!session.has_active_transaction());

    session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    assert!(!session.autocommit());
    assert!(session.has_active_transaction());

    session
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .unwrap();

    assert!(session.autocommit());
    assert!(!session.has_active_transaction());

    session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    assert!(!session.autocommit());

    session
        .execute_sql("ROLLBACK", &mut executor, &mut manager)
        .unwrap();

    assert!(session.autocommit());
    assert!(!session.has_active_transaction());
}

#[test]
fn standalone_dml_commits_automatically() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    session
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            &mut executor,
            &mut manager,
        )
        .unwrap();

    assert!(session.autocommit());
    assert!(!session.has_active_transaction());

    let rows = result_set(
        session
            .execute_sql("SELECT id, name FROM users", &mut executor, &mut manager)
            .unwrap(),
    );

    assert_eq!(
        rows.rows,
        vec![Row {
            values: vec![Value::Int(1), Value::Text("Ada".to_string())],
        }]
    );
}

#[test]
fn failed_implicit_statement_leaves_no_visible_writes() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    let error = session
        .execute_sql(
            "INSERT INTO users (id, name)
             VALUES (1, 'Ada'), (1, 'Duplicate')",
            &mut executor,
            &mut manager,
        )
        .unwrap_err();

    assert!(matches!(error, Error::ConstraintViolation(_)));
    assert!(session.autocommit());
    assert!(!session.has_active_transaction());

    let rows = result_set(
        session
            .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
            .unwrap(),
    );

    assert!(rows.rows.is_empty());
}

#[test]
fn explicit_transaction_supports_read_your_writes_and_commit() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut writer = SqlSession::new();
    let mut reader = SqlSession::new();

    create_users(&mut writer, &mut executor, &mut manager);

    let started = writer
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    assert!(matches!(
        started,
        ExecutionResult::TransactionStarted {
            transaction_id: TxnId(1),
            start_ts: Timestamp(1),
        }
    ));

    writer
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            &mut executor,
            &mut manager,
        )
        .unwrap();

    let outside_rows = result_set(
        reader
            .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
            .unwrap(),
    );

    assert!(outside_rows.rows.is_empty());

    let own_rows = result_set(
        writer
            .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
            .unwrap(),
    );

    assert_eq!(
        own_rows.rows,
        vec![Row {
            values: vec![Value::Int(1)],
        }]
    );

    let committed = writer
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .unwrap();

    assert!(matches!(
        committed,
        ExecutionResult::TransactionCommitted {
            transaction_id: TxnId(1),
            commit_ts: Some(Timestamp(3)),
            committed_writes: 1,
        }
    ));
    assert!(writer.autocommit());

    let committed_rows = result_set(
        reader
            .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
            .unwrap(),
    );

    assert_eq!(
        committed_rows.rows,
        vec![Row {
            values: vec![Value::Int(1)],
        }]
    );
}

#[test]
fn explicit_read_only_commit_does_not_allocate_commit_timestamp() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    assert_eq!(manager.last_allocated_timestamp(), Timestamp(1));

    session
        .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
        .unwrap();

    assert_eq!(manager.last_allocated_timestamp(), Timestamp(1));

    let committed = session
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .unwrap();

    assert!(matches!(
        committed,
        ExecutionResult::TransactionCommitted {
            transaction_id: TxnId(1),
            commit_ts: None,
            committed_writes: 0,
        }
    ));
    assert_eq!(manager.last_allocated_timestamp(), Timestamp(1));
    assert!(session.autocommit());
}

#[test]
fn rollback_clears_explicit_transaction_and_discards_writes() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            &mut executor,
            &mut manager,
        )
        .unwrap();

    let rolled_back = session
        .execute_sql("ROLLBACK", &mut executor, &mut manager)
        .unwrap();

    assert!(matches!(
        rolled_back,
        ExecutionResult::TransactionRolledBack {
            transaction_id: TxnId(1),
            discarded_writes: 1,
        }
    ));
    assert!(session.autocommit());

    let rows = result_set(
        session
            .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
            .unwrap(),
    );

    assert!(rows.rows.is_empty());
}

#[test]
fn explicit_statement_error_preserves_earlier_successful_statements() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            &mut executor,
            &mut manager,
        )
        .unwrap();

    let error = session
        .execute_sql(
            "INSERT INTO users (id, name)
             VALUES (2, 'Grace'), (2, 'Duplicate')",
            &mut executor,
            &mut manager,
        )
        .unwrap_err();

    assert!(matches!(error, Error::ConstraintViolation(_)));
    assert!(session.has_active_transaction());
    assert!(!session.autocommit());

    session
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .unwrap();

    let rows = result_set(
        session
            .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
            .unwrap(),
    );

    assert_eq!(
        rows.rows,
        vec![Row {
            values: vec![Value::Int(1)],
        }]
    );
}

#[test]
fn analysis_error_preserves_explicit_transaction() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            &mut executor,
            &mut manager,
        )
        .unwrap();

    let error = session
        .execute_sql(
            "SELECT missing_column FROM users",
            &mut executor,
            &mut manager,
        )
        .unwrap_err();

    assert!(matches!(error, Error::SchemaMismatch(_)));
    assert!(session.has_active_transaction());

    session
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .unwrap();

    let rows = result_set(
        session
            .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
            .unwrap(),
    );

    assert_eq!(
        rows.rows,
        vec![Row {
            values: vec![Value::Int(1)],
        }]
    );
}

#[test]
fn write_conflict_during_commit_clears_losing_session() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut losing_session = SqlSession::new();
    let mut winning_session = SqlSession::new();

    create_users(&mut losing_session, &mut executor, &mut manager);

    losing_session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    losing_session
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Losing writer')",
            &mut executor,
            &mut manager,
        )
        .unwrap();

    winning_session
        .execute_sql(
            "INSERT INTO users (id, name) VALUES (1, 'Winning writer')",
            &mut executor,
            &mut manager,
        )
        .unwrap();

    let error = losing_session
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .unwrap_err();

    assert!(matches!(error, Error::WriteConflict(_)));
    assert!(!losing_session.has_active_transaction());
    assert!(losing_session.autocommit());

    let rows = result_set(
        winning_session
            .execute_sql(
                "SELECT name FROM users WHERE id = 1",
                &mut executor,
                &mut manager,
            )
            .unwrap(),
    );

    assert_eq!(
        rows.rows,
        vec![Row {
            values: vec![Value::Text("Winning writer".to_string())],
        }]
    );
}

#[test]
fn shared_manager_allocates_unique_metadata_across_sessions() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut first_session = SqlSession::new();
    let mut second_session = SqlSession::new();

    let first = first_session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    let second = second_session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    assert!(matches!(
        first,
        ExecutionResult::TransactionStarted {
            transaction_id: TxnId(1),
            start_ts: Timestamp(1),
        }
    ));

    assert!(matches!(
        second,
        ExecutionResult::TransactionStarted {
            transaction_id: TxnId(2),
            start_ts: Timestamp(2),
        }
    ));

    first_session
        .execute_sql("ROLLBACK", &mut executor, &mut manager)
        .unwrap();

    second_session
        .execute_sql("ROLLBACK", &mut executor, &mut manager)
        .unwrap();
}

#[test]
fn transaction_control_requires_valid_session_state() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    let commit_error = session
        .execute_sql("COMMIT", &mut executor, &mut manager)
        .unwrap_err();

    assert!(matches!(commit_error, Error::InvalidArgument(_)));

    let rollback_error = session
        .execute_sql("ROLLBACK", &mut executor, &mut manager)
        .unwrap_err();

    assert!(matches!(rollback_error, Error::InvalidArgument(_)));

    session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    let nested_begin = session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap_err();

    assert!(matches!(nested_begin, Error::InvalidArgument(_)));
    assert!(session.has_active_transaction());
}

#[test]
fn create_table_is_rejected_inside_explicit_transaction() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    session
        .execute_sql("BEGIN", &mut executor, &mut manager)
        .unwrap();

    let error = session
        .execute_sql(
            "CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
            &mut executor,
            &mut manager,
        )
        .unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(_)));
    assert!(session.has_active_transaction());

    session
        .execute_sql("ROLLBACK", &mut executor, &mut manager)
        .unwrap();
}

#[test]
fn read_only_implicit_transaction_does_not_allocate_commit_timestamp() {
    let mut executor = LocalExecutor::new();
    let mut manager = LocalTransactionManager::new();
    let mut session = SqlSession::new();

    create_users(&mut session, &mut executor, &mut manager);

    session
        .execute_sql("SELECT id FROM users", &mut executor, &mut manager)
        .unwrap();

    assert_eq!(manager.last_allocated_timestamp(), Timestamp(1));
}
