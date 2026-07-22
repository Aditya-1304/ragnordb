use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use ragnordb_common::{
    Error, Result,
    codec::{Row, Value},
    encoding::encode_row,
    ids::{TableId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    mvcc::{InMemoryMvcc, Mutation, MvccStats, MvccStorage},
    wal::{RagnorDbWalAdapter, RagnorDbWalRecordType, SingleNodeTxnCommit},
};
use ragnordb_txn::{LocalTransactionManager, SingleNodeCommitCoordinator, Transaction};
use wal::{
    config::WalConfig, io::fault::FaultDirectory, lsn::Lsn, types::WalIdentity, wal::WalHandle,
};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must follow the Unix epoch")
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "ragnordb-commit-coordinator-{prefix}-{}-{nanos}",
            process::id()
        ));

        fs::create_dir_all(&path).expect("failed to create commit coordinator test directory");

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

type TestAdapter = RagnorDbWalAdapter<FaultDirectory, ()>;
type TestWalHandle = WalHandle<FaultDirectory, ()>;

fn open_adapter(test_dir: &TestDir, directory: FaultDirectory) -> (TestAdapter, TestWalHandle) {
    let config = WalConfig {
        dir: test_dir.path().to_path_buf(),
        identity: WalIdentity::new(1, 1, 1),
        ..WalConfig::default()
    };

    let (handle, _report) =
        WalHandle::open(directory, config, ()).expect("failed to open coordinator test WAL");

    let observer = handle.clone();

    (RagnorDbWalAdapter::new(handle), observer)
}

fn encoded_key(table_id: TableId, id: i64) -> Vec<u8> {
    let key = make_row_key(table_id, &[Value::Int(id)]).expect("test row key must be valid");

    encode_row_key(&key).expect("test row key must encode")
}

fn encoded_row(id: i64, name: &str) -> Vec<u8> {
    encode_row(&Row {
        values: vec![Value::Int(id), Value::Text(name.to_string())],
    })
    .expect("test row must encode")
}

fn put_transaction(
    table_id: TableId,
    txn_id: u64,
    start_ts: u64,
    row_id: i64,
    name: &str,
) -> (Transaction, Vec<u8>, Vec<u8>) {
    let key = encoded_key(table_id, row_id);
    let row = encoded_row(row_id, name);

    let mut transaction = Transaction::new(TxnId(txn_id), Timestamp(start_ts))
        .expect("test transaction metadata must be valid");

    transaction
        .buffer_put(key.clone(), row.clone())
        .expect("test mutation must be valid");

    (transaction, key, row)
}

/// Realistic bug caught:
///
/// The coordinator could acknowledge the transaction or apply it to MVCC
/// before its complete commit record reaches A-WAL's durable frontier.
#[test]
fn successful_commit_is_durable_before_mvcc_visibility_is_published() {
    let test_dir = TestDir::new("success");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    let (adapter, observer) = open_adapter(&test_dir, directory);

    let table_id = TableId(9);
    let storage = InMemoryMvcc::new();

    let mut coordinator = SingleNodeCommitCoordinator::new(table_id, storage, adapter)
        .expect("coordinator configuration must be valid");

    let mut manager = LocalTransactionManager::new();

    let (transaction, key, row) = put_transaction(table_id, 7, 11, 1, "alice");

    let outcome = coordinator
        .commit(transaction, &mut manager)
        .expect("commit must succeed");

    assert_eq!(outcome.transaction_id, TxnId(7));
    assert_eq!(outcome.commit_timestamp, Some(Timestamp(12)));
    assert_eq!(outcome.committed_writes, 1);

    let extent = outcome
        .wal_extent
        .expect("write commit must return its durable extent");

    assert!(observer.durable_lsn() >= extent.end_lsn);

    let visible = coordinator
        .storage()
        .read(&key, Timestamp(12))
        .expect("MVCC read must succeed");

    assert_eq!(visible, Some(row));

    let durable_record = observer
        .read_at(extent.start_lsn)
        .expect("durable commit record must be readable");

    assert_eq!(
        durable_record.record_type,
        RagnorDbWalRecordType::SingleNodeTxnCommit.as_wal_record_type()
    );

    let decoded = SingleNodeTxnCommit::decode(&durable_record.payload)
        .expect("durable commit payload must decode");

    assert_eq!(decoded.txn_id, TxnId(7));
    assert_eq!(decoded.start_timestamp, Timestamp(11));
    assert_eq!(decoded.commit_timestamp, Timestamp(12));
    assert_eq!(decoded.writes.len(), 1);
}

/// Realistic bug caught:
///
/// A conflicting transaction could receive a timestamp and durable WAL record
/// before complete MVCC conflict validation rejects it.
#[test]
fn preflight_failure_does_not_allocate_timestamp_or_append_wal() {
    let test_dir = TestDir::new("preflight-failure");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    let (adapter, observer) = open_adapter(&test_dir, directory);

    let table_id = TableId(9);
    let key = encoded_key(table_id, 1);

    let mut storage = InMemoryMvcc::new();
    let winner = BTreeMap::from([(key.clone(), Mutation::Put(encoded_row(1, "winner")))]);

    storage
        .commit_batch(TxnId(50), Timestamp(2), Timestamp(3), &winner)
        .expect("winner must commit");

    let mut coordinator = SingleNodeCommitCoordinator::new(table_id, storage, adapter)
        .expect("coordinator configuration must be valid");

    let mut manager = LocalTransactionManager::new();

    let (loser, _, _) = put_transaction(table_id, 7, 1, 1, "loser");

    let error = coordinator.commit(loser, &mut manager).unwrap_err();

    assert!(matches!(error, Error::WriteConflict(_)));
    assert_eq!(manager.last_allocated_timestamp(), Timestamp(0));
    assert_eq!(observer.durable_lsn(), Lsn::ZERO);
    assert!(!coordinator.requires_recovery());
}

/// Realistic bug caught:
///
/// A sync failure after staging could leave MVCC data visible or allow another
/// write through the same coordinator while the first outcome remains unknown.
#[test]
fn synchronization_failure_keeps_mvcc_unmodified_and_stops_writes() {
    let test_dir = TestDir::new("sync-failure");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    directory
        .inject_sync_error(1)
        .expect("failed to inject synchronization failure");

    let (adapter, observer) = open_adapter(&test_dir, directory);

    let table_id = TableId(9);

    let mut coordinator = SingleNodeCommitCoordinator::new(table_id, InMemoryMvcc::new(), adapter)
        .expect("coordinator configuration must be valid");

    let mut manager = LocalTransactionManager::new();

    let (transaction, key, _) = put_transaction(table_id, 7, 11, 1, "alice");

    let error = coordinator.commit(transaction, &mut manager).unwrap_err();

    assert!(matches!(error, Error::CommitOutcomeUnknown { .. }));

    assert!(coordinator.requires_recovery());

    assert_eq!(
        coordinator
            .storage()
            .read(&key, Timestamp(u64::MAX))
            .expect("MVCC read must succeed"),
        None
    );

    assert_eq!(observer.durable_lsn(), Lsn::ZERO);
    assert_eq!(manager.last_allocated_timestamp(), Timestamp(12));

    let (retry, _, _) = put_transaction(table_id, 8, 13, 2, "second");

    let retry_error = coordinator.commit(retry, &mut manager).unwrap_err();

    assert!(matches!(retry_error, Error::RecoveryRequired { .. }));

    assert_eq!(manager.last_allocated_timestamp(), Timestamp(12));
}

/// Realistic bug caught:
///
/// Read-only transactions could unnecessarily allocate a commit timestamp or
/// append an empty transaction record, changing durable history for a no-op.
#[test]
fn read_only_commit_is_wal_free_and_does_not_allocate_timestamp() {
    let test_dir = TestDir::new("read-only");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    let (adapter, observer) = open_adapter(&test_dir, directory);

    let mut coordinator =
        SingleNodeCommitCoordinator::new(TableId(9), InMemoryMvcc::new(), adapter)
            .expect("coordinator configuration must be valid");

    let mut manager = LocalTransactionManager::new();

    let transaction =
        Transaction::new(TxnId(7), Timestamp(11)).expect("read-only transaction must be valid");

    let outcome = coordinator
        .commit(transaction, &mut manager)
        .expect("read-only commit must succeed");

    assert_eq!(outcome.transaction_id, TxnId(7));
    assert_eq!(outcome.commit_timestamp, None);
    assert_eq!(outcome.committed_writes, 0);
    assert_eq!(outcome.wal_extent, None);

    assert_eq!(manager.last_allocated_timestamp(), Timestamp(0));
    assert_eq!(observer.durable_lsn(), Lsn::ZERO);
}

struct FailApplyMvcc {
    inner: InMemoryMvcc,
}

impl FailApplyMvcc {
    fn new() -> Self {
        Self {
            inner: InMemoryMvcc::new(),
        }
    }
}

impl MvccStorage for FailApplyMvcc {
    fn read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        self.inner.read(key, read_ts)
    }

    fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        read_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan(start, end, read_ts)
    }

    fn validate_commit_batch(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<()> {
        self.inner
            .validate_commit_batch(txn_id, start_ts, mutations)
    }

    fn commit_batch(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _commit_ts: Timestamp,
        _mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<usize> {
        Err(Error::CorruptData(
            "injected MVCC application failure".to_string(),
        ))
    }

    fn stats(&self) -> MvccStats {
        self.inner.stats()
    }
}

/// Realistic bug caught:
///
/// An MVCC application failure after WAL durability could be reported as an
/// ordinary transaction abort, allowing later writes despite an authoritative
/// durable commit record requiring recovery.
#[test]
fn post_durability_apply_failure_requires_recovery() {
    let test_dir = TestDir::new("apply-failure");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    let (adapter, observer) = open_adapter(&test_dir, directory);

    let table_id = TableId(9);

    let mut coordinator = SingleNodeCommitCoordinator::new(table_id, FailApplyMvcc::new(), adapter)
        .expect("coordinator configuration must be valid");

    let mut manager = LocalTransactionManager::new();

    let (transaction, key, _) = put_transaction(table_id, 7, 11, 1, "alice");

    let error = coordinator.commit(transaction, &mut manager).unwrap_err();

    assert!(matches!(error, Error::RecoveryRequired { .. }));

    assert!(coordinator.requires_recovery());
    assert!(observer.durable_lsn() > Lsn::ZERO);

    let durable_record = observer
        .read_at(Lsn::ZERO)
        .expect("authoritative durable commit must remain readable");

    assert_eq!(
        durable_record.record_type,
        RagnorDbWalRecordType::SingleNodeTxnCommit.as_wal_record_type()
    );

    assert_eq!(
        coordinator
            .storage()
            .read(&key, Timestamp(u64::MAX))
            .expect("MVCC read must succeed"),
        None
    );
}
