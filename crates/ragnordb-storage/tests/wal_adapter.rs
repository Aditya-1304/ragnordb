use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use ragnordb_common::{
    Error,
    codec::{Row, Value},
    encoding::encode_row,
    ids::{TableId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    wal::{RagnorDbWalAdapter, RagnorDbWalRecordType, SingleNodeTxnCommit, WalMutation},
};
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
            .expect("system time must be later than the Unix epoch")
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "ragnordb-wal-adapter-{prefix}-{}-{nanos}",
            process::id()
        ));

        fs::create_dir_all(&path).expect("failed to create WAL adapter test directory");

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

type TestWalHandle = WalHandle<FaultDirectory, ()>;

fn wal_config(test_dir: &TestDir) -> WalConfig {
    WalConfig {
        dir: test_dir.path().to_path_buf(),
        identity: WalIdentity::new(1, 1, 1),
        ..WalConfig::default()
    }
}

fn open_handle(directory: FaultDirectory, config: WalConfig) -> TestWalHandle {
    WalHandle::open(directory, config, ())
        .expect("failed to open adapter test WAL")
        .0
}

fn encoded_key(table_id: TableId, id: i64) -> Vec<u8> {
    let row_key = make_row_key(table_id, &[Value::Int(id)]).expect("test row key must be valid");

    encode_row_key(&row_key).expect("test row key must encode")
}

fn encoded_row(id: i64, name: &str) -> Vec<u8> {
    encode_row(&Row {
        values: vec![Value::Int(id), Value::Text(name.to_string())],
    })
    .expect("test row must encode")
}

fn valid_commit() -> SingleNodeTxnCommit {
    let table_id = TableId(9);

    let writes = BTreeMap::from([
        (
            encoded_key(table_id, 1),
            WalMutation::Put(encoded_row(1, "alice")),
        ),
        (encoded_key(table_id, 2), WalMutation::Delete),
    ]);

    SingleNodeTxnCommit {
        table_id,
        txn_id: TxnId(7),
        start_timestamp: Timestamp(11),
        commit_timestamp: Timestamp(12),
        schema_version: 1,
        writes,
    }
}

/// Verifies the complete RagnorDB-to-A-WAL success boundary.
///
/// Realistic bug caught:
///
/// A valid transaction could be encoded correctly but appended under the wrong
/// durable record identifier, or the adapter could return success before the
/// complete encoded record extent reaches A-WAL's durable frontier.
#[test]
fn adapter_appends_commit_with_owned_record_type_and_returns_durable_extent() {
    let test_dir = TestDir::new("durable-commit");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    let handle = open_handle(directory, wal_config(&test_dir));
    let observer = handle.clone();
    let adapter = RagnorDbWalAdapter::new(handle);
    let commit = valid_commit();

    let extent = adapter
        .append_single_node_commit(&commit)
        .expect("valid commit must be appended durably");

    assert!(extent.start_lsn < extent.end_lsn);
    assert!(observer.durable_lsn() >= extent.end_lsn);

    let stored_record = observer
        .read_at(extent.start_lsn)
        .expect("durable commit record must be readable");

    assert_eq!(
        stored_record.record_type,
        RagnorDbWalRecordType::SingleNodeTxnCommit.as_wal_record_type()
    );

    let recovered_commit = SingleNodeTxnCommit::decode(&stored_record.payload)
        .expect("adapter must store the versioned RagnorDB protobuf payload");

    assert_eq!(recovered_commit, commit);
}

/// Verifies that deterministic A-WAL admission rejection remains distinguishable
/// from an append whose durability is unknown.
///
/// Realistic bug caught:
///
/// The adapter could collapse a pre-staging rejection into
/// `COMMIT_OUTCOME_UNKNOWN`, incorrectly telling the caller that recovery is
/// needed even though no logical extent was assigned.
#[test]
fn adapter_preserves_definitely_not_staged_failure() {
    let test_dir = TestDir::new("not-staged");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());
    let mut config = wal_config(&test_dir);

    // The valid transaction payload is intentionally larger than this limit.
    // A-WAL must reject it during deterministic admission before assigning an
    // extent or performing mutating record I/O.
    config.max_record_size = 32;

    let handle = open_handle(directory, config);
    let observer = handle.clone();
    let adapter = RagnorDbWalAdapter::new(handle);

    let error = adapter
        .append_single_node_commit(&valid_commit())
        .unwrap_err();

    assert!(matches!(error, Error::WalAppendNotStaged { .. }));

    assert_eq!(observer.durable_lsn(), Lsn::ZERO);
}

/// Verifies preservation of the indeterminate durable-commit outcome.
///
/// Realistic bug caught:
///
/// A synchronization failure after record staging could be converted into a
/// normal storage error, allowing transaction infrastructure to retry the
/// logical commit even though recovery may retain the first record.
#[test]
fn adapter_maps_post_staging_sync_failure_to_commit_outcome_unknown() {
    let test_dir = TestDir::new("outcome-unknown");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    directory
        .inject_sync_error(1)
        .expect("failed to install synchronization fault");

    let handle = open_handle(directory, wal_config(&test_dir));
    let observer = handle.clone();
    let adapter = RagnorDbWalAdapter::new(handle);
    let commit = valid_commit();

    let error = adapter.append_single_node_commit(&commit).unwrap_err();

    let (start_lsn, end_lsn) = match error {
        Error::CommitOutcomeUnknown {
            start_lsn, end_lsn, ..
        } => (start_lsn, end_lsn),

        other => panic!("expected COMMIT_OUTCOME_UNKNOWN after staging, got {other:?}"),
    };

    assert!(start_lsn < end_lsn);
    assert!(observer.durable_lsn().as_u64() < end_lsn);

    // A-WAL must remain fail-stopped after uncertain mutating I/O. This second
    // record therefore receives the definitely-not-staged classification while
    // the original commit remains outcome-unknown.
    let retry = adapter.append_single_node_commit(&commit).unwrap_err();

    assert!(matches!(retry, Error::WalAppendNotStaged { .. }));
}
