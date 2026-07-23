use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use ragnordb_common::{
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    command_codec::{CatalogCommand, CatalogOperation, CreateTableOperation},
    ids::{ColumnId, TableId, Timestamp},
};
use ragnordb_storage::{
    recovery::{RecoveryPayload, scan_recovery_records},
    wal::{CatalogUpdate, RagnorDbWalRecordType},
};
use wal::{
    config::WalConfig, io::fault::FaultDirectory, lsn::Lsn, types::WalIdentity, wal::WalHandle,
};

/// isolated filesystem directory owned by one recovery stream test
///
/// the directory is removed on drop so every test begins with an independent
/// WAL identity and cannot observe records from an earlier run
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
            "ragnordb-recovery-stream-{prefix}-{}-{nanos}",
            process::id()
        ));

        fs::create_dir_all(&path).expect("failed to create recovery-stream test directory");

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

fn open_handle(test_dir: &TestDir) -> TestWalHandle {
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    let config = WalConfig {
        dir: test_dir.path().to_path_buf(),
        identity: WalIdentity::new(1, 1, 1),
        ..WalConfig::default()
    };

    WalHandle::open(directory, config, ())
        .expect("failed to open recovery-stream test WAL")
        .0
}

fn catalog_update(table_id: u64, table_name: &str, update_timestamp: u64) -> CatalogUpdate {
    CatalogUpdate {
        table_id: TableId(table_id),
        update_timestamp: Timestamp(update_timestamp),
        command: CatalogCommand {
            operation: CatalogOperation::CreateTable(CreateTableOperation {
                table_def: TableDefinition {
                    table_id,
                    name: table_name.to_string(),
                    columns: vec![ColumnDefinition {
                        column_id: ColumnId(1),
                        name: "id".to_string(),
                        ty: DataType::Int,
                        nullable: false,
                    }],
                    primary_key_column_ids: vec![ColumnId(1)],
                    schema_version: 1,
                    tablet_count: 1,
                },
            }),
        },
    }
}

/// verifies that recovery follows the physical stream and honors the selected
/// replay boundary exactly
///
/// Realistic bug caught:
///
/// A recovery implementation could always begin at LSN zero, sort records by
/// semantic type, or include records already represented by a selected
/// snapshot. Any of those behaviors could apply metadata twice or reconstruct
/// a state that never existed in durable WAL order
#[test]
fn recovery_stream_preserves_wal_order_and_honors_replay_boundary() {
    let test_dir = TestDir::new("ordered-boundary");
    let handle = open_handle(&test_dir);

    let first_update = catalog_update(9, "users", 11);
    let second_update = catalog_update(10, "orders", 12);

    let first_payload = first_update
        .encode()
        .expect("first catalog update must encode");

    let second_payload = second_update
        .encode()
        .expect("second catalog update must encode");

    let first_extent = handle
        .append_and_sync(
            RagnorDbWalRecordType::CatalogUpdate.as_wal_record_type(),
            &first_payload,
        )
        .expect("first catalog update must become durable");

    let second_extent = handle
        .append_and_sync(
            RagnorDbWalRecordType::CatalogUpdate.as_wal_record_type(),
            &second_payload,
        )
        .expect("second catalog update must become durable");

    let mut complete_stream =
        scan_recovery_records(&handle, Lsn::ZERO).expect("complete recovery stream must open");

    let first_record = complete_stream
        .next_record()
        .expect("first recovery read must succeed")
        .expect("first catalog record must exist");

    let second_record = complete_stream
        .next_record()
        .expect("second recovery read must succeed")
        .expect("second catalog record must exist");

    assert_eq!(first_record.lsn, first_extent.start_lsn);
    assert_eq!(
        first_record.payload,
        RecoveryPayload::CatalogUpdate(first_update)
    );

    assert_eq!(second_record.lsn, second_extent.start_lsn);
    assert_eq!(
        second_record.payload,
        RecoveryPayload::CatalogUpdate(second_update.clone())
    );

    assert!(
        complete_stream
            .next_record()
            .expect("end-of-stream check must succeed")
            .is_none(),
        "recovery must stop after the complete physical WAL snapshot"
    );

    // A snapshot replay boundary points at the first WAL record not represented
    // by that snapshot. Starting at the second extent must not return the first
    // catalog update again
    let mut suffix_stream = scan_recovery_records(&handle, second_extent.start_lsn)
        .expect("suffix recovery stream must open");

    let suffix_record = suffix_stream
        .next_record()
        .expect("suffix recovery read must succeed")
        .expect("record at replay boundary must exist");

    assert_eq!(suffix_record.lsn, second_extent.start_lsn);
    assert_eq!(
        suffix_record.payload,
        RecoveryPayload::CatalogUpdate(second_update)
    );

    assert!(
        suffix_stream
            .next_record()
            .expect("suffix end-of-stream check must succeed")
            .is_none()
    );
}
