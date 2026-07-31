use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use ragnordb_catalog::{
    Catalog, CatalogLogExtent, CatalogLogRecord, ColumnSchema, DurableCatalog, DurableCatalogLog,
};
use ragnordb_common::{
    Error, Result,
    catalog_codec::DataType,
    ids::{ColumnId, Timestamp},
};

#[derive(Debug, Clone, Copy)]
enum NextAppend {
    Succeed,
    OutcomeUnknown,
}

struct LogState {
    next_lsn: u64,
    next_append: NextAppend,
    records: Vec<CatalogLogRecord>,
}

struct TestCatalogLog {
    state: Mutex<LogState>,
}

impl TestCatalogLog {
    fn new() -> Self {
        Self {
            state: Mutex::new(LogState {
                next_lsn: 0,
                next_append: NextAppend::Succeed,
                records: Vec::new(),
            }),
        }
    }

    fn fail_next_with_outcome_unknown(&self) {
        self.state
            .lock()
            .expect("test catalog-log mutex must not be poisoned")
            .next_append = NextAppend::OutcomeUnknown;
    }

    fn records(&self) -> Vec<CatalogLogRecord> {
        self.state
            .lock()
            .expect("test catalog-log mutex must not be poisoned")
            .records
            .clone()
    }
}

impl DurableCatalogLog for TestCatalogLog {
    fn append_catalog_update(&self, update: &CatalogLogRecord) -> Result<CatalogLogExtent> {
        let mut state = self
            .state
            .lock()
            .expect("test catalog-log mutex must not be poisoned");

        let start_lsn = state.next_lsn;
        let end_lsn = start_lsn
            .checked_add(1)
            .expect("test catalog LSN space must not overflow");

        state.next_lsn = end_lsn;
        state.records.push(update.clone());

        match std::mem::replace(&mut state.next_append, NextAppend::Succeed) {
            NextAppend::Succeed => Ok(CatalogLogExtent { start_lsn, end_lsn }),

            NextAppend::OutcomeUnknown => Err(Error::CatalogOutcomeUnknown {
                start_lsn,
                end_lsn,
                reason: "injected catalog synchronization failure".to_string(),
            }),
        }
    }
}

fn columns() -> Vec<ColumnSchema> {
    vec![
        ColumnSchema {
            id: ColumnId(1),
            name: "id".to_string(),
            ty: DataType::Int,
            nullable: false,
        },
        ColumnSchema {
            id: ColumnId(2),
            name: "name".to_string(),
            ty: DataType::Text,
            nullable: false,
        },
    ]
}

/// Realistic bug caught:
///
/// CREATE TABLE could publish schema metadata before its durable CatalogUpdate
/// exists, allowing later rows and WAL records to reference a table that
/// recovery cannot reconstruct.
#[test]
fn table_is_published_only_after_catalog_update_is_durable() {
    let log = Arc::new(TestCatalogLog::new());
    let mut catalog = DurableCatalog::new(log.clone());

    assert!(catalog.catalog().table_by_name("users").is_none());

    let outcome = catalog
        .create_table("users", columns(), vec![ColumnId(1)], || Ok(Timestamp(11)))
        .expect("durable table creation must succeed");

    assert_eq!(outcome.update_timestamp, Timestamp(11));
    assert_eq!(outcome.schema.name, "users");
    assert_eq!(outcome.schema.id.0, 1);
    assert_eq!(outcome.wal_extent.start_lsn, 0);
    assert_eq!(outcome.wal_extent.end_lsn, 1);

    let records = log.records();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].table_id, outcome.schema.id);
    assert_eq!(records[0].update_timestamp, Timestamp(11));

    let published = catalog
        .catalog()
        .table_by_name("users")
        .expect("schema must be published after durability");

    assert_eq!(published.as_ref(), outcome.schema.as_ref());
}

/// Realistic bug caught:
///
/// A post-staging synchronization failure could still publish the table,
/// incorrectly treating an unknown catalog outcome as an ordinary success.
#[test]
fn outcome_unknown_does_not_publish_table_and_stops_catalog_writes() {
    let log = Arc::new(TestCatalogLog::new());
    log.fail_next_with_outcome_unknown();

    let mut catalog = DurableCatalog::new(log.clone());

    let error = catalog
        .create_table("users", columns(), vec![ColumnId(1)], || Ok(Timestamp(11)))
        .unwrap_err();

    assert!(matches!(error, Error::CatalogOutcomeUnknown { .. }));

    assert!(catalog.requires_recovery());
    assert!(catalog.catalog().table_by_name("users").is_none());
    assert_eq!(log.records().len(), 1);

    let retry = catalog
        .create_table("orders", columns(), vec![ColumnId(1)], || Ok(Timestamp(12)))
        .unwrap_err();

    assert!(matches!(retry, Error::RecoveryRequired { .. }));

    assert!(catalog.catalog().table_by_name("orders").is_none());
    assert_eq!(log.records().len(), 1);
}

/// Realistic bug caught:
///
/// Catalog validation could happen after timestamp allocation or WAL append,
/// durably recording a duplicate table operation that cannot be published.
#[test]
fn invalid_catalog_operation_is_rejected_before_timestamp_and_wal() {
    let log = Arc::new(TestCatalogLog::new());
    let mut catalog = DurableCatalog::new(log.clone());

    let _ = catalog
        .create_table("users", columns(), vec![ColumnId(1)], || Ok(Timestamp(11)))
        .expect("first table creation must succeed");

    let allocator_called = AtomicBool::new(false);

    let error = catalog
        .create_table("users", columns(), vec![ColumnId(1)], || {
            allocator_called.store(true, Ordering::SeqCst);
            Ok(Timestamp(12))
        })
        .unwrap_err();

    assert!(matches!(error, Error::ConstraintViolation(_)));

    assert!(!allocator_called.load(Ordering::SeqCst));
    assert_eq!(log.records().len(), 1);
    assert_eq!(catalog.catalog().list_tables().len(), 1);
}

/// Realistic bug caught:
///
/// An invalid update timestamp could cross into durable catalog history instead
/// of being rejected before the append.
#[test]
fn zero_catalog_timestamp_is_rejected_before_wal_append() {
    let log = Arc::new(TestCatalogLog::new());
    let mut catalog = DurableCatalog::new(log.clone());

    let error = catalog
        .create_table("users", columns(), vec![ColumnId(1)], || Ok(Timestamp(0)))
        .unwrap_err();

    assert!(matches!(error, Error::Configuration(_)));
    assert!(log.records().is_empty());
    assert!(catalog.catalog().list_tables().is_empty());
}
