//! server owned local database runtime
//!
//! a running local node owns exactly onw executor and one transaction
//! manager. every client connection shares this runtime, while each connection
//! retains its own `SqlSession` and therefore its own explicit transaction state
//!
//! the runtime us protected by one asynchronous mutex. this serializes
//! physical statement execution while still allowing explicit transactions from
//! different connections to interleave between statements
//! recovery constructs catalog, MVCC, allocator, and WAL-writer state privately
//! The complete `LocalDatabase` is returned only after every startup boundary
//! succeeds

use std::{fs, path::Path, sync::Arc};

use ragnordb_common::{Error, Result, ids::NodeId, proto::snapshot as snapshot_proto};
use ragnordb_exec::{ExecutionResult, LocalExecutor, SqlSession};
use ragnordb_storage::{
    recovery::{
        RecoveryState, load_recovery_checkpoint, replay_recovery_stream_from_state,
        scan_recovery_records, select_recovery_checkpoint,
    },
    wal::RagnorDbWalAdapter,
};
use ragnordb_txn::{LocalTransactionManager, TransactionManager};
use tokio::sync::Mutex;
use wal::{
    config::WalConfig,
    io::directory::FsSegmentDirectory,
    lsn::Lsn,
    types::WalIdentity,
    wal::{WalHandle, report::RecoveryReport},
};

/// Shared server wide local database runtime
pub type SharedLocalDatabase = Arc<Mutex<LocalDatabase>>;

/// in memory database state owned by one running server node
///
/// the executor and transaction manager must remain paired. Constructing either
/// one per connection would isolate catalog/tablet state or reuse transaction
/// identities and MVCC timestamps
#[derive(Debug)]
pub struct LocalDatabase {
    executor: LocalExecutor,
    transaction_manager: LocalTransactionManager,
    next_snapshot_id: Option<u64>,
}

impl Default for LocalDatabase {
    fn default() -> Self {
        Self {
            executor: LocalExecutor::default(),
            transaction_manager: LocalTransactionManager::default(),
            next_snapshot_id: Some(1),
        }
    }
}

impl LocalDatabase {
    /// an empty local database runtime
    pub fn new() -> Self {
        Self::default()
    }

    /// recover a complete local runtime from the node's A-WAL directory
    ///
    /// A-WAL first establishes and repairs its physically valid prefix. RagnorDB
    /// then selects and loads the newest published checkpoint when one exists,
    /// replays its exact WAL suffix into private state, and publishes the
    /// reconstructed runtime only after every recovery boundary succeeds
    pub fn recover(data_dir: impl AsRef<Path>, node_id: NodeId) -> Result<(Self, RecoveryReport)> {
        if node_id.0 == 0 {
            return Err(Error::Configuration(
                "node ID 0 cannot identify a recovery WAL".to_string(),
            ));
        }

        let data_dir = data_dir.as_ref();
        let wal_dir = data_dir.join("wal");

        fs::create_dir_all(&wal_dir).map_err(|source| Error::RecoveryFailed {
            reason: format!(
                "failed to create WAL directory {}: {}",
                wal_dir.display(),
                source
            ),
        })?;

        let wal_config = WalConfig {
            dir: wal_dir.clone(),
            identity: WalIdentity::new(node_id.0, 1, 1),
            ..WalConfig::default()
        };

        let directory = FsSegmentDirectory::new(wal_dir);

        let (wal, recovery_report) =
            WalHandle::open(directory, wal_config, ()).map_err(|source| Error::RecoveryFailed {
                reason: format!("failed to open and physically recover A-WAL: {source}"),
            })?;

        // recovery must begin at the first retained record rather than
        // unconditionally at zero. A published checkpoint may have made older
        // segments eligible for safe removal before this process restarted
        let first_retained_lsn = recovery_report.first_lsn.unwrap_or(Lsn::ZERO);

        let recovery_pin = wal
            .acquire_retention_pin("ragnordb-startup-recovery", first_retained_lsn)
            .map_err(|source| Error::RecoveryFailed {
                reason: format!(
                    "failed to pin WAL retention at startup LSN {}: {source}",
                    first_retained_lsn.as_u64()
                ),
            })?;

        let checkpoint_stream = scan_recovery_records(&wal, first_retained_lsn)?;
        let checkpoint_candidate = select_recovery_checkpoint(checkpoint_stream)?;

        let (recovered_state, replay_from_lsn) = match checkpoint_candidate {
            Some(candidate) => {
                // loading validates the referenced file's envelope, identity,
                // encoded length, checksum, metadata, and replay frontier before
                // any contained state is accepted
                let loaded = load_recovery_checkpoint(data_dir, &candidate)?;
                (loaded.state, loaded.replay_from_lsn)
            }

            None if first_retained_lsn != Lsn::ZERO => {
                // full replay is impossible once the retained WAL no longer begins
                // at zero. Starting without a checkpoint would publish incomplete
                // catalog or MVCC state
                return Err(Error::RecoveryFailed {
                    reason: format!(
                        "retained WAL begins at LSN {}, but no published \
                     checkpoint can reconstruct the pruned prefix",
                        first_retained_lsn.as_u64()
                    ),
                });
            }

            None => (RecoveryState::new(), Lsn::ZERO),
        };

        let replay_stream = scan_recovery_records(&wal, replay_from_lsn)?;
        let recovered_state = replay_recovery_stream_from_state(replay_stream, recovered_state)?;

        let high_water_marks = recovered_state.high_water_marks();
        let floors = high_water_marks.checked_allocator_floors()?;

        let (mut catalog, mvcc_by_table, recovered_marks) = recovered_state.into_parts();

        catalog.restore_table_id_floor(floors.next_table_id)?;

        let transaction_manager = LocalTransactionManager::from_recovered_floors(
            floors.next_transaction_id,
            floors.next_timestamp,
        )?;

        let replay_from_end_lsn = wal.durable_lsn().as_u64();

        // Every recovery reader has reached the immutable durable frontier.
        // Release its pin before transferring the WAL handle into live runtime
        // components, which establish their own retention lifetimes.
        drop(recovery_pin);

        let adapter = Arc::new(RagnorDbWalAdapter::new(wal));

        let executor = LocalExecutor::from_recovered(
            catalog,
            mvcc_by_table,
            adapter.clone(),
            adapter,
            recovered_marks.max_timestamp,
            replay_from_end_lsn,
        )?;

        // No caller can observe the executor, manager, allocator floors, or WAL
        // handle until every recovery and construction step has succeeded.
        Ok((
            Self {
                executor,
                transaction_manager,
                next_snapshot_id: Some(floors.next_snapshot_id),
            },
            recovery_report,
        ))
    }

    /// Construct an empty shared runtime for tests that do not exercise restart.
    pub fn shared() -> SharedLocalDatabase {
        Arc::new(Mutex::new(Self::new()))
    }

    /// Publish an already recovered runtime to connection tasks.
    pub fn into_shared(self) -> SharedLocalDatabase {
        Arc::new(Mutex::new(self))
    }

    /// Execute one SQL statement through the connection's SQL session.
    pub fn execute_sql(&mut self, session: &mut SqlSession, sql: &str) -> Result<ExecutionResult> {
        let Self {
            executor,
            transaction_manager,
            ..
        } = self;

        session.execute_sql(sql, executor, transaction_manager)
    }

    /// capture one immutable database image under the runtime's write barrier
    ///
    /// The server stores `LocalDatabase` behind one asynchronous mutex and all
    /// commit/catalog paths require `&mut self`. Therefore this method fixes the
    /// catalog, MVCC maps, allocator maxima, and replay frontier as one cut.
    /// It does not write or publish a snapshot file.
    pub fn capture_checkpoint_image(&mut self) -> Result<snapshot_proto::DatabaseSnapshot> {
        let snapshot_id = self.next_snapshot_id.ok_or_else(|| {
            Error::Configuration("snapshot ID allocator is exhausted".to_string())
        })?;

        let previous_timestamp = self.transaction_manager.last_allocated_timestamp();

        let snapshot_timestamp = self
            .transaction_manager
            .allocate_commit_timestamp(previous_timestamp)?;

        let tables = self.executor.capture_snapshot_tables()?;
        let replay_from_lsn = self.executor.replay_from_end_lsn();
        let max_table_id = self.executor.catalog().table_id_high_water_mark();

        let max_transaction_id = self.transaction_manager.last_allocated_transaction_id();

        self.next_snapshot_id = snapshot_id.checked_add(1);

        Ok(snapshot_proto::DatabaseSnapshot {
            snapshot_id,
            snapshot_timestamp: Some(snapshot_timestamp.to_proto()),
            replay_from_lsn,
            high_water_marks: Some(snapshot_proto::AllocatorHighWaterMarks {
                max_transaction_id: Some(max_transaction_id.to_proto()),
                max_timestamp: Some(snapshot_timestamp.to_proto()),
                max_table_id: Some(max_table_id.to_proto()),
                max_snapshot_id: snapshot_id,
            }),
            tables,
        })
    }
}
