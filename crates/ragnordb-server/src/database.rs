//! server owned local database runtime
//!
//! a running node owns exactly one executor, transaction manager, WAL adapter,
//! and checkpoint publication coordinator. Every connection shares this runtime,
//! while each connection retains its own `SqlSession` and explicit transaction
//!
//! startup recovery reconstructs catalog, MVCC, allocator, and WAL-writer state
//! privately. No client-facing runtime is returned until every recovery boundary
//! has succeeded

use std::{
    fmt, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use ragnordb_common::{Error, Result, ids::NodeId, proto::snapshot as snapshot_proto};
use ragnordb_exec::{ExecutionResult, LocalExecutor, SqlSession};
use ragnordb_storage::{
    checkpoint::{
        PublishedCheckpoint, publish_checkpoint as publish_checkpoint_metadata,
        publish_snapshot_file,
    },
    recovery::{
        RecoveryState, load_recovery_checkpoint, replay_recovery_stream_from_state,
        scan_recovery_records, select_recovery_checkpoint,
    },
    wal::{CheckpointRetentionPin, RagnorDbWalAdapter},
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

/// concrete local WAL adapter shared by transaction, catalog, checkpoint, and
/// recovery-owned runtime paths
type LocalWalAdapter = RagnorDbWalAdapter<FsSegmentDirectory, ()>;

/// shared server-wide local database runtime
pub type SharedLocalDatabase = Arc<Mutex<LocalDatabase>>;

/// result returned only after the complete live checkpoint workflow succeeds
#[must_use = "checkpoint publication contains the durable and retention boundaries"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveCheckpointPublication {
    /// snapshot pointer and matching marker proven durable by WAL
    pub checkpoint: PublishedCheckpoint,

    /// retention floor installed after the checkpoint marker became durable
    pub retention_advanced_to: Lsn,

    /// sealed WAL segments physically removed during this advancement
    ///
    /// active readers may keep this value at zero even though the retention floor
    /// was advanced successfully
    pub pruned_segments: usize,
}

/// in memory state owned by one running local database node
///
/// the executor and transaction manager must remain paired. Constructing either
/// per connection would isolate catalog/tablet state or reuse transaction IDs and
/// MVCC timestamps
///
/// checkpoint publication has its own mutex because the database state lock is
/// deliberately released while the immutable snapshot file is written. This
/// permits later commits to proceed without allowing two checkpoints to publish
/// out of order
pub struct LocalDatabase {
    executor: LocalExecutor,
    transaction_manager: LocalTransactionManager,
    next_snapshot_id: Option<u64>,
    data_dir: Option<PathBuf>,
    checkpoint_adapter: Option<Arc<LocalWalAdapter>>,
    checkpoint_publication_lock: Arc<Mutex<()>>,
}

impl fmt::Debug for LocalDatabase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDatabase")
            .field("executor", &self.executor)
            .field("transaction_manager", &self.transaction_manager)
            .field("next_snapshot_id", &self.next_snapshot_id)
            .field("data_dir", &self.data_dir)
            .field("has_checkpoint_adapter", &self.checkpoint_adapter.is_some())
            .finish_non_exhaustive()
    }
}

impl Default for LocalDatabase {
    fn default() -> Self {
        Self {
            executor: LocalExecutor::default(),
            transaction_manager: LocalTransactionManager::default(),
            next_snapshot_id: Some(1),
            data_dir: None,
            checkpoint_adapter: None,
            checkpoint_publication_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl LocalDatabase {
    /// construct an in memory runtime for tests that do not exercise durability
    pub fn new() -> Self {
        Self::default()
    }

    /// recover a complete local runtime from the node's A-WAL directory
    ///
    /// A-WAL first establishes and repairs its physically valid prefix. RagnorDB
    /// then selects and loads the newest published checkpoint, when one exists,
    /// replays its exact WAL suffix into private state, and publishes the runtime
    /// only after every recovery boundary succeeds
    pub fn recover(data_dir: impl AsRef<Path>, node_id: NodeId) -> Result<(Self, RecoveryReport)> {
        if node_id.0 == 0 {
            return Err(Error::Configuration(
                "node ID 0 cannot identify a recovery WAL".to_string(),
            ));
        }

        let data_dir = data_dir.as_ref().to_path_buf();
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

        // recovery begins at the first retained record rather than assuming that
        // the WAL prefix still begins at zero
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
                // loading validates the snapshot envelope, checksum, identity,
                // metadata, allocator maxima, and replay boundary before state is
                // accepted
                let loaded = load_recovery_checkpoint(&data_dir, &candidate)?;
                (loaded.state, loaded.replay_from_lsn)
            }

            None if first_retained_lsn != Lsn::ZERO => {
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

        // every startup reader has reached the immutable durable frontier. The
        // live runtime establishes separate retention protection for checkpoint
        // publication
        drop(recovery_pin);

        let adapter = Arc::new(RagnorDbWalAdapter::new(wal));

        let executor = LocalExecutor::from_recovered(
            catalog,
            mvcc_by_table,
            adapter.clone(),
            adapter.clone(),
            recovered_marks.max_timestamp,
            replay_from_end_lsn,
        )?;

        Ok((
            Self {
                executor,
                transaction_manager,
                next_snapshot_id: Some(floors.next_snapshot_id),
                data_dir: Some(data_dir),
                checkpoint_adapter: Some(adapter),
                checkpoint_publication_lock: Arc::new(Mutex::new(())),
            },
            recovery_report,
        ))
    }

    /// construct an empty shared runtime for tests without durable storage
    pub fn shared() -> SharedLocalDatabase {
        Arc::new(Mutex::new(Self::new()))
    }

    /// publish an already recovered runtime to connection tasks
    pub fn into_shared(self) -> SharedLocalDatabase {
        Arc::new(Mutex::new(self))
    }

    /// publish one complete checkpoint through the live database owner
    ///
    /// the workflow owns the complete correctness sequence:
    ///
    /// 1. serialize against another checkpoint publication,
    /// 2. pin the existing WAL recovery path,
    /// 3. capture catalog, MVCC, allocators, and replay frontier under the
    ///    database state barrier,
    /// 4. release the state barrier while writing and synchronizing the detached
    ///    snapshot file,
    /// 5. append and synchronize the snapshot pointer,
    /// 6. append and synchronize the exactly matching checkpoint marker,
    /// 7. release the old-history retention pin,
    /// 8. advance retention to the published replay boundary and prune only
    ///    complete segments allowed by remaining pins.
    ///
    /// the separate publication mutex prevents an older snapshot from publishing
    /// its marker after a newer snapshot and incorrectly becoming recovery's last
    /// checkpoint candidate
    pub async fn publish_checkpoint(
        database: &SharedLocalDatabase,
    ) -> Result<LiveCheckpointPublication> {
        let publication_lock = {
            let runtime = database.lock().await;
            runtime.checkpoint_publication_lock.clone()
        };

        let _publication_guard = publication_lock.lock_owned().await;

        let (data_dir, snapshot, adapter, retention_pin) = {
            let mut runtime = database.lock().await;

            let data_dir = runtime.data_dir.clone().ok_or_else(|| {
                Error::Configuration(
                    "checkpoint publication requires a recovered durable database runtime"
                        .to_string(),
                )
            })?;

            let adapter = runtime.checkpoint_adapter.clone().ok_or_else(|| {
                Error::Configuration(
                    "checkpoint publication requires the live database WAL adapter".to_string(),
                )
            })?;

            // acquire protection before fixing the snapshot frontier. If capture
            // fails, ordinary RAII drop releases the pin without changing
            // retention
            let retention_pin: CheckpointRetentionPin =
                adapter.acquire_checkpoint_retention_pin()?;
            let snapshot = runtime.capture_checkpoint_image()?;

            (data_dir, snapshot, adapter, retention_pin)
        };

        // snapshot file I/O and WAL synchronization are blocking operations.
        // Running them on the blocking pool prevents the async server executor
        // from being stalled while the database mutex remains available to
        // normal SQL work
        tokio::task::spawn_blocking(move || -> Result<LiveCheckpointPublication> {
            let snapshot_file = publish_snapshot_file(&data_dir, &snapshot)?;
            let checkpoint = publish_checkpoint_metadata(adapter.as_ref(), &snapshot_file)?;

            // the matching marker is now durable. The new snapshot is a valid
            // recovery source, so the older WAL path no longer needs this
            // publication pin
            drop(retention_pin);

            let pruned_segments =
                adapter.advance_checkpoint_retention(checkpoint.replay_from_lsn)?;

            Ok(LiveCheckpointPublication {
                checkpoint,
                retention_advanced_to: checkpoint.replay_from_lsn,
                pruned_segments,
            })
        })
        .await
        .map_err(|source| Error::RecoveryRequired {
            reason: format!("checkpoint publication worker failed: {source}"),
        })?
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

    /// capture one immutable database image under the runtime's write barrier.
    ///
    /// this operation fixes catalog state, MVCC state, allocator maxima, and the
    /// exact replay frontier as one consistent cut. It does not perform file or
    /// WAL publication; the live `publish_checkpoint` workflow owns those later
    /// stages
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
