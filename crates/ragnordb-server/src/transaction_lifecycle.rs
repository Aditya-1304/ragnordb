//! Bounded transaction lifecycle tracking and replicated status heartbeats.
//!
//! The gateway activates an entry only after the primary prewrite command has
//! crossed its Raft apply boundary. Buffered SQL transactions therefore consume
//! no heartbeat work and cannot acquire a lease before an intent exists.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ragnordb_catalog::{MetadataApplyOutcome, MetadataRejection};
use ragnordb_common::{
    Error, Result,
    codec::{Row, TxnStatus, TxnStatusRecord, Value},
    command_codec::{
        CachedTabletCommandOutcome, ExpirePendingTransactionStatus, HeartbeatTransactionStatus,
        PrewriteCommand, TabletCommand, TabletCommandEnvelope,
    },
    ids::{
        ClientRequestId, CommandKind, LogicalCommandId, NodeId, RequestId, RowKey, TableId,
        Timestamp, TxnId, request_id_for_logical_command,
    },
    rpc_codec::{TabletPointReadInspection, TabletRoute, TabletScanBatch},
};
use ragnordb_exec::{ResultSet, TabletGateway, TabletScanRoute};
use ragnordb_tablet::command::TabletCommandApplyOutcome;
use ragnordb_tablet::{IntentCleanupReport, ScanSpan};
use ragnordb_txn::{
    IntentResolutionDecision, TransactionFootprint, TransactionFootprintPolicy,
    plan_intent_resolution,
};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{metrics, multiraft_runtime::MetadataProposalClient, rpc::TabletRpcClient};
use ragnordb_multiraft::meta::MetadataRuntimeHandle;

const MAX_LIFECYCLE_ENTRIES: usize = 4_096;
const MAX_CLEANER_CLIENT_ID: u128 = u128::MAX - 1;
const HEARTBEAT_RPC_TIMEOUT_MS: u64 = 1_000;
const MAX_CLEANER_PAGES_PER_TABLET: usize = 64;
const MAX_CLEANER_TABLETS_PER_PASS: usize = 256;
const MAX_CLEANER_BYTES_PER_PAGE: u32 = 8 * 1024 * 1024;

/// Runtime limits for distributed transaction tracking, cleanup, and MVCC
/// history protection. Each environment variable has a conservative default;
/// all work-related values are validated before listeners are opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionRuntimeConfig {
    pub footprint: TransactionFootprintPolicy,
    pub max_active_transactions: usize,
    pub status_lease_ms: u64,
    pub heartbeat_interval: Duration,
    pub heartbeat_batch_size: usize,
    pub heartbeat_concurrency: usize,
    pub cleaner_interval: Duration,
    pub cleaner_policy: IntentCleanerPolicy,
    pub gc_sweep_interval: Duration,
    pub gc_protection_lease_ms: u64,
    pub metadata_timeout: Duration,
}

impl TransactionRuntimeConfig {
    pub fn from_environment(statement_timeout_ms: u64) -> Result<Self> {
        let defaults = TransactionFootprintPolicy::default();
        let footprint = TransactionFootprintPolicy {
            max_age_ms: env_value("RAGNORDB_TXN_MAX_AGE_MS", defaults.max_age_ms)?,
            max_write_bytes: env_value("RAGNORDB_TXN_MAX_WRITE_BYTES", defaults.max_write_bytes)?,
            max_write_keys: env_value("RAGNORDB_TXN_MAX_WRITE_KEYS", defaults.max_write_keys)?,
            max_read_spans: env_value("RAGNORDB_TXN_MAX_READ_SPANS", defaults.max_read_spans)?,
            max_participant_tablets: env_value(
                "RAGNORDB_TXN_MAX_PARTICIPANT_TABLETS",
                defaults.max_participant_tablets,
            )?,
            max_intent_accounting_bytes: env_value(
                "RAGNORDB_TXN_MAX_INTENT_ACCOUNTING_BYTES",
                defaults.max_intent_accounting_bytes,
            )?,
            max_participant_command_bytes: env_value(
                "RAGNORDB_TXN_MAX_PARTICIPANT_COMMAND_BYTES",
                defaults.max_participant_command_bytes,
            )?,
        };
        footprint.validate()?;

        let status_lease_ms = env_value("RAGNORDB_TXN_STATUS_LEASE_MS", 300_000_u64)?;
        let heartbeat_interval_ms = env_value("RAGNORDB_TXN_HEARTBEAT_INTERVAL_MS", 1_000_u64)?;
        let heartbeat_batch_size = env_value("RAGNORDB_TXN_HEARTBEAT_BATCH_SIZE", 128_usize)?;
        let heartbeat_concurrency = env_value("RAGNORDB_TXN_HEARTBEAT_CONCURRENCY", 64_usize)?;
        let max_active_transactions = env_value("RAGNORDB_TXN_MAX_ACTIVE", 1_024_usize)?;
        let rpc_timeout = Duration::from_millis(HEARTBEAT_RPC_TIMEOUT_MS);
        let cleaner_timeout_ms = env_value("RAGNORDB_TXN_BACKGROUND_RPC_TIMEOUT_MS", 2_000_u64)?;
        let cleaner_policy = IntentCleanerPolicy {
            page_size: env_value("RAGNORDB_TXN_CLEANER_PAGE_SIZE", 64_u32)?,
            max_pages_per_tablet: env_value("RAGNORDB_TXN_CLEANER_PAGES_PER_TABLET", 2_usize)?,
            max_tablets_per_pass: env_value("RAGNORDB_TXN_CLEANER_TABLETS_PER_PASS", 16_usize)?,
            max_bytes_per_page: env_value(
                "RAGNORDB_TXN_CLEANER_MAX_BYTES_PER_PAGE",
                1_048_576_u32,
            )?,
            timeout: Duration::from_millis(cleaner_timeout_ms),
        }
        .validate()?;
        let gc_protection_lease_ms = env_value(
            "RAGNORDB_TXN_GC_PROTECTION_LEASE_MS",
            footprint
                .max_age_ms
                .saturating_add(status_lease_ms)
                .saturating_add(statement_timeout_ms.saturating_mul(2)),
        )?;
        let config = Self {
            footprint,
            max_active_transactions,
            status_lease_ms,
            heartbeat_interval: Duration::from_millis(heartbeat_interval_ms),
            heartbeat_batch_size,
            heartbeat_concurrency,
            cleaner_interval: Duration::from_millis(env_value(
                "RAGNORDB_TXN_CLEANER_INTERVAL_MS",
                1_000_u64,
            )?),
            cleaner_policy,
            gc_sweep_interval: Duration::from_millis(env_value(
                "RAGNORDB_TXN_GC_SWEEP_INTERVAL_MS",
                5_000_u64,
            )?),
            gc_protection_lease_ms,
            metadata_timeout: Duration::from_millis(env_value(
                "RAGNORDB_TXN_METADATA_TIMEOUT_MS",
                5_000_u64,
            )?),
        };
        config.validate(statement_timeout_ms, rpc_timeout)?;
        Ok(config)
    }

    fn validate(self, statement_timeout_ms: u64, heartbeat_rpc_timeout: Duration) -> Result<()> {
        if self.max_active_transactions == 0
            || self.max_active_transactions > MAX_LIFECYCLE_ENTRIES
            || self.status_lease_ms == 0
            || self.heartbeat_interval.is_zero()
            || self.heartbeat_batch_size == 0
            || self.heartbeat_batch_size > 256
            || self.heartbeat_concurrency == 0
            || self.heartbeat_concurrency > 64
            || self.heartbeat_concurrency > self.heartbeat_batch_size
            || self.cleaner_interval.is_zero()
            || self.gc_sweep_interval.is_zero()
            || self.gc_protection_lease_ms
                < self
                    .footprint
                    .max_age_ms
                    .saturating_add(self.status_lease_ms)
                    .saturating_add(statement_timeout_ms)
            || self.metadata_timeout.is_zero()
            || self.heartbeat_interval.as_millis() >= self.status_lease_ms as u128
        {
            return Err(Error::Configuration(
                "transaction runtime bounds and lease timings are inconsistent".to_string(),
            ));
        }

        // Every registry entry must receive one attempt before its status lease
        // expires, even when each bounded RPC reaches its timeout. One candidate
        // performs a status read and at most one replicated heartbeat proposal.
        let candidates_per_round = self.heartbeat_batch_size as u128;
        let round_count = self
            .max_active_transactions
            .div_ceil(self.heartbeat_batch_size) as u128;
        let concurrent_waves = self
            .heartbeat_batch_size
            .div_ceil(self.heartbeat_concurrency) as u128;
        let rpc_timeout_ms = heartbeat_rpc_timeout.as_millis();
        let round_work_ms = concurrent_waves
            .saturating_mul(
                rpc_timeout_ms
                    .saturating_mul(2)
                    .saturating_add(self.metadata_timeout.as_millis()),
            )
            .saturating_add(self.heartbeat_interval.as_millis());
        let full_rotation_ms = round_count.saturating_mul(round_work_ms);
        if full_rotation_ms >= self.status_lease_ms as u128 || candidates_per_round == 0 {
            return Err(Error::Configuration(
                "transaction status lease is too short for the configured bounded heartbeat rotation"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

fn env_value<T>(name: &'static str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };
    value.parse().map_err(|error| {
        Error::Configuration(format!("{name} must contain a valid value: {error}"))
    })
}

const AGGREGATE_GC_PROTECTION_ID: u128 = u128::MAX - 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcProtectionPublication {
    Register { floor: Timestamp, deadline_ms: u64 },
    Update { floor: Timestamp, deadline_ms: u64 },
    Release,
}

/// Process-local membership for ordinary transaction history pins. The
/// metadata state machine stores one durable floor for this owner; this tracker
/// keeps the transaction-to-floor relationship local and publishes only the
/// transitions that can change the aggregate protection.
#[derive(Debug, Clone, Default)]
struct GcProtectionTracker {
    state: Arc<Mutex<GcProtectionTrackerState>>,
    /// Serializes transaction admission with safe-point advancement. A newly
    /// allocated transaction must publish its protection before a sweep can
    /// advance past that transaction's read timestamp.
    admission: Arc<Mutex<()>>,
}

#[derive(Debug, Default)]
struct GcProtectionTrackerState {
    active: BTreeMap<TxnId, Timestamp>,
    published_floor: Option<Timestamp>,
    published_deadline_ms: Option<u64>,
}

impl GcProtectionTracker {
    fn admission_guard(&self) -> std::sync::MutexGuard<'_, ()> {
        self.admission
            .lock()
            .expect("GC protection admission lock poisoned")
    }

    #[cfg(test)]
    fn register(&self, txn_id: TxnId, read_ts: Timestamp, deadline_ms: u64) -> bool {
        self.register_with_publish(txn_id, read_ts, deadline_ms, |_| Ok(()))
            .expect("in-memory GC protection publication cannot fail")
            .is_some()
    }

    fn register_with_publish<F>(
        &self,
        txn_id: TxnId,
        read_ts: Timestamp,
        deadline_ms: u64,
        publish: F,
    ) -> Result<Option<GcProtectionPublication>>
    where
        F: FnOnce(GcProtectionPublication) -> Result<()>,
    {
        let mut state = self.state.lock().expect("GC protection tracker poisoned");
        if state.active.insert(txn_id, read_ts).is_some() {
            return Ok(None);
        }

        let floor = state
            .active
            .values()
            .min()
            .copied()
            .expect("newly registered transaction must establish an active floor");
        let publication = match state.published_floor {
            None => GcProtectionPublication::Register { floor, deadline_ms },
            Some(previous_floor) if floor < previous_floor => {
                GcProtectionPublication::Update { floor, deadline_ms }
            }
            Some(_) => return Ok(None),
        };

        if let Err(error) = publish(publication) {
            state.active.remove(&txn_id);
            return Err(error);
        }
        state.published_floor = Some(floor);
        state.published_deadline_ms = Some(deadline_ms);
        Ok(Some(publication))
    }

    #[cfg(test)]
    fn release(&self, txn_id: TxnId) -> bool {
        self.release_with(txn_id, |_| Ok(()))
            .expect("in-memory GC protection release cannot fail")
            .is_some()
    }

    fn release_with<F>(&self, txn_id: TxnId, release: F) -> Result<Option<GcProtectionPublication>>
    where
        F: FnOnce(GcProtectionPublication) -> Result<()>,
    {
        let mut state = self.state.lock().expect("GC protection tracker poisoned");
        if state.active.remove(&txn_id).is_none() {
            return Ok(None);
        }

        let Some(previous_floor) = state.published_floor else {
            return Ok(None);
        };
        let publication = match state.active.values().min().copied() {
            None => GcProtectionPublication::Release,
            Some(floor) if floor != previous_floor => GcProtectionPublication::Update {
                floor,
                deadline_ms: state
                    .published_deadline_ms
                    .expect("published floor must have a lease deadline"),
            },
            Some(_) => return Ok(None),
        };

        if let Err(error) = release(publication) {
            // Keep the old durable floor in local state. It remains
            // conservative, and retaining it lets a later update/renew repair
            // the metadata record without creating a protection gap.
            return Err(error);
        }

        match publication {
            GcProtectionPublication::Release => {
                state.published_floor = None;
                state.published_deadline_ms = None;
            }
            GcProtectionPublication::Register { .. }
            | GcProtectionPublication::Update {
                floor: _,
                deadline_ms: _,
            } => {
                let floor = state
                    .active
                    .values()
                    .min()
                    .copied()
                    .expect("non-release publication requires an active transaction");
                state.published_floor = Some(floor);
            }
        }
        Ok(Some(publication))
    }

    fn renew<F>(&self, deadline_ms: u64, renew: F) -> Result<bool>
    where
        F: FnOnce() -> Result<()>,
    {
        let mut state = self.state.lock().expect("GC protection tracker poisoned");
        let Some(previous_deadline_ms) = state.published_deadline_ms else {
            return Ok(false);
        };
        if state.active.is_empty() || deadline_ms <= previous_deadline_ms {
            return Ok(false);
        }

        renew()?;
        state.published_deadline_ms = Some(deadline_ms);
        Ok(true)
    }

    #[cfg(test)]
    fn active_count(&self) -> usize {
        self.state
            .lock()
            .expect("GC protection tracker poisoned")
            .active
            .len()
    }
}

/// Shared transaction control plane used by SQL, background maintenance, and
/// status endpoints. Metadata mutations always pass through its Raft proposal
/// client; the runtime handle is read-only and supplies published safe points.
#[derive(Clone)]
pub struct TransactionRuntime {
    pub lifecycle: TransactionLifecycleRegistry,
    pub config: TransactionRuntimeConfig,
    metadata: MetadataRuntimeHandle,
    metadata_control: MetadataProposalClient,
    owner_id: u128,
    gc_protection_tracker: GcProtectionTracker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcProtectionSnapshot {
    pub safe_point: Timestamp,
    pub active_protections: usize,
    pub minimum_protected_timestamp: Option<Timestamp>,
    pub earliest_lease_deadline_ms: Option<u64>,
}

impl TransactionRuntime {
    pub fn new(
        node_id: NodeId,
        config: TransactionRuntimeConfig,
        metadata: MetadataRuntimeHandle,
        metadata_control: MetadataProposalClient,
    ) -> Result<Self> {
        let lifecycle = TransactionLifecycleRegistry::new(
            config.max_active_transactions,
            config.status_lease_ms,
            config.status_lease_ms,
        )?;
        let process_nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(1);
        let owner_id = process_nonce
            .wrapping_add(u128::from(node_id.0) << 64)
            .wrapping_add(u128::from(std::process::id()))
            .max(1);

        Ok(Self {
            lifecycle,
            config,
            metadata,
            metadata_control,
            owner_id,
            gc_protection_tracker: GcProtectionTracker::default(),
        })
    }

    pub fn gc_safe_point(&self) -> Timestamp {
        self.metadata.state_snapshot().gc_safe_point()
    }

    /// Hold the admission barrier across transaction timestamp allocation and
    /// GC-protection publication. The background safe-point worker acquires
    /// the same barrier before advancing the durable MVCC history floor.
    pub(crate) fn gc_protection_admission(&self) -> std::sync::MutexGuard<'_, ()> {
        self.gc_protection_tracker.admission_guard()
    }

    pub fn gc_protection_snapshot(&self) -> GcProtectionSnapshot {
        let now_ms = unix_time_millis();
        let state = self.metadata.state_snapshot();
        let mut active_protections = 0usize;
        let mut minimum_protected_timestamp = None;
        let mut earliest_lease_deadline_ms = None;
        for protection in state.gc_protections() {
            if protection.lease_deadline_ms <= now_ms {
                continue;
            }
            active_protections = active_protections.saturating_add(1);
            minimum_protected_timestamp = Some(
                minimum_protected_timestamp
                    .map_or(protection.protected_timestamp, |current: Timestamp| {
                        current.min(protection.protected_timestamp)
                    }),
            );
            earliest_lease_deadline_ms = Some(
                earliest_lease_deadline_ms.map_or(protection.lease_deadline_ms, |current: u64| {
                    current.min(protection.lease_deadline_ms)
                }),
            );
        }
        GcProtectionSnapshot {
            safe_point: state.gc_safe_point(),
            active_protections,
            minimum_protected_timestamp,
            earliest_lease_deadline_ms,
        }
    }

    /// Publish one aggregate history protection for all ordinary transactions
    /// owned by this process. Floor changes use one metadata apply, so there is
    /// no interval in which the GC safe point can pass an active reader.
    pub fn register_gc_protection(&self, txn_id: TxnId, read_ts: Timestamp) -> Result<()> {
        let now_ms = unix_time_millis();
        let deadline_ms = now_ms
            .checked_add(self.config.gc_protection_lease_ms)
            .ok_or_else(|| Error::InvalidArgument("GC protection deadline overflowed".into()))?;
        let tracker = self.gc_protection_tracker.clone();
        let started = Instant::now();
        let publication =
            tracker.register_with_publish(txn_id, read_ts, deadline_ms, |publication| {
                self.publish_gc_protection(publication, now_ms, self.config.metadata_timeout)
            });
        metrics::histogram_record(
            "ragnordb_txn_gc_protection_register_seconds",
            started.elapsed().as_secs_f64(),
        );
        let publication = publication?;
        metrics::counter_inc("ragnordb_txn_gc_protection_register_total");
        match publication {
            Some(GcProtectionPublication::Register { .. }) => {
                metrics::counter_inc("ragnordb_txn_gc_protection_aggregate_register_total");
            }
            Some(GcProtectionPublication::Update { .. }) => {
                metrics::counter_inc("ragnordb_txn_gc_protection_aggregate_update_total");
            }
            Some(GcProtectionPublication::Release) | None => {}
        }
        Ok(())
    }

    fn publish_gc_protection(
        &self,
        publication: GcProtectionPublication,
        now_ms: u64,
        timeout: Duration,
    ) -> Result<()> {
        let outcome = match publication {
            GcProtectionPublication::Register { floor, deadline_ms } => {
                self.metadata_control.register_gc_protection(
                    self.owner_id,
                    AGGREGATE_GC_PROTECTION_ID,
                    floor,
                    deadline_ms,
                    now_ms,
                    timeout,
                )?
            }
            GcProtectionPublication::Update { floor, deadline_ms } => {
                self.metadata_control.update_gc_protection(
                    self.owner_id,
                    AGGREGATE_GC_PROTECTION_ID,
                    floor,
                    deadline_ms,
                    now_ms,
                    timeout,
                )?
            }
            GcProtectionPublication::Release => self.metadata_control.release_gc_protection(
                self.owner_id,
                AGGREGATE_GC_PROTECTION_ID,
                timeout,
            )?,
        };

        match outcome {
            MetadataApplyOutcome::Applied | MetadataApplyOutcome::AlreadyApplied => Ok(()),
            MetadataApplyOutcome::Rejected(MetadataRejection::GcProtectionBelowSafePoint {
                protected,
                safe_point,
            }) => Err(Error::WriteConflict(format!(
                "transaction snapshot {} is below the MVCC GC safe point {}",
                protected.0, safe_point.0
            ))),
            MetadataApplyOutcome::Rejected(rejection) => Err(Error::InvalidArgument(format!(
                "metadata rejected transaction GC protection transition: {rejection}"
            ))),
            other => Err(Error::CorruptData(format!(
                "metadata returned unexpected GC protection result {other:?}"
            ))),
        }
    }

    /// Extend the aggregate history lease only after the caller has confirmed
    /// that the authoritative transaction status is still pending and live.
    fn renew_gc_protection(&self, deadline_ms: u64, now_ms: u64, timeout: Duration) -> Result<()> {
        let tracker = self.gc_protection_tracker.clone();
        tracker.renew(deadline_ms, || {
            let outcome = self.metadata_control.renew_gc_protection(
                self.owner_id,
                AGGREGATE_GC_PROTECTION_ID,
                deadline_ms,
                now_ms,
                timeout,
            )?;
            match outcome {
                MetadataApplyOutcome::Applied | MetadataApplyOutcome::AlreadyApplied => Ok(()),
                MetadataApplyOutcome::Rejected(rejection) => Err(Error::WriteConflict(format!(
                    "metadata refused transaction GC lease renewal: {rejection}"
                ))),
                other => Err(Error::CorruptData(format!(
                    "metadata returned unexpected GC renewal result {other:?}"
                ))),
            }
        })?;
        Ok(())
    }

    pub fn release_gc_protection(&self, txn_id: TxnId) -> Result<()> {
        let now_ms = unix_time_millis();
        let tracker = self.gc_protection_tracker.clone();
        let started = Instant::now();
        let publication = tracker.release_with(txn_id, |publication| {
            self.publish_gc_protection(publication, now_ms, self.config.metadata_timeout)
        });
        metrics::histogram_record(
            "ragnordb_txn_gc_protection_release_seconds",
            started.elapsed().as_secs_f64(),
        );
        let publication = publication?;
        match publication {
            Some(GcProtectionPublication::Release) => {
                metrics::counter_inc("ragnordb_txn_gc_protection_aggregate_release_total");
            }
            Some(GcProtectionPublication::Update { .. }) => {
                metrics::counter_inc("ragnordb_txn_gc_protection_aggregate_update_total");
            }
            Some(GcProtectionPublication::Register { .. }) | None => {}
        }
        Ok(())
    }
    pub fn advance_gc_safe_point_once(&self) -> Result<Timestamp> {
        let _admission = self.gc_protection_tracker.admission_guard();
        let state = self.metadata.state_snapshot();
        let candidate = state.timestamp_reserved_until();
        let now_ms = unix_time_millis();
        let outcome = self.metadata_control.advance_gc_safe_point(
            candidate,
            now_ms,
            self.config.metadata_timeout,
        )?;
        match outcome {
            MetadataApplyOutcome::Applied | MetadataApplyOutcome::AlreadyApplied => {
                let published = self.metadata.state_snapshot().gc_safe_point();
                metrics::gauge_set("ragnordb_txn_gc_safe_point", published.0 as f64);
                metrics::gauge_set(
                    "ragnordb_txn_gc_protections_active",
                    self.metadata
                        .state_snapshot()
                        .gc_protections()
                        .filter(|protection| protection.lease_deadline_ms > now_ms)
                        .count() as f64,
                );
                Ok(published)
            }
            MetadataApplyOutcome::Rejected(rejection) => Err(Error::WriteConflict(format!(
                "metadata rejected MVCC GC safe-point advancement: {rejection}"
            ))),
            other => Err(Error::CorruptData(format!(
                "metadata returned unexpected GC safe-point result {other:?}"
            ))),
        }
    }
}

/// Advance the global MVCC safe point only through metadata Raft apply. The
/// operation is serialized per node and never writes through A-WAL retention.
pub async fn run_gc_safe_point_loop(
    runtime: Arc<TransactionRuntime>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut ticker = tokio::time::interval(runtime.config.gc_sweep_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticker.tick() => {
                let worker = runtime.clone();
                match tokio::task::spawn_blocking(move || worker.advance_gc_safe_point_once()).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => tracing::warn!(error = %error, "MVCC GC safe-point advance failed"),
                    Err(error) => tracing::warn!(error = %error, "MVCC GC safe-point task failed"),
                }
            }
        }
    }
}

/// Safe SQL/admin projection of one durable pending transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionDiagnostic {
    pub transaction_id: u64,
    pub status: &'static str,
    pub age_ms: u64,
    pub participant_tablets: usize,
    pub write_keys: usize,
    pub write_bytes: usize,
    pub read_spans: usize,
    pub intent_accounting_bytes: usize,
    pub participant_command_bytes: usize,
    pub last_heartbeat_timestamp: Option<u64>,
    pub lease_deadline_ms: Option<u64>,
    pub lease_remaining_ms: Option<u64>,
    pub last_heartbeat_age_ms: Option<u64>,
}

/// Bounded transaction-status result shared by SQL and admin surfaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionStatusSnapshot {
    pub active_count: usize,
    pub truncated: bool,
    pub transactions: Vec<TransactionDiagnostic>,
}

impl TransactionStatusSnapshot {
    /// Convert this value-only diagnostic snapshot into the stable SQL result
    /// shape used by `SHOW TRANSACTIONS`.
    pub fn into_result_set(self) -> ResultSet {
        let rows = self
            .transactions
            .into_iter()
            .map(|transaction| Row {
                values: vec![
                    Value::Int(saturating_sql_integer(transaction.transaction_id)),
                    Value::Text(transaction.status.to_string()),
                    Value::Int(saturating_sql_integer(transaction.age_ms)),
                    Value::Int(saturating_sql_integer(transaction.participant_tablets)),
                    Value::Int(saturating_sql_integer(transaction.write_keys)),
                    Value::Int(saturating_sql_integer(transaction.write_bytes)),
                    Value::Int(saturating_sql_integer(transaction.read_spans)),
                    Value::Int(saturating_sql_integer(transaction.intent_accounting_bytes)),
                    Value::Int(saturating_sql_integer(
                        transaction.participant_command_bytes,
                    )),
                    optional_sql_integer(transaction.lease_deadline_ms),
                    optional_sql_integer(transaction.lease_remaining_ms),
                    optional_sql_integer(transaction.last_heartbeat_timestamp),
                    optional_sql_integer(transaction.last_heartbeat_age_ms),
                ],
            })
            .collect();
        ResultSet {
            columns: ragnordb_exec::transaction_diagnostic_columns(),
            rows,
        }
    }
}

fn saturating_sql_integer(value: impl TryInto<u64>) -> i64 {
    value.try_into().unwrap_or(u64::MAX).min(i64::MAX as u64) as i64
}

fn optional_sql_integer(value: Option<u64>) -> Value {
    value.map_or(Value::Null, |value| {
        Value::Int(saturating_sql_integer(value))
    })
}

#[derive(Debug)]
struct TrackedTransaction {
    /// Original status identity fences retries that establish the same
    /// transaction after a route refresh or an uncertain RPC outcome.
    initial_status: TxnStatusRecord,
    current_status: TxnStatusRecord,
    started_at: Instant,
    last_heartbeat_at: Instant,
    write_keys: usize,
    write_bytes: usize,
    intent_accounting_bytes: usize,
    read_spans: usize,
    participants: BTreeSet<u64>,
    participant_command_bytes: usize,
}

#[derive(Debug, Default)]
struct RegistryState {
    pending_registrations: BTreeMap<TxnId, TxnStatusRecord>,
    pending_footprints: BTreeMap<TxnId, TransactionFootprint>,
    active: BTreeMap<TxnId, TrackedTransaction>,
    heartbeat_cursor: Option<TxnId>,
}

/// Per-process registry for transactions that have durably entered prewrite.
/// Capacity reservation happens immediately before the first primary proposal;
/// the public active count changes only once apply is confirmed or uncertain.
#[derive(Debug, Clone)]
pub struct TransactionLifecycleRegistry {
    state: Arc<Mutex<RegistryState>>,
    max_active: usize,
    lease_duration_ms: u64,
    lock_ttl_ms: u64,
}

impl TransactionLifecycleRegistry {
    pub fn new(max_active: usize, lease_duration_ms: u64, lock_ttl_ms: u64) -> Result<Self> {
        if max_active == 0 || lease_duration_ms == 0 || lock_ttl_ms == 0 {
            return Err(Error::InvalidArgument(
                "transaction lifecycle bounds and lease timings must be non-zero".to_string(),
            ));
        }
        Ok(Self {
            state: Arc::new(Mutex::new(RegistryState::default())),
            max_active,
            lease_duration_ms,
            lock_ttl_ms,
        })
    }

    fn prepare_primary_status(
        &self,
        status: &TxnStatusRecord,
        now_ms: u64,
    ) -> Result<TxnStatusRecord> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = state
            .active
            .get(&status.txn_id)
            .map(|tracked| &tracked.initial_status)
            .or_else(|| state.pending_registrations.get(&status.txn_id))
        {
            if !same_transaction_identity(existing, status) {
                return Err(Error::CorruptData(
                    "primary prewrite retry changed transaction status identity".to_string(),
                ));
            }
            return Ok(existing.clone());
        }

        if !state.active.contains_key(&status.txn_id)
            && !state.pending_registrations.contains_key(&status.txn_id)
            && !state.pending_footprints.contains_key(&status.txn_id)
            && lifecycle_reservation_count(&state) >= self.max_active
        {
            return Err(Error::ProposalUnavailable {
                reason: "active transaction lifecycle registry is at capacity".to_string(),
            });
        }

        let mut prepared = status.clone();
        if prepared.lease_deadline_ms.is_none() {
            prepared.lease_deadline_ms =
                Some(now_ms.checked_add(self.lease_duration_ms).ok_or_else(|| {
                    Error::InvalidArgument("transaction lease deadline overflowed".to_string())
                })?);
        }
        if prepared.last_heartbeat_timestamp.is_none() {
            prepared.last_heartbeat_timestamp = Some(prepared.start_timestamp);
        }
        prepared
            .validate()
            .map_err(|reason| Error::InvalidArgument(reason.to_string()))?;
        state
            .pending_registrations
            .insert(status.txn_id, prepared.clone());
        Ok(prepared)
    }

    fn activate(&self, status: TxnStatusRecord, started_at: Instant) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let initial_status = state
            .pending_registrations
            .remove(&status.txn_id)
            .unwrap_or_else(|| status.clone());
        let footprint = state.pending_footprints.remove(&status.txn_id);
        let started_at = footprint
            .and_then(|footprint| started_at.checked_sub(Duration::from_millis(footprint.age_ms)))
            .unwrap_or(started_at);
        state
            .active
            .entry(status.txn_id)
            .or_insert_with(|| TrackedTransaction {
                initial_status,
                current_status: status,
                started_at,
                last_heartbeat_at: Instant::now(),
                write_keys: footprint.map_or(0, |footprint| footprint.write_keys),
                write_bytes: footprint.map_or(0, |footprint| footprint.write_bytes),
                intent_accounting_bytes: footprint
                    .map_or(0, |footprint| footprint.intent_accounting_bytes),
                read_spans: footprint.map_or(0, |footprint| footprint.read_spans),
                participants: BTreeSet::new(),
                participant_command_bytes: 0,
            });
        set_active_gauge(&state);
    }

    fn release_reservation(&self, txn_id: TxnId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.pending_registrations.remove(&txn_id);
        state.pending_footprints.remove(&txn_id);
        set_active_gauge(&state);
    }

    /// Record the SQL transaction's bounded logical footprint before commit
    /// planning can admit any participant proposal. The staged value is
    /// consumed only if the durable primary status activates this transaction.
    pub fn record_transaction_footprint(
        &self,
        txn_id: TxnId,
        footprint: TransactionFootprint,
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(tracked) = state.active.get_mut(&txn_id) {
            tracked.write_keys = footprint.write_keys;
            tracked.write_bytes = footprint.write_bytes;
            tracked.intent_accounting_bytes = footprint.intent_accounting_bytes;
            tracked.read_spans = footprint.read_spans;
            return Ok(());
        }
        if !state.pending_footprints.contains_key(&txn_id)
            && lifecycle_reservation_count(&state) >= self.max_active
        {
            return Err(Error::ProposalUnavailable {
                reason: "active transaction lifecycle registry is at capacity".to_string(),
            });
        }
        state.pending_footprints.insert(txn_id, footprint);
        Ok(())
    }

    fn record_prewrite(
        &self,
        txn_id: TxnId,
        route: &TabletRoute,
        command: &PrewriteCommand,
        encoded_command_bytes: usize,
        count_bytes: bool,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(tracked) = state.active.get_mut(&txn_id) else {
            return;
        };
        tracked.participants.insert(route.tablet_id.0);
        if count_bytes {
            tracked.participant_command_bytes = tracked
                .participant_command_bytes
                .saturating_add(encoded_command_bytes);
        }
        if let Some(status) = &command.pending_status {
            tracked.current_status = status.clone();
        }
    }

    fn update_status(&self, status: TxnStatusRecord) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(tracked) = state.active.get_mut(&status.txn_id) {
            tracked.current_status = status;
            tracked.last_heartbeat_at = Instant::now();
        }
    }

    pub fn unregister(&self, txn_id: TxnId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.pending_registrations.remove(&txn_id);
        state.pending_footprints.remove(&txn_id);
        state.active.remove(&txn_id);
        set_active_gauge(&state);
    }

    pub fn is_active(&self, txn_id: TxnId) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .active
            .contains_key(&txn_id)
    }

    fn heartbeat_candidates(&self, limit: usize) -> Vec<(TxnId, TxnStatusRecord, Duration)> {
        use std::ops::Bound::{Excluded, Unbounded};

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut candidates = Vec::with_capacity(limit.min(state.active.len()));
        if let Some(cursor) = state.heartbeat_cursor {
            candidates.extend(
                state
                    .active
                    .range((Excluded(cursor), Unbounded))
                    .take(limit)
                    .map(|(txn_id, tracked)| {
                        (
                            *txn_id,
                            tracked.current_status.clone(),
                            tracked.started_at.elapsed(),
                        )
                    }),
            );
        } else {
            candidates.extend(state.active.iter().take(limit).map(|(txn_id, tracked)| {
                (
                    *txn_id,
                    tracked.current_status.clone(),
                    tracked.started_at.elapsed(),
                )
            }));
        }
        if candidates.len() < limit {
            let remaining = limit - candidates.len();
            candidates.extend(
                state
                    .active
                    .iter()
                    .take(remaining)
                    .map(|(txn_id, tracked)| {
                        (
                            *txn_id,
                            tracked.current_status.clone(),
                            tracked.started_at.elapsed(),
                        )
                    }),
            );
        }
        if let Some((last, _, _)) = candidates.last() {
            state.heartbeat_cursor = Some(*last);
        }
        candidates
    }

    /// Return a bounded value-only snapshot. Keys and mutation payloads never
    /// cross this diagnostics boundary.
    pub fn status_snapshot(&self, limit: usize) -> TransactionStatusSnapshot {
        let now_ms = unix_time_millis();
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let active_count = state.active.len();
        let transactions = state
            .active
            .iter()
            .take(limit)
            .map(|(txn_id, tracked)| {
                let last_heartbeat = tracked.current_status.last_heartbeat_timestamp;
                let last_heartbeat_age_ms = last_heartbeat.map(|_| {
                    tracked
                        .last_heartbeat_at
                        .elapsed()
                        .as_millis()
                        .min(u64::MAX as u128) as u64
                });
                let lease_deadline_ms = tracked.current_status.lease_deadline_ms;
                let participant_tablets = tracked
                    .participants
                    .len()
                    .max(tracked.current_status.participant_tablet_ids.len());
                TransactionDiagnostic {
                    transaction_id: txn_id.0,
                    status: match tracked.current_status.status {
                        TxnStatus::Pending => "pending",
                        TxnStatus::Committed => "committed",
                        TxnStatus::Aborted => "aborted",
                    },
                    age_ms: tracked
                        .started_at
                        .elapsed()
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                    participant_tablets,
                    write_keys: tracked.write_keys,
                    write_bytes: tracked.write_bytes,
                    read_spans: tracked.read_spans,
                    intent_accounting_bytes: tracked
                        .intent_accounting_bytes
                        .saturating_add(64)
                        .saturating_add(tracked.initial_status.primary_key.len())
                        .saturating_add(
                            participant_tablets.saturating_mul(std::mem::size_of::<u64>()),
                        ),
                    participant_command_bytes: tracked.participant_command_bytes,
                    last_heartbeat_timestamp: last_heartbeat.map(|timestamp| timestamp.0),
                    lease_deadline_ms,
                    lease_remaining_ms: lease_deadline_ms
                        .map(|deadline| deadline.saturating_sub(now_ms)),
                    last_heartbeat_age_ms,
                }
            })
            .collect();
        TransactionStatusSnapshot {
            active_count,
            truncated: active_count > limit,
            transactions,
        }
    }
}

fn set_active_gauge(state: &RegistryState) {
    metrics::set_active_transactions(state.active.len());
}

fn lifecycle_reservation_count(state: &RegistryState) -> usize {
    state
        .active
        .keys()
        .chain(state.pending_registrations.keys())
        .chain(state.pending_footprints.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .len()
}

fn same_transaction_identity(left: &TxnStatusRecord, right: &TxnStatusRecord) -> bool {
    left.txn_id == right.txn_id
        && left.start_timestamp == right.start_timestamp
        && left.primary_key == right.primary_key
        && left.participant_tablet_ids == right.participant_tablet_ids
}

/// SQL-facing TabletGateway wrapper that installs the initial lease atomically
/// with primary prewrite apply and observes terminal status apply.
#[derive(Clone)]
pub struct LifecycleTabletGateway {
    inner: TabletRpcClient,
    registry: TransactionLifecycleRegistry,
}

impl LifecycleTabletGateway {
    pub fn new(inner: TabletRpcClient, registry: TransactionLifecycleRegistry) -> Self {
        Self { inner, registry }
    }

    fn submit(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: Option<LogicalCommandId>,
        acknowledged_through: Option<u64>,
        mut command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        let mut initial_status = None;
        if let TabletCommand::Prewrite(prewrite) = &mut command {
            if let Some(status) = prewrite.pending_status.as_ref() {
                let prepared = self
                    .registry
                    .prepare_primary_status(status, unix_time_millis())?;
                prewrite.pending_status = Some(prepared.clone());
                prewrite.ttl_ms = self.registry.lock_ttl_ms;
                initial_status = Some(prepared);
            }
        }

        let encoded_command_bytes = measure_command_envelope(
            route,
            &request_id,
            logical_command_id,
            acknowledged_through,
            &command,
        );
        let encoded_command_bytes = match encoded_command_bytes {
            Ok(bytes) => bytes,
            Err(error) => {
                if let Some(status) = initial_status {
                    self.registry.release_reservation(status.txn_id);
                }
                return Err(error);
            }
        };
        let result = self.inner.submit_command_with_identity_and_ack(
            route,
            request_id,
            logical_command_id,
            acknowledged_through,
            command.clone(),
            timeout,
        );

        match result {
            Ok(outcome) => {
                if let TabletCommand::Prewrite(prewrite) = &command {
                    if let Some(status) = initial_status {
                        self.registry.activate(status, Instant::now());
                    }
                    self.registry.record_prewrite(
                        prewrite.txn_id,
                        route,
                        prewrite,
                        encoded_command_bytes,
                        !outcome.deduplicated,
                    );
                }
                match &command {
                    TabletCommand::Commit(command) if command.committed_status.is_some() => {
                        self.registry.unregister(command.txn_id);
                        metrics::counter_inc("ragnordb_txn_commits_total");
                    }
                    TabletCommand::PublishAbortedTransactionStatus(command) => {
                        self.registry.unregister(command.status_record.txn_id);
                        metrics::counter_inc("ragnordb_txn_aborts_total");
                    }
                    TabletCommand::ExpirePendingTransactionStatus(command) => {
                        self.registry.unregister(command.expected_status.txn_id);
                        metrics::counter_inc("ragnordb_txn_aborts_total");
                    }
                    TabletCommand::HeartbeatTransactionStatus(command) => {
                        self.registry.update_status(command.next_status.clone());
                    }
                    TabletCommand::ResolveIntent(_) => {
                        metrics::record_reader_intent_resolution();
                    }
                    _ => {}
                }
                Ok(outcome)
            }
            Err(error) => {
                if let Some(status) = initial_status {
                    if matches!(
                        error,
                        Error::ProposalUnavailable { .. }
                            | Error::TabletUnavailable { .. }
                            | Error::RequestOutcomeUnknown { .. }
                    ) {
                        // The proposal may have crossed Raft apply even though
                        // the caller lost its response. Heartbeats still read
                        // the authoritative record first and remove this
                        // provisional registration if it is absent.
                        self.registry.activate(status, Instant::now());
                        if let TabletCommand::Prewrite(prewrite) = &command {
                            self.registry.record_prewrite(
                                prewrite.txn_id,
                                route,
                                prewrite,
                                encoded_command_bytes,
                                true,
                            );
                        }
                    } else {
                        self.registry.release_reservation(status.txn_id);
                    }
                }
                Err(error)
            }
        }
    }
}

fn measure_command_envelope(
    route: &TabletRoute,
    request_id: &RequestId,
    logical_command_id: Option<LogicalCommandId>,
    acknowledged_through: Option<u64>,
    command: &TabletCommand,
) -> Result<usize> {
    let envelope = match logical_command_id {
        Some(identity) => TabletCommandEnvelope::new_with_logical_command_id_and_ack(
            request_id.clone(),
            identity,
            route.tablet_id,
            route.tablet_epoch,
            acknowledged_through,
            command.clone(),
        ),
        None => TabletCommandEnvelope::new(
            request_id.clone(),
            route.tablet_id,
            route.tablet_epoch,
            command.clone(),
        ),
    }
    .map_err(|error| Error::InvalidArgument(error.to_string()))?;
    envelope
        .encode()
        .map(|bytes| bytes.len())
        .map_err(|error| Error::InvalidArgument(error.to_string()))
}

impl TabletGateway for LifecycleTabletGateway {
    fn lookup_tablet_route(&self, table_id: TableId, key: &[u8]) -> Result<TabletRoute> {
        TabletGateway::lookup_tablet_route(&self.inner, table_id, key)
    }

    fn inspect_point(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        row_key: RowKey,
        read_timestamp: Timestamp,
        timeout: Duration,
    ) -> Result<TabletPointReadInspection> {
        TabletGateway::inspect_point(
            &self.inner,
            route,
            request_id,
            row_key,
            read_timestamp,
            timeout,
        )
    }

    fn transaction_status(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        txn_id: TxnId,
        timeout: Duration,
    ) -> Result<Option<TxnStatusRecord>> {
        TabletGateway::transaction_status(&self.inner, route, request_id, txn_id, timeout)
    }

    fn read_point(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        row_key: RowKey,
        read_timestamp: Timestamp,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>> {
        TabletGateway::read_point(
            &self.inner,
            route,
            request_id,
            row_key,
            read_timestamp,
            timeout,
        )
    }

    fn lookup_scan_routes(
        &self,
        table_id: TableId,
        span: &ScanSpan,
    ) -> Result<Vec<TabletScanRoute>> {
        TabletGateway::lookup_scan_routes(&self.inner, table_id, span)
    }

    fn scan_page(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        span: &ScanSpan,
        resume_after: Option<&[u8]>,
        read_timestamp: Timestamp,
        max_rows: u32,
        max_bytes: u32,
        timeout: Duration,
    ) -> Result<TabletScanBatch> {
        TabletGateway::scan_page(
            &self.inner,
            route,
            request_id,
            span,
            resume_after,
            read_timestamp,
            max_rows,
            max_bytes,
            timeout,
        )
    }

    fn submit_command(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        self.submit(route, request_id, None, None, command, timeout)
    }

    fn submit_command_with_identity(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        self.submit(
            route,
            request_id,
            Some(logical_command_id),
            None,
            command,
            timeout,
        )
    }

    fn submit_command_with_identity_and_ack(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        acknowledged_through: Option<u64>,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        self.submit(
            route,
            request_id,
            Some(logical_command_id),
            acknowledged_through,
            command,
            timeout,
        )
    }

    fn query_original_outcome(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        timeout: Duration,
    ) -> Result<Option<CachedTabletCommandOutcome>> {
        TabletGateway::query_original_outcome(
            &self.inner,
            route,
            request_id,
            logical_command_id,
            timeout,
        )
    }
}

/// Run one bounded heartbeat pass. A round rotates through at most the
/// configured batch size, and a fixed semaphore bounds concurrent RPC work.
/// Each status transition still uses the tablet's replicated compare-and-set.
pub async fn heartbeat_once_bounded(
    runtime: Arc<TransactionRuntime>,
    gateway: TabletRpcClient,
) -> Result<usize> {
    let config = runtime.config;
    let candidates = runtime
        .lifecycle
        .heartbeat_candidates(config.heartbeat_batch_size);
    let permits = Arc::new(Semaphore::new(config.heartbeat_concurrency));
    let mut tasks = JoinSet::new();

    for (txn_id, cached_status, age) in candidates {
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Configuration("heartbeat admission is closed".into()))?;
        let worker_runtime = runtime.clone();
        let worker_gateway = gateway.clone();
        let timeout = Duration::from_millis(HEARTBEAT_RPC_TIMEOUT_MS);
        tasks.spawn(async move {
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                heartbeat_one(
                    &worker_gateway,
                    &worker_runtime,
                    txn_id,
                    cached_status,
                    age,
                    timeout,
                )
            })
            .await
            .map_err(|error| Error::Configuration(format!("heartbeat worker failed: {error}")))?
        });
    }

    let mut renewed = 0usize;
    let mut first_error = None;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(did_renew)) => renewed = renewed.saturating_add(usize::from(did_renew)),
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(Error::Configuration(format!(
                        "heartbeat task failed: {error}"
                    )));
                }
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(renewed),
    }
}

/// Run durable heartbeat scheduling until shutdown. The loop has at most one
/// bounded batch in flight and uses delay semantics when a pass takes longer
/// than its cadence.
pub async fn run_transaction_heartbeat_loop(
    runtime: Arc<TransactionRuntime>,
    gateway: TabletRpcClient,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut ticker = tokio::time::interval(runtime.config.heartbeat_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticker.tick() => {
                if let Err(error) = heartbeat_once_bounded(runtime.clone(), gateway.clone()).await {
                    tracing::warn!(error = %error, "bounded transaction heartbeat pass failed");
                }
            }
        }
    }
}

fn heartbeat_one(
    gateway: &TabletRpcClient,
    runtime: &TransactionRuntime,
    txn_id: TxnId,
    cached_status: TxnStatusRecord,
    age: Duration,
    timeout: Duration,
) -> Result<bool> {
    metrics::record_heartbeat_attempt();
    let primary = ragnordb_storage::key::decode_row_key(&cached_status.primary_key)
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    let route = gateway.lookup_tablet_route(primary.table_id, &primary.primary_key_bytes)?;
    let request_id = status_read_request(txn_id, route.raft_group_id);
    let Some(current) = gateway.transaction_status(&route, request_id, txn_id, timeout)? else {
        runtime.lifecycle.unregister(txn_id);
        metrics::record_heartbeat_failure();
        return Ok(false);
    };
    if current.status != TxnStatus::Pending {
        runtime.lifecycle.unregister(txn_id);
        return Ok(false);
    }
    let Some(existing_deadline) = current.lease_deadline_ms else {
        runtime.lifecycle.unregister(txn_id);
        metrics::record_heartbeat_failure();
        return Ok(false);
    };
    let now_ms = unix_time_millis();
    if now_ms >= existing_deadline {
        // Expiry belongs to the cleaner's replicated CAS, never a local
        // heartbeat decision.
        metrics::record_heartbeat_failure();
        return Ok(false);
    }
    if age.as_millis() >= runtime.config.footprint.max_age_ms as u128 {
        // Maximum transaction age is a hard cap. We still read authoritative
        // status above so terminal work is unregistered, but do not renew an
        // over-age pending transaction.
        metrics::record_heartbeat_failure();
        return Ok(false);
    }
    let next_deadline = now_ms
        .checked_add(runtime.config.status_lease_ms)
        .ok_or_else(|| Error::InvalidArgument("heartbeat lease deadline overflowed".into()))?
        .max(existing_deadline);
    if next_deadline == existing_deadline {
        return Ok(false);
    }

    // Renew the independent metadata history pin before extending the status
    // lease. If that authority is unavailable, this transaction is not given a
    // longer lifetime than its safe MVCC history protection.
    if let Err(error) =
        runtime.renew_gc_protection(next_deadline, now_ms, runtime.config.metadata_timeout)
    {
        metrics::record_heartbeat_failure();
        return if matches!(error, Error::WriteConflict(_)) {
            Ok(false)
        } else {
            Err(error)
        };
    }

    let previous_heartbeat = current
        .last_heartbeat_timestamp
        .unwrap_or(current.start_timestamp);
    let next_heartbeat = Timestamp(previous_heartbeat.0.checked_add(1).ok_or_else(|| {
        Error::Configuration("transaction heartbeat timestamp space is exhausted".into())
    })?);
    let mut next = current.clone();
    next.lease_deadline_ms = Some(next_deadline);
    next.last_heartbeat_timestamp = Some(next_heartbeat.max(current.start_timestamp));
    let command = HeartbeatTransactionStatus {
        expected_status: current.clone(),
        next_status: next.clone(),
        now_ms,
    };
    let identity = maintenance_identity(txn_id, existing_deadline, CommandKind::Noop);
    let request_id = request_id_for_logical_command(identity, route.raft_group_id)
        .map_err(|error| Error::InvalidArgument(error.to_string()))?;
    match gateway.submit_command_with_identity_and_ack(
        &route,
        request_id,
        Some(identity),
        None,
        TabletCommand::HeartbeatTransactionStatus(command),
        timeout,
    ) {
        Ok(_) => {
            runtime.lifecycle.update_status(next);
            Ok(true)
        }
        Err(error) => {
            metrics::record_heartbeat_failure();
            if matches!(error, Error::WriteConflict(_)) {
                Ok(false)
            } else {
                Err(error)
            }
        }
    }
}

fn maintenance_identity(txn_id: TxnId, sequence: u64, kind: CommandKind) -> LogicalCommandId {
    LogicalCommandId {
        client_request_id: ClientRequestId {
            client_id: (u128::MAX << 64) | u128::from(txn_id.0),
            session_epoch: txn_id.0.max(1),
            request_sequence: sequence.max(1),
        },
        command_ordinal: 1,
        kind,
    }
}

fn status_read_request(txn_id: TxnId, group_id: ragnordb_common::ids::RaftGroupId) -> RequestId {
    RequestId {
        client_id: MAX_CLEANER_CLIENT_ID - u128::from(txn_id.0),
        sequence: 1,
        raft_group_id: group_id,
    }
}

pub fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

/// Per-node bounded work limits for one cleaner scheduling round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntentCleanerPolicy {
    pub page_size: u32,
    pub max_pages_per_tablet: usize,
    pub max_tablets_per_pass: usize,
    pub max_bytes_per_page: u32,
    pub timeout: Duration,
}

impl IntentCleanerPolicy {
    pub fn validate(self) -> Result<Self> {
        if self.page_size == 0
            || self.page_size > 64
            || self.max_pages_per_tablet == 0
            || self.max_pages_per_tablet > MAX_CLEANER_PAGES_PER_TABLET
            || self.max_tablets_per_pass == 0
            || self.max_tablets_per_pass > MAX_CLEANER_TABLETS_PER_PASS
            || self.max_bytes_per_page == 0
            || self.max_bytes_per_page > MAX_CLEANER_BYTES_PER_PAGE
            || self.timeout.is_zero()
        {
            return Err(Error::InvalidArgument(
                "intent cleaner page, tablet, byte, and timeout limits must be bounded and non-zero"
                    .to_string(),
            ));
        }
        Ok(self)
    }
}

/// Round-robin cursor state is kept by the single cleaner task. A metadata
/// generation change invalidates every old continuation because a tablet split
/// may move keys to a new owner below a previous cursor.
#[derive(Debug, Default)]
pub struct IntentCleanerProgress {
    metadata_generation: Option<u64>,
    next_route_after: Option<(TableId, ragnordb_common::ids::TabletId)>,
    resume_after: BTreeMap<ragnordb_common::ids::TabletId, Vec<u8>>,
}

/// Perform one bounded round-robin sweep over current metadata routes.
/// Foreground SQL resolution uses the gateway directly and is never queued
/// behind this single background worker.
pub fn clean_intents_once(
    gateway: &TabletRpcClient,
    progress: &mut IntentCleanerProgress,
    policy: IntentCleanerPolicy,
    shutdown: &CancellationToken,
) -> Result<IntentCleanupReport> {
    let policy = policy.validate()?;
    let cursor_before = progress.next_route_after;
    let (mut generation, mut routes, mut route_page_has_more) = gateway
        .tablet_route_page_with_generation(
            progress.next_route_after,
            policy.max_tablets_per_pass,
        )?;
    if progress.metadata_generation != Some(generation) {
        progress.resume_after.clear();
        progress.next_route_after = None;
        (generation, routes, route_page_has_more) =
            gateway.tablet_route_page_with_generation(None, policy.max_tablets_per_pass)?;
        progress.metadata_generation = Some(generation);
    }
    if routes.is_empty() && cursor_before.is_some() {
        progress.next_route_after = None;
        let (wrapped_generation, wrapped_routes, wrapped_has_more) =
            gateway.tablet_route_page_with_generation(None, policy.max_tablets_per_pass)?;
        if wrapped_generation != generation {
            progress.resume_after.clear();
            progress.metadata_generation = Some(wrapped_generation);
        }
        routes = wrapped_routes;
        route_page_has_more = wrapped_has_more;
    }
    if routes.is_empty() || shutdown.is_cancelled() {
        return Ok(IntentCleanupReport::default());
    }

    let mut report = IntentCleanupReport::default();
    let mut expired_transactions = BTreeSet::new();
    for (table_id, route) in &routes {
        if shutdown.is_cancelled() {
            report.truncated = true;
            break;
        }
        let mut cursor = progress.resume_after.get(&route.tablet_id).cloned();
        let mut exhausted = false;

        for _ in 0..policy.max_pages_per_tablet {
            if shutdown.is_cancelled() {
                report.truncated = true;
                break;
            }
            let request_id = RequestId {
                client_id: MAX_CLEANER_CLIENT_ID,
                sequence: 1,
                raft_group_id: route.raft_group_id,
            };
            let batch = match gateway.scan_intents_page(
                route,
                request_id,
                &ScanSpan::unbounded(),
                cursor.as_deref(),
                policy.page_size,
                policy.max_bytes_per_page,
                policy.timeout,
            ) {
                Ok(batch) => batch,
                Err(Error::StaleTabletEpoch { .. }) => {
                    progress.resume_after.clear();
                    report.truncated = true;
                    break;
                }
                Err(error) => return Err(error),
            };
            report.pages_scanned = report.pages_scanned.saturating_add(1);
            for intent in &batch.intents {
                if shutdown.is_cancelled() {
                    report.truncated = true;
                    break;
                }
                report.intents_scanned = report.intents_scanned.saturating_add(1);
                clean_one_intent(
                    gateway,
                    *table_id,
                    intent,
                    policy.timeout,
                    &mut report,
                    &mut expired_transactions,
                )?;
            }
            if shutdown.is_cancelled() {
                break;
            }
            cursor = batch.next_resume_after;
            if batch.exhausted {
                exhausted = true;
                progress.resume_after.remove(&route.tablet_id);
                break;
            }
            if let Some(cursor_key) = cursor.clone() {
                progress.resume_after.insert(route.tablet_id, cursor_key);
            } else {
                return Err(Error::CorruptData(
                    "intent cleaner received a non-exhausted page without a cursor".to_string(),
                ));
            }
        }
        if !exhausted {
            report.truncated = true;
        }
    }
    let (last_table_id, last_route) = routes.last().expect("non-empty route page passed above");
    progress.next_route_after = if route_page_has_more {
        Some((*last_table_id, last_route.tablet_id))
    } else {
        None
    };
    if route_page_has_more {
        report.truncated = true;
    }
    Ok(report)
}

fn clean_one_intent(
    gateway: &TabletRpcClient,
    table_id: TableId,
    intent: &ragnordb_common::rpc_codec::TabletScanIntent,
    timeout: Duration,
    report: &mut IntentCleanupReport,
    expired_transactions: &mut BTreeSet<TxnId>,
) -> Result<()> {
    let primary = ragnordb_storage::key::decode_row_key(&intent.lock.primary_key)
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    let status_route = gateway.lookup_tablet_route(primary.table_id, &primary.primary_key_bytes)?;
    let Some(mut status) = gateway.transaction_status(
        &status_route,
        status_read_request(intent.lock.txn_id, status_route.raft_group_id),
        intent.lock.txn_id,
        timeout,
    )?
    else {
        report.uncertain_intents = report.uncertain_intents.saturating_add(1);
        return Ok(());
    };
    if status
        .primary_tablet_id()
        .map_err(|reason| Error::CorruptData(reason.to_string()))?
        != status_route.tablet_id
    {
        return Err(Error::CorruptData(
            "cleaner status response came from a non-authoritative tablet".to_string(),
        ));
    }

    match status.status {
        TxnStatus::Pending => {
            let Some(deadline_ms) = status.lease_deadline_ms else {
                report.uncertain_intents = report.uncertain_intents.saturating_add(1);
                return Ok(());
            };
            let now_ms = unix_time_millis();
            if now_ms < deadline_ms {
                report.pending_intents = report.pending_intents.saturating_add(1);
                return Ok(());
            }
            let mut aborted = status.clone();
            aborted.status = TxnStatus::Aborted;
            aborted.commit_timestamp = None;
            let command = ExpirePendingTransactionStatus {
                expected_status: status.clone(),
                now_ms,
            };
            let identity =
                maintenance_identity(intent.lock.txn_id, deadline_ms, CommandKind::Catalog);
            let request_id = request_id_for_logical_command(identity, status_route.raft_group_id)
                .map_err(|error| Error::InvalidArgument(error.to_string()))?;
            match gateway.submit_command_with_identity_and_ack(
                &status_route,
                request_id,
                Some(identity),
                None,
                TabletCommand::ExpirePendingTransactionStatus(command),
                timeout,
            ) {
                Ok(outcome)
                    if outcome.result
                        == ragnordb_tablet::command::TabletCommandApplyResult::PublishAbortedTransactionStatus =>
                {
                    status = aborted;
                    if expired_transactions.insert(intent.lock.txn_id) {
                        report.expired_transactions = report.expired_transactions.saturating_add(1);
                    }
                }
                Ok(_) => {
                    return Err(Error::CorruptData(
                        "expiry command returned an unexpected apply result".to_string(),
                    ));
                }
                Err(Error::WriteConflict(_)) => {
                    report.uncertain_intents = report.uncertain_intents.saturating_add(1);
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        TxnStatus::Committed | TxnStatus::Aborted => {}
    }

    let encoded_key = ragnordb_storage::key::encode_row_key(&RowKey {
        table_id,
        primary_key_bytes: intent.key.clone(),
    })?;
    let IntentResolutionDecision::Resolve(plan) =
        plan_intent_resolution(&encoded_key, &intent.lock, &status)?
    else {
        report.pending_intents = report.pending_intents.saturating_add(1);
        return Ok(());
    };
    let participant_route = gateway.lookup_tablet_route(table_id, &intent.key)?;
    let request_id =
        request_id_for_logical_command(plan.logical_command_id, participant_route.raft_group_id)
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
    let outcome = gateway.submit_command_with_identity_and_ack(
        &participant_route,
        request_id,
        Some(plan.logical_command_id),
        None,
        TabletCommand::ResolveIntent(plan.command),
        timeout,
    )?;
    if outcome.result != ragnordb_tablet::command::TabletCommandApplyResult::ResolveIntent {
        return Err(Error::CorruptData(
            "cleaner received a non-resolution apply result".to_string(),
        ));
    }
    report.resolved_intents = report.resolved_intents.saturating_add(1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_status(txn_id: u64) -> TxnStatusRecord {
        let row_key =
            ragnordb_storage::key::make_row_key(TableId(1), &[Value::Int(txn_id as i64)]).unwrap();
        let primary_key = ragnordb_storage::key::encode_row_key(&row_key).unwrap();
        TxnStatusRecord {
            txn_id: TxnId(txn_id),
            start_timestamp: Timestamp(txn_id + 100),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key,
            participant_tablet_ids: vec![1],
            last_heartbeat_timestamp: None,
            lease_deadline_ms: None,
        }
    }

    #[test]
    fn gc_protection_lease_cannot_expire_before_a_pending_status_lease() {
        // This catches a crash-recovery hole where an abandoned transaction
        // still has a live pending status after its MVCC history pin expires.
        let config = TransactionRuntimeConfig {
            footprint: TransactionFootprintPolicy::default(),
            max_active_transactions: 1,
            status_lease_ms: 300_000,
            heartbeat_interval: Duration::from_millis(1_000),
            heartbeat_batch_size: 1,
            heartbeat_concurrency: 1,
            cleaner_interval: Duration::from_millis(1_000),
            cleaner_policy: IntentCleanerPolicy {
                page_size: 1,
                max_pages_per_tablet: 1,
                max_tablets_per_pass: 1,
                max_bytes_per_page: 1_024,
                timeout: Duration::from_millis(2_000),
            },
            gc_sweep_interval: Duration::from_millis(5_000),
            gc_protection_lease_ms: 120_000,
            metadata_timeout: Duration::from_millis(5_000),
        };

        assert!(
            config
                .validate(30_000, Duration::from_millis(1_000))
                .is_err()
        );
    }

    #[test]
    fn initial_lease_is_stable_across_prewrite_retries() {
        let registry = TransactionLifecycleRegistry::new(2, 1_000, 1_000).unwrap();
        let original = pending_status(7);

        let first = registry.prepare_primary_status(&original, 10_000).unwrap();
        let retry = registry.prepare_primary_status(&original, 10_500).unwrap();

        assert_eq!(first.lease_deadline_ms, Some(11_000));
        assert_eq!(
            first.last_heartbeat_timestamp,
            Some(original.start_timestamp)
        );
        assert_eq!(retry, first);
    }

    #[test]
    fn buffered_footprints_do_not_activate_or_consume_heartbeat_slots() {
        let registry = TransactionLifecycleRegistry::new(1, 1_000, 1_000).unwrap();
        let footprint = TransactionFootprint {
            age_ms: 10,
            write_bytes: 100,
            write_keys: 1,
            read_spans: 1,
            participant_tablets: 1,
            intent_accounting_bytes: 164,
            participant_command_bytes: 200,
        };

        registry
            .record_transaction_footprint(TxnId(7), footprint)
            .unwrap();
        assert_eq!(registry.status_snapshot(10).active_count, 0);
        assert!(registry.heartbeat_candidates(10).is_empty());

        let pending = registry
            .prepare_primary_status(&pending_status(7), 10_000)
            .unwrap();
        registry.activate(pending, Instant::now());
        assert_eq!(registry.status_snapshot(10).active_count, 1);
        assert!(matches!(
            registry.prepare_primary_status(&pending_status(8), 10_000),
            Err(Error::ProposalUnavailable { .. })
        ));

        registry.unregister(TxnId(7));
        assert_eq!(registry.status_snapshot(10).active_count, 0);
        assert!(
            registry
                .prepare_primary_status(&pending_status(8), 10_000)
                .is_ok()
        );
    }

    #[test]
    fn unregister_releases_a_rejected_transactions_staged_footprint() {
        let registry = TransactionLifecycleRegistry::new(1, 1_000, 1_000).unwrap();
        let footprint = TransactionFootprint {
            age_ms: 10,
            write_bytes: 100,
            write_keys: 1,
            read_spans: 0,
            participant_tablets: 0,
            intent_accounting_bytes: 164,
            participant_command_bytes: 0,
        };

        registry
            .record_transaction_footprint(TxnId(7), footprint)
            .unwrap();
        registry.unregister(TxnId(7));

        assert!(
            registry
                .record_transaction_footprint(TxnId(8), footprint)
                .is_ok()
        );
    }

    #[test]
    fn active_status_snapshot_keeps_the_staged_transaction_footprint() {
        let registry = TransactionLifecycleRegistry::new(1, 1_000, 1_000).unwrap();
        registry
            .record_transaction_footprint(
                TxnId(7),
                TransactionFootprint {
                    age_ms: 10,
                    write_bytes: 40,
                    write_keys: 2,
                    read_spans: 3,
                    participant_tablets: 1,
                    intent_accounting_bytes: 168,
                    participant_command_bytes: 0,
                },
            )
            .unwrap();
        registry.activate(pending_status(7), Instant::now());

        let row = registry.status_snapshot(1).into_result_set();

        assert_eq!(row.rows[0].values[4], Value::Int(2));
        assert_eq!(row.rows[0].values[5], Value::Int(40));
        assert_eq!(row.rows[0].values[6], Value::Int(3));
    }

    #[test]
    fn show_transaction_projection_contains_counts_but_no_mutation_data() {
        let registry = TransactionLifecycleRegistry::new(2, 1_000, 1_000).unwrap();
        let mut status = pending_status(7);
        status.lease_deadline_ms = Some(unix_time_millis().saturating_add(10_000));
        status.last_heartbeat_timestamp = Some(status.start_timestamp);
        registry
            .record_transaction_footprint(
                TxnId(7),
                TransactionFootprint {
                    age_ms: 10,
                    write_bytes: 37,
                    write_keys: 1,
                    read_spans: 3,
                    participant_tablets: 1,
                    intent_accounting_bytes: 101,
                    participant_command_bytes: 0,
                },
            )
            .unwrap();
        registry.activate(status, Instant::now());
        {
            let mut state = registry.state.lock().unwrap();
            let tracked = state.active.get_mut(&TxnId(7)).unwrap();
            tracked.participant_command_bytes = 200;
        }

        let rows = registry.status_snapshot(10).into_result_set();
        let rendered = format!("{rows:?}");
        assert_eq!(rows.rows.len(), 1);
        assert!(!rendered.contains("private-row-value"));
        assert_eq!(rows.rows[0].values[4], Value::Int(1));
        assert_eq!(rows.rows[0].values[5], Value::Int(37));
        assert_eq!(rows.rows[0].values[6], Value::Int(3));
        assert_eq!(rows.rows[0].values[8], Value::Int(200));
    }

    #[test]
    fn gc_protection_tracker_publishes_one_aggregate_lease_for_multiple_transactions() {
        let tracker = GcProtectionTracker::default();

        assert!(tracker.register(TxnId(1), Timestamp(10), 100));
        assert!(!tracker.register(TxnId(2), Timestamp(20), 100));
        assert_eq!(tracker.active_count(), 2);
        assert!(tracker.release(TxnId(1)));
        assert!(tracker.release(TxnId(2)));
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn gc_protection_admission_blocks_safe_point_until_registration_finishes() {
        let tracker = GcProtectionTracker::default();
        let admission = tracker.admission_guard();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let sweep_tracker = tracker.clone();
        let sweep = std::thread::spawn(move || {
            let _sweep_admission = sweep_tracker.admission_guard();
            entered_tx.send(()).unwrap();
        });

        assert!(entered_rx.recv_timeout(Duration::from_millis(25)).is_err());
        assert!(tracker.register(TxnId(1), Timestamp(10), 100));
        drop(admission);

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("safe-point sweep did not enter after transaction admission");
        sweep.join().unwrap();
    }
}
