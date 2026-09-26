//! local transaction identity and timestamp allocation
//!
//! SQL session code depends on the `TransctionManager` boundary instead of
//! allocating transction metadata itself. we currently use the in-memory
//! implementation; later metadata raft timestamp service can implement
//! the same boundary without changing session transaction semantics

use crate::Transaction;
use ragnordb_common::{
    Error, Result,
    ids::{Timestamp, TxnId},
};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

/// Allocates transaction identities and MVCC timestamps.
///
/// Implementations must guarantee:
///
/// - transaction IDs are nonzero and never reused,
/// - start timestamps are nonzero and monotonically increasing,
/// - commit timestamps are greater than their transaction's start timestamp.
pub trait TransactionManager {
    /// Begin a transaction with a newly allocated identity and start timestamp.
    fn begin_transaction(&mut self) -> Result<Transaction>;

    /// Allocate a commit timestamp strictly greater than `start_ts`.
    fn allocate_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp>;

    /// Observe a committed replicated high-water mark before serving new work.
    /// Local implementations use this during recovery and follower catch-up;
    /// the timestamp-oracle implementation advances its local cursor without
    /// pretending that an unreserved range is locally available.
    fn observe_replicated_high_water(&mut self, transaction_id: TxnId, timestamp: Timestamp);

    fn last_allocated_transaction_id(&self) -> TxnId;

    fn last_allocated_timestamp(&self) -> Timestamp;

    fn timestamp_oracle_stats(&self) -> TimestampOracleStats {
        TimestampOracleStats::default()
    }
}

impl<M> TransactionManager for Box<M>
where
    M: TransactionManager + ?Sized,
{
    fn begin_transaction(&mut self) -> Result<Transaction> {
        (**self).begin_transaction()
    }

    fn allocate_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp> {
        (**self).allocate_commit_timestamp(start_ts)
    }

    fn observe_replicated_high_water(&mut self, transaction_id: TxnId, timestamp: Timestamp) {
        (**self).observe_replicated_high_water(transaction_id, timestamp);
    }

    fn last_allocated_transaction_id(&self) -> TxnId {
        (**self).last_allocated_transaction_id()
    }

    fn last_allocated_timestamp(&self) -> Timestamp {
        (**self).last_allocated_timestamp()
    }

    fn timestamp_oracle_stats(&self) -> TimestampOracleStats {
        (**self).timestamp_oracle_stats()
    }
}

/// Cumulative timestamp-oracle counters exposed to the server metrics layer.
/// Latencies are nanoseconds so callers can report deltas without coupling the
/// transaction crate to a particular metrics backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampOracleStats {
    pub allocations: u64,
    pub reservations: u64,
    pub allocation_latency_nanos: u64,
    pub reservation_latency_nanos: u64,
    pub last_allocated: Timestamp,
    pub reserved_until: Timestamp,
}

impl Default for TimestampOracleStats {
    fn default() -> Self {
        Self {
            allocations: 0,
            reservations: 0,
            allocation_latency_nanos: 0,
            reservation_latency_nanos: 0,
            last_allocated: Timestamp(0),
            reserved_until: Timestamp(0),
        }
    }
}

/// Durable reservation boundary used by the live timestamp oracle.
///
/// The provider must return only after the requested frontier has committed in
/// the metadata Raft group. Returning a merely proposed or locally appended
/// frontier would allow a crash to reuse timestamps that were handed to SQL
/// sessions but never became part of the recovery authority.
pub trait TimestampReservationProvider {
    fn reserve_timestamps(&mut self, requested_until: Timestamp) -> Result<TimestampReservation>;
}

/// One disjoint interval returned by the durable reservation authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampReservation {
    pub reserved_from: Timestamp,
    pub reserved_until: Timestamp,
}

/// In-memory allocator over a metadata-owned durable timestamp frontier.
///
/// The oracle consumes a committed interval locally and extends it before the
/// interval is exhausted. Unused values are intentionally abandoned on
/// restart or leadership loss: the next owner starts strictly above the
/// durable frontier, which is the safety property that prevents reuse.
#[derive(Debug)]
pub struct TimestampOracle<P> {
    provider: P,
    next_timestamp: u64,
    last_allocated: Timestamp,
    reserved_until: Timestamp,
    reservation_size: u64,
    prefetch_threshold: u64,
    allocations: u64,
    reservations: u64,
    allocation_latency_nanos: u64,
    reservation_latency_nanos: u64,
}

impl<P> TimestampOracle<P>
where
    P: TimestampReservationProvider,
{
    /// Create an oracle and durably reserve its first interval.
    pub fn new(provider: P, reservation_size: u64, prefetch_threshold: u64) -> Result<Self> {
        validate_reservation_policy(reservation_size, prefetch_threshold)?;

        let mut oracle = Self {
            provider,
            next_timestamp: 1,
            last_allocated: Timestamp(0),
            reserved_until: Timestamp(0),
            reservation_size,
            prefetch_threshold,
            allocations: 0,
            reservations: 0,
            allocation_latency_nanos: 0,
            reservation_latency_nanos: 0,
        };
        oracle.ensure_reserved_through(Timestamp(1))?;
        Ok(oracle)
    }

    /// Recreate an oracle from a metadata snapshot or a committed failover
    /// read. No local range is considered reusable until a new reservation
    /// command commits for this owner.
    pub fn from_durable_frontier(
        provider: P,
        durable_frontier: Timestamp,
        reservation_size: u64,
        prefetch_threshold: u64,
    ) -> Result<Self> {
        validate_reservation_policy(reservation_size, prefetch_threshold)?;
        let next_timestamp = durable_frontier.0.checked_add(1).ok_or_else(|| {
            Error::Configuration(
                "timestamp oracle has exhausted the u64 timestamp space".to_string(),
            )
        })?;

        Ok(Self {
            provider,
            next_timestamp,
            last_allocated: durable_frontier,
            reserved_until: durable_frontier,
            reservation_size,
            prefetch_threshold,
            allocations: 0,
            reservations: 0,
            allocation_latency_nanos: 0,
            reservation_latency_nanos: 0,
        })
    }

    /// Allocate one timestamp from the committed local interval.
    pub fn allocate_timestamp(&mut self) -> Result<Timestamp> {
        let started = Instant::now();
        let result = (|| {
            let next = Timestamp(self.next_timestamp);
            if next.0 == 0 {
                return Err(Error::Configuration(
                    "timestamp oracle generated the reserved zero timestamp".to_string(),
                ));
            }

            if next > self.reserved_until {
                self.ensure_reserved_through(next)?;
            } else if self.remaining() <= self.prefetch_threshold {
                // Prefetch happens before consuming the threshold-crossing value.
                // A failed reservation therefore fails the request rather than
                // handing out a timestamp while silently losing the refill error.
                let next_frontier = self.reserved_until.0.checked_add(1).ok_or_else(|| {
                    Error::Configuration(
                        "timestamp oracle has exhausted the u64 timestamp space".to_string(),
                    )
                })?;
                self.ensure_reserved_through(Timestamp(next_frontier))?;
            }

            let allocated = Timestamp(self.next_timestamp);
            self.next_timestamp = self.next_timestamp.checked_add(1).ok_or_else(|| {
                Error::Configuration(
                    "timestamp oracle has exhausted the u64 timestamp space".to_string(),
                )
            })?;
            self.last_allocated = allocated;
            Ok(allocated)
        })();
        if result.is_ok() {
            self.allocations = self.allocations.saturating_add(1);
            self.allocation_latency_nanos = self
                .allocation_latency_nanos
                .saturating_add(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        }
        result
    }

    /// Allocate a commit timestamp strictly newer than the transaction start.
    ///
    /// Any unused local values below `start_ts` are skipped. This is required
    /// when a transaction begins on one owner and commits after a leadership
    /// transition or after another transaction advanced the shared frontier.
    pub fn allocate_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp> {
        if start_ts.0 == 0 {
            return Err(Error::InvalidArgument(
                "transaction start timestamp 0 is reserved".to_string(),
            ));
        }

        let minimum = start_ts.0.checked_add(1).ok_or_else(|| {
            Error::Configuration("commit timestamp cannot be newer than u64::MAX".to_string())
        })?;
        if self.next_timestamp < minimum {
            self.next_timestamp = minimum;
        }
        self.allocate_timestamp()
    }

    /// Discard the current in-memory interval after leadership loss and begin
    /// above the newly observed durable metadata frontier.
    pub fn reset_after_failover(&mut self, durable_frontier: Timestamp) -> Result<()> {
        if durable_frontier < self.reserved_until {
            return Err(Error::Configuration(format!(
                "failover frontier {} is below the local durable reservation {}",
                durable_frontier.0, self.reserved_until.0
            )));
        }

        self.next_timestamp = durable_frontier.0.checked_add(1).ok_or_else(|| {
            Error::Configuration(
                "timestamp oracle has exhausted the u64 timestamp space".to_string(),
            )
        })?;
        self.last_allocated = durable_frontier;
        self.reserved_until = durable_frontier;
        Ok(())
    }

    /// Advance the local cursor after applying a replicated commit observed
    /// outside the local allocator's own request path.
    pub fn observe_replicated_high_water(&mut self, timestamp: Timestamp) {
        self.last_allocated = self.last_allocated.max(timestamp);
        if timestamp.0 >= self.next_timestamp {
            self.next_timestamp = timestamp.0.checked_add(1).unwrap_or(0);
        }
        self.reserved_until = self.reserved_until.max(timestamp);
    }

    pub fn last_allocated(&self) -> Timestamp {
        self.last_allocated
    }

    pub fn reserved_until(&self) -> Timestamp {
        self.reserved_until
    }

    pub fn provider(&self) -> &P {
        &self.provider
    }

    pub fn provider_mut(&mut self) -> &mut P {
        &mut self.provider
    }

    pub fn stats(&self) -> TimestampOracleStats {
        TimestampOracleStats {
            allocations: self.allocations,
            reservations: self.reservations,
            allocation_latency_nanos: self.allocation_latency_nanos,
            reservation_latency_nanos: self.reservation_latency_nanos,
            last_allocated: self.last_allocated,
            reserved_until: self.reserved_until,
        }
    }

    fn remaining(&self) -> u64 {
        self.reserved_until
            .0
            .saturating_sub(self.next_timestamp)
            .saturating_add(1)
    }

    fn ensure_reserved_through(&mut self, target: Timestamp) -> Result<()> {
        if target <= self.reserved_until {
            return Ok(());
        }

        let required_extension = target.0 - self.reserved_until.0;
        let extension = self.reservation_size.max(required_extension);
        let requested_until = self
            .reserved_until
            .0
            .checked_add(extension)
            .ok_or_else(|| {
                Error::Configuration(
                    "timestamp oracle has exhausted the u64 timestamp space".to_string(),
                )
            })?;
        let started = Instant::now();
        let previous_frontier = self.reserved_until;
        let reservation = self
            .provider
            .reserve_timestamps(Timestamp(requested_until))?;
        if reservation.reserved_from <= previous_frontier
            || reservation.reserved_until < Timestamp(requested_until)
            || reservation.reserved_from > reservation.reserved_until
        {
            return Err(Error::Configuration(format!(
                "timestamp reservation provider returned invalid interval {}..={} for requested {}",
                reservation.reserved_from.0, reservation.reserved_until.0, requested_until,
            )));
        }
        if self.next_timestamp > previous_frontier.0 {
            self.next_timestamp = self.next_timestamp.max(reservation.reserved_from.0);
        }
        self.reserved_until = reservation.reserved_until;
        self.reservations = self.reservations.saturating_add(1);
        self.reservation_latency_nanos = self
            .reservation_latency_nanos
            .saturating_add(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        Ok(())
    }
}

fn validate_reservation_policy(reservation_size: u64, prefetch_threshold: u64) -> Result<()> {
    if reservation_size == 0 {
        return Err(Error::InvalidArgument(
            "timestamp reservation size must be nonzero".to_string(),
        ));
    }
    if prefetch_threshold >= reservation_size {
        return Err(Error::InvalidArgument(
            "timestamp prefetch threshold must be below reservation size".to_string(),
        ));
    }
    Ok(())
}

/// Transaction manager backed by the metadata timestamp oracle.
///
/// Transaction IDs intentionally reuse the globally unique start timestamp.
/// This keeps the Phase 6.1 authority single-sourced: a transaction identity
/// cannot be minted independently on two nodes while the later transaction
/// status protocol is still under construction.
#[derive(Debug)]
pub struct ReservedTimestampTransactionManager<P> {
    oracle: TimestampOracle<P>,
}

impl<P> ReservedTimestampTransactionManager<P>
where
    P: TimestampReservationProvider,
{
    pub fn new(provider: P, reservation_size: u64, prefetch_threshold: u64) -> Result<Self> {
        Ok(Self {
            oracle: TimestampOracle::new(provider, reservation_size, prefetch_threshold)?,
        })
    }

    pub fn from_durable_frontier(
        provider: P,
        durable_frontier: Timestamp,
        reservation_size: u64,
        prefetch_threshold: u64,
    ) -> Result<Self> {
        Ok(Self {
            oracle: TimestampOracle::from_durable_frontier(
                provider,
                durable_frontier,
                reservation_size,
                prefetch_threshold,
            )?,
        })
    }

    pub fn oracle(&self) -> &TimestampOracle<P> {
        &self.oracle
    }

    pub fn oracle_mut(&mut self) -> &mut TimestampOracle<P> {
        &mut self.oracle
    }
}

impl<P> TransactionManager for ReservedTimestampTransactionManager<P>
where
    P: TimestampReservationProvider,
{
    fn begin_transaction(&mut self) -> Result<Transaction> {
        let start_ts = self.oracle.allocate_timestamp()?;
        Transaction::new(TxnId(start_ts.0), start_ts)
    }

    fn allocate_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp> {
        self.oracle.allocate_commit_timestamp(start_ts)
    }

    fn observe_replicated_high_water(&mut self, _transaction_id: TxnId, timestamp: Timestamp) {
        self.oracle.observe_replicated_high_water(timestamp);
    }

    fn last_allocated_transaction_id(&self) -> TxnId {
        TxnId(self.oracle.last_allocated().0)
    }

    fn last_allocated_timestamp(&self) -> Timestamp {
        self.oracle.last_allocated()
    }

    fn timestamp_oracle_stats(&self) -> TimestampOracleStats {
        self.oracle.stats()
    }
}

/// In-memory transaction manager for local Milestone 2 execution.
///
/// Exactly one `LocalTransactionManager` must be shared by every `SqlSession`
/// operating against the same `LocalExecutor`. Constructing one manager per
/// session would reuse transaction IDs and timestamps.
///
/// This allocator is intentionally not durable. The future metadata timestamp
/// authority will replace it without changing the `TransactionManager`
/// interface used by SQL sessions.
/// later milestone.
#[derive(Debug, Default)]
pub struct LocalTransactionManager {
    last_transaction_id: u64,
    last_timestamp: u64,
}

impl LocalTransactionManager {
    /// Construct an empty local allocator.
    pub fn new() -> Self {
        Self::default()
    }

    /// initialize the local allocator from checked recovery floors
    ///
    /// Arguments are the first values available for allocation, not the last
    /// durable values. Internally the manager stores the preceding values so
    /// its existing allocation path returns these floors exactly once
    pub fn from_recovered_floors(
        next_transaction_id: TxnId,
        next_timestamp: Timestamp,
    ) -> Result<Self> {
        let last_transaction_id = next_transaction_id.0.checked_sub(1).ok_or_else(|| {
            Error::Configuration("recovered next transaction ID must be nonzero".to_string())
        })?;

        let last_timestamp = next_timestamp.0.checked_sub(1).ok_or_else(|| {
            Error::Configuration("recovered next timestamp must be nonzero".to_string())
        })?;

        Ok(Self {
            last_transaction_id,
            last_timestamp,
        })
    }

    /// Return the most recently allocated transaction identifier.
    ///
    /// Zero means no transaction has been allocated yet.
    pub fn last_allocated_transaction_id(&self) -> TxnId {
        TxnId(self.last_transaction_id)
    }

    /// Return the most recently allocated MVCC timestamp.
    ///
    /// Zero means no timestamp has been allocated yet.
    pub fn last_allocated_timestamp(&self) -> Timestamp {
        Timestamp(self.last_timestamp)
    }

    /// Raise local allocator floors after observing a replicated commit.
    ///
    /// This prevents a follower promoted to leader from allocating identifiers
    /// or MVCC timestamps below state it learned through Raft.
    pub fn observe_replicated_high_water(&mut self, transaction_id: TxnId, timestamp: Timestamp) {
        self.last_transaction_id = self.last_transaction_id.max(transaction_id.0);
        self.last_timestamp = self.last_timestamp.max(timestamp.0);
    }

    fn allocate_timestamp_after(&mut self, minimum_exclusive: Timestamp) -> Result<Timestamp> {
        let allocation_floor = self.last_timestamp.max(minimum_exclusive.0);

        let next = allocation_floor.checked_add(1).ok_or_else(|| {
            Error::Configuration(
                "local timestamp allocator has exhausted the u64 timestamp space".to_string(),
            )
        })?;

        self.last_timestamp = next;
        Ok(Timestamp(next))
    }
}

/// commit timestamp source invoked inside the serialized commit boundary
///
/// production sessions use a shared `TransactionManager`. A pre-finalized
/// timestamp remains supported for lower-level deterministic executor tests and
/// future recovery application, but it is not consulted until MVCC preflight
/// has completed
pub trait CommitTimestampAllocator {
    fn finalize_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp>;
}

impl<M> CommitTimestampAllocator for &mut M
where
    M: TransactionManager + ?Sized,
{
    fn finalize_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp> {
        self.allocate_commit_timestamp(start_ts)
    }
}

impl CommitTimestampAllocator for Timestamp {
    fn finalize_commit_timestamp(&mut self, _start_ts: Timestamp) -> Result<Timestamp> {
        Ok(*self)
    }
}

impl TransactionManager for LocalTransactionManager {
    fn begin_transaction(&mut self) -> Result<Transaction> {
        // Calculate the complete next state before publishing either counter.
        // If any validation or allocation step fails, both counters remain
        // unchanged.
        let next_transaction_id = self.last_transaction_id.checked_add(1).ok_or_else(|| {
            Error::Configuration(
                "local transaction ID allocator has exhausted the u64 ID space".to_string(),
            )
        })?;

        let next_timestamp = self.last_timestamp.checked_add(1).ok_or_else(|| {
            Error::Configuration(
                "local timestamp allocator has exhausted the u64 timestamp space".to_string(),
            )
        })?;

        let transaction = Transaction::new(TxnId(next_transaction_id), Timestamp(next_timestamp))?;

        self.last_transaction_id = next_transaction_id;
        self.last_timestamp = next_timestamp;

        Ok(transaction)
    }

    fn allocate_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp> {
        self.allocate_timestamp_after(start_ts)
    }

    fn observe_replicated_high_water(&mut self, transaction_id: TxnId, timestamp: Timestamp) {
        LocalTransactionManager::observe_replicated_high_water(self, transaction_id, timestamp);
    }

    fn last_allocated_transaction_id(&self) -> TxnId {
        LocalTransactionManager::last_allocated_transaction_id(self)
    }

    fn last_allocated_timestamp(&self) -> Timestamp {
        LocalTransactionManager::last_allocated_timestamp(self)
    }

    fn timestamp_oracle_stats(&self) -> TimestampOracleStats {
        TimestampOracleStats::default()
    }
}

impl CommitTimestampAllocator for LocalTransactionManager {
    fn finalize_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp> {
        self.allocate_commit_timestamp(start_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, thread};

    #[derive(Debug)]
    struct FakeReservationProvider {
        durable_frontier: Timestamp,
        requests: Vec<Timestamp>,
    }

    impl Default for FakeReservationProvider {
        fn default() -> Self {
            Self {
                durable_frontier: Timestamp(0),
                requests: Vec::new(),
            }
        }
    }

    impl TimestampReservationProvider for FakeReservationProvider {
        fn reserve_timestamps(
            &mut self,
            requested_until: Timestamp,
        ) -> Result<TimestampReservation> {
            let reserved_from = self
                .durable_frontier
                .0
                .checked_add(1)
                .ok_or_else(|| Error::Configuration("fake frontier exhausted".to_string()))?;
            self.requests.push(requested_until);
            if requested_until <= self.durable_frontier {
                return Err(Error::Configuration(
                    "fake reservation provider regressed the durable frontier".to_string(),
                ));
            }
            self.durable_frontier = requested_until;
            Ok(TimestampReservation {
                reserved_from: Timestamp(reserved_from),
                reserved_until: self.durable_frontier,
            })
        }
    }

    #[test]
    fn concurrent_timestamp_oracle_allocates_unique_values_without_serializing_fast_path() {
        let oracle = Arc::new(
            ConcurrentTimestampOracle::new(FakeReservationProvider::default(), 4_096, 1_024)
                .unwrap(),
        );
        let mut workers = Vec::new();

        for _ in 0..8 {
            let oracle = Arc::clone(&oracle);
            workers.push(thread::spawn(move || {
                (0..1_000)
                    .map(|_| oracle.allocate_timestamp().unwrap())
                    .collect::<Vec<_>>()
            }));
        }

        let mut timestamps = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        timestamps.sort_unstable();

        assert_eq!(timestamps.len(), 8_000);
        assert_eq!(timestamps[0], Timestamp(1));
        assert_eq!(timestamps[7_999], Timestamp(8_000));
        assert!(timestamps.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(oracle.stats().allocations, 8_000);
    }

    #[test]
    fn reserved_manager_prefetches_before_exhaustion_and_preserves_commit_ordering() {
        let provider = FakeReservationProvider::default();
        let mut manager = ReservedTimestampTransactionManager::new(provider, 10, 3).unwrap();

        let first = manager.begin_transaction().unwrap();

        assert_eq!(first.start_ts(), Timestamp(1));
        assert_eq!(manager.oracle().reserved_until(), Timestamp(10));
        assert_eq!(manager.oracle().last_allocated(), Timestamp(1));

        let commit = manager.allocate_commit_timestamp(Timestamp(50)).unwrap();

        assert_eq!(commit, Timestamp(51));
        assert!(commit > first.start_ts());
        assert!(manager.oracle().reserved_until() >= Timestamp(51));
        assert!(
            manager
                .oracle()
                .provider()
                .requests
                .iter()
                .any(|requested| *requested >= Timestamp(51))
        );
    }

    #[test]
    fn oracle_restart_starts_strictly_after_durable_frontier_and_skips_unused_values() {
        let provider = FakeReservationProvider {
            durable_frontier: Timestamp(100),
            requests: Vec::new(),
        };
        let mut oracle =
            TimestampOracle::from_durable_frontier(provider, Timestamp(100), 10, 2).unwrap();

        assert_eq!(oracle.allocate_timestamp().unwrap(), Timestamp(101));
        oracle.reset_after_failover(Timestamp(200)).unwrap();
        oracle.provider_mut().durable_frontier = Timestamp(200);

        assert_eq!(oracle.allocate_timestamp().unwrap(), Timestamp(201));
        assert!(oracle.last_allocated() > Timestamp(200));
    }

    #[test]
    fn local_manager_allocates_monotonic_transaction_metadata() {
        let mut manager = LocalTransactionManager::new();

        let first = manager.begin_transaction().unwrap();

        assert_eq!(first.id(), TxnId(1));
        assert_eq!(first.start_ts(), Timestamp(1));

        let first_commit = manager.allocate_commit_timestamp(first.start_ts()).unwrap();

        assert_eq!(first_commit, Timestamp(2));

        let second = manager.begin_transaction().unwrap();

        assert_eq!(second.id(), TxnId(2));
        assert_eq!(second.start_ts(), Timestamp(3));
    }

    #[test]
    fn commit_timestamp_is_always_newer_than_start_timestamp() {
        let mut manager = LocalTransactionManager::new();

        let commit_ts = manager.allocate_commit_timestamp(Timestamp(100)).unwrap();

        assert_eq!(commit_ts, Timestamp(101));
        assert!(commit_ts > Timestamp(100));
    }

    #[test]
    fn allocator_diagnostics_report_current_state() {
        let mut manager = LocalTransactionManager::new();

        assert_eq!(manager.last_allocated_transaction_id(), TxnId(0));
        assert_eq!(manager.last_allocated_timestamp(), Timestamp(0));

        let transaction = manager.begin_transaction().unwrap();

        assert_eq!(manager.last_allocated_transaction_id(), transaction.id());
        assert_eq!(manager.last_allocated_timestamp(), transaction.start_ts());
    }

    #[test]
    fn timestamp_exhaustion_returns_an_error_without_wrapping() {
        let mut manager = LocalTransactionManager::new();

        let error = manager
            .allocate_commit_timestamp(Timestamp(u64::MAX))
            .unwrap_err();

        assert!(matches!(error, Error::Configuration(_)));
        assert_eq!(manager.last_allocated_timestamp(), Timestamp(0));
    }

    #[test]
    fn failed_begin_leaves_allocator_state_unchanged() {
        let mut manager = LocalTransactionManager {
            last_transaction_id: 41,
            last_timestamp: u64::MAX,
        };

        let error = manager.begin_transaction().unwrap_err();

        assert!(matches!(error, Error::Configuration(_)));
        assert_eq!(manager.last_allocated_transaction_id(), TxnId(41));
        assert_eq!(manager.last_allocated_timestamp(), Timestamp(u64::MAX));
    }
}
/// Thread-safe timestamp allocation for the replicated SQL fast path.
///
/// The atomic cursor is independent from the durable refill lock. Ordinary
/// allocations therefore do not wait behind metadata Raft reservation work.
#[derive(Debug)]
pub struct ConcurrentTimestampOracle<P> {
    provider: Mutex<P>,
    refill_lock: Mutex<()>,
    next_timestamp: std::sync::atomic::AtomicU64,
    last_allocated: std::sync::atomic::AtomicU64,
    reserved_until: std::sync::atomic::AtomicU64,
    reservation_size: u64,
    prefetch_threshold: u64,
    allocations: std::sync::atomic::AtomicU64,
    reservations: std::sync::atomic::AtomicU64,
    allocation_latency_nanos: std::sync::atomic::AtomicU64,
    reservation_latency_nanos: std::sync::atomic::AtomicU64,
}

impl<P> ConcurrentTimestampOracle<P>
where
    P: TimestampReservationProvider,
{
    pub fn new(provider: P, reservation_size: u64, prefetch_threshold: u64) -> Result<Self> {
        validate_reservation_policy(reservation_size, prefetch_threshold)?;
        let oracle = Self {
            provider: Mutex::new(provider),
            refill_lock: Mutex::new(()),
            next_timestamp: std::sync::atomic::AtomicU64::new(1),
            last_allocated: std::sync::atomic::AtomicU64::new(0),
            reserved_until: std::sync::atomic::AtomicU64::new(0),
            reservation_size,
            prefetch_threshold,
            allocations: std::sync::atomic::AtomicU64::new(0),
            reservations: std::sync::atomic::AtomicU64::new(0),
            allocation_latency_nanos: std::sync::atomic::AtomicU64::new(0),
            reservation_latency_nanos: std::sync::atomic::AtomicU64::new(0),
        };
        oracle.ensure_reserved_through(Timestamp(1))?;
        Ok(oracle)
    }

    pub fn from_durable_frontier(
        provider: P,
        durable_frontier: Timestamp,
        reservation_size: u64,
        prefetch_threshold: u64,
    ) -> Result<Self> {
        validate_reservation_policy(reservation_size, prefetch_threshold)?;
        let next_timestamp = durable_frontier.0.checked_add(1).ok_or_else(|| {
            Error::Configuration("timestamp oracle has exhausted the u64 timestamp space".into())
        })?;
        Ok(Self {
            provider: Mutex::new(provider),
            refill_lock: Mutex::new(()),
            next_timestamp: std::sync::atomic::AtomicU64::new(next_timestamp),
            last_allocated: std::sync::atomic::AtomicU64::new(durable_frontier.0),
            reserved_until: std::sync::atomic::AtomicU64::new(durable_frontier.0),
            reservation_size,
            prefetch_threshold,
            allocations: std::sync::atomic::AtomicU64::new(0),
            reservations: std::sync::atomic::AtomicU64::new(0),
            allocation_latency_nanos: std::sync::atomic::AtomicU64::new(0),
            reservation_latency_nanos: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn allocate_timestamp(&self) -> Result<Timestamp> {
        let started = Instant::now();
        let result = loop {
            let next = self
                .next_timestamp
                .load(std::sync::atomic::Ordering::Acquire);
            if next == 0 {
                break Err(Error::Configuration(
                    "timestamp oracle generated the reserved zero timestamp".into(),
                ));
            }
            let reserved = self
                .reserved_until
                .load(std::sync::atomic::Ordering::Acquire);
            let remaining = reserved.saturating_sub(next).saturating_add(1);
            if next > reserved || remaining <= self.prefetch_threshold {
                let target = if next > reserved {
                    next
                } else {
                    reserved.checked_add(1).ok_or_else(|| {
                        Error::Configuration(
                            "timestamp oracle has exhausted the u64 timestamp space".into(),
                        )
                    })?
                };
                self.ensure_reserved_through(Timestamp(target))?;
                continue;
            }
            let next_after = next.checked_add(1).ok_or_else(|| {
                Error::Configuration(
                    "timestamp oracle has exhausted the u64 timestamp space".into(),
                )
            })?;
            if self
                .next_timestamp
                .compare_exchange(
                    next,
                    next_after,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                )
                .is_ok()
            {
                self.last_allocated
                    .fetch_max(next, std::sync::atomic::Ordering::AcqRel);
                break Ok(Timestamp(next));
            }
        };
        if result.is_ok() {
            self.allocations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.allocation_latency_nanos.fetch_add(
                started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        result
    }

    pub fn allocate_commit_timestamp(&self, start_ts: Timestamp) -> Result<Timestamp> {
        if start_ts.0 == 0 {
            return Err(Error::InvalidArgument(
                "transaction start timestamp 0 is reserved".into(),
            ));
        }
        let minimum = start_ts.0.checked_add(1).ok_or_else(|| {
            Error::Configuration("commit timestamp cannot be newer than u64::MAX".into())
        })?;
        loop {
            let current = self
                .next_timestamp
                .load(std::sync::atomic::Ordering::Acquire);
            if current == 0 || current >= minimum {
                break;
            }
            if self
                .next_timestamp
                .compare_exchange(
                    current,
                    minimum,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                )
                .is_ok()
            {
                break;
            }
        }
        self.allocate_timestamp()
    }

    /// Discard the current local interval after failover and restart above
    /// the durable metadata frontier. The refill lock serializes this
    /// transition with any in-flight reservation proposal.
    pub fn reset_after_failover(&self, durable_frontier: Timestamp) -> Result<()> {
        let _guard = self
            .refill_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self
            .reserved_until
            .load(std::sync::atomic::Ordering::Acquire);
        if durable_frontier.0 < current {
            return Err(Error::Configuration(format!(
                "failover frontier {} is below the local durable reservation {}",
                durable_frontier.0, current
            )));
        }
        let next = durable_frontier.0.checked_add(1).ok_or_else(|| {
            Error::Configuration("timestamp oracle has exhausted the u64 timestamp space".into())
        })?;
        self.next_timestamp
            .store(next, std::sync::atomic::Ordering::Release);
        self.last_allocated
            .store(durable_frontier.0, std::sync::atomic::Ordering::Release);
        self.reserved_until
            .store(durable_frontier.0, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub fn observe_replicated_high_water(&self, timestamp: Timestamp) {
        self.last_allocated
            .fetch_max(timestamp.0, std::sync::atomic::Ordering::AcqRel);
        self.reserved_until
            .fetch_max(timestamp.0, std::sync::atomic::Ordering::AcqRel);
        if timestamp.0 == u64::MAX {
            self.next_timestamp
                .store(0, std::sync::atomic::Ordering::Release);
        } else {
            self.next_timestamp
                .fetch_max(timestamp.0 + 1, std::sync::atomic::Ordering::AcqRel);
        }
    }

    pub fn last_allocated(&self) -> Timestamp {
        Timestamp(
            self.last_allocated
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    pub fn reserved_until(&self) -> Timestamp {
        Timestamp(
            self.reserved_until
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    pub fn stats(&self) -> TimestampOracleStats {
        TimestampOracleStats {
            allocations: self.allocations.load(std::sync::atomic::Ordering::Acquire),
            reservations: self.reservations.load(std::sync::atomic::Ordering::Acquire),
            allocation_latency_nanos: self
                .allocation_latency_nanos
                .load(std::sync::atomic::Ordering::Acquire),
            reservation_latency_nanos: self
                .reservation_latency_nanos
                .load(std::sync::atomic::Ordering::Acquire),
            last_allocated: self.last_allocated(),
            reserved_until: self.reserved_until(),
        }
    }

    fn ensure_reserved_through(&self, target: Timestamp) -> Result<()> {
        let _guard = self
            .refill_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self
            .reserved_until
            .load(std::sync::atomic::Ordering::Acquire);
        if target.0 <= current {
            return Ok(());
        }
        let requested_until = current
            .checked_add(self.reservation_size.max(target.0 - current))
            .ok_or_else(|| {
                Error::Configuration(
                    "timestamp oracle has exhausted the u64 timestamp space".into(),
                )
            })?;
        let started = Instant::now();
        let reservation = self
            .provider
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .reserve_timestamps(Timestamp(requested_until))?;
        if reservation.reserved_from <= Timestamp(current)
            || reservation.reserved_until < Timestamp(requested_until)
            || reservation.reserved_from > reservation.reserved_until
        {
            return Err(Error::Configuration(format!(
                "timestamp reservation provider returned invalid interval {}..={} for requested {}",
                reservation.reserved_from.0, reservation.reserved_until.0, requested_until
            )));
        }
        self.reserved_until.store(
            reservation.reserved_until.0,
            std::sync::atomic::Ordering::Release,
        );
        self.reservations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.reservation_latency_nanos.fetch_add(
            started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(())
    }
}
/// Shared allocator operations used by server request handlers without a
/// process-wide mutex around the timestamp fast path.
pub trait SharedTransactionManager: Send + Sync {
    fn begin_transaction_shared(&self) -> Result<Transaction>;
    fn allocate_commit_timestamp_shared(&self, start_ts: Timestamp) -> Result<Timestamp>;
    fn observe_replicated_high_water_shared(&self, transaction_id: TxnId, timestamp: Timestamp);
    fn last_allocated_transaction_id_shared(&self) -> TxnId;
    fn last_allocated_timestamp_shared(&self) -> Timestamp;
    fn timestamp_oracle_stats_shared(&self) -> TimestampOracleStats;
}

#[derive(Debug)]
pub struct ConcurrentReservedTimestampTransactionManager<P> {
    oracle: ConcurrentTimestampOracle<P>,
}

impl<P> ConcurrentReservedTimestampTransactionManager<P>
where
    P: TimestampReservationProvider,
{
    pub fn from_durable_frontier(
        provider: P,
        durable_frontier: Timestamp,
        reservation_size: u64,
        prefetch_threshold: u64,
    ) -> Result<Self> {
        Ok(Self {
            oracle: ConcurrentTimestampOracle::from_durable_frontier(
                provider,
                durable_frontier,
                reservation_size,
                prefetch_threshold,
            )?,
        })
    }

    pub fn oracle(&self) -> &ConcurrentTimestampOracle<P> {
        &self.oracle
    }
}

impl<P> SharedTransactionManager for ConcurrentReservedTimestampTransactionManager<P>
where
    P: TimestampReservationProvider + Send,
{
    fn begin_transaction_shared(&self) -> Result<Transaction> {
        let start_ts = self.oracle.allocate_timestamp()?;
        Transaction::new(TxnId(start_ts.0), start_ts)
    }

    fn allocate_commit_timestamp_shared(&self, start_ts: Timestamp) -> Result<Timestamp> {
        self.oracle.allocate_commit_timestamp(start_ts)
    }

    fn observe_replicated_high_water_shared(&self, _transaction_id: TxnId, timestamp: Timestamp) {
        self.oracle.observe_replicated_high_water(timestamp);
    }

    fn last_allocated_transaction_id_shared(&self) -> TxnId {
        TxnId(self.oracle.last_allocated().0)
    }

    fn last_allocated_timestamp_shared(&self) -> Timestamp {
        self.oracle.last_allocated()
    }

    fn timestamp_oracle_stats_shared(&self) -> TimestampOracleStats {
        self.oracle.stats()
    }
}

#[derive(Clone)]
pub struct SharedTransactionManagerHandle<P> {
    inner: Arc<ConcurrentReservedTimestampTransactionManager<P>>,
}

impl<P> SharedTransactionManagerHandle<P> {
    pub fn new(inner: Arc<ConcurrentReservedTimestampTransactionManager<P>>) -> Self {
        Self { inner }
    }
}

impl<P> TransactionManager for SharedTransactionManagerHandle<P>
where
    P: TimestampReservationProvider + Send,
{
    fn begin_transaction(&mut self) -> Result<Transaction> {
        self.inner.begin_transaction_shared()
    }

    fn allocate_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp> {
        self.inner.allocate_commit_timestamp_shared(start_ts)
    }

    fn observe_replicated_high_water(&mut self, transaction_id: TxnId, timestamp: Timestamp) {
        self.inner
            .observe_replicated_high_water_shared(transaction_id, timestamp);
    }

    fn last_allocated_transaction_id(&self) -> TxnId {
        self.inner.last_allocated_transaction_id_shared()
    }

    fn last_allocated_timestamp(&self) -> Timestamp {
        self.inner.last_allocated_timestamp_shared()
    }

    fn timestamp_oracle_stats(&self) -> TimestampOracleStats {
        self.inner.timestamp_oracle_stats_shared()
    }
}
