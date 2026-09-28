//! Multi-version concurrency control over a pluggable ordered storage backend.
//!
//! the engine maintains the three logical maps used by RagnorDB's MVCC model:
//!
//! ```text
//! default/{row_key}/{start_ts} -> encoded row
//! lock/{row_key}               -> uncommitted lock record
//! write/{row_key}/{commit_ts}  -> committed write record
//! ```
//!
//! `default` stores row payloads by transaction start timestamp
//! `write` stores the commit history used by snapshot readers.
//! `put` record points back to its payload in `default`;
//! `Delete` is a tombstone; `Rollback` prevents a delayed transaction
//! message from resurrecting an absorbed write
//!
//! For committed `Put` and `Delete` records, `write_ts` is the commit
//! timestamp and must be greater than the transaction start timestamp.
//!
//! A `Rollback` record has no independently allocated commit timestamp.
//! Consequently, it is stored at the aborted transaction's start timestamp,
//! and its `commit_timestamp` field also contains that start timestamp. The
//! field name is retained for compatibility with the existing shared codec.
//!
//! this commits buffered single tablet transaction directly after
//! validating the entire batch.
//! Distributed prewrite, terminal intent resolution, transaction status
//! records, Raft application, WAL durability, and garbage collection are
//! implemented at separate layer boundaries. Read-time resolution returns a
//! durable command plan to the tablet/Raft owner; it never mutates MVCC state
//! as a side effect of an ordinary read.
//!
//! the existing raft `WriteEntry` currently stores one `Value`,
//! while `Mutation::Put` stores a complete canonical encoded row
//! those representation must be aligned before `SingleShardCommit` is wired
//! into raft

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    ops::Bound::{self, Excluded, Included, Unbounded},
    sync::Arc,
};

use crate::{
    key::decode_row_key,
    lsm::{
        CommandGenerationMetadata, DEFAULT_TABLET_IMMUTABLE_HARD_COUNT,
        DEFAULT_TABLET_IMMUTABLE_SOFT_COUNT, MemoryClass, MemoryReservation, MemtablePressure,
        NodeMemtableBudget, RecoveryFrontier, memory::MemtableCharge,
    },
};
use prost::Message;

use ragnordb_common::{
    Error, Result,
    codec::{LockRecord, WriteKind, WriteRecord},
    encoding::decode_row,
    ids::{TableId, Timestamp, TxnId},
    proto::snapshot as snapshot_proto,
};

use crate::checkpoint::CapturedMvccState;

/// Owned range boundaries used for canonical encoded row-key scans.
type EncodedScanBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// Maximum physical records or keys fetched by one MVCC cursor page.
const MVCC_WRITE_CURSOR_PAGE_SIZE: usize = 128;
const MVCC_ROW_CURSOR_PAGE_SIZE: usize = 128;

fn max_lower_bound(left: Bound<Timestamp>, right: Bound<Timestamp>) -> Bound<Timestamp> {
    match (left, right) {
        (Unbounded, bound) | (bound, Unbounded) => bound,
        (Included(left), Included(right)) => Included(left.max(right)),
        (Excluded(left), Excluded(right)) => Excluded(left.max(right)),
        (Included(left), Excluded(right)) | (Excluded(right), Included(left)) => {
            if left > right {
                Included(left)
            } else if right > left {
                Excluded(right)
            } else {
                Excluded(left)
            }
        }
    }
}

fn min_upper_bound(left: Bound<Timestamp>, right: Bound<Timestamp>) -> Bound<Timestamp> {
    match (left, right) {
        (Unbounded, bound) | (bound, Unbounded) => bound,
        (Included(left), Included(right)) => Included(left.min(right)),
        (Excluded(left), Excluded(right)) => Excluded(left.min(right)),
        (Included(left), Excluded(right)) | (Excluded(right), Included(left)) => {
            if left < right {
                Included(left)
            } else if right < left {
                Excluded(right)
            } else {
                Excluded(left)
            }
        }
    }
}

fn timestamp_range_is_empty(lower: &Bound<Timestamp>, upper: &Bound<Timestamp>) -> bool {
    match (lower, upper) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(lower), Bound::Included(upper)) => lower > upper,
        (Bound::Included(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper))
        | (Bound::Excluded(lower), Bound::Excluded(upper)) => lower >= upper,
    }
}

/// A transaction local mutation waiting to be committed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    /// insert or replace a row using its canonical encoded representation
    Put(Vec<u8>),

    /// make the row absent for snapshots at or after the commit timestamp
    Delete,
}

impl Mutation {
    fn write_kind(&self) -> WriteKind {
        match self {
            Self::Put(_) => WriteKind::Put,
            Self::Delete => WriteKind::Delete,
        }
    }
}

/// Distribution of retained versions or write records across keys.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VersionChainStats {
    /// Number of keys that currently retain exactly one version or record.
    pub one: usize,
    /// Number of keys with two through four versions or records.
    pub two_to_four: usize,
    /// Number of keys with five through sixteen versions or records.
    pub five_to_sixteen: usize,
    /// Number of keys with more than sixteen versions or records.
    pub more_than_sixteen: usize,
    /// Largest version or record chain retained for any one key.
    pub max_per_key: usize,
}

fn version_chain_stats(lengths: impl Iterator<Item = usize>) -> VersionChainStats {
    lengths.fold(VersionChainStats::default(), |mut stats, length| {
        stats.max_per_key = stats.max_per_key.max(length);
        match length {
            0 => {}
            1 => stats.one += 1,
            2..=4 => stats.two_to_four += 1,
            5..=16 => stats.five_to_sixteen += 1,
            _ => stats.more_than_sixteen += 1,
        }
        stats
    })
}

/// Diagnostic counters for the in-memory MVCC engine.
///
/// These values describe logical retained state. Computing the totals and
/// chain distributions walks the version maps, so callers should sample them
/// for diagnostics rather than on every transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MvccStats {
    /// number of distinct row keys with at least one default value
    pub default_keys: usize,

    /// total number of default value version
    pub default_versions: usize,

    /// Distribution of retained default-value versions across keys.
    pub default_version_chains: VersionChainStats,

    /// number of unresolved locks
    pub locks: usize,

    /// number of unresolved locks
    pub write_keys: usize,

    /// total number of Put, Delete and Rollback write records.
    pub write_records: usize,

    /// Distribution of retained write records across keys.
    pub write_record_chains: VersionChainStats,
}

/// One bounded, ordered MVCC scan response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccScanPage {
    /// Rows visible at the requested snapshot, ordered by encoded row key.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,

    /// Whether another visible row remains after the final returned key.
    pub has_more: bool,
}

/// One bounded, ordered scan response over unresolved transaction intents.
#[derive(Debug, Clone, PartialEq)]
pub struct IntentScanPage {
    /// Canonical row keys paired with the validated lock records they own.
    pub locks: Vec<(Vec<u8>, LockRecord)>,

    /// Whether another intent remains after the final returned key.
    pub has_more: bool,
}

/// Identifies the logical record family whose distinct row keys are scanned.
///
/// The family is a logical API value. It does not prescribe whether the
/// persistent implementation uses one ordered keyspace or separate trees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MvccKeyFamily {
    /// Committed write records, which provide the row-key scan domain.
    Writes,
    /// Current transaction intents, including keys with no committed history.
    Locks,
}

/// Direction used to traverse one row's ordered write history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MvccCursorDirection {
    /// Visit the oldest matching timestamp first.
    Forward,
    /// Visit the newest matching timestamp first.
    Reverse,
}

/// A bounded page of distinct row keys from one logical MVCC family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccKeyPage {
    /// Row keys in canonical byte order.
    pub keys: Vec<Vec<u8>>,
    /// Whether the backend has another matching key after this page.
    pub has_more: bool,
}

/// Maintains an independent bounded cursor for one logical key family while
/// scans merge keys from writes and locks in canonical row-key order.
struct MvccFamilyKeyCursor {
    family: MvccKeyFamily,
    resume_after: Option<Vec<u8>>,
    buffered: VecDeque<Vec<u8>>,
    exhausted: bool,
}

impl MvccFamilyKeyCursor {
    fn new(family: MvccKeyFamily, resume_after: Option<&[u8]>) -> Self {
        Self {
            family,
            resume_after: resume_after.map(ToOwned::to_owned),
            buffered: VecDeque::new(),
            exhausted: false,
        }
    }

    /// Refill only after the current bounded page has been consumed. Advancing
    /// one family's physical cursor never changes the other family's position.
    fn fill<R: MvccReadGeneration + ?Sized>(
        &mut self,
        generation: &R,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<()> {
        if self.exhausted || !self.buffered.is_empty() {
            return Ok(());
        }

        let page = generation.key_page(
            self.family,
            start,
            end,
            self.resume_after.as_deref(),
            MVCC_ROW_CURSOR_PAGE_SIZE,
        )?;

        if page.keys.is_empty() {
            if page.has_more {
                return Err(Error::CorruptData(
                    "MVCC key cursor returned an empty page with has_more=true".to_string(),
                ));
            }

            self.exhausted = true;
            return Ok(());
        }

        self.resume_after = page.keys.last().cloned();
        self.exhausted = !page.has_more;
        self.buffered.extend(page.keys);

        Ok(())
    }
}

/// Return the next distinct key in the ordered union of the write and lock
/// families while retaining separate physical continuation positions.
fn next_candidate_key<R: MvccReadGeneration + ?Sized>(
    generation: &R,
    writes: &mut MvccFamilyKeyCursor,
    locks: &mut MvccFamilyKeyCursor,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Result<Option<Vec<u8>>> {
    writes.fill(generation, start, end)?;
    locks.fill(generation, start, end)?;

    let write_key = writes.buffered.front().cloned();
    let lock_key = locks.buffered.front().cloned();

    match (write_key, lock_key) {
        (None, None) => Ok(None),
        (Some(key), None) => {
            writes.buffered.pop_front();
            Ok(Some(key))
        }
        (None, Some(key)) => {
            locks.buffered.pop_front();
            Ok(Some(key))
        }
        (Some(write_key), Some(lock_key)) => match write_key.cmp(&lock_key) {
            std::cmp::Ordering::Less => {
                writes.buffered.pop_front();
                Ok(Some(write_key))
            }
            std::cmp::Ordering::Greater => {
                locks.buffered.pop_front();
                Ok(Some(lock_key))
            }
            std::cmp::Ordering::Equal => {
                writes.buffered.pop_front();
                locks.buffered.pop_front();
                Ok(Some(write_key))
            }
        },
    }
}

/// A bounded page from one row's committed write history.
#[derive(Debug, Clone, PartialEq)]
pub struct MvccWritePage {
    /// Timestamp and validated logical write-record pairs in requested order.
    pub writes: Vec<(Timestamp, WriteRecord)>,
    /// Whether another matching write record remains in the requested range.
    pub has_more: bool,
}

/// One checked change to the logical MVCC record set.
#[derive(Debug, Clone, PartialEq)]
pub enum MvccRecordEdit {
    /// Store a row payload under its transaction start timestamp.
    PutDefault {
        /// Canonical encoded row key.
        key: Vec<u8>,
        /// Transaction start timestamp that owns the payload.
        start_ts: Timestamp,
        /// Canonical encoded row bytes.
        row: Vec<u8>,
    },
    /// Remove a row payload when an intent is rolled back.
    DeleteDefault {
        /// Canonical encoded row key.
        key: Vec<u8>,
        /// Transaction start timestamp whose payload is removed.
        start_ts: Timestamp,
    },
    /// Install or replace one validated transaction lock.
    PutLock {
        /// Canonical encoded row key.
        key: Vec<u8>,
        /// Transaction lock record.
        lock: LockRecord,
    },
    /// Remove the lock resolved by a committed or rolled-back intent.
    DeleteLock {
        /// Canonical encoded row key.
        key: Vec<u8>,
    },
    /// Store a committed write or rollback witness.
    PutWrite {
        /// Canonical encoded row key.
        key: Vec<u8>,
        /// Timestamp used to order this record in the write family.
        write_ts: Timestamp,
        /// Logical write record.
        write: WriteRecord,
    },
}

/// An MVCC-only batch prepared by the shared transaction rules.
///
/// This does not yet represent the complete tablet `CommandDelta`: primary
/// status, retry outcomes, processed Raft position, and other command-owned
/// state are added at Stage 4.3. It gives backends one all-or-nothing boundary
/// for the row/default, lock, and write records handled by the MVCC engine.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MvccDelta {
    /// Ordered edits that become visible together when publication succeeds.
    pub edits: Vec<MvccRecordEdit>,
}

/// Read-only physical interface for one immutable MVCC generation.
///
/// Implementations expose logical records, not persistent byte encodings.
/// Cursor methods must enforce their row/record limits during traversal; they
/// must not collect an unbounded range and trim it afterward. Implementations
/// that can block on filesystem or cache-miss I/O belong behind the bounded
/// node storage service, never inline on an ownership reactor.
pub trait MvccReadGeneration {
    /// Look up one row payload by its logical family key.
    fn get_default(&self, key: &[u8], start_ts: Timestamp) -> Result<Option<Vec<u8>>>;

    /// Look up the current transaction intent for one row key.
    fn get_lock(&self, key: &[u8]) -> Result<Option<LockRecord>>;

    /// Look up one write record by row key and ordering timestamp.
    fn get_write(&self, key: &[u8], write_ts: Timestamp) -> Result<Option<WriteRecord>>;

    /// Traverse one row's write history through a bounded ordered cursor.
    fn write_page(
        &self,
        key: &[u8],
        lower: Bound<Timestamp>,
        upper: Bound<Timestamp>,
        resume_after: Option<Timestamp>,
        direction: MvccCursorDirection,
        max_records: usize,
    ) -> Result<MvccWritePage>;

    /// Traverse distinct row keys in a family through a bounded ordered cursor.
    fn key_page(
        &self,
        family: MvccKeyFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_keys: usize,
    ) -> Result<MvccKeyPage>;

    /// Traverse lock records in canonical row-key order under row and byte caps.
    fn lock_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_locks: usize,
        max_bytes: usize,
    ) -> Result<IntentScanPage>;

    /// Read the row version visible at `read_ts` using the shared MVCC rules.
    fn read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        validate_encoded_key_argument(key, "read key")?;
        read_visible_version(self, key, read_ts)
    }

    /// Return the lock that conflicts with a snapshot read, if any.
    fn intent_for_read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<LockRecord>> {
        intent_for_read(self, key, read_ts)
    }

    /// Scan unresolved intents in canonical key order using bounded cursors.
    fn scan_intent_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_locks: usize,
    ) -> Result<IntentScanPage> {
        scan_intent_page(self, start, end, resume_after, max_locks)
    }

    /// Inspect the bounded intent page that conflicts with a snapshot read.
    fn scan_conflicting_intents(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        read_ts: Timestamp,
        max_locks: usize,
        max_bytes: usize,
    ) -> Result<IntentScanPage> {
        scan_conflicting_intents(
            self,
            start,
            end,
            resume_after,
            read_ts,
            max_locks,
            max_bytes,
        )
    }

    /// Scan one bounded page of rows visible at `read_ts`.
    fn scan_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        read_ts: Timestamp,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<MvccScanPage> {
        scan_page_from(self, start, end, resume_after, read_ts, max_rows, max_bytes)
    }

    /// Collect all visible rows by repeatedly using the bounded scan contract.
    fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        read_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .scan_page(start, end, None, read_ts, usize::MAX, usize::MAX)?
            .rows)
    }

    /// Return the durable recovery boundary covered by this generation.
    fn recovery_frontier(&self) -> Result<Option<RecoveryFrontier>>;

    /// Export the legacy materialized snapshot image from this generation.
    ///
    /// This compatibility path is optional so a persistent backend is not
    /// required to decode and reserialize its complete immutable file set.
    /// Such a backend may return an explicit unsupported error until the
    /// file-based snapshot contract is introduced.
    fn export_snapshot(&self) -> Result<CapturedMvccState> {
        Err(Error::NotImplemented(
            "materialized MVCC snapshot export is not supported by this backend",
        ))
    }
}

/// Mutable physical backend consumed by the shared MVCC rules engine.
///
/// Its live read operations implement [`MvccReadGeneration`]. A pin returns
/// another read generation that stays stable while newer deltas are published.
pub trait MvccBackend: MvccReadGeneration {
    /// Immutable generation handle returned by [`Self::pin_generation`].
    type PinnedGeneration: MvccReadGeneration + Send + Sync + 'static;

    /// Publish all edits in one MVCC delta or leave the generation unchanged.
    fn publish_atomic(&mut self, delta: MvccDelta) -> Result<()>;

    /// Publish with retained proposal capacity, or use progress capacity when
    /// the entry arrives from a follower or recovery replay.
    fn publish_atomic_with_reservation(
        &mut self,
        delta: MvccDelta,
        reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        if reservation.is_some() {
            return Err(Error::NotImplemented(
                "this MVCC backend cannot consume a retained memtable reservation",
            ));
        }
        self.publish_atomic(delta)
    }

    /// Determine whether a complete tablet command needs the current active
    /// MVCC generation frozen before its edits are applied. The tablet owner
    /// makes this decision because it owns the matching command metadata.
    fn command_generation_requires_freeze(
        &self,
        _delta: &MvccDelta,
        _metadata: &CommandGenerationMetadata,
    ) -> Result<bool> {
        Ok(false)
    }

    /// Publish MVCC edits together with the complete command metadata. The
    /// storage publication owner plans and performs any boundary rollover.
    fn publish_command_generation_with_reservation(
        &mut self,
        _delta: MvccDelta,
        _metadata: CommandGenerationMetadata,
        _reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "complete tablet command-generation publication is not supported by this backend",
        ))
    }

    /// Attach complete metadata/frontier restored from a snapshot base.
    fn install_restored_command_metadata(
        &mut self,
        _metadata: CommandGenerationMetadata,
    ) -> Result<()> {
        Ok(())
    }

    /// Return current memtable pressure when this backend is budgeted.
    fn memtable_pressure(&self) -> Option<MemtablePressure> {
        None
    }

    /// Return the shared node budget when this backend is budgeted.
    fn node_memtable_budget(&self) -> Option<NodeMemtableBudget> {
        None
    }

    /// Pin a stable view for serving reads, snapshot export, and compaction.
    fn pin_generation(&self) -> Result<Self::PinnedGeneration>;

    /// Return logical state counters without exposing backend representation.
    fn stats(&self) -> MvccStats;

    /// Return identifier and timestamp maxima represented by stored records.
    fn allocator_high_water_marks(&self) -> (TxnId, Timestamp);
}

/// A stable physical generation with the shared logical MVCC read behavior.
///
/// Async readers retain this value for the duration of their read. A later
/// backend publication may expose a newer generation without changing the
/// records or recovery frontier observed through this view.
pub struct MvccReadView<G> {
    generation: G,
}

impl<G: MvccReadGeneration> MvccReadView<G> {
    fn new(generation: G) -> Self {
        Self { generation }
    }

    /// Read the row version visible at `read_ts` from this pinned generation.
    pub fn read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        self.generation.read(key, read_ts)
    }

    /// Return the lock that conflicts with a snapshot read, if any.
    pub fn intent_for_read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<LockRecord>> {
        self.generation.intent_for_read(key, read_ts)
    }

    /// Scan unresolved intents in canonical key order using an exclusive
    /// continuation key.
    pub fn scan_intent_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_locks: usize,
    ) -> Result<IntentScanPage> {
        self.generation
            .scan_intent_page(start, end, resume_after, max_locks)
    }

    /// Inspect a bounded page of intents that conflict with a foreground
    /// snapshot read.
    pub fn scan_conflicting_intents(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        read_ts: Timestamp,
        max_locks: usize,
        max_bytes: usize,
    ) -> Result<IntentScanPage> {
        self.generation.scan_conflicting_intents(
            start,
            end,
            resume_after,
            read_ts,
            max_locks,
            max_bytes,
        )
    }

    /// Scan one bounded page of rows visible at `read_ts`.
    pub fn scan_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        read_ts: Timestamp,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<MvccScanPage> {
        self.generation
            .scan_page(start, end, resume_after, read_ts, max_rows, max_bytes)
    }

    /// Return all rows in the range from this generation.
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        read_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.generation.scan(start, end, read_ts)
    }

    /// Return the recovery boundary covered by this same pinned generation.
    pub fn recovery_frontier(&self) -> Result<Option<RecoveryFrontier>> {
        self.generation.recovery_frontier()
    }

    /// Export checked snapshot state from this same pinned generation.
    pub fn capture_snapshot_state(&self) -> Result<CapturedMvccState> {
        self.generation.export_snapshot()
    }
}

/// Storage contract required by the transaction-aware tablet layer.
///
/// All keys passed to this trait must be complete canonical row-key encodings
/// produced by `ragnordb_storage::key::encode_row_key`.
pub trait MvccStorage {
    /// Pin the complete current MVCC generation for a coherent tablet-level
    /// read or snapshot. Reference backends may materialize an owned copy;
    /// persistent backends should return a lightweight immutable generation
    /// handle.
    fn pin_read_generation(&self) -> Result<Box<dyn MvccReadGeneration + Send + Sync>> {
        Err(Error::NotImplemented(
            "pinning a complete MVCC generation is not supported by this backend",
        ))
    }

    /// Read one physical Default record for complete command-delta validation.
    fn get_default_record(&self, _key: &[u8], _start_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        Err(Error::NotImplemented(
            "physical default-record lookup is not supported by this backend",
        ))
    }

    /// Read one physical Lock record for complete command-delta validation.
    fn get_lock_record(&self, _key: &[u8]) -> Result<Option<LockRecord>> {
        Err(Error::NotImplemented(
            "physical lock-record lookup is not supported by this backend",
        ))
    }

    /// Read one physical Write record for complete command-delta validation.
    fn get_write_record(&self, _key: &[u8], _write_ts: Timestamp) -> Result<Option<WriteRecord>> {
        Err(Error::NotImplemented(
            "physical write-record lookup is not supported by this backend",
        ))
    }

    /// Read the row version visible at `read_ts`.
    fn read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Vec<u8>>>;

    /// Return the lock that conflicts with a snapshot read, if any.
    ///
    /// This is a read-only inspection boundary for intent resolution. The
    /// caller must consult the authoritative transaction status and submit a
    /// replicated resolve command; it must not mutate the backend directly
    /// from this inspection method.
    fn intent_for_read(&self, _key: &[u8], _read_ts: Timestamp) -> Result<Option<LockRecord>> {
        Err(Error::NotImplemented(
            "intent inspection is not supported by this MVCC backend",
        ))
    }

    /// Scan unresolved intents in canonical key order using an exclusive
    /// continuation key.
    ///
    /// The cleaner uses this read-only boundary to bound work per background
    /// pass. It must never infer expiry from the local lock age; the caller
    /// still has to consult the authoritative transaction-status record before
    /// dispatching a replicated resolution command.
    fn scan_intent_page(
        &self,
        _start: Option<&[u8]>,
        _end: Option<&[u8]>,
        _resume_after: Option<&[u8]>,
        _max_locks: usize,
    ) -> Result<IntentScanPage> {
        Err(Error::NotImplemented(
            "intent scanning is not supported by this MVCC backend",
        ))
    }

    /// Inspect a bounded page of intents that conflict with a foreground
    /// snapshot read. The tablet owner invokes this before returning visible
    /// scan rows so lock-only inserts and old committed values cannot hide a
    /// read-conflicting intent.
    fn scan_conflicting_intents(
        &self,
        _start: Option<&[u8]>,
        _end: Option<&[u8]>,
        _resume_after: Option<&[u8]>,
        _read_ts: Timestamp,
        _max_locks: usize,
        _max_bytes: usize,
    ) -> Result<IntentScanPage> {
        Err(Error::NotImplemented(
            "foreground scan intent inspection is not supported by this MVCC backend",
        ))
    }

    /// Scan the half-open encoded-key range `[start, end)` at `read_ts`.
    ///
    /// Returned rows must be ordered by canonical encoded row key.
    fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        read_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Scan one bounded page using an exclusive continuation key.
    ///
    /// `max_bytes` counts encoded key bytes plus encoded row bytes. This method
    /// is required so a physical backend must enforce the page bound while
    /// traversing its ordered storage; a collecting `scan` fallback would let
    /// an LSM cache miss materialize an unbounded range on the owner path.
    fn scan_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        read_ts: Timestamp,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<MvccScanPage>;

    /// Atomically install one distributed transaction intent.
    ///
    /// An exact replay is idempotent. A conflicting lock, a newer committed write,
    /// or an existing rollback marker rejects the prewrite without changing either
    /// the default-value or lock map.
    ///
    /// Backends that have not implemented distributed intents fail explicitly.
    fn prewrite(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _key: &[u8],
        _mutation: &Mutation,
        _primary_key: &[u8],
        _ttl_ms: u64,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed prewrite is not supported by this MVCC backend",
        ))
    }

    /// Atomically install every intent owned by one tablet participant.
    fn prewrite_batch(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _mutations: &BTreeMap<Vec<u8>, Mutation>,
        _primary_key: &[u8],
        _ttl_ms: u64,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed prewrite batches are not supported by this MVCC backend",
        ))
    }

    /// Prepare a distributed prewrite without publishing any MVCC record.
    /// Replicated apply combines this with status, retry, and Raft metadata.
    fn prepare_prewrite_batch(
        &self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _mutations: &BTreeMap<Vec<u8>, Mutation>,
        _primary_key: &[u8],
        _ttl_ms: u64,
    ) -> Result<MvccDelta> {
        Err(Error::NotImplemented(
            "side-effect-free distributed prewrite preparation is not supported by this MVCC backend",
        ))
    }

    /// Prepare an atomic single-shard commit without publishing its records.
    fn prepare_commit_batch(
        &self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _commit_ts: Timestamp,
        _mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<MvccDelta> {
        Err(Error::NotImplemented(
            "side-effect-free commit preparation is not supported by this MVCC backend",
        ))
    }

    /// Prepare an atomic intent commit without removing any live intent.
    fn prepare_commit_intents_batch(
        &self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _commit_ts: Timestamp,
        _keys: &BTreeSet<Vec<u8>>,
    ) -> Result<MvccDelta> {
        Err(Error::NotImplemented(
            "side-effect-free intent commit preparation is not supported by this MVCC backend",
        ))
    }

    /// Prepare rollback witnesses and intent cleanup without publishing them.
    fn prepare_rollback_intents_batch(
        &self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _keys: &BTreeSet<Vec<u8>>,
    ) -> Result<MvccDelta> {
        Err(Error::NotImplemented(
            "side-effect-free intent rollback preparation is not supported by this MVCC backend",
        ))
    }

    /// Prepare against the base generation plus earlier edits staged by the
    /// same Raft entry. Implementations that cannot provide a sparse overlay
    /// must fail explicitly instead of validating against stale base state.
    fn prepare_prewrite_batch_with_overlay(
        &self,
        staged: &MvccDelta,
        txn_id: TxnId,
        start_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
        primary_key: &[u8],
        ttl_ms: u64,
    ) -> Result<MvccDelta> {
        if staged.edits.is_empty() {
            self.prepare_prewrite_batch(txn_id, start_ts, mutations, primary_key, ttl_ms)
        } else {
            Err(Error::NotImplemented(
                "MVCC backend does not support sparse command-batch overlays",
            ))
        }
    }

    /// Prepare a single-shard commit against the private effects of earlier
    /// subcommands in the same committed Raft entry.
    fn prepare_commit_batch_with_overlay(
        &self,
        staged: &MvccDelta,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<MvccDelta> {
        if staged.edits.is_empty() {
            self.prepare_commit_batch(txn_id, start_ts, commit_ts, mutations)
        } else {
            Err(Error::NotImplemented(
                "MVCC backend does not support sparse command-batch overlays",
            ))
        }
    }

    /// Prepare intent commit using locks, defaults, and writes staged by prior
    /// commands in this Raft entry.
    fn prepare_commit_intents_batch_with_overlay(
        &self,
        staged: &MvccDelta,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<MvccDelta> {
        if staged.edits.is_empty() {
            self.prepare_commit_intents_batch(txn_id, start_ts, commit_ts, keys)
        } else {
            Err(Error::NotImplemented(
                "MVCC backend does not support sparse command-batch overlays",
            ))
        }
    }

    /// Prepare rollback against the staged view, preserving sequential batch
    /// semantics without exposing intermediate edits.
    fn prepare_rollback_intents_batch_with_overlay(
        &self,
        staged: &MvccDelta,
        txn_id: TxnId,
        start_ts: Timestamp,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<MvccDelta> {
        if staged.edits.is_empty() {
            self.prepare_rollback_intents_batch(txn_id, start_ts, keys)
        } else {
            Err(Error::NotImplemented(
                "MVCC backend does not support sparse command-batch overlays",
            ))
        }
    }

    /// Publish one already prepared MVCC delta. Tablet command application
    /// uses this only after complete-delta validation succeeds.
    fn publish_mvcc_delta(&mut self, _delta: MvccDelta) -> Result<()> {
        Err(Error::NotImplemented(
            "prepared MVCC delta publication is not supported by this backend",
        ))
    }

    /// Publish using capacity retained before local Raft admission. Followers
    /// and recovery pass no reservation and reserve from committed-progress
    /// capacity at apply time.
    fn publish_mvcc_delta_with_reservation(
        &mut self,
        delta: MvccDelta,
        reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        if reservation.is_some() {
            return Err(Error::NotImplemented(
                "this MVCC backend cannot consume a retained memtable reservation",
            ));
        }
        self.publish_mvcc_delta(delta)
    }

    /// Ask the tablet owner whether applying this command requires freezing
    /// the currently active MVCC generation.
    fn command_generation_requires_freeze(
        &self,
        _delta: &MvccDelta,
        _metadata: &CommandGenerationMetadata,
    ) -> Result<bool> {
        Err(Error::NotImplemented(
            "complete tablet generation rollover planning is not supported by this backend",
        ))
    }

    /// Publish one complete command generation, including any rollover.
    fn publish_command_generation_with_reservation(
        &mut self,
        _delta: MvccDelta,
        _metadata: CommandGenerationMetadata,
        _reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "complete tablet command-generation publication is not supported by this backend",
        ))
    }

    /// Attach complete metadata/frontier restored from a snapshot base.
    fn install_restored_command_metadata(
        &mut self,
        _metadata: CommandGenerationMetadata,
    ) -> Result<()> {
        Ok(())
    }

    /// Current managed-memory pressure for the tablet, if memtable-backed.
    fn memtable_pressure(&self) -> Option<MemtablePressure> {
        None
    }

    /// Shared node budget for this storage, if memtable-backed.
    fn node_memtable_budget(&self) -> Option<NodeMemtableBudget> {
        None
    }

    /// commit one previously installed distributed transaction intent
    ///
    /// an exact replay succeeds without creating a second write version. A
    /// missing, conflicting, or rolled-back intent fails without mutation
    fn commit_intent(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _commit_ts: Timestamp,
        _key: &[u8],
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed intent commit is not supported by this MVCC backend",
        ))
    }

    /// Atomically commit all participant intents or leave all keys unchanged.
    fn commit_intents_batch(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _commit_ts: Timestamp,
        _keys: &BTreeSet<Vec<u8>>,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed intent commit batches are not supported by this MVCC backend",
        ))
    }

    /// roll back one distributed transaction intent and persist its tombstone
    ///
    /// the rollback marker prevents a delayed prewrite or commit from
    /// resurrecting an aborted transaction. Exact replays are idempotent
    fn rollback_intent(&mut self, _txn_id: TxnId, _start_ts: Timestamp, _key: &[u8]) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed intent rollback is not supported by this MVCC backend",
        ))
    }

    /// Atomically roll back all participant intents or leave all keys unchanged.
    fn rollback_intents_batch(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _keys: &BTreeSet<Vec<u8>>,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed intent rollback batches are not supported by this MVCC backend",
        ))
    }

    /// Atomically validate and commit a transaction's complete mutation set.
    ///
    /// No mutation may be applied when validation of any key fails.
    fn commit_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<usize>;

    /// validate a transactions complete mutatin set without changing MVCC state
    ///
    /// this boundary intentionally does not accept a commit timestamp. conflict
    /// validation must finish before the durable commit coordinator allocates
    /// the final visibility timestamp and appends the transction to WAL
    ///
    /// an empty mutation set is valid at this generic storage boundary
    /// the tablet or commit coordinator rejects empty write commits while
    /// handling read only transaction without entering the durable write path
    fn validate_commit_batch(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<()>;

    /// Return current diagnostic counters.
    fn stats(&self) -> MvccStats;
}

/// Lazy active and immutable memtable implementation of the MVCC families.
///
/// The shared [`MvccEngine`] owns transaction semantics; this backend owns the
/// tablet-local generation queue, tombstones, and node-budget reservations.
///
/// Production retirement is intentionally unavailable until durable generation
/// publication exists. A nonblocking sink handoff alone does not authorize data
/// removal from serving state.
///
/// ```compile_fail
/// use ragnordb_storage::lsm::NodeMemtableBudget;
/// use ragnordb_storage::mvcc::MemtableMvcc;
///
/// let budget = NodeMemtableBudget::new_with_progress_reserve(4096, 1024).unwrap();
/// let mut storage = MemtableMvcc::with_memtable_budget(budget, 300).unwrap();
/// storage.retire_immutable_generation(1).unwrap();
/// ```
#[derive(Debug, Clone, Default)]
pub struct MemtableMvccBackend {
    /// `row_key -> start_ts -> encoded row`.
    default: BTreeMap<Vec<u8>, BTreeMap<Timestamp, Vec<u8>>>,

    /// `row_key -> unresolved lock`.
    ///
    /// Locks participate in reads and commit validation. Public distributed
    /// lock creation and resolution belong to Milestone 6.
    locks: BTreeMap<Vec<u8>, LockRecord>,

    /// `row_key -> write_ts -> write record`.
    writes: BTreeMap<Vec<u8>, BTreeMap<Timestamp, WriteRecord>>,

    /// Active-generation tombstones prevent deleted immutable records from
    /// becoming visible again when reads traverse older generations.
    default_tombstones: BTreeSet<(Vec<u8>, Timestamp)>,
    lock_tombstones: BTreeSet<Vec<u8>>,

    /// Read-only snapshot-restored base, kept outside the mutable limit and
    /// immutable flush-debt queue until durable storage replaces it.
    restored_base: Option<ImmutableMemtableGeneration>,
    /// Frozen generations remain serving and charged until explicit retirement.
    immutable_memtables: VecDeque<Arc<ImmutableMemtableGeneration>>,
    /// Generations accepted by the nonblocking sink, but still owned by serving state.
    handed_off_immutables: BTreeSet<u64>,
    /// Optional nonblocking Stage-4.5 handoff endpoint.
    immutable_sink: Option<Arc<dyn ImmutableMemtableSink>>,
    /// Most recent permanent sink failure; queue-full backpressure is not retained.
    last_flush_handoff_error: Option<FlushHandoffError>,
    /// Monotonic identity assigned before each successful immutable freeze.
    next_immutable_generation_id: u64,
    /// Ordered command metadata belonging to the active MVCC generation.
    active_command_metadata: Option<CommandGenerationMetadata>,
    /// Metadata for the command currently being published through the tablet
    /// owner. It is consumed only after the MVCC edits have passed validation.
    pending_command_metadata: Option<CommandGenerationMetadata>,
    /// Rollover decision made by the complete tablet-generation owner.
    pending_freeze_decision: Option<bool>,
    /// Once this backend participates in replicated tablet publication, reject
    /// later MVCC-only edits that could separate rows from command metadata.
    command_generation_mode: bool,
    immutable_memtable_bytes: usize,
    immutable_memtable_soft_limit_bytes: usize,
    immutable_memtable_limit_bytes: usize,
    immutable_memtable_soft_count_limit: usize,
    immutable_memtable_count_limit: usize,

    /// Shared node budget used by this tablet's mutable MVCC generation.
    memtable_budget: Option<NodeMemtableBudget>,

    /// Per-tablet upper bound for the active mutable generation.
    memtable_limit_bytes: usize,

    /// RAII charge acquired before the first active record is inserted.
    memtable_charge: Option<MemtableCharge>,
}

/// Nonblocking ownership boundary consumed by a future flush worker.
pub trait ImmutableMemtableSink: Send + Sync + fmt::Debug {
    /// Queue a complete immutable generation without waiting for worker progress.
    fn try_submit(
        &self,
        generation: Arc<ImmutableMemtableGeneration>,
    ) -> std::result::Result<(), FlushHandoffError>;
}

/// A bounded sink refused a generation without taking ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlushHandoffError {
    /// The handoff queue currently has no free slot.
    Full,
    /// The sink is shutting down and will not accept more generations.
    Closed,
    /// The sink rejected the generation for another explicit reason.
    Rejected(String),
}

impl fmt::Display for FlushHandoffError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("immutable flush handoff queue is full"),
            Self::Closed => formatter.write_str("immutable flush handoff queue is closed"),
            Self::Rejected(reason) => formatter.write_str(reason),
        }
    }
}

impl std::error::Error for FlushHandoffError {}

/// Complete frozen tablet memtable generation. Metadata and its exact frontier
/// are carried with Default, Write, Lock, and tombstone state for future flush.
#[derive(Debug, Clone, Default)]
pub struct ImmutableMemtableGeneration {
    /// Unique per-tablet identity used for exact retirement.
    pub id: u64,
    pub default: BTreeMap<Vec<u8>, BTreeMap<Timestamp, Vec<u8>>>,
    pub locks: BTreeMap<Vec<u8>, LockRecord>,
    pub writes: BTreeMap<Vec<u8>, BTreeMap<Timestamp, WriteRecord>>,
    pub default_tombstones: BTreeSet<(Vec<u8>, Timestamp)>,
    pub lock_tombstones: BTreeSet<Vec<u8>>,
    /// Latest complete command metadata projected into this generation.
    pub command_metadata: Option<CommandGenerationMetadata>,
    /// Greatest processed Raft boundary represented by this generation.
    pub frontier: Option<RecoveryFrontier>,
    _charge: Option<MemtableCharge>,
}

impl ImmutableMemtableGeneration {
    fn read_copy(&self) -> Self {
        Self {
            id: self.id,
            default: self.default.clone(),
            locks: self.locks.clone(),
            writes: self.writes.clone(),
            default_tombstones: self.default_tombstones.clone(),
            lock_tombstones: self.lock_tombstones.clone(),
            command_metadata: self.command_metadata.clone(),
            frontier: self.frontier,
            _charge: None,
        }
    }

    /// Return the exact immutable identity used by the retirement boundary.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Return the highest replicated boundary represented by this generation.
    pub fn recovery_frontier(&self) -> Option<RecoveryFrontier> {
        self.frontier
    }

    /// Return the managed bytes still charged to this generation.
    pub fn charged_bytes(&self) -> usize {
        self._charge.as_ref().map_or(0, MemtableCharge::bytes)
    }
}

/// Fixed managed-byte allowance for each ordered memtable record and index
/// entry. Encoded key/value bytes are charged in addition to this allowance.
const MEMTABLE_INDEX_ENTRY_OVERHEAD_BYTES: usize = 128;

fn immutable_memtable_limits(active_bytes: usize) -> Result<(usize, usize)> {
    let soft = active_bytes
        .checked_mul(DEFAULT_TABLET_IMMUTABLE_SOFT_COUNT)
        .ok_or_else(|| {
            Error::InvalidArgument("immutable soft byte limit overflowed".to_string())
        })?;
    let hard = active_bytes
        .checked_mul(DEFAULT_TABLET_IMMUTABLE_HARD_COUNT)
        .ok_or_else(|| {
            Error::InvalidArgument("immutable hard byte limit overflowed".to_string())
        })?;
    Ok((soft, hard))
}

/// Shared MVCC rules parameterized by a physical record backend.
///
/// Mutation methods require exclusive access. A tablet owner or bounded
/// storage worker must serialize mutable access to one engine instance.
#[derive(Debug, Clone, Default)]
pub struct MvccEngine<B = InMemoryMvccBackend> {
    backend: B,
}

/// Read-only sparse overlay used while preparing subcommands in one Raft
/// batch. It keeps only the batch's edited records and delegates untouched
/// reads to the immutable base backend.
struct DeltaOverlayBackend<'a, B> {
    base: &'a B,
    staged: &'a MvccDelta,
}

/// Simple in-memory MVCC reference engine retained for differential comparisons.
pub type InMemoryMvcc = MvccEngine<InMemoryMvccBackend>;

/// Production transitional memtable engine used by replicated tablets.
pub type MemtableMvcc = MvccEngine<MemtableMvccBackend>;

/// Flat ordered maps used only by the correctness/reference MVCC backend.
#[derive(Debug, Clone, Default)]
pub struct InMemoryMvccBackend {
    default: BTreeMap<Vec<u8>, BTreeMap<Timestamp, Vec<u8>>>,
    locks: BTreeMap<Vec<u8>, LockRecord>,
    writes: BTreeMap<Vec<u8>, BTreeMap<Timestamp, WriteRecord>>,
}

/// A validated prewrite delta prepared while the tablet owner is exclusive.
struct PreparedPrewrite {
    key: Vec<u8>,
    default_value: Option<Vec<u8>>,
    lock: LockRecord,
}

/// A validated committed write delta for one intent key.
struct PreparedIntentCommit {
    key: Vec<u8>,
    write: WriteRecord,
}

/// A validated rollback delta for one intent key.
struct PreparedIntentRollback {
    key: Vec<u8>,
    remove_default_value: bool,
}

/// MVCC state reconstructed from one validated snapshot table
///
/// the observed maxima are compared with the snapshot's declared allocator
/// high-water marks before recovery may publish the restored state
pub(crate) struct RestoredMvccState {
    pub(crate) storage: InMemoryMvcc,
    pub(crate) max_transaction_id: TxnId,
    pub(crate) max_timestamp: Timestamp,
}

impl<B: MvccBackend> MvccEngine<B> {
    /// Construct an MVCC engine over one physical record backend.
    pub fn with_backend(backend: B) -> Self {
        Self { backend }
    }

    /// Return allocator maxima represented by committed values and live locks.
    ///
    /// Replicated startup uses these values to seed a node-local SQL allocator
    /// above the tablet state restored from Raft. They are observations, not a
    /// replacement for the future metadata timestamp authority.
    pub fn allocator_high_water_marks(&self) -> (TxnId, Timestamp) {
        self.backend.allocator_high_water_marks()
    }

    /// Pin one immutable generation for bounded reads outside the mutable
    /// owner turn. The returned view shares the same MVCC visibility rules as
    /// the live engine.
    pub fn pin_read_view(&self) -> Result<MvccReadView<B::PinnedGeneration>> {
        Ok(MvccReadView::new(self.backend.pin_generation()?))
    }

    /// clone the complete logical MVCC maps into an immutable snapshot image
    ///
    /// the owning database runtime calls this only while holding its serialized
    /// commit/catalog barrier. Flattening the ordered maps here fixes both the
    /// state image and its deterministic protobuf ordering before that barrier
    /// is released
    pub fn capture_snapshot_state(&self) -> Result<CapturedMvccState> {
        self.pin_read_view()?.capture_snapshot_state()
    }

    /// Return the backend's current recovery boundary without pinning a read
    /// generation or materializing a snapshot of its records.
    pub fn recovery_frontier(&self) -> Result<Option<RecoveryFrontier>> {
        self.backend.recovery_frontier()
    }

    fn overlay_engine<'a>(
        &'a self,
        staged: &'a MvccDelta,
    ) -> MvccEngine<DeltaOverlayBackend<'a, B>> {
        MvccEngine::with_backend(DeltaOverlayBackend {
            base: &self.backend,
            staged,
        })
    }
}

impl MemtableMvcc {
    /// Construct a lazily grown tablet memtable engine with node and
    /// tablet-local bounds for the active generation.
    pub fn with_memtable_budget(
        budget: NodeMemtableBudget,
        max_active_bytes: usize,
    ) -> Result<Self> {
        Ok(Self::with_backend(
            MemtableMvccBackend::with_memtable_budget(budget, max_active_bytes)?,
        ))
    }

    /// Move a restored reference snapshot into a read-only, node-accounted base
    /// generation with an empty lazy mutable generation above it.
    pub fn from_restored(
        restored: InMemoryMvcc,
        budget: NodeMemtableBudget,
        max_active_bytes: usize,
    ) -> Result<Self> {
        let InMemoryMvccBackend {
            default,
            locks,
            writes,
        } = restored.backend;
        let mut backend = MemtableMvccBackend {
            default,
            locks,
            writes,
            ..MemtableMvccBackend::default()
        };
        backend.bind_memtable_budget(budget, max_active_bytes)?;
        Ok(Self::with_backend(backend))
    }

    /// Bind an existing restored reference snapshot to the node budget.
    pub fn bind_memtable_budget(
        &mut self,
        budget: NodeMemtableBudget,
        max_active_bytes: usize,
    ) -> Result<()> {
        self.backend.bind_memtable_budget(budget, max_active_bytes)
    }

    /// Return managed bytes charged to the active mutable generation.
    pub fn active_memtable_bytes(&self) -> usize {
        self.backend
            .memtable_charge
            .as_ref()
            .map_or(0, MemtableCharge::bytes)
    }

    /// Return this tablet's configured active-generation byte limit.
    pub fn active_memtable_limit_bytes(&self) -> Option<usize> {
        self.backend
            .memtable_budget
            .as_ref()
            .map(|_| self.backend.memtable_limit_bytes)
    }

    /// Return active and immutable generation pressure.
    pub fn memtable_pressure(&self) -> Option<MemtablePressure> {
        self.backend.memtable_pressure()
    }

    /// Return the shared node budget used by this storage instance.
    pub fn node_memtable_budget(&self) -> Option<NodeMemtableBudget> {
        self.backend.memtable_budget.clone()
    }

    /// Attach a nonblocking immutable-generation handoff endpoint.
    pub fn set_immutable_memtable_sink(&mut self, sink: Arc<dyn ImmutableMemtableSink>) {
        self.backend.set_immutable_memtable_sink(sink);
    }

    /// Retry nonblocking handoff for immutable generations not yet accepted.
    pub fn try_handoff_pending_immutables(
        &mut self,
    ) -> std::result::Result<usize, FlushHandoffError> {
        self.backend.try_handoff_pending_immutables()
    }

    /// Return the most recent permanent immutable-handoff failure, if any.
    pub fn last_flush_handoff_error(&self) -> Option<FlushHandoffError> {
        self.backend.last_flush_handoff_error.clone()
    }

    /// Return immutable generations still retained for serving reads.
    pub fn immutable_memtable_generations(&self) -> Vec<Arc<ImmutableMemtableGeneration>> {
        self.backend.immutable_generations()
    }

    /// Test-only hook for exercising immutable charge retirement.
    ///
    /// A fake sink handoff is not a durable publication proof. Production
    /// retirement will be introduced with the Stage 4.6 publication boundary.
    #[cfg(test)]
    fn retire_immutable_generation_for_test(&mut self, generation_id: u64) -> Result<()> {
        self.backend
            .retire_immutable_generation_for_test(generation_id)
    }

    /// Bytes held by the restored read-only base, outside the active cap.
    pub fn restored_base_bytes(&self) -> usize {
        self.backend
            .restored_base
            .as_ref()
            .map_or(0, ImmutableMemtableGeneration::charged_bytes)
    }
}

impl InMemoryMvcc {
    /// Reconstruct one table's complete MVCC maps from snapshot entries.
    ///
    /// Duplicate map keys, cross-table row keys, malformed rows, invalid
    /// records, and `Put` writes without their referenced default value are
    /// rejected before the state can enter recovery staging.
    pub(crate) fn from_snapshot_table(
        table_id: TableId,
        table: &snapshot_proto::SnapshotTable,
    ) -> Result<RestoredMvccState> {
        let mut storage = InMemoryMvccBackend::default();
        let mut max_transaction_id = TxnId(0);
        let mut max_timestamp = Timestamp(0);

        for entry in &table.default_values {
            validate_snapshot_row_key(table_id, &entry.key, "default value")?;

            let start_timestamp = entry
                .start_timestamp
                .as_ref()
                .cloned()
                .map(Timestamp::from_proto)
                .ok_or_else(|| {
                    Error::CorruptData(
                        "snapshot default value is missing its start timestamp".to_string(),
                    )
                })?;

            if start_timestamp.0 == 0 {
                return Err(Error::CorruptData(
                    "snapshot default value contains reserved timestamp 0".to_string(),
                ));
            }

            decode_row(&entry.row).map_err(|source| {
                Error::CorruptData(format!(
                    "snapshot default value contains an invalid encoded row: {source}"
                ))
            })?;

            let previous = storage
                .default
                .entry(entry.key.clone())
                .or_default()
                .insert(start_timestamp, entry.row.clone());

            if previous.is_some() {
                return Err(Error::CorruptData(format!(
                    "snapshot contains duplicate default value for table {} at \
                     start timestamp {}",
                    table_id.0, start_timestamp.0
                )));
            }

            max_timestamp = Timestamp(max_timestamp.0.max(start_timestamp.0));
        }

        for entry in &table.locks {
            validate_snapshot_row_key(table_id, &entry.key, "lock")?;

            let record = entry
                .record
                .as_ref()
                .cloned()
                .ok_or_else(|| Error::CorruptData("snapshot lock record is missing".to_string()))
                .and_then(|record| {
                    LockRecord::from_proto(record).map_err(|message| {
                        Error::CorruptData(format!("snapshot lock record is invalid: {message}"))
                    })
                })?;

            if record.txn_id.0 == 0 || record.start_timestamp.0 == 0 {
                return Err(Error::CorruptData(
                    "snapshot lock contains a reserved transaction ID or timestamp 0".to_string(),
                ));
            }

            if storage
                .locks
                .insert(entry.key.clone(), record.clone())
                .is_some()
            {
                return Err(Error::CorruptData(format!(
                    "snapshot contains duplicate lock for table {}",
                    table_id.0
                )));
            }

            max_transaction_id = TxnId(max_transaction_id.0.max(record.txn_id.0));
            max_timestamp = Timestamp(max_timestamp.0.max(record.start_timestamp.0));
        }

        for entry in &table.writes {
            validate_snapshot_row_key(table_id, &entry.key, "write")?;

            let write_timestamp = entry
                .write_timestamp
                .as_ref()
                .cloned()
                .map(Timestamp::from_proto)
                .ok_or_else(|| {
                    Error::CorruptData("snapshot write is missing its write timestamp".to_string())
                })?;
            let record = entry
                .record
                .as_ref()
                .cloned()
                .ok_or_else(|| Error::CorruptData("snapshot write record is missing".to_string()))
                .and_then(|record| {
                    WriteRecord::from_proto(record).map_err(|message| {
                        Error::CorruptData(format!("snapshot write record is invalid: {message}"))
                    })
                })?;

            validate_write_record(write_timestamp, &record)?;

            let previous = storage
                .writes
                .entry(entry.key.clone())
                .or_default()
                .insert(write_timestamp, record.clone());

            if previous.is_some() {
                return Err(Error::CorruptData(format!(
                    "snapshot contains duplicate write for table {} at timestamp {}",
                    table_id.0, write_timestamp.0
                )));
            }

            max_timestamp = Timestamp(
                max_timestamp
                    .0
                    .max(record.start_timestamp.0)
                    .max(write_timestamp.0),
            );
        }

        for (key, versions) in &storage.writes {
            for record in versions.values() {
                if record.op == WriteKind::Put
                    && !storage
                        .default
                        .get(key)
                        .is_some_and(|values| values.contains_key(&record.start_timestamp))
                {
                    return Err(Error::CorruptData(format!(
                        "snapshot Put for table {} references missing default value \
                         at start timestamp {}",
                        table_id.0, record.start_timestamp.0
                    )));
                }
            }
        }

        Ok(RestoredMvccState {
            storage: MvccEngine::with_backend(storage),
            max_transaction_id,
            max_timestamp,
        })
    }
}

impl<B: MvccBackend> MvccEngine<B> {
    fn validate_mutation(
        &self,
        key: &[u8],
        mutation: &Mutation,
        start_ts: Timestamp,
    ) -> Result<()> {
        validate_encoded_key_argument(key, "mutation key")?;

        match mutation {
            Mutation::Put(row) => {
                decode_row(row).map_err(|error| {
                    Error::InvalidArgument(format!(
                        "Put mutation does not contain a canonical encoded row: {error}"
                    ))
                })?;

                if let Some(existing) = self.backend.get_default(key, start_ts)?
                    && existing != *row
                {
                    return Err(Error::CorruptData(format!(
                        "start timestamp {} already has a different default value",
                        start_ts.0
                    )));
                }
            }

            Mutation::Delete => {
                if self.backend.get_default(key, start_ts)?.is_some() {
                    return Err(Error::CorruptData(format!(
                        "Delete at start timestamp {} conflicts with an existing \
                         default value for the same transaction",
                        start_ts.0
                    )));
                }
            }
        }

        Ok(())
    }

    fn validate_lock(
        &self,
        key: &[u8],
        mutation: &Mutation,
        txn_id: TxnId,
        start_ts: Timestamp,
    ) -> Result<()> {
        let Some(lock) = self.backend.get_lock(key)? else {
            return Ok(());
        };

        if lock.txn_id != txn_id || lock.start_timestamp != start_ts {
            return Err(Error::WriteConflict(format!(
                "row is locked by transaction {} at start timestamp {}",
                lock.txn_id.0, lock.start_timestamp.0
            )));
        }

        if lock.op != mutation.write_kind() {
            return Err(Error::CorruptData(format!(
                "transaction {} has a lock operation inconsistent with its mutation",
                txn_id.0
            )));
        }

        Ok(())
    }

    fn validate_write_history(&self, key: &[u8], start_ts: Timestamp) -> Result<()> {
        // Write records are validated when they are published or restored. A
        // conflict check only needs this transaction's rollback witness and
        // writes newer than its snapshot; scanning older history on every
        // prewrite made steady-state writes slower as chains grew.
        if let Some(write) = self.backend.get_write(key, start_ts)? {
            validate_write_record(start_ts, &write)?;
            if write.op == WriteKind::Rollback && write.start_timestamp == start_ts {
                return Err(Error::WriteConflict(format!(
                    "transaction starting at timestamp {} was already rolled back",
                    start_ts.0
                )));
            }
        }

        let mut resume_after = None;
        loop {
            let page = self.backend.write_page(
                key,
                Excluded(start_ts),
                Unbounded,
                resume_after,
                MvccCursorDirection::Reverse,
                MVCC_WRITE_CURSOR_PAGE_SIZE,
            )?;
            for (write_ts, write) in &page.writes {
                if write.op != WriteKind::Rollback {
                    validate_write_record(*write_ts, write)?;
                    return Err(Error::WriteConflict(format!(
                        "row was modified at timestamp {} after transaction start \
                         timestamp {}",
                        write_ts.0, start_ts.0
                    )));
                }
            }
            if !page.has_more {
                return Ok(());
            }
            resume_after = page.writes.last().map(|(timestamp, _)| *timestamp);
        }
    }

    /// the finalized commit timestamp can be inserted after every existing
    /// write map entry
    ///
    /// snapshot conflicts are checked by `validate_write_history` before timestamp
    /// allocation. This second check protects the physical MVCC ordering invariant
    /// when applying an already finalized durable commit
    fn validate_commit_timestamp(&self, key: &[u8], commit_ts: Timestamp) -> Result<()> {
        let latest = self.backend.write_page(
            key,
            Unbounded,
            Unbounded,
            None,
            MvccCursorDirection::Reverse,
            1,
        )?;

        if let Some((latest_write_ts, _)) = latest.writes.first()
            && *latest_write_ts >= commit_ts
        {
            return Err(Error::WriteConflict(format!(
                "commit timestamp {} does not advance the row's latest write \
             timestamp {}",
                commit_ts.0, latest_write_ts.0
            )));
        }

        Ok(())
    }

    fn is_exactly_applied(
        &self,
        key: &[u8],
        mutation: &Mutation,
        start_ts: Timestamp,
        commit_ts: Timestamp,
    ) -> Result<bool> {
        let Some(write) = self.backend.get_write(key, commit_ts)? else {
            return Ok(false);
        };

        validate_write_record(commit_ts, &write)?;

        if write.start_timestamp != start_ts || write.op != mutation.write_kind() {
            return Err(Error::CorruptData(format!(
                "write timestamp {} is already occupied by a different write",
                commit_ts.0
            )));
        }

        if let Mutation::Put(expected_row) = mutation {
            let stored_row = self.backend.get_default(key, start_ts)?.ok_or_else(|| {
                Error::CorruptData(format!(
                    "replayed Put at timestamp {} has no default value",
                    commit_ts.0
                ))
            })?;

            if stored_row != *expected_row {
                return Err(Error::CorruptData(format!(
                    "replayed Put at timestamp {} references different row bytes",
                    commit_ts.0
                )));
            }
        }

        Ok(true)
    }

    fn validate_batch_replay(
        &self,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
        start_ts: Timestamp,
        commit_ts: Timestamp,
    ) -> Result<bool> {
        let mut applied = 0;

        for (key, mutation) in mutations {
            if self.is_exactly_applied(key, mutation, start_ts, commit_ts)? {
                applied += 1;
            }
        }

        if applied == 0 {
            return Ok(false);
        }

        if applied == mutations.len() {
            return Ok(true);
        }

        Err(Error::CorruptData(format!(
            "transaction at start timestamp {} is only partially present at \
             write timestamp {}",
            start_ts.0, commit_ts.0
        )))
    }

    /// Validate one prewrite and return its sparse state delta, if it is not an
    /// exact replay. The owner must remain exclusive until the delta is published.
    fn prepare_prewrite(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        key: &[u8],
        mutation: &Mutation,
        primary_key: &[u8],
        ttl_ms: u64,
    ) -> Result<Option<PreparedPrewrite>> {
        self.validate_mutation(key, mutation, start_ts)?;
        self.validate_write_history(key, start_ts)?;

        let expected_lock = LockRecord {
            txn_id,
            primary_key: primary_key.to_vec(),
            start_timestamp: start_ts,
            ttl_ms,
            op: mutation.write_kind(),
        };

        if let Some(existing_lock) = self.backend.get_lock(key)? {
            if existing_lock.txn_id != txn_id || existing_lock.start_timestamp != start_ts {
                return Err(Error::WriteConflict(format!(
                    "row is locked by transaction {} at start timestamp {}",
                    existing_lock.txn_id.0, existing_lock.start_timestamp.0
                )));
            }

            if existing_lock != expected_lock {
                return Err(Error::CorruptData(format!(
                    "transaction {} replayed prewrite with different lock metadata",
                    txn_id.0
                )));
            }

            if let Mutation::Put(expected_row) = mutation {
                let stored_row = self.backend.get_default(key, start_ts)?.ok_or_else(|| {
                    Error::CorruptData(format!(
                        "transaction {} has a Put lock without its default value",
                        txn_id.0
                    ))
                })?;

                if stored_row != *expected_row {
                    return Err(Error::CorruptData(format!(
                        "transaction {} replayed prewrite with different row bytes",
                        txn_id.0
                    )));
                }
            }

            return Ok(None);
        }

        if self.backend.get_default(key, start_ts)?.is_some() {
            return Err(Error::CorruptData(format!(
                "transaction {} has a default value without its prewrite lock",
                txn_id.0
            )));
        }

        Ok(Some(PreparedPrewrite {
            key: key.to_vec(),
            default_value: match mutation {
                Mutation::Put(row) => Some(row.clone()),
                Mutation::Delete => None,
            },
            lock: expected_lock,
        }))
    }

    /// Publish a prevalidated prewrite delta.
    fn append_prewrite_edit(delta: &mut MvccDelta, prepared: PreparedPrewrite) {
        let PreparedPrewrite {
            key,
            default_value,
            lock,
        } = prepared;

        if let Some(row) = default_value {
            delta.edits.push(MvccRecordEdit::PutDefault {
                key: key.clone(),
                start_ts: lock.start_timestamp,
                row,
            });
        }

        delta.edits.push(MvccRecordEdit::PutLock { key, lock });
    }

    /// Validate one intent commit. An exact replay returns no delta because its
    /// committed write record is already present.
    fn prepare_intent_commit(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        key: &[u8],
    ) -> Result<Option<PreparedIntentCommit>> {
        validate_encoded_key_argument(key, "intent commit key")?;

        if let Some(write) = self.backend.get_write(key, commit_ts)? {
            validate_write_record(commit_ts, &write)?;

            let mut resume_after = None;
            loop {
                let page = self.backend.write_page(
                    key,
                    Unbounded,
                    Unbounded,
                    resume_after,
                    MvccCursorDirection::Forward,
                    MVCC_WRITE_CURSOR_PAGE_SIZE,
                )?;
                for (other_write_ts, other) in &page.writes {
                    validate_write_record(*other_write_ts, other)?;

                    if *other_write_ts != commit_ts && other.start_timestamp == start_ts {
                        return Err(Error::CorruptData(format!(
                            "transaction starting at timestamp {} has multiple durable outcomes at write timestamps {} and {}",
                            start_ts.0, commit_ts.0, other_write_ts.0
                        )));
                    }
                }
                if !page.has_more {
                    break;
                }
                resume_after = page.writes.last().map(|(timestamp, _)| *timestamp);
            }

            if write.start_timestamp != start_ts || write.op == WriteKind::Rollback {
                return Err(Error::CorruptData(format!(
                    "write timestamp {} is occupied by another transaction outcome",
                    commit_ts.0
                )));
            }

            if self
                .backend
                .get_lock(key)?
                .is_some_and(|lock| lock.start_timestamp == start_ts)
            {
                return Err(Error::CorruptData(format!(
                    "committed transaction at timestamp {} still has an intent lock",
                    commit_ts.0
                )));
            }

            match write.op {
                WriteKind::Put => {
                    let row = self.backend.get_default(key, start_ts)?.ok_or_else(|| {
                        Error::CorruptData(format!(
                            "committed Put at timestamp {} has no default value",
                            commit_ts.0
                        ))
                    })?;

                    decode_row(&row)?;
                }
                WriteKind::Delete => {
                    if self.backend.get_default(key, start_ts)?.is_some() {
                        return Err(Error::CorruptData(format!(
                            "committed Delete at timestamp {} retains a default value",
                            commit_ts.0
                        )));
                    }
                }
                WriteKind::Rollback => unreachable!("handled above"),
            }

            return Ok(None);
        }

        let lock = self.backend.get_lock(key)?.ok_or_else(|| {
            Error::WriteConflict(format!(
                "transaction {} has no intent to commit at start timestamp {}",
                txn_id.0, start_ts.0
            ))
        })?;

        if lock.txn_id != txn_id || lock.start_timestamp != start_ts {
            return Err(Error::WriteConflict(format!(
                "row is locked by transaction {} at start timestamp {}",
                lock.txn_id.0, lock.start_timestamp.0
            )));
        }

        match lock.op {
            WriteKind::Put => {
                let row = self.backend.get_default(key, start_ts)?.ok_or_else(|| {
                    Error::CorruptData(format!(
                        "transaction {} has a Put lock without its default value",
                        txn_id.0
                    ))
                })?;

                decode_row(&row)?;
            }
            WriteKind::Delete => {
                if self.backend.get_default(key, start_ts)?.is_some() {
                    return Err(Error::CorruptData(format!(
                        "transaction {} has a Delete lock with a default value",
                        txn_id.0
                    )));
                }
            }
            WriteKind::Rollback => {
                return Err(Error::CorruptData(format!(
                    "transaction {} has an invalid Rollback intent lock",
                    txn_id.0
                )));
            }
        }

        self.validate_write_history(key, start_ts)?;
        self.validate_commit_timestamp(key, commit_ts)?;

        Ok(Some(PreparedIntentCommit {
            key: key.to_vec(),
            write: WriteRecord {
                start_timestamp: start_ts,
                commit_timestamp: commit_ts,
                op: lock.op,
            },
        }))
    }

    /// Publish a prevalidated intent commit delta.
    fn append_intent_commit_edit(delta: &mut MvccDelta, prepared: PreparedIntentCommit) {
        let PreparedIntentCommit { key, write } = prepared;
        let commit_ts = write.commit_timestamp;

        delta.edits.push(MvccRecordEdit::PutWrite {
            key: key.clone(),
            write_ts: commit_ts,
            write,
        });
        delta.edits.push(MvccRecordEdit::DeleteLock { key });
    }

    /// Validate one rollback. An existing rollback witness is an exact replay.
    fn prepare_intent_rollback(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        key: &[u8],
    ) -> Result<Option<PreparedIntentRollback>> {
        validate_encoded_key_argument(key, "intent rollback key")?;

        let mut resume_after = None;
        let mut rollback_witness = None;
        loop {
            let page = self.backend.write_page(
                key,
                Unbounded,
                Unbounded,
                resume_after,
                MvccCursorDirection::Forward,
                MVCC_WRITE_CURSOR_PAGE_SIZE,
            )?;
            for (stored_write_ts, write) in &page.writes {
                validate_write_record(*stored_write_ts, write)?;

                if write.start_timestamp == start_ts && write.op != WriteKind::Rollback {
                    return Err(Error::WriteConflict(format!(
                        "transaction starting at timestamp {} is already committed",
                        start_ts.0
                    )));
                }
                if *stored_write_ts == start_ts {
                    if write.start_timestamp != start_ts || write.op != WriteKind::Rollback {
                        return Err(Error::CorruptData(format!(
                            "write timestamp {} is occupied by a non-rollback outcome",
                            start_ts.0
                        )));
                    }
                    rollback_witness = Some(write.clone());
                }
            }

            if !page.has_more {
                break;
            }
            resume_after = page.writes.last().map(|(timestamp, _)| *timestamp);
        }

        if rollback_witness.is_some() {
            if self
                .backend
                .get_lock(key)?
                .is_some_and(|lock| lock.start_timestamp == start_ts)
                || self.backend.get_default(key, start_ts)?.is_some()
            {
                return Err(Error::CorruptData(format!(
                    "rolled-back transaction at timestamp {} retains intent state",
                    start_ts.0
                )));
            }

            return Ok(None);
        }

        let lock = self.backend.get_lock(key)?;
        let locked_op = match lock.as_ref() {
            Some(lock) if lock.txn_id != txn_id || lock.start_timestamp != start_ts => {
                return Err(Error::WriteConflict(format!(
                    "row is locked by transaction {} at start timestamp {}",
                    lock.txn_id.0, lock.start_timestamp.0
                )));
            }
            Some(lock) => Some(lock.op),
            None => None,
        };

        match locked_op {
            Some(WriteKind::Put) => {
                let row = self.backend.get_default(key, start_ts)?.ok_or_else(|| {
                    Error::CorruptData(format!(
                        "transaction {} has a Put lock without its default value",
                        txn_id.0
                    ))
                })?;

                decode_row(&row)?;
            }
            Some(WriteKind::Delete) | None => {
                if self.backend.get_default(key, start_ts)?.is_some() {
                    return Err(Error::CorruptData(format!(
                        "transaction {} has a default value without a matching Put lock",
                        txn_id.0
                    )));
                }
            }
            Some(WriteKind::Rollback) => {
                return Err(Error::CorruptData(format!(
                    "transaction {} has an invalid Rollback intent lock",
                    txn_id.0
                )));
            }
        }

        Ok(Some(PreparedIntentRollback {
            key: key.to_vec(),
            remove_default_value: locked_op == Some(WriteKind::Put),
        }))
    }

    /// Publish a prevalidated rollback delta and its replay witness.
    fn append_intent_rollback_edit(
        delta: &mut MvccDelta,
        prepared: PreparedIntentRollback,
        start_ts: Timestamp,
    ) {
        let PreparedIntentRollback {
            key,
            remove_default_value,
        } = prepared;

        if remove_default_value {
            delta.edits.push(MvccRecordEdit::DeleteDefault {
                key: key.clone(),
                start_ts,
            });
        }

        delta
            .edits
            .push(MvccRecordEdit::DeleteLock { key: key.clone() });
        delta.edits.push(MvccRecordEdit::PutWrite {
            key,
            write_ts: start_ts,
            write: WriteRecord {
                start_timestamp: start_ts,
                commit_timestamp: start_ts,
                op: WriteKind::Rollback,
            },
        });
    }

    /// restore one tablet's complete MVCC state from validated snapshot entries
    ///
    /// the tablet owns its table identity separately from the catalog definition,
    /// so this path intentionally restores only the MVCC records required by the
    /// tablet state machine
    pub fn restore_from_snapshot_entries(
        table_id: TableId,
        default_values: Vec<snapshot_proto::DefaultValueEntry>,
        locks: Vec<snapshot_proto::LockEntry>,
        writes: Vec<snapshot_proto::WriteEntry>,
    ) -> Result<InMemoryMvcc> {
        let table = snapshot_proto::SnapshotTable {
            definition: None,
            default_values,
            locks,
            writes,
        };

        Ok(MvccEngine::<InMemoryMvccBackend>::from_snapshot_table(table_id, &table)?.storage)
    }
}

impl InMemoryMvcc {
    /// Construct an empty memory-backed MVCC reference engine.
    pub fn new() -> Self {
        Self::with_backend(InMemoryMvccBackend::default())
    }
}

fn read_visible_version<R: MvccReadGeneration + ?Sized>(
    generation: &R,
    key: &[u8],
    read_ts: Timestamp,
) -> Result<Option<Vec<u8>>> {
    if let Some(lock) = generation.get_lock(key)?
        && lock.start_timestamp <= read_ts
    {
        return Err(Error::WriteConflict(format!(
            "row is locked by transaction {} at start timestamp {}",
            lock.txn_id.0, lock.start_timestamp.0
        )));
    }

    let mut resume_after = None;
    loop {
        let page = generation.write_page(
            key,
            Unbounded,
            Included(read_ts),
            resume_after,
            MvccCursorDirection::Reverse,
            MVCC_WRITE_CURSOR_PAGE_SIZE,
        )?;

        for (stored_write_ts, write) in &page.writes {
            validate_write_record(*stored_write_ts, write)?;

            match write.op {
                WriteKind::Put => {
                    let row = generation
                        .get_default(key, write.start_timestamp)?
                        .ok_or_else(|| {
                            Error::CorruptData(format!(
                                "Put at write timestamp {} references missing default \
                                 value at start timestamp {}",
                                stored_write_ts.0, write.start_timestamp.0
                            ))
                        })?;

                    decode_row(&row)?;
                    return Ok(Some(row));
                }
                WriteKind::Delete => return Ok(None),
                WriteKind::Rollback => {}
            }
        }

        if !page.has_more {
            return Ok(None);
        }
        resume_after = page.writes.last().map(|(timestamp, _)| *timestamp);
    }
}

fn intent_for_read<R: MvccReadGeneration + ?Sized>(
    generation: &R,
    key: &[u8],
    read_ts: Timestamp,
) -> Result<Option<LockRecord>> {
    validate_encoded_key_argument(key, "intent read key")?;

    let Some(lock) = generation.get_lock(key)? else {
        return Ok(None);
    };
    if lock.start_timestamp > read_ts {
        return Ok(None);
    }

    lock.validate().map_err(|error| {
        Error::CorruptData(format!("intent lock cannot be used for a read: {error}"))
    })?;
    Ok(Some(lock))
}

fn scan_intent_page<R: MvccReadGeneration + ?Sized>(
    generation: &R,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    resume_after: Option<&[u8]>,
    max_locks: usize,
) -> Result<IntentScanPage> {
    if max_locks == 0 {
        return Err(Error::InvalidArgument(
            "intent scan page max_locks must be greater than zero".to_string(),
        ));
    }

    encoded_scan_bounds(start, end)?;
    if let Some(resume_after) = resume_after {
        validate_encoded_key_argument(resume_after, "intent scan resume key")?;
        if end.is_some_and(|end| resume_after >= end) {
            return Ok(IntentScanPage {
                locks: Vec::new(),
                has_more: false,
            });
        }
    }

    let page = generation.lock_page(start, end, resume_after, max_locks, usize::MAX)?;
    for (key, lock) in &page.locks {
        validate_encoded_key_argument(key, "intent scan key")?;
        lock.validate().map_err(|error| {
            Error::CorruptData(format!("intent scan found an invalid lock: {error}"))
        })?;
    }
    Ok(page)
}

fn scan_conflicting_intents<R: MvccReadGeneration + ?Sized>(
    generation: &R,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    resume_after: Option<&[u8]>,
    read_ts: Timestamp,
    max_locks: usize,
    max_bytes: usize,
) -> Result<IntentScanPage> {
    if max_locks == 0 {
        return Err(Error::InvalidArgument(
            "foreground scan intent max_locks must be greater than zero".to_string(),
        ));
    }
    if max_bytes == 0 {
        return Err(Error::InvalidArgument(
            "foreground scan intent max_bytes must be greater than zero".to_string(),
        ));
    }

    encoded_scan_bounds(start, end)?;
    if let Some(resume_after) = resume_after {
        validate_encoded_key_argument(resume_after, "foreground scan intent resume key")?;
        if end.is_some_and(|end| resume_after >= end) {
            return Ok(IntentScanPage {
                locks: Vec::new(),
                has_more: false,
            });
        }
    }

    let mut locks = Vec::new();
    let mut encoded_bytes = 0usize;
    let mut cursor = resume_after.map(ToOwned::to_owned);
    loop {
        // Filter by the snapshot timestamp before applying result limits. A
        // later-starting intent is not a conflict and must not create a false
        // continuation page after the final conflicting intent.
        let page = generation.lock_page(start, end, cursor.as_deref(), 1, usize::MAX)?;
        let Some((key, lock)) = page.locks.into_iter().next() else {
            return Ok(IntentScanPage {
                locks,
                has_more: false,
            });
        };

        validate_encoded_key_argument(&key, "foreground scan intent key")?;
        lock.validate().map_err(|error| {
            Error::CorruptData(format!("foreground scan found an invalid lock: {error}"))
        })?;
        cursor = Some(key.clone());
        if lock.start_timestamp > read_ts {
            if !page.has_more {
                return Ok(IntentScanPage {
                    locks,
                    has_more: false,
                });
            }
            continue;
        }

        if locks.len() >= max_locks {
            return Ok(IntentScanPage {
                locks,
                has_more: true,
            });
        }

        let encoded_lock = lock
            .to_proto()
            .map_err(|error| Error::CorruptData(error.to_string()))?
            .encoded_len();
        let entry_bytes = key.len().checked_add(encoded_lock).ok_or_else(|| {
            Error::InvalidArgument("foreground scan intent byte count overflowed".to_string())
        })?;
        let next_bytes = encoded_bytes.checked_add(entry_bytes).ok_or_else(|| {
            Error::InvalidArgument("foreground scan intent byte count overflowed".to_string())
        })?;
        if next_bytes > max_bytes {
            if locks.is_empty() {
                return Err(Error::InvalidArgument(
                    "foreground scan intent byte budget is smaller than the first encoded lock"
                        .to_string(),
                ));
            }
            return Ok(IntentScanPage {
                locks,
                has_more: true,
            });
        }

        encoded_bytes = next_bytes;
        locks.push((key, lock));
        if !page.has_more {
            return Ok(IntentScanPage {
                locks,
                has_more: false,
            });
        }
    }
}

fn scan_page_from<R: MvccReadGeneration + ?Sized>(
    generation: &R,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    resume_after: Option<&[u8]>,
    read_ts: Timestamp,
    max_rows: usize,
    max_bytes: usize,
) -> Result<MvccScanPage> {
    validate_scan_page_limits(max_rows, max_bytes)?;
    encoded_scan_bounds(start, end)?;

    if let Some(resume_after) = resume_after {
        validate_encoded_key_argument(resume_after, "scan resume key")?;
        if end.is_some_and(|end| resume_after >= end) {
            return Ok(MvccScanPage {
                rows: Vec::new(),
                has_more: false,
            });
        }
    }

    let mut rows = Vec::new();
    let mut encoded_bytes = 0_usize;
    let mut writes = MvccFamilyKeyCursor::new(MvccKeyFamily::Writes, resume_after);
    let mut locks = MvccFamilyKeyCursor::new(MvccKeyFamily::Locks, resume_after);

    loop {
        let Some(key) = next_candidate_key(generation, &mut writes, &mut locks, start, end)? else {
            return Ok(MvccScanPage {
                rows,
                has_more: false,
            });
        };

        // Locks must participate even when they have no committed history;
        // otherwise a scan could pass a locked insertion.
        let Some(row) = read_visible_version(generation, &key, read_ts)? else {
            continue;
        };

        if rows.len() >= max_rows {
            return Ok(MvccScanPage {
                rows,
                has_more: true,
            });
        }

        let row_bytes = key.len().checked_add(row.len()).ok_or_else(|| {
            Error::InvalidArgument("scan page encoded byte count overflowed".to_string())
        })?;
        let next_bytes = encoded_bytes.checked_add(row_bytes).ok_or_else(|| {
            Error::InvalidArgument("scan page encoded byte count overflowed".to_string())
        })?;
        if next_bytes > max_bytes {
            if rows.is_empty() {
                return Err(Error::InvalidArgument(
                    "scan page byte budget is smaller than the first encoded row".to_string(),
                ));
            }
            return Ok(MvccScanPage {
                rows,
                has_more: true,
            });
        }

        encoded_bytes = next_bytes;
        rows.push((key, row));
    }
}

impl<B: MvccBackend> MvccStorage for MvccEngine<B> {
    fn pin_read_generation(&self) -> Result<Box<dyn MvccReadGeneration + Send + Sync>> {
        Ok(Box::new(self.backend.pin_generation()?))
    }

    fn get_default_record(&self, key: &[u8], start_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        self.backend.get_default(key, start_ts)
    }

    fn get_lock_record(&self, key: &[u8]) -> Result<Option<LockRecord>> {
        self.backend.get_lock(key)
    }

    fn get_write_record(&self, key: &[u8], write_ts: Timestamp) -> Result<Option<WriteRecord>> {
        self.backend.get_write(key, write_ts)
    }

    fn read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        self.backend.read(key, read_ts)
    }

    fn intent_for_read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<LockRecord>> {
        self.backend.intent_for_read(key, read_ts)
    }

    fn scan_intent_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_locks: usize,
    ) -> Result<IntentScanPage> {
        self.backend
            .scan_intent_page(start, end, resume_after, max_locks)
    }

    fn scan_conflicting_intents(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        read_ts: Timestamp,
        max_locks: usize,
        max_bytes: usize,
    ) -> Result<IntentScanPage> {
        self.backend.scan_conflicting_intents(
            start,
            end,
            resume_after,
            read_ts,
            max_locks,
            max_bytes,
        )
    }

    fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        read_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .scan_page(start, end, None, read_ts, usize::MAX, usize::MAX)?
            .rows)
    }

    fn scan_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        read_ts: Timestamp,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<MvccScanPage> {
        self.backend
            .scan_page(start, end, resume_after, read_ts, max_rows, max_bytes)
    }

    fn validate_commit_batch(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<()> {
        validate_commit_preflight_metadata(txn_id, start_ts)?;

        // this will validate the entire representation before consulting conflict state
        for (key, mutation) in mutations {
            self.validate_mutation(key, mutation, start_ts)?;
        }

        for (key, mutation) in mutations {
            self.validate_lock(key, mutation, txn_id, start_ts)?;
            self.validate_write_history(key, start_ts)?;
        }

        Ok(())
    }

    fn commit_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<usize> {
        let applied_writes = mutations.len();
        let delta = self.prepare_commit_batch(txn_id, start_ts, commit_ts, mutations)?;
        self.publish_mvcc_delta(delta)?;
        Ok(applied_writes)
    }

    fn prepare_commit_batch(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<MvccDelta> {
        validate_commit_metadata(txn_id, start_ts, commit_ts)?;

        // validate persisted representations before considering an idempotent
        // replay; corrupt replay input must never be accepted merely because a
        // matching timestamp exists in the write map
        for (key, mutation) in mutations {
            self.validate_mutation(key, mutation, start_ts)?;
        }

        if mutations.is_empty() {
            return Ok(MvccDelta::default());
        }

        // identical batch is a safe deterministic replay A
        // partially present batch is impossible after atomic application and
        // therefore represents corrupted state
        if self.validate_batch_replay(mutations, start_ts, commit_ts)? {
            return Ok(MvccDelta::default());
        }

        self.validate_commit_batch(txn_id, start_ts, mutations)?;

        // commit timestamps are finalized only after snapshot-conflict
        // preflight; Validate their insertion position before changing any map
        for key in mutations.keys() {
            self.validate_commit_timestamp(key, commit_ts)?;
        }

        // Prepare one sparse backend delta only after every logical validation
        // has succeeded. The caller can combine it with tablet metadata before
        // invoking the single command publication boundary.
        let mut delta = MvccDelta::default();
        for (key, mutation) in mutations {
            match mutation {
                Mutation::Put(row) => {
                    delta.edits.push(MvccRecordEdit::PutDefault {
                        key: key.clone(),
                        start_ts,
                        row: row.clone(),
                    });
                }

                Mutation::Delete => {
                    // Deletes have no default payload. Their committed write
                    // record is the tombstone.
                }
            }

            delta.edits.push(MvccRecordEdit::PutWrite {
                key: key.clone(),
                write_ts: commit_ts,
                write: WriteRecord {
                    start_timestamp: start_ts,
                    commit_timestamp: commit_ts,
                    op: mutation.write_kind(),
                },
            });

            if self
                .backend
                .get_lock(key)?
                .is_some_and(|lock| lock.txn_id == txn_id && lock.start_timestamp == start_ts)
            {
                delta
                    .edits
                    .push(MvccRecordEdit::DeleteLock { key: key.clone() });
            }
        }

        Ok(delta)
    }

    fn stats(&self) -> MvccStats {
        self.backend.stats()
    }

    fn prewrite(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        key: &[u8],
        mutation: &Mutation,
        primary_key: &[u8],
        ttl_ms: u64,
    ) -> Result<()> {
        validate_commit_preflight_metadata(txn_id, start_ts)?;
        validate_encoded_key_argument(primary_key, "prewrite primary key")?;

        if ttl_ms == 0 {
            return Err(Error::InvalidArgument(
                "prewrite lock TTL must be non-zero".to_string(),
            ));
        }

        if let Some(prepared) =
            self.prepare_prewrite(txn_id, start_ts, key, mutation, primary_key, ttl_ms)?
        {
            let mut delta = MvccDelta::default();
            Self::append_prewrite_edit(&mut delta, prepared);
            self.backend.publish_atomic(delta)?;
        }

        Ok(())
    }

    fn prewrite_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
        primary_key: &[u8],
        ttl_ms: u64,
    ) -> Result<()> {
        let delta =
            self.prepare_prewrite_batch(txn_id, start_ts, mutations, primary_key, ttl_ms)?;
        self.publish_mvcc_delta(delta)
    }

    fn prepare_prewrite_batch(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
        primary_key: &[u8],
        ttl_ms: u64,
    ) -> Result<MvccDelta> {
        if mutations.is_empty() {
            return Err(Error::InvalidArgument(
                "distributed prewrite batch must contain at least one mutation".to_string(),
            ));
        }

        validate_commit_preflight_metadata(txn_id, start_ts)?;
        validate_encoded_key_argument(primary_key, "prewrite primary key")?;

        if ttl_ms == 0 {
            return Err(Error::InvalidArgument(
                "prewrite lock TTL must be non-zero".to_string(),
            ));
        }

        let mut prepared = Vec::with_capacity(mutations.len());
        for (key, mutation) in mutations {
            if let Some(edit) =
                self.prepare_prewrite(txn_id, start_ts, key, mutation, primary_key, ttl_ms)?
            {
                prepared.push(edit);
            }
        }

        let mut delta = MvccDelta::default();
        for prepared in prepared {
            Self::append_prewrite_edit(&mut delta, prepared);
        }
        Ok(delta)
    }

    fn publish_mvcc_delta(&mut self, delta: MvccDelta) -> Result<()> {
        self.backend.publish_atomic(delta)
    }

    fn publish_mvcc_delta_with_reservation(
        &mut self,
        delta: MvccDelta,
        reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        self.backend
            .publish_atomic_with_reservation(delta, reservation)
    }

    fn command_generation_requires_freeze(
        &self,
        delta: &MvccDelta,
        metadata: &CommandGenerationMetadata,
    ) -> Result<bool> {
        self.backend
            .command_generation_requires_freeze(delta, metadata)
    }

    fn publish_command_generation_with_reservation(
        &mut self,
        delta: MvccDelta,
        metadata: CommandGenerationMetadata,
        reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        self.backend
            .publish_command_generation_with_reservation(delta, metadata, reservation)
    }

    fn install_restored_command_metadata(
        &mut self,
        metadata: CommandGenerationMetadata,
    ) -> Result<()> {
        self.backend.install_restored_command_metadata(metadata)
    }

    fn memtable_pressure(&self) -> Option<MemtablePressure> {
        self.backend.memtable_pressure()
    }

    fn node_memtable_budget(&self) -> Option<NodeMemtableBudget> {
        self.backend.node_memtable_budget()
    }

    fn commit_intent(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        key: &[u8],
    ) -> Result<()> {
        validate_commit_metadata(txn_id, start_ts, commit_ts)?;

        if let Some(prepared) = self.prepare_intent_commit(txn_id, start_ts, commit_ts, key)? {
            let mut delta = MvccDelta::default();
            Self::append_intent_commit_edit(&mut delta, prepared);
            self.backend.publish_atomic(delta)?;
        }

        Ok(())
    }

    fn commit_intents_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<()> {
        let delta = self.prepare_commit_intents_batch(txn_id, start_ts, commit_ts, keys)?;
        self.publish_mvcc_delta(delta)
    }

    fn prepare_commit_intents_batch(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<MvccDelta> {
        if keys.is_empty() {
            return Err(Error::InvalidArgument(
                "distributed commit batch must contain at least one key".to_string(),
            ));
        }

        validate_commit_metadata(txn_id, start_ts, commit_ts)?;

        let mut prepared = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(delta) = self.prepare_intent_commit(txn_id, start_ts, commit_ts, key)? {
                prepared.push(delta);
            }
        }

        let mut delta = MvccDelta::default();
        for prepared in prepared {
            Self::append_intent_commit_edit(&mut delta, prepared);
        }
        Ok(delta)
    }

    fn rollback_intent(&mut self, txn_id: TxnId, start_ts: Timestamp, key: &[u8]) -> Result<()> {
        validate_commit_preflight_metadata(txn_id, start_ts)?;

        if let Some(prepared) = self.prepare_intent_rollback(txn_id, start_ts, key)? {
            let mut delta = MvccDelta::default();
            Self::append_intent_rollback_edit(&mut delta, prepared, start_ts);
            self.backend.publish_atomic(delta)?;
        }

        Ok(())
    }

    fn rollback_intents_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<()> {
        let delta = self.prepare_rollback_intents_batch(txn_id, start_ts, keys)?;
        self.publish_mvcc_delta(delta)
    }

    fn prepare_rollback_intents_batch(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<MvccDelta> {
        if keys.is_empty() {
            return Err(Error::InvalidArgument(
                "distributed rollback batch must contain at least one key".to_string(),
            ));
        }

        validate_commit_preflight_metadata(txn_id, start_ts)?;

        let mut prepared = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(delta) = self.prepare_intent_rollback(txn_id, start_ts, key)? {
                prepared.push(delta);
            }
        }

        let mut delta = MvccDelta::default();
        for prepared in prepared {
            Self::append_intent_rollback_edit(&mut delta, prepared, start_ts);
        }
        Ok(delta)
    }

    fn prepare_prewrite_batch_with_overlay(
        &self,
        staged: &MvccDelta,
        txn_id: TxnId,
        start_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
        primary_key: &[u8],
        ttl_ms: u64,
    ) -> Result<MvccDelta> {
        self.overlay_engine(staged).prepare_prewrite_batch(
            txn_id,
            start_ts,
            mutations,
            primary_key,
            ttl_ms,
        )
    }

    fn prepare_commit_batch_with_overlay(
        &self,
        staged: &MvccDelta,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<MvccDelta> {
        self.overlay_engine(staged)
            .prepare_commit_batch(txn_id, start_ts, commit_ts, mutations)
    }

    fn prepare_commit_intents_batch_with_overlay(
        &self,
        staged: &MvccDelta,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<MvccDelta> {
        self.overlay_engine(staged)
            .prepare_commit_intents_batch(txn_id, start_ts, commit_ts, keys)
    }

    fn prepare_rollback_intents_batch_with_overlay(
        &self,
        staged: &MvccDelta,
        txn_id: TxnId,
        start_ts: Timestamp,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<MvccDelta> {
        self.overlay_engine(staged)
            .prepare_rollback_intents_batch(txn_id, start_ts, keys)
    }
}

impl MvccReadGeneration for MemtableMvccBackend {
    fn get_default(&self, key: &[u8], start_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        Ok(self.visible_default(key, start_ts).cloned())
    }

    fn get_lock(&self, key: &[u8]) -> Result<Option<LockRecord>> {
        Ok(self.visible_lock(key).cloned())
    }

    fn get_write(&self, key: &[u8], write_ts: Timestamp) -> Result<Option<WriteRecord>> {
        Ok(self.visible_write(key, write_ts).cloned())
    }

    fn write_page(
        &self,
        key: &[u8],
        lower: Bound<Timestamp>,
        upper: Bound<Timestamp>,
        resume_after: Option<Timestamp>,
        direction: MvccCursorDirection,
        max_records: usize,
    ) -> Result<MvccWritePage> {
        if max_records == 0 {
            return Err(Error::InvalidArgument(
                "MVCC write cursor max_records must be greater than zero".to_string(),
            ));
        }

        let all_writes = self.materialize_writes();
        let Some(versions) = all_writes.get(key) else {
            return Ok(MvccWritePage {
                writes: Vec::new(),
                has_more: false,
            });
        };

        let (lower, upper) = match (direction, resume_after) {
            (MvccCursorDirection::Forward, Some(timestamp)) => {
                (max_lower_bound(lower, Excluded(timestamp)), upper)
            }
            (MvccCursorDirection::Reverse, Some(timestamp)) => {
                (lower, min_upper_bound(upper, Excluded(timestamp)))
            }
            (_, None) => (lower, upper),
        };

        if timestamp_range_is_empty(&lower, &upper) {
            return Ok(MvccWritePage {
                writes: Vec::new(),
                has_more: false,
            });
        }

        let page_capacity = max_records.checked_add(1).ok_or_else(|| {
            Error::InvalidArgument("MVCC write cursor page size overflowed".to_string())
        })?;
        let mut writes = Vec::with_capacity(page_capacity);
        match direction {
            MvccCursorDirection::Forward => {
                for (timestamp, write) in versions.range((lower, upper)).take(page_capacity) {
                    writes.push((*timestamp, write.clone()));
                }
            }
            MvccCursorDirection::Reverse => {
                for (timestamp, write) in versions.range((lower, upper)).rev().take(page_capacity) {
                    writes.push((*timestamp, write.clone()));
                }
            }
        }

        let has_more = writes.len() > max_records;
        writes.truncate(max_records);
        Ok(MvccWritePage { writes, has_more })
    }

    fn key_page(
        &self,
        family: MvccKeyFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_keys: usize,
    ) -> Result<MvccKeyPage> {
        if max_keys == 0 {
            return Err(Error::InvalidArgument(
                "MVCC key cursor max_keys must be greater than zero".to_string(),
            ));
        }

        let (_, upper) = encoded_scan_bounds(start, end)?;
        if let Some(resume_after) = resume_after {
            validate_encoded_key_argument(resume_after, "MVCC key cursor resume key")?;
            if end.is_some_and(|end| resume_after >= end) {
                return Ok(MvccKeyPage {
                    keys: Vec::new(),
                    has_more: false,
                });
            }
        }

        let lower = scan_lower_bound(start, resume_after);
        let page_capacity = max_keys.checked_add(1).ok_or_else(|| {
            Error::InvalidArgument("MVCC key cursor page size overflowed".to_string())
        })?;
        let mut keys = Vec::with_capacity(page_capacity);
        match family {
            MvccKeyFamily::Writes => {
                let records = self.materialize_writes();
                for key in records.range((lower, upper)).map(|(key, _)| key) {
                    validate_encoded_key_argument(key, "MVCC write cursor key")?;
                    keys.push(key.clone());
                    if keys.len() > max_keys {
                        break;
                    }
                }
            }
            MvccKeyFamily::Locks => {
                let records = self.materialize_locks();
                for key in records.range((lower, upper)).map(|(key, _)| key) {
                    validate_encoded_key_argument(key, "MVCC lock cursor key")?;
                    keys.push(key.clone());
                    if keys.len() > max_keys {
                        break;
                    }
                }
            }
        }

        let has_more = keys.len() > max_keys;
        keys.truncate(max_keys);
        Ok(MvccKeyPage { keys, has_more })
    }

    fn lock_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_locks: usize,
        max_bytes: usize,
    ) -> Result<IntentScanPage> {
        if max_locks == 0 || max_bytes == 0 {
            return Err(Error::InvalidArgument(
                "MVCC lock cursor limits must be greater than zero".to_string(),
            ));
        }

        let (_, upper) = encoded_scan_bounds(start, end)?;
        if let Some(resume_after) = resume_after {
            validate_encoded_key_argument(resume_after, "MVCC lock cursor resume key")?;
            if end.is_some_and(|end| resume_after >= end) {
                return Ok(IntentScanPage {
                    locks: Vec::new(),
                    has_more: false,
                });
            }
        }

        let lower = scan_lower_bound(start, resume_after);
        let mut locks = Vec::new();
        let mut encoded_bytes = 0usize;
        let current_locks = self.materialize_locks();
        for (key, lock) in current_locks.range((lower, upper)) {
            if locks.len() >= max_locks {
                return Ok(IntentScanPage {
                    locks,
                    has_more: true,
                });
            }

            let lock_bytes = lock
                .to_proto()
                .map_err(|error| Error::CorruptData(error.to_string()))?
                .encoded_len();
            let entry_bytes = key.len().checked_add(lock_bytes).ok_or_else(|| {
                Error::InvalidArgument("MVCC lock cursor byte count overflowed".to_string())
            })?;
            let next_bytes = encoded_bytes.checked_add(entry_bytes).ok_or_else(|| {
                Error::InvalidArgument("MVCC lock cursor byte count overflowed".to_string())
            })?;
            if next_bytes > max_bytes {
                if locks.is_empty() {
                    return Err(Error::InvalidArgument(
                        "MVCC lock cursor byte budget is smaller than its first record".to_string(),
                    ));
                }
                return Ok(IntentScanPage {
                    locks,
                    has_more: true,
                });
            }

            encoded_bytes = next_bytes;
            locks.push((key.clone(), lock.clone()));
        }

        Ok(IntentScanPage {
            locks,
            has_more: false,
        })
    }

    fn recovery_frontier(&self) -> Result<Option<RecoveryFrontier>> {
        Ok(None)
    }

    fn export_snapshot(&self) -> Result<CapturedMvccState> {
        let materialized_default = self.materialize_default();
        let materialized_locks = self.materialize_locks();
        let materialized_writes = self.materialize_writes();
        let default_values = materialized_default
            .iter()
            .flat_map(|(key, versions)| {
                versions.iter().map(move |(start_timestamp, row)| {
                    snapshot_proto::DefaultValueEntry {
                        key: key.clone(),
                        start_timestamp: Some(start_timestamp.to_proto()),
                        row: row.clone(),
                    }
                })
            })
            .collect();

        let locks = materialized_locks
            .iter()
            .map(|(key, record)| {
                record.validate().map_err(|error| {
                    Error::CorruptData(format!("in-memory lock cannot be exported: {error}"))
                })?;
                Ok(snapshot_proto::LockEntry {
                    key: key.clone(),
                    record: Some(record.to_proto().map_err(|error| {
                        Error::CorruptData(format!("in-memory lock cannot be exported: {error}"))
                    })?),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let writes = materialized_writes
            .iter()
            .flat_map(|(key, versions)| {
                versions.iter().map(move |(write_timestamp, record)| {
                    validate_write_record(*write_timestamp, record)?;
                    Ok(snapshot_proto::WriteEntry {
                        key: key.clone(),
                        write_timestamp: Some(write_timestamp.to_proto()),
                        record: Some(record.to_proto().map_err(|error| {
                            Error::CorruptData(format!(
                                "in-memory write cannot be exported: {error}"
                            ))
                        })?),
                    })
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(CapturedMvccState::new(default_values, locks, writes))
    }
}

impl<B: MvccBackend> MvccReadGeneration for DeltaOverlayBackend<'_, B> {
    fn get_default(&self, key: &[u8], start_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        for edit in self.staged.edits.iter().rev() {
            match edit {
                MvccRecordEdit::PutDefault {
                    key: edit_key,
                    start_ts: edit_ts,
                    row,
                } if edit_key == key && *edit_ts == start_ts => return Ok(Some(row.clone())),
                MvccRecordEdit::DeleteDefault {
                    key: edit_key,
                    start_ts: edit_ts,
                } if edit_key == key && *edit_ts == start_ts => return Ok(None),
                _ => {}
            }
        }
        self.base.get_default(key, start_ts)
    }

    fn get_lock(&self, key: &[u8]) -> Result<Option<LockRecord>> {
        for edit in self.staged.edits.iter().rev() {
            match edit {
                MvccRecordEdit::PutLock {
                    key: edit_key,
                    lock,
                } if edit_key == key => return Ok(Some(lock.clone())),
                MvccRecordEdit::DeleteLock { key: edit_key } if edit_key == key => {
                    return Ok(None);
                }
                _ => {}
            }
        }
        self.base.get_lock(key)
    }

    fn get_write(&self, key: &[u8], write_ts: Timestamp) -> Result<Option<WriteRecord>> {
        if let Some(write) = self.staged.edits.iter().rev().find_map(|edit| match edit {
            MvccRecordEdit::PutWrite {
                key: edit_key,
                write_ts: edit_ts,
                write,
            } if edit_key == key && *edit_ts == write_ts => Some(write.clone()),
            _ => None,
        }) {
            return Ok(Some(write));
        }
        self.base.get_write(key, write_ts)
    }

    fn write_page(
        &self,
        key: &[u8],
        lower: Bound<Timestamp>,
        upper: Bound<Timestamp>,
        resume_after: Option<Timestamp>,
        direction: MvccCursorDirection,
        max_records: usize,
    ) -> Result<MvccWritePage> {
        if max_records == 0 {
            return Err(Error::InvalidArgument(
                "MVCC write cursor max_records must be greater than zero".to_string(),
            ));
        }

        let staged_writes = self
            .staged
            .edits
            .iter()
            .filter_map(|edit| match edit {
                MvccRecordEdit::PutWrite {
                    key: edit_key,
                    write_ts,
                    write,
                } if edit_key == key && timestamp_in_bounds(*write_ts, &lower, &upper) => {
                    let after_resume = match (direction, resume_after) {
                        (MvccCursorDirection::Forward, Some(resume)) => *write_ts > resume,
                        (MvccCursorDirection::Reverse, Some(resume)) => *write_ts < resume,
                        (_, None) => true,
                    };
                    after_resume.then_some((*write_ts, write.clone()))
                }
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        let base_limit = max_records
            .checked_add(staged_writes.len())
            .ok_or_else(|| {
                Error::InvalidArgument("MVCC overlay page size overflowed".to_string())
            })?;
        let base_page =
            self.base
                .write_page(key, lower, upper, resume_after, direction, base_limit)?;

        let mut merged = base_page.writes.into_iter().collect::<BTreeMap<_, _>>();
        merged.extend(staged_writes);
        let mut writes = merged
            .into_iter()
            .filter(|(timestamp, _)| {
                timestamp_in_bounds(*timestamp, &lower, &upper)
                    && match (direction, resume_after) {
                        (MvccCursorDirection::Forward, Some(resume)) => *timestamp > resume,
                        (MvccCursorDirection::Reverse, Some(resume)) => *timestamp < resume,
                        (_, None) => true,
                    }
            })
            .collect::<Vec<_>>();
        if direction == MvccCursorDirection::Reverse {
            writes.reverse();
        }
        let has_more = base_page.has_more || writes.len() > max_records;
        writes.truncate(max_records);
        Ok(MvccWritePage { writes, has_more })
    }

    fn key_page(
        &self,
        family: MvccKeyFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_keys: usize,
    ) -> Result<MvccKeyPage> {
        self.base
            .key_page(family, start, end, resume_after, max_keys)
    }

    fn lock_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_locks: usize,
        max_bytes: usize,
    ) -> Result<IntentScanPage> {
        self.base
            .lock_page(start, end, resume_after, max_locks, max_bytes)
    }

    fn recovery_frontier(&self) -> Result<Option<RecoveryFrontier>> {
        self.base.recovery_frontier()
    }

    fn export_snapshot(&self) -> Result<CapturedMvccState> {
        self.base.export_snapshot()
    }
}

impl<B: MvccBackend> MvccBackend for DeltaOverlayBackend<'_, B> {
    // This private preparation view is never pinned or published. Keeping the
    // associated type valid lets it reuse the shared MVCC preparation rules.
    type PinnedGeneration = B::PinnedGeneration;

    fn publish_atomic(&mut self, _delta: MvccDelta) -> Result<()> {
        Err(Error::NotImplemented(
            "a private MVCC command overlay cannot publish independently",
        ))
    }

    fn pin_generation(&self) -> Result<Self::PinnedGeneration> {
        Err(Error::NotImplemented(
            "a private MVCC command overlay cannot be pinned",
        ))
    }

    fn stats(&self) -> MvccStats {
        self.base.stats()
    }

    fn allocator_high_water_marks(&self) -> (TxnId, Timestamp) {
        self.base.allocator_high_water_marks()
    }
}

fn timestamp_in_bounds(
    timestamp: Timestamp,
    lower: &Bound<Timestamp>,
    upper: &Bound<Timestamp>,
) -> bool {
    let above_lower = match lower {
        Bound::Included(bound) => timestamp >= *bound,
        Bound::Excluded(bound) => timestamp > *bound,
        Bound::Unbounded => true,
    };
    let below_upper = match upper {
        Bound::Included(bound) => timestamp <= *bound,
        Bound::Excluded(bound) => timestamp < *bound,
        Bound::Unbounded => true,
    };
    above_lower && below_upper
}

impl MemtableMvccBackend {
    fn with_memtable_budget(budget: NodeMemtableBudget, max_active_bytes: usize) -> Result<Self> {
        if max_active_bytes == 0 {
            return Err(Error::InvalidArgument(
                "tablet active memtable limit must be greater than zero".to_string(),
            ));
        }

        let (soft_bytes, hard_bytes) = immutable_memtable_limits(max_active_bytes)?;
        Ok(Self {
            memtable_budget: Some(budget),
            memtable_limit_bytes: max_active_bytes,
            immutable_memtable_soft_limit_bytes: soft_bytes,
            immutable_memtable_limit_bytes: hard_bytes,
            immutable_memtable_soft_count_limit: DEFAULT_TABLET_IMMUTABLE_SOFT_COUNT,
            immutable_memtable_count_limit: DEFAULT_TABLET_IMMUTABLE_HARD_COUNT,
            next_immutable_generation_id: 1,
            ..Self::default()
        })
    }

    fn bind_memtable_budget(
        &mut self,
        budget: NodeMemtableBudget,
        max_active_bytes: usize,
    ) -> Result<()> {
        if max_active_bytes == 0 {
            return Err(Error::InvalidArgument(
                "tablet active memtable limit must be greater than zero".to_string(),
            ));
        }
        if self.memtable_budget.is_some() || self.memtable_charge.is_some() {
            return Err(Error::InvalidArgument(
                "MVCC storage already has an active memtable budget".to_string(),
            ));
        }

        let current_bytes = self.current_memtable_bytes()?;
        let (soft_bytes, hard_bytes) = immutable_memtable_limits(max_active_bytes)?;
        let charge = if current_bytes == 0 {
            None
        } else {
            Some(
                budget
                    .reserve(MemoryClass::Progress, current_bytes)?
                    .commit()?,
            )
        };

        let restored_base = Some(ImmutableMemtableGeneration {
            id: 0,
            default: std::mem::take(&mut self.default),
            locks: std::mem::take(&mut self.locks),
            writes: std::mem::take(&mut self.writes),
            default_tombstones: std::mem::take(&mut self.default_tombstones),
            lock_tombstones: std::mem::take(&mut self.lock_tombstones),
            command_metadata: std::mem::take(&mut self.active_command_metadata),
            frontier: None,
            _charge: charge,
        });
        self.memtable_budget = Some(budget);
        self.memtable_limit_bytes = max_active_bytes;
        self.immutable_memtable_soft_limit_bytes = soft_bytes;
        self.immutable_memtable_limit_bytes = hard_bytes;
        self.immutable_memtable_soft_count_limit = DEFAULT_TABLET_IMMUTABLE_SOFT_COUNT;
        self.immutable_memtable_count_limit = DEFAULT_TABLET_IMMUTABLE_HARD_COUNT;
        self.restored_base = restored_base;
        self.memtable_charge = None;
        Ok(())
    }

    fn memtable_pressure(&self) -> Option<MemtablePressure> {
        let budget = self.memtable_budget.as_ref()?;
        Some(MemtablePressure {
            active_bytes: self
                .memtable_charge
                .as_ref()
                .map_or(0, MemtableCharge::bytes),
            active_limit_bytes: self.memtable_limit_bytes,
            immutable_memtable_bytes: self.immutable_memtable_bytes,
            immutable_memtable_limit_bytes: self.immutable_memtable_soft_limit_bytes,
            immutable_memtable_hard_limit_bytes: self.immutable_memtable_limit_bytes,
            immutable_memtable_count: self.immutable_memtables.len(),
            immutable_memtable_count_limit: self.immutable_memtable_soft_count_limit,
            immutable_memtable_hard_count_limit: self.immutable_memtable_count_limit,
            node_used_bytes: budget.used_bytes(),
            node_limit_bytes: budget.limit_bytes(),
            node_user_used_bytes: budget.user_used_bytes(),
            node_user_limit_bytes: budget.user_limit_bytes(),
        })
    }

    fn command_generation_requires_freeze(
        &self,
        delta: &MvccDelta,
        metadata: &CommandGenerationMetadata,
    ) -> Result<bool> {
        if self.memtable_budget.is_none() {
            return Ok(false);
        }
        Ok(
            self.projected_generation_bytes(delta, Some(metadata), false)?
                > self.memtable_limit_bytes,
        )
    }

    fn set_immutable_memtable_sink(&mut self, sink: Arc<dyn ImmutableMemtableSink>) {
        self.immutable_sink = Some(sink);
    }

    fn immutable_generations(&self) -> Vec<Arc<ImmutableMemtableGeneration>> {
        self.immutable_memtables.iter().cloned().collect()
    }

    fn try_handoff_pending_immutables(&mut self) -> std::result::Result<usize, FlushHandoffError> {
        let Some(sink) = self.immutable_sink.clone() else {
            return Ok(0);
        };
        let pending = self
            .immutable_memtables
            .iter()
            .filter(|generation| !self.handed_off_immutables.contains(&generation.id))
            .cloned()
            .collect::<Vec<_>>();
        let mut submitted = 0usize;
        for generation in pending {
            if let Err(error) = sink.try_submit(Arc::clone(&generation)) {
                match error {
                    FlushHandoffError::Full => return Err(FlushHandoffError::Full),
                    permanent @ (FlushHandoffError::Closed | FlushHandoffError::Rejected(_)) => {
                        self.last_flush_handoff_error = Some(permanent.clone());
                        return Err(permanent);
                    }
                }
            }
            self.handed_off_immutables.insert(generation.id);
            submitted = submitted.checked_add(1).ok_or_else(|| {
                FlushHandoffError::Rejected("handoff submission count overflowed".to_string())
            })?;
        }
        self.last_flush_handoff_error = None;
        Ok(submitted)
    }

    #[cfg(test)]
    fn retire_immutable_generation_for_test(&mut self, generation_id: u64) -> Result<()> {
        let index = self
            .immutable_memtables
            .iter()
            .position(|generation| generation.id == generation_id)
            .ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "immutable memtable generation {generation_id} is not retained"
                ))
            })?;
        if !self.handed_off_immutables.contains(&generation_id) {
            return Err(Error::InvalidArgument(format!(
                "immutable memtable generation {generation_id} has not been handed off"
            )));
        }
        let generation = &self.immutable_memtables[index];
        let charge = generation._charge.as_ref().ok_or_else(|| {
            Error::CorruptData("retained immutable generation has no memory charge".to_string())
        })?;
        let bytes = charge.bytes();
        let next_bytes = self
            .immutable_memtable_bytes
            .checked_sub(bytes)
            .ok_or_else(|| Error::CorruptData("immutable memtable debt underflowed".to_string()))?;
        charge.release()?;
        self.immutable_memtables.remove(index);
        self.handed_off_immutables.remove(&generation_id);
        self.immutable_memtable_bytes = next_bytes;
        Ok(())
    }

    fn immutable_has_default(&self, key: &[u8], start_ts: Timestamp) -> bool {
        for memtable in self.immutable_memtables.iter().rev() {
            let identity = (key.to_vec(), start_ts);
            if memtable.default_tombstones.contains(&identity) {
                return false;
            }
            if memtable
                .default
                .get(key)
                .is_some_and(|versions| versions.contains_key(&start_ts))
            {
                return true;
            }
        }
        if let Some(base) = &self.restored_base {
            let identity = (key.to_vec(), start_ts);
            if base.default_tombstones.contains(&identity) {
                return false;
            }
            return base
                .default
                .get(key)
                .is_some_and(|versions| versions.contains_key(&start_ts));
        }
        false
    }

    fn immutable_has_lock(&self, key: &[u8]) -> bool {
        for memtable in self.immutable_memtables.iter().rev() {
            if memtable.lock_tombstones.contains(key) {
                return false;
            }
            if memtable.locks.contains_key(key) {
                return true;
            }
        }
        let Some(base) = &self.restored_base else {
            return false;
        };
        !base.lock_tombstones.contains(key) && base.locks.contains_key(key)
    }

    fn visible_default(&self, key: &[u8], start_ts: Timestamp) -> Option<&Vec<u8>> {
        if self.default_tombstones.contains(&(key.to_vec(), start_ts)) {
            return None;
        }
        if let Some(row) = self
            .default
            .get(key)
            .and_then(|versions| versions.get(&start_ts))
        {
            return Some(row);
        }
        for memtable in self.immutable_memtables.iter().rev() {
            if memtable
                .default_tombstones
                .contains(&(key.to_vec(), start_ts))
            {
                return None;
            }
            if let Some(row) = memtable
                .default
                .get(key)
                .and_then(|versions| versions.get(&start_ts))
            {
                return Some(row);
            }
        }
        let base = self.restored_base.as_ref()?;
        if base.default_tombstones.contains(&(key.to_vec(), start_ts)) {
            return None;
        }
        base.default
            .get(key)
            .and_then(|versions| versions.get(&start_ts))
    }

    fn visible_lock(&self, key: &[u8]) -> Option<&LockRecord> {
        if self.lock_tombstones.contains(key) {
            return None;
        }
        if let Some(lock) = self.locks.get(key) {
            return Some(lock);
        }
        for memtable in self.immutable_memtables.iter().rev() {
            if memtable.lock_tombstones.contains(key) {
                return None;
            }
            if let Some(lock) = memtable.locks.get(key) {
                return Some(lock);
            }
        }
        let base = self.restored_base.as_ref()?;
        if base.lock_tombstones.contains(key) {
            return None;
        }
        base.locks.get(key)
    }

    fn visible_write(&self, key: &[u8], write_ts: Timestamp) -> Option<&WriteRecord> {
        if let Some(write) = self
            .writes
            .get(key)
            .and_then(|versions| versions.get(&write_ts))
        {
            return Some(write);
        }
        self.immutable_memtables
            .iter()
            .rev()
            .find_map(|memtable| {
                memtable
                    .writes
                    .get(key)
                    .and_then(|versions| versions.get(&write_ts))
            })
            .or_else(|| {
                self.restored_base
                    .as_ref()?
                    .writes
                    .get(key)
                    .and_then(|versions| versions.get(&write_ts))
            })
    }

    fn projected_generation_bytes(
        &self,
        delta: &MvccDelta,
        incoming_metadata: Option<&CommandGenerationMetadata>,
        after_freeze: bool,
    ) -> Result<usize> {
        let mut projected = if after_freeze {
            self.projected_after_freeze(delta)?
        } else {
            self.projected_memtable_bytes(delta)?
        };
        let current_metadata_bytes = self
            .active_command_metadata
            .as_ref()
            .map(command_generation_metadata_charge)
            .transpose()?
            .unwrap_or(0);
        let next_metadata_bytes = match incoming_metadata {
            Some(incoming) if after_freeze => command_generation_metadata_charge(incoming)?,
            Some(incoming) => {
                let mut merged = self
                    .active_command_metadata
                    .clone()
                    .unwrap_or_else(|| incoming.clone());
                if self.active_command_metadata.is_some() {
                    merged.absorb(incoming.clone());
                }
                command_generation_metadata_charge(&merged)?
            }
            None if after_freeze => 0,
            None => current_metadata_bytes,
        };
        if !after_freeze {
            projected = projected
                .checked_sub(current_metadata_bytes)
                .ok_or_else(|| {
                    Error::CorruptData("active command metadata charge underflowed".to_string())
                })?;
        }
        projected
            .checked_add(next_metadata_bytes)
            .ok_or_else(|| Error::TabletUnavailable {
                reason: "active command metadata byte accounting overflowed".to_string(),
            })
    }

    fn projected_after_freeze(&self, delta: &MvccDelta) -> Result<usize> {
        let mut projected = 0usize;
        for edit in &delta.edits {
            match edit {
                MvccRecordEdit::PutDefault { key, row, .. } => {
                    add_memtable_record_charge(&mut projected, key.len(), row.len())?;
                }
                MvccRecordEdit::DeleteDefault { key, start_ts } => {
                    if self.visible_default(key, *start_ts).is_some() {
                        add_memtable_record_charge(&mut projected, key.len(), 0)?;
                    }
                }
                MvccRecordEdit::PutLock { key, lock } => {
                    add_memtable_record_charge(
                        &mut projected,
                        key.len(),
                        memtable_lock_payload_size(lock)?,
                    )?;
                }
                MvccRecordEdit::DeleteLock { key } => {
                    if self.visible_lock(key).is_some() {
                        add_memtable_record_charge(&mut projected, key.len(), 0)?;
                    }
                }
                MvccRecordEdit::PutWrite { key, write, .. } => {
                    add_memtable_record_charge(
                        &mut projected,
                        key.len(),
                        memtable_write_payload_size(write)?,
                    )?;
                }
            }
        }
        Ok(projected)
    }

    fn validate_freeze_capacity(&self, bytes: usize) -> Result<usize> {
        let next_bytes = self
            .immutable_memtable_bytes
            .checked_add(bytes)
            .ok_or_else(|| Error::TabletUnavailable {
                reason: "immutable memtable byte accounting overflowed".to_string(),
            })?;
        if self.immutable_memtables.len() >= self.immutable_memtable_count_limit
            || next_bytes > self.immutable_memtable_limit_bytes
        {
            return Err(Error::TabletUnavailable {
                reason: "immutable memtable hard progress capacity is exhausted".to_string(),
            });
        }
        Ok(next_bytes)
    }

    fn freeze_active_memtable(&mut self) -> Result<()> {
        let bytes = self
            .memtable_charge
            .as_ref()
            .map_or(0, MemtableCharge::bytes);
        if bytes == 0 {
            return Ok(());
        }
        let next_bytes = self.validate_freeze_capacity(bytes)?;
        let next_id = self
            .next_immutable_generation_id
            .checked_add(1)
            .ok_or_else(|| Error::TabletUnavailable {
                reason: "immutable memtable generation identity overflowed".to_string(),
            })?;
        let command_metadata = std::mem::take(&mut self.active_command_metadata);
        let frontier = command_metadata.as_ref().map(|metadata| metadata.frontier);
        let frozen = Arc::new(ImmutableMemtableGeneration {
            id: self.next_immutable_generation_id,
            default: std::mem::take(&mut self.default),
            locks: std::mem::take(&mut self.locks),
            writes: std::mem::take(&mut self.writes),
            default_tombstones: std::mem::take(&mut self.default_tombstones),
            lock_tombstones: std::mem::take(&mut self.lock_tombstones),
            command_metadata,
            frontier,
            _charge: self.memtable_charge.take(),
        });
        self.immutable_memtable_bytes = next_bytes;
        self.next_immutable_generation_id = next_id;
        self.immutable_memtables.push_back(frozen);
        Ok(())
    }

    fn materialize_default(&self) -> BTreeMap<Vec<u8>, BTreeMap<Timestamp, Vec<u8>>> {
        let mut records = BTreeMap::new();
        if let Some(base) = &self.restored_base {
            apply_default_generation(&mut records, &base.default, &base.default_tombstones);
        }
        for memtable in &self.immutable_memtables {
            apply_default_generation(
                &mut records,
                &memtable.default,
                &memtable.default_tombstones,
            );
        }
        apply_default_generation(&mut records, &self.default, &self.default_tombstones);
        records
    }

    fn materialize_locks(&self) -> BTreeMap<Vec<u8>, LockRecord> {
        let mut records = BTreeMap::new();
        if let Some(base) = &self.restored_base {
            for key in &base.lock_tombstones {
                records.remove(key);
            }
            records.extend(base.locks.clone());
        }
        for memtable in &self.immutable_memtables {
            for key in &memtable.lock_tombstones {
                records.remove(key);
            }
            records.extend(memtable.locks.clone());
        }
        for key in &self.lock_tombstones {
            records.remove(key);
        }
        records.extend(self.locks.clone());
        records
    }

    fn materialize_writes(&self) -> BTreeMap<Vec<u8>, BTreeMap<Timestamp, WriteRecord>> {
        let mut records = BTreeMap::new();
        if let Some(base) = &self.restored_base {
            merge_write_generation(&mut records, &base.writes);
        }
        for memtable in &self.immutable_memtables {
            merge_write_generation(&mut records, &memtable.writes);
        }
        merge_write_generation(&mut records, &self.writes);
        records
    }

    fn projected_memtable_bytes(&self, delta: &MvccDelta) -> Result<usize> {
        let mut projected = self
            .memtable_charge
            .as_ref()
            .map_or(0, MemtableCharge::bytes);

        for edit in &delta.edits {
            match edit {
                MvccRecordEdit::PutDefault { key, start_ts, row } => {
                    let previous = self
                        .default
                        .get(key)
                        .and_then(|versions| versions.get(start_ts))
                        .map(Vec::len);
                    replace_memtable_record_charge(
                        &mut projected,
                        key.len(),
                        previous,
                        Some(row.len()),
                    )?;
                    if self.default_tombstones.contains(&(key.clone(), *start_ts)) {
                        replace_memtable_record_charge(&mut projected, key.len(), Some(0), None)?;
                    }
                }
                MvccRecordEdit::DeleteDefault { key, start_ts } => {
                    let previous = self
                        .default
                        .get(key)
                        .and_then(|versions| versions.get(start_ts))
                        .map(Vec::len);
                    replace_memtable_record_charge(&mut projected, key.len(), previous, None)?;
                    if self.immutable_has_default(key, *start_ts)
                        && !self.default_tombstones.contains(&(key.clone(), *start_ts))
                    {
                        add_memtable_record_charge(&mut projected, key.len(), 0)?;
                    }
                }
                MvccRecordEdit::PutLock { key, lock } => {
                    let previous = self
                        .locks
                        .get(key)
                        .map(memtable_lock_payload_size)
                        .transpose()?;
                    replace_memtable_record_charge(
                        &mut projected,
                        key.len(),
                        previous,
                        Some(memtable_lock_payload_size(lock)?),
                    )?;
                    if self.lock_tombstones.contains(key) {
                        replace_memtable_record_charge(&mut projected, key.len(), Some(0), None)?;
                    }
                }
                MvccRecordEdit::DeleteLock { key } => {
                    let previous = self
                        .locks
                        .get(key)
                        .map(memtable_lock_payload_size)
                        .transpose()?;
                    replace_memtable_record_charge(&mut projected, key.len(), previous, None)?;
                    if self.immutable_has_lock(key) && !self.lock_tombstones.contains(key) {
                        add_memtable_record_charge(&mut projected, key.len(), 0)?;
                    }
                }
                MvccRecordEdit::PutWrite {
                    key,
                    write_ts,
                    write,
                } => {
                    let previous = self
                        .writes
                        .get(key)
                        .and_then(|versions| versions.get(write_ts))
                        .map(memtable_write_payload_size)
                        .transpose()?;
                    replace_memtable_record_charge(
                        &mut projected,
                        key.len(),
                        previous,
                        Some(memtable_write_payload_size(write)?),
                    )?;
                }
            }
        }

        Ok(projected)
    }

    fn current_memtable_bytes(&self) -> Result<usize> {
        let mut bytes = 0usize;
        for (key, versions) in &self.default {
            for row in versions.values() {
                add_memtable_record_charge(&mut bytes, key.len(), row.len())?;
            }
        }
        for (key, lock) in &self.locks {
            add_memtable_record_charge(&mut bytes, key.len(), memtable_lock_payload_size(lock)?)?;
        }
        for (key, versions) in &self.writes {
            for write in versions.values() {
                add_memtable_record_charge(
                    &mut bytes,
                    key.len(),
                    memtable_write_payload_size(write)?,
                )?;
            }
        }
        for (key, _) in &self.default_tombstones {
            add_memtable_record_charge(&mut bytes, key.len(), 0)?;
        }
        for key in &self.lock_tombstones {
            add_memtable_record_charge(&mut bytes, key.len(), 0)?;
        }
        if let Some(metadata) = &self.active_command_metadata {
            bytes = bytes
                .checked_add(command_generation_metadata_charge(metadata)?)
                .ok_or_else(|| Error::TabletUnavailable {
                    reason: "active command metadata byte accounting overflowed".to_string(),
                })?;
        }
        Ok(bytes)
    }

    fn publish_memtable_charge(
        &mut self,
        next_bytes: usize,
        growth_bytes: usize,
        reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        let current_bytes = self
            .memtable_charge
            .as_ref()
            .map_or(0, MemtableCharge::bytes);
        match (self.memtable_charge.as_ref(), reservation) {
            (Some(charge), Some(reservation)) => {
                reservation.commit_into_amount(charge, growth_bytes)?;
            }
            (None, Some(reservation)) => {
                self.memtable_charge = reservation.commit_amount(growth_bytes)?;
            }
            (Some(charge), None) if next_bytes < current_bytes => {
                charge.try_shrink(current_bytes - next_bytes)?;
            }
            _ => {}
        }

        if next_bytes == 0 {
            self.memtable_charge = None;
        }
        Ok(())
    }
}

fn replace_memtable_record_charge(
    total: &mut usize,
    key_bytes: usize,
    previous_value_bytes: Option<usize>,
    next_value_bytes: Option<usize>,
) -> Result<()> {
    if let Some(value_bytes) = previous_value_bytes {
        let previous = memtable_record_charge(key_bytes, value_bytes)?;
        *total = total.checked_sub(previous).ok_or_else(|| {
            Error::CorruptData("active memtable byte accounting underflowed".to_string())
        })?;
    }
    if let Some(value_bytes) = next_value_bytes {
        *total = total
            .checked_add(memtable_record_charge(key_bytes, value_bytes)?)
            .ok_or_else(|| Error::TabletUnavailable {
                reason: "active memtable byte accounting overflowed".to_string(),
            })?;
    }
    Ok(())
}

fn add_memtable_record_charge(
    total: &mut usize,
    key_bytes: usize,
    value_bytes: usize,
) -> Result<()> {
    *total = total
        .checked_add(memtable_record_charge(key_bytes, value_bytes)?)
        .ok_or_else(|| Error::TabletUnavailable {
            reason: "active memtable byte accounting overflowed".to_string(),
        })?;
    Ok(())
}

fn memtable_record_charge(key_bytes: usize, value_bytes: usize) -> Result<usize> {
    key_bytes
        .checked_add(value_bytes)
        .and_then(|bytes| bytes.checked_add(MEMTABLE_INDEX_ENTRY_OVERHEAD_BYTES))
        .ok_or_else(|| Error::TabletUnavailable {
            reason: "active memtable record charge overflowed".to_string(),
        })
}

fn command_generation_metadata_charge(metadata: &CommandGenerationMetadata) -> Result<usize> {
    let mut total = std::mem::size_of::<CommandGenerationMetadata>();
    add_memtable_record_charge(&mut total, 0, 0)?;

    for edit in &metadata.transaction_status_edits {
        let ragnordb_storage_status = match edit {
            crate::lsm::TxnStatusEdit::Put { status, .. } => status,
        };
        let participants = ragnordb_storage_status
            .participant_tablet_ids
            .len()
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(metadata_charge_overflow)?;
        let payload = std::mem::size_of_val(ragnordb_storage_status)
            .checked_add(ragnordb_storage_status.primary_key.len())
            .and_then(|bytes| bytes.checked_add(participants))
            .ok_or_else(metadata_charge_overflow)?;
        add_memtable_record_charge(&mut total, std::mem::size_of::<TxnId>(), payload)?;
    }
    for edit in &metadata.logical_outcome_edits {
        let (id, outcome) = match edit {
            crate::lsm::LogicalOutcomeEdit::Put { id, outcome } => (Some(id), Some(outcome)),
            crate::lsm::LogicalOutcomeEdit::Delete { id } => (Some(id), None),
        };
        let key_bytes = id.map_or(0, std::mem::size_of_val);
        let value_bytes = outcome
            .map(cached_outcome_payload_charge)
            .transpose()?
            .unwrap_or(0);
        add_memtable_record_charge(&mut total, key_bytes, value_bytes)?;
    }
    for edit in &metadata.legacy_outcome_edits {
        let (client_id, outcome) = match edit {
            crate::lsm::LegacyOutcomeEdit::Put {
                client_id, outcome, ..
            } => (*client_id, outcome),
        };
        add_memtable_record_charge(
            &mut total,
            std::mem::size_of_val(&client_id),
            cached_outcome_payload_charge(outcome)?,
        )?;
    }
    for _ in &metadata.retry_floor_edits {
        add_memtable_record_charge(
            &mut total,
            std::mem::size_of::<(u128, u64)>(),
            std::mem::size_of::<u64>(),
        )?;
    }
    Ok(total)
}

fn cached_outcome_payload_charge(
    outcome: &ragnordb_common::command_codec::CachedTabletCommandOutcome,
) -> Result<usize> {
    let diagnostic_bytes = match outcome {
        ragnordb_common::command_codec::CachedTabletCommandOutcome::Applied(_) => 0,
        ragnordb_common::command_codec::CachedTabletCommandOutcome::Rejected(rejection) => {
            rejection.reason.len()
        }
    };
    std::mem::size_of_val(outcome)
        .checked_add(diagnostic_bytes)
        .ok_or_else(metadata_charge_overflow)
}

fn metadata_charge_overflow() -> Error {
    Error::TabletUnavailable {
        reason: "command metadata memtable accounting overflowed".to_string(),
    }
}

fn validate_mvcc_delta(delta: &MvccDelta) -> Result<()> {
    let mut changed_records = BTreeSet::new();
    for edit in &delta.edits {
        let (family, key, timestamp) = match edit {
            MvccRecordEdit::PutDefault { key, start_ts, row } => {
                validate_encoded_key_argument(key, "MVCC default edit key")?;
                decode_row(row).map_err(|error| {
                    Error::InvalidArgument(format!(
                        "default edit does not contain a canonical encoded row: {error}"
                    ))
                })?;
                (0, key, start_ts.0)
            }
            MvccRecordEdit::DeleteDefault { key, start_ts } => {
                validate_encoded_key_argument(key, "MVCC default edit key")?;
                (0, key, start_ts.0)
            }
            MvccRecordEdit::PutLock { key, lock } => {
                validate_encoded_key_argument(key, "MVCC lock edit key")?;
                lock.validate().map_err(|error| {
                    Error::CorruptData(format!("MVCC lock edit is invalid: {error}"))
                })?;
                (1, key, 0)
            }
            MvccRecordEdit::DeleteLock { key } => {
                validate_encoded_key_argument(key, "MVCC lock edit key")?;
                (1, key, 0)
            }
            MvccRecordEdit::PutWrite {
                key,
                write_ts,
                write,
            } => {
                validate_encoded_key_argument(key, "MVCC write edit key")?;
                validate_write_record(*write_ts, write)?;
                (2, key, write_ts.0)
            }
        };
        if !changed_records.insert((family, key.clone(), timestamp)) {
            return Err(Error::InvalidArgument(
                "MVCC delta edits the same logical record more than once".to_string(),
            ));
        }
    }
    Ok(())
}

impl MvccBackend for MemtableMvccBackend {
    type PinnedGeneration = Self;

    fn publish_atomic(&mut self, delta: MvccDelta) -> Result<()> {
        self.publish_atomic_with_reservation(delta, None)
    }

    fn publish_atomic_with_reservation(
        &mut self,
        delta: MvccDelta,
        mut admission_reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        if self.command_generation_mode && self.pending_command_metadata.is_none() {
            return Err(Error::InvalidArgument(
                "replicated tablet MVCC edits must publish with complete command metadata"
                    .to_string(),
            ));
        }
        match (&self.memtable_budget, &admission_reservation) {
            (Some(budget), Some(reservation)) if !reservation.belongs_to(budget) => {
                return Err(Error::InvalidArgument(
                    "memtable admission reservation belongs to another node budget".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(Error::InvalidArgument(
                    "unbudgeted memtable storage cannot consume an admission reservation"
                        .to_string(),
                ));
            }
            _ => {}
        }

        validate_mvcc_delta(&delta)?;

        let (next_memtable_bytes, required_growth_bytes, freeze_active, requires_freeze) = if self
            .memtable_budget
            .is_some()
        {
            let incoming_metadata = self.pending_command_metadata.as_ref();
            let next_bytes = self.projected_generation_bytes(&delta, incoming_metadata, false)?;
            if next_bytes > self.memtable_limit_bytes {
                let fresh_bytes =
                    self.projected_generation_bytes(&delta, incoming_metadata, true)?;
                if fresh_bytes > self.memtable_limit_bytes {
                    return Err(Error::TabletUnavailable {
                        reason: format!(
                            "tablet command needs {fresh_bytes} active memtable bytes with a {}-byte limit",
                            self.memtable_limit_bytes
                        ),
                    });
                }
                let current_bytes = self
                    .memtable_charge
                    .as_ref()
                    .map_or(0, MemtableCharge::bytes);
                if current_bytes > 0 {
                    self.validate_freeze_capacity(current_bytes)?;
                }
                (fresh_bytes, fresh_bytes, current_bytes > 0, true)
            } else {
                let current_bytes = self
                    .memtable_charge
                    .as_ref()
                    .map_or(0, MemtableCharge::bytes);
                let growth = if next_bytes >= current_bytes {
                    next_bytes.checked_sub(current_bytes).ok_or_else(|| {
                        Error::CorruptData(
                            "active memtable growth calculation underflowed".to_string(),
                        )
                    })?
                } else {
                    0
                };
                (next_bytes, growth, false, false)
            }
        } else {
            (0, 0, false, false)
        };

        let freeze_active = if let Some(owner_decision) = self.pending_freeze_decision {
            if owner_decision != requires_freeze {
                return Err(Error::InvalidArgument(
                    "tablet generation freeze decision changed after projection".to_string(),
                ));
            }
            owner_decision && freeze_active
        } else {
            freeze_active
        };

        let growth_reservation = if required_growth_bytes == 0 {
            // A deterministic rejection or metadata-only command consumes no
            // MVCC capacity; its proposal lease ends at this publication.
            admission_reservation.take();
            None
        } else if let Some(reservation) = admission_reservation.take() {
            if reservation.bytes() < required_growth_bytes {
                return Err(Error::TabletUnavailable {
                    reason: format!(
                        "committed memtable edit needs {required_growth_bytes} bytes but its retained admission reservation holds {}",
                        reservation.bytes()
                    ),
                });
            }
            Some(reservation)
        } else if let Some(budget) = &self.memtable_budget {
            Some(budget.reserve(MemoryClass::Progress, required_growth_bytes)?)
        } else {
            None
        };

        if freeze_active {
            self.freeze_active_memtable()?;
            match self.try_handoff_pending_immutables() {
                Ok(_) | Err(FlushHandoffError::Full) => {}
                Err(FlushHandoffError::Closed | FlushHandoffError::Rejected(_)) => {
                    // The command publication is valid even if background
                    // handoff is unavailable. The backend retains this failure
                    // for diagnostics and later retry without rolling back data.
                }
            }
        }

        if self.memtable_budget.is_some() {
            self.publish_memtable_charge(
                next_memtable_bytes,
                required_growth_bytes,
                growth_reservation,
            )?;
        }

        // All fallible checks and memory ownership transfers precede the first
        // row-family map change; the complete CommandDelta remains one publication.
        for edit in delta.edits {
            match edit {
                MvccRecordEdit::PutDefault { key, start_ts, row } => {
                    self.default_tombstones.remove(&(key.clone(), start_ts));
                    self.default.entry(key).or_default().insert(start_ts, row);
                }
                MvccRecordEdit::DeleteDefault { key, start_ts } => {
                    let remove_key = if let Some(versions) = self.default.get_mut(&key) {
                        versions.remove(&start_ts);
                        versions.is_empty()
                    } else {
                        false
                    };
                    if remove_key {
                        self.default.remove(&key);
                    }
                    if self.immutable_has_default(&key, start_ts) {
                        self.default_tombstones.insert((key, start_ts));
                    } else {
                        self.default_tombstones.remove(&(key, start_ts));
                    }
                }
                MvccRecordEdit::PutLock { key, lock } => {
                    self.lock_tombstones.remove(&key);
                    self.locks.insert(key, lock);
                }
                MvccRecordEdit::DeleteLock { key } => {
                    self.locks.remove(&key);
                    if self.immutable_has_lock(&key) {
                        self.lock_tombstones.insert(key);
                    } else {
                        self.lock_tombstones.remove(&key);
                    }
                }
                MvccRecordEdit::PutWrite {
                    key,
                    write_ts,
                    write,
                } => {
                    self.writes.entry(key).or_default().insert(write_ts, write);
                }
            }
        }

        if let Some(metadata) = self.pending_command_metadata.take() {
            if let Some(active) = &mut self.active_command_metadata {
                active.absorb(metadata);
            } else {
                self.active_command_metadata = Some(metadata);
            }
            self.command_generation_mode = true;
        }
        self.pending_freeze_decision = None;

        Ok(())
    }

    fn command_generation_requires_freeze(
        &self,
        delta: &MvccDelta,
        metadata: &CommandGenerationMetadata,
    ) -> Result<bool> {
        MemtableMvccBackend::command_generation_requires_freeze(self, delta, metadata)
    }

    fn publish_command_generation_with_reservation(
        &mut self,
        delta: MvccDelta,
        metadata: CommandGenerationMetadata,
        reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        let freeze_active = self.command_generation_requires_freeze(&delta, &metadata)?;
        if self.pending_command_metadata.is_some() {
            return Err(Error::InvalidArgument(
                "another tablet command generation is already being published".to_string(),
            ));
        }
        if self
            .active_command_metadata
            .as_ref()
            .is_some_and(|active| active.storage_identity != metadata.storage_identity)
        {
            return Err(Error::InvalidArgument(
                "tablet command metadata changed replica identity within one active generation"
                    .to_string(),
            ));
        }

        self.pending_command_metadata = Some(metadata);
        self.pending_freeze_decision = Some(freeze_active);
        let result = self.publish_atomic_with_reservation(delta, reservation);
        if result.is_err() {
            self.pending_command_metadata.take();
            self.pending_freeze_decision = None;
        }
        result
    }

    fn install_restored_command_metadata(
        &mut self,
        metadata: CommandGenerationMetadata,
    ) -> Result<()> {
        let Some(budget) = self.memtable_budget.as_ref() else {
            return Ok(());
        };
        let bytes = command_generation_metadata_charge(&metadata)?;
        let reservation = budget.reserve(MemoryClass::Progress, bytes)?;
        let base = self
            .restored_base
            .get_or_insert_with(ImmutableMemtableGeneration::default);
        if base.command_metadata.is_some() {
            return Err(Error::InvalidArgument(
                "restored base already owns command metadata".to_string(),
            ));
        }
        match base._charge.as_ref() {
            Some(charge) => reservation.commit_into_amount(charge, bytes)?,
            None => base._charge = reservation.commit_amount(bytes)?,
        }
        base.frontier = Some(metadata.frontier);
        base.command_metadata = Some(metadata);
        self.command_generation_mode = true;
        Ok(())
    }

    fn memtable_pressure(&self) -> Option<MemtablePressure> {
        MemtableMvccBackend::memtable_pressure(self)
    }

    fn node_memtable_budget(&self) -> Option<NodeMemtableBudget> {
        self.memtable_budget.clone()
    }

    fn pin_generation(&self) -> Result<Self::PinnedGeneration> {
        // Pinned readers own detached copies of the active and immutable
        // records. These read generations do not share or release serving charges.
        Ok(Self {
            default: self.default.clone(),
            locks: self.locks.clone(),
            writes: self.writes.clone(),
            default_tombstones: self.default_tombstones.clone(),
            lock_tombstones: self.lock_tombstones.clone(),
            restored_base: self
                .restored_base
                .as_ref()
                .map(ImmutableMemtableGeneration::read_copy),
            active_command_metadata: self.active_command_metadata.clone(),
            pending_command_metadata: None,
            pending_freeze_decision: None,
            command_generation_mode: self.command_generation_mode,
            immutable_memtables: self
                .immutable_memtables
                .iter()
                .map(|generation| Arc::new(generation.read_copy()))
                .collect(),
            handed_off_immutables: BTreeSet::new(),
            immutable_sink: None,
            last_flush_handoff_error: None,
            next_immutable_generation_id: self.next_immutable_generation_id,
            immutable_memtable_bytes: 0,
            immutable_memtable_soft_limit_bytes: 0,
            immutable_memtable_limit_bytes: 0,
            immutable_memtable_soft_count_limit: 0,
            immutable_memtable_count_limit: 0,
            memtable_budget: None,
            memtable_limit_bytes: 0,
            memtable_charge: None,
        })
    }

    fn stats(&self) -> MvccStats {
        self.compute_stats()
    }

    fn allocator_high_water_marks(&self) -> (TxnId, Timestamp) {
        self.compute_allocator_high_water_marks()
    }
}

impl MvccReadGeneration for InMemoryMvccBackend {
    fn get_default(&self, key: &[u8], start_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        Ok(self
            .default
            .get(key)
            .and_then(|versions| versions.get(&start_ts))
            .cloned())
    }

    fn get_lock(&self, key: &[u8]) -> Result<Option<LockRecord>> {
        Ok(self.locks.get(key).cloned())
    }

    fn get_write(&self, key: &[u8], write_ts: Timestamp) -> Result<Option<WriteRecord>> {
        Ok(self
            .writes
            .get(key)
            .and_then(|versions| versions.get(&write_ts))
            .cloned())
    }

    fn write_page(
        &self,
        key: &[u8],
        lower: Bound<Timestamp>,
        upper: Bound<Timestamp>,
        resume_after: Option<Timestamp>,
        direction: MvccCursorDirection,
        max_records: usize,
    ) -> Result<MvccWritePage> {
        if max_records == 0 {
            return Err(Error::InvalidArgument(
                "MVCC write cursor max_records must be greater than zero".to_string(),
            ));
        }
        let Some(versions) = self.writes.get(key) else {
            return Ok(MvccWritePage {
                writes: Vec::new(),
                has_more: false,
            });
        };
        let (lower, upper) = match (direction, resume_after) {
            (MvccCursorDirection::Forward, Some(timestamp)) => {
                (max_lower_bound(lower, Excluded(timestamp)), upper)
            }
            (MvccCursorDirection::Reverse, Some(timestamp)) => {
                (lower, min_upper_bound(upper, Excluded(timestamp)))
            }
            (_, None) => (lower, upper),
        };
        if timestamp_range_is_empty(&lower, &upper) {
            return Ok(MvccWritePage {
                writes: Vec::new(),
                has_more: false,
            });
        }
        let page_capacity = max_records.checked_add(1).ok_or_else(|| {
            Error::InvalidArgument("MVCC write cursor page size overflowed".to_string())
        })?;
        let mut writes = Vec::with_capacity(page_capacity);
        match direction {
            MvccCursorDirection::Forward => {
                for (timestamp, write) in versions.range((lower, upper)).take(page_capacity) {
                    writes.push((*timestamp, write.clone()));
                }
            }
            MvccCursorDirection::Reverse => {
                for (timestamp, write) in versions.range((lower, upper)).rev().take(page_capacity) {
                    writes.push((*timestamp, write.clone()));
                }
            }
        }
        let has_more = writes.len() > max_records;
        writes.truncate(max_records);
        Ok(MvccWritePage { writes, has_more })
    }

    fn key_page(
        &self,
        family: MvccKeyFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_keys: usize,
    ) -> Result<MvccKeyPage> {
        if max_keys == 0 {
            return Err(Error::InvalidArgument(
                "MVCC key cursor max_keys must be greater than zero".to_string(),
            ));
        }
        let (_, upper) = encoded_scan_bounds(start, end)?;
        if let Some(resume_after) = resume_after {
            validate_encoded_key_argument(resume_after, "MVCC key cursor resume key")?;
            if end.is_some_and(|end| resume_after >= end) {
                return Ok(MvccKeyPage {
                    keys: Vec::new(),
                    has_more: false,
                });
            }
        }
        let lower = scan_lower_bound(start, resume_after);
        let page_capacity = max_keys.checked_add(1).ok_or_else(|| {
            Error::InvalidArgument("MVCC key cursor page size overflowed".to_string())
        })?;
        let mut keys = Vec::with_capacity(page_capacity);
        match family {
            MvccKeyFamily::Writes => {
                for key in self.writes.range((lower, upper)).map(|(key, _)| key) {
                    validate_encoded_key_argument(key, "MVCC write cursor key")?;
                    keys.push(key.clone());
                    if keys.len() > max_keys {
                        break;
                    }
                }
            }
            MvccKeyFamily::Locks => {
                for key in self.locks.range((lower, upper)).map(|(key, _)| key) {
                    validate_encoded_key_argument(key, "MVCC lock cursor key")?;
                    keys.push(key.clone());
                    if keys.len() > max_keys {
                        break;
                    }
                }
            }
        }
        let has_more = keys.len() > max_keys;
        keys.truncate(max_keys);
        Ok(MvccKeyPage { keys, has_more })
    }

    fn lock_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_locks: usize,
        max_bytes: usize,
    ) -> Result<IntentScanPage> {
        if max_locks == 0 || max_bytes == 0 {
            return Err(Error::InvalidArgument(
                "MVCC lock cursor limits must be greater than zero".to_string(),
            ));
        }
        let (_, upper) = encoded_scan_bounds(start, end)?;
        if let Some(resume_after) = resume_after {
            validate_encoded_key_argument(resume_after, "MVCC lock cursor resume key")?;
            if end.is_some_and(|end| resume_after >= end) {
                return Ok(IntentScanPage {
                    locks: Vec::new(),
                    has_more: false,
                });
            }
        }
        let lower = scan_lower_bound(start, resume_after);
        let mut locks = Vec::new();
        let mut encoded_bytes = 0usize;
        for (key, lock) in self.locks.range((lower, upper)) {
            if locks.len() >= max_locks {
                return Ok(IntentScanPage {
                    locks,
                    has_more: true,
                });
            }
            let lock_bytes = lock
                .to_proto()
                .map_err(|error| Error::CorruptData(error.to_string()))?
                .encoded_len();
            let entry_bytes = key.len().checked_add(lock_bytes).ok_or_else(|| {
                Error::InvalidArgument("MVCC lock cursor byte count overflowed".to_string())
            })?;
            let next_bytes = encoded_bytes.checked_add(entry_bytes).ok_or_else(|| {
                Error::InvalidArgument("MVCC lock cursor byte count overflowed".to_string())
            })?;
            if next_bytes > max_bytes {
                if locks.is_empty() {
                    return Err(Error::InvalidArgument(
                        "MVCC lock cursor byte budget is smaller than its first record".to_string(),
                    ));
                }
                return Ok(IntentScanPage {
                    locks,
                    has_more: true,
                });
            }
            encoded_bytes = next_bytes;
            locks.push((key.clone(), lock.clone()));
        }
        Ok(IntentScanPage {
            locks,
            has_more: false,
        })
    }

    fn recovery_frontier(&self) -> Result<Option<RecoveryFrontier>> {
        Ok(None)
    }

    fn export_snapshot(&self) -> Result<CapturedMvccState> {
        let default_values = self
            .default
            .iter()
            .flat_map(|(key, versions)| {
                versions.iter().map(move |(start_timestamp, row)| {
                    snapshot_proto::DefaultValueEntry {
                        key: key.clone(),
                        start_timestamp: Some(start_timestamp.to_proto()),
                        row: row.clone(),
                    }
                })
            })
            .collect();
        let locks = self
            .locks
            .iter()
            .map(|(key, record)| {
                record.validate().map_err(|error| {
                    Error::CorruptData(format!("in-memory lock cannot be exported: {error}"))
                })?;
                Ok(snapshot_proto::LockEntry {
                    key: key.clone(),
                    record: Some(record.to_proto().map_err(|error| {
                        Error::CorruptData(format!("in-memory lock cannot be exported: {error}"))
                    })?),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let writes = self
            .writes
            .iter()
            .flat_map(|(key, versions)| {
                versions.iter().map(move |(write_timestamp, record)| {
                    validate_write_record(*write_timestamp, record)?;
                    Ok(snapshot_proto::WriteEntry {
                        key: key.clone(),
                        write_timestamp: Some(write_timestamp.to_proto()),
                        record: Some(record.to_proto().map_err(|error| {
                            Error::CorruptData(format!(
                                "in-memory write cannot be exported: {error}"
                            ))
                        })?),
                    })
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(CapturedMvccState::new(default_values, locks, writes))
    }
}

impl MvccBackend for InMemoryMvccBackend {
    type PinnedGeneration = Self;

    fn publish_atomic(&mut self, delta: MvccDelta) -> Result<()> {
        validate_mvcc_delta(&delta)?;
        for edit in delta.edits {
            match edit {
                MvccRecordEdit::PutDefault { key, start_ts, row } => {
                    self.default.entry(key).or_default().insert(start_ts, row);
                }
                MvccRecordEdit::DeleteDefault { key, start_ts } => {
                    if let Some(versions) = self.default.get_mut(&key) {
                        versions.remove(&start_ts);
                        if versions.is_empty() {
                            self.default.remove(&key);
                        }
                    }
                }
                MvccRecordEdit::PutLock { key, lock } => {
                    self.locks.insert(key, lock);
                }
                MvccRecordEdit::DeleteLock { key } => {
                    self.locks.remove(&key);
                }
                MvccRecordEdit::PutWrite {
                    key,
                    write_ts,
                    write,
                } => {
                    self.writes.entry(key).or_default().insert(write_ts, write);
                }
            }
        }
        Ok(())
    }

    fn publish_atomic_with_reservation(
        &mut self,
        delta: MvccDelta,
        reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        if reservation.is_some() {
            return Err(Error::InvalidArgument(
                "reference MVCC storage cannot consume a memtable reservation".to_string(),
            ));
        }
        self.publish_atomic(delta)
    }

    fn publish_command_generation_with_reservation(
        &mut self,
        delta: MvccDelta,
        _metadata: CommandGenerationMetadata,
        reservation: Option<MemoryReservation>,
    ) -> Result<()> {
        self.publish_atomic_with_reservation(delta, reservation)
    }

    fn pin_generation(&self) -> Result<Self::PinnedGeneration> {
        Ok(self.clone())
    }

    fn stats(&self) -> MvccStats {
        MvccStats {
            default_keys: self.default.len(),
            default_versions: self.default.values().map(BTreeMap::len).sum(),
            default_version_chains: version_chain_stats(self.default.values().map(BTreeMap::len)),
            locks: self.locks.len(),
            write_keys: self.writes.len(),
            write_records: self.writes.values().map(BTreeMap::len).sum(),
            write_record_chains: version_chain_stats(self.writes.values().map(BTreeMap::len)),
        }
    }

    fn allocator_high_water_marks(&self) -> (TxnId, Timestamp) {
        let mut max_transaction_id = TxnId(0);
        let mut max_timestamp = Timestamp(0);
        for versions in self.default.values() {
            for start_timestamp in versions.keys() {
                max_timestamp = Timestamp(max_timestamp.0.max(start_timestamp.0));
            }
        }
        for lock in self.locks.values() {
            max_transaction_id = TxnId(max_transaction_id.0.max(lock.txn_id.0));
            max_timestamp = Timestamp(max_timestamp.0.max(lock.start_timestamp.0));
        }
        for versions in self.writes.values() {
            for (commit_timestamp, record) in versions {
                max_timestamp = Timestamp(
                    max_timestamp
                        .0
                        .max(commit_timestamp.0)
                        .max(record.start_timestamp.0),
                );
            }
        }
        (max_transaction_id, max_timestamp)
    }
}

impl MemtableMvccBackend {
    fn compute_stats(&self) -> MvccStats {
        let default = self.materialize_default();
        let locks = self.materialize_locks();
        let writes = self.materialize_writes();
        MvccStats {
            default_keys: default.len(),
            default_versions: default.values().map(BTreeMap::len).sum(),
            default_version_chains: version_chain_stats(default.values().map(BTreeMap::len)),
            locks: locks.len(),
            write_keys: writes.len(),
            write_records: writes.values().map(BTreeMap::len).sum(),
            write_record_chains: version_chain_stats(writes.values().map(BTreeMap::len)),
        }
    }

    fn compute_allocator_high_water_marks(&self) -> (TxnId, Timestamp) {
        let mut max_transaction_id = TxnId(0);
        let mut max_timestamp = Timestamp(0);

        for versions in self.materialize_default().values() {
            for start_timestamp in versions.keys() {
                max_timestamp = Timestamp(max_timestamp.0.max(start_timestamp.0));
            }
        }
        for lock in self.materialize_locks().values() {
            max_transaction_id = TxnId(max_transaction_id.0.max(lock.txn_id.0));
            max_timestamp = Timestamp(max_timestamp.0.max(lock.start_timestamp.0));
        }
        for versions in self.materialize_writes().values() {
            for (commit_timestamp, record) in versions {
                max_timestamp = Timestamp(
                    max_timestamp
                        .0
                        .max(commit_timestamp.0)
                        .max(record.start_timestamp.0),
                );
            }
        }

        (max_transaction_id, max_timestamp)
    }
}

fn validate_snapshot_row_key(table_id: TableId, key: &[u8], context: &str) -> Result<()> {
    let row_key = decode_row_key(key).map_err(|source| {
        Error::CorruptData(format!(
            "snapshot {context} has an invalid row key: {source}"
        ))
    })?;

    if row_key.table_id != table_id {
        return Err(Error::CorruptData(format!(
            "snapshot {context} row key belongs to table {}, expected table {}",
            row_key.table_id.0, table_id.0
        )));
    }

    Ok(())
}

fn validate_commit_preflight_metadata(txn_id: TxnId, start_ts: Timestamp) -> Result<()> {
    if txn_id.0 == 0 {
        return Err(Error::InvalidArgument(
            "transaction ID 0 is reserved".to_string(),
        ));
    }

    if start_ts.0 == 0 {
        return Err(Error::InvalidArgument(
            "transaction start timestamp 0 is reserved".to_string(),
        ));
    }

    Ok(())
}

fn validate_commit_metadata(
    txn_id: TxnId,
    start_ts: Timestamp,
    commit_ts: Timestamp,
) -> Result<()> {
    validate_commit_preflight_metadata(txn_id, start_ts)?;

    if commit_ts.0 == 0 {
        return Err(Error::InvalidArgument(
            "transaction commit timestamp 0 is reserved".to_string(),
        ));
    }

    if commit_ts <= start_ts {
        return Err(Error::InvalidArgument(format!(
            "commit timestamp {} must be greater than start timestamp {}",
            commit_ts.0, start_ts.0
        )));
    }

    Ok(())
}

/// Validate one record against the timestamp used as its write-map key.
///
/// `Put` and `Delete` use a true commit timestamp. `Rollback` uses `start_ts`
/// because the rollback command does not allocate or carry a commit timestamp.
fn validate_write_record(stored_write_ts: Timestamp, write: &WriteRecord) -> Result<()> {
    if write.commit_timestamp != stored_write_ts {
        return Err(Error::CorruptData(format!(
            "write-map timestamp {} does not match record timestamp {}",
            stored_write_ts.0, write.commit_timestamp.0
        )));
    }

    if write.start_timestamp.0 == 0 {
        return Err(Error::CorruptData(
            "write record contains reserved start timestamp 0".to_string(),
        ));
    }

    match write.op {
        WriteKind::Put | WriteKind::Delete => {
            if write.commit_timestamp <= write.start_timestamp {
                return Err(Error::CorruptData(format!(
                    "committed write timestamp {} does not follow start \
                     timestamp {}",
                    write.commit_timestamp.0, write.start_timestamp.0
                )));
            }
        }

        WriteKind::Rollback => {
            if write.commit_timestamp != write.start_timestamp {
                return Err(Error::CorruptData(format!(
                    "rollback record timestamp {} must equal start timestamp {}",
                    write.commit_timestamp.0, write.start_timestamp.0
                )));
            }
        }
    }

    Ok(())
}

fn validate_encoded_key_argument(key: &[u8], context: &str) -> Result<()> {
    decode_row_key(key).map(|_| ()).map_err(|error| {
        Error::InvalidArgument(format!(
            "{context} is not a canonical encoded row key: {error}"
        ))
    })
}

fn apply_default_generation(
    target: &mut BTreeMap<Vec<u8>, BTreeMap<Timestamp, Vec<u8>>>,
    source: &BTreeMap<Vec<u8>, BTreeMap<Timestamp, Vec<u8>>>,
    tombstones: &BTreeSet<(Vec<u8>, Timestamp)>,
) {
    for (key, timestamp) in tombstones {
        let remove_key = if let Some(versions) = target.get_mut(key) {
            versions.remove(timestamp);
            versions.is_empty()
        } else {
            false
        };
        if remove_key {
            target.remove(key);
        }
    }
    for (key, versions) in source {
        let target_versions = target.entry(key.clone()).or_default();
        for (timestamp, row) in versions {
            target_versions.insert(*timestamp, row.clone());
        }
    }
}

fn merge_write_generation(
    target: &mut BTreeMap<Vec<u8>, BTreeMap<Timestamp, WriteRecord>>,
    source: &BTreeMap<Vec<u8>, BTreeMap<Timestamp, WriteRecord>>,
) {
    for (key, versions) in source {
        let target_versions = target.entry(key.clone()).or_default();
        for (timestamp, write) in versions {
            target_versions.insert(*timestamp, write.clone());
        }
    }
}

fn memtable_lock_payload_size(lock: &LockRecord) -> Result<usize> {
    lock.primary_key
        .len()
        .checked_add(std::mem::size_of::<LockRecord>())
        .ok_or_else(|| Error::TabletUnavailable {
            reason: "active memtable lock charge overflowed".to_string(),
        })
}

fn memtable_write_payload_size(_write: &WriteRecord) -> Result<usize> {
    Ok(std::mem::size_of::<WriteRecord>())
}

fn encoded_scan_bounds(start: Option<&[u8]>, end: Option<&[u8]>) -> Result<EncodedScanBounds> {
    if let Some(start) = start {
        validate_encoded_key_argument(start, "scan start key")?;
    }

    if let Some(end) = end {
        validate_encoded_key_argument(end, "scan end key")?;
    }

    if let (Some(start), Some(end)) = (start, end)
        && start >= end
    {
        return Err(Error::InvalidArgument(
            "scan start key must be less than scan end key".to_string(),
        ));
    }

    Ok((
        start.map_or(Unbounded, |key| Included(key.to_vec())),
        end.map_or(Unbounded, |key| Excluded(key.to_vec())),
    ))
}

fn validate_scan_page_limits(max_rows: usize, max_bytes: usize) -> Result<()> {
    if max_rows == 0 {
        return Err(Error::InvalidArgument(
            "scan page max_rows must be greater than zero".to_string(),
        ));
    }
    if max_bytes == 0 {
        return Err(Error::InvalidArgument(
            "scan page max_bytes must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn scan_lower_bound(start: Option<&[u8]>, resume_after: Option<&[u8]>) -> Bound<Vec<u8>> {
    match (start, resume_after) {
        (None, None) => Unbounded,
        (Some(start), None) => Included(start.to_vec()),
        (None, Some(resume_after)) => Excluded(resume_after.to_vec()),
        (Some(start), Some(resume_after)) if resume_after < start => Included(start.to_vec()),
        (Some(_), Some(resume_after)) => Excluded(resume_after.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsm::{
        CommandGenerationMetadata, LegacyOutcomeEdit, LogicalOutcomeEdit, RetryFloorEdit,
        TabletStorageIdentity, TxnStatusEdit,
    };
    use ragnordb_common::{
        codec::{Row, TxnStatus, TxnStatusRecord, Value},
        command_codec::{
            CachedTabletCommandOutcome, CachedTabletCommandRejection,
            CachedTabletCommandRejectionKind, CachedTabletCommandResult,
        },
        encoding::encode_row,
        ids::{
            ClientRequestId, CommandKind, LogicalCommandId, RaftGroupId, ReplicaId, TableId,
            TabletId, TxnId,
        },
    };

    use crate::key::{encode_row_key, make_row_key};

    fn encoded_key(id: i64) -> Vec<u8> {
        encode_row_key(&make_row_key(TableId(1), &[Value::Int(id)]).unwrap()).unwrap()
    }

    fn encoded_row(id: i64, name: &str) -> Vec<u8> {
        encode_row(&Row {
            values: vec![Value::Int(id), Value::Text(name.to_string())],
        })
        .unwrap()
    }

    fn put_batch(key: Vec<u8>, row: Vec<u8>) -> BTreeMap<Vec<u8>, Mutation> {
        BTreeMap::from([(key, Mutation::Put(row))])
    }

    fn delete_batch(key: Vec<u8>) -> BTreeMap<Vec<u8>, Mutation> {
        BTreeMap::from([(key, Mutation::Delete)])
    }

    #[test]
    fn active_memtable_reserves_shared_bytes_before_publication_and_releases_on_drop() {
        let node_budget = crate::lsm::NodeMemtableBudget::new(300).unwrap();
        let mut first = MemtableMvcc::with_memtable_budget(node_budget.clone(), 300).unwrap();
        let mut second = MemtableMvcc::with_memtable_budget(node_budget.clone(), 300).unwrap();

        assert_eq!(first.active_memtable_bytes(), 0);
        assert_eq!(second.active_memtable_bytes(), 0);
        assert_eq!(node_budget.used_bytes(), 0);

        let key = encoded_key(1);
        let row = encoded_row(1, "budgeted");
        let delta = MvccDelta {
            edits: vec![MvccRecordEdit::PutDefault {
                key: key.clone(),
                start_ts: Timestamp(1),
                row,
            }],
        };

        first.publish_mvcc_delta(delta.clone()).unwrap();
        let first_charge = first.active_memtable_bytes();
        assert!(first_charge > 0);
        assert_eq!(node_budget.used_bytes(), first_charge);

        let old_generation = second.pin_read_generation().unwrap();
        assert!(second.publish_mvcc_delta(delta.clone()).is_err());
        assert_eq!(second.active_memtable_bytes(), 0);
        assert_eq!(node_budget.used_bytes(), first_charge);
        assert_eq!(second.get_default_record(&key, Timestamp(1)).unwrap(), None);
        assert_eq!(
            old_generation.get_default(&key, Timestamp(1)).unwrap(),
            None
        );

        drop(first);
        assert_eq!(node_budget.used_bytes(), 0);

        second.publish_mvcc_delta(delta).unwrap();
        assert_eq!(node_budget.used_bytes(), second.active_memtable_bytes());
        assert!(
            second
                .get_default_record(&key, Timestamp(1))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn many_cold_tablets_have_no_active_memtable_charge() {
        let budget = crate::lsm::NodeMemtableBudget::new(1024 * 1024).unwrap();
        let tablets = (0..128)
            .map(|_| MemtableMvcc::with_memtable_budget(budget.clone(), 1024).unwrap())
            .collect::<Vec<_>>();

        assert!(
            tablets
                .iter()
                .all(|tablet| tablet.active_memtable_bytes() == 0)
        );
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn node_budget_failure_precedes_first_record_allocation() {
        let budget = crate::lsm::NodeMemtableBudget::new_with_progress_reserve(300, 100).unwrap();
        let occupied = budget.reserve(MemoryClass::User, 200).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(budget.clone(), 300).unwrap();
        let key = encoded_key(13);
        let result = storage.publish_mvcc_delta(MvccDelta {
            edits: vec![MvccRecordEdit::PutDefault {
                key: key.clone(),
                start_ts: Timestamp(13),
                row: encoded_row(13, &"x".repeat(64)),
            }],
        });

        assert!(result.is_err());
        assert_eq!(storage.active_memtable_bytes(), 0);
        assert_eq!(budget.used_bytes(), 200);
        assert_eq!(
            storage.get_default_record(&key, Timestamp(13)).unwrap(),
            None
        );
        drop(occupied);
    }

    #[test]
    fn active_and_immutable_limit_overflow_fails_closed() {
        let budget = crate::lsm::NodeMemtableBudget::new(usize::MAX).unwrap();
        assert!(MemtableMvcc::with_memtable_budget(budget, usize::MAX).is_err());
    }

    #[test]
    fn concurrent_tablets_cannot_overdraw_the_shared_memtable_budget() {
        let node_budget = crate::lsm::NodeMemtableBudget::new(300).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

        let ((first_ok, first), (second_ok, second)) = std::thread::scope(|scope| {
            let first_budget = node_budget.clone();
            let first_barrier = std::sync::Arc::clone(&barrier);
            let first_thread = scope.spawn(move || {
                let mut storage = MemtableMvcc::with_memtable_budget(first_budget, 300).unwrap();
                first_barrier.wait();
                let result = storage.publish_mvcc_delta(MvccDelta {
                    edits: vec![MvccRecordEdit::PutDefault {
                        key: encoded_key(11),
                        start_ts: Timestamp(11),
                        row: encoded_row(11, "concurrent"),
                    }],
                });
                (result.is_ok(), storage)
            });

            let second_budget = node_budget.clone();
            let second_barrier = std::sync::Arc::clone(&barrier);
            let second_thread = scope.spawn(move || {
                let mut storage = MemtableMvcc::with_memtable_budget(second_budget, 300).unwrap();
                second_barrier.wait();
                let result = storage.publish_mvcc_delta(MvccDelta {
                    edits: vec![MvccRecordEdit::PutDefault {
                        key: encoded_key(12),
                        start_ts: Timestamp(12),
                        row: encoded_row(12, "concurrent"),
                    }],
                });
                (result.is_ok(), storage)
            });

            let first = first_thread.join().unwrap();
            let second = second_thread.join().unwrap();
            (first, second)
        });

        assert_ne!(first_ok, second_ok, "only one active generation fits");
        assert!(node_budget.used_bytes() <= node_budget.limit_bytes());
        assert!(node_budget.used_bytes() > 0);
        assert_eq!(
            node_budget.used_bytes(),
            if first_ok {
                first.active_memtable_bytes()
            } else {
                second.active_memtable_bytes()
            }
        );

        drop(first);
        drop(second);
        assert_eq!(node_budget.used_bytes(), 0);
    }

    #[test]
    fn foreign_admission_reservation_fails_before_memtable_publication() {
        let first_budget = crate::lsm::NodeMemtableBudget::new(1024).unwrap();
        let second_budget = crate::lsm::NodeMemtableBudget::new(1024).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(first_budget.clone(), 1024).unwrap();
        let reservation = second_budget.reserve(MemoryClass::User, 512).unwrap();
        let key = encoded_key(14);
        let result = storage.publish_mvcc_delta_with_reservation(
            MvccDelta {
                edits: vec![MvccRecordEdit::PutDefault {
                    key: key.clone(),
                    start_ts: Timestamp(14),
                    row: encoded_row(14, "foreign budget"),
                }],
            },
            Some(reservation),
        );

        assert!(result.is_err());
        assert_eq!(first_budget.used_bytes(), 0);
        assert_eq!(second_budget.used_bytes(), 0);
        assert_eq!(storage.active_memtable_bytes(), 0);
        assert_eq!(
            storage.get_default_record(&key, Timestamp(14)).unwrap(),
            None
        );
    }

    #[test]
    fn active_memtable_capacity_rejection_leaves_records_and_charge_unchanged() {
        let node_budget = crate::lsm::NodeMemtableBudget::new(1024).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(node_budget.clone(), 1).unwrap();
        let key = encoded_key(2);
        let row = encoded_row(2, "too large for this active memtable");

        let result = storage.publish_mvcc_delta(MvccDelta {
            edits: vec![MvccRecordEdit::PutDefault {
                key: key.clone(),
                start_ts: Timestamp(2),
                row,
            }],
        });

        assert!(result.is_err());
        assert_eq!(storage.active_memtable_bytes(), 0);
        assert_eq!(node_budget.used_bytes(), 0);
        assert_eq!(
            storage.get_default_record(&key, Timestamp(2)).unwrap(),
            None
        );
    }

    #[test]
    fn active_memtable_charge_tracks_replacement_and_delete_edits() {
        let node_budget = crate::lsm::NodeMemtableBudget::new(1024).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(node_budget.clone(), 1024).unwrap();
        let key = encoded_key(3);

        storage
            .publish_mvcc_delta(MvccDelta {
                edits: vec![MvccRecordEdit::PutDefault {
                    key: key.clone(),
                    start_ts: Timestamp(3),
                    row: encoded_row(3, "a longer row value"),
                }],
            })
            .unwrap();
        let original_charge = storage.active_memtable_bytes();
        assert_eq!(node_budget.used_bytes(), original_charge);

        storage
            .publish_mvcc_delta(MvccDelta {
                edits: vec![MvccRecordEdit::PutDefault {
                    key: key.clone(),
                    start_ts: Timestamp(3),
                    row: encoded_row(3, "x"),
                }],
            })
            .unwrap();
        let replacement_charge = storage.active_memtable_bytes();
        assert!(replacement_charge < original_charge);
        assert_eq!(node_budget.used_bytes(), replacement_charge);

        storage
            .publish_mvcc_delta(MvccDelta {
                edits: vec![MvccRecordEdit::DeleteDefault {
                    key: key.clone(),
                    start_ts: Timestamp(3),
                }],
            })
            .unwrap();
        assert_eq!(storage.active_memtable_bytes(), 0);
        assert_eq!(node_budget.used_bytes(), 0);
        assert_eq!(
            storage.get_default_record(&key, Timestamp(3)).unwrap(),
            None
        );
    }

    /// Regression: a freeze triggered by a later row edit must retain the
    /// earlier status, retry, identity, and frontier transitions for the same
    /// serving generation instead of freezing MVCC records alone.
    #[test]
    fn rollover_keeps_command_metadata_with_its_mvcc_generation() {
        let node_budget = crate::lsm::NodeMemtableBudget::new(4096).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(node_budget, 2000).unwrap();
        let identity = TabletStorageIdentity {
            tablet_id: TabletId(1),
            table_id: TableId(1),
            raft_group_id: RaftGroupId(1),
            replica_id: ReplicaId(1),
        };
        let key = encoded_key(31);
        let status = TxnStatusRecord {
            txn_id: TxnId(31),
            start_timestamp: Timestamp(31),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key: key.clone(),
            participant_tablet_ids: vec![TabletId(1).0],
            last_heartbeat_timestamp: None,
            lease_deadline_ms: None,
        };
        let logical_id = LogicalCommandId {
            client_request_id: ClientRequestId {
                client_id: 31,
                session_epoch: 1,
                request_sequence: 1,
            },
            command_ordinal: 1,
            kind: CommandKind::Prewrite,
        };
        let metadata = CommandGenerationMetadata {
            storage_identity: identity,
            transaction_status_edits: vec![TxnStatusEdit::Put {
                txn_id: status.txn_id,
                status,
            }],
            logical_outcome_edits: vec![LogicalOutcomeEdit::Put {
                id: logical_id,
                outcome: CachedTabletCommandOutcome::Rejected(CachedTabletCommandRejection {
                    kind: CachedTabletCommandRejectionKind::WriteConflict,
                    reason: "cached conflict".to_string(),
                }),
            }],
            legacy_outcome_edits: vec![LegacyOutcomeEdit::Put {
                client_id: 31,
                last_sequence_applied: 1,
                outcome: CachedTabletCommandOutcome::Applied(CachedTabletCommandResult::Noop),
            }],
            retry_floor_edits: vec![RetryFloorEdit::Advance {
                client_id: 31,
                session_epoch: 1,
                acknowledged_through: 1,
            }],
            frontier: RecoveryFrontier::ReplicatedTablet {
                raft_group_id: RaftGroupId(1),
                replica_id: ReplicaId(1),
                applied_index: 1,
                applied_term: 3,
            },
        };
        let first = MvccDelta::default();
        assert!(
            !storage
                .command_generation_requires_freeze(&first, &metadata)
                .unwrap()
        );
        storage
            .publish_command_generation_with_reservation(first, metadata.clone(), None)
            .unwrap();
        assert!(storage.active_memtable_bytes() > 0);
        assert_eq!(storage.stats().default_versions, 0);
        assert_eq!(
            storage
                .backend
                .active_command_metadata
                .as_ref()
                .unwrap()
                .frontier,
            RecoveryFrontier::ReplicatedTablet {
                raft_group_id: RaftGroupId(1),
                replica_id: ReplicaId(1),
                applied_index: 1,
                applied_term: 3,
            }
        );
        assert!(storage.publish_mvcc_delta(MvccDelta::default()).is_err());

        let second = MvccDelta {
            edits: vec![MvccRecordEdit::PutDefault {
                key: key.clone(),
                start_ts: Timestamp(31),
                row: encoded_row(31, &"a".repeat(512)),
            }],
        };
        let second_metadata = CommandGenerationMetadata {
            storage_identity: identity,
            transaction_status_edits: Vec::new(),
            logical_outcome_edits: Vec::new(),
            legacy_outcome_edits: Vec::new(),
            retry_floor_edits: Vec::new(),
            frontier: RecoveryFrontier::ReplicatedTablet {
                raft_group_id: RaftGroupId(1),
                replica_id: ReplicaId(1),
                applied_index: 2,
                applied_term: 3,
            },
        };
        let freeze_active = storage
            .command_generation_requires_freeze(&second, &second_metadata)
            .unwrap();
        assert!(!freeze_active);
        storage
            .publish_command_generation_with_reservation(second, second_metadata.clone(), None)
            .unwrap();

        let third = MvccDelta {
            edits: vec![MvccRecordEdit::PutDefault {
                key: encoded_key(32),
                start_ts: Timestamp(32),
                row: encoded_row(32, &"b".repeat(512)),
            }],
        };
        let third_metadata = CommandGenerationMetadata {
            storage_identity: identity,
            transaction_status_edits: Vec::new(),
            logical_outcome_edits: Vec::new(),
            legacy_outcome_edits: Vec::new(),
            retry_floor_edits: Vec::new(),
            frontier: RecoveryFrontier::ReplicatedTablet {
                raft_group_id: RaftGroupId(1),
                replica_id: ReplicaId(1),
                applied_index: 3,
                applied_term: 3,
            },
        };
        let freeze_active = storage
            .command_generation_requires_freeze(&third, &third_metadata)
            .unwrap();
        assert!(freeze_active);
        storage
            .publish_command_generation_with_reservation(third, third_metadata.clone(), None)
            .unwrap();

        let frozen = storage.backend.immutable_memtables.front().unwrap();
        let mut expected_frozen_metadata = metadata.clone();
        expected_frozen_metadata.frontier = second_metadata.frontier;
        assert_eq!(frozen.command_metadata, Some(expected_frozen_metadata));
        assert!(frozen.default.contains_key(&key));
        let frozen_metadata = frozen.command_metadata.as_ref().unwrap();
        assert_eq!(frozen_metadata.storage_identity, identity);
        assert_eq!(
            frozen_metadata.frontier,
            RecoveryFrontier::ReplicatedTablet {
                raft_group_id: RaftGroupId(1),
                replica_id: ReplicaId(1),
                applied_index: 2,
                applied_term: 3,
            }
        );
        assert_eq!(
            storage.backend.active_command_metadata,
            Some(third_metadata)
        );
    }

    #[test]
    fn committed_apply_uses_hard_immutable_capacity_after_user_soft_limit() {
        let node_budget = crate::lsm::NodeMemtableBudget::new(4096).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(node_budget.clone(), 300).unwrap();
        let value = "v".repeat(64);

        for id in 1..=5 {
            storage
                .publish_mvcc_delta(MvccDelta {
                    edits: vec![MvccRecordEdit::PutDefault {
                        key: encoded_key(id),
                        start_ts: Timestamp(id as u64),
                        row: encoded_row(id, &value),
                    }],
                })
                .unwrap();
        }

        let pressure = storage.memtable_pressure().unwrap();
        assert_eq!(pressure.immutable_memtable_count, 4);
        assert!(pressure.user_writes_stalled());
        for id in 1..=5 {
            assert!(
                storage
                    .get_default_record(&encoded_key(id), Timestamp(id as u64))
                    .unwrap()
                    .is_some()
            );
        }

        let rejected_key = encoded_key(6);
        let rejected = storage.publish_mvcc_delta(MvccDelta {
            edits: vec![MvccRecordEdit::PutDefault {
                key: rejected_key.clone(),
                start_ts: Timestamp(6),
                row: encoded_row(6, &value),
            }],
        });

        assert!(rejected.is_err());
        assert_eq!(node_budget.used_bytes(), pressure.node_used_bytes);
        assert_eq!(
            storage
                .get_default_record(&rejected_key, Timestamp(6))
                .unwrap(),
            None
        );
    }

    #[test]
    fn retained_proposal_and_progress_capacity_allow_committed_rollover() {
        let node_budget =
            crate::lsm::NodeMemtableBudget::new_with_progress_reserve(4096, 1024).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(node_budget.clone(), 300).unwrap();
        let value = "v".repeat(64);
        let first = MvccDelta {
            edits: vec![MvccRecordEdit::PutDefault {
                key: encoded_key(21),
                start_ts: Timestamp(21),
                row: encoded_row(21, &value),
            }],
        };
        let second = MvccDelta {
            edits: vec![MvccRecordEdit::PutDefault {
                key: encoded_key(22),
                start_ts: Timestamp(22),
                row: encoded_row(22, &value),
            }],
        };
        let third = MvccDelta {
            edits: vec![MvccRecordEdit::PutDefault {
                key: encoded_key(23),
                start_ts: Timestamp(23),
                row: encoded_row(23, &value),
            }],
        };
        let fourth = MvccDelta {
            edits: vec![MvccRecordEdit::PutDefault {
                key: encoded_key(24),
                start_ts: Timestamp(24),
                row: encoded_row(24, &value),
            }],
        };

        storage.publish_mvcc_delta(first).unwrap();
        let first_proposal_lease = node_budget
            .reserve(MemoryClass::User, 512)
            .expect("leader admission reserves bytes before proposal");
        let second_proposal_lease = node_budget
            .reserve(MemoryClass::User, 512)
            .expect("the second proposal retains its own capacity before Raft");
        let fill_user_capacity = node_budget
            .reserve(
                MemoryClass::User,
                node_budget.user_limit_bytes() - node_budget.user_used_bytes(),
            )
            .unwrap();

        storage
            .publish_mvcc_delta_with_reservation(second, Some(first_proposal_lease))
            .expect("the first committed entry consumes its retained lease");
        assert_eq!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count,
            1
        );

        storage
            .publish_mvcc_delta_with_reservation(third, Some(second_proposal_lease))
            .expect("the second already-admitted entry can use hard queue headroom");
        assert_eq!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count,
            2
        );

        drop(fill_user_capacity);
        let fill_user_capacity = node_budget
            .reserve(
                MemoryClass::User,
                node_budget.user_limit_bytes() - node_budget.user_used_bytes(),
            )
            .unwrap();
        storage
            .publish_mvcc_delta(fourth)
            .expect("follower apply continues through the progress reserve");
        assert_eq!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count,
            3
        );
        assert!(node_budget.used_bytes() <= node_budget.limit_bytes());
        drop(fill_user_capacity);
    }

    #[derive(Debug)]
    struct FakeImmutableSink {
        failure: std::sync::Mutex<Option<FlushHandoffError>>,
        accepted: std::sync::Mutex<Vec<Arc<ImmutableMemtableGeneration>>>,
    }

    impl ImmutableMemtableSink for FakeImmutableSink {
        fn try_submit(
            &self,
            generation: Arc<ImmutableMemtableGeneration>,
        ) -> std::result::Result<(), FlushHandoffError> {
            if let Some(error) = self
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
            {
                return Err(error);
            }
            self.accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(generation);
            Ok(())
        }
    }

    #[test]
    fn immutable_handoff_does_not_authorize_production_retirement() {
        let budget = crate::lsm::NodeMemtableBudget::new_with_progress_reserve(4096, 1024).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(budget.clone(), 300).unwrap();
        let sink = Arc::new(FakeImmutableSink {
            failure: std::sync::Mutex::new(Some(FlushHandoffError::Full)),
            accepted: std::sync::Mutex::new(Vec::new()),
        });
        storage.set_immutable_memtable_sink(sink.clone());
        let value = "v".repeat(64);

        for id in 41..=43 {
            storage
                .publish_mvcc_delta(MvccDelta {
                    edits: vec![MvccRecordEdit::PutDefault {
                        key: encoded_key(id),
                        start_ts: Timestamp(id as u64),
                        row: encoded_row(id, &value),
                    }],
                })
                .unwrap();
        }

        let pressure = storage.memtable_pressure().unwrap();
        assert_eq!(pressure.immutable_memtable_count, 2);
        assert!(pressure.user_admission_throttled());
        assert!(storage.try_handoff_pending_immutables().is_err());
        let held_bytes = budget.used_bytes();
        let immutable = storage.immutable_memtable_generations();
        assert_eq!(immutable.len(), 2);
        let retired_id = immutable[1].id();
        let retired_bytes = immutable[1].charged_bytes();
        assert!(
            storage
                .retire_immutable_generation_for_test(retired_id)
                .is_err()
        );
        assert_eq!(budget.used_bytes(), held_bytes);
        assert_eq!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count,
            2
        );

        *sink
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        assert_eq!(storage.try_handoff_pending_immutables().unwrap(), 2);
        assert_eq!(budget.used_bytes(), held_bytes);
        assert_eq!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count,
            2
        );
        assert_eq!(sink.accepted.lock().unwrap().len(), 2);
        for id in 41..=43 {
            assert!(
                storage
                    .get_default_record(&encoded_key(id), Timestamp(id as u64))
                    .unwrap()
                    .is_some()
            );
        }

        // Queue acceptance leaves the generation readable and charged. The
        // test-only hook exercises retirement mechanics without claiming that
        // this fake handoff is durable.
        for id in 41..=43 {
            assert!(
                storage
                    .get_default_record(&encoded_key(id), Timestamp(id as u64))
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(budget.used_bytes(), held_bytes);
        storage
            .retire_immutable_generation_for_test(retired_id)
            .unwrap();
        assert_eq!(budget.used_bytes(), held_bytes - retired_bytes);
        assert_eq!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count,
            1
        );
        assert!(
            !storage
                .memtable_pressure()
                .unwrap()
                .user_admission_throttled()
        );
        let after_retirement = budget.used_bytes();
        assert!(
            storage
                .retire_immutable_generation_for_test(retired_id)
                .is_err()
        );
        assert_eq!(budget.used_bytes(), after_retirement);
        assert_eq!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count,
            1
        );
        storage
            .retire_immutable_generation_for_test(immutable[0].id())
            .unwrap();
        assert_eq!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count,
            0
        );
    }

    #[test]
    fn automatic_freeze_retains_permanent_handoff_failure_for_diagnostics() {
        let budget = crate::lsm::NodeMemtableBudget::new_with_progress_reserve(4096, 1024).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(budget, 300).unwrap();
        let sink = Arc::new(FakeImmutableSink {
            failure: std::sync::Mutex::new(Some(FlushHandoffError::Rejected(
                "flush worker rejected generation".to_string(),
            ))),
            accepted: std::sync::Mutex::new(Vec::new()),
        });
        storage.set_immutable_memtable_sink(sink.clone());
        let value = "v".repeat(64);

        for id in 51..=53 {
            storage
                .publish_mvcc_delta(MvccDelta {
                    edits: vec![MvccRecordEdit::PutDefault {
                        key: encoded_key(id),
                        start_ts: Timestamp(id as u64),
                        row: encoded_row(id, &value),
                    }],
                })
                .expect("asynchronous handoff failure must not reject a valid publication");
        }

        assert_eq!(
            storage.last_flush_handoff_error(),
            Some(FlushHandoffError::Rejected(
                "flush worker rejected generation".to_string()
            ))
        );
        assert!(
            storage
                .get_default_record(&encoded_key(53), Timestamp(53))
                .unwrap()
                .is_some()
        );

        *sink
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        assert_eq!(storage.try_handoff_pending_immutables().unwrap(), 2);
        assert_eq!(storage.last_flush_handoff_error(), None);
    }

    #[test]
    fn deleting_a_frozen_record_does_not_resurface_from_immutable_queue() {
        let node_budget = crate::lsm::NodeMemtableBudget::new(1024).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(node_budget, 300).unwrap();
        let value = "v".repeat(64);
        let first_key = encoded_key(11);

        for id in 11..=12 {
            storage
                .publish_mvcc_delta(MvccDelta {
                    edits: vec![MvccRecordEdit::PutDefault {
                        key: encoded_key(id),
                        start_ts: Timestamp(id as u64),
                        row: encoded_row(id, &value),
                    }],
                })
                .unwrap();
        }

        storage
            .publish_mvcc_delta(MvccDelta {
                edits: vec![MvccRecordEdit::DeleteDefault {
                    key: first_key.clone(),
                    start_ts: Timestamp(11),
                }],
            })
            .unwrap();

        assert_eq!(
            storage
                .get_default_record(&first_key, Timestamp(11))
                .unwrap(),
            None
        );
        assert!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count
                >= 1
        );
    }

    #[test]
    fn restored_snapshot_larger_than_active_limit_uses_a_read_only_base() {
        let mut restored = InMemoryMvcc::new();
        for id in 4..7 {
            restored
                .publish_mvcc_delta(MvccDelta {
                    edits: vec![MvccRecordEdit::PutDefault {
                        key: encoded_key(id),
                        start_ts: Timestamp(id as u64),
                        row: encoded_row(id, "restored snapshot row"),
                    }],
                })
                .unwrap();
        }

        let budget = crate::lsm::NodeMemtableBudget::new(1024).unwrap();
        let mut restored = MemtableMvcc::from_restored(restored, budget.clone(), 200).unwrap();
        let base_bytes = restored.restored_base_bytes();
        assert!(base_bytes > 200);
        assert_eq!(restored.active_memtable_bytes(), 0);
        assert_eq!(budget.used_bytes(), base_bytes);
        for id in 4..7 {
            assert!(
                restored
                    .get_default_record(&encoded_key(id), Timestamp(id as u64))
                    .unwrap()
                    .is_some()
            );
        }

        restored
            .publish_mvcc_delta(MvccDelta {
                edits: vec![MvccRecordEdit::PutDefault {
                    key: encoded_key(8),
                    start_ts: Timestamp(8),
                    row: encoded_row(8, "new active row"),
                }],
            })
            .unwrap();
        assert!(restored.active_memtable_bytes() > 0);
        assert_eq!(
            budget.used_bytes(),
            base_bytes + restored.active_memtable_bytes()
        );
    }

    #[test]
    fn backend_atomic_publish_rejects_invalid_late_edit_without_partial_publication() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);

        let mut backend = MemtableMvccBackend::default();

        let delta = MvccDelta {
            edits: vec![
                MvccRecordEdit::PutDefault {
                    key: first_key.clone(),
                    start_ts: Timestamp(1),
                    row: encoded_row(1, "valid"),
                },
                MvccRecordEdit::PutDefault {
                    key: second_key,
                    start_ts: Timestamp(2),
                    row: vec![0xff],
                },
            ],
        };

        let error = backend.publish_atomic(delta).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(backend.default.is_empty());
        assert!(backend.locks.is_empty());
        assert!(backend.writes.is_empty());
    }

    #[test]
    fn backend_atomic_publish_rejects_duplicate_record_edits_without_mutation() {
        let key = encoded_key(1);

        let mut backend = MemtableMvccBackend::default();

        let delta = MvccDelta {
            edits: vec![
                MvccRecordEdit::PutDefault {
                    key: key.clone(),
                    start_ts: Timestamp(1),
                    row: encoded_row(1, "first"),
                },
                MvccRecordEdit::PutDefault {
                    key,
                    start_ts: Timestamp(1),
                    row: encoded_row(1, "second"),
                },
            ],
        };

        let error = backend.publish_atomic(delta).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(backend.default.is_empty());
    }

    #[test]
    fn write_cursor_resume_preserves_inclusive_range_edges() {
        let key = encoded_key(1);
        let mut backend = MemtableMvccBackend::default();
        backend.writes.insert(
            key.clone(),
            BTreeMap::from([
                (
                    Timestamp(7),
                    WriteRecord {
                        start_timestamp: Timestamp(5),
                        commit_timestamp: Timestamp(7),
                        op: WriteKind::Delete,
                    },
                ),
                (
                    Timestamp(9),
                    WriteRecord {
                        start_timestamp: Timestamp(8),
                        commit_timestamp: Timestamp(9),
                        op: WriteKind::Delete,
                    },
                ),
            ]),
        );

        let forward = backend
            .write_page(
                &key,
                Included(Timestamp(7)),
                Unbounded,
                Some(Timestamp(6)),
                MvccCursorDirection::Forward,
                1,
            )
            .unwrap();
        assert_eq!(forward.writes[0].0, Timestamp(7));

        let reverse = backend
            .write_page(
                &key,
                Unbounded,
                Included(Timestamp(7)),
                Some(Timestamp(7)),
                MvccCursorDirection::Reverse,
                1,
            )
            .unwrap();
        assert!(reverse.writes.is_empty());
    }

    /// This catches an in-flight tablet read that silently switches from its
    #[test]
    fn historical_reads_and_snapshot_pins_span_active_and_multiple_immutables() {
        let budget = crate::lsm::NodeMemtableBudget::new(4096).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(budget, 500).unwrap();
        let key = encoded_key(71);
        let rows = [
            encoded_row(71, "history-one"),
            encoded_row(71, "history-two"),
            encoded_row(71, "history-three"),
        ];
        let starts = [Timestamp(1), Timestamp(11), Timestamp(21)];
        let commits = [Timestamp(10), Timestamp(20), Timestamp(30)];
        let first = MvccDelta {
            edits: vec![
                MvccRecordEdit::PutDefault {
                    key: key.clone(),
                    start_ts: starts[0],
                    row: rows[0].clone(),
                },
                MvccRecordEdit::PutWrite {
                    key: key.clone(),
                    write_ts: commits[0],
                    write: WriteRecord {
                        start_timestamp: starts[0],
                        commit_timestamp: commits[0],
                        op: WriteKind::Put,
                    },
                },
            ],
        };
        storage.publish_mvcc_delta(first).unwrap();
        let pinned = storage.pin_read_view().unwrap();

        for version in 1..3 {
            storage
                .publish_mvcc_delta(MvccDelta {
                    edits: vec![
                        MvccRecordEdit::PutDefault {
                            key: key.clone(),
                            start_ts: starts[version],
                            row: rows[version].clone(),
                        },
                        MvccRecordEdit::PutWrite {
                            key: key.clone(),
                            write_ts: commits[version],
                            write: WriteRecord {
                                start_timestamp: starts[version],
                                commit_timestamp: commits[version],
                                op: WriteKind::Put,
                            },
                        },
                    ],
                })
                .unwrap();
        }

        assert_eq!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count,
            2
        );
        assert_eq!(
            pinned.read(&key, Timestamp(15)).unwrap(),
            Some(rows[0].clone())
        );
        assert_eq!(
            storage.read(&key, Timestamp(15)).unwrap(),
            Some(rows[0].clone())
        );
        assert_eq!(
            storage.read(&key, Timestamp(25)).unwrap(),
            Some(rows[1].clone())
        );
        assert_eq!(
            storage.read(&key, Timestamp(35)).unwrap(),
            Some(rows[2].clone())
        );

        let (pinned_defaults, _, pinned_writes) = pinned
            .capture_snapshot_state()
            .unwrap()
            .into_snapshot_entries();
        assert_eq!(pinned_defaults.len(), 1);
        assert_eq!(pinned_writes.len(), 1);
        assert_eq!(pinned_defaults[0].row, rows[0]);
    }

    #[test]
    fn locks_in_older_immutables_remain_visible_to_intent_reads() {
        let budget = crate::lsm::NodeMemtableBudget::new(2048).unwrap();
        let mut storage = MemtableMvcc::with_memtable_budget(budget, 300).unwrap();
        let key = encoded_key(72);
        let lock = LockRecord {
            txn_id: TxnId(72),
            primary_key: key.clone(),
            start_timestamp: Timestamp(72),
            ttl_ms: 3_000,
            op: WriteKind::Put,
        };
        storage
            .publish_mvcc_delta(MvccDelta {
                edits: vec![MvccRecordEdit::PutLock {
                    key: key.clone(),
                    lock: lock.clone(),
                }],
            })
            .unwrap();
        storage
            .publish_mvcc_delta(MvccDelta {
                edits: vec![MvccRecordEdit::PutDefault {
                    key: encoded_key(73),
                    start_ts: Timestamp(73),
                    row: encoded_row(73, &"x".repeat(64)),
                }],
            })
            .unwrap();

        assert_eq!(
            storage
                .memtable_pressure()
                .unwrap()
                .immutable_memtable_count,
            1
        );
        assert_eq!(storage.get_lock_record(&key).unwrap(), Some(lock.clone()));
        assert_eq!(
            storage.intent_for_read(&key, Timestamp(80)).unwrap(),
            Some(lock)
        );
        assert_eq!(
            storage
                .scan_intent_page(None, None, None, 16)
                .unwrap()
                .locks
                .len(),
            1
        );
    }

    /// pinned storage generation to records published after the read began.
    #[test]
    fn pinned_read_view_keeps_its_generation_after_live_commit() {
        let key = encoded_key(1);
        let before = encoded_row(1, "before");
        let after = encoded_row(1, "after");
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(
                TxnId(1),
                Timestamp(1),
                Timestamp(2),
                &put_batch(key.clone(), before.clone()),
            )
            .unwrap();
        let pinned = engine.pin_read_view().unwrap();

        engine
            .commit_batch(
                TxnId(2),
                Timestamp(3),
                Timestamp(4),
                &put_batch(key.clone(), after.clone()),
            )
            .unwrap();

        assert_eq!(pinned.read(&key, Timestamp(5)).unwrap(), Some(before));
        assert_eq!(engine.read(&key, Timestamp(5)).unwrap(), Some(after));
    }

    /// Install a valid single-key Put intent for batch atomicity tests.
    fn install_test_put_intent(
        engine: &mut InMemoryMvcc,
        txn_id: TxnId,
        start_ts: Timestamp,
        key: &[u8],
        row_id: i64,
    ) {
        engine
            .prewrite(
                txn_id,
                start_ts,
                key,
                &Mutation::Put(encoded_row(row_id, "intent")),
                key,
                30_000,
            )
            .unwrap();
    }

    #[test]
    fn snapshot_reads_select_latest_visible_version() {
        let key = encoded_key(1);
        let first = encoded_row(1, "first");
        let second = encoded_row(1, "second");
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(
                TxnId(1),
                Timestamp(1),
                Timestamp(2),
                &put_batch(key.clone(), first.clone()),
            )
            .unwrap();

        engine
            .commit_batch(
                TxnId(2),
                Timestamp(3),
                Timestamp(4),
                &put_batch(key.clone(), second.clone()),
            )
            .unwrap();

        assert_eq!(engine.read(&key, Timestamp(1)).unwrap(), None);
        assert_eq!(
            engine.read(&key, Timestamp(2)).unwrap(),
            Some(first.clone())
        );
        assert_eq!(engine.read(&key, Timestamp(3)).unwrap(), Some(first));
        assert_eq!(engine.read(&key, Timestamp(4)).unwrap(), Some(second));
    }

    #[test]
    fn delete_tombstone_hides_only_newer_snapshots() {
        let key = encoded_key(1);
        let row = encoded_row(1, "visible");
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(
                TxnId(1),
                Timestamp(1),
                Timestamp(2),
                &put_batch(key.clone(), row.clone()),
            )
            .unwrap();

        engine
            .commit_batch(
                TxnId(2),
                Timestamp(3),
                Timestamp(4),
                &delete_batch(key.clone()),
            )
            .unwrap();

        assert_eq!(engine.read(&key, Timestamp(3)).unwrap(), Some(row));
        assert_eq!(engine.read(&key, Timestamp(4)).unwrap(), None);
    }

    #[test]
    fn rollback_is_stored_at_start_timestamp_and_skipped_by_reads() {
        let key = encoded_key(1);
        let original = encoded_row(1, "original");
        let delayed = encoded_row(1, "delayed");
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(
                TxnId(1),
                Timestamp(1),
                Timestamp(2),
                &put_batch(key.clone(), original.clone()),
            )
            .unwrap();

        engine
            .backend
            .writes
            .entry(key.clone())
            .or_default()
            .insert(
                Timestamp(3),
                WriteRecord {
                    start_timestamp: Timestamp(3),
                    commit_timestamp: Timestamp(3),
                    op: WriteKind::Rollback,
                },
            );

        assert_eq!(engine.read(&key, Timestamp(3)).unwrap(), Some(original));

        let error = engine
            .commit_batch(
                TxnId(2),
                Timestamp(3),
                Timestamp(5),
                &put_batch(key, delayed),
            )
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
    }

    /// Protects snapshot boundary semantics while newer rollback records are
    /// present in the same key's retained history.
    ///
    /// Realistic bug caught: treating a write committed exactly at the
    /// transaction start timestamp as newer, or treating an unrelated rollback
    /// marker above that timestamp as a committed write, would reject a valid
    /// transaction after the history lookup was narrowed.
    #[test]
    fn write_history_preflight_preserves_snapshot_boundary_and_ignores_newer_rollbacks() {
        let key = encoded_key(1);
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(
                TxnId(1),
                Timestamp(1),
                Timestamp(2),
                &put_batch(key.clone(), encoded_row(1, "older")),
            )
            .unwrap();
        engine
            .commit_batch(
                TxnId(2),
                Timestamp(6),
                Timestamp(8),
                &put_batch(key.clone(), encoded_row(1, "at-boundary")),
            )
            .unwrap();
        engine
            .backend
            .writes
            .entry(key.clone())
            .or_default()
            .insert(
                Timestamp(9),
                WriteRecord {
                    start_timestamp: Timestamp(9),
                    commit_timestamp: Timestamp(9),
                    op: WriteKind::Rollback,
                },
            );

        engine
            .validate_commit_batch(
                TxnId(3),
                Timestamp(8),
                &put_batch(key, encoded_row(1, "valid-at-boundary")),
            )
            .unwrap();
    }

    /// Exact intent retries must preserve one lock/default value, and replaying
    /// its committed outcome must not create another write record.
    ///
    /// Realistic bug caught: a lookup optimization that loses the transaction's
    /// original rollback or commit witness could reject a safe retry or apply
    /// the same logical intent twice.
    #[test]
    fn exact_prewrite_and_intent_commit_replays_are_idempotent() {
        let key = encoded_key(1);
        let row = encoded_row(1, "intent");
        let mutation = Mutation::Put(row.clone());
        let mut engine = InMemoryMvcc::new();

        engine
            .prewrite(TxnId(44), Timestamp(100), &key, &mutation, &key, 3_000)
            .unwrap();
        let prewritten_stats = engine.stats();

        engine
            .prewrite(TxnId(44), Timestamp(100), &key, &mutation, &key, 3_000)
            .unwrap();
        assert_eq!(engine.stats(), prewritten_stats);

        engine
            .commit_intent(TxnId(44), Timestamp(100), Timestamp(110), &key)
            .unwrap();
        engine
            .commit_intent(TxnId(44), Timestamp(100), Timestamp(110), &key)
            .unwrap();

        assert_eq!(engine.read(&key, Timestamp(110)).unwrap(), Some(row));
        assert_eq!(engine.stats().write_records, 1);
        assert_eq!(engine.stats().locks, 0);
    }

    /// Snapshot recovery must validate every persisted write record before the
    /// restored maps become available to normal transaction validation.
    ///
    /// Realistic bug caught: skipping old history in the hot conflict path must
    /// not allow malformed write metadata to enter state through a snapshot.
    #[test]
    fn snapshot_restore_rejects_invalid_write_timestamp_metadata() {
        let key = encoded_key(1);
        let invalid_record = WriteRecord {
            start_timestamp: Timestamp(1),
            commit_timestamp: Timestamp(4),
            op: WriteKind::Put,
        };

        let error = InMemoryMvcc::restore_from_snapshot_entries(
            TableId(1),
            Vec::new(),
            Vec::new(),
            vec![snapshot_proto::WriteEntry {
                key,
                write_timestamp: Some(Timestamp(5).to_proto()),
                record: Some(invalid_record.to_proto().unwrap()),
            }],
        )
        .unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
    }

    #[test]
    fn malformed_rollback_timestamp_is_corruption() {
        let key = encoded_key(1);
        let mut engine = InMemoryMvcc::new();

        engine
            .backend
            .writes
            .entry(key.clone())
            .or_default()
            .insert(
                Timestamp(4),
                WriteRecord {
                    start_timestamp: Timestamp(3),
                    commit_timestamp: Timestamp(4),
                    op: WriteKind::Rollback,
                },
            );

        let error = engine.read(&key, Timestamp(4)).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
    }

    #[test]
    fn write_conflict_rejects_entire_batch() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let winner = encoded_row(2, "winner");
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(
                TxnId(2),
                Timestamp(4),
                Timestamp(5),
                &put_batch(second_key.clone(), winner.clone()),
            )
            .unwrap();

        let losing_batch = BTreeMap::from([
            (first_key.clone(), Mutation::Put(encoded_row(1, "loser"))),
            (second_key.clone(), Mutation::Put(encoded_row(2, "loser"))),
        ]);

        let error = engine
            .commit_batch(TxnId(1), Timestamp(3), Timestamp(6), &losing_batch)
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
        assert_eq!(engine.read(&first_key, Timestamp(6)).unwrap(), None);
        assert_eq!(
            engine.read(&second_key, Timestamp(6)).unwrap(),
            Some(winner)
        );
    }

    #[test]
    fn visible_lock_causes_retryable_conflict() {
        let key = encoded_key(1);
        let mut engine = InMemoryMvcc::new();

        engine.backend.locks.insert(
            key.clone(),
            LockRecord {
                txn_id: TxnId(9),
                primary_key: key.clone(),
                start_timestamp: Timestamp(5),
                ttl_ms: 3_000,
                op: WriteKind::Put,
            },
        );

        assert_eq!(engine.read(&key, Timestamp(4)).unwrap(), None);

        let error = engine.read(&key, Timestamp(5)).unwrap_err();
        assert!(matches!(error, Error::WriteConflict(_)));
    }

    #[test]
    fn same_transaction_lock_is_removed_after_commit() {
        let key = encoded_key(1);
        let row = encoded_row(1, "value");
        let mut engine = InMemoryMvcc::new();

        engine.backend.locks.insert(
            key.clone(),
            LockRecord {
                txn_id: TxnId(1),
                primary_key: key.clone(),
                start_timestamp: Timestamp(1),
                ttl_ms: 3_000,
                op: WriteKind::Put,
            },
        );

        engine
            .commit_batch(
                TxnId(1),
                Timestamp(1),
                Timestamp(2),
                &put_batch(key.clone(), row.clone()),
            )
            .unwrap();

        assert_eq!(engine.stats().locks, 0);
        assert_eq!(engine.read(&key, Timestamp(2)).unwrap(), Some(row));
    }

    #[test]
    fn scan_is_ordered_and_includes_lock_only_candidates() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let third_key = encoded_key(3);
        let first_row = encoded_row(1, "first");
        let third_row = encoded_row(3, "third");
        let mut engine = InMemoryMvcc::new();

        let committed = BTreeMap::from([
            (third_key.clone(), Mutation::Put(third_row.clone())),
            (first_key.clone(), Mutation::Put(first_row.clone())),
        ]);

        engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &committed)
            .unwrap();

        assert_eq!(
            engine.scan(None, None, Timestamp(2)).unwrap(),
            vec![
                (first_key.clone(), first_row),
                (third_key.clone(), third_row),
            ]
        );

        engine.backend.locks.insert(
            second_key.clone(),
            LockRecord {
                txn_id: TxnId(2),
                primary_key: second_key,
                start_timestamp: Timestamp(3),
                ttl_ms: 3_000,
                op: WriteKind::Put,
            },
        );

        let error = engine.scan(None, None, Timestamp(3)).unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
    }

    #[test]
    fn scan_respects_half_open_bounds() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let third_key = encoded_key(3);
        let mut engine = InMemoryMvcc::new();

        let mutations = BTreeMap::from([
            (first_key, Mutation::Put(encoded_row(1, "first"))),
            (second_key.clone(), Mutation::Put(encoded_row(2, "second"))),
            (third_key.clone(), Mutation::Put(encoded_row(3, "third"))),
        ]);

        engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap();

        let rows = engine
            .scan(Some(&second_key), Some(&third_key), Timestamp(2))
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, second_key);
    }

    #[test]
    fn scan_rejects_invalid_order() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let engine = InMemoryMvcc::new();

        let error = engine
            .scan(Some(&second_key), Some(&first_key), Timestamp(1))
            .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));

        let error = engine
            .scan(Some(&first_key), Some(&first_key), Timestamp(1))
            .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn scan_page_is_ordered_bounded_and_resume_after_is_exclusive() {
        // Regression: page implementations that materialize or resume from an
        // inclusive key duplicate the boundary row and can exceed the caller's
        // row or encoded-byte budget.
        let keys = [
            encoded_key(1),
            encoded_key(2),
            encoded_key(3),
            encoded_key(4),
        ];
        let rows = [
            encoded_row(1, "a"),
            encoded_row(2, "bb"),
            encoded_row(3, "ccc"),
            encoded_row(4, "dddd"),
        ];
        let mutations = keys
            .iter()
            .cloned()
            .zip(rows.iter().cloned())
            .map(|(key, row)| (key, Mutation::Put(row)))
            .collect::<BTreeMap<_, _>>();
        let mut engine = InMemoryMvcc::new();
        engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap();

        let first = engine
            .scan_page(None, None, None, Timestamp(2), 2, usize::MAX)
            .unwrap();
        assert_eq!(
            first.rows.iter().map(|(key, _)| key).collect::<Vec<_>>(),
            vec![&keys[0], &keys[1]]
        );
        assert!(first.has_more);

        let second = engine
            .scan_page(
                None,
                None,
                Some(&first.rows.last().unwrap().0),
                Timestamp(2),
                2,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(
            second.rows.iter().map(|(key, _)| key).collect::<Vec<_>>(),
            vec![&keys[2], &keys[3]]
        );
        assert!(!second.has_more);

        let byte_budget = keys[0].len() + rows[0].len();
        let byte_page = engine
            .scan_page(None, None, None, Timestamp(2), 10, byte_budget)
            .unwrap();
        assert_eq!(byte_page.rows.len(), 1);
        assert_eq!(
            byte_page.rows[0].0.len() + byte_page.rows[0].1.len(),
            byte_budget
        );
        assert!(byte_page.has_more);
    }

    #[test]
    fn scan_does_not_skip_write_keys_when_lock_cursor_runs_ahead() {
        let mut engine = InMemoryMvcc::new();

        let mutations = (1_i64..=200)
            .map(|id| (encoded_key(id), Mutation::Put(encoded_row(id, "committed"))))
            .collect::<BTreeMap<_, _>>();

        engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap();

        // Keep this future lock beyond the first bounded write-key page. It
        // must remain invisible to this read timestamp without moving the
        // independent write-family cursor past committed rows.
        let future_lock_key = encoded_key(10_000);
        engine.backend.locks.insert(
            future_lock_key.clone(),
            LockRecord {
                txn_id: TxnId(999),
                primary_key: future_lock_key,
                start_timestamp: Timestamp(100),
                ttl_ms: 30_000,
                op: WriteKind::Put,
            },
        );

        let rows = engine.scan(None, None, Timestamp(2)).unwrap();

        assert_eq!(rows.len(), 200);
        assert_eq!(rows.first().unwrap().0, encoded_key(1));
        assert_eq!(rows.last().unwrap().0, encoded_key(200));
    }

    #[test]
    fn foreground_intent_scan_finds_lock_only_keys_and_filters_future_locks() {
        // A visible-row scan omits a newly inserted key until its transaction
        // commits. Foreground range reads need this bounded lock view to avoid
        // returning a silently incomplete result.
        let keys = [encoded_key(1), encoded_key(2), encoded_key(3)];
        let mut engine = InMemoryMvcc::new();
        for (index, (key, start_timestamp)) in keys.iter().zip([3, 4, 9]).enumerate() {
            engine.backend.locks.insert(
                key.clone(),
                LockRecord {
                    txn_id: TxnId(index as u64 + 1),
                    primary_key: key.clone(),
                    start_timestamp: Timestamp(start_timestamp),
                    ttl_ms: 1_000,
                    op: WriteKind::Put,
                },
            );
        }

        let first = engine
            .scan_conflicting_intents(None, None, None, Timestamp(5), 1, usize::MAX)
            .unwrap();
        assert_eq!(
            first.locks,
            vec![(keys[0].clone(), engine.backend.locks[&keys[0]].clone())]
        );
        assert!(first.has_more);

        let second = engine
            .scan_conflicting_intents(None, None, Some(&keys[0]), Timestamp(5), 1, usize::MAX)
            .unwrap();
        assert_eq!(
            second.locks,
            vec![(keys[1].clone(), engine.backend.locks[&keys[1]].clone())]
        );
        assert!(!second.has_more);

        let error = engine
            .scan_conflicting_intents(None, None, None, Timestamp(5), 1, 1)
            .unwrap_err();
        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn malformed_mutation_key_is_rejected() {
        let mut engine = InMemoryMvcc::new();
        let mutations = BTreeMap::from([(vec![0xff], Mutation::Put(encoded_row(1, "value")))]);

        let error = engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert_eq!(engine.stats(), MvccStats::default());
    }

    #[test]
    fn malformed_put_row_is_rejected() {
        let mut engine = InMemoryMvcc::new();
        let mutations = BTreeMap::from([(encoded_key(1), Mutation::Put(vec![0xff]))]);

        let error = engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert_eq!(engine.stats(), MvccStats::default());
    }

    #[test]
    fn exact_multi_key_replay_is_idempotent() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let mutations = BTreeMap::from([
            (first_key, Mutation::Put(encoded_row(1, "first"))),
            (second_key, Mutation::Put(encoded_row(2, "second"))),
        ]);
        let mut engine = InMemoryMvcc::new();

        assert_eq!(
            engine
                .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations,)
                .unwrap(),
            2
        );

        assert_eq!(
            engine
                .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations,)
                .unwrap(),
            2
        );

        assert_eq!(engine.stats().default_versions, 2);
        assert_eq!(engine.stats().write_records, 2);
    }

    #[test]
    fn partial_replay_is_reported_as_corruption() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let mutations = BTreeMap::from([
            (first_key, Mutation::Put(encoded_row(1, "first"))),
            (second_key.clone(), Mutation::Put(encoded_row(2, "second"))),
        ]);
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap();

        engine
            .backend
            .writes
            .get_mut(&second_key)
            .unwrap()
            .remove(&Timestamp(2));

        let error = engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
    }

    #[test]
    fn missing_default_value_is_corruption() {
        let key = encoded_key(1);
        let mut engine = InMemoryMvcc::new();

        engine
            .backend
            .writes
            .entry(key.clone())
            .or_default()
            .insert(
                Timestamp(2),
                WriteRecord {
                    start_timestamp: Timestamp(1),
                    commit_timestamp: Timestamp(2),
                    op: WriteKind::Put,
                },
            );

        let error = engine.read(&key, Timestamp(2)).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
    }

    #[test]
    fn commit_requires_monotonic_nonzero_metadata() {
        let batch = put_batch(encoded_key(1), encoded_row(1, "value"));
        let mut engine = InMemoryMvcc::new();

        assert!(matches!(
            engine
                .commit_batch(TxnId(0), Timestamp(1), Timestamp(2), &batch,)
                .unwrap_err(),
            Error::InvalidArgument(_)
        ));

        assert!(matches!(
            engine
                .commit_batch(TxnId(1), Timestamp(2), Timestamp(2), &batch,)
                .unwrap_err(),
            Error::InvalidArgument(_)
        ));
    }

    /// Ensures an unresolved lock owned by another transaction is discovered
    /// during the mutation-free commit preflight.
    ///
    /// Realistic bug caught:
    ///
    /// Deferring lock validation until MVCC application could leave a durable WAL
    /// record for a transaction that must lose to an existing lock owner.
    #[test]
    fn preflight_rejects_conflicting_lock_without_mutating_state() {
        let key = encoded_key(1);
        let row = encoded_row(1, "pending");
        let mut engine = InMemoryMvcc::new();

        engine.backend.locks.insert(
            key.clone(),
            LockRecord {
                txn_id: TxnId(9),
                primary_key: key.clone(),
                start_timestamp: Timestamp(1),
                ttl_ms: 3_000,
                op: WriteKind::Put,
            },
        );

        let mutations = put_batch(key, row);
        let stats_before = engine.stats();

        let error = engine
            .validate_commit_batch(TxnId(1), Timestamp(1), &mutations)
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
        assert_eq!(engine.stats(), stats_before);
    }

    /// Ensures a durable rollback marker prevents the corresponding transaction
    /// from being accepted during commit preflight.
    ///
    /// Realistic bug caught:
    ///
    /// A delayed commit request could otherwise be appended after recovery had
    /// already established that the transaction was rolled back.
    #[test]
    fn preflight_rejects_transaction_with_rollback_record() {
        let key = encoded_key(1);
        let mut engine = InMemoryMvcc::new();

        engine
            .backend
            .writes
            .entry(key.clone())
            .or_default()
            .insert(
                Timestamp(1),
                WriteRecord {
                    start_timestamp: Timestamp(1),
                    commit_timestamp: Timestamp(1),
                    op: WriteKind::Rollback,
                },
            );

        let mutations = put_batch(key, encoded_row(1, "delayed"));
        let stats_before = engine.stats();

        let error = engine
            .validate_commit_batch(TxnId(1), Timestamp(1), &mutations)
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
        assert_eq!(engine.stats(), stats_before);
    }

    /// Realistic bug caught: an exact commit replay sees its requested write
    /// record and returns success without noticing another durable outcome for
    /// the same transaction start timestamp.
    #[test]
    fn commit_replay_rejects_multiple_durable_outcomes_for_start_timestamp() {
        let key = encoded_key(1);
        let row = encoded_row(1, "committed");
        let mut engine = InMemoryMvcc::new();
        engine
            .prewrite(
                TxnId(44),
                Timestamp(100),
                &key,
                &Mutation::Put(row),
                &key,
                3_000,
            )
            .unwrap();
        engine
            .commit_intent(TxnId(44), Timestamp(100), Timestamp(110), &key)
            .unwrap();

        engine
            .backend
            .writes
            .entry(key.clone())
            .or_default()
            .insert(
                Timestamp(100),
                WriteRecord {
                    start_timestamp: Timestamp(100),
                    commit_timestamp: Timestamp(100),
                    op: WriteKind::Rollback,
                },
            );

        let error = engine
            .commit_intent(TxnId(44), Timestamp(100), Timestamp(110), &key)
            .unwrap_err();
        assert!(matches!(error, Error::CorruptData(_)));
    }

    #[test]
    fn prewrite_batch_leaves_earlier_key_untouched_when_last_key_conflicts() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        assert!(first_key.as_slice() < second_key.as_slice());

        let mut engine = InMemoryMvcc::new();

        // A separate transaction owns the later key in the ordered batch.
        install_test_put_intent(&mut engine, TxnId(2), Timestamp(20), &second_key, 2);
        let before = engine.stats();

        let mutations = BTreeMap::from([
            (
                first_key.clone(),
                Mutation::Put(encoded_row(1, "must not be installed")),
            ),
            (
                second_key.clone(),
                Mutation::Put(encoded_row(2, "conflicting mutation")),
            ),
        ]);

        let error = engine
            .prewrite_batch(TxnId(1), Timestamp(10), &mutations, &first_key, 30_000)
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
        assert_eq!(engine.stats().default_versions, before.default_versions);
        assert_eq!(engine.stats().locks, before.locks);
        assert_eq!(engine.stats().write_records, before.write_records);
        assert!(!engine.backend.default.contains_key(&first_key));
        assert!(!engine.backend.locks.contains_key(&first_key));
        assert_eq!(
            engine.backend.locks.get(&second_key).unwrap().txn_id,
            TxnId(2)
        );
    }

    #[test]
    fn prewrite_preparation_does_not_publish_before_the_command_boundary() {
        let key = encoded_key(91);
        let mutations = put_batch(key.clone(), encoded_row(91, "prepared"));
        let mut engine = InMemoryMvcc::new();

        let delta = engine
            .prepare_prewrite_batch(TxnId(7), Timestamp(11), &mutations, &key, 30_000)
            .unwrap();

        assert!(!engine.backend.default.contains_key(&key));
        assert!(!engine.backend.locks.contains_key(&key));

        engine.publish_mvcc_delta(delta).unwrap();

        assert!(engine.backend.default.contains_key(&key));
        assert_eq!(engine.backend.locks.get(&key).unwrap().txn_id, TxnId(7));
    }

    #[test]
    fn rollback_preparation_keeps_intent_and_witness_unmodified_until_publish() {
        let key = encoded_key(92);
        let mut engine = InMemoryMvcc::new();
        let mutations = put_batch(key.clone(), encoded_row(92, "rollback"));
        let prewrite = engine
            .prepare_prewrite_batch(TxnId(8), Timestamp(12), &mutations, &key, 30_000)
            .unwrap();
        engine.publish_mvcc_delta(prewrite).unwrap();
        let keys = BTreeSet::from([key.clone()]);

        let rollback = engine
            .prepare_rollback_intents_batch(TxnId(8), Timestamp(12), &keys)
            .unwrap();

        assert!(engine.backend.default.contains_key(&key));
        assert!(engine.backend.locks.contains_key(&key));
        assert!(
            engine
                .backend
                .get_write(&key, Timestamp(12))
                .unwrap()
                .is_none()
        );

        engine.publish_mvcc_delta(rollback).unwrap();

        assert!(!engine.backend.default.contains_key(&key));
        assert!(!engine.backend.locks.contains_key(&key));
        assert_eq!(
            engine
                .backend
                .get_write(&key, Timestamp(12))
                .unwrap()
                .unwrap()
                .op,
            WriteKind::Rollback
        );
    }

    #[test]
    fn commit_intents_batch_leaves_earlier_intent_untouched_when_last_key_conflicts() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        assert!(first_key.as_slice() < second_key.as_slice());

        let mut engine = InMemoryMvcc::new();
        install_test_put_intent(&mut engine, TxnId(1), Timestamp(10), &first_key, 1);
        install_test_put_intent(&mut engine, TxnId(2), Timestamp(20), &second_key, 2);
        let keys = BTreeSet::from([first_key.clone(), second_key.clone()]);

        let error = engine
            .commit_intents_batch(TxnId(1), Timestamp(10), Timestamp(30), &keys)
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
        assert_eq!(engine.stats().default_versions, 2);
        assert_eq!(engine.stats().locks, 2);
        assert_eq!(engine.stats().write_records, 0);
        assert_eq!(
            engine.backend.locks.get(&first_key).unwrap().txn_id,
            TxnId(1)
        );
        assert_eq!(
            engine.backend.locks.get(&second_key).unwrap().txn_id,
            TxnId(2)
        );
    }

    #[test]
    fn rollback_intents_batch_leaves_earlier_intent_untouched_when_last_key_conflicts() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        assert!(first_key.as_slice() < second_key.as_slice());

        let mut engine = InMemoryMvcc::new();
        install_test_put_intent(&mut engine, TxnId(1), Timestamp(10), &first_key, 1);
        install_test_put_intent(&mut engine, TxnId(2), Timestamp(20), &second_key, 2);
        let keys = BTreeSet::from([first_key.clone(), second_key.clone()]);

        let error = engine
            .rollback_intents_batch(TxnId(1), Timestamp(10), &keys)
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
        assert_eq!(engine.stats().default_versions, 2);
        assert_eq!(engine.stats().locks, 2);
        assert_eq!(engine.stats().write_records, 0);
        assert_eq!(
            engine.backend.locks.get(&first_key).unwrap().txn_id,
            TxnId(1)
        );
        assert_eq!(
            engine.backend.locks.get(&second_key).unwrap().txn_id,
            TxnId(2)
        );
    }
}
