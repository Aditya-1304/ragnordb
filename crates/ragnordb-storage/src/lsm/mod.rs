//! Tablet-local log-structured storage primitives.
//!
//! Recovery metadata lives here independently of any one segment format. In
//! particular, replicated Raft progress and local A-WAL byte positions remain
//! different types and cannot be ordered against one another.

pub mod command_delta;
pub mod frontier;
pub mod internal_key;
pub mod memory;
pub mod value;

pub use command_delta::{
    CommandDelta, LegacyOutcomeEdit, LogicalOutcomeEdit, MAX_COMMAND_DELTA_BYTES, RetryFloorEdit,
    TabletStorageIdentity, TxnStatusEdit,
};
pub use frontier::{RecoveryFrontier, RecoveryFrontierError, ReplicatedWalMapping};
pub use memory::{
    DEFAULT_NODE_MEMTABLE_BUDGET_BYTES, DEFAULT_NODE_MEMTABLE_PROGRESS_RESERVE_BYTES,
    DEFAULT_TABLET_ACTIVE_MEMTABLE_BYTES, DEFAULT_TABLET_IMMUTABLE_HARD_BYTES,
    DEFAULT_TABLET_IMMUTABLE_HARD_COUNT, DEFAULT_TABLET_IMMUTABLE_MEMTABLE_COUNT,
    DEFAULT_TABLET_IMMUTABLE_SOFT_BYTES, DEFAULT_TABLET_IMMUTABLE_SOFT_COUNT, MemoryClass,
    MemoryReservation, MemtablePressure, NodeMemtableBudget,
};
