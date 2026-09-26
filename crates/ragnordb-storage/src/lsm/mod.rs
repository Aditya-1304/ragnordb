//! Tablet-local log-structured storage primitives.
//!
//! Recovery metadata lives here independently of any one segment format. In
//! particular, replicated Raft progress and local A-WAL byte positions remain
//! different types and cannot be ordered against one another.

pub mod frontier;

pub use frontier::{RecoveryFrontier, RecoveryFrontierError, ReplicatedWalMapping};
