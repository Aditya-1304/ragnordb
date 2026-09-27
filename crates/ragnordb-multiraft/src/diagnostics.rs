//! Opt-in instrumentation shared by the physical Raft host and transport.
//!
//! The detailed pipeline measurements are intended for short diagnostic runs.
//! Keeping them behind one process-wide environment check avoids adding label
//! construction and cross-node timestamp work to ordinary production traffic.

use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

const DIAGNOSTICS_ENV: &str = "RAGNORDB_STAGE35_DIAGNOSTICS";
static ENABLED: OnceLock<bool> = OnceLock::new();

/// Return whether the opt-in Stage 3.5.2b metrics are enabled in this process.
pub(crate) fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var_os(DIAGNOSTICS_ENV).is_some())
}

/// Return a wall-clock timestamp suitable for comparing events across nodes.
///
/// This timestamp is used only for best-effort diagnostic intervals. Consensus
/// ordering and timeout behavior continue to use monotonic clocks exclusively.
pub(crate) fn unix_time_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}
