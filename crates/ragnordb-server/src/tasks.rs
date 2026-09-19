//! Bounded server background-task scheduling.
//!
//! The scheduler owns cadence and shutdown only. The transaction/tablet
//! cleaner callback owns routing, status lookup, and replicated dispatch, so a
//! timer cannot bypass the tablet's durability boundary or turn a transient
//! status lookup failure into a rollback.

use std::{future::Future, time::Duration};

use ragnordb_common::{Error, Result};
use ragnordb_tablet::IntentCleanupReport;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::metrics;

/// Cadence for one per-tablet intent-cleaner loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntentCleanerSchedule {
    interval: Duration,
}

impl IntentCleanerSchedule {
    pub fn new(interval: Duration) -> Result<Self> {
        if interval.is_zero() {
            return Err(Error::InvalidArgument(
                "intent cleaner interval must be greater than zero".to_string(),
            ));
        }

        Ok(Self { interval })
    }

    pub fn interval(self) -> Duration {
        self.interval
    }
}

/// Run one cleaner callback per interval until the node cancels the task.
///
/// A failed pass is recorded and logged, then the scheduler continues with
/// the next bounded pass. This is safe because the cleaner itself only
/// dispatches status-validated replicated commands; a failed or uncertain pass
/// leaves the underlying intent available for a later retry.
pub async fn run_intent_cleaner_loop<F, Fut>(
    schedule: IntentCleanerSchedule,
    shutdown: CancellationToken,
    mut clean_once: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<IntentCleanupReport>>,
{
    let mut ticker = tokio::time::interval(schedule.interval());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticker.tick() => {
                metrics::counter_inc("ragnordb_txn_intent_cleaner_runs_total");
                match clean_once().await {
                    Ok(report) => metrics::record_intent_cleaner_report(&report),
                    Err(error) => {
                        metrics::counter_inc("ragnordb_txn_intent_cleaner_failures_total");
                        warn!(error = %error, "transaction intent cleaner pass failed");
                    }
                }
            }
        }
    }
}
