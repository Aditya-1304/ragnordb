use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use ragnordb_common::Error;
use ragnordb_server::tasks::{IntentCleanerSchedule, run_intent_cleaner_loop};
use ragnordb_tablet::IntentCleanupReport;
use tokio_util::sync::CancellationToken;

#[test]
fn intent_cleaner_schedule_requires_a_positive_interval() {
    let error = IntentCleanerSchedule::new(Duration::ZERO).unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(message) if message.contains("interval")));
}

#[tokio::test]
async fn intent_cleaner_loop_runs_a_pass_and_stops_on_cancellation() {
    let schedule = IntentCleanerSchedule::new(Duration::from_millis(1)).unwrap();
    let shutdown = CancellationToken::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let callback_runs = runs.clone();
    let callback_shutdown = shutdown.clone();

    tokio::time::timeout(
        Duration::from_secs(1),
        run_intent_cleaner_loop(schedule, shutdown, move || {
            let callback_runs = callback_runs.clone();
            let callback_shutdown = callback_shutdown.clone();
            async move {
                callback_runs.fetch_add(1, Ordering::Relaxed);
                callback_shutdown.cancel();
                Ok(IntentCleanupReport::default())
            }
        }),
    )
    .await
    .expect("cleaner loop should stop after cancellation")
    .unwrap();

    assert_eq!(runs.load(Ordering::Relaxed), 1);
}
