use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;
use tracing::warn;

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn init_metrics() {
    if PROMETHEUS_HANDLE.get().is_some() {
        return;
    }

    match PrometheusBuilder::new().install_recorder() {
        Ok(handle) => {
            let _ = PROMETHEUS_HANDLE.set(handle);
            describe_metrics();
        }
        Err(error) => {
            warn!(
                error = %error,
                "metrics recorder already installed or unavailable"
            );
        }
    }
}

fn describe_metrics() {
    metrics::describe_counter!(
        "RagnorDB_connections_accepted_total",
        "Total client connections accepted"
    );

    metrics::describe_gauge!(
        "RagnorDB_connections_active",
        "Currently active client connections"
    );

    metrics::describe_counter!(
        "RagnorDB_requests_received_total",
        "Total SQL requests received"
    );

    metrics::describe_counter!(
        "RagnorDB_requests_success_total",
        "SQL requests that completed successfully"
    );

    metrics::describe_counter!(
        "RagnorDB_requests_error_total",
        "SQL requests that returned an error"
    );

    metrics::describe_counter!(
        "RagnorDB_response_rows_read_total",
        "Rows reported as read across successful SQL responses"
    );

    metrics::describe_counter!(
        "RagnorDB_response_rows_written_total",
        "Rows reported as written across successful SQL responses"
    );
}

pub fn render_metrics() -> String {
    match PROMETHEUS_HANDLE.get() {
        Some(handle) => handle.render(),
        None => String::from("# metrics not initialized"),
    }
}

pub fn counter_inc(name: &'static str) {
    counter_add(name, 1);
}

pub fn counter_add(name: &'static str, value: u64) {
    metrics::counter!(name).increment(value);
}

pub fn gauge_set(name: &'static str, value: f64) {
    metrics::gauge!(name).set(value);
}
