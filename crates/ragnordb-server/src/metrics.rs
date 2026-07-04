use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn init_metrics() {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");
    PROMETHEUS_HANDLE
        .set(handle)
        .unwrap_or_else(|_| panic!("metrics already initialized"));

    metrics::describe_counter!(
        "RagnorDB_connections_accepted_total",
        "Total connections accepted"
    );
    metrics::describe_gauge!(
        "RagnorDB_connections_active",
        "Currently active connections"
    );
    metrics::describe_counter!(
        "RagnorDB_requests_received_total",
        "Total SQL requests received"
    );
    metrics::describe_counter!(
        "RagnorDB_requests_ok_total",
        "Requests that returned success"
    );
    metrics::describe_counter!(
        "RagnorDB_requests_error_total",
        "Requests that returned an error"
    );
}

pub fn render_metrics() -> String {
    match PROMETHEUS_HANDLE.get() {
        Some(handle) => handle.render(),
        None => String::from("# metrics not initialized"),
    }
}

pub fn counter_inc(name: &'static str) {
    metrics::counter!(name).increment(1);
}

pub fn gauge_set(name: &'static str, value: f64) {
    metrics::gauge!(name).set(value);
}
