use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, watch},
};

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "ragnordb-bench")]
#[command(about = "RagnorDB Milestone 4 native benchmark client")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Execute one SQL statement and print the raw JSON response.
    Exec {
        #[arg(long)]
        addr: String,

        #[arg(long)]
        sql: String,

        #[arg(long, default_value_t = 5_000)]
        timeout_ms: u64,
    },

    /// Create/load the benchmark table.
    Load {
        #[arg(long)]
        addr: String,

        #[arg(long, default_value_t = 100_000)]
        rows: u64,

        #[arg(long, default_value_t = 256)]
        value_bytes: usize,

        #[arg(long, default_value_t = 100)]
        batch_size: u64,

        #[arg(long, default_value_t = 5_000)]
        timeout_ms: u64,

        #[arg(long)]
        create_table: bool,
    },

    /// Run a closed-loop multi-client benchmark.
    Run {
        #[arg(long)]
        addr: String,

        #[arg(long, value_enum)]
        workload: Workload,

        #[arg(long, default_value_t = 1)]
        clients: u32,

        #[arg(long, default_value_t = 60)]
        seconds: u64,

        /// Number of rows present in the benchmark table.
        #[arg(long, default_value_t = 100_000)]
        rows: u64,

        /// Size of the TEXT payload used by point writes.
        #[arg(long, default_value_t = 256)]
        value_bytes: usize,

        /// Used only by mixed.
        #[arg(long, default_value_t = 80)]
        read_percent: u64,

        /// Warmup operations performed by each client.
        #[arg(long, default_value_t = 1_000)]
        warmup: u64,

        /// Rows returned by range-scan. Must not exceed rows.
        #[arg(long, default_value_t = 1_000)]
        scan_rows: u64,

        #[arg(long, default_value_t = 5_000)]
        timeout_ms: u64,

        #[arg(long, default_value_t = 42)]
        seed: u64,

        /// Optional TSV file containing the merged HDR histogram buckets.
        #[arg(long)]
        histogram_out: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum Workload {
    PointRead,
    PointWrite,
    Mixed,
    RangeScan,
}

struct SqlClient {
    stream: TcpStream,
    timeout: Duration,
}

impl SqlClient {
    async fn connect(addr: &str, timeout: Duration) -> Result<Self> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .context("TCP connect timed out")??;

        stream
            .set_nodelay(true)
            .context("enable TCP_NODELAY on benchmark connection")?;

        Ok(Self { stream, timeout })
    }

    async fn execute(&mut self, sql: &str) -> Result<Value> {
        let payload = sql.as_bytes();

        if payload.len() > MAX_FRAME_SIZE {
            bail!("SQL request exceeds the 16 MiB frame limit");
        }

        let length =
            u32::try_from(payload.len()).context("SQL request length does not fit u32 framing")?;

        let response = tokio::time::timeout(self.timeout, async {
            self.stream.write_all(&length.to_le_bytes()).await?;
            self.stream.write_all(payload).await?;
            self.stream.flush().await?;

            let mut header = [0_u8; 4];
            self.stream.read_exact(&mut header).await?;

            let response_len = u32::from_le_bytes(header) as usize;
            if response_len > MAX_FRAME_SIZE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "server response frame is {response_len} bytes, above {MAX_FRAME_SIZE}"
                    ),
                ));
            }

            let mut response = vec![0_u8; response_len];
            self.stream.read_exact(&mut response).await?;
            Ok::<Vec<u8>, std::io::Error>(response)
        })
        .await
        .context("SQL operation timed out")??;

        serde_json::from_slice(&response).context("server response was not valid JSON")
    }
}

fn response_is_success(response: &Value) -> bool {
    response.get("ok").and_then(Value::as_bool).unwrap_or(false)
}

fn response_error_code(response: &Value) -> String {
    response
        .get("error")
        .and_then(|error| error.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN_SERVER_ERROR")
        .to_string()
}

/// SplitMix64 is deterministic and has much better bit mixing than using
/// low bits from a simple LCG. It is sufficient for reproducible benchmark
/// key and operation selection.
#[derive(Clone, Copy)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

fn point_read(row_id: u64) -> String {
    format!("SELECT * FROM bench WHERE id = {row_id}")
}

fn point_write(row_id: u64, value: &str) -> String {
    format!("UPDATE bench SET value = '{value}' WHERE id = {row_id}")
}

fn range_scan(scan_rows: u64) -> String {
    format!("SELECT * FROM bench WHERE id <= {scan_rows}")
}

fn workload_sql(
    workload: Workload,
    random: &mut SplitMix64,
    rows: u64,
    read_percent: u64,
    scan_rows: u64,
    write_value: &str,
) -> String {
    match workload {
        Workload::PointRead => {
            let row_id = random.next() % rows + 1;
            point_read(row_id)
        }

        Workload::PointWrite => {
            let row_id = random.next() % rows + 1;
            point_write(row_id, write_value)
        }

        Workload::Mixed => {
            // Independent draws are intentional.
            // Key selection must not accidentally determine operation type.
            let row_id = random.next() % rows + 1;
            let operation_draw = random.next() % 100;

            if operation_draw < read_percent {
                point_read(row_id)
            } else {
                point_write(row_id, write_value)
            }
        }

        Workload::RangeScan => range_scan(scan_rows),
    }
}

struct WorkerStats {
    attempted: u64,
    successful: u64,
    failed: u64,
    latency_us: Histogram<u64>,
    errors: BTreeMap<String, u64>,
}

impl WorkerStats {
    fn new() -> Result<Self> {
        Ok(Self {
            attempted: 0,
            successful: 0,
            failed: 0,
            latency_us: Histogram::new(3).context("create HDR latency histogram")?,
            errors: BTreeMap::new(),
        })
    }

    fn record_success(&mut self, latency: Duration) -> Result<()> {
        self.successful += 1;
        let micros = u64::try_from(latency.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        self.latency_us
            .record(micros)
            .context("record successful request latency")
    }

    fn record_failure(&mut self, code: impl Into<String>) {
        self.failed += 1;
        *self.errors.entry(code.into()).or_insert(0) += 1;
    }
}

#[derive(Clone)]
struct RunOptions {
    addr: Arc<str>,
    workload: Workload,
    seconds: u64,
    rows: u64,
    value_bytes: usize,
    read_percent: u64,
    warmup: u64,
    scan_rows: u64,
    timeout: Duration,
    seed: u64,
}

async fn run_worker(
    client_number: u32,
    options: RunOptions,
    ready_tx: mpsc::Sender<std::result::Result<(), String>>,
    mut start_rx: watch::Receiver<Option<Instant>>,
) -> Result<WorkerStats> {
    let mut client = match SqlClient::connect(&options.addr, options.timeout).await {
        Ok(client) => client,
        Err(error) => {
            let message = format!("client {client_number} connect failed: {error:#}");
            let _ = ready_tx.send(Err(message.clone())).await;
            bail!(message);
        }
    };

    let client_seed =
        options.seed ^ (u64::from(client_number) + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut random = SplitMix64(client_seed);

    // Keep the update payload at the configured benchmark size.
    // Different clients use a different ASCII byte, but every value has
    // exactly value_bytes bytes.
    let write_byte = b'a' + (client_number % 26) as u8;
    let write_value = String::from_utf8(vec![write_byte; options.value_bytes])
        .expect("ASCII benchmark payload must be UTF-8");

    // Warmup uses exactly the same workload generator as the measured phase.
    for operation in 0..options.warmup {
        let sql = workload_sql(
            options.workload,
            &mut random,
            options.rows,
            options.read_percent,
            options.scan_rows,
            &write_value,
        );

        let response = match client.execute(&sql).await {
            Ok(response) => response,
            Err(error) => {
                let message = format!(
                    "client {client_number} warmup transport failure at operation {operation}: {error:#}"
                );
                let _ = ready_tx.send(Err(message.clone())).await;
                bail!(message);
            }
        };

        if !response_is_success(&response) {
            let message = format!(
                "client {client_number} warmup SQL failure at operation {operation}: {response}"
            );
            let _ = ready_tx.send(Err(message.clone())).await;
            bail!(message);
        }
    }

    ready_tx
        .send(Ok(()))
        .await
        .context("send benchmark worker ready state")?;

    while start_rx.borrow().is_none() {
        start_rx
            .changed()
            .await
            .context("benchmark start channel closed")?;
    }

    let start = (*start_rx.borrow()).context("benchmark start instant missing")?;
    let deadline = start + Duration::from_secs(options.seconds);

    let mut stats = WorkerStats::new()?;

    while Instant::now() < deadline {
        let sql = workload_sql(
            options.workload,
            &mut random,
            options.rows,
            options.read_percent,
            options.scan_rows,
            &write_value,
        );

        stats.attempted += 1;
        let operation_start = Instant::now();

        match client.execute(&sql).await {
            Ok(response) if response_is_success(&response) => {
                stats.record_success(operation_start.elapsed())?;
            }

            Ok(response) => {
                stats.record_failure(response_error_code(&response));
            }

            Err(_) => {
                // The TCP stream may no longer be synchronized after a framing,
                // EOF, timeout, or connection failure. Record the failure and
                // stop this worker. The complete run will be marked invalid.
                stats.record_failure("TRANSPORT");
                break;
            }
        }
    }

    Ok(stats)
}

#[derive(Debug, Serialize)]
struct RunReport {
    benchmark: &'static str,
    load_model: &'static str,
    addr: String,
    workload: Workload,
    clients: u32,
    dataset_rows: u64,
    value_bytes: usize,
    read_percent: Option<u64>,
    scan_rows: Option<u64>,
    duration_seconds_requested: u64,
    elapsed_seconds: f64,
    warmup_operations_per_client: u64,
    attempted_operations: u64,
    successful_operations: u64,
    failed_operations: u64,
    attempted_ops_per_second: f64,
    successful_ops_per_second: f64,
    p50_us: Option<u64>,
    p95_us: Option<u64>,
    p99_us: Option<u64>,
    p999_us: Option<u64>,
    max_us: Option<u64>,
    error_counts: BTreeMap<String, u64>,
    valid_run: bool,
    seed: u64,
    histogram_file: Option<String>,
}

fn write_histogram(path: &Path, histogram: &Histogram<u64>) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create histogram directory {}", parent.display()))?;
    }

    let mut file =
        File::create(path).with_context(|| format!("create histogram {}", path.display()))?;

    writeln!(file, "latency_us\tcount")?;
    for value in histogram.iter_recorded() {
        writeln!(
            file,
            "{}\t{}",
            value.value_iterated_to(),
            value.count_since_last_iteration()
        )?;
    }

    file.flush()?;
    Ok(())
}

async fn exec_one(addr: String, sql: String, timeout_ms: u64) -> Result<()> {
    let mut client = SqlClient::connect(&addr, Duration::from_millis(timeout_ms)).await?;
    let response = client.execute(&sql).await?;

    println!("{}", serde_json::to_string_pretty(&response)?);

    if !response_is_success(&response) {
        bail!("SQL statement failed");
    }

    Ok(())
}

async fn load_table(
    addr: String,
    rows: u64,
    value_bytes: usize,
    batch_size: u64,
    timeout_ms: u64,
    create_table: bool,
) -> Result<()> {
    if rows == 0 {
        bail!("rows must be greater than zero");
    }

    if batch_size == 0 {
        bail!("batch-size must be greater than zero");
    }

    let timeout = Duration::from_millis(timeout_ms);
    let mut client = SqlClient::connect(&addr, timeout).await?;

    if create_table {
        let response = client
            .execute("CREATE TABLE bench (id INT PRIMARY KEY, value TEXT NOT NULL)")
            .await?;

        if !response_is_success(&response) {
            bail!("creating benchmark table failed: {response}");
        }
    }

    let value = "x".repeat(value_bytes);
    let started = Instant::now();

    let mut first = 1_u64;
    while first <= rows {
        let last = first.saturating_add(batch_size - 1).min(rows);

        let mut sql = String::from("INSERT INTO bench (id, value) VALUES ");

        for row_id in first..=last {
            if row_id != first {
                sql.push_str(", ");
            }
            sql.push('(');
            sql.push_str(&row_id.to_string());
            sql.push_str(", '");
            sql.push_str(&value);
            sql.push_str("')");
        }

        let response = client.execute(&sql).await?;
        if !response_is_success(&response) {
            bail!("load failed for rows {first}..={last}: {response}");
        }

        first = last + 1;
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "loaded_rows": rows,
            "value_bytes": value_bytes,
            "batch_size": batch_size,
            "elapsed_seconds": started.elapsed().as_secs_f64()
        }))?
    );

    Ok(())
}

struct BenchmarkConfig {
    addr: String,
    workload: Workload,
    clients: u32,
    seconds: u64,
    rows: u64,
    value_bytes: usize,
    read_percent: u64,
    warmup: u64,
    scan_rows: u64,
    timeout_ms: u64,
    seed: u64,
    histogram_out: Option<PathBuf>,
}

async fn run_benchmark(config: BenchmarkConfig) -> Result<()> {
    let BenchmarkConfig {
        addr,
        workload,
        clients,
        seconds,
        rows,
        value_bytes,
        read_percent,
        warmup,
        scan_rows,
        timeout_ms,
        seed,
        histogram_out,
    } = config;

    if clients == 0 {
        bail!("clients must be greater than zero");
    }

    if clients > 1_024 {
        bail!("clients above 1024 are rejected by this benchmark harness");
    }

    if seconds == 0 {
        bail!("seconds must be greater than zero");
    }

    if rows == 0 {
        bail!("rows must be greater than zero");
    }

    if read_percent > 100 {
        bail!("read-percent must be between 0 and 100");
    }

    if scan_rows == 0 || scan_rows > rows {
        bail!("scan-rows must be in 1..=rows");
    }

    if matches!(workload, Workload::RangeScan) && scan_rows > 10_000 {
        bail!(
            "network range-scan is capped at 10,000 rows while the protocol has a 16 MiB frame limit"
        );
    }

    let options = RunOptions {
        addr: Arc::from(addr.as_str()),
        workload,
        seconds,
        rows,
        value_bytes,
        read_percent,
        warmup,
        scan_rows,
        timeout: Duration::from_millis(timeout_ms),
        seed,
    };

    let (ready_tx, mut ready_rx) =
        mpsc::channel::<std::result::Result<(), String>>(clients as usize);
    let (start_tx, start_rx) = watch::channel::<Option<Instant>>(None);

    let mut handles = Vec::with_capacity(clients as usize);

    for client_number in 0..clients {
        handles.push(tokio::spawn(run_worker(
            client_number,
            options.clone(),
            ready_tx.clone(),
            start_rx.clone(),
        )));
    }

    drop(ready_tx);

    for _ in 0..clients {
        match ready_rx.recv().await {
            Some(Ok(())) => {}
            Some(Err(message)) => {
                for handle in &handles {
                    handle.abort();
                }
                bail!("warmup failed: {message}");
            }
            None => {
                for handle in &handles {
                    handle.abort();
                }
                bail!("worker readiness channel closed before all clients completed warmup");
            }
        }
    }

    let start = Instant::now();
    start_tx
        .send(Some(start))
        .context("release benchmark workers")?;

    let mut aggregate = WorkerStats::new()?;

    for handle in handles {
        let worker = handle.await.context("benchmark worker panicked")??;

        aggregate.attempted += worker.attempted;
        aggregate.successful += worker.successful;
        aggregate.failed += worker.failed;

        aggregate
            .latency_us
            .add(&worker.latency_us)
            .context("merge worker HDR histogram")?;

        for (code, count) in worker.errors {
            *aggregate.errors.entry(code).or_insert(0) += count;
        }
    }

    let elapsed = start.elapsed();
    let elapsed_seconds = elapsed.as_secs_f64().max(f64::EPSILON);

    if let Some(path) = histogram_out.as_deref() {
        write_histogram(path, &aggregate.latency_us)?;
    }

    let has_success = aggregate.successful > 0;

    let report = RunReport {
        benchmark: "RagnorDB Milestone 4",
        load_model: "closed-loop",
        addr,
        workload,
        clients,
        dataset_rows: rows,
        value_bytes,
        read_percent: matches!(workload, Workload::Mixed).then_some(read_percent),
        scan_rows: matches!(workload, Workload::RangeScan).then_some(scan_rows),
        duration_seconds_requested: seconds,
        elapsed_seconds,
        warmup_operations_per_client: warmup,
        attempted_operations: aggregate.attempted,
        successful_operations: aggregate.successful,
        failed_operations: aggregate.failed,
        attempted_ops_per_second: aggregate.attempted as f64 / elapsed_seconds,
        successful_ops_per_second: aggregate.successful as f64 / elapsed_seconds,
        p50_us: has_success.then(|| aggregate.latency_us.value_at_quantile(0.50)),
        p95_us: has_success.then(|| aggregate.latency_us.value_at_quantile(0.95)),
        p99_us: has_success.then(|| aggregate.latency_us.value_at_quantile(0.99)),
        p999_us: has_success.then(|| aggregate.latency_us.value_at_quantile(0.999)),
        max_us: has_success.then(|| aggregate.latency_us.max()),
        error_counts: aggregate.errors,
        valid_run: aggregate.failed == 0 && aggregate.successful > 0,
        seed,
        histogram_file: histogram_out.map(|path| path.display().to_string()),
    };

    println!("{}", serde_json::to_string_pretty(&report)?);

    if !report.valid_run {
        bail!("benchmark run is invalid because one or more operations failed");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Exec {
            addr,
            sql,
            timeout_ms,
        } => exec_one(addr, sql, timeout_ms).await,

        Command::Load {
            addr,
            rows,
            value_bytes,
            batch_size,
            timeout_ms,
            create_table,
        } => {
            load_table(
                addr,
                rows,
                value_bytes,
                batch_size,
                timeout_ms,
                create_table,
            )
            .await
        }

        Command::Run {
            addr,
            workload,
            clients,
            seconds,
            rows,
            value_bytes,
            read_percent,
            warmup,
            scan_rows,
            timeout_ms,
            seed,
            histogram_out,
        } => {
            run_benchmark(BenchmarkConfig {
                addr,
                workload,
                clients,
                seconds,
                rows,
                value_bytes,
                read_percent,
                warmup,
                scan_rows,
                timeout_ms,
                seed,
                histogram_out,
            })
            .await
        }
    }
}
