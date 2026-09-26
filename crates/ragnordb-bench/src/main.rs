use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;
use ragnordb_common::protocol::{ClientRequestV2, encode_client_request_v2};
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

        /// SQL table identifier used by this load. Keeping the identifier
        /// explicit lets the release gate exercise independent tablet groups
        /// without changing the benchmark client's deterministic workload.
        #[arg(long, default_value = "bench")]
        table: String,

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

        /// Wire protocol used for the load. V2 preserves client identity and
        /// request ordering across the distributed SQL path.
        #[arg(long, value_enum, default_value_t = Protocol::V1)]
        protocol: Protocol,

        /// Non-zero V2 client identity. Ignored by V1.
        #[arg(long, default_value_t = 1)]
        client_id: u128,

        /// Non-zero V2 session epoch. Ignored by V1.
        #[arg(long, default_value_t = 1)]
        session_epoch: u64,
    },

    /// Run a closed-loop multi-client benchmark.
    Run {
        #[arg(long)]
        addr: String,

        /// SQL table identifier used by all workers in this run.
        #[arg(long, default_value = "bench")]
        table: String,

        /// Optional comma-separated table identifiers used by cross-tablet transactions.
        #[arg(long, value_delimiter = ',')]
        participant_tables: Vec<String>,

        /// Number of writes issued between BEGIN and COMMIT for transaction workloads.
        #[arg(long, default_value_t = 1)]
        txn_writes: usize,

        /// Key distribution used by the transaction-contention workload.
        #[arg(long, value_enum, default_value_t = ContentionDistribution::Disjoint)]
        contention: ContentionDistribution,

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

        /// Wire protocol used for the run. V2 is required for distributed
        /// SQL routing and request deduplication measurements.
        #[arg(long, value_enum, default_value_t = Protocol::V1)]
        protocol: Protocol,

        /// Base non-zero V2 client identity. Each worker receives a distinct
        /// identity by adding its worker number.
        #[arg(long, default_value_t = 1)]
        client_id: u128,

        /// Non-zero V2 session epoch shared by this benchmark invocation.
        #[arg(long, default_value_t = 1)]
        session_epoch: u64,
    },
}

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum Workload {
    PointRead,
    PointWrite,
    Mixed,
    RangeScan,
    SingleShardTxn,
    CrossShardTxn,
    TxnContention,
}

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum ContentionDistribution {
    Disjoint,
    Moderate,
    Hotspot,
}

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum Protocol {
    V1,
    V2,
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

    async fn execute_frame_with_metrics(&mut self, frame: &[u8]) -> Result<(Value, usize, usize)> {
        let response = tokio::time::timeout(self.timeout, async {
            self.stream.write_all(frame).await?;
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

        let response_bytes = 4_usize
            .checked_add(response.len())
            .context("response byte count overflowed")?;
        let response =
            serde_json::from_slice(&response).context("server response was not valid JSON")?;
        Ok((response, frame.len(), response_bytes))
    }

    async fn execute_frame(&mut self, frame: &[u8]) -> Result<Value> {
        self.execute_frame_with_metrics(frame)
            .await
            .map(|(response, _, _)| response)
    }

    async fn execute(&mut self, sql: &str) -> Result<Value> {
        let payload = sql.as_bytes();

        if payload.len() > MAX_FRAME_SIZE {
            bail!("SQL request exceeds the 16 MiB frame limit");
        }

        let length =
            u32::try_from(payload.len()).context("SQL request length does not fit u32 framing")?;
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&length.to_le_bytes());
        frame.extend_from_slice(payload);
        self.execute_frame(&frame).await
    }

    async fn execute_v2_with_metrics(
        &mut self,
        client_id: u128,
        session_epoch: u64,
        request_sequence: u64,
        sql: &str,
    ) -> Result<(Value, usize, usize)> {
        let frame = encode_client_request_v2(&ClientRequestV2 {
            protocol_version: 2,
            client_id,
            client_session_epoch: session_epoch,
            request_sequence,
            acknowledged_through: request_sequence.checked_sub(1),
            statement_timeout_ms: self
                .timeout
                .as_millis()
                .try_into()
                .context("benchmark timeout does not fit the V2 request field")?,
            sql: sql.to_owned(),
        })
        .map_err(|error| anyhow!("encode V2 benchmark request: {error}"))?;

        self.execute_frame_with_metrics(&frame).await
    }
}

async fn execute_benchmark_request(
    client: &mut SqlClient,
    protocol: Protocol,
    client_id: u128,
    session_epoch: u64,
    request_sequence: u64,
    sql: &str,
) -> Result<Value> {
    execute_benchmark_request_with_metrics(
        client,
        protocol,
        client_id,
        session_epoch,
        request_sequence,
        sql,
    )
    .await
    .map(|(response, _, _)| response)
}

async fn execute_benchmark_request_with_metrics(
    client: &mut SqlClient,
    protocol: Protocol,
    client_id: u128,
    session_epoch: u64,
    request_sequence: u64,
    sql: &str,
) -> Result<(Value, usize, usize)> {
    match protocol {
        Protocol::V1 => {
            let payload = sql.as_bytes();
            if payload.len() > MAX_FRAME_SIZE {
                bail!("SQL request exceeds the 16 MiB frame limit");
            }
            let length = u32::try_from(payload.len())
                .context("SQL request length does not fit u32 framing")?;
            let mut frame = Vec::with_capacity(4 + payload.len());
            frame.extend_from_slice(&length.to_le_bytes());
            frame.extend_from_slice(payload);
            client.execute_frame_with_metrics(&frame).await
        }
        Protocol::V2 => {
            client
                .execute_v2_with_metrics(client_id, session_epoch, request_sequence, sql)
                .await
        }
    }
}

fn response_is_success(response: &Value) -> bool {
    response.get("ok").and_then(Value::as_bool).unwrap_or(false)
}

fn response_is_retryable(response: &Value) -> bool {
    response
        .get("error")
        .and_then(|error| error.get("retryable"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
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

fn point_read(table: &str, row_id: u64) -> String {
    format!("SELECT * FROM {table} WHERE id = {row_id}")
}

fn point_write(table: &str, row_id: u64, value: &str) -> String {
    format!("UPDATE {table} SET value = '{value}' WHERE id = {row_id}")
}

fn range_scan(table: &str, scan_rows: u64) -> String {
    format!("SELECT * FROM {table} WHERE id <= {scan_rows}")
}

/// Validate the restricted SQL identifier grammar used by generated workload
/// statements. The benchmark client intentionally accepts identifiers rather
/// than arbitrary SQL fragments so parallel-table measurements cannot turn a
/// command-line value into an injection or a second statement.
fn validate_table_name(table: &str) -> Result<()> {
    let mut characters = table.chars();
    let Some(first) = characters.next() else {
        bail!("table must not be empty");
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        bail!(
            "table must be a simple SQL identifier containing ASCII letters, digits, and underscores"
        );
    }
    Ok(())
}

fn workload_sql(
    workload: Workload,
    random: &mut SplitMix64,
    table: &str,
    rows: u64,
    read_percent: u64,
    scan_rows: u64,
    write_value: &str,
) -> String {
    match workload {
        Workload::PointRead => {
            let row_id = random.next() % rows + 1;
            point_read(table, row_id)
        }

        Workload::PointWrite => {
            let row_id = random.next() % rows + 1;
            point_write(table, row_id, write_value)
        }

        Workload::Mixed => {
            // Independent draws are intentional.
            // Key selection must not accidentally determine operation type.
            let row_id = random.next() % rows + 1;
            let operation_draw = random.next() % 100;

            if operation_draw < read_percent {
                point_read(table, row_id)
            } else {
                point_write(table, row_id, write_value)
            }
        }

        Workload::RangeScan => range_scan(table, scan_rows),
        Workload::SingleShardTxn | Workload::CrossShardTxn | Workload::TxnContention => {
            let row_id = random.next() % rows + 1;
            point_write(table, row_id, write_value)
        }
    }
}

fn transaction_workload_sql(
    workload: Workload,
    random: &mut SplitMix64,
    tables: &[Arc<str>],
    rows: u64,
    txn_writes: usize,
    contention: ContentionDistribution,
    write_value: &str,
) -> Vec<String> {
    let mut statements = vec!["BEGIN".to_string()];
    for index in 0..txn_writes {
        let row_id = match workload {
            Workload::TxnContention => match contention {
                ContentionDistribution::Disjoint => random.next() % rows + 1,
                ContentionDistribution::Moderate => random.next() % 100 + 1,
                ContentionDistribution::Hotspot => 1,
            },
            _ => random.next() % rows + 1,
        };
        let table = if matches!(workload, Workload::CrossShardTxn) {
            &tables[index % tables.len()]
        } else {
            &tables[0]
        };
        statements.push(point_write(table, row_id, write_value));
    }
    statements.push("COMMIT".to_string());
    statements
}

// Each argument is an independently configured workload input; keeping them
// explicit makes the dimensions consumed by this generator easy to audit.
#[allow(clippy::too_many_arguments)]
fn workload_statements(
    workload: Workload,
    random: &mut SplitMix64,
    table: &str,
    participant_tables: &[Arc<str>],
    rows: u64,
    read_percent: u64,
    scan_rows: u64,
    txn_writes: usize,
    contention: ContentionDistribution,
    write_value: &str,
) -> Vec<String> {
    match workload {
        Workload::SingleShardTxn | Workload::CrossShardTxn | Workload::TxnContention => {
            transaction_workload_sql(
                workload,
                random,
                participant_tables,
                rows,
                txn_writes,
                contention,
                write_value,
            )
        }
        _ => vec![workload_sql(
            workload,
            random,
            table,
            rows,
            read_percent,
            scan_rows,
            write_value,
        )],
    }
}

struct WorkerStats {
    attempted: u64,
    successful: u64,
    failed: u64,
    latency_us: Histogram<u64>,
    errors: BTreeMap<String, u64>,
    request_bytes: u64,
    response_bytes: u64,
}

impl WorkerStats {
    fn new() -> Result<Self> {
        Ok(Self {
            attempted: 0,
            successful: 0,
            failed: 0,
            latency_us: Histogram::new(3).context("create HDR latency histogram")?,
            errors: BTreeMap::new(),
            request_bytes: 0,
            response_bytes: 0,
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
    table: Arc<str>,
    participant_tables: Arc<Vec<Arc<str>>>,
    workload: Workload,
    seconds: u64,
    rows: u64,
    value_bytes: usize,
    read_percent: u64,
    warmup: u64,
    scan_rows: u64,
    txn_writes: usize,
    contention: ContentionDistribution,
    timeout: Duration,
    seed: u64,
    protocol: Protocol,
    client_id: u128,
    session_epoch: u64,
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
    let worker_client_id = options
        .client_id
        .checked_add(u128::from(client_number))
        .context("V2 client identity range overflowed")?;
    let mut request_sequence = 1_u64;

    // Keep the update payload at the configured benchmark size.
    // Different clients use a different ASCII byte, but every value has
    // exactly value_bytes bytes.
    let write_byte = b'a' + (client_number % 26) as u8;
    let write_value = String::from_utf8(vec![write_byte; options.value_bytes])
        .expect("ASCII benchmark payload must be UTF-8");

    // Warmup uses exactly the same operation generator as the measured phase.
    for operation in 0..options.warmup {
        let statements = workload_statements(
            options.workload,
            &mut random,
            &options.table,
            &options.participant_tables,
            options.rows,
            options.read_percent,
            options.scan_rows,
            options.txn_writes,
            options.contention,
            &write_value,
        );
        for (statement_index, sql) in statements.iter().enumerate() {
            let response = match execute_benchmark_request(
                &mut client,
                options.protocol,
                worker_client_id,
                options.session_epoch,
                request_sequence,
                sql,
            )
            .await
            {
                Ok(response) => response,
                Err(error) => {
                    let message = format!(
                        "client {client_number} warmup transport failure at operation {operation}, statement {statement_index}: {error:#}"
                    );
                    let _ = ready_tx.send(Err(message.clone())).await;
                    bail!(message);
                }
            };
            request_sequence = request_sequence
                .checked_add(1)
                .context("benchmark request sequence exhausted during warmup")?;
            if !response_is_success(&response) {
                let message = format!(
                    "client {client_number} warmup SQL failure at operation {operation}, statement {statement_index}: {response}"
                );
                let _ = ready_tx.send(Err(message.clone())).await;
                bail!(message);
            }
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
        let statements = workload_statements(
            options.workload,
            &mut random,
            &options.table,
            &options.participant_tables,
            options.rows,
            options.read_percent,
            options.scan_rows,
            options.txn_writes,
            options.contention,
            &write_value,
        );

        stats.attempted += 1;
        let operation_start = Instant::now();
        let mut operation_success = true;
        let mut failure_code = None;
        let mut transport_failed = false;

        for sql in &statements {
            match execute_benchmark_request_with_metrics(
                &mut client,
                options.protocol,
                worker_client_id,
                options.session_epoch,
                request_sequence,
                sql,
            )
            .await
            {
                Ok((response, request_bytes, response_bytes)) => {
                    stats.request_bytes = stats.request_bytes.saturating_add(
                        u64::try_from(request_bytes)
                            .context("request byte count does not fit u64")?,
                    );
                    stats.response_bytes = stats.response_bytes.saturating_add(
                        u64::try_from(response_bytes)
                            .context("response byte count does not fit u64")?,
                    );
                    request_sequence = request_sequence
                        .checked_add(1)
                        .context("benchmark request sequence exhausted")?;
                    if !response_is_success(&response) {
                        operation_success = false;
                        failure_code = Some(response_error_code(&response));
                        break;
                    }
                }
                Err(_) => {
                    operation_success = false;
                    transport_failed = true;
                    failure_code = Some("TRANSPORT".to_string());
                    request_sequence = request_sequence
                        .checked_add(1)
                        .context("benchmark request sequence exhausted")?;
                    break;
                }
            }
        }

        if operation_success {
            stats.record_success(operation_start.elapsed())?;
        } else {
            stats
                .record_failure(failure_code.unwrap_or_else(|| "UNKNOWN_SERVER_ERROR".to_string()));
        }
        if transport_failed {
            // The TCP stream may no longer be synchronized after a framing,
            // EOF, timeout, or connection failure. The complete run is invalid.
            break;
        }
    }

    Ok(stats)
}
#[derive(Debug, Serialize)]
struct RunReport {
    benchmark: &'static str,
    load_model: &'static str,
    addr: String,
    table: String,
    participant_tables: Vec<String>,
    txn_writes: usize,
    contention: ContentionDistribution,
    protocol: Protocol,
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
    measured_request_bytes: u64,
    measured_response_bytes: u64,
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

struct LoadConfig {
    addr: String,
    table: String,
    rows: u64,
    value_bytes: usize,
    batch_size: u64,
    timeout_ms: u64,
    create_table: bool,
    protocol: Protocol,
    client_id: u128,
    session_epoch: u64,
}

async fn load_table(config: LoadConfig) -> Result<()> {
    let LoadConfig {
        addr,
        table,
        rows,
        value_bytes,
        batch_size,
        timeout_ms,
        create_table,
        protocol,
        client_id,
        session_epoch,
    } = config;

    if rows == 0 {
        bail!("rows must be greater than zero");
    }

    validate_table_name(&table)?;

    if batch_size == 0 {
        bail!("batch-size must be greater than zero");
    }

    if matches!(protocol, Protocol::V2) && client_id == 0 {
        bail!("client-id must be non-zero for V2");
    }

    if matches!(protocol, Protocol::V2) && session_epoch == 0 {
        bail!("session-epoch must be non-zero for V2");
    }

    let timeout = Duration::from_millis(timeout_ms);
    let mut client = SqlClient::connect(&addr, timeout).await?;
    let mut request_sequence = 1_u64;

    if create_table {
        let response = execute_benchmark_request(
            &mut client,
            protocol,
            client_id,
            session_epoch,
            request_sequence,
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY, value TEXT NOT NULL)"),
        )
        .await?;
        request_sequence = request_sequence
            .checked_add(1)
            .context("benchmark request sequence exhausted while creating table")?;

        if !response_is_success(&response) {
            bail!("creating benchmark table failed: {response}");
        }
    }

    let value = "x".repeat(value_bytes);
    let started = Instant::now();
    let setup_retry_deadline = Instant::now() + Duration::from_secs(30);

    let mut first = 1_u64;
    while first <= rows {
        let last = first.saturating_add(batch_size - 1).min(rows);

        let mut sql = format!("INSERT INTO {table} (id, value) VALUES ");

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

        // Metadata publication and local tablet activation are separate
        // committed transitions. Setup may therefore briefly return a
        // retryable unavailability response even after CREATE TABLE succeeds.
        // Reusing the same sequence is required: a retry is one logical V2
        // request and must remain deduplicable after the tablet becomes ready.
        let response = loop {
            let response = execute_benchmark_request(
                &mut client,
                protocol,
                client_id,
                session_epoch,
                request_sequence,
                &sql,
            )
            .await?;

            if response_is_success(&response) || !response_is_retryable(&response) {
                break response;
            }

            if Instant::now() >= setup_retry_deadline {
                break response;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        request_sequence = request_sequence
            .checked_add(1)
            .context("benchmark request sequence exhausted while loading table")?;
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
    table: String,
    participant_tables: Vec<String>,
    txn_writes: usize,
    contention: ContentionDistribution,
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
    protocol: Protocol,
    client_id: u128,
    session_epoch: u64,
}

async fn run_benchmark(config: BenchmarkConfig) -> Result<()> {
    let BenchmarkConfig {
        addr,
        table,
        participant_tables,
        txn_writes,
        contention,
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
        protocol,
        client_id,
        session_epoch,
    } = config;

    if clients == 0 {
        bail!("clients must be greater than zero");
    }

    validate_table_name(&table)?;
    for participant_table in &participant_tables {
        validate_table_name(participant_table)?;
    }

    if matches!(protocol, Protocol::V2) && client_id == 0 {
        bail!("client-id must be non-zero for V2");
    }

    if matches!(protocol, Protocol::V2) && session_epoch == 0 {
        bail!("session-epoch must be non-zero for V2");
    }

    if matches!(protocol, Protocol::V2) && client_id.checked_add(u128::from(clients - 1)).is_none()
    {
        bail!("client-id range overflows the V2 client identity field");
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

    let transaction_workload = matches!(
        workload,
        Workload::SingleShardTxn | Workload::CrossShardTxn | Workload::TxnContention
    );
    if transaction_workload && txn_writes == 0 {
        bail!("txn-writes must be greater than zero for transaction workloads");
    }
    if matches!(workload, Workload::CrossShardTxn) && participant_tables.len() < 2 {
        bail!("cross-shard-txn requires at least two --participant-tables");
    }
    if read_percent > 100 {
        bail!("read-percent must be between 0 and 100");
    }

    if scan_rows == 0 {
        bail!("scan-rows must be greater than zero");
    }

    if matches!(workload, Workload::RangeScan) && scan_rows > rows {
        bail!("scan-rows must not exceed rows for a range-scan workload");
    }

    if matches!(workload, Workload::RangeScan) && scan_rows > 10_000 {
        bail!(
            "network range-scan is capped at 10,000 rows while the protocol has a 16 MiB frame limit"
        );
    }

    let participant_table_names = if participant_tables.is_empty() {
        vec![table.clone()]
    } else {
        participant_tables
    };
    let report_participant_tables = participant_table_names.clone();
    let participant_table_arcs: Vec<Arc<str>> =
        participant_table_names.into_iter().map(Arc::from).collect();

    let options = RunOptions {
        addr: Arc::from(addr.as_str()),
        table: Arc::from(table.as_str()),
        participant_tables: Arc::new(participant_table_arcs),
        workload,
        seconds,
        rows,
        value_bytes,
        read_percent,
        warmup,
        scan_rows,
        txn_writes,
        contention,
        timeout: Duration::from_millis(timeout_ms),
        seed,
        protocol,
        client_id,
        session_epoch,
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
        aggregate.request_bytes = aggregate.request_bytes.saturating_add(worker.request_bytes);
        aggregate.response_bytes = aggregate
            .response_bytes
            .saturating_add(worker.response_bytes);

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
        benchmark: "RagnorDB SQL",
        load_model: "closed-loop",
        addr,
        table,
        participant_tables: report_participant_tables,
        txn_writes,
        contention,
        protocol,
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
        measured_request_bytes: aggregate.request_bytes,
        measured_response_bytes: aggregate.response_bytes,
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
            table,
            rows,
            value_bytes,
            batch_size,
            timeout_ms,
            create_table,
            protocol,
            client_id,
            session_epoch,
        } => {
            load_table(LoadConfig {
                addr,
                table,
                rows,
                value_bytes,
                batch_size,
                timeout_ms,
                create_table,
                protocol,
                client_id,
                session_epoch,
            })
            .await
        }

        Command::Run {
            addr,
            table,
            participant_tables,
            txn_writes,
            contention,
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
            protocol,
            client_id,
            session_epoch,
        } => {
            run_benchmark(BenchmarkConfig {
                addr,
                table,
                participant_tables,
                txn_writes,
                contention,
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
                protocol,
                client_id,
                session_epoch,
            })
            .await
        }
    }
}
