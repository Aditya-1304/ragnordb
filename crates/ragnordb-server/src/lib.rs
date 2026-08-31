pub mod admin;
mod bootstrap;
pub mod build_info;
pub mod config;
pub mod data_directory_lock;
pub mod database;
pub mod metrics;
pub mod multiraft_runtime;
pub mod protocol;
pub mod replicated_tablet;
pub mod session;
mod snapshot_transport;

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use admin::AdminState;
use build_info::BUILD_INFO;
use config::{NodeConfig, StatementLogging};
use data_directory_lock::DataDirectoryLock;
use database::{LocalDatabase, SharedLocalDatabase};
use multiraft_runtime::MultiRaftRuntime;
use protocol::{error_response, execution_response, execution_stats, internal_error_response};
use ragnordb_common::protocol::{read_frame, write_frame};
use replicated_tablet::ReplicatedTabletHandle;
use session::Session;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Debug)]
pub struct Server {
    config: NodeConfig,
}

impl Server {
    pub fn new(config: NodeConfig) -> Self {
        Self { config }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let max_connections = self.config.max_connections;
        let data_dir = self.config.data_dir.clone();
        let listen_addr = self.config.listen_addr;
        let admin_addr = self.config.admin_addr;
        let statement_timeout_ms = self.config.statement_timeout_ms;
        let shutdown_grace = Duration::from_millis(self.config.shutdown_grace_period_ms);
        let statement_logging = self.config.statement_logging;

        metrics::init_metrics();

        info!(
            node_id = self.config.node_id.0,
            data_dir = %data_dir.display(),
            listen = %listen_addr,
            admin = %admin_addr,
            max_connections,
            ragnordb_version = BUILD_INFO.ragnordb_version,
            raft_version = BUILD_INFO.raft_version,
            raft_revision = BUILD_INFO.raft_revision,
            wal_version = BUILD_INFO.wal_version,
            wal_revision = BUILD_INFO.wal_revision,
            bloom_version = BUILD_INFO.bloom_version,
            bloom_revision = BUILD_INFO.bloom_revision,
            feature_flags = BUILD_INFO.feature_flags,
            "node starting",
        );

        info!(
            cluster_id = self.config.cluster_id.as_deref().unwrap_or("single-node"),
            bootstrap = self.config.bootstrap,
            seed_nodes = self.config.seed_nodes.len(),
            "cluster configuration loaded",
        );

        tokio::fs::create_dir_all(&data_dir).await?;
        info!("data directory ready");

        let started_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);

        let connection_semaphore = Arc::new(Semaphore::new(max_connections as usize));

        // recover the complete runtime before binding client facing listeners
        // no session can allocate identifiers or observe state while physical
        // WAL recovery, semantic replay, or allocator restoration is incomplete
        let replicated = self.config.cluster_id.is_some() && !self.config.seed_nodes.is_empty();
        let (database, recovery_report, recovered_raft) = if replicated {
            // Acquire process ownership before touching any bootstrap or WAL
            // state. The same guard is transferred into LocalDatabase and held
            // for the entire live database lifetime.
            let data_directory_lock = DataDirectoryLock::acquire(&data_dir)?;

            let configurations =
                MultiRaftRuntime::recovery_configurations(&self.config, &data_directory_lock)?;

            let (database, report, recovered) = LocalDatabase::recover_shared_with_raft_with_lock(
                &data_dir,
                self.config.node_id,
                &configurations,
                data_directory_lock,
            )?;

            (database, report, Some(recovered))
        } else {
            let (database, report) = LocalDatabase::recover(&data_dir, self.config.node_id)?;
            (database, report, None)
        };
        let replicated_wal = replicated.then(|| database.wal_handle()).transpose()?;

        metrics::histogram_record(
            "ragnordb_recovery_duration_seconds",
            recovery_report.recovery_duration.as_secs_f64(),
        );
        metrics::counter_add(
            "ragnordb_recovery_records_replayed_total",
            recovery_report.records_scanned,
        );
        metrics::gauge_set(
            "ragnordb_wal_durable_lsn",
            recovery_report.next_lsn.as_u64() as f64,
        );

        let database = database.into_shared();
        let replicated_runtime = match (replicated_wal, recovered_raft) {
            (Some(wal), Some(recovered)) => {
                let runtime = MultiRaftRuntime::start_from_shared_recovery(
                    &self.config,
                    wal,
                    database.clone(),
                    recovered,
                )?;
                database.lock().await.replace_commit_log(runtime.handle());
                database.lock().await.replace_catalog_log(runtime.handle());
                Some(runtime)
            }
            (None, None) => None,
            _ => unreachable!("replicated WAL and shared Raft recovery are created together"),
        };
        let replicated_handle = replicated_runtime.as_ref().map(MultiRaftRuntime::handle);
        let admin_state = Arc::new(AdminState {
            started_at,
            connection_semaphore: connection_semaphore.clone(),
            max_connections,
            durability_gate: database.lock().await.durability_gate(),
            database: database.clone(),
            replicated_tablet: replicated_handle.clone(),
        });

        info!(
            segments_scanned = recovery_report.segments_scanned,
            records_scanned = recovery_report.records_scanned,
            corrupt_records_found = recovery_report.corrupt_records_found,
            truncated_bytes = recovery_report.truncated_bytes,
            next_lsn = recovery_report.next_lsn.as_u64(),
            clean_shutdown = recovery_report.clean_shutdown,
            recovery_skipped = recovery_report.recovery_skipped,
            "database recovery completed",
        );

        // Bind every required endpoint before starting background work. The node
        // must not report successful startup when either endpoint is unavailable.
        let admin_listener = TcpListener::bind(admin_addr).await?;
        let sql_listener = TcpListener::bind(listen_addr).await?;

        let admin_shutdown = CancellationToken::new();
        let server_shutdown = CancellationToken::new();
        let admin_task_shutdown = admin_shutdown.clone();
        let mut connection_tasks = JoinSet::new();
        let mut shutdown_signal = Box::pin(wait_for_shutdown_signal());

        let admin_task = tokio::spawn(async move {
            admin::serve_admin(admin_listener, admin_state, admin_task_shutdown).await
        });

        info!(listen = %listen_addr, "listening (SQL protocol)");

        loop {
            tokio::select! {
                result = sql_listener.accept() => {
                    match result {
                        Ok((stream, address)) => {
                            let semaphore = connection_semaphore.clone();

                            match semaphore.try_acquire_owned() {
                                Ok(permit) => {
                                    metrics::counter_inc(
                                        "RagnorDB_connections_accepted_total"
                                    );

                                    let active_connections =
                                        max_connections as usize
                                            - connection_semaphore.available_permits();

                                    metrics::gauge_set(
                                        "RagnorDB_connections_active",
                                        active_connections as f64,
                                    );

                                    info!(
                                        from = %address,
                                        active_connections,
                                        "accepted connection"
                                    );

                                    let connection_semaphore =
                                        connection_semaphore.clone();

                                    let connection_database = database.clone();
                                    let connection_replicated = replicated_handle.clone();

                                    let connection_shutdown = server_shutdown.clone();

                                    connection_tasks.spawn(async move {
                                        if let Err(connection_error) =
                                            handle_connection_with_policy(
                                                stream,
                                                connection_database,
                                                connection_replicated,
                                                connection_shutdown,
                                                statement_timeout_ms,
                                                statement_logging,
                                            )
                                            .await
                                        {
                                            warn!(
                                                from = %address,
                                                error = %connection_error,
                                                "connection error"
                                            );
                                        }

                                        drop(permit);

                                        let active_connections =
                                            max_connections as usize
                                                - connection_semaphore
                                                    .available_permits();

                                        metrics::gauge_set(
                                            "RagnorDB_connections_active",
                                            active_connections as f64,
                                        );

                                        info!(
                                            from = %address,
                                            "connection closed"
                                        );
                                    });
                                }

                                Err(_) => {
                                    warn!(
                                        from = %address,
                                        max = max_connections,
                                        "connection limit reached; rejecting client"
                                    );

                                    let response = error_response(
                                        "CONNECTION_LIMIT",
                                        &format!(
                                            "server has reached its configured \
                                             max connection count \
                                             ({max_connections})"
                                        ),
                                        true,
                                    );

                                    let mut stream = stream;
                                    let _ = write_frame(&mut stream, &response).await;
                                }
                            }
                        }

                        Err(accept_error) => {
                            error!(
                                error = %accept_error,
                                "SQL connection accept error"
                            );

                            tokio::time::sleep(
                                std::time::Duration::from_millis(100),
                            )
                            .await;
                        }
                    }
                }

                signal = &mut shutdown_signal => {
                    info!(signal, "received shutdown signal; draining server");
                    server_shutdown.cancel();
                    admin_shutdown.cancel();
                    break;
                }
            }
        }

        let drain_connections = async {
            while let Some(join_result) = connection_tasks.join_next().await {
                if let Err(join_error) = join_result {
                    warn!(error = %join_error, "connection task failed while draining");
                }
            }
        };

        if tokio::time::timeout(shutdown_grace, drain_connections)
            .await
            .is_err()
        {
            warn!(
                grace_period_ms = shutdown_grace.as_millis(),
                "connection drain deadline expired; aborting remaining tasks"
            );
            connection_tasks.abort_all();
            while connection_tasks.join_next().await.is_some() {}
        }

        match admin_task.await {
            Ok(Ok(())) => {}

            Ok(Err(admin_error)) => {
                return Err(admin_error);
            }

            Err(join_error) => {
                return Err(Box::new(join_error));
            }
        }

        // Client work has drained, so the Ready owner can stop before A-WAL's
        // clean-shutdown witness is published. No background tick may append a
        // later Raft record beyond that witness.
        drop(replicated_handle);
        drop(replicated_runtime);

        let shutdown_database = database.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let mut database = shutdown_database;
            database.shutdown()
        })
        .await??;

        info!("goodbye");
        Ok(())
    }
}

/// Handle one framed SQL client connection.
///
/// Each connection owns one server session and processes at most one statement
/// at a time. All connections share the same local database runtime.
///
/// The database mutex is released before the response is written so a slow
/// client cannot block SQL execution for every other connection.
pub async fn handle_connection(
    stream: tokio::net::TcpStream,
    database: SharedLocalDatabase,
) -> Result<(), Box<dyn std::error::Error>> {
    handle_connection_with_policy(
        stream,
        database,
        None,
        CancellationToken::new(),
        30_000,
        StatementLogging::MetadataOnly,
    )
    .await
}

async fn handle_connection_with_policy(
    stream: tokio::net::TcpStream,
    database: SharedLocalDatabase,
    replicated_tablet: Option<Arc<ReplicatedTabletHandle>>,
    shutdown: CancellationToken,
    statement_timeout_ms: u64,
    statement_logging: StatementLogging,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = stream.into_split();
    let mut session = Session::new();
    session.statement_timeout_ms = statement_timeout_ms;

    loop {
        let sql = tokio::select! {
            _ = shutdown.cancelled() => break,
            frame = read_frame(&mut reader) => match frame {
                Ok(sql) => sql,
                Err(_) => break,
            },
        };

        let trimmed = sql.trim().to_string();

        metrics::counter_inc("RagnorDB_requests_received_total");

        log_statement(statement_logging, session.session_id.0, &trimmed);

        // Latest reads are served only after an exact no-op has committed and
        // applied on the current leader. This check happens before database
        // admission so the Ready owner never waits on the SQL state mutex.
        let read_barrier_error = if is_latest_read(&trimmed) {
            if let Some(replicated) = replicated_tablet.clone() {
                let timeout = Duration::from_millis(session.statement_timeout_ms);
                tokio::task::spawn_blocking(move || replicated.read_barrier(timeout))
                    .await?
                    .err()
            } else {
                None
            }
        } else {
            None
        };

        // The deadline covers admission to the serialized owner. Once admitted,
        // the operation runs to its authoritative durability outcome: timing out
        // an already staged commit would incorrectly turn uncertainty into an
        // ordinary cancellation. Synchronous SQL and fsync execute on the
        // blocking pool so Tokio workers remain available to network tasks.
        let execution = if let Some(error) = read_barrier_error {
            Err(error)
        } else {
            match tokio::time::timeout(
                Duration::from_millis(session.statement_timeout_ms),
                database.clone().lock_owned(),
            )
            .await
            {
                Ok(database_guard) if !shutdown.is_cancelled() => {
                    let mut sql_session = std::mem::take(&mut session.sql);
                    let started = Instant::now();
                    let statement = trimmed.clone();
                    let (returned_session, result, status) =
                        tokio::task::spawn_blocking(move || {
                            let mut database = database_guard;
                            let result = database.execute_sql(&mut sql_session, &statement);
                            let status = database.status();
                            (sql_session, result, status)
                        })
                        .await?;
                    session.sql = returned_session;
                    metrics::histogram_record(
                        "ragnordb_statement_execution_seconds",
                        started.elapsed().as_secs_f64(),
                    );
                    metrics::gauge_set("ragnordb_wal_durable_lsn", status.durable_lsn as f64);
                    metrics::gauge_set(
                        "ragnordb_wal_retained_bytes",
                        status.wal_retained_bytes as f64,
                    );
                    metrics::histogram_record(
                        "ragnordb_wal_append_latency_seconds",
                        status.wal_last_append_nanos as f64 / 1_000_000_000.0,
                    );
                    metrics::histogram_record(
                        "ragnordb_wal_sync_latency_seconds",
                        status.wal_last_sync_nanos as f64 / 1_000_000_000.0,
                    );
                    metrics::gauge_set(
                        "ragnordb_wal_oldest_retention_pin",
                        status.oldest_retention_pin_lsn.unwrap_or(0) as f64,
                    );
                    result
                }
                Ok(_) => break,
                Err(_) => Err(ragnordb_common::Error::StatementTimeout {
                    timeout_ms: session.statement_timeout_ms,
                }),
            }
        };

        let response = match execution {
            Ok(result) => {
                match &result {
                    ragnordb_exec::ExecutionResult::TransactionCommitted { .. } => {
                        metrics::counter_inc("ragnordb_txn_commits_total");
                    }
                    ragnordb_exec::ExecutionResult::TransactionRolledBack { .. } => {
                        metrics::counter_inc("ragnordb_txn_aborts_total");
                    }
                    _ => {}
                }
                let stats = execution_stats(&result);

                metrics::counter_inc("RagnorDB_requests_success_total");

                if stats.rows_read > 0 {
                    metrics::counter_add("RagnorDB_response_rows_read_total", stats.rows_read);
                }

                if stats.rows_written > 0 {
                    metrics::counter_add(
                        "RagnorDB_response_rows_written_total",
                        stats.rows_written,
                    );
                }

                execution_response(&result)
            }

            Err(execution_error) => {
                if matches!(
                    execution_error,
                    ragnordb_common::Error::CommitOutcomeUnknown { .. }
                ) {
                    metrics::counter_inc("ragnordb_txn_commit_unknown_total");
                }
                metrics::counter_inc("RagnorDB_requests_error_total");

                warn!(
                    session_id = session.session_id.0,
                    error = %execution_error,
                    "SQL execution failed"
                );

                internal_error_response(&execution_error)
            }
        };

        // No database guard is held while awaiting network I/O.
        write_frame(&mut writer, &response).await?;
    }

    if let Some(transaction_id) = session.current_transaction_id() {
        info!(
            session_id = session.session_id.0,
            transaction_id = transaction_id.0,
            "connection closed with an active transaction; discarding buffered writes"
        );
    }

    Ok(())
}

fn log_statement(policy: StatementLogging, session_id: u64, statement: &str) {
    let operation = statement.split_whitespace().next().unwrap_or("empty");

    match policy {
        StatementLogging::Off => {}
        StatementLogging::MetadataOnly => info!(
            session_id,
            operation,
            statement_bytes = statement.len(),
            "received SQL"
        ),
        StatementLogging::Redacted => info!(
            session_id,
            operation,
            statement_bytes = statement.len(),
            statement = "<redacted>",
            "received SQL"
        ),
        StatementLogging::Full => {
            info!(session_id, statement = %statement, "received SQL");
        }
    }
}

fn is_latest_read(statement: &str) -> bool {
    statement
        .split_whitespace()
        .next()
        .is_some_and(|operation| operation.eq_ignore_ascii_case("SELECT"))
}

async fn wait_for_shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("installing the SIGTERM handler must succeed");

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                let _ = result;
                "SIGINT"
            }
            _ = terminate.recv() => "SIGTERM",
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "interrupt"
    }
}

#[cfg(test)]
mod operational_tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    /// Realistic bug caught:
    ///
    /// A configured statement timeout could remain passive session metadata,
    /// allowing a request queued behind another database owner to wait forever.
    /// The response must also prove execution never started by remaining safely
    /// retryable instead of reporting an uncertain commit.
    #[tokio::test]
    async fn statement_deadline_rejects_before_database_admission() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let database = LocalDatabase::shared();
        let held_database_owner = database.lock().await;
        let handler_database = database.clone();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection_with_policy(
                stream,
                handler_database,
                None,
                CancellationToken::new(),
                20,
                StatementLogging::Off,
            )
            .await
            .unwrap();
        });

        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let sql = b"SELECT 1";
        client
            .write_all(&(sql.len() as u32).to_le_bytes())
            .await
            .unwrap();
        client.write_all(sql).await.unwrap();
        client.flush().await.unwrap();

        let response = read_frame(&mut client).await.unwrap();
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();

        assert_eq!(response["error"]["code"], "STATEMENT_TIMEOUT");
        assert_eq!(response["error"]["retryable"], true);

        drop(client);
        drop(held_database_owner);
        server.await.unwrap();
    }
}
