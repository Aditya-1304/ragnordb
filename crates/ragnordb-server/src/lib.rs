pub mod admin;
pub mod build_info;
pub mod config;
pub mod database;
pub mod metrics;
pub mod protocol;
pub mod session;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use admin::AdminState;
use build_info::BUILD_INFO;
use config::NodeConfig;
use database::{LocalDatabase, SharedLocalDatabase};
use protocol::{error_response, execution_response, execution_stats, internal_error_response};
use ragnordb_common::protocol::{read_frame, write_frame};
use session::Session;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
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

        metrics::init_metrics();

        info!(
            node_id = self.config.node_id.0,
            data_dir = %data_dir.display(),
            listen = %listen_addr,
            admin = %admin_addr,
            max_connections,
            ragnordb_version = BUILD_INFO.ragnordb_version,
            raft_version = BUILD_INFO.raft_version,
            wal_version = BUILD_INFO.wal_version,
            bloom_version = BUILD_INFO.bloom_version,
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

        let admin_state = Arc::new(AdminState {
            started_at,
            connection_semaphore: connection_semaphore.clone(),
            max_connections,
        });

        // recover the complete runtime before binding client facing listeners
        // no session can allocate identifiers or observe state while physical
        // WAL recovery, semantic replay, or allocator restoration is incomplete
        let (database, recovery_report) = LocalDatabase::recover(&data_dir, self.config.node_id)?;

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

        let database = database.into_shared();

        // Bind every required endpoint before starting background work. The node
        // must not report successful startup when either endpoint is unavailable.
        let admin_listener = TcpListener::bind(admin_addr).await?;
        let sql_listener = TcpListener::bind(listen_addr).await?;

        let admin_shutdown = CancellationToken::new();
        let admin_task_shutdown = admin_shutdown.clone();

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

                                    tokio::spawn(async move {
                                        if let Err(connection_error) =
                                            handle_connection(
                                                stream,
                                                connection_database,
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

                _ = tokio::signal::ctrl_c() => {
                    info!("received SIGINT; shutting down");
                    admin_shutdown.cancel();
                    break;
                }
            }
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
    let (mut reader, mut writer) = stream.into_split();
    let mut session = Session::new();

    loop {
        let sql = match read_frame(&mut reader).await {
            Ok(sql) => sql,
            Err(_) => break,
        };

        let trimmed = sql.trim().to_string();

        metrics::counter_inc("RagnorDB_requests_received_total");

        info!(
            session_id = session.session_id.0,
            statement = %trimmed,
            "received SQL"
        );

        // The guard exists only while the statement accesses shared database
        // state. The owned result or error survives after the guard is dropped.
        let execution = {
            let mut database = database.lock().await;
            database.execute_sql(&mut session.sql, &trimmed)
        };

        let response = match execution {
            Ok(result) => {
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
