use std::net::SocketAddr;

use ragnordb_common::protocol::read_frame;
use ragnordb_server::{database::LocalDatabase, handle_connection};
use serde_json::Value;
use tokio::{
    io::AsyncWriteExt,
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

struct TestSqlServer {
    address: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl TestSqlServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let database = LocalDatabase::shared();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = task_shutdown.cancelled() => {
                        break;
                    }

                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else {
                            break;
                        };

                        let database = database.clone();

                        tokio::spawn(async move {
                            handle_connection(stream, database)
                                .await
                                .unwrap();
                        });
                    }
                }
            }
        });

        Self {
            address,
            shutdown,
            task,
        }
    }

    async fn connect(&self) -> SqlClient {
        SqlClient::connect(self.address).await
    }

    async fn shutdown(self) {
        self.shutdown.cancel();
        self.task.await.unwrap();
    }
}

struct SqlClient {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
}

impl SqlClient {
    async fn connect(address: SocketAddr) -> Self {
        let stream = TcpStream::connect(address).await.unwrap();
        let (reader, writer) = stream.into_split();

        Self { reader, writer }
    }

    async fn execute(&mut self, sql: &str) -> Value {
        let bytes = sql.as_bytes();
        let length = u32::try_from(bytes.len()).unwrap();

        self.writer.write_all(&length.to_le_bytes()).await.unwrap();

        self.writer.write_all(bytes).await.unwrap();
        self.writer.flush().await.unwrap();

        let response = read_frame(&mut self.reader).await.unwrap();

        serde_json::from_str(&response).unwrap()
    }
}

#[tokio::test]
async fn framed_sql_executes_and_converts_every_value_type() {
    let server = TestSqlServer::start().await;
    let mut client = server.connect().await;

    let create = client
        .execute(
            "CREATE TABLE typed_values (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                active BOOL NOT NULL,
                note TEXT
            )",
        )
        .await;

    assert_eq!(create["ok"], true);
    assert_eq!(create["result"]["type"], "created_table");
    assert_eq!(create["result"]["table_id"], 1);

    let insert = client
        .execute(
            "INSERT INTO typed_values (id, name, active, note)
             VALUES (1, 'Ada', true, NULL)",
        )
        .await;

    assert_eq!(insert["ok"], true);
    assert_eq!(insert["result"]["type"], "mutation");
    assert_eq!(insert["result"]["operation"], "insert");
    assert_eq!(insert["result"]["affected_rows"], 1);

    let select = client
        .execute(
            "SELECT id, name, active, note
             FROM typed_values
             WHERE id = 1",
        )
        .await;

    assert_eq!(select["ok"], true);
    assert_eq!(
        select["columns"],
        serde_json::json!(["id", "name", "active", "note"])
    );
    assert_eq!(select["rows"], serde_json::json!([[1, "Ada", true, null]]));
    assert_eq!(select["stats"]["rows_read"], 1);

    let malformed = client.execute("SELECT '").await;

    assert_eq!(malformed["ok"], false);
    assert_eq!(malformed["error"]["code"], "SQL_PARSE_ERROR");

    let commit_without_begin = client.execute("COMMIT").await;

    assert_eq!(commit_without_begin["ok"], false);
    assert_eq!(commit_without_begin["error"]["code"], "INVALID_ARGUMENT");

    drop(client);
    server.shutdown().await;
}

#[tokio::test]
async fn connections_share_database_state_and_preserve_transaction_semantics() {
    let server = TestSqlServer::start().await;
    let mut first = server.connect().await;
    let mut second = server.connect().await;

    first
        .execute(
            "CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .await;

    let begin = first.execute("BEGIN").await;

    assert_eq!(begin["result"]["type"], "transaction_started");

    first
        .execute(
            "INSERT INTO users (id, name)
             VALUES (1, 'Rolled back')",
        )
        .await;

    let read_your_writes = first.execute("SELECT name FROM users WHERE id = 1").await;

    assert_eq!(
        read_your_writes["rows"],
        serde_json::json!([["Rolled back"]])
    );

    first.execute("ROLLBACK").await;

    let after_rollback = second.execute("SELECT id FROM users WHERE id = 1").await;

    assert_eq!(after_rollback["rows"], serde_json::json!([]));

    first.execute("BEGIN").await;

    first
        .execute(
            "INSERT INTO users (id, name)
             VALUES (2, 'Committed')",
        )
        .await;

    let commit = first.execute("COMMIT").await;

    assert_eq!(commit["result"]["type"], "transaction_committed");
    assert_eq!(commit["result"]["committed_writes"], 1);

    let visible_to_second_connection = second.execute("SELECT name FROM users WHERE id = 2").await;

    assert_eq!(
        visible_to_second_connection["rows"],
        serde_json::json!([["Committed"]])
    );

    // BEGIN results from two connections prove that both connections use one
    // shared transaction/timestamp allocator.
    let first_begin = first.execute("BEGIN").await;
    let second_begin = second.execute("BEGIN").await;

    let first_transaction_id = first_begin["result"]["transaction_id"].as_u64().unwrap();

    let second_transaction_id = second_begin["result"]["transaction_id"].as_u64().unwrap();

    let first_start_ts = first_begin["result"]["start_timestamp"].as_u64().unwrap();

    let second_start_ts = second_begin["result"]["start_timestamp"].as_u64().unwrap();

    assert!(second_transaction_id > first_transaction_id);
    assert!(second_start_ts > first_start_ts);

    first.execute("ROLLBACK").await;
    second.execute("ROLLBACK").await;

    // Closing the first connection drops its SqlSession and therefore discards
    // its uncommitted buffered transaction.
    first.execute("BEGIN").await;

    first
        .execute(
            "INSERT INTO users (id, name)
             VALUES (3, 'Disconnected')",
        )
        .await;

    drop(first);

    let disconnected_row = second.execute("SELECT id FROM users WHERE id = 3").await;

    assert_eq!(disconnected_row["rows"], serde_json::json!([]));

    drop(second);
    server.shutdown().await;
}

#[tokio::test]
async fn write_conflict_returns_retryable_error_and_preserves_winner() {
    let server = TestSqlServer::start().await;
    let mut losing_client = server.connect().await;
    let mut winning_client = server.connect().await;

    losing_client
        .execute(
            "CREATE TABLE accounts (
                id INT PRIMARY KEY,
                owner TEXT NOT NULL
            )",
        )
        .await;

    losing_client.execute("BEGIN").await;

    losing_client
        .execute(
            "INSERT INTO accounts (id, owner)
             VALUES (1, 'Losing writer')",
        )
        .await;

    let winner = winning_client
        .execute(
            "INSERT INTO accounts (id, owner)
             VALUES (1, 'Winning writer')",
        )
        .await;

    assert_eq!(winner["ok"], true);

    let conflict = losing_client.execute("COMMIT").await;

    assert_eq!(conflict["ok"], false);
    assert_eq!(conflict["error"]["code"], "WRITE_CONFLICT");
    assert_eq!(conflict["error"]["retryable"], true);

    // COMMIT clears the losing SQL session even when storage reports a conflict.
    let second_commit = losing_client.execute("COMMIT").await;

    assert_eq!(second_commit["ok"], false);
    assert_eq!(second_commit["error"]["code"], "INVALID_ARGUMENT");

    let visible_winner = winning_client
        .execute("SELECT owner FROM accounts WHERE id = 1")
        .await;

    assert_eq!(
        visible_winner["rows"],
        serde_json::json!([["Winning writer"]])
    );

    drop(losing_client);
    drop(winning_client);
    server.shutdown().await;
}
