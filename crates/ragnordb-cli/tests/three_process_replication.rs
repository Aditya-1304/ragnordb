use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

struct ProcessNode {
    config_path: PathBuf,
    sql_addr: SocketAddr,
    admin_addr: SocketAddr,
    child: Option<Child>,
}

impl ProcessNode {
    fn start(&mut self) {
        self.child = Some(
            Command::new(env!("CARGO_BIN_EXE_ragnordb"))
                .args(["node", "--config"])
                .arg(&self.config_path)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn status(&self) -> Option<Value> {
        let mut stream =
            TcpStream::connect_timeout(&self.admin_addr, Duration::from_millis(150)).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(1))).ok()?;
        stream
            .write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .ok()?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).ok()?;
        let boundary = response.windows(4).position(|bytes| bytes == b"\r\n\r\n")? + 4;
        serde_json::from_slice(&response[boundary..]).ok()
    }
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        self.kill();
    }
}

fn sql(address: SocketAddr, statement: &str) -> Option<Value> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(250)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(35)))
        .ok()?;
    let bytes = statement.as_bytes();
    stream.write_all(&(bytes.len() as u32).to_le_bytes()).ok()?;
    stream.write_all(bytes).ok()?;
    stream.flush().ok()?;
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).ok()?;
    let mut response = vec![0_u8; u32::from_le_bytes(length) as usize];
    stream.read_exact(&mut response).ok()?;
    serde_json::from_slice(&response).ok()
}

fn wait_for_leader(nodes: &[ProcessNode], excluded: Option<usize>) -> usize {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if excluded == Some(index) {
                continue;
            }
            if node
                .status()
                .and_then(|status| status["replication"]["is_leader"].as_bool())
                == Some(true)
            {
                return index;
            }
        }
        if Instant::now() >= deadline {
            let statuses = nodes.iter().map(ProcessNode::status).collect::<Vec<_>>();
            panic!("three processes did not elect a leader; statuses={statuses:?}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_metadata_table(nodes: &[ProcessNode], table_name: &str, excluded: Option<usize>) {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if excluded == Some(index) {
                continue;
            }

            if metadata_table_visible_at(node, table_name) {
                return;
            }
        }

        if Instant::now() >= deadline {
            let statuses = nodes.iter().map(ProcessNode::status).collect::<Vec<_>>();
            panic!("metadata table {table_name} did not become visible; statuses={statuses:?}");
        }

        thread::sleep(Duration::from_millis(50));
    }
}

fn metadata_table_visible_at(node: &ProcessNode, table_name: &str) -> bool {
    sql(node.sql_addr, "SHOW TABLES").is_some_and(|response| {
        response["ok"] == true
            && response["rows"].as_array().is_some_and(|rows| {
                rows.iter().any(|row| {
                    row.as_array().is_some_and(|columns| {
                        columns.first().and_then(Value::as_str) == Some(table_name)
                    })
                })
            })
    })
}

fn create_metadata_table(nodes: &[ProcessNode], statement: &str) {
    let deadline = Instant::now() + Duration::from_secs(12);
    let mut last_response = None;

    loop {
        for node in nodes {
            if let Some(response) = sql(node.sql_addr, statement) {
                if response["ok"] == true {
                    return;
                }

                // If the response to the first successful metadata proposal
                // was lost, a retry can observe the deterministic name
                // conflict even though the requested table already exists.
                if response["error"]["code"] == "CONSTRAINT_VIOLATION"
                    && metadata_table_visible_at(node, "users")
                {
                    return;
                }

                last_response = Some(response);
            }
        }

        if Instant::now() >= deadline {
            let statuses = nodes.iter().map(ProcessNode::status).collect::<Vec<_>>();
            panic!(
                "no node accepted the metadata CREATE TABLE: {last_response:?}; statuses={statuses:?}"
            );
        }

        thread::sleep(Duration::from_millis(50));
    }
}

/// Realistic bug caught:
///
/// An in-process cluster can hide missing CLI/server wiring, port binding, data
/// directory recovery, and process-lifetime failures. This test crosses the
/// public SQL and admin sockets of three OS processes, creates a metadata-owned
/// table, stops a process, and verifies that the committed metadata projection
/// survives both process restart and full-cluster restart. DML is intentionally
/// not asserted here: distributed SQL routing remains a later Phase 5.6
/// boundary even though Phase 5.5 now materializes the local tablet groups.
#[test]
fn metadata_table_creation_survives_process_restart() {
    let root = tempfile::tempdir().unwrap();
    // Hold every reservation until the complete unique address set is known.
    // Dropping one port-zero listener at a time allows the OS to immediately
    // recycle its port for a different endpoint in the same test.
    let reservations = (0..12)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let reserved = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    let addresses = reserved
        .chunks_exact(4)
        .map(|chunk| (chunk[0], chunk[1], chunk[2], chunk[3]))
        .collect::<Vec<_>>();
    drop(reservations);
    let mut nodes = Vec::new();

    for index in 0..3 {
        let node_id = index + 1;
        let config_path = root.path().join(format!("node-{node_id}.toml"));
        let data_dir = root.path().join(format!("node-{node_id}"));
        let mut config = format!(
            "node_id = {node_id}\ndata_dir = \"{}\"\nlisten_addr = \"{}\"\nadmin_addr = \"{}\"\ncluster_id = \"process-test\"\nbootstrap = true\nstatement_timeout_ms = 5000\nshutdown_grace_period_ms = 1000\nsnapshot_interval_entries = 4\nsnapshot_interval_bytes = 536870912\nsnapshot_min_elapsed_ms = 0\nmax_snapshot_file_bytes = 536870912\nsnapshot_chunk_bytes = 65536\n",
            data_dir.display(),
            addresses[index].2,
            addresses[index].3,
        );
        for (seed_index, (raft, snapshot, sql, admin)) in addresses.iter().enumerate() {
            config.push_str(&format!(
                "\n[[seed_nodes]]\nid = {}\nraft_addr = \"{}\"\nsnapshot_addr = \"{}\"\nsql_addr = \"{}\"\nadmin_addr = \"{}\"\n",
                seed_index + 1,
                raft,
                snapshot,
                sql,
                admin,
            ));
        }
        fs::write(&config_path, config).unwrap();
        let mut node = ProcessNode {
            config_path,
            sql_addr: addresses[index].2,
            admin_addr: addresses[index].3,
            child: None,
        };
        node.start();
        nodes.push(node);
    }

    let first_leader = wait_for_leader(&nodes, None);
    create_metadata_table(
        &nodes,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)",
    );
    // Do not stop the legacy leader until at least one other process has
    // materialized the committed metadata projection. The metadata proposal
    // may be acknowledged before every follower has refreshed its local SQL
    // cache, and observing only the leader would not prove restart safety.
    wait_for_metadata_table(&nodes, "users", Some(first_leader));

    // The metadata command has committed and applied on the initial quorum.
    // Stopping the legacy tablet leader must not remove metadata already
    // replicated to the surviving nodes.
    nodes[first_leader].kill();
    wait_for_metadata_table(&nodes, "users", Some(first_leader));

    nodes[first_leader].start();
    wait_for_metadata_table(&nodes, "users", None);

    // Restart every process. The metadata Raft log/snapshot restores the
    // schema, while the local SQL catalog cache is rebuilt from that authority.
    for node in &mut nodes {
        node.kill();
    }
    for node in &mut nodes {
        node.start();
    }
    let recovered_leader = wait_for_leader(&nodes, None);
    wait_for_metadata_table(&nodes, "users", Some(recovered_leader));
}
