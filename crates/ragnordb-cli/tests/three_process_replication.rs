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

fn wait_for_successful_write(
    nodes: &[ProcessNode],
    excluded: Option<usize>,
    statement: &str,
) -> usize {
    let deadline = Instant::now() + Duration::from_secs(12);
    let mut last_response = None;
    loop {
        let leader = wait_for_leader(nodes, excluded);
        if let Some(response) = sql(nodes[leader].sql_addr, statement) {
            if response["ok"] == true {
                return leader;
            }
            // A lost response may be followed by a deterministic duplicate-key
            // rejection even though the replicated write already applied.
            if response["error"]["code"] == "CONSTRAINT_VIOLATION" {
                return leader;
            }
            last_response = Some(response);
        }
        if Instant::now() >= deadline {
            let statuses = nodes.iter().map(ProcessNode::status).collect::<Vec<_>>();
            panic!(
                "replacement leader did not accept the SQL write: {last_response:?}; statuses={statuses:?}"
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// Realistic bug caught:
///
/// An in-process cluster can hide missing CLI/server wiring, port binding, data
/// directory recovery, and process-lifetime failures. This test crosses the
/// public SQL and admin sockets of three OS processes, kills the leader,
/// commits on its replacement, and restarts the old process after the retained
/// log has been compacted. This catches premature leader serving, missing
/// out-of-band snapshot wiring, and snapshot recovery that loses SQL catalog
/// metadata even when the tablet rows themselves survive.
#[test]
fn sql_survives_process_leader_failure_and_restart_catchup() {
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
            "node_id = {node_id}\ndata_dir = \"{}\"\nlisten_addr = \"{}\"\nadmin_addr = \"{}\"\ncluster_id = \"process-test\"\nbootstrap = true\nstatement_timeout_ms = 5000\nshutdown_grace_period_ms = 1000\n",
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
    assert_eq!(
        sql(
            nodes[first_leader].sql_addr,
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)",
        )
        .unwrap()["ok"],
        true
    );
    assert_eq!(
        sql(
            nodes[first_leader].sql_addr,
            "INSERT INTO users (id, name) VALUES (1, 'before')",
        )
        .unwrap()["ok"],
        true
    );
    let failed_replica_index =
        nodes[first_leader].status().unwrap()["replication"]["applied_index"]
            .as_u64()
            .unwrap();

    // Kill immediately after the acknowledged response. The surviving quorum
    // may have persisted the entry without yet learning the advanced commit
    // index; replacement-term progress must commit and apply that durable
    // prefix without relying on another heartbeat from the failed leader.
    nodes[first_leader].kill();
    let second_leader = wait_for_successful_write(
        &nodes,
        Some(first_leader),
        "INSERT INTO users (id, name) VALUES (2, 'after')",
    );
    let target_index = nodes[second_leader].status().unwrap()["replication"]["applied_index"]
        .as_u64()
        .unwrap();
    let snapshot_index = nodes[second_leader].status().unwrap()["replication"]["snapshot_index"]
        .as_u64()
        .unwrap();
    assert!(
        snapshot_index > failed_replica_index,
        "replacement leader must compact beyond the failed replica before restart"
    );

    nodes[first_leader].start();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let caught_up = nodes[first_leader].status().is_some_and(|status| {
            status["replication"]["applied_index"]
                .as_u64()
                .is_some_and(|index| index >= target_index)
                && status["replication"]["snapshot_index"]
                    .as_u64()
                    .is_some_and(|index| index >= snapshot_index)
        });
        if caught_up {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restarted process did not catch up"
        );
        thread::sleep(Duration::from_millis(50));
    }

    let response = sql(nodes[second_leader].sql_addr, "SELECT id, name FROM users").unwrap();
    assert_eq!(response["ok"], true);
    assert_eq!(response["rows"].as_array().unwrap().len(), 2);

    // Restart every process after compaction. The tablet snapshot restores the
    // replicated rows, while the derived local catalog cache restores the SQL
    // schema whose original Raft entry is now below the snapshot boundary.
    for node in &mut nodes {
        node.kill();
    }
    for node in &mut nodes {
        node.start();
    }
    let recovered_leader = wait_for_leader(&nodes, None);
    let response = sql(
        nodes[recovered_leader].sql_addr,
        "SELECT id, name FROM users",
    )
    .unwrap();
    assert_eq!(response["ok"], true);
    assert_eq!(response["rows"].as_array().unwrap().len(), 2);
}
