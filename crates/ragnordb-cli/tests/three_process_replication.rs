use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use ragnordb_common::protocol::{ClientRequestV2, encode_client_request_v2};
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
    read_sql_response(&mut stream)
}

fn sql_v2(
    address: SocketAddr,
    client_id: u128,
    request_sequence: u64,
    acknowledged_through: Option<u64>,
    statement: &str,
) -> Option<Value> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(250)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(35)))
        .ok()?;
    let request = ClientRequestV2 {
        protocol_version: 2,
        client_id,
        client_session_epoch: 1,
        request_sequence,
        acknowledged_through,
        statement_timeout_ms: 5_000,
        sql: statement.to_string(),
    };
    stream
        .write_all(&encode_client_request_v2(&request).ok()?)
        .ok()?;
    stream.flush().ok()?;
    read_sql_response(&mut stream)
}

fn read_sql_response(stream: &mut TcpStream) -> Option<Value> {
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

fn send_v2_until_ok(
    address: SocketAddr,
    client_id: u128,
    request_sequence: u64,
    acknowledged_through: Option<u64>,
    statement: &str,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_response = None;
    loop {
        if let Some(response) = sql_v2(
            address,
            client_id,
            request_sequence,
            acknowledged_through,
            statement,
        ) {
            if response["ok"] == true {
                return response;
            }
            last_response = Some(response);
        }

        if Instant::now() >= deadline {
            panic!(
                "V2 SQL request did not succeed: statement={statement:?}, response={last_response:?}"
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_v2_rows(
    address: SocketAddr,
    client_id: u128,
    request_sequence: u64,
    acknowledged_through: Option<u64>,
    statement: &str,
    expected_rows: Value,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = send_v2_until_ok(
            address,
            client_id,
            request_sequence,
            acknowledged_through,
            statement,
        );
        if response["rows"] == expected_rows {
            return response;
        }
        if Instant::now() >= deadline {
            panic!(
                "V2 SQL query did not observe expected rows: statement={statement:?}, response={response}"
            );
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

fn wait_for_routed_tablet_group(nodes: &[ProcessNode]) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let group_id = nodes
            .iter()
            .filter_map(ProcessNode::status)
            .find_map(|status| {
                status["multiraft"]["groups"]
                    .as_array()?
                    .iter()
                    .filter_map(|group| group["raft_group_id"].as_u64())
                    // Group 1 is the legacy tablet and group 2 is metadata. A
                    // CREATE TABLE-created routed tablet receives a later group.
                    .find(|group_id| *group_id > 2)
            });
        if let Some(group_id) = group_id {
            return group_id;
        }

        if Instant::now() >= deadline {
            let statuses = nodes.iter().map(ProcessNode::status).collect::<Vec<_>>();
            panic!("routed tablet group did not become visible; statuses={statuses:?}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn tablet_group_progress(
    status: &Value,
    group_id: u64,
) -> Option<(u64, Option<u64>, u64, u64, bool, bool)> {
    let group = status["multiraft"]["groups"]
        .as_array()?
        .iter()
        .find(|group| group["raft_group_id"].as_u64() == Some(group_id))?;
    let role = group["role"].as_str();
    Some((
        group["replica_id"].as_u64()?,
        group["leader_replica_id"].as_u64(),
        group["commit_index"].as_u64()?,
        group["applied_index"].as_u64()?,
        role == Some("leader"),
        role == Some("follower"),
    ))
}

fn wait_for_routed_tablet_leader(
    nodes: &[ProcessNode],
    group_id: u64,
    excluded: Option<usize>,
) -> usize {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if excluded == Some(index) {
                continue;
            }
            let is_leader = node
                .status()
                .and_then(|status| tablet_group_progress(&status, group_id))
                .is_some_and(|(replica_id, leader_replica_id, _, _, is_leader, _)| {
                    is_leader && leader_replica_id == Some(replica_id)
                });
            if is_leader {
                return index;
            }
        }

        if Instant::now() >= deadline {
            let statuses = nodes.iter().map(ProcessNode::status).collect::<Vec<_>>();
            panic!(
                "routed tablet group {group_id} did not elect a replacement leader; \
                 statuses={statuses:?}"
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_routed_tablet_catch_up(
    nodes: &[ProcessNode],
    group_id: u64,
    restarted_index: usize,
    leader_index: usize,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let leader_progress = nodes[leader_index]
            .status()
            .and_then(|status| tablet_group_progress(&status, group_id));
        let restarted_progress = nodes[restarted_index]
            .status()
            .and_then(|status| tablet_group_progress(&status, group_id));

        if let (
            Some((
                leader_replica_id,
                Some(observed_leader),
                leader_commit,
                leader_applied,
                true,
                _,
            )),
            Some((_, Some(restarted_leader), restarted_commit, restarted_applied, _, true)),
        ) = (leader_progress, restarted_progress)
            && observed_leader == leader_replica_id
            && restarted_leader == leader_replica_id
            && restarted_commit >= leader_commit
            && restarted_applied >= leader_applied
        {
            return;
        }

        if Instant::now() >= deadline {
            let statuses = nodes.iter().map(ProcessNode::status).collect::<Vec<_>>();
            panic!(
                "restarted routed tablet replica {restarted_index} did not catch up to \
                 leader {leader_index} for group {group_id}; statuses={statuses:?}"
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
/// table, routes V2 DML through a live process, stops a process, and verifies
/// that both the metadata projection and committed row survive a full process
/// restart. The in-process runtime test covers the separate non-leader gateway
/// route; this process test additionally covers the public V2 socket boundary.
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
                "\n[[seed_nodes]]\nid = {}\nraft_addr = \"{}\"\nsnapshot_addr = \"{}\"\nsql_addr = \"{}\"\nadmin_addr = \"{}\"\nregion = \"region-a\"\nzone = \"zone-{}\"\nrack = \"rack-{}\"\nstorage_class = \"default\"\n",
                seed_index + 1,
                raft,
                snapshot,
                sql,
                admin,
                seed_index + 1,
                seed_index + 1,
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

    let metadata_leader = wait_for_leader(&nodes, None);
    // A fresh V2 session must be able to register and issue metadata-backed
    // SQL through a gateway that is not the currently observed leader.
    let follower_ingress = (metadata_leader + 1) % nodes.len();
    let client_id = 0xfeed_cafe_u128;

    let insert = send_v2_until_ok(
        nodes[follower_ingress].sql_addr,
        client_id,
        1,
        None,
        "INSERT INTO users (id, name) VALUES (7, 'alice')",
    );
    assert_eq!(insert["result"]["affected_rows"], 1);

    let update = send_v2_until_ok(
        nodes[follower_ingress].sql_addr,
        client_id,
        2,
        Some(1),
        "UPDATE users SET name = 'bob' WHERE id = 7",
    );
    assert_eq!(update["result"]["affected_rows"], 1);

    let before_restart = send_v2_until_ok(
        nodes[follower_ingress].sql_addr,
        client_id,
        3,
        Some(2),
        "SELECT id, name FROM users WHERE id = 7",
    );
    assert_eq!(
        before_restart["rows"],
        serde_json::json!([[7, "bob"]]),
        "routed V2 read before restart: {before_restart}"
    );

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

    let after_restart = wait_for_v2_rows(
        nodes[recovered_leader].sql_addr,
        client_id,
        4,
        Some(3),
        "SELECT id, name FROM users WHERE id = 7",
        serde_json::json!([[7, "bob"]]),
    );
    assert_eq!(
        after_restart["rows"],
        serde_json::json!([[7, "bob"]]),
        "routed V2 read after restart: {after_restart}"
    );
}

/// Realistic bug caught:
///
/// A gateway can retain a dead routed-tablet leader in its cache after a
/// process failure. This acceptance test warms that cache, stops the cached
/// leader, and resubmits the same V2 write identity through the same gateway.
/// It fails if the gateway does not fail over, if a retry is treated as a new
/// logical mutation, or if the restarted replica does not catch up.
#[test]
fn routed_tablet_leader_failover_retries_stable_v2_write_and_catches_up() {
    let root = tempfile::tempdir().unwrap();
    // Hold every reservation until the complete unique address set is known.
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
        let config_path = root.path().join(format!("failover-node-{node_id}.toml"));
        let data_dir = root.path().join(format!("failover-node-{node_id}"));
        let mut config = format!(
            "node_id = {node_id}\ndata_dir = \"{}\"\nlisten_addr = \"{}\"\nadmin_addr = \"{}\"\ncluster_id = \"process-failover-test\"\nbootstrap = true\nstatement_timeout_ms = 5000\nshutdown_grace_period_ms = 1000\nsnapshot_interval_entries = 4\nsnapshot_interval_bytes = 536870912\nsnapshot_min_elapsed_ms = 0\nmax_snapshot_file_bytes = 536870912\nsnapshot_chunk_bytes = 65536\n",
            data_dir.display(),
            addresses[index].2,
            addresses[index].3,
        );
        for (seed_index, (raft, snapshot, sql, admin)) in addresses.iter().enumerate() {
            config.push_str(&format!(
                "\n[[seed_nodes]]\nid = {}\nraft_addr = \"{}\"\nsnapshot_addr = \"{}\"\nsql_addr = \"{}\"\nadmin_addr = \"{}\"\nregion = \"region-a\"\nzone = \"zone-{}\"\nrack = \"rack-{}\"\nstorage_class = \"default\"\n",
                seed_index + 1,
                raft,
                snapshot,
                sql,
                admin,
                seed_index + 1,
                seed_index + 1,
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

    wait_for_leader(&nodes, None);
    create_metadata_table(
        &nodes,
        "CREATE TABLE failover_users (id INT PRIMARY KEY, name TEXT NOT NULL)",
    );
    let tablet_group_id = wait_for_routed_tablet_group(&nodes);
    let old_tablet_leader = wait_for_routed_tablet_leader(&nodes, tablet_group_id, None);
    let client_id = 0xfeed_fade_u128;

    // Warm both possible surviving gateways. This keeps the acceptance test
    // deterministic when the failed node happened to lead metadata as well as
    // the routed tablet: whichever survivor wins metadata leadership retains
    // the stale routed-tablet leader hint needed for the failover assertion.
    let surviving_gateways = (0..nodes.len())
        .filter(|index| *index != old_tablet_leader)
        .collect::<Vec<_>>();
    for (offset, gateway) in surviving_gateways.iter().enumerate() {
        let warmup = send_v2_until_ok(
            nodes[*gateway].sql_addr,
            client_id,
            offset as u64 + 1,
            (offset > 0).then_some(offset as u64),
            "SELECT id, name FROM failover_users WHERE id = 1",
        );
        assert_eq!(warmup["rows"], serde_json::json!([]));
    }

    nodes[old_tablet_leader].kill();
    let new_tablet_leader =
        wait_for_routed_tablet_leader(&nodes, tablet_group_id, Some(old_tablet_leader));
    assert_ne!(
        new_tablet_leader, old_tablet_leader,
        "tablet failover must elect a surviving process"
    );
    // A node hosts both metadata and routed-tablet groups. If the failed
    // tablet leader also led metadata, wait for that control-plane election
    // before issuing the next public SQL request.
    let gateway = wait_for_routed_tablet_leader(&nodes, 2, Some(old_tablet_leader));

    let failover_write = send_v2_until_ok(
        nodes[gateway].sql_addr,
        client_id,
        3,
        Some(2),
        "INSERT INTO failover_users (id, name) VALUES (2, 'after-failover')",
    );
    assert_eq!(failover_write["result"]["affected_rows"], 1);

    nodes[old_tablet_leader].start();
    wait_for_routed_tablet_catch_up(
        &nodes,
        tablet_group_id,
        old_tablet_leader,
        new_tablet_leader,
    );
    let after_catch_up = wait_for_v2_rows(
        nodes[old_tablet_leader].sql_addr,
        client_id,
        4,
        Some(3),
        "SELECT id, name FROM failover_users WHERE id = 2",
        serde_json::json!([[2, "after-failover"]]),
    );
    assert_eq!(
        after_catch_up["rows"],
        serde_json::json!([[2, "after-failover"]]),
        "failover row after restarted replica catch-up: {after_catch_up}"
    );
}
