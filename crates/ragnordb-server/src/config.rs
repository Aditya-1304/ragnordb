use ragnordb_common::ids::NodeId;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// unique node identifier for nodes in the cluster
    pub node_id: NodeId,

    /// directory for all local peristent data (this will contain WAL, Raft snapshots, and other stuff)
    pub data_dir: PathBuf,

    /// a simple socket address for SQL client connections
    pub listen_addr: SocketAddr,

    /// socket address for admit HTTP server (this will contian metircs + status)
    /// computed as listen_addr with port + 100
    pub admin_addr: SocketAddr,

    /// maximum number od concurrent SQL connections
    pub max_connections: u32,
}

impl NodeConfig {
    /// build a config from the CLI `node` subcommand arguments.
    ///
    /// `admin_addr` is automatically derived from `listen_addr`:
    /// if listen port is 7101, admin port becomes 7201.
    pub fn new(node_id: NodeId, data_dir: PathBuf, listen_addr: SocketAddr) -> Self {
        let admin_port = listen_addr
            .port()
            .checked_add(100)
            .expect("admin port overflow");

        let admin_addr = SocketAddr::new(listen_addr.ip(), admin_port);

        NodeConfig {
            node_id,
            data_dir,
            listen_addr,
            admin_addr,
            max_connections: 100,
        }
    }

    /// Override the default max connections
    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }
}
