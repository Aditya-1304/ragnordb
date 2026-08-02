//! Node and static cluster configuration.
//!
//! Configuration may be constructed programmatically for tests and single-node
//! development or loaded from a validated TOML file. Static seed-node metadata
//! is retained here so the same configuration format can bootstrap the metadata
//! Raft group in a later milestone.

use std::collections::HashSet;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use ragnordb_common::ids::NodeId;
use ragnordb_common::{Error, Result};
use serde::Deserialize;

const DEFAULT_MAX_CONNECTIONS: u32 = 100;
const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 5_000;
const ADMIN_PORT_OFFSET: u16 = 100;

/// controls whether client SQL text may enter structured server logs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StatementLogging {
    Off,
    MetadataOnly,
    Redacted,
    Full,
}

const fn default_statement_timeout_ms() -> u64 {
    DEFAULT_STATEMENT_TIMEOUT_MS
}

const fn default_shutdown_grace_period_ms() -> u64 {
    DEFAULT_SHUTDOWN_GRACE_PERIOD_MS
}

const fn default_statement_logging() -> StatementLogging {
    StatementLogging::MetadataOnly
}

/// Static address information for one cluster seed node.
///
/// Seed-node IDs and addresses must be stable across restarts. The metadata
/// bootstrap milestone will use this list to form the initial metadata group.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedNodeConfig {
    pub id: NodeId,
    pub raft_addr: SocketAddr,
    pub sql_addr: SocketAddr,
    pub admin_addr: SocketAddr,
}

/// Validated runtime configuration for one RagnorDB node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    /// Stable identifier for this process within the cluster.
    pub node_id: NodeId,

    /// Directory containing WAL segments, snapshots, and future storage files.
    pub data_dir: PathBuf,

    /// Address used by SQL clients.
    pub listen_addr: SocketAddr,

    /// Address serving `/status` and `/metrics`.
    pub admin_addr: SocketAddr,

    /// Maximum number of concurrent SQL client connections.
    pub max_connections: u32,

    /// Maximum time a request may wait to acquire the database execution owner.
    pub statement_timeout_ms: u64,

    /// Time allowed for accepted connections to drain during shutdown.
    pub shutdown_grace_period_ms: u64,

    /// Policy governing SQL statement text in logs.
    pub statement_logging: StatementLogging,

    /// Stable cluster identity used by metadata bootstrap.
    pub cluster_id: Option<String>,

    /// Whether this node participates in initial metadata-group bootstrap.
    pub bootstrap: bool,

    /// Static initial cluster membership.
    pub seed_nodes: Vec<SeedNodeConfig>,
}

/// Deserialization-only representation of the TOML file
///
/// Keeping this separate from `NodeConfig` allows `admin_addr` and defaults to
/// be derived before the validated runtime value is published.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeConfigFile {
    node_id: NodeId,
    data_dir: PathBuf,
    listen_addr: SocketAddr,
    admin_addr: Option<SocketAddr>,

    #[serde(default = "default_max_connections")]
    max_connections: u32,

    #[serde(default = "default_statement_timeout_ms")]
    statement_timeout_ms: u64,

    #[serde(default = "default_shutdown_grace_period_ms")]
    shutdown_grace_period_ms: u64,

    #[serde(default = "default_statement_logging")]
    statement_logging: StatementLogging,

    cluster_id: Option<String>,

    #[serde(default)]
    bootstrap: bool,

    #[serde(default)]
    seed_nodes: Vec<SeedNodeConfig>,
}

const fn default_max_connections() -> u32 {
    DEFAULT_MAX_CONNECTIONS
}

impl NodeConfig {
    /// Construct a validated single-node development configuration.
    ///
    /// The admin port is derived as `listen_port + 100`. Invalid addresses are
    /// returned as configuration errors rather than causing a process panic.
    pub fn new(node_id: NodeId, data_dir: PathBuf, listen_addr: SocketAddr) -> Result<Self> {
        let admin_addr = derive_admin_addr(listen_addr)?;

        let config = Self {
            node_id,
            data_dir,
            listen_addr,
            admin_addr,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            statement_timeout_ms: DEFAULT_STATEMENT_TIMEOUT_MS,
            shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
            statement_logging: StatementLogging::MetadataOnly,
            cluster_id: None,
            bootstrap: false,
            seed_nodes: Vec::new(),
        };

        config.validate()?;
        Ok(config)
    }

    /// Load and validate node configuration from a TOML file.
    pub fn load_from_toml(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let contents = fs::read_to_string(path).map_err(|error| {
            Error::Configuration(format!("failed to read {}: {error}", path.display()))
        })?;

        Self::from_toml_str(&contents)
    }

    /// Parse and validate node configuration from TOML text.
    ///
    /// This method is public primarily to support deterministic configuration
    /// tests without creating temporary files.
    pub fn from_toml_str(contents: &str) -> Result<Self> {
        let file: NodeConfigFile = toml::from_str(contents).map_err(|error| {
            Error::Configuration(format!("invalid node configuration: {error}"))
        })?;

        let admin_addr = match file.admin_addr {
            Some(address) => address,
            None => derive_admin_addr(file.listen_addr)?,
        };

        let config = Self {
            node_id: file.node_id,
            data_dir: file.data_dir,
            listen_addr: file.listen_addr,
            admin_addr,
            max_connections: file.max_connections,
            statement_timeout_ms: file.statement_timeout_ms,
            shutdown_grace_period_ms: file.shutdown_grace_period_ms,
            statement_logging: file.statement_logging,
            cluster_id: file.cluster_id,
            bootstrap: file.bootstrap,
            seed_nodes: file.seed_nodes,
        };

        config.validate()?;
        Ok(config)
    }

    /// Override the maximum connection count while preserving validation.
    pub fn with_max_connections(mut self, max_connections: u32) -> Result<Self> {
        self.max_connections = max_connections;
        self.validate()?;
        Ok(self)
    }

    /// Override the administrative address while preserving validation.
    pub fn with_admin_addr(mut self, admin_addr: SocketAddr) -> Result<Self> {
        self.admin_addr = admin_addr;
        self.validate()?;
        Ok(self)
    }

    /// Validate all local and static cluster invariants.
    pub fn validate(&self) -> Result<()> {
        if self.node_id.0 == 0 {
            return Err(Error::Configuration(
                "node ID 0 is reserved and cannot identify a running node".to_string(),
            ));
        }

        if self.data_dir.as_os_str().is_empty() {
            return Err(Error::Configuration(
                "data directory cannot be empty".to_string(),
            ));
        }

        if self.listen_addr == self.admin_addr {
            return Err(Error::Configuration(
                "SQL and admin addresses must be different".to_string(),
            ));
        }

        if self.max_connections == 0 {
            return Err(Error::Configuration(
                "max_connections must be greater than zero".to_string(),
            ));
        }

        if self.statement_timeout_ms == 0 {
            return Err(Error::Configuration(
                "statement_timeout_ms must be greater than zero".to_string(),
            ));
        }

        if self.shutdown_grace_period_ms == 0 {
            return Err(Error::Configuration(
                "shutdown_grace_period_ms must be greater than zero".to_string(),
            ));
        }

        let normalized_cluster_id = self
            .cluster_id
            .as_deref()
            .map(str::trim)
            .filter(|cluster_id| !cluster_id.is_empty());

        if self.cluster_id.is_some() && normalized_cluster_id.is_none() {
            return Err(Error::Configuration(
                "cluster_id cannot be empty".to_string(),
            ));
        }

        if self.bootstrap && normalized_cluster_id.is_none() {
            return Err(Error::Configuration(
                "bootstrap configuration requires cluster_id".to_string(),
            ));
        }

        if self.bootstrap && self.seed_nodes.is_empty() {
            return Err(Error::Configuration(
                "bootstrap configuration requires at least one seed node".to_string(),
            ));
        }

        if !self.seed_nodes.is_empty() && normalized_cluster_id.is_none() {
            return Err(Error::Configuration(
                "a static seed-node list requires cluster_id".to_string(),
            ));
        }

        let mut node_ids = HashSet::new();
        let mut raft_addresses = HashSet::new();
        let mut sql_addresses = HashSet::new();
        let mut admin_addresses = HashSet::new();

        for seed in &self.seed_nodes {
            if seed.id.0 == 0 {
                return Err(Error::Configuration(
                    "seed node ID 0 is reserved".to_string(),
                ));
            }

            if !node_ids.insert(seed.id) {
                return Err(Error::Configuration(format!(
                    "duplicate seed node ID: {}",
                    seed.id.0
                )));
            }

            if !raft_addresses.insert(seed.raft_addr) {
                return Err(Error::Configuration(format!(
                    "duplicate seed Raft address: {}",
                    seed.raft_addr
                )));
            }

            if !sql_addresses.insert(seed.sql_addr) {
                return Err(Error::Configuration(format!(
                    "duplicate seed SQL address: {}",
                    seed.sql_addr
                )));
            }

            if !admin_addresses.insert(seed.admin_addr) {
                return Err(Error::Configuration(format!(
                    "duplicate seed admin address: {}",
                    seed.admin_addr
                )));
            }

            if seed.raft_addr == seed.sql_addr
                || seed.raft_addr == seed.admin_addr
                || seed.sql_addr == seed.admin_addr
            {
                return Err(Error::Configuration(format!(
                    "seed node {} must use distinct Raft, SQL, and admin addresses",
                    seed.id.0
                )));
            }
        }

        if !self.seed_nodes.is_empty()
            && !self.seed_nodes.iter().any(|seed| seed.id == self.node_id)
        {
            return Err(Error::Configuration(format!(
                "local node ID {} is missing from the static seed-node list",
                self.node_id.0
            )));
        }

        Ok(())
    }
}

fn derive_admin_addr(listen_addr: SocketAddr) -> Result<SocketAddr> {
    let admin_port = listen_addr
        .port()
        .checked_add(ADMIN_PORT_OFFSET)
        .ok_or_else(|| {
            Error::Configuration(format!(
                "cannot derive admin port from SQL address {listen_addr}: port overflow"
            ))
        })?;

    Ok(SocketAddr::new(listen_addr.ip(), admin_port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_admin_address_for_single_node_configuration() {
        let config = NodeConfig::new(
            NodeId(1),
            PathBuf::from("./data"),
            "127.0.0.1:7101".parse().unwrap(),
        )
        .unwrap();

        assert_eq!(config.admin_addr, "127.0.0.1:7201".parse().unwrap());
        assert_eq!(config.max_connections, 100);
        assert_eq!(config.statement_timeout_ms, 30_000);
        assert_eq!(config.shutdown_grace_period_ms, 5_000);
        assert_eq!(config.statement_logging, StatementLogging::MetadataOnly);
        assert!(config.seed_nodes.is_empty());
    }

    #[test]
    fn rejects_admin_port_overflow() {
        let error = NodeConfig::new(
            NodeId(1),
            PathBuf::from("./data"),
            "127.0.0.1:65500".parse().unwrap(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("port overflow"));
    }

    #[test]
    fn parses_static_cluster_configuration() {
        let config = NodeConfig::from_toml_str(
            r#"
node_id = 1
data_dir = "./data/n1"
listen_addr = "127.0.0.1:7101"
max_connections = 128
statement_timeout_ms = 2500
shutdown_grace_period_ms = 7000
statement_logging = "redacted"
cluster_id = "ragnordb-dev"
bootstrap = true

[[seed_nodes]]
id = 1
raft_addr = "127.0.0.1:7001"
sql_addr = "127.0.0.1:7101"
admin_addr = "127.0.0.1:7201"

[[seed_nodes]]
id = 2
raft_addr = "127.0.0.1:7002"
sql_addr = "127.0.0.1:7102"
admin_addr = "127.0.0.1:7202"

[[seed_nodes]]
id = 3
raft_addr = "127.0.0.1:7003"
sql_addr = "127.0.0.1:7103"
admin_addr = "127.0.0.1:7203"
"#,
        )
        .unwrap();

        assert_eq!(config.node_id, NodeId(1));
        assert_eq!(config.max_connections, 128);
        assert_eq!(config.statement_timeout_ms, 2_500);
        assert_eq!(config.shutdown_grace_period_ms, 7_000);
        assert_eq!(config.statement_logging, StatementLogging::Redacted);
        assert_eq!(config.seed_nodes.len(), 3);
        assert_eq!(config.cluster_id.as_deref(), Some("ragnordb-dev"));
        assert!(config.bootstrap);
    }

    #[test]
    fn rejects_duplicate_seed_node_ids() {
        let error = NodeConfig::from_toml_str(
            r#"
node_id = 1
data_dir = "./data/n1"
listen_addr = "127.0.0.1:7101"
cluster_id = "ragnordb-dev"

[[seed_nodes]]
id = 1
raft_addr = "127.0.0.1:7001"
sql_addr = "127.0.0.1:7101"
admin_addr = "127.0.0.1:7201"

[[seed_nodes]]
id = 1
raft_addr = "127.0.0.1:7002"
sql_addr = "127.0.0.1:7102"
admin_addr = "127.0.0.1:7202"
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("duplicate seed node ID"));
    }

    #[test]
    fn rejects_zero_max_connections() {
        let error = NodeConfig::from_toml_str(
            r#"
node_id = 1
data_dir = "./data/n1"
listen_addr = "127.0.0.1:7101"
max_connections = 0
"#,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("max_connections must be greater than zero")
        );
    }

    #[test]
    fn rejects_unknown_configuration_fields() {
        let error = NodeConfig::from_toml_str(
            r#"
node_id = 1
data_dir = "./data/n1"
listen_addr = "127.0.0.1:7101"
unknown_setting = true
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown field"));
    }
}
