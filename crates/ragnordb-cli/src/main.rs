use clap::{Parser, Subcommand};
use ragnordb_common::ids::NodeId;
use ragnordb_common::protocol::read_frame;
use ragnordb_server::config::NodeConfig;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info};
mod inspect;

#[derive(Parser)]
#[command(name = "ragnordb", about = "Distributed OLTP SQL Database")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// command to start a single ragnorDB node
    Node {
        /// Load node and static cluster configuration from TOML.
        ///
        /// When provided, the configuration file is authoritative and individual node
        /// flags cannot be supplied at the same time.
        #[arg(long, value_name = "PATH", conflicts_with_all = [
            "id",
            "data_dir",
            "listen",
            "admin_listen",
            "max_connections"
            ]
        )]
        config: Option<PathBuf>,

        #[arg(long, default_value = "1")]
        id: u64,

        #[arg(long, default_value = "./data")]
        data_dir: String,

        #[arg(long, default_value = "127.0.0.1:7101")]
        listen: SocketAddr,

        #[arg(long)]
        admin_listen: Option<SocketAddr>,

        #[arg(long, default_value = "100")]
        max_connections: u32,
    },

    /// command to open interactive SQL shell
    Sql {
        #[arg(long, default_value = "127.0.0.1:7101")]
        addr: SocketAddr,
    },

    /// command to get status of node
    Status {
        #[arg(long, default_value = "127.0.0.1:7101")]
        addr: SocketAddr,

        #[arg(long)]
        admin_addr: Option<SocketAddr>,
    },

    /// inspect persisted RagnorDB state without starting a database server.
    Inspect {
        #[command(subcommand)]
        command: InspectCommands,
    },
}

#[derive(Subcommand)]
enum InspectCommands {
    /// inspect an offline node local A-WAL
    ///
    /// Stop the RagnorDB server first. This standalone command is read-only
    /// and cannot hold the server process's retention pin
    Wal {
        /// directory containing the node's `wal/` directory
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// stable node identity used to open this node's A-WAL
        #[arg(long, default_value = "1")]
        node_id: u64,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Node {
            config,
            id,
            data_dir,
            listen,
            admin_listen,
            max_connections,
        } => run_node(config, id, &data_dir, listen, admin_listen, max_connections).await,
        Commands::Sql { addr } => run_sql(addr).await,
        Commands::Status { addr, admin_addr } => run_status(addr, admin_addr).await,
        Commands::Inspect { command } => match command {
            InspectCommands::Wal { data_dir, node_id } => {
                inspect::run_wal(&data_dir, NodeId(node_id))
            }
        },
    };

    if let Err(e) = result {
        error!(error = %e, "fatal");
        std::process::exit(1);
    }
}

async fn run_node(
    config_path: Option<PathBuf>,
    id: u64,
    data_dir: &str,
    listen: SocketAddr,
    admin_listen: Option<SocketAddr>,
    max_connections: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = match config_path {
        Some(path) => NodeConfig::load_from_toml(path)?,
        None => {
            let mut config = NodeConfig::new(NodeId(id), PathBuf::from(data_dir), listen)?
                .with_max_connections(max_connections)?;

            if let Some(admin_addr) = admin_listen {
                config = config.with_admin_addr(admin_addr)?;
            }

            config
        }
    };

    let server = ragnordb_server::Server::new(config);
    server.start().await?;

    Ok(())
}

async fn send_frame(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = sql.as_bytes();
    let len = bytes.len() as u32;
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Run the interactive V1 SQL shell.
///
/// The V1 connection policy permits one in-flight statement per connection.
/// The shell therefore follows a strict request-response cycle:
///
/// 1. display the prompt,
/// 2. read one statement,
/// 3. send one request frame,
/// 4. wait for exactly one response frame,
/// 5. print the response,
/// 6. display the next prompt.
///
/// Waiting for the response before drawing the next prompt prevents asynchronous
/// server output from overwriting or visually displacing the prompt.
async fn run_sql(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect(addr).await?;

    info!(%addr, "connected to RagnorDB");
    info!("type 'exit' or 'quit' to disconnect");

    let (mut reader, mut writer) = stream.into_split();
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut line = String::new();

    loop {
        line.clear();

        print!("ragnordb> ");

        use std::io::Write;
        std::io::stdout().flush()?;

        match stdin.read_line(&mut line).await {
            Ok(0) => {
                println!();
                break;
            }

            Ok(_) => {}

            Err(error) => {
                error!(error = %error, "stdin read error");
                break;
            }
        }

        let trimmed = line.trim();

        if trimmed.is_empty() {
            continue;
        }

        if matches!(trimmed, "exit" | "quit") {
            info!("bye");
            break;
        }

        send_frame(&mut writer, trimmed).await?;

        // V1 guarantees one response for every request. Reading the response in
        // this loop preserves statement ordering and ensures the next prompt is
        // rendered only after the complete response has been printed.
        let response = read_frame(&mut reader).await?;

        match serde_json::from_str::<serde_json::Value>(&response) {
            Ok(json) => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json)
                        .expect("serializing serde_json::Value cannot fail")
                );
            }

            Err(_) => {
                println!("{response}");
            }
        }
    }

    Ok(())
}

/// Check if a RagnorDB node is alive by attempting a TCP connection.
/// Also prints build info for the local binary.
async fn run_status(
    addr: SocketAddr,
    admin_addr: Option<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", ragnordb_server::build_info::BUILD_INFO);

    match TcpStream::connect(addr).await {
        Ok(_) => println!("  SQL port: alive"),
        Err(e) => println!("  SQL port: unreachable ({e})"),
    }

    let admin_addr = match admin_addr {
        Some(admin_addr) => admin_addr,
        None => match addr.port().checked_add(100) {
            Some(admin_port) => SocketAddr::new(addr.ip(), admin_port),
            None => {
                println!("  Admin HTTP: unreachable (derived admin port overflow)");
                return Ok(());
            }
        },
    };

    let url = format!("http://{admin_addr}/status");

    match reqwest::get(&url).await {
        Ok(resp) => match resp.text().await {
            Ok(body) => {
                println!("  Admin HTTP: alive");
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                    println!("  Status: {}", serde_json::to_string_pretty(&json).unwrap());
                }
            }
            Err(e) => println!("  Admin HTTP: response error ({e})"),
        },
        Err(e) => println!("  Admin HTTP: unreachable ({e})"),
    }

    Ok(())
}
