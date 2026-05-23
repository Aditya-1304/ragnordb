use clap::{Parser, Subcommand};
use ragnordb_common::ids::NodeId;
use ragnordb_server::config::NodeConfig;
use std::net::SocketAddr;

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
        #[arg(long, default_value = "1")]
        id: u64,

        #[arg(long, default_value = "./data")]
        data_dir: String,

        #[arg(long, default_value = "127.0.0.1:7101")]
        listen: SocketAddr,
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
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Node {
            id,
            data_dir,
            listen,
        } => run_node(id, &data_dir, listen).await,
        Commands::Sql { addr } => run_sql(addr).await,
        Commands::Status { addr } => run_status(addr).await,
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run_node(
    id: u64,
    data_dir: &str,
    listen: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = NodeConfig::new(NodeId(id), std::path::PathBuf::from(data_dir), listen);
    let server = ragnordb_server::Server::new(config);
    server.start().await?;
    Ok(())
}
async fn run_sql(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("run_sql not implemented yet (addr={addr})");
    Ok(())
}

async fn run_status(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("run_status not implemented yet (addr={addr})");
    Ok(())
}
