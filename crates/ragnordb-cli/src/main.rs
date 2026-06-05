use clap::{Parser, Subcommand};
use ragnordb_common::ids::NodeId;
use ragnordb_server::config::NodeConfig;
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{error, info, warn};

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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

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
        error!(error = %e, "fatal");
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

/// Open a TCP connection to a RagnorDB node and start an interactive REPL.
///
/// The user can type SQL statements and see JSON responses.
async fn run_sql(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect(addr).await?;
    info!(%addr, "connected to RagnorDB");
    info!("type 'exit' or 'quit' to disconnect");

    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut stdin = BufReader::new(tokio::io::stdin());

    let server_task = tokio::spawn(async move {
        let mut response = String::new();
        loop {
            response.clear();
            match reader.read_line(&mut response).await {
                Ok(0) | Err(_) => break,
                Ok(_) => match serde_json::from_str::<serde_json::Value>(response.trim()) {
                    Ok(json) => {
                        println!("{}", serde_json::to_string_pretty(&json).unwrap())
                    }
                    Err(_) => {
                        println!("{}", response.trim());
                    }
                },
            }
        }

        warn!("connection closed by server")
    });

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
            Err(e) => {
                error!(error = %e, "stdin read error");
                break;
            }
            Ok(_) => {}
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if matches!(trimmed, "exit" | "quit") {
            info!("bye");
            break;
        }

        writer.write_all(trimmed.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

    server_task.abort();

    Ok(())
}

/// Check if a RagnorDB node is alive by attempting a TCP connection.
/// Also prints build info for the local binary.
async fn run_status(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", ragnordb_server::build_info::BUILD_INFO);

    match TcpStream::connect(addr).await {
        Ok(_stream) => {
            println!("  Alive: yes");
        }
        Err(e) => {
            println!("  Alive: no");
            println!("  Error: {e}");
        }
    }

    Ok(())
}
