use clap::{Parser, Subcommand};
use ragnordb_common::ids::NodeId;
use ragnordb_common::protocol::read_frame;
use ragnordb_server::config::NodeConfig;
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
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

async fn run_sql(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect(addr).await?;
    info!(%addr, "connected to RagnorDB");
    info!("type 'exit' or 'quit' to disconnect");

    let (mut reader, mut writer) = stream.into_split();
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());

    let server_task = tokio::spawn(async move {
        loop {
            match read_frame(&mut reader).await {
                Ok(response) => match serde_json::from_str::<serde_json::Value>(&response) {
                    Ok(json) => println!("{}", serde_json::to_string_pretty(&json).unwrap()),
                    Err(_) => println!("{response}"),
                },
                Err(_) => break,
            }
        }
        warn!("connection closed by server");
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

        send_frame(&mut writer, trimmed).await?;
    }

    server_task.abort();

    Ok(())
}

/// Check if a RagnorDB node is alive by attempting a TCP connection.
/// Also prints build info for the local binary.
async fn run_status(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", ragnordb_server::build_info::BUILD_INFO);

    match TcpStream::connect(addr).await {
        Ok(_) => println!("  SQL port: alive"),
        Err(e) => println!("  SQL port: unreachable ({e})"),
    }

    let admin_port = addr.port() + 100;
    let admin_addr = SocketAddr::new(addr.ip(), admin_port);
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
