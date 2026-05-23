use clap::{Parser, Subcommand};
use ragnordb_common::ids::NodeId;
use ragnordb_server::config::NodeConfig;
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

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

/// this will open a TCP connection to RagnorDB node and start an interactive REPL
///
/// the user can type SQL statements and see JSON responses
async fn run_sql(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect(addr).await?;
    println!("connected to RagnorDB at {addr}");
    println!("type 'exit' or 'quit' to disconnect.\n");

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

        println!("[connection closed]")
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
                eprintln!("read error: {e}");
                break;
            }
            Ok(_) => {}
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if matches!(trimmed, "exit" | "quit") {
            println!("bye");
            break;
        }

        writer.write_all(trimmed.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

    server_task.abort();

    Ok(())
}

/// this checks if a ragnordb node is alive by attempting a TCP connection
/// this will only verify if process is listening or not
async fn run_status(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    println!("RagnorDB node status");
    println!("  Address: {addr}");

    match TcpStream::connect(addr).await {
        Ok(_stream) => {
            println!("  Alive: yes");
        }
        Err(e) => {
            println!(" Alive: no");
            println!(" Error: {e}");
        }
    }

    Ok(())
}
