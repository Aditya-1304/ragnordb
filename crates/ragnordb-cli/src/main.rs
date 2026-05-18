fn main() {
    let server = ragnordb_server::Server::new();

    if let Err(error) = server.start() {
        eprintln!("ragnordb failed: {error}");
        std::process::exit(1);
    }

    println!("RagnorDB workspace initialized.");
}
