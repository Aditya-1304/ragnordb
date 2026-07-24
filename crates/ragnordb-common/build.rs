//! compiles RagnorDB's protobuf schemas into Rust modules
//!
//! generated modules are written into `src/proto`. Durable and cross-node
//! schemas must remain protobuf-backed so recovery does not depend on
//! process-local Rust layouts

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto"; // this is because i am using relative path to crate root
    let protos = &[
        "../../proto/ids.proto",
        "../../proto/row.proto",
        "../../proto/catalog.proto",
        "../../proto/mvcc.proto",
        "../../proto/command.proto",
        "../../proto/rpc.proto",
        "../../proto/wal.proto",
        "../../proto/snapshot.proto",
    ];

    std::fs::create_dir_all("src/proto")?;

    let mut config = prost_build::Config::new();
    config.out_dir("src/proto");

    config.compile_protos(protos, &[proto_dir])?;

    println!("cargo:rerun-if-changed=../../proto");
    Ok(())
}
