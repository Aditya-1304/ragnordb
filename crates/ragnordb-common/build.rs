//! This file compiles all six .proto schema files into Rust modules at build time using the proto-build crate.
//! all the output are stored inside the src/proto/*.rs
//!
//! should rerun if the schema files are changed as all other crate uses this protobufs

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
    ];

    std::fs::create_dir_all("src/proto")?;

    let mut config = prost_build::Config::new();
    config.out_dir("src/proto");

    config.compile_protos(protos, &[proto_dir])?;

    println!("cargo:rerun-if-changed=../../proto");
    Ok(())
}
