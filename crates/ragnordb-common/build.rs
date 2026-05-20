fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";
    let protos = &[
        "../../proto/ids.proto",
        "../../proto/row.proto",
        "../../proto/catalog.proto",
        "../../proto/mvcc.proto",
        "../../proto/command.proto",
        "../../proto/rpc.proto",
    ];

    std::fs::create_dir_all("src/proto")?;

    let mut config = prost_build::Config::new();
    config.out_dir("src/proto");

    config.compile_protos(protos, &[proto_dir])?;

    println!("cargo:rerun-if-changed=../../proto");
    Ok(())
}
