/// compile time build metadata for RagnorDB .
pub struct BuildInfo {
    pub ragnordb_version: &'static str,
    pub target: &'static str,
    pub built_at: &'static str,
    pub rust_version: &'static str,
    pub raft_version: &'static str,
    pub wal_version: &'static str,
    pub bloom_version: &'static str,
}

pub const BUILD_INFO: BuildInfo = BuildInfo {
    ragnordb_version: env!("CARGO_PKG_VERSION"),
    target: env!("TARGET"),
    built_at: env!("BUILT_AT"),
    rust_version: env!("RUSTC_VERSION"),
    raft_version: "dev",
    wal_version: "dev",
    bloom_version: "dev",
};

impl std::fmt::Display for BuildInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RagnorDB v{}
  Target:        {}
  Built:         {}
  Rust:          {}
  Infrastructure:
    raft:        {} (path: ../Papers/raft)
    wal:         {} (path: ../wal)
    bloom-bloom: {} (path: ../bloom-bloom)",
            self.ragnordb_version,
            self.target,
            self.built_at,
            self.rust_version,
            self.raft_version,
            self.wal_version,
            self.bloom_version,
        )
    }
}
