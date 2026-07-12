use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be available"),
    );

    let raft_manifest = manifest_dir.join("../../../Papers/raft/Cargo.toml");
    let wal_manifest = manifest_dir.join("../../../wal/Cargo.toml");
    let bloom_manifest = manifest_dir.join("../../../bloom-bloom/Cargo.toml");

    emit_rerun_directive(&raft_manifest);
    emit_rerun_directive(&wal_manifest);
    emit_rerun_directive(&bloom_manifest);

    println!(
        "cargo:rustc-env=TARGET={}",
        env::var("TARGET").unwrap_or_else(|_| "unknown".to_string())
    );

    println!(
        "cargo:rustc-env=BUILT_AT={}",
        command_output("date", &["+%Y-%m-%d"])
    );

    println!(
        "cargo:rustc-env=RUSTC_VERSION={}",
        command_output("rustc", &["--version"])
    );

    println!(
        "cargo:rustc-env=RAFT_VERSION={}",
        package_version(&raft_manifest)
    );

    println!(
        "cargo:rustc-env=WAL_VERSION={}",
        package_version(&wal_manifest)
    );

    println!(
        "cargo:rustc-env=BLOOM_VERSION={}",
        package_version(&bloom_manifest)
    );

    println!("cargo:rustc-env=RAGNORDB_FEATURES={}", enabled_features());
}

fn emit_rerun_directive(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|output| !output.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn package_version(manifest_path: &Path) -> String {
    let contents = fs::read_to_string(manifest_path).unwrap_or_else(|error| {
        panic!(
            "failed to read dependency manifest {}: {error}",
            manifest_path.display()
        )
    });

    let manifest: toml::Value = toml::from_str(&contents).unwrap_or_else(|error| {
        panic!(
            "failed to parse dependency manifest {}: {error}",
            manifest_path.display()
        )
    });

    manifest
        .get("package")
        .and_then(|package| package.get("version"))
        .and_then(toml::Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| {
            panic!(
                "dependency manifest {} has no package.version",
                manifest_path.display()
            )
        })
}

fn enabled_features() -> String {
    let mut features = env::vars()
        .filter_map(|(name, _)| {
            name.strip_prefix("CARGO_FEATURE_")
                .map(|feature| feature.to_ascii_lowercase().replace('_', "-"))
        })
        .collect::<Vec<_>>();

    features.sort_unstable();

    if features.is_empty() {
        "none".to_string()
    } else {
        features.join(",")
    }
}
