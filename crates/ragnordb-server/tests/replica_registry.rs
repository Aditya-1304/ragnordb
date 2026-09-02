use std::{fs, path::PathBuf};

use ragnordb_common::{
    Error,
    ids::{RaftGroupId, ReplicaId, TableId, TabletId},
};
use ragnordb_server::replica_registry::{
    DurableFrontier, LocalReplicaRecord, LocalReplicaRegistry, RegistryMutation, ReplicaLifecycle,
};

fn registry_path() -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::tempdir().expect("temporary registry directory");
    let path = directory.path().join("replica-registry.json");
    (directory, path)
}

fn record() -> LocalReplicaRecord {
    LocalReplicaRecord::new(
        RaftGroupId(17),
        ReplicaId(3),
        TabletId(9),
        TableId(5),
        4,
        ReplicaLifecycle::Creating,
    )
    .with_frontiers(
        Some(DurableFrontier::new(22, 7)),
        Some(DurableFrontier::new(25, 8)),
    )
}

/// Realistic bug caught: a crash/restart between local replica allocation and
/// Raft activation must not lose the identity or create a second lifetime.
#[test]
fn registry_reopen_preserves_record_and_exact_replay_is_idempotent() {
    let (_directory, path) = registry_path();
    let expected = record();

    let mut registry = LocalReplicaRegistry::open(&path, "cluster-a").unwrap();
    assert_eq!(
        registry.ensure_replica(expected.clone()).unwrap(),
        RegistryMutation::Created
    );
    registry.mark_active(expected.key()).unwrap();
    registry
        .update_frontiers(
            expected.key(),
            Some(DurableFrontier::new(24, 7)),
            Some(DurableFrontier::new(27, 8)),
        )
        .unwrap();
    drop(registry);

    let mut reopened = LocalReplicaRegistry::open(&path, "cluster-a").unwrap();
    let persisted = reopened.record(expected.key()).unwrap().unwrap();
    assert_eq!(persisted.lifecycle, ReplicaLifecycle::Active);
    assert_eq!(
        persisted.snapshot_frontier,
        Some(DurableFrontier::new(24, 7))
    );
    assert_eq!(persisted.apply_frontier, Some(DurableFrontier::new(27, 8)));
    assert_eq!(
        reopened.ensure_replica(persisted.clone()).unwrap(),
        RegistryMutation::AlreadyPresent
    );
}

/// Realistic bug caught: an allocator or reconciler must not silently bind an
/// existing `(group, replica)` lifetime to a different tablet epoch.
#[test]
fn registry_rejects_conflicting_identity_reuse() {
    let (_directory, path) = registry_path();
    let mut registry = LocalReplicaRegistry::open(&path, "cluster-a").unwrap();
    let original = record();
    registry.ensure_replica(original).unwrap();

    let conflicting = LocalReplicaRecord::new(
        RaftGroupId(17),
        ReplicaId(3),
        TabletId(9),
        TableId(5),
        5,
        ReplicaLifecycle::Creating,
    );
    let error = registry
        .ensure_replica(conflicting)
        .expect_err("conflicting replica lifetime must be rejected");
    assert!(error.to_string().contains("conflicts"));
}

/// Realistic bug caught: a torn or hand-edited registry must not be accepted
/// as an empty registry, because doing so could cause a second local lifetime
/// to be allocated for durable tablet data that still exists.
#[test]
fn registry_rejects_corrupt_bytes_and_cluster_mismatch() {
    let (_directory, path) = registry_path();
    fs::write(&path, b"not-json").unwrap();
    let error = LocalReplicaRegistry::open(&path, "cluster-a").unwrap_err();
    assert!(error.to_string().contains("decode local replica registry"));

    let (_directory, valid_path) = registry_path();
    let mut registry = LocalReplicaRegistry::open(&valid_path, "cluster-a").unwrap();
    registry.ensure_replica(record()).unwrap();
    drop(registry);
    let error = LocalReplicaRegistry::open(&valid_path, "cluster-b").unwrap_err();
    assert!(error.to_string().contains("belongs to cluster"));
}

/// Realistic bug caught: publishing a stale apply or snapshot frontier could
/// make restart discard committed state or restore an invalid snapshot.
#[test]
fn registry_rejects_frontier_regression() {
    let (_directory, path) = registry_path();
    let mut registry = LocalReplicaRegistry::open(&path, "cluster-a").unwrap();
    let expected = record();
    registry.ensure_replica(expected.clone()).unwrap();

    let error = registry
        .update_frontiers(
            expected.key(),
            Some(DurableFrontier::new(30, 8)),
            Some(DurableFrontier::new(29, 8)),
        )
        .unwrap_err();
    assert!(error.to_string().contains("ahead of apply"));

    let error = registry
        .update_frontiers(
            expected.key(),
            Some(DurableFrontier::new(22, 9)),
            Some(DurableFrontier::new(22, 8)),
        )
        .unwrap_err();
    assert!(error.to_string().contains("ahead of apply"));

    registry
        .update_frontiers(
            expected.key(),
            Some(DurableFrontier::new(22, 7)),
            Some(DurableFrontier::new(25, 8)),
        )
        .unwrap();
    let error = registry
        .update_frontiers(
            expected.key(),
            Some(DurableFrontier::new(21, 7)),
            Some(DurableFrontier::new(24, 8)),
        )
        .unwrap_err();
    assert!(error.to_string().contains("regresses durable state"));

    let error = registry
        .update_frontiers(
            expected.key(),
            Some(DurableFrontier::new(25, 7)),
            Some(DurableFrontier::new(26, 7)),
        )
        .unwrap_err();
    assert!(error.to_string().contains("regresses durable state"));
}

/// Realistic bug caught: after a filesystem error during publication, the
/// handle must not continue from an in-memory image that may differ from the
/// image a restart will recover.
#[test]
fn failed_publication_poisons_handle_until_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("replica-registry.json");
    let mut registry = LocalReplicaRegistry::open(&path, "cluster-a").unwrap();

    // A directory at the final registry path makes the durable replacement
    // fail before the new image can be installed.
    fs::create_dir(&path).unwrap();
    let first_error = registry.ensure_replica(record()).unwrap_err();
    assert!(matches!(first_error, Error::RecoveryFailed { .. }));

    let second_error = registry.ensure_replica(record()).unwrap_err();
    assert!(matches!(second_error, Error::RecoveryRequired { .. }));

    drop(registry);
    fs::remove_dir(&path).unwrap();
    let mut reopened = LocalReplicaRegistry::open(&path, "cluster-a").unwrap();
    assert_eq!(
        reopened.ensure_replica(record()).unwrap(),
        RegistryMutation::Created
    );
}

/// Realistic bug caught: concurrent lifecycle owners must not publish
/// competing full-image replacements that silently discard one another.
#[test]
fn registry_enforces_single_writer_ownership() {
    let (_directory, path) = registry_path();
    let first = LocalReplicaRegistry::open(&path, "cluster-a").unwrap();
    let error = LocalReplicaRegistry::open(&path, "cluster-a").unwrap_err();
    assert!(matches!(error, Error::Configuration(_)));
    drop(first);

    LocalReplicaRegistry::open(&path, "cluster-a").unwrap();
}

/// Realistic bug caught: restart must not silently omit a replica that the
/// local registry had already declared active.
#[test]
fn active_registry_record_must_be_present_in_recovered_lifetimes() {
    let (_directory, path) = registry_path();
    let mut registry = LocalReplicaRegistry::open(&path, "cluster-a").unwrap();
    let expected = record();
    registry.ensure_replica(expected.clone()).unwrap();
    registry.mark_active(expected.key()).unwrap();

    let error = registry
        .validate_recovered_lifetimes(std::iter::empty())
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("missing from shared-WAL recovery")
    );

    registry
        .validate_recovered_lifetimes([expected.key()])
        .unwrap();
}
