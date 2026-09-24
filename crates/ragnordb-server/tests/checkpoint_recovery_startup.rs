use std::{error::Error as StdError, fs, path::Path};

use ragnordb_common::{
    Error, RetryAction,
    codec::{Row, TxnStatus, TxnStatusRecord, Value, WriteKind},
    command_codec::{
        CommitCommand, PrewriteCommand, TabletCommand, TabletCommandEnvelope, WriteEntry,
    },
    ids::{
        ClientRequestId, CommandKind, LogicalCommandId, NodeId, RaftGroupId, ReplicaId, RequestId,
        TabletId, Timestamp, TxnId,
    },
};
use ragnordb_exec::{ExecutionResult, ResultSet, SqlSession};
use ragnordb_multiraft::storage::{
    codec::{
        DurableRaftEntryPayload, RAFT_LOG_ENTRY_RECORD_VERSION, RaftHardStateRecord,
        RaftLogEntryRecord, RaftReplicaIdentity,
    },
    persistence::RaftWalRecordType,
    recovery::RaftStorageRecoveryError,
    shared_recovery::SharedStorageRecoveryError,
};
use ragnordb_server::database::LocalDatabase;
use ragnordb_storage::{
    checkpoint::{PublishedSnapshotFile, publish_checkpoint, publish_snapshot_file},
    wal::RagnorDbWalAdapter,
};
use wal::{
    config::WalConfig, io::directory::FsSegmentDirectory, types::WalIdentity, wal::WalHandle,
};

fn result_set(result: ExecutionResult) -> ResultSet {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query result");
    };

    result
}

fn open_checkpoint_adapter(
    data_dir: &Path,
    node_id: NodeId,
) -> RagnorDbWalAdapter<FsSegmentDirectory, ()> {
    let wal_dir = data_dir.join("wal");
    let config = WalConfig {
        dir: wal_dir.clone(),
        identity: WalIdentity::new(node_id.0, 1, 1),
        ..WalConfig::default()
    };

    let (wal, _) = WalHandle::open(FsSegmentDirectory::new(wal_dir), config, ())
        .expect("checkpoint WAL must reopen");

    RagnorDbWalAdapter::new(wal)
}

fn durably_publish_checkpoint_metadata(
    data_dir: &Path,
    node_id: NodeId,
    snapshot_file: &PublishedSnapshotFile,
) {
    let adapter = open_checkpoint_adapter(data_dir, node_id);
    let published = publish_checkpoint(&adapter, snapshot_file)
        .expect("checkpoint pointer and marker must become durable");

    assert!(published.pointer_extent.end_lsn <= published.marker_extent.start_lsn);
}

/// Realistic bug caught:
///
/// Startup could validate a checkpoint pair but still replay from `Lsn::ZERO`,
/// or restore the snapshot without applying commits at its captured suffix
/// boundary. Either error duplicates covered writes or loses commits completed
/// while the immutable snapshot file was being published.
#[test]
fn startup_restores_checkpoint_then_replays_only_its_wal_suffix() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let node_id = NodeId(17);
    let (mut database, _) =
        LocalDatabase::recover(data_dir.path(), node_id).expect("empty recovery must succeed");
    let mut session = SqlSession::new();

    database
        .execute_sql(
            &mut session,
            "CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .expect("table creation must become durable");

    database
        .execute_sql(
            &mut session,
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
        )
        .expect("checkpoint-covered row must become durable");

    let snapshot = database
        .capture_checkpoint_image()
        .expect("consistent checkpoint image must be captured");

    let snapshot_file = publish_snapshot_file(data_dir.path(), &snapshot)
        .expect("snapshot file must become durable");

    database
        .execute_sql(
            &mut session,
            "INSERT INTO users (id, name) VALUES (2, 'Grace')",
        )
        .expect("post-checkpoint row must become durable");

    drop(session);
    drop(database);

    durably_publish_checkpoint_metadata(data_dir.path(), node_id, &snapshot_file);

    let (mut recovered, _) = LocalDatabase::recover(data_dir.path(), node_id)
        .expect("checkpoint-based startup must succeed");
    let mut recovered_session = SqlSession::new();

    let started = recovered
        .execute_sql(&mut recovered_session, "BEGIN")
        .expect("post-recovery transaction allocation must succeed");

    assert!(matches!(
        started,
        ExecutionResult::TransactionStarted {
            transaction_id: TxnId(3),
            start_ts: Timestamp(7),
        }
    ));

    let rows = result_set(
        recovered
            .execute_sql(&mut recovered_session, "SELECT id, name FROM users")
            .expect("restored table and WAL suffix must be queryable"),
    );

    assert_eq!(
        rows.rows,
        vec![
            Row {
                values: vec![Value::Int(1), Value::Text("Ada".to_string())],
            },
            Row {
                values: vec![Value::Int(2), Value::Text("Grace".to_string())],
            },
        ]
    );

    recovered
        .execute_sql(&mut recovered_session, "ROLLBACK")
        .expect("recovered transaction must roll back");

    let next_snapshot = recovered
        .capture_checkpoint_image()
        .expect("snapshot allocator must resume above the recovered checkpoint");

    assert_eq!(next_snapshot.snapshot_id, 2);
}

/// Realistic bug caught:
///
/// Startup could select a durable pointer/marker pair but ignore a missing or
/// corrupt referenced file and silently fall back to full WAL replay. Once
/// retention has pruned the covered prefix, that fallback no longer exists, so
/// accepting the startup would expose incomplete or unrecoverable state.
#[test]
fn startup_fails_closed_when_selected_snapshot_is_corrupt() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let node_id = NodeId(18);
    let (mut database, _) =
        LocalDatabase::recover(data_dir.path(), node_id).expect("empty recovery must succeed");
    let mut session = SqlSession::new();

    database
        .execute_sql(
            &mut session,
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)",
        )
        .expect("table creation must become durable");

    database
        .execute_sql(
            &mut session,
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
        )
        .expect("row insertion must become durable");

    let snapshot = database
        .capture_checkpoint_image()
        .expect("consistent checkpoint image must be captured");

    let snapshot_file = publish_snapshot_file(data_dir.path(), &snapshot)
        .expect("snapshot file must become durable");

    let snapshot_path = data_dir.path().join(snapshot_file.relative_path());

    drop(session);
    drop(database);

    durably_publish_checkpoint_metadata(data_dir.path(), node_id, &snapshot_file);

    let mut bytes = fs::read(&snapshot_path).expect("published snapshot must be readable");
    let last = bytes
        .last_mut()
        .expect("published snapshot must contain a protobuf body");

    *last ^= 0x01;

    fs::write(&snapshot_path, bytes).expect("snapshot corruption must be installed");

    let error = match LocalDatabase::recover(data_dir.path(), node_id) {
        Err(error) => error,
        Ok(_) => panic!("startup must reject the corrupt selected snapshot"),
    };

    assert!(matches!(
        error,
        Error::CorruptData(message) if message.contains("checksum mismatch")
    ));
}

/// A physical WAL-open failure must retain A-WAL's typed error as a source.
/// Formatting the cause into `RecoveryFailed.reason` alone loses the variant
/// needed by operators and recovery tooling to distinguish physical failures.
#[test]
fn startup_preserves_typed_awal_recovery_failure() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let wal_dir = data_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).expect("temporary WAL directory must be created");

    // A segment with a valid filename but an invalid header forces the real
    // filesystem A-WAL recovery path to return a concrete WalError.
    let segment_path = wal_dir.join(wal::io::directory::format_segment_filename(
        1,
        wal::lsn::Lsn::ZERO,
    ));
    fs::write(&segment_path, [0u8; 128]).expect("corrupt segment header must be installed");

    let error = match LocalDatabase::recover(data_dir.path(), NodeId(19)) {
        Err(error) => error,
        Ok(_) => panic!("startup must reject a segment with an invalid WAL header"),
    };

    assert!(matches!(&error, Error::RecoveryFailedWithSource { .. }));
    assert_eq!(error.retry_action(), RetryAction::None);

    let source = StdError::source(&error)
        .expect("physical A-WAL recovery error must be preserved as an error source");
    assert!(
        source.downcast_ref::<wal::error::WalError>().is_some(),
        "the source must retain A-WAL's concrete error type, got: {source}"
    );
}

/// Realistic bug caught: shared database/Raft startup used to flatten a typed
/// Raft replay failure into a display string, preventing recovery tooling from
/// distinguishing a malformed durable Raft record from a physical WAL error.
#[test]
fn shared_startup_preserves_typed_raft_recovery_failure() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let node_id = NodeId(23);
    let wal_dir = data_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).expect("test WAL directory must be created");
    let config = WalConfig {
        dir: wal_dir.clone(),
        identity: WalIdentity::new(node_id.0, 1, 1),
        ..WalConfig::default()
    };
    let (wal, _) = WalHandle::open(FsSegmentDirectory::new(wal_dir), config, ())
        .expect("test WAL must open before the malformed Raft record is appended");
    let _extent = wal
        .append_and_sync(
            RaftWalRecordType::LogEntry.as_wal_record_type(),
            b"invalid durable Raft log entry",
        )
        .expect("A-WAL must durably accept the malformed Raft payload as opaque bytes");
    drop(wal);

    let configurations = std::collections::BTreeMap::new();
    let error =
        match LocalDatabase::recover_shared_with_raft(data_dir.path(), node_id, &configurations) {
            Err(error) => error,
            Ok(_) => panic!("shared recovery must reject a malformed durable Raft log entry"),
        };

    assert!(matches!(
        &error,
        Error::RecoveryFailedWithSource { context, .. }
            if context.contains("shared") || context.contains("Raft")
    ));
    let shared_error = StdError::source(&error)
        .and_then(|source| source.downcast_ref::<SharedStorageRecoveryError>())
        .expect("shared startup must retain the typed shared-recovery error");
    let raft_error = StdError::source(shared_error)
        .and_then(|source| source.downcast_ref::<RaftStorageRecoveryError>())
        .expect("shared recovery must retain the typed Raft recovery cause");
    assert!(matches!(
        raft_error,
        RaftStorageRecoveryError::InvalidLogEntry(_)
    ));
}

/// Realistic bug caught: command codec round trips and Raft WAL recovery can
/// each pass independently while their boundary drops transaction identity or
/// commit metadata from the recovered Raft entry.
#[test]
fn shared_recovery_retains_transaction_command_and_logical_phase_identity() {
    let data_dir = tempfile::tempdir().expect("temporary database directory must be created");
    let node_id = NodeId(29);
    let wal_dir = data_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).expect("test WAL directory must be created");

    let raft_identity = RaftReplicaIdentity::new(RaftGroupId(31), ReplicaId(7))
        .expect("Raft recovery identity must be valid");
    let tablet_id = TabletId(13);
    let transaction_id = TxnId(0x2a);
    let start_timestamp = Timestamp(101);
    let commit_timestamp = Timestamp(109);
    let key = vec![0x31, 0x32, 0x33];
    let pending_status = TxnStatusRecord {
        txn_id: transaction_id,
        start_timestamp,
        commit_timestamp: None,
        status: TxnStatus::Pending,
        primary_key: key.clone(),
        participant_tablet_ids: vec![tablet_id.0],
        last_heartbeat_timestamp: Some(start_timestamp),
        lease_deadline_ms: Some(10_000),
    };
    let prewrite_command = TabletCommand::Prewrite(PrewriteCommand {
        txn_id: transaction_id,
        start_timestamp,
        writes: vec![WriteEntry {
            key: key.clone(),
            row: Some(Row {
                values: vec![Value::Int(8), Value::Text("wal-recovered".to_string())],
            }),
            op: WriteKind::Put,
        }],
        primary_key: key.clone(),
        ttl_ms: 2_000,
        pending_status: Some(pending_status.clone()),
    });
    let mut committed_status = pending_status;
    committed_status.status = TxnStatus::Committed;
    committed_status.commit_timestamp = Some(commit_timestamp);
    let commit_command = TabletCommand::Commit(CommitCommand {
        txn_id: transaction_id,
        start_timestamp,
        commit_timestamp,
        keys: vec![key],
        committed_status: Some(committed_status),
    });
    let client_request_id = ClientRequestId {
        client_id: 0x29,
        session_epoch: 3,
        request_sequence: 17,
    };
    let prewrite_identity = LogicalCommandId {
        client_request_id,
        command_ordinal: 1,
        kind: CommandKind::Prewrite,
    };
    let commit_identity = LogicalCommandId {
        client_request_id,
        command_ordinal: 2,
        kind: CommandKind::Commit,
    };
    let prewrite_bytes = TabletCommandEnvelope::new_with_logical_command_id(
        RequestId {
            client_id: client_request_id.client_id,
            sequence: 1,
            raft_group_id: raft_identity.raft_group_id,
        },
        prewrite_identity,
        tablet_id,
        4,
        prewrite_command.clone(),
    )
    .expect("primary prewrite envelope must validate")
    .encode()
    .expect("primary prewrite envelope must encode");
    let commit_bytes = TabletCommandEnvelope::new_with_logical_command_id(
        RequestId {
            client_id: client_request_id.client_id,
            sequence: 2,
            raft_group_id: raft_identity.raft_group_id,
        },
        commit_identity,
        tablet_id,
        4,
        commit_command.clone(),
    )
    .expect("primary commit envelope must validate")
    .encode()
    .expect("primary commit envelope must encode");

    let wal_config = WalConfig {
        dir: wal_dir.clone(),
        identity: WalIdentity::new(node_id.0, 1, 1),
        ..WalConfig::default()
    };
    let (wal, _) = WalHandle::open(FsSegmentDirectory::new(wal_dir), wal_config, ())
        .expect("test WAL must open before transaction records are appended");
    for (index, envelope_bytes) in [(1, prewrite_bytes), (2, commit_bytes)] {
        let record = RaftLogEntryRecord {
            format_version: RAFT_LOG_ENTRY_RECORD_VERSION,
            identity: raft_identity,
            index,
            term: 4,
            payload: DurableRaftEntryPayload::Normal(envelope_bytes),
        }
        .encode()
        .expect("transaction Raft entry must encode");
        let _extent = wal
            .append_and_sync(RaftWalRecordType::LogEntry.as_wal_record_type(), &record)
            .expect("transaction Raft entry must become durable");
    }
    let hard_state = RaftHardStateRecord::from_core(
        raft_identity,
        raft::types::HardState {
            current_term: 4,
            voted_for: None,
            commit: 2,
        },
    )
    .expect("committed Raft HardState must validate")
    .encode()
    .expect("committed Raft HardState must encode");
    let _extent = wal
        .append_and_sync(
            RaftWalRecordType::HardState.as_wal_record_type(),
            &hard_state,
        )
        .expect("committed Raft HardState must become durable");
    drop(wal);

    let configurations = std::collections::BTreeMap::new();
    let (_, _, recovered_raft) =
        LocalDatabase::recover_shared_with_raft(data_dir.path(), node_id, &configurations)
            .expect("shared startup must recover both committed transaction entries");
    let recovered_replica = recovered_raft
        .replica(raft_identity)
        .expect("recovered Raft state must retain its exact replica identity");

    for (index, identity, expected_command) in [
        (1, prewrite_identity, prewrite_command),
        (2, commit_identity, commit_command),
    ] {
        let recovered_entry = recovered_replica
            .log_view()
            .entry(index)
            .expect("committed transaction entry must remain in the recovered log");
        assert_eq!(recovered_entry.record.identity, raft_identity);
        assert_eq!(recovered_entry.record.term, 4);
        let DurableRaftEntryPayload::Normal(command_bytes) = &recovered_entry.record.payload else {
            panic!("transaction entry must recover as a normal Raft command");
        };
        let envelope = TabletCommandEnvelope::decode(command_bytes)
            .expect("recovered transaction envelope must decode");
        assert_eq!(envelope.logical_command_id, Some(identity));
        assert_eq!(envelope.command, expected_command);
    }
}
