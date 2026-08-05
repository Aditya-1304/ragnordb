use std::collections::{BTreeMap, BTreeSet};

use raft::{
    entry::LogEntry,
    types::{ConfState, HardState, ReplicaId as CoreReplicaId},
};

use ragnordb_common::{
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    command_codec::{CatalogCommand, CatalogOperation, CreateTableOperation},
    ids::{ColumnId, RaftGroupId, ReplicaId, TableId, Timestamp},
};

use ragnordb_multiraft::storage::{
    codec::{RaftHardStateRecord, RaftLogEntryRecord, RaftReplicaIdentity},
    persistence::RaftWalRecordType,
    recovery::RaftWalRecoverySource,
    shared_recovery::recover_shared_storage_from_state,
};

use ragnordb_storage::{
    recovery::{RecoveryState, decode_recovery_record},
    wal::{CatalogUpdate, RagnorDbWalRecordType},
};

use wal::{error::WalError, lsn::Lsn, wal::iterator::WalRecord};

struct Source(std::vec::IntoIter<WalRecord>);

impl RaftWalRecoverySource for Source {
    fn next_record(&mut self) -> Result<Option<WalRecord>, WalError> {
        Ok(self.0.next())
    }
}

/// Catches checkpoint state being silently replaced with an empty database
/// state when the shared Raft/database WAL suffix is replayed after restart.
#[test]
fn checkpoint_state_is_preserved_while_the_shared_suffix_replays() {
    let identity = RaftReplicaIdentity::new(RaftGroupId(501), ReplicaId(601)).unwrap();

    let checkpoint_catalog = CatalogUpdate {
        table_id: TableId(9),
        update_timestamp: Timestamp(10),
        command: CatalogCommand {
            operation: CatalogOperation::CreateTable(CreateTableOperation {
                table_def: TableDefinition {
                    table_id: 9,
                    name: "users".to_string(),
                    columns: vec![ColumnDefinition {
                        column_id: ColumnId(1),
                        name: "id".to_string(),
                        ty: DataType::Int,
                        nullable: false,
                    }],
                    primary_key_column_ids: vec![ColumnId(1)],
                    schema_version: 1,
                    tablet_count: 1,
                },
            }),
        },
    };

    let checkpoint_payload = checkpoint_catalog.encode().unwrap();

    let checkpoint_record = decode_recovery_record(
        Lsn::new(10),
        RagnorDbWalRecordType::CatalogUpdate.as_wal_record_type(),
        &checkpoint_payload,
    )
    .unwrap()
    .unwrap();

    let mut checkpoint_state = RecoveryState::new();
    checkpoint_state.apply_record(&checkpoint_record).unwrap();

    let entry = RaftLogEntryRecord::from_core(
        identity,
        LogEntry::normal_with_size(1, 1, b"command".to_vec(), 7),
    )
    .unwrap();

    let hard_state = RaftHardStateRecord::from_core(
        identity,
        HardState {
            current_term: 1,
            voted_for: Some(CoreReplicaId::must(601)),
            commit: 1,
        },
    )
    .unwrap();

    let mut source = Source(
        vec![
            WalRecord {
                lsn: Lsn::new(20),
                record_type: RaftWalRecordType::LogEntry.as_wal_record_type(),
                payload: entry.encode().unwrap(),
                total_len: 1,
            },
            WalRecord {
                lsn: Lsn::new(30),
                record_type: RaftWalRecordType::HardState.as_wal_record_type(),
                payload: hard_state.encode().unwrap(),
                total_len: 1,
            },
        ]
        .into_iter(),
    );

    let configurations = BTreeMap::from([(
        identity,
        ConfState {
            version: 1,
            voters: BTreeSet::from([CoreReplicaId::must(601)]),
            learners: BTreeSet::new(),
            outgoing_voters: BTreeSet::new(),
        },
    )]);

    let recovered =
        recover_shared_storage_from_state(&mut source, checkpoint_state, &configurations).unwrap();

    assert_eq!(
        recovered.database.high_water_marks().max_table_id,
        TableId(9)
    );

    assert_eq!(
        recovered
            .raft
            .replica(identity)
            .unwrap()
            .hard_state()
            .unwrap()
            .commit,
        1
    );
}
