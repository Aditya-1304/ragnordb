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
    shared_recovery::recover_shared_storage,
};
use ragnordb_storage::wal::{CatalogUpdate, RagnorDbWalRecordType};
use wal::{error::WalError, lsn::Lsn, wal::iterator::WalRecord};

struct Source(std::vec::IntoIter<WalRecord>);

impl RaftWalRecoverySource for Source {
    fn next_record(&mut self) -> Result<Option<WalRecord>, WalError> {
        Ok(self.0.next())
    }
}

/// Realistic bug caught: database recovery rejects valid Raft IDs or startup
/// runs independent full-WAL scans whose routing rules drift apart.
#[test]
fn one_physical_pass_dispatches_interleaved_database_and_raft_records() {
    let identity = RaftReplicaIdentity::new(RaftGroupId(501), ReplicaId(601)).unwrap();
    let catalog = CatalogUpdate {
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
                lsn: Lsn::new(10),
                record_type: RagnorDbWalRecordType::CatalogUpdate.as_wal_record_type(),
                payload: catalog.encode().unwrap(),
                total_len: 1,
            },
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

    let recovered = recover_shared_storage(&mut source, &configurations).unwrap();
    assert_eq!(
        recovered.database.high_water_marks().max_table_id,
        TableId(9)
    );
    assert_eq!(recovered.raft.scanned_records(), 3);
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
