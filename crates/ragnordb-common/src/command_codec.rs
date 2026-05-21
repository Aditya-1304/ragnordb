use super::catalog_codec::TableDefinition as TableDef;
use super::codec::{Value, WriteKind};
use crate::ids::{Timestamp, TxnId};

use crate::proto::command;

#[derive(Debug, Clone, PartialEq)]
pub struct PrewriteCommand {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub key: Vec<u8>,
    pub value: Value,
    pub primary_key: Vec<u8>,
    pub op: WriteKind,
    pub ttl_ms: u64,
}

impl PrewriteCommand {
    pub fn to_proto(&self) -> command::PrewriteCommand {
        command::PrewriteCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            key: self.key.clone(),
            value: Some(self.value.to_proto()),
            primary_key: self.primary_key.clone(),
            op: self.op.to_proto() as i32,
            ttl_ms: self.ttl_ms,
        }
    }

    pub fn from_proto(proto: command::PrewriteCommand) -> Result<Self, &'static str> {
        Ok(PrewriteCommand {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            key: proto.key,
            value: Value::from_proto(proto.value.ok_or("missing value")?)?,
            primary_key: proto.primary_key,
            op: WriteKind::from_proto(
                crate::proto::mvcc::WriteKind::try_from(proto.op).map_err(|_| "invalid op")?,
            )?,
            ttl_ms: proto.ttl_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommitCommand {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub commit_timestamp: Timestamp,
    pub key: Vec<u8>,
}

impl CommitCommand {
    pub fn to_proto(&self) -> command::CommitCommand {
        command::CommitCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            commit_timestamp: Some(self.commit_timestamp.to_proto()),
            key: self.key.clone(),
        }
    }

    pub fn from_proto(proto: command::CommitCommand) -> Result<Self, &'static str> {
        Ok(CommitCommand {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            commit_timestamp: Timestamp::from_proto(
                proto.commit_timestamp.ok_or("missing commit_timestamp")?,
            ),
            key: proto.key,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RollbackCommand {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub key: Vec<u8>,
}

impl RollbackCommand {
    pub fn to_proto(&self) -> command::RollbackCommand {
        command::RollbackCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            key: self.key.clone(),
        }
    }

    pub fn from_proto(proto: command::RollbackCommand) -> Result<Self, &'static str> {
        Ok(RollbackCommand {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            key: proto.key,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriteEntry {
    pub key: Vec<u8>,
    pub value: Value,
    pub op: WriteKind,
}

impl WriteEntry {
    pub fn to_proto(&self) -> command::WriteEntry {
        command::WriteEntry {
            key: self.key.clone(),
            value: Some(self.value.to_proto()),
            op: self.op.to_proto() as i32,
        }
    }

    pub fn from_proto(proto: command::WriteEntry) -> Result<Self, &'static str> {
        Ok(WriteEntry {
            key: proto.key,
            value: Value::from_proto(proto.value.ok_or("missing value")?)?,
            op: WriteKind::from_proto(
                crate::proto::mvcc::WriteKind::try_from(proto.op).map_err(|_| "invalid op")?,
            )?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SingleShardCommitCommand {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub commit_timestamp: Timestamp,
    pub writes: Vec<WriteEntry>,
}
impl SingleShardCommitCommand {
    pub fn to_proto(&self) -> command::SingleShardCommitCommand {
        command::SingleShardCommitCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            commit_timestamp: Some(self.commit_timestamp.to_proto()),
            writes: self.writes.iter().map(|w| w.to_proto()).collect(),
        }
    }

    pub fn from_proto(proto: command::SingleShardCommitCommand) -> Result<Self, &'static str> {
        let writes = proto
            .writes
            .into_iter()
            .map(WriteEntry::from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SingleShardCommitCommand {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            commit_timestamp: Timestamp::from_proto(
                proto.commit_timestamp.ok_or("missing commit_timestamp")?,
            ),
            writes,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolveIntentCommand {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub key: Vec<u8>,
    pub resolved_status: crate::codec::TxnStatus,
    pub commit_timestamp: Timestamp,
}

impl ResolveIntentCommand {
    pub fn to_proto(&self) -> command::ResolveIntentCommand {
        command::ResolveIntentCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            key: self.key.clone(),
            resolved_status: self.resolved_status.to_proto() as i32,
            commit_timestamp: Some(self.commit_timestamp.to_proto()),
        }
    }

    pub fn from_proto(proto: command::ResolveIntentCommand) -> Result<Self, &'static str> {
        Ok(ResolveIntentCommand {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            key: proto.key,
            resolved_status: crate::codec::TxnStatus::from_proto(
                crate::proto::mvcc::TxnStatus::try_from(proto.resolved_status)
                    .map_err(|_| "invalid status")?,
            )?,
            commit_timestamp: Timestamp::from_proto(
                proto.commit_timestamp.ok_or("missing commit_timestamp")?,
            ),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableOperation {
    pub table_def: TableDef,
}

impl CreateTableOperation {
    pub fn to_proto(&self) -> command::CreateTableOperation {
        command::CreateTableOperation {
            table_definition: Some(self.table_def.to_proto()),
        }
    }

    pub fn from_proto(proto: command::CreateTableOperation) -> Result<Self, &'static str> {
        Ok(CreateTableOperation {
            table_def: TableDef::from_proto(
                proto.table_definition.ok_or("missing table_definition")?,
            )?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogCommand {
    pub operation: CatalogOperation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CatalogOperation {
    CreateTable(CreateTableOperation),
}

impl CatalogCommand {
    pub fn to_proto(&self) -> command::CatalogCommand {
        let operation = match &self.operation {
            CatalogOperation::CreateTable(op) => Some(
                command::catalog_command::Operation::CreateTable(op.to_proto()),
            ),
        };

        command::CatalogCommand { operation }
    }

    pub fn from_proto(proto: command::CatalogCommand) -> Result<Self, &'static str> {
        let operation = match proto.operation {
            Some(command::catalog_command::Operation::CreateTable(op)) => {
                CatalogOperation::CreateTable(CreateTableOperation::from_proto(op)?)
            }
            None => return Err("missing catalog operation"),
        };

        Ok(CatalogCommand { operation })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NoopCommand;

impl NoopCommand {
    pub fn to_proto(&self) -> command::NoopCommand {
        command::NoopCommand {}
    }

    pub fn from_proto(_proto: command::NoopCommand) -> Result<Self, &'static str> {
        Ok(NoopCommand)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TabletCommand {
    Prewrite(PrewriteCommand),
    Commit(CommitCommand),
    Rollback(RollbackCommand),
    SingleShardCommit(SingleShardCommitCommand),
    ResolveIntent(ResolveIntentCommand),
    Catalog(CatalogCommand),
    Noop(NoopCommand),
}

impl TabletCommand {
    pub fn to_proto(&self) -> command::TabletCommand {
        let command = match self {
            TabletCommand::Prewrite(c) => {
                Some(command::tablet_command::Command::Prewrite(c.to_proto()))
            }
            TabletCommand::Commit(c) => {
                Some(command::tablet_command::Command::Commit(c.to_proto()))
            }
            TabletCommand::Rollback(c) => {
                Some(command::tablet_command::Command::Rollback(c.to_proto()))
            }
            TabletCommand::SingleShardCommit(c) => Some(
                command::tablet_command::Command::SingleShardCommit(c.to_proto()),
            ),
            TabletCommand::ResolveIntent(c) => Some(
                command::tablet_command::Command::ResolveIntent(c.to_proto()),
            ),
            TabletCommand::Catalog(c) => Some(command::tablet_command::Command::CatalogUpdate(
                c.to_proto(),
            )),
            TabletCommand::Noop(c) => Some(command::tablet_command::Command::Noop(c.to_proto())),
        };
        command::TabletCommand { command }
    }

    pub fn from_proto(proto: command::TabletCommand) -> Result<Self, &'static str> {
        match proto.command {
            Some(command::tablet_command::Command::Prewrite(c)) => {
                Ok(TabletCommand::Prewrite(PrewriteCommand::from_proto(c)?))
            }
            Some(command::tablet_command::Command::Commit(c)) => {
                Ok(TabletCommand::Commit(CommitCommand::from_proto(c)?))
            }
            Some(command::tablet_command::Command::Rollback(c)) => {
                Ok(TabletCommand::Rollback(RollbackCommand::from_proto(c)?))
            }
            Some(command::tablet_command::Command::SingleShardCommit(c)) => Ok(
                TabletCommand::SingleShardCommit(SingleShardCommitCommand::from_proto(c)?),
            ),
            Some(command::tablet_command::Command::ResolveIntent(c)) => Ok(
                TabletCommand::ResolveIntent(ResolveIntentCommand::from_proto(c)?),
            ),
            Some(command::tablet_command::Command::CatalogUpdate(c)) => {
                Ok(TabletCommand::Catalog(CatalogCommand::from_proto(c)?))
            }
            Some(command::tablet_command::Command::Noop(c)) => {
                Ok(TabletCommand::Noop(NoopCommand::from_proto(c)?))
            }
            None => Err("missing tablet command"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::catalog_codec::{ColumnDefinition, DataType};
    use super::*;
    use crate::codec::{TxnStatus, WriteKind};

    #[test]
    fn prewrite_command_roundtrip() {
        let cmd = PrewriteCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            key: b"/table/1/pk/1".to_vec(),
            value: Value::Text("Ada".to_string()),
            primary_key: b"/table/1/pk/1".to_vec(),
            op: WriteKind::Put,
            ttl_ms: 30_000,
        };
        let proto = cmd.to_proto();
        let decoded = PrewriteCommand::from_proto(proto).unwrap();
        assert_eq!(decoded.txn_id.0, 1);
        assert_eq!(decoded.start_timestamp.0, 100);
        assert!(matches!(decoded.value, Value::Text(ref s) if s == "Ada"));
        assert!(matches!(decoded.op, WriteKind::Put));
    }

    #[test]
    fn commit_command_roundtrip() {
        let cmd = CommitCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            commit_timestamp: Timestamp(105),
            key: b"/table/1/pk/1".to_vec(),
        };
        let proto = cmd.to_proto();
        let decoded = CommitCommand::from_proto(proto).unwrap();
        assert_eq!(decoded.commit_timestamp.0, 105);
    }

    #[test]
    fn rollback_command_roundtrip() {
        let cmd = RollbackCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            key: b"/table/1/pk/1".to_vec(),
        };
        let proto = cmd.to_proto();
        let decoded = RollbackCommand::from_proto(proto).unwrap();
        assert_eq!(decoded.txn_id.0, 1);
    }

    #[test]
    fn write_entry_roundtrip() {
        let entry = WriteEntry {
            key: b"/table/1/pk/1".to_vec(),
            value: Value::Int(42),
            op: WriteKind::Put,
        };
        let proto = entry.to_proto();
        let decoded = WriteEntry::from_proto(proto).unwrap();
        assert!(matches!(decoded.value, Value::Int(42)));
    }

    #[test]
    fn single_shard_commit_roundtrip() {
        let cmd = SingleShardCommitCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            commit_timestamp: Timestamp(110),
            writes: vec![
                WriteEntry {
                    key: b"/table/1/pk/1".to_vec(),
                    value: Value::Text("Ada".to_string()),
                    op: WriteKind::Put,
                },
                WriteEntry {
                    key: b"/table/1/pk/2".to_vec(),
                    value: Value::Text("Bob".to_string()),
                    op: WriteKind::Put,
                },
            ],
        };
        let proto = cmd.to_proto();
        let decoded = SingleShardCommitCommand::from_proto(proto).unwrap();
        assert_eq!(decoded.writes.len(), 2);
    }

    #[test]
    fn resolve_intent_roundtrip() {
        let cmd = ResolveIntentCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            key: b"/table/1/pk/1".to_vec(),
            resolved_status: TxnStatus::Committed,
            commit_timestamp: Timestamp(105),
        };
        let proto = cmd.to_proto();
        let decoded = ResolveIntentCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded.resolved_status, TxnStatus::Committed));
    }

    #[test]
    fn create_table_operation_roundtrip() {
        let op = CreateTableOperation {
            table_def: TableDef {
                table_id: 100,
                name: "users".to_string(),
                columns: vec![ColumnDefinition {
                    column_id: 1,
                    name: "id".to_string(),
                    ty: DataType::Int,
                    nullable: false,
                }],
                primary_key_column_ids: vec![1],
                schema_version: 1,
                tablet_count: 4,
            },
        };
        let proto = op.to_proto();
        let decoded = CreateTableOperation::from_proto(proto).unwrap();
        assert_eq!(decoded.table_def.table_id, 100);
        assert_eq!(decoded.table_def.columns.len(), 1);
    }

    #[test]
    fn catalog_command_roundtrip() {
        let cmd = CatalogCommand {
            operation: CatalogOperation::CreateTable(CreateTableOperation {
                table_def: TableDef {
                    table_id: 200,
                    name: "orders".to_string(),
                    columns: vec![],
                    primary_key_column_ids: vec![1],
                    schema_version: 1,
                    tablet_count: 2,
                },
            }),
        };
        let proto = cmd.to_proto();
        let decoded = CatalogCommand::from_proto(proto).unwrap();
        assert!(matches!(
            decoded.operation,
            CatalogOperation::CreateTable(_)
        ));
    }

    #[test]
    fn noop_command_roundtrip() {
        let cmd = NoopCommand;
        let proto = cmd.to_proto();
        let decoded = NoopCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded, NoopCommand));
    }

    #[test]
    fn tablet_command_prewrite_roundtrip() {
        let cmd = TabletCommand::Prewrite(PrewriteCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            key: b"/table/1/pk/1".to_vec(),
            value: Value::Int(1),
            primary_key: b"/table/1/pk/1".to_vec(),
            op: WriteKind::Put,
            ttl_ms: 30_000,
        });
        let proto = cmd.to_proto();
        let decoded = TabletCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded, TabletCommand::Prewrite(_)));
    }

    #[test]
    fn tablet_command_commit_roundtrip() {
        let cmd = TabletCommand::Commit(CommitCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            commit_timestamp: Timestamp(105),
            key: b"/table/1/pk/1".to_vec(),
        });
        let proto = cmd.to_proto();
        let decoded = TabletCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded, TabletCommand::Commit(_)));
    }

    #[test]
    fn tablet_command_rollback_roundtrip() {
        let cmd = TabletCommand::Rollback(RollbackCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            key: b"/table/1/pk/1".to_vec(),
        });
        let proto = cmd.to_proto();
        let decoded = TabletCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded, TabletCommand::Rollback(_)));
    }

    #[test]
    fn tablet_command_noop_roundtrip() {
        let cmd = TabletCommand::Noop(NoopCommand);
        let proto = cmd.to_proto();
        let decoded = TabletCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded, TabletCommand::Noop(_)));
    }

    #[test]
    fn tablet_command_missing_rejected() {
        let proto = command::TabletCommand { command: None };
        assert!(TabletCommand::from_proto(proto).is_err());
    }
}
