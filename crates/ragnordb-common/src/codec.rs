use crate::ids::{Timestamp, TxnId};
use crate::proto::{mvcc, row};

pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Null,
}

impl Value {
    pub fn to_proto(&self) -> row::Value {
        let kind = match self {
            Value::Int(v) => row::value::Kind::IntValue(*v),
            Value::Text(v) => row::value::Kind::TextValue(v.clone()),
            Value::Bool(v) => row::value::Kind::BoolValue(*v),
            Value::Null => row::value::Kind::NullValue(row::NullValue::Null as i32),
        };

        row::Value { kind: Some(kind) }
    }

    pub fn from_proto(proto: row::Value) -> Result<Self, &'static str> {
        match proto.kind {
            Some(row::value::Kind::IntValue(v)) => Ok(Value::Int(v)),
            Some(row::value::Kind::TextValue(v)) => Ok(Value::Text(v)),
            Some(row::value::Kind::BoolValue(v)) => Ok(Value::Bool(v)),
            Some(row::value::Kind::NullValue(_)) => Ok(Value::Null),
            None => Err("missing value kind"),
        }
    }
}

pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn to_proto(&self) -> row::Row {
        row::Row {
            values: self.values.iter().map(|v| v.to_proto()).collect(),
        }
    }

    pub fn from_proto(proto: row::Row) -> Result<Self, &'static str> {
        let values = proto
            .values
            .into_iter()
            .map(Value::from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Row { values })
    }
}

pub struct LockRecord {
    pub txn_id: TxnId,
    pub primary_key: Vec<u8>,
    pub start_timestamp: Timestamp,
    pub ttl_ms: u64,
    pub op: WriteKind,
}

pub struct WriteRecord {
    pub start_timestamp: Timestamp,
    pub commit_timestamp: Timestamp,
    pub op: WriteKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteKind {
    Put,
    Delete,
    Rollback,
}

impl WriteKind {
    pub fn to_proto(&self) -> mvcc::WriteKind {
        match self {
            WriteKind::Put => mvcc::WriteKind::Put,
            WriteKind::Delete => mvcc::WriteKind::Delete,
            WriteKind::Rollback => mvcc::WriteKind::Rollback,
        }
    }

    pub fn from_proto(proto: mvcc::WriteKind) -> Result<Self, &'static str> {
        match proto {
            mvcc::WriteKind::Put => Ok(WriteKind::Put),
            mvcc::WriteKind::Delete => Ok(WriteKind::Delete),
            mvcc::WriteKind::Rollback => Ok(WriteKind::Rollback),
            mvcc::WriteKind::Unspecified => Err("unspecified write kind"),
        }
    }
}

impl LockRecord {
    pub fn to_proto(&self) -> mvcc::LockRecord {
        mvcc::LockRecord {
            txn_id: Some(self.txn_id.to_proto()),
            primary_key: self.primary_key.clone(),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            ttl_ms: self.ttl_ms,
            op: self.op.to_proto() as i32,
        }
    }

    pub fn from_proto(proto: mvcc::LockRecord) -> Result<Self, &'static str> {
        Ok(LockRecord {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            primary_key: proto.primary_key,
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_ts")?,
            ),
            ttl_ms: proto.ttl_ms,
            op: WriteKind::from_proto(mvcc::WriteKind::try_from(proto.op).map_err(|_| "inv")?)?,
        })
    }
}

impl WriteRecord {
    pub fn to_proto(&self) -> mvcc::WriteRecord {
        mvcc::WriteRecord {
            start_timestamp: Some(self.start_timestamp.to_proto()),
            commit_timestamp: Some(self.commit_timestamp.to_proto()),
            op: self.op.to_proto() as i32,
        }
    }

    pub fn from_proto(proto: mvcc::WriteRecord) -> Result<Self, &'static str> {
        Ok(WriteRecord {
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_ts")?,
            ),
            commit_timestamp: Timestamp::from_proto(
                proto.commit_timestamp.ok_or("missing commit_ts")?,
            ),
            op: WriteKind::from_proto(mvcc::WriteKind::try_from(proto.op).map_err(|_| "inv")?)?,
        })
    }
}

pub struct TxnStatusRecord {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub commit_timestamp: Option<Timestamp>,
    pub status: TxnStatus,
    pub primary_key: Vec<u8>,
    pub participant_tablet_ids: Vec<u64>,
    pub last_heartbeat_timestamp: Option<Timestamp>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Pending,
    Committed,
    Aborted,
}

impl TxnStatus {
    pub fn to_proto(&self) -> mvcc::TxnStatus {
        match self {
            TxnStatus::Pending => mvcc::TxnStatus::Pending,
            TxnStatus::Committed => mvcc::TxnStatus::Committed,
            TxnStatus::Aborted => mvcc::TxnStatus::Aborted,
        }
    }

    pub fn from_proto(proto: mvcc::TxnStatus) -> Result<Self, &'static str> {
        match proto {
            mvcc::TxnStatus::Pending => Ok(TxnStatus::Pending),
            mvcc::TxnStatus::Committed => Ok(TxnStatus::Committed),
            mvcc::TxnStatus::Aborted => Ok(TxnStatus::Aborted),
            mvcc::TxnStatus::Unspecified => Err("unspecified txn status"),
        }
    }
}

impl TxnStatusRecord {
    pub fn to_proto(&self) -> mvcc::TxnStatusRecord {
        mvcc::TxnStatusRecord {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            commit_timestamp: self.commit_timestamp.map(|ts| ts.to_proto()),
            status: self.status.to_proto() as i32,
            primary_key: self.primary_key.clone(),
            participant_tablet_ids: self.participant_tablet_ids.clone(),
            last_heartbeat_timestamp: self.last_heartbeat_timestamp.map(|ts| ts.to_proto()),
        }
    }

    pub fn from_proto(proto: mvcc::TxnStatusRecord) -> Result<Self, &'static str> {
        Ok(TxnStatusRecord {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_ts")?,
            ),
            commit_timestamp: proto.commit_timestamp.map(Timestamp::from_proto),
            status: TxnStatus::from_proto(
                mvcc::TxnStatus::try_from(proto.status).map_err(|_| "inv")?,
            )?,
            primary_key: proto.primary_key,
            participant_tablet_ids: proto.participant_tablet_ids,
            last_heartbeat_timestamp: proto.last_heartbeat_timestamp.map(Timestamp::from_proto),
        })
    }
}
