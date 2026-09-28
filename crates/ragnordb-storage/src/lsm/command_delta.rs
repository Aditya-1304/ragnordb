//! Complete tablet command publication contract.
//!
//! The MVCC edits remain the transaction engine's responsibility. This
//! aggregate adds the replicated transaction decision, retry state, and exact
//! Raft boundary so a tablet backend can validate and publish one transition.

use std::collections::BTreeMap;

use ragnordb_common::{
    Error, Result,
    codec::TxnStatusRecord,
    command_codec::CachedTabletCommandOutcome,
    encoding::decode_row,
    ids::{LogicalCommandId, RaftGroupId, ReplicaId, TableId, TabletId},
};

use crate::{
    key::decode_row_key,
    lsm::RecoveryFrontier,
    mvcc::{MvccDelta, MvccRecordEdit},
};

/// Maximum V1 encoded command-delta payload accepted by tablet storage.
pub const MAX_COMMAND_DELTA_BYTES: usize = 64 * 1024 * 1024;

/// Binds one tablet storage instance to a replica lifetime and its table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TabletStorageIdentity {
    pub tablet_id: TabletId,
    pub table_id: TableId,
    pub raft_group_id: RaftGroupId,
    pub replica_id: ReplicaId,
}

/// A transaction-status replacement published with the row edits it governs.
#[derive(Debug, Clone, PartialEq)]
pub enum TxnStatusEdit {
    Put {
        txn_id: ragnordb_common::ids::TxnId,
        status: TxnStatusRecord,
    },
}

/// A route-independent logical command retry outcome edit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalOutcomeEdit {
    Put {
        id: LogicalCommandId,
        outcome: CachedTabletCommandOutcome,
    },
    Delete {
        id: LogicalCommandId,
    },
}

/// Legacy request-sequence state retained for compatibility callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyOutcomeEdit {
    Put {
        client_id: u128,
        last_sequence_applied: u64,
        outcome: CachedTabletCommandOutcome,
    },
}

/// Monotonic client-session retry-retirement edit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryFloorEdit {
    Advance {
        client_id: u128,
        session_epoch: u64,
        acknowledged_through: u64,
    },
}

/// Complete logical state transition for one committed Raft position.
///
/// Backends must validate the whole value before changing any visible record.
/// `frontier` covers every edit in this aggregate, including cached rejection
/// outcomes and retry-floor advancement.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CommandDelta {
    pub mvcc: MvccDelta,
    pub transaction_status_edits: Vec<TxnStatusEdit>,
    pub logical_outcome_edits: Vec<LogicalOutcomeEdit>,
    pub legacy_outcome_edits: Vec<LegacyOutcomeEdit>,
    pub retry_floor_edits: Vec<RetryFloorEdit>,
    pub frontier: Option<RecoveryFrontier>,
}

impl CommandDelta {
    /// Build a metadata-only transition, commonly used by a deterministic
    /// rejection or a committed no-op that still advances applied progress.
    pub fn frontier_only(frontier: RecoveryFrontier) -> Self {
        Self {
            frontier: Some(frontier),
            ..Self::default()
        }
    }

    /// Validate identities, uniqueness, monotonic state, record encodings, and
    /// the bounded encoded-size contract before a backend mutates state.
    pub fn validate(
        &self,
        identity: TabletStorageIdentity,
        previous_frontier: Option<RecoveryFrontier>,
        current_retry_floors: &BTreeMap<(u128, u64), u64>,
    ) -> Result<()> {
        validate_identity(identity)?;
        let frontier = self.frontier.ok_or_else(|| {
            Error::InvalidArgument("command delta has no Raft frontier".to_string())
        })?;
        validate_frontier(frontier, identity, previous_frontier)?;

        let mut records = std::collections::BTreeSet::new();
        for edit in &self.mvcc.edits {
            validate_mvcc_edit(edit, identity, &mut records)?;
        }

        let mut transaction_ids = std::collections::BTreeSet::new();
        for edit in &self.transaction_status_edits {
            match edit {
                TxnStatusEdit::Put { txn_id, status } => {
                    if !transaction_ids.insert(*txn_id) {
                        return Err(invalid(
                            "command delta repeats a transaction-status identity",
                        ));
                    }
                    if status.txn_id != *txn_id {
                        return Err(invalid(
                            "transaction-status edit key differs from its record",
                        ));
                    }
                    status.validate().map_err(|reason| {
                        invalid(format!("invalid transaction status: {reason}"))
                    })?;
                    let primary_tablet = status.primary_tablet_id().map_err(|reason| {
                        invalid(format!("invalid primary authority: {reason}"))
                    })?;
                    if primary_tablet != identity.tablet_id {
                        return Err(invalid("transaction status belongs to another tablet"));
                    }
                    validate_owned_row_key(&status.primary_key, identity.table_id)?;
                }
            }
        }

        let mut logical_ids = std::collections::BTreeSet::new();
        for edit in &self.logical_outcome_edits {
            let id = match edit {
                LogicalOutcomeEdit::Put { id, outcome } => {
                    id.validate().map_err(|reason| {
                        invalid(format!("invalid logical command ID: {reason}"))
                    })?;
                    validate_outcome(outcome)?;
                    id
                }
                LogicalOutcomeEdit::Delete { id } => {
                    id.validate().map_err(|reason| {
                        invalid(format!("invalid logical command ID: {reason}"))
                    })?;
                    id
                }
            };
            if !logical_ids.insert(*id) {
                return Err(invalid(
                    "command delta repeats a logical retry-outcome identity",
                ));
            }
        }

        let mut legacy_clients = std::collections::BTreeSet::new();
        for edit in &self.legacy_outcome_edits {
            match edit {
                LegacyOutcomeEdit::Put {
                    client_id,
                    last_sequence_applied,
                    outcome,
                } => {
                    if *client_id == 0 || *last_sequence_applied == 0 {
                        return Err(invalid("legacy outcome identity must be non-zero"));
                    }
                    validate_outcome(outcome)?;
                    if !legacy_clients.insert(*client_id) {
                        return Err(invalid("command delta repeats a legacy outcome identity"));
                    }
                }
            }
        }

        let mut retry_sessions = std::collections::BTreeSet::new();
        for edit in &self.retry_floor_edits {
            match edit {
                RetryFloorEdit::Advance {
                    client_id,
                    session_epoch,
                    acknowledged_through,
                } => {
                    if *client_id == 0 || *session_epoch == 0 || *acknowledged_through == 0 {
                        return Err(invalid("retry-floor identity and value must be non-zero"));
                    }
                    let key = (*client_id, *session_epoch);
                    if !retry_sessions.insert(key) {
                        return Err(invalid("command delta repeats a retry-floor identity"));
                    }
                    if current_retry_floors
                        .get(&key)
                        .is_some_and(|current| acknowledged_through < current)
                    {
                        return Err(invalid("retry floor cannot regress"));
                    }
                }
            }
        }

        let estimated = self.encoded_size_upper_bound()?;
        if estimated > MAX_COMMAND_DELTA_BYTES {
            return Err(invalid(format!(
                "command delta exceeds the {MAX_COMMAND_DELTA_BYTES}-byte V1 limit"
            )));
        }
        Ok(())
    }

    /// Return a checked, conservative byte estimate for the encoded records
    /// plus one sparse staging copy of their variable key/value payloads.
    pub fn encoded_size_upper_bound(&self) -> Result<usize> {
        let mut size = 64usize;
        for edit in &self.mvcc.edits {
            let (key, value_bytes) = match edit {
                MvccRecordEdit::PutDefault { key, row, .. } => (key.len(), row.len()),
                MvccRecordEdit::DeleteDefault { key, .. } | MvccRecordEdit::DeleteLock { key } => {
                    (key.len(), 0)
                }
                MvccRecordEdit::PutLock { key, lock } => {
                    let value_bytes = lock
                        .primary_key
                        .len()
                        .checked_add(32)
                        .ok_or_else(|| invalid("lock value size overflowed"))?;
                    (key.len(), value_bytes)
                }
                MvccRecordEdit::PutWrite { key, .. } => (key.len(), 24),
            };
            size = add_record_size(size, key, value_bytes)?;
        }
        for edit in &self.transaction_status_edits {
            match edit {
                TxnStatusEdit::Put { status, .. } => {
                    let participants = status
                        .participant_tablet_ids
                        .len()
                        .checked_mul(8)
                        .ok_or_else(|| invalid("participant byte count overflowed"))?;
                    let value_bytes = status
                        .primary_key
                        .len()
                        .checked_add(participants)
                        .and_then(|bytes| bytes.checked_add(64))
                        .ok_or_else(|| invalid("transaction status byte count overflowed"))?;
                    size = add_record_size(size, 8, value_bytes)?;
                }
            }
        }
        for edit in &self.logical_outcome_edits {
            let outcome_bytes = match edit {
                LogicalOutcomeEdit::Put { outcome, .. } => outcome_size(outcome)?,
                LogicalOutcomeEdit::Delete { .. } => 0,
            };
            size = add_record_size(size, 64, outcome_bytes)?;
        }
        for edit in &self.legacy_outcome_edits {
            let outcome_bytes = match edit {
                LegacyOutcomeEdit::Put { outcome, .. } => outcome_size(outcome)?,
            };
            size = add_record_size(size, 16, outcome_bytes)?;
        }
        for _edit in &self.retry_floor_edits {
            size = add_record_size(size, 32, 32)?;
        }
        Ok(size)
    }
}

fn validate_identity(identity: TabletStorageIdentity) -> Result<()> {
    if identity.tablet_id.0 == 0
        || identity.table_id.0 == 0
        || identity.raft_group_id.0 == 0
        || identity.replica_id.0 == 0
    {
        return Err(invalid(
            "tablet storage identity contains a reserved zero ID",
        ));
    }
    Ok(())
}

fn validate_frontier(
    frontier: RecoveryFrontier,
    identity: TabletStorageIdentity,
    previous: Option<RecoveryFrontier>,
) -> Result<()> {
    frontier
        .validate()
        .map_err(|error| invalid(format!("invalid recovery frontier: {error}")))?;
    match frontier {
        RecoveryFrontier::ReplicatedTablet {
            raft_group_id,
            replica_id,
            ..
        } if raft_group_id == identity.raft_group_id && replica_id == identity.replica_id => {}
        RecoveryFrontier::ReplicatedTablet { .. } => {
            return Err(invalid(
                "command frontier belongs to another replica lineage",
            ));
        }
        RecoveryFrontier::SingleNode { .. } => {
            return Err(invalid(
                "tablet command delta requires a replicated Raft frontier",
            ));
        }
    }
    if let Some(previous) = previous {
        frontier
            .validate_successor_of(&previous)
            .map_err(|error| invalid(format!("command frontier regressed: {error}")))?;
        if let (
            RecoveryFrontier::ReplicatedTablet {
                applied_index: previous_index,
                ..
            },
            RecoveryFrontier::ReplicatedTablet {
                applied_index: current_index,
                ..
            },
        ) = (previous, frontier)
        {
            let expected = previous_index
                .checked_add(1)
                .ok_or_else(|| invalid("previous Raft applied index is exhausted"))?;
            if current_index != expected {
                return Err(invalid(format!(
                    "command frontier must advance the processed prefix from {previous_index} to {expected}, received {current_index}"
                )));
            }
        }
    } else if let RecoveryFrontier::ReplicatedTablet { applied_index, .. } = frontier
        && applied_index != 1
    {
        return Err(invalid(format!(
            "initial command frontier must begin at Raft index 1, received {applied_index}"
        )));
    }
    Ok(())
}

fn validate_mvcc_edit(
    edit: &MvccRecordEdit,
    identity: TabletStorageIdentity,
    changed_records: &mut std::collections::BTreeSet<(u8, Vec<u8>, u64)>,
) -> Result<()> {
    let (family, key, timestamp) = match edit {
        MvccRecordEdit::PutDefault { key, start_ts, row } => {
            validate_owned_row_key(key, identity.table_id)?;
            decode_row(row)
                .map_err(|error| invalid(format!("default payload is malformed: {error}")))?;
            (0, key, start_ts.0)
        }
        MvccRecordEdit::DeleteDefault { key, start_ts } => {
            validate_owned_row_key(key, identity.table_id)?;
            (0, key, start_ts.0)
        }
        MvccRecordEdit::PutLock { key, lock } => {
            validate_owned_row_key(key, identity.table_id)?;
            lock.validate()
                .map_err(|reason| invalid(format!("lock edit is malformed: {reason}")))?;
            // A secondary participant's lock points to the transaction's
            // primary row, which may be owned by another table and tablet.
            // Validate that reference as a canonical row key without applying
            // this storage family's table-ownership check to it.
            validate_row_key_encoding(&lock.primary_key)?;
            (1, key, 0)
        }
        MvccRecordEdit::DeleteLock { key } => {
            validate_owned_row_key(key, identity.table_id)?;
            (1, key, 0)
        }
        MvccRecordEdit::PutWrite {
            key,
            write_ts,
            write,
        } => {
            validate_owned_row_key(key, identity.table_id)?;
            write
                .validate()
                .map_err(|reason| invalid(format!("write edit is malformed: {reason}")))?;
            if *write_ts != write.commit_timestamp {
                return Err(invalid(
                    "write family key timestamp differs from its record",
                ));
            }
            (2, key, write_ts.0)
        }
    };
    if !changed_records.insert((family, key.clone(), timestamp)) {
        return Err(invalid("command delta repeats an MVCC record identity"));
    }
    Ok(())
}

fn validate_owned_row_key(key: &[u8], table_id: TableId) -> Result<()> {
    let decoded = decode_row_key(key).map_err(|error| {
        invalid(format!(
            "command delta contains a malformed row key: {error}"
        ))
    })?;
    if decoded.table_id != table_id {
        return Err(invalid("command delta row key belongs to another table"));
    }
    Ok(())
}

fn validate_row_key_encoding(key: &[u8]) -> Result<()> {
    decode_row_key(key).map(|_| ()).map_err(|error| {
        invalid(format!(
            "command delta contains a malformed row key: {error}"
        ))
    })
}

fn validate_outcome(outcome: &CachedTabletCommandOutcome) -> Result<()> {
    if let CachedTabletCommandOutcome::Rejected(rejection) = outcome
        && rejection.reason.len() > 4096
    {
        return Err(invalid("cached rejection diagnostic exceeds the V1 limit"));
    }
    Ok(())
}

fn outcome_size(outcome: &CachedTabletCommandOutcome) -> Result<usize> {
    match outcome {
        CachedTabletCommandOutcome::Applied(_) => Ok(16),
        CachedTabletCommandOutcome::Rejected(rejection) => 16usize
            .checked_add(rejection.reason.len())
            .ok_or_else(|| invalid("cached outcome byte count overflowed")),
    }
}

fn add_record_size(current: usize, key: usize, value: usize) -> Result<usize> {
    let payload = key
        .checked_add(value)
        .ok_or_else(|| invalid("command delta record size overflowed"))?;
    // Account for the primary encoding and one sparse prepared copy, plus
    // fixed family/key framing and per-record index overhead.
    let charged = payload
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(128))
        .ok_or_else(|| invalid("command delta staging size overflowed"))?;
    current
        .checked_add(charged)
        .ok_or_else(|| invalid("command delta total size overflowed"))
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidArgument(message.into())
}

#[cfg(test)]
mod tests {
    use super::{CommandDelta, TabletStorageIdentity};
    use crate::lsm::RecoveryFrontier;
    use crate::{
        key::{encode_row_key, make_row_key},
        mvcc::{MvccDelta, MvccRecordEdit},
    };
    use ragnordb_common::{
        codec::{LockRecord, Value, WriteKind},
        ids::{RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
    };

    #[test]
    fn secondary_table_lock_may_reference_primary_row_in_another_table() {
        let identity = TabletStorageIdentity {
            tablet_id: TabletId(5),
            table_id: TableId(8),
            raft_group_id: RaftGroupId(13),
            replica_id: ReplicaId(21),
        };
        let locked_row =
            encode_row_key(&make_row_key(TableId(8), &[Value::Int(2)]).unwrap()).unwrap();
        let primary_row =
            encode_row_key(&make_row_key(TableId(9), &[Value::Int(1)]).unwrap()).unwrap();
        let delta = CommandDelta {
            mvcc: MvccDelta {
                edits: vec![MvccRecordEdit::PutLock {
                    key: locked_row,
                    lock: LockRecord {
                        txn_id: TxnId(7),
                        primary_key: primary_row,
                        start_timestamp: Timestamp(11),
                        ttl_ms: 30_000,
                        op: WriteKind::Put,
                    },
                }],
            },
            frontier: Some(RecoveryFrontier::ReplicatedTablet {
                raft_group_id: identity.raft_group_id,
                replica_id: identity.replica_id,
                applied_index: 1,
                applied_term: 1,
            }),
            ..CommandDelta::default()
        };

        assert!(delta.validate(identity, None, &Default::default()).is_ok());
    }

    #[test]
    fn command_delta_rejects_frontier_from_another_replica_lifetime() {
        let identity = TabletStorageIdentity {
            tablet_id: TabletId(5),
            table_id: TableId(8),
            raft_group_id: RaftGroupId(13),
            replica_id: ReplicaId(21),
        };
        let delta = CommandDelta::frontier_only(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(13),
            replica_id: ReplicaId(22),
            applied_index: 4,
            applied_term: 2,
        });

        assert!(delta.validate(identity, None, &Default::default()).is_err());
    }

    #[test]
    fn command_delta_rejects_same_index_even_when_term_matches() {
        let identity = TabletStorageIdentity {
            tablet_id: TabletId(5),
            table_id: TableId(8),
            raft_group_id: RaftGroupId(13),
            replica_id: ReplicaId(21),
        };
        let previous = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(13),
            replica_id: ReplicaId(21),
            applied_index: 4,
            applied_term: 2,
        };
        let delta = CommandDelta::frontier_only(previous);

        assert!(
            delta
                .validate(identity, Some(previous), &Default::default())
                .is_err()
        );
    }

    #[test]
    fn command_delta_rejects_a_frontier_that_skips_a_processed_entry() {
        let identity = TabletStorageIdentity {
            tablet_id: TabletId(5),
            table_id: TableId(8),
            raft_group_id: RaftGroupId(13),
            replica_id: ReplicaId(21),
        };
        let previous = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(13),
            replica_id: ReplicaId(21),
            applied_index: 4,
            applied_term: 2,
        };
        let delta = CommandDelta::frontier_only(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(13),
            replica_id: ReplicaId(21),
            applied_index: 6,
            applied_term: 3,
        });

        assert!(
            delta
                .validate(identity, Some(previous), &Default::default())
                .is_err()
        );
    }

    #[test]
    fn command_delta_rejects_a_noninitial_frontier_without_a_predecessor() {
        let identity = TabletStorageIdentity {
            tablet_id: TabletId(5),
            table_id: TableId(8),
            raft_group_id: RaftGroupId(13),
            replica_id: ReplicaId(21),
        };
        let delta = CommandDelta::frontier_only(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(13),
            replica_id: ReplicaId(21),
            applied_index: 2,
            applied_term: 1,
        });

        assert!(delta.validate(identity, None, &Default::default()).is_err());
    }
}
