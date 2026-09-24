//! Deterministic construction of distributed prewrite participant batches.
//!
//! This module stops at the command-planning boundary. It validates the
//! complete transaction write set, groups mutations by the current tablet
//! route, and carries one stable logical command plan per mutation. Transport,
//! Raft proposal, route refresh, and failure compensation remain separate
//! responsibilities so planning cannot accidentally acknowledge a prewrite.

use std::collections::BTreeMap;

use ragnordb_common::{
    Error, Result,
    codec::{TxnStatus, TxnStatusRecord, WriteKind},
    command_codec::{PrewriteCommand, TabletCommand, TabletCommandEnvelope, WriteEntry},
    encoding::decode_row,
    ids::{ParticipantCommandPhase, TabletId},
};
use ragnordb_storage::mvcc::Mutation;

use crate::coordinator::{
    DistributedTransactionCoordinator, ParticipantCommandPlan, ParticipantRoute,
};

/// One validated prewrite command for one participant tablet.
///
/// `participant_plans` retains the topology-independent identity of each
/// logical mutation in the batch. A later dispatcher may use those identities
/// when constructing a transport envelope, but this type itself has no side
/// effects and does not imply that any command has been durably proposed.
#[derive(Debug, Clone, PartialEq)]
pub struct PrewriteBatchPlan {
    /// Current physical route shared by every mutation in the batch.
    pub route: ParticipantRoute,

    /// Complete command payload to be proposed to the participant tablet.
    pub command: PrewriteCommand,

    /// Stable per-mutation identities corresponding to `command.writes` in
    /// the same canonical order.
    pub participant_plans: Vec<ParticipantCommandPlan>,
}

impl PrewriteBatchPlan {
    /// Return the physical tablet selected by this batch's route hint.
    pub fn tablet_id(&self) -> TabletId {
        self.route.tablet_id
    }
}

struct BatchBuilder {
    route: ParticipantRoute,
    writes: Vec<WriteEntry>,
    participant_plans: Vec<ParticipantCommandPlan>,
}

/// Build deterministic prewrite batches from the coordinator's complete write
/// set.
pub(crate) fn plan_prewrite(
    coordinator: &DistributedTransactionCoordinator,
    ttl_ms: u64,
) -> Result<Vec<PrewriteBatchPlan>> {
    if ttl_ms == 0 {
        return Err(Error::InvalidArgument(
            "prewrite lock TTL must be non-zero".to_string(),
        ));
    }

    if coordinator.write_set().is_empty() {
        return Err(Error::InvalidArgument(
            "prewrite requires at least one write".to_string(),
        ));
    }

    if !coordinator
        .write_set()
        .contains_key(coordinator.primary_key())
    {
        return Err(Error::InvalidArgument(
            "prewrite primary key must be present in the transaction write set".to_string(),
        ));
    }

    // BTreeMap ordering makes both participant order and write order
    // deterministic. The route equality check prevents one batch from mixing
    // epochs or Raft groups when a topology refresh updates only part of a
    // transaction's route hints.
    let mut groups: BTreeMap<TabletId, BatchBuilder> = BTreeMap::new();
    for (key, mutation) in coordinator.write_set() {
        let route = coordinator.participant_route(key).copied().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "prewrite participant key has no current route hint: {:?}",
                key
            ))
        })?;
        let participant_plan =
            coordinator.participant_command_plan(ParticipantCommandPhase::Prewrite, key)?;
        let write = write_entry(key, mutation)?;

        match groups.entry(route.tablet_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(BatchBuilder {
                    route,
                    writes: vec![write],
                    participant_plans: vec![participant_plan],
                });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let batch = entry.get_mut();
                if batch.route != route {
                    return Err(Error::InvalidArgument(format!(
                        "prewrite participant tablet {} has conflicting route hints",
                        route.tablet_id.0
                    )));
                }
                batch.writes.push(write);
                batch.participant_plans.push(participant_plan);
            }
        }
    }

    let participant_tablets = groups.keys().copied().collect();
    let participant_tablet_ids = coordinator
        .status_location()
        .ordered_participant_tablet_ids(&participant_tablets)?;
    let pending_status = TxnStatusRecord {
        txn_id: coordinator.transaction_id(),
        start_timestamp: coordinator.start_timestamp(),
        commit_timestamp: None,
        status: TxnStatus::Pending,
        primary_key: coordinator.primary_key().to_vec(),
        participant_tablet_ids,
        last_heartbeat_timestamp: None,
        lease_deadline_ms: None,
    };
    pending_status
        .validate()
        .map_err(|error| Error::InvalidArgument(error.to_string()))?;

    groups
        .into_values()
        .map(|batch| {
            let includes_primary = batch
                .writes
                .iter()
                .any(|write| write.key == coordinator.primary_key());
            let command = PrewriteCommand {
                txn_id: coordinator.transaction_id(),
                start_timestamp: coordinator.start_timestamp(),
                writes: batch.writes,
                primary_key: coordinator.primary_key().to_vec(),
                ttl_ms,
                pending_status: includes_primary.then(|| pending_status.clone()),
            };
            command
                .validate()
                .map_err(|error| Error::InvalidArgument(error.to_string()))?;

            Ok(PrewriteBatchPlan {
                route: batch.route,
                command,
                participant_plans: batch.participant_plans,
            })
        })
        .collect()
}

/// Validate aggregate transaction limits and measure the conservative
/// serialized size of each single-key command emitted by
/// `GatewayTransactionDispatcher` before any participant is admitted to Raft.
pub(crate) fn preflight_prewrite_commands(
    coordinator: &DistributedTransactionCoordinator,
    batches: &[PrewriteBatchPlan],
    acknowledged_through: Option<u64>,
    check_age: bool,
) -> Result<crate::TransactionFootprint> {
    let mut max_command_bytes = 0usize;
    for batch in batches {
        if batch.command.writes.len() != batch.participant_plans.len() {
            return Err(Error::CorruptData(
                "prewrite command lost its per-key logical identities".to_string(),
            ));
        }
        for (write, participant_plan) in batch.command.writes.iter().zip(&batch.participant_plans) {
            let is_primary = write.key == batch.command.primary_key;
            let mut prewrite = PrewriteCommand {
                txn_id: batch.command.txn_id,
                start_timestamp: batch.command.start_timestamp,
                writes: vec![write.clone()],
                primary_key: batch.command.primary_key.clone(),
                ttl_ms: batch.command.ttl_ms,
                pending_status: if is_primary {
                    batch.command.pending_status.clone()
                } else {
                    None
                },
            };
            if let Some(status) = prewrite.pending_status.as_mut() {
                // The production lifecycle gateway fills these fields just
                // before the first Raft proposal. Measure their maximum wire
                // representation here so every participant passes admission
                // before any command can become durable, even with a large
                // configured lease duration.
                prewrite.ttl_ms = u64::MAX;
                status.last_heartbeat_timestamp = Some(status.start_timestamp);
                status.lease_deadline_ms = Some(u64::MAX);
            }
            let command = TabletCommand::Prewrite(prewrite);
            let envelope = TabletCommandEnvelope::new_with_logical_command_id_and_ack(
                participant_plan.request_id.clone(),
                participant_plan.logical_command_id,
                participant_plan.route.tablet_id,
                participant_plan.route.tablet_epoch,
                acknowledged_through,
                command,
            )
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
            max_command_bytes = max_command_bytes.max(
                envelope
                    .encode()
                    .map_err(|error| Error::InvalidArgument(error.to_string()))?
                    .len(),
            );
        }
    }

    // The primary status record retains the primary key and one tablet ID per
    // participant in addition to the independently charged per-intent state.
    let status_accounting_bytes = 64usize
        .saturating_add(coordinator.primary_key().len())
        .saturating_add(batches.len().saturating_mul(std::mem::size_of::<u64>()));
    coordinator.transaction().validate_distributed_footprint(
        batches.len(),
        max_command_bytes,
        status_accounting_bytes,
        check_age,
    )
}

fn write_entry(key: &[u8], mutation: &Mutation) -> Result<WriteEntry> {
    let (row, op) = match mutation {
        Mutation::Put(encoded_row) => {
            let row = decode_row(encoded_row).map_err(|error| {
                Error::InvalidArgument(format!(
                    "prewrite Put mutation contains a noncanonical row: {error}"
                ))
            })?;
            (Some(row), WriteKind::Put)
        }
        Mutation::Delete => (None, WriteKind::Delete),
    };

    Ok(WriteEntry {
        key: key.to_vec(),
        row,
        op,
    })
}
