//! Deterministic rollback-decision planning for distributed transactions.
//!
//! Rollback planning fences the complete logical write set, including keys
//! whose prewrite result may have been lost. Publishing a rollback marker for
//! those keys prevents a delayed prewrite or commit message from resurrecting
//! an aborted transaction. This module only builds validated plans; dispatch,
//! durable status publication, and outcome recovery belong to Slice 2.

use std::collections::BTreeMap;

use ragnordb_common::{
    Error, Result,
    codec::{TxnStatus, TxnStatusRecord},
    command_codec::RollbackCommand,
    ids::{ParticipantCommandPhase, TabletId},
};

use crate::{
    coordinator::{DistributedTransactionCoordinator, ParticipantCommandPlan, ParticipantRoute},
    status::TransactionStatusKey,
};

/// One validated rollback command for one participant tablet.
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackBatchPlan {
    /// Current physical route for the participant tablet.
    pub route: ParticipantRoute,

    /// Complete logical key set fenced by this rollback batch.
    pub command: RollbackCommand,

    /// Stable per-key rollback identities in the same canonical order as
    /// `command.keys`.
    pub participant_plans: Vec<ParticipantCommandPlan>,
}

impl RollbackBatchPlan {
    /// Return the physical tablet selected by this batch.
    pub fn tablet_id(&self) -> TabletId {
        self.route.tablet_id
    }
}

/// Complete side-effect-free rollback decision plan.
///
/// The primary batch is separated from secondary batches so Slice 2 can apply
/// the chosen abort/status ordering without reconstructing the logical plan.
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackPhasePlan {
    pub status_key: TransactionStatusKey,
    pub status_route: ParticipantRoute,
    pub status_record: TxnStatusRecord,
    pub primary: RollbackBatchPlan,
    pub secondary: Vec<RollbackBatchPlan>,
}

struct BatchBuilder {
    route: ParticipantRoute,
    keys: Vec<Vec<u8>>,
    participant_plans: Vec<ParticipantCommandPlan>,
}

/// Validate routes and build deterministic rollback batches for the complete
/// transaction write set.
pub(crate) fn plan_rollback(
    coordinator: &DistributedTransactionCoordinator,
) -> Result<RollbackPhasePlan> {
    if coordinator.write_set().is_empty() {
        return Err(Error::InvalidArgument(
            "rollback requires at least one transaction key".to_string(),
        ));
    }

    let status_route = coordinator.status_location().route().ok_or_else(|| {
        Error::InvalidArgument(
            "rollback planning requires a current transaction status route".to_string(),
        )
    })?;
    let primary_route = coordinator
        .participant_route(coordinator.primary_key())
        .copied()
        .ok_or_else(|| {
            Error::InvalidArgument(
                "rollback planning requires a current primary participant route".to_string(),
            )
        })?;
    if status_route != primary_route {
        return Err(Error::InvalidArgument(
            "transaction status route must match the primary participant route".to_string(),
        ));
    }

    // The BTreeMap provides canonical tablet and key ordering. The route
    // equality check prevents one rollback batch from mixing epochs or Raft
    // groups after a partial topology refresh.
    let mut groups: BTreeMap<TabletId, BatchBuilder> = BTreeMap::new();
    for key in coordinator.write_set().keys() {
        let route = coordinator.participant_route(key).copied().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "rollback participant key has no current route hint: {:?}",
                key
            ))
        })?;
        let participant_plan =
            coordinator.participant_command_plan(ParticipantCommandPhase::Rollback, key)?;

        match groups.entry(route.tablet_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(BatchBuilder {
                    route,
                    keys: vec![key.clone()],
                    participant_plans: vec![participant_plan],
                });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let batch = entry.get_mut();
                if batch.route != route {
                    return Err(Error::InvalidArgument(format!(
                        "rollback participant tablet {} has conflicting route hints",
                        route.tablet_id.0
                    )));
                }
                batch.keys.push(key.clone());
                batch.participant_plans.push(participant_plan);
            }
        }
    }

    let participant_tablets = groups.keys().copied().collect();
    let participant_tablet_ids = coordinator
        .status_location()
        .ordered_participant_tablet_ids(&participant_tablets)?;
    let status_record = TxnStatusRecord {
        txn_id: coordinator.transaction_id(),
        start_timestamp: coordinator.start_timestamp(),
        commit_timestamp: None,
        status: TxnStatus::Aborted,
        primary_key: coordinator.primary_key().to_vec(),
        participant_tablet_ids,
        last_heartbeat_timestamp: None,
    };
    status_record
        .validate()
        .map_err(|error| Error::InvalidArgument(error.to_string()))?;

    let mut batches = groups
        .into_values()
        .map(|batch| {
            let command = RollbackCommand {
                txn_id: coordinator.transaction_id(),
                start_timestamp: coordinator.start_timestamp(),
                keys: batch.keys,
            };
            command
                .validate()
                .map_err(|error| Error::InvalidArgument(error.to_string()))?;

            Ok(RollbackBatchPlan {
                route: batch.route,
                command,
                participant_plans: batch.participant_plans,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let primary_index = batches
        .iter()
        .position(|batch| batch.route.tablet_id == primary_route.tablet_id)
        .ok_or_else(|| {
            Error::CorruptData("rollback plan omitted the primary participant batch".to_string())
        })?;
    let primary = batches.swap_remove(primary_index);
    batches.sort_by_key(|batch| batch.route.tablet_id);

    Ok(RollbackPhasePlan {
        status_key: coordinator.status_location().status_key().clone(),
        status_route,
        status_record,
        primary,
        secondary: batches,
    })
}
