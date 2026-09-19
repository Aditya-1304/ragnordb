//! Deterministic commit-decision planning for distributed transactions.
//!
//! This module allocates the commit timestamp only after the coordinator has
//! a complete, coherent participant route view. It then materializes the
//! primary/status record and per-tablet commit batches without submitting any
//! command. Durable primary dispatch and secondary cleanup belong to the next
//! commit slice.

use std::collections::BTreeMap;

use ragnordb_common::{
    Error, Result,
    codec::{TxnStatus, TxnStatusRecord},
    command_codec::CommitCommand,
    ids::{ParticipantCommandPhase, TabletId, Timestamp},
};

use crate::{
    TransactionManager,
    coordinator::{DistributedTransactionCoordinator, ParticipantCommandPlan, ParticipantRoute},
};

/// One validated commit command for one participant tablet.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitBatchPlan {
    /// Current physical route for the participant tablet.
    pub route: ParticipantRoute,

    /// Complete set of prewritten keys to commit on this tablet.
    pub command: CommitCommand,

    /// Stable per-key commit identities in the same canonical order as
    /// `command.keys`.
    pub participant_plans: Vec<ParticipantCommandPlan>,
}

impl CommitBatchPlan {
    /// Return the physical tablet selected by this batch.
    pub fn tablet_id(&self) -> TabletId {
        self.route.tablet_id
    }
}

/// Complete side-effect-free commit decision plan.
///
/// `primary` is dispatched first by Slice 2 because it carries the primary
/// key's commit command. `status_record` is the durable transaction outcome
/// that must be published with the primary/status tablet's commit point.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitPhasePlan {
    pub commit_timestamp: Timestamp,
    pub status_route: ParticipantRoute,
    pub status_record: TxnStatusRecord,
    pub primary: CommitBatchPlan,
    pub secondary: Vec<CommitBatchPlan>,
}

struct BatchBuilder {
    route: ParticipantRoute,
    keys: Vec<Vec<u8>>,
    participant_plans: Vec<ParticipantCommandPlan>,
}

/// Validate the current participant view, allocate a strictly newer commit
/// timestamp, and construct primary-before-secondary commit batches.
pub(crate) fn plan_commit<M: TransactionManager>(
    coordinator: &DistributedTransactionCoordinator,
    timestamp_manager: &mut M,
) -> Result<CommitPhasePlan> {
    if coordinator.write_set().is_empty() {
        return Err(Error::InvalidArgument(
            "commit requires at least one prewritten key".to_string(),
        ));
    }

    let status_route = coordinator.status_location().route().ok_or_else(|| {
        Error::InvalidArgument(
            "commit planning requires a current transaction status route".to_string(),
        )
    })?;
    let primary_route = coordinator
        .participant_route(coordinator.primary_key())
        .copied()
        .ok_or_else(|| {
            Error::InvalidArgument(
                "commit planning requires a current primary participant route".to_string(),
            )
        })?;
    if status_route != primary_route {
        return Err(Error::InvalidArgument(
            "transaction status route must match the primary participant route".to_string(),
        ));
    }

    // Grouping is performed before timestamp allocation so malformed or stale
    // routing cannot consume a commit timestamp that will never be used.
    let mut groups: BTreeMap<TabletId, BatchBuilder> = BTreeMap::new();
    for key in coordinator.write_set().keys() {
        let route = coordinator.participant_route(key).copied().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "commit participant key has no current route hint: {:?}",
                key
            ))
        })?;
        let participant_plan =
            coordinator.participant_command_plan(ParticipantCommandPhase::Commit, key)?;

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
                        "commit participant tablet {} has conflicting route hints",
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
    let commit_timestamp =
        timestamp_manager.allocate_commit_timestamp(coordinator.start_timestamp())?;
    if commit_timestamp.0 == 0 || commit_timestamp <= coordinator.start_timestamp() {
        return Err(Error::InvalidArgument(
            "commit timestamp must be greater than the transaction start timestamp".to_string(),
        ));
    }

    let status_record = TxnStatusRecord {
        txn_id: coordinator.transaction_id(),
        start_timestamp: coordinator.start_timestamp(),
        commit_timestamp: Some(commit_timestamp),
        status: TxnStatus::Committed,
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
            let command = CommitCommand {
                txn_id: coordinator.transaction_id(),
                start_timestamp: coordinator.start_timestamp(),
                commit_timestamp,
                keys: batch.keys,
            };
            command
                .validate()
                .map_err(|error| Error::InvalidArgument(error.to_string()))?;

            Ok(CommitBatchPlan {
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
            Error::CorruptData("commit plan omitted the primary participant batch".to_string())
        })?;
    let primary = batches.swap_remove(primary_index);
    batches.sort_by_key(|batch| batch.route.tablet_id);

    Ok(CommitPhasePlan {
        commit_timestamp,
        status_route,
        status_record,
        primary,
        secondary: batches,
    })
}
