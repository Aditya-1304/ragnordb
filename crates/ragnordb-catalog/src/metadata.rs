//! Deterministic replicated state owned by the metadata Raft group.
//!
//! Metadata contains durable desired topology and SQL catalog state.
//! Current leaders and committed Raft `ConfState` are intentionally separate
//! authorities.
//!
//! A syntactically valid committed command may be rejected by metadata
//! preconditions without damaging the Raft state machine. Such outcomes are
//! represented by `MetadataApplyOutcome::Rejected`; only malformed committed
//! bytes or an invalid snapshot are fatal state-machine errors.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use ragnordb_common::{
    Error, Result,
    ids::{NodeId, RaftGroupId, ReplicaId, TableId, TabletId},
    metadata_codec::{
        DesiredReplicaPlacement, DesiredReplicaRole, MetadataCommand, MetadataSnapshot,
        NodeDescriptor, PartitionSpec, RetiredReplicaLifetime, TabletDescriptor,
    },
};

use crate::{Catalog, TableSchema};

/// Result of applying one already-committed metadata command.
///
/// `Rejected` is part of normal deterministic state-machine behavior. It must
/// eventually be returned to the proposal waiter, but must never quarantine the
/// metadata Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataApplyOutcome {
    Applied,
    AlreadyApplied,
    Rejected(MetadataRejection),
}

impl MetadataApplyOutcome {
    pub const fn changed_state(&self) -> bool {
        matches!(self, Self::Applied)
    }
}

/// Deterministic logical rejection of a committed metadata command.
///
/// These are not storage corruption and not Raft failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetadataRejection {
    #[error("metadata command is invalid: {0}")]
    InvalidCommand(String),

    #[error("metadata has not committed ClusterInitialized")]
    NotInitialized,

    #[error("metadata is already initialized for cluster {existing}, not {received}")]
    ClusterConflict { existing: String, received: String },

    #[error(
        "node {} is already registered with a different directory record",
        .0.0
    )]
    NodeIdConflict(NodeId),

    #[error(
        "endpoint {endpoint} is already owned by node {existing_node}; node {attempted_node} cannot reuse it"
    )]
    NodeEndpointConflict {
        endpoint: String,
        existing_node: u64,
        attempted_node: u64,
    },

    #[error("table definition is invalid: {0}")]
    InvalidTable(String),

    #[error(
        "table ID {} is already assigned to another table",
        .0.0
    )]
    TableIdConflict(TableId),

    #[error("table name {0} is already assigned")]
    TableNameConflict(String),

    #[error(
        "metadata references unknown table {}",
        .0.0
    )]
    UnknownTable(TableId),

    #[error(
        "tablet ID {} is already assigned differently",
        .0.0
    )]
    TabletIdConflict(TabletId),

    #[error(
        "Raft group {} is already assigned to another tablet",
        .0.0
    )]
    RaftGroupConflict(RaftGroupId),

    #[error(
        "table {} expects {expected} hash buckets but tablet metadata declares {received}",
        .table_id.0
    )]
    PartitionCountMismatch {
        table_id: TableId,
        expected: u32,
        received: u32,
    },

    #[error(
        "table {} already owns hash bucket {bucket}",
        .table_id.0
    )]
    PartitionConflict { table_id: TableId, bucket: u32 },

    #[error(
        "table {} already has all {limit} configured tablets",
        .table_id.0
    )]
    TabletCountExceeded { table_id: TableId, limit: u32 },

    #[error(
        "metadata references unknown tablet {}",
        .0.0
    )]
    UnknownTablet(TabletId),

    #[error(
        "desired placement references unknown node {}",
        .0.0
    )]
    UnknownNode(NodeId),

    #[error(
        "first desired placement for tablet {} must use configuration epoch 1, received {received}",
        .tablet_id.0
    )]
    InitialPlacementEpoch { tablet_id: TabletId, received: u64 },

    #[error(
        "tablet {} placement must advance to epoch {expected}, received {received}",
        .tablet_id.0
    )]
    PlacementEpochMismatch {
        tablet_id: TabletId,
        expected: u64,
        received: u64,
    },

    #[error(
        "configuration epoch space is exhausted for tablet {}",
        .0.0
    )]
    PlacementEpochExhausted(TabletId),

    #[error(
        "replica {} of Raft group {} was retired and may never be reused",
        .replica_id.0,
        .raft_group_id.0
    )]
    RetiredReplicaReuse {
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    },

    #[error(
        "replica {} cannot move from node {} to node {}; allocate a new ReplicaId",
        .replica_id.0,
        .previous_node.0,
        .new_node.0
    )]
    ReplicaHostChanged {
        replica_id: ReplicaId,
        previous_node: NodeId,
        new_node: NodeId,
    },

    #[error(
        "replica {} cannot transition from voter back to learner; replace the replica lifetime instead",
        .0.0
    )]
    UnsupportedVoterDemotion(ReplicaId),

    #[error(
        "schema update for table {} expected version {expected} but current version is {current}",
        .table_id.0
    )]
    SchemaPreconditionMismatch {
        table_id: TableId,
        expected: u64,
        current: u64,
    },

    #[error(
        "schema version space is exhausted for table {}",
        .0.0
    )]
    SchemaVersionExhausted(TableId),

    #[error(
        "schema update for table {} must advance to version {expected}, received {received}",
        .table_id.0
    )]
    SchemaVersionNotNext {
        table_id: TableId,
        expected: u64,
        received: u64,
    },

    #[error(
        "schema update cannot rename table {}",
        .0.0
    )]
    TableRenameNotSupported(TableId),

    #[error(
        "schema update cannot change tablet count for table {}",
        .0.0
    )]
    TabletCountChangeNotSupported(TableId),

    #[error(
        "schema update cannot change the primary key for table {}",
        .0.0
    )]
    PrimaryKeyChangeNotSupported(TableId),

    #[error(
        "schema update changed existing column {}",
        .0.0
    )]
    ExistingColumnChanged(ragnordb_common::ids::ColumnId),

    #[error(
        "new column {} must be nullable until a default/backfill protocol exists",
        .0.0
    )]
    AddedColumnMustBeNullable(ragnordb_common::ids::ColumnId),

    #[error(
        "new column ID {} must be greater than all previously allocated column IDs",
        .0.0
    )]
    ColumnIdReuse(ragnordb_common::ids::ColumnId),
}

/// Complete deterministic projection of committed metadata.
#[derive(Debug, Clone, Default)]
pub struct MetadataState {
    cluster_id: Option<String>,

    nodes: BTreeMap<NodeId, NodeDescriptor>,

    tables: BTreeMap<TableId, Arc<TableSchema>>,

    table_ids_by_name: BTreeMap<String, TableId>,

    tablets: BTreeMap<TabletId, TabletDescriptor>,

    tablet_ids_by_raft_group: BTreeMap<RaftGroupId, TabletId>,

    tablet_ids_by_partition: BTreeMap<(TableId, u32), TabletId>,

    desired_placements: BTreeMap<TabletId, DesiredReplicaPlacement>,

    /// Consensus identities whose removal has been authoritatively completed.
    ///
    /// Phase 5.1 does not populate this set merely because a replica disappears
    /// from desired placement. Desired topology is intent; a replica lifetime ends
    /// only after the affected Raft group has committed its removal.
    ///
    /// Phase 5.10 will connect committed membership removal to this durable
    /// retirement history so removed ReplicaIds can never be reused.
    retired_replicas: BTreeSet<(RaftGroupId, ReplicaId)>,
}

impl MetadataState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    pub fn node(&self, node_id: NodeId) -> Option<&NodeDescriptor> {
        self.nodes.get(&node_id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &NodeDescriptor> {
        self.nodes.values()
    }

    pub fn table(&self, table_id: TableId) -> Option<&TableSchema> {
        self.tables.get(&table_id).map(Arc::as_ref)
    }

    pub fn tablet(&self, tablet_id: TabletId) -> Option<&TabletDescriptor> {
        self.tablets.get(&tablet_id)
    }

    pub fn tablet_for_raft_group(&self, raft_group_id: RaftGroupId) -> Option<&TabletDescriptor> {
        let tablet_id = self.tablet_ids_by_raft_group.get(&raft_group_id)?;

        self.tablets.get(tablet_id)
    }

    pub fn desired_placement(&self, tablet_id: TabletId) -> Option<&DesiredReplicaPlacement> {
        self.desired_placements.get(&tablet_id)
    }

    pub fn is_replica_retired(&self, raft_group_id: RaftGroupId, replica_id: ReplicaId) -> bool {
        self.retired_replicas.contains(&(raft_group_id, replica_id))
    }

    /// Apply one command that has already committed in metadata Raft.
    ///
    /// Domain conflicts are deterministic rejection outcomes. They deliberately
    /// do not escape as `Result::Err`, because an ordinary stale request must
    /// not quarantine the metadata Raft group.
    pub fn apply(&mut self, command: MetadataCommand) -> MetadataApplyOutcome {
        if let Err(error) = command.validate() {
            return MetadataApplyOutcome::Rejected(MetadataRejection::InvalidCommand(
                error.to_string(),
            ));
        }

        match command {
            MetadataCommand::ClusterInitialized { cluster_id } => {
                self.apply_cluster_initialized(cluster_id)
            }

            MetadataCommand::RegisterNode(node) => self.apply_register_node(node),

            MetadataCommand::CreateTable { table } => self.apply_create_table(table),

            MetadataCommand::CreateTablet { tablet } => self.apply_create_tablet(tablet),

            MetadataCommand::SetDesiredReplicaPlacement(placement) => {
                self.apply_desired_placement(placement)
            }

            MetadataCommand::UpdateTableSchema {
                expected_schema_version,
                table,
            } => self.apply_schema_update(expected_schema_version, table),
        }
    }

    /// Build the canonical state-machine snapshot.
    pub fn to_snapshot(&self) -> MetadataSnapshot {
        MetadataSnapshot {
            cluster_id: self.cluster_id.clone(),

            nodes: self.nodes.values().cloned().collect(),

            tables: self
                .tables
                .values()
                .map(|table| table.to_definition())
                .collect(),

            tablets: self.tablets.values().cloned().collect(),

            desired_placements: self.desired_placements.values().cloned().collect(),

            retired_replicas: self
                .retired_replicas
                .iter()
                .map(|(raft_group_id, replica_id)| RetiredReplicaLifetime {
                    raft_group_id: *raft_group_id,
                    replica_id: *replica_id,
                })
                .collect(),
        }
    }

    /// Restore one metadata snapshot while re-running the same semantic
    /// invariants as log replay.
    ///
    /// Unlike normal committed-command conflicts, an impossible snapshot is
    /// durable corruption and therefore returns an actual error.
    pub fn from_snapshot(snapshot: MetadataSnapshot) -> Result<Self> {
        snapshot
            .validate()
            .map_err(|error| Error::CorruptData(format!("invalid metadata snapshot: {error}")))?;

        let MetadataSnapshot {
            cluster_id,
            nodes,
            tables,
            tablets,
            desired_placements,
            retired_replicas,
        } = snapshot;

        if cluster_id.is_none() {
            return Ok(Self::new());
        }

        let mut state = Self::new();

        apply_snapshot_command(
            &mut state,
            MetadataCommand::ClusterInitialized {
                cluster_id: cluster_id.expect("checked above"),
            },
        )?;

        for node in nodes {
            apply_snapshot_command(&mut state, MetadataCommand::RegisterNode(node))?;
        }

        for table in tables {
            apply_snapshot_command(&mut state, MetadataCommand::CreateTable { table })?;
        }

        for tablet in tablets {
            apply_snapshot_command(&mut state, MetadataCommand::CreateTablet { tablet })?;
        }

        for placement in desired_placements {
            // Snapshot restoration must be able to reconstitute the latest
            // placement at any epoch (e.g. epoch 2 after compaction), not just
            // epoch 1. Re-run the same domain validations as log replay except
            // for the strictly-sequential epoch progression, which is a log
            // append invariant and not a snapshot invariant.
            if !state.tablets.contains_key(&placement.tablet_id) {
                return Err(Error::CorruptData(format!(
                    "metadata snapshot references unknown tablet {}",
                    placement.tablet_id.0
                )));
            }
            for replica in &placement.replicas {
                if !state.nodes.contains_key(&replica.node_id) {
                    return Err(Error::CorruptData(format!(
                        "metadata snapshot references unknown node {}",
                        replica.node_id.0
                    )));
                }
                // Retired-replica reuse is also checked below for the retired
                // set, but a snapshot that directly reuses a retired replica
                // in its desired placement is already corrupt.
                if state.retired_replicas.contains(&(
                    state.tablets[&placement.tablet_id].raft_group_id,
                    replica.replica_id,
                )) {
                    return Err(Error::CorruptData(format!(
                        "metadata snapshot reuses retired replica {} of group {}",
                        replica.replica_id.0, state.tablets[&placement.tablet_id].raft_group_id.0
                    )));
                }
            }
            // Canonical ordering and voter checks are already enforced by
            // `snapshot.validate()`, which was called above.
            state
                .desired_placements
                .insert(placement.tablet_id, placement);
        }

        for retired in retired_replicas {
            if !state
                .tablet_ids_by_raft_group
                .contains_key(&retired.raft_group_id)
            {
                return Err(Error::CorruptData(format!(
                    "metadata snapshot retires replica {} for unknown Raft group {}",
                    retired.replica_id.0, retired.raft_group_id.0,
                )));
            }

            let tablet_id = state.tablet_ids_by_raft_group[&retired.raft_group_id];

            if state
                .desired_placements
                .get(&tablet_id)
                .is_some_and(|placement| {
                    placement
                        .replicas
                        .iter()
                        .any(|replica| replica.replica_id == retired.replica_id)
                })
            {
                return Err(Error::CorruptData(format!(
                    "metadata snapshot marks active replica {} of group {} as retired",
                    retired.replica_id.0, retired.raft_group_id.0,
                )));
            }

            state
                .retired_replicas
                .insert((retired.raft_group_id, retired.replica_id));
        }

        Ok(state)
    }

    fn apply_cluster_initialized(&mut self, cluster_id: String) -> MetadataApplyOutcome {
        match &self.cluster_id {
            None => {
                self.cluster_id = Some(cluster_id);

                MetadataApplyOutcome::Applied
            }

            Some(existing) if existing == &cluster_id => MetadataApplyOutcome::AlreadyApplied,

            Some(existing) => MetadataApplyOutcome::Rejected(MetadataRejection::ClusterConflict {
                existing: existing.clone(),
                received: cluster_id,
            }),
        }
    }

    fn apply_register_node(&mut self, node: NodeDescriptor) -> MetadataApplyOutcome {
        if let Err(rejection) = self.require_initialized() {
            return MetadataApplyOutcome::Rejected(rejection);
        }

        if let Some(existing) = self.nodes.get(&node.node_id) {
            return if existing == &node {
                MetadataApplyOutcome::AlreadyApplied
            } else {
                MetadataApplyOutcome::Rejected(MetadataRejection::NodeIdConflict(node.node_id))
            };
        }

        for existing in self.nodes.values() {
            for attempted_endpoint in node_endpoints(&node) {
                if node_endpoints(existing).contains(&attempted_endpoint) {
                    return MetadataApplyOutcome::Rejected(
                        MetadataRejection::NodeEndpointConflict {
                            endpoint: attempted_endpoint.to_string(),
                            existing_node: existing.node_id.0,
                            attempted_node: node.node_id.0,
                        },
                    );
                }
            }
        }

        self.nodes.insert(node.node_id, node);

        MetadataApplyOutcome::Applied
    }

    fn apply_create_table(
        &mut self,
        table: ragnordb_common::catalog_codec::TableDefinition,
    ) -> MetadataApplyOutcome {
        if let Err(rejection) = self.require_initialized() {
            return MetadataApplyOutcome::Rejected(rejection);
        }

        let schema = match TableSchema::from_definition(table) {
            Ok(schema) => schema,

            Err(error) => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::InvalidTable(
                    error.to_string(),
                ));
            }
        };

        if let Some(existing) = self.tables.get(&schema.id) {
            return if existing.as_ref() == &schema {
                MetadataApplyOutcome::AlreadyApplied
            } else {
                MetadataApplyOutcome::Rejected(MetadataRejection::TableIdConflict(schema.id))
            };
        }

        if self.table_ids_by_name.contains_key(&schema.name) {
            return MetadataApplyOutcome::Rejected(MetadataRejection::TableNameConflict(
                schema.name,
            ));
        }

        let table_id = schema.id;
        let table_name = schema.name.clone();

        self.tables.insert(table_id, Arc::new(schema));

        self.table_ids_by_name.insert(table_name, table_id);

        MetadataApplyOutcome::Applied
    }

    fn apply_create_tablet(&mut self, tablet: TabletDescriptor) -> MetadataApplyOutcome {
        if let Err(rejection) = self.require_initialized() {
            return MetadataApplyOutcome::Rejected(rejection);
        }

        if let Some(existing) = self.tablets.get(&tablet.tablet_id) {
            return if existing == &tablet {
                MetadataApplyOutcome::AlreadyApplied
            } else {
                MetadataApplyOutcome::Rejected(MetadataRejection::TabletIdConflict(
                    tablet.tablet_id,
                ))
            };
        }

        if self
            .tablet_ids_by_raft_group
            .contains_key(&tablet.raft_group_id)
        {
            return MetadataApplyOutcome::Rejected(MetadataRejection::RaftGroupConflict(
                tablet.raft_group_id,
            ));
        }

        let table = match self.tables.get(&tablet.table_id) {
            Some(table) => table,

            None => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::UnknownTable(
                    tablet.table_id,
                ));
            }
        };

        let (bucket, bucket_count) = match &tablet.partition {
            PartitionSpec::Hash {
                bucket,
                bucket_count,
            } => (*bucket, *bucket_count),
        };

        if bucket_count != table.tablet_count {
            return MetadataApplyOutcome::Rejected(MetadataRejection::PartitionCountMismatch {
                table_id: tablet.table_id,
                expected: table.tablet_count,
                received: bucket_count,
            });
        }

        if self
            .tablet_ids_by_partition
            .contains_key(&(tablet.table_id, bucket))
        {
            return MetadataApplyOutcome::Rejected(MetadataRejection::PartitionConflict {
                table_id: tablet.table_id,
                bucket,
            });
        }

        let existing_count = self
            .tablets
            .values()
            .filter(|existing| existing.table_id == tablet.table_id)
            .count();

        let limit = table.tablet_count as usize;

        if existing_count >= limit {
            return MetadataApplyOutcome::Rejected(MetadataRejection::TabletCountExceeded {
                table_id: tablet.table_id,
                limit: table.tablet_count,
            });
        }

        self.tablet_ids_by_raft_group
            .insert(tablet.raft_group_id, tablet.tablet_id);

        self.tablet_ids_by_partition
            .insert((tablet.table_id, bucket), tablet.tablet_id);

        self.tablets.insert(tablet.tablet_id, tablet);

        MetadataApplyOutcome::Applied
    }

    fn apply_desired_placement(
        &mut self,
        placement: DesiredReplicaPlacement,
    ) -> MetadataApplyOutcome {
        if let Err(rejection) = self.require_initialized() {
            return MetadataApplyOutcome::Rejected(rejection);
        }

        let tablet = match self.tablets.get(&placement.tablet_id) {
            Some(tablet) => tablet.clone(),

            None => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::UnknownTablet(
                    placement.tablet_id,
                ));
            }
        };

        for replica in &placement.replicas {
            if !self.nodes.contains_key(&replica.node_id) {
                return MetadataApplyOutcome::Rejected(MetadataRejection::UnknownNode(
                    replica.node_id,
                ));
            }

            if self
                .retired_replicas
                .contains(&(tablet.raft_group_id, replica.replica_id))
            {
                return MetadataApplyOutcome::Rejected(MetadataRejection::RetiredReplicaReuse {
                    raft_group_id: tablet.raft_group_id,
                    replica_id: replica.replica_id,
                });
            }
        }

        let existing = self.desired_placements.get(&placement.tablet_id).cloned();

        if existing
            .as_ref()
            .is_some_and(|existing| existing == &placement)
        {
            return MetadataApplyOutcome::AlreadyApplied;
        }

        match &existing {
            None => {
                if placement.configuration_epoch != 1 {
                    return MetadataApplyOutcome::Rejected(
                        MetadataRejection::InitialPlacementEpoch {
                            tablet_id: placement.tablet_id,
                            received: placement.configuration_epoch,
                        },
                    );
                }
            }

            Some(existing) => {
                let expected = match existing.configuration_epoch.checked_add(1) {
                    Some(expected) => expected,

                    None => {
                        return MetadataApplyOutcome::Rejected(
                            MetadataRejection::PlacementEpochExhausted(placement.tablet_id),
                        );
                    }
                };

                if placement.configuration_epoch != expected {
                    return MetadataApplyOutcome::Rejected(
                        MetadataRejection::PlacementEpochMismatch {
                            tablet_id: placement.tablet_id,
                            expected,
                            received: placement.configuration_epoch,
                        },
                    );
                }

                let old_by_id: BTreeMap<ReplicaId, _> = existing
                    .replicas
                    .iter()
                    .map(|replica| (replica.replica_id, replica))
                    .collect();

                for new_replica in &placement.replicas {
                    let Some(old_replica) = old_by_id.get(&new_replica.replica_id) else {
                        continue;
                    };

                    if old_replica.node_id != new_replica.node_id {
                        return MetadataApplyOutcome::Rejected(
                            MetadataRejection::ReplicaHostChanged {
                                replica_id: new_replica.replica_id,
                                previous_node: old_replica.node_id,
                                new_node: new_replica.node_id,
                            },
                        );
                    }

                    if old_replica.role == DesiredReplicaRole::Voter
                        && new_replica.role == DesiredReplicaRole::Learner
                    {
                        return MetadataApplyOutcome::Rejected(
                            MetadataRejection::UnsupportedVoterDemotion(new_replica.replica_id),
                        );
                    }
                }
            }
        }

        self.desired_placements
            .insert(placement.tablet_id, placement);

        MetadataApplyOutcome::Applied
    }

    fn apply_schema_update(
        &mut self,
        expected_schema_version: u64,
        table: ragnordb_common::catalog_codec::TableDefinition,
    ) -> MetadataApplyOutcome {
        if let Err(rejection) = self.require_initialized() {
            return MetadataApplyOutcome::Rejected(rejection);
        }

        let updated = match TableSchema::from_definition(table) {
            Ok(schema) => schema,

            Err(error) => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::InvalidTable(
                    error.to_string(),
                ));
            }
        };

        let existing = match self.tables.get(&updated.id) {
            Some(existing) => existing.clone(),

            None => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::UnknownTable(updated.id));
            }
        };

        // Exact replay must succeed even if its expected-version precondition is
        // now stale.
        if existing.as_ref() == &updated {
            return MetadataApplyOutcome::AlreadyApplied;
        }

        if expected_schema_version != existing.schema_version {
            return MetadataApplyOutcome::Rejected(MetadataRejection::SchemaPreconditionMismatch {
                table_id: updated.id,
                expected: expected_schema_version,
                current: existing.schema_version,
            });
        }

        let next_schema_version = match existing.schema_version.checked_add(1) {
            Some(version) => version,

            None => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::SchemaVersionExhausted(
                    updated.id,
                ));
            }
        };

        if updated.schema_version != next_schema_version {
            return MetadataApplyOutcome::Rejected(MetadataRejection::SchemaVersionNotNext {
                table_id: updated.id,
                expected: next_schema_version,
                received: updated.schema_version,
            });
        }

        if updated.name != existing.name {
            return MetadataApplyOutcome::Rejected(MetadataRejection::TableRenameNotSupported(
                updated.id,
            ));
        }

        if updated.tablet_count != existing.tablet_count {
            return MetadataApplyOutcome::Rejected(
                MetadataRejection::TabletCountChangeNotSupported(updated.id),
            );
        }

        if updated.primary_key_column_ids != existing.primary_key_column_ids {
            return MetadataApplyOutcome::Rejected(
                MetadataRejection::PrimaryKeyChangeNotSupported(updated.id),
            );
        }

        if updated.columns.len() < existing.columns.len() {
            return MetadataApplyOutcome::Rejected(MetadataRejection::ExistingColumnChanged(
                existing.columns[updated.columns.len()].id,
            ));
        }

        for (existing_column, updated_column) in existing.columns.iter().zip(updated.columns.iter())
        {
            if existing_column != updated_column {
                return MetadataApplyOutcome::Rejected(MetadataRejection::ExistingColumnChanged(
                    existing_column.id,
                ));
            }
        }

        let max_existing_column_id = existing
            .columns
            .iter()
            .map(|column| column.id.0)
            .max()
            .unwrap_or(0);

        for added_column in updated.columns.iter().skip(existing.columns.len()) {
            if added_column.id.0 <= max_existing_column_id {
                return MetadataApplyOutcome::Rejected(MetadataRejection::ColumnIdReuse(
                    added_column.id,
                ));
            }

            // Adding NOT NULL without a default/backfill protocol would make old
            // encoded rows immediately violate the new schema.
            if !added_column.nullable {
                return MetadataApplyOutcome::Rejected(
                    MetadataRejection::AddedColumnMustBeNullable(added_column.id),
                );
            }
        }

        let table_id = updated.id;
        let updated_name = updated.name.clone();

        self.tables.insert(table_id, Arc::new(updated));

        self.table_ids_by_name.insert(updated_name, table_id);

        MetadataApplyOutcome::Applied
    }

    fn require_initialized(&self) -> std::result::Result<(), MetadataRejection> {
        if self.cluster_id.is_none() {
            return Err(MetadataRejection::NotInitialized);
        }

        Ok(())
    }
}

impl Catalog for MetadataState {
    fn table_by_name(&self, name: &str) -> Option<Arc<TableSchema>> {
        let table_id = self.table_ids_by_name.get(name)?;

        self.tables.get(table_id).cloned()
    }

    fn table_by_id(&self, id: TableId) -> Option<Arc<TableSchema>> {
        self.tables.get(&id).cloned()
    }

    fn list_tables(&self) -> Vec<Arc<TableSchema>> {
        self.tables.values().cloned().collect()
    }
}

fn node_endpoints(node: &NodeDescriptor) -> [&str; 4] {
    [
        node.raft_addr.as_str(),
        node.snapshot_addr.as_str(),
        node.sql_addr.as_str(),
        node.admin_addr.as_str(),
    ]
}

fn apply_snapshot_command(state: &mut MetadataState, command: MetadataCommand) -> Result<()> {
    match state.apply(command) {
        MetadataApplyOutcome::Applied | MetadataApplyOutcome::AlreadyApplied => Ok(()),

        MetadataApplyOutcome::Rejected(rejection) => Err(Error::CorruptData(format!(
            "metadata snapshot violates state-machine invariant: {rejection}"
        ))),
    }
}
