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
    catalog_codec::TableDefinition,
    ids::{
        ClientRequestId, CommandKind, LogicalCommandId, NodeId, RaftGroupId, ReplicaId, RequestId,
        TableId, TabletId,
    },
    metadata_codec::{
        CreateTableRequest, DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole,
        MetadataAllocatorState, MetadataCachedOutcome, MetadataClientSession, MetadataCommand,
        MetadataRequestDeduplication, MetadataSnapshot, NodeDescriptor, NodeLifecycle,
        PartitionSpec, PlacementPolicy, RetiredReplicaLifetime, TabletDescriptor,
    },
};

use crate::{Catalog, TableSchema};

/// Phase 5.2 creates one initial ordered range per table. The persisted table
/// count remains only an initial topology hint; live ownership is descriptor
/// metadata and may change without changing the table identity.
const INITIAL_TABLET_COUNT: u32 = 1;

/// Initial desired replication factor. Small development clusters use every
/// available node up to this limit; larger clusters do not join every node to
/// every newly created tablet group.
const INITIAL_REPLICATION_FACTOR: usize = 3;

/// Cluster-global identities assigned by one successful atomic CREATE TABLE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataTableCreated {
    pub table_id: TableId,
    pub tablet_id: TabletId,
    pub raft_group_id: RaftGroupId,
}

/// Result of applying one already-committed metadata command.
///
/// `Rejected` is part of normal deterministic state-machine behavior. It must
/// eventually be returned to the proposal waiter, but must never quarantine the
/// metadata Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataApplyOutcome {
    Applied,
    AlreadyApplied,
    ClientRegistered { client_id: u128, session_epoch: u64 },
    ClientRenewed,
    TableCreated(MetadataTableCreated),
    Rejected(MetadataRejection),
}

impl MetadataApplyOutcome {
    pub const fn changed_state(&self) -> bool {
        matches!(
            self,
            Self::Applied
                | Self::ClientRegistered { .. }
                | Self::ClientRenewed
                | Self::TableCreated(_)
        )
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

    #[error("metadata has no registered retry session for client {0}")]
    UnknownClient(u128),

    #[error(
        "client {client_id} session epoch conflicts with durable epoch {existing}; received {received}"
    )]
    ClientSessionEpochConflict {
        client_id: u128,
        existing: u64,
        received: u64,
    },

    #[error("client {0} session epoch space is exhausted")]
    ClientSessionEpochExhausted(u128),

    #[error("client {client_id} acknowledgement regressed from {existing} to {received}")]
    AcknowledgementRegression {
        client_id: u128,
        existing: u64,
        received: u64,
    },

    #[error("client {0} retry horizon sequence space is exhausted")]
    RetryHorizonExhausted(u128),

    #[error(
        "node {} is already registered with a different directory record",
        .0.0
    )]
    NodeIdConflict(NodeId),

    #[error(
        "node {} cannot transition from lifecycle {from} to {to}",
        .node_id.0
    )]
    InvalidNodeLifecycleTransition {
        node_id: NodeId,
        from: &'static str,
        to: &'static str,
    },

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

    #[error("CREATE TABLE requires at least one registered physical node")]
    NoRegisteredNodes,

    #[error("metadata {0} identity space is exhausted")]
    IdentitySpaceExhausted(&'static str),

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

    #[error("desired placement references node {} in lifecycle state {lifecycle}", .node_id.0)]
    NodeNotEligible {
        node_id: NodeId,
        lifecycle: &'static str,
    },

    #[error(
        "desired placement requires {required} distinct {domain}, but only {observed} eligible values are present"
    )]
    PlacementDomainUnsatisfied {
        domain: &'static str,
        required: u32,
        observed: usize,
    },

    #[error(
        "desired placement requires storage class {required}, but node {node_id:?} has {received}"
    )]
    PlacementStorageClassMismatch {
        node_id: NodeId,
        required: String,
        received: String,
    },

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
#[derive(Debug, Clone)]
pub struct MetadataState {
    cluster_id: Option<String>,

    allocator: MetadataAllocatorState,

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

    /// Durable result cache for request identities accepted by metadata Raft.
    ///
    /// A replay must return the original result without re-running allocation
    /// against the current table-name map. This is what makes a retry after a
    /// leadership change different from an independent CREATE TABLE request.
    request_deduplication: BTreeMap<LogicalCommandId, MetadataCachedOutcome>,

    /// Durable retry-session authority. Tablet/gateway code must fail closed
    /// when a request carries an epoch absent from this committed map.
    client_sessions: BTreeMap<u128, MetadataClientSession>,
}

impl Default for MetadataState {
    fn default() -> Self {
        Self {
            cluster_id: None,
            allocator: MetadataAllocatorState::initial(),
            nodes: BTreeMap::new(),
            tables: BTreeMap::new(),
            table_ids_by_name: BTreeMap::new(),
            tablets: BTreeMap::new(),
            tablet_ids_by_raft_group: BTreeMap::new(),
            tablet_ids_by_partition: BTreeMap::new(),
            desired_placements: BTreeMap::new(),
            retired_replicas: BTreeSet::new(),
            request_deduplication: BTreeMap::new(),
            client_sessions: BTreeMap::new(),
        }
    }
}

impl MetadataState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    pub const fn allocator_state(&self) -> MetadataAllocatorState {
        self.allocator
    }

    pub fn node(&self, node_id: NodeId) -> Option<&NodeDescriptor> {
        self.nodes.get(&node_id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &NodeDescriptor> {
        self.nodes.values()
    }

    pub fn client_session(&self, client_id: u128) -> Option<&MetadataClientSession> {
        self.client_sessions.get(&client_id)
    }

    pub fn table(&self, table_id: TableId) -> Option<&TableSchema> {
        self.tables.get(&table_id).map(Arc::as_ref)
    }

    pub fn tablet(&self, tablet_id: TabletId) -> Option<&TabletDescriptor> {
        self.tablets.get(&tablet_id)
    }

    /// Return every committed tablet descriptor in canonical tablet-ID order.
    ///
    /// Server lifecycle reconciliation needs a detached view of all tablets,
    /// not only the tablets belonging to a caller-selected table. Exposing an
    /// iterator keeps metadata as the read-only placement authority without
    /// permitting callers to mutate the state-machine maps.
    pub fn tablets(&self) -> impl Iterator<Item = &TabletDescriptor> {
        self.tablets.values()
    }

    /// Return all committed tablet descriptors for one table.
    ///
    /// The returned descriptors are detached from the state machine so a
    /// caller can validate completeness and publish an immutable routing view
    /// without holding any metadata-state borrow across execution work.
    pub fn tablets_for_table(&self, table_id: TableId) -> Vec<TabletDescriptor> {
        self.tablets
            .values()
            .filter(|tablet| tablet.table_id == table_id)
            .cloned()
            .collect()
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
        self.apply_command(command)
    }

    /// Apply one request-bearing metadata command with durable retry
    /// deduplication.
    ///
    /// The request identity is checked before allocation. An exact replay
    /// returns the cached original outcome, while a different request identity
    /// still evaluates normal uniqueness and allocation rules independently.
    pub fn apply_with_request_id(
        &mut self,
        request_id: RequestId,
        command: MetadataCommand,
    ) -> MetadataApplyOutcome {
        let logical_command_id = compatibility_metadata_logical_id(&request_id);
        self.apply_with_logical_command_id(logical_command_id, command)
    }

    /// Apply a request using the topology-independent identity carried by the
    /// metadata envelope. Proposal/RPC correlation remains separate so a
    /// reconnect with the same sequence in a newer session cannot collide with
    /// an older durable command.
    pub fn apply_with_logical_command_id(
        &mut self,
        logical_command_id: LogicalCommandId,
        command: MetadataCommand,
    ) -> MetadataApplyOutcome {
        if let Some(outcome) = self.request_deduplication.get(&logical_command_id) {
            return metadata_outcome_from_cached(outcome);
        }

        let outcome = self.apply_command(command);

        self.request_deduplication
            .insert(logical_command_id, metadata_outcome_to_cached(&outcome));

        outcome
    }

    fn apply_command(&mut self, command: MetadataCommand) -> MetadataApplyOutcome {
        if let Err(error) = command.validate() {
            return MetadataApplyOutcome::Rejected(MetadataRejection::InvalidCommand(
                error.to_string(),
            ));
        }

        match command {
            MetadataCommand::ClusterInitialized { cluster_id } => {
                self.apply_cluster_initialized(cluster_id)
            }

            MetadataCommand::RegisterClient {
                client_id,
                requested_session_epoch,
            } => self.apply_register_client(client_id, requested_session_epoch),

            MetadataCommand::RenewClient {
                client_id,
                session_epoch,
                acknowledged_through,
            } => self.apply_renew_client(client_id, session_epoch, acknowledged_through),

            MetadataCommand::RegisterNode(node) => self.apply_register_node(node),

            MetadataCommand::CreateTable { table } => self.apply_create_table(table),

            MetadataCommand::CreateTablet { tablet } => self.apply_create_tablet(tablet),

            MetadataCommand::CreateTableTopology(request) => {
                self.apply_create_table_topology(request)
            }

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

            allocator: self.allocator,

            request_deduplication: self
                .request_deduplication
                .iter()
                .map(
                    |(logical_command_id, outcome)| MetadataRequestDeduplication {
                        request_id: request_id_for_logical_id(logical_command_id),
                        logical_command_id: *logical_command_id,
                        outcome: outcome.clone(),
                    },
                )
                .collect(),

            client_sessions: self.client_sessions.values().cloned().collect(),
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
            allocator,
            request_deduplication,
            client_sessions,
        } = snapshot;

        if cluster_id.is_none() {
            let mut state = Self::new();
            state.allocator = allocator;
            return Ok(state);
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

        // Snapshot validation proved that these high-water marks dominate all
        // visible identities. Install them only after the semantic projection
        // has been reconstructed successfully.
        state.allocator = allocator;

        for request in request_deduplication {
            if let MetadataCachedOutcome::TableCreated {
                table_id,
                tablet_id,
                raft_group_id,
            } = request.outcome
            {
                let Some(table) = state.tables.get(&table_id) else {
                    return Err(Error::CorruptData(format!(
                        "metadata snapshot request cache references unknown table {}",
                        table_id.0,
                    )));
                };

                let Some(tablet) = state.tablets.get(&tablet_id) else {
                    return Err(Error::CorruptData(format!(
                        "metadata snapshot request cache references unknown tablet {}",
                        tablet_id.0,
                    )));
                };

                if tablet.table_id != table_id || tablet.raft_group_id != raft_group_id {
                    return Err(Error::CorruptData(format!(
                        "metadata snapshot request cache has inconsistent CREATE TABLE identities for table {}",
                        table.id.0,
                    )));
                }

                state.request_deduplication.insert(
                    request.logical_command_id,
                    MetadataCachedOutcome::TableCreated {
                        table_id,
                        tablet_id,
                        raft_group_id,
                    },
                );
            } else {
                state
                    .request_deduplication
                    .insert(request.logical_command_id, request.outcome);
            }
        }

        for session in client_sessions {
            if state
                .client_sessions
                .insert(session.client_id, session)
                .is_some()
            {
                return Err(Error::CorruptData(
                    "metadata snapshot contains duplicate client sessions".to_string(),
                ));
            }
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

    fn apply_register_client(
        &mut self,
        client_id: u128,
        requested_session_epoch: u64,
    ) -> MetadataApplyOutcome {
        if let Err(rejection) = self.require_initialized() {
            return MetadataApplyOutcome::Rejected(rejection);
        }

        let next_epoch = match self.client_sessions.get(&client_id) {
            None => requested_session_epoch.max(1),
            Some(existing) if requested_session_epoch == existing.session_epoch => {
                return MetadataApplyOutcome::AlreadyApplied;
            }
            Some(existing)
                if requested_session_epoch != 0
                    && requested_session_epoch < existing.session_epoch =>
            {
                return MetadataApplyOutcome::Rejected(
                    MetadataRejection::ClientSessionEpochConflict {
                        client_id,
                        existing: existing.session_epoch,
                        received: requested_session_epoch,
                    },
                );
            }
            Some(existing) => match existing.session_epoch.checked_add(1) {
                Some(epoch) if requested_session_epoch == 0 || requested_session_epoch == epoch => {
                    epoch
                }
                Some(epoch) => {
                    return MetadataApplyOutcome::Rejected(
                        MetadataRejection::ClientSessionEpochConflict {
                            client_id,
                            existing: epoch - 1,
                            received: requested_session_epoch,
                        },
                    );
                }
                None => {
                    return MetadataApplyOutcome::Rejected(
                        MetadataRejection::ClientSessionEpochExhausted(client_id),
                    );
                }
            },
        };

        self.client_sessions.insert(
            client_id,
            MetadataClientSession {
                client_id,
                session_epoch: next_epoch,
                acknowledged_through: 0,
                first_retained_sequence: 1,
            },
        );

        MetadataApplyOutcome::ClientRegistered {
            client_id,
            session_epoch: next_epoch,
        }
    }

    fn apply_renew_client(
        &mut self,
        client_id: u128,
        session_epoch: u64,
        acknowledged_through: u64,
    ) -> MetadataApplyOutcome {
        if let Err(rejection) = self.require_initialized() {
            return MetadataApplyOutcome::Rejected(rejection);
        }

        let Some(session) = self.client_sessions.get_mut(&client_id) else {
            return MetadataApplyOutcome::Rejected(MetadataRejection::UnknownClient(client_id));
        };
        if session.session_epoch != session_epoch {
            return MetadataApplyOutcome::Rejected(MetadataRejection::ClientSessionEpochConflict {
                client_id,
                existing: session.session_epoch,
                received: session_epoch,
            });
        }
        if acknowledged_through < session.acknowledged_through {
            return MetadataApplyOutcome::Rejected(MetadataRejection::AcknowledgementRegression {
                client_id,
                existing: session.acknowledged_through,
                received: acknowledged_through,
            });
        }
        if acknowledged_through == session.acknowledged_through {
            return MetadataApplyOutcome::AlreadyApplied;
        }

        let first_retained_sequence = match acknowledged_through.checked_add(1) {
            Some(sequence) => sequence,
            None => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::RetryHorizonExhausted(
                    client_id,
                ));
            }
        };
        session.acknowledged_through = acknowledged_through;
        session.first_retained_sequence = first_retained_sequence;
        MetadataApplyOutcome::ClientRenewed
    }

    fn apply_register_node(&mut self, node: NodeDescriptor) -> MetadataApplyOutcome {
        if let Err(rejection) = self.require_initialized() {
            return MetadataApplyOutcome::Rejected(rejection);
        }

        if let Some(existing) = self.nodes.get(&node.node_id) {
            return if existing == &node {
                MetadataApplyOutcome::AlreadyApplied
            } else if same_node_directory(existing, &node)
                && lifecycle_can_advance(existing.lifecycle, node.lifecycle)
            {
                // Lifecycle is metadata-owned placement intent. Updating it
                // must preserve the stable directory identity so a draining
                // node cannot be accidentally reintroduced under a new
                // endpoint record.
                self.nodes.insert(node.node_id, node);
                MetadataApplyOutcome::Applied
            } else if same_node_directory(existing, &node) {
                MetadataApplyOutcome::Rejected(MetadataRejection::InvalidNodeLifecycleTransition {
                    node_id: node.node_id,
                    from: node_lifecycle_name(existing.lifecycle),
                    to: node_lifecycle_name(node.lifecycle),
                })
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

    fn apply_create_table_topology(&mut self, request: CreateTableRequest) -> MetadataApplyOutcome {
        if let Err(rejection) = self.require_initialized() {
            return MetadataApplyOutcome::Rejected(rejection);
        }

        // Duplicate-name rejection is intentionally before allocation. A
        // rejected CREATE TABLE must not consume cluster-global identities.
        if self.table_ids_by_name.contains_key(&request.table_name) {
            return MetadataApplyOutcome::Rejected(MetadataRejection::TableNameConflict(
                request.table_name,
            ));
        }

        let Some((selected_nodes, placement_policy)) = select_initial_placement(&self.nodes) else {
            return MetadataApplyOutcome::Rejected(MetadataRejection::NoRegisteredNodes);
        };

        if selected_nodes.is_empty() {
            return MetadataApplyOutcome::Rejected(MetadataRejection::NoRegisteredNodes);
        }

        // Calculate every identity before mutating state. Checked arithmetic
        // turns an exhausted identity space into a deterministic rejection.
        let table_id = match self.allocator.max_table_id.checked_add(1) {
            Some(id) => TableId(id),
            None => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::IdentitySpaceExhausted(
                    "table",
                ));
            }
        };

        let tablet_id = match self.allocator.max_tablet_id.checked_add(1) {
            Some(id) => TabletId(id),
            None => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::IdentitySpaceExhausted(
                    "tablet",
                ));
            }
        };

        let raft_group_id = match self.allocator.max_raft_group_id.checked_add(1) {
            Some(id) => RaftGroupId(id),
            None => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::IdentitySpaceExhausted(
                    "Raft group",
                ));
            }
        };

        let table_definition = TableDefinition {
            table_id: table_id.0,
            name: request.table_name,
            columns: request.columns,
            primary_key_column_ids: request.primary_key_column_ids,
            schema_version: 1,
            tablet_count: INITIAL_TABLET_COUNT,
        };

        let schema = match TableSchema::from_definition(table_definition) {
            Ok(schema) => schema,
            Err(error) => {
                return MetadataApplyOutcome::Rejected(MetadataRejection::InvalidTable(
                    error.to_string(),
                ));
            }
        };

        let tablet = TabletDescriptor {
            tablet_id,
            table_id,
            raft_group_id,
            tablet_epoch: 1,
            partition: PartitionSpec::Range {
                start_key: Vec::new(),
                end_key: Vec::new(),
            },
        };

        if let Err(error) = tablet.validate() {
            return MetadataApplyOutcome::Rejected(MetadataRejection::InvalidCommand(
                error.to_string(),
            ));
        }

        let placement = DesiredReplicaPlacement {
            tablet_id,
            configuration_epoch: 1,
            placement_policy,
            replicas: selected_nodes
                .into_iter()
                .enumerate()
                .map(|(index, node_id)| DesiredReplica {
                    replica_id: ReplicaId((index as u64) + 1),
                    node_id,
                    role: DesiredReplicaRole::Voter,
                })
                .collect(),
        };

        if let Err(error) = placement.validate() {
            return MetadataApplyOutcome::Rejected(MetadataRejection::InvalidCommand(
                error.to_string(),
            ));
        }

        // An occupied identity would indicate state corruption because all
        // successful legacy and topology creations advance these high-water
        // marks. Do not partially publish if that invariant is violated.
        if self.tables.contains_key(&table_id)
            || self.tablets.contains_key(&tablet_id)
            || self.tablet_ids_by_raft_group.contains_key(&raft_group_id)
        {
            return MetadataApplyOutcome::Rejected(MetadataRejection::InvalidCommand(
                "metadata allocator produced an occupied identity".to_string(),
            ));
        }

        // Atomic publication boundary: every operation below is an infallible
        // ordered-map insertion, so observers cannot see a table without its
        // tablet and desired placement.
        let table_name = schema.name.clone();

        self.tables.insert(table_id, Arc::new(schema));
        self.table_ids_by_name.insert(table_name, table_id);
        self.tablet_ids_by_raft_group
            .insert(raft_group_id, tablet_id);
        self.tablets.insert(tablet_id, tablet);
        self.desired_placements.insert(tablet_id, placement);

        self.allocator.max_table_id = table_id.0;
        self.allocator.max_tablet_id = tablet_id.0;
        self.allocator.max_raft_group_id = raft_group_id.0;

        MetadataApplyOutcome::TableCreated(MetadataTableCreated {
            table_id,
            tablet_id,
            raft_group_id,
        })
    }

    fn apply_create_table(&mut self, table: TableDefinition) -> MetadataApplyOutcome {
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

        self.allocator.max_table_id = self.allocator.max_table_id.max(table_id.0);

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

        match &tablet.partition {
            PartitionSpec::Hash {
                bucket,
                bucket_count,
            } => {
                if *bucket_count != table.tablet_count {
                    return MetadataApplyOutcome::Rejected(
                        MetadataRejection::PartitionCountMismatch {
                            table_id: tablet.table_id,
                            expected: table.tablet_count,
                            received: *bucket_count,
                        },
                    );
                }

                if self
                    .tablet_ids_by_partition
                    .contains_key(&(tablet.table_id, *bucket))
                {
                    return MetadataApplyOutcome::Rejected(MetadataRejection::PartitionConflict {
                        table_id: tablet.table_id,
                        bucket: *bucket,
                    });
                }

                let existing_count = self
                    .tablets
                    .values()
                    .filter(|existing| existing.table_id == tablet.table_id)
                    .count();
                if existing_count >= table.tablet_count as usize {
                    return MetadataApplyOutcome::Rejected(
                        MetadataRejection::TabletCountExceeded {
                            table_id: tablet.table_id,
                            limit: table.tablet_count,
                        },
                    );
                }
            }
            PartitionSpec::Range { start_key, end_key } => {
                let overlaps = self.tablets.values().filter(|existing| {
                    existing.table_id == tablet.table_id
                        && match &existing.partition {
                            PartitionSpec::Range {
                                start_key: existing_start,
                                end_key: existing_end,
                            } => ranges_overlap(start_key, end_key, existing_start, existing_end),
                            PartitionSpec::Hash { .. } => false,
                        }
                });
                if overlaps.count() != 0 {
                    return MetadataApplyOutcome::Rejected(MetadataRejection::InvalidCommand(
                        "tablet key range overlaps an existing range".to_string(),
                    ));
                }
            }
        }

        let tablet_id = tablet.tablet_id;
        let raft_group_id = tablet.raft_group_id;
        let table_id = tablet.table_id;

        self.tablet_ids_by_raft_group
            .insert(raft_group_id, tablet_id);

        if let PartitionSpec::Hash { bucket, .. } = &tablet.partition {
            self.tablet_ids_by_partition
                .insert((table_id, *bucket), tablet_id);
        }

        self.tablets.insert(tablet_id, tablet);

        self.allocator.max_tablet_id = self.allocator.max_tablet_id.max(tablet_id.0);
        self.allocator.max_raft_group_id = self.allocator.max_raft_group_id.max(raft_group_id.0);

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
            let Some(node) = self.nodes.get(&replica.node_id) else {
                return MetadataApplyOutcome::Rejected(MetadataRejection::UnknownNode(
                    replica.node_id,
                ));
            };

            if node.lifecycle != NodeLifecycle::Active {
                return MetadataApplyOutcome::Rejected(MetadataRejection::NodeNotEligible {
                    node_id: replica.node_id,
                    lifecycle: node_lifecycle_name(node.lifecycle),
                });
            }

            if let Some(required) = placement.placement_policy.required_storage_class.as_ref()
                && &node.storage_class != required
            {
                return MetadataApplyOutcome::Rejected(
                    MetadataRejection::PlacementStorageClassMismatch {
                        node_id: replica.node_id,
                        required: required.clone(),
                        received: node.storage_class.clone(),
                    },
                );
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

        for preferred_node_id in &placement.placement_policy.preferred_leader_nodes {
            let Some(node) = self.nodes.get(preferred_node_id) else {
                return MetadataApplyOutcome::Rejected(MetadataRejection::UnknownNode(
                    *preferred_node_id,
                ));
            };

            if node.lifecycle != NodeLifecycle::Active {
                return MetadataApplyOutcome::Rejected(MetadataRejection::NodeNotEligible {
                    node_id: *preferred_node_id,
                    lifecycle: node_lifecycle_name(node.lifecycle),
                });
            }
        }

        for (domain, required, observed) in [
            (
                "regions",
                placement.placement_policy.min_distinct_regions,
                placement
                    .replicas
                    .iter()
                    .filter_map(|replica| self.nodes[&replica.node_id].region.as_deref())
                    .collect::<BTreeSet<_>>()
                    .len(),
            ),
            (
                "zones",
                placement.placement_policy.min_distinct_zones,
                placement
                    .replicas
                    .iter()
                    .filter_map(|replica| self.nodes[&replica.node_id].zone.as_deref())
                    .collect::<BTreeSet<_>>()
                    .len(),
            ),
            (
                "racks",
                placement.placement_policy.min_distinct_racks,
                placement
                    .replicas
                    .iter()
                    .filter_map(|replica| self.nodes[&replica.node_id].rack.as_deref())
                    .collect::<BTreeSet<_>>()
                    .len(),
            ),
        ] {
            if required > 0 && observed < required as usize {
                return MetadataApplyOutcome::Rejected(
                    MetadataRejection::PlacementDomainUnsatisfied {
                        domain,
                        required,
                        observed,
                    },
                );
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

/// Select the initial voter set deterministically while spreading replicas
/// across the locality labels that metadata actually knows. Empty labels are
/// never counted as a diversity guarantee: placement must not claim failure
/// isolation that bootstrap configuration did not provide.
fn select_initial_placement(
    nodes: &BTreeMap<NodeId, NodeDescriptor>,
) -> Option<(Vec<NodeId>, PlacementPolicy)> {
    let mut candidates = nodes
        .values()
        .filter(|node| node.lifecycle == NodeLifecycle::Active)
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return None;
    }

    let target_count = candidates.len().min(INITIAL_REPLICATION_FACTOR);
    let mut selected = Vec::with_capacity(target_count);
    while selected.len() < target_count {
        let best_index = (0..candidates.len()).max_by(|left, right| {
            let left_score = locality_gain(candidates[*left], &selected, nodes);
            let right_score = locality_gain(candidates[*right], &selected, nodes);
            left_score
                .cmp(&right_score)
                .then_with(|| candidates[*right].node_id.cmp(&candidates[*left].node_id))
        })?;
        selected.push(candidates.swap_remove(best_index).node_id);
    }

    let selected_nodes = selected
        .iter()
        .filter_map(|node_id| nodes.get(node_id))
        .collect::<Vec<_>>();
    let distinct = |value: fn(&NodeDescriptor) -> Option<&str>| {
        selected_nodes
            .iter()
            .filter_map(|node| value(node))
            .collect::<BTreeSet<_>>()
            .len() as u32
    };

    Some((
        selected,
        PlacementPolicy {
            replication_factor: target_count as u32,
            min_distinct_regions: distinct(|node| node.region.as_deref()),
            min_distinct_zones: distinct(|node| node.zone.as_deref()),
            min_distinct_racks: distinct(|node| node.rack.as_deref()),
            required_storage_class: None,
            preferred_leader_nodes: Vec::new(),
        },
    ))
}

fn locality_gain(
    candidate: &NodeDescriptor,
    selected: &[NodeId],
    nodes: &BTreeMap<NodeId, NodeDescriptor>,
) -> usize {
    let selected_nodes = selected.iter().filter_map(|node_id| nodes.get(node_id));
    let regions = selected_nodes
        .clone()
        .filter_map(|node| node.region.as_deref())
        .collect::<BTreeSet<_>>();
    let zones = selected_nodes
        .clone()
        .filter_map(|node| node.zone.as_deref())
        .collect::<BTreeSet<_>>();
    let racks = selected_nodes
        .filter_map(|node| node.rack.as_deref())
        .collect::<BTreeSet<_>>();
    usize::from(
        candidate
            .region
            .as_deref()
            .is_some_and(|value| !regions.contains(value)),
    ) + usize::from(
        candidate
            .zone
            .as_deref()
            .is_some_and(|value| !zones.contains(value)),
    ) + usize::from(
        candidate
            .rack
            .as_deref()
            .is_some_and(|value| !racks.contains(value)),
    )
}

/// Return whether two half-open ranges overlap. Empty starts/ends represent
/// negative/positive infinity respectively, matching the metadata wire model.
fn ranges_overlap(
    left_start: &[u8],
    left_end: &[u8],
    right_start: &[u8],
    right_end: &[u8],
) -> bool {
    let left_before_right =
        !left_end.is_empty() && !right_start.is_empty() && left_end <= right_start;
    let right_before_left =
        !right_end.is_empty() && !left_start.is_empty() && right_end <= left_start;
    !(left_before_right || right_before_left)
}

fn compatibility_metadata_logical_id(request_id: &RequestId) -> LogicalCommandId {
    LogicalCommandId {
        client_request_id: ClientRequestId {
            client_id: request_id.client_id,
            session_epoch: 1,
            request_sequence: request_id.sequence,
        },
        command_ordinal: 1,
        kind: CommandKind::Catalog,
    }
}

fn request_id_for_logical_id(logical_command_id: &LogicalCommandId) -> RequestId {
    RequestId {
        client_id: logical_command_id.client_request_id.client_id,
        sequence: logical_command_id.client_request_id.request_sequence,
        raft_group_id: ragnordb_common::metadata_codec::RESERVED_METADATA_RAFT_GROUP_ID,
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

fn metadata_outcome_to_cached(outcome: &MetadataApplyOutcome) -> MetadataCachedOutcome {
    match outcome {
        MetadataApplyOutcome::Applied => MetadataCachedOutcome::Applied,
        MetadataApplyOutcome::AlreadyApplied => MetadataCachedOutcome::AlreadyApplied,
        MetadataApplyOutcome::ClientRegistered {
            client_id,
            session_epoch,
        } => MetadataCachedOutcome::ClientRegistered {
            client_id: *client_id,
            session_epoch: *session_epoch,
        },
        MetadataApplyOutcome::ClientRenewed => MetadataCachedOutcome::ClientRenewed,
        MetadataApplyOutcome::TableCreated(created) => MetadataCachedOutcome::TableCreated {
            table_id: created.table_id,
            tablet_id: created.tablet_id,
            raft_group_id: created.raft_group_id,
        },
        MetadataApplyOutcome::Rejected(rejection) => {
            MetadataCachedOutcome::Rejected(rejection.to_string())
        }
    }
}

fn metadata_outcome_from_cached(outcome: &MetadataCachedOutcome) -> MetadataApplyOutcome {
    match outcome {
        MetadataCachedOutcome::Applied => MetadataApplyOutcome::Applied,
        MetadataCachedOutcome::AlreadyApplied => MetadataApplyOutcome::AlreadyApplied,
        MetadataCachedOutcome::ClientRegistered {
            client_id,
            session_epoch,
        } => MetadataApplyOutcome::ClientRegistered {
            client_id: *client_id,
            session_epoch: *session_epoch,
        },
        MetadataCachedOutcome::ClientRenewed => MetadataApplyOutcome::ClientRenewed,
        MetadataCachedOutcome::TableCreated {
            table_id,
            tablet_id,
            raft_group_id,
        } => MetadataApplyOutcome::TableCreated(MetadataTableCreated {
            table_id: *table_id,
            tablet_id: *tablet_id,
            raft_group_id: *raft_group_id,
        }),
        MetadataCachedOutcome::Rejected(reason) => {
            MetadataApplyOutcome::Rejected(MetadataRejection::InvalidCommand(reason.clone()))
        }
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

fn same_node_directory(left: &NodeDescriptor, right: &NodeDescriptor) -> bool {
    left.node_id == right.node_id
        && left.raft_addr == right.raft_addr
        && left.snapshot_addr == right.snapshot_addr
        && left.sql_addr == right.sql_addr
        && left.admin_addr == right.admin_addr
        && left.region == right.region
        && left.zone == right.zone
        && left.rack == right.rack
        && left.storage_class == right.storage_class
}

fn lifecycle_can_advance(from: NodeLifecycle, to: NodeLifecycle) -> bool {
    lifecycle_rank(to) >= lifecycle_rank(from)
}

const fn lifecycle_rank(lifecycle: NodeLifecycle) -> u8 {
    match lifecycle {
        NodeLifecycle::Active => 0,
        NodeLifecycle::Draining => 1,
        NodeLifecycle::Decommissioning => 2,
        NodeLifecycle::Decommissioned => 3,
        NodeLifecycle::Tombstoned => 4,
    }
}

fn node_lifecycle_name(lifecycle: NodeLifecycle) -> &'static str {
    match lifecycle {
        NodeLifecycle::Active => "active",
        NodeLifecycle::Draining => "draining",
        NodeLifecycle::Decommissioning => "decommissioning",
        NodeLifecycle::Decommissioned => "decommissioned",
        NodeLifecycle::Tombstoned => "tombstoned",
    }
}

fn apply_snapshot_command(state: &mut MetadataState, command: MetadataCommand) -> Result<()> {
    match state.apply(command) {
        MetadataApplyOutcome::Applied
        | MetadataApplyOutcome::AlreadyApplied
        | MetadataApplyOutcome::ClientRegistered { .. }
        | MetadataApplyOutcome::ClientRenewed
        | MetadataApplyOutcome::TableCreated(_) => Ok(()),

        MetadataApplyOutcome::Rejected(rejection) => Err(Error::CorruptData(format!(
            "metadata snapshot violates state-machine invariant: {rejection}"
        ))),
    }
}
