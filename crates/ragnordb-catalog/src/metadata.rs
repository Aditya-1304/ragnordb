//! Deterministic metadata state owned by the metadata Raft group.
//!
//! The state is intentionally free of leader caches and Raft `ConfState`.
//! Those values have different authorities and must not be reconstructed from
//! metadata command replay.

use std::collections::BTreeMap;

use ragnordb_common::{
    Error, Result,
    ids::{NodeId, RaftGroupId, TableId, TabletId},
    metadata_codec::{DesiredReplicaPlacement, MetadataCommand, NodeDescriptor, TabletDescriptor},
};

use crate::TableSchema;

/// Replicated metadata projection. Only committed commands may mutate it.
#[derive(Debug, Default)]
pub struct MetadataState {
    cluster_id: Option<String>,
    nodes: BTreeMap<NodeId, NodeDescriptor>,
    tables: BTreeMap<TableId, TableSchema>,
    tablets: BTreeMap<TabletId, TabletDescriptor>,
    tablet_ids_by_raft_group: BTreeMap<RaftGroupId, TabletId>,
    desired_placements: BTreeMap<TabletId, DesiredReplicaPlacement>,
}

/// Whether a committed command changed the projection or was an exact replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataApplyOutcome {
    Applied,
    AlreadyApplied,
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

    pub fn table(&self, table_id: TableId) -> Option<&TableSchema> {
        self.tables.get(&table_id)
    }

    pub fn tablet(&self, tablet_id: TabletId) -> Option<&TabletDescriptor> {
        self.tablets.get(&tablet_id)
    }

    pub fn desired_placement(&self, tablet_id: TabletId) -> Option<&DesiredReplicaPlacement> {
        self.desired_placements.get(&tablet_id)
    }

    /// Applies one already-committed command.
    ///
    /// Exact command replays are harmless. A conflicting record for an
    /// established identity is rejected instead of being partially merged, so
    /// live application and recovery replay retain the same projection.
    pub fn apply(&mut self, command: MetadataCommand) -> Result<MetadataApplyOutcome> {
        command
            .validate()
            .map_err(|error| Error::ConstraintViolation(error.to_string()))?;

        match command {
            MetadataCommand::ClusterInitialized { cluster_id } => {
                self.apply_cluster_initialized(cluster_id)
            }
            MetadataCommand::RegisterNode(node) => self.apply_register_node(node),
            MetadataCommand::CreateTable { table } => self.apply_create_table(table),
            MetadataCommand::CreateTablet { tablet } => self.apply_create_tablet(tablet),
            MetadataCommand::SetDesiredReplicaPlacement(placement) => {
                self.apply_desired_replica_placement(placement)
            }
            MetadataCommand::UpdateTableSchema {
                expected_schema_version,
                table,
            } => self.apply_table_schema_update(expected_schema_version, table),
        }
    }

    fn apply_cluster_initialized(&mut self, cluster_id: String) -> Result<MetadataApplyOutcome> {
        match &self.cluster_id {
            None => {
                self.cluster_id = Some(cluster_id);
                Ok(MetadataApplyOutcome::Applied)
            }
            Some(existing) if existing == &cluster_id => Ok(MetadataApplyOutcome::AlreadyApplied),
            Some(existing) => Err(Error::ConstraintViolation(format!(
                "metadata is already initialized for cluster {existing}"
            ))),
        }
    }

    fn apply_register_node(&mut self, node: NodeDescriptor) -> Result<MetadataApplyOutcome> {
        self.require_initialized()?;

        match self.nodes.get(&node.node_id) {
            None => {
                self.nodes.insert(node.node_id, node);
                Ok(MetadataApplyOutcome::Applied)
            }
            Some(existing) if existing == &node => Ok(MetadataApplyOutcome::AlreadyApplied),
            Some(_) => Err(Error::ConstraintViolation(format!(
                "node {} is already registered with a different directory record",
                node.node_id.0
            ))),
        }
    }

    fn apply_create_table(
        &mut self,
        table: ragnordb_common::catalog_codec::TableDefinition,
    ) -> Result<MetadataApplyOutcome> {
        self.require_initialized()?;
        let schema = TableSchema::from_definition(table)?;

        if let Some(existing) = self.tables.get(&schema.id) {
            return if existing == &schema {
                Ok(MetadataApplyOutcome::AlreadyApplied)
            } else {
                Err(Error::ConstraintViolation(format!(
                    "table ID {} is already assigned to {}",
                    schema.id.0, existing.name
                )))
            };
        }

        if let Some(existing) = self
            .tables
            .values()
            .find(|existing| existing.name == schema.name)
        {
            return Err(Error::ConstraintViolation(format!(
                "table name {} is already assigned to table ID {}",
                schema.name, existing.id.0
            )));
        }

        self.tables.insert(schema.id, schema);
        Ok(MetadataApplyOutcome::Applied)
    }

    fn apply_create_tablet(&mut self, tablet: TabletDescriptor) -> Result<MetadataApplyOutcome> {
        self.require_initialized()?;

        let table = self.tables.get(&tablet.table_id).ok_or_else(|| {
            Error::ConstraintViolation(format!(
                "tablet {} references unknown table {}",
                tablet.tablet_id.0, tablet.table_id.0
            ))
        })?;
        if tablet.schema_version != table.schema_version {
            return Err(Error::SchemaMismatch(format!(
                "tablet {} has schema version {} but table {} is at version {}",
                tablet.tablet_id.0, tablet.schema_version, table.id.0, table.schema_version
            )));
        }

        if let Some(existing) = self.tablets.get(&tablet.tablet_id) {
            return if existing == &tablet {
                Ok(MetadataApplyOutcome::AlreadyApplied)
            } else {
                Err(Error::ConstraintViolation(format!(
                    "tablet ID {} is already assigned to Raft group {}",
                    tablet.tablet_id.0, existing.raft_group_id.0
                )))
            };
        }

        if let Some(existing_tablet_id) = self.tablet_ids_by_raft_group.get(&tablet.raft_group_id) {
            return Err(Error::ConstraintViolation(format!(
                "Raft group {} is already assigned to tablet {}",
                tablet.raft_group_id.0, existing_tablet_id.0
            )));
        }

        let tablet_count = self
            .tablets
            .values()
            .filter(|existing| existing.table_id == tablet.table_id)
            .count();
        if tablet_count >= usize::try_from(table.tablet_count).expect("u32 fits usize") {
            return Err(Error::ConstraintViolation(format!(
                "table {} already has its configured {} tablets",
                table.id.0, table.tablet_count
            )));
        }

        self.tablet_ids_by_raft_group
            .insert(tablet.raft_group_id, tablet.tablet_id);
        self.tablets.insert(tablet.tablet_id, tablet);
        Ok(MetadataApplyOutcome::Applied)
    }

    fn apply_desired_replica_placement(
        &mut self,
        placement: DesiredReplicaPlacement,
    ) -> Result<MetadataApplyOutcome> {
        self.require_initialized()?;
        if !self.tablets.contains_key(&placement.tablet_id) {
            return Err(Error::ConstraintViolation(format!(
                "desired placement references unknown tablet {}",
                placement.tablet_id.0
            )));
        }
        for replica in &placement.replicas {
            if !self.nodes.contains_key(&replica.node_id) {
                return Err(Error::ConstraintViolation(format!(
                    "desired replica {} references unknown node {}",
                    replica.replica_id.0, replica.node_id.0
                )));
            }
        }

        match self.desired_placements.get(&placement.tablet_id) {
            None => {
                self.desired_placements
                    .insert(placement.tablet_id, placement);
                Ok(MetadataApplyOutcome::Applied)
            }
            Some(existing) if existing == &placement => Ok(MetadataApplyOutcome::AlreadyApplied),
            Some(existing) if placement.configuration_epoch <= existing.configuration_epoch => {
                Err(Error::ConstraintViolation(format!(
                    "tablet {} placement epoch {} does not advance current epoch {}",
                    placement.tablet_id.0,
                    placement.configuration_epoch,
                    existing.configuration_epoch
                )))
            }
            Some(_) => {
                self.desired_placements
                    .insert(placement.tablet_id, placement);
                Ok(MetadataApplyOutcome::Applied)
            }
        }
    }

    fn apply_table_schema_update(
        &mut self,
        expected_schema_version: u64,
        table: ragnordb_common::catalog_codec::TableDefinition,
    ) -> Result<MetadataApplyOutcome> {
        self.require_initialized()?;
        let updated = TableSchema::from_definition(table)?;
        let existing = self.tables.get(&updated.id).ok_or_else(|| {
            Error::ConstraintViolation(format!(
                "schema update references unknown table {}",
                updated.id.0
            ))
        })?;

        if existing == &updated {
            return Ok(MetadataApplyOutcome::AlreadyApplied);
        }
        if updated.name != existing.name {
            return Err(Error::SchemaMismatch(format!(
                "schema update for table {} cannot rename it from {} to {}",
                updated.id.0, existing.name, updated.name
            )));
        }
        if updated.tablet_count != existing.tablet_count {
            return Err(Error::SchemaMismatch(format!(
                "schema update for table {} cannot change tablet count",
                updated.id.0
            )));
        }
        if expected_schema_version != existing.schema_version {
            return Err(Error::SchemaMismatch(format!(
                "schema update for table {} expected version {} but current version is {}",
                updated.id.0, expected_schema_version, existing.schema_version
            )));
        }
        let next_schema_version = existing.schema_version.checked_add(1).ok_or_else(|| {
            Error::ConstraintViolation(format!(
                "schema version space is exhausted for table {}",
                updated.id.0
            ))
        })?;
        if updated.schema_version != next_schema_version {
            return Err(Error::SchemaMismatch(format!(
                "schema update for table {} must advance from version {} to {}",
                updated.id.0, existing.schema_version, next_schema_version
            )));
        }

        self.tables.insert(updated.id, updated);
        Ok(MetadataApplyOutcome::Applied)
    }

    fn require_initialized(&self) -> Result<()> {
        if self.cluster_id.is_none() {
            return Err(Error::Configuration(
                "metadata command requires committed cluster initialization".to_string(),
            ));
        }
        Ok(())
    }
}
