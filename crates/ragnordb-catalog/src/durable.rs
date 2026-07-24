//! Single node durable catalog publication.
//!
//! A catalog writer validates one complete metadata operation, appends its
//! semantic durable record, and only then publishes the immutable schema
//! snapshot to readers

use std::sync::{Arc, Mutex};

use ragnordb_common::{
    Error, Result,
    command_codec::{CatalogCommand, CatalogOperation, CreateTableOperation},
    ids::{ColumnId, TableId, Timestamp},
};

use crate::{ColumnSchema, MemoryCatalog, TableSchema};

/// storage independent logical extent of one durable catalog update
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogLogExtent {
    pub start_lsn: u64,
    pub end_lsn: u64,
}

/// semantic catalog record supplied to the storage WAL adapter
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogLogRecord {
    pub table_id: TableId,
    pub update_timestamp: Timestamp,
    pub command: CatalogCommand,
}

/// durable catalog log boundary
///
/// the catalog crate owns validation and publication. Storage owns protobuf
/// encoding, A-WAL record identities, physical framing, and synchronization
pub trait DurableCatalogLog {
    fn append_catalog_update(&self, update: &CatalogLogRecord) -> Result<CatalogLogExtent>;
}

impl<T> DurableCatalogLog for Arc<T>
where
    T: DurableCatalogLog + ?Sized,
{
    fn append_catalog_update(&self, update: &CatalogLogRecord) -> Result<CatalogLogExtent> {
        (**self).append_catalog_update(update)
    }
}

impl<T> DurableCatalogLog for Mutex<T>
where
    T: DurableCatalogLog,
{
    fn append_catalog_update(&self, update: &CatalogLogRecord) -> Result<CatalogLogExtent> {
        self.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .append_catalog_update(update)
    }
}

/// published result of one durable table-creation operation
#[must_use = "catalog creation outcomes contain durable publication metadata"]
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogCreateOutcome {
    pub schema: Arc<TableSchema>,
    pub update_timestamp: Timestamp,
    pub wal_extent: CatalogLogExtent,
}

/// serialized single-node catalog writer
///
/// The coordinator owns the mutable catalog and does not expose mutable access.
/// Consequently, no second writer can change allocation or uniqueness state
/// between validation, WAL synchronization, and publication
pub struct DurableCatalog<L>
where
    L: DurableCatalogLog,
{
    catalog: MemoryCatalog,
    durable_log: L,
    recovery_required_reason: Option<String>,
}

impl<L> DurableCatalog<L>
where
    L: DurableCatalogLog,
{
    pub fn new(durable_log: L) -> Self {
        Self {
            catalog: MemoryCatalog::new(),
            durable_log,
            recovery_required_reason: None,
        }
    }

    /// construct the durable writer around a fully recovered catalog
    ///
    /// the supplied catalog must come from completed recovery or a validated
    /// snapshot. No catalog operation can bypass the durable log after this
    /// ownership transfer
    pub fn from_recovered(catalog: MemoryCatalog, durable_log: L) -> Self {
        Self {
            catalog,
            durable_log,
            recovery_required_reason: None,
        }
    }

    /// Borrow the published catalog snapshot used by SQL analysis and reads.
    pub fn catalog(&self) -> &MemoryCatalog {
        &self.catalog
    }

    pub fn requires_recovery(&self) -> bool {
        self.recovery_required_reason.is_some()
    }

    /// Validate, durably append, and publish one local table definition.
    pub fn create_table<F>(
        &mut self,
        name: impl Into<String>,
        columns: Vec<ColumnSchema>,
        primary_key_column_ids: Vec<ColumnId>,
        allocate_timestamp: F,
    ) -> Result<CatalogCreateOutcome>
    where
        F: FnOnce() -> Result<Timestamp>,
    {
        self.ensure_write_path_available()?;

        // Preparation validates uniqueness, table identity, columns, primary
        // keys, schema version, and tablet assignment without publication.
        let prepared = self
            .catalog
            .prepare_table(name, columns, primary_key_column_ids)?;

        // Timestamp allocation occurs only after complete validation.
        let update_timestamp = allocate_timestamp()?;

        if update_timestamp.0 == 0 {
            return Err(Error::Configuration(
                "catalog timestamp allocator returned reserved timestamp 0".to_string(),
            ));
        }

        let command = CatalogCommand {
            operation: CatalogOperation::CreateTable(CreateTableOperation {
                table_def: prepared.to_definition(),
            }),
        };

        let record = CatalogLogRecord {
            table_id: prepared.id,
            update_timestamp,
            command,
        };

        let wal_extent = match self.durable_log.append_catalog_update(&record) {
            Ok(extent) => extent,

            Err(error @ Error::CatalogOutcomeUnknown { .. }) => {
                if self.recovery_required_reason.is_none() {
                    self.recovery_required_reason = Some(error.to_string());
                }

                return Err(error);
            }

            Err(error) => return Err(error),
        };

        let schema = match self.catalog.publish_prepared_table(prepared) {
            Ok(schema) => schema,

            Err(source) => {
                let reason = format!(
                    "durable catalog update for table {} at timestamp {} \
                         failed during publication: {}",
                    record.table_id.0, update_timestamp.0, source
                );

                return Err(self.stop_for_recovery(reason));
            }
        };

        Ok(CatalogCreateOutcome {
            schema,
            update_timestamp,
            wal_extent,
        })
    }

    /// Stop later catalog writes after a post-durability runtime invariant
    /// failure outside the catalog map itself.
    pub fn stop_for_recovery(&mut self, reason: impl Into<String>) -> Error {
        if self.recovery_required_reason.is_none() {
            self.recovery_required_reason = Some(reason.into());
        }

        Error::RecoveryRequired {
            reason: self
                .recovery_required_reason
                .clone()
                .expect("recovery reason was initialized above"),
        }
    }

    fn ensure_write_path_available(&self) -> Result<()> {
        if let Some(reason) = &self.recovery_required_reason {
            return Err(Error::RecoveryRequired {
                reason: reason.clone(),
            });
        }

        Ok(())
    }
}
