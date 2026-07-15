//! Local SQL plan execution.
//!
//! it consumes parser-independent logical plans and executes them through
//! the catalog, transaction, and tablet APIs implemented in earlier phases.
//!
//! The local executor owns:
//!
//! - the mutable `MemoryCatalog`,
//! - one in-memory tablet for every locally created table,
//! - physical access-path selection between point lookup and table scan.
//!
//! The `session` module owns implicit and explicit SQL transaction lifecycles.
//! `SqlSession` contains only transaction policy and active transaction state;
//! connection identity, deadlines, and transport concerns remain in the server
//! crate. The lower-level `LocalExecutor` receives an active `Transaction` and
//! remains independent from connection-level state.
//!
//! The executor never depends directly on `sqlparser`. Unsupported SQL clauses
//! remain the analyzer's responsibility and cannot reach this layer as a Plan.

mod expression;
mod result;
mod session;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use expression::evaluate;
use ragnordb_catalog::{Catalog, ColumnSchema, MemoryCatalog, TableSchema};
use ragnordb_common::{
    Error, Result,
    catalog_codec::DataType,
    codec::{Row, Value},
    ids::{ColumnId, RowKey, TableId, TabletId, Timestamp},
};
use ragnordb_sql::{
    BoundBinaryOperator, BoundColumnRef, BoundExpr, BoundExprKind, BoundTableRef, CreateTablePlan,
    DeletePlan, ExpressionType, InsertPlan, Plan, SelectPlan, UpdateAssignmentPlan, UpdatePlan,
};
use ragnordb_storage::key::{decode_row_key, make_row_key};
use ragnordb_tablet::{RowMutation, Tablet};
use ragnordb_txn::Transaction;

pub use result::{DmlOperation, ExecutionResult, ResultColumn, ResultSet};
pub use session::SqlSession;

/// Local single-node executor for Milestone 2.
///
/// Every locally created table receives one dedicated tablet. Using a separate
/// tablet per table preserves the ownership boundary introduced in Phase 2.6,
/// even though distributed routing is not implemented yet.
#[derive(Debug)]
pub struct LocalExecutor {
    catalog: MemoryCatalog,
    tablets: BTreeMap<TableId, Tablet>,
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalExecutor {
    /// Construct an empty local database executor.
    pub fn new() -> Self {
        Self {
            catalog: MemoryCatalog::new(),
            tablets: BTreeMap::new(),
        }
    }

    /// Return the catalog snapshot used by SQL analysis.
    ///
    /// The mutable catalog remains private so table creation cannot bypass
    /// corresponding tablet creation.
    pub fn catalog(&self) -> &MemoryCatalog {
        &self.catalog
    }

    /// Execute one logical plan.
    ///
    /// SELECT and DML plans require a transaction supplied by the caller. CREATE
    /// TABLE is autocommit-only and therefore rejects an attached transaction.
    /// Session transitions for BEGIN, COMMIT, and ROLLBACK are introduced in
    /// Phase 2.8 and are deliberately not simulated here.
    pub fn execute(
        &mut self,
        plan: Plan,
        transaction: Option<&mut Transaction>,
    ) -> Result<ExecutionResult> {
        match plan {
            Plan::CreateTable(plan) => {
                if transaction.is_some() {
                    return Err(Error::InvalidArgument(
                        "CREATE TABLE is autocommit-only and must not \
                         receive a transaction context"
                            .to_string(),
                    ));
                }

                self.execute_create_table(plan)
            }

            Plan::Insert(plan) => {
                self.execute_insert(plan, require_transaction(transaction, "INSERT")?)
            }

            Plan::Select(plan) => {
                self.execute_select(plan, require_transaction(transaction, "SELECT")?)
            }

            Plan::Update(plan) => {
                self.execute_update(plan, require_transaction(transaction, "UPDATE")?)
            }

            Plan::Delete(plan) => {
                self.execute_delete(plan, require_transaction(transaction, "DELETE")?)
            }

            Plan::ShowTables => self.execute_show_tables(),

            Plan::Begin | Plan::Commit | Plan::Rollback => Err(Error::NotImplemented(
                "transaction-control plans are handled by the \
                     Phase 2.8 session layer",
            )),
        }
    }

    /// Commit a transaction after the caller allocates its commit timestamp.
    ///
    /// currently it supports only transactions whose writes belong to one tablet.
    /// Read-only transactions commit as no-ops. Cross-table transactions remain
    /// unsupported until distributed transaction coordination is implemented.
    pub fn commit_transaction(
        &mut self,
        transaction: Transaction,
        commit_ts: Timestamp,
    ) -> Result<usize> {
        // A read-only transaction has no storage-side commit to timestamp.
        // Returning before validation lets the session layer avoid allocating
        // a commit timestamp for snapshot-only work.
        if transaction.is_empty() {
            return Ok(0);
        }

        validate_commit_timestamp(transaction.start_ts(), commit_ts)?;

        let mut table_ids = BTreeSet::new();

        for encoded_key in transaction.write_set().keys() {
            let row_key = decode_row_key(encoded_key)?;

            if self.catalog.table_by_id(row_key.table_id).is_none() {
                return Err(Error::SchemaMismatch(format!(
                    "transaction references unknown table ID {}",
                    row_key.table_id.0
                )));
            }

            table_ids.insert(row_key.table_id);
        }

        if table_ids.len() != 1 {
            return Err(Error::UnsupportedSql(
                "a Phase 2.7 transaction may write only one local \
                 tablet; cross-table transactions are introduced later"
                    .to_string(),
            ));
        }

        let table_id = *table_ids
            .first()
            .expect("non-empty table-ID set was checked above");

        let tablet = self.tablets.get_mut(&table_id).ok_or_else(|| {
            Error::CorruptData(format!("catalog table {} has no local tablet", table_id.0))
        })?;

        tablet.commit(transaction, commit_ts)
    }

    /// Abort an uncommitted transaction by discarding its buffered mutations.
    pub fn rollback_transaction(&self, transaction: Transaction) -> usize {
        transaction.len()
    }

    fn execute_create_table(&mut self, plan: CreateTablePlan) -> Result<ExecutionResult> {
        let table_id =
            self.catalog
                .add_table(plan.table_name, plan.columns, plan.primary_key_column_ids)?;

        if self.tablets.contains_key(&table_id) {
            return Err(Error::CorruptData(format!(
                "newly allocated table ID {} already has a tablet",
                table_id.0
            )));
        }

        // Local Milestone 2 tablet IDs mirror their owning table IDs. Future
        // metadata allocation will assign independent tablet identities.
        let tablet = Tablet::new(TabletId(table_id.0), table_id)?;

        self.tablets.insert(table_id, tablet);

        Ok(ExecutionResult::CreatedTable { table_id })
    }

    fn execute_show_tables(&self) -> Result<ExecutionResult> {
        let rows = self
            .catalog
            .list_tables()
            .into_iter()
            .map(|table| Row {
                values: vec![Value::Text(table.name.clone())],
            })
            .collect();

        Ok(ExecutionResult::Query(ResultSet {
            columns: vec![ResultColumn {
                name: "table_name".to_string(),
                data_type: DataType::Text,
                nullable: false,
            }],
            rows,
        }))
    }

    fn execute_insert(
        &self,
        plan: InsertPlan,
        transaction: &mut Transaction,
    ) -> Result<ExecutionResult> {
        let InsertPlan {
            table,
            target_columns,
            rows,
        } = plan;

        let schema = self.resolve_table(&table)?;
        let tablet = self.tablet_for(schema.id)?;

        for column in &target_columns {
            validate_bound_column(schema.as_ref(), column)?;
        }

        let mut prepared = Vec::with_capacity(rows.len());
        let mut statement_keys = BTreeSet::new();

        // Construct every row and key before touching the transaction buffer.
        // This gives a multi-row INSERT statement an all-or-nothing preparation
        // boundary for malformed rows and duplicate input keys.
        for values in rows {
            let row = materialize_insert_row(schema.as_ref(), &target_columns, values)?;

            let key = row_key_for_constructed_row(schema.as_ref(), &row)?;

            if !statement_keys.insert(key.clone()) {
                return Err(Error::ConstraintViolation(
                    "INSERT statement contains duplicate primary keys".to_string(),
                ));
            }

            prepared.push((key, row));
        }

        // Check every destination before buffering any mutation. Since this
        // executor holds exclusive access during the call, a later apply pass
        // cannot observe a different local storage state.
        for (key, _) in &prepared {
            if tablet.get(transaction, key)?.is_some() {
                return Err(Error::ConstraintViolation(format!(
                    "cannot insert duplicate primary key into table {}",
                    schema.name
                )));
            }
        }

        let affected_rows = prepared.len();

        tablet.buffer_batch(
            transaction,
            prepared
                .into_iter()
                .map(|(key, row)| RowMutation::Put { key, row }),
        )?;

        Ok(ExecutionResult::Mutation {
            operation: DmlOperation::Insert,
            affected_rows,
        })
    }

    fn execute_select(
        &self,
        plan: SelectPlan,
        transaction: &Transaction,
    ) -> Result<ExecutionResult> {
        let SelectPlan {
            table,
            projection,
            filter,
        } = plan;

        let schema = self.resolve_table(&table)?;
        let tablet = self.tablet_for(schema.id)?;

        if projection.is_empty() {
            return Err(Error::SchemaMismatch(
                "SELECT plan contains an empty projection".to_string(),
            ));
        }

        for column in &projection {
            validate_bound_column(schema.as_ref(), column)?;
        }

        if let Some(filter) = &filter {
            validate_filter(schema.as_ref(), filter)?;
        }

        let matching = matching_rows(tablet, transaction, schema.as_ref(), filter.as_ref())?;

        let rows = matching
            .iter()
            .map(|row| project_row(&row.row, &projection))
            .collect::<Result<Vec<_>>>()?;

        let columns = projection
            .into_iter()
            .map(|column| ResultColumn {
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
            })
            .collect();

        Ok(ExecutionResult::Query(ResultSet { columns, rows }))
    }

    fn execute_update(
        &self,
        plan: UpdatePlan,
        transaction: &mut Transaction,
    ) -> Result<ExecutionResult> {
        let UpdatePlan {
            table,
            assignments,
            filter,
        } = plan;

        let schema = self.resolve_table(&table)?;
        let tablet = self.tablet_for(schema.id)?;

        if assignments.is_empty() {
            return Err(Error::InvalidArgument(
                "UPDATE plan contains no assignments".to_string(),
            ));
        }

        validate_filter(schema.as_ref(), &filter)?;

        for assignment in &assignments {
            validate_update_assignment(schema.as_ref(), assignment)?;
        }

        let matching = matching_rows(tablet, transaction, schema.as_ref(), Some(&filter))?;

        let mut prepared = Vec::with_capacity(matching.len());

        // Evaluate all assignments for all rows before buffering anything. All
        // right-hand expressions observe the row as it existed before this
        // UPDATE statement, matching SQL simultaneous-assignment semantics.
        for keyed_row in matching {
            let original = keyed_row.row;
            let mut updated = original.clone();

            let evaluated = assignments
                .iter()
                .map(|assignment| evaluate(&assignment.value, &original))
                .collect::<Result<Vec<_>>>()?;

            for (assignment, value) in assignments.iter().zip(evaluated) {
                let column = validate_bound_column(schema.as_ref(), &assignment.column)?;

                validate_constructed_value(column, &value)?;

                updated.values[assignment.column.ordinal] = value;
            }

            validate_constructed_row(schema.as_ref(), &updated)?;

            prepared.push((keyed_row.key, updated));
        }

        let affected_rows = prepared.len();

        tablet.buffer_batch(
            transaction,
            prepared
                .into_iter()
                .map(|(key, row)| RowMutation::Put { key, row }),
        )?;

        Ok(ExecutionResult::Mutation {
            operation: DmlOperation::Update,
            affected_rows,
        })
    }

    fn execute_delete(
        &self,
        plan: DeletePlan,
        transaction: &mut Transaction,
    ) -> Result<ExecutionResult> {
        let DeletePlan { table, filter } = plan;

        let schema = self.resolve_table(&table)?;
        let tablet = self.tablet_for(schema.id)?;

        validate_filter(schema.as_ref(), &filter)?;

        let matching = matching_rows(tablet, transaction, schema.as_ref(), Some(&filter))?;

        // Matching completes before the atomic buffer operation, so neither a
        // filter error nor a mutation-encoding error can partially apply this
        // statement to the transaction.
        let affected_rows = matching.len();

        tablet.buffer_batch(
            transaction,
            matching
                .into_iter()
                .map(|keyed_row| RowMutation::Delete { key: keyed_row.key }),
        )?;

        Ok(ExecutionResult::Mutation {
            operation: DmlOperation::Delete,
            affected_rows,
        })
    }

    fn resolve_table(&self, table: &BoundTableRef) -> Result<Arc<TableSchema>> {
        let schema = self.catalog.table_by_id(table.table_id).ok_or_else(|| {
            Error::SchemaMismatch(format!(
                "plan references unknown table ID {}",
                table.table_id.0
            ))
        })?;

        if schema.name != table.name {
            return Err(Error::SchemaMismatch(format!(
                "table ID {} is named {}, but plan expects {}",
                table.table_id.0, schema.name, table.name
            )));
        }

        if schema.schema_version != table.schema_version {
            return Err(Error::SchemaMismatch(format!(
                "table {} is at schema version {}, but plan was \
                 bound against version {}",
                schema.name, schema.schema_version, table.schema_version
            )));
        }

        if schema.tablet_count != 1 {
            return Err(Error::UnsupportedSql(format!(
                "Phase 2.7 local execution requires exactly one \
                 tablet for table {}, found {}",
                schema.name, schema.tablet_count
            )));
        }

        Ok(schema)
    }

    fn tablet_for(&self, table_id: TableId) -> Result<&Tablet> {
        self.tablets.get(&table_id).ok_or_else(|| {
            Error::CorruptData(format!("catalog table {} has no local tablet", table_id.0))
        })
    }
}

fn require_transaction<'a>(
    transaction: Option<&'a mut Transaction>,
    statement: &str,
) -> Result<&'a mut Transaction> {
    transaction.ok_or_else(|| {
        Error::InvalidArgument(format!(
            "{statement} requires an active transaction context"
        ))
    })
}

fn validate_commit_timestamp(start_ts: Timestamp, commit_ts: Timestamp) -> Result<()> {
    if commit_ts.0 == 0 {
        return Err(Error::InvalidArgument(
            "commit timestamp 0 is reserved".to_string(),
        ));
    }

    if commit_ts <= start_ts {
        return Err(Error::InvalidArgument(format!(
            "commit timestamp {} must be greater than start \
             timestamp {}",
            commit_ts.0, start_ts.0
        )));
    }

    Ok(())
}

/// One row and its stable primary-key identity.
#[derive(Debug)]
struct KeyedRow {
    key: RowKey,
    row: Row,
}

/// Pull-based internal row source.
///
/// Storage currently materializes tablet scans, but execution above that layer
/// pulls one keyed row at a time. This preserves a clean path to a fully
/// streaming storage scan in a later phase.
trait KeyedRowExecutor {
    fn next(&mut self) -> Result<Option<KeyedRow>>;
}

struct MaterializedRows {
    rows: std::vec::IntoIter<(RowKey, Row)>,
}

impl MaterializedRows {
    fn new(rows: Vec<(RowKey, Row)>) -> Self {
        Self {
            rows: rows.into_iter(),
        }
    }
}

impl KeyedRowExecutor for MaterializedRows {
    fn next(&mut self) -> Result<Option<KeyedRow>> {
        Ok(self.rows.next().map(|(key, row)| KeyedRow { key, row }))
    }
}

struct FilterRows<'a, E> {
    input: E,
    schema: &'a TableSchema,
    predicate: Option<&'a BoundExpr>,
}

impl<'a, E: KeyedRowExecutor> FilterRows<'a, E> {
    fn new(input: E, schema: &'a TableSchema, predicate: Option<&'a BoundExpr>) -> Self {
        Self {
            input,
            schema,
            predicate,
        }
    }
}

impl<E: KeyedRowExecutor> KeyedRowExecutor for FilterRows<'_, E> {
    fn next(&mut self) -> Result<Option<KeyedRow>> {
        loop {
            let Some(row) = self.input.next()? else {
                return Ok(None);
            };

            validate_stored_keyed_row(self.schema, &row)?;

            let matches = match self.predicate {
                Some(predicate) => expression::evaluate_filter(predicate, &row.row)?,
                None => true,
            };

            if matches {
                return Ok(Some(row));
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum AccessPath {
    Empty,
    Point(RowKey),
    Scan,
}

fn matching_rows(
    tablet: &Tablet,
    transaction: &Transaction,
    schema: &TableSchema,
    filter: Option<&BoundExpr>,
) -> Result<Vec<KeyedRow>> {
    let candidates = match choose_access_path(schema, filter)? {
        AccessPath::Empty => Vec::new(),

        AccessPath::Point(key) => tablet
            .get(transaction, &key)?
            .map(|row| vec![(key, row)])
            .unwrap_or_default(),

        AccessPath::Scan => tablet.scan(transaction, None, None)?,
    };

    let source = MaterializedRows::new(candidates);
    let mut filtered = FilterRows::new(source, schema, filter);
    let mut rows = Vec::new();

    while let Some(row) = filtered.next()? {
        rows.push(row);
    }

    Ok(rows)
}

/// Select point lookup only when every primary-key column is constrained by a
/// literal equality in an AND-connected predicate.
fn choose_access_path(schema: &TableSchema, filter: Option<&BoundExpr>) -> Result<AccessPath> {
    let Some(filter) = filter else {
        return Ok(AccessPath::Scan);
    };

    let primary_key_ids = schema
        .primary_key_column_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();

    let mut equalities = BTreeMap::new();

    if collect_primary_key_equalities(filter, &primary_key_ids, &mut equalities) {
        return Ok(AccessPath::Empty);
    }

    let mut values = Vec::with_capacity(schema.primary_key_column_ids.len());

    for column_id in &schema.primary_key_column_ids {
        let Some(value) = equalities.get(column_id) else {
            return Ok(AccessPath::Scan);
        };

        let column = schema.column_by_id(*column_id).ok_or_else(|| {
            Error::SchemaMismatch(format!(
                "table {} references missing primary-key column ID {}",
                schema.name, column_id.0
            ))
        })?;

        if !value_matches_type(value, column.ty) {
            return Err(Error::SchemaMismatch(format!(
                "primary-key predicate for column {} requires {}, found {}",
                column.name,
                data_type_name(column.ty),
                value_type_name(value)
            )));
        }

        values.push(value.clone());
    }

    Ok(AccessPath::Point(make_row_key(schema.id, &values)?))
}

/// Collect safe primary-key equalities.
///
/// Returns `true` when two predicates constrain the same primary-key column to
/// different literals, making the predicate unsatisfiable.
fn collect_primary_key_equalities(
    expression: &BoundExpr,
    primary_key_ids: &BTreeSet<ColumnId>,
    equalities: &mut BTreeMap<ColumnId, Value>,
) -> bool {
    let BoundExprKind::Binary {
        left,
        operator,
        right,
    } = &expression.kind
    else {
        return false;
    };

    if *operator == BoundBinaryOperator::And {
        return collect_primary_key_equalities(left, primary_key_ids, equalities)
            || collect_primary_key_equalities(right, primary_key_ids, equalities);
    }

    if *operator != BoundBinaryOperator::Equal {
        return false;
    }

    let column_and_value = match (&left.kind, &right.kind) {
        (BoundExprKind::Column(column), BoundExprKind::Literal(value))
        | (BoundExprKind::Literal(value), BoundExprKind::Column(column)) => Some((column, value)),

        _ => None,
    };

    let Some((column, value)) = column_and_value else {
        return false;
    };

    if value == &Value::Null || !primary_key_ids.contains(&column.column_id) {
        return false;
    }

    if let Some(existing) = equalities.get(&column.column_id) {
        return existing != value;
    }

    equalities.insert(column.column_id, value.clone());
    false
}

fn materialize_insert_row(
    schema: &TableSchema,
    target_columns: &[BoundColumnRef],
    source_values: Vec<Value>,
) -> Result<Row> {
    if source_values.len() != target_columns.len() {
        return Err(Error::SchemaMismatch(format!(
            "INSERT plan contains {} values for {} target columns",
            source_values.len(),
            target_columns.len()
        )));
    }

    let mut values = vec![Value::Null; schema.columns.len()];
    let mut assigned = BTreeSet::new();

    for (target, value) in target_columns.iter().zip(source_values) {
        let column = validate_bound_column(schema, target)?;

        if !assigned.insert(target.ordinal) {
            return Err(Error::SchemaMismatch(format!(
                "INSERT plan assigns column {} more than once",
                target.name
            )));
        }

        validate_constructed_value(column, &value)?;
        values[target.ordinal] = value;
    }

    let row = Row { values };
    validate_constructed_row(schema, &row)?;

    Ok(row)
}

fn row_key_for_constructed_row(schema: &TableSchema, row: &Row) -> Result<RowKey> {
    let mut values = Vec::with_capacity(schema.primary_key_column_ids.len());

    for column in schema.primary_key_columns()? {
        let ordinal = schema.column_ordinal(column.id).ok_or_else(|| {
            Error::SchemaMismatch(format!(
                "primary-key column {} has no row ordinal",
                column.name
            ))
        })?;

        let value = row.values.get(ordinal).ok_or_else(|| {
            Error::SchemaMismatch(format!(
                "constructed row does not contain primary-key \
                 column {}",
                column.name
            ))
        })?;

        if value == &Value::Null {
            return Err(Error::ConstraintViolation(format!(
                "primary-key column {} cannot be NULL",
                column.name
            )));
        }

        values.push(value.clone());
    }

    make_row_key(schema.id, &values)
}

fn project_row(row: &Row, projection: &[BoundColumnRef]) -> Result<Row> {
    let values = projection
        .iter()
        .map(|column| {
            row.values.get(column.ordinal).cloned().ok_or_else(|| {
                Error::CorruptData(format!(
                    "stored row has no ordinal {} for projected \
                         column {}",
                    column.ordinal, column.name
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Row { values })
}

fn validate_update_assignment(
    schema: &TableSchema,
    assignment: &UpdateAssignmentPlan,
) -> Result<()> {
    validate_bound_column(schema, &assignment.column)?;

    if schema
        .primary_key_column_ids
        .contains(&assignment.column.column_id)
    {
        return Err(Error::ConstraintViolation(format!(
            "updating primary-key column {} is not supported",
            assignment.column.name
        )));
    }

    validate_expression_columns(schema, &assignment.value)
}

fn validate_filter(schema: &TableSchema, filter: &BoundExpr) -> Result<()> {
    if filter.data_type != ExpressionType::Bool {
        return Err(Error::SchemaMismatch(format!(
            "WHERE expression must return BOOL, found {}",
            filter.data_type
        )));
    }

    validate_expression_columns(schema, filter)
}

fn validate_expression_columns(schema: &TableSchema, expression: &BoundExpr) -> Result<()> {
    match &expression.kind {
        BoundExprKind::Column(column) => {
            validate_bound_column(schema, column)?;
        }

        BoundExprKind::Literal(_) => {}

        BoundExprKind::Unary { expression, .. } | BoundExprKind::IsNull { expression, .. } => {
            validate_expression_columns(schema, expression)?;
        }

        BoundExprKind::Binary { left, right, .. } => {
            validate_expression_columns(schema, left)?;
            validate_expression_columns(schema, right)?;
        }
    }

    Ok(())
}

fn validate_bound_column<'a>(
    schema: &'a TableSchema,
    column: &BoundColumnRef,
) -> Result<&'a ColumnSchema> {
    if column.table_id != schema.id {
        return Err(Error::SchemaMismatch(format!(
            "column {} belongs to table {}, but plan targets table {}",
            column.name, column.table_id.0, schema.id.0
        )));
    }

    let actual = schema.columns.get(column.ordinal).ok_or_else(|| {
        Error::SchemaMismatch(format!(
            "column {} uses invalid row ordinal {}",
            column.name, column.ordinal
        ))
    })?;

    if actual.id != column.column_id
        || actual.name != column.name
        || actual.ty != column.data_type
        || actual.nullable != column.nullable
    {
        return Err(Error::SchemaMismatch(format!(
            "bound metadata for column {} no longer matches schema \
             version {}",
            column.name, schema.schema_version
        )));
    }

    Ok(actual)
}

fn validate_constructed_row(schema: &TableSchema, row: &Row) -> Result<()> {
    if row.values.len() != schema.columns.len() {
        return Err(Error::SchemaMismatch(format!(
            "constructed row for table {} has {} values, expected {}",
            schema.name,
            row.values.len(),
            schema.columns.len()
        )));
    }

    for (column, value) in schema.columns.iter().zip(&row.values) {
        validate_constructed_value(column, value)?;
    }

    Ok(())
}

fn validate_constructed_value(column: &ColumnSchema, value: &Value) -> Result<()> {
    if value == &Value::Null {
        if column.nullable {
            return Ok(());
        }

        return Err(Error::ConstraintViolation(format!(
            "column {} cannot contain NULL",
            column.name
        )));
    }

    if !value_matches_type(value, column.ty) {
        return Err(Error::SchemaMismatch(format!(
            "column {} requires {}, found {}",
            column.name,
            data_type_name(column.ty),
            value_type_name(value)
        )));
    }

    Ok(())
}

fn validate_stored_keyed_row(schema: &TableSchema, keyed_row: &KeyedRow) -> Result<()> {
    validate_stored_row(schema, &keyed_row.row)?;

    let expected_key = stored_row_key(schema, &keyed_row.row)?;

    if expected_key != keyed_row.key {
        return Err(Error::CorruptData(format!(
            "stored row primary key does not match its tablet key \
             in table {}",
            schema.name
        )));
    }

    Ok(())
}

fn validate_stored_row(schema: &TableSchema, row: &Row) -> Result<()> {
    if row.values.len() != schema.columns.len() {
        return Err(Error::CorruptData(format!(
            "stored row for table {} has {} values, expected {}",
            schema.name,
            row.values.len(),
            schema.columns.len()
        )));
    }

    for (column, value) in schema.columns.iter().zip(&row.values) {
        if value == &Value::Null {
            if !column.nullable {
                return Err(Error::CorruptData(format!(
                    "stored row contains NULL in non-nullable \
                     column {}",
                    column.name
                )));
            }

            continue;
        }

        if !value_matches_type(value, column.ty) {
            return Err(Error::CorruptData(format!(
                "stored column {} contains {}, expected {}",
                column.name,
                value_type_name(value),
                data_type_name(column.ty)
            )));
        }
    }

    Ok(())
}

fn stored_row_key(schema: &TableSchema, row: &Row) -> Result<RowKey> {
    let mut values = Vec::with_capacity(schema.primary_key_column_ids.len());

    for column in schema.primary_key_columns()? {
        let ordinal = schema.column_ordinal(column.id).ok_or_else(|| {
            Error::CorruptData(format!(
                "primary-key column {} has no row ordinal",
                column.name
            ))
        })?;

        let value = row.values.get(ordinal).ok_or_else(|| {
            Error::CorruptData(format!(
                "stored row does not contain primary-key column {}",
                column.name
            ))
        })?;

        if value == &Value::Null {
            return Err(Error::CorruptData(format!(
                "stored primary-key column {} contains NULL",
                column.name
            )));
        }

        values.push(value.clone());
    }

    make_row_key(schema.id, &values).map_err(|error| {
        Error::CorruptData(format!("stored row has an invalid primary key: {error}"))
    })
}

fn value_matches_type(value: &Value, data_type: DataType) -> bool {
    matches!(
        (value, data_type),
        (Value::Int(_), DataType::Int)
            | (Value::Text(_), DataType::Text)
            | (Value::Bool(_), DataType::Bool)
    )
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Int(_) => "INT",
        Value::Text(_) => "TEXT",
        Value::Bool(_) => "BOOL",
        Value::Null => "NULL",
    }
}

fn data_type_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Int => "INT",
        DataType::Text => "TEXT",
        DataType::Bool => "BOOL",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_sql::{analyze, parse_one, plan};

    fn build(executor: &LocalExecutor, sql: &str) -> Plan {
        let parsed = parse_one(sql).unwrap();
        let bound = analyze(&parsed, executor.catalog()).unwrap();

        plan(bound)
    }

    fn create_memberships(executor: &mut LocalExecutor) {
        let create = build(
            executor,
            "CREATE TABLE memberships (
                user_id INT,
                group_id INT,
                role TEXT NOT NULL,
                PRIMARY KEY (user_id, group_id)
            )",
        );

        executor.execute(create, None).unwrap();
    }

    fn select_access_path(executor: &LocalExecutor, sql: &str) -> AccessPath {
        let Plan::Select(select) = build(executor, sql) else {
            panic!("expected SELECT plan");
        };

        let schema = executor.resolve_table(&select.table).unwrap();

        choose_access_path(schema.as_ref(), select.filter.as_ref()).unwrap()
    }

    #[test]
    fn access_path_selection_requires_the_complete_primary_key() {
        let mut executor = LocalExecutor::new();
        create_memberships(&mut executor);

        assert_eq!(
            select_access_path(
                &executor,
                "SELECT role FROM memberships
                 WHERE group_id = 20 AND user_id = 1",
            ),
            AccessPath::Point(make_row_key(TableId(1), &[Value::Int(1), Value::Int(20)]).unwrap())
        );

        assert_eq!(
            select_access_path(&executor, "SELECT role FROM memberships WHERE user_id = 1",),
            AccessPath::Scan
        );

        assert_eq!(
            select_access_path(
                &executor,
                "SELECT role FROM memberships
                 WHERE user_id = 1 AND user_id = 2 AND group_id = 20",
            ),
            AccessPath::Empty
        );
    }
}
