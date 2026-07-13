//! Semantic analysis and binding for RagnorDB SQL statements.
//!
//! This module is the only layer after parsing that understands `sqlparser`
//! statement and expression types. It validates RagnorDB's supported SQL
//! subset, resolves catalog objects, performs type checking, and lowers parser
//! values into fully owned types defined in `bound.rs`.
//!
//! The resulting `BoundStatement` contains stable table and column identities,
//! schema versions, row ordinals, resolved expression types, and nullability.
//! Neither the planner nor any future executor should perform name resolution
//! or depend directly on `sqlparser`.

use std::collections::HashSet;

use ragnordb_catalog::{Catalog, ColumnSchema, TableSchema};
use ragnordb_common::catalog_codec::DataType;
use ragnordb_common::codec::Value;
use ragnordb_common::ids::ColumnId;
use ragnordb_common::{Error, Result};
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ColumnOption, DataType as SqlDataType, Delete,
    Expr, FromTable, GroupByExpr, HiveDistributionStyle, Ident, ObjectName, Query, SelectItem,
    SetExpr, ShowStatementOptions, Statement as SqlStatement, TableConstraint, TableFactor,
    TableWithJoins, UnaryOperator, Value as SqlValue, WildcardAdditionalOptions,
};

use crate::bound::{
    BoundAssignment, BoundBinaryOperator, BoundColumnRef, BoundCreateTable, BoundDelete, BoundExpr,
    BoundExprKind, BoundInsert, BoundSelect, BoundStatement, BoundTableRef, BoundUnaryOperator,
    BoundUpdate, ExpressionType,
};
use crate::parser::Statement;

/// Resolve and type-check one parsed SQL statement.
///
/// The analyzer is the enforcement boundary for RagnorDB's supported SQL
/// surface. Every unsupported parser feature is rejected explicitly so that
/// syntax is never accepted and then silently discarded during planning.
///
/// Successful analysis returns a fully RagnorDB-owned bound statement. The
/// returned value contains no `sqlparser` statements, expressions, operators,
/// identifiers, or literal values.
pub fn analyze(statement: &Statement, catalog: &dyn Catalog) -> Result<BoundStatement> {
    match &statement.ast {
        SqlStatement::CreateTable(create) => analyze_create_table(create, catalog),
        SqlStatement::Insert(insert) => analyze_insert(insert, catalog),
        SqlStatement::Query(query) => analyze_select(query, catalog),
        SqlStatement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
        } => analyze_update(
            table,
            assignments,
            from,
            selection.as_ref(),
            returning,
            or,
            catalog,
        ),
        SqlStatement::Delete(delete) => analyze_delete(delete, catalog),
        SqlStatement::StartTransaction {
            modes, modifier, ..
        } => {
            if !modes.is_empty() || modifier.is_some() {
                return Err(unsupported(
                    "transaction modes, isolation levels, access modes, and BEGIN modifiers are not supported yet",
                ));
            }

            Ok(BoundStatement::Begin)
        }
        SqlStatement::Commit { chain } => {
            if *chain {
                return Err(unsupported("COMMIT AND CHAIN is not supported yet"));
            }

            Ok(BoundStatement::Commit)
        }
        SqlStatement::Rollback { chain, savepoint } => {
            if *chain {
                return Err(unsupported("ROLLBACK AND CHAIN is not supported yet"));
            }

            if savepoint.is_some() {
                return Err(unsupported("ROLLBACK TO SAVEPOINT is not supported yet"));
            }

            Ok(BoundStatement::Rollback)
        }
        SqlStatement::ShowTables {
            terse,
            history,
            extended,
            full,
            external,
            show_options,
        } => analyze_show_tables(*terse, *history, *extended, *full, *external, show_options),
        other => Err(unsupported(format!(
            "statement type is not supported yet: {other}"
        ))),
    }
}

/// Validate and bind a `CREATE TABLE` statement.
///
/// The analyzer assigns stable nonzero column IDs because column order and
/// primary-key membership are known during binding. The catalog remains the
/// authority responsible for allocating the final `TableId`, initializing the
/// schema version, setting the local tablet count, and publishing the schema.
fn analyze_create_table(
    create: &sqlparser::ast::CreateTable,
    catalog: &dyn Catalog,
) -> Result<BoundStatement> {
    reject_create_table_features(create)?;

    let table_name = simple_name(&create.name)?;

    if catalog.table_by_name(&table_name).is_some() {
        return Err(Error::ConstraintViolation(format!(
            "table already exists: {table_name}"
        )));
    }

    if create.columns.is_empty() {
        return Err(Error::ConstraintViolation(format!(
            "table {table_name} must define at least one column"
        )));
    }

    let mut seen_columns = HashSet::new();
    let mut columns = Vec::with_capacity(create.columns.len());
    let mut primary_key_definition: Option<Vec<String>> = None;

    for (index, column) in create.columns.iter().enumerate() {
        if column.collation.is_some() {
            return Err(unsupported(format!(
                "column collation is not supported yet: {}",
                column.name
            )));
        }

        let name = normalize_identifier(&column.name);

        if !seen_columns.insert(name.clone()) {
            return Err(Error::ConstraintViolation(format!(
                "duplicate column: {name}"
            )));
        }

        let mut explicit_nullability = None;
        let mut inline_primary_key = false;

        for option in &column.options {
            if option.name.is_some() {
                return Err(unsupported(format!(
                    "named column constraints are not supported yet on column {name}"
                )));
            }

            match &option.option {
                ColumnOption::NotNull => {
                    set_nullability(&mut explicit_nullability, false, &name)?;
                }
                ColumnOption::Null => {
                    set_nullability(&mut explicit_nullability, true, &name)?;
                }
                ColumnOption::Unique {
                    is_primary: true,
                    characteristics,
                } => {
                    if characteristics.is_some() {
                        return Err(unsupported(format!(
                            "primary-key characteristics are not supported yet on column {name}"
                        )));
                    }

                    inline_primary_key = true;
                }
                other => {
                    return Err(unsupported(format!(
                        "unsupported column option on {name}: {other}"
                    )));
                }
            }
        }

        if inline_primary_key && explicit_nullability == Some(true) {
            return Err(Error::ConstraintViolation(format!(
                "primary key column {name} cannot be declared NULL"
            )));
        }

        if inline_primary_key {
            register_primary_key(&mut primary_key_definition, vec![name.clone()])?;
        }

        let column_number = index.checked_add(1).ok_or_else(|| {
            Error::ConstraintViolation(
                "table contains too many columns to assign stable IDs".to_string(),
            )
        })?;

        let column_id = u64::try_from(column_number).map_err(|_| {
            Error::ConstraintViolation(
                "table contains too many columns to assign stable IDs".to_string(),
            )
        })?;

        columns.push(ColumnSchema {
            id: ColumnId(column_id),
            name,
            ty: analyze_data_type(&column.data_type)?,
            nullable: if inline_primary_key {
                false
            } else {
                explicit_nullability.unwrap_or(true)
            },
        });
    }

    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey {
                name,
                index_name,
                index_type,
                columns,
                index_options,
                characteristics,
            } => {
                if name.is_some()
                    || index_name.is_some()
                    || index_type.is_some()
                    || !index_options.is_empty()
                    || characteristics.is_some()
                {
                    return Err(unsupported(
                        "named or implementation-specific primary-key options are not supported yet",
                    ));
                }

                if columns.is_empty() {
                    return Err(Error::ConstraintViolation(
                        "primary key must contain at least one column".to_string(),
                    ));
                }

                let primary_key_columns =
                    columns.iter().map(normalize_identifier).collect::<Vec<_>>();

                register_primary_key(&mut primary_key_definition, primary_key_columns)?;
            }
            other => {
                return Err(unsupported(format!(
                    "unsupported table constraint: {other}"
                )));
            }
        }
    }

    let primary_key_names = primary_key_definition.ok_or_else(|| {
        Error::ConstraintViolation(format!(
            "table {table_name} must define exactly one primary key"
        ))
    })?;

    let mut primary_key_column_ids = Vec::with_capacity(primary_key_names.len());

    for primary_key_name in primary_key_names {
        let column = columns
            .iter_mut()
            .find(|column| column.name == primary_key_name)
            .ok_or_else(|| {
                Error::SchemaMismatch(format!(
                    "primary key column does not exist: {primary_key_name}"
                ))
            })?;

        if primary_key_column_ids.contains(&column.id) {
            return Err(Error::ConstraintViolation(format!(
                "primary key contains duplicate column: {primary_key_name}"
            )));
        }

        column.nullable = false;
        primary_key_column_ids.push(column.id);
    }

    Ok(BoundStatement::CreateTable(BoundCreateTable {
        table_name,
        columns,
        primary_key_column_ids,
    }))
}

/// Bind and validate an `INSERT ... VALUES` statement.
///
/// Target columns are resolved to stable identities before row values are
/// validated. Defaults are not supported yet, so every primary-key and
/// non-nullable column must be supplied explicitly.
fn analyze_insert(
    insert: &sqlparser::ast::Insert,
    catalog: &dyn Catalog,
) -> Result<BoundStatement> {
    reject_insert_features(insert)?;

    let table_name = simple_name(&insert.table_name)?;
    let table = catalog
        .table_by_name(&table_name)
        .ok_or_else(|| Error::SchemaMismatch(format!("unknown table: {table_name}")))?;

    if insert.columns.is_empty() {
        return Err(unsupported(
            "INSERT without an explicit column list is not supported yet",
        ));
    }

    let column_names = insert
        .columns
        .iter()
        .map(normalize_identifier)
        .collect::<Vec<_>>();

    let mut seen_columns = HashSet::new();
    let mut target_columns = Vec::with_capacity(column_names.len());

    for column_name in &column_names {
        if !seen_columns.insert(column_name.clone()) {
            return Err(Error::ConstraintViolation(format!(
                "duplicate INSERT column: {column_name}"
            )));
        }

        let column = table.column_by_name(column_name).ok_or_else(|| {
            Error::SchemaMismatch(format!(
                "unknown column {column_name} on table {table_name}"
            ))
        })?;

        target_columns.push(bind_column_ref(table.as_ref(), column)?);
    }

    // Resolve primary-key columns first so missing-primary-key errors remain
    // specific and actionable to SQL clients.
    for primary_key_column in table.primary_key_columns()? {
        if !column_names.contains(&primary_key_column.name) {
            return Err(Error::ConstraintViolation(format!(
                "INSERT must include primary key column: {}",
                primary_key_column.name
            )));
        }
    }

    // Defaults are not supported yet. Every non-nullable column must therefore
    // be supplied explicitly by the client.
    for column in &table.columns {
        if !column.nullable && !column_names.contains(&column.name) {
            return Err(Error::ConstraintViolation(format!(
                "INSERT must include non-nullable column: {}",
                column.name
            )));
        }
    }

    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| unsupported("INSERT source is required"))?;

    reject_query_clauses(source)?;

    let values = match source.body.as_ref() {
        SetExpr::Values(values) => values,
        _ => return Err(unsupported("only INSERT ... VALUES is supported yet")),
    };

    if values.explicit_row {
        return Err(unsupported(
            "the explicit ROW constructor in INSERT is not supported yet",
        ));
    }

    if values.rows.is_empty() {
        return Err(Error::InvalidArgument(
            "INSERT must contain at least one row".to_string(),
        ));
    }

    let mut rows = Vec::with_capacity(values.rows.len());

    for row in &values.rows {
        if row.len() != target_columns.len() {
            return Err(Error::ConstraintViolation(format!(
                "INSERT row has {} values for {} columns",
                row.len(),
                target_columns.len()
            )));
        }

        let mut bound_row = Vec::with_capacity(row.len());

        for (expression, target_column) in row.iter().zip(target_columns.iter()) {
            let value = analyze_insert_literal(expression)?;
            validate_value_for_bound_column(&value, target_column)?;
            bound_row.push(value);
        }

        rows.push(bound_row);
    }

    Ok(BoundStatement::Insert(BoundInsert {
        table: bind_table_ref(table.as_ref()),
        target_columns,
        rows,
    }))
}

/// Bind a single-table `SELECT` statement.
///
/// Wildcards are expanded during binding so downstream layers always receive a
/// concrete ordered projection. Catalog order is preserved because that order
/// defines the schema-version-specific row layout.
fn analyze_select(query: &Query, catalog: &dyn Catalog) -> Result<BoundStatement> {
    reject_query_clauses(query)?;

    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return Err(unsupported("only simple SELECT is supported yet")),
    };

    reject_select_features(select)?;

    if select.from.len() != 1 {
        return Err(unsupported("SELECT must read from exactly one table"));
    }

    let table = resolve_direct_table(&select.from[0], catalog, "SELECT")?;
    let mut projection = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(options) if wildcard_options_are_empty(options) => {
                for column in &table.columns {
                    projection.push(bind_column_ref(table.as_ref(), column)?);
                }
            }
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => {
                let column_name = normalize_identifier(identifier);
                let column = table.column_by_name(&column_name).ok_or_else(|| {
                    Error::SchemaMismatch(format!(
                        "unknown column {column_name} on table {}",
                        table.name
                    ))
                })?;

                projection.push(bind_column_ref(table.as_ref(), column)?);
            }
            other => {
                return Err(unsupported(format!(
                    "unsupported SELECT projection: {other}"
                )));
            }
        }
    }

    let filter = select
        .selection
        .as_ref()
        .map(|selection| bind_boolean_filter(selection, table.as_ref()))
        .transpose()?;

    Ok(BoundStatement::Select(BoundSelect {
        table: bind_table_ref(table.as_ref()),
        projection,
        filter,
    }))
}

/// Bind and validate a single-table `UPDATE`.
///
/// Requiring a `WHERE` clause is a deliberate initial safety boundary. Full
/// table updates can be introduced later behind explicit syntax or policy once
/// execution, authorization, and observability behavior are established.
fn analyze_update(
    table: &TableWithJoins,
    assignments: &[Assignment],
    from: &Option<TableWithJoins>,
    selection: Option<&Expr>,
    returning: &Option<Vec<SelectItem>>,
    conflict_action: &Option<sqlparser::ast::SqliteOnConflict>,
    catalog: &dyn Catalog,
) -> Result<BoundStatement> {
    if from.is_some() {
        return Err(unsupported("UPDATE ... FROM is not supported yet"));
    }

    if returning.is_some() {
        return Err(unsupported("UPDATE ... RETURNING is not supported yet"));
    }

    if conflict_action.is_some() {
        return Err(unsupported(
            "SQLite UPDATE conflict modifiers are not supported yet",
        ));
    }

    if assignments.is_empty() {
        return Err(Error::InvalidArgument(
            "UPDATE must contain at least one assignment".to_string(),
        ));
    }

    let table = resolve_direct_table(table, catalog, "UPDATE")?;
    let selection = selection.ok_or_else(|| {
        Error::ConstraintViolation(
            "UPDATE requires a WHERE clause in the current SQL version".to_string(),
        )
    })?;

    let mut seen_columns = HashSet::new();
    let mut bound_assignments = Vec::with_capacity(assignments.len());

    for assignment in assignments {
        let column_name = assignment_column_name(&assignment.target)?;
        let column = table.column_by_name(&column_name).ok_or_else(|| {
            Error::SchemaMismatch(format!(
                "unknown column {column_name} on table {}",
                table.name
            ))
        })?;

        if !seen_columns.insert(column.id) {
            return Err(Error::ConstraintViolation(format!(
                "duplicate UPDATE assignment for column: {column_name}"
            )));
        }

        if table.primary_key_column_ids.contains(&column.id) {
            return Err(Error::ConstraintViolation(format!(
                "updating primary key column {column_name} is not supported"
            )));
        }

        let bound_column = bind_column_ref(table.as_ref(), column)?;
        let value = bind_expression(&assignment.value, table.as_ref())?;

        validate_assignment_expression(&value, &bound_column)?;

        bound_assignments.push(BoundAssignment {
            column: bound_column,
            value,
        });
    }

    let filter = bind_boolean_filter(selection, table.as_ref())?;

    Ok(BoundStatement::Update(BoundUpdate {
        table: bind_table_ref(table.as_ref()),
        assignments: bound_assignments,
        filter,
    }))
}

/// Bind and validate a single-table `DELETE`.
///
/// A `WHERE` clause is mandatory in this initial implementation to prevent
/// accidental unbounded deletion before explicit full-table DML semantics and
/// operational safeguards exist.
fn analyze_delete(delete: &Delete, catalog: &dyn Catalog) -> Result<BoundStatement> {
    if !delete.tables.is_empty() {
        return Err(unsupported("multi-table DELETE is not supported yet"));
    }

    if delete.using.is_some() {
        return Err(unsupported("DELETE ... USING is not supported yet"));
    }

    if delete.returning.is_some() {
        return Err(unsupported("DELETE ... RETURNING is not supported yet"));
    }

    if !delete.order_by.is_empty() {
        return Err(unsupported("DELETE ... ORDER BY is not supported yet"));
    }

    if delete.limit.is_some() {
        return Err(unsupported("DELETE ... LIMIT is not supported yet"));
    }

    let from = match &delete.from {
        FromTable::WithFromKeyword(from) => from,
        FromTable::WithoutKeyword(_) => {
            return Err(unsupported("DELETE requires an explicit FROM keyword"));
        }
    };

    if from.len() != 1 {
        return Err(unsupported("DELETE must target exactly one direct table"));
    }

    let table = resolve_direct_table(&from[0], catalog, "DELETE")?;
    let selection = delete.selection.as_ref().ok_or_else(|| {
        Error::ConstraintViolation(
            "DELETE requires a WHERE clause in the current SQL version".to_string(),
        )
    })?;

    let filter = bind_boolean_filter(selection, table.as_ref())?;

    Ok(BoundStatement::Delete(BoundDelete {
        table: bind_table_ref(table.as_ref()),
        filter,
    }))
}

/// Validate a plain `SHOW TABLES` statement.
///
/// Filters and dialect-specific modifiers are rejected because the bound
/// statement intentionally represents only deterministic enumeration of the
/// current catalog snapshot.
fn analyze_show_tables(
    terse: bool,
    history: bool,
    extended: bool,
    full: bool,
    external: bool,
    options: &ShowStatementOptions,
) -> Result<BoundStatement> {
    let has_options = options.show_in.is_some()
        || options.starts_with.is_some()
        || options.limit.is_some()
        || options.limit_from.is_some()
        || options.filter_position.is_some();

    if terse || history || extended || full || external || has_options {
        return Err(unsupported(
            "SHOW TABLES modifiers, filters, prefixes, scopes, history, and limits are not supported yet",
        ));
    }

    Ok(BoundStatement::ShowTables)
}

/// Resolve one direct table reference and reject aliases, joins, functions,
/// hints, schema versions, partitions, and dialect-specific table features.
fn resolve_direct_table(
    table: &TableWithJoins,
    catalog: &dyn Catalog,
    statement_name: &str,
) -> Result<std::sync::Arc<TableSchema>> {
    if !table.joins.is_empty() {
        return Err(unsupported(format!(
            "{statement_name} does not support JOIN"
        )));
    }

    let table_name = match &table.relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
        } => {
            if alias.is_some()
                || args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
            {
                return Err(unsupported(format!(
                    "{statement_name} supports only direct, unaliased table references"
                )));
            }

            simple_name(name)?
        }
        _ => {
            return Err(unsupported(format!(
                "{statement_name} supports only direct table references"
            )));
        }
    };

    catalog
        .table_by_name(&table_name)
        .ok_or_else(|| Error::SchemaMismatch(format!("unknown table: {table_name}")))
}

/// Extract an UPDATE assignment target.
///
/// Tuple assignment and qualified assignment targets are intentionally rejected
/// until their evaluation and ownership semantics are represented explicitly in
/// the internal bound statement.
fn assignment_column_name(target: &AssignmentTarget) -> Result<String> {
    match target {
        AssignmentTarget::ColumnName(name) => simple_name(name),
        AssignmentTarget::Tuple(_) => Err(unsupported(
            "tuple assignment targets are not supported in UPDATE",
        )),
    }
}

/// Construct the immutable table identity retained by bound statements.
fn bind_table_ref(table: &TableSchema) -> BoundTableRef {
    BoundTableRef {
        table_id: table.id,
        name: table.name.clone(),
        schema_version: table.schema_version,
    }
}

/// Resolve a catalog column into its stable identity and row-layout position.
///
/// `column_id` survives schema evolution, while `ordinal` identifies the value
/// position in rows encoded using this exact schema version. Keeping both
/// prevents the executor from incorrectly treating column ID as row position.
fn bind_column_ref(table: &TableSchema, column: &ColumnSchema) -> Result<BoundColumnRef> {
    let ordinal = table.column_ordinal(column.id).ok_or_else(|| {
        Error::SchemaMismatch(format!(
            "column {} with ID {} is not part of table {}",
            column.name, column.id.0, table.name
        ))
    })?;

    Ok(BoundColumnRef {
        table_id: table.id,
        column_id: column.id,
        ordinal,
        name: column.name.clone(),
        data_type: column.ty,
        nullable: column.nullable,
    })
}

/// Bind and validate a predicate used by SELECT, UPDATE, or DELETE.
fn bind_boolean_filter(expression: &Expr, table: &TableSchema) -> Result<BoundExpr> {
    let expression = bind_expression(expression, table)?;

    if expression.data_type != ExpressionType::Bool {
        return Err(Error::SchemaMismatch(format!(
            "WHERE expression must evaluate to BOOL, found {}",
            expression.data_type
        )));
    }

    Ok(expression)
}

/// Bind an SQL expression into a fully owned RagnorDB expression tree.
///
/// Every identifier is resolved against an immutable table schema snapshot.
/// The returned tree contains no parser nodes and records the result type and
/// nullability of every expression.
fn bind_expression(expression: &Expr, table: &TableSchema) -> Result<BoundExpr> {
    match expression {
        Expr::Identifier(identifier) => {
            let column_name = normalize_identifier(identifier);
            let column = table.column_by_name(&column_name).ok_or_else(|| {
                Error::SchemaMismatch(format!(
                    "unknown column {column_name} on table {}",
                    table.name
                ))
            })?;

            let column = bind_column_ref(table, column)?;

            Ok(BoundExpr {
                data_type: expression_type_for_data_type(column.data_type),
                nullable: column.nullable,
                kind: BoundExprKind::Column(column),
            })
        }
        Expr::Value(value) => bind_literal(value),
        Expr::Nested(inner) => bind_expression(inner, table),
        Expr::IsNull(inner) => bind_is_null_expression(inner, false, table),
        Expr::IsNotNull(inner) => bind_is_null_expression(inner, true, table),
        Expr::UnaryOp { op, expr } => bind_unary_expression(op, expr, table),
        Expr::BinaryOp { left, op, right } => bind_binary_expression(left, op, right, table),
        other => Err(unsupported(format!("unsupported expression: {other}"))),
    }
}

/// Bind `IS NULL` and `IS NOT NULL`.
///
/// These predicates always produce a non-null Boolean, even if their operand
/// evaluates to SQL NULL.
fn bind_is_null_expression(
    expression: &Expr,
    negated: bool,
    table: &TableSchema,
) -> Result<BoundExpr> {
    let expression = bind_expression(expression, table)?;

    Ok(BoundExpr {
        kind: BoundExprKind::IsNull {
            expression: Box::new(expression),
            negated,
        },
        data_type: ExpressionType::Bool,
        nullable: false,
    })
}

/// Bind a scalar literal and validate integer range boundaries.
fn bind_literal(value: &SqlValue) -> Result<BoundExpr> {
    let (value, data_type, nullable) = match value {
        SqlValue::Number(value, _) => (
            Value::Int(parse_integer_literal(value, false)?),
            ExpressionType::Int,
            false,
        ),
        SqlValue::SingleQuotedString(value) => {
            (Value::Text(value.clone()), ExpressionType::Text, false)
        }
        SqlValue::Boolean(value) => (Value::Bool(*value), ExpressionType::Bool, false),
        SqlValue::Null => (Value::Null, ExpressionType::Null, true),
        other => return Err(unsupported(format!("unsupported literal: {other}"))),
    };

    Ok(BoundExpr {
        kind: BoundExprKind::Literal(value),
        data_type,
        nullable,
    })
}

/// Bind a unary expression and enforce operator-specific operand types.
fn bind_unary_expression(
    operator: &UnaryOperator,
    expression: &Expr,
    table: &TableSchema,
) -> Result<BoundExpr> {
    // Parse a signed numeric literal as one value so the complete i64 domain,
    // including i64::MIN, remains representable.
    if let Expr::Value(SqlValue::Number(value, _)) = expression {
        match operator {
            UnaryOperator::Minus => {
                return Ok(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Int(parse_integer_literal(value, true)?)),
                    data_type: ExpressionType::Int,
                    nullable: false,
                });
            }
            UnaryOperator::Plus => {
                return Ok(BoundExpr {
                    kind: BoundExprKind::Literal(Value::Int(parse_integer_literal(value, false)?)),
                    data_type: ExpressionType::Int,
                    nullable: false,
                });
            }
            _ => {}
        }
    }

    let expression = bind_expression(expression, table)?;

    let (operator, data_type) = match operator {
        UnaryOperator::Plus if expression.data_type == ExpressionType::Int => {
            (BoundUnaryOperator::Positive, ExpressionType::Int)
        }
        UnaryOperator::Minus if expression.data_type == ExpressionType::Int => {
            (BoundUnaryOperator::Negative, ExpressionType::Int)
        }
        UnaryOperator::Not if expression.data_type == ExpressionType::Bool => {
            (BoundUnaryOperator::Not, ExpressionType::Bool)
        }
        UnaryOperator::Plus | UnaryOperator::Minus => {
            return Err(Error::SchemaMismatch(format!(
                "unary {operator} requires INT, found {}",
                expression.data_type
            )));
        }
        UnaryOperator::Not => {
            return Err(Error::SchemaMismatch(format!(
                "NOT requires BOOL, found {}",
                expression.data_type
            )));
        }
        _ => {
            return Err(unsupported(format!(
                "unsupported unary operator: {operator}"
            )));
        }
    };

    let nullable = expression.nullable;

    Ok(BoundExpr {
        kind: BoundExprKind::Unary {
            operator,
            expression: Box::new(expression),
        },
        data_type,
        nullable,
    })
}

/// Bind a binary expression and convert the parser operator into RagnorDB's
/// internal operator representation.
fn bind_binary_expression(
    left: &Expr,
    operator: &BinaryOperator,
    right: &Expr,
    table: &TableSchema,
) -> Result<BoundExpr> {
    let left = bind_expression(left, table)?;
    let right = bind_expression(right, table)?;

    let left_type = left.data_type;
    let right_type = right.data_type;

    let (operator, data_type) = match operator {
        BinaryOperator::And => {
            require_types(operator, left_type, right_type, ExpressionType::Bool)?;
            (BoundBinaryOperator::And, ExpressionType::Bool)
        }
        BinaryOperator::Or => {
            require_types(operator, left_type, right_type, ExpressionType::Bool)?;
            (BoundBinaryOperator::Or, ExpressionType::Bool)
        }
        BinaryOperator::Plus => {
            require_types(operator, left_type, right_type, ExpressionType::Int)?;
            (BoundBinaryOperator::Add, ExpressionType::Int)
        }
        BinaryOperator::Minus => {
            require_types(operator, left_type, right_type, ExpressionType::Int)?;
            (BoundBinaryOperator::Subtract, ExpressionType::Int)
        }
        BinaryOperator::Multiply => {
            require_types(operator, left_type, right_type, ExpressionType::Int)?;
            (BoundBinaryOperator::Multiply, ExpressionType::Int)
        }
        BinaryOperator::Divide => {
            require_types(operator, left_type, right_type, ExpressionType::Int)?;
            (BoundBinaryOperator::Divide, ExpressionType::Int)
        }
        BinaryOperator::Modulo => {
            require_types(operator, left_type, right_type, ExpressionType::Int)?;
            (BoundBinaryOperator::Modulo, ExpressionType::Int)
        }
        BinaryOperator::Eq => {
            require_comparable_types(operator, left_type, right_type, false)?;
            (BoundBinaryOperator::Equal, ExpressionType::Bool)
        }
        BinaryOperator::NotEq => {
            require_comparable_types(operator, left_type, right_type, false)?;
            (BoundBinaryOperator::NotEqual, ExpressionType::Bool)
        }
        BinaryOperator::Gt => {
            require_comparable_types(operator, left_type, right_type, true)?;
            (BoundBinaryOperator::GreaterThan, ExpressionType::Bool)
        }
        BinaryOperator::GtEq => {
            require_comparable_types(operator, left_type, right_type, true)?;
            (
                BoundBinaryOperator::GreaterThanOrEqual,
                ExpressionType::Bool,
            )
        }
        BinaryOperator::Lt => {
            require_comparable_types(operator, left_type, right_type, true)?;
            (BoundBinaryOperator::LessThan, ExpressionType::Bool)
        }
        BinaryOperator::LtEq => {
            require_comparable_types(operator, left_type, right_type, true)?;
            (BoundBinaryOperator::LessThanOrEqual, ExpressionType::Bool)
        }
        _ => {
            return Err(unsupported(format!(
                "unsupported binary operator: {operator}"
            )));
        }
    };

    let nullable = left.nullable || right.nullable;

    Ok(BoundExpr {
        kind: BoundExprKind::Binary {
            left: Box::new(left),
            operator,
            right: Box::new(right),
        },
        data_type,
        nullable,
    })
}

/// Require both operands to have an exact operator-specific type.
fn require_types(
    operator: &BinaryOperator,
    left: ExpressionType,
    right: ExpressionType,
    expected: ExpressionType,
) -> Result<()> {
    if left == expected && right == expected {
        return Ok(());
    }

    Err(Error::SchemaMismatch(format!(
        "operator {operator} requires {expected} operands, found {left} and {right}"
    )))
}

/// Require operands to be comparable under RagnorDB's currently supported SQL
/// type system.
fn require_comparable_types(
    operator: &BinaryOperator,
    left: ExpressionType,
    right: ExpressionType,
    ordered: bool,
) -> Result<()> {
    if left == ExpressionType::Null || right == ExpressionType::Null {
        return Err(Error::UnsupportedSql(
            "use IS NULL or IS NOT NULL instead of comparing with NULL".to_string(),
        ));
    }

    if left != right {
        return Err(Error::SchemaMismatch(format!(
            "operator {operator} cannot compare {left} with {right}"
        )));
    }

    if ordered && !matches!(left, ExpressionType::Int | ExpressionType::Text) {
        return Err(Error::SchemaMismatch(format!(
            "operator {operator} does not support ordered comparison for {left}"
        )));
    }

    Ok(())
}

/// Validate that an UPDATE expression is assignable to its target column.
fn validate_assignment_expression(expression: &BoundExpr, column: &BoundColumnRef) -> Result<()> {
    if expression.data_type == ExpressionType::Null {
        if column.nullable {
            return Ok(());
        }

        return Err(Error::ConstraintViolation(format!(
            "column {} cannot be assigned NULL",
            column.name
        )));
    }

    let expected = expression_type_for_data_type(column.data_type);

    if expression.data_type != expected {
        return Err(Error::SchemaMismatch(format!(
            "assignment for column {} requires {}, found {}",
            column.name, expected, expression.data_type
        )));
    }

    // A nullable expression can produce NULL even when it is not a literal
    // NULL. Rejecting it here ensures non-nullability is enforced before the
    // statement reaches an executor without runtime constraint handling.
    if expression.nullable && !column.nullable {
        return Err(Error::ConstraintViolation(format!(
            "assignment for non-nullable column {} may evaluate to NULL",
            column.name
        )));
    }

    Ok(())
}

/// Map a catalog type into the scalar expression type system.
fn expression_type_for_data_type(data_type: DataType) -> ExpressionType {
    match data_type {
        DataType::Int => ExpressionType::Int,
        DataType::Text => ExpressionType::Text,
        DataType::Bool => ExpressionType::Bool,
    }
}

/// Convert an INSERT expression into a stored scalar value.
///
/// INSERT currently supports literal VALUES only. General expression evaluation
/// remains an executor responsibility and must not be introduced implicitly in
/// this phase.
fn analyze_insert_literal(expression: &Expr) -> Result<Value> {
    match expression {
        Expr::Value(SqlValue::Number(value, _)) => {
            Ok(Value::Int(parse_integer_literal(value, false)?))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(SqlValue::Number(value, _)) => {
                Ok(Value::Int(parse_integer_literal(value, true)?))
            }
            _ => Err(unsupported(
                "INSERT supports unary minus only on integer literals",
            )),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(SqlValue::Number(value, _)) => {
                Ok(Value::Int(parse_integer_literal(value, false)?))
            }
            _ => Err(unsupported(
                "INSERT supports unary plus only on integer literals",
            )),
        },
        Expr::Value(SqlValue::SingleQuotedString(value)) => Ok(Value::Text(value.clone())),
        Expr::Value(SqlValue::Boolean(value)) => Ok(Value::Bool(*value)),
        Expr::Value(SqlValue::Null) => Ok(Value::Null),
        other => Err(unsupported(format!(
            "INSERT values must be literals, found: {other}"
        ))),
    }
}

/// Parse an integer literal across the complete signed i64 range.
///
/// The parser exposes the sign as a separate unary operator. Parsing the
/// unsigned magnitude into i128 first allows `-9223372036854775808` to be
/// accepted even though its positive magnitude is greater than `i64::MAX`.
fn parse_integer_literal(value: &str, negative: bool) -> Result<i64> {
    let magnitude = value
        .parse::<i128>()
        .map_err(|_| Error::SchemaMismatch(format!("invalid INT literal: {value}")))?;

    let signed = if negative {
        magnitude.checked_neg().ok_or_else(|| {
            Error::SchemaMismatch(format!("INT literal is outside the i64 range: -{value}"))
        })?
    } else {
        magnitude
    };

    i64::try_from(signed).map_err(|_| {
        let prefix = if negative { "-" } else { "" };

        Error::SchemaMismatch(format!(
            "INT literal is outside the i64 range: {prefix}{value}"
        ))
    })
}

/// Validate a stored INSERT value against its resolved target column.
fn validate_value_for_bound_column(value: &Value, column: &BoundColumnRef) -> Result<()> {
    match value {
        Value::Null if column.nullable => Ok(()),
        Value::Null => Err(Error::ConstraintViolation(format!(
            "column {} cannot be NULL",
            column.name
        ))),
        Value::Int(_) if column.data_type == DataType::Int => Ok(()),
        Value::Text(_) if column.data_type == DataType::Text => Ok(()),
        Value::Bool(_) if column.data_type == DataType::Bool => Ok(()),
        _ => Err(Error::SchemaMismatch(format!(
            "value for column {} does not match type {:?}",
            column.name, column.data_type
        ))),
    }
}

/// Convert one supported SQL DDL type into RagnorDB's catalog type.
fn analyze_data_type(data_type: &SqlDataType) -> Result<DataType> {
    match data_type {
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Int),
        SqlDataType::Text => Ok(DataType::Text),
        SqlDataType::Bool | SqlDataType::Boolean => Ok(DataType::Bool),
        other => Err(unsupported(format!("unsupported data type: {other}"))),
    }
}

/// Record an explicit NULL or NOT NULL declaration.
///
/// Duplicate declarations are rejected even if they agree because silently
/// accepting redundant constraints can conceal generated or malformed DDL.
fn set_nullability(current: &mut Option<bool>, nullable: bool, column_name: &str) -> Result<()> {
    if let Some(previous) = current {
        if *previous != nullable {
            return Err(Error::ConstraintViolation(format!(
                "column {column_name} has conflicting NULL and NOT NULL declarations"
            )));
        }

        return Err(Error::ConstraintViolation(format!(
            "column {column_name} repeats its nullability declaration"
        )));
    }

    *current = Some(nullable);
    Ok(())
}

/// Register the table's single primary-key definition.
fn register_primary_key(current: &mut Option<Vec<String>>, columns: Vec<String>) -> Result<()> {
    if current.is_some() {
        return Err(Error::ConstraintViolation(
            "table defines more than one primary key".to_string(),
        ));
    }

    *current = Some(columns);
    Ok(())
}

/// Reject CREATE TABLE syntax that the bound representation cannot preserve.
fn reject_create_table_features(create: &sqlparser::ast::CreateTable) -> Result<()> {
    if create.if_not_exists {
        return Err(unsupported(
            "CREATE TABLE IF NOT EXISTS is not supported yet",
        ));
    }

    if create.query.is_some() {
        return Err(unsupported("CREATE TABLE AS SELECT is not supported yet"));
    }

    let has_unsupported_feature = create.or_replace
        || create.temporary
        || create.external
        || create.global.is_some()
        || create.transient
        || create.volatile
        || !matches!(&create.hive_distribution, HiveDistributionStyle::NONE)
        || has_hive_format_options(&create.hive_formats)
        || !create.table_properties.is_empty()
        || !create.with_options.is_empty()
        || create.file_format.is_some()
        || create.location.is_some()
        || create.without_rowid
        || create.like.is_some()
        || create.clone.is_some()
        || create.engine.is_some()
        || create.comment.is_some()
        || create.auto_increment_offset.is_some()
        || create.default_charset.is_some()
        || create.collation.is_some()
        || create.on_commit.is_some()
        || create.on_cluster.is_some()
        || create.primary_key.is_some()
        || create.order_by.is_some()
        || create.partition_by.is_some()
        || create.cluster_by.is_some()
        || create.clustered_by.is_some()
        || create.options.is_some()
        || create.strict
        || create.copy_grants
        || create.enable_schema_evolution.is_some()
        || create.change_tracking.is_some()
        || create.data_retention_time_in_days.is_some()
        || create.max_data_extension_time_in_days.is_some()
        || create.default_ddl_collation.is_some()
        || create.with_aggregation_policy.is_some()
        || create.with_row_access_policy.is_some()
        || create.with_tags.is_some();

    if has_unsupported_feature {
        return Err(unsupported(
            "CREATE TABLE options beyond columns and one primary key are not supported yet",
        ));
    }

    Ok(())
}

/// Determine whether a parsed `CREATE TABLE` contains actual Hive options.
///
/// `sqlparser` stores `Some(HiveFormat::default())` even when the statement
/// contains no Hive syntax. Checking only `Option::is_some()` would therefore
/// reject every ordinary `CREATE TABLE`.
fn has_hive_format_options(hive_formats: &Option<sqlparser::ast::HiveFormat>) -> bool {
    hive_formats.as_ref().is_some_and(|format| {
        format.row_format.is_some()
            || format.serde_properties.is_some()
            || format.storage.is_some()
            || format.location.is_some()
    })
}

/// Reject INSERT syntax that cannot be represented by `BoundInsert`.
fn reject_insert_features(insert: &sqlparser::ast::Insert) -> Result<()> {
    if insert.or.is_some()
        || insert.ignore
        || !insert.into
        || insert.table_alias.is_some()
        || insert.overwrite
        || insert.partitioned.is_some()
        || !insert.after_columns.is_empty()
        || insert.table
        || insert.on.is_some()
        || insert.returning.is_some()
        || insert.replace_into
        || insert.priority.is_some()
        || insert.insert_alias.is_some()
    {
        return Err(unsupported(
            "INSERT modifiers, aliases, conflict handlers, partitions, and RETURNING are not supported yet",
        ));
    }

    Ok(())
}

/// Reject query clauses that the current logical representation cannot retain.
fn reject_query_clauses(query: &Query) -> Result<()> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit.is_some()
        || !query.limit_by.is_empty()
        || query.offset.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return Err(unsupported(
            "WITH, ORDER BY, LIMIT, OFFSET, FETCH, locking, and format clauses are not supported yet",
        ));
    }

    Ok(())
}

/// Reject SELECT clauses that the current bound representation cannot retain.
fn reject_select_features(select: &sqlparser::ast::Select) -> Result<()> {
    let has_group_by = match &select.group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(expressions, modifiers) => {
            !expressions.is_empty() || !modifiers.is_empty()
        }
    };

    if select.distinct.is_some()
        || select.top.is_some()
        || select.top_before_distinct
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || has_group_by
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.window_before_qualify
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
    {
        return Err(unsupported(
            "DISTINCT, TOP, INTO, GROUP BY, HAVING, window, QUALIFY, and dialect-specific SELECT clauses are not supported yet",
        ));
    }

    Ok(())
}

/// Determine whether an unqualified wildcard has no parser-level modifiers.
fn wildcard_options_are_empty(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
}

/// Convert an SQL identifier into its canonical catalog form.
///
/// Unquoted names are folded to lowercase. Quoted identifiers preserve their
/// exact spelling, providing case-insensitive ordinary SQL identifiers and
/// explicit case sensitivity when requested by the client.
fn normalize_identifier(identifier: &Ident) -> String {
    if identifier.quote_style.is_some() {
        identifier.value.clone()
    } else {
        identifier.value.to_lowercase()
    }
}

/// Resolve a currently supported unqualified object name.
fn simple_name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(unsupported(format!(
            "qualified names are not supported yet: {name}"
        )));
    }

    Ok(normalize_identifier(&name.0[0]))
}

/// Construct a consistent unsupported-SQL error.
fn unsupported(message: impl Into<String>) -> Error {
    Error::UnsupportedSql(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use ragnordb_catalog::MemoryCatalog;
    use ragnordb_common::ids::TableId;

    /// Build the deterministic schema shared by analyzer tests.
    fn make_catalog() -> MemoryCatalog {
        let mut catalog = MemoryCatalog::new();

        catalog
            .add_table(
                "users",
                vec![
                    ColumnSchema {
                        id: ColumnId(1),
                        name: "id".into(),
                        ty: DataType::Int,
                        nullable: false,
                    },
                    ColumnSchema {
                        id: ColumnId(2),
                        name: "name".into(),
                        ty: DataType::Text,
                        nullable: true,
                    },
                    ColumnSchema {
                        id: ColumnId(3),
                        name: "active".into(),
                        ty: DataType::Bool,
                        nullable: true,
                    },
                ],
                vec![ColumnId(1)],
            )
            .unwrap();

        catalog
    }

    fn parse(sql: &str) -> Statement {
        crate::parser::parse_one(sql).unwrap()
    }

    #[test]
    fn analyze_create_table() {
        let catalog = MemoryCatalog::new();
        let statement = parse("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)");

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::CreateTable(table) = bound else {
            panic!("expected CREATE TABLE");
        };

        assert_eq!(table.table_name, "items");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.primary_key_column_ids, vec![ColumnId(1)]);
    }

    #[test]
    fn ordinary_create_table_is_not_treated_as_hive_ddl() {
        let catalog = MemoryCatalog::new();
        let statement = parse(
            "CREATE TABLE items (
                id INT PRIMARY KEY,
                name TEXT
            )",
        );

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::CreateTable(table) = bound else {
            panic!("expected CREATE TABLE");
        };

        assert_eq!(table.table_name, "items");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.primary_key_column_ids, vec![ColumnId(1)]);
    }

    #[test]
    fn reject_create_duplicate_table() {
        let catalog = make_catalog();
        let statement = parse("CREATE TABLE users (id INT PRIMARY KEY)");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("table already exists"));
    }

    #[test]
    fn reject_create_without_primary_key() {
        let catalog = MemoryCatalog::new();
        let statement = parse("CREATE TABLE items (id INT)");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("primary key"));
    }

    #[test]
    fn reject_unsupported_data_type() {
        let catalog = MemoryCatalog::new();
        let statement = parse("CREATE TABLE items (id FLOAT PRIMARY KEY)");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("unsupported data type"));
    }

    #[test]
    fn rejects_create_table_if_not_exists() {
        let catalog = MemoryCatalog::new();
        let statement = parse("CREATE TABLE IF NOT EXISTS items (id INT PRIMARY KEY)");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("IF NOT EXISTS"));
    }

    #[test]
    fn rejects_multiple_primary_key_definitions() {
        let catalog = MemoryCatalog::new();
        let statement = parse(
            "CREATE TABLE items (
                id INT PRIMARY KEY,
                name TEXT,
                PRIMARY KEY (name)
            )",
        );

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("more than one primary key"));
    }

    #[test]
    fn rejects_duplicate_columns_inside_composite_primary_key() {
        let catalog = MemoryCatalog::new();
        let statement = parse(
            "CREATE TABLE items (
                tenant_id INT,
                id INT,
                PRIMARY KEY (tenant_id, tenant_id)
            )",
        );

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("primary key contains duplicate column")
        );
    }

    #[test]
    fn rejects_create_table_with_hive_storage_format() {
        let catalog = MemoryCatalog::new();
        let statement = parse(
            "CREATE TABLE items (
                id INT PRIMARY KEY
            ) STORED AS PARQUET",
        );

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(matches!(error, Error::UnsupportedSql(_)));
    }

    #[test]
    fn analyze_insert_success() {
        let catalog = make_catalog();
        let statement = parse("INSERT INTO users (id, name, active) VALUES (1, 'Ada', true)");

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::Insert(insert) = bound else {
            panic!("expected INSERT");
        };

        assert_eq!(insert.table.table_id, TableId(1));
        assert_eq!(insert.table.name, "users");
        assert_eq!(insert.table.schema_version, 1);

        assert_eq!(
            insert
                .target_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name", "active"]
        );

        assert_eq!(insert.target_columns[0].column_id, ColumnId(1));
        assert_eq!(insert.target_columns[0].ordinal, 0);
        assert_eq!(insert.rows[0][0], Value::Int(1));
        assert_eq!(insert.rows[0][1], Value::Text("Ada".into()));
        assert_eq!(insert.rows[0][2], Value::Bool(true));
    }

    #[test]
    fn reject_insert_unknown_table() {
        let catalog = MemoryCatalog::new();
        let statement = parse("INSERT INTO ghost (id) VALUES (1)");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("unknown table"));
    }

    #[test]
    fn reject_insert_missing_primary_key() {
        let catalog = make_catalog();
        let statement = parse("INSERT INTO users (name) VALUES ('Ada')");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("primary key"));
    }

    #[test]
    fn reject_insert_unknown_column() {
        let catalog = make_catalog();
        let statement = parse("INSERT INTO users (id, nonexistent) VALUES (1, 'x')");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("unknown column"));
    }

    #[test]
    fn reject_insert_duplicate_column() {
        let catalog = make_catalog();
        let statement = parse("INSERT INTO users (id, id) VALUES (1, 2)");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("duplicate INSERT column"));
    }

    #[test]
    fn reject_insert_wrong_type() {
        let catalog = make_catalog();
        let statement = parse("INSERT INTO users (id, name) VALUES ('abc', 'Ada')");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("type"));
    }

    #[test]
    fn reject_insert_null_into_non_nullable_column() {
        let catalog = make_catalog();
        let statement = parse("INSERT INTO users (id, name) VALUES (NULL, 'Ada')");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("NULL"));
    }

    #[test]
    fn reject_insert_wrong_value_count() {
        let catalog = make_catalog();
        let statement = parse("INSERT INTO users (id, name) VALUES (1, 'Ada', true)");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("values"));
    }

    #[test]
    fn accepts_minimum_signed_integer_literal() {
        let catalog = make_catalog();
        let statement = parse("INSERT INTO users (id, name) VALUES (-9223372036854775808, 'Ada')");

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::Insert(insert) = bound else {
            panic!("expected INSERT");
        };

        assert_eq!(insert.rows[0][0], Value::Int(i64::MIN));
    }

    #[test]
    fn rejects_integer_literal_above_i64_maximum() {
        let catalog = make_catalog();
        let statement = parse("INSERT INTO users (id, name) VALUES (9223372036854775808, 'Ada')");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("outside the i64 range"));
    }

    #[test]
    fn analyze_select_wildcard_expands_catalog_columns() {
        let catalog = make_catalog();
        let statement = parse("SELECT * FROM users");

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected SELECT");
        };

        assert_eq!(select.table.table_id, TableId(1));
        assert_eq!(select.table.schema_version, 1);

        assert_eq!(
            select
                .projection
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name", "active"]
        );

        assert_eq!(select.projection[0].column_id, ColumnId(1));
        assert_eq!(select.projection[0].ordinal, 0);
        assert_eq!(select.projection[1].column_id, ColumnId(2));
        assert_eq!(select.projection[1].ordinal, 1);
        assert_eq!(select.projection[2].column_id, ColumnId(3));
        assert_eq!(select.projection[2].ordinal, 2);
    }

    #[test]
    fn analyze_select_named_columns() {
        let catalog = make_catalog();
        let statement = parse("SELECT id, name FROM users");

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected SELECT");
        };

        assert_eq!(
            select
                .projection
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name"]
        );
    }

    #[test]
    fn select_filter_retains_resolved_column_identity() {
        let catalog = make_catalog();
        let statement = parse("SELECT name FROM users WHERE id = 42");

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected SELECT");
        };

        let filter = select.filter.expect("WHERE clause must be bound");

        let BoundExprKind::Binary {
            left,
            operator,
            right,
        } = filter.kind
        else {
            panic!("expected a bound binary predicate");
        };

        assert_eq!(operator, BoundBinaryOperator::Equal);
        assert_eq!(filter.data_type, ExpressionType::Bool);
        assert!(!filter.nullable);

        let BoundExprKind::Column(column) = left.kind else {
            panic!("expected a resolved column reference");
        };

        assert_eq!(column.table_id, select.table.table_id);
        assert_eq!(column.column_id, ColumnId(1));
        assert_eq!(column.ordinal, 0);
        assert_eq!(column.name, "id");
        assert_eq!(right.kind, BoundExprKind::Literal(Value::Int(42)));
    }

    #[test]
    fn reject_select_unknown_table() {
        let catalog = MemoryCatalog::new();
        let statement = parse("SELECT * FROM ghost");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("unknown table"));
    }

    #[test]
    fn reject_select_unknown_column() {
        let catalog = make_catalog();
        let statement = parse("SELECT nonexistent FROM users");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("unknown column"));
    }

    #[test]
    fn reject_select_where_type_mismatch() {
        let catalog = make_catalog();
        let statement = parse("SELECT * FROM users WHERE id = 'abc'");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("cannot compare INT with TEXT"));
    }

    #[test]
    fn reject_non_boolean_where_expression() {
        let catalog = make_catalog();
        let statement = parse("SELECT * FROM users WHERE id");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("WHERE expression must evaluate to BOOL")
        );
    }

    #[test]
    fn supports_is_null_without_conflating_column_nullability() {
        let catalog = make_catalog();
        let statement = parse("SELECT * FROM users WHERE name IS NULL");

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected SELECT");
        };

        let filter = select.filter.expect("expected bound filter");

        assert_eq!(filter.data_type, ExpressionType::Bool);
        assert!(!filter.nullable);
    }

    #[test]
    fn rejects_equality_comparison_with_null() {
        let catalog = make_catalog();
        let statement = parse("SELECT * FROM users WHERE id = NULL");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("use IS NULL"));
    }

    #[test]
    fn accepts_boolean_predicate_composition() {
        let catalog = make_catalog();
        let statement = parse(
            "SELECT * FROM users
             WHERE id >= 1 AND active = true",
        );

        analyze(&statement, &catalog).unwrap();
    }

    #[test]
    fn rejects_arithmetic_as_final_where_result() {
        let catalog = make_catalog();
        let statement = parse("SELECT * FROM users WHERE id + 1");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("WHERE expression must evaluate to BOOL")
        );
    }

    #[test]
    fn rejects_select_distinct_instead_of_discarding_it() {
        let catalog = make_catalog();
        let statement = parse("SELECT DISTINCT id FROM users");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("DISTINCT"));
    }

    #[test]
    fn rejects_group_by_instead_of_discarding_it() {
        let catalog = make_catalog();
        let statement = parse("SELECT id FROM users GROUP BY id");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("GROUP BY"));
    }

    #[test]
    fn reject_select_with_order_by() {
        let catalog = make_catalog();
        let statement = parse("SELECT * FROM users ORDER BY id");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("ORDER BY"));
    }

    #[test]
    fn unquoted_identifiers_are_case_insensitive() {
        let catalog = make_catalog();
        let statement = parse("SELECT ID, NAME FROM USERS");

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::Select(select) = bound else {
            panic!("expected SELECT");
        };

        assert_eq!(
            select
                .projection
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name"]
        );
    }

    #[test]
    fn analyze_update_success() {
        let catalog = make_catalog();
        let statement = parse("UPDATE users SET name = 'Grace', active = false WHERE id = 1");

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::Update(update) = bound else {
            panic!("expected UPDATE");
        };

        assert_eq!(update.table.table_id, TableId(1));
        assert_eq!(update.table.schema_version, 1);
        assert_eq!(update.assignments.len(), 2);

        assert_eq!(update.assignments[0].column.column_id, ColumnId(2));
        assert_eq!(update.assignments[0].column.ordinal, 1);
        assert_eq!(update.assignments[0].column.name, "name");
        assert_eq!(
            update.assignments[0].value.kind,
            BoundExprKind::Literal(Value::Text("Grace".into()))
        );

        assert_eq!(update.assignments[1].column.column_id, ColumnId(3));
        assert_eq!(update.assignments[1].column.ordinal, 2);
        assert_eq!(update.filter.data_type, ExpressionType::Bool);
    }

    #[test]
    fn reject_update_without_where() {
        let catalog = make_catalog();
        let statement = parse("UPDATE users SET name = 'Grace'");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("requires a WHERE clause"));
    }

    #[test]
    fn reject_update_primary_key_assignment() {
        let catalog = make_catalog();
        let statement = parse("UPDATE users SET id = 2 WHERE id = 1");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("primary key"));
    }

    #[test]
    fn reject_duplicate_update_assignment() {
        let catalog = make_catalog();
        let statement = parse("UPDATE users SET name = 'A', name = 'B' WHERE id = 1");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("duplicate UPDATE assignment"));
    }

    #[test]
    fn reject_update_unknown_column() {
        let catalog = make_catalog();
        let statement = parse("UPDATE users SET missing = 1 WHERE id = 1");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("unknown column"));
    }

    #[test]
    fn reject_update_assignment_type_mismatch() {
        let catalog = make_catalog();
        let statement = parse("UPDATE users SET name = 42 WHERE id = 1");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("requires TEXT"));
    }

    #[test]
    fn reject_update_null_for_non_nullable_column() {
        let mut catalog = MemoryCatalog::new();

        catalog
            .add_table(
                "accounts",
                vec![
                    ColumnSchema {
                        id: ColumnId(1),
                        name: "id".into(),
                        ty: DataType::Int,
                        nullable: false,
                    },
                    ColumnSchema {
                        id: ColumnId(2),
                        name: "enabled".into(),
                        ty: DataType::Bool,
                        nullable: false,
                    },
                ],
                vec![ColumnId(1)],
            )
            .unwrap();

        let statement = parse("UPDATE accounts SET enabled = NULL WHERE id = 1");
        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("cannot be assigned NULL"));
    }

    #[test]
    fn reject_update_from() {
        let catalog = make_catalog();
        let statement = parse("UPDATE users SET name = 'Grace' FROM users AS source WHERE id = 1");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("UPDATE ... FROM"));
    }

    #[test]
    fn reject_update_returning() {
        let catalog = make_catalog();
        let statement = parse("UPDATE users SET name = 'Grace' WHERE id = 1 RETURNING name");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("RETURNING"));
    }

    #[test]
    fn analyze_delete_success() {
        let catalog = make_catalog();
        let statement = parse("DELETE FROM users WHERE id = 1");

        let bound = analyze(&statement, &catalog).unwrap();

        let BoundStatement::Delete(delete) = bound else {
            panic!("expected DELETE");
        };

        assert_eq!(delete.table.table_id, TableId(1));
        assert_eq!(delete.table.name, "users");
        assert_eq!(delete.table.schema_version, 1);
        assert_eq!(delete.filter.data_type, ExpressionType::Bool);
    }

    #[test]
    fn reject_delete_without_where() {
        let catalog = make_catalog();
        let statement = parse("DELETE FROM users");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("requires a WHERE clause"));
    }

    #[test]
    fn reject_delete_unknown_table() {
        let catalog = MemoryCatalog::new();
        let statement = parse("DELETE FROM ghost WHERE id = 1");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("unknown table"));
    }

    #[test]
    fn reject_delete_non_boolean_filter() {
        let catalog = make_catalog();
        let statement = parse("DELETE FROM users WHERE id + 1");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("WHERE expression must evaluate to BOOL")
        );
    }

    #[test]
    fn reject_delete_returning() {
        let catalog = make_catalog();
        let statement = parse("DELETE FROM users WHERE id = 1 RETURNING id");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("RETURNING"));
    }

    #[test]
    fn reject_delete_order_by() {
        let catalog = make_catalog();
        let statement = parse("DELETE FROM users WHERE id = 1 ORDER BY id");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("ORDER BY"));
    }

    #[test]
    fn reject_delete_limit() {
        let catalog = make_catalog();
        let statement = parse("DELETE FROM users WHERE id = 1 LIMIT 1");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("LIMIT"));
    }

    #[test]
    fn analyze_plain_begin() {
        let catalog = MemoryCatalog::new();

        assert!(matches!(
            analyze(&parse("BEGIN"), &catalog).unwrap(),
            BoundStatement::Begin
        ));
    }

    #[test]
    fn analyze_start_transaction() {
        let catalog = MemoryCatalog::new();

        assert!(matches!(
            analyze(&parse("START TRANSACTION"), &catalog).unwrap(),
            BoundStatement::Begin
        ));
    }

    #[test]
    fn reject_transaction_modes() {
        let catalog = MemoryCatalog::new();
        let statement = parse("START TRANSACTION READ ONLY");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("transaction modes"));
    }

    #[test]
    fn analyze_plain_commit() {
        let catalog = MemoryCatalog::new();

        assert!(matches!(
            analyze(&parse("COMMIT"), &catalog).unwrap(),
            BoundStatement::Commit
        ));
    }

    #[test]
    fn reject_commit_and_chain() {
        let catalog = MemoryCatalog::new();
        let statement = parse("COMMIT AND CHAIN");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("AND CHAIN"));
    }

    #[test]
    fn analyze_plain_rollback() {
        let catalog = MemoryCatalog::new();

        assert!(matches!(
            analyze(&parse("ROLLBACK"), &catalog).unwrap(),
            BoundStatement::Rollback
        ));
    }

    #[test]
    fn reject_rollback_and_chain() {
        let catalog = MemoryCatalog::new();
        let statement = parse("ROLLBACK AND CHAIN");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("AND CHAIN"));
    }

    #[test]
    fn reject_rollback_to_savepoint() {
        let catalog = MemoryCatalog::new();
        let statement = parse("ROLLBACK TO SAVEPOINT checkpoint");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("SAVEPOINT"));
    }

    #[test]
    fn analyze_plain_show_tables() {
        let catalog = MemoryCatalog::new();

        assert!(matches!(
            analyze(&parse("SHOW TABLES"), &catalog).unwrap(),
            BoundStatement::ShowTables
        ));
    }

    #[test]
    fn reject_show_tables_filter() {
        let catalog = MemoryCatalog::new();
        let statement = parse("SHOW TABLES LIKE 'user%'");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("SHOW TABLES modifiers"));
    }

    #[test]
    fn reject_unsupported_statement() {
        let catalog = MemoryCatalog::new();
        let statement = parse("DROP TABLE users");

        let error = analyze(&statement, &catalog).unwrap_err();

        assert!(error.to_string().contains("statement type"));
    }
}
