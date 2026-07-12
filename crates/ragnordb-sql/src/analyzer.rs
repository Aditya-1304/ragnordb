use std::collections::HashSet;

use ragnordb_catalog::{Catalog, ColumnSchema, TableSchema};
use ragnordb_common::catalog_codec::DataType;
use ragnordb_common::codec::Value;
use ragnordb_common::{Error, Result};
use sqlparser::ast::{
    BinaryOperator, ColumnOption, DataType as SqlDataType, Expr, GroupByExpr,
    HiveDistributionStyle, Ident, ObjectName, Query, SelectItem, SetExpr,
    Statement as SqlStatement, TableConstraint, TableFactor, UnaryOperator, Value as SqlValue,
    WildcardAdditionalOptions,
};

use crate::parser::Statement;

/// Result of resolving and type-checking one parsed statement.
///
/// The enum temporarily stores the validated `WHERE` AST until the planner
/// introduces RagnorDB-owned expression nodes
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum AnalyzedStatement {
    CreateTable(AnalyzedCreateTable),
    Insert(AnalyzedInsert),
    Select(AnalyzedSelect),
}

/// Validated information required to create a table schema.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedCreateTable {
    pub table_name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key_column_ids: Vec<u64>,
}

/// Validated literal rows for an `INSERT ... VALUES` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedInsert {
    pub table_name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

/// Validated simple-table `SELECT`.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedSelect {
    pub table_name: String,
    pub columns: Vec<SelectColumn>,
    pub selection: Option<Expr>,
}

/// Projection supported by the first local executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectColumn {
    Wildcard,
    Named(String),
}

/// Internal type returned while validating SQL expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpressionType {
    Int,
    Text,
    Bool,
    Null,
}

impl std::fmt::Display for ExpressionType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Int => "INT",
            Self::Text => "TEXT",
            Self::Bool => "BOOL",
            Self::Null => "NULL",
        };

        formatter.write_str(name)
    }
}

/// Resolve and type-check one parsed SQL statement.
///
/// for now this supports only `CREATE TABLE`, `INSERT ... VALUES`, and simple
/// single-table `SELECT`. Other statement kinds are rejected here rather than
/// being silently discarded by the planner.
pub fn analyze(statement: &Statement, catalog: &dyn Catalog) -> Result<AnalyzedStatement> {
    match &statement.ast {
        SqlStatement::CreateTable(create) => analyze_create_table(create, catalog),
        SqlStatement::Insert(insert) => analyze_insert(insert, catalog),
        SqlStatement::Query(query) => analyze_select(query, catalog),
        other => Err(unsupported(format!(
            "statement type is not supported yet: {other}"
        ))),
    }
}

fn analyze_create_table(
    create: &sqlparser::ast::CreateTable,
    catalog: &dyn Catalog,
) -> Result<AnalyzedStatement> {
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

        let mut explicit_nullability: Option<bool> = None;
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

        columns.push(ColumnSchema {
            id: u64::try_from(index + 1).map_err(|_| {
                Error::ConstraintViolation(
                    "table contains too many columns to assign stable IDs".to_string(),
                )
            })?,
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

    Ok(AnalyzedStatement::CreateTable(AnalyzedCreateTable {
        table_name,
        columns,
        primary_key_column_ids,
    }))
}

fn analyze_insert(
    insert: &sqlparser::ast::Insert,
    catalog: &dyn Catalog,
) -> Result<AnalyzedStatement> {
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
    let mut insert_columns = Vec::with_capacity(column_names.len());

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

        insert_columns.push(column);
    }

    for column in &table.columns {
        if !column.nullable && !column_names.contains(&column.name) {
            return Err(Error::ConstraintViolation(format!(
                "INSERT must include non-nullable column: {}",
                column.name
            )));
        }
    }

    // Resolve the primary-key list explicitly so malformed catalog state is
    // detected before any row reaches the planner or transaction layer.
    for primary_key_column in table.primary_key_columns()? {
        if !column_names.contains(&primary_key_column.name) {
            return Err(Error::ConstraintViolation(format!(
                "INSERT must include primary key column: {}",
                primary_key_column.name
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
        if row.len() != insert_columns.len() {
            return Err(Error::ConstraintViolation(format!(
                "INSERT row has {} values for {} columns",
                row.len(),
                insert_columns.len()
            )));
        }

        let mut analyzed_row = Vec::with_capacity(row.len());

        for (expression, column) in row.iter().zip(insert_columns.iter()) {
            let value = analyze_insert_literal(expression)?;
            validate_insert_value(&value, column)?;
            analyzed_row.push(value);
        }

        rows.push(analyzed_row);
    }

    Ok(AnalyzedStatement::Insert(AnalyzedInsert {
        table_name,
        columns: column_names,
        rows,
    }))
}

fn analyze_select(query: &Query, catalog: &dyn Catalog) -> Result<AnalyzedStatement> {
    reject_query_clauses(query)?;

    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return Err(unsupported("only simple SELECT is supported yet")),
    };

    reject_select_features(select)?;

    if select.from.len() != 1 {
        return Err(unsupported("SELECT must read from exactly one table"));
    }

    let from = &select.from[0];

    if !from.joins.is_empty() {
        return Err(unsupported("JOIN is not supported yet"));
    }

    let table_name = match &from.relation {
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
                return Err(unsupported(
                    "table aliases, functions, hints, versions, partitions, and JSON paths are not supported yet",
                ));
            }

            simple_name(name)?
        }
        _ => {
            return Err(unsupported(
                "only direct table references are supported in SELECT",
            ));
        }
    };

    let table = catalog
        .table_by_name(&table_name)
        .ok_or_else(|| Error::SchemaMismatch(format!("unknown table: {table_name}")))?;

    let mut columns = Vec::with_capacity(select.projection.len());

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(options) if wildcard_options_are_empty(options) => {
                columns.push(SelectColumn::Wildcard);
            }
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => {
                let column_name = normalize_identifier(identifier);

                if table.column_by_name(&column_name).is_none() {
                    return Err(Error::SchemaMismatch(format!(
                        "unknown column {column_name} on table {table_name}"
                    )));
                }

                columns.push(SelectColumn::Named(column_name));
            }
            other => {
                return Err(unsupported(format!(
                    "unsupported SELECT projection: {other}"
                )));
            }
        }
    }

    if let Some(selection) = &select.selection {
        let selection_type = infer_expression_type(selection, table)?;

        if selection_type != ExpressionType::Bool {
            return Err(Error::SchemaMismatch(format!(
                "WHERE expression must evaluate to BOOL, found {selection_type}"
            )));
        }
    }

    Ok(AnalyzedStatement::Select(AnalyzedSelect {
        table_name,
        columns,
        selection: select.selection.clone(),
    }))
}

fn infer_expression_type(expression: &Expr, table: &TableSchema) -> Result<ExpressionType> {
    match expression {
        Expr::Identifier(identifier) => {
            let column_name = normalize_identifier(identifier);
            let column = table.column_by_name(&column_name).ok_or_else(|| {
                Error::SchemaMismatch(format!(
                    "unknown column {column_name} on table {}",
                    table.name
                ))
            })?;

            Ok(expression_type_for_column(column))
        }
        Expr::Value(value) => expression_type_for_literal(value),
        Expr::Nested(inner) => infer_expression_type(inner, table),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            infer_expression_type(inner, table)?;
            Ok(ExpressionType::Bool)
        }
        Expr::UnaryOp { op, expr } => infer_unary_expression_type(op, expr, table),
        Expr::BinaryOp { left, op, right } => infer_binary_expression_type(left, op, right, table),
        other => Err(unsupported(format!("unsupported expression: {other}"))),
    }
}

fn infer_unary_expression_type(
    operator: &UnaryOperator,
    expression: &Expr,
    table: &TableSchema,
) -> Result<ExpressionType> {
    // Validate the complete signed integer boundary, including i64::MIN.
    if let Expr::Value(SqlValue::Number(value, _)) = expression {
        match operator {
            UnaryOperator::Minus => {
                parse_integer_literal(value, true)?;
                return Ok(ExpressionType::Int);
            }
            UnaryOperator::Plus => {
                parse_integer_literal(value, false)?;
                return Ok(ExpressionType::Int);
            }
            _ => {}
        }
    }

    let operand_type = infer_expression_type(expression, table)?;

    match operator {
        UnaryOperator::Plus | UnaryOperator::Minus if operand_type == ExpressionType::Int => {
            Ok(ExpressionType::Int)
        }
        UnaryOperator::Not if operand_type == ExpressionType::Bool => Ok(ExpressionType::Bool),
        UnaryOperator::Plus | UnaryOperator::Minus => Err(Error::SchemaMismatch(format!(
            "unary {operator} requires INT, found {operand_type}"
        ))),
        UnaryOperator::Not => Err(Error::SchemaMismatch(format!(
            "NOT requires BOOL, found {operand_type}"
        ))),
        _ => Err(unsupported(format!(
            "unsupported unary operator: {operator}"
        ))),
    }
}

fn infer_binary_expression_type(
    left: &Expr,
    operator: &BinaryOperator,
    right: &Expr,
    table: &TableSchema,
) -> Result<ExpressionType> {
    let left_type = infer_expression_type(left, table)?;
    let right_type = infer_expression_type(right, table)?;

    match operator {
        BinaryOperator::And | BinaryOperator::Or => {
            require_types(operator, left_type, right_type, ExpressionType::Bool)?;
            Ok(ExpressionType::Bool)
        }
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo => {
            require_types(operator, left_type, right_type, ExpressionType::Int)?;
            Ok(ExpressionType::Int)
        }
        BinaryOperator::Eq | BinaryOperator::NotEq => {
            require_comparable_types(operator, left_type, right_type, false)?;
            Ok(ExpressionType::Bool)
        }
        BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq => {
            require_comparable_types(operator, left_type, right_type, true)?;
            Ok(ExpressionType::Bool)
        }
        _ => Err(unsupported(format!(
            "unsupported binary operator: {operator}"
        ))),
    }
}

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

fn expression_type_for_column(column: &ColumnSchema) -> ExpressionType {
    match column.ty {
        DataType::Int => ExpressionType::Int,
        DataType::Text => ExpressionType::Text,
        DataType::Bool => ExpressionType::Bool,
    }
}

fn expression_type_for_literal(value: &SqlValue) -> Result<ExpressionType> {
    match value {
        SqlValue::Number(value, _) => {
            parse_integer_literal(value, false)?;
            Ok(ExpressionType::Int)
        }
        SqlValue::SingleQuotedString(_) => Ok(ExpressionType::Text),
        SqlValue::Boolean(_) => Ok(ExpressionType::Bool),
        SqlValue::Null => Ok(ExpressionType::Null),
        other => Err(unsupported(format!("unsupported literal: {other}"))),
    }
}

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

fn validate_insert_value(value: &Value, column: &ColumnSchema) -> Result<()> {
    match value {
        Value::Null if column.nullable => Ok(()),
        Value::Null => Err(Error::ConstraintViolation(format!(
            "column {} cannot be NULL",
            column.name
        ))),
        Value::Int(_) if column.ty == DataType::Int => Ok(()),
        Value::Text(_) if column.ty == DataType::Text => Ok(()),
        Value::Bool(_) if column.ty == DataType::Bool => Ok(()),
        _ => Err(Error::SchemaMismatch(format!(
            "value for column {} does not match type {:?}",
            column.name, column.ty
        ))),
    }
}

fn analyze_data_type(data_type: &SqlDataType) -> Result<DataType> {
    match data_type {
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Int),
        SqlDataType::Text => Ok(DataType::Text),
        SqlDataType::Bool | SqlDataType::Boolean => Ok(DataType::Bool),
        other => Err(unsupported(format!("unsupported data type: {other}"))),
    }
}

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

fn register_primary_key(current: &mut Option<Vec<String>>, columns: Vec<String>) -> Result<()> {
    if current.is_some() {
        return Err(Error::ConstraintViolation(
            "table defines more than one primary key".to_string(),
        ));
    }

    *current = Some(columns);
    Ok(())
}

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
        || create.hive_formats.is_some()
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

fn wildcard_options_are_empty(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
}

/// Convert an SQL identifier to its catalog form.
///
/// Unquoted names are folded to lowercase. Quoted names preserve their exact
/// spelling, giving predictable case-insensitive behavior for ordinary SQL and
/// case-sensitive behavior when explicitly requested by the client.
fn normalize_identifier(identifier: &Ident) -> String {
    if identifier.quote_style.is_some() {
        identifier.value.clone()
    } else {
        identifier.value.to_lowercase()
    }
}

fn simple_name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(unsupported(format!(
            "qualified names are not supported yet: {name}"
        )));
    }

    Ok(normalize_identifier(&name.0[0]))
}

fn unsupported(message: impl Into<String>) -> Error {
    Error::UnsupportedSql(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_catalog::{ColumnSchema, MemoryCatalog};
    use ragnordb_common::catalog_codec::DataType;
    use ragnordb_common::codec::Value;

    fn make_catalog() -> MemoryCatalog {
        let mut catalog = MemoryCatalog::new();
        catalog
            .add_table(
                "users",
                vec![
                    ColumnSchema {
                        id: 1,
                        name: "id".into(),
                        ty: DataType::Int,
                        nullable: false,
                    },
                    ColumnSchema {
                        id: 2,
                        name: "name".into(),
                        ty: DataType::Text,
                        nullable: true,
                    },
                    ColumnSchema {
                        id: 3,
                        name: "active".into(),
                        ty: DataType::Bool,
                        nullable: true,
                    },
                ],
                vec![1],
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
        let stmt = parse("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)");
        let analyzed = analyze(&stmt, &catalog).unwrap();
        match analyzed {
            AnalyzedStatement::CreateTable(t) => {
                assert_eq!(t.table_name, "items");
                assert_eq!(t.columns.len(), 2);
                assert_eq!(t.primary_key_column_ids, vec![1]);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn reject_create_duplicate_table() {
        let mut catalog = MemoryCatalog::new();
        catalog
            .add_table(
                "items",
                vec![ColumnSchema {
                    id: 1,
                    name: "id".into(),
                    ty: DataType::Int,
                    nullable: false,
                }],
                vec![1],
            )
            .unwrap();
        let stmt = parse("CREATE TABLE items (id INT PRIMARY KEY)");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn reject_create_no_pk() {
        let catalog = MemoryCatalog::new();
        let stmt = parse("CREATE TABLE items (id INT)");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("primary key"));
    }

    #[test]
    fn reject_unsupported_data_type() {
        let catalog = MemoryCatalog::new();
        let stmt = parse("CREATE TABLE items (id FLOAT PRIMARY KEY)");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("unsupported data type"));
    }

    #[test]
    fn analyze_insert_success() {
        let catalog = make_catalog();
        let stmt = parse("INSERT INTO users (id, name, active) VALUES (1, 'Ada', true)");
        let analyzed = analyze(&stmt, &catalog).unwrap();
        match analyzed {
            AnalyzedStatement::Insert(ins) => {
                assert_eq!(ins.table_name, "users");
                assert_eq!(ins.rows.len(), 1);
                assert_eq!(ins.rows[0][0], Value::Int(1));
                assert_eq!(ins.rows[0][1], Value::Text("Ada".into()));
                assert_eq!(ins.rows[0][2], Value::Bool(true));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn reject_insert_unknown_table() {
        let catalog = MemoryCatalog::new();
        let stmt = parse("INSERT INTO ghost (id) VALUES (1)");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("unknown table"));
    }

    #[test]
    fn reject_insert_missing_pk() {
        let catalog = make_catalog();
        let stmt = parse("INSERT INTO users (name) VALUES ('Ada')");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("primary key"));
    }

    #[test]
    fn reject_insert_unknown_column() {
        let catalog = make_catalog();
        let stmt = parse("INSERT INTO users (id, nonexistent) VALUES (1, 'x')");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("unknown column"));
    }

    #[test]
    fn reject_insert_wrong_type() {
        let catalog = make_catalog();
        let stmt = parse("INSERT INTO users (id, name) VALUES ('abc', 'Ada')");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("type"));
    }

    #[test]
    fn reject_insert_null_into_not_null_column() {
        let catalog = make_catalog();
        let stmt = parse("INSERT INTO users (id, name) VALUES (NULL, 'Ada')");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("NULL"));
    }

    #[test]
    fn reject_insert_wrong_value_count() {
        let catalog = make_catalog();
        let stmt = parse("INSERT INTO users (id, name) VALUES (1, 'Ada', true)");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("values"));
    }

    #[test]
    fn analyze_select_wildcard() {
        let catalog = make_catalog();
        let stmt = parse("SELECT * FROM users");
        let analyzed = analyze(&stmt, &catalog).unwrap();
        match analyzed {
            AnalyzedStatement::Select(s) => {
                assert_eq!(s.table_name, "users");
                assert_eq!(s.columns, vec![SelectColumn::Wildcard]);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn analyze_select_named_columns() {
        let catalog = make_catalog();
        let stmt = parse("SELECT id, name FROM users");
        let analyzed = analyze(&stmt, &catalog).unwrap();
        match analyzed {
            AnalyzedStatement::Select(s) => {
                assert_eq!(s.table_name, "users");
                assert_eq!(
                    s.columns,
                    vec![
                        SelectColumn::Named("id".into()),
                        SelectColumn::Named("name".into())
                    ]
                );
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn analyze_select_with_where() {
        let catalog = make_catalog();
        let stmt = parse("SELECT * FROM users WHERE id = 1");
        analyze(&stmt, &catalog).unwrap();
    }

    #[test]
    fn reject_select_unknown_table() {
        let catalog = MemoryCatalog::new();
        let stmt = parse("SELECT * FROM ghost");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("unknown table"));
    }

    #[test]
    fn reject_select_unknown_column() {
        let catalog = make_catalog();
        let stmt = parse("SELECT nonexistent FROM users");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("unknown column"));
    }

    #[test]
    fn reject_select_where_wrong_type_int_vs_text() {
        let catalog = make_catalog();
        let stmt = parse("SELECT * FROM users WHERE id = 'abc'");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("type"));
    }

    #[test]
    fn reject_select_where_wrong_type_text_vs_int() {
        let catalog = make_catalog();
        let stmt = parse("SELECT * FROM users WHERE name = 123");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("type"));
    }

    #[test]
    fn reject_select_where_wrong_type_bool() {
        let catalog = make_catalog();
        let stmt = parse("SELECT * FROM users WHERE active = 42");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("type"));
    }

    #[test]
    fn reject_unsupported_statement() {
        let catalog = MemoryCatalog::new();
        let stmt = parse("DROP TABLE users");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("unsupported SQL statement"));
    }

    #[test]
    fn reject_select_with_order_by() {
        let catalog = make_catalog();
        let stmt = parse("SELECT * FROM users ORDER BY id");
        let err = analyze(&stmt, &catalog).unwrap_err();
        assert!(err.to_string().contains("unsupported SELECT"));
    }
}
