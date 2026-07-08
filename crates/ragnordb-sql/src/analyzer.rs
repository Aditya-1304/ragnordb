use std::collections::HashSet;

use ragnordb_catalog::{Catalog, ColumnSchema};
use ragnordb_common::catalog_codec::DataType;
use ragnordb_common::codec::Value;
use ragnordb_common::{Error, Result};
use sqlparser::ast::{
    ColumnOption, DataType as SqlDataType, Expr, ObjectName, Query, SelectItem, SetExpr,
    Statement as SqlStatement, TableConstraint, TableFactor, Value as SqlValue,
};

use crate::parser::Statement;

#[derive(Debug, Clone, PartialEq)]
pub enum AnalyzedStatement {
    CreateTable(AnalyzedCreateTable),
    Insert(AnalyzedInsert),
    Select(AnalyzedSelect),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedCreateTable {
    pub table_name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key_column_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedInsert {
    pub table_name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedSelect {
    pub table_name: String,
    pub columns: Vec<SelectColumn>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectColumn {
    Wildcard,
    Named(String),
}

pub fn analyze(statement: &Statement, catalog: &dyn Catalog) -> Result<AnalyzedStatement> {
    match &statement.ast {
        SqlStatement::CreateTable(create) => analyze_create_table(create, catalog),
        SqlStatement::Insert(insert) => analyze_insert(insert, catalog),
        SqlStatement::Query(query) => analyze_select(query, catalog),
        other => Err(unsupported(format!("unsupported SQL statement: {other}"))),
    }
}

fn analyze_create_table(
    create: &sqlparser::ast::CreateTable,
    catalog: &dyn Catalog,
) -> Result<AnalyzedStatement> {
    let table_name = simple_name(&create.name)?;

    if catalog.table_by_name(&table_name).is_some() {
        return Err(Error::InvalidArgument(format!(
            "table already exists: {table_name}"
        )));
    }

    if create.columns.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "table {table_name} must define at least one column"
        )));
    }

    if create.query.is_some() {
        return Err(unsupported("CREATE TABLE AS SELECT is not supported yet"));
    }

    let mut seen_columns = HashSet::new();
    let mut columns = Vec::new();
    let mut primary_key_names = Vec::new();

    for (idx, column) in create.columns.iter().enumerate() {
        let name = column.name.value.clone();

        if !seen_columns.insert(name.clone()) {
            return Err(Error::InvalidArgument(format!("duplicate column: {name}")));
        }

        let mut nullable = true;

        for option in &column.options {
            match &option.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Null => nullable = true,
                ColumnOption::Unique {
                    is_primary: true, ..
                } => {
                    nullable = false;
                    primary_key_names.push(name.clone());
                }
                other => {
                    return Err(unsupported(format!(
                        "unsupported column option on {name}: {other}"
                    )));
                }
            }
        }

        columns.push(ColumnSchema {
            id: (idx + 1) as u64,
            name,
            ty: analyze_data_type(&column.data_type)?,
            nullable,
        });
    }

    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey { columns, .. } => {
                for column in columns {
                    primary_key_names.push(column.value.clone());
                }
            }
            other => {
                return Err(unsupported(format!(
                    "unsupported table constraint: {other}"
                )));
            }
        }
    }

    if primary_key_names.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "table {table_name} must define a primary key"
        )));
    }

    let mut primary_key_column_ids = Vec::new();

    for pk_name in &primary_key_names {
        let column = columns
            .iter_mut()
            .find(|column| column.name == *pk_name)
            .ok_or_else(|| {
                Error::InvalidArgument(format!("primary key column does not exist: {pk_name}"))
            })?;

        column.nullable = false;

        if !primary_key_column_ids.contains(&column.id) {
            primary_key_column_ids.push(column.id);
        }
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
    let table_name = simple_name(&insert.table_name)?;
    let table = catalog
        .table_by_name(&table_name)
        .ok_or_else(|| Error::InvalidArgument(format!("unknown table: {table_name}")))?;

    if insert.columns.is_empty() {
        return Err(unsupported(
            "INSERT without an explicit column list is not supported yet",
        ));
    }

    let column_names = insert
        .columns
        .iter()
        .map(|column| column.value.clone())
        .collect::<Vec<_>>();

    let mut seen_columns = HashSet::new();
    let mut insert_columns = Vec::new();

    for column_name in &column_names {
        if !seen_columns.insert(column_name.clone()) {
            return Err(Error::InvalidArgument(format!(
                "duplicate INSERT column: {column_name}"
            )));
        }

        let column = table.column_by_name(column_name).ok_or_else(|| {
            Error::InvalidArgument(format!(
                "unknown column {column_name} on table {table_name}"
            ))
        })?;

        insert_columns.push(column);
    }

    for column in table.primary_key_columns() {
        if !column_names.contains(&column.name) {
            return Err(Error::InvalidArgument(format!(
                "INSERT must include primary key column: {}",
                column.name
            )));
        }
    }

    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| unsupported("INSERT source is required"))?;

    let values = match source.body.as_ref() {
        SetExpr::Values(values) => values,
        _ => return Err(unsupported("only INSERT ... VALUES is supported yet")),
    };

    let mut rows = Vec::new();

    for row in &values.rows {
        if row.len() != insert_columns.len() {
            return Err(Error::InvalidArgument(format!(
                "INSERT row has {} values for {} columns",
                row.len(),
                insert_columns.len()
            )));
        }

        let mut analyzed_row = Vec::new();

        for (expr, column) in row.iter().zip(insert_columns.iter()) {
            let value = analyze_literal(expr)?;
            validate_value_type(&value, column)?;
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
            partitions,
            ..
        } => {
            if alias.is_some()
                || args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || !partitions.is_empty()
            {
                return Err(unsupported("table aliases/options are not supported yet"));
            }

            simple_name(name)?
        }
        _ => return Err(unsupported("only table scans are supported in SELECT")),
    };

    let table = catalog
        .table_by_name(&table_name)
        .ok_or_else(|| Error::InvalidArgument(format!("unknown table: {table_name}")))?;

    let mut columns = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => columns.push(SelectColumn::Wildcard),
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let column_name = ident.value.clone();

                if table.column_by_name(&column_name).is_none() {
                    return Err(Error::InvalidArgument(format!(
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
        validate_expr_columns(selection, table)?;
        validate_where_types(selection, table)?;
    }

    Ok(AnalyzedStatement::Select(AnalyzedSelect {
        table_name,
        columns,
        selection: select.selection.clone(),
    }))
}

fn validate_expr_columns(expr: &Expr, table: &ragnordb_catalog::TableSchema) -> Result<()> {
    match expr {
        Expr::Identifier(ident) => {
            if table.column_by_name(&ident.value).is_none() {
                return Err(Error::InvalidArgument(format!(
                    "unknown column {} on table {}",
                    ident.value, table.name
                )));
            }
            Ok(())
        }
        Expr::Value(_) => Ok(()),
        Expr::BinaryOp { left, right, .. } => {
            validate_expr_columns(left, table)?;
            validate_expr_columns(right, table)
        }
        Expr::Nested(e) => validate_expr_columns(e, table),
        Expr::UnaryOp { expr: e, .. } => validate_expr_columns(e, table),
        other => Err(unsupported(format!("unsupported expression: {other}"))),
    }
}

fn validate_where_types(expr: &Expr, table: &ragnordb_catalog::TableSchema) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            validate_where_types(left, table)?;
            validate_where_types(right, table)?;

            let col_name = get_column_name(left).or_else(|| get_column_name(right));
            let lit_expr = if is_literal(left) {
                Some(left)
            } else if is_literal(right) {
                Some(right)
            } else {
                None
            };

            if let (Some(col_name), Some(lit)) = (col_name, lit_expr) {
                let column = table
                    .column_by_name(&col_name)
                    .ok_or_else(|| Error::InvalidArgument(format!("unknown column: {col_name}")))?;
                let value = analyze_literal(lit)?;
                validate_value_type(&value, column)?;
            }

            Ok(())
        }
        Expr::Nested(e) => validate_where_types(e, table),
        Expr::UnaryOp { expr: e, .. } => validate_where_types(e, table),
        Expr::Identifier(_) | Expr::Value(_) => Ok(()),
        other => Err(unsupported(format!("unsupported expression: {other}"))),
    }
}

fn get_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        _ => None,
    }
}

fn is_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(_))
}

fn analyze_data_type(data_type: &SqlDataType) -> Result<DataType> {
    match data_type {
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Int),
        SqlDataType::Text => Ok(DataType::Text),
        SqlDataType::Bool | SqlDataType::Boolean => Ok(DataType::Bool),
        other => Err(unsupported(format!("unsupported data type: {other}"))),
    }
}

fn analyze_literal(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(SqlValue::Number(value, _)) => {
            let parsed = value
                .parse::<i64>()
                .map_err(|_| Error::InvalidArgument(format!("invalid INT literal: {value}")))?;
            Ok(Value::Int(parsed))
        }
        Expr::Value(SqlValue::SingleQuotedString(value)) => Ok(Value::Text(value.clone())),
        Expr::Value(SqlValue::Boolean(value)) => Ok(Value::Bool(*value)),
        Expr::Value(SqlValue::Null) => Ok(Value::Null),
        other => Err(unsupported(format!(
            "unsupported literal expression: {other}"
        ))),
    }
}

fn validate_value_type(value: &Value, column: &ColumnSchema) -> Result<()> {
    match value {
        Value::Null if column.nullable => Ok(()),
        Value::Null => Err(Error::InvalidArgument(format!(
            "column {} cannot be NULL",
            column.name
        ))),
        Value::Int(_) if column.ty == DataType::Int => Ok(()),
        Value::Text(_) if column.ty == DataType::Text => Ok(()),
        Value::Bool(_) if column.ty == DataType::Bool => Ok(()),
        _ => Err(Error::InvalidArgument(format!(
            "value for column {} does not match type {:?}",
            column.name, column.ty
        ))),
    }
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
        return Err(unsupported("unsupported SELECT query clause"));
    }
    Ok(())
}

fn simple_name(name: &ObjectName) -> Result<String> {
    let name = name.to_string();
    if name.contains('.') {
        return Err(unsupported(format!(
            "qualified names are not supported yet: {name}"
        )));
    }
    Ok(name)
}

fn unsupported(message: impl Into<String>) -> Error {
    Error::InvalidArgument(message.into())
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
