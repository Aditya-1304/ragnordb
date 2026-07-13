//! fully resolved SQL statements and expressions.
//!
//! These types are entirely owned by RagnorDB. They deliberately expose no
//! `sqlparser` AST values, identifiers, values, or operators.

use ragnordb_catalog::ColumnSchema;
use ragnordb_common::catalog_codec::DataType;
use ragnordb_common::codec::Value;
use ragnordb_common::ids::{ColumnId, TableId};

/// bound immutable table identity
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundTableRef {
    pub table_id: TableId,
    pub name: String,
    pub schema_version: u64,
}

/// Bound column identity and row layout position
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundColumnRef {
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub ordinal: usize,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Result type of a bound scalar expression
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionType {
    Int,
    Text,
    Bool,
    Null,
}

impl std::fmt::Display for ExpressionType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Int => "INT",
            Self::Text => "TEXT",
            Self::Bool => "BOOL",
            Self::Null => "NULL",
        })
    }
}

/// Supported bound unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundUnaryOperator {
    Positive,
    Negative,
    Not,
}

/// Supported bound binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundBinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,

    And,
    Or,
}

/// Fully bound and typed scalar expression
#[derive(Debug, Clone, PartialEq)]
pub struct BoundExpr {
    pub kind: BoundExprKind,
    pub data_type: ExpressionType,

    /// Whether evaluation may produce SQL NULL.
    pub nullable: bool,
}

/// Bound scalar expression kind
#[derive(Debug, Clone, PartialEq)]
pub enum BoundExprKind {
    Column(BoundColumnRef),
    Literal(Value),

    Unary {
        operator: BoundUnaryOperator,
        expression: Box<BoundExpr>,
    },

    Binary {
        left: Box<BoundExpr>,
        operator: BoundBinaryOperator,
        right: Box<BoundExpr>,
    },

    IsNull {
        expression: Box<BoundExpr>,
        negated: bool,
    },
}

/// Validated CREATE TABLE definition before catalog publication
///
/// The catalog assigns `TableId`, schema version one, and local tablet count
/// one when the definition is published
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundCreateTable {
    pub table_name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key_column_ids: Vec<ColumnId>,
}

/// Fully bound INSERT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundInsert {
    pub table: BoundTableRef,
    pub target_columns: Vec<BoundColumnRef>,
    pub rows: Vec<Vec<Value>>,
}

/// Fully bound SELECT statement
///
/// Wildcards have already been expanded into concrete ordered columns
#[derive(Debug, Clone, PartialEq)]
pub struct BoundSelect {
    pub table: BoundTableRef,
    pub projection: Vec<BoundColumnRef>,
    pub filter: Option<BoundExpr>,
}

/// One fully bound UPDATE assignment
#[derive(Debug, Clone, PartialEq)]
pub struct BoundAssignment {
    pub column: BoundColumnRef,
    pub value: BoundExpr,
}

/// Fully bound UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct BoundUpdate {
    pub table: BoundTableRef,
    pub assignments: Vec<BoundAssignment>,
    pub filter: BoundExpr,
}

/// Fully bound DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct BoundDelete {
    pub table: BoundTableRef,
    pub filter: BoundExpr,
}

/// Final output of semantic analysis and binding
#[derive(Debug, Clone, PartialEq)]
pub enum BoundStatement {
    CreateTable(BoundCreateTable),
    Insert(BoundInsert),
    Select(BoundSelect),
    Update(BoundUpdate),
    Delete(BoundDelete),
    Begin,
    Commit,
    Rollback,
    ShowTables,
}
