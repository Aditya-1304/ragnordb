//! Crate root for the SQL layer.
//!
//! SQL parsing, binding, and logical planning.
//!
//! ```text
//! parse_one(sql)
//!     -> parser-owned Statement
//! analyze(statement, catalog)
//!     -> RagnorDB-owned BoundStatement
//! plan(bound)
//!     -> RagnorDB-owned Plan
//! ```

pub mod analyzer;
pub mod bound;
pub mod parser;
pub mod planner;

pub use analyzer::analyze;

pub use bound::{
    BoundAssignment, BoundBinaryOperator, BoundColumnRef, BoundCreateTable, BoundDelete, BoundExpr,
    BoundExprKind, BoundInsert, BoundSelect, BoundStatement, BoundTableRef, BoundUnaryOperator,
    BoundUpdate, ExpressionType,
};

pub use parser::{Statement, parse_one};

pub use planner::{
    CreateTablePlan, DeletePlan, InsertPlan, Plan, SelectPlan, UpdateAssignmentPlan, UpdatePlan,
    plan,
};

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::Statement as SqlStatement;

    #[test]
    fn parse_create_table() {
        let statement = parse_one("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();

        assert_eq!(
            statement.raw,
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
        );
        assert!(matches!(statement.ast, SqlStatement::CreateTable(_)));
    }

    #[test]
    fn parse_insert() {
        let statement = parse_one("INSERT INTO users (id, name) VALUES (1, 'Ada')").unwrap();

        assert!(matches!(statement.ast, SqlStatement::Insert(_)));
    }

    #[test]
    fn parse_select() {
        let statement = parse_one("SELECT id, name FROM users WHERE id = 1").unwrap();

        assert!(matches!(statement.ast, SqlStatement::Query(_)));
    }

    #[test]
    fn reject_empty_statement() {
        let err = parse_one("   ").unwrap_err();

        assert!(err.to_string().contains("SQL statement is empty"));
    }

    #[test]
    fn reject_invalid_sql_with_clean_error() {
        let err = parse_one("SELECT FROM").unwrap_err();

        assert!(err.to_string().contains("SQL parse error"));
    }

    #[test]
    fn reject_multiple_statements() {
        let err = parse_one("SELECT 1; SELECT 2").unwrap_err();

        assert!(
            err.to_string()
                .contains("expected exactly one SQL statement")
        );
    }
}
