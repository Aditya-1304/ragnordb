//! Crate root for the SQL layer.
//!
//! Re-exports the two public APIs:
//!   parse_one(&str) -> Result<Statement>
//!   analyze(&Statement, &dyn Catalog) -> Result<AnalyzedStatement>
//!
//! Also re-exports the core types:
//!   Statement            — parsed SQL (raw + AST)
//!   AnalyzedStatement    — analyzed statement with resolved names & types
//!
//! This is the only public surface other crates (ragnordb-server)
//! should use

pub mod analyzer;
pub mod parser;

pub use analyzer::{AnalyzedStatement, analyze};
pub use parser::{Statement, parse_one};

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
