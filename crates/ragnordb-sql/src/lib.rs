use ragnordb_common::{Error, Result};
use sqlparser::ast::Statement as SqlStatement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub mod parser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    pub raw: String,
    pub ast: SqlStatement,
}

pub fn parse_one(sql: &str) -> Result<Statement> {
    let raw = sql.trim();

    if raw.is_empty() {
        return Err(Error::InvalidArgument("SQL statement is empty".to_string()));
    }

    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, raw)
        .map_err(|e| Error::InvalidArgument(format!("SQL parse error: {e}")))?;

    match statements.len() {
        0 => Err(Error::InvalidArgument("SQL statement is empty".to_string())),
        1 => Ok(Statement {
            raw: raw.to_string(),
            ast: statements.remove(0),
        }),
        count => Err(Error::InvalidArgument(format!(
            "expected exactly one SQL statement, got {count}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
