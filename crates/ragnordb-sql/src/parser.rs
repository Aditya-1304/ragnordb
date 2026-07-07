use ragnordb_common::{Error, Result};
use sqlparser::ast::Statement as SqlStatement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
