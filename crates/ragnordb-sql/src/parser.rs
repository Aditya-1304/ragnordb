use ragnordb_common::{Error, Result};
use sqlparser::ast::Statement as SqlStatement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// A single parsed SQL statement.
///
/// raw: the original SQL text (trimmed), will be used for logging and debugging
/// ast: the sqlparser AST node, will be used by
/// the analyzer to extract table/column/type information.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    pub raw: String,
    pub ast: SqlStatement,
}

/// This function parse exactly one SQL statement from a string
///
/// Returns an error if:
///   - The input is empty or whitespace-only
///   - The SQL is syntactically invalid (delegates to sqlparser)
///   - The input contains more than one statement (semicolons)
///
/// The GenericDialect is used, which supports standard SQL syntax
/// compatible with most common databases
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
