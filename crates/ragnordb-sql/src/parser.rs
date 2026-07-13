use ragnordb_common::{Error, Result};
use sqlparser::ast::Statement as SqlStatement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// this contains one syntactically valid SQL statement
///
/// `raw` preserves the trimmed client input for diagnostics and structured
/// logging.
/// `ast` is consumed exclusively by the analyzer. Planner and executor
/// code must not depend directly on `sqlparser` AST types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    pub raw: String,
    pub(crate) ast: SqlStatement,
}

/// Parse exactly one SQL statement from a client request.
///
/// The generic dialect deliberately accepts a broad SQL grammar. The analyzer
/// is therefore the enforcement boundary for RagnorDB's supported SQL subset
/// and must explicitly reject every AST feature it does not preserve.
///
/// # Errors
///
/// Returns an error when:
///
/// - the input is empty or contains only comments;
/// - `sqlparser` reports invalid syntax;
/// - the input contains more than one statement.
pub fn parse_one(sql: &str) -> Result<Statement> {
    let raw = sql.trim();

    if raw.is_empty() {
        return Err(Error::InvalidArgument("SQL statement is empty".to_string()));
    }

    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, raw).map_err(|error| Error::SqlParse(error.to_string()))?;

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
