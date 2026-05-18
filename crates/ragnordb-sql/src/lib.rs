use ragnordb_common::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    pub raw: String,
}

pub fn parse_one(sql: &str) -> Result<Statement> {
    let raw = sql.trim();

    if raw.is_empty() {
        return Err(Error::InvalidArgument("SQL statement is empty".to_string()));
    }

    Ok(Statement {
        raw: raw.to_string(),
    })
}
