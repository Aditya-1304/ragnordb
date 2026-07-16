//! conversion between internal execution results and V1 client JSON
//!
//! JSON is intentionally confined to the server protocol boundary. The SQL
//! executor returns typed RagnorDB values and remains reusable by future binary
//! or PostgreSQL-compatible protocols.

use ragnordb_common::{Error, codec::Value as SqlValue};
use ragnordb_exec::{DmlOperation, ExecutionResult};
use serde_json::{Value as JsonValue, json};

/// row counters reported for one execution result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionStats {
    pub rows_read: u64,
    pub rows_written: u64,
}

/// Converts a successful internal execution result into V1 response JSON.
pub fn execution_response(result: &ExecutionResult) -> JsonValue {
    let stats = execution_stats(result);

    match result {
        ExecutionResult::CreatedTable { table_id } => json!({
            "ok": true,
            "result": {
                "type": "created_table",
                "table_id": table_id.0
            },
            "columns": [],
            "rows": [],
            "stats": {
                "rows_read": stats.rows_read,
                "rows_written": stats.rows_written
            }
        }),

        ExecutionResult::Mutation {
            operation,
            affected_rows,
        } => json!({
            "ok": true,
            "result": {
                "type": "mutation",
                "operation": mutation_name(*operation),
                "affected_rows": affected_rows
            },
            "columns": [],
            "rows": [],
            "stats": {
                "rows_read": stats.rows_read,
                "rows_written": stats.rows_written
            }
        }),

        ExecutionResult::Query(result_set) => {
            let columns = result_set
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();

            let rows = result_set
                .rows
                .iter()
                .map(|row| row.values.iter().map(sql_value_to_json).collect::<Vec<_>>())
                .collect::<Vec<_>>();

            json!({
                "ok": true,
                "columns": columns,
                "rows": rows,
                "stats": {
                    "rows_read": stats.rows_read,
                    "rows_written": stats.rows_written
                }
            })
        }

        ExecutionResult::TransactionStarted {
            transaction_id,
            start_ts,
        } => json!({
            "ok": true,
            "result": {
                "type": "transaction_started",
                "transaction_id": transaction_id.0,
                "start_timestamp": start_ts.0
            },
            "columns": [],
            "rows": [],
            "stats": {
                "rows_read": stats.rows_read,
                "rows_written": stats.rows_written
            }
        }),

        ExecutionResult::TransactionCommitted {
            transaction_id,
            commit_ts,
            committed_writes,
        } => {
            let commit_timestamp = commit_ts.as_ref().map(|timestamp| timestamp.0);

            json!({
                "ok": true,
                "result": {
                    "type": "transaction_committed",
                    "transaction_id": transaction_id.0,
                    "commit_timestamp": commit_timestamp,
                    "committed_writes": committed_writes
                },
                "columns": [],
                "rows": [],
                "stats": {
                    "rows_read": stats.rows_read,
                    "rows_written": stats.rows_written
                }
            })
        }

        ExecutionResult::TransactionRolledBack {
            transaction_id,
            discarded_writes,
        } => json!({
            "ok": true,
            "result": {
                "type": "transaction_rolled_back",
                "transaction_id": transaction_id.0,
                "discarded_writes": discarded_writes
            },
            "columns": [],
            "rows": [],
            "stats": {
                "rows_read": stats.rows_read,
                "rows_written": stats.rows_written
            }
        }),
    }
}

/// Return row counters represented by one execution result.
pub fn execution_stats(result: &ExecutionResult) -> ExecutionStats {
    match result {
        ExecutionResult::Query(result_set) => ExecutionStats {
            rows_read: usize_to_u64(result_set.rows.len()),
            rows_written: 0,
        },

        ExecutionResult::Mutation { affected_rows, .. } => ExecutionStats {
            rows_read: 0,
            rows_written: usize_to_u64(*affected_rows),
        },

        ExecutionResult::TransactionCommitted {
            committed_writes, ..
        } => ExecutionStats {
            rows_read: 0,
            rows_written: usize_to_u64(*committed_writes),
        },

        ExecutionResult::CreatedTable { .. }
        | ExecutionResult::TransactionStarted { .. }
        | ExecutionResult::TransactionRolledBack { .. } => ExecutionStats {
            rows_read: 0,
            rows_written: 0,
        },
    }
}

/// Convert a canonical internal error into a V1 error response.
///
/// Internal corruption and configuration details are logged by the server but
/// deliberately replaced with a safe client-facing message.
pub fn internal_error_response(error: &Error) -> JsonValue {
    match error {
        Error::WriteConflict(_) => error_response("WRITE_CONFLICT", &error.to_string(), true),

        Error::ConstraintViolation(_) => {
            error_response("CONSTRAINT_VIOLATION", &error.to_string(), false)
        }

        Error::SchemaMismatch(_) => error_response("SCHEMA_MISMATCH", &error.to_string(), false),

        Error::UnsupportedSql(_) => error_response("UNSUPPORTED_SQL", &error.to_string(), false),

        Error::SqlParse(_) => error_response("SQL_PARSE_ERROR", &error.to_string(), false),

        Error::InvalidArgument(_) => error_response("INVALID_ARGUMENT", &error.to_string(), false),

        Error::NotImplemented(_) => error_response("UNSUPPORTED_SQL", &error.to_string(), false),

        Error::CorruptData(_) | Error::Configuration(_) => error_response(
            "INTERNAL_ERROR",
            "an internal database error occurred",
            false,
        ),
    }
}

/// Construct a V1 client error response.
pub fn error_response(code: &str, message: &str, retryable: bool) -> JsonValue {
    json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message,
            "retryable": retryable
        }
    })
}

fn mutation_name(operation: DmlOperation) -> &'static str {
    match operation {
        DmlOperation::Insert => "insert",
        DmlOperation::Update => "update",
        DmlOperation::Delete => "delete",
    }
}

fn sql_value_to_json(value: &SqlValue) -> JsonValue {
    match value {
        SqlValue::Int(value) => json!(*value),
        SqlValue::Text(value) => json!(value),
        SqlValue::Bool(value) => json!(*value),
        SqlValue::Null => JsonValue::Null,
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::{catalog_codec::DataType, codec::Row};
    use ragnordb_exec::{ResultColumn, ResultSet};

    #[test]
    fn query_response_converts_every_sql_value_type() {
        let result = ExecutionResult::Query(ResultSet {
            columns: vec![
                ResultColumn {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ResultColumn {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                },
                ResultColumn {
                    name: "active".to_string(),
                    data_type: DataType::Bool,
                    nullable: false,
                },
                ResultColumn {
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
            ],
            rows: vec![Row {
                values: vec![
                    SqlValue::Int(1),
                    SqlValue::Text("Ada".to_string()),
                    SqlValue::Bool(true),
                    SqlValue::Null,
                ],
            }],
        });

        let response = execution_response(&result);

        assert_eq!(response["ok"], true);
        assert_eq!(response["columns"], json!(["id", "name", "active", "note"]));
        assert_eq!(response["rows"], json!([[1, "Ada", true, null]]));
        assert_eq!(response["stats"]["rows_read"], 1);
        assert_eq!(response["stats"]["rows_written"], 0);
    }

    #[test]
    fn internal_errors_do_not_expose_corruption_details() {
        let response = internal_error_response(&Error::CorruptData(
            "secret internal storage detail".to_string(),
        ));

        assert_eq!(response["error"]["code"], "INTERNAL_ERROR");
        assert_eq!(
            response["error"]["message"],
            "an internal database error occurred"
        );
        assert!(
            !response
                .to_string()
                .contains("secret internal storage detail")
        );
    }

    #[test]
    fn write_conflicts_are_retryable() {
        let response = internal_error_response(&Error::WriteConflict(
            "conflicting committed version".to_string(),
        ));

        assert_eq!(response["error"]["code"], "WRITE_CONFLICT");
        assert_eq!(response["error"]["retryable"], true);
    }
}
