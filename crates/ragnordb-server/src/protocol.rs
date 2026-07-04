use serde_json::Value;

pub fn ok_response(columns: Vec<&str>, rows: Vec<Vec<Value>>) -> Value {
    serde_json::json!({
        "ok": true,
        "columns": columns,
        "rows": rows,
        "stats": {
            "rows_read": 0,
            "rows_written": 0
        }
    })
}

pub fn error_response(code: &str, message: &str, retryable: bool) -> Value {
    serde_json::json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message,
            "retryable": retryable
        }
    })
}
