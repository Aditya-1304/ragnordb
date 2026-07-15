//! this file contains evaluation of fully bound scalar expressions.
//!
//! The analyzer has already resolved names and established expression types.
//! This module therefore evaluates only RagnorDB owned `BoundExpr` values and
//! never inspects parser AST nodes.
//!
//! SQL NULL behavior follows three-valued logic:
//!
//! - arithmetic involving NULL returns NULL,
//! - ordinary comparisons involving NULL return NULL,
//! - WHERE keeps only TRUE,
//! - FALSE AND NULL is FALSE,
//! - TRUE OR NULL is TRUE.

use std::cmp::Ordering;

use ragnordb_common::{
    Error, Result,
    codec::{Row, Value},
};
use ragnordb_sql::{BoundBinaryOperator, BoundExpr, BoundExprKind, BoundUnaryOperator};

/// Evaluate one bound expression against a complete stored row.
pub(crate) fn evaluate(expression: &BoundExpr, row: &Row) -> Result<Value> {
    match &expression.kind {
        BoundExprKind::Column(column) => row.values.get(column.ordinal).cloned().ok_or_else(|| {
            Error::CorruptData(format!(
                "row does not contain ordinal {} for column {}",
                column.ordinal, column.name
            ))
        }),

        BoundExprKind::Literal(value) => Ok(value.clone()),

        BoundExprKind::Unary {
            operator,
            expression,
        } => {
            let value = evaluate(expression, row)?;
            evaluate_unary(*operator, value)
        }

        BoundExprKind::Binary {
            left,
            operator,
            right,
        } => {
            let left = evaluate(left, row)?;
            let right = evaluate(right, row)?;

            evaluate_binary(*operator, left, right)
        }

        BoundExprKind::IsNull {
            expression,
            negated,
        } => {
            let is_null = matches!(evaluate(expression, row)?, Value::Null);

            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
    }
}

/// Evaluate a WHERE predicate.
///
/// SQL WHERE retains only rows for which the predicate is TRUE. FALSE and NULL
/// both reject the row.
pub(crate) fn evaluate_filter(expression: &BoundExpr, row: &Row) -> Result<bool> {
    match evaluate(expression, row)? {
        Value::Bool(value) => Ok(value),
        Value::Null => Ok(false),
        value => Err(Error::SchemaMismatch(format!(
            "WHERE expression returned {}, expected BOOL",
            value_type_name(&value)
        ))),
    }
}

fn evaluate_unary(operator: BoundUnaryOperator, value: Value) -> Result<Value> {
    if value == Value::Null {
        return Ok(Value::Null);
    }

    match (operator, value) {
        (BoundUnaryOperator::Positive, Value::Int(value)) => Ok(Value::Int(value)),

        (BoundUnaryOperator::Negative, Value::Int(value)) => {
            value.checked_neg().map(Value::Int).ok_or_else(|| {
                Error::InvalidArgument("integer overflow while evaluating unary minus".to_string())
            })
        }

        (BoundUnaryOperator::Not, Value::Bool(value)) => Ok(Value::Bool(!value)),

        (operator, value) => Err(Error::SchemaMismatch(format!(
            "bound unary operator {operator:?} cannot evaluate {}",
            value_type_name(&value)
        ))),
    }
}

fn evaluate_binary(operator: BoundBinaryOperator, left: Value, right: Value) -> Result<Value> {
    match operator {
        BoundBinaryOperator::Add
        | BoundBinaryOperator::Subtract
        | BoundBinaryOperator::Multiply
        | BoundBinaryOperator::Divide
        | BoundBinaryOperator::Modulo => evaluate_arithmetic(operator, left, right),

        BoundBinaryOperator::Equal
        | BoundBinaryOperator::NotEqual
        | BoundBinaryOperator::GreaterThan
        | BoundBinaryOperator::GreaterThanOrEqual
        | BoundBinaryOperator::LessThan
        | BoundBinaryOperator::LessThanOrEqual => evaluate_comparison(operator, left, right),

        BoundBinaryOperator::And => evaluate_and(left, right),
        BoundBinaryOperator::Or => evaluate_or(left, right),
    }
}

fn evaluate_arithmetic(operator: BoundBinaryOperator, left: Value, right: Value) -> Result<Value> {
    if left == Value::Null || right == Value::Null {
        return Ok(Value::Null);
    }

    let (Value::Int(left), Value::Int(right)) = (left, right) else {
        return Err(Error::SchemaMismatch(
            "bound arithmetic expression received non-INT values".to_string(),
        ));
    };

    let result = match operator {
        BoundBinaryOperator::Add => left.checked_add(right),

        BoundBinaryOperator::Subtract => left.checked_sub(right),

        BoundBinaryOperator::Multiply => left.checked_mul(right),

        BoundBinaryOperator::Divide => {
            if right == 0 {
                return Err(Error::InvalidArgument("division by zero".to_string()));
            }

            left.checked_div(right)
        }

        BoundBinaryOperator::Modulo => {
            if right == 0 {
                return Err(Error::InvalidArgument("modulo by zero".to_string()));
            }

            left.checked_rem(right)
        }

        _ => {
            return Err(Error::SchemaMismatch(
                "non-arithmetic operator reached arithmetic evaluator".to_string(),
            ));
        }
    };

    result.map(Value::Int).ok_or_else(|| {
        Error::InvalidArgument(format!("integer overflow while evaluating {operator:?}"))
    })
}

fn evaluate_comparison(operator: BoundBinaryOperator, left: Value, right: Value) -> Result<Value> {
    if left == Value::Null || right == Value::Null {
        return Ok(Value::Null);
    }

    match operator {
        BoundBinaryOperator::Equal | BoundBinaryOperator::NotEqual => {
            let equal = values_equal(&left, &right)?;

            Ok(Value::Bool(if operator == BoundBinaryOperator::Equal {
                equal
            } else {
                !equal
            }))
        }

        BoundBinaryOperator::GreaterThan
        | BoundBinaryOperator::GreaterThanOrEqual
        | BoundBinaryOperator::LessThan
        | BoundBinaryOperator::LessThanOrEqual => {
            let ordering = ordered_comparison(&left, &right)?;

            Ok(Value::Bool(match operator {
                BoundBinaryOperator::GreaterThan => ordering == Ordering::Greater,
                BoundBinaryOperator::GreaterThanOrEqual => ordering != Ordering::Less,
                BoundBinaryOperator::LessThan => ordering == Ordering::Less,
                BoundBinaryOperator::LessThanOrEqual => ordering != Ordering::Greater,
                _ => unreachable!("comparison operator was matched above"),
            }))
        }

        _ => Err(Error::SchemaMismatch(
            "non-comparison operator reached comparison evaluator".to_string(),
        )),
    }
}

fn values_equal(left: &Value, right: &Value) -> Result<bool> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Ok(left == right),

        (Value::Text(left), Value::Text(right)) => Ok(left == right),

        (Value::Bool(left), Value::Bool(right)) => Ok(left == right),

        _ => Err(Error::SchemaMismatch(format!(
            "cannot compare {} with {}",
            value_type_name(left),
            value_type_name(right)
        ))),
    }
}

fn ordered_comparison(left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Ok(left.cmp(right)),

        (Value::Text(left), Value::Text(right)) => Ok(left.cmp(right)),

        _ => Err(Error::SchemaMismatch(format!(
            "ordered comparison cannot compare {} with {}",
            value_type_name(left),
            value_type_name(right)
        ))),
    }
}

fn evaluate_and(left: Value, right: Value) -> Result<Value> {
    let left = nullable_boolean(left)?;
    let right = nullable_boolean(right)?;

    Ok(match (left, right) {
        (Some(false), _) | (_, Some(false)) => Value::Bool(false),

        (Some(true), Some(true)) => Value::Bool(true),

        _ => Value::Null,
    })
}

fn evaluate_or(left: Value, right: Value) -> Result<Value> {
    let left = nullable_boolean(left)?;
    let right = nullable_boolean(right)?;

    Ok(match (left, right) {
        (Some(true), _) | (_, Some(true)) => Value::Bool(true),

        (Some(false), Some(false)) => Value::Bool(false),

        _ => Value::Null,
    })
}

fn nullable_boolean(value: Value) -> Result<Option<bool>> {
    match value {
        Value::Bool(value) => Ok(Some(value)),
        Value::Null => Ok(None),
        value => Err(Error::SchemaMismatch(format!(
            "Boolean expression received {}",
            value_type_name(&value)
        ))),
    }
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Int(_) => "INT",
        Value::Text(_) => "TEXT",
        Value::Bool(_) => "BOOL",
        Value::Null => "NULL",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_sql::{BoundExpr, BoundExprKind, ExpressionType};

    fn literal(value: Value) -> BoundExpr {
        let data_type = match value {
            Value::Int(_) => ExpressionType::Int,
            Value::Text(_) => ExpressionType::Text,
            Value::Bool(_) => ExpressionType::Bool,
            Value::Null => ExpressionType::Null,
        };

        BoundExpr {
            nullable: value == Value::Null,
            data_type,
            kind: BoundExprKind::Literal(value),
        }
    }

    fn binary(
        left: Value,
        operator: BoundBinaryOperator,
        right: Value,
        data_type: ExpressionType,
    ) -> BoundExpr {
        BoundExpr {
            kind: BoundExprKind::Binary {
                left: Box::new(literal(left)),
                operator,
                right: Box::new(literal(right)),
            },
            data_type,
            nullable: true,
        }
    }

    #[test]
    fn boolean_operators_use_three_valued_logic() {
        let row = Row { values: vec![] };

        assert_eq!(
            evaluate(
                &binary(
                    Value::Bool(false),
                    BoundBinaryOperator::And,
                    Value::Null,
                    ExpressionType::Bool,
                ),
                &row,
            )
            .unwrap(),
            Value::Bool(false)
        );

        assert_eq!(
            evaluate(
                &binary(
                    Value::Bool(true),
                    BoundBinaryOperator::And,
                    Value::Null,
                    ExpressionType::Bool,
                ),
                &row,
            )
            .unwrap(),
            Value::Null
        );

        assert_eq!(
            evaluate(
                &binary(
                    Value::Bool(true),
                    BoundBinaryOperator::Or,
                    Value::Null,
                    ExpressionType::Bool,
                ),
                &row,
            )
            .unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn null_comparison_is_unknown_and_where_rejects_it() {
        let expression = binary(
            Value::Int(1),
            BoundBinaryOperator::Equal,
            Value::Null,
            ExpressionType::Bool,
        );
        let row = Row { values: vec![] };

        assert_eq!(evaluate(&expression, &row).unwrap(), Value::Null);
        assert!(!evaluate_filter(&expression, &row).unwrap());
    }

    #[test]
    fn arithmetic_checks_division_by_zero_and_overflow() {
        let row = Row { values: vec![] };

        let divide_by_zero = binary(
            Value::Int(10),
            BoundBinaryOperator::Divide,
            Value::Int(0),
            ExpressionType::Int,
        );

        assert!(matches!(
            evaluate(&divide_by_zero, &row).unwrap_err(),
            Error::InvalidArgument(_)
        ));

        let overflow = binary(
            Value::Int(i64::MAX),
            BoundBinaryOperator::Add,
            Value::Int(1),
            ExpressionType::Int,
        );

        assert!(matches!(
            evaluate(&overflow, &row).unwrap_err(),
            Error::InvalidArgument(_)
        ));
    }
}
