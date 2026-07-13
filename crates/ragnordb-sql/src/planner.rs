//! this does the logical planning from fully bound SQL statements
//!
//! This module intentionally has no `sqlparser` dependency
//! things like name resolution, type checking, wildcard expression and expression
//! binding have already been handled by the analyzer

use ragnordb_catalog::ColumnSchema;
use ragnordb_common::codec::Value;
use ragnordb_common::ids::ColumnId;

use crate::bound::{
    BoundAssignment, BoundColumnRef, BoundCreateTable, BoundDelete, BoundExpr, BoundInsert,
    BoundSelect, BoundStatement, BoundTableRef, BoundUpdate,
};

/// Logical plan for one SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    CreateTable(CreateTablePlan),
    Insert(InsertPlan),
    Select(SelectPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    Begin,
    Commit,
    Rollback,
    ShowTables,
}

/// validate new table definition
///
/// No tableID exists before this, the catalog assigns it whrn this plan executes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePlan {
    pub table_name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key_column_ids: Vec<ColumnId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertPlan {
    pub table: BoundTableRef,
    pub target_columns: Vec<BoundColumnRef>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectPlan {
    pub table: BoundTableRef,
    pub projection: Vec<BoundColumnRef>,
    pub filter: Option<BoundExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateAssignmentPlan {
    pub column: BoundColumnRef,
    pub value: BoundExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdatePlan {
    pub table: BoundTableRef,
    pub assignments: Vec<UpdateAssignmentPlan>,
    pub filter: BoundExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeletePlan {
    pub table: BoundTableRef,
    pub filter: BoundExpr,
}

/// Perform structural lowering from a fully bound statement.
///
/// This conversion is infallible: all semantic validation and parser-AST
/// conversion have already happened in the binder.
pub fn plan(statement: BoundStatement) -> Plan {
    match statement {
        BoundStatement::CreateTable(create) => Plan::CreateTable(plan_create_table(create)),

        BoundStatement::Insert(insert) => Plan::Insert(plan_insert(insert)),

        BoundStatement::Select(select) => Plan::Select(plan_select(select)),

        BoundStatement::Update(update) => Plan::Update(plan_update(update)),

        BoundStatement::Delete(delete) => Plan::Delete(plan_delete(delete)),

        BoundStatement::Begin => Plan::Begin,
        BoundStatement::Commit => Plan::Commit,
        BoundStatement::Rollback => Plan::Rollback,
        BoundStatement::ShowTables => Plan::ShowTables,
    }
}

fn plan_create_table(create: BoundCreateTable) -> CreateTablePlan {
    CreateTablePlan {
        table_name: create.table_name,
        columns: create.columns,
        primary_key_column_ids: create.primary_key_column_ids,
    }
}

fn plan_insert(insert: BoundInsert) -> InsertPlan {
    InsertPlan {
        table: insert.table,
        target_columns: insert.target_columns,
        rows: insert.rows,
    }
}

fn plan_select(select: BoundSelect) -> SelectPlan {
    SelectPlan {
        table: select.table,
        projection: select.projection,
        filter: select.filter,
    }
}

fn plan_update(update: BoundUpdate) -> UpdatePlan {
    UpdatePlan {
        table: update.table,
        assignments: update
            .assignments
            .into_iter()
            .map(plan_assignment)
            .collect(),
        filter: update.filter,
    }
}

fn plan_assignment(assignment: BoundAssignment) -> UpdateAssignmentPlan {
    UpdateAssignmentPlan {
        column: assignment.column,
        value: assignment.value,
    }
}

fn plan_delete(delete: BoundDelete) -> DeletePlan {
    DeletePlan {
        table: delete.table,
        filter: delete.filter,
    }
}

#[cfg(test)]
mod tests {
    use ragnordb_catalog::{ColumnSchema, MemoryCatalog};
    use ragnordb_common::catalog_codec::DataType;
    use ragnordb_common::ids::{ColumnId, TableId};

    use super::*;
    use crate::bound::{BoundBinaryOperator, BoundExprKind, BoundStatement, ExpressionType};
    use crate::{analyze, parse_one};

    fn catalog() -> MemoryCatalog {
        let mut catalog = MemoryCatalog::new();

        catalog
            .add_table(
                "users",
                vec![
                    ColumnSchema {
                        id: ColumnId(1),
                        name: "id".into(),
                        ty: DataType::Int,
                        nullable: false,
                    },
                    ColumnSchema {
                        id: ColumnId(2),
                        name: "name".into(),
                        ty: DataType::Text,
                        nullable: true,
                    },
                    ColumnSchema {
                        id: ColumnId(3),
                        name: "active".into(),
                        ty: DataType::Bool,
                        nullable: true,
                    },
                ],
                vec![ColumnId(1)],
            )
            .unwrap();

        catalog
    }

    fn build(sql: &str) -> Plan {
        let catalog = catalog();
        let parsed = parse_one(sql).unwrap();
        let bound = analyze(&parsed, &catalog).unwrap();

        plan(bound)
    }

    #[test]
    fn planner_preserves_resolved_select_metadata() {
        let plan = build("SELECT name FROM users WHERE id = 1");

        let Plan::Select(select) = plan else {
            panic!("expected SELECT plan");
        };

        assert_eq!(select.table.table_id, TableId(1));
        assert_eq!(select.table.schema_version, 1);

        assert_eq!(select.projection[0].column_id, ColumnId(2));
        assert_eq!(select.projection[0].ordinal, 1);
        assert_eq!(select.projection[0].data_type, DataType::Text);
        assert!(select.projection[0].nullable);

        let Some(filter) = select.filter else {
            panic!("expected filter");
        };

        assert_eq!(filter.data_type, ExpressionType::Bool);

        assert!(matches!(
            filter.kind,
            BoundExprKind::Binary {
                operator: BoundBinaryOperator::Equal,
                ..
            }
        ));
    }

    #[test]
    fn planner_performs_infallible_structural_lowering() {
        let catalog = catalog();
        let parsed = parse_one(
            "UPDATE users
             SET name = 'Ada'
             WHERE id = 1",
        )
        .unwrap();

        let bound: BoundStatement = analyze(&parsed, &catalog).unwrap();

        // `plan` returns Plan directly, not Result. All expression conversion
        // and semantic validation were completed by the binder.
        let plan: Plan = plan(bound);

        let Plan::Update(update) = plan else {
            panic!("expected UPDATE plan");
        };

        assert_eq!(update.table.table_id, TableId(1));
        assert_eq!(update.assignments[0].column.column_id, ColumnId(2));
        assert_eq!(update.assignments[0].column.ordinal, 1);
    }

    #[test]
    fn wildcard_is_absent_from_plan_representation() {
        let plan = build("SELECT * FROM users");

        let Plan::Select(select) = plan else {
            panic!("expected SELECT plan");
        };

        assert_eq!(select.projection.len(), 3);
        assert_eq!(
            select
                .projection
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2), ColumnId(3),]
        );
    }

    #[test]
    fn transaction_and_show_plans_are_structural() {
        assert_eq!(build("BEGIN"), Plan::Begin);
        assert_eq!(build("COMMIT"), Plan::Commit);
        assert_eq!(build("ROLLBACK"), Plan::Rollback);
        assert_eq!(build("SHOW TABLES"), Plan::ShowTables);
    }
}
