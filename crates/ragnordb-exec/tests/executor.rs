use std::sync::Arc;
use std::time::Duration;

use ragnordb_catalog::Catalog;
use ragnordb_common::{
    Error, Result,
    catalog_codec::TableDefinition,
    codec::{Row, Value},
    ids::{RaftGroupId, RequestId, TableId, TabletId, Timestamp, TxnId},
    metadata_codec::{CreateTableRequest, PartitionSpec, TabletDescriptor},
};
use ragnordb_exec::{
    DmlOperation, ExecutionResult, LocalExecutor, MetadataTableCreator, ResultSet,
};
use ragnordb_sql::{BoundExprKind, Plan, analyze, parse_one, plan};
use ragnordb_storage::key::encode_primary_key;
use ragnordb_storage::mvcc::InMemoryMvcc;
use ragnordb_tablet::HashTabletPartitioner;
use ragnordb_txn::Transaction;

fn build(executor: &LocalExecutor, sql: &str) -> Result<Plan> {
    let parsed = parse_one(sql)?;
    let bound = analyze(&parsed, executor.catalog())?;

    Ok(plan(bound))
}

fn transaction(id: u64, start_ts: u64) -> Transaction {
    Transaction::new(TxnId(id), Timestamp(start_ts)).unwrap()
}

fn create_users(executor: &mut LocalExecutor) {
    let plan = build(
        executor,
        "CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            score INT NOT NULL,
            active BOOL,
            note TEXT
        )",
    )
    .unwrap();

    executor.execute(plan, None).unwrap();
}

fn result_set(result: ExecutionResult) -> ResultSet {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query result");
    };

    result
}

struct TestMetadataTableCreator {
    with_topology: bool,
    tablet_count: u32,
}

impl MetadataTableCreator for TestMetadataTableCreator {
    fn create_table(
        &self,
        request: CreateTableRequest,
        _request_id: RequestId,
        _timeout: Duration,
    ) -> Result<TableDefinition> {
        assert_eq!(request.table_name, "authoritative");

        Ok(TableDefinition {
            table_id: 42,
            name: request.table_name,
            columns: request.columns,
            primary_key_column_ids: request.primary_key_column_ids,
            schema_version: 1,
            tablet_count: self.tablet_count,
        })
    }

    fn list_tables(&self) -> Vec<TableDefinition> {
        Vec::new()
    }

    fn table_descriptors(&self, table_id: TableId) -> Result<Vec<TabletDescriptor>> {
        if !self.with_topology {
            return Err(Error::NotImplemented(
                "metadata tablet topology lookup is unavailable",
            ));
        }

        Ok((0..self.tablet_count)
            .map(|bucket| TabletDescriptor {
                tablet_id: TabletId(table_id.0 + u64::from(bucket)),
                table_id,
                raft_group_id: RaftGroupId(3 + u64::from(bucket)),
                tablet_epoch: 1,
                partition: PartitionSpec::Hash {
                    bucket,
                    bucket_count: self.tablet_count,
                },
            })
            .collect())
    }
}

fn key_for_bucket(table_id: TableId, bucket: u32, bucket_count: u32) -> i64 {
    let partitioner = HashTabletPartitioner::new();

    (1..10_000)
        .find(|candidate| {
            let primary_key = encode_primary_key(&[Value::Int(*candidate)]).unwrap();
            partitioner
                .bucket_for(table_id, &primary_key, bucket_count)
                .unwrap()
                == bucket
        })
        .expect("test key search must find every bucket")
}

#[test]
fn metadata_create_requires_authoritative_tablet_topology() {
    let mut executor = LocalExecutor::new();
    executor.replace_metadata_table_creator(Arc::new(TestMetadataTableCreator {
        with_topology: false,
        tablet_count: 1,
    }));

    let Plan::CreateTable(plan) =
        build(&executor, "CREATE TABLE authoritative (id INT PRIMARY KEY)").unwrap()
    else {
        panic!("expected CREATE TABLE plan");
    };

    let error = executor
        .execute_create_table_with_metadata(
            plan,
            RequestId {
                client_id: 7,
                sequence: 1,
                raft_group_id: RaftGroupId(2),
            },
            Duration::from_secs(1),
        )
        .unwrap_err();

    assert!(matches!(error, Error::NotImplemented(_)));
}

#[test]
fn metadata_create_installs_the_identity_returned_by_metadata() {
    let mut executor = LocalExecutor::new();
    executor.replace_metadata_table_creator(Arc::new(TestMetadataTableCreator {
        with_topology: true,
        tablet_count: 1,
    }));

    let Plan::CreateTable(plan) =
        build(&executor, "CREATE TABLE authoritative (id INT PRIMARY KEY)").unwrap()
    else {
        panic!("expected CREATE TABLE plan");
    };

    let result = executor
        .execute_create_table_with_metadata(
            plan,
            RequestId {
                client_id: 7,
                sequence: 1,
                raft_group_id: RaftGroupId(2),
            },
            Duration::from_secs(1),
        )
        .unwrap();

    assert_eq!(
        result,
        ExecutionResult::CreatedTable {
            table_id: TableId(42)
        }
    );
    assert!(executor.catalog().table_by_id(TableId(42)).is_some());
    assert!(executor.catalog().table_by_id(TableId(1)).is_none());
}

#[test]
fn metadata_routing_drives_point_lookup_and_scan_fanout() {
    let mut executor = LocalExecutor::new();
    executor.replace_metadata_table_creator(Arc::new(TestMetadataTableCreator {
        with_topology: true,
        tablet_count: 2,
    }));

    let Plan::CreateTable(plan) =
        build(&executor, "CREATE TABLE authoritative (id INT PRIMARY KEY)").unwrap()
    else {
        panic!("expected CREATE TABLE plan");
    };

    executor
        .execute_create_table_with_metadata(
            plan,
            RequestId {
                client_id: 7,
                sequence: 1,
                raft_group_id: RaftGroupId(2),
            },
            Duration::from_secs(1),
        )
        .unwrap();

    let table_id = TableId(42);
    let local_key = key_for_bucket(table_id, 0, 2);
    let remote_key = key_for_bucket(table_id, 1, 2);

    // The compatibility executor has one physically installed tablet. The
    // metadata router still decides whether a key belongs to that tablet;
    // another bucket must fail closed instead of reading the local mirror.
    assert!(
        executor
            .install_replicated_storage(table_id, InMemoryMvcc::new())
            .unwrap()
    );

    let mut writer = transaction(1, 1);
    let insert = build(
        &executor,
        &format!("INSERT INTO authoritative (id) VALUES ({local_key})"),
    )
    .unwrap();
    executor.execute(insert, Some(&mut writer)).unwrap();
    executor.commit_transaction(writer, Timestamp(2)).unwrap();

    let mut reader = transaction(2, 3);
    let point = build(
        &executor,
        &format!("SELECT id FROM authoritative WHERE id = {local_key}"),
    )
    .unwrap();
    assert_eq!(
        result_set(executor.execute(point, Some(&mut reader)).unwrap())
            .rows
            .len(),
        1
    );

    let remote_point = build(
        &executor,
        &format!("SELECT id FROM authoritative WHERE id = {remote_key}"),
    )
    .unwrap();
    assert!(matches!(
        executor.execute(remote_point, Some(&mut reader)),
        Err(Error::UnsupportedSql(_))
    ));

    let scan = build(&executor, "SELECT id FROM authoritative").unwrap();
    assert!(matches!(
        executor.execute(scan, Some(&mut reader)),
        Err(Error::UnsupportedSql(_))
    ));

    // A statement spanning buckets is rejected before it contributes any
    // buffered writes, preserving the current single-coordinator durability
    // boundary until shard-aware transaction records exist.
    let mut cross_tablet = transaction(3, 4);
    let multi_insert = build(
        &executor,
        &format!("INSERT INTO authoritative (id) VALUES ({local_key}), ({remote_key})"),
    )
    .unwrap();
    assert!(matches!(
        executor.execute(multi_insert, Some(&mut cross_tablet)),
        Err(Error::UnsupportedSql(_))
    ));
    assert_eq!(cross_tablet.len(), 0);
}

#[test]
fn create_table_and_show_tables_execute_end_to_end() {
    let mut executor = LocalExecutor::new();

    let users = build(
        &executor,
        "CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT NOT NULL
        )",
    )
    .unwrap();

    let created = executor.execute(users, None).unwrap();

    assert!(matches!(
        created,
        ExecutionResult::CreatedTable { table_id }
            if table_id.0 == 1
    ));

    let orders = build(
        &executor,
        "CREATE TABLE orders (
            id INT PRIMARY KEY,
            description TEXT
        )",
    )
    .unwrap();

    executor.execute(orders, None).unwrap();

    let show = build(&executor, "SHOW TABLES").unwrap();
    let result = result_set(executor.execute(show, None).unwrap());

    assert_eq!(result.columns[0].name, "table_name");
    assert_eq!(
        result.rows,
        vec![
            Row {
                values: vec![Value::Text("users".to_string())],
            },
            Row {
                values: vec![Value::Text("orders".to_string())],
            },
        ]
    );
}

#[test]
fn insert_and_select_support_read_your_writes() {
    let mut executor = LocalExecutor::new();
    create_users(&mut executor);

    let insert = build(
        &executor,
        "INSERT INTO users (id, name, score)
         VALUES
            (1, 'Ada', 10),
            (2, 'Grace', 20)",
    )
    .unwrap();

    let mut writer = transaction(1, 1);

    let result = executor.execute(insert, Some(&mut writer)).unwrap();

    assert_eq!(
        result,
        ExecutionResult::Mutation {
            operation: DmlOperation::Insert,
            affected_rows: 2,
        }
    );

    let select = build(&executor, "SELECT name, score FROM users WHERE id = 2").unwrap();

    let result = result_set(executor.execute(select, Some(&mut writer)).unwrap());

    assert_eq!(
        result.rows,
        vec![Row {
            values: vec![Value::Text("Grace".to_string()), Value::Int(20),],
        }]
    );

    executor.commit_transaction(writer, Timestamp(2)).unwrap();

    let mut reader = transaction(2, 3);
    let select = build(&executor, "SELECT id, name FROM users").unwrap();

    let result = result_set(executor.execute(select, Some(&mut reader)).unwrap());

    assert_eq!(result.rows.len(), 2);
}

#[test]
fn update_evaluates_expressions_against_original_row() {
    let mut executor = LocalExecutor::new();
    create_users(&mut executor);

    let mut seed = transaction(1, 1);
    let insert = build(
        &executor,
        "INSERT INTO users (id, name, score)
         VALUES (1, 'Ada', 10)",
    )
    .unwrap();

    executor.execute(insert, Some(&mut seed)).unwrap();
    executor.commit_transaction(seed, Timestamp(2)).unwrap();

    let mut updater = transaction(2, 3);
    let update = build(
        &executor,
        "UPDATE users
         SET score = score + 5,
             name = 'Ada Lovelace'
         WHERE id = 1",
    )
    .unwrap();

    let result = executor.execute(update, Some(&mut updater)).unwrap();

    assert_eq!(
        result,
        ExecutionResult::Mutation {
            operation: DmlOperation::Update,
            affected_rows: 1,
        }
    );

    let select = build(&executor, "SELECT name, score FROM users WHERE id = 1").unwrap();

    let result = result_set(executor.execute(select, Some(&mut updater)).unwrap());

    assert_eq!(
        result.rows,
        vec![Row {
            values: vec![Value::Text("Ada Lovelace".to_string()), Value::Int(15),],
        }]
    );
}

#[test]
fn delete_buffers_tombstone_and_reports_affected_rows() {
    let mut executor = LocalExecutor::new();
    create_users(&mut executor);

    let mut seed = transaction(1, 1);
    let insert = build(
        &executor,
        "INSERT INTO users (id, name, score)
         VALUES (1, 'Ada', 10)",
    )
    .unwrap();

    executor.execute(insert, Some(&mut seed)).unwrap();
    executor.commit_transaction(seed, Timestamp(2)).unwrap();

    let mut deleter = transaction(2, 3);
    let delete = build(&executor, "DELETE FROM users WHERE id = 1").unwrap();

    let result = executor.execute(delete, Some(&mut deleter)).unwrap();

    assert_eq!(
        result,
        ExecutionResult::Mutation {
            operation: DmlOperation::Delete,
            affected_rows: 1,
        }
    );

    let select = build(&executor, "SELECT id FROM users WHERE id = 1").unwrap();

    let result = result_set(executor.execute(select, Some(&mut deleter)).unwrap());

    assert!(result.rows.is_empty());
}

#[test]
fn scans_apply_filters_projection_and_sql_null_semantics() {
    let mut executor = LocalExecutor::new();
    create_users(&mut executor);

    let mut seed = transaction(1, 1);
    let insert = build(
        &executor,
        "INSERT INTO users
            (id, name, score, active)
         VALUES
            (1, 'Ada', 10, true),
            (2, 'Grace', 20, false),
            (3, 'Linus', 30, NULL)",
    )
    .unwrap();

    executor.execute(insert, Some(&mut seed)).unwrap();
    executor.commit_transaction(seed, Timestamp(2)).unwrap();

    let mut reader = transaction(2, 3);

    let select = build(
        &executor,
        "SELECT id, name FROM users
         WHERE score > 10 AND active = false",
    )
    .unwrap();

    let result = result_set(executor.execute(select, Some(&mut reader)).unwrap());

    assert_eq!(
        result.rows,
        vec![Row {
            values: vec![Value::Int(2), Value::Text("Grace".to_string()),],
        }]
    );

    let null_select = build(&executor, "SELECT id FROM users WHERE active IS NULL").unwrap();

    let result = result_set(executor.execute(null_select, Some(&mut reader)).unwrap());

    assert_eq!(
        result.rows,
        vec![Row {
            values: vec![Value::Int(3)],
        }]
    );
}

#[test]
fn composite_primary_key_predicate_executes_correctly() {
    let mut executor = LocalExecutor::new();

    let create = build(
        &executor,
        "CREATE TABLE memberships (
            user_id INT,
            group_id INT,
            role TEXT NOT NULL,
            PRIMARY KEY (user_id, group_id)
        )",
    )
    .unwrap();

    executor.execute(create, None).unwrap();

    let mut writer = transaction(1, 1);
    let insert = build(
        &executor,
        "INSERT INTO memberships
            (user_id, group_id, role)
         VALUES
            (1, 10, 'admin'),
            (1, 20, 'member')",
    )
    .unwrap();

    executor.execute(insert, Some(&mut writer)).unwrap();
    executor.commit_transaction(writer, Timestamp(2)).unwrap();

    let mut reader = transaction(2, 3);
    let select = build(
        &executor,
        "SELECT role FROM memberships
         WHERE group_id = 20 AND user_id = 1",
    )
    .unwrap();

    let result = result_set(executor.execute(select, Some(&mut reader)).unwrap());

    assert_eq!(
        result.rows,
        vec![Row {
            values: vec![Value::Text("member".to_string())],
        }]
    );
}

#[test]
fn duplicate_multi_row_insert_is_statement_atomic() {
    let mut executor = LocalExecutor::new();
    create_users(&mut executor);

    let mut transaction = transaction(1, 1);

    let earlier_insert = build(
        &executor,
        "INSERT INTO users (id, name, score)
         VALUES (9, 'Earlier', 90)",
    )
    .unwrap();

    executor
        .execute(earlier_insert, Some(&mut transaction))
        .unwrap();

    let insert = build(
        &executor,
        "INSERT INTO users (id, name, score)
         VALUES
            (1, 'Ada', 10),
            (1, 'Duplicate', 20)",
    )
    .unwrap();

    let error = executor
        .execute(insert, Some(&mut transaction))
        .unwrap_err();

    assert!(matches!(error, Error::ConstraintViolation(_)));

    // The failing statement contributes no mutations while the earlier
    // successful statement remains part of the transaction.
    assert_eq!(transaction.len(), 1);

    let select = build(&executor, "SELECT id FROM users").unwrap();
    let result = result_set(executor.execute(select, Some(&mut transaction)).unwrap());

    assert_eq!(
        result.rows,
        vec![Row {
            values: vec![Value::Int(9)],
        }]
    );
}

#[test]
fn concurrent_executor_commits_produce_one_winner() {
    let mut executor = LocalExecutor::new();
    create_users(&mut executor);

    let insert_sql = "INSERT INTO users (id, name, score)
         VALUES (1, 'Ada', 10)";

    let mut first = transaction(1, 1);
    let first_plan = build(&executor, insert_sql).unwrap();

    executor.execute(first_plan, Some(&mut first)).unwrap();

    let mut second = transaction(2, 2);
    let second_plan = build(&executor, insert_sql).unwrap();

    executor.execute(second_plan, Some(&mut second)).unwrap();

    executor.commit_transaction(second, Timestamp(3)).unwrap();

    let error = executor
        .commit_transaction(first, Timestamp(4))
        .unwrap_err();

    assert!(matches!(error, Error::WriteConflict(_)));
}

#[test]
fn cross_table_transaction_is_rejected_before_commit() {
    let mut executor = LocalExecutor::new();
    create_users(&mut executor);

    let create_orders = build(
        &executor,
        "CREATE TABLE orders (
            id INT PRIMARY KEY,
            description TEXT NOT NULL
        )",
    )
    .unwrap();

    executor.execute(create_orders, None).unwrap();

    let mut writer = transaction(1, 1);

    let users_insert = build(
        &executor,
        "INSERT INTO users (id, name, score)
         VALUES (1, 'Ada', 10)",
    )
    .unwrap();

    executor.execute(users_insert, Some(&mut writer)).unwrap();

    let orders_insert = build(
        &executor,
        "INSERT INTO orders (id, description)
         VALUES (1, 'Book')",
    )
    .unwrap();

    executor.execute(orders_insert, Some(&mut writer)).unwrap();

    let error = executor
        .commit_transaction(writer, Timestamp(2))
        .unwrap_err();

    assert!(matches!(error, Error::UnsupportedSql(_)));

    // The failed commit never invoked either tablet's MVCC commit path.
    let mut reader = transaction(2, 3);

    let users = build(&executor, "SELECT id FROM users").unwrap();

    let result = result_set(executor.execute(users, Some(&mut reader)).unwrap());

    assert!(result.rows.is_empty());
}

#[test]
fn transaction_control_and_unsupported_sql_keep_clear_boundaries() {
    let mut executor = LocalExecutor::new();
    create_users(&mut executor);

    let begin = build(&executor, "BEGIN").unwrap();

    let error = executor.execute(begin, None).unwrap_err();

    assert!(matches!(error, Error::NotImplemented(_)));

    let parsed = parse_one("UPDATE users SET name = 'unsafe'").unwrap();

    let error = analyze(&parsed, executor.catalog()).unwrap_err();

    assert!(matches!(error, Error::UnsupportedSql(_)));
}

#[test]
fn data_statements_require_transaction_context() {
    let mut executor = LocalExecutor::new();
    create_users(&mut executor);

    let select = build(&executor, "SELECT id FROM users").unwrap();

    let error = executor.execute(select, None).unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(_)));
}

#[test]
fn read_only_commit_is_a_no_op_without_a_commit_timestamp() {
    let mut executor = LocalExecutor::new();
    let reader = transaction(1, 7);

    // Timestamp zero represents the absence of an allocated commit timestamp.
    assert_eq!(
        executor.commit_transaction(reader, Timestamp(0)).unwrap(),
        0
    );
}

#[test]
fn forged_point_lookup_with_wrong_primary_key_type_is_rejected() {
    let mut executor = LocalExecutor::new();
    create_users(&mut executor);

    let plan = build(&executor, "SELECT name FROM users WHERE id = 1").unwrap();
    let Plan::Select(mut select) = plan else {
        panic!("expected SELECT plan");
    };

    let Some(filter) = select.filter.as_mut() else {
        panic!("expected SELECT filter");
    };

    let BoundExprKind::Binary { right, .. } = &mut filter.kind else {
        panic!("expected equality predicate");
    };

    // Simulate a stale or manually constructed plan that bypassed analyzer
    // type checking while retaining otherwise valid bound metadata.
    right.kind = BoundExprKind::Literal(Value::Text("wrong-type".to_string()));

    let mut reader = transaction(1, 1);
    let error = executor
        .execute(Plan::Select(select), Some(&mut reader))
        .unwrap_err();

    assert!(matches!(error, Error::SchemaMismatch(_)));
}
