//! Measure fixed-size distributed MVCC batches against increasing amounts of
//! unrelated retained tablet data.
//!
//! Criterion setup clones the baseline before each timed operation. The clone
//! is fixture setup and is not included in the reported operation time. This
//! benchmark measures scaling of the current in-memory batch methods; it does
//! not claim an end-to-end speedup or select the future LSM key layout.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ragnordb_common::{
    codec::{Row, Value},
    encoding::encode_row,
    ids::{TableId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    mvcc::{InMemoryMvcc, Mutation, MvccStorage},
};
use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;

const TABLE_ID: TableId = TableId(9);
const BATCH_SIZE: usize = 8;
const RETAINED_ROW_COUNTS: [usize; 3] = [1_000, 10_000, 100_000];
const BATCH_TXN_ID: TxnId = TxnId(2);
const BATCH_START_TS: Timestamp = Timestamp(1_000_000);
const BATCH_COMMIT_TS: Timestamp = Timestamp(1_000_001);
const LOCK_TTL_MS: u64 = 30_000;

struct BatchWorkload {
    mutations: BTreeMap<Vec<u8>, Mutation>,
    keys: BTreeSet<Vec<u8>>,
    primary_key: Vec<u8>,
}

fn encoded_key(id: i64) -> Vec<u8> {
    let row_key = make_row_key(TABLE_ID, &[Value::Int(id)]).expect("benchmark row key must encode");
    encode_row_key(&row_key).expect("benchmark row key must encode")
}

fn encoded_row(id: i64) -> Vec<u8> {
    encode_row(&Row {
        values: vec![Value::Int(id), Value::Text("x".repeat(64))],
    })
    .expect("benchmark row must encode")
}

fn batch_workload(first_id: i64) -> BatchWorkload {
    let mutations: BTreeMap<Vec<u8>, Mutation> = (0..BATCH_SIZE)
        .map(|offset| {
            let id = first_id + offset as i64;
            (encoded_key(id), Mutation::Put(encoded_row(id)))
        })
        .collect();

    let keys = mutations.keys().cloned().collect();
    let primary_key = mutations
        .keys()
        .next()
        .expect("benchmark batch is non-empty")
        .clone();

    BatchWorkload {
        mutations,
        keys,
        primary_key,
    }
}

fn populated_storage(retained_rows: usize) -> InMemoryMvcc {
    let mut storage = InMemoryMvcc::new();
    if retained_rows == 0 {
        return storage;
    }

    let mutations: BTreeMap<Vec<u8>, Mutation> = (1..=retained_rows)
        .map(|id| {
            let id = id as i64;
            (encoded_key(id), Mutation::Put(encoded_row(id)))
        })
        .collect();

    storage
        .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
        .expect("benchmark baseline must load");

    storage
}

fn prewritten_storage(retained_rows: usize, workload: &BatchWorkload) -> InMemoryMvcc {
    let mut storage = populated_storage(retained_rows);
    storage
        .prewrite_batch(
            BATCH_TXN_ID,
            BATCH_START_TS,
            &workload.mutations,
            &workload.primary_key,
            LOCK_TTL_MS,
        )
        .expect("benchmark intents must prewrite");

    storage
}

fn bench_prewrite_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("bug1/prewrite_batch_scaling");
    group.sample_size(10);
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    for retained_rows in RETAINED_ROW_COUNTS {
        let baseline = populated_storage(retained_rows);
        let workload = batch_workload(retained_rows as i64 + 1);

        group.bench_with_input(
            BenchmarkId::new("retained_rows", retained_rows),
            &retained_rows,
            |bencher, _| {
                bencher.iter_batched(
                    || baseline.clone(),
                    |mut storage| {
                        storage
                            .prewrite_batch(
                                BATCH_TXN_ID,
                                BATCH_START_TS,
                                &workload.mutations,
                                &workload.primary_key,
                                LOCK_TTL_MS,
                            )
                            .expect("benchmark prewrite must succeed");
                        black_box(storage)
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_commit_intents_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("bug1/commit_intents_batch_scaling");
    group.sample_size(10);
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    for retained_rows in RETAINED_ROW_COUNTS {
        let workload = batch_workload(retained_rows as i64 + 1);
        let baseline = prewritten_storage(retained_rows, &workload);

        group.bench_with_input(
            BenchmarkId::new("retained_rows", retained_rows),
            &retained_rows,
            |bencher, _| {
                bencher.iter_batched(
                    || baseline.clone(),
                    |mut storage| {
                        storage
                            .commit_intents_batch(
                                BATCH_TXN_ID,
                                BATCH_START_TS,
                                BATCH_COMMIT_TS,
                                &workload.keys,
                            )
                            .expect("benchmark commit must succeed");
                        black_box(storage)
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_rollback_intents_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("bug1/rollback_intents_batch_scaling");
    group.sample_size(10);
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    for retained_rows in RETAINED_ROW_COUNTS {
        let workload = batch_workload(retained_rows as i64 + 1);
        let baseline = prewritten_storage(retained_rows, &workload);

        group.bench_with_input(
            BenchmarkId::new("retained_rows", retained_rows),
            &retained_rows,
            |bencher, _| {
                bencher.iter_batched(
                    || baseline.clone(),
                    |mut storage| {
                        storage
                            .rollback_intents_batch(BATCH_TXN_ID, BATCH_START_TS, &workload.keys)
                            .expect("benchmark rollback must succeed");
                        black_box(storage)
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_prewrite_batch,
    bench_commit_intents_batch,
    bench_rollback_intents_batch,
);
criterion_main!(benches);
