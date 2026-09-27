//! Measure mutation-free commit preflight across increasingly long histories
//! for one key. The benchmark constructs valid retained MVCC state directly
//! through the storage API and times only the preflight operation.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ragnordb_common::{
    codec::{Row, Value},
    encoding::encode_row,
    ids::{TableId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    mvcc::{InMemoryMvcc, Mutation, MvccStorage},
};
use std::{collections::BTreeMap, hint::black_box, time::Duration};

const TABLE_ID: TableId = TableId(35);
const HISTORY_SIZES: [usize; 5] = [1, 10, 100, 1_000, 10_000];

fn encoded_key(id: i64) -> Vec<u8> {
    let row_key = make_row_key(TABLE_ID, &[Value::Int(id)]).expect("benchmark key must encode");
    encode_row_key(&row_key).expect("benchmark key must encode")
}

fn encoded_row(id: i64) -> Vec<u8> {
    encode_row(&Row {
        values: vec![Value::Int(id), Value::Text("history benchmark".to_string())],
    })
    .expect("benchmark row must encode")
}

fn storage_with_history(retained_writes: usize, key: &[u8]) -> InMemoryMvcc {
    let mut storage = InMemoryMvcc::new();

    for index in 0..retained_writes {
        let start_ts = Timestamp(index as u64 * 2 + 1);
        let commit_ts = Timestamp(start_ts.0 + 1);
        let mutation =
            BTreeMap::from([(key.to_vec(), Mutation::Put(encoded_row(index as i64 + 1)))]);

        storage
            .commit_batch(TxnId(index as u64 + 1), start_ts, commit_ts, &mutation)
            .expect("benchmark history entry must commit");
    }

    storage
}

fn bench_write_history_preflight(c: &mut Criterion) {
    let key = encoded_key(1);
    let mutation = BTreeMap::from([(key.clone(), Mutation::Put(encoded_row(0)))]);
    let mut group = c.benchmark_group("mvcc/write_history_preflight");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    for retained_writes in HISTORY_SIZES {
        // Keep fixture construction outside the timed loop so the reported
        // duration isolates the prewrite/commit-history validation path.
        let storage = storage_with_history(retained_writes, &key);
        let start_ts = Timestamp(retained_writes as u64 * 2 + 1);
        let txn_id = TxnId(retained_writes as u64 + 1);

        group.bench_function(
            BenchmarkId::new("retained_writes_per_key", retained_writes),
            |bencher| {
                bencher.iter(|| {
                    black_box(storage.validate_commit_batch(txn_id, start_ts, &mutation))
                        .expect("fresh-snapshot preflight must succeed")
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_write_history_preflight);
criterion_main!(benches);
