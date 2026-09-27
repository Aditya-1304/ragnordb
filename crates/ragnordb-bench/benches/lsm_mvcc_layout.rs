//! Compare two experimental MVCC key layouts over the same deterministic data.
//!
//! Candidate A stores every record in one ordered map under a row-first,
//! prefix-free internal key. Candidate B stores defaults, writes, and locks in
//! separate ordered maps; its versioned-family keys use the same prefix-free
//! logical-key framing and descending timestamp suffix, with family identity
//! supplied by the selected tree. These are benchmark prototypes only; neither
//! encoding is a durable V1 storage contract.

use std::{
    collections::{BTreeMap, BTreeSet},
    hint::black_box,
    time::Duration,
};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use ragnordb_common::{codec::Value, ids::TableId};
use ragnordb_storage::key::{encode_row_key, make_row_key};

const TABLE_ID: TableId = TableId(44);
const ROW_COUNT: usize = 10_000;
const VERSIONS_PER_ROW: usize = 4;
const PAYLOAD_BYTES: usize = 64;
const LOCK_STRIDE: usize = 100;
const ROLLBACK_STRIDE: usize = 100;
const DELETE_STRIDE: usize = 200;
const SCAN_ROWS: usize = 100;
const COMMIT_BATCH_SIZE: usize = 32;
const LATEST_READ_TS: u64 = (VERSIONS_PER_ROW * 2 + 2) as u64;
const HISTORICAL_READ_TS: u64 = LATEST_READ_TS / 2;
const ROLLBACK_TIMESTAMP: u64 = LATEST_READ_TS + 1;
const PREWRITE_START_TS: u64 = LATEST_READ_TS + 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Family {
    Default = 1,
    Write = 2,
    Lock = 3,
}

#[derive(Clone, Copy, Debug)]
enum WriteKind {
    Put,
    Delete,
    Rollback,
}

#[derive(Clone, Copy, Debug)]
struct WriteValue {
    start_ts: u64,
    commit_ts: u64,
    kind: WriteKind,
}

#[derive(Clone)]
enum UnifiedValue {
    Default(Vec<u8>),
    Write(WriteValue),
    Lock,
}

#[derive(Clone, Default)]
struct UnifiedKeyspace {
    records: BTreeMap<Vec<u8>, UnifiedValue>,
}

#[derive(Clone, Default)]
struct SeparateTrees {
    defaults: BTreeMap<Vec<u8>, Vec<u8>>,
    writes: BTreeMap<Vec<u8>, WriteValue>,
    locks: BTreeSet<Vec<u8>>,
}

struct CommitItem {
    key: Vec<u8>,
    start_ts: u64,
    commit_ts: u64,
    value: Vec<u8>,
}

/// Encodes a logical key without changing its bytewise ordering, then adds a
/// terminator so the record-family suffix cannot be confused with key bytes.
fn encode_logical_key_prefix(logical_key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(logical_key.len() + 2);
    for byte in logical_key {
        if *byte == 0 {
            encoded.extend_from_slice(&[0, 0xff]);
        } else {
            encoded.push(*byte);
        }
    }
    encoded.extend_from_slice(&[0, 0]);
    encoded
}

fn encode_family_prefix(logical_key: &[u8], family: Family) -> Vec<u8> {
    let mut encoded = encode_logical_key_prefix(logical_key);
    encoded.push(family as u8);
    encoded
}

/// Encodes the common row-key/timestamp portion used inside one Candidate B
/// family tree. The family is identified by the tree, not by bytes in the key.
fn encode_versioned_tree_key(logical_key: &[u8], timestamp: u64) -> Vec<u8> {
    let mut encoded = encode_logical_key_prefix(logical_key);
    encoded.extend_from_slice(&(!timestamp).to_be_bytes());
    encoded
}

/// Encodes one versioned record key for Candidate A. Timestamp bytes sort in
/// descending timestamp order so the newest version is physically first.
fn encode_internal_key(logical_key: &[u8], family: Family, timestamp: Option<u64>) -> Vec<u8> {
    let mut encoded = encode_family_prefix(logical_key, family);
    if let Some(timestamp) = timestamp {
        encoded.extend_from_slice(&(!timestamp).to_be_bytes());
    }
    encoded
}

fn decode_logical_key_prefix_into(encoded: &[u8], logical_key: &mut Vec<u8>) -> usize {
    logical_key.clear();
    let mut offset = 0;

    loop {
        let byte = encoded[offset];
        offset += 1;
        if byte != 0 {
            logical_key.push(byte);
            continue;
        }

        match encoded[offset] {
            0 => {
                offset += 1;
                break;
            }
            0xff => {
                logical_key.push(0);
                offset += 1;
            }
            marker => panic!("unexpected logical-key escape marker {marker}"),
        }
    }

    offset
}

fn decode_internal_key_parts(encoded: &[u8], logical_key: &mut Vec<u8>) -> (Family, Option<u64>) {
    let mut offset = decode_logical_key_prefix_into(encoded, logical_key);
    let family = match encoded[offset] {
        value if value == Family::Default as u8 => Family::Default,
        value if value == Family::Write as u8 => Family::Write,
        value if value == Family::Lock as u8 => Family::Lock,
        value => panic!("unknown benchmark record family {value}"),
    };
    offset += 1;

    let timestamp = match encoded.len() - offset {
        0 => None,
        8 => {
            let bytes: [u8; 8] = encoded[offset..]
                .try_into()
                .expect("timestamp suffix has its declared width");
            Some(!u64::from_be_bytes(bytes))
        }
        length => panic!("invalid benchmark timestamp suffix length {length}"),
    };

    (family, timestamp)
}

fn encoded_key(id: i64) -> Vec<u8> {
    let row_key = make_row_key(TABLE_ID, &[Value::Int(id)]).expect("benchmark key must encode");
    encode_row_key(&row_key).expect("benchmark key must encode")
}

impl UnifiedKeyspace {
    fn put_lock(&mut self, key: &[u8]) {
        self.records.insert(
            encode_internal_key(key, Family::Lock, None),
            UnifiedValue::Lock,
        );
    }

    fn has_lock(&self, key: &[u8]) -> bool {
        self.records
            .contains_key(&encode_internal_key(key, Family::Lock, None))
    }

    fn has_rollback_witness(&self, key: &[u8], start_ts: u64) -> bool {
        matches!(
            self.records
                .get(&encode_internal_key(key, Family::Write, Some(start_ts),)),
            Some(UnifiedValue::Write(WriteValue {
                kind: WriteKind::Rollback,
                ..
            }))
        )
    }

    fn write_at(&self, key: &[u8], read_ts: u64) -> Option<WriteValue> {
        let prefix = encode_family_prefix(key, Family::Write);
        let mut lower_bound = prefix.clone();
        lower_bound.extend_from_slice(&(!read_ts).to_be_bytes());

        self.records
            .range(lower_bound..)
            .take_while(|(encoded, _)| encoded.starts_with(&prefix))
            .find_map(|(_, value)| match value {
                UnifiedValue::Write(write) if !matches!(write.kind, WriteKind::Rollback) => {
                    Some(*write)
                }
                UnifiedValue::Write(_) => None,
                UnifiedValue::Default(_) | UnifiedValue::Lock => None,
            })
    }

    fn read_at(&self, key: &[u8], read_ts: u64) -> Option<&[u8]> {
        let write = self.write_at(key, read_ts)?;
        if matches!(write.kind, WriteKind::Delete) {
            return None;
        }

        let default_key = encode_internal_key(key, Family::Default, Some(write.start_ts));
        match self.records.get(&default_key) {
            Some(UnifiedValue::Default(value)) => Some(value),
            _ => None,
        }
    }

    fn validate_prewrite(&self, key: &[u8], start_ts: u64) -> bool {
        !self.has_lock(key)
            && self
                .write_at(key, u64::MAX)
                .is_none_or(|write| write.commit_ts <= start_ts)
    }

    /// Walk the unified row-first keyspace and count all records inspected to
    /// produce one bounded visible-row page.
    fn scan_page(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        read_ts: u64,
        limit: usize,
    ) -> (usize, usize, usize) {
        let start = encode_family_prefix(start_key, Family::Default);
        let mut decoded_key = Vec::with_capacity(start_key.len());
        let mut current_key = Vec::new();
        let mut selected_write = false;
        let mut visible_rows = 0;
        let mut payload_bytes = 0;
        let mut records_examined = 0;

        for (encoded, value) in self.records.range(start..) {
            let (family, timestamp) = decode_internal_key_parts(encoded, &mut decoded_key);
            if decoded_key.as_slice() >= end_key {
                break;
            }
            records_examined += 1;

            if current_key != decoded_key {
                current_key.clear();
                current_key.extend_from_slice(&decoded_key);
                selected_write = false;
            }

            if family != Family::Write || selected_write || timestamp.is_none_or(|ts| ts > read_ts)
            {
                continue;
            }

            let UnifiedValue::Write(write) = value else {
                continue;
            };
            if matches!(write.kind, WriteKind::Rollback) {
                continue;
            }

            selected_write = true;
            if matches!(write.kind, WriteKind::Delete) {
                continue;
            }

            let default_key =
                encode_internal_key(&current_key, Family::Default, Some(write.start_ts));
            if let Some(UnifiedValue::Default(payload)) = self.records.get(&default_key) {
                visible_rows += 1;
                payload_bytes += payload.len();
                if visible_rows == limit {
                    break;
                }
            }
        }

        (visible_rows, payload_bytes, records_examined)
    }

    fn commit_batch(&mut self, batch: &[CommitItem]) {
        for item in batch {
            self.records
                .remove(&encode_internal_key(&item.key, Family::Lock, None));
            self.records.insert(
                encode_internal_key(&item.key, Family::Default, Some(item.start_ts)),
                UnifiedValue::Default(item.value.clone()),
            );
            self.records.insert(
                encode_internal_key(&item.key, Family::Write, Some(item.commit_ts)),
                UnifiedValue::Write(WriteValue {
                    start_ts: item.start_ts,
                    commit_ts: item.commit_ts,
                    kind: WriteKind::Put,
                }),
            );
        }
    }

    fn encoded_key_bytes(&self) -> usize {
        self.records.keys().map(Vec::len).sum()
    }
}

impl SeparateTrees {
    fn put_lock(&mut self, key: &[u8]) {
        self.locks.insert(key.to_vec());
    }

    fn has_lock(&self, key: &[u8]) -> bool {
        self.locks.contains(key)
    }

    fn has_rollback_witness(&self, key: &[u8], start_ts: u64) -> bool {
        matches!(
            self.writes.get(&encode_versioned_tree_key(key, start_ts)),
            Some(WriteValue {
                kind: WriteKind::Rollback,
                ..
            })
        )
    }

    fn write_at(&self, key: &[u8], read_ts: u64) -> Option<WriteValue> {
        let prefix = encode_logical_key_prefix(key);
        let lower = encode_versioned_tree_key(key, read_ts);
        self.writes
            .range(lower..)
            .take_while(|(encoded, _)| encoded.starts_with(&prefix))
            .find(|(_, write)| !matches!(write.kind, WriteKind::Rollback))
            .map(|(_, write)| *write)
    }

    fn read_at(&self, key: &[u8], read_ts: u64) -> Option<&[u8]> {
        let write = self.write_at(key, read_ts)?;
        if matches!(write.kind, WriteKind::Delete) {
            return None;
        }

        self.defaults
            .get(&encode_versioned_tree_key(key, write.start_ts))
            .map(Vec::as_slice)
    }

    fn validate_prewrite(&self, key: &[u8], start_ts: u64) -> bool {
        !self.has_lock(key)
            && self
                .write_at(key, u64::MAX)
                .is_none_or(|write| write.commit_ts <= start_ts)
    }

    /// Scan only the ordered write tree, then point-read payloads from the
    /// default tree for the newest version visible at `read_ts`.
    fn scan_page(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        read_ts: u64,
        limit: usize,
    ) -> (usize, usize, usize) {
        let lower = encode_versioned_tree_key(start_key, read_ts);
        let upper = encode_logical_key_prefix(end_key);
        let mut decoded_key = Vec::with_capacity(start_key.len());
        let mut current_key = Vec::new();
        let mut selected_write = false;
        let mut visible_rows = 0;
        let mut payload_bytes = 0;
        let mut records_examined = 0;

        for (encoded_key, write) in self.writes.range(lower..upper) {
            decode_logical_key_prefix_into(encoded_key, &mut decoded_key);
            records_examined += 1;
            if current_key != decoded_key {
                current_key.clear();
                current_key.extend_from_slice(&decoded_key);
                selected_write = false;
            }
            if selected_write {
                continue;
            }
            if write.commit_ts > read_ts {
                continue;
            }

            if matches!(write.kind, WriteKind::Rollback) {
                continue;
            }
            selected_write = true;
            if matches!(write.kind, WriteKind::Delete) {
                continue;
            }

            if let Some(payload) = self
                .defaults
                .get(&encode_versioned_tree_key(&current_key, write.start_ts))
            {
                visible_rows += 1;
                payload_bytes += payload.len();
                if visible_rows == limit {
                    break;
                }
            }
        }

        (visible_rows, payload_bytes, records_examined)
    }

    fn commit_batch(&mut self, batch: &[CommitItem]) {
        for item in batch {
            self.locks.remove(&item.key);
            self.defaults.insert(
                encode_versioned_tree_key(&item.key, item.start_ts),
                item.value.clone(),
            );
            self.writes.insert(
                encode_versioned_tree_key(&item.key, item.commit_ts),
                WriteValue {
                    start_ts: item.start_ts,
                    commit_ts: item.commit_ts,
                    kind: WriteKind::Put,
                },
            );
        }
    }

    fn encoded_key_bytes(&self) -> usize {
        self.defaults.keys().map(Vec::len).sum::<usize>()
            + self.writes.keys().map(Vec::len).sum::<usize>()
            + self.locks.iter().map(Vec::len).sum::<usize>()
    }
}

fn build_fixtures() -> (Vec<Vec<u8>>, UnifiedKeyspace, SeparateTrees) {
    let mut keys = (0..ROW_COUNT)
        .map(|id| encoded_key(id as i64))
        .collect::<Vec<_>>();
    keys.sort_unstable();

    let mut unified = UnifiedKeyspace::default();
    let mut separate = SeparateTrees::default();

    for (row_id, key) in keys.iter().enumerate() {
        for version in 0..VERSIONS_PER_ROW {
            let start_ts = (version * 2 + 1) as u64;
            let commit_ts = start_ts + 1;
            let payload = vec![(row_id % 251) as u8; PAYLOAD_BYTES];
            let write = WriteValue {
                start_ts,
                commit_ts,
                kind: WriteKind::Put,
            };

            unified.records.insert(
                encode_internal_key(key, Family::Default, Some(start_ts)),
                UnifiedValue::Default(payload.clone()),
            );
            unified.records.insert(
                encode_internal_key(key, Family::Write, Some(commit_ts)),
                UnifiedValue::Write(write),
            );
            separate
                .defaults
                .insert(encode_versioned_tree_key(key, start_ts), payload);
            separate
                .writes
                .insert(encode_versioned_tree_key(key, commit_ts), write);
        }

        if row_id % DELETE_STRIDE == 0 {
            let delete = WriteValue {
                start_ts: LATEST_READ_TS - 1,
                commit_ts: LATEST_READ_TS,
                kind: WriteKind::Delete,
            };
            unified.records.insert(
                encode_internal_key(key, Family::Write, Some(delete.commit_ts)),
                UnifiedValue::Write(delete),
            );
            separate
                .writes
                .insert(encode_versioned_tree_key(key, delete.commit_ts), delete);
        }

        if row_id % ROLLBACK_STRIDE == 0 {
            let rollback = WriteValue {
                start_ts: ROLLBACK_TIMESTAMP,
                commit_ts: ROLLBACK_TIMESTAMP,
                kind: WriteKind::Rollback,
            };
            unified.records.insert(
                encode_internal_key(key, Family::Write, Some(rollback.commit_ts)),
                UnifiedValue::Write(rollback),
            );
            separate
                .writes
                .insert(encode_versioned_tree_key(key, rollback.commit_ts), rollback);
        }

        if row_id % LOCK_STRIDE == 0 {
            unified.put_lock(key);
            separate.put_lock(key);
        }
    }

    (keys, unified, separate)
}

fn build_commit_batch() -> Vec<CommitItem> {
    (0..COMMIT_BATCH_SIZE)
        .map(|index| {
            let start_ts = LATEST_READ_TS + 100 + (index as u64 * 2);
            CommitItem {
                key: encoded_key((ROW_COUNT + index) as i64),
                start_ts,
                commit_ts: start_ts + 1,
                value: vec![0xa5; PAYLOAD_BYTES],
            }
        })
        .collect()
}

fn bench_layouts(criterion: &mut Criterion) {
    let (keys, unified, separate) = build_fixtures();
    let point_key = &keys[ROW_COUNT / 2 + 1];
    let prewrite_key = &keys[1];
    let lock_hit_key = &keys[0];
    let lock_miss_key = &keys[1];
    let rollback_key = &keys[ROLLBACK_STRIDE];
    let scan_start = ROW_COUNT / 2;
    let scan_start_key = &keys[scan_start];
    let scan_end_key = &keys[scan_start + SCAN_ROWS + 1];
    let commit_batch = build_commit_batch();

    for pair in keys.windows(2) {
        assert_eq!(
            encode_logical_key_prefix(&pair[0]).cmp(&encode_logical_key_prefix(&pair[1])),
            pair[0].cmp(&pair[1]),
            "Candidate A key framing must preserve logical row-key ordering"
        );
    }
    assert!(
        encode_internal_key(point_key, Family::Write, Some(LATEST_READ_TS))
            < encode_internal_key(point_key, Family::Write, Some(HISTORICAL_READ_TS)),
        "newer versions must sort before older versions"
    );
    assert_eq!(
        unified.read_at(point_key, LATEST_READ_TS),
        separate.read_at(point_key, LATEST_READ_TS),
        "latest reads must agree before timing"
    );
    assert!(unified.read_at(point_key, LATEST_READ_TS).is_some());
    assert_eq!(
        unified.read_at(point_key, HISTORICAL_READ_TS),
        separate.read_at(point_key, HISTORICAL_READ_TS),
        "historical reads must agree before timing"
    );
    assert_eq!(
        unified.read_at(rollback_key, ROLLBACK_TIMESTAMP),
        separate.read_at(rollback_key, ROLLBACK_TIMESTAMP),
        "rollback witnesses must remain invisible to reads"
    );
    assert!(unified.read_at(rollback_key, ROLLBACK_TIMESTAMP).is_some());
    assert_eq!(
        unified.read_at(&keys[0], LATEST_READ_TS),
        separate.read_at(&keys[0], LATEST_READ_TS),
        "delete tombstones must agree before timing"
    );
    assert!(unified.read_at(&keys[0], LATEST_READ_TS).is_none());
    assert!(unified.has_rollback_witness(rollback_key, ROLLBACK_TIMESTAMP));
    assert_eq!(
        unified.has_rollback_witness(rollback_key, ROLLBACK_TIMESTAMP),
        separate.has_rollback_witness(rollback_key, ROLLBACK_TIMESTAMP),
        "rollback witness lookup must agree"
    );
    assert_eq!(
        unified.validate_prewrite(prewrite_key, PREWRITE_START_TS),
        separate.validate_prewrite(prewrite_key, PREWRITE_START_TS),
        "prewrite decisions must agree before timing"
    );
    assert!(unified.validate_prewrite(prewrite_key, PREWRITE_START_TS));
    let unified_scan = unified.scan_page(scan_start_key, scan_end_key, LATEST_READ_TS, SCAN_ROWS);
    let separate_scan = separate.scan_page(scan_start_key, scan_end_key, LATEST_READ_TS, SCAN_ROWS);
    assert_eq!(
        (unified_scan.0, unified_scan.1),
        (separate_scan.0, separate_scan.1),
        "scan results must agree before timing"
    );
    assert_eq!(unified_scan.0, SCAN_ROWS);
    let unified_historical_scan =
        unified.scan_page(scan_start_key, scan_end_key, HISTORICAL_READ_TS, SCAN_ROWS);
    let separate_historical_scan =
        separate.scan_page(scan_start_key, scan_end_key, HISTORICAL_READ_TS, SCAN_ROWS);
    assert_eq!(
        (unified_historical_scan.0, unified_historical_scan.1),
        (separate_historical_scan.0, separate_historical_scan.1),
        "historical scan results must agree before timing"
    );
    assert_eq!(unified_historical_scan.0, SCAN_ROWS);

    let mut committed_unified = unified.clone();
    let mut committed_separate = separate.clone();
    for item in &commit_batch {
        committed_unified.put_lock(&item.key);
        committed_separate.put_lock(&item.key);
    }
    committed_unified.commit_batch(&commit_batch);
    committed_separate.commit_batch(&commit_batch);
    for item in &commit_batch {
        assert_eq!(
            committed_unified.read_at(&item.key, item.commit_ts),
            committed_separate.read_at(&item.key, item.commit_ts),
            "commit results must agree before timing"
        );
    }

    eprintln!(
        "layout fixture: rows={ROW_COUNT}, versions_per_row={VERSIONS_PER_ROW}, payload_bytes={PAYLOAD_BYTES}, lock_rate=1%, rollback_rate=1%, tombstone_rate=0.5%; key_bytes unified={} separate={}; scan_100 records_examined unified={} separate={}",
        unified.encoded_key_bytes(),
        separate.encoded_key_bytes(),
        unified_scan.2,
        separate_scan.2,
    );

    let mut group = criterion.benchmark_group("stage4_2/mvcc_layout");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(300));

    group.bench_function("point_latest/unified", |bencher| {
        bencher.iter(|| black_box(unified.read_at(black_box(point_key), LATEST_READ_TS)));
    });
    group.bench_function("point_latest/separate_trees", |bencher| {
        bencher.iter(|| black_box(separate.read_at(black_box(point_key), LATEST_READ_TS)));
    });
    group.bench_function("point_historical/unified", |bencher| {
        bencher.iter(|| black_box(unified.read_at(black_box(point_key), HISTORICAL_READ_TS)));
    });
    group.bench_function("point_historical/separate_trees", |bencher| {
        bencher.iter(|| black_box(separate.read_at(black_box(point_key), HISTORICAL_READ_TS)));
    });
    group.bench_function("lock_hit_miss/unified", |bencher| {
        bencher.iter(|| {
            black_box(unified.has_lock(black_box(lock_hit_key)));
            black_box(unified.has_lock(black_box(lock_miss_key)));
        });
    });
    group.bench_function("lock_hit_miss/separate_trees", |bencher| {
        bencher.iter(|| {
            black_box(separate.has_lock(black_box(lock_hit_key)));
            black_box(separate.has_lock(black_box(lock_miss_key)));
        });
    });
    group.bench_function("rollback_witness/unified", |bencher| {
        bencher.iter(|| {
            black_box(unified.has_rollback_witness(black_box(rollback_key), ROLLBACK_TIMESTAMP))
        });
    });
    group.bench_function("rollback_witness/separate_trees", |bencher| {
        bencher.iter(|| {
            black_box(separate.has_rollback_witness(black_box(rollback_key), ROLLBACK_TIMESTAMP))
        });
    });
    group.bench_function("prewrite_validation/unified", |bencher| {
        bencher.iter(|| {
            black_box(unified.validate_prewrite(black_box(prewrite_key), PREWRITE_START_TS))
        });
    });
    group.bench_function("prewrite_validation/separate_trees", |bencher| {
        bencher.iter(|| {
            black_box(separate.validate_prewrite(black_box(prewrite_key), PREWRITE_START_TS))
        });
    });
    group.bench_function("scan_100/unified", |bencher| {
        bencher.iter(|| {
            black_box(unified.scan_page(
                black_box(scan_start_key),
                black_box(scan_end_key),
                LATEST_READ_TS,
                SCAN_ROWS,
            ))
        });
    });
    group.bench_function("scan_100/separate_trees", |bencher| {
        bencher.iter(|| {
            black_box(separate.scan_page(
                black_box(scan_start_key),
                black_box(scan_end_key),
                LATEST_READ_TS,
                SCAN_ROWS,
            ))
        });
    });
    group.bench_function("scan_100_historical/unified", |bencher| {
        bencher.iter(|| {
            black_box(unified.scan_page(
                black_box(scan_start_key),
                black_box(scan_end_key),
                HISTORICAL_READ_TS,
                SCAN_ROWS,
            ))
        });
    });
    group.bench_function("scan_100_historical/separate_trees", |bencher| {
        bencher.iter(|| {
            black_box(separate.scan_page(
                black_box(scan_start_key),
                black_box(scan_end_key),
                HISTORICAL_READ_TS,
                SCAN_ROWS,
            ))
        });
    });
    group.bench_function("commit_batch_32/unified", |bencher| {
        bencher.iter_batched_ref(
            || {
                let mut state = unified.clone();
                for item in &commit_batch {
                    state.put_lock(&item.key);
                }
                state
            },
            |state| {
                state.commit_batch(&commit_batch);
                black_box(state.records.len())
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("commit_batch_32/separate_trees", |bencher| {
        bencher.iter_batched_ref(
            || {
                let mut state = separate.clone();
                for item in &commit_batch {
                    state.put_lock(&item.key);
                }
                state
            },
            |state| {
                state.commit_batch(&commit_batch);
                black_box(state.defaults.len() + state.writes.len() + state.locks.len())
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_layouts);
criterion_main!(benches);
