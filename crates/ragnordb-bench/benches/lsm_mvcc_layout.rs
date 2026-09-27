//! Stage 4.2 MVCC layout comparison.
//!
//! This target contains two deliberately separate measurements:
//! - Criterion microbenchmarks over the same in-memory MVCC model.
//! - A bounded benchmark-only immutable-block model that reports byte, cache,
//!   scan-amplification, flush, and leveled-compaction behavior.
//!
//! Neither the key encodings nor the block model is a production storage
//! format. In particular, the compaction counters model a declared policy and
//! do not claim filesystem or device latency.

use std::{
    collections::{HashMap, VecDeque},
    hint::black_box,
    time::{Duration, Instant},
};

use criterion::{Criterion, criterion_group, criterion_main};
use ragnordb_bench::lsm_layout::{
    BENCHMARK_LOCK_VALUE_BYTES, BENCHMARK_WRITE_VALUE_BYTES, EXPERIMENTAL_ENCODING_VERSION, Engine,
    KeyScratch, Layout, Namespace, RecordEdit, WriteKind, WriteRecord,
};
use ragnordb_common::{codec::Value, ids::TableId};
use ragnordb_storage::key::{encode_row_key, make_row_key};

const TABLE_ID: TableId = TableId(44);
const PROFILE_LOGICAL_ROWS: usize = 100_000;
const BLOCK_BYTES: usize = 4 * 1024;
const CACHE_BYTES: usize = 16 * 1024 * 1024;
const FLUSH_BYTES: usize = 4 * 1024 * 1024;
const L0_TRIGGER_RUNS: usize = 4;
const LEVEL_SIZE_RATIO: usize = 10;
const LEVEL_COUNT: usize = 4;
const LAYOUTS: [Layout; 3] = [
    Layout::UnifiedRowFirst,
    Layout::SeparateFamilies,
    Layout::UnifiedFamilyFirst,
];

/// One deterministic workload point varying rows, MVCC history, payload size,
/// lock density, rollback/delete frequency, and canonical key distribution.
#[derive(Clone, Copy)]
struct Profile {
    name: &'static str,
    rows: usize,
    versions: usize,
    payload_bytes: usize,
    lock_stride: usize,
    rollback_stride: usize,
    delete_stride: usize,
    random_keys: bool,
}

impl Profile {
    fn latest_ts(self) -> u64 {
        (self.versions as u64 * 2) + 2
    }

    fn middle_ts(self) -> u64 {
        self.versions as u64
    }

    fn old_ts(self) -> u64 {
        2
    }

    fn rollback_ts(self) -> u64 {
        self.latest_ts() + 1
    }
}

const PROFILES: [Profile; 6] = [
    Profile {
        name: "oltp_normal_seq",
        rows: 100_000,
        versions: 4,
        payload_bytes: 96,
        lock_stride: 100,
        rollback_stride: 100,
        delete_stride: 400,
        random_keys: false,
    },
    Profile {
        name: "history_heavy_random",
        rows: 100_000,
        versions: 16,
        payload_bytes: 64,
        lock_stride: 100,
        rollback_stride: 100,
        delete_stride: 0,
        random_keys: true,
    },
    Profile {
        name: "large_rows",
        rows: 100_000,
        versions: 4,
        payload_bytes: 1024,
        lock_stride: 0,
        rollback_stride: 0,
        delete_stride: 0,
        random_keys: false,
    },
    Profile {
        name: "lock_heavy",
        rows: 100_000,
        versions: 4,
        payload_bytes: 128,
        lock_stride: 10,
        rollback_stride: 100,
        delete_stride: 0,
        random_keys: true,
    },
    Profile {
        name: "write_churn",
        rows: 100_000,
        versions: 8,
        payload_bytes: 256,
        lock_stride: 100,
        rollback_stride: 10,
        delete_stride: 20,
        random_keys: false,
    },
    Profile {
        name: "scan_heavy_million",
        rows: 1_000_000,
        versions: 4,
        payload_bytes: 128,
        lock_stride: 1_000,
        rollback_stride: 1_000,
        delete_stride: 0,
        random_keys: false,
    },
];

fn encoded_key(id: i64) -> Vec<u8> {
    let row = make_row_key(TABLE_ID, &[Value::Int(id)]).expect("benchmark row key encodes");
    encode_row_key(&row).expect("benchmark row key encodes")
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Builds logical row keys with the production canonical row-key codec, then
/// sorts them once so all physical candidates receive identical identities.
fn logical_keys(profile: Profile) -> Vec<Vec<u8>> {
    let mut keys = (0..profile.rows)
        .map(|index| {
            let id = if profile.random_keys {
                splitmix64(index as u64 + 17) as i64
            } else {
                index as i64
            };
            encoded_key(id)
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys
}

/// Generates identical logical records for every candidate; only physical key
/// encoding and tree organization differ between engines.
fn row_fixture_edits<'a>(
    profile: Profile,
    keys: &'a [Vec<u8>],
) -> impl Iterator<Item = RecordEdit> + 'a {
    keys.iter().enumerate().flat_map(move |(row_index, key)| {
        let mut edits = Vec::with_capacity(profile.versions * 2 + 3);
        for version in 0..profile.versions {
            let start_ts = (version as u64 * 2) + 1;
            let commit_ts = start_ts + 1;
            let payload_byte = row_index.wrapping_add(version) as u8;
            edits.push(RecordEdit::put_default(
                key.clone(),
                start_ts,
                &vec![payload_byte; profile.payload_bytes],
            ));
            edits.push(RecordEdit::put_write(
                key.clone(),
                WriteRecord {
                    start_ts,
                    commit_ts,
                    kind: WriteKind::Put,
                },
            ));
        }
        if profile.delete_stride != 0 && row_index % profile.delete_stride == 0 {
            edits.push(RecordEdit::put_write(
                key.clone(),
                WriteRecord {
                    start_ts: profile.latest_ts(),
                    commit_ts: profile.latest_ts(),
                    kind: WriteKind::Delete,
                },
            ));
        }
        if profile.rollback_stride != 0 && row_index % profile.rollback_stride == 0 {
            edits.push(RecordEdit::put_write(
                key.clone(),
                WriteRecord {
                    start_ts: profile.rollback_ts(),
                    commit_ts: profile.rollback_ts(),
                    kind: WriteKind::Rollback,
                },
            ));
        }
        if profile.lock_stride != 0 && row_index % profile.lock_stride == 0 {
            edits.push(RecordEdit::put_lock(key.clone()));
        }
        edits
    })
}

fn commit_edits(profile: Profile) -> Vec<RecordEdit> {
    let mut edits = Vec::with_capacity(96);
    for index in 0..32 {
        let key = encoded_key(i64::MAX - index as i64 - 100);
        let start_ts = profile.latest_ts() + 100 + (index as u64 * 2);
        edits.push(RecordEdit::delete_lock(key.clone()));
        edits.push(RecordEdit::put_default(
            key.clone(),
            start_ts,
            &vec![0xa5; profile.payload_bytes],
        ));
        edits.push(RecordEdit::put_write(
            key,
            WriteRecord {
                start_ts,
                commit_ts: start_ts + 1,
                kind: WriteKind::Put,
            },
        ));
    }
    edits
}

fn rollback_edit(profile: Profile, key: &[u8]) -> Vec<RecordEdit> {
    vec![RecordEdit::put_write(
        key.to_vec(),
        WriteRecord {
            start_ts: profile.rollback_ts() + 1,
            commit_ts: profile.rollback_ts() + 1,
            kind: WriteKind::Rollback,
        },
    )]
}

fn bench_engine_profile(
    criterion: &mut Criterion,
    profile: Profile,
    layout: Layout,
    keys: &[Vec<u8>],
) {
    let build_started = Instant::now();
    let engine = Engine::from_fixture(layout, row_fixture_edits(profile, keys))
        .expect("benchmark fixture uses valid unique records");
    let fixture_build = build_started.elapsed();

    let point_index = (0..keys.len())
        .find(|index| profile.delete_stride == 0 || index % profile.delete_stride != 0)
        .unwrap_or(keys.len() / 2);
    let point_key = &keys[point_index];
    let lock_index = if profile.lock_stride == 0 { 1 } else { 0 };
    let lock_hit_key = &keys[lock_index];
    let lock_miss_key = &keys[keys.len() - 1];
    let rollback_index = 0;
    let rollback_key = &keys[rollback_index];
    let miss_key = encoded_key(i64::MAX - 7);
    let batch = commit_edits(profile);
    let rollback = rollback_edit(profile, &miss_key);
    let mut group =
        criterion.benchmark_group(format!("stage4_2/{}/{}", profile.name, layout_name(layout)));
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(30));
    group.measurement_time(Duration::from_millis(60));

    for (name, read_ts) in [
        ("point_latest", profile.latest_ts()),
        ("point_middle", profile.middle_ts()),
        ("point_old", profile.old_ts()),
    ] {
        group.bench_function(name, |bencher| {
            let mut scratch = KeyScratch::default();
            bencher.iter(|| {
                black_box(engine.read_len_at(black_box(point_key), read_ts, &mut scratch))
            });
        });
    }
    group.bench_function("point_miss", |bencher| {
        let mut scratch = KeyScratch::default();
        bencher.iter(|| {
            black_box(engine.read_len_at(black_box(&miss_key), profile.latest_ts(), &mut scratch))
        });
    });
    group.bench_function("lock_hit_miss_pair", |bencher| {
        let mut scratch = KeyScratch::default();
        bencher.iter(|| {
            black_box(engine.contains_lock(black_box(lock_hit_key), &mut scratch));
            black_box(engine.contains_lock(black_box(lock_miss_key), &mut scratch));
        });
    });
    group.bench_function("rollback_witness", |bencher| {
        let mut scratch = KeyScratch::default();
        bencher.iter(|| {
            black_box(engine.has_rollback_witness(
                black_box(rollback_key),
                profile.rollback_ts(),
                &mut scratch,
            ))
        });
    });
    group.bench_function("prewrite_validation", |bencher| {
        let mut scratch = KeyScratch::default();
        bencher.iter(|| {
            black_box(engine.validate_prewrite(
                black_box(point_key),
                profile.latest_ts() + 10,
                &mut scratch,
            ))
        });
    });
    group.bench_function("intent_commit_batch_32", |bencher| {
        bencher.iter(|| {
            engine.publish_atomic(black_box(&batch)).unwrap();
            black_box(())
        });
    });
    group.bench_function("rollback_publish", |bencher| {
        bencher.iter(|| {
            engine.publish_atomic(black_box(&rollback)).unwrap();
            black_box(())
        });
    });

    let scan_start = keys.len() / 4;
    for rows in [10, 100, 1_000] {
        let start = scan_start.min(keys.len() - 1);
        let end = (start + rows * 3 + 1).min(keys.len() - 1);
        let start_key = &keys[start];
        let end_key = &keys[end];
        for (suffix, read_ts) in [("latest", profile.latest_ts()), ("old", profile.old_ts())] {
            group.bench_function(format!("scan_{rows}_{suffix}"), |bencher| {
                let mut scratch = KeyScratch::default();
                bencher.iter(|| {
                    black_box(engine.scan_page(
                        black_box(start_key),
                        black_box(end_key),
                        read_ts,
                        rows,
                        &mut scratch,
                    ))
                });
            });
        }
    }
    group.finish();

    eprintln!(
        "LOGICAL_FIXTURE profile={} layout={} rows={} versions={} payload={} records={} key_bytes={} build_ms={:.1}",
        profile.name,
        layout_name(layout),
        profile.rows,
        profile.versions,
        profile.payload_bytes,
        engine.record_count(),
        engine.encoded_key_bytes(),
        fixture_build.as_secs_f64() * 1_000.0,
    );
}

/// Merges sorted input runs using the candidate's encoded physical keys.
fn merge_sorted_runs(runs: &[Vec<Vec<u8>>]) -> Vec<Vec<u8>> {
    let mut merged = runs.first().cloned().unwrap_or_default();
    for run in runs.iter().skip(1) {
        let mut next = Vec::with_capacity(merged.len() + run.len());
        let (mut left, mut right) = (0, 0);
        while left < merged.len() && right < run.len() {
            if merged[left] <= run[right] {
                next.push(merged[left].clone());
                left += 1;
            } else {
                next.push(run[right].clone());
                right += 1;
            }
        }
        next.extend(merged[left..].iter().cloned());
        next.extend(run[right..].iter().cloned());
        merged = next;
    }
    merged
}

/// Builds four overlapping L0-like runs from one deterministic row set.
/// Candidate B keeps one four-run group per family; A and C use one group.
fn sorted_run_fixture(profile: Profile, keys: &[Vec<u8>], layout: Layout) -> Vec<Vec<Vec<u8>>> {
    const RUNS: usize = 4;
    const MERGE_ROWS: usize = 10_000;
    let family_count = if layout == Layout::SeparateFamilies {
        3
    } else {
        1
    };
    let mut output = vec![Vec::new(); family_count * RUNS];
    let mut encoded = Vec::with_capacity(64);
    for (row_index, key) in keys.iter().take(MERGE_ROWS).enumerate() {
        let run = row_index % RUNS;
        for namespace in [Namespace::Default, Namespace::Write, Namespace::Lock] {
            let family_index = match (layout, namespace) {
                (Layout::SeparateFamilies, Namespace::Default) => 0,
                (Layout::SeparateFamilies, Namespace::Write) => 1,
                (Layout::SeparateFamilies, Namespace::Lock) => 2,
                (_, _) => 0,
            };
            if namespace == Namespace::Default {
                for version in (0..profile.versions).rev() {
                    encode_physical_key(
                        layout,
                        namespace,
                        key,
                        Some(version as u64 * 2 + 1),
                        &mut encoded,
                    );
                    output[family_index * RUNS + run].push(encoded.clone());
                }
            } else if namespace == Namespace::Write {
                if profile.rollback_stride != 0 && row_index % profile.rollback_stride == 0 {
                    encode_physical_key(
                        layout,
                        namespace,
                        key,
                        Some(profile.rollback_ts()),
                        &mut encoded,
                    );
                    output[family_index * RUNS + run].push(encoded.clone());
                }
                if profile.delete_stride != 0 && row_index % profile.delete_stride == 0 {
                    encode_physical_key(
                        layout,
                        namespace,
                        key,
                        Some(profile.latest_ts()),
                        &mut encoded,
                    );
                    output[family_index * RUNS + run].push(encoded.clone());
                }
                for version in (0..profile.versions).rev() {
                    encode_physical_key(
                        layout,
                        namespace,
                        key,
                        Some(version as u64 * 2 + 2),
                        &mut encoded,
                    );
                    output[family_index * RUNS + run].push(encoded.clone());
                }
            } else if profile.lock_stride != 0 && row_index % profile.lock_stride == 0 {
                encode_physical_key(layout, namespace, key, None, &mut encoded);
                output[family_index * RUNS + run].push(encoded.clone());
            }
        }
    }
    for run in &mut output {
        run.sort_unstable();
    }
    output
}

fn bench_sorted_run_merge(
    criterion: &mut Criterion,
    profile: Profile,
    keys: &[Vec<u8>],
    layout: Layout,
) {
    const RUNS: usize = 4;
    let runs = sorted_run_fixture(profile, keys, layout);
    let input_records = runs.iter().map(Vec::len).sum::<usize>();
    let mut group =
        criterion.benchmark_group(format!("stage4_2/{}/{}", profile.name, layout_name(layout)));
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(30));
    group.measurement_time(Duration::from_millis(60));
    group.bench_function("sorted_run_merge_4_l0", |bencher| {
        bencher.iter(|| {
            let merged_count = if layout == Layout::SeparateFamilies {
                (0..3)
                    .map(|family| {
                        merge_sorted_runs(&runs[family * RUNS..family * RUNS + RUNS]).len()
                    })
                    .sum()
            } else {
                merge_sorted_runs(&runs).len()
            };
            black_box(merged_count)
        });
    });
    group.finish();
    eprintln!(
        "SORTED_RUN_MERGE_FIXTURE profile={} layout={} rows=10000 input_runs={} input_records={}",
        profile.name,
        layout_name(layout),
        runs.len(),
        input_records,
    );
}

fn layout_name(layout: Layout) -> &'static str {
    match layout {
        Layout::UnifiedRowFirst => "A_row_first",
        Layout::SeparateFamilies => "B_separate_families",
        Layout::UnifiedFamilyFirst => "C_family_first",
    }
}

/// Builds simulated immutable blocks from exact key and value lengths. Values
/// are not copied into the model; fence keys and byte counts preserve bounded
/// memory use for the million-row profile.
#[derive(Default)]
struct BlockBuilder {
    blocks: Vec<BlockMeta>,
    current_first: Vec<u8>,
    current_last: Vec<u8>,
    current_bytes: usize,
    total_bytes: usize,
    key_bytes: usize,
    records: usize,
}

struct BlockMeta {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    bytes: usize,
}

impl BlockBuilder {
    fn push(&mut self, key: &[u8], value_bytes: usize) {
        if self.records > 0 {
            debug_assert!(
                self.current_last.as_slice() <= key,
                "physical fixture keys must arrive in comparator order"
            );
        }
        let record_bytes = key.len() + value_bytes;
        if self.current_bytes > 0 && self.current_bytes + record_bytes > BLOCK_BYTES {
            self.finish_block();
        }
        if self.current_bytes == 0 {
            self.current_first.clear();
            self.current_first.extend_from_slice(key);
        }
        self.current_last.clear();
        self.current_last.extend_from_slice(key);
        self.current_bytes += record_bytes;
        self.total_bytes += record_bytes;
        self.key_bytes += key.len();
        self.records += 1;
    }

    fn finish_block(&mut self) {
        if self.current_bytes == 0 {
            return;
        }
        self.blocks.push(BlockMeta {
            first_key: self.current_first.clone(),
            last_key: self.current_last.clone(),
            bytes: self.current_bytes,
        });
        self.current_bytes = 0;
    }

    fn finish(mut self) -> BlockFile {
        self.finish_block();
        BlockFile {
            blocks: self.blocks,
            total_bytes: self.total_bytes,
            key_bytes: self.key_bytes,
            records: self.records,
        }
    }
}

/// One immutable sorted run represented by a sparse fence-key index and block
/// byte totals shared across the candidate layouts.
struct BlockFile {
    blocks: Vec<BlockMeta>,
    total_bytes: usize,
    key_bytes: usize,
    records: usize,
}

impl BlockFile {
    fn point_block(&self, search_key: &[u8]) -> Option<usize> {
        let index = self
            .blocks
            .partition_point(|block| block.last_key.as_slice() < search_key);
        self.blocks.get(index).map(|_| index)
    }

    fn range_blocks(&self, start: &[u8], end: &[u8]) -> std::ops::Range<usize> {
        let first = self
            .blocks
            .partition_point(|block| block.last_key.as_slice() < start);
        let last = self
            .blocks
            .partition_point(|block| block.first_key.as_slice() < end);
        first.min(last)..last
    }
}

/// Candidate files for row families. SeparateFamilies keeps three files;
/// every file in that candidate is read through the same bounded cache.
struct PhysicalFiles {
    layout: Layout,
    files: [Option<BlockFile>; 3],
}

impl PhysicalFiles {
    fn file_index(&self, namespace: Namespace) -> usize {
        if self.layout != Layout::SeparateFamilies {
            0
        } else {
            match namespace {
                Namespace::Default => 0,
                Namespace::Write => 1,
                Namespace::Lock => 2,
                _ => unreachable!("physical fixture includes row MVCC families only"),
            }
        }
    }

    fn file(&self, namespace: Namespace) -> Option<&BlockFile> {
        self.files[self.file_index(namespace)].as_ref()
    }

    fn point_block(&self, namespace: Namespace, key: &[u8]) -> Option<(usize, usize)> {
        let file_index = self.file_index(namespace);
        let block = self.file(namespace)?.point_block(key)?;
        Some((file_index, block))
    }

    fn scan_blocks(
        &self,
        namespace: Namespace,
        start: &[u8],
        end: &[u8],
    ) -> Option<(usize, std::ops::Range<usize>)> {
        let file_index = self.file_index(namespace);
        let file = self.file(namespace)?;
        Some((file_index, file.range_blocks(start, end)))
    }

    fn total_bytes(&self) -> usize {
        self.files
            .iter()
            .filter_map(Option::as_ref)
            .map(|file| file.total_bytes)
            .sum()
    }

    fn total_key_bytes(&self) -> usize {
        self.files
            .iter()
            .filter_map(Option::as_ref)
            .map(|file| file.key_bytes)
            .sum()
    }

    fn total_records(&self) -> usize {
        self.files
            .iter()
            .filter_map(Option::as_ref)
            .map(|file| file.records)
            .sum()
    }

    fn total_blocks(&self) -> usize {
        self.files
            .iter()
            .filter_map(Option::as_ref)
            .map(|file| file.blocks.len())
            .sum()
    }
}

fn encode_prefix(logical_key: &[u8], encoded: &mut Vec<u8>) {
    encoded.clear();
    for byte in logical_key {
        if *byte == 0 {
            encoded.extend_from_slice(&[0, 0xff]);
        } else {
            encoded.push(*byte);
        }
    }
    encoded.extend_from_slice(&[0, 0]);
}

fn encode_physical_key(
    layout: Layout,
    namespace: Namespace,
    key: &[u8],
    timestamp: Option<u64>,
    encoded: &mut Vec<u8>,
) {
    encoded.clear();
    match layout {
        Layout::UnifiedRowFirst => {
            encode_prefix(key, encoded);
            encoded.push(match namespace {
                Namespace::Write => 0x01,
                Namespace::Default => 0x02,
                Namespace::Lock => 0x03,
                _ => namespace as u8,
            });
        }
        Layout::SeparateFamilies => match namespace {
            Namespace::Lock => encoded.extend_from_slice(key),
            Namespace::Default | Namespace::Write => encode_prefix(key, encoded),
            _ => unreachable!(),
        },
        Layout::UnifiedFamilyFirst => {
            encoded.push(namespace as u8);
            encode_prefix(key, encoded);
        }
    }
    encoded.push(EXPERIMENTAL_ENCODING_VERSION);
    if let Some(timestamp) = timestamp {
        encoded.extend_from_slice(&(!timestamp).to_be_bytes());
    }
}

fn emit_row_records(
    profile: Profile,
    row_index: usize,
    key: &[u8],
    layout: Layout,
    target: Namespace,
    builder: &mut BlockBuilder,
    encoded: &mut Vec<u8>,
) {
    match target {
        Namespace::Write => {
            if profile.rollback_stride != 0 && row_index.is_multiple_of(profile.rollback_stride) {
                encode_physical_key(
                    layout,
                    Namespace::Write,
                    key,
                    Some(profile.rollback_ts()),
                    encoded,
                );
                builder.push(encoded, BENCHMARK_WRITE_VALUE_BYTES);
            }
            if profile.delete_stride != 0 && row_index.is_multiple_of(profile.delete_stride) {
                encode_physical_key(
                    layout,
                    Namespace::Write,
                    key,
                    Some(profile.latest_ts()),
                    encoded,
                );
                builder.push(encoded, BENCHMARK_WRITE_VALUE_BYTES);
            }
            for version in (0..profile.versions).rev() {
                let timestamp = (version as u64 * 2) + 2;
                encode_physical_key(layout, Namespace::Write, key, Some(timestamp), encoded);
                builder.push(encoded, BENCHMARK_WRITE_VALUE_BYTES);
            }
        }
        Namespace::Default => {
            for version in (0..profile.versions).rev() {
                let start_ts = (version as u64 * 2) + 1;
                encode_physical_key(layout, Namespace::Default, key, Some(start_ts), encoded);
                builder.push(encoded, profile.payload_bytes + 2);
            }
        }
        Namespace::Lock => {
            if profile.lock_stride != 0 && row_index.is_multiple_of(profile.lock_stride) {
                encode_physical_key(layout, Namespace::Lock, key, None, encoded);
                builder.push(encoded, BENCHMARK_LOCK_VALUE_BYTES);
            }
        }
        _ => unreachable!("benchmark fixture emits only row MVCC families"),
    }
}

fn build_physical_files(profile: Profile, keys: &[Vec<u8>], layout: Layout) -> PhysicalFiles {
    let build_started = Instant::now();
    let mut encoded = Vec::with_capacity(64);
    let files = match layout {
        Layout::UnifiedRowFirst => {
            let mut builder = BlockBuilder::default();
            for (row_index, key) in keys.iter().enumerate() {
                for family in [Namespace::Write, Namespace::Default, Namespace::Lock] {
                    emit_row_records(
                        profile,
                        row_index,
                        key,
                        layout,
                        family,
                        &mut builder,
                        &mut encoded,
                    );
                }
            }
            [Some(builder.finish()), None, None]
        }
        Layout::SeparateFamilies => {
            let mut default_builder = BlockBuilder::default();
            let mut write_builder = BlockBuilder::default();
            let mut lock_builder = BlockBuilder::default();
            for (row_index, key) in keys.iter().enumerate() {
                emit_row_records(
                    profile,
                    row_index,
                    key,
                    layout,
                    Namespace::Default,
                    &mut default_builder,
                    &mut encoded,
                );
                emit_row_records(
                    profile,
                    row_index,
                    key,
                    layout,
                    Namespace::Write,
                    &mut write_builder,
                    &mut encoded,
                );
                emit_row_records(
                    profile,
                    row_index,
                    key,
                    layout,
                    Namespace::Lock,
                    &mut lock_builder,
                    &mut encoded,
                );
            }
            let lock_file = lock_builder.finish();
            [
                Some(default_builder.finish()),
                Some(write_builder.finish()),
                (lock_file.records > 0).then_some(lock_file),
            ]
        }
        Layout::UnifiedFamilyFirst => {
            let mut builder = BlockBuilder::default();
            for family in [Namespace::Default, Namespace::Write, Namespace::Lock] {
                for (row_index, key) in keys.iter().enumerate() {
                    emit_row_records(
                        profile,
                        row_index,
                        key,
                        layout,
                        family,
                        &mut builder,
                        &mut encoded,
                    );
                }
            }
            [Some(builder.finish()), None, None]
        }
    };
    let files = PhysicalFiles { layout, files };
    eprintln!(
        "BLOCK_BUILD profile={} layout={} rows={} records={} blocks={} build_ms={:.1}",
        profile.name,
        layout_name(layout),
        profile.rows,
        files.total_records(),
        files.total_blocks(),
        build_started.elapsed().as_secs_f64() * 1_000.0,
    );
    files
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct CacheKey {
    file: usize,
    block: usize,
}

#[derive(Default)]
/// Disk-read accounting for one cold or warm operation trace.
struct TraceStats {
    block_touches: u64,
    cache_hits: u64,
    cache_misses: u64,
    bytes_read: u64,
}

/// A bounded LRU block cache shared across the candidate's family files.
struct BlockCache {
    capacity_bytes: usize,
    used_bytes: usize,
    lru: VecDeque<(CacheKey, usize)>,
    sizes: HashMap<CacheKey, usize>,
}

impl BlockCache {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            used_bytes: 0,
            lru: VecDeque::new(),
            sizes: HashMap::new(),
        }
    }

    fn access(&mut self, key: CacheKey, size: usize, stats: &mut TraceStats) {
        stats.block_touches += 1;
        if let Some(cached_size) = self.sizes.get(&key).copied() {
            stats.cache_hits += 1;
            if let Some(position) = self.lru.iter().position(|(candidate, _)| *candidate == key) {
                self.lru.remove(position);
            }
            self.lru.push_back((key, cached_size));
            return;
        }

        stats.cache_misses += 1;
        stats.bytes_read += size as u64;
        if size > self.capacity_bytes {
            return;
        }
        while self.used_bytes + size > self.capacity_bytes {
            let Some((evicted, evicted_size)) = self.lru.pop_front() else {
                break;
            };
            self.sizes.remove(&evicted);
            self.used_bytes -= evicted_size;
        }
        self.used_bytes += size;
        self.sizes.insert(key, size);
        self.lru.push_back((key, size));
    }
}

fn access_point(
    files: &PhysicalFiles,
    cache: &mut BlockCache,
    namespace: Namespace,
    search_key: &[u8],
    stats: &mut TraceStats,
) {
    if let Some((file_index, block_index)) = files.point_block(namespace, search_key) {
        let size = files.files[file_index].as_ref().unwrap().blocks[block_index].bytes;
        cache.access(
            CacheKey {
                file: file_index,
                block: block_index,
            },
            size,
            stats,
        );
    }
}

/// Resolves a fixture row's visible payload timestamp. Physical key seeks are
/// then accounted separately by the block-index model.
fn selected_start_ts(profile: Profile, row_index: usize, read_ts: u64) -> Option<u64> {
    if profile.delete_stride != 0
        && row_index.is_multiple_of(profile.delete_stride)
        && read_ts >= profile.latest_ts()
    {
        return None;
    }
    (0..profile.versions)
        .rev()
        .map(|version| (version as u64 * 2) + 1)
        .find(|start_ts| *start_ts < read_ts)
}

fn trace_point_reads(
    profile: Profile,
    keys: &[Vec<u8>],
    files: &PhysicalFiles,
    read_ts: u64,
    stats: &mut TraceStats,
    cache: &mut BlockCache,
) {
    let mut encoded = Vec::with_capacity(64);
    for probe in 0..997 {
        let row_index = splitmix64(probe as u64 + read_ts) as usize % keys.len();
        encode_physical_key(
            files.layout,
            Namespace::Write,
            &keys[row_index],
            Some(read_ts),
            &mut encoded,
        );
        access_point(files, cache, Namespace::Write, &encoded, stats);
        if let Some(start_ts) = selected_start_ts(profile, row_index, read_ts) {
            encode_physical_key(
                files.layout,
                Namespace::Default,
                &keys[row_index],
                Some(start_ts),
                &mut encoded,
            );
            access_point(files, cache, Namespace::Default, &encoded, stats);
        }
    }
}

fn trace_lock_checks(
    profile: Profile,
    keys: &[Vec<u8>],
    files: &PhysicalFiles,
    stats: &mut TraceStats,
    cache: &mut BlockCache,
) {
    let mut encoded = Vec::with_capacity(64);
    let hit_index = if profile.lock_stride == 0 {
        None
    } else {
        Some(0)
    };
    for index in 0..512 {
        if let Some(hit_index) = hit_index {
            encode_physical_key(
                files.layout,
                Namespace::Lock,
                &keys[hit_index],
                None,
                &mut encoded,
            );
            access_point(files, cache, Namespace::Lock, &encoded, stats);
        }
        let miss = encoded_key(i64::MAX - index as i64 - 1_000);
        encode_physical_key(files.layout, Namespace::Lock, &miss, None, &mut encoded);
        access_point(files, cache, Namespace::Lock, &encoded, stats);
    }
}

fn trace_rollback_and_prewrite(
    profile: Profile,
    keys: &[Vec<u8>],
    files: &PhysicalFiles,
    stats: &mut TraceStats,
    cache: &mut BlockCache,
) {
    let mut encoded = Vec::with_capacity(64);
    let mut lock = Vec::with_capacity(64);
    for index in 0..512 {
        let row_index = if profile.rollback_stride == 0 {
            index
        } else {
            (index * profile.rollback_stride) % keys.len()
        };
        if profile.rollback_stride != 0 {
            encode_physical_key(
                files.layout,
                Namespace::Write,
                &keys[row_index],
                Some(profile.rollback_ts()),
                &mut encoded,
            );
            access_point(files, cache, Namespace::Write, &encoded, stats);
        }

        encode_physical_key(
            files.layout,
            Namespace::Lock,
            &keys[row_index],
            None,
            &mut lock,
        );
        access_point(files, cache, Namespace::Lock, &lock, stats);
        encode_physical_key(
            files.layout,
            Namespace::Write,
            &keys[row_index],
            Some(u64::MAX),
            &mut encoded,
        );
        access_point(files, cache, Namespace::Write, &encoded, stats);
    }
}

/// Simulates one bounded ordered scan and the default-value reads it selects.
fn trace_scan(
    profile: Profile,
    keys: &[Vec<u8>],
    files: &PhysicalFiles,
    read_ts: u64,
    wanted_rows: usize,
    stats: &mut TraceStats,
    cache: &mut BlockCache,
) -> usize {
    let mut selected = Vec::with_capacity(wanted_rows);
    let mut index = keys.len() / 3;
    while index < keys.len() && selected.len() < wanted_rows {
        if selected_start_ts(profile, index, read_ts).is_some() {
            selected.push(index);
        }
        index += 1;
    }
    if selected.is_empty() {
        return 0;
    }

    let mut start = Vec::with_capacity(64);
    let mut end = Vec::with_capacity(64);
    encode_physical_key(
        files.layout,
        Namespace::Write,
        &keys[selected[0]],
        Some(read_ts),
        &mut start,
    );
    if let Some(after_last) = keys.get((index).min(keys.len() - 1)) {
        encode_physical_key(
            files.layout,
            Namespace::Write,
            after_last,
            Some(read_ts),
            &mut end,
        );
    } else {
        end.extend_from_slice(&[0xff; 64]);
    }
    if let Some((file_index, blocks)) = files.scan_blocks(Namespace::Write, &start, &end) {
        for block_index in blocks {
            let size = files.files[file_index].as_ref().unwrap().blocks[block_index].bytes;
            cache.access(
                CacheKey {
                    file: file_index,
                    block: block_index,
                },
                size,
                stats,
            );
        }
    }

    let mut encoded = Vec::with_capacity(64);
    for row_index in selected.iter().copied() {
        let Some(start_ts) = selected_start_ts(profile, row_index, read_ts) else {
            continue;
        };
        encode_physical_key(
            files.layout,
            Namespace::Default,
            &keys[row_index],
            Some(start_ts),
            &mut encoded,
        );
        access_point(files, cache, Namespace::Default, &encoded, stats);
    }
    selected.len()
}

/// Flush and compaction byte totals under the declared benchmark policy.
#[derive(Default)]
struct CompactionStats {
    flushes: usize,
    output_files_per_flush: usize,
    flush_bytes: usize,
    compaction_read_bytes: usize,
    compaction_written_bytes: usize,
}

/// Applies a four-run L0 trigger and ten-to-one level targets. These counters
/// report rewritten bytes, not device latency or production SSTable behavior.
fn add_leveled_compactions(file_bytes: usize, flushes: usize, stats: &mut CompactionStats) {
    if flushes == 0 || file_bytes == 0 {
        return;
    }
    let base_run_bytes = file_bytes / flushes;
    let remainder = file_bytes % flushes;
    let mut l0_count = 0;
    let mut l0_bytes = 0;
    let mut level_bytes = [0_usize; LEVEL_COUNT];
    for flush in 0..flushes {
        l0_count += 1;
        l0_bytes += base_run_bytes + usize::from(flush < remainder);
        if l0_count < L0_TRIGGER_RUNS {
            continue;
        }
        stats.compaction_read_bytes += l0_bytes;
        stats.compaction_written_bytes += l0_bytes;
        level_bytes[1] += l0_bytes;
        l0_count = 0;
        l0_bytes = 0;

        for level in 1..LEVEL_COUNT - 1 {
            let capacity = FLUSH_BYTES
                .saturating_mul(L0_TRIGGER_RUNS)
                .saturating_mul(LEVEL_SIZE_RATIO.saturating_pow(level as u32));
            if level_bytes[level] <= capacity {
                break;
            }
            let compacted = std::mem::take(&mut level_bytes[level]);
            stats.compaction_read_bytes += compacted;
            stats.compaction_written_bytes += compacted;
            level_bytes[level + 1] += compacted;
        }
    }
}

fn compaction_stats(files: &PhysicalFiles) -> CompactionStats {
    let total_bytes = files.total_bytes();
    let flushes = total_bytes.div_ceil(FLUSH_BYTES);
    let output_files_per_flush = files.files.iter().filter(|file| file.is_some()).count();
    let mut stats = CompactionStats {
        flushes,
        output_files_per_flush,
        flush_bytes: total_bytes,
        ..CompactionStats::default()
    };
    for file in files.files.iter().filter_map(Option::as_ref) {
        add_leveled_compactions(file.total_bytes, flushes, &mut stats);
    }
    stats
}

fn logical_ingested_bytes(profile: Profile, keys: &[Vec<u8>]) -> usize {
    keys.iter()
        .enumerate()
        .map(|(row_index, key)| {
            let versioned = profile.versions * (key.len() + profile.payload_bytes + 2)
                + profile.versions * (key.len() + BENCHMARK_WRITE_VALUE_BYTES);
            let delete =
                usize::from(profile.delete_stride != 0 && row_index % profile.delete_stride == 0)
                    * (key.len() + BENCHMARK_WRITE_VALUE_BYTES);
            let rollback = usize::from(
                profile.rollback_stride != 0 && row_index % profile.rollback_stride == 0,
            ) * (key.len() + BENCHMARK_WRITE_VALUE_BYTES);
            let lock =
                usize::from(profile.lock_stride != 0 && row_index % profile.lock_stride == 0)
                    * (key.len() + BENCHMARK_LOCK_VALUE_BYTES);
            versioned + delete + rollback + lock
        })
        .sum()
}

fn emit_physical_profile(profile: Profile, keys: &[Vec<u8>], layout: Layout) {
    let files = build_physical_files(profile, keys, layout);
    let compaction = compaction_stats(&files);
    let data_mib = files.total_bytes() as f64 / (1024.0 * 1024.0);
    let logical_bytes = logical_ingested_bytes(profile, keys);
    let l3_resident_excess = if files.total_bytes() > 24 * 1024 * 1024 {
        files.total_bytes() - 24 * 1024 * 1024
    } else {
        0
    };
    eprintln!(
        "PHYSICAL_SUMMARY profile={} layout={} rows={} versions={} payload={} lock_stride={} rollback_stride={} delete_stride={} keys={} records={} key_bytes={} encoded_mib={:.1} logical_ingest_mib={:.1} blocks={} blocks_4k={} cache_mib={} cache_smaller_than_data={} bytes_over_24mib={} flush_runs={} family_files_per_flush={} flush_mib={:.1} compaction_read_mib={:.1} compaction_written_mib={:.1} compaction_write_amplification={:.3}",
        profile.name,
        layout_name(layout),
        profile.rows,
        profile.versions,
        profile.payload_bytes,
        profile.lock_stride,
        profile.rollback_stride,
        profile.delete_stride,
        profile.rows,
        files.total_records(),
        files.total_key_bytes(),
        data_mib,
        logical_bytes as f64 / (1024.0 * 1024.0),
        files.total_blocks(),
        BLOCK_BYTES,
        CACHE_BYTES / (1024 * 1024),
        files.total_bytes() > CACHE_BYTES,
        l3_resident_excess,
        compaction.flushes,
        compaction.output_files_per_flush,
        compaction.flush_bytes as f64 / (1024.0 * 1024.0),
        compaction.compaction_read_bytes as f64 / (1024.0 * 1024.0),
        compaction.compaction_written_bytes as f64 / (1024.0 * 1024.0),
        compaction.compaction_written_bytes as f64 / logical_bytes.max(1) as f64,
    );

    for (operation, read_ts) in [
        ("point_latest", profile.latest_ts()),
        ("point_middle", profile.middle_ts()),
        ("point_old", profile.old_ts()),
    ] {
        let mut cache = BlockCache::new(CACHE_BYTES);
        let mut stats = TraceStats::default();
        trace_point_reads(profile, keys, &files, read_ts, &mut stats, &mut cache);
        let cold = stats;
        let mut warm = TraceStats::default();
        trace_point_reads(profile, keys, &files, read_ts, &mut warm, &mut cache);
        emit_trace(profile, layout, operation, cold, warm, 997);
    }

    let mut cache = BlockCache::new(CACHE_BYTES);
    let mut cold = TraceStats::default();
    trace_lock_checks(profile, keys, &files, &mut cold, &mut cache);
    let mut warm = TraceStats::default();
    trace_lock_checks(profile, keys, &files, &mut warm, &mut cache);
    emit_trace(profile, layout, "lock_hit_miss", cold, warm, 512);

    let mut cache = BlockCache::new(CACHE_BYTES);
    let mut cold = TraceStats::default();
    trace_rollback_and_prewrite(profile, keys, &files, &mut cold, &mut cache);
    let mut warm = TraceStats::default();
    trace_rollback_and_prewrite(profile, keys, &files, &mut warm, &mut cache);
    emit_trace(profile, layout, "rollback_and_prewrite", cold, warm, 512);

    for scan_rows in [10, 100, 1_000, 10_000] {
        if scan_rows > profile.rows / 2 {
            continue;
        }
        let mut cache = BlockCache::new(CACHE_BYTES);
        let mut cold = TraceStats::default();
        let returned = trace_scan(
            profile,
            keys,
            &files,
            profile.middle_ts(),
            scan_rows,
            &mut cold,
            &mut cache,
        );
        let mut warm = TraceStats::default();
        trace_scan(
            profile,
            keys,
            &files,
            profile.middle_ts(),
            scan_rows,
            &mut warm,
            &mut cache,
        );
        emit_trace(
            profile,
            layout,
            match scan_rows {
                10 => "scan_10_middle",
                100 => "scan_100_middle",
                1_000 => "scan_1000_middle",
                10_000 => "scan_10000_middle",
                _ => unreachable!(),
            },
            cold,
            warm,
            returned,
        );
    }

    let commit_bytes = (0..32)
        .map(|index| {
            let key = encoded_key(i64::MAX - index as i64 - 100);
            let mut encoded = Vec::new();
            encode_physical_key(
                layout,
                Namespace::Default,
                &key,
                Some(profile.latest_ts() + 100 + index as u64 * 2),
                &mut encoded,
            );
            let default_len = encoded.len() + profile.payload_bytes + 2;
            encode_physical_key(
                layout,
                Namespace::Write,
                &key,
                Some(profile.latest_ts() + 101 + index as u64 * 2),
                &mut encoded,
            );
            default_len + encoded.len() + BENCHMARK_WRITE_VALUE_BYTES
        })
        .sum::<usize>();
    eprintln!(
        "PHYSICAL_WRITE profile={} layout={} intent_commit_batch=32 logical_bytes={} rollback_witness_bytes={}",
        profile.name,
        layout_name(layout),
        commit_bytes,
        keys.first()
            .map_or(0, |key| key.len() + BENCHMARK_WRITE_VALUE_BYTES),
    );
}

fn emit_trace(
    profile: Profile,
    layout: Layout,
    operation: &str,
    cold: TraceStats,
    warm: TraceStats,
    rows: usize,
) {
    eprintln!(
        "PHYSICAL_TRACE profile={} layout={} operation={} rows={} cold_blocks={} cold_bytes_read={} cold_cache_hits={} cold_cache_misses={} warm_blocks={} warm_bytes_read={} warm_cache_hits={} warm_cache_misses={}",
        profile.name,
        layout_name(layout),
        operation,
        rows,
        cold.block_touches,
        cold.bytes_read,
        cold.cache_hits,
        cold.cache_misses,
        warm.block_touches,
        warm.bytes_read,
        warm.cache_hits,
        warm.cache_misses,
    );
}

fn bench_stage4_2(criterion: &mut Criterion) {
    eprintln!(
        "STAGE4_2_MODEL block_bytes={} cache_bytes={} flush_bytes={} l0_trigger={} level_ratio={} levels={} namespaces=Default,Write,Lock,TxnPrimaryStatus,RetryOutcome,RetryFloor,SecondaryIndex,UniqueClaim rollback=WriteKind::Rollback",
        BLOCK_BYTES, CACHE_BYTES, FLUSH_BYTES, L0_TRIGGER_RUNS, LEVEL_SIZE_RATIO, LEVEL_COUNT,
    );

    for profile in PROFILES {
        let keys = logical_keys(profile);
        for layout in LAYOUTS {
            emit_physical_profile(profile, &keys, layout);
        }
        if profile.name == "oltp_normal_seq" {
            for layout in LAYOUTS {
                bench_sorted_run_merge(criterion, profile, &keys, layout);
            }
        }
        if profile.rows <= PROFILE_LOGICAL_ROWS {
            for layout in LAYOUTS {
                bench_engine_profile(criterion, profile, layout, &keys);
            }
        }
    }
}

criterion_group!(benches, bench_stage4_2);
criterion_main!(benches);
