#![allow(clippy::unit_arg)]

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use ragnordb_common::{
    codec::{Row, Value, WriteKind},
    command_codec::{
        NoopCommand, SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope, WriteEntry,
    },
    encoding::{decode_row, encode_row},
    ids::{RaftGroupId, ReplicaId, RequestId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_multiraft::{
    proposal::ProposalPosition, storage::persistence::RaftWal, tablet_apply::TabletCommandApplier,
    tablet_cluster::InMemoryTabletCluster,
};
use ragnordb_storage::{
    key::{decode_row_key, encode_row_key, make_row_key},
    mvcc::{InMemoryMvcc, Mutation, MvccStorage},
};
use ragnordb_tablet::{
    Tablet,
    command::TabletStateMachine,
    snapshot::{
        AppliedTabletFrontier, TabletSnapshotConfState, TabletSnapshotMetadata,
        generate_local_snapshot,
    },
};
use std::{
    collections::BTreeMap,
    hint::black_box,
    time::{Duration, Instant},
};
use wal::{
    error::BatchAppendFailure,
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult},
};

const TABLE_ID: TableId = TableId(9);
const TABLET_ID: TabletId = TabletId(41);
const RAFT_GROUP_ID: RaftGroupId = RaftGroupId(91);
const TABLET_EPOCH: u64 = 7;

fn encoded_key(id: i64) -> Vec<u8> {
    let key = make_row_key(TABLE_ID, &[Value::Int(id)]).unwrap();
    encode_row_key(&key).unwrap()
}

fn encoded_row(id: i64, text: String) -> Vec<u8> {
    encode_row(&Row {
        values: vec![Value::Int(id), Value::Text(text)],
    })
    .unwrap()
}

fn build_distinct_mvcc(row_count: u64) -> InMemoryMvcc {
    let mut storage = InMemoryMvcc::new();
    let mut mutations = BTreeMap::new();

    for id in 1..=row_count {
        mutations.insert(
            encoded_key(id as i64),
            Mutation::Put(encoded_row(id as i64, "x".repeat(64))),
        );
    }

    storage
        .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
        .unwrap();

    storage
}

fn build_version_chain(version_count: u64) -> InMemoryMvcc {
    let mut storage = InMemoryMvcc::new();
    let key = encoded_key(1);

    for version in 1..=version_count {
        let start_ts = version * 2 - 1;
        let commit_ts = version * 2;

        let mutations = BTreeMap::from([(
            key.clone(),
            Mutation::Put(encoded_row(1, format!("version-{version}"))),
        )]);

        storage
            .commit_batch(
                TxnId(version),
                Timestamp(start_ts),
                Timestamp(commit_ts),
                &mutations,
            )
            .unwrap();
    }

    storage
}

fn request_id(sequence: u64) -> RequestId {
    RequestId {
        client_id: 41,
        sequence,
        raft_group_id: RAFT_GROUP_ID,
    }
}

fn write_envelope(write_count: usize, sequence: u64) -> TabletCommandEnvelope {
    let writes = (1..=write_count)
        .map(|id| {
            let id = id as i64;
            WriteEntry {
                key: encoded_key(id),
                row: Some(Row {
                    values: vec![Value::Int(id), Value::Text("x".repeat(64))],
                }),
                op: WriteKind::Put,
            }
        })
        .collect();

    let start_ts = 1_000 + sequence * 2;
    let commit_ts = start_ts + 1;

    TabletCommandEnvelope::new(
        request_id(sequence),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: TxnId(1_000 + sequence),
            start_timestamp: Timestamp(start_ts),
            commit_timestamp: Timestamp(commit_ts),
            writes,
        }),
    )
    .unwrap()
}

fn noop_envelope(sequence: u64) -> TabletCommandEnvelope {
    TabletCommandEnvelope::new(
        request_id(sequence),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Noop(NoopCommand),
    )
    .unwrap()
}

fn make_state_machine() -> TabletStateMachine<InMemoryMvcc> {
    let tablet = Tablet::new(TABLET_ID, TABLE_ID).unwrap();
    TabletStateMachine::new(tablet, TABLET_EPOCH, RAFT_GROUP_ID).unwrap()
}

fn make_applier() -> TabletCommandApplier<InMemoryMvcc> {
    TabletCommandApplier::new(make_state_machine())
}

fn bench_common_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("common_encoding");

    for text_bytes in [32_usize, 256, 1_024] {
        group.bench_with_input(
            BenchmarkId::new("row_encode", text_bytes),
            &text_bytes,
            |b, &text_bytes| {
                let row = Row {
                    values: vec![
                        Value::Int(42),
                        Value::Text("x".repeat(text_bytes)),
                        Value::Bool(true),
                    ],
                };

                b.iter(|| black_box(encode_row(black_box(&row)).unwrap()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("row_decode", text_bytes),
            &text_bytes,
            |b, &text_bytes| {
                let encoded = encode_row(&Row {
                    values: vec![
                        Value::Int(42),
                        Value::Text("x".repeat(text_bytes)),
                        Value::Bool(true),
                    ],
                })
                .unwrap();

                b.iter(|| black_box(decode_row(black_box(&encoded)).unwrap()));
            },
        );
    }

    let row_key = make_row_key(TABLE_ID, &[Value::Int(42)]).unwrap();
    let encoded = encode_row_key(&row_key).unwrap();

    group.bench_function("row_key_encode", |b| {
        b.iter(|| black_box(encode_row_key(black_box(&row_key)).unwrap()))
    });

    group.bench_function("row_key_decode", |b| {
        b.iter(|| black_box(decode_row_key(black_box(&encoded)).unwrap()))
    });

    group.finish();
}

fn bench_command_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("command_codec");

    for write_count in [1_usize, 10, 100] {
        group.bench_with_input(
            BenchmarkId::new("encode_preconstructed", write_count),
            &write_count,
            |b, &write_count| {
                let envelope = write_envelope(write_count, 1);
                b.iter(|| black_box(envelope.encode().unwrap()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("decode_preencoded", write_count),
            &write_count,
            |b, &write_count| {
                let bytes = write_envelope(write_count, 1).encode().unwrap();
                b.iter(|| black_box(TabletCommandEnvelope::decode(black_box(&bytes)).unwrap()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("construct_and_encode", write_count),
            &write_count,
            |b, &write_count| {
                b.iter(|| {
                    let envelope = write_envelope(black_box(write_count), 1);
                    black_box(envelope.encode().unwrap())
                });
            },
        );
    }

    group.finish();
}

fn bench_mvcc_point(c: &mut Criterion) {
    let storage = build_version_chain(1_000);
    let key = encoded_key(1);
    let miss_key = encoded_key(2);

    let mut group = c.benchmark_group("mvcc_point");

    group.bench_function("latest_hit", |b| {
        b.iter(|| {
            black_box(
                storage
                    .read(black_box(&key), black_box(Timestamp(2_001)))
                    .unwrap(),
            )
        })
    });

    group.bench_function("historical_hit", |b| {
        b.iter(|| {
            black_box(
                storage
                    .read(black_box(&key), black_box(Timestamp(1_000)))
                    .unwrap(),
            )
        })
    });

    group.bench_function("miss", |b| {
        b.iter(|| {
            black_box(
                storage
                    .read(black_box(&miss_key), black_box(Timestamp(2_001)))
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_mvcc_version_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_version_chain");

    for version_count in [10_u64, 100, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("latest_read", version_count),
            &version_count,
            |b, &version_count| {
                let storage = build_version_chain(version_count);
                let key = encoded_key(1);
                let read_ts = Timestamp(version_count * 2 + 1);

                b.iter(|| black_box(storage.read(black_box(&key), black_box(read_ts)).unwrap()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("oldest_visible_read", version_count),
            &version_count,
            |b, &version_count| {
                let storage = build_version_chain(version_count);
                let key = encoded_key(1);

                b.iter(|| {
                    black_box(
                        storage
                            .read(black_box(&key), black_box(Timestamp(2)))
                            .unwrap(),
                    )
                });
            },
        );
    }

    group.finish();
}

fn bench_mvcc_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_scan");

    for row_count in [1_000_u64, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(row_count),
            &row_count,
            |b, &row_count| {
                let storage = build_distinct_mvcc(row_count);

                b.iter(|| black_box(storage.scan(None, None, black_box(Timestamp(3))).unwrap()));
            },
        );
    }

    group.finish();
}

fn bench_tablet_validation(c: &mut Criterion) {
    let state_machine = make_state_machine();
    let envelope = write_envelope(10, 1);

    let mut group = c.benchmark_group("tablet_validation");

    group.bench_function("valid_single_shard_commit_10_writes", |b| {
        b.iter(|| {
            black_box(
                state_machine
                    .validate_proposal(black_box(&envelope))
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_tablet_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("tablet_apply");

    for write_count in [1_usize, 10, 100] {
        group.bench_with_input(
            BenchmarkId::new("new_single_shard_commit", write_count),
            &write_count,
            |b, &write_count| {
                b.iter_batched(
                    || {
                        let applier = make_applier();
                        let command = write_envelope(write_count, 1).encode().unwrap();
                        (applier, command)
                    },
                    |(mut applier, command)| {
                        black_box(
                            applier
                                .apply_committed(ProposalPosition { term: 1, index: 1 }, &command)
                                .unwrap(),
                        )
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.bench_function("exact_duplicate", |b| {
        b.iter_batched(
            || {
                let mut applier = make_applier();
                let command = write_envelope(1, 1).encode().unwrap();

                applier
                    .apply_committed(ProposalPosition { term: 1, index: 1 }, &command)
                    .unwrap();

                (applier, command)
            },
            |(mut applier, command)| {
                black_box(
                    applier
                        .apply_committed(ProposalPosition { term: 1, index: 2 }, &command)
                        .unwrap(),
                )
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("stale_request_rejection", |b| {
        b.iter_batched(
            || {
                let mut applier = make_applier();
                let first = noop_envelope(1).encode().unwrap();
                let second = noop_envelope(2).encode().unwrap();

                applier
                    .apply_committed(ProposalPosition { term: 1, index: 1 }, &first)
                    .unwrap();

                applier
                    .apply_committed(ProposalPosition { term: 1, index: 2 }, &second)
                    .unwrap();

                (applier, first)
            },
            |(mut applier, stale)| {
                black_box(
                    applier
                        .apply_committed(ProposalPosition { term: 1, index: 3 }, &stale)
                        .unwrap(),
                )
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

struct BenchWal {
    next_lsn: Lsn,
}

impl BenchWal {
    fn new() -> Self {
        Self {
            next_lsn: Lsn::new(100),
        }
    }
}

impl RaftWal for BenchWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        let mut extents = Vec::with_capacity(records.len());

        for (_, payload) in records {
            let start_lsn = self.next_lsn;
            let end_lsn = start_lsn
                .checked_add_bytes(payload.len() as u64 + 32)
                .unwrap();

            self.next_lsn = end_lsn;
            extents.push(AppendResult { start_lsn, end_lsn });
        }

        Ok(BatchAppendResult {
            final_end_lsn: extents
                .last()
                .map(|extent| extent.end_lsn)
                .unwrap_or(Lsn::ZERO),
            record_extents: extents,
        })
    }
}

fn simulated_cluster() -> InMemoryTabletCluster<BenchWal> {
    InMemoryTabletCluster::new(
        [BenchWal::new(), BenchWal::new(), BenchWal::new()],
        TABLET_ID,
        TABLE_ID,
        RAFT_GROUP_ID,
        TABLET_EPOCH,
    )
    .unwrap()
}

fn bench_raft_simulated(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_simulated");

    group.bench_function("simulated_3replica_proposal_to_apply", |b| {
        b.iter_batched(
            || {
                let mut cluster = simulated_cluster();
                cluster.elect_leader().unwrap();

                let request = request_id(1);
                let command = write_envelope(1, 1).encode().unwrap();

                (cluster, request, command)
            },
            |(mut cluster, request, command)| {
                let ticket = cluster
                    .propose(request, command, Instant::now() + Duration::from_secs(30))
                    .unwrap();

                black_box(ticket.recv_timeout(Duration::from_secs(1)).unwrap())
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("simulated_linearizable_read_barrier", |b| {
        b.iter_batched(
            || {
                let mut cluster = simulated_cluster();
                cluster.elect_leader().unwrap();
                cluster
            },
            |mut cluster| black_box(cluster.prepare_leader_for_latest_reads().unwrap()),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn snapshot_fixture(row_count: u64) -> TabletStateMachine<InMemoryMvcc> {
    let storage = build_distinct_mvcc(row_count);
    let tablet = Tablet::with_storage(TABLET_ID, TABLE_ID, storage).unwrap();
    TabletStateMachine::new(tablet, TABLET_EPOCH, RAFT_GROUP_ID).unwrap()
}

fn snapshot_conf_state() -> TabletSnapshotConfState {
    TabletSnapshotConfState::new(
        1,
        [ReplicaId(1), ReplicaId(2), ReplicaId(3)],
        Vec::<ReplicaId>::new(),
        Vec::<ReplicaId>::new(),
    )
    .unwrap()
}

fn bench_snapshot_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_codec");

    for row_count in [1_000_u64, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("generate_image", row_count),
            &row_count,
            |b, &row_count| {
                let state_machine = snapshot_fixture(row_count);
                let conf_state = snapshot_conf_state();

                b.iter(|| {
                    black_box(
                        generate_local_snapshot(
                            &state_machine,
                            "m4-benchmark",
                            ReplicaId(1),
                            1,
                            conf_state.clone(),
                            AppliedTabletFrontier::new(100, 5),
                        )
                        .unwrap(),
                    )
                });
            },
        );
    }

    let state_machine = snapshot_fixture(10_000);
    let image = generate_local_snapshot(
        &state_machine,
        "m4-benchmark",
        ReplicaId(1),
        1,
        snapshot_conf_state(),
        AppliedTabletFrontier::new(100, 5),
    )
    .unwrap();

    let metadata_bytes = image.metadata.encode().unwrap();

    group.bench_function("metadata_encode_10k_rows", |b| {
        b.iter(|| black_box(image.metadata.encode().unwrap()))
    });

    group.bench_function("metadata_decode_10k_rows", |b| {
        b.iter(|| black_box(TabletSnapshotMetadata::decode(black_box(&metadata_bytes)).unwrap()))
    });

    group.bench_function("verify_payload_10k_rows", |b| {
        b.iter(|| {
            black_box(
                image
                    .metadata
                    .verify_payload(black_box(&image.data))
                    .unwrap(),
            )
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_common_encoding,
    bench_command_codec,
    bench_mvcc_point,
    bench_mvcc_version_chain,
    bench_mvcc_scan,
    bench_tablet_validation,
    bench_tablet_apply,
    bench_raft_simulated,
    bench_snapshot_codec,
);
criterion_main!(benches);
