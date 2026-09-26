#![allow(clippy::unit_arg)]

//! Criterion decomposition suite for Milestone 6 transaction performance.
//!
//! These benchmarks isolate CPU-side ceilings and planning/encoding costs. The
//! live runner records consensus, RPC, recovery, and contention behavior against
//! a real cluster; this target keeps those measurements separate from the
//! client/network harness so future baselines remain comparable.

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use ragnordb_common::{
    Result,
    codec::{Row, Value, WriteKind},
    command_codec::{SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope, WriteEntry},
    ids::{RaftGroupId, TabletId, Timestamp, TxnId},
};
use ragnordb_txn::{ConcurrentTimestampOracle, TimestampReservation, TimestampReservationProvider};
use std::hint::black_box;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    thread,
};

#[derive(Default)]
struct BenchmarkReservationProvider {
    frontier: u64,
}

impl TimestampReservationProvider for BenchmarkReservationProvider {
    fn reserve_timestamps(&mut self, requested_until: Timestamp) -> Result<TimestampReservation> {
        let reserved_from = Timestamp(self.frontier.saturating_add(1));
        if requested_until <= Timestamp(self.frontier) || reserved_from > requested_until {
            return Err(ragnordb_common::Error::InvalidArgument(
                "benchmark reservation frontier regressed".to_string(),
            ));
        }
        self.frontier = requested_until.0;
        Ok(TimestampReservation {
            reserved_from,
            reserved_until: requested_until,
        })
    }
}

fn allocate_in_parallel(
    oracle: &ConcurrentTimestampOracle<BenchmarkReservationProvider>,
    workers: usize,
    allocations_per_worker: usize,
) -> u64 {
    thread::scope(|scope| {
        let handles = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    for _ in 0..allocations_per_worker {
                        black_box(oracle.allocate_timestamp().unwrap());
                    }
                    allocations_per_worker as u64
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("timestamp benchmark worker panicked"))
            .sum()
    })
}

fn bench_timestamp_oracle(c: &mut Criterion) {
    let mut group = c.benchmark_group("m6_timestamp_oracle");
    for workers in [1_usize, 2, 4, 8, 16, 32, 64] {
        group.bench_with_input(
            BenchmarkId::new("fast_path_allocations", workers),
            &workers,
            |b, &workers| {
                b.iter_batched(
                    || {
                        Arc::new(
                            ConcurrentTimestampOracle::new(
                                BenchmarkReservationProvider::default(),
                                65_536,
                                16_384,
                            )
                            .unwrap(),
                        )
                    },
                    |oracle| black_box(allocate_in_parallel(&oracle, workers, 1_024)),
                    BatchSize::SmallInput,
                );
            },
        );
    }

    for (reservation_size, prefetch_threshold) in [
        (256_u64, 64_u64),
        (1_024, 256),
        (4_096, 1_024),
        (16_384, 4_096),
        (65_536, 16_384),
    ] {
        group.bench_with_input(
            BenchmarkId::new("reservation_boundary", reservation_size),
            &(reservation_size, prefetch_threshold),
            |b, &(reservation_size, prefetch_threshold)| {
                b.iter_batched(
                    || {
                        ConcurrentTimestampOracle::new(
                            BenchmarkReservationProvider::default(),
                            reservation_size,
                            prefetch_threshold,
                        )
                        .unwrap()
                    },
                    |oracle| {
                        for _ in 0..(reservation_size as usize * 2) {
                            black_box(oracle.allocate_timestamp().unwrap());
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_gc_protection(c: &mut Criterion) {
    let mut group = c.benchmark_group("m6_gc_protection");
    for active_transactions in [1_usize, 10, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("aggregate_tracker", active_transactions),
            &active_transactions,
            |b, &active_transactions| {
                b.iter(|| {
                    let tracker = Mutex::new(BTreeMap::<u64, usize>::new());
                    let mut state = tracker.lock().unwrap();
                    for transaction_id in 0..active_transactions as u64 {
                        *state.entry(transaction_id).or_default() += 1;
                    }
                    black_box(state.iter().next().map(|(timestamp, _)| *timestamp));
                });
            },
        );
    }
    group.finish();
}

fn commit_envelope(write_count: usize) -> TabletCommandEnvelope {
    let writes = (1..=write_count)
        .map(|id| WriteEntry {
            key: format!("key-{id}").into_bytes(),
            row: Some(Row {
                values: vec![Value::Int(id as i64), Value::Text("x".to_string())],
            }),
            op: WriteKind::Put,
        })
        .collect();
    TabletCommandEnvelope::new(
        ragnordb_common::ids::RequestId {
            client_id: 7,
            sequence: 1,
            raft_group_id: RaftGroupId(2),
        },
        TabletId(3),
        1,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: TxnId(9),
            start_timestamp: Timestamp(10),
            commit_timestamp: Timestamp(11),
            writes,
        }),
    )
    .unwrap()
}

fn bench_single_shard_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("m6_single_shard_commit");
    for write_count in [1_usize, 4, 16, 64] {
        group.bench_with_input(
            BenchmarkId::new("command_encode", write_count),
            &write_count,
            |b, &write_count| {
                let envelope = commit_envelope(write_count);
                b.iter(|| black_box(envelope.encode().unwrap()));
            },
        );
    }
    group.finish();
}

fn bench_participant_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("m6_cross_tablet_participants");
    for participants in [1_usize, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("parallel_dispatch_bookkeeping", participants),
            &participants,
            |b, &participants| {
                b.iter(|| {
                    thread::scope(|scope| {
                        let handles = (0..participants)
                            .map(|participant| scope.spawn(move || black_box(participant * 2 + 1)))
                            .collect::<Vec<_>>();
                        black_box(
                            handles
                                .into_iter()
                                .map(|handle| handle.join().unwrap())
                                .collect::<Vec<_>>(),
                        );
                    });
                });
            },
        );
    }
    group.finish();
}

fn bench_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("m6_contention");
    for clients in [1_usize, 2, 8, 32, 64] {
        group.bench_with_input(
            BenchmarkId::new("hotspot_atomic_increment", clients),
            &clients,
            |b, &clients| {
                b.iter_batched(
                    || Arc::new(std::sync::atomic::AtomicU64::new(0)),
                    |counter| {
                        thread::scope(|scope| {
                            let handles = (0..clients)
                                .map(|_| {
                                    let counter = Arc::clone(&counter);
                                    scope.spawn(move || {
                                        for _ in 0..128 {
                                            counter
                                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        }
                                    })
                                })
                                .collect::<Vec<_>>();
                            for handle in handles {
                                handle.join().unwrap();
                            }
                        });
                        black_box(counter.load(std::sync::atomic::Ordering::Relaxed));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_recovery_evidence(c: &mut Criterion) {
    let mut group = c.benchmark_group("m6_recovery");
    let evidence = (0..256_u64)
        .map(|command_id| (command_id, command_id % 3 == 0))
        .collect::<Vec<_>>();
    group.bench_function("scan_durable_outcome_evidence", |b| {
        b.iter(|| black_box(evidence.iter().filter(|(_, applied)| *applied).count()))
    });
    group.finish();
}

criterion_group!(
    milestone6,
    bench_timestamp_oracle,
    bench_gc_protection,
    bench_single_shard_commit,
    bench_participant_scaling,
    bench_contention,
    bench_recovery_evidence
);
criterion_main!(milestone6);
