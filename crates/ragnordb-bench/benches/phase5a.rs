#![allow(clippy::unit_arg)]

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use prost::Message;
use ragnordb_common::{
    codec::{Row, Value, WriteKind},
    command_codec::{SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope, WriteEntry},
    ids::{NodeId, RaftGroupId, ReplicaId, RequestId, TableId, TabletId, Timestamp, TxnId},
    proto::rpc,
    rpc_codec::{MessageType, RpcFrame},
};
use ragnordb_multiraft::{
    host::{HostedGroupError, HostedRaftGroup, MultiRaftHost, MultiRaftTurnBudget},
    proposal::ProposalPosition,
    storage::{codec::RaftReplicaIdentity, persistence::RaftWal},
    tablet_apply::TabletCommandApplier,
    tablet_cluster::InMemoryTabletCluster,
};
use ragnordb_storage::{
    key::{encode_primary_key, encode_row_key, make_row_key},
    mvcc::InMemoryMvcc,
};
use ragnordb_tablet::{Tablet, command::TabletStateMachine};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};
use wal::{
    config::WalConfig,
    error::BatchAppendFailure,
    io::directory::FsSegmentDirectory,
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_types},
    wal::{AppendResult, BatchAppendResult, WalHandle},
};

const TABLE_ID: TableId = TableId(9);
const TABLET_ID: TabletId = TabletId(41);
const RAFT_GROUP_ID: RaftGroupId = RaftGroupId(91);
const TABLET_EPOCH: u64 = 7;

fn encoded_key(id: i64) -> Vec<u8> {
    let key = make_row_key(TABLE_ID, &[Value::Int(id)]).unwrap();
    encode_row_key(&key).unwrap()
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

    TabletCommandEnvelope::new(
        request_id(sequence),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: TxnId(1_000 + sequence),
            start_timestamp: Timestamp(1_000 + sequence * 2),
            commit_timestamp: Timestamp(1_001 + sequence * 2),
            writes,
        }),
    )
    .unwrap()
}

fn make_applier() -> TabletCommandApplier<InMemoryMvcc> {
    let tablet = Tablet::new(TABLET_ID, TABLE_ID).unwrap();
    let state_machine = TabletStateMachine::new(tablet, TABLET_EPOCH, RAFT_GROUP_ID).unwrap();
    TabletCommandApplier::new(state_machine)
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

struct IdleHostedGroup {
    identity: RaftReplicaIdentity,
}

impl HostedRaftGroup for IdleHostedGroup {
    fn identity(&self) -> RaftReplicaIdentity {
        self.identity
    }

    fn tick_and_drain(
        &mut self,
        _ticks: u64,
    ) -> Result<Vec<ragnordb_multiraft::host::RaftMessageEnvelope>, HostedGroupError> {
        Ok(Vec::new())
    }

    fn step_and_drain(
        &mut self,
        _message: ragnordb_multiraft::host::RaftMessageEnvelope,
    ) -> Result<Vec<ragnordb_multiraft::host::RaftMessageEnvelope>, HostedGroupError> {
        Ok(Vec::new())
    }

    fn propose_and_drain(
        &mut self,
        _command: Vec<u8>,
        _encoded_len: usize,
    ) -> Result<(u64, Vec<ragnordb_multiraft::host::RaftMessageEnvelope>), HostedGroupError> {
        Ok((0, Vec::new()))
    }
}

fn host_with_groups(group_count: u64) -> MultiRaftHost<BenchWal> {
    let mut host = MultiRaftHost::new(
        NodeId(1),
        ragnordb_multiraft::storage::persistence::NodeRaftWal::new(BenchWal::new()),
    );

    for group_number in 1..=group_count {
        let identity = RaftReplicaIdentity::new(RaftGroupId(group_number), ReplicaId(1)).unwrap();
        host.issue_group_writer(identity).unwrap();
        host.register_new_group(Box::new(IdleHostedGroup { identity }))
            .unwrap();
    }

    host.activate().unwrap();
    host.schedule_all_groups_after(1).unwrap();
    host
}

fn bench_multiraft_density(c: &mut Criterion) {
    let mut group = c.benchmark_group("multiraft_density");

    for group_count in [1_u64, 100, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("scheduler_turn", group_count),
            &group_count,
            |b, &group_count| {
                let mut host = host_with_groups(group_count);
                let budget = MultiRaftTurnBudget {
                    max_groups: group_count as usize,
                    ..MultiRaftTurnBudget::default()
                };

                b.iter(|| {
                    host.schedule_due_ticks(1).unwrap();
                    let result = host.run_turn(0, budget).unwrap();
                    black_box((result.groups_serviced, result.ready_generations));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("status_snapshot", group_count),
            &group_count,
            |b, &group_count| {
                let host = host_with_groups(group_count);
                b.iter(|| black_box(host.status()));
            },
        );
    }

    group.finish();
}

fn bench_persist_vs_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("persist_vs_apply");

    for write_count in [1_usize, 10, 100] {
        group.bench_with_input(
            BenchmarkId::new("logical_wal_append", write_count),
            &write_count,
            |b, &write_count| {
                let envelope = write_envelope(write_count, 1).encode().unwrap();
                let mut wal = BenchWal::new();
                let record_type = RecordType::new(record_types::USER_MIN + 1);
                b.iter(|| {
                    let records = [(record_type, envelope.as_slice())];
                    black_box(wal.append_batch_and_sync(&records).unwrap())
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("tablet_apply", write_count),
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

    group.finish();
}

fn bench_replication_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("replication_pipeline");
    group.bench_function("three_replica_proposal_to_apply", |b| {
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
    group.finish();
}

fn bench_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("routing");
    let router =
        ragnordb_tablet::router::TabletRouter::for_single_tablet(TABLE_ID, TABLET_ID).unwrap();
    let key = encode_primary_key(&[Value::Int(42)]).unwrap();

    group.bench_function("point_route", |b| {
        b.iter(|| black_box(router.route_point(black_box(&key)).unwrap()))
    });
    group.bench_function("full_scan_route", |b| {
        b.iter(|| black_box(router.route_scan()))
    });
    group.finish();
}

fn bench_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    let directory = tempfile::tempdir().unwrap();
    let config = WalConfig {
        dir: directory.path().to_path_buf(),
        identity: WalIdentity::new(5, 1, 1),
        ..WalConfig::default()
    };
    let wal = WalHandle::open(
        FsSegmentDirectory::new(directory.path().to_path_buf()),
        config,
        (),
    )
    .unwrap()
    .0;
    let payload = vec![0x5a_u8; 256];
    let record_type = RecordType::new(record_types::USER_MIN);

    group.bench_function("fs_append_and_sync_256_bytes", |b| {
        b.iter(|| {
            black_box(
                wal.append_and_sync(record_type, black_box(&payload))
                    .unwrap(),
            )
        })
    });
    group.finish();
}

fn bench_transport_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("transport_hol");

    for payload_bytes in [64_usize, 1_024, 16_384] {
        group.bench_with_input(
            BenchmarkId::new("rpc_frame_encode_decode", payload_bytes),
            &payload_bytes,
            |b, &payload_bytes| {
                let frame = RpcFrame {
                    msg_type: MessageType::TabletReadRequest,
                    raft_group_id: RAFT_GROUP_ID,
                    payload: vec![0xa5; payload_bytes],
                };

                b.iter(|| {
                    let encoded = frame.to_proto().encode_to_vec();
                    let decoded = rpc::RpcFrame::decode(encoded.as_slice()).unwrap();
                    black_box(RpcFrame::from_proto(decoded).unwrap())
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_multiraft_density,
    bench_persist_vs_apply,
    bench_replication_pipeline,
    bench_routing,
    bench_wal,
    bench_transport_codec,
);
criterion_main!(benches);
