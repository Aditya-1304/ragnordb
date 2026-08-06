//! deterministic three node runtime for one replicated tablet group
//!
//! this runtime owns one Raft replica per node, drives the existing Ready
//! persistence boundary, transfers outbound messages through an in-memory
//! transport, and applies committed tablet commands on every replica

use std::{
    collections::VecDeque,
    sync::mpsc::RecvTimeoutError,
    time::{Duration, Instant},
};

use raft::{
    core::node::RaftNode,
    entry::EntryPayload,
    message::Envelope,
    storage::mem::MemStorage,
    types::{LogIndex, Role},
};

use ragnordb_common::{
    command_codec::{
        NoopCommand, TabletCommand, TabletCommandEnvelope, TabletCommandEnvelopeError,
    },
    ids::{RaftGroupId, ReplicaId, RequestId, RowKey, TableId, TabletId},
};

use ragnordb_txn::Transaction;
use wal::lsn::Lsn;

use ragnordb_tablet::{
    Tablet,
    command::{TabletCommandApplyError, TabletCommandApplyResult, TabletStateMachine},
    read::{LeaderReadGate, LeaderReadGateError, ReadBarrierPosition},
    snapshot::{
        AppliedTabletFrontier, FileTabletSnapshotStore, TabletSnapshotConfState,
        TabletSnapshotImage, TabletSnapshotInstallTarget,
    },
};

use crate::{
    proposal::{
        ProposalCompletion, ProposalFailure, ProposalRegistry, ProposalRegistryError,
        ProposalTicket,
    },
    runtime::{AppliedRaftFrontier, RaftReadyLoop, ReadyLoopError},
    snapshot::{
        SnapshotWorkController, TabletSnapshotTransfer, generate_tablet_snapshot_from_ready_loop,
        install_incoming_tablet_snapshot, persist_tablet_snapshot_boundary,
        raft_pointer_for_tablet,
    },
    storage::{
        codec::RaftReplicaIdentity,
        persistence::{RaftWal, RaftWalStorage},
    },
    tablet_apply::{CommittedTabletCommandDisposition, TabletApplyError, TabletCommandApplier},
};

const REPLICA_IDS: [u64; 3] = [1, 2, 3];
const ELECTION_TIMEOUT: u64 = 5;
const HEARTBEAT_INTERVAL: u64 = 2;
const INTERNAL_READ_BARRIER_CLIENT_ID: u128 = 1_u128 << 127;

fn tablet_conf_state(
    conf_state: &raft::types::ConfState,
) -> Result<TabletSnapshotConfState, TabletClusterError> {
    TabletSnapshotConfState::new(
        conf_state.version,
        conf_state
            .voters
            .iter()
            .map(|replica_id| ReplicaId(replica_id.get())),
        conf_state
            .learners
            .iter()
            .map(|replica_id| ReplicaId(replica_id.get())),
        conf_state
            .outgoing_voters
            .iter()
            .map(|replica_id| ReplicaId(replica_id.get())),
    )
    .map_err(|error| TabletClusterError::Snapshot(error.to_string()))
}

type CoreRaftNode =
    RaftNode<Vec<u8>, Vec<u8>, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

/// public Ready loop type used when a deterministic tablet replica is
/// reconstructed from acknowledged durable Raft state
pub type TabletRaftReadyLoop<W> =
    RaftReadyLoop<W, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

type ReplicaReadyLoop<W> = TabletRaftReadyLoop<W>;

struct TabletReplica<W>
where
    W: RaftWal,
{
    node_id: u64,
    raft: ReplicaReadyLoop<W>,
    tablet: TabletCommandApplier,
    available: bool,
}

/// errors raised by the deterministic tablet cluster runtime
#[derive(Debug, thiserror::Error)]
pub enum TabletClusterError {
    #[error("invalid tablet cluster configuration: {0}")]
    Configuration(String),

    #[error("no tablet leader is currently available")]
    NoLeader,

    #[error("multiple tablet leaders are visible: {leaders:?}")]
    MultipleLeaders { leaders: Vec<u64> },

    #[error("replica {replica_id} is not part of the tablet cluster")]
    UnknownReplica { replica_id: u64 },

    #[error("replica {node_id} Ready processing failed: {source}")]
    Ready {
        node_id: u64,
        source: Box<ReadyLoopError>,
    },

    #[error("replica {node_id} rejected a proposal before Raft admission: {source}")]
    ProposalValidation {
        node_id: u64,
        source: TabletCommandApplyError,
    },

    #[error("replica {node_id} proposal failed: {source}")]
    Proposal {
        node_id: u64,
        source: Box<ReadyLoopError>,
    },

    #[error("replica {node_id} failed to apply entry {index}: {source}")]
    Apply {
        node_id: u64,
        index: LogIndex,
        source: TabletApplyError,
    },

    #[error("committed tablet command envelope is invalid: {0}")]
    Envelope(#[from] TabletCommandEnvelopeError),

    #[error("proposal RequestId does not match the command envelope RequestId")]
    RequestIdentityMismatch,

    #[error("proposal registry failed: {0}")]
    Registry(#[from] ProposalRegistryError),

    #[error("replica {replica_id} is unavailable")]
    ReplicaUnavailable { replica_id: u64 },

    #[error("proposal {request_id:?} expired before Raft admission")]
    ProposalDeadlineExceeded { request_id: RequestId },

    #[error("leader read barrier state error: {0}")]
    ReadBarrier(#[from] LeaderReadGateError),

    #[error("leader read barrier response timed out")]
    ReadBarrierTimeout,

    #[error("leader read barrier response channel closed")]
    ReadBarrierChannelClosed,

    #[error("leader read barrier became retryable: {failure:?}")]
    ReadBarrierRetryable { failure: ProposalFailure },

    #[error("leader read barrier was deterministically rejected: {rejection}")]
    ReadBarrierRejected { rejection: TabletCommandApplyError },

    #[error("leader read barrier applied an unexpected tablet result")]
    UnexpectedReadBarrierResult,

    #[error("latest read deadline expired before its barrier was applied")]
    LatestReadDeadlineExceeded,

    #[error(
        "latest read lost leadership: expected replica {expected_leader} term \
     {expected_term}, observed replica {observed_leader} term {observed_term}"
    )]
    LatestReadLeadershipLost {
        expected_leader: u64,
        expected_term: u64,
        observed_leader: u64,
        observed_term: u64,
    },

    #[error("latest tablet read failed: {0}")]
    LatestRead(#[from] ragnordb_common::Error),

    #[error("tablet snapshot operation failed: {0}")]
    Snapshot(String),
}

/// one in memory three-node Raft group owning one tablet
pub struct InMemoryTabletCluster<W: RaftWal> {
    replicas: [TabletReplica<W>; 3],
    transport: VecDeque<Envelope<Vec<u8>, Vec<u8>>>,
    proposals: ProposalRegistry<
        ragnordb_tablet::command::TabletCommandApplyOutcome,
        TabletCommandApplyError,
    >,
    raft_group_id: RaftGroupId,
    tablet_id: TabletId,
    tablet_epoch: u64,
    read_gate: LeaderReadGate,
    next_read_barrier_sequence: u64,
    latest_snapshot: Option<TabletSnapshotImage>,
}

impl<W: RaftWal> InMemoryTabletCluster<W> {
    /// construct three replicas with one tablet state machine each
    ///
    /// the constructor persists every initial bootstrap Ready before the
    /// cluster can begin elections or message processing
    pub fn new(
        wals: [W; 3],
        tablet_id: TabletId,
        table_id: TableId,
        raft_group_id: RaftGroupId,
        tablet_epoch: u64,
    ) -> Result<Self, TabletClusterError> {
        let replicas = wals
            .into_iter()
            .enumerate()
            .map(|(index, wal)| {
                let node_id = REPLICA_IDS[index];

                let identity = RaftReplicaIdentity::new(raft_group_id, ReplicaId(node_id))
                    .map_err(|error| TabletClusterError::Configuration(error.to_string()))?;

                let peers = REPLICA_IDS
                    .into_iter()
                    .filter(|peer| *peer != node_id)
                    .collect::<Vec<_>>();

                let raft_node: CoreRaftNode = RaftNode::new(
                    node_id,
                    peers,
                    MemStorage::new(),
                    MemStorage::new(),
                    ELECTION_TIMEOUT,
                    HEARTBEAT_INTERVAL,
                );

                let tablet = Tablet::new(tablet_id, table_id)
                    .map_err(|error| TabletClusterError::Configuration(error.to_string()))?;

                let state_machine = TabletStateMachine::new(tablet, tablet_epoch, raft_group_id)
                    .map_err(|error| TabletClusterError::Configuration(error.to_string()))?;

                Ok(TabletReplica {
                    node_id,
                    available: true,
                    raft: RaftReadyLoop::new(raft_node, RaftWalStorage::new(wal, identity)),
                    tablet: TabletCommandApplier::new(state_machine),
                })
            })
            .collect::<Result<Vec<_>, TabletClusterError>>()?;

        let replicas: [TabletReplica<W>; 3] = replicas.try_into().map_err(|_| {
            TabletClusterError::Configuration(
                "three replica construction returned the wrong count".to_string(),
            )
        })?;

        let mut cluster = Self {
            replicas,
            transport: VecDeque::new(),
            proposals: ProposalRegistry::new(),
            raft_group_id,
            tablet_id,
            tablet_epoch,
            read_gate: LeaderReadGate::new(),
            next_read_barrier_sequence: 1,
            latest_snapshot: None,
        };

        for replica_index in 0..REPLICA_IDS.len() {
            cluster.drain_ready(replica_index)?;
        }

        Ok(cluster)
    }

    /// remove one replica from the in-memory transport and election path
    ///
    /// this models a crashed process without deleting its Raft or tablet
    /// state, allowing the replica to rejoin and catch up later
    pub fn kill_replica(&mut self, node_id: u64) -> Result<(), TabletClusterError> {
        let replica_index = self.replica_index(node_id)?;
        let was_leader = self.replicas[replica_index].available
            && matches!(
                self.replicas[replica_index].raft.raft().role(),
                Role::Leader
            );
        let observed_term = self.replicas[replica_index].raft.raft().current_term();

        self.replicas[replica_index].available = false;

        if was_leader {
            self.proposals.mark_leadership_lost(observed_term);
        }

        Ok(())
    }

    /// reattach a previously killed replica to the in memory transport
    pub fn restart_replica(&mut self, node_id: u64) -> Result<(), TabletClusterError> {
        let replica_index = self.replica_index(node_id)?;
        self.replicas[replica_index].available = true;
        Ok(())
    }

    /// Return a clone of the replica's WAL handle for startup recovery tests or
    /// host-controlled restart orchestration.
    ///
    /// The returned WAL must be recovered before constructing a replacement
    /// `RaftReadyLoop`. This method intentionally does not expose mutable storage
    /// internals or create a second persistence path.
    pub fn replica_wal(&self, node_id: u64) -> Result<W, TabletClusterError>
    where
        W: Clone,
    {
        let replica_index = self.replica_index(node_id)?;

        Ok(self.replicas[replica_index]
            .raft
            .persistence()
            .wal()
            .clone())
    }

    /// generate and publish the leader's current applied tablet image
    ///
    /// The file is published before its identity-bound Raft pointer and
    /// HardState are appended to A-WAL. Only after that durable boundary is
    /// complete does the core compact its logical log and advertise the
    /// snapshot to lagging followers
    pub fn publish_tablet_snapshot(
        &mut self,
        leader_id: u64,
        cluster_id: impl Into<String>,
        store: &FileTabletSnapshotStore,
        work: &SnapshotWorkController,
        snapshot_id: u64,
    ) -> Result<(), TabletClusterError> {
        let leader_index = self.leader_index()?;
        if self.replicas[leader_index].node_id != leader_id {
            return Err(TabletClusterError::Configuration(
                "tablet snapshots must be generated by the current leader".to_string(),
            ));
        }

        let frontier = self.replicas[leader_index]
            .raft
            .applied_frontier()
            .ok_or_else(|| {
                TabletClusterError::Snapshot(
                    "leader has no acknowledged applied frontier".to_string(),
                )
            })?;

        let conf_state = tablet_conf_state(self.replicas[leader_index].raft.raft().conf_state())?;
        let image = generate_tablet_snapshot_from_ready_loop(
            work,
            &self.replicas[leader_index].raft,
            self.replicas[leader_index].tablet.state_machine(),
            cluster_id,
            ReplicaId(leader_id),
            snapshot_id,
            conf_state,
        )
        .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;

        let pointer = store
            .publish(&image)
            .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;
        let identity = self.replicas[leader_index]
            .raft
            .persistence()
            .log_view()
            .identity();
        let raft_pointer = raft_pointer_for_tablet(identity, &pointer)
            .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;
        let hard_state = self.replicas[leader_index].raft.raft().hard_state();

        persist_tablet_snapshot_boundary(
            self.replicas[leader_index].raft.persistence_mut(),
            &pointer,
            AppliedTabletFrontier::new(frontier.index, frontier.term),
            hard_state,
        )
        .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;

        let core_snapshot = TabletSnapshotTransfer::from_image(image.clone())
            .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?
            .into_core_snapshot();

        self.replicas[leader_index]
            .raft
            .restore_persisted_snapshot(&raft_pointer, core_snapshot)
            .map_err(|error| TabletClusterError::Ready {
                node_id: leader_id,
                source: Box::new(error),
            })?;

        // a locally restored snapshot is already persisted before the core is
        // updated. Drain any ordinary Ready state produced by that transition
        // through the normal path; the core's `restore_snapshot` API does not
        // create an incoming snapshot-install Ready generation
        let ready = self.replicas[leader_index]
            .raft
            .persist_next_ready(None)
            .map_err(|source| TabletClusterError::Ready {
                node_id: leader_id,
                source: Box::new(source),
            })?
            .ok_or_else(|| {
                TabletClusterError::Snapshot(
                    "published tablet snapshot did not produce a Ready generation".to_string(),
                )
            })?;

        for entry in &ready.committed_entries {
            if let EntryPayload::Normal(command) = &entry.payload {
                let position = crate::proposal::ProposalPosition {
                    term: entry.term,
                    index: entry.index,
                };
                let disposition = self.replicas[leader_index]
                    .tablet
                    .apply_committed(position, command)
                    .map_err(|source| TabletClusterError::Apply {
                        node_id: leader_id,
                        index: entry.index,
                        source,
                    })?;
                self.resolve_committed(disposition)?;
            }
        }

        self.transport.extend(ready.messages);
        self.deliver_messages()?;
        self.release_replica_retention(leader_index)?;

        self.latest_snapshot = Some(image);
        Ok(())
    }

    /// transfer the latest leader snapshot to a rejoined follower through the
    /// Raft metadata control event, bounded tablet chunks, verified restore,
    /// and the follower's exact post-snapshot Ready persistence boundary
    pub fn catch_up_replica_with_snapshot(
        &mut self,
        leader_id: u64,
        follower_id: u64,
        store: &FileTabletSnapshotStore,
        work: &SnapshotWorkController,
        max_chunk_bytes: u64,
    ) -> Result<(), TabletClusterError> {
        let leader_index = self.replica_index(leader_id)?;
        let follower_index = self.replica_index(follower_id)?;

        if !self.replicas[leader_index].available {
            return Err(TabletClusterError::ReplicaUnavailable {
                replica_id: leader_id,
            });
        }
        if !self.replicas[follower_index].available {
            return Err(TabletClusterError::ReplicaUnavailable {
                replica_id: follower_id,
            });
        }

        let source_image = self.latest_snapshot.clone().ok_or_else(|| {
            TabletClusterError::Snapshot("no leader snapshot has been published".to_string())
        })?;

        // retain the WAL prefixes owned by both participants until the
        // external image has been received, restored, and acknowledged by the
        // follower's Ready loop. The boundary helper takes a shorter nested
        // pin for its append; these host-level pins cover the whole transfer
        let leader_retention_floor = self.replicas[leader_index]
            .raft
            .persistence()
            .log_view()
            .first_retained_lsn()
            .unwrap_or(Lsn::ZERO);
        let _leader_retention_pin = self.replicas[leader_index]
            .raft
            .persistence()
            .acquire_retention_pin("tablet-snapshot-send", leader_retention_floor)
            .map_err(TabletClusterError::Snapshot)?;

        let follower_retention_floor = self.replicas[follower_index]
            .raft
            .persistence()
            .log_view()
            .first_retained_lsn()
            .unwrap_or(Lsn::ZERO);
        let _follower_retention_pin = self.replicas[follower_index]
            .raft
            .persistence()
            .acquire_retention_pin("tablet-snapshot-install", follower_retention_floor)
            .map_err(TabletClusterError::Snapshot)?;

        // the database image is copied into the receiving replica's identity
        // namespace before publication. The payload remains byte identical;
        // only the durable file/pointer ownership changes on the target
        let mut target_metadata = source_image.metadata.clone();
        target_metadata.replica_id = ReplicaId(follower_id);
        let target_image = TabletSnapshotImage::new(target_metadata, source_image.data.clone())
            .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;
        let _target_pointer = store
            .publish(&target_image)
            .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;

        // the leader's next heartbeat publishes the already durable snapshot
        // metadata. The follower's Ready control event is consumed by the
        // normal transport path and leaves the core waiting for host data
        self.tick_replica(leader_id, HEARTBEAT_INTERVAL)?;

        let mut sender = TabletSnapshotTransfer::from_image(target_image.clone())
            .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?
            .into_sender(work, max_chunk_bytes)
            .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;
        let mut receiver = crate::snapshot::TabletSnapshotReceiveSession::begin(
            work,
            store,
            target_image.metadata.clone(),
            max_chunk_bytes,
        )
        .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;

        while let Some(chunk) = sender.next_chunk() {
            receiver
                .push_chunk(&chunk)
                .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;
        }

        let mut hard_state = self.replicas[follower_index].raft.raft().hard_state();
        hard_state.commit = hard_state
            .commit
            .max(target_image.metadata.last_included_index);

        let target = TabletSnapshotInstallTarget {
            cluster_id: target_image.metadata.cluster_id.clone(),
            raft_group_id: self.raft_group_id,
            tablet_id: self.tablet_id,
            table_id: self.replicas[follower_index]
                .tablet
                .state_machine()
                .tablet()
                .table_id(),
            tablet_epoch: self.tablet_epoch,
        };

        let installed = install_incoming_tablet_snapshot(
            work,
            store,
            receiver,
            &target,
            self.replicas[follower_index].raft.persistence_mut(),
            hard_state,
        )
        .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;

        let core_snapshot = TabletSnapshotTransfer::from_image(target_image)
            .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?
            .into_core_snapshot();
        self.replicas[follower_index]
            .raft
            .complete_snapshot_install(core_snapshot)
            .map_err(|source| TabletClusterError::Ready {
                node_id: follower_id,
                source: Box::new(source),
            })?;

        let identity = self.replicas[follower_index]
            .raft
            .persistence()
            .log_view()
            .identity();
        let raft_pointer = raft_pointer_for_tablet(identity, &installed.installed.pointer)
            .map_err(|error| TabletClusterError::Snapshot(error.to_string()))?;
        let ready = self.replicas[follower_index]
            .raft
            .persist_ready_after_snapshot_boundary(&raft_pointer)
            .map_err(|source| TabletClusterError::Ready {
                node_id: follower_id,
                source: Box::new(source),
            })?
            .ok_or_else(|| {
                TabletClusterError::Snapshot(
                    "completed snapshot installation did not produce a Ready generation"
                        .to_string(),
                )
            })?;

        self.replicas[follower_index].tablet =
            TabletCommandApplier::new(installed.installed.state_machine);

        for entry in &ready.committed_entries {
            if let EntryPayload::Normal(command) = &entry.payload {
                let position = crate::proposal::ProposalPosition {
                    term: entry.term,
                    index: entry.index,
                };
                let disposition = self.replicas[follower_index]
                    .tablet
                    .apply_committed(position, command)
                    .map_err(|source| TabletClusterError::Apply {
                        node_id: follower_id,
                        index: entry.index,
                        source,
                    })?;
                self.resolve_committed(disposition)?;
            }
        }

        let applied_frontier = ready
            .committed_entries
            .last()
            .map(|entry| AppliedRaftFrontier::new(entry.index, entry.term))
            .or_else(|| {
                ready.snapshot.as_ref().map(|snapshot| {
                    AppliedRaftFrontier::new(
                        snapshot.last_included_index,
                        snapshot.last_included_term,
                    )
                })
            })
            .ok_or_else(|| {
                TabletClusterError::Snapshot("snapshot Ready has no applied frontier".to_string())
            })?;

        self.replicas[follower_index]
            .raft
            .advance_applied_frontier(applied_frontier)
            .map_err(|source| TabletClusterError::Ready {
                node_id: follower_id,
                source: Box::new(source),
            })?;
        self.transport.extend(ready.messages);
        self.deliver_messages()?;

        // the transfer pins protect the old prefixes during receive, restore,
        // and Ready acknowledgement. They must be released before asking the
        // shared-WAL owner to reclaim the now-obsolete prefixes
        drop(_leader_retention_pin);
        drop(_follower_retention_pin);

        self.release_replica_retention(leader_index)?;
        self.release_replica_retention(follower_index)
    }

    /// replace an unavailable replica with a newly reconstructed Raft loop and
    /// tablet applier
    ///
    /// the caller must build both objects exclusively from acknowledged durable
    /// state. The method validates group identity, tablet identity, epoch, and
    /// replica identity before making the replacement visible to transport
    pub fn restart_replica_from_durable_state(
        &mut self,
        node_id: u64,
        raft: TabletRaftReadyLoop<W>,
        tablet: TabletCommandApplier,
    ) -> Result<(), TabletClusterError> {
        let replica_index = self.replica_index(node_id)?;

        if self.replicas[replica_index].available {
            return Err(TabletClusterError::Configuration(format!(
                "replica {node_id} must be unavailable before durable restart"
            )));
        }

        let expected_identity = RaftReplicaIdentity::new(self.raft_group_id, ReplicaId(node_id))
            .map_err(|error| TabletClusterError::Configuration(error.to_string()))?;

        if raft.persistence().log_view().identity() != expected_identity {
            return Err(TabletClusterError::Configuration(format!(
                "restarted replica {node_id} has the wrong Raft identity"
            )));
        }

        if tablet.state_machine().tablet().id() != self.tablet_id {
            return Err(TabletClusterError::Configuration(format!(
                "restarted replica {node_id} owns the wrong tablet"
            )));
        }

        if tablet.state_machine().raft_group_id() != self.raft_group_id {
            return Err(TabletClusterError::Configuration(format!(
                "restarted replica {node_id} owns the wrong Raft group"
            )));
        }

        if tablet.state_machine().epoch() != self.tablet_epoch {
            return Err(TabletClusterError::Configuration(format!(
                "restarted replica {node_id} owns the wrong tablet epoch"
            )));
        }

        // discard messages generated for the old in-memory process. The active
        // leader will publish the current suffix and commit frontier through a
        // fresh heartbeat after restart
        self.transport
            .retain(|message| message.from.get() != node_id && message.to.get() != node_id);

        self.replicas[replica_index] = TabletReplica {
            node_id,
            raft,
            tablet,
            available: true,
        };

        Ok(())
    }

    /// elect one leader through the actual Raft message path
    pub fn elect_leader(&mut self) -> Result<u64, TabletClusterError> {
        for _ in 0..16 {
            for node_id in REPLICA_IDS {
                let replica_index = self.replica_index(node_id)?;
                if !self.replicas[replica_index].available {
                    continue;
                }

                let ticks = self.replicas[replica_index]
                    .raft
                    .raft()
                    .current_election_timeout();

                self.tick_replica(node_id, ticks)?;

                match self.leader_id() {
                    Ok(leader_id) => return Ok(leader_id),
                    Err(TabletClusterError::NoLeader) => {}
                    Err(error) => return Err(error),
                }
            }
        }

        Err(TabletClusterError::NoLeader)
    }

    /// return the unique currently visible leader
    pub fn leader_id(&self) -> Result<u64, TabletClusterError> {
        let leaders = self
            .replicas
            .iter()
            .filter(|replica| {
                replica.available && matches!(replica.raft.raft().role(), Role::Leader)
            })
            .map(|replica| replica.node_id)
            .collect::<Vec<_>>();

        match leaders.as_slice() {
            [] => Err(TabletClusterError::NoLeader),
            [leader_id] => Ok(*leader_id),
            _ => Err(TabletClusterError::MultipleLeaders { leaders }),
        }
    }

    /// route a client proposal to the current tablet leader
    pub fn propose(
        &mut self,
        request_id: RequestId,
        command: Vec<u8>,
        deadline: Instant,
    ) -> Result<
        ProposalTicket<
            ragnordb_tablet::command::TabletCommandApplyOutcome,
            TabletCommandApplyError,
        >,
        TabletClusterError,
    > {
        let envelope = TabletCommandEnvelope::decode(&command)?;

        if envelope.request_id != request_id {
            return Err(TabletClusterError::RequestIdentityMismatch);
        }

        if deadline <= Instant::now() {
            return Err(TabletClusterError::ProposalDeadlineExceeded { request_id });
        }

        if self.proposals.is_pending(&request_id) {
            return Err(TabletClusterError::Registry(
                ProposalRegistryError::DuplicateRequest { request_id },
            ));
        }

        let leader_index = self.leader_index()?;
        let leader_id = self.replicas[leader_index].node_id;

        self.replicas[leader_index]
            .tablet
            .state_machine()
            .validate_proposal(&envelope)
            .map_err(|source| TabletClusterError::ProposalValidation {
                node_id: leader_id,
                source,
            })?;

        let proposed_term = self.replicas[leader_index].raft.raft().current_term();

        let encoded_len = command.len();
        let proposed_index = self.replicas[leader_index]
            .raft
            .propose(command, encoded_len)
            .map_err(|source| TabletClusterError::Proposal {
                node_id: leader_id,
                source: Box::new(source),
            })?;

        let ticket = self.proposals.register(
            request_id,
            crate::proposal::ProposalPosition {
                term: proposed_term,
                index: proposed_index,
            },
            deadline,
        )?;

        // ready persistence and message delivery happen only after the
        // proposal has a registered response waiter
        self.drain_ready(leader_index)?;
        self.deliver_messages()?;

        // The initial AppendEntries replicated the log entry with the previous
        // leader_commit value. This heartbeat publishes the newly committed frontier
        // so every follower advances and applies the entry.
        self.tick_replica(leader_id, HEARTBEAT_INTERVAL)?;

        Ok(ticket)
    }

    /// advance one replica's Raft clock and process resulting Ready output
    pub fn tick_replica(&mut self, node_id: u64, ticks: u64) -> Result<(), TabletClusterError> {
        let replica_index = self.replica_index(node_id)?;
        if !self.replicas[replica_index].available {
            return Err(TabletClusterError::ReplicaUnavailable {
                replica_id: node_id,
            });
        }

        self.replicas[replica_index]
            .raft
            .tick(ticks)
            .map_err(|source| TabletClusterError::Ready {
                node_id,
                source: Box::new(source),
            })?;

        self.drain_ready(replica_index)?;
        self.deliver_messages()
    }

    /// return the applied index for one replica
    pub fn last_applied(&self, node_id: u64) -> Result<LogIndex, TabletClusterError> {
        let replica_index = self.replica_index(node_id)?;
        Ok(self.replicas[replica_index].raft.raft().last_applied())
    }

    /// borrow one replica's tablet applier for verification and diagnostics
    pub fn tablet(&self, node_id: u64) -> Result<&TabletCommandApplier, TabletClusterError> {
        let replica_index = self.replica_index(node_id)?;
        Ok(&self.replicas[replica_index].tablet)
    }

    fn drain_ready(&mut self, replica_index: usize) -> Result<(), TabletClusterError> {
        let node_id = self.replicas[replica_index].node_id;

        let Some(ready) = self.replicas[replica_index]
            .raft
            .persist_next_ready(None)
            .map_err(|source| TabletClusterError::Ready {
                node_id,
                source: Box::new(source),
            })?
        else {
            return Ok(());
        };

        for entry in &ready.committed_entries {
            if let EntryPayload::Normal(command) = &entry.payload {
                let position = crate::proposal::ProposalPosition {
                    term: entry.term,
                    index: entry.index,
                };

                let disposition = match self.replicas[replica_index]
                    .tablet
                    .apply_committed(position, command)
                {
                    Ok(applied) => applied,
                    Err(source) => {
                        self.replicas[replica_index].raft.quarantine();

                        return Err(TabletClusterError::Apply {
                            node_id,
                            index: entry.index,
                            source,
                        });
                    }
                };

                if let Err(error) = self.resolve_committed(disposition) {
                    self.replicas[replica_index].raft.quarantine();
                    return Err(error);
                }
            }
        }

        let applied_frontier = ready
            .committed_entries
            .last()
            .map(|entry| AppliedRaftFrontier::new(entry.index, entry.term))
            .or_else(|| {
                ready.snapshot.as_ref().map(|snapshot| {
                    AppliedRaftFrontier::new(
                        snapshot.last_included_index,
                        snapshot.last_included_term,
                    )
                })
            });

        if let Some(applied_frontier) = applied_frontier {
            self.replicas[replica_index]
                .raft
                .advance_applied_frontier(applied_frontier)
                .map_err(|source| TabletClusterError::Ready {
                    node_id,
                    source: Box::new(source),
                })?;
        }

        // outbound messages are released only after this Ready generation has
        // crossed persistence and local apply boundaries
        self.transport.extend(ready.messages);

        Ok(())
    }

    fn resolve_committed(
        &mut self,
        disposition: CommittedTabletCommandDisposition,
    ) -> Result<(), TabletClusterError> {
        match disposition.resolve(&mut self.proposals) {
            Ok(()) => Ok(()),

            // followers apply the same command but do not own the originating
            // client's response waiter in this deterministic harness
            Err(ProposalRegistryError::UnknownRequest { .. })
            | Err(ProposalRegistryError::ResponseChannelClosed { .. }) => Ok(()),

            Err(error) => Err(TabletClusterError::Registry(error)),
        }
    }

    /// Advance this replica's shared-WAL retention floor only after its
    /// snapshot file, pointer, state-machine restore, and applied frontier
    /// are all durable or acknowledged. A node-wide WAL wrapper combines this
    /// floor with every other registered group's floor before pruning.
    fn release_replica_retention(
        &mut self,
        replica_index: usize,
    ) -> Result<(), TabletClusterError> {
        let floor = self.replicas[replica_index]
            .raft
            .persistence()
            .log_view()
            .first_retained_lsn()
            .or_else(|| {
                self.replicas[replica_index]
                    .raft
                    .persistence()
                    .durable_end_lsn()
            })
            .unwrap_or(Lsn::ZERO);

        self.replicas[replica_index]
            .raft
            .persistence_mut()
            .release_retention(floor)
            .map(|_| ())
            .map_err(TabletClusterError::Snapshot)
    }

    fn deliver_messages(&mut self) -> Result<(), TabletClusterError> {
        while let Some(message) = self.transport.pop_front() {
            let target_id = message.to.get();
            let target_index = self.replica_index(target_id)?;
            let source_index = self.replica_index(message.from.get())?;

            // Messages involving a crashed replica are discarded. Once that
            // replica rejoins, the active leader publishes the current log
            // and commit frontier through its normal heartbeat path.
            if !self.replicas[source_index].available || !self.replicas[target_index].available {
                continue;
            }

            self.replicas[target_index]
                .raft
                .step(message)
                .map_err(|source| TabletClusterError::Ready {
                    node_id: target_id,
                    source: Box::new(source),
                })?;

            self.drain_ready(target_index)?;
        }

        Ok(())
    }

    fn leader_index(&self) -> Result<usize, TabletClusterError> {
        let leader_id = self.leader_id()?;
        self.replica_index(leader_id)
    }

    fn replica_index(&self, node_id: u64) -> Result<usize, TabletClusterError> {
        self.replicas
            .iter()
            .position(|replica| replica.node_id == node_id)
            .ok_or(TabletClusterError::UnknownReplica {
                replica_id: node_id,
            })
    }

    /// Return the Raft group hosted by this deterministic cluster.
    pub const fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    /// Return whether the current leader has established and applied its
    /// current-term read barrier.
    pub fn latest_reads_ready(&self) -> Result<bool, TabletClusterError> {
        let leader_index = self.leader_index()?;
        let leader_term = self.replicas[leader_index].raft.raft().current_term();

        Ok(self.read_gate.can_serve_latest(leader_term))
    }

    /// establish a fresh current-term barrier using the default barrier deadline
    pub fn prepare_leader_for_latest_reads(
        &mut self,
    ) -> Result<ReadBarrierPosition, TabletClusterError> {
        self.prepare_leader_for_latest_reads_until(Instant::now() + Duration::from_secs(30))
    }

    /// establish a fresh current-term barrier before a latest read
    ///
    /// every invocation proposes a new no-op. Reusing an older applied barrier
    /// would allow a later read to bypass the exact ordering point required by the
    /// current request
    fn prepare_leader_for_latest_reads_until(
        &mut self,
        deadline: Instant,
    ) -> Result<ReadBarrierPosition, TabletClusterError> {
        if deadline <= Instant::now() {
            return Err(TabletClusterError::LatestReadDeadlineExceeded);
        }

        let leader_id = self.leader_id()?;
        let leader_index = self.replica_index(leader_id)?;
        let leader_term = self.replicas[leader_index].raft.raft().current_term();

        if self.read_gate.leader_term() != Some(leader_term) {
            self.read_gate.on_leader_elected(leader_term)?;
        }

        let sequence = self.next_read_barrier_sequence;
        let next_sequence = sequence.checked_add(1).ok_or_else(|| {
            TabletClusterError::Configuration(
                "read barrier RequestId sequence exhausted".to_string(),
            )
        })?;

        let request_id = RequestId {
            client_id: INTERNAL_READ_BARRIER_CLIENT_ID,
            sequence,
            raft_group_id: self.raft_group_id,
        };

        let command = TabletCommandEnvelope::new(
            request_id.clone(),
            self.tablet_id,
            self.tablet_epoch,
            TabletCommand::Noop(NoopCommand),
        )?
        .encode()?;

        let ticket = self.propose(request_id, command, deadline)?;

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(TabletClusterError::LatestReadDeadlineExceeded);
        }

        let completion = match ticket.recv_timeout(remaining.min(Duration::from_secs(1))) {
            Ok(completion) => completion,
            Err(RecvTimeoutError::Timeout) => {
                if Instant::now() >= deadline {
                    return Err(TabletClusterError::LatestReadDeadlineExceeded);
                }

                return Err(TabletClusterError::ReadBarrierTimeout);
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(TabletClusterError::ReadBarrierChannelClosed);
            }
        };

        match completion {
            ProposalCompletion::Applied {
                position, result, ..
            } => {
                if result.result != TabletCommandApplyResult::Noop {
                    return Err(TabletClusterError::UnexpectedReadBarrierResult);
                }

                let barrier = ReadBarrierPosition {
                    term: position.term,
                    index: position.index,
                };

                self.read_gate.register_barrier(barrier)?;
                self.read_gate.mark_barrier_applied(barrier)?;
                self.next_read_barrier_sequence = next_sequence;

                Ok(barrier)
            }

            ProposalCompletion::Retryable { failure, .. } => {
                Err(TabletClusterError::ReadBarrierRetryable { failure })
            }

            ProposalCompletion::Rejected { rejection, .. } => {
                Err(TabletClusterError::ReadBarrierRejected { rejection })
            }
        }
    }

    /// execute a latest read through the current tablet leader
    ///
    /// the method never reads a follower directly. It first establishes a fresh
    /// current-term Raft barrier, verifies that leadership remained stable, and
    /// only then reads the leader's MVCC state
    pub fn latest_read(
        &mut self,
        transaction: &Transaction,
        key: &RowKey,
        deadline: Instant,
    ) -> Result<Option<ragnordb_common::codec::Row>, TabletClusterError> {
        if deadline <= Instant::now() {
            return Err(TabletClusterError::LatestReadDeadlineExceeded);
        }

        let expected_leader = self.leader_id()?;
        let barrier = self.prepare_leader_for_latest_reads_until(deadline)?;

        if deadline <= Instant::now() {
            return Err(TabletClusterError::LatestReadDeadlineExceeded);
        }

        let observed_leader = self.leader_id()?;
        let observed_index = self.replica_index(observed_leader)?;
        let observed_term = self.replicas[observed_index].raft.raft().current_term();

        if observed_leader != expected_leader
            || observed_term != barrier.term
            || !self.read_gate.can_serve_latest(observed_term)
        {
            return Err(TabletClusterError::LatestReadLeadershipLost {
                expected_leader,
                expected_term: barrier.term,
                observed_leader,
                observed_term,
            });
        }

        self.replicas[observed_index]
            .tablet
            .state_machine()
            .tablet()
            .get(transaction, key)
            .map_err(TabletClusterError::LatestRead)
    }
}
