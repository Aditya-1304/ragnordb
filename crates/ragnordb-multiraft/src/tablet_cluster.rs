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
    ids::{RaftGroupId, ReplicaId, RequestId, TableId, TabletId},
};

use ragnordb_tablet::{
    Tablet,
    command::{TabletCommandApplyError, TabletCommandApplyResult, TabletStateMachine},
    read::{LeaderReadGate, LeaderReadGateError, ReadBarrierPosition},
};

use crate::{
    proposal::{
        ProposalCompletion, ProposalFailure, ProposalRegistry, ProposalRegistryError,
        ProposalTicket,
    },
    runtime::{RaftReadyLoop, ReadyLoopError},
    storage::{
        codec::RaftReplicaIdentity,
        persistence::{RaftWal, RaftWalStorage},
    },
    tablet_apply::{AppliedTabletCommand, TabletApplyError, TabletCommandApplier},
};

const REPLICA_IDS: [u64; 3] = [1, 2, 3];
const ELECTION_TIMEOUT: u64 = 5;
const HEARTBEAT_INTERVAL: u64 = 2;
const INTERNAL_READ_BARRIER_CLIENT_ID: u128 = 1_u128 << 127;

type CoreRaftNode =
    RaftNode<Vec<u8>, Vec<u8>, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

type ReplicaReadyLoop<W> =
    RaftReadyLoop<W, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

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
        source: ReadyLoopError,
    },

    #[error("replica {node_id} rejected a proposal before Raft admission: {source}")]
    ProposalValidation {
        node_id: u64,
        source: TabletCommandApplyError,
    },

    #[error("replica {node_id} proposal failed: {source}")]
    Proposal {
        node_id: u64,
        source: ReadyLoopError,
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

    #[error("leader read barrier applied an unexpected tablet result")]
    UnexpectedReadBarrierResult,
}

/// one in memory three-node Raft group owning one tablet
pub struct InMemoryTabletCluster<W: RaftWal> {
    replicas: [TabletReplica<W>; 3],
    transport: VecDeque<Envelope<Vec<u8>, Vec<u8>>>,
    proposals: ProposalRegistry<ragnordb_tablet::command::TabletCommandApplyOutcome>,
    raft_group_id: RaftGroupId,
    tablet_id: TabletId,
    tablet_epoch: u64,
    read_gate: LeaderReadGate,
    next_read_barrier_sequence: u64,
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
        ProposalTicket<ragnordb_tablet::command::TabletCommandApplyOutcome>,
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
                source,
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
            .map_err(|source| TabletClusterError::Ready { node_id, source })?;

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
            .map_err(|source| TabletClusterError::Ready { node_id, source })?
        else {
            return Ok(());
        };

        for entry in &ready.committed_entries {
            if let EntryPayload::Normal(command) = &entry.payload {
                let position = crate::proposal::ProposalPosition {
                    term: entry.term,
                    index: entry.index,
                };

                let applied = match self.replicas[replica_index]
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

                if let Err(error) = self.resolve_applied(applied) {
                    self.replicas[replica_index].raft.quarantine();
                    return Err(error);
                }
            }
        }

        if let Some(applied_through) = ready.apply_through() {
            self.replicas[replica_index]
                .raft
                .advance_applied(applied_through)
                .map_err(|source| TabletClusterError::Ready { node_id, source })?;
        }

        // outbound messages are released only after this Ready generation has
        // crossed persistence and local apply boundaries
        self.transport.extend(ready.messages);

        Ok(())
    }

    fn resolve_applied(&mut self, applied: AppliedTabletCommand) -> Result<(), TabletClusterError> {
        match applied.resolve(&mut self.proposals) {
            Ok(()) => Ok(()),

            // followers apply the same command but do not own the originating
            // client's response waiter in this deterministic harness
            Err(ProposalRegistryError::UnknownRequest { .. })
            | Err(ProposalRegistryError::ResponseChannelClosed { .. }) => Ok(()),

            Err(error) => Err(TabletClusterError::Registry(error)),
        }
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
                    source,
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

    /// Propose and apply the current-term no-op required before latest reads.
    ///
    /// The barrier is registered only after the proposal completion comes from the
    /// tablet state-machine apply path. Raft proposal acceptance or commit-index
    /// movement alone never opens the read gate.
    pub fn prepare_leader_for_latest_reads(
        &mut self,
    ) -> Result<ReadBarrierPosition, TabletClusterError> {
        let leader_id = self.leader_id()?;
        let leader_index = self.replica_index(leader_id)?;
        let leader_term = self.replicas[leader_index].raft.raft().current_term();

        if self.read_gate.leader_term() != Some(leader_term) {
            self.read_gate.on_leader_elected(leader_term)?;
        }

        if self.read_gate.can_serve_latest(leader_term) {
            return Ok(self
                .read_gate
                .pending_barrier()
                .ok_or(LeaderReadGateError::NoPendingBarrier)?);
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

        let ticket = self.propose(
            request_id,
            command,
            Instant::now() + Duration::from_secs(30),
        )?;

        let completion = match ticket.recv_timeout(Duration::from_secs(1)) {
            Ok(completion) => completion,
            Err(RecvTimeoutError::Timeout) => {
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
        }
    }
}
