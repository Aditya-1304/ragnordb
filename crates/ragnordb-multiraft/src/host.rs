//! Physical-node host for the independent Raft replicas assigned to one server.
//!
//! The host deliberately performs no fair scheduling or cross-group batching.
//! Its responsibility is narrower: preserve each group's Ready ordering while
//! ensuring that a shared A-WAL recovery fence stops every local replica.

use std::collections::{BTreeMap, BTreeSet};

use raft::{
    message::Envelope,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::LogIndex,
};
use ragnordb_common::ids::{NodeId, RaftGroupId, ReplicaId};

use crate::{
    runtime::{
        RaftReadyLoop, RaftReadyStateMachine, RaftSnapshotStore, ReadyApplyError, ReadyLoopError,
    },
    storage::{
        codec::RaftReplicaIdentity,
        persistence::{NodeRaftWal, NodeRaftWalHandle, RaftWal},
        recovery::RecoveredRaftStorage,
    },
};

/// Wire envelope used by the byte-oriented Raft runtime.
pub type RaftMessageEnvelope = Envelope<Vec<u8>, Vec<u8>>;

/// An inter-node Raft envelope tagged with its logical group.
///
/// Raft-core envelopes identify replicas but intentionally do not identify a
/// logical group. A physical node may host multiple replica lifetimes, so the
/// transport boundary must carry this tag before the envelope reaches a local
/// `RaftReadyLoop`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedRaftMessage {
    pub raft_group_id: RaftGroupId,
    pub envelope: RaftMessageEnvelope,
}

/// Result of admitting one client command to a particular hosted group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostedProposal {
    pub index: LogIndex,
    pub outbound: Vec<RoutedRaftMessage>,
}

/// Type-erased lifecycle boundary for one hosted Raft replica.
///
/// A single call performs the triggering Raft operation and drains the
/// resulting Ready lifecycle before returning. This prevents another caller
/// from interleaving work between `step()` and persistence/apply.
pub trait HostedRaftGroup: Send {
    fn identity(&self) -> RaftReplicaIdentity;

    fn tick_and_drain(&mut self, ticks: u64) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError>;

    fn step_and_drain(
        &mut self,
        message: RaftMessageEnvelope,
    ) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError>;

    fn propose_and_drain(
        &mut self,
        command: Vec<u8>,
        encoded_len: usize,
    ) -> Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError>;
}

/// Error reported by an individual hosted group.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HostedGroupError {
    #[error("the shared Raft WAL requires recovery")]
    RecoveryRequired,

    #[error("hosted Raft group failed: {0}")]
    Group(String),
}

/// Concrete adapter that drives the existing Ready runtime and its application
/// dependencies as one host-managed group.
pub struct ReadyLoopHostedGroup<W, LS, SS, SM, SF>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
    SM: RaftReadyStateMachine,
    SF: RaftSnapshotStore,
{
    ready_loop: RaftReadyLoop<W, LS, SS>,
    state_machine: SM,
    snapshot_store: SF,
}

impl<W, LS, SS, SM, SF> ReadyLoopHostedGroup<W, LS, SS, SM, SF>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
    SM: RaftReadyStateMachine,
    SF: RaftSnapshotStore,
{
    pub fn new(
        ready_loop: RaftReadyLoop<W, LS, SS>,
        state_machine: SM,
        snapshot_store: SF,
    ) -> Self {
        Self {
            ready_loop,
            state_machine,
            snapshot_store,
        }
    }

    pub fn ready_loop(&self) -> &RaftReadyLoop<W, LS, SS> {
        &self.ready_loop
    }

    fn drain_ready(&mut self) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
        let mut outbound = Vec::new();

        loop {
            match self
                .ready_loop
                .persist_and_apply_next_ready(&mut self.snapshot_store, &mut self.state_machine)
                .map_err(classify_apply_error)?
            {
                Some(ready) => {
                    outbound.extend(ready.messages);
                }

                None => return Ok(outbound),
            }
        }
    }
}

impl<W, LS, SS, SM, SF> HostedRaftGroup for ReadyLoopHostedGroup<W, LS, SS, SM, SF>
where
    W: RaftWal + Send,
    LS: LogStore<Vec<u8>> + Send,
    SS: StableStore + Send,
    SM: RaftReadyStateMachine + Send,
    SF: RaftSnapshotStore + Send,
{
    fn identity(&self) -> RaftReplicaIdentity {
        self.ready_loop.persistence().log_view().identity()
    }

    fn tick_and_drain(&mut self, ticks: u64) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
        self.ready_loop.tick(ticks).map_err(classify_ready_error)?;

        self.drain_ready()
    }

    fn step_and_drain(
        &mut self,
        message: RaftMessageEnvelope,
    ) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
        self.ready_loop
            .step(message)
            .map_err(classify_ready_error)?;

        self.drain_ready()
    }

    fn propose_and_drain(
        &mut self,
        command: Vec<u8>,
        encoded_len: usize,
    ) -> Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError> {
        let index = self
            .ready_loop
            .propose(command, encoded_len)
            .map_err(classify_ready_error)?;

        let outbound = self.drain_ready()?;

        Ok((index, outbound))
    }
}

fn classify_ready_error(error: ReadyLoopError) -> HostedGroupError {
    if matches!(error, ReadyLoopError::RecoveryRequired) {
        HostedGroupError::RecoveryRequired
    } else {
        HostedGroupError::Group(error.to_string())
    }
}

fn classify_apply_error(error: ReadyApplyError) -> HostedGroupError {
    if matches!(
        error,
        ReadyApplyError::Ready(ReadyLoopError::RecoveryRequired)
    ) {
        HostedGroupError::RecoveryRequired
    } else {
        HostedGroupError::Group(error.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostState {
    Registering,
    Active,
    RecoveryRequired,
}

/// Minimum physical-node multi-Raft host.
///
/// All recovered identities must be registered before [`Self::activate`]. The
/// caller must obtain each group's writer through `NodeRaftWal::group_writer_for`
/// before activation; activation seals that registration so retention always
/// considers every replica represented in the shared WAL.
pub struct MultiRaftHost<W>
where
    W: RaftWal,
{
    node_id: NodeId,

    /// The one physical Raft persistence authority for this node.
    node_wal: NodeRaftWal<W>,

    state: HostState,

    groups: BTreeMap<RaftGroupId, Box<dyn HostedRaftGroup>>,

    /// Durable identities discovered by the one shared-WAL scan which have
    /// not yet been accounted for by startup reconstruction.
    pending_recovered: BTreeSet<RaftReplicaIdentity>,

    /// Identities for which this host issued an identity-bound writer.
    ///
    /// A hosted group cannot be registered without first obtaining its WAL
    /// handle from the same NodeRaftWal owned by this host.
    issued_writers: BTreeSet<RaftReplicaIdentity>,

    /// Permanently isolated groups for this process lifetime.
    ///
    /// Group-local corruption/apply/snapshot failures do not stop unrelated
    /// Raft groups. Shared-WAL uncertainty still moves the entire host into
    /// RecoveryRequired.
    quarantined: BTreeMap<RaftGroupId, String>,
}

impl<W> MultiRaftHost<W>
where
    W: RaftWal,
{
    pub fn new(node_id: NodeId, node_wal: NodeRaftWal<W>) -> Self {
        Self {
            node_id,
            node_wal,
            state: HostState::Registering,
            groups: BTreeMap::new(),
            pending_recovered: BTreeSet::new(),
            issued_writers: BTreeSet::new(),
            quarantined: BTreeMap::new(),
        }
    }

    pub fn from_recovered(
        node_id: NodeId,
        node_wal: NodeRaftWal<W>,
        recovered: &RecoveredRaftStorage,
    ) -> Self {
        Self {
            node_id,
            node_wal,
            state: HostState::Registering,
            groups: BTreeMap::new(),
            pending_recovered: recovered
                .replicas()
                .map(|(identity, _)| *identity)
                .collect(),
            issued_writers: BTreeSet::new(),
            quarantined: BTreeMap::new(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Clone the node-wide WAL authority.
    ///
    /// Clones share the same physical persistence state and recovery fence.
    pub fn node_wal(&self) -> NodeRaftWal<W> {
        self.node_wal.clone()
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn group_failure(&self, raft_group_id: RaftGroupId) -> Option<&str> {
        self.quarantined.get(&raft_group_id).map(String::as_str)
    }

    pub fn register_new_group(
        &mut self,
        group: Box<dyn HostedRaftGroup>,
    ) -> Result<(), MultiRaftHostError> {
        self.register_group(group, false)
    }

    pub fn register_recovered_group(
        &mut self,
        group: Box<dyn HostedRaftGroup>,
    ) -> Result<(), MultiRaftHostError> {
        self.register_group(group, true)
    }

    /// Records a recovered replica lifetime which must remain in shared-WAL
    /// retention accounting but has no active runtime after membership
    /// reconstruction (for example, a removed predecessor replica).
    ///
    /// The caller must still have registered this identity with
    /// `NodeRaftWal::group_writer_for` before activation. Keeping retired
    /// identities separate from the active group map avoids conflating a
    /// Raft-group ID with a replica lifetime.
    pub fn register_inactive_recovered_identity(
        &mut self,
        identity: RaftReplicaIdentity,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_registering()?;
        self.ensure_shared_wal_healthy()?;
        if !self.issued_writers.contains(&identity) {
            return Err(MultiRaftHostError::WalWriterNotIssued(identity));
        }
        if self.pending_recovered.remove(&identity) {
            Ok(())
        } else {
            Err(MultiRaftHostError::UnexpectedRecovered(identity))
        }
    }

    fn register_group(
        &mut self,
        group: Box<dyn HostedRaftGroup>,
        recovered: bool,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_registering()?;
        self.ensure_shared_wal_healthy()?;
        let identity = group.identity();
        if !self.issued_writers.contains(&identity) {
            return Err(MultiRaftHostError::WalWriterNotIssued(identity));
        }
        if self.groups.contains_key(&identity.raft_group_id) {
            return Err(MultiRaftHostError::DuplicateGroup(identity.raft_group_id));
        }
        if recovered {
            if !self.pending_recovered.remove(&identity) {
                return Err(MultiRaftHostError::UnexpectedRecovered(identity));
            }
        } else if self.pending_recovered.contains(&identity) {
            return Err(MultiRaftHostError::RecoveredIdentityRequiresRecovery(
                identity,
            ));
        }
        self.groups.insert(identity.raft_group_id, group);
        Ok(())
    }

    /// Issue the only valid persistence handle for one replica lifetime.
    ///
    /// The caller must use this handle to construct the corresponding hosted
    /// group. Registration without prior issuance is rejected.
    pub fn issue_group_writer(
        &mut self,
        identity: RaftReplicaIdentity,
    ) -> Result<NodeRaftWalHandle<W>, MultiRaftHostError> {
        self.ensure_registering()?;
        self.ensure_shared_wal_healthy()?;

        if !self.issued_writers.insert(identity) {
            return Err(MultiRaftHostError::WalWriterAlreadyIssued(identity));
        }

        self.node_wal
            .group_writer_for(identity)
            .map_err(MultiRaftHostError::WalRegistration)
    }

    /// Seals local replica discovery and permits Ready processing.
    pub fn activate(&mut self) -> Result<(), MultiRaftHostError> {
        self.ensure_registering()?;
        self.ensure_shared_wal_healthy()?;
        if !self.pending_recovered.is_empty() {
            return Err(MultiRaftHostError::MissingRecovered(
                self.pending_recovered.iter().copied().collect(),
            ));
        }
        self.node_wal
            .seal_retention_registry()
            .map_err(MultiRaftHostError::RetentionRegistry)?;
        self.state = HostState::Active;
        Ok(())
    }

    /// Delivers an inbound group-tagged Raft message and then drains that
    /// group's full Ready persistence/application lifecycle.
    pub fn route(
        &mut self,
        message: RoutedRaftMessage,
    ) -> Result<Vec<RoutedRaftMessage>, MultiRaftHostError> {
        self.ensure_active()?;

        let raft_group_id = message.raft_group_id;

        if let Some(reason) = self.quarantined.get(&raft_group_id) {
            return Err(MultiRaftHostError::GroupQuarantined {
                raft_group_id,
                reason: reason.clone(),
            });
        }

        let identity = self
            .groups
            .get(&raft_group_id)
            .ok_or(MultiRaftHostError::UnknownGroup(raft_group_id))?
            .identity();

        let expected = identity
            .replica_id
            .to_raft()
            .expect("registered replica identity is validated");

        if message.envelope.to != expected {
            return Err(MultiRaftHostError::RecipientMismatch {
                raft_group_id,
                expected: identity.replica_id,
                received: ReplicaId::from_raft(message.envelope.to),
            });
        }

        let result = {
            let group = self
                .groups
                .get_mut(&raft_group_id)
                .expect("group was resolved above");

            group.step_and_drain(message.envelope)
        };

        let messages = match result {
            Ok(messages) => messages,

            Err(HostedGroupError::RecoveryRequired) => {
                self.state = HostState::RecoveryRequired;

                return Err(MultiRaftHostError::RecoveryRequired);
            }

            Err(HostedGroupError::Group(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;

                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                self.quarantined.insert(raft_group_id, reason.clone());

                return Err(MultiRaftHostError::Group {
                    raft_group_id,
                    reason,
                });
            }
        };

        self.ensure_shared_wal_healthy()?;

        Ok(messages
            .into_iter()
            .map(|envelope| RoutedRaftMessage {
                raft_group_id,
                envelope,
            })
            .collect())
    }

    /// Tick every healthy local group once.
    ///
    /// A group-local failure quarantines only that group and iteration
    /// continues. Shared-WAL uncertainty aborts the complete host immediately.
    pub fn tick_all(&mut self, ticks: u64) -> Result<Vec<RoutedRaftMessage>, MultiRaftHostError> {
        self.ensure_active()?;

        let group_ids: Vec<_> = self.groups.keys().copied().collect();

        let mut outbound = Vec::new();

        for raft_group_id in group_ids {
            if self.quarantined.contains_key(&raft_group_id) {
                continue;
            }

            let result = {
                let group = self
                    .groups
                    .get_mut(&raft_group_id)
                    .expect("group ID came from group registry");

                group.tick_and_drain(ticks)
            };

            match result {
                Ok(messages) => {
                    // Another group could have discovered shared-WAL
                    // uncertainty while this group was running.
                    self.ensure_shared_wal_healthy()?;

                    outbound.extend(messages.into_iter().map(|envelope| RoutedRaftMessage {
                        raft_group_id,
                        envelope,
                    }));
                }

                Err(HostedGroupError::RecoveryRequired) => {
                    self.state = HostState::RecoveryRequired;

                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                Err(HostedGroupError::Group(reason)) => {
                    // If the underlying shared authority entered recovery,
                    // this is node-wide even if the outer group adapter lost
                    // the typed error.
                    if self.node_wal.recovery_required() {
                        self.state = HostState::RecoveryRequired;

                        return Err(MultiRaftHostError::RecoveryRequired);
                    }

                    self.quarantined.insert(raft_group_id, reason);

                    // Continue ticking unrelated groups.
                }
            }
        }

        Ok(outbound)
    }

    pub fn propose(
        &mut self,
        raft_group_id: RaftGroupId,
        command: Vec<u8>,
        encoded_len: usize,
    ) -> Result<HostedProposal, MultiRaftHostError> {
        self.ensure_active()?;

        if let Some(reason) = self.quarantined.get(&raft_group_id) {
            return Err(MultiRaftHostError::GroupQuarantined {
                raft_group_id,
                reason: reason.clone(),
            });
        }

        let result = {
            let group = self
                .groups
                .get_mut(&raft_group_id)
                .ok_or(MultiRaftHostError::UnknownGroup(raft_group_id))?;

            group.propose_and_drain(command, encoded_len)
        };

        let (index, messages) = match result {
            Ok(result) => result,

            Err(HostedGroupError::RecoveryRequired) => {
                self.state = HostState::RecoveryRequired;

                return Err(MultiRaftHostError::RecoveryRequired);
            }

            Err(HostedGroupError::Group(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;

                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                self.quarantined.insert(raft_group_id, reason.clone());

                return Err(MultiRaftHostError::Group {
                    raft_group_id,
                    reason,
                });
            }
        };

        self.ensure_shared_wal_healthy()?;

        Ok(HostedProposal {
            index,
            outbound: messages
                .into_iter()
                .map(|envelope| RoutedRaftMessage {
                    raft_group_id,
                    envelope,
                })
                .collect(),
        })
    }

    fn ensure_registering(&self) -> Result<(), MultiRaftHostError> {
        match self.state {
            HostState::Registering => Ok(()),
            HostState::Active => Err(MultiRaftHostError::AlreadyActive),
            HostState::RecoveryRequired => Err(MultiRaftHostError::RecoveryRequired),
        }
    }

    fn ensure_active(&mut self) -> Result<(), MultiRaftHostError> {
        if self.node_wal.recovery_required() {
            self.state = HostState::RecoveryRequired;
        }
        match self.state {
            HostState::Active => Ok(()),
            HostState::Registering => Err(MultiRaftHostError::NotActive),
            HostState::RecoveryRequired => Err(MultiRaftHostError::RecoveryRequired),
        }
    }

    fn ensure_shared_wal_healthy(&mut self) -> Result<(), MultiRaftHostError> {
        if self.node_wal.recovery_required() {
            self.state = HostState::RecoveryRequired;
            Err(MultiRaftHostError::RecoveryRequired)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MultiRaftHostError {
    #[error("the host has not completed local replica registration")]
    NotActive,
    #[error("the host is already active")]
    AlreadyActive,
    #[error("the shared Raft WAL requires full node recovery")]
    RecoveryRequired,
    #[error("no local replica is registered for Raft group {0:?}")]
    UnknownGroup(RaftGroupId),
    #[error(
        "Raft envelope for group {raft_group_id:?} targets replica {received:?}, not local replica {expected:?}"
    )]
    RecipientMismatch {
        raft_group_id: RaftGroupId,
        expected: ReplicaId,
        received: ReplicaId,
    },
    #[error("Raft group {0:?} is already registered on this node")]
    DuplicateGroup(RaftGroupId),
    #[error("recovered identity {0:?} must use the recovered startup path")]
    RecoveredIdentityRequiresRecovery(RaftReplicaIdentity),
    #[error("identity {0:?} was not discovered by shared-WAL recovery")]
    UnexpectedRecovered(RaftReplicaIdentity),
    #[error("cannot activate before recovering local replica identities: {0:?}")]
    MissingRecovered(Vec<RaftReplicaIdentity>),
    #[error("could not seal shared-WAL retention registry: {0}")]
    RetentionRegistry(String),
    #[error("hosted Raft group {raft_group_id:?} failed: {reason}")]
    Group {
        raft_group_id: RaftGroupId,
        reason: String,
    },
    #[error(
        "Raft WAL writer for replica lifetime {0:?} \
         was not issued by this MultiRaft host"
    )]
    WalWriterNotIssued(RaftReplicaIdentity),

    #[error(
        "Raft WAL writer for replica lifetime {0:?} \
         was already issued"
    )]
    WalWriterAlreadyIssued(RaftReplicaIdentity),

    #[error("could not register replica lifetime with shared Raft WAL: {0}")]
    WalRegistration(String),

    #[error("Raft group {raft_group_id:?} is quarantined: {reason}")]
    GroupQuarantined {
        raft_group_id: RaftGroupId,
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::{message::Message, types::ReplicaId as RaftReplicaId};
    use ragnordb_common::ids::ReplicaId;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    use wal::{error::BatchAppendFailure, types::RecordType, wal::BatchAppendResult};

    struct TestWal;

    impl RaftWal for TestWal {
        fn append_batch_and_sync(
            &mut self,
            _: &[(RecordType, &[u8])],
        ) -> Result<BatchAppendResult, BatchAppendFailure> {
            unreachable!("host tests do not persist Ready generations")
        }
    }

    #[derive(Clone, Copy)]
    enum TickBehavior {
        Healthy,
        GroupFailure,
        RecoveryRequired,
    }

    struct TestGroup {
        identity: RaftReplicaIdentity,
        tick_behavior: TickBehavior,
        ticks: Arc<AtomicU64>,
        stepped: Arc<AtomicU64>,
        outbound: Vec<RaftMessageEnvelope>,
    }

    impl HostedRaftGroup for TestGroup {
        fn identity(&self) -> RaftReplicaIdentity {
            self.identity
        }

        fn tick_and_drain(&mut self, _: u64) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
            match self.tick_behavior {
                TickBehavior::Healthy => {
                    self.ticks.fetch_add(1, Ordering::SeqCst);
                    Ok(Vec::new())
                }
                TickBehavior::GroupFailure => Err(HostedGroupError::Group(
                    "injected group-local failure".to_string(),
                )),
                TickBehavior::RecoveryRequired => Err(HostedGroupError::RecoveryRequired),
            }
        }

        fn step_and_drain(
            &mut self,
            _: RaftMessageEnvelope,
        ) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
            self.stepped.fetch_add(1, Ordering::SeqCst);
            Ok(std::mem::take(&mut self.outbound))
        }

        fn propose_and_drain(
            &mut self,
            _: Vec<u8>,
            _: usize,
        ) -> Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError> {
            Ok((0, std::mem::take(&mut self.outbound)))
        }
    }

    fn identity(group: u64, replica: u64) -> RaftReplicaIdentity {
        RaftReplicaIdentity::new(RaftGroupId(group), ReplicaId(replica)).unwrap()
    }

    fn healthy_group(
        identity: RaftReplicaIdentity,
        outbound: Vec<RaftMessageEnvelope>,
    ) -> TestGroup {
        TestGroup {
            identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound,
        }
    }

    #[test]
    fn route_demultiplexes_to_the_tagged_group_and_releases_only_its_messages() {
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let first_identity = identity(10, 10);
        let second_identity = identity(11, 11);
        let _first_writer = host.issue_group_writer(first_identity).unwrap();
        let _second_writer = host.issue_group_writer(second_identity).unwrap();
        let inbound = Envelope {
            from: RaftReplicaId::must(20),
            to: RaftReplicaId::must(11),
            msg: Message::PreVoteResponse(raft::message::PreVoteResponse {
                term: 0,
                vote_granted: true,
            }),
        };
        let outbound = Envelope {
            from: RaftReplicaId::must(11),
            to: RaftReplicaId::must(21),
            msg: Message::PreVoteResponse(raft::message::PreVoteResponse {
                term: 0,
                vote_granted: true,
            }),
        };
        host.register_new_group(Box::new(healthy_group(first_identity, Vec::new())))
            .unwrap();
        host.register_new_group(Box::new(healthy_group(
            second_identity,
            vec![outbound.clone()],
        )))
        .unwrap();
        host.activate().unwrap();
        assert_eq!(
            host.route(RoutedRaftMessage {
                raft_group_id: RaftGroupId(11),
                envelope: inbound
            })
            .unwrap(),
            vec![RoutedRaftMessage {
                raft_group_id: RaftGroupId(11),
                envelope: outbound
            }]
        );
    }

    #[test]
    fn quarantined_group_does_not_starve_other_groups() {
        let healthy_ticks = Arc::new(AtomicU64::new(0));
        let failing_identity = identity(10, 101);
        let healthy_identity = identity(20, 202);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _failing_writer = host.issue_group_writer(failing_identity).unwrap();
        let _healthy_writer = host.issue_group_writer(healthy_identity).unwrap();

        host.register_new_group(Box::new(TestGroup {
            identity: failing_identity,
            tick_behavior: TickBehavior::GroupFailure,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: healthy_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::clone(&healthy_ticks),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();

        host.tick_all(1).unwrap();
        assert_eq!(healthy_ticks.load(Ordering::SeqCst), 1);
        assert!(host.group_failure(RaftGroupId(10)).is_some());

        host.tick_all(1).unwrap();
        assert_eq!(healthy_ticks.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn recovery_required_from_one_group_fences_whole_host() {
        let failing_identity = identity(10, 101);
        let healthy_identity = identity(20, 202);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _failing_writer = host.issue_group_writer(failing_identity).unwrap();
        let _healthy_writer = host.issue_group_writer(healthy_identity).unwrap();

        host.register_new_group(Box::new(TestGroup {
            identity: failing_identity,
            tick_behavior: TickBehavior::RecoveryRequired,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.register_new_group(Box::new(healthy_group(healthy_identity, Vec::new())))
            .unwrap();
        host.activate().unwrap();

        assert_eq!(
            host.tick_all(1).unwrap_err(),
            MultiRaftHostError::RecoveryRequired
        );
        assert_eq!(
            host.propose(RaftGroupId(20), vec![1], 1).unwrap_err(),
            MultiRaftHostError::RecoveryRequired
        );
    }
}
