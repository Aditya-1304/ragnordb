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
        persistence::{NodeRaftWal, RaftWal},
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

/// The node-wide A-WAL health boundary observed by every hosted group.
///
/// An uncertain shared-WAL operation has no group-local recovery proof. The
/// host therefore checks this state before it routes, ticks, proposes, or
/// drains any group, rather than allowing another group to continue serving.
pub trait SharedWalHealth {
    fn recovery_required(&self) -> bool;
    fn seal_retention_registry(&self) -> Result<(), String>;
}

impl<W> SharedWalHealth for NodeRaftWal<W> {
    fn recovery_required(&self) -> bool {
        Self::recovery_required(self)
    }

    fn seal_retention_registry(&self) -> Result<(), String> {
        Self::seal_retention_registry(self)
    }
}

/// Type-erased lifecycle boundary for one hosted Raft replica.
///
/// Erasure permits a single physical node to host groups backed by different
/// state-machine and snapshot-store implementations without weakening the
/// per-group Ready-loop ordering contract.
pub trait HostedRaftGroup {
    fn identity(&self) -> RaftReplicaIdentity;
    fn tick(&mut self, ticks: u64) -> Result<(), HostedGroupError>;
    fn step(&mut self, message: RaftMessageEnvelope) -> Result<(), HostedGroupError>;
    fn propose(
        &mut self,
        command: Vec<u8>,
        encoded_len: usize,
    ) -> Result<LogIndex, HostedGroupError>;
    fn persist_and_apply(&mut self) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError>;
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
}

impl<W, LS, SS, SM, SF> HostedRaftGroup for ReadyLoopHostedGroup<W, LS, SS, SM, SF>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
    SM: RaftReadyStateMachine,
    SF: RaftSnapshotStore,
{
    fn identity(&self) -> RaftReplicaIdentity {
        self.ready_loop.persistence().log_view().identity()
    }

    fn tick(&mut self, ticks: u64) -> Result<(), HostedGroupError> {
        self.ready_loop.tick(ticks).map_err(classify_ready_error)
    }

    fn step(&mut self, message: RaftMessageEnvelope) -> Result<(), HostedGroupError> {
        self.ready_loop.step(message).map_err(classify_ready_error)
    }

    fn propose(
        &mut self,
        command: Vec<u8>,
        encoded_len: usize,
    ) -> Result<LogIndex, HostedGroupError> {
        self.ready_loop
            .propose(command, encoded_len)
            .map_err(classify_ready_error)
    }

    fn persist_and_apply(&mut self) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
        self.ready_loop
            .persist_and_apply_next_ready(&mut self.snapshot_store, &mut self.state_machine)
            .map_err(classify_apply_error)
            .map(|ready| ready.map_or_else(Vec::new, |ready| ready.messages))
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
pub struct MultiRaftHost {
    node_id: NodeId,
    wal_health: Box<dyn SharedWalHealth>,
    state: HostState,
    groups: BTreeMap<RaftGroupId, Box<dyn HostedRaftGroup>>,
    pending_recovered: BTreeSet<RaftReplicaIdentity>,
}

impl MultiRaftHost {
    pub fn new(node_id: NodeId, wal_health: Box<dyn SharedWalHealth>) -> Self {
        Self {
            node_id,
            wal_health,
            state: HostState::Registering,
            groups: BTreeMap::new(),
            pending_recovered: BTreeSet::new(),
        }
    }

    /// Creates a host whose startup barrier covers every identity discovered by
    /// the single shared-WAL recovery scan.
    pub fn from_recovered(
        node_id: NodeId,
        wal_health: Box<dyn SharedWalHealth>,
        recovered: &RecoveredRaftStorage,
    ) -> Self {
        Self::with_recovered_identities(
            node_id,
            wal_health,
            recovered.replicas().map(|(identity, _)| *identity),
        )
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
    pub fn group_count(&self) -> usize {
        self.groups.len()
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

    fn with_recovered_identities(
        node_id: NodeId,
        wal_health: Box<dyn SharedWalHealth>,
        identities: impl IntoIterator<Item = RaftReplicaIdentity>,
    ) -> Self {
        Self {
            node_id,
            wal_health,
            state: HostState::Registering,
            groups: BTreeMap::new(),
            pending_recovered: identities.into_iter().collect(),
        }
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
        self.wal_health
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
        let group = self
            .groups
            .get_mut(&raft_group_id)
            .ok_or(MultiRaftHostError::UnknownGroup(raft_group_id))?;
        let identity = group.identity();
        let expected = identity
            .replica_id
            .to_raft()
            .expect("stored replica identities are validated");
        if message.envelope.to != expected {
            return Err(MultiRaftHostError::RecipientMismatch {
                raft_group_id,
                expected: identity.replica_id,
                received: ReplicaId::from_raft(message.envelope.to),
            });
        }
        group
            .step(message.envelope)
            .map_err(|error| self.group_error(raft_group_id, error))?;
        self.drain_group(raft_group_id)
    }

    pub fn tick_all(&mut self, ticks: u64) -> Result<Vec<RoutedRaftMessage>, MultiRaftHostError> {
        self.ensure_active()?;
        let group_ids: Vec<_> = self.groups.keys().copied().collect();
        let mut outbound = Vec::new();
        for raft_group_id in group_ids {
            let group = self
                .groups
                .get_mut(&raft_group_id)
                .expect("group id collected from map");
            group
                .tick(ticks)
                .map_err(|error| self.group_error(raft_group_id, error))?;
            outbound.extend(self.drain_group(raft_group_id)?);
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
        let group = self
            .groups
            .get_mut(&raft_group_id)
            .ok_or(MultiRaftHostError::UnknownGroup(raft_group_id))?;
        let index = group
            .propose(command, encoded_len)
            .map_err(|error| self.group_error(raft_group_id, error))?;
        let outbound = self.drain_group(raft_group_id)?;
        Ok(HostedProposal { index, outbound })
    }

    fn drain_group(
        &mut self,
        raft_group_id: RaftGroupId,
    ) -> Result<Vec<RoutedRaftMessage>, MultiRaftHostError> {
        self.ensure_shared_wal_healthy()?;
        let group = self
            .groups
            .get_mut(&raft_group_id)
            .expect("known hosted group");
        let messages = group
            .persist_and_apply()
            .map_err(|error| self.group_error(raft_group_id, error))?;
        self.ensure_shared_wal_healthy()?;
        Ok(messages
            .into_iter()
            .map(|envelope| RoutedRaftMessage {
                raft_group_id,
                envelope,
            })
            .collect())
    }

    fn ensure_registering(&self) -> Result<(), MultiRaftHostError> {
        match self.state {
            HostState::Registering => Ok(()),
            HostState::Active => Err(MultiRaftHostError::AlreadyActive),
            HostState::RecoveryRequired => Err(MultiRaftHostError::RecoveryRequired),
        }
    }

    fn ensure_active(&mut self) -> Result<(), MultiRaftHostError> {
        if self.wal_health.recovery_required() {
            self.state = HostState::RecoveryRequired;
        }
        match self.state {
            HostState::Active => Ok(()),
            HostState::Registering => Err(MultiRaftHostError::NotActive),
            HostState::RecoveryRequired => Err(MultiRaftHostError::RecoveryRequired),
        }
    }

    fn ensure_shared_wal_healthy(&mut self) -> Result<(), MultiRaftHostError> {
        if self.wal_health.recovery_required() {
            self.state = HostState::RecoveryRequired;
            Err(MultiRaftHostError::RecoveryRequired)
        } else {
            Ok(())
        }
    }

    fn group_error(
        &mut self,
        raft_group_id: RaftGroupId,
        error: HostedGroupError,
    ) -> MultiRaftHostError {
        if matches!(error, HostedGroupError::RecoveryRequired) {
            self.state = HostState::RecoveryRequired;
            MultiRaftHostError::RecoveryRequired
        } else {
            MultiRaftHostError::Group {
                raft_group_id,
                reason: error.to_string(),
            }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::{message::Message, types::ReplicaId as RaftReplicaId};
    use ragnordb_common::ids::ReplicaId;
    use std::{cell::Cell, rc::Rc};

    struct TestWalHealth {
        recovery_required: Rc<Cell<bool>>,
        sealed: Rc<Cell<bool>>,
    }
    impl SharedWalHealth for TestWalHealth {
        fn recovery_required(&self) -> bool {
            self.recovery_required.get()
        }
        fn seal_retention_registry(&self) -> Result<(), String> {
            self.sealed.set(true);
            Ok(())
        }
    }

    struct TestGroup {
        identity: RaftReplicaIdentity,
        stepped: usize,
        outbound: Vec<RaftMessageEnvelope>,
    }
    impl HostedRaftGroup for TestGroup {
        fn identity(&self) -> RaftReplicaIdentity {
            self.identity
        }
        fn tick(&mut self, _: u64) -> Result<(), HostedGroupError> {
            Ok(())
        }
        fn step(&mut self, _: RaftMessageEnvelope) -> Result<(), HostedGroupError> {
            self.stepped += 1;
            Ok(())
        }
        fn propose(&mut self, _: Vec<u8>, _: usize) -> Result<LogIndex, HostedGroupError> {
            Ok(0)
        }
        fn persist_and_apply(&mut self) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
            if self.stepped == 0 {
                return Err(HostedGroupError::Group(
                    "inbound message was not delivered".to_string(),
                ));
            }
            Ok(std::mem::take(&mut self.outbound))
        }
    }
    fn identity(group: u64, replica: u64) -> RaftReplicaIdentity {
        RaftReplicaIdentity::new(RaftGroupId(group), ReplicaId(replica)).unwrap()
    }
    #[test]
    fn route_demultiplexes_to_the_tagged_group_and_releases_only_its_messages() {
        let recovery_required = Rc::new(Cell::new(false));
        let mut host = MultiRaftHost::new(
            NodeId(7),
            Box::new(TestWalHealth {
                recovery_required,
                sealed: Rc::new(Cell::new(false)),
            }),
        );
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
        host.register_new_group(Box::new(TestGroup {
            identity: identity(10, 10),
            stepped: 0,
            outbound: Vec::new(),
        }))
        .unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: identity(11, 11),
            stepped: 0,
            outbound: vec![outbound.clone()],
        }))
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
    fn recovered_identities_must_be_registered_before_retention_is_sealed() {
        let recovery_required = Rc::new(Cell::new(false));
        let sealed = Rc::new(Cell::new(false));
        let recovered = identity(41, 9);
        let mut host = MultiRaftHost::with_recovered_identities(
            NodeId(7),
            Box::new(TestWalHealth {
                recovery_required,
                sealed: Rc::clone(&sealed),
            }),
            [recovered],
        );

        let new_group = || {
            Box::new(TestGroup {
                identity: recovered,
                stepped: 0,
                outbound: Vec::new(),
            }) as Box<dyn HostedRaftGroup>
        };
        assert_eq!(
            host.register_new_group(new_group()).unwrap_err(),
            MultiRaftHostError::RecoveredIdentityRequiresRecovery(recovered)
        );
        assert_eq!(
            host.activate().unwrap_err(),
            MultiRaftHostError::MissingRecovered(vec![recovered])
        );
        assert!(!sealed.get());

        host.register_recovered_group(new_group()).unwrap();
        host.activate().unwrap();
        assert!(sealed.get());
    }

    #[test]
    fn retired_recovered_replica_does_not_block_its_replacement_runtime() {
        let recovery_required = Rc::new(Cell::new(false));
        let sealed = Rc::new(Cell::new(false));
        let retired = identity(51, 8);
        let replacement = identity(51, 9);
        let mut host = MultiRaftHost::with_recovered_identities(
            NodeId(7),
            Box::new(TestWalHealth {
                recovery_required,
                sealed: Rc::clone(&sealed),
            }),
            [retired, replacement],
        );

        host.register_inactive_recovered_identity(retired).unwrap();
        host.register_recovered_group(Box::new(TestGroup {
            identity: replacement,
            stepped: 0,
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();
        assert!(sealed.get());
    }

    #[test]
    fn shared_wal_recovery_fences_every_hosted_group() {
        let recovery_required = Rc::new(Cell::new(false));
        let mut host = MultiRaftHost::new(
            NodeId(7),
            Box::new(TestWalHealth {
                recovery_required: Rc::clone(&recovery_required),
                sealed: Rc::new(Cell::new(false)),
            }),
        );
        host.register_new_group(Box::new(TestGroup {
            identity: identity(31, 31),
            stepped: 0,
            outbound: Vec::new(),
        }))
        .unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: identity(32, 32),
            stepped: 0,
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();

        recovery_required.set(true);
        assert_eq!(
            host.tick_all(1).unwrap_err(),
            MultiRaftHostError::RecoveryRequired
        );
        assert_eq!(
            host.propose(RaftGroupId(31), vec![1], 1).unwrap_err(),
            MultiRaftHostError::RecoveryRequired
        );
    }
}
