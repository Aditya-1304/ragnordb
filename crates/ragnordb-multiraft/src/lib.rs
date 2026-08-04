use ragnordb_common::ids::NodeId;

pub mod bootstrap;
pub mod runtime;
pub mod storage;

/// Hosts all Raft group replicas assigned to one physical database node.
///
/// Group registration must pass through the exactly-once bootstrap path before
/// a runtime enters the active group map. The scheduler and A-WAL-backed group
/// runtime are introduced in the following Milestone 4 phases.
#[derive(Debug, Clone)]
pub struct MultiRaftNode {
    pub id: NodeId,
}
