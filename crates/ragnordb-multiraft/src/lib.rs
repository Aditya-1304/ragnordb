use ragnordb_common::ids::NodeId;

pub mod bootstrap;
pub mod host;
pub mod proposal;
pub mod replica_startup;
pub mod runtime;
pub mod snapshot;
pub mod storage;
pub mod tablet_apply;
pub mod tablet_cluster;

/// Stable identity of one physical database node.
///
/// [`host::MultiRaftHost`] owns the registered replica runtimes and their
/// shared-WAL lifecycle; this lightweight value remains the node identity used
/// by bootstrap and transport configuration.
#[derive(Debug, Clone)]
pub struct MultiRaftNode {
    pub id: NodeId,
}
