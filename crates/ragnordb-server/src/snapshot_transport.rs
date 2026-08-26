//! Node-level bounded transport for tablet snapshot images.
//!
//! A physical RagnorDB node owns exactly one snapshot listener. Incoming
//! snapshots are demultiplexed by the `(raft_group_id, replica_id)` already
//! carried by `TabletSnapshotMetadata`.
//!
//! Raft consensus traffic remains on the independent MultiRaft transport so a
//! large snapshot cannot head-of-line block elections or heartbeats.

use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::Duration,
};

use ragnordb_common::ids::{NodeId, RaftGroupId, ReplicaId};
use ragnordb_multiraft::snapshot::{
    SnapshotWorkController, TabletSnapshotReceiveSession, TabletSnapshotTransfer,
};
use ragnordb_tablet::snapshot::{
    FileTabletSnapshotStore, TabletSnapshotImage, TabletSnapshotMetadata,
};

const MAX_METADATA_BYTES: usize = 64 * 1024;
const SNAPSHOT_INBOUND_CAPACITY: usize = 8;

/// Fully received snapshot whose temporary file remains owned by the verified
/// receive session until the corresponding Raft Ready owner installs it.
pub(crate) struct ReceivedTabletSnapshot {
    pub metadata: TabletSnapshotMetadata,
    pub session: TabletSnapshotReceiveSession,
}

#[derive(Clone)]
struct SnapshotRoute {
    local_replica_id: ReplicaId,
    store: Arc<FileTabletSnapshotStore>,
    inbound: SyncSender<ReceivedTabletSnapshot>,
}

/// Lifecycle shared by every clone of the physical snapshot transport.
///
/// The listener does not own this Arc itself. Therefore the final transport
/// clone dropping means no runtime can send or receive snapshots and it is safe
/// to terminate and join the listener.
struct SnapshotListenerLifecycle {
    shutdown: Arc<AtomicBool>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl Drop for SnapshotListenerLifecycle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);

        let worker = self
            .worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();

        if let Some(worker) = worker {
            let _ = worker.join();
        }
    }
}

/// One physical snapshot transport per RagnorDB node.
///
/// Network destinations are indexed by physical `NodeId`. Local delivery is
/// indexed independently by logical `RaftGroupId` and validated against the
/// expected local `ReplicaId`.
#[derive(Clone)]
pub(crate) struct NodeSnapshotTransport {
    peers: Arc<BTreeMap<NodeId, SocketAddr>>,
    routes: Arc<RwLock<BTreeMap<RaftGroupId, SnapshotRoute>>>,
    work: SnapshotWorkController,
    max_chunk_bytes: u64,
    _lifecycle: Arc<SnapshotListenerLifecycle>,
}

/// Result of binding the one physical snapshot listener.
pub(crate) struct NodeSnapshotEndpoint {
    pub transport: NodeSnapshotTransport,
    pub local_addr: SocketAddr,
}

/// Group-scoped view of the physical snapshot transport.
///
/// The Ready owner never chooses a snapshot listener. It receives only the
/// snapshots for its logical group and uses this handle for outbound images.
pub(crate) struct GroupSnapshotEndpoint {
    raft_group_id: RaftGroupId,
    transport: NodeSnapshotTransport,
    pub inbound: Receiver<ReceivedTabletSnapshot>,
}

impl NodeSnapshotTransport {
    pub fn bind(
        local_addr: SocketAddr,
        peers: BTreeMap<NodeId, SocketAddr>,
        work: SnapshotWorkController,
        max_chunk_bytes: u64,
    ) -> io::Result<NodeSnapshotEndpoint> {
        if max_chunk_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot chunk size must be non-zero",
            ));
        }

        let listener = TcpListener::bind(local_addr)?;
        listener.set_nonblocking(true)?;

        let local_addr = listener.local_addr()?;
        let routes = Arc::new(RwLock::new(BTreeMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let worker = spawn_listener(
            listener,
            Arc::clone(&routes),
            work.clone(),
            max_chunk_bytes,
            Arc::clone(&shutdown),
        )?;

        let lifecycle = Arc::new(SnapshotListenerLifecycle {
            shutdown,
            worker: Mutex::new(Some(worker)),
        });

        Ok(NodeSnapshotEndpoint {
            transport: Self {
                peers: Arc::new(peers),
                routes,
                work,
                max_chunk_bytes,
                _lifecycle: lifecycle,
            },
            local_addr,
        })
    }

    /// Register one local Raft group's snapshot installation boundary.
    ///
    /// A group may be registered exactly once for the lifetime of this minimum
    /// Phase 5.0 host. Dynamic group removal/replacement belongs with later
    /// membership lifecycle work.
    pub fn register_group(
        &self,
        raft_group_id: RaftGroupId,
        local_replica_id: ReplicaId,
        store: Arc<FileTabletSnapshotStore>,
    ) -> io::Result<GroupSnapshotEndpoint> {
        if raft_group_id.0 == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot route cannot use Raft group ID 0",
            ));
        }

        if local_replica_id.0 == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot route cannot use replica ID 0",
            ));
        }

        let (inbound_tx, inbound_rx) = mpsc::sync_channel(SNAPSHOT_INBOUND_CAPACITY);

        let mut routes = self
            .routes
            .write()
            .map_err(|_| io::Error::other("snapshot route registry lock is poisoned"))?;

        if routes.contains_key(&raft_group_id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "snapshot route already exists for Raft group {}",
                    raft_group_id.0
                ),
            ));
        }

        routes.insert(
            raft_group_id,
            SnapshotRoute {
                local_replica_id,
                store,
                inbound: inbound_tx,
            },
        );

        Ok(GroupSnapshotEndpoint {
            raft_group_id,
            transport: self.clone(),
            inbound: inbound_rx,
        })
    }

    fn send(
        &self,
        raft_group_id: RaftGroupId,
        target_node_id: NodeId,
        target_replica_id: ReplicaId,
        source: TabletSnapshotImage,
    ) -> io::Result<()> {
        if source.metadata.raft_group_id != raft_group_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "snapshot image belongs to Raft group {}, transport is scoped to group {}",
                    source.metadata.raft_group_id.0, raft_group_id.0,
                ),
            ));
        }

        let address = self.peers.get(&target_node_id).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "snapshot target physical node {} has no configured address",
                    target_node_id.0
                ),
            )
        })?;

        let work = self.work.clone();
        let max_chunk_bytes = self.max_chunk_bytes;

        thread::Builder::new()
            .name(format!(
                "ragnordb-snapshot-send-{}-{}",
                raft_group_id.0, target_replica_id.0
            ))
            .spawn(move || {
                if let Err(error) = send_snapshot(
                    address,
                    raft_group_id,
                    target_replica_id,
                    source,
                    &work,
                    max_chunk_bytes,
                ) {
                    tracing::warn!(
                        raft_group_id = raft_group_id.0,
                        target_node_id = target_node_id.0,
                        target_replica_id = target_replica_id.0,
                        error = %error,
                        "tablet snapshot transfer failed",
                    );
                }
            })?;

        Ok(())
    }
}

impl GroupSnapshotEndpoint {
    #[allow(dead_code)]
    pub fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    /// Send one group-owned snapshot to a physical node while preserving the
    /// target Raft replica identity independently.
    pub fn send(
        &self,
        target_node_id: NodeId,
        target_replica_id: ReplicaId,
        source: TabletSnapshotImage,
    ) -> io::Result<()> {
        self.transport.send(
            self.raft_group_id,
            target_node_id,
            target_replica_id,
            source,
        )
    }
}

fn spawn_listener(
    listener: TcpListener,
    routes: Arc<RwLock<BTreeMap<RaftGroupId, SnapshotRoute>>>,
    work: SnapshotWorkController,
    max_chunk_bytes: u64,
    shutdown: Arc<AtomicBool>,
) -> io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name("ragnordb-snapshot-listener".to_string())
        .spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let routes = Arc::clone(&routes);
                        let work = work.clone();

                        // Snapshot I/O is deliberately separated from the
                        // listener so one slow snapshot cannot prevent the node
                        // from admitting another group's transfer.
                        if let Err(error) = thread::Builder::new()
                            .name("ragnordb-snapshot-receive".to_string())
                            .spawn(move || {
                                if let Err(error) =
                                    receive_snapshot(stream, &routes, &work, max_chunk_bytes)
                                {
                                    tracing::warn!(
                                        error = %error,
                                        "incoming tablet snapshot was rejected"
                                    );
                                }
                            })
                        {
                            tracing::warn!(
                                error = %error,
                                "snapshot receive worker could not be created"
                            );
                        }
                    }

                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }

                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "snapshot listener accept failed"
                        );

                        thread::sleep(Duration::from_millis(25));
                    }
                }
            }
        })
}

fn send_snapshot(
    address: SocketAddr,
    raft_group_id: RaftGroupId,
    target_replica_id: ReplicaId,
    source: TabletSnapshotImage,
    work: &SnapshotWorkController,
    max_chunk_bytes: u64,
) -> io::Result<()> {
    let TabletSnapshotImage { mut metadata, data } = source;

    if metadata.raft_group_id != raft_group_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot group changed before transfer",
        ));
    }

    // The network route is selected using NodeId. Only the target replica field
    // changes here; NodeId must never be serialized as a ReplicaId.
    metadata.replica_id = target_replica_id;

    let image = TabletSnapshotImage::new(metadata, data)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;

    let mut sender = TabletSnapshotTransfer::from_image(image)
        .and_then(|transfer| transfer.into_sender(work, max_chunk_bytes))
        .map_err(|error| io::Error::other(error.to_string()))?;

    let metadata = sender
        .metadata()
        .encode()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;

    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;

    write_frame(&mut stream, &metadata)?;

    while let Some(chunk) = sender.next_chunk() {
        write_frame(&mut stream, &chunk)?;
    }

    write_frame(&mut stream, &[])?;
    stream.flush()
}

fn receive_snapshot(
    mut stream: TcpStream,
    routes: &RwLock<BTreeMap<RaftGroupId, SnapshotRoute>>,
    work: &SnapshotWorkController,
    max_chunk_bytes: u64,
) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;

    // Metadata comes first and already carries the exact group and target
    // replica identities required for node-level demultiplexing.
    let metadata_bytes = read_frame(&mut stream, MAX_METADATA_BYTES)?;

    let metadata = TabletSnapshotMetadata::decode(&metadata_bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;

    let route = {
        let routes = routes
            .read()
            .map_err(|_| io::Error::other("snapshot route registry lock is poisoned"))?;

        routes
            .get(&metadata.raft_group_id)
            .cloned()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "no local snapshot route for Raft group {}",
                        metadata.raft_group_id.0
                    ),
                )
            })?
    };

    if metadata.replica_id != route.local_replica_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "snapshot for Raft group {} targets replica {}, \
                 but this node hosts replica {}",
                metadata.raft_group_id.0, metadata.replica_id.0, route.local_replica_id.0,
            ),
        ));
    }

    let mut session = TabletSnapshotReceiveSession::begin(
        work,
        route.store.as_ref(),
        metadata.clone(),
        max_chunk_bytes,
    )
    .map_err(|error| io::Error::other(error.to_string()))?;

    let maximum_chunk_bytes = usize::try_from(max_chunk_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "snapshot chunk size overflow"))?;

    loop {
        let chunk = read_frame(&mut stream, maximum_chunk_bytes)?;

        if chunk.is_empty() {
            break;
        }

        session
            .push_chunk(&chunk)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    }

    route
        .inbound
        .send(ReceivedTabletSnapshot { metadata, session })
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "snapshot group owner stopped"))
}

fn write_frame(stream: &mut TcpStream, bytes: &[u8]) -> io::Result<()> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "snapshot frame too large"))?;

    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(bytes)
}

fn read_frame(stream: &mut TcpStream, maximum: usize) -> io::Result<Vec<u8>> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;

    let length = u32::from_be_bytes(length) as usize;

    if length > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot frame exceeds configured bound",
        ));
    }

    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        net::TcpListener,
        time::Duration,
    };

    use ragnordb_common::ids::{NodeId, RaftGroupId, ReplicaId, TabletId};

    use ragnordb_multiraft::snapshot::{SnapshotWorkController, SnapshotWorkLimits};

    use ragnordb_tablet::snapshot::{
        AppliedTabletFrontier, FileTabletSnapshotStore, TabletSnapshotConfState,
        TabletSnapshotImage, TabletSnapshotMetadata, TabletSnapshotMetadataInput,
    };

    use super::NodeSnapshotTransport;

    fn unused_address() -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();

        listener.local_addr().unwrap()
    }

    fn image(
        group: RaftGroupId,
        source_replica: ReplicaId,
        other_replica: ReplicaId,
        tablet_id: TabletId,
    ) -> TabletSnapshotImage {
        let payload = format!("snapshot-group-{}", group.0).into_bytes();

        let conf_state = TabletSnapshotConfState::new(
            1,
            BTreeSet::from([source_replica, other_replica]),
            BTreeSet::new(),
            BTreeSet::new(),
        )
        .unwrap();

        let metadata = TabletSnapshotMetadata::for_payload(
            TabletSnapshotMetadataInput {
                cluster_id: "snapshot-multiraft-test".to_string(),
                raft_group_id: group,
                replica_id: source_replica,
                tablet_id,
                tablet_epoch: 1,
                snapshot_id: 1,
                applied_frontier: AppliedTabletFrontier::new(1, 1),
                conf_state,
            },
            &payload,
        )
        .unwrap();

        TabletSnapshotImage::new(metadata, payload).unwrap()
    }

    #[test]
    fn one_physical_listener_demultiplexes_two_groups_and_preserves_replica_identity() {
        let node_1_addr = unused_address();
        let node_2_addr = unused_address();

        let node_1_store = tempfile::tempdir().unwrap();

        let node_2_store = tempfile::tempdir().unwrap();

        let store_1 = std::sync::Arc::new(
            FileTabletSnapshotStore::new(node_1_store.path(), 16 * 1024 * 1024).unwrap(),
        );

        let store_2 = std::sync::Arc::new(
            FileTabletSnapshotStore::new(node_2_store.path(), 16 * 1024 * 1024).unwrap(),
        );

        let limits = SnapshotWorkLimits {
            max_generations: 4,
            max_sends: 4,
            max_receives: 4,
            max_installs: 4,
        };
        let node_1 = NodeSnapshotTransport::bind(
            node_1_addr,
            BTreeMap::from([(NodeId(2), node_2_addr)]),
            SnapshotWorkController::new(limits).unwrap(),
            64 * 1024,
        )
        .unwrap();

        let node_2 = NodeSnapshotTransport::bind(
            node_2_addr,
            BTreeMap::from([(NodeId(1), node_1_addr)]),
            SnapshotWorkController::new(limits).unwrap(),
            64 * 1024,
        )
        .unwrap();

        let group_10_sender = node_1
            .transport
            .register_group(RaftGroupId(10), ReplicaId(101), store_1.clone())
            .unwrap();

        let group_20_sender = node_1
            .transport
            .register_group(RaftGroupId(20), ReplicaId(301), store_1)
            .unwrap();

        let group_10_receiver = node_2
            .transport
            .register_group(RaftGroupId(10), ReplicaId(205), store_2.clone())
            .unwrap();

        let group_20_receiver = node_2
            .transport
            .register_group(RaftGroupId(20), ReplicaId(405), store_2)
            .unwrap();

        group_10_sender
            .send(
                NodeId(2),
                ReplicaId(205),
                image(
                    RaftGroupId(10),
                    ReplicaId(101),
                    ReplicaId(205),
                    TabletId(10),
                ),
            )
            .unwrap();

        group_20_sender
            .send(
                NodeId(2),
                ReplicaId(405),
                image(
                    RaftGroupId(20),
                    ReplicaId(301),
                    ReplicaId(405),
                    TabletId(20),
                ),
            )
            .unwrap();

        let received_10 = group_10_receiver
            .inbound
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let received_20 = group_20_receiver
            .inbound
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        assert_eq!(received_10.metadata.raft_group_id, RaftGroupId(10),);

        assert_eq!(
            received_10.metadata.replica_id,
            ReplicaId(205),
            "physical NodeId(2) must never overwrite ReplicaId(205)",
        );

        assert_eq!(received_20.metadata.raft_group_id, RaftGroupId(20),);

        assert_eq!(received_20.metadata.replica_id, ReplicaId(405),);
    }
}
