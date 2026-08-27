//! physical node Raft transport for multiple independent Raft groups
//!
//! The reusable Raft crate intentionally knows only replica identities.
//! RagnorDB owns the additional `(raft_group_id, replica_id) -> node_id`
//! routing layer required when one process hosts many independent groups.

use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread,
    time::Duration,
};

use raft::{
    message::Envelope,
    runtime::transport_tcp::TcpEnvelopeCodec,
    storage::codec::{CommandCodec, SnapshotCodec},
};
use ragnordb_common::{
    ids::{NodeId, RaftGroupId, ReplicaId},
    raft_bootstrap::RaftGroupBootstrap,
};

use crate::host::RoutedRaftMessage;

const MULTIRAFT_WIRE_MAGIC: &[u8; 4] = b"RMR1";
const MULTIRAFT_WIRE_VERSION: u16 = 1;
const MAX_MULTIRAFT_FRAME_BYTES: usize = 64 * 1024 * 1024 + 14;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// Identity codec for already serialized database command/snapshot bytes.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesCodec;

impl CommandCodec<Vec<u8>> for BytesCodec {
    fn encode(&self, command: &Vec<u8>) -> io::Result<Vec<u8>> {
        Ok(command.clone())
    }

    fn decode(&self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

impl SnapshotCodec<Vec<u8>> for BytesCodec {
    fn encode(&self, snapshot: &Vec<u8>) -> io::Result<Vec<u8>> {
        Ok(snapshot.clone())
    }

    fn decode(&self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

type ByteEnvelopeCodec = TcpEnvelopeCodec<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>;

/// One physical-node endpoint shared by every Raft group on that node.
pub struct NodeRaftEndpoint {
    pub transport: NodeRaftTransport,
    pub inbound: Receiver<RoutedRaftMessage>,
    pub local_addr: SocketAddr,
}

struct NodeRaftListenerLifecycle {
    shutdown: Arc<AtomicBool>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl Drop for NodeRaftListenerLifecycle {
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

/// Cloneable physical-node transport.
///
/// `routes` is group-qualified because the same ReplicaId may legitimately
/// occur in different Raft groups.
#[derive(Clone)]
pub struct NodeRaftTransport {
    local_node_id: NodeId,
    node_addresses: Arc<BTreeMap<NodeId, SocketAddr>>,
    routes: Arc<RwLock<BTreeMap<(RaftGroupId, ReplicaId), NodeId>>>,
    codec: ByteEnvelopeCodec,

    // Used for a rare local destination without unnecessarily entering TCP.
    loopback: Sender<RoutedRaftMessage>,

    /// Keeps the one physical listener alive while any node/group transport
    /// handle still exists and joins it when the final handle disappears.
    _lifecycle: Arc<NodeRaftListenerLifecycle>,
}

impl NodeRaftTransport {
    pub fn bind(
        local_node_id: NodeId,
        bind_addr: SocketAddr,
        node_addresses: BTreeMap<NodeId, SocketAddr>,
    ) -> io::Result<NodeRaftEndpoint> {
        if local_node_id.0 == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "physical node ID 0 is reserved",
            ));
        }

        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;

        let codec = ByteEnvelopeCodec::new(BytesCodec, BytesCodec);
        let (inbound_tx, inbound_rx) = mpsc::channel();

        let shutdown = Arc::new(AtomicBool::new(false));

        let worker = spawn_listener(
            listener,
            inbound_tx.clone(),
            codec.clone(),
            Arc::clone(&shutdown),
        )?;

        let lifecycle = Arc::new(NodeRaftListenerLifecycle {
            shutdown,
            worker: Mutex::new(Some(worker)),
        });

        let transport = Self {
            local_node_id,
            node_addresses: Arc::new(node_addresses),
            routes: Arc::new(RwLock::new(BTreeMap::new())),
            codec,
            loopback: inbound_tx,
            _lifecycle: lifecycle,
        };

        Ok(NodeRaftEndpoint {
            transport,
            inbound: inbound_rx,
            local_addr,
        })
    }

    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Register the durable replica-to-node mapping for one Raft group.
    ///
    /// Exact replay is harmless. Conflicting routing authority is rejected.
    pub fn register_group(&self, bootstrap: &RaftGroupBootstrap) -> io::Result<GroupRaftTransport> {
        bootstrap
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;

        let mut routes = self
            .routes
            .write()
            .map_err(|_| io::Error::other("MultiRaft route registry lock is poisoned"))?;

        for (replica_id, node_id) in &bootstrap.replica_to_node {
            let key = (bootstrap.raft_group_id, *replica_id);

            match routes.get(&key) {
                Some(existing) if existing != node_id => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!(
                            "conflicting route for Raft group {} replica {}: \
                             node {} vs node {}",
                            bootstrap.raft_group_id.0, replica_id.0, existing.0, node_id.0,
                        ),
                    ));
                }

                Some(_) => {}

                None => {
                    routes.insert(key, *node_id);
                }
            }
        }

        Ok(GroupRaftTransport {
            raft_group_id: bootstrap.raft_group_id,
            transport: self.clone(),
        })
    }

    pub fn try_send(&self, message: RoutedRaftMessage) -> io::Result<()> {
        let target_replica = ReplicaId::from_raft(message.envelope.to);

        let target_node = {
            let routes = self
                .routes
                .read()
                .map_err(|_| io::Error::other("MultiRaft route registry lock is poisoned"))?;

            routes
                .get(&(message.raft_group_id, target_replica))
                .copied()
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "no physical route for Raft group {} replica {}",
                            message.raft_group_id.0, target_replica.0,
                        ),
                    )
                })?
        };

        if target_node == self.local_node_id {
            return self.loopback.send(message).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "local MultiRaft receiver has stopped",
                )
            });
        }

        let address = self
            .node_addresses
            .get(&target_node)
            .copied()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "no Raft address configured for physical node {}",
                        target_node.0
                    ),
                )
            })?;

        let payload = encode_routed_message(&self.codec, &message)?;

        let mut stream = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)?;
        stream.set_nodelay(true)?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;

        write_frame(&mut stream, &payload)?;
        stream.flush()?;

        Ok(())
    }

    pub fn try_send_all(
        &self,
        messages: impl IntoIterator<Item = RoutedRaftMessage>,
    ) -> io::Result<()> {
        let mut first_error = None;

        for message in messages {
            if let Err(error) = self.try_send(message)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn target_node(
        &self,
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    ) -> io::Result<NodeId> {
        self.routes
            .read()
            .map_err(|_| io::Error::other("MultiRaft route registry lock is poisoned"))?
            .get(&(raft_group_id, replica_id))
            .copied()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "no physical route for Raft group {} replica {}",
                        raft_group_id.0, replica_id.0,
                    ),
                )
            })
    }
}

/// Group-scoped view of the physical transport.
///
/// Group workers never manually attach a group ID to an outbound Raft
/// envelope. The scoped handle does that at the transport boundary.
#[derive(Clone)]
pub struct GroupRaftTransport {
    raft_group_id: RaftGroupId,
    transport: NodeRaftTransport,
}

impl GroupRaftTransport {
    pub fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    pub fn local_node_id(&self) -> NodeId {
        self.transport.local_node_id()
    }

    pub fn target_node_for_replica(&self, replica_id: ReplicaId) -> io::Result<NodeId> {
        self.transport.target_node(self.raft_group_id, replica_id)
    }

    pub fn try_send(&self, envelope: Envelope<Vec<u8>, Vec<u8>>) -> io::Result<()> {
        self.transport.try_send(RoutedRaftMessage {
            raft_group_id: self.raft_group_id,
            envelope,
        })
    }
}

fn encode_routed_message(
    codec: &ByteEnvelopeCodec,
    message: &RoutedRaftMessage,
) -> io::Result<Vec<u8>> {
    if message.raft_group_id.0 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft group ID 0 is reserved",
        ));
    }

    let inner = codec.encode_envelope(&message.envelope)?;

    let mut payload = Vec::with_capacity(14 + inner.len());
    payload.extend_from_slice(MULTIRAFT_WIRE_MAGIC);
    payload.extend_from_slice(&MULTIRAFT_WIRE_VERSION.to_be_bytes());
    payload.extend_from_slice(&message.raft_group_id.0.to_be_bytes());
    payload.extend_from_slice(&inner);

    Ok(payload)
}

fn decode_routed_message(
    codec: &ByteEnvelopeCodec,
    payload: &[u8],
) -> io::Result<RoutedRaftMessage> {
    if payload.len() < 14 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MultiRaft frame is shorter than its fixed header",
        ));
    }

    if &payload[..4] != MULTIRAFT_WIRE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid MultiRaft wire magic",
        ));
    }

    let version = u16::from_be_bytes([payload[4], payload[5]]);

    if version != MULTIRAFT_WIRE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported MultiRaft wire version {version}"),
        ));
    }

    let raft_group_id = RaftGroupId(u64::from_be_bytes(
        payload[6..14]
            .try_into()
            .expect("fixed-size group ID slice"),
    ));

    if raft_group_id.0 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft group ID 0 is reserved",
        ));
    }

    let envelope = codec.decode_envelope(&payload[14..])?;

    Ok(RoutedRaftMessage {
        raft_group_id,
        envelope,
    })
}

fn spawn_listener(
    listener: TcpListener,
    inbound: Sender<RoutedRaftMessage>,
    codec: ByteEnvelopeCodec,
    shutdown: Arc<AtomicBool>,
) -> io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name("ragnordb-multiraft-listener".to_string())
        .spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let inbound = inbound.clone();
                        let codec = codec.clone();

                        if let Err(error) = thread::Builder::new()
                            .name("ragnordb-multiraft-connection".to_string())
                            .spawn(move || {
                                if let Err(error) = handle_connection(stream, inbound, codec) {
                                    tracing::debug!(
                                        error = %error,
                                        "MultiRaft connection closed with error",
                                    );
                                }
                            })
                        {
                            tracing::warn!(
                                error = %error,
                                "MultiRaft connection worker could not be created",
                            );
                        }
                    }

                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }

                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "MultiRaft listener accept failed",
                        );

                        thread::sleep(Duration::from_millis(25));
                    }
                }
            }
        })
}

fn handle_connection(
    mut stream: TcpStream,
    inbound: Sender<RoutedRaftMessage>,
    codec: ByteEnvelopeCodec,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;

    loop {
        let Some(payload) = read_frame(&mut stream)? else {
            return Ok(());
        };

        let message = decode_routed_message(&codec, &payload)?;

        inbound.send(message).map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "MultiRaft inbound receiver has stopped",
            )
        })?;
    }
}

fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    if payload.len() > MAX_MULTIRAFT_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "MultiRaft frame exceeds maximum size",
        ));
    }

    let length = u32::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "MultiRaft frame length exceeds u32",
        )
    })?;

    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(payload)?;

    Ok(())
}

fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut length_bytes = [0_u8; 4];

    match stream.read_exact(&mut length_bytes) {
        Ok(()) => {}

        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
            ) =>
        {
            return Ok(None);
        }

        Err(error) => return Err(error),
    }

    let length = u32::from_be_bytes(length_bytes) as usize;

    if length == 0 || length > MAX_MULTIRAFT_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid MultiRaft frame length {length}"),
        ));
    }

    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload)?;

    Ok(Some(payload))
}
