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
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError},
    },
    thread,
    time::{Duration, Instant},
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

const MULTIRAFT_WIRE_VERSION: u8 = 1;
const MULTIRAFT_RAFT_MESSAGE_TYPE: u8 = 0x01;
const MULTIRAFT_FRAME_HEADER_BYTES: usize = 14;
const MAX_MULTIRAFT_FRAME_BYTES: usize = 64 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// Node-local limits for the physical MultiRaft transport.
///
/// The two receive lanes have independent byte and message budgets. A burst
/// of append or snapshot-control traffic therefore cannot consume the queue
/// reserved for elections and heartbeats. The frame limit is checked before a
/// payload buffer is allocated by the listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeRaftTransportConfig {
    pub max_frame_bytes: usize,
    pub control_queue_capacity: usize,
    pub bulk_queue_capacity: usize,
    pub control_queue_bytes: usize,
    pub bulk_queue_bytes: usize,
}

impl Default for NodeRaftTransportConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_MULTIRAFT_FRAME_BYTES,
            control_queue_capacity: 256,
            bulk_queue_capacity: 1_024,
            control_queue_bytes: 256 * 1024,
            bulk_queue_bytes: MAX_MULTIRAFT_FRAME_BYTES + MULTIRAFT_FRAME_HEADER_BYTES,
        }
    }
}

impl NodeRaftTransportConfig {
    fn validate(self) -> io::Result<()> {
        if self.max_frame_bytes == 0 || self.max_frame_bytes > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MultiRaft frame limit must fit a non-zero u32 length",
            ));
        }

        if self.control_queue_capacity == 0
            || self.bulk_queue_capacity == 0
            || self.control_queue_bytes == 0
            || self.bulk_queue_bytes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MultiRaft transport queue limits must be non-zero",
            ));
        }

        Ok(())
    }
}

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
    pub inbound: NodeRaftInbound,
    pub local_addr: SocketAddr,
}

/// Priority-aware receive side of the physical-node transport.
///
/// Control traffic is always checked before bulk traffic. The receive methods
/// return only the decoded Raft envelope; byte reservations are released at
/// that exact handoff, so queued memory remains bounded until the host owns the
/// message.
pub struct NodeRaftInbound {
    control: InboundQueueReceiver,
    bulk: InboundQueueReceiver,
}

impl NodeRaftInbound {
    pub fn try_recv(&self) -> Result<RoutedRaftMessage, TryRecvError> {
        match self.control.try_recv() {
            Ok(message) => Ok(message),
            Err(TryRecvError::Disconnected) => self.bulk.try_recv(),
            Err(TryRecvError::Empty) => self.bulk.try_recv(),
        }
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<RoutedRaftMessage, RecvTimeoutError> {
        let deadline = Instant::now().checked_add(timeout);

        loop {
            match self.try_recv() {
                Ok(message) => return Ok(message),
                Err(TryRecvError::Disconnected) => return Err(RecvTimeoutError::Disconnected),
                Err(TryRecvError::Empty) => {}
            }

            let Some(deadline) = deadline else {
                thread::yield_now();
                continue;
            };

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }

            thread::sleep(remaining.min(Duration::from_millis(1)));
        }
    }

    pub fn recv(&self) -> Result<RoutedRaftMessage, mpsc::RecvError> {
        loop {
            match self.try_recv() {
                Ok(message) => return Ok(message),
                Err(TryRecvError::Disconnected) => return Err(mpsc::RecvError),
                Err(TryRecvError::Empty) => thread::sleep(Duration::from_millis(1)),
            }
        }
    }
}

struct QueuedInbound {
    message: RoutedRaftMessage,
    wire_bytes: usize,
}

#[derive(Clone)]
struct InboundQueueSender {
    sender: SyncSender<QueuedInbound>,
    available_bytes: Arc<std::sync::atomic::AtomicUsize>,
    max_bytes: usize,
}

struct InboundQueueReceiver {
    receiver: Receiver<QueuedInbound>,
    available_bytes: Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Clone)]
struct InboundSenders {
    control: InboundQueueSender,
    bulk: InboundQueueSender,
}

impl InboundSenders {
    fn try_send(&self, message: RoutedRaftMessage, wire_bytes: usize) -> io::Result<()> {
        if crate::host::is_control_message(&message.envelope) {
            self.control.try_send(message, wire_bytes)
        } else {
            self.bulk.try_send(message, wire_bytes)
        }
    }
}

impl InboundQueueSender {
    fn try_send(&self, message: RoutedRaftMessage, wire_bytes: usize) -> io::Result<()> {
        if wire_bytes > self.max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MultiRaft transport queue cannot admit this message",
            ));
        }

        self.reserve(wire_bytes)?;

        match self.sender.try_send(QueuedInbound {
            message,
            wire_bytes,
        }) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.release(wire_bytes);
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "MultiRaft transport queue is full",
                ))
            }
            Err(TrySendError::Disconnected(_)) => {
                self.release(wire_bytes);
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "MultiRaft inbound receiver has stopped",
                ))
            }
        }
    }

    fn reserve(&self, wire_bytes: usize) -> io::Result<()> {
        let mut available = self.available_bytes.load(Ordering::Acquire);

        loop {
            if available < wire_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "MultiRaft transport byte budget is full",
                ));
            }

            match self.available_bytes.compare_exchange(
                available,
                available - wire_bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(updated) => available = updated,
            }
        }
    }

    fn release(&self, wire_bytes: usize) {
        self.available_bytes
            .fetch_add(wire_bytes, Ordering::Release);
    }
}

impl InboundQueueReceiver {
    fn try_recv(&self) -> Result<RoutedRaftMessage, TryRecvError> {
        self.receiver.try_recv().map(|queued| {
            self.available_bytes
                .fetch_add(queued.wire_bytes, Ordering::Release);
            queued.message
        })
    }
}

fn inbound_queues(config: NodeRaftTransportConfig) -> (InboundSenders, NodeRaftInbound) {
    let (control_sender, control_receiver) = mpsc::sync_channel(config.control_queue_capacity);
    let (bulk_sender, bulk_receiver) = mpsc::sync_channel(config.bulk_queue_capacity);

    let control_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(
        config.control_queue_bytes,
    ));
    let bulk_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(config.bulk_queue_bytes));

    (
        InboundSenders {
            control: InboundQueueSender {
                sender: control_sender,
                available_bytes: Arc::clone(&control_bytes),
                max_bytes: config.control_queue_bytes,
            },
            bulk: InboundQueueSender {
                sender: bulk_sender,
                available_bytes: Arc::clone(&bulk_bytes),
                max_bytes: config.bulk_queue_bytes,
            },
        },
        NodeRaftInbound {
            control: InboundQueueReceiver {
                receiver: control_receiver,
                available_bytes: control_bytes,
            },
            bulk: InboundQueueReceiver {
                receiver: bulk_receiver,
                available_bytes: bulk_bytes,
            },
        },
    )
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
    max_frame_bytes: usize,
    connections: Arc<Mutex<BTreeMap<NodeId, TcpStream>>>,

    // Used for a rare local destination without unnecessarily entering TCP.
    loopback: InboundSenders,

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
        Self::bind_with_config(
            local_node_id,
            bind_addr,
            node_addresses,
            NodeRaftTransportConfig::default(),
        )
    }

    pub fn bind_with_config(
        local_node_id: NodeId,
        bind_addr: SocketAddr,
        node_addresses: BTreeMap<NodeId, SocketAddr>,
        config: NodeRaftTransportConfig,
    ) -> io::Result<NodeRaftEndpoint> {
        if local_node_id.0 == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "physical node ID 0 is reserved",
            ));
        }
        config.validate()?;

        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;

        let codec = ByteEnvelopeCodec::new(BytesCodec, BytesCodec);
        let (inbound_tx, inbound_rx) = inbound_queues(config);

        let shutdown = Arc::new(AtomicBool::new(false));

        let worker = spawn_listener(
            listener,
            inbound_tx.clone(),
            codec.clone(),
            Arc::clone(&shutdown),
            config.max_frame_bytes,
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
            max_frame_bytes: config.max_frame_bytes,
            connections: Arc::new(Mutex::new(BTreeMap::new())),
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

        let payload = encode_routed_message(&self.codec, &message)?;

        if payload.len() - MULTIRAFT_FRAME_HEADER_BYTES > self.max_frame_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MultiRaft frame exceeds maximum size",
            ));
        }

        if target_node == self.local_node_id {
            return self.loopback.try_send(message, payload.len());
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

        let mut connections = self
            .connections
            .lock()
            .map_err(|_| io::Error::other("MultiRaft connection registry lock is poisoned"))?;
        let stream = match connections.entry(target_node) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let stream = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)?;
                stream.set_nodelay(true)?;
                stream.set_write_timeout(Some(Duration::from_secs(2)))?;
                entry.insert(stream)
            }
        };

        if let Err(error) =
            write_frame(stream, &payload, self.max_frame_bytes).and_then(|()| stream.flush())
        {
            connections.remove(&target_node);
            return Err(error);
        }

        Ok(())
    }

    pub fn try_send_all(
        &self,
        messages: impl IntoIterator<Item = RoutedRaftMessage>,
    ) -> io::Result<()> {
        let mut messages = messages.into_iter().collect::<Vec<_>>();
        // Stable sorting preserves the producer's FIFO order within each lane
        // while ensuring elections and heartbeats are attempted before bulk
        // append traffic when a node is under pressure.
        messages.sort_by_key(|message| {
            usize::from(!crate::host::is_control_message(&message.envelope))
        });

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

    let inner_length = u32::try_from(inner.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "MultiRaft payload length exceeds u32",
        )
    })?;

    let mut payload = Vec::with_capacity(MULTIRAFT_FRAME_HEADER_BYTES + inner.len());
    payload.push(MULTIRAFT_WIRE_VERSION);
    payload.push(MULTIRAFT_RAFT_MESSAGE_TYPE);
    payload.extend_from_slice(&message.raft_group_id.0.to_le_bytes());
    payload.extend_from_slice(&inner_length.to_le_bytes());
    payload.extend_from_slice(&inner);

    Ok(payload)
}

pub(crate) fn routed_message_wire_size(message: &RoutedRaftMessage) -> io::Result<usize> {
    let codec = ByteEnvelopeCodec::new(BytesCodec, BytesCodec);
    encode_routed_message(&codec, message).map(|payload| payload.len())
}

fn decode_routed_message(
    codec: &ByteEnvelopeCodec,
    payload: &[u8],
) -> io::Result<RoutedRaftMessage> {
    if payload.len() < MULTIRAFT_FRAME_HEADER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MultiRaft frame is shorter than its fixed header",
        ));
    }

    let version = payload[0];

    if version != MULTIRAFT_WIRE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported MultiRaft wire version {version}"),
        ));
    }

    if payload[1] != MULTIRAFT_RAFT_MESSAGE_TYPE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported MultiRaft message type {}", payload[1]),
        ));
    }

    let raft_group_id = RaftGroupId(u64::from_le_bytes(
        payload[2..10]
            .try_into()
            .expect("fixed-size group ID slice"),
    ));

    if raft_group_id.0 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft group ID 0 is reserved",
        ));
    }

    let declared_length = u32::from_le_bytes(
        payload[10..14]
            .try_into()
            .expect("fixed-size payload length slice"),
    ) as usize;
    if declared_length != payload.len() - MULTIRAFT_FRAME_HEADER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MultiRaft frame payload length does not match its header",
        ));
    }

    let envelope = codec.decode_envelope(&payload[MULTIRAFT_FRAME_HEADER_BYTES..])?;

    Ok(RoutedRaftMessage {
        raft_group_id,
        envelope,
    })
}

fn spawn_listener(
    listener: TcpListener,
    inbound: InboundSenders,
    codec: ByteEnvelopeCodec,
    shutdown: Arc<AtomicBool>,
    max_frame_bytes: usize,
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
                                if let Err(error) =
                                    handle_connection(stream, inbound, codec, max_frame_bytes)
                                {
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
    inbound: InboundSenders,
    codec: ByteEnvelopeCodec,
    max_frame_bytes: usize,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;

    loop {
        let Some(payload) = read_frame(&mut stream, max_frame_bytes)? else {
            return Ok(());
        };

        let message = decode_routed_message(&codec, &payload)?;

        let control = crate::host::is_control_message(&message.envelope);
        let queue = if control {
            &inbound.control
        } else {
            &inbound.bulk
        };
        queue.try_send(message, payload.len())?;
    }
}

fn write_frame(stream: &mut TcpStream, payload: &[u8], max_frame_bytes: usize) -> io::Result<()> {
    if payload.len() < MULTIRAFT_FRAME_HEADER_BYTES
        || payload.len() - MULTIRAFT_FRAME_HEADER_BYTES > max_frame_bytes
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "MultiRaft frame exceeds maximum size",
        ));
    }

    stream.write_all(payload)?;

    Ok(())
}

fn read_frame(stream: &mut TcpStream, max_frame_bytes: usize) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0_u8; MULTIRAFT_FRAME_HEADER_BYTES];

    match stream.read_exact(&mut header) {
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

    let length = u32::from_le_bytes(
        header[10..14]
            .try_into()
            .expect("fixed-size payload length slice"),
    ) as usize;

    if header[0] != MULTIRAFT_WIRE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported MultiRaft wire version {}", header[0]),
        ));
    }

    if header[1] != MULTIRAFT_RAFT_MESSAGE_TYPE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported MultiRaft message type {}", header[1]),
        ));
    }

    if length == 0 || length > max_frame_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid MultiRaft frame length {length}"),
        ));
    }

    let mut payload = Vec::with_capacity(MULTIRAFT_FRAME_HEADER_BYTES + length);
    payload.extend_from_slice(&header);
    payload.resize(MULTIRAFT_FRAME_HEADER_BYTES + length, 0);
    stream.read_exact(&mut payload[MULTIRAFT_FRAME_HEADER_BYTES..])?;

    Ok(Some(payload))
}
