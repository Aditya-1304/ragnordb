//! Bounded out-of-band transport for production tablet snapshot images.
//!
//! Raft carries immutable snapshot metadata only. This transport streams the
//! matching database image on a dedicated endpoint so a large catch-up cannot
//! head-of-line block elections, heartbeats, or append responses.

use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::Duration,
};

use ragnordb_common::ids::ReplicaId;
use ragnordb_multiraft::snapshot::{
    SnapshotWorkController, TabletSnapshotReceiveSession, TabletSnapshotTransfer,
};
use ragnordb_tablet::snapshot::{
    FileTabletSnapshotStore, TabletSnapshotImage, TabletSnapshotMetadata,
};

const MAX_METADATA_BYTES: usize = 64 * 1024;

/// Fully received stream whose temporary file remains owned by the verified
/// tablet receiver until the Ready owner performs the durable install.
pub(crate) struct ReceivedTabletSnapshot {
    pub metadata: TabletSnapshotMetadata,
    pub session: TabletSnapshotReceiveSession,
}

/// Cloneable sender plus the sole inbound queue for one snapshot endpoint.
pub(crate) struct SnapshotEndpoint {
    peers: Arc<BTreeMap<u64, SocketAddr>>,
    work: SnapshotWorkController,
    max_chunk_bytes: u64,
    pub inbound: Receiver<ReceivedTabletSnapshot>,
}

impl SnapshotEndpoint {
    pub fn bind(
        local_addr: SocketAddr,
        peers: BTreeMap<u64, SocketAddr>,
        store: Arc<FileTabletSnapshotStore>,
        work: SnapshotWorkController,
        max_chunk_bytes: u64,
        shutdown: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(local_addr)?;
        listener.set_nonblocking(true)?;
        let (sender, inbound) = mpsc::sync_channel(8);
        let receiver_work = work.clone();

        thread::Builder::new()
            .name("ragnordb-snapshot-listener".to_string())
            .spawn(move || {
                while !shutdown.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            if let Err(error) = receive_snapshot(
                                stream,
                                &store,
                                &receiver_work,
                                max_chunk_bytes,
                                &sender,
                            ) {
                                tracing::warn!(error = %error, "incoming tablet snapshot was rejected");
                            }
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "snapshot listener accept failed");
                            thread::sleep(Duration::from_millis(25));
                        }
                    }
                }
            })?;

        Ok(Self {
            peers: Arc::new(peers),
            work,
            max_chunk_bytes,
            inbound,
        })
    }

    /// Start a bounded transfer without blocking the Ready owner on network
    /// backpressure. The Raft metadata message is sent independently and the
    /// receiver matches both halves before installation.
    pub fn send(&self, target_replica_id: u64, source: TabletSnapshotImage) {
        let Some(address) = self.peers.get(&target_replica_id).copied() else {
            tracing::warn!(
                target_replica_id,
                "snapshot target is not a configured peer"
            );
            return;
        };
        let work = self.work.clone();
        let max_chunk_bytes = self.max_chunk_bytes;
        thread::spawn(move || {
            if let Err(error) = send_snapshot(
                address,
                ReplicaId(target_replica_id),
                source,
                &work,
                max_chunk_bytes,
            ) {
                tracing::warn!(target_replica_id, error = %error, "tablet snapshot transfer failed");
            }
        });
    }
}

fn send_snapshot(
    address: SocketAddr,
    target_replica_id: ReplicaId,
    source: TabletSnapshotImage,
    work: &SnapshotWorkController,
    max_chunk_bytes: u64,
) -> io::Result<()> {
    let mut metadata = source.metadata;
    metadata.replica_id = target_replica_id;
    let image = TabletSnapshotImage::new(metadata, source.data)
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
    store: &FileTabletSnapshotStore,
    work: &SnapshotWorkController,
    max_chunk_bytes: u64,
    sender: &SyncSender<ReceivedTabletSnapshot>,
) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    let metadata_bytes = read_frame(&mut stream, MAX_METADATA_BYTES)?;
    let metadata = TabletSnapshotMetadata::decode(&metadata_bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    let mut session =
        TabletSnapshotReceiveSession::begin(work, store, metadata.clone(), max_chunk_bytes)
            .map_err(|error| io::Error::other(error.to_string()))?;
    let max_chunk_bytes = usize::try_from(max_chunk_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "snapshot chunk size overflow"))?;

    loop {
        let chunk = read_frame(&mut stream, max_chunk_bytes)?;
        if chunk.is_empty() {
            break;
        }
        session
            .push_chunk(&chunk)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    }

    sender
        .send(ReceivedTabletSnapshot { metadata, session })
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "snapshot owner stopped"))
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
