use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use bloom_bloom::BloomFilter;
use raft::{core::node::RaftNode, storage::mem::MemStorage, types::Role};
use wal::{
    config::WalConfig,
    io::directory::FsSegmentDirectory,
    lsn::Lsn,
    types::{RecordType, WalIdentity},
    wal::WalHandle,
};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();

        let path =
            std::env::temp_dir().join(format!("ragnordb-smoke-{prefix}-{}-{nanos}", process::id()));
        fs::create_dir_all(&path).expect("failed to create test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn open_wal_handle(
    dir: &TestDir,
) -> (
    WalHandle<FsSegmentDirectory, ()>,
    wal::wal::report::RecoveryReport,
) {
    let config = WalConfig {
        dir: dir.path().to_path_buf(),
        identity: WalIdentity::new(1, 1, 1),
        ..WalConfig::default()
    };

    WalHandle::open(
        FsSegmentDirectory::new(dir.path().to_path_buf()),
        config,
        (),
    )
    .expect("failed to open WAL")
}

#[test]
fn wal_open_append_sync_read_shutdown() {
    let dir = TestDir::new("wal-smoke");

    let (handle, _report) = open_wal_handle(&dir);

    let payload = b"hello ragnordb";
    let record_type = RecordType::new(1024);

    let lsn = handle.append(record_type, payload).expect("append failed");

    handle.sync().expect("sync failed");

    // LSN is a byte offset; first record starts at 0
    let record = handle.read_at(lsn).expect("read failed");
    assert_eq!(record.payload, payload);
    assert_eq!(record.record_type, record_type);

    handle.shutdown().expect("shutdown failed");
}

#[test]
fn wal_survives_restart() {
    let dir = TestDir::new("wal-restart");

    let payload = b"persistent data";
    let record_type = RecordType::new(1025);

    {
        let (handle, _report) = open_wal_handle(&dir);
        let lsn = handle.append(record_type, payload).expect("append failed");
        handle.sync().expect("sync failed");
        handle.shutdown().expect("shutdown failed");

        let _ = lsn;
    }

    {
        let (handle, report) = open_wal_handle(&dir);
        assert!(
            report.records_scanned > 0,
            "expected records to be recovered"
        );

        let first_lsn = handle
            .iter_from(Lsn::ZERO)
            .expect("iter failed")
            .next()
            .expect("record missing")
            .expect("read error");
        assert_eq!(first_lsn.payload, payload);

        handle.shutdown().expect("shutdown failed");
    }
}

type TestStorage = MemStorage<(), ()>;
type TestNode = RaftNode<(), (), TestStorage, TestStorage>;

fn new_node(id: u64, peers: Vec<u64>) -> TestNode {
    RaftNode::new(id, peers, MemStorage::new(), MemStorage::new(), 5, 2)
}

#[test]
fn raft_node_instantiates_as_follower() {
    let node = new_node(1, vec![2, 3]);

    assert_eq!(node.id(), 1);
    assert_eq!(node.role(), &Role::Follower);
    assert_eq!(node.leader_id(), None);
    assert_eq!(node.commit_index(), 0);
    assert_eq!(node.last_applied(), 0);
}

#[test]
fn raft_elects_single_leader() {
    let mut n1 = new_node(1, vec![2, 3]);
    let mut n2 = new_node(2, vec![1, 3]);
    let mut n3 = new_node(3, vec![1, 2]);

    n1.tick(n1.current_election_timeout());
    let prevotes = n1.ready().messages;

    assert_eq!(n1.role(), &Role::Candidate);

    for msg in prevotes {
        match msg.to {
            2 => n2.step(msg),
            3 => n3.step(msg),
            _ => unreachable!(),
        }
    }

    let responses: Vec<_> = n2
        .ready()
        .messages
        .into_iter()
        .chain(n3.ready().messages)
        .collect();

    for msg in responses {
        n1.step(msg);
    }

    let vote_requests = n1.ready().messages;

    for msg in vote_requests {
        match msg.to {
            2 => n2.step(msg),
            3 => n3.step(msg),
            _ => unreachable!(),
        }
    }

    let vote_responses: Vec<_> = n2
        .ready()
        .messages
        .into_iter()
        .chain(n3.ready().messages)
        .collect();

    for msg in vote_responses {
        n1.step(msg);
    }

    n1.tick(1);

    assert_eq!(n1.role(), &Role::Leader);
}

#[test]
fn bloom_filter_round_trip() {
    let mut filter = BloomFilter::with_false_positive_rate(1000, 0.01);

    filter.insert_key(b"ragnordb");
    filter.insert_key(b"test-key");
    filter.insert_str("hello");

    assert!(filter.contains_key(b"ragnordb"));
    assert!(filter.contains_key(b"test-key"));
    assert!(filter.contains_str("hello"));
    assert!(!filter.contains_key(b"not-inserted"));
}

#[test]
fn bloom_serialize_deserialize() {
    let mut filter = BloomFilter::with_false_positive_rate(100, 0.01);

    for i in 0..50u64 {
        filter.insert_key(&i.to_be_bytes());
    }

    let bytes = filter.to_bytes();
    let decoded = BloomFilter::from_bytes(&bytes).expect("decode failed");

    assert_eq!(decoded.num_bits(), filter.num_bits());
    assert_eq!(decoded.num_blocks(), filter.num_blocks());
    assert_eq!(decoded.num_hashes(), filter.num_hashes());

    for i in 0..50u64 {
        assert!(decoded.contains_key(&i.to_be_bytes()));
    }
}
