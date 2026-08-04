use std::{
    collections::{BTreeMap, BTreeSet},
    fs, process,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use ragnordb_common::{
    ids::{NodeId, RaftGroupId, ReplicaId},
    raft_bootstrap::{RaftGroupBootstrap, RaftGroupBootstrapError},
};
use ragnordb_multiraft::bootstrap::{
    BootstrapGroupError, BootstrapOutcome, BootstrapStore, BootstrapStoreError,
    BootstrapStoreInstall, FileBootstrapStore, bootstrap_group_exactly_once,
};

#[derive(Default)]
struct DurableBootstrapState {
    records: Mutex<BTreeMap<RaftGroupId, Vec<u8>>>,
    successful_installs: AtomicUsize,
}

struct MemoryBootstrapStore {
    durable: Arc<DurableBootstrapState>,
    return_unknown_after_persist: bool,
}

impl MemoryBootstrapStore {
    fn new() -> Self {
        Self {
            durable: Arc::new(DurableBootstrapState::default()),
            return_unknown_after_persist: false,
        }
    }

    fn reopen(&self) -> Self {
        Self {
            durable: Arc::clone(&self.durable),
            return_unknown_after_persist: false,
        }
    }

    fn with_unknown_after_persist(mut self) -> Self {
        self.return_unknown_after_persist = true;
        self
    }

    fn successful_installs(&self) -> usize {
        self.durable.successful_installs.load(Ordering::SeqCst)
    }
}

impl BootstrapStore for MemoryBootstrapStore {
    fn load_bootstrap(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<Option<Vec<u8>>, BootstrapStoreError> {
        let records = self.durable.records.lock().map_err(|_| {
            BootstrapStoreError::Unavailable("bootstrap test store mutex is poisoned".to_owned())
        })?;

        Ok(records.get(&raft_group_id).cloned())
    }

    fn install_bootstrap_and_sync(
        &mut self,
        raft_group_id: RaftGroupId,
        encoded_bootstrap: &[u8],
    ) -> Result<BootstrapStoreInstall, BootstrapStoreError> {
        let mut records = self.durable.records.lock().map_err(|_| {
            BootstrapStoreError::Unavailable("bootstrap test store mutex is poisoned".to_owned())
        })?;

        if let Some(existing) = records.get(&raft_group_id) {
            return Ok(BootstrapStoreInstall::AlreadyExists(existing.clone()));
        }

        records.insert(raft_group_id, encoded_bootstrap.to_vec());
        self.durable
            .successful_installs
            .fetch_add(1, Ordering::SeqCst);

        if self.return_unknown_after_persist {
            self.return_unknown_after_persist = false;
            return Err(BootstrapStoreError::OutcomeUnknown(
                "sync acknowledgement was lost after persistence".to_owned(),
            ));
        }

        Ok(BootstrapStoreInstall::Installed)
    }
}

fn bootstrap() -> RaftGroupBootstrap {
    RaftGroupBootstrap::new(
        "ragnordb-dev".to_owned(),
        RaftGroupId(100),
        1,
        BTreeMap::from([
            (ReplicaId(11), NodeId(1)),
            (ReplicaId(12), NodeId(2)),
            (ReplicaId(13), NodeId(3)),
        ]),
        BTreeSet::from([ReplicaId(11), ReplicaId(12), ReplicaId(13)]),
        BTreeSet::new(),
    )
    .expect("valid bootstrap")
}

#[test]
fn identical_bootstrap_after_restart_is_idempotent() {
    let requested = bootstrap();
    let mut first_process = MemoryBootstrapStore::new();

    assert_eq!(
        bootstrap_group_exactly_once(&mut first_process, &requested)
            .expect("first bootstrap should succeed"),
        BootstrapOutcome::Installed
    );

    let mut restarted_process = first_process.reopen();

    assert_eq!(
        bootstrap_group_exactly_once(&mut restarted_process, &requested)
            .expect("identical restart bootstrap should succeed"),
        BootstrapOutcome::AlreadyInstalled
    );

    assert_eq!(restarted_process.successful_installs(), 1);
}

#[test]
fn restart_rejects_changed_static_membership() {
    let requested = bootstrap();
    let mut first_process = MemoryBootstrapStore::new();

    bootstrap_group_exactly_once(&mut first_process, &requested)
        .expect("first bootstrap should succeed");

    let mut changed = requested.clone();
    changed.replica_to_node.insert(ReplicaId(12), NodeId(99));

    let mut restarted_process = first_process.reopen();
    let error = bootstrap_group_exactly_once(&mut restarted_process, &changed)
        .expect_err("changed bootstrap must be rejected");

    assert!(matches!(
        error,
        BootstrapGroupError::Envelope(RaftGroupBootstrapError::BootstrapConflict { .. })
    ));
    assert_eq!(restarted_process.successful_installs(), 1);
}

#[test]
fn uncertain_first_install_is_resolved_from_restart_state() {
    let requested = bootstrap();
    let initial_store = MemoryBootstrapStore::new();
    let durable_state = Arc::clone(&initial_store.durable);
    let mut first_process = initial_store.with_unknown_after_persist();

    let error = bootstrap_group_exactly_once(&mut first_process, &requested)
        .expect_err("the first process must report an uncertain outcome");

    assert!(matches!(
        error,
        BootstrapGroupError::Storage(BootstrapStoreError::OutcomeUnknown(_))
    ));

    let mut restarted_process = MemoryBootstrapStore {
        durable: durable_state,
        return_unknown_after_persist: false,
    };

    assert_eq!(
        bootstrap_group_exactly_once(&mut restarted_process, &requested)
            .expect("restart should discover the durable bootstrap"),
        BootstrapOutcome::AlreadyInstalled
    );

    assert_eq!(restarted_process.successful_installs(), 1);
}

/// Realistic bug caught: exactly-once bootstrap works only in a test memory
/// store and a real process restart loses or replaces the initial membership.
#[test]
fn filesystem_bootstrap_store_survives_process_reopen() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("ragnordb-bootstrap-{}-{unique}", process::id()));
    let requested = bootstrap();

    let mut first = FileBootstrapStore::open(&directory).unwrap();
    assert_eq!(
        bootstrap_group_exactly_once(&mut first, &requested).unwrap(),
        BootstrapOutcome::Installed
    );
    drop(first);

    let mut reopened = FileBootstrapStore::open(&directory).unwrap();
    assert_eq!(
        bootstrap_group_exactly_once(&mut reopened, &requested).unwrap(),
        BootstrapOutcome::AlreadyInstalled
    );

    fs::remove_dir_all(directory).unwrap();
}
