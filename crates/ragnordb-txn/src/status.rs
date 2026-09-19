//! Durable transaction-status identity, placement, and lookup.
//!
//! A transaction status record is physically stored with the tablet that owns
//! the transaction's primary key. The tablet route is only a cached transport
//! hint: the primary key and deterministic status key remain authoritative when
//! a split, merge, replica move, or leader change invalidates that hint.

use std::collections::BTreeSet;

use ragnordb_common::{
    Error, Result,
    codec::{TxnStatus, TxnStatusRecord},
    ids::{TabletId, TxnId},
};

use crate::coordinator::{LogicalMutationId, ParticipantRoute};

/// Stable key namespace for durable transaction-status records.
const TRANSACTION_STATUS_KEY_PREFIX: &[u8] = b"/txn-status/";

/// Deterministic durable key for one transaction-status record.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionStatusKey {
    txn_id: TxnId,
    bytes: Vec<u8>,
}

impl TransactionStatusKey {
    /// Construct `/txn-status/{txn_id}` using a fixed-width big-endian suffix.
    ///
    /// The human-readable path notation identifies the namespace; the fixed
    /// binary suffix avoids decimal formatting ambiguity and preserves one
    /// canonical byte representation for durable and RPC boundaries.
    pub fn new(txn_id: TxnId) -> Result<Self> {
        if txn_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "transaction status key cannot use transaction ID 0".to_string(),
            ));
        }

        let mut bytes = Vec::with_capacity(TRANSACTION_STATUS_KEY_PREFIX.len() + 8);
        bytes.extend_from_slice(TRANSACTION_STATUS_KEY_PREFIX);
        bytes.extend_from_slice(&txn_id.0.to_be_bytes());

        Ok(Self { txn_id, bytes })
    }

    /// Decode and validate a status key received from durable or transport data.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let expected_length = TRANSACTION_STATUS_KEY_PREFIX.len() + 8;
        if bytes.len() != expected_length {
            return Err(Error::CorruptData(format!(
                "transaction status key has length {}, expected {expected_length}",
                bytes.len()
            )));
        }
        if !bytes.starts_with(TRANSACTION_STATUS_KEY_PREFIX) {
            return Err(Error::CorruptData(
                "transaction status key has an invalid namespace".to_string(),
            ));
        }

        let mut txn_id_bytes = [0_u8; 8];
        txn_id_bytes.copy_from_slice(&bytes[TRANSACTION_STATUS_KEY_PREFIX.len()..]);
        let key = Self::new(TxnId(u64::from_be_bytes(txn_id_bytes)))
            .map_err(|error| Error::CorruptData(error.to_string()))?;

        if key.bytes != bytes {
            return Err(Error::CorruptData(
                "transaction status key is not in canonical form".to_string(),
            ));
        }

        Ok(key)
    }

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Semantic location of a transaction's primary/status record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionStatusLocation {
    txn_id: TxnId,
    primary_key: LogicalMutationId,
    status_key: TransactionStatusKey,
    route: Option<ParticipantRoute>,
}

/// Storage boundary for the status record kept by the primary/status tablet.
///
/// The implementation behind this trait is expected to be owned by the tablet
/// state machine and replicated through its normal durable command path. The
/// transaction layer only owns the identity, validation, and transition rules.
pub trait TransactionStatusStore {
    fn read_status(&self, key: &TransactionStatusKey) -> Result<Option<TxnStatusRecord>>;

    fn write_status(&mut self, key: &TransactionStatusKey, record: TxnStatusRecord) -> Result<()>;
}

/// Small deterministic status table used by unit tests and single-owner
/// adapters before a tablet-backed implementation is connected.
#[derive(Debug, Default)]
pub struct InMemoryTransactionStatusStore {
    records: std::collections::BTreeMap<TransactionStatusKey, TxnStatusRecord>,
}

impl InMemoryTransactionStatusStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TransactionStatusStore for InMemoryTransactionStatusStore {
    fn read_status(&self, key: &TransactionStatusKey) -> Result<Option<TxnStatusRecord>> {
        Ok(self.records.get(key).cloned())
    }

    fn write_status(&mut self, key: &TransactionStatusKey, record: TxnStatusRecord) -> Result<()> {
        record
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;

        if record.txn_id != key.txn_id() {
            return Err(Error::InvalidArgument(
                "transaction status key and record transaction ID differ".to_string(),
            ));
        }

        if let Some(existing) = self.records.get(key) {
            validate_status_update(existing, &record)?;
        }

        self.records.insert(key.clone(), record);
        Ok(())
    }
}

fn validate_status_update(existing: &TxnStatusRecord, next: &TxnStatusRecord) -> Result<()> {
    if existing.txn_id != next.txn_id
        || existing.start_timestamp != next.start_timestamp
        || existing.primary_key != next.primary_key
        || existing.participant_tablet_ids != next.participant_tablet_ids
    {
        return Err(Error::CorruptData(
            "transaction status identity changed after publication".to_string(),
        ));
    }

    match (existing.status, next.status) {
        (TxnStatus::Pending, TxnStatus::Pending)
        | (TxnStatus::Pending, TxnStatus::Committed)
        | (TxnStatus::Pending, TxnStatus::Aborted) => Ok(()),
        (TxnStatus::Committed, TxnStatus::Committed) | (TxnStatus::Aborted, TxnStatus::Aborted)
            if existing.commit_timestamp == next.commit_timestamp =>
        {
            Ok(())
        }
        (TxnStatus::Committed, _) | (TxnStatus::Aborted, _) => Err(Error::WriteConflict(
            "terminal transaction status cannot transition again".to_string(),
        )),
    }
}

impl TransactionStatusLocation {
    pub fn new(txn_id: TxnId, primary_key: Vec<u8>) -> Result<Self> {
        let status_key = TransactionStatusKey::new(txn_id)?;

        Ok(Self {
            txn_id,
            primary_key: LogicalMutationId::from_key(&primary_key)?,
            status_key,
            route: None,
        })
    }

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    pub fn primary_key(&self) -> &[u8] {
        self.primary_key.as_key()
    }

    pub fn status_key(&self) -> &TransactionStatusKey {
        &self.status_key
    }

    pub fn route(&self) -> Option<ParticipantRoute> {
        self.route
    }

    /// Install a route only as a refreshable hint for the primary/status tablet.
    pub fn set_route(&mut self, route: ParticipantRoute) -> Result<()> {
        self.route = Some(route);
        Ok(())
    }

    /// Return the participant-tablet snapshot with the status tablet first.
    ///
    /// The primary/status tablet must be present in the participant set. The
    /// remaining IDs are emitted in ascending order so the status record has a
    /// deterministic representation independent of coordinator insertion order.
    pub fn ordered_participant_tablet_ids(
        &self,
        participants: &BTreeSet<TabletId>,
    ) -> Result<Vec<u64>> {
        let primary_tablet = self
            .route
            .ok_or_else(|| {
                Error::InvalidArgument(
                    "status placement requires a current primary tablet route".to_string(),
                )
            })?
            .tablet_id;

        if participants.is_empty() {
            return Err(Error::InvalidArgument(
                "status placement requires at least one participant tablet".to_string(),
            ));
        }
        if participants.iter().any(|tablet| tablet.0 == 0) {
            return Err(Error::InvalidArgument(
                "status placement cannot contain tablet ID 0".to_string(),
            ));
        }
        if !participants.contains(&primary_tablet) {
            return Err(Error::InvalidArgument(
                "status placement participants must include the primary tablet".to_string(),
            ));
        }

        let mut ordered = Vec::with_capacity(participants.len());
        ordered.push(primary_tablet.0);
        ordered.extend(
            participants
                .iter()
                .filter(|tablet| **tablet != primary_tablet)
                .map(|tablet| tablet.0),
        );
        Ok(ordered)
    }

    /// Resolve the status record through the primary key and refresh its route
    /// after a stale route response. Reads are safe to retry because they do not
    /// create a new transaction mutation or status identity.
    pub fn lookup_with_retry<R, L>(
        &mut self,
        resolver: &mut R,
        reader: &mut L,
        max_route_refreshes: usize,
    ) -> Result<Option<L::Output>>
    where
        R: TransactionStatusRouteResolver,
        L: TransactionStatusReader,
    {
        if max_route_refreshes == 0 {
            return Err(Error::InvalidArgument(
                "transaction status lookup retry budget must be non-zero".to_string(),
            ));
        }

        let mut refreshes = 0;
        loop {
            let route = match self.route {
                Some(route) => route,
                None => {
                    let route = resolver.resolve_status_route(self.primary_key(), None)?;
                    self.route = Some(route);
                    route
                }
            };

            match reader.read_status(route, &self.status_key) {
                Ok(status) => return Ok(status),
                Err(TransactionStatusLookupError::RouteRefreshRequired { reason }) => {
                    if refreshes >= max_route_refreshes {
                        return Err(Error::TabletUnavailable {
                            reason: format!(
                                "transaction status route refresh budget exhausted: {reason}"
                            ),
                        });
                    }

                    let refreshed_route =
                        resolver.resolve_status_route(self.primary_key(), Some(route))?;
                    if refreshed_route == route {
                        return Err(Error::TabletUnavailable {
                            reason: "status route refresh returned the unchanged route".to_string(),
                        });
                    }
                    self.route = Some(refreshed_route);
                    refreshes += 1;
                }
                Err(TransactionStatusLookupError::Unavailable { reason }) => {
                    return Err(Error::TabletUnavailable { reason });
                }
            }
        }
    }
}

/// Resolve the current tablet route for the primary/status key.
pub trait TransactionStatusRouteResolver {
    fn resolve_status_route(
        &mut self,
        primary_key: &[u8],
        previous_route: Option<ParticipantRoute>,
    ) -> Result<ParticipantRoute>;
}

/// Typed read outcomes for the status record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionStatusLookupError {
    /// The cached route no longer owns the primary key or its epoch is stale.
    RouteRefreshRequired { reason: String },

    /// The lookup cannot currently be served by the selected route.
    Unavailable { reason: String },
}

/// Read one status record from a selected route.
pub trait TransactionStatusReader {
    type Output;

    fn read_status(
        &mut self,
        route: ParticipantRoute,
        status_key: &TransactionStatusKey,
    ) -> std::result::Result<Option<Self::Output>, TransactionStatusLookupError>;
}
