//! Shared byte reservations for tablet-local mutable memtables.
//!
//! The reference MVCC backend charges each retained record's owned key/value
//! storage plus a fixed per-record index allowance. This is a managed
//! memtable budget; process RSS and temporary pinned read copies are reported
//! separately and are not represented by this counter.

use std::sync::{Arc, Mutex};

use ragnordb_common::{Error, Result};

/// Initial node-wide memtable cap used until operator-configurable storage
/// budgets are introduced at the shared node-resource boundary.
pub const DEFAULT_NODE_MEMTABLE_BUDGET_BYTES: usize = 1024 * 1024 * 1024;

/// Node capacity held back for committed apply and recovery progress.
pub const DEFAULT_NODE_MEMTABLE_PROGRESS_RESERVE_BYTES: usize = 256 * 1024 * 1024;

/// Maximum active mutable-generation charge for one tablet.
pub const DEFAULT_TABLET_ACTIVE_MEMTABLE_BYTES: usize = 64 * 1024 * 1024;

/// Soft immutable-generation limit that throttles new user proposals.
pub const DEFAULT_TABLET_IMMUTABLE_SOFT_COUNT: usize = 2;

/// Hard immutable-generation limit reserved for already committed progress.
pub const DEFAULT_TABLET_IMMUTABLE_HARD_COUNT: usize = 4;

/// Soft immutable byte limit that throttles new user proposals.
pub const DEFAULT_TABLET_IMMUTABLE_SOFT_BYTES: usize =
    DEFAULT_TABLET_ACTIVE_MEMTABLE_BYTES * DEFAULT_TABLET_IMMUTABLE_SOFT_COUNT;

/// Hard immutable byte limit available to committed apply and recovery.
pub const DEFAULT_TABLET_IMMUTABLE_HARD_BYTES: usize =
    DEFAULT_TABLET_ACTIVE_MEMTABLE_BYTES * DEFAULT_TABLET_IMMUTABLE_HARD_COUNT;

/// Compatibility name for callers that use the user-admission soft limit.
pub const DEFAULT_TABLET_IMMUTABLE_MEMTABLE_COUNT: usize = DEFAULT_TABLET_IMMUTABLE_SOFT_COUNT;

/// Reservation purpose controls which part of the shared node budget may be
/// consumed. Progress work can use the reserved headroom; user admission cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryClass {
    /// Capacity reserved before a new user proposal enters Raft.
    User,
    /// Capacity needed to apply committed entries or continue recovery.
    Progress,
}

/// A node-owned limit shared by all tablet memtables constructed from it.
#[derive(Debug, Clone)]
pub struct NodeMemtableBudget {
    inner: Arc<NodeMemtableBudgetInner>,
}

/// Point-in-time managed-memory pressure for one budgeted tablet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemtablePressure {
    /// Bytes retained by the current mutable generation.
    pub active_bytes: usize,
    /// Maximum managed bytes allowed in the mutable generation.
    pub active_limit_bytes: usize,
    /// Bytes retained by immutable generations awaiting storage publication.
    pub immutable_memtable_bytes: usize,
    /// Maximum managed bytes retained by this tablet's immutable queue.
    pub immutable_memtable_limit_bytes: usize,
    /// Number of immutable generations awaiting storage publication.
    pub immutable_memtable_count: usize,
    /// Maximum immutable generations retained by this tablet.
    pub immutable_memtable_count_limit: usize,
    /// Current node-wide managed memtable charge.
    pub node_used_bytes: usize,
    /// Maximum node-wide managed memtable charge.
    pub node_limit_bytes: usize,
    /// Committed and reserved bytes charged against ordinary user capacity.
    pub node_user_used_bytes: usize,
    /// Maximum committed and reserved bytes allowed for new user admission.
    pub node_user_limit_bytes: usize,
}

impl MemtablePressure {
    /// Return whether another user write must wait for immutable-memory relief.
    pub fn user_writes_stalled(self) -> bool {
        self.immutable_memtable_bytes >= self.immutable_memtable_limit_bytes
            || self.immutable_memtable_count >= self.immutable_memtable_count_limit
            || self.node_user_used_bytes >= self.node_user_limit_bytes
    }
}

#[derive(Debug)]
struct NodeMemtableBudgetInner {
    limit_bytes: usize,
    progress_reserve_bytes: usize,
    usage: Mutex<BudgetUsage>,
}

#[derive(Debug, Default)]
struct BudgetUsage {
    committed_bytes: usize,
    user_reserved_bytes: usize,
    progress_reserved_bytes: usize,
}

impl NodeMemtableBudget {
    /// Create a shared memtable budget with a fixed node-wide byte limit.
    pub fn new(limit_bytes: usize) -> Result<Self> {
        let progress_reserve_bytes =
            (limit_bytes / 4).min(DEFAULT_NODE_MEMTABLE_PROGRESS_RESERVE_BYTES);
        Self::new_with_progress_reserve(limit_bytes, progress_reserve_bytes)
    }

    /// Create a shared budget with an explicit committed-progress reserve.
    pub fn new_with_progress_reserve(
        limit_bytes: usize,
        progress_reserve_bytes: usize,
    ) -> Result<Self> {
        if limit_bytes == 0 {
            return Err(Error::InvalidArgument(
                "node memtable budget must be greater than zero".to_string(),
            ));
        }
        if progress_reserve_bytes > limit_bytes {
            return Err(Error::InvalidArgument(
                "node memtable progress reserve exceeds its total budget".to_string(),
            ));
        }

        Ok(Self {
            inner: Arc::new(NodeMemtableBudgetInner {
                limit_bytes,
                progress_reserve_bytes,
                usage: Mutex::new(BudgetUsage::default()),
            }),
        })
    }

    /// Return the configured node-wide managed memtable limit.
    pub fn limit_bytes(&self) -> usize {
        self.inner.limit_bytes
    }

    /// Return the bytes currently held by active memtable reservations.
    pub fn used_bytes(&self) -> usize {
        let usage = self
            .inner
            .usage
            .lock()
            .expect("memtable budget mutex poisoned");
        usage.committed_bytes + usage.user_reserved_bytes + usage.progress_reserved_bytes
    }

    /// Return bytes committed or reserved against ordinary user capacity.
    pub fn user_used_bytes(&self) -> usize {
        let usage = self
            .inner
            .usage
            .lock()
            .expect("memtable budget mutex poisoned");
        usage.committed_bytes + usage.user_reserved_bytes
    }

    /// Return the ordinary capacity limit after protecting progress headroom.
    pub fn user_limit_bytes(&self) -> usize {
        self.inner.limit_bytes - self.inner.progress_reserve_bytes
    }

    /// Reserve exact managed bytes before admitting or allocating the edit.
    pub fn reserve(&self, class: MemoryClass, bytes: usize) -> Result<MemoryReservation> {
        let mut usage = self
            .inner
            .usage
            .lock()
            .expect("memtable budget mutex poisoned");
        let user_used = usage
            .committed_bytes
            .checked_add(usage.user_reserved_bytes)
            .ok_or_else(accounting_overflow)?;
        let total_used = user_used
            .checked_add(usage.progress_reserved_bytes)
            .ok_or_else(accounting_overflow)?;
        let next_total = total_used
            .checked_add(bytes)
            .ok_or_else(accounting_overflow)?;
        let next_user = user_used
            .checked_add(bytes)
            .ok_or_else(accounting_overflow)?;
        let allowed = match class {
            MemoryClass::User => {
                next_user <= self.user_limit_bytes() && next_total <= self.inner.limit_bytes
            }
            MemoryClass::Progress => next_total <= self.inner.limit_bytes,
        };
        if !allowed {
            return Err(Error::TabletUnavailable {
                reason: match class {
                    MemoryClass::User => format!(
                        "node user memtable capacity exhausted: requested {bytes} bytes with {user_used} of {} bytes already committed or reserved",
                        self.user_limit_bytes()
                    ),
                    MemoryClass::Progress => format!(
                        "node memtable progress capacity exhausted: requested {bytes} bytes with {total_used} of {} bytes already committed or reserved",
                        self.inner.limit_bytes
                    ),
                },
            });
        }

        match class {
            MemoryClass::User => usage.user_reserved_bytes = next_user - usage.committed_bytes,
            MemoryClass::Progress => usage.progress_reserved_bytes += bytes,
        }
        Ok(MemoryReservation {
            budget: self.clone(),
            class,
            bytes,
            committed: false,
        })
    }

    fn release_reserved(&self, class: MemoryClass, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut usage = self
            .inner
            .usage
            .lock()
            .expect("memtable budget mutex poisoned");
        let counter = match class {
            MemoryClass::User => &mut usage.user_reserved_bytes,
            MemoryClass::Progress => &mut usage.progress_reserved_bytes,
        };
        *counter = counter.checked_sub(bytes).expect("reservation underflow");
    }

    fn publish_reserved(&self, class: MemoryClass, reserved: usize, committed: usize) {
        let mut usage = self
            .inner
            .usage
            .lock()
            .expect("memtable budget mutex poisoned");
        let counter = match class {
            MemoryClass::User => &mut usage.user_reserved_bytes,
            MemoryClass::Progress => &mut usage.progress_reserved_bytes,
        };
        *counter = counter
            .checked_sub(reserved)
            .expect("reservation underflow");
        usage.committed_bytes = usage
            .committed_bytes
            .checked_add(committed)
            .expect("committed memtable accounting overflow");
    }

    fn release_committed(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut usage = self
            .inner
            .usage
            .lock()
            .expect("memtable budget mutex poisoned");
        usage.committed_bytes = usage
            .committed_bytes
            .checked_sub(bytes)
            .expect("committed memtable charge underflow");
    }
}

fn accounting_overflow() -> Error {
    Error::TabletUnavailable {
        reason: "node memtable byte accounting overflowed".to_string(),
    }
}

/// A reservation held before the corresponding memtable edit is published.
#[derive(Debug)]
pub struct MemoryReservation {
    budget: NodeMemtableBudget,
    class: MemoryClass,
    bytes: usize,
    committed: bool,
}

impl MemoryReservation {
    /// Exact bytes held by this reservation.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Reservation class retained for this proposal or committed operation.
    pub fn class(&self) -> MemoryClass {
        self.class
    }

    /// Convert the reserved bytes into committed memtable ownership.
    pub(crate) fn commit_amount(mut self, bytes: usize) -> Result<Option<MemtableCharge>> {
        if bytes > self.bytes {
            return Err(Error::TabletUnavailable {
                reason: "memtable publication exceeded its retained admission reservation"
                    .to_string(),
            });
        }
        self.budget.publish_reserved(self.class, self.bytes, bytes);
        self.committed = true;
        if bytes == 0 {
            return Ok(None);
        }
        Ok(Some(MemtableCharge {
            inner: Arc::new(MemtableChargeInner {
                budget: self.budget.clone(),
                bytes: Mutex::new(bytes),
            }),
        }))
    }

    pub(crate) fn commit(mut self) -> MemtableCharge {
        self.budget
            .publish_reserved(self.class, self.bytes, self.bytes);
        self.committed = true;
        MemtableCharge {
            inner: Arc::new(MemtableChargeInner {
                budget: self.budget.clone(),
                bytes: Mutex::new(self.bytes),
            }),
        }
    }

    pub(crate) fn commit_into_amount(self, charge: &MemtableCharge, bytes: usize) -> Result<()> {
        if bytes > self.bytes {
            return Err(Error::TabletUnavailable {
                reason: "memtable publication exceeded its retained admission reservation"
                    .to_string(),
            });
        }
        let budget = self.budget.clone();
        let mut reservation = self;
        budget.publish_reserved(reservation.class, reservation.bytes, bytes);
        reservation.committed = true;
        debug_assert!(Arc::ptr_eq(&budget.inner, &charge.inner.budget.inner));
        let mut charge_bytes = charge
            .inner
            .bytes
            .lock()
            .expect("memtable charge mutex poisoned");
        *charge_bytes = charge_bytes
            .checked_add(bytes)
            .expect("memtable charge overflow");
        Ok(())
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.budget.release_reserved(self.class, self.bytes);
        }
    }
}

/// RAII owner for the current active memtable's node-budget charge.
#[derive(Debug, Clone)]
pub(crate) struct MemtableCharge {
    inner: Arc<MemtableChargeInner>,
}

#[derive(Debug)]
struct MemtableChargeInner {
    budget: NodeMemtableBudget,
    bytes: Mutex<usize>,
}

impl MemtableCharge {
    pub(crate) fn bytes(&self) -> usize {
        *self
            .inner
            .bytes
            .lock()
            .expect("memtable charge mutex poisoned")
    }

    pub(crate) fn shrink(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        {
            let mut charged = self
                .inner
                .bytes
                .lock()
                .expect("memtable charge mutex poisoned");
            *charged = charged
                .checked_sub(bytes)
                .expect("memtable charge underflow");
        }
        self.inner.budget.release_committed(bytes);
    }
}

impl Drop for MemtableChargeInner {
    fn drop(&mut self) {
        let bytes = *self.bytes.lock().expect("memtable charge mutex poisoned");
        self.budget.release_committed(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_reservations_remain_available_when_user_capacity_is_full() {
        let budget = NodeMemtableBudget::new_with_progress_reserve(100, 40).unwrap();
        let user = budget.reserve(MemoryClass::User, 60).unwrap();

        assert!(budget.reserve(MemoryClass::User, 1).is_err());
        let progress = budget.reserve(MemoryClass::Progress, 40).unwrap();
        assert_eq!(budget.used_bytes(), 100);

        drop(progress);
        assert_eq!(budget.used_bytes(), 60);
        drop(user);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn reservation_arithmetic_fails_closed_on_overflow() {
        let budget = NodeMemtableBudget::new_with_progress_reserve(usize::MAX, 0).unwrap();
        let held = budget.reserve(MemoryClass::User, usize::MAX - 1).unwrap();

        assert!(budget.reserve(MemoryClass::User, 2).is_err());
        drop(held);
        assert_eq!(budget.used_bytes(), 0);
    }
}
