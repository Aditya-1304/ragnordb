//! Shared byte reservations for tablet-local mutable memtables.
//!
//! The reference MVCC backend charges each retained record's owned key/value
//! storage plus a fixed per-record index allowance. This is a managed
//! memtable budget; process RSS and temporary pinned read copies are reported
//! separately and are not represented by this counter.

use std::sync::{Arc, Mutex, MutexGuard};

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
pub const DEFAULT_TABLET_IMMUTABLE_SOFT_BYTES: usize = checked_memtable_limit(
    DEFAULT_TABLET_ACTIVE_MEMTABLE_BYTES,
    DEFAULT_TABLET_IMMUTABLE_SOFT_COUNT,
);

/// Hard immutable byte limit available to committed apply and recovery.
pub const DEFAULT_TABLET_IMMUTABLE_HARD_BYTES: usize = checked_memtable_limit(
    DEFAULT_TABLET_ACTIVE_MEMTABLE_BYTES,
    DEFAULT_TABLET_IMMUTABLE_HARD_COUNT,
);

const fn checked_memtable_limit(active_bytes: usize, count: usize) -> usize {
    match active_bytes.checked_mul(count) {
        Some(bytes) => bytes,
        None => panic!("memtable byte limit overflows usize"),
    }
}

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
    /// Soft immutable-byte threshold that throttles new user proposals.
    pub immutable_memtable_limit_bytes: usize,
    /// Hard immutable-byte cap reserved for committed/progress work.
    pub immutable_memtable_hard_limit_bytes: usize,
    /// Number of immutable generations awaiting storage publication.
    pub immutable_memtable_count: usize,
    /// Soft immutable-generation threshold that throttles user proposals.
    pub immutable_memtable_count_limit: usize,
    /// Hard immutable-generation cap reserved for committed/progress work.
    pub immutable_memtable_hard_count_limit: usize,
    /// Current node-wide managed memtable charge.
    pub node_used_bytes: usize,
    /// Maximum node-wide managed memtable charge.
    pub node_limit_bytes: usize,
    /// User-class committed and reserved bytes.
    pub node_user_used_bytes: usize,
    /// Maximum committed and reserved bytes allowed for new user admission.
    pub node_user_limit_bytes: usize,
}

impl MemtablePressure {
    /// Return whether ordinary user proposals should be throttled.
    pub fn user_admission_throttled(self) -> bool {
        self.immutable_memtable_bytes >= self.immutable_memtable_limit_bytes
            || self.immutable_memtable_count >= self.immutable_memtable_count_limit
            || self.node_user_used_bytes >= self.node_user_limit_bytes
    }

    /// Return whether the bounded immutable queue has no remaining hard room.
    pub fn hard_capacity_exhausted(self) -> bool {
        self.immutable_memtable_bytes >= self.immutable_memtable_hard_limit_bytes
            || self.immutable_memtable_count >= self.immutable_memtable_hard_count_limit
    }

    /// Compatibility spelling retained for existing callers.
    pub fn user_writes_stalled(self) -> bool {
        self.user_admission_throttled()
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
    user_committed_bytes: usize,
    progress_committed_bytes: usize,
    user_reserved_bytes: usize,
    progress_reserved_bytes: usize,
    total_used_bytes: usize,
    user_used_bytes: usize,
}

impl NodeMemtableBudget {
    /// Create a shared memtable budget with a fixed node-wide byte limit.
    pub fn new(limit_bytes: usize) -> Result<Self> {
        let progress_reserve_bytes = if limit_bytes == 0 {
            0
        } else {
            (limit_bytes / 4)
                .max(1)
                .min(DEFAULT_NODE_MEMTABLE_PROGRESS_RESERVE_BYTES)
        };
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
        if progress_reserve_bytes == 0 {
            return Err(Error::InvalidArgument(
                "node memtable progress reserve must be greater than zero".to_string(),
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
        lock_unpoisoned(&self.inner.usage).total_used_bytes
    }

    /// Return bytes committed or reserved against ordinary user capacity.
    pub fn user_used_bytes(&self) -> usize {
        lock_unpoisoned(&self.inner.usage).user_used_bytes
    }

    /// Return the ordinary capacity limit after protecting progress headroom.
    pub fn user_limit_bytes(&self) -> usize {
        self.inner.limit_bytes - self.inner.progress_reserve_bytes
    }

    /// Reserve exact managed bytes before admitting or allocating the edit.
    pub fn reserve(&self, class: MemoryClass, bytes: usize) -> Result<MemoryReservation> {
        let mut usage = lock_unpoisoned(&self.inner.usage);
        let next_total = usage
            .total_used_bytes
            .checked_add(bytes)
            .ok_or_else(accounting_overflow)?;
        let next_user = match class {
            MemoryClass::User => usage
                .user_used_bytes
                .checked_add(bytes)
                .ok_or_else(accounting_overflow)?,
            MemoryClass::Progress => usage.user_used_bytes,
        };
        let next_reserved = match class {
            MemoryClass::User => usage
                .user_reserved_bytes
                .checked_add(bytes)
                .ok_or_else(accounting_overflow)?,
            MemoryClass::Progress => usage
                .progress_reserved_bytes
                .checked_add(bytes)
                .ok_or_else(accounting_overflow)?,
        };
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
                        "node user memtable capacity exhausted: requested {bytes} bytes with {} of {} bytes already committed or reserved",
                        usage.user_used_bytes,
                        self.user_limit_bytes()
                    ),
                    MemoryClass::Progress => format!(
                        "node memtable progress capacity exhausted: requested {bytes} bytes with {} of {} bytes already committed or reserved",
                        usage.total_used_bytes, self.inner.limit_bytes
                    ),
                },
            });
        }

        match class {
            MemoryClass::User => {
                usage.user_reserved_bytes = next_reserved;
                usage.user_used_bytes = next_user;
            }
            MemoryClass::Progress => usage.progress_reserved_bytes = next_reserved,
        }
        usage.total_used_bytes = next_total;

        Ok(MemoryReservation {
            budget: self.clone(),
            class,
            bytes,
            committed: false,
        })
    }

    fn release_reserved(&self, class: MemoryClass, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut usage = lock_unpoisoned(&self.inner.usage);
        let reserved = match class {
            MemoryClass::User => usage.user_reserved_bytes,
            MemoryClass::Progress => usage.progress_reserved_bytes,
        };
        let next_reserved = reserved
            .checked_sub(bytes)
            .ok_or_else(|| accounting_error("memtable reservation release exceeds held bytes"))?;
        let next_total = usage
            .total_used_bytes
            .checked_sub(bytes)
            .ok_or_else(|| accounting_error("memtable reservation total underflow"))?;
        let next_user = if class == MemoryClass::User {
            Some(
                usage
                    .user_used_bytes
                    .checked_sub(bytes)
                    .ok_or_else(|| accounting_error("user memtable usage underflow"))?,
            )
        } else {
            None
        };

        match (class, next_user) {
            (MemoryClass::User, Some(next_user)) => {
                usage.user_reserved_bytes = next_reserved;
                usage.user_used_bytes = next_user;
            }
            (MemoryClass::Progress, None) => usage.progress_reserved_bytes = next_reserved,
            _ => return Err(accounting_error("memtable reservation class mismatch")),
        }
        usage.total_used_bytes = next_total;
        Ok(())
    }

    fn publish_reserved(
        &self,
        class: MemoryClass,
        reserved: usize,
        committed: usize,
    ) -> Result<()> {
        let mut usage = lock_unpoisoned(&self.inner.usage);
        transition_reserved_usage(&mut usage, class, reserved, committed)
    }

    fn release_committed(&self, user_bytes: usize, progress_bytes: usize) -> Result<()> {
        if user_bytes == 0 && progress_bytes == 0 {
            return Ok(());
        }
        let total_bytes = user_bytes
            .checked_add(progress_bytes)
            .ok_or_else(accounting_overflow)?;
        let mut usage = lock_unpoisoned(&self.inner.usage);
        let next_user_committed = usage
            .user_committed_bytes
            .checked_sub(user_bytes)
            .ok_or_else(|| accounting_error("user committed memtable charge underflow"))?;
        let next_progress_committed = usage
            .progress_committed_bytes
            .checked_sub(progress_bytes)
            .ok_or_else(|| accounting_error("progress committed memtable charge underflow"))?;
        let next_total = usage
            .total_used_bytes
            .checked_sub(total_bytes)
            .ok_or_else(|| accounting_error("committed memtable total underflow"))?;
        let next_user = usage
            .user_used_bytes
            .checked_sub(user_bytes)
            .ok_or_else(|| accounting_error("user memtable usage underflow"))?;

        usage.user_committed_bytes = next_user_committed;
        usage.progress_committed_bytes = next_progress_committed;
        usage.total_used_bytes = next_total;
        usage.user_used_bytes = next_user;
        Ok(())
    }
}

fn transition_reserved_usage(
    usage: &mut BudgetUsage,
    class: MemoryClass,
    reserved: usize,
    committed: usize,
) -> Result<()> {
    let (reserved_bytes, committed_bytes) = match class {
        MemoryClass::User => (usage.user_reserved_bytes, usage.user_committed_bytes),
        MemoryClass::Progress => (
            usage.progress_reserved_bytes,
            usage.progress_committed_bytes,
        ),
    };
    let next_reserved = reserved_bytes
        .checked_sub(reserved)
        .ok_or_else(|| accounting_error("memtable reservation publication underflow"))?;
    let next_committed = committed_bytes
        .checked_add(committed)
        .ok_or_else(accounting_overflow)?;
    let released = reserved
        .checked_sub(committed)
        .ok_or_else(|| accounting_error("published bytes exceed the held reservation"))?;
    let next_total = usage
        .total_used_bytes
        .checked_sub(released)
        .ok_or_else(|| accounting_error("memtable total underflow during publication"))?;
    let next_user = if class == MemoryClass::User {
        Some(
            usage
                .user_used_bytes
                .checked_sub(released)
                .ok_or_else(|| accounting_error("user memtable usage underflow"))?,
        )
    } else {
        None
    };

    match (class, next_user) {
        (MemoryClass::User, Some(next_user)) => {
            usage.user_reserved_bytes = next_reserved;
            usage.user_committed_bytes = next_committed;
            usage.user_used_bytes = next_user;
        }
        (MemoryClass::Progress, None) => {
            usage.progress_reserved_bytes = next_reserved;
            usage.progress_committed_bytes = next_committed;
        }
        _ => return Err(accounting_error("memtable publication class mismatch")),
    }
    usage.total_used_bytes = next_total;
    Ok(())
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn accounting_overflow() -> Error {
    Error::TabletUnavailable {
        reason: "node memtable byte accounting overflowed".to_string(),
    }
}

fn accounting_error(reason: &str) -> Error {
    Error::TabletUnavailable {
        reason: reason.to_string(),
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

    pub(crate) fn belongs_to(&self, budget: &NodeMemtableBudget) -> bool {
        Arc::ptr_eq(&self.budget.inner, &budget.inner)
    }

    /// Convert the reserved bytes into committed memtable ownership.
    pub(crate) fn commit_amount(mut self, bytes: usize) -> Result<Option<MemtableCharge>> {
        if bytes > self.bytes {
            return Err(Error::TabletUnavailable {
                reason: "memtable publication exceeded its retained admission reservation"
                    .to_string(),
            });
        }
        self.budget
            .publish_reserved(self.class, self.bytes, bytes)?;
        self.committed = true;
        if bytes == 0 {
            return Ok(None);
        }
        Ok(Some(MemtableCharge {
            inner: Arc::new(MemtableChargeInner {
                budget: self.budget.clone(),
                bytes: Mutex::new(ChargeBytes::new(self.class, bytes)),
            }),
        }))
    }

    pub(crate) fn commit(mut self) -> Result<MemtableCharge> {
        self.budget
            .publish_reserved(self.class, self.bytes, self.bytes)?;
        self.committed = true;
        Ok(MemtableCharge {
            inner: Arc::new(MemtableChargeInner {
                budget: self.budget.clone(),
                bytes: Mutex::new(ChargeBytes::new(self.class, self.bytes)),
            }),
        })
    }

    pub(crate) fn commit_into_amount(self, charge: &MemtableCharge, bytes: usize) -> Result<()> {
        if bytes > self.bytes {
            return Err(Error::TabletUnavailable {
                reason: "memtable publication exceeded its retained admission reservation"
                    .to_string(),
            });
        }
        if !Arc::ptr_eq(&self.budget.inner, &charge.inner.budget.inner) {
            return Err(accounting_error(
                "cannot transfer a memtable reservation into a charge from another budget",
            ));
        }

        let mut charged = lock_unpoisoned(&charge.inner.bytes);
        let next_class_bytes = match self.class {
            MemoryClass::User => charged
                .user_bytes
                .checked_add(bytes)
                .ok_or_else(accounting_overflow)?,
            MemoryClass::Progress => charged
                .progress_bytes
                .checked_add(bytes)
                .ok_or_else(accounting_overflow)?,
        };
        let next_total_bytes = charged
            .total_bytes
            .checked_add(bytes)
            .ok_or_else(accounting_overflow)?;

        {
            let mut usage = lock_unpoisoned(&self.budget.inner.usage);
            transition_reserved_usage(&mut usage, self.class, self.bytes, bytes)?;
        }
        match self.class {
            MemoryClass::User => charged.user_bytes = next_class_bytes,
            MemoryClass::Progress => charged.progress_bytes = next_class_bytes,
        }
        charged.total_bytes = next_total_bytes;
        drop(charged);

        let mut reservation = self;
        reservation.committed = true;
        Ok(())
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.budget
                .release_reserved(self.class, self.bytes)
                .expect("memtable reservation ownership must release exactly once");
        }
    }
}

/// RAII owner for the current active memtable's node-budget charge.
#[derive(Debug, Clone)]
pub(crate) struct MemtableCharge {
    inner: Arc<MemtableChargeInner>,
}

#[derive(Debug, Default)]
struct ChargeBytes {
    user_bytes: usize,
    progress_bytes: usize,
    total_bytes: usize,
}

impl ChargeBytes {
    fn new(class: MemoryClass, bytes: usize) -> Self {
        match class {
            MemoryClass::User => Self {
                user_bytes: bytes,
                progress_bytes: 0,
                total_bytes: bytes,
            },
            MemoryClass::Progress => Self {
                user_bytes: 0,
                progress_bytes: bytes,
                total_bytes: bytes,
            },
        }
    }
}

#[derive(Debug)]
struct MemtableChargeInner {
    budget: NodeMemtableBudget,
    bytes: Mutex<ChargeBytes>,
}

impl MemtableCharge {
    pub(crate) fn bytes(&self) -> usize {
        lock_unpoisoned(&self.inner.bytes).total_bytes
    }

    /// Release this charge's remaining User and Progress ownership exactly once.
    pub(crate) fn release(&self) -> Result<()> {
        let mut charged = lock_unpoisoned(&self.inner.bytes);
        if charged.total_bytes == 0 {
            return Err(accounting_error(
                "memtable charge has already been released",
            ));
        }

        let charge_total = charged
            .user_bytes
            .checked_add(charged.progress_bytes)
            .ok_or_else(accounting_overflow)?;
        if charge_total != charged.total_bytes {
            return Err(accounting_error("memtable charge counters do not match"));
        }

        let mut usage = lock_unpoisoned(&self.inner.budget.inner.usage);
        let next_user_committed = usage
            .user_committed_bytes
            .checked_sub(charged.user_bytes)
            .ok_or_else(|| accounting_error("user committed memtable charge underflow"))?;
        let next_progress_committed = usage
            .progress_committed_bytes
            .checked_sub(charged.progress_bytes)
            .ok_or_else(|| accounting_error("progress committed memtable charge underflow"))?;
        let next_total = usage
            .total_used_bytes
            .checked_sub(charged.total_bytes)
            .ok_or_else(|| accounting_error("committed memtable total underflow"))?;
        let next_user = usage
            .user_used_bytes
            .checked_sub(charged.user_bytes)
            .ok_or_else(|| accounting_error("user memtable usage underflow"))?;

        usage.user_committed_bytes = next_user_committed;
        usage.progress_committed_bytes = next_progress_committed;
        usage.total_used_bytes = next_total;
        usage.user_used_bytes = next_user;
        *charged = ChargeBytes::default();
        Ok(())
    }

    /// Try to release bytes from this charge. User-class ownership is released
    /// first, then progress-class ownership, and invalid shrinks leave all
    /// accounting unchanged.
    pub(crate) fn try_shrink(&self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut charged = lock_unpoisoned(&self.inner.bytes);
        let next_total = charged
            .total_bytes
            .checked_sub(bytes)
            .ok_or_else(|| accounting_error("memtable charge shrink exceeds its retained bytes"))?;
        let user_released = charged.user_bytes.min(bytes);
        let progress_released = bytes
            .checked_sub(user_released)
            .ok_or_else(accounting_overflow)?;
        let next_user = charged
            .user_bytes
            .checked_sub(user_released)
            .ok_or_else(|| accounting_error("user charge shrink underflow"))?;
        let next_progress = charged
            .progress_bytes
            .checked_sub(progress_released)
            .ok_or_else(|| accounting_error("progress charge shrink underflow"))?;

        self.inner
            .budget
            .release_committed(user_released, progress_released)?;
        charged.user_bytes = next_user;
        charged.progress_bytes = next_progress;
        charged.total_bytes = next_total;
        Ok(())
    }
}

impl Drop for MemtableChargeInner {
    fn drop(&mut self) {
        let bytes = self
            .bytes
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.budget
            .release_committed(bytes.user_bytes, bytes.progress_bytes)
            .expect("memtable charge ownership must release exactly once");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_failure_leaves_user_and_progress_counters_unchanged() {
        let budget = NodeMemtableBudget::new_with_progress_reserve(100, 40).unwrap();
        let user = budget.reserve(MemoryClass::User, 60).unwrap();

        assert!(budget.reserve(MemoryClass::User, 1).is_err());
        assert!(budget.reserve(MemoryClass::Progress, 41).is_err());
        assert_eq!(budget.used_bytes(), 60);
        assert_eq!(budget.user_used_bytes(), 60);
        drop(user);

        let progress = budget.reserve(MemoryClass::Progress, 100).unwrap();
        assert_eq!(budget.used_bytes(), 100);
        drop(progress);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.user_used_bytes(), 0);
    }

    #[test]
    fn commit_transfers_reservation_into_classed_committed_usage() {
        let budget = NodeMemtableBudget::new_with_progress_reserve(100, 40).unwrap();
        let progress_charge = budget
            .reserve(MemoryClass::Progress, 40)
            .unwrap()
            .commit()
            .unwrap();

        assert_eq!(progress_charge.bytes(), 40);
        assert_eq!(budget.used_bytes(), 40);
        assert_eq!(budget.user_used_bytes(), 0);

        let user_charge = budget
            .reserve(MemoryClass::User, 60)
            .unwrap()
            .commit()
            .unwrap();
        assert_eq!(budget.used_bytes(), 100);
        assert_eq!(budget.user_used_bytes(), 60);
        drop(user_charge);
        drop(progress_charge);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.user_used_bytes(), 0);
    }

    #[test]
    fn commit_detects_accounting_corruption_without_partial_transfer() {
        let budget = NodeMemtableBudget::new_with_progress_reserve(100, 20).unwrap();
        let reservation = budget.reserve(MemoryClass::User, 5).unwrap();
        {
            let mut usage = lock_unpoisoned(&budget.inner.usage);
            usage.user_reserved_bytes = 0;
            usage.total_used_bytes = 0;
            usage.user_used_bytes = 0;
        }

        let drop_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert!(reservation.commit().is_err());
        }));
        assert!(drop_result.is_err());
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.user_used_bytes(), 0);
    }

    #[test]
    fn reservation_drop_commit_amount_and_charge_clones_release_exactly_once() {
        let budget = NodeMemtableBudget::new_with_progress_reserve(100, 20).unwrap();
        let dropped = budget.reserve(MemoryClass::User, 10).unwrap();
        drop(dropped);
        assert_eq!(budget.used_bytes(), 0);

        let partial = budget
            .reserve(MemoryClass::User, 20)
            .unwrap()
            .commit_amount(12)
            .unwrap()
            .unwrap();
        assert_eq!(partial.bytes(), 12);
        assert_eq!(budget.used_bytes(), 12);
        assert_eq!(budget.user_used_bytes(), 12);

        let clone = partial.clone();
        drop(partial);
        assert_eq!(budget.used_bytes(), 12);
        drop(clone);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.user_used_bytes(), 0);
    }

    #[test]
    fn growth_transfers_both_classes_into_one_charge_and_shrinks_exactly() {
        let budget = NodeMemtableBudget::new_with_progress_reserve(100, 30).unwrap();
        let charge = budget
            .reserve(MemoryClass::User, 20)
            .unwrap()
            .commit()
            .unwrap();
        budget
            .reserve(MemoryClass::Progress, 10)
            .unwrap()
            .commit_into_amount(&charge, 10)
            .unwrap();

        assert_eq!(charge.bytes(), 30);
        assert_eq!(budget.used_bytes(), 30);
        assert_eq!(budget.user_used_bytes(), 20);

        charge.try_shrink(25).unwrap();
        assert_eq!(charge.bytes(), 5);
        assert_eq!(budget.used_bytes(), 5);
        assert_eq!(budget.user_used_bytes(), 0);

        assert!(charge.try_shrink(6).is_err());
        assert_eq!(charge.bytes(), 5);
        assert_eq!(budget.used_bytes(), 5);

        let clone = charge.clone();
        drop(charge);
        assert_eq!(budget.used_bytes(), 5);
        drop(clone);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn explicit_release_zeros_mixed_class_charge_once_across_clones() {
        let budget = NodeMemtableBudget::new_with_progress_reserve(100, 30).unwrap();
        let charge = budget
            .reserve(MemoryClass::User, 20)
            .unwrap()
            .commit()
            .unwrap();
        budget
            .reserve(MemoryClass::Progress, 10)
            .unwrap()
            .commit_into_amount(&charge, 10)
            .unwrap();
        let clone = charge.clone();

        assert_eq!(charge.bytes(), 30);
        assert_eq!(budget.used_bytes(), 30);
        assert_eq!(budget.user_used_bytes(), 20);
        charge.release().unwrap();
        assert_eq!(charge.bytes(), 0);
        assert_eq!(clone.bytes(), 0);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.user_used_bytes(), 0);

        let unrelated = budget
            .reserve(MemoryClass::User, 5)
            .unwrap()
            .commit()
            .unwrap();
        assert!(clone.release().is_err());
        assert_eq!(clone.bytes(), 0);
        assert_eq!(budget.used_bytes(), 5);
        assert_eq!(budget.user_used_bytes(), 5);
        drop(unrelated);
        drop(charge);
        drop(clone);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn invalid_commit_amount_and_mismatched_budget_do_not_partially_transfer() {
        let first = NodeMemtableBudget::new_with_progress_reserve(100, 20).unwrap();
        let second = NodeMemtableBudget::new_with_progress_reserve(100, 20).unwrap();
        let charge = first
            .reserve(MemoryClass::User, 10)
            .unwrap()
            .commit()
            .unwrap();

        let mismatch = second
            .reserve(MemoryClass::Progress, 5)
            .unwrap()
            .commit_into_amount(&charge, 5);
        assert!(mismatch.is_err());
        assert_eq!(first.used_bytes(), 10);
        assert_eq!(second.used_bytes(), 0);

        let invalid_amount = first
            .reserve(MemoryClass::User, 4)
            .unwrap()
            .commit_amount(5);
        assert!(invalid_amount.is_err());
        assert_eq!(first.used_bytes(), 10);
        assert_eq!(first.user_used_bytes(), 10);
        drop(charge);
        assert_eq!(first.used_bytes(), 0);
    }

    #[test]
    fn v1_limits_and_pressure_keep_soft_and_hard_thresholds_distinct() {
        assert!(NodeMemtableBudget::new_with_progress_reserve(1, 0).is_err());
        let tiny = NodeMemtableBudget::new(1).unwrap();
        assert!(tiny.reserve(MemoryClass::User, 1).is_err());
        assert!(tiny.reserve(MemoryClass::Progress, 1).is_ok());

        assert_eq!(DEFAULT_TABLET_ACTIVE_MEMTABLE_BYTES, 64 * 1024 * 1024);
        assert_eq!(DEFAULT_TABLET_IMMUTABLE_SOFT_COUNT, 2);
        assert_eq!(DEFAULT_TABLET_IMMUTABLE_HARD_COUNT, 4);
        assert_eq!(DEFAULT_TABLET_IMMUTABLE_SOFT_BYTES, 2 * 64 * 1024 * 1024);
        assert_eq!(DEFAULT_TABLET_IMMUTABLE_HARD_BYTES, 4 * 64 * 1024 * 1024);

        let byte_soft = MemtablePressure {
            active_bytes: 0,
            active_limit_bytes: 300,
            immutable_memtable_bytes: 600,
            immutable_memtable_limit_bytes: 600,
            immutable_memtable_hard_limit_bytes: 1200,
            immutable_memtable_count: 0,
            immutable_memtable_count_limit: 2,
            immutable_memtable_hard_count_limit: 4,
            node_used_bytes: 0,
            node_limit_bytes: 1000,
            node_user_used_bytes: 0,
            node_user_limit_bytes: 800,
        };
        assert!(byte_soft.user_admission_throttled());
        assert!(!byte_soft.hard_capacity_exhausted());

        let byte_hard = MemtablePressure {
            immutable_memtable_bytes: 1200,
            immutable_memtable_limit_bytes: 600,
            immutable_memtable_hard_limit_bytes: 1200,
            immutable_memtable_count: 1,
            immutable_memtable_count_limit: 2,
            immutable_memtable_hard_count_limit: 4,
            ..byte_soft
        };
        assert!(byte_hard.hard_capacity_exhausted());

        let count_soft = MemtablePressure {
            immutable_memtable_bytes: 0,
            immutable_memtable_count: 2,
            ..byte_soft
        };
        assert!(count_soft.user_admission_throttled());
        assert!(!count_soft.hard_capacity_exhausted());

        let count_hard = MemtablePressure {
            immutable_memtable_bytes: 0,
            immutable_memtable_count: 4,
            ..byte_soft
        };
        assert!(count_hard.hard_capacity_exhausted());
    }

    #[test]
    fn reservation_arithmetic_fails_closed_on_overflow() {
        let budget = NodeMemtableBudget::new_with_progress_reserve(usize::MAX, 1).unwrap();
        let held = budget.reserve(MemoryClass::User, usize::MAX - 1).unwrap();

        assert!(budget.reserve(MemoryClass::User, 2).is_err());
        assert_eq!(budget.used_bytes(), usize::MAX - 1);
        drop(held);
        assert_eq!(budget.used_bytes(), 0);

        let full = budget.reserve(MemoryClass::Progress, usize::MAX).unwrap();
        assert_eq!(budget.used_bytes(), usize::MAX);
        assert!(budget.reserve(MemoryClass::Progress, 1).is_err());
        drop(full);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn invalid_direct_release_is_reported_without_changing_any_counter() {
        let budget = NodeMemtableBudget::new_with_progress_reserve(100, 20).unwrap();
        let held = budget.reserve(MemoryClass::User, 10).unwrap();

        assert!(budget.release_reserved(MemoryClass::User, 11).is_err());
        assert_eq!(budget.used_bytes(), 10);
        assert_eq!(budget.user_used_bytes(), 10);
        drop(held);
        assert_eq!(budget.used_bytes(), 0);
    }
}
