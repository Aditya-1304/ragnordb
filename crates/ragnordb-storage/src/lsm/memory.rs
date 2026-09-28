//! Shared byte reservations for tablet-local mutable memtables.
//!
//! The reference MVCC backend charges each retained record's owned key/value
//! storage plus a fixed per-record index allowance. This is a managed
//! memtable budget; process RSS and temporary pinned read copies are reported
//! separately and are not represented by this counter.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use ragnordb_common::{Error, Result};

/// Initial node-wide memtable cap used until operator-configurable storage
/// budgets are introduced at the shared node-resource boundary.
pub const DEFAULT_NODE_MEMTABLE_BUDGET_BYTES: usize = 1024 * 1024 * 1024;

/// Initial active-generation cap for one tablet. Whole command deltas are
/// admitted atomically, so this leaves headroom above the encoded delta limit.
pub const DEFAULT_TABLET_ACTIVE_MEMTABLE_BYTES: usize = 128 * 1024 * 1024;

/// A node-owned limit shared by all tablet memtables constructed from it.
#[derive(Debug, Clone)]
pub struct NodeMemtableBudget {
    inner: Arc<NodeMemtableBudgetInner>,
}

#[derive(Debug)]
struct NodeMemtableBudgetInner {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
}

impl NodeMemtableBudget {
    /// Create a shared memtable budget with a fixed node-wide byte limit.
    pub fn new(limit_bytes: usize) -> Result<Self> {
        if limit_bytes == 0 {
            return Err(Error::InvalidArgument(
                "node memtable budget must be greater than zero".to_string(),
            ));
        }

        Ok(Self {
            inner: Arc::new(NodeMemtableBudgetInner {
                limit_bytes,
                used_bytes: AtomicUsize::new(0),
            }),
        })
    }

    /// Return the configured node-wide managed memtable limit.
    pub fn limit_bytes(&self) -> usize {
        self.inner.limit_bytes
    }

    /// Return the bytes currently held by active memtable reservations.
    pub fn used_bytes(&self) -> usize {
        self.inner.used_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Result<MemoryReservation> {
        let mut used = self.inner.used_bytes.load(Ordering::Acquire);
        loop {
            let next = used
                .checked_add(bytes)
                .ok_or_else(|| Error::TabletUnavailable {
                    reason: "node memtable byte accounting overflowed".to_string(),
                })?;
            if next > self.inner.limit_bytes {
                return Err(Error::TabletUnavailable {
                    reason: format!(
                        "node memtable budget exhausted: requested {bytes} bytes with {used} of {} bytes already charged",
                        self.inner.limit_bytes
                    ),
                });
            }
            match self.inner.used_bytes.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(MemoryReservation {
                        budget: self.clone(),
                        bytes,
                        committed: false,
                    });
                }
                Err(observed) => used = observed,
            }
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let released =
            self.inner
                .used_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    used.checked_sub(bytes)
                });
        debug_assert!(released.is_ok(), "memtable budget charge underflow");
    }
}

/// A reservation held before the corresponding memtable edit is published.
#[derive(Debug)]
pub(crate) struct MemoryReservation {
    budget: NodeMemtableBudget,
    bytes: usize,
    committed: bool,
}

impl MemoryReservation {
    pub(crate) fn commit(mut self) -> MemtableCharge {
        self.committed = true;
        MemtableCharge {
            inner: Arc::new(MemtableChargeInner {
                budget: self.budget.clone(),
                bytes: AtomicUsize::new(self.bytes),
            }),
        }
    }

    pub(crate) fn commit_into(self, charge: &MemtableCharge) {
        let bytes = self.bytes;
        let budget = self.budget.clone();
        let mut reservation = self;
        reservation.committed = true;
        debug_assert!(Arc::ptr_eq(&budget.inner, &charge.inner.budget.inner));
        charge.inner.bytes.fetch_add(bytes, Ordering::AcqRel);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.budget.release(self.bytes);
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
    bytes: AtomicUsize,
}

impl MemtableCharge {
    pub(crate) fn bytes(&self) -> usize {
        self.inner.bytes.load(Ordering::Acquire)
    }

    pub(crate) fn shrink(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let previous = self.inner.bytes.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes, "memtable charge underflow");
        self.inner.budget.release(bytes);
    }
}

impl Drop for MemtableChargeInner {
    fn drop(&mut self) {
        self.budget.release(self.bytes.load(Ordering::Acquire));
    }
}
