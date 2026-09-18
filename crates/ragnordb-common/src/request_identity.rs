//! Shared V2 client-session and retry-horizon contracts.
//!
//! The ledger is deliberately small and deterministic. Metadata/tablet
//! implementations may persist its fields in their own replicated state, but
//! every implementation must use the same fail-closed expiry decisions.

use crate::ids::{ClientRequestId, LogicalCommandId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryHorizonDecision {
    RetainOutcome,
    QueryOriginalOutcome,
    RequestIdExpired,
}

/// Monotonic compaction boundary for one registered client session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryHorizon {
    pub acknowledged_through: u64,
    pub first_retained_sequence: u64,
    pub highest_issued_sequence: u64,
}

impl Default for RetryHorizon {
    fn default() -> Self {
        Self::new()
    }
}

impl RetryHorizon {
    pub const fn new() -> Self {
        Self {
            acknowledged_through: 0,
            first_retained_sequence: 1,
            highest_issued_sequence: 0,
        }
    }

    pub fn observe_issued(&mut self, sequence: u64) -> Result<(), &'static str> {
        if sequence == 0 {
            return Err("request sequence must be non-zero");
        }
        self.highest_issued_sequence = self.highest_issued_sequence.max(sequence);
        Ok(())
    }

    pub fn acknowledge(&mut self, sequence: u64) -> Result<(), &'static str> {
        if sequence < self.acknowledged_through {
            return Err("acknowledged request floor cannot regress");
        }
        self.acknowledged_through = sequence;
        self.first_retained_sequence = self.first_retained_sequence.max(sequence.saturating_add(1));
        Ok(())
    }

    pub const fn decide(&self, sequence: u64) -> RetryHorizonDecision {
        if sequence < self.first_retained_sequence {
            RetryHorizonDecision::RequestIdExpired
        } else if sequence <= self.highest_issued_sequence {
            RetryHorizonDecision::RetainOutcome
        } else {
            RetryHorizonDecision::QueryOriginalOutcome
        }
    }
}

/// Durable identity state for one registered client session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientRetrySession {
    pub client_id: u128,
    pub session_epoch: u64,
    pub next_sequence: u64,
    pub horizon: RetryHorizon,
}

impl ClientRetrySession {
    pub fn new(client_id: u128, session_epoch: u64) -> Result<Self, &'static str> {
        if client_id == 0 || session_epoch == 0 {
            return Err("client retry session identity must be non-zero");
        }
        Ok(Self {
            client_id,
            session_epoch,
            next_sequence: 1,
            horizon: RetryHorizon::new(),
        })
    }

    pub fn next_request_id(&mut self) -> Result<ClientRequestId, &'static str> {
        let request_sequence = self.next_sequence;
        self.horizon.observe_issued(request_sequence)?;
        self.next_sequence = request_sequence
            .checked_add(1)
            .ok_or("client request sequence space is exhausted")?;
        Ok(ClientRequestId {
            client_id: self.client_id,
            session_epoch: self.session_epoch,
            request_sequence,
        })
    }

    pub fn derive_command_id(
        &self,
        root: ClientRequestId,
        command_ordinal: u32,
        kind: crate::ids::CommandKind,
    ) -> Result<LogicalCommandId, &'static str> {
        if root.client_id != self.client_id || root.session_epoch != self.session_epoch {
            return Err("logical command belongs to another client session");
        }
        let identity = LogicalCommandId {
            client_request_id: root,
            command_ordinal,
            kind,
        };
        identity.validate()?;
        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientRetrySession, RetryHorizon, RetryHorizonDecision};

    #[test]
    fn acknowledged_floor_is_monotonic_and_expires_old_ids() {
        let mut horizon = RetryHorizon::new();
        horizon.acknowledge(3).unwrap();
        assert_eq!(horizon.decide(2), RetryHorizonDecision::RequestIdExpired);
        assert_eq!(
            horizon.decide(4),
            RetryHorizonDecision::QueryOriginalOutcome
        );
        assert!(horizon.acknowledge(2).is_err());
    }

    #[test]
    fn session_derives_identity_without_topology_fields() {
        let mut session = ClientRetrySession::new(7, 2).unwrap();
        let root = session.next_request_id().unwrap();
        let logical = session
            .derive_command_id(root, 1, crate::ids::CommandKind::Noop)
            .unwrap();
        assert_eq!(logical.client_request_id, root);
        assert_eq!(
            session.horizon.decide(root.request_sequence),
            RetryHorizonDecision::RetainOutcome
        );
    }
}
