//! What the latest enumeration proved for each scope, pending the
//! acknowledgement that makes it durable.
//!
//! Coverage cannot travel with the checkpoint itself: `Checkpoint::Change`
//! carries a `ChangeCursor`, which is protocol-owned opaque bytes plus an
//! envelope tag, and it crosses the broadcast channel to a consumer and back
//! through `ack_checkpoint`. Widening it would push an engine-internal concept
//! through a published type every consumer matches on.
//!
//! So the producer records coverage here when it emits a checkpoint-bearing
//! batch, and the single durable writer reads it back when the matching
//! acknowledgement arrives. The record it then writes is still ONE atomic
//! store operation carrying both cursor and coverage - this map is in-memory
//! engine state on the path to that write, not a second durable lane.
//!
//! Losing it on a crash is consistent by construction: if the process dies
//! before the acknowledgement, the checkpoint never became durable either, so
//! no cursor advanced and there is nothing to remember.

use std::collections::HashMap;
use std::sync::Mutex;

use bifrost_types::{CursorScope, InventoryCoverage};

/// Per-account map of scope to the coverage of its most recent walk.
#[derive(Debug, Default)]
pub struct PendingCoverage {
    inner: Mutex<HashMap<CursorScope, InventoryCoverage>>,
}

impl PendingCoverage {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record what the latest enumeration of `scope` proved.
    ///
    /// Latest walk wins. A walk that resolves everything reports `Complete`
    /// and clears prior debt for that scope; a walk that leaves obligations
    /// replaces it with its own.
    pub fn record(&self, scope: CursorScope, coverage: InventoryCoverage) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(scope, coverage);
    }

    /// Coverage to persist alongside a checkpoint for `scope`.
    ///
    /// Defaults to `Complete` for a scope no enumeration has reported on -
    /// an ordinary changes-stream advance carries no coverage claim of its
    /// own and must not invent debt.
    ///
    /// Deliberately a read, not a take: debt survives until a later walk
    /// reports otherwise. Consuming it here would let a second acknowledgement
    /// for the same scope silently persist `Complete` over unresolved
    /// obligations.
    #[must_use]
    pub fn for_scope(&self, scope: &CursorScope) -> InventoryCoverage {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(scope)
            .cloned()
            .unwrap_or(InventoryCoverage::Complete)
    }

    /// Whether any scope on this account is carrying unresolved debt.
    #[must_use]
    pub fn any_degraded(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .any(|coverage| !coverage.is_complete())
    }
}

#[cfg(test)]
mod tests {
    use super::PendingCoverage;
    use bifrost_types::{CursorScope, InventoryCoverage, ObjectType};

    fn degraded() -> InventoryCoverage {
        InventoryCoverage::Degraded {
            obligations: Vec::new(),
        }
    }

    #[test]
    fn an_unreported_scope_defaults_to_complete() {
        let coverage = PendingCoverage::new();
        assert!(
            coverage
                .for_scope(&CursorScope::Type(ObjectType::Email))
                .is_complete(),
            "an ordinary changes advance makes no coverage claim and must not invent debt"
        );
    }

    /// Reading must not consume. A consumer may acknowledge more than once for
    /// a scope, and a taking read would let the second acknowledgement persist
    /// `Complete` over debt the first one recorded - silently discharging an
    /// obligation nothing resolved.
    #[test]
    fn reading_coverage_does_not_discharge_it() {
        let coverage = PendingCoverage::new();
        let scope = CursorScope::Type(ObjectType::Email);
        coverage.record(scope.clone(), degraded());

        assert!(!coverage.for_scope(&scope).is_complete());
        assert!(
            !coverage.for_scope(&scope).is_complete(),
            "debt must survive being read"
        );
        assert!(coverage.any_degraded());

        // Only a later walk that resolves everything clears it.
        coverage.record(scope.clone(), InventoryCoverage::Complete);
        assert!(coverage.for_scope(&scope).is_complete());
        assert!(!coverage.any_degraded());
    }
}
