//! Backfill orchestrator.
//!
//! Backfill is the cold-start hydration pass. The partitioner plans
//! newest-first partitions (so a user sees recent mail before deep
//! history), and the runner emits a consumer-ack-deferred checkpoint
//! at every partition boundary.
//!
//! `BackfillPolicy` is configurable per account; the default is
//! `TimeWindowed` with exponentially-widening windows per
//! `bifrost-sync.md` -> Default partition policy.

pub mod partitioner;
pub mod runner;

use bifrost_types::{AccountId, CursorScope};

pub use partitioner::{BackfillPolicy, BackfillStrategy, PartitionPlan, default_time_boundaries};
pub use runner::{BackfillPartitionOutcome, BackfillRunner};

/// Backfill registry: tracks which account scopes are pending, running,
/// or complete. Durable resume state lives in the checkpoint store.
#[derive(Debug, Default)]
pub struct BackfillRegistry {
    inner: dashmap::DashMap<(AccountId, CursorScope), BackfillState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BackfillState {
    Pending,
    Running,
    Completed,
}

impl BackfillRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark(&self, account: AccountId, scope: CursorScope, state: BackfillState) {
        self.inner.insert((account, scope), state);
    }

    #[must_use]
    pub fn snapshot(&self, account: &AccountId, scope: &CursorScope) -> Option<BackfillState> {
        self.inner
            .get(&(account.clone(), scope.clone()))
            .map(|r| *r.value())
    }

    pub fn forget_account(&self, account: &AccountId) {
        self.inner.retain(|(candidate, _), _| candidate != account);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_isolates_identical_scopes_by_account() {
        let registry = BackfillRegistry::new();
        let first = AccountId("first".into());
        let second = AccountId("second".into());
        let scope = CursorScope::Account;

        registry.mark(first.clone(), scope.clone(), BackfillState::Completed);
        registry.mark(second.clone(), scope.clone(), BackfillState::Running);

        assert_eq!(
            registry.snapshot(&first, &scope),
            Some(BackfillState::Completed)
        );
        assert_eq!(
            registry.snapshot(&second, &scope),
            Some(BackfillState::Running)
        );

        registry.forget_account(&first);
        assert_eq!(registry.snapshot(&first, &scope), None);
        assert_eq!(
            registry.snapshot(&second, &scope),
            Some(BackfillState::Running)
        );
    }
}
