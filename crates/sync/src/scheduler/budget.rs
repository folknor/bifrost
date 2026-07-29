//! Concurrency budget.
//!
//! Two-layer `tokio::sync::Semaphore`: per-account first (so two
//! big accounts under one engine do not starve each other), global
//! second. A reserved mutation share carves out a sub-pool so a
//! 200K-flag mutation cannot peg every permit and freeze the
//! multiplexer.
//!
//! Rate-shaping (Gmail's quota-units-per-second budget) is NOT here -
//! it lives in `bifrost-net`. This gate counts concurrent operations,
//! not request rate.

use std::sync::Arc;

use bifrost_types::AccountId;
use dashmap::DashMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::Error;
use crate::scheduler::lanes::WorkKind;

/// Tunable concurrency budget.
#[derive(Debug, Clone, Copy)]
pub struct ConcurrencyBudget {
    /// Per-account permit pool. Default 8.
    pub per_account: usize,
    /// Engine-wide permit pool. Default 64.
    pub global: usize,
    /// Numerator of the per-account mutation share. Default 1.
    ///
    /// Encoded as a rational `(num, den)` so we avoid float math and
    /// avoid clippy's cast lints. Default `(1, 4)` = 25% of
    /// `per_account` reserved for mutation work, rounded up to at
    /// least 1.
    pub mutation_share_num: u32,
    /// Denominator of the per-account mutation share. Default 4.
    pub mutation_share_den: u32,
}

impl Default for ConcurrencyBudget {
    fn default() -> Self {
        Self {
            per_account: 8,
            global: 64,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }
    }
}

impl ConcurrencyBudget {
    /// Reject obviously-degenerate configurations: a zero
    /// `per_account` would mean no account can run any work, and a
    /// zero `mutation_share_num` previously silently rewrote to 1.
    /// Construct the budget through this validated accessor so the
    /// failure surface is a typed `Error` rather than a silent
    /// rewrite.
    ///
    /// Returns `Error::Other` with a descriptive message on
    /// invalid input; the engine surfaces this to the consumer at
    /// build time.
    pub fn validate(&self) -> Result<(), Error> {
        if self.per_account == 0 {
            return Err(Error::Other(
                "ConcurrencyBudget: per_account must be > 0".into(),
            ));
        }
        if self.global == 0 {
            return Err(Error::Other("ConcurrencyBudget: global must be > 0".into()));
        }
        if self.mutation_share_num == 0 {
            return Err(Error::Other(
                "ConcurrencyBudget: mutation_share_num must be > 0".into(),
            ));
        }
        if self.mutation_share_den == 0 {
            return Err(Error::Other(
                "ConcurrencyBudget: mutation_share_den must be > 0".into(),
            ));
        }
        Ok(())
    }

    /// Number of permits carved out of `per_account` for the mutation
    /// sub-pool. Always at least 1 when `per_account >= 1`.
    ///
    /// Callers should call `validate` first; this accessor preserves
    /// the historical `max(1)` rounding for backwards compatibility
    /// with consumers that construct the struct directly.
    #[must_use]
    pub fn mutation_permits(&self) -> usize {
        if self.per_account == 0 {
            return 0;
        }
        let den = self.mutation_share_den.max(1) as usize;
        let num = (self.mutation_share_num as usize).min(den);
        // Round up: (a * num + den - 1) / den.
        let raw = self.per_account.saturating_mul(num).saturating_add(den - 1) / den;
        raw.max(1).min(self.per_account)
    }

    /// Number of permits left in the sync (non-mutation) sub-pool.
    #[must_use]
    pub fn sync_permits(&self) -> usize {
        self.per_account.saturating_sub(self.mutation_permits())
    }
}

/// Engine-internal layered budget gate.
#[derive(Clone)]
pub struct BudgetGate {
    inner: Arc<BudgetInner>,
}

impl std::fmt::Debug for BudgetGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetGate")
            .field("budget", &self.inner.budget)
            .finish_non_exhaustive()
    }
}

struct BudgetInner {
    budget: ConcurrencyBudget,
    global: Arc<Semaphore>,
    account_sync: DashMap<AccountId, Arc<Semaphore>>,
    account_mutation: DashMap<AccountId, Arc<Semaphore>>,
}

impl BudgetGate {
    #[must_use]
    pub fn new(budget: ConcurrencyBudget) -> Self {
        let global_permits = budget.global.max(1);
        Self {
            inner: Arc::new(BudgetInner {
                budget,
                global: Arc::new(Semaphore::new(global_permits)),
                account_sync: DashMap::new(),
                account_mutation: DashMap::new(),
            }),
        }
    }

    /// Ensure per-account semaphores exist for `account` so the first
    /// `acquire` call doesn't race with later config changes. Called
    /// from `SyncEngine::attach`.
    pub fn register(&self, account: AccountId) {
        let sync = Arc::new(Semaphore::new(self.inner.budget.sync_permits().max(1)));
        let mutation = Arc::new(Semaphore::new(self.inner.budget.mutation_permits().max(1)));
        self.inner.account_sync.insert(account.clone(), sync);
        self.inner.account_mutation.insert(account, mutation);
    }

    /// Drop per-account semaphores. Called from `SyncEngine::detach`.
    pub fn forget(&self, account: &AccountId) {
        self.inner.account_sync.remove(account);
        self.inner.account_mutation.remove(account);
    }

    /// Acquire a permit. Returns a `BudgetPermit` that releases the
    /// permit on drop.
    ///
    /// `kind` picks between the sync and mutation sub-pools. The
    /// global outer permit is shared.
    pub async fn acquire(
        &self,
        account: &AccountId,
        kind: WorkKind,
    ) -> Result<BudgetPermit, Error> {
        let inner_sem = match kind {
            WorkKind::Sync => self.sync_semaphore(account),
            WorkKind::Mutation => self.mutation_semaphore(account),
        };
        let inner = inner_sem
            .acquire_owned()
            .await
            .map_err(|e| Error::Other(format!("account semaphore closed: {e}")))?;
        let outer = Arc::clone(&self.inner.global)
            .acquire_owned()
            .await
            .map_err(|e| Error::Other(format!("global semaphore closed: {e}")))?;
        Ok(BudgetPermit {
            _outer: outer,
            _inner: inner,
        })
    }

    fn sync_semaphore(&self, account: &AccountId) -> Arc<Semaphore> {
        // `entry().or_insert_with` is atomic on `DashMap`; this
        // closes the race where two concurrent `acquire` calls on an
        // unregistered account would each allocate a fresh
        // `Semaphore`, with the loser then gating against an orphan
        // that has no permits drained by the winner's holders.
        let permits = self.inner.budget.sync_permits().max(1);
        self.inner
            .account_sync
            .entry(account.clone())
            .or_insert_with(|| Arc::new(Semaphore::new(permits)))
            .clone()
    }

    fn mutation_semaphore(&self, account: &AccountId) -> Arc<Semaphore> {
        let permits = self.inner.budget.mutation_permits().max(1);
        self.inner
            .account_mutation
            .entry(account.clone())
            .or_insert_with(|| Arc::new(Semaphore::new(permits)))
            .clone()
    }
}

/// Held permit; releases on drop.
pub struct BudgetPermit {
    _outer: OwnedSemaphorePermit,
    _inner: OwnedSemaphorePermit,
}

impl std::fmt::Debug for BudgetPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetPermit").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn account_waiter_does_not_hold_a_global_permit() {
        let budget = ConcurrencyBudget {
            per_account: 2,
            global: 2,
            mutation_share_num: 1,
            mutation_share_den: 2,
        };
        let gate = BudgetGate::new(budget);
        let account_a = AccountId("a".into());
        let account_b = AccountId("b".into());
        gate.register(account_a.clone());
        gate.register(account_b.clone());

        let held = gate
            .acquire(&account_a, WorkKind::Sync)
            .await
            .expect("first account permit");
        let waiting_gate = gate.clone();
        let waiting_account = account_a.clone();
        let waiter =
            tokio::spawn(
                async move { waiting_gate.acquire(&waiting_account, WorkKind::Sync).await },
            );
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Without this the permit assertion below passes vacuously if
        // the waiter never got scheduled: it has to be parked on the
        // account semaphore for "does it hold a global permit" to mean
        // anything.
        assert!(
            !waiter.is_finished(),
            "waiter must be parked on the exhausted account semaphore"
        );
        assert_eq!(
            gate.inner.global.available_permits(),
            1,
            "an account-local waiter must not consume the spare global permit"
        );
        let other = gate
            .acquire(&account_b, WorkKind::Sync)
            .await
            .expect("other account uses spare global permit");

        drop(other);
        drop(held);
        waiter
            .await
            .expect("waiter task completes")
            .expect("waiter acquires after release");
    }
}
