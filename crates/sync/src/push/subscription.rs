//! Push subscription tracking.
//!
//! `Account::push_subscribe` returns a `SubscriptionHandle` the engine
//! stashes here so the consumer can `unsubscribe_push(account_id)`
//! later. The registry is per-engine, not per-account.

use bifrost_types::{AccountId, SubscriptionHandle};
use dashmap::DashMap;

/// Per-engine subscription handle registry.
#[derive(Debug, Default)]
pub struct SubscriptionRegistry {
    inner: DashMap<AccountId, Vec<SubscriptionHandle>>,
}

impl SubscriptionRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a handle for an account.
    pub fn record(&self, account: AccountId, handle: SubscriptionHandle) {
        self.inner.entry(account).or_default().push(handle);
    }

    /// Take all handles registered for an account. Returns an empty
    /// vec if none.
    #[must_use]
    pub fn take(&self, account: &AccountId) -> Vec<SubscriptionHandle> {
        match self.inner.remove(account) {
            Some((_, v)) => v,
            None => Vec::new(),
        }
    }

    /// Snapshot for tests.
    #[must_use]
    pub fn len(&self, account: &AccountId) -> usize {
        self.inner.get(account).map(|v| v.len()).unwrap_or(0)
    }

    /// Convenience: total subscription count across accounts.
    #[must_use]
    pub fn total(&self) -> usize {
        self.inner.iter().map(|r| r.value().len()).sum()
    }
}
