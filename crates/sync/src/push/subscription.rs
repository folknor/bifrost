//! Push subscription tracking.
//!
//! `Account::push_subscribe` returns a `SubscriptionHandle` the engine
//! stashes here so the consumer can `unsubscribe_push(account_id)`
//! later. The registry is per-engine, not per-account.

use bifrost_types::{AccountId, CursorScope, SubscriptionHandle};
use dashmap::DashMap;

#[derive(Debug, Clone)]
pub(crate) struct RegisteredSubscription {
    pub handle: SubscriptionHandle,
    pub scopes: Vec<CursorScope>,
}

/// Per-engine subscription handle registry.
#[derive(Debug, Default)]
pub struct SubscriptionRegistry {
    inner: DashMap<AccountId, Vec<RegisteredSubscription>>,
}

impl SubscriptionRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a handle for an account.
    pub fn record(&self, account: AccountId, handle: SubscriptionHandle, scopes: Vec<CursorScope>) {
        self.inner
            .entry(account)
            .or_default()
            .push(RegisteredSubscription { handle, scopes });
    }

    /// Take all handles registered for an account. Returns an empty
    /// vec if none.
    #[must_use]
    pub fn take(&self, account: &AccountId) -> Vec<SubscriptionHandle> {
        match self.inner.remove(account) {
            Some((_, v)) => v.into_iter().map(|record| record.handle).collect(),
            None => Vec::new(),
        }
    }

    #[must_use]
    pub(crate) fn snapshot(&self, account: &AccountId) -> Vec<RegisteredSubscription> {
        self.inner
            .get(account)
            .map(|records| records.clone())
            .unwrap_or_default()
    }

    pub(crate) fn replace(&self, account: AccountId, records: Vec<RegisteredSubscription>) {
        if records.is_empty() {
            self.inner.remove(&account);
        } else {
            self.inner.insert(account, records);
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
