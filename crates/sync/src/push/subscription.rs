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
    /// A server-side teardown for this handle was attempted and failed, so
    /// the subscription may still be live on the provider. The record is kept
    /// purely so the teardown can be retried; it is never recreated against a
    /// replacement connection, and a further failed retry is logged rather
    /// than aborting the caller - the handle may belong to a connection that
    /// is already gone, and one unreachable orphan must not wedge every
    /// future reopen.
    pub teardown_unconfirmed: bool,
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
            .push(RegisteredSubscription {
                handle,
                scopes,
                teardown_unconfirmed: false,
            });
    }

    /// Take all records registered for an account. Returns an empty
    /// vec if none. Callers that cannot tear down a handle restore its
    /// record so the same account-side handle remains retryable.
    #[must_use]
    pub(crate) fn take(&self, account: &AccountId) -> Vec<RegisteredSubscription> {
        match self.inner.remove(account) {
            Some((_, v)) => v,
            None => Vec::new(),
        }
    }

    /// Restore records whose server-side teardown failed. Merge rather than
    /// replace so a concurrent `subscribe_push` registration is preserved.
    pub(crate) fn restore(&self, account: AccountId, records: Vec<RegisteredSubscription>) {
        if records.is_empty() {
            return;
        }
        self.inner.entry(account).or_default().extend(records);
    }

    /// Flag a still-registered handle whose server-side teardown failed.
    /// Reopen keeps carrying the record and retrying it, but stops treating
    /// its failure as a reason to abandon the swap.
    pub(crate) fn mark_unconfirmed(&self, account: &AccountId, handle: &SubscriptionHandle) {
        if let Some(mut records) = self.inner.get_mut(account) {
            for record in records.iter_mut() {
                if &record.handle == handle {
                    record.teardown_unconfirmed = true;
                }
            }
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
