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
    /// the subscription may still be live on the provider. A further failed
    /// retry is logged rather than aborting the caller - the handle may belong
    /// to a connection that is already gone, and one unreachable orphan must
    /// not wedge every future reopen.
    ///
    /// This says nothing about whether the consumer still wants the coverage;
    /// that is `desired`. The two were once one flag, and a reopen whose
    /// old-side teardown failed then treated the consumer's live subscription
    /// as an orphan: the next attempt carried the old handle but never
    /// recreated it on the replacement, so the subscription silently vanished.
    pub teardown_unconfirmed: bool,
    /// The consumer still wants this subscription's coverage, so a reopen
    /// recreates it on the replacement connection. False for a pure orphan -
    /// a handle the consumer asked to tear down, a replacement-side handle
    /// unwound by an aborted reopen, or an old handle carried past a
    /// committed reopen whose desire now lives in the recreated record - which
    /// is kept only so its server-side teardown can be retried and is never
    /// recreated. A record is always `desired`, `teardown_unconfirmed`, or
    /// both; `into_orphan` is the only way to clear `desired`.
    pub desired: bool,
    /// The old-side teardown of this handle already SUCCEEDED in a reopen
    /// attempt that then aborted, so the server-side subscription is gone and
    /// the record survives only as the consumer's desire, to be recreated by
    /// the next attempt. Such a record is never handed to `push_unsubscribe`
    /// again (a provider that errors on an unknown handle would turn that into
    /// another abort) and is dropped, not torn down, by `take`. Implies
    /// `desired` and not `teardown_unconfirmed`.
    pub torn_down: bool,
}

impl RegisteredSubscription {
    /// This record as an orphan: its server-side teardown is unconfirmed and
    /// nobody wants its coverage any more, so it is carried for retry and
    /// never recreated.
    #[must_use]
    pub(crate) fn into_orphan(self) -> Self {
        Self {
            teardown_unconfirmed: true,
            desired: false,
            torn_down: false,
            ..self
        }
    }
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
                desired: true,
                torn_down: false,
            });
    }

    /// Take all records registered for an account that still have a
    /// server-side handle to tear down. Returns an empty vec if none. Callers
    /// that cannot tear down a handle restore its record so the same
    /// account-side handle remains retryable. A `torn_down` record is dropped
    /// here: its server side is already gone and the caller taking the
    /// registry (a consumer unsubscribe, a detach) is ending the desire it
    /// carried.
    #[must_use]
    pub(crate) fn take(&self, account: &AccountId) -> Vec<RegisteredSubscription> {
        match self.inner.remove(account) {
            Some((_, mut v)) => {
                v.retain(|record| !record.torn_down);
                v
            }
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
    /// its failure as a reason to abandon the swap. `desired` is left alone:
    /// the caller is an aborted reopen, which leaves the old account
    /// installed and the consumer still wanting the coverage, so the next
    /// attempt must recreate it on its replacement.
    pub(crate) fn mark_unconfirmed(&self, account: &AccountId, handle: &SubscriptionHandle) {
        if let Some(mut records) = self.inner.get_mut(account) {
            for record in records.iter_mut() {
                if &record.handle == handle {
                    record.teardown_unconfirmed = true;
                }
            }
        }
    }

    /// Record that a still-registered handle's server-side teardown
    /// SUCCEEDED in a reopen attempt that may yet abort. A wanted record
    /// stays, flagged `torn_down`, so the next attempt recreates it without
    /// unsubscribing the dead handle again; a pure orphan has nothing left to
    /// carry and is removed.
    pub(crate) fn mark_torn_down(&self, account: &AccountId, handle: &SubscriptionHandle) {
        if let Some(mut records) = self.inner.get_mut(account) {
            records.retain_mut(|record| {
                if &record.handle != handle {
                    return true;
                }
                if record.desired {
                    record.torn_down = true;
                    record.teardown_unconfirmed = false;
                    true
                } else {
                    false
                }
            });
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
