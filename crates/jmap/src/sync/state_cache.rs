//! Per-`(accountId, ObjectType)` JMAP state cache.
//!
//! JMAP `Email/changes`, `Mailbox/changes`, and `Thread/changes` state
//! strings are per-`accountId` on the server (RFC 8620 §1.5.2 - state is
//! a property of an `{accountId, type}` pair). A single shared
//! `Option<String>` cannot hold the primary account's state and a
//! foreign (shared/delegate) account's state at once, so the cache is
//! keyed by JMAP `accountId`. The primary account is one ordinary key
//! (`self.mail.id_str()`); there is no primary-vs-foreign branch in the
//! cache itself, only in *which* key a call site looks up.
//!
//! The inner `Option<String>` preserves the tri-state the scalar form
//! had: an absent key means "never probed"; `Some(None)` means "probed,
//! empty"; `Some(Some(s))` means "known state".

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

/// Per-`accountId` state cache for one JMAP object type.
pub(crate) type StateMap = Arc<Mutex<HashMap<String, Option<String>>>>;

/// The current cached state for an account, or `None` when the account
/// has never been probed (absent key) or was probed empty (`Some(None)`).
pub(crate) async fn get(map: &StateMap, account_id: &str) -> Option<String> {
    let guard = map.lock().await;
    guard.get(account_id).cloned().flatten()
}

/// Unconditionally set the cached state for an account.
pub(crate) async fn set(map: &StateMap, account_id: &str, state: String) {
    let mut guard = map.lock().await;
    guard.insert(account_id.to_string(), Some(state));
}

/// Advance the cached state for an account to `state`, but only when the
/// current value exactly matches `expected` (last-writer-wins guard against
/// a concurrent advance racing ahead). `None` matches only an absent or
/// explicitly empty entry.
pub(crate) async fn advance(
    map: &StateMap,
    account_id: &str,
    expected: Option<&str>,
    state: String,
) {
    let mut guard = map.lock().await;
    match guard.get(account_id).and_then(|entry| entry.as_deref()) {
        current if current == expected => {
            guard.insert(account_id.to_string(), Some(state));
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map() -> StateMap {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[tokio::test]
    async fn absent_account_reads_none() {
        let m = map();
        assert_eq!(get(&m, "acct-a").await, None);
    }

    #[tokio::test]
    async fn set_and_get_round_trip_per_account() {
        let m = map();
        set(&m, "acct-a", "s-a".to_string()).await;
        set(&m, "acct-b", "s-b".to_string()).await;
        assert_eq!(get(&m, "acct-a").await, Some("s-a".to_string()));
        assert_eq!(get(&m, "acct-b").await, Some("s-b".to_string()));
    }

    #[tokio::test]
    async fn advance_respects_expected_guard() {
        let m = map();
        set(&m, "acct-a", "s1".to_string()).await;
        // Expected mismatch: do not advance.
        advance(&m, "acct-a", Some("other"), "s2".to_string()).await;
        assert_eq!(get(&m, "acct-a").await, Some("s1".to_string()));
        // Expected match: advance.
        advance(&m, "acct-a", Some("s1"), "s2".to_string()).await;
        assert_eq!(get(&m, "acct-a").await, Some("s2".to_string()));
    }

    #[tokio::test]
    async fn advance_with_expected_state_does_not_initialize_absent_entry() {
        let m = map();
        advance(&m, "acct-a", Some("ignored"), "s1".to_string()).await;
        assert_eq!(get(&m, "acct-a").await, None);
    }

    #[tokio::test]
    async fn advance_without_expected_state_initializes_absent_entry() {
        let m = map();
        advance(&m, "acct-a", None, "s1".to_string()).await;
        assert_eq!(get(&m, "acct-a").await, Some("s1".to_string()));
    }
}
