//! Cursor registry, envelope versioning, and checkpoint store
//! plumbing.
//!
//! The cursor lifecycle:
//! 1. `establish_initial_cursor(scope)` produces either a `Ready` or
//!    `EstablishViaInventory` outcome.
//! 2. The engine persists the established cursor through the
//!    `CheckpointStore` trait.
//! 3. Multiplexer, backfill, and reconciler consult the
//!    `CursorRegistry` at every batch boundary; advance-checkpoints
//!    rewrite both the registry and the store atomically.

pub mod coverage;
pub mod envelope;
pub mod store;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use bifrost_types::{ChangeCursor, CursorScope, MembershipScope};

pub use coverage::PendingCoverage;
pub use envelope::{
    CursorEnvelope, ENGINE_VERSION, EnvelopeKind, MIN_MIGRATABLE, decode_envelope, encode_envelope,
};
pub use store::{
    BackfillCheckpointRecord, ChangeCheckpointRecord, CheckpointStore, DynCheckpointStore,
    InMemoryCheckpointStore,
};

/// In-memory cursor registry. Holds the latest known `ChangeCursor`
/// per `(account, scope)` pair plus a side index from `MembershipScope`
/// back to the cursor scopes that membership belongs to (push reconciler
/// uses the side index to enumerate `scopes_for_hint`).
#[derive(Debug, Default, Clone)]
struct RegistryState {
    cursors: HashMap<CursorScope, ChangeCursor>,
    membership_index: HashMap<MembershipScope, Vec<CursorScope>>,
    incarnations: HashMap<CursorScope, u64>,
}

#[derive(Debug, Default)]
pub struct CursorRegistry {
    state: RwLock<RegistryState>,
    drive_leases: RwLock<HashMap<CursorScope, Arc<AsyncMutex<()>>>>,
    next_incarnation: AtomicU64,
    registry_generation: AtomicU64,
}

pub struct ScopeDriveGuard {
    _guard: OwnedMutexGuard<()>,
    registry_generation: u64,
}

impl ScopeDriveGuard {
    #[must_use]
    pub fn registry_generation(&self) -> u64 {
        self.registry_generation
    }
}

impl CursorRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Store the latest cursor for a scope.
    pub fn put(&self, cursor: ChangeCursor) {
        let mut guard = self.state.write().expect("poisoned");
        self.put_locked(&mut guard, cursor);
    }

    fn put_locked(&self, guard: &mut RegistryState, cursor: ChangeCursor) {
        if !guard.cursors.contains_key(&cursor.scope) {
            let incarnation = self.next_incarnation.fetch_add(1, Ordering::Relaxed);
            guard.incarnations.insert(cursor.scope.clone(), incarnation);
        }
        guard.cursors.insert(cursor.scope.clone(), cursor);
    }

    /// Publish one drive result while its account generation is still
    /// current. The callback and optional cursor install happen under the
    /// topology lock, making them atomic with reattach's generation fence.
    pub fn publish_if_generation<R>(
        &self,
        cursor: Option<ChangeCursor>,
        generation: u64,
        publish: impl FnOnce() -> R,
    ) -> Option<R> {
        let mut guard = self.state.write().expect("poisoned");
        if self.registry_generation.load(Ordering::SeqCst) != generation {
            return None;
        }
        let result = publish();
        if let Some(cursor) = cursor {
            self.put_locked(&mut guard, cursor);
        }
        Some(result)
    }

    /// Drop the cursor for a scope and any membership index entries
    /// that referenced it. Used by `ScopeLifecycle::Deleted` and by
    /// the engine's `EngineDirective::RestartScope` recovery path.
    pub fn delete(&self, scope: &CursorScope) {
        let mut state = self.state.write().expect("poisoned");
        state.cursors.remove(scope);
        state.incarnations.remove(scope);
        for entry in state.membership_index.values_mut() {
            entry.retain(|s| s != scope);
        }
        state
            .membership_index
            .retain(|_, scopes| !scopes.is_empty());
    }

    /// Read a snapshot of the cursor for a scope.
    #[must_use]
    pub fn snapshot(&self, scope: &CursorScope) -> Option<ChangeCursor> {
        let guard = self.state.read().expect("poisoned");
        guard.cursors.get(scope).cloned()
    }

    /// Register a membership -> cursor-scope edge. Called when the
    /// engine discovers that a specific membership is covered by one
    /// or more cursor scopes (e.g. a JMAP mailbox is covered by the
    /// `Type(Email)` cursor and any `Query` cursor with that mailbox
    /// as filter).
    pub fn link_membership(&self, membership: MembershipScope, scope: CursorScope) {
        let mut guard = self.state.write().expect("poisoned");
        let entry = guard.membership_index.entry(membership).or_default();
        if !entry.contains(&scope) {
            entry.push(scope);
        }
    }

    /// Enumerate the cursor scopes covering a membership.
    #[must_use]
    pub fn scopes_for_membership(&self, membership: &MembershipScope) -> Vec<CursorScope> {
        let guard = self.state.read().expect("poisoned");
        guard
            .membership_index
            .get(membership)
            .cloned()
            .unwrap_or_default()
    }

    /// Enumerate every known cursor scope. Used by the reconciler on
    /// `HintPayload::Unknown`.
    #[must_use]
    pub fn all_scopes(&self) -> Vec<CursorScope> {
        let guard = self.state.read().expect("poisoned");
        guard.cursors.keys().cloned().collect()
    }

    /// Enumerate scope identities together with the incarnation assigned
    /// when each scope entered the registry. Cursor advancement preserves
    /// the value; delete plus re-establish receives a new one.
    ///
    /// Enumeration walks `cursors`, not `incarnations`, so a scope can
    /// never be reported here without a live cursor: the backfill
    /// orchestrator drives its rescan off this list and would otherwise
    /// plan a walk for a scope the registry has already dropped. The
    /// incarnation map is an annotation on `cursors`, and that
    /// direction of dependency is what keeps the two from drifting.
    #[must_use]
    pub fn all_scope_incarnations(&self) -> Vec<(CursorScope, u64)> {
        let guard = self.state.read().expect("poisoned");
        guard
            .cursors
            .keys()
            .map(|scope| {
                let incarnation = guard.incarnations.get(scope).copied();
                debug_assert!(
                    incarnation.is_some(),
                    "every registered cursor carries an incarnation"
                );
                (scope.clone(), incarnation.unwrap_or(0))
            })
            .collect()
    }

    /// Atomically replace cursor membership topology from a fully-discovered
    /// staging registry while overlaying the latest live cursor for every
    /// retained scope. Account reopen builds the replacement off to the side;
    /// workers see neither a half-refreshed index nor an older staged cursor.
    pub fn replace_topology_preserving_cursors(&self, replacement: &Self) {
        let mut replacement = replacement.state.read().expect("poisoned").clone();
        let mut current = self.state.write().expect("poisoned");
        for (scope, cursor) in &current.cursors {
            if replacement.cursors.contains_key(scope) {
                replacement.cursors.insert(scope.clone(), cursor.clone());
            }
        }
        self.registry_generation.fetch_add(1, Ordering::SeqCst);
        for scope in replacement.cursors.keys() {
            let incarnation = current
                .incarnations
                .get(scope)
                .copied()
                .unwrap_or_else(|| self.next_incarnation.fetch_add(1, Ordering::Relaxed));
            replacement.incarnations.insert(scope.clone(), incarnation);
        }
        *current = replacement;
    }

    /// Claim exclusive ownership of a scope's change cursor drive.
    /// Polling and push reconciliation use the same lease, so no two
    /// producers can start from and advance one scope concurrently.
    pub async fn claim_drive(&self, scope: &CursorScope) -> ScopeDriveGuard {
        let lease = {
            let mut leases = self.drive_leases.write().expect("poisoned");
            Arc::clone(
                leases
                    .entry(scope.clone())
                    .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
            )
        };
        let guard = lease.lock_owned().await;
        ScopeDriveGuard {
            _guard: guard,
            registry_generation: self.registry_generation.load(Ordering::SeqCst),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bifrost_types::{
        ChangeCursor, CursorScope, MailboxId, MembershipScope, ObjectType, OpaqueChangeState,
        ProtocolKind,
    };
    use tokio::sync::oneshot;

    use super::CursorRegistry;

    fn cursor(scope: CursorScope, state: &[u8]) -> ChangeCursor {
        ChangeCursor {
            scope,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Jmap,
                envelope_version: 1,
                bytes: state.to_vec(),
            },
            advanced_through: None,
            envelope_version: 1,
        }
    }

    #[tokio::test]
    async fn one_scope_drive_lease_has_exactly_one_holder() {
        let registry = Arc::new(CursorRegistry::new());
        let scope = CursorScope::Account;
        let first = registry.claim_drive(&scope).await;
        let (started_tx, started_rx) = oneshot::channel();
        let (acquired_tx, mut acquired_rx) = oneshot::channel();
        let contender_registry = Arc::clone(&registry);
        let contender_scope = scope.clone();
        let contender = tokio::spawn(async move {
            started_tx.send(()).expect("test receiver remains live");
            let _guard = contender_registry.claim_drive(&contender_scope).await;
            acquired_tx.send(()).expect("test receiver remains live");
        });

        started_rx.await.expect("contender reached the claim");
        tokio::task::yield_now().await;
        assert!(
            acquired_rx.try_recv().is_err(),
            "a second producer acquired the same scope lease"
        );
        drop(first);
        acquired_rx.await.expect("contender acquires after release");
        contender.await.expect("contender task completes");
    }

    #[test]
    fn replacement_swaps_cursor_and_membership_as_one_state() {
        let live = CursorRegistry::new();
        live.put(cursor(CursorScope::Account, b"old"));
        live.link_membership(
            MembershipScope::Mailbox(MailboxId("old".into())),
            CursorScope::Account,
        );

        let replacement = CursorRegistry::new();
        let scope = CursorScope::Type(ObjectType::Email);
        replacement.put(cursor(scope.clone(), b"new"));
        let membership = MembershipScope::Mailbox(MailboxId("new".into()));
        replacement.link_membership(membership.clone(), scope.clone());

        live.replace_topology_preserving_cursors(&replacement);

        assert!(live.snapshot(&CursorScope::Account).is_none());
        assert_eq!(
            live.snapshot(&scope)
                .expect("new cursor")
                .server_state
                .bytes,
            b"new"
        );
        assert_eq!(live.scopes_for_membership(&membership), vec![scope]);
        assert!(
            live.scopes_for_membership(&MembershipScope::Mailbox(MailboxId("old".into())))
                .is_empty()
        );
    }

    #[test]
    fn delete_and_reestablish_changes_scope_incarnation() {
        let registry = CursorRegistry::new();
        let scope = CursorScope::Account;
        registry.put(cursor(scope.clone(), b"one"));
        let first = registry.all_scope_incarnations()[0].1;
        registry.put(cursor(scope.clone(), b"two"));
        assert_eq!(registry.all_scope_incarnations()[0].1, first);
        registry.delete(&scope);
        registry.put(cursor(scope, b"three"));
        assert_ne!(registry.all_scope_incarnations()[0].1, first);
    }

    #[tokio::test]
    async fn replacement_rejects_a_cursor_from_the_old_account_generation() {
        let live = CursorRegistry::new();
        let scope = CursorScope::Account;
        live.put(cursor(scope.clone(), b"before"));
        let old_drive = live.claim_drive(&scope).await;

        let replacement = CursorRegistry::new();
        replacement.put(cursor(scope.clone(), b"cutover"));
        live.replace_topology_preserving_cursors(&replacement);
        let published = live.publish_if_generation(
            Some(cursor(scope.clone(), b"stale")),
            old_drive.registry_generation(),
            || (),
        );

        assert!(published.is_none());
        assert_eq!(
            live.snapshot(&scope)
                .expect("replacement cursor")
                .server_state
                .bytes,
            b"before"
        );
    }
}
