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
pub mod ledger;
pub mod ledger_envelope;
pub mod store;

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use bifrost_types::{ChangeCursor, CursorScope, MembershipScope};

pub use coverage::{
    ClaimLookup, CoverageClaim, PendingCoverage, PublicationId, PublicationReceipt, Publications,
};
pub use envelope::{
    CursorEnvelope, ENGINE_VERSION, EnvelopeKind, MIN_MIGRATABLE, decode_envelope, encode_envelope,
};
pub use ledger::{
    BarrierIncident, DebtLedger, DischargeAudit, DischargeEvidence, LedgerEntry, PolicyStatus,
    ProofStatus, ReplacementProgress, ReplacementRefusal, discharge_fingerprint,
};
pub use ledger_envelope::{
    LEDGER_ENVELOPE_VERSION, MIN_MIGRATABLE_LEDGER, decode_ledger, encode_ledger,
};
pub use store::{
    CheckpointStore, CheckpointTransition, DynCheckpointStore, InMemoryCheckpointStore,
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
    /// Per-scope publication fence, bumped by `delete`. Kept separate
    /// from the account-wide `registry_generation` so deleting one scope
    /// does not invalidate in-flight drives of every OTHER scope, which
    /// would make them discard valid results and repeat their work from
    /// the cursors they started at.
    scope_generations: HashMap<CursorScope, u64>,
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
    generation: DriveGeneration,
}

impl ScopeDriveGuard {
    #[must_use]
    pub fn registry_generation(&self) -> u64 {
        self.generation.registry
    }

    /// The full fence this drive must publish under: the account-wide
    /// registry generation plus the scope's own delete counter.
    #[must_use]
    pub fn drive_generation(&self) -> DriveGeneration {
        self.generation
    }
}

/// The generation pair a drive publishes under.
///
/// `registry` moves when account topology is replaced wholesale;
/// `scope` moves when THIS scope is deleted. Splitting them is what
/// stops one scope's deletion from fencing every sibling drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct DriveGeneration {
    pub registry: u64,
    pub scope: u64,
}

impl DriveGeneration {
    /// Construct a fence pair. The type is `#[non_exhaustive]`, so this
    /// is how a consumer outside the crate names one - `drive_changes_stream`
    /// takes it, and a caller driving that surface directly needs to be
    /// able to build one.
    #[must_use]
    pub fn new(registry: u64, scope: u64) -> Self {
        Self { registry, scope }
    }
}

impl CursorRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Store the latest cursor for a scope.
    ///
    /// Every cursor reaching the registry has already had its outer envelope
    /// version validated at the seam that produced it - the changes and
    /// inventory drive loops, `fuse_inventory_done`, `run_establish`, and the
    /// attach path all map a mismatch to classified schema recovery, and the
    /// envelope decoder migrates and stamps whatever it reads off disk. So an
    /// unsupported version here is unreachable by construction.
    ///
    /// The check is a `debug_assert!` rather than an `assert!` deliberately:
    /// the value is authored by a protocol crate, and a wrong number in a
    /// third-party `Account` impl is a bad-cursor condition the engine already
    /// knows how to recover from at the seams. Aborting the process for it
    /// would trade a recoverable resync for an outage.
    pub fn put(&self, cursor: ChangeCursor) {
        let mut guard = self.state.write().expect("poisoned");
        self.put_locked(&mut guard, cursor);
    }

    fn put_locked(&self, guard: &mut RegistryState, cursor: ChangeCursor) {
        debug_assert!(
            cursor.validate_envelope().is_ok(),
            "CursorRegistry received an unsupported ChangeCursor envelope version"
        );
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
        // Fence any drive already in flight for THIS scope: its
        // publication is refused, and a later incarnation starts from a
        // generation it cannot match. Sibling scopes are untouched.
        *state.scope_generations.entry(scope.clone()).or_default() += 1;
        drop(state);
        // The lease entry deliberately SURVIVES the delete. Removing it
        // would let a re-established scope mint a second, different
        // mutex and drive the same scope concurrently with a drive still
        // running against the old one - the generation fence stops the
        // stale publication, but not the concurrent protocol stream, and
        // exclusive drive is the documented invariant. Entries no drive
        // holds are pruned instead, which is safe precisely because a
        // held lease keeps a second `Arc` alive and `claim_drive` clones
        // it under this same write lock.
        let mut leases = self.drive_leases.write().expect("poisoned");
        leases.retain(|_, lease| Arc::strong_count(lease) > 1);
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
    ///
    /// Published and working, but NOT the door a drive path may use:
    /// [`CursorRegistry::with_drive`] is, because it bounds the lease to the
    /// drive itself. Both call sites here once held a bare claim across a whole
    /// poll iteration, which made a push invalidation wait out up to `poll_max`
    /// of idle sleep - push was then strictly no better than polling. Do not
    /// reintroduce a bare claim in a drive path; this stays only because
    /// removing a published item is not this crate's call.
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
            generation: self.drive_generation(scope),
        }
    }

    /// Current fence pair for a scope.
    #[must_use]
    pub fn drive_generation(&self, scope: &CursorScope) -> DriveGeneration {
        let state = self.state.read().expect("poisoned");
        DriveGeneration {
            registry: self.registry_generation.load(Ordering::SeqCst),
            scope: state.scope_generations.get(scope).copied().unwrap_or(0),
        }
    }

    /// Publish one drive result while BOTH its account generation and
    /// its scope generation are still current.
    ///
    /// This is the form every drive path uses.
    /// [`CursorRegistry::publish_if_generation`] remains available and
    /// unchanged for callers that only hold the account-wide value; it
    /// cannot see a per-scope delete, so a drive that has one should
    /// prefer this.
    pub fn publish_if_drive_generation<R>(
        &self,
        cursor: Option<ChangeCursor>,
        scope: &CursorScope,
        generation: DriveGeneration,
        publish: impl FnOnce() -> R,
    ) -> Option<R> {
        let mut guard = self.state.write().expect("poisoned");
        if self.registry_generation.load(Ordering::SeqCst) != generation.registry {
            return None;
        }
        if guard.scope_generations.get(scope).copied().unwrap_or(0) != generation.scope {
            return None;
        }
        let result = publish();
        if let Some(cursor) = cursor {
            self.put_locked(&mut guard, cursor);
        }
        Some(result)
    }

    /// Run exactly one change-stream drive while holding the scope lease.
    ///
    /// The callback receives the cursor snapshot and registry generation
    /// captured under the lease. The lease is released before this method
    /// returns, so callers cannot accidentally retain it across recovery,
    /// channel backpressure, or cadence sleeps. A poll loop that held the
    /// lease over its whole iteration made a push reconcile wait out the
    /// cadence sleep and every recovery backoff before it could touch the
    /// scope at all.
    ///
    /// The contract that buys is narrow and must not be widened back:
    /// **only the drive is serialized**. Anything that reads the drive's
    /// effect - the cursor it installed, above all - has to be measured
    /// inside this callback, because the reconciler (or the poll loop) may
    /// start its own drive on this scope the instant the future returns.
    /// The generation fence is what protects the durable side:
    /// `publish_if_drive_generation` refuses a publication whose registry OR
    /// scope generation has moved past the one captured here, and a deleted
    /// scope makes the next `with_drive` yield `None` rather than driving a
    /// cursor nobody owns. The lease entry itself survives a delete, so a
    /// re-established scope still waits for this drive to finish.
    pub async fn with_drive<T, F, Fut>(&self, scope: &CursorScope, drive: F) -> Option<T>
    where
        F: FnOnce(ChangeCursor, DriveGeneration) -> Fut,
        Fut: Future<Output = T>,
    {
        let guard = self.claim_drive(scope).await;
        let cursor = self.snapshot(scope)?;
        let generation = guard.drive_generation();
        Some(drive(cursor, generation).await)
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

    /// A delete fences the deleted scope's own in-flight drive.
    #[tokio::test]
    async fn a_deleted_scope_refuses_its_in_flight_drives_publication() {
        let registry = CursorRegistry::new();
        let scope = CursorScope::Account;
        registry.put(cursor(scope.clone(), b"before"));
        let drive = registry.claim_drive(&scope).await;

        registry.delete(&scope);
        let published = registry.publish_if_drive_generation(
            Some(cursor(scope.clone(), b"stale")),
            &scope,
            drive.drive_generation(),
            || (),
        );

        assert!(published.is_none(), "a deleted scope must reject its drive");
        assert!(registry.snapshot(&scope).is_none());
    }

    /// ...but it must not fence anybody ELSE. An account-wide generation
    /// bump on delete made every unrelated scope discard a completed
    /// drive's result and repeat the work from its old cursor.
    #[tokio::test]
    async fn deleting_one_scope_does_not_fence_a_sibling_drive() {
        let registry = CursorRegistry::new();
        let mine = CursorScope::Type(ObjectType::Email);
        let other = CursorScope::Type(ObjectType::CalendarEvent);
        registry.put(cursor(mine.clone(), b"before"));
        registry.put(cursor(other.clone(), b"before"));
        let drive = registry.claim_drive(&mine).await;

        registry.delete(&other);

        let published = registry.publish_if_drive_generation(
            Some(cursor(mine.clone(), b"advanced")),
            &mine,
            drive.drive_generation(),
            || (),
        );
        assert!(
            published.is_some(),
            "an unrelated scope's delete must not invalidate this drive"
        );
        assert_eq!(
            registry
                .snapshot(&mine)
                .expect("sibling cursor")
                .server_state
                .bytes,
            b"advanced"
        );
    }

    /// The exclusive-drive invariant has to survive delete plus
    /// re-establish. Pruning the lease on delete let the new incarnation
    /// mint a second mutex and run concurrently with the old drive's
    /// still-open protocol stream; the generation fence stops the stale
    /// publication but not the concurrent wire work.
    #[tokio::test]
    async fn a_recreated_scope_waits_for_the_previous_incarnations_drive() {
        let registry = Arc::new(CursorRegistry::new());
        let scope = CursorScope::Account;
        registry.put(cursor(scope.clone(), b"first"));
        let first = registry.claim_drive(&scope).await;

        // The scope is deleted and immediately re-established while the
        // first drive is still running.
        registry.delete(&scope);
        registry.put(cursor(scope.clone(), b"second"));

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
        tokio::task::yield_now().await;
        assert!(
            acquired_rx.try_recv().is_err(),
            "the re-established scope drove concurrently with the previous \
             incarnation's still-open drive"
        );
        drop(first);
        acquired_rx.await.expect("contender acquires after release");
        contender.await.expect("contender task completes");
    }

    /// The lease map must not grow without bound just because entries
    /// survive a delete: an entry nobody holds is pruned, which is safe
    /// exactly because a held lease keeps a second `Arc` alive.
    #[tokio::test]
    async fn unheld_leases_are_pruned_on_delete() {
        let registry = CursorRegistry::new();
        let stale = CursorScope::Type(ObjectType::Email);
        let live = CursorScope::Type(ObjectType::CalendarEvent);
        registry.put(cursor(stale.clone(), b"a"));
        registry.put(cursor(live.clone(), b"b"));
        drop(registry.claim_drive(&stale).await);
        let held = registry.claim_drive(&live).await;

        registry.delete(&stale);

        let leases = registry.drive_leases.read().expect("poisoned");
        assert!(
            !leases.contains_key(&stale),
            "an unheld lease is pruned rather than retained forever"
        );
        assert!(
            leases.contains_key(&live),
            "a held lease survives, whichever scope was deleted"
        );
        drop(leases);
        drop(held);
    }
}
