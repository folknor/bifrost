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

pub mod envelope;
pub mod store;

use std::collections::HashMap;
use std::sync::RwLock;

use bifrost_types::{ChangeCursor, CursorScope, MembershipScope};

pub use envelope::{
    CursorEnvelope, ENGINE_VERSION, EnvelopeKind, MIN_MIGRATABLE, decode_envelope, encode_envelope,
};
pub use store::{CheckpointStore, DynCheckpointStore, InMemoryCheckpointStore};

/// In-memory cursor registry. Holds the latest known `ChangeCursor`
/// per `(account, scope)` pair plus a side index from `MembershipScope`
/// back to the cursor scopes that membership belongs to (push reconciler
/// uses the side index to enumerate `scopes_for_hint`).
#[derive(Debug, Default)]
pub struct CursorRegistry {
    cursors: RwLock<HashMap<CursorScope, ChangeCursor>>,
    membership_index: RwLock<HashMap<MembershipScope, Vec<CursorScope>>>,
}

impl CursorRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Store the latest cursor for a scope.
    pub fn put(&self, cursor: ChangeCursor) {
        let mut guard = self.cursors.write().expect("poisoned");
        guard.insert(cursor.scope.clone(), cursor);
    }

    /// Drop the cursor for a scope and any membership index entries
    /// that referenced it. Used by `ScopeLifecycle::Deleted` and by
    /// the engine's `EngineDirective::RestartScope` recovery path.
    pub fn delete(&self, scope: &CursorScope) {
        {
            let mut guard = self.cursors.write().expect("poisoned");
            guard.remove(scope);
        }
        let mut idx = self.membership_index.write().expect("poisoned");
        for entry in idx.values_mut() {
            entry.retain(|s| s != scope);
        }
        idx.retain(|_, scopes| !scopes.is_empty());
    }

    /// Read a snapshot of the cursor for a scope.
    #[must_use]
    pub fn snapshot(&self, scope: &CursorScope) -> Option<ChangeCursor> {
        let guard = self.cursors.read().expect("poisoned");
        guard.get(scope).cloned()
    }

    /// Register a membership -> cursor-scope edge. Called when the
    /// engine discovers that a specific membership is covered by one
    /// or more cursor scopes (e.g. a JMAP mailbox is covered by the
    /// `Type(Email)` cursor and any `Query` cursor with that mailbox
    /// as filter).
    pub fn link_membership(&self, membership: MembershipScope, scope: CursorScope) {
        let mut guard = self.membership_index.write().expect("poisoned");
        let entry = guard.entry(membership).or_default();
        if !entry.contains(&scope) {
            entry.push(scope);
        }
    }

    /// Enumerate the cursor scopes covering a membership.
    #[must_use]
    pub fn scopes_for_membership(&self, membership: &MembershipScope) -> Vec<CursorScope> {
        let guard = self.membership_index.read().expect("poisoned");
        guard.get(membership).cloned().unwrap_or_default()
    }

    /// Enumerate every known cursor scope. Used by the reconciler on
    /// `HintPayload::Unknown`.
    #[must_use]
    pub fn all_scopes(&self) -> Vec<CursorScope> {
        let guard = self.cursors.read().expect("poisoned");
        guard.keys().cloned().collect()
    }
}
