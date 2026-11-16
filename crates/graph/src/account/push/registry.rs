//! Who owns a Graph webhook subscription: the factory-shared row store, and
//! each opened account's lease view onto it.
//!
//! Graph refuses a second subscription with the same `resource` and
//! `changeType` (409, "Subscription Id <> already exists for the requested
//! combination"). Every subscription this crate creates uses the same
//! `changeType`, so one resource can have exactly one server row, and every
//! consumer of that resource must share it. Two consumers meet routinely: a
//! reopen subscribes on the replacement account while the old account's
//! handles are still live (the engine only tears them down afterwards, so the
//! old account stays installed and retryable if the swap aborts), and two
//! subscribe calls whose scopes share a resource (every calendar scope maps to
//! the mailbox-wide `/events`, every contact scope to `/contacts`) meet on the
//! same account. When rows belonged to handles, both cases 409ed.
//!
//! So the STORE owns rows, their generations and their recovery, and a handle
//! only holds LEASES on rows. The store is shared by every account one
//! `GraphAccountFactory` opens (one credential, one notification URL and
//! clientState); each opened account is an instance that owns its leases and
//! its push-event delivery.
//!
//! - A subscribe attaches to a live row without a POST, or creates the row.
//! - A release removes the lease and DELETEs the row only when no other live
//!   lease remains. A failed DELETE condemns the lease, which keeps it
//!   reachable for a retry and for `close()`.
//! - A lease is live only while it is not condemned AND its instance is still
//!   alive, so a dropped, never-closed account cannot pin a row forever.
//! - Every row-lifecycle network effect (create, attach recovery, renewal,
//!   recreate, delete) runs under one async exchange lock, held through its
//!   local commit, inside a task the caller's cancellation cannot reach.
//! - Generations come from one counter that never resets, so a queued event
//!   naming `(resource, generation)` can never match a later incarnation.
//!
//! Not covered: two independently built factories for the same credentials
//! do not share a store, and a row left by a previous process (it lives up to
//! the expiry we request) is unknown to this one. A create that meets either
//! still answers 409 and fails as before.
use bifrost_types::{AccountError, CursorScope, SubscriptionHandle};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use tokio::sync::{OwnedMutexGuard, watch};

/// One opened account's identity in the store. Clones and metered views of
/// an account share it.
pub(crate) type InstanceId = u64;

/// One handle's claim on a row.
#[derive(Debug, Clone)]
pub(crate) struct Lease {
    pub(crate) instance: InstanceId,
    /// The scopes this handle asked this row to cover.
    pub(crate) scopes: Vec<CursorScope>,
    /// Its release failed to DELETE the row. The lease stays so a retry or
    /// `close()` can finish the job, but it no longer wants coverage: it is
    /// never renewed for, never attributed, and never keeps a row alive.
    pub(crate) condemned: bool,
}

/// One Graph server subscription.
#[derive(Debug, Clone)]
pub(crate) struct Row {
    pub(crate) server_id: String,
    /// Bumped by every change of the row's identity or health (a fresh
    /// server id, a terminal verdict, a recovery), from the store-wide
    /// counter.
    pub(crate) generation: u64,
    // Retain malformed server expiry alongside server_id, rather than fail
    // creation before registration and orphan a subscription with a known id.
    // Outlook users expect push to recover: the renewal fallback keeps coverage
    // alive and this row reachable for close/unsubscribe.
    pub(crate) expires_at: String,
    /// The renewal session already warned about an unparseable `expires_at`
    /// for this row, so the warning fires once per row, not once per tick.
    pub(crate) warned_unparseable_expiry: bool,
    /// A renewal or recreate failed terminally (auth lost, permission
    /// revoked, the folder deleted), so nothing renews it. The row stays: a
    /// subscription we stopped renewing is not one that stopped existing,
    /// and release and `close()` still DELETE it (ruled 2026-10-04). A
    /// subscribe that attaches to it recovers it first.
    pub(crate) terminated: bool,
    pub(crate) leases: HashMap<SubscriptionHandle, Lease>,
}

/// What an instance's renewal session owes the engine.
#[derive(Debug, Clone)]
pub(crate) enum InstanceEvent {
    /// The row covering this scope was recreated or recovered, and Graph
    /// does not replay what happened while it was missing.
    Gap(CursorScope),
    /// The row failed terminally at this generation. Attributed at delivery
    /// to the instance's live scopes on the row, and dropped if the row has
    /// since moved on.
    Terminal {
        resource: String,
        generation: u64,
        error: AccountError,
    },
}

/// Per-instance delivery state, shared by every clone of one opened account.
pub(crate) struct InstanceSink {
    id: InstanceId,
    events: Mutex<VecDeque<InstanceEvent>>,
    topology: watch::Sender<u64>,
}

impl InstanceSink {
    fn push(&self, event: InstanceEvent) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(event);
    }

    fn poke(&self) {
        self.topology.send_modify(|n| *n = n.wrapping_add(1));
    }
}

#[derive(Default)]
struct StoreState {
    next_generation: u64,
    rows: BTreeMap<String, Row>,
    instances: HashMap<InstanceId, Weak<InstanceSink>>,
}

impl StoreState {
    fn generation(&mut self) -> u64 {
        self.next_generation += 1;
        self.next_generation
    }

    fn instance_alive(&self, instance: InstanceId) -> bool {
        self.instances
            .get(&instance)
            .is_some_and(|sink| sink.strong_count() > 0)
    }

    fn lease_live(&self, lease: &Lease) -> bool {
        !lease.condemned && self.instance_alive(lease.instance)
    }

    fn row_has_live_lease(&self, row: &Row) -> bool {
        row.leases.values().any(|lease| self.lease_live(lease))
    }

    fn sink(&self, instance: InstanceId) -> Option<Arc<InstanceSink>> {
        self.instances.get(&instance).and_then(Weak::upgrade)
    }

    /// Every live instance holding a live lease on `row`, once each.
    fn live_instances(&self, row: &Row) -> Vec<Arc<InstanceSink>> {
        let mut seen = HashSet::new();
        row.leases
            .values()
            .filter(|lease| self.lease_live(lease) && seen.insert(lease.instance))
            .filter_map(|lease| self.sink(lease.instance))
            .collect()
    }
}

/// The factory-shared row store. See the module doc.
pub(crate) struct WebhookStore {
    exchange: Arc<tokio::sync::Mutex<()>>,
    state: Mutex<StoreState>,
    next_instance: AtomicU64,
    /// Test seam: every commit that follows a network effect passes through
    /// this, so a test can park a task between "the server answered" and
    /// "the store recorded it".
    #[cfg(test)]
    pub(crate) commit_gate: tokio::sync::RwLock<()>,
}

impl Default for WebhookStore {
    fn default() -> Self {
        Self {
            exchange: Arc::default(),
            state: Mutex::default(),
            next_instance: AtomicU64::new(1),
            #[cfg(test)]
            commit_gate: tokio::sync::RwLock::new(()),
        }
    }
}

/// Proof that the holder owns the store's exchange. Internal operations take
/// it by reference, so only the owned-task entry points ever acquire it and
/// nothing re-enters it.
pub(crate) struct Exchange(#[allow(dead_code)] OwnedMutexGuard<()>);

/// The state of one row as a lifecycle step sees it under the exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowStatus {
    Absent,
    /// Has a live lease and is healthy: attach without a request.
    Live,
    /// Has a live lease but failed terminally: recover before attaching.
    Terminated {
        server_id: String,
    },
    /// No live lease at all (condemned leases only, or rollback debt): the
    /// server row may or may not still exist, so DELETE it before creating.
    Unowned {
        server_id: String,
    },
}

/// One row due for renewal, as snapshotted by a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DueRow {
    pub(crate) resource: String,
    pub(crate) server_id: String,
    pub(crate) generation: u64,
}

impl WebhookStore {
    fn state(&self) -> MutexGuard<'_, StoreState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) async fn exchange(&self) -> Exchange {
        Exchange(Arc::clone(&self.exchange).lock_owned().await)
    }

    #[cfg(test)]
    pub(crate) async fn pass_commit_gate(&self) {
        drop(self.commit_gate.read().await);
    }

    #[cfg(not(test))]
    #[allow(clippy::unused_async)]
    pub(crate) async fn pass_commit_gate(&self) {}

    fn new_instance(&self) -> Arc<InstanceSink> {
        let id = self.next_instance.fetch_add(1, Ordering::Relaxed);
        let sink = Arc::new(InstanceSink {
            id,
            events: Mutex::default(),
            topology: watch::Sender::new(0),
        });
        let mut state = self.state();
        state.instances.retain(|_, sink| sink.strong_count() > 0);
        state.instances.insert(id, Arc::downgrade(&sink));
        sink
    }

    pub(crate) fn status(&self, _exchange: &Exchange, resource: &str) -> RowStatus {
        let state = self.state();
        match state.rows.get(resource) {
            None => RowStatus::Absent,
            Some(row) if !state.row_has_live_lease(row) => RowStatus::Unowned {
                server_id: row.server_id.clone(),
            },
            Some(row) if row.terminated => RowStatus::Terminated {
                server_id: row.server_id.clone(),
            },
            Some(_) => RowStatus::Live,
        }
    }

    /// Record a freshly created row with no lease yet: provisional to the
    /// transaction that created it, and cleanup debt if it never commits.
    pub(crate) fn insert_row(
        &self,
        _exchange: &Exchange,
        resource: String,
        server_id: String,
        expires_at: String,
    ) {
        let mut state = self.state();
        let generation = state.generation();
        state.rows.insert(
            resource,
            Row {
                server_id,
                generation,
                expires_at,
                warned_unparseable_expiry: false,
                terminated: false,
                leases: HashMap::new(),
            },
        );
    }

    /// Forget a row whose server subscription is gone, with every lease on
    /// it: a condemned lease's debt is paid by the same DELETE.
    pub(crate) fn remove_row(&self, _exchange: &Exchange, resource: &str) {
        self.state().rows.remove(resource);
    }

    /// The row came back (a PATCH succeeded on a terminated row, or a fresh
    /// create replaced a vanished one). Every live holder is owed a gap: Graph
    /// does not replay what happened while the row was missing or unrenewed.
    /// The new server id is recorded whoever holds the row, so it stays
    /// reachable by release. Returns false, changing nothing, when the row is
    /// gone.
    pub(crate) fn recover(
        &self,
        _exchange: &Exchange,
        resource: &str,
        server_id: Option<String>,
        expires_at: String,
    ) -> bool {
        let mut state = self.state();
        if !state.rows.contains_key(resource) {
            return false;
        }
        let generation = state.generation();
        let mut owed = Vec::new();
        {
            let row = state.rows.get(resource).expect("checked above");
            for lease in row.leases.values() {
                if state.lease_live(lease)
                    && let Some(sink) = state.sink(lease.instance)
                {
                    owed.push((sink, lease.scopes.clone()));
                }
            }
        }
        let row = state.rows.get_mut(resource).expect("checked above");
        if let Some(server_id) = server_id {
            row.server_id = server_id;
        }
        row.generation = generation;
        row.expires_at = expires_at;
        row.warned_unparseable_expiry = false;
        row.terminated = false;
        drop(state);
        for (sink, scopes) in owed {
            for scope in scopes {
                sink.push(InstanceEvent::Gap(scope));
            }
            sink.poke();
        }
        true
    }

    /// Commit one subscribe's leases in one step. The caller checked its
    /// instance's shutdown token immediately before, with no await between.
    /// A row recovered for this subscribe owes the new lease a gap too.
    pub(crate) fn commit_leases(
        &self,
        _exchange: &Exchange,
        sink: &InstanceSink,
        handle: &SubscriptionHandle,
        leases: Vec<(String, Vec<CursorScope>, bool)>,
    ) {
        let mut state = self.state();
        for (resource, scopes, recovered) in leases {
            let Some(row) = state.rows.get_mut(&resource) else {
                continue;
            };
            if recovered {
                for scope in &scopes {
                    sink.push(InstanceEvent::Gap(scope.clone()));
                }
            }
            row.leases.insert(
                handle.clone(),
                Lease {
                    instance: sink.id,
                    scopes,
                    condemned: false,
                },
            );
        }
        drop(state);
        sink.poke();
    }

    pub(crate) fn server_id(&self, _exchange: &Exchange, resource: &str) -> Option<String> {
        self.state()
            .rows
            .get(resource)
            .map(|row| row.server_id.clone())
    }

    /// Every resource `handle` holds a lease on, condemned or not.
    pub(crate) fn leased_by(&self, handle: &SubscriptionHandle) -> Vec<String> {
        self.state()
            .rows
            .iter()
            .filter(|(_, row)| row.leases.contains_key(handle))
            .map(|(resource, _)| resource.clone())
            .collect()
    }

    /// Every handle this instance holds a lease through, condemned or not.
    pub(crate) fn handles_of(&self, instance: InstanceId) -> Vec<SubscriptionHandle> {
        let state = self.state();
        let handles: HashSet<SubscriptionHandle> = state
            .rows
            .values()
            .flat_map(|row| row.leases.iter())
            .filter(|(_, lease)| lease.instance == instance)
            .map(|(handle, _)| handle.clone())
            .collect();
        let mut handles: Vec<_> = handles.into_iter().collect();
        handles.sort_by(|a, b| a.0.cmp(&b.0));
        handles
    }

    /// Whether some live lease other than `handle`'s holds the row.
    pub(crate) fn shared_with_others(
        &self,
        _exchange: &Exchange,
        resource: &str,
        handle: &SubscriptionHandle,
    ) -> bool {
        let state = self.state();
        state.rows.get(resource).is_some_and(|row| {
            row.leases
                .iter()
                .any(|(other, lease)| other != handle && state.lease_live(lease))
        })
    }

    pub(crate) fn remove_lease(
        &self,
        _exchange: &Exchange,
        resource: &str,
        handle: &SubscriptionHandle,
    ) {
        if let Some(row) = self.state().rows.get_mut(resource) {
            row.leases.remove(handle);
        }
    }

    pub(crate) fn condemn(
        &self,
        _exchange: &Exchange,
        resource: &str,
        handle: &SubscriptionHandle,
    ) {
        if let Some(lease) = self
            .state()
            .rows
            .get_mut(resource)
            .and_then(|row| row.leases.get_mut(handle))
        {
            lease.condemned = true;
        }
    }

    /// Whether any row in the store currently names `server_id`.
    pub(crate) fn holds_id(&self, _exchange: &Exchange, server_id: &str) -> bool {
        self.state()
            .rows
            .values()
            .any(|row| row.server_id == server_id)
    }

    /// The rows this instance should renew: healthy, held live by one of its
    /// leases. Expiry is judged by `due`, so the threshold logic stays with
    /// the renewal session.
    pub(crate) fn renewable(
        &self,
        instance: InstanceId,
        mut due: impl FnMut(&mut Row) -> bool,
    ) -> Vec<DueRow> {
        let mut state = self.state();
        let wanted: Vec<String> = state
            .rows
            .iter()
            .filter(|(_, row)| {
                !row.terminated
                    && row
                        .leases
                        .values()
                        .any(|lease| lease.instance == instance && state.lease_live(lease))
            })
            .map(|(resource, _)| resource.clone())
            .collect();
        let mut out = Vec::new();
        for resource in wanted {
            let row = state.rows.get_mut(&resource).expect("collected above");
            if due(row) {
                out.push(DueRow {
                    resource,
                    server_id: row.server_id.clone(),
                    generation: row.generation,
                });
            }
        }
        out
    }

    /// Re-read a snapshotted row under the exchange: same identity, still
    /// wanted by this instance, still healthy, and (by `due`) still due.
    pub(crate) fn still_due(
        &self,
        _exchange: &Exchange,
        instance: InstanceId,
        snapshot: &DueRow,
        due: impl FnOnce(&mut Row) -> bool,
    ) -> bool {
        let mut state = self.state();
        let wanted = state.rows.get(&snapshot.resource).is_some_and(|row| {
            row.server_id == snapshot.server_id
                && row.generation == snapshot.generation
                && !row.terminated
                && row
                    .leases
                    .values()
                    .any(|lease| lease.instance == instance && state.lease_live(lease))
        });
        wanted && state.rows.get_mut(&snapshot.resource).is_some_and(due)
    }

    /// Whether some live lease, of any instance, still holds the row.
    pub(crate) fn wanted(&self, _exchange: &Exchange, resource: &str) -> bool {
        let state = self.state();
        state
            .rows
            .get(resource)
            .is_some_and(|row| state.row_has_live_lease(row))
    }

    /// Whether the row still has the identity a recreate started from.
    pub(crate) fn unchanged(&self, _exchange: &Exchange, snapshot: &DueRow) -> bool {
        self.state()
            .rows
            .get(&snapshot.resource)
            .is_some_and(|row| {
                row.server_id == snapshot.server_id && row.generation == snapshot.generation
            })
    }

    pub(crate) fn renewed(&self, _exchange: &Exchange, snapshot: &DueRow, expires_at: String) {
        if let Some(row) = self.state().rows.get_mut(&snapshot.resource)
            && row.generation == snapshot.generation
        {
            row.expires_at = expires_at;
        }
    }

    /// Record a terminal verdict and owe it to every instance holding the
    /// row live, each of which attributes it to its own scopes at delivery.
    pub(crate) fn terminate(&self, _exchange: &Exchange, snapshot: &DueRow, error: &AccountError) {
        let mut state = self.state();
        let Some(row) = state.rows.get(&snapshot.resource) else {
            return;
        };
        if row.generation != snapshot.generation {
            return;
        }
        let sinks = state.live_instances(row);
        let generation = state.generation();
        let row = state
            .rows
            .get_mut(&snapshot.resource)
            .expect("checked above");
        row.terminated = true;
        row.generation = generation;
        drop(state);
        for sink in sinks {
            sink.push(InstanceEvent::Terminal {
                resource: snapshot.resource.clone(),
                generation,
                error: error.clone(),
            });
            sink.poke();
        }
    }

    /// This instance's live scopes on `resource` if the row is still at
    /// `generation`; `None` when the verdict is obsolete.
    pub(crate) fn terminal_scopes(
        &self,
        instance: InstanceId,
        resource: &str,
        generation: u64,
    ) -> Option<Vec<CursorScope>> {
        // One lock for the check and the read: a recovery between them would
        // attribute an obsolete verdict to the recovered row.
        let state = self.state();
        let row = state.rows.get(resource)?;
        if row.generation != generation {
            return None;
        }
        let scopes = Self::scopes_in(&state, row, instance);
        (!scopes.is_empty()).then_some(scopes)
    }

    /// This instance's live scopes on `resource`.
    pub(crate) fn scopes_of(&self, instance: InstanceId, resource: &str) -> Vec<CursorScope> {
        let state = self.state();
        state
            .rows
            .get(resource)
            .map_or_else(Vec::new, |row| Self::scopes_in(&state, row, instance))
    }

    /// The distinct union: two leases covering one scope still name one
    /// scope, so a failure stays attributed to it.
    fn scopes_in(state: &StoreState, row: &Row, instance: InstanceId) -> Vec<CursorScope> {
        let mut scopes: Vec<CursorScope> = Vec::new();
        for scope in row
            .leases
            .values()
            .filter(|lease| lease.instance == instance && state.lease_live(lease))
            .flat_map(|lease| lease.scopes.iter())
        {
            if !scopes.contains(scope) {
                scopes.push(scope.clone());
            }
        }
        scopes
    }

    /// Rows no live lease holds: rollback debt, condemned-only rows, rows
    /// whose holders were dropped unclosed. Nobody will release them, so
    /// `close()` retries their DELETE. Rows `closing` has a lease on are left
    /// out: its own release attempts those.
    pub(crate) fn unowned(&self, _exchange: &Exchange, closing: InstanceId) -> Vec<String> {
        let state = self.state();
        state
            .rows
            .iter()
            .filter(|(_, row)| {
                !state.row_has_live_lease(row)
                    && !row.leases.values().any(|lease| lease.instance == closing)
            })
            .map(|(resource, _)| resource.clone())
            .collect()
    }

    fn any_row(&self, instance: InstanceId, terminated: bool) -> bool {
        let state = self.state();
        state.rows.values().any(|row| {
            row.terminated == terminated
                && row
                    .leases
                    .values()
                    .any(|lease| lease.instance == instance && state.lease_live(lease))
        })
    }

    #[cfg(test)]
    pub(crate) fn rows_for_tests(&self) -> BTreeMap<String, Row> {
        self.state().rows.clone()
    }

    #[cfg(test)]
    pub(crate) fn edit_for_tests(&self, edit: impl FnOnce(&mut BTreeMap<String, Row>)) {
        edit(&mut self.state().rows);
    }
}

/// One opened account's view: the shared store plus its own sink.
#[derive(Clone)]
pub(crate) struct WebhookRegistry {
    store: Arc<WebhookStore>,
    sink: Arc<InstanceSink>,
}

impl Default for WebhookRegistry {
    fn default() -> Self {
        Self::attached(Arc::default())
    }
}

impl WebhookRegistry {
    /// A new instance of `store`. Every account a factory opens is one.
    pub(crate) fn attached(store: Arc<WebhookStore>) -> Self {
        let sink = store.new_instance();
        Self { store, sink }
    }

    pub(crate) fn store(&self) -> &Arc<WebhookStore> {
        &self.store
    }

    pub(crate) fn instance(&self) -> InstanceId {
        self.sink.id
    }

    pub(crate) fn sink(&self) -> &InstanceSink {
        &self.sink
    }

    pub(super) fn topology(&self) -> watch::Receiver<u64> {
        self.sink.topology.subscribe()
    }

    pub(super) fn changed(&self) {
        self.sink.poke();
    }

    pub(super) fn pop_event(&self) -> Option<InstanceEvent> {
        self.sink
            .events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }

    /// Whether this instance holds a healthy row it must renew.
    pub(super) fn has_live(&self) -> bool {
        self.store.any_row(self.instance(), false)
    }

    /// Whether this instance holds a row that failed terminally.
    pub(super) fn has_terminated(&self) -> bool {
        self.store.any_row(self.instance(), true)
    }

    pub(super) fn handles(&self) -> Vec<SubscriptionHandle> {
        self.store.handles_of(self.instance())
    }

    #[cfg(test)]
    pub(crate) fn events_for_tests(&self) -> usize {
        self.sink
            .events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Give a test row a live lease of this instance.
    #[cfg(test)]
    pub(crate) fn lease_for_tests(
        &self,
        handle: &SubscriptionHandle,
        resource: &str,
        server_id: &str,
        expires_at: &str,
        scopes: Vec<CursorScope>,
    ) {
        let instance = self.instance();
        self.store.edit_for_tests(|rows| {
            let row = rows.entry(resource.to_string()).or_insert_with(|| Row {
                server_id: server_id.to_string(),
                generation: 0,
                expires_at: expires_at.to_string(),
                warned_unparseable_expiry: false,
                terminated: false,
                leases: HashMap::new(),
            });
            row.leases.insert(
                handle.clone(),
                Lease {
                    instance,
                    scopes,
                    condemned: false,
                },
            );
        });
        self.changed();
    }
}
