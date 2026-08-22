//! Backfill partition runner.
//!
//! Walks `inventory_partition_stream(scope)` and forwards each `Batch`
//! as a `MultiplexerEvent` carrying `Change::ObjectChange::Created`
//! items plus a `BackfillCheckpoint` checkpoint. The runner filters
//! each page through `LiveSupersedes`, which is intentionally never
//! populated - see that type's docs for why "we broadcast it" is not
//! evidence the consumer received it, and what a sound producer would
//! have to prove first.
//!
//! ## Checkpoint-durability contract
//!
//! Durable persistence is consumer-ack-deferred, mirroring
//! `drive_changes_stream`. The runner broadcasts each page and its
//! `BackfillCheckpoint` but does NOT write to `CheckpointStore`. The
//! consumer atomically persists the page items in their own store, then
//! calls `SyncEngine::ack_checkpoint` with the `Checkpoint::Backfill`;
//! the per-account ack writer routes that to
//! `CheckpointStore::put_backfill` and fires `record_checkpoint` to wake
//! pause / checkpoint waiters on a durable boundary.
//!
//! This closes the at-least-once gap an eager engine-side write would
//! open: a crash between broadcast and the consumer's write re-walks the
//! partition on restart (the orchestrator always restarts a scope from
//! its first partition), so no inventory page is lost. An eager write
//! would let the store record progress the consumer never durably
//! persisted, permanently losing that page's objects on cold start.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use bifrost_types::{
    Account, BackfillCheckpoint, BackfillProgress, Batch, Change, Checkpoint, CursorScope,
    InventoryEntry, InventoryPartition, ObjectChange, ObjectChangeKind, ObjectId, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::broadcast;

use crate::control::SyncControl;
use crate::error::Error;
use crate::multiplexer::MultiplexerEvent;

use super::partitioner::partition_key;

/// Default cap on `LiveSupersedes`. Beyond this many entries, the
/// oldest insertions are evicted via a FIFO ring so the set does not
/// grow unboundedly in long-running sessions.
pub const LIVE_SUPERSEDES_DEFAULT_CAP: usize = 100_000;

/// Side channel intended to let live `Created` changes pre-empt
/// inventory entries, so cold-start hydration does not double-emit
/// objects the live stream has already shown.
///
/// The set is capped (default `LIVE_SUPERSEDES_DEFAULT_CAP`); on
/// overflow the oldest insertion is dropped.
///
/// # Nothing populates this set, deliberately
///
/// `BackfillRunner::run_partition` filters against it, but no producer
/// calls `add`, so it filters nothing. That is a considered decision,
/// not an oversight, and wiring the multiplexer or push reconciler into
/// it re-introduces silent data loss. Two independent reasons:
///
/// 1. **Broadcasting is not receiving.** The per-account channel is a
///    `tokio::broadcast`. The slot holds a sentinel receiver, so `send`
///    reports success even when no consumer is attached; a subscriber
///    that arrives later starts at the ring's tail and never sees
///    earlier values; and a subscriber that lags has values overwritten
///    out from under it, which this crate does not detect or replay.
///    Recording an id on `send` therefore records ids the consumer will
///    never receive - and suppressing the inventory copy of one of
///    those is permanent loss of that object for the session, because
///    the in-memory cursor has already advanced past it.
///
/// 2. **Selection and publication are separate operations.** Even with
///    a correct record trigger, backfill decides to keep an entry and
///    broadcasts it much later. A live change landing in that gap is
///    recorded too late to suppress anything, so the runner still
///    emits a `Created` for an object the live stream just destroyed.
///    Any working design has to make "decide + publish" indivisible for
///    a given id on both sides.
///
/// The only sound record trigger this engine has is the consumer ack:
/// `SyncEngine::ack_checkpoint` is what the crate already treats as
/// proof of receipt, precisely because broadcast delivery is not. A
/// correct implementation buffers ids as pending, keyed by the
/// checkpoint of the batch carrying them, and promotes them to
/// suppressing only once that checkpoint is acked - with a policy for
/// batches whose `checkpoint` is `None`, since those can never be
/// promoted, and a bound on the pending buffer.
///
/// That is worth building only if the duplicate is expensive, and it is
/// not: consumers must already tolerate a repeated `Created`, because a
/// crash mid-plan re-walks every partition and re-emits acked pages
/// (see the module docs). An unpopulated set costs duplicates the
/// consumer already absorbs. A wrongly populated one drops mail.
#[derive(Debug)]
pub struct LiveSupersedes {
    inner: Mutex<LiveSupersedesInner>,
    cap: usize,
}

#[derive(Debug, Default)]
struct LiveSupersedesInner {
    /// Insertion order for ring eviction. One slot per `add` that did
    /// not collide; on overflow the front slot is popped and its id
    /// removed from `present`.
    ///
    /// `order` is a superset of `present`: `take` removes from
    /// `present` only and leaves a tombstone slot here, so the ring can
    /// hold ids the set no longer suppresses. Eviction reclaims those
    /// slots for free as they reach the front - popping a tombstone
    /// removes nothing from `present`, which lets `present` grow back
    /// toward `cap`. The two therefore reconverge without any scan.
    order: VecDeque<ObjectId>,
    /// The authoritative membership. `take` and `add` both decide
    /// purely from this; `order` only decides eviction ORDER, never
    /// whether an id is suppressed.
    present: std::collections::HashSet<ObjectId>,
}

impl Default for LiveSupersedes {
    fn default() -> Self {
        Self::new()
    }
}

impl LiveSupersedes {
    /// Construct with the default cap.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(LIVE_SUPERSEDES_DEFAULT_CAP)
    }

    /// Construct with a specific cap. Useful for tests that want to
    /// observe ring-eviction behavior cheaply.
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: Mutex::new(LiveSupersedesInner::default()),
            cap: cap.max(1),
        }
    }

    /// Mark an id as superseded by the live stream. Subsequent
    /// backfill passes will skip it. On overflow the oldest entry is
    /// dropped from the ring.
    ///
    /// No production path calls this, and adding one is not a matter of
    /// finding a convenient call site - see the type docs for what a
    /// sound record trigger has to prove.
    pub fn add(&self, id: ObjectId) {
        let mut g = self.inner.lock().expect("poisoned");
        if g.present.contains(&id) {
            return;
        }
        if g.order.len() >= self.cap
            && let Some(old) = g.order.pop_front()
        {
            g.present.remove(&old);
        }
        g.present.insert(id.clone());
        g.order.push_back(id);
    }

    /// True if the id has been superseded. Removes the entry on hit
    /// so the set drains as backfill walks past entries it can skip.
    ///
    /// O(1). Only `present` is touched; the id's slot in `order` is
    /// left behind as a tombstone rather than scanned out. Cold-start
    /// backfill calls this once per inventory entry, so scanning a
    /// ring that holds up to `LIVE_SUPERSEDES_DEFAULT_CAP` entries
    /// would make hydrating a large mailbox quadratic.
    ///
    /// Tombstones cannot cause a WRONG suppression - the outcome that
    /// would be worse than the duplicate this set exists to prevent.
    /// Suppression requires membership in `present`, and only `add`
    /// ever inserts there; a slot lingering in `order` grants nothing.
    /// The single behavioral consequence is in the other, safe
    /// direction: if an id is taken and then re-announced by the live
    /// stream, `order` holds two slots for it, and evicting the older
    /// one drops the live entry earlier than its ring position implies.
    /// That costs at most one suppression, i.e. one duplicate emit -
    /// which the cap already permits, since this set is an
    /// optimization and never a guarantee.
    #[must_use]
    pub fn take(&self, id: &ObjectId) -> bool {
        let mut g = self.inner.lock().expect("poisoned");
        g.present.remove(id)
    }

    /// Snapshot for tests.
    #[must_use]
    pub fn len(&self) -> usize {
        let g = self.inner.lock().expect("poisoned");
        g.present.len()
    }

    /// Convenience: true iff `len() == 0`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Slots currently held in the eviction ring, live plus tombstoned.
    /// Test-only: the ring is an implementation detail, but its size is
    /// the observable footprint of `take` not scanning it.
    #[cfg(test)]
    fn ring_len(&self) -> usize {
        let g = self.inner.lock().expect("poisoned");
        g.order.len()
    }
}

/// One partition's runner.
pub struct BackfillRunner;

/// Outcome for one partition pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillPartitionOutcome {
    /// Inventory entries observed before the live-supersedes filter.
    pub seen: u64,
    /// Inventory entries forwarded after the live-supersedes filter.
    pub kept: u64,
}

impl BackfillRunner {
    /// Walk one inventory pass to completion. Forwards each `Batch`
    /// onto the per-account `changes_tx` broadcast as
    /// `Change::ObjectChange::Created` (skipping ids in
    /// `LiveSupersedes`), each page carrying a `BackfillCheckpoint`.
    /// Does NOT write to `CheckpointStore`: durable advance is
    /// consumer-ack-driven (see module docs). Checkpoint progress counts
    /// entries observed before filtering because it describes the
    /// inventory position, while the returned outcome reports both the
    /// observed and forwarded totals.
    pub async fn run_partition(
        account: &dyn Account,
        scope: CursorScope,
        partition: InventoryPartition,
        live: &LiveSupersedes,
        changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
        envelope_version: u32,
        control: Option<&SyncControl>,
    ) -> Result<BackfillPartitionOutcome, Error> {
        let _activity = match control {
            Some(control) => Some(control.begin_activity().ok_or(Error::Paused)?),
            None => None,
        };
        let partition_key = partition_key(&partition);
        let mut stream = account.inventory_partition_stream(scope.clone(), partition);
        let mut seen_total: u64 = 0;
        let mut kept_total: u64 = 0;
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Batch(batch) => {
                    let seen = u64::try_from(batch.items.len()).unwrap_or(u64::MAX);
                    seen_total = seen_total.saturating_add(seen);
                    let kept = filter_supersedes(&batch.items, live);
                    let kept_count = u64::try_from(kept.len()).unwrap_or(u64::MAX);
                    kept_total = kept_total.saturating_add(kept_count);

                    // Forward the page as a synthetic Batch carrying a
                    // BackfillCheckpoint. Durable persistence awaits the
                    // consumer ack (see module docs); the runner never
                    // writes to CheckpointStore.
                    if let Some(tx) = &changes_tx {
                        let bf = BackfillCheckpoint {
                            scope: scope.clone(),
                            partition: partition_key.clone(),
                            progress_marker: None,
                            progress: BackfillProgress {
                                items_done: seen_total,
                                items_estimated: None,
                            },
                            envelope_version,
                        };
                        let changes: Vec<Change> = kept
                            .into_iter()
                            .map(|entry| {
                                Change::ObjectChange(ObjectChange {
                                    id: entry.id.clone(),
                                    kind: ObjectChangeKind::Created,
                                })
                            })
                            .collect();
                        let synthetic = Batch {
                            items: changes,
                            page_boundary: batch.page_boundary,
                            server_latency: batch.server_latency,
                            bytes_in: batch.bytes_in,
                            checkpoint: Some(Checkpoint::Backfill(bf.clone())),
                        };
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Batch(synthetic)),
                            checkpoint: Some(Checkpoint::Backfill(bf.clone())),
                        };
                        // Register before publishing so a fast consumer
                        // ack cannot land before the entry exists and
                        // leave it outstanding forever.
                        let expected = Checkpoint::Backfill(bf);
                        if let Some(control) = control {
                            control.expect_checkpoint(expected.clone());
                        }
                        let delivered = tx.send(me).unwrap_or(0);
                        if !crate::multiplexer::delivered_to_real_subscriber(delivered)
                            && let Some(control) = control
                        {
                            control.retire_checkpoint(&expected);
                        }
                    }
                }
                SyncEvent::Done(_) => break,
                SyncEvent::Terminated(err) => {
                    if let Some(tx) = &changes_tx {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Terminated(err.clone())),
                            checkpoint: None,
                        };
                        let _ = tx.send(me);
                    }
                    return Err(Error::Account(err));
                }
                SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
                _ => {}
            }
        }
        Ok(BackfillPartitionOutcome {
            seen: seen_total,
            kept: kept_total,
        })
    }
}

fn filter_supersedes<'a>(
    items: &'a [InventoryEntry],
    live: &LiveSupersedes,
) -> Vec<&'a InventoryEntry> {
    items.iter().filter(|entry| !live.take(&entry.id)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{Fingerprint, InventoryEntry, ObjectId, ServerVersion};

    fn entry(id: &str) -> InventoryEntry {
        InventoryEntry {
            id: ObjectId(id.into()),
            memberships: Vec::new(),
            size: None,
            blob_id: None,
            fingerprint: Fingerprint {
                server_version: ServerVersion::Unavailable,
                size: None,
                flags_hash: 0,
            },
            thread_id: None,
            message_id: None,
            references: Vec::new(),
            in_reply_to: None,
        }
    }

    #[test]
    fn an_unpopulated_set_forwards_every_inventory_entry() {
        // The production state: nothing calls `add`, so backfill keeps
        // every entry. Pinned deliberately - a change that starts
        // populating the set flips this test, which is the moment to
        // re-read the `LiveSupersedes` docs on why "we broadcast it" is
        // not evidence the consumer received it.
        let live = LiveSupersedes::new();
        let page = vec![entry("a"), entry("b"), entry("c")];
        assert_eq!(filter_supersedes(&page, &live).len(), 3);
        assert!(live.is_empty());
    }

    #[test]
    fn supersedes_records_and_consumes() {
        let live = LiveSupersedes::new();
        let id = ObjectId("abc".into());
        live.add(id.clone());
        assert_eq!(live.len(), 1);
        assert!(live.take(&id));
        assert!(live.is_empty());
        assert!(!live.take(&id));
    }

    #[test]
    fn take_leaves_the_ring_slot_alone() {
        // `take` is O(1) because it never touches `order`. The
        // observable footprint of that is the ring keeping its slot
        // count across a hit; a scan-out would drop it to 1.
        let live = LiveSupersedes::with_capacity(8);
        live.add(ObjectId("a".into()));
        live.add(ObjectId("b".into()));
        assert_eq!(live.ring_len(), 2);
        assert!(live.take(&ObjectId("a".into())));
        assert_eq!(live.ring_len(), 2, "the tombstone stays in the ring");
        assert_eq!(live.len(), 1, "but it no longer suppresses anything");
    }

    #[test]
    fn a_tombstone_never_causes_a_suppression() {
        // The one outcome worse than a duplicate emit would be
        // suppressing an object that should have been forwarded.
        // Suppression reads `present` only, so a slot lingering in
        // `order` can never grant one - not before eviction, not after,
        // and not while other ids churn past it.
        let live = LiveSupersedes::with_capacity(4);
        let ghost = ObjectId("ghost".into());
        live.add(ghost.clone());
        assert!(live.take(&ghost));
        for id in ["p", "q", "r", "s", "t", "u"] {
            live.add(ObjectId(id.into()));
            assert!(
                !live.take(&ghost),
                "a tombstoned id must stay unsuppressed however the ring churns"
            );
        }
    }

    #[test]
    fn tombstones_do_not_break_the_capacity_bound() {
        // Ring slots are still bounded by `cap` even though `take`
        // leaves them behind, so a take-heavy cold start cannot grow
        // the set without limit.
        let live = LiveSupersedes::with_capacity(4);
        for n in 0..64 {
            live.add(ObjectId(format!("id-{n}")));
            if n % 3 == 0 {
                let _ = live.take(&ObjectId(format!("id-{n}")));
            }
            assert!(live.ring_len() <= 4);
            assert!(live.len() <= live.ring_len());
        }
    }

    #[test]
    fn evicting_a_tombstone_lets_membership_grow_back_to_capacity() {
        // Popping a tombstone removes nothing from `present`, so the
        // live set reconverges on `cap` instead of being permanently
        // eroded by earlier takes. This is why eviction needs no
        // skip-stale loop: the plain single-pop self-heals.
        let live = LiveSupersedes::with_capacity(3);
        live.add(ObjectId("a".into()));
        live.add(ObjectId("b".into()));
        live.add(ObjectId("c".into()));
        assert!(live.take(&ObjectId("a".into())));
        assert_eq!(live.len(), 2);
        assert_eq!(live.ring_len(), 3, "a's slot is a tombstone");

        // "a"'s tombstone is at the front, so this add reclaims it and
        // spares a live entry.
        live.add(ObjectId("d".into()));
        assert_eq!(live.len(), 3);
        assert!(live.take(&ObjectId("b".into())));
        assert!(live.take(&ObjectId("c".into())));
        assert!(live.take(&ObjectId("d".into())));
    }

    #[test]
    fn re_adding_a_taken_id_costs_at_most_a_duplicate() {
        // The single behavioral consequence of tombstoning: a taken id
        // that the live stream announces again owns two ring slots, so
        // evicting the older one drops the live entry early. That costs
        // a suppression (a duplicate emit), never a wrong suppression.
        let live = LiveSupersedes::with_capacity(2);
        let x = ObjectId("x".into());
        live.add(x.clone());
        assert!(live.take(&x));
        live.add(x.clone());
        assert!(live.len() <= 2);
        // Whatever eviction does to x's two slots, the invariant that
        // matters holds: x is suppressed at most once more.
        live.add(ObjectId("y".into()));
        live.add(ObjectId("z".into()));
        // Whichever way eviction resolved x's two slots, x is
        // suppressible at most once more - never twice off one `add`,
        // and never after it has been consumed.
        let _ = live.take(&x);
        assert!(!live.take(&x), "never suppressible twice from one add");
    }

    #[test]
    fn supersedes_caps_at_capacity() {
        let live = LiveSupersedes::with_capacity(3);
        live.add(ObjectId("a".into()));
        live.add(ObjectId("b".into()));
        live.add(ObjectId("c".into()));
        live.add(ObjectId("d".into()));
        assert_eq!(live.len(), 3);
        // "a" was evicted; "b","c","d" remain.
        assert!(!live.take(&ObjectId("a".into())));
        assert!(live.take(&ObjectId("b".into())));
        assert!(live.take(&ObjectId("c".into())));
        assert!(live.take(&ObjectId("d".into())));
    }
}
