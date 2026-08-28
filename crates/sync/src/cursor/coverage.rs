//! Coverage claims awaiting the acknowledgement that makes them durable.
//!
//! Coverage cannot travel with the checkpoint itself: `Checkpoint::Change`
//! carries a `ChangeCursor`, which is protocol-owned opaque bytes plus an
//! envelope tag, and it crosses the broadcast channel to a consumer and back
//! through `ack_checkpoint`. Widening it would push engine-internal concepts
//! through a published type every consumer matches on.
//!
//! So the producer records the claim here when it emits a checkpoint-bearing
//! batch, and the single durable writer reads it back when the matching
//! acknowledgement arrives. The record it then writes is still ONE atomic store
//! operation carrying both cursor and coverage - this map is in-memory engine
//! state on the path to that write, not a second durable lane.
//!
//! Losing it on a crash is consistent by construction: if the process dies
//! before the acknowledgement, the checkpoint never became durable either, so
//! no cursor advanced and there is nothing to remember.
//!
//! # Why publication identity, and not the checkpoint
//!
//! An earlier design keyed claims by `CursorScope`. That is unsound the moment
//! more than one thing per scope is in flight: two backfill partitions of one
//! scope each publish a checkpoint, the second overwrites the first's claim,
//! and acknowledging the FIRST then persists the second's coverage.
//!
//! Keying by `Checkpoint` is better and still not sufficient, because
//! `Checkpoint: Eq` is value equality, not a claim about publication identity:
//!
//! - `BackfillRunner` counts only entries a page MATERIALIZED, so a page whose
//!   content was entirely unrepresentable increments nothing; with no progress
//!   marker the next `BackfillCheckpoint` is byte-identical to its predecessor
//!   while describing a different boundary and a different coverage report.
//! - Inventory fusion publishes the same final checkpoint twice, once on the
//!   final batch and once on `Done`.
//! - A later walk can legitimately produce the same cursor bytes as an earlier
//!   one while proving different coverage.
//!
//! So the engine issues its own monotonic `PublicationId` per checkpoint-
//! bearing publication, and the acknowledgement names it.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{Checkpoint, CursorScope, InventoryCoverageReport};

/// Engine-issued identity for one checkpoint publication.
///
/// Monotonic identity plus the immutable receipt needed to acknowledge after
/// the issuing writer has restarted. Consumers persist the whole value beside
/// the batch, not only its numeric field.
#[derive(Debug, Clone)]
pub struct PublicationReceipt {
    /// The exact checkpoint lane this receipt may acknowledge. `None` is the
    /// repair lane, which intentionally has no checkpoint.
    pub checkpoint: Option<Checkpoint>,
    /// The coverage evidence that must land atomically with the checkpoint.
    pub claim: CoverageClaim,
}

/// Engine-issued acknowledgement token.
///
/// The first field is retained as the compact identity consumers may log and
/// key on. The receipt is the durable meaning of that identity and must be
/// retained with it across writer restarts.
#[derive(Debug, Clone)]
pub struct PublicationId(pub u64, pub std::sync::Arc<PublicationReceipt>);

impl PartialEq for PublicationId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for PublicationId {}
impl std::hash::Hash for PublicationId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
impl PartialOrd for PublicationId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PublicationId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

/// What a publication will make durable once acknowledged.
#[derive(Debug, Clone)]
pub struct CoverageClaim {
    /// The coverage reports this acknowledgement makes safe to persist.
    ///
    /// A `Vec`, not one report, because supersession FOLDS: when a newer
    /// publication supersedes an older outstanding one for acknowledgement
    /// purposes, the survivor has to absorb the superseded claim or the
    /// control path stops waiting for it while its obligations are quietly
    /// discarded. Within one inventory walk reports are cumulative and this is
    /// harmless duplication; across backfill partitions it is the only thing
    /// that keeps partition A's debt alive when B supersedes it.
    pub reports: Vec<InventoryCoverageReport>,
    /// Walk generation, ordering proof events even when cursor bytes repeat.
    pub generation: u64,
}

impl CoverageClaim {
    #[must_use]
    pub fn new(report: InventoryCoverageReport, generation: u64) -> Self {
        Self {
            reports: vec![report],
            generation,
        }
    }

    /// Absorb a superseded claim. Order is preserved so the older proof is
    /// ingested first.
    pub fn absorb(&mut self, mut other: Self) {
        other.reports.append(&mut self.reports);
        self.reports = other.reports;
        self.generation = self.generation.max(other.generation);
    }

    /// Keep only what this claim OWES, discarding what it proves clean.
    ///
    /// The carry-forward path for a batch the consumer never received. A
    /// degraded report describes obligations the engine must remember whether
    /// or not anyone saw the batch, so it survives. A `Complete` report is the
    /// opposite: it DISCHARGES debt on the strength of an enumeration the
    /// consumer took delivery of, and folding it into a later publication would
    /// discharge obligations against a batch that was destroyed in the ring.
    /// Under-reporting coverage costs a re-walk; over-reporting it loses
    /// objects nobody sees again.
    #[must_use]
    pub fn debt_only(mut self) -> Self {
        self.reports.retain(|report| !report.is_complete());
        self
    }

    #[must_use]
    fn is_empty(&self) -> bool {
        self.reports.is_empty()
    }
}

/// Which stream a publication belongs to.
///
/// Two jobs, and the second is why this is not just a supersession key.
///
/// It is the supersession key: broadcasts within one lane come from a single
/// sequential task, so the newest subsumes its predecessors.
///
/// It is also the ACKNOWLEDGEMENT lane. `ack_checkpoint` resolves an id in the
/// lane of the checkpoint it was handed, and `ack_publication` resolves one in
/// `Repair`. A repair acknowledgement naming a checkpoint publication therefore
/// finds nothing instead of consuming it - without that check it would consume
/// the claim, the later real acknowledgement would short-circuit as already
/// persisted, and the engine would announce a durable boundary the store never
/// wrote.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Lane {
    /// One scope's changes stream. Driven by a single sequential task, so the
    /// newest advance subsumes its predecessors.
    Change(CursorScope),
    /// One backfill PARTITION of one scope. The partition belongs in the key:
    /// two partitions of a scope are explicitly in flight together, neither
    /// subsumes the other, and folding them into one lane would make
    /// acknowledging partition A - a batch the consumer really received -
    /// resolve to an unknown publication once B published.
    Backfill(CursorScope, bifrost_types::Partition),
    /// A `Checkpoint` variant this revision does not recognise. Never
    /// supersedes: with no key there is no proof that one publication subsumes
    /// another.
    UnkeyedCheckpoint,
    /// A repair notification. Carries no checkpoint and is acknowledged through
    /// `SyncEngine::ack_publication`.
    Repair,
}

impl Lane {
    fn of(checkpoint: &Checkpoint) -> Self {
        match checkpoint {
            Checkpoint::Change(cursor) => Self::Change(cursor.scope.clone()),
            Checkpoint::Backfill(backfill) => {
                Self::Backfill(backfill.scope.clone(), backfill.partition.clone())
            }
            _ => Self::UnkeyedCheckpoint,
        }
    }

    fn supersedes(&self) -> bool {
        matches!(self, Self::Change(_) | Self::Backfill(..))
    }

    /// The cursor scope whose durable rows this lane writes, if any. Repair and
    /// unkeyed lanes write no scope-owned row and are therefore never fenced by
    /// a scope reset.
    fn scope(&self) -> Option<&CursorScope> {
        match self {
            Self::Change(scope) | Self::Backfill(scope, _) => Some(scope),
            Self::UnkeyedCheckpoint | Self::Repair => None,
        }
    }

    fn belongs_to(&self, scope: &CursorScope) -> bool {
        self.scope() == Some(scope)
    }
}

/// One live publication.
#[derive(Debug, Clone)]
struct Publication {
    lane: Lane,
    claim: CoverageClaim,
}

/// Everything the ledger knows, under ONE lock.
///
/// One mutex rather than four, because the interesting operations are
/// transitions across several of these fields at once. `register` looks up a
/// superseded boundary, removes it, folds its claim into the survivor and
/// inserts the survivor; performing that under separate locks leaves a window
/// in which a second backfill partition of the same scope inserts its own entry
/// for the same key, and the one-entry-per-key bound - which is what makes a
/// single acknowledgement discharge the whole lane - silently stops holding.
#[derive(Debug, Default)]
struct Ledger {
    /// Publications that have not yet been resolved.
    claims: HashMap<PublicationId, Publication>,
    /// Debt rescued from publications nobody can acknowledge, waiting for the
    /// next publication that someone can.
    carried: Option<CoverageClaim>,
    /// Highest PERSISTED publication per lane. This is the bounded replacement
    /// for a per-publication tombstone set: within a lane, publications are
    /// sequential and supersession folds older claims into newer ones, so a
    /// persisted id at or above `id` means everything `id` proved is durable.
    /// One entry per lane bounds this by the account's scope count, where a
    /// tombstone per acknowledgement grew for the life of the attachment.
    persisted: HashMap<Lane, PublicationId>,
    /// Highest publication per lane whose claim was FOLDED into a successor.
    ///
    /// A superseded publication's claim moves to the survivor so coarse acking
    /// ("persist N batches, ack the last") cannot strand it. But the consumer
    /// may still acknowledge the superseded batch - it really received it, and
    /// producers routinely run ahead of consumers - and that acknowledgement
    /// must persist its checkpoint while ingesting nothing, because its
    /// coverage now rides the survivor. One entry per lane, so this stays
    /// bounded where a per-publication record would not.
    folded: HashMap<Lane, PublicationId>,
    /// Publications whose checkpoint the control path is still waiting on, in
    /// publication order.
    boundaries: Vec<(PublicationId, Lane, Checkpoint)>,
    /// Per-scope acknowledgement fence installed by a durable scope reset.
    ///
    /// Holds the mint counter as of the moment the reset closed. Every
    /// publication id at or below it names a batch published against durable
    /// rows the reset has since deleted, so acknowledging one would re-create
    /// the very cursor the reset dropped to force re-establishment. Ids minted
    /// after the reset closed - i.e. by the re-establish that follows - are
    /// above the fence and unaffected, so no unfencing step exists to be
    /// forgotten. One entry per scope, so it stays bounded.
    fenced: HashMap<CursorScope, u64>,
}

/// Hard ceiling on outstanding boundary registrations. Reachable only through a
/// `Checkpoint` variant this revision does not key; keyed lanes are already
/// bounded by the account's scope count.
const PENDING_BOUNDARY_CAP: usize = 1024;

/// Per-account publication registry.
#[derive(Debug)]
pub struct PendingCoverage {
    ledger: Mutex<Ledger>,
    next: AtomicU64,
    next_generation: AtomicU64,
}

static NEXT_LEDGER_ID: AtomicU64 = AtomicU64::new(0);

impl Default for PendingCoverage {
    fn default() -> Self {
        Self {
            ledger: Mutex::new(Ledger::default()),
            next: AtomicU64::new(NEXT_LEDGER_ID.fetch_add(1, Ordering::Relaxed) << 32),
            next_generation: AtomicU64::new(0),
        }
    }
}

/// What the writer should do with an acknowledgement.
#[derive(Debug, Clone)]
pub enum ClaimLookup {
    /// First acknowledgement: ingest these reports, then persist.
    Apply(CoverageClaim),
    /// Already persisted. Report success, change nothing.
    AlreadyPersisted,
    /// No such publication. NEVER treat this as complete coverage - an unknown
    /// acknowledgement is a bug or a stale caller, and inventing a completeness
    /// claim for it is exactly the lying record this whole mechanism exists to
    /// prevent.
    Unknown,
}

impl PendingCoverage {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Issue a generation for one enumeration walk.
    ///
    /// Orders proof events. Two walks can produce identical cursor bytes while
    /// proving different coverage, so the ledger needs something other than the
    /// checkpoint to tell "newer" from "older" - but note that generation alone
    /// never PROVES coverage: a newer partial walk is still partial, which is
    /// why discharge requires a covering domain as well.
    pub fn next_generation(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::Relaxed)
    }

    fn guard(&self) -> std::sync::MutexGuard<'_, Ledger> {
        self.ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn mint(&self, receipt: PublicationReceipt) -> PublicationId {
        PublicationId(
            self.next.fetch_add(1, Ordering::Relaxed),
            std::sync::Arc::new(receipt),
        )
    }

    /// Issue an identity for a REPAIR publication that carries no coverage
    /// claim.
    ///
    /// A repair proves things about specific obligations, not about a region of
    /// an enumeration, so it must not touch the coverage lattice - hence the
    /// explicit empty claim rather than an absent entry, which is a different
    /// fact ("the report went missing").
    pub fn publish_without_report(&self, generation: u64) -> PublicationId {
        self.publish(CoverageClaim {
            reports: Vec::new(),
            generation,
        })
    }

    /// Register a REPAIR-lane claim and issue its publication identity.
    ///
    /// Repair publications carry no checkpoint, gate no boundary waiter, and
    /// are acknowledged through `SyncEngine::ack_publication`. Checkpoint
    /// publications go through [`PendingCoverage::register`] instead, which is
    /// what keeps the two halves of a checkpoint publication - the claim and
    /// the boundary registration - from ever existing separately.
    pub fn publish(&self, claim: CoverageClaim) -> PublicationId {
        let id = self.mint(PublicationReceipt {
            checkpoint: None,
            claim: claim.clone(),
        });
        self.guard().claims.insert(
            id.clone(),
            Publication {
                lane: Lane::Repair,
                claim,
            },
        );
        id
    }

    /// Register one checkpoint, its coverage claim and its boundary
    /// registration as a SINGLE publication.
    ///
    /// The whole transition - carry-forward, supersession lookup, removal,
    /// folding, insertion of the survivor and its boundary - happens under one
    /// lock. Two backfill partitions of one scope are in flight together by
    /// design, and any gap here lets both leave a live entry for the same lane,
    /// after which one acknowledgement discharges only one of them.
    pub fn register(&self, checkpoint: Checkpoint, claim: CoverageClaim) -> PublicationId {
        let lane = Lane::of(&checkpoint);
        let id = self.mint(PublicationReceipt {
            checkpoint: Some(checkpoint.clone()),
            claim: claim.clone(),
        });
        let mut claim = claim;
        let mut ledger = self.guard();

        if let Some(carried) = ledger.carried.take() {
            claim.absorb(carried);
        }

        if lane.supersedes()
            && let Some(index) = ledger
                .boundaries
                .iter()
                .position(|(_, existing, _)| *existing == lane)
        {
            let (superseded, _, _) = ledger.boundaries.remove(index);
            // Only mark it folded if there was something to fold. A publication
            // whose claim an acknowledgement already consumed is answered by
            // the persisted watermark, or by nothing - it must not be handed a
            // second, empty acknowledgement on the strength of this fold.
            if let Some(old) = ledger.claims.remove(&superseded) {
                claim.absorb(old.claim);
                let mark = ledger
                    .folded
                    .entry(lane.clone())
                    .or_insert(superseded.clone());
                if *mark < superseded {
                    *mark = superseded;
                }
            }
        }

        // Backstop for an unkeyed `Checkpoint` variant, which never supersedes.
        // The debt still carries forward; only the boundary registration goes.
        if ledger.boundaries.len() >= PENDING_BOUNDARY_CAP {
            let (dropped, _, checkpoint) = ledger.boundaries.remove(0);
            tracing::warn!(
                target: "bifrost.sync.control",
                cap = PENDING_BOUNDARY_CAP,
                dropped = ?checkpoint,
                "boundary registrations at capacity; dropping the oldest outstanding broadcast"
            );
            if let Some(old) = ledger.claims.remove(&dropped) {
                carry_forward(&mut ledger.carried, old.claim.debt_only());
            }
        }

        ledger.claims.insert(
            id.clone(),
            Publication {
                lane: lane.clone(),
                claim,
            },
        );
        ledger.boundaries.push((id.clone(), lane, checkpoint));
        id
    }

    /// Release the boundary registration of an acknowledged publication.
    ///
    /// Identified by PUBLICATION, never by checkpoint value: equal checkpoint
    /// values legitimately describe different publications (a backfill page
    /// that materialised nothing repeats its predecessor's bytes; fusion emits
    /// the final checkpoint on both the last batch and `Done`), so a
    /// value search can release a boundary belonging to a different, still
    /// in-flight publication.
    pub fn acknowledge_publication(&self, id: PublicationId) {
        let mut ledger = self.guard();
        if let Some(index) = ledger
            .boundaries
            .iter()
            .position(|(candidate, _, _)| *candidate == id)
        {
            ledger.boundaries.remove(index);
        }
    }

    /// Release the boundary registration of a publication that will never
    /// produce a durable checkpoint, and drop its claim.
    ///
    /// The claim is NOT carried forward. Either the batch reached no real
    /// subscriber - so nothing it proved was consumed and the next walk must
    /// re-prove it - or its store write failed, in which case its claim was
    /// already applied to the in-memory ledger by the acknowledgement.
    pub fn retire_publication(&self, id: PublicationId) {
        let mut ledger = self.guard();
        if let Some(index) = ledger
            .boundaries
            .iter()
            .position(|(candidate, _, _)| *candidate == id)
        {
            ledger.boundaries.remove(index);
        }
        ledger.claims.remove(&id);
    }

    /// Best-effort boundary release for a caller that holds only a checkpoint
    /// value - the public [`crate::SyncControl::record_checkpoint`] hook. Every
    /// engine-internal path uses [`PendingCoverage::acknowledge_publication`],
    /// which cannot mistake one publication for another.
    pub fn acknowledge_checkpoint(&self, checkpoint: &Checkpoint) {
        let mut ledger = self.guard();
        if let Some(index) = ledger
            .boundaries
            .iter()
            .position(|(_, _, candidate)| candidate == checkpoint)
        {
            ledger.boundaries.remove(index);
        }
    }

    /// Abandon every outstanding boundary registration after a subscriber lag.
    ///
    /// Their batches were destroyed in the ring, so no acknowledgement can ever
    /// arrive and boundary waiters would wait forever. The DEBT each claim
    /// carried survives into the next acknowledgeable publication; what it
    /// PROVED CLEAN does not, because discharging obligations on the strength
    /// of a batch nobody received is the silent loss this ledger exists to
    /// prevent.
    ///
    /// Returns how many registrations were abandoned.
    pub fn abandon_checkpoints(&self) -> usize {
        let ids: Vec<_> = {
            let mut ledger = self.guard();
            ledger.boundaries.drain(..).map(|(id, _, _)| id).collect()
        };
        let count = ids.len();
        self.abandon(ids);
        count
    }

    /// Retire every checkpoint publication for one scope, fence late
    /// acknowledgements of them, and return the debt that must survive the
    /// invalidation - as ONE operation under the ledger lock.
    ///
    /// Atomicity is the point. Snapshotting the debt, awaiting the store
    /// deletes, and retiring afterwards leaves a window in which a still-running
    /// scope registers a further publication: the later retirement drops it, but
    /// its debt was never in the snapshot the writer persisted, so the coverage
    /// obligation is lost silently. The writer therefore calls this BEFORE its
    /// first await and persists what it returns.
    pub(crate) fn invalidate_scope(&self, scope: &CursorScope) -> CoverageClaim {
        let mut ledger = self.guard();
        let ids: Vec<_> = ledger
            .boundaries
            .iter()
            .filter(|(_, lane, _)| lane.belongs_to(scope))
            .map(|(id, _, _)| id.clone())
            .collect();
        ledger
            .boundaries
            .retain(|(_, lane, _)| !lane.belongs_to(scope));
        let mut carried = CoverageClaim {
            reports: Vec::new(),
            generation: 0,
        };
        for id in ids {
            if let Some(publication) = ledger.claims.remove(&id) {
                carried.absorb(publication.claim.clone().debt_only());
            }
        }
        Self::fence_in(&mut ledger, scope, self.next.load(Ordering::Relaxed));
        carried
    }

    fn fence_in(ledger: &mut Ledger, scope: &CursorScope, watermark: u64) {
        let entry = ledger.fenced.entry(scope.clone()).or_insert(watermark);
        *entry = (*entry).max(watermark);
    }

    #[must_use]
    pub fn pending_checkpoints(&self) -> usize {
        self.guard().boundaries.len()
    }

    /// Fold `superseded` into `survivor` and drop the superseded entry.
    ///
    /// Removing the older claim WITHOUT folding it is how partition A's
    /// obligations get discarded while the engine believes B accounted for
    /// them. `register` performs this inline for the checkpoint lane; this is
    /// the standalone form.
    pub fn supersede(&self, superseded: PublicationId, survivor: PublicationId) {
        let mut ledger = self.guard();
        let Some(old) = ledger.claims.remove(&superseded) else {
            return;
        };
        if let Some(new) = ledger.claims.get_mut(&survivor) {
            new.claim.absorb(old.claim);
        } else {
            // The survivor is gone or already persisted, so folding has nowhere
            // to land. Put the claim back rather than dropping it on the floor.
            ledger.claims.insert(superseded, old);
        }
    }

    /// Resolve an acknowledgement of a CHECKPOINT publication.
    ///
    /// The checkpoint names the lane, so an id belonging to a repair
    /// publication - or to a different lane entirely - resolves to `Unknown`
    /// rather than consuming a claim it does not name.
    pub fn claim_checkpoint(&self, id: PublicationId, checkpoint: &Checkpoint) -> ClaimLookup {
        self.claim_in(id, &Lane::of(checkpoint))
    }

    /// Resolve an acknowledgement of a REPAIR publication.
    ///
    /// A checkpoint publication's id resolves to `Unknown` here. That is the
    /// whole point: consuming it would let the later, real `ack_checkpoint`
    /// short-circuit as already persisted and make the engine announce a
    /// durable boundary the store never wrote.
    pub fn claim_repair(&self, id: PublicationId) -> ClaimLookup {
        self.claim_in(id, &Lane::Repair)
    }

    fn claim_in(&self, id: PublicationId, lane: &Lane) -> ClaimLookup {
        let mut ledger = self.guard();
        // The fence is checked before the live claims, not only after them: a
        // publication registered DURING a scope reset's store deletes is still
        // live in `claims`, and applying it would re-create the cursor row the
        // reset deleted to force re-establishment. The fence is an exclusive
        // bound - it holds the mint counter, whose current value is the id the
        // next, post-reset publication will take.
        if let Some(scope) = lane.scope()
            && ledger
                .fenced
                .get(scope)
                .is_some_and(|watermark| *watermark > id.0)
        {
            // Nothing is dropped here: `invalidate_scope` runs twice around the
            // deletes and has already retired these publications and carried
            // their degraded debt into the persisted ledger. This arm only
            // refuses the acknowledgement.
            return ClaimLookup::Unknown;
        }
        match ledger.claims.get(&id) {
            Some(publication) if publication.lane == *lane => {
                let publication = ledger
                    .claims
                    .remove(&id)
                    .unwrap_or_else(|| unreachable!("checked present under the same lock"));
                ClaimLookup::Apply(publication.claim)
            }
            // Live, but in another lane. Leave it alone; the acknowledgement
            // that really names it can still arrive.
            Some(_) => ClaimLookup::Unknown,
            None => {
                if ledger
                    .persisted
                    .get(lane)
                    .is_some_and(|watermark| *watermark >= id)
                {
                    ClaimLookup::AlreadyPersisted
                } else if let Some(claim) = ledger
                    .folded
                    .get(lane)
                    .filter(|watermark| **watermark >= id)
                    .map(|_| CoverageClaim {
                        reports: Vec::new(),
                        generation: 0,
                    })
                {
                    // Superseded, and its coverage moved to a successor that is
                    // still outstanding. The BOUNDARY is real - the consumer
                    // reached it - so persist the checkpoint, but ingest
                    // nothing, or the successor's acknowledgement would ingest
                    // the same reports a second time.
                    ClaimLookup::Apply(claim)
                } else if id
                    .1
                    .checkpoint
                    .as_ref()
                    .is_some_and(|saved| Lane::of(saved) == *lane)
                    || (*lane == Lane::Repair && id.1.checkpoint.is_none())
                {
                    ClaimLookup::Apply(id.1.claim.clone())
                } else {
                    ClaimLookup::Unknown
                }
            }
        }
    }

    /// Record that a publication's store write LANDED.
    ///
    /// Called after the write, not before it, so a retried acknowledgement of a
    /// write that failed reports `Unknown` - honestly refusing - rather than
    /// `AlreadyPersisted`, which would make the engine announce a durable
    /// boundary that does not exist.
    pub fn settle_checkpoint(&self, id: PublicationId, checkpoint: &Checkpoint) {
        self.settle_in(id, Lane::of(checkpoint));
    }

    /// Record that a repair publication was durably applied.
    pub fn settle_repair(&self, id: PublicationId) {
        self.settle_in(id, Lane::Repair);
    }

    fn settle_in(&self, id: PublicationId, lane: Lane) {
        let mut ledger = self.guard();
        let watermark = ledger.persisted.entry(lane).or_insert(id.clone());
        if *watermark < id {
            *watermark = id;
        }
    }

    /// Kind-agnostic lookup, retained for callers that already know which lane
    /// an id belongs to. Engine acknowledgement paths use
    /// [`PendingCoverage::claim_checkpoint`] or
    /// [`PendingCoverage::claim_repair`], which refuse an id from the wrong
    /// lane instead of consuming it.
    pub fn claim(&self, id: PublicationId) -> ClaimLookup {
        let lane = self.guard().claims.get(&id).map(|p| p.lane.clone());
        match lane {
            Some(lane) => self.claim_in(id, &lane),
            None => ClaimLookup::Unknown,
        }
    }

    /// Abandon publications whose batches were lost, preserving the DEBT they
    /// carried for the next publication that can be acknowledged.
    ///
    /// See [`CoverageClaim::debt_only`] for why only the debt travels.
    pub fn abandon<I>(&self, ids: I)
    where
        I: IntoIterator<Item = PublicationId>,
    {
        let mut ledger = self.guard();
        for id in ids {
            if let Some(publication) = ledger.claims.remove(&id) {
                let debt = publication.claim.debt_only();
                carry_forward(&mut ledger.carried, debt);
            }
            if let Some(index) = ledger
                .boundaries
                .iter()
                .position(|(candidate, _, _)| *candidate == id)
            {
                ledger.boundaries.remove(index);
            }
        }
    }

    /// Drop a publication that can never be acknowledged: it reached no real
    /// subscriber, its subscriber lagged out, or the account detached.
    ///
    /// Without this the registry grows for the life of the attachment.
    pub fn retire(&self, id: PublicationId) {
        self.guard().claims.remove(&id);
    }

    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.guard().claims.len()
    }
}

fn carry_forward(carried: &mut Option<CoverageClaim>, claim: CoverageClaim) {
    if claim.is_empty() {
        return;
    }
    match carried.as_mut() {
        Some(existing) => existing.absorb(claim),
        None => *carried = Some(claim),
    }
}

/// The unified per-account publication ledger.
///
/// The old name remains as a compatibility alias while callers migrate.
pub type Publications = PendingCoverage;

#[cfg(test)]
mod tests {
    use super::{ClaimLookup, CoverageClaim, PendingCoverage};
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, Cause, ChangeCursor, Checkpoint, CoverageDomain,
        CursorScope, DiagnosticText, InventoryCoverageReport, InventoryObligation, ObjectId,
        ObjectType, ObligationKey, OpaqueChangeState, ProtocolKind, RequestCause, RequestErrorKind,
    };

    fn scope() -> CursorScope {
        CursorScope::Type(ObjectType::Email)
    }

    fn degraded(key: &str) -> InventoryCoverageReport {
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("unrepresentable"),
            }),
        )
        .try_build()
        .expect("valid account error classification");
        InventoryCoverageReport::degraded(
            CoverageDomain::full(scope()),
            vec![InventoryObligation::Object {
                key: ObligationKey(key.as_bytes().to_vec()),
                id: ObjectId(key.into()),
                error,
                repair: Vec::new(),
            }],
        )
    }

    fn checkpoint(state: &[u8]) -> Checkpoint {
        Checkpoint::Change(ChangeCursor {
            scope: scope(),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Imap,
                envelope_version: 1,
                bytes: state.to_vec(),
            },
            advanced_through: None,
            envelope_version: 1,
        })
    }

    fn backfill_checkpoint(partition: &[u8]) -> Checkpoint {
        Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
            scope: scope(),
            partition: bifrost_types::Partition(partition.to_vec()),
            progress_marker: None,
            progress: bifrost_types::BackfillProgress::default(),
            envelope_version: 1,
        })
    }

    #[test]
    fn a_restart_crossing_acknowledgement_carries_its_claim() {
        let first_writer = PendingCoverage::new();
        let id = first_writer.publish(CoverageClaim::new(degraded("restart"), 1));
        let restarted_writer = PendingCoverage::new();
        assert!(matches!(
            restarted_writer.claim_repair(id),
            ClaimLookup::Apply(_)
        ));
    }

    /// Two publications for one scope must not share a slot. This is the
    /// partitioned-backfill case: keyed by scope, acknowledging the first
    /// picked up the second's report.
    #[test]
    fn each_publication_keeps_its_own_claim() {
        let pending = PendingCoverage::new();
        let first = pending.publish(CoverageClaim::new(degraded("a"), 1));
        let second = pending.publish(CoverageClaim::new(degraded("b"), 1));

        let ClaimLookup::Apply(claim) = pending.claim(first) else {
            panic!("first publication must yield its own claim");
        };
        assert_eq!(claim.reports[0].obligations()[0].key().0, b"a".to_vec());

        let ClaimLookup::Apply(claim) = pending.claim(second) else {
            panic!("second publication must yield its own claim");
        };
        assert_eq!(claim.reports[0].obligations()[0].key().0, b"b".to_vec());
    }

    /// A repeated acknowledgement of the SAME publication must be idempotent -
    /// not reapplied, not rejected, and above all not defaulted to complete.
    #[test]
    fn a_duplicate_acknowledgement_is_idempotent() {
        let pending = PendingCoverage::new();
        let cp = checkpoint(b"a");
        let id = pending.register(cp.clone(), CoverageClaim::new(degraded("a"), 1));

        assert!(matches!(
            pending.claim_checkpoint(id.clone(), &cp),
            ClaimLookup::Apply(_)
        ));
        pending.settle_checkpoint(id.clone(), &cp);
        assert!(matches!(
            pending.claim_checkpoint(id, &cp),
            ClaimLookup::AlreadyPersisted
        ));
        assert_eq!(pending.outstanding(), 0);
    }

    /// Idempotency must not expire. A tombstone-per-acknowledgement set is the
    /// leak this ledger was rebuilt to close, and a fixed-size ring closes it by
    /// forgetting: after enough later acknowledgements the retry of an older
    /// successful one reports `Unknown`. A per-lane watermark is bounded by the
    /// account's scope count AND never forgets, so a delayed consumer retry
    /// still answers correctly.
    #[test]
    fn duplicate_acknowledgement_survives_many_later_acknowledgements() {
        let pending = PendingCoverage::new();
        let first_cp = checkpoint(b"first");
        let first = pending.register(first_cp.clone(), CoverageClaim::new(degraded("a"), 1));
        assert!(matches!(
            pending.claim_checkpoint(first.clone(), &first_cp),
            ClaimLookup::Apply(_)
        ));
        pending.settle_checkpoint(first.clone(), &first_cp);

        for round in 0..1024u32 {
            let cp = checkpoint(&round.to_be_bytes());
            let id = pending.register(cp.clone(), CoverageClaim::new(degraded("later"), 2));
            let _ = pending.claim_checkpoint(id.clone(), &cp);
            pending.settle_checkpoint(id, &cp);
        }

        assert!(
            matches!(
                pending.claim_checkpoint(first, &first_cp),
                ClaimLookup::AlreadyPersisted
            ),
            "a delayed retry of an older successful acknowledgement stays idempotent"
        );
        assert!(
            pending.outstanding() <= 1,
            "the ledger must not grow one entry per acknowledgement"
        );
    }

    /// A retried acknowledgement whose store write FAILED must not report
    /// success. The watermark moves only after the write lands, so the retry
    /// gets `Unknown` - refused - rather than `AlreadyPersisted`, which would
    /// make the engine announce a durable boundary that does not exist.
    #[test]
    fn a_failed_write_can_retry_from_its_receipt() {
        let pending = PendingCoverage::new();
        let cp = checkpoint(b"a");
        let id = pending.register(cp.clone(), CoverageClaim::new(degraded("a"), 1));

        assert!(matches!(
            pending.claim_checkpoint(id.clone(), &cp),
            ClaimLookup::Apply(_)
        ));
        // No `settle_checkpoint`: the store write failed.
        assert!(matches!(
            pending.claim_checkpoint(id, &cp),
            ClaimLookup::Apply(_)
        ));
    }

    #[test]
    fn scope_invalidation_refuses_a_late_pre_reset_acknowledgement() {
        let pending = PendingCoverage::new();
        let cp = checkpoint(b"stale");
        let id = pending.register(cp.clone(), CoverageClaim::new(degraded("owed"), 1));

        let carried = pending.invalidate_scope(&scope());

        assert_eq!(carried.reports.len(), 1);
        assert!(matches!(
            pending.claim_checkpoint(id, &cp),
            ClaimLookup::Unknown
        ));
    }

    /// A repair acknowledgement must not be able to consume a CHECKPOINT
    /// publication. If it could, the later real `ack_checkpoint` would find the
    /// claim already resolved, short-circuit as persisted, and the engine would
    /// report a durable boundary the store never wrote.
    #[test]
    fn a_repair_acknowledgement_cannot_consume_a_checkpoint_publication() {
        let pending = PendingCoverage::new();
        let cp = checkpoint(b"a");
        let id = pending.register(cp.clone(), CoverageClaim::new(degraded("a"), 1));

        assert!(matches!(
            pending.claim_repair(id.clone()),
            ClaimLookup::Unknown
        ));
        assert!(
            matches!(pending.claim_checkpoint(id, &cp), ClaimLookup::Apply(_)),
            "the checkpoint acknowledgement must still find its claim intact"
        );
    }

    /// And the mirror: a checkpoint acknowledgement must not consume a repair
    /// publication.
    #[test]
    fn a_checkpoint_acknowledgement_cannot_consume_a_repair_publication() {
        let pending = PendingCoverage::new();
        let repair = pending.publish_without_report(0);

        assert!(matches!(
            pending.claim_checkpoint(repair.clone(), &checkpoint(b"a")),
            ClaimLookup::Unknown
        ));
        assert!(matches!(
            pending.claim_repair(repair),
            ClaimLookup::Apply(_)
        ));
    }

    /// An id this ledger never issued must never resolve to "already
    /// persisted". Both watermarks only ever hold ids this ledger minted, so an
    /// id it never issued is above them and falls through to `Unknown`.
    #[test]
    fn an_id_this_ledger_never_issued_is_unknown() {
        let pending = PendingCoverage::new();
        let cp = checkpoint(b"a");
        let id = pending.register(cp.clone(), CoverageClaim::new(degraded("a"), 1));
        let _ = pending.claim_checkpoint(id.clone(), &cp);
        pending.settle_checkpoint(id, &cp);

        assert!(matches!(
            pending.claim_checkpoint(pending.publish_without_report(0), &cp),
            ClaimLookup::Unknown
        ));
    }

    /// Equal checkpoint VALUES are not one publication. A backfill page that
    /// materialised nothing repeats its predecessor's bytes, and fusion emits
    /// the final checkpoint on both the last batch and `Done`. Releasing a
    /// boundary by value therefore reaches into a DIFFERENT, still in-flight
    /// publication: publication A's completion would release publication B's
    /// registration, and a boundary waiter would be told the consumer reached a
    /// position it has not acknowledged.
    #[test]
    fn completing_one_publication_leaves_an_equal_valued_sibling_registered() {
        let pending = PendingCoverage::new();
        let cp = backfill_checkpoint(b"page-a");
        let first = pending.register(cp.clone(), CoverageClaim::new(degraded("first"), 1));
        let second = pending.register(cp.clone(), CoverageClaim::new(degraded("second"), 2));
        assert_ne!(first, second, "equal values, different publications");
        assert_eq!(pending.pending_checkpoints(), 1);

        // A's store write completes while B is still outstanding.
        pending.acknowledge_publication(first);
        assert_eq!(
            pending.pending_checkpoints(),
            1,
            "B's registration must survive A's completion"
        );

        // The value-identified path cannot tell them apart - which is why every
        // engine path uses the publication.
        pending.acknowledge_checkpoint(&cp);
        assert_eq!(pending.pending_checkpoints(), 0);
    }

    /// Same shape for retirement, which additionally takes the claim with it.
    /// Retiring by value would discard publication B's coverage debt because
    /// publication A's write failed.
    #[test]
    fn retiring_one_publication_leaves_an_equal_valued_siblings_claim_intact() {
        let pending = PendingCoverage::new();
        let cp = backfill_checkpoint(b"page-a");
        let first = pending.register(cp.clone(), CoverageClaim::new(degraded("first"), 1));
        let second = pending.register(cp.clone(), CoverageClaim::new(degraded("second"), 2));

        pending.retire_publication(first);

        assert_eq!(pending.outstanding(), 1);
        let ClaimLookup::Apply(claim) = pending.claim_checkpoint(second, &cp) else {
            panic!("the surviving publication must still be acknowledgeable");
        };
        assert_eq!(
            claim.reports.len(),
            2,
            "and it must still carry the folded debt of its predecessor"
        );
    }

    /// A consumer may acknowledge a batch that has since been superseded -
    /// producers routinely run ahead of consumers. The boundary is real, so it
    /// persists; the coverage is not, because it moved to the survivor and
    /// ingesting it twice would double-count.
    #[test]
    fn acknowledging_a_superseded_publication_persists_without_reingesting() {
        let pending = PendingCoverage::new();
        let cp = backfill_checkpoint(b"page-a");
        let first = pending.register(cp.clone(), CoverageClaim::new(degraded("first"), 1));
        let second = pending.register(cp.clone(), CoverageClaim::new(degraded("second"), 2));

        let ClaimLookup::Apply(claim) = pending.claim_checkpoint(first, &cp) else {
            panic!("a superseded batch the consumer received is still acknowledgeable");
        };
        assert!(
            claim.reports.is_empty(),
            "its coverage rides the survivor now"
        );

        let ClaimLookup::Apply(claim) = pending.claim_checkpoint(second, &cp) else {
            panic!("the survivor keeps its own acknowledgement");
        };
        assert_eq!(claim.reports.len(), 2);
    }

    /// Two producers registering for one lane concurrently must leave exactly
    /// ONE live entry. A gap between removing the superseded entry and
    /// inserting the survivor lets both insert, and a later acknowledgement
    /// then discharges only half the lane.
    #[test]
    fn concurrent_registration_leaves_one_entry_per_lane() {
        use std::sync::Arc;
        let pending = Arc::new(PendingCoverage::new());
        let mut handles = Vec::new();
        for round in 0..8u32 {
            let pending = Arc::clone(&pending);
            handles.push(std::thread::spawn(move || {
                for _ in 0..64 {
                    pending.register(
                        backfill_checkpoint(b"page-a"),
                        CoverageClaim::new(degraded("x"), u64::from(round)),
                    );
                }
            }));
        }
        for handle in handles {
            handle.join().expect("registrar thread");
        }

        assert_eq!(
            pending.pending_checkpoints(),
            1,
            "one lane must hold exactly one boundary registration"
        );
        assert_eq!(
            pending.outstanding(),
            1,
            "and exactly one live claim, with every superseded report folded in"
        );
    }

    /// Superseding must FOLD. Dropping the superseded claim discards debt the
    /// survivor never re-reported, which is the whole failure mode.
    #[test]
    fn supersession_folds_the_older_claim_into_the_survivor() {
        let pending = PendingCoverage::new();
        let older = pending.publish(CoverageClaim::new(degraded("a"), 1));
        let newer = pending.publish(CoverageClaim::new(degraded("b"), 2));

        pending.supersede(older.clone(), newer.clone());

        assert!(matches!(pending.claim(older), ClaimLookup::Unknown));
        let ClaimLookup::Apply(claim) = pending.claim(newer) else {
            panic!("survivor must still be acknowledgeable");
        };
        assert_eq!(claim.reports.len(), 2, "the superseded report must survive");
        assert_eq!(claim.reports[0].obligations()[0].key().0, b"a".to_vec());
        assert_eq!(claim.generation, 2);
    }

    #[test]
    fn registering_a_new_checkpoint_folds_the_superseded_claim() {
        let publications = PendingCoverage::new();
        publications.register(checkpoint(b"old"), CoverageClaim::new(degraded("a"), 1));
        let survivor =
            publications.register(checkpoint(b"new"), CoverageClaim::new(degraded("b"), 2));

        let ClaimLookup::Apply(claim) = publications.claim(survivor) else {
            panic!("survivor must be acknowledgeable");
        };
        assert_eq!(claim.reports.len(), 2);
        assert_eq!(claim.reports[0].obligations()[0].key().0, b"a".to_vec());
    }

    /// If the survivor cannot absorb it, the claim stays put rather than
    /// vanishing.
    #[test]
    fn supersession_into_a_persisted_survivor_keeps_the_older_claim() {
        let pending = PendingCoverage::new();
        let older = pending.publish(CoverageClaim::new(degraded("a"), 1));
        let newer = pending.publish(CoverageClaim::new(degraded("b"), 2));
        let _ = pending.claim(newer.clone());

        pending.supersede(older.clone(), newer);
        assert!(matches!(pending.claim(older), ClaimLookup::Apply(_)));
    }

    /// Ring overflow destroys a batch. Its DEBT must survive into the next
    /// publication anyone can acknowledge, or a degraded page destroyed by the
    /// ring produces no obligation at all - the under-reporting that costs
    /// objects nobody ever sees again.
    #[test]
    fn lagged_publication_claims_are_carried_to_the_next_acknowledgeable_batch() {
        let pending = PendingCoverage::new();
        pending.register(checkpoint(b"lost"), CoverageClaim::new(degraded("lost"), 1));

        assert_eq!(pending.abandon_checkpoints(), 1);
        assert_eq!(pending.outstanding(), 0);

        let next = checkpoint(b"retained");
        let survivor = pending.register(next.clone(), CoverageClaim::new(degraded("retained"), 2));
        let ClaimLookup::Apply(claim) = pending.claim_checkpoint(survivor, &next) else {
            panic!("next publication must remain acknowledgeable");
        };
        assert_eq!(claim.reports.len(), 2);
        assert_eq!(claim.reports[0].obligations()[0].key().0, b"lost".to_vec());
        assert_eq!(
            claim.reports[1].obligations()[0].key().0,
            b"retained".to_vec()
        );
    }

    /// What a lost batch PROVED CLEAN must not travel. Folding a `Complete`
    /// report into a later publication discharges obligations on the strength
    /// of an enumeration the consumer never received - the same silent loss,
    /// arriving through the fix for it.
    #[test]
    fn a_lagged_complete_report_does_not_discharge_anything() {
        let pending = PendingCoverage::new();
        let clean = InventoryCoverageReport::complete(CoverageDomain::full(scope()));
        pending.register(checkpoint(b"lost"), CoverageClaim::new(clean, 1));

        assert_eq!(pending.abandon_checkpoints(), 1);

        let next = checkpoint(b"next");
        let survivor = pending.register(next.clone(), CoverageClaim::new(degraded("owed"), 2));
        let ClaimLookup::Apply(claim) = pending.claim_checkpoint(survivor, &next) else {
            panic!("next publication must remain acknowledgeable");
        };
        assert_eq!(
            claim.reports.len(),
            1,
            "only the survivor's own report may prove coverage"
        );
        assert_eq!(claim.reports[0].obligations()[0].key().0, b"owed".to_vec());
    }

    /// An ordinary changes advance carries no coverage claim and must leave the
    /// ledger alone - distinct from a missing report.
    #[test]
    fn a_changes_advance_publishes_an_empty_claim() {
        let pending = PendingCoverage::new();
        let id = pending.publish_without_report(1);
        let ClaimLookup::Apply(claim) = pending.claim(id) else {
            panic!("an empty claim is still a known publication");
        };
        assert!(claim.reports.is_empty());
    }

    #[test]
    fn retiring_an_unacknowledgeable_publication_frees_it() {
        let pending = PendingCoverage::new();
        let id = pending.publish(CoverageClaim::new(degraded("a"), 1));
        assert_eq!(pending.outstanding(), 1);
        pending.retire(id.clone());
        assert_eq!(pending.outstanding(), 0);
        assert!(matches!(pending.claim(id), ClaimLookup::Unknown));
    }
}
