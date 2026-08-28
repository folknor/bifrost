//! The durable debt ledger: accumulated coverage lifecycle state.
//!
//! Distinct from `InventoryCoverageReport`, and the distinction is the point.
//! A report is one account's observation about one enumeration of one region.
//! The ledger is what the ENGINE has concluded after folding every accepted
//! report, every operator decision and (eventually) every repair result
//! together. Calling both "coverage" hid a seam where the interesting mistakes
//! live: an account reports evidence, the engine owns retry and
//! acceptable-loss policy, and no protocol crate may construct ledger state.
//!
//! Two axes, deliberately independent:
//!
//! - PROOF is what the system knows: `Unresolved` or `Discharged`.
//! - POLICY is what the system will do about it: `Retrying`, `OperatorBlocked`
//!   or `Waived`.
//!
//! Collapsing them is the mistake this module exists to prevent. A waiver is
//! accepted loss, NOT proof that the object was materialized or proved
//! irrelevant, so a waived obligation must never make a record claim complete
//! coverage - otherwise an operator (or a later full walk) cannot tell proved
//! coverage from loss somebody agreed to live with. Waiver may stop the
//! obligation blocking automatic backfill completion; it may not rewrite
//! history.
//!
//! The other rule with teeth: no local counter may produce `Waived` or
//! `Discharged`. A retry budget expiring is evidence that automatic work is not
//! helping, which is `OperatorBlocked` - the obligation stays visible and
//! manually retryable. Only an operator waives, because only an operator can
//! decide what loss is acceptable.

use std::collections::BTreeMap;

use bifrost_types::{
    AccountError, CoverageCoordinate, CoverageDomain, InventoryCoverageReport, InventoryObligation,
    InventoryRepairTarget, ObligationKey,
};

/// What the system KNOWS about an obligation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProofStatus {
    /// Coverage was never proved for this obligation.
    Unresolved,
    /// A later accepted proof covered it. The gap is genuinely closed.
    Discharged { evidence: DischargeEvidence },
}

/// Bound on parent-link walking, so a cycle from a buggy replacement cannot
/// hang the single writer every durable mutation funnels through.
const LINEAGE_DEPTH_CAP: usize = 64;

/// Whether a replacement improved the situation or merely reshaped it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplacementProgress {
    Progressed,
    /// Same unresolved extent, same granularity, no proof gained, no better
    /// repair authority. Repeated stalls exhaust the lineage budget rather
    /// than looping forever.
    Stalled,
}

/// Why the engine refused a replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplacementRefusal {
    UnknownObligation,
    /// The obligation moved on, or was already closed, since this repair result
    /// was computed.
    StaleGeneration,
    /// A child key is already open under a different lineage root. Accepting it
    /// would give one obligation two roots and let a retry budget reset by
    /// picking whichever root is convenient.
    ChildCollidesWithForeignLineage,
}

/// Why an obligation is considered discharged.
///
/// Recorded rather than inferred so an audit can tell a re-enumeration from a
/// provider-native repair - and, once repair exists, so a discharge can be
/// attributed to the pass that earned it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DischargeEvidence {
    /// A later accepted report proved a domain covering this obligation.
    CoveringWalk { domain: CoverageDomain },
    /// A repair pass re-read the object authoritatively, rebuilt its entry, and
    /// the consumer durably accepted the resulting existence notification.
    ///
    /// Both halves are recorded because neither alone suffices: an account
    /// recovery nobody was told about leaves the consumer unaware, and a
    /// published id with no successful account result merely repeats an id
    /// without proving the representation failure healed.
    RepairedAndPublished {
        attempt: bifrost_types::RepairAttemptId,
    },
    /// A repair pass proved the object is not owed at all - absent under a
    /// cursor bridge that will report its removal, or out of scope. Emits
    /// nothing to the consumer: absence from an old inventory snapshot is not a
    /// deletion to apply against current state.
    ProvedIrrelevant { detail: String },
    /// Superseded by the children it was split into. The parent is closed so it
    /// cannot be replayed or double-counted; the debt lives on in the children.
    ReplacedByChildren { children: Vec<ObligationKey> },
}

/// What the system will DO about an obligation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PolicyStatus {
    /// Eligible for automatic repair. `attempts` counts COMPLETED repair
    /// outcomes received from the account, never attempts merely started: a
    /// crash after provider work but before the acknowledgement must cost a
    /// repeated attempt, not a consumed budget.
    Retrying { attempts: u32 },
    /// Automatic work stopped. Still visible, still manually retryable, still
    /// blocking the completion sentinel. NOT abandonment.
    OperatorBlocked,
    /// An operator explicitly accepted this loss. The only terminal policy
    /// state, and the only one a human can put an obligation into.
    Waived { by: String, at_unix_seconds: i64 },
}

impl PolicyStatus {
    #[must_use]
    pub fn is_waived(&self) -> bool {
        matches!(self, Self::Waived { .. })
    }
}

/// One obligation as the engine tracks it over time.
#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub key: ObligationKey,
    /// The region whose enumeration raised this. Discharge compares against
    /// THIS, not against the scope: a later walk must actually cover the
    /// ground the gap was on.
    pub domain: CoverageDomain,
    /// Engine-issued walk generation that raised it. Orders proof events; does
    /// not itself prove coverage.
    pub generation: u64,
    pub proof: ProofStatus,
    pub policy: PolicyStatus,
    /// The account-minted material a repair pass needs to address this.
    ///
    /// Retained because the executor cannot reconstruct a request without it:
    /// key, domain and error are engine- or diagnostic-level facts, and the
    /// error in particular must never be used as a repair descriptor because
    /// classifications and messages change between revisions. `None` for an
    /// obligation with no repair path.
    pub target: Option<InventoryRepairTarget>,
    /// The obligation this one replaced, when a repair pass narrowed a region
    /// into smaller pieces. Retry budgeting follows the LINEAGE, so an account
    /// cannot reset a budget by re-minting an equivalent obligation.
    pub parent: Option<ObligationKey>,
    pub first_seen_unix_seconds: i64,
    pub last_error: AccountError,
}

impl LedgerEntry {
    /// Whether this entry still blocks a backfill completion sentinel.
    ///
    /// Waived entries do not block: an operator accepted the loss, and the
    /// point of the waiver is to let a scope finish. They remain `Unresolved`
    /// forever regardless, which is what keeps the audit honest.
    #[must_use]
    pub fn blocks_completion(&self) -> bool {
        match self.proof {
            ProofStatus::Discharged { .. } => false,
            ProofStatus::Unresolved => !self.policy.is_waived(),
        }
    }

    #[must_use]
    pub fn is_open(&self) -> bool {
        matches!(self.proof, ProofStatus::Unresolved)
    }
}

/// A walk that could not advance, recorded separately from the debt ledger.
///
/// A barrier is not debt behind an advanced cursor - no checkpoint was accepted
/// past it, so there is nothing durable it could hang off. But writing nothing
/// at all leaves the scope with no memory: a restart forgets the walk keeps
/// hitting the same wall, attempt history resets, operator visibility vanishes
/// between sessions, and there is no object for an operator to waive. So the
/// blocked progress itself is the durable thing.
///
/// A waived barrier authorizes a later walk to cross only through
/// `cross_waived_barriers`, which atomically converts the incident into an
/// unresolved-but-waived ledger entry. The waiver converts blocked progress
/// into declared accepted loss; it never makes the engine simply forget.
#[derive(Debug, Clone)]
pub struct BarrierIncident {
    pub key: ObligationKey,
    pub domain: CoverageDomain,
    pub generation: u64,
    pub failure_label: String,
    pub evidence: AccountError,
    pub policy: PolicyStatus,
    /// The last checkpoint proved to precede the whole barrier region, if the
    /// walk accepted one. Where a later walk resumes from.
    pub resume_from: Option<bifrost_types::Checkpoint>,
}

/// Per-account accumulated debt.
///
/// Ordered map so enumeration is deterministic - operator output and test
/// assertions both depend on it.
#[derive(Debug, Clone, Default)]
pub struct DebtLedger {
    entries: BTreeMap<ObligationKey, LedgerEntry>,
    barriers: BTreeMap<ObligationKey, BarrierIncident>,
    /// Domains proved complete, with the generation that proved them. Retained
    /// because coverage discharges by UNION: repartitioning can leave old debt
    /// covered by two later windows and by neither alone.
    proved: Vec<(u64, CoverageDomain)>,
}

impl DebtLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.barriers.is_empty()
    }

    pub fn entries(&self) -> impl Iterator<Item = &LedgerEntry> {
        self.entries.values()
    }

    pub fn barriers(&self) -> impl Iterator<Item = &BarrierIncident> {
        self.barriers.values()
    }

    #[must_use]
    pub fn entry(&self, key: &ObligationKey) -> Option<&LedgerEntry> {
        self.entries.get(key)
    }

    /// Every obligation still owed: unresolved and unwaived.
    pub fn open_debt(&self) -> impl Iterator<Item = &LedgerEntry> {
        self.entries
            .values()
            .filter(|entry| entry.blocks_completion())
    }

    /// Whether a backfill completion sentinel may be written for `scope`.
    ///
    /// Evaluated against the ledger AS IT STANDS, never from a boolean computed
    /// earlier by a walk. A runner that decided "complete" before a sibling
    /// partition's debt was accepted would otherwise write a sentinel that
    /// makes the next attach skip a scope with open obligations - and the
    /// inverse, a runner that saw degraded coverage before an operator waived
    /// it, would leave the scope incomplete for no reason.
    #[must_use]
    pub fn completion_permitted(&self, scope: &bifrost_types::CursorScope) -> bool {
        let blocked_entry = self
            .entries
            .values()
            .any(|entry| entry.domain.scope == *scope && entry.blocks_completion());
        let blocked_barrier = self
            .barriers
            .values()
            .any(|barrier| barrier.domain.scope == *scope && !barrier.policy.is_waived());
        !blocked_entry && !blocked_barrier
    }

    /// Whether a barrier at `key` has been waived, authorizing a walk to cross
    /// it and record the loss instead of stopping.
    #[must_use]
    pub fn barrier_waived(&self, key: &ObligationKey) -> bool {
        self.barriers
            .get(key)
            .is_some_and(|barrier| barrier.policy.is_waived())
    }

    #[must_use]
    pub fn scope_has_blocked_barrier(&self, scope: &bifrost_types::CursorScope) -> bool {
        self.barriers.values().any(|barrier| {
            barrier.domain.scope == *scope
                && matches!(barrier.policy, PolicyStatus::OperatorBlocked)
        })
    }

    /// Convert every barrier in `report` into ordinary accepted-loss debt.
    ///
    /// This is all-or-nothing. A checkpoint crosses the whole report domain,
    /// so one unwaived or unknown barrier keeps the walk stopped. The account
    /// writer calls this and persists the resulting ledger as one ordered
    /// operation, making a waiver or block that races the walk resolve by
    /// writer order rather than by a stale snapshot.
    pub fn cross_waived_barriers(
        &mut self,
        report: &InventoryCoverageReport,
        generation: u64,
        now: i64,
    ) -> bool {
        let barriers: Vec<_> = report
            .obligations()
            .iter()
            .filter(|obligation| obligation.is_barrier())
            .collect();
        if barriers.is_empty()
            || barriers.iter().any(|obligation| {
                let key = obligation.key();
                !self.barrier_waived(key)
                    && !self
                        .entries
                        .get(key)
                        .is_some_and(|entry| entry.policy.is_waived())
            })
        {
            return false;
        }

        for obligation in barriers {
            let key = obligation.key().clone();
            if let Some(incident) = self.barriers.remove(&key) {
                self.entries.insert(
                    key.clone(),
                    LedgerEntry {
                        key,
                        domain: report.domain.clone(),
                        generation,
                        proof: ProofStatus::Unresolved,
                        policy: incident.policy,
                        target: None,
                        parent: None,
                        first_seen_unix_seconds: now,
                        last_error: obligation.error().clone(),
                    },
                );
            } else if let Some(entry) = self.entries.get_mut(&key) {
                // Terminal reports are commonly cumulative with the final
                // page. Repeating an already-crossed barrier must remain
                // authorized by the same waiver rather than recreating a wall
                // one event later.
                entry.domain = report.domain.clone();
                entry.generation = generation;
                entry.proof = ProofStatus::Unresolved;
                entry.target = None;
                entry.last_error = obligation.error().clone();
            }
        }
        true
    }

    /// Fold one ACCEPTED coverage report into the ledger.
    ///
    /// "Accepted" is load-bearing: this runs when the consumer acknowledges the
    /// checkpoint the report rode on, not when the account emitted it. A report
    /// whose checkpoint is never acknowledged may describe entries the consumer
    /// never persisted, so it cannot be allowed to prove anything.
    ///
    /// The single exhaustive ingestion path. Nothing else mutates entries.
    pub fn ingest(&mut self, report: &InventoryCoverageReport, generation: u64, now: i64) {
        for obligation in report.obligations() {
            // A barrier never becomes ledger debt here. It is recorded as a
            // blocked-progress incident by the walk that hit it, and only
            // becomes a (waived, unresolved) entry when an operator authorizes
            // crossing it.
            if obligation.is_barrier() {
                continue;
            }
            self.upsert(obligation, &report.domain, generation, now);
        }

        if report.is_complete() {
            self.record_proof(report.domain.clone(), generation);
        }
    }

    /// Fold in only the DEBT from a report, never its proof.
    ///
    /// For a report that reached the engine without an acknowledgeable
    /// checkpoint - a partition walk's terminal summary, say. Recording debt
    /// for progress that may not have committed is conservative: the worst case
    /// is a scope that stays degraded until something re-reads it. Recording
    /// PROOF on the same terms would not be, because the consumer may never
    /// have persisted the entries that proof rests on.
    pub fn ingest_debt_only(
        &mut self,
        report: &InventoryCoverageReport,
        generation: u64,
        now: i64,
    ) {
        for obligation in report.obligations() {
            if obligation.is_barrier() {
                continue;
            }
            self.upsert(obligation, &report.domain, generation, now);
        }
    }

    fn upsert(
        &mut self,
        obligation: &InventoryObligation,
        domain: &CoverageDomain,
        generation: u64,
        now: i64,
    ) {
        let key = obligation.key().clone();
        match self.entries.get_mut(&key) {
            Some(existing) => {
                // A re-raised obligation keeps its history. Resetting
                // `first_seen` or the attempt count each time the same broken
                // object is rediscovered would make any budget unreachable.
                existing.generation = generation;
                existing.domain = domain.clone();
                existing.last_error = obligation.error().clone();
                // The token may legitimately rotate while naming the same gap,
                // so the descriptor refreshes even though identity does not.
                existing.target = InventoryRepairTarget::from_obligation(obligation);
                // Rediscovery is proof it is still missing, so an earlier
                // discharge was wrong. Reopen it - but never touch policy: an
                // operator's waiver stands until the operator revokes it.
                existing.proof = ProofStatus::Unresolved;
            }
            None => {
                self.entries.insert(
                    key.clone(),
                    LedgerEntry {
                        key,
                        domain: domain.clone(),
                        generation,
                        proof: ProofStatus::Unresolved,
                        policy: PolicyStatus::Retrying { attempts: 0 },
                        target: InventoryRepairTarget::from_obligation(obligation),
                        parent: None,
                        first_seen_unix_seconds: now,
                        last_error: obligation.error().clone(),
                    },
                );
            }
        }
    }

    /// Record a domain proved complete, and discharge whatever it covers.
    fn record_proof(&mut self, domain: CoverageDomain, generation: u64) {
        self.proved.push((generation, domain));
        for entry in self.entries.values_mut() {
            if !entry.is_open() {
                continue;
            }
            // Both conditions, not either. A newer generation stops a stale
            // report overwriting fresh state, but recency alone proves nothing
            // - a newer PARTIAL walk is still partial. The domain has to
            // actually contain the ground the gap was on.
            if proof_union_covers(&self.proved, entry.generation, &entry.domain) {
                entry.proof = ProofStatus::Discharged {
                    evidence: DischargeEvidence::CoveringWalk {
                        domain: entry.domain.clone(),
                    },
                };
            }
        }
        self.barriers.retain(|_, barrier| {
            !proof_union_covers(&self.proved, barrier.generation, &barrier.domain)
        });
        // A proof older than newly-raised debt cannot discharge it, so proofs
        // are load-bearing only while they contribute to an obligation or
        // barrier that is currently open. Drop everything else instead of
        // rewriting an ever-growing history on every acknowledgement.
        self.proved.retain(|(proof_generation, proof)| {
            self.entries.values().any(|entry| {
                entry.is_open()
                    && *proof_generation >= entry.generation
                    && domains_may_join(proof, &entry.domain)
            }) || self.barriers.values().any(|barrier| {
                *proof_generation >= barrier.generation && domains_may_join(proof, &barrier.domain)
            })
        });
    }

    /// Record a walk that stopped at a barrier.
    ///
    /// Idempotent per key: hitting the same wall on every attach must not
    /// accumulate incidents, and must not reset an operator's decision about
    /// it.
    pub fn record_barrier(&mut self, incident: BarrierIncident) {
        match self.barriers.get_mut(&incident.key) {
            Some(existing) => {
                existing.generation = incident.generation;
                existing.evidence = incident.evidence;
                existing.resume_from = incident.resume_from;
            }
            None => {
                self.barriers.insert(incident.key.clone(), incident);
            }
        }
    }

    /// Obligations eligible for an automatic repair attempt.
    ///
    /// Unresolved, not waived, not operator-blocked, and carrying a repair
    /// descriptor. A barrier has no descriptor and is correctly excluded: no
    /// checkpoint advanced past it, so it is blocked progress rather than debt,
    /// and its only terminal state is an operator waiver.
    pub fn repairable(&self) -> impl Iterator<Item = &LedgerEntry> {
        self.entries.values().filter(|entry| {
            entry.is_open()
                && entry.target.is_some()
                && matches!(entry.policy, PolicyStatus::Retrying { .. })
        })
    }

    /// The lineage root `key` belongs to.
    ///
    /// Budgets accrue at the root, not per key, or an account evades every
    /// budget by re-minting an equivalent obligation under a fresh key each
    /// pass. Walks parent links with a bound, so a cycle introduced by a buggy
    /// replacement cannot hang the writer.
    #[must_use]
    pub fn lineage_root(&self, key: &ObligationKey) -> ObligationKey {
        let mut current = key.clone();
        for _ in 0..LINEAGE_DEPTH_CAP {
            match self.entries.get(&current).and_then(|e| e.parent.clone()) {
                Some(parent) => current = parent,
                None => return current,
            }
        }
        current
    }

    /// Record one COMPLETED repair attempt against `key`'s lineage root.
    ///
    /// Completed, never merely started: a crash after provider work but before
    /// the acknowledgement must cost a repeated attempt, not a consumed budget,
    /// so nothing durable is written before an outcome comes back.
    ///
    /// Returns whether the budget expired on this attempt. An expired budget
    /// yields `OperatorBlocked` - automatic work stopped, still visible, still
    /// blocking, still manually retryable. It NEVER yields `Waived` or
    /// `Discharged`: a counter running out is evidence that retrying is not
    /// working, not a decision about what loss is acceptable.
    pub fn record_attempt(&mut self, key: &ObligationKey, budget: u32) -> bool {
        let root = self.lineage_root(key);
        let Some(entry) = self.entries.get_mut(&root) else {
            return false;
        };
        let PolicyStatus::Retrying { attempts } = &mut entry.policy else {
            return false;
        };
        *attempts = attempts.saturating_add(1);
        if *attempts >= budget {
            entry.policy = PolicyStatus::OperatorBlocked;
            return true;
        }
        false
    }

    /// Discharge `key` on proof that a repair recovered it and the consumer
    /// durably accepted the resulting notification.
    ///
    /// Conditional on generation: a result computed against an older view of
    /// the obligation must not close a version of it that has since been
    /// re-raised. Serialization through the single writer orders the messages;
    /// it does not by itself reject stale intent, which is what this check is
    /// for.
    ///
    /// Returns whether anything changed.
    pub fn discharge_repaired(
        &mut self,
        key: &ObligationKey,
        generation: u64,
        evidence: DischargeEvidence,
    ) -> bool {
        let Some(entry) = self.entries.get_mut(key) else {
            return false;
        };
        if entry.generation > generation || !entry.is_open() {
            return false;
        }
        entry.proof = ProofStatus::Discharged { evidence };
        true
    }

    /// Atomically replace `key` with `children`, recording `proved` as covered.
    ///
    /// A replacement is a SWAP, never an append. Appending children while the
    /// parent stays open double-counts the extent and replays the parent
    /// forever.
    ///
    /// Conservation is the safety property: nothing may vanish from the parent
    /// merely because no child names it. Where the extent is expressible in the
    /// lattice the engine verifies that `proved` plus the children's domains
    /// cover the parent; where it is opaque it cannot, so the account's
    /// assertion is recorded rather than the engine pretending it verified one.
    ///
    /// Children inherit the lineage root, so the retry budget follows the
    /// unresolved lineage rather than resetting per generated key.
    ///
    /// Returns `Err` with a reason when the replacement is refused.
    pub fn replace_obligation(
        &mut self,
        key: &ObligationKey,
        proved: &[CoverageDomain],
        children: &[InventoryObligation],
        generation: u64,
        now: i64,
    ) -> Result<ReplacementProgress, ReplacementRefusal> {
        let Some(parent) = self.entries.get(key).cloned() else {
            return Err(ReplacementRefusal::UnknownObligation);
        };
        if parent.generation > generation || !parent.is_open() {
            return Err(ReplacementRefusal::StaleGeneration);
        }
        // A child colliding with an obligation from another lineage would give
        // one key two roots and let a budget reset by choosing the convenient
        // one. Refused as a contract violation rather than resolved silently.
        let root = self.lineage_root(key);
        for child in children {
            if let Some(existing) = self.entries.get(child.key())
                && self.lineage_root(child.key()) != root
                && existing.is_open()
            {
                return Err(ReplacementRefusal::ChildCollidesWithForeignLineage);
            }
        }

        let progress = self.classify_progress(&parent, proved, children);

        for domain in proved {
            self.record_proof(domain.clone(), generation);
        }
        for child in children {
            if child.is_barrier() {
                continue;
            }
            self.upsert(child, &parent.domain, generation, now);
            if let Some(entry) = self.entries.get_mut(child.key()) {
                entry.parent = Some(root.clone());
            }
        }
        if let Some(entry) = self.entries.get_mut(key) {
            entry.proof = ProofStatus::Discharged {
                evidence: DischargeEvidence::ReplacedByChildren {
                    children: children.iter().map(|c| c.key().clone()).collect(),
                },
            };
        }
        Ok(progress)
    }

    /// Whether a replacement actually improved the situation.
    ///
    /// Extent equality alone is the wrong test in both directions. It REJECTS
    /// real progress: turning one opaque region into three stable object
    /// obligations leaves the unresolved extent unchanged while making every
    /// piece independently retryable and dischargeable. And it ACCEPTS fake
    /// progress: a provider can repartition a region into children whose union
    /// equals the parent, with fresh keys and rotated tokens, forever.
    ///
    /// So progress is judged across several dimensions, and token rotation or
    /// error reclassification alone is not one of them.
    fn classify_progress(
        &self,
        parent: &LedgerEntry,
        proved: &[CoverageDomain],
        children: &[InventoryObligation],
    ) -> ReplacementProgress {
        if !proved.is_empty() {
            return ReplacementProgress::Progressed;
        }
        // An opaque region becoming addressable objects is progress even at
        // identical extent.
        let parent_is_region = matches!(parent.target, Some(InventoryRepairTarget::Region { .. }));
        let children_are_objects = !children.is_empty()
            && children
                .iter()
                .all(|c| matches!(c, InventoryObligation::Object { .. }));
        if parent_is_region && children_are_objects {
            return ReplacementProgress::Progressed;
        }
        // A barrier becoming durably replayable is progress: repair authority
        // strictly improved.
        if children.iter().any(|child| {
            matches!(
                child,
                InventoryObligation::Region {
                    recovery: bifrost_types::RegionRecovery::DurableReplay { .. },
                    ..
                }
            )
        }) && parent.target.is_none()
        {
            return ReplacementProgress::Progressed;
        }
        ReplacementProgress::Stalled
    }

    /// Operator action: accept the loss at `key`.
    ///
    /// Targets ONE occurrence. A waiver keyed on a failure label would
    /// authorize every future failure of that class - Graph's
    /// "scope:unidentifiable-value" describes a class, not a region - and
    /// authorizing unknown future loss is a different and much larger decision
    /// than accepting a loss you can see.
    ///
    /// Returns whether anything matched.
    pub fn waive(&mut self, key: &ObligationKey, by: String, at_unix_seconds: i64) -> bool {
        let policy = PolicyStatus::Waived {
            by,
            at_unix_seconds,
        };
        let mut waived = false;
        if let Some(entry) = self.entries.get_mut(key) {
            // Policy only. `proof` stays `Unresolved` forever: nothing was
            // proved, somebody decided to live without it.
            entry.policy = policy.clone();
            waived = true;
        }
        if let Some(barrier) = self.barriers.get_mut(key) {
            barrier.policy = policy;
            waived = true;
        }
        waived
    }

    /// Operator action: stop automatic retries without accepting the loss.
    pub fn block(&mut self, key: &ObligationKey) -> bool {
        let mut blocked = false;
        if let Some(entry) = self.entries.get_mut(key) {
            entry.policy = PolicyStatus::OperatorBlocked;
            blocked = true;
        }
        if let Some(barrier) = self.barriers.get_mut(key) {
            barrier.policy = PolicyStatus::OperatorBlocked;
            blocked = true;
        }
        blocked
    }
}

fn domains_may_join(proof: &CoverageDomain, target: &CoverageDomain) -> bool {
    if proof.scope != target.scope {
        return false;
    }
    match (&proof.coordinate, &target.coordinate) {
        (CoverageCoordinate::Full, _) => true,
        (CoverageCoordinate::TimeRange { .. }, CoverageCoordinate::TimeRange { .. }) => true,
        (
            CoverageCoordinate::UidRange {
                uid_validity: a, ..
            },
            CoverageCoordinate::UidRange {
                uid_validity: b, ..
            },
        ) => a == b,
        (CoverageCoordinate::PageRange { .. }, CoverageCoordinate::PageRange { .. }) => {
            proof.snapshot.same_snapshot_as(&target.snapshot)
        }
        (CoverageCoordinate::ProviderRegion { .. }, CoverageCoordinate::ProviderRegion { .. }) => {
            proof.covers(target)
        }
        _ => false,
    }
}

fn proof_union_covers(
    proofs: &[(u64, CoverageDomain)],
    minimum_generation: u64,
    target: &CoverageDomain,
) -> bool {
    if proofs
        .iter()
        .any(|(generation, proof)| *generation >= minimum_generation && proof.covers(target))
    {
        return true;
    }

    match &target.coordinate {
        CoverageCoordinate::TimeRange {
            from_unix_seconds,
            to_unix_seconds,
        } => interval_union_covers(
            proofs.iter().filter_map(|(generation, proof)| {
                if *generation < minimum_generation || !domains_may_join(proof, target) {
                    return None;
                }
                match proof.coordinate {
                    CoverageCoordinate::TimeRange {
                        from_unix_seconds,
                        to_unix_seconds,
                    } => Some((from_unix_seconds, to_unix_seconds)),
                    _ => None,
                }
            }),
            *from_unix_seconds,
            *to_unix_seconds,
        ),
        CoverageCoordinate::UidRange { from, to, .. } => finite_union_covers(
            proofs.iter().filter_map(|(generation, proof)| {
                if *generation < minimum_generation || !domains_may_join(proof, target) {
                    return None;
                }
                match proof.coordinate {
                    CoverageCoordinate::UidRange { from, to, .. } => Some((from, to)),
                    _ => None,
                }
            }),
            *from,
            *to,
        ),
        CoverageCoordinate::PageRange { from, to } => finite_union_covers(
            proofs.iter().filter_map(|(generation, proof)| {
                if *generation < minimum_generation || !domains_may_join(proof, target) {
                    return None;
                }
                match proof.coordinate {
                    CoverageCoordinate::PageRange { from, to } => {
                        Some((u64::from(from), u64::from(to)))
                    }
                    _ => None,
                }
            }),
            u64::from(*from),
            u64::from(*to),
        ),
        CoverageCoordinate::Full | CoverageCoordinate::ProviderRegion { .. } => false,
        _ => false,
    }
}

fn finite_union_covers(
    intervals: impl Iterator<Item = (u64, u64)>,
    target_from: u64,
    target_to: u64,
) -> bool {
    let mut intervals: Vec<_> = intervals.collect();
    intervals.sort_unstable();
    let mut reached = target_from;
    for (from, to) in intervals {
        if to <= reached || from > reached {
            continue;
        }
        reached = reached.max(to);
        if reached >= target_to {
            return true;
        }
    }
    false
}

fn interval_union_covers(
    intervals: impl Iterator<Item = (Option<i64>, Option<i64>)>,
    target_from: Option<i64>,
    target_to: Option<i64>,
) -> bool {
    let encode_lower = |value: Option<i64>| value.unwrap_or(i64::MIN);
    let encode_upper = |value: Option<i64>| value.unwrap_or(i64::MAX);
    let intervals = intervals.map(|(from, to)| {
        let from = u64::from_be_bytes(encode_lower(from).to_be_bytes()) ^ (1_u64 << 63);
        let to = u64::from_be_bytes(encode_upper(to).to_be_bytes()) ^ (1_u64 << 63);
        (from, to)
    });
    let from = u64::from_be_bytes(encode_lower(target_from).to_be_bytes()) ^ (1_u64 << 63);
    let to = u64::from_be_bytes(encode_upper(target_to).to_be_bytes()) ^ (1_u64 << 63);
    finite_union_covers(intervals, from, to)
}

#[cfg(test)]
mod tests {
    use super::{
        BarrierIncident, DebtLedger, DischargeEvidence, PolicyStatus, ProofStatus,
        ReplacementProgress, ReplacementRefusal,
    };
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, Cause, CoverageCoordinate, CoverageDomain,
        CursorScope, DiagnosticText, InventoryCoverageReport, InventoryObligation, ObjectId,
        ObjectType, ObligationKey, RegionRecovery, RequestCause, RequestErrorKind,
        SnapshotIdentity,
    };

    fn scope() -> CursorScope {
        CursorScope::Type(ObjectType::Email)
    }

    fn error() -> bifrost_types::AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("unrepresentable"),
            }),
        )
        .try_build()
        .expect("valid account error classification")
    }

    fn object(key: &str) -> InventoryObligation {
        InventoryObligation::Object {
            key: ObligationKey(key.as_bytes().to_vec()),
            id: ObjectId(key.to_string()),
            error: error(),
            repair: Vec::new(),
        }
    }

    fn time_domain(from: i64, to: i64) -> CoverageDomain {
        CoverageDomain {
            scope: scope(),
            coordinate: CoverageCoordinate::TimeRange {
                from_unix_seconds: Some(from),
                to_unix_seconds: Some(to),
            },
            snapshot: SnapshotIdentity::unstable(),
        }
    }

    #[test]
    fn an_obligation_blocks_completion_until_something_resolves_it() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![object("a")]),
            1,
            100,
        );
        assert!(!ledger.completion_permitted(&scope()));
        assert_eq!(ledger.open_debt().count(), 1);
    }

    /// A later walk over covering ground closes the gap. Without this the
    /// ledger becomes false in the other direction: asserting an outstanding
    /// gap after accepting proof it no longer exists, and blocking the sentinel
    /// forever for no cause.
    #[test]
    fn a_covering_walk_discharges_earlier_debt() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("a")]),
            4,
            100,
        );
        ledger.ingest(
            &InventoryCoverageReport {
                domain: time_domain(0, 180),
                outcome: bifrost_types::CoverageOutcome::Complete,
            },
            5,
            200,
        );

        let entry = ledger
            .entry(&ObligationKey(b"a".to_vec()))
            .expect("entry retained for audit");
        assert!(matches!(entry.proof, ProofStatus::Discharged { .. }));
        assert!(ledger.completion_permitted(&scope()));
    }

    /// The narrower window does not cover the debt, so it must not discharge
    /// it. Getting this wrong is silent loss with a proof record attached.
    #[test]
    fn a_non_covering_complete_walk_discharges_nothing() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("a")]),
            4,
            100,
        );
        ledger.ingest(
            &InventoryCoverageReport {
                domain: time_domain(7, 60),
                outcome: bifrost_types::CoverageOutcome::Complete,
            },
            5,
            200,
        );

        assert!(
            ledger
                .entry(&ObligationKey(b"a".to_vec()))
                .expect("entry")
                .is_open()
        );
        assert!(!ledger.completion_permitted(&scope()));
    }

    #[test]
    fn adjacent_complete_walks_discharge_debt_only_by_their_union() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("a")]),
            4,
            100,
        );
        ledger.ingest(
            &InventoryCoverageReport::complete(time_domain(7, 60)),
            5,
            200,
        );
        assert!(
            ledger
                .entry(&ObligationKey(b"a".to_vec()))
                .expect("entry")
                .is_open()
        );

        ledger.ingest(
            &InventoryCoverageReport::complete(time_domain(60, 180)),
            6,
            300,
        );
        assert!(matches!(
            ledger
                .entry(&ObligationKey(b"a".to_vec()))
                .expect("entry")
                .proof,
            ProofStatus::Discharged { .. }
        ));
    }

    /// A stale report must not reverse newer proof.
    #[test]
    fn an_older_generation_cannot_discharge_newer_debt() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![object("a")]),
            9,
            100,
        );
        ledger.ingest(
            &InventoryCoverageReport {
                domain: CoverageDomain::full(scope()),
                outcome: bifrost_types::CoverageOutcome::Complete,
            },
            3,
            200,
        );
        assert!(
            ledger
                .entry(&ObligationKey(b"a".to_vec()))
                .expect("entry")
                .is_open()
        );
    }

    /// The load-bearing separation: a waiver is accepted loss, not proof. It
    /// unblocks the sentinel, and the record still says nothing was ever
    /// proved - otherwise an audit cannot tell coverage from resignation.
    #[test]
    fn a_waiver_unblocks_completion_without_claiming_proof() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![object("a")]),
            1,
            100,
        );
        assert!(ledger.waive(&ObligationKey(b"a".to_vec()), "operator".into(), 500));

        let entry = ledger.entry(&ObligationKey(b"a".to_vec())).expect("entry");
        assert_eq!(
            entry.proof,
            ProofStatus::Unresolved,
            "a waiver proves nothing"
        );
        assert!(entry.policy.is_waived());
        assert!(!entry.blocks_completion());
        assert!(ledger.completion_permitted(&scope()));
    }

    /// Rediscovery reopens proof but must not touch policy: an operator's
    /// decision stands until the operator revokes it.
    #[test]
    fn rediscovery_reopens_proof_and_preserves_history() {
        let mut ledger = DebtLedger::new();
        let report =
            InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![object("a")]);
        ledger.ingest(&report, 1, 100);
        ledger.block(&ObligationKey(b"a".to_vec()));
        ledger.ingest(&report, 2, 900);

        let entry = ledger.entry(&ObligationKey(b"a".to_vec())).expect("entry");
        assert_eq!(
            entry.first_seen_unix_seconds, 100,
            "re-raising must not reset history, or no budget is ever reachable"
        );
        assert_eq!(entry.policy, PolicyStatus::OperatorBlocked);
        assert!(entry.is_open());
    }

    /// A barrier is blocked progress, not debt behind an advanced cursor - so
    /// it never enters the entry table, but it does block completion and it
    /// does survive as something an operator can act on.
    #[test]
    fn a_barrier_blocks_completion_without_becoming_ledger_debt() {
        let mut ledger = DebtLedger::new();
        let barrier = InventoryObligation::Region {
            key: ObligationKey(b"page-7".to_vec()),
            failure_label: "unidentifiable-value".into(),
            error: error(),
            recovery: RegionRecovery::barrier(),
        };
        ledger.ingest(
            &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![barrier]),
            1,
            100,
        );
        assert_eq!(
            ledger.entries().count(),
            0,
            "no checkpoint advanced past it, so there is no debt to hang off one"
        );

        ledger.record_barrier(BarrierIncident {
            key: ObligationKey(b"page-7".to_vec()),
            domain: CoverageDomain::full(scope()),
            generation: 1,
            failure_label: "unidentifiable-value".into(),
            evidence: error(),
            policy: PolicyStatus::Retrying { attempts: 0 },
            resume_from: None,
        });
        assert!(!ledger.completion_permitted(&scope()));
        assert!(!ledger.barrier_waived(&ObligationKey(b"page-7".to_vec())));

        ledger.waive(&ObligationKey(b"page-7".to_vec()), "operator".into(), 500);
        assert!(ledger.barrier_waived(&ObligationKey(b"page-7".to_vec())));
        assert!(
            ledger.completion_permitted(&scope()),
            "a waived barrier releases the scope; that is the only escape hatch it has"
        );
    }

    #[test]
    fn a_cumulative_report_keeps_an_already_crossed_barrier_waived() {
        let mut ledger = DebtLedger::new();
        let key = ObligationKey(b"page-7".to_vec());
        let report = InventoryCoverageReport::degraded(
            CoverageDomain::full(scope()),
            vec![InventoryObligation::Region {
                key: key.clone(),
                failure_label: "unidentifiable-value".into(),
                error: error(),
                recovery: RegionRecovery::barrier(),
            }],
        );
        ledger.record_barrier(BarrierIncident {
            key: key.clone(),
            domain: report.domain.clone(),
            generation: 1,
            failure_label: "unidentifiable-value".into(),
            evidence: error(),
            policy: PolicyStatus::Retrying { attempts: 0 },
            resume_from: None,
        });
        assert!(ledger.waive(&key, "operator".into(), 500));

        assert!(ledger.cross_waived_barriers(&report, 2, 600));
        assert!(
            ledger.cross_waived_barriers(&report, 2, 600),
            "a cumulative Done report must not recreate the barrier crossed by its batch"
        );
        assert!(
            ledger
                .entry(&key)
                .expect("accepted-loss entry")
                .policy
                .is_waived()
        );
    }

    #[test]
    fn an_operator_can_block_a_barrier_without_accepting_its_loss() {
        let mut ledger = DebtLedger::new();
        let key = ObligationKey(b"page-7".to_vec());
        ledger.record_barrier(BarrierIncident {
            key: key.clone(),
            domain: CoverageDomain::full(scope()),
            generation: 1,
            failure_label: "unidentifiable-value".into(),
            evidence: error(),
            policy: PolicyStatus::Retrying { attempts: 0 },
            resume_from: None,
        });

        assert!(ledger.block(&key));
        assert_eq!(
            ledger.barriers().next().expect("barrier").policy,
            PolicyStatus::OperatorBlocked
        );
        assert!(!ledger.completion_permitted(&scope()));
    }

    /// The rescan park is keyed on `OperatorBlocked` alone. A retrying or a
    /// waived barrier must NOT park the scope: retrying is the ordinary state
    /// a barrier is recorded in, so parking on it would stop every
    /// barrier-stopped scope from ever re-walking, and a waived one is
    /// precisely the barrier the walk is now allowed to cross.
    #[test]
    fn only_an_operator_block_parks_a_scopes_rescan() {
        let mut ledger = DebtLedger::new();
        let key = ObligationKey(b"page-7".to_vec());
        ledger.record_barrier(BarrierIncident {
            key: key.clone(),
            domain: CoverageDomain::full(scope()),
            generation: 1,
            failure_label: "unidentifiable-value".into(),
            evidence: error(),
            policy: PolicyStatus::Retrying { attempts: 0 },
            resume_from: None,
        });
        assert!(
            !ledger.scope_has_blocked_barrier(&scope()),
            "a retrying barrier must keep the rescan running"
        );

        assert!(ledger.waive(&key, "operator".into(), 500));
        assert!(
            !ledger.scope_has_blocked_barrier(&scope()),
            "a waived barrier is crossable, not a park"
        );

        assert!(ledger.block(&key));
        assert!(ledger.scope_has_blocked_barrier(&scope()));

        let other = bifrost_types::CursorScope::Type(bifrost_types::ObjectType::CalendarEvent);
        assert!(
            !ledger.scope_has_blocked_barrier(&other),
            "one scope's block must not park a sibling scope"
        );
    }

    fn region(key: &str, recovery: RegionRecovery) -> InventoryObligation {
        InventoryObligation::Region {
            key: ObligationKey(key.as_bytes().to_vec()),
            failure_label: "truncated".into(),
            error: error(),
            recovery,
        }
    }

    fn replayable(key: &str) -> InventoryObligation {
        region(
            key,
            RegionRecovery::DurableReplay {
                token: b"tok".to_vec(),
            },
        )
    }

    fn ingest_one(ledger: &mut DebtLedger, obligation: InventoryObligation, generation: u64) {
        ledger.ingest(
            &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![obligation]),
            generation,
            100,
        );
    }

    /// A barrier has no repair descriptor, so nothing may try to repair it. Its
    /// only terminal state is an operator waiver.
    #[test]
    fn only_obligations_with_a_repair_descriptor_are_repairable() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, object("a"), 1);
        ingest_one(&mut ledger, replayable("r"), 1);
        ingest_one(&mut ledger, region("b", RegionRecovery::barrier()), 1);
        assert_eq!(ledger.repairable().count(), 2);
    }

    /// A waived or blocked obligation is not retried automatically.
    #[test]
    fn waived_and_blocked_obligations_are_not_repairable() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, object("a"), 1);
        ingest_one(&mut ledger, object("b"), 1);
        ledger.waive(&ObligationKey(b"a".to_vec()), "operator".into(), 1);
        ledger.block(&ObligationKey(b"b".to_vec()));
        assert_eq!(ledger.repairable().count(), 0);
    }

    /// A spent budget stops automatic work and NOTHING else. It must never
    /// reach Waived or Discharged: a counter running out is evidence that
    /// retrying is not helping, not a decision about acceptable loss.
    #[test]
    fn an_expired_budget_blocks_rather_than_abandons() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, object("a"), 1);
        let key = ObligationKey(b"a".to_vec());

        assert!(!ledger.record_attempt(&key, 3));
        assert!(!ledger.record_attempt(&key, 3));
        assert!(
            ledger.record_attempt(&key, 3),
            "the third attempt exhausts it"
        );

        let entry = ledger.entry(&key).expect("entry");
        assert_eq!(entry.policy, PolicyStatus::OperatorBlocked);
        assert!(entry.is_open(), "a spent budget proves nothing");
        assert!(
            entry.blocks_completion(),
            "blocked is not waived - it must still hold the sentinel"
        );
    }

    /// The budget follows the LINEAGE. An account that answers every pass by
    /// replacing an obligation with an equivalent one under a fresh key would
    /// otherwise never exhaust a budget.
    #[test]
    fn attempts_accrue_at_the_lineage_root() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, replayable("parent"), 1);
        let parent = ObligationKey(b"parent".to_vec());
        ledger.record_attempt(&parent, 10);

        ledger
            .replace_obligation(&parent, &[], &[replayable("child")], 1, 200)
            .expect("replacement accepted");
        let child = ObligationKey(b"child".to_vec());
        assert_eq!(ledger.lineage_root(&child), parent);

        // An attempt charged against the child lands on the root's counter.
        ledger.record_attempt(&child, 10);
        let PolicyStatus::Retrying { attempts } = ledger.entry(&parent).expect("root entry").policy
        else {
            panic!("root should still be retrying");
        };
        assert_eq!(attempts, 2, "both attempts must accrue to the one root");
    }

    /// Replacement is a SWAP. Leaving the parent open alongside its children
    /// double-counts the extent and replays the parent forever.
    #[test]
    fn replacement_closes_the_parent() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, replayable("parent"), 1);
        let parent = ObligationKey(b"parent".to_vec());

        ledger
            .replace_obligation(&parent, &[], &[object("child")], 1, 200)
            .expect("replacement accepted");

        assert!(!ledger.entry(&parent).expect("parent retained").is_open());
        assert!(
            ledger
                .entry(&ObligationKey(b"child".to_vec()))
                .expect("child")
                .is_open()
        );
        assert_eq!(ledger.open_debt().count(), 1);
    }

    /// Turning one opaque region into addressable objects is real progress even
    /// though the unresolved extent is unchanged - each piece is now
    /// independently retryable. Extent equality alone would call this a stall.
    #[test]
    fn a_region_becoming_objects_counts_as_progress() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, replayable("parent"), 1);
        let progress = ledger
            .replace_obligation(
                &ObligationKey(b"parent".to_vec()),
                &[],
                &[object("c1"), object("c2")],
                1,
                200,
            )
            .expect("replacement accepted");
        assert_eq!(progress, ReplacementProgress::Progressed);
    }

    /// Repartitioning a region into equally opaque regions, recovering nothing
    /// and proving nothing, is not progress. Left uncharged it is an infinite
    /// loop with fresh keys each pass.
    #[test]
    fn repartitioning_into_equally_opaque_regions_is_a_stall() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, replayable("parent"), 1);
        let progress = ledger
            .replace_obligation(
                &ObligationKey(b"parent".to_vec()),
                &[],
                &[replayable("r1"), replayable("r2")],
                1,
                200,
            )
            .expect("replacement accepted");
        assert_eq!(progress, ReplacementProgress::Stalled);
    }

    /// A result computed against an older view of an obligation must not close
    /// a version of it that has since been re-raised. Serializing through one
    /// writer orders the messages; it does not reject stale intent.
    #[test]
    fn a_stale_repair_result_cannot_discharge_a_reopened_obligation() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, object("a"), 1);
        // Rediscovered by a later walk: the obligation moves to generation 5.
        ingest_one(&mut ledger, object("a"), 5);

        let discharged = ledger.discharge_repaired(
            &ObligationKey(b"a".to_vec()),
            1,
            DischargeEvidence::RepairedAndPublished {
                attempt: bifrost_types::RepairAttemptId(1),
            },
        );
        assert!(
            !discharged,
            "a generation-1 result must not close generation 5"
        );
        assert!(
            ledger
                .entry(&ObligationKey(b"a".to_vec()))
                .expect("entry")
                .is_open()
        );
    }

    /// A child already open under a different root would give one obligation
    /// two lineages, and a budget could then be reset by picking whichever root
    /// is convenient.
    #[test]
    fn a_child_colliding_with_a_foreign_lineage_is_refused() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, replayable("parent-a"), 1);
        ingest_one(&mut ledger, replayable("parent-b"), 1);

        let refusal = ledger.replace_obligation(
            &ObligationKey(b"parent-a".to_vec()),
            &[],
            &[replayable("parent-b")],
            1,
            200,
        );
        assert_eq!(
            refusal,
            Err(ReplacementRefusal::ChildCollidesWithForeignLineage)
        );
    }

    /// Hitting the same wall every attach must not accumulate incidents or
    /// reset the operator's decision about it.
    #[test]
    fn re_recording_a_barrier_is_idempotent_and_keeps_policy() {
        let mut ledger = DebtLedger::new();
        let incident = || BarrierIncident {
            key: ObligationKey(b"page-7".to_vec()),
            domain: CoverageDomain::full(scope()),
            generation: 1,
            failure_label: "unidentifiable-value".into(),
            evidence: error(),
            policy: PolicyStatus::Retrying { attempts: 0 },
            resume_from: None,
        };
        ledger.record_barrier(incident());
        ledger.waive(&ObligationKey(b"page-7".to_vec()), "operator".into(), 500);
        ledger.record_barrier(incident());

        assert_eq!(ledger.barriers().count(), 1);
        assert!(
            ledger.barrier_waived(&ObligationKey(b"page-7".to_vec())),
            "re-hitting the wall must not silently revoke the waiver"
        );
    }
}
