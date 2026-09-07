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
//!
//! The two axes also draw the compaction line: only `Discharged` entries fold
//! into [`DischargeAudit`], because only `Discharged` is terminal. Every
//! `Unresolved` entry stays a live entry, WAIVED ONES INCLUDED - a waiver is a
//! policy decision that never makes the proof terminal. See [`DischargeAudit`]
//! for the full argument.

use std::collections::{BTreeMap, BTreeSet};

use bifrost_types::{
    AccountError, CoverageCoordinate, CoverageDomain, CursorScope, InventoryCoverageReport,
    InventoryObligation, InventoryRepairTarget, ObligationKey,
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

/// Entry count at which an ingest folds terminal history into the audit table.
///
/// A threshold rather than a per-discharge sweep, so the common ingest does no
/// extra work and a test-sized ledger keeps every entry verbatim. Compaction
/// itself is one linear pass, which is the cost `record_proof` already pays on
/// every accepted report, so crossing the threshold does not change the shape
/// of an ingest.
const COMPACTION_THRESHOLD: usize = 512;

/// What survives a compacted `Discharged` entry.
///
/// # Why only the proved side compacts
///
/// The line falls out of [`ProofStatus`], and the next person to touch this
/// will be tempted to move it, so the reasoning lives here rather than in a
/// commit message.
///
/// `Discharged` is TERMINAL. Something proved the coverage, and no path in this
/// module transitions a discharged entry into anything an operator can act on -
/// rediscovery does not mutate it, it raises the obligation afresh. So a count
/// plus an audit root loses nothing anybody could have used.
///
/// `Unresolved` is NOT terminal, even when waived. A waiver is a POLICY
/// decision: it stops the entry blocking completion and leaves the proof
/// `Unresolved` forever by design, because nothing was proved. A later walk
/// over covering ground can still discharge it, and an operator may still need
/// its `ObligationKey` to revoke the waiver or drive a manual repair.
/// Compacting a waived entry would destroy both.
///
/// That is also what satisfies the constraint this work was filed under -
/// preserve the proved-versus-waived distinction rather than flattening it -
/// BY CONSTRUCTION rather than by bookkeeping. Only the proved side compacts,
/// so the waived entries are exactly the ones left sitting in the table in
/// full.
///
/// # What the root can and cannot answer
///
/// The root is a DETECTION root. It answers "does this candidate history fold
/// to the same value the ledger recorded", which is what makes a restored,
/// replicated or externally-recorded discharge history checkable, and what
/// makes a discharge that quietly vanished - or a corrupted durable row -
/// detectable. It is emphatically NOT a proof of exact history: the root is a
/// commutative SUM of digests, and a sum of digests is not collision-resistant
/// the way a single digest is. Two different histories can in principle fold to
/// the same root, and nothing here defends against an adversary who can choose
/// obligation keys to make that happen. Accidental divergence and loss is the
/// threat model; forgery is not.
///
/// The audited history is also NARROWER than the word "history" suggests. Only
/// `(key, generation, evidence KIND)` enters the fold. A change to a repair
/// attempt id, a covering domain, a `ReplacedByChildren` child list or the text
/// of a `ProvedIrrelevant` reason is INVISIBLE to the root, at any hash
/// strength, because those payloads are diagnostic and legitimately vary
/// between revisions.
///
/// It deliberately cannot answer "was key K discharged" on its own, and that is
/// a decision rather than an oversight:
///
/// - The actionable state is open debt, and open debt is retained in full.
/// - An exact membership index means retaining the keys, which is the
///   unbounded growth this compaction exists to remove.
/// - A probabilistic index (a Bloom filter, say) errs by reporting DISCHARGED
///   for something that never was. That is precisely the silent-loss shape this
///   module exists to prevent, so a bounded-but-lying answer is worse than no
///   answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct DischargeAudit {
    /// How many entries were folded away for this scope.
    pub count: u64,
    /// Order-independent fold of every folded entry's fingerprint.
    ///
    /// Wrapping 256-bit ADDITION, not XOR: an obligation that is discharged,
    /// compacted, rediscovered, discharged and compacted again must count
    /// twice, and under XOR the second fold would silently cancel the first.
    /// Addition keeps the fold commutative - compaction order is not part of
    /// the contract - at the cost of the sum being weaker than its summands,
    /// which is why the guarantee above is detection rather than verification.
    pub root: [u8; 32],
    /// The newest generation folded away, so an audit can bound when the
    /// compacted history stops.
    pub latest_generation: u64,
}

impl DischargeAudit {
    /// Fold one discharged obligation in.
    ///
    /// Public so an audit holding a candidate history can rebuild a root and
    /// compare it against the one the ledger carries. The fingerprint is the
    /// stable part of the contract: key, generation, and which KIND of evidence
    /// closed it.
    pub fn fold(&mut self, key: &ObligationKey, generation: u64, evidence: &DischargeEvidence) {
        self.count = self.count.saturating_add(1);
        self.root = add_wrapping_256(self.root, discharge_fingerprint(key, generation, evidence));
        self.latest_generation = self.latest_generation.max(generation);
    }
}

/// Little-endian 256-bit wrapping addition.
///
/// Byte-wise with a carry rather than four `u64` limbs, because the durable
/// encoding is a byte array and a limb split would be one more place for an
/// endianness mistake to hide in a format that has to round-trip exactly.
fn add_wrapping_256(accumulator: [u8; 32], addend: [u8; 32]) -> [u8; 32] {
    let mut out = [0u8; 32];
    let mut carry = 0u16;
    for ((slot, left), right) in out.iter_mut().zip(accumulator).zip(addend) {
        let sum = u16::from(left) + u16::from(right) + carry;
        *slot = u8::try_from(sum & 0x00ff).expect("masked to one byte");
        carry = sum >> 8;
    }
    out
}

/// Fingerprint of one discharged obligation.
///
/// SHA-256 over an unambiguous framing. The predecessor of this function used a
/// 128-bit FNV-1a, and the additive fold made that genuinely weak rather than
/// merely finite: the histories `{"00", "05"}` and `{"01", "04"}` at the same
/// generation and evidence kind fold to the same count, generation and sum, out
/// of ordinary short keys nobody chose adversarially. `sha2` was already a
/// pinned workspace dependency, so the fix cost a line in `Cargo.toml`.
///
/// The framing is length-prefixed and domain-separated so no two distinct
/// obligations can serialize alike. Only the evidence DISCRIMINANT enters it -
/// see [`DischargeAudit`] for what that leaves unaudited.
#[must_use]
pub fn discharge_fingerprint(
    key: &ObligationKey,
    generation: u64,
    evidence: &DischargeEvidence,
) -> [u8; 32] {
    use sha2::Digest;

    let mut hasher = sha2::Sha256::new();
    hasher.update(b"bifrost.sync.debt-ledger.discharge.v1");
    hasher.update(u64::try_from(key.0.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(&key.0);
    hasher.update(generation.to_le_bytes());
    hasher.update([evidence_tag(evidence)]);
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// Which kind of proof closed an obligation.
///
/// Only the discriminant enters the fingerprint. The payloads are unbounded
/// (`ReplacedByChildren` carries a key list, `ProvedIrrelevant` a free string)
/// and their exact bytes are diagnostic rather than load-bearing, so hashing
/// them would make the root sensitive to text that legitimately changes between
/// revisions.
fn evidence_tag(evidence: &DischargeEvidence) -> u8 {
    match evidence {
        DischargeEvidence::CoveringWalk { .. } => 0,
        DischargeEvidence::RepairedAndPublished { .. } => 1,
        DischargeEvidence::ProvedIrrelevant { .. } => 2,
        DischargeEvidence::ReplacedByChildren { .. } => 3,
    }
}

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
    /// Terminal history that no longer has an entry, per scope.
    ///
    /// A `Vec` with a linear lookup rather than a map, because `CursorScope` is
    /// a `#[non_exhaustive]` public enum in `bifrost-types` with no `Ord`, and
    /// the count of scopes on one account is small enough that a scan is not
    /// the interesting cost. Order is deterministic: scopes are appended in the
    /// order compaction first encounters them, and compaction walks `entries`
    /// in key order.
    compacted: Vec<(CursorScope, DischargeAudit)>,
    /// Parent links of entries compaction folded away, kept only while the
    /// chain they start still reaches a retained entry.
    ///
    /// This is the ledger's answer to a dependency it could not previously SEE:
    /// a repair attempt in flight against key A charges its budget through
    /// `lineage_root(A)` when the result comes back, and nothing in the ledger
    /// names A, so no amount of pinning from A's siblings preserves it. Without
    /// this table, a covering proof that discharges A mid-flight lets compaction
    /// fold A, after which the deferred result resolves A's root to A itself,
    /// finds no entry, and charges NOTHING - the attempt vanishes from the
    /// budget silently, which is the un-capped retry loop budgets exist to
    /// close.
    ///
    /// Chosen over the alternative (retain the entry path of every pending
    /// attempt until it resolves) because retention needs a dispatch-time
    /// signal the writer never receives: `run_repair_pass` plans against a
    /// ledger snapshot and only speaks to the writer again when results are
    /// already in hand, so a pin would have to be a new cross-module protocol
    /// with a release on every abnormal exit, and a missed release is a leak of
    /// exactly the entries compaction exists to reclaim. A parent link costs
    /// two keys and no coordination.
    ///
    /// IN-MEMORY ONLY, deliberately, and that is why nothing in
    /// `ledger_envelope` carries it. The dependency it protects is an attempt in
    /// flight in THIS process; a restart loses the executor that would deliver
    /// the result, so a tombstone restored from disk protects nothing and would
    /// just be durable state with no reader.
    ///
    /// A folded entry with NO parent gets no tombstone. Its budget identity is
    /// itself, and it is discharged, so the only counter a late result could
    /// reach sits on a terminal entry that `repairable` already refuses - the
    /// charge was never load-bearing.
    folded_lineage: BTreeMap<ObligationKey, ObligationKey>,
}

impl DebtLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild a ledger from durable parts.
    ///
    /// The one door into the private maps that does not go through `ingest`,
    /// and deliberately crate-private: it exists for
    /// [`crate::cursor::ledger_envelope::decode_ledger`], which is the only
    /// caller that legitimately holds a whole ledger's state without having
    /// folded a report to get it. Keeping it out of the public surface is what
    /// preserves "no protocol crate may construct ledger state" - a consumer
    /// restores a ledger by decoding bytes this engine wrote, never by
    /// asserting one.
    pub(crate) fn from_parts(
        entries: BTreeMap<ObligationKey, LedgerEntry>,
        barriers: BTreeMap<ObligationKey, BarrierIncident>,
        proved: Vec<(u64, CoverageDomain)>,
        compacted: Vec<(CursorScope, DischargeAudit)>,
    ) -> Self {
        Self {
            entries,
            barriers,
            proved,
            compacted,
            // Empty by construction: see the field's own note on why folded
            // parent links are process-local and never restored.
            folded_lineage: BTreeMap::new(),
        }
    }

    /// The per-scope audit table, in its durable order.
    ///
    /// Crate-private for the same reason `proved` is: it is a consequence of
    /// compaction, not a query. The codec needs it because dropping it on
    /// restart would silently reset an account's discharged count to zero and
    /// make the root unverifiable forever after.
    pub(crate) fn compacted(&self) -> &[(CursorScope, DischargeAudit)] {
        &self.compacted
    }

    /// The compacted terminal history for `scope`, if any has been folded.
    ///
    /// `None` and a zero-count audit mean the same thing to a reader; the
    /// distinction is only that nothing has ever compacted for that scope.
    #[must_use]
    pub fn discharge_audit(&self, scope: &CursorScope) -> Option<&DischargeAudit> {
        self.compacted
            .iter()
            .find(|(candidate, _)| candidate == scope)
            .map(|(_, audit)| audit)
    }

    /// The retained proofs, with the generation that proved each.
    ///
    /// Not public for the same reason the field is not: proof retention is an
    /// engine-internal consequence of `record_proof`, not a query. The codec
    /// needs it because a restart that forgets a retained proof re-opens debt
    /// a later partial walk can no longer discharge by union.
    pub(crate) fn proved(&self) -> &[(u64, CoverageDomain)] {
        &self.proved
    }

    /// Whether this ledger holds nothing worth persisting.
    ///
    /// The audit table counts. A ledger whose entries have all been compacted
    /// away still carries the only surviving record that they existed, and a
    /// caller that skips writing an "empty" ledger would erase exactly the
    /// history compaction was supposed to preserve.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.barriers.is_empty() && self.compacted.is_empty()
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
                // Same rule as `upsert`: a key becoming an entry again starts a
                // fresh lineage, so any folded parent link for it goes.
                self.folded_lineage.remove(&key);
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
        self.compact_if_large();
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
        self.compact_if_large();
    }

    /// Fold terminal history into the audit table once the entry map is large.
    ///
    /// Runs at the END of a fold, in the single writer every durable mutation
    /// goes through, and never mid-transition. That placement is what makes it
    /// safe against a concurrent transition rather than merely unlikely to race
    /// one: see [`Self::compact_discharged`] for why removing a terminal entry
    /// cannot change the outcome of anything in flight.
    fn compact_if_large(&mut self) {
        if self.entries.len() >= COMPACTION_THRESHOLD {
            self.compact_discharged();
        }
    }

    /// Fold every compactable `Discharged` entry into its scope's audit,
    /// returning how many entries were removed.
    ///
    /// Called automatically once the entry map crosses
    /// `COMPACTION_THRESHOLD`; exposed so an operator tool or a consumer that
    /// knows a burst of repair just closed can reclaim without waiting for one
    /// more report.
    ///
    /// # Why this cannot disturb work in flight
    ///
    /// Only `Discharged` entries are candidates, and every mutating path keyed
    /// on an entry already refuses a non-open one: `discharge_repaired` and
    /// `replace_obligation` both bail on `!is_open()`, and the foreign-lineage
    /// collision check only fires on an open entry. So for those, a removed
    /// terminal entry and a present terminal entry produce the same answer.
    ///
    /// The exception is the LINEAGE, and it is the one thing that made this
    /// dangerous. `replace_obligation` discharges the parent while leaving its
    /// policy `Retrying`, and children point at that discharged root; a budget
    /// charged against a child lands on the root's counter. Compacting a root
    /// out from under a live child would make `record_attempt` find nothing,
    /// silently un-cap the retry budget, and reopen the loop budgets exist to
    /// close. So a discharged entry that any surviving entry names as its
    /// parent is retained - terminal as proof, still load-bearing as structure.
    ///
    /// # Which guarantee the pin set implements
    ///
    /// The STRONGER of the two available, stated here because they are easy to
    /// confuse and the code must not be ambiguous about which it promises:
    ///
    /// **Every entry this ledger retains has its whole parent chain retained
    /// too.** That is the ancestor CLOSURE - each survivor's ancestors are
    /// pinned, and each newly pinned ancestor is then walked in turn, with a
    /// visited set to terminate.
    ///
    /// The weaker alternative - one bounded walk from each unresolved entry,
    /// pinning what it passes - preserves that entry's own `lineage_root`
    /// lookup and nothing more. It leaves a retained endpoint pointing at a
    /// removed ancestor, so a `record_attempt` charged against the ENDPOINT
    /// resolves to a key that is no longer here. The closure has no such edge.
    ///
    /// Two details the walk has to get exactly right, both of which reintroduce
    /// the original hole if fumbled:
    ///
    /// - `lineage_root` follows at most `LINEAGE_DEPTH_CAP` edges and returns
    ///   the key reached AFTER that last edge, even when it has a further
    ///   parent. So the pin walk must retain distances 1 THROUGH the cap
    ///   INCLUSIVE. A loop that pins the current node before advancing, and
    ///   stops after the cap's worth of iterations, misses the endpoint - the
    ///   one key the resolver actually returns.
    /// - Both use [`Self::lineage_ancestors`], one primitive, so they cannot
    ///   disagree about where a chain ends.
    ///
    /// `DischargeEvidence::ReplacedByChildren` also names entries, and those are
    /// deliberately NOT pinned. The list is EVIDENCE: nothing in the engine
    /// dereferences it for a decision (the codec encodes and decodes it, and no
    /// other reader exists), so a child key in it is a record of what happened,
    /// not a link something will follow. If a reader ever does resolve those
    /// keys, they become operational dependencies and belong in the pin set.
    ///
    /// # What a later rediscovery costs
    ///
    /// A compacted key rediscovered by a later walk is raised as a NEW entry,
    /// with `first_seen_unix_seconds` set to now and the retry budget at zero -
    /// where an uncompacted discharged entry would have kept both. That is a
    /// real behaviour change and it is accepted: the entry was proved covered
    /// before it was folded, so a re-raise after proof is a fresh gap rather
    /// than a continuing one, and charging it the old budget would be charging
    /// it for failures that provably stopped.
    pub fn compact_discharged(&mut self) -> usize {
        let mut candidates: Vec<ObligationKey> = Vec::new();
        for entry in self.entries.values() {
            if !entry.is_open() {
                candidates.push(entry.key.clone());
            }
        }
        if candidates.is_empty() {
            return 0;
        }
        // The ancestor closure of the entries that will SURVIVE. A discharged
        // ancestor named only from within the folding set is compactable,
        // because everything that named it goes too.
        let candidate_set: BTreeSet<&ObligationKey> = candidates.iter().collect();
        let mut pinned: BTreeSet<ObligationKey> = BTreeSet::new();
        let mut frontier: Vec<ObligationKey> = self
            .entries
            .values()
            .filter(|entry| !candidate_set.contains(&entry.key))
            .map(|entry| entry.key.clone())
            .collect();
        while let Some(key) = frontier.pop() {
            for ancestor in self.lineage_ancestors(&key) {
                // Already pinned means already walked from, so the rest of this
                // chain is covered. That is the visited set, and it is also
                // what makes a cycle terminate.
                if !pinned.insert(ancestor.clone()) {
                    break;
                }
                frontier.push(ancestor);
            }
        }

        let mut removed = 0;
        for key in &candidates {
            if pinned.contains(key) {
                continue;
            }
            let Some(entry) = self.entries.remove(key) else {
                continue;
            };
            let ProofStatus::Discharged { evidence } = &entry.proof else {
                // Unreachable: candidates are exactly the non-open entries.
                // Restored rather than dropped, because losing an entry here
                // would be silent debt loss.
                self.entries.insert(key.clone(), entry);
                continue;
            };
            self.audit_for(&entry.domain.scope)
                .fold(&entry.key, entry.generation, evidence);
            if let Some(parent) = entry.parent.clone() {
                // The budget identity of an attempt still in flight against
                // this key. See `folded_lineage`.
                self.folded_lineage.insert(entry.key.clone(), parent);
            }
            removed += 1;
        }
        self.prune_folded_lineage();
        removed
    }

    /// Drop folded parent links whose chain no longer reaches a retained entry.
    ///
    /// The bound on the table. A tombstone is only useful while the root it
    /// leads to is still here to be charged; once the whole lineage has folded
    /// there is no counter at the end of the walk and the link is dead weight.
    fn prune_folded_lineage(&mut self) {
        let folded: Vec<ObligationKey> = self.folded_lineage.keys().cloned().collect();
        for key in folded {
            let reaches_a_retained_entry = self
                .lineage_ancestors(&key)
                .iter()
                .any(|ancestor| self.entries.contains_key(ancestor));
            if !reaches_a_retained_entry {
                self.folded_lineage.remove(&key);
            }
        }
    }

    /// One step along the parent chain.
    ///
    /// The entry first, a folded parent link second. The fallback is what keeps
    /// a budget resolvable across compaction; it is consulted only when no
    /// entry exists, so a REDISCOVERED key resolves through its fresh entry
    /// (parent `None`, budget from zero) exactly as the compaction contract
    /// says it should, rather than through a stale link to its old lineage.
    fn parent_of(&self, key: &ObligationKey) -> Option<ObligationKey> {
        match self.entries.get(key) {
            Some(entry) => entry.parent.clone(),
            None => self.folded_lineage.get(key).cloned(),
        }
    }

    /// Every ancestor of `key`, nearest first: distances 1 through
    /// `LINEAGE_DEPTH_CAP` INCLUSIVE.
    ///
    /// The one traversal primitive. [`Self::lineage_root`] takes the last
    /// element of this, and compaction pins every element of it, so the
    /// resolver and the pin walk cannot disagree about where a chain ends -
    /// which is the whole reason the endpoint semantics are stated here rather
    /// than reimplemented at each caller.
    fn lineage_ancestors(&self, key: &ObligationKey) -> Vec<ObligationKey> {
        let mut chain = Vec::new();
        let mut current = key.clone();
        for _ in 0..LINEAGE_DEPTH_CAP {
            let Some(parent) = self.parent_of(&current) else {
                break;
            };
            chain.push(parent.clone());
            current = parent;
        }
        chain
    }

    fn audit_for(&mut self, scope: &CursorScope) -> &mut DischargeAudit {
        if let Some(index) = self
            .compacted
            .iter()
            .position(|(candidate, _)| candidate == scope)
        {
            return &mut self.compacted[index].1;
        }
        self.compacted
            .push((scope.clone(), DischargeAudit::default()));
        &mut self
            .compacted
            .last_mut()
            .expect("just pushed an audit entry")
            .1
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
                // A rediscovered key is a FRESH gap, with no parent and no
                // budget - so its old folded parent link must not outlive the
                // re-raise and quietly hand the new entry a lineage. `parent_of`
                // already prefers the entry; this keeps the table honest rather
                // than merely shadowed.
                self.folded_lineage.remove(&key);
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
    ///
    /// Bounded by `LINEAGE_DEPTH_CAP` EDGES: at the cap this returns the key
    /// reached after the last edge even when that key has a further parent.
    /// Compaction pins the same set this walks, through the same primitive, so
    /// the key returned here is always one the ledger still holds.
    #[must_use]
    pub fn lineage_root(&self, key: &ObligationKey) -> ObligationKey {
        self.lineage_ancestors(key)
            .pop()
            .unwrap_or_else(|| key.clone())
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
    ///
    /// `key` need not still BE an entry. A repair dispatched against an
    /// obligation that a covering proof discharged mid-flight has had its entry
    /// folded by the time the result arrives, and the attempt must still be
    /// charged against the lineage its surviving siblings share - so the root
    /// resolves through the folded parent link when no entry remains. See
    /// `folded_lineage`.
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

    /// The ruling's line, in one test. `Discharged` is terminal, so it folds
    /// into a count and a root; `Unresolved` is not terminal even when waived,
    /// so it stays a live entry with its key intact - a later covering walk can
    /// still discharge it and an operator may still need to act on it.
    #[test]
    fn compaction_folds_the_proved_side_and_leaves_every_waived_entry_whole() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, object("proved"), 1);
        ingest_one(&mut ledger, object("waived"), 1);
        ingest_one(&mut ledger, object("owed"), 1);
        assert!(ledger.waive(&ObligationKey(b"waived".to_vec()), "op".into(), 500));
        assert!(ledger.discharge_repaired(
            &ObligationKey(b"proved".to_vec()),
            1,
            DischargeEvidence::RepairedAndPublished {
                attempt: bifrost_types::RepairAttemptId(7),
            },
        ));

        assert_eq!(ledger.compact_discharged(), 1);
        assert!(
            ledger.entry(&ObligationKey(b"proved".to_vec())).is_none(),
            "a terminal entry does not survive as an entry"
        );

        let waived = ledger
            .entry(&ObligationKey(b"waived".to_vec()))
            .expect("a waived entry is never compacted");
        assert_eq!(waived.key, ObligationKey(b"waived".to_vec()));
        assert_eq!(
            waived.proof,
            ProofStatus::Unresolved,
            "a waiver proves nothing, so it is not terminal and cannot compact"
        );
        assert!(waived.policy.is_waived());
        assert!(
            ledger.entry(&ObligationKey(b"owed".to_vec())).is_some(),
            "open debt is untouched"
        );

        let audit = ledger.discharge_audit(&scope()).expect("audit recorded");
        assert_eq!(audit.count, 1);
        assert_eq!(audit.latest_generation, 1);
    }

    /// PRESERVATION CHECK, not a demonstration of a defect: replace
    /// `compact_discharged` with a no-op and this still passes, because the
    /// waived entry was never a fold candidate. It pins the rule that a waived
    /// entry keeps its key - which is what a later covering walk needs - and it
    /// earns its place on that basis alone. The tests that bite the compaction
    /// logic itself are the lineage ones below.
    #[test]
    fn a_covering_walk_still_discharges_a_waived_entry_after_compaction() {
        let mut ledger = DebtLedger::new();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("w")]),
            4,
            100,
        );
        assert!(ledger.waive(&ObligationKey(b"w".to_vec()), "op".into(), 500));
        ledger.compact_discharged();

        ledger.ingest(
            &InventoryCoverageReport::complete(time_domain(0, 180)),
            5,
            200,
        );
        assert!(matches!(
            ledger
                .entry(&ObligationKey(b"w".to_vec()))
                .expect("entry")
                .proof,
            ProofStatus::Discharged { .. }
        ));
    }

    /// PRESERVATION CHECK for the ONE-LEVEL case. `replace_obligation`
    /// discharges the parent but leaves its policy `Retrying`, and children
    /// charge their attempts against it, so folding that root away would
    /// silently un-cap the retry budget. Stub compaction out and this still
    /// passes - it fixes the shallowest arrangement rather than exercising the
    /// pin walk. `a_multi_level_lineage_survives_compaction_whole` and
    /// `the_lineage_endpoint_at_the_depth_cap_is_pinned_too` are the ones that
    /// fail against a pin set built from immediate parents.
    #[test]
    fn a_discharged_lineage_root_survives_while_a_child_is_open() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, replayable("parent"), 1);
        let parent = ObligationKey(b"parent".to_vec());
        ledger
            .replace_obligation(&parent, &[], &[replayable("child")], 1, 200)
            .expect("replacement accepted");

        assert_eq!(
            ledger.compact_discharged(),
            0,
            "the root is terminal as proof and load-bearing as structure"
        );
        assert!(ledger.entry(&parent).is_some());

        let child = ObligationKey(b"child".to_vec());
        assert!(ledger.record_attempt(&child, 1));
        assert_eq!(
            ledger.entry(&parent).expect("root").policy,
            PolicyStatus::OperatorBlocked,
            "the budget must still reach its cap through the retained root"
        );
    }

    /// Once the child closes too, nothing names the root and both fold.
    #[test]
    fn a_lineage_folds_entirely_once_no_open_entry_names_the_root() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, replayable("parent"), 1);
        ledger
            .replace_obligation(
                &ObligationKey(b"parent".to_vec()),
                &[],
                &[replayable("child")],
                1,
                200,
            )
            .expect("replacement accepted");
        assert!(ledger.discharge_repaired(
            &ObligationKey(b"child".to_vec()),
            1,
            DischargeEvidence::ProvedIrrelevant {
                detail: "not owed".into(),
            },
        ));

        assert_eq!(ledger.compact_discharged(), 2);
        assert_eq!(ledger.entries().count(), 0);
        assert_eq!(ledger.discharge_audit(&scope()).expect("audit").count, 2);
    }

    /// What the root PROMISES, and no more: fold a candidate history
    /// independently and the two agree regardless of order, and a history of
    /// the same SIZE over different keys is distinguished. Detection, not proof
    /// of exact history - a sum of digests can in principle collide, and the
    /// doc says so.
    #[test]
    fn audit_fold_is_order_independent_and_distinguishes_sample_histories() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, object("a"), 3);
        ingest_one(&mut ledger, object("b"), 3);
        let evidence = || DischargeEvidence::ProvedIrrelevant {
            detail: "not owed".into(),
        };
        assert!(ledger.discharge_repaired(&ObligationKey(b"a".to_vec()), 3, evidence()));
        assert!(ledger.discharge_repaired(&ObligationKey(b"b".to_vec()), 3, evidence()));
        assert_eq!(ledger.compact_discharged(), 2);
        let audit = *ledger.discharge_audit(&scope()).expect("audit");

        let mut rebuilt = super::DischargeAudit::default();
        rebuilt.fold(&ObligationKey(b"b".to_vec()), 3, &evidence());
        rebuilt.fold(&ObligationKey(b"a".to_vec()), 3, &evidence());
        assert_eq!(rebuilt, audit, "the fold is order-independent");

        let mut wrong = super::DischargeAudit::default();
        wrong.fold(&ObligationKey(b"a".to_vec()), 3, &evidence());
        wrong.fold(&ObligationKey(b"c".to_vec()), 3, &evidence());
        assert_eq!(wrong.count, audit.count, "same size, different history");
        assert_ne!(wrong.root, audit.root, "the root must see the difference");
    }

    /// The same key discharged, folded, rediscovered and folded again must
    /// count twice. Under an XOR fold the second would cancel the first and the
    /// root would claim the obligation was never discharged at all.
    #[test]
    fn folding_the_same_obligation_twice_does_not_cancel_it() {
        let evidence = DischargeEvidence::ProvedIrrelevant {
            detail: "not owed".into(),
        };
        let mut audit = super::DischargeAudit::default();
        audit.fold(&ObligationKey(b"a".to_vec()), 3, &evidence);
        let once = audit.root;
        audit.fold(&ObligationKey(b"a".to_vec()), 3, &evidence);
        assert_eq!(audit.count, 2);
        assert_ne!(audit.root, once);
        assert_ne!(
            audit.root, [0u8; 32],
            "an additive fold must not cancel a repeated obligation back to the empty root"
        );
    }

    /// The concrete collision the additive FNV-1a root admitted, kept as a
    /// regression: two histories of two ordinary short keys, same size, same
    /// generation, same evidence kind, folding to one identical root. Nobody
    /// chose these keys adversarially - they are two-byte decimal strings - so
    /// this was weakness in the construction, not the unavoidable fact that
    /// finite hashes collide.
    #[test]
    fn the_short_key_collision_that_sank_the_additive_fnv_root_is_gone() {
        let evidence = || DischargeEvidence::ProvedIrrelevant {
            detail: "not owed".into(),
        };
        let fold = |left: &[u8], right: &[u8]| {
            let mut audit = super::DischargeAudit::default();
            audit.fold(&ObligationKey(left.to_vec()), 3, &evidence());
            audit.fold(&ObligationKey(right.to_vec()), 3, &evidence());
            audit
        };
        let one = fold(b"00", b"05");
        let other = fold(b"01", b"04");
        assert_eq!(one.count, other.count, "same size, different history");
        assert_eq!(one.latest_generation, other.latest_generation);
        assert_ne!(
            one.root, other.root,
            "these two folded to the same 128-bit FNV sum, which is what made \
             the exactness claim false"
        );
    }

    fn lineage_entry(key: &str, parent: Option<&str>, discharged: bool) -> super::LedgerEntry {
        super::LedgerEntry {
            key: ObligationKey(key.as_bytes().to_vec()),
            domain: CoverageDomain::full(scope()),
            generation: 1,
            proof: if discharged {
                ProofStatus::Discharged {
                    evidence: DischargeEvidence::ReplacedByChildren {
                        children: Vec::new(),
                    },
                }
            } else {
                ProofStatus::Unresolved
            },
            policy: PolicyStatus::Retrying { attempts: 0 },
            target: None,
            parent: parent.map(|key| ObligationKey(key.as_bytes().to_vec())),
            first_seen_unix_seconds: 100,
            last_error: error(),
        }
    }

    fn ledger_of(entries: Vec<super::LedgerEntry>) -> DebtLedger {
        let mut map = std::collections::BTreeMap::new();
        for entry in entries {
            map.insert(entry.key.clone(), entry);
        }
        DebtLedger::from_parts(
            map,
            std::collections::BTreeMap::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    /// The pin set must be the ancestor CLOSURE, not the immediate parents of
    /// the survivors. Here `root` is the grandparent of the only open entry, so
    /// a shallow pin folds it, `lineage_root` then answers with a key that is
    /// no longer in the map, and the budget stops being reachable.
    ///
    /// Built through `from_parts` because that is how a multi-level chain
    /// actually arrives: `replace_obligation` re-points every child at the
    /// lineage root, so the shapes it mints in one process are flat, while a
    /// durable row decoded here carries whatever parent links were written into
    /// it.
    #[test]
    fn a_multi_level_lineage_survives_compaction_whole() {
        let mut ledger = ledger_of(vec![
            lineage_entry("root", None, true),
            lineage_entry("middle", Some("root"), true),
            lineage_entry("leaf", Some("middle"), false),
        ]);

        assert_eq!(
            ledger.compact_discharged(),
            0,
            "every discharged entry on the open leaf's chain is load-bearing"
        );
        assert!(ledger.entry(&ObligationKey(b"middle".to_vec())).is_some());
        assert!(
            ledger.entry(&ObligationKey(b"root".to_vec())).is_some(),
            "the grandparent is what a shallow pin set drops"
        );

        assert!(ledger.record_attempt(&ObligationKey(b"leaf".to_vec()), 1));
        assert_eq!(
            ledger
                .entry(&ObligationKey(b"root".to_vec()))
                .expect("root")
                .policy,
            PolicyStatus::OperatorBlocked,
            "the budget must still reach the true root"
        );
    }

    /// The endpoint case, which is where an off-by-one recreates the identical
    /// hole. `lineage_root` follows at most `LINEAGE_DEPTH_CAP` edges and
    /// returns the key reached AFTER the last one, even though that key has a
    /// further parent - so the pin walk has to retain distances 1 through the
    /// cap INCLUSIVE. A walk that pins the current node before advancing and
    /// stops after the cap's iterations drops exactly the key the resolver
    /// returns.
    #[test]
    fn the_lineage_endpoint_at_the_depth_cap_is_pinned_too() {
        let depth = super::LINEAGE_DEPTH_CAP;
        let name = |index: usize| format!("n{index}");
        let mut entries = vec![lineage_entry(&name(0), None, true)];
        for index in 1..=depth + 1 {
            entries.push(lineage_entry(
                &name(index),
                Some(&name(index - 1)),
                // Everything but the leaf is discharged, so everything but the
                // leaf is a fold candidate and only the pin set saves it.
                index != depth + 1,
            ));
        }
        // n{depth+1} is the only open entry; its ancestors run n{depth}..n1,
        // and n0 is reachable only by continuing the walk from n1 - which is
        // the closure, not a single bounded walk from the survivor.
        let mut ledger = ledger_of(entries);
        let leaf = ObligationKey(name(depth + 1).into_bytes());
        let endpoint = ledger.lineage_root(&leaf);
        assert_eq!(
            endpoint,
            ObligationKey(name(1).into_bytes()),
            "the cap stops the walk one short of the true root"
        );

        assert_eq!(
            ledger.compact_discharged(),
            0,
            "the ancestor closure retains the whole chain, including the node \
             past the resolver's cap"
        );
        assert!(
            ledger.entry(&endpoint).is_some(),
            "the key the resolver returns must survive the fold that follows it"
        );
        assert!(
            ledger.entry(&ObligationKey(name(0).into_bytes())).is_some(),
            "and so must ITS parent - the guarantee is the closure, not one walk"
        );
        assert!(ledger.record_attempt(&leaf, 1));
        assert_eq!(
            ledger.entry(&endpoint).expect("endpoint").policy,
            PolicyStatus::OperatorBlocked
        );
    }

    /// The dependency the ledger could not SEE: a repair attempt in flight
    /// against `a`, a covering proof discharging `a` while it is out, and
    /// compaction folding `a` because nothing in the ledger names it. When the
    /// deferred result lands, the attempt still has to be charged against the
    /// lineage its sibling shares, or it is silently lost against the budget and
    /// the retry loop stops being capped.
    ///
    /// `record_attempt` on a key with no entry is exactly what
    /// `apply_repair_resolutions` does for `RepairResolution::Deferred`.
    #[test]
    fn a_deferred_repair_result_still_charges_a_budget_after_its_entry_folds() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, replayable("root"), 1);
        let root = ObligationKey(b"root".to_vec());
        ledger
            .replace_obligation(&root, &[], &[replayable("a"), replayable("b")], 1, 200)
            .expect("replacement accepted");
        // `a` is discharged by a covering proof while its repair is in flight.
        assert!(ledger.discharge_repaired(
            &ObligationKey(b"a".to_vec()),
            1,
            DischargeEvidence::ProvedIrrelevant {
                detail: "covered".into(),
            },
        ));

        assert_eq!(ledger.compact_discharged(), 1, "only `a` folds");
        assert!(ledger.entry(&ObligationKey(b"a".to_vec())).is_none());
        assert!(
            ledger.entry(&root).is_some(),
            "`b` still names the root, so it is pinned"
        );

        // The deferred result for the folded `a` arrives now.
        assert!(
            ledger.record_attempt(&ObligationKey(b"a".to_vec()), 1),
            "the attempt must reach a budget, not evaporate"
        );
        assert_eq!(
            ledger.entry(&root).expect("root").policy,
            PolicyStatus::OperatorBlocked,
            "the charge lands on the lineage the surviving sibling shares"
        );
    }

    /// The folded parent links are BOUNDED: they live only while the chain they
    /// start still reaches a retained entry. Once the whole lineage folds there
    /// is no counter at the end of the walk, so keeping the link would be
    /// exactly the unbounded growth compaction exists to remove.
    #[test]
    fn folded_parent_links_do_not_outlive_their_lineage() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, replayable("root"), 1);
        let root = ObligationKey(b"root".to_vec());
        ledger
            .replace_obligation(&root, &[], &[replayable("a"), replayable("b")], 1, 200)
            .expect("replacement accepted");
        let irrelevant = || DischargeEvidence::ProvedIrrelevant {
            detail: "covered".into(),
        };
        assert!(ledger.discharge_repaired(&ObligationKey(b"a".to_vec()), 1, irrelevant()));
        assert_eq!(ledger.compact_discharged(), 1);
        assert_eq!(ledger.folded_lineage.len(), 1);

        assert!(ledger.discharge_repaired(&ObligationKey(b"b".to_vec()), 1, irrelevant()));
        assert_eq!(ledger.compact_discharged(), 2, "root and `b` fold together");
        assert_eq!(ledger.entries().count(), 0);
        assert!(
            ledger.folded_lineage.is_empty(),
            "nothing retained is left for a link to lead to"
        );
    }

    /// A rediscovered key is a FRESH gap: no parent, budget from zero. This
    /// pins the RESOLUTION ORDER that keeps that true now that a folded parent
    /// link exists at all - the entry wins over the link, so a re-raise does
    /// not inherit the lineage it was folded out of and charge an old root for
    /// a new gap.
    #[test]
    fn a_rediscovered_key_does_not_inherit_its_folded_lineage() {
        let mut ledger = DebtLedger::new();
        ingest_one(&mut ledger, replayable("root"), 1);
        let root = ObligationKey(b"root".to_vec());
        ledger
            .replace_obligation(&root, &[], &[replayable("a"), replayable("b")], 1, 200)
            .expect("replacement accepted");
        assert!(ledger.discharge_repaired(
            &ObligationKey(b"a".to_vec()),
            1,
            DischargeEvidence::ProvedIrrelevant {
                detail: "covered".into(),
            },
        ));
        assert_eq!(ledger.compact_discharged(), 1);

        ingest_one(&mut ledger, replayable("a"), 9);
        let raised = ObligationKey(b"a".to_vec());
        assert_eq!(
            ledger.lineage_root(&raised),
            raised,
            "a re-raise starts its own lineage"
        );
        assert!(!ledger.record_attempt(&raised, 3), "and its own budget");
        assert_eq!(
            ledger.entry(&root).expect("root").policy,
            PolicyStatus::Retrying { attempts: 0 },
            "the old root must not be charged for a fresh gap"
        );
    }

    /// Compaction is not something a caller has to remember. An ingest that
    /// carries the entry map over the threshold folds terminal history on its
    /// own, which is what actually bounds the ledger.
    #[test]
    fn a_large_ingest_compacts_without_being_asked() {
        let mut ledger = DebtLedger::new();
        let obligations: Vec<_> = (0..super::COMPACTION_THRESHOLD)
            .map(|index| object(&format!("obj-{index}")))
            .collect();
        ledger.ingest(
            &InventoryCoverageReport::degraded(time_domain(30, 90), obligations),
            4,
            100,
        );
        assert_eq!(ledger.entries().count(), super::COMPACTION_THRESHOLD);
        assert!(
            ledger.discharge_audit(&scope()).is_none(),
            "nothing is terminal yet, so nothing folds"
        );

        ledger.ingest(
            &InventoryCoverageReport::complete(time_domain(0, 180)),
            5,
            200,
        );
        assert_eq!(
            ledger.entries().count(),
            0,
            "a covering walk discharges them and the same fold reclaims them"
        );
        assert_eq!(
            ledger.discharge_audit(&scope()).expect("audit").count,
            u64::try_from(super::COMPACTION_THRESHOLD).expect("threshold fits a u64")
        );
        assert!(ledger.completion_permitted(&scope()));
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
