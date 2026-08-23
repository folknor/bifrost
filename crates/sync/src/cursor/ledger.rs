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
    AccountError, CoverageDomain, InventoryCoverageReport, InventoryObligation, ObligationKey,
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
/// A waived barrier is what later authorizes a crossing checkpoint, and that
/// crossing must atomically record an unresolved-but-waived ledger entry. The
/// waiver converts blocked progress into declared accepted loss; it never makes
/// the engine simply forget.
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
        for entry in self.entries.values_mut() {
            if !entry.is_open() {
                continue;
            }
            // Both conditions, not either. A newer generation stops a stale
            // report overwriting fresh state, but recency alone proves nothing
            // - a newer PARTIAL walk is still partial. The domain has to
            // actually contain the ground the gap was on.
            if generation >= entry.generation && domain.covers(&entry.domain) {
                entry.proof = ProofStatus::Discharged {
                    evidence: DischargeEvidence::CoveringWalk {
                        domain: domain.clone(),
                    },
                };
            }
        }
        self.barriers.retain(|_, barrier| {
            !(generation >= barrier.generation && domain.covers(&barrier.domain))
        });
        self.proved.push((generation, domain));
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
        match self.entries.get_mut(key) {
            Some(entry) => {
                entry.policy = PolicyStatus::OperatorBlocked;
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BarrierIncident, DebtLedger, PolicyStatus, ProofStatus};
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
