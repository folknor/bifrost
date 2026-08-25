//! What an enumeration PROVED, and about which part of which snapshot.
//!
//! The load-bearing rule for inventory is not that failures are visible, it is
//! that no accepted checkpoint may certify coverage it does not have:
//!
//! > Any accepted inventory progress checkpoint must certify that every
//! > provider result before that checkpoint was either materialized as an
//! > `InventoryEntry` or proved irrelevant to the inventory snapshot.
//!
//! A checkpoint advances a cursor past the objects behind it, and the changes
//! stream only reports SUBSEQUENT changes - so an object the walk skipped
//! without recording becomes permanently invisible to that account. Surfacing
//! the failure to a consumer does not fix that; the checkpoint has to carry the
//! unresolved obligations with it, atomically, or not advance.
//!
//! A report therefore has to say what it is a report ABOUT. "Complete" is not a
//! fact about a scope, it is a fact about a region of one enumeration snapshot,
//! and discharging debt on the strength of an unrelated region's success is how
//! a proof ledger starts lying in the other direction.

use crate::cursor::CursorScope;
use crate::error::AccountError;
use crate::ids::ObjectId;

/// Stable, account-defined identity for one obligation.
///
/// Deliberately separate from the repair or replay token. A token may rotate
/// while naming the same gap, and if identity rode the token an account could
/// evade any retry budget by minting a fresh token each pass. Equally
/// deliberately separate from the `AccountError`: classifications and messages
/// change between revisions, and identity must not.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObligationKey(pub Vec<u8>);

/// Which provider state an enumeration observed.
///
/// `None` means the account cannot name a stable snapshot for this walk. That
/// is not a defect - most delta APIs cannot - but it makes positional
/// coordinates from this walk incomparable with any other walk's, because
/// "page 500" over a mutating query denotes different objects each time.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SnapshotIdentity(pub Option<Vec<u8>>);

impl SnapshotIdentity {
    /// No stable snapshot. Positional coordinates cannot cross walks.
    #[must_use]
    pub fn unstable() -> Self {
        Self(None)
    }

    #[must_use]
    pub fn is_stable(&self) -> bool {
        self.0.is_some()
    }

    /// Whether two observations describe the same provider state. Two unstable
    /// identities are never the same snapshot, even though they compare equal
    /// as values - which is exactly why this is not `==`.
    #[must_use]
    pub fn same_snapshot_as(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
}

/// The semantic extent a coverage report speaks for.
///
/// This is NOT the durable `Partition` key. That key names an execution unit
/// and indexes resume state; it says nothing about what range of the provider's
/// objects the unit covered, so it cannot decide whether a later walk's success
/// covers an earlier walk's debt. Repartitioning makes the difference concrete:
/// debt raised under a `30d..90d` window is covered by the union of `7d..60d`
/// and `60d..180d` and by neither alone, and an opaque key comparison sees
/// three unrelated strings.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CoverageCoordinate {
    /// The whole scope.
    Full,
    /// Unix epoch seconds, inclusive-exclusive. `None` is unbounded.
    TimeRange {
        from_unix_seconds: Option<i64>,
        to_unix_seconds: Option<i64>,
    },
    /// Inclusive-exclusive UID range within a UIDVALIDITY epoch. The epoch is part of the
    /// coordinate because UIDs are only comparable inside one.
    UidRange {
        uid_validity: u32,
        from: u64,
        to: u64,
    },
    /// Positional page range, inclusive-exclusive. Meaningful only within one
    /// snapshot: see [`CoverageDomain::covers`].
    PageRange { from: u32, to: u32 },
    /// A provider-defined region that is not positional. `namespace` scopes the
    /// opaque `region` bytes so two providers cannot collide.
    ProviderRegion { namespace: String, region: Vec<u8> },
}

/// What one coverage report speaks for: a region of a snapshot of a scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageDomain {
    pub scope: CursorScope,
    pub coordinate: CoverageCoordinate,
    pub snapshot: SnapshotIdentity,
}

impl CoverageDomain {
    /// The whole of `scope`, with no stable snapshot.
    #[must_use]
    pub fn full(scope: CursorScope) -> Self {
        Self {
            scope,
            coordinate: CoverageCoordinate::Full,
            snapshot: SnapshotIdentity::unstable(),
        }
    }

    /// The extent an [`crate::InventoryPartition`] request covers.
    ///
    /// `uid_validity` is required for a UID range because UIDs only compare
    /// inside one epoch, and a partition request does not carry it - the
    /// account knows it from the folder it opened.
    #[must_use]
    pub fn for_partition(
        scope: CursorScope,
        partition: &crate::InventoryPartition,
        uid_validity: u32,
    ) -> Self {
        let coordinate = match partition {
            crate::InventoryPartition::Full => CoverageCoordinate::Full,
            crate::InventoryPartition::Time {
                from_unix_seconds,
                to_unix_seconds,
            } => CoverageCoordinate::TimeRange {
                from_unix_seconds: *from_unix_seconds,
                to_unix_seconds: *to_unix_seconds,
            },
            crate::InventoryPartition::Uid { from, to } => CoverageCoordinate::UidRange {
                uid_validity,
                from: *from,
                to: *to,
            },
            crate::InventoryPartition::Page { from, to } => CoverageCoordinate::PageRange {
                from: *from,
                to: *to,
            },
        };
        Self {
            scope,
            coordinate,
            snapshot: SnapshotIdentity::unstable(),
        }
    }

    #[must_use]
    pub fn with_snapshot(mut self, snapshot: SnapshotIdentity) -> Self {
        self.snapshot = snapshot;
        self
    }

    /// Whether proving `self` complete also proves `other` complete.
    ///
    /// Conservative by construction: an unrepresentable relation answers
    /// `false`, because the cost of a wrong `true` is discharging debt nothing
    /// re-enumerated - silent loss with a proof record attached - while the
    /// cost of a wrong `false` is a scope that stays degraded until a wider
    /// walk covers it.
    #[must_use]
    pub fn covers(&self, other: &Self) -> bool {
        if self.scope != other.scope {
            return false;
        }
        match (&self.coordinate, &other.coordinate) {
            // A full-scope walk covers anything in that scope, in any snapshot:
            // it re-enumerated the whole space, so whatever the older region
            // held was either seen again or is genuinely gone.
            (CoverageCoordinate::Full, _) => true,
            (_, CoverageCoordinate::Full) => false,
            (
                CoverageCoordinate::TimeRange {
                    from_unix_seconds: self_from,
                    to_unix_seconds: self_to,
                },
                CoverageCoordinate::TimeRange {
                    from_unix_seconds: other_from,
                    to_unix_seconds: other_to,
                },
            ) => {
                lower_bound_covers(*self_from, *other_from)
                    && upper_bound_covers(*self_to, *other_to)
            }
            (
                CoverageCoordinate::UidRange {
                    uid_validity: self_validity,
                    from: self_from,
                    to: self_to,
                },
                CoverageCoordinate::UidRange {
                    uid_validity: other_validity,
                    from: other_from,
                    to: other_to,
                },
            ) => {
                // A UIDVALIDITY change renumbers the mailbox. Ranges either
                // side of it name different objects.
                self_validity == other_validity && self_from <= other_from && self_to >= other_to
            }
            (
                CoverageCoordinate::PageRange {
                    from: self_from,
                    to: self_to,
                },
                CoverageCoordinate::PageRange {
                    from: other_from,
                    to: other_to,
                },
            ) => {
                // Page indices over a mutable query are not a durable region.
                // Page 500..1000 of a later walk may hold entirely different
                // objects, so repeating the coordinate proves nothing unless
                // both reports observed the same snapshot.
                self.snapshot.same_snapshot_as(&other.snapshot)
                    && self_from <= other_from
                    && self_to >= other_to
            }
            (
                CoverageCoordinate::ProviderRegion {
                    namespace: self_namespace,
                    region: self_region,
                },
                CoverageCoordinate::ProviderRegion {
                    namespace: other_namespace,
                    region: other_region,
                },
            ) => self_namespace == other_namespace && self_region == other_region,
            // Mixed coordinate kinds have no defined relation.
            _ => false,
        }
    }
}

fn lower_bound_covers(covering: Option<i64>, covered: Option<i64>) -> bool {
    match (covering, covered) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(covering), Some(covered)) => covering <= covered,
    }
}

fn upper_bound_covers(covering: Option<i64>, covered: Option<i64>) -> bool {
    match (covering, covered) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(covering), Some(covered)) => covering >= covered,
    }
}

/// Whether a region can be re-read once the walk that found it has moved on.
///
/// Two variants, not three. A finite-horizon token - "this page link works for
/// another thirty minutes" - is not durable coverage: the engine cannot
/// guarantee repair before a deadline across crashes, offline periods, disabled
/// accounts or operator blocking, so at the moment the checkpoint is considered
/// it has exactly the same safe disposition as no token at all. Encoding the
/// deadline in the discriminant only invites a future scheduler to read "not
/// expired yet" as permission to advance.
///
/// Converting a perishable continuation into a durable artifact is the
/// ACCOUNT's job, done before it hands the obligation over. If it cannot, it
/// says [`RegionRecovery::CheckpointBarrier`] and the walk stops there.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RegionRecovery {
    /// The token re-reads this region independently of the walk that produced
    /// it. Not a claim of immortality - credentials expire and providers
    /// disappear - but a claim that replay does not depend on the uncommitted
    /// walk session, the superseded cursor, or a provider continuation whose
    /// ordinary lifetime is shorter than retained engine state.
    DurableReplay { token: Vec<u8> },
    /// This region cannot be replayed once the cursor moves past it, so the
    /// cursor must not move past it. Taints the walk: no checkpoint whose
    /// certified prefix includes any part of this region may be accepted.
    ///
    /// `transient_replay` is diagnostics only. It exists so an operator can see
    /// "this was replayable for another few minutes" without any code path
    /// being able to mistake it for advancement authority - which is precisely
    /// why it is a field on the barrier rather than a variant beside it.
    CheckpointBarrier {
        transient_replay: Option<TransientReplayHint>,
    },
}

impl RegionRecovery {
    /// A barrier with nothing to say about transient replay.
    #[must_use]
    pub fn barrier() -> Self {
        Self::CheckpointBarrier {
            transient_replay: None,
        }
    }

    #[must_use]
    pub fn is_barrier(&self) -> bool {
        matches!(self, Self::CheckpointBarrier { .. })
    }
}

/// Non-authoritative note that a region was briefly replayable. Never consulted
/// by any advancement decision.
#[derive(Debug, Clone)]
pub struct TransientReplayHint {
    pub token: Vec<u8>,
    pub note: String,
}

/// One thing an enumeration pass could not account for.
///
/// Split by what is KNOWN, because that decides what repair is possible.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum InventoryObligation {
    /// A discovered object with a stable identity that could not be
    /// represented. Repairable by id: a later read either produces the entry
    /// or proves the object absent.
    Object {
        key: ObligationKey,
        id: ObjectId,
        error: AccountError,
        /// Account-owned opaque repair token. The engine persists and returns
        /// it without interpreting it - only the protocol crate knows what a
        /// provider-native re-read of this object requires. Empty is legal and
        /// means the id alone suffices.
        repair: Vec<u8>,
    },
    /// A provider result that could not even be assigned an identity, or a page
    /// whose completeness could not be established.
    ///
    /// Deliberately NOT an `Object` with a synthetic id. An absent id may mean
    /// one malformed object, a schema mismatch affecting many, a truncated
    /// page, or a response that cannot be correlated with pagination at all -
    /// so calling it a single-object loss overstates what is known, and there
    /// is nothing to name, retry, or reconcile against. The obligation is
    /// therefore scoped to a replayable REGION instead.
    Region {
        key: ObligationKey,
        /// Account-defined label naming the failure CLASS, for deduplicated
        /// operator reporting. Not an identity: several distinct regions may
        /// share one label, which is why waiver targets `key`.
        failure_label: String,
        error: AccountError,
        recovery: RegionRecovery,
    },
}

impl InventoryObligation {
    #[must_use]
    pub fn key(&self) -> &ObligationKey {
        match self {
            Self::Object { key, .. } | Self::Region { key, .. } => key,
        }
    }

    /// Whether this obligation forbids the cursor advancing past it.
    #[must_use]
    pub fn is_barrier(&self) -> bool {
        match self {
            Self::Object { .. } => false,
            Self::Region { recovery, .. } => recovery.is_barrier(),
        }
    }

    #[must_use]
    pub fn error(&self) -> &AccountError {
        match self {
            Self::Object { error, .. } | Self::Region { error, .. } => error,
        }
    }
}

/// What an enumeration pass proved about the region it walked.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CoverageOutcome {
    /// Every result in this domain was materialized or definitively
    /// discharged.
    Complete,
    /// The walk left obligations open.
    Degraded {
        obligations: NonEmptyInventoryObligations,
    },
}

/// A non-empty obligation ledger. Its private storage prevents callers from
/// constructing a degraded coverage claim with nothing to repair.
#[derive(Debug, Clone)]
pub struct NonEmptyInventoryObligations(Vec<InventoryObligation>);

impl NonEmptyInventoryObligations {
    #[must_use]
    pub fn new(first: InventoryObligation, rest: Vec<InventoryObligation>) -> Self {
        let mut obligations = Vec::with_capacity(rest.len() + 1);
        obligations.push(first);
        obligations.extend(rest);
        Self(obligations)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[InventoryObligation] {
        &self.0
    }
}

/// An account's report about one enumeration, and the extent it speaks for.
///
/// Renamed from the older `InventoryCoverage` to keep it distinct from the
/// engine's durable debt ledger. This is a per-walk observation crossing the
/// Account boundary; the ledger is accumulated lifecycle state the engine owns.
/// Calling both "coverage" hid the seam, and the seam is where the interesting
/// mistakes live.
#[derive(Debug, Clone)]
pub struct InventoryCoverageReport {
    pub domain: CoverageDomain,
    pub outcome: CoverageOutcome,
}

impl InventoryCoverageReport {
    /// A clean walk over the stated domain.
    #[must_use]
    pub fn complete(domain: CoverageDomain) -> Self {
        Self {
            domain,
            outcome: CoverageOutcome::Complete,
        }
    }

    #[must_use]
    pub fn degraded(domain: CoverageDomain, obligations: Vec<InventoryObligation>) -> Self {
        Self::from_obligations(domain, &obligations)
    }

    /// Build from a running obligation list: `Complete` exactly when it is
    /// empty. The common shape in every protocol crate's walk loop.
    #[must_use]
    pub fn from_obligations(domain: CoverageDomain, obligations: &[InventoryObligation]) -> Self {
        if obligations.is_empty() {
            Self {
                domain,
                outcome: CoverageOutcome::Complete,
            }
        } else {
            let mut obligations = obligations.to_vec();
            let first = obligations.remove(0);
            Self {
                domain,
                outcome: CoverageOutcome::Degraded {
                    obligations: NonEmptyInventoryObligations::new(first, obligations),
                },
            }
        }
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self.outcome, CoverageOutcome::Complete)
    }

    #[must_use]
    pub fn obligations(&self) -> &[InventoryObligation] {
        match &self.outcome {
            CoverageOutcome::Complete => &[],
            CoverageOutcome::Degraded { obligations } => obligations.as_slice(),
        }
    }

    /// Whether any obligation in this report forbids advancing the cursor.
    #[must_use]
    pub fn has_barrier(&self) -> bool {
        self.obligations()
            .iter()
            .any(InventoryObligation::is_barrier)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CoverageCoordinate, CoverageDomain, InventoryCoverageReport, InventoryObligation,
        ObligationKey, RegionRecovery, SnapshotIdentity,
    };
    use crate::ObjectType;
    use crate::cursor::CursorScope;
    use crate::error::{
        AccountError, AccountErrorBuilder, AccountErrorKind, Cause, DiagnosticText, RequestCause,
        RequestErrorKind,
    };

    fn scope() -> CursorScope {
        CursorScope::Type(ObjectType::Email)
    }

    fn error() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("unrepresentable inventory result"),
            }),
        )
        .try_build()
        .expect("valid account error classification")
    }

    fn page_domain(from: u32, to: u32, snapshot: SnapshotIdentity) -> CoverageDomain {
        CoverageDomain {
            scope: scope(),
            coordinate: CoverageCoordinate::PageRange { from, to },
            snapshot,
        }
    }

    #[test]
    fn a_full_walk_covers_any_region_of_its_scope() {
        let full = CoverageDomain::full(scope());
        assert!(full.covers(&page_domain(0, 100, SnapshotIdentity::unstable())));
        assert!(!page_domain(0, 100, SnapshotIdentity::unstable()).covers(&full));
    }

    /// The repartitioning case: neither new window covers the old one alone,
    /// and nothing may pretend otherwise. Union coverage is the engine's job,
    /// but each individual comparison has to answer honestly first.
    #[test]
    fn a_narrower_time_window_does_not_cover_a_wider_one() {
        let wide = CoverageDomain {
            scope: scope(),
            coordinate: CoverageCoordinate::TimeRange {
                from_unix_seconds: Some(30),
                to_unix_seconds: Some(90),
            },
            snapshot: SnapshotIdentity::unstable(),
        };
        let narrow = CoverageDomain {
            scope: scope(),
            coordinate: CoverageCoordinate::TimeRange {
                from_unix_seconds: Some(7),
                to_unix_seconds: Some(60),
            },
            snapshot: SnapshotIdentity::unstable(),
        };
        assert!(!narrow.covers(&wide));

        let wider = CoverageDomain {
            scope: scope(),
            coordinate: CoverageCoordinate::TimeRange {
                from_unix_seconds: Some(0),
                to_unix_seconds: None,
            },
            snapshot: SnapshotIdentity::unstable(),
        };
        assert!(wider.covers(&wide));
    }

    /// Page indices over a mutable query name different objects each walk, so
    /// repeating the coordinate proves nothing across snapshots. Getting this
    /// wrong discharges debt nothing re-read.
    #[test]
    fn page_ranges_are_incomparable_across_snapshots() {
        let first = page_domain(0, 500, SnapshotIdentity(Some(b"snap-1".to_vec())));
        let second = page_domain(0, 500, SnapshotIdentity(Some(b"snap-2".to_vec())));
        assert!(!second.covers(&first));

        let same = page_domain(0, 500, SnapshotIdentity(Some(b"snap-1".to_vec())));
        assert!(same.covers(&first));
    }

    /// Two walks that cannot name their snapshot are not the same snapshot,
    /// even though `SnapshotIdentity(None) == SnapshotIdentity(None)`.
    #[test]
    fn an_unstable_snapshot_never_matches_itself() {
        let first = page_domain(0, 500, SnapshotIdentity::unstable());
        let second = page_domain(0, 500, SnapshotIdentity::unstable());
        assert!(!second.covers(&first));
    }

    #[test]
    fn a_uid_range_does_not_cross_a_uidvalidity_change() {
        let old = CoverageDomain {
            scope: scope(),
            coordinate: CoverageCoordinate::UidRange {
                uid_validity: 1,
                from: 1,
                to: 100,
            },
            snapshot: SnapshotIdentity::unstable(),
        };
        let renumbered = CoverageDomain {
            scope: scope(),
            coordinate: CoverageCoordinate::UidRange {
                uid_validity: 2,
                from: 1,
                to: 1000,
            },
            snapshot: SnapshotIdentity::unstable(),
        };
        assert!(!renumbered.covers(&old));
    }

    #[test]
    fn a_report_with_a_barrier_region_is_flagged() {
        let report = InventoryCoverageReport::degraded(
            CoverageDomain::full(scope()),
            vec![InventoryObligation::Region {
                key: ObligationKey(b"region-1".to_vec()),
                failure_label: "unidentifiable-value".into(),
                error: error(),
                recovery: RegionRecovery::barrier(),
            }],
        );
        assert!(report.has_barrier());

        let replayable = InventoryCoverageReport::degraded(
            CoverageDomain::full(scope()),
            vec![InventoryObligation::Region {
                key: ObligationKey(b"region-1".to_vec()),
                failure_label: "unidentifiable-value".into(),
                error: error(),
                recovery: RegionRecovery::DurableReplay {
                    token: b"tok".to_vec(),
                },
            }],
        );
        assert!(!replayable.has_barrier());
    }

    /// `from_obligations` splits the head off the caller's list and
    /// `NonEmptyInventoryObligations::new` puts it back in front. Both halves
    /// of that round trip have to agree, and a single-element list cannot tell
    /// an order-preserving reassembly from an order-reversing one - so this
    /// pins the order with a list long enough to distinguish them.
    #[test]
    fn a_degraded_report_preserves_the_caller_obligation_order() {
        let keys: [&[u8]; 3] = [b"region-1", b"region-2", b"region-3"];
        let report = InventoryCoverageReport::degraded(
            CoverageDomain::full(scope()),
            keys.iter()
                .map(|key| InventoryObligation::Region {
                    key: ObligationKey((*key).to_vec()),
                    failure_label: "unidentifiable-value".into(),
                    error: error(),
                    recovery: RegionRecovery::barrier(),
                })
                .collect(),
        );

        let super::CoverageOutcome::Degraded { obligations } = &report.outcome else {
            panic!("expected a degraded report");
        };
        let observed: Vec<&[u8]> = obligations
            .as_slice()
            .iter()
            .map(|obligation| match obligation {
                InventoryObligation::Region { key, .. } => key.0.as_slice(),
                other => panic!("expected a Region obligation, got {other:?}"),
            })
            .collect();
        assert_eq!(observed, keys);
    }

    #[test]
    fn an_empty_obligation_list_reports_complete() {
        let report = InventoryCoverageReport::from_obligations(CoverageDomain::full(scope()), &[]);
        assert!(report.is_complete());
        // `degraded` is the sibling that used to build the unresolvable
        // "degraded with nothing to repair" state directly. It now funnels
        // through the same rule, so both doors give the same answer.
        let sibling = InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![]);
        assert!(sibling.is_complete());
        assert!(sibling.obligations().is_empty());
    }
}
