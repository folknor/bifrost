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

use bifrost_types::InventoryCoverageReport;

/// Engine-issued identity for one checkpoint publication.
///
/// Monotonic within an attached account. Not durable: it identifies a
/// publication within the lifetime of the writer that issued it, and a
/// duplicate acknowledgement from a previous process cannot reach that writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PublicationId(pub u64);

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
}

/// What happened to a publication.
#[derive(Debug, Clone)]
enum ClaimState {
    /// Published, not yet acknowledged.
    Pending(CoverageClaim),
    /// Acknowledged and persisted. Retained so a duplicate acknowledgement is
    /// idempotent rather than reinterpreted: re-applying would double-ingest,
    /// and treating the second as unknown would either reject a successfully
    /// persisted checkpoint or - far worse - default it to `Complete`.
    Persisted,
}

/// Per-account publication registry.
#[derive(Debug, Default)]
pub struct PendingCoverage {
    inner: Mutex<HashMap<PublicationId, ClaimState>>,
    next: AtomicU64,
    next_generation: AtomicU64,
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

    /// Issue an identity for a publication that carries no coverage claim.
    ///
    /// An ordinary changes-stream advance proves nothing about enumeration
    /// coverage, and must leave the ledger UNCHANGED. That is a different fact
    /// from "the report went missing", which is why it is an explicit empty
    /// claim rather than an absent entry.
    pub fn publish_without_report(&self, generation: u64) -> PublicationId {
        self.publish(CoverageClaim {
            reports: Vec::new(),
            generation,
        })
    }

    /// Register a claim and issue its publication identity.
    pub fn publish(&self, claim: CoverageClaim) -> PublicationId {
        let id = PublicationId(self.next.fetch_add(1, Ordering::Relaxed));
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, ClaimState::Pending(claim));
        id
    }

    /// Fold `superseded` into `survivor` and drop the superseded entry.
    ///
    /// Called when the control path stops waiting for an older outstanding
    /// checkpoint because a newer one covers it. Removing the older claim
    /// WITHOUT folding it is how partition A's obligations get discarded while
    /// the engine believes B accounted for them.
    pub fn supersede(&self, superseded: PublicationId, survivor: PublicationId) {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(ClaimState::Pending(old)) = guard.remove(&superseded) else {
            return;
        };
        if let Some(ClaimState::Pending(new)) = guard.get_mut(&survivor) {
            new.absorb(old);
        } else {
            // The survivor is gone or already persisted, so folding has nowhere
            // to land. Put the claim back rather than dropping it on the floor.
            guard.insert(superseded, ClaimState::Pending(old));
        }
    }

    /// Look up an acknowledgement and mark it persisted.
    ///
    /// Marks before the store write completes, and that is deliberate: a second
    /// acknowledgement racing the first must not apply the claim twice. A
    /// failed store write is reported to the caller, which leaves the record
    /// unwritten - the same position as any other failed ack, and the
    /// checkpoint simply does not advance.
    pub fn claim(&self, id: PublicationId) -> ClaimLookup {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match guard.get(&id) {
            None => ClaimLookup::Unknown,
            Some(ClaimState::Persisted) => ClaimLookup::AlreadyPersisted,
            Some(ClaimState::Pending(_)) => {
                let Some(ClaimState::Pending(claim)) = guard.insert(id, ClaimState::Persisted)
                else {
                    unreachable!("checked Pending under the same lock");
                };
                ClaimLookup::Apply(claim)
            }
        }
    }

    /// Drop a publication that can never be acknowledged: it reached no real
    /// subscriber, its subscriber lagged out, or the account detached.
    ///
    /// Without this the registry grows for the life of the attachment.
    pub fn retire(&self, id: PublicationId) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }

    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|state| matches!(state, ClaimState::Pending(_)))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::{ClaimLookup, CoverageClaim, PendingCoverage};
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, Cause, CoverageDomain, CursorScope, DiagnosticText,
        InventoryCoverageReport, InventoryObligation, ObjectId, ObjectType, ObligationKey,
        RequestCause, RequestErrorKind,
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

    #[test]
    fn an_unknown_acknowledgement_is_never_treated_as_complete() {
        let pending = PendingCoverage::new();
        assert!(matches!(
            pending.claim(super::PublicationId(42)),
            ClaimLookup::Unknown
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
        let id = pending.publish(CoverageClaim::new(degraded("a"), 1));

        assert!(matches!(pending.claim(id), ClaimLookup::Apply(_)));
        assert!(matches!(pending.claim(id), ClaimLookup::AlreadyPersisted));
    }

    /// Superseding must FOLD. Dropping the superseded claim discards debt the
    /// survivor never re-reported, which is the whole failure mode.
    #[test]
    fn supersession_folds_the_older_claim_into_the_survivor() {
        let pending = PendingCoverage::new();
        let older = pending.publish(CoverageClaim::new(degraded("a"), 1));
        let newer = pending.publish(CoverageClaim::new(degraded("b"), 2));

        pending.supersede(older, newer);

        assert!(matches!(pending.claim(older), ClaimLookup::Unknown));
        let ClaimLookup::Apply(claim) = pending.claim(newer) else {
            panic!("survivor must still be acknowledgeable");
        };
        assert_eq!(claim.reports.len(), 2, "the superseded report must survive");
        assert_eq!(claim.reports[0].obligations()[0].key().0, b"a".to_vec());
        assert_eq!(claim.generation, 2);
    }

    /// If the survivor cannot absorb it, the claim stays put rather than
    /// vanishing.
    #[test]
    fn supersession_into_a_persisted_survivor_keeps_the_older_claim() {
        let pending = PendingCoverage::new();
        let older = pending.publish(CoverageClaim::new(degraded("a"), 1));
        let newer = pending.publish(CoverageClaim::new(degraded("b"), 2));
        let _ = pending.claim(newer);

        pending.supersede(older, newer);
        assert!(matches!(pending.claim(older), ClaimLookup::Apply(_)));
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
        pending.retire(id);
        assert_eq!(pending.outstanding(), 0);
        assert!(matches!(pending.claim(id), ClaimLookup::Unknown));
    }
}
