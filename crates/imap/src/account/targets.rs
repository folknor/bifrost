//! Id-to-outcome accounting for one wire command.
//!
//! Every account-layer path that turns decoded object ids into a UID operand
//! and then attributes the server's answer back to those ids used to
//! re-derive "requested set -> wire operand -> outcome attribution" by hand.
//! Four copies of that derivation (flag mutation, destroy, move, hydration)
//! agreed only by convention, and the class of bug that convention admits is
//! real: an id silently dropped between the requested set and the operand
//! became either a fabricated `Succeeded(Applied)` or an id with no outcome
//! at all, both of which break the one-outcome-per-id contract the sync
//! engine holds.
//!
//! [`TargetBatch`] is the single owner of that derivation. It takes the
//! decoded ids, builds the wire [`UidSet`] from those ids and nothing else,
//! keeps an explicit lane for ids the operand could not represent, and is the
//! only way to mint the batch's outcomes: [`TargetBatch::settle`] consumes
//! the batch, mints the excluded lane itself, and debug-asserts that the
//! caller's minted outcomes cover the target ids exactly once each. A batch
//! that is dropped without being settled panics in debug builds, so a path
//! that returns early with ids unaccounted for cannot go unnoticed.

use bifrost_types::{AccountError, AccountOperation, BatchFailure, BatchItemId, ItemOutcome};

use super::envelope::DecodedObjectId;
use crate::types::{MailboxName, Uid, UidSet};

/// The single predicate for "this UID can be named on the wire".
///
/// Every operand in the account layer is built from this and nothing else,
/// so the set of UIDs a command names and the set of UIDs a caller believes
/// it named cannot disagree.
fn operand_uid(uid: u32) -> Option<Uid> {
    Uid::new(uid)
}

/// A wire UID operand plus the UIDs that did not reach it.
///
/// This is the one place a `UidSet` is built from decoded UIDs.
/// [`TargetBatch`] is the batch-shaped wrapper around it, for lanes that owe
/// one [`ItemOutcome`] per id; the single-id and PIM-primitive lanes, whose
/// results are not per-id outcomes, use `UidOperand` directly so that they
/// share the operand's definition of what a UID set can carry rather than
/// re-deriving it. A lane with no per-id outcome lane has nowhere to report
/// an excluded UID, so it must refuse the whole command
/// ([`UidOperand::excluded`] non-empty) instead of quietly sending a
/// narrower operand than it was asked for.
pub(crate) struct UidOperand {
    uids: Vec<u32>,
    excluded: Vec<u32>,
    uid_set: Option<UidSet>,
}

impl UidOperand {
    /// Split `uids` into the operand and the UIDs it cannot carry.
    pub(crate) fn build<I: IntoIterator<Item = u32>>(uids: I) -> Self {
        let mut kept = Vec::new();
        let mut excluded = Vec::new();
        let mut carried = Vec::new();
        for uid in uids {
            match operand_uid(uid) {
                Some(valid) => {
                    kept.push(uid);
                    carried.push(valid);
                }
                None => excluded.push(uid),
            }
        }
        Self {
            uids: kept,
            excluded,
            uid_set: UidSet::from_uids(carried),
        }
    }

    /// The wire operand, or `None` when no UID reached it.
    pub(crate) fn uid_set(&self) -> Option<&UidSet> {
        self.uid_set.as_ref()
    }

    /// The UIDs the operand carries, in request order.
    pub(crate) fn uids(&self) -> &[u32] {
        &self.uids
    }

    /// The UIDs the operand could not carry, in request order.
    pub(crate) fn excluded(&self) -> &[u32] {
        &self.excluded
    }
}

/// The ids one wire command is accountable for, split into the ids the
/// operand carries and the ids it excluded.
pub(crate) struct TargetBatch {
    /// Ids whose UID is in `uid_set`, in request order.
    targets: Vec<DecodedObjectId>,
    /// The UIDs of `targets`, positionally aligned with it.
    uids: Vec<u32>,
    /// The wire operand, built from `uids` only. `None` when there is no
    /// target to send.
    uid_set: Option<UidSet>,
    /// Ids the operand could not represent. The server never sees these, so
    /// they may never be reported as applied and may never be dropped.
    excluded: Vec<DecodedObjectId>,
    settled: bool,
}

impl TargetBatch {
    /// Split `ids` into the wire operand and the excluded lane.
    ///
    /// The only UID a `UidSet` cannot carry is 0. `decode_object_id` rejects
    /// a 0 UID today, so the excluded lane is normally empty - it exists so
    /// that a future decode path, or any other producer of
    /// `DecodedObjectId`, cannot reintroduce the silent-drop hole by
    /// construction rather than by review.
    pub(crate) fn new(ids: Vec<DecodedObjectId>) -> Self {
        let operand = UidOperand::build(ids.iter().map(|id| id.uid));
        let mut targets = Vec::with_capacity(ids.len());
        let mut excluded = Vec::new();
        for id in ids {
            // The same predicate the operand used, so the accountable set and
            // the wire set are partitioned identically by construction.
            match operand_uid(id.uid) {
                Some(_) => targets.push(id),
                None => excluded.push(id),
            }
        }
        Self {
            targets,
            uids: operand.uids,
            uid_set: operand.uid_set,
            excluded,
            settled: false,
        }
    }

    /// The wire operand, or `None` when no id reached it.
    pub(crate) fn uid_set(&self) -> Option<&UidSet> {
        self.uid_set.as_ref()
    }

    /// The UIDs in the operand, in request order.
    pub(crate) fn uids(&self) -> &[u32] {
        &self.uids
    }

    /// A follow-up operand covering a subset of this batch's UIDs (the
    /// applied subset of a guarded STORE, the UIDs to EXPUNGE after a
    /// `\Deleted` mark). UIDs outside the batch are refused: a second wire
    /// command must never name a message this batch was not given.
    pub(crate) fn subset_uid_set(&self, uids: &[u32]) -> Option<UidSet> {
        debug_assert!(
            uids.iter().all(|uid| self.uids.contains(uid)),
            "a follow-up operand may only name UIDs from its own batch"
        );
        UidOperand::build(uids.iter().copied().filter(|uid| self.uids.contains(uid))).uid_set
    }

    /// Mint the batch's outcomes: exactly one per id, across both lanes.
    ///
    /// `mint` receives the target ids by value and must return one outcome
    /// per id it was given. The excluded lane is minted here, as a
    /// `Failed(Request(Malformed))` naming `operation` and `folder` - a UID
    /// the server never saw is a client-side request defect, never a
    /// success and never a silent drop.
    pub(crate) fn settle<T, F>(
        mut self,
        operation: AccountOperation,
        folder: &MailboxName,
        mint: F,
    ) -> Vec<ItemOutcome<T>>
    where
        F: FnOnce(Vec<DecodedObjectId>) -> Vec<ItemOutcome<T>>,
    {
        self.settled = true;
        let targets = std::mem::take(&mut self.targets);
        let excluded = std::mem::take(&mut self.excluded);
        let expected: Vec<BatchItemId> = targets.iter().map(item_id).collect();
        let mut results: Vec<ItemOutcome<T>> = excluded
            .into_iter()
            .map(|id| {
                let error = excluded_from_operand_error(operation, folder);
                ItemOutcome::Failed(BatchFailure::new(item_id(&id), error))
            })
            .collect();
        let minted = mint(targets);
        debug_assert_covers_exactly(&expected, &minted);
        results.extend(minted);
        results
    }

    /// Settle a batch whose outcomes are published elsewhere (the hydration
    /// stream sends them down a channel rather than returning them), taking
    /// the target ids out for the caller to walk. The excluded lane is
    /// still minted here and returned, so it cannot be forgotten.
    pub(crate) fn settle_streaming<T>(
        mut self,
        operation: AccountOperation,
        folder: &MailboxName,
    ) -> (Vec<DecodedObjectId>, Vec<ItemOutcome<T>>) {
        self.settled = true;
        let targets = std::mem::take(&mut self.targets);
        let excluded = std::mem::take(&mut self.excluded);
        let excluded_outcomes = excluded
            .into_iter()
            .map(|id| {
                let error = excluded_from_operand_error(operation, folder);
                ItemOutcome::Failed(BatchFailure::new(item_id(&id), error))
            })
            .collect();
        (targets, excluded_outcomes)
    }
}

impl Drop for TargetBatch {
    fn drop(&mut self) {
        // Never assert while another panic is unwinding: a panic in a drop
        // during unwind aborts the process, which would turn a clear test
        // failure into an unreadable one.
        if std::thread::panicking() {
            return;
        }
        debug_assert!(
            self.settled || (self.targets.is_empty() && self.excluded.is_empty()),
            "TargetBatch dropped with {} target and {} excluded ids unaccounted for",
            self.targets.len(),
            self.excluded.len()
        );
    }
}

fn item_id(id: &DecodedObjectId) -> BatchItemId {
    BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0)
}

fn outcome_item_id<T>(outcome: &ItemOutcome<T>) -> &BatchItemId {
    match outcome {
        ItemOutcome::Succeeded(success) => &success.item,
        ItemOutcome::Failed(failure) => &failure.item,
        ItemOutcome::Uncertain(uncertain) => &uncertain.item,
    }
}

/// Debug-time proof that a settle covered its targets exactly once each.
/// The check is a debug assertion rather than a type-level guarantee
/// because the mint closures classify per UID against the server's answer;
/// what the type does enforce unconditionally is that the operand and the
/// accountable set come from the same place.
fn debug_assert_covers_exactly<T>(expected: &[BatchItemId], minted: &[ItemOutcome<T>]) {
    #[cfg(debug_assertions)]
    {
        use std::collections::HashMap;

        let mut want: HashMap<&str, isize> = HashMap::new();
        for id in expected {
            *want.entry(id.0.as_str()).or_default() += 1;
        }
        for outcome in minted {
            *want.entry(outcome_item_id(outcome).0.as_str()).or_default() -= 1;
        }
        let unbalanced: Vec<(&str, isize)> =
            want.into_iter().filter(|(_, count)| *count != 0).collect();
        assert!(
            unbalanced.is_empty(),
            "TargetBatch settled with ids not covered exactly once: {unbalanced:?}"
        );
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = (expected, minted);
    }
}

fn excluded_from_operand_error(operation: AccountOperation, folder: &MailboxName) -> AccountError {
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, Cause, DiagnosticText, Protocol, RequestCause,
        RequestErrorKind,
    };

    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(
                "message id could not be placed in an IMAP UID operand",
            ),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Cursor(super::folder_scope(
        folder,
    )))
    .try_build()
    .expect("valid account error classification")
}

#[cfg(test)]
mod tests {
    use bifrost_types::{
        AccountErrorKind, AccountOperation, BatchSuccess, ItemOutcome, MutationSuccess,
        RequestErrorKind,
    };

    use super::{TargetBatch, UidOperand, item_id};
    use crate::account::envelope::DecodedObjectId;
    use crate::types::MailboxName;

    fn folder() -> MailboxName {
        MailboxName::new("INBOX").expect("valid mailbox")
    }

    fn id(uid: u32) -> DecodedObjectId {
        DecodedObjectId {
            folder: folder(),
            uidvalidity: 7,
            uid,
        }
    }

    fn applied(ids: Vec<DecodedObjectId>) -> Vec<ItemOutcome<MutationSuccess>> {
        ids.into_iter()
            .map(|id| {
                ItemOutcome::Succeeded(BatchSuccess::new(item_id(&id), MutationSuccess::Applied))
            })
            .collect()
    }

    // `UidSet` operands must never go out empty, and UID 0 is not a UID
    // (RFC 3501 Section 9), so an all-zero request yields no operand rather
    // than an empty or `0`-bearing sequence set - and the zeros are visible
    // in `excluded()` instead of vanishing.
    #[test]
    fn the_shared_operand_reports_what_it_could_not_carry() {
        let empty = UidOperand::build([]);
        assert!(empty.uid_set().is_none());
        assert!(empty.excluded().is_empty());

        let zeros = UidOperand::build([0, 0]);
        assert!(zeros.uid_set().is_none());
        assert_eq!(zeros.excluded(), &[0, 0], "a dropped UID stays visible");
        assert!(zeros.uids().is_empty());

        let mixed = UidOperand::build([3, 1, 2, 0, 2]);
        assert_eq!(
            mixed
                .uid_set()
                .expect("non-empty")
                .as_sequence_set()
                .as_str(),
            "1:3",
            "adjacent UIDs coalesce into a range and the 0 is dropped",
        );
        assert_eq!(mixed.uids(), &[3, 1, 2, 2]);
        assert_eq!(mixed.excluded(), &[0]);
    }

    // The batch lane and the single-id / PIM lanes must agree on what a UID
    // operand can carry, because they share one constructor. If `TargetBatch`
    // ever re-derived the operand itself, this is what would diverge.
    #[test]
    fn the_batch_operand_is_the_shared_operand() {
        let uids = [4u32, 0, 6];
        let batch = TargetBatch::new(uids.iter().map(|uid| id(*uid)).collect());
        let shared = UidOperand::build(uids);
        assert_eq!(batch.uids(), shared.uids());
        assert_eq!(
            batch.uid_set().map(|set| set.as_sequence_set().as_str()),
            shared.uid_set().map(|set| set.as_sequence_set().as_str()),
        );
        assert_eq!(
            batch
                .subset_uid_set(&[6])
                .map(|set| set.as_sequence_set().as_str().to_owned()),
            UidOperand::build([6u32])
                .uid_set()
                .map(|set| set.as_sequence_set().as_str().to_owned()),
        );
        let _ = batch.settle(AccountOperation::UpdateFlags, &folder(), applied);
    }

    #[test]
    fn an_id_the_operand_cannot_carry_lands_in_the_excluded_lane() {
        let batch = TargetBatch::new(vec![id(1), id(0), id(3)]);
        assert_eq!(batch.uids(), &[1, 3]);
        assert_eq!(
            batch
                .uid_set()
                .expect("non-empty operand")
                .as_sequence_set()
                .as_str(),
            "1,3"
        );
        let results = batch.settle(AccountOperation::UpdateFlags, &folder(), applied);
        assert_eq!(results.len(), 3);
        let failed: Vec<_> = results
            .iter()
            .filter_map(|outcome| match outcome {
                ItemOutcome::Failed(failure) => Some(failure),
                _ => None,
            })
            .collect();
        assert_eq!(failed.len(), 1, "the excluded id must have its own lane");
        assert_eq!(failed[0].item, item_id(&id(0)));
        assert!(matches!(
            failed[0].error.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn every_requested_id_yields_exactly_one_outcome() {
        // One id in each lane: an accounting that answers only the ids the
        // operand carried leaves the excluded one unanswered.
        let ids = vec![id(1), id(0), id(3)];
        let expected: Vec<_> = ids.iter().map(item_id).collect();
        let batch = TargetBatch::new(ids);
        let results = batch.settle(AccountOperation::BulkDestroy, &folder(), applied);
        let seen: Vec<_> = results
            .iter()
            .map(|outcome| match outcome {
                ItemOutcome::Succeeded(success) => success.item.clone(),
                ItemOutcome::Failed(failure) => failure.item.clone(),
                ItemOutcome::Uncertain(uncertain) => uncertain.item.clone(),
            })
            .collect();
        assert_eq!(seen.len(), expected.len());
        for want in &expected {
            assert_eq!(
                seen.iter().filter(|got| *got == want).count(),
                1,
                "each requested id is answered exactly once"
            );
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "not covered exactly once")]
    fn dropping_an_id_during_minting_is_caught() {
        let batch = TargetBatch::new(vec![id(1), id(2)]);
        let _ = batch.settle(AccountOperation::UpdateFlags, &folder(), |ids| {
            applied(ids.into_iter().take(1).collect())
        });
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "unaccounted for")]
    fn an_unsettled_batch_is_caught_on_drop() {
        let batch = TargetBatch::new(vec![id(1)]);
        drop(batch);
    }

    #[test]
    fn a_follow_up_operand_is_built_only_from_the_batch() {
        let batch = TargetBatch::new(vec![id(4), id(5)]);
        let set = batch.subset_uid_set(&[5]).expect("non-empty subset");
        assert_eq!(set.as_sequence_set().as_str(), "5");
        assert!(batch.subset_uid_set(&[]).is_none());
        // A UID outside the batch is filtered out of the operand in release
        // builds and caught by the debug assertion in debug builds; the
        // debug half is pinned by the panic test below.
        #[cfg(not(debug_assertions))]
        assert!(batch.subset_uid_set(&[9]).is_none());
        let _ = batch.settle(AccountOperation::UpdateFlags, &folder(), applied);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "only name UIDs from its own batch")]
    fn a_follow_up_operand_naming_a_foreign_uid_is_caught() {
        let batch = TargetBatch::new(vec![id(4)]);
        let _ = batch.subset_uid_set(&[9]);
    }

    #[test]
    fn a_streaming_settle_still_mints_the_excluded_lane() {
        let batch = TargetBatch::new(vec![id(1), id(0)]);
        let (targets, excluded): (_, Vec<ItemOutcome<MutationSuccess>>) =
            batch.settle_streaming(AccountOperation::Hydrate, &folder());
        assert_eq!(targets, vec![id(1)]);
        assert_eq!(excluded.len(), 1);
    }
}
