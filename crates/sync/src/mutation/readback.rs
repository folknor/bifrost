//! Read-back guard for `MutationReplaySafety::None` protocols.
//!
//! After a retry, the engine cannot tell whether the server applied
//! the original attempt or just the retry. The read-back guard
//! re-fetches each retried batch via
//! `account.get_stream(_, Projection::FlagsOnly)` and reconciles
//! `Failed` outcomes against actual server state: if the flags now
//! match the intended target, the failure is downgraded to `Skipped`
//! (the original apply succeeded).
//!
//! Applies to all four protocols today; the
//! `MutationReplaySafety::ReplayToken` variant is reserved but unused.

use std::collections::HashSet;

use bifrost_types::{
    Account, FlagOp, HydratedObject, HydratedObjectKind, ObjectId, Projection, SyncEvent,
};
use futures::stream::{self, StreamExt};

use crate::error::Error;

/// Outcome of a read-back guard pass.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadbackOutcome {
    /// Items whose post-mutation flag state matched the requested
    /// target. These were applied (possibly by the pre-retry attempt)
    /// and are downgraded to `Skipped`.
    pub skipped: u64,
    /// Items whose state did NOT match the target. The protocol's
    /// `Failed` outcome stands.
    pub still_failed: u64,
}

/// Run the read-back guard against a set of ids and a target flag-op.
///
/// `account.get_stream(ids, Projection::FlagsOnly)` yields
/// `HydratedObjectKind::FlagsOnly(HashSet<String>)` per id. The guard
/// compares each id's hydrated flags against the target state encoded
/// in `op`:
///
/// - `Add(set)` -> target satisfied iff every flag in `set` is present.
/// - `Remove(set)` -> target satisfied iff no flag in `set` is present.
/// - `Set(set)` -> target satisfied iff hydrated == `set`.
/// - `Patch { add, remove }` -> add + remove must both hold.
pub async fn run_readback_guard(
    account: &dyn Account,
    ids: Vec<ObjectId>,
    op: &FlagOp,
) -> Result<ReadbackOutcome, Error> {
    let input = stream::iter(ids).boxed();
    let mut stream = account.get_stream(input, Projection::FlagsOnly);
    let mut outcome = ReadbackOutcome::default();
    while let Some(event) = stream.next().await {
        match event {
            SyncEvent::Batch(batch) => {
                for hydrated in batch.items {
                    if matches_target(&hydrated, op) {
                        outcome.skipped = outcome.skipped.saturating_add(1);
                    } else {
                        outcome.still_failed = outcome.still_failed.saturating_add(1);
                    }
                }
            }
            SyncEvent::Done(_) => break,
            SyncEvent::Fatal(f) => {
                return Err(Error::Account(f.0.clone()));
            }
            SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
            _ => {}
        }
    }
    Ok(outcome)
}

fn matches_target(hydrated: &HydratedObject, op: &FlagOp) -> bool {
    let HydratedObjectKind::FlagsOnly(observed) = &hydrated.kind else {
        // Wrong projection - cannot reconcile. Be safe: treat as not
        // matched so the original `Failed` outcome stands.
        return false;
    };
    matches_target_set(observed, op)
}

#[must_use]
pub(crate) fn matches_target_set(observed: &HashSet<String>, op: &FlagOp) -> bool {
    match op {
        FlagOp::Add(set) => set.iter().all(|f| observed.contains(f)),
        FlagOp::Remove(set) => set.iter().all(|f| !observed.contains(f)),
        FlagOp::Set(set) => observed == set,
        FlagOp::Patch { add, remove } => {
            add.iter().all(|f| observed.contains(f)) && remove.iter().all(|f| !observed.contains(f))
        }
        // `FlagOp` is `#[non_exhaustive]`; an unknown operation cannot
        // be reconciled against observed flags, so the original
        // `Failed` outcome stands.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn add_target_matches_when_subset_present() {
        let obs = set(&["\\Seen", "\\Flagged"]);
        let op = FlagOp::Add(set(&["\\Seen"]));
        assert!(matches_target_set(&obs, &op));
    }

    #[test]
    fn add_target_does_not_match_when_missing() {
        let obs = set(&["\\Flagged"]);
        let op = FlagOp::Add(set(&["\\Seen"]));
        assert!(!matches_target_set(&obs, &op));
    }

    #[test]
    fn remove_target_matches_when_absent() {
        let obs = set(&["\\Flagged"]);
        let op = FlagOp::Remove(set(&["\\Seen"]));
        assert!(matches_target_set(&obs, &op));
    }

    #[test]
    fn set_target_requires_exact_match() {
        let obs = set(&["\\Seen"]);
        let op = FlagOp::Set(set(&["\\Seen"]));
        assert!(matches_target_set(&obs, &op));
        let op2 = FlagOp::Set(set(&["\\Seen", "\\Flagged"]));
        assert!(!matches_target_set(&obs, &op2));
    }

    #[test]
    fn patch_requires_both_sides() {
        let obs = set(&["\\Seen"]);
        let op = FlagOp::Patch {
            add: set(&["\\Seen"]),
            remove: set(&["\\Flagged"]),
        };
        assert!(matches_target_set(&obs, &op));
        let op2 = FlagOp::Patch {
            add: set(&["\\Seen"]),
            remove: set(&["\\Seen"]),
        };
        assert!(!matches_target_set(&obs, &op2));
    }
}
