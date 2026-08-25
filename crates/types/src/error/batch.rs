use std::collections::HashSet;
use std::fmt;

use serde::Serialize;

use super::account_error::AccountError;
use super::cause::{BatchInputInvalidItem, BatchInputInvalidReason};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchItem<I> {
    pub id: BatchItemId,
    pub input: I,
}

impl<I> BatchItem<I> {
    #[must_use]
    pub fn new(id: BatchItemId, input: I) -> Self {
        Self { id, input }
    }
}

/// Returned from `Account` methods that take a `Vec<BatchItem<_>>`.
/// Immutable: lanes are populated via `BatchOutcomeBuilder` and frozen
/// by `finalize`. The three lanes are closed by design - adding a
/// fourth lane is a deliberate breaking change.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchOutcome<T> {
    succeeded: Vec<BatchSuccess<T>>,
    failed: Vec<BatchFailure>,
    uncertain: Vec<BatchUncertain>,
    order: Vec<BatchLane>,
}

impl<T> BatchOutcome<T> {
    #[must_use]
    pub fn succeeded(&self) -> &[BatchSuccess<T>] {
        &self.succeeded
    }

    #[must_use]
    pub fn failed(&self) -> &[BatchFailure] {
        &self.failed
    }

    #[must_use]
    pub fn uncertain(&self) -> &[BatchUncertain] {
        &self.uncertain
    }

    /// Iterate over per-item outcomes in submission order (the order
    /// the `BatchItem`s appeared in the original input vec).
    pub fn iter(&self) -> impl Iterator<Item = BatchItemOutcome<'_, T>> {
        self.order.iter().map(|lane| match lane {
            BatchLane::Succeeded(index) => BatchItemOutcome::Succeeded(
                self.succeeded
                    .get(*index)
                    .expect("BatchOutcome order references existing success"),
            ),
            BatchLane::Failed(index) => BatchItemOutcome::Failed(
                self.failed
                    .get(*index)
                    .expect("BatchOutcome order references existing failure"),
            ),
            BatchLane::Uncertain(index) => BatchItemOutcome::Uncertain(
                self.uncertain
                    .get(*index)
                    .expect("BatchOutcome order references existing uncertainty"),
            ),
        })
    }
}

/// Mutable builder for [`BatchOutcome`]. Protocol crates push per-item
/// results as they classify them, then call [`finalize`](BatchOutcomeBuilder::finalize)
/// with the original submitted ids. `finalize` validates that every
/// submitted id appears exactly once across the three lanes and
/// returns the immutable outcome.
#[derive(Clone, Debug)]
pub struct BatchOutcomeBuilder<T> {
    succeeded: Vec<BatchSuccess<T>>,
    failed: Vec<BatchFailure>,
    uncertain: Vec<BatchUncertain>,
    order: Vec<BatchLane>,
}

impl<T> Default for BatchOutcomeBuilder<T> {
    fn default() -> Self {
        Self {
            succeeded: Vec::new(),
            failed: Vec::new(),
            uncertain: Vec::new(),
            order: Vec::new(),
        }
    }
}

impl<T> BatchOutcomeBuilder<T> {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_succeeded(&mut self, item: BatchItemId, output: T) {
        let index = self.succeeded.len();
        self.succeeded.push(BatchSuccess::new(item, output));
        self.order.push(BatchLane::Succeeded(index));
    }

    pub fn push_failed(&mut self, item: BatchItemId, error: AccountError) {
        let index = self.failed.len();
        self.failed.push(BatchFailure::new(item, error));
        self.order.push(BatchLane::Failed(index));
    }

    pub fn push_uncertain(&mut self, item: BatchItemId, error: AccountError) {
        let index = self.uncertain.len();
        self.uncertain.push(BatchUncertain::new(item, error));
        self.order.push(BatchLane::Uncertain(index));
    }

    /// Freeze the builder into an immutable [`BatchOutcome`]. Validates
    /// that every id in `expected` appears in exactly one lane, no
    /// unknown ids were added, and no id was duplicated across lanes.
    /// Returns `Err(BatchInvariantError)` on any violation; protocol
    /// crates with bugs that miscount items surface them here rather
    /// than silently shipping a wrong outcome.
    pub fn finalize(
        self,
        expected: &[BatchItemId],
    ) -> Result<BatchOutcome<T>, BatchInvariantError> {
        // Index `expected` once. The membership test used to be a
        // linear `expected.iter().any(..)` of `String` comparisons
        // inside the per-item loop, i.e. O(n*m) on a hot mutation path:
        // a 1000-item Gmail batch cost roughly a million string
        // compares to prove an invariant that is almost always
        // satisfied.
        let known: HashSet<&BatchItemId> = expected.iter().collect();
        let mut seen: HashSet<&BatchItemId> = HashSet::with_capacity(expected.len());
        let mut duplicates: Vec<BatchItemId> = Vec::new();
        let mut unknown: Vec<BatchItemId> = Vec::new();

        for id in self
            .succeeded
            .iter()
            .map(|s| &s.item)
            .chain(self.failed.iter().map(|f| &f.item))
            .chain(self.uncertain.iter().map(|u| &u.item))
        {
            if !known.contains(id) {
                unknown.push(id.clone());
                continue;
            }
            if !seen.insert(id) {
                duplicates.push(id.clone());
            }
        }

        let missing: Vec<BatchItemId> = expected
            .iter()
            .filter(|id| !seen.contains(id))
            .cloned()
            .collect();

        if !missing.is_empty() || !duplicates.is_empty() || !unknown.is_empty() {
            return Err(BatchInvariantError {
                missing,
                duplicates,
                unknown,
            });
        }

        Ok(BatchOutcome {
            succeeded: self.succeeded,
            failed: self.failed,
            uncertain: self.uncertain,
            order: self.order,
        })
    }
}

/// Invariant violation detected by `BatchOutcomeBuilder::finalize`.
/// Producer-bug surface: every submitted item must appear in exactly
/// one lane. Tests inside protocol crates catch this before it ships.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BatchInvariantError {
    /// Submitted ids that did not appear in any lane.
    pub missing: Vec<BatchItemId>,
    /// Ids that appeared in more than one lane.
    pub duplicates: Vec<BatchItemId>,
    /// Ids appearing in a lane that were not in the submitted vec.
    pub unknown: Vec<BatchItemId>,
}

impl fmt::Display for BatchInvariantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BatchOutcome invariant violation: missing={} duplicates={} unknown={}",
            self.missing.len(),
            self.duplicates.len(),
            self.unknown.len(),
        )
    }
}

impl std::error::Error for BatchInvariantError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchLane {
    Succeeded(usize),
    Failed(usize),
    Uncertain(usize),
}

#[derive(Clone, Debug)]
pub enum BatchItemOutcome<'a, T> {
    Succeeded(&'a BatchSuccess<T>),
    Failed(&'a BatchFailure),
    Uncertain(&'a BatchUncertain),
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchSuccess<T> {
    pub item: BatchItemId,
    pub output: T,
}

impl<T> BatchSuccess<T> {
    #[must_use]
    pub fn new(item: BatchItemId, output: T) -> Self {
        Self { item, output }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchFailure {
    pub item: BatchItemId,
    pub error: AccountError,
}

impl BatchFailure {
    #[must_use]
    pub fn new(item: BatchItemId, error: AccountError) -> Self {
        Self { item, error }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchUncertain {
    pub item: BatchItemId,
    pub error: AccountError,
}

impl BatchUncertain {
    #[must_use]
    pub fn new(item: BatchItemId, error: AccountError) -> Self {
        Self { item, error }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct BatchItemId(pub String);

/// Pre-flight validation for Vec-batch input. Returns the list of
/// offending items if any `BatchItemId` is empty or duplicate, or if
/// the input itself is empty. Protocol crates that build a
/// `Vec<BatchItem<_>>` interface must call this before any byte
/// crosses the side-effect boundary so empty/duplicate identifiers
/// surface as `Err(AccountError { kind: Request(BatchInputInvalid),
/// .. })` rather than silently splitting the caller's intent.
pub fn validate_batch_input<I>(items: &[BatchItem<I>]) -> Result<(), Vec<BatchInputInvalidItem>> {
    if items.is_empty() {
        return Err(vec![BatchInputInvalidItem {
            id: BatchItemId(String::new()),
            reason: BatchInputInvalidReason::Empty,
        }]);
    }

    let mut seen = HashSet::new();
    let mut invalid = Vec::new();
    for item in items {
        if item.id.0.is_empty() {
            invalid.push(BatchInputInvalidItem {
                id: item.id.clone(),
                reason: BatchInputInvalidReason::Empty,
            });
            continue;
        }
        if !seen.insert(item.id.clone()) {
            invalid.push(BatchInputInvalidItem {
                id: item.id.clone(),
                reason: BatchInputInvalidReason::Duplicate,
            });
        }
    }

    if invalid.is_empty() {
        Ok(())
    } else {
        Err(invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{
        AccountErrorBuilder, AccountErrorKind, Cause, DiagnosticText, RequestCause,
        RequestErrorKind,
    };

    fn error() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("bad input"),
            }),
        )
        .try_build()
        .expect("valid account error classification")
    }

    fn ids(strs: &[&str]) -> Vec<BatchItemId> {
        strs.iter().map(|s| BatchItemId(s.to_string())).collect()
    }

    #[test]
    fn iter_preserves_push_order() {
        let mut builder = BatchOutcomeBuilder::<()>::new();
        builder.push_failed(BatchItemId("b".to_string()), error());
        builder.push_succeeded(BatchItemId("a".to_string()), ());
        builder.push_uncertain(BatchItemId("c".to_string()), error());

        let outcome = builder.finalize(&ids(&["b", "a", "c"])).unwrap();

        let lanes = outcome
            .iter()
            .map(|item| match item {
                BatchItemOutcome::Succeeded(success) => success.item.0.as_str(),
                BatchItemOutcome::Failed(failure) => failure.item.0.as_str(),
                BatchItemOutcome::Uncertain(uncertain) => uncertain.item.0.as_str(),
            })
            .collect::<Vec<_>>();

        assert_eq!(lanes, ["b", "a", "c"]);
    }

    #[test]
    fn lane_vectors_preserve_lane_order() {
        let mut builder = BatchOutcomeBuilder::<()>::new();
        builder.push_succeeded(BatchItemId("a".to_string()), ());
        builder.push_failed(BatchItemId("b".to_string()), error());
        builder.push_succeeded(BatchItemId("c".to_string()), ());

        let outcome = builder.finalize(&ids(&["a", "b", "c"])).unwrap();

        assert_eq!(outcome.succeeded()[0].item.0, "a");
        assert_eq!(outcome.succeeded()[1].item.0, "c");
        assert_eq!(outcome.failed()[0].item.0, "b");
    }

    #[test]
    fn finalize_detects_missing_duplicate_and_unknown_ids() {
        let mut builder = BatchOutcomeBuilder::<()>::new();
        builder.push_succeeded(BatchItemId("a".to_string()), ());
        builder.push_succeeded(BatchItemId("a".to_string()), ()); // duplicate
        builder.push_succeeded(BatchItemId("z".to_string()), ()); // unknown
        // missing: "b"

        let err = builder.finalize(&ids(&["a", "b"])).expect_err("invalid");
        assert_eq!(err.missing, ids(&["b"]));
        assert_eq!(err.duplicates, ids(&["a"]));
        assert_eq!(err.unknown, ids(&["z"]));
    }

    /// `finalize` indexes `expected` into a `HashSet` rather than
    /// rescanning it per item. A caller that submitted a duplicate id
    /// must reach the same verdict it did under the linear scan: the id
    /// is known, so it is not `unknown`, and one lane entry satisfies
    /// both copies of the expectation rather than reporting `missing`.
    #[test]
    fn a_duplicated_expectation_does_not_change_the_verdict() {
        let mut builder = BatchOutcomeBuilder::<()>::new();
        builder.push_succeeded(BatchItemId("a".to_string()), ());

        let outcome = builder
            .finalize(&ids(&["a", "a"]))
            .expect("a known id is accounted for");
        assert_eq!(outcome.succeeded.len(), 1);
    }

    #[test]
    fn validate_rejects_empty_and_duplicate_ids() {
        let items = vec![
            BatchItem {
                id: BatchItemId(String::new()),
                input: (),
            },
            BatchItem {
                id: BatchItemId("a".to_string()),
                input: (),
            },
            BatchItem {
                id: BatchItemId("a".to_string()),
                input: (),
            },
        ];

        let errors = validate_batch_input(&items).expect_err("input must be invalid");

        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0].reason, BatchInputInvalidReason::Empty);
        assert_eq!(errors[1].reason, BatchInputInvalidReason::Duplicate);
    }
}
