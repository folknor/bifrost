use std::collections::HashSet;

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

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchOutcome<T> {
    pub succeeded: Vec<BatchSuccess<T>>,
    pub failed: Vec<BatchFailure>,
    pub uncertain: Vec<BatchUncertain>,
    order: Vec<BatchLane>,
}

impl<T> Default for BatchOutcome<T> {
    fn default() -> Self {
        Self {
            succeeded: Vec::new(),
            failed: Vec::new(),
            uncertain: Vec::new(),
            order: Vec::new(),
        }
    }
}

impl<T> BatchOutcome<T> {
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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
        .build()
    }

    #[test]
    fn iter_preserves_push_order() {
        let mut outcome = BatchOutcome::default();
        outcome.push_failed(BatchItemId("b".to_string()), error());
        outcome.push_succeeded(BatchItemId("a".to_string()), ());
        outcome.push_uncertain(BatchItemId("c".to_string()), error());

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
        let mut outcome = BatchOutcome::default();
        outcome.push_succeeded(BatchItemId("a".to_string()), ());
        outcome.push_failed(BatchItemId("b".to_string()), error());
        outcome.push_succeeded(BatchItemId("c".to_string()), ());

        assert_eq!(outcome.succeeded[0].item.0, "a");
        assert_eq!(outcome.succeeded[1].item.0, "c");
        assert_eq!(outcome.failed[0].item.0, "b");
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
