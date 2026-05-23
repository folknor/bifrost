use super::batch::{BatchFailure, BatchSuccess, BatchUncertain};

#[derive(Clone, Debug)]
pub enum ItemOutcome<T> {
    Succeeded(BatchSuccess<T>),
    Failed(BatchFailure),
    Uncertain(BatchUncertain),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MutationSuccess {
    Applied,
    Skipped,
}
