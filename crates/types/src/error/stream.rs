use super::batch::{BatchFailure, BatchSuccess, BatchUncertain};

/// Per-item outcome in a streaming bulk operation. The three-lane
/// model is closed by design: adding a fourth lane is a deliberate
/// breaking change, not a smooth extension. Wildcard arms would let
/// stale consumer policy silently apply to new lanes and lose audit
/// signal.
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
