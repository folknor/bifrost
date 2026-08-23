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
    /// The requested mutation was performed as asked.
    Applied,
    /// Nothing was done, and nothing needed to be: the target was already
    /// in the requested state.
    Skipped,
    /// Something was done, but NOT what was asked - the provider accepted a
    /// weaker form of the operation.
    ///
    /// Distinct from `Applied` because the target is not in the requested
    /// state, and from `Skipped` because the target DID change. Reporting
    /// either in its place is a wrong answer a consumer cannot detect: claiming
    /// `Applied` makes an engine believe a state it will keep re-observing as
    /// false, and claiming `Skipped` hides a mutation that really happened.
    ///
    /// The motivating case is Gmail `bulk_destroy` under the `gmail.modify`
    /// OAuth scope, which does not permit permanent delete. `messages/batchDelete`
    /// answers 403 and the account falls back to a TRASH label patch: the
    /// messages move, they do not cease to exist. Reported as `Applied`, they
    /// came back on the next inventory pass and were destroyed again forever.
    ///
    /// `bifrost-sync` routes this through the read-back guard rather than
    /// trusting it, so the final accounting comes from observed state rather
    /// than from the provider's claim.
    Downgraded,
}
