//! Mutation pipeline.
//!
//! The engine wraps each `Account::bulk_*` call with:
//! - `IdempotencyKey` vending (run_id + monotonic sequence + protocol salt),
//! - per-id accumulation through `ItemOutcome<MutationSuccess>` lanes
//!   (applied / skipped / failed_terminal / pending_retry /
//!   pending_readback / blocked_by_engine),
//! - retry on `RecoveryPlan::Retry`, sleeping per `RetryAdvice` and
//!   re-submitting unresolved ids with the same key,
//! - the read-back guard, which re-fetches each pending-readback id via
//!   `account.get_stream(_, Projection::FlagsOnly)` and reconciles
//!   apparent failures against actual server state.

pub mod idempotency;
pub mod readback;

pub use idempotency::{IdempotencyVendor, MutationCampaignId};
pub use readback::{
    ReadbackOutcome, run_destroy_readback_guard, run_move_readback_guard, run_readback_guard,
};

/// Aggregate counters for a mutation campaign.
///
/// `failed_terminal` and `blocked_by_engine` are deliberately separate:
/// the first means the item itself terminally failed; the second means
/// the campaign was halted by an engine directive raised against the
/// stream (`RestartScope`, `SchemaIncompatible`, etc.) and the item's
/// fate is decoupled from any per-item classification. Telemetry
/// dashboards pivot on one or the other.
///
/// `dedupe_by_client_id` records how many items the reconcile guidance
/// asked the consumer to dedupe by client id (see
/// [`ReconcileAction::DedupeByClientId`]). The engine cannot perform
/// that dedupe itself (the client-id space lives at the consumer);
/// surfacing the count plus a `Warning::OperatorAttentionNeeded` is
/// the contract.
#[derive(Debug, Default, Clone, Copy)]
pub struct MutationCounters {
    pub applied: u64,
    pub skipped: u64,
    pub failed_terminal: u64,
    pub blocked_by_engine: u64,
    pub pending_retry: u64,
    pub dedupe_by_client_id: u64,
}

impl MutationCounters {
    pub fn record_applied(&mut self) {
        self.applied = self.applied.saturating_add(1);
    }
    pub fn record_skipped(&mut self) {
        self.skipped = self.skipped.saturating_add(1);
    }
    pub fn record_failed(&mut self) {
        self.failed_terminal = self.failed_terminal.saturating_add(1);
    }
    pub fn record_blocked_by_engine(&mut self) {
        self.blocked_by_engine = self.blocked_by_engine.saturating_add(1);
    }
    pub fn record_pending(&mut self) {
        self.pending_retry = self.pending_retry.saturating_add(1);
    }
    pub fn record_dedupe_by_client_id(&mut self) {
        self.dedupe_by_client_id = self.dedupe_by_client_id.saturating_add(1);
    }
}
