//! Engine-side error vocabulary.
//!
//! Distinct from `bifrost_types::Error` (which is per-operation,
//! protocol-facing) and `bifrost_types::Fatal` (which is the
//! stream-terminating recovery signal). `engine::Error` wraps both
//! plus the engine's own failure modes (account not attached,
//! checkpoint store rejected the write, shutdown in progress).

use bifrost_types::{
    AccountId, CapabilityDelta, CursorScope, Fatal as TypesFatal, RecoveryClass, StrategyDowngrade,
    Warning as TypesWarning,
};

/// Engine-side failure type.
///
/// Returned from `SyncEngine` orchestration calls (`attach`, `detach`,
/// `bulk_set_flags`, `checkpoint_now`, etc.). Distinct from the
/// per-operation `bifrost_types::Error` returned from inside Account
/// trait calls.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Caller addressed an account that was never attached or has
    /// already been detached.
    #[error("account {0:?} is not attached to this engine")]
    AccountNotAttached(AccountId),
    /// `attach` was called twice for the same account id without an
    /// intervening `detach`.
    #[error("account {0:?} is already attached")]
    AccountAlreadyAttached(AccountId),
    /// `AccountFactory::open` returned an error. Carries the typed
    /// source so the consumer can match on `Error::Auth` / `Transport`
    /// / etc. rather than parsing a formatted string.
    #[error("failed to open account: {0}")]
    OpenFailed(#[source] bifrost_types::Error),
    /// `establish_initial_cursor` returned an error while attaching
    /// the account.
    #[error("failed to establish initial cursor: {0}")]
    EstablishCursorFailed(String),
    /// Checkpoint persistence failed.
    #[error("checkpoint store rejected the write: {0}")]
    CheckpointStore(String),
    /// Cursor envelope on disk uses a schema this engine cannot read.
    #[error("checkpoint envelope schema is incompatible")]
    SchemaIncompatible,
    /// Engine is shutting down; no new work accepted.
    #[error("engine is shutting down")]
    ShuttingDown,
    /// Account is paused.
    #[error("account is paused")]
    Paused,
    /// Generic wrap-around for `bifrost_types::Error`.
    #[error("account error: {0}")]
    Account(#[from] bifrost_types::Error),
    /// Catch-all.
    #[error("{0}")]
    Other(String),
}

/// Engine-side mapping of a `RecoveryClass` to a coarse `Fatal`
/// shape the consumer sees.
///
/// The five recovery actions the engine takes are mutually exclusive:
/// retry, restart-scope, restart-account, downgrade-strategy,
/// surface-to-consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FatalAction {
    /// Retry the same stream after a server-supplied delay.
    Retry,
    /// Drop the cursor for one scope and re-establish via inventory.
    RestartScope,
    /// Reopen the account (factory.open) and restart all streams.
    RestartAccount,
    /// Continue the stream with a downgraded protocol strategy.
    DowngradeStrategy,
    /// Bubble up to the consumer; engine cannot recover unattended.
    SurfaceToConsumer,
}

/// Map a `RecoveryClass` into the engine's coarse `FatalAction`
/// taxonomy. Lives here (not in `bifrost-types`) because the action
/// table is engine policy, not protocol contract.
#[must_use]
pub fn map_recovery_to_fatal(recovery: &RecoveryClass) -> FatalAction {
    match recovery {
        RecoveryClass::Retry { .. } => FatalAction::Retry,
        RecoveryClass::DowngradeStrategy(_) => FatalAction::DowngradeStrategy,
        RecoveryClass::DowngradeCapabilityForScope(_) | RecoveryClass::RestartScope(_) => {
            FatalAction::RestartScope
        }
        RecoveryClass::RestartAccount | RecoveryClass::CapabilityChanged { .. } => {
            FatalAction::RestartAccount
        }
        RecoveryClass::AuthLost
        | RecoveryClass::SchemaIncompatible
        | RecoveryClass::OperatorOverrideRequired { .. }
        | RecoveryClass::Fatal => FatalAction::SurfaceToConsumer,
        // `RecoveryClass` is `#[non_exhaustive]`; route unknown
        // variants to the consumer so the engine fails closed rather
        // than silently swallowing a recovery action it does not
        // understand.
        _ => FatalAction::SurfaceToConsumer,
    }
}

/// Engine-side wrapper around `bifrost_types::Fatal` carrying the
/// derived `FatalAction` so consumers don't have to re-derive it.
#[derive(Debug)]
pub struct Fatal {
    pub action: FatalAction,
    pub inner: TypesFatal,
}

impl Fatal {
    #[must_use]
    pub fn from_types(inner: TypesFatal) -> Self {
        let action = map_recovery_to_fatal(&inner.recovery);
        Self { action, inner }
    }
}

/// Engine-side wrapper around `bifrost_types::Warning`. Identical
/// shape today; the type exists so the engine can grow its own
/// warning kinds without churning `bifrost-types`.
pub type Warning = TypesWarning;

/// Helpers exposed for tests + multiplexer wiring.
#[must_use]
pub fn recovery_targets_scope(recovery: &RecoveryClass) -> Option<&CursorScope> {
    match recovery {
        RecoveryClass::DowngradeCapabilityForScope(s) | RecoveryClass::RestartScope(s) => Some(s),
        _ => None,
    }
}

#[must_use]
pub fn recovery_targets_downgrade(recovery: &RecoveryClass) -> Option<StrategyDowngrade> {
    match recovery {
        RecoveryClass::DowngradeStrategy(d) => Some(*d),
        _ => None,
    }
}

#[must_use]
pub fn recovery_targets_capability(recovery: &RecoveryClass) -> Option<&CapabilityDelta> {
    match recovery {
        RecoveryClass::CapabilityChanged { delta } => Some(delta),
        _ => None,
    }
}
