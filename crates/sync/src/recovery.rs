//! Engine-side recovery dispatch helpers.
//!
//! Protocol crates derive a `RecoveryClass` on every `AccountError`
//! they produce. The engine's job is to dispatch the verdict, not to
//! reclassify. These helpers exist for the small amount of plumbing the
//! engine reuses across multiple dispatch sites:
//!
//! - `plan_recovery` collapses a derived `RecoveryClass` into a closed
//!   four-arm `RecoveryPlan` so the engine never variant-matches the
//!   open `RecoveryClass` enum directly. Adding a future variant fails
//!   to compile here, not silently routes to terminal.
//! - `retry_delay` resolves a `RetryAdvice` to a concrete `Duration`
//!   using the cause's [`RetryHint`] when present.
//! - `directive_target_scope` resolves the `CursorScope` an
//!   [`EngineDirective`] targets, when any. `#[non_exhaustive]` on
//!   `EngineDirective` is enforced: no catch-all fallthrough arm.
//! - `restart_scope_error` constructs the single account-error shape
//!   the engine itself emits when synthesizing a `RestartScope`
//!   directive (scope lifecycle Created / Renamed, or fresh
//!   establishment via the reopen listener).
//! - `cursor_decode_failure` translates a cursor envelope schema
//!   mismatch into an `AccountError` whose recovery derives to
//!   `Engine(SchemaIncompatible)`.
//! - [`ThrottleBucket`] holds engine-scope throttle waits keyed by
//!   [`ThrottleKey`]. `ThrottleScope::CurrentOperation` never enters
//!   the bucket; the caller delays the single work item inline.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountId, AccountOperation, Cause,
    CursorScope, EngineDirective, ErrorScope, Fatal, MailboxId, Provider, ReconcileAdvice,
    RecoveryClass, RetryAdvice, StateCause, SyncStateErrorKind, ThrottleKey, ThrottleScope,
};

/// Closed four-arm plan derived from a `RecoveryClass`. Every engine
/// dispatch site routes through [`plan_recovery`] so an added
/// `RecoveryClass` variant fails to compile here rather than silently
/// landing in the terminal arm.
#[derive(Debug, Clone)]
pub(crate) enum RecoveryPlan {
    Retry(RetryAdvice),
    Reconcile(ReconcileAdvice),
    Engine(EngineDirective),
    Terminal(Fatal),
}

/// Collapse an `AccountError`'s recovery into a [`RecoveryPlan`]. The
/// helper consumes the error so the terminal arm owns the value handed
/// to [`Fatal::try_from`]; engine sites that need to retain a copy for
/// telemetry should clone before calling.
///
/// Branching uses the four mutually-exclusive helpers on
/// `RecoveryClass` (`is_retryable`, `requires_reconciliation`,
/// `requires_engine_action`, `is_terminal`). The wildcard at the end
/// is unreachable because the helpers cover every variant; we panic
/// rather than silently choose a default if a future helper bug
/// breaks the invariant.
pub(crate) fn plan_recovery(error: AccountError) -> RecoveryPlan {
    let recovery = error.recovery();
    if recovery.is_retryable() {
        match recovery.clone() {
            RecoveryClass::Retry(advice) => RecoveryPlan::Retry(advice),
            other => unreachable!("is_retryable lied about {other:?}"),
        }
    } else if recovery.requires_reconciliation() {
        match recovery.clone() {
            RecoveryClass::Reconcile(advice) => RecoveryPlan::Reconcile(advice),
            other => unreachable!("requires_reconciliation lied about {other:?}"),
        }
    } else if recovery.requires_engine_action() {
        match recovery.clone() {
            RecoveryClass::Engine(directive) => RecoveryPlan::Engine(directive),
            other => unreachable!("requires_engine_action lied about {other:?}"),
        }
    } else {
        // Terminal. `Fatal::try_from` rejects non-terminal errors;
        // because `is_terminal` returned true here, the conversion
        // always succeeds.
        let fatal = Fatal::try_from(error)
            .expect("is_terminal returned true but Fatal::try_from rejected the error");
        RecoveryPlan::Terminal(fatal)
    }
}

/// Resolve the effective sleep an engine path should observe before
/// retrying. `now` is the caller's notion of the current wall clock;
/// `fallback` is the duration used when the advice carries no hint.
#[must_use]
pub(crate) fn retry_delay(advice: &RetryAdvice, now: SystemTime, fallback: Duration) -> Duration {
    advice
        .retry_hint
        .map_or(fallback, |hint| hint.min_delay(now))
}

/// True when `directive` targets a single cursor scope. Used by the
/// multiplexer to populate `ReopenRequest::Recovery::scope`.
///
/// `EngineDirective` is `#[non_exhaustive]` from `bifrost-types`, so the
/// `_ =>` arm is required for compilation. New variants land here as
/// account-wide by default; review this match when extending
/// `EngineDirective` to decide whether a new variant deserves a scope.
#[must_use]
pub(crate) fn directive_target_scope(directive: &EngineDirective) -> Option<CursorScope> {
    match directive {
        EngineDirective::RestartScope(scope)
        | EngineDirective::DowngradeCapabilityForScope(scope)
        | EngineDirective::DisableScope(scope) => Some(scope.clone()),
        EngineDirective::DowngradeStrategy(_)
        | EngineDirective::RestartAccount
        | EngineDirective::SchemaIncompatible
        | EngineDirective::OperatorOverrideRequired { .. } => None,
        // `EngineDirective` is `#[non_exhaustive]` from `bifrost-types`, so
        // this wildcard is REQUIRED for compilation across the crate
        // boundary even though every current variant is named above. A new
        // scope-bearing variant still defaults account-wide here until a
        // human adds its arm (the sync-N1 residual).
        _ => None,
    }
}

/// Build a sync-state cursor-invalid error scoped to a single cursor.
/// Used by scope lifecycle (`Created`, `Renamed`) and the reopen
/// listener when the engine needs to ask itself for a fresh
/// establishment of one scope. The derived recovery is
/// `Engine(RestartScope(scope))`.
#[must_use]
pub(crate) fn restart_scope_error(scope: CursorScope, operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .scope(ErrorScope::Cursor(scope))
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// Synthesize an `AccountError` for an engine-level re-establishment
/// failure that has no `AccountError` of its own (`EstablishCursorFailed`,
/// `CheckpointStore`, and the other non-`Account` `engine::Error`
/// variants surfaced from `run_establish`). Without this, a scope that
/// exhausts its reopen budget on purely engine-level errors would
/// broadcast no `SyncEvent::Terminated` - only the operator warning -
/// violating the documented "after three failures broadcast
/// `Terminated(last_error)`" contract. Classified as
/// `SyncState(CursorInvalid)` (the engine could not establish or persist
/// the scope's cursor); the carried scope lets consumers route the
/// termination to the affected scope.
#[must_use]
pub(crate) fn establish_failure_error(
    scope: CursorScope,
    operation: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .scope(ErrorScope::Cursor(scope))
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// Translate a cursor envelope schema mismatch into an `AccountError`
/// whose recovery derives to `Engine(SchemaIncompatible)`. Producers
/// (specifically the cursor envelope decoder) hit this path when the
/// on-disk schema is below `MIN_MIGRATABLE`. The original engine
/// `Error::SchemaIncompatible` becomes a derived account error so the
/// engine's recovery dispatch can drive the schema-clear loop.
#[must_use]
pub(crate) fn cursor_decode_failure(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// Engine-scope throttle bucket. Records "do not drive work for this
/// key before `wait_until`" deadlines and answers "how long should I
/// pause work that maps to this key now?".
///
/// `Tenant` and `Provider` keys cross account boundaries by design: a
/// tenant throttle pauses every account on that tenant, and a
/// provider-wide throttle pauses every account on that provider.
/// `Mailbox` and `Account` keys are per-account.
#[derive(Debug, Default)]
pub struct ThrottleBucket {
    waits: HashMap<ThrottleKey, SystemTime>,
}

impl ThrottleBucket {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a wait-until deadline for `key`. If a wait already
    /// exists, the later of the two deadlines wins (a stricter throttle
    /// from a parallel response should not shorten an earlier one).
    pub fn record(&mut self, key: ThrottleKey, wait_until: SystemTime) {
        self.waits
            .entry(key)
            .and_modify(|existing| {
                if wait_until > *existing {
                    *existing = wait_until;
                }
            })
            .or_insert(wait_until);
    }

    /// Return the remaining wait for `key` at `now`. `None` means no
    /// throttle applies (or the recorded deadline has expired).
    #[must_use]
    pub fn wait_for(&self, key: &ThrottleKey, now: SystemTime) -> Option<Duration> {
        let until = *self.waits.get(key)?;
        until.duration_since(now).ok().filter(|d| !d.is_zero())
    }

    /// Drop expired entries. Called opportunistically by the engine
    /// (e.g. between work items) to keep the map small.
    pub fn cleanup_expired(&mut self, now: SystemTime) {
        self.waits.retain(|_, until| *until > now);
    }
}

/// Build a `ThrottleKey` from a `ThrottleScope` plus the relevant
/// identities. `CurrentOperation` returns `None` because it is a
/// per-call hint, not a bucket entry.
#[must_use]
pub(crate) fn throttle_key_for(
    scope: ThrottleScope,
    account: &AccountId,
    mailbox: Option<&MailboxId>,
    tenant: Option<&str>,
    provider: Option<Provider>,
) -> Option<ThrottleKey> {
    match scope {
        ThrottleScope::CurrentOperation => None,
        ThrottleScope::Mailbox => mailbox.map(|m| ThrottleKey::Mailbox {
            account: account.clone(),
            mailbox: m.clone(),
        }),
        ThrottleScope::Account => Some(ThrottleKey::Account(account.clone())),
        ThrottleScope::Tenant => tenant.map(|t| ThrottleKey::Tenant(t.to_string())),
        ThrottleScope::Provider => provider.map(ThrottleKey::Provider),
        // ThrottleScope is #[non_exhaustive]; new variants default to
        // "no bucket entry" (the conservative classification) and
        // require explicit handling here.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        AttemptCause, AuthCause, AuthErrorKind, RetryDisposition, RetryHint, RetryReason,
        TransmissionState, TransportCause, TransportErrorKind, TransportKind,
    };

    fn build_retry() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        )
        .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid")
    }

    fn build_reconcile() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        )
        .operation(AccountOperation::Send)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::InFlight,
        )))
        .try_build()
        .expect("valid")
    }

    fn build_engine() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid")
    }

    fn build_terminal() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid")
    }

    #[test]
    fn plan_recovery_retry_arm() {
        assert!(matches!(
            plan_recovery(build_retry()),
            RecoveryPlan::Retry(_)
        ));
    }

    #[test]
    fn plan_recovery_reconcile_arm() {
        assert!(matches!(
            plan_recovery(build_reconcile()),
            RecoveryPlan::Reconcile(_)
        ));
    }

    #[test]
    fn plan_recovery_engine_arm() {
        assert!(matches!(
            plan_recovery(build_engine()),
            RecoveryPlan::Engine(EngineDirective::SchemaIncompatible)
        ));
    }

    #[test]
    fn plan_recovery_terminal_arm() {
        assert!(matches!(
            plan_recovery(build_terminal()),
            RecoveryPlan::Terminal(_)
        ));
    }

    #[test]
    fn retry_delay_uses_retry_hint_when_present() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let advice = RetryAdvice::new(
            RetryDisposition::SameRequest,
            Some(RetryHint::After(Duration::from_secs(7))),
            RetryReason::Transport,
            None,
        );
        assert_eq!(
            retry_delay(&advice, now, Duration::from_secs(1)),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn retry_delay_uses_fallback_without_hint() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let advice = RetryAdvice::new(
            RetryDisposition::SameRequest,
            None,
            RetryReason::Transport,
            None,
        );
        assert_eq!(
            retry_delay(&advice, now, Duration::from_secs(3)),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn restart_scope_error_derives_engine_restart_scope() {
        let scope = CursorScope::Account;
        let err = restart_scope_error(scope.clone(), AccountOperation::SyncChanges);
        assert!(matches!(
            err.recovery(),
            RecoveryClass::Engine(EngineDirective::RestartScope(got)) if *got == scope
        ));
    }

    #[test]
    fn cursor_decode_failure_routes_to_schema_incompatible() {
        let err = cursor_decode_failure(AccountOperation::SyncChanges);
        assert!(matches!(
            err.recovery(),
            RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
        ));
    }

    #[test]
    fn directive_target_scope_picks_scope_bound_directives() {
        let scope = CursorScope::Account;
        assert_eq!(
            directive_target_scope(&EngineDirective::RestartScope(scope.clone())),
            Some(scope.clone())
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::DowngradeCapabilityForScope(scope.clone())),
            Some(scope.clone())
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::DisableScope(scope.clone())),
            Some(scope)
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::RestartAccount),
            None
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::SchemaIncompatible),
            None
        );
    }

    #[test]
    fn throttle_bucket_records_and_expires() {
        let mut bucket = ThrottleBucket::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let key = ThrottleKey::Account(AccountId("a".into()));
        bucket.record(key.clone(), now + Duration::from_secs(5));

        assert_eq!(bucket.wait_for(&key, now), Some(Duration::from_secs(5)));
        // After the deadline.
        assert_eq!(bucket.wait_for(&key, now + Duration::from_secs(10)), None);
    }

    #[test]
    fn throttle_bucket_take_max_of_overlapping_records() {
        let mut bucket = ThrottleBucket::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let key = ThrottleKey::Account(AccountId("a".into()));
        bucket.record(key.clone(), now + Duration::from_secs(5));
        bucket.record(key.clone(), now + Duration::from_secs(2));
        assert_eq!(bucket.wait_for(&key, now), Some(Duration::from_secs(5)));
    }

    #[test]
    fn throttle_bucket_cleanup_drops_expired() {
        let mut bucket = ThrottleBucket::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let a = ThrottleKey::Account(AccountId("a".into()));
        let b = ThrottleKey::Account(AccountId("b".into()));
        bucket.record(a.clone(), now + Duration::from_secs(1));
        bucket.record(b.clone(), now + Duration::from_secs(60));
        bucket.cleanup_expired(now + Duration::from_secs(30));
        assert!(bucket.wait_for(&a, now + Duration::from_secs(30)).is_none());
        assert!(bucket.wait_for(&b, now + Duration::from_secs(30)).is_some());
    }

    #[test]
    fn throttle_bucket_tenant_crosses_accounts() {
        // A `Tenant` key is a single map entry; multiple accounts
        // querying it observe the same wait.
        let mut bucket = ThrottleBucket::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let key = ThrottleKey::Tenant("tenant-x".into());
        bucket.record(key.clone(), now + Duration::from_secs(10));
        // Two accounts; both observe the same tenant throttle because
        // the key does not include account identity.
        let a = throttle_key_for(
            ThrottleScope::Tenant,
            &AccountId("acc-1".into()),
            None,
            Some("tenant-x"),
            None,
        )
        .expect("tenant key");
        let b = throttle_key_for(
            ThrottleScope::Tenant,
            &AccountId("acc-2".into()),
            None,
            Some("tenant-x"),
            None,
        )
        .expect("tenant key");
        assert_eq!(a, b);
        assert_eq!(a, key);
        assert!(bucket.wait_for(&a, now).is_some());
        assert!(bucket.wait_for(&b, now).is_some());
    }

    #[test]
    fn throttle_scope_current_operation_does_not_enter_bucket() {
        let key = throttle_key_for(
            ThrottleScope::CurrentOperation,
            &AccountId("a".into()),
            None,
            None,
            None,
        );
        assert!(key.is_none());
    }
}
