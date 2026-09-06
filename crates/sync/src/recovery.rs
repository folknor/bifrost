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
//!   [`EngineDirective`] targets, when any. Its required catch-all
//!   defaults an unknown future variant to account-wide until a human
//!   adds the appropriate scope-bearing arm.
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

use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountId, AccountOperation, Cause,
    CursorScope, EngineDirective, ErrorScope, Fatal, ReconcileAdvice, RecoveryClass, RetryAdvice,
    StateCause, SyncStateErrorKind, ThrottleKey, ThrottleScope,
};

#[cfg(test)]
use bifrost_types::MailboxId;

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

/// The reconcile counterpart of [`retry_delay`]. A throttle that lands
/// mid-flight on a non-idempotent operation classifies as
/// `Reconcile(ThrottledMidFlight)` rather than `Retry`, and it carries
/// the provider's retry hint; honoring it keeps the read-back probe
/// from re-entering the same rate limit it just hit.
#[must_use]
pub(crate) fn reconcile_delay(
    advice: &ReconcileAdvice,
    now: SystemTime,
    fallback: Duration,
) -> Duration {
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
        // human adds its arm.
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

/// Classify an account's violation of the batch boundary contract: a
/// `PageBoundary::Partial` batch that nonetheless carries a checkpoint.
///
/// This MUST be a classified `AccountError` and not a bare engine error.
/// `Batch`'s fields are public, so the type system cannot stop a protocol
/// crate from emitting the shape, and an engine that merely logged it would
/// re-poll the same unchanged cursor at the scope's cadence forever: no
/// backoff past `poll_max`, no recovery dispatch, no `Terminated` event, and
/// a consumer that sees a scope which has silently stopped making progress
/// with nothing in the stream to say so. `ProviderContractViolation` is
/// terminal, so `plan_recovery` stops the scope instead - the honest outcome,
/// because a provider emitting a nonsense boundary does not heal by being
/// asked again.
///
/// The protocol tag is read off the offending checkpoint. There is no
/// protocol accessor on `dyn Account`, so where the checkpoint carries no
/// tag of its own the error says `Protocol::Unknown` rather than guessing.
#[must_use]
pub(crate) fn batch_boundary_violation(
    checkpoint: Option<&bifrost_types::Checkpoint>,
    operation: AccountOperation,
    scope: &CursorScope,
) -> AccountError {
    contract_violation(
        checkpoint,
        operation,
        scope,
        "a PageBoundary::Partial batch carried a checkpoint",
    )
}

/// A live batch carrying a BACKFILL checkpoint. The same classification and the
/// same terminal outcome, with its own words: the boundary was fine, the
/// checkpoint belongs to a lane this stream does not publish on, and an operator
/// reading "a PageBoundary::Partial batch carried a checkpoint" would go looking
/// for the wrong thing.
#[must_use]
pub(crate) fn backfill_checkpoint_on_changes(
    checkpoint: Option<&bifrost_types::Checkpoint>,
    operation: AccountOperation,
    scope: &CursorScope,
) -> AccountError {
    contract_violation(
        checkpoint,
        operation,
        scope,
        "a changes batch carried a backfill checkpoint",
    )
}

fn contract_violation(
    checkpoint: Option<&bifrost_types::Checkpoint>,
    operation: AccountOperation,
    scope: &CursorScope,
    detail: &'static str,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::ContractViolation),
        Cause::Wire(bifrost_types::WireCause::MalformedResponse {
            protocol: checkpoint_protocol(checkpoint),
            detail: Some(bifrost_types::DiagnosticText::support_only(detail)),
        }),
    )
    .operation(operation)
    .scope(ErrorScope::Cursor(scope.clone()))
    .try_build()
    .expect("valid account error classification")
}

fn checkpoint_protocol(checkpoint: Option<&bifrost_types::Checkpoint>) -> bifrost_types::Protocol {
    use bifrost_types::{Checkpoint, Protocol, ProtocolKind};
    let Some(Checkpoint::Change(cursor)) = checkpoint else {
        // A backfill checkpoint carries no protocol tag at all.
        return Protocol::Unknown;
    };
    match cursor.server_state.protocol {
        ProtocolKind::Jmap => Protocol::Jmap,
        ProtocolKind::Imap => Protocol::Imap,
        ProtocolKind::Gmail => Protocol::Gmail,
        ProtocolKind::CardDav => Protocol::CardDav,
        ProtocolKind::CalDav => Protocol::CalDav,
        ProtocolKind::Graph => Protocol::Graph,
        // `ProtocolKind` is `#[non_exhaustive]`; a variant added upstream is
        // unmappable here, and naming the wrong protocol is worse than
        // declining to name one.
        _ => Protocol::Unknown,
    }
}

/// Engine-scope throttle bucket. Records "do not drive work for this
/// key before `wait_until`" deadlines and answers "how long should I
/// pause work that maps to this key now?".
///
/// One bucket per `SyncEngine`, shared by every attached account.
/// `Tenant` and `Provider` keys cross account boundaries by design: a
/// tenant throttle pauses every account on that tenant, and a
/// provider-wide throttle pauses every account on that provider.
/// `Mailbox` and `Account` keys are per-account.
///
/// Cross-account keys reach a sibling account through the membership
/// index: an account joins a `Tenant` / `Provider` bucket the first
/// time its own error stream names that identity, and from then on a
/// deadline any account records under the key pauses it too. The index
/// survives `cleanup_expired` deliberately - membership is an identity
/// fact, not a deadline - and is bounded by the identity space (a
/// handful of providers and tenants per process).
#[derive(Debug, Default)]
pub struct ThrottleBucket {
    waits: HashMap<ThrottleKey, SystemTime>,
    memberships: HashMap<AccountId, HashSet<ThrottleKey>>,
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

    /// Record a deadline observed by `account`. Cross-account keys
    /// (`Tenant`, `Provider`) also enroll the account in the key's
    /// membership so `wait_for_account` sees deadlines siblings record
    /// later. Per-account keys (`Account`, `Mailbox`) embed their
    /// account and need no membership entry.
    pub fn record_for(&mut self, account: &AccountId, key: ThrottleKey, wait_until: SystemTime) {
        if matches!(key, ThrottleKey::Tenant(_) | ThrottleKey::Provider(_)) {
            self.memberships
                .entry(account.clone())
                .or_default()
                .insert(key.clone());
        }
        self.record(key, wait_until);
    }

    /// Return the remaining wait for `key` at `now`. `None` means no
    /// throttle applies (or the recorded deadline has expired).
    #[must_use]
    pub fn wait_for(&self, key: &ThrottleKey, now: SystemTime) -> Option<Duration> {
        let until = *self.waits.get(key)?;
        until.duration_since(now).ok().filter(|d| !d.is_zero())
    }

    /// Longest remaining wait that applies to `account` as a whole at
    /// `now`: the account's own `Account` key plus every cross-account
    /// key it is enrolled in. `Mailbox` keys are deliberately excluded -
    /// a per-mailbox throttle must not pause the whole account, and the
    /// engine has no scope-to-mailbox mapping to pause anything
    /// narrower with.
    #[must_use]
    pub fn wait_for_account(&self, account: &AccountId, now: SystemTime) -> Option<Duration> {
        let own = self.wait_for(&ThrottleKey::Account(account.clone()), now);
        let shared = self
            .memberships
            .get(account)
            .into_iter()
            .flatten()
            .filter_map(|key| self.wait_for(key, now));
        shared.chain(own).max()
    }

    /// Drop expired entries. Called opportunistically by the engine
    /// (e.g. between work items) to keep the map small. Memberships are
    /// retained: they record identity, not deadlines.
    pub fn cleanup_expired(&mut self, now: SystemTime) {
        self.waits.retain(|_, until| *until > now);
    }

    /// Forget an account's memberships. Called at detach: without it,
    /// unique detached ids accumulate for the engine's lifetime, and an
    /// `AccountId` reattached against a different factory or provider
    /// would inherit the previous life's enrollments and pause on an
    /// unrelated provider's deadline. Deadlines under the account's own
    /// key are left to expire on their own - they are per-account facts
    /// a reattach of the same account may still want to honor.
    pub fn forget_account(&mut self, account: &AccountId) {
        self.memberships.remove(account);
    }
}

/// Resolve the `ThrottleKey` a `ThrottleScope` maps to, given the
/// identities the classified error actually carries. Broader provider and
/// tenant scopes may conservatively fall back to the account key. Mailbox
/// scope cannot: an account key is wider than the producer's throttle.
///
/// - `CurrentOperation` never enters the bucket (per-call hint).
/// - `Mailbox` uses the error's mailbox identity in EITHER shape it is
///   produced in - `ErrorScope::Mailbox { id }` or
///   `ErrorScope::Cursor(CursorScope::Folder(id))`, the latter being what
///   every production folder producer actually builds. An error that names
///   no mailbox at all remains local to the current operation.
/// - `Tenant` ALWAYS degrades to `Account` today: the error contract
///   carries no tenant identity, so there is nothing to key a
///   cross-account tenant bucket on. Cross-account tenant pausing is
///   blocked on that types-level identity channel.
/// - `Provider` uses `AccountError::provider()`, degrading to
///   `Account` when absent.
///
/// The asymmetry in that list is deliberate and is the whole design. A BROADER
/// scope may degrade toward the account key, because the account is a SUBSET
/// of what it describes and pausing it is conservative. A NARROWER one may not:
/// widening a per-mailbox 429 to the account key converts one mailbox's
/// throttle into an account-wide stall, so a mailbox scope whose error names no
/// mailbox stays operation-local instead. Do not "complete" the match by
/// giving it an account fallback.
///
/// `Account`-keyed deadlines also deliberately survive a detach until they
/// expire. They describe the stable account IDENTITY, not the connection
/// incarnation, so clearing them on detach would let a detach-plus-reattach
/// bypass a provider wait. Growth is bounded by expiry pruning.
#[must_use]
pub(crate) fn resolve_throttle_key(
    scope: ThrottleScope,
    account: &AccountId,
    error: &AccountError,
) -> Option<ThrottleKey> {
    let account_key = || ThrottleKey::Account(account.clone());
    match scope {
        ThrottleScope::CurrentOperation => None,
        // BOTH mailbox shapes must be accepted here. Folder producers build
        // `ErrorScope::Cursor(Folder(id))`, not `ErrorScope::Mailbox { id }` -
        // and IMAP's `mailbox_throttle`, the only production emitter of
        // `ThrottleScope::Mailbox` in the workspace, fires for either. Matching
        // only the latter made `ThrottleKey::Mailbox` unreachable in
        // production: every real per-folder `[LIMIT]` recorded no deadline at
        // all, so the poll loop, push reconciler and backfill runner kept
        // driving that folder at full cadence and re-earned the 429. The
        // synthetic `Mailbox { .. }` shape is built only by tests.
        ThrottleScope::Mailbox => match error.scope() {
            Some(ErrorScope::Mailbox { id }) => Some(ThrottleKey::Mailbox {
                account: account.clone(),
                mailbox: id.clone(),
            }),
            Some(ErrorScope::Cursor(CursorScope::Folder(folder))) => Some(ThrottleKey::Mailbox {
                account: account.clone(),
                mailbox: bifrost_types::MailboxId(folder.0.clone()),
            }),
            _ => None,
        },
        ThrottleScope::Account | ThrottleScope::Tenant => Some(account_key()),
        ThrottleScope::Provider => Some(
            error
                .provider()
                .map_or_else(account_key, ThrottleKey::Provider),
        ),
        // ThrottleScope is #[non_exhaustive]; new variants default to
        // "no bucket entry" (the conservative classification) and
        // require explicit handling here.
        _ => None,
    }
}

/// Record a `RetryAdvice`'s throttle deadline on the shared bucket,
/// resolving the key from the error's own identities. No-ops when the
/// advice carries no throttle scope or no retry hint, and when the
/// bucket mutex is poisoned (a throttle wait is advisory; panicking a
/// worker over it would trade a pause for an outage).
pub(crate) fn record_throttle(
    bucket: &std::sync::Mutex<ThrottleBucket>,
    account: &AccountId,
    advice: &RetryAdvice,
    error: &AccountError,
) {
    record_throttle_parts(
        bucket,
        account,
        advice.throttle_scope,
        advice.retry_hint,
        error,
    );
}

pub(crate) fn record_reconcile_throttle(
    bucket: &std::sync::Mutex<ThrottleBucket>,
    account: &AccountId,
    advice: &ReconcileAdvice,
    error: &AccountError,
) {
    record_throttle_parts(
        bucket,
        account,
        advice.throttle_scope,
        advice.retry_hint,
        error,
    );
}

fn record_throttle_parts(
    bucket: &std::sync::Mutex<ThrottleBucket>,
    account: &AccountId,
    throttle_scope: Option<ThrottleScope>,
    retry_hint: Option<bifrost_types::RetryHint>,
    error: &AccountError,
) {
    let Some(scope) = throttle_scope else {
        return;
    };
    let Some(hint) = retry_hint else {
        return;
    };
    let Some(key) = resolve_throttle_key(scope, account, error) else {
        return;
    };
    let wait_until = hint.not_before(SystemTime::now());
    if let Ok(mut guard) = bucket.lock() {
        guard.record_for(account, key, wait_until);
    }
}

/// Longest account-wide throttle wait currently pending for `account`,
/// pruning expired deadlines on the way. `None` when the account may
/// drive work now (including when the mutex is poisoned - see
/// [`record_throttle`]).
#[must_use]
pub(crate) fn account_throttle_wait(
    bucket: &std::sync::Mutex<ThrottleBucket>,
    account: &AccountId,
    now: SystemTime,
) -> Option<Duration> {
    let mut guard = bucket.lock().ok()?;
    guard.cleanup_expired(now);
    guard.wait_for_account(account, now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        AttemptCause, AuthCause, AuthErrorKind, Provider, RetryDisposition, RetryHint, RetryReason,
        ServerCause, ServerErrorKind, TransmissionState, TransportCause, TransportErrorKind,
        TransportKind,
    };

    /// The two contract violations say different things.
    ///
    /// They share a classification and a terminal outcome, and for a while they
    /// shared a message too - so a live batch carrying a BACKFILL checkpoint was
    /// reported as "a PageBoundary::Partial batch carried a checkpoint", sending
    /// whoever read the log after the wrong thing entirely. The boundary was fine;
    /// the checkpoint belonged to a lane that stream does not publish on.
    #[test]
    fn the_two_contract_violations_do_not_share_a_message() {
        let scope = CursorScope::Account;
        let boundary = format!(
            "{:?}",
            batch_boundary_violation(None, AccountOperation::SyncChanges, &scope).chain()
        );
        let backfill = format!(
            "{:?}",
            backfill_checkpoint_on_changes(None, AccountOperation::SyncChanges, &scope).chain()
        );
        assert_ne!(
            boundary, backfill,
            "an operator has to be able to tell a bad boundary from a checkpoint on the \
             wrong lane"
        );
        assert!(
            backfill.contains("backfill checkpoint"),
            "and the message must name what actually happened: {backfill}"
        );
    }

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
    fn throttle_bucket_shared_key_crosses_enrolled_accounts() {
        // A cross-account key is a single map entry; every account
        // enrolled in it observes a deadline any of them records.
        // Enrollment is a PRECONDITION, and it only happens when an
        // account's own error stream names the identity - so the very
        // first provider-wide deadline is invisible to a sibling that
        // has never failed. Attach-time enrollment needs a provider
        // identity channel that does not exist yet.
        let mut bucket = ThrottleBucket::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let a = AccountId("acc-1".into());
        let b = AccountId("acc-2".into());
        let key = ThrottleKey::Provider(Provider::Microsoft);
        // Both accounts have observed the provider identity at some
        // point (enrollment); only `a` records the live deadline.
        bucket.record_for(&b, key.clone(), now);
        bucket.record_for(&a, key, now + Duration::from_secs(10));

        assert_eq!(
            bucket.wait_for_account(&a, now),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            bucket.wait_for_account(&b, now),
            Some(Duration::from_secs(10))
        );
        // An account never enrolled sees nothing.
        assert_eq!(
            bucket.wait_for_account(&AccountId("acc-3".into()), now),
            None
        );
    }

    #[test]
    fn wait_for_account_takes_the_longest_applicable_wait() {
        let mut bucket = ThrottleBucket::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let a = AccountId("acc-1".into());
        bucket.record_for(
            &a,
            ThrottleKey::Account(a.clone()),
            now + Duration::from_secs(3),
        );
        bucket.record_for(
            &a,
            ThrottleKey::Provider(Provider::Microsoft),
            now + Duration::from_secs(8),
        );
        assert_eq!(
            bucket.wait_for_account(&a, now),
            Some(Duration::from_secs(8))
        );
    }

    #[test]
    fn wait_for_account_excludes_mailbox_keys() {
        // A per-mailbox throttle must not pause the whole account.
        let mut bucket = ThrottleBucket::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let a = AccountId("acc-1".into());
        bucket.record_for(
            &a,
            ThrottleKey::Mailbox {
                account: a.clone(),
                mailbox: MailboxId("shared@example.com".into()),
            },
            now + Duration::from_secs(30),
        );
        assert_eq!(bucket.wait_for_account(&a, now), None);
    }

    #[test]
    fn membership_survives_cleanup() {
        // Enrollment is identity, not a deadline: after the deadline
        // expires and cleanup prunes it, a NEW deadline recorded by a
        // sibling still reaches the enrolled account.
        let mut bucket = ThrottleBucket::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let a = AccountId("acc-1".into());
        let key = ThrottleKey::Provider(Provider::Microsoft);
        bucket.record_for(&a, key.clone(), now + Duration::from_secs(1));
        bucket.cleanup_expired(now + Duration::from_secs(5));
        // Sibling records without enrolling `a` again.
        bucket.record(key, now + Duration::from_secs(60));
        assert!(
            bucket
                .wait_for_account(&a, now + Duration::from_secs(5))
                .is_some()
        );
    }

    fn throttled_error(scope: Option<ErrorScope>, provider: Option<Provider>) -> AccountError {
        let mut builder = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        )
        .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
        .operation(AccountOperation::SyncChanges);
        if let Some(scope) = scope {
            builder = builder.scope(scope);
        }
        if let Some(provider) = provider {
            builder = builder.provider(provider);
        }
        builder.try_build().expect("valid")
    }

    #[test]
    fn reconcile_throttle_is_recorded_for_the_account() {
        let account = AccountId("a".into());
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited {
                retry_hint: Some(RetryHint::After(Duration::from_secs(30))),
            }),
        )
        .operation(AccountOperation::Send)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::InFlight,
        )))
        .throttle_scope(ThrottleScope::Account)
        .try_build()
        .expect("valid");
        let RecoveryClass::Reconcile(advice) = error.recovery() else {
            panic!("in-flight send must reconcile");
        };
        let bucket = std::sync::Mutex::new(ThrottleBucket::new());

        record_reconcile_throttle(&bucket, &account, advice, &error);

        assert!(account_throttle_wait(&bucket, &account, SystemTime::now()).is_some());

        // The drive loop and the push reconciler sleep this before
        // probing; the provider hint must win over the 1s fallback or
        // the read-back walks straight back into the rate limit.
        let delay = reconcile_delay(advice, SystemTime::now(), Duration::from_secs(1));
        assert!(
            delay > Duration::from_secs(25),
            "throttled reconcile must honor the provider hint, got {delay:?}"
        );
    }

    #[test]
    fn reconcile_delay_falls_back_when_no_hint_is_carried() {
        // A transport drop mid-send reconciles without any provider
        // timing signal; the caller's fallback is what must apply.
        let error = build_reconcile();
        let RecoveryClass::Reconcile(advice) = error.recovery() else {
            panic!("in-flight non-idempotent transport drop must reconcile");
        };
        assert_eq!(advice.retry_hint, None);

        assert_eq!(
            reconcile_delay(advice, SystemTime::now(), Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn resolve_throttle_key_current_operation_does_not_enter_bucket() {
        let account = AccountId("a".into());
        let error = throttled_error(None, None);
        assert!(resolve_throttle_key(ThrottleScope::CurrentOperation, &account, &error).is_none());
    }

    #[test]
    fn resolve_throttle_key_reads_identities_from_the_error() {
        let account = AccountId("a".into());
        let mailbox_err = throttled_error(
            Some(ErrorScope::Mailbox {
                id: "shared@example.com".into(),
            }),
            None,
        );
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Mailbox, &account, &mailbox_err),
            Some(ThrottleKey::Mailbox {
                account: account.clone(),
                mailbox: MailboxId("shared@example.com".into()),
            })
        );
        let provider_err = throttled_error(None, Some(Provider::Microsoft));
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Provider, &account, &provider_err),
            Some(ThrottleKey::Provider(Provider::Microsoft))
        );
    }

    /// The shape production actually produces.
    ///
    /// The test above builds `ErrorScope::Mailbox { .. }`, which only tests
    /// construct: IMAP's `with_mailbox` is annotated production-dead, and every
    /// real folder producer goes through `with_folder_scope`, yielding
    /// `Cursor(Folder(_))`. IMAP's `mailbox_throttle` is the only production
    /// emitter of `ThrottleScope::Mailbox` anywhere in the workspace and it
    /// fires for both shapes, so a reader accepting only the synthetic one
    /// degraded on every real per-folder throttle and on none of the tested
    /// ones - the bucket entry was simply never recorded.
    #[test]
    fn a_folder_scoped_throttle_keys_the_mailbox_bucket() {
        let account = AccountId("a".into());
        let folder_err = throttled_error(
            Some(ErrorScope::Cursor(CursorScope::Folder(
                bifrost_types::FolderId("INBOX/shared".into()),
            ))),
            None,
        );
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Mailbox, &account, &folder_err),
            Some(ThrottleKey::Mailbox {
                account,
                mailbox: MailboxId("INBOX/shared".into()),
            }),
            "a folder-scoped per-mailbox throttle must record a mailbox bucket \
             entry, not fall through to no entry at all"
        );
    }

    #[test]
    fn broader_throttle_keys_degrade_toward_the_account_key() {
        // Tenant always degrades today because the error contract has no
        // tenant identity channel. A provider throttle with no provider
        // identity follows the same conservative subset rule.
        let account = AccountId("a".into());
        let bare = throttled_error(None, None);
        let account_key = Some(ThrottleKey::Account(account.clone()));
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Tenant, &account, &bare),
            account_key
        );
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Mailbox, &account, &bare),
            None
        );
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Provider, &account, &bare),
            account_key
        );
        assert_eq!(
            resolve_throttle_key(ThrottleScope::Account, &account, &bare),
            account_key
        );
    }
}
