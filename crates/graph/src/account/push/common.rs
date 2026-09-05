//! The pieces both push arms and the dispatcher share: the webhook endpoint
//! configuration, the two `Unsupported(PushSubscribe)` refusals, and the
//! subscription handle mint.

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, CursorScope,
    ErrorScope, Protocol, Provider, RequestCause, SubscriptionHandle, TransmissionState,
};

use crate::account::graph_error::{GraphErrorContext, into_account_error};

#[derive(Debug, Clone)]
pub(crate) struct PushEndpoint {
    pub(crate) webhook_url: String,
    /// A consumer-owned account-wide secret carried in every Graph webhook
    /// subscription so its out-of-process receiver can validate clientState.
    ///
    /// Mandatory. The alternative was a per-resource random value minted
    /// inside `create_subscription` and dropped on the floor, which produced
    /// subscriptions no receiver could authenticate - a secret nobody holds
    /// is not a secret, it is an unvalidated webhook with a field filled in.
    pub(crate) client_state: String,
}

pub(super) fn unsupported_push_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe),
        Cause::Request(RequestCause::Unsupported {
            operation: AccountOperation::PushSubscribe,
        }),
    )
    .operation(AccountOperation::PushSubscribe)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .try_build()
    .expect("valid account error classification")
}

/// The same refusal, correlated to the scope that caused it. A per-scope lane
/// entry the caller cannot attribute is not a report it can act on: it would
/// know a scope was declined without knowing which registration to drop.
pub(super) fn unsupported_push_scope_error(scope: CursorScope) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe),
        Cause::Request(RequestCause::Unsupported {
            operation: AccountOperation::PushSubscribe,
        }),
    )
    .operation(AccountOperation::PushSubscribe)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .scope(ErrorScope::Cursor(scope))
    .try_build()
    .expect("valid account error classification")
}

/// The whole-request refusal for a request whose scopes all died INSIDE an
/// arm: every resource unresolvable in `subscribe_graph`, or every id refused
/// or omitted by `translateExchangeIds`.
///
/// `Err(_)` from `push_subscribe` means nothing was subscribed, and the arms
/// used to answer that case with `Ok(PushSubscription)` carrying no handle and
/// an all-failed ledger - a shape the documented contract does not have and
/// which reads to a caller matching on `Ok` as a partial success with an
/// unusable handle.
///
/// The kind is DERIVED from the scopes rather than minted fresh. The jmap
/// precedent (`no_mappable_push_scopes`) answers `Request(Malformed)` from a
/// named constructor, and that is right there because its whole population has
/// exactly one failure mode - the scope names no JMAP data type, which is
/// always a caller-shape fault. These arms do not: a scope can die as
/// `Unsupported(PushSubscribe)` (no Graph resource for its shape), as
/// `Request(Malformed)` (a shared mailbox this account no longer configures,
/// or an id Graph declines to convert), as `Protocol(ContractViolation)`
/// (`translateExchangeIds` omitted an answer it owed), or as a transport
/// failure on the translation POST. Flattening a provider contract violation
/// or a retryable transport drop into `Request(Malformed)` would tell the
/// engine the CALLER is buggy and derive `ClientBug` for a fault the caller
/// cannot fix. So the first failure is promoted verbatim - it is already
/// classified, already scope-correlated - and every other failure's causes
/// ride along as secondary chain evidence, so the per-scope diagnosis the
/// ledger held is not lost with the ledger.
pub(super) fn no_subscribable_push_scopes(failed: &[bifrost_types::BatchFailure]) -> AccountError {
    let Some((first, rest)) = failed.split_first() else {
        // Unreachable through `push_subscribe`: an arm answers `None` only
        // when every eligible scope was filed on the failed lane. Answer the
        // uncorrelated refusal rather than panicking on a lane invariant that
        // `finalize` already enforces.
        return unsupported_push_error();
    };
    let mut builder = first.error.clone().into_builder();
    for failure in rest {
        for cause in failure.error.chain().iter() {
            builder = builder.push_cause(cause.clone());
        }
    }
    builder.try_build().unwrap_or_else(|_| first.error.clone())
}

/// Raise the account's push-down latch, emitting `Disconnected` on the edge.
pub(super) fn mark_push_disconnected(account: &crate::account::GraphAccount) {
    if !account
        .push_disconnected
        .swap(true, std::sync::atomic::Ordering::SeqCst)
    {
        let _ = account
            .push_tx
            .send(bifrost_types::WatchEvent::Disconnected);
    }
}

/// Clear the account's push-down latch, emitting `Reconnected` on the edge.
///
/// Edge-triggered on purpose: `Reconnected` costs the engine a full reconcile
/// of every registered cursor scope, so it is owed only where coverage was
/// actually lost and regained. A subscribe on a healthy account is not that
/// edge. Returns whether the event was published.
pub(super) fn mark_push_reconnected(account: &crate::account::GraphAccount) -> bool {
    if account
        .push_disconnected
        .swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        let _ = account.push_tx.send(bifrost_types::WatchEvent::Reconnected);
        return true;
    }
    false
}

/// Publish `Reconnected` unconditionally and clear the latch.
///
/// For the one caller that KNOWS a coverage gap happened without the latch
/// ever going up: the renewal worker's recreate of a subscription Graph had
/// already dropped. The resource had no live subscription between its
/// disappearance and the create, and Graph replays nothing for that window,
/// so the reconcile is owed even though no renewal tick reported a failure.
pub(super) fn announce_push_recovered(account: &crate::account::GraphAccount) {
    account
        .push_disconnected
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let _ = account.push_tx.send(bifrost_types::WatchEvent::Reconnected);
}

pub(super) fn new_handle() -> Result<SubscriptionHandle, AccountError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| {
        // RNG failure is a host-environment problem. Surface it as a
        // transport "Network" failure with `transmission_state:
        // Unsent` (no bytes left the process) so the recovery mapping
        // classifies it as a retryable client-side issue.
        let net = bifrost_net::Error::Network {
            message: format!("RNG failed: {error}"),
            transmission_state: TransmissionState::Unsent,
            source: None,
        };
        into_account_error(
            crate::error::GraphError::Net(net),
            GraphErrorContext::graph(AccountOperation::PushSubscribe),
        )
    })?;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    Ok(SubscriptionHandle(out))
}
