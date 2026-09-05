//! The `Account` push doors: per-scope eligibility, mode dispatch, and the
//! three-lane outcome ledger every subscription request answers with.

use bifrost_types::{
    AccountError, AccountOperation, CursorScope, ProtocolErrorKind, SubscriptionHandle,
};

use crate::account::graph_error::{invalid_account_error, protocol_violation};
use crate::account::{GraphAccount, PushMode};

use super::common::{unsupported_push_error, unsupported_push_scope_error};
use super::ews::subscribe_ews;
use super::ews::unsubscribe_ews;
use super::webhook::{subscribe_graph, unsubscribe_graph};

pub(crate) async fn push_subscribe(
    account: GraphAccount,
    scopes: Vec<CursorScope>,
) -> Result<bifrost_types::PushSubscription, AccountError> {
    // A subscription covering nothing is not a subscription. Registering it
    // anyway minted a handle over an empty group: teardown had no server id
    // to walk, so the group was never retired and the renewal worker it
    // started was never stopped. The engine already skips an empty scope
    // list at its own reattach boundary, so this can only be a caller bug.
    if scopes.is_empty() {
        return Err(invalid_account_error(
            AccountOperation::PushSubscribe,
            "push_subscribe requires at least one scope",
        ));
    }
    let expected = push_item_ids(scopes.len());
    let mut outcomes = bifrost_types::BatchOutcomeBuilder::new();
    let mut eligible = Vec::with_capacity(scopes.len());
    for (item, scope) in expected.iter().cloned().zip(scopes) {
        // Public folders are poll-only in v1: a bare `CursorScope::Folder`
        // has no push surface (EWS streaming notifications do not cover the
        // public-folder hierarchy mailbox). It is refused per scope, not per
        // request: one stale or poll-only scope in a mixed list must not
        // disable push for every valid sibling, which is what bailing out
        // before dispatch did.
        if matches!(scope, CursorScope::Folder(_)) {
            outcomes.push_failed(item, unsupported_push_scope_error(scope));
            continue;
        }
        eligible.push((item, scope));
    }
    // Nothing in the request was ever subscribable. There is no partial
    // success to report and no handle to hold, so this stays a whole-request
    // refusal the caller can act on rather than an empty success.
    if eligible.is_empty() {
        return Err(unsupported_push_error());
    }
    let handle = match account.push_mode {
        PushMode::GraphSubscriptions => subscribe_graph(account, eligible, &mut outcomes).await?,
        PushMode::EwsStreaming => subscribe_ews(account, eligible, &mut outcomes).await?,
    };
    Ok(bifrost_types::PushSubscription::new(
        handle,
        finalize_push_outcomes(outcomes, &expected)?,
    ))
}

/// Positional ids for one `push_subscribe` request's scopes. The lanes are
/// keyed on submission position because `CursorScope` is not an id and two
/// requested scopes can legitimately name the same folder.
pub(super) fn push_item_ids(len: usize) -> Vec<bifrost_types::BatchItemId> {
    (0..len)
        .map(|index| bifrost_types::BatchItemId(index.to_string()))
        .collect()
}

fn finalize_push_outcomes(
    outcomes: bifrost_types::BatchOutcomeBuilder<CursorScope>,
    expected: &[bifrost_types::BatchItemId],
) -> Result<bifrost_types::BatchOutcome<CursorScope>, AccountError> {
    outcomes.finalize(expected).map_err(|error| {
        protocol_violation(
            ProtocolErrorKind::ContractViolation,
            AccountOperation::PushSubscribe,
            None,
            format!("push scope accounting invariant failed: {error}"),
        )
    })
}

pub(crate) async fn push_unsubscribe(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), AccountError> {
    match account.push_mode {
        PushMode::GraphSubscriptions => unsubscribe_graph(account, handle).await,
        PushMode::EwsStreaming => unsubscribe_ews(account, handle).await,
    }
}
