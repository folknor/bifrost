//! The Graph `/subscriptions` webhook arm: per-scope resource construction,
//! subscribe (attach to a shared row or create it, with rollback), and
//! release (`push_unsubscribe` plus `close()`'s walk).
//!
//! Rows belong to the factory-shared store in `super::registry`; a handle
//! holds leases. The renewal session that keeps rows alive lives in
//! `super::renewal`.

use std::collections::BTreeMap;

use bifrost_types::{AccountError, AccountOperation, CursorScope, ErrorScope, SubscriptionHandle};

use crate::account::GraphAccount;
use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::webhooks::{
    create_subscription, delete_subscription, renew_subscription, subscription_is_gone,
};

use super::common::{
    DecodedHandle, account_closed_error, decode_handle, graph_handle, new_handle_token,
    unsupported_push_error, unsupported_push_scope_error,
};
use super::dispatch::{ArmOutcome, finalize_push_outcomes};
use super::registry::{Exchange, RowStatus};

type Grouped = BTreeMap<String, Vec<(bifrost_types::SubmissionIndex, CursorScope)>>;

/// The webhook arm of `push_subscribe`: lease one row per resource, close the
/// ledger, and only then commit the leases and wake the renewal session.
///
/// The ledger is closed HERE, between the last row step and the commit, not
/// by the dispatcher afterwards. `finalize` is fallible (an unfiled or
/// double-filed lane id is this crate's invariant breaking); closed before the
/// commit, its failure path has leased nothing and only has to roll back the
/// rows this request created.
///
/// The row steps, rollback and commit run in a spawned task so a dropped
/// caller cannot strand them; see the comments at the spawn and on
/// `run_subscribe`.
pub(super) async fn subscribe_graph(
    account: GraphAccount,
    eligible: Vec<(
        bifrost_types::SubmissionIndex,
        CursorScope,
        crate::account::scope::GraphScope,
    )>,
    mut outcomes: bifrost_types::BatchOutcomeBuilder<CursorScope, bifrost_types::SubmissionIndex>,
    expected: &[bifrost_types::SubmissionIndex],
) -> Result<ArmOutcome, AccountError> {
    let Some(endpoint) = account.push_config.endpoint().cloned() else {
        return Err(unsupported_push_error());
    };
    // Graph webhooks reject any subscription whose `resource` we cannot
    // construct. A scope with no resource is refused into the failed lane,
    // where the caller sees exactly which scope was declined and keeps
    // polling coverage for it, while its subscribable siblings still get a
    // live subscription.
    let mut grouped: Grouped = BTreeMap::new();
    for (item, scope, parsed) in eligible {
        match parsed.subscription_resource(&account) {
            Ok(Some(resource)) => {
                grouped.entry(resource).or_default().push((item, scope));
            }
            Ok(None) => outcomes.push_failed(item, unsupported_push_scope_error(scope)),
            Err(error) => {
                // A stale shared mailbox has no subscribable resource.
                outcomes.push_failed(
                    item,
                    into_account_error(
                        error,
                        GraphErrorContext::graph(AccountOperation::PushSubscribe)
                            .with_scope(ErrorScope::Cursor(scope)),
                    ),
                );
            }
        }
    }
    if grouped.is_empty() {
        return Ok((None, finalize_push_outcomes(outcomes, expected)?));
    }

    // Mint the handle's random token BEFORE the first request.
    // `new_handle_token` is fallible (the host entropy source can fail; that
    // is `Internal(RuntimeFailure)`, never retried), and a failure here has
    // written nothing. The handle itself is assembled after the row steps,
    // because it embeds the server ids (see `common::decode_handle`).
    let token = new_handle_token()?;

    // A closed account must not gain leases. Refusing here, before the first
    // request, keeps the common case (subscribing on a dead account) from
    // touching the server at all; the task re-checks under the exchange and
    // again right before the commit.
    if account.shutdown.is_cancelled() {
        return Err(account_closed_error());
    }

    // Everything from here on runs in a task the ACCOUNT's runtime owns, and
    // this future only waits on it. `push_subscribe` is a caller-visible
    // future and callers drop those (a timeout, a `select!`, an aborted
    // reattach); dropped mid-way, a body that ran inline would strand every
    // server-side create made so far, skip the rollback DELETEs, or leave a
    // committed lease whose handle nobody received. A spawned task is not torn
    // down by its awaiter going away, so the steps, the rollback and the
    // commit each run to completion.
    //
    // Nothing is spawned, and nothing has been written, until this future is
    // first polled and reaches this point, so a future dropped before its
    // first poll has nothing to leak: the guard is the spawn itself, not an
    // object constructed inside the async body.
    //
    // The hand-off is two-phase because a delivered result is not a received
    // one: a `oneshot` send succeeds while the receiver is alive even if the
    // receiver's future is then dropped without ever being polled again, and
    // the value dies with it. The waiter therefore acks, in the same
    // synchronous step that takes the result, and the task treats a missing
    // ack (or a failed send) as "the handle was never received" and releases
    // it. Exactly one side ever cleans up.
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(run_subscribe(
        account,
        SubscribeRequest {
            endpoint,
            grouped,
            token,
            outcomes,
            expected: expected.to_vec(),
        },
        result_tx,
        ack_rx,
    ));
    let Ok(result) = result_rx.await else {
        // The task ended without answering: it panicked, or the runtime is
        // shutting down under it. Nothing more can be done from here.
        return Err(into_account_error(
            crate::error::GraphError::RuntimeFailure {
                message: "the push_subscribe task ended without an answer".to_string(),
            },
            GraphErrorContext::graph(AccountOperation::PushSubscribe),
        ));
    };
    // No await between taking the result and acking it: there is no drop
    // point between "received" and "acknowledged".
    let _ = ack_tx.send(());
    result
}

/// Everything the spawned subscribe task owns.
struct SubscribeRequest {
    endpoint: super::common::PushEndpoint,
    grouped: Grouped,
    /// The random part of the handle, minted before any request.
    token: String,
    outcomes: bifrost_types::BatchOutcomeBuilder<CursorScope, bifrost_types::SubmissionIndex>,
    expected: Vec<bifrost_types::SubmissionIndex>,
}

/// The spawned half of the webhook `push_subscribe`: lease, commit, and hand
/// the result to the waiter, releasing the handle again if the waiter never
/// took it.
///
/// What cannot be made safe, and why:
/// - A create whose response never arrived (a timeout, a reset) may have
///   succeeded on Graph. The subscription id exists only in the response we
///   did not read, so there is nothing to DELETE by; it lives until Graph's
///   own expiry (up to seven days, as requested for basic notifications) and
///   posts to the receiver meanwhile, and a later create for that resource
///   answers 409. Recovering it would mean listing `/subscriptions` and
///   deleting by resource and notification URL, which could also delete
///   another process's identical subscription; that is a product decision,
///   not something to do silently here.
/// - A process exit or runtime shutdown kills this task like any other, with
///   the same result for whatever it had created.
async fn run_subscribe(
    account: GraphAccount,
    request: SubscribeRequest,
    result_tx: tokio::sync::oneshot::Sender<Result<ArmOutcome, AccountError>>,
    ack_rx: tokio::sync::oneshot::Receiver<()>,
) {
    // The exchange is released inside `create_and_register`, before this
    // waits for the ack: an abandoned or never-polled caller must not hold up
    // every other row-lifecycle step of the factory.
    let result = create_and_register(&account, request).await;
    // Only a success committed leases, and the success names its handle.
    let Some(handle) = result.as_ref().ok().and_then(|(handle, _)| handle.clone()) else {
        // Nothing committed, so nothing to release; a dropped waiter has
        // nobody to tell.
        let _ = result_tx.send(result);
        return;
    };
    let received = result_tx.send(result).is_ok() && ack_rx.await.is_ok();
    if received {
        return;
    }
    // The leases are committed and renewable, but no caller holds the
    // handle, so no `push_unsubscribe` will ever name it. Release it the way
    // a caller would. A failed DELETE condemns the lease, which is exactly
    // what lets `close()` retry it.
    if let Err(error) = unsubscribe_graph(account, handle).await {
        let telemetry = error.telemetry_fields();
        tracing::warn!(
            target: "bifrost_graph::push",
            message_key = telemetry.message_key,
            recovery = telemetry.recovery_discriminant,
            "could not release a Graph webhook subscription whose subscribe was abandoned"
        );
    }
}

/// How one resource step of a subscribe failed.
enum StepError {
    /// The POST for this resource was refused. A permission refusal is the
    /// resource's lane failure; anything else fails the whole request.
    Create(AccountError),
    /// A prerequisite (the DELETE of an unowned row, the recovery PATCH of a
    /// terminated one) failed. Always whole-request: the permission laning
    /// rationale is about a standing refusal to SUBSCRIBE, which a failed
    /// prerequisite does not show.
    Prerequisite(AccountError),
}

/// What one resource step leaves for the commit.
struct Leased {
    resource: String,
    covered: Vec<(bifrost_types::SubmissionIndex, CursorScope)>,
    /// The row was recovered from a terminal failure for this request, so
    /// the new lease is owed a gap like every other holder.
    recovered: bool,
}

async fn create_and_register(
    account: &GraphAccount,
    request: SubscribeRequest,
) -> Result<ArmOutcome, AccountError> {
    let SubscribeRequest {
        endpoint,
        grouped,
        token,
        mut outcomes,
        expected,
    } = request;
    let expected = expected.as_slice();
    let store = std::sync::Arc::clone(account.webhooks.store());
    // The whole multi-resource transaction holds the exchange until its
    // leases are committed or rolled back, so no other subscriber can attach
    // to a row this request created and rollback may treat such a row as its
    // own.
    let exchange = store.exchange().await;
    // Acquisition fence: a `close()` that cancelled while this waited for the
    // exchange has walked, or will walk, its leases; this must not add one.
    if account.shutdown.is_cancelled() {
        return Err(account_closed_error());
    }
    let mut created: Vec<String> = Vec::new();
    let mut leased: Vec<Leased> = Vec::new();
    for (resource, covered) in grouped {
        match lease_row(account, &exchange, &endpoint, &resource).await {
            Ok(step) => {
                if step == RowStep::Created {
                    created.push(resource.clone());
                }
                for (item, scope) in &covered {
                    outcomes.push_succeeded(*item, scope.clone());
                }
                leased.push(Leased {
                    resource,
                    covered,
                    recovered: step == RowStep::Recovered,
                });
            }
            Err(StepError::Create(error)) if is_standing_resource_refusal(&error) => {
                // A permission refusal names THIS resource: a token that can
                // list calendars with Calendars.ReadBasic still cannot
                // subscribe to `/events` without Calendars.Read. It is that
                // group's lane failure, so mail subscriptions created
                // beside it survive; an Outlook user missing calendar
                // consent still expects live mail. Only a permission refusal
                // is a lane failure: it stays true until consent changes,
                // so recording the scopes as unaccepted loses nothing,
                // whereas a transient failure filed per lane would drop
                // those scopes from the engine's accepted set and never be
                // retried. Everything else keeps the whole-request rollback.
                for (item, scope) in covered {
                    let scoped = error
                        .clone()
                        .into_builder()
                        .scope(bifrost_types::ErrorScope::Cursor(scope))
                        .try_build()
                        .unwrap_or_else(|_| error.clone());
                    outcomes.push_failed(item, scoped);
                }
            }
            Err(StepError::Create(error) | StepError::Prerequisite(error)) => {
                // No lease of this request is committed and no handle is
                // returned, so nothing downstream could reach a row this
                // request created. Cleanup is best effort: preserve the error
                // the caller needs to act on even if a DELETE fails (the row
                // then stays in the store as debt).
                //
                // This rolls back only what we hold ids for. THIS create may
                // itself have succeeded on Graph (a timeout after the server
                // acted): its id is unknowable, see `run_subscribe`.
                roll_back_created(account, &exchange, &created).await;
                return Err(error);
            }
        }
    }

    // Every lane must be filed before anything becomes live. See the
    // function doc: on this path nothing is leased, so rolling back the
    // creates is the whole cleanup.
    let outcomes = match finalize_push_outcomes(outcomes, expected) {
        Ok(outcomes) => outcomes,
        Err(error) => {
            roll_back_created(account, &exchange, &created).await;
            return Err(error);
        }
    };
    // Every group refused on permission: the handle-less outcome, which the
    // dispatcher turns into the whole-request refusal carrying each cause.
    if leased.is_empty() {
        return Ok((None, outcomes));
    }

    store.pass_commit_gate().await;
    // Second acquisition fence, with no await between it and the commit: a
    // `close()` that cancelled while the requests were in flight either sees
    // these leases on its walk (it waits for the exchange) or this refuses.
    if account.shutdown.is_cancelled() {
        roll_back_created(account, &exchange, &created).await;
        return Err(account_closed_error());
    }
    let ids: Vec<String> = leased
        .iter()
        .filter_map(|leased| store.server_id(&exchange, &leased.resource))
        .collect();
    // Mint before the commit; the handle carries the server ids.
    let handle = graph_handle(
        &token,
        own_binding(account).as_deref(),
        ids.iter().map(String::as_str),
    );
    let mut leases = Vec::with_capacity(leased.len());
    for Leased {
        resource,
        covered,
        recovered,
    } in leased
    {
        leases.push((
            resource,
            covered.into_iter().map(|(_, scope)| scope).collect(),
            recovered,
        ));
    }
    store.commit_leases(&exchange, account.webhooks.sink(), &handle, leases);
    drop(exchange);
    Ok((Some(handle), outcomes))
}

/// What a resource step did to the row it leases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowStep {
    /// A healthy row another lease already holds: no request was sent.
    Attached,
    /// A fresh row this request created, provisional until the commit.
    Created,
    /// A terminated row brought back for this request.
    Recovered,
}

fn push_error(error: crate::error::GraphError) -> AccountError {
    into_account_error(
        error,
        GraphErrorContext::graph(AccountOperation::PushSubscribe),
    )
}

fn release_error(error: crate::error::GraphError) -> AccountError {
    into_account_error(
        error,
        GraphErrorContext::graph(AccountOperation::PushUnsubscribe),
    )
}

/// Make `resource` leasable for one subscribe, under the exchange.
///
/// A healthy shared row is attached to as it is: Graph would refuse a second
/// subscription for the resource, and the row already delivers to the same
/// receiver. A terminated row is recovered first (a lease must not be handed
/// coverage nobody is renewing): a PATCH that succeeds revives it, a 404/410
/// means it is gone and a fresh create replaces it. A row nobody holds live
/// (only condemned leases, or a failed rollback's debt) may or may not still
/// exist on Graph, so it is DELETEd before a fresh create; creating beside it
/// would answer 409.
async fn lease_row(
    account: &GraphAccount,
    exchange: &Exchange,
    endpoint: &super::common::PushEndpoint,
    resource: &str,
) -> Result<RowStep, StepError> {
    let store = account.webhooks.store();
    let client = account.mailboxes.primary();
    match store.status(exchange, resource) {
        RowStatus::Live => Ok(RowStep::Attached),
        RowStatus::Terminated { server_id } => {
            match renew_subscription(client, &server_id, None).await {
                Ok(expires_at) => {
                    store.recover(exchange, resource, None, expires_at);
                    Ok(RowStep::Recovered)
                }
                Err(error) if subscription_is_gone(&error) => {
                    let response = create_subscription(
                        client,
                        resource,
                        &endpoint.webhook_url,
                        &endpoint.client_state,
                        None,
                    )
                    .await
                    .map_err(|error| StepError::Create(push_error(error)))?;
                    store.recover(
                        exchange,
                        resource,
                        Some(response.id),
                        response.expiration_date_time,
                    );
                    Ok(RowStep::Recovered)
                }
                Err(error) => Err(StepError::Prerequisite(push_error(error))),
            }
        }
        RowStatus::Unowned { server_id } => {
            delete_subscription(client, &server_id)
                .await
                .map_err(|error| StepError::Prerequisite(release_error(error)))?;
            store.remove_row(exchange, resource);
            create_row(account, exchange, endpoint, resource).await
        }
        RowStatus::Absent => create_row(account, exchange, endpoint, resource).await,
    }
}

async fn create_row(
    account: &GraphAccount,
    exchange: &Exchange,
    endpoint: &super::common::PushEndpoint,
    resource: &str,
) -> Result<RowStep, StepError> {
    let response = create_subscription(
        account.mailboxes.primary(),
        resource,
        &endpoint.webhook_url,
        &endpoint.client_state,
        None,
    )
    .await
    .map_err(|error| StepError::Create(push_error(error)))?;
    account.webhooks.store().insert_row(
        exchange,
        resource.to_string(),
        response.id,
        response.expiration_date_time,
    );
    Ok(RowStep::Created)
}

/// Whether a subscription create was refused for lack of permission on its
/// resource, a standing fact rather than a transport, throttle or server
/// fault.
///
/// A 409 duplicate-subscription refusal is deliberately NOT laned. Rows are
/// shared through the store, so a 409 now means a row this store does not
/// know (left by a previous process, or by a second factory for the same
/// credentials). Laning it would report the scopes as refused while that
/// unknown row may still be delivering; whole-request failure keeps the
/// caller's earlier coverage (a reopen's old handle) installed instead.
///
/// This trusts the classifier's `PermissionDenied`. A Conditional Access
/// claims challenge (403 with a Bearer `insufficient_claims` challenge) is
/// recoverable by re-authenticating, not by consent; the classifier maps it to
/// `Authentication(ReauthorizationRequired)`, so it is never laned here and
/// fails the whole request instead.
fn is_standing_resource_refusal(error: &AccountError) -> bool {
    matches!(
        error.kind(),
        bifrost_types::AccountErrorKind::Authorization(
            bifrost_types::AccessErrorKind::PermissionDenied
        )
    )
}

/// Best-effort DELETE of the rows this request created and will never lease.
///
/// A DELETE that fails leaves the row in the store with no lease: cleanup
/// debt, which the next subscribe for that resource retries before anything
/// is created beside it. Failures are logged, not returned: the caller needs
/// the error that failed the request.
async fn roll_back_created(account: &GraphAccount, exchange: &Exchange, created: &[String]) {
    let store = account.webhooks.store();
    for resource in created {
        let Some(server_id) = store.server_id(exchange, resource) else {
            continue;
        };
        match delete_subscription(account.mailboxes.primary(), &server_id).await {
            Ok(()) => store.remove_row(exchange, resource),
            Err(cleanup_error) => {
                tracing::warn!(
                    target: "bifrost_graph::webhooks",
                    server_id = %server_id,
                    error = ?cleanup_error,
                    "failed to roll back Graph webhook subscription; kept as cleanup debt"
                );
            }
        }
    }
}

/// The account binding this instance mints into and checks against handles:
/// its engine `AccountId`, when an `AccountNet` is attached (always, in
/// production). With none attached, a bound `graph2:` handle matches nothing
/// and is left alone - the fail-safe direction, since this instance cannot
/// prove the handle is its own.
fn own_binding(account: &GraphAccount) -> Option<String> {
    account
        .mailboxes
        .primary()
        .account_net()
        .map(|net| super::common::account_binding(net.account()))
}

/// `push_unsubscribe` for a webhook handle: release its leases.
///
/// Runs in an owned task holding the exchange through every DELETE and its
/// local commit, so a dropped caller cannot leave a DELETE that succeeded
/// unrecorded or a failed one unflagged.
pub(super) async fn unsubscribe_graph(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), AccountError> {
    let task = tokio::spawn(async move {
        let store = std::sync::Arc::clone(account.webhooks.store());
        let exchange = store.exchange().await;
        release_locked(&account, &exchange, &handle).await
    });
    task.await.unwrap_or_else(|_| {
        Err(push_error(crate::error::GraphError::RuntimeFailure {
            message: "the push_unsubscribe task ended without an answer".to_string(),
        }))
    })
}

/// Release every lease `handle` holds, under the exchange.
///
/// A row some other live lease still holds keeps delivering: only this lease
/// goes. A row left with no other live holder is DELETEd (404/410 count as
/// gone) and forgotten with every condemned lease on it; a failed DELETE
/// condemns this lease so a retry or `close()` can finish. Every row is
/// attempted and the first failure returned.
///
/// A handle that holds no lease is an orphan from an earlier process (or one
/// already released): the handle itself names the server ids, and only ids no
/// row in the store holds are DELETEd, so a repeated release, or a stale or
/// forged handle, cannot tear down a row another lease renews. A handle bound
/// to another account deletes nothing: its ids are reachable with this
/// account's credentials when both sit in one tenant.
async fn release_locked(
    account: &GraphAccount,
    exchange: &Exchange,
    handle: &SubscriptionHandle,
) -> Result<(), AccountError> {
    let decoded = decode_handle(handle);
    if let DecodedHandle::Graph {
        account: Some(bound),
        ..
    } = &decoded
        && own_binding(account).as_deref() != Some(bound.as_str())
    {
        tracing::warn!(
            target: "bifrost_graph::push",
            "push_unsubscribe was handed a webhook handle minted by another account; \
             nothing deleted"
        );
        return Ok(());
    }
    let store = account.webhooks.store();
    let client = account.mailboxes.primary();
    let resources = store.leased_by(handle);
    let mut first_error = None;
    if resources.is_empty() {
        let DecodedHandle::Graph { ids, .. } = decoded else {
            return Ok(());
        };
        for server_id in ids {
            if store.holds_id(exchange, &server_id) {
                continue;
            }
            if let Err(error) = delete_subscription(client, &server_id).await {
                first_error.get_or_insert_with(|| release_error(error));
            }
        }
        return first_error.map_or(Ok(()), Err);
    }
    for resource in resources {
        if store.shared_with_others(exchange, &resource, handle) {
            store.remove_lease(exchange, &resource, handle);
            continue;
        }
        let Some(server_id) = store.server_id(exchange, &resource) else {
            continue;
        };
        match delete_subscription(client, &server_id).await {
            Ok(()) => store.remove_row(exchange, &resource),
            Err(error) => {
                store.condemn(exchange, &resource, handle);
                first_error.get_or_insert_with(|| release_error(error));
            }
        }
    }
    account.webhooks.changed();
    first_error.map_or(Ok(()), Err)
}

/// Best-effort release of every lease this account holds, for `close()`.
///
/// A Graph subscription lives on the server for up to its expiry and keeps
/// POSTing to the consumer's HTTPS receiver whether or not this process still
/// exists. Nothing in the `Account` contract promises `push_unsubscribe`
/// before `close()`, so `close()` has to do it. A row another account of the
/// same factory still holds live (the replacement of a reopen) keeps
/// delivering; only this account's leases go.
///
/// Failures are logged, not returned: `close()` must still settle its
/// account-owned push tasks, and the engine has no useful recovery for "the
/// server kept a subscription we asked it to drop".
pub(crate) async fn retire_all_graph_subscriptions(account: &GraphAccount) {
    // Cancel BEFORE taking the exchange: a subscribe checks the token after
    // it holds the exchange and again just before its commit, so its leases
    // either precede this walk (and are released below) or are refused.
    // `close()` relies on this being its only cancel.
    account.shutdown.cancel();
    let account = account.clone();
    let task = tokio::spawn(async move {
        let store = std::sync::Arc::clone(account.webhooks.store());
        let exchange = store.exchange().await;
        // Rows nobody holds live any more (a failed rollback's debt, or
        // leases whose account was dropped unclosed) have no handle anyone
        // will release; Graph keeps delivering them until expiry. Any closing
        // account of the factory retries their DELETE, once. Taken before
        // this account's own releases, which already attempt their rows. A
        // subscribe's provisional rows are never seen here: it holds the
        // exchange.
        let unowned = store.unowned(&exchange, account.webhooks.instance());
        for handle in account.webhooks.handles() {
            if let Err(error) = release_locked(&account, &exchange, &handle).await {
                let telemetry = error.telemetry_fields();
                tracing::warn!(
                    target: "bifrost_graph::push",
                    message_key = telemetry.message_key,
                    recovery = telemetry.recovery_discriminant,
                    "close() could not retire a Graph webhook subscription"
                );
            }
        }
        for resource in unowned {
            let Some(server_id) = store.server_id(&exchange, &resource) else {
                continue;
            };
            match delete_subscription(account.mailboxes.primary(), &server_id).await {
                Ok(()) => store.remove_row(&exchange, &resource),
                Err(error) => tracing::warn!(
                    target: "bifrost_graph::push",
                    server_id = %server_id,
                    error = ?error,
                    "close() could not retire an unowned Graph webhook subscription"
                ),
            }
        }
    });
    let _ = task.await;
}

#[cfg(test)]
pub(super) fn resource_for_scope(
    account: &GraphAccount,
    scope: &CursorScope,
) -> Result<Option<String>, crate::error::GraphError> {
    match crate::account::scope::GraphScope::parse(scope) {
        Ok(parsed) => parsed.subscription_resource(account),
        Err(_) => Ok(None),
    }
}
