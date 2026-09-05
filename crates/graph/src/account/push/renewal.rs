//! The webhook renewal health worker: the tick loop that re-issues expiring
//! Graph subscriptions, recreates the ones Graph has already dropped, emits
//! `Disconnected` / `Reconnected` / `Terminated` on the push channel, and
//! retires its own worker slot when nothing is left to renew.

use std::collections::HashMap;
use std::time::Duration;

use bifrost_types::WatchEvent;
use bifrost_types::{AccountOperation, CursorScope, ErrorScope, SubscriptionHandle};

use crate::account::GraphAccount;
use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::webhooks::{
    ExpiryCheck, check_expiry, create_subscription, delete_subscription, renew_subscription,
    subscription_is_gone,
};

use super::common::PushEndpoint;
use super::webhook::{GraphSubscriptionGroup, GraphSubscriptionState, remove_subscription_state};

pub(super) const RENEWAL_CHECK_INTERVAL: Duration = Duration::from_secs(10 * 60);
pub(super) const RENEWAL_THRESHOLD_MINUTES: i64 = 30;

pub(super) async fn run_graph_subscription_worker(account: GraphAccount) {
    let mut disconnected = false;
    loop {
        tokio::select! {
            () = account.shutdown.cancelled() => return,
            () = tokio::time::sleep(RENEWAL_CHECK_INTERVAL) => {}
        }

        let due = {
            let mut groups = account.graph_subscriptions.write().await;
            if !has_live_graph_subscription_group(&groups) {
                // Retire the slot BEFORE releasing the guard that made this
                // decision. A concurrent `push_subscribe` cannot install its
                // live group until this guard drops, so by the time its
                // `ensure_graph_worker` runs the slot is empty and it spawns a
                // replacement. Observing the emptiness here and letting the
                // task merely run out instead left a window where the new
                // subscription saw a still-unfinished `JoinHandle`, declined
                // to spawn, and was never renewed until some later subscribe.
                retire_graph_worker_slot(&account).await;
                return;
            }
            due_renewals(&mut groups)
        };

        let mut had_error = false;
        for DueRenewal {
            handle,
            server_id,
            resource,
            scopes,
        } in due
        {
            match renew_subscription(&account.client, &server_id, None).await {
                Ok(new_expiry) => {
                    let mut groups = account.graph_subscriptions.write().await;
                    if let Some(group) = groups.get_mut(&handle)
                        && let Some(state) = group
                            .subscriptions
                            .iter_mut()
                            .find(|state| state.server_id == server_id)
                    {
                        state.expires_at = new_expiry;
                    }
                }
                Err(error) => {
                    // Graph retains no deleted subscription to PATCH, so a
                    // vanished (404/410) subscription is replaced by a fresh
                    // create for the same resource; otherwise every renewal
                    // tick retries a permanent 404. The stale state stays
                    // installed until the replacement is in hand: dropping it
                    // first meant a failed create left the resource with no
                    // state at all, so no later tick could ever see it as due
                    // and coverage was lost until reopen.
                    let (phase, error) = match account.push_endpoint.as_ref() {
                        Some(endpoint) if subscription_is_gone(&error) => {
                            match replace_gone_subscription(
                                &account, endpoint, &handle, &server_id, &resource, &scopes,
                            )
                            .await
                            {
                                Ok(Replacement::Installed) => {
                                    // The resource had NO live subscription
                                    // between its disappearance and this
                                    // create, and Graph does not replay
                                    // notifications for that window.
                                    // `Reconnected` is the only event the
                                    // engine turns into a full reconcile
                                    // across every registered scope, so
                                    // without it the changes missed while the
                                    // subscription was absent wait for the
                                    // ordinary poll interval.
                                    let _ = account.push_tx.send(WatchEvent::Reconnected);
                                    disconnected = false;
                                    continue;
                                }
                                Ok(Replacement::HandleUnsubscribed) => continue,
                                // Classify the failure that actually blocks
                                // recovery. The original 404 only says the
                                // old subscription is gone, which is already
                                // known and acted on; the create error is the
                                // one whose recovery class decides whether
                                // this is worth another tick.
                                Err(recreate_error) => ("recreate", recreate_error),
                            }
                        }
                        _ => ("renew", error),
                    };
                    // Name what lost coverage. A resource covers exactly one
                    // scope in every ordinary case (each resource string is
                    // built from one folder or calendar id), so the common
                    // path gets precise attribution; the ambiguous case -
                    // `Event` and `CalendarEvent` scopes over one calendar -
                    // stays account-scoped rather than picking a scope
                    // arbitrarily, and the covered list is logged either way.
                    let context = GraphErrorContext::graph(AccountOperation::PushSubscribe);
                    let context = match scopes.as_slice() {
                        [only] => context.with_scope(ErrorScope::Cursor(only.clone())),
                        _ => context,
                    };
                    let account_error = into_account_error(error, context);
                    // Terminal recovery classes (AuthLost,
                    // NeedsPolicyChange, NoPermission, etc.) cannot
                    // be recovered without engine intervention.
                    // Surface them on the push channel so the engine
                    // tears the subscription down. Retryable classes
                    // ride out the next renewal tick - we log
                    // structured telemetry instead of formatted text
                    // so the dashboard can group on kind / message-key.
                    {
                        let telemetry = account_error.telemetry_fields();
                        tracing::warn!(
                            target: "bifrost_graph::webhooks",
                            provider = ?telemetry.provider,
                            protocol = ?telemetry.protocol,
                            message_key = telemetry.message_key,
                            recovery = telemetry.recovery_discriminant,
                            server_id = %server_id,
                            phase = phase,
                            scopes = ?scopes,
                            "Graph webhook renewal failed"
                        );
                    }
                    if account_error.recovery().is_terminal() {
                        remove_subscription_state(&account, &handle, &server_id).await;
                        let _ = account.push_tx.send(WatchEvent::Terminated(account_error));
                    }
                    had_error = true;
                }
            }
        }

        if had_error {
            if !disconnected {
                let _ = account.push_tx.send(WatchEvent::Disconnected);
                disconnected = true;
            }
        } else if disconnected {
            let _ = account.push_tx.send(WatchEvent::Reconnected);
            disconnected = false;
        }
    }
}

pub(super) fn has_live_graph_subscription_group(
    groups: &HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
) -> bool {
    groups.values().any(|group| !group.tearing_down)
}

/// Clear the worker slot on the way out of `run_graph_subscription_worker`,
/// so `ensure_graph_worker` starts a fresh worker for the next subscription.
/// See `worker_slot` for the ordering rule this call is one half of; the
/// caller must still hold the `graph_subscriptions` guard that decided to
/// exit.
async fn retire_graph_worker_slot(account: &GraphAccount) {
    crate::account::worker_slot::retire_worker_slot(&account.graph_worker).await;
}

/// One subscription inside the renewal threshold, with everything the worker
/// needs to renew it, recreate it, or report what it covered.
#[derive(Debug, Clone)]
pub(super) struct DueRenewal {
    pub(super) handle: SubscriptionHandle,
    pub(super) server_id: String,
    pub(super) resource: String,
    pub(super) scopes: Vec<CursorScope>,
}

/// The subscriptions inside the renewal threshold.
///
/// A condemned (`tearing_down`) group contributes nothing. Renewing its
/// subscriptions would extend the life of exactly what the caller asked to
/// delete, and recreating a vanished one would hand teardown a server id its
/// snapshot cannot contain. Their rows stay registered only so that a failed
/// DELETE can be retried against them.
pub(super) fn due_renewals(
    groups: &mut HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
) -> Vec<DueRenewal> {
    let mut due = Vec::new();
    for (handle, group) in groups.iter_mut() {
        if group.tearing_down {
            continue;
        }
        for state in &mut group.subscriptions {
            match check_expiry(&state.expires_at, RENEWAL_THRESHOLD_MINUTES) {
                ExpiryCheck::Live => continue,
                ExpiryCheck::ExpiringSoon => {}
                ExpiryCheck::Unparseable => {
                    if !state.warned_unparseable_expiry {
                        state.warned_unparseable_expiry = true;
                        tracing::warn!(
                            subscription = state.server_id,
                            expiration = state.expires_at,
                            "[Graph webhooks] Unparseable subscription expiry; treating as due for renewal"
                        );
                    }
                }
            }
            due.push(DueRenewal {
                handle: handle.clone(),
                server_id: state.server_id.clone(),
                resource: state.resource.clone(),
                scopes: state.scopes.clone(),
            });
        }
    }
    due
}

/// What happened to a subscription the renewal worker had to recreate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Replacement {
    /// The fresh subscription took the vanished one's place in the group.
    Installed,
    /// The handle stopped being registered while the create was in flight,
    /// so the fresh subscription was deleted again instead of installed.
    HandleUnsubscribed,
}

/// Create a replacement for a vanished subscription and install it under
/// `handle`, or undo the create when the handle is no longer registered.
///
/// The create happens before any local state changes so a failure leaves the
/// stale entry in place for the next renewal tick to retry - the resource
/// keeps a row that reads as due rather than silently losing coverage.
async fn replace_gone_subscription(
    account: &GraphAccount,
    endpoint: &PushEndpoint,
    handle: &SubscriptionHandle,
    stale_server_id: &str,
    resource: &str,
    scopes: &[CursorScope],
) -> Result<Replacement, crate::error::GraphError> {
    let response = create_subscription(
        &account.client,
        resource,
        &endpoint.webhook_url,
        &endpoint.client_state,
        None,
    )
    .await?;
    let replacement = GraphSubscriptionState {
        server_id: response.id,
        expires_at: response.expiration_date_time,
        resource: resource.to_string(),
        scopes: scopes.to_vec(),
        warned_unparseable_expiry: false,
    };
    let created_id = replacement.server_id.clone();

    let mut groups = account.graph_subscriptions.write().await;
    let installed = install_replacement(&mut groups, handle, stale_server_id, replacement);
    drop(groups);
    if installed {
        return Ok(Replacement::Installed);
    }

    // `push_unsubscribe` retired the handle, or condemned it and is walking
    // a server-id snapshot that cannot contain this create, while the create
    // was in flight. Either way it deletes every subscription it knows about
    // and reports teardown as successful, so installing this one would keep
    // notifications flowing for a subscription the caller believes is gone -
    // and re-registering the group would resurrect a handle nothing will
    // ever tear down again. Delete the subscription we just minted instead.
    // Best effort: the caller's `push_unsubscribe` may already have
    // returned, so there may be nobody left to report a cleanup failure to.
    if let Err(cleanup_error) = delete_subscription(&account.client, &created_id).await {
        tracing::warn!(
            target: "bifrost_graph::webhooks",
            server_id = %created_id,
            error = ?cleanup_error,
            "failed to delete a Graph webhook subscription recreated for an unsubscribed handle"
        );
    }
    Ok(Replacement::HandleUnsubscribed)
}

/// Swap `replacement` in for `stale_server_id` under `handle`.
///
/// Returns `false` when the replacement must not be installed, in which case
/// the caller deletes the subscription it just minted. Two refusals:
///
/// - `handle` is no longer registered. The lookup is a plain `get_mut`,
///   never an `entry().or_insert_with()`: the worker's due list is a
///   snapshot, and a concurrent `push_unsubscribe` can retire the handle
///   before the replacement lands. Re-creating the group there would
///   contradict a teardown the caller was already told succeeded.
/// - the group is registered but marked `tearing_down`. Teardown keeps the
///   group registered while it deletes each server subscription, so
///   "registered" no longer implies "live". It walks a snapshot of the
///   server ids taken when the marker went up, so a replacement installed
///   after that point is invisible to it: teardown would delete the stale
///   id, retire the group, return success - and the replacement would keep
///   delivering notifications for a handle the caller believes is gone.
pub(super) fn install_replacement(
    groups: &mut HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
    handle: &SubscriptionHandle,
    stale_server_id: &str,
    replacement: GraphSubscriptionState,
) -> bool {
    let Some(group) = groups.get_mut(handle) else {
        return false;
    };
    if group.tearing_down {
        return false;
    }
    group
        .subscriptions
        .retain(|state| state.server_id != stale_server_id);
    group.subscriptions.push(replacement);
    true
}
