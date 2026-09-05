//! The Graph `/subscriptions` webhook arm: per-scope resource construction,
//! subscription creation with rollback, the registered `GraphSubscriptionGroup`
//! state, and teardown (`push_unsubscribe` plus `close()`'s best-effort walk).
//!
//! The renewal worker that keeps these subscriptions alive lives in
//! `super::renewal`.

use std::collections::HashMap;

use bifrost_types::{
    AccountError, AccountOperation, CursorScope, ErrorScope, ObjectType, SubscriptionHandle,
};

use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::account::{GraphAccount, PushMode};
use crate::webhooks::{create_subscription, delete_subscription};

use super::common::{
    mark_push_reconnected, new_handle, unsupported_push_error, unsupported_push_scope_error,
};
use super::renewal::run_graph_subscription_worker;

#[derive(Debug, Clone)]
pub(crate) struct GraphSubscriptionGroup {
    pub(crate) subscriptions: Vec<GraphSubscriptionState>,
    /// Set the moment `push_unsubscribe` starts deleting this group, and
    /// never cleared - teardown intent is monotone per handle, because a
    /// later `push_subscribe` mints a fresh handle rather than reviving
    /// this one.
    ///
    /// Teardown deletes over the network, so the group must stay registered
    /// across those awaits for a failed DELETE to remain retryable. That
    /// keeps the handle visible to the renewal worker, whose recreate path
    /// would otherwise install a replacement into a group teardown has
    /// already snapshotted: the removal that follows only knows the stale
    /// server id, so the replacement would stay registered and live while
    /// `push_unsubscribe` reported success. The marker is a plain flag
    /// rather than a lock because the decision it drives is local and
    /// synchronous - serializing the two paths on a mutex would hold it
    /// across DELETE and create round trips.
    pub(crate) tearing_down: bool,
}

impl GraphSubscriptionGroup {
    /// A freshly registered group, not yet being torn down.
    pub(crate) fn live(subscriptions: Vec<GraphSubscriptionState>) -> Self {
        Self {
            subscriptions,
            tearing_down: false,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GraphSubscriptionState {
    pub(crate) server_id: String,
    pub(crate) expires_at: String,
    pub(crate) resource: String,
    /// The cursor scopes this one subscription covers.
    ///
    /// Retained so a terminal renewal failure can name what lost coverage.
    /// `subscribe_graph` groups by resource string and used to discard the
    /// scopes it grouped, leaving `Terminated` with no scope attribution at
    /// all - the engine could see that push broke without being able to tell
    /// which scopes to fall back to polling. Every other error path in this
    /// crate carries an `ErrorScope`.
    pub(crate) scopes: Vec<CursorScope>,
    /// Whether the renewal worker has already warned about an unparseable
    /// `expires_at` for this subscription, so the warning fires once per
    /// subscription rather than once per renewal tick.
    pub(crate) warned_unparseable_expiry: bool,
}

pub(super) async fn subscribe_graph(
    account: GraphAccount,
    eligible: Vec<(bifrost_types::BatchItemId, CursorScope)>,
    outcomes: &mut bifrost_types::BatchOutcomeBuilder<CursorScope>,
) -> Result<Option<SubscriptionHandle>, AccountError> {
    let Some(endpoint) = account.push_endpoint.clone() else {
        return Err(unsupported_push_error());
    };
    // Graph webhooks reject any subscription whose `resource` we cannot
    // construct. A scope with no resource is refused into the failed lane,
    // where the caller sees exactly which scope was declined and keeps
    // polling coverage for it, while its subscribable siblings still get a
    // live subscription.
    let mut grouped: HashMap<String, Vec<(bifrost_types::BatchItemId, CursorScope)>> =
        HashMap::new();
    for (item, scope) in eligible {
        match resource_for_scope(&account, &scope) {
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
        return Ok(None);
    }

    // Mint the handle BEFORE the first create. `new_handle` is fallible
    // (it can fail on host RNG) and classifies its error `Unsent`, which
    // is only true while nothing has been written. Minting it after the
    // creates would let an RNG failure return a no-bytes-sent error with
    // live server-side subscriptions behind it, so a caller acting on
    // that classification by retrying would duplicate them.
    let handle = new_handle()?;

    let mut subscriptions = Vec::new();
    for (resource, covered) in grouped {
        match create_subscription(
            &account.client,
            &resource,
            &endpoint.webhook_url,
            &endpoint.client_state,
            None,
        )
        .await
        {
            Ok(response) => {
                for (item, scope) in &covered {
                    outcomes.push_succeeded(item.clone(), scope.clone());
                }
                subscriptions.push(GraphSubscriptionState {
                    server_id: response.id,
                    expires_at: response.expiration_date_time,
                    resource,
                    scopes: covered.into_iter().map(|(_, scope)| scope).collect(),
                    warned_unparseable_expiry: false,
                });
            }
            Err(error) => {
                // The handle is never registered on the account and never
                // returned on this path, so nothing downstream could ever
                // reach these subscriptions to tear them down. Retain none
                // of them. Cleanup is best effort: preserve the create
                // error the caller needs to act on even if a DELETE fails.
                for subscription in &subscriptions {
                    if let Err(cleanup_error) =
                        delete_subscription(&account.client, &subscription.server_id).await
                    {
                        tracing::warn!(
                            target: "bifrost_graph::webhooks",
                            server_id = %subscription.server_id,
                            error = ?cleanup_error,
                            "failed to roll back Graph webhook subscription"
                        );
                    }
                }
                return Err(into_account_error(
                    error,
                    GraphErrorContext::graph(AccountOperation::PushSubscribe),
                ));
            }
        }
    }

    account
        .graph_subscriptions
        .write()
        .await
        .insert(handle.clone(), GraphSubscriptionGroup::live(subscriptions));
    // Only on the recovery EDGE. `Reconnected` is the engine's account-wide
    // full-reconcile trigger, and a first subscribe has no gap to cover: the
    // engine established or resumed these cursors moments earlier and is
    // already streaming them. Publishing it unconditionally charged every
    // subscribe - including the first one an account ever makes - one
    // redundant reconcile over every registered scope. A subscribe that lands
    // while the renewal worker has push latched down is a real recovery and
    // still emits.
    mark_push_reconnected(&account);
    ensure_graph_worker(account).await;
    Ok(Some(handle))
}

pub(super) async fn unsubscribe_graph(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), AccountError> {
    let Some(server_ids) = begin_graph_teardown(&account, &handle).await else {
        return Ok(());
    };
    for server_id in server_ids {
        delete_subscription(&account.client, &server_id)
            .await
            .map_err(|e| {
                into_account_error(
                    e,
                    GraphErrorContext::graph(AccountOperation::PushUnsubscribe),
                )
            })?;
        remove_subscription_state(&account, &handle, &server_id).await;
    }
    // "No groups left" and "stop the worker" must be one atomic step against
    // a concurrent `push_subscribe`, which inserts its live group under the
    // WRITE lock and only then calls `ensure_graph_worker`. Sampling
    // emptiness and dropping the guard first would let that subscribe see the
    // still-running worker, decline to spawn a replacement, and then have it
    // aborted here - leaving a live subscription with nothing renewing it.
    // Holding the guard makes the insert wait until the slot is empty. The
    // named binding is deliberate: the same guarantee via a temporary in the
    // condition would rest on `if`-temporary scoping rather than on
    // something a reader can see. Lock order is subscriptions-then-worker
    // here and in the worker's own retire path; nothing takes them inverted.
    let groups = account.graph_subscriptions.read().await;
    if groups.is_empty()
        && let Some(worker) = account.graph_worker.lock().await.take()
    {
        worker.abort();
    }
    drop(groups);
    Ok(())
}

/// Best-effort DELETE of every webhook subscription still registered, for
/// `close()`.
///
/// A Graph subscription lives on the server for up to its ~24h expiry and
/// keeps POSTing to the consumer's HTTPS receiver whether or not this process
/// still exists. Reopen builds a FRESH `GraphAccount` and the engine
/// resubscribes, so a `close()` that dropped the local map stranded one live
/// subscription per resource per reopen. Nothing in the `Account` contract
/// promises `push_unsubscribe` before `close()`, so `close()` has to do it.
///
/// Failures are logged, not returned: `close()` must still cancel the
/// shutdown token and retire its workers, and the engine has no useful
/// recovery for "the server kept a subscription we asked it to drop".
pub(crate) async fn retire_all_graph_subscriptions(account: &GraphAccount) {
    let handles: Vec<SubscriptionHandle> = account
        .graph_subscriptions
        .read()
        .await
        .keys()
        .cloned()
        .collect();
    for handle in handles {
        let Some(server_ids) = begin_graph_teardown(account, &handle).await else {
            continue;
        };
        for server_id in server_ids {
            match delete_subscription(&account.client, &server_id).await {
                Ok(()) => remove_subscription_state(account, &handle, &server_id).await,
                Err(error) => {
                    let error = into_account_error(
                        error,
                        GraphErrorContext::graph(AccountOperation::PushUnsubscribe),
                    );
                    let telemetry = error.telemetry_fields();
                    tracing::warn!(
                        target: "bifrost_graph::push",
                        message_key = telemetry.message_key,
                        recovery = telemetry.recovery_discriminant,
                        "close() could not retire a Graph webhook subscription"
                    );
                }
            }
        }
    }
}

async fn begin_graph_teardown(
    account: &GraphAccount,
    handle: &SubscriptionHandle,
) -> Option<Vec<String>> {
    let mut groups = account.graph_subscriptions.write().await;
    mark_group_tearing_down(&mut groups, handle)
}

/// Condemn one handle's group and snapshot the server ids teardown must
/// delete, without retiring their local state.
///
/// Marking and snapshotting happen under the SAME write lock, which is what
/// makes the pair race-free against the renewal worker: any recreate that
/// installs before this call is in the snapshot, and any recreate that
/// resolves after it sees the marker and refuses. The state itself stays
/// registered so a failed DELETE leaves that id and every later id
/// reachable under the handle for a retry.
///
/// Returns `None` for an unknown handle - teardown is idempotent.
pub(super) fn mark_group_tearing_down(
    groups: &mut HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
    handle: &SubscriptionHandle,
) -> Option<Vec<String>> {
    let group = groups.get_mut(handle)?;
    group.tearing_down = true;
    Some(
        group
            .subscriptions
            .iter()
            .map(|state| state.server_id.clone())
            .collect(),
    )
}

pub(super) async fn ensure_graph_worker(account: GraphAccount) {
    if account.push_mode != PushMode::GraphSubscriptions {
        return;
    }
    let worker_account = account.clone();
    crate::account::worker_slot::ensure_worker(&account.graph_worker, move || {
        tokio::spawn(async move {
            run_graph_subscription_worker(worker_account).await;
        })
    })
    .await;
}

pub(super) async fn remove_subscription_state(
    account: &GraphAccount,
    handle: &SubscriptionHandle,
    server_id: &str,
) {
    let mut groups = account.graph_subscriptions.write().await;
    remove_subscription_from_groups(&mut groups, handle, server_id);
}

/// Forget one server subscription only after its DELETE has succeeded.
///
/// Returns whether this was the group's final subscription. Keeping this
/// state transition pure pins the teardown invariant without requiring a
/// live Graph DELETE transport seam.
pub(super) fn remove_subscription_from_groups(
    groups: &mut HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
    handle: &SubscriptionHandle,
    server_id: &str,
) -> bool {
    let remove_group = if let Some(group) = groups.get_mut(handle) {
        group
            .subscriptions
            .retain(|state| state.server_id != server_id);
        group.subscriptions.is_empty()
    } else {
        false
    };
    if remove_group {
        groups.remove(handle);
    }
    remove_group
}

pub(super) fn resource_for_scope(
    account: &GraphAccount,
    scope: &CursorScope,
) -> Result<Option<String>, crate::error::GraphError> {
    // Route the resource through the scope's owning client (primary `/me`
    // or a shared mailbox's `/users/{owner}`) and use the *native* folder
    // id in the path. A foreign scope carries the owning mailbox inside the
    // `FolderId`; percent-encoding that raw foreign id into the URL (the
    // old behavior) produced a `/me/mailFolders/{owner%1Ffolder}/...`
    // resource Graph cannot resolve.
    let prefix = account.client_for_scope(scope)?.api_path_prefix();
    match scope {
        CursorScope::FolderType { folder, ty } => match ty {
            ObjectType::Email => {
                let native = crate::account::foreign::parse_folder(folder)
                    .native_id()
                    .to_string();
                let encoded = bifrost_net::url::encode_path_component(&native);
                Ok(Some(format!("{prefix}/mailFolders/{encoded}/messages")))
            }
            ObjectType::Event | ObjectType::CalendarEvent => {
                // The scope's folder is the calendar id - the same id
                // `inventory.rs` builds `/calendars/{id}/calendarView/delta`
                // from. Discarding it and subscribing to `{prefix}/events`
                // subscribes to the *default* calendar for every calendar
                // scope: secondary calendars would never see a notification
                // while the engine believed them push-covered, and the
                // grouping in `subscribe_graph` (keyed on this string) would
                // collapse every calendar scope into one entry.
                let native = crate::account::foreign::parse_folder(folder)
                    .native_id()
                    .to_string();
                let encoded = bifrost_net::url::encode_path_component(&native);
                Ok(Some(format!("{prefix}/calendars/{encoded}/events")))
            }
            ObjectType::Contact => {
                let native = crate::account::foreign::parse_folder(folder)
                    .native_id()
                    .to_string();
                let encoded = bifrost_net::url::encode_path_component(&native);
                Ok(Some(format!("{prefix}/contactFolders/{encoded}/contacts")))
            }
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}
