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
use super::dispatch::{ArmOutcome, finalize_push_outcomes};
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

/// The webhook arm of `push_subscribe`: create one server subscription per
/// resource, close the ledger, and only then register and start renewing.
///
/// The ledger is closed HERE, between the last create and the registration,
/// not by the dispatcher afterwards. `finalize` is fallible (an unfiled or
/// double-filed lane id is this crate's invariant breaking), and it used to
/// run after the group was registered and the renewal worker started: a
/// failure returned `Err` - no handle - over subscriptions the worker kept
/// renewing and no caller could ever name to unsubscribe. Closed before the
/// registration, the failure path has registered nothing and only has to
/// roll back the server-side creates, exactly as a failed create does.
///
/// The creates, rollback and registration run in a spawned task so a dropped
/// caller cannot strand them; see the comments at the spawn and on
/// `run_subscribe`.
pub(super) async fn subscribe_graph(
    account: GraphAccount,
    eligible: Vec<(bifrost_types::BatchItemId, CursorScope)>,
    mut outcomes: bifrost_types::BatchOutcomeBuilder<CursorScope>,
    expected: &[bifrost_types::BatchItemId],
) -> Result<ArmOutcome, AccountError> {
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
        return Ok((None, finalize_push_outcomes(outcomes, expected)?));
    }

    // Mint the handle BEFORE the first create. `new_handle` is fallible
    // (the host entropy source can fail; that is `Internal(RuntimeFailure)`,
    // never retried), and a failure here has written nothing. Minting it
    // after the creates would return an error with live server-side
    // subscriptions behind it and no handle any teardown could name them
    // by, orphaning them until they expire.
    let handle = new_handle()?;

    // A closed account must not gain server-side state. `close()` walks the
    // registered groups once; a create that started after that walk could
    // only ever be rolled back. Refusing here, before the first POST, keeps
    // the common case (subscribing on a dead account) from touching the
    // server at all; the registration step re-checks under the write lock
    // for the create that was already in flight when `close()` began.
    if account.shutdown.is_cancelled() {
        return Err(account_closed_error());
    }

    // Everything from here on runs in a task the ACCOUNT's runtime owns, and
    // this future only waits on it. `push_subscribe` is a caller-visible
    // future and callers drop those (a timeout, a `select!`, an aborted
    // reattach); dropped mid-way, a body that ran inline would strand every
    // server-side create made so far, skip the remaining rollback DELETEs, or
    // leave a registered group whose handle nobody received. A spawned task
    // is not torn down by its awaiter going away, so the creates, the
    // rollback and the registration each run to completion.
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
    // ack (or a failed send) as "the handle was never received" and tears the
    // registered group down. Exactly one side ever cleans up.
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(run_subscribe(
        account,
        SubscribeRequest {
            endpoint,
            grouped,
            handle,
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
    grouped: HashMap<String, Vec<(bifrost_types::BatchItemId, CursorScope)>>,
    handle: SubscriptionHandle,
    outcomes: bifrost_types::BatchOutcomeBuilder<CursorScope>,
    expected: Vec<bifrost_types::BatchItemId>,
}

/// The spawned half of the webhook `push_subscribe`: create, register, and
/// hand the result to the waiter, tearing the group down again if the waiter
/// never took it.
///
/// What cannot be made safe, and why:
/// - A create whose response never arrived (a timeout, a reset) may have
///   succeeded on Graph. The subscription id exists only in the response we
///   did not read, so there is nothing to DELETE by; it lives until Graph's
///   own expiry (about a day) and posts to the receiver meanwhile. Recovering
///   it would mean listing `/subscriptions` and deleting by resource and
///   notification URL, which could also delete another process's identical
///   subscription; that is a product decision, not something to do silently
///   here.
/// - A process exit or runtime shutdown kills this task like any other, with
///   the same result for whatever it had created.
async fn run_subscribe(
    account: GraphAccount,
    request: SubscribeRequest,
    result_tx: tokio::sync::oneshot::Sender<Result<ArmOutcome, AccountError>>,
    ack_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let handle = request.handle.clone();
    let result = create_and_register(&account, request).await;
    if result.is_err() {
        // Nothing registered, so nothing to retire; a dropped waiter has
        // nobody to tell.
        let _ = result_tx.send(result);
        return;
    }
    let received = result_tx.send(result).is_ok() && ack_rx.await.is_ok();
    if received {
        return;
    }
    // The group is registered and the worker running, but no caller holds
    // the handle, so no `push_unsubscribe` will ever name it. Retire it the
    // way a caller would. If a DELETE fails the group stays registered
    // (marked `tearing_down`), which is exactly what lets `close()` retry it.
    if let Err(error) = unsubscribe_graph(account, handle).await {
        let telemetry = error.telemetry_fields();
        tracing::warn!(
            target: "bifrost_graph::push",
            message_key = telemetry.message_key,
            recovery = telemetry.recovery_discriminant,
            "could not retire a Graph webhook subscription whose subscribe was abandoned"
        );
    }
}

async fn create_and_register(
    account: &GraphAccount,
    request: SubscribeRequest,
) -> Result<ArmOutcome, AccountError> {
    let SubscribeRequest {
        endpoint,
        grouped,
        handle,
        mut outcomes,
        expected,
    } = request;
    let expected = expected.as_slice();
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
                //
                // This rolls back only what we hold ids for. THIS create may
                // itself have succeeded on Graph (a timeout after the server
                // acted): its id is unknowable, see `run_subscribe`.
                roll_back_created(account, &subscriptions).await;
                return Err(into_account_error(
                    error,
                    GraphErrorContext::graph(AccountOperation::PushSubscribe),
                ));
            }
        }
    }

    // Every lane must be filed before anything becomes live. See the
    // function doc: on this path nothing is registered and no worker was
    // started, so rolling back the creates is the whole cleanup.
    let outcomes = match finalize_push_outcomes(outcomes, expected) {
        Ok(outcomes) => outcomes,
        Err(error) => {
            roll_back_created(account, &subscriptions).await;
            return Err(error);
        }
    };

    let mut groups = account.graph_subscriptions.write().await;
    // Checked under the write lock, and paired with `retire_all_graph_
    // subscriptions` cancelling the token BEFORE it walks the groups: either
    // this insert lands before the cancel (and the walk, which needs the read
    // lock afterwards, sees and deletes it) or it sees the cancel here and
    // rolls back. Without the pairing a `close()` that walked the groups while
    // this task was mid-create would leave a registered group nobody retires.
    if account.shutdown.is_cancelled() {
        drop(groups);
        roll_back_created(account, &subscriptions).await;
        return Err(account_closed_error());
    }
    groups.insert(handle.clone(), GraphSubscriptionGroup::live(subscriptions));
    drop(groups);
    // Only on the recovery EDGE. `Reconnected` is the engine's account-wide
    // full-reconcile trigger, and a first subscribe has no gap to cover: the
    // engine established or resumed these cursors moments earlier and is
    // already streaming them. Publishing it unconditionally charged every
    // subscribe - including the first one an account ever makes - one
    // redundant reconcile over every registered scope. A subscribe that lands
    // while the renewal worker has push latched down is a real recovery and
    // still emits.
    mark_push_reconnected(account);
    ensure_graph_worker(account.clone()).await;
    Ok((Some(handle), outcomes))
}

/// `push_subscribe` on (or racing) a closed account.
fn account_closed_error() -> AccountError {
    into_account_error(
        crate::error::GraphError::RuntimeFailure {
            message: "the account is closed".to_string(),
        },
        GraphErrorContext::graph(AccountOperation::PushSubscribe),
    )
}

/// Best-effort DELETE of subscriptions this request created but will never
/// register or return a handle for.
///
/// Nothing downstream can reach these rows once the request fails - they are
/// in no group, so neither `push_unsubscribe` nor `close()` walks them - so a
/// DELETE that fails here leaves the subscription to Graph's own expiry.
/// Failures are logged, not returned: the caller needs the error that failed
/// the request, not the cleanup's.
async fn roll_back_created(account: &GraphAccount, subscriptions: &[GraphSubscriptionState]) {
    for subscription in subscriptions {
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
    // Cancel BEFORE the walk: an in-flight `push_subscribe` checks the token
    // under the same write lock it registers under, so a group it registers
    // either precedes this cancel (and is walked below) or is refused.
    // `close()` cancels again afterwards; that is idempotent.
    account.shutdown.cancel();
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
