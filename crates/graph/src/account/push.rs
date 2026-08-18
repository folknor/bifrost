use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bifrost_types::WatchEvent;
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, CursorScope,
    ErrorScope, ObjectType, Protocol, ProtocolErrorKind, Provider, RequestCause,
    SubscriptionHandle, TransmissionState,
};
use serde::{Deserialize, Serialize};

use crate::webhooks::{
    create_subscription, delete_subscription, is_expiring_soon, renew_subscription,
    subscription_is_gone,
};

use super::graph_error::{
    GraphErrorContext, id_translation_refused, into_account_error, invalid_account_error,
    protocol_violation,
};
use super::{GraphAccount, PushMode};

const RENEWAL_CHECK_INTERVAL: Duration = Duration::from_secs(10 * 60);
const RENEWAL_THRESHOLD_MINUTES: i64 = 30;

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
}

#[derive(Debug, Clone)]
pub(crate) struct EwsSubscriptionState {
    /// The Graph cursor scope and EWS id for its folder. Graph `restId` and
    /// EWS `ewsId` are distinct opaque formats. The live EWS subscription id
    /// is NOT stored here: it is worker-local state, minted per Subscribe
    /// and abandoned (with a best-effort Unsubscribe) on every reconnect or
    /// topology handoff, so nothing outside the worker's loop may hold it.
    pub(crate) scopes: Vec<EwsSubscriptionScope>,
}

#[derive(Debug, Clone)]
pub(crate) struct EwsSubscriptionScope {
    pub(crate) scope: CursorScope,
    pub(crate) ews_folder_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TranslateExchangeIdsRequest {
    input_ids: Vec<String>,
    source_id_type: &'static str,
    target_id_type: &'static str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TranslateExchangeIdsResponse {
    value: Vec<TranslatedExchangeId>,
}

/// One `convertIdResult`.
///
/// Graph answers PER input id inside an otherwise successful 200: a converted
/// id carries `targetId`, an id it could not convert carries `errorDetails`
/// and no target. Requiring `targetId` made a single refused id fail
/// deserialization of the whole response, so one stale folder surfaced as a
/// terminal `Protocol(ParseFailed)` naming nothing instead of a
/// scope-correlated refusal naming the folder and Graph's own code.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TranslatedExchangeId {
    source_id: String,
    target_id: Option<String>,
    error_details: Option<ConvertIdError>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConvertIdError {
    code: Option<String>,
    message: Option<String>,
}

/// Graph caps `translateExchangeIds`' `inputIds` collection at 1,000 strings
/// and rejects the whole request above it.
const TRANSLATE_EXCHANGE_IDS_MAX_INPUTS: usize = 1_000;

pub(crate) async fn push_subscribe(
    account: GraphAccount,
    scopes: Vec<CursorScope>,
) -> Result<SubscriptionHandle, AccountError> {
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
    // Public folders are poll-only in v1: a bare `CursorScope::Folder`
    // has no push surface (EWS streaming notifications do not cover the
    // public-folder hierarchy mailbox). Reject before dispatch so both
    // push modes are consistent.
    if scopes
        .iter()
        .any(|scope| matches!(scope, CursorScope::Folder(_)))
    {
        return Err(unsupported_push_error());
    }
    match account.push_mode {
        PushMode::GraphSubscriptions => subscribe_graph(account, scopes).await,
        PushMode::EwsStreaming => subscribe_ews(account, scopes).await,
    }
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

fn unsupported_push_error() -> AccountError {
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

async fn subscribe_graph(
    account: GraphAccount,
    scopes: Vec<CursorScope>,
) -> Result<SubscriptionHandle, AccountError> {
    let Some(endpoint) = account.push_endpoint.clone() else {
        return Err(unsupported_push_error());
    };
    // Graph webhooks reject any subscription whose `resource` we
    // cannot construct. Resolving partial subsets silently and
    // returning success would mask consumer-side bugs where the
    // engine asked for a scope we can never serve. Fail loudly.
    let mut grouped: HashMap<String, Vec<CursorScope>> = HashMap::new();
    for scope in scopes {
        match resource_for_scope(&account, &scope) {
            Ok(Some(resource)) => {
                grouped.entry(resource).or_default().push(scope);
            }
            Ok(None) => return Err(unsupported_push_error()),
            Err(error) => {
                // A stale shared mailbox has no subscribable resource.
                // `push_subscribe` answers per REQUEST (one handle covers
                // the whole scope list), so refusing the call is right -
                // but the refusal must name the scope that caused it, or
                // the caller cannot tell which registration to drop.
                return Err(into_account_error(
                    error,
                    GraphErrorContext::graph(AccountOperation::PushSubscribe)
                        .with_scope(ErrorScope::Cursor(scope)),
                ));
            }
        }
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
            Ok(response) => subscriptions.push(GraphSubscriptionState {
                server_id: response.id,
                expires_at: response.expiration_date_time,
                resource,
                scopes: covered,
            }),
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
    let _ = account.push_tx.send(WatchEvent::Reconnected);
    ensure_graph_worker(account).await;
    Ok(handle)
}

async fn unsubscribe_graph(
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
        if let Err(error) = unsubscribe_graph(account.clone(), handle).await {
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
fn mark_group_tearing_down(
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

async fn ensure_graph_worker(account: GraphAccount) {
    if account.push_mode != PushMode::GraphSubscriptions {
        return;
    }
    let mut worker = account.graph_worker.lock().await;
    let needs_start = worker
        .as_ref()
        .is_none_or(tokio::task::JoinHandle::is_finished);
    if needs_start {
        let worker_account = account.clone();
        *worker = Some(tokio::spawn(async move {
            run_graph_subscription_worker(worker_account).await;
        }));
    }
}

async fn run_graph_subscription_worker(account: GraphAccount) {
    let mut disconnected = false;
    loop {
        tokio::select! {
            () = account.shutdown.cancelled() => return,
            () = tokio::time::sleep(RENEWAL_CHECK_INTERVAL) => {}
        }

        let due = {
            let groups = account.graph_subscriptions.read().await;
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
            due_renewals(&groups)
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

fn has_live_graph_subscription_group(
    groups: &HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
) -> bool {
    groups.values().any(|group| !group.tearing_down)
}

/// Clear the worker slot on the way out of `run_graph_subscription_worker`,
/// so `ensure_graph_worker` starts a fresh worker for the next subscription.
///
/// Dropping whatever is in the slot is safe without identifying the handle:
/// `ensure_graph_worker` installs one only when the slot is empty or its task
/// has already finished, and the caller here is neither, so the slot holds
/// either this task's own handle or `None` (`push_unsubscribe` took it to
/// abort us). Dropping a `JoinHandle` detaches, it does not cancel.
///
/// Lock order is subscriptions-then-worker everywhere (`push_unsubscribe`
/// holds its read guard across the same acquisition in a let-chain), and
/// `ensure_graph_worker` takes only the worker lock, so this cannot deadlock.
async fn retire_graph_worker_slot(account: &GraphAccount) {
    drop(account.graph_worker.lock().await.take());
}

/// One subscription inside the renewal threshold, with everything the worker
/// needs to renew it, recreate it, or report what it covered.
#[derive(Debug, Clone)]
struct DueRenewal {
    handle: SubscriptionHandle,
    server_id: String,
    resource: String,
    scopes: Vec<CursorScope>,
}

/// The subscriptions inside the renewal threshold.
///
/// A condemned (`tearing_down`) group contributes nothing. Renewing its
/// subscriptions would extend the life of exactly what the caller asked to
/// delete, and recreating a vanished one would hand teardown a server id its
/// snapshot cannot contain. Their rows stay registered only so that a failed
/// DELETE can be retried against them.
fn due_renewals(groups: &HashMap<SubscriptionHandle, GraphSubscriptionGroup>) -> Vec<DueRenewal> {
    groups
        .iter()
        .filter(|(_, group)| !group.tearing_down)
        .flat_map(|(handle, group)| {
            group.subscriptions.iter().filter_map(move |state| {
                if is_expiring_soon(&state.expires_at, RENEWAL_THRESHOLD_MINUTES) {
                    Some(DueRenewal {
                        handle: handle.clone(),
                        server_id: state.server_id.clone(),
                        resource: state.resource.clone(),
                        scopes: state.scopes.clone(),
                    })
                } else {
                    None
                }
            })
        })
        .collect()
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
fn install_replacement(
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

async fn remove_subscription_state(
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
fn remove_subscription_from_groups(
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

/// The Graph `restId` this scope contributes to the EWS Subscribe request, or
/// `None` when the EWS streaming worker cannot subscribe to it at all.
///
/// Two exclusions, both of which the worker would otherwise turn into a
/// remote failure that reads as a provider fault instead of the caller's
/// unsupported request:
///
/// - A non-`FolderType` scope contributes nothing to the Subscribe body's
///   `FolderIds`, so a request built only from such scopes ships an empty
///   `<t:FolderIds></t:FolderIds>` and EWS rejects the whole subscription.
/// - A foreign (shared-mailbox) folder is addressable only with that
///   mailbox's EWS routing headers, and the Subscribe path sends
///   `EwsHeaders::default()`. Its native folder id would be resolved
///   against the PRIMARY mailbox's namespace - a wrong folder or a miss,
///   never the intended one. Mailbox-grouped EWS subscriptions are the fix;
///   until then this rejects rather than silently mis-targets.
///
/// Returning the id rather than a bool fuses the check with the extraction:
/// the translation request, the reconciliation, and the retained state all
/// read the one string this produced, so no later phase can re-derive it
/// differently or have to assert a shape the predicate already guaranteed.
fn ews_subscribable_folder_id(scope: &CursorScope) -> Option<String> {
    let CursorScope::FolderType { folder, .. } = scope else {
        return None;
    };
    let parsed = super::foreign::parse_folder(folder);
    if parsed.foreign().is_some() {
        return None;
    }
    Some(parsed.native_id().to_string())
}

async fn subscribe_ews(
    account: GraphAccount,
    scopes: Vec<CursorScope>,
) -> Result<SubscriptionHandle, AccountError> {
    let mut pending = Vec::with_capacity(scopes.len());
    for scope in scopes {
        let Some(source_id) = ews_subscribable_folder_id(&scope) else {
            return Err(unsupported_push_error());
        };
        pending.push((scope, source_id));
    }
    let scopes = translate_ews_scopes(&account, pending).await?;
    let handle = new_handle()?;
    account
        .ews_subscriptions
        .write()
        .await
        .insert(handle.clone(), EwsSubscriptionState { scopes });
    // Bump the topology generation AFTER the map write: the worker only
    // re-reads the map after observing a generation it has not seen, so the
    // read this bump provokes is guaranteed to include the new registration.
    account
        .ews_topology
        .send_modify(|generation| *generation = generation.wrapping_add(1));
    super::push_stream::ensure_ews_worker(account).await;
    Ok(handle)
}

/// The error context for the translation request itself.
///
/// The operation stays `PushSubscribe` - that IS what the caller asked for,
/// and borrowing an unrelated idempotent operation would put a name in every
/// telemetry export that no call site matches. What the context corrects is
/// the recovery derivation: `PushSubscribe` is non-idempotent, so an
/// in-flight transport drop would derive
/// `Reconcile(TransportDropAfterSend, [CheckTarget])` and send the engine
/// looking for a subscription to probe. This POST is a read-only id
/// conversion that runs BEFORE any local subscription state, EWS
/// subscription, or handle exists: there is no target, nothing was created,
/// and repeating it is free.
fn translate_error_context() -> GraphErrorContext {
    GraphErrorContext::graph(AccountOperation::PushSubscribe).idempotent()
}

/// Converts Graph REST ids into the EWS ids required by this subscription's
/// SOAP request and its notification folder ids. The response can be
/// unordered, and is fanned out over several requests once the folder count
/// passes Graph's cap, so each scope is preserved by matching its `sourceId`
/// rather than by position.
async fn translate_ews_scopes(
    account: &GraphAccount,
    pending: Vec<(CursorScope, String)>,
) -> Result<Vec<EwsSubscriptionScope>, AccountError> {
    let mut translated = Vec::new();
    for input_ids in translation_input_chunks(&pending) {
        let request = TranslateExchangeIdsRequest {
            input_ids,
            source_id_type: "restId",
            target_id_type: "ewsId",
        };
        let response: TranslateExchangeIdsResponse = account
            .client
            .post("/me/translateExchangeIds", &request)
            .await
            .map_err(|error| into_account_error(error, translate_error_context()))?;
        translated.extend(response.value);
    }
    reconcile_translated_ews_scopes(pending, translated)
}

/// The `inputIds` collections one subscription's translation needs.
///
/// Two scopes can name the same folder - an `Email` and a `Contact` scope
/// over one container decode to the same native id - and Graph caps
/// `inputIds` at `TRANSLATE_EXCHANGE_IDS_MAX_INPUTS`, rejecting the whole
/// request above it. So deduplicate first (that alone keeps a mailbox under
/// the cap for the folder counts that produce duplicates) and then chunk, so
/// a genuinely large mailbox fans out instead of failing before EWS setup is
/// even attempted. First-seen order is preserved so the chunk boundaries are
/// deterministic and a failure names a stable set of ids.
fn translation_input_chunks(pending: &[(CursorScope, String)]) -> Vec<Vec<String>> {
    let mut seen = HashSet::new();
    let mut unique = Vec::with_capacity(pending.len());
    for (_, source_id) in pending {
        if seen.insert(source_id.as_str()) {
            unique.push(source_id.clone());
        }
    }
    unique
        .chunks(TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
        .map(<[String]>::to_vec)
        .collect()
}

/// Pairs every submitted scope back with its translated EWS id, or fails the
/// subscription.
///
/// One answer can serve several scopes (the deduplicated request asked once
/// for a folder two scopes share), and the three ways an answer can fail are
/// distinct enough to classify apart:
///
/// - refused (`errorDetails`, no target): Graph answered, and the answer is
///   "not this id". Terminal, scope-correlated, carries Graph's code.
/// - omitted: Graph must answer every id it was given. A missing answer is
///   the provider breaking its own contract, not a malformed request.
/// - answered with neither a target nor an error: the same contract
///   violation in a different shape, and the one case where falling through
///   would leave a scope with no id at all.
///
/// None of the three may degrade to sending the untranslated `restId`: EWS
/// cannot parse it, so the Subscribe would fail as an opaque
/// `SoapFaultCode::Unknown` instead of the diagnosable local error.
fn reconcile_translated_ews_scopes(
    pending: Vec<(CursorScope, String)>,
    translated: Vec<TranslatedExchangeId>,
) -> Result<Vec<EwsSubscriptionScope>, AccountError> {
    let answers: HashMap<String, TranslatedExchangeId> = translated
        .into_iter()
        .map(|entry| (entry.source_id.clone(), entry))
        .collect();
    pending
        .into_iter()
        .map(|(scope, source_id)| {
            let error_scope = ErrorScope::Cursor(scope.clone());
            let Some(answer) = answers.get(&source_id) else {
                return Err(protocol_violation(
                    ProtocolErrorKind::ContractViolation,
                    AccountOperation::PushSubscribe,
                    Some(error_scope),
                    "translateExchangeIds omitted a subscribed folder",
                ));
            };
            if let Some(ews_folder_id) = answer
                .target_id
                .as_ref()
                .filter(|target| !target.trim().is_empty())
            {
                return Ok(EwsSubscriptionScope {
                    scope,
                    ews_folder_id: ews_folder_id.clone(),
                });
            }
            match answer.error_details.as_ref() {
                Some(details) => Err(id_translation_refused(
                    AccountOperation::PushSubscribe,
                    error_scope,
                    details.code.as_deref(),
                    details.message.as_deref(),
                )),
                None => Err(protocol_violation(
                    ProtocolErrorKind::ContractViolation,
                    AccountOperation::PushSubscribe,
                    Some(error_scope),
                    "translateExchangeIds answered without a target id or error details",
                )),
            }
        })
        .collect()
}

async fn unsubscribe_ews(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), AccountError> {
    let removed = account
        .ews_subscriptions
        .write()
        .await
        .remove(&handle)
        .is_some();
    // Only a registration that actually existed changes the scope union;
    // bumping on an idempotent re-unsubscribe would churn the live stream
    // through a pointless resubscribe.
    if removed {
        account
            .ews_topology
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
    Ok(())
}

fn resource_for_scope(
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
                let native = super::foreign::parse_folder(folder).native_id().to_string();
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
                let native = super::foreign::parse_folder(folder).native_id().to_string();
                let encoded = bifrost_net::url::encode_path_component(&native);
                Ok(Some(format!("{prefix}/calendars/{encoded}/events")))
            }
            ObjectType::Contact => {
                let native = super::foreign::parse_folder(folder).native_id().to_string();
                let encoded = bifrost_net::url::encode_path_component(&native);
                Ok(Some(format!("{prefix}/contactFolders/{encoded}/contacts")))
            }
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

fn new_handle() -> Result<SubscriptionHandle, AccountError> {
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

#[cfg(test)]
mod tests {
    use bifrost_types::FolderId;

    use crate::client::{GraphClient, ScriptedRestResponse};

    use super::*;

    /// The EWS Subscribe boundary and the webhook boundary must agree on
    /// what "subscribable" means. A non-folder scope contributes nothing
    /// to `FolderIds` (empty element -> EWS rejects the whole request),
    /// and a foreign folder would be resolved against the PRIMARY
    /// mailbox's namespace because the Subscribe path sends no routing
    /// headers. Both must be refused locally as `Unsupported`, not
    /// converted into a remote failure that reads as a provider fault.
    #[test]
    fn ews_subscribe_accepts_only_primary_mailbox_folder_scopes() {
        let primary = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        // The accepted scope also yields the id the translation request
        // sends: bare, never the raw foreign-encoded string.
        assert_eq!(
            ews_subscribable_folder_id(&primary).as_deref(),
            Some("inbox")
        );

        let foreign = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        assert!(ews_subscribable_folder_id(&foreign).is_none());

        // A public-folder scope, and the account-wide scope the webhook
        // path already rejects loudly.
        assert!(
            ews_subscribable_folder_id(&CursorScope::Folder(FolderId("pf".to_string()))).is_none()
        );
        assert!(ews_subscribable_folder_id(&CursorScope::Account).is_none());
    }

    #[test]
    fn graph_subscription_resource_uses_folder_messages() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        assert_eq!(
            resource_for_scope(&account, &scope)
                .expect("primary")
                .as_deref(),
            Some("/me/mailFolders/inbox/messages")
        );
    }

    /// Each calendar scope must subscribe to its own calendar. `{prefix}/events`
    /// is the default calendar, so a shared resource string both mis-targets
    /// secondary calendars and collapses them together in `subscribe_graph`'s
    /// resource-keyed grouping.
    #[test]
    fn graph_event_subscription_names_the_scope_calendar() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let scope = |id: &str| CursorScope::FolderType {
            folder: FolderId(id.to_string()),
            ty: ObjectType::Event,
        };
        assert_eq!(
            resource_for_scope(&account, &scope("calendar-id"))
                .expect("primary")
                .as_deref(),
            Some("/me/calendars/calendar-id/events")
        );
        assert_ne!(
            resource_for_scope(&account, &scope("secondary")).expect("primary"),
            resource_for_scope(&account, &scope("calendar-id")).expect("primary"),
        );
        let opaque = resource_for_scope(&account, &scope("AAMk/GI2="))
            .expect("primary")
            .expect("subscribable");
        assert!(!opaque.contains("AAMk/GI2="), "{opaque}");
        assert!(opaque.ends_with("/events"), "{opaque}");
    }

    #[test]
    fn graph_subscription_resource_routes_foreign_scope_to_owner() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        // The owning mailbox rides in the `/users/{id}` segment and only
        // the native folder id is in `/mailFolders/{id}` - no raw foreign
        // id (no `%1F` separator) is percent-encoded into the URL.
        let resource = resource_for_scope(&account, &scope)
            .expect("configured mailbox")
            .expect("foreign scope resolves");
        assert_eq!(
            resource,
            "/users/shared%40contoso.com/mailFolders/AAMk/messages"
        );
        assert!(!resource.contains("%1F"));
    }

    /// `subscribe_graph` must fail loudly when any requested scope
    /// cannot resolve to a Graph resource. Silently subscribing to
    /// the resolvable subset returns `Ok` while quietly losing
    /// coverage of the unresolved scopes; tests pin
    /// `Unsupported(PushSubscribe)` instead.
    #[tokio::test]
    async fn subscribe_partial_resolve_returns_unsupported() {
        let mut account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        account.push_endpoint = Some(PushEndpoint {
            webhook_url: "https://example.test/webhook".to_string(),
            client_state: "secret".to_string(),
        });
        // CursorScope::Account is not a `FolderType` and so cannot
        // be mapped to a Graph subscription resource. This must
        // surface as Unsupported(PushSubscribe), not as an
        // empty-success.
        let result = subscribe_graph(account, vec![CursorScope::Account]).await;
        let err = result.expect_err("expected Unsupported");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
        ));
    }

    #[tokio::test]
    async fn webhook_creation_rolls_back_each_already_created_subscription() {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::CREATED,
                serde_json::json!({"id":"first","expirationDateTime":"2099-01-01T00:00:00Z"}),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({"error":{"code":"ErrorInternalServerError","message":"no"}}),
            ),
            ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
        ]);
        let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        account.push_endpoint = Some(PushEndpoint {
            webhook_url: "https://example.test/hook".to_string(),
            client_state: "secret".to_string(),
        });
        let scopes = vec![
            CursorScope::FolderType {
                folder: FolderId("inbox".to_string()),
                ty: ObjectType::Email,
            },
            CursorScope::FolderType {
                folder: FolderId("contacts".to_string()),
                ty: ObjectType::Contact,
            },
        ];
        assert!(subscribe_graph(account.clone(), scopes).await.is_err());
        assert!(account.graph_subscriptions.read().await.is_empty());
        let requests = client.take_rest_requests();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.method.as_str())
                .collect::<Vec<_>>(),
            ["POST", "POST", "DELETE"]
        );
        assert!(requests[2].url.ends_with("/subscriptions/first"));
    }

    #[tokio::test]
    async fn unsubscribe_graph_deletes_every_server_row() {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
            ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let handle = SubscriptionHandle("h".to_string());
        account.graph_subscriptions.write().await.insert(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![
                state("one", "/me/messages"),
                state("two", "/me/events"),
            ]),
        );
        unsubscribe_graph(account.clone(), handle)
            .await
            .expect("delete loop succeeds");
        assert!(account.graph_subscriptions.read().await.is_empty());
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].url.ends_with("/subscriptions/one"));
        assert!(requests[1].url.ends_with("/subscriptions/two"));
    }

    /// A subscription Graph has already dropped must not fail teardown.
    /// `subscription_is_gone` used to read only `GraphError::Response`,
    /// which a REST call never produces - the transport converts a 404 into
    /// `bifrost_net::Error::Status` first - so on the live path the 404
    /// tolerance never applied, the DELETE loop aborted on the vanished row,
    /// and the handle stayed registered with its remaining siblings
    /// undeleted.
    #[tokio::test]
    async fn unsubscribe_tolerates_a_subscription_the_server_already_dropped() {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::NOT_FOUND,
                serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
            ),
            ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let handle = SubscriptionHandle("h".to_string());
        account.graph_subscriptions.write().await.insert(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![
                state("vanished", "/me/messages"),
                state("live", "/me/events"),
            ]),
        );

        unsubscribe_graph(account.clone(), handle)
            .await
            .expect("a row the server already dropped is not a teardown failure");
        assert!(account.graph_subscriptions.read().await.is_empty());
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].url.ends_with("/subscriptions/live"));
    }

    #[tokio::test]
    async fn exchange_id_translation_posts_each_wire_chunk_and_accumulates_answers() {
        let client = GraphClient::new("token");
        let pending: Vec<_> = (0..=TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
            .map(|index| {
                (
                    CursorScope::FolderType {
                        folder: FolderId(format!("f{index}")),
                        ty: ObjectType::Email,
                    },
                    format!("f{index}"),
                )
            })
            .collect();
        let answer = |start: usize, end: usize| serde_json::json!({"value": (start..end).map(|index| serde_json::json!({"sourceId":format!("f{index}"),"targetId":format!("e{index}")})).collect::<Vec<_>>()});
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                answer(0, TRANSLATE_EXCHANGE_IDS_MAX_INPUTS),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                answer(
                    TRANSLATE_EXCHANGE_IDS_MAX_INPUTS,
                    TRANSLATE_EXCHANGE_IDS_MAX_INPUTS + 1,
                ),
            ),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::EwsStreaming);
        let translated = translate_ews_scopes(&account, pending)
            .await
            .expect("wire fan-out translates all folders");
        assert_eq!(translated.len(), TRANSLATE_EXCHANGE_IDS_MAX_INPUTS + 1);
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.url.ends_with("/me/translateExchangeIds"))
        );
        assert_eq!(
            requests[0]
                .body
                .as_ref()
                .and_then(|body| body["inputIds"].as_array())
                .map(Vec::len),
            Some(TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
        );
        assert_eq!(
            requests[1]
                .body
                .as_ref()
                .and_then(|body| body["inputIds"].as_array())
                .map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn contact_scope_subscribes_to_the_contact_folder_collection() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("contacts".to_string()),
            ty: ObjectType::Contact,
        };
        assert_eq!(
            resource_for_scope(&account, &scope)
                .expect("primary")
                .as_deref(),
            Some("/me/contactFolders/contacts/contacts")
        );
    }

    #[test]
    fn unsubscribable_scopes_have_no_graph_resource() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        assert!(
            resource_for_scope(&account, &CursorScope::Account)
                .expect("primary")
                .is_none()
        );
        // A public folder is poll-only; it must not resolve to a resource.
        assert!(
            resource_for_scope(
                &account,
                &CursorScope::Folder(FolderId("AAMkPF=".to_string()))
            )
            .expect("primary")
            .is_none()
        );
        assert!(
            resource_for_scope(
                &account,
                &CursorScope::FolderType {
                    folder: FolderId("inbox".to_string()),
                    ty: ObjectType::Mailbox,
                }
            )
            .expect("primary")
            .is_none()
        );
    }

    #[test]
    fn opaque_folder_ids_are_percent_encoded_into_the_resource() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("AAMk/GI2=".to_string()),
            ty: ObjectType::Email,
        };
        let resource = resource_for_scope(&account, &scope)
            .expect("primary")
            .expect("email scope resolves");
        assert!(!resource.contains("AAMk/GI2="), "{resource}");
        assert!(resource.ends_with("/messages"), "{resource}");
    }

    /// A persisted scope whose shared mailbox was removed must fail before
    /// it can be rewritten into a `/me` subscription resource.
    #[test]
    fn an_unconfigured_foreign_mailbox_is_rejected_locally() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("other@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        assert!(matches!(
            resource_for_scope(&account, &scope),
            Err(crate::error::GraphError::Configuration { .. })
        ));
    }

    #[tokio::test]
    async fn webhook_mode_without_an_endpoint_is_unsupported() {
        // `with_push_endpoint` was never called: there is nowhere for Graph
        // to deliver, so subscribing must refuse rather than create a
        // subscription pointing at nothing.
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let err = subscribe_graph(account, vec![scope])
            .await
            .expect_err("expected Unsupported");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
        ));
    }

    #[tokio::test]
    async fn a_public_folder_scope_is_rejected_in_both_push_modes() {
        // Rejected before mode dispatch, so neither mode can start a worker
        // for a folder it can never observe.
        for mode in [PushMode::GraphSubscriptions, PushMode::EwsStreaming] {
            let account = GraphAccount::new_for_tests(GraphClient::new("token"), mode);
            let err = push_subscribe(
                account,
                vec![
                    CursorScope::FolderType {
                        folder: FolderId("inbox".to_string()),
                        ty: ObjectType::Email,
                    },
                    CursorScope::Folder(FolderId("AAMkPF=".to_string())),
                ],
            )
            .await
            .expect_err("public-folder push is unsupported");
            assert!(matches!(
                err.kind(),
                AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
            ));
        }
    }

    #[test]
    fn subscription_handles_are_distinct_hex_tokens() {
        let first = new_handle().expect("rng");
        let second = new_handle().expect("rng");
        // 16 random bytes, lowercase hex: 32 digits, parseable as a u128.
        assert_eq!(first.0.len(), 32);
        assert!(u128::from_str_radix(&first.0, 16).is_ok(), "{}", first.0);
        assert_ne!(first, second);
    }

    fn state(server_id: &str, resource: &str) -> GraphSubscriptionState {
        GraphSubscriptionState {
            server_id: server_id.to_string(),
            expires_at: "2099-01-01T00:00:00Z".to_string(),
            resource: resource.to_string(),
            scopes: vec![CursorScope::FolderType {
                folder: FolderId(resource.to_string()),
                ty: ObjectType::Email,
            }],
        }
    }

    /// A subscription whose expiry is already past, so it is unconditionally
    /// inside the renewal threshold.
    fn expiring(server_id: &str, resource: &str) -> GraphSubscriptionState {
        GraphSubscriptionState {
            server_id: server_id.to_string(),
            expires_at: "2000-01-01T00:00:00Z".to_string(),
            resource: resource.to_string(),
            scopes: vec![CursorScope::FolderType {
                folder: FolderId(resource.to_string()),
                ty: ObjectType::Email,
            }],
        }
    }

    /// A recreated subscription takes the vanished one's place inside the
    /// SAME group rather than piling up beside it: the stale `server_id`
    /// would otherwise stay in the due list forever, 404 on every renewal
    /// tick, and drive an endless recreate loop. Sibling resources under
    /// the same handle are untouched.
    #[test]
    fn a_recreated_subscription_replaces_the_vanished_one_in_place() {
        let handle = SubscriptionHandle("h".to_string());
        let mut groups = HashMap::from([(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![
                state("gone", "/me/mailFolders/inbox/messages"),
                state("healthy", "/me/events"),
            ]),
        )]);

        assert!(install_replacement(
            &mut groups,
            &handle,
            "gone",
            state("fresh", "/me/mailFolders/inbox/messages"),
        ));

        let subscriptions = &groups[&handle].subscriptions;
        assert_eq!(subscriptions.len(), 2);
        assert!(
            !subscriptions.iter().any(|s| s.server_id == "gone"),
            "the vanished subscription must not survive its replacement"
        );
        let fresh = subscriptions
            .iter()
            .find(|s| s.server_id == "fresh")
            .expect("replacement installed");
        assert_eq!(fresh.resource, "/me/mailFolders/inbox/messages");
        assert!(subscriptions.iter().any(|s| s.server_id == "healthy"));
        assert_eq!(groups.len(), 1);
    }

    /// The renewal worker walks a SNAPSHOT of the due subscriptions, so a
    /// concurrent `push_unsubscribe` can retire the handle (and delete its
    /// server subscriptions) while a replacement create is in flight.
    /// Re-registering the group there would tell the caller teardown
    /// succeeded while notifications kept arriving; the replacement is
    /// refused instead, and `replace_gone_subscription` deletes the
    /// server-side subscription it just minted.
    #[test]
    fn a_replacement_never_resurrects_an_unsubscribed_handle() {
        let handle = SubscriptionHandle("h".to_string());
        let mut groups: HashMap<SubscriptionHandle, GraphSubscriptionGroup> = HashMap::new();
        assert!(!install_replacement(
            &mut groups,
            &handle,
            "gone",
            state("fresh", "/me/mailFolders/inbox/messages"),
        ));
        assert!(groups.is_empty(), "an unsubscribed handle stays retired");

        // A DIFFERENT live handle is not a substitute: the replacement is
        // scoped to the handle whose subscription vanished.
        let other = SubscriptionHandle("other".to_string());
        groups.insert(
            other.clone(),
            GraphSubscriptionGroup::live(vec![state("other-sub", "/me/events")]),
        );
        assert!(!install_replacement(
            &mut groups,
            &handle,
            "gone",
            state("fresh", "/me/mailFolders/inbox/messages"),
        ));
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[&other].subscriptions.len(), 1);
    }

    /// A group that lost its stale row some other way (a terminal renewal
    /// on the same tick) still takes the replacement: the handle is live,
    /// so the resource needs coverage.
    #[test]
    fn a_replacement_installs_into_a_live_group_that_lost_the_stale_row() {
        let handle = SubscriptionHandle("h".to_string());
        let mut groups = HashMap::from([(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![state("healthy", "/me/events")]),
        )]);
        assert!(install_replacement(
            &mut groups,
            &handle,
            "already-removed",
            state("fresh", "/me/mailFolders/inbox/messages"),
        ));
        assert_eq!(groups[&handle].subscriptions.len(), 2);
    }

    /// Teardown removes only the subscription whose DELETE was confirmed.
    /// If the next DELETE fails, the remaining server ids are still in the
    /// group, so retrying the same handle can finish the cleanup rather than
    /// orphaning notifications until Graph's expiry window closes.
    #[test]
    fn unsubscribe_retains_not_yet_deleted_subscriptions_for_retry() {
        let handle = SubscriptionHandle("h".to_string());
        let mut groups = HashMap::from([(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![
                state("deleted", "/me/mailFolders/inbox/messages"),
                state("retry-me", "/me/events"),
                state("not-attempted", "/me/contacts"),
            ]),
        )]);

        assert!(!remove_subscription_from_groups(
            &mut groups,
            &handle,
            "deleted"
        ));
        let remaining = &groups[&handle].subscriptions;
        assert_eq!(remaining.len(), 2);
        assert!(remaining.iter().any(|state| state.server_id == "retry-me"));
        assert!(
            remaining
                .iter()
                .any(|state| state.server_id == "not-attempted")
        );
    }

    /// The teardown/renewal race the deletes-before-removing repair opened.
    ///
    /// Teardown now keeps the group registered while it deletes, so
    /// "registered" stopped implying "live": the renewal worker's recreate
    /// path would install a replacement into a group whose server-id
    /// snapshot had already been taken. Teardown then deleted the stale id,
    /// retired the group, and returned success while the replacement kept
    /// delivering notifications. The marker raised by
    /// `mark_group_tearing_down` is what refuses that install.
    #[test]
    fn a_replacement_never_lands_in_a_group_being_torn_down() {
        let handle = SubscriptionHandle("h".to_string());
        let mut groups = HashMap::from([(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![
                state("first", "/me/mailFolders/inbox/messages"),
                state("second", "/me/events"),
            ]),
        )]);

        // Teardown condemns the group and snapshots its ids under one lock.
        let snapshot = mark_group_tearing_down(&mut groups, &handle).expect("group registered");
        assert_eq!(snapshot, vec!["first".to_string(), "second".to_string()]);

        // The first DELETE is confirmed; the group stays registered so the
        // ids still to delete remain reachable.
        assert!(!remove_subscription_from_groups(
            &mut groups,
            &handle,
            "first"
        ));

        // The renewal worker's replacement for the vanished `second` lands
        // mid-teardown. Installing it would put a live server subscription
        // under a handle whose snapshot can never name it.
        assert!(
            !install_replacement(&mut groups, &handle, "second", state("fresh", "/me/events")),
            "a condemned group must refuse a replacement"
        );
        assert!(
            !groups[&handle]
                .subscriptions
                .iter()
                .any(|state| state.server_id == "fresh"),
            "the replacement must not become state teardown cannot see"
        );

        // Teardown finishes on the ids it snapshotted, and the handle is gone.
        assert!(remove_subscription_from_groups(
            &mut groups,
            &handle,
            "second"
        ));
        assert!(groups.is_empty(), "teardown retires the handle");
    }

    /// Condemning is idempotent and monotone: a retry after a failed DELETE
    /// re-marks the same group and returns the ids still to delete, while an
    /// unknown handle stays a no-op (teardown may be retried after a reopen).
    #[test]
    fn marking_teardown_is_idempotent_and_unknown_handles_are_none() {
        let handle = SubscriptionHandle("h".to_string());
        let mut groups = HashMap::from([(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![state("retry-me", "/me/events")]),
        )]);
        assert_eq!(
            mark_group_tearing_down(&mut groups, &handle),
            Some(vec!["retry-me".to_string()])
        );
        assert!(groups[&handle].tearing_down);
        assert_eq!(
            mark_group_tearing_down(&mut groups, &handle),
            Some(vec!["retry-me".to_string()]),
            "a teardown retry sees the ids whose DELETE has not been confirmed"
        );
        assert!(
            mark_group_tearing_down(&mut groups, &SubscriptionHandle("never".to_string()))
                .is_none()
        );
    }

    /// A condemned group is not renewed either. Renewal would extend the
    /// life of exactly what the caller asked to delete, and the recreate
    /// branch behind it is the same orphan by another route.
    #[test]
    fn due_renewals_skip_a_group_being_torn_down() {
        let live = SubscriptionHandle("live".to_string());
        let condemned = SubscriptionHandle("condemned".to_string());
        let mut groups = HashMap::from([
            (
                live.clone(),
                GraphSubscriptionGroup::live(vec![expiring("live-sub", "/me/events")]),
            ),
            (
                condemned.clone(),
                GraphSubscriptionGroup::live(vec![expiring("condemned-sub", "/me/contacts")]),
            ),
        ]);
        assert!(mark_group_tearing_down(&mut groups, &condemned).is_some());

        let due = due_renewals(&groups);
        assert_eq!(due.len(), 1, "only the live group is due");
        assert_eq!(due[0].handle, live);
        assert_eq!(due[0].server_id, "live-sub");
        assert_eq!(due[0].resource, "/me/events");
        // The scopes ride along so a terminal failure can name what lost
        // coverage rather than reporting an unattributed `Terminated`.
        assert!(!due[0].scopes.is_empty());

        // And an expiry outside the threshold is not due at all.
        groups.get_mut(&live).expect("live group").subscriptions[0].expires_at =
            "2099-01-01T00:00:00Z".to_string();
        assert!(due_renewals(&groups).is_empty());
    }

    /// Letting the worker exit on condemned-only groups opened a window in
    /// which a brand-new subscription got NO renewal worker: the worker had
    /// decided to exit but its `JoinHandle` was not finished yet, so the
    /// concurrent `push_subscribe`'s `ensure_graph_worker` saw a live handle
    /// and declined to spawn; the old task then completed and nothing renewed
    /// the new subscription until some later subscribe. The exit therefore
    /// clears the slot itself, under the guard that made the decision.
    ///
    /// The tick here is driven with paused time and the map holds only a
    /// condemned group, so the worker reaches its exit without a single HTTP
    /// call. (The renewal HTTP legs themselves are scripted through the
    /// REST seam where a test needs them - see the recreate test below.)
    #[tokio::test(start_paused = true)]
    async fn a_retiring_worker_clears_its_slot_so_the_next_subscribe_respawns() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let handle = SubscriptionHandle("condemned".to_string());
        {
            let mut groups = account.graph_subscriptions.write().await;
            groups.insert(
                handle.clone(),
                GraphSubscriptionGroup::live(vec![state("sub", "/me/events")]),
            );
            assert!(mark_group_tearing_down(&mut groups, &handle).is_some());
        }

        ensure_graph_worker(account.clone()).await;
        assert!(
            account.graph_worker.lock().await.is_some(),
            "the worker slot is occupied while the worker runs"
        );

        // Drive renewal ticks until the worker retires. Time is paused, so
        // this advances the test clock rather than the wall clock; the extra
        // iterations only exist because the worker has to be scheduled onto
        // its first `sleep` before a tick can fire at all.
        for _ in 0..64 {
            if account.graph_worker.lock().await.is_none() {
                break;
            }
            tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            account.graph_worker.lock().await.is_none(),
            "a retiring worker must clear its own slot"
        );

        // The next subscription therefore gets a worker again.
        account.graph_subscriptions.write().await.insert(
            SubscriptionHandle("fresh".to_string()),
            GraphSubscriptionGroup::live(vec![state("fresh-sub", "/me/events")]),
        );
        ensure_graph_worker(account.clone()).await;
        assert!(
            account.graph_worker.lock().await.is_some(),
            "a live subscription must always have a renewal worker"
        );
        account.shutdown.cancel();
    }

    /// The recreate leg, end to end through the REST seam with paused time:
    /// a renewal PATCH that 404s (Graph retains no deleted subscription)
    /// must mint a fresh create for the SAME resource, install it in place
    /// of the vanished row under the same handle, and emit `Reconnected` -
    /// the engine's full-reconcile trigger - because nothing was delivered
    /// between the disappearance and the replacement. No `Disconnected`
    /// precedes it: the recovery succeeded within one tick.
    #[tokio::test(start_paused = true)]
    async fn a_vanished_subscription_is_recreated_and_reconnected_on_the_next_tick() {
        let client = GraphClient::new("token");
        client.script_rest([
            // The renewal PATCH answers 404: the subscription vanished.
            ScriptedRestResponse::json(
                reqwest::StatusCode::NOT_FOUND,
                serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
            ),
            // The replacement create.
            ScriptedRestResponse::json(
                reqwest::StatusCode::CREATED,
                serde_json::json!({"id":"fresh","expirationDateTime":"2099-01-01T00:00:00Z"}),
            ),
        ]);
        let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        account.push_endpoint = Some(PushEndpoint {
            webhook_url: "https://example.test/hook".to_string(),
            client_state: "secret".to_string(),
        });
        let mut events = account.push_tx.subscribe();
        let handle = SubscriptionHandle("h".to_string());
        account.graph_subscriptions.write().await.insert(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![expiring("stale", "/me/mailFolders/inbox/messages")]),
        );

        ensure_graph_worker(account.clone()).await;
        // Drive renewal ticks until the replacement lands. Paused time, so
        // this advances the test clock; the loop bound only covers task
        // scheduling slack.
        for _ in 0..64 {
            let installed = account
                .graph_subscriptions
                .read()
                .await
                .get(&handle)
                .is_some_and(|group| {
                    group
                        .subscriptions
                        .iter()
                        .any(|state| state.server_id == "fresh")
                });
            if installed {
                break;
            }
            tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        {
            let groups = account.graph_subscriptions.read().await;
            let group = groups.get(&handle).expect("the handle stays registered");
            assert_eq!(group.subscriptions.len(), 1, "in place, not beside");
            assert_eq!(group.subscriptions[0].server_id, "fresh");
            assert_eq!(
                group.subscriptions[0].resource,
                "/me/mailFolders/inbox/messages"
            );
        }

        // The failed PATCH, then the create for the same resource - and the
        // create carries the caller-owned clientState like any first-time
        // subscribe would.
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, "PATCH");
        assert!(requests[0].url.ends_with("/subscriptions/stale"));
        assert_eq!(requests[1].method, "POST");
        assert!(requests[1].url.ends_with("/subscriptions"));
        let created = requests[1].body.as_ref().expect("create body");
        assert_eq!(
            created["resource"].as_str(),
            Some("/me/mailFolders/inbox/messages")
        );
        assert_eq!(created["clientState"].as_str(), Some("secret"));

        match events.try_recv() {
            Ok(WatchEvent::Reconnected) => {}
            other => panic!("expected Reconnected, got {other:?}"),
        }
        assert!(
            events.try_recv().is_err(),
            "a within-tick recovery emits no Disconnected"
        );

        account.shutdown.cancel();
    }

    /// The plain SUCCESS leg, driven as a loop across ticks.
    ///
    /// A due subscription is PATCHed once; the new expiry it returns is
    /// written back into the state the NEXT tick reads, so the subscription
    /// stops being due and no further request is issued no matter how many
    /// ticks fire. Renewing on every tick (the failure mode a state that
    /// never updates produces) would be an unbounded request loop against
    /// Graph, and the seam is armed with exactly one response, so a second
    /// PATCH panics rather than passing.
    ///
    /// The quiet path is also pinned: a healthy renewal emits no push event
    /// at all - neither `Disconnected` nor a spurious `Reconnected`.
    #[tokio::test(start_paused = true)]
    async fn a_successful_renewal_updates_the_expiry_and_the_next_tick_finds_nothing_due() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::empty(reqwest::StatusCode::OK)]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let mut events = account.push_tx.subscribe();
        let handle = SubscriptionHandle("h".to_string());
        account.graph_subscriptions.write().await.insert(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![expiring("sub", "/me/mailFolders/inbox/messages")]),
        );

        ensure_graph_worker(account.clone()).await;
        // Many more ticks than renewals: the point is that the extra ticks
        // issue nothing.
        for _ in 0..64 {
            tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        {
            let groups = account.graph_subscriptions.read().await;
            let group = groups.get(&handle).expect("the handle stays registered");
            assert_eq!(group.subscriptions.len(), 1);
            assert_eq!(group.subscriptions[0].server_id, "sub", "renewed in place");
            assert_ne!(
                group.subscriptions[0].expires_at, "2000-01-01T00:00:00Z",
                "the renewed expiry must replace the stale one, or every \
                 tick re-renews forever"
            );
            assert!(!is_expiring_soon(
                &group.subscriptions[0].expires_at,
                RENEWAL_THRESHOLD_MINUTES
            ));
        }

        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 1, "one renewal, not one per tick");
        assert_eq!(requests[0].method, "PATCH");
        assert!(requests[0].url.ends_with("/subscriptions/sub"));
        assert!(
            requests[0].body.as_ref().expect("renewal body")["expirationDateTime"]
                .as_str()
                .is_some_and(|expiry| expiry.ends_with('Z')),
            "the PATCH carries the new expiry it then stores"
        );
        assert!(
            events.try_recv().is_err(),
            "a healthy renewal is silent on the push channel"
        );

        account.shutdown.cancel();
    }

    /// A terminal renewal failure must name the scope that lost push
    /// coverage. `subscribe_graph` groups by resource string and used to
    /// discard the scopes it grouped, so `Terminated` arrived with no
    /// attribution at all and the engine could not tell which scopes to fall
    /// back to polling.
    #[tokio::test(start_paused = true)]
    async fn a_terminal_renewal_failure_names_the_scope_that_lost_coverage() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::FORBIDDEN,
            serde_json::json!({"error":{"code":"ErrorAccessDenied","message":"no"}}),
        )]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let mut events = account.push_tx.subscribe();
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let mut state = expiring("sub", "/me/mailFolders/inbox/messages");
        state.scopes = vec![scope.clone()];
        account.graph_subscriptions.write().await.insert(
            SubscriptionHandle("h".to_string()),
            GraphSubscriptionGroup::live(vec![state]),
        );

        ensure_graph_worker(account.clone()).await;
        // A terminal failure retires the local state, so the group emptying
        // is the signal the tick has run.
        for _ in 0..64 {
            let retired = account
                .graph_subscriptions
                .read()
                .await
                .values()
                .all(|group| group.subscriptions.is_empty());
            if retired {
                break;
            }
            tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;

        let mut terminated = None;
        while let Ok(event) = events.try_recv() {
            match event {
                WatchEvent::Terminated(error) => terminated = Some(error),
                WatchEvent::Disconnected => {}
                other => panic!("unexpected push event {other:?}"),
            }
        }
        let terminated = terminated.expect("a terminal failure is reported");
        assert_eq!(terminated.scope(), Some(&ErrorScope::Cursor(scope)));

        account.shutdown.cancel();
    }

    /// The recovery edge of the same loop: a retryable renewal failure
    /// announces `Disconnected` ONCE, and the tick that finally succeeds
    /// announces `Reconnected`. Without the success leg clearing the
    /// latch, a recovered subscription would stay reported as
    /// disconnected for the life of the account.
    #[tokio::test(start_paused = true)]
    async fn a_failed_tick_disconnects_and_the_next_successful_tick_reconnects() {
        let client = GraphClient::new("token");
        client.script_rest([
            // Retryable (429), so the subscription stays installed and due.
            ScriptedRestResponse::json(
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                serde_json::json!({"error":{"code":"activityLimitReached","message":"slow down"}}),
            ),
            ScriptedRestResponse::empty(reqwest::StatusCode::OK),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let mut events = account.push_tx.subscribe();
        let handle = SubscriptionHandle("h".to_string());
        account.graph_subscriptions.write().await.insert(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![expiring("sub", "/me/events")]),
        );

        ensure_graph_worker(account.clone()).await;
        for _ in 0..64 {
            let renewed = account
                .graph_subscriptions
                .read()
                .await
                .get(&handle)
                .is_some_and(|group| {
                    !is_expiring_soon(
                        &group.subscriptions[0].expires_at,
                        RENEWAL_THRESHOLD_MINUTES,
                    )
                });
            if renewed {
                break;
            }
            tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        // Let the tick that renewed finish its post-loop bookkeeping (the
        // `Reconnected` it owes) before the channel is read.
        tokio::task::yield_now().await;

        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2, "the failed renewal is retried once");
        assert!(requests.iter().all(|request| request.method == "PATCH"));

        match events.try_recv() {
            Ok(WatchEvent::Disconnected) => {}
            other => panic!("expected Disconnected, got {other:?}"),
        }
        match events.try_recv() {
            Ok(WatchEvent::Reconnected) => {}
            other => panic!("expected Reconnected, got {other:?}"),
        }
        assert!(
            events.try_recv().is_err(),
            "the latch must not re-announce on later quiet ticks"
        );

        account.shutdown.cancel();
    }

    #[test]
    fn condemned_only_groups_do_not_keep_the_renewal_worker_alive() {
        let handle = SubscriptionHandle("condemned".to_string());
        let mut groups = HashMap::from([(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![expiring("sub", "/me/events")]),
        )]);
        assert!(has_live_graph_subscription_group(&groups));
        assert!(mark_group_tearing_down(&mut groups, &handle).is_some());
        assert!(!has_live_graph_subscription_group(&groups));
    }

    /// An empty scope list is not a subscription. Registering one minted a
    /// handle over an empty group: teardown had no server id to walk, so the
    /// group was never retired and the renewal worker it started was never
    /// stopped. Both modes refuse it before any state is installed.
    #[tokio::test]
    async fn subscribing_to_no_scopes_is_rejected_in_both_modes() {
        for mode in [PushMode::GraphSubscriptions, PushMode::EwsStreaming] {
            let account = GraphAccount::new_for_tests(GraphClient::new("token"), mode);
            let error = push_subscribe(account.clone(), Vec::new())
                .await
                .expect_err("an empty scope list covers nothing");
            assert!(matches!(
                error.kind(),
                AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
            ));
            assert!(account.graph_subscriptions.read().await.is_empty());
            assert!(account.ews_subscriptions.read().await.is_empty());
        }
    }

    #[tokio::test]
    async fn unsubscribing_an_unknown_handle_is_a_no_op() {
        // Idempotent teardown: the engine may retry `push_unsubscribe`
        // after a reopen, when the in-memory group map is already empty.
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        unsubscribe_graph(account, SubscriptionHandle("never-issued".to_string()))
            .await
            .expect("unknown handle unsubscribes cleanly");
    }

    fn email_scope(folder: &str) -> CursorScope {
        CursorScope::FolderType {
            folder: FolderId(folder.to_string()),
            ty: ObjectType::Email,
        }
    }

    /// What `subscribe_ews` hands the translation phase: every scope already
    /// validated and paired with the native id it contributes.
    fn pending(folders: &[&str]) -> Vec<(CursorScope, String)> {
        folders
            .iter()
            .map(|folder| {
                let scope = email_scope(folder);
                let source_id =
                    ews_subscribable_folder_id(&scope).expect("a primary folder scope is pending");
                (scope, source_id)
            })
            .collect()
    }

    fn converted(source_id: &str, target_id: &str) -> TranslatedExchangeId {
        TranslatedExchangeId {
            source_id: source_id.to_string(),
            target_id: Some(target_id.to_string()),
            error_details: None,
        }
    }

    #[test]
    fn translated_ews_ids_stay_paired_with_their_graph_scopes_when_response_is_reordered() {
        let translated = vec![
            converted("rest-archive", "ews-archive"),
            converted("rest-inbox", "ews-inbox"),
        ];

        let resolved =
            reconcile_translated_ews_scopes(pending(&["rest-inbox", "rest-archive"]), translated)
                .expect("every subscribed folder was translated");
        assert_eq!(resolved[0].ews_folder_id, "ews-inbox");
        assert_eq!(resolved[1].ews_folder_id, "ews-archive");
    }

    /// Graph must answer every id it was given. A dropped answer is the
    /// provider breaking its own contract, so it classifies as one - and it
    /// still names the folder, because "a subscription failed" without a
    /// container is not a diagnosable report.
    #[test]
    fn a_missing_ews_translation_rejects_the_subscription() {
        let error = reconcile_translated_ews_scopes(pending(&["rest-inbox"]), Vec::new())
            .expect_err("a REST id must never be sent to EWS as a fallback");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        ));
        assert_eq!(
            error.scope(),
            Some(&ErrorScope::Cursor(email_scope("rest-inbox")))
        );
    }

    /// Graph converts ids one at a time and reports a failure per id inside
    /// an otherwise successful 200. The refused id must fail its own
    /// subscription with its own scope and Graph's own code - not take the
    /// whole response down as an unparseable body, which is what a required
    /// `targetId` produced.
    #[test]
    fn a_per_id_translation_failure_names_its_scope_and_the_provider_code() {
        let translated = vec![
            converted("rest-inbox", "ews-inbox"),
            TranslatedExchangeId {
                source_id: "rest-stale".to_string(),
                target_id: None,
                error_details: Some(ConvertIdError {
                    code: Some("ErrorInvalidIdMalformed".to_string()),
                    message: Some("Id is malformed.".to_string()),
                }),
            },
        ];

        let error =
            reconcile_translated_ews_scopes(pending(&["rest-inbox", "rest-stale"]), translated)
                .expect_err("a folder with no EWS id cannot be subscribed");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(error.recovery().is_terminal());
        assert_eq!(
            error.scope(),
            Some(&ErrorScope::Cursor(email_scope("rest-stale")))
        );
        let carries_code = error.chain().iter().any(|cause| {
            matches!(
                cause,
                Cause::Wire(bifrost_types::WireCause::Graph(
                    bifrost_types::GraphSignal::Unknown { code }
                )) if code == "ErrorInvalidIdMalformed"
            )
        });
        assert!(carries_code, "the provider code must survive to support");
    }

    /// The one shape that would otherwise fall through with no id at all.
    #[test]
    fn a_translation_answer_with_neither_target_nor_error_is_a_contract_violation() {
        let translated = vec![TranslatedExchangeId {
            source_id: "rest-inbox".to_string(),
            target_id: Some("   ".to_string()),
            error_details: None,
        }];
        let error = reconcile_translated_ews_scopes(pending(&["rest-inbox"]), translated)
            .expect_err("a blank target id is not a translation");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        ));
    }

    /// A `restId` is per folder, not per scope, so one answer serves every
    /// scope over that folder. Asking twice would waste a slot against
    /// Graph's cap for no gain.
    #[test]
    fn scopes_sharing_a_folder_translate_once_and_both_resolve() {
        let pending = vec![
            (email_scope("rest-shared"), "rest-shared".to_string()),
            (
                CursorScope::FolderType {
                    folder: FolderId("rest-shared".to_string()),
                    ty: ObjectType::Contact,
                },
                "rest-shared".to_string(),
            ),
        ];
        let chunks = translation_input_chunks(&pending);
        assert_eq!(chunks, vec![vec!["rest-shared".to_string()]]);

        let resolved =
            reconcile_translated_ews_scopes(pending, vec![converted("rest-shared", "ews-shared")])
                .expect("one answer serves both scopes");
        assert_eq!(resolved.len(), 2);
        assert!(resolved.iter().all(|s| s.ews_folder_id == "ews-shared"));
    }

    /// Graph rejects an `inputIds` collection over 1,000 strings outright,
    /// so a large mailbox has to fan out. Before chunking, every scope went
    /// into one collection and the subscription failed before EWS setup was
    /// even attempted.
    #[test]
    fn translation_requests_stay_inside_the_graph_input_id_cap() {
        let folders: Vec<String> = (0..2_500).map(|n| format!("rest-{n}")).collect();
        let pending: Vec<(CursorScope, String)> = folders
            .iter()
            .map(|folder| (email_scope(folder), folder.clone()))
            .collect();

        let chunks = translation_input_chunks(&pending);
        assert_eq!(chunks.len(), 3);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.len() <= TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
        );
        let flattened: Vec<String> = chunks.into_iter().flatten().collect();
        assert_eq!(flattened, folders, "order and coverage both survive");
    }

    /// The response Graph documents, decoded end to end: a converted id and
    /// a refused one in the same 200. Deserialization must survive the
    /// refused entry - the whole finding was that it did not.
    #[test]
    fn a_mixed_convert_id_result_deserializes() {
        let body = r#"{
            "value": [
                { "sourceId": "rest-a", "targetId": "ews-a" },
                {
                    "sourceId": "rest-b",
                    "errorDetails": {
                        "code": "ErrorInvalidIdMalformed",
                        "message": "Id is malformed."
                    }
                }
            ]
        }"#;
        let response: TranslateExchangeIdsResponse =
            serde_json::from_str(body).expect("a per-id failure is not a parse failure");
        assert_eq!(response.value.len(), 2);
        assert_eq!(response.value[0].target_id.as_deref(), Some("ews-a"));
        assert!(response.value[1].target_id.is_none());
        assert_eq!(
            response.value[1]
                .error_details
                .as_ref()
                .and_then(|details| details.code.as_deref()),
            Some("ErrorInvalidIdMalformed")
        );
    }

    /// The translation POST creates nothing: it runs before the handle, the
    /// local state, and the EWS subscription all exist. A drop mid-flight
    /// has no target to reconcile against, so it must stay a plain retry
    /// even though `PushSubscribe` is a non-idempotent operation.
    #[test]
    fn a_dropped_translation_request_retries_instead_of_reconciling() {
        let dropped = crate::error::GraphError::Net(bifrost_net::Error::Network {
            message: "connection reset".to_string(),
            transmission_state: TransmissionState::InFlight,
            source: None,
        });
        let error = into_account_error(dropped, translate_error_context());
        assert!(
            error.recovery().is_retryable(),
            "recovery was {:?}",
            error.recovery()
        );

        // The same drop under the plain PushSubscribe context is the
        // behavior the override corrects, not a coincidence of the kind.
        let dropped = crate::error::GraphError::Net(bifrost_net::Error::Network {
            message: "connection reset".to_string(),
            transmission_state: TransmissionState::InFlight,
            source: None,
        });
        let reconciled = into_account_error(
            dropped,
            GraphErrorContext::graph(AccountOperation::PushSubscribe),
        );
        assert!(reconciled.recovery().requires_reconciliation());
    }
}
