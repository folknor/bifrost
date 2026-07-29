use std::collections::HashMap;
use std::time::Duration;

use bifrost_types::WatchEvent;
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, CursorScope,
    ObjectType, Protocol, Provider, RequestCause, SubscriptionHandle, TransmissionState,
};

use crate::webhooks::{
    create_subscription, delete_subscription, is_expiring_soon, renew_subscription,
    subscription_is_gone,
};

use super::graph_error::{GraphErrorContext, into_account_error, invalid_account_error};
use super::{GraphAccount, PushMode};

const RENEWAL_CHECK_INTERVAL: Duration = Duration::from_secs(10 * 60);
const RENEWAL_THRESHOLD_MINUTES: i64 = 30;

#[derive(Debug, Clone)]
pub(crate) struct PushEndpoint {
    pub(crate) webhook_url: String,
    /// A consumer-owned account-wide secret carried in every Graph webhook
    /// subscription so its out-of-process receiver can validate clientState.
    pub(crate) client_state: Option<String>,
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
    fn live(subscriptions: Vec<GraphSubscriptionState>) -> Self {
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
}

#[derive(Debug, Clone)]
pub(crate) struct EwsSubscriptionState {
    pub(crate) ews_subscription_id: Option<String>,
    pub(crate) watermark: Option<String>,
    pub(crate) scopes: Vec<CursorScope>,
}

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
            Some(resource) => {
                grouped.entry(resource).or_default().push(scope);
            }
            None => return Err(unsupported_push_error()),
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
    for (resource, _) in grouped {
        match create_subscription(
            &account.client,
            &resource,
            &endpoint.webhook_url,
            endpoint.client_state.as_deref(),
            None,
        )
        .await
        {
            Ok(response) => subscriptions.push(GraphSubscriptionState {
                server_id: response.id,
                expires_at: response.expiration_date_time,
                resource,
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
    if account.graph_subscriptions.read().await.is_empty()
        && let Some(worker) = account.graph_worker.lock().await.take()
    {
        worker.abort();
    }
    Ok(())
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
            if groups.is_empty() {
                return;
            }
            due_renewals(&groups)
        };

        let mut had_error = false;
        for (handle, server_id, resource) in due {
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
                                &account, endpoint, &handle, &server_id, &resource,
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
                    let account_error = into_account_error(
                        error,
                        GraphErrorContext::graph(AccountOperation::PushSubscribe),
                    );
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

/// The `(handle, server_id, resource)` triples inside the renewal threshold.
///
/// A condemned (`tearing_down`) group contributes nothing. Renewing its
/// subscriptions would extend the life of exactly what the caller asked to
/// delete, and recreating a vanished one would hand teardown a server id its
/// snapshot cannot contain. Their rows stay registered only so that a failed
/// DELETE can be retried against them.
fn due_renewals(
    groups: &HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
) -> Vec<(SubscriptionHandle, String, String)> {
    groups
        .iter()
        .filter(|(_, group)| !group.tearing_down)
        .flat_map(|(handle, group)| {
            group.subscriptions.iter().filter_map(move |state| {
                if is_expiring_soon(&state.expires_at, RENEWAL_THRESHOLD_MINUTES) {
                    Some((
                        handle.clone(),
                        state.server_id.clone(),
                        state.resource.clone(),
                    ))
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
) -> Result<Replacement, crate::error::GraphError> {
    let response = create_subscription(
        &account.client,
        resource,
        &endpoint.webhook_url,
        endpoint.client_state.as_deref(),
        None,
    )
    .await?;
    let replacement = GraphSubscriptionState {
        server_id: response.id,
        expires_at: response.expiration_date_time,
        resource: resource.to_string(),
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

/// Whether the EWS streaming worker can actually subscribe to `scope`.
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
fn ews_scope_is_subscribable(scope: &CursorScope) -> bool {
    match scope {
        CursorScope::FolderType { folder, .. } => {
            super::foreign::parse_folder(folder).foreign().is_none()
        }
        _ => false,
    }
}

async fn subscribe_ews(
    account: GraphAccount,
    scopes: Vec<CursorScope>,
) -> Result<SubscriptionHandle, AccountError> {
    if !scopes.iter().all(ews_scope_is_subscribable) {
        return Err(unsupported_push_error());
    }
    let handle = new_handle()?;
    account.ews_subscriptions.write().await.insert(
        handle.clone(),
        EwsSubscriptionState {
            ews_subscription_id: None,
            watermark: None,
            scopes,
        },
    );
    account.ews_subscription_changed.notify_one();
    super::push_stream::ensure_ews_worker(account).await;
    Ok(handle)
}

async fn unsubscribe_ews(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), AccountError> {
    account.ews_subscriptions.write().await.remove(&handle);
    account.ews_subscription_changed.notify_one();
    Ok(())
}

fn resource_for_scope(account: &GraphAccount, scope: &CursorScope) -> Option<String> {
    // Route the resource through the scope's owning client (primary `/me`
    // or a shared mailbox's `/users/{owner}`) and use the *native* folder
    // id in the path. A foreign scope carries the owning mailbox inside the
    // `FolderId`; percent-encoding that raw foreign id into the URL (the
    // old behavior) produced a `/me/mailFolders/{owner%1Ffolder}/...`
    // resource Graph cannot resolve.
    let prefix = account.client_for_scope(scope).api_path_prefix();
    match scope {
        CursorScope::FolderType { folder, ty } => match ty {
            ObjectType::Email => {
                let native = super::foreign::parse_folder(folder).native_id().to_string();
                let encoded = bifrost_net::url::encode_path_component(&native);
                Some(format!("{prefix}/mailFolders/{encoded}/messages"))
            }
            ObjectType::Event | ObjectType::CalendarEvent => Some(format!("{prefix}/events")),
            ObjectType::Contact => {
                let native = super::foreign::parse_folder(folder).native_id().to_string();
                let encoded = bifrost_net::url::encode_path_component(&native);
                Some(format!("{prefix}/contactFolders/{encoded}/contacts"))
            }
            _ => None,
        },
        _ => None,
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

    use crate::client::GraphClient;

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
        assert!(ews_scope_is_subscribable(&primary));

        let foreign = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        assert!(!ews_scope_is_subscribable(&foreign));

        // A public-folder scope, and the account-wide scope the webhook
        // path already rejects loudly.
        assert!(!ews_scope_is_subscribable(&CursorScope::Folder(FolderId(
            "pf".to_string()
        ))));
        assert!(!ews_scope_is_subscribable(&CursorScope::Account));
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
            resource_for_scope(&account, &scope).as_deref(),
            Some("/me/mailFolders/inbox/messages")
        );
    }

    #[test]
    fn graph_event_subscription_is_account_wide() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("calendar-id".to_string()),
            ty: ObjectType::Event,
        };
        assert_eq!(
            resource_for_scope(&account, &scope).as_deref(),
            Some("/me/events")
        );
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
        let resource = resource_for_scope(&account, &scope).expect("foreign scope resolves");
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
            client_state: None,
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

    #[test]
    fn contact_scope_subscribes_to_the_contact_folder_collection() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("contacts".to_string()),
            ty: ObjectType::Contact,
        };
        assert_eq!(
            resource_for_scope(&account, &scope).as_deref(),
            Some("/me/contactFolders/contacts/contacts")
        );
    }

    #[test]
    fn unsubscribable_scopes_have_no_graph_resource() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        assert!(resource_for_scope(&account, &CursorScope::Account).is_none());
        // A public folder is poll-only; it must not resolve to a resource.
        assert!(
            resource_for_scope(
                &account,
                &CursorScope::Folder(FolderId("AAMkPF=".to_string()))
            )
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
        let resource = resource_for_scope(&account, &scope).expect("email scope resolves");
        assert!(!resource.contains("AAMk/GI2="), "{resource}");
        assert!(resource.ends_with("/messages"), "{resource}");
    }

    /// Documents current behavior worth knowing about, NOT an endorsement:
    /// `client_for_scope` falls back to the
    /// PRIMARY client for a foreign scope whose mailbox is not configured,
    /// while `parse_folder` still strips the owner off the folder id. The
    /// result subscribes `/me` to a folder id that belongs to a different
    /// mailbox. Only reachable when a persisted scope outlives the
    /// `with_shared_mailbox` entry that minted it, but it fails silently
    /// (Graph 404s the resource) rather than reporting the stale config.
    #[test]
    fn an_unconfigured_foreign_mailbox_subscribes_against_the_primary_prefix() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("other@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        assert_eq!(
            resource_for_scope(&account, &scope).as_deref(),
            Some("/me/mailFolders/AAMk/messages")
        );
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
        }
    }

    /// A subscription whose expiry is already past, so it is unconditionally
    /// inside the renewal threshold.
    fn expiring(server_id: &str, resource: &str) -> GraphSubscriptionState {
        GraphSubscriptionState {
            server_id: server_id.to_string(),
            expires_at: "2000-01-01T00:00:00Z".to_string(),
            resource: resource.to_string(),
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
        assert_eq!(due[0].0, live);
        assert_eq!(due[0].1, "live-sub");
        assert_eq!(due[0].2, "/me/events");

        // And an expiry outside the threshold is not due at all.
        groups.get_mut(&live).expect("live group").subscriptions[0].expires_at =
            "2099-01-01T00:00:00Z".to_string();
        assert!(due_renewals(&groups).is_empty());
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
}
