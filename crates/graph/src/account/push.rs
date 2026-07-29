use std::collections::HashMap;
use std::time::Duration;

use bifrost_types::WatchEvent;
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, CursorScope,
    ObjectType, Protocol, Provider, RequestCause, SubscriptionHandle, TransmissionState,
};

use crate::webhooks::{
    create_subscription, delete_subscription, is_expiring_soon, renew_subscription,
};

use super::graph_error::{GraphErrorContext, into_account_error};
use super::{GraphAccount, PushMode};

const RENEWAL_CHECK_INTERVAL: Duration = Duration::from_secs(10 * 60);
const RENEWAL_THRESHOLD_MINUTES: i64 = 30;

#[derive(Debug, Clone)]
pub(crate) struct PushEndpoint {
    pub(crate) webhook_url: String,
}

#[derive(Debug, Clone)]
pub(crate) struct GraphSubscriptionGroup {
    pub(crate) subscriptions: Vec<GraphSubscriptionState>,
}

#[derive(Debug, Clone)]
pub(crate) struct GraphSubscriptionState {
    pub(crate) server_id: String,
    pub(crate) expires_at: String,
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

    let mut subscriptions = Vec::new();
    for (resource, _) in grouped {
        let response = create_subscription(&account.client, &resource, &endpoint.webhook_url, None)
            .await
            .map_err(|e| {
                into_account_error(e, GraphErrorContext::graph(AccountOperation::PushSubscribe))
            })?;
        subscriptions.push(GraphSubscriptionState {
            server_id: response.id,
            expires_at: response.expiration_date_time,
        });
    }

    let handle = new_handle()?;
    account
        .graph_subscriptions
        .write()
        .await
        .insert(handle.clone(), GraphSubscriptionGroup { subscriptions });
    let _ = account.push_tx.send(WatchEvent::Reconnected);
    ensure_graph_worker(account).await;
    Ok(handle)
}

async fn unsubscribe_graph(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), AccountError> {
    let Some(group) = account.graph_subscriptions.write().await.remove(&handle) else {
        return Ok(());
    };
    for state in group.subscriptions {
        delete_subscription(&account.client, &state.server_id)
            .await
            .map_err(|e| {
                into_account_error(
                    e,
                    GraphErrorContext::graph(AccountOperation::PushUnsubscribe),
                )
            })?;
    }
    if account.graph_subscriptions.read().await.is_empty()
        && let Some(worker) = account.graph_worker.lock().await.take()
    {
        worker.abort();
    }
    Ok(())
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
            groups
                .iter()
                .flat_map(|(handle, group)| {
                    group.subscriptions.iter().filter_map(move |state| {
                        if is_expiring_soon(&state.expires_at, RENEWAL_THRESHOLD_MINUTES) {
                            Some((handle.clone(), state.server_id.clone()))
                        } else {
                            None
                        }
                    })
                })
                .collect::<Vec<_>>()
        };

        let mut had_error = false;
        for (handle, server_id) in due {
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
                            "Graph webhook renewal failed"
                        );
                    }
                    if account_error.recovery().is_terminal() {
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

async fn subscribe_ews(
    account: GraphAccount,
    scopes: Vec<CursorScope>,
) -> Result<SubscriptionHandle, AccountError> {
    let handle = new_handle()?;
    account.ews_subscriptions.write().await.insert(
        handle.clone(),
        EwsSubscriptionState {
            ews_subscription_id: None,
            watermark: None,
            scopes,
        },
    );
    super::push_stream::ensure_ews_worker(account).await;
    Ok(handle)
}

async fn unsubscribe_ews(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), AccountError> {
    account.ews_subscriptions.write().await.remove(&handle);
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
                let encoded = bifrost_net::url::encode_component(&native);
                Some(format!("{prefix}/mailFolders/{encoded}/messages"))
            }
            ObjectType::Event | ObjectType::CalendarEvent => Some(format!("{prefix}/events")),
            ObjectType::Contact => {
                let native = super::foreign::parse_folder(folder).native_id().to_string();
                let encoded = bifrost_net::url::encode_component(&native);
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
