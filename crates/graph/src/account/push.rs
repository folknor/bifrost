use std::collections::HashMap;
use std::time::Duration;

use bifrost_types::{CursorScope, Error, ObjectType, SubscriptionHandle, WatchEvent};

use crate::webhooks::{
    create_subscription, delete_subscription, is_expiring_soon, renew_subscription,
};

use super::{GraphAccount, PushMode};

const RENEWAL_CHECK_INTERVAL: Duration = Duration::from_secs(10 * 60);
const RENEWAL_THRESHOLD_MINUTES: i64 = 30;

#[derive(Debug, Clone)]
pub struct PushEndpoint {
    pub webhook_url: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct GraphSubscriptionGroup {
    pub subscriptions: Vec<GraphSubscriptionState>,
    pub scopes: Vec<CursorScope>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct GraphSubscriptionState {
    pub server_id: String,
    pub resource: String,
    pub client_state: String,
    pub expires_at: String,
}

#[derive(Debug, Clone)]
pub(crate) struct EwsSubscriptionState {
    pub ews_subscription_id: Option<String>,
    pub watermark: Option<String>,
    pub scopes: Vec<CursorScope>,
}

pub(crate) async fn push_subscribe(
    account: GraphAccount,
    scopes: Vec<CursorScope>,
) -> Result<SubscriptionHandle, Error> {
    match account.push_mode {
        PushMode::GraphSubscriptions => subscribe_graph(account, scopes).await,
        PushMode::EwsStreaming => subscribe_ews(account, scopes).await,
    }
}

pub(crate) async fn push_unsubscribe(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), Error> {
    match account.push_mode {
        PushMode::GraphSubscriptions => unsubscribe_graph(account, handle).await,
        PushMode::EwsStreaming => unsubscribe_ews(account, handle).await,
    }
}

async fn subscribe_graph(
    account: GraphAccount,
    scopes: Vec<CursorScope>,
) -> Result<SubscriptionHandle, Error> {
    let Some(endpoint) = account.push_endpoint.clone() else {
        return Err(Error::MissingCoreCapability);
    };
    let mut grouped: HashMap<String, Vec<CursorScope>> = HashMap::new();
    for scope in scopes {
        if let Some(resource) = resource_for_scope(&account, &scope) {
            grouped.entry(resource).or_default().push(scope);
        }
    }

    let mut subscriptions = Vec::new();
    let mut all_scopes = Vec::new();
    for (resource, resource_scopes) in grouped {
        let response = create_subscription(&account.client, &resource, &endpoint.webhook_url, None)
            .await
            .map_err(Error::Transport)?;
        all_scopes.extend(resource_scopes);
        subscriptions.push(GraphSubscriptionState {
            server_id: response.id,
            resource,
            client_state: response.client_state.unwrap_or_default(),
            expires_at: response.expiration_date_time,
        });
    }

    let handle = new_handle()?;
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup {
            subscriptions,
            scopes: all_scopes,
        },
    );
    let _ = account.push_tx.send(WatchEvent::Reconnected);
    ensure_graph_worker(account).await;
    Ok(handle)
}

async fn unsubscribe_graph(account: GraphAccount, handle: SubscriptionHandle) -> Result<(), Error> {
    let Some(group) = account.graph_subscriptions.write().await.remove(&handle) else {
        return Ok(());
    };
    for state in group.subscriptions {
        delete_subscription(&account.client, &state.server_id)
            .await
            .map_err(Error::Transport)?;
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
                    tracing::warn!(
                        "[Graph webhooks] Failed to renew subscription {server_id}: {error}"
                    );
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
) -> Result<SubscriptionHandle, Error> {
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

async fn unsubscribe_ews(account: GraphAccount, handle: SubscriptionHandle) -> Result<(), Error> {
    account.ews_subscriptions.write().await.remove(&handle);
    Ok(())
}

fn resource_for_scope(account: &GraphAccount, scope: &CursorScope) -> Option<String> {
    let prefix = account.client.api_path_prefix();
    match scope {
        CursorScope::FolderType { folder, ty } => match ty {
            ObjectType::Email => {
                let encoded = bifrost_net::url::encode_component(&folder.0);
                Some(format!("{prefix}/mailFolders/{encoded}/messages"))
            }
            ObjectType::Event | ObjectType::CalendarEvent => Some(format!("{prefix}/events")),
            ObjectType::Contact => {
                let encoded = bifrost_net::url::encode_component(&folder.0);
                Some(format!("{prefix}/contactFolders/{encoded}/contacts"))
            }
            _ => None,
        },
        _ => None,
    }
}

fn new_handle() -> Result<SubscriptionHandle, Error> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| Error::Other(error.to_string()))?;
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
}
