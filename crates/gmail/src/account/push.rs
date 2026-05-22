use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bifrost_types::{
    AccountFuture, AccountStream, CursorScope, Error as AccountError, SubscriptionHandle,
    WatchEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;

use super::recovery;

const DEFAULT_RENEW_AFTER: Duration = Duration::from_secs(6 * 24 * 60 * 60);
const RENEW_BEFORE_EXPIRY: Duration = Duration::from_secs(24 * 60 * 60);
const RENEW_RETRY_AFTER: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone)]
pub struct PubSubConfig {
    pub topic: String,
    pub label_ids: Vec<String>,
}

impl PubSubConfig {
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            label_ids: Vec::new(),
        }
    }

    pub fn with_label_ids(
        mut self,
        label_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.label_ids = label_ids.into_iter().map(Into::into).collect();
        self
    }
}

pub(crate) struct PubSubControl {
    config: Option<PubSubConfig>,
    last_history_id: Mutex<Option<String>>,
    expiration: Mutex<Option<SystemTime>>,
    renewer: Mutex<Option<JoinHandle<()>>>,
    active_handles: Mutex<HashSet<String>>,
    health_tx: broadcast::Sender<WatchEvent>,
}

impl PubSubControl {
    pub(crate) fn new(config: Option<PubSubConfig>) -> Self {
        let (health_tx, _) = broadcast::channel(32);
        Self {
            config,
            last_history_id: Mutex::new(None),
            expiration: Mutex::new(None),
            renewer: Mutex::new(None),
            active_handles: Mutex::new(HashSet::new()),
            health_tx,
        }
    }

    pub(crate) fn config(&self) -> Option<&PubSubConfig> {
        self.config.as_ref()
    }

    async fn store_watch_response(&self, response: &GmailWatchResponse) {
        *self.last_history_id.lock().await = Some(response.history_id.clone());
        *self.expiration.lock().await = response.expiration.as_deref().and_then(parse_expiration);
    }

    pub(crate) async fn abort_renewer(&self) {
        if let Some(handle) = self.renewer.lock().await.take() {
            handle.abort();
        }
    }

    async fn insert_handle(&self, handle: &SubscriptionHandle) {
        self.active_handles.lock().await.insert(handle.0.clone());
    }

    async fn remove_handle(&self, handle: &SubscriptionHandle) -> bool {
        let mut handles = self.active_handles.lock().await;
        handles.remove(&handle.0);
        handles.is_empty()
    }

    fn report_health(&self, event: WatchEvent) {
        let _ = self.health_tx.send(event);
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GmailWatchResponse {
    history_id: String,
    expiration: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GmailSubscriptionHandle {
    topic: String,
    history_id: String,
    expiration: Option<String>,
}

pub(crate) fn push_subscribe(
    client: Arc<GmailClient>,
    pubsub: Arc<PubSubControl>,
    shutdown: CancellationToken,
    scopes: Vec<CursorScope>,
) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
    Box::pin(async move {
        if scopes.is_empty()
            || scopes
                .iter()
                .any(|scope| !matches!(scope, CursorScope::Account))
        {
            return Err(AccountError::Unsupported);
        }
        let Some(config) = pubsub.config().cloned() else {
            return Err(AccountError::Unsupported);
        };
        let response = watch_once(&client, &config)
            .await
            .map_err(|error| recovery::account_error_from_gmail(&error))?;
        pubsub.store_watch_response(&response).await;
        pubsub.report_health(WatchEvent::Reconnected);
        start_renewer(
            Arc::clone(&client),
            Arc::clone(&pubsub),
            config.clone(),
            shutdown,
        )
        .await;
        let handle = GmailSubscriptionHandle {
            topic: config.topic,
            history_id: response.history_id,
            expiration: response.expiration,
        };
        let handle = serde_json::to_string(&handle)
            .map(SubscriptionHandle)
            .map_err(|error| AccountError::Other(error.to_string()))?;
        pubsub.insert_handle(&handle).await;
        Ok(handle)
    })
}

pub(crate) fn push_unsubscribe(
    client: Arc<GmailClient>,
    pubsub: Arc<PubSubControl>,
    handle: SubscriptionHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let _decoded: GmailSubscriptionHandle =
            serde_json::from_str(&handle.0).map_err(|error| {
                AccountError::Other(format!("invalid gmail subscription handle: {error}"))
            })?;
        if !pubsub.remove_handle(&handle).await {
            return Ok(());
        }
        stop_watch(&client)
            .await
            .map_err(|error| recovery::account_error_from_gmail(&error))?;
        *pubsub.expiration.lock().await = None;
        *pubsub.last_history_id.lock().await = None;
        pubsub.abort_renewer().await;
        Ok(())
    })
}

pub(crate) fn push_stream(
    pubsub: Arc<PubSubControl>,
    shutdown: CancellationToken,
) -> AccountStream<WatchEvent> {
    let receiver = pubsub.health_tx.subscribe();
    Box::pin(futures::stream::unfold(
        (receiver, shutdown),
        |(mut receiver, shutdown)| async move {
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return None,
                    result = receiver.recv() => {
                        match result {
                            Ok(event) => return Some((event, (receiver, shutdown))),
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => return None,
                        }
                    }
                }
            }
        },
    ))
}

async fn start_renewer(
    client: Arc<GmailClient>,
    pubsub: Arc<PubSubControl>,
    config: PubSubConfig,
    shutdown: CancellationToken,
) {
    let mut guard = pubsub.renewer.lock().await;
    if guard.is_some() {
        return;
    }
    let control = Arc::clone(&pubsub);
    *guard = Some(tokio::spawn(async move {
        let mut disconnected = false;
        let mut retry_after = None;
        loop {
            let delay = match retry_after.take() {
                Some(delay) => delay,
                None => {
                    let expiration = *control.expiration.lock().await;
                    renewal_delay(expiration)
                }
            };
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(delay) => {}
            }
            match watch_once(&client, &config).await {
                Ok(response) => {
                    control.store_watch_response(&response).await;
                    retry_after = None;
                    if disconnected {
                        control.report_health(WatchEvent::Reconnected);
                        disconnected = false;
                    }
                }
                Err(error) => {
                    tracing::warn!("gmail Pub/Sub watch renewal failed: {error}");
                    if !disconnected {
                        control.report_health(WatchEvent::Disconnected);
                        disconnected = true;
                    }
                    retry_after = Some(RENEW_RETRY_AFTER);
                }
            }
        }
    }));
}

async fn watch_once(
    client: &GmailClient,
    config: &PubSubConfig,
) -> crate::Result<GmailWatchResponse> {
    let mut body = json!({ "topicName": config.topic });
    if !config.label_ids.is_empty() {
        body["labelIds"] = json!(config.label_ids);
    }
    client.post("/watch", &body).await
}

async fn stop_watch(client: &GmailClient) -> crate::Result<()> {
    client.post_no_content("/stop", &json!({})).await
}

fn parse_expiration(value: &str) -> Option<SystemTime> {
    let millis = value.parse::<u64>().ok()?;
    UNIX_EPOCH.checked_add(Duration::from_millis(millis))
}

fn renewal_delay(expiration: Option<SystemTime>) -> Duration {
    let Some(expiration) = expiration else {
        return DEFAULT_RENEW_AFTER;
    };
    let Ok(until_expiration) = expiration.duration_since(SystemTime::now()) else {
        return Duration::ZERO;
    };
    until_expiration
        .checked_sub(RENEW_BEFORE_EXPIRY)
        .unwrap_or(Duration::ZERO)
}
