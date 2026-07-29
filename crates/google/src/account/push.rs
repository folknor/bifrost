use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AccountStream, CursorScope, SubscriptionHandle,
    WatchEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;

use super::error;

const DEFAULT_RENEW_AFTER: Duration = Duration::from_secs(6 * 24 * 60 * 60);
const RENEW_BEFORE_EXPIRY: Duration = Duration::from_secs(24 * 60 * 60);
const RENEW_RETRY_AFTER: Duration = Duration::from_secs(5 * 60);
/// Floor for any *computed* renewal delay.
///
/// `start_renewer` clears `retry_after` on a successful re-watch, so the
/// failure-path damper never engages on the success path. Without a floor,
/// any expiration inside the one-day renewal window - a clock skewed
/// forward, a watch whose real TTL is under a day, an expiration Gmail
/// returns unchanged - collapses the delay to zero and turns the renewer
/// into an unthrottled `users.watch` storm. Deliberately not applied to
/// the `None` fallback, which is already six days.
const MIN_RENEW_DELAY: Duration = RENEW_RETRY_AFTER;

/// Gmail Cloud Pub/Sub watch configuration for `GoogleAccountFactory`.
#[derive(Debug, Clone)]
pub struct PubSubConfig {
    /// Full Pub/Sub topic name passed to Gmail `users.watch`.
    pub topic: String,
    /// Optional Gmail label filter for watch subscriptions.
    pub label_ids: Vec<String>,
}

impl PubSubConfig {
    /// Create an account-wide watch configuration for `topic`.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            label_ids: Vec::new(),
        }
    }

    /// Restrict watch notifications to selected Gmail label ids.
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
            return Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::PushSubscribe),
                error::GmailErrorContext::push_subscribe(),
            ));
        }
        let Some(config) = pubsub.config().cloned() else {
            return Err(error::into_account_error(
                crate::error::Error::unsupported_with(
                    AccountOperation::PushSubscribe,
                    "no Pub/Sub topic configured",
                ),
                error::GmailErrorContext::push_subscribe(),
            ));
        };
        let response = watch_once(&client, &config).await.map_err(|error| {
            error::into_account_error(error, error::GmailErrorContext::push_subscribe())
        })?;
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
            .map_err(|error| {
                error::into_account_error(
                    crate::error::Error::invalid_request(
                        AccountOperation::PushSubscribe,
                        format!("subscription handle encode failed: {error}"),
                    ),
                    error::GmailErrorContext::push_subscribe(),
                )
            })?;
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
                error::into_account_error(
                    crate::error::Error::invalid_request(
                        AccountOperation::PushUnsubscribe,
                        format!("invalid gmail subscription handle: {error}"),
                    ),
                    error::GmailErrorContext::push_unsubscribe(),
                )
            })?;
        if !pubsub.remove_handle(&handle).await {
            return Ok(());
        }
        stop_watch(&client).await.map_err(|error| {
            error::into_account_error(error, error::GmailErrorContext::push_unsubscribe())
        })?;
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
                Err(err) => {
                    // gmail-D3: classify every error through the
                    // central translator and route on
                    // `is_terminal()`. Terminal classes (auth lost,
                    // policy, scope, account disabled, schema break)
                    // emit `WatchEvent::Terminated(AccountError)` and
                    // exit the renewer loop. Transient classes emit
                    // `Disconnected` once and retry on
                    // `RENEW_RETRY_AFTER`.
                    let account_error =
                        error::into_account_error(err, error::GmailErrorContext::push_subscribe());
                    if account_error.recovery().is_terminal() {
                        tracing::warn!(
                            target: "bifrost_google::push",
                            kind = ?account_error.kind(),
                            message_key = account_error.message_key(),
                            "gmail Pub/Sub watch renewal terminal failure",
                        );
                        control.report_health(WatchEvent::Terminated(account_error));
                        return;
                    }
                    // gmail-F2: emit a structured Warning alongside
                    // the Disconnected health signal so consumers see
                    // a typed transient-failure event instead of a
                    // bare tracing log.
                    let warning = bifrost_types::Warning::support_only(
                        bifrost_types::WarningKind::OperatorAttentionNeeded,
                        format!(
                            "gmail Pub/Sub renewal transient failure: {}",
                            account_error.message_key(),
                        ),
                    )
                    .with_retry_count(1);
                    control.report_health(WatchEvent::Warning(warning));
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
        return MIN_RENEW_DELAY;
    };
    until_expiration
        .checked_sub(RENEW_BEFORE_EXPIRY)
        .unwrap_or(Duration::ZERO)
        .max(MIN_RENEW_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn in_future(after: Duration) -> SystemTime {
        SystemTime::now() + after
    }

    // ---- parse_expiration ---------------------------------------------

    /// Gmail returns the watch expiration as epoch milliseconds in a
    /// JSON string.
    #[test]
    fn expiration_parses_epoch_millis() {
        let parsed = parse_expiration("1700000000000").expect("epoch millis parse");
        assert_eq!(
            parsed
                .duration_since(UNIX_EPOCH)
                .expect("after the epoch")
                .as_millis(),
            1_700_000_000_000
        );
    }

    #[test]
    fn expiration_rejects_non_numeric_and_negative_values() {
        assert!(parse_expiration("").is_none());
        assert!(parse_expiration("not-a-number").is_none());
        assert!(parse_expiration("-1").is_none());
        assert!(
            parse_expiration("1.5e12").is_none(),
            "a float rendering is not the documented shape"
        );
        assert!(
            parse_expiration(" 1700000000000 ").is_none(),
            "the parser does not trim; a padded value is rejected rather than guessed at"
        );
    }

    // ---- renewal_delay --------------------------------------------------

    /// No expiration in the watch response means Gmail told us nothing;
    /// fall back to the six-day default rather than renewing eagerly.
    #[test]
    fn missing_expiration_falls_back_to_the_six_day_default() {
        assert_eq!(renewal_delay(None), DEFAULT_RENEW_AFTER);
        assert_eq!(DEFAULT_RENEW_AFTER, Duration::from_secs(6 * 24 * 60 * 60));
    }

    /// The normal case: Gmail's watch lasts seven days, so the renewer
    /// wakes one day before expiry.
    #[test]
    fn a_seven_day_expiration_renews_one_day_early() {
        let seven_days = Duration::from_secs(7 * 24 * 60 * 60);
        let delay = renewal_delay(Some(in_future(seven_days)));
        let expected = seven_days - RENEW_BEFORE_EXPIRY;
        assert!(delay <= expected, "got {delay:?}");
        assert!(
            delay + Duration::from_secs(5) >= expected,
            "got {delay:?}, expected roughly {expected:?}"
        );
    }

    #[test]
    fn an_expiration_inside_the_renewal_window_uses_the_minimum_delay() {
        assert_eq!(
            renewal_delay(Some(in_future(Duration::from_secs(60)))),
            MIN_RENEW_DELAY,
            "one minute from expiry is inside the one-day window"
        );
        assert_eq!(
            renewal_delay(Some(in_future(RENEW_BEFORE_EXPIRY))),
            MIN_RENEW_DELAY,
            "exactly at the window boundary"
        );
        assert_eq!(
            renewal_delay(Some(SystemTime::now() - Duration::from_secs(3600))),
            MIN_RENEW_DELAY,
            "an already-expired watch still observes the floor"
        );
        assert_eq!(
            renewal_delay(Some(UNIX_EPOCH)),
            MIN_RENEW_DELAY,
            "a nonsense epoch-zero expiration behaves the same way"
        );
    }

    /// Just past the window the delay becomes positive again, so the
    /// zero above is a boundary behaviour rather than a constant.
    #[test]
    fn an_expiration_past_the_renewal_window_yields_a_positive_delay() {
        let delay = renewal_delay(Some(in_future(
            RENEW_BEFORE_EXPIRY + Duration::from_secs(600),
        )));
        assert!(delay > Duration::ZERO, "got {delay:?}");
        assert!(delay <= Duration::from_secs(600));
    }

    #[test]
    fn retry_cadence_constants_are_the_documented_ones() {
        assert_eq!(RENEW_RETRY_AFTER, Duration::from_secs(5 * 60));
        assert_eq!(RENEW_BEFORE_EXPIRY, Duration::from_secs(24 * 60 * 60));
    }

    // ---- subscription handle envelope ----------------------------------

    /// The handle a consumer holds is a JSON envelope; `push_unsubscribe`
    /// round-trips it before touching the active-handle set, so a
    /// malformed handle is rejected as a local request error rather than
    /// stopping someone else's watch.
    #[test]
    fn subscription_handle_round_trips_through_json() {
        let handle = GmailSubscriptionHandle {
            topic: "projects/p/topics/t".to_owned(),
            history_id: "12345".to_owned(),
            expiration: Some("1700000000000".to_owned()),
        };
        let encoded = serde_json::to_string(&handle).expect("encode");
        let decoded: GmailSubscriptionHandle = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded.topic, handle.topic);
        assert_eq!(decoded.history_id, handle.history_id);
        assert_eq!(decoded.expiration, handle.expiration);

        assert!(
            serde_json::from_str::<GmailSubscriptionHandle>("not json").is_err(),
            "a malformed handle must not decode"
        );
        assert!(
            serde_json::from_str::<GmailSubscriptionHandle>(r#"{"topic":"t"}"#).is_err(),
            "history_id is required in the envelope"
        );
    }

    #[test]
    fn watch_response_decodes_gmails_string_shaped_fields() {
        let response: GmailWatchResponse =
            serde_json::from_str(r#"{"historyId":"987","expiration":"1700000000000"}"#)
                .expect("watch response decodes");
        assert_eq!(response.history_id, "987");
        assert_eq!(response.expiration.as_deref(), Some("1700000000000"));

        let no_expiry: GmailWatchResponse =
            serde_json::from_str(r#"{"historyId":"987"}"#).expect("expiration is optional");
        assert!(no_expiry.expiration.is_none());
    }

    // ---- config builder -------------------------------------------------

    #[test]
    fn pubsub_config_defaults_to_an_account_wide_watch() {
        let config = PubSubConfig::new("projects/p/topics/t");
        assert_eq!(config.topic, "projects/p/topics/t");
        assert!(
            config.label_ids.is_empty(),
            "an empty label filter means watch the whole account"
        );

        let filtered = PubSubConfig::new("t").with_label_ids(["INBOX", "Label_1"]);
        assert_eq!(filtered.label_ids, vec!["INBOX", "Label_1"]);
    }
}
