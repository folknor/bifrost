use std::time::Duration;

use bifrost_types::{
    AccountOperation, CursorScope, DiagnosticText, HintPayload, InvalidationHint, PushSource,
    WatchEvent,
};
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::{BytesStart, Event};

use crate::ews::{EwsClient, EwsError, EwsHeaders};

use super::GraphAccount;
use super::graph_error::{GraphErrorContext, ews_error_to_account_error};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum EwsStreamingEventType {
    NewMail,
    Created,
    Deleted,
    Modified,
    Moved,
    Copied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsStreamingNotification {
    pub(crate) subscription_id: Option<String>,
    pub(crate) watermark: Option<String>,
    pub(crate) event_type: EwsStreamingEventType,
    pub(crate) item_id: Option<String>,
    pub(crate) item_change_key: Option<String>,
    pub(crate) parent_folder_id: Option<String>,
    pub(crate) parent_folder_change_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsStreamingSubscription {
    pub(crate) subscription_id: String,
    pub(crate) watermark: Option<String>,
}

#[derive(Default)]
struct NotificationBuilder {
    subscription_id: Option<String>,
    watermark: Option<String>,
    event_type: Option<EwsStreamingEventType>,
    item_id: Option<String>,
    item_change_key: Option<String>,
    parent_folder_id: Option<String>,
    parent_folder_change_key: Option<String>,
}

#[derive(Debug)]
enum StreamLoopExit {
    Disconnected,
    Shutdown,
    /// Terminal classification (auth lost, conditional-access, etc.)
    /// surfaced through the EWS worker. The worker exits and emits
    /// `WatchEvent::Terminated(error)` on the push channel.
    Terminated(bifrost_types::AccountError),
}

pub(crate) async fn run_streaming_worker(account: GraphAccount) {
    let Some(account_net) = account.client.account_net() else {
        tracing::warn!("[Graph EWS] Streaming worker started before account attach");
        return;
    };
    let ews = EwsClient::new(account_net, account.client.outlook_base());
    let mut disconnected = false;

    loop {
        if account.shutdown.is_cancelled() {
            return;
        }
        let scopes = active_ews_scopes(&account).await;
        if scopes.is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        match subscribe(&ews, &account, &scopes).await {
            Ok(subscription) => {
                record_subscription_id(&account, &subscription).await;
                if disconnected {
                    let _ = account.push_tx.send(WatchEvent::Reconnected);
                }
                match run_get_events_loop(&ews, &account, subscription).await {
                    StreamLoopExit::Disconnected => disconnected = true,
                    StreamLoopExit::Terminated(error) => {
                        let _ = account.push_tx.send(WatchEvent::Terminated(error));
                        return;
                    }
                    StreamLoopExit::Shutdown => return,
                }
            }
            Err(error) => {
                let account_error = ews_error_to_account_error(
                    error,
                    GraphErrorContext::ews(AccountOperation::PushSubscribe),
                );
                if account_error.recovery().is_terminal() {
                    let _ = account.push_tx.send(WatchEvent::Terminated(account_error));
                    return;
                }
                let telemetry = account_error.telemetry_fields();
                tracing::warn!(
                    target: "bifrost_graph::ews",
                    message_key = telemetry.message_key,
                    recovery = telemetry.recovery_discriminant,
                    "EWS Subscribe failed"
                );
                if !disconnected {
                    let _ = account.push_tx.send(WatchEvent::Disconnected);
                    disconnected = true;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

pub(crate) fn parse_streaming_notifications(
    xml: &str,
) -> Result<Vec<EwsStreamingNotification>, String> {
    let mut reader = Reader::from_str(xml);
    let mut notifications = Vec::new();
    let mut current = NotificationBuilder::default();
    let mut in_event = false;
    let mut notification_subscription_id: Option<String> = None;
    let mut current_tag = String::new();
    let mut buf = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) => {
                let local = local_name(event.name().as_ref()).to_string();
                if local == "Notification" {
                    notification_subscription_id = None;
                }
                if let Some(event_type) = event_type_for(&local) {
                    in_event = true;
                    current = NotificationBuilder {
                        subscription_id: notification_subscription_id.clone(),
                        event_type: Some(event_type),
                        ..NotificationBuilder::default()
                    };
                }
                if in_event && local == "ItemId" {
                    current.item_id = attr(event, "Id");
                    current.item_change_key = attr(event, "ChangeKey");
                }
                if in_event && local == "ParentFolderId" {
                    current.parent_folder_id = attr(event, "Id");
                    current.parent_folder_change_key = attr(event, "ChangeKey");
                }
                current_tag = local;
                buf.clear();
            }
            Ok(Event::Empty(ref event)) => {
                let local = local_name(event.name().as_ref()).to_string();
                if in_event && local == "ItemId" {
                    current.item_id = attr(event, "Id");
                    current.item_change_key = attr(event, "ChangeKey");
                }
                if in_event && local == "ParentFolderId" {
                    current.parent_folder_id = attr(event, "Id");
                    current.parent_folder_change_key = attr(event, "ChangeKey");
                }
            }
            Ok(Event::Text(ref event)) => {
                if let Ok(raw) = std::str::from_utf8(event.as_ref())
                    && let Ok(text) = unescape(raw)
                {
                    buf.push_str(&text);
                }
            }
            Ok(Event::End(ref event)) => {
                let local = local_name(event.name().as_ref()).to_string();
                let trimmed = buf.trim();
                if in_event {
                    match current_tag.as_str() {
                        "SubscriptionId" => current.subscription_id = non_empty(trimmed),
                        "Watermark" => current.watermark = non_empty(trimmed),
                        _ => {}
                    }
                } else if current_tag == "SubscriptionId" {
                    notification_subscription_id = non_empty(trimmed);
                }
                if event_type_for(&local).is_some() && in_event {
                    if let Some(notification) = finish_notification(&current) {
                        notifications.push(notification);
                    }
                    current = NotificationBuilder::default();
                    in_event = false;
                }
                current_tag.clear();
                buf.clear();
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("EWS streaming XML parse failed: {error}")),
            _ => {}
        }
    }

    Ok(notifications)
}

pub(crate) fn parse_subscribe_response(xml: &str) -> Result<Vec<EwsStreamingSubscription>, String> {
    let mut reader = Reader::from_str(xml);
    let mut current_tag = String::new();
    let mut buf = String::new();
    let mut current_subscription_id = None;
    let mut current_watermark = None;
    let mut subscriptions = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) => {
                current_tag = local_name(event.name().as_ref()).to_string();
                buf.clear();
            }
            Ok(Event::Text(ref event)) => {
                if let Ok(raw) = std::str::from_utf8(event.as_ref())
                    && let Ok(text) = unescape(raw)
                {
                    buf.push_str(&text);
                }
            }
            Ok(Event::End(ref event)) => {
                let local = local_name(event.name().as_ref()).to_string();
                let trimmed = buf.trim();
                match current_tag.as_str() {
                    "SubscriptionId" => current_subscription_id = non_empty(trimmed),
                    "Watermark" => current_watermark = non_empty(trimmed),
                    _ => {}
                }
                if (local == "StreamingSubscription" || local == "SubscribeResponseMessage")
                    && let Some(subscription_id) = current_subscription_id.take()
                {
                    subscriptions.push(EwsStreamingSubscription {
                        subscription_id,
                        watermark: current_watermark.take(),
                    });
                }
                current_tag.clear();
                buf.clear();
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("EWS Subscribe XML parse failed: {error}")),
            _ => {}
        }
    }

    Ok(subscriptions)
}

pub(crate) fn build_subscribe_request(scopes: &[CursorScope], watermark: Option<&str>) -> String {
    let mut folder_ids = String::new();
    for scope in scopes {
        if let CursorScope::FolderType { folder, .. } = scope {
            folder_ids.push_str(&format!(r#"<t:FolderId Id="{}"/>"#, xml_escape(&folder.0)));
        }
    }
    let watermark_xml = watermark
        .map(|watermark| format!("<t:Watermark>{}</t:Watermark>", xml_escape(watermark)))
        .unwrap_or_default();

    format!(
        r#"<m:Subscribe>
  <m:StreamingSubscriptionRequest>
    <t:FolderIds>{folder_ids}</t:FolderIds>
    <t:EventTypes>
      <t:EventType>NewMailEvent</t:EventType>
      <t:EventType>CreatedEvent</t:EventType>
      <t:EventType>DeletedEvent</t:EventType>
      <t:EventType>ModifiedEvent</t:EventType>
      <t:EventType>MovedEvent</t:EventType>
      <t:EventType>CopiedEvent</t:EventType>
    </t:EventTypes>
    {watermark_xml}
  </m:StreamingSubscriptionRequest>
</m:Subscribe>"#
    )
}

pub(crate) fn build_get_streaming_events_request(
    subscription_id: &str,
    timeout_minutes: u32,
) -> String {
    format!(
        r#"<m:GetStreamingEvents>
  <m:SubscriptionIds>
    <t:SubscriptionId>{}</t:SubscriptionId>
  </m:SubscriptionIds>
  <m:ConnectionTimeout>{}</m:ConnectionTimeout>
</m:GetStreamingEvents>"#,
        xml_escape(subscription_id),
        timeout_minutes.min(30)
    )
}

async fn subscribe(
    ews: &EwsClient,
    account: &GraphAccount,
    scopes: &[CursorScope],
) -> Result<EwsStreamingSubscription, EwsError> {
    let watermark = current_watermark(account).await;
    let body = build_subscribe_request(scopes, watermark.as_deref());
    let xml = ews.execute(&body, &EwsHeaders::default()).await?;
    let subscriptions = parse_subscribe_response(&xml)
        .map_err(|error| EwsError::MalformedXml(DiagnosticText::support_only(error)))?;
    subscriptions.into_iter().next().ok_or_else(|| {
        EwsError::MalformedXml(DiagnosticText::support_only(
            "EWS Subscribe returned no StreamingSubscription".to_string(),
        ))
    })
}

async fn run_get_events_loop(
    ews: &EwsClient,
    account: &GraphAccount,
    subscription: EwsStreamingSubscription,
) -> StreamLoopExit {
    // The subscription this loop polls is fixed for the lifetime of the
    // loop; the worker re-subscribes (minting a fresh id) on reconnect.
    // Re-reading the id from the shared state map each iteration was
    // fragile - it returned the first state's id, which under the
    // stamp-all behavior is whatever the last resubscribe wrote.
    let subscription_id = subscription.subscription_id;
    loop {
        if account.shutdown.is_cancelled() {
            return StreamLoopExit::Shutdown;
        }
        let body = build_get_streaming_events_request(&subscription_id, 30);
        match ews.execute(&body, &EwsHeaders::default()).await {
            Ok(xml) => match parse_streaming_notifications(&xml) {
                Ok(notifications) => {
                    for notification in notifications {
                        if let Some(watermark) = notification.watermark.as_ref() {
                            record_watermark(
                                account,
                                notification.subscription_id.as_deref(),
                                watermark,
                            )
                            .await;
                        }
                        // A notification with a parent folder id resolves
                        // to a known subscribed scope -> a specific hint.
                        // One without it (a status/keep-alive-style frame,
                        // or an unmapped folder) must NOT fabricate a
                        // `FolderType { folder: FolderId(""), .. }` - that
                        // is a hint for a scope that does not exist. Emit
                        // an account-wide `Unknown` hint so the reconciler
                        // re-checks broadly instead of chasing an empty id.
                        let payload = match notification.parent_folder_id.as_deref() {
                            Some(folder_id) => match scope_for_folder(account, folder_id).await {
                                Some(scope) => HintPayload::SpecificCursorScope(scope),
                                None => HintPayload::Unknown,
                            },
                            None => HintPayload::Unknown,
                        };
                        let _ = account.push_tx.send(WatchEvent::Invalidated {
                            hint: InvalidationHint {
                                source: PushSource::EwsStreaming,
                                payload,
                            },
                        });
                    }
                }
                Err(error) => {
                    // A parse miss mid-long-poll is not necessarily a
                    // permanent contract violation: Microsoft interleaves
                    // keep-alive / status frames into the streaming
                    // response, and a single malformed chunk should
                    // reconnect (re-subscribe), not tear push down for
                    // good. The HTTP-error branch already reconnects;
                    // treat a transient parse failure the same way rather
                    // than terminating.
                    let account_error = ews_error_to_account_error(
                        EwsError::MalformedXml(DiagnosticText::support_only(error)),
                        GraphErrorContext::ews(AccountOperation::PushStream),
                    );
                    let telemetry = account_error.telemetry_fields();
                    tracing::warn!(
                        target: "bifrost_graph::ews",
                        message_key = telemetry.message_key,
                        recovery = telemetry.recovery_discriminant,
                        "EWS GetStreamingEvents parse failed; reconnecting"
                    );
                    let _ = account.push_tx.send(WatchEvent::Disconnected);
                    return StreamLoopExit::Disconnected;
                }
            },
            Err(error) => {
                let account_error = ews_error_to_account_error(
                    error,
                    GraphErrorContext::ews(AccountOperation::PushStream),
                );
                if account_error.recovery().is_terminal() {
                    return StreamLoopExit::Terminated(account_error);
                }
                let telemetry = account_error.telemetry_fields();
                tracing::warn!(
                    target: "bifrost_graph::ews",
                    message_key = telemetry.message_key,
                    recovery = telemetry.recovery_discriminant,
                    "EWS GetStreamingEvents failed"
                );
                let _ = account.push_tx.send(WatchEvent::Disconnected);
                return StreamLoopExit::Disconnected;
            }
        }
    }
}

async fn active_ews_scopes(account: &GraphAccount) -> Vec<CursorScope> {
    let states = account.ews_subscriptions.read().await;
    let mut scopes = Vec::new();
    for state in states.values() {
        scopes.extend(state.scopes.clone());
    }
    scopes
}

async fn record_subscription_id(account: &GraphAccount, subscription: &EwsStreamingSubscription) {
    let mut states = account.ews_subscriptions.write().await;
    for state in states.values_mut() {
        state.ews_subscription_id = Some(subscription.subscription_id.clone());
        // Seed the watermark only for a state that has none yet. The
        // worker subscribes to the union of all handles' folders and
        // resubscribes on every reconnect; blindly overwriting here would
        // discard per-handle watermark progress recorded by
        // `record_watermark` since the last subscribe, replaying already
        // delivered notifications (or, worse, regressing past them).
        if state.watermark.is_none() {
            state.watermark = subscription.watermark.clone();
        }
    }
}

async fn record_watermark(account: &GraphAccount, subscription_id: Option<&str>, watermark: &str) {
    let mut states = account.ews_subscriptions.write().await;
    let mut matched = false;
    for state in states.values_mut() {
        if subscription_id.is_some() && state.ews_subscription_id.as_deref() != subscription_id {
            continue;
        }
        state.watermark = Some(watermark.to_string());
        matched = true;
    }
    if !matched && subscription_id.is_none() {
        for state in states.values_mut() {
            state.watermark = Some(watermark.to_string());
        }
    }
}

async fn current_watermark(account: &GraphAccount) -> Option<String> {
    let states = account.ews_subscriptions.read().await;
    states.values().find_map(|state| state.watermark.clone())
}

async fn scope_for_folder(account: &GraphAccount, folder_id: &str) -> Option<CursorScope> {
    let states = account.ews_subscriptions.read().await;
    states.values().find_map(|state| {
        state.scopes.iter().find_map(|scope| match scope {
            CursorScope::FolderType { folder, .. } if folder.0 == folder_id => Some(scope.clone()),
            _ => None,
        })
    })
}

fn finish_notification(builder: &NotificationBuilder) -> Option<EwsStreamingNotification> {
    Some(EwsStreamingNotification {
        subscription_id: builder.subscription_id.clone(),
        watermark: builder.watermark.clone(),
        event_type: builder.event_type.clone()?,
        item_id: builder.item_id.clone(),
        item_change_key: builder.item_change_key.clone(),
        parent_folder_id: builder.parent_folder_id.clone(),
        parent_folder_change_key: builder.parent_folder_change_key.clone(),
    })
}

fn event_type_for(local: &str) -> Option<EwsStreamingEventType> {
    match local {
        "NewMailEvent" => Some(EwsStreamingEventType::NewMail),
        "CreatedEvent" => Some(EwsStreamingEventType::Created),
        "DeletedEvent" => Some(EwsStreamingEventType::Deleted),
        "ModifiedEvent" => Some(EwsStreamingEventType::Modified),
        "MovedEvent" => Some(EwsStreamingEventType::Moved),
        "CopiedEvent" => Some(EwsStreamingEventType::Copied),
        _ => None,
    }
}

fn attr(event: &BytesStart<'_>, name: &str) -> Option<String> {
    event.attributes().flatten().find_map(|attr| {
        (local_name(attr.key.as_ref()) == name)
            .then(|| String::from_utf8_lossy(&attr.value).to_string())
    })
}

fn local_name(name: &[u8]) -> &str {
    let raw = std::str::from_utf8(name).unwrap_or_default();
    raw.rsplit_once(':').map_or(raw, |(_, local)| local)
}

fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_streaming_notification_xml() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:NewMailEvent>
                <t:Watermark>wm-1</t:Watermark>
                <t:ItemId Id="item-1" ChangeKey="ck-1"/>
                <t:ParentFolderId Id="folder-1" ChangeKey="fck-1"/>
              </t:NewMailEvent>
              <t:ModifiedEvent>
                <t:Watermark>wm-2</t:Watermark>
                <t:ItemId Id="item-2"/>
                <t:ParentFolderId Id="folder-1"/>
              </t:ModifiedEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

        let notifications = parse_streaming_notifications(xml).expect("parse should succeed");
        assert_eq!(notifications.len(), 2);
        assert_eq!(notifications[0].event_type, EwsStreamingEventType::NewMail);
        assert_eq!(notifications[0].item_id.as_deref(), Some("item-1"));
        assert_eq!(
            notifications[0].parent_folder_id.as_deref(),
            Some("folder-1")
        );
        assert_eq!(notifications[1].watermark.as_deref(), Some("wm-2"));
    }

    #[test]
    fn parses_subscribe_response_xml() {
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:SubscribeResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                         xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:SubscribeResponseMessage ResponseClass="Success">
          <m:SubscriptionId>sub-1</m:SubscriptionId>
          <m:Watermark>wm-1</m:Watermark>
        </m:SubscribeResponseMessage>
      </m:ResponseMessages>
    </m:SubscribeResponse>
  </s:Body>
</s:Envelope>"#;

        let subscriptions = parse_subscribe_response(xml).expect("parse should succeed");
        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].subscription_id, "sub-1");
        assert_eq!(subscriptions[0].watermark.as_deref(), Some("wm-1"));
    }

    fn email_scope(folder: &str) -> CursorScope {
        CursorScope::FolderType {
            folder: bifrost_types::FolderId(folder.to_string()),
            ty: bifrost_types::ObjectType::Email,
        }
    }

    #[test]
    fn subscribe_request_lists_every_folder_and_the_six_event_types() {
        let body = build_subscribe_request(&[email_scope("f1"), email_scope("f2")], None);
        assert!(body.contains(r#"<t:FolderId Id="f1"/>"#), "{body}");
        assert!(body.contains(r#"<t:FolderId Id="f2"/>"#), "{body}");
        for event in [
            "NewMailEvent",
            "CreatedEvent",
            "DeletedEvent",
            "ModifiedEvent",
            "MovedEvent",
            "CopiedEvent",
        ] {
            assert!(body.contains(event), "{event} missing from {body}");
        }
        assert!(!body.contains("<t:Watermark>"));
    }

    #[test]
    fn subscribe_request_carries_a_resume_watermark_when_one_is_known() {
        let body = build_subscribe_request(&[email_scope("f1")], Some("wm-1"));
        assert!(body.contains("<t:Watermark>wm-1</t:Watermark>"), "{body}");
    }

    #[test]
    fn subscribe_request_escapes_xml_metacharacters() {
        let body = build_subscribe_request(&[email_scope(r#"a&b<c>"d'"#)], Some("wm&1"));
        assert!(
            body.contains(r#"<t:FolderId Id="a&amp;b&lt;c&gt;&quot;d&apos;"/>"#),
            "{body}"
        );
        assert!(
            body.contains("<t:Watermark>wm&amp;1</t:Watermark>"),
            "{body}"
        );
    }

    /// Documents a defect, NOT the intended contract.
    /// Every other request builder decodes a
    /// foreign `FolderId` before putting it on the wire
    /// (`push::resource_for_scope`, `inventory::initial_delta_url`); the EWS
    /// Subscribe body does not. A shared-mailbox scope therefore emits the
    /// raw `mailbox\u{1f}folder` string inside an XML attribute - U+001F is
    /// not a legal XML 1.0 character and `xml_escape` covers only the five
    /// metacharacters - and even if it parsed, it names a folder id the
    /// primary mailbox does not own (no routing header is sent either).
    #[test]
    fn subscribe_request_leaks_the_foreign_separator_into_the_folder_id() {
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: bifrost_types::ObjectType::Email,
        };
        let body = build_subscribe_request(&[scope], None);
        assert!(body.contains('\u{1f}'), "{body:?}");
        assert!(body.contains("shared@contoso.com"), "{body}");
        // The native-id form the other builders would have produced.
        assert!(!body.contains(r#"<t:FolderId Id="AAMk"/>"#), "{body}");
    }

    /// Documents a defect, NOT the intended contract.
    /// A scope that is not `FolderType` is
    /// silently skipped, so a request built from only such scopes ships an
    /// empty `<t:FolderIds>`, which EWS rejects with
    /// `ErrorInvalidSubscriptionRequest` - and that response code is not in
    /// `SoapFaultCode::parse`, so it lands on the terminal Unknown path and
    /// kills the worker.
    #[test]
    fn subscribe_request_silently_drops_non_folder_scopes() {
        let body = build_subscribe_request(&[CursorScope::Account], None);
        assert!(body.contains("<t:FolderIds></t:FolderIds>"), "{body}");
    }

    #[test]
    fn get_streaming_events_clamps_the_connection_timeout_to_the_ews_maximum() {
        assert!(
            build_get_streaming_events_request("sub-1", 120)
                .contains("<m:ConnectionTimeout>30</m:ConnectionTimeout>")
        );
        assert!(
            build_get_streaming_events_request("sub-1", 5)
                .contains("<m:ConnectionTimeout>5</m:ConnectionTimeout>")
        );
        assert!(
            build_get_streaming_events_request("a&b", 30)
                .contains("<t:SubscriptionId>a&amp;b</t:SubscriptionId>")
        );
    }

    #[test]
    fn the_envelope_subscription_id_is_stamped_on_every_event_in_the_notification() {
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:PreviousWatermark>wm-0</t:PreviousWatermark>
              <t:MoreEvents>false</t:MoreEvents>
              <t:CreatedEvent>
                <t:Watermark>wm-1</t:Watermark>
                <t:ItemId Id="item-1"/>
                <t:ParentFolderId Id="folder-1"/>
              </t:CreatedEvent>
              <t:DeletedEvent>
                <t:Watermark>wm-2</t:Watermark>
                <t:ItemId Id="item-2"/>
                <t:ParentFolderId Id="folder-1"/>
              </t:DeletedEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

        let notifications = parse_streaming_notifications(xml).expect("parse should succeed");
        assert_eq!(notifications.len(), 2);
        for notification in &notifications {
            assert_eq!(notification.subscription_id.as_deref(), Some("sub-1"));
        }
        assert_eq!(notifications[0].event_type, EwsStreamingEventType::Created);
        assert_eq!(notifications[1].event_type, EwsStreamingEventType::Deleted);
        // `PreviousWatermark` / `MoreEvents` must not be mistaken for the
        // per-event watermark.
        assert_eq!(notifications[0].watermark.as_deref(), Some("wm-1"));
        assert_eq!(notifications[1].watermark.as_deref(), Some("wm-2"));
    }

    #[test]
    fn a_status_frame_without_a_parent_folder_parses_without_fabricating_one() {
        // The worker maps a missing parent folder onto an account-wide
        // `Unknown` hint; the parser must report the absence honestly
        // rather than defaulting to an empty FolderId.
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:NewMailEvent>
                <t:Watermark>wm-9</t:Watermark>
              </t:NewMailEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

        let notifications = parse_streaming_notifications(xml).expect("parse should succeed");
        assert_eq!(notifications.len(), 1);
        assert!(notifications[0].parent_folder_id.is_none());
        assert!(notifications[0].item_id.is_none());
        assert_eq!(notifications[0].watermark.as_deref(), Some("wm-9"));
    }

    #[test]
    fn unrecognized_event_elements_produce_no_notification() {
        // Forward compatibility: an EWS event type this build does not
        // know must be ignored, not turned into an untyped invalidation.
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:StatusEvent>
                <t:Watermark>wm-1</t:Watermark>
              </t:StatusEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

        assert!(
            parse_streaming_notifications(xml)
                .expect("parse should succeed")
                .is_empty()
        );
    }

    #[test]
    fn subscribe_response_without_a_subscription_id_yields_no_subscription() {
        // `subscribe` turns the empty vec into `MalformedXml`; pin that the
        // parser does not invent an empty id.
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:SubscribeResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:SubscribeResponseMessage ResponseClass="Success">
          <m:SubscriptionId></m:SubscriptionId>
        </m:SubscribeResponseMessage>
      </m:ResponseMessages>
    </m:SubscribeResponse>
  </s:Body>
</s:Envelope>"#;
        assert!(
            parse_subscribe_response(xml)
                .expect("parse should succeed")
                .is_empty()
        );
    }

    #[test]
    fn local_name_strips_any_namespace_prefix() {
        assert_eq!(local_name(b"t:ItemId"), "ItemId");
        assert_eq!(local_name(b"ItemId"), "ItemId");
        assert_eq!(local_name(b""), "");
    }
}
