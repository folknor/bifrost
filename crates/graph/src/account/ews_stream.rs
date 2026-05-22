use std::time::Duration;

use bifrost_types::{
    CursorScope, FolderId, HintPayload, InvalidationHint, ObjectType, PushSource, WatchEvent,
};
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::{BytesStart, Event};

use crate::ews::EwsClient;

use super::GraphAccount;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EwsStreamingEventType {
    NewMail,
    Created,
    Deleted,
    Modified,
    Moved,
    Copied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EwsStreamingNotification {
    pub subscription_id: Option<String>,
    pub watermark: Option<String>,
    pub event_type: EwsStreamingEventType,
    pub item_id: Option<String>,
    pub item_change_key: Option<String>,
    pub parent_folder_id: Option<String>,
    pub parent_folder_change_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EwsStreamingSubscription {
    pub subscription_id: String,
    pub watermark: Option<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamLoopExit {
    Disconnected,
    Shutdown,
}

pub(crate) async fn run_streaming_worker(account: GraphAccount) {
    let ews = EwsClient::new(account.client.account_net().clone());
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
                    StreamLoopExit::Shutdown => return,
                }
            }
            Err(error) => {
                if !disconnected {
                    let _ = account.push_tx.send(WatchEvent::Disconnected);
                    disconnected = true;
                }
                tracing::warn!("[Graph EWS] Subscribe failed: {error}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

pub fn parse_streaming_notifications(xml: &str) -> Result<Vec<EwsStreamingNotification>, String> {
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

pub fn parse_subscribe_response(xml: &str) -> Result<Vec<EwsStreamingSubscription>, String> {
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

pub fn build_subscribe_request(scopes: &[CursorScope], watermark: Option<&str>) -> String {
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

pub fn build_get_streaming_events_request(subscription_id: &str, timeout_minutes: u32) -> String {
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
) -> Result<EwsStreamingSubscription, String> {
    let watermark = current_watermark(account).await;
    let body = build_subscribe_request(scopes, watermark.as_deref());
    let xml = ews.execute(&body, None).await?;
    parse_subscribe_response(&xml)?
        .into_iter()
        .next()
        .ok_or_else(|| "EWS Subscribe returned no StreamingSubscription".to_string())
}

async fn run_get_events_loop(
    ews: &EwsClient,
    account: &GraphAccount,
    subscription: EwsStreamingSubscription,
) -> StreamLoopExit {
    let mut subscription_id = subscription.subscription_id;
    loop {
        if account.shutdown.is_cancelled() {
            return StreamLoopExit::Shutdown;
        }
        let body = build_get_streaming_events_request(&subscription_id, 30);
        match ews.execute(&body, None).await {
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
                        let scope = notification
                            .parent_folder_id
                            .as_deref()
                            .and_then(|folder_id| scope_for_folder(account, folder_id))
                            .unwrap_or_else(|| CursorScope::FolderType {
                                folder: FolderId(
                                    notification.parent_folder_id.clone().unwrap_or_default(),
                                ),
                                ty: ObjectType::Email,
                            });
                        let _ = account.push_tx.send(WatchEvent::Invalidated {
                            hint: InvalidationHint {
                                source: PushSource::EwsStreaming,
                                payload: HintPayload::SpecificCursorScope(scope),
                            },
                        });
                    }
                }
                Err(error) => {
                    tracing::warn!("[Graph EWS] GetStreamingEvents parse failed: {error}");
                    let _ = account.push_tx.send(WatchEvent::Disconnected);
                    return StreamLoopExit::Disconnected;
                }
            },
            Err(error) => {
                tracing::warn!("[Graph EWS] GetStreamingEvents failed: {error}");
                let _ = account.push_tx.send(WatchEvent::Disconnected);
                return StreamLoopExit::Disconnected;
            }
        }
        subscription_id = current_subscription_id(account)
            .await
            .unwrap_or(subscription_id);
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
        state.watermark = subscription.watermark.clone();
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

async fn current_subscription_id(account: &GraphAccount) -> Option<String> {
    let states = account.ews_subscriptions.read().await;
    states
        .values()
        .find_map(|state| state.ews_subscription_id.clone())
}

fn scope_for_folder(account: &GraphAccount, folder_id: &str) -> Option<CursorScope> {
    account
        .ews_subscriptions
        .try_read()
        .ok()
        .and_then(|states| {
            states.values().find_map(|state| {
                state.scopes.iter().find_map(|scope| match scope {
                    CursorScope::FolderType { folder, .. } if folder.0 == folder_id => {
                        Some(scope.clone())
                    }
                    _ => None,
                })
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
}
