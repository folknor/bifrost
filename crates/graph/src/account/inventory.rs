use std::time::Duration;

use bifrost_types::{
    Batch, ChangeCursor, Checkpoint, CursorScope, Fatal, Fingerprint, FolderId, InventoryEntry,
    MembershipScope, ObjectId, ObjectType, PageBoundary, ServerVersion, SyncEvent, ThreadId,
};
use serde_json::Value;

use crate::types::{CONTACT_SELECT, MESSAGE_SELECT, ODataCollection};

use super::GraphAccount;
use super::cursor::{
    GraphCursorPayload, GraphPageMarker, encode_cursor, kind_for_scope, scope_for_kind,
};
use super::error::graph_error_to_fatal;

const EVENT_SELECT: &str = "\
id,subject,bodyPreview,start,end,isAllDay,location,organizer,\
attendees,webLink,iCalUId,categories,recurrence,showAs,\
responseStatus,isCancelled,changeKey";

pub(crate) async fn inventory_events(
    account: GraphAccount,
    scope: CursorScope,
) -> Vec<SyncEvent<InventoryEntry>> {
    match inventory_inner(&account, scope.clone()).await {
        Ok(events) => events,
        Err(fatal) => vec![SyncEvent::Fatal(fatal), SyncEvent::Done(None)],
    }
}

async fn inventory_inner(
    account: &GraphAccount,
    scope: CursorScope,
) -> Result<Vec<SyncEvent<InventoryEntry>>, Fatal> {
    let mut current_url = initial_delta_url(account, &scope)
        .map_err(|error| graph_error_to_fatal(error.to_string(), scope.clone()))?;
    let kind = kind_for_scope(&scope)
        .map_err(|error| graph_error_to_fatal(error.to_string(), scope.clone()))?;
    let mut events = Vec::new();
    let mut checkpoint = None;
    let mut etags = Vec::new();

    loop {
        let page: ODataCollection<Value> = fetch_page(account, &current_url)
            .await
            .map_err(|error| graph_error_to_fatal(error, scope.clone()))?;
        let mut entries = Vec::new();
        for value in page.value {
            if is_removed(&value) {
                continue;
            }
            if let Some(entry) = inventory_entry_from_value(&scope, &value) {
                if let ServerVersion::ETag(etag) = &entry.fingerprint.server_version {
                    etags.push((entry.id.0.clone(), etag.clone()));
                }
                entries.push(entry);
            }
        }

        if let Some(next_link) = page.next_link {
            if !entries.is_empty() {
                events.push(batch(entries, PageBoundary::Page, None));
            }
            current_url = next_link;
        } else if let Some(delta_link) = page.delta_link {
            let cursor = encode_cursor(
                scope.clone(),
                GraphCursorPayload::new(kind, delta_link, None),
            )
            .map_err(|error| graph_error_to_fatal(error.to_string(), scope.clone()))?;
            checkpoint = Some(Checkpoint::Change(cursor.clone()));
            events.push(batch(entries, PageBoundary::Final, Some(cursor)));
            break;
        } else {
            if !entries.is_empty() {
                events.push(batch(entries, PageBoundary::Final, None));
            }
            break;
        }
    }

    if !etags.is_empty() {
        let mut cache = account.etag_index.write().await;
        for (id, etag) in etags {
            cache.insert(id, etag);
        }
    }

    events.push(SyncEvent::Done(checkpoint));
    Ok(events)
}

pub(crate) async fn fetch_delta_page(
    account: &GraphAccount,
    url: &str,
) -> Result<ODataCollection<Value>, String> {
    fetch_page(account, url).await
}

pub(crate) fn inventory_entry_from_value(
    scope: &CursorScope,
    value: &Value,
) -> Option<InventoryEntry> {
    let id = value.get("id")?.as_str()?.to_string();
    let membership = membership_from_value(scope, value);
    let etag = graph_etag(value);
    let fingerprint = Fingerprint {
        server_version: etag
            .map(ServerVersion::ETag)
            .unwrap_or(ServerVersion::Unavailable),
        size: None,
        flags_hash: flags_hash(value),
    };
    let message_id = internet_header(value, "Message-ID").or_else(|| {
        value
            .get("internetMessageId")
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    let references = internet_header(value, "References")
        .map(|header| header.split_whitespace().map(str::to_string).collect())
        .unwrap_or_default();
    let in_reply_to = internet_header(value, "In-Reply-To");
    let thread_id = value
        .get("conversationId")
        .and_then(Value::as_str)
        .map(|thread| ThreadId(thread.to_string()));

    Some(InventoryEntry {
        id: ObjectId(id),
        memberships: vec![membership],
        size: None,
        blob_id: None,
        fingerprint,
        thread_id,
        message_id,
        references,
        in_reply_to,
    })
}

pub(crate) fn membership_from_value(scope: &CursorScope, value: &Value) -> MembershipScope {
    let fallback = match scope {
        CursorScope::FolderType { folder, .. } | CursorScope::Folder(folder) => folder.0.clone(),
        _ => String::new(),
    };
    let folder = value
        .get("parentFolderId")
        .and_then(Value::as_str)
        .filter(|folder| !folder.is_empty())
        .unwrap_or(&fallback);
    MembershipScope::Folder(FolderId(folder.to_string()))
}

pub(crate) fn graph_etag(value: &Value) -> Option<String> {
    value
        .get("changeKey")
        .and_then(Value::as_str)
        .or_else(|| value.get("@odata.etag").and_then(Value::as_str))
        .map(str::to_string)
}

pub(crate) fn is_removed(value: &Value) -> bool {
    value.get("@removed").is_some()
}

pub(crate) fn removed_id(value: &Value) -> Option<ObjectId> {
    value
        .get("id")
        .and_then(Value::as_str)
        .map(|id| ObjectId(id.to_string()))
}

pub(crate) fn page_marker(next_link: String, last_seen_id: Option<String>) -> GraphPageMarker {
    GraphPageMarker {
        next_link,
        last_seen_id,
    }
}

pub(crate) fn initial_delta_url(
    account: &GraphAccount,
    scope: &CursorScope,
) -> Result<String, &'static str> {
    let prefix = account.client.api_path_prefix();
    match scope {
        CursorScope::FolderType { folder, ty } => {
            let encoded = urlencoding::encode(&folder.0);
            match ty {
                ObjectType::Email => Ok(format!(
                    "{prefix}/mailFolders/{encoded}/messages/delta?$select={MESSAGE_SELECT}&$top=50"
                )),
                ObjectType::Event | ObjectType::CalendarEvent => {
                    let start = (chrono::Utc::now() - chrono::Duration::days(90))
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                    let end = (chrono::Utc::now() + chrono::Duration::days(365))
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                    Ok(format!(
                        "{prefix}/calendars/{encoded}/calendarView/delta?startDateTime={start}&endDateTime={end}&$select={EVENT_SELECT}"
                    ))
                }
                ObjectType::Contact => Ok(format!(
                    "{prefix}/contactFolders/{encoded}/contacts/delta?$select={CONTACT_SELECT}&$top=250"
                )),
                _ => Err("unsupported Graph cursor scope type"),
            }
        }
        _ => Err("unsupported Graph cursor scope"),
    }
}

pub(crate) fn batch<T>(
    items: Vec<T>,
    page_boundary: PageBoundary,
    cursor: Option<ChangeCursor>,
) -> SyncEvent<T> {
    SyncEvent::Batch(Batch {
        items,
        page_boundary,
        server_latency: Duration::default(),
        bytes_in: 0,
        checkpoint: cursor.map(Checkpoint::Change),
    })
}

pub(crate) fn scope_matches_payload(scope: &CursorScope, payload: &GraphCursorPayload) -> bool {
    *scope == scope_for_kind(&payload.kind)
}

async fn fetch_page(account: &GraphAccount, url: &str) -> Result<ODataCollection<Value>, String> {
    if url.starts_with("http") {
        account.client.get_absolute(url).await
    } else {
        account.client.get_json(url).await
    }
}

fn internet_header(value: &Value, name: &str) -> Option<String> {
    let headers = value.get("internetMessageHeaders")?.as_array()?;
    headers.iter().find_map(|header| {
        let header_name = header.get("name")?.as_str()?;
        header_name.eq_ignore_ascii_case(name).then(|| {
            header
                .get("value")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        })
    })
}

fn flags_hash(value: &Value) -> u64 {
    let mut flags = Vec::new();
    if let Some(is_read) = value.get("isRead").and_then(Value::as_bool) {
        flags.push(format!("isread={is_read}"));
    }
    if let Some(status) = value
        .get("flag")
        .and_then(|flag| flag.get("flagStatus"))
        .and_then(Value::as_str)
    {
        flags.push(format!("flag={}", status.to_ascii_lowercase()));
    }
    if let Some(categories) = value.get("categories").and_then(Value::as_array) {
        for category in categories {
            if let Some(category) = category.as_str() {
                flags.push(format!("category={}", category.to_ascii_lowercase()));
            }
        }
    }
    flags.sort();

    let mut hash = 0xcbf29ce484222325_u64;
    for flag in flags {
        for byte in flag.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= u64::from(0xff_u8);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn membership_scope_uses_parent_folder_id() {
        let scope = CursorScope::FolderType {
            folder: FolderId("fallback".to_string()),
            ty: ObjectType::Email,
        };
        let value = json!({
            "id": "m1",
            "parentFolderId": "actual-parent"
        });
        assert_eq!(
            membership_from_value(&scope, &value),
            MembershipScope::Folder(FolderId("actual-parent".to_string()))
        );
    }

    #[test]
    fn inventory_entry_omits_graph_message_size() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let value = json!({
            "id": "m1",
            "changeKey": "ck1",
            "conversationId": "thread1",
            "internetMessageHeaders": [
                { "name": "Message-ID", "value": "<m1@example.test>" },
                { "name": "References", "value": "<a> <b>" },
                { "name": "In-Reply-To", "value": "<b>" }
            ],
            "isRead": true,
            "categories": ["Blue"]
        });

        let entry = inventory_entry_from_value(&scope, &value).expect("entry expected");
        assert!(entry.size.is_none());
        assert_eq!(entry.message_id.as_deref(), Some("<m1@example.test>"));
        assert_eq!(entry.references.len(), 2);
        assert!(matches!(
            entry.fingerprint.server_version,
            ServerVersion::ETag(_)
        ));
    }
}
