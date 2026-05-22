use std::time::Duration;

use bifrost_types::{
    AccountStream, Batch, ChangeCursor, Checkpoint, CursorScope, Fingerprint, FolderId,
    InventoryEntry, MembershipScope, ObjectId, ObjectType, PageBoundary, ServerVersion, SyncEvent,
    ThreadId,
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

pub(crate) fn inventory_stream(
    account: GraphAccount,
    scope: CursorScope,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    Box::pin(async_stream::stream! {
        let mut current_url = match initial_delta_url(&account, &scope) {
            Ok(url) => url,
            Err(error) => {
                yield SyncEvent::Fatal(graph_error_to_fatal(error.to_string(), scope.clone()));
                yield SyncEvent::Done(None);
                return;
            }
        };
        let kind = match kind_for_scope(&scope) {
            Ok(kind) => kind,
            Err(error) => {
                yield SyncEvent::Fatal(graph_error_to_fatal(error.to_string(), scope.clone()));
                yield SyncEvent::Done(None);
                return;
            }
        };

        loop {
            let page: ODataCollection<Value> = match fetch_page(&account, &current_url).await {
                Ok(page) => page,
                Err(error) => {
                    yield SyncEvent::Fatal(graph_error_to_fatal(error, scope.clone()));
                    yield SyncEvent::Done(None);
                    return;
                }
            };
            let mut entries = Vec::new();
            let mut etags = Vec::new();
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

            if !etags.is_empty() {
                let mut cache = account.etag_index.write().await;
                for (id, etag) in etags {
                    cache.insert(id, etag);
                }
            }

            if let Some(next_link) = page.next_link {
                if !entries.is_empty() {
                    yield batch(entries, PageBoundary::Page, None);
                }
                current_url = next_link;
            } else if let Some(delta_link) = page.delta_link {
                let cursor = match encode_cursor(
                    scope.clone(),
                    GraphCursorPayload::new(kind.clone(), delta_link, None),
                ) {
                    Ok(cursor) => cursor,
                    Err(error) => {
                        yield SyncEvent::Fatal(graph_error_to_fatal(
                            error.to_string(),
                            scope.clone(),
                        ));
                        yield SyncEvent::Done(None);
                        return;
                    }
                };
                let checkpoint = Checkpoint::Change(cursor.clone());
                yield batch(entries, PageBoundary::Final, Some(cursor));
                yield SyncEvent::Done(Some(checkpoint));
                return;
            } else {
                if !entries.is_empty() {
                    yield batch(entries, PageBoundary::Final, None);
                }
                yield SyncEvent::Done(None);
                return;
            }
        }
    })
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
            let encoded = bifrost_net::url::encode_component(&folder.0);
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
    let canonical = scope_for_kind(&payload.kind);
    *scope == canonical || event_aliases(scope, &canonical)
}

fn event_aliases(a: &CursorScope, b: &CursorScope) -> bool {
    let (
        CursorScope::FolderType { folder: fa, ty: ta },
        CursorScope::FolderType { folder: fb, ty: tb },
    ) = (a, b)
    else {
        return false;
    };
    let aliased = matches!(
        (ta, tb),
        (ObjectType::Event, ObjectType::CalendarEvent)
            | (ObjectType::CalendarEvent, ObjectType::Event)
    );
    aliased && fa == fb
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
    use bifrost_types::QueryId;
    use futures::StreamExt;
    use serde_json::json;

    use crate::client::GraphClient;

    use super::super::PushMode;
    use super::*;

    fn test_account() -> GraphAccount {
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions)
    }

    #[test]
    fn initial_delta_url_supports_email_scope() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let url = initial_delta_url(&account, &scope).expect("email scope is supported");
        assert!(url.starts_with("/me/mailFolders/inbox/messages/delta?"));
    }

    #[test]
    fn initial_delta_url_supports_event_scope() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::Event,
        };
        let url = initial_delta_url(&account, &scope).expect("event scope is supported");
        assert!(url.starts_with("/me/calendars/calendar/calendarView/delta?"));
    }

    #[test]
    fn initial_delta_url_supports_contact_scope() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("contacts".to_string()),
            ty: ObjectType::Contact,
        };
        let url = initial_delta_url(&account, &scope).expect("contact scope is supported");
        assert!(url.starts_with("/me/contactFolders/contacts/contacts/delta?"));
    }

    #[test]
    fn initial_delta_url_rejects_account_scope() {
        let account = test_account();
        assert!(initial_delta_url(&account, &CursorScope::Account).is_err());
    }

    #[test]
    fn initial_delta_url_rejects_query_scope() {
        let account = test_account();
        let scope = CursorScope::Query(QueryId("q1".to_string()));
        assert!(initial_delta_url(&account, &scope).is_err());
    }

    #[test]
    fn initial_delta_url_rejects_bare_folder_scope() {
        let account = test_account();
        let scope = CursorScope::Folder(FolderId("inbox".to_string()));
        assert!(initial_delta_url(&account, &scope).is_err());
    }

    #[test]
    fn initial_delta_url_rejects_unsupported_object_type() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Mailbox,
        };
        assert!(initial_delta_url(&account, &scope).is_err());
    }

    #[test]
    fn scope_matches_payload_treats_event_and_calendar_event_as_aliases() {
        let event_scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::Event,
        };
        let calendar_event_scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::CalendarEvent,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&calendar_event_scope).expect("calendar event scope maps"),
            "https://graph.example/delta".to_string(),
            None,
        );

        assert!(scope_matches_payload(&calendar_event_scope, &payload));
        assert!(scope_matches_payload(&event_scope, &payload));
    }

    #[test]
    fn scope_matches_payload_rejects_unrelated_object_types() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let other_scope = CursorScope::FolderType {
            folder: FolderId("calendar".to_string()),
            ty: ObjectType::Event,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&other_scope).expect("event scope maps"),
            "https://graph.example/delta".to_string(),
            None,
        );

        assert!(!scope_matches_payload(&scope, &payload));
    }

    #[test]
    fn scope_matches_payload_rejects_aliased_object_types_for_different_folders() {
        use super::super::cursor::GraphCursorKind;

        let payload = GraphCursorPayload::new(
            GraphCursorKind::Events {
                calendar_id: "calendar-a".to_string(),
            },
            "https://graph.example/delta".to_string(),
            None,
        );
        let scope = CursorScope::FolderType {
            folder: FolderId("calendar-b".to_string()),
            ty: ObjectType::CalendarEvent,
        };
        assert!(!scope_matches_payload(&scope, &payload));
    }

    #[tokio::test]
    async fn inventory_rejects_account_scope_with_fatal() {
        let account = test_account();
        let mut stream = inventory_stream(account, CursorScope::Account);
        let first = stream.next().await.expect("fatal event");
        assert!(matches!(first, SyncEvent::Fatal(_)));
        let second = stream.next().await.expect("done event");
        assert!(matches!(second, SyncEvent::Done(None)));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn inventory_rejects_query_scope_with_fatal() {
        let account = test_account();
        let scope = CursorScope::Query(QueryId("q1".to_string()));
        let mut stream = inventory_stream(account, scope);
        let first = stream.next().await.expect("fatal event");
        assert!(matches!(first, SyncEvent::Fatal(_)));
        let second = stream.next().await.expect("done event");
        assert!(matches!(second, SyncEvent::Done(None)));
    }

    #[tokio::test]
    async fn inventory_rejects_unsupported_folder_type_with_fatal() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Mailbox,
        };
        let mut stream = inventory_stream(account, scope);
        let first = stream.next().await.expect("fatal event");
        assert!(matches!(first, SyncEvent::Fatal(_)));
    }

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
