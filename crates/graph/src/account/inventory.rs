use std::time::Duration;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, ChangeCursor, Checkpoint, CursorScope, ErrorScope,
    Fingerprint, FolderId, InventoryEntry, MembershipScope, ObjectId, ObjectType, PageBoundary,
    ServerVersion, SyncEvent,
};
use serde_json::Value;

use crate::types::{CONTACT_SELECT, MESSAGE_SELECT, ODataCollection};

use super::GraphAccount;
use super::cursor::{
    GraphCursorPayload, GraphPageMarker, encode_cursor, kind_for_scope, scope_for_kind,
};
use super::graph_error::{GraphErrorContext, cursor_error_to_account_error};

const EVENT_SELECT: &str = "\
id,subject,bodyPreview,start,end,isAllDay,location,organizer,\
attendees,webLink,iCalUId,categories,recurrence,showAs,\
responseStatus,isCancelled,changeKey";

pub(crate) fn inventory_stream(
    account: GraphAccount,
    scope: CursorScope,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    Box::pin(async_stream::stream! {
        let sync_ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
            .with_scope(ErrorScope::Cursor(scope.clone()));

        let client = match account.client_for_scope(&scope) {
            Ok(client) => client.clone(),
            Err(error) => {
                yield SyncEvent::Terminated(cursor_error_to_account_error(
                    super::cursor::routing_error(error),
                    sync_ctx,
                ));
                yield SyncEvent::Done(None);
                return;
            }
        };
        let mut current_url = match initial_delta_url(&account, &scope) {
            Ok(url) => url,
            Err(error) => {
                yield SyncEvent::Terminated(cursor_error_to_account_error(error, sync_ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };
        // A foreign (shared) scope tags every inventory item with its
        // owning mailbox so the consumer maps the item to its shared
        // owner and the foreign mailbox's native folder ids cannot be
        // conflated with the primary's in the membership index (the
        // A5c-established owner-tag pattern). `None` for a primary scope.
        let owner = account.owner_of_scope(&scope);
        let kind = match kind_for_scope(&scope) {
            Ok(kind) => kind,
            Err(error) => {
                let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };

        loop {
            let page: ODataCollection<Value> = match fetch_page(&client, &current_url).await {
                Ok(page) => page,
                Err(error) => {
                    let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                        .with_scope(ErrorScope::Cursor(scope.clone()));
                    // A permission denial on a foreign (shared) scope
                    // quarantines just this scope; a primary-scope
                    // denial stays terminal account-wide. `owner` is
                    // hoisted above the loop (it is scope-invariant).
                    yield SyncEvent::Terminated(super::graph_error::graph_shared_scope_error(
                        error,
                        &scope,
                        owner.as_ref(),
                        ctx,
                    ));
                    yield SyncEvent::Done(None);
                    return;
                }
            };
            let mut entries = Vec::new();
            let mut etags = Vec::new();
            let mut removed_etag_ids = Vec::new();
            for value in page.value {
                if is_removed(&value) {
                    if let Some(id) = value.get("id").and_then(Value::as_str) {
                        removed_etag_ids.push(super::foreign::encode_message_id(&scope, id).0);
                    }
                    continue;
                }
                if let Some(mut entry) = inventory_entry_from_value(&scope, &value) {
                    if let ServerVersion::ETag(etag) = &entry.fingerprint.server_version {
                        etags.push((entry.id.0.clone(), etag.clone()));
                    }
                    if let Some(owner) = &owner {
                        entry
                            .memberships
                            .push(bifrost_types::MembershipScope::Mailbox(owner.clone()));
                    }
                    entries.push(entry);
                }
            }

            if !etags.is_empty() || !removed_etag_ids.is_empty() {
                let mut cache = account.etag_index.write().await;
                for (id, etag) in etags {
                    cache.insert(id, etag);
                }
                for id in removed_etag_ids {
                    cache.remove(&id);
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
                        let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                            .with_scope(ErrorScope::Cursor(scope.clone()));
                        yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                        yield SyncEvent::Done(None);
                        return;
                    }
                };
                let checkpoint = Checkpoint::Change(cursor.clone());
                yield batch(entries, PageBoundary::Final, Some(cursor));
                yield SyncEvent::Done(Some(checkpoint));
                return;
            } else {
                // A delta page must advance with either a next link or a
                // delta link. Completing without a checkpoint would make
                // the engine restart inventory from page one forever.
                yield SyncEvent::Terminated(super::graph_error::protocol_violation(
                    bifrost_types::ProtocolErrorKind::ContractViolation,
                    AccountOperation::SyncInventory,
                    Some(ErrorScope::Cursor(scope.clone())),
                    "Graph delta page had neither @odata.nextLink nor @odata.deltaLink",
                ));
                yield SyncEvent::Done(None);
                return;
            }
        }
    })
}

pub(crate) async fn fetch_delta_page(
    client: &crate::client::GraphClient,
    url: &str,
) -> Result<ODataCollection<Value>, crate::error::GraphError> {
    fetch_page(client, url).await
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
        .map(|thread| super::foreign::encode_thread_id(scope, thread));

    Some(InventoryEntry {
        // Foreign-encode the id at mint so later hydration / blob /
        // raw-RFC822 reads route to `/users/{owner}`; a primary-scope
        // item stays bare. Exactly one wire form per logical item.
        id: super::foreign::encode_message_id(scope, &id),
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
        CursorScope::FolderType { folder, .. } | CursorScope::Folder(folder) => {
            super::foreign::parse_folder(folder).native_id().to_string()
        }
        _ => String::new(),
    };
    let native = value
        .get("parentFolderId")
        .and_then(Value::as_str)
        .filter(|folder| !folder.is_empty())
        .unwrap_or(&fallback);
    // For a foreign (shared/delegate) scope the item's `parentFolderId`
    // is a *native* id within the owning mailbox. Discovery emits the
    // folder membership foreign-encoded (`encode_foreign(mailbox,
    // native)`), so emit the same encoding here - otherwise the engine's
    // covering rule (folder-typed cursor covers the matching folder)
    // cannot reconcile inventory/changes against discovery, and a foreign
    // native id colliding with a primary id (e.g. `inbox`) would conflate
    // into the primary mailbox's membership set.
    let folder = match scope {
        CursorScope::FolderType { folder, .. } | CursorScope::Folder(folder) => {
            match super::foreign::parse_folder(folder).foreign() {
                Some(foreign) => super::foreign::encode_foreign(&foreign.mailbox, native),
                None => FolderId(native.to_string()),
            }
        }
        _ => FolderId(native.to_string()),
    };
    MembershipScope::Folder(folder)
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

/// A `calendarView` window bound, `days` from `now`, in the second-precision
/// UTC form Graph expects. A bound that runs off the representable range
/// falls back to `now` rather than widening the window unpredictably.
fn calendar_view_bound(now: jiff::Timestamp, days: i64) -> String {
    let at = jiff::Span::new()
        .try_days(days)
        .and_then(|span| now.checked_add(span))
        .unwrap_or(now);
    jiff::tz::Offset::UTC
        .to_datetime(at)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
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
) -> Result<String, super::cursor::CursorError> {
    // Route the prefix through the scope's owning client (primary or a
    // shared mailbox) and use the *native* folder id in the path - the
    // foreign-mailbox prefix lives in the URL's `/users/{id}` segment,
    // not in the `/mailFolders/{id}` segment.
    let prefix = account
        .client_for_scope(scope)
        .map_err(super::cursor::routing_error)?
        .api_path_prefix();
    match scope {
        CursorScope::FolderType { folder, ty } => {
            let native = super::foreign::parse_folder(folder).native_id().to_string();
            let encoded = bifrost_net::url::encode_path_component(&native);
            match ty {
                ObjectType::Email => Ok(format!(
                    "{prefix}/mailFolders/{encoded}/messages/delta?$select={MESSAGE_SELECT}&$top=50"
                )),
                ObjectType::Event | ObjectType::CalendarEvent => {
                    let now = jiff::Timestamp::now();
                    let start = calendar_view_bound(now, -90);
                    let end = calendar_view_bound(now, 365);
                    Ok(format!(
                        "{prefix}/calendars/{encoded}/calendarView/delta?startDateTime={start}&endDateTime={end}&$select={EVENT_SELECT}"
                    ))
                }
                ObjectType::Contact => Ok(format!(
                    "{prefix}/contactFolders/{encoded}/contacts/delta?$select={CONTACT_SELECT}&$top=250"
                )),
                _ => Err(super::cursor::CursorError::Unsupported),
            }
        }
        _ => Err(super::cursor::CursorError::Unsupported),
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

async fn fetch_page(
    client: &crate::client::GraphClient,
    url: &str,
) -> Result<ODataCollection<Value>, crate::error::GraphError> {
    if url.starts_with("http") {
        client.get_absolute(url).await
    } else {
        client.get_json(url).await
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
    use std::collections::HashMap;

    use bifrost_types::QueryId;
    use futures::StreamExt;
    use serde_json::json;

    use crate::client::GraphClient;

    use super::super::PushMode;
    use super::*;
    use crate::client::ScriptedRestResponse;

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
    fn client_for_scope_routes_foreign_prefix() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let foreign_scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        let primary_scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        // The foreign scope routes through the shared client, whose
        // prefix targets `/users/{mailbox}`.
        assert_eq!(
            account
                .client_for_scope(&foreign_scope)
                .expect("configured")
                .api_path_prefix(),
            "/users/shared%40contoso.com"
        );
        // A primary scope stays on `/me`.
        assert_eq!(
            account
                .client_for_scope(&primary_scope)
                .expect("primary")
                .api_path_prefix(),
            "/me"
        );
        let unconfigured = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("other@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        assert!(matches!(
            account.client_for_scope(&unconfigured),
            Err(crate::error::GraphError::Configuration { .. })
        ));
    }

    /// The engine-facing half of that rejection: a persisted scope whose
    /// shared mailbox left the configuration must quarantine ONE scope, not
    /// escalate. `ScopeRevoked` carrying `ErrorScope::Cursor` derives
    /// `DisableScope`; the same kind without a scope would derive
    /// `RestartAccount` and re-run discovery for every other mailbox over a
    /// condition discovery cannot fix.
    ///
    /// Hermetic: the scope is refused before the first delta request.
    #[tokio::test]
    async fn a_stale_foreign_scope_disables_only_itself() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let stale = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("gone@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        let mut stream = inventory_stream(account, stale.clone());
        let SyncEvent::Terminated(error) = stream.next().await.expect("an event") else {
            panic!("expected SyncEvent::Terminated");
        };
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::ScopeRevoked
            )
        ));
        assert_eq!(error.scope(), Some(&ErrorScope::Cursor(stale.clone())));
        match error.recovery() {
            bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::DisableScope(
                scope,
            )) => assert_eq!(scope, &stale),
            other => panic!("expected DisableScope, got {other:?}"),
        }
        assert!(matches!(stream.next().await, Some(SyncEvent::Done(None))));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn inventory_walks_the_absolute_next_link_and_rejects_a_page_without_either_link() {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{"id": "one", "changeKey": "ck-one"}],
                    "@odata.nextLink": "https://graph.example/next?page=2"
                }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{"id": "two", "changeKey": "ck-two"}]
                }),
            ),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let mut stream = inventory_stream(account, scope);
        assert!(matches!(stream.next().await, Some(SyncEvent::Batch(_))));
        let SyncEvent::Terminated(error) = stream.next().await.expect("neither-link failure")
        else {
            panic!("expected termination")
        };
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        ));
        assert!(matches!(stream.next().await, Some(SyncEvent::Done(None))));
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .url
                .contains("/me/mailFolders/inbox/messages/delta?")
        );
        assert_eq!(requests[1].url, "https://graph.example/next?page=2");
    }

    #[tokio::test]
    async fn inventory_tombstone_evicts_its_cached_etag() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{"id":"gone", "@removed":{"reason":"deleted"}}],
                "@odata.deltaLink":"https://graph.example/delta"
            }),
        )]);
        let account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
        account
            .etag_index
            .write()
            .await
            .insert("gone".to_string(), "old".to_string());
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let mut stream = inventory_stream(account.clone(), scope);
        let _ = stream.next().await;
        let _ = stream.next().await;
        assert_eq!(account.etag_index.write().await.get("gone"), None);
    }

    #[test]
    fn initial_delta_url_uses_shared_mailbox_prefix() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        let url = initial_delta_url(&account, &scope).expect("foreign email scope is supported");
        // The mailbox rides in the `/users/{id}` segment; the native
        // folder id (the foreign prefix stripped) rides in
        // `/mailFolders/{id}`.
        assert!(
            url.starts_with("/users/shared%40contoso.com/mailFolders/AAMk/messages/delta?"),
            "unexpected url: {url}"
        );
    }

    /// Every page of a foreign inventory walk rides the OWNING mailbox's
    /// client, including the absolute `@odata.nextLink` continuation.
    ///
    /// The primary client is armed with an empty script, so a walk that
    /// falls back to it panics at the offending request rather than
    /// answering from a queue it shares with the shared client. Asserting
    /// the URL alone would prove nothing here: `initial_delta_url` has
    /// always built the `/users/{mailbox}` prefix off `client_for_scope`,
    /// and the continuation URL is whatever Graph minted, so both are
    /// identical whichever client carries them.
    #[tokio::test]
    async fn every_page_of_a_foreign_inventory_walk_rides_the_owner_client() {
        let primary = GraphClient::new("primary-token");
        // Armed, empty: the primary must not be asked for anything.
        primary.script_rest([]);
        let shared = GraphClient::new("shared-token").for_shared_mailbox("shared@contoso.com");
        shared.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{ "id": "m1", "changeKey": "ck1" }],
                    "@odata.nextLink": "https://graph.example/users/shared%40contoso.com/delta?$skiptoken=p2"
                }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [],
                    "@odata.deltaLink": "https://graph.example/users/shared%40contoso.com/delta?$deltatoken=d1"
                }),
            ),
        ]);
        let account = GraphAccount::new_for_tests_with_shared_clients(
            primary.clone(),
            PushMode::GraphSubscriptions,
            HashMap::from([("shared@contoso.com".to_string(), shared.clone())]),
        );
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };

        let mut stream = inventory_stream(account, scope);
        assert!(matches!(stream.next().await, Some(SyncEvent::Batch(_))));
        assert!(matches!(stream.next().await, Some(SyncEvent::Batch(_))));
        assert!(matches!(
            stream.next().await,
            Some(SyncEvent::Done(Some(_)))
        ));

        let requests = shared.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .url
                .contains("/users/shared%40contoso.com/mailFolders/AAMk/messages/delta?"),
            "unexpected first URL: {}",
            requests[0].url,
        );
        assert_eq!(
            requests[1].url,
            "https://graph.example/users/shared%40contoso.com/delta?$skiptoken=p2"
        );
        assert!(
            primary.take_rest_requests().is_empty(),
            "the primary client must not see a foreign-scope request"
        );
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
        assert!(matches!(first, SyncEvent::Terminated(_)));
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
        assert!(matches!(first, SyncEvent::Terminated(_)));
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
        assert!(matches!(first, SyncEvent::Terminated(_)));
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
    fn membership_scope_foreign_encodes_for_foreign_scope() {
        // A foreign (shared) scope must emit the parentFolderId
        // foreign-encoded so it reconciles with discovery
        // (`Folder(encode_foreign(mailbox, native))`) and a foreign
        // native id colliding with a primary id cannot conflate into the
        // primary mailbox's membership set.
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkRoot"),
            ty: ObjectType::Email,
        };
        let value = json!({
            "id": "m1",
            "parentFolderId": "inbox"
        });
        assert_eq!(
            membership_from_value(&scope, &value),
            MembershipScope::Folder(super::super::foreign::encode_foreign(
                "shared@contoso.com",
                "inbox"
            ))
        );
    }

    #[test]
    fn membership_scope_primary_stays_native() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let value = json!({ "id": "m1", "parentFolderId": "inbox" });
        assert_eq!(
            membership_from_value(&scope, &value),
            MembershipScope::Folder(FolderId("inbox".to_string()))
        );
    }

    #[test]
    fn inventory_entry_id_encodes_foreign_scope_and_leaves_primary_bare() {
        // Foreign scope: the entry id carries the owning mailbox so later
        // hydration / blob reads route to `/users/{owner}`.
        let foreign_scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        let value = json!({ "id": "AAMkmsg", "changeKey": "ck1" });
        let entry = inventory_entry_from_value(&foreign_scope, &value).expect("entry");
        assert_eq!(entry.id.0, "shared@contoso.com\u{1f}AAMkmsg");

        // Primary scope: the id stays bare. One encoding per logical item.
        let primary_scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let entry = inventory_entry_from_value(&primary_scope, &value).expect("entry");
        assert_eq!(entry.id.0, "AAMkmsg");
    }

    #[test]
    fn inventory_entry_thread_id_encodes_foreign_scope() {
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        let entry = inventory_entry_from_value(
            &scope,
            &json!({ "id": "AAMkmsg", "conversationId": "conversation-1" }),
        )
        .expect("entry");
        assert_eq!(
            entry.thread_id.expect("thread").0,
            "shared@contoso.com\u{1f}conversation-1"
        );
    }

    #[test]
    fn membership_falls_back_to_the_scope_folder_when_parent_is_absent() {
        // A delta `@removed` tombstone carries only `{id, @removed}`, so the
        // fallback is the only membership source for a deletion.
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        for value in [
            json!({ "id": "m1" }),
            json!({ "id": "m1", "parentFolderId": "" }),
            json!({ "id": "m1", "@removed": { "reason": "deleted" } }),
        ] {
            assert_eq!(
                membership_from_value(&scope, &value),
                MembershipScope::Folder(FolderId("inbox".to_string()))
            );
        }
    }

    #[test]
    fn foreign_scope_membership_uses_the_native_fallback_when_parent_is_absent() {
        let scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkRoot"),
            ty: ObjectType::Email,
        };
        let tombstone = json!({ "id": "m1", "@removed": { "reason": "deleted" } });

        assert_eq!(
            membership_from_value(&scope, &tombstone),
            MembershipScope::Folder(super::super::foreign::encode_foreign(
                "shared@contoso.com",
                "AAMkRoot"
            ))
        );
    }

    #[test]
    fn graph_etag_prefers_change_key_over_the_odata_etag() {
        assert_eq!(
            graph_etag(&json!({ "changeKey": "CK1", "@odata.etag": "W/\"E1\"" })).as_deref(),
            Some("CK1")
        );
        assert_eq!(
            graph_etag(&json!({ "@odata.etag": "W/\"E1\"" })).as_deref(),
            Some("W/\"E1\"")
        );
        assert_eq!(graph_etag(&json!({ "id": "m1" })), None);
    }

    #[test]
    fn removed_tombstones_are_detected_and_keep_their_id() {
        let removed = json!({ "id": "m1", "@removed": { "reason": "deleted" } });
        assert!(is_removed(&removed));
        assert_eq!(removed_id(&removed), Some(ObjectId("m1".to_string())));
        assert!(!is_removed(&json!({ "id": "m1" })));
        assert_eq!(removed_id(&json!({ "@removed": {} })), None);
    }

    #[test]
    fn internet_message_id_falls_back_to_the_odata_property() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let value = json!({
            "id": "m1",
            "internetMessageId": "<fallback@example.test>"
        });
        let entry = inventory_entry_from_value(&scope, &value).expect("entry");
        assert_eq!(entry.message_id.as_deref(), Some("<fallback@example.test>"));

        // The header wins when both are present.
        let both = json!({
            "id": "m1",
            "internetMessageId": "<fallback@example.test>",
            "internetMessageHeaders": [
                { "name": "message-id", "value": "<header@example.test>" }
            ]
        });
        let entry = inventory_entry_from_value(&scope, &both).expect("entry");
        assert_eq!(entry.message_id.as_deref(), Some("<header@example.test>"));
    }

    #[test]
    fn missing_change_key_projects_server_version_unavailable() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let entry =
            inventory_entry_from_value(&scope, &json!({ "id": "m1" })).expect("entry expected");
        assert!(matches!(
            entry.fingerprint.server_version,
            ServerVersion::Unavailable
        ));
    }

    #[test]
    fn an_entry_without_an_id_is_dropped() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        assert!(inventory_entry_from_value(&scope, &json!({ "changeKey": "CK1" })).is_none());
    }

    #[test]
    fn flags_hash_is_stable_under_category_order_and_case() {
        let a = json!({ "isRead": true, "categories": ["Blue", "Red"] });
        let b = json!({ "isRead": true, "categories": ["red", "BLUE"] });
        assert_eq!(flags_hash(&a), flags_hash(&b));
    }

    #[test]
    fn flags_hash_moves_when_any_tracked_flag_moves() {
        let base = json!({ "isRead": true, "flag": { "flagStatus": "notFlagged" } });
        let read_cleared = json!({ "isRead": false, "flag": { "flagStatus": "notFlagged" } });
        let flagged = json!({ "isRead": true, "flag": { "flagStatus": "flagged" } });
        let categorized = json!({ "isRead": true, "flag": { "flagStatus": "notFlagged" }, "categories": ["Work"] });

        let base_hash = flags_hash(&base);
        assert_ne!(base_hash, flags_hash(&read_cleared));
        assert_ne!(base_hash, flags_hash(&flagged));
        assert_ne!(base_hash, flags_hash(&categorized));
    }

    #[test]
    fn flags_hash_ignores_untracked_fields() {
        // Subject / body churn must not look like a flag change, or every
        // edit would re-hydrate the message.
        let a = json!({ "isRead": true, "subject": "one" });
        let b = json!({ "isRead": true, "subject": "two" });
        assert_eq!(flags_hash(&a), flags_hash(&b));
    }

    #[test]
    fn the_message_select_carries_every_field_the_projection_reads() {
        let account = test_account();
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let url = initial_delta_url(&account, &scope).expect("email scope is supported");
        // `parentFolderId` drives membership, `isRead`/`categories`/`flag`
        // drive the flags hash, `conversationId` the thread id, and
        // `internetMessageHeaders` the threading headers. Losing any of
        // them degrades sync silently rather than failing.
        for field in [
            "parentFolderId",
            "isRead",
            "categories",
            "flag",
            "conversationId",
            "internetMessageHeaders",
            "internetMessageId",
        ] {
            assert!(url.contains(field), "{field} missing from {url}");
        }
        assert!(url.contains("$top=50"));
        // `changeKey` is the documented concurrency token. The OData etag
        // remains a useful fallback, but inventory must request it directly.
        assert!(url.contains("changeKey"), "{url}");
    }

    #[test]
    fn initial_delta_url_percent_encodes_opaque_folder_ids() {
        let account = test_account();
        // Graph mail-folder ids are URL-safe base64 but can carry `=`
        // padding, which must not leak into the path unencoded.
        let scope = CursorScope::FolderType {
            folder: FolderId("AAMkAGI2/x=".to_string()),
            ty: ObjectType::Email,
        };
        let url = initial_delta_url(&account, &scope).expect("email scope is supported");
        assert!(!url.contains("AAMkAGI2/x="), "unencoded id in {url}");
        assert!(url.starts_with("/me/mailFolders/"));
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
