use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountErrorKind, AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId,
    BatchSuccess, Checkpoint, CursorScope, Fingerprint, HydratedObject, HydratedObjectKind,
    InventoryEntry, ItemOutcome, LabelId, MembershipScope, ObjectId, PageBoundary, Projection,
    ResourceKind, ServerVersion, SyncEvent, ThreadId,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use serde::Deserialize;

use crate::client::GmailClient;
use crate::encoding::decode_base64url_nopad;
use crate::headers::find_header_value_case_insensitive;
use crate::types::{GmailHeader, GmailLabel, GmailMessage, GmailProfile};

use super::blobs;
use super::cursor::cursor_for_history;
use super::error;
use super::flags;
use super::scopes::{ScopeCache, labels_for_flags};

const LIST_PAGE_SIZE: u32 = 500;
const HYDRATE_BATCH_SIZE: usize = 32;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListMessagesResponse {
    #[serde(default)]
    messages: Vec<GmailMessageStub>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GmailMessageStub {
    id: String,
}

pub(crate) fn inventory_stream(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    scope: CursorScope,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    if !matches!(scope, CursorScope::Account) {
        let account_error = error::into_account_error(
            crate::error::Error::unsupported(bifrost_types::AccountOperation::SyncInventory),
            error::GmailErrorContext::inventory(),
        );
        return Box::pin(stream::iter([SyncEvent::Terminated(account_error)]));
    }

    Box::pin(async_stream::stream! {
        // Both preludes must complete before the first list page.
        //
        // The checkpoint anchors to the historyId sampled *before* the
        // walk, not after it. `users.messages.list` returns newest-first,
        // so a message arriving mid-walk sorts ahead of every page still
        // to come and appears in none of them; anchoring to the id
        // observed at the end would put that message in neither the
        // inventory nor the subsequent change stream. Anchoring early
        // replays a few rows instead, which is idempotent for the engine.
        let prelude = async {
            let profile = client.get_profile().await?;
            let checkpoint = inventory_checkpoint(&profile)?;
            let labels = labels_for_flags(&client, &cache).await?;
            Ok::<_, crate::Error>((checkpoint, Arc::new(labels)))
        };
        let (checkpoint, labels) = match prelude.await {
            Ok(prelude) => prelude,
            Err(error) => {
                let account_error = error::into_account_error(
                    error,
                    error::GmailErrorContext::inventory(),
                );
                yield SyncEvent::Terminated(account_error);
                return;
            }
        };
        let mut page_token = None;

        loop {
            let started = Instant::now();
            let page = match list_messages_page(&client, page_token.as_deref()).await {
                Ok(page) => page,
                Err(error) => {
                    let account_error = error::into_account_error(
                        error,
                        error::GmailErrorContext::inventory(),
                    );
                    yield SyncEvent::Terminated(account_error);
                    return;
                }
            };

            let final_page = page.next_page_token.is_none();
            let mut hydrated = stream::iter(page.messages.into_iter().map(|stub| {
                let client = Arc::clone(&client);
                let labels = Arc::clone(&labels);
                async move {
                    client
                        .get_message(&stub.id, "metadata")
                        .await
                        .map(|message| inventory_entry_from_message(&message, labels.as_slice()))
                }
            }))
            .buffer_unordered(HYDRATE_BATCH_SIZE);

            let mut items = Vec::with_capacity(HYDRATE_BATCH_SIZE);
            while let Some(result) = hydrated.next().await {
                match result {
                    Ok(item) => {
                        items.push(item);
                        if items.len() >= HYDRATE_BATCH_SIZE {
                            let out = std::mem::take(&mut items);
                            yield SyncEvent::Batch(Batch {
                                items: out,
                                page_boundary: PageBoundary::Page,
                                server_latency: started.elapsed(),
                                bytes_in: 0,
                                checkpoint: None,
                            });
                        }
                    }
                    Err(error) => {
                        let account_error = error::into_account_error(
                            error,
                            error::GmailErrorContext::inventory(),
                        );
                        // A message may be deleted after users.messages.list
                        // names it and before users.messages.get hydrates it.
                        // Inventory has no per-item failure lane, and absence
                        // is the correct current state, so absorb only this
                        // precisely classified race. Every other failure still
                        // terminates the walk.
                        if matches!(
                            account_error.kind(),
                            AccountErrorKind::NotFound(ResourceKind::Message)
                        ) {
                            continue;
                        }
                        yield SyncEvent::Terminated(account_error);
                        return;
                    }
                }
            }

            if final_page {
                yield SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in: 0,
                    checkpoint: checkpoint.clone(),
                });
                yield SyncEvent::Done(checkpoint);
                break;
            }

            if !items.is_empty() {
                yield SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Page,
                    server_latency: started.elapsed(),
                    bytes_in: 0,
                    checkpoint: None,
                });
            }
            page_token = page.next_page_token;
        }
    })
}

pub(crate) fn get_stream(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    ids: AccountStream<ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
    let state = HydrateState {
        client,
        cache,
        ids,
        projection,
        finished: false,
        emitted_done: false,
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        if state.finished {
            if state.emitted_done {
                return None;
            }
            state.emitted_done = true;
            return Some((SyncEvent::Done(None), state));
        }

        let mut ids = Vec::new();
        // `final_batch` is set only when the drain itself observes the id
        // stream close. Never poll past a full batch to learn whether more
        // ids follow: `ids` is a backpressured producer that may be waiting
        // on our output before it yields again, so a lookahead poll here
        // deadlocks hydration against its own caller, and even when it does
        // not it holds every batch hostage until one more id arrives. A full
        // last batch therefore stays `Page` and the terminator is the
        // following `Done`; the boundary is advisory and losing it on that
        // one alignment is strictly cheaper than a stall.
        let mut final_batch = false;
        while ids.len() < HYDRATE_BATCH_SIZE {
            match state.ids.next().await {
                Some(id) => ids.push(id),
                None => {
                    final_batch = true;
                    break;
                }
            }
        }
        if ids.is_empty() {
            state.finished = true;
            state.emitted_done = true;
            return Some((SyncEvent::Done(None), state));
        }
        if final_batch {
            state.finished = true;
        }

        // Resolved after the id drain, not before it: `labels_for_flags`
        // can hit the network, and the poll that discovers an exhausted
        // id stream has no hydration to canonicalize for.
        let labels = match labels_for_flags(&state.client, &state.cache).await {
            Ok(labels) => labels,
            Err(error) => {
                state.finished = true;
                state.emitted_done = true;
                let account_error = error::into_account_error(
                    error,
                    error::GmailErrorContext::hydrate_message(ids[0].0.clone()),
                );
                return Some((SyncEvent::Terminated(account_error), state));
            }
        };

        let started = Instant::now();
        let mut items: Vec<ItemOutcome<HydratedObject>> = Vec::with_capacity(ids.len());
        for id in ids {
            // Clone the id so a failing hydrate can attach
            // `ErrorScope::Message { id }` to the resulting
            // `AccountError` (and so the per-item lane carries it as
            // a `BatchItemId`); moved into `hydrate_one`, the error
            // scope would carry an empty string.
            let id_for_error = id.0.clone();
            match hydrate_one(&state.client, &labels, id, state.projection).await {
                Ok(hydrated) => {
                    items.push(ItemOutcome::Succeeded(BatchSuccess::new(
                        BatchItemId(id_for_error),
                        hydrated,
                    )));
                }
                Err(err) => {
                    let account_error = error::into_account_error(
                        err,
                        error::GmailErrorContext::hydrate_message(id_for_error.clone()),
                    );
                    items.push(ItemOutcome::Failed(BatchFailure::new(
                        BatchItemId(id_for_error),
                        account_error,
                    )));
                }
            }
        }

        Some((
            SyncEvent::Batch(Batch {
                items,
                page_boundary: if final_batch {
                    PageBoundary::Final
                } else {
                    PageBoundary::Page
                },
                server_latency: started.elapsed(),
                bytes_in: 0,
                checkpoint: None,
            }),
            state,
        ))
    }))
}

struct HydrateState {
    client: Arc<GmailClient>,
    cache: ScopeCache,
    ids: AccountStream<ObjectId>,
    projection: Projection,
    finished: bool,
    emitted_done: bool,
}

async fn list_messages_page(
    client: &GmailClient,
    page_token: Option<&str>,
) -> crate::Result<ListMessagesResponse> {
    let path = inventory_list_path(page_token);
    client.get(&path).await
}

fn inventory_list_path(page_token: Option<&str>) -> String {
    format!(
        "/messages{}",
        crate::api::mail_list_query(None, Some(LIST_PAGE_SIZE), page_token)
    )
}

fn inventory_checkpoint(profile: &GmailProfile) -> crate::Result<Option<Checkpoint>> {
    let history_id = profile.history_id.parse::<u64>().map_err(|error| {
        crate::Error::invalid_request(
            AccountOperation::SyncInventory,
            format!("gmail profile carried invalid history id: {error}"),
        )
    })?;
    Ok(Some(Checkpoint::Change(cursor_for_history(
        history_id,
        &profile.email_address,
    ))))
}

async fn hydrate_one(
    client: &GmailClient,
    labels: &[GmailLabel],
    id: ObjectId,
    projection: Projection,
) -> crate::Result<HydratedObject> {
    match projection {
        Projection::FlagsOnly => {
            let message = client.get_message(&id.0, "minimal").await?;
            Ok(HydratedObject {
                id,
                kind: HydratedObjectKind::FlagsOnly(flags::flag_set(&message.label_ids, labels)),
                blobs: Vec::new(),
            })
        }
        Projection::Metadata => {
            let message = client.get_message(&id.0, "metadata").await?;
            Ok(HydratedObject {
                id,
                kind: HydratedObjectKind::Metadata(inventory_entry_from_message(&message, labels)),
                blobs: Vec::new(),
            })
        }
        Projection::FullWithBlobs => {
            let raw = client.get_message(&id.0, "raw").await?;
            let full = client.get_message(&id.0, "full").await?;
            Ok(HydratedObject {
                id,
                kind: HydratedObjectKind::RawMime(raw_bytes(&raw)?),
                blobs: blobs::blob_handles_for_message(&full),
            })
        }
        Projection::Headers | Projection::Preview(_) | Projection::TextOnly | Projection::Full => {
            let message = client.get_message(&id.0, "raw").await?;
            Ok(HydratedObject {
                id,
                kind: HydratedObjectKind::RawMime(raw_bytes(&message)?),
                blobs: Vec::new(),
            })
        }
        _ => {
            let message = client.get_message(&id.0, "metadata").await?;
            Ok(HydratedObject {
                id,
                kind: HydratedObjectKind::Metadata(inventory_entry_from_message(&message, labels)),
                blobs: Vec::new(),
            })
        }
    }
}

pub(crate) fn raw_bytes(message: &GmailMessage) -> crate::Result<Bytes> {
    let raw = message.raw.as_deref().ok_or_else(|| {
        crate::Error::missing_field("raw", "gmail raw projection did not include raw bytes")
    })?;
    let bytes = decode_base64url_nopad(raw)?;
    Ok(Bytes::from(bytes))
}

pub(crate) fn inventory_entry_from_message(
    message: &GmailMessage,
    labels: &[GmailLabel],
) -> InventoryEntry {
    let size = non_negative_u64(message.size_estimate);
    let canonical = flags::canonical_flags(&message.label_ids, labels);
    InventoryEntry {
        id: ObjectId(message.id.clone()),
        memberships: message
            .label_ids
            .iter()
            .map(|label| MembershipScope::Label(LabelId(label.clone())))
            .collect(),
        size,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: message
                .history_id
                .as_deref()
                .and_then(|id| id.parse::<u64>().ok())
                .map_or(ServerVersion::Unavailable, ServerVersion::HistoryAt),
            size,
            flags_hash: canonical.hash,
        },
        thread_id: Some(ThreadId(message.thread_id.clone())),
        message_id: header(message, "Message-ID"),
        references: header(message, "References").map_or_else(Vec::new, |value| {
            value
                .split_whitespace()
                .map(|item| item.trim_matches(|ch| ch == '<' || ch == '>').to_string())
                .collect()
        }),
        in_reply_to: header(message, "In-Reply-To"),
    }
}

fn header(message: &GmailMessage, name: &str) -> Option<String> {
    find_header_value_case_insensitive(
        headers(message),
        name,
        |h| h.name.as_str(),
        |h| h.value.as_str(),
    )
}

fn headers(message: &GmailMessage) -> &[GmailHeader] {
    message
        .payload
        .as_ref()
        .map_or(&[] as &[GmailHeader], |payload| payload.headers.as_slice())
}

fn non_negative_u64(value: Option<i64>) -> Option<u64> {
    value.and_then(|value| u64::try_from(value).ok())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bifrost_net::test_support::{Canned, ScriptedDispatch, canned};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use futures::StreamExt;
    use reqwest::StatusCode;

    use super::*;
    use serde_json::json;

    fn message(value: serde_json::Value) -> GmailMessage {
        serde_json::from_value(value).expect("message fixture deserializes")
    }

    fn with_headers(id: &str, headers: serde_json::Value) -> GmailMessage {
        message(json!({
            "id": id,
            "threadId": format!("thread-{id}"),
            "labelIds": ["INBOX"],
            "historyId": "12345",
            "sizeEstimate": 2048,
            "payload": { "mimeType": "text/plain", "headers": headers },
        }))
    }

    fn user_label() -> Vec<GmailLabel> {
        vec![GmailLabel {
            id: "Label_1".to_owned(),
            name: "Work".to_owned(),
            label_type: Some("user".to_owned()),
            color: None,
        }]
    }

    fn scripted_client(steps: Vec<Canned>) -> Arc<GmailClient> {
        let script = ScriptedDispatch::new(steps);
        let token_source = Arc::new(StaticTokenSource::new("token", None));
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            token_source,
            RetryPolicy::disabled(),
        );
        Arc::new(GmailClient::with_account_net("https://gmail.test", net))
    }

    fn ok_json(value: serde_json::Value) -> Canned {
        let body = serde_json::to_vec(&value).expect("fixture serializes");
        Canned::Response {
            status: StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(body),
        }
    }

    #[test]
    fn non_negative_conversion_drops_negatives_and_absent_values() {
        assert_eq!(non_negative_u64(Some(0)), Some(0));
        assert_eq!(non_negative_u64(Some(2048)), Some(2048));
        assert_eq!(non_negative_u64(None), None);
        assert_eq!(
            non_negative_u64(Some(-1)),
            None,
            "a negative size must not wrap into a huge u64"
        );
        assert_eq!(non_negative_u64(Some(i64::MIN)), None);
    }

    #[test]
    fn inventory_entry_projects_ids_memberships_and_size() {
        let msg = with_headers("m1", json!([]));
        let entry = inventory_entry_from_message(&msg, &user_label());

        assert_eq!(entry.id, ObjectId("m1".to_owned()));
        assert_eq!(entry.thread_id, Some(ThreadId("thread-m1".to_owned())));
        assert_eq!(entry.size, Some(2048));
        assert_eq!(entry.fingerprint.size, Some(2048));
        assert_eq!(
            entry.memberships,
            vec![MembershipScope::Label(LabelId("INBOX".to_owned()))]
        );
        assert!(
            entry.blob_id.is_none(),
            "the inventory pass carries no blob handle"
        );
    }

    /// Gmail's per-message `historyId` becomes the fingerprint's server
    /// version; a missing or unparseable one degrades to `Unavailable`
    /// rather than to a bogus zero.
    #[test]
    fn server_version_comes_from_the_messages_history_id() {
        let msg = with_headers("m2", json!([]));
        assert_eq!(
            inventory_entry_from_message(&msg, &[])
                .fingerprint
                .server_version,
            ServerVersion::HistoryAt(12345)
        );

        let no_history = message(json!({ "id": "m3", "threadId": "t3" }));
        assert_eq!(
            inventory_entry_from_message(&no_history, &[])
                .fingerprint
                .server_version,
            ServerVersion::Unavailable
        );

        let junk_history =
            message(json!({ "id": "m4", "threadId": "t4", "historyId": "not-a-number" }));
        assert_eq!(
            inventory_entry_from_message(&junk_history, &[])
                .fingerprint
                .server_version,
            ServerVersion::Unavailable
        );
    }

    #[test]
    fn threading_headers_are_read_case_insensitively() {
        let msg = with_headers(
            "m5",
            json!([
                { "name": "message-id", "value": "<a@example.test>" },
                { "name": "IN-REPLY-TO", "value": "<parent@example.test>" },
            ]),
        );
        let entry = inventory_entry_from_message(&msg, &[]);
        assert_eq!(entry.message_id.as_deref(), Some("<a@example.test>"));
        assert_eq!(
            entry.in_reply_to.as_deref(),
            Some("<parent@example.test>"),
            "header lookup must not depend on Gmail's capitalisation"
        );
    }

    /// `References` is split on whitespace and the angle brackets are
    /// trimmed, so the entry carries bare message ids.
    #[test]
    fn references_are_split_and_unbracketed() {
        let msg = with_headers(
            "m6",
            json!([{
                "name": "References",
                "value": "<a@example.test> <b@example.test>\r\n\t<c@example.test>",
            }]),
        );
        let entry = inventory_entry_from_message(&msg, &[]);
        assert_eq!(
            entry.references,
            vec![
                "a@example.test".to_owned(),
                "b@example.test".to_owned(),
                "c@example.test".to_owned(),
            ]
        );
    }

    #[test]
    fn absent_threading_headers_yield_empty_projections() {
        let msg = with_headers("m7", json!([]));
        let entry = inventory_entry_from_message(&msg, &[]);
        assert!(entry.message_id.is_none());
        assert!(entry.in_reply_to.is_none());
        assert!(entry.references.is_empty());
    }

    /// The fingerprint's `flags_hash` is what the engine compares to
    /// decide an object changed. It must move when the label set moves
    /// and must not move for an identical set.
    #[test]
    fn flags_hash_is_stable_for_a_given_label_set() {
        let unread = message(json!({
            "id": "m8", "threadId": "t8", "labelIds": ["INBOX", "UNREAD"],
        }));
        let read = message(json!({
            "id": "m8", "threadId": "t8", "labelIds": ["INBOX"],
        }));
        let a = inventory_entry_from_message(&unread, &[])
            .fingerprint
            .flags_hash;
        let b = inventory_entry_from_message(&unread, &[])
            .fingerprint
            .flags_hash;
        let c = inventory_entry_from_message(&read, &[])
            .fingerprint
            .flags_hash;
        assert_eq!(a, b, "the same label set must hash identically");
        assert_ne!(a, c, "flipping UNREAD must move the hash");
    }

    /// `canonical_flags` renders a user label as
    /// `$gmail-label:<id>:<name>` and falls back to the id when the
    /// label list does not know the id. The stream entry points refresh
    /// the vocabulary before calling this helper so they cannot persist
    /// the fallback hash on a cold start.
    #[test]
    fn an_empty_label_list_changes_the_flags_hash_for_user_labels() {
        let msg = message(json!({
            "id": "m9", "threadId": "t9", "labelIds": ["Label_1"],
        }));
        let with_names = inventory_entry_from_message(&msg, &user_label());
        let without_names = inventory_entry_from_message(&msg, &[]);
        assert_ne!(
            with_names.fingerprint.flags_hash, without_names.fingerprint.flags_hash,
            "the same message hashes differently depending on whether the label \
             cache happened to be populated",
        );
    }

    // ---- raw_bytes ----------------------------------------------------

    #[test]
    fn raw_bytes_decodes_base64url_without_padding() {
        // "From: a@b\r\n\r\nhi" base64url, unpadded.
        let msg = message(json!({
            "id": "m10",
            "threadId": "t10",
            "raw": "RnJvbTogYUBiDQoNCmhp",
        }));
        let bytes = raw_bytes(&msg).expect("raw projection decodes");
        assert_eq!(bytes.as_ref(), b"From: a@b\r\n\r\nhi");
    }

    #[test]
    fn raw_bytes_rejects_a_message_without_raw_octets() {
        let msg = message(json!({ "id": "m11", "threadId": "t11" }));
        assert!(
            raw_bytes(&msg).is_err(),
            "a raw projection that came back without raw bytes is a protocol fault, \
             not an empty message",
        );
    }

    #[test]
    fn raw_bytes_rejects_undecodable_base64() {
        let msg = message(json!({ "id": "m12", "threadId": "t12", "raw": "!!!not base64!!!" }));
        assert!(raw_bytes(&msg).is_err());
    }

    // ---- paging constants ---------------------------------------------

    #[test]
    fn page_and_batch_sizes_stay_inside_the_gmail_limits() {
        assert_eq!(
            LIST_PAGE_SIZE, 500,
            "500 is the users.messages.list maximum"
        );
        assert_eq!(HYDRATE_BATCH_SIZE, 32);
    }

    #[test]
    fn inventory_lists_spam_and_trash_and_encodes_its_page_token() {
        assert_eq!(
            inventory_list_path(None),
            "/messages?includeSpamTrash=true&maxResults=500"
        );
        assert_eq!(
            inventory_list_path(Some("next+page")),
            "/messages?includeSpamTrash=true&maxResults=500&pageToken=next%2Bpage"
        );
    }

    #[test]
    fn inventory_checkpoint_uses_the_profile_sampled_before_paging() {
        let checkpoint = inventory_checkpoint(&GmailProfile {
            email_address: "person@example.com".to_string(),
            history_id: "100".to_string(),
        })
        .expect("valid profile")
        .expect("change checkpoint");
        let Checkpoint::Change(cursor) = checkpoint else {
            panic!("expected a Gmail change checkpoint");
        };
        let state =
            super::super::cursor::decode_gmail_state(&cursor.server_state).expect("decode state");
        assert_eq!(state.history_id, 100);
        assert_eq!(state.profile_email, "person@example.com");
    }

    #[tokio::test]
    async fn inventory_absorbs_a_list_get_not_found_race_and_finishes() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({ "labels": [] })),
            ok_json(json!({ "messages": [{ "id": "gone" }, { "id": "live" }] })),
            canned(
                StatusCode::NOT_FOUND,
                br#"{"error":{"code":404,"message":"Not Found","status":"NOT_FOUND"}}"#,
            ),
            ok_json(json!({ "id": "live", "threadId": "thread-live" })),
        ]);

        let events = inventory_stream(
            client,
            Arc::new(std::sync::RwLock::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            CursorScope::Account,
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events.len(), 2);
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("inventory must emit its surviving final batch");
        };
        assert!(matches!(batch.page_boundary, PageBoundary::Final));
        assert_eq!(batch.items.len(), 1);
        assert_eq!(batch.items[0].id, ObjectId("live".to_string()));
        assert!(matches!(events[1], SyncEvent::Done(Some(_))));
    }

    #[tokio::test]
    async fn empty_inventory_emits_a_checkpointed_final_batch() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({ "labels": [] })),
            ok_json(json!({ "messages": [] })),
        ]);

        let events = inventory_stream(
            client,
            Arc::new(std::sync::RwLock::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            CursorScope::Account,
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events.len(), 2);
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("empty inventory must still expose its final boundary");
        };
        assert!(batch.items.is_empty());
        assert!(matches!(batch.page_boundary, PageBoundary::Final));
        assert!(batch.checkpoint.is_some());
        assert!(matches!(events[1], SyncEvent::Done(Some(_))));
    }

    #[tokio::test]
    async fn get_stream_marks_its_last_nonempty_batch_final() {
        let client = scripted_client(vec![
            ok_json(json!({ "labels": [] })),
            ok_json(json!({ "id": "m1", "threadId": "t1" })),
            ok_json(json!({ "id": "m2", "threadId": "t2" })),
        ]);
        let ids = Box::pin(stream::iter([
            ObjectId("m1".to_string()),
            ObjectId("m2".to_string()),
        ]));

        let events = get_stream(
            client,
            Arc::new(std::sync::RwLock::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            ids,
            Projection::Metadata,
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events.len(), 2);
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("hydration must emit a batch");
        };
        assert!(matches!(batch.page_boundary, PageBoundary::Final));
        assert_eq!(batch.items.len(), 2);
        assert!(matches!(events[1], SyncEvent::Done(None)));
    }

    // A backpressured producer hands over a full batch and then goes quiet
    // while it waits for hydration output. Hydration must emit that batch
    // without polling the id stream again; a lookahead poll to decide the
    // page boundary parks here forever. Time is paused, so the timeout fires
    // only once every other task is idle - that is the deadlock, not a race
    // against a wall clock.
    #[tokio::test(start_paused = true)]
    async fn hydration_emits_a_full_batch_without_waiting_for_another_id() {
        let mut script = vec![ok_json(json!({ "labels": [] }))];
        let mut ids = Vec::new();
        for index in 0..HYDRATE_BATCH_SIZE {
            let id = format!("m{index}");
            script.push(ok_json(json!({ "id": id, "threadId": "t" })));
            ids.push(ObjectId(id));
        }

        // Yields exactly one full batch, then stays open and pending.
        let id_stream = Box::pin(stream::iter(ids).chain(stream::once(futures::future::pending())));

        let mut events = get_stream(
            scripted_client(script),
            Arc::new(std::sync::RwLock::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            id_stream,
            Projection::Metadata,
        );

        let first = tokio::time::timeout(std::time::Duration::from_secs(30), events.next())
            .await
            .expect("hydration must not block on an id beyond the batch it already holds");

        let Some(SyncEvent::Batch(batch)) = first else {
            panic!("hydration must emit the full batch it already holds");
        };
        assert_eq!(batch.items.len(), HYDRATE_BATCH_SIZE);
        // The producer is still open, so this batch cannot claim to be final.
        assert!(matches!(batch.page_boundary, PageBoundary::Page));
    }
}
