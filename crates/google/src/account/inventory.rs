use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId, BatchSuccess, Checkpoint,
    CursorScope, Fingerprint, HydratedObject, HydratedObjectKind, InventoryEntry, ItemOutcome,
    LabelId, MembershipScope, ObjectId, PageBoundary, Projection, ServerVersion, SyncEvent,
    ThreadId,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use serde::Deserialize;

use crate::client::GmailClient;
use crate::encoding::decode_base64url_nopad;
use crate::headers::find_header_value_case_insensitive;
use crate::types::{GmailHeader, GmailLabel, GmailMessage};

use super::blobs;
use super::cursor::cursor_for_history;
use super::error;
use super::flags;
use super::scopes::{ScopeCache, snapshot};

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
            let labels = Arc::new(snapshot(&cache).labels);
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
                        yield SyncEvent::Terminated(account_error);
                        return;
                    }
                }
            }

            if final_page {
                let checkpoint = match inventory_checkpoint(&client).await {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        let account_error = error::into_account_error(
                            error,
                            error::GmailErrorContext::inventory(),
                        );
                        yield SyncEvent::Terminated(account_error);
                        return;
                    }
                };
                if !items.is_empty() {
                    yield SyncEvent::Batch(Batch {
                        items,
                        page_boundary: PageBoundary::Final,
                        server_latency: started.elapsed(),
                        bytes_in: 0,
                        checkpoint: checkpoint.clone(),
                    });
                }
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
        while ids.len() < HYDRATE_BATCH_SIZE {
            match state.ids.next().await {
                Some(id) => ids.push(id),
                None => break,
            }
        }
        if ids.is_empty() {
            state.finished = true;
            state.emitted_done = true;
            return Some((SyncEvent::Done(None), state));
        }

        let started = Instant::now();
        let labels = snapshot(&state.cache).labels;
        let mut items: Vec<ItemOutcome<HydratedObject>> = Vec::with_capacity(ids.len());
        for id in ids {
            // gmail-N1: clone the id so a failing hydrate can attach
            // `ErrorScope::Message { id }` to the resulting
            // `AccountError` (and so the per-item lane carries it as
            // a `BatchItemId`). Previously the id was moved into
            // `hydrate_one` and the error scope was emitted with an
            // empty string.
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
                page_boundary: PageBoundary::Page,
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
    let mut path = format!("/messages?maxResults={LIST_PAGE_SIZE}");
    if let Some(page_token) = page_token {
        path.push_str("&pageToken=");
        path.push_str(&bifrost_net::url::encode_component(page_token));
    }
    client.get(&path).await
}

async fn inventory_checkpoint(client: &GmailClient) -> crate::Result<Option<Checkpoint>> {
    let profile = client.get_profile().await?;
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

    /// DOCUMENTS A BUG, NOT AN ENDORSEMENT. `canonical_flags` renders a
    /// user label as `$gmail-label:<id>:<name>` and falls back to the id
    /// when the label list does not know the id. `inventory_stream` and
    /// `get_stream` both read the scope cache without refreshing it, and
    /// `ScopeSnapshot::empty()` claims to be fresh for five minutes
    /// after `open()` - so a cold-start inventory can hash the fallback
    /// spelling and then disagree with every later hydrate for the same
    /// unchanged message.
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
}
