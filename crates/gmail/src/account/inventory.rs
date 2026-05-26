use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, Checkpoint, CursorScope, Fingerprint, HydratedObject,
    HydratedObjectKind, InventoryEntry, LabelId, MembershipScope, ObjectId, PageBoundary,
    Projection, ServerVersion, SyncEvent, ThreadId,
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
use super::flags;
use super::recovery;
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
        let account_error = recovery::into_account_error(
            crate::error::Error::unsupported(bifrost_types::AccountOperation::SyncInventory),
            recovery::GmailErrorContext::inventory(),
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
                    let account_error = recovery::into_account_error(
                        error,
                        recovery::GmailErrorContext::inventory(),
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
                        let account_error = recovery::into_account_error(
                            error,
                            recovery::GmailErrorContext::inventory(),
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
                        let account_error = recovery::into_account_error(
                            error,
                            recovery::GmailErrorContext::inventory(),
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
) -> AccountStream<SyncEvent<HydratedObject>> {
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
        let mut items = Vec::with_capacity(ids.len());
        for id in ids {
            match hydrate_one(&state.client, &labels, id, state.projection).await {
                Ok(hydrated) => items.push(hydrated),
                Err(error) => {
                    let account_error = recovery::into_account_error(
                        error,
                        recovery::GmailErrorContext::hydrate_message(""),
                    );
                    state.finished = true;
                    state.emitted_done = true;
                    return Some((SyncEvent::Terminated(account_error), state));
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

fn raw_bytes(message: &GmailMessage) -> crate::Result<Bytes> {
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
