use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, Error as AccountError, FlagOp, IdempotencyKey, LabelId, MembershipScope,
    MutationOutcome, MutationResult, ObjectId, PageBoundary, RecoveryClass, SyncEvent,
};
use futures::{StreamExt, stream};
use serde::Serialize;

use crate::client::GmailClient;
use crate::error::Error as GmailError;

use super::capabilities::GMAIL_BATCH_MODIFY_LIMIT;
use super::flags::{LabelPatch, translate_flag_op};
use super::idempotency;
use super::recovery;
use super::scopes::{ScopeCache, labels_for_flags};

pub(crate) fn bulk_set_flags(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    mutation_stream(client, cache, targets, MutationKind::SetFlags(op), key)
}

pub(crate) fn bulk_move(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    mutation_stream(client, cache, targets, MutationKind::Move(destination), key)
}

pub(crate) fn bulk_destroy(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    mutation_stream(client, cache, targets, MutationKind::Destroy, key)
}

fn mutation_stream(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    kind: MutationKind,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    let state = MutationState {
        client,
        cache,
        targets,
        kind,
        key,
        patch: None,
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

        if state.patch.is_none() {
            match &state.kind {
                MutationKind::SetFlags(op) => {
                    let labels = labels_for_flags(&state.client, &state.cache).await;
                    state.patch = Some(translate_flag_op(op, &labels));
                }
                MutationKind::Move(destination) => {
                    state.patch = Some(move_patch(destination));
                }
                MutationKind::Destroy => {}
            }
        }

        let mut ids = Vec::new();
        while ids.len() < GMAIL_BATCH_MODIFY_LIMIT {
            match state.targets.next().await {
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
        let event = match &state.kind {
            MutationKind::Destroy => apply_destroy(&state.client, &ids, &state.key).await,
            MutationKind::SetFlags(_) | MutationKind::Move(_) => {
                let patch = state.patch.clone().unwrap_or_default();
                apply_label_patch(&state.client, &ids, patch, &state.key).await
            }
        };

        match event {
            MutationApply::Batch(items) => Some((
                SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Page,
                    server_latency: started.elapsed(),
                    bytes_in: 0,
                    checkpoint: None,
                }),
                state,
            )),
            MutationApply::Fatal(fatal) => {
                state.finished = true;
                state.emitted_done = true;
                Some((SyncEvent::Fatal(fatal), state))
            }
        }
    }))
}

enum MutationKind {
    SetFlags(FlagOp),
    Move(MembershipScope),
    Destroy,
}

struct MutationState {
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    kind: MutationKind,
    key: IdempotencyKey,
    patch: Option<LabelPatch>,
    finished: bool,
    emitted_done: bool,
}

enum MutationApply {
    Batch(Vec<MutationResult>),
    Fatal(bifrost_types::Fatal),
}

async fn apply_label_patch(
    client: &GmailClient,
    ids: &[ObjectId],
    patch: LabelPatch,
    key: &IdempotencyKey,
) -> MutationApply {
    if !patch.unsupported_flags.is_empty() {
        return MutationApply::Batch(
            ids.iter()
                .map(|id| MutationResult {
                    id: id.clone(),
                    outcome: MutationOutcome::Skipped,
                })
                .collect(),
        );
    }
    if patch.add_label_ids.is_empty() && patch.remove_label_ids.is_empty() {
        return MutationApply::Batch(
            ids.iter()
                .map(|id| MutationResult {
                    id: id.clone(),
                    outcome: MutationOutcome::Skipped,
                })
                .collect(),
        );
    }
    let body = BatchModifyRequest {
        ids: ids.iter().map(|id| id.0.clone()).collect(),
        add_label_ids: patch.add_label_ids,
        remove_label_ids: patch.remove_label_ids,
    };
    match post_empty_json(client, "/messages/batchModify", &body, key).await {
        Ok(()) => MutationApply::Batch(applied(ids)),
        Err(error) => mutation_error(ids, error),
    }
}

async fn apply_destroy(
    client: &GmailClient,
    ids: &[ObjectId],
    key: &IdempotencyKey,
) -> MutationApply {
    let body = BatchDeleteRequest {
        ids: ids.iter().map(|id| id.0.clone()).collect(),
    };
    match post_empty_json(client, "/messages/batchDelete", &body, key).await {
        Ok(()) => MutationApply::Batch(applied(ids)),
        Err(error) if is_batch_delete_scope_failure(&error) => {
            let fallback = LabelPatch {
                add_label_ids: vec!["TRASH".to_string()],
                remove_label_ids: vec!["INBOX".to_string()],
                unsupported_flags: Vec::new(),
            };
            apply_label_patch(client, ids, fallback, key).await
        }
        Err(error) => mutation_error(ids, error),
    }
}

fn mutation_error(ids: &[ObjectId], error: GmailError) -> MutationApply {
    let recovery = recovery::classify_general_error(&error);
    if matches!(
        recovery,
        RecoveryClass::Retry { .. } | RecoveryClass::AuthLost
    ) {
        return MutationApply::Fatal(recovery::fatal_for_error(error, recovery));
    }
    let account_error = recovery::account_error_from_gmail(&error);
    MutationApply::Batch(
        ids.iter()
            .map(|id| MutationResult {
                id: id.clone(),
                outcome: MutationOutcome::Failed(account_error_from_template(&account_error)),
            })
            .collect(),
    )
}

fn account_error_from_template(error: &AccountError) -> AccountError {
    match error {
        AccountError::CursorProtocolMismatch => AccountError::CursorProtocolMismatch,
        AccountError::CursorEnvelopeUnknown => AccountError::CursorEnvelopeUnknown,
        AccountError::SchemaIncompatible => AccountError::SchemaIncompatible,
        AccountError::Unsupported => AccountError::Unsupported,
        AccountError::MissingCoreCapability => AccountError::MissingCoreCapability,
        AccountError::IdleBusy => AccountError::IdleBusy,
        AccountError::RangeOutOfBounds { start, total } => AccountError::RangeOutOfBounds {
            start: *start,
            total: *total,
        },
        AccountError::RangeNotSupported => AccountError::RangeNotSupported,
        AccountError::BlobNotByteStream => AccountError::BlobNotByteStream,
        AccountError::ConcurrencyConflict => AccountError::ConcurrencyConflict,
        AccountError::Transport(message) => AccountError::Transport(message.clone()),
        AccountError::Auth(message) => AccountError::Auth(message.clone()),
        AccountError::Other(message) => AccountError::Other(message.clone()),
        _ => AccountError::Other(error.to_string()),
    }
}

fn applied(ids: &[ObjectId]) -> Vec<MutationResult> {
    ids.iter()
        .map(|id| MutationResult {
            id: id.clone(),
            outcome: MutationOutcome::Applied,
        })
        .collect()
}

fn move_patch(destination: &MembershipScope) -> LabelPatch {
    match destination {
        MembershipScope::Label(LabelId(label_id)) => LabelPatch {
            add_label_ids: vec![label_id.clone()],
            remove_label_ids: if label_id.eq_ignore_ascii_case("INBOX") {
                Vec::new()
            } else {
                vec!["INBOX".to_string()]
            },
            unsupported_flags: Vec::new(),
        },
        _ => LabelPatch {
            unsupported_flags: vec!["gmail move destination must be a label".to_string()],
            ..LabelPatch::default()
        },
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchModifyRequest {
    ids: Vec<String>,
    add_label_ids: Vec<String>,
    remove_label_ids: Vec<String>,
}

#[derive(Serialize)]
struct BatchDeleteRequest {
    ids: Vec<String>,
}

async fn post_empty_json<B: Serialize>(
    client: &GmailClient,
    path: &str,
    body: &B,
    key: &IdempotencyKey,
) -> crate::Result<()> {
    let url = if path.starts_with('/') {
        format!("{}{}", client.api_base(), path)
    } else {
        format!("{}/{}", client.api_base(), path)
    };
    let access_token = client.access_token().await;
    let mut request = client
        .http_client()
        .post(url)
        .header("Authorization", format!("Bearer {access_token}"))
        .header("Content-Type", "application/json")
        .json(body);
    for (name, value) in idempotency::wire_idempotency_headers(key) {
        request = request.header(*name, *value);
    }
    let response = request.send().await.map_err(GmailError::from)?;
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let body = response.text().await.map_err(GmailError::from)?;
    Err(GmailError::status("Gmail API", status, body))
}

fn is_batch_delete_scope_failure(error: &GmailError) -> bool {
    matches!(
        error,
        GmailError::HttpStatus {
            status,
            ..
        } if *status == reqwest::StatusCode::FORBIDDEN
    )
}
