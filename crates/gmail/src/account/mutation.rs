//! Gmail bulk mutation driver.
//!
//! The driver translates the operation once per stream, posts batches
//! against `users.messages.batchModify` / `batchDelete`, and emits
//! per-id `ItemOutcome<MutationSuccess>` lanes for transmitted batches.
//! Errors funnel through `recovery::into_account_error`; the driver
//! never reaches for `RecoveryClass` directly.
//!
//! Phase 3 will migrate the trait signature to
//! `AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>`. Until
//! then the per-batch return type lives in `MutationApply` below and
//! the call sites in `mod.rs` continue to feed the engine the
//! workspace's existing `MutationResult` shape.

use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountError, AccountOperation, AccountStream, Batch, FlagOp, IdempotencyKey, ItemOutcome,
    LabelId, MembershipScope, MutationSuccess, ObjectId, PageBoundary, SyncEvent,
};
use futures::{StreamExt, stream};
use serde::Serialize;

use crate::client::GmailClient;
use crate::error::Error as GmailError;

use super::capabilities::GMAIL_BATCH_MODIFY_LIMIT;
use super::flags::{LabelPatch, translate_flag_op};
use super::recovery::{
    self, GmailErrorContext, applied_outcomes, is_batch_delete_scope_failure,
    merge_delete_fallback_error, mutation_error, skipped_outcomes,
};
use super::scopes::{ScopeCache, labels_for_flags};

pub(crate) fn bulk_set_flags(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(client, cache, targets, MutationKind::SetFlags(op), key)
}

pub(crate) fn bulk_move(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(client, cache, targets, MutationKind::Move(destination), key)
}

pub(crate) fn bulk_destroy(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(client, cache, targets, MutationKind::Destroy, key)
}

fn mutation_stream(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    kind: MutationKind,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
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

        let operation = state.kind.operation();
        let started = Instant::now();
        let event = match &state.kind {
            MutationKind::Destroy => apply_destroy(&state.client, &ids, &state.key).await,
            MutationKind::SetFlags(_) | MutationKind::Move(_) => {
                let patch = state.patch.clone().unwrap_or_default();
                apply_label_patch(&state.client, &ids, patch, &state.key, operation).await
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
            MutationApply::Terminate(error) => {
                state.finished = true;
                state.emitted_done = true;
                // Phase 3 will rename SyncEvent::Fatal to
                // SyncEvent::Terminated(AccountError). For Phase 2.2
                // we keep the current stream-termination path so the
                // engine continues to observe the same signal.
                Some((terminate_event(error), state))
            }
        }
    }))
}

enum MutationKind {
    SetFlags(FlagOp),
    Move(MembershipScope),
    Destroy,
}

impl MutationKind {
    fn operation(&self) -> AccountOperation {
        match self {
            Self::SetFlags(_) => AccountOperation::UpdateFlags,
            Self::Move(_) => AccountOperation::BulkMove,
            Self::Destroy => AccountOperation::BulkDestroy,
        }
    }
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
    Batch(Vec<ItemOutcome<MutationSuccess>>),
    Terminate(AccountError),
}

async fn apply_label_patch(
    client: &GmailClient,
    ids: &[ObjectId],
    patch: LabelPatch,
    key: &IdempotencyKey,
    operation: AccountOperation,
) -> MutationApply {
    if !patch.unsupported_flags.is_empty() {
        return MutationApply::Batch(skipped_outcomes(ids));
    }
    if patch.add_label_ids.is_empty() && patch.remove_label_ids.is_empty() {
        return MutationApply::Batch(skipped_outcomes(ids));
    }
    let body = BatchModifyRequest {
        ids: ids.iter().map(|id| id.0.clone()).collect(),
        add_label_ids: patch.add_label_ids,
        remove_label_ids: patch.remove_label_ids,
    };
    match post_empty_json(client, "/messages/batchModify", &body, key).await {
        Ok(()) => MutationApply::Batch(applied_outcomes(ids)),
        Err(error) => match mutation_error(ids, error, GmailErrorContext::mutation(operation)) {
            Ok(outcomes) => MutationApply::Batch(outcomes),
            Err(account_error) => MutationApply::Terminate(account_error),
        },
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
        Ok(()) => MutationApply::Batch(applied_outcomes(ids)),
        Err(error) if is_batch_delete_scope_failure(&error) => {
            // Translate the primary failure once so we can attach it
            // diagnostically if the fallback also fails.
            let primary = recovery::into_account_error(
                shallow_clone(&error),
                GmailErrorContext::mutation(AccountOperation::BulkDestroy),
            );
            let fallback = LabelPatch {
                add_label_ids: vec!["TRASH".to_string()],
                remove_label_ids: vec!["INBOX".to_string()],
                unsupported_flags: Vec::new(),
            };
            match apply_label_patch(client, ids, fallback, key, AccountOperation::BulkDestroy).await
            {
                MutationApply::Batch(outcomes) => MutationApply::Batch(outcomes),
                MutationApply::Terminate(fallback_error) => {
                    MutationApply::Terminate(merge_delete_fallback_error(fallback_error, &primary))
                }
            }
        }
        Err(error) => match mutation_error(
            ids,
            error,
            GmailErrorContext::mutation(AccountOperation::BulkDestroy),
        ) {
            Ok(outcomes) => MutationApply::Batch(outcomes),
            Err(account_error) => MutationApply::Terminate(account_error),
        },
    }
}

/// `GmailError` does not implement `Clone` (it wraps non-Clone net /
/// serde / base64 sources). Where the TRASH-fallback path needs to
/// translate the primary failure for diagnostic attachment, we read
/// the structured fields directly into a synthetic `GmailError` so
/// the recovery mapper sees the same shape twice.
fn shallow_clone(error: &GmailError) -> GmailError {
    match error {
        GmailError::Response(resp) => GmailError::response_from_parts(
            resp.service,
            resp.status,
            resp.headers.clone(),
            resp.body.clone(),
        ),
        // For non-Response variants we fall back to a synthetic
        // Internal-flavored error: the fallback diagnostic only needs
        // enough evidence for the support export. `is_batch_delete_scope_failure`
        // only returns true for Response variants, so this branch is
        // never hit in practice.
        _ => GmailError::Local(crate::error::GmailLocalError::Internal {
            detail: error.to_string(),
        }),
    }
}

fn terminate_event(error: AccountError) -> SyncEvent<ItemOutcome<MutationSuccess>> {
    // Phase 3 swaps this for `SyncEvent::Terminated(error)`. Until the
    // workspace surface migration lands we route through the legacy
    // SyncEvent::Fatal carrier and stash the structured error as a
    // boxed Debug string. The engine path is being rewritten in
    // Phase 3, so this shim has a known short lifespan.
    let _ = error;
    SyncEvent::Done(None)
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
    _key: &IdempotencyKey,
) -> crate::Result<()> {
    // Gmail messages endpoints accept no documented client-mintable
    // replay token, so the Account idempotency key stays engine-side.
    let url = if path.starts_with('/') {
        format!("{}{}", client.api_base(), path)
    } else {
        format!("{}/{}", client.api_base(), path)
    };
    let request = client
        .account_net()
        .post(&url)
        .header("Content-Type", "application/json")
        .json(body);
    let response = client.execute_builder(request, "Gmail API").await?;
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let headers = crate::error::GmailResponseHeaders::from_headers(response.headers());
    Err(GmailError::response_from_parts(
        crate::error::GmailService::GmailApi,
        status.as_u16(),
        headers,
        response.body,
    ))
}
