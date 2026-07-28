//! Gmail bulk mutation driver.
//!
//! The driver translates the operation once per stream, posts batches
//! against `users.messages.batchModify` / `batchDelete`, and emits
//! per-id `ItemOutcome<MutationSuccess>` lanes for transmitted batches.
//! Errors funnel through `account_error::into_account_error`; the driver
//! never reaches for `RecoveryClass` directly.
//!
//! The trait signature is `AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>`.
//! Per-batch outcomes use the `MutationApply` internal enum before being
//! lifted into `SyncEvent` at the stream boundary.

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
use super::error as account_error;
use super::error::{
    GmailErrorContext, applied_outcomes, is_batch_delete_scope_failure,
    merge_delete_fallback_error, mutation_error, skipped_outcomes,
};
use super::flags;
use super::flags::{LABEL_TRASH, LabelPatch, translate_flag_op};
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
    source: Option<MembershipScope>,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(
        client,
        cache,
        targets,
        MutationKind::Move {
            destination,
            source,
        },
        key,
    )
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
                MutationKind::Move {
                    destination,
                    source,
                } => {
                    state.patch = Some(move_patch(destination, source.as_ref()));
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
            MutationKind::SetFlags(_) | MutationKind::Move { .. } => {
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
                Some((terminate_event(error), state))
            }
        }
    }))
}

enum MutationKind {
    SetFlags(FlagOp),
    Move {
        destination: MembershipScope,
        source: Option<MembershipScope>,
    },
    Destroy,
}

impl MutationKind {
    fn operation(&self) -> AccountOperation {
        match self {
            Self::SetFlags(_) => AccountOperation::UpdateFlags,
            Self::Move { .. } => AccountOperation::BulkMove,
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
            // gmail-N5: translate the primary failure once and consume
            // it. The original `Error` is not used after this point;
            // the fallback diagnostic attaches the primary's outermost
            // cause via `merge_delete_fallback_error`.
            let primary = account_error::into_account_error(
                error,
                GmailErrorContext::mutation(AccountOperation::BulkDestroy),
            );
            // The fallback is a move into TRASH, so it goes through the
            // same relocation rule as every other move rather than
            // hand-rolling a patch that would leave SPAM attached.
            let fallback = flags::move_placement_patch(LABEL_TRASH);
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

fn terminate_event(error: AccountError) -> SyncEvent<ItemOutcome<MutationSuccess>> {
    SyncEvent::Terminated(error)
}

/// Translate a bulk move destination (and optional source) into a
/// Gmail label patch.
///
/// Gmail has no "move" verb; a move is an add plus the removal of every
/// container the message is leaving. The exclusive-container half of
/// that is destination-derived and lives in
/// [`flags::move_placement_patch`], shared with the single-object
/// builders in `pim.rs` so both entry points agree on the wire shape.
///
/// `source` covers the part the destination cannot imply: a *user*
/// label the message is being filed out of. Folding it into the same
/// `batchModify` is what lets a consumer drop the O(n) "bulk_move plus
/// a per-id `remove_from_container`" composition - Gmail's
/// `batchModify` expresses add-and-remove in one request.
fn move_patch(destination: &MembershipScope, source: Option<&MembershipScope>) -> LabelPatch {
    let Some(LabelId(destination_id)) = as_label(destination) else {
        return LabelPatch {
            unsupported_flags: vec!["gmail move destination must be a label".to_string()],
            ..LabelPatch::default()
        };
    };
    let mut patch = flags::move_placement_patch(destination_id);
    match source {
        None => {}
        Some(scope) => {
            let Some(LabelId(source_id)) = as_label(scope) else {
                return LabelPatch {
                    unsupported_flags: vec!["gmail move source must be a label".to_string()],
                    ..LabelPatch::default()
                };
            };
            // Removing the synthetic `archive` id is a no-op (there is
            // no such Gmail label), and a source equal to the
            // destination would ask Gmail to add and remove the same
            // label in one request.
            let redundant = flags::is_archive_id(source_id)
                || source_id.eq_ignore_ascii_case(destination_id)
                || patch
                    .remove_label_ids
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(source_id));
            if !redundant {
                patch.remove_label_ids.push(source_id.clone());
            }
        }
    }
    patch
}

fn as_label(scope: &MembershipScope) -> Option<&LabelId> {
    match scope {
        MembershipScope::Label(label) => Some(label),
        _ => None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::FolderId;

    fn label(id: &str) -> MembershipScope {
        MembershipScope::Label(LabelId(id.to_string()))
    }

    #[test]
    fn move_to_inbox_clears_spam_and_trash() {
        let patch = move_patch(&label("INBOX"), None);
        assert_eq!(patch.add_label_ids, vec!["INBOX".to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec!["SPAM".to_string(), "TRASH".to_string()],
            "bulk un-spam must strip SPAM, not merely add INBOX"
        );
        assert!(patch.unsupported_flags.is_empty());
    }

    #[test]
    fn move_to_inbox_is_case_insensitive() {
        let patch = move_patch(&label("inbox"), None);
        assert_eq!(
            patch.remove_label_ids,
            vec!["SPAM".to_string(), "TRASH".to_string()]
        );
    }

    #[test]
    fn move_to_user_label_clears_every_exclusive_container() {
        let patch = move_patch(&label("Label_42"), None);
        assert_eq!(patch.add_label_ids, vec!["Label_42".to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec!["INBOX".to_string(), "SPAM".to_string(), "TRASH".to_string()],
            "filing a spammed message into a label must take it out of Spam"
        );
    }

    #[test]
    fn bulk_move_to_archive_adds_no_label() {
        let patch = move_patch(&label("archive"), None);
        assert!(
            patch.add_label_ids.is_empty(),
            "`archive` is synthetic; asking Gmail to apply it is a 400"
        );
        assert_eq!(
            patch.remove_label_ids,
            vec!["INBOX".to_string(), "SPAM".to_string(), "TRASH".to_string()]
        );
        assert!(patch.unsupported_flags.is_empty());
    }

    #[test]
    fn source_label_rides_the_same_batch_modify() {
        let patch = move_patch(&label("Label_42"), Some(&label("Label_7")));
        assert_eq!(patch.add_label_ids, vec!["Label_42".to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec![
                "INBOX".to_string(),
                "SPAM".to_string(),
                "TRASH".to_string(),
                "Label_7".to_string()
            ],
            "the source detach must not cost a second request"
        );
    }

    #[test]
    fn source_already_implied_by_the_destination_is_not_repeated() {
        let patch = move_patch(&label("Label_42"), Some(&label("inbox")));
        assert_eq!(
            patch.remove_label_ids,
            vec!["INBOX".to_string(), "SPAM".to_string(), "TRASH".to_string()]
        );
    }

    #[test]
    fn synthetic_archive_source_is_dropped() {
        let patch = move_patch(&label("Label_42"), Some(&label("archive")));
        assert_eq!(
            patch.remove_label_ids,
            vec!["INBOX".to_string(), "SPAM".to_string(), "TRASH".to_string()],
            "`archive` is not a removable Gmail label"
        );
    }

    #[test]
    fn source_equal_to_destination_is_dropped() {
        let patch = move_patch(&label("Label_42"), Some(&label("Label_42")));
        assert_eq!(patch.add_label_ids, vec!["Label_42".to_string()]);
        assert!(
            !patch.remove_label_ids.contains(&"Label_42".to_string()),
            "one request must not both add and remove the same label"
        );
    }

    #[test]
    fn non_label_destination_is_unsupported() {
        let patch = move_patch(&MembershipScope::Folder(FolderId("INBOX".into())), None);
        assert!(patch.add_label_ids.is_empty());
        assert!(patch.remove_label_ids.is_empty());
        assert_eq!(patch.unsupported_flags.len(), 1);
    }

    #[test]
    fn non_label_source_is_unsupported() {
        let patch = move_patch(
            &label("Label_42"),
            Some(&MembershipScope::Folder(FolderId("INBOX".into()))),
        );
        assert!(patch.add_label_ids.is_empty());
        assert!(patch.remove_label_ids.is_empty());
        assert_eq!(patch.unsupported_flags.len(), 1);
    }
}
