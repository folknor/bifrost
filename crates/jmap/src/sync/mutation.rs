use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchItemId, ErrorScope, FlagOp, IdempotencyKey,
    ItemOutcome, MembershipScope, MutationOutcome, MutationResult, MutationSuccess, ObjectId,
    PageBoundary, SyncEvent,
};
use futures::StreamExt;
use tokio::sync::Mutex;

use crate::email::{EmailGet, EmailId, EmailPatch, EmailSet};
use crate::mailbox::MailboxId;
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;

type MailAccount = crate::account::Account<ReqwestTransport>;

pub(crate) fn set_flags(
    mail: MailAccount,
    limits: CoreLimits,
    email_state: Arc<Mutex<Option<String>>>,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    mutation_stream(mail, limits, email_state, targets, MutationKind::Flags(op))
}

pub(crate) fn move_to(
    mail: MailAccount,
    limits: CoreLimits,
    email_state: Arc<Mutex<Option<String>>>,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    let kind = match destination {
        MembershipScope::Mailbox(mailbox) => MutationKind::Move(MailboxId::new(mailbox.0)),
        _ => {
            return Box::pin(async_stream::stream! {
                yield super::error::terminated(super::error::unsupported_error(
                    bifrost_types::AccountOperation::BulkMove,
                    None,
                    "JMAP bulk_move only supports MembershipScope::Mailbox",
                ));
            });
        }
    };
    mutation_stream(mail, limits, email_state, targets, kind)
}

pub(crate) fn destroy(
    mail: MailAccount,
    limits: CoreLimits,
    email_state: Arc<Mutex<Option<String>>>,
    targets: AccountStream<ObjectId>,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    mutation_stream(mail, limits, email_state, targets, MutationKind::Destroy)
}

enum MutationKind {
    Flags(FlagOp),
    Move(MailboxId),
    Destroy,
}

fn operation_for_kind(kind: &MutationKind) -> AccountOperation {
    match kind {
        MutationKind::Flags(_) => AccountOperation::UpdateFlags,
        MutationKind::Move(_) => AccountOperation::BulkMove,
        MutationKind::Destroy => AccountOperation::BulkDestroy,
    }
}

fn mutation_stream(
    mail: MailAccount,
    limits: CoreLimits,
    email_state: Arc<Mutex<Option<String>>>,
    mut targets: AccountStream<ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<MutationResult>> {
    Box::pin(async_stream::stream! {
        let batch_size = limits.max_objects_in_set.clamp(1, 500);
        let mut batch = Vec::with_capacity(batch_size);
        while let Some(target) = targets.next().await {
            batch.push(target);
            if batch.len() >= batch_size {
                match apply_batch(&mail, &email_state, &kind, &mut batch).await {
                    Ok(Some(out)) => yield SyncEvent::Batch(out),
                    Ok(None) => {}
                    Err(err) => {
                        yield super::error::terminated_from_jmap(
                            err,
                            super::error::JmapErrorContext::new(operation_for_kind(&kind)),
                        );
                        return;
                    }
                }
            }
        }

        if !batch.is_empty() {
            match apply_batch(&mail, &email_state, &kind, &mut batch).await {
                Ok(Some(out)) => yield SyncEvent::Batch(out),
                Ok(None) => {}
                Err(err) => {
                    yield super::error::terminated_from_jmap(
                        err,
                        super::error::JmapErrorContext::new(operation_for_kind(&kind)),
                    );
                    return;
                }
            }
        }

        yield SyncEvent::Done(None);
    })
}

async fn apply_batch(
    mail: &MailAccount,
    email_state: &Arc<Mutex<Option<String>>>,
    kind: &MutationKind,
    batch: &mut Vec<ObjectId>,
) -> crate::Result<Option<Batch<MutationResult>>> {
    let started = Instant::now();
    let mut state = current_or_probe_state(mail, email_state).await?;
    let ids = std::mem::take(batch);
    if ids.is_empty() {
        return Ok(None);
    }

    let mut response = match send_set(mail, &state, &ids, kind).await {
        Ok(response) => response,
        Err(err) => {
            if !super::error::is_state_mismatch(&err) {
                return Err(err);
            }
            let fresh = probe_email_state(mail).await?;
            set_email_state(email_state, fresh.clone()).await;
            state = fresh;
            send_set(mail, &state, &ids, kind).await?
        }
    };

    let new_state = response.new_state().to_string();
    if !new_state.is_empty() {
        advance_email_state(email_state, Some(&state), new_state).await;
    }

    let operation = operation_for_kind(kind);
    let mut results = Vec::with_capacity(ids.len());
    for id in ids {
        let email_id = EmailId::new(id.0.clone());
        let raw = match kind {
            MutationKind::Destroy => response.destroyed(&email_id),
            MutationKind::Flags(_) | MutationKind::Move(_) => {
                response.updated(&email_id).map(|_| ())
            }
        };
        let outcome = match raw {
            Ok(()) => MutationOutcome::Applied,
            Err(crate::Error::Set(set_error)) => {
                // Wire the per-item classification helper. This is
                // the Phase 2 contact point - Phase 3 changes the
                // stream signature to `ItemOutcome<MutationSuccess>`
                // and bypasses the `MutationOutcome` adapter below.
                let ctx = super::error::JmapErrorContext::new(operation);
                let item_scope = Some(ErrorScope::Message { id: id.0.clone() });
                match super::error::classify_set_item(
                    set_error,
                    ctx,
                    BatchItemId(id.0.clone()),
                    item_scope,
                ) {
                    ItemOutcome::Succeeded(success) => match success.output {
                        MutationSuccess::Applied => MutationOutcome::Applied,
                        MutationSuccess::Skipped => MutationOutcome::Skipped,
                    },
                    ItemOutcome::Failed(failure) => MutationOutcome::Failed(failure.error),
                    ItemOutcome::Uncertain(uncertain) => MutationOutcome::Failed(uncertain.error),
                }
            }
            Err(err) => MutationOutcome::Failed(super::error::into_account_error(
                err,
                super::error::JmapErrorContext::new(operation)
                    .with_scope(ErrorScope::Message { id: id.0.clone() }),
            )),
        };
        results.push(MutationResult { id, outcome });
    }

    Ok(Some(Batch {
        items: results,
        page_boundary: PageBoundary::Page,
        server_latency: started.elapsed(),
        bytes_in: 0,
        checkpoint: None,
    }))
}

async fn send_set(
    mail: &MailAccount,
    state: &str,
    ids: &[ObjectId],
    kind: &MutationKind,
) -> crate::Result<crate::core::set::SetResponse<crate::email::Email>> {
    let mut set = EmailSet::new().if_in_state(state.to_string());

    match kind {
        MutationKind::Destroy => {
            set = set.destroy(ids.iter().map(|id| EmailId::new(id.0.clone())));
        }
        MutationKind::Flags(op) => {
            for id in ids {
                apply_flags(set.update(EmailId::new(id.0.clone())), op);
            }
        }
        MutationKind::Move(mailbox_id) => {
            for id in ids {
                set.update(EmailId::new(id.0.clone()))
                    .mailbox_ids([mailbox_id.clone()]);
            }
        }
    }

    mail.call(set).await
}

fn apply_flags(patch: &mut EmailPatch, op: &FlagOp) {
    match op {
        FlagOp::Add(flags) => {
            for flag in flags {
                patch.keyword(flag, true);
            }
        }
        FlagOp::Remove(flags) => {
            for flag in flags {
                patch.keyword(flag, false);
            }
        }
        FlagOp::Set(flags) => {
            patch.keywords(flags.iter().cloned());
        }
        FlagOp::Patch { add, remove } => {
            for flag in add {
                patch.keyword(flag, true);
            }
            for flag in remove {
                patch.keyword(flag, false);
            }
        }
        _ => {}
    }
}

async fn current_or_probe_state(
    mail: &MailAccount,
    email_state: &Arc<Mutex<Option<String>>>,
) -> crate::Result<String> {
    let cached = {
        let guard = email_state.lock().await;
        guard.clone()
    };

    match cached {
        Some(state) => Ok(state),
        None => {
            let state = probe_email_state(mail).await?;
            let mut guard = email_state.lock().await;
            match guard.clone() {
                Some(existing) => Ok(existing),
                None => {
                    *guard = Some(state.clone());
                    Ok(state)
                }
            }
        }
    }
}

pub(crate) async fn probe_email_state(mail: &MailAccount) -> crate::Result<String> {
    Ok(mail
        .call(EmailGet::new().ids(Vec::<EmailId>::new()))
        .await?
        .into_state())
}

async fn set_email_state(email_state: &Arc<Mutex<Option<String>>>, state: String) {
    let mut guard = email_state.lock().await;
    *guard = Some(state);
}

async fn advance_email_state(
    email_state: &Arc<Mutex<Option<String>>>,
    expected: Option<&str>,
    state: String,
) {
    let mut guard = email_state.lock().await;
    match (guard.as_deref(), expected) {
        (Some(current), Some(expected)) if current != expected => {}
        _ => *guard = Some(state),
    }
}
