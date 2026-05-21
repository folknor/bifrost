use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, Error, FlagOp, IdempotencyKey, MembershipScope, MutationOutcome,
    MutationResult, ObjectId, PageBoundary, SyncEvent,
};
use futures_util::StreamExt;
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
                yield super::error::fatal_from_account_error(
                    Error::Unsupported,
                    None,
                    "JMAP bulk_move only supports MembershipScope::Mailbox",
                );
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

fn mutation_stream(
    mail: MailAccount,
    limits: CoreLimits,
    email_state: Arc<Mutex<Option<String>>>,
    mut targets: AccountStream<ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<MutationResult>> {
    Box::pin(async_stream::stream! {
        let mut batch = Vec::with_capacity(limits.max_objects_in_set.min(500).max(1));
        while let Some(target) = targets.next().await {
            batch.push(target);
            if batch.len() >= limits.max_objects_in_set.min(500).max(1) {
                match apply_batch(&mail, &email_state, &kind, &mut batch).await {
                    Ok(Some(out)) => yield SyncEvent::Batch(out),
                    Ok(None) => {}
                    Err(err) => {
                        yield super::error::fatal_from_jmap(err, None);
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
                    yield super::error::fatal_from_jmap(err, None);
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
    let ids = batch.drain(..).collect::<Vec<_>>();
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

    let mut results = Vec::with_capacity(ids.len());
    for id in ids {
        let email_id = EmailId::new(id.0.clone());
        let outcome = match kind {
            MutationKind::Destroy => match response.destroyed(&email_id) {
                Ok(()) => MutationOutcome::Applied,
                Err(err) => MutationOutcome::Failed(super::error::to_account_error(err)),
            },
            MutationKind::Flags(_) | MutationKind::Move(_) => match response.updated(&email_id) {
                Ok(_) => MutationOutcome::Applied,
                Err(err) => MutationOutcome::Failed(super::error::to_account_error(err)),
            },
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
