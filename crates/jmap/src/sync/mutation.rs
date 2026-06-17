use std::time::Instant;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId, BatchSuccess, ErrorScope,
    FlagOp, IdempotencyKey, ItemOutcome, MembershipScope, MutationSuccess, ObjectId, PageBoundary,
    SyncEvent,
};
use futures::StreamExt;

use crate::email::{EmailGet, EmailId, EmailPatch, EmailSet};
use crate::mailbox::MailboxId;
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;
use super::state_cache::{self, StateMap};

type MailAccount = crate::account::Account<ReqwestTransport>;

pub(crate) fn set_flags(
    mail: MailAccount,
    limits: CoreLimits,
    email_states: StateMap,
    account_id: String,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(
        mail,
        limits,
        email_states,
        account_id,
        targets,
        MutationKind::Flags(op),
    )
}

pub(crate) fn move_to(
    mail: MailAccount,
    limits: CoreLimits,
    email_states: StateMap,
    account_id: String,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
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
    mutation_stream(mail, limits, email_states, account_id, targets, kind)
}

pub(crate) fn destroy(
    mail: MailAccount,
    limits: CoreLimits,
    email_states: StateMap,
    account_id: String,
    targets: AccountStream<ObjectId>,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(
        mail,
        limits,
        email_states,
        account_id,
        targets,
        MutationKind::Destroy,
    )
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
    email_states: StateMap,
    account_id: String,
    mut targets: AccountStream<ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    Box::pin(async_stream::stream! {
        let batch_size = limits.max_objects_in_set.clamp(1, 500);
        let mut batch = Vec::with_capacity(batch_size);
        while let Some(target) = targets.next().await {
            batch.push(target);
            if batch.len() >= batch_size {
                match apply_batch(&mail, &email_states, &account_id, &kind, &mut batch).await {
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
            match apply_batch(&mail, &email_states, &account_id, &kind, &mut batch).await {
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
    email_states: &StateMap,
    account_id: &str,
    kind: &MutationKind,
    batch: &mut Vec<ObjectId>,
) -> crate::Result<Option<Batch<ItemOutcome<MutationSuccess>>>> {
    let started = Instant::now();
    let mut state = current_or_probe_state(mail, email_states, account_id).await?;
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
            state_cache::set(email_states, account_id, fresh.clone()).await;
            state = fresh;
            match send_set(mail, &state, &ids, kind).await {
                Ok(response) => response,
                Err(err) if super::error::is_state_mismatch(&err) => {
                    // A second method-level `stateMismatch` after a
                    // state refresh is a genuine concurrency conflict,
                    // not a stream-fatal condition. Propagating it as
                    // `Err` would both terminate the whole bulk stream
                    // (instead of the documented per-item
                    // `Failed(ConcurrencyConflict)`) and falsely assert
                    // "nothing was transmitted" - the retried
                    // `Email/set` did cross the wire. Convert the
                    // method error into a per-item `Failed` lane so the
                    // engine drives `Retry::AfterStateRefresh` per id.
                    return Ok(Some(state_mismatch_failed_batch(err, kind, ids, started)));
                }
                Err(err) => return Err(err),
            }
        }
    };

    let new_state = response.new_state().to_string();
    if !new_state.is_empty() {
        state_cache::advance(email_states, account_id, Some(&state), new_state).await;
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
        let outcome: ItemOutcome<MutationSuccess> = match raw {
            Ok(()) => ItemOutcome::Succeeded(BatchSuccess::new(
                BatchItemId(id.0.clone()),
                MutationSuccess::Applied,
            )),
            Err(crate::Error::Set(set_error)) => {
                let ctx = super::error::JmapErrorContext::new(operation);
                let item_scope = Some(ErrorScope::Message { id: id.0.clone() });
                super::error::classify_set_item(
                    set_error,
                    ctx,
                    BatchItemId(id.0.clone()),
                    item_scope,
                )
            }
            Err(err) => ItemOutcome::Failed(BatchFailure::new(
                BatchItemId(id.0.clone()),
                super::error::into_account_error(
                    err,
                    super::error::JmapErrorContext::new(operation)
                        .with_scope(ErrorScope::Message { id: id.0.clone() }),
                ),
            )),
        };
        results.push(outcome);
    }

    Ok(Some(Batch {
        items: results,
        page_boundary: PageBoundary::Page,
        server_latency: started.elapsed(),
        bytes_in: 0,
        checkpoint: None,
    }))
}

/// Build a per-item `Failed(ConcurrencyConflict)` batch for every id in
/// `ids` from a surviving method-level `stateMismatch`. Each id gets its
/// own `AccountError` (scoped to that message) routed through
/// `into_account_error`, which classifies the `stateMismatch` as
/// `ConcurrencyConflict -> Retry::AfterStateRefresh`. This keeps the bulk
/// stream alive: a method-level conflict is per-item recoverable, not a
/// whole-stream terminator.
fn state_mismatch_failed_batch(
    err: crate::Error,
    kind: &MutationKind,
    ids: Vec<ObjectId>,
    started: Instant,
) -> Batch<ItemOutcome<MutationSuccess>> {
    let operation = operation_for_kind(kind);
    let mut results = Vec::with_capacity(ids.len());
    let mut err = Some(err);
    for id in ids {
        // `into_account_error` consumes the error; clone the typed
        // method error per id so every lane entry carries a full,
        // correctly-scoped `ConcurrencyConflict` classification.
        let item_err = match err.take() {
            Some(e) => e,
            None => crate::Error::Method(state_mismatch_method_error()),
        };
        let account_error = super::error::into_account_error(
            item_err,
            super::error::JmapErrorContext::new(operation)
                .with_scope(ErrorScope::Message { id: id.0.clone() }),
        );
        results.push(ItemOutcome::Failed(BatchFailure::new(
            BatchItemId(id.0.clone()),
            account_error,
        )));
    }
    Batch {
        items: results,
        page_boundary: PageBoundary::Page,
        server_latency: started.elapsed(),
        bytes_in: 0,
        checkpoint: None,
    }
}

/// A synthetic `stateMismatch` method error for the second-and-later ids
/// in a conflicted batch (the wire error is consumed by the first id).
fn state_mismatch_method_error() -> crate::core::error::MethodError {
    serde_json::from_str(r#"{"type":"stateMismatch"}"#)
        .expect("stateMismatch is a valid MethodError shape")
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
    email_states: &StateMap,
    account_id: &str,
) -> crate::Result<String> {
    if let Some(state) = state_cache::get(email_states, account_id).await {
        return Ok(state);
    }
    let state = probe_email_state(mail).await?;
    // Re-check under the lock: another task may have probed concurrently.
    if let Some(existing) = state_cache::get(email_states, account_id).await {
        return Ok(existing);
    }
    state_cache::set(email_states, account_id, state.clone()).await;
    Ok(state)
}

pub(crate) async fn probe_email_state(mail: &MailAccount) -> crate::Result<String> {
    Ok(mail
        .call(EmailGet::new().ids(Vec::<EmailId>::new()))
        .await?
        .into_state())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{AccountErrorKind, RecoveryClass, RetryDisposition};

    #[test]
    fn surviving_state_mismatch_routes_to_per_item_failed_concurrency_conflict() {
        // A second method-level `stateMismatch` must NOT terminate the
        // bulk stream; every id is emitted on the per-item `Failed` lane
        // classified `ConcurrencyConflict -> Retry::AfterStateRefresh`.
        let err = crate::Error::Method(state_mismatch_method_error());
        let ids = vec![ObjectId("m1".into()), ObjectId("m2".into())];
        let batch = state_mismatch_failed_batch(
            err,
            &MutationKind::Destroy,
            ids,
            Instant::now(),
        );

        assert_eq!(batch.items.len(), 2);
        for (idx, item) in batch.items.iter().enumerate() {
            match item {
                ItemOutcome::Failed(failure) => {
                    assert_eq!(failure.error.kind(), &AccountErrorKind::ConcurrencyConflict);
                    match failure.error.recovery() {
                        RecoveryClass::Retry(advice) => {
                            assert_eq!(advice.disposition, RetryDisposition::AfterStateRefresh);
                        }
                        other => panic!("expected Retry::AfterStateRefresh, got {other:?}"),
                    }
                    // Each lane entry is scoped to its own message id.
                    let expected = if idx == 0 { "m1" } else { "m2" };
                    assert_eq!(failure.item.0, expected);
                }
                other => panic!("expected Failed, got {other:?}"),
            }
        }
    }
}
