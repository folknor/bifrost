use std::time::Instant;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId, BatchSuccess, ErrorScope,
    FlagOp, IdempotencyKey, ItemOutcome, MembershipScope, MutationSuccess, ObjectId, PageBoundary,
    SyncEvent,
};
use futures::StreamExt;

use crate::core::transport::HttpTransport;
use crate::email::{EmailGet, EmailId, EmailPatch, EmailSet};
use crate::mailbox::MailboxId;

use super::capabilities::CoreLimits;
use super::state_cache::{self, StateMap};

type MailAccount<T> = crate::account::Account<T>;

pub(crate) fn set_flags<T: HttpTransport>(
    mail: MailAccount<T>,
    limits: CoreLimits,
    email_states: StateMap,
    account_id: String,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    match &op {
        FlagOp::Add(flags) | FlagOp::Remove(flags) if flags.is_empty() => {
            skipped_flag_stream(targets, limits)
        }
        FlagOp::Patch { add, remove } if add.is_empty() && remove.is_empty() => {
            skipped_flag_stream(targets, limits)
        }
        FlagOp::Add(_) | FlagOp::Remove(_) | FlagOp::Set(_) | FlagOp::Patch { .. } => {
            mutation_stream(
                mail,
                limits,
                email_states,
                account_id,
                targets,
                MutationKind::Flags(op),
            )
        }
        _ => Box::pin(async_stream::stream! {
            yield super::error::terminated_unsupported(
                AccountOperation::UpdateFlags,
                None,
                "JMAP does not support this FlagOp variant",
            );
        }),
    }
}

/// An empty additive/subtractive flag operation is an intentional local
/// no-op. Preserve one outcome per target, but never send `{}` patches that a
/// server would acknowledge as if a mutation had been applied.
fn skipped_flag_stream(
    mut targets: AccountStream<ObjectId>,
    limits: CoreLimits,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    Box::pin(async_stream::stream! {
        let batch_size = limits.max_objects_in_set.clamp(1, 500);
        let mut items = Vec::with_capacity(batch_size);
        while let Some(id) = targets.next().await {
            items.push(ItemOutcome::Succeeded(BatchSuccess::new(
                BatchItemId(id.0),
                MutationSuccess::Skipped,
            )));
            if items.len() >= batch_size {
                yield SyncEvent::Batch(Batch {
                    items: std::mem::take(&mut items),
                    page_boundary: PageBoundary::Page,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: None,
                });
            }
        }
        if !items.is_empty() {
            yield SyncEvent::Batch(Batch {
                items,
                page_boundary: PageBoundary::Page,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: None,
            });
        }
        yield SyncEvent::Done(None);
    })
}

pub(crate) fn move_to<T: HttpTransport>(
    mail: MailAccount<T>,
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

pub(crate) fn destroy<T: HttpTransport>(
    mail: MailAccount<T>,
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

fn mutation_stream<T: HttpTransport>(
    mail: MailAccount<T>,
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

async fn apply_batch<T: HttpTransport>(
    mail: &MailAccount<T>,
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
        results.push(classify_set_response(raw, id, operation));
    }

    Ok(Some(Batch {
        items: results,
        page_boundary: PageBoundary::Page,
        server_latency: started.elapsed(),
        bytes_in: 0,
        checkpoint: None,
    }))
}

/// Turn the answer for one submitted `Email/set` id into exactly one
/// per-item outcome. The response method has already arrived, so any
/// protocol-level omission must retain acknowledged transmission evidence.
fn classify_set_response(
    raw: crate::Result<()>,
    id: ObjectId,
    operation: AccountOperation,
) -> ItemOutcome<MutationSuccess> {
    match raw {
        Ok(()) => ItemOutcome::Succeeded(BatchSuccess::new(
            BatchItemId(id.0.clone()),
            MutationSuccess::Applied,
        )),
        Err(crate::Error::Set(set_error)) => {
            let ctx = super::error::JmapErrorContext::new(operation);
            let item_scope = Some(ErrorScope::Message { id: id.0.clone() });
            super::error::classify_set_item(set_error, ctx, BatchItemId(id.0.clone()), item_scope)
        }
        Err(crate::Error::IdNotFound(_)) => ItemOutcome::Failed(BatchFailure::new(
            BatchItemId(id.0.clone()),
            super::error::set_id_unanswered(
                &id.0,
                super::error::JmapErrorContext::new(operation)
                    .with_scope(ErrorScope::Message { id: id.0.clone() }),
            ),
        )),
        Err(err) => ItemOutcome::Failed(BatchFailure::new(
            BatchItemId(id.0.clone()),
            super::error::into_account_error(
                err,
                super::error::JmapErrorContext::new(operation)
                    .with_scope(ErrorScope::Message { id: id.0.clone() }),
            ),
        )),
    }
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

async fn send_set<T: HttpTransport>(
    mail: &MailAccount<T>,
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

async fn current_or_probe_state<T: HttpTransport>(
    mail: &MailAccount<T>,
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

pub(crate) async fn probe_email_state<T: HttpTransport>(
    mail: &MailAccount<T>,
) -> crate::Result<String> {
    Ok(mail
        .call(EmailGet::new().ids(Vec::<EmailId>::new()))
        .await?
        .into_state())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{AccountErrorKind, RecoveryClass, RetryDisposition};

    /// Build the `Email/set` body `send_set` would put on the wire for one
    /// id under one mutation kind, without a transport. Mirrors `send_set`
    /// exactly, so the assertions below pin the real request shape.
    fn set_body(kind: &MutationKind, ids: &[&str], state: &str) -> serde_json::Value {
        let mut set = EmailSet::new().if_in_state(state.to_string());
        match kind {
            MutationKind::Destroy => {
                set = set.destroy(ids.iter().map(|id| EmailId::new(*id)));
            }
            MutationKind::Flags(op) => {
                for id in ids {
                    apply_flags(set.update(EmailId::new(*id)), op);
                }
            }
            MutationKind::Move(mailbox_id) => {
                for id in ids {
                    set.update(EmailId::new(*id))
                        .mailbox_ids([mailbox_id.clone()]);
                }
            }
        }
        serde_json::to_value(&set).expect("Email/set serializes")
    }

    fn flags(values: &[&str]) -> std::collections::HashSet<String> {
        values.iter().map(|value| String::from(*value)).collect()
    }

    #[test]
    fn every_batch_is_gated_by_if_in_state() {
        // `MutationConcurrency::StateBased` is advertised in
        // `capabilities.rs`; the guard has to actually be on the wire or
        // the engine's optimistic-concurrency contract is a fiction.
        let body = set_body(&MutationKind::Destroy, &["m1"], "state-7");
        assert_eq!(body["ifInState"], serde_json::json!("state-7"));
        assert_eq!(body["destroy"], serde_json::json!(["m1"]));
    }

    #[test]
    fn flag_add_and_remove_use_dotted_keyword_paths() {
        // RFC 8620 s5.3: a PatchObject removes a key with `null`, never
        // with `false`. Add sets `true`, remove sets `null`, and both ride
        // dotted `keywords/<flag>` paths so unrelated keywords survive.
        let add = set_body(
            &MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
            &["m1"],
            "s1",
        );
        assert_eq!(
            add["update"]["m1"]["keywords/$seen"],
            serde_json::json!(true)
        );

        let remove = set_body(
            &MutationKind::Flags(FlagOp::Remove(flags(&["$seen"]))),
            &["m1"],
            "s1",
        );
        assert_eq!(
            remove["update"]["m1"].get("keywords/$seen"),
            Some(&serde_json::Value::Null),
            "removal must be an explicit null key, not false and not an omission"
        );
        // Neither form assigns the whole `keywords` map.
        assert!(add["update"]["m1"].get("keywords").is_none());
        assert!(remove["update"]["m1"].get("keywords").is_none());
    }

    #[test]
    fn flag_set_replaces_the_whole_keyword_map() {
        let body = set_body(
            &MutationKind::Flags(FlagOp::Set(flags(&["$seen"]))),
            &["m1"],
            "s1",
        );
        assert_eq!(
            body["update"]["m1"]["keywords"],
            serde_json::json!({"$seen": true}),
            "FlagOp::Set is a wholesale replacement, so it assigns the map"
        );
        assert!(body["update"]["m1"].get("keywords/$seen").is_none());
    }

    #[test]
    fn a_flag_patch_applies_removals_after_additions() {
        // `FlagOp::Patch` writes adds then removes into one dotted patch.
        // A flag named in both sets therefore ends up removed - pinning the
        // precedence so a reordering does not silently invert it.
        let body = set_body(
            &MutationKind::Flags(FlagOp::Patch {
                add: flags(&["$seen", "$flagged"]),
                remove: flags(&["$seen"]),
            }),
            &["m1"],
            "s1",
        );
        assert_eq!(
            body["update"]["m1"]["keywords/$flagged"],
            serde_json::json!(true)
        );
        assert_eq!(
            body["update"]["m1"].get("keywords/$seen"),
            Some(&serde_json::Value::Null),
            "remove is applied last and wins"
        );
    }

    #[tokio::test]
    async fn an_empty_flag_op_short_circuits_to_skipped_outcomes() {
        let targets: AccountStream<ObjectId> = Box::pin(futures::stream::iter([
            ObjectId("m1".into()),
            ObjectId("m2".into()),
        ]));
        let mut stream = skipped_flag_stream(
            targets,
            CoreLimits {
                max_objects_in_get: 1,
                max_objects_in_set: 1,
            },
        );

        for expected in ["m1", "m2"] {
            match stream.next().await {
                Some(SyncEvent::Batch(batch)) => match &batch.items[..] {
                    [ItemOutcome::Succeeded(success)] => {
                        assert_eq!(success.item.0, expected);
                        assert_eq!(success.output, MutationSuccess::Skipped);
                    }
                    other => panic!("expected one skipped outcome, got {other:?}"),
                },
                other => panic!("expected skipped batch, got {other:?}"),
            }
        }
        assert!(matches!(stream.next().await, Some(SyncEvent::Done(None))));
    }

    #[test]
    fn a_move_assigns_the_destination_as_the_only_mailbox() {
        // JMAP `bulk_move` is a move, not a copy: assigning the whole
        // `mailboxIds` map (rather than dotted add/remove paths) is what
        // drops the source membership.
        let body = set_body(
            &MutationKind::Move(MailboxId::new("mbx-target")),
            &["m1"],
            "s1",
        );
        assert_eq!(
            body["update"]["m1"]["mailboxIds"],
            serde_json::json!({"mbx-target": true})
        );
    }

    #[test]
    fn a_multi_id_batch_carries_one_update_entry_per_id() {
        let body = set_body(
            &MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
            &["m1", "m2", "m3"],
            "s1",
        );
        let update = body["update"].as_object().expect("update is an object");
        assert_eq!(update.len(), 3);
        for id in ["m1", "m2", "m3"] {
            assert_eq!(update[id]["keywords/$seen"], serde_json::json!(true));
        }
    }

    #[test]
    fn surviving_state_mismatch_routes_to_per_item_failed_concurrency_conflict() {
        // A second method-level `stateMismatch` must NOT terminate the
        // bulk stream; every id is emitted on the per-item `Failed` lane
        // classified `ConcurrencyConflict -> Retry::AfterStateRefresh`.
        let err = crate::Error::Method(state_mismatch_method_error());
        let ids = vec![ObjectId("m1".into()), ObjectId("m2".into())];
        let batch = state_mismatch_failed_batch(err, &MutationKind::Destroy, ids, Instant::now());

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

    #[test]
    fn an_unanswered_set_id_is_a_reconcilable_partial_response() {
        let outcome = classify_set_response(
            Err(crate::Error::IdNotFound("m1".to_string())),
            ObjectId("m1".to_string()),
            AccountOperation::BulkDestroy,
        );

        let ItemOutcome::Failed(failure) = outcome else {
            panic!("an omitted set answer must fail on the item lane");
        };
        assert_eq!(failure.item.0, "m1");
        assert_eq!(
            failure.error.kind(),
            &AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::PartialResponse)
        );
        assert!(failure.error.recovery().requires_reconciliation());
        assert_eq!(
            failure.error.telemetry_fields().transmission_state,
            Some(bifrost_types::TransmissionState::Acknowledged)
        );
    }
}
