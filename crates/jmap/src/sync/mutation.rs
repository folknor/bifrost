use std::collections::HashMap;
use std::sync::Arc;
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
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    limits: CoreLimits,
    email_states: StateMap,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    match &op {
        FlagOp::Add(flags) | FlagOp::Remove(flags) if flags.is_empty() => mutation_stream(
            mail,
            foreign_mail,
            limits,
            email_states,
            targets,
            MutationKind::SkipFlags,
        ),
        FlagOp::Patch { add, remove } if add.is_empty() && remove.is_empty() => mutation_stream(
            mail,
            foreign_mail,
            limits,
            email_states,
            targets,
            MutationKind::SkipFlags,
        ),
        FlagOp::Add(_) | FlagOp::Remove(_) | FlagOp::Set(_) | FlagOp::Patch { .. } => {
            mutation_stream(
                mail,
                foreign_mail,
                limits,
                email_states,
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

pub(crate) fn move_to<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    limits: CoreLimits,
    email_states: StateMap,
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
    mutation_stream(mail, foreign_mail, limits, email_states, targets, kind)
}

pub(crate) fn destroy<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    limits: CoreLimits,
    email_states: StateMap,
    targets: AccountStream<ObjectId>,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(
        mail,
        foreign_mail,
        limits,
        email_states,
        targets,
        MutationKind::Destroy,
    )
}

enum MutationKind {
    /// Empty additive/subtractive flag operations are local no-ops, but use
    /// the same routing, batching, tail flush, and Done path as wire mutations.
    ///
    /// They ride `mutation_stream` rather than getting a stream of their own on
    /// purpose. A separate "simple case" path was tried and missed owner
    /// routing: a foreign-account target went out against the primary handle.
    /// One batching engine, one owner-routing rule.
    SkipFlags,
    Flags(FlagOp),
    Move(MailboxId),
    Destroy,
}

fn operation_for_kind(kind: &MutationKind) -> AccountOperation {
    match kind {
        MutationKind::SkipFlags | MutationKind::Flags(_) => AccountOperation::UpdateFlags,
        MutationKind::Move(_) => AccountOperation::BulkMove,
        MutationKind::Destroy => AccountOperation::BulkDestroy,
    }
}

/// One owner account's pending work, held between flushes.
///
/// `last_touch` is the input position at which this route last received a
/// target. It is what bounds a partial batch's wait: without it a route that
/// fell one target short of `batch_size` would sit in the map until the whole
/// input stream ended, so an owner that trickles could be starved
/// indefinitely by an owner that streams.
#[derive(Default)]
struct PendingRoute {
    /// Targets bound for this account's `Email/set`, in arrival order.
    items: Vec<ObjectId>,
    /// Outcomes already decided locally (a destination this account cannot
    /// express). They ride out in the same batch as the wire results so the
    /// caller still sees exactly one outcome per input target.
    rejected: Vec<ItemOutcome<MutationSuccess>>,
    /// Input position of the most recent target routed here.
    last_touch: usize,
}

impl PendingRoute {
    fn pending(&self) -> usize {
        self.items.len() + self.rejected.len()
    }

    fn is_empty(&self) -> bool {
        self.pending() == 0
    }
}

/// Per-owner batching for one bulk mutation stream.
///
/// Two properties the flat `HashMap` + `Vec<String>` it replaced did not
/// have, both of which only bite on multi-account (shared-mailbox) input:
///
/// * `order` holds each owner exactly ONCE. Draining empties a route in
///   place instead of removing it, so a re-fed owner does not append a
///   second entry. Removing the entry made `order` grow by one `String` per
///   input target whenever `batch_size` was 1, i.e. unbounded in the input.
/// * A partial batch is flushed once `batch_size` further targets have gone
///   to other owners. Waiting for end-of-input instead let one owner's
///   stream hold another owner's short batch for the entire run.
struct RouteBuffers {
    batch_size: usize,
    /// Count of targets accepted so far; the clock `last_touch` is read on.
    seen: usize,
    /// Distinct owners in first-arrival order.
    order: Vec<String>,
    pending: HashMap<String, PendingRoute>,
}

impl RouteBuffers {
    fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            seen: 0,
            order: Vec::new(),
            pending: HashMap::new(),
        }
    }

    /// Record one routed target and report the owners due for flush now:
    /// this owner if its batch just filled, plus any owner sitting on a
    /// partial batch that has gone `batch_size` inputs without being fed.
    fn push(
        &mut self,
        account_id: String,
        target: ObjectId,
        rejection: Option<ItemOutcome<MutationSuccess>>,
    ) -> Vec<String> {
        self.seen += 1;
        let seen = self.seen;
        let batch_size = self.batch_size;
        let order = &mut self.order;
        let route = self.pending.entry(account_id.clone()).or_insert_with(|| {
            order.push(account_id.clone());
            PendingRoute::default()
        });
        route.last_touch = seen;
        match rejection {
            Some(outcome) => route.rejected.push(outcome),
            None => route.items.push(target),
        }
        let full = route.pending() >= batch_size;

        let mut due: Vec<String> = self
            .order
            .iter()
            .filter(|owner| {
                self.pending.get(*owner).is_some_and(|route| {
                    !route.is_empty() && seen.saturating_sub(route.last_touch) >= batch_size
                })
            })
            .cloned()
            .collect();
        if full {
            due.push(account_id);
        }
        due
    }

    /// Every owner still holding work, in first-arrival order. Used for the
    /// end-of-input drain.
    fn remaining(&self) -> Vec<String> {
        self.order.clone()
    }

    /// Take one owner's pending work, leaving its route in place so the
    /// owner is never re-registered in `order`.
    fn drain(
        &mut self,
        account_id: &str,
    ) -> Option<(Vec<ObjectId>, Vec<ItemOutcome<MutationSuccess>>)> {
        let route = self.pending.get_mut(account_id)?;
        if route.is_empty() {
            return None;
        }
        Some((
            std::mem::take(&mut route.items),
            std::mem::take(&mut route.rejected),
        ))
    }
}

fn mutation_stream<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    limits: CoreLimits,
    email_states: StateMap,
    mut targets: AccountStream<ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    Box::pin(async_stream::stream! {
        let batch_size = limits.max_objects_in_set.clamp(1, 500);
        let primary_account_id = mail.id_str().to_string();
        let mut buffers = RouteBuffers::new(batch_size);

        loop {
            let (due, at_end) = match targets.next().await {
                Some(target) => {
                    let account_id =
                        account_id_for_target(&target, &primary_account_id, &foreign_mail);
                    let rejection = destination_rejection(&kind, &target);
                    (buffers.push(account_id, target, rejection), false)
                }
                None => (buffers.remaining(), true),
            };

            for account_id in due {
                let Some((mut batch, rejected)) = buffers.drain(&account_id) else {
                    continue;
                };
                let routed_mail = mail_for_account(&mail, &foreign_mail, &account_id);
                let foreign_account = account_id != primary_account_id;
                let started = Instant::now();
                match apply_batch(
                    routed_mail,
                    &email_states,
                    &account_id,
                    foreign_account,
                    &kind,
                    &mut batch,
                )
                .await
                {
                    Ok(Some(mut out)) => {
                        out.items.extend(rejected);
                        yield SyncEvent::Batch(out);
                    }
                    Ok(None) => {
                        if !rejected.is_empty() {
                            yield SyncEvent::Batch(Batch {
                                items: rejected,
                                page_boundary: PageBoundary::Page,
                                server_latency: started.elapsed(),
                                bytes_in: 0,
                                checkpoint: None,
                            });
                        }
                    }
                    Err(err) => {
                        yield super::error::terminated_from_jmap(
                            err,
                            super::error::JmapErrorContext::new(operation_for_kind(&kind)),
                        );
                        return;
                    }
                }
            }

            if at_end {
                break;
            }
        }

        yield SyncEvent::Done(None);
    })
}

/// Reject a target whose owner disagrees with the move destination's owner
/// before it can reach the wire.
///
/// The destination is one `MembershipScope` for the whole call while targets
/// are routed per owner, so the two can disagree per target. `Email/set`
/// resolves both operands inside the routed `accountId`, which makes the
/// disagreement silent rather than loud: a bare primary mailbox id sent
/// against a foreign account resolves to whatever mailbox that account has
/// under that id. Fail the individual target, not the stream - the caller's
/// other targets may be perfectly expressible.
fn destination_rejection(
    kind: &MutationKind,
    target: &ObjectId,
) -> Option<ItemOutcome<MutationSuccess>> {
    let MutationKind::Move(destination) = kind else {
        return None;
    };
    let target_owner = super::foreign::owner_of(&target.0);
    let destination_owner = super::foreign::owner_of(destination.as_str());
    if target_owner == destination_owner {
        return None;
    }
    Some(ItemOutcome::Failed(BatchFailure::new(
        BatchItemId(target.0.clone()),
        super::error::cross_account_destination(
            AccountOperation::BulkMove,
            &target.0,
            target_owner,
            destination.as_str(),
            destination_owner,
        ),
    )))
}

/// Select the account that owns a bulk target. An object qualified for an
/// account no longer available in this session deliberately keeps the primary
/// route, so the server returns its normal not-found result rather than the
/// client fabricating a local routing error.
fn account_id_for_target<T: HttpTransport>(
    target: &ObjectId,
    primary_account_id: &str,
    foreign_mail: &HashMap<String, MailAccount<T>>,
) -> String {
    super::foreign::parse_object(&target.0)
        .filter(|(account_id, _)| foreign_mail.contains_key(*account_id))
        .map_or_else(
            || primary_account_id.to_string(),
            |(account_id, _)| account_id.to_string(),
        )
}

fn mail_for_account<'a, T: HttpTransport>(
    primary: &'a MailAccount<T>,
    foreign_mail: &'a HashMap<String, MailAccount<T>>,
    account_id: &str,
) -> &'a MailAccount<T> {
    foreign_mail.get(account_id).unwrap_or(primary)
}

async fn apply_batch<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    foreign_account: bool,
    kind: &MutationKind,
    batch: &mut Vec<ObjectId>,
) -> crate::Result<Option<Batch<ItemOutcome<MutationSuccess>>>> {
    let started = Instant::now();
    let ids = std::mem::take(batch);
    if ids.is_empty() {
        // A route can hold only locally-rejected targets. Probing Email
        // state for it would spend a round trip to send nothing.
        return Ok(None);
    }
    if matches!(kind, MutationKind::SkipFlags) {
        return Ok(Some(skipped_batch(ids)));
    }
    let mut state = current_or_probe_state(mail, email_states, account_id).await?;

    let mut response = match send_set(mail, &state, &ids, kind, foreign_account).await {
        Ok(response) => response,
        Err(err) => {
            if !super::error::is_state_mismatch(&err) {
                return Err(err);
            }
            let fresh = probe_email_state(mail).await?;
            state_cache::set(email_states, account_id, fresh.clone()).await;
            state = fresh;
            match send_set(mail, &state, &ids, kind, foreign_account).await {
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
        let email_id = wire_email_id(&id, account_id, foreign_account);
        let raw = match kind {
            MutationKind::Destroy => response.destroyed(&email_id),
            MutationKind::Flags(_) | MutationKind::Move(_) => {
                response.updated(&email_id).map(|_| ())
            }
            MutationKind::SkipFlags => unreachable!("skip batches never reach Email/set"),
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

fn skipped_batch(ids: Vec<ObjectId>) -> Batch<ItemOutcome<MutationSuccess>> {
    Batch {
        items: ids
            .into_iter()
            .map(|id| {
                ItemOutcome::Succeeded(BatchSuccess::new(
                    BatchItemId(id.0),
                    MutationSuccess::Skipped,
                ))
            })
            .collect(),
        page_boundary: PageBoundary::Page,
        server_latency: std::time::Duration::ZERO,
        bytes_in: 0,
        checkpoint: None,
    }
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
            let item_scope = Some(ErrorScope::Message {
                id: (id.0.clone()).into(),
            });
            super::error::classify_set_item(set_error, ctx, BatchItemId(id.0.clone()), item_scope)
        }
        Err(crate::Error::IdNotFound(_)) => ItemOutcome::Failed(BatchFailure::new(
            BatchItemId(id.0.clone()),
            super::error::set_id_unanswered(
                &id.0,
                super::error::JmapErrorContext::new(operation).with_scope(ErrorScope::Message {
                    id: (id.0.clone()).into(),
                }),
            ),
        )),
        Err(err) => ItemOutcome::Failed(BatchFailure::new(
            BatchItemId(id.0.clone()),
            super::error::into_account_error(
                err,
                super::error::JmapErrorContext::new(operation).with_scope(ErrorScope::Message {
                    id: (id.0.clone()).into(),
                }),
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
            super::error::JmapErrorContext::new(operation).with_scope(ErrorScope::Message {
                id: (id.0.clone()).into(),
            }),
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
    foreign_account: bool,
) -> crate::Result<crate::core::set::SetResponse<crate::email::Email>> {
    let mut set = EmailSet::new().if_in_state(state.to_string());

    match kind {
        MutationKind::SkipFlags => unreachable!("skip batches never reach Email/set"),
        MutationKind::Destroy => {
            set = set.destroy(
                ids.iter()
                    .map(|id| wire_email_id(id, mail.id_str(), foreign_account)),
            );
        }
        MutationKind::Flags(op) => {
            for id in ids {
                apply_flags(
                    set.update(wire_email_id(id, mail.id_str(), foreign_account)),
                    op,
                );
            }
        }
        MutationKind::Move(mailbox_id) => {
            for id in ids {
                set.update(wire_email_id(id, mail.id_str(), foreign_account))
                    .mailbox_ids([wire_mailbox_id(mailbox_id, mail.id_str(), foreign_account)]);
            }
        }
    }

    mail.call(set).await
}

/// Convert a routed foreign object back to the native id JMAP accepts on the
/// selected account. Keep an id for an unregistered foreign account intact on
/// the primary route, preserving the real server-side not-found behavior.
fn wire_email_id(id: &ObjectId, account_id: &str, foreign_account: bool) -> EmailId {
    match super::foreign::parse_object(&id.0) {
        Some((owner, native)) if foreign_account && owner == account_id => EmailId::new(native),
        _ => EmailId::new(id.0.clone()),
    }
}

/// The native form of the move destination for the routed account.
/// `destination_rejection` has already established that the destination and
/// every target in this batch name the same owner, so the only work left is
/// stripping the qualification the foreign account does not use.
fn wire_mailbox_id(id: &MailboxId, account_id: &str, foreign_account: bool) -> MailboxId {
    match super::foreign::parse_object(id.as_str()) {
        Some((owner, native)) if foreign_account && owner == account_id => MailboxId::new(native),
        _ => id.clone(),
    }
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
            MutationKind::SkipFlags => unreachable!("skip batches do not build Email/set"),
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
        let batch = skipped_batch(vec![ObjectId("m1".into()), ObjectId("m2".into())]);

        assert_eq!(batch.items.len(), 2);
        for (outcome, expected) in batch.items.iter().zip(["m1", "m2"]) {
            let ItemOutcome::Succeeded(success) = outcome else {
                panic!("expected skipped success, got {outcome:?}");
            };
            assert_eq!(success.item.0, expected);
            assert_eq!(success.output, MutationSuccess::Skipped);
        }
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

    /// Route membership is per OWNER, not per input target. Draining used
    /// to remove the map entry, so the next target for the same owner
    /// re-registered it and appended another `String`; at
    /// `maxObjectsInSet = 1` that is one allocation retained per input item
    /// for the whole run.
    #[test]
    fn a_repeatedly_drained_route_is_registered_exactly_once() {
        let mut buffers = RouteBuffers::new(1);
        for n in 0..64 {
            let due = buffers.push("acct-9".to_string(), ObjectId(format!("m{n}")), None);
            assert_eq!(due, vec!["acct-9".to_string()]);
            assert!(buffers.drain("acct-9").is_some());
        }
        assert_eq!(buffers.remaining(), vec!["acct-9".to_string()]);
    }

    /// A short batch cannot be held hostage by another owner's traffic. The
    /// quiet owner is flushed once `batch_size` further targets have gone
    /// elsewhere - the same wait a full batch already accepts - instead of
    /// waiting for the input stream to end.
    #[test]
    fn a_partial_batch_flushes_once_a_full_batch_of_input_has_passed_it() {
        let mut buffers = RouteBuffers::new(4);
        assert!(
            buffers
                .push("quiet".to_string(), ObjectId("q1".to_string()), None)
                .is_empty()
        );
        for n in 0..3 {
            assert!(
                buffers
                    .push("busy".to_string(), ObjectId(format!("b{n}")), None)
                    .is_empty(),
                "nothing is due while both routes are short and freshly fed"
            );
        }

        // The fourth `busy` target both fills that route and ages `quiet`
        // by a full batch worth of input.
        let due = buffers.push("busy".to_string(), ObjectId("b3".to_string()), None);
        assert_eq!(due, vec!["quiet".to_string(), "busy".to_string()]);
        assert_eq!(
            buffers.drain("quiet").expect("quiet route").0,
            vec![ObjectId("q1".to_string())]
        );
    }

    /// An emptied route is never re-flushed by the idle sweep, and the
    /// end-of-input drain skips it too.
    #[test]
    fn an_emptied_route_is_not_flushed_again() {
        let mut buffers = RouteBuffers::new(1);
        buffers.push("acct-9".to_string(), ObjectId("m1".to_string()), None);
        assert!(buffers.drain("acct-9").is_some());
        let due = buffers.push("primary".to_string(), ObjectId("m2".to_string()), None);
        assert_eq!(due, vec!["primary".to_string()]);
        assert!(buffers.drain("acct-9").is_none());
    }

    /// A locally-rejected target still occupies a slot in its owner's
    /// batch, so a stream of nothing but rejections cannot accumulate
    /// without bound waiting for a wire flush that never comes.
    #[test]
    fn a_rejected_target_counts_toward_its_owners_batch() {
        let rejected = ItemOutcome::Failed(BatchFailure::new(
            BatchItemId("m1".to_string()),
            super::super::error::cross_account_destination(
                AccountOperation::BulkMove,
                "m1",
                None,
                "acct-9\u{1f}mbx",
                Some("acct-9"),
            ),
        ));
        let mut buffers = RouteBuffers::new(1);
        let due = buffers.push(
            "primary".to_string(),
            ObjectId("m1".to_string()),
            Some(rejected),
        );
        assert_eq!(due, vec!["primary".to_string()]);
        let (wire, rejected) = buffers.drain("primary").expect("primary route");
        assert!(wire.is_empty(), "a rejected target never reaches the wire");
        assert_eq!(rejected.len(), 1);
    }

    /// The owner comparison behind the wire-level rejection, enumerated.
    /// Both operands bare, or both qualified with the same account, is the
    /// only expressible pairing.
    #[test]
    fn a_move_destination_must_name_the_targets_own_account() {
        let foreign_target = ObjectId(super::super::foreign::encode_object("acct-9", "M1"));
        let primary_target = ObjectId("m1".to_string());
        let foreign_box = |account: &str| {
            MutationKind::Move(MailboxId::new(
                super::super::foreign::encode_foreign(account, "mbx").0,
            ))
        };
        let primary_box = MutationKind::Move(MailboxId::new("inbox"));

        assert!(destination_rejection(&primary_box, &primary_target).is_none());
        assert!(destination_rejection(&foreign_box("acct-9"), &foreign_target).is_none());

        // The silent one: a bare primary mailbox id would otherwise be
        // resolved inside acct-9's namespace.
        let rejected = destination_rejection(&primary_box, &foreign_target)
            .expect("a primary destination cannot receive a foreign message");
        let ItemOutcome::Failed(failure) = rejected else {
            panic!("a cross-account move must land on the item Failed lane");
        };
        assert_eq!(failure.item.0, foreign_target.0);
        assert_eq!(
            failure.error.kind(),
            &AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
        assert!(matches!(failure.error.recovery(), RecoveryClass::ClientBug));

        assert!(destination_rejection(&foreign_box("acct-9"), &primary_target).is_some());
        assert!(destination_rejection(&foreign_box("acct-7"), &foreign_target).is_some());

        // Kinds with no destination never reject.
        assert!(destination_rejection(&MutationKind::Destroy, &foreign_target).is_none());
        assert!(
            destination_rejection(
                &MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
                &foreign_target,
            )
            .is_none()
        );
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
