use std::collections::{BTreeMap, HashMap, HashSet};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, AccountStream,
    BatchFailure, BatchItemId, BatchSuccess, BatchUncertain, Cause, DiagnosticText, FlagOp,
    IdempotencyKey, ItemOutcome, MembershipScope, MutationSuccess, PageBoundary, Protocol,
    RequestCause, RequestErrorKind, StateCause, SyncEvent,
};
use futures::StreamExt;

use crate::types::{Flag, MailboxName, ResponseCode, StoreOperation};

use super::{
    DecodedObjectId, ImapAccount, batch, boxed_receiver_stream, decode_object_id, uid_set_from_u32,
};

pub(crate) fn bulk_set_flags(
    account: ImapAccount,
    targets: AccountStream<bifrost_types::ObjectId>,
    op: FlagOp,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(account, targets, MutationKind::Flags(op))
}

pub(crate) fn bulk_move(
    account: ImapAccount,
    targets: AccountStream<bifrost_types::ObjectId>,
    destination: MembershipScope,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(account, targets, MutationKind::Move(destination))
}

pub(crate) fn bulk_destroy(
    account: ImapAccount,
    targets: AccountStream<bifrost_types::ObjectId>,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(account, targets, MutationKind::Destroy)
}

pub(super) enum MutationKind {
    Flags(FlagOp),
    Move(MembershipScope),
    Destroy,
}

/// True when a folder-level mutation failure must collapse the entire
/// stream rather than surfacing per-item `Uncertain` for the failing
/// folder. Reserved for failures that prevent any further folder
/// attempt: authentication / authorization loss, schema / capability
/// breaks. Per-folder transient failures (transport, rate limit, mailbox
/// unavailable) emit per-item `Uncertain` and continue.
fn stream_terminating(err: &AccountError) -> bool {
    matches!(
        err.kind(),
        AccountErrorKind::Authentication(_)
            | AccountErrorKind::Authorization(_)
            | AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::SchemaIncompatible
                    | bifrost_types::SyncStateErrorKind::CapabilityChanged
            )
    )
}

fn mutation_operation(kind: &MutationKind) -> AccountOperation {
    match kind {
        MutationKind::Flags(_) => AccountOperation::UpdateFlags,
        MutationKind::Move(_) => AccountOperation::BulkMove,
        MutationKind::Destroy => AccountOperation::BulkDestroy,
    }
}

pub(super) fn mutation_stream(
    account: ImapAccount,
    mut targets: AccountStream<bifrost_types::ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    tokio::spawn(async move {
        let move_destination = match validated_move_destination(&kind) {
            Ok(destination) => destination,
            Err(error) => {
                while let Some(id) = targets.next().await {
                    let item = BatchItemId(id.0);
                    if tx
                        .send(batch(
                            vec![ItemOutcome::Failed(BatchFailure::new(item, error.clone()))],
                            PageBoundary::Page,
                            None,
                        ))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let _ = tx.send(SyncEvent::Done(None)).await;
                return;
            }
        };
        let mut grouped: HashMap<String, (MailboxName, Vec<DecodedObjectId>)> = HashMap::new();
        let mut buffered = 0usize;
        while let Some(id) = targets.next().await {
            match decode_object_id(&id) {
                Ok(decoded) => {
                    grouped
                        .entry(decoded.folder.as_str().to_owned())
                        .or_insert_with(|| (decoded.folder.clone(), Vec::new()))
                        .1
                        .push(decoded);
                    buffered += 1;
                }
                Err(err) => {
                    let item_id = BatchItemId(id.0.clone());
                    if tx
                        .send(batch(
                            vec![ItemOutcome::Failed(BatchFailure::new(item_id, err))],
                            PageBoundary::Page,
                            None,
                        ))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            if buffered >= super::TARGET_BUFFER_ITEMS {
                if flush_mutation_groups(
                    &account,
                    &mut grouped,
                    &kind,
                    move_destination.as_ref(),
                    &tx,
                )
                .await
                .is_err()
                {
                    return;
                }
                buffered = 0;
            }
        }
        if flush_mutation_groups(
            &account,
            &mut grouped,
            &kind,
            move_destination.as_ref(),
            &tx,
        )
        .await
        .is_err()
        {
            return;
        }
        let _ = tx.send(SyncEvent::Done(None)).await;
    });
    boxed_receiver_stream(rx)
}

async fn flush_mutation_groups(
    account: &ImapAccount,
    grouped: &mut HashMap<String, (MailboxName, Vec<DecodedObjectId>)>,
    kind: &MutationKind,
    move_destination: Option<&MailboxName>,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<ItemOutcome<MutationSuccess>>>,
) -> Result<(), ()> {
    // Per-folder failure semantics:
    // - A folder error after per-item emissions in any prior folder
    //   must NOT collapse those items by emitting a global
    //   `SyncEvent::Terminated`. The failing folder's items surface
    //   as `ItemOutcome::Uncertain` carrying the classified error
    //   (the engine cannot tell, post-hoc, whether the mutation
    //   landed).
    // - Stream-level `Terminated` is reserved for failures that
    //   prevent any further folder attempts at all (auth lost,
    //   schema break, capability shift). Those classify as
    //   terminal or as an engine directive; we surface them with
    //   `Terminated` and stop.
    // - Anything else (transient transport, rate limit, per-folder
    //   server error) emits per-item `Uncertain` for the failing
    //   folder and continues to the next folder.
    for (folder, ids) in sorted_mutation_groups(std::mem::take(grouped)) {
        match run_folder_mutation(account, &folder, ids.clone(), kind, move_destination).await {
            Ok(results) => {
                if tx
                    .send(batch(results, PageBoundary::Page, None))
                    .await
                    .is_err()
                {
                    return Err(());
                }
            }
            Err(err) => {
                let account_err = super::account_error_with(
                    err,
                    super::error::ImapErrorContext::operation(mutation_operation(kind))
                        .with_folder_scope(&folder),
                );
                if stream_terminating(&account_err) {
                    let _ = tx.send(SyncEvent::Terminated(account_err)).await;
                    return Err(());
                }
                let uncertain = ids
                    .into_iter()
                    .map(|id| {
                        let item = BatchItemId(
                            super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0,
                        );
                        ItemOutcome::Uncertain(BatchUncertain::new(item, account_err.clone()))
                    })
                    .collect();
                if tx
                    .send(batch(uncertain, PageBoundary::Page, None))
                    .await
                    .is_err()
                {
                    return Err(());
                }
            }
        }
    }
    Ok(())
}

fn sorted_mutation_groups(
    grouped: HashMap<String, (MailboxName, Vec<DecodedObjectId>)>,
) -> Vec<(MailboxName, Vec<DecodedObjectId>)> {
    let mut grouped: Vec<_> = grouped.into_values().collect();
    grouped.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
    grouped
}

/// Validate a bulk-move destination before opening any source folder. This
/// is our own request validation, so every submitted target must fail rather
/// than being labelled uncertain by the per-folder error path.
fn validated_move_destination(kind: &MutationKind) -> Result<Option<MailboxName>, AccountError> {
    let MutationKind::Move(destination) = kind else {
        return Ok(None);
    };
    let MembershipScope::Folder(destination) = destination else {
        return Err(invalid_move_destination_error());
    };
    MailboxName::new(destination.0.clone())
        .map(Some)
        .map_err(|_| invalid_move_destination_error())
}

async fn run_folder_mutation(
    account: &ImapAccount,
    folder: &MailboxName,
    ids: Vec<DecodedObjectId>,
    kind: &MutationKind,
    move_destination: Option<&MailboxName>,
) -> Result<Vec<ItemOutcome<MutationSuccess>>, crate::Error> {
    let mut conn = account.checkout_for_folder(folder).await?;
    let cursor = account.folders.get(folder).and_then(|entry| entry.cursor());
    let selected = account
        .select_folder(&mut conn, folder, cursor.as_ref(), false)
        .await?;
    let uidvalidity = selected
        .mailbox
        .uid_validity
        .ok_or_else(|| crate::Error::Protocol("SELECT missing UIDVALIDITY".into()))?;
    for range in selected.mailbox.vanished.clone() {
        let uids = super::folder_registry::expand_range(range);
        account.folders.clear_modseqs(folder, uidvalidity, &uids);
    }
    for fetch in &selected.mailbox.changed_messages {
        if let (Some(uid), Some(modseq)) = (fetch.uid, fetch.mod_seq) {
            account
                .folders
                .record_modseq(folder, uidvalidity, uid, modseq)?;
        }
    }
    let (valid, stale) = split_by_uidvalidity(ids, uidvalidity);
    let stale_results = failed_all(
        stale,
        uidvalidity_changed_error(mutation_operation(kind), folder),
    );
    if let MutationKind::Flags(op) = kind {
        let mut results = stale_results;
        results.extend(
            run_flag_mutation_groups(account, &conn, folder, uidvalidity, valid, op).await?,
        );
        return Ok(results);
    }
    if matches!(kind, MutationKind::Destroy) {
        let mut results = stale_results;
        results
            .extend(run_destroy_mutation_groups(account, &conn, folder, uidvalidity, valid).await?);
        return Ok(results);
    }
    let uids: Vec<u32> = valid.iter().map(|id| id.uid).collect();
    let Some(uid_set) = uid_set_from_u32(&uids) else {
        return Ok(stale_results);
    };

    let outcome = match kind {
        MutationKind::Flags(_) => unreachable!("flags handled above"),
        MutationKind::Move(_) => {
            let destination =
                move_destination.expect("bulk-move destination validated before folder loop");
            conn.connection()
                .uid_move_messages(
                    uid_set.as_sequence_set(),
                    destination.as_str(),
                    account.command_timeout(),
                )
                .await
                .map(|_| StoreWireOutcome::Applied)
        }
        MutationKind::Destroy => unreachable!("destroy handled above"),
    };

    let mut results = stale_results;
    results.extend(match outcome {
        Ok(outcome) => {
            account.folders.clear_modseqs(folder, uidvalidity, &uids);
            mutation_results(valid, outcome, mutation_operation(kind), folder)
        }
        Err(err) => failed_all(
            valid,
            super::account_error_with(
                err,
                super::error::ImapErrorContext::operation(mutation_operation(kind))
                    .with_folder_scope(folder),
            ),
        ),
    });
    Ok(results)
}

async fn run_destroy_mutation_groups(
    account: &ImapAccount,
    conn: &super::PooledConn,
    folder: &MailboxName,
    uidvalidity: u32,
    ids: Vec<DecodedObjectId>,
) -> Result<Vec<ItemOutcome<MutationSuccess>>, crate::Error> {
    let mut results = Vec::new();
    for (unchanged_since, ids) in partition_by_modseq(account, folder, uidvalidity, ids) {
        let uids: Vec<u32> = ids.iter().map(|id| id.uid).collect();
        let Some(uid_set) = uid_set_from_u32(&uids) else {
            continue;
        };
        let store = conn
            .connection()
            .uid_store(
                uid_set.as_sequence_set(),
                StoreOperation::AddSilent,
                &[Flag::Deleted],
                unchanged_since,
                account.command_timeout(),
            )
            .await;
        let outcome = match store {
            Ok(result) => StoreWireOutcome::from_response_code(result.code.as_ref(), true),
            Err(err) => {
                results.extend(failed_all(
                    ids,
                    super::account_error_with(
                        err,
                        super::error::ImapErrorContext::operation(AccountOperation::BulkDestroy)
                            .with_folder_scope(folder),
                    ),
                ));
                continue;
            }
        };
        let expunge_uids = applied_uids_after_store(&uids, &outcome);
        account
            .folders
            .clear_modseqs(folder, uidvalidity, &expunge_uids);
        if let Some(expunge_set) = uid_set_from_u32(&expunge_uids)
            && let Err(err) = expunge_uids_or_fall_back(
                account,
                conn,
                expunge_set.as_sequence_set(),
                &expunge_uids,
            )
            .await
        {
            let (expunging_ids, remaining_ids) = split_ids_by_uid(ids, &expunge_uids);
            results.extend(failed_all(
                expunging_ids,
                expunge_failed_after_delete_mark(folder, err),
            ));
            results.extend(mutation_results(
                remaining_ids,
                outcome,
                AccountOperation::BulkDestroy,
                folder,
            ));
            continue;
        }
        results.extend(mutation_results(
            ids,
            outcome,
            AccountOperation::BulkDestroy,
            folder,
        ));
    }
    Ok(results)
}

async fn run_flag_mutation_groups(
    account: &ImapAccount,
    conn: &super::PooledConn,
    folder: &MailboxName,
    uidvalidity: u32,
    ids: Vec<DecodedObjectId>,
    op: &FlagOp,
) -> Result<Vec<ItemOutcome<MutationSuccess>>, crate::Error> {
    let mut results = Vec::new();
    for (unchanged_since, ids) in partition_by_modseq(account, folder, uidvalidity, ids) {
        let uids: Vec<u32> = ids.iter().map(|id| id.uid).collect();
        let Some(uid_set) = uid_set_from_u32(&uids) else {
            continue;
        };
        if let FlagOp::Patch { add, remove } = op
            && !add.is_empty()
            && !remove.is_empty()
        {
            results.extend(
                run_patch_mutation_group(
                    account,
                    conn,
                    folder,
                    uidvalidity,
                    PatchMutationWork {
                        ids,
                        uids: &uids,
                        add,
                        remove,
                        unchanged_since,
                    },
                )
                .await,
            );
            continue;
        }
        let outcome = apply_flag_op(
            conn,
            uid_set.as_sequence_set(),
            op,
            unchanged_since,
            account.command_timeout(),
        )
        .await;
        results.extend(match outcome {
            Ok(outcome) => {
                let changed_uids = applied_uids_after_store(&uids, &outcome);
                account
                    .folders
                    .clear_modseqs(folder, uidvalidity, &changed_uids);
                mutation_results(ids, outcome, AccountOperation::UpdateFlags, folder)
            }
            Err(err) => {
                if matches!(op, FlagOp::Patch { .. }) {
                    account.folders.clear_modseqs(folder, uidvalidity, &uids);
                }
                failed_all(
                    ids,
                    super::account_error_with(
                        err,
                        super::error::ImapErrorContext::operation(AccountOperation::UpdateFlags)
                            .with_folder_scope(folder),
                    ),
                )
            }
        });
    }
    Ok(results)
}

/// Apply the two halves of a flag patch with exact per-item accounting.
/// A guarded add can apply to the non-conflicting subset, so its successful
/// UIDs must receive the unguarded remove before they can be reported as
/// `Succeeded(Applied)`.
struct PatchMutationWork<'a> {
    ids: Vec<DecodedObjectId>,
    uids: &'a [u32],
    add: &'a HashSet<String>,
    remove: &'a HashSet<String>,
    unchanged_since: Option<u64>,
}

async fn run_patch_mutation_group(
    account: &ImapAccount,
    conn: &super::PooledConn,
    folder: &MailboxName,
    uidvalidity: u32,
    work: PatchMutationWork<'_>,
) -> Vec<ItemOutcome<MutationSuccess>> {
    let set = uid_set_from_u32(work.uids).expect("non-empty UID list has a sequence set");
    let first = match store_flags(
        conn,
        set.as_sequence_set(),
        StoreOperation::AddSilent,
        work.add,
        work.unchanged_since,
        account.command_timeout(),
    )
    .await
    {
        Ok(first) => first,
        // A failure of the first STORE decides only this MODSEQ group. It
        // must not escape the group loop: outcomes already established for
        // earlier groups are known truth and may never be downgraded to the
        // folder-wide uncertain lane.
        Err(err) => {
            account
                .folders
                .clear_modseqs(folder, uidvalidity, work.uids);
            return patch_first_store_failure(work.ids, err, folder);
        }
    };
    let applied = applied_uids_after_store(work.uids, &first);
    account.folders.clear_modseqs(folder, uidvalidity, &applied);

    let second = match uid_set_from_u32(&applied) {
        Some(set) => store_flags(
            conn,
            set.as_sequence_set(),
            StoreOperation::RemoveSilent,
            work.remove,
            None,
            account.command_timeout(),
        )
        .await
        .map_err(|err| {
            super::account_error_with(
                err,
                super::error::ImapErrorContext::operation(AccountOperation::UpdateFlags)
                    .with_folder_scope(folder),
            )
        }),
        None => Ok(StoreWireOutcome::Applied),
    };
    patch_mutation_results(work.ids, work.uids, first, second, folder)
}

/// Per-item outcomes for a MODSEQ group whose guarded add STORE errored on
/// the wire. Scoped to the group's own ids: the surrounding loop keeps the
/// outcomes it has already established for other groups, matching the
/// single-outcome-per-id contract the sync engine relies on.
fn patch_first_store_failure(
    ids: Vec<DecodedObjectId>,
    err: crate::Error,
    folder: &MailboxName,
) -> Vec<ItemOutcome<MutationSuccess>> {
    failed_all(
        ids,
        super::account_error_with(
            err,
            super::error::ImapErrorContext::operation(AccountOperation::UpdateFlags)
                .with_folder_scope(folder),
        ),
    )
}

fn patch_mutation_results(
    ids: Vec<DecodedObjectId>,
    requested_uids: &[u32],
    first: StoreWireOutcome,
    second: Result<StoreWireOutcome, AccountError>,
    folder: &MailboxName,
) -> Vec<ItemOutcome<MutationSuccess>> {
    let applied = applied_uids_after_store(requested_uids, &first);
    let (applied_ids, conflicting_ids) = split_ids_by_uid(ids, &applied);
    let mut results = match first {
        StoreWireOutcome::Applied => Vec::new(),
        StoreWireOutcome::Modified(modified) => mutation_results(
            conflicting_ids,
            StoreWireOutcome::Modified(modified),
            AccountOperation::UpdateFlags,
            folder,
        ),
        outcome @ (StoreWireOutcome::PendingRetry(_) | StoreWireOutcome::Failed) => {
            return mutation_results(
                ids_from_parts(applied_ids, conflicting_ids),
                outcome,
                AccountOperation::UpdateFlags,
                folder,
            );
        }
    };

    match second {
        Ok(StoreWireOutcome::Applied) => results.extend(mutation_results(
            applied_ids,
            StoreWireOutcome::Applied,
            AccountOperation::UpdateFlags,
            folder,
        )),
        Ok(_) => results.extend(uncertain_all(
            applied_ids,
            store_failed_error(AccountOperation::UpdateFlags, folder),
        )),
        Err(error) => results.extend(uncertain_all(applied_ids, error)),
    }
    results
}

fn ids_from_parts(
    mut matching: Vec<DecodedObjectId>,
    mut remaining: Vec<DecodedObjectId>,
) -> Vec<DecodedObjectId> {
    matching.append(&mut remaining);
    matching
}

fn applied_uids_after_store(requested_uids: &[u32], outcome: &StoreWireOutcome) -> Vec<u32> {
    match outcome {
        StoreWireOutcome::Applied => requested_uids.to_vec(),
        StoreWireOutcome::Modified(modified) => requested_uids
            .iter()
            .copied()
            .filter(|uid| !modified.contains(uid))
            .collect(),
        StoreWireOutcome::PendingRetry(_) | StoreWireOutcome::Failed => Vec::new(),
    }
}

fn partition_by_modseq(
    account: &ImapAccount,
    folder: &MailboxName,
    uidvalidity: u32,
    ids: Vec<DecodedObjectId>,
) -> Vec<(Option<u64>, Vec<DecodedObjectId>)> {
    let mut protected: BTreeMap<u64, Vec<DecodedObjectId>> = BTreeMap::new();
    let mut unprotected = Vec::new();
    for id in ids {
        if let Some(modseq) = account.folders.modseq(folder, uidvalidity, id.uid) {
            protected.entry(modseq).or_default().push(id);
        } else {
            unprotected.push(id);
        }
    }
    let mut groups = protected
        .into_iter()
        .map(|(modseq, ids)| (Some(modseq), ids))
        .collect::<Vec<_>>();
    if !unprotected.is_empty() {
        groups.push((None, unprotected));
    }
    groups
}

async fn apply_flag_op(
    conn: &super::PooledConn,
    set: &crate::types::SequenceSet,
    op: &FlagOp,
    unchanged_since: Option<u64>,
    timeout: std::time::Duration,
) -> Result<StoreWireOutcome, crate::Error> {
    match op {
        FlagOp::Add(flags) => {
            store_flags(
                conn,
                set,
                StoreOperation::AddSilent,
                flags,
                unchanged_since,
                timeout,
            )
            .await
        }
        FlagOp::Remove(flags) => {
            store_flags(
                conn,
                set,
                StoreOperation::RemoveSilent,
                flags,
                unchanged_since,
                timeout,
            )
            .await
        }
        FlagOp::Set(flags) => {
            store_flags(
                conn,
                set,
                StoreOperation::ReplaceSilent,
                flags,
                unchanged_since,
                timeout,
            )
            .await
        }
        FlagOp::Patch { add, remove } if add.is_empty() && remove.is_empty() => {
            Ok(StoreWireOutcome::Applied)
        }
        FlagOp::Patch { add, remove } if add.is_empty() => {
            store_flags(
                conn,
                set,
                StoreOperation::RemoveSilent,
                remove,
                unchanged_since,
                timeout,
            )
            .await
        }
        FlagOp::Patch { add, remove } if remove.is_empty() => {
            store_flags(
                conn,
                set,
                StoreOperation::AddSilent,
                add,
                unchanged_since,
                timeout,
            )
            .await
        }
        FlagOp::Patch { .. } => {
            unreachable!("two-sided patches are handled with per-UID accounting before this call")
        }
        _ => Err(crate::Error::Protocol("unsupported flag operation".into())),
    }
}

async fn store_flags(
    conn: &super::PooledConn,
    set: &crate::types::SequenceSet,
    operation: StoreOperation,
    flags: &HashSet<String>,
    unchanged_since: Option<u64>,
    timeout: std::time::Duration,
) -> Result<StoreWireOutcome, crate::Error> {
    let flags = flags
        .iter()
        .map(|flag| Flag::from(flag.as_str()))
        .collect::<Vec<_>>();
    let result = conn
        .connection()
        .uid_store(set, operation, &flags, unchanged_since, timeout)
        .await?;
    Ok(StoreWireOutcome::from_response_code(
        result.code.as_ref(),
        true,
    ))
}

// protocol-specific: IMAP STORE can return MODIFIED before it maps to shared MutationOutcome.
#[derive(Debug, Clone, PartialEq, Eq)]
enum StoreWireOutcome {
    Applied,
    Modified(Vec<u32>),
    PendingRetry(Vec<u32>),
    Failed,
}

impl StoreWireOutcome {
    fn from_response_code(code: Option<&ResponseCode>, tagged_ok: bool) -> Self {
        match (tagged_ok, modified_uids(code)) {
            (true, None) => Self::Applied,
            (true, Some(modified)) => Self::Modified(modified),
            (false, Some(modified)) => Self::PendingRetry(modified),
            (false, None) => Self::Failed,
        }
    }
}

fn modified_uids(code: Option<&ResponseCode>) -> Option<Vec<u32>> {
    match code {
        Some(ResponseCode::Modified(ranges)) => Some(
            ranges
                .iter()
                .copied()
                .flat_map(super::folder_registry::expand_range)
                .collect(),
        ),
        _ => None,
    }
}

fn mutation_results(
    ids: Vec<DecodedObjectId>,
    outcome: StoreWireOutcome,
    operation: AccountOperation,
    folder: &MailboxName,
) -> Vec<ItemOutcome<MutationSuccess>> {
    match outcome {
        StoreWireOutcome::Applied => ids
            .into_iter()
            .map(|id| {
                let item =
                    BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0);
                ItemOutcome::Succeeded(BatchSuccess::new(item, MutationSuccess::Applied))
            })
            .collect(),
        StoreWireOutcome::Modified(modified) => ids
            .into_iter()
            .map(|id| {
                let item =
                    BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0);
                if modified.contains(&id.uid) {
                    ItemOutcome::Failed(BatchFailure::new(
                        item,
                        concurrency_conflict_error(operation, folder),
                    ))
                } else {
                    ItemOutcome::Succeeded(BatchSuccess::new(item, MutationSuccess::Applied))
                }
            })
            .collect(),
        // Tagged-NO STORE carrying `[MODIFIED ...]`: the server
        // explicitly rejected the command and named the conflicting
        // UIDs. This is a *server-acknowledged* conflict - a complete
        // response crossed the wire, so there is no transmission
        // ambiguity. Per the error model the conflicting UIDs are
        // therefore `Failed(ConcurrencyConflict)` (deriving
        // `Retry::AfterStateRefresh`), NOT `Uncertain` (which is the
        // analogue of an in-flight transport drop and queues for
        // read-back). The remaining UIDs were not committed because the
        // whole STORE was rejected.
        StoreWireOutcome::PendingRetry(modified) => ids
            .into_iter()
            .map(|id| {
                let item =
                    BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0);
                if modified.contains(&id.uid) {
                    ItemOutcome::Failed(BatchFailure::new(
                        item,
                        concurrency_conflict_error(operation, folder),
                    ))
                } else {
                    ItemOutcome::Failed(BatchFailure::new(
                        item,
                        store_failed_error(operation, folder),
                    ))
                }
            })
            .collect(),
        StoreWireOutcome::Failed => failed_all(ids, store_failed_error(operation, folder)),
    }
}

fn failed_all(ids: Vec<DecodedObjectId>, error: AccountError) -> Vec<ItemOutcome<MutationSuccess>> {
    ids.into_iter()
        .map(|id| {
            let item = BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0);
            ItemOutcome::Failed(BatchFailure::new(item, error.clone()))
        })
        .collect()
}

fn uncertain_all(
    ids: Vec<DecodedObjectId>,
    error: AccountError,
) -> Vec<ItemOutcome<MutationSuccess>> {
    ids.into_iter()
        .map(|id| {
            let item = BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0);
            ItemOutcome::Uncertain(BatchUncertain::new(item, error.clone()))
        })
        .collect()
}

fn invalid_move_destination_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("bulk-move destination is not a sendable mailbox"),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::BulkMove)
    .try_build()
    .expect("valid account error classification")
}

/// Build a `ConcurrencyConflict` `AccountError` for STORE UNCHANGEDSINCE
/// conflicts. Takes the caller's `AccountOperation` so flag, move, and
/// destroy paths each surface their own operation - the central recovery
/// mapping needs the correct op to pick `Retry::AfterStateRefresh` for
/// the right kind of work.
fn concurrency_conflict_error(operation: AccountOperation, folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::ConcurrencyConflict,
        Cause::State(StateCause::ConcurrencyConflict),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Mailbox {
        id: folder.as_str().to_owned(),
    })
    .try_build()
    .expect("valid account error classification")
}

/// Build a generic `Request(Malformed)` for UIDVALIDITY mismatch before mutation.
fn uidvalidity_changed_error(operation: AccountOperation, folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("UIDVALIDITY changed before mutation"),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Mailbox {
        id: folder.as_str().to_owned(),
    })
    .try_build()
    .expect("valid account error classification")
}

/// Build a generic protocol error for a STORE command failure without a
/// specific per-item response code. Takes the caller's `AccountOperation`
/// so move / destroy / flag mutations each carry their own op.
fn store_failed_error(operation: AccountOperation, folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(
                "STORE command failed with no per-item response code",
            ),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Mailbox {
        id: folder.as_str().to_owned(),
    })
    .try_build()
    .expect("valid account error classification")
}

/// Remove the just-marked messages, with a bounded fallback for servers
/// that have neither UIDPLUS nor IMAP4rev2.
///
/// `UID EXPUNGE` (RFC 4315 Section 2) is the only command that names the
/// messages to remove. Plain `EXPUNGE` (RFC 3501 Section 6.4.3) removes
/// *every* `\Deleted` message in the mailbox, including ones another
/// client marked and has not committed to removing - so it is only run
/// when `UID SEARCH DELETED` shows the mailbox's `\Deleted` set is
/// exactly the set this batch marked. When a foreign `\Deleted` message
/// is present the original `MissingCapability` is returned and the batch
/// fails with its messages left flagged, which is the honest outcome:
/// better a reported failure than silently expunging someone else's mail.
pub(super) async fn expunge_uids_or_fall_back(
    account: &ImapAccount,
    conn: &super::PooledConn,
    expunge_set: &crate::types::SequenceSet,
    expunge_uids: &[u32],
) -> Result<(), crate::Error> {
    let timeout = account.command_timeout();
    match conn.connection().uid_expunge(expunge_set, timeout).await {
        Ok(_) => return Ok(()),
        Err(crate::Error::MissingCapability(_)) => {}
        Err(err) => return Err(err),
    }
    let deleted = conn.connection().uid_search("DELETED", timeout).await?;
    let marked: std::collections::BTreeSet<u32> = expunge_uids.iter().copied().collect();
    let found: std::collections::BTreeSet<u32> = deleted.ids.into_iter().collect();
    if !found.is_subset(&marked) {
        return Err(crate::Error::MissingCapability("UIDPLUS".into()));
    }
    conn.connection().expunge(timeout).await?;
    Ok(())
}

/// The STORE half of IMAP deletion is already acknowledged before this
/// helper runs, so a failed UID EXPUNGE leaves a confirmed server-side
/// partial effect: those messages are still flagged `\Deleted`.
///
/// The failure keeps its own classification. Collapsing every EXPUNGE
/// error into `Protocol(PartialResponse)` would erase exactly the
/// distinctions `reference/error-model.md` asks the producer to preserve -
/// an auth loss, an ACL denial, a quota or rate-limit throttle, and a
/// capability loss each derive a different `RecoveryClass`, and none of
/// them is served by a generic reconcile. What this helper adds is the
/// side-effect evidence, as support-only diagnostic text riding on the
/// real error, so the engine can see both "why it failed" and "what
/// already landed".
fn expunge_failed_after_delete_mark(folder: &MailboxName, error: crate::Error) -> AccountError {
    super::account_error_with(
        error,
        super::error::ImapErrorContext::operation(AccountOperation::BulkDestroy)
            .with_folder_scope(folder)
            .with_extra_text(DiagnosticText::support_only(format!(
                "UID EXPUNGE failed for {} after UID STORE +FLAGS.SILENT \\Deleted was \
                 acknowledged; those messages remain flagged \\Deleted server-side",
                folder.as_str(),
            ))),
    )
}

fn split_by_uidvalidity(
    ids: Vec<DecodedObjectId>,
    uidvalidity: u32,
) -> (Vec<DecodedObjectId>, Vec<DecodedObjectId>) {
    let mut valid = Vec::new();
    let mut stale = Vec::new();
    for id in ids {
        if id.uidvalidity == uidvalidity {
            valid.push(id);
        } else {
            stale.push(id);
        }
    }
    (valid, stale)
}

fn split_ids_by_uid(
    ids: Vec<DecodedObjectId>,
    uids: &[u32],
) -> (Vec<DecodedObjectId>, Vec<DecodedObjectId>) {
    let uid_set: HashSet<u32> = uids.iter().copied().collect();
    let mut matching = Vec::new();
    let mut remaining = Vec::new();
    for id in ids {
        if uid_set.contains(&id.uid) {
            matching.push(id);
        } else {
            remaining.push(id);
        }
    }
    (matching, remaining)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::UidRange;

    #[test]
    fn flag_op_maps_to_wire_atoms() {
        let flags = HashSet::from(["\\Seen".to_owned(), "$Important".to_owned()]);
        let atoms = flags
            .iter()
            .map(|flag| Flag::from(flag.as_str()).as_imap_str().to_owned())
            .collect::<HashSet<_>>();
        assert!(atoms.contains("\\Seen"));
        assert!(atoms.contains("$Important"));
    }

    #[test]
    fn mutation_groups_are_processed_in_mailbox_order() {
        let mut grouped = HashMap::new();
        for name in ["Zeta", "Archive", "INBOX"] {
            let folder = MailboxName::new(name).expect("valid mailbox");
            grouped.insert(name.to_owned(), (folder, Vec::new()));
        }
        let names: Vec<_> = sorted_mutation_groups(grouped)
            .into_iter()
            .map(|(folder, _)| folder.as_str().to_owned())
            .collect();
        assert_eq!(names, ["Archive", "INBOX", "Zeta"]);
    }

    #[test]
    fn modified_code_maps_per_uid_outcomes() {
        let code = ResponseCode::Modified(vec![UidRange::range(2, 3)]);
        assert_eq!(
            StoreWireOutcome::from_response_code(Some(&code), true),
            StoreWireOutcome::Modified(vec![2, 3])
        );
        assert_eq!(
            StoreWireOutcome::from_response_code(Some(&code), false),
            StoreWireOutcome::PendingRetry(vec![2, 3])
        );
        assert_eq!(
            StoreWireOutcome::from_response_code(None, false),
            StoreWireOutcome::Failed
        );
    }

    #[test]
    fn pending_retry_conflict_is_failed_not_uncertain() {
        // A tagged-NO STORE carrying `[MODIFIED ...]` is a
        // server-acknowledged conflict with no transmission ambiguity:
        // the conflicting UIDs must surface as `Failed(ConcurrencyConflict)`
        // (driving Retry::AfterStateRefresh), never `Uncertain` (the
        // in-flight-drop analogue that queues for read-back).
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let ids = vec![
            DecodedObjectId {
                folder: folder.clone(),
                uidvalidity: 7,
                uid: 2,
            },
            DecodedObjectId {
                folder: folder.clone(),
                uidvalidity: 7,
                uid: 5,
            },
        ];
        let outcomes = mutation_results(
            ids,
            StoreWireOutcome::PendingRetry(vec![2]),
            AccountOperation::UpdateFlags,
            &folder,
        );
        // Every item is Failed - none Uncertain, none Succeeded.
        assert!(
            outcomes.iter().all(|o| matches!(o, ItemOutcome::Failed(_))),
            "PendingRetry conflict must surface as Failed, never Uncertain"
        );
        // The conflicting UID (2) carries ConcurrencyConflict.
        let has_conflict = outcomes.iter().any(|o| {
            matches!(o, ItemOutcome::Failed(f) if matches!(
                f.error.kind(),
                AccountErrorKind::ConcurrencyConflict
            ))
        });
        assert!(
            has_conflict,
            "conflicting UID must surface ConcurrencyConflict"
        );
    }

    #[test]
    fn applied_uids_after_store_excludes_modified_conflicts() {
        assert_eq!(
            applied_uids_after_store(&[1, 2, 3, 4], &StoreWireOutcome::Applied),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            applied_uids_after_store(&[1, 2, 3, 4], &StoreWireOutcome::Modified(vec![2, 4])),
            vec![1, 3]
        );
        assert!(
            applied_uids_after_store(&[1, 2, 3, 4], &StoreWireOutcome::PendingRetry(vec![2, 4]))
                .is_empty()
        );
        assert!(applied_uids_after_store(&[1], &StoreWireOutcome::Failed).is_empty());
    }

    #[test]
    fn concurrency_conflict_error_reports_callers_operation() {
        // concurrency_conflict_error / store_failed_error /
        // uidvalidity_changed_error must surface the caller's op so
        // recovery routes correctly for non-idempotent paths.
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        for op in [
            AccountOperation::UpdateFlags,
            AccountOperation::BulkMove,
            AccountOperation::BulkDestroy,
        ] {
            let err = concurrency_conflict_error(op, &folder);
            assert_eq!(err.operation(), Some(op));
            // Mailbox scope must thread too.
            assert!(matches!(
                err.scope(),
                Some(bifrost_types::ErrorScope::Mailbox { .. })
            ));
        }
    }

    #[test]
    fn store_failed_error_reports_callers_operation() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let err = store_failed_error(AccountOperation::BulkDestroy, &folder);
        assert_eq!(err.operation(), Some(AccountOperation::BulkDestroy));
        assert!(matches!(
            err.scope(),
            Some(bifrost_types::ErrorScope::Mailbox { .. })
        ));
    }

    /// The partial-effect note must ride on the real error, not replace it.
    /// An ACL denial and a quota exhaustion derive different recovery, and
    /// an engine that saw only `Protocol(PartialResponse)` would reconcile
    /// (or retry) both identically.
    #[test]
    fn expunge_failure_keeps_its_classification_and_adds_the_side_effect_note() {
        let denied = expunge_failed_after_delete_mark(
            &folder(),
            crate::Error::No {
                text: "permission denied".to_owned(),
                code: Some(crate::types::ResponseCode::NoPerm),
                attempt: None,
            },
        );
        assert!(
            matches!(
                denied.kind(),
                AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied)
            ),
            "ACL denial must not be laundered into a generic partial response: {:?}",
            denied.kind()
        );

        let over_quota = expunge_failed_after_delete_mark(
            &folder(),
            crate::Error::No {
                text: "over quota".to_owned(),
                code: Some(crate::types::ResponseCode::OverQuota),
                attempt: None,
            },
        );
        assert!(matches!(
            over_quota.kind(),
            AccountErrorKind::Server(bifrost_types::ServerErrorKind::QuotaExhausted)
        ));
        assert_ne!(
            denied.recovery(),
            over_quota.recovery(),
            "distinct failures must keep deriving distinct recovery classes"
        );

        for err in [&denied, &over_quota] {
            assert!(
                err.support_consented().support_text.iter().any(|text| {
                    text.contains("UID EXPUNGE failed")
                        && text.contains("remain flagged \\Deleted server-side")
                }),
                "the confirmed partial side effect must be attached"
            );
        }
    }

    #[test]
    fn uidvalidity_changed_error_reports_callers_operation() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let err = uidvalidity_changed_error(AccountOperation::BulkMove, &folder);
        assert_eq!(err.operation(), Some(AccountOperation::BulkMove));
    }

    #[test]
    fn stream_terminating_only_for_auth_and_schema() {
        use bifrost_types::{
            AccountErrorBuilder, AuthCause, AuthErrorKind, StateCause, SyncStateErrorKind,
        };
        let auth = AccountErrorBuilder::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("auth error");
        assert!(stream_terminating(&auth));

        let schema = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("schema error");
        assert!(stream_terminating(&schema));

        let transient = AccountErrorBuilder::new(
            AccountErrorKind::Server(bifrost_types::ServerErrorKind::Unavailable),
            Cause::Server(bifrost_types::ServerCause::Unavailable { retry_hint: None }),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("server error");
        assert!(!stream_terminating(&transient));
    }

    fn folder() -> MailboxName {
        MailboxName::new("INBOX").expect("valid mailbox")
    }

    fn ids(uids: &[u32]) -> Vec<DecodedObjectId> {
        uids.iter()
            .map(|uid| DecodedObjectId {
                folder: folder(),
                uidvalidity: 7,
                uid: *uid,
            })
            .collect()
    }

    #[test]
    fn only_the_modified_response_code_yields_conflicting_uids() {
        assert_eq!(modified_uids(None), None);
        assert_eq!(modified_uids(Some(&ResponseCode::Alert)), None);
        assert_eq!(
            modified_uids(Some(&ResponseCode::Modified(vec![
                UidRange::single(2),
                UidRange::range(5, 7),
            ]))),
            Some(vec![2, 5, 6, 7])
        );
        // An empty MODIFIED list is still a MODIFIED response: the STORE
        // outcome must not silently degrade to "no conflict reported".
        assert_eq!(
            modified_uids(Some(&ResponseCode::Modified(Vec::new()))),
            Some(Vec::new())
        );
    }

    // A tagged-OK STORE carrying `[MODIFIED ...]` partially applied: the
    // named UIDs conflicted, everything else in the same command landed.
    #[test]
    fn modified_outcome_splits_conflicts_from_successes_per_uid() {
        let outcomes = mutation_results(
            ids(&[1, 2, 3]),
            StoreWireOutcome::Modified(vec![2]),
            AccountOperation::UpdateFlags,
            &folder(),
        );
        assert_eq!(outcomes.len(), 3);
        let failed: Vec<&BatchFailure> = outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                ItemOutcome::Failed(failure) => Some(failure),
                _ => None,
            })
            .collect();
        assert_eq!(failed.len(), 1, "only the MODIFIED uid conflicts");
        assert!(
            failed[0].item.0.ends_with(":7:2"),
            "item: {}",
            failed[0].item.0
        );
        assert!(matches!(
            failed[0].error.kind(),
            AccountErrorKind::ConcurrencyConflict
        ));
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
                .count(),
            2,
        );
    }

    #[test]
    fn partially_conflicting_patch_only_succeeds_after_the_remove_lands() {
        let outcomes = patch_mutation_results(
            ids(&[1, 2, 3]),
            &[1, 2, 3],
            StoreWireOutcome::Modified(vec![2]),
            Ok(StoreWireOutcome::Applied),
            &folder(),
        );
        assert_eq!(outcomes.len(), 3);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
                .count(),
            2,
            "only the add-applied subset may succeed after the remove does too",
        );
        assert!(outcomes.iter().any(|outcome| {
            matches!(outcome, ItemOutcome::Failed(failure) if matches!(
                failure.error.kind(),
                AccountErrorKind::ConcurrencyConflict
            ))
        }));
    }

    #[test]
    fn failed_second_half_of_a_partially_applied_patch_is_uncertain() {
        let outcomes = patch_mutation_results(
            ids(&[1, 2]),
            &[1, 2],
            StoreWireOutcome::Modified(vec![2]),
            Err(store_failed_error(AccountOperation::UpdateFlags, &folder())),
            &folder(),
        );
        assert!(
            outcomes
                .iter()
                .any(|outcome| matches!(outcome, ItemOutcome::Uncertain(_)))
        );
        assert!(outcomes.iter().any(|outcome| {
            matches!(outcome, ItemOutcome::Failed(failure) if matches!(
                failure.error.kind(),
                AccountErrorKind::ConcurrencyConflict
            ))
        }));
        assert!(
            outcomes
                .iter()
                .all(|outcome| !matches!(outcome, ItemOutcome::Succeeded(_))),
            "a failed remove after a successful add must not claim a complete patch",
        );
    }

    // A wire error on the first STORE of a two-sided patch decides only the
    // MODSEQ group it belongs to. Earlier groups already carry known
    // outcomes, and known truth must never be downgraded by a later failure.
    #[test]
    fn first_store_error_in_a_patch_group_keeps_earlier_group_outcomes() {
        let mut results = patch_mutation_results(
            ids(&[1, 2]),
            &[1, 2],
            StoreWireOutcome::Applied,
            Ok(StoreWireOutcome::Applied),
            &folder(),
        );
        assert_eq!(
            results
                .iter()
                .filter(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
                .count(),
            2,
        );
        results.extend(patch_first_store_failure(
            ids(&[5]),
            crate::Error::Protocol("STORE never answered".into()),
            &folder(),
        ));
        assert_eq!(results.len(), 3);
        assert_eq!(
            results
                .iter()
                .filter(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
                .count(),
            2,
            "the failing group must not erase outcomes proven for other groups",
        );
        let failed: Vec<&BatchFailure> = results
            .iter()
            .filter_map(|outcome| match outcome {
                ItemOutcome::Failed(failure) => Some(failure),
                _ => None,
            })
            .collect();
        assert_eq!(failed.len(), 1);
        assert_eq!(
            failed[0].item,
            BatchItemId(super::super::encode_object_id(&folder(), 7, 5).0),
            "only the failing group's id may carry the failure",
        );
    }

    #[test]
    fn invalid_bulk_move_destination_is_rejected_before_folder_work() {
        let kind = MutationKind::Move(MembershipScope::Folder(bifrost_types::FolderId(
            "IN\r\nBOX".to_owned(),
        )));
        let err = validated_move_destination(&kind).expect_err("CRLF cannot be sent as a mailbox");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        assert_eq!(err.operation(), Some(AccountOperation::BulkMove));
    }

    #[test]
    fn applied_outcome_succeeds_every_target() {
        let outcomes = mutation_results(
            ids(&[4, 5]),
            StoreWireOutcome::Applied,
            AccountOperation::BulkMove,
            &folder(),
        );
        assert!(
            outcomes
                .iter()
                .all(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
        );
    }

    // A tagged-NO STORE with no `[MODIFIED ...]` names no conflicting
    // UIDs, so every target fails - none may be reported Succeeded.
    #[test]
    fn failed_outcome_fails_every_target() {
        let outcomes = mutation_results(
            ids(&[4, 5]),
            StoreWireOutcome::Failed,
            AccountOperation::BulkDestroy,
            &folder(),
        );
        assert_eq!(outcomes.len(), 2);
        for outcome in &outcomes {
            match outcome {
                ItemOutcome::Failed(failure) => assert_eq!(
                    failure.error.operation(),
                    Some(AccountOperation::BulkDestroy)
                ),
                other => panic!("expected Failed, got {other:?}"),
            }
        }
    }

    #[test]
    fn split_ids_by_uid_partitions_on_membership() {
        let (matching, remaining) = split_ids_by_uid(ids(&[1, 2, 3]), &[2]);
        assert_eq!(
            matching.iter().map(|id| id.uid).collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(
            remaining.iter().map(|id| id.uid).collect::<Vec<_>>(),
            vec![1, 3]
        );

        // An empty expunge set leaves everything on the remaining side.
        let (matching, remaining) = split_ids_by_uid(ids(&[1, 2]), &[]);
        assert!(matching.is_empty());
        assert_eq!(remaining.len(), 2);
    }

    // Every target handed to `failed_all` must come back as its own
    // `Failed` item carrying the encoded object id: a stale-UIDVALIDITY
    // batch must not collapse into one aggregate failure.
    #[test]
    fn failed_all_emits_one_outcome_per_target() {
        let error = uidvalidity_changed_error(AccountOperation::BulkMove, &folder());
        let outcomes = failed_all(ids(&[1, 2, 3]), error);
        assert_eq!(outcomes.len(), 3);
        let items: Vec<String> = outcomes
            .iter()
            .map(|outcome| match outcome {
                ItemOutcome::Failed(failure) => failure.item.0.clone(),
                other => panic!("expected Failed, got {other:?}"),
            })
            .collect();
        assert!(items[0].ends_with(":7:1"), "item: {}", items[0]);
        assert!(items[2].ends_with(":7:3"), "item: {}", items[2]);
    }

    #[test]
    fn stale_uidvalidity_targets_are_kept_for_failed_results() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let ids = vec![
            DecodedObjectId {
                folder: folder.clone(),
                uidvalidity: 9,
                uid: 1,
            },
            DecodedObjectId {
                folder,
                uidvalidity: 10,
                uid: 2,
            },
        ];

        let (valid, stale) = split_by_uidvalidity(ids, 10);
        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0].uid, 2);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].uid, 1);
    }
}
