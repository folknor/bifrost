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

enum MutationKind {
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

fn mutation_stream(
    account: ImapAccount,
    mut targets: AccountStream<bifrost_types::ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    tokio::spawn(async move {
        let mut grouped: HashMap<String, (MailboxName, Vec<DecodedObjectId>)> = HashMap::new();
        while let Some(id) = targets.next().await {
            match decode_object_id(&id) {
                Ok(decoded) => {
                    grouped
                        .entry(decoded.folder.as_str().to_owned())
                        .or_insert_with(|| (decoded.folder.clone(), Vec::new()))
                        .1
                        .push(decoded);
                }
                Err(err) => {
                    let item_id = BatchItemId(id.0.clone());
                    let _ = tx
                        .send(batch(
                            vec![ItemOutcome::Failed(BatchFailure::new(item_id, err))],
                            PageBoundary::Page,
                            None,
                        ))
                        .await;
                }
            }
        }

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
        for (_name, (folder, ids)) in grouped {
            match run_folder_mutation(&account, &folder, ids.clone(), &kind).await {
                Ok(results) => {
                    let _ = tx.send(batch(results, PageBoundary::Page, None)).await;
                }
                Err(err) => {
                    let account_err = super::account_error_with(
                        err,
                        super::error::ImapErrorContext::operation(mutation_operation(&kind))
                            .with_mailbox(&folder),
                    );
                    if stream_terminating(&account_err) {
                        let _ = tx.send(SyncEvent::Terminated(account_err)).await;
                        return;
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
                    let _ = tx.send(batch(uncertain, PageBoundary::Page, None)).await;
                }
            }
        }
        let _ = tx.send(SyncEvent::Done(None)).await;
    });
    boxed_receiver_stream(rx)
}

async fn run_folder_mutation(
    account: &ImapAccount,
    folder: &MailboxName,
    ids: Vec<DecodedObjectId>,
    kind: &MutationKind,
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
        MutationKind::Move(destination) => {
            let folder = if let MembershipScope::Folder(id) = destination {
                MailboxName::new(id.0.clone()).map_err(crate::Error::from)?
            } else {
                return Ok(failed_all(
                    valid,
                    super::error::unsupported(AccountOperation::BulkMove),
                ));
            };
            conn.connection()
                .uid_move_messages(
                    uid_set.as_sequence_set(),
                    folder.as_str(),
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
            mutation_results(valid, &uids, outcome, mutation_operation(kind), folder)
        }
        Err(err) => failed_all(
            valid,
            super::account_error_with(
                err,
                super::error::ImapErrorContext::operation(mutation_operation(kind))
                    .with_mailbox(folder),
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
                            .with_mailbox(folder),
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
            && let Err(err) = conn
                .connection()
                .uid_expunge(expunge_set.as_sequence_set(), account.command_timeout())
                .await
        {
            let (expunging_ids, remaining_ids) = split_ids_by_uid(ids, &expunge_uids);
            results.extend(failed_all(
                expunging_ids,
                super::account_error_with(
                    err,
                    super::error::ImapErrorContext::operation(AccountOperation::BulkDestroy)
                        .with_mailbox(folder),
                ),
            ));
            results.extend(mutation_results(
                remaining_ids,
                &uids,
                outcome,
                AccountOperation::BulkDestroy,
                folder,
            ));
            continue;
        }
        results.extend(mutation_results(
            ids,
            &uids,
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
                mutation_results(ids, &uids, outcome, AccountOperation::UpdateFlags, folder)
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
                            .with_mailbox(folder),
                    ),
                )
            }
        });
    }
    Ok(results)
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
        FlagOp::Patch { add, remove } => {
            store_flags(conn, set, StoreOperation::AddSilent, add, None, timeout).await?;
            store_flags(
                conn,
                set,
                StoreOperation::RemoveSilent,
                remove,
                None,
                timeout,
            )
            .await
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
    _requested_uids: &[u32],
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
        // UNCHANGEDSINCE conflict on pending-retry batch: modified UIDs
        // are concurrency conflicts; remaining UIDs were not committed
        // because the whole STORE was rejected.
        StoreWireOutcome::PendingRetry(modified) => ids
            .into_iter()
            .map(|id| {
                let item =
                    BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0);
                if modified.contains(&id.uid) {
                    ItemOutcome::Uncertain(BatchUncertain::new(
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
        // imap-N3: concurrency_conflict_error / store_failed_error /
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
            // Mailbox scope must thread too (imap-N2).
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
