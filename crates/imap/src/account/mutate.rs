use std::collections::{HashMap, HashSet};

use bifrost_types::{
    AccountStream, Error as AccountError, FlagOp, IdempotencyKey, MembershipScope, MutationOutcome,
    MutationResult, PageBoundary, SyncEvent,
};
use futures::StreamExt;

use crate::types::{Flag, MailboxName, ResponseCode, StoreOperation};

use super::{
    DecodedObjectId, ImapAccount, batch, boxed_receiver_stream, decode_object_id, fatal_event,
    uid_set_from_u32,
};

pub(crate) fn bulk_set_flags(
    account: ImapAccount,
    targets: AccountStream<bifrost_types::ObjectId>,
    op: FlagOp,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    mutation_stream(account, targets, MutationKind::Flags(op))
}

pub(crate) fn bulk_move(
    account: ImapAccount,
    targets: AccountStream<bifrost_types::ObjectId>,
    destination: MembershipScope,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    mutation_stream(account, targets, MutationKind::Move(destination))
}

pub(crate) fn bulk_destroy(
    account: ImapAccount,
    targets: AccountStream<bifrost_types::ObjectId>,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    mutation_stream(account, targets, MutationKind::Destroy)
}

enum MutationKind {
    Flags(FlagOp),
    Move(MembershipScope),
    Destroy,
}

fn mutation_stream(
    account: ImapAccount,
    mut targets: AccountStream<bifrost_types::ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<MutationResult>> {
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
                    let _ = tx
                        .send(batch(
                            vec![MutationResult {
                                id,
                                outcome: MutationOutcome::Failed(err),
                            }],
                            PageBoundary::Page,
                            None,
                        ))
                        .await;
                }
            }
        }

        for (_name, (folder, ids)) in grouped {
            match run_folder_mutation(&account, &folder, ids, &kind).await {
                Ok(results) => {
                    let _ = tx.send(batch(results, PageBoundary::Page, None)).await;
                }
                Err(err) => {
                    let _ = tx.send(fatal_event(err)).await;
                    return;
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
) -> Result<Vec<MutationResult>, crate::Error> {
    let mut conn = account.checkout_for_folder(folder).await?;
    let cursor = account.folders.get(folder).and_then(|entry| entry.cursor());
    let selected = account
        .select_folder(&mut conn, folder, cursor.as_ref(), false)
        .await?;
    let uidvalidity = selected.mailbox.uid_validity.unwrap_or_default();
    let (valid, stale) = split_by_uidvalidity(ids, uidvalidity);
    let stale_results = failed_all(
        stale,
        AccountError::Other("UIDVALIDITY changed before mutation".into()),
    );
    let uids: Vec<u32> = valid.iter().map(|id| id.uid).collect();
    let Some(uid_set) = uid_set_from_u32(&uids) else {
        return Ok(stale_results);
    };

    let outcome = match kind {
        MutationKind::Flags(op) => {
            apply_flag_op(
                &conn,
                uid_set.as_sequence_set(),
                op,
                account.command_timeout(),
            )
            .await
        }
        MutationKind::Move(destination) => {
            let folder = if let MembershipScope::Folder(id) = destination {
                MailboxName::new(id.0.clone()).map_err(crate::Error::from)?
            } else {
                return Ok(failed_all(valid, AccountError::Unsupported));
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
        MutationKind::Destroy => {
            conn.connection()
                .uid_store(
                    uid_set.as_sequence_set(),
                    StoreOperation::AddSilent,
                    &[Flag::Deleted],
                    None,
                    account.command_timeout(),
                )
                .await?;
            conn.connection()
                .uid_expunge(uid_set.as_sequence_set(), account.command_timeout())
                .await
                .map(|_| StoreWireOutcome::Applied)
        }
    };

    let mut results = stale_results;
    results.extend(match outcome {
        Ok(outcome) => mutation_results(valid, &uids, outcome),
        Err(err) => failed_all(valid, super::account_error(err)),
    });
    Ok(results)
}

async fn apply_flag_op(
    conn: &super::PooledConn,
    set: &crate::types::SequenceSet,
    op: &FlagOp,
    timeout: std::time::Duration,
) -> Result<StoreWireOutcome, crate::Error> {
    match op {
        FlagOp::Add(flags) => {
            store_flags(conn, set, StoreOperation::AddSilent, flags, timeout).await
        }
        FlagOp::Remove(flags) => {
            store_flags(conn, set, StoreOperation::RemoveSilent, flags, timeout).await
        }
        FlagOp::Set(flags) => {
            store_flags(conn, set, StoreOperation::ReplaceSilent, flags, timeout).await
        }
        FlagOp::Patch { add, remove } => {
            store_flags(conn, set, StoreOperation::AddSilent, add, timeout).await?;
            store_flags(conn, set, StoreOperation::RemoveSilent, remove, timeout).await
        }
        _ => Err(crate::Error::Protocol("unsupported flag operation".into())),
    }
}

async fn store_flags(
    conn: &super::PooledConn,
    set: &crate::types::SequenceSet,
    operation: StoreOperation,
    flags: &HashSet<String>,
    timeout: std::time::Duration,
) -> Result<StoreWireOutcome, crate::Error> {
    let flags = flags
        .iter()
        .map(|flag| Flag::from(flag.as_str()))
        .collect::<Vec<_>>();
    let result = conn
        .connection()
        .uid_store(set, operation, &flags, None, timeout)
        .await?;
    Ok(StoreWireOutcome::from_response_code(
        result.code.as_ref(),
        true,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreWireOutcome {
    Applied,
    Modified(Vec<u32>),
    PendingRetry(Vec<u32>),
    Failed,
}

impl StoreWireOutcome {
    pub(crate) fn from_response_code(code: Option<&ResponseCode>, tagged_ok: bool) -> Self {
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
    requested_uids: &[u32],
    outcome: StoreWireOutcome,
) -> Vec<MutationResult> {
    match outcome {
        StoreWireOutcome::Applied => ids
            .into_iter()
            .map(|id| MutationResult {
                id: super::encode_object_id(&id.folder, id.uidvalidity, id.uid),
                outcome: MutationOutcome::Applied,
            })
            .collect(),
        StoreWireOutcome::Modified(modified) => ids
            .into_iter()
            .map(|id| {
                let failed = modified.contains(&id.uid);
                MutationResult {
                    id: super::encode_object_id(&id.folder, id.uidvalidity, id.uid),
                    outcome: if failed {
                        MutationOutcome::Failed(AccountError::ConcurrencyConflict)
                    } else {
                        MutationOutcome::Applied
                    },
                }
            })
            .collect(),
        StoreWireOutcome::PendingRetry(modified) => ids
            .into_iter()
            .map(|id| MutationResult {
                id: super::encode_object_id(&id.folder, id.uidvalidity, id.uid),
                outcome: if modified.contains(&id.uid) {
                    MutationOutcome::Failed(AccountError::ConcurrencyConflict)
                } else {
                    MutationOutcome::Failed(AccountError::Other("pending retry".into()))
                },
            })
            .collect(),
        StoreWireOutcome::Failed => failed_all_by_uids(ids, requested_uids),
    }
}

fn failed_all(ids: Vec<DecodedObjectId>, error: AccountError) -> Vec<MutationResult> {
    ids.into_iter()
        .map(|id| MutationResult {
            id: super::encode_object_id(&id.folder, id.uidvalidity, id.uid),
            outcome: MutationOutcome::Failed(AccountError::Other(error.to_string())),
        })
        .collect()
}

fn failed_all_by_uids(ids: Vec<DecodedObjectId>, _requested_uids: &[u32]) -> Vec<MutationResult> {
    failed_all(ids, AccountError::Other("mutation failed".into()))
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
