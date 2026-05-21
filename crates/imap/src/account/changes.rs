use std::collections::BTreeSet;

use bifrost_types::{
    Change, ChangeCursor, Checkpoint, CostClass, CursorDescriptor, Error as AccountError, Fatal,
    ObjectChange, ObjectChangeKind, PageBoundary, RecoveryClass, ScopeChange, ScopeChangeKind,
    SyncEvent, SyncStrategy, Warning, WarningKind,
};

use crate::connection::FetchStreamItem;
use crate::types::{FetchAttr, MailboxName, UidSet};

use super::folder_registry::expand_range;
use super::{
    BATCH_ITEMS, CompactUidSet, FolderCursor, ImapAccount, batch, boxed_receiver_stream,
    decode_cursor, encode_cursor, encode_object_id, fatal_event, folder_from_scope, folder_scope,
    membership_scope,
};

pub(crate) fn describe_cursor(account: &ImapAccount, cursor: &ChangeCursor) -> CursorDescriptor {
    let decoded = decode_cursor(cursor);
    let folder = folder_from_scope(&cursor.scope).ok();
    let freshness = folder
        .as_ref()
        .and_then(|folder| account.folders.get(folder))
        .and_then(|entry| entry.last_seen());

    match decoded {
        Ok(FolderCursor::QResync { .. }) => CursorDescriptor {
            cost_class: CostClass::Cheap,
            strategy: SyncStrategy::QResync,
            freshness,
        },
        Ok(FolderCursor::Condstore { .. }) => CursorDescriptor {
            cost_class: CostClass::Medium,
            strategy: SyncStrategy::Condstore,
            freshness,
        },
        Ok(FolderCursor::Basic { .. }) => CursorDescriptor {
            cost_class: CostClass::Expensive,
            strategy: SyncStrategy::Basic,
            freshness,
        },
        Err(_) => CursorDescriptor {
            cost_class: CostClass::Expensive,
            strategy: SyncStrategy::None,
            freshness: None,
        },
    }
}

pub(crate) fn changes_stream(
    account: ImapAccount,
    cursor: ChangeCursor,
) -> bifrost_types::AccountStream<SyncEvent<Change>> {
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    tokio::spawn(async move {
        match run_changes(account, cursor, tx.clone()).await {
            Ok(()) => {}
            Err(ChangeError::Account(err)) => {
                let _ = tx
                    .send(SyncEvent::Fatal(bifrost_types::Fatal {
                        recovery: RecoveryClass::SchemaIncompatible,
                        message: err.to_string(),
                        source: Some(err),
                    }))
                    .await;
            }
            Err(ChangeError::Imap(err)) => {
                let _ = tx.send(fatal_event(err)).await;
            }
            Err(ChangeError::UidValidityChanged {
                folder,
                expected,
                actual,
            }) => {
                let _ = tx
                    .send(SyncEvent::Fatal(uidvalidity_changed_fatal(
                        &folder, expected, actual,
                    )))
                    .await;
            }
            Err(ChangeError::ModSeqReset {
                folder,
                previous,
                current,
            }) => {
                let _ = tx
                    .send(SyncEvent::Fatal(modseq_reset_fatal(
                        &folder, previous, current,
                    )))
                    .await;
            }
        }
    });
    boxed_receiver_stream(rx)
}

#[derive(Debug)]
enum ChangeError {
    Account(AccountError),
    Imap(crate::Error),
    UidValidityChanged {
        folder: MailboxName,
        expected: u32,
        actual: u32,
    },
    ModSeqReset {
        folder: MailboxName,
        previous: u64,
        current: Option<u64>,
    },
}

impl From<crate::Error> for ChangeError {
    fn from(value: crate::Error) -> Self {
        Self::Imap(value)
    }
}

impl From<AccountError> for ChangeError {
    fn from(value: AccountError) -> Self {
        Self::Account(value)
    }
}

async fn run_changes(
    account: ImapAccount,
    change_cursor: ChangeCursor,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    let folder = folder_from_scope(&change_cursor.scope)?;
    let cursor = decode_cursor(&change_cursor)?;
    match cursor {
        FolderCursor::QResync { .. } => run_qresync(account, folder, cursor, tx).await,
        FolderCursor::Condstore { .. } => run_condstore(account, folder, cursor, tx).await,
        FolderCursor::Basic { .. } => run_basic(account, folder, cursor, tx).await,
    }
}

async fn run_qresync(
    account: ImapAccount,
    folder: MailboxName,
    cursor: FolderCursor,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    let FolderCursor::QResync {
        uidvalidity: expected_uidvalidity,
        modseq,
        known_uids,
        known_uids_complete,
    } = cursor.clone()
    else {
        return Ok(());
    };
    let mut conn = account.checkout_for_folder(&folder).await?;
    let selected = account
        .select_folder(&mut conn, &folder, None, true)
        .await?;
    let uidvalidity = selected.mailbox.uid_validity.unwrap_or_default();
    validate_uidvalidity(&folder, expected_uidvalidity, uidvalidity)?;
    if selected.mailbox.no_mod_seq || selected.mailbox.highest_mod_seq.is_none() {
        send_strategy_downgrade(
            &tx,
            SyncStrategy::QResync,
            SyncStrategy::Basic,
            "selected mailbox has no persistent mod-sequences",
        )
        .await?;
        return run_basic_from_selected(account, folder, known_uids, selected.mailbox, conn, tx)
            .await;
    }
    validate_modseq_not_reset(&folder, modseq, selected.mailbox.highest_mod_seq)?;
    let mut live_uids = if known_uids_complete {
        known_uids.to_uids().into_iter().collect::<BTreeSet<_>>()
    } else {
        send_warning(
            &tx,
            WarningKind::Other("imap_qresync_baseline_seeded".to_string()),
            "QRESYNC cursor did not carry a complete UID baseline; seeding from UID SEARCH ALL",
        )
        .await?;
        search_all(&account, conn.connection())
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>()
    };
    let attrs = [FetchAttr::Uid, FetchAttr::Flags, FetchAttr::ModSeq];
    let all_uids = UidSet::all();
    let (mut fetch_rx, fetch_fut) = conn.connection().uid_fetch_vanished_stream(
        all_uids.as_sequence_set(),
        &attrs,
        modseq,
        account.command_timeout(),
    )?;
    tokio::pin!(fetch_fut);

    let mut changes = Vec::with_capacity(BATCH_ITEMS);
    let mut fetch_result = None;
    let mut fallback_error = None;
    let mut flushed_qresync_changes = false;
    loop {
        tokio::select! {
            result = &mut fetch_fut, if fetch_result.is_none() => {
                fetch_result = Some(result);
            }
            item = fetch_rx.recv() => {
                match item {
                    Some(Ok(FetchStreamItem::Fetch(fetch))) => {
                        if let Some(uid) = fetch.uid {
                            if live_uids.insert(uid) {
                                changes.push(added_change(&folder, uidvalidity, uid));
                            } else {
                                changes.push(updated_change(&folder, uidvalidity, uid));
                            }
                        }
                    }
                    Some(Ok(FetchStreamItem::VanishedEarlier(ranges))) => {
                        for uid in ranges.into_iter().flat_map(expand_range) {
                            live_uids.remove(&uid);
                            changes.push(removed_change(&folder, uidvalidity, uid));
                            if changes.len() >= BATCH_ITEMS {
                                let out = std::mem::take(&mut changes);
                                tx.send(batch(out, PageBoundary::Page, None))
                                    .await
                                    .map_err(|_| crate::Error::Closed)?;
                                flushed_qresync_changes = true;
                            }
                        }
                    }
                    Some(Err(err)) => {
                        fallback_error = Some(err);
                        break;
                    }
                    None => {
                        if let Some(Err(err)) = fetch_result.take() {
                            fallback_error = Some(err);
                        }
                        break;
                    }
                }
                if changes.len() >= BATCH_ITEMS {
                    let out = std::mem::take(&mut changes);
                    tx.send(batch(out, PageBoundary::Page, None))
                        .await
                        .map_err(|_| crate::Error::Closed)?;
                    flushed_qresync_changes = true;
                }
            }
        }
    }
    if let Some(err) = fallback_error {
        if should_disable_qresync(&err) && !flushed_qresync_changes {
            account.disable_qresync_for_session();
            send_strategy_downgrade(
                &tx,
                SyncStrategy::QResync,
                SyncStrategy::Condstore,
                "QRESYNC VANISHED fetch failed; continuing with CONDSTORE",
            )
            .await?;
            let cursor = FolderCursor::Condstore {
                uidvalidity,
                modseq,
                known_uids,
            };
            // Any buffered QRESYNC changes are intentionally discarded:
            // the CONDSTORE retry re-derives them from the same modseq
            // before a checkpoint is committed.
            return run_condstore(account, folder, cursor, tx).await;
        }
        return Err(err.into());
    }

    let live_set = CompactUidSet::from_uids(live_uids);
    warn_if_uid_count_mismatch(&tx, &folder, selected.mailbox.exists, live_set.len()).await?;
    let next = account.cursor_from_select(&selected.mailbox, Some(live_set));
    account.folders.set_cursor(&folder, next.clone());
    let checkpoint = Some(Checkpoint::Change(encode_cursor(
        folder_scope(&folder),
        &next,
    )));
    if !changes.is_empty() {
        tx.send(batch(changes, PageBoundary::Final, checkpoint.clone()))
            .await
            .map_err(|_| crate::Error::Closed)?;
    }
    tx.send(SyncEvent::Done(checkpoint))
        .await
        .map_err(|_| crate::Error::Closed)?;
    Ok(())
}

async fn run_condstore(
    account: ImapAccount,
    folder: MailboxName,
    cursor: FolderCursor,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    let FolderCursor::Condstore {
        modseq, known_uids, ..
    } = cursor.clone()
    else {
        return Ok(());
    };
    let mut conn = account.checkout_for_folder(&folder).await?;
    let selected = account
        .select_folder(&mut conn, &folder, Some(&cursor), true)
        .await?;
    let uidvalidity = selected.mailbox.uid_validity.unwrap_or_default();
    validate_uidvalidity(&folder, cursor.uidvalidity(), uidvalidity)?;
    if selected.mailbox.no_mod_seq || selected.mailbox.highest_mod_seq.is_none() {
        send_strategy_downgrade(
            &tx,
            SyncStrategy::Condstore,
            SyncStrategy::Basic,
            "selected mailbox has no persistent mod-sequences",
        )
        .await?;
        return run_basic_from_selected(account, folder, known_uids, selected.mailbox, conn, tx)
            .await;
    }
    validate_modseq_not_reset(&folder, modseq, selected.mailbox.highest_mod_seq)?;

    let all_uids = UidSet::all();
    let fetches = conn
        .connection()
        .uid_fetch_changed_since(
            all_uids.as_sequence_set(),
            &[FetchAttr::Uid, FetchAttr::Flags, FetchAttr::ModSeq],
            modseq,
            account.command_timeout(),
        )
        .await?;
    let mut changes = Vec::new();
    for fetch in fetches {
        if let Some(uid) = fetch.uid {
            changes.push(updated_change(&folder, uidvalidity, uid));
        }
    }
    let live = search_all(&account, conn.connection()).await?;
    let live_set = CompactUidSet::from_uids(live);
    warn_if_uid_count_mismatch(&tx, &folder, selected.mailbox.exists, live_set.len()).await?;
    let diff = known_uids.diff(&live_set);
    for uid in diff.added {
        changes.push(added_change(&folder, uidvalidity, uid));
    }
    for uid in diff.removed {
        changes.push(removed_change(&folder, uidvalidity, uid));
    }
    let next = FolderCursor::Condstore {
        uidvalidity,
        modseq: selected.mailbox.highest_mod_seq.unwrap_or(modseq),
        known_uids: live_set,
    };
    finish_changes(account, folder, next, changes, tx).await
}

async fn run_basic(
    account: ImapAccount,
    folder: MailboxName,
    cursor: FolderCursor,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    let known_uids = cursor.known_uids().cloned().unwrap_or_default();
    let mut conn = account.checkout_for_folder(&folder).await?;
    let selected = account
        .select_folder(&mut conn, &folder, Some(&cursor), true)
        .await?;
    let uidvalidity = selected.mailbox.uid_validity.unwrap_or_default();
    validate_uidvalidity(&folder, cursor.uidvalidity(), uidvalidity)?;
    let live = search_all(&account, conn.connection()).await?;
    let live_set = CompactUidSet::from_uids(live);
    warn_if_uid_count_mismatch(&tx, &folder, selected.mailbox.exists, live_set.len()).await?;
    let diff = known_uids.diff(&live_set);
    let mut changes = Vec::new();
    for fetch in &selected.mailbox.changed_messages {
        if let Some(uid) = fetch.uid {
            changes.push(updated_change(&folder, uidvalidity, uid));
        }
    }
    for uid in diff.added {
        changes.push(added_change(&folder, uidvalidity, uid));
    }
    for uid in diff.removed {
        changes.push(removed_change(&folder, uidvalidity, uid));
    }
    let next = FolderCursor::Basic {
        uidvalidity,
        uidnext: selected.mailbox.uid_next.unwrap_or_default(),
        known_uids: live_set,
    };
    finish_changes(account, folder, next, changes, tx).await
}

async fn run_basic_from_selected(
    account: ImapAccount,
    folder: MailboxName,
    known_uids: CompactUidSet,
    selected: crate::types::SelectedMailbox,
    conn: super::PooledConn,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    let uidvalidity = selected.uid_validity.unwrap_or_default();
    let live = search_all(&account, conn.connection()).await?;
    let live_set = CompactUidSet::from_uids(live);
    warn_if_uid_count_mismatch(&tx, &folder, selected.exists, live_set.len()).await?;
    let diff = known_uids.diff(&live_set);
    let mut changes = Vec::new();
    for uid in diff.added {
        changes.push(added_change(&folder, uidvalidity, uid));
    }
    for uid in diff.removed {
        changes.push(removed_change(&folder, uidvalidity, uid));
    }
    let next = FolderCursor::Basic {
        uidvalidity,
        uidnext: selected.uid_next.unwrap_or_default(),
        known_uids: live_set,
    };
    finish_changes(account, folder, next, changes, tx).await
}

async fn search_all(
    account: &ImapAccount,
    conn: &crate::ImapConnection,
) -> Result<Vec<u32>, crate::Error> {
    Ok(conn.uid_search("ALL", account.command_timeout()).await?.ids)
}

async fn finish_changes(
    account: ImapAccount,
    folder: MailboxName,
    cursor: FolderCursor,
    changes: Vec<Change>,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    account.folders.set_cursor(&folder, cursor.clone());
    let checkpoint = Some(Checkpoint::Change(encode_cursor(
        folder_scope(&folder),
        &cursor,
    )));
    if !changes.is_empty() {
        tx.send(batch(changes, PageBoundary::Final, checkpoint.clone()))
            .await
            .map_err(|_| crate::Error::Closed)?;
    }
    tx.send(SyncEvent::Done(checkpoint))
        .await
        .map_err(|_| crate::Error::Closed)?;
    Ok(())
}

fn added_change(folder: &MailboxName, uidvalidity: u32, uid: u32) -> Change {
    Change::ScopeChange(ScopeChange {
        id: encode_object_id(folder, uidvalidity, uid),
        membership: membership_scope(folder),
        kind: ScopeChangeKind::Added,
    })
}

fn removed_change(folder: &MailboxName, uidvalidity: u32, uid: u32) -> Change {
    Change::ScopeChange(ScopeChange {
        id: encode_object_id(folder, uidvalidity, uid),
        membership: membership_scope(folder),
        kind: ScopeChangeKind::Removed,
    })
}

fn updated_change(folder: &MailboxName, uidvalidity: u32, uid: u32) -> Change {
    Change::ObjectChange(ObjectChange {
        id: encode_object_id(folder, uidvalidity, uid),
        kind: ObjectChangeKind::Updated,
    })
}

fn validate_uidvalidity(
    folder: &MailboxName,
    expected: u32,
    actual: u32,
) -> Result<(), ChangeError> {
    if expected == actual {
        return Ok(());
    }
    Err(ChangeError::UidValidityChanged {
        folder: folder.clone(),
        expected,
        actual,
    })
}

fn validate_modseq_not_reset(
    folder: &MailboxName,
    previous: u64,
    current: Option<u64>,
) -> Result<(), ChangeError> {
    if current.is_some_and(|current| current >= previous) {
        return Ok(());
    }
    Err(ChangeError::ModSeqReset {
        folder: folder.clone(),
        previous,
        current,
    })
}

async fn warn_if_uid_count_mismatch<T>(
    tx: &tokio::sync::mpsc::Sender<SyncEvent<T>>,
    folder: &MailboxName,
    expected: u32,
    actual: usize,
) -> Result<(), ChangeError> {
    if usize::try_from(expected).ok() == Some(actual) {
        return Ok(());
    }
    tx.send(SyncEvent::Warning(uid_count_mismatch_warning(
        folder, expected, actual,
    )))
    .await
    .map_err(|_| crate::Error::Closed)?;
    Ok(())
}

fn uid_count_mismatch_warning(folder: &MailboxName, expected: u32, actual: usize) -> Warning {
    Warning {
        kind: WarningKind::Other("imap_uid_count_mismatch".to_string()),
        message: format!(
            "IMAP UID count differs from SELECT EXISTS for {}: SELECT EXISTS {}, UID set {}",
            folder.as_str(),
            expected,
            actual,
        ),
        retry_count: 0,
        next_action: None,
        protocol_detail: Some("imap".to_string()),
    }
}

async fn send_warning<T>(
    tx: &tokio::sync::mpsc::Sender<SyncEvent<T>>,
    kind: WarningKind,
    message: &str,
) -> Result<(), ChangeError> {
    tx.send(SyncEvent::Warning(Warning {
        kind,
        message: message.to_string(),
        retry_count: 0,
        next_action: None,
        protocol_detail: Some("imap".to_string()),
    }))
    .await
    .map_err(|_| crate::Error::Closed)?;
    Ok(())
}

async fn send_strategy_downgrade<T>(
    tx: &tokio::sync::mpsc::Sender<SyncEvent<T>>,
    from: SyncStrategy,
    to: SyncStrategy,
    reason: &str,
) -> Result<(), ChangeError> {
    tx.send(SyncEvent::Warning(Warning {
        kind: WarningKind::StrategyDowngraded { from, to },
        message: reason.to_string(),
        retry_count: 0,
        next_action: None,
        protocol_detail: Some("imap".to_string()),
    }))
    .await
    .map_err(|_| crate::Error::Closed)?;
    Ok(())
}

fn should_disable_qresync(err: &crate::Error) -> bool {
    matches!(err, crate::Error::Parse(_))
}

fn uidvalidity_changed_fatal(folder: &MailboxName, expected: u32, actual: u32) -> Fatal {
    Fatal {
        recovery: RecoveryClass::RestartScope(folder_scope(folder)),
        message: format!(
            "IMAP UIDVALIDITY changed for {} from {} to {}",
            folder.as_str(),
            expected,
            actual,
        ),
        source: Some(AccountError::Other(
            "IMAP UIDVALIDITY changed before changes_stream".to_string(),
        )),
    }
}

fn modseq_reset_fatal(folder: &MailboxName, previous: u64, current: Option<u64>) -> Fatal {
    Fatal {
        recovery: RecoveryClass::RestartScope(folder_scope(folder)),
        message: format!(
            "IMAP HIGHESTMODSEQ reset for {} from {} to {:?}",
            folder.as_str(),
            previous,
            current,
        ),
        source: Some(AccountError::Other(
            "IMAP mod-sequence reset before changes_stream".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use bifrost_types::CursorScope;

    use super::*;

    #[test]
    fn uidvalidity_mismatch_requests_scope_restart() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let err = validate_uidvalidity(&folder, 7, 8).expect_err("mismatch should fail");
        let ChangeError::UidValidityChanged {
            folder,
            expected,
            actual,
        } = err
        else {
            panic!("expected uidvalidity change");
        };
        let fatal = uidvalidity_changed_fatal(&folder, expected, actual);
        assert!(matches!(
            fatal.recovery,
            RecoveryClass::RestartScope(CursorScope::Folder(_))
        ));
    }

    #[test]
    fn modseq_reset_requests_scope_restart() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let err = validate_modseq_not_reset(&folder, 10, Some(9)).expect_err("reset should fail");
        let ChangeError::ModSeqReset {
            folder,
            previous,
            current,
        } = err
        else {
            panic!("expected modseq reset");
        };
        let fatal = modseq_reset_fatal(&folder, previous, current);
        assert!(matches!(
            fatal.recovery,
            RecoveryClass::RestartScope(CursorScope::Folder(_))
        ));
    }

    #[test]
    fn uid_count_mismatch_is_warning() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let warning = uid_count_mismatch_warning(&folder, 2, 1);
        assert!(matches!(
            warning.kind,
            WarningKind::Other(ref kind) if kind == "imap_uid_count_mismatch"
        ));
    }

    #[test]
    fn qresync_disable_fallback_only_uses_parse_errors() {
        assert!(should_disable_qresync(&crate::Error::Parse(
            "malformed FETCH".to_string(),
        )));
        assert!(!should_disable_qresync(&crate::Error::Protocol(
            "missing FLAGS".to_string(),
        )));
    }
}
