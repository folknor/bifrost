use std::collections::BTreeSet;

use bifrost_types::{
    Change, ChangeCursor, Checkpoint, CostClass, CursorDescriptor, Error as AccountError, Fatal,
    ObjectChange, ObjectChangeKind, PageBoundary, RecoveryClass, ScopeChange, ScopeChangeKind,
    SyncEvent, SyncStrategy, Warning, WarningKind,
};

use crate::connection::FetchStreamItem;
use crate::types::{FetchAttr, MailboxName, SelectedMailbox, UidSet};

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
        Ok(FolderCursor::QResync { .. }) if account.qresync_enabled() => CursorDescriptor {
            cost_class: CostClass::Cheap,
            strategy: SyncStrategy::QResync,
            freshness,
        },
        Ok(FolderCursor::QResync { .. }) => CursorDescriptor {
            cost_class: CostClass::Medium,
            strategy: SyncStrategy::Condstore,
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
    let qresync_negotiation_warning = if matches!(cursor, FolderCursor::QResync { .. }) {
        account.take_qresync_negotiation_warning()
    } else {
        None
    };
    match cursor {
        FolderCursor::QResync { .. } if account.qresync_enabled() => {
            run_qresync(account, folder, cursor, tx).await
        }
        FolderCursor::QResync {
            uidvalidity,
            modseq,
            known_uids,
            known_uids_complete,
        } => {
            let reason = qresync_negotiation_warning.unwrap_or_else(|| {
                "QRESYNC is disabled for this account session; continuing with CONDSTORE"
                    .to_string()
            });
            send_strategy_downgrade(&tx, SyncStrategy::QResync, SyncStrategy::Condstore, &reason)
                .await?;
            run_condstore_with_baseline(
                account,
                folder,
                FolderCursor::Condstore {
                    uidvalidity,
                    modseq,
                    known_uids,
                },
                known_uids_complete,
                tx,
            )
            .await
        }
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
        mut known_uids,
        mut known_uids_complete,
    } = cursor.clone()
    else {
        return Ok(());
    };
    let mut conn = account.checkout_for_folder(&folder).await?;
    let selected = match account
        .select_folder(&mut conn, &folder, Some(&cursor), true)
        .await
    {
        Ok(selected) => selected,
        Err(err) if should_disable_qresync(&err) => {
            if should_discard_conn_after_qresync_error(&err) {
                conn.discard();
            }
            account.disable_qresync_for_session();
            send_strategy_downgrade(
                &tx,
                SyncStrategy::QResync,
                SyncStrategy::Condstore,
                "QRESYNC SELECT failed during response parsing; continuing with CONDSTORE",
            )
            .await?;
            return run_condstore_with_baseline(
                account,
                folder,
                FolderCursor::Condstore {
                    uidvalidity: expected_uidvalidity,
                    modseq,
                    known_uids,
                },
                known_uids_complete,
                tx,
            )
            .await;
        }
        Err(err) => return Err(err.into()),
    };
    let uidvalidity = selected_uidvalidity(&selected.mailbox)?;
    validate_uidvalidity(&folder, expected_uidvalidity, uidvalidity)?;
    let seeded_qresync_baseline = !known_uids_complete;
    if !known_uids_complete {
        send_warning(
            &tx,
            WarningKind::Other("imap_qresync_baseline_seeded".to_string()),
            "QRESYNC cursor did not carry a complete UID baseline; seeding from UID SEARCH ALL",
        )
        .await?;
        known_uids = CompactUidSet::from_uids(search_all(&account, conn.connection()).await?);
        known_uids_complete = true;
    }
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
    let mut live_uids = known_uids.to_uids().into_iter().collect::<BTreeSet<_>>();
    let mut fallback_known_uids = live_uids.clone();
    let mut fetch_change_seen = BTreeSet::new();
    let mut removed_seen = BTreeSet::new();
    let mut changes = Vec::with_capacity(BATCH_ITEMS);
    for range in selected.mailbox.vanished.clone() {
        for uid in expand_range(range) {
            if seeded_qresync_baseline {
                fallback_known_uids.insert(uid);
            }
            record_removed_change(
                &folder,
                uidvalidity,
                uid,
                &mut live_uids,
                &mut removed_seen,
                &mut changes,
            );
            account.folders.clear_modseqs(&folder, uidvalidity, &[uid]);
        }
    }
    for fetch in selected.mailbox.changed_messages.clone() {
        if let (Some(uid), Some(modseq)) = (fetch.uid, fetch.mod_seq) {
            account
                .folders
                .record_modseq(&folder, uidvalidity, uid, modseq)?;
        }
        record_fetch_change(
            &folder,
            uidvalidity,
            fetch.uid,
            &mut live_uids,
            &mut fetch_change_seen,
            &mut removed_seen,
            &mut changes,
        );
    }
    let attrs = [FetchAttr::Uid, FetchAttr::Flags, FetchAttr::ModSeq];
    let all_uids = UidSet::all();
    let (fallback_error, flushed_qresync_changes) = {
        let (mut fetch_rx, fetch_fut) = conn.connection().uid_fetch_vanished_stream(
            all_uids.as_sequence_set(),
            &attrs,
            modseq,
            account.command_timeout(),
        )?;
        tokio::pin!(fetch_fut);

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
                            if let (Some(uid), Some(modseq)) = (fetch.uid, fetch.mod_seq) {
                                account.folders.record_modseq(&folder, uidvalidity, uid, modseq)?;
                            }
                            record_fetch_change(
                                &folder,
                                uidvalidity,
                                fetch.uid,
                                &mut live_uids,
                                &mut fetch_change_seen,
                                &mut removed_seen,
                                &mut changes,
                            );
                        }
                        Some(Ok(FetchStreamItem::VanishedEarlier(ranges))) => {
                            for uid in ranges.into_iter().flat_map(expand_range) {
                                if seeded_qresync_baseline {
                                    fallback_known_uids.insert(uid);
                                }
                                record_removed_change(
                                    &folder,
                                    uidvalidity,
                                    uid,
                                    &mut live_uids,
                                    &mut removed_seen,
                                    &mut changes,
                                );
                                account.folders.clear_modseqs(&folder, uidvalidity, &[uid]);
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
        (fallback_error, flushed_qresync_changes)
    };
    if let Some(err) = fallback_error {
        if should_disable_qresync(&err) && !flushed_qresync_changes {
            if should_discard_conn_after_qresync_error(&err) {
                conn.discard();
            }
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
                known_uids: CompactUidSet::from_uids(fallback_known_uids),
            };
            // Any buffered QRESYNC changes are intentionally discarded:
            // the CONDSTORE retry re-derives them from the same modseq
            // before a checkpoint is committed.
            return run_condstore_with_baseline(account, folder, cursor, known_uids_complete, tx)
                .await;
        }
        return Err(err.into());
    }

    let live_set = CompactUidSet::from_uids(live_uids);
    warn_if_uid_count_mismatch(&tx, &folder, selected.mailbox.exists, live_set.len()).await?;
    let next = account.cursor_from_select(&selected.mailbox, Some(live_set))?;
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
    run_condstore_with_baseline(account, folder, cursor, true, tx).await
}

async fn run_condstore_with_baseline(
    account: ImapAccount,
    folder: MailboxName,
    cursor: FolderCursor,
    known_uids_complete: bool,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    let FolderCursor::Condstore {
        modseq,
        mut known_uids,
        ..
    } = cursor.clone()
    else {
        return Ok(());
    };
    let mut conn = account.checkout_for_folder(&folder).await?;
    let selected = account
        .select_folder(&mut conn, &folder, Some(&cursor), true)
        .await?;
    let uidvalidity = selected_uidvalidity(&selected.mailbox)?;
    validate_uidvalidity(&folder, cursor.uidvalidity(), uidvalidity)?;
    if !known_uids_complete {
        send_warning(
            &tx,
            WarningKind::Other("imap_condstore_baseline_seeded".to_string()),
            "CONDSTORE fallback received a partial UID baseline; seeding from UID SEARCH ALL",
        )
        .await?;
        known_uids = CompactUidSet::from_uids(search_all(&account, conn.connection()).await?);
    }
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
            if let Some(modseq) = fetch.mod_seq {
                account
                    .folders
                    .record_modseq(&folder, uidvalidity, uid, modseq)?;
            }
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
        account.folders.clear_modseqs(&folder, uidvalidity, &[uid]);
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
    let uidvalidity = selected_uidvalidity(&selected.mailbox)?;
    validate_uidvalidity(&folder, cursor.uidvalidity(), uidvalidity)?;
    let live = search_all(&account, conn.connection()).await?;
    let live_set = CompactUidSet::from_uids(live);
    warn_if_uid_count_mismatch(&tx, &folder, selected.mailbox.exists, live_set.len()).await?;
    let diff = known_uids.diff(&live_set);
    let mut changes = Vec::new();
    for fetch in &selected.mailbox.changed_messages {
        if let Some(uid) = fetch.uid {
            if let Some(modseq) = fetch.mod_seq {
                account
                    .folders
                    .record_modseq(&folder, uidvalidity, uid, modseq)?;
            }
            changes.push(updated_change(&folder, uidvalidity, uid));
        }
    }
    for uid in diff.added {
        changes.push(added_change(&folder, uidvalidity, uid));
    }
    for uid in diff.removed {
        account.folders.clear_modseqs(&folder, uidvalidity, &[uid]);
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
    let uidvalidity = selected_uidvalidity(&selected)?;
    let live = search_all(&account, conn.connection()).await?;
    let live_set = CompactUidSet::from_uids(live);
    warn_if_uid_count_mismatch(&tx, &folder, selected.exists, live_set.len()).await?;
    let diff = known_uids.diff(&live_set);
    let mut changes = Vec::new();
    for uid in diff.added {
        changes.push(added_change(&folder, uidvalidity, uid));
    }
    for uid in diff.removed {
        account.folders.clear_modseqs(&folder, uidvalidity, &[uid]);
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

fn record_fetch_change(
    folder: &MailboxName,
    uidvalidity: u32,
    uid: Option<u32>,
    live_uids: &mut BTreeSet<u32>,
    fetch_change_seen: &mut BTreeSet<u32>,
    removed_seen: &mut BTreeSet<u32>,
    changes: &mut Vec<Change>,
) {
    let Some(uid) = uid else {
        return;
    };
    if !fetch_change_seen.insert(uid) {
        return;
    }
    if removed_seen.remove(&uid) {
        let removed_was_buffered =
            remove_buffered_removed_change(folder, uidvalidity, uid, changes);
        live_uids.insert(uid);
        if removed_was_buffered {
            changes.push(updated_change(folder, uidvalidity, uid));
        } else {
            changes.push(added_change(folder, uidvalidity, uid));
        }
        return;
    }
    if live_uids.insert(uid) {
        changes.push(added_change(folder, uidvalidity, uid));
    } else {
        changes.push(updated_change(folder, uidvalidity, uid));
    }
}

fn remove_buffered_removed_change(
    folder: &MailboxName,
    uidvalidity: u32,
    uid: u32,
    changes: &mut Vec<Change>,
) -> bool {
    let id = encode_object_id(folder, uidvalidity, uid);
    let Some(index) = changes.iter().rposition(|change| {
        matches!(
            change,
            Change::ScopeChange(ScopeChange {
                id: change_id,
                kind: ScopeChangeKind::Removed,
                ..
            }) if change_id == &id
        )
    }) else {
        return false;
    };
    changes.remove(index);
    true
}

fn record_removed_change(
    folder: &MailboxName,
    uidvalidity: u32,
    uid: u32,
    live_uids: &mut BTreeSet<u32>,
    removed_seen: &mut BTreeSet<u32>,
    changes: &mut Vec<Change>,
) {
    live_uids.remove(&uid);
    if removed_seen.insert(uid) {
        changes.push(removed_change(folder, uidvalidity, uid));
    }
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

fn selected_uidvalidity(selected: &SelectedMailbox) -> Result<u32, ChangeError> {
    selected
        .uid_validity
        .ok_or_else(|| crate::Error::Protocol("SELECT missing UIDVALIDITY".into()).into())
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
    send_warning(
        tx,
        WarningKind::Other("imap_uid_count_mismatch".to_string()),
        &format!(
            "IMAP UID count differs from SELECT EXISTS for {}: SELECT EXISTS {}, UID set {}",
            folder.as_str(),
            expected,
            actual,
        ),
    )
    .await
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
    match err {
        crate::Error::Parse(_) => true,
        crate::Error::MissingCapability(cap) => mentions_qresync_capability(cap),
        _ => false,
    }
}

fn should_discard_conn_after_qresync_error(err: &crate::Error) -> bool {
    matches!(err, crate::Error::Parse(_))
}

fn mentions_qresync_capability(value: &str) -> bool {
    value
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '=')))
        .any(|token| token.eq_ignore_ascii_case("QRESYNC"))
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

    #[tokio::test]
    async fn uid_count_mismatch_emits_warning() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let (tx, mut rx) = tokio::sync::mpsc::channel::<SyncEvent<Change>>(1);

        warn_if_uid_count_mismatch(&tx, &folder, 2, 1)
            .await
            .expect("warning should send");
        let event = rx.recv().await.expect("warning event");
        assert!(matches!(
            event,
            SyncEvent::Warning(Warning {
                kind: WarningKind::Other(_),
                ..
            })
        ));
    }

    #[test]
    fn qresync_record_helpers_deduplicate_select_and_fetch_data() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let mut live_uids = BTreeSet::from([1, 2]);
        let mut fetch_seen = BTreeSet::new();
        let mut removed_seen = BTreeSet::new();
        let mut changes = Vec::new();

        record_fetch_change(
            &folder,
            99,
            Some(3),
            &mut live_uids,
            &mut fetch_seen,
            &mut removed_seen,
            &mut changes,
        );
        record_fetch_change(
            &folder,
            99,
            Some(3),
            &mut live_uids,
            &mut fetch_seen,
            &mut removed_seen,
            &mut changes,
        );
        record_removed_change(
            &folder,
            99,
            2,
            &mut live_uids,
            &mut removed_seen,
            &mut changes,
        );
        record_removed_change(
            &folder,
            99,
            2,
            &mut live_uids,
            &mut removed_seen,
            &mut changes,
        );
        record_fetch_change(
            &folder,
            99,
            Some(2),
            &mut live_uids,
            &mut fetch_seen,
            &mut removed_seen,
            &mut changes,
        );

        assert_eq!(changes.len(), 2);
        assert!(matches!(
            changes[0],
            Change::ScopeChange(ScopeChange {
                kind: ScopeChangeKind::Added,
                ..
            })
        ));
        assert!(matches!(
            changes[1],
            Change::ObjectChange(ObjectChange {
                kind: ObjectChangeKind::Updated,
                ..
            })
        ));
        assert_eq!(live_uids, BTreeSet::from([1, 2, 3]));
    }

    #[test]
    fn qresync_disable_fallback_uses_parse_errors_and_enable_misses() {
        assert!(should_disable_qresync(&crate::Error::Parse(
            "malformed FETCH".to_string(),
        )));
        assert!(should_disable_qresync(&crate::Error::MissingCapability(
            "QRESYNC (not ENABLEd)".to_string(),
        )));
        assert!(!should_disable_qresync(&crate::Error::Protocol(
            "missing FLAGS".to_string(),
        )));
        assert!(!should_disable_qresync(&crate::Error::MissingCapability(
            "CONDSTORE".to_string(),
        )));
        assert!(!should_disable_qresync(&crate::Error::MissingCapability(
            "QRESYNC_V2".to_string(),
        )));
        assert!(mentions_qresync_capability(
            "SELECT (QRESYNC) requires QRESYNC"
        ));
    }
}
