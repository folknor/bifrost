use bifrost_types::{
    Change, ChangeCursor, Checkpoint, CostClass, CursorDescriptor, Error as AccountError, Fatal,
    ObjectChange, ObjectChangeKind, PageBoundary, RecoveryClass, ScopeChange, ScopeChangeKind,
    SyncEvent, SyncStrategy,
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
    loop {
        tokio::select! {
            result = &mut fetch_fut, if fetch_result.is_none() => {
                fetch_result = Some(result);
            }
            item = fetch_rx.recv() => {
                match item {
                    Some(Ok(FetchStreamItem::Fetch(fetch))) => {
                        if let Some(uid) = fetch.uid {
                            changes.push(updated_change(&folder, uidvalidity, uid));
                        }
                    }
                    Some(Ok(FetchStreamItem::VanishedEarlier(ranges))) => {
                        for uid in ranges.into_iter().flat_map(expand_range) {
                            changes.push(removed_change(&folder, uidvalidity, uid));
                            if changes.len() >= BATCH_ITEMS {
                                let out = std::mem::take(&mut changes);
                                tx.send(batch(out, PageBoundary::Page, None))
                                    .await
                                    .map_err(|_| crate::Error::Closed)?;
                            }
                        }
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        if let Some(result) = fetch_result.take() {
                            result?;
                        }
                        break;
                    }
                }
                if changes.len() >= BATCH_ITEMS {
                    let out = std::mem::take(&mut changes);
                    tx.send(batch(out, PageBoundary::Page, None))
                        .await
                        .map_err(|_| crate::Error::Closed)?;
                }
            }
        }
    }
    let next = account.cursor_from_select(&selected.mailbox, cursor.known_uids().cloned());
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
        let next = account.cursor_from_select(&selected.mailbox, Some(known_uids));
        account.folders.set_cursor(&folder, next.clone());
        return finish_changes(account, folder, next, Vec::new(), tx).await;
    }

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
}
