use bifrost_types::{
    Change, ChangeCursor, Checkpoint, CostClass, CursorDescriptor, Error as AccountError,
    ObjectChange, ObjectChangeKind, PageBoundary, RecoveryClass, ScopeChange, ScopeChangeKind,
    SyncEvent, SyncStrategy,
};

use crate::types::{FetchAttr, MailboxName, UidSet};

use super::folder_registry::expand_range;
use super::{
    CompactUidSet, FolderCursor, ImapAccount, batch, boxed_receiver_stream, decode_cursor,
    encode_cursor, encode_object_id, fatal_event, folder_from_scope, folder_scope,
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
        }
    });
    boxed_receiver_stream(rx)
}

enum ChangeError {
    Account(AccountError),
    Imap(crate::Error),
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
    let mut conn = account.checkout_for_folder(&folder).await?;
    let selected = account
        .select_folder(&mut conn, &folder, Some(&cursor), true)
        .await?;
    let mut changes = Vec::new();
    let uidvalidity = selected.mailbox.uid_validity.unwrap_or_default();
    for fetch in &selected.mailbox.changed_messages {
        if let Some(uid) = fetch.uid {
            changes.push(Change::ObjectChange(ObjectChange {
                id: encode_object_id(&folder, uidvalidity, uid),
                kind: ObjectChangeKind::Updated,
            }));
        }
    }
    for uid in selected
        .mailbox
        .vanished
        .iter()
        .copied()
        .flat_map(expand_range)
    {
        changes.push(removed_change(&folder, uidvalidity, uid));
    }
    let next = account.cursor_from_select(&selected.mailbox, cursor.known_uids().cloned());
    finish_changes(account, folder, next, changes, tx).await
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
            changes.push(Change::ObjectChange(ObjectChange {
                id: encode_object_id(
                    &folder,
                    selected.mailbox.uid_validity.unwrap_or_default(),
                    uid,
                ),
                kind: ObjectChangeKind::Updated,
            }));
        }
    }
    let live = search_all(&account, conn.connection()).await?;
    let live_set = CompactUidSet::from_uids(live);
    let diff = known_uids.diff(&live_set);
    for uid in diff.added {
        changes.push(added_change(
            &folder,
            selected.mailbox.uid_validity.unwrap_or_default(),
            uid,
        ));
    }
    for uid in diff.removed {
        changes.push(removed_change(
            &folder,
            selected.mailbox.uid_validity.unwrap_or_default(),
            uid,
        ));
    }
    let next = FolderCursor::Condstore {
        uidvalidity: selected.mailbox.uid_validity.unwrap_or_default(),
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
    let live = search_all(&account, conn.connection()).await?;
    let live_set = CompactUidSet::from_uids(live);
    let diff = known_uids.diff(&live_set);
    let mut changes = Vec::new();
    for uid in diff.added {
        changes.push(added_change(
            &folder,
            selected.mailbox.uid_validity.unwrap_or_default(),
            uid,
        ));
    }
    for uid in diff.removed {
        changes.push(removed_change(
            &folder,
            selected.mailbox.uid_validity.unwrap_or_default(),
            uid,
        ));
    }
    let next = FolderCursor::Basic {
        uidvalidity: selected.mailbox.uid_validity.unwrap_or_default(),
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
