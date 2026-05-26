use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{
    AccountError, AccountFuture, AccountStream, CursorScope, HintPayload, InvalidationHint,
    PushSource, SubscriptionHandle, WatchEvent,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::connection::IdleEvent;

use super::{ImapAccount, account_error, boxed_receiver_stream, folder_scope};

pub(crate) struct PushState {
    tx: broadcast::Sender<WatchEvent>,
    scopes: Mutex<HashMap<String, HashSet<CursorScope>>>,
    task_cancel: Mutex<Option<CancellationToken>>,
    next_id: AtomicU64,
}

impl PushState {
    pub(crate) fn new() -> Self {
        let (tx, _rx) = broadcast::channel(128);
        Self {
            tx,
            scopes: Mutex::new(HashMap::new()),
            task_cancel: Mutex::new(None),
            next_id: AtomicU64::new(1),
        }
    }

    pub(crate) fn stop(&self) {
        if let Some(cancel) = self
            .task_cancel
            .lock()
            .expect("push task lock poisoned")
            .take()
        {
            cancel.cancel();
        }
    }
}

pub(crate) fn push_subscribe(
    account: ImapAccount,
    scopes: Vec<CursorScope>,
) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
    Box::pin(async move {
        let id = account.push.next_id.fetch_add(1, Ordering::AcqRel);
        let handle = SubscriptionHandle(format!("imap-idle-{id}"));
        account
            .push
            .scopes
            .lock()
            .expect("push scopes lock poisoned")
            .insert(handle.0.clone(), scopes.into_iter().collect());
        ensure_idle_task(account.clone()).map_err(account_error)?;
        Ok(handle)
    })
}

pub(crate) fn push_unsubscribe(
    account: ImapAccount,
    handle: SubscriptionHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let empty = {
            let mut scopes = account
                .push
                .scopes
                .lock()
                .expect("push scopes lock poisoned");
            scopes.remove(&handle.0);
            scopes.is_empty()
        };
        if empty {
            account.push.stop();
        }
        Ok(())
    })
}

pub(crate) fn push_stream(account: ImapAccount) -> AccountStream<WatchEvent> {
    let mut rx = account.push.tx.subscribe();
    let shutdown = account.shutdown.clone();
    let (tx, out) = tokio::sync::mpsc::channel(128);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                result = rx.recv() => {
                    match result {
                        Ok(event) => {
                            if tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            let event = WatchEvent::Invalidated {
                                hint: InvalidationHint {
                                    source: PushSource::Coalesced,
                                    payload: HintPayload::Unknown,
                                },
                            };
                            if tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    });
    boxed_receiver_stream(out)
}

fn ensure_idle_task(account: ImapAccount) -> Result<(), crate::Error> {
    let mut guard = account
        .push
        .task_cancel
        .lock()
        .map_err(|_| crate::Error::Internal("push task lock poisoned".into()))?;
    if guard.is_some() {
        return Ok(());
    }
    let cancel = CancellationToken::new();
    *guard = Some(cancel.clone());
    drop(guard);
    tokio::spawn(async move {
        idle_loop(account, cancel).await;
    });
    Ok(())
}

async fn idle_loop(account: ImapAccount, cancel: CancellationToken) {
    loop {
        if cancel.is_cancelled() || account.shutdown.is_cancelled() {
            break;
        }
        let folder = choose_idle_folder(&account);
        let Some(folder) = folder else {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        };
        let conn = match account.pool.dial_idle().await {
            Ok(conn) => conn,
            Err(_) => {
                let _ = account.push.tx.send(WatchEvent::Disconnected);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        let selected = match conn
            .select(folder.as_str(), account.command_timeout())
            .await
        {
            Ok(selected) => selected,
            Err(_) => {
                let _ = account.push.tx.send(WatchEvent::Disconnected);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        let uidvalidity = selected.uid_validity;
        let _ = account.push.tx.send(WatchEvent::Reconnected);
        loop {
            if cancel.is_cancelled() || account.shutdown.is_cancelled() {
                let _ = conn.logout().await;
                return;
            }
            match conn.idle(account.config.idle_timeout, cancel.clone()).await {
                Ok(event) => {
                    if absorb_idle_event(&account, &folder, uidvalidity, &event).is_err() {
                        let _ = account.push.tx.send(invalidated(HintPayload::Unknown));
                    }
                    if let Some(event) = map_idle_event(event, &folder) {
                        let _ = account.push.tx.send(event);
                    }
                }
                Err(_) => {
                    let _ = account.push.tx.send(WatchEvent::Disconnected);
                    break;
                }
            }
        }
    }
}

fn choose_idle_folder(account: &ImapAccount) -> Option<crate::types::MailboxName> {
    let scopes = account
        .push
        .scopes
        .lock()
        .expect("push scopes lock poisoned")
        .values()
        .flat_map(|set| set.iter().cloned())
        .collect::<Vec<_>>();
    for scope in scopes {
        if let CursorScope::Folder(folder) = scope
            && let Ok(mailbox) = crate::types::MailboxName::new(folder.0)
        {
            return Some(mailbox);
        }
    }
    account
        .folders
        .entries()
        .into_iter()
        .find(|entry| entry.name.as_str().eq_ignore_ascii_case("INBOX"))
        .map(|entry| entry.name.clone())
}

fn absorb_idle_event(
    account: &ImapAccount,
    selected: &crate::types::MailboxName,
    uidvalidity: Option<u32>,
    event: &IdleEvent,
) -> Result<(), crate::Error> {
    match event {
        IdleEvent::Fetch(fetch) => {
            if let (Some(uidvalidity), Some(uid), Some(modseq)) =
                (uidvalidity, fetch.uid, fetch.mod_seq)
            {
                account
                    .folders
                    .record_modseq(selected, uidvalidity, uid, modseq)?;
            }
        }
        IdleEvent::Vanished { uids, .. } => {
            if let Some(uidvalidity) = uidvalidity {
                for range in uids {
                    let uids = super::folder_registry::expand_range(*range);
                    account.folders.clear_modseqs(selected, uidvalidity, &uids);
                }
            }
        }
        IdleEvent::MailboxEvent(info) => account.folders.apply_mailbox_event(info.clone()),
        _ => {}
    }
    Ok(())
}

pub(crate) fn map_idle_event(
    event: IdleEvent,
    selected: &crate::types::MailboxName,
) -> Option<WatchEvent> {
    match event {
        IdleEvent::Exists(_)
        | IdleEvent::Expunge(_)
        | IdleEvent::Vanished { .. }
        | IdleEvent::Fetch(_)
        | IdleEvent::Recent(_) => Some(invalidated(HintPayload::SpecificCursorScope(
            folder_scope(selected),
        ))),
        IdleEvent::MailboxStatus { mailbox, .. } => Some(invalidated(
            HintPayload::SpecificCursorScope(folder_scope(&mailbox)),
        )),
        IdleEvent::MailboxEvent(info) => Some(invalidated(HintPayload::SpecificCursorScope(
            folder_scope(&info.name),
        ))),
        IdleEvent::MetadataChange { .. }
        | IdleEvent::SearchUpdate(_)
        | IdleEvent::StatusUpdate { .. }
        | IdleEvent::NotificationOverflow { .. }
        | IdleEvent::Alert(_)
        | IdleEvent::Bye { .. }
        | IdleEvent::ExtensionEvent(_) => Some(invalidated(HintPayload::Unknown)),
        IdleEvent::Timeout | IdleEvent::Cancelled | IdleEvent::ServerTerminated => None,
    }
}

fn invalidated(payload: HintPayload) -> WatchEvent {
    WatchEvent::Invalidated {
        hint: InvalidationHint {
            source: PushSource::ImapNotify,
            payload,
        },
    }
}
