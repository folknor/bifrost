use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AccountStream, CursorScope, HintPayload,
    InvalidationHint, PushSource, SubscriptionHandle, WatchEvent,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::account::error::ImapErrorContext;
use crate::connection::IdleEvent;

use super::{ImapAccount, account_error_with, boxed_receiver_stream, folder_scope};

pub(crate) struct PushState {
    tx: broadcast::Sender<WatchEvent>,
    scopes: Mutex<HashMap<String, HashSet<CursorScope>>>,
    task_cancel: Mutex<Option<CancellationToken>>,
    /// Wakes a parked IDLE loop when the subscribed scope set changes, so
    /// a scope added after IDLE has already parked on one folder gets a
    /// chance to be chosen instead of waiting for the current connection
    /// to break. The loop re-evaluates `choose_idle_folder` on each wake.
    resubscribe: std::sync::Arc<tokio::sync::Notify>,
    next_id: AtomicU64,
}

impl PushState {
    pub(crate) fn new() -> Self {
        let (tx, _rx) = broadcast::channel(128);
        Self {
            tx,
            scopes: Mutex::new(HashMap::new()),
            task_cancel: Mutex::new(None),
            resubscribe: std::sync::Arc::new(tokio::sync::Notify::new()),
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
        ensure_idle_task(account.clone()).map_err(|e| {
            account_error_with(
                e,
                ImapErrorContext::operation(AccountOperation::PushSubscribe),
            )
        })?;
        // Nudge an already-running IDLE loop so a scope added after it
        // parked on another folder is reconsidered without waiting for the
        // current IDLE connection to break.
        account.push.resubscribe.notify_one();
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
    // Track whether the consumer has seen a `Disconnected` since the last
    // `Reconnected`. The very first successful connect must NOT emit
    // `Reconnected` (there was no prior disconnect): the reconciler treats
    // `Reconnected` as a full account-wide `Unknown` reconcile, which is
    // spurious right after subscribe when discovery/inventory just ran.
    let mut was_disconnected = false;
    let resubscribe = std::sync::Arc::clone(&account.push.resubscribe);
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
                was_disconnected = true;
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
                was_disconnected = true;
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        let uidvalidity = selected.uid_validity;
        if was_disconnected {
            let _ = account.push.tx.send(WatchEvent::Reconnected);
            was_disconnected = false;
        }
        loop {
            if cancel.is_cancelled() || account.shutdown.is_cancelled() {
                let _ = conn.logout().await;
                return;
            }
            // Each IDLE round gets a child cancel token so a resubscribe
            // notification (a new scope was added) can break this IDLE
            // cleanly; the outer loop then re-runs `choose_idle_folder`,
            // letting the newly-subscribed folder be picked up instead of
            // waiting for this connection to break.
            let round_cancel = cancel.child_token();
            let notify_cancel = round_cancel.clone();
            let resubscribe_for_round = std::sync::Arc::clone(&resubscribe);
            let nudge = tokio::spawn(async move {
                tokio::select! {
                    () = resubscribe_for_round.notified() => notify_cancel.cancel(),
                    () = notify_cancel.cancelled() => {}
                }
            });
            let idle_result = conn
                .idle(account.config.idle_timeout, round_cancel.clone())
                .await;
            let interrupted_by_resubscribe = round_cancel.is_cancelled() && !cancel.is_cancelled();
            nudge.abort();
            match idle_result {
                Ok(_) if interrupted_by_resubscribe => {
                    // A new scope arrived: redial and re-choose the folder.
                    break;
                }
                Ok(event) => {
                    if absorb_idle_event(&account, &folder, uidvalidity, &event).is_err() {
                        let _ = account.push.tx.send(invalidated(HintPayload::Unknown));
                    }
                    // A server BYE (or any server-initiated termination)
                    // closes the connection: surface it as a disconnect
                    // and tear down this IDLE so the outer loop redials,
                    // rather than reporting an `Unknown` invalidation and
                    // spinning `idle()` on a dead socket until it errors.
                    if event_closes_connection(&event) {
                        let _ = account.push.tx.send(WatchEvent::Disconnected);
                        was_disconnected = true;
                        break;
                    }
                    if let Some(event) = map_idle_event(event, &folder) {
                        let _ = account.push.tx.send(event);
                    }
                }
                Err(_) => {
                    let _ = account.push.tx.send(WatchEvent::Disconnected);
                    was_disconnected = true;
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

/// Whether an IDLE event signals that the server is closing this
/// connection. A `BYE` or server-initiated termination leaves the socket
/// unusable; the push loop must redial rather than keep issuing `idle()`.
pub(crate) fn event_closes_connection(event: &IdleEvent) -> bool {
    matches!(event, IdleEvent::Bye { .. } | IdleEvent::ServerTerminated)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bye_and_server_termination_close_the_connection() {
        assert!(event_closes_connection(&IdleEvent::Bye {
            code: None,
            text: "logging out".to_string(),
        }));
        assert!(event_closes_connection(&IdleEvent::ServerTerminated));
        assert!(!event_closes_connection(&IdleEvent::Exists(3)));
        assert!(!event_closes_connection(&IdleEvent::Timeout));
    }
}
