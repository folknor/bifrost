use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AccountStream, CursorScope, HintPayload,
    InvalidationHint, ObjectType, PushCapability, PushSource, SubscriptionHandle, WatchEvent,
};

use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;

use crate::client::Client;
use crate::{DataType, PushObject};

pub(crate) type DataTypeSet = HashSet<DataType>;

pub(crate) struct WsState {
    pub(crate) tx: broadcast::Sender<WatchEvent>,
    pub(crate) enabled: Arc<Mutex<DataTypeSet>>,
}

// pub: re-exported through crate::sync for AccountFactory reconnect tuning.
#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    pub initial: Duration,
    pub max: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
        }
    }
}

impl WsState {
    pub(crate) fn spawn(
        client: Client,
        push_available: bool,
        shutdown: CancellationToken,
        policy: ReconnectPolicy,
    ) -> Self {
        let (tx, _) = broadcast::channel(256);
        let enabled = Arc::new(Mutex::new(HashSet::new()));
        if push_available {
            let _reader = tokio::spawn(reader_loop(
                client,
                tx.clone(),
                Arc::clone(&enabled),
                shutdown,
                policy,
            ));
        }

        Self { tx, enabled }
    }
}

pub(crate) fn stream(
    mut rx: broadcast::Receiver<WatchEvent>,
    shutdown: CancellationToken,
) -> AccountStream<WatchEvent> {
    Box::pin(async_stream::stream! {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                result = rx.recv() => {
                    match result {
                        Ok(event) => yield event,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            yield WatchEvent::Invalidated {
                                hint: InvalidationHint {
                                    source: PushSource::Coalesced,
                                    payload: HintPayload::Unknown,
                                },
                            };
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    })
}

pub(crate) fn subscribe(
    client: Client,
    push: PushCapability,
    handle: SubscriptionHandle,
    scopes: Vec<CursorScope>,
    subscriptions: Arc<Mutex<HashMap<SubscriptionHandle, DataTypeSet>>>,
    enabled: Arc<Mutex<DataTypeSet>>,
) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
    Box::pin(async move {
        if push != PushCapability::InProcess {
            return Err(super::error::unsupported_error(
                AccountOperation::PushSubscribe,
                None,
                "JMAP push requires WebSocket subprotocol support",
            ));
        }

        let data_types = scopes
            .iter()
            .filter_map(data_type_for_scope)
            .collect::<DataTypeSet>();

        if data_types.is_empty() {
            return Err(super::error::unsupported_error(
                AccountOperation::PushSubscribe,
                None,
                "JMAP push subscribe requires at least one supported scope",
            ));
        }

        let union = {
            let mut guard = subscriptions.lock().await;
            guard.insert(handle.clone(), data_types);
            union_data_types(&guard)
        };

        set_enabled_data_types(&enabled, union.clone()).await;
        apply_push_set(&client, &union).await?;
        Ok(handle)
    })
}

pub(crate) fn unsubscribe(
    client: Client,
    handle: SubscriptionHandle,
    subscriptions: Arc<Mutex<HashMap<SubscriptionHandle, DataTypeSet>>>,
    enabled: Arc<Mutex<DataTypeSet>>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let union = {
            let mut guard = subscriptions.lock().await;
            guard.remove(&handle);
            union_data_types(&guard)
        };

        set_enabled_data_types(&enabled, union.clone()).await;
        apply_push_set(&client, &union).await
    })
}

fn data_type_for_scope(scope: &CursorScope) -> Option<DataType> {
    match scope {
        CursorScope::Type(ObjectType::Email) => Some(DataType::Email),
        CursorScope::Type(ObjectType::Mailbox) => Some(DataType::Mailbox),
        CursorScope::Type(ObjectType::Thread) => Some(DataType::Thread),
        _ => None,
    }
}

fn scope_for_data_type(data_type: &DataType) -> Option<CursorScope> {
    match data_type {
        DataType::Email => Some(CursorScope::Type(ObjectType::Email)),
        DataType::Mailbox => Some(CursorScope::Type(ObjectType::Mailbox)),
        DataType::Thread => Some(CursorScope::Type(ObjectType::Thread)),
        _ => None,
    }
}

fn union_data_types(subscriptions: &HashMap<SubscriptionHandle, DataTypeSet>) -> DataTypeSet {
    let mut union = HashSet::new();
    for values in subscriptions.values() {
        for value in values {
            union.insert(value.clone());
        }
    }
    union
}

async fn set_enabled_data_types(enabled: &Arc<Mutex<DataTypeSet>>, data_types: DataTypeSet) {
    let mut guard = enabled.lock().await;
    *guard = data_types;
}

async fn apply_push_set(client: &Client, data_types: &DataTypeSet) -> Result<(), AccountError> {
    let result = if data_types.is_empty() {
        client.disable_push_ws().await
    } else {
        let values = data_types.iter().cloned().collect::<Vec<_>>();
        client.enable_push_ws(Some(values), None::<String>).await
    };

    match result {
        Ok(()) => Ok(()),
        Err(crate::Error::WebSocketNotConnected) => Err(super::error::unsupported_error(
            AccountOperation::PushSubscribe,
            None,
            "JMAP push: WebSocket not connected",
        )),
        Err(err) => Err(super::error::into_account_error(
            err,
            super::error::JmapErrorContext::new(AccountOperation::PushSubscribe),
        )),
    }
}

async fn reader_loop(
    client: Client,
    tx: broadcast::Sender<WatchEvent>,
    enabled: Arc<Mutex<DataTypeSet>>,
    shutdown: CancellationToken,
    policy: ReconnectPolicy,
) {
    let mut backoff = policy.initial;
    loop {
        if shutdown.is_cancelled() {
            break;
        }

        match client.connect_ws().await {
            Ok(mut ws) => {
                reenable_current_push_set(&client, &enabled).await;
                let _ = tx.send(WatchEvent::Reconnected);
                backoff = policy.initial;

                let mut terminal_err: Option<bifrost_types::AccountError> = None;
                while let Some(message) = futures::StreamExt::next(&mut ws).await {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    match message {
                        Ok(crate::client_ws::WebSocketMessage::PushNotification(push)) => {
                            emit_push(push, &tx);
                        }
                        Ok(crate::client_ws::WebSocketMessage::Response(_)) => {}
                        Err(err) => {
                            // Classify every exit error so consumers
                            // learn whether the loop ended for an
                            // auth-lost / schema-mismatch reason or for
                            // a transient drop. The previous shape
                            // (`Err(_) => break`) erased the signal.
                            let acct = super::error::into_account_error(
                                err,
                                super::error::JmapErrorContext::new(AccountOperation::PushStream),
                            );
                            if acct.recovery().is_terminal() {
                                terminal_err = Some(acct);
                            }
                            break;
                        }
                    }
                }

                if let Some(err) = terminal_err {
                    // Terminal class: emit `Terminated(AccountError)`
                    // and stop the reader. The engine reads
                    // `recovery()` and decides what to do next.
                    let _ = tx.send(WatchEvent::Terminated(err));
                    break;
                }

                let _ = tx.send(WatchEvent::Disconnected);
            }
            Err(err) => {
                // Pre-handshake failure (`Error::WebSocketHandshake` or
                // any other connect-time error). Classify and emit
                // `Terminated` for terminal classes (auth lost, etc.)
                // so consumers see what stopped the push reader.
                let acct = super::error::into_account_error(
                    err,
                    super::error::JmapErrorContext::new(AccountOperation::PushStream),
                );
                if acct.recovery().is_terminal() {
                    let _ = tx.send(WatchEvent::Terminated(acct));
                    break;
                }
                let _ = tx.send(WatchEvent::Disconnected);
            }
        }

        tokio::time::sleep(backoff).await;
        backoff = std::cmp::min(backoff.saturating_mul(2), policy.max);
    }
}

async fn reenable_current_push_set(client: &Client, enabled: &Arc<Mutex<DataTypeSet>>) {
    let current = {
        let guard = enabled.lock().await;
        guard.clone()
    };
    let _ = apply_push_set(client, &current).await;
}

fn emit_push(push: PushObject, tx: &broadcast::Sender<WatchEvent>) {
    match push {
        PushObject::StateChange { changed } => {
            for by_type in changed.values() {
                for data_type in by_type.keys() {
                    let payload = scope_for_data_type(data_type)
                        .map(HintPayload::SpecificCursorScope)
                        .unwrap_or(HintPayload::Unknown);
                    let _ = tx.send(WatchEvent::Invalidated {
                        hint: InvalidationHint {
                            source: PushSource::JmapStateChange,
                            payload,
                        },
                    });
                }
            }
        }
        PushObject::Group { entries } => {
            for entry in entries {
                emit_push(entry, tx);
            }
        }
        _ => {
            let _ = tx.send(WatchEvent::Invalidated {
                hint: InvalidationHint {
                    source: PushSource::JmapStateChange,
                    payload: HintPayload::Unknown,
                },
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn drain(rx: &mut broadcast::Receiver<WatchEvent>) -> Vec<WatchEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    fn state_change(account_id: &str, data_type: DataType, state: &str) -> PushObject {
        let mut changed = HashMap::new();
        let mut by_type = HashMap::new();
        by_type.insert(data_type, state.to_string());
        changed.insert(account_id.to_string(), by_type);
        PushObject::StateChange { changed }
    }

    fn hinted_scope(event: &WatchEvent) -> Option<CursorScope> {
        match event {
            WatchEvent::Invalidated { hint } => match &hint.payload {
                HintPayload::SpecificCursorScope(scope) => Some(scope.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    #[test]
    fn a_primary_email_state_change_invalidates_the_email_cursor_scope() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(state_change("acct-primary", DataType::Email, "s2"), &tx);

        let events = drain(&mut rx);
        assert_eq!(events.len(), 1);
        match &events[0] {
            WatchEvent::Invalidated { hint } => {
                assert_eq!(hint.source, PushSource::JmapStateChange);
            }
            other => panic!("expected Invalidated, got {other:?}"),
        }
        assert_eq!(
            hinted_scope(&events[0]),
            Some(CursorScope::Type(ObjectType::Email))
        );
    }

    #[test]
    fn a_data_type_with_no_cursor_scope_degrades_to_an_unknown_hint() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(state_change("acct-primary", DataType::Principal, "s2"), &tx);

        let events = drain(&mut rx);
        assert_eq!(events.len(), 1);
        assert_eq!(hinted_scope(&events[0]), None, "no scope maps to Principal");
    }

    #[test]
    fn a_grouped_push_fans_out_to_one_event_per_entry() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            PushObject::Group {
                entries: vec![
                    state_change("acct-primary", DataType::Email, "s2"),
                    state_change("acct-primary", DataType::Mailbox, "m2"),
                ],
            },
            &tx,
        );

        let events = drain(&mut rx);
        assert_eq!(events.len(), 2);
        let scopes = events.iter().filter_map(hinted_scope).collect::<Vec<_>>();
        assert!(scopes.contains(&CursorScope::Type(ObjectType::Email)));
        assert!(scopes.contains(&CursorScope::Type(ObjectType::Mailbox)));
    }

    // BUG, documented rather than endorsed. RFC 8620 s7.1 keys
    // `StateChange.changed` by `accountId`, and this crate syncs each
    // shared / delegate account's mailboxes as
    // `CursorScope::Folder(encode_foreign(accountId, mailboxId))`.
    // `emit_push` iterates `changed.values()` and throws the accountId
    // away, so a push announcing a change in a SHARED account is emitted
    // as an invalidation of the PRIMARY `Type(Email)` scope. Two
    // consequences: the shared account's `Folder` scopes are never
    // invalidated (they only refresh at the next reopen, which is what
    // "push is live" is supposed to prevent), and the primary scope is
    // repolled for a change that did not happen in it.
    //
    // Fix: carry the accountId through `emit_push` and, when it names a
    // registered foreign account, emit one hint per seeded
    // `Folder(accountId, *)` scope instead of the primary type scope.
    #[test]
    fn a_foreign_account_state_change_is_announced_as_a_primary_scope_change() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(state_change("acct-foreign", DataType::Email, "s2"), &tx);

        let events = drain(&mut rx);
        assert_eq!(events.len(), 1);
        assert_eq!(
            hinted_scope(&events[0]),
            Some(CursorScope::Type(ObjectType::Email)),
            "the owning accountId is discarded; the hint names the primary scope"
        );
    }

    // BUG, documented rather than endorsed. `push_subscribe` maps the
    // engine's scopes onto JMAP `DataType`s, and a foreign `Folder` scope
    // maps to nothing. A subscribe call carrying only foreign scopes is
    // rejected outright ("requires at least one supported scope"); a
    // mixed call silently drops the foreign half. Since JMAP WebSocket
    // push is subscribed per DataType and delivered for every account,
    // `Folder(_)` should map to `DataType::Email`.
    #[test]
    fn a_foreign_folder_scope_maps_to_no_push_data_type() {
        let folder = CursorScope::Folder(super::super::foreign::encode_foreign("acct-9", "mbx-1"));
        assert_eq!(data_type_for_scope(&folder), None);

        // Primary type scopes do map.
        assert_eq!(
            data_type_for_scope(&CursorScope::Type(ObjectType::Email)),
            Some(DataType::Email)
        );
        assert_eq!(
            data_type_for_scope(&CursorScope::Type(ObjectType::Mailbox)),
            Some(DataType::Mailbox)
        );
    }

    #[test]
    fn data_type_and_scope_mappings_are_inverse_for_the_supported_types() {
        for (scope, data_type) in [
            (CursorScope::Type(ObjectType::Email), DataType::Email),
            (CursorScope::Type(ObjectType::Mailbox), DataType::Mailbox),
            (CursorScope::Type(ObjectType::Thread), DataType::Thread),
        ] {
            assert_eq!(data_type_for_scope(&scope), Some(data_type.clone()));
            assert_eq!(scope_for_data_type(&data_type), Some(scope));
        }
    }

    #[test]
    fn the_enabled_data_type_union_spans_every_live_subscription() {
        let mut subscriptions: HashMap<SubscriptionHandle, DataTypeSet> = HashMap::new();
        subscriptions.insert(
            SubscriptionHandle("a".to_string()),
            [DataType::Email].into_iter().collect(),
        );
        subscriptions.insert(
            SubscriptionHandle("b".to_string()),
            [DataType::Email, DataType::Mailbox].into_iter().collect(),
        );

        let union = union_data_types(&subscriptions);
        assert_eq!(union.len(), 2);
        assert!(union.contains(&DataType::Email));
        assert!(union.contains(&DataType::Mailbox));

        // Dropping one handle leaves only the other's types, which is what
        // drives `apply_push_set` back down to a narrower subscription.
        subscriptions.remove(&SubscriptionHandle("b".to_string()));
        let union = union_data_types(&subscriptions);
        assert_eq!(union.len(), 1);
        assert!(union.contains(&DataType::Email));
    }

    #[tokio::test]
    async fn the_push_stream_ends_when_the_shutdown_token_is_cancelled() {
        let (tx, rx) = broadcast::channel(4);
        let shutdown = CancellationToken::new();
        let mut events = super::stream(rx, shutdown.clone());

        let _ = tx.send(WatchEvent::Reconnected);
        assert!(matches!(events.next().await, Some(WatchEvent::Reconnected)));

        shutdown.cancel();
        assert!(
            events.next().await.is_none(),
            "cancellation must terminate the subscriber, not just stop new sends"
        );
    }

    #[tokio::test]
    async fn a_lagged_broadcast_slot_coalesces_into_an_unknown_invalidation() {
        // A slow consumer must never lose a wake-up: an overflowed slot
        // becomes one coalesced invalidation so the engine full-repolls.
        let (tx, rx) = broadcast::channel(1);
        let shutdown = CancellationToken::new();
        let mut events = super::stream(rx, shutdown);

        let _ = tx.send(WatchEvent::Reconnected);
        let _ = tx.send(WatchEvent::Disconnected);

        match events.next().await {
            Some(WatchEvent::Invalidated { hint }) => {
                assert_eq!(hint.source, PushSource::Coalesced);
                assert!(matches!(hint.payload, HintPayload::Unknown));
            }
            other => panic!("expected a coalesced invalidation, got {other:?}"),
        }
    }
}
