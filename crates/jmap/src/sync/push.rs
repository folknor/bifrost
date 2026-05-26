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

                while let Some(message) = futures::StreamExt::next(&mut ws).await {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    match message {
                        Ok(crate::client_ws::WebSocketMessage::PushNotification(push)) => {
                            emit_push(push, &tx);
                        }
                        Ok(crate::client_ws::WebSocketMessage::Response(_)) => {}
                        Err(_) => break,
                    }
                }

                let _ = tx.send(WatchEvent::Disconnected);
            }
            Err(_) => {
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
