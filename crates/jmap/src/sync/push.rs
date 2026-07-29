use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
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

/// Routing snapshot for push notifications: which JMAP `accountId` is the
/// primary, and which seeded `Folder` cursor scopes each foreign
/// (shared/delegate) account's state changes drive.
///
/// RFC 8620 s7.1 keys `StateChange.changed` by `accountId`, so a
/// notification names the account whose state moved. The engine's cursor
/// topology is derived from the same seed snapshot at attach, so the two
/// views cannot drift within a session; a share granted after open is
/// invisible to both until reopen (the documented foreign-lifecycle
/// limit).
pub(crate) struct PushRouting {
    primary_account_id: String,
    /// Foreign JMAP `accountId` -> every seeded `Folder` scope in it.
    foreign_scopes: HashMap<String, Vec<CursorScope>>,
}

impl PushRouting {
    pub(crate) fn new<'a>(
        primary_account_id: String,
        seed_scopes: impl Iterator<Item = &'a CursorScope>,
    ) -> Self {
        let mut foreign_scopes: HashMap<String, Vec<CursorScope>> = HashMap::new();
        for scope in seed_scopes {
            if let CursorScope::Folder(folder) = scope
                && let Some(parsed) = super::foreign::parse_foreign(folder)
            {
                foreign_scopes
                    .entry(parsed.account_id)
                    .or_default()
                    .push(scope.clone());
            }
        }
        Self {
            primary_account_id,
            foreign_scopes,
        }
    }
}

pub(crate) struct WsState {
    pub(crate) tx: broadcast::Sender<WatchEvent>,
    pub(crate) enabled: Arc<Mutex<DataTypeSet>>,
}

// pub: re-exported through crate::sync for AccountFactory reconnect tuning.
#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    pub initial: Duration,
    pub max: Duration,
    /// Upper bound on each pre-read setup await in the push reader: the
    /// WebSocket handshake and the push re-enable frame that follows it.
    ///
    /// Neither has a protocol-level deadline, and both can park on a
    /// half-open socket indefinitely. Without a bound a wedged handshake
    /// stalls push for the life of the session without ever reaching the
    /// reconnect backoff, and a wedged re-enable holds the client's
    /// WebSocket sink lock, blocking every `push_subscribe` caller too.
    /// Exceeding the bound is treated as a transient disconnect.
    pub connect_timeout: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(30),
        }
    }
}

/// The client operations the push reader awaits outside its read loop.
///
/// The sync layer hardwires `ReqwestTransport` into `Client`, so the
/// reader has no in-process seam of its own; this trait is the narrow one
/// that makes the reader's shutdown and timeout behavior testable without
/// a socket. It carries nothing the reader does not already call.
pub(crate) trait PushTransport: Send + Sync + 'static {
    type Stream: futures::Stream<Item = crate::Result<crate::client_ws::WebSocketMessage>>
        + Send
        + Unpin;

    /// Perform the WebSocket handshake and yield the message stream.
    fn connect_push(&self) -> impl Future<Output = crate::Result<Self::Stream>> + Send;

    /// Apply `data_types` as the connection's push subscription.
    fn set_push_data_types(
        &self,
        data_types: &DataTypeSet,
    ) -> impl Future<Output = Result<(), AccountError>> + Send;
}

type BoxedWsStream =
    Pin<Box<dyn futures::Stream<Item = crate::Result<crate::client_ws::WebSocketMessage>> + Send>>;

impl PushTransport for Client {
    type Stream = BoxedWsStream;

    async fn connect_push(&self) -> crate::Result<Self::Stream> {
        let stream: Self::Stream = self.connect_ws().await?;
        Ok(stream)
    }

    fn set_push_data_types(
        &self,
        data_types: &DataTypeSet,
    ) -> impl Future<Output = Result<(), AccountError>> + Send {
        apply_push_set(self, data_types)
    }
}

impl WsState {
    pub(crate) fn spawn(
        client: Client,
        push_available: bool,
        shutdown: CancellationToken,
        policy: ReconnectPolicy,
        routing: Arc<PushRouting>,
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
                routing,
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
        // Push subscriptions are per data type, not per mailbox. An Email
        // subscription covers every account visible to this session,
        // including delegate/shared accounts represented by Folder scopes.
        CursorScope::Folder(_) => Some(DataType::Email),
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

/// How one connect-read pass of the reader ended.
enum ReaderStep {
    /// Shutdown was requested. Stop without emitting anything further.
    Stop,
    /// Terminal classification: emit `Terminated` and stop the reader so
    /// the engine can reopen the account.
    Terminal(AccountError),
    /// Transient: emit `Disconnected` and back off. `reset_backoff` is
    /// set only when the pass got all the way to a live, subscribed
    /// connection, which is the evidence that the endpoint is healthy and
    /// the backoff should start over.
    Retry { reset_backoff: bool },
}

/// Await `future` under the reader's shutdown token and a wall-clock
/// bound, yielding `None` if either fires first.
///
/// Every await the reader performs before it reaches its (already
/// cancellation-covered) read loop goes through here. Dropping the
/// in-flight future is what makes `close()` prompt: otherwise a handshake
/// against a black-holed peer, or a re-enable frame on a half-open
/// socket, keeps the detached reader task and the client resources it
/// borrows alive long after the account is gone.
async fn bounded<F: Future>(
    shutdown: &CancellationToken,
    timeout: Duration,
    future: F,
) -> Option<F::Output> {
    tokio::select! {
        () = shutdown.cancelled() => None,
        () = tokio::time::sleep(timeout) => None,
        output = future => Some(output),
    }
}

/// Disambiguate a `bounded` miss: cancellation stops the reader, a
/// timeout is just another transient failure to reconnect through.
fn interrupted(shutdown: &CancellationToken) -> ReaderStep {
    if shutdown.is_cancelled() {
        ReaderStep::Stop
    } else {
        ReaderStep::Retry {
            reset_backoff: false,
        }
    }
}

async fn reader_loop<T: PushTransport>(
    transport: T,
    tx: broadcast::Sender<WatchEvent>,
    enabled: Arc<Mutex<DataTypeSet>>,
    shutdown: CancellationToken,
    policy: ReconnectPolicy,
    routing: Arc<PushRouting>,
) {
    let mut backoff = policy.initial;
    loop {
        if shutdown.is_cancelled() {
            break;
        }

        match reader_pass(&transport, &tx, &enabled, &shutdown, policy, &routing).await {
            ReaderStep::Stop => break,
            ReaderStep::Terminal(err) => {
                let _ = tx.send(WatchEvent::Terminated(err));
                break;
            }
            ReaderStep::Retry { reset_backoff } => {
                if reset_backoff {
                    backoff = policy.initial;
                }
                let _ = tx.send(WatchEvent::Disconnected);
            }
        }

        tokio::select! {
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = std::cmp::min(backoff.saturating_mul(2), policy.max);
    }
}

/// Connect, re-apply the push subscription, and read until the link ends.
async fn reader_pass<T: PushTransport>(
    transport: &T,
    tx: &broadcast::Sender<WatchEvent>,
    enabled: &Arc<Mutex<DataTypeSet>>,
    shutdown: &CancellationToken,
    policy: ReconnectPolicy,
    routing: &PushRouting,
) -> ReaderStep {
    let mut ws = match bounded(shutdown, policy.connect_timeout, transport.connect_push()).await {
        None => return interrupted(shutdown),
        Some(Err(err)) => {
            // Pre-handshake failure (`Error::WebSocketHandshake` or any
            // other connect-time error). Classify so terminal classes
            // (auth lost, etc.) surface as `Terminated` instead of an
            // endless silent reconnect.
            let acct = super::error::into_account_error(
                err,
                super::error::JmapErrorContext::new(AccountOperation::PushStream),
            );
            return if acct.recovery().is_terminal() {
                ReaderStep::Terminal(acct)
            } else {
                ReaderStep::Retry {
                    reset_backoff: false,
                }
            };
        }
        Some(Ok(ws)) => ws,
    };

    // A re-enable that never completes means the sink is wedged even
    // though the handshake answered, so drop the connection rather than
    // read from a link whose subscription was never applied.
    if bounded(
        shutdown,
        policy.connect_timeout,
        reenable_current_push_set(transport, enabled),
    )
    .await
    .is_none()
    {
        return interrupted(shutdown);
    }

    let _ = tx.send(WatchEvent::Reconnected);

    loop {
        let message = tokio::select! {
            () = shutdown.cancelled() => return ReaderStep::Stop,
            message = futures::StreamExt::next(&mut ws) => message,
        };
        let Some(message) = message else {
            break;
        };
        match message {
            Ok(crate::client_ws::WebSocketMessage::PushNotification(push)) => {
                emit_push(push, tx, routing);
            }
            Ok(crate::client_ws::WebSocketMessage::Response(_)) => {}
            Err(err) => {
                // Classify every exit error so consumers learn whether
                // the loop ended for an auth-lost / schema-mismatch
                // reason or for a transient drop. The previous shape
                // (`Err(_) => break`) erased the signal.
                let acct = super::error::into_account_error(
                    err,
                    super::error::JmapErrorContext::new(AccountOperation::PushStream),
                );
                if acct.recovery().is_terminal() {
                    return ReaderStep::Terminal(acct);
                }
                break;
            }
        }
    }

    if shutdown.is_cancelled() {
        return ReaderStep::Stop;
    }
    ReaderStep::Retry {
        reset_backoff: true,
    }
}

async fn reenable_current_push_set<T: PushTransport>(
    transport: &T,
    enabled: &Arc<Mutex<DataTypeSet>>,
) {
    let current = {
        let guard = enabled.lock().await;
        guard.clone()
    };
    let _ = transport.set_push_data_types(&current).await;
}

fn emit_push(push: PushObject, tx: &broadcast::Sender<WatchEvent>, routing: &PushRouting) {
    match push {
        PushObject::StateChange { changed } => {
            // RFC 8620 s7.1: `changed` is keyed by `accountId`. Route each
            // entry to the cursor scopes that account actually drives
            // instead of collapsing every account onto the primary type
            // scopes.
            for (account_id, by_type) in &changed {
                for data_type in by_type.keys() {
                    emit_state_change(account_id, data_type, tx, routing);
                }
            }
        }
        PushObject::Group { entries } => {
            for entry in entries {
                emit_push(entry, tx, routing);
            }
        }
        _ => {
            let _ = tx.send(invalidated(HintPayload::Unknown));
        }
    }
}

fn emit_state_change(
    account_id: &str,
    data_type: &DataType,
    tx: &broadcast::Sender<WatchEvent>,
    routing: &PushRouting,
) {
    if account_id == routing.primary_account_id {
        let payload = scope_for_data_type(data_type)
            .map(HintPayload::SpecificCursorScope)
            .unwrap_or(HintPayload::Unknown);
        let _ = tx.send(invalidated(payload));
        return;
    }
    let Some(scopes) = routing.foreign_scopes.get(account_id) else {
        // An accountId this session never seeded: a probe-skipped share
        // or one granted after open. JMAP state is per-(accountId, type),
        // so no registered cursor's state can have moved - there is
        // nothing to invalidate, and a broad repoll would gain nothing.
        return;
    };
    if !matches!(data_type, DataType::Email) {
        // Foreign Mailbox / Thread state is tracked by no cursor: foreign
        // mailbox lifecycle is reopen-only by design and no foreign
        // Thread scope exists. A count-only Mailbox bump arrives alongside
        // the Email entry that caused it, which is routed below; degrading
        // to `Unknown` here would full-repoll the whole account on every
        // foreign delivery and defeat the narrow routing.
        return;
    }
    // A foreign account's Email state is account-wide, and each of its
    // mailboxes syncs as its own `Folder` cursor scope, so every seeded
    // scope of the account is invalidated. The engine's reconciler skips
    // any scope without a registered cursor, so a hint for a quarantined
    // scope is a no-op.
    for scope in scopes {
        let _ = tx.send(invalidated(HintPayload::SpecificCursorScope(scope.clone())));
    }
}

fn invalidated(payload: HintPayload) -> WatchEvent {
    WatchEvent::Invalidated {
        hint: InvalidationHint {
            source: PushSource::JmapStateChange,
            payload,
        },
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

    fn foreign_scope(account_id: &str, mailbox_id: &str) -> CursorScope {
        CursorScope::Folder(super::super::foreign::encode_foreign(
            account_id, mailbox_id,
        ))
    }

    /// A primary account plus one seeded foreign account with two
    /// mailboxes - the same snapshot `factory.rs::open` builds from
    /// `seed_states`.
    fn routing() -> PushRouting {
        let seeds = [
            foreign_scope("acct-foreign", "mbx-inbox"),
            foreign_scope("acct-foreign", "mbx-archive"),
        ];
        PushRouting::new("acct-primary".to_string(), seeds.iter())
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
        emit_push(
            state_change("acct-primary", DataType::Email, "s2"),
            &tx,
            &routing(),
        );

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
        emit_push(
            state_change("acct-primary", DataType::Principal, "s2"),
            &tx,
            &routing(),
        );

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
            &routing(),
        );

        let events = drain(&mut rx);
        assert_eq!(events.len(), 2);
        let scopes = events.iter().filter_map(hinted_scope).collect::<Vec<_>>();
        assert!(scopes.contains(&CursorScope::Type(ObjectType::Email)));
        assert!(scopes.contains(&CursorScope::Type(ObjectType::Mailbox)));
    }

    // RFC 8620 s7.1 keys `StateChange.changed` by `accountId`. A foreign
    // account's Email state is account-wide, and each of its mailboxes
    // syncs as its own `Folder` cursor scope, so a foreign Email change
    // must invalidate every seeded `Folder(accountId, *)` scope - and
    // must NOT touch the primary `Type(Email)` scope, which did not
    // change.
    #[test]
    fn a_foreign_account_email_change_invalidates_each_seeded_folder_scope() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-foreign", DataType::Email, "s2"),
            &tx,
            &routing(),
        );

        let events = drain(&mut rx);
        let scopes = events.iter().filter_map(hinted_scope).collect::<Vec<_>>();
        assert_eq!(scopes.len(), 2);
        assert!(scopes.contains(&foreign_scope("acct-foreign", "mbx-inbox")));
        assert!(scopes.contains(&foreign_scope("acct-foreign", "mbx-archive")));
        assert!(
            !scopes.contains(&CursorScope::Type(ObjectType::Email)),
            "the primary scope did not change and must not be repolled"
        );
    }

    // Foreign Mailbox state is tracked by no cursor (foreign mailbox
    // lifecycle is reopen-only by design). Count-only Mailbox bumps ride
    // alongside the Email entry that caused them, so emitting anything
    // here - especially `Unknown`, a full-account repoll - would be
    // noise on every foreign delivery.
    #[test]
    fn a_foreign_mailbox_change_emits_no_hint() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-foreign", DataType::Mailbox, "m2"),
            &tx,
            &routing(),
        );

        assert!(drain(&mut rx).is_empty());
    }

    // An accountId this session never seeded (probe-skipped share, or
    // one granted after open) drives no registered cursor: JMAP state is
    // per-(accountId, type), so nothing this session tracks can have
    // moved.
    #[test]
    fn an_unseeded_account_change_emits_no_hint() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-stranger", DataType::Email, "s2"),
            &tx,
            &routing(),
        );

        assert!(drain(&mut rx).is_empty());
    }

    #[test]
    fn a_mixed_group_routes_each_entry_to_its_own_account() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            PushObject::Group {
                entries: vec![
                    state_change("acct-primary", DataType::Email, "s2"),
                    state_change("acct-foreign", DataType::Email, "f2"),
                ],
            },
            &tx,
            &routing(),
        );

        let scopes = drain(&mut rx)
            .iter()
            .filter_map(hinted_scope)
            .collect::<Vec<_>>();
        assert_eq!(scopes.len(), 3);
        assert!(scopes.contains(&CursorScope::Type(ObjectType::Email)));
        assert!(scopes.contains(&foreign_scope("acct-foreign", "mbx-inbox")));
        assert!(scopes.contains(&foreign_scope("acct-foreign", "mbx-archive")));
    }

    // `PushRouting::new` buckets only foreign-encoded `Folder` scopes;
    // primary `Type(_)` seeds and a codec-less `Folder` id contribute
    // nothing.
    #[test]
    fn push_routing_buckets_only_foreign_folder_scopes() {
        let seeds = [
            CursorScope::Type(ObjectType::Email),
            CursorScope::Folder(bifrost_types::FolderId("no-separator".to_string())),
            foreign_scope("acct-a", "m1"),
            foreign_scope("acct-a", "m2"),
            foreign_scope("acct-b", "m1"),
        ];
        let routing = PushRouting::new("acct-primary".to_string(), seeds.iter());
        assert_eq!(routing.foreign_scopes.len(), 2);
        assert_eq!(routing.foreign_scopes["acct-a"].len(), 2);
        assert_eq!(routing.foreign_scopes["acct-b"].len(), 1);
    }

    #[test]
    fn a_foreign_folder_scope_maps_to_email_push_data_type() {
        let folder = CursorScope::Folder(super::super::foreign::encode_foreign("acct-9", "mbx-1"));
        assert_eq!(data_type_for_scope(&folder), Some(DataType::Email));

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

    /// Which setup await the stub transport parks on forever.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Hang {
        Handshake,
        Reenable,
    }

    /// Push transport whose chosen setup await never resolves.
    ///
    /// `entered` gains a permit the instant the reader parks inside the
    /// await under test, so a test cancels from *inside* that await
    /// rather than racing the spawn (a cancel that lands before the task
    /// runs would exit at the loop's top-of-iteration check and prove
    /// nothing).
    struct HangingTransport {
        hang: Hang,
        entered: Arc<tokio::sync::Semaphore>,
    }

    impl PushTransport for HangingTransport {
        type Stream = BoxedWsStream;

        fn connect_push(&self) -> impl Future<Output = crate::Result<Self::Stream>> + Send {
            let entered = Arc::clone(&self.entered);
            let hang = self.hang == Hang::Handshake;
            async move {
                if hang {
                    entered.add_permits(1);
                    std::future::pending::<()>().await;
                }
                // A connection that is up but silent: the reader must
                // reach its next await, not fall out of the read loop.
                let stream: Self::Stream = Box::pin(futures::stream::pending());
                Ok(stream)
            }
        }

        fn set_push_data_types(
            &self,
            _data_types: &DataTypeSet,
        ) -> impl Future<Output = Result<(), AccountError>> + Send {
            let entered = Arc::clone(&self.entered);
            let hang = self.hang == Hang::Reenable;
            async move {
                if hang {
                    entered.add_permits(1);
                    std::future::pending::<()>().await;
                }
                Ok(())
            }
        }
    }

    /// Spawn a reader on a stub that hangs at `hang`, wait until it is
    /// parked in that await, then cancel and require the task to finish.
    ///
    /// The `connect_timeout` is set far beyond the assertion window so
    /// cancellation is the only thing that can end the reader; the outer
    /// bound only fires when the fix is absent.
    async fn assert_cancel_unblocks(hang: Hang) {
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let shutdown = CancellationToken::new();
        let (tx, _rx) = broadcast::channel(8);
        let reader = tokio::spawn(reader_loop(
            HangingTransport {
                hang,
                entered: Arc::clone(&entered),
            },
            tx,
            Arc::new(Mutex::new(HashSet::new())),
            shutdown.clone(),
            ReconnectPolicy {
                initial: Duration::from_secs(3600),
                max: Duration::from_secs(3600),
                connect_timeout: Duration::from_secs(3600),
            },
            Arc::new(routing()),
        ));

        let _permit = entered.acquire().await.expect("reader reached the await");
        shutdown.cancel();

        tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("shutdown must drop the in-flight setup future, not wait on the network")
            .expect("reader task panicked");
    }

    // `close()` cancels the shutdown token while the reader may be parked
    // anywhere in its lifecycle. The handshake has no protocol deadline,
    // so a cancel that only takes effect between connections leaves the
    // detached task and its client resources alive indefinitely.
    #[tokio::test]
    async fn a_shutdown_during_the_websocket_handshake_ends_the_reader() {
        assert_cancel_unblocks(Hang::Handshake).await;
    }

    // Same for the push re-enable that follows a successful handshake:
    // it awaits a WebSocket send holding the client's sink lock, so a
    // half-open socket parks the reader there just as indefinitely.
    #[tokio::test]
    async fn a_shutdown_during_the_push_re_enable_ends_the_reader() {
        assert_cancel_unblocks(Hang::Reenable).await;
    }

    // Independently of shutdown: a handshake that never answers must
    // fall to the reconnect backoff rather than stall push for the life
    // of the session. Time is paused, so the bound elapses without any
    // wall-clock wait.
    #[tokio::test(start_paused = true)]
    async fn a_handshake_that_outlives_its_bound_falls_to_the_reconnect_backoff() {
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let shutdown = CancellationToken::new();
        let (tx, mut rx) = broadcast::channel(8);
        let reader = tokio::spawn(reader_loop(
            HangingTransport {
                hang: Hang::Handshake,
                entered,
            },
            tx,
            Arc::new(Mutex::new(HashSet::new())),
            shutdown.clone(),
            ReconnectPolicy {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(60),
                connect_timeout: Duration::from_secs(30),
            },
            Arc::new(routing()),
        ));

        assert!(
            matches!(rx.recv().await, Ok(WatchEvent::Disconnected)),
            "an unbounded handshake must surface as a transient disconnect"
        );

        shutdown.cancel();
        reader.await.expect("reader task panicked");
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
