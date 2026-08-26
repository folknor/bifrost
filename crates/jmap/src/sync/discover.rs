use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bifrost_types::{
    AccountStream, Batch, CursorScope, MembershipScope, PageBoundary, ScopeLifecycle,
    ScopeLifecycleEvent, SyncEvent,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::core::transport::HttpTransport;
use crate::mailbox::{Mailbox, MailboxChanges, MailboxGet, MailboxId, Property};

use super::capabilities::CoreLimits;
use super::state_cache::{self, StateMap};

type MailAccount<T> = crate::account::Account<T>;

pub(crate) fn cursor_scopes(scopes: Vec<CursorScope>) -> AccountStream<SyncEvent<CursorScope>> {
    Box::pin(async_stream::stream! {
        if !scopes.is_empty() {
            yield SyncEvent::Batch(Batch {
                items: scopes,
                page_boundary: PageBoundary::Final,
                server_latency: Duration::ZERO,
                // Synthetic: the scopes were derived from the session
                // already in hand, so this batch performed no request
                // and genuinely cost nothing.
                bytes_in: 0,
                checkpoint: None,
            });
        }
        yield SyncEvent::Done(None);
    })
}

pub(crate) fn memberships<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_owners: Vec<MembershipScope>,
) -> AccountStream<SyncEvent<MembershipScope>> {
    Box::pin(async_stream::stream! {
        let (mail, tally) = mail.metered();
        let started = Instant::now();
        let response = mail
            .call(MailboxGet::new().properties([Property::Id]))
            .await;

        match response {
            Ok(response) => {
                let mut items = response
                    .into_list()
                    .into_iter()
                    .filter_map(|mut mailbox| {
                        let id = mailbox.take_id();
                        if id.as_str().is_empty() {
                            None
                        } else {
                            Some(MembershipScope::Mailbox(bifrost_types::MailboxId(
                                id.into_string(),
                            )))
                        }
                    })
                    .collect::<Vec<_>>();

                // Each foreign (shared/delegate) account contributes its
                // owner tag (`Mailbox(accountId)`) so the consumer maps
                // its foreign scopes to the shared-account identity.
                items.extend(foreign_owners);

                if !items.is_empty() {
                    yield SyncEvent::Batch(Batch {
                        items,
                        page_boundary: PageBoundary::Final,
                        server_latency: started.elapsed(),
                        bytes_in: tally.take(),
                        checkpoint: None,
                    });
                }
                yield SyncEvent::Done(None);
            }
            Err(err) => {
                yield super::error::terminated_from_jmap(
                    err,
                    super::error::JmapErrorContext::new(bifrost_types::AccountOperation::DiscoverMemberships),
                );
            }
        }
    })
}

pub(crate) async fn fetch_mailbox_names<T: HttpTransport>(
    mail: &MailAccount<T>,
) -> crate::Result<(String, HashMap<String, String>)> {
    let response = mail
        .call(MailboxGet::new().properties([Property::Id, Property::Name]))
        .await?;
    let state = response.state().to_string();
    let mut names = HashMap::new();

    for mut mailbox in response.into_list() {
        let id = mailbox.take_id();
        if !id.as_str().is_empty() {
            names.insert(id.into_string(), mailbox.name().unwrap_or("").to_string());
        }
    }

    Ok((state, names))
}

/// Pause between lifecycle polls without outliving shutdown.
///
/// The stream is pull-based, so a bare sleep cannot leak a task - but a
/// consumer still polling after `close()` cancels the token would wait
/// out the rest of the interval before the loop's top-of-iteration check
/// ends the stream. Selecting on the token makes the end prompt by
/// construction; every call site falls through to that check, which is
/// where cancellation terminates the loop.
async fn poll_pause(shutdown: &CancellationToken) {
    tokio::select! {
        () = shutdown.cancelled() => {}
        () = tokio::time::sleep(POLL_INTERVAL) => {}
    }
}

const POLL_INTERVAL: Duration = Duration::from_secs(300);

pub(crate) fn scope_lifecycle<T: HttpTransport>(
    mail: MailAccount<T>,
    limits: CoreLimits,
    mailbox_states: StateMap,
    account_id: String,
    mailbox_names: Arc<Mutex<HashMap<String, String>>>,
    shutdown: CancellationToken,
    client: crate::client::Client<T>,
) -> AccountStream<ScopeLifecycleEvent> {
    Box::pin(async_stream::stream! {
        loop {
            if shutdown.is_cancelled() {
                break;
            }

            if !client.is_session_updated() {
                yield ScopeLifecycleEvent::Terminated(
                    super::capabilities::session_state_changed(),
                );
                break;
            }

            let since_state = state_cache::get(&mailbox_states, &account_id).await;

            let Some(since_state) = since_state else {
                poll_pause(&shutdown).await;
                continue;
            };

            let max_changes = NonZeroUsize::new(limits.max_objects_in_get.max(1));
            let Some(max_changes) = max_changes else {
                break;
            };

            let response = mail
                .call(MailboxChanges::new(since_state).max_changes(max_changes))
                .await;

            match response {
                Ok(response) => {
                    let created = response.created().to_vec();
                    let updated = response.updated().to_vec();
                    let destroyed = response.destroyed().to_vec();
                    let new_state = response.new_state().to_string();

                    let fetched = if !created.is_empty() || !updated.is_empty() {
                        match fetch_mailboxes(&mail, created.iter().chain(&updated)).await {
                            Ok(fetched) => fetched,
                            Err(err) => {
                                let acct = super::error::into_account_error(
                                    err,
                                    super::error::JmapErrorContext::new(
                                        bifrost_types::AccountOperation::ScopeLifecycle,
                                    ),
                                );
                                if acct.recovery().is_terminal()
                                    || acct.recovery().requires_engine_action()
                                {
                                    yield ScopeLifecycleEvent::Terminated(acct);
                                    break;
                                }
                                // Do not advance the changes state: retrying
                                // this response is the only way to preserve
                                // the created/renamed lifecycle event.
                                poll_pause(&shutdown).await;
                                continue;
                            }
                        }
                    } else {
                        Vec::new()
                    };

                    for mailbox in fetched {
                        let id = mailbox.id().map(ToString::to_string);
                        let name = mailbox.name().unwrap_or("").to_string();
                        let Some(id) = id else {
                            continue;
                        };

                        if created
                            .iter()
                            .any(|created_id| created_id.as_str() == id.as_str())
                        {
                            update_mailbox_name(&mailbox_names, id.clone(), name).await;
                            yield ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(MembershipScope::Mailbox(
                                bifrost_types::MailboxId(id),
                            )));
                        } else {
                            let old_name = replace_mailbox_name(
                                &mailbox_names,
                                id.clone(),
                                name.clone(),
                            )
                            .await;
                            if old_name.as_deref() != Some(name.as_str()) {
                                let scope = MembershipScope::Mailbox(
                                    bifrost_types::MailboxId(id),
                                );
                                yield ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Renamed {
                                    old: scope.clone(),
                                    new: scope,
                                });
                            }
                        }
                    }

                    for destroyed_id in destroyed {
                        let id = destroyed_id.into_string();
                        remove_mailbox_name(&mailbox_names, &id).await;
                        yield ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Deleted(MembershipScope::Mailbox(
                            bifrost_types::MailboxId(id),
                        )));
                    }

                    // Commit only after every follow-up needed to describe
                    // this changes response has completed successfully.
                    state_cache::set(&mailbox_states, &account_id, new_state).await;

                    if !response.has_more_changes() {
                        poll_pause(&shutdown).await;
                    }
                }
                Err(err) => {
                    // Classify rather than swallow. Terminal or
                    // engine-action classes (auth lost, capability
                    // changed, schema break) emit a structured
                    // `ScopeLifecycleEvent::Terminated(AccountError)`
                    // so the engine can route through `plan_recovery`
                    // (escalate to `Pause` after the reopen budget, or
                    // route an engine directive via the reopen
                    // channel). Retry classes still sleep-and-continue
                    // since transient transport/server hiccups are
                    // expected for a long-running poll.
                    let acct = super::error::into_account_error(
                        err,
                        super::error::JmapErrorContext::new(
                            bifrost_types::AccountOperation::ScopeLifecycle,
                        ),
                    );
                    if acct.recovery().is_terminal()
                        || acct.recovery().requires_engine_action()
                    {
                        yield ScopeLifecycleEvent::Terminated(acct);
                        break;
                    }
                    poll_pause(&shutdown).await;
                }
            }
        }
    })
}

async fn fetch_mailboxes<'a, T: HttpTransport>(
    mail: &MailAccount<T>,
    ids: impl Iterator<Item = &'a MailboxId>,
) -> crate::Result<Vec<Mailbox>> {
    let ids = ids.cloned().collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    Ok(mail
        .call(
            MailboxGet::new()
                .ids(ids)
                .properties([Property::Id, Property::Name]),
        )
        .await?
        .into_list())
}

async fn update_mailbox_name(
    names: &Arc<Mutex<HashMap<String, String>>>,
    id: String,
    name: String,
) {
    let mut guard = names.lock().await;
    guard.insert(id, name);
}

async fn replace_mailbox_name(
    names: &Arc<Mutex<HashMap<String, String>>>,
    id: String,
    name: String,
) -> Option<String> {
    let mut guard = names.lock().await;
    guard.insert(id, name)
}

async fn remove_mailbox_name(names: &Arc<Mutex<HashMap<String, String>>>, id: &str) {
    let mut guard = names.lock().await;
    guard.remove(id);
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;

    use bifrost_types::{AccountErrorKind, EngineDirective, RecoveryClass, SyncStateErrorKind};
    use futures::StreamExt;

    use super::*;
    use crate::core::transport::TransportError;

    /// A JMAP HTTP boundary answering a fixed script of response bodies.
    /// Requests are recorded; an unscripted request is an error rather
    /// than a plausible answer, so a loop that polls more than the test
    /// planned for cannot pass quietly.
    struct ScriptTransport {
        replies: StdMutex<VecDeque<String>>,
        requests: StdMutex<Vec<serde_json::Value>>,
    }

    impl ScriptTransport {
        fn new(replies: impl IntoIterator<Item = String>) -> Self {
            Self {
                replies: StdMutex::new(replies.into_iter().collect()),
                requests: StdMutex::new(Vec::new()),
            }
        }
    }

    impl HttpTransport for ScriptTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, TransportError> {
            let request: serde_json::Value =
                serde_json::from_slice(&body).expect("client emits JSON");
            self.requests
                .lock()
                .expect("recorded requests")
                .push(request);
            // Exhaustion answers a TERMINAL JMAP error rather than a
            // transport failure. The lifecycle loop retries transport
            // hiccups forever by design, so a transport-shaped
            // exhaustion would turn "the loop polled more than planned"
            // into a hang instead of a failed assertion.
            match self.replies.lock().expect("script").pop_front() {
                Some(reply) => Ok(bytes::Bytes::from(reply)),
                None => Ok(bytes::Bytes::from(
                    serde_json::json!({
                        "sessionState": "session-1",
                        "methodResponses": [[
                            "error",
                            {"type": "invalidArguments", "description": "script exhausted"},
                            "s0"
                        ]]
                    })
                    .to_string(),
                )),
            }
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, TransportError> {
            Err(TransportError::new("no upload reply"))
        }

        async fn download(&self, _url: &str) -> Result<bytes::Bytes, TransportError> {
            Err(TransportError::new("no download reply"))
        }

        async fn get_session(&self, _url: &str) -> Result<bytes::Bytes, TransportError> {
            Err(TransportError::new("no session reply"))
        }
    }

    fn test_session() -> crate::core::session::Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {"primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}},
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("test session parses")
    }

    /// One `Mailbox/changes` answer, stamped with whatever
    /// `sessionState` the test wants the server to claim.
    fn mailbox_changes_reply(session_state: &str, created: &[&str]) -> String {
        serde_json::json!({
            "sessionState": session_state,
            "methodResponses": [[
                "Mailbox/changes",
                {
                    "accountId": "primary",
                    "oldState": "mbx-1",
                    "newState": "mbx-2",
                    "hasMoreChanges": false,
                    "created": created,
                    "updated": [],
                    "destroyed": []
                },
                "s0"
            ]]
        })
        .to_string()
    }

    fn mailbox_get_reply(session_state: &str, id: &str, name: &str) -> String {
        serde_json::json!({
            "sessionState": session_state,
            "methodResponses": [[
                "Mailbox/get",
                {
                    "accountId": "primary",
                    "state": "mbx-2",
                    "list": [{"id": id, "name": name}],
                    "notFound": []
                },
                "s0"
            ]]
        })
        .to_string()
    }

    fn lifecycle_stream(
        replies: impl IntoIterator<Item = String>,
    ) -> (
        AccountStream<ScopeLifecycleEvent>,
        CancellationToken,
        StateMap,
    ) {
        let client = crate::client::Client::with_transport(
            ScriptTransport::new(replies),
            test_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let mail = crate::account::Account::new(client.clone(), "primary");
        let states: StateMap = Arc::new(Mutex::new(HashMap::from([(
            "primary".to_string(),
            Some("mbx-1".to_string()),
        )])));
        let shutdown = CancellationToken::new();
        let stream = scope_lifecycle(
            mail,
            CoreLimits {
                max_objects_in_get: 256,
                max_objects_in_set: 256,
            },
            Arc::clone(&states),
            "primary".to_string(),
            Arc::new(Mutex::new(HashMap::new())),
            shutdown.clone(),
            client,
        );
        (stream, shutdown, states)
    }

    /// The engine drives the scope-lifecycle stream for the whole life
    /// of an account, which makes it the one always-running consumer
    /// positioned to notice that the server has moved to a different
    /// session. It is not enough for the client's staleness flag to
    /// flip: `JmapAccount` froze its limits, capability set, primary and
    /// foreign routing, and push topology from the OLD session document,
    /// so the only correct consumer action is a full reopen. This pins
    /// the whole path - divergence on the wire, through the detector,
    /// into a classified `Terminated`, out as `RestartAccount`.
    #[tokio::test(start_paused = true)]
    async fn a_diverged_session_state_terminates_the_lifecycle_into_a_reopen() {
        // The server answers the first poll while claiming a session
        // the client has never read.
        let (stream, _shutdown, _states) =
            lifecycle_stream([mailbox_changes_reply("session-2", &[])]);
        let events: Vec<_> = stream.collect().await;

        match events.as_slice() {
            [ScopeLifecycleEvent::Terminated(err)] => {
                assert_eq!(
                    err.kind(),
                    &AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
                    "divergence is a capability-shift class, not a transport error"
                );
                assert_eq!(
                    err.recovery(),
                    &RecoveryClass::Engine(EngineDirective::RestartAccount),
                    "an in-place refresh cannot heal a frozen account; the engine must reopen"
                );
            }
            other => panic!("expected a single terminal reopen directive, got {other:?}"),
        }
    }

    /// The other half of the guard's bite: a server that keeps its
    /// session must not have its lifecycle stream torn down. Without
    /// this, a detector wired backwards - or one that fired on every
    /// pass - would still satisfy the divergence test above while
    /// making the account unusable.
    #[tokio::test]
    async fn a_matching_session_state_keeps_the_lifecycle_stream_running() {
        let (mut stream, shutdown, states) = lifecycle_stream([
            mailbox_changes_reply("session-1", &["mbx-new"]),
            mailbox_get_reply("session-1", "mbx-new", "Archive"),
        ]);

        match stream.next().await {
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Mailbox(id),
            ))) => assert_eq!(id.0, "mbx-new"),
            other => panic!("expected the created-mailbox lifecycle event, got {other:?}"),
        }
        // Cancelling before the next pull lets the loop run past its
        // commit and out through the top-of-iteration check, rather than
        // parking on the poll interval. That the stream ends here at all
        // is the cancellation guarantee `close()` depends on.
        shutdown.cancel();
        assert!(
            stream.next().await.is_none(),
            "a cancelled token ends the loop instead of sleeping out the interval"
        );
        // The poller committed its OWN position, and only after every
        // follow-up describing that response succeeded. Nothing else
        // writes this map, which is the whole reason it is separate from
        // the shared `mailbox_states` cache that a delta pass or a local
        // `Mailbox/set` may fast-forward past unseen lifecycle events.
        assert_eq!(
            state_cache::get(&states, "primary").await,
            Some("mbx-2".to_string()),
            "the lifecycle poller advances the map it was handed"
        );
    }
}
