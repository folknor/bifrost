//! Renewal sessions preserve every row's outcome and committed gap.
use super::fixtures::{email_scope, lease_expiring, webhook_account};
use crate::account::GraphAccount;
use crate::account::push::renewal::renewal_session;
use crate::client::ScriptedRestResponse;
use bifrost_types::{
    CursorScope, PushGate, PushSessionEnd as End, PushSessionEvent as E, SessionStop,
    SubscriptionHandle,
};
use futures::StreamExt;

/// A due row under handle `h` covering exactly `scopes`.
fn expiring_with(account: &GraphAccount, resource: &str, id: &str, scopes: Vec<CursorScope>) {
    account.webhooks.lease_for_tests(
        &SubscriptionHandle("h".into()),
        resource,
        id,
        "2000-01-01T00:00:00Z",
        scopes,
    );
}

fn row(account: &GraphAccount, resource: &str) -> crate::account::push::registry::Row {
    account.webhooks.store().rows_for_tests()[resource].clone()
}

#[tokio::test(start_paused = true)]
async fn a_terminal_renewal_failure_names_the_scope_that_lost_coverage() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account
        .mailboxes
        .primary()
        .script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::FORBIDDEN,
            serde_json::json!({"error":{"code":"ErrorAccessDenied","message":"denied"}}),
        )]);
    expiring_with(
        &account,
        "/me/mailFolders/inbox/messages",
        "old",
        vec![email_scope("inbox")],
    );
    // A second subscribe of the same scope: two leases, still one scope.
    account.webhooks.lease_for_tests(
        &SubscriptionHandle("h2".into()),
        "/me/mailFolders/inbox/messages",
        "old",
        "2000-01-01T00:00:00Z",
        vec![email_scope("inbox")],
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(
        matches!(session.next().await, Some(E::UnitFailed(e)) if e.recovery().is_terminal() && e.scope() == Some(&bifrost_types::ErrorScope::Cursor(email_scope("inbox"))))
    );
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    assert!(session.next().await.is_none());
    let mut next = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    let mut event = Box::pin(next.next());
    assert!(futures::poll!(&mut event).is_pending());
    assert_eq!(account.mailboxes.primary().take_rest_requests().len(), 1);
}

/// The recreate leg, end to end through the REST seam: a renewal PATCH that
/// 404s (Graph retains no deleted subscription) mints a fresh create for the
/// SAME resource, carrying the caller-owned clientState, installs it in
/// place of the vanished row under the same handle, and invalidates the
/// row's scopes through `pending_gaps` - nothing was delivered between the
/// disappearance and the replacement. The recovery succeeded within the
/// pass, so no `UnitFailed` precedes it and the pass ends `Healthy`.
#[tokio::test(start_paused = true)]
async fn a_vanished_subscription_is_recreated_and_reconnected_on_the_next_tick() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.mailboxes.primary().script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        ),
        super::fixtures::created("fresh"),
    ]);
    expiring_with(
        &account,
        "/me/mailFolders/inbox/messages",
        "stale",
        vec![email_scope("inbox")],
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    match session.next().await {
        Some(E::Invalidated(hint)) => {
            assert!(matches!(
                hint.source,
                bifrost_types::PushSource::GraphSubscription
            ));
            assert!(
                matches!(&hint.payload, bifrost_types::HintPayload::SpecificCursorScope(scope) if *scope == email_scope("inbox")),
                "the recreated row's scope, got {:?}",
                hint.payload
            );
        }
        other => panic!("expected the gap invalidation, got {other:?}"),
    }
    assert!(matches!(session.next().await, Some(E::Healthy)));
    assert_eq!(account.webhooks.events_for_tests(), 0);
    assert_eq!(
        super::fixtures::row_ids(&account),
        [(
            "/me/mailFolders/inbox/messages".to_string(),
            "fresh".to_string()
        )],
        "in place, not beside"
    );
    let requests = account.mailboxes.primary().take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "PATCH");
    assert!(requests[0].url.ends_with("/subscriptions/stale"));
    assert_eq!(requests[1].method, "POST");
    assert!(requests[1].url.ends_with("/subscriptions"));
    let created = requests[1].body.as_ref().expect("create body");
    assert_eq!(
        created["resource"].as_str(),
        Some("/me/mailFolders/inbox/messages")
    );
    assert_eq!(created["clientState"].as_str(), Some("secret"));
}

/// If the folder disappeared with the old subscription, the replacement
/// create answers 404 too. That refusal is terminal: the stale row stays
/// registered for teardown, is flagged `terminated`, and no later session
/// repeats the doomed PATCH and POST. The script holds exactly the two
/// responses, so a retry would exhaust it and panic rather than pass.
#[tokio::test(start_paused = true)]
async fn a_missing_resource_on_recreate_terminates_the_subscription_row() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    let not_found = || {
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({
                "error": { "code": "ResourceNotFound", "message": "resource is gone" }
            }),
        )
    };
    account
        .mailboxes
        .primary()
        .script_rest([not_found(), not_found()]);
    let resource = "/me/mailFolders/deleted-folder/messages";
    lease_expiring(&account, "h", resource, "stale");
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(
        matches!(session.next().await, Some(E::UnitFailed(e)) if e.recovery().is_terminal()),
        "a terminal recreate refusal reaches the engine"
    );
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    assert!(session.next().await.is_none());
    {
        let row = row(&account, resource);
        assert_eq!(row.server_id, "stale", "stale row stays for teardown");
        assert!(row.terminated);
    }
    let requests = account.mailboxes.primary().take_rest_requests();
    assert_eq!(requests.len(), 2, "one PATCH and one failed create");
    assert_eq!(requests[0].method, "PATCH");
    assert_eq!(requests[1].method, "POST");

    // The next session finds no live row and parks, however long it waits.
    let mut next = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    let mut event = Box::pin(next.next());
    for _ in 0..4 {
        assert!(futures::poll!(&mut event).is_pending());
        tokio::time::advance(
            crate::account::push::renewal::RENEWAL_CHECK_INTERVAL
                + std::time::Duration::from_secs(1),
        )
        .await;
        tokio::task::yield_now().await;
    }
    assert!(futures::poll!(&mut event).is_pending());
    assert_eq!(
        account.mailboxes.primary().wire_attempts(),
        2,
        "terminated rows are not retried"
    );
}

/// The plain success leg, driven across several cadence waits. A due
/// subscription is PATCHed once; the expiry Graph GRANTED is written back
/// into the state the next pass reads, so the row stops being due and no
/// further request is issued however many passes run. Renewing on every
/// pass is an unbounded request loop against Graph, and the script holds
/// exactly one response, so a second PATCH panics rather than passing.
///
/// The stored expiry is the granted string exactly, not the one requested:
/// Graph may cap a renewal below the request, and a locally computed expiry
/// would have the session believe in coverage the server already dropped.
/// A healthy renewal is also quiet: only `Healthy`, no invalidation and no
/// failure.
#[tokio::test(start_paused = true)]
async fn a_successful_renewal_updates_the_expiry_and_the_next_tick_finds_nothing_due() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account
        .mailboxes
        .primary()
        .script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({
                "id": "sub",
                "resource": "/me/mailFolders/inbox/messages",
                "expirationDateTime": "2099-01-01T00:00:00Z"
            }),
        )]);
    lease_expiring(&account, "h", "/me/mailFolders/inbox/messages", "sub");
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    // Many more passes than renewals: the point is that the extra passes,
    // each one cadence wait apart under paused time, issue nothing.
    for _ in 0..16 {
        let event = session.next().await;
        assert!(
            matches!(event, Some(E::Healthy)),
            "a healthy pass reports only Healthy, got {event:?}"
        );
    }
    {
        let row = row(&account, "/me/mailFolders/inbox/messages");
        assert_eq!(row.server_id, "sub", "renewed in place");
        assert_eq!(
            row.expires_at, "2099-01-01T00:00:00Z",
            "the stored expiry is the server-granted one"
        );
        assert!(!crate::webhooks::is_expiring_soon(
            &row.expires_at,
            crate::account::push::renewal::RENEWAL_THRESHOLD_MINUTES
        ));
    }
    let requests = account.mailboxes.primary().take_rest_requests();
    assert_eq!(requests.len(), 1, "one renewal, not one per pass");
    assert_eq!(requests[0].method, "PATCH");
    assert!(requests[0].url.ends_with("/subscriptions/sub"));
    assert!(
        requests[0].body.as_ref().expect("renewal body")["expirationDateTime"]
            .as_str()
            .is_some_and(|expiry| expiry.ends_with('Z') && expiry != "2099-01-01T00:00:00Z"),
        "the PATCH requests its own expiry, which is not what gets stored"
    );
}

/// A pass whose ONLY failure is terminal still ends `Incomplete`. A
/// terminal row is flagged and never renewed again, so it is not "worth
/// another tick" - but its coverage is gone, and a sibling's successful
/// renewal in the same pass must not let the lane read healthy.
#[tokio::test(start_paused = true)]
async fn a_pass_whose_only_failure_is_terminal_ends_incomplete() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.mailboxes.primary().script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::FORBIDDEN,
            serde_json::json!({"error":{"code":"ErrorAccessDenied","message":"denied"}}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"id":"healthy","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
    ]);
    lease_expiring(&account, "h", "/me/events", "denied");
    lease_expiring(&account, "h", "/me/messages", "healthy");
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::UnitFailed(e)) if e.recovery().is_terminal()));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    assert!(session.next().await.is_none());
    assert_eq!(
        account.mailboxes.primary().take_rest_requests().len(),
        2,
        "both rows attempted"
    );
    assert!(row(&account, "/me/events").terminated);
    assert_eq!(
        row(&account, "/me/messages").expires_at,
        "2099-01-01T00:00:00Z"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_tick_disconnects_and_the_next_successful_tick_reconnects() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.mailboxes.primary().script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({"error":{"code":"TooManyRequests"}}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"id":"old","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
    ]);
    lease_expiring(&account, "h", "/me/events", "old");
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::UnitFailed(_))));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    drop(session);
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Healthy)));
    assert_eq!(account.mailboxes.primary().take_rest_requests().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn one_tick_recreating_several_subscriptions_reconnects_once() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.mailboxes.primary().script_rest((0..2).flat_map(|i| [
        ScriptedRestResponse::json(reqwest::StatusCode::NOT_FOUND, serde_json::json!({"error":{"code":"ResourceNotFound"}})),
        ScriptedRestResponse::json(reqwest::StatusCode::CREATED, serde_json::json!({"id":format!("new-{i}"),"expirationDateTime":"2099-01-01T00:00:00Z"})),
    ]));
    expiring_with(&account, "/me/events", "old-1", vec![email_scope("f0")]);
    expiring_with(&account, "/me/messages", "old-2", vec![email_scope("f1")]);
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    for _ in 0..2 {
        assert!(matches!(session.next().await, Some(E::Invalidated(_))));
    }
    assert!(matches!(session.next().await, Some(E::Healthy)));
    assert_eq!(account.webhooks.events_for_tests(), 0);
    assert_eq!(account.mailboxes.primary().take_rest_requests().len(), 4);
}

#[tokio::test(start_paused = true)]
async fn a_worker_cancelled_before_its_first_renewal_sends_nothing() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    lease_expiring(&account, "h", "/me/events", "old");
    let (stopper, stop) = SessionStop::pair();
    stopper.stop();
    let mut session = renewal_session(account.clone(), stop, PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Ended(End::Stopped))));
    assert!(account.mailboxes.primary().take_rest_requests().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_failing_row_does_not_starve_a_healthy_row_in_the_same_pass() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.mailboxes.primary().script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({"error":{"code":"TooManyRequests"}}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"id":"second","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
    ]);
    lease_expiring(&account, "h", "/me/events", "first");
    lease_expiring(&account, "h", "/me/messages", "second");
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::UnitFailed(_))));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    assert_eq!(account.mailboxes.primary().take_rest_requests().len(), 2);
    assert_eq!(
        row(&account, "/me/messages").expires_at,
        "2099-01-01T00:00:00Z"
    );
}
#[tokio::test(start_paused = true)]
async fn every_row_failure_in_a_pass_is_reported() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.mailboxes.primary().script_rest((0..2).map(|_| {
        ScriptedRestResponse::json(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({"error":{"code":"TooManyRequests"}}),
        )
    }));
    lease_expiring(&account, "h", "/me/messages", "first");
    lease_expiring(&account, "h", "/me/events", "second");
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    for _ in 0..2 {
        assert!(matches!(session.next().await, Some(E::UnitFailed(_))));
    }
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
}
#[tokio::test(start_paused = true)]
async fn a_pass_after_a_retired_row_stays_incomplete_until_the_handle_is_unsubscribed() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    lease_expiring(&account, "h", "/me/messages", "dead");
    super::fixtures::lease(&account, "h", "/me/events", "live");
    account.webhooks.store().edit_for_tests(|rows| {
        rows.get_mut("/me/messages").expect("row").terminated = true;
    });
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    drop(session);
    account.webhooks.store().edit_for_tests(|rows| {
        rows.remove("/me/messages");
    });
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Healthy)));
    assert!(account.mailboxes.primary().take_rest_requests().is_empty());
}
#[tokio::test(start_paused = true)]
async fn a_renewal_pass_waits_for_the_push_gate_before_each_row() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.mailboxes.primary().script_rest((0..2).map(|_| {
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"id":"renewed","expirationDateTime":"2099-01-01T00:00:00Z"}),
        )
    }));
    lease_expiring(&account, "h", "/me/messages", "first");
    lease_expiring(&account, "h", "/me/events", "second");
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    let gate = std::sync::Arc::clone(&permits);
    let gate = PushGate::new(move || {
        let gate = std::sync::Arc::clone(&gate);
        Box::pin(async move {
            gate.acquire().await.unwrap().forget();
        })
    });
    let mut session = renewal_session(account.clone(), SessionStop::never(), gate);
    assert!(matches!(session.next().await, Some(E::Established)));
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    assert!(account.mailboxes.primary().take_rest_requests().is_empty());
    permits.add_permits(1);
    for _ in 0..20 {
        assert!(futures::poll!(&mut next).is_pending());
        tokio::task::yield_now().await;
    }
    assert_eq!(account.mailboxes.primary().take_rest_requests().len(), 1);
    permits.add_permits(1);
    assert!(matches!(next.await, Some(E::Healthy)));
    assert_eq!(account.mailboxes.primary().take_rest_requests().len(), 1);
}
#[tokio::test]
async fn a_recreate_landing_after_its_session_dropped_is_invalidated_by_the_next_session() {
    use crate::account::push::renewal::maintain_row;
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.mailboxes.primary().script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        ),
        super::fixtures::created("new"),
    ]);
    lease_expiring(&account, "h", "/me/messages", "old");
    let snapshot = account
        .webhooks
        .store()
        .renewable(account.webhooks.instance(), |_| true)
        .remove(0);
    let held = account.webhooks.store().commit_gate.write().await;
    let owner = account.clone();
    let task = tokio::spawn(async move { maintain_row(&owner, snapshot).await });
    assert!(
        super::fixtures::settle(|| account.mailboxes.primary().rest_requests().len() == 2).await
    );
    task.abort();
    let _ = task.await;
    drop(held);
    assert!(super::fixtures::settle(|| account.webhooks.events_for_tests() == 1).await);
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Invalidated(_))));
    assert_eq!(account.webhooks.events_for_tests(), 0);
}
#[tokio::test]
async fn close_joins_the_worker_and_so_waits_for_the_recreates_undo() {
    use bifrost_types::Account;
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    let gate = std::sync::Arc::new(tokio::sync::Notify::new());
    let complete = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ready = std::sync::Arc::clone(&gate);
    let done = std::sync::Arc::clone(&complete);
    crate::account::push_tasks::track(
        &account,
        tokio::spawn(async move {
            ready.notified().await;
            done.store(true, std::sync::atomic::Ordering::SeqCst);
        }),
    );
    let mut close = Box::pin(account.close());
    assert!(futures::poll!(&mut close).is_pending());
    gate.notify_one();
    close.await.unwrap();
    assert!(complete.load(std::sync::atomic::Ordering::SeqCst));
}
#[tokio::test]
async fn push_lanes_follow_mode_and_endpoint() {
    use crate::account::{GraphAccount, PushMode};
    let client = crate::client::GraphClient::new("token");
    let no_endpoint = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    assert!(crate::account::push_stream::push_lanes(&no_endpoint).is_empty());
    assert_eq!(
        crate::account::push_stream::push_lanes(&webhook_account(&client))[0].kind,
        bifrost_types::PushLaneKind::Registration
    );
    let ews = GraphAccount::new_for_tests(client, PushMode::EwsStreaming);
    assert_eq!(
        crate::account::push_stream::push_lanes(&ews)[0].kind,
        bifrost_types::PushLaneKind::Connection
    );
}

#[tokio::test]
async fn the_worker_stops_on_cancellation_while_a_renewal_is_in_flight() {
    let client = crate::client::GraphClient::new("token");
    client.script_aux_pending(1);
    let account = webhook_account(&client);
    lease_expiring(&account, "h", "/me/events", "old");
    let (stopper, stop) = SessionStop::pair();
    let mut session = renewal_session(account, stop, PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    let mut next = Box::pin(session.next());
    for _ in 0..20 {
        assert!(futures::poll!(&mut next).is_pending());
        tokio::task::yield_now().await;
    }
    assert_eq!(client.wire_attempts(), 1);
    stopper.stop();
    assert!(matches!(next.await, Some(E::Ended(End::Stopped))));
    assert!(session.next().await.is_none());
}

/// Another account's step terminated the only row this account holds. Its
/// session delivers the verdict and ends `Incomplete`, as if its own pass had
/// found it, instead of parking silently with the lane believed healthy.
#[tokio::test]
async fn a_verdict_reached_through_the_queue_ends_the_session_incomplete() {
    let client = crate::client::GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        serde_json::json!({"error":{"code":"ErrorAccessDenied","message":"denied"}}),
    )]);
    let old = webhook_account(&client);
    lease_expiring(&old, "old", "/me/events", "shared");
    let new = super::fixtures::reopened(&old, &client);
    lease_expiring(&new, "new", "/me/events", "shared");
    let snapshot = old
        .webhooks
        .store()
        .renewable(old.webhooks.instance(), |_| true)
        .remove(0);
    crate::account::push::renewal::maintain_row(&old, snapshot).await;

    let mut session = renewal_session(new.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::UnitFailed(e)) if e.recovery().is_terminal()));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
}

/// A row nobody holds live (here a lease whose account was dropped unclosed)
/// has no handle anyone will release; the next close of any account of the
/// factory retries its DELETE.
#[tokio::test]
async fn close_retries_a_row_nobody_holds() {
    let client = crate::client::GraphClient::new("token");
    client.script_rest([super::fixtures::deleted()]);
    let survivor = webhook_account(&client);
    {
        let dropped = super::fixtures::reopened(&survivor, &client);
        super::fixtures::lease(&dropped, "abandoned", "/me/events", "debt");
    }
    crate::account::push::retire_all_graph_subscriptions(&survivor).await;
    assert_eq!(client.wire_attempts(), 1);
    assert!(super::fixtures::rows_empty(&survivor));
}

/// A metered view is the same opened account: it renews the same leases and
/// delivers the same queued events. Minting it a fresh instance would make
/// its renewal step see a lease of "another account" and skip it.
#[tokio::test]
async fn a_metered_view_shares_its_accounts_leases_and_events() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    lease_expiring(&account, "h", "/me/events", "old");
    let (metered, _) = account.metered();
    assert_eq!(metered.webhooks.instance(), account.webhooks.instance());
    assert!(metered.webhooks.has_live());
    assert_eq!(
        metered
            .webhooks
            .store()
            .renewable(metered.webhooks.instance(), |_| true)
            .len(),
        1
    );
    let store = std::sync::Arc::clone(account.webhooks.store());
    let exchange = store.exchange().await;
    assert!(store.recover(&exchange, "/me/events", Some("new".into()), "x".into()));
    drop(exchange);
    assert!(matches!(
        metered.webhooks.pop_event(),
        Some(crate::account::push::registry::InstanceEvent::Gap(_))
    ));
    assert_eq!(account.webhooks.events_for_tests(), 0);
}
