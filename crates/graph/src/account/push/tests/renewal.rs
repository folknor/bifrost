//! The renewal health worker: the due list, the recreate of a vanished
//! subscription, the worker-slot retirement, and the Disconnected /
//! Reconnected edges the tick loop publishes.

use std::collections::HashMap;
use std::time::Duration;

use bifrost_types::{
    CursorScope, ErrorScope, FolderId, ObjectType, SubscriptionHandle, WatchEvent,
};

use crate::account::push::common::PushEndpoint;
use crate::account::push::renewal::{
    RENEWAL_CHECK_INTERVAL, RENEWAL_THRESHOLD_MINUTES, due_renewals,
    has_live_graph_subscription_group, install_replacement,
};
use crate::account::push::webhook::{
    GraphSubscriptionGroup, ensure_graph_worker, mark_group_tearing_down,
    remove_subscription_from_groups,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};

use super::fixtures::{expiring, state};

/// A recreated subscription takes the vanished one's place inside the
/// SAME group rather than piling up beside it: the stale `server_id`
/// would otherwise stay in the due list forever, 404 on every renewal
/// tick, and drive an endless recreate loop. Sibling resources under
/// the same handle are untouched.
#[test]
fn a_recreated_subscription_replaces_the_vanished_one_in_place() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("gone", "/me/mailFolders/inbox/messages"),
            state("healthy", "/me/events"),
        ]),
    )]);

    assert!(install_replacement(
        &mut groups,
        &handle,
        "gone",
        state("fresh", "/me/mailFolders/inbox/messages"),
    ));

    let subscriptions = &groups[&handle].subscriptions;
    assert_eq!(subscriptions.len(), 2);
    assert!(
        !subscriptions.iter().any(|s| s.server_id == "gone"),
        "the vanished subscription must not survive its replacement"
    );
    let fresh = subscriptions
        .iter()
        .find(|s| s.server_id == "fresh")
        .expect("replacement installed");
    assert_eq!(fresh.resource, "/me/mailFolders/inbox/messages");
    assert!(subscriptions.iter().any(|s| s.server_id == "healthy"));
    assert_eq!(groups.len(), 1);
}

/// The renewal worker walks a SNAPSHOT of the due subscriptions, so a
/// concurrent `push_unsubscribe` can retire the handle (and delete its
/// server subscriptions) while a replacement create is in flight.
/// Re-registering the group there would tell the caller teardown
/// succeeded while notifications kept arriving; the replacement is
/// refused instead, and `replace_gone_subscription` deletes the
/// server-side subscription it just minted.
#[test]
fn a_replacement_never_resurrects_an_unsubscribed_handle() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups: HashMap<SubscriptionHandle, GraphSubscriptionGroup> = HashMap::new();
    assert!(!install_replacement(
        &mut groups,
        &handle,
        "gone",
        state("fresh", "/me/mailFolders/inbox/messages"),
    ));
    assert!(groups.is_empty(), "an unsubscribed handle stays retired");

    // A DIFFERENT live handle is not a substitute: the replacement is
    // scoped to the handle whose subscription vanished.
    let other = SubscriptionHandle("other".to_string());
    groups.insert(
        other.clone(),
        GraphSubscriptionGroup::live(vec![state("other-sub", "/me/events")]),
    );
    assert!(!install_replacement(
        &mut groups,
        &handle,
        "gone",
        state("fresh", "/me/mailFolders/inbox/messages"),
    ));
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[&other].subscriptions.len(), 1);
}

/// A group that lost its stale row some other way (a terminal renewal
/// on the same tick) still takes the replacement: the handle is live,
/// so the resource needs coverage.
#[test]
fn a_replacement_installs_into_a_live_group_that_lost_the_stale_row() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![state("healthy", "/me/events")]),
    )]);
    assert!(install_replacement(
        &mut groups,
        &handle,
        "already-removed",
        state("fresh", "/me/mailFolders/inbox/messages"),
    ));
    assert_eq!(groups[&handle].subscriptions.len(), 2);
}

/// The teardown/renewal race the deletes-before-removing repair opened.
///
/// Teardown now keeps the group registered while it deletes, so
/// "registered" stopped implying "live": the renewal worker's recreate
/// path would install a replacement into a group whose server-id
/// snapshot had already been taken. Teardown then deleted the stale id,
/// retired the group, and returned success while the replacement kept
/// delivering notifications. The marker raised by
/// `mark_group_tearing_down` is what refuses that install.
#[test]
fn a_replacement_never_lands_in_a_group_being_torn_down() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("first", "/me/mailFolders/inbox/messages"),
            state("second", "/me/events"),
        ]),
    )]);

    // Teardown condemns the group and snapshots its ids under one lock.
    let snapshot = mark_group_tearing_down(&mut groups, &handle).expect("group registered");
    assert_eq!(snapshot, vec!["first".to_string(), "second".to_string()]);

    // The first DELETE is confirmed; the group stays registered so the
    // ids still to delete remain reachable.
    assert!(!remove_subscription_from_groups(
        &mut groups,
        &handle,
        "first"
    ));

    // The renewal worker's replacement for the vanished `second` lands
    // mid-teardown. Installing it would put a live server subscription
    // under a handle whose snapshot can never name it.
    assert!(
        !install_replacement(&mut groups, &handle, "second", state("fresh", "/me/events")),
        "a condemned group must refuse a replacement"
    );
    assert!(
        !groups[&handle]
            .subscriptions
            .iter()
            .any(|state| state.server_id == "fresh"),
        "the replacement must not become state teardown cannot see"
    );

    // Teardown finishes on the ids it snapshotted, and the handle is gone.
    assert!(remove_subscription_from_groups(
        &mut groups,
        &handle,
        "second"
    ));
    assert!(groups.is_empty(), "teardown retires the handle");
}

/// A condemned group is not renewed either. Renewal would extend the
/// life of exactly what the caller asked to delete, and the recreate
/// branch behind it is the same orphan by another route.
#[test]
fn due_renewals_skip_a_group_being_torn_down() {
    let live = SubscriptionHandle("live".to_string());
    let condemned = SubscriptionHandle("condemned".to_string());
    let mut groups = HashMap::from([
        (
            live.clone(),
            GraphSubscriptionGroup::live(vec![expiring("live-sub", "/me/events")]),
        ),
        (
            condemned.clone(),
            GraphSubscriptionGroup::live(vec![expiring("condemned-sub", "/me/contacts")]),
        ),
    ]);
    assert!(mark_group_tearing_down(&mut groups, &condemned).is_some());

    let due = due_renewals(&mut groups);
    assert_eq!(due.len(), 1, "only the live group is due");
    assert_eq!(due[0].handle, live);
    assert_eq!(due[0].server_id, "live-sub");
    assert_eq!(due[0].resource, "/me/events");
    // The scopes ride along so a terminal failure can name what lost
    // coverage rather than reporting an unattributed `Terminated`.
    assert!(!due[0].scopes.is_empty());

    // And an expiry outside the threshold is not due at all.
    groups.get_mut(&live).expect("live group").subscriptions[0].expires_at =
        "2099-01-01T00:00:00Z".to_string();
    assert!(due_renewals(&mut groups).is_empty());
}

/// Letting the worker exit on condemned-only groups opened a window in
/// which a brand-new subscription got NO renewal worker: the worker had
/// decided to exit but its `JoinHandle` was not finished yet, so the
/// concurrent `push_subscribe`'s `ensure_graph_worker` saw a live handle
/// and declined to spawn; the old task then completed and nothing renewed
/// the new subscription until some later subscribe. The exit therefore
/// clears the slot itself, under the guard that made the decision.
///
/// The tick here is driven with paused time and the map holds only a
/// condemned group, so the worker reaches its exit without a single HTTP
/// call. (The renewal HTTP legs themselves are scripted through the
/// REST seam where a test needs them - see the recreate test below.)
#[tokio::test(start_paused = true)]
async fn a_retiring_worker_clears_its_slot_so_the_next_subscribe_respawns() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let handle = SubscriptionHandle("condemned".to_string());
    {
        let mut groups = account.graph_subscriptions.write().await;
        groups.insert(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![state("sub", "/me/events")]),
        );
        assert!(mark_group_tearing_down(&mut groups, &handle).is_some());
    }

    ensure_graph_worker(account.clone()).await;
    assert!(
        account.graph_worker.lock().await.is_some(),
        "the worker slot is occupied while the worker runs"
    );

    // Drive renewal ticks until the worker retires. Time is paused, so
    // this advances the test clock rather than the wall clock; the extra
    // iterations only exist because the worker has to be scheduled onto
    // its first `sleep` before a tick can fire at all.
    for _ in 0..64 {
        if account.graph_worker.lock().await.is_none() {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    assert!(
        account.graph_worker.lock().await.is_none(),
        "a retiring worker must clear its own slot"
    );

    // The next subscription therefore gets a worker again.
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("fresh".to_string()),
        GraphSubscriptionGroup::live(vec![state("fresh-sub", "/me/events")]),
    );
    ensure_graph_worker(account.clone()).await;
    assert!(
        account.graph_worker.lock().await.is_some(),
        "a live subscription must always have a renewal worker"
    );
    account.shutdown.cancel();
}

/// The recreate leg, end to end through the REST seam with paused time:
/// a renewal PATCH that 404s (Graph retains no deleted subscription)
/// must mint a fresh create for the SAME resource, install it in place
/// of the vanished row under the same handle, and emit `Reconnected` -
/// the engine's full-reconcile trigger - because nothing was delivered
/// between the disappearance and the replacement. No `Disconnected`
/// precedes it: the recovery succeeded within one tick.
#[tokio::test(start_paused = true)]
async fn a_vanished_subscription_is_recreated_and_reconnected_on_the_next_tick() {
    let client = GraphClient::new("token");
    client.script_rest([
        // The renewal PATCH answers 404: the subscription vanished.
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        ),
        // The replacement create.
        ScriptedRestResponse::json(
            reqwest::StatusCode::CREATED,
            serde_json::json!({"id":"fresh","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
    ]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/hook".to_string(),
        client_state: "secret".to_string(),
    });
    let mut events = account.push_tx.subscribe();
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("stale", "/me/mailFolders/inbox/messages")]),
    );

    ensure_graph_worker(account.clone()).await;
    // Drive renewal ticks until the replacement lands. Paused time, so
    // this advances the test clock; the loop bound only covers task
    // scheduling slack.
    for _ in 0..64 {
        let installed = account
            .graph_subscriptions
            .read()
            .await
            .get(&handle)
            .is_some_and(|group| {
                group
                    .subscriptions
                    .iter()
                    .any(|state| state.server_id == "fresh")
            });
        if installed {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    {
        let groups = account.graph_subscriptions.read().await;
        let group = groups.get(&handle).expect("the handle stays registered");
        assert_eq!(group.subscriptions.len(), 1, "in place, not beside");
        assert_eq!(group.subscriptions[0].server_id, "fresh");
        assert_eq!(
            group.subscriptions[0].resource,
            "/me/mailFolders/inbox/messages"
        );
    }

    // The failed PATCH, then the create for the same resource - and the
    // create carries the caller-owned clientState like any first-time
    // subscribe would.
    let requests = client.take_rest_requests();
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

    match events.try_recv() {
        Ok(WatchEvent::Reconnected) => {}
        other => panic!("expected Reconnected, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "a within-tick recovery emits no Disconnected"
    );

    account.shutdown.cancel();
}

/// One `Reconnected` per TICK, not per recreated subscription.
///
/// `Reconnected` is account-wide: the engine's push reconciler answers it
/// with a `Coalesced` / `HintPayload::Unknown` invalidation over every
/// registered cursor scope. So the second recreate of a tick reconciles
/// exactly what the first already did, and a tick that finds several
/// subscriptions vanished at once - the case where the duplicates cost the
/// most - used to publish one full account reconcile per resource.
#[tokio::test(start_paused = true)]
async fn one_tick_recreating_several_subscriptions_reconnects_once() {
    let client = GraphClient::new("token");
    let gone = || {
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        )
    };
    let created = |id: &str| {
        ScriptedRestResponse::json(
            reqwest::StatusCode::CREATED,
            serde_json::json!({"id":id,"expirationDateTime":"2099-01-01T00:00:00Z"}),
        )
    };
    // Both rows are due in the same tick, so the worker walks them back to
    // back: PATCH 404, create, PATCH 404, create.
    client.script_rest([gone(), created("fresh-a"), gone(), created("fresh-b")]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/hook".to_string(),
        client_state: "secret".to_string(),
    });
    let mut events = account.push_tx.subscribe();
    let handle = SubscriptionHandle("h".to_string());
    // One group, so the due order is the vector's and the script's wire
    // order is an assertion rather than a coin flip on map iteration.
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            expiring("stale-a", "/me/mailFolders/inbox/messages"),
            expiring("stale-b", "/me/events"),
        ]),
    );

    ensure_graph_worker(account.clone()).await;
    for _ in 0..64 {
        let both_replaced = account
            .graph_subscriptions
            .read()
            .await
            .get(&handle)
            .is_some_and(|group| {
                group
                    .subscriptions
                    .iter()
                    .filter(|state| state.server_id.starts_with("fresh-"))
                    .count()
                    == 2
            });
        if both_replaced {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    // Let the tick that recreated finish its post-loop bookkeeping before
    // the channel is drained.
    tokio::task::yield_now().await;

    assert_eq!(
        client.take_rest_requests().len(),
        4,
        "two renewals, two recreates"
    );
    match events.try_recv() {
        Ok(WatchEvent::Reconnected) => {}
        other => panic!("expected Reconnected, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "a tick owes exactly one account-wide reconcile"
    );

    account.shutdown.cancel();
}

/// The plain SUCCESS leg, driven as a loop across ticks.
///
/// A due subscription is PATCHed once; the new expiry it returns is
/// written back into the state the NEXT tick reads, so the subscription
/// stops being due and no further request is issued no matter how many
/// ticks fire. Renewing on every tick (the failure mode a state that
/// never updates produces) would be an unbounded request loop against
/// Graph, and the seam is armed with exactly one response, so a second
/// PATCH panics rather than passing.
///
/// The quiet path is also pinned: a healthy renewal emits no push event
/// at all - neither `Disconnected` nor a spurious `Reconnected`.
#[tokio::test(start_paused = true)]
async fn a_successful_renewal_updates_the_expiry_and_the_next_tick_finds_nothing_due() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        serde_json::json!({
            "id": "sub",
            "resource": "/me/mailFolders/inbox/messages",
            "expirationDateTime": "2099-01-01T00:00:00Z"
        }),
    )]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let mut events = account.push_tx.subscribe();
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("sub", "/me/mailFolders/inbox/messages")]),
    );

    ensure_graph_worker(account.clone()).await;
    // Many more ticks than renewals: the point is that the extra ticks
    // issue nothing.
    for _ in 0..64 {
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    {
        let groups = account.graph_subscriptions.read().await;
        let group = groups.get(&handle).expect("the handle stays registered");
        assert_eq!(group.subscriptions.len(), 1);
        assert_eq!(group.subscriptions[0].server_id, "sub", "renewed in place");
        // The expiry stored is the one Graph GRANTED, not the one we
        // asked for. Graph may cap a renewal below the request, and a
        // locally computed expiry then has the renewer believing it has
        // coverage the server already dropped. Asserting only that the
        // stale value was replaced passes against the computed value
        // too, so this pins the granted string exactly.
        assert_eq!(
            group.subscriptions[0].expires_at, "2099-01-01T00:00:00Z",
            "the stored expiry is the server-granted one"
        );
        assert!(!crate::webhooks::is_expiring_soon(
            &group.subscriptions[0].expires_at,
            RENEWAL_THRESHOLD_MINUTES
        ));
    }

    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 1, "one renewal, not one per tick");
    assert_eq!(requests[0].method, "PATCH");
    assert!(requests[0].url.ends_with("/subscriptions/sub"));
    assert!(
        requests[0].body.as_ref().expect("renewal body")["expirationDateTime"]
            .as_str()
            .is_some_and(|expiry| expiry.ends_with('Z')),
        "the PATCH carries the new expiry it then stores"
    );
    assert!(
        events.try_recv().is_err(),
        "a healthy renewal is silent on the push channel"
    );

    account.shutdown.cancel();
}

/// A terminal renewal failure must name the scope that lost push
/// coverage. `subscribe_graph` groups by resource string and used to
/// discard the scopes it grouped, so `Terminated` arrived with no
/// attribution at all and the engine could not tell which scopes to fall
/// back to polling.
#[tokio::test(start_paused = true)]
async fn a_terminal_renewal_failure_names_the_scope_that_lost_coverage() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        serde_json::json!({"error":{"code":"ErrorAccessDenied","message":"no"}}),
    )]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let mut events = account.push_tx.subscribe();
    let scope = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Email,
    };
    let mut state = expiring("sub", "/me/mailFolders/inbox/messages");
    state.scopes = vec![scope.clone()];
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".to_string()),
        GraphSubscriptionGroup::live(vec![state]),
    );

    ensure_graph_worker(account.clone()).await;
    // A terminal failure retires the local state, so the group emptying
    // is the signal the tick has run.
    for _ in 0..64 {
        let retired = account
            .graph_subscriptions
            .read()
            .await
            .values()
            .all(|group| group.subscriptions.is_empty());
        if retired {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;

    let mut terminated = None;
    while let Ok(event) = events.try_recv() {
        match event {
            WatchEvent::Terminated(error) => terminated = Some(error),
            WatchEvent::Disconnected => {}
            other => panic!("unexpected push event {other:?}"),
        }
    }
    let terminated = terminated.expect("a terminal failure is reported");
    assert_eq!(terminated.scope(), Some(&ErrorScope::Cursor(scope)));

    account.shutdown.cancel();
}

/// The recovery edge of the same loop: a retryable renewal failure
/// announces `Disconnected` ONCE, and the tick that finally succeeds
/// announces `Reconnected`. Without the success leg clearing the
/// latch, a recovered subscription would stay reported as
/// disconnected for the life of the account.
#[tokio::test(start_paused = true)]
async fn a_failed_tick_disconnects_and_the_next_successful_tick_reconnects() {
    let client = GraphClient::new("token");
    client.script_rest([
        // Retryable (429), so the subscription stays installed and due.
        ScriptedRestResponse::json(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({"error":{"code":"activityLimitReached","message":"slow down"}}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({
                "id": "sub",
                "resource": "/me/events",
                "expirationDateTime": "2099-01-01T00:00:00Z"
            }),
        ),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let mut events = account.push_tx.subscribe();
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("sub", "/me/events")]),
    );

    ensure_graph_worker(account.clone()).await;
    for _ in 0..64 {
        let renewed = account
            .graph_subscriptions
            .read()
            .await
            .get(&handle)
            .is_some_and(|group| {
                !crate::webhooks::is_expiring_soon(
                    &group.subscriptions[0].expires_at,
                    RENEWAL_THRESHOLD_MINUTES,
                )
            });
        if renewed {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    // Let the tick that renewed finish its post-loop bookkeeping (the
    // `Reconnected` it owes) before the channel is read.
    tokio::task::yield_now().await;

    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2, "the failed renewal is retried once");
    assert!(requests.iter().all(|request| request.method == "PATCH"));

    match events.try_recv() {
        Ok(WatchEvent::Disconnected) => {}
        other => panic!("expected Disconnected, got {other:?}"),
    }
    match events.try_recv() {
        Ok(WatchEvent::Reconnected) => {}
        other => panic!("expected Reconnected, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "the latch must not re-announce on later quiet ticks"
    );

    account.shutdown.cancel();
}

#[test]
fn condemned_only_groups_do_not_keep_the_renewal_worker_alive() {
    let handle = SubscriptionHandle("condemned".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("sub", "/me/events")]),
    )]);
    assert!(has_live_graph_subscription_group(&groups));
    assert!(mark_group_tearing_down(&mut groups, &handle).is_some());
    assert!(!has_live_graph_subscription_group(&groups));
}
