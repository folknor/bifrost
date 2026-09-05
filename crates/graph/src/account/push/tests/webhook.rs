//! The Graph `/subscriptions` arm: resource construction, subscribe with its
//! rollback, teardown, and the health-latch edge a subscribe publishes on.

use std::collections::HashMap;

use bifrost_types::{
    AccountErrorKind, AccountOperation, CursorScope, FolderId, ObjectType, SubscriptionHandle,
    WatchEvent,
};

use crate::account::push::common::PushEndpoint;
use crate::account::push::dispatch::push_subscribe;
use crate::account::push::webhook::{
    GraphSubscriptionGroup, mark_group_tearing_down, remove_subscription_from_groups,
    resource_for_scope, retire_all_graph_subscriptions, unsubscribe_graph,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};

use super::fixtures::{email_scope, state};

#[test]
fn graph_subscription_resource_uses_folder_messages() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Email,
    };
    assert_eq!(
        resource_for_scope(&account, &scope)
            .expect("primary")
            .as_deref(),
        Some("/me/mailFolders/inbox/messages")
    );
}

/// Each calendar scope must subscribe to its own calendar. `{prefix}/events`
/// is the default calendar, so a shared resource string both mis-targets
/// secondary calendars and collapses them together in `subscribe_graph`'s
/// resource-keyed grouping.
#[test]
fn graph_event_subscription_names_the_scope_calendar() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = |id: &str| CursorScope::FolderType {
        folder: FolderId(id.to_string()),
        ty: ObjectType::Event,
    };
    assert_eq!(
        resource_for_scope(&account, &scope("calendar-id"))
            .expect("primary")
            .as_deref(),
        Some("/me/calendars/calendar-id/events")
    );
    assert_ne!(
        resource_for_scope(&account, &scope("secondary")).expect("primary"),
        resource_for_scope(&account, &scope("calendar-id")).expect("primary"),
    );
    let opaque = resource_for_scope(&account, &scope("AAMk/GI2="))
        .expect("primary")
        .expect("subscribable");
    assert!(!opaque.contains("AAMk/GI2="), "{opaque}");
    assert!(opaque.ends_with("/events"), "{opaque}");
}

#[test]
fn graph_subscription_resource_routes_foreign_scope_to_owner() {
    let account = GraphAccount::new_for_tests_with_shared(
        GraphClient::new("token"),
        PushMode::GraphSubscriptions,
        &["shared@contoso.com".to_string()],
    );
    let scope = CursorScope::FolderType {
        folder: crate::account::foreign::encode_foreign("shared@contoso.com", "AAMk"),
        ty: ObjectType::Email,
    };
    // The owning mailbox rides in the `/users/{id}` segment and only
    // the native folder id is in `/mailFolders/{id}` - no raw foreign
    // id (no `%1F` separator) is percent-encoded into the URL.
    let resource = resource_for_scope(&account, &scope)
        .expect("configured mailbox")
        .expect("foreign scope resolves");
    assert_eq!(
        resource,
        "/users/shared%40contoso.com/mailFolders/AAMk/messages"
    );
    assert!(!resource.contains("%1F"));
}

#[tokio::test]
async fn webhook_creation_rolls_back_each_already_created_subscription() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::CREATED,
            serde_json::json!({"id":"first","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error":{"code":"ErrorInternalServerError","message":"no"}}),
        ),
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
    ]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/hook".to_string(),
        client_state: "secret".to_string(),
    });
    let scopes = vec![
        CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        },
        CursorScope::FolderType {
            folder: FolderId("contacts".to_string()),
            ty: ObjectType::Contact,
        },
    ];
    assert!(push_subscribe(account.clone(), scopes).await.is_err());
    assert!(account.graph_subscriptions.read().await.is_empty());
    let requests = client.take_rest_requests();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        ["POST", "POST", "DELETE"]
    );
    assert!(requests[2].url.ends_with("/subscriptions/first"));
}

#[tokio::test]
async fn unsubscribe_graph_deletes_every_server_row() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("one", "/me/messages"),
            state("two", "/me/events"),
        ]),
    );
    unsubscribe_graph(account.clone(), handle)
        .await
        .expect("delete loop succeeds");
    assert!(account.graph_subscriptions.read().await.is_empty());
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].url.ends_with("/subscriptions/one"));
    assert!(requests[1].url.ends_with("/subscriptions/two"));
}

#[tokio::test]
async fn close_continues_after_one_subscription_delete_fails() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error":{"code":"InternalServerError","message":"failed"}}),
        ),
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle,
        GraphSubscriptionGroup::live(vec![
            state("fails", "/me/messages"),
            state("succeeds", "/me/events"),
        ]),
    );
    retire_all_graph_subscriptions(&account).await;
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].url.ends_with("/subscriptions/succeeds"));
}

/// A subscription Graph has already dropped must not fail teardown.
/// `subscription_is_gone` used to read only `GraphError::Response`,
/// which a REST call never produces - the transport converts a 404 into
/// `bifrost_net::Error::Status` first - so on the live path the 404
/// tolerance never applied, the DELETE loop aborted on the vanished row,
/// and the handle stayed registered with its remaining siblings
/// undeleted.
#[tokio::test]
async fn unsubscribe_tolerates_a_subscription_the_server_already_dropped() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        ),
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("vanished", "/me/messages"),
            state("live", "/me/events"),
        ]),
    );

    unsubscribe_graph(account.clone(), handle)
        .await
        .expect("a row the server already dropped is not a teardown failure");
    assert!(account.graph_subscriptions.read().await.is_empty());
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].url.ends_with("/subscriptions/live"));
}

#[test]
fn contact_scope_subscribes_to_the_contact_folder_collection() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = CursorScope::FolderType {
        folder: FolderId("contacts".to_string()),
        ty: ObjectType::Contact,
    };
    assert_eq!(
        resource_for_scope(&account, &scope)
            .expect("primary")
            .as_deref(),
        Some("/me/contactFolders/contacts/contacts")
    );
}

#[test]
fn unsubscribable_scopes_have_no_graph_resource() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    assert!(
        resource_for_scope(&account, &CursorScope::Account)
            .expect("primary")
            .is_none()
    );
    // A public folder is poll-only; it must not resolve to a resource.
    assert!(
        resource_for_scope(
            &account,
            &CursorScope::Folder(FolderId("AAMkPF=".to_string()))
        )
        .expect("primary")
        .is_none()
    );
    assert!(
        resource_for_scope(
            &account,
            &CursorScope::FolderType {
                folder: FolderId("inbox".to_string()),
                ty: ObjectType::Mailbox,
            }
        )
        .expect("primary")
        .is_none()
    );
}

#[test]
fn opaque_folder_ids_are_percent_encoded_into_the_resource() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = CursorScope::FolderType {
        folder: FolderId("AAMk/GI2=".to_string()),
        ty: ObjectType::Email,
    };
    let resource = resource_for_scope(&account, &scope)
        .expect("primary")
        .expect("email scope resolves");
    assert!(!resource.contains("AAMk/GI2="), "{resource}");
    assert!(resource.ends_with("/messages"), "{resource}");
}

/// A persisted scope whose shared mailbox was removed must fail before
/// it can be rewritten into a `/me` subscription resource.
#[test]
fn an_unconfigured_foreign_mailbox_is_rejected_locally() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = CursorScope::FolderType {
        folder: crate::account::foreign::encode_foreign("other@contoso.com", "AAMk"),
        ty: ObjectType::Email,
    };
    assert!(matches!(
        resource_for_scope(&account, &scope),
        Err(crate::error::GraphError::Configuration { .. })
    ));
}

#[tokio::test]
async fn webhook_mode_without_an_endpoint_is_unsupported() {
    // `with_push_endpoint` was never called: there is nowhere for Graph
    // to deliver, so subscribing must refuse rather than create a
    // subscription pointing at nothing.
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Email,
    };
    let err = push_subscribe(account, vec![scope])
        .await
        .expect_err("expected Unsupported");
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
    ));
}

/// Teardown removes only the subscription whose DELETE was confirmed.
/// If the next DELETE fails, the remaining server ids are still in the
/// group, so retrying the same handle can finish the cleanup rather than
/// orphaning notifications until Graph's expiry window closes.
#[test]
fn unsubscribe_retains_not_yet_deleted_subscriptions_for_retry() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("deleted", "/me/mailFolders/inbox/messages"),
            state("retry-me", "/me/events"),
            state("not-attempted", "/me/contacts"),
        ]),
    )]);

    assert!(!remove_subscription_from_groups(
        &mut groups,
        &handle,
        "deleted"
    ));
    let remaining = &groups[&handle].subscriptions;
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().any(|state| state.server_id == "retry-me"));
    assert!(
        remaining
            .iter()
            .any(|state| state.server_id == "not-attempted")
    );
}

/// Condemning is idempotent and monotone: a retry after a failed DELETE
/// re-marks the same group and returns the ids still to delete, while an
/// unknown handle stays a no-op (teardown may be retried after a reopen).
#[test]
fn marking_teardown_is_idempotent_and_unknown_handles_are_none() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![state("retry-me", "/me/events")]),
    )]);
    assert_eq!(
        mark_group_tearing_down(&mut groups, &handle),
        Some(vec!["retry-me".to_string()])
    );
    assert!(groups[&handle].tearing_down);
    assert_eq!(
        mark_group_tearing_down(&mut groups, &handle),
        Some(vec!["retry-me".to_string()]),
        "a teardown retry sees the ids whose DELETE has not been confirmed"
    );
    assert!(
        mark_group_tearing_down(&mut groups, &SubscriptionHandle("never".to_string())).is_none()
    );
}

#[tokio::test]
async fn unsubscribing_an_unknown_handle_is_a_no_op() {
    // Idempotent teardown: the engine may retry `push_unsubscribe`
    // after a reopen, when the in-memory group map is already empty.
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    unsubscribe_graph(account, SubscriptionHandle("never-issued".to_string()))
        .await
        .expect("unknown handle unsubscribes cleanly");
}

/// `Reconnected` is the engine's account-wide full-reconcile trigger, so it
/// is owed only on a real recovery edge. A first subscribe has no gap to
/// cover - the engine established or resumed those cursors moments earlier -
/// and publishing one there charged every account one redundant reconcile
/// over every registered scope at startup.
#[tokio::test]
async fn a_first_webhook_subscribe_publishes_no_reconnect() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::CREATED,
            serde_json::json!({"id":"first","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::CREATED,
            serde_json::json!({"id":"second","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
    ]);
    let mut account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/webhook".to_string(),
        client_state: "secret".to_string(),
    });
    let mut events = account.push_tx.subscribe();

    push_subscribe(account.clone(), vec![email_scope("inbox")])
        .await
        .expect("the first subscribe succeeds");
    assert!(
        events.try_recv().is_err(),
        "a first subscribe owes no reconcile"
    );

    // Now push IS down: the renewal worker has latched it. The next
    // successful subscribe is a genuine recovery edge and still emits, so
    // nothing that relied on the event for gap coverage loses it.
    account
        .push_disconnected
        .store(true, std::sync::atomic::Ordering::SeqCst);
    push_subscribe(account.clone(), vec![email_scope("archive")])
        .await
        .expect("the second subscribe succeeds");
    assert!(
        matches!(events.try_recv(), Ok(WatchEvent::Reconnected)),
        "a subscribe on a degraded account is the recovery edge"
    );
    assert!(
        !account
            .push_disconnected
            .load(std::sync::atomic::Ordering::SeqCst),
        "the latch is cleared by the edge it fires on"
    );
}
