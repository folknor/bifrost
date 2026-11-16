//! The Graph `/subscriptions` arm: resource construction, subscribe with its
//! rollback, release, and the shared-row ownership a reopen and overlapping
//! subscribes rely on.

use bifrost_types::{
    AccountErrorKind, AccountId, AccountOperation, CursorScope, FolderId, ObjectType,
    SubscriptionHandle,
};

use crate::account::push::common::PushEndpoint;
use crate::account::push::common::{
    DecodedHandle, account_binding, decode_handle, ews_handle, graph_handle,
};
use crate::account::push::dispatch::{push_subscribe, push_unsubscribe};
use crate::account::push::webhook::{
    resource_for_scope, retire_all_graph_subscriptions, unsubscribe_graph,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};

use super::fixtures::{
    created, deleted, email_scope, lease, reopened, row_ids, rows_empty, settle, webhook_account,
};

fn server_error() -> ScriptedRestResponse {
    ScriptedRestResponse::json(
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({"error":{"code":"InternalServerError","message":"no"}}),
    )
}

fn calendar_scope(id: &str) -> CursorScope {
    CursorScope::FolderType {
        folder: FolderId(id.to_string()),
        ty: ObjectType::CalendarEvent,
    }
}

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

/// Graph documents only the mailbox-wide `/events` and `/contacts`
/// notification resources (all calendars, all personal contacts). A
/// per-collection path is undocumented, and one rejected create rolls back
/// the whole subscribe group, mail included, so every calendar scope maps to
/// `/events` and every contact scope to `/contacts`, and the resource-keyed
/// grouping folds each type into one subscription.
#[test]
fn graph_event_and_contact_scopes_share_the_mailbox_wide_resources() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = |id: &str, ty| CursorScope::FolderType {
        folder: FolderId(id.to_string()),
        ty,
    };
    for id in ["calendar-id", "secondary", "AAMk/GI2="] {
        assert_eq!(
            resource_for_scope(&account, &scope(id, ObjectType::CalendarEvent))
                .expect("primary")
                .as_deref(),
            Some("/me/events")
        );
    }
    for id in ["contacts", "nested"] {
        assert_eq!(
            resource_for_scope(&account, &scope(id, ObjectType::Contact))
                .expect("primary")
                .as_deref(),
            Some("/me/contacts")
        );
    }
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

/// A token that lists calendars (Calendars.ReadBasic) may still be refused
/// an `/events` subscription. That refusal is the calendar group's lane
/// failure: the other group's subscription survives and nothing is deleted.
/// Both orders are driven (refusal first, refusal second), and the failed
/// lane is matched to the scope whose resource was actually refused.
#[tokio::test]
async fn a_permission_refused_create_fails_its_lane_and_keeps_the_rest() {
    let denied = || {
        ScriptedRestResponse::json(
            reqwest::StatusCode::FORBIDDEN,
            serde_json::json!({"error":{"code":"ExtensionError","message":"refused"}}),
        )
    };
    for refuse_first in [true, false] {
        let client = GraphClient::new("token");
        if refuse_first {
            client.script_rest([denied(), created("kept")]);
        } else {
            client.script_rest([created("kept"), denied()]);
        }
        let account = webhook_account(&client);
        let inbox = email_scope("inbox");
        let calendar = CursorScope::FolderType {
            folder: FolderId("cal".to_string()),
            ty: ObjectType::CalendarEvent,
        };
        let subscription = push_subscribe(account.clone(), vec![inbox.clone(), calendar.clone()])
            .await
            .expect("the surviving group is a partial success");
        let requests = client.take_rest_requests();
        assert!(
            requests
                .iter()
                .all(|request| request.method.as_str() == "POST")
        );
        let refused_index = usize::from(!refuse_first);
        let refused_resource = requests[refused_index]
            .body
            .as_ref()
            .and_then(|body| body.get("resource"))
            .and_then(serde_json::Value::as_str)
            .expect("a subscription body")
            .to_string();
        let (refused, kept) = if refused_resource.ends_with("/events") {
            (calendar, inbox)
        } else {
            (inbox, calendar)
        };
        assert!(subscription.handle().is_some());
        assert_eq!(subscription.outcomes().succeeded().len(), 1);
        assert_eq!(subscription.outcomes().succeeded()[0].output, kept);
        let failed = subscription.outcomes().failed();
        assert_eq!(failed.len(), 1);
        assert!(matches!(
            failed[0].error.kind(),
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied)
        ));
        assert_eq!(
            failed[0].error.scope(),
            Some(&bifrost_types::ErrorScope::Cursor(refused))
        );
        assert_eq!(row_ids(&account).len(), 1);
    }
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
    account.push_config = crate::account::PushConfig::Webhook(PushEndpoint {
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
    assert!(rows_empty(&account));
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
    lease(&account, "h", "/me/events", "one");
    lease(&account, "h", "/me/messages", "two");
    unsubscribe_graph(account.clone(), handle)
        .await
        .expect("delete loop succeeds");
    assert!(rows_empty(&account));
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
    lease(&account, "h", "/me/events", "fails");
    lease(&account, "h", "/me/messages", "succeeds");
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
    lease(&account, "h", "/me/events", "vanished");
    lease(&account, "h", "/me/messages", "live");

    unsubscribe_graph(account.clone(), handle)
        .await
        .expect("a row the server already dropped is not a teardown failure");
    assert!(rows_empty(&account));
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].url.ends_with("/subscriptions/live"));
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

/// Release forgets only the rows whose DELETE was confirmed. A failed DELETE
/// condemns the lease, keeping the row reachable, so retrying the same handle
/// finishes the cleanup rather than orphaning notifications until Graph's
/// expiry window closes; the retry re-deletes only what is left.
#[tokio::test]
async fn a_failed_release_keeps_the_undeleted_row_for_a_retry() {
    let client = GraphClient::new("token");
    client.script_rest([deleted(), server_error(), deleted(), deleted()]);
    let account = webhook_account(&client);
    let handle = SubscriptionHandle("h".to_string());
    lease(&account, "h", "/me/contacts", "deleted");
    lease(&account, "h", "/me/events", "retry-me");
    lease(&account, "h", "/me/messages", "deleted-too");

    unsubscribe_graph(account.clone(), handle.clone())
        .await
        .expect_err("the failed DELETE is reported");
    assert_eq!(
        row_ids(&account),
        [("/me/events".to_string(), "retry-me".to_string())]
    );
    let rows = account.webhooks.store().rows_for_tests();
    assert!(rows["/me/events"].leases[&handle].condemned);
    assert!(
        !account.webhooks.has_live(),
        "a condemned lease is not renewed for"
    );

    unsubscribe_graph(account.clone(), handle)
        .await
        .expect("the retry finishes");
    assert!(rows_empty(&account));
    let requests = urls(&client);
    assert_eq!(requests.len(), 4, "{requests:?}");
    assert!(requests[3].1.ends_with("/subscriptions/retry-me"));
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

// ---- cancellation safety of the webhook `push_subscribe` ----------------
//
// The scripted-wire helpers (`webhook_account`, `created`, `deleted`,
// `settle`, `rows_empty`) live in `fixtures`; the renewal suite uses them too.

fn two_resources() -> Vec<CursorScope> {
    vec![
        email_scope("inbox"),
        CursorScope::FolderType {
            folder: FolderId("contacts".to_string()),
            ty: ObjectType::Contact,
        },
    ]
}

/// A future dropped after its first poll, while the create loop has not run
/// to its end, must still complete the loop and roll back what it created
/// (the second create fails, so the first must be deleted). Inline, the drop
/// unwinds the loop wherever it is and the first subscription stays live on
/// Graph until its expiry.
///
/// A parked (`Pending`) create cannot be used here: the spawned task is
/// deliberately not cancelled by the drop, so it would wait on that create
/// too, and the scripted wire cannot release it.
///
/// Fails if `subscribe_graph` awaits `create_and_register` inline instead of
/// handing it to `tokio::spawn` (the first poll then either completes the
/// whole flow, failing the pending assertion, or is dropped mid-way).
#[tokio::test]
async fn dropping_subscribe_mid_create_loop_rolls_back_the_earlier_creates() {
    let client = GraphClient::new("token");
    client.script_rest([
        created("first"),
        ScriptedRestResponse::json(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error":{"code":"ErrorInternalServerError","message":"no"}}),
        ),
        deleted(),
    ]);
    let account = webhook_account(&client);

    let mut fut = Box::pin(push_subscribe(account.clone(), two_resources()));
    assert!(
        futures::FutureExt::now_or_never(fut.as_mut()).is_none(),
        "the request is handed to a task and the caller waits"
    );
    drop(fut);

    assert!(
        settle(|| client.wire_attempts() == 3).await,
        "the abandoned request must still issue the rollback DELETE"
    );
    let requests = client.take_rest_requests();
    let last = requests.last().expect("requests recorded");
    assert_eq!(last.method.as_str(), "DELETE");
    assert!(last.url.ends_with("/subscriptions/first"));
    assert!(rows_empty(&account));
}

/// A future dropped after the creates but while registration is blocked must
/// not leave a registered group behind a handle nobody received.
///
/// Fails if the abandoned-result teardown in `run_subscribe` (the
/// `result_tx.send` failure arm) is removed.
#[tokio::test]
async fn dropping_subscribe_before_registration_tears_down_the_orphan() {
    let client = GraphClient::new("token");
    client.script_rest([created("first"), created("second"), deleted(), deleted()]);
    let account = webhook_account(&client);

    let registration_blocked = account.webhooks.store().commit_gate.write().await;
    let caller = tokio::spawn(push_subscribe(account.clone(), two_resources()));
    assert!(
        settle(|| client.wire_attempts() == 2).await,
        "both creates done"
    );
    caller.abort();
    let _ = caller.await;
    drop(registration_blocked);

    assert!(
        settle(|| client.wire_attempts() == 4).await,
        "the orphaned group's subscriptions are deleted"
    );
    assert!(
        settle(|| rows_empty(&account)).await,
        "no group stays registered under a handle nobody holds"
    );
}

/// The result was delivered into the channel but the future was dropped
/// before it took it. A successful `oneshot::send` is not receipt, so the
/// task waits for the waiter's ack.
///
/// Fails if `run_subscribe` stops waiting for `ack_rx` (treating a
/// successful send as receipt).
#[tokio::test]
async fn dropping_subscribe_with_the_result_in_flight_tears_down_the_group() {
    let client = GraphClient::new("token");
    client.script_rest([created("only"), deleted()]);
    let account = webhook_account(&client);

    let mut fut = Box::pin(push_subscribe(account.clone(), vec![email_scope("inbox")]));
    assert!(
        futures::FutureExt::now_or_never(fut.as_mut()).is_none(),
        "spawned, awaiting"
    );
    assert!(
        settle(|| !rows_empty(&account)).await,
        "the task registered the group and delivered its result"
    );
    drop(fut);

    assert!(
        settle(|| rows_empty(&account)).await,
        "an unreceived handle's group is retired"
    );
    let requests = client.take_rest_requests();
    assert_eq!(requests.last().expect("recorded").method.as_str(), "DELETE");
}

/// The normal path must disarm: a caller that receives the handle keeps the
/// registered group and nothing is deleted behind its back.
///
/// Fails if the waiter stops acking (`ack_tx.send`), because the task then
/// retires a group the caller legitimately holds.
#[tokio::test]
async fn a_received_subscribe_is_not_torn_down() {
    let client = GraphClient::new("token");
    client.script_rest([created("only")]);
    let account = webhook_account(&client);

    let subscription = push_subscribe(account.clone(), vec![email_scope("inbox")])
        .await
        .expect("subscribe succeeds");
    let handle = subscription.handle().cloned().expect("a handle");
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert!(!account.webhooks.store().leased_by(&handle).is_empty());
    assert_eq!(client.wire_attempts(), 1, "no DELETE was issued");
}

/// Nothing is written until the future is first polled, so a future dropped
/// before its first poll has nothing to leak.
#[tokio::test]
async fn a_subscribe_dropped_before_its_first_poll_touches_nothing() {
    let client = GraphClient::new("token");
    client.script_rest([]);
    let account = webhook_account(&client);

    drop(push_subscribe(account.clone(), two_resources()));
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert_eq!(client.wire_attempts(), 0);
    assert!(rows_empty(&account));
}

/// A closed account refuses before creating anything server-side.
///
/// Fails if the early `shutdown.is_cancelled()` check in `subscribe_graph` is
/// removed.
#[tokio::test]
async fn subscribing_on_a_closed_account_creates_nothing() {
    let client = GraphClient::new("token");
    client.script_rest([]);
    let account = webhook_account(&client);
    account.shutdown.cancel();

    assert!(
        push_subscribe(account.clone(), two_resources())
            .await
            .is_err()
    );
    assert_eq!(client.wire_attempts(), 0);
}

/// `close()` began while a subscribe was already past its creates: the
/// registration must refuse and roll back instead of installing a group the
/// finished `close()` walk will never see.
///
/// Fails if the `shutdown.is_cancelled()` check under the write lock in
/// `create_and_register` is removed.
#[tokio::test]
async fn a_subscribe_racing_close_rolls_back_instead_of_registering() {
    let client = GraphClient::new("token");
    client.script_rest([created("only"), deleted()]);
    let account = webhook_account(&client);

    let registration_blocked = account.webhooks.store().commit_gate.write().await;
    let caller = tokio::spawn(push_subscribe(account.clone(), vec![email_scope("inbox")]));
    assert!(settle(|| client.wire_attempts() == 1).await, "create done");
    account.shutdown.cancel();
    drop(registration_blocked);

    let result = caller.await.expect("caller not aborted");
    assert!(result.is_err(), "a closing account hands out no handle");
    assert_eq!(client.wire_attempts(), 2, "the create was rolled back");
    assert!(rows_empty(&account));
}

/// `close()`'s retire walk cancels the token first: that is what pairs with
/// the registration-time check above.
///
/// Fails if `retire_all_graph_subscriptions` stops cancelling the token.
#[tokio::test]
async fn retiring_all_subscriptions_cancels_the_shutdown_token_first() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    retire_all_graph_subscriptions(&account).await;
    assert!(account.shutdown.is_cancelled());
}

// ---- orphan handles: teardown on an instance that never saw the subscribe ----

fn urls(client: &GraphClient) -> Vec<(String, String)> {
    client
        .take_rest_requests()
        .into_iter()
        .map(|request| (request.method, request.url))
        .collect()
}

/// The handle `push_subscribe` returns must name the server ids, or a reopened
/// instance has nothing to delete by.
#[tokio::test]
async fn a_subscribed_handle_embeds_its_account_and_the_created_server_ids() {
    let client = GraphClient::new("token");
    client.attach_account(AccountId("acct-a".to_string()));
    client.script_rest([created("sub-a")]);
    let account = webhook_account(&client);
    let subscription = push_subscribe(account.clone(), vec![email_scope("inbox")])
        .await
        .expect("subscribe");
    let handle = subscription.handle().cloned().expect("a handle");
    assert_eq!(
        decode_handle(&handle),
        DecodedHandle::Graph {
            account: Some(account_binding(&AccountId("acct-a".to_string()))),
            ids: vec!["sub-a".to_string()],
        }
    );
    assert!(
        !account.webhooks.store().leased_by(&handle).is_empty(),
        "the lease is held under the returned handle"
    );
}

/// The filed defect: the old instance's teardown failed, the account was
/// reopened, and the engine retries on the NEW instance, whose map is empty.
#[tokio::test]
async fn an_orphan_handle_is_torn_down_by_a_fresh_instance() {
    let client = GraphClient::new("token");
    client.script_rest([deleted(), deleted()]);
    let fresh = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let orphan = graph_handle("ab12", None, ["old-one", "old-two"]);

    push_unsubscribe(fresh, orphan)
        .await
        .expect("orphan teardown succeeds");
    let requests = urls(&client);
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(requests[0].0, "DELETE");
    assert!(requests[0].1.ends_with("/subscriptions/old-one"));
    assert_eq!(requests[1].0, "DELETE");
    assert!(requests[1].1.ends_with("/subscriptions/old-two"));
}

/// A row Graph already expired is done, and its siblings are still deleted.
#[tokio::test]
async fn an_orphan_teardown_treats_404_and_410_as_done() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::GONE,
            serde_json::json!({"error":{"code":"Gone","message":"gone"}}),
        ),
        deleted(),
    ]);
    let fresh = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    push_unsubscribe(fresh, graph_handle("ab12", None, ["a", "b", "c"]))
        .await
        .expect("already-gone rows are not failures");
    assert_eq!(client.take_rest_requests().len(), 3);
}

/// A failed DELETE is returned (the engine keeps the orphan and retries), but
/// it does not stop the remaining ids being attempted.
#[tokio::test]
async fn an_orphan_teardown_attempts_every_id_and_reports_a_failure() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error":{"code":"InternalServerError","message":"no"}}),
        ),
        deleted(),
    ]);
    let fresh = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    push_unsubscribe(fresh, graph_handle("ab12", None, ["a", "b"]))
        .await
        .expect_err("the failed DELETE is reported");
    assert_eq!(client.take_rest_requests().len(), 2);
}

/// Two live instances during a reopen: the old instance's orphan, retried on
/// the new one, deletes the OLD ids only. The new instance's own subscription
/// survives, and an id this instance still owns is never deleted through a
/// handle that is not the one it registered.
#[tokio::test]
async fn an_orphan_never_deletes_what_this_instance_owns() {
    let client = GraphClient::new("token");
    client.script_rest([deleted()]);
    let new_instance = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let own = graph_handle("cd34", None, ["new-sub"]);
    lease(&new_instance, &own.0, "/me/events", "new-sub");

    // A stale or forged handle that names both the old id and the live one.
    push_unsubscribe(
        new_instance.clone(),
        graph_handle("ab12", None, ["old-sub", "new-sub"]),
    )
    .await
    .expect("orphan teardown");

    let requests = urls(&client);
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(requests[0].1.ends_with("/subscriptions/old-sub"));
    assert!(
        !new_instance.webhooks.store().leased_by(&own).is_empty(),
        "the live lease is untouched"
    );
}

/// The renewal session replaced a vanished row on the old account, that
/// account's teardown never landed, and the engine retries the handle on a
/// reopened account of the same factory. The handle string names only the
/// subscribe-time id; the store knows the row's current id, so the retry
/// deletes the replacement rather than the long-gone original.
#[tokio::test]
async fn a_retried_release_on_a_reopened_account_deletes_the_recreated_row() {
    let client = GraphClient::new("token");
    client.script_rest([deleted()]);
    let old = webhook_account(&client);
    let handle = graph_handle("ab12", None, ["original"]);
    lease(&old, &handle.0, "/me/events", "original");
    // What a recreate installs.
    old.webhooks.store().edit_for_tests(|rows| {
        rows.get_mut("/me/events").expect("row").server_id = "recreated".to_string();
    });

    let fresh = reopened(&old, &client);
    push_unsubscribe(fresh, handle)
        .await
        .expect("the retried release succeeds");
    let requests = urls(&client);
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(requests[0].1.ends_with("/subscriptions/recreated"));
    assert!(rows_empty(&old));
}

/// A row whose renewal failed terminally stays for `close()` to reach:
/// a subscription we stopped renewing is one Graph still delivers.
#[tokio::test]
async fn close_deletes_a_subscription_whose_renewal_failed_terminally() {
    let client = GraphClient::new("token");
    client.script_rest([deleted()]);
    let account = webhook_account(&client);
    lease(&account, "h", "/me/events", "only");
    account.webhooks.store().edit_for_tests(|rows| {
        rows.get_mut("/me/events").expect("row").terminated = true;
    });
    assert!(!account.webhooks.has_live());

    retire_all_graph_subscriptions(&account).await;
    let requests = urls(&client);
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0].0, "DELETE");
    assert!(requests[0].1.ends_with("/subscriptions/only"));
    assert!(rows_empty(&account));
}

// ---- one row per resource, shared by every lease --------------------------

/// The reopen overlap. Graph refuses a second subscription for a resource,
/// and the engine subscribes on the replacement account BEFORE it tears the
/// old account's handles down. The replacement must attach to the live row
/// without a POST, the old handle's release must not delete it, and the old
/// account's `close()` must leave it to the replacement.
///
/// The script holds no 409: a POST for the shared resource would consume the
/// DELETE meant for the end and fail the assertions.
#[tokio::test]
async fn a_reopen_attaches_to_the_old_accounts_rows_and_keeps_them() {
    let client = GraphClient::new("token");
    client.script_rest([created("shared"), deleted()]);
    let old = webhook_account(&client);
    let old_handle = push_subscribe(old.clone(), vec![email_scope("inbox")])
        .await
        .expect("first subscribe")
        .handle()
        .cloned()
        .expect("a handle");

    let new = reopened(&old, &client);
    let subscription = push_subscribe(new.clone(), vec![email_scope("inbox")])
        .await
        .expect("the replacement attaches");
    let new_handle = subscription.handle().cloned().expect("a handle");
    assert_eq!(subscription.outcomes().succeeded().len(), 1);
    assert!(matches!(
        decode_handle(&new_handle),
        DecodedHandle::Graph { ids, .. } if ids == ["shared"]
    ));
    assert_eq!(client.wire_attempts(), 1, "no second POST");

    // The engine's old-side teardown, then the old account's close.
    push_unsubscribe(old.clone(), old_handle.clone())
        .await
        .expect("release");
    retire_all_graph_subscriptions(&old).await;
    assert_eq!(client.wire_attempts(), 1, "the shared row was not deleted");
    assert_eq!(
        row_ids(&new),
        [(
            "/me/mailFolders/inbox/messages".to_string(),
            "shared".to_string()
        )]
    );
    assert!(new.webhooks.has_live());

    // A repeated release of the old handle finds no lease and decodes the
    // shared id, which the store still holds: nothing is deleted.
    push_unsubscribe(old, old_handle).await.expect("repeat");
    assert_eq!(client.wire_attempts(), 1);

    push_unsubscribe(new.clone(), new_handle)
        .await
        .expect("last release");
    let requests = urls(&client);
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(requests[1].0, "DELETE");
    assert!(requests[1].1.ends_with("/subscriptions/shared"));
    assert!(rows_empty(&new));
}

/// Two subscribes whose scopes share a resource: every calendar maps to the
/// mailbox-wide `/events`, so disjoint calendar sets meet on one row. The
/// second attaches, and each release only drops its own lease until the last.
#[tokio::test]
async fn overlapping_subscribes_share_one_row_per_resource() {
    let client = GraphClient::new("token");
    client.script_rest([created("events"), deleted()]);
    let account = webhook_account(&client);
    let first = push_subscribe(account.clone(), vec![calendar_scope("a")])
        .await
        .expect("first")
        .handle()
        .cloned()
        .expect("a handle");
    let second = push_subscribe(account.clone(), vec![calendar_scope("b")])
        .await
        .expect("the second attaches instead of answering 409")
        .handle()
        .cloned()
        .expect("a handle");
    assert_eq!(client.wire_attempts(), 1);
    assert_ne!(first, second);

    unsubscribe_graph(account.clone(), first)
        .await
        .expect("release first");
    assert_eq!(client.wire_attempts(), 1, "the other lease keeps the row");
    let rows = account.webhooks.store().rows_for_tests();
    assert_eq!(
        rows["/me/events"].leases[&second].scopes,
        [calendar_scope("b")]
    );

    unsubscribe_graph(account.clone(), second)
        .await
        .expect("release second");
    assert_eq!(client.wire_attempts(), 2);
    assert!(rows_empty(&account));
}

/// A row left with only condemned leases (its DELETE failed, and may or may
/// not have taken effect) is not handed to a new subscriber as coverage: the
/// subscribe finishes the DELETE first, then creates fresh, and the condemned
/// lease's debt is paid with it.
#[tokio::test]
async fn a_subscribe_over_a_condemned_row_finishes_its_delete_first() {
    let client = GraphClient::new("token");
    client.script_rest([server_error(), deleted(), created("fresh")]);
    let account = webhook_account(&client);
    lease(&account, "old", "/me/mailFolders/inbox/messages", "stale");
    unsubscribe_graph(account.clone(), SubscriptionHandle("old".to_string()))
        .await
        .expect_err("the DELETE failed");

    let handle = push_subscribe(account.clone(), vec![email_scope("inbox")])
        .await
        .expect("subscribe")
        .handle()
        .cloned()
        .expect("a handle");
    let requests = urls(&client);
    let methods: Vec<&str> = requests.iter().map(|(m, _)| m.as_str()).collect();
    assert_eq!(methods, ["DELETE", "DELETE", "POST"]);
    assert!(requests[1].1.ends_with("/subscriptions/stale"));
    let rows = account.webhooks.store().rows_for_tests();
    let row = &rows["/me/mailFolders/inbox/messages"];
    assert_eq!(row.server_id, "fresh");
    assert_eq!(row.leases.len(), 1, "the condemned lease went with its row");
    assert!(row.leases.contains_key(&handle));
}

/// Attaching to a row whose renewal failed terminally must not hand out
/// coverage nobody renews: the subscribe recovers it with a PATCH, and the
/// recovered row owes every holder a gap.
#[tokio::test]
async fn a_subscribe_over_a_terminated_row_recovers_it() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        serde_json::json!({"id":"dead","expirationDateTime":"2099-01-01T00:00:00Z"}),
    )]);
    let account = webhook_account(&client);
    lease(&account, "old", "/me/mailFolders/inbox/messages", "dead");
    account.webhooks.store().edit_for_tests(|rows| {
        rows.get_mut("/me/mailFolders/inbox/messages")
            .expect("row")
            .terminated = true;
    });

    push_subscribe(account.clone(), vec![email_scope("inbox")])
        .await
        .expect("subscribe");
    let requests = urls(&client);
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0].0, "PATCH");
    let rows = account.webhooks.store().rows_for_tests();
    assert!(!rows["/me/mailFolders/inbox/messages"].terminated);
    assert!(account.webhooks.has_live());
    assert_eq!(
        account.webhooks.events_for_tests(),
        2,
        "a gap for the old holder and for the new lease"
    );
}

/// An account dropped without `close()` cannot pin a shared row forever: its
/// leases stop counting as live, so the last real holder's release deletes.
#[tokio::test]
async fn a_dropped_accounts_lease_does_not_keep_a_row_alive() {
    let client = GraphClient::new("token");
    client.script_rest([deleted()]);
    let survivor = webhook_account(&client);
    lease(&survivor, "kept", "/me/events", "shared");
    {
        let dropped = reopened(&survivor, &client);
        lease(&dropped, "abandoned", "/me/events", "shared");
    }
    unsubscribe_graph(survivor.clone(), SubscriptionHandle("kept".to_string()))
        .await
        .expect("release");
    assert_eq!(client.wire_attempts(), 1, "the row was deleted");
    assert!(rows_empty(&survivor));
}

/// A handle that is not a well-formed webhook handle chooses nothing to
/// delete, whatever it contains.
#[tokio::test]
async fn a_forged_handle_issues_no_requests() {
    let client = GraphClient::new("token");
    let fresh = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    for raw in [
        "graph1:ab12:../me/messages",
        "graph1:ab12:a?x=1",
        "graph1:nothex:id",
        "h",
    ] {
        push_unsubscribe(fresh.clone(), SubscriptionHandle(raw.to_string()))
            .await
            .expect("unrecognized handles are a no-op");
    }
    assert!(client.take_rest_requests().is_empty());
}

/// A handle names the account that minted it. Handed to an instance of a
/// DIFFERENT account - reachable with this account's credentials when both
/// sit in one tenant - it deletes nothing; handed to an instance of the same
/// account, as a reopen does, it tears its orphan down.
#[tokio::test]
async fn an_orphan_bound_to_another_account_deletes_nothing() {
    let client = GraphClient::new("token");
    client.attach_account(AccountId("acct-a".to_string()));
    client.script_rest([deleted()]);
    let instance = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);

    let foreign = account_binding(&AccountId("acct-b".to_string()));
    push_unsubscribe(
        instance.clone(),
        graph_handle("ab12", Some(&foreign), ["their-sub"]),
    )
    .await
    .expect("another account's handle is not this instance's to retire");
    assert!(
        client.take_rest_requests().is_empty(),
        "another account's subscription must not be deleted"
    );

    let own = account_binding(&AccountId("acct-a".to_string()));
    push_unsubscribe(instance, graph_handle("ab12", Some(&own), ["my-old-sub"]))
        .await
        .expect("the same account's orphan tears down");
    let requests = urls(&client);
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(requests[0].1.ends_with("/subscriptions/my-old-sub"));
}

/// A reopen can switch the push mode. The handle decides, not the mode: a
/// webhook orphan on an EWS instance still deletes, and an EWS handle on a
/// webhook instance touches nothing (EWS state dies with the connection).
#[tokio::test]
async fn orphan_routing_follows_the_handle_not_the_instance_mode() {
    let client = GraphClient::new("token");
    client.script_rest([deleted()]);
    let ews = GraphAccount::new_for_tests(client.clone(), PushMode::EwsStreaming);
    push_unsubscribe(ews, graph_handle("ab12", None, ["old"]))
        .await
        .expect("teardown");
    assert_eq!(client.take_rest_requests().len(), 1);

    let webhook = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    push_unsubscribe(webhook, ews_handle("ab12"))
        .await
        .expect("no-op");
    assert!(client.take_rest_requests().is_empty());
}
