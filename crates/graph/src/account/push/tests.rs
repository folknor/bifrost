//! The push suite, kept as one module across the split: the webhook
//! subscribe / teardown / renewal legs and the EWS translation legs share
//! the same account fixtures and scope builders.

use std::collections::HashMap;
use std::time::Duration;

use bifrost_types::{
    AccountErrorKind, AccountOperation, Cause, CursorScope, ErrorScope, FolderId, ObjectType,
    ProtocolErrorKind, SubscriptionHandle, TransmissionState, WatchEvent,
};

use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};

use super::common::*;
use super::dispatch::*;
use super::ews::*;
use super::renewal::*;
use super::webhook::*;

/// The EWS Subscribe boundary and the webhook boundary must agree on
/// what "subscribable" means. A non-folder scope contributes nothing
/// to `FolderIds` (empty element -> EWS rejects the whole request),
/// and a foreign folder would be resolved against the PRIMARY
/// mailbox's namespace because the Subscribe path sends no routing
/// headers. Both must be refused locally as `Unsupported`, not
/// converted into a remote failure that reads as a provider fault.
#[test]
fn ews_subscribe_accepts_only_primary_mailbox_folder_scopes() {
    let primary = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Email,
    };
    // The accepted scope also yields the id the translation request
    // sends: bare, never the raw foreign-encoded string.
    assert_eq!(
        ews_subscribable_folder_id(&primary).as_deref(),
        Some("inbox")
    );

    let foreign = CursorScope::FolderType {
        folder: crate::account::foreign::encode_foreign("shared@contoso.com", "AAMk"),
        ty: ObjectType::Email,
    };
    assert!(ews_subscribable_folder_id(&foreign).is_none());

    // A public-folder scope, and the account-wide scope the webhook
    // path already rejects loudly.
    assert!(ews_subscribable_folder_id(&CursorScope::Folder(FolderId("pf".to_string()))).is_none());
    assert!(ews_subscribable_folder_id(&CursorScope::Account).is_none());
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

/// A scope that cannot resolve to a Graph resource must never be
/// silently dropped from an otherwise-successful subscription: the
/// caller would believe it has push coverage it does not have. It lands
/// in the failed lane, correlated to the scope, and the handle covers
/// only what actually subscribed.
#[tokio::test]
async fn an_unresolvable_scope_is_reported_and_not_covered_by_the_handle() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::CREATED,
        serde_json::json!({"id":"first","expirationDateTime":"2099-01-01T00:00:00Z"}),
    )]);
    let mut account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/webhook".to_string(),
        client_state: "secret".to_string(),
    });
    let inbox = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Email,
    };
    // `CursorScope::Account` is not a `FolderType`, so it maps to no
    // Graph subscription resource.
    let result = push_subscribe(account, vec![CursorScope::Account, inbox.clone()])
        .await
        .expect("an unresolvable scope does not sink its resolvable sibling");

    assert!(result.handle.is_some(), "the inbox scope did subscribe");
    assert_eq!(
        result
            .outcomes
            .succeeded()
            .iter()
            .map(|success| success.output.clone())
            .collect::<Vec<_>>(),
        vec![inbox]
    );
    let failed = result.outcomes.failed();
    assert_eq!(failed.len(), 1);
    assert!(matches!(
        failed[0].error.kind(),
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
    ));
    assert!(matches!(
        failed[0].error.scope(),
        Some(ErrorScope::Cursor(CursorScope::Account))
    ));
}

/// Nothing in the request was ever subscribable, so there is no partial
/// coverage to report and no handle to hold. That stays a whole-request
/// refusal the caller can act on rather than an empty success.
#[tokio::test]
async fn an_all_poll_only_request_is_still_refused_outright() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let error = push_subscribe(
        account,
        vec![CursorScope::Folder(FolderId("public".to_string()))],
    )
    .await
    .expect_err("a request of only poll-only scopes is refused");
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
    ));
}

/// The poll-only public-folder refusal is per scope, not per request.
/// One stale public folder in a mixed list used to disable push for
/// every valid sibling, which is precisely the symptom the per-scope
/// outcome contract exists to eliminate.
#[tokio::test]
async fn a_poll_only_public_folder_does_not_sink_its_valid_siblings() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::CREATED,
        serde_json::json!({"id":"first","expirationDateTime":"2099-01-01T00:00:00Z"}),
    )]);
    let mut account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/webhook".to_string(),
        client_state: "secret".to_string(),
    });
    let public = CursorScope::Folder(FolderId("public".to_string()));
    let inbox = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Email,
    };
    let result = push_subscribe(account, vec![public.clone(), inbox.clone()])
        .await
        .expect("a poll-only scope is a per-scope refusal");

    assert!(result.handle.is_some());
    assert_eq!(result.outcomes.succeeded().len(), 1);
    assert_eq!(result.outcomes.succeeded()[0].output, inbox);
    assert_eq!(result.outcomes.failed().len(), 1);
    assert!(matches!(
        result.outcomes.failed()[0].error.scope(),
        Some(ErrorScope::Cursor(scope)) if *scope == public
    ));
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

#[tokio::test]
async fn exchange_id_translation_posts_each_wire_chunk_and_accumulates_answers() {
    let client = GraphClient::new("token");
    let pending: Vec<_> = (0..=TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
        .map(|index| {
            (
                bifrost_types::BatchItemId(index.to_string()),
                CursorScope::FolderType {
                    folder: FolderId(format!("f{index}")),
                    ty: ObjectType::Email,
                },
                format!("f{index}"),
            )
        })
        .collect();
    let answer = |start: usize, end: usize| serde_json::json!({"value": (start..end).map(|index| serde_json::json!({"sourceId":format!("f{index}"),"targetId":format!("e{index}")})).collect::<Vec<_>>()});
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            answer(0, TRANSLATE_EXCHANGE_IDS_MAX_INPUTS),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            answer(
                TRANSLATE_EXCHANGE_IDS_MAX_INPUTS,
                TRANSLATE_EXCHANGE_IDS_MAX_INPUTS + 1,
            ),
        ),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::EwsStreaming);
    let translated = translate_ews_scopes(&account, pending, &mut ledger())
        .await
        .expect("wire fan-out translates all folders");
    assert_eq!(translated.len(), TRANSLATE_EXCHANGE_IDS_MAX_INPUTS + 1);
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.url.ends_with("/me/translateExchangeIds"))
    );
    assert_eq!(
        requests[0]
            .body
            .as_ref()
            .and_then(|body| body["inputIds"].as_array())
            .map(Vec::len),
        Some(TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
    );
    assert_eq!(
        requests[1]
            .body
            .as_ref()
            .and_then(|body| body["inputIds"].as_array())
            .map(Vec::len),
        Some(1)
    );
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

#[tokio::test]
async fn a_public_folder_scope_is_rejected_in_both_push_modes() {
    // Refused before mode dispatch, so neither mode can start a worker
    // for a folder it can never observe. A request made only of such
    // scopes has no subscribable remainder, so it is refused outright.
    for mode in [PushMode::GraphSubscriptions, PushMode::EwsStreaming] {
        let account = GraphAccount::new_for_tests(GraphClient::new("token"), mode);
        let err = push_subscribe(
            account.clone(),
            vec![CursorScope::Folder(FolderId("AAMkPF=".to_string()))],
        )
        .await
        .expect_err("public-folder push is unsupported");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
        ));
        assert!(account.graph_subscriptions.read().await.is_empty());
        assert!(account.ews_subscriptions.read().await.is_empty());
    }
}

/// The EWS half of the per-scope contract. A foreign (shared-mailbox)
/// folder cannot be routed by this Subscribe, and used to refuse the
/// whole request; it must now fail alone while its primary-mailbox
/// sibling gets a live EWS subscription.
#[tokio::test]
async fn an_unsubscribable_ews_scope_does_not_sink_its_valid_siblings() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        serde_json::json!({"value":[{"sourceId":"rest-inbox","targetId":"ews-inbox"}]}),
    )]);
    let account = GraphAccount::new_for_tests(client, PushMode::EwsStreaming);
    let inbox = email_scope("rest-inbox");
    // `CursorScope::Account` contributes no folder id at all.
    let result = push_subscribe(account.clone(), vec![CursorScope::Account, inbox.clone()])
        .await
        .expect("an unsubscribable scope is a per-scope refusal");

    let handle = result.handle.expect("the primary folder did subscribe");
    assert_eq!(
        result.outcomes.succeeded().len(),
        1,
        "only the inbox scope is covered"
    );
    assert_eq!(result.outcomes.succeeded()[0].output, inbox);
    assert_eq!(result.outcomes.failed().len(), 1);
    assert!(matches!(
        result.outcomes.failed()[0].error.scope(),
        Some(ErrorScope::Cursor(CursorScope::Account))
    ));
    let registered = account.ews_subscriptions.read().await;
    let state = registered.get(&handle).expect("the group is registered");
    assert_eq!(state.scopes.len(), 1);
    assert_eq!(state.scopes[0].ews_folder_id, "ews-inbox");
}

#[test]
fn subscription_handles_are_distinct_hex_tokens() {
    let first = new_handle().expect("rng");
    let second = new_handle().expect("rng");
    // 16 random bytes, lowercase hex: 32 digits, parseable as a u128.
    assert_eq!(first.0.len(), 32);
    assert!(u128::from_str_radix(&first.0, 16).is_ok(), "{}", first.0);
    assert_ne!(first, second);
}

fn state(server_id: &str, resource: &str) -> GraphSubscriptionState {
    GraphSubscriptionState {
        server_id: server_id.to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
        resource: resource.to_string(),
        scopes: vec![CursorScope::FolderType {
            folder: FolderId(resource.to_string()),
            ty: ObjectType::Email,
        }],
        warned_unparseable_expiry: false,
    }
}

/// A subscription whose expiry is already past, so it is unconditionally
/// inside the renewal threshold.
fn expiring(server_id: &str, resource: &str) -> GraphSubscriptionState {
    GraphSubscriptionState {
        server_id: server_id.to_string(),
        expires_at: "2000-01-01T00:00:00Z".to_string(),
        resource: resource.to_string(),
        scopes: vec![CursorScope::FolderType {
            folder: FolderId(resource.to_string()),
            ty: ObjectType::Email,
        }],
        warned_unparseable_expiry: false,
    }
}

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

/// An empty scope list is not a subscription. Registering one minted a
/// handle over an empty group: teardown had no server id to walk, so the
/// group was never retired and the renewal worker it started was never
/// stopped. Both modes refuse it before any state is installed.
#[tokio::test]
async fn subscribing_to_no_scopes_is_rejected_in_both_modes() {
    for mode in [PushMode::GraphSubscriptions, PushMode::EwsStreaming] {
        let account = GraphAccount::new_for_tests(GraphClient::new("token"), mode);
        let error = push_subscribe(account.clone(), Vec::new())
            .await
            .expect_err("an empty scope list covers nothing");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(account.graph_subscriptions.read().await.is_empty());
        assert!(account.ews_subscriptions.read().await.is_empty());
    }
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

fn email_scope(folder: &str) -> CursorScope {
    CursorScope::FolderType {
        folder: FolderId(folder.to_string()),
        ty: ObjectType::Email,
    }
}

/// What `subscribe_ews` hands the translation phase: every scope already
/// validated and paired with the native id it contributes.
fn pending(folders: &[&str]) -> Vec<PendingEwsScope> {
    folders
        .iter()
        .enumerate()
        .map(|(index, folder)| {
            let scope = email_scope(folder);
            let source_id =
                ews_subscribable_folder_id(&scope).expect("a primary folder scope is pending");
            (
                bifrost_types::BatchItemId(index.to_string()),
                scope,
                source_id,
            )
        })
        .collect()
}

/// A fresh per-request ledger for a `reconcile_translated_ews_scopes`
/// call made outside `push_subscribe`.
fn ledger() -> bifrost_types::BatchOutcomeBuilder<CursorScope> {
    bifrost_types::BatchOutcomeBuilder::new()
}

fn converted(source_id: &str, target_id: &str) -> TranslatedExchangeId {
    TranslatedExchangeId {
        source_id: source_id.to_string(),
        target_id: Some(target_id.to_string()),
        error_details: None,
    }
}

#[test]
fn translated_ews_ids_stay_paired_with_their_graph_scopes_when_response_is_reordered() {
    let translated = vec![
        converted("rest-archive", "ews-archive"),
        converted("rest-inbox", "ews-inbox"),
    ];

    let resolved = reconcile_translated_ews_scopes(
        pending(&["rest-inbox", "rest-archive"]),
        translated,
        &mut ledger(),
    );
    assert_eq!(resolved[0].ews_folder_id, "ews-inbox");
    assert_eq!(resolved[1].ews_folder_id, "ews-archive");
}

/// Graph must answer every id it was given. A dropped answer is the
/// provider breaking its own contract, so it classifies as one - and it
/// still names the folder, because "a subscription failed" without a
/// container is not a diagnosable report.
#[test]
fn a_missing_ews_translation_rejects_the_subscription() {
    let mut outcomes = ledger();
    let resolved =
        reconcile_translated_ews_scopes(pending(&["rest-inbox"]), Vec::new(), &mut outcomes);
    assert!(
        resolved.is_empty(),
        "an unanswered folder is not subscribed"
    );
    let outcomes = outcomes
        .finalize(&push_item_ids(1))
        .expect("every scope is accounted for");
    let error = &outcomes.failed()[0].error;
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    ));
    assert_eq!(
        error.scope(),
        Some(&ErrorScope::Cursor(email_scope("rest-inbox")))
    );
}

/// Graph converts ids one at a time and reports a failure per id inside
/// an otherwise successful 200. The refused id must fail its own
/// subscription with its own scope and Graph's own code - not take the
/// whole response down as an unparseable body, which is what a required
/// `targetId` produced.
#[test]
fn a_per_id_translation_failure_names_its_scope_and_the_provider_code() {
    let translated = vec![
        converted("rest-inbox", "ews-inbox"),
        TranslatedExchangeId {
            source_id: "rest-stale".to_string(),
            target_id: None,
            error_details: Some(ConvertIdError {
                code: Some("ErrorInvalidIdMalformed".to_string()),
                message: Some("Id is malformed.".to_string()),
            }),
        },
    ];

    let mut outcomes = ledger();
    let resolved = reconcile_translated_ews_scopes(
        pending(&["rest-inbox", "rest-stale"]),
        translated,
        &mut outcomes,
    );
    assert_eq!(resolved.len(), 1, "the accepted sibling still subscribes");
    assert_eq!(resolved[0].scope, email_scope("rest-inbox"));
    let outcomes = outcomes
        .finalize(&push_item_ids(2))
        .expect("every scope is accounted for");
    assert_eq!(outcomes.succeeded().len(), 1);
    assert_eq!(outcomes.succeeded()[0].output, email_scope("rest-inbox"));
    assert_eq!(outcomes.failed().len(), 1);
    let error = &outcomes.failed()[0].error;
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert!(error.recovery().is_terminal());
    assert_eq!(
        error.scope(),
        Some(&ErrorScope::Cursor(email_scope("rest-stale")))
    );
    let carries_code = error.chain().iter().any(|cause| {
        matches!(
            cause,
            Cause::Wire(bifrost_types::WireCause::Graph(
                bifrost_types::GraphSignal::Unknown { code }
            )) if code == "ErrorInvalidIdMalformed"
        )
    });
    assert!(carries_code, "the provider code must survive to support");
}

/// The one shape that would otherwise fall through with no id at all.
#[test]
fn a_translation_answer_with_neither_target_nor_error_is_a_contract_violation() {
    let translated = vec![TranslatedExchangeId {
        source_id: "rest-inbox".to_string(),
        target_id: Some("   ".to_string()),
        error_details: None,
    }];
    let mut outcomes = ledger();
    let resolved =
        reconcile_translated_ews_scopes(pending(&["rest-inbox"]), translated, &mut outcomes);
    assert!(resolved.is_empty());
    let outcomes = outcomes
        .finalize(&push_item_ids(1))
        .expect("every scope is accounted for");
    let error = &outcomes.failed()[0].error;
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    ));
}

/// A `restId` is per folder, not per scope, so one answer serves every
/// scope over that folder. Asking twice would waste a slot against
/// Graph's cap for no gain.
#[test]
fn scopes_sharing_a_folder_translate_once_and_both_resolve() {
    let pending = vec![
        (
            bifrost_types::BatchItemId("0".to_string()),
            email_scope("rest-shared"),
            "rest-shared".to_string(),
        ),
        (
            bifrost_types::BatchItemId("1".to_string()),
            CursorScope::FolderType {
                folder: FolderId("rest-shared".to_string()),
                ty: ObjectType::Contact,
            },
            "rest-shared".to_string(),
        ),
    ];
    let chunks = translation_input_chunks(&pending);
    assert_eq!(chunks, vec![vec!["rest-shared".to_string()]]);

    let resolved = reconcile_translated_ews_scopes(
        pending,
        vec![converted("rest-shared", "ews-shared")],
        &mut ledger(),
    );
    assert_eq!(resolved.len(), 2);
    assert!(resolved.iter().all(|s| s.ews_folder_id == "ews-shared"));
}

/// Graph rejects an `inputIds` collection over 1,000 strings outright,
/// so a large mailbox has to fan out. Before chunking, every scope went
/// into one collection and the subscription failed before EWS setup was
/// even attempted.
#[test]
fn translation_requests_stay_inside_the_graph_input_id_cap() {
    let folders: Vec<String> = (0..2_500).map(|n| format!("rest-{n}")).collect();
    let pending: Vec<PendingEwsScope> = folders
        .iter()
        .enumerate()
        .map(|(index, folder)| {
            (
                bifrost_types::BatchItemId(index.to_string()),
                email_scope(folder),
                folder.clone(),
            )
        })
        .collect();

    let chunks = translation_input_chunks(&pending);
    assert_eq!(chunks.len(), 3);
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.len() <= TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
    );
    let flattened: Vec<String> = chunks.into_iter().flatten().collect();
    assert_eq!(flattened, folders, "order and coverage both survive");
}

/// The response Graph documents, decoded end to end: a converted id and
/// a refused one in the same 200. Deserialization must survive the
/// refused entry - the whole finding was that it did not.
#[test]
fn a_mixed_convert_id_result_deserializes() {
    let body = r#"{
        "value": [
            { "sourceId": "rest-a", "targetId": "ews-a" },
            {
                "sourceId": "rest-b",
                "errorDetails": {
                    "code": "ErrorInvalidIdMalformed",
                    "message": "Id is malformed."
                }
            }
        ]
    }"#;
    let response: TranslateExchangeIdsResponse =
        serde_json::from_str(body).expect("a per-id failure is not a parse failure");
    assert_eq!(response.value.len(), 2);
    assert_eq!(response.value[0].target_id.as_deref(), Some("ews-a"));
    assert!(response.value[1].target_id.is_none());
    assert_eq!(
        response.value[1]
            .error_details
            .as_ref()
            .and_then(|details| details.code.as_deref()),
        Some("ErrorInvalidIdMalformed")
    );
}

/// The translation POST creates nothing: it runs before the handle, the
/// local state, and the EWS subscription all exist. A drop mid-flight
/// has no target to reconcile against, so it must stay a plain retry
/// even though `PushSubscribe` is a non-idempotent operation.
#[test]
fn a_dropped_translation_request_retries_instead_of_reconciling() {
    let dropped = crate::error::GraphError::Net(bifrost_net::Error::Network {
        message: "connection reset".to_string(),
        transmission_state: TransmissionState::InFlight,
        source: None,
    });
    let error = into_account_error(dropped, translate_error_context());
    assert!(
        error.recovery().is_retryable(),
        "recovery was {:?}",
        error.recovery()
    );

    // The same drop under the plain PushSubscribe context is the
    // behavior the override corrects, not a coincidence of the kind.
    let dropped = crate::error::GraphError::Net(bifrost_net::Error::Network {
        message: "connection reset".to_string(),
        transmission_state: TransmissionState::InFlight,
        source: None,
    });
    let reconciled = into_account_error(
        dropped,
        GraphErrorContext::graph(AccountOperation::PushSubscribe),
    );
    assert!(reconciled.recovery().requires_reconciliation());
}
