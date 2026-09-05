//! The mode-agnostic `push_subscribe` contract: which refusals are per scope
//! and which are per request, in both push modes.

use bifrost_types::{
    AccountErrorKind, AccountOperation, CursorScope, ErrorScope, FolderId, ObjectType,
};

use crate::account::push::common::{PushEndpoint, new_handle};
use crate::account::push::dispatch::push_subscribe;
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};

use super::fixtures::email_scope;

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

/// `Err(_)` from `push_subscribe` means nothing was subscribed. That has to
/// hold for the scopes that die INSIDE an arm too, not only for the
/// pre-dispatch poll-only filter: when no scope resolves to a Graph
/// subscription resource the arm has no handle to hand back, and answering
/// `Ok` with a handle-less `PushSubscription` gave a caller matching on
/// success an all-failed ledger and nothing to unsubscribe.
#[tokio::test]
async fn a_webhook_request_whose_every_scope_dies_in_the_arm_is_a_whole_request_error() {
    let client = GraphClient::new("token");
    // An empty script: a subscription create here would panic in the
    // dispatcher, which is the assertion that no resource was resolved.
    client.script_rest([]);
    let mut account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/webhook".to_string(),
        client_state: "secret".to_string(),
    });
    // Neither is a `CursorScope::Folder`, so both pass the pre-dispatch
    // filter and die inside `subscribe_graph`: the first has no resource for
    // its object type, the second names a shared mailbox this account does
    // not configure.
    let shapeless = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Mailbox,
    };
    let stale_foreign = CursorScope::FolderType {
        folder: crate::account::foreign::encode_foreign("other@contoso.com", "AAMk"),
        ty: ObjectType::Email,
    };
    let error = push_subscribe(account, vec![shapeless.clone(), stale_foreign])
        .await
        .expect_err("no scope was subscribable, so nothing was subscribed");
    // The kind is the first failure's own classification, promoted verbatim,
    // and it stays correlated to the scope that produced it.
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
    ));
    assert_eq!(error.scope(), Some(&ErrorScope::Cursor(shapeless)));
    // The other scope's diagnosis rides along as secondary chain evidence
    // rather than dying with the ledger.
    assert!(
        error.chain().iter().count() > 1,
        "the sibling failure's causes must survive: {:?}",
        error.chain()
    );
}

/// The same rule on the EWS arm, where every scope is refused by
/// `translateExchangeIds` rather than by local resource construction.
#[tokio::test]
async fn an_ews_request_whose_every_translation_is_refused_is_a_whole_request_error() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        serde_json::json!({"value":[{
            "sourceId":"inbox",
            "errorDetails":{"code":"ErrorInvalidIdMalformed","message":"Id is malformed."}
        }]}),
    )]);
    let account = GraphAccount::new_for_tests(client, PushMode::EwsStreaming);
    let error = push_subscribe(account.clone(), vec![email_scope("inbox")])
        .await
        .expect_err("a request whose only folder cannot be translated subscribes nothing");
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert_eq!(
        error.scope(),
        Some(&ErrorScope::Cursor(email_scope("inbox")))
    );
    assert!(
        account.ews_subscriptions.read().await.is_empty(),
        "no registration survives a request that subscribed nothing"
    );
}
