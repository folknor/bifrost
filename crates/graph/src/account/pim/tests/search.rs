use crate::account::pim::search::{kql_quoted, odata_filter, search_message_rows, search_url};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};
use bifrost_types::{
    AccessErrorKind, AccountErrorKind, AccountOperation, ContainerId, ErrorScope, ObjectId,
    RecoveryClass, RemediationAction, RequestErrorKind, SearchFilter, SearchRequest, ThreadId,
};
use serde_json::json;
use std::collections::HashMap;
use std::time::SystemTime;

// The query string carries the KQL phrase percent-encoded; compare
// against the same encoder the production path uses rather than a
// hand-maintained decode table.
fn search_param(url: &str) -> String {
    url.split("$search=")
        .nth(1)
        .expect("url has a $search param")
        .to_string()
}

fn shared_clients_for_tests(mailboxes: &[&str]) -> HashMap<String, GraphClient> {
    mailboxes
        .iter()
        .map(|mailbox| {
            let client = GraphClient::new("token");
            client.script_rest([]);
            ((*mailbox).to_string(), client.for_shared_mailbox(*mailbox))
        })
        .collect()
}

#[tokio::test]
async fn search_walks_shared_mailboxes_and_qualifies_their_ids() {
    let primary = GraphClient::new("token");
    primary.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{ "id": "primary-message", "conversationId": "primary-thread" }],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/messages?$skiptoken=primary"
            }),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "value": [{ "id": "primary-message-2", "conversationId": "primary-thread-2" }] }),
        ),
    ]);
    let shared_root = GraphClient::new("token");
    shared_root.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "shared-message", "conversationId": "shared-thread" }] }),
    )]);
    let mailbox = "shared@contoso.com";
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), shared_root.for_shared_mailbox(mailbox))]),
    );

    let first = search_message_rows(&account, SearchRequest::default(), AccountOperation::Search)
        .await
        .expect("primary page succeeds");
    assert_eq!(first.items[0].id, ObjectId("primary-message".to_string()));
    assert_eq!(
        first.items[0].thread_id,
        Some(ThreadId("primary-thread".to_string()))
    );
    let mut continuation = SearchRequest::default();
    continuation.page_cursor = first.next_cursor;
    let primary_continuation =
        search_message_rows(&account, continuation, AccountOperation::Search)
            .await
            .expect("primary continuation succeeds");
    assert_eq!(
        primary_continuation.items[0].id,
        ObjectId("primary-message-2".to_string())
    );
    let mut continuation = SearchRequest::default();
    continuation.page_cursor = primary_continuation.next_cursor;
    let second = search_message_rows(&account, continuation, AccountOperation::Search)
        .await
        .expect("shared page succeeds");
    assert_eq!(
        second.items[0].id,
        ObjectId(format!("{mailbox}\u{1f}shared-message"))
    );
    assert_eq!(
        second.items[0].thread_id,
        Some(ThreadId(format!("{mailbox}\u{1f}shared-thread")))
    );
    assert!(second.next_cursor.is_none());

    let primary_requests = primary.take_rest_requests();
    assert_eq!(primary_requests.len(), 2);
    assert!(primary_requests[0].url.contains("/me/messages?"));
    assert!(primary_requests[1].url.contains("$skiptoken=primary"));
    let shared_requests = shared_root.take_rest_requests();
    assert_eq!(shared_requests.len(), 1);
    assert!(
        shared_requests[0]
            .url
            .contains("/users/shared%40contoso.com/messages?")
    );
}

/// A full walk across THREE shared mailboxes where every one of them
/// returns results, and where the first shared mailbox pages twice on
/// its own.
///
/// Two things only this shape can catch. A shared mailbox's own
/// `@odata.nextLink` must be followed against THAT mailbox's client -
/// the cursor carries the owner beside the link, and a continuation
/// that forgot the owner would issue the absolute link on the primary
/// (which is armed empty here, so it panics rather than passing).
/// And the hand-off must be `owner -> the NEXT mailbox after it`, not
/// `owner -> the first`: with only two mailboxes an off-by-one that
/// restarted the tail is indistinguishable from a correct walk.
#[tokio::test]
async fn search_pages_within_and_across_three_shared_mailboxes() {
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "p1", "conversationId": "pt" }] }),
    )]);
    let a_root = GraphClient::new("token");
    a_root.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{ "id": "a1", "conversationId": "at" }],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/a%40contoso.com/messages?$skiptoken=a2"
            }),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "value": [{ "id": "a2", "conversationId": "at2" }] }),
        ),
    ]);
    let b_root = GraphClient::new("token");
    b_root.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "b1", "conversationId": "bt" }] }),
    )]);
    let c_root = GraphClient::new("token");
    c_root.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "c1", "conversationId": "ct" }] }),
    )]);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([
            (
                "a@contoso.com".to_string(),
                a_root.for_shared_mailbox("a@contoso.com"),
            ),
            (
                "b@contoso.com".to_string(),
                b_root.for_shared_mailbox("b@contoso.com"),
            ),
            (
                "c@contoso.com".to_string(),
                c_root.for_shared_mailbox("c@contoso.com"),
            ),
        ]),
    );

    // Walk the whole surface, one page at a time, exactly as a consumer
    // would: feed each page's cursor back until there is none.
    let mut ids = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let mut request = SearchRequest::default();
        request.page_cursor = cursor;
        let page = search_message_rows(&account, request, AccountOperation::Search)
            .await
            .expect("every page of the walk succeeds");
        ids.extend(page.items.iter().map(|row| row.id.0.clone()));
        assert!(page.skipped_scopes.is_empty());
        pages += 1;
        assert!(pages <= 8, "the walk must terminate");
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    assert_eq!(pages, 5, "primary, a@ twice, b@, c@");
    assert_eq!(
        ids,
        vec![
            "p1".to_string(),
            "a@contoso.com\u{1f}a1".to_string(),
            "a@contoso.com\u{1f}a2".to_string(),
            "b@contoso.com\u{1f}b1".to_string(),
            "c@contoso.com\u{1f}c1".to_string(),
        ]
    );

    assert_eq!(primary.take_rest_requests().len(), 1);
    let a_requests = a_root.take_rest_requests();
    assert_eq!(a_requests.len(), 2);
    assert!(
        a_requests[0]
            .url
            .contains("/users/a%40contoso.com/messages?"),
        "{}",
        a_requests[0].url
    );
    assert!(
        a_requests[1].url.ends_with("$skiptoken=a2"),
        "the shared mailbox's own continuation is issued verbatim: {}",
        a_requests[1].url
    );
    assert_eq!(b_root.take_rest_requests().len(), 1);
    assert_eq!(c_root.take_rest_requests().len(), 1);
}

/// O-28: search was the one shared-mailbox door where a revoked share
/// escalated account-wide. The dead mailbox stays configured, so the
/// old `Err(NoPermission)` did not just fail one call - every retry
/// and every restarted search died at the same walk position, taking
/// the mailboxes behind it down too. The walk must quarantine the
/// revoked share like its sibling doors do, continue into the next
/// mailbox, and REPORT the skip on the page so "no matches in b@" is
/// distinguishable from "b@ was never searched".
#[tokio::test]
async fn a_revoked_shared_mailbox_is_skipped_and_the_walk_continues() {
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "primary-message", "conversationId": "primary-thread" }] }),
    )]);
    let dead = GraphClient::new("token");
    dead.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        json!({ "error": { "code": "AccessDenied", "message": "delegate access revoked" } }),
    )]);
    let alive = GraphClient::new("token");
    alive.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "alive-message", "conversationId": "alive-thread" }] }),
    )]);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([
            (
                "b@contoso.com".to_string(),
                dead.for_shared_mailbox("b@contoso.com"),
            ),
            (
                "c@contoso.com".to_string(),
                alive.for_shared_mailbox("c@contoso.com"),
            ),
        ]),
    );

    let first = search_message_rows(&account, SearchRequest::default(), AccountOperation::Search)
        .await
        .expect("primary page succeeds");
    assert!(first.skipped_scopes.is_empty());
    let mut continuation = SearchRequest::default();
    continuation.page_cursor = first.next_cursor;
    let resumed = search_message_rows(&account, continuation, AccountOperation::Search)
        .await
        .expect("the walk continues past the revoked mailbox");

    // The page holds the LIVE mailbox's results, owner-qualified.
    assert_eq!(
        resumed.items[0].id,
        ObjectId("c@contoso.com\u{1f}alive-message".to_string())
    );
    assert!(resumed.next_cursor.is_none());
    // The quarantined mailbox is reported, carrying the terminal
    // classification the old code raised account-wide.
    assert_eq!(resumed.skipped_scopes.len(), 1);
    let skip = &resumed.skipped_scopes[0];
    assert_eq!(
        skip.scope,
        ErrorScope::Owner {
            id: ("b@contoso.com".to_string()).into()
        }
    );
    assert!(matches!(
        skip.error.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
    ));
    assert!(matches!(
        skip.error.recovery(),
        RecoveryClass::NoPermission { .. }
    ));
    assert_eq!(skip.error.scope(), Some(&skip.scope));

    // One request per mailbox: the revoked one was asked exactly once,
    // and the walk resumed at the next mailbox in the same call.
    assert_eq!(primary.take_rest_requests().len(), 1);
    assert_eq!(dead.take_rest_requests().len(), 1);
    let alive_requests = alive.take_rest_requests();
    assert_eq!(alive_requests.len(), 1);
    assert!(
        alive_requests[0]
            .url
            .contains("/users/c%40contoso.com/messages?")
    );
}

/// The revoked mailbox at the END of the walk must not turn "search
/// complete, minus one quarantined scope" into an error: the call
/// answers a terminal empty page that still reports the skip.
#[tokio::test]
async fn a_walk_ending_in_a_revoked_mailbox_completes_and_reports_the_skip() {
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "primary-message", "conversationId": "primary-thread" }] }),
    )]);
    let dead = GraphClient::new("token");
    dead.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        json!({ "error": { "code": "AccessDenied", "message": "delegate access revoked" } }),
    )]);
    let mailbox = "shared@contoso.com";
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary,
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), dead.for_shared_mailbox(mailbox))]),
    );

    let mut continuation = SearchRequest::default();
    continuation.page_cursor =
        search_message_rows(&account, SearchRequest::default(), AccountOperation::Search)
            .await
            .expect("primary page succeeds")
            .next_cursor;
    let last = search_message_rows(&account, continuation, AccountOperation::Search)
        .await
        .expect("the walk completes despite the revoked tail mailbox");
    assert!(last.items.is_empty());
    assert!(last.next_cursor.is_none());
    assert_eq!(last.skipped_scopes.len(), 1);
    assert_eq!(
        last.skipped_scopes[0].scope,
        ErrorScope::Owner {
            id: (mailbox.to_string()).into()
        }
    );
}

/// The quarantine is exactly as narrow as its sibling doors': only a
/// permission denial on a FOREIGN mailbox skips. A transient failure
/// there still fails the call - the caller retries the same cursor and
/// can succeed - and a primary-mailbox 403 is a genuine account-level
/// signal, not something a walk may step around.
#[tokio::test]
async fn only_a_foreign_permission_denial_is_skipped() {
    // Transient failure on the shared mailbox: propagates retryable.
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [] }),
    )]);
    let flaky = GraphClient::new("token");
    flaky.script_rest([ScriptedRestResponse::empty(
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
    )]);
    let mailbox = "shared@contoso.com";
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary,
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), flaky.for_shared_mailbox(mailbox))]),
    );
    let mut continuation = SearchRequest::default();
    continuation.page_cursor =
        search_message_rows(&account, SearchRequest::default(), AccountOperation::Search)
            .await
            .expect("primary page succeeds")
            .next_cursor;
    let transient = search_message_rows(&account, continuation, AccountOperation::Search)
        .await
        .expect_err("a transient shared-mailbox failure fails the call");
    assert!(transient.recovery().is_retryable());

    // Permission denial on the PRIMARY mailbox: propagates terminal.
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        json!({ "error": { "code": "AccessDenied", "message": "nope" } }),
    )]);
    let account = GraphAccount::new_for_tests(primary, PushMode::GraphSubscriptions);
    let denied = search_message_rows(&account, SearchRequest::default(), AccountOperation::Search)
        .await
        .expect_err("a primary permission denial fails the call");
    assert!(matches!(
        denied.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
    ));
    assert!(denied.recovery().is_terminal());
}

/// A search cursor names a position in a WALK over several mailboxes,
/// so it is only meaningful against the mailbox list that minted it.
/// Both resuming accounts are armed EMPTY: a cursor that decoded into a
/// plausible-looking position would issue its first request and hit the
/// seam's exhaustion panic rather than quietly returning a page set
/// that silently omits a whole mailbox.
#[tokio::test]
async fn a_search_cursor_from_another_shared_mailbox_set_is_rejected() {
    let minting_primary = GraphClient::new("token");
    minting_primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [] }),
    )]);
    let minting = GraphAccount::new_for_tests_with_shared_clients(
        minting_primary,
        PushMode::GraphSubscriptions,
        shared_clients_for_tests(&["b@contoso.com", "c@contoso.com"]),
    );
    let cursor = search_message_rows(&minting, SearchRequest::default(), AccountOperation::Search)
        .await
        .expect("primary page succeeds")
        .next_cursor
        .expect("the walk continues into the shared mailboxes");

    // Both sets still configure the cursor's own mailbox, so nothing but
    // the pinned set can reject them: `a@` sorts BEFORE the position and
    // would never be searched, and dropping `c@` would end the walk a
    // mailbox early. Either way the caller gets fewer results than it
    // asked for and no signal that it did.
    for mailboxes in [
        vec!["a@contoso.com", "b@contoso.com", "c@contoso.com"],
        vec!["b@contoso.com"],
    ] {
        let primary = GraphClient::new("token");
        primary.script_rest([]);
        let account = GraphAccount::new_for_tests_with_shared_clients(
            primary,
            PushMode::GraphSubscriptions,
            shared_clients_for_tests(&mailboxes),
        );
        let mut request = SearchRequest::default();
        request.page_cursor = Some(cursor.clone());
        let error = search_message_rows(&account, request, AccountOperation::Search)
            .await
            .expect_err("a cursor minted against another mailbox set is rejected");
        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Request(RequestErrorKind::Malformed)
            ),
            "{mailboxes:?}: {:?}",
            error.kind()
        );
    }
}

/// Cursor bytes are request INPUT the caller handed back, so a corrupt
/// or stale one is the caller's to fix - not evidence that Graph broke
/// its contract. `Protocol(ContractViolation)` would derive
/// `ProviderContractViolation` / `ContactProviderSupport` and point an
/// operator at Microsoft for damaged client state.
#[tokio::test]
async fn a_corrupt_search_cursor_blames_the_request_not_the_provider() {
    for cursor in [
        // truncated payload under the current version prefix
        b"{\"version\":1,\"cursor\":{\"surface\":\"search\"".to_vec(),
        // a bare Graph nextLink - what this crate emitted before the
        // walk existed, and what a truncated cursor degrades to. It must
        // be refused, never re-issued as a URL on the caller's word.
        b"https://graph.microsoft.com/v1.0/me/messages?$skiptoken=x".to_vec(),
        // not UTF-8 at all
        vec![0xff, 0xfe],
    ] {
        let primary = GraphClient::new("token");
        primary.script_rest([]);
        let account = GraphAccount::new_for_tests(primary, PushMode::GraphSubscriptions);
        let mut request = SearchRequest::default();
        request.page_cursor = Some(cursor.clone().into());
        let error = search_message_rows(&account, request, AccountOperation::Search)
            .await
            .expect_err("an undecodable cursor is refused");
        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Request(RequestErrorKind::Malformed)
            ),
            "{}: {:?}",
            String::from_utf8_lossy(&cursor),
            error.kind()
        );
        assert_eq!(error.recovery(), &RecoveryClass::ClientBug);
        assert_eq!(
            error.suggested_remediation(),
            Some(&RemediationAction::FixClientRequest)
        );
        assert_eq!(error.operation(), Some(AccountOperation::Search));
    }
}

#[test]
fn search_filter_escapes_odata_strings() {
    let filter =
        odata_filter(&SearchFilter::Subject("Bob's plan".to_string())).expect("filter builds");
    assert_eq!(filter, "contains(subject,'Bob''s plan')");
}

#[test]
fn from_to_filters_route_through_search_kql_not_filter() {
    // A From substring must hit `$search` (KQL), never the `$filter`
    // `contains()` Graph rejects.
    let req = SearchRequest::filter(SearchFilter::From("alice".to_string()));
    let url = search_url("/me", &req).expect("url builds");
    assert!(url.contains("$search="), "{url}");
    assert!(!url.contains("$filter="), "{url}");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value("\"from:\"alice\"\"")
    );

    let to = SearchRequest::filter(SearchFilter::To("bob@x".to_string()));
    let to_url = search_url("/me", &to).expect("url builds");
    assert_eq!(
        search_param(&to_url),
        bifrost_net::url::encode_query_value("\"(to:\"bob@x\" OR cc:\"bob@x\")\"")
    );
}

#[test]
fn non_sender_filter_still_uses_odata_filter() {
    // Subject-only search has a clean `$filter` shape; it must not be
    // forced onto `$search`.
    let req = SearchRequest::filter(SearchFilter::Subject("invoice".to_string()));
    let url = search_url("/me", &req).expect("url builds");
    assert!(url.contains("$filter="), "{url}");
    assert!(!url.contains("$search="), "{url}");
}

#[test]
fn mixed_from_and_date_range_collapses_to_one_kql_search() {
    // From substring AND a date range: the whole thing goes to KQL,
    // never a mixed `$search`+`$filter` request.
    let req = SearchRequest::filter(SearchFilter::And(vec![
        SearchFilter::From("alice".to_string()),
        SearchFilter::DateRange {
            after: Some(SystemTime::UNIX_EPOCH),
            before: None,
        },
    ]));
    let url = search_url("/me", &req).expect("url builds");
    assert!(url.contains("$search="), "{url}");
    assert!(!url.contains("$filter="), "{url}");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value("\"(from:\"alice\") AND (sent>=1970-01-01)\"")
    );
}

#[test]
fn kql_date_range_is_send_time_rounded_outward() {
    // The `$filter` path ranges over `sentDateTime`; a KQL-forcing sibling
    // must not move the range onto the received clock, and whole-date KQL
    // bounds must not drop a message sent inside the requested interval.
    let day = std::time::Duration::from_secs(86_400);
    let midday = SystemTime::UNIX_EPOCH + day + std::time::Duration::from_secs(43_200);
    let req = SearchRequest::filter(SearchFilter::And(vec![
        SearchFilter::From("alice".to_string()),
        SearchFilter::DateRange {
            after: Some(midday),
            before: Some(midday),
        },
    ]));
    let url = search_url("/me", &req).expect("url builds");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value(
            "\"(from:\"alice\") AND (sent>=1970-01-02 AND sent<1970-01-03)\""
        )
    );
    let midnight = SystemTime::UNIX_EPOCH + day + day;
    let req = SearchRequest::filter(SearchFilter::And(vec![
        SearchFilter::From("alice".to_string()),
        SearchFilter::DateRange {
            after: None,
            before: Some(midnight),
        },
    ]));
    let url = search_url("/me", &req).expect("url builds");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value("\"(from:\"alice\") AND (sent<1970-01-03)\"")
    );
}

#[test]
fn from_filter_and_provider_query_combine_in_kql() {
    // A structured From plus a raw provider query AND together into one
    // `$search`; the request must not emit both `$filter` and `$search`.
    let mut req = SearchRequest::filter(SearchFilter::From("alice".to_string()));
    req.provider_query = Some("importance:high".to_string());
    let url = search_url("/me", &req).expect("url builds");
    assert!(url.contains("$search="), "{url}");
    assert!(!url.contains("$filter="), "{url}");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value("\"(from:\"alice\") AND (importance:high)\"")
    );
}

#[test]
fn a_top_level_folder_becomes_the_search_endpoint() {
    // KQL has no folder property, but Graph searches a folder's own message
    // collection: a positive top-level `In` scopes the endpoint, and an exact
    // repeat of it is redundant rather than a second refused restriction.
    let req = SearchRequest::filter(SearchFilter::And(vec![
        SearchFilter::From("alice".to_string()),
        SearchFilter::In(ContainerId("inbox".to_string())),
        SearchFilter::And(vec![SearchFilter::In(ContainerId("inbox".to_string()))]),
    ]));
    let url = search_url("/me", &req).expect("url builds");
    assert!(url.starts_with("/me/mailFolders/inbox/messages?"), "{url}");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value("\"from:\"alice\"\"")
    );

    // The `$filter` path lifts it the same way instead of `parentFolderId`.
    let req = SearchRequest::filter(SearchFilter::And(vec![
        SearchFilter::In(ContainerId("inbox".to_string())),
        SearchFilter::Subject("q3".to_string()),
    ]));
    let url = search_url("/me", &req).expect("url builds");
    assert!(url.starts_with("/me/mailFolders/inbox/messages?"), "{url}");
    assert!(!url.contains("parentFolderId"), "{url}");

    // A folder the endpoint cannot carry stays inexpressible in KQL and
    // fails cleanly rather than silently searching every folder.
    for filter in [
        SearchFilter::Or(vec![
            SearchFilter::From("alice".to_string()),
            SearchFilter::In(ContainerId("inbox".to_string())),
        ]),
        SearchFilter::And(vec![
            SearchFilter::From("alice".to_string()),
            SearchFilter::In(ContainerId("inbox".to_string())),
            SearchFilter::In(ContainerId("archive".to_string())),
        ]),
    ] {
        let err = search_url("/me", &SearchRequest::filter(filter)).expect_err("inexpressible");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(err.operation(), Some(AccountOperation::Search));
    }
}

/// Folder identity is (owner, native id). A search restricted to a shared
/// mailbox's folder visits only that mailbox, through its folder endpoint;
/// the primary is armed EMPTY, so requesting it would panic.
#[tokio::test]
async fn a_shared_folder_scope_searches_only_its_mailbox() {
    let primary = GraphClient::new("token");
    primary.script_rest([]);
    let mailbox = "shared@contoso.com";
    let shared_root = GraphClient::new("token");
    shared_root.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "value": [{ "id": "m1", "conversationId": "t1" }] }),
    )]);
    // `other@` is armed empty too: it sorts before `shared@`, so a walk
    // that did not skip ruled-out mailboxes would request it first.
    let mut clients = shared_clients_for_tests(&["other@contoso.com"]);
    clients.insert(mailbox.to_string(), shared_root.for_shared_mailbox(mailbox));
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary,
        PushMode::GraphSubscriptions,
        clients,
    );
    let request = SearchRequest::filter(SearchFilter::And(vec![
        SearchFilter::From("alice".to_string()),
        SearchFilter::In(ContainerId(format!("{mailbox}\u{1f}FolderX"))),
    ]));
    let page = search_message_rows(&account, request, AccountOperation::Search)
        .await
        .expect("scoped search succeeds");
    assert_eq!(page.items[0].id, ObjectId(format!("{mailbox}\u{1f}m1")));
    assert!(page.next_cursor.is_none(), "no other mailbox can match");
    assert!(!page.truncated);
    let requests = shared_root.take_rest_requests();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .url
            .contains("/users/shared%40contoso.com/mailFolders/FolderX/messages?"),
        "{}",
        requests[0].url
    );
}

fn rows(count: usize, prefix: &str) -> serde_json::Value {
    serde_json::Value::Array(
        (0..count)
            .map(|index| json!({ "id": format!("{prefix}{index}"), "conversationId": "t" }))
            .collect(),
    )
}

/// Graph's `$search` stops at 1,000 results with no further `nextLink`.
/// A mailbox that runs out AT the ceiling reports `truncated`, on the page
/// where it ran out, even though the walk continues into the next mailbox;
/// the same count short of the ceiling, or a `$filter` search, does not.
#[tokio::test]
async fn a_search_mailbox_exhausted_at_the_ceiling_is_truncated() {
    let primary = GraphClient::new("token");
    primary.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": rows(250, "a"),
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/messages?$skip=250"
            }),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": rows(250, "b"),
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/messages?$skip=500"
            }),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": rows(250, "c"),
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/messages?$skip=750"
            }),
        ),
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({ "value": rows(250, "d") })),
    ]);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary,
        PushMode::GraphSubscriptions,
        shared_clients_for_tests(&["shared@contoso.com"]),
    );
    let search = || SearchRequest::filter(SearchFilter::From("alice".to_string()));
    let mut cursor = None;
    let mut flags = Vec::new();
    for _ in 0..4 {
        let mut request = search();
        request.page_cursor = cursor;
        let page = search_message_rows(&account, request, AccountOperation::Search)
            .await
            .expect("page succeeds");
        flags.push(page.truncated);
        cursor = page.next_cursor;
    }
    assert_eq!(flags, [false, false, false, true]);
    assert!(
        cursor.is_some(),
        "the walk still continues into the shared mailbox"
    );

    // 999 rows is an exhaustive answer, and a `$filter` search has no ceiling.
    for (filter, count) in [
        (SearchFilter::From("alice".to_string()), 999),
        (SearchFilter::Subject("q3".to_string()), 1_000),
    ] {
        let primary = GraphClient::new("token");
        primary.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "value": rows(count, "x") }),
        )]);
        let account = GraphAccount::new_for_tests(primary, PushMode::GraphSubscriptions);
        let page = search_message_rows(
            &account,
            SearchRequest::filter(filter),
            AccountOperation::Search,
        )
        .await
        .expect("page succeeds");
        assert!(!page.truncated, "{count}");
    }
}

/// A `nextLink` carries its endpoint, so a cursor minted for one folder
/// scope must not resume under a request scoped to another; the resuming
/// account is armed EMPTY, so a resumed request would panic.
#[tokio::test]
async fn a_search_cursor_from_another_folder_scope_is_rejected() {
    let scoped = |folder: &str| {
        SearchRequest::filter(SearchFilter::And(vec![
            SearchFilter::From("alice".to_string()),
            SearchFilter::In(ContainerId(folder.to_string())),
        ]))
    };
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({
            "value": rows(1, "a"),
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$skip=1"
        }),
    )]);
    let account = GraphAccount::new_for_tests(primary, PushMode::GraphSubscriptions);
    let cursor = search_message_rows(&account, scoped("inbox"), AccountOperation::Search)
        .await
        .expect("page succeeds")
        .next_cursor
        .expect("continues");
    for mut request in [scoped("archive"), SearchRequest::default()] {
        request.page_cursor = Some(cursor.clone());
        let error = search_message_rows(&account, request, AccountOperation::Search)
            .await
            .expect_err("another scope refuses the cursor");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
    }
}

#[test]
fn attachment_presence_stays_odata_and_a_name_routes_to_kql() {
    use bifrost_types::{AttachmentFilter, MediaTypePattern};
    let any = SearchRequest::filter(SearchFilter::Attachment(AttachmentFilter::Any));
    let url = search_url("/me", &any).expect("url builds");
    assert!(url.contains("$filter="), "{url}");
    assert!(!url.contains("$search="), "{url}");

    // A name leaf forces KQL even beneath Not, and drags its siblings along.
    let req = SearchRequest::filter(SearchFilter::And(vec![
        SearchFilter::Subject("q3".to_string()),
        SearchFilter::Not(Box::new(SearchFilter::Attachment(AttachmentFilter::Name(
            "report".to_string(),
        )))),
    ]));
    let url = search_url("/me", &req).expect("url builds");
    assert!(!url.contains("$filter="), "{url}");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value(
            "\"(subject:\"q3\") AND (NOT (attachment:\"report\"))\""
        )
    );

    let malformed = |filter: SearchFilter| {
        let err = search_url("/me", &SearchRequest::filter(filter)).expect_err("refused");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
    };
    malformed(SearchFilter::Attachment(AttachmentFilter::Name(" ".into())));
    // No KQL folder property: a negated folder beside a name is refused, as
    // with From (a positive top-level one becomes the endpoint instead).
    malformed(SearchFilter::And(vec![
        SearchFilter::Not(Box::new(SearchFilter::In(ContainerId("inbox".to_string())))),
        SearchFilter::Attachment(AttachmentFilter::Name("report".into())),
    ]));

    let pdf = SearchFilter::Attachment(AttachmentFilter::MediaType(
        MediaTypePattern::parse("application/pdf").unwrap(),
    ));
    let err = search_url("/me", &SearchRequest::filter(pdf)).expect_err("no type property");
    assert!(matches!(
        err.kind(),
        bifrost_types::AccountErrorKind::Unsupported(_)
    ));
}

#[test]
fn a_raw_kql_leaf_routes_to_search_and_stays_grouped() {
    let req = SearchRequest::filter(SearchFilter::Or(vec![
        SearchFilter::ProviderQuery("importance:high OR size>1000".to_string()),
        SearchFilter::Subject("q3".to_string()),
    ]));
    let url = search_url("/me", &req).expect("url builds");
    assert!(!url.contains("$filter="), "{url}");
    assert_eq!(
        search_param(&url),
        bifrost_net::url::encode_query_value(
            "\"((importance:high OR size>1000)) OR (subject:\"q3\")\""
        )
    );
    for bad in ["", "  ", "a) OR (b", "\"open"] {
        let err = search_url(
            "/me",
            &SearchRequest::filter(SearchFilter::ProviderQuery(bad.to_string())),
        )
        .expect_err(bad);
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
    }
}

#[test]
fn kql_quoting_escapes_embedded_double_quotes() {
    assert_eq!(kql_quoted(r#"a"b"#), r#""a\"b""#);
}
