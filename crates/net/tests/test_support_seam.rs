//! Downstream view of the `test-support` seam.
//!
//! These run as a separate crate against the public API, which is the
//! point: the in-crate unit tests in `request.rs` would still pass if
//! the seam leaked a private type, forgot the `http` dependency, or
//! gated a needed item out of the feature. This file is the compile
//! check that an account crate riding bifrost-net can actually use it.
//!
//! They also pin the two contract facts a hand-rolled double has to
//! re-derive, and which xc-3 records one crate getting wrong: a 4xx
//! never surfaces as `Ok(Response)`, and the pipeline above the wire
//! is the production one.

#![cfg(feature = "test-support")]

use std::sync::Arc;
use std::time::Duration;

use bifrost_net::test_support::{Canned, ScriptedDispatch, canned, canned_with_headers};
use bifrost_net::{
    AccountId, AccountSpec, Error, Method, NetConfig, RetryPolicy, StaticTokenSource,
};
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};

fn account(script: &Arc<ScriptedDispatch>, retry: RetryPolicy) -> bifrost_net::AccountNet {
    bifrost_net::test_support::scripted_account(
        script,
        NetConfig::default(),
        Vec::new(),
        Arc::new(StaticTokenSource::new("token", None)),
        retry,
    )
}

fn no_retry() -> RetryPolicy {
    RetryPolicy::disabled()
}

#[tokio::test]
async fn extension_method_and_no_bearer_account_use_the_shared_pipeline() {
    let script = ScriptedDispatch::new([canned(StatusCode::MULTI_STATUS, b"dav")]);
    let net = bifrost_net::test_support::scripted_net(&script, NetConfig::default());
    let account = net.attach_account(
        AccountId("basic-dav".to_owned()),
        AccountSpec {
            hosts: Vec::new(),
            default_retry: no_retry(),
            ..AccountSpec::new(None)
        },
    );
    let method = Method::from_bytes(b"PROPFIND").unwrap();

    account
        .request(method.clone(), "https://dav.test/calendars")
        .without_bearer_auth()
        .send()
        .await
        .expect("Basic-auth shape needs no fabricated bearer source");

    let sent = script.requests();
    assert_eq!(sent[0].method, method);
    assert!(
        sent[0]
            .headers
            .get(reqwest::header::AUTHORIZATION)
            .is_none()
    );
}

/// The token source is optional, so bearer auth left enabled on an
/// account that has none is a caller configuration error. It must be
/// caught before dispatch: an unauthenticated request that reaches the
/// server would either leak the resource's existence or come back as a
/// 401 the retry path would try to recover from with a refresh that
/// cannot exist.
#[tokio::test]
async fn bearer_auth_without_a_token_source_fails_locally_and_sends_nothing() {
    let script = ScriptedDispatch::new([canned(StatusCode::OK, b"never reached")]);
    let net = bifrost_net::test_support::scripted_net(&script, NetConfig::default());
    let account = net.attach_account(
        AccountId("no-source".to_owned()),
        AccountSpec {
            hosts: Vec::new(),
            default_retry: no_retry(),
            ..AccountSpec::new(None)
        },
    );

    let Err(error) = account.get("https://api.test/thing").send().await else {
        panic!("bearer auth with no token source is a local configuration failure");
    };

    match &error {
        Error::InvalidRequest { field, .. } => assert_eq!(*field, "bearer_auth"),
        other => panic!("expected a local InvalidRequest, got {other:?}"),
    }
    assert!(
        script.requests().is_empty(),
        "no unauthenticated request may reach the transport"
    );

    let account_error = bifrost_net::into_account_error(
        error,
        bifrost_net::NetErrorContext {
            provider: None,
            protocol: bifrost_types::Protocol::CalDav,
            operation: bifrost_types::AccountOperation::Discover,
            scope: None,
        },
    );
    assert!(
        matches!(
            account_error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ),
        "kind was {:?}",
        account_error.kind()
    );
    assert!(
        matches!(
            account_error.recovery(),
            bifrost_types::RecoveryClass::ClientBug
        ),
        "recovery was {:?}",
        account_error.recovery()
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_scripted_2xx_reaches_the_caller_as_a_real_response() {
    let script = ScriptedDispatch::new([canned(StatusCode::OK, b"body")]);

    let response = account(&script, no_retry())
        .get("https://consumer.test/resource")
        .send()
        .await
        .expect("2xx surfaces as Ok");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.body, bytes::Bytes::from_static(b"body"));
    assert_eq!(script.requests().len(), 1);
    assert_eq!(script.remaining(), 0);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_scripted_4xx_never_surfaces_as_a_response() {
    // The contract an in-crate double must re-derive, and the one whose
    // mis-derivation hid a live Graph defect: a 404 is an `Err` at the
    // caller, so a consumer branch that pattern-matches a 4xx off an
    // `Ok(Response)` is dead on the production path.
    let script = ScriptedDispatch::new([canned(StatusCode::NOT_FOUND, br#"{"error":"gone"}"#)]);

    // `Response` is deliberately not `Debug` (it carries response bodies),
    // so unwrap the result by hand rather than via `expect_err`.
    let Err(error) = account(&script, no_retry())
        .get("https://consumer.test/missing")
        .send()
        .await
    else {
        panic!("4xx surfaces as Err, never as Ok(Response)");
    };

    // The response evidence is preserved on the error, which is how
    // account crates classify a typed provider error code.
    match error {
        Error::Status { code, body, .. } => {
            assert_eq!(code, StatusCode::NOT_FOUND);
            assert_eq!(body, bytes::Bytes::from_static(br#"{"error":"gone"}"#));
        }
        other => panic!("expected Error::Status, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_terminal_status_whose_body_stalls_does_not_block_forever() {
    // The client-level read timeout went away when `NetConfig` split
    // into process-wide and per-account halves, so the terminal-status
    // drains had to move onto the per-account one. A server that sends
    // 4xx headers and then hangs mid-body is the shape that catches
    // it, and google/graph accounts carry no total request deadline to
    // rescue them. Draining with `bytes()` here never returns.
    //
    // The stall is infinite, so with the drain reverted to `bytes()`
    // this test hangs and brokkr's per-test timeout reports it as a
    // failure. Verified by ablation rather than assumed.
    let script = ScriptedDispatch::new([Canned::StreamThenStall {
        status: StatusCode::FORBIDDEN,
        headers: HeaderMap::new(),
        chunks: vec![bytes::Bytes::from_static(b"partial")],
    }]);

    let Err(error) = account(&script, no_retry())
        .get("https://consumer.test/stalls")
        .send()
        .await
    else {
        panic!("4xx surfaces as Err, never as Ok(Response)");
    };

    match error {
        Error::Status { code, body, .. } => {
            assert_eq!(code, StatusCode::FORBIDDEN);
            assert_eq!(
                body,
                bytes::Bytes::from_static(b"partial"),
                "the bytes that did arrive before the stall are preserved \
                 as error evidence rather than discarded"
            );
        }
        other => panic!("expected Error::Status, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_consumer_seam_drives_the_real_retry_loop() {
    // What no seam above `AccountNet` can reach (graph-T1): the number
    // of attempts and the `Retry-After` honor are decided inside
    // bifrost-net, below every account crate's own funnel.
    let mut retry_after = HeaderMap::new();
    retry_after.insert(RETRY_AFTER, HeaderValue::from_static("0"));
    let script = ScriptedDispatch::new([
        canned_with_headers(StatusCode::SERVICE_UNAVAILABLE, retry_after, b"transient"),
        canned(StatusCode::OK, b"recovered"),
    ]);

    let response = account(
        &script,
        RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        },
    )
    .get("https://consumer.test/flaky")
    .send()
    .await
    .expect("the retried 503 succeeds on the second attempt");

    assert_eq!(response.body, bytes::Bytes::from_static(b"recovered"));
    assert_eq!(
        script.requests().len(),
        2,
        "the retry happened below the consumer's own funnel"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_transport_failure_is_scriptable_without_a_socket() {
    let script = ScriptedDispatch::new([
        Canned::Error(Error::Network {
            message: "synthetic reset".to_string(),
            transmission_state: bifrost_types::TransmissionState::Unsent,
            source: None,
        }),
        canned(StatusCode::OK, b"recovered"),
    ]);

    let response = account(
        &script,
        RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        },
    )
    .get("https://consumer.test/reset")
    .without_bearer_auth()
    .send()
    .await
    .expect("the network failure retries through the same loop");

    assert_eq!(response.body, bytes::Bytes::from_static(b"recovered"));
    assert_eq!(script.requests().len(), 2);
}

/// The framing guarantee the streaming variants exist for. A consumer
/// pinning "the blob stream forwards every transport chunk rather than
/// coalescing or re-splitting" needs the chunks it scripted to arrive as
/// the chunks it scripted.
#[tokio::test(flavor = "current_thread")]
async fn a_streamed_body_preserves_its_chunk_boundaries() {
    use futures::StreamExt;

    let script = ScriptedDispatch::new([Canned::Stream {
        status: StatusCode::OK,
        headers: HeaderMap::new(),
        chunks: vec![
            bytes::Bytes::from_static(b"abc"),
            bytes::Bytes::from_static(b"de"),
            bytes::Bytes::from_static(b"fghi"),
        ],
    }]);

    let stream = account(&script, no_retry())
        .download_stream("https://consumer.test/blob", None)
        .await
        .expect("the stream opens");
    let chunks: Vec<bytes::Bytes> = stream.map(|chunk| chunk.expect("chunk")).collect().await;

    assert_eq!(
        chunks,
        vec![
            bytes::Bytes::from_static(b"abc"),
            bytes::Bytes::from_static(b"de"),
            bytes::Bytes::from_static(b"fghi"),
        ]
    );
}

/// A body that fails partway through is the one failure the status check
/// cannot pre-empt: the caller already holds a stream. The delivered
/// chunks survive and the failure arrives as `Error::Network`, which is
/// what a real socket failure mid-body produces.
#[tokio::test(flavor = "current_thread")]
async fn a_mid_body_failure_arrives_after_the_delivered_chunks() {
    use futures::StreamExt;

    let script = ScriptedDispatch::new([Canned::StreamThenError {
        status: StatusCode::OK,
        headers: HeaderMap::new(),
        chunks: vec![bytes::Bytes::from_static(b"head")],
        message: "connection reset mid-body".to_string(),
    }]);

    let stream = account(&script, no_retry())
        .download_stream("https://consumer.test/blob", None)
        .await
        .expect("the stream opens: the status was already good");
    let outcomes: Vec<Result<bytes::Bytes, Error>> = stream.collect().await;

    assert_eq!(outcomes.len(), 2);
    assert_eq!(
        outcomes[0].as_ref().expect("first chunk delivered"),
        &bytes::Bytes::from_static(b"head")
    );
    match outcomes[1].as_ref() {
        Err(Error::Network { .. }) => {}
        other => panic!("expected a mid-body Error::Network, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_transport_injects_authorization_below_the_consumer() {
    let script = ScriptedDispatch::new([canned(StatusCode::OK, b"ok")]);

    account(&script, no_retry())
        .get("https://consumer.test/authed")
        .send()
        .await
        .expect("2xx");

    let sent = script.requests();
    let auth = sent[0]
        .headers
        .get(reqwest::header::AUTHORIZATION)
        .expect("the transport injected the bearer token");
    assert_eq!(auth, "Bearer token");
}
