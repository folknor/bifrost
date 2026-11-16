//! Hermetic JMAP servers for tests, scripted at the wire.
//!
//! There is one JMAP transport, `ReqwestTransport` over `bifrost-net`, and
//! tests drive it rather than replacing it: a server double is a function
//! from the request the wire received to the outcome the wire produces, run
//! by `bifrost_net::test_support::ScriptedDispatch`. Everything above the
//! wire - status classification, retry, replay policy, the concurrency
//! gates, the byte meter - is the production pipeline, so a double cannot
//! pin a branch production never reaches, and it never restates net's
//! status contract: it answers with a status, never with a transport error.
//!
//! A door a double does not serve panics, as an exhausted script does.

use std::future::Future;
use std::sync::Arc;

use bifrost_net::test_support::{Canned, RequestSnapshot, ResponderFuture, ScriptedDispatch};
use bytes::Bytes;

use crate::client::Client;
use crate::core::session::Session;
use crate::transport_reqwest::ReqwestTransport;

/// The session URL every scripted client claims to have discovered.
pub(crate) const SESSION_URL: &str = "https://jmap.test/.well-known/jmap";

/// A server whose answer is computed synchronously from the request.
pub(crate) fn server<F>(answer: F) -> Arc<ScriptedDispatch>
where
    F: Fn(&RequestSnapshot) -> Canned + Send + Sync + 'static,
{
    ScriptedDispatch::responding(move |request| {
        let outcome = answer(&request);
        Box::pin(async move { outcome }) as ResponderFuture
    })
}

/// A server whose answer may wait, for tests that hold one request
/// outstanding while another proceeds.
pub(crate) fn async_server<F, Fut>(answer: F) -> Arc<ScriptedDispatch>
where
    F: Fn(RequestSnapshot) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Canned> + Send + 'static,
{
    ScriptedDispatch::responding(move |request| Box::pin(answer(request)) as ResponderFuture)
}

/// A server double's refusal to answer: the connection is refused before
/// anything reaches it. A test that means a different failure (a status, a
/// drop after sending) answers with that wire outcome through [`server`]
/// instead.
pub(crate) struct Refused(Canned);

impl Refused {
    /// The connection is refused: nothing was sent.
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(refused(&message.into()))
    }

    /// The connection is lost after the request may have reached the
    /// server, so whether it was applied is unknown.
    pub(crate) fn lost(message: impl Into<String>) -> Self {
        Self(lost(&message.into()))
    }
}

/// A stateful JMAP server double, one method per HTTP door. It answers
/// with response bodies, never with transport errors: [`serve`] turns each
/// answer into the wire outcome beneath the production transport. A door a
/// double does not override panics.
pub(crate) trait ServerDouble: Send + Sync + 'static {
    /// Answer one API POST.
    fn api_request(&self, url: &str, body: Vec<u8>) -> Result<Bytes, Refused>;

    /// Answer one blob upload.
    fn upload(
        &self,
        url: &str,
        _body: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<Bytes, Refused> {
        panic!("unexpected upload to {url}")
    }

    /// Answer one blob download.
    fn download(&self, url: &str) -> Result<Bytes, Refused> {
        panic!("unexpected download of {url}")
    }

    /// Answer one session fetch.
    fn get_session(&self, url: &str) -> Result<Bytes, Refused> {
        panic!("unexpected session fetch of {url}")
    }
}

fn answered(result: Result<Bytes, Refused>) -> Canned {
    match result {
        Ok(body) => ok(body),
        Err(Refused(canned)) => canned,
    }
}

/// Run `double` as the server behind a scripted net.
pub(crate) fn serve<D: ServerDouble>(double: D) -> Arc<ScriptedDispatch> {
    server(move |request| {
        let url = request.url.as_str();
        if is_api(request) {
            answered(double.api_request(url, body_bytes(request).to_vec()))
        } else if request.method == reqwest::Method::POST {
            let content_type = request
                .headers
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok());
            answered(double.upload(url, body_bytes(request).to_vec(), content_type))
        } else if request.method == reqwest::Method::GET && is_session_url(request) {
            answered(double.get_session(url))
        } else if request.method == reqwest::Method::GET {
            answered(double.download(url))
        } else {
            unexpected(request)
        }
    })
}

/// Session fetches go to the discovery URL a test client was built with:
/// [`SESSION_URL`] or one ending in `/.well-known/jmap`, or a fixture's
/// literal `/session` path. No fixture's download template yields either.
fn is_session_url(request: &RequestSnapshot) -> bool {
    let path = request.url.path();
    path.ends_with("/.well-known/jmap") || path == "/session"
}

impl<D: ServerDouble> From<D> for ReqwestTransport {
    fn from(double: D) -> Self {
        transport(&serve(double))
    }
}

/// Whether `request` is a JMAP API call, as opposed to an upload, a
/// download or a session fetch. Identified by its body - a JSON object
/// carrying `methodCalls`, the RFC 8620 request envelope - rather than by
/// content type, which an upload of a JSON attachment shares.
pub(crate) fn is_api(request: &RequestSnapshot) -> bool {
    request.method == reqwest::Method::POST
        && request
            .body
            .as_deref()
            .and_then(|body| serde_json::from_slice::<serde_json::Value>(body).ok())
            .is_some_and(|value| value["methodCalls"].is_array())
}

/// A server that answers only API calls, from the parsed request body.
/// Every other door panics.
pub(crate) fn api_server<F>(answer: F) -> Arc<ScriptedDispatch>
where
    F: Fn(&serde_json::Value) -> Canned + Send + Sync + 'static,
{
    server(move |request| {
        if is_api(request) {
            answer(&body_json(request))
        } else {
            unexpected(request)
        }
    })
}

/// Every API request body `script` received, parsed, in arrival order.
pub(crate) fn api_bodies(script: &ScriptedDispatch) -> Vec<serde_json::Value> {
    script
        .requests()
        .iter()
        .filter(|request| is_api(request))
        .map(body_json)
        .collect()
}

/// The production transport over `script`, with ordinary retry disabled so
/// a scripted failure surfaces once. 401 recovery still runs.
pub(crate) fn transport(script: &Arc<ScriptedDispatch>) -> ReqwestTransport {
    ReqwestTransport::scripted_with_retry(script, bifrost_net::RetryPolicy::disabled())
}

/// The scripted server beneath a test client.
pub(crate) fn script_of(client: &Client) -> &Arc<ScriptedDispatch> {
    client
        .transport()
        .script
        .as_ref()
        .expect("the client rides a scripted net")
}

/// A client over `script` with `session` already installed.
pub(crate) fn client(script: &Arc<ScriptedDispatch>, session: Session) -> Client {
    Client::with_transport(transport(script), session, SESSION_URL).expect("scripted client builds")
}

/// A client over `script` whose session is the given JSON.
pub(crate) fn client_json(script: &Arc<ScriptedDispatch>, session: serde_json::Value) -> Client {
    client(
        script,
        serde_json::from_value(session).expect("session fixture parses"),
    )
}

/// A 200 carrying `body`.
pub(crate) fn ok(body: impl Into<Bytes>) -> Canned {
    bifrost_net::test_support::canned_owned(reqwest::StatusCode::OK, body)
}

/// A 200 carrying `value` as JSON.
pub(crate) fn ok_json(value: &serde_json::Value) -> Canned {
    ok(value.to_string())
}

/// A response with the given status and an empty body.
pub(crate) fn status(code: u16) -> Canned {
    bifrost_net::test_support::canned_owned(
        reqwest::StatusCode::from_u16(code).expect("valid status"),
        Bytes::new(),
    )
}

/// A connection failure before anything reached the server.
pub(crate) fn refused(message: &str) -> Canned {
    Canned::Error(bifrost_net::Error::Network {
        message: message.into(),
        transmission_state: bifrost_net::TransmissionState::Unsent,
        source: None,
    })
}

/// A connection lost after the request may have reached the server.
pub(crate) fn lost(message: &str) -> Canned {
    Canned::Error(bifrost_net::Error::Network {
        message: message.into(),
        transmission_state: bifrost_net::TransmissionState::InFlight,
        source: None,
    })
}

/// The request body as JSON.
pub(crate) fn body_json(request: &RequestSnapshot) -> serde_json::Value {
    serde_json::from_slice(request.body.as_deref().expect("request carries a body"))
        .expect("the client emits JSON")
}

/// The request body as raw bytes, empty when there is none.
pub(crate) fn body_bytes(request: &RequestSnapshot) -> Bytes {
    request.body.clone().unwrap_or_default()
}

/// Panic on a door the double does not serve.
pub(crate) fn unexpected(request: &RequestSnapshot) -> Canned {
    panic!(
        "unexpected {} {} reached the scripted server",
        request.method, request.url
    )
}
