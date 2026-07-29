#![cfg(test)]

use serde_json::json;

use std::marker::PhantomData;

use super::SetCreate;
use super::get::{GetObject, GetResponse};
use super::method::JmapMethod;
use super::query::QueryObject;
use super::request::CallHandle;
use super::response::Response;
use crate::Error;

// -- Minimal test types --

mod test_marker {
    pub(crate) enum TestObj {}
}
pub(crate) type TestObjId = crate::core::id::Id<test_marker::TestObj>;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TestObj {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct TestObjCreate {
    #[serde(skip)]
    _create_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct TestObjPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(crate) enum TestProp {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "name")]
    Name,
}

impl std::fmt::Display for TestProp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestProp::Id => write!(f, "id"),
            TestProp::Name => write!(f, "name"),
        }
    }
}

impl super::Object for TestObj {
    type Property = TestProp;
    type Id = TestObjId;
    fn requires_account_id() -> bool {
        true
    }
}

impl GetObject for TestObj {
    type GetArguments = ();
}

impl super::set::SetObject for TestObj {
    type Create = TestObjCreate;
    type Patch = TestObjPatch;
    type SetArguments = ();
}

impl SetCreate for TestObjCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }
    fn new(create_id: Option<usize>) -> Self {
        TestObjCreate {
            _create_id: create_id,
            name: None,
        }
    }
}

impl super::changes::ChangesObject for TestObj {
    type ChangesResponse = ();
}

impl QueryObject for TestObj {
    type QueryArguments = ();
    type Filter = ();
    type Sort = ();
}

crate::define_get_method!(TestGet, TestObj, "Test/get", crate::core::capability::Core);
crate::define_query_method!(
    TestQuery,
    TestObj,
    "Test/query",
    crate::core::capability::Core
);
// A second get method on a DIFFERENT capability, so the `using`
// accumulation in `Request::call` has something to accumulate. Every
// other test method rides `Core`, which `Request::new` already seeds.
crate::define_get_method!(
    TestMailGet,
    TestObj,
    "TestMail/get",
    crate::core::capability::Mail
);

fn make_handle<M: JmapMethod>(call_id: &str) -> CallHandle<M> {
    CallHandle {
        call_id: call_id.to_string(),
        method_name: M::NAME,
        _phantom: PhantomData,
    }
}

#[test]
fn response_get_extracts_typed_result() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["Test/get", {
                "accountId": "A1",
                "state": "s1",
                "list": [{"id": "t1", "name": "hello"}],
                "notFound": []
            }, "s0"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();
    let handle = make_handle::<TestGet>("s0");
    let result: GetResponse<TestObj> = response.get(&handle).unwrap();

    assert_eq!(result.state(), "s1");
    assert_eq!(result.list().len(), 1);
    assert_eq!(result.list()[0].name.as_deref(), Some("hello"));
}

#[test]
fn response_get_returns_call_not_found_for_wrong_id() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["Test/get", {"accountId": "A1", "state": "s1", "list": [], "notFound": []}, "s0"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();
    let handle = make_handle::<TestGet>("s99");
    let err = response.get(&handle).unwrap_err();

    assert!(matches!(err, Error::CallNotFound(id) if id == "s99"));
}

#[test]
fn response_get_returns_method_error() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["error", {"type": "unknownMethod"}, "s0"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();
    let handle = make_handle::<TestGet>("s0");
    let err = response.get(&handle).unwrap_err();

    assert!(matches!(err, Error::Method(_)));
}

#[test]
fn response_mixed_success_and_error() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["Test/get", {
                "accountId": "A1",
                "state": "s1",
                "list": [{"id": "t1"}],
                "notFound": []
            }, "s0"],
            ["error", {"type": "unknownMethod"}, "s1"],
            ["Test/query", {
                "accountId": "A1",
                "queryState": "q1",
                "canCalculateChanges": true,
                "position": 0,
                "ids": ["t1", "t2"]
            }, "s2"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();

    let handle_get = make_handle::<TestGet>("s0");
    let get_result = response.get(&handle_get).unwrap();
    assert_eq!(get_result.list().len(), 1);

    let handle_err = make_handle::<TestGet>("s1");
    assert!(matches!(response.get(&handle_err), Err(Error::Method(_))));

    let handle_query = make_handle::<TestQuery>("s2");
    let query_result = response.get(&handle_query).unwrap();
    assert_eq!(query_result.ids().len(), 2);
}

#[test]
fn response_get_consumes_entry() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["Test/get", {
                "accountId": "A1",
                "state": "s1",
                "list": [],
                "notFound": []
            }, "s0"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();
    let handle = make_handle::<TestGet>("s0");

    let _ = response.get(&handle).unwrap();

    assert!(matches!(response.get(&handle), Err(Error::CallNotFound(_))));
}

#[test]
fn call_handle_result_reference() {
    let handle = make_handle::<TestGet>("s0");
    let ref_ = handle.result_reference("/ids");

    assert_eq!(ref_.result_of, "s0");
    assert_eq!(ref_.name, "Test/get");
    assert_eq!(ref_.path, "/ids");
}

#[test]
fn problem_details_from_transport_error() {
    use crate::core::transport::TransportError;

    let problem_json = json!({
        "type": "urn:ietf:params:jmap:error:limit",
        "title": "Too many requests",
        "status": 429
    });

    let err = TransportError::with_body("HTTP 429", serde_json::to_vec(&problem_json).unwrap());

    let error: Error = err.into();
    assert!(matches!(error, Error::Problem { .. }));
}

#[test]
fn transport_error_without_body_stays_transport() {
    use crate::core::transport::TransportError;

    let err = TransportError::new("connection refused");
    let error: Error = err.into();
    assert!(matches!(error, Error::Transport(_)));
}

#[test]
fn set_response_deserializes() {
    use super::set::SetResponse;

    let raw = json!({
        "accountId": "A1",
        "oldState": "s1",
        "newState": "s2",
        "created": {
            "c0": {"id": "new-1", "name": "created-obj"}
        },
        "updated": null,
        "destroyed": null,
        "notCreated": null,
        "notUpdated": null,
        "notDestroyed": null
    });

    let mut response: SetResponse<TestObj> = serde_json::from_value(raw).unwrap();
    assert_eq!(response.new_state(), "s2");
    let created = response.created("c0").unwrap();
    assert_eq!(created.name.as_deref(), Some("created-obj"));
}

#[test]
fn request_serializes_correctly() {
    let handle = make_handle::<TestQuery>("q0");
    let mut get = TestGet::new()
        .ids([TestObjId::new("obj-1")])
        .ids_ref(handle.result_reference("/ids"))
        .properties([TestProp::Id])
        .properties_ref(handle.result_reference("/properties"));
    get.set_account_id(&crate::core::id::AccountId::new("account-1"));
    let value = serde_json::to_value(&get).unwrap();
    assert_eq!(value.get("accountId"), Some(&json!("account-1")));

    let query = TestQuery::new()
        .filter(())
        .sort([super::query::Comparator::new(())])
        .position(1)
        .anchor("obj-1")
        .anchor_offset(-1)
        .limit(50)
        .calculate_total(true);
    let value = serde_json::to_value(&query).unwrap();
    assert_eq!(value.get("limit"), Some(&json!(50)));
}

// ---------------------------------------------------------------------------
// Request envelope over a stub transport
//
// `HttpTransport` is the crate's own transport seam, so the whole
// request/response envelope - `using` construction, `methodCalls`
// encoding, `accountId` injection, result references, `Response::get`
// matching, `send_methods` - can be exercised in-process with no
// listener, no port, and no daemon. The stub records the exact JSON body
// the client would have POSTed and replays a queued reply.
// ---------------------------------------------------------------------------

mod envelope {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use serde_json::{Value, json};

    use super::{TestGet, TestMailGet, TestObjId, TestQuery};
    use crate::client::Client;
    use crate::core::capability;
    use crate::core::session::Session;
    use crate::core::transport::{HttpTransport, TransportError};

    /// In-process `HttpTransport`. Captures every outbound API body as
    /// parsed JSON and answers from a FIFO of canned reply bodies. An
    /// exhausted queue answers a transport error, which is what a real
    /// connection failure looks like to the caller.
    struct StubTransport {
        sent: Arc<Mutex<Vec<Value>>>,
        replies: Arc<Mutex<VecDeque<String>>>,
    }

    impl HttpTransport for StubTransport {
        async fn api_request(&self, _url: &str, body: Vec<u8>) -> Result<Bytes, TransportError> {
            let parsed: Value =
                serde_json::from_slice(&body).expect("client emitted a non-JSON request body");
            self.sent.lock().expect("stub sent lock").push(parsed);
            let next = self.replies.lock().expect("stub reply lock").pop_front();
            match next {
                Some(reply) => Ok(Bytes::from(reply)),
                None => Err(TransportError::new("stub transport has no queued reply")),
            }
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not upload"))
        }

        async fn download(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not download"))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport has no session route"))
        }
    }

    /// A stub client plus a handle on everything it sent.
    struct Stub {
        client: Client<StubTransport>,
        sent: Arc<Mutex<Vec<Value>>>,
    }

    impl Stub {
        /// The `n`th request body the client POSTed.
        fn request(&self, n: usize) -> Value {
            self.sent.lock().expect("stub sent lock")[n].clone()
        }

        fn request_count(&self) -> usize {
            self.sent.lock().expect("stub sent lock").len()
        }
    }

    /// A session fixture. `primary_accounts` is spelled out per test so
    /// `Client::default_account_id` (which picks `primary_accounts().next()`
    /// off a `HashMap`) stays deterministic: every fixture here lists at
    /// most one primary account.
    fn session(primary_accounts: Value) -> Session {
        serde_json::from_value(json!({
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
                }
            },
            "accounts": {},
            "primaryAccounts": primary_accounts,
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session fixture parses")
    }

    fn stub(primary_accounts: Value, replies: impl IntoIterator<Item = String>) -> Stub {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let transport = StubTransport {
            sent: Arc::clone(&sent),
            replies: Arc::new(Mutex::new(replies.into_iter().collect())),
        };
        let client = Client::with_transport(transport, session(primary_accounts))
            .expect("stub client builds");
        Stub { client, sent }
    }

    fn mail_primary() -> Value {
        json!({ "urn:ietf:params:jmap:mail": "A1" })
    }

    fn get_result(call_id: &str) -> Value {
        json!([
            "Test/get",
            {
                "accountId": "A1",
                "state": "s1",
                "list": [{"id": "t1", "name": "hello"}],
                "notFound": []
            },
            call_id
        ])
    }

    fn query_result(call_id: &str) -> Value {
        json!([
            "Test/query",
            {
                "accountId": "A1",
                "queryState": "q1",
                "canCalculateChanges": true,
                "position": 0,
                "ids": ["t1", "t2"]
            },
            call_id
        ])
    }

    fn reply(session_state: &str, results: Vec<Value>) -> String {
        json!({
            "sessionState": session_state,
            "methodResponses": results,
        })
        .to_string()
    }

    #[tokio::test]
    async fn request_serializes_using_method_calls_and_injected_account_id() {
        let stub = stub(mail_primary(), [reply("session-1", vec![get_result("s0")])]);

        let mut request = stub.client.build();
        let handle = request
            .call(TestGet::new().ids([TestObjId::new("t1")]))
            .expect("method encodes");
        let mut response = request.send().await.expect("stub replies");
        let result = response.get(&handle).expect("typed extraction");
        assert_eq!(result.state(), "s1");

        let body = stub.request(0);
        // `using` always carries core; the request has no other capability.
        assert_eq!(body["using"], json!(["urn:ietf:params:jmap:core"]));
        // `createdIds` is omitted entirely when unset (no `null` on the wire).
        assert!(body.get("createdIds").is_none());

        let calls = body["methodCalls"]
            .as_array()
            .expect("methodCalls is an array");
        assert_eq!(calls.len(), 1);
        // Each call is the RFC 8620 3-tuple [name, arguments, callId].
        assert_eq!(calls[0][0], json!("Test/get"));
        assert_eq!(calls[0][2], json!("s0"));
        // The accountId is injected from the request, not from the
        // method-struct constructor.
        assert_eq!(calls[0][1]["accountId"], json!("A1"));
        assert_eq!(calls[0][1]["ids"], json!(["t1"]));
    }

    #[tokio::test]
    async fn using_accumulates_each_capability_exactly_once() {
        let stub = stub(
            mail_primary(),
            [reply(
                "session-1",
                vec![get_result("s0"), get_result("s1"), get_result("s2")],
            )],
        );

        let mut request = stub.client.build();
        let _core_a = request.call(TestGet::new()).expect("encodes");
        let _mail = request.call(TestMailGet::new()).expect("encodes");
        let _core_b = request.call(TestGet::new()).expect("encodes");
        request.send().await.expect("stub replies");

        let body = stub.request(0);
        // Core is seeded by `Request::new`; Mail is appended once even
        // though a Core method is added on either side of it.
        assert_eq!(
            body["using"],
            json!(["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"])
        );
        let calls = body["methodCalls"].as_array().expect("array");
        // Call ids are positional and monotonic in insertion order.
        assert_eq!(calls[0][2], json!("s0"));
        assert_eq!(calls[1][2], json!("s1"));
        assert_eq!(calls[2][2], json!("s2"));
        assert_eq!(calls[1][0], json!("TestMail/get"));
    }

    #[tokio::test]
    async fn account_scoped_and_explicitly_overridden_account_ids_reach_the_wire() {
        let stub = stub(
            mail_primary(),
            [
                reply("session-1", vec![get_result("s0")]),
                reply("session-1", vec![get_result("s0")]),
            ],
        );

        // `Account::build` stamps the capability's primary account.
        let mail = stub
            .client
            .primary_account::<capability::Mail>()
            .expect("session lists a mail primary");
        mail.call(TestGet::new()).await.expect("stub replies");
        assert_eq!(
            stub.request(0)["methodCalls"][0][1]["accountId"],
            json!("A1")
        );

        // An explicit override wins over the client default.
        let mut request = stub.client.build().account_id("B2");
        let _ = request.call(TestGet::new()).expect("encodes");
        request.send().await.expect("stub replies");
        assert_eq!(
            stub.request(1)["methodCalls"][0][1]["accountId"],
            json!("B2")
        );
    }

    #[tokio::test]
    async fn send_methods_batches_in_order_and_returns_typed_responses() {
        let stub = stub(
            mail_primary(),
            [reply(
                "session-1",
                vec![get_result("s0"), query_result("s1")],
            )],
        );

        let (get, query) = stub
            .client
            .build()
            .send_methods((TestGet::new(), TestQuery::new()))
            .await
            .expect("batch send");

        assert_eq!(get.list().len(), 1);
        assert_eq!(query.ids().len(), 2);
        assert_eq!(stub.request_count(), 1, "one batch is one round trip");

        let calls = stub.request(0)["methodCalls"]
            .as_array()
            .expect("array")
            .clone();
        assert_eq!(calls[0][0], json!("Test/get"));
        assert_eq!(calls[1][0], json!("Test/query"));
    }

    #[tokio::test]
    async fn responses_returned_out_of_order_still_match_their_handles() {
        // RFC 8620 lets a server return method responses in any order it
        // likes; `Response::get` matches on the call id, so the tuple
        // extraction must not depend on positional alignment.
        let stub = stub(
            mail_primary(),
            [reply(
                "session-1",
                vec![query_result("s1"), get_result("s0")],
            )],
        );

        let (get, query) = stub
            .client
            .build()
            .send_methods((TestGet::new(), TestQuery::new()))
            .await
            .expect("batch send");

        assert_eq!(get.state(), "s1");
        assert_eq!(query.ids().len(), 2);
    }

    #[tokio::test]
    async fn result_references_serialize_as_hash_prefixed_arguments() {
        let stub = stub(
            mail_primary(),
            [reply(
                "session-1",
                vec![query_result("s0"), get_result("s1")],
            )],
        );

        let mut request = stub.client.build();
        let query = request.call(TestQuery::new()).expect("encodes");
        let _get = request
            .call(TestGet::new().ids_ref(query.result_reference("/ids")))
            .expect("encodes");
        request.send().await.expect("stub replies");

        let args = stub.request(0)["methodCalls"][1][1].clone();
        assert_eq!(
            args["#ids"],
            json!({"resultOf": "s0", "name": "Test/query", "path": "/ids"})
        );
        // Setting a reference clears the literal ids, so the server never
        // sees both forms of the same argument.
        assert!(args.get("ids").is_none());
    }

    #[tokio::test]
    async fn a_diverging_session_state_marks_the_cached_session_stale() {
        let stub = stub(
            mail_primary(),
            [
                reply("session-1", vec![get_result("s0")]),
                reply("session-2", vec![get_result("s0")]),
            ],
        );
        assert!(stub.client.is_session_updated());

        // A reply echoing the session's own state leaves it fresh.
        let mut request = stub.client.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        request.send().await.expect("stub replies");
        assert!(stub.client.is_session_updated());

        // A reply carrying a newer sessionState marks it stale so the
        // caller knows to re-read the session resource.
        let mut request = stub.client.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        request.send().await.expect("stub replies");
        assert!(!stub.client.is_session_updated());
    }

    #[tokio::test]
    async fn a_transport_failure_surfaces_as_error_transport() {
        // No queued reply: the stub answers the way a dropped connection
        // does, and the error must not be laundered into a decode error.
        let stub = stub(mail_primary(), Vec::<String>::new());
        let mut request = stub.client.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        let err = request.send().await.expect_err("transport failed");
        assert!(matches!(err, crate::Error::Transport(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn an_unparseable_response_body_is_a_response_decode_error() {
        let stub = stub(mail_primary(), ["not json at all".to_string()]);
        let mut request = stub.client.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        let err = request.send().await.expect_err("body is not JSON");
        assert!(
            matches!(err, crate::Error::ResponseDecode(_)),
            "got {err:?}"
        );
    }
}

// BUG, documented rather than endorsed. `reference/jmap.md` states
// "`CallHandle<M>` validates call_id and method name", but
// `Response::get` matches ONLY on the call id - `handle.method_name` is
// never compared against the name the server echoed back. A server that
// answers call `s0` with a different method's result silently
// deserializes into `M::Response` whenever the two shapes are
// compatible, and every JMAP `/get` response IS shape-compatible
// (accountId + state + list + notFound, with all object fields
// optional). So a `Mailbox/get` body extracted through an `EmailGet`
// handle yields a list of blank `Email`s rather than an error, and the
// hydration path reports them as successfully hydrated.
//
// Fix: compare `name` against `handle.method_name` in `Response::get`
// (the field is already stored on the handle for exactly this purpose)
// and return a contract-violation error on mismatch.
#[test]
fn response_get_matches_the_call_id_only_and_ignores_the_method_name() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["Definitely/not-what-was-asked", {
                "accountId": "A1",
                "state": "s1",
                "list": [{"id": "t1", "name": "hello"}],
                "notFound": []
            }, "s0"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();
    let handle = make_handle::<TestGet>("s0");
    let result = response
        .get(&handle)
        .expect("today this succeeds; it should be a contract violation");
    assert_eq!(result.list().len(), 1);
}
