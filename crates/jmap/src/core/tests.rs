#![cfg(test)]

use serde_json::json;

use std::marker::PhantomData;

use super::response::Response;
use super::request::CallHandle;
use super::method::JmapMethod;
use super::get::{GetObject, GetResponse};
use super::query::QueryObject;
use crate::{Get, Set, Error};

// -- Minimal test types --

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TestObj<State = Get> {
    #[serde(skip)]
    _state: std::marker::PhantomData<State>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum TestProp {
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

impl super::Object for TestObj<Set> {
    type Property = TestProp;
    fn requires_account_id() -> bool { true }
}
impl super::Object for TestObj<Get> {
    type Property = TestProp;
    fn requires_account_id() -> bool { true }
}
impl GetObject for TestObj<Set> {
    type GetArguments = ();
}
impl GetObject for TestObj<Get> {
    type GetArguments = ();
}
impl super::set::SetObject for TestObj<Set> {
    type SetArguments = ();
    fn create_id(&self) -> Option<String> { None }
}
impl super::set::SetObjectCreatable for TestObj<Set> {
    fn new(_: Option<usize>) -> Self {
        TestObj { _state: Default::default(), id: None, name: None }
    }
}
impl super::set::SetObject for TestObj<Get> {
    type SetArguments = ();
    fn create_id(&self) -> Option<String> { None }
}
impl super::changes::ChangesObject for TestObj<Set> {
    type ChangesResponse = ();
}
impl super::changes::ChangesObject for TestObj<Get> {
    type ChangesResponse = ();
}
impl QueryObject for TestObj<Set> {
    type QueryArguments = ();
    type Filter = ();
    type Sort = ();
}

// Define test method types (allow dead_code — some macro-generated methods unused in tests)
#[allow(dead_code)]
crate::define_get_method!(
    TestGet, TestObj<Set>, "Test/get",
    crate::core::capability::Core,
    GetResponse<TestObj<Get>>
);

#[allow(dead_code)]
crate::define_query_method!(
    TestQuery, TestObj<Set>, "Test/query",
    crate::core::capability::Core
);

// -- Helper: build a CallHandle without going through Request --

fn make_handle<M: JmapMethod>(call_id: &str) -> CallHandle<M> {
    // CallHandle fields are pub(crate), accessible from tests
    CallHandle {
        call_id: call_id.to_string(),
        method_name: M::NAME,
        _phantom: PhantomData,
    }
}

// -- Tests --

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
    let result: GetResponse<TestObj<Get>> = response.get(&handle).unwrap();

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

    // Extract get — succeeds
    let handle_get = make_handle::<TestGet>("s0");
    let get_result = response.get(&handle_get).unwrap();
    assert_eq!(get_result.list().len(), 1);

    // Extract error call — returns MethodError
    let handle_err = make_handle::<TestGet>("s1");
    assert!(matches!(response.get(&handle_err), Err(Error::Method(_))));

    // Extract query — succeeds
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

    // First extraction succeeds
    let _ = response.get(&handle).unwrap();

    // Second extraction fails — entry consumed
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

    let err = TransportError::with_body(
        "HTTP 429",
        serde_json::to_vec(&problem_json).unwrap(),
    );

    let error: Error = err.into();
    assert!(matches!(error, Error::Problem(_)));
}

#[test]
fn transport_error_without_body_stays_transport() {
    use crate::core::transport::TransportError;

    let err = TransportError::new("connection refused");
    let error: Error = err.into();
    assert!(matches!(error, Error::Transport(_)));
}

#[test]
fn set_response_deserializes_without_creatable() {
    // SetResponse<TestObj<Get>> must work even though TestObj<Get>
    // does not implement SetObjectCreatable — only SetObject.
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

    let mut response: SetResponse<TestObj<Get>> = serde_json::from_value(raw).unwrap();
    assert_eq!(response.new_state(), "s2");
    let created = response.created("c0").unwrap();
    assert_eq!(created.name.as_deref(), Some("created-obj"));
}

#[test]
fn request_serializes_correctly() {
    let get = TestGet::new("account-1");
    let value = serde_json::to_value(&get).unwrap();
    assert_eq!(value.get("accountId"), Some(&json!("account-1")));
}
