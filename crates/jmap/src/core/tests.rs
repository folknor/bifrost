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
