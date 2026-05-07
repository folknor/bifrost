use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use super::Object;

pub trait ChangesObject: Object {
    type ChangesResponse;
}

#[derive(Debug, Clone, Serialize)]
pub struct ChangesRequest {
    #[serde(rename = "accountId")]
    account_id: String,

    #[serde(rename = "sinceState")]
    since_state: String,

    #[serde(rename = "maxChanges")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_changes: Option<NonZeroUsize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChangesResponse<O: ChangesObject> {
    #[serde(rename = "accountId")]
    account_id: String,

    #[serde(rename = "oldState")]
    old_state: String,

    #[serde(rename = "newState")]
    new_state: String,

    #[serde(rename = "hasMoreChanges")]
    has_more_changes: bool,

    created: Vec<String>,

    updated: Vec<String>,

    destroyed: Vec<String>,

    #[serde(flatten)]
    arguments: O::ChangesResponse,
}

impl ChangesRequest {
    /// Construct a `ChangesRequest` with `accountId` left empty; the
    /// account ID is filled in by
    /// [`crate::core::request::Request::call`] when the method is
    /// added to a request batch.
    pub fn new(since_state: impl Into<String>) -> Self {
        ChangesRequest {
            account_id: String::new(),
            since_state: since_state.into(),
            max_changes: None,
        }
    }

    pub fn account_id(&mut self, account_id: impl Into<String>) -> &mut Self {
        self.account_id = account_id.into();
        self
    }

    /// Cap the response at most `max_changes` ID entries. RFC 8620
    /// requires this to be a positive integer; using `NonZeroUsize`
    /// rejects `0` at compile time.
    pub fn max_changes(&mut self, max_changes: NonZeroUsize) -> &mut Self {
        self.max_changes = Some(max_changes);
        self
    }
}

impl<O: ChangesObject> ChangesResponse<O> {
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn into_account_id(self) -> String {
        self.account_id
    }

    pub fn old_state(&self) -> &str {
        &self.old_state
    }

    pub fn new_state(&self) -> &str {
        &self.new_state
    }

    pub fn into_new_state(self) -> String {
        self.new_state
    }

    pub fn has_more_changes(&self) -> bool {
        self.has_more_changes
    }

    pub fn created(&self) -> &[String] {
        &self.created
    }

    pub fn into_created(self) -> Vec<String> {
        self.created
    }

    pub fn updated(&self) -> &[String] {
        &self.updated
    }

    pub fn into_updated(self) -> Vec<String> {
        self.updated
    }

    pub fn destroyed(&self) -> &[String] {
        &self.destroyed
    }

    pub fn into_destroyed(self) -> Vec<String> {
        self.destroyed
    }

    pub fn arguments(&self) -> &O::ChangesResponse {
        &self.arguments
    }

    pub fn total_changes(&self) -> usize {
        self.created.len() + self.updated.len() + self.destroyed.len()
    }
}
