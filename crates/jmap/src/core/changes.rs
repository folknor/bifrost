use std::num::NonZeroUsize;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::Object;
use super::id::AccountId;

pub trait ChangesObject: Object {
    type ChangesResponse: DeserializeOwned;
}

#[derive(Debug, Clone, Serialize)]
pub struct ChangesRequest {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "sinceState")]
    since_state: String,

    #[serde(rename = "maxChanges")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_changes: Option<NonZeroUsize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChangesResponse<O: ChangesObject> {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "oldState")]
    old_state: String,

    #[serde(rename = "newState")]
    new_state: String,

    #[serde(rename = "hasMoreChanges")]
    has_more_changes: bool,

    created: Vec<O::Id>,

    updated: Vec<O::Id>,

    destroyed: Vec<O::Id>,

    #[serde(flatten)]
    arguments: O::ChangesResponse,
}

impl ChangesRequest {
    pub fn new(since_state: impl Into<String>) -> Self {
        ChangesRequest {
            account_id: AccountId::new(""),
            since_state: since_state.into(),
            max_changes: None,
        }
    }

    pub fn account_id(&mut self, account_id: impl Into<AccountId>) -> &mut Self {
        self.account_id = account_id.into();
        self
    }

    pub fn max_changes(&mut self, max_changes: NonZeroUsize) -> &mut Self {
        self.max_changes = Some(max_changes);
        self
    }
}

impl<O: ChangesObject> ChangesResponse<O> {
    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn into_account_id(self) -> AccountId {
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

    pub fn created(&self) -> &[O::Id] {
        &self.created
    }

    pub fn into_created(self) -> Vec<O::Id> {
        self.created
    }

    pub fn updated(&self) -> &[O::Id] {
        &self.updated
    }

    pub fn into_updated(self) -> Vec<O::Id> {
        self.updated
    }

    pub fn destroyed(&self) -> &[O::Id] {
        &self.destroyed
    }

    pub fn into_destroyed(self) -> Vec<O::Id> {
        self.destroyed
    }

    pub fn arguments(&self) -> &O::ChangesResponse {
        &self.arguments
    }

    pub fn total_changes(&self) -> usize {
        self.created.len() + self.updated.len() + self.destroyed.len()
    }
}
