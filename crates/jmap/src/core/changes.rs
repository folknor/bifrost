use std::num::NonZeroUsize;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::Object;
use super::id::AccountId;

pub(crate) trait ChangesObject: Object {
    type ChangesResponse: DeserializeOwned;
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ChangesRequest {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "sinceState")]
    since_state: String,

    #[serde(rename = "maxChanges")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_changes: Option<NonZeroUsize>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ChangesResponse<O: ChangesObject> {
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
    pub(crate) fn new(since_state: impl Into<String>) -> Self {
        ChangesRequest {
            account_id: AccountId::new(""),
            since_state: since_state.into(),
            max_changes: None,
        }
    }

    pub(crate) fn account_id(&mut self, account_id: impl Into<AccountId>) -> &mut Self {
        self.account_id = account_id.into();
        self
    }

    pub(crate) fn max_changes(&mut self, max_changes: NonZeroUsize) -> &mut Self {
        self.max_changes = Some(max_changes);
        self
    }
}

impl<O: ChangesObject> ChangesResponse<O> {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn into_account_id(self) -> AccountId {
        self.account_id
    }

    pub(crate) fn old_state(&self) -> &str {
        &self.old_state
    }

    pub(crate) fn new_state(&self) -> &str {
        &self.new_state
    }

    pub(crate) fn into_new_state(self) -> String {
        self.new_state
    }

    pub(crate) fn has_more_changes(&self) -> bool {
        self.has_more_changes
    }

    pub(crate) fn created(&self) -> &[O::Id] {
        &self.created
    }

    pub(crate) fn into_created(self) -> Vec<O::Id> {
        self.created
    }

    pub(crate) fn updated(&self) -> &[O::Id] {
        &self.updated
    }

    pub(crate) fn into_updated(self) -> Vec<O::Id> {
        self.updated
    }

    pub(crate) fn destroyed(&self) -> &[O::Id] {
        &self.destroyed
    }

    pub(crate) fn into_destroyed(self) -> Vec<O::Id> {
        self.destroyed
    }

    pub(crate) fn arguments(&self) -> &O::ChangesResponse {
        &self.arguments
    }

    pub(crate) fn total_changes(&self) -> usize {
        self.created.len() + self.updated.len() + self.destroyed.len()
    }
}
