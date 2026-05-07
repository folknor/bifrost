use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Error;

use super::set::{SetError, SetObject, SetObjectCreatable};

#[derive(Debug, Clone, Serialize)]
pub struct CopyRequest<O: SetObject> {
    #[serde(rename = "fromAccountId")]
    from_account_id: String,

    #[serde(rename = "ifFromInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    if_from_in_state: Option<String>,

    #[serde(rename = "accountId")]
    account_id: String,

    #[serde(rename = "ifInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    if_in_state: Option<String>,

    #[serde(rename = "create")]
    create: HashMap<String, O>,

    #[serde(rename = "onSuccessDestroyOriginal")]
    on_success_destroy_original: bool,

    #[serde(rename = "destroyFromIfInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    destroy_from_if_in_state: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CopyResponse<O: SetObject> {
    #[serde(rename = "fromAccountId")]
    from_account_id: String,

    #[serde(rename = "accountId")]
    account_id: String,

    #[serde(rename = "oldState")]
    old_state: Option<String>,

    #[serde(rename = "newState")]
    new_state: String,

    #[serde(rename = "created")]
    created: Option<HashMap<String, O>>,

    #[serde(rename = "notCreated")]
    not_created: Option<HashMap<String, SetError<O::Property>>>,
}

impl<T: SetObjectCreatable> CopyRequest<T> {
    /// Construct a `CopyRequest`. The destination `accountId` is left
    /// empty and filled in by
    /// [`crate::core::request::Request::call`] when the method is
    /// added to a request batch (the destination is the account that
    /// owns the request). The source `fromAccountId` is the only
    /// account argument here, since it is genuinely a per-call value.
    pub fn new(from_account_id: impl Into<String>) -> Self {
        CopyRequest {
            from_account_id: from_account_id.into(),
            if_from_in_state: None,
            account_id: String::new(),
            if_in_state: None,
            create: HashMap::new(),
            on_success_destroy_original: false,
            destroy_from_if_in_state: None,
        }
    }

    pub fn account_id(&mut self, account_id: impl Into<String>) -> &mut Self {
        self.account_id = account_id.into();
        self
    }

    pub fn if_from_in_state(&mut self, if_from_in_state: impl Into<String>) -> &mut Self {
        self.if_from_in_state = Some(if_from_in_state.into());
        self
    }

    pub fn if_in_state(&mut self, if_in_state: impl Into<String>) -> &mut Self {
        self.if_in_state = Some(if_in_state.into());
        self
    }

    pub fn create(&mut self, id: impl Into<String>) -> &mut T {
        let id = id.into();
        self.create.insert(id.clone(), T::new(None));
        self.create.get_mut(&id).unwrap()
    }

    pub fn on_success_destroy_original(&mut self, on_success_destroy_original: bool) -> &mut Self {
        self.on_success_destroy_original = on_success_destroy_original;
        self
    }

    pub fn destroy_from_if_in_state(
        &mut self,
        destroy_from_if_in_state: impl Into<String>,
    ) -> &mut Self {
        self.destroy_from_if_in_state = Some(destroy_from_if_in_state.into());
        self
    }
}

impl<O: SetObject> CopyResponse<O> {
    pub fn from_account_id(&self) -> &str {
        &self.from_account_id
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn old_state(&self) -> Option<&str> {
        self.old_state.as_deref()
    }

    pub fn new_state(&self) -> &str {
        &self.new_state
    }

    pub fn created(&mut self, id: &str) -> crate::Result<O> {
        if let Some(result) = self.created.as_mut().and_then(|r| r.remove(id)) {
            Ok(result)
        } else if let Some(error) = self.not_created.as_mut().and_then(|r| r.remove(id)) {
            Err(error.to_string_error().into())
        } else {
            Err(Error::IdNotFound(id.to_string()))
        }
    }

    pub fn into_created(self) -> Option<Vec<O>> {
        self.created.map(|map| map.into_values().collect())
    }

    pub fn created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.created.as_ref().map(|map| map.keys())
    }

    pub fn not_created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.not_created.as_ref().map(|map| map.keys())
    }
}
