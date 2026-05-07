use serde::{Deserialize, Serialize};

use super::Object;
use super::id::AccountId;
use super::request::ResultReference;

pub trait GetObject: Object {
    type GetArguments: Default + Serialize;
}

#[derive(Debug, Clone, Serialize)]
pub struct GetRequest<O: GetObject> {
    #[serde(rename = "accountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    account_id: Option<AccountId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    ids: Option<Vec<O::Id>>,

    #[serde(rename = "#ids")]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    ids_ref: Option<ResultReference>,

    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<Vec<O::Property>>,

    #[serde(rename = "#properties")]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    properties_ref: Option<ResultReference>,

    #[serde(flatten)]
    arguments: O::GetArguments,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetResponse<O: Object> {
    #[serde(rename = "accountId")]
    account_id: Option<AccountId>,

    state: String,

    list: Vec<O>,

    #[serde(rename = "notFound")]
    not_found: Vec<O::Id>,
}

impl<O: GetObject> GetRequest<O> {
    pub fn new() -> Self {
        GetRequest {
            account_id: if O::requires_account_id() {
                Some(AccountId::new(""))
            } else {
                None
            },
            ids: None,
            ids_ref: None,
            properties: None,
            properties_ref: None,
            arguments: O::GetArguments::default(),
        }
    }

    pub fn account_id(&mut self, account_id: impl Into<AccountId>) -> &mut Self {
        if O::requires_account_id() {
            self.account_id = Some(account_id.into());
        }
        self
    }

    pub fn ids<U, V>(&mut self, ids: U) -> &mut Self
    where
        U: IntoIterator<Item = V>,
        V: Into<O::Id>,
    {
        self.ids = Some(ids.into_iter().map(std::convert::Into::into).collect());
        self.ids_ref = None;
        self
    }

    pub fn ids_ref(&mut self, reference: ResultReference) -> &mut Self {
        self.ids_ref = reference.into();
        self.ids = None;
        self
    }

    pub fn properties(&mut self, properties: impl IntoIterator<Item = O::Property>) -> &mut Self {
        self.properties = Some(properties.into_iter().collect());
        self.properties_ref = None;
        self
    }

    pub fn properties_ref(&mut self, reference: ResultReference) -> &mut Self {
        self.properties_ref = Some(reference);
        self.properties = None;
        self
    }

    pub fn arguments(&mut self) -> &mut O::GetArguments {
        &mut self.arguments
    }
}

impl<O: GetObject> Default for GetRequest<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Object> GetResponse<O> {
    pub fn account_id(&self) -> Option<&AccountId> {
        self.account_id.as_ref()
    }

    pub fn state(&self) -> &str {
        &self.state
    }

    pub fn into_state(self) -> String {
        self.state
    }

    pub fn list(&self) -> &[O] {
        &self.list
    }

    pub fn not_found(&self) -> &[O::Id] {
        &self.not_found
    }

    pub fn into_list(self) -> Vec<O> {
        self.list
    }

    pub fn pop(&mut self) -> Option<O> {
        self.list.pop()
    }

    pub fn into_not_found(self) -> Vec<O::Id> {
        self.not_found
    }
}
