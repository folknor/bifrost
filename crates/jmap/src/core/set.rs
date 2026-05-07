use crate::Error;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};

use super::{Object, request::ResultReference};

pub trait SetObject: Object {
    type SetArguments: Default;

    fn create_id(&self) -> Option<String>;
}

pub trait SetObjectCreatable: SetObject {
    fn new(create_id: Option<usize>) -> Self;
}

#[derive(Debug, Clone, Serialize)]
pub struct SetRequest<O: SetObject> {
    #[serde(rename = "accountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    account_id: Option<String>,

    #[serde(rename = "ifInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    if_in_state: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    create: Option<HashMap<String, O>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    update: Option<HashMap<String, O>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    destroy: Option<Vec<String>>,

    #[serde(rename = "#destroy")]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    destroy_ref: Option<ResultReference>,

    #[serde(flatten)]
    arguments: O::SetArguments,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SetResponse<O: SetObject> {
    #[serde(rename = "accountId")]
    account_id: Option<String>,

    #[serde(rename = "oldState")]
    old_state: Option<String>,

    #[serde(rename = "newState")]
    new_state: Option<String>,

    #[serde(rename = "created")]
    created: Option<HashMap<String, O>>,

    #[serde(rename = "updated")]
    updated: Option<HashMap<String, Option<O>>>,

    #[serde(rename = "destroyed")]
    destroyed: Option<Vec<String>>,

    #[serde(rename = "notCreated")]
    not_created: Option<HashMap<String, SetError<O::Property>>>,

    #[serde(rename = "notUpdated")]
    not_updated: Option<HashMap<String, SetError<O::Property>>>,

    #[serde(rename = "notDestroyed")]
    not_destroyed: Option<HashMap<String, SetError<O::Property>>>,
}

#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct SetError<U>
where
    U: Display,
{
    #[serde(rename = "type")]
    type_: SetErrorType,
    description: Option<String>,
    properties: Option<Vec<U>>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
#[non_exhaustive]
pub enum SetErrorType {
    #[serde(rename = "forbidden")]
    Forbidden,
    #[serde(rename = "overQuota")]
    OverQuota,
    #[serde(rename = "tooLarge")]
    TooLarge,
    #[serde(rename = "rateLimit")]
    RateLimit,
    #[serde(rename = "notFound")]
    NotFound,
    #[serde(rename = "invalidPatch")]
    InvalidPatch,
    #[serde(rename = "willDestroy")]
    WillDestroy,
    #[serde(rename = "invalidProperties")]
    InvalidProperties,
    #[serde(rename = "singleton")]
    Singleton,
    #[serde(rename = "mailboxHasChild")]
    MailboxHasChild,
    #[serde(rename = "mailboxHasEmail")]
    MailboxHasEmail,
    #[serde(rename = "blobNotFound")]
    BlobNotFound,
    #[serde(rename = "tooManyKeywords")]
    TooManyKeywords,
    #[serde(rename = "tooManyMailboxes")]
    TooManyMailboxes,
    #[serde(rename = "forbiddenFrom")]
    ForbiddenFrom,
    #[serde(rename = "invalidEmail")]
    InvalidEmail,
    #[serde(rename = "tooManyRecipients")]
    TooManyRecipients,
    #[serde(rename = "noRecipients")]
    NoRecipients,
    #[serde(rename = "invalidRecipients")]
    InvalidRecipients,
    #[serde(rename = "forbiddenMailFrom")]
    ForbiddenMailFrom,
    #[serde(rename = "forbiddenToSend")]
    ForbiddenToSend,
    #[serde(rename = "cannotUnsend")]
    CannotUnsend,
    #[serde(rename = "alreadyExists")]
    AlreadyExists,
    #[serde(rename = "invalidScript")]
    InvalidScript,
    #[serde(rename = "scriptIsActive")]
    ScriptIsActive,
    #[serde(other)]
    Other,
}

impl<O: SetObject> SetRequest<O> {
    /// Construct an empty `SetRequest`. The `accountId` field is left
    /// unset (or `None` for non-account-scoped objects); it is filled
    /// in by [`crate::core::request::Request::call`] when the method
    /// is added to a request batch.
    pub fn new() -> Self {
        Self {
            account_id: if O::requires_account_id() {
                Some(String::new())
            } else {
                None
            },
            if_in_state: None,
            create: None,
            update: None,
            destroy: None,
            destroy_ref: None,
            arguments: Default::default(),
        }
    }

    pub fn account_id(&mut self, account_id: impl Into<String>) -> &mut Self {
        if O::requires_account_id() {
            self.account_id = Some(account_id.into());
        }
        self
    }

    pub fn if_in_state(&mut self, if_in_state: impl Into<String>) -> &mut Self {
        self.if_in_state = Some(if_in_state.into());
        self
    }

    pub fn destroy<U, V>(&mut self, ids: U) -> &mut Self
    where
        U: IntoIterator<Item = V>,
        V: Into<String>,
    {
        self.destroy
            .get_or_insert_with(Vec::new)
            .extend(ids.into_iter().map(std::convert::Into::into));
        self.destroy_ref = None;
        self
    }

    pub fn destroy_ref(&mut self, reference: ResultReference) -> &mut Self {
        self.destroy_ref = reference.into();
        self.destroy = None;
        self
    }

    pub fn arguments(&mut self) -> &mut O::SetArguments {
        &mut self.arguments
    }
}

impl<O: SetObject> Default for SetRequest<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: SetObjectCreatable> SetRequest<O> {
    pub fn create(&mut self) -> &mut O {
        let create_id = self
            .create
            .as_ref()
            .map_or(0, std::collections::HashMap::len);
        let create_id_str = format!("c{create_id}");
        self.create
            .get_or_insert_with(HashMap::new)
            .entry(create_id_str)
            .or_insert_with(|| O::new(create_id.into()))
    }

    pub fn create_with_id(&mut self, create_id: impl Into<String>) -> &mut O {
        let create_id = create_id.into();
        self.create
            .get_or_insert_with(HashMap::new)
            .entry(create_id)
            .or_insert_with(|| O::new(0.into()))
    }

    pub fn create_item(&mut self, item: O) -> String {
        let create_id = self
            .create
            .as_ref()
            .map_or(0, std::collections::HashMap::len);
        let create_id_str = format!("c{create_id}");
        self.create
            .get_or_insert_with(HashMap::new)
            .insert(create_id_str.clone(), item);
        create_id_str
    }

    pub fn update(&mut self, id: impl Into<String>) -> &mut O {
        let id: String = id.into();
        self.update
            .get_or_insert_with(HashMap::new)
            .entry(id)
            .or_insert_with(|| O::new(None))
    }

    pub fn update_item(&mut self, id: impl Into<String>, item: O) {
        self.update
            .get_or_insert_with(HashMap::new)
            .insert(id.into(), item);
    }
}

impl<O: SetObject> SetResponse<O> {
    pub fn account_id(&self) -> Option<&str> {
        self.account_id.as_deref()
    }

    pub fn old_state(&self) -> Option<&str> {
        self.old_state.as_deref()
    }

    pub fn new_state(&self) -> &str {
        self.new_state.as_deref().unwrap_or("")
    }

    pub fn take_new_state(&mut self) -> String {
        self.new_state.take().unwrap_or_default()
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

    pub fn updated(&mut self, id: &str) -> crate::Result<Option<O>> {
        if let Some(result) = self.updated.as_mut().and_then(|r| r.remove(id)) {
            Ok(result)
        } else if let Some(error) = self.not_updated.as_mut().and_then(|r| r.remove(id)) {
            Err(error.to_string_error().into())
        } else {
            Err(Error::IdNotFound(id.to_string()))
        }
    }

    pub fn destroyed(&mut self, id: &str) -> crate::Result<()> {
        if self
            .destroyed
            .as_ref()
            .is_some_and(|r| r.iter().any(|i| i == id))
        {
            Ok(())
        } else if let Some(error) = self.not_destroyed.as_mut().and_then(|r| r.remove(id)) {
            Err(error.to_string_error().into())
        } else {
            Err(Error::IdNotFound(id.to_string()))
        }
    }

    pub fn created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.created.as_ref().map(|map| map.keys())
    }

    pub fn updated_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.updated.as_ref().map(|map| map.keys())
    }

    pub fn take_updated_ids(&mut self) -> Option<Vec<String>> {
        self.updated.take().map(|map| map.into_keys().collect())
    }

    pub fn destroyed_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.destroyed.as_ref().map(|list| list.iter())
    }

    pub fn take_destroyed_ids(&mut self) -> Option<Vec<String>> {
        self.destroyed.take()
    }

    pub fn not_created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.not_created.as_ref().map(|map| map.keys())
    }

    pub fn not_updated_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.not_updated.as_ref().map(|map| map.keys())
    }

    pub fn not_destroyed_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.not_destroyed.as_ref().map(|map| map.keys())
    }

    pub fn has_updated(&self) -> bool {
        self.updated.as_ref().is_some_and(|m| !m.is_empty())
    }

    pub fn has_created(&self) -> bool {
        self.created.as_ref().is_some_and(|m| !m.is_empty())
    }

    pub fn has_destroyed(&self) -> bool {
        self.destroyed.as_ref().is_some_and(|m| !m.is_empty())
    }

    pub fn unwrap_update_errors(&self) -> crate::Result<()> {
        if let Some(errors) = &self.not_updated
            && let Some(err) = errors.values().next()
        {
            return Err(err.to_string_error().into());
        }
        Ok(())
    }

    pub fn unwrap_create_errors(&self) -> crate::Result<()> {
        if let Some(errors) = &self.not_created
            && let Some(err) = errors.values().next()
        {
            return Err(err.to_string_error().into());
        }
        Ok(())
    }
}

impl<U: Display> SetError<U> {
    pub fn error_type(&self) -> &SetErrorType {
        &self.type_
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn properties(&self) -> Option<&[U]> {
        self.properties.as_deref()
    }

    pub fn to_string_error(&self) -> SetError<String> {
        SetError {
            type_: self.type_.clone(),
            description: self.description.clone(),
            properties: self
                .properties
                .as_ref()
                .map(|s| s.iter().map(std::string::ToString::to_string).collect()),
        }
    }
}

impl<U: Display> Display for SetError<U> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.type_.fmt(f)?;
        if let Some(description) = &self.description {
            write!(f, ": {description}")?;
        }
        if let Some(properties) = &self.properties {
            write!(
                f,
                " (properties: {})",
                properties
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }
        Ok(())
    }
}

impl Display for SetErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SetErrorType::Forbidden => write!(f, "forbidden"),
            SetErrorType::OverQuota => write!(f, "overQuota"),
            SetErrorType::TooLarge => write!(f, "tooLarge"),
            SetErrorType::RateLimit => write!(f, "rateLimit"),
            SetErrorType::NotFound => write!(f, "notFound"),
            SetErrorType::InvalidPatch => write!(f, "invalidPatch"),
            SetErrorType::WillDestroy => write!(f, "willDestroy"),
            SetErrorType::InvalidProperties => write!(f, "invalidProperties"),
            SetErrorType::Singleton => write!(f, "singleton"),
            SetErrorType::MailboxHasChild => write!(f, "mailboxHasChild"),
            SetErrorType::MailboxHasEmail => write!(f, "mailboxHasEmail"),
            SetErrorType::BlobNotFound => write!(f, "blobNotFound"),
            SetErrorType::TooManyKeywords => write!(f, "tooManyKeywords"),
            SetErrorType::TooManyMailboxes => write!(f, "tooManyMailboxes"),
            SetErrorType::ForbiddenFrom => write!(f, "forbiddenFrom"),
            SetErrorType::InvalidEmail => write!(f, "invalidEmail"),
            SetErrorType::TooManyRecipients => write!(f, "tooManyRecipients"),
            SetErrorType::NoRecipients => write!(f, "noRecipients"),
            SetErrorType::InvalidRecipients => write!(f, "invalidRecipients"),
            SetErrorType::ForbiddenMailFrom => write!(f, "forbiddenMailFrom"),
            SetErrorType::ForbiddenToSend => write!(f, "forbiddenToSend"),
            SetErrorType::CannotUnsend => write!(f, "cannotUnsend"),
            SetErrorType::AlreadyExists => write!(f, "alreadyExists"),
            SetErrorType::InvalidScript => write!(f, "invalidScript"),
            SetErrorType::ScriptIsActive => write!(f, "scriptIsActive"),
            SetErrorType::Other => write!(f, "other"),
        }
    }
}

pub fn from_timestamp(timestamp: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(timestamp, 0).unwrap_or_default()
}

pub fn skip_if_empty_str(string: &Option<String>) -> bool {
    matches!(string, Some(string) if string.is_empty())
}

pub fn skip_if_zero_date(date: &Option<DateTime<Utc>>) -> bool {
    matches!(date, Some(date) if date.timestamp() == 0)
}

pub fn skip_if_empty_list<O>(list: &Option<Vec<O>>) -> bool {
    matches!(list, Some(list) if list.is_empty() )
}

pub fn skip_if_empty_map<K, V>(list: &Option<HashMap<K, V>>) -> bool {
    matches!(list, Some(list) if list.is_empty() )
}
