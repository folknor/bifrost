pub mod parser;
pub mod stream;

#[cfg(feature = "calendars")]
use crate::CalendarAlert;
use crate::{DataType, core::session::URLParser};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[non_exhaustive]
pub enum URLParameter {
    Types,
    CloseAfter,
    Ping,
}

impl URLParser for URLParameter {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "types" => Some(URLParameter::Types),
            "closeafter" => Some(URLParameter::CloseAfter),
            "ping" => Some(URLParameter::Ping),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushNotification {
    StateChange(Changes),
    #[cfg(feature = "calendars")]
    CalendarAlert(CalendarAlert),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Changes {
    id: Option<String>,
    changes: HashMap<String, HashMap<DataType, String>>,
}

impl Changes {
    pub fn new(id: Option<String>, changes: HashMap<String, HashMap<DataType, String>>) -> Self {
        Self { id, changes }
    }

    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn account_changes(&mut self, account_id: &str) -> Option<HashMap<DataType, String>> {
        self.changes.remove(account_id)
    }

    pub fn changed_accounts(&self) -> impl Iterator<Item = &String> {
        self.changes.keys()
    }

    pub fn changes(&self, account_id: &str) -> Option<impl Iterator<Item = (&DataType, &String)>> {
        self.changes.get(account_id).map(|changes| changes.iter())
    }

    pub fn has_type(&self, type_: DataType) -> bool {
        self.changes
            .values()
            .any(|changes| changes.contains_key(&type_))
    }

    pub fn into_inner(self) -> HashMap<String, HashMap<DataType, String>> {
        self.changes
    }

    pub fn is_empty(&self) -> bool {
        !self.changes.values().any(|changes| !changes.is_empty())
    }
}
