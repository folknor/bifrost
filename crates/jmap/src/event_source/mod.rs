// The Account impl uses the WebSocket push path; SSE / RFC 8620 §7.3
// types and the parser are kept built for the HTTP-push consumer a
// future stage may want.
#![allow(dead_code)]

pub(crate) mod parser;
pub(crate) mod stream;

#[cfg(feature = "calendars")]
use crate::CalendarAlert;
use crate::{DataType, core::session::URLParser};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[non_exhaustive]
pub(crate) enum URLParameter {
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
pub(crate) enum PushNotification {
    StateChange(Changes),
    #[cfg(feature = "calendars")]
    CalendarAlert(CalendarAlert),
    /// An `EmailPush` payload, forwarded as what it is rather than dropped.
    ///
    /// It carries no state string of its own, so it cannot be folded into the
    /// block's merged `StateChange`: a synthesised state string would be a lie
    /// about a position the consumer compares against a stored cursor. It is
    /// surfaced verbatim instead - `account_id` plus the raw `email` JSON the
    /// server sent - so that a block whose state change commits the resume
    /// token cannot silently bury a push object that will never be replayed.
    #[cfg(feature = "mail")]
    EmailPush {
        account_id: String,
        email: serde_json::Value,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Changes {
    id: Option<String>,
    changes: HashMap<String, HashMap<DataType, String>>,
}

impl Changes {
    pub(crate) fn new(
        id: Option<String>,
        changes: HashMap<String, HashMap<DataType, String>>,
    ) -> Self {
        Self { id, changes }
    }

    pub(crate) fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub(crate) fn account_changes(
        &mut self,
        account_id: &str,
    ) -> Option<HashMap<DataType, String>> {
        self.changes.remove(account_id)
    }

    pub(crate) fn changed_accounts(&self) -> impl Iterator<Item = &String> {
        self.changes.keys()
    }

    pub(crate) fn changes(
        &self,
        account_id: &str,
    ) -> Option<impl Iterator<Item = (&DataType, &String)>> {
        self.changes.get(account_id).map(|changes| changes.iter())
    }

    pub(crate) fn has_type(&self, type_: DataType) -> bool {
        self.changes
            .values()
            .any(|changes| changes.contains_key(&type_))
    }

    pub(crate) fn into_inner(self) -> HashMap<String, HashMap<DataType, String>> {
        self.changes
    }

    pub(crate) fn is_empty(&self) -> bool {
        !self.changes.values().any(|changes| !changes.is_empty())
    }
}
