pub mod get;
pub mod helpers;
pub mod query;
pub mod set;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::Get;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalendarEventNotification<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(rename = "created")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<String>,

    #[serde(rename = "changedBy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_by: Option<ChangedBy>,

    #[serde(rename = "calendarEventId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calendar_event_id: Option<String>,

    #[serde(rename = "isDraft")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_draft: Option<bool>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_: Option<NotificationType>,

    #[serde(rename = "event")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<serde_json::Value>,

    #[serde(rename = "eventPatch")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_patch: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedBy {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(rename = "email")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,

    #[serde(rename = "principalId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum NotificationType {
    #[serde(rename = "created")]
    Created,
    #[serde(rename = "updated")]
    Updated,
    #[serde(rename = "destroyed")]
    Destroyed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "created")]
    Created,
    #[serde(rename = "changedBy")]
    ChangedBy,
    #[serde(rename = "calendarEventId")]
    CalendarEventId,
    #[serde(rename = "isDraft")]
    IsDraft,
    #[serde(rename = "type")]
    Type,
    #[serde(rename = "event")]
    Event,
    #[serde(rename = "eventPatch")]
    EventPatch,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Created => write!(f, "created"),
            Property::ChangedBy => write!(f, "changedBy"),
            Property::CalendarEventId => write!(f, "calendarEventId"),
            Property::IsDraft => write!(f, "isDraft"),
            Property::Type => write!(f, "type"),
            Property::Event => write!(f, "event"),
            Property::EventPatch => write!(f, "eventPatch"),
        }
    }
}

crate::impl_jmap_object!(CalendarEventNotification<State>, Property, true);

use crate::Set;

// Method structs for the new architecture
crate::define_get_method!(CalendarEventNotificationGet, CalendarEventNotification<Set>, "CalendarEventNotification/get", crate::core::capability::Calendars, crate::core::get::GetResponse<CalendarEventNotification<Get>>);
crate::define_set_method!(CalendarEventNotificationSet, CalendarEventNotification<Set>, "CalendarEventNotification/set", crate::core::capability::Calendars, crate::core::set::SetResponse<CalendarEventNotification<Get>>);
crate::define_changes_method!(CalendarEventNotificationChanges, "CalendarEventNotification/changes", crate::core::capability::Calendars, crate::core::changes::ChangesResponse<CalendarEventNotification<Get>>);
crate::define_query_method!(CalendarEventNotificationQuery, CalendarEventNotification<Set>, "CalendarEventNotification/query", crate::core::capability::Calendars);
crate::define_query_changes_method!(CalendarEventNotificationQueryChanges, CalendarEventNotification<Set>, "CalendarEventNotification/queryChanges", crate::core::capability::Calendars);
