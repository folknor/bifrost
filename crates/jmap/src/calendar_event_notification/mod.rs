pub mod get;
pub mod query;
pub mod set;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

mod marker {
    pub enum CalendarEventNotification {}
}
/// Strongly-typed CalendarEventNotification ID.
pub type CalendarEventNotificationId = crate::core::id::Id<marker::CalendarEventNotification>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalendarEventNotification {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<CalendarEventNotificationId>,

    #[serde(rename = "created")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) created: Option<String>,

    #[serde(rename = "changedBy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) changed_by: Option<ChangedBy>,

    #[serde(rename = "calendarEventId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) calendar_event_id: Option<crate::calendar_event::CalendarEventId>,

    #[serde(rename = "isDraft")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_draft: Option<bool>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) type_: Option<NotificationType>,

    #[serde(rename = "event")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) event: Option<serde_json::Value>,

    #[serde(rename = "eventPatch")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) event_patch: Option<serde_json::Value>,
}

/// Uninhabitable - notifications are server-generated, only destroy is allowed.
#[derive(Debug, Clone, Serialize)]
pub enum CalendarEventNotificationCreate {}

#[derive(Debug, Clone, Serialize)]
pub enum CalendarEventNotificationPatch {}

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
    pub principal_id: Option<crate::principal::PrincipalId>,
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

impl crate::core::Object for CalendarEventNotification {
    type Property = Property;
    type Id = CalendarEventNotificationId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for CalendarEventNotification {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for CalendarEventNotification {
    type GetArguments = ();
}

impl crate::core::set::SetObject for CalendarEventNotification {
    type Create = CalendarEventNotificationCreate;
    type Patch = CalendarEventNotificationPatch;
    type SetArguments = ();
}

crate::define_get_method!(
    CalendarEventNotificationGet,
    CalendarEventNotification,
    "CalendarEventNotification/get",
    crate::core::capability::Calendars
);
crate::define_set_method!(
    CalendarEventNotificationSet,
    CalendarEventNotification,
    "CalendarEventNotification/set",
    crate::core::capability::Calendars
);
crate::define_changes_method!(
    CalendarEventNotificationChanges,
    CalendarEventNotification,
    "CalendarEventNotification/changes",
    crate::core::capability::Calendars
);
crate::define_query_method!(
    CalendarEventNotificationQuery,
    CalendarEventNotification,
    "CalendarEventNotification/query",
    crate::core::capability::Calendars
);
crate::define_query_changes_method!(
    CalendarEventNotificationQueryChanges,
    CalendarEventNotification,
    "CalendarEventNotification/queryChanges",
    crate::core::capability::Calendars
);
