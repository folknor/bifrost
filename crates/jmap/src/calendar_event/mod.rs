//! CalendarEvent wraps a JSCalendar Event (RFC 8984) object.
//!
//! Unlike Email or Mailbox, a CalendarEvent IS a JSCalendar object - its
//! property set is open-ended and includes vendor extension properties
//! (e.g. `example.com:custom-field`). The struct therefore stores all
//! properties in a `serde_json::Map` for round-trip fidelity.
//!
//! Typed accessor and builder methods are provided for common properties.
//! For extension or less-common properties, use [`CalendarEvent::property()`]
//! and [`CalendarEvent::set_property()`].
//!
//! For iCalendar ↔ JSCalendar conversion, the `calcard` crate (re-exported
//! from this crate) can parse the serialized JSON.

pub mod get;
pub mod parse;
pub mod query;
pub mod set;

use serde::{Deserialize, Serialize};

mod marker {
    pub enum CalendarEvent {}
}
/// Strongly-typed CalendarEvent ID.
pub type CalendarEventId = crate::core::id::Id<marker::CalendarEvent>;

crate::json_object_struct!(CalendarEvent, "a JSCalendar object", Property, SetArguments);

// ---- Alert type (used by both CalendarEvent and Calendar default alerts) ----

/// JSCalendar Alert object (RFC 8984 Section 4.5).
///
/// Used directly in Calendar's `defaultAlertsWithTime` /
/// `defaultAlertsWithoutTime`, and also representable in CalendarEvent's
/// `alerts` map (though CalendarEvent stores all properties as JSON).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    #[serde(rename = "@type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,

    #[serde(rename = "trigger")]
    pub trigger: AlertTrigger,

    #[serde(rename = "action")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<AlertAction>,

    #[serde(rename = "acknowledged")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acknowledged: Option<String>,

    #[serde(rename = "relatedTo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_to: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "@type")]
#[non_exhaustive]
pub enum AlertTrigger {
    #[serde(rename = "OffsetTrigger")]
    OffsetTrigger {
        #[serde(rename = "offset")]
        offset: String,

        #[serde(rename = "relativeTo")]
        #[serde(skip_serializing_if = "Option::is_none")]
        relative_to: Option<RelativeTo>,
    },
    #[serde(rename = "AbsoluteTrigger")]
    AbsoluteTrigger {
        #[serde(rename = "when")]
        when: String,
    },
    /// Catch-all for unrecognized trigger types. Matches any `@type`
    /// value not handled by the other variants.
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum AlertAction {
    #[serde(rename = "display")]
    Display,
    #[serde(rename = "email")]
    Email,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum RelativeTo {
    #[serde(rename = "start")]
    Start,
    #[serde(rename = "end")]
    End,
}

// ---- CalendarEvent/get arguments ----

#[derive(Debug, Clone, Default, Serialize)]
pub struct GetArguments {
    #[serde(rename = "recurrenceOverridesBefore")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recurrence_overrides_before: Option<String>,

    #[serde(rename = "recurrenceOverridesAfter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recurrence_overrides_after: Option<String>,

    #[serde(rename = "reduceParticipants")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_participants: Option<bool>,

    #[serde(rename = "timeZone")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
}

impl GetArguments {
    pub fn recurrence_overrides_before(&mut self, before: impl Into<String>) -> &mut Self {
        self.recurrence_overrides_before = Some(before.into());
        self
    }

    pub fn recurrence_overrides_after(&mut self, after: impl Into<String>) -> &mut Self {
        self.recurrence_overrides_after = Some(after.into());
        self
    }

    pub fn reduce_participants(&mut self, reduce: bool) -> &mut Self {
        self.reduce_participants = Some(reduce);
        self
    }

    pub fn time_zone(&mut self, tz: impl Into<String>) -> &mut Self {
        self.time_zone = Some(tz.into());
        self
    }
}

// ---- CalendarEvent/set arguments ----

#[derive(Debug, Clone, Default, Serialize)]
pub struct SetArguments {
    #[serde(rename = "sendSchedulingMessages")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_scheduling_messages: Option<bool>,
}

impl SetArguments {
    pub fn send_scheduling_messages(&mut self, send: bool) -> &mut Self {
        self.send_scheduling_messages = Some(send);
        self
    }
}

// ---- CalendarEvent/query arguments ----

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryArguments {
    #[serde(rename = "expandRecurrences")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expand_recurrences: Option<bool>,

    #[serde(rename = "timeZone")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
}

impl QueryArguments {
    pub fn expand_recurrences(&mut self, expand: bool) -> &mut Self {
        self.expand_recurrences = Some(expand);
        self
    }

    pub fn time_zone(&mut self, tz: impl Into<String>) -> &mut Self {
        self.time_zone = Some(tz.into());
        self
    }
}

// ---- Property enum ----

// (Lifted-arguments forwarders are emitted after the macro-generated
// CalendarEventGet / CalendarEventSet / CalendarEventQuery structs.)

crate::define_open_property_enum! {
    /// Property names for CalendarEvent/get `properties` lists.
    ///
    /// Common JSCalendar properties have typed variants. Extension or
    /// less-common properties use `Other(String)`.
    #[non_exhaustive]
    pub enum Property {
        Id => "id",
        Uid => "uid",
        CalendarIds => "calendarIds",
        IsDraft => "isDraft",
        Title => "title",
        Description => "description",
        DescriptionContentType => "descriptionContentType",
        Created => "created",
        Updated => "updated",
        Start => "start",
        Duration => "duration",
        TimeZone => "timeZone",
        ShowWithoutTime => "showWithoutTime",
        Status => "status",
        FreeBusyStatus => "freeBusyStatus",
        RecurrenceId => "recurrenceId",
        RecurrenceIdTimeZone => "recurrenceIdTimeZone",
        RecurrenceRules => "recurrenceRules",
        RecurrenceOverrides => "recurrenceOverrides",
        ExcludedRecurrenceRules => "excludedRecurrenceRules",
        Priority => "priority",
        Color => "color",
        Locale => "locale",
        Keywords => "keywords",
        Categories => "categories",
        ProdId => "prodId",
        ReplyTo => "replyTo",
        Participants => "participants",
        UseDefaultAlerts => "useDefaultAlerts",
        Alerts => "alerts",
        Locations => "locations",
        VirtualLocations => "virtualLocations",
        Links => "links",
        RelatedTo => "relatedTo",
        ExcludedDates => "excludedDates",
        Localizations => "localizations",
        Method => "method",
        /// Any JSCalendar property not covered by the typed variants,
        /// including vendor extension properties.
        Sequence => "sequence",
    }
}

// Object, ChangesObject, and SetObject impls generated by json_object_struct! macro.

use crate::{Get, Set};

// Method structs for the new architecture
crate::define_get_method!(
    CalendarEventGet,
    CalendarEvent<Set>,
    "CalendarEvent/get",
    crate::core::capability::Calendars,
    crate::core::get::GetResponse<CalendarEvent<Get>>
);
crate::define_set_method!(
    CalendarEventSet,
    CalendarEvent<Set>,
    "CalendarEvent/set",
    crate::core::capability::Calendars,
    crate::core::set::SetResponse<CalendarEvent<Get>>
);
crate::define_changes_method!(
    CalendarEventChanges,
    "CalendarEvent/changes",
    crate::core::capability::Calendars,
    crate::core::changes::ChangesResponse<CalendarEvent<Get>>
);
crate::define_query_method!(
    CalendarEventQuery,
    CalendarEvent<Set>,
    "CalendarEvent/query",
    crate::core::capability::Calendars
);
crate::define_query_changes_method!(
    CalendarEventQueryChanges,
    CalendarEvent<Set>,
    "CalendarEvent/queryChanges",
    crate::core::capability::Calendars
);
crate::define_copy_method!(
    CalendarEventCopy,
    CalendarEvent<Set>,
    "CalendarEvent/copy",
    crate::core::capability::Calendars,
    crate::core::copy::CopyResponse<CalendarEvent<Get>>
);

// -- Lifted method arguments (plans/API.md §5) --

impl CalendarEventGet {
    #[must_use]
    pub fn recurrence_overrides_before(mut self, before: impl Into<String>) -> Self {
        self.arguments().recurrence_overrides_before(before);
        self
    }

    #[must_use]
    pub fn recurrence_overrides_after(mut self, after: impl Into<String>) -> Self {
        self.arguments().recurrence_overrides_after(after);
        self
    }

    #[must_use]
    pub fn reduce_participants(mut self, reduce: bool) -> Self {
        self.arguments().reduce_participants(reduce);
        self
    }

    #[must_use]
    pub fn time_zone(mut self, tz: impl Into<String>) -> Self {
        self.arguments().time_zone(tz);
        self
    }
}

impl CalendarEventSet {
    #[must_use]
    pub fn send_scheduling_messages(mut self, send: bool) -> Self {
        self.arguments().send_scheduling_messages(send);
        self
    }
}

impl CalendarEventQuery {
    #[must_use]
    pub fn expand_recurrences(mut self, expand: bool) -> Self {
        self.arguments().expand_recurrences(expand);
        self
    }

    #[must_use]
    pub fn time_zone(mut self, tz: impl Into<String>) -> Self {
        self.arguments().time_zone(tz);
        self
    }
}
