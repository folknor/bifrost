//! Calendar and event primitives for the unified PIM surface.
//!
//! Providers expose JSCalendar, Google Calendar JSON, Microsoft Graph
//! events, or iCalendar through CalDAV. These types preserve native ids,
//! etags, recurrence text, and provenance while keeping the shared
//! Account surface small.

use crate::cursor::ProtocolKind;

/// Engine-facing calendar identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CalendarId(pub String);

/// Engine-facing event identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventId(pub String);

/// Wire-level provenance for a calendar or event id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CalendarProvenance {
    /// Which protocol family minted the native id.
    pub provider: ProtocolKind,
    /// Native id string used by the provider on the wire.
    pub native: String,
    /// Native calendar id, when the provider scopes events under a
    /// calendar collection.
    pub calendar_native: Option<String>,
}

/// Calendar collection exposed by a provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Calendar {
    pub id: CalendarId,
    pub native_id: String,
    pub name: String,
    pub color: Option<String>,
    pub provenance: CalendarProvenance,
    pub is_default: bool,
    pub can_create_events: bool,
    pub can_update_events: bool,
    pub can_delete_events: bool,
}

/// Event transparency / availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventAvailability {
    Busy,
    Free,
    Tentative,
    OutOfOffice,
    Unknown,
}

/// Event visibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventVisibility {
    Default,
    Public,
    Private,
    Confidential,
}

/// Event lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventStatus {
    Confirmed,
    Tentative,
    Cancelled,
    Unknown,
}

/// Attendee participation status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RsvpStatus {
    NeedsAction,
    Accepted,
    Declined,
    Tentative,
    Delegated,
    Unknown,
}

/// Attendee role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AttendeeRole {
    Required,
    Optional,
    Resource,
    Chair,
    Unknown,
}

/// Event timestamp represented as an RFC 3339 date-time string, or a
/// provider-local all-day date in `YYYY-MM-DD` form when `is_all_day`
/// is true.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTime {
    pub value: String,
    pub timezone: Option<String>,
}

/// Event attendee.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventAttendee {
    pub email: String,
    pub name: Option<String>,
    pub role: AttendeeRole,
    pub status: RsvpStatus,
}

/// Event organizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventOrganizer {
    pub email: String,
    pub name: Option<String>,
}

/// Canonical recurrence payload. `rrule`, `rdate`, and `exdate` use
/// RFC 5545 text forms. `recurrence_id` identifies an overridden
/// occurrence using the provider's canonical string for that instance.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventRecurrence {
    pub rrule: Option<String>,
    pub rdate: Vec<String>,
    pub exdate: Vec<String>,
    pub recurrence_id: Option<String>,
}

/// Unified calendar event returned by calendar primitives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalendarEvent {
    pub id: EventId,
    pub calendar_id: CalendarId,
    pub native_id: String,
    pub uid: Option<String>,
    pub etag: Option<String>,
    pub provenance: CalendarProvenance,
    pub title: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    pub start: EventTime,
    pub end: EventTime,
    pub is_all_day: bool,
    pub status: EventStatus,
    pub availability: EventAvailability,
    pub visibility: EventVisibility,
    /// The opened account user's participation status when the provider
    /// exposes it independently from attendee rows.
    pub self_response: RsvpStatus,
    pub organizer: Option<EventOrganizer>,
    pub attendees: Vec<EventAttendee>,
    pub recurrence: EventRecurrence,
    pub html_link: Option<String>,
    pub raw_ical: Option<String>,
}

/// Provider-side event range query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRange {
    pub calendar_id: CalendarId,
    pub start: EventTime,
    pub end: EventTime,
    pub page_cursor: Option<Vec<u8>>,
    pub limit: Option<u32>,
}

/// Event creation payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCreate {
    pub calendar_id: CalendarId,
    pub title: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    pub start: EventTime,
    pub end: EventTime,
    pub is_all_day: bool,
    pub status: EventStatus,
    pub availability: EventAvailability,
    pub visibility: EventVisibility,
    pub organizer: Option<EventOrganizer>,
    pub attendees: Vec<EventAttendee>,
    pub recurrence: EventRecurrence,
}

/// Partial event update payload.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventPatch {
    pub calendar_id: Option<CalendarId>,
    pub title: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub location: Option<Option<String>>,
    pub start: Option<EventTime>,
    pub end: Option<EventTime>,
    pub is_all_day: Option<bool>,
    pub status: Option<EventStatus>,
    pub availability: Option<EventAvailability>,
    pub visibility: Option<EventVisibility>,
    pub attendees: Option<Vec<EventAttendee>>,
    pub recurrence: Option<EventRecurrence>,
}

/// Provider-side event search request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSearchRequest {
    pub query: String,
    pub calendar_id: Option<CalendarId>,
    pub page_cursor: Option<Vec<u8>>,
    pub limit: Option<u32>,
}

impl EventSearchRequest {
    #[must_use]
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            calendar_id: None,
            page_cursor: None,
            limit: None,
        }
    }
}
