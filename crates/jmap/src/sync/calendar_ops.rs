use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AttendeeRole, Calendar, CalendarEvent,
    CalendarId, CalendarProvenance, DiagnosticText, EventAttendee, EventAvailability, EventCreate,
    EventId, EventOrganizer, EventPatch, EventRange, EventRecurrence, EventReminder,
    EventSearchRequest, EventStatus, EventTime, EventVisibility, Page, ProtocolKind,
    ReminderRelativeTo, ReminderTrigger, RsvpStatus,
};
use jiff::tz::{Offset, TimeZone};
use jiff::{SignedDuration, Span, Timestamp, civil};
use serde_json::{Map, Value, json};

use crate::account::Account as JmapProtoAccount;
use crate::calendar::{
    Calendar as JmapCalendar, CalendarGet, CalendarId as JmapCalendarId, CalendarRights,
};
use crate::calendar_event::query::Filter as EventFilter;
use crate::calendar_event::{
    CalendarEvent as JmapCalendarEvent, CalendarEventCreate, CalendarEventGet, CalendarEventId,
    CalendarEventPatch, CalendarEventSet,
};
use crate::core::SetCreate;
use crate::core::query::Filter as QueryFilter;
use crate::core::transport::HttpTransport;

type CalendarAccount<T> = JmapProtoAccount<T>;

const PAGE_LIMIT: usize = 250;

pub(crate) fn calendars_list<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::CalendarsList)?;
        let response = calendars
            .call(CalendarGet::new())
            .await
            .map_err(to_acct_err(AccountOperation::CalendarsList))?;
        Ok(response
            .into_list()
            .into_iter()
            .map(calendar_from_jmap)
            .collect())
    })
}

pub(crate) fn events_in_range<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    range: EventRange,
) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventsInRange)?;
        let position = decode_position(range.page_cursor.clone(), AccountOperation::EventsInRange)?;
        let filter = range_filter(&range);
        let query = crate::calendar_event::CalendarEventQuery::new()
            .filter(filter)
            .position(position)
            .limit(limit(range.limit))
            .calculate_total(true);
        let response = calendars
            .call(query)
            .await
            .map_err(to_acct_err(AccountOperation::EventsInRange))?;
        let total = response
            .total()
            .map(|total| usize_to_u64(total, AccountOperation::EventsInRange))
            .transpose()?;
        let next_cursor = next_cursor(response.position(), response.ids().len(), response.total());
        let events = get_events(
            &calendars,
            response.into_ids(),
            AccountOperation::EventsInRange,
        )
        .await?
        .into_iter()
        .filter(|event| event_in_range(event, &range.start, &range.end))
        .collect::<Vec<_>>();
        Ok(Page {
            items: events,
            next_cursor,
            estimated_total: total,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

fn range_filter(range: &EventRange) -> QueryFilter<EventFilter> {
    QueryFilter::and(vec![
        QueryFilter::from(EventFilter::in_calendar(range.calendar_id.0.clone())),
        QueryFilter::from(EventFilter::after(jmap_utc_filter_time(&range.start))),
        QueryFilter::from(EventFilter::before(jmap_utc_filter_time(&range.end))),
    ])
}

pub(crate) fn get<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    event: EventId,
) -> AccountFuture<Result<CalendarEvent, AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventGet)?;
        let events = get_events(
            &calendars,
            vec![CalendarEventId::new(event.0)],
            AccountOperation::EventGet,
        )
        .await?;
        events.into_iter().next().ok_or_else(|| {
            unsupported(
                AccountOperation::EventGet,
                "JMAP CalendarEvent/get returned no event",
            )
        })
    })
}

pub(crate) fn create<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    event: EventCreate,
) -> AccountFuture<Result<EventId, AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventCreate)?;
        validate_shared_recurrence(
            &event.recurrence,
            Some(&event.start),
            event.is_all_day,
            AccountOperation::EventCreate,
        )?;
        validate_shared_attendees(&event.attendees, AccountOperation::EventCreate)?;
        let mut set = CalendarEventSet::new();
        let create_id = set.create_item(jmap_create_from_event(
            &event,
            AccountOperation::EventCreate,
        )?);
        let mut response = calendars
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::EventCreate))?;
        let event = response
            .created(&create_id)
            .map_err(to_acct_err(AccountOperation::EventCreate))?;
        let id = event
            .id()
            .map(CalendarEventId::into_string)
            .unwrap_or_default();
        Ok(EventId(id))
    })
}

pub(crate) fn update<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    event: EventId,
    patch: EventPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventUpdate)?;
        let id = CalendarEventId::new(event.0);
        // A patch that changes only the recurrence rule carries neither the
        // event start nor its all-day flag, yet `UNTIL` conversion and the
        // start/duration write both need them. Read the current server state
        // for exactly the fields the patch leaves unset rather than defaulting
        // to a timed, floating event and mangling the value. An attendee patch
        // needs the read too: JSCalendar `participants` holds the owner
        // participant beside the attendees, so writing the attendee list as a
        // whole-map replacement without carrying the owner over would silently
        // delete the organizer from the event.
        let context = if event_patch_needs_current(&patch) {
            let raw = get_raw_event(&calendars, id.clone(), AccountOperation::EventUpdate).await?;
            event_context_from_patch(&patch, Some(&raw))
        } else {
            event_context_from_patch(&patch, None)
        };
        if let Some(recurrence) = &patch.recurrence {
            validate_shared_recurrence(
                recurrence,
                context.start.as_ref(),
                context.is_all_day,
                AccountOperation::EventUpdate,
            )?;
        }
        if let Some(attendees) = &patch.attendees {
            validate_shared_attendees(attendees, AccountOperation::EventUpdate)?;
        }
        let jmap_patch =
            jmap_patch_from_event_patch(&patch, &context, AccountOperation::EventUpdate)?;
        let mut set = CalendarEventSet::new();
        set.update_item(id.clone(), jmap_patch);
        let mut response = calendars
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::EventUpdate))?;
        response
            .updated(&id)
            .map_err(to_acct_err(AccountOperation::EventUpdate))?;
        Ok(())
    })
}

pub(crate) fn delete<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    event: EventId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventDelete)?;
        let id = CalendarEventId::new(event.0);
        let set = CalendarEventSet::new().destroy([id.clone()]);
        let mut response = calendars
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::EventDelete))?;
        response
            .destroyed(&id)
            .map_err(to_acct_err(AccountOperation::EventDelete))
    })
}

pub(crate) fn rsvp<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    self_emails: Vec<String>,
    event: EventId,
    status: RsvpStatus,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventRsvp)?;
        let id = CalendarEventId::new(event.0);
        let current = get_raw_event(&calendars, id.clone(), AccountOperation::EventRsvp).await?;
        let patch = jmap_rsvp_patch_from_participants(current.participants(), &self_emails, status)
            .map_err(|message| unsupported(AccountOperation::EventRsvp, message))?;
        let mut set = CalendarEventSet::new();
        set.update_item(id.clone(), patch);
        let mut response = calendars
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::EventRsvp))?;
        response
            .updated(&id)
            .map_err(to_acct_err(AccountOperation::EventRsvp))?;
        Ok(())
    })
}

pub(crate) fn search<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    request: EventSearchRequest,
) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
    Box::pin(async move {
        let calendars = require_calendars(calendars, AccountOperation::EventSearch)?;
        let position = decode_position(request.page_cursor, AccountOperation::EventSearch)?;
        let query = crate::calendar_event::CalendarEventQuery::new()
            .filter(EventFilter::text(request.query.clone()))
            .position(position)
            .limit(limit(request.limit))
            .calculate_total(true);
        let response = calendars
            .call(query)
            .await
            .map_err(to_acct_err(AccountOperation::EventSearch))?;
        let total = response
            .total()
            .map(|total| usize_to_u64(total, AccountOperation::EventSearch))
            .transpose()?;
        let next_cursor = next_cursor(response.position(), response.ids().len(), response.total());
        let events = get_events(
            &calendars,
            response.into_ids(),
            AccountOperation::EventSearch,
        )
        .await?
        .into_iter()
        .filter(|event| {
            request
                .calendar_id
                .as_ref()
                .is_none_or(|calendar| event.calendar_id == *calendar)
        })
        .collect();
        Ok(Page {
            items: events,
            next_cursor,
            estimated_total: total,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

async fn get_events<T: HttpTransport>(
    calendars: &CalendarAccount<T>,
    ids: Vec<CalendarEventId>,
    operation: AccountOperation,
) -> Result<Vec<CalendarEvent>, AccountError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let response = calendars
        .call(CalendarEventGet::new().ids(ids))
        .await
        .map_err(to_acct_err(operation))?;
    response
        .into_list()
        .into_iter()
        .map(|event| event_from_jmap(event, operation))
        .collect()
}

async fn get_raw_event<T: HttpTransport>(
    calendars: &CalendarAccount<T>,
    id: CalendarEventId,
    operation: AccountOperation,
) -> Result<JmapCalendarEvent, AccountError> {
    let response = calendars
        .call(CalendarEventGet::new().ids([id]))
        .await
        .map_err(to_acct_err(operation))?;
    response
        .into_list()
        .into_iter()
        .next()
        .ok_or_else(|| unsupported(operation, "JMAP CalendarEvent/get returned no event"))
}

fn calendar_from_jmap(calendar: JmapCalendar) -> Calendar {
    let native = calendar
        .id()
        .cloned()
        .map(JmapCalendarId::into_string)
        .unwrap_or_default();
    let rights = calendar.my_rights();
    let can_write = rights_can_write(rights);
    Calendar {
        id: CalendarId(native.clone()),
        native_id: native.clone(),
        name: calendar.name().unwrap_or("Calendar").to_string(),
        color: calendar.color().map(ToString::to_string),
        provenance: CalendarProvenance {
            provider: ProtocolKind::Jmap,
            native,
            calendar_native: None,
        },
        is_default: calendar.is_default().unwrap_or(false),
        can_create_events: can_write,
        can_update_events: can_write,
        can_delete_events: rights_can_delete(rights),
    }
}

fn event_from_jmap(
    event: JmapCalendarEvent,
    operation: AccountOperation,
) -> Result<CalendarEvent, AccountError> {
    let native = event
        .id()
        .map(CalendarEventId::into_string)
        .unwrap_or_default();
    let calendar_id = event
        .calendar_ids()
        .and_then(|ids| ids.keys().next().cloned())
        .map(CalendarId)
        .unwrap_or_else(|| CalendarId(String::new()));
    let is_all_day = event.show_without_time().unwrap_or(false);
    let raw_start = event.start().unwrap_or_default();
    let raw_end =
        end_from_start_duration(raw_start, event.duration().unwrap_or_default(), is_all_day);
    let timezone = event.time_zone().as_value().map(ToString::to_string);
    let start = EventTime {
        value: shared_time_from_jmap(raw_start, is_all_day),
        timezone: timezone.clone(),
    };
    let end = EventTime {
        value: shared_time_from_jmap(&raw_end, is_all_day),
        timezone: timezone.clone(),
    };
    let (rdate, mut exdate) = recurrence_dates_from_overrides(event.recurrence_overrides())
        .map_err(|message| unsupported(operation, message))?;
    if let Some(dates) = event.excluded_dates() {
        exdate.extend(dates.keys().cloned());
    }
    let alerts = event.alerts();
    let reminders = reminders_from_alerts(alerts.as_value().copied());
    let recurrence = recurrence_from_jmap(&event, raw_start, is_all_day, timezone.as_deref())
        .map_err(|message| unsupported(operation, message))?;
    let attendees =
        attendees(event.participants()).map_err(|message| unsupported(operation, message))?;
    Ok(CalendarEvent {
        id: EventId(native.clone()),
        calendar_id: calendar_id.clone(),
        native_id: native.clone(),
        uid: event.uid().map(ToString::to_string),
        etag: None,
        provenance: CalendarProvenance {
            provider: ProtocolKind::Jmap,
            native,
            calendar_native: Some(calendar_id.0),
        },
        title: event.title().map(ToString::to_string),
        description: event.description().map(ToString::to_string),
        location: first_location(event.locations()),
        start,
        end,
        is_all_day,
        status: event_status(event.status()),
        availability: availability(event.free_busy_status()),
        visibility: visibility(event.privacy()),
        self_response: RsvpStatus::Unknown,
        organizer: organizer(event.participants()),
        attendees,
        reminders,
        recurrence: EventRecurrence {
            rdate,
            exdate,
            ..recurrence
        },
        html_link: None,
        raw_ical: None,
    })
}

fn recurrence_from_jmap(
    event: &JmapCalendarEvent,
    start: &str,
    is_all_day: bool,
    timezone: Option<&str>,
) -> Result<EventRecurrence, &'static str> {
    if event
        .excluded_recurrence_rules()
        .is_some_and(|rules| !rules.is_empty())
    {
        return Err(
            "JMAP excludedRecurrenceRules cannot be represented by the shared recurrence model",
        );
    }
    let rules = event.recurrence_rules().map(Vec::as_slice).unwrap_or(&[]);
    if rules.len() > 1 {
        return Err(
            "multiple JMAP recurrenceRules cannot be represented by the shared recurrence model",
        );
    }
    let rrule = rules
        .first()
        .map(|rule| rrule_from_jmap_recurrence_rule(rule, start, is_all_day, timezone))
        .transpose()?
        .flatten();
    Ok(EventRecurrence {
        rrule,
        recurrence_id: event.recurrence_id().map(ToString::to_string),
        ..EventRecurrence::default()
    })
}

fn jmap_create_from_event(
    event: &EventCreate,
    operation: AccountOperation,
) -> Result<CalendarEventCreate, AccountError> {
    let mut create = CalendarEventCreate::new(None);
    write_event_create(&mut create, event, operation)?;
    Ok(create)
}

/// The event start and all-day flag that JSCalendar conversion needs but an
/// `EventPatch` is not required to carry, plus the owner participants an
/// attendee patch must preserve.
#[derive(Debug, Default, Clone)]
struct EventContext {
    start: Option<EventTime>,
    is_all_day: bool,
    /// The current event's owner-roled `participants` entries, keyed as the
    /// server stores them. `EventPatch.attendees` describes only attendees,
    /// but JSCalendar keeps the organizer in the same `participants` map, so
    /// a whole-map attendee write must carry these over or it deletes the
    /// organizer. Empty when the patch has no attendees or no read was made.
    owner_participants: Map<String, Value>,
}

fn rrule_has_until(patch: &EventPatch) -> bool {
    patch
        .recurrence
        .as_ref()
        .and_then(|recurrence| recurrence.rrule.as_deref())
        .is_some_and(|rrule| {
            rrule.split(';').any(|part| {
                part.split_once('=')
                    .is_some_and(|(key, _)| key.eq_ignore_ascii_case("UNTIL"))
            })
        })
}

/// True when the patch cannot supply the conversion context itself. `UNTIL`
/// needs the start's timezone; any start/end or `UNTIL` write needs to know
/// whether the event is all-day, and a patch that does not restate
/// `is_all_day` is not asserting that the event became timed.
fn event_patch_needs_context(patch: &EventPatch) -> bool {
    let until = rrule_has_until(patch);
    (until && patch.start.is_none())
        || (patch.is_all_day.is_none() && (until || patch.start.is_some() || patch.end.is_some()))
}

/// Whether `update` must read the current event before building its patch:
/// either the conversion context is missing (`event_patch_needs_context`),
/// or the patch writes the attendee list, whose whole-map write must carry
/// the current owner participants over.
fn event_patch_needs_current(patch: &EventPatch) -> bool {
    event_patch_needs_context(patch) || patch.attendees.is_some()
}

fn event_context_from_patch(
    patch: &EventPatch,
    current: Option<&JmapCalendarEvent>,
) -> EventContext {
    let is_all_day = patch
        .is_all_day
        .or_else(|| current.map(|event| event.show_without_time().unwrap_or(false)))
        .unwrap_or(false);
    let start = patch.start.clone().or_else(|| {
        current.map(|event| EventTime {
            value: shared_time_from_jmap(event.start().unwrap_or_default(), is_all_day),
            timezone: event.time_zone().as_value().map(ToString::to_string),
        })
    });
    let owner_participants = current
        .and_then(JmapCalendarEvent::participants)
        .into_iter()
        .flat_map(Map::iter)
        .filter(|(_, value)| {
            value
                .as_object()
                .and_then(|object| object.get("roles"))
                .and_then(Value::as_object)
                .is_some_and(|roles| role_enabled(roles, "owner"))
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    EventContext {
        start,
        is_all_day,
        owner_participants,
    }
}

fn jmap_patch_from_event_patch(
    patch: &EventPatch,
    context: &EventContext,
    operation: AccountOperation,
) -> Result<CalendarEventPatch, AccountError> {
    let mut out = CalendarEventPatch::default();
    if let Some(calendar) = &patch.calendar_id {
        out.calendar_ids([calendar.0.clone()]);
    }
    if let Some(title) = &patch.title {
        match title {
            Some(title) => {
                out.title(title.clone());
            }
            None => {
                out.set_property("title", Value::Null);
            }
        }
    }
    if let Some(description) = &patch.description {
        match description {
            Some(description) => {
                out.description(description.clone());
            }
            None => {
                out.set_property("description", Value::Null);
            }
        }
    }
    if let Some(location) = &patch.location {
        match location {
            Some(location) => {
                out.locations(single_location(location));
            }
            None => {
                out.set_property("locations", Value::Null);
            }
        }
    }
    // JSCalendar models the end as `start` + `duration`; there is no
    // standalone end property. Recomputing one bound requires the other, so a
    // patch carrying only `start` or only `end` cannot be applied losslessly
    // without reading the current event. Reject rather than silently keep a
    // stale duration (start-only) or drop the change entirely (end-only).
    match (&patch.start, &patch.end) {
        (Some(start), Some(end)) => {
            let all_day = context.is_all_day;
            out.start(jmap_time_from_shared(start, all_day));
            out.time_zone(start.timezone.clone());
            out.duration(duration(&start.value, &end.value));
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(unsupported(
                operation,
                "JMAP event time patches must set both start and end; JSCalendar derives the end from start + duration",
            ));
        }
        (None, None) => {}
    }
    if let Some(is_all_day) = patch.is_all_day {
        out.show_without_time(is_all_day);
    }
    if let Some(status) = patch.status {
        out.status(jmap_event_status(status));
    }
    if let Some(availability) = patch.availability {
        out.free_busy_status(jmap_availability(availability));
    }
    if let Some(visibility) = patch.visibility {
        out.set_property("privacy", json!(jmap_visibility(visibility)));
    }
    if let Some(attendees) = &patch.attendees {
        out.participants(merge_owner_participants(
            &context.owner_participants,
            attendees,
        ));
    }
    if let Some(recurrence) = &patch.recurrence {
        match recurrence.rrule.as_deref() {
            // No rule in the shared recurrence means "no recurrence rule":
            // clear whatever the server holds.
            None => {
                out.set_property("recurrenceRules", Value::Null);
            }
            // A rule that fails conversion is an error, never a clear. The
            // callers validate first, but the builder must not depend on
            // that call order: a future caller that skipped validation
            // would otherwise silently erase the event's recurrence.
            Some(rrule) => {
                let rule = jmap_recurrence_rule_from_rrule(
                    rrule,
                    context.start.as_ref(),
                    context.is_all_day,
                )
                .ok_or_else(|| {
                    unsupported(
                        operation,
                        "JMAP shared recurrence contains RRULE fields unsupported by the JSCalendar mapper",
                    )
                })?;
                out.recurrence_rules(vec![rule]);
            }
        }
        let overrides = recurrence_overrides_from_shared(recurrence);
        if overrides.is_empty() {
            out.set_property("recurrenceOverrides", Value::Null);
        } else {
            out.recurrence_overrides(overrides);
        }
    }
    Ok(out)
}

/// Merge a whole-list attendee write with the owner participants the
/// current event holds.
///
/// JSCalendar keeps the organizer in the same `participants` map as the
/// attendees, so replacing the map with only the new attendee entries
/// would silently delete the organizer. The owner entries are carried
/// over under their existing keys. An attendee whose email matches an
/// owner participant updates that entry's name and participation status
/// in place (its owner role is kept - the read path surfaces an owner as
/// a `Chair` attendee, so a read-modify-write round trip stays stable);
/// the rest get fresh keys that never collide with a kept one.
fn merge_owner_participants(
    owner_participants: &Map<String, Value>,
    attendees: &[EventAttendee],
) -> Map<String, Value> {
    let mut participants = owner_participants.clone();
    let mut next_key = 0_usize;
    for attendee in attendees {
        let owner_key = participants.iter().find_map(|(key, value)| {
            let email = value.as_object()?.get("email")?.as_str()?;
            email
                .eq_ignore_ascii_case(&attendee.email)
                .then(|| key.clone())
        });
        if let Some(owner_key) = owner_key {
            if let Some(Value::Object(entry)) = participants.get_mut(&owner_key) {
                match &attendee.name {
                    Some(name) => {
                        entry.insert("name".to_string(), json!(name));
                    }
                    None => {
                        entry.remove("name");
                    }
                }
                entry.insert(
                    "participationStatus".to_string(),
                    json!(rsvp_value(attendee.status)),
                );
            }
            continue;
        }
        let mut key = format!("p{next_key}");
        while participants.contains_key(&key) {
            next_key += 1;
            key = format!("p{next_key}");
        }
        next_key += 1;
        let entry = participants_from_attendees(std::slice::from_ref(attendee))
            .into_iter()
            .next()
            .map(|(_, value)| value)
            .unwrap_or(Value::Null);
        participants.insert(key, entry);
    }
    participants
}

fn jmap_rsvp_patch_from_participants(
    participants: Option<&Map<String, Value>>,
    self_emails: &[String],
    status: RsvpStatus,
) -> Result<CalendarEventPatch, &'static str> {
    let participant_id = rsvp_participant_id(participants, self_emails)?;
    let mut patch = CalendarEventPatch::default();
    patch.set_property(
        format!("participants/{participant_id}/participationStatus"),
        json!(rsvp_value(status)),
    );
    Ok(patch)
}

fn rsvp_participant_id<'a>(
    participants: Option<&'a Map<String, Value>>,
    self_emails: &[String],
) -> Result<&'a str, &'static str> {
    let Some(participants) = participants else {
        return Err("JMAP event has no participants to RSVP");
    };
    let candidates = participants
        .iter()
        .filter_map(|(id, value)| {
            let object = value.as_object()?;
            let has_email = object.get("email").and_then(Value::as_str).is_some();
            let is_owner = object
                .get("roles")
                .and_then(Value::as_object)
                .is_some_and(|roles| role_enabled(roles, "owner"));
            (has_email && !is_owner).then_some((id.as_str(), object))
        })
        .collect::<Vec<_>>();
    if !self_emails.is_empty() {
        let matches = candidates
            .iter()
            .filter_map(|(id, object)| {
                let email = object.get("email")?.as_str()?;
                self_emails
                    .iter()
                    .any(|self_email| email.eq_ignore_ascii_case(self_email))
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [id] => return Ok(id),
            [] => {}
            _ => {
                return Err("JMAP event has multiple participants matching the authenticated user");
            }
        }
    }
    match candidates.as_slice() {
        [(id, _)] => Ok(id),
        [] => Err("JMAP event has no attendee participant to RSVP"),
        _ => Err(
            "JMAP event has multiple attendee participants; authenticated attendee lookup is unavailable",
        ),
    }
}

fn write_event_create(
    target: &mut CalendarEventCreate,
    event: &EventCreate,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    target.set_property("@type", json!("Event"));
    target.calendar_ids([event.calendar_id.0.clone()]);
    if let Some(title) = &event.title {
        target.title(title.clone());
    }
    if let Some(description) = &event.description {
        target.description(description.clone());
    }
    if let Some(location) = &event.location {
        target.locations(single_location(location));
    }
    target.start(jmap_time_from_shared(&event.start, event.is_all_day));
    target.time_zone(event.start.timezone.clone());
    target.duration(duration(&event.start.value, &event.end.value));
    target.show_without_time(event.is_all_day);
    target.status(jmap_event_status(event.status));
    target.free_busy_status(jmap_availability(event.availability));
    target.set_property("privacy", json!(jmap_visibility(event.visibility)));
    let participants = participants_from_event_create(event);
    if !participants.is_empty() {
        target.participants(participants);
    }
    // A rule that fails conversion is an error, never a silent omission.
    // `create` validates first, but the builder must not depend on that
    // call order: a future caller that skipped validation would otherwise
    // create the event with its recurrence quietly dropped.
    if let Some(rrule) = event.recurrence.rrule.as_deref() {
        let rule = jmap_recurrence_rule_from_rrule(rrule, Some(&event.start), event.is_all_day)
            .ok_or_else(|| {
                unsupported(
                    operation,
                    "JMAP shared recurrence contains RRULE fields unsupported by the JSCalendar mapper",
                )
            })?;
        target.recurrence_rules(vec![rule]);
    }
    let overrides = recurrence_overrides_from_shared(&event.recurrence);
    if !overrides.is_empty() {
        target.recurrence_overrides(overrides);
    }
    Ok(())
}

fn recurrence_overrides_from_shared(recurrence: &EventRecurrence) -> Map<String, Value> {
    let mut overrides = Map::new();
    for date in &recurrence.rdate {
        overrides.insert(date.clone(), Value::Object(Map::new()));
    }
    for date in &recurrence.exdate {
        overrides.insert(date.clone(), json!({ "excluded": true }));
    }
    overrides
}

fn recurrence_dates_from_overrides(
    overrides: Option<&Map<String, Value>>,
) -> Result<(Vec<String>, Vec<String>), &'static str> {
    let mut rdate = Vec::new();
    let mut exdate = Vec::new();
    let Some(overrides) = overrides else {
        return Ok((rdate, exdate));
    };
    for (date, value) in overrides {
        let Some(object) = value.as_object() else {
            return Err("JMAP recurrenceOverrides entry is not an object");
        };
        // The shared model has only RDATE and EXDATE. An override that merely
        // excludes an occurrence is an EXDATE and one that adds an unmodified
        // occurrence is an RDATE; anything that patches the occurrence itself
        // has no shared representation, and dropping it would hand back the
        // master event as though the modified occurrence did not exist.
        match object.get("excluded").and_then(Value::as_bool) {
            Some(true) => exdate.push(date.clone()),
            _ if object.keys().all(|key| key == "excluded") => rdate.push(date.clone()),
            _ => {
                return Err(
                    "a modified JMAP recurrence override cannot be represented by the shared recurrence model",
                );
            }
        }
    }
    Ok((rdate, exdate))
}

fn validate_shared_recurrence(
    recurrence: &EventRecurrence,
    start: Option<&EventTime>,
    is_all_day: bool,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if recurrence
        .rrule
        .as_deref()
        .is_some_and(|rrule| jmap_recurrence_rule_from_rrule(rrule, start, is_all_day).is_none())
    {
        return Err(unsupported(
            operation,
            "JMAP shared recurrence contains RRULE fields unsupported by the JSCalendar mapper",
        ));
    }
    Ok(())
}

fn jmap_recurrence_rule_from_rrule(
    rrule: &str,
    start: Option<&EventTime>,
    is_all_day: bool,
) -> Option<Value> {
    let mut object = Map::new();
    object.insert("@type".to_string(), json!("RecurrenceRule"));
    for part in rrule.split(';') {
        let (key, value) = part.split_once('=')?;
        match key.to_ascii_uppercase().as_str() {
            "FREQ" => {
                let frequency = value.to_ascii_lowercase();
                if ![
                    "yearly", "monthly", "weekly", "daily", "hourly", "minutely", "secondly",
                ]
                .contains(&frequency.as_str())
                {
                    return None;
                }
                object.insert("frequency".to_string(), Value::String(frequency));
            }
            "INTERVAL" => {
                object.insert("interval".to_string(), json!(value.parse::<u64>().ok()?));
            }
            "COUNT" => {
                object.insert("count".to_string(), json!(value.parse::<u64>().ok()?));
            }
            "UNTIL" => {
                object.insert(
                    "until".to_string(),
                    Value::String(jmap_until_from_ical(value, start, is_all_day)?),
                );
            }
            "BYDAY" => {
                object.insert("byDay".to_string(), Value::Array(jmap_by_day(value)?));
            }
            "BYMONTH" => {
                object.insert("byMonth".to_string(), Value::Array(integer_list(value)?));
            }
            "BYMONTHDAY" => {
                object.insert("byMonthDay".to_string(), Value::Array(integer_list(value)?));
            }
            _ => return None,
        }
    }
    object
        .contains_key("frequency")
        .then_some(Value::Object(object))
}

fn rrule_from_jmap_recurrence_rule(
    rule: &Value,
    start: &str,
    is_all_day: bool,
    timezone: Option<&str>,
) -> Result<Option<String>, &'static str> {
    let object = rule
        .as_object()
        .ok_or("JMAP recurrence rule is not an object")?;
    const SUPPORTED: &[&str] = &[
        "@type",
        "frequency",
        "interval",
        "count",
        "until",
        "byDay",
        "byMonth",
        "byMonthDay",
    ];
    if object.keys().any(|key| !SUPPORTED.contains(&key.as_str())) {
        return Err("JMAP recurrence rule contains an unsupported component");
    }
    let mut parts = Vec::new();
    let frequency = object
        .get("frequency")
        .and_then(Value::as_str)
        .ok_or("JMAP recurrence frequency is missing or invalid")?;
    if ![
        "yearly", "monthly", "weekly", "daily", "hourly", "minutely", "secondly",
    ]
    .contains(&frequency)
    {
        return Err("JMAP recurrence frequency is unsupported");
    }
    parts.push(format!("FREQ={}", frequency.to_ascii_uppercase()));
    if let Some(interval) = object.get("interval") {
        parts.push(format!(
            "INTERVAL={}",
            interval
                .as_u64()
                .ok_or("JMAP recurrence interval is invalid")?
        ));
    }
    if let Some(count) = object.get("count") {
        parts.push(format!(
            "COUNT={}",
            count.as_u64().ok_or("JMAP recurrence count is invalid")?
        ));
    }
    if object.contains_key("count") && object.contains_key("until") {
        return Err("JMAP recurrence rule contains both count and until");
    }
    if let Some(until) = object.get("until") {
        parts.push(format!(
            "UNTIL={}",
            ical_until_from_jmap(
                until
                    .as_str()
                    .ok_or("JMAP recurrence until is not a string")?,
                start,
                is_all_day,
                timezone,
            )?
        ));
    }
    if let Some(by_day) = object.get("byDay") {
        parts.push(format!(
            "BYDAY={}",
            rrule_by_day(by_day).ok_or("JMAP recurrence byDay is invalid")?
        ));
    }
    if let Some(by_month) = object.get("byMonth") {
        parts.push(format!(
            "BYMONTH={}",
            rrule_integer_list(by_month).ok_or("JMAP recurrence byMonth is invalid")?
        ));
    }
    if let Some(by_month_day) = object.get("byMonthDay") {
        parts.push(format!(
            "BYMONTHDAY={}",
            rrule_integer_list(by_month_day).ok_or("JMAP recurrence byMonthDay is invalid")?
        ));
    }
    Ok(Some(parts.join(";")))
}

fn jmap_until_from_ical(
    value: &str,
    start: Option<&EventTime>,
    is_all_day: bool,
) -> Option<String> {
    if is_all_day {
        let date = civil::Date::strptime("%Y%m%d", value).ok()?;
        return Some(date.strftime("%Y-%m-%dT00:00:00").to_string());
    }
    let raw = value.trim_end_matches(['Z', 'z']);
    let datetime = civil::DateTime::strptime("%Y%m%dT%H%M%S", raw).ok()?;
    if value.ends_with(['Z', 'z']) {
        let start = start?;
        let timezone = TimeZone::get(start.timezone.as_deref()?).ok()?;
        let instant = Offset::UTC.to_timestamp(datetime).ok()?;
        return Some(
            timezone
                .to_datetime(instant)
                .strftime("%Y-%m-%dT%H:%M:%S")
                .to_string(),
        );
    }
    start.filter(|start| start.timezone.is_none())?;
    Some(datetime.strftime("%Y-%m-%dT%H:%M:%S").to_string())
}

fn ical_until_from_jmap(
    value: &str,
    start: &str,
    is_all_day: bool,
    timezone: Option<&str>,
) -> Result<String, &'static str> {
    let until = civil::DateTime::strptime("%Y-%m-%dT%H:%M:%S", value)
        .map_err(|_| "JMAP recurrence until is not a LocalDateTime")?;
    if is_all_day || start.len() == 10 {
        if until.time() != civil::Time::MIN {
            return Err("an all-day JMAP recurrence until must be midnight");
        }
        return Ok(until.date().strftime("%Y%m%d").to_string());
    }
    if let Some(timezone) = timezone {
        let timezone = TimeZone::get(timezone)
            .map_err(|_| "JMAP event timeZone is unknown; recurrence until cannot be normalized")?;
        let instant = timezone
            .to_timestamp(until)
            .map_err(|_| "JMAP recurrence until is ambiguous or invalid in the event timeZone")?;
        return Ok(Offset::UTC
            .to_datetime(instant)
            .strftime("%Y%m%dT%H%M%SZ")
            .to_string());
    }
    Ok(until.strftime("%Y%m%dT%H%M%S").to_string())
}

fn jmap_by_day(value: &str) -> Option<Vec<Value>> {
    value
        .split(',')
        .map(|day| {
            let (nth, day) = split_by_day(day)?;
            let mut object = Map::new();
            object.insert("@type".to_string(), json!("NDay"));
            object.insert("day".to_string(), Value::String(day.to_ascii_lowercase()));
            if let Some(nth) = nth {
                object.insert("nthOfPeriod".to_string(), json!(nth));
            }
            Some(Value::Object(object))
        })
        .collect()
}

fn split_by_day(value: &str) -> Option<(Option<i64>, &str)> {
    if value.len() == 2 {
        return ["MO", "TU", "WE", "TH", "FR", "SA", "SU"]
            .contains(&value.to_ascii_uppercase().as_str())
            .then_some((None, value));
    }
    let split = value.len().checked_sub(2)?;
    let nth = value[..split].parse::<i64>().ok()?;
    let day = &value[split..];
    ["MO", "TU", "WE", "TH", "FR", "SA", "SU"]
        .contains(&day.to_ascii_uppercase().as_str())
        .then_some((Some(nth), day))
}

fn integer_list(value: &str) -> Option<Vec<Value>> {
    value
        .split(',')
        .map(|item| Some(json!(item.parse::<i64>().ok()?)))
        .collect()
}

fn rrule_by_day(value: &Value) -> Option<String> {
    let days = value.as_array()?;
    days.iter()
        .map(|day| {
            let object = day.as_object()?;
            if object
                .keys()
                .any(|key| !["@type", "day", "nthOfPeriod"].contains(&key.as_str()))
            {
                return None;
            }
            let day = object.get("day")?.as_str()?;
            split_by_day(day)?;
            let prefix = object
                .get("nthOfPeriod")
                .and_then(Value::as_i64)
                .map(|nth| nth.to_string())
                .unwrap_or_default();
            Some(format!("{}{}", prefix, day.to_ascii_uppercase()))
        })
        .collect::<Option<Vec<_>>>()
        .map(|days| days.join(","))
}

fn rrule_integer_list(value: &Value) -> Option<String> {
    value
        .as_array()?
        .iter()
        .map(|item| item.as_i64().map(|item| item.to_string()))
        .collect::<Option<Vec<_>>>()
        .map(|items| items.join(","))
}

fn participants_from_attendees(attendees: &[EventAttendee]) -> Map<String, Value> {
    attendees
        .iter()
        .enumerate()
        .map(|(index, attendee)| {
            let mut value = Map::new();
            value.insert("@type".to_string(), json!("Participant"));
            value.insert("email".to_string(), json!(attendee.email));
            if let Some(name) = &attendee.name {
                value.insert("name".to_string(), json!(name));
            }
            value.insert(
                "participationStatus".to_string(),
                json!(rsvp_value(attendee.status)),
            );
            value.insert("expectReply".to_string(), Value::Bool(true));
            if let Some(roles) = roles_from_attendee(attendee.role) {
                value.insert("roles".to_string(), Value::Object(roles));
            }
            (format!("p{index}"), Value::Object(value))
        })
        .collect()
}

fn participants_from_event_create(event: &EventCreate) -> Map<String, Value> {
    let mut participants = Map::new();
    if let Some(organizer) = &event.organizer {
        let mut value = Map::new();
        value.insert("@type".to_string(), json!("Participant"));
        value.insert("email".to_string(), json!(organizer.email));
        if let Some(name) = &organizer.name {
            value.insert("name".to_string(), json!(name));
        }
        value.insert("roles".to_string(), json!({ "owner": true }));
        participants.insert("owner".to_string(), Value::Object(value));
    }
    participants.extend(participants_from_attendees(&event.attendees));
    participants
}

fn roles_from_attendee(role: AttendeeRole) -> Option<Map<String, Value>> {
    let role = match role {
        AttendeeRole::Required => "attendee",
        AttendeeRole::Optional => "optional",
        AttendeeRole::Resource => "resource",
        AttendeeRole::Chair => "chair",
        _ => return None,
    };
    let mut roles = Map::new();
    roles.insert(role.to_string(), Value::Bool(true));
    Some(roles)
}

fn attendees(
    participants: Option<&Map<String, Value>>,
) -> Result<Vec<EventAttendee>, &'static str> {
    participants
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| value.as_object())
        .filter(|object| object.get("email").is_some())
        .map(|object| {
            Ok(EventAttendee {
                email: object
                    .get("email")
                    .and_then(Value::as_str)
                    .ok_or("JMAP participant email is not a string")?
                    .to_string(),
                name: object
                    .get("name")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                role: attendee_role(object.get("roles"))?,
                status: rsvp_status(object.get("participationStatus").and_then(Value::as_str))?,
            })
        })
        .collect()
}

fn attendee_role(value: Option<&Value>) -> Result<AttendeeRole, &'static str> {
    let Some(roles) = value.and_then(Value::as_object) else {
        return Ok(AttendeeRole::Unknown);
    };
    let known = ["chair", "owner", "optional", "resource", "attendee"];
    if roles
        .iter()
        .any(|(role, enabled)| enabled.as_bool() == Some(true) && !known.contains(&role.as_str()))
    {
        return Err("JMAP participant has an unsupported role");
    }
    Ok(
        if role_enabled(roles, "chair") || role_enabled(roles, "owner") {
            AttendeeRole::Chair
        } else if role_enabled(roles, "optional") {
            AttendeeRole::Optional
        } else if role_enabled(roles, "resource") {
            AttendeeRole::Resource
        } else if role_enabled(roles, "attendee") {
            AttendeeRole::Required
        } else {
            AttendeeRole::Unknown
        },
    )
}

fn role_enabled(roles: &Map<String, Value>, role: &str) -> bool {
    roles.get(role).and_then(Value::as_bool).unwrap_or(false)
}

fn organizer(participants: Option<&Map<String, Value>>) -> Option<EventOrganizer> {
    participants
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| {
            let object = value.as_object()?;
            object
                .get("roles")
                .and_then(Value::as_object)
                .is_some_and(|roles| roles.contains_key("owner"))
                .then(|| EventOrganizer {
                    email: object
                        .get("email")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    name: object
                        .get("name")
                        .and_then(Value::as_str)
                        .map(ToString::to_string),
                })
        })
        .next()
}

/// Project JSCalendar `alerts` into `EventReminder`s. Each alert's
/// `trigger` is either an `OffsetTrigger` (relative to the event start or
/// end) or an `AbsoluteTrigger` (a UTC instant); the alert `action`
/// (`display` / `email`) is carried through uppercased.
fn reminders_from_alerts(alerts: Option<&Map<String, Value>>) -> Vec<EventReminder> {
    alerts
        .into_iter()
        .flat_map(Map::values)
        .filter_map(reminder_from_alert)
        .collect()
}

fn reminder_from_alert(value: &Value) -> Option<EventReminder> {
    let alert = value.as_object()?;
    let trigger = alert.get("trigger").and_then(Value::as_object)?;
    let action = alert
        .get("action")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase);
    let trigger = match trigger.get("@type").and_then(Value::as_str) {
        Some("AbsoluteTrigger") => {
            ReminderTrigger::Absolute(trigger.get("when").and_then(Value::as_str)?.to_string())
        }
        // OffsetTrigger is the default when @type is absent.
        _ => {
            let offset = trigger.get("offset").and_then(Value::as_str)?.to_string();
            let relative_to = match trigger.get("relativeTo").and_then(Value::as_str) {
                Some("end") => ReminderRelativeTo::End,
                _ => ReminderRelativeTo::Start,
            };
            ReminderTrigger::Relative {
                offset,
                relative_to,
            }
        }
    };
    Some(EventReminder { trigger, action })
}

fn first_location(locations: Option<&Map<String, Value>>) -> Option<String> {
    locations
        .and_then(|locations| locations.values().next())
        .and_then(Value::as_object)
        .and_then(|location| location.get("name"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn single_location(value: &str) -> Map<String, Value> {
    let mut location = Map::new();
    location.insert("@type".to_string(), json!("Location"));
    location.insert("name".to_string(), json!(value));
    let mut locations = Map::new();
    locations.insert("loc1".to_string(), Value::Object(location));
    locations
}

fn duration(start: &str, end: &str) -> String {
    if start.len() == 10
        && end.len() == 10
        && let (Ok(start), Ok(end)) = (
            civil::Date::strptime("%Y-%m-%d", start),
            civil::Date::strptime("%Y-%m-%d", end),
        )
    {
        // `EventTime`'s all-day end is exclusive, so the JSCalendar
        // duration is exactly end - start days (a single all-day event,
        // start D / end D+1, is P1D).
        let days = (end - start).get_days().max(0);
        return format!("P{days}D");
    }
    let Ok(start) = start.parse::<Timestamp>() else {
        return "PT0S".to_string();
    };
    let Ok(end) = end.parse::<Timestamp>() else {
        return "PT0S".to_string();
    };
    let seconds = end.duration_since(start).as_secs().max(0);
    format!("PT{seconds}S")
}

fn shared_time_from_jmap(value: &str, is_all_day: bool) -> String {
    if is_all_day {
        // The shared all-day contract is a bare date; JSCalendar carries
        // midnight on that date. Keep only the date half so a server that
        // stores the conformant LocalDateTime reads back correctly.
        return value.split('T').next().unwrap_or(value).to_string();
    }
    if value.len() == 10 {
        return value.to_string();
    }
    if value.parse::<Timestamp>().is_ok() {
        return value.to_string();
    }
    if let Some(time) = local_datetime(value) {
        return shared_rfc3339(time);
    }
    value.to_string()
}

fn jmap_time_from_shared(time: &EventTime, is_all_day: bool) -> String {
    if is_all_day || time.value.len() == 10 {
        // JSCalendar has no DATE type: RFC 8984 `start` is always a
        // LocalDateTime, and an all-day event is midnight on its date with
        // `showWithoutTime` carrying the all-day sense. Writing the shared
        // bare DATE straight through emits a value no conforming server
        // accepts, and only reads back because our own read path truncates
        // the same way.
        return if time.value.len() == 10 {
            format!("{}T00:00:00", time.value)
        } else {
            time.value.clone()
        };
    }
    // Rendered in the value's own offset, not normalized to UTC: the
    // JSCalendar `timeZone` property carries the zone separately, so the
    // wall clock is what belongs in the LocalDateTime.
    // A numeric offset yields the wall clock directly. A `Z` suffix is
    // rejected by the civil parser (Temporal reads it as an unknown
    // offset), so route that through the instant and read it back at UTC.
    let wall = time
        .value
        .parse::<civil::DateTime>()
        .ok()
        .or_else(|| Some(Offset::UTC.to_datetime(time.value.parse::<Timestamp>().ok()?)));
    if let Some(wall) = wall {
        return wall.strftime("%Y-%m-%dT%H:%M:%S").to_string();
    }
    time.value.clone()
}

// The JSCalendar event-query `after`/`before` filter conditions are
// LocalDateTime values (draft-ietf-jmap-calendars-26 section 5.11.1): a bare
// wall-clock string with no zone and no trailing `Z`, interpreted in the
// query's `timeZone` argument, which defaults to Etc/UTC. A range boundary is
// always a timed instant, so an offset-bearing RFC 3339 input is normalized to
// UTC and rendered bare, matching that Etc/UTC default. Date-only or
// unparseable values pass through untouched.
fn jmap_utc_filter_time(time: &EventTime) -> String {
    if let Ok(parsed) = time.value.parse::<Timestamp>() {
        return Offset::UTC
            .to_datetime(parsed)
            .strftime("%Y-%m-%dT%H:%M:%S")
            .to_string();
    }
    time.value.clone()
}

fn event_in_range(event: &CalendarEvent, start: &EventTime, end: &EventTime) -> bool {
    if start.value.is_empty() || end.value.is_empty() {
        return true;
    }
    let Some((range_start, range_end)) = time_interval(start, end, false) else {
        return true;
    };
    let Some((event_start, event_end)) = time_interval(&event.start, &event.end, event.is_all_day)
    else {
        return true;
    };
    // Both the query window and event interval use exclusive ends. An event
    // ending exactly at the window start, or starting exactly at its end,
    // does not overlap the window.
    event_start < range_end && event_end > range_start
}

fn time_interval(
    start: &EventTime,
    end: &EventTime,
    is_all_day: bool,
) -> Option<(Timestamp, Timestamp)> {
    let start = comparable_time(start, is_all_day)?;
    let end = comparable_time(end, is_all_day).unwrap_or(start);
    Some((start, end))
}

fn comparable_time(time: &EventTime, is_all_day: bool) -> Option<Timestamp> {
    if is_all_day || time.value.len() == 10 {
        let date = civil::Date::strptime("%Y-%m-%d", &time.value).ok()?;
        return Offset::UTC
            .to_timestamp(date.to_datetime(civil::Time::MIN))
            .ok();
    }
    time.value
        .parse::<Timestamp>()
        .ok()
        .or_else(|| local_datetime(&time.value))
}

/// Render an instant for the shared `EventTime` layer, which spells the
/// UTC offset out as `+00:00` rather than `Z`.
fn shared_rfc3339(at: Timestamp) -> String {
    Offset::UTC
        .to_datetime(at)
        .strftime("%Y-%m-%dT%H:%M:%S+00:00")
        .to_string()
}

/// A zoneless JSCalendar LocalDateTime, anchored at UTC so two of them
/// compare against each other and against offset-bearing values on one
/// consistent scale.
fn local_datetime(value: &str) -> Option<Timestamp> {
    let naive = civil::DateTime::strptime("%Y-%m-%dT%H:%M:%S", value).ok()?;
    Offset::UTC.to_timestamp(naive).ok()
}

fn end_from_start_duration(start: &str, duration: &str, is_all_day: bool) -> String {
    let seconds = parse_duration_seconds(duration).unwrap_or(0);
    if (is_all_day || start.len() == 10)
        && let Ok(date) =
            civil::Date::strptime("%Y-%m-%d", start.split('T').next().unwrap_or(start))
    {
        // `EventTime`'s all-day end is exclusive: end = start + duration
        // days (a P1D JSCalendar all-day event, start D, ends D+1).
        let days = seconds.div_ceil(86_400);
        let end = Span::new()
            .try_days(i64::try_from(days).unwrap_or(i64::MAX))
            .and_then(|span| date.checked_add(span))
            .unwrap_or(date);
        return end.strftime("%Y-%m-%d").to_string();
    }
    let offset = SignedDuration::from_secs(i64::try_from(seconds).unwrap_or(i64::MAX));
    if let Ok(start) = start.parse::<Timestamp>() {
        return shared_rfc3339(start.checked_add(offset).unwrap_or(start));
    }
    if let Some(start) = local_datetime(start) {
        let end = start.checked_add(offset).unwrap_or(start);
        return Offset::UTC
            .to_datetime(end)
            .strftime("%Y-%m-%dT%H:%M:%S")
            .to_string();
    }
    start.to_string()
}

fn parse_duration_seconds(value: &str) -> Option<u64> {
    let value = value.strip_prefix('P')?;
    let mut seconds = 0_u64;
    let mut number = String::new();
    let mut in_time = false;
    for ch in value.chars() {
        match ch {
            'T' => in_time = true,
            'D' => {
                seconds = seconds.checked_add(number.parse::<u64>().ok()?.checked_mul(86_400)?)?;
                number.clear();
            }
            'H' if in_time => {
                seconds = seconds.checked_add(number.parse::<u64>().ok()?.checked_mul(3_600)?)?;
                number.clear();
            }
            'M' if in_time => {
                seconds = seconds.checked_add(number.parse::<u64>().ok()?.checked_mul(60)?)?;
                number.clear();
            }
            'S' if in_time => {
                seconds = seconds.checked_add(number.parse::<u64>().ok()?)?;
                number.clear();
            }
            digit if digit.is_ascii_digit() => number.push(digit),
            _ => return None,
        }
    }
    number.is_empty().then_some(seconds)
}

fn rights_can_write(rights: Option<&CalendarRights>) -> bool {
    rights
        .and_then(|rights| rights.may_write_all.or(rights.may_write_own))
        .unwrap_or(false)
}

fn rights_can_delete(rights: Option<&CalendarRights>) -> bool {
    rights.and_then(|rights| rights.may_delete).unwrap_or(false)
}

fn event_status(value: Option<&str>) -> EventStatus {
    match value.unwrap_or_default() {
        "confirmed" => EventStatus::Confirmed,
        "tentative" => EventStatus::Tentative,
        "cancelled" => EventStatus::Cancelled,
        _ => EventStatus::Unknown,
    }
}

fn jmap_event_status(value: EventStatus) -> &'static str {
    match value {
        EventStatus::Tentative => "tentative",
        EventStatus::Cancelled => "cancelled",
        _ => "confirmed",
    }
}

fn availability(value: Option<&str>) -> EventAvailability {
    match value.unwrap_or_default() {
        "free" => EventAvailability::Free,
        "busy" => EventAvailability::Busy,
        _ => EventAvailability::Unknown,
    }
}

fn jmap_availability(value: EventAvailability) -> &'static str {
    match value {
        EventAvailability::Free => "free",
        _ => "busy",
    }
}

fn visibility(value: Option<&str>) -> EventVisibility {
    match value.unwrap_or_default() {
        "public" => EventVisibility::Public,
        "private" => EventVisibility::Private,
        "secret" => EventVisibility::Confidential,
        _ => EventVisibility::Default,
    }
}

fn jmap_visibility(value: EventVisibility) -> &'static str {
    match value {
        EventVisibility::Public => "public",
        EventVisibility::Private => "private",
        EventVisibility::Confidential => "secret",
        EventVisibility::Default => "public",
        _ => "public",
    }
}

fn rsvp_value(value: RsvpStatus) -> &'static str {
    match value {
        RsvpStatus::Accepted => "accepted",
        RsvpStatus::Declined => "declined",
        RsvpStatus::Tentative => "tentative",
        RsvpStatus::Delegated => "delegated",
        RsvpStatus::NeedsAction | RsvpStatus::Unknown => "needs-action",
        _ => "needs-action",
    }
}

fn rsvp_status(value: Option<&str>) -> Result<RsvpStatus, &'static str> {
    match value {
        Some("accepted") => Ok(RsvpStatus::Accepted),
        Some("declined") => Ok(RsvpStatus::Declined),
        Some("tentative") => Ok(RsvpStatus::Tentative),
        Some("delegated") => Ok(RsvpStatus::Delegated),
        Some("needs-action") => Ok(RsvpStatus::NeedsAction),
        None => Ok(RsvpStatus::Unknown),
        Some(_) => Err("JMAP participant has an unsupported participationStatus"),
    }
}

fn validate_shared_attendees(
    attendees: &[EventAttendee],
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if attendees.iter().any(|attendee| {
        matches!(attendee.role, AttendeeRole::Unknown)
            || matches!(attendee.status, RsvpStatus::Unknown)
    }) {
        return Err(unsupported(
            operation,
            "JMAP cannot serialize an unknown attendee role or participation status",
        ));
    }
    Ok(())
}

fn next_cursor(position: i32, count: usize, total: Option<usize>) -> Option<Vec<u8>> {
    let count = i32::try_from(count).ok()?;
    let next = position.saturating_add(count);
    let total = total.and_then(|total| i32::try_from(total).ok())?;
    (next < total).then(|| next.to_string().into_bytes())
}

fn limit(value: Option<u32>) -> usize {
    value
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(PAGE_LIMIT)
}

fn usize_to_u64(value: usize, operation: AccountOperation) -> Result<u64, AccountError> {
    u64::try_from(value).map_err(|error| {
        super::error::unsupported_error(
            operation,
            None,
            format!("JMAP calendar total does not fit u64: {error}"),
        )
    })
}

fn decode_position(
    page_cursor: Option<Vec<u8>>,
    operation: AccountOperation,
) -> Result<i32, AccountError> {
    let Some(cursor) = page_cursor else {
        return Ok(0);
    };
    let cursor =
        String::from_utf8(cursor).map_err(|error| cursor_error(operation, error.to_string()))?;
    cursor
        .parse::<i32>()
        .map_err(|error| cursor_error(operation, error.to_string()))
}

fn cursor_error(operation: AccountOperation, message: String) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::SyncState(
            bifrost_types::SyncStateErrorKind::SchemaIncompatible,
        ),
        bifrost_types::Cause::State(bifrost_types::StateCause::SchemaIncompatible),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(operation)
    .text(DiagnosticText::support_only(message))
    .try_build()
    .expect("valid account error classification")
}

fn require_calendars<T: HttpTransport>(
    calendars: Option<CalendarAccount<T>>,
    operation: AccountOperation,
) -> Result<CalendarAccount<T>, AccountError> {
    calendars.ok_or_else(|| unsupported(operation, "JMAP Calendars capability is unavailable"))
}

fn unsupported(operation: AccountOperation, message: &str) -> AccountError {
    super::error::unsupported_error(operation, None, message)
}

fn to_acct_err(
    operation: AccountOperation,
) -> impl FnOnce(crate::Error) -> AccountError + Send + 'static {
    move |error| {
        super::error::into_account_error(error, super::error::JmapErrorContext::new(operation))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(value: &str) -> EventTime {
        EventTime {
            value: value.to_string(),
            timezone: None,
        }
    }

    fn create_payload(event: &EventCreate) -> CalendarEventCreate {
        jmap_create_from_event(event, AccountOperation::EventCreate).expect("create payload")
    }

    /// Builds the JMAP patch using only the context the `EventPatch` itself
    /// carries, which is what `update` does when it does not need to read the
    /// current event.
    fn patch_with_patch_context(
        patch: &EventPatch,
        operation: AccountOperation,
    ) -> Result<CalendarEventPatch, AccountError> {
        jmap_patch_from_event_patch(patch, &event_context_from_patch(patch, None), operation)
    }

    fn event(start: &str, end: &str, is_all_day: bool) -> CalendarEvent {
        CalendarEvent {
            id: EventId("e1".to_string()),
            calendar_id: CalendarId("c1".to_string()),
            native_id: "e1".to_string(),
            uid: None,
            etag: None,
            provenance: CalendarProvenance {
                provider: ProtocolKind::Jmap,
                native: "e1".to_string(),
                calendar_native: Some("c1".to_string()),
            },
            title: None,
            description: None,
            location: None,
            start: time(start),
            end: time(end),
            is_all_day,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            self_response: RsvpStatus::Unknown,
            organizer: None,
            attendees: Vec::new(),
            reminders: Vec::new(),
            recurrence: EventRecurrence::default(),
            html_link: None,
            raw_ical: None,
        }
    }

    #[test]
    fn duration_handles_all_day_dates() {
        // All-day ends are exclusive: a single all-day event is start D /
        // end D+1 (P1D); a three-day event is start D / end D+3 (P3D).
        assert_eq!(duration("2026-06-02", "2026-06-03"), "P1D");
        assert_eq!(duration("2026-06-02", "2026-06-05"), "P3D");
    }

    #[test]
    fn end_from_duration_handles_timed_and_all_day_events() {
        assert_eq!(
            end_from_start_duration("2026-06-02T12:00:00Z", "PT90M", false),
            "2026-06-02T13:30:00+00:00"
        );
        // Exclusive end: start 2026-06-02 + P3D = 2026-06-05.
        assert_eq!(
            end_from_start_duration("2026-06-02", "P3D", true),
            "2026-06-05"
        );
    }

    #[test]
    fn jmap_local_datetime_reads_as_shared_rfc3339() {
        assert_eq!(
            shared_time_from_jmap("2026-06-02T12:00:00", false),
            "2026-06-02T12:00:00+00:00"
        );
        assert_eq!(shared_time_from_jmap("2026-06-02", true), "2026-06-02");
    }

    #[test]
    fn shared_rfc3339_writes_as_jmap_local_datetime() {
        assert_eq!(
            jmap_time_from_shared(
                &EventTime {
                    value: "2026-06-02T12:00:00+02:00".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                false,
            ),
            "2026-06-02T12:00:00"
        );
    }

    #[test]
    fn filter_time_normalizes_offset_to_bare_utc_local_datetime() {
        assert_eq!(
            jmap_utc_filter_time(&EventTime {
                value: "2026-06-02T12:00:00+02:00".to_string(),
                timezone: Some("Europe/Oslo".to_string()),
            }),
            "2026-06-02T10:00:00"
        );
    }

    #[test]
    fn range_filter_uses_overlap_not_lexicographic_start() {
        let event = event("2026-06-01T23:00:00Z", "2026-06-02T01:00:00Z", false);

        assert!(event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-02T23:59:59Z")
        ));
    }

    #[test]
    fn range_filter_includes_all_day_on_window_start() {
        // Single all-day event on 2026-06-02 under the exclusive contract:
        // start 2026-06-02, end 2026-06-03.
        let event = event("2026-06-02", "2026-06-03", true);

        assert!(event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    #[test]
    fn range_filter_excludes_events_touching_only_an_exclusive_boundary() {
        let before = event("2026-06-01T23:00:00Z", "2026-06-02T00:00:00Z", false);
        let after = event("2026-06-03T00:00:00Z", "2026-06-03T01:00:00Z", false);
        let start = time("2026-06-02T00:00:00Z");
        let end = time("2026-06-03T00:00:00Z");
        assert!(!event_in_range(&before, &start, &end));
        assert!(!event_in_range(&after, &start, &end));
    }

    #[test]
    fn range_query_filter_includes_calendar_and_time_bounds() {
        let value = serde_json::to_value(range_filter(&EventRange {
            calendar_id: CalendarId("cal".to_string()),
            start: time("2026-06-02T00:00:00Z"),
            end: time("2026-06-03T00:00:00Z"),
            page_cursor: None,
            limit: None,
        }))
        .expect("filter json");

        assert_eq!(value["operator"].as_str(), Some("AND"));
        assert_eq!(value["conditions"][0]["inCalendar"].as_str(), Some("cal"));
        assert_eq!(
            value["conditions"][1]["after"].as_str(),
            Some("2026-06-02T00:00:00")
        );
        assert_eq!(
            value["conditions"][2]["before"].as_str(),
            Some("2026-06-03T00:00:00")
        );
    }

    #[test]
    fn decode_position_rejects_invalid_cursor() {
        let error = decode_position(
            Some(Vec::from("not-a-number")),
            AccountOperation::EventsInRange,
        )
        .expect_err("invalid cursor should fail");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::SchemaIncompatible
            )
        ));
    }

    #[test]
    fn create_payload_stamps_jscalendar_types() {
        let create = create_payload(&EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: None,
            description: None,
            location: Some("Room".to_string()),
            start: time("2026-06-02T12:00:00Z"),
            end: time("2026-06-02T13:00:00Z"),
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: vec![EventAttendee {
                email: "a@example.test".to_string(),
                name: None,
                role: AttendeeRole::Required,
                status: RsvpStatus::NeedsAction,
            }],
            recurrence: EventRecurrence {
                rrule: Some("FREQ=WEEKLY;BYDAY=MO".to_string()),
                rdate: Vec::new(),
                exdate: Vec::new(),
                recurrence_id: None,
            },
        });

        assert_eq!(create.properties.get("@type"), Some(&json!("Event")));
        assert_eq!(
            create.properties["locations"]["loc1"]["@type"],
            json!("Location")
        );
        assert_eq!(
            create.properties["participants"]["p0"]["@type"],
            json!("Participant")
        );
        assert_eq!(
            create.properties["recurrenceRules"][0]["@type"],
            json!("RecurrenceRule")
        );
        assert_eq!(
            create.properties["recurrenceRules"][0]["byDay"][0]["@type"],
            json!("NDay")
        );
    }

    #[test]
    fn event_patch_recomputes_duration_from_both_bounds() {
        let patch = patch_with_patch_context(
            &EventPatch {
                start: Some(time("2026-06-02T12:00:00Z")),
                end: Some(time("2026-06-02T13:30:00Z")),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("patch");

        assert_eq!(
            patch.properties.get("start").and_then(Value::as_str),
            Some("2026-06-02T12:00:00")
        );
        assert_eq!(
            patch.properties.get("duration").and_then(Value::as_str),
            Some("PT5400S")
        );
    }

    #[test]
    fn event_patch_rejects_single_bound_time_change() {
        for patch in [
            EventPatch {
                start: Some(time("2026-06-02T12:00:00Z")),
                ..EventPatch::default()
            },
            EventPatch {
                end: Some(time("2026-06-02T13:00:00Z")),
                ..EventPatch::default()
            },
        ] {
            let error = patch_with_patch_context(&patch, AccountOperation::EventUpdate)
                .expect_err("single-bound time patch should reject");
            assert!(matches!(
                error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
            ));
        }
    }

    #[test]
    fn recurrence_rule_normalizes_until_to_event_local_datetime() {
        let zoned_start = EventTime {
            value: "2025-01-01T09:00:00+01:00".to_string(),
            timezone: Some("Europe/Oslo".to_string()),
        };
        let utc = jmap_recurrence_rule_from_rrule(
            "FREQ=DAILY;UNTIL=20260601T070000Z",
            Some(&zoned_start),
            false,
        )
        .expect("UTC until");
        assert_eq!(utc["until"].as_str(), Some("2026-06-01T09:00:00"));

        let floating_start = EventTime {
            value: "2025-01-01T09:00:00+00:00".to_string(),
            timezone: None,
        };
        let floating = jmap_recurrence_rule_from_rrule(
            "FREQ=DAILY;UNTIL=20260101T000000",
            Some(&floating_start),
            false,
        )
        .expect("floating until");
        assert_eq!(floating["until"].as_str(), Some("2026-01-01T00:00:00"));
        let date_only = jmap_recurrence_rule_from_rrule(
            "FREQ=DAILY;UNTIL=20260101",
            Some(&time("2025-01-01")),
            true,
        )
        .expect("date until");
        assert_eq!(date_only["until"].as_str(), Some("2026-01-01T00:00:00"));
    }

    #[test]
    fn event_patch_clears_nullable_fields_with_null() {
        let patch = patch_with_patch_context(
            &EventPatch {
                title: Some(None),
                description: Some(None),
                location: Some(None),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("patch");

        assert_eq!(patch.properties.get("title"), Some(&Value::Null));
        assert_eq!(patch.properties.get("description"), Some(&Value::Null));
        assert_eq!(patch.properties.get("locations"), Some(&Value::Null));
    }

    #[test]
    fn recurrence_rule_writes_jscalendar_object() {
        let rule = jmap_recurrence_rule_from_rrule(
            "FREQ=WEEKLY;INTERVAL=2;COUNT=5;BYDAY=MO,WE;BYMONTH=6;BYMONTHDAY=2",
            None,
            false,
        )
        .expect("rrule should convert");

        assert_eq!(rule["frequency"].as_str(), Some("weekly"));
        assert_eq!(rule["interval"].as_u64(), Some(2));
        assert_eq!(rule["count"].as_u64(), Some(5));
        assert_eq!(rule["byDay"][0]["day"].as_str(), Some("mo"));
        assert_eq!(rule["byDay"][1]["day"].as_str(), Some("we"));
        assert_eq!(rule["byMonth"][0].as_i64(), Some(6));
        assert_eq!(rule["byMonthDay"][0].as_i64(), Some(2));
    }

    #[test]
    fn recurrence_rule_reads_jscalendar_object_as_rrule() {
        let rrule = rrule_from_jmap_recurrence_rule(
            &json!({
                "frequency": "monthly",
                "interval": 1,
                "count": 3,
                "byDay": [{"day": "tu", "nthOfPeriod": 2}],
                "byMonthDay": [14]
            }),
            "2025-01-01T09:00:00",
            false,
            None,
        )
        .expect("jscalendar rule should convert")
        .expect("rrule");

        assert_eq!(
            rrule,
            "FREQ=MONTHLY;INTERVAL=1;COUNT=3;BYDAY=2TU;BYMONTHDAY=14"
        );
    }

    #[test]
    fn zoned_jscalendar_until_writes_inclusive_utc_ical_until() {
        let rrule = rrule_from_jmap_recurrence_rule(
            &json!({
                "@type": "RecurrenceRule",
                "frequency": "daily",
                "until": "2026-06-01T09:00:00"
            }),
            "2025-01-01T09:00:00",
            false,
            Some("Europe/Oslo"),
        )
        .expect("supported rule")
        .expect("rrule");
        assert_eq!(rrule, "FREQ=DAILY;UNTIL=20260601T070000Z");
    }

    #[test]
    fn unknown_jscalendar_recurrence_component_is_not_silently_dropped() {
        let error = rrule_from_jmap_recurrence_rule(
            &json!({
                "@type": "RecurrenceRule",
                "frequency": "daily",
                "byHour": [9]
            }),
            "2025-01-01T09:00:00",
            false,
            None,
        )
        .expect_err("unknown recurrence component must reject");
        assert_eq!(
            error,
            "JMAP recurrence rule contains an unsupported component"
        );
    }

    #[test]
    fn an_all_day_start_is_written_as_a_jscalendar_local_datetime() {
        // Absolute wire values, not a round trip: both directions previously
        // shared a bare-DATE assumption, so a round trip proved nothing.
        assert_eq!(
            jmap_time_from_shared(&time("2026-06-02"), true),
            "2026-06-02T00:00:00"
        );
        // And a conformant server's midnight LocalDateTime reads back as the
        // shared bare date.
        assert_eq!(
            shared_time_from_jmap("2026-06-02T00:00:00", true),
            "2026-06-02"
        );
        // The exclusive all-day end still lands a day out from either form.
        assert_eq!(
            end_from_start_duration("2026-06-02T00:00:00", "P1D", true),
            "2026-06-03"
        );
        assert_eq!(
            end_from_start_duration("2026-06-02", "P1D", true),
            "2026-06-03"
        );
    }

    #[test]
    fn recurrence_only_patch_with_until_demands_the_current_event() {
        // A patch that touches nothing but the RRULE cannot supply the start
        // timezone or the all-day flag, so `update` must read them back.
        let recurrence_only = EventPatch {
            recurrence: Some(EventRecurrence {
                rrule: Some("FREQ=DAILY;UNTIL=20260601T070000Z".to_string()),
                ..EventRecurrence::default()
            }),
            ..EventPatch::default()
        };
        assert!(event_patch_needs_context(&recurrence_only));
        // Without that read the conversion cannot succeed, and the guard that
        // `update` runs first turns it into Unsupported. (The payload builder
        // also rejects a failed conversion on its own, so this validation is
        // an early door, not the only thing standing between a bad patch and
        // an erased recurrence.)
        let error = validate_shared_recurrence(
            recurrence_only.recurrence.as_ref().expect("recurrence"),
            None,
            false,
            AccountOperation::EventUpdate,
        )
        .expect_err("no context, no conversion");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        ));

        // A recurrence patch with no UNTIL needs nothing extra.
        let countable = EventPatch {
            recurrence: Some(EventRecurrence {
                rrule: Some("FREQ=DAILY;COUNT=3".to_string()),
                ..EventRecurrence::default()
            }),
            ..EventPatch::default()
        };
        assert!(!event_patch_needs_context(&countable));
    }

    #[test]
    fn recurrence_context_falls_back_to_the_current_event() {
        let current = JmapCalendarEvent {
            properties: serde_json::from_value(json!({
                "id": "e1",
                "start": "2025-01-01T09:00:00",
                "timeZone": "Europe/Oslo",
                "showWithoutTime": false
            }))
            .expect("event"),
        };
        let patch = EventPatch {
            recurrence: Some(EventRecurrence {
                rrule: Some("FREQ=DAILY;UNTIL=20260601T070000Z".to_string()),
                ..EventRecurrence::default()
            }),
            ..EventPatch::default()
        };
        let context = event_context_from_patch(&patch, Some(&current));
        assert_eq!(
            context
                .start
                .as_ref()
                .and_then(|start| start.timezone.as_deref()),
            Some("Europe/Oslo")
        );
        assert!(!context.is_all_day);

        let built = jmap_patch_from_event_patch(&patch, &context, AccountOperation::EventUpdate)
            .expect("patch");
        // 07:00 UTC is 09:00 Oslo summer wall clock; the inclusive bound holds.
        assert_eq!(
            built.properties["recurrenceRules"][0]["until"].as_str(),
            Some("2026-06-01T09:00:00")
        );
    }

    #[test]
    fn an_all_day_event_keeps_its_date_start_when_a_patch_omits_is_all_day() {
        let current = JmapCalendarEvent {
            properties: serde_json::from_value(json!({
                "id": "e1",
                "start": "2026-06-01T00:00:00",
                "showWithoutTime": true
            }))
            .expect("event"),
        };
        let patch = EventPatch {
            start: Some(time("2026-06-02")),
            end: Some(time("2026-06-03")),
            ..EventPatch::default()
        };
        assert!(event_patch_needs_context(&patch));
        let context = event_context_from_patch(&patch, Some(&current));
        assert!(context.is_all_day);

        let built = jmap_patch_from_event_patch(&patch, &context, AccountOperation::EventUpdate)
            .expect("patch");
        assert_eq!(
            built.properties["start"].as_str(),
            Some("2026-06-02T00:00:00")
        );
        assert_eq!(built.properties["duration"].as_str(), Some("P1D"));
    }

    #[test]
    fn a_modified_recurrence_override_is_not_silently_discarded() {
        let overrides: Map<String, Value> = serde_json::from_value(json!({
            "2026-06-03T12:00:00": {"title": "moved"}
        }))
        .expect("overrides");
        assert_eq!(
            recurrence_dates_from_overrides(Some(&overrides)),
            Err(
                "a modified JMAP recurrence override cannot be represented by the shared recurrence model"
            )
        );

        // Pure exclusions and pure additions still project.
        let plain: Map<String, Value> = serde_json::from_value(json!({
            "2026-06-04T12:00:00": {"excluded": true},
            "2026-06-05T12:00:00": {}
        }))
        .expect("overrides");
        let (rdate, exdate) =
            recurrence_dates_from_overrides(Some(&plain)).expect("representable overrides");
        assert_eq!(rdate, vec!["2026-06-05T12:00:00".to_string()]);
        assert_eq!(exdate, vec!["2026-06-04T12:00:00".to_string()]);
    }

    #[test]
    fn a_modified_override_fails_event_hydration() {
        let event = JmapCalendarEvent {
            properties: serde_json::from_value(json!({
                "id": "e1",
                "start": "2026-06-01T09:00:00",
                "duration": "PT1H",
                "recurrenceOverrides": {"2026-06-03T09:00:00": {"title": "moved"}}
            }))
            .expect("event"),
        };
        let error = event_from_jmap(event, AccountOperation::EventGet)
            .expect_err("modified override must reject");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventGet)
        ));
    }

    #[test]
    fn recurrence_patch_clears_or_writes_recurrence_rules() {
        let clear = patch_with_patch_context(
            &EventPatch {
                recurrence: Some(EventRecurrence::default()),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("patch");
        assert_eq!(clear.properties.get("recurrenceRules"), Some(&Value::Null));
        assert_eq!(
            clear.properties.get("recurrenceOverrides"),
            Some(&Value::Null)
        );

        let write = patch_with_patch_context(
            &EventPatch {
                recurrence: Some(EventRecurrence {
                    rrule: Some("FREQ=DAILY;COUNT=2".to_string()),
                    rdate: vec!["2026-06-03T12:00:00".to_string()],
                    exdate: vec!["2026-06-04T12:00:00".to_string()],
                    recurrence_id: None,
                }),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("patch");
        let rules = write.properties["recurrenceRules"]
            .as_array()
            .expect("rules array");
        assert_eq!(rules[0]["frequency"].as_str(), Some("daily"));
        assert_eq!(rules[0]["count"].as_u64(), Some(2));
        assert_eq!(
            write.properties["recurrenceOverrides"]["2026-06-03T12:00:00"],
            json!({})
        );
        assert_eq!(
            write.properties["recurrenceOverrides"]["2026-06-04T12:00:00"]["excluded"].as_bool(),
            Some(true)
        );
    }

    /// The payload builders must reject a failed RRULE conversion on their
    /// own, not clear or drop the recurrence. The callers validate first,
    /// but an invariant that holds only by call order is not a guard: a
    /// future caller that built a payload without validating would have
    /// silently erased (patch) or omitted (create) the recurrence.
    #[test]
    fn payload_builders_reject_a_failed_rrule_conversion_instead_of_clearing() {
        let bad_recurrence = EventRecurrence {
            rrule: Some("FREQ=DAILY;BYHOUR=9".to_string()),
            ..EventRecurrence::default()
        };

        let error = patch_with_patch_context(
            &EventPatch {
                recurrence: Some(bad_recurrence.clone()),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect_err("a failed conversion must not become a clear");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        ));

        // An explicit no-rule recurrence still clears.
        let clear = patch_with_patch_context(
            &EventPatch {
                recurrence: Some(EventRecurrence::default()),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("clearing patch");
        assert_eq!(clear.properties.get("recurrenceRules"), Some(&Value::Null));

        let error = jmap_create_from_event(
            &EventCreate {
                calendar_id: CalendarId("cal".to_string()),
                title: None,
                description: None,
                location: None,
                start: time("2026-06-02T12:00:00Z"),
                end: time("2026-06-02T13:00:00Z"),
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: bad_recurrence,
            },
            AccountOperation::EventCreate,
        )
        .expect_err("a failed conversion must not become an omission");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));
    }

    /// JSCalendar keeps the organizer in the same `participants` map as the
    /// attendees, so a whole-map attendee write must carry the owner entries
    /// over or the patch silently deletes the organizer from the event.
    /// Driven through the whole path `update` takes - the fetch decision,
    /// the context extraction, and the patch builder - so an ablation of
    /// any one stage fails it.
    #[test]
    fn an_attendee_patch_preserves_the_owner_participant() {
        let current = JmapCalendarEvent {
            properties: serde_json::from_value(json!({
                "id": "e1",
                "start": "2026-06-01T09:00:00",
                "participants": {
                    "owner": {
                        "@type": "Participant",
                        "email": "owner@example.test",
                        "roles": {"owner": true}
                    },
                    "x1": {
                        "@type": "Participant",
                        "email": "old@example.test",
                        "roles": {"attendee": true}
                    }
                }
            }))
            .expect("event"),
        };
        let patch = EventPatch {
            attendees: Some(vec![EventAttendee {
                email: "a@example.test".to_string(),
                name: None,
                role: AttendeeRole::Required,
                status: RsvpStatus::NeedsAction,
            }]),
            ..EventPatch::default()
        };
        // An attendee-only patch needs no conversion context, but it still
        // has to read the current event for the owner participants.
        assert!(!event_patch_needs_context(&patch));
        assert!(event_patch_needs_current(&patch));

        let context = event_context_from_patch(&patch, Some(&current));
        let built = jmap_patch_from_event_patch(&patch, &context, AccountOperation::EventUpdate)
            .expect("patch");
        let participants = built.properties["participants"]
            .as_object()
            .expect("participants map");

        assert_eq!(
            participants["owner"]["email"].as_str(),
            Some("owner@example.test"),
            "the owner participant survives an attendee write"
        );
        assert_eq!(
            participants["owner"]["roles"]["owner"].as_bool(),
            Some(true)
        );
        assert_eq!(participants["p0"]["email"].as_str(), Some("a@example.test"));
        assert_eq!(
            participants.len(),
            2,
            "non-owner participants are replaced by the new attendee list"
        );
    }

    /// The read path surfaces an owner as a `Chair` attendee, so a
    /// read-modify-write of the attendee list includes the organizer's
    /// email. That entry must update the kept owner participant in place -
    /// keeping its owner role and key - rather than duplicating it or
    /// demoting it to a plain chair.
    #[test]
    fn an_attendee_matching_the_owner_updates_it_in_place() {
        let owners: Map<String, Value> = serde_json::from_value(json!({
            "owner": {
                "@type": "Participant",
                "email": "owner@example.test",
                "roles": {"owner": true},
                "participationStatus": "needs-action"
            }
        }))
        .expect("owners");

        let merged = merge_owner_participants(
            &owners,
            &[EventAttendee {
                email: "Owner@Example.Test".to_string(),
                name: Some("The Owner".to_string()),
                role: AttendeeRole::Chair,
                status: RsvpStatus::Accepted,
            }],
        );

        assert_eq!(merged.len(), 1, "no duplicate participant for the owner");
        assert_eq!(merged["owner"]["roles"]["owner"].as_bool(), Some(true));
        assert_eq!(
            merged["owner"]["participationStatus"].as_str(),
            Some("accepted")
        );
        assert_eq!(merged["owner"]["name"].as_str(), Some("The Owner"));
    }

    /// Fresh attendee keys must never collide with a kept owner key, even
    /// when the server happened to store its owner participant under the
    /// same `p{n}` shape the attendee writer mints.
    #[test]
    fn fresh_attendee_keys_skip_a_colliding_owner_key() {
        let owners: Map<String, Value> = serde_json::from_value(json!({
            "p0": {
                "@type": "Participant",
                "email": "owner@example.test",
                "roles": {"owner": true}
            }
        }))
        .expect("owners");

        let merged = merge_owner_participants(
            &owners,
            &[EventAttendee {
                email: "a@example.test".to_string(),
                name: None,
                role: AttendeeRole::Required,
                status: RsvpStatus::NeedsAction,
            }],
        );

        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged["p0"]["email"].as_str(),
            Some("owner@example.test"),
            "the owner keeps its key"
        );
        assert_eq!(merged["p1"]["email"].as_str(), Some("a@example.test"));
    }

    #[test]
    fn recurrence_validation_rejects_unsupported_rrule_parts() {
        let recurrence = EventRecurrence {
            rrule: Some("FREQ=DAILY;BYHOUR=9".to_string()),
            rdate: Vec::new(),
            exdate: Vec::new(),
            recurrence_id: None,
        };

        let create =
            validate_shared_recurrence(&recurrence, None, false, AccountOperation::EventCreate)
                .expect_err("unsupported create recurrence should fail");
        assert!(matches!(
            create.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));

        let update =
            validate_shared_recurrence(&recurrence, None, false, AccountOperation::EventUpdate)
                .expect_err("unsupported update recurrence should fail");
        assert!(matches!(
            update.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        ));
    }

    #[test]
    fn recurrence_overrides_read_simple_rdate_and_exdate() {
        let overrides = serde_json::from_value::<Map<String, Value>>(json!({
            "2026-06-03T12:00:00": {},
            "2026-06-04T12:00:00": {"excluded": true}
        }))
        .expect("override map");
        let (rdate, exdate) =
            recurrence_dates_from_overrides(Some(&overrides)).expect("representable overrides");

        assert_eq!(rdate, vec!["2026-06-03T12:00:00"]);
        assert_eq!(exdate, vec!["2026-06-04T12:00:00"]);
    }

    #[test]
    fn alerts_project_offset_and_absolute_reminders() {
        let alerts = serde_json::from_value(json!({
            "a1": {
                "@type": "Alert",
                "trigger": {
                    "@type": "OffsetTrigger",
                    "offset": "-PT15M"
                },
                "action": "display"
            },
            "a2": {
                "@type": "Alert",
                "trigger": {
                    "@type": "OffsetTrigger",
                    "offset": "PT5M",
                    "relativeTo": "end"
                },
                "action": "email"
            },
            "a3": {
                "@type": "Alert",
                "trigger": {
                    "@type": "AbsoluteTrigger",
                    "when": "2026-06-02T11:00:00Z"
                }
            }
        }))
        .expect("alerts");

        let reminders = reminders_from_alerts(Some(&alerts));

        assert_eq!(reminders.len(), 3);
        assert!(reminders.iter().any(|reminder| reminder.trigger
            == ReminderTrigger::Relative {
                offset: "-PT15M".to_string(),
                relative_to: ReminderRelativeTo::Start,
            }
            && reminder.action.as_deref() == Some("DISPLAY")));
        assert!(reminders.iter().any(|reminder| reminder.trigger
            == ReminderTrigger::Relative {
                offset: "PT5M".to_string(),
                relative_to: ReminderRelativeTo::End,
            }
            && reminder.action.as_deref() == Some("EMAIL")));
        assert!(reminders.iter().any(|reminder| reminder.trigger
            == ReminderTrigger::Absolute("2026-06-02T11:00:00Z".to_string())
            && reminder.action.is_none()));
    }

    #[test]
    fn attendees_read_roles_from_jscalendar_roles() {
        let participants = serde_json::from_value(json!({
            "p1": {
                "email": "required@example.test",
                "roles": {"attendee": true},
                "participationStatus": "accepted"
            },
            "p2": {
                "email": "optional@example.test",
                "roles": {"optional": true}
            },
            "p3": {
                "email": "room@example.test",
                "roles": {"resource": true}
            },
            "p4": {
                "email": "chair@example.test",
                "roles": {"chair": true}
            }
        }))
        .expect("participants");

        let attendees = attendees(Some(&participants)).expect("supported participants");

        assert_eq!(attendees[0].role, AttendeeRole::Required);
        assert_eq!(attendees[1].role, AttendeeRole::Optional);
        assert_eq!(attendees[2].role, AttendeeRole::Resource);
        assert_eq!(attendees[3].role, AttendeeRole::Chair);
        assert_eq!(attendees[0].status, RsvpStatus::Accepted);
    }

    #[test]
    fn participants_write_roles_to_jscalendar_roles() {
        let participants = participants_from_attendees(&[
            EventAttendee {
                email: "required@example.test".to_string(),
                name: None,
                role: AttendeeRole::Required,
                status: RsvpStatus::NeedsAction,
            },
            EventAttendee {
                email: "room@example.test".to_string(),
                name: None,
                role: AttendeeRole::Resource,
                status: RsvpStatus::Accepted,
            },
        ]);

        assert_eq!(
            participants["p0"]["roles"]["attendee"].as_bool(),
            Some(true)
        );
        assert_eq!(
            participants["p1"]["roles"]["resource"].as_bool(),
            Some(true)
        );
        assert_eq!(participants["p0"]["expectReply"].as_bool(), Some(true));
        assert_eq!(participants["p1"]["expectReply"].as_bool(), Some(true));
    }

    #[test]
    fn unknown_participation_vocabulary_is_not_presented_as_success() {
        let participants = serde_json::from_value(json!({
            "p0": {
                "email": "future@example.test",
                "roles": {"future-role": true},
                "participationStatus": "future-status"
            }
        }))
        .expect("participants");
        assert!(attendees(Some(&participants)).is_err());
    }

    #[test]
    fn create_payload_writes_organizer_as_owner_participant() {
        let create = create_payload(&EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: None,
            description: None,
            location: None,
            start: time("2026-06-02T12:00:00Z"),
            end: time("2026-06-02T13:00:00Z"),
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: Some(EventOrganizer {
                email: "owner@example.test".to_string(),
                name: Some("Owner".to_string()),
            }),
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        });

        let participants = create
            .properties
            .get("participants")
            .and_then(Value::as_object)
            .expect("participants");
        assert_eq!(
            participants["owner"]["email"].as_str(),
            Some("owner@example.test")
        );
        assert_eq!(
            participants["owner"]["roles"]["owner"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn rsvp_patch_updates_single_participant_status_path() {
        let participants = serde_json::from_value(json!({
            "owner": {
                "email": "owner@example.test",
                "roles": {"owner": true}
            },
            "p1": {
                "email": "attendee@example.test",
                "roles": {"attendee": true},
                "participationStatus": "needs-action"
            }
        }))
        .expect("participants");

        let patch =
            jmap_rsvp_patch_from_participants(Some(&participants), &[], RsvpStatus::Accepted)
                .expect("single attendee should patch");

        assert_eq!(
            patch
                .properties
                .get("participants/p1/participationStatus")
                .and_then(Value::as_str),
            Some("accepted")
        );
        assert!(!patch.properties.contains_key("participants"));
    }

    #[test]
    fn rsvp_patch_uses_authenticated_email_for_multi_attendee_event() {
        let participants = serde_json::from_value(json!({
            "p1": {
                "email": "one@example.test",
                "roles": {"attendee": true}
            },
            "p2": {
                "email": "Two@Example.Test",
                "roles": {"attendee": true}
            }
        }))
        .expect("participants");

        let patch = jmap_rsvp_patch_from_participants(
            Some(&participants),
            &["two@example.test".to_string()],
            RsvpStatus::Declined,
        )
        .expect("authenticated attendee should patch");

        assert_eq!(
            patch
                .properties
                .get("participants/p2/participationStatus")
                .and_then(Value::as_str),
            Some("declined")
        );
    }

    #[test]
    fn rsvp_patch_refuses_ambiguous_participants_without_authenticated_email() {
        let participants = serde_json::from_value(json!({
            "p1": {
                "email": "one@example.test",
                "roles": {"attendee": true}
            },
            "p2": {
                "email": "two@example.test",
                "roles": {"attendee": true}
            }
        }))
        .expect("participants");

        let error =
            jmap_rsvp_patch_from_participants(Some(&participants), &[], RsvpStatus::Accepted)
                .expect_err("ambiguous attendees should fail");

        assert!(error.contains("multiple attendee participants"));
    }

    #[test]
    fn create_payload_does_not_synthesize_uid() {
        let create = create_payload(&EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: Some("Planning".to_string()),
            description: None,
            location: None,
            start: time("2026-06-02T12:00:00Z"),
            end: time("2026-06-02T13:00:00Z"),
            is_all_day: false,
            status: EventStatus::Tentative,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        });

        assert!(!create.properties.contains_key("uid"));
        assert!(create.properties.contains_key("calendarIds"));
        assert_eq!(create.properties.get("status"), Some(&json!("tentative")));
    }

    #[test]
    fn visibility_maps_to_and_from_jscalendar_privacy() {
        assert_eq!(visibility(Some("public")), EventVisibility::Public);
        assert_eq!(visibility(Some("private")), EventVisibility::Private);
        assert_eq!(visibility(Some("secret")), EventVisibility::Confidential);
        assert_eq!(jmap_visibility(EventVisibility::Confidential), "secret");

        let patch = patch_with_patch_context(
            &EventPatch {
                visibility: Some(EventVisibility::Private),
                status: Some(EventStatus::Cancelled),
                ..EventPatch::default()
            },
            AccountOperation::EventUpdate,
        )
        .expect("patch");
        assert_eq!(patch.properties.get("privacy"), Some(&json!("private")));
        assert_eq!(patch.properties.get("status"), Some(&json!("cancelled")));
    }

    #[test]
    fn create_payload_writes_privacy() {
        let create = create_payload(&EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: None,
            description: None,
            location: None,
            start: time("2026-06-02T12:00:00Z"),
            end: time("2026-06-02T13:00:00Z"),
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Confidential,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        });

        assert_eq!(create.properties.get("privacy"), Some(&json!("secret")));
    }

    #[test]
    fn create_payload_writes_recurrence_rules_as_objects() {
        let create = create_payload(&EventCreate {
            calendar_id: CalendarId("cal".to_string()),
            title: None,
            description: None,
            location: None,
            start: time("2026-06-02T12:00:00Z"),
            end: time("2026-06-02T13:00:00Z"),
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence {
                rrule: Some("FREQ=DAILY;COUNT=2".to_string()),
                rdate: vec!["2026-06-03T12:00:00".to_string()],
                exdate: vec!["2026-06-04T12:00:00".to_string()],
                recurrence_id: None,
            },
        });

        let rules = create.properties["recurrenceRules"]
            .as_array()
            .expect("rules array");
        assert_eq!(rules[0]["frequency"].as_str(), Some("daily"));
        assert_eq!(rules[0]["count"].as_u64(), Some(2));
        assert_eq!(
            create.properties["recurrenceOverrides"]["2026-06-03T12:00:00"],
            json!({})
        );
        assert_eq!(
            create.properties["recurrenceOverrides"]["2026-06-04T12:00:00"]["excluded"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn absent_calendar_rights_are_not_writable() {
        assert!(!rights_can_write(None));
        assert!(!rights_can_delete(None));

        let rights = CalendarRights {
            may_read_free_busy: Some(true),
            may_read_items: Some(true),
            may_write_all: None,
            may_write_own: Some(true),
            may_update_private: None,
            may_rsvp: None,
            may_share: None,
            may_delete: Some(false),
        };
        assert!(rights_can_write(Some(&rights)));
        assert!(!rights_can_delete(Some(&rights)));
    }
}
