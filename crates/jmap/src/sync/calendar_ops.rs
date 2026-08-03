use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AttendeeRole, Calendar, CalendarEvent,
    CalendarId, CalendarProvenance, DiagnosticText, EventAttendee, EventAvailability, EventCreate,
    EventId, EventOrganizer, EventPatch, EventRange, EventRecurrence, EventReminder,
    EventSearchRequest, EventStatus, EventTime, EventVisibility, Page, ProtocolKind,
    ReminderRelativeTo, ReminderTrigger, RsvpStatus,
};
use jiff::tz::Offset;
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
        validate_shared_recurrence(&event.recurrence, AccountOperation::EventCreate)?;
        let mut set = CalendarEventSet::new();
        let create_id = set.create_item(jmap_create_from_event(&event));
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
        if let Some(recurrence) = &patch.recurrence {
            validate_shared_recurrence(recurrence, AccountOperation::EventUpdate)?;
        }
        let id = CalendarEventId::new(event.0);
        let jmap_patch = jmap_patch_from_event_patch(&patch, AccountOperation::EventUpdate)?;
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
    Ok(response
        .into_list()
        .into_iter()
        .map(event_from_jmap)
        .collect())
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

fn event_from_jmap(event: JmapCalendarEvent) -> CalendarEvent {
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
        timezone,
    };
    let (rdate, mut exdate) = recurrence_dates_from_overrides(event.recurrence_overrides());
    if let Some(dates) = event.excluded_dates() {
        exdate.extend(dates.keys().cloned());
    }
    let alerts = event.alerts();
    let reminders = reminders_from_alerts(alerts.as_value().copied());
    CalendarEvent {
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
        attendees: attendees(event.participants()),
        reminders,
        recurrence: EventRecurrence {
            rrule: event
                .recurrence_rules()
                .and_then(|rules| rules.first())
                .and_then(rrule_from_jmap_recurrence_rule),
            rdate,
            exdate,
            recurrence_id: event.recurrence_id().map(ToString::to_string),
        },
        html_link: None,
        raw_ical: None,
    }
}

fn jmap_create_from_event(event: &EventCreate) -> CalendarEventCreate {
    let mut create = CalendarEventCreate::new(None);
    write_event_create(&mut create, event);
    create
}

fn jmap_patch_from_event_patch(
    patch: &EventPatch,
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
            let all_day = patch.is_all_day.unwrap_or(false);
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
        out.participants(participants_from_attendees(attendees));
    }
    if let Some(recurrence) = &patch.recurrence {
        if let Some(rule) = recurrence
            .rrule
            .as_deref()
            .and_then(jmap_recurrence_rule_from_rrule)
        {
            out.recurrence_rules(vec![rule]);
        } else {
            out.set_property("recurrenceRules", Value::Null);
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

fn write_event_create(target: &mut CalendarEventCreate, event: &EventCreate) {
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
    if let Some(rule) = event
        .recurrence
        .rrule
        .as_deref()
        .and_then(jmap_recurrence_rule_from_rrule)
    {
        target.recurrence_rules(vec![rule]);
    }
    let overrides = recurrence_overrides_from_shared(&event.recurrence);
    if !overrides.is_empty() {
        target.recurrence_overrides(overrides);
    }
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
) -> (Vec<String>, Vec<String>) {
    let mut rdate = Vec::new();
    let mut exdate = Vec::new();
    let Some(overrides) = overrides else {
        return (rdate, exdate);
    };
    for (date, value) in overrides {
        let Some(object) = value.as_object() else {
            continue;
        };
        match object.get("excluded").and_then(Value::as_bool) {
            Some(true) if object.len() == 1 => exdate.push(date.clone()),
            None if object.is_empty() => rdate.push(date.clone()),
            _ => {}
        }
    }
    (rdate, exdate)
}

fn validate_shared_recurrence(
    recurrence: &EventRecurrence,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if recurrence
        .rrule
        .as_deref()
        .is_some_and(|rrule| jmap_recurrence_rule_from_rrule(rrule).is_none())
    {
        return Err(unsupported(
            operation,
            "JMAP shared recurrence contains RRULE fields unsupported by the JSCalendar mapper",
        ));
    }
    Ok(())
}

fn jmap_recurrence_rule_from_rrule(rrule: &str) -> Option<Value> {
    let mut object = Map::new();
    object.insert("@type".to_string(), json!("RecurrenceRule"));
    for part in rrule.split(';') {
        let (key, value) = part.split_once('=')?;
        match key.to_ascii_uppercase().as_str() {
            "FREQ" => {
                object.insert(
                    "frequency".to_string(),
                    Value::String(value.to_ascii_lowercase()),
                );
            }
            "INTERVAL" => {
                object.insert("interval".to_string(), json!(value.parse::<u64>().ok()?));
            }
            "COUNT" => {
                object.insert("count".to_string(), json!(value.parse::<u64>().ok()?));
            }
            "UNTIL" => {
                // JSCalendar `until` is a LocalDateTime: no `Z`, no offset.
                // An RFC 5545 UTC `UNTIL=...Z` cannot be normalized to local
                // time without the event's timeZone, which this mapper does
                // not see, so reject it (caught by `validate_shared_recurrence`
                // before payload construction) rather than emit a non-conformant
                // `Z`-suffixed `until`. Floating (no-`Z`) and date-only forms
                // pass through unchanged.
                if value.ends_with('Z') || value.ends_with('z') {
                    return None;
                }
                object.insert("until".to_string(), Value::String(value.to_string()));
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

fn rrule_from_jmap_recurrence_rule(rule: &Value) -> Option<String> {
    let object = rule.as_object()?;
    let mut parts = Vec::new();
    parts.push(format!(
        "FREQ={}",
        object.get("frequency")?.as_str()?.to_ascii_uppercase()
    ));
    if let Some(interval) = object.get("interval").and_then(Value::as_u64) {
        parts.push(format!("INTERVAL={interval}"));
    }
    if let Some(count) = object.get("count").and_then(Value::as_u64) {
        parts.push(format!("COUNT={count}"));
    }
    if let Some(until) = object.get("until").and_then(Value::as_str) {
        parts.push(format!("UNTIL={until}"));
    }
    if let Some(by_day) = object.get("byDay").and_then(rrule_by_day) {
        parts.push(format!("BYDAY={by_day}"));
    }
    if let Some(by_month) = object.get("byMonth").and_then(rrule_integer_list) {
        parts.push(format!("BYMONTH={by_month}"));
    }
    if let Some(by_month_day) = object.get("byMonthDay").and_then(rrule_integer_list) {
        parts.push(format!("BYMONTHDAY={by_month_day}"));
    }
    Some(parts.join(";"))
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
        return Some((None, value));
    }
    let split = value.len().checked_sub(2)?;
    let nth = value[..split].parse::<i64>().ok()?;
    Some((Some(nth), &value[split..]))
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
            let prefix = object
                .get("nthOfPeriod")
                .and_then(Value::as_i64)
                .map(|nth| nth.to_string())
                .unwrap_or_default();
            Some(format!(
                "{}{}",
                prefix,
                object.get("day")?.as_str()?.to_ascii_uppercase()
            ))
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

fn attendees(participants: Option<&Map<String, Value>>) -> Vec<EventAttendee> {
    participants
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| {
            let object = value.as_object()?;
            Some(EventAttendee {
                email: object.get("email")?.as_str()?.to_string(),
                name: object
                    .get("name")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                role: attendee_role(object.get("roles")),
                status: rsvp_status(object.get("participationStatus").and_then(Value::as_str)),
            })
        })
        .collect()
}

fn attendee_role(value: Option<&Value>) -> AttendeeRole {
    let Some(roles) = value.and_then(Value::as_object) else {
        return AttendeeRole::Unknown;
    };
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
    }
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
    if is_all_day || value.len() == 10 {
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
        return time.value.clone();
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
    event_start <= range_end && event_end >= range_start
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
        && let Ok(date) = civil::Date::strptime("%Y-%m-%d", start)
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

fn rsvp_status(value: Option<&str>) -> RsvpStatus {
    match value.unwrap_or_default() {
        "accepted" => RsvpStatus::Accepted,
        "declined" => RsvpStatus::Declined,
        "tentative" => RsvpStatus::Tentative,
        "delegated" => RsvpStatus::Delegated,
        "needs-action" => RsvpStatus::NeedsAction,
        _ => RsvpStatus::Unknown,
    }
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
        let create = jmap_create_from_event(&EventCreate {
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
        let patch = jmap_patch_from_event_patch(
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
            let error = jmap_patch_from_event_patch(&patch, AccountOperation::EventUpdate)
                .expect_err("single-bound time patch should reject");
            assert!(matches!(
                error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
            ));
        }
    }

    #[test]
    fn recurrence_rule_rejects_utc_until() {
        assert!(jmap_recurrence_rule_from_rrule("FREQ=DAILY;UNTIL=20260101T000000Z").is_none());
        // Floating and date-only UNTIL pass through unchanged.
        let floating = jmap_recurrence_rule_from_rrule("FREQ=DAILY;UNTIL=20260101T000000")
            .expect("floating until");
        assert_eq!(floating["until"].as_str(), Some("20260101T000000"));
        let date_only =
            jmap_recurrence_rule_from_rrule("FREQ=DAILY;UNTIL=20260101").expect("date until");
        assert_eq!(date_only["until"].as_str(), Some("20260101"));
    }

    #[test]
    fn event_patch_clears_nullable_fields_with_null() {
        let patch = jmap_patch_from_event_patch(
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
        let rrule = rrule_from_jmap_recurrence_rule(&json!({
            "frequency": "monthly",
            "interval": 1,
            "count": 3,
            "byDay": [{"day": "tu", "nthOfPeriod": 2}],
            "byMonthDay": [14]
        }))
        .expect("jscalendar rule should convert");

        assert_eq!(
            rrule,
            "FREQ=MONTHLY;INTERVAL=1;COUNT=3;BYDAY=2TU;BYMONTHDAY=14"
        );
    }

    #[test]
    fn recurrence_patch_clears_or_writes_recurrence_rules() {
        let clear = jmap_patch_from_event_patch(
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

        let write = jmap_patch_from_event_patch(
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

    #[test]
    fn recurrence_validation_rejects_unsupported_rrule_parts() {
        let recurrence = EventRecurrence {
            rrule: Some("FREQ=DAILY;BYHOUR=9".to_string()),
            rdate: Vec::new(),
            exdate: Vec::new(),
            recurrence_id: None,
        };

        let create = validate_shared_recurrence(&recurrence, AccountOperation::EventCreate)
            .expect_err("unsupported create recurrence should fail");
        assert!(matches!(
            create.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));

        let update = validate_shared_recurrence(&recurrence, AccountOperation::EventUpdate)
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
            "2026-06-04T12:00:00": {"excluded": true},
            "2026-06-05T12:00:00": {"title": "Moved"}
        }))
        .expect("override map");
        let (rdate, exdate) = recurrence_dates_from_overrides(Some(&overrides));

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

        let attendees = attendees(Some(&participants));

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
    }

    #[test]
    fn create_payload_writes_organizer_as_owner_participant() {
        let create = jmap_create_from_event(&EventCreate {
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
        let create = jmap_create_from_event(&EventCreate {
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

        let patch = jmap_patch_from_event_patch(
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
        let create = jmap_create_from_event(&EventCreate {
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
        let create = jmap_create_from_event(&EventCreate {
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
