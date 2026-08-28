use std::collections::HashSet;
use std::sync::Arc;

use bifrost_types::{
    AccountErrorBuilder, AccountErrorKind, AttemptCause, AttendeeRole, Calendar, CalendarEvent,
    CalendarId, CalendarProvenance, Cause, DiagnosticText, EventAttendee, EventAvailability,
    EventCreate, EventId, EventOrganizer, EventPatch, EventRange, EventRecurrence,
    EventSearchRequest, EventStatus, EventTime, EventVisibility, Page, Protocol, ProtocolErrorKind,
    ProtocolKind, RsvpStatus, TransmissionState, WireCause,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::client::GmailClient;

use super::error::{self, GmailErrorContext};
use super::non_empty;
use bifrost_types::{AccountError, AccountFuture, AccountOperation};

const EVENT_ID_SEPARATOR: &str = "::";
const CALENDAR_LIST_PAGE_SIZE: u16 = 250;
const MAX_CALENDAR_LIST_PAGES: usize = 10_000;

pub(crate) fn calendars_list(
    client: Arc<GmailClient>,
) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
    Box::pin(async move {
        let base_url = format!(
            "{}/users/me/calendarList?maxResults={CALENDAR_LIST_PAGE_SIZE}",
            client.calendar_base()
        );
        let mut calendars = Vec::new();
        let mut page_token = None;
        let mut seen_tokens = HashSet::new();
        for _ in 0..MAX_CALENDAR_LIST_PAGES {
            let mut url = base_url.clone();
            if let Some(token) = page_token.as_deref() {
                url.push_str("&pageToken=");
                url.push_str(&bifrost_net::url::encode_query_value(token));
            }
            let response: CalendarListResponse = client
                .get(&url)
                .await
                .map_err(|error| collection_error(error, AccountOperation::CalendarsList))?;
            calendars.extend(
                response
                    .items
                    .unwrap_or_default()
                    .into_iter()
                    .map(calendar_from_google),
            );
            let Some(next_token) = response.next_page_token else {
                return Ok(calendars);
            };
            if !seen_tokens.insert(next_token.clone()) {
                return Err(calendar_contract_error(
                    AccountOperation::CalendarsList,
                    "Google Calendar calendarList repeated a page token".to_string(),
                ));
            }
            page_token = Some(next_token);
        }
        Err(calendar_contract_error(
            AccountOperation::CalendarsList,
            format!("Google Calendar calendarList exceeded {MAX_CALENDAR_LIST_PAGES} pages"),
        ))
    })
}

pub(crate) fn events_in_range(
    client: Arc<GmailClient>,
    range: EventRange,
) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
    Box::pin(async move {
        let page_token = range
            .page_cursor
            .map(String::from_utf8)
            .transpose()
            .map_err(|error| {
                local_error_with_field(
                    AccountOperation::EventsInRange,
                    "eventPageToken",
                    error.to_string(),
                )
            })?;
        let calendar_id = range.calendar_id.0;
        let encoded = bifrost_net::url::encode_path_component(&calendar_id);
        let time_min = google_range_bound(&range.start, "timeMin")?;
        let time_max = google_range_bound(&range.end, "timeMax")?;
        let mut url = format!(
            "{}/calendars/{encoded}/events?singleEvents=true&showDeleted=true&orderBy=startTime&timeMin={}&timeMax={}",
            client.calendar_base(),
            bifrost_net::url::encode_query_value(&time_min),
            bifrost_net::url::encode_query_value(&time_max)
        );
        if let Some(limit) = range.limit {
            url.push_str("&maxResults=");
            url.push_str(&limit.min(2500).to_string());
        }
        if let Some(token) = page_token {
            url.push_str("&pageToken=");
            url.push_str(&bifrost_net::url::encode_query_value(&token));
        }
        let response: EventsResponse = client
            .get(&url)
            .await
            .map_err(|error| collection_error(error, AccountOperation::EventsInRange))?;
        page_from_events(calendar_id, response, AccountOperation::EventsInRange)
    })
}

pub(crate) fn get(
    client: Arc<GmailClient>,
    event: EventId,
) -> AccountFuture<Result<CalendarEvent, AccountError>> {
    Box::pin(async move {
        let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventGet)?;
        let url = event_url(client.calendar_base(), &calendar_id, &event_id);
        let event: GoogleEvent = client
            .get(&url)
            .await
            .map_err(|error| event_error(error, AccountOperation::EventGet, event.0.clone()))?;
        event_from_google(calendar_id, event, AccountOperation::EventGet)
    })
}

pub(crate) fn create(
    client: Arc<GmailClient>,
    event: EventCreate,
) -> AccountFuture<Result<EventId, AccountError>> {
    Box::pin(async move {
        reject_create_organizer(&event)?;
        let calendar_id = event.calendar_id.0.clone();
        let encoded = bifrost_net::url::encode_path_component(&calendar_id);
        let url = format!("{}/calendars/{encoded}/events", client.calendar_base());
        let created: GoogleEvent = client
            .post(&url, &google_event_from_create(&event))
            .await
            .map_err(|error| collection_error(error, AccountOperation::EventCreate))?;
        let id = created
            .id
            .ok_or_else(|| local_error(AccountOperation::EventCreate, "missing event id".into()))?;
        Ok(EventId(join_event_id(&calendar_id, &id)))
    })
}

fn reject_create_organizer(event: &EventCreate) -> Result<(), AccountError> {
    if event.organizer.is_some() {
        return Err(error::into_account_error(
            crate::error::Error::unsupported_with(
                AccountOperation::EventCreate,
                "Google Calendar organizer is server-derived on event creation",
            ),
            GmailErrorContext::calendar_collection(AccountOperation::EventCreate),
        ));
    }
    Ok(())
}

pub(crate) fn update(
    client: Arc<GmailClient>,
    event: EventId,
    patch: EventPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (mut calendar_id, native_event_id) =
            split_event_id(&event.0, AccountOperation::EventUpdate)?;
        let has_field_patch = event_patch_has_non_move_fields(&patch);
        // Local, patch-only validation. It cannot see post-move state,
        // so hoisting it ahead of the move is behaviour-preserving for
        // any patch that would have passed, and it stops a patch that
        // could never be applied from stranding the event in the
        // destination calendar.
        if has_field_patch {
            reject_unexpressible_all_day_patch(&patch)?;
        }
        let mut moved = false;
        if let Some(target_calendar) = patch.calendar_id.as_ref()
            && target_calendar.0 != calendar_id
        {
            let _: GoogleEvent = client
                .post(
                    &event_move_url(
                        client.calendar_base(),
                        &calendar_id,
                        &native_event_id,
                        &target_calendar.0,
                    ),
                    &json!({}),
                )
                .await
                .map_err(|error| {
                    event_error(error, AccountOperation::EventUpdate, event.0.clone())
                })?;
            calendar_id = target_calendar.0.clone();
            moved = true;
        }
        if !has_field_patch {
            return Ok(());
        }
        let url = event_url(client.calendar_base(), &calendar_id, &native_event_id);
        // After a move the event lives under the destination calendar, so
        // the composite id the caller passed in no longer addresses it.
        // Scope the patch failure to where the event actually is - a
        // reconcile directive that names the source calendar sends the
        // consumer to look at a resource that is no longer there.
        let target_event_id = join_event_id(&calendar_id, &native_event_id);
        let _: GoogleEvent = client
            .patch(&url, &google_event_from_patch(&patch))
            .await
            .map_err(|error| {
                let error = event_error(error, AccountOperation::EventUpdate, target_event_id);
                if moved {
                    event_move_patch_error(&error)
                } else {
                    error
                }
            })?;
        Ok(())
    })
}

pub(crate) fn delete(
    client: Arc<GmailClient>,
    event: EventId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventDelete)?;
        client
            .delete(&event_url(client.calendar_base(), &calendar_id, &event_id))
            .await
            .map_err(|error| event_error(error, AccountOperation::EventDelete, event.0.clone()))
    })
}

pub(crate) fn rsvp(
    client: Arc<GmailClient>,
    self_email: String,
    event: EventId,
    status: RsvpStatus,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventRsvp)?;
        let mut current: GoogleEvent = client
            .get(&event_url(client.calendar_base(), &calendar_id, &event_id))
            .await
            .map_err(|error| event_error(error, AccountOperation::EventRsvp, event.0.clone()))?;
        rsvp_google_attendees_for_self(&mut current.attendees, &self_email, status)?;
        let _: GoogleEvent = client
            .patch(
                &event_url(client.calendar_base(), &calendar_id, &event_id),
                &GoogleEventPatch {
                    attendees: current.attendees,
                    ..GoogleEventPatch::default()
                },
            )
            .await
            .map_err(|error| event_error(error, AccountOperation::EventRsvp, event.0.clone()))?;
        Ok(())
    })
}

pub(crate) fn search(
    client: Arc<GmailClient>,
    request: EventSearchRequest,
) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
    Box::pin(async move {
        if let Some(calendar_id) = request.calendar_id {
            return search_one_calendar(
                &client,
                calendar_id.0,
                &request.query,
                request.limit,
                decode_page_token(request.page_cursor, AccountOperation::EventSearch)?,
            )
            .await;
        }

        let calendars = calendars_list(Arc::clone(&client)).await?;
        let calendar_ids = calendars
            .into_iter()
            .map(|calendar| calendar.id.0)
            .collect::<Vec<_>>();
        let (mut index, mut page_token) =
            decode_cross_calendar_cursor(request.page_cursor, &calendar_ids)?;
        let limit = request
            .limit
            .and_then(|limit| usize::try_from(limit).ok())
            .unwrap_or(250)
            .min(2500);
        let mut items = Vec::new();
        while let Some(calendar_id) = calendar_ids.get(index) {
            let remaining = limit.saturating_sub(items.len());
            if remaining == 0 {
                return Ok(Page {
                    items,
                    next_cursor: Some(encode_cross_calendar_cursor(calendar_id, &[])),
                    estimated_total: None,
                    failed_ids: Vec::new(),
                    skipped_scopes: Vec::new(),
                });
            }
            let page = search_one_calendar(
                &client,
                calendar_id.clone(),
                &request.query,
                Some(u32::try_from(remaining).unwrap_or(2500)),
                page_token.take(),
            )
            .await?;
            // Google honours `maxResults`, but guard the cap defensively so
            // a loose page can never push the aggregate over the request
            // limit. Any clipped tail is recoverable: the page carried a
            // next_cursor only when more remained, and the boundary cursor
            // re-enters this calendar otherwise.
            let next_token = page.next_cursor;
            for item in page.items {
                if items.len() >= limit {
                    break;
                }
                items.push(item);
            }
            if let Some(next_token) = next_token {
                return Ok(Page {
                    items,
                    next_cursor: Some(encode_cross_calendar_cursor(calendar_id, &next_token)),
                    estimated_total: None,
                    failed_ids: Vec::new(),
                    skipped_scopes: Vec::new(),
                });
            }
            index += 1;
            if items.len() >= limit {
                return Ok(Page {
                    items,
                    next_cursor: calendar_ids
                        .get(index)
                        .map(|calendar_id| encode_cross_calendar_cursor(calendar_id, &[])),
                    estimated_total: None,
                    failed_ids: Vec::new(),
                    skipped_scopes: Vec::new(),
                });
            }
        }
        Ok(Page {
            items,
            next_cursor: None,
            estimated_total: None,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

async fn search_one_calendar(
    client: &GmailClient,
    calendar_id: String,
    query: &str,
    limit: Option<u32>,
    page_token: Option<String>,
) -> Result<Page<CalendarEvent>, AccountError> {
    let encoded = bifrost_net::url::encode_path_component(&calendar_id);
    let mut url = format!(
        "{}/calendars/{encoded}/events?singleEvents=true&orderBy=startTime&q={}",
        client.calendar_base(),
        bifrost_net::url::encode_query_value(query)
    );
    if let Some(limit) = limit {
        url.push_str("&maxResults=");
        url.push_str(&limit.min(2500).to_string());
    }
    if let Some(token) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&bifrost_net::url::encode_query_value(&token));
    }
    let response: EventsResponse = client
        .get(&url)
        .await
        .map_err(|error| collection_error(error, AccountOperation::EventSearch))?;
    page_from_events(calendar_id, response, AccountOperation::EventSearch)
}

fn page_from_events(
    calendar_id: String,
    response: EventsResponse,
    operation: AccountOperation,
) -> Result<Page<CalendarEvent>, AccountError> {
    let items = response
        .items
        .unwrap_or_default()
        .into_iter()
        .map(|event| event_from_google(calendar_id.clone(), event, operation))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Page {
        items,
        next_cursor: response.next_page_token.map(String::into_bytes),
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

fn decode_page_token(
    page_cursor: Option<Vec<u8>>,
    operation: AccountOperation,
) -> Result<Option<String>, AccountError> {
    page_cursor
        .map(String::from_utf8)
        .transpose()
        .map_err(|error| local_error_with_field(operation, "eventPageToken", error.to_string()))
}

fn encode_cross_calendar_cursor(calendar_id: &str, page_token: &[u8]) -> Vec<u8> {
    let mut cursor = calendar_id.as_bytes().to_vec();
    cursor.push(b'\n');
    cursor.extend_from_slice(page_token);
    cursor
}

fn decode_cross_calendar_cursor(
    page_cursor: Option<Vec<u8>>,
    calendar_ids: &[String],
) -> Result<(usize, Option<String>), AccountError> {
    let Some(cursor) = decode_page_token(page_cursor, AccountOperation::EventSearch)? else {
        return Ok((0, None));
    };
    let (calendar_id, page_token) = cursor.split_once('\n').ok_or_else(|| {
        local_error_with_field(
            AccountOperation::EventSearch,
            "eventPageToken",
            "cross-calendar search cursor is missing calendar id".to_string(),
        )
    })?;
    let index = calendar_ids
        .iter()
        .position(|candidate| candidate == calendar_id)
        .ok_or_else(|| {
            local_error_with_field(
                AccountOperation::EventSearch,
                "eventPageToken",
                "cross-calendar search cursor references an unknown calendar".to_string(),
            )
        })?;
    let page_token = (!page_token.is_empty()).then(|| page_token.to_string());
    Ok((index, page_token))
}

fn calendar_from_google(calendar: GoogleCalendarListEntry) -> Calendar {
    let native = calendar.id.unwrap_or_default();
    let access = calendar.access_role.unwrap_or_default();
    let can_write = matches!(access.as_str(), "owner" | "writer");
    Calendar {
        id: CalendarId(native.clone()),
        native_id: native.clone(),
        name: calendar.summary.unwrap_or_else(|| "Calendar".to_string()),
        color: calendar.background_color,
        provenance: CalendarProvenance {
            provider: ProtocolKind::Gmail,
            native,
            calendar_native: None,
        },
        is_default: calendar.primary.unwrap_or(false),
        can_create_events: can_write,
        can_update_events: can_write,
        can_delete_events: can_write,
    }
}

fn event_from_google(
    calendar_id: String,
    event: GoogleEvent,
    operation: AccountOperation,
) -> Result<CalendarEvent, AccountError> {
    let event_id = event.id.unwrap_or_default();
    let native = join_event_id(&calendar_id, &event_id);
    let cancelled_instance_time = (event.status.as_deref() == Some("cancelled"))
        .then(|| event.original_start_time.clone())
        .flatten();
    // `is_all_day` must be read off the EFFECTIVE start, not `event.start`.
    // A cancelled instance of an all-day recurrence arrives with no `start`
    // at all and only `originalStartTime.date`, so deciding all-day-ness
    // from `event.start` alone yields a date-valued start and end paired
    // with `is_all_day: false` - a shape the shared surface treats as a
    // timed event and renders at midnight.
    let effective_start = event.start.or_else(|| cancelled_instance_time.clone());
    let is_all_day = effective_start
        .as_ref()
        .is_some_and(|time| time.date.is_some() && time.date_time.is_none());
    let start = effective_start.map(event_time).ok_or_else(|| {
        local_error_with_field(operation, "start", "Google event missing start".to_string())
    })?;
    let end = event
        .end
        .or(cancelled_instance_time)
        .map(event_time)
        .ok_or_else(|| {
            local_error_with_field(operation, "end", "Google event missing end".to_string())
        })?;
    Ok(CalendarEvent {
        id: EventId(native.clone()),
        calendar_id: CalendarId(calendar_id.clone()),
        native_id: native.clone(),
        uid: event.i_cal_uid,
        etag: event.etag,
        provenance: CalendarProvenance {
            provider: ProtocolKind::Gmail,
            native,
            calendar_native: Some(calendar_id),
        },
        title: event.summary,
        description: event.description,
        location: event.location,
        start,
        end,
        is_all_day,
        status: event_status(event.status.as_deref()),
        availability: availability(event.transparency.as_deref()),
        visibility: visibility(event.visibility.as_deref()),
        self_response: RsvpStatus::Unknown,
        organizer: event.organizer.and_then(|organizer| {
            Some(EventOrganizer {
                email: organizer.email?,
                name: organizer.display_name,
            })
        }),
        attendees: event
            .attendees
            .unwrap_or_default()
            .into_iter()
            .filter_map(attendee_from_google)
            .collect(),
        // Google Calendar reminders are not projected onto the shared read
        // surface yet; other providers (CalDAV VALARM, JMAP alerts) supply
        // them.
        reminders: Vec::new(),
        recurrence: EventRecurrence {
            rrule: event
                .recurrence
                .as_ref()
                .and_then(|values| values.iter().find_map(|value| value.strip_prefix("RRULE:")))
                .map(ToString::to_string),
            rdate: prefixed_values(event.recurrence.as_deref(), "RDATE:"),
            exdate: prefixed_values(event.recurrence.as_deref(), "EXDATE:"),
            recurrence_id: event
                .original_start_time
                .map(event_time)
                .map(|time| time.value),
        },
        html_link: event.html_link,
        raw_ical: None,
    })
}

fn google_event_from_create(event: &EventCreate) -> GoogleEventPatch {
    GoogleEventPatch {
        summary: event.title.clone().map(Some),
        description: event.description.clone().map(Some),
        location: event.location.clone().map(Some),
        start: Some(google_time(&event.start, event.is_all_day)),
        end: Some(google_time(&event.end, event.is_all_day)),
        status: Some(google_event_status(event.status).to_string()),
        attendees: non_empty(event.attendees.iter().map(google_attendee)),
        recurrence: recurrence_lines(&event.recurrence),
        visibility: Some(match event.visibility {
            EventVisibility::Private => "private".to_string(),
            EventVisibility::Public => "public".to_string(),
            EventVisibility::Confidential => "confidential".to_string(),
            EventVisibility::Default => "default".to_string(),
            _ => "default".to_string(),
        }),
        transparency: Some(transparency(event.availability).to_string()),
    }
}

fn google_event_from_patch(patch: &EventPatch) -> GoogleEventPatch {
    GoogleEventPatch {
        summary: patch.title.clone(),
        description: patch.description.clone(),
        location: patch.location.clone(),
        start: patch
            .start
            .as_ref()
            .map(|time| google_time(time, patch.is_all_day.unwrap_or(is_ymd_date(&time.value)))),
        end: patch
            .end
            .as_ref()
            .map(|time| google_time(time, patch.is_all_day.unwrap_or(is_ymd_date(&time.value)))),
        status: patch
            .status
            .map(|status| google_event_status(status).to_string()),
        attendees: patch
            .attendees
            .as_ref()
            .map(|attendees| attendees.iter().map(google_attendee).collect()),
        // A present recurrence patch always emits the key. An empty
        // `EventRecurrence` produces an empty array, which is how the
        // Google API clears an existing RRULE/RDATE/EXDATE; an absent
        // patch omits the key and leaves the server value untouched.
        recurrence: patch
            .recurrence
            .as_ref()
            .map(|recurrence| recurrence_lines(recurrence).unwrap_or_default()),
        visibility: patch.visibility.map(|visibility| match visibility {
            EventVisibility::Private => "private".to_string(),
            EventVisibility::Public => "public".to_string(),
            EventVisibility::Confidential => "confidential".to_string(),
            EventVisibility::Default => "default".to_string(),
            _ => "default".to_string(),
        }),
        transparency: patch
            .availability
            .map(|availability| transparency(availability).to_string()),
    }
}

fn attendee_from_google(attendee: GoogleAttendee) -> Option<EventAttendee> {
    Some(EventAttendee {
        email: attendee.email?,
        name: attendee.display_name,
        role: attendee.resource.map_or_else(
            || {
                attendee
                    .optional
                    .map_or(AttendeeRole::Required, |optional| {
                        if optional {
                            AttendeeRole::Optional
                        } else {
                            AttendeeRole::Required
                        }
                    })
            },
            |resource| {
                if resource {
                    AttendeeRole::Resource
                } else {
                    AttendeeRole::Required
                }
            },
        ),
        status: rsvp_status(attendee.response_status.as_deref()),
    })
}

fn google_attendee(attendee: &EventAttendee) -> GoogleAttendee {
    GoogleAttendee {
        email: Some(attendee.email.clone()),
        display_name: attendee.name.clone(),
        response_status: Some(rsvp_value(attendee.status).to_string()),
        optional: Some(matches!(attendee.role, AttendeeRole::Optional)),
        resource: matches!(attendee.role, AttendeeRole::Resource).then_some(true),
        extra: Map::new(),
    }
}

fn google_time(time: &EventTime, all_day: bool) -> GoogleEventTime {
    if all_day {
        GoogleEventTime {
            date: Some(time.value.clone()),
            date_time: None,
            time_zone: time.timezone.clone(),
        }
    } else {
        GoogleEventTime {
            date: None,
            date_time: Some(time.value.clone()),
            time_zone: time.timezone.clone(),
        }
    }
}

fn google_range_bound(time: &EventTime, field: &'static str) -> Result<String, AccountError> {
    if is_ymd_date(&time.value) {
        return Ok(format!("{}T00:00:00Z", time.value));
    }
    if has_rfc3339_offset(&time.value) {
        return Ok(time.value.clone());
    }
    if time
        .timezone
        .as_deref()
        .is_some_and(|timezone| matches!(timezone, "UTC" | "Etc/UTC" | "Etc/GMT"))
    {
        return Ok(format!("{}Z", time.value));
    }
    Err(local_error_with_field(
        AccountOperation::EventsInRange,
        field,
        "Google Calendar range bound requires an RFC 3339 offset".to_string(),
    ))
}

fn is_ymd_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
}

fn has_rfc3339_offset(value: &str) -> bool {
    if value.ends_with('Z') {
        return true;
    }
    let Some(time_index) = value.find('T') else {
        return false;
    };
    let Some(offset) = value.get(value.len().saturating_sub(6)..) else {
        return false;
    };
    value.len() > time_index + 6
        && (offset.starts_with('+') || offset.starts_with('-'))
        && offset.as_bytes()[3] == b':'
        && offset
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 0 || index == 3 || byte.is_ascii_digit())
}

fn event_time(time: GoogleEventTime) -> EventTime {
    EventTime {
        value: time.date_time.or(time.date).unwrap_or_default(),
        timezone: time.time_zone,
    }
}

fn event_url(base: &str, calendar_id: &str, event_id: &str) -> String {
    format!(
        "{base}/calendars/{}/events/{}",
        bifrost_net::url::encode_path_component(calendar_id),
        bifrost_net::url::encode_path_component(event_id)
    )
}

fn event_move_url(
    base: &str,
    calendar_id: &str,
    event_id: &str,
    target_calendar_id: &str,
) -> String {
    format!(
        "{}/move?destination={}",
        event_url(base, calendar_id, event_id),
        // Query encoder, not the path encoder: the two differ on the complete
        // `.` and `..` components, which the path encoder double-escapes
        // against WHATWG path navigation. A calendar id is a query value here.
        bifrost_net::url::encode_query_value(target_calendar_id)
    )
}

// Reclassify a destination-side PATCH failure that followed a
// successful `events.move`. The primary kind changes, so the error
// model requires a fresh builder rather than `into_builder`, which is
// a decoration path only. Everything a consumer or a support export
// would otherwise lose is copied across by hand: the scope (which now
// names the event in its destination calendar), the provenance, the
// transport diagnostics, the tagged diagnostic text, and the whole
// original cause chain as secondary evidence. A `Reconcile` directive
// that does not say what to reconcile is barely better than the silent
// partial write it replaces.
fn event_move_patch_error(error: &AccountError) -> AccountError {
    let consented = error.support_consented();
    let telemetry = &consented.telemetry;
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: telemetry.protocol.unwrap_or(Protocol::Gmail),
            detail: Some(DiagnosticText::support_only(
                "event move succeeded but the destination field patch failed",
            )),
        }),
    )
    .protocol(telemetry.protocol.unwrap_or(Protocol::Gmail))
    .operation(AccountOperation::EventUpdate)
    // The generic known-id EventUpdate is an absolute-state write and is
    // idempotent. This composite path has already completed events.move,
    // however, so replaying the whole request would apply a second move.
    .idempotency_override(false)
    .status(telemetry.status)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )));
    if let Some(scope) = consented.scope {
        builder = builder.scope(scope.clone());
    }
    if let Some(provider) = telemetry.provider {
        builder = builder.provider(provider);
    }
    if let Some(request_id) = telemetry.request_id {
        builder = builder.request_id(request_id);
    }
    if let Some(trace_id) = telemetry.trace_id {
        builder = builder.trace_id(trace_id);
    }
    if let Some(native_code) = telemetry.native_code {
        builder = builder.native_code(native_code);
    }
    for text in &consented.user_safe_text {
        builder = builder.text(DiagnosticText::user_safe(*text));
    }
    for text in &consented.support_text {
        builder = builder.text(DiagnosticText::support_only(*text));
    }
    for cause in error.chain().iter() {
        builder = builder.push_cause(cause.clone());
    }
    builder
        .try_build()
        .expect("valid Calendar move partial-response classification")
}

fn event_patch_has_non_move_fields(patch: &EventPatch) -> bool {
    patch.title.is_some()
        || patch.description.is_some()
        || patch.location.is_some()
        || patch.start.is_some()
        || patch.end.is_some()
        || patch.is_all_day.is_some()
        || patch.availability.is_some()
        || patch.visibility.is_some()
        || patch.attendees.is_some()
        || patch.recurrence.is_some()
}

// Google Calendar carries the timed/all-day distinction in the shape of
// the `start`/`end` objects (`date` vs `dateTime`), not in a standalone
// flag. An `is_all_day` flip can only be expressed when the patch also
// carries the start/end values to re-emit under the new shape. A patch
// that sets `is_all_day` without both bounds would issue a PATCH that
// silently never converts the event, so reject it rather than accept a
// no-op (reject-not-drop).
fn reject_unexpressible_all_day_patch(patch: &EventPatch) -> Result<(), AccountError> {
    if patch.is_all_day.is_some() && !(patch.start.is_some() && patch.end.is_some()) {
        return Err(error::into_account_error(
            crate::error::Error::unsupported_with(
                AccountOperation::EventUpdate,
                "Google Calendar all-day conversion requires both start and end in the same patch",
            ),
            GmailErrorContext::calendar_collection(AccountOperation::EventUpdate),
        ));
    }
    Ok(())
}

fn join_event_id(calendar_id: &str, event_id: &str) -> String {
    format!("{calendar_id}{EVENT_ID_SEPARATOR}{event_id}")
}

fn split_event_id(
    value: &str,
    operation: AccountOperation,
) -> Result<(String, String), AccountError> {
    value
        .split_once(EVENT_ID_SEPARATOR)
        .map(|(calendar, event)| (calendar.to_string(), event.to_string()))
        .ok_or_else(|| {
            local_error(
                operation,
                "Google event id is missing calendar id".to_string(),
            )
        })
}

fn recurrence_lines(recurrence: &EventRecurrence) -> Option<Vec<String>> {
    let mut lines = Vec::new();
    if let Some(rrule) = &recurrence.rrule {
        lines.push(format!("RRULE:{rrule}"));
    }
    lines.extend(
        recurrence
            .rdate
            .iter()
            .map(|value| format!("RDATE:{value}")),
    );
    lines.extend(
        recurrence
            .exdate
            .iter()
            .map(|value| format!("EXDATE:{value}")),
    );
    (!lines.is_empty()).then_some(lines)
}

fn prefixed_values(values: Option<&[String]>, prefix: &str) -> Vec<String> {
    values
        .unwrap_or_default()
        .iter()
        .filter_map(|value| value.strip_prefix(prefix).map(ToString::to_string))
        .collect()
}

fn event_status(value: Option<&str>) -> EventStatus {
    match value.unwrap_or_default() {
        "confirmed" => EventStatus::Confirmed,
        "tentative" => EventStatus::Tentative,
        "cancelled" => EventStatus::Cancelled,
        _ => EventStatus::Unknown,
    }
}

fn google_event_status(value: EventStatus) -> &'static str {
    match value {
        EventStatus::Tentative => "tentative",
        EventStatus::Cancelled => "cancelled",
        EventStatus::Confirmed | EventStatus::Unknown => "confirmed",
        _ => "confirmed",
    }
}

fn visibility(value: Option<&str>) -> EventVisibility {
    match value.unwrap_or_default() {
        "private" => EventVisibility::Private,
        "public" => EventVisibility::Public,
        "confidential" => EventVisibility::Confidential,
        _ => EventVisibility::Default,
    }
}

fn availability(value: Option<&str>) -> EventAvailability {
    match value.unwrap_or_default() {
        "transparent" => EventAvailability::Free,
        "opaque" => EventAvailability::Busy,
        _ => EventAvailability::Unknown,
    }
}

fn transparency(value: EventAvailability) -> &'static str {
    match value {
        EventAvailability::Free => "transparent",
        EventAvailability::Busy | EventAvailability::Tentative | EventAvailability::OutOfOffice => {
            "opaque"
        }
        EventAvailability::Unknown => "opaque",
        _ => "opaque",
    }
}

fn rsvp_status(value: Option<&str>) -> RsvpStatus {
    match value.unwrap_or_default() {
        "accepted" => RsvpStatus::Accepted,
        "declined" => RsvpStatus::Declined,
        "tentative" => RsvpStatus::Tentative,
        "needsAction" => RsvpStatus::NeedsAction,
        _ => RsvpStatus::Unknown,
    }
}

fn rsvp_value(value: RsvpStatus) -> &'static str {
    match value {
        RsvpStatus::Accepted => "accepted",
        RsvpStatus::Declined => "declined",
        RsvpStatus::Tentative => "tentative",
        RsvpStatus::NeedsAction | RsvpStatus::Unknown => "needsAction",
        RsvpStatus::Delegated => "needsAction",
        _ => "needsAction",
    }
}

fn rsvp_google_attendees_for_self(
    attendees: &mut Option<Vec<GoogleAttendee>>,
    self_email: &str,
    status: RsvpStatus,
) -> Result<(), AccountError> {
    let Some(attendees) = attendees.as_mut() else {
        return Err(local_error(
            AccountOperation::EventRsvp,
            "Google Calendar event does not contain the account attendee".to_string(),
        ));
    };
    let Some(attendee) = attendees.iter_mut().find(|attendee| {
        attendee
            .email
            .as_deref()
            .is_some_and(|email| email.eq_ignore_ascii_case(self_email))
    }) else {
        return Err(local_error(
            AccountOperation::EventRsvp,
            "Google Calendar event does not contain the account attendee".to_string(),
        ));
    };
    attendee.response_status = Some(rsvp_value(status).to_string());
    Ok(())
}

fn collection_error(error: crate::Error, operation: AccountOperation) -> AccountError {
    error::into_account_error(error, GmailErrorContext::calendar_collection(operation))
}

fn event_error(error: crate::Error, operation: AccountOperation, id: String) -> AccountError {
    error::into_account_error(error, GmailErrorContext::calendar_event(operation, id))
}

fn local_error(operation: AccountOperation, message: String) -> AccountError {
    local_error_with_field(operation, "calendar", message)
}

fn calendar_contract_error(operation: AccountOperation, detail: String) -> AccountError {
    error::into_account_error(
        crate::error::Error::Local(crate::error::GmailLocalError::Internal { detail }),
        GmailErrorContext::calendar_collection(operation),
    )
}

fn local_error_with_field(
    operation: AccountOperation,
    field: &'static str,
    message: String,
) -> AccountError {
    error::into_account_error(
        crate::error::Error::missing_field(field, message),
        GmailErrorContext::calendar_collection(operation),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CalendarListResponse {
    items: Option<Vec<GoogleCalendarListEntry>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleCalendarListEntry {
    id: Option<String>,
    summary: Option<String>,
    background_color: Option<String>,
    primary: Option<bool>,
    access_role: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventsResponse {
    items: Option<Vec<GoogleEvent>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GoogleEvent {
    id: Option<String>,
    etag: Option<String>,
    i_cal_uid: Option<String>,
    summary: Option<String>,
    description: Option<String>,
    location: Option<String>,
    start: Option<GoogleEventTime>,
    end: Option<GoogleEventTime>,
    status: Option<String>,
    visibility: Option<String>,
    organizer: Option<GoogleOrganizer>,
    attendees: Option<Vec<GoogleAttendee>>,
    recurrence: Option<Vec<String>>,
    original_start_time: Option<GoogleEventTime>,
    transparency: Option<String>,
    html_link: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GoogleEventPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<GoogleEventTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<GoogleEventTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attendees: Option<Vec<GoogleAttendee>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recurrence: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    visibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transparency: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleEventTime {
    #[serde(skip_serializing_if = "Option::is_none")]
    date: Option<String>,
    #[serde(rename = "dateTime", skip_serializing_if = "Option::is_none")]
    date_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_zone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleOrganizer {
    email: Option<String>,
    display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleAttendee {
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    optional: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resource: Option<bool>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[cfg(test)]
mod tests {
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use bifrost_types::{ReconcileReason, RecoveryClass};
    use bytes::Bytes;
    use reqwest::StatusCode;

    use super::*;

    fn canned_json(status: StatusCode, value: serde_json::Value) -> Canned {
        Canned::Response {
            status,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(serde_json::to_vec(&value).expect("fixture serializes")),
        }
    }

    fn scripted_client(steps: Vec<Canned>) -> (Arc<GmailClient>, Arc<ScriptedDispatch>) {
        let script = ScriptedDispatch::new(steps);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        (
            Arc::new(GmailClient::with_account_net("https://gmail.test", net)),
            script,
        )
    }

    fn timed(value: &str) -> GoogleEventTime {
        GoogleEventTime {
            date: None,
            date_time: Some(value.to_string()),
            time_zone: None,
        }
    }

    #[test]
    fn google_event_maps_transparency_and_original_start_time() {
        let event = event_from_google(
            "primary".to_string(),
            GoogleEvent {
                id: Some("e1".to_string()),
                start: Some(timed("2026-06-02T12:00:00Z")),
                end: Some(timed("2026-06-02T13:00:00Z")),
                transparency: Some("transparent".to_string()),
                original_start_time: Some(timed("2026-06-01T12:00:00Z")),
                ..GoogleEvent::default()
            },
            AccountOperation::EventGet,
        )
        .expect("valid event");

        assert_eq!(event.availability, EventAvailability::Free);
        assert_eq!(
            event.recurrence.recurrence_id.as_deref(),
            Some("2026-06-01T12:00:00Z")
        );
    }

    #[test]
    fn google_event_rejects_missing_times() {
        let error = event_from_google(
            "primary".to_string(),
            GoogleEvent {
                id: Some("e1".to_string()),
                end: Some(timed("2026-06-02T13:00:00Z")),
                ..GoogleEvent::default()
            },
            AccountOperation::EventGet,
        )
        .expect_err("missing start should fail");

        assert_eq!(error.operation(), Some(AccountOperation::EventGet));
    }

    #[tokio::test]
    async fn events_in_range_requests_and_surfaces_cancelled_instances() {
        let (client, script) = scripted_client(vec![canned_json(
            StatusCode::OK,
            json!({
                "items": [{
                    "id": "cancelled-instance",
                    "status": "cancelled",
                    "originalStartTime": {"dateTime": "2026-06-02T12:00:00Z"}
                }]
            }),
        )]);
        let range = EventRange {
            calendar_id: CalendarId("primary".to_string()),
            start: bifrost_types::EventTime {
                value: "2026-06-01T00:00:00Z".to_string(),
                timezone: None,
            },
            end: bifrost_types::EventTime {
                value: "2026-06-03T00:00:00Z".to_string(),
                timezone: None,
            },
            limit: None,
            page_cursor: None,
        };

        let page = events_in_range(client, range).await.expect("range loads");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].status, EventStatus::Cancelled);
        assert!(
            !page.items[0].is_all_day,
            "a dateTime tombstone is a timed instance",
        );
        assert!(script.requests()[0].url.query().is_some_and(|query| {
            query.contains("singleEvents=true") && query.contains("showDeleted=true")
        }));
    }

    /// A cancelled instance of an ALL-DAY recurrence carries no `start`
    /// and only `originalStartTime.date`. Reading all-day-ness off
    /// `event.start` alone left `is_all_day: false` beside date-valued
    /// start and end fields, which the shared surface reads as a timed
    /// event at midnight.
    #[tokio::test]
    async fn a_cancelled_all_day_instance_stays_all_day() {
        let (client, _script) = scripted_client(vec![canned_json(
            StatusCode::OK,
            json!({
                "items": [{
                    "id": "cancelled-all-day",
                    "status": "cancelled",
                    "originalStartTime": {"date": "2026-06-02"}
                }]
            }),
        )]);
        let range = EventRange {
            calendar_id: CalendarId("primary".to_string()),
            start: bifrost_types::EventTime {
                value: "2026-06-01T00:00:00Z".to_string(),
                timezone: None,
            },
            end: bifrost_types::EventTime {
                value: "2026-06-03T00:00:00Z".to_string(),
                timezone: None,
            },
            limit: None,
            page_cursor: None,
        };

        let page = events_in_range(client, range).await.expect("range loads");

        let event = &page.items[0];
        assert_eq!(event.status, EventStatus::Cancelled);
        assert_eq!(event.start.value, "2026-06-02");
        assert_eq!(event.end.value, "2026-06-02");
        assert!(
            event.is_all_day,
            "a date-valued tombstone must project as an all-day instance",
        );
    }

    #[test]
    fn cross_calendar_search_cursor_round_trips_calendar_and_token() {
        let calendars = vec!["primary".to_string(), "work".to_string()];
        let cursor = encode_cross_calendar_cursor("work", b"next-token");

        let (index, token) =
            decode_cross_calendar_cursor(Some(cursor), &calendars).expect("cursor");

        assert_eq!(index, 1);
        assert_eq!(token.as_deref(), Some("next-token"));
    }

    #[test]
    fn cross_calendar_search_cursor_can_resume_at_calendar_boundary() {
        let calendars = vec!["primary".to_string(), "work".to_string()];
        let cursor = encode_cross_calendar_cursor("work", b"");

        let (index, token) =
            decode_cross_calendar_cursor(Some(cursor), &calendars).expect("cursor");

        assert_eq!(index, 1);
        assert_eq!(token, None);
    }

    #[tokio::test]
    async fn calendars_list_walks_every_page_and_encodes_page_tokens() {
        let (client, script) = scripted_client(vec![
            canned_json(
                StatusCode::OK,
                json!({
                    "items": [{"id": "first", "summary": "First"}],
                    "nextPageToken": "next+page&owner=me"
                }),
            ),
            canned_json(
                StatusCode::OK,
                json!({"items": [{"id": "second", "summary": "Second"}]}),
            ),
        ]);

        let calendars = calendars_list(client).await.expect("all pages load");

        assert_eq!(
            calendars
                .iter()
                .map(|calendar| calendar.id.0.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .url
                .as_str()
                .ends_with("calendarList?maxResults=250")
        );
        assert!(
            requests[1]
                .url
                .as_str()
                .ends_with("calendarList?maxResults=250&pageToken=next%2Bpage%26owner%3Dme")
        );
    }

    #[tokio::test]
    async fn calendars_list_rejects_a_repeated_page_token() {
        let (client, script) = scripted_client(vec![
            canned_json(StatusCode::OK, json!({"nextPageToken": "stalled"})),
            canned_json(StatusCode::OK, json!({"nextPageToken": "stalled"})),
        ]);

        let error = calendars_list(client)
            .await
            .expect_err("repeated token must terminate");

        assert_eq!(error.operation(), Some(AccountOperation::CalendarsList));
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        ));
        assert_eq!(script.requests().len(), 2);
    }

    /// A server handing out a fresh token every page slips past the
    /// repeated-token guard, so the page budget is the only thing left
    /// stopping an unbounded walk. Observe it firing rather than
    /// asserting the constant exists.
    #[tokio::test]
    async fn calendars_list_stops_at_the_page_budget() {
        let steps = (0..MAX_CALENDAR_LIST_PAGES)
            .map(|page| canned_json(StatusCode::OK, json!({"nextPageToken": format!("p{page}")})))
            .collect();
        let (client, script) = scripted_client(steps);

        let error = calendars_list(client)
            .await
            .expect_err("the page budget must terminate the walk");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        ));
        assert_eq!(script.requests().len(), MAX_CALENDAR_LIST_PAGES);
    }

    #[test]
    fn event_move_url_encodes_destination_calendar() {
        // The path and query encoders differ on exactly two inputs: the
        // complete `.` and `..` components, which the path encoder
        // double-escapes so the WHATWG parser cannot resolve them as
        // navigation. `destination` is a query value, where dots carry
        // no structural meaning, so both must survive verbatim.
        let base = "https://www.googleapis.com/calendar/v3";
        let url = event_move_url(base, "primary", "event/1", ".");
        assert!(url.contains("/calendars/primary/events/event%2F1/move"));
        assert!(url.ends_with("destination=."));

        let parent = event_move_url(base, "primary", "event/1", "..");
        assert!(parent.ends_with("destination=.."));

        // Everything else encodes identically under both, so a value
        // carrying query delimiters still pins that the destination is
        // escaped at all.
        let delimited = event_move_url(base, "primary", "event/1", "a&b=c+d");
        assert!(delimited.ends_with("destination=a%26b%3Dc%2Bd"));
    }

    #[tokio::test]
    async fn failed_patch_after_move_reports_partial_completion() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::HeaderName::from_static("x-goog-request-id"),
            reqwest::header::HeaderValue::from_static("req-77"),
        );
        let (client, script) = scripted_client(vec![
            canned_json(StatusCode::OK, json!({"id": "event-1"})),
            Canned::Response {
                status: StatusCode::FORBIDDEN,
                headers,
                body: Bytes::from(
                    serde_json::to_vec(&json!({
                        "error": {
                            "code": 403,
                            "message": "denied",
                            "errors": [{"reason": "forbidden", "message": "denied"}]
                        }
                    }))
                    .expect("fixture serializes"),
                ),
            },
        ]);
        let patch = EventPatch {
            calendar_id: Some(CalendarId("destination".to_string())),
            title: Some(Some("Updated".to_string())),
            ..EventPatch::default()
        };

        let error = update(client, EventId("source::event-1".to_string()), patch)
            .await
            .expect_err("destination patch fails");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
        ));
        assert!(matches!(
            error.recovery(),
            RecoveryClass::Reconcile(advice)
                if advice.reason == ReconcileReason::PartialCompletionSignal
        ));
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            Cause::Attempt(attempt)
                if attempt.transmission_state == TransmissionState::Acknowledged
        )));

        // The whole value of the PartialResponse classification is that
        // the consumer can act on it: the scope must name the moved
        // event where it now lives, not the composite id it arrived
        // under.
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Calendar {
                id: ("destination::event-1".to_string()).into()
            })
        );

        // The reclassification must not discard the underlying
        // transport evidence.
        let telemetry = error.telemetry_fields();
        assert_eq!(telemetry.status, Some(403));
        assert_eq!(telemetry.request_id, Some("req-77"));
        assert_eq!(telemetry.provider, Some(bifrost_types::Provider::Gmail));
        assert_eq!(telemetry.protocol, Some(Protocol::Gmail));
        assert!(telemetry.native_code.is_some());
        assert!(
            error
                .support_consented()
                .support_text
                .iter()
                .any(|text| text.contains("denied")),
            "the provider's own diagnostic text must survive reclassification"
        );

        // The original failure survives as secondary evidence rather
        // than being replaced by the reclassification.
        assert!(
            error
                .chain()
                .iter()
                .any(|cause| matches!(cause, Cause::Wire(WireCause::Gmail(_))))
        );

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, reqwest::Method::POST);
        assert_eq!(requests[1].method, reqwest::Method::PATCH);
    }

    /// Local patch validation is purely a function of the patch, so it
    /// can run before the move. Doing so keeps a patch that could never
    /// be valid from stranding the event in a new calendar: the cheap
    /// prevention, as opposed to the `TransmissionState` evidence that
    /// covers second-leg failures which genuinely cannot be prevented.
    #[tokio::test]
    async fn an_unexpressible_patch_is_rejected_before_the_move() {
        let (client, script) = scripted_client(Vec::new());
        let patch = EventPatch {
            calendar_id: Some(CalendarId("destination".to_string())),
            is_all_day: Some(true),
            ..EventPatch::default()
        };

        let error = update(client, EventId("source::event-1".to_string()), patch)
            .await
            .expect_err("an all-day flip with no bounds is unexpressible");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        ));
        assert!(
            script.requests().is_empty(),
            "no move may be issued for a patch that can never be applied"
        );
    }

    #[tokio::test]
    async fn failed_patch_without_a_move_keeps_its_own_classification() {
        let (client, script) = scripted_client(vec![canned_json(
            StatusCode::FORBIDDEN,
            json!({"error": {"code": 403, "message": "denied"}}),
        )]);
        let patch = EventPatch {
            title: Some(Some("Updated".to_string())),
            ..EventPatch::default()
        };

        let error = update(client, EventId("source::event-1".to_string()), patch)
            .await
            .expect_err("patch fails");

        assert!(
            !matches!(
                error.kind(),
                AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
            ),
            "a patch that was never preceded by a move is not a partial write"
        );
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Calendar {
                id: ("source::event-1".to_string()).into()
            })
        );
        assert_eq!(script.requests().len(), 1);
    }

    #[test]
    fn event_patch_detects_non_move_fields() {
        assert!(!event_patch_has_non_move_fields(&EventPatch {
            calendar_id: Some(CalendarId("work".to_string())),
            ..EventPatch::default()
        }));
        assert!(event_patch_has_non_move_fields(&EventPatch {
            title: Some(Some("Planning".to_string())),
            ..EventPatch::default()
        }));
    }

    #[test]
    fn google_attendee_maps_resource_role() {
        let attendee = attendee_from_google(GoogleAttendee {
            email: Some("room@example.test".to_string()),
            display_name: None,
            response_status: Some("accepted".to_string()),
            optional: Some(false),
            resource: Some(true),
            extra: Map::new(),
        })
        .expect("attendee");

        assert_eq!(attendee.role, AttendeeRole::Resource);
        assert_eq!(attendee.status, RsvpStatus::Accepted);

        let google = google_attendee(&EventAttendee {
            email: "room@example.test".to_string(),
            name: None,
            role: AttendeeRole::Resource,
            status: RsvpStatus::Accepted,
        });
        assert_eq!(google.resource, Some(true));
        assert_eq!(google.optional, Some(false));
    }

    #[test]
    fn rsvp_updates_matching_google_attendee_without_dropping_extra_fields() {
        let mut extra = Map::new();
        extra.insert("comment".to_string(), serde_json::json!("bring notes"));
        let mut attendees = Some(vec![
            GoogleAttendee {
                email: Some("other@example.test".to_string()),
                display_name: None,
                response_status: Some("needsAction".to_string()),
                optional: None,
                resource: None,
                extra: Map::new(),
            },
            GoogleAttendee {
                email: Some("SELF@example.test".to_string()),
                display_name: None,
                response_status: Some("needsAction".to_string()),
                optional: None,
                resource: None,
                extra,
            },
        ]);

        rsvp_google_attendees_for_self(&mut attendees, "self@example.test", RsvpStatus::Accepted)
            .expect("matching attendee");
        let attendees = attendees.expect("attendees");
        assert_eq!(attendees[0].response_status.as_deref(), Some("needsAction"));
        assert_eq!(attendees[1].response_status.as_deref(), Some("accepted"));
        assert_eq!(attendees[1].extra["comment"].as_str(), Some("bring notes"));

        assert!(
            rsvp_google_attendees_for_self(&mut None, "self@example.test", RsvpStatus::Accepted)
                .is_err()
        );
    }

    #[test]
    fn google_event_create_writes_transparency() {
        let patch = google_event_from_create(&EventCreate {
            calendar_id: CalendarId("primary".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00Z".to_string(),
                timezone: None,
            },
            end: EventTime {
                value: "2026-06-02T13:00:00Z".to_string(),
                timezone: None,
            },
            is_all_day: false,
            status: EventStatus::Tentative,
            availability: EventAvailability::Free,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        });

        assert_eq!(patch.transparency.as_deref(), Some("transparent"));
        assert_eq!(patch.status.as_deref(), Some("tentative"));
    }

    #[test]
    fn google_event_create_organizer_is_rejected() {
        let error = reject_create_organizer(&EventCreate {
            calendar_id: CalendarId("primary".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00Z".to_string(),
                timezone: None,
            },
            end: EventTime {
                value: "2026-06-02T13:00:00Z".to_string(),
                timezone: None,
            },
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: Some(EventOrganizer {
                email: "owner@example.test".to_string(),
                name: None,
            }),
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        })
        .expect_err("organizer should be unsupported");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));
    }

    #[test]
    fn google_event_patch_serializes_clears_as_null_and_omits_absent_fields() {
        let patch = google_event_from_patch(&EventPatch {
            title: Some(None),
            description: None,
            location: Some(Some("Room 1".to_string())),
            status: Some(EventStatus::Cancelled),
            ..EventPatch::default()
        });
        let value = serde_json::to_value(&patch).expect("patch json");

        assert!(value.get("summary").is_some_and(serde_json::Value::is_null));
        assert!(value.get("description").is_none());
        assert_eq!(
            value.get("location").and_then(serde_json::Value::as_str),
            Some("Room 1")
        );
        assert_eq!(
            value.get("status").and_then(serde_json::Value::as_str),
            Some("cancelled")
        );
    }

    #[test]
    fn google_event_patch_keeps_all_day_date_shape() {
        let patch = google_event_from_patch(&EventPatch {
            start: Some(EventTime {
                value: "2026-06-02".to_string(),
                timezone: Some("UTC".to_string()),
            }),
            end: Some(EventTime {
                value: "2026-06-03".to_string(),
                timezone: Some("UTC".to_string()),
            }),
            ..EventPatch::default()
        });
        let value = serde_json::to_value(&patch).expect("patch json");

        assert_eq!(
            value
                .get("start")
                .and_then(|start| start.get("date"))
                .and_then(serde_json::Value::as_str),
            Some("2026-06-02")
        );
        assert!(
            value
                .get("start")
                .and_then(|start| start.get("dateTime"))
                .is_none()
        );
    }

    #[test]
    fn google_event_patch_clears_recurrence_with_empty_array() {
        let patch = google_event_from_patch(&EventPatch {
            recurrence: Some(EventRecurrence::default()),
            ..EventPatch::default()
        });
        let value = serde_json::to_value(&patch).expect("patch json");

        assert_eq!(
            value
                .get("recurrence")
                .and_then(serde_json::Value::as_array),
            Some(&Vec::new())
        );
    }

    #[test]
    fn google_event_patch_omits_recurrence_when_absent() {
        let patch = google_event_from_patch(&EventPatch {
            title: Some(Some("Planning".to_string())),
            ..EventPatch::default()
        });
        let value = serde_json::to_value(&patch).expect("patch json");

        assert!(value.get("recurrence").is_none());
    }

    #[test]
    fn google_event_patch_emits_recurrence_lines_when_present() {
        let patch = google_event_from_patch(&EventPatch {
            recurrence: Some(EventRecurrence {
                rrule: Some("FREQ=WEEKLY".to_string()),
                ..EventRecurrence::default()
            }),
            ..EventPatch::default()
        });

        assert_eq!(
            patch.recurrence.as_deref(),
            Some(["RRULE:FREQ=WEEKLY".to_string()].as_slice())
        );
    }

    #[test]
    fn all_day_only_patch_is_rejected_as_unsupported() {
        let error = reject_unexpressible_all_day_patch(&EventPatch {
            is_all_day: Some(true),
            ..EventPatch::default()
        })
        .expect_err("all-day-only patch should be rejected");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
        ));
    }

    #[test]
    fn all_day_patch_with_both_bounds_is_accepted() {
        reject_unexpressible_all_day_patch(&EventPatch {
            is_all_day: Some(true),
            start: Some(EventTime {
                value: "2026-06-02".to_string(),
                timezone: None,
            }),
            end: Some(EventTime {
                value: "2026-06-03".to_string(),
                timezone: None,
            }),
            ..EventPatch::default()
        })
        .expect("all-day patch carrying both bounds is expressible");
    }

    #[test]
    fn google_range_bound_expands_all_day_dates() {
        assert_eq!(
            google_range_bound(
                &EventTime {
                    value: "2026-06-02".to_string(),
                    timezone: None,
                },
                "timeMin"
            )
            .unwrap(),
            "2026-06-02T00:00:00Z"
        );
    }

    #[test]
    fn google_range_bound_keeps_existing_offsets() {
        assert_eq!(
            google_range_bound(
                &EventTime {
                    value: "2026-06-02T12:00:00+02:00".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                "timeMin"
            )
            .unwrap(),
            "2026-06-02T12:00:00+02:00"
        );
    }

    #[test]
    fn google_range_bound_adds_utc_offset_when_timezone_is_utc() {
        assert_eq!(
            google_range_bound(
                &EventTime {
                    value: "2026-06-02T12:00:00".to_string(),
                    timezone: Some("UTC".to_string()),
                },
                "timeMin"
            )
            .unwrap(),
            "2026-06-02T12:00:00Z"
        );
    }

    #[test]
    fn google_range_bound_rejects_offsetless_non_utc_datetime() {
        assert!(
            google_range_bound(
                &EventTime {
                    value: "2026-06-02T12:00:00".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                "timeMin",
            )
            .is_err()
        );
    }
}
