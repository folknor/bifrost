use bifrost_types::{
    AccountError, AccountOperation, AttendeeRole, Calendar, CalendarEvent, CalendarId,
    CalendarProvenance, EventAttendee, EventAvailability, EventCreate, EventId, EventOrganizer,
    EventPatch, EventRange, EventRecurrence, EventSearchRequest, EventStatus, EventTime,
    EventVisibility, Page, ProtocolKind, RsvpStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::types::ODataCollection;

use super::GraphAccount;
use super::graph_error::{self, GraphErrorContext};

const DEFAULT_CALENDAR_ID: &str = "calendar";
/// Sentinel calendar segment for composite `EventId`s whose hosting
/// calendar is unknown. Graph event ids are mailbox-unique, so such ids
/// address through `/me/events/{id}` instead of
/// `/me/calendars/{cal}/events/{id}`. The Graph Search API spans the
/// whole mailbox without reporting each hit's calendar, so search hits
/// are minted with this segment to keep `event_get`/`update`/`delete`
/// routing honest.
const MAILBOX_SCOPE: &str = "$mailbox";
const EVENT_ID_SEPARATOR: &str = "::";
const EVENT_SELECT: &str = "id,subject,body,location,start,end,isAllDay,showAs,sensitivity,organizer,attendees,seriesMasterId,webLink,categories,responseStatus,isCancelled,changeKey,recurrence";
const EVENT_TIMEZONE_PREFER: &str = "outlook.timezone=\"UTC\"";
const EVENT_SEARCH_FIELDS: &[&str] = &[
    "id",
    "subject",
    "body",
    "location",
    "start",
    "end",
    "isAllDay",
    "showAs",
    "sensitivity",
    "organizer",
    "attendees",
    "seriesMasterId",
    "webLink",
    "categories",
    "responseStatus",
    "isCancelled",
    "changeKey",
    "recurrence",
];

pub(crate) async fn calendars_list(account: GraphAccount) -> Result<Vec<Calendar>, AccountError> {
    let prefix = account.client.api_path_prefix();
    let mut calendars = Vec::new();
    let mut next = Some(format!(
        "{prefix}/calendars?$select=id,name,canEdit,isDefaultCalendar&$top=250"
    ));
    while let Some(url) = next {
        let page: ODataCollection<GraphCalendar> =
            get_page(&account, &url, AccountOperation::CalendarsList).await?;
        calendars.extend(page.value.into_iter().map(calendar_from_graph));
        next = page.next_link;
    }
    Ok(calendars)
}

pub(crate) async fn events_in_range(
    account: GraphAccount,
    range: EventRange,
) -> Result<Page<CalendarEvent>, AccountError> {
    let next_url = range
        .page_cursor
        .map(String::from_utf8)
        .transpose()
        .map_err(|error| local_error(AccountOperation::EventsInRange, error.to_string()))?;
    let url = next_url.unwrap_or_else(|| {
        let prefix = account.client.api_path_prefix();
        let calendar = bifrost_net::url::encode_component(&range.calendar_id.0);
        format!(
            "{prefix}/calendars/{calendar}/calendarView?startDateTime={}&endDateTime={}&$select={EVENT_SELECT}&$top={}",
            bifrost_net::url::encode_component(&range.start.value),
            bifrost_net::url::encode_component(&range.end.value),
            range.limit.unwrap_or(250).clamp(1, 250)
        )
    });
    let page: ODataCollection<GraphEvent> =
        get_event_page(&account, &url, AccountOperation::EventsInRange).await?;
    Ok(Page {
        items: page
            .value
            .into_iter()
            .map(|event| event_from_graph(range.calendar_id.0.clone(), event))
            .collect(),
        next_cursor: page.next_link.map(String::into_bytes),
        estimated_total: None,
        failed_ids: Vec::new(),
    })
}

pub(crate) async fn get(
    account: GraphAccount,
    event: EventId,
) -> Result<CalendarEvent, AccountError> {
    let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventGet)?;
    let path = event_url(&account, &calendar_id, &event_id);
    let event = account
        .client
        .get_json_prefer::<GraphEvent>(&path, EVENT_TIMEZONE_PREFER)
        .await
        .map_err(|error| into_error(error, AccountOperation::EventGet))?;
    Ok(event_from_graph(calendar_id, event))
}

pub(crate) async fn create(
    account: GraphAccount,
    event: EventCreate,
) -> Result<EventId, AccountError> {
    reject_create_organizer(&event)?;
    reject_unwritable_status(event.status, AccountOperation::EventCreate)?;
    validate_event_create_timezones(&event)?;
    let calendar_id = event.calendar_id.0.clone();
    let prefix = account.client.api_path_prefix();
    let encoded = bifrost_net::url::encode_component(&calendar_id);
    let path = format!("{prefix}/calendars/{encoded}/events");
    let created = account
        .client
        .post::<GraphEvent, _>(&path, &graph_event_from_create(&event))
        .await
        .map_err(|error| into_error(error, AccountOperation::EventCreate))?;
    Ok(EventId(join_event_id(&calendar_id, &created.id)))
}

fn reject_create_organizer(event: &EventCreate) -> Result<(), AccountError> {
    if event.organizer.is_some() {
        return Err(local_error(
            AccountOperation::EventCreate,
            "Graph event organizer is server-derived on event creation".to_string(),
        ));
    }
    Ok(())
}

/// Graph has no writable event status: `isCancelled` is server-derived
/// from cancellation actions, and there is no confirmed/tentative knob.
/// Accept only `Confirmed` (the state a freshly created or patched Graph
/// event reports) and reject any other requested status before payload
/// construction, mirroring the organizer-on-create rejection rather than
/// silently writing a confirmed event.
fn reject_unwritable_status(
    status: EventStatus,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if matches!(status, EventStatus::Confirmed) {
        return Ok(());
    }
    Err(local_error(
        operation,
        "Graph event status is server-derived; only Confirmed can be expressed".to_string(),
    ))
}

fn validate_event_create_timezones(event: &EventCreate) -> Result<(), AccountError> {
    validate_graph_time_zone(
        event.start.timezone.as_deref(),
        AccountOperation::EventCreate,
    )?;
    validate_graph_time_zone(event.end.timezone.as_deref(), AccountOperation::EventCreate)
}

fn validate_event_patch_timezones(patch: &EventPatch) -> Result<(), AccountError> {
    if let Some(start) = &patch.start {
        validate_graph_time_zone(start.timezone.as_deref(), AccountOperation::EventUpdate)?;
    }
    if let Some(end) = &patch.end {
        validate_graph_time_zone(end.timezone.as_deref(), AccountOperation::EventUpdate)?;
    }
    Ok(())
}

pub(crate) async fn update(
    account: GraphAccount,
    event: EventId,
    patch: EventPatch,
) -> Result<(), AccountError> {
    validate_event_patch_timezones(&patch)?;
    if let Some(status) = patch.status {
        reject_unwritable_status(status, AccountOperation::EventUpdate)?;
    }
    let current = get(account.clone(), event.clone()).await?;
    let calendar_id = patch
        .calendar_id
        .clone()
        .unwrap_or_else(|| current.calendar_id.clone())
        .0;
    let (_, native_event_id) = split_event_id(&event.0, AccountOperation::EventUpdate)?;
    let etag = current.etag.clone();
    let path = event_url(&account, &calendar_id, &native_event_id);
    let body = graph_event_from_patch(&patch);
    let result = if let Some(etag) = etag.as_deref() {
        account.client.patch_if_match(&path, etag, &body).await
    } else {
        account.client.patch(&path, &body).await
    };
    result.map_err(|error| into_error(error, AccountOperation::EventUpdate))
}

pub(crate) async fn delete(account: GraphAccount, event: EventId) -> Result<(), AccountError> {
    let current = get(account.clone(), event.clone()).await?;
    let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventDelete)?;
    let path = event_url(&account, &calendar_id, &event_id);
    let result = if let Some(etag) = current.etag.as_deref() {
        account.client.delete_if_match(&path, etag).await
    } else {
        account.client.delete(&path).await
    };
    result.map_err(|error| into_error(error, AccountOperation::EventDelete))
}

pub(crate) async fn rsvp(
    account: GraphAccount,
    event: EventId,
    status: RsvpStatus,
) -> Result<(), AccountError> {
    let (calendar_id, event_id) = split_event_id(&event.0, AccountOperation::EventRsvp)?;
    let action = rsvp_action(status)?;
    let path = format!("{}/{action}", event_url(&account, &calendar_id, &event_id));
    account
        .client
        .post_no_response(
            &path,
            &GraphRsvpAction {
                comment: None,
                send_response: false,
            },
        )
        .await
        .map_err(|error| into_error(error, AccountOperation::EventRsvp))
}

/// Sentinel prefix marking a `page_cursor` minted by the Graph Search API
/// path (`search_with_graph_api`). The Search API pages by `from`/`size`
/// offset rather than an `@odata.nextLink`, so its cursor cannot be fed to
/// the local nextLink walk and vice versa. The bytes after the prefix are
/// the decimal next `from` offset. `\u{1f}` cannot appear in a Graph
/// `@odata.nextLink` URL, so the two cursor shapes never collide.
const SEARCH_API_CURSOR_PREFIX: &[u8] = b"\x1fsearchapi:";

pub(crate) async fn search(
    account: GraphAccount,
    request: EventSearchRequest,
) -> Result<Page<CalendarEvent>, AccountError> {
    // A Search-API cursor must resume through the Search API: a local
    // nextLink walk cannot consume a `from`-offset cursor.
    if let Some(from) = search_api_cursor_offset(request.page_cursor.as_deref()) {
        return search_with_graph_api(account, request, from).await;
    }
    if graph_search_api_supported(&account, &request) {
        return search_with_graph_api(account, request, 0).await;
    }
    search_locally(account, request).await
}

/// Decode a Search-API `page_cursor` into its `from` offset, or `None` if
/// the cursor is absent or is a local nextLink cursor.
fn search_api_cursor_offset(cursor: Option<&[u8]>) -> Option<u32> {
    let bytes = cursor?.strip_prefix(SEARCH_API_CURSOR_PREFIX)?;
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Encode a Search-API next-page `from` offset into an opaque cursor.
fn search_api_cursor(from: u32) -> Vec<u8> {
    let mut cursor = SEARCH_API_CURSOR_PREFIX.to_vec();
    cursor.extend_from_slice(from.to_string().as_bytes());
    cursor
}

async fn search_locally(
    account: GraphAccount,
    request: EventSearchRequest,
) -> Result<Page<CalendarEvent>, AccountError> {
    let explicit_calendar_id = request.calendar_id.clone();
    let calendar_id = explicit_calendar_id
        .clone()
        .unwrap_or_else(|| CalendarId(DEFAULT_CALENDAR_ID.to_string()));
    let next_url = request
        .page_cursor
        .map(String::from_utf8)
        .transpose()
        .map_err(|error| local_error(AccountOperation::EventSearch, error.to_string()))?;
    let mut url = next_url.unwrap_or_else(|| {
        let prefix = account.client.api_path_prefix();
        format!(
            "{}?$select={EVENT_SELECT}&$top={}",
            event_search_path(&prefix, explicit_calendar_id.as_ref()),
            request.limit.unwrap_or(250).min(250)
        )
    });
    let needle = request.query.to_ascii_lowercase();
    let limit = request
        .limit
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(250)
        .max(1);
    let mut items = Vec::new();
    let next_cursor;
    loop {
        let page: ODataCollection<GraphEvent> =
            get_event_page(&account, &url, AccountOperation::EventSearch).await?;
        items.extend(
            page.value
                .into_iter()
                .map(|event| event_from_graph(calendar_id.0.clone(), event))
                .filter(|event| event_matches(event, &needle)),
        );
        if items.len() >= limit {
            items.truncate(limit);
            next_cursor = page.next_link.map(String::into_bytes);
            break;
        }
        let Some(next) = page.next_link else {
            next_cursor = None;
            break;
        };
        url = next;
    }
    Ok(Page {
        items,
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
    })
}

async fn search_with_graph_api(
    account: GraphAccount,
    request: EventSearchRequest,
    from: u32,
) -> Result<Page<CalendarEvent>, AccountError> {
    let size = request.limit.unwrap_or(250).clamp(1, 250);
    let body = GraphSearchRequest {
        requests: vec![GraphSearchEntityRequest {
            entity_types: vec!["event"],
            query: GraphSearchQuery {
                query_string: request.query.clone(),
            },
            from,
            size,
            fields: EVENT_SEARCH_FIELDS,
        }],
    };
    let response = account
        .client
        .post::<GraphSearchResponse, _>("/search/query", &body)
        .await
        .map_err(|error| into_error(error, AccountOperation::EventSearch))?;
    // Graph Search pages by `from`/`size`; `moreResultsAvailable` on the
    // hits container signals a further page. Carry the next `from` offset
    // in a sentinel cursor so the next call resumes the Search API rather
    // than capping at one page.
    let more_results = response
        .value
        .iter()
        .flat_map(|set| set.hits_containers.iter())
        .any(|container| container.more_results_available.unwrap_or(false));
    let items: Vec<CalendarEvent> = response
        .value
        .into_iter()
        .flat_map(|set| set.hits_containers)
        .flat_map(|container| container.hits)
        .filter_map(|hit| hit.resource)
        .map(|event| event_from_graph(MAILBOX_SCOPE.to_string(), event))
        .collect();
    let next_cursor = (more_results && !items.is_empty())
        .then(|| search_api_cursor(from + u32::try_from(items.len()).unwrap_or(size)));
    Ok(Page {
        items,
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
    })
}

fn graph_search_api_supported(account: &GraphAccount, request: &EventSearchRequest) -> bool {
    request.page_cursor.is_none()
        && request.calendar_id.is_none()
        && !request.query.trim().is_empty()
        && account.client.uses_default_mailbox()
}

async fn get_page<T: serde::de::DeserializeOwned>(
    account: &GraphAccount,
    url: &str,
    operation: AccountOperation,
) -> Result<ODataCollection<T>, AccountError> {
    if url.starts_with("http") {
        account
            .client
            .get_absolute(url)
            .await
            .map_err(|error| into_error(error, operation))
    } else {
        account
            .client
            .get_json(url)
            .await
            .map_err(|error| into_error(error, operation))
    }
}

async fn get_event_page<T: serde::de::DeserializeOwned>(
    account: &GraphAccount,
    url: &str,
    operation: AccountOperation,
) -> Result<ODataCollection<T>, AccountError> {
    if url.starts_with("http") {
        account
            .client
            .get_absolute_prefer(url, EVENT_TIMEZONE_PREFER)
            .await
            .map_err(|error| into_error(error, operation))
    } else {
        account
            .client
            .get_json_prefer(url, EVENT_TIMEZONE_PREFER)
            .await
            .map_err(|error| into_error(error, operation))
    }
}

fn calendar_from_graph(calendar: GraphCalendar) -> Calendar {
    let can_edit = calendar.can_edit.unwrap_or(true);
    Calendar {
        id: CalendarId(calendar.id.clone()),
        native_id: calendar.id.clone(),
        name: calendar.name.unwrap_or(calendar.id.clone()),
        color: None,
        provenance: CalendarProvenance {
            provider: ProtocolKind::Graph,
            native: calendar.id,
            calendar_native: None,
        },
        is_default: calendar.is_default_calendar.unwrap_or(false),
        can_create_events: can_edit,
        can_update_events: can_edit,
        can_delete_events: can_edit,
    }
}

fn event_from_graph(calendar_id: String, event: GraphEvent) -> CalendarEvent {
    let native = join_event_id(&calendar_id, &event.id);
    let self_response = rsvp_status(
        event
            .response_status
            .as_ref()
            .and_then(|status| status.get("response"))
            .and_then(Value::as_str),
    );
    CalendarEvent {
        id: EventId(native.clone()),
        calendar_id: CalendarId(calendar_id.clone()),
        native_id: native.clone(),
        uid: Some(event.id.clone()),
        etag: event.change_key.clone(),
        provenance: CalendarProvenance {
            provider: ProtocolKind::Graph,
            native,
            calendar_native: Some(calendar_id),
        },
        title: event.subject,
        description: event.body.and_then(|body| body.content),
        location: event.location.and_then(|location| location.display_name),
        is_all_day: event.is_all_day.unwrap_or(false),
        start: event
            .start
            .map(|time| event_time(time, event.is_all_day.unwrap_or(false)))
            .unwrap_or_else(empty_time),
        end: event
            .end
            .map(|time| event_time(time, event.is_all_day.unwrap_or(false)))
            .unwrap_or_else(empty_time),
        status: if event.is_cancelled.unwrap_or(false) {
            EventStatus::Cancelled
        } else {
            EventStatus::Confirmed
        },
        availability: availability(event.show_as.as_deref()),
        visibility: visibility(event.sensitivity.as_deref()),
        self_response,
        organizer: event.organizer.and_then(|recipient| {
            let email = recipient.email_address?;
            Some(EventOrganizer {
                email: email.address?,
                name: email.name,
            })
        }),
        attendees: event
            .attendees
            .unwrap_or_default()
            .into_iter()
            .filter_map(attendee_from_graph)
            .collect(),
        // Microsoft Graph reminders are not projected onto the shared read
        // surface yet; other providers (CalDAV VALARM, JMAP alerts) supply
        // them.
        reminders: Vec::new(),
        recurrence: EventRecurrence {
            rrule: event.recurrence.as_ref().and_then(rrule_from_graph),
            recurrence_id: event.series_master_id,
            ..EventRecurrence::default()
        },
        html_link: event.web_link,
        raw_ical: None,
    }
}

fn graph_event_from_create(event: &EventCreate) -> GraphEventPatch {
    GraphEventPatch {
        subject: event.title.clone().map(Value::String),
        body: event.description.as_ref().map(|description| {
            json!(GraphBody {
                content_type: Some("text".to_string()),
                content: Some(description.clone()),
            })
        }),
        location: event.location.as_ref().map(|name| {
            json!(GraphLocation {
                display_name: Some(name.clone()),
            })
        }),
        start: Some(graph_time(&event.start, event.is_all_day)),
        end: Some(graph_time(&event.end, event.is_all_day)),
        is_all_day: Some(event.is_all_day),
        show_as: Some(show_as(event.availability).to_string()),
        sensitivity: Some(sensitivity(event.visibility).to_string()),
        attendees: non_empty(event.attendees.iter().map(graph_attendee)),
        recurrence: recurrence_from_event(event),
    }
}

fn graph_event_from_patch(patch: &EventPatch) -> GraphEventPatch {
    // Effective all-day flag for the time emission. With no explicit
    // `is_all_day`, a date-only start/end value infers all-day. The same
    // value must then drive both the `start`/`end` dateTime shape AND the
    // top-level `isAllDay` field: if `graph_time` writes a midnight
    // `dateTime` (the all-day shape) while `isAllDay` stays unset, Graph
    // keeps a previously-timed event timed at midnight instead of
    // converting it to all-day.
    let effective_all_day = patch.is_all_day.unwrap_or_else(|| {
        patch
            .start
            .as_ref()
            .map(|time| is_all_day_value(&time.value))
            .or_else(|| patch.end.as_ref().map(|time| is_all_day_value(&time.value)))
            .unwrap_or(false)
    });
    // Only emit `isAllDay` when the patch actually touches a time field
    // (or set it explicitly): a metadata-only patch must not flip the
    // event's all-day state as a side effect.
    let is_all_day_out = patch
        .is_all_day
        .or_else(|| (patch.start.is_some() || patch.end.is_some()).then_some(effective_all_day));
    GraphEventPatch {
        subject: patch.title.clone().map(|value| match value {
            Some(value) => Value::String(value),
            None => Value::Null,
        }),
        body: patch
            .description
            .as_ref()
            .map(|description| match description {
                Some(description) => json!(GraphBody {
                    content_type: Some("text".to_string()),
                    content: Some(description.clone()),
                }),
                None => Value::Null,
            }),
        location: patch.location.as_ref().map(|location| match location {
            Some(location) => json!(GraphLocation {
                display_name: Some(location.clone()),
            }),
            None => Value::Null,
        }),
        start: patch
            .start
            .as_ref()
            .map(|time| graph_time(time, effective_all_day)),
        end: patch
            .end
            .as_ref()
            .map(|time| graph_time(time, effective_all_day)),
        is_all_day: is_all_day_out,
        show_as: patch
            .availability
            .map(|availability| show_as(availability).to_string()),
        sensitivity: patch
            .visibility
            .map(|visibility| sensitivity(visibility).to_string()),
        attendees: patch
            .attendees
            .as_ref()
            .and_then(|attendees| non_empty(attendees.iter().map(graph_attendee))),
        recurrence: patch.recurrence.as_ref().and_then(|recurrence| {
            let event = EventCreate {
                calendar_id: patch
                    .calendar_id
                    .clone()
                    .unwrap_or_else(|| CalendarId(DEFAULT_CALENDAR_ID.to_string())),
                title: None,
                description: None,
                location: None,
                start: patch.start.clone().unwrap_or_else(empty_time),
                end: patch.end.clone().unwrap_or_else(empty_time),
                is_all_day: patch.is_all_day.unwrap_or(false),
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: recurrence.clone(),
            };
            recurrence_from_event(&event)
        }),
    }
}

fn event_url(account: &GraphAccount, calendar_id: &str, event_id: &str) -> String {
    let prefix = account.client.api_path_prefix();
    let event = bifrost_net::url::encode_component(event_id);
    if calendar_id == MAILBOX_SCOPE {
        // Graph event ids are mailbox-unique; the mailbox-scoped path
        // resolves the hit without knowing its hosting calendar.
        format!("{prefix}/events/{event}")
    } else {
        format!(
            "{prefix}/calendars/{}/events/{event}",
            bifrost_net::url::encode_component(calendar_id),
        )
    }
}

fn event_search_path(prefix: &str, calendar_id: Option<&CalendarId>) -> String {
    if let Some(calendar_id) = calendar_id {
        let calendar = bifrost_net::url::encode_component(&calendar_id.0);
        format!("{prefix}/calendars/{calendar}/events")
    } else {
        format!("{prefix}/events")
    }
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
                "Graph event id is missing calendar id".to_string(),
            )
        })
}

fn event_time(time: GraphDateTime, is_all_day: bool) -> EventTime {
    let value = time.date_time.unwrap_or_default();
    EventTime {
        value: if is_all_day {
            all_day_date(&value).unwrap_or(value)
        } else {
            value
        },
        timezone: time.time_zone,
    }
}

fn graph_time(time: &EventTime, is_all_day: bool) -> GraphDateTime {
    GraphDateTime {
        date_time: Some(if is_all_day {
            graph_all_day_date_time(&time.value)
        } else {
            time.value.clone()
        }),
        time_zone: Some(graph_time_zone(time.timezone.as_deref())),
    }
}

fn graph_time_zone(timezone: Option<&str>) -> String {
    graph_time_zone_name(timezone).unwrap_or("UTC").to_string()
}

fn validate_graph_time_zone(
    timezone: Option<&str>,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if graph_time_zone_name(timezone).is_none() {
        return Err(local_error(
            operation,
            "Graph event timezone is not in the outbound IANA-to-Windows mapping".to_string(),
        ));
    }
    Ok(())
}

fn graph_time_zone_name(timezone: Option<&str>) -> Option<&str> {
    Some(match timezone.unwrap_or("UTC") {
        "UTC" | "Etc/UTC" => "UTC",
        "Europe/Amsterdam" | "Europe/Berlin" | "Europe/Busingen" | "Europe/Oslo"
        | "Europe/Rome" | "Europe/Stockholm" | "Europe/Vienna" | "Europe/Zurich" => {
            "W. Europe Standard Time"
        }
        "Europe/Brussels" | "Europe/Copenhagen" | "Europe/Madrid" | "Europe/Paris" => {
            "Romance Standard Time"
        }
        "Europe/Bratislava" | "Europe/Budapest" | "Europe/Ljubljana" | "Europe/Prague"
        | "Europe/Warsaw" | "Europe/Zagreb" => "Central Europe Standard Time",
        "Europe/Dublin" | "Europe/Lisbon" | "Europe/London" => "GMT Standard Time",
        "Europe/Athens" | "Europe/Bucharest" => "GTB Standard Time",
        "Europe/Helsinki" | "Europe/Kyiv" | "Europe/Riga" | "Europe/Sofia" | "Europe/Tallinn"
        | "Europe/Vilnius" => "FLE Standard Time",
        "Europe/Istanbul" => "Turkey Standard Time",
        "Europe/Moscow" => "Russian Standard Time",
        "America/Detroit"
        | "America/Indiana/Indianapolis"
        | "America/New_York"
        | "America/Toronto" => "Eastern Standard Time",
        "America/Chicago" => "Central Standard Time",
        "America/Denver" => "Mountain Standard Time",
        "America/Phoenix" => "US Mountain Standard Time",
        "America/Los_Angeles" | "America/Vancouver" => "Pacific Standard Time",
        "America/Anchorage" => "Alaskan Standard Time",
        "Pacific/Honolulu" => "Hawaiian Standard Time",
        "America/Mexico_City" => "Central Standard Time (Mexico)",
        "America/Bogota" | "America/Lima" | "America/Guayaquil" => "SA Pacific Standard Time",
        "America/Santiago" => "Pacific SA Standard Time",
        "America/Sao_Paulo" => "E. South America Standard Time",
        "America/Argentina/Buenos_Aires" => "Argentina Standard Time",
        "Africa/Cairo" => "Egypt Standard Time",
        "Africa/Johannesburg" => "South Africa Standard Time",
        "Asia/Dubai" => "Arabian Standard Time",
        "Asia/Jerusalem" => "Israel Standard Time",
        "Asia/Tokyo" => "Tokyo Standard Time",
        "Asia/Seoul" => "Korea Standard Time",
        "Asia/Shanghai" => "China Standard Time",
        "Asia/Hong_Kong" => "Hong Kong Standard Time",
        "Asia/Singapore" => "Singapore Standard Time",
        "Asia/Kolkata" => "India Standard Time",
        "Australia/Brisbane" => "E. Australia Standard Time",
        "Australia/Perth" => "W. Australia Standard Time",
        "Australia/Melbourne" | "Australia/Sydney" => "AUS Eastern Standard Time",
        "Pacific/Auckland" => "New Zealand Standard Time",
        // An IANA id we don't have a Windows mapping for: reject locally.
        value if value.contains('/') => return None,
        // A slash-free string is treated as an already-Windows tz id so
        // they round-trip. Windows tz ids are multi-word (e.g. "W. Europe
        // Standard Time"); the sole single-token id is "UTC" (handled
        // above). Reject a slash-free single token as junk rather than
        // forwarding an unresolvable tz to Graph.
        value if value.contains(' ') => value,
        _ => return None,
    })
}

fn all_day_date(value: &str) -> Option<String> {
    value.split_once('T').map(|(date, _)| date.to_string())
}

fn graph_all_day_date_time(value: &str) -> String {
    let date = all_day_date(value).unwrap_or_else(|| value.to_string());
    format!("{date}T00:00:00.0000000")
}

fn is_all_day_value(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes().get(4) == Some(&b'-')
        && value.as_bytes().get(7) == Some(&b'-')
}

fn rrule_from_graph(recurrence: &GraphRecurrence) -> Option<String> {
    let pattern = recurrence.pattern.as_ref()?;
    let mut parts = Vec::new();
    match pattern.kind.as_deref()? {
        "daily" => parts.push("FREQ=DAILY".to_string()),
        "weekly" => {
            parts.push("FREQ=WEEKLY".to_string());
            if let Some(days) = pattern.days_of_week.as_ref() {
                let byday = days
                    .iter()
                    .filter_map(|day| graph_day_to_rrule(day))
                    .collect::<Vec<_>>();
                if !byday.is_empty() {
                    parts.push(format!("BYDAY={}", byday.join(",")));
                }
            }
        }
        "absoluteMonthly" => {
            parts.push("FREQ=MONTHLY".to_string());
            if let Some(day) = pattern.day_of_month {
                parts.push(format!("BYMONTHDAY={day}"));
            }
        }
        "relativeMonthly" => {
            parts.push("FREQ=MONTHLY".to_string());
            push_relative_rule_parts(pattern, &mut parts);
        }
        "absoluteYearly" => {
            parts.push("FREQ=YEARLY".to_string());
            if let Some(month) = pattern.month {
                parts.push(format!("BYMONTH={month}"));
            }
            if let Some(day) = pattern.day_of_month {
                parts.push(format!("BYMONTHDAY={day}"));
            }
        }
        "relativeYearly" => {
            parts.push("FREQ=YEARLY".to_string());
            if let Some(month) = pattern.month {
                parts.push(format!("BYMONTH={month}"));
            }
            push_relative_rule_parts(pattern, &mut parts);
        }
        _ => return None,
    }
    if let Some(interval) = pattern.interval.filter(|interval| *interval > 1) {
        parts.push(format!("INTERVAL={interval}"));
    }
    if let Some(range) = recurrence.range.as_ref() {
        match range.kind.as_deref() {
            Some("numbered") => {
                if let Some(count) = range.number_of_occurrences {
                    parts.push(format!("COUNT={count}"));
                }
            }
            Some("endDate") => {
                if let Some(end) = range.end_date.as_deref() {
                    parts.push(format!("UNTIL={}", end.replace('-', "")));
                }
            }
            _ => {}
        }
    }
    Some(parts.join(";"))
}

fn push_relative_rule_parts(pattern: &GraphRecurrencePattern, parts: &mut Vec<String>) {
    if let Some(days) = pattern.days_of_week.as_ref() {
        let byday = days
            .iter()
            .filter_map(|day| graph_day_to_rrule(day))
            .collect::<Vec<_>>();
        if !byday.is_empty() {
            parts.push(format!("BYDAY={}", byday.join(",")));
        }
    }
    if let Some(index) = pattern.index.as_deref().and_then(graph_index_to_setpos) {
        parts.push(format!("BYSETPOS={index}"));
    }
}

fn recurrence_from_event(event: &EventCreate) -> Option<GraphRecurrence> {
    let rrule = event.recurrence.rrule.as_deref()?;
    graph_recurrence_from_rrule(rrule, graph_recurrence_start_date(&event.start.value))
}

fn graph_recurrence_from_rrule(rrule: &str, start_date: String) -> Option<GraphRecurrence> {
    let parsed = ParsedRRule::parse(rrule);
    if !parsed.keys_supported(&[
        "FREQ",
        "INTERVAL",
        "BYMONTH",
        "BYMONTHDAY",
        "BYDAY",
        "BYSETPOS",
        "COUNT",
        "UNTIL",
    ]) {
        return None;
    }
    let frequency = parsed.value("FREQ")?;
    let mut pattern = GraphRecurrencePattern {
        kind: Some(
            match frequency {
                "DAILY" => "daily",
                "WEEKLY" => "weekly",
                "MONTHLY" if parsed.value("BYMONTHDAY").is_some() => "absoluteMonthly",
                "MONTHLY" => "relativeMonthly",
                "YEARLY" if parsed.value("BYMONTHDAY").is_some() => "absoluteYearly",
                "YEARLY" => "relativeYearly",
                _ => return None,
            }
            .to_string(),
        ),
        interval: parsed
            .value("INTERVAL")
            .and_then(|value| value.parse().ok()),
        month: parsed.value("BYMONTH").and_then(|value| value.parse().ok()),
        day_of_month: parsed
            .value("BYMONTHDAY")
            .and_then(|value| value.parse().ok()),
        days_of_week: parsed.value("BYDAY").map(|value| {
            value
                .split(',')
                .filter_map(rrule_day_to_graph)
                .map(ToString::to_string)
                .collect()
        }),
        index: parsed
            .value("BYSETPOS")
            .and_then(rrule_setpos_to_graph)
            .map(ToString::to_string),
    };
    if pattern.interval.is_none() {
        pattern.interval = Some(1);
    }
    if pattern.days_of_week.as_ref().is_some_and(Vec::is_empty) {
        return None;
    }
    // Graph relative monthly/yearly patterns require daysOfWeek; an
    // RRULE with neither BYMONTHDAY (which would have selected the
    // absolute kind) nor BYDAY cannot build a valid pattern, and Graph
    // rejects it with 400. Reject locally before payload construction.
    if matches!(
        pattern.kind.as_deref(),
        Some("relativeMonthly" | "relativeYearly")
    ) && pattern.days_of_week.is_none()
    {
        return None;
    }
    if parsed.value("BYSETPOS").is_some() && pattern.index.is_none() {
        return None;
    }
    // A relative monthly/yearly pattern requires an `index`
    // (first/second/.../last). Graph silently defaults a missing index to
    // "first", so an RRULE like `FREQ=MONTHLY;BYDAY=MO` (every Monday)
    // would become "first Monday" without the caller's knowledge. We
    // cannot represent "every Monday" as a Graph relative pattern, so
    // reject locally rather than ship a silently-narrowed recurrence.
    if matches!(
        pattern.kind.as_deref(),
        Some("relativeMonthly" | "relativeYearly")
    ) && pattern.index.is_none()
    {
        return None;
    }
    Some(GraphRecurrence {
        pattern: Some(pattern),
        range: Some(GraphRecurrenceRange {
            kind: Some(
                if parsed.value("COUNT").is_some() {
                    "numbered"
                } else if parsed.value("UNTIL").is_some() {
                    "endDate"
                } else {
                    "noEnd"
                }
                .to_string(),
            ),
            start_date: Some(start_date),
            end_date: parsed.value("UNTIL").map(rrule_until_to_graph_date),
            recurrence_time_zone: None,
            number_of_occurrences: parsed.value("COUNT").and_then(|value| value.parse().ok()),
        }),
    })
}

fn graph_recurrence_start_date(value: &str) -> String {
    let date = all_day_date(value).unwrap_or_else(|| value.to_string());
    if date.len() >= 10 {
        date[..10].to_string()
    } else {
        date
    }
}

fn graph_day_to_rrule(day: &str) -> Option<&'static str> {
    match day {
        "sunday" => Some("SU"),
        "monday" => Some("MO"),
        "tuesday" => Some("TU"),
        "wednesday" => Some("WE"),
        "thursday" => Some("TH"),
        "friday" => Some("FR"),
        "saturday" => Some("SA"),
        _ => None,
    }
}

fn rrule_day_to_graph(day: &str) -> Option<&'static str> {
    match day {
        "SU" => Some("sunday"),
        "MO" => Some("monday"),
        "TU" => Some("tuesday"),
        "WE" => Some("wednesday"),
        "TH" => Some("thursday"),
        "FR" => Some("friday"),
        "SA" => Some("saturday"),
        _ => None,
    }
}

fn graph_index_to_setpos(index: &str) -> Option<i32> {
    match index {
        "first" => Some(1),
        "second" => Some(2),
        "third" => Some(3),
        "fourth" => Some(4),
        "last" => Some(-1),
        _ => None,
    }
}

fn rrule_setpos_to_graph(index: &str) -> Option<&'static str> {
    match index {
        "1" => Some("first"),
        "2" => Some("second"),
        "3" => Some("third"),
        "4" => Some("fourth"),
        "-1" => Some("last"),
        _ => None,
    }
}

fn rrule_until_to_graph_date(value: &str) -> String {
    if value.len() >= 8 {
        format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8])
    } else {
        value.to_string()
    }
}

struct ParsedRRule<'a> {
    parts: Vec<(&'a str, &'a str)>,
}

impl<'a> ParsedRRule<'a> {
    fn parse(value: &'a str) -> Self {
        let parts = value
            .split(';')
            .filter_map(|part| part.split_once('='))
            .collect();
        Self { parts }
    }

    fn value(&self, key: &str) -> Option<&'a str> {
        self.parts
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
            .map(|(_, value)| *value)
    }

    fn keys_supported(&self, supported: &[&str]) -> bool {
        self.parts.iter().all(|(key, _)| {
            supported
                .iter()
                .any(|supported_key| key.eq_ignore_ascii_case(supported_key))
        })
    }
}

fn empty_time() -> EventTime {
    EventTime {
        value: String::new(),
        timezone: None,
    }
}

fn attendee_from_graph(attendee: GraphAttendee) -> Option<EventAttendee> {
    let email = attendee.email_address?;
    Some(EventAttendee {
        email: email.address?,
        name: email.name,
        role: if attendee.kind.as_deref() == Some("optional") {
            AttendeeRole::Optional
        } else if attendee.kind.as_deref() == Some("resource") {
            AttendeeRole::Resource
        } else {
            AttendeeRole::Required
        },
        status: rsvp_status(
            attendee
                .status
                .and_then(|status| status.response)
                .as_deref(),
        ),
    })
}

fn graph_attendee(attendee: &EventAttendee) -> GraphAttendee {
    GraphAttendee {
        email_address: Some(GraphEmailAddress {
            address: Some(attendee.email.clone()),
            name: attendee.name.clone(),
        }),
        kind: Some(
            match attendee.role {
                AttendeeRole::Optional => "optional",
                AttendeeRole::Resource => "resource",
                AttendeeRole::Required | AttendeeRole::Chair | AttendeeRole::Unknown => "required",
                _ => "required",
            }
            .to_string(),
        ),
        status: Some(GraphResponseStatus {
            response: Some(rsvp_value(attendee.status).to_string()),
        }),
    }
}

fn event_matches(event: &CalendarEvent, needle: &str) -> bool {
    needle.is_empty()
        || event
            .title
            .as_deref()
            .is_some_and(|value| contains(value, needle))
        || event
            .description
            .as_deref()
            .is_some_and(|value| contains(value, needle))
        || event
            .location
            .as_deref()
            .is_some_and(|value| contains(value, needle))
}

fn contains(value: &str, needle: &str) -> bool {
    value.to_ascii_lowercase().contains(needle)
}

fn availability(value: Option<&str>) -> EventAvailability {
    match value.unwrap_or_default() {
        "free" => EventAvailability::Free,
        "tentative" => EventAvailability::Tentative,
        "oof" => EventAvailability::OutOfOffice,
        "busy" | "workingElsewhere" => EventAvailability::Busy,
        _ => EventAvailability::Unknown,
    }
}

fn show_as(value: EventAvailability) -> &'static str {
    match value {
        EventAvailability::Free => "free",
        EventAvailability::Tentative => "tentative",
        EventAvailability::OutOfOffice => "oof",
        EventAvailability::Busy | EventAvailability::Unknown => "busy",
        _ => "busy",
    }
}

fn visibility(value: Option<&str>) -> EventVisibility {
    match value.unwrap_or_default() {
        "private" => EventVisibility::Private,
        "confidential" => EventVisibility::Confidential,
        "normal" => EventVisibility::Default,
        _ => EventVisibility::Default,
    }
}

fn sensitivity(value: EventVisibility) -> &'static str {
    match value {
        EventVisibility::Private => "private",
        EventVisibility::Confidential => "confidential",
        EventVisibility::Public | EventVisibility::Default => "normal",
        _ => "normal",
    }
}

fn rsvp_status(value: Option<&str>) -> RsvpStatus {
    match value.unwrap_or_default() {
        "accepted" => RsvpStatus::Accepted,
        "declined" => RsvpStatus::Declined,
        "tentativelyAccepted" => RsvpStatus::Tentative,
        "notResponded" => RsvpStatus::NeedsAction,
        _ => RsvpStatus::Unknown,
    }
}

fn rsvp_value(value: RsvpStatus) -> &'static str {
    match value {
        RsvpStatus::Accepted => "accepted",
        RsvpStatus::Declined => "declined",
        RsvpStatus::Tentative => "tentativelyAccepted",
        RsvpStatus::NeedsAction | RsvpStatus::Unknown | RsvpStatus::Delegated => "notResponded",
        _ => "notResponded",
    }
}

fn rsvp_action(value: RsvpStatus) -> Result<&'static str, AccountError> {
    match value {
        RsvpStatus::Accepted => Ok("accept"),
        RsvpStatus::Declined => Ok("decline"),
        RsvpStatus::Tentative => Ok("tentativelyAccept"),
        RsvpStatus::NeedsAction | RsvpStatus::Delegated | RsvpStatus::Unknown => Err(local_error(
            AccountOperation::EventRsvp,
            "Graph RSVP only supports accepted, declined, or tentative".to_string(),
        )),
        _ => Err(local_error(
            AccountOperation::EventRsvp,
            "Graph RSVP status is unsupported".to_string(),
        )),
    }
}

fn non_empty<T>(iter: impl Iterator<Item = T>) -> Option<Vec<T>> {
    let values = iter.collect::<Vec<_>>();
    (!values.is_empty()).then_some(values)
}

fn into_error(error: crate::error::GraphError, operation: AccountOperation) -> AccountError {
    graph_error::into_account_error(error, GraphErrorContext::graph(operation))
}

fn local_error(operation: AccountOperation, message: String) -> AccountError {
    graph_error::unsupported_account_error(operation)
        .into_builder()
        .text(bifrost_types::DiagnosticText::support_only(message))
        .try_build()
        .expect("valid account error classification")
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphCalendar {
    id: String,
    name: Option<String>,
    can_edit: Option<bool>,
    is_default_calendar: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchRequest<'a> {
    requests: Vec<GraphSearchEntityRequest<'a>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchEntityRequest<'a> {
    entity_types: Vec<&'a str>,
    query: GraphSearchQuery,
    from: u32,
    size: u32,
    fields: &'a [&'a str],
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchQuery {
    query_string: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchResponse {
    value: Vec<GraphSearchSet>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchSet {
    hits_containers: Vec<GraphSearchHitsContainer>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchHitsContainer {
    #[serde(default)]
    hits: Vec<GraphSearchHit>,
    more_results_available: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphSearchHit {
    resource: Option<GraphEvent>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphEvent {
    id: String,
    subject: Option<String>,
    body: Option<GraphBody>,
    location: Option<GraphLocation>,
    start: Option<GraphDateTime>,
    end: Option<GraphDateTime>,
    is_all_day: Option<bool>,
    show_as: Option<String>,
    sensitivity: Option<String>,
    organizer: Option<GraphRecipient>,
    attendees: Option<Vec<GraphAttendee>>,
    series_master_id: Option<String>,
    recurrence: Option<GraphRecurrence>,
    web_link: Option<String>,
    response_status: Option<Value>,
    is_cancelled: Option<bool>,
    change_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphEventPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<GraphDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<GraphDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_all_day: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    show_as: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sensitivity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attendees: Option<Vec<GraphAttendee>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recurrence: Option<GraphRecurrence>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphRsvpAction {
    comment: Option<String>,
    send_response: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphDateTime {
    #[serde(rename = "dateTime", skip_serializing_if = "Option::is_none")]
    date_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_zone: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphLocation {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphRecipient {
    email_address: Option<GraphEmailAddress>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphEmailAddress {
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphAttendee {
    #[serde(skip_serializing_if = "Option::is_none")]
    email_address: Option<GraphEmailAddress>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<GraphResponseStatus>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphResponseStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    response: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphRecurrence {
    #[serde(skip_serializing_if = "Option::is_none")]
    pattern: Option<GraphRecurrencePattern>,
    #[serde(skip_serializing_if = "Option::is_none")]
    range: Option<GraphRecurrenceRange>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphRecurrencePattern {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    month: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    day_of_month: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    days_of_week: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphRecurrenceRange {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recurrence_time_zone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    number_of_occurrences: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph_time(value: &str) -> GraphDateTime {
        GraphDateTime {
            date_time: Some(value.to_string()),
            time_zone: Some("UTC".to_string()),
        }
    }

    #[test]
    fn graph_calendar_does_not_project_enum_color_as_shared_color() {
        let calendar = calendar_from_graph(GraphCalendar {
            id: "cal".to_string(),
            name: Some("Calendar".to_string()),
            can_edit: Some(true),
            is_default_calendar: Some(true),
        });

        assert_eq!(calendar.id.0, "cal");
        assert_eq!(calendar.color, None);
        assert!(calendar.is_default);
    }

    #[test]
    fn graph_event_maps_body_cancelled_and_resource_attendee() {
        let event = event_from_graph(
            "calendar".to_string(),
            GraphEvent {
                id: "e1".to_string(),
                subject: Some("Planning".to_string()),
                body: Some(GraphBody {
                    content_type: Some("text".to_string()),
                    content: Some("Full description".to_string()),
                }),
                location: None,
                start: Some(graph_time("2026-06-02T12:00:00")),
                end: Some(graph_time("2026-06-02T13:00:00")),
                is_all_day: Some(false),
                show_as: Some("busy".to_string()),
                sensitivity: Some("normal".to_string()),
                organizer: None,
                attendees: Some(vec![GraphAttendee {
                    email_address: Some(GraphEmailAddress {
                        address: Some("room@example.test".to_string()),
                        name: None,
                    }),
                    kind: Some("resource".to_string()),
                    status: Some(GraphResponseStatus {
                        response: Some("accepted".to_string()),
                    }),
                }]),
                series_master_id: None,
                recurrence: None,
                web_link: None,
                response_status: Some(json!({"response": "tentativelyAccepted"})),
                is_cancelled: Some(true),
                change_key: Some("etag".to_string()),
            },
        );

        assert_eq!(event.description.as_deref(), Some("Full description"));
        assert_eq!(event.status, EventStatus::Cancelled);
        assert_eq!(event.self_response, RsvpStatus::Tentative);
        assert_eq!(event.attendees[0].role, AttendeeRole::Resource);
        assert_eq!(event.attendees[0].status, RsvpStatus::Accepted);
    }

    #[test]
    fn graph_event_create_writes_body_and_resource_attendee() {
        let patch = graph_event_from_create(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: Some("Planning".to_string()),
            description: Some("Full description".to_string()),
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-02T13:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: vec![EventAttendee {
                email: "room@example.test".to_string(),
                name: None,
                role: AttendeeRole::Resource,
                status: RsvpStatus::Accepted,
            }],
            recurrence: EventRecurrence::default(),
        });

        assert_eq!(
            patch
                .body
                .as_ref()
                .and_then(|body| body.get("content"))
                .and_then(Value::as_str),
            Some("Full description")
        );
        assert_eq!(
            patch
                .attendees
                .as_ref()
                .and_then(|attendees| attendees.first())
                .and_then(|attendee| attendee.kind.as_deref()),
            Some("resource")
        );
    }

    #[test]
    fn graph_event_create_organizer_is_rejected() {
        let error = reject_create_organizer(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-02T13:00:00".to_string(),
                timezone: Some("UTC".to_string()),
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
    fn graph_event_status_rejects_unwritable_values() {
        assert!(
            reject_unwritable_status(EventStatus::Confirmed, AccountOperation::EventCreate).is_ok()
        );
        for status in [
            EventStatus::Cancelled,
            EventStatus::Tentative,
            EventStatus::Unknown,
        ] {
            let error = reject_unwritable_status(status, AccountOperation::EventCreate)
                .expect_err("status should be unsupported");
            assert!(matches!(
                error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
            ));
        }
    }

    #[test]
    fn graph_time_zone_maps_common_iana_names_to_windows() {
        assert_eq!(
            graph_time_zone(Some("Europe/Oslo")),
            "W. Europe Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("Europe/Paris")),
            "Romance Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("Europe/Warsaw")),
            "Central Europe Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("America/New_York")),
            "Eastern Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("America/Mexico_City")),
            "Central Standard Time (Mexico)"
        );
        assert_eq!(
            graph_time_zone(Some("Asia/Singapore")),
            "Singapore Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("Australia/Perth")),
            "W. Australia Standard Time"
        );
        assert_eq!(
            graph_time_zone(Some("Eastern Standard Time")),
            "Eastern Standard Time"
        );
        assert_eq!(graph_time_zone(Some("America/Unknown")), "UTC");
        assert_eq!(graph_time_zone(None), "UTC");
    }

    #[test]
    fn graph_time_zone_validation_rejects_unknown_iana_names() {
        let error =
            validate_graph_time_zone(Some("America/Unknown"), AccountOperation::EventCreate)
                .expect_err("unknown IANA timezone should be unsupported");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventCreate)
        ));
        assert!(
            validate_graph_time_zone(Some("Eastern Standard Time"), AccountOperation::EventCreate)
                .is_ok()
        );
    }

    #[test]
    fn graph_event_patch_preserves_scalar_clear_semantics() {
        let patch = graph_event_from_patch(&EventPatch {
            title: Some(None),
            description: Some(None),
            location: Some(Some("Room 1".to_string())),
            ..EventPatch::default()
        });
        let value = serde_json::to_value(&patch).expect("patch json");

        assert!(value.get("subject").is_some_and(Value::is_null));
        assert!(value.get("body").is_some_and(Value::is_null));
        assert_eq!(
            value
                .get("location")
                .and_then(|location| location.get("displayName"))
                .and_then(Value::as_str),
            Some("Room 1")
        );
    }

    #[test]
    fn date_only_start_patch_infers_all_day_flag() {
        // A date-only start with no explicit is_all_day must emit
        // isAllDay:true alongside the midnight dateTime, else Graph keeps
        // the event timed at midnight.
        let patch = graph_event_from_patch(&EventPatch {
            start: Some(EventTime {
                value: "2026-06-02".to_string(),
                timezone: Some("UTC".to_string()),
            }),
            ..EventPatch::default()
        });
        let value = serde_json::to_value(&patch).expect("patch json");
        assert_eq!(value.get("isAllDay"), Some(&json!(true)));
        assert_eq!(
            value
                .get("start")
                .and_then(|start| start.get("dateTime"))
                .and_then(Value::as_str),
            Some("2026-06-02T00:00:00.0000000")
        );
    }

    #[test]
    fn timed_start_patch_does_not_set_all_day() {
        let patch = graph_event_from_patch(&EventPatch {
            start: Some(EventTime {
                value: "2026-06-02T09:30:00".to_string(),
                timezone: Some("UTC".to_string()),
            }),
            ..EventPatch::default()
        });
        let value = serde_json::to_value(&patch).expect("patch json");
        assert_eq!(value.get("isAllDay"), Some(&json!(false)));
    }

    #[test]
    fn metadata_only_patch_omits_all_day() {
        // A patch touching no time field must not flip the event's
        // all-day state.
        let patch = graph_event_from_patch(&EventPatch {
            title: Some(Some("Renamed".to_string())),
            ..EventPatch::default()
        });
        let value = serde_json::to_value(&patch).expect("patch json");
        assert!(value.get("isAllDay").is_none());
    }

    #[test]
    fn relative_monthly_without_index_is_rejected() {
        // FREQ=MONTHLY;BYDAY=MO (every Monday) has no Graph relative-
        // pattern index; Graph would silently default to "first", so we
        // reject locally rather than ship a narrowed recurrence.
        assert!(
            graph_recurrence_from_rrule("FREQ=MONTHLY;BYDAY=MO", "2026-06-02".to_string())
                .is_none()
        );
        assert!(
            graph_recurrence_from_rrule("FREQ=YEARLY;BYMONTH=6;BYDAY=MO", "2026-06-02".to_string())
                .is_none()
        );
    }

    #[test]
    fn slash_free_junk_timezone_is_rejected() {
        // A multi-word slash-free value round-trips as a Windows tz id; a
        // single-token junk value is rejected rather than forwarded.
        assert_eq!(
            graph_time_zone_name(Some("Eastern Standard Time")),
            Some("Eastern Standard Time")
        );
        assert_eq!(graph_time_zone_name(Some("foo")), None);
        assert_eq!(graph_time_zone_name(Some("UTC")), Some("UTC"));
    }

    #[test]
    fn search_api_cursor_round_trips_offset() {
        let cursor = search_api_cursor(50);
        assert_eq!(search_api_cursor_offset(Some(&cursor)), Some(50));
        // A local nextLink cursor (an https URL) is not a Search-API
        // cursor.
        assert_eq!(
            search_api_cursor_offset(Some(b"https://graph.microsoft.com/next")),
            None
        );
        assert_eq!(search_api_cursor_offset(None), None);
    }

    #[test]
    fn graph_event_maps_all_day_values_to_dates() {
        let event = event_from_graph(
            "calendar".to_string(),
            GraphEvent {
                id: "e1".to_string(),
                subject: None,
                body: None,
                location: None,
                start: Some(GraphDateTime {
                    date_time: Some("2026-06-02T00:00:00.0000000".to_string()),
                    time_zone: Some("UTC".to_string()),
                }),
                end: Some(GraphDateTime {
                    date_time: Some("2026-06-03T00:00:00.0000000".to_string()),
                    time_zone: Some("UTC".to_string()),
                }),
                is_all_day: Some(true),
                show_as: None,
                sensitivity: None,
                organizer: None,
                attendees: None,
                series_master_id: None,
                recurrence: None,
                web_link: None,
                response_status: None,
                is_cancelled: None,
                change_key: None,
            },
        );

        assert!(event.is_all_day);
        assert_eq!(event.start.value, "2026-06-02");
        assert_eq!(event.end.value, "2026-06-03");
    }

    #[test]
    fn graph_event_create_writes_all_day_midnight_values() {
        let patch = graph_event_from_create(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-03".to_string(),
                timezone: Some("UTC".to_string()),
            },
            is_all_day: true,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence::default(),
        });

        assert_eq!(patch.is_all_day, Some(true));
        assert_eq!(
            patch.start.and_then(|time| time.date_time),
            Some("2026-06-02T00:00:00.0000000".to_string())
        );
        assert_eq!(
            patch.end.and_then(|time| time.date_time),
            Some("2026-06-03T00:00:00.0000000".to_string())
        );
    }

    #[test]
    fn graph_event_maps_weekly_recurrence_to_rrule() {
        let event = event_from_graph(
            "calendar".to_string(),
            GraphEvent {
                id: "e1".to_string(),
                subject: None,
                body: None,
                location: None,
                start: Some(graph_time("2026-06-02T12:00:00")),
                end: Some(graph_time("2026-06-02T13:00:00")),
                is_all_day: Some(false),
                show_as: None,
                sensitivity: None,
                organizer: None,
                attendees: None,
                series_master_id: None,
                recurrence: Some(GraphRecurrence {
                    pattern: Some(GraphRecurrencePattern {
                        kind: Some("weekly".to_string()),
                        interval: Some(2),
                        month: None,
                        day_of_month: None,
                        days_of_week: Some(vec!["monday".to_string(), "wednesday".to_string()]),
                        index: None,
                    }),
                    range: Some(GraphRecurrenceRange {
                        kind: Some("numbered".to_string()),
                        start_date: Some("2026-06-02".to_string()),
                        end_date: None,
                        recurrence_time_zone: None,
                        number_of_occurrences: Some(4),
                    }),
                }),
                web_link: None,
                response_status: None,
                is_cancelled: None,
                change_key: None,
            },
        );

        assert_eq!(
            event.recurrence.rrule.as_deref(),
            Some("FREQ=WEEKLY;BYDAY=MO,WE;INTERVAL=2;COUNT=4")
        );
    }

    #[test]
    fn graph_event_create_writes_recurrence() {
        let patch = graph_event_from_create(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-02T13:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence {
                rrule: Some("FREQ=MONTHLY;BYDAY=MO;BYSETPOS=2;COUNT=3".to_string()),
                ..EventRecurrence::default()
            },
        });
        let recurrence = patch.recurrence.expect("recurrence");
        let pattern = recurrence.pattern.expect("pattern");
        let range = recurrence.range.expect("range");

        assert_eq!(pattern.kind.as_deref(), Some("relativeMonthly"));
        assert_eq!(pattern.days_of_week, Some(vec!["monday".to_string()]));
        assert_eq!(pattern.index.as_deref(), Some("second"));
        assert_eq!(range.kind.as_deref(), Some("numbered"));
        assert_eq!(range.start_date.as_deref(), Some("2026-06-02"));
        assert_eq!(range.number_of_occurrences, Some(3));
    }

    #[test]
    fn graph_event_create_rejects_unsupported_rrule_parts() {
        let patch = graph_event_from_create(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-02T13:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence {
                rrule: Some("FREQ=MONTHLY;BYDAY=MO;BYHOUR=9".to_string()),
                ..EventRecurrence::default()
            },
        });

        assert!(patch.recurrence.is_none());
    }

    #[test]
    fn graph_recurrence_rejects_relative_monthly_without_byday() {
        assert!(graph_recurrence_from_rrule("FREQ=MONTHLY", "2026-06-02".to_string()).is_none());
        assert!(
            graph_recurrence_from_rrule("FREQ=YEARLY;BYMONTH=6", "2026-06-02".to_string())
                .is_none()
        );
    }

    #[test]
    fn graph_event_create_writes_relative_yearly_recurrence() {
        let patch = graph_event_from_create(&EventCreate {
            calendar_id: CalendarId("calendar".to_string()),
            title: None,
            description: None,
            location: None,
            start: EventTime {
                value: "2026-06-02T12:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            end: EventTime {
                value: "2026-06-02T13:00:00".to_string(),
                timezone: Some("UTC".to_string()),
            },
            is_all_day: false,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            organizer: None,
            attendees: Vec::new(),
            recurrence: EventRecurrence {
                rrule: Some(
                    "FREQ=YEARLY;BYMONTH=6;BYDAY=MO;BYSETPOS=-1;UNTIL=20290602".to_string(),
                ),
                ..EventRecurrence::default()
            },
        });
        let recurrence = patch.recurrence.expect("recurrence");
        let pattern = recurrence.pattern.expect("pattern");
        let range = recurrence.range.expect("range");

        assert_eq!(pattern.kind.as_deref(), Some("relativeYearly"));
        assert_eq!(pattern.month, Some(6));
        assert_eq!(pattern.days_of_week, Some(vec!["monday".to_string()]));
        assert_eq!(pattern.index.as_deref(), Some("last"));
        assert_eq!(range.kind.as_deref(), Some("endDate"));
        assert_eq!(range.end_date.as_deref(), Some("2029-06-02"));
    }

    #[test]
    fn search_hit_id_round_trips_through_mailbox_scope() {
        let event = event_from_graph(
            MAILBOX_SCOPE.to_string(),
            GraphEvent {
                id: "e1".to_string(),
                subject: Some("Planning".to_string()),
                body: None,
                location: None,
                start: Some(graph_time("2026-06-02T12:00:00")),
                end: Some(graph_time("2026-06-02T13:00:00")),
                is_all_day: Some(false),
                show_as: None,
                sensitivity: None,
                organizer: None,
                attendees: None,
                series_master_id: None,
                recurrence: None,
                web_link: None,
                response_status: None,
                is_cancelled: None,
                change_key: None,
            },
        );

        let (calendar_id, event_id) =
            split_event_id(&event.id.0, AccountOperation::EventGet).expect("split");
        assert_eq!(calendar_id, MAILBOX_SCOPE);
        assert_eq!(event_id, "e1");

        let account = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token"),
            crate::account::PushMode::GraphSubscriptions,
        );
        assert_eq!(
            event_url(&account, &calendar_id, &event_id),
            "/me/events/e1"
        );
    }

    #[test]
    fn event_url_routes_calendar_scoped_ids_through_calendar_collection() {
        let account = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token"),
            crate::account::PushMode::GraphSubscriptions,
        );
        assert_eq!(
            event_url(&account, "calendar", "e1"),
            "/me/calendars/calendar/events/e1"
        );
    }

    #[test]
    fn event_search_path_uses_default_events_collection() {
        assert_eq!(event_search_path("/me", None), "/me/events");
        assert_eq!(
            event_search_path("/me", Some(&CalendarId("calendar with spaces".to_string()))),
            "/me/calendars/calendar%20with%20spaces/events"
        );
    }

    #[test]
    fn graph_search_api_support_requires_unscoped_default_mailbox_search() {
        let account = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token"),
            crate::account::PushMode::GraphSubscriptions,
        );
        assert!(graph_search_api_supported(
            &account,
            &EventSearchRequest {
                calendar_id: None,
                query: "planning".to_string(),
                page_cursor: None,
                limit: None,
            }
        ));
        assert!(!graph_search_api_supported(
            &account,
            &EventSearchRequest {
                calendar_id: Some(CalendarId("calendar".to_string())),
                query: "planning".to_string(),
                page_cursor: None,
                limit: None,
            }
        ));
        assert!(!graph_search_api_supported(
            &account,
            &EventSearchRequest {
                calendar_id: None,
                query: " ".to_string(),
                page_cursor: None,
                limit: None,
            }
        ));

        let shared = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token").for_shared_mailbox("shared"),
            crate::account::PushMode::GraphSubscriptions,
        );
        assert!(!graph_search_api_supported(
            &shared,
            &EventSearchRequest {
                calendar_id: None,
                query: "planning".to_string(),
                page_cursor: None,
                limit: None,
            }
        ));
    }

    #[test]
    fn graph_event_search_request_serializes_entity_type_and_fields() {
        let request = GraphSearchRequest {
            requests: vec![GraphSearchEntityRequest {
                entity_types: vec!["event"],
                query: GraphSearchQuery {
                    query_string: "planning".to_string(),
                },
                from: 0,
                size: 10,
                fields: EVENT_SEARCH_FIELDS,
            }],
        };
        let value = serde_json::to_value(&request).expect("search request json");

        assert_eq!(value["requests"][0]["entityTypes"], json!(["event"]));
        assert_eq!(value["requests"][0]["query"]["queryString"], "planning");
        assert_eq!(value["requests"][0]["size"], 10);
        assert_eq!(value["requests"][0]["fields"][0], "id");
    }

    #[test]
    fn rsvp_status_maps_to_graph_action() {
        assert_eq!(rsvp_action(RsvpStatus::Accepted).unwrap(), "accept");
        assert_eq!(rsvp_action(RsvpStatus::Declined).unwrap(), "decline");
        assert_eq!(
            rsvp_action(RsvpStatus::Tentative).unwrap(),
            "tentativelyAccept"
        );
        assert!(rsvp_action(RsvpStatus::NeedsAction).is_err());
    }
}
