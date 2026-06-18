use bifrost_types::{
    AttendeeRole, CalendarEvent, CalendarId, CalendarProvenance, EventAttendee, EventAvailability,
    EventCreate, EventId, EventOrganizer, EventPatch, EventRecurrence, EventStatus, EventTime,
    EventVisibility, ProtocolKind, RsvpStatus,
};
use caldata::ContentLineParser;
use chrono::{DateTime, Days, NaiveDate, Utc};
use uuid::Uuid;

/// Projection failed because the resource body could not be tokenized into
/// content lines (unclosed quoted parameter, missing property name/value,
/// invalid UTF-8). The caller degrades a single bad `.ics` to a per-resource
/// skip routed through `failed_hrefs` rather than failing the whole sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IcalParseError(pub(crate) String);

pub(crate) fn event_from_ical(
    uri: String,
    calendar_id: CalendarId,
    etag: Option<String>,
    data: &str,
) -> Result<CalendarEvent, IcalParseError> {
    let props = parse_vevent(data)?;
    let uid = props.first("UID").map(ToString::to_string);
    let title = props.first("SUMMARY").map(unescape_text);
    let description = props.first("DESCRIPTION").map(unescape_text);
    let location = props.first("LOCATION").map(unescape_text);
    // A real-world emitter (Outlook bridges, some CalDAV servers) can send a
    // TZID DTSTART paired with a floating fallback DTSTART. caldata's typed
    // builder would reject that as a singleton conflict; tokenizing instead
    // lets us keep the event and pick the most specific candidate (VALUE=DATE
    // > TZID > UTC > floating) rather than blindly taking document order.
    let dtstart = pick_datetime(&props.all_with_name("DTSTART"));
    let dtend = pick_datetime(&props.all_with_name("DTEND"));
    let start = dtstart
        .map(|prop| event_time_from_property(prop, false))
        .unwrap_or_else(default_time);
    let end = dtend
        .map(|prop| event_time_from_property(prop, true))
        .unwrap_or_else(default_time);
    let is_all_day =
        dtstart.is_some_and(Prop::value_type_date) || dtend.is_some_and(Prop::value_type_date);
    let organizer = props
        .first_with_name("ORGANIZER")
        .and_then(organizer_from_property);
    let attendees = props
        .all_with_name("ATTENDEE")
        .into_iter()
        .filter_map(attendee_from_property)
        .collect::<Vec<_>>();
    let recurrence = EventRecurrence {
        rrule: props.first("RRULE").map(ToString::to_string),
        rdate: props
            .all("RDATE")
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        exdate: props
            .all("EXDATE")
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        recurrence_id: props.first("RECURRENCE-ID").map(ToString::to_string),
    };
    Ok(CalendarEvent {
        id: EventId(uri.clone()),
        calendar_id: calendar_id.clone(),
        native_id: uri.clone(),
        uid,
        etag,
        provenance: CalendarProvenance {
            provider: ProtocolKind::CalDav,
            native: uri,
            calendar_native: Some(calendar_id.0),
        },
        title,
        description,
        location,
        start,
        end,
        is_all_day,
        status: event_status(props.first("STATUS")),
        availability: event_availability(props.first("TRANSP")),
        visibility: event_visibility(props.first("CLASS")),
        self_response: RsvpStatus::Unknown,
        organizer,
        attendees,
        recurrence,
        html_link: None,
        raw_ical: Some(data.to_string()),
    })
}

pub(crate) fn create_to_ical(event: &EventCreate, uid: &str) -> String {
    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "PRODID:-//folknor//bifrost//EN".to_string(),
    ];
    push_vtimezones(&mut lines, event);
    lines.push("BEGIN:VEVENT".to_string());
    lines.push(format!("UID:{uid}"));
    push_optional(&mut lines, "SUMMARY", event.title.as_deref());
    push_optional(&mut lines, "DESCRIPTION", event.description.as_deref());
    push_optional(&mut lines, "LOCATION", event.location.as_deref());
    push_time(&mut lines, "DTSTART", &event.start, event.is_all_day, false);
    push_time(&mut lines, "DTEND", &event.end, event.is_all_day, true);
    lines.push(format!("STATUS:{}", ical_event_status(event.status)));
    lines.push(format!("TRANSP:{}", transparency(event.availability)));
    if let Some(classification) = classification(event.visibility) {
        lines.push(format!("CLASS:{classification}"));
    }
    if let Some(organizer) = &event.organizer {
        lines.push(organizer_to_line(organizer));
    }
    if let Some(rrule) = &event.recurrence.rrule {
        lines.push(format!("RRULE:{rrule}"));
    }
    for rdate in &event.recurrence.rdate {
        lines.push(format!("RDATE:{rdate}"));
    }
    for exdate in &event.recurrence.exdate {
        lines.push(format!("EXDATE:{exdate}"));
    }
    if let Some(recurrence_id) = &event.recurrence.recurrence_id {
        lines.push(format!("RECURRENCE-ID:{recurrence_id}"));
    }
    for attendee in &event.attendees {
        lines.push(attendee_to_line(attendee));
    }
    lines.extend(["END:VEVENT".to_string(), "END:VCALENDAR".to_string()]);
    fold_ical_lines(lines)
}

pub(crate) fn patch_to_ical(
    current: &CalendarEvent,
    patch: &EventPatch,
) -> Result<String, &'static str> {
    let merged = patch_event(current, patch);
    let Some(raw_ical) = current.raw_ical.as_deref() else {
        let uid = current.uid.as_deref().unwrap_or(&current.native_id);
        return Ok(create_to_ical(&merged, uid));
    };
    if patch.recurrence.is_some() && has_recurrence_override_vevent(raw_ical) {
        return Err("recurrence override patch unsupported");
    }

    let mut replacements = Vec::new();
    let mut replace_names = Vec::new();
    if patch.title.is_some() {
        push_optional(&mut replacements, "SUMMARY", merged.title.as_deref());
        replace_names.push("SUMMARY");
    }
    if patch.description.is_some() {
        push_optional(
            &mut replacements,
            "DESCRIPTION",
            merged.description.as_deref(),
        );
        replace_names.push("DESCRIPTION");
    }
    if patch.location.is_some() {
        push_optional(&mut replacements, "LOCATION", merged.location.as_deref());
        replace_names.push("LOCATION");
    }
    if patch.start.is_some() || patch.is_all_day.is_some() {
        push_time(
            &mut replacements,
            "DTSTART",
            &merged.start,
            merged.is_all_day,
            false,
        );
        replace_names.push("DTSTART");
    }
    if patch.end.is_some() || patch.is_all_day.is_some() {
        push_time(
            &mut replacements,
            "DTEND",
            &merged.end,
            merged.is_all_day,
            true,
        );
        replace_names.push("DTEND");
    }
    if let Some(status) = patch.status {
        replacements.push(format!("STATUS:{}", ical_event_status(status)));
        replace_names.push("STATUS");
    }
    if patch.availability.is_some() {
        replacements.push(format!("TRANSP:{}", transparency(merged.availability)));
        replace_names.push("TRANSP");
    }
    if patch.visibility.is_some() {
        if let Some(classification) = classification(merged.visibility) {
            replacements.push(format!("CLASS:{classification}"));
        }
        replace_names.push("CLASS");
    }
    if patch.recurrence.is_some() {
        push_recurrence(&mut replacements, &merged.recurrence);
        replace_names.extend(["RRULE", "RDATE", "EXDATE", "RECURRENCE-ID"]);
    }
    if patch.attendees.is_some() {
        replacements.extend(merged.attendees.iter().map(attendee_to_line));
        replace_names.push("ATTENDEE");
    }

    Ok(replace_first_vevent_properties(
        raw_ical,
        &replace_names,
        replacements,
    ))
}

pub(crate) fn patch_event(current: &CalendarEvent, patch: &EventPatch) -> EventCreate {
    EventCreate {
        calendar_id: patch
            .calendar_id
            .clone()
            .unwrap_or_else(|| current.calendar_id.clone()),
        title: patch.title.clone().unwrap_or_else(|| current.title.clone()),
        description: patch
            .description
            .clone()
            .unwrap_or_else(|| current.description.clone()),
        location: patch
            .location
            .clone()
            .unwrap_or_else(|| current.location.clone()),
        start: patch.start.clone().unwrap_or_else(|| current.start.clone()),
        end: patch.end.clone().unwrap_or_else(|| current.end.clone()),
        is_all_day: patch.is_all_day.unwrap_or(current.is_all_day),
        status: patch.status.unwrap_or(current.status),
        availability: patch.availability.unwrap_or(current.availability),
        visibility: patch.visibility.unwrap_or(current.visibility),
        organizer: current.organizer.clone(),
        attendees: patch
            .attendees
            .clone()
            .unwrap_or_else(|| current.attendees.clone()),
        recurrence: patch
            .recurrence
            .clone()
            .unwrap_or_else(|| current.recurrence.clone()),
    }
}

pub(crate) fn rsvp_patch(
    current: &CalendarEvent,
    status: RsvpStatus,
    self_email: &str,
) -> Result<EventPatch, &'static str> {
    let mut attendees = current.attendees.clone();
    let attendee = attendees
        .iter_mut()
        .find(|attendee| attendee.email.eq_ignore_ascii_case(self_email))
        .ok_or("authenticated attendee not found")?;
    attendee.status = status;
    Ok(EventPatch {
        attendees: Some(attendees),
        ..EventPatch::default()
    })
}

pub(crate) fn rsvp_reply_ical(
    current: &CalendarEvent,
    status: RsvpStatus,
    self_email: &str,
) -> Result<String, &'static str> {
    let mut attendee = current
        .attendees
        .iter()
        .find(|attendee| attendee.email.eq_ignore_ascii_case(self_email))
        .cloned()
        .ok_or("authenticated attendee not found")?;
    attendee.status = status;
    let organizer = current
        .organizer
        .as_ref()
        .ok_or("event organizer not found")?;
    let uid = current.uid.as_deref().ok_or("event uid not found")?;
    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "METHOD:REPLY".to_string(),
        "BEGIN:VEVENT".to_string(),
        format!("UID:{}", escape_text(uid)),
        format!("DTSTAMP:{}", chrono::Utc::now().format("%Y%m%dT%H%M%SZ")),
    ];
    push_time(
        &mut lines,
        "DTSTART",
        &current.start,
        current.is_all_day,
        false,
    );
    push_time(&mut lines, "DTEND", &current.end, current.is_all_day, true);
    lines.push(organizer_to_line(organizer));
    lines.push(attendee_to_line(&attendee));
    lines.push("END:VEVENT".to_string());
    lines.push("END:VCALENDAR".to_string());
    Ok(fold_ical_lines(lines))
}

pub(crate) fn new_uid() -> String {
    Uuid::new_v4().to_string()
}

/// Project the first VEVENT's properties using caldata's streaming content
/// line tokenizer. caldata unfolds (stripping exactly one WSP per RFC 5545
/// sec 3.1, not the whole leading run the old hand-rolled unfolder ate) and
/// splits quoted parameter values that legally contain `:`/`;`/`,` - both
/// classes of bug the previous `split_once(':')` path had. Values are stored
/// raw (caldata never unescapes); text fields are unescaped at the point we
/// read them into the model.
fn parse_vevent(data: &str) -> Result<Props, IcalParseError> {
    let mut props = Vec::new();
    let mut in_event = false;
    let mut seen_event = false;
    for line in ContentLineParser::from_slice(data.as_bytes()) {
        let line = line.map_err(|error| IcalParseError(error.to_string()))?;
        let name = line.name;
        if name == "BEGIN" && line.value.eq_ignore_ascii_case("VEVENT") {
            // Only the first VEVENT is projected (the master); override
            // instances are preserved verbatim on the patch path.
            if seen_event {
                break;
            }
            in_event = true;
            seen_event = true;
            continue;
        }
        if name == "END" && line.value.eq_ignore_ascii_case("VEVENT") {
            break;
        }
        if !in_event {
            continue;
        }
        props.push(Prop {
            name,
            params: line.params,
            value: line.value,
        });
    }
    Ok(Props(props))
}

/// Order DTSTART/DTEND candidates by descending specificity so a duplicate
/// property does not silently project to document order. Mirrors ratatoskr's
/// precedence ladder: VALUE=DATE > TZID > UTC (`Z`) > floating.
fn pick_datetime<'a>(candidates: &[&'a Prop]) -> Option<&'a Prop> {
    candidates
        .iter()
        .copied()
        .max_by_key(|prop| datetime_specificity(prop))
}

fn datetime_specificity(prop: &Prop) -> u8 {
    if prop.value_type_date() {
        3
    } else if prop.param("TZID").is_some() {
        2
    } else if prop.value.ends_with('Z') {
        1
    } else {
        0
    }
}

#[derive(Debug)]
struct Props(Vec<Prop>);

impl Props {
    fn first(&self, name: &str) -> Option<&str> {
        self.first_with_name(name).map(|prop| prop.value.as_str())
    }

    fn all(&self, name: &str) -> Vec<&str> {
        self.all_with_name(name)
            .into_iter()
            .map(|prop| prop.value.as_str())
            .collect()
    }

    fn first_with_name(&self, name: &str) -> Option<&Prop> {
        self.0.iter().find(|prop| prop.name == name)
    }

    fn all_with_name(&self, name: &str) -> Vec<&Prop> {
        self.0.iter().filter(|prop| prop.name == name).collect()
    }
}

#[derive(Debug)]
struct Prop {
    name: String,
    params: caldata::parser::ContentLineParams,
    value: String,
}

impl Prop {
    fn param(&self, name: &str) -> Option<&str> {
        self.params.get_param(name)
    }

    fn value_type_date(&self) -> bool {
        self.param("VALUE")
            .is_some_and(|value| value.eq_ignore_ascii_case("DATE"))
    }
}

fn event_time_from_property(prop: &Prop, is_end: bool) -> EventTime {
    let tzid = prop.param("TZID");
    EventTime {
        value: format_ical_time(&prop.value, prop.value_type_date(), is_end, tzid.is_some()),
        // Map Microsoft/Windows zone names ("W. Europe Standard Time") to
        // their IANA equivalent when caldata's table knows them; otherwise
        // pass the TZID through verbatim.
        timezone: tzid.map(canonical_tzid),
    }
}

/// Resolve a raw TZID to its IANA name when it is a known Microsoft/Windows
/// zone alias, leaving already-IANA (or unknown) names untouched.
fn canonical_tzid(tzid: &str) -> String {
    caldata::types::get_proprietary_tzid(tzid)
        .map(|tz| tz.name().to_string())
        .unwrap_or_else(|| tzid.to_string())
}

fn format_ical_time(value: &str, is_date: bool, is_end: bool, has_tzid: bool) -> String {
    if is_date && value.len() == 8 {
        if is_end
            && let Ok(date) = NaiveDate::parse_from_str(value, "%Y%m%d")
            && let Some(date) = date.checked_sub_days(Days::new(1))
        {
            return date.format("%Y-%m-%d").to_string();
        }
        return format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8]);
    }
    if value.len() >= 15 {
        let suffix = ical_offset_suffix(value);
        let mut formatted = format!(
            "{}-{}-{}T{}:{}:{}{}",
            &value[0..4],
            &value[4..6],
            &value[6..8],
            &value[9..11],
            &value[11..13],
            &value[13..15],
            suffix
        );
        if value.ends_with('Z') {
            return formatted;
        }
        if !suffix.is_empty() {
            if let Ok(time) = DateTime::parse_from_rfc3339(&formatted) {
                formatted = time.to_rfc3339();
            }
            return formatted;
        }
        // A TZID-bearing local time is a wall-clock value, not UTC: leave it
        // bare so the `timezone` field is the sole source of the zone. Only a
        // truly floating time (no TZID, no offset) is normalized to UTC `Z`.
        if !has_tzid {
            formatted.push('Z');
        }
        return formatted;
    }
    value.to_string()
}

fn ical_offset_suffix(value: &str) -> String {
    if value.ends_with('Z') {
        return "Z".to_string();
    }
    if value.len() == 20 {
        let offset = &value[15..20];
        if offset.as_bytes()[0].is_ascii() && matches!(&offset[0..1], "+" | "-") {
            return format!("{}:{}", &offset[0..3], &offset[3..5]);
        }
    }
    String::new()
}

fn ical_time_from_event_time(time: &EventTime, is_all_day: bool, is_end: bool) -> String {
    if is_all_day {
        if is_end
            && let Ok(date) = NaiveDate::parse_from_str(&time.value, "%Y-%m-%d")
            && let Some(date) = date.checked_add_days(Days::new(1))
        {
            return date.format("%Y%m%d").to_string();
        }
        return time.value.replace('-', "");
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(&time.value) {
        if time.timezone.is_some() {
            return parsed.format("%Y%m%dT%H%M%S").to_string();
        }
        return parsed
            .with_timezone(&Utc)
            .format("%Y%m%dT%H%M%SZ")
            .to_string();
    }
    let mut value = time.value.replace(['-', ':'], "");
    if time.timezone.is_some() {
        value = value.trim_end_matches('Z').to_string();
    }
    value
}

fn default_time() -> EventTime {
    EventTime {
        value: String::new(),
        timezone: None,
    }
}

fn event_status(value: Option<&str>) -> EventStatus {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "CONFIRMED" => EventStatus::Confirmed,
        "TENTATIVE" => EventStatus::Tentative,
        "CANCELLED" => EventStatus::Cancelled,
        _ => EventStatus::Unknown,
    }
}

fn ical_event_status(status: EventStatus) -> &'static str {
    match status {
        EventStatus::Tentative => "TENTATIVE",
        EventStatus::Cancelled => "CANCELLED",
        EventStatus::Confirmed | EventStatus::Unknown => "CONFIRMED",
        _ => "CONFIRMED",
    }
}

fn organizer_from_property(prop: &Prop) -> Option<EventOrganizer> {
    Some(EventOrganizer {
        email: mailto(&prop.value)?,
        name: prop.param("CN").map(unescape_text),
    })
}

fn organizer_to_line(organizer: &EventOrganizer) -> String {
    let mut line = String::from("ORGANIZER");
    if let Some(name) = &organizer.name {
        line.push_str(";CN=");
        line.push_str(&escape_param(name));
    }
    line.push_str(":mailto:");
    line.push_str(&organizer.email);
    line
}

fn attendee_from_property(prop: &Prop) -> Option<EventAttendee> {
    Some(EventAttendee {
        email: mailto(&prop.value)?,
        name: prop.param("CN").map(unescape_text),
        role: attendee_role(prop.param("ROLE")),
        status: rsvp_status(prop.param("PARTSTAT")),
    })
}

fn attendee_to_line(attendee: &EventAttendee) -> String {
    let mut line = String::from("ATTENDEE");
    if let Some(name) = &attendee.name {
        line.push_str(";CN=");
        line.push_str(&escape_param(name));
    }
    line.push_str(";ROLE=");
    line.push_str(match attendee.role {
        AttendeeRole::Required => "REQ-PARTICIPANT",
        AttendeeRole::Optional => "OPT-PARTICIPANT",
        AttendeeRole::Resource => "NON-PARTICIPANT",
        AttendeeRole::Chair => "CHAIR",
        AttendeeRole::Unknown | _ => "REQ-PARTICIPANT",
    });
    line.push_str(";PARTSTAT=");
    line.push_str(match attendee.status {
        RsvpStatus::Accepted => "ACCEPTED",
        RsvpStatus::Declined => "DECLINED",
        RsvpStatus::Tentative => "TENTATIVE",
        RsvpStatus::Delegated => "DELEGATED",
        RsvpStatus::NeedsAction | RsvpStatus::Unknown | _ => "NEEDS-ACTION",
    });
    line.push_str(":mailto:");
    line.push_str(&attendee.email);
    line
}

fn attendee_role(value: Option<&str>) -> AttendeeRole {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "REQ-PARTICIPANT" => AttendeeRole::Required,
        "OPT-PARTICIPANT" => AttendeeRole::Optional,
        "NON-PARTICIPANT" => AttendeeRole::Resource,
        "CHAIR" => AttendeeRole::Chair,
        _ => AttendeeRole::Unknown,
    }
}

fn rsvp_status(value: Option<&str>) -> RsvpStatus {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "ACCEPTED" => RsvpStatus::Accepted,
        "DECLINED" => RsvpStatus::Declined,
        "TENTATIVE" => RsvpStatus::Tentative,
        "DELEGATED" => RsvpStatus::Delegated,
        "NEEDS-ACTION" => RsvpStatus::NeedsAction,
        _ => RsvpStatus::Unknown,
    }
}

fn mailto(value: &str) -> Option<String> {
    value
        .strip_prefix("mailto:")
        .or_else(|| value.strip_prefix("MAILTO:"))
        .map(ToString::to_string)
}

fn push_optional(lines: &mut Vec<String>, name: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        lines.push(format!("{name}:{}", escape_text(value)));
    }
}

fn push_vtimezones(lines: &mut Vec<String>, event: &EventCreate) {
    if event.is_all_day {
        return;
    }
    let mut tzids = Vec::new();
    for tzid in [&event.start.timezone, &event.end.timezone]
        .into_iter()
        .flatten()
        .filter(|tzid| !tzid.is_empty())
    {
        if !tzids.iter().any(|known: &String| known == tzid) {
            tzids.push(tzid.clone());
        }
    }
    for tzid in tzids {
        // Anchor the offset at whichever event time carries this zone (start
        // preferred), so the emitted VTIMEZONE matches the instant the server
        // will resolve the DTSTART/DTEND against.
        let anchor = [&event.start, &event.end]
            .into_iter()
            .find(|time| time.timezone.as_deref() == Some(tzid.as_str()))
            .and_then(event_naive_local);

        // Resolve the real UTC offset for the event's instant. `canonical_tzid`
        // folds Windows/Exchange names ("W. Europe Standard Time") to IANA so
        // the name parses to a `chrono_tz::Tz`. An unknown zone, or a value we
        // cannot parse to a naive wall-clock, falls through to `None`.
        let offset = tzid_offset_for_naive(&tzid, anchor);

        lines.push("BEGIN:VTIMEZONE".to_string());
        lines.push(format!("TZID:{}", escape_text(&tzid)));
        // A single STANDARD block carrying the correct offset for the event's
        // instant. This is approximate for a recurring event that spans a DST
        // transition (off by the DST delta on the far side of the transition),
        // but strictly better than the previous fixed `+0000` UTC stub, which
        // made servers that trust the supplied VTIMEZONE read an Oslo
        // wall-clock time as UTC and shift the event by the real offset
        // (1-2h). Full STANDARD/DAYLIGHT transition rules are not generated;
        // most servers re-resolve the TZID by name regardless. An unknown /
        // unparseable zone omits the offset sub-block rather than emit a
        // misleading `+0000`: the bare VTIMEZONE still names the TZID for
        // servers that resolve it themselves, and we avoid asserting a wrong
        // offset.
        if let Some(offset) = offset {
            lines.push("BEGIN:STANDARD".to_string());
            lines.push("DTSTART:19700101T000000".to_string());
            lines.push(format!("TZOFFSETFROM:{offset}"));
            lines.push(format!("TZOFFSETTO:{offset}"));
            lines.push("END:STANDARD".to_string());
        }
        lines.push("END:VTIMEZONE".to_string());
    }
}

/// Derive the wall-clock `NaiveDateTime` an event time names. With a TZID the
/// stored value is a local wall-clock instant (mirrors `push_time`, which
/// strips the trailing `Z` and emits the date/time verbatim under the zone).
fn event_naive_local(time: &EventTime) -> Option<chrono::NaiveDateTime> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(&time.value) {
        return Some(parsed.naive_local());
    }
    // Fall back to parsing a bare iCalendar-style `YYYYMMDDTHHMMSS[Z]` value.
    let raw = time.value.replace(['-', ':'], "");
    let raw = raw.trim_end_matches('Z');
    chrono::NaiveDateTime::parse_from_str(raw, "%Y%m%dT%H%M%S").ok()
}

/// Resolve the iCalendar UTC-offset string (`+HHMM` / `-HHMM`) for `tzid` at
/// the given wall-clock instant, or `None` if the zone is unknown.
///
/// LocalResult discipline mirrors ratatoskr's `resolve_local_to_timestamp`:
/// `Single` is used directly; `Ambiguous` (fall-back) picks the earlier
/// instant (matches Outlook/Google/Apple); `None` (spring-forward gap) walks
/// past the gap and uses the post-gap offset.
fn tzid_offset_for_naive(tzid: &str, naive: Option<chrono::NaiveDateTime>) -> Option<String> {
    use chrono::{LocalResult, Offset, TimeZone};

    let tz: chrono_tz::Tz = canonical_tzid(tzid).parse().ok()?;
    // With no usable anchor, fall back to the Unix epoch so we still emit the
    // zone's standard-time offset rather than nothing.
    let naive = naive.unwrap_or_else(|| {
        chrono::DateTime::from_timestamp(0, 0)
            .expect("epoch is representable")
            .naive_utc()
    });

    let offset = match tz.from_local_datetime(&naive) {
        LocalResult::Single(dt) => dt.offset().fix(),
        LocalResult::Ambiguous(early, _late) => early.offset().fix(),
        LocalResult::None => resolve_offset_through_gap(tz, naive)?,
    };
    Some(format_utc_offset(offset))
}

/// Spring-forward gap: walk back to the last valid wall clock and forward to
/// the first, shift `naive` past the gap by the gap width, and take the
/// resulting (post-gap) offset. Mirrors ratatoskr's `resolve_through_gap`.
fn resolve_offset_through_gap(
    tz: chrono_tz::Tz,
    naive: chrono::NaiveDateTime,
) -> Option<chrono::FixedOffset> {
    use chrono::{Duration, LocalResult, Offset, TimeZone};

    const MAX_PROBE_MINUTES: i64 = 60 * 48;

    let mut backward = 0i64;
    let gap_start = loop {
        backward += 1;
        if backward > MAX_PROBE_MINUTES {
            return None;
        }
        if !matches!(
            tz.from_local_datetime(&(naive - Duration::minutes(backward))),
            LocalResult::None
        ) {
            break backward;
        }
    };
    let mut forward = 0i64;
    let gap_end = loop {
        forward += 1;
        if forward > MAX_PROBE_MINUTES {
            return None;
        }
        if !matches!(
            tz.from_local_datetime(&(naive + Duration::minutes(forward))),
            LocalResult::None
        ) {
            break forward;
        }
    };

    let gap_width = gap_start + gap_end - 1;
    let shifted = naive.checked_add_signed(Duration::minutes(gap_width))?;
    match tz.from_local_datetime(&shifted) {
        LocalResult::Single(dt) => Some(dt.offset().fix()),
        LocalResult::Ambiguous(_early, late) => Some(late.offset().fix()),
        LocalResult::None => None,
    }
}

/// Format a `FixedOffset` as the iCalendar UTC-offset form `+HHMM` / `-HHMM`.
fn format_utc_offset(offset: chrono::FixedOffset) -> String {
    let total = offset.local_minus_utc();
    let sign = if total < 0 { '-' } else { '+' };
    let abs = total.abs();
    format!("{sign}{:02}{:02}", abs / 3600, (abs % 3600) / 60)
}

fn push_time(
    lines: &mut Vec<String>,
    name: &str,
    time: &EventTime,
    is_all_day: bool,
    is_end: bool,
) {
    let value = ical_time_from_event_time(time, is_all_day, is_end);
    if is_all_day {
        lines.push(format!("{name};VALUE=DATE:{value}"));
    } else if let Some(tzid) = &time.timezone {
        lines.push(format!("{name};TZID={}:{}", escape_param(tzid), value));
    } else {
        lines.push(format!("{name}:{value}"));
    }
}

fn push_recurrence(lines: &mut Vec<String>, recurrence: &EventRecurrence) {
    if let Some(rrule) = &recurrence.rrule {
        lines.push(format!("RRULE:{rrule}"));
    }
    for rdate in &recurrence.rdate {
        lines.push(format!("RDATE:{rdate}"));
    }
    for exdate in &recurrence.exdate {
        lines.push(format!("EXDATE:{exdate}"));
    }
    if let Some(recurrence_id) = &recurrence.recurrence_id {
        lines.push(format!("RECURRENCE-ID:{recurrence_id}"));
    }
}

fn event_availability(value: Option<&str>) -> EventAvailability {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "TRANSPARENT" => EventAvailability::Free,
        "OPAQUE" => EventAvailability::Busy,
        _ => EventAvailability::Unknown,
    }
}

fn event_visibility(value: Option<&str>) -> EventVisibility {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "PUBLIC" => EventVisibility::Public,
        "PRIVATE" => EventVisibility::Private,
        "CONFIDENTIAL" => EventVisibility::Confidential,
        _ => EventVisibility::Default,
    }
}

fn transparency(availability: EventAvailability) -> &'static str {
    match availability {
        EventAvailability::Free => "TRANSPARENT",
        EventAvailability::Busy
        | EventAvailability::Tentative
        | EventAvailability::OutOfOffice
        | EventAvailability::Unknown => "OPAQUE",
        _ => "OPAQUE",
    }
}

fn classification(visibility: EventVisibility) -> Option<&'static str> {
    match visibility {
        EventVisibility::Public => Some("PUBLIC"),
        EventVisibility::Private => Some("PRIVATE"),
        EventVisibility::Confidential => Some("CONFIDENTIAL"),
        EventVisibility::Default => None,
        _ => None,
    }
}

/// Splice replacements into the first VEVENT operating on *physical* lines.
///
/// The previous implementation unfolded the whole document and re-folded it,
/// so every preserved/unmodeled line was re-wrapped at column 75 and (via the
/// old WSP-eating unfolder) could lose characters. Here untouched logical
/// lines - including their original fold continuations - are emitted byte for
/// byte; only the freshly emitted replacement lines are folded. A long
/// preserved value therefore round-trips losslessly through an update.
fn replace_first_vevent_properties(
    raw_ical: &str,
    replace_names: &[&str],
    replacements: Vec<String>,
) -> String {
    let mut out = String::new();
    let mut replacements = Some(replacements);
    let mut in_first_event = false;
    let mut finished_first_event = false;
    for group in logical_line_groups(raw_ical) {
        let name = ical_line_name(group.logical_head());
        if name == Some("BEGIN")
            && line_value(group.logical_head()).is_some_and(|v| v.eq_ignore_ascii_case("VEVENT"))
            && !finished_first_event
        {
            in_first_event = true;
            group.push_verbatim(&mut out);
            continue;
        }
        if in_first_event
            && name == Some("END")
            && line_value(group.logical_head()).is_some_and(|v| v.eq_ignore_ascii_case("VEVENT"))
        {
            if let Some(replacements) = replacements.take() {
                out.push_str(&fold_ical_lines(replacements));
            }
            in_first_event = false;
            finished_first_event = true;
            group.push_verbatim(&mut out);
            continue;
        }
        if in_first_event && name.is_some_and(|name| replace_names.contains(&name)) {
            continue;
        }
        group.push_verbatim(&mut out);
    }
    out
}

fn has_recurrence_override_vevent(raw_ical: &str) -> bool {
    let mut in_event = false;
    for group in logical_line_groups(raw_ical) {
        let head = group.logical_head();
        let name = ical_line_name(head);
        if name == Some("BEGIN")
            && line_value(head).is_some_and(|v| v.eq_ignore_ascii_case("VEVENT"))
        {
            in_event = true;
            continue;
        }
        if in_event
            && name == Some("END")
            && line_value(head).is_some_and(|v| v.eq_ignore_ascii_case("VEVENT"))
        {
            in_event = false;
            continue;
        }
        if in_event && name == Some("RECURRENCE-ID") {
            return true;
        }
    }
    false
}

/// One logical content line as a run of physical lines: the head plus any
/// folded continuation lines (those starting with SPACE or TAB). Carries its
/// own trailing newline shape so it can be re-emitted verbatim.
struct LineGroup<'a> {
    physical: Vec<&'a str>,
}

impl<'a> LineGroup<'a> {
    fn logical_head(&self) -> &'a str {
        self.physical.first().copied().unwrap_or_default()
    }

    fn push_verbatim(&self, out: &mut String) {
        for line in &self.physical {
            out.push_str(line);
            out.push_str("\r\n");
        }
    }
}

/// Group `raw_ical` into logical lines without unfolding their content, so
/// preserved lines can be re-emitted byte for byte. CR is trimmed from each
/// physical line and re-added on emit; folding markers (leading WSP) stay
/// attached to the continuation lines they belong to.
fn logical_line_groups(raw_ical: &str) -> Vec<LineGroup<'_>> {
    let mut groups: Vec<LineGroup<'_>> = Vec::new();
    for raw in raw_ical.lines() {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if (line.starts_with(' ') || line.starts_with('\t'))
            && let Some(last) = groups.last_mut()
        {
            last.physical.push(line);
        } else {
            groups.push(LineGroup {
                physical: vec![line],
            });
        }
    }
    groups
}

fn ical_line_name(line: &str) -> Option<&str> {
    line.split_once(':')?.0.split(';').next().map(str::trim)
}

fn line_value(line: &str) -> Option<&str> {
    line.split_once(':').map(|(_, value)| value)
}

fn fold_ical_lines(lines: Vec<String>) -> String {
    let mut folded = String::new();
    for line in lines {
        let mut current = String::new();
        let mut prefix = 0;
        for ch in line.chars() {
            if prefix + current.len() + ch.len_utf8() > 75 {
                folded.push_str(&current);
                folded.push_str("\r\n ");
                current.clear();
                // The folding space occupies one octet of the 75-octet
                // budget on every continuation line.
                prefix = 1;
            }
            current.push(ch);
        }
        folded.push_str(&current);
        folded.push_str("\r\n");
    }
    folded
}

fn escape_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\r', "")
        .replace('\n', "\\n")
        .replace(',', "\\,")
        .replace(';', "\\;")
}

/// Single left-to-right scan rather than an ordering-dependent chain of
/// `replace()` calls. A chain mis-handles adjacency such as `\\n` (backslash
/// followed by a literal `n`), which a multi-pass replace would turn into a
/// backslash plus newline. RFC 5545 sec 3.3.11 escapes only: `\\`, `\;`,
/// `\,`, and `\N`/`\n` (newline); any other backslash pair is kept verbatim.
fn unescape_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n' | 'N') => out.push('\n'),
            Some(',') => out.push(','),
            Some(';') => out.push(';'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

fn escape_param(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\r', "")
        .replace('\n', "\\n");
    if escaped.contains([';', ',', ':']) {
        format!("\"{escaped}\"")
    } else {
        escaped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test shim: the production projector is fallible (malformed bodies
    /// degrade to a per-resource skip); these tests feed well-formed input
    /// and expect a successful projection.
    fn parse_event(
        uri: String,
        calendar_id: CalendarId,
        etag: Option<String>,
        data: &str,
    ) -> CalendarEvent {
        event_from_ical(uri, calendar_id, etag, data).expect("valid iCalendar projects")
    }

    #[test]
    fn parses_basic_vevent() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            Some("e1".to_string()),
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Meet\\, now\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;CN=Ada;PARTSTAT=ACCEPTED:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.uid.as_deref(), Some("u1"));
        assert_eq!(event.title.as_deref(), Some("Meet, now"));
        assert_eq!(event.start.value, "2026-06-02T12:00:00Z");
        assert_eq!(event.attendees[0].status, RsvpStatus::Accepted);
    }

    #[test]
    fn parses_numeric_offset_datetime() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000+0200\r\nDTEND:20260602T130000+0200\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.value, "2026-06-02T12:00:00+02:00");
        assert_eq!(event.end.value, "2026-06-02T13:00:00+02:00");
    }

    #[test]
    fn parses_tzid_datetime_as_wall_clock_with_timezone_metadata() {
        // Previously this test enshrined the false-`Z` bug, asserting the
        // wall-clock value was tagged UTC. A TZID-bearing local time is NOT
        // UTC: the value must stay bare and the zone lives in `timezone`.
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART;TZID=Europe/Oslo:20260602T120000\r\nDTEND;TZID=Europe/Oslo:20260602T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.value, "2026-06-02T12:00:00");
        assert!(!event.start.value.ends_with('Z'));
        assert_eq!(event.start.timezone.as_deref(), Some("Europe/Oslo"));
        assert_eq!(event.end.value, "2026-06-02T13:00:00");
        assert_eq!(event.end.timezone.as_deref(), Some("Europe/Oslo"));
    }

    #[test]
    fn maps_windows_tzid_to_iana() {
        // caldata ships the Microsoft/CLDR zone table; a Windows zone name
        // must surface as its IANA equivalent, not the opaque vendor string.
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART;TZID=W. Europe Standard Time:20260602T120000\r\nDTEND;TZID=W. Europe Standard Time:20260602T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.timezone.as_deref(), Some("Europe/Berlin"));
        assert_eq!(event.start.value, "2026-06-02T12:00:00");
    }

    #[test]
    fn duplicate_dtstart_prefers_most_specific_candidate() {
        // Outlook bridges emit a floating-fallback DTSTART alongside the
        // TZID one. caldata's typed builder would reject the duplicate; the
        // tokenizer path keeps the event and picks the TZID candidate.
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000\r\nDTSTART;TZID=Europe/Oslo:20260602T130000\r\nDTEND:20260602T140000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.timezone.as_deref(), Some("Europe/Oslo"));
        assert_eq!(event.start.value, "2026-06-02T13:00:00");
    }

    #[test]
    fn quoted_parameter_with_colon_and_semicolon_parses() {
        // A quoted CN containing `,` and a quoted TZID containing `:`/`;`
        // must not be mis-split on the first `:` (the old parser bug).
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART;TZID=\"Custom:Zone;X\":20260602T120000\r\nATTENDEE;CN=\"Doe, John\";PARTSTAT=ACCEPTED:mailto:john@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.value, "2026-06-02T12:00:00");
        assert_eq!(event.start.timezone.as_deref(), Some("Custom:Zone;X"));
        assert_eq!(event.attendees.len(), 1);
        assert_eq!(event.attendees[0].name.as_deref(), Some("Doe, John"));
        assert_eq!(event.attendees[0].email, "john@example.test");
        assert_eq!(event.attendees[0].status, RsvpStatus::Accepted);
    }

    #[test]
    fn malformed_resource_degrades_to_skip_not_hard_failure() {
        // An unterminated quoted parameter is a tokenizer error; the
        // projector returns Err so the caller can route the resource to
        // failed_hrefs instead of failing the whole sync.
        let result = event_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART;TZID=\"unterminated:20260602T120000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert!(result.is_err());
    }

    #[test]
    fn long_preserved_line_round_trips_losslessly_through_update() {
        // The H round-trip bug: a long folded preserved value must come back
        // byte-identical after a patch that does not touch it. We feed a
        // DESCRIPTION pre-folded at a non-75 column and assert the unfolded
        // value survives a SUMMARY-only patch.
        let long_value = "x:    y ".repeat(40);
        let mut body = String::from(
            "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nDESCRIPTION:",
        );
        // Fold the description manually at column ~40 so a refold would move
        // the boundaries (and the old WSP-eating unfolder would drop the
        // run-of-spaces after the colon).
        for (index, chunk) in long_value.as_bytes().chunks(40).enumerate() {
            if index > 0 {
                body.push_str("\r\n ");
            }
            body.push_str(std::str::from_utf8(chunk).unwrap());
        }
        body.push_str("\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n");

        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            Some("e1".to_string()),
            &body,
        );
        let original_description = current.description.clone();
        assert_eq!(original_description.as_deref(), Some(long_value.as_str()));

        let patched = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("title patch should serialize");

        let reparsed = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &patched,
        );
        assert!(patched.contains("SUMMARY:New"));
        assert_eq!(reparsed.description, original_description);
    }

    #[test]
    fn all_day_dtend_round_trips_as_exclusive_ical_end() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02".to_string(),
                    timezone: None,
                },
                end: EventTime {
                    value: "2026-06-02".to_string(),
                    timezone: None,
                },
                is_all_day: true,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("DTSTART;VALUE=DATE:20260602"));
        assert!(body.contains("DTEND;VALUE=DATE:20260603"));

        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &body,
        );
        assert_eq!(event.start.value, "2026-06-02");
        assert_eq!(event.end.value, "2026-06-02");
    }

    #[test]
    fn dtend_date_marks_event_all_day() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T000000Z\r\nDTEND;VALUE=DATE:20260603\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert!(event.is_all_day);
        assert_eq!(event.end.value, "2026-06-02");
    }

    #[test]
    fn parses_recurrence_properties_from_master_vevent() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\nRDATE:20260609T120000Z\r\nEXDATE:20260616T120000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(
            event.recurrence.rrule.as_deref(),
            Some("FREQ=WEEKLY;COUNT=3")
        );
        assert_eq!(event.recurrence.rdate, vec!["20260609T120000Z"]);
        assert_eq!(event.recurrence.exdate, vec!["20260616T120000Z"]);
        assert_eq!(event.recurrence.recurrence_id, None);
    }

    #[test]
    fn multi_vevent_projection_reads_only_first_vevent() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Master\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY;COUNT=2\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:Override\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.title.as_deref(), Some("Master"));
        assert_eq!(event.start.value, "2026-06-02T12:00:00Z");
        assert_eq!(
            event.recurrence.rrule.as_deref(),
            Some("FREQ=WEEKLY;COUNT=2")
        );
        assert_eq!(event.recurrence.recurrence_id, None);
    }

    #[test]
    fn serializes_create_payload() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: Some("Planning".to_string()),
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
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("UID:uid-1"));
        assert!(body.contains("SUMMARY:Planning"));
        assert!(body.contains("DTSTART:20260602T120000Z"));
    }

    #[test]
    fn serializes_create_organizer() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
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
                    email: "ada@example.test".to_string(),
                    name: Some("Ada Lovelace".to_string()),
                }),
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("ORGANIZER;CN=Ada Lovelace:mailto:ada@example.test"));
    }

    #[test]
    fn serializes_numeric_offset_datetime_as_utc() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00+02:00".to_string(),
                    timezone: None,
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00+02:00".to_string(),
                    timezone: None,
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("DTSTART:20260602T100000Z"));
        assert!(body.contains("DTEND:20260602T110000Z"));
    }

    #[test]
    fn serializes_timezone_datetime_as_local_ical_value() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("BEGIN:VTIMEZONE"));
        assert!(body.contains("TZID:Europe/Oslo"));
        assert!(body.contains("BEGIN:STANDARD"));
        // June 2 is summer time in Oslo: CEST = +0200. Previously this stub
        // asserted +0000, which encoded the very bug being fixed (a server
        // trusting the VTIMEZONE read the wall-clock as UTC and shifted the
        // event by 2h).
        assert!(body.contains("TZOFFSETFROM:+0200"));
        assert!(body.contains("TZOFFSETTO:+0200"));
        assert!(body.contains("DTSTART;TZID=Europe/Oslo:20260602T120000"));
        assert!(body.contains("DTEND;TZID=Europe/Oslo:20260602T130000"));
    }

    #[test]
    fn vtimezone_offset_tracks_winter_standard_time() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-01-15T12:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                end: EventTime {
                    value: "2026-01-15T13:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo".to_string()),
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        // January is winter time in Oslo: CET = +0100.
        assert!(body.contains("TZOFFSETTO:+0100"));
        assert!(body.contains("DTSTART;TZID=Europe/Oslo:20260115T120000"));
    }

    #[test]
    fn vtimezone_unknown_zone_omits_offset_block() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: None,
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00Z".to_string(),
                    timezone: Some("Custom/Bogus".to_string()),
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00Z".to_string(),
                    timezone: Some("Custom/Bogus".to_string()),
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        // The bare VTIMEZONE still names the zone for servers that resolve it
        // themselves, but we never assert a misleading offset for an unknown
        // zone.
        assert!(body.contains("BEGIN:VTIMEZONE"));
        assert!(body.contains("TZID:Custom/Bogus"));
        assert!(!body.contains("TZOFFSETTO"));
        assert!(!body.contains("BEGIN:STANDARD"));
    }

    #[test]
    fn tzid_offset_resolves_known_zone_and_instant() {
        let summer = NaiveDate::from_ymd_opt(2026, 6, 2)
            .and_then(|d| d.and_hms_opt(12, 0, 0))
            .expect("valid");
        let winter = NaiveDate::from_ymd_opt(2026, 1, 15)
            .and_then(|d| d.and_hms_opt(12, 0, 0))
            .expect("valid");
        assert_eq!(
            tzid_offset_for_naive("Europe/Oslo", Some(summer)).as_deref(),
            Some("+0200")
        );
        assert_eq!(
            tzid_offset_for_naive("Europe/Oslo", Some(winter)).as_deref(),
            Some("+0100")
        );
        // Windows/Exchange alias folds to IANA before parsing.
        assert_eq!(
            tzid_offset_for_naive("W. Europe Standard Time", Some(summer)).as_deref(),
            Some("+0200")
        );
        // Unknown zone yields no offset (graceful fallback, not +0000).
        assert_eq!(tzid_offset_for_naive("Custom/Bogus", Some(summer)), None);
    }

    #[test]
    fn create_payload_writes_transparency_and_classification() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
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
                visibility: EventVisibility::Private,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("STATUS:TENTATIVE"));
        assert!(body.contains("TRANSP:TRANSPARENT"));
        assert!(body.contains("CLASS:PRIVATE"));
    }

    #[test]
    fn patch_preserves_unmodeled_vevent_properties() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            Some("e1".to_string()),
            "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nSTATUS:CANCELLED\r\nORGANIZER;CN=Owner:mailto:owner@example.test\r\nTRANSP:TRANSPARENT\r\nCLASS:PRIVATE\r\nRECURRENCE-ID:20260609T120000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("title patch should serialize");

        assert!(body.contains("SUMMARY:New"));
        assert!(!body.contains("SUMMARY:Old"));
        assert!(body.contains("STATUS:CANCELLED"));
        assert!(body.contains("ORGANIZER;CN=Owner:mailto:owner@example.test"));
        assert!(body.contains("TRANSP:TRANSPARENT"));
        assert!(body.contains("CLASS:PRIVATE"));
        assert!(body.contains("RECURRENCE-ID:20260609T120000Z"));
    }

    #[test]
    fn patch_preserves_non_event_calendar_components() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            Some("e1".to_string()),
            "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTIMEZONE\r\nTZID:Europe/Oslo\r\nEND:VTIMEZONE\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nDTSTART;TZID=Europe/Oslo:20260602T120000\r\nDTEND;TZID=Europe/Oslo:20260602T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("title patch should serialize");

        assert!(body.contains("BEGIN:VTIMEZONE"));
        assert!(body.contains("TZID:Europe/Oslo"));
        assert!(body.contains("END:VTIMEZONE"));
        assert!(body.contains("SUMMARY:New"));
    }

    #[test]
    fn parses_transparency_and_classification() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nTRANSP:TRANSPARENT\r\nCLASS:CONFIDENTIAL\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.availability, EventAvailability::Free);
        assert_eq!(event.visibility, EventVisibility::Confidential);
    }

    #[test]
    fn missing_transparency_and_classification_default_to_unknown_and_default() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.availability, EventAvailability::Unknown);
        assert_eq!(event.visibility, EventVisibility::Default);
    }

    #[test]
    fn patch_replaces_transparency_and_classification() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            Some("e1".to_string()),
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Keep\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nTRANSP:OPAQUE\r\nCLASS:PUBLIC\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                availability: Some(EventAvailability::Free),
                visibility: Some(EventVisibility::Private),
                ..EventPatch::default()
            },
        )
        .expect("availability/visibility patch should serialize");

        assert!(body.contains("TRANSP:TRANSPARENT"));
        assert!(!body.contains("TRANSP:OPAQUE"));
        assert!(body.contains("CLASS:PRIVATE"));
        assert!(!body.contains("CLASS:PUBLIC"));
        assert!(body.contains("SUMMARY:Keep"));

        // Round-trips back to the modeled values written by the patch.
        let updated = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &body,
        );
        assert_eq!(updated.availability, EventAvailability::Free);
        assert_eq!(updated.visibility, EventVisibility::Private);
    }

    #[test]
    fn patch_clearing_visibility_to_default_strips_class() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nCLASS:PRIVATE\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                visibility: Some(EventVisibility::Default),
                ..EventPatch::default()
            },
        )
        .expect("visibility clear should serialize");

        assert!(!body.contains("CLASS:"));
    }

    #[test]
    fn patch_clear_removes_only_targeted_scalar_property() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nDESCRIPTION:Details\r\nLOCATION:Room\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nSTATUS:CONFIRMED\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                description: Some(None),
                ..EventPatch::default()
            },
        )
        .expect("description patch should serialize");

        assert!(!body.contains("DESCRIPTION:"));
        assert!(body.contains("SUMMARY:Old"));
        assert!(body.contains("LOCATION:Room"));
        assert!(body.contains("STATUS:CONFIRMED"));
    }

    #[test]
    fn rsvp_patch_updates_matching_attendee_only() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;PARTSTAT=NEEDS-ACTION:mailto:one@example.test\r\nATTENDEE;PARTSTAT=NEEDS-ACTION:mailto:two@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let patch = rsvp_patch(&current, RsvpStatus::Accepted, "two@example.test")
            .expect("matching attendee");
        let attendees = patch.attendees.expect("attendee patch");

        assert_eq!(attendees[0].status, RsvpStatus::NeedsAction);
        assert_eq!(attendees[1].status, RsvpStatus::Accepted);
    }

    #[test]
    fn rsvp_patch_rejects_missing_authenticated_attendee() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;PARTSTAT=NEEDS-ACTION:mailto:one@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let error = rsvp_patch(&current, RsvpStatus::Accepted, "two@example.test")
            .expect_err("missing attendee should fail");

        assert_eq!(error, "authenticated attendee not found");
    }

    #[test]
    fn rsvp_reply_ical_builds_itip_reply() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nORGANIZER;CN=Owner:mailto:owner@example.test\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;CN=Ada;PARTSTAT=NEEDS-ACTION:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = rsvp_reply_ical(&current, RsvpStatus::Accepted, "ada@example.test")
            .expect("itip reply");
        let unfolded = logical_line_groups(&body)
            .iter()
            .map(|group| {
                group
                    .physical
                    .iter()
                    .enumerate()
                    .map(|(index, line)| {
                        if index == 0 {
                            (*line).to_string()
                        } else {
                            // Strip exactly one leading fold WSP.
                            line[1..].to_string()
                        }
                    })
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(unfolded.contains("METHOD:REPLY"));
        assert!(unfolded.contains("UID:u1"));
        assert!(unfolded.contains("ORGANIZER;CN=Owner:mailto:owner@example.test"));
        assert!(unfolded.contains(
            "ATTENDEE;CN=Ada;ROLE=REQ-PARTICIPANT;PARTSTAT=ACCEPTED:mailto:ada@example.test"
        ));
    }

    #[test]
    fn recurrence_patch_replaces_stale_recurrence_lines() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY\r\nRDATE:20260603T120000Z\r\nEXDATE:20260604T120000Z\r\nSTATUS:CONFIRMED\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                recurrence: Some(EventRecurrence {
                    rrule: Some("FREQ=DAILY;COUNT=2".to_string()),
                    rdate: Vec::new(),
                    exdate: Vec::new(),
                    recurrence_id: None,
                }),
                ..EventPatch::default()
            },
        )
        .expect("recurrence patch should serialize");

        assert!(body.contains("RRULE:FREQ=DAILY;COUNT=2"));
        assert!(!body.contains("RRULE:FREQ=WEEKLY"));
        assert!(!body.contains("RDATE:20260603T120000Z"));
        assert!(!body.contains("EXDATE:20260604T120000Z"));
        assert!(body.contains("STATUS:CONFIRMED"));
    }

    #[test]
    fn recurrence_patch_rejects_override_vevents() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:Override\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let error = patch_to_ical(
            &current,
            &EventPatch {
                recurrence: Some(EventRecurrence {
                    rrule: Some("FREQ=DAILY;COUNT=2".to_string()),
                    rdate: Vec::new(),
                    exdate: Vec::new(),
                    recurrence_id: None,
                }),
                ..EventPatch::default()
            },
        )
        .expect_err("recurrence override patch should be refused");

        assert_eq!(error, "recurrence override patch unsupported");
    }

    #[test]
    fn scalar_patch_preserves_override_vevents() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Master\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:Override\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("Updated".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("scalar patch should preserve override VEVENTs");

        assert!(body.contains("SUMMARY:Updated"));
        assert!(body.contains("RECURRENCE-ID:20260609T120000Z"));
        assert!(body.contains("SUMMARY:Override"));
    }

    #[test]
    fn folds_long_output_lines() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: Some("A".repeat(90)),
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
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("\r\n "));
        // Every physical line, including space-prefixed continuation
        // lines, stays within the RFC 5545 75-octet budget.
        assert!(body.lines().all(|line| line.len() <= 75));
        assert!(body.lines().filter(|line| line.starts_with(' ')).count() >= 1);
    }

    #[test]
    fn fold_counts_continuation_space_in_octet_budget() {
        // 200 'A's forces multiple continuation lines; each continuation
        // line's leading space must count toward the 75-octet limit.
        let folded = fold_ical_lines(vec![format!("SUMMARY:{}", "A".repeat(200))]);
        assert!(folded.lines().all(|line| line.len() <= 75));
        assert!(folded.lines().filter(|line| line.starts_with(' ')).count() >= 2);
    }

    #[test]
    fn ignores_non_mailto_attendees() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;CN=Room:/principals/rooms/one\r\nATTENDEE;CN=Ada:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.attendees.len(), 1);
        assert_eq!(event.attendees[0].email, "ada@example.test");
    }

    #[test]
    fn escapes_text_and_params_without_raw_line_breaks() {
        let body = create_to_ical(
            &EventCreate {
                calendar_id: CalendarId("/cal/".to_string()),
                title: Some("Plan\r\nNext".to_string()),
                description: None,
                location: None,
                start: EventTime {
                    value: "2026-06-02T12:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo:Main".to_string()),
                },
                end: EventTime {
                    value: "2026-06-02T13:00:00Z".to_string(),
                    timezone: Some("Europe/Oslo:Main".to_string()),
                },
                is_all_day: false,
                status: EventStatus::Confirmed,
                availability: EventAvailability::Busy,
                visibility: EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: EventRecurrence::default(),
            },
            "uid-1",
        );

        assert!(body.contains("SUMMARY:Plan\\nNext"));
        assert!(body.contains("DTSTART;TZID=\"Europe/Oslo:Main\":"));
    }
}
