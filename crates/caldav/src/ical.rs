use bifrost_types::{
    AttendeeRole, CalendarEvent, CalendarId, CalendarProvenance, EventAttendee, EventAvailability,
    EventCreate, EventId, EventOrganizer, EventPatch, EventRecurrence, EventReminder, EventStatus,
    EventTime, EventVisibility, ProtocolKind, ReminderRelativeTo, ReminderTrigger, RsvpStatus,
};
use caldata::{ContentLineParser, LineReader};
use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, Utc};
use uuid::Uuid;

/// Projection failed because the resource body could not be tokenized into
/// content lines (unclosed quoted parameter, missing property name/value,
/// invalid UTF-8). The caller degrades a single bad `.ics` to a per-resource
/// skip routed through `failed_hrefs` rather than failing the whole sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IcalParseError(pub(crate) String);

/// Project the master (first) VEVENT of a resource. Used by the direct
/// `event_get` / `event_update` paths, which operate on the master.
pub(crate) fn event_from_ical(
    uri: String,
    calendar_id: CalendarId,
    etag: Option<String>,
    data: &str,
) -> Result<CalendarEvent, IcalParseError> {
    let block = parse_vevents(data)?
        .into_iter()
        .next()
        .ok_or_else(|| IcalParseError("iCalendar resource contains no VEVENT".to_string()))?;
    Ok(project_event(
        EventId(uri.clone()),
        uri,
        calendar_id,
        etag,
        data,
        block,
    ))
}

/// Project *every* VEVENT in a resource: the master plus each
/// recurrence override / cancellation. Each projected event carries its
/// own `RECURRENCE-ID` (in `recurrence.recurrence_id`) and `STATUS`, so
/// a `CANCELLED` override or a moved instance survives the range/search
/// listing instead of being discarded with the rest of the resource.
///
/// Override instances share the resource's native id but take a
/// recurrence-qualified `EventId` so they do not collide with the master
/// in a consumer index; `native_id` / provenance stay the resource uri.
///
/// A tokenizable resource that holds no VEVENT at all (a VTODO or
/// VJOURNAL sharing the collection, which is legal) yields no events
/// rather than a fabricated empty one: it is not a projection failure -
/// nothing is wrong with the resource and a retry cannot change the
/// outcome - so it belongs in neither the item lane nor `failed_ids`.
pub(crate) fn events_from_ical(
    uri: String,
    calendar_id: CalendarId,
    etag: Option<String>,
    data: &str,
) -> Result<Vec<CalendarEvent>, IcalParseError> {
    let blocks = parse_vevents(data)?;
    Ok(blocks
        .into_iter()
        .map(|block| {
            let recurrence_id = block
                .props
                .iter()
                .find(|prop| prop.name == "RECURRENCE-ID")
                .map(|prop| prop.value.clone());
            let id = match &recurrence_id {
                Some(recurrence_id) => EventId(format!("{uri}#{recurrence_id}")),
                None => EventId(uri.clone()),
            };
            project_event(
                id,
                uri.clone(),
                calendar_id.clone(),
                etag.clone(),
                data,
                block,
            )
        })
        .collect())
}

fn project_event(
    id: EventId,
    uri: String,
    calendar_id: CalendarId,
    etag: Option<String>,
    data: &str,
    block: VeventBlock,
) -> CalendarEvent {
    let reminders = reminders_from_valarms(&block.alarms);
    let props = Props(block.props);
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
        .map(event_time_from_property)
        .unwrap_or_else(default_time);
    let end = dtend
        .map(event_time_from_property)
        .or_else(|| {
            dtstart.and_then(|start| {
                props
                    .first("DURATION")
                    .and_then(|duration| event_end_from_duration(start, duration))
            })
        })
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
    CalendarEvent {
        id,
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
        reminders,
        recurrence,
        html_link: None,
        raw_ical: Some(data.to_string()),
    }
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
    push_time(&mut lines, "DTSTART", &event.start, event.is_all_day);
    push_time(&mut lines, "DTEND", &event.end, event.is_all_day);
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
        );
        replace_names.push("DTSTART");
    }
    if patch.end.is_some() || patch.is_all_day.is_some() {
        push_time(&mut replacements, "DTEND", &merged.end, merged.is_all_day);
        replace_names.extend(["DTEND", "DURATION"]);
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

    replace_first_vevent_properties(raw_ical, &replace_names, replacements)
        .ok_or("iCalendar body has no spliceable VEVENT")
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
    push_time(&mut lines, "DTSTART", &current.start, current.is_all_day);
    push_time(&mut lines, "DTEND", &current.end, current.is_all_day);
    lines.push(organizer_to_line(organizer));
    lines.push(attendee_to_line(&attendee));
    lines.push("END:VEVENT".to_string());
    lines.push("END:VCALENDAR".to_string());
    Ok(fold_ical_lines(lines))
}

pub(crate) fn new_uid() -> String {
    Uuid::new_v4().to_string()
}

/// One VEVENT component: its top-level properties plus each nested
/// VALARM's properties. RFC 5545 permits only VALARM sub-components
/// inside a VEVENT, so any other nested `BEGIN` line stays a top-level
/// property (harmless - nothing reads it).
#[derive(Debug, Default)]
struct VeventBlock {
    props: Vec<Prop>,
    alarms: Vec<Vec<Prop>>,
}

/// Tokenize a resource into every VEVENT block using caldata's streaming
/// content line tokenizer. caldata unfolds (stripping exactly one WSP per
/// RFC 5545 sec 3.1, not the whole leading run the old hand-rolled
/// unfolder ate) and splits quoted parameter values that legally contain
/// `:`/`;`/`,` - both classes of bug the previous `split_once(':')` path
/// had. Values are stored raw (caldata never unescapes); text fields are
/// unescaped at the point we read them into the model.
///
/// Every VEVENT is returned in document order (master first, then each
/// recurrence override / cancellation), and each block's nested VALARMs
/// are captured separately so reminders project without polluting the
/// event's own property list.
fn parse_vevents(data: &str) -> Result<Vec<VeventBlock>, IcalParseError> {
    let mut blocks = Vec::new();
    let mut current: Option<VeventBlock> = None;
    let mut alarm: Option<Vec<Prop>> = None;
    for line in LineReader::from_slice(data.as_bytes()) {
        let line = line.map_err(|error| IcalParseError(error.to_string()))?;
        let normalized = normalize_exchange_cn_param(line.as_str());
        let mut parser = ContentLineParser::from_slice(normalized.as_bytes());
        let line = parser
            .next()
            .expect("one logical line produces one content line")
            .map_err(|error| IcalParseError(error.to_string()))?;
        let name = line.name;
        if name == "BEGIN" && line.value.eq_ignore_ascii_case("VEVENT") {
            current = Some(VeventBlock::default());
            continue;
        }
        if name == "END" && line.value.eq_ignore_ascii_case("VEVENT") {
            if let Some(block) = current.take() {
                blocks.push(block);
            }
            alarm = None;
            continue;
        }
        let Some(block) = current.as_mut() else {
            // Content outside any VEVENT (VTIMEZONE, VCALENDAR props).
            continue;
        };
        if name == "BEGIN" && line.value.eq_ignore_ascii_case("VALARM") {
            alarm = Some(Vec::new());
            continue;
        }
        if name == "END" && line.value.eq_ignore_ascii_case("VALARM") {
            if let Some(alarm) = alarm.take() {
                block.alarms.push(alarm);
            }
            continue;
        }
        let prop = Prop {
            name,
            params: line.params,
            value: line.value,
        };
        match alarm.as_mut() {
            Some(alarm) => alarm.push(prop),
            None => block.props.push(prop),
        }
    }
    Ok(blocks)
}

/// Exchange sometimes emits an unquoted text-escaped CN (`CN=Doe\\, John`).
/// RFC 5545 requires a quoted parameter value here, and caldata correctly
/// treats an unquoted comma as a parameter-value separator, so the legacy
/// form has to be repaired before tokenization or the display name is
/// truncated at the comma.
///
/// The repair is deliberately narrow: only an unquoted CN value that
/// actually carries an RFC 5545 *text* escape (`\,`, `\;`, `\n`) is
/// rewritten, and the rewrite resolves those escapes here and re-encodes
/// the result with [`escape_param`]. Confining the tolerance to this one
/// legacy shape is what lets the CN read path stay a pure RFC 6868 decode:
/// a CN that merely contains a literal backslash is left alone and
/// round-trips verbatim, which a blanket `unescape_text` on every CN would
/// silently eat. RFC 6868 defines no escape for a backslash, so verbatim is
/// both the conformant encoding and the only one that survives a round trip.
fn normalize_exchange_cn_param(line: &str) -> String {
    let bytes = line.as_bytes();
    let mut index = 0;
    let mut in_quotes = false;
    let mut escaped = false;

    while index < bytes.len() {
        match bytes[index] {
            b'\\' if !in_quotes => escaped = !escaped,
            b'"' if !escaped => in_quotes = !in_quotes,
            b';' if !in_quotes && !escaped => {
                let name_start = index + 1;
                let mut name_end = name_start;
                while name_end < bytes.len()
                    && bytes[name_end] != b'='
                    && bytes[name_end] != b';'
                    && bytes[name_end] != b':'
                {
                    name_end += 1;
                }
                if bytes.get(name_end) == Some(&b'=')
                    && line[name_start..name_end].eq_ignore_ascii_case("CN")
                    && bytes.get(name_end + 1) != Some(&b'"')
                {
                    let value_start = name_end + 1;
                    let mut value_end = value_start;
                    let mut value_escaped = false;
                    while value_end < bytes.len() {
                        match bytes[value_end] {
                            b'\\' => value_escaped = !value_escaped,
                            b';' | b':' if !value_escaped => break,
                            _ => value_escaped = false,
                        }
                        value_end += 1;
                    }
                    let value = &line[value_start..value_end];
                    // Only a separator escape marks the value as legacy: a
                    // bare `\` (or `\n`) in an unquoted value does not
                    // confuse the tokenizer, so it stays verbatim.
                    if value.contains("\\,") || value.contains("\\;") {
                        return format!(
                            "{}{}{}",
                            &line[..value_start],
                            escape_param(&unescape_text(value)),
                            &line[value_end..]
                        );
                    }
                }
            }
            b':' if !in_quotes && !escaped => break,
            _ => escaped = false,
        }
        index += 1;
    }
    line.to_string()
}

/// Project each VALARM into an `EventReminder`, dropping alarms with no
/// usable TRIGGER.
fn reminders_from_valarms(alarms: &[Vec<Prop>]) -> Vec<EventReminder> {
    alarms
        .iter()
        .filter_map(|alarm| reminder_from_valarm(alarm))
        .collect()
}

fn reminder_from_valarm(alarm: &[Prop]) -> Option<EventReminder> {
    let trigger_prop = alarm.iter().find(|prop| prop.name == "TRIGGER")?;
    let trigger = trigger_from_property(trigger_prop)?;
    let action = alarm
        .iter()
        .find(|prop| prop.name == "ACTION")
        .map(|prop| prop.value.to_ascii_uppercase());
    Some(EventReminder { trigger, action })
}

/// Read a VALARM TRIGGER. Default value type is DURATION (relative to
/// the event start, or end when `RELATED=END`); `VALUE=DATE-TIME`
/// (or a bare UTC date-time) is an absolute trigger.
fn trigger_from_property(prop: &Prop) -> Option<ReminderTrigger> {
    let value = prop.value.trim();
    if value.is_empty() {
        return None;
    }
    let looks_duration = value
        .strip_prefix(['-', '+'])
        .unwrap_or(value)
        .starts_with('P');
    let is_duration = match prop.param("VALUE") {
        Some(kind) if kind.eq_ignore_ascii_case("DATE-TIME") => false,
        Some(kind) if kind.eq_ignore_ascii_case("DURATION") => true,
        _ => looks_duration,
    };
    if is_duration {
        let relative_to = match prop.param("RELATED") {
            Some(related) if related.eq_ignore_ascii_case("END") => ReminderRelativeTo::End,
            _ => ReminderRelativeTo::Start,
        };
        Some(ReminderTrigger::Relative {
            offset: value.to_string(),
            relative_to,
        })
    } else {
        Some(ReminderTrigger::Absolute(format_ical_time(
            value, false, false,
        )))
    }
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

    /// A parameter carrying free text (CN, TZID) rather than a grammar
    /// token. caldata hands back the raw parameter, so the RFC 6868 caret
    /// decoding that undoes [`escape_param`] happens here; without it a
    /// display name written by us - or by any of the many servers that
    /// caret-encode - reads back with `^'` in it.
    fn text_param(&self, name: &str) -> Option<String> {
        self.param(name).map(unescape_param)
    }

    fn value_type_date(&self) -> bool {
        self.param("VALUE")
            .is_some_and(|value| value.eq_ignore_ascii_case("DATE"))
    }
}

fn event_time_from_property(prop: &Prop) -> EventTime {
    let tzid = prop.text_param("TZID");
    EventTime {
        value: format_ical_time(&prop.value, prop.value_type_date(), tzid.is_some()),
        // Map Microsoft/Windows zone names ("W. Europe Standard Time") to
        // their IANA equivalent when caldata's table knows them; otherwise
        // pass the TZID through verbatim.
        timezone: tzid.as_deref().map(canonical_tzid),
    }
}

/// Resolve a raw TZID to its IANA name when it is a known Microsoft/Windows
/// zone alias, leaving already-IANA (or unknown) names untouched.
fn canonical_tzid(tzid: &str) -> String {
    caldata::types::get_proprietary_tzid(tzid)
        .map(|tz| tz.name().to_string())
        .unwrap_or_else(|| tzid.to_string())
}

fn format_ical_time(value: &str, is_date: bool, has_tzid: bool) -> String {
    if !value.is_ascii() {
        return value.to_string();
    }
    if is_date && value.len() == 8 {
        // All-day dates project verbatim. The iCalendar all-day DTEND is
        // already exclusive and `EventTime`'s all-day contract is exclusive
        // too, so an end date is NOT decremented to an inclusive last day
        // (that would diverge from bifrost-google, which passes Google's
        // exclusive end through verbatim).
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
    if !value.is_ascii() {
        return String::new();
    }
    if value.ends_with('Z') {
        return "Z".to_string();
    }
    if value.len() == 20 {
        let offset = &value[15..20];
        if matches!(&offset[0..1], "+" | "-") {
            return format!("{}:{}", &offset[0..3], &offset[3..5]);
        }
    }
    String::new()
}

fn event_end_from_duration(start: &Prop, duration: &str) -> Option<EventTime> {
    let duration = caldata::types::parse_duration(duration).ok()?;
    let start = event_time_from_property(start);
    let value = if let Ok(value) = DateTime::parse_from_rfc3339(&start.value) {
        let end = value.checked_add_signed(duration)?;
        end.to_rfc3339_opts(SecondsFormat::Secs, start.value.ends_with('Z'))
    } else if let Ok(value) = NaiveDateTime::parse_from_str(&start.value, "%Y-%m-%dT%H:%M:%S") {
        value
            .checked_add_signed(duration)?
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string()
    } else if let Ok(value) = NaiveDate::parse_from_str(&start.value, "%Y-%m-%d") {
        value
            .and_hms_opt(0, 0, 0)?
            .checked_add_signed(duration)?
            .date()
            .format("%Y-%m-%d")
            .to_string()
    } else {
        return None;
    };
    Some(EventTime {
        value,
        timezone: start.timezone,
    })
}

fn ical_time_from_event_time(time: &EventTime, is_all_day: bool) -> String {
    if is_all_day {
        // `EventTime`'s all-day end is exclusive, matching iCalendar's
        // exclusive DTEND, so the date serializes verbatim - no +1 day.
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
        // A pure RFC 6868 decode. Legacy Exchange text escapes are resolved
        // before tokenization by `normalize_exchange_cn_param`; applying
        // `unescape_text` again here would eat a literal backslash.
        name: prop.text_param("CN"),
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
        name: prop.text_param("CN"),
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
        .get(.."mailto:".len())
        .filter(|prefix| prefix.eq_ignore_ascii_case("mailto:"))
        .and_then(|_| value.get("mailto:".len()..))
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

fn push_time(lines: &mut Vec<String>, name: &str, time: &EventTime, is_all_day: bool) {
    let value = ical_time_from_event_time(time, is_all_day);
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
///
/// Returns `None` when the body gave the splice nowhere to land: no
/// `BEGIN:VEVENT`, or a first VEVENT whose nested components or own
/// `END` never close. Emitting the body anyway would drop every patched
/// property and PUT an unchanged resource back, which the caller cannot
/// tell apart from a successful edit.
fn replace_first_vevent_properties(
    raw_ical: &str,
    replace_names: &[&str],
    replacements: Vec<String>,
) -> Option<String> {
    let mut out = String::new();
    let mut replacements = Some(replacements);
    let mut in_first_event = false;
    let mut finished_first_event = false;
    let mut nested_component_depth = 0_usize;
    for group in logical_line_groups(raw_ical) {
        let name = ical_line_name(group.logical_head());
        if name.is_some_and(|name| name.eq_ignore_ascii_case("BEGIN"))
            && line_value(group.logical_head()).is_some_and(|v| v.eq_ignore_ascii_case("VEVENT"))
            && !finished_first_event
        {
            in_first_event = true;
            group.push_verbatim(&mut out);
            continue;
        }
        if in_first_event
            && nested_component_depth == 0
            && name.is_some_and(|name| name.eq_ignore_ascii_case("END"))
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
        if in_first_event
            && name.is_some_and(|name| name.eq_ignore_ascii_case("BEGIN"))
            && line_value(group.logical_head()).is_some()
        {
            if nested_component_depth == 0
                && let Some(replacements) = replacements.take()
            {
                out.push_str(&fold_ical_lines(replacements));
            }
            nested_component_depth += 1;
            group.push_verbatim(&mut out);
            continue;
        }
        if in_first_event
            && nested_component_depth > 0
            && name.is_some_and(|name| name.eq_ignore_ascii_case("END"))
            && line_value(group.logical_head()).is_some()
        {
            nested_component_depth -= 1;
            group.push_verbatim(&mut out);
            continue;
        }
        if in_first_event
            && nested_component_depth == 0
            && name.is_some_and(|name| {
                replace_names
                    .iter()
                    .any(|replace| name.eq_ignore_ascii_case(replace))
            })
        {
            continue;
        }
        group.push_verbatim(&mut out);
    }
    replacements.is_none().then_some(out)
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

/// RFC 6868 parameter-value decoding, the inverse of [`escape_param`]:
/// `^n` is a newline, `^'` a `"`, `^^` a caret. A caret before anything
/// else is literal, so a value from a producer that predates RFC 6868
/// survives unchanged.
fn unescape_param(value: &str) -> String {
    if !value.contains('^') {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '^' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('\'') => out.push('"'),
            Some('^') => out.push('^'),
            Some(other) => {
                out.push('^');
                out.push(other);
            }
            None => out.push('^'),
        }
    }
    out
}

fn escape_param(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '^' => escaped.push_str("^^"),
            '"' => escaped.push_str("^'"),
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                escaped.push_str("^n");
            }
            '\n' => escaped.push_str("^n"),
            _ => escaped.push(ch),
        }
    }
    if escaped.contains([';', ',', ':']) {
        format!("\"{escaped}\"")
    } else {
        escaped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

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
        // A single all-day event on 2026-06-02 is start 2026-06-02, end
        // 2026-06-03 under the exclusive-end contract. It serializes to an
        // exclusive iCalendar DTEND verbatim (no +1) and parses back
        // verbatim (no -1).
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
                    value: "2026-06-03".to_string(),
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
        assert_eq!(event.end.value, "2026-06-03");
    }

    #[test]
    fn dtend_date_marks_event_all_day() {
        // The exclusive iCalendar all-day DTEND passes through verbatim
        // into the exclusive `EventTime` end - no inclusive decrement.
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T000000Z\r\nDTEND;VALUE=DATE:20260603\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert!(event.is_all_day);
        assert_eq!(event.end.value, "2026-06-03");
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
    fn events_from_ical_projects_master_and_overrides() {
        // The whole resource projects: master (RRULE) plus a moved
        // override and a cancellation, each carrying its RECURRENCE-ID and
        // STATUS. Overrides take a recurrence-qualified id but keep the
        // resource native id.
        let events = events_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Master\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\nSUMMARY:Moved\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260616T120000Z\r\nSTATUS:CANCELLED\r\nDTSTART:20260616T120000Z\r\nDTEND:20260616T130000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        )
        .expect("valid iCalendar projects");

        assert_eq!(events.len(), 3);

        let master = &events[0];
        assert_eq!(master.title.as_deref(), Some("Master"));
        assert_eq!(master.id.0, "/cal/one.ics");
        assert_eq!(master.recurrence.recurrence_id, None);
        assert_eq!(
            master.recurrence.rrule.as_deref(),
            Some("FREQ=WEEKLY;COUNT=3")
        );

        let moved = &events[1];
        assert_eq!(moved.title.as_deref(), Some("Moved"));
        assert_eq!(moved.id.0, "/cal/one.ics#20260609T120000Z");
        assert_eq!(moved.native_id, "/cal/one.ics");
        assert_eq!(
            moved.recurrence.recurrence_id.as_deref(),
            Some("20260609T120000Z")
        );

        let cancelled = &events[2];
        assert_eq!(cancelled.status, EventStatus::Cancelled);
        assert_eq!(
            cancelled.recurrence.recurrence_id.as_deref(),
            Some("20260616T120000Z")
        );
    }

    #[test]
    fn valarm_projects_relative_and_absolute_reminders() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\nBEGIN:VALARM\r\nACTION:EMAIL\r\nTRIGGER;RELATED=END:PT5M\r\nEND:VALARM\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER;VALUE=DATE-TIME:20260602T110000Z\r\nEND:VALARM\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.reminders.len(), 3);
        assert_eq!(
            event.reminders[0].trigger,
            ReminderTrigger::Relative {
                offset: "-PT15M".to_string(),
                relative_to: ReminderRelativeTo::Start,
            }
        );
        assert_eq!(event.reminders[0].action.as_deref(), Some("DISPLAY"));
        assert_eq!(
            event.reminders[1].trigger,
            ReminderTrigger::Relative {
                offset: "PT5M".to_string(),
                relative_to: ReminderRelativeTo::End,
            }
        );
        assert_eq!(
            event.reminders[2].trigger,
            ReminderTrigger::Absolute("2026-06-02T11:00:00Z".to_string())
        );
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
    fn non_ascii_datetime_value_does_not_panic_in_projection() {
        let body = format!(
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:{}\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            "\u{20ac}".repeat(5)
        );
        let event = event_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &body,
        )
        .expect("non-ASCII property value remains a per-resource value");

        assert_eq!(event.start.value, "\u{20ac}".repeat(5));
    }

    #[test]
    fn duration_based_end_is_projected() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDURATION:PT1H\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.value, "2026-06-02T12:00:00Z");
        assert_eq!(event.end.value, "2026-06-02T13:00:00Z");
    }

    #[test]
    fn end_patch_replaces_duration_instead_of_emitting_both() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\nDURATION:PT1H\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                end: Some(EventTime {
                    value: "2026-06-02T14:00:00Z".to_string(),
                    timezone: None,
                }),
                ..EventPatch::default()
            },
        )
        .expect("end patch should serialize");

        assert!(body.contains("DTEND:20260602T140000Z"));
        assert!(!body.contains("DURATION:"));
    }

    #[test]
    fn scalar_patch_preserves_same_named_valarm_properties() {
        let current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDESCRIPTION:Event\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nDESCRIPTION:Ring\r\nEND:VALARM\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = patch_to_ical(
            &current,
            &EventPatch {
                description: Some(Some("Updated".to_string())),
                ..EventPatch::default()
            },
        )
        .expect("description patch should serialize");

        assert!(body.contains("DESCRIPTION:Updated\r\nBEGIN:VALARM"));
        assert!(body.contains("DESCRIPTION:Ring\r\nEND:VALARM"));
        assert!(!body.contains("DESCRIPTION:Event"));
    }

    #[test]
    fn resource_without_vevent_is_rejected() {
        let result = event_from_ical(
            "/cal/todo.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:t1\r\nEND:VTODO\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(
            result.expect_err("VTODO-only resource is not an event").0,
            "iCalendar resource contains no VEVENT"
        );
    }

    #[test]
    fn unspliceable_body_fails_instead_of_writing_back_unchanged() {
        // A truncated VEVENT reaches neither splice point (the first nested
        // component boundary, or END:VEVENT). Returning the body verbatim
        // would PUT the pre-patch resource back and report success, with the
        // patched properties silently gone.
        let mut current = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );
        current.raw_ical =
            Some("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\n".to_string());

        let result = patch_to_ical(
            &current,
            &EventPatch {
                title: Some(Some("New".to_string())),
                ..EventPatch::default()
            },
        );

        assert_eq!(result, Err("iCalendar body has no spliceable VEVENT"));
    }

    #[test]
    fn bulk_projection_skips_a_resource_without_vevent() {
        // The listing lanes must not fabricate an empty event for a VTODO
        // sharing the collection: `event_in_range` waves through an event
        // with an empty start, so the phantom would reach the consumer and
        // then fail to open, `event_get` having rejected the same body.
        let events = events_from_ical(
            "/cal/todo.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:t1\r\nEND:VTODO\r\nEND:VCALENDAR\r\n",
        )
        .expect("a VEVENT-less resource is not a projection failure");

        assert!(events.is_empty());
    }

    #[test]
    fn parameter_values_use_rfc6868_caret_encoding() {
        assert_eq!(escape_param("Ada \"Ace\" ^ Team"), "Ada ^'Ace^' ^^ Team");
        assert_eq!(escape_param("one\r\ntwo"), "one^ntwo");
    }

    #[test]
    fn caret_encoded_parameter_round_trips_through_projection() {
        // Writing carets a reader keeps verbatim would surface `^'` as part
        // of an attendee's display name.
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nATTENDEE;CN=Ada ^'Ace^' Lovelace:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(
            event.attendees[0].name.as_deref(),
            Some("Ada \"Ace\" Lovelace")
        );
    }

    #[test]
    fn attendee_cn_with_a_literal_backslash_round_trips() {
        let name = "Doe\\n John";
        let line = attendee_to_line(&EventAttendee {
            email: "doe@example.test".to_string(),
            name: Some(name.to_string()),
            role: AttendeeRole::Required,
            status: RsvpStatus::NeedsAction,
        });
        let data = format!(
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\n{line}\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
        );

        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &data,
        );

        // RFC 6868 defines no backslash escape, so the wire form is the
        // literal character. Doubling it would be invented syntax that only
        // this crate could read back.
        assert!(line.contains("CN=Doe\\n John"));
        assert_eq!(event.attendees[0].name.as_deref(), Some(name));
    }

    #[test]
    fn tzid_with_a_literal_backslash_round_trips() {
        let tzid = "Custom\\Zone";
        let line = format!("DTSTART;TZID={}:20260602T120000", super::escape_param(tzid));
        let data = format!(
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\n{line}\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
        );

        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            &data,
        );

        assert!(line.contains("TZID=Custom\\Zone:"));
        assert_eq!(event.start.timezone.as_deref(), Some(tzid));
    }

    #[test]
    fn attendee_cn_accepts_exchange_style_text_escapes_on_read() {
        let event = parse_event(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nATTENDEE;CN=Doe\\, John:mailto:doe@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.attendees[0].name.as_deref(), Some("Doe, John"));
    }

    #[test]
    fn unescape_text_handles_adjacent_backslashes_in_a_single_pass() {
        // Raw `\\n` is an escaped backslash followed by a literal n, not a
        // newline; an ordering-dependent replace-chain corrupts it.
        assert_eq!(unescape_text("a\\\\nb"), "a\\nb");
        assert_eq!(unescape_text("line\\nbreak"), "line\nbreak");
        assert_eq!(unescape_text("big\\Nbreak"), "big\nbreak");
        assert_eq!(unescape_text("semi\\;comma\\,"), "semi;comma,");
        // Unknown escapes and a trailing backslash pass through verbatim.
        assert_eq!(unescape_text("odd\\x"), "odd\\x");
        assert_eq!(unescape_text("tail\\"), "tail\\");
    }

    #[test]
    fn escape_then_unescape_round_trips_text() {
        let original = "a,b;c\\d\nnewline";
        assert_eq!(unescape_text(&escape_text(original)), original);
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
