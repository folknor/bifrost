use bifrost_types::{
    AttendeeRole, CalendarEvent, CalendarId, CalendarProvenance, EventAttendee, EventAvailability,
    EventCreate, EventId, EventOrganizer, EventPatch, EventRecurrence, EventStatus, EventTime,
    EventVisibility, ProtocolKind, RsvpStatus,
};
use chrono::{DateTime, Days, NaiveDate, Utc};
use uuid::Uuid;

pub(crate) fn event_from_ical(
    uri: String,
    calendar_id: CalendarId,
    etag: Option<String>,
    data: &str,
) -> CalendarEvent {
    let props = parse_vevent(data);
    let uid = props.first("UID").map(ToString::to_string);
    let title = props.first("SUMMARY").map(unescape_text);
    let description = props.first("DESCRIPTION").map(unescape_text);
    let location = props.first("LOCATION").map(unescape_text);
    let start = props
        .first_with_name("DTSTART")
        .map(|prop| event_time_from_property(prop, false))
        .unwrap_or_else(default_time);
    let end = props
        .first_with_name("DTEND")
        .map(|prop| event_time_from_property(prop, true))
        .unwrap_or_else(default_time);
    let is_all_day = props
        .first_with_name("DTSTART")
        .is_some_and(Prop::value_type_date)
        || props
            .first_with_name("DTEND")
            .is_some_and(Prop::value_type_date);
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
        availability: EventAvailability::Unknown,
        visibility: EventVisibility::Default,
        self_response: RsvpStatus::Unknown,
        organizer,
        attendees,
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

fn parse_vevent(data: &str) -> Props {
    let mut props = Vec::new();
    let mut in_event = false;
    for line in unfold_lines(data) {
        let Some((raw_name, value)) = line.split_once(':') else {
            continue;
        };
        let mut name_parts = raw_name.split(';');
        let name = name_parts.next().unwrap_or_default().to_ascii_uppercase();
        if name == "BEGIN" && value.eq_ignore_ascii_case("VEVENT") {
            in_event = true;
            continue;
        }
        if name == "END" && value.eq_ignore_ascii_case("VEVENT") {
            break;
        }
        if !in_event {
            continue;
        }
        let params = name_parts
            .filter_map(|part| part.split_once('='))
            .map(|(key, value)| {
                (
                    key.to_ascii_uppercase(),
                    value.trim_matches('"').to_string(),
                )
            })
            .collect();
        props.push(Prop {
            name,
            params,
            value: value.to_string(),
        });
    }
    Props(props)
}

fn unfold_lines(data: &str) -> Vec<String> {
    let mut lines = Vec::<String>::new();
    for raw in data.lines() {
        let line = raw.trim_end_matches('\r');
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(last) = lines.last_mut() {
                last.push_str(line.trim_start());
            }
        } else {
            lines.push(line.to_string());
        }
    }
    lines
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
    params: Vec<(String, String)>,
    value: String,
}

impl Prop {
    fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    fn value_type_date(&self) -> bool {
        self.param("VALUE")
            .is_some_and(|value| value.eq_ignore_ascii_case("DATE"))
    }
}

fn event_time_from_property(prop: &Prop, is_end: bool) -> EventTime {
    EventTime {
        value: format_ical_time(&prop.value, prop.value_type_date(), is_end),
        timezone: prop.param("TZID").map(ToString::to_string),
    }
}

fn format_ical_time(value: &str, is_date: bool, is_end: bool) -> String {
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
        if suffix.is_empty() {
            formatted.push('Z');
        } else if let Ok(time) = DateTime::parse_from_rfc3339(&formatted) {
            formatted = time.to_rfc3339();
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
        lines.push("BEGIN:VTIMEZONE".to_string());
        lines.push(format!("TZID:{}", escape_text(&tzid)));
        lines.push("BEGIN:STANDARD".to_string());
        lines.push("DTSTART:19700101T000000".to_string());
        lines.push("TZOFFSETFROM:+0000".to_string());
        lines.push("TZOFFSETTO:+0000".to_string());
        lines.push("END:STANDARD".to_string());
        lines.push("END:VTIMEZONE".to_string());
    }
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

fn replace_first_vevent_properties(
    raw_ical: &str,
    replace_names: &[&str],
    replacements: Vec<String>,
) -> String {
    let mut lines = Vec::new();
    let mut in_first_event = false;
    let mut finished_first_event = false;
    for line in unfold_lines(raw_ical) {
        let name = ical_line_name(&line);
        if name == Some("BEGIN")
            && line_value(&line).is_some_and(|value| value.eq_ignore_ascii_case("VEVENT"))
            && !finished_first_event
        {
            in_first_event = true;
            lines.push(line);
            continue;
        }
        if in_first_event
            && name == Some("END")
            && line_value(&line).is_some_and(|value| value.eq_ignore_ascii_case("VEVENT"))
        {
            lines.extend(replacements.clone());
            in_first_event = false;
            finished_first_event = true;
            lines.push(line);
            continue;
        }
        if in_first_event && name.is_some_and(|name| replace_names.contains(&name)) {
            continue;
        }
        lines.push(line);
    }
    fold_ical_lines(lines)
}

fn has_recurrence_override_vevent(raw_ical: &str) -> bool {
    let mut in_event = false;
    for line in unfold_lines(raw_ical) {
        let name = ical_line_name(&line);
        if name == Some("BEGIN")
            && line_value(&line).is_some_and(|value| value.eq_ignore_ascii_case("VEVENT"))
        {
            in_event = true;
            continue;
        }
        if in_event
            && name == Some("END")
            && line_value(&line).is_some_and(|value| value.eq_ignore_ascii_case("VEVENT"))
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
        for ch in line.chars() {
            if current.len() + ch.len_utf8() > 75 {
                folded.push_str(&current);
                folded.push_str("\r\n ");
                current.clear();
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

fn unescape_text(value: &str) -> String {
    value
        .replace("\\n", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
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

    #[test]
    fn parses_basic_vevent() {
        let event = event_from_ical(
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
        let event = event_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000+0200\r\nDTEND:20260602T130000+0200\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.value, "2026-06-02T12:00:00+02:00");
        assert_eq!(event.end.value, "2026-06-02T13:00:00+02:00");
    }

    #[test]
    fn parses_tzid_datetime_as_rfc3339_with_timezone_metadata() {
        let event = event_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART;TZID=Europe/Oslo:20260602T120000\r\nDTEND;TZID=Europe/Oslo:20260602T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        assert_eq!(event.start.value, "2026-06-02T12:00:00Z");
        assert_eq!(event.start.timezone.as_deref(), Some("Europe/Oslo"));
        assert_eq!(event.end.value, "2026-06-02T13:00:00Z");
        assert_eq!(event.end.timezone.as_deref(), Some("Europe/Oslo"));
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

        let event = event_from_ical(
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
        let event = event_from_ical(
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
        let event = event_from_ical(
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
        let event = event_from_ical(
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
        assert!(body.contains("TZOFFSETFROM:+0000"));
        assert!(body.contains("TZOFFSETTO:+0000"));
        assert!(body.contains("DTSTART;TZID=Europe/Oslo:20260602T120000"));
        assert!(body.contains("DTEND;TZID=Europe/Oslo:20260602T130000"));
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
        let current = event_from_ical(
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
        let current = event_from_ical(
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
    fn patch_clear_removes_only_targeted_scalar_property() {
        let current = event_from_ical(
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
        let current = event_from_ical(
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
        let current = event_from_ical(
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
        let current = event_from_ical(
            "/cal/one.ics".to_string(),
            CalendarId("/cal/".to_string()),
            None,
            "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nORGANIZER;CN=Owner:mailto:owner@example.test\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nATTENDEE;CN=Ada;PARTSTAT=NEEDS-ACTION:mailto:ada@example.test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );

        let body = rsvp_reply_ical(&current, RsvpStatus::Accepted, "ada@example.test")
            .expect("itip reply");
        let unfolded = unfold_lines(&body).join("\n");

        assert!(unfolded.contains("METHOD:REPLY"));
        assert!(unfolded.contains("UID:u1"));
        assert!(unfolded.contains("ORGANIZER;CN=Owner:mailto:owner@example.test"));
        assert!(unfolded.contains(
            "ATTENDEE;CN=Ada;ROLE=REQ-PARTICIPANT;PARTSTAT=ACCEPTED:mailto:ada@example.test"
        ));
    }

    #[test]
    fn recurrence_patch_replaces_stale_recurrence_lines() {
        let current = event_from_ical(
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
        let current = event_from_ical(
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
        let current = event_from_ical(
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
        assert!(body.lines().all(|line| line.len() <= 75));
    }

    #[test]
    fn ignores_non_mailto_attendees() {
        let event = event_from_ical(
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
