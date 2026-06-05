use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::Event;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalendarCollection {
    pub(crate) href: String,
    pub(crate) display_name: Option<String>,
    pub(crate) color: Option<String>,
    pub(crate) can_edit: Option<bool>,
    pub(crate) sync_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalDavEventEntry {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalDavFetchedEvent {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
    pub(crate) data: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalDavSyncReport {
    pub(crate) sync_token: Option<String>,
    pub(crate) entries: Vec<CalDavSyncEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalDavSyncEntry {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
    pub(crate) status: Option<u16>,
}

pub(crate) fn parse_calendar_collections(xml: &str) -> Result<Vec<CalendarCollection>, String> {
    let mut reader = Reader::from_str(xml);
    let mut stack = Vec::new();
    let mut text = String::new();
    let mut current = ResponseParts::default();
    let mut collections = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_name(element.name().as_ref());
                if name == "response" {
                    current = ResponseParts {
                        in_response: true,
                        ..ResponseParts::default()
                    };
                }
                if current.in_response && name == "propstat" {
                    current.begin_propstat();
                }
                if current.in_response && name == "calendar" {
                    current.mark_calendar();
                }
                if current.in_response && name == "privilege" {
                    current.mark_privilege_seen();
                }
                if current.in_response
                    && (name == "write" || name == "write-content" || name == "all")
                    && stack.iter().any(|item| item == "privilege")
                {
                    current.mark_write_seen();
                }
                stack.push(name);
                text.clear();
            }
            Ok(Event::Empty(element)) => {
                let name = local_name(element.name().as_ref());
                if current.in_response && name == "calendar" {
                    current.mark_calendar();
                }
                if current.in_response && name == "privilege" {
                    current.mark_privilege_seen();
                }
                if current.in_response
                    && (name == "write" || name == "write-content" || name == "all")
                    && stack.iter().any(|item| item == "privilege")
                {
                    current.mark_write_seen();
                }
            }
            Ok(Event::Text(value)) => push_text(&mut text, value.as_ref())?,
            Ok(Event::CData(value)) => {
                let value = value.decode().map_err(|error| error.to_string())?;
                text.push_str(&value);
            }
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                if current.in_response {
                    match (parent, name.as_str()) {
                        (Some("response"), "href") => current.href = trimmed(&text),
                        (Some("prop"), "displayname") => {
                            current.propstat_display_name = trimmed(&text);
                        }
                        (Some("prop"), "calendar-color") => {
                            current.propstat_color =
                                trimmed(&text).map(|value| value.trim().to_string());
                        }
                        (Some("prop"), "sync-token") => {
                            current.propstat_sync_token = trimmed(&text);
                        }
                        (Some("propstat"), "status") => {
                            current.propstat_success =
                                status_code(&text).map(|code| matches!(code, 200..=299));
                        }
                        _ => {}
                    }
                }
                if name == "propstat" {
                    current.commit_propstat();
                }
                if name == "response" {
                    current.in_response = false;
                    if let Some(collection) = current.as_calendar_collection() {
                        collections.push(collection);
                    }
                }
                stack.pop();
                text.clear();
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("XML parse error: {error}")),
        }
    }

    Ok(collections)
}

pub(crate) fn parse_propfind_events(xml: &str) -> Result<Vec<CalDavEventEntry>, String> {
    let mut reader = Reader::from_str(xml);
    let mut entries = Vec::new();
    let mut current = ResponseParts::default();
    let mut stack = Vec::new();
    let mut text = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_name(element.name().as_ref());
                if name == "response" {
                    current = ResponseParts::default();
                    current.in_response = true;
                }
                if current.in_response && name == "collection" {
                    current.is_collection = true;
                }
                stack.push(name);
                text.clear();
            }
            Ok(Event::Empty(element)) => {
                let name = local_name(element.name().as_ref());
                if current.in_response && name == "collection" {
                    current.is_collection = true;
                }
            }
            Ok(Event::Text(value)) => push_text(&mut text, value.as_ref())?,
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                if current.in_response {
                    match (parent, name.as_str()) {
                        (Some("response"), "href") => current.href = trimmed(&text),
                        (Some("prop"), "getetag") => current.etag = normalize_etag(&text),
                        (Some("prop"), "getcontenttype") => current.content_type = trimmed(&text),
                        _ => {}
                    }
                }
                if name == "response" {
                    current.in_response = false;
                    if let Some(entry) = current.as_event_entry() {
                        entries.push(entry);
                    }
                }
                stack.pop();
                text.clear();
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("XML parse error: {error}")),
        }
    }

    Ok(entries)
}

pub(crate) fn parse_multiget_report(xml: &str) -> Result<Vec<CalDavFetchedEvent>, String> {
    let mut reader = Reader::from_str(xml);
    let mut results = Vec::new();
    let mut current = ResponseParts::default();
    let mut stack = Vec::new();
    let mut text = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_name(element.name().as_ref());
                if name == "response" {
                    current = ResponseParts::default();
                    current.in_response = true;
                }
                if current.in_response && name == "collection" {
                    current.is_collection = true;
                }
                stack.push(name);
                text.clear();
            }
            Ok(Event::Empty(element)) => {
                let name = local_name(element.name().as_ref());
                if current.in_response && name == "collection" {
                    current.is_collection = true;
                }
            }
            Ok(Event::Text(value)) => push_text(&mut text, value.as_ref())?,
            Ok(Event::CData(value)) => {
                let value = value.decode().map_err(|error| error.to_string())?;
                text.push_str(&value);
            }
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                if current.in_response {
                    match (parent, name.as_str()) {
                        (Some("response"), "href") => current.href = trimmed(&text),
                        (Some("prop"), "getetag") => current.etag = normalize_etag(&text),
                        (Some("prop"), "calendar-data") => current.calendar_data = trimmed(&text),
                        (Some("propstat"), "status") => current.status = trimmed(&text),
                        _ => {}
                    }
                }
                if name == "response" {
                    current.in_response = false;
                    if let Some(status) = current.error_status() {
                        return Err(format!(
                            "multiget response for {} returned {status}",
                            current.href.as_deref().unwrap_or("<unknown>")
                        ));
                    }
                    if let Some(event) = current.as_fetched_event() {
                        results.push(event);
                    }
                }
                stack.pop();
                text.clear();
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("XML parse error: {error}")),
        }
    }

    Ok(results)
}

pub(crate) fn parse_sync_collection_report(xml: &str) -> Result<CalDavSyncReport, String> {
    let mut reader = Reader::from_str(xml);
    let mut report = CalDavSyncReport {
        sync_token: None,
        entries: Vec::new(),
    };
    let mut current = ResponseParts::default();
    let mut stack = Vec::new();
    let mut text = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_name(element.name().as_ref());
                if name == "response" {
                    current = ResponseParts::default();
                    current.in_response = true;
                }
                stack.push(name);
                text.clear();
            }
            Ok(Event::Text(value)) => push_text(&mut text, value.as_ref())?,
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                if current.in_response {
                    match (parent, name.as_str()) {
                        (Some("response"), "href") => current.href = trimmed(&text),
                        (Some("response"), "status") => current.status = trimmed(&text),
                        (Some("prop"), "getetag") => current.etag = normalize_etag(&text),
                        (Some("propstat"), "status") => current.propstat_status = trimmed(&text),
                        _ => {}
                    }
                } else if matches!(parent, Some("multistatus")) && name == "sync-token" {
                    report.sync_token = trimmed(&text);
                }
                if name == "response" {
                    current.in_response = false;
                    if let Some(entry) = current.as_sync_entry() {
                        report.entries.push(entry);
                    }
                }
                stack.pop();
                text.clear();
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("XML parse error: {error}")),
        }
    }

    Ok(report)
}

pub(crate) fn extract_href_property(
    xml: &str,
    property_name: &str,
) -> Result<Option<String>, String> {
    let mut reader = Reader::from_str(xml);
    let mut in_property = false;
    let mut current_tag = String::new();
    let mut text = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_name(element.name().as_ref());
                if name == property_name {
                    in_property = true;
                }
                current_tag = name;
                text.clear();
            }
            Ok(Event::Text(value)) => push_text(&mut text, value.as_ref())?,
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                if in_property
                    && current_tag == "href"
                    && let Some(href) = trimmed(&text)
                {
                    return Ok(Some(href));
                }
                if name == property_name {
                    in_property = false;
                }
                current_tag.clear();
                text.clear();
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("XML parse error: {error}")),
        }
    }

    Ok(None)
}

fn push_text(target: &mut String, raw: &[u8]) -> Result<(), String> {
    let raw =
        std::str::from_utf8(raw).map_err(|error| format!("XML text is not UTF-8: {error}"))?;
    let text = unescape(raw).map_err(|error| format!("XML text escape error: {error}"))?;
    target.push_str(&text);
    Ok(())
}

fn trimmed(text: &str) -> Option<String> {
    let value = text.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn normalize_etag(text: &str) -> Option<String> {
    trimmed(text).map(|value| value.trim_matches('"').to_string())
}

fn is_calendar_resource(href: &str, content_type: &Option<String>) -> bool {
    content_type
        .as_deref()
        .is_some_and(|ty| ty.to_ascii_lowercase().contains("text/calendar"))
        || href.to_ascii_lowercase().ends_with(".ics")
}

fn local_name(raw: &[u8]) -> String {
    let full = String::from_utf8_lossy(raw);
    match full.rfind(':') {
        Some(index) => full[index + 1..].to_string(),
        None => full.to_string(),
    }
}

#[derive(Default)]
struct ResponseParts {
    in_response: bool,
    in_propstat: bool,
    propstat_success: Option<bool>,
    is_calendar: bool,
    propstat_is_calendar: bool,
    privilege_seen: bool,
    propstat_privilege_seen: bool,
    write_seen: bool,
    propstat_write_seen: bool,
    is_collection: bool,
    href: Option<String>,
    etag: Option<String>,
    content_type: Option<String>,
    calendar_data: Option<String>,
    status: Option<String>,
    display_name: Option<String>,
    propstat_display_name: Option<String>,
    color: Option<String>,
    propstat_color: Option<String>,
    sync_token: Option<String>,
    propstat_sync_token: Option<String>,
    propstat_status: Option<String>,
}

impl ResponseParts {
    fn begin_propstat(&mut self) {
        self.in_propstat = true;
        self.propstat_success = None;
        self.propstat_is_calendar = false;
        self.propstat_privilege_seen = false;
        self.propstat_write_seen = false;
        self.propstat_display_name = None;
        self.propstat_color = None;
        self.propstat_sync_token = None;
        self.propstat_status = None;
    }

    fn mark_calendar(&mut self) {
        if self.in_propstat {
            self.propstat_is_calendar = true;
        } else {
            self.is_calendar = true;
        }
    }

    fn mark_privilege_seen(&mut self) {
        if self.in_propstat {
            self.propstat_privilege_seen = true;
        } else {
            self.privilege_seen = true;
        }
    }

    fn mark_write_seen(&mut self) {
        if self.in_propstat {
            self.propstat_write_seen = true;
        } else {
            self.write_seen = true;
        }
    }

    fn commit_propstat(&mut self) {
        if self.propstat_success.unwrap_or(true) {
            self.is_calendar |= self.propstat_is_calendar;
            self.privilege_seen |= self.propstat_privilege_seen;
            self.write_seen |= self.propstat_write_seen;
            if self.propstat_display_name.is_some() {
                self.display_name = self.propstat_display_name.take();
            }
            if self.propstat_color.is_some() {
                self.color = self.propstat_color.take();
            }
            if self.propstat_sync_token.is_some() {
                self.sync_token = self.propstat_sync_token.take();
            }
        }
        self.in_propstat = false;
        self.propstat_success = None;
        self.propstat_is_calendar = false;
        self.propstat_privilege_seen = false;
        self.propstat_write_seen = false;
        self.propstat_display_name = None;
        self.propstat_color = None;
        self.propstat_sync_token = None;
        self.propstat_status = None;
    }

    fn as_calendar_collection(&self) -> Option<CalendarCollection> {
        if !self.is_calendar {
            return None;
        }
        Some(CalendarCollection {
            href: self.href.as_ref()?.clone(),
            display_name: self.display_name.clone(),
            color: self.color.clone(),
            can_edit: self.privilege_seen.then_some(self.write_seen),
            sync_token: self.sync_token.clone(),
        })
    }

    fn as_event_entry(&self) -> Option<CalDavEventEntry> {
        let href = self.href.as_ref()?;
        if self.is_collection {
            return None;
        }
        if !is_calendar_resource(href, &self.content_type) {
            return None;
        }
        Some(CalDavEventEntry {
            uri: href.clone(),
            etag: self.etag.clone(),
        })
    }

    fn as_fetched_event(&self) -> Option<CalDavFetchedEvent> {
        Some(CalDavFetchedEvent {
            uri: self.href.as_ref()?.clone(),
            etag: self.etag.clone(),
            data: self.calendar_data.as_ref()?.clone(),
        })
    }

    fn error_status(&self) -> Option<&str> {
        let status = self.status.as_deref()?;
        let code = status_code(status)?;
        (!matches!(code, 200..=299)).then_some(status)
    }

    fn as_sync_entry(&self) -> Option<CalDavSyncEntry> {
        let href = self.href.as_ref()?;
        if !href.to_ascii_lowercase().ends_with(".ics") {
            return None;
        }
        Some(CalDavSyncEntry {
            uri: href.clone(),
            etag: self.etag.clone(),
            status: self
                .status
                .as_deref()
                .and_then(status_code)
                .or_else(|| self.propstat_status.as_deref().and_then(status_code)),
        })
    }
}

fn status_code(status: &str) -> Option<u16> {
    status
        .split_whitespace()
        .find_map(|part| part.parse::<u16>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_collections_read_display_metadata_and_privileges() {
        let xml = r##"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:A="http://apple.com/ns/ical/">
  <D:response>
    <D:href>/cal/personal/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:calendar/></D:resourcetype>
        <D:displayname>Personal</D:displayname>
        <A:calendar-color>#112233</A:calendar-color>
        <D:current-user-privilege-set>
          <D:privilege><D:read/></D:privilege>
          <D:privilege><D:write-content/></D:privilege>
        </D:current-user-privilege-set>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"##;

        let calendars = parse_calendar_collections(xml).expect("valid XML");
        assert_eq!(
            calendars,
            vec![CalendarCollection {
                href: "/cal/personal/".to_string(),
                display_name: Some("Personal".to_string()),
                color: Some("#112233".to_string()),
                can_edit: Some(true),
                sync_token: None,
            }]
        );
    }

    #[test]
    fn calendar_collections_read_sync_token() {
        let xml = r##"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/personal/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:calendar/></D:resourcetype>
        <D:sync-token>token-1</D:sync-token>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"##;

        let calendars = parse_calendar_collections(xml).expect("valid XML");

        assert_eq!(calendars[0].sync_token.as_deref(), Some("token-1"));
    }

    #[test]
    fn calendar_collections_ignore_failed_propstat_values() {
        let xml = r##"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:A="http://apple.com/ns/ical/">
  <D:response>
    <D:href>/cal/personal/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:calendar/></D:resourcetype>
        <D:displayname>Personal</D:displayname>
        <A:calendar-color>#112233</A:calendar-color>
      </D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"##;

        let calendars = parse_calendar_collections(xml).expect("valid XML");
        assert!(calendars.is_empty());
    }

    #[test]
    fn propfind_events_extracts_ics_resources() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat><D:prop>
      <D:getetag>"abc"</D:getetag>
      <D:getcontenttype>text/calendar</D:getcontenttype>
    </D:prop></D:propstat>
  </D:response>
</D:multistatus>"#;

        let entries = parse_propfind_events(xml).expect("valid XML");
        assert_eq!(
            entries,
            vec![CalDavEventEntry {
                uri: "/cal/one.ics".to_string(),
                etag: Some("abc".to_string()),
            }]
        );
    }

    #[test]
    fn propfind_events_ignores_nested_href_properties() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat><D:prop>
      <C:calendar-user-address-set>
        <D:href>mailto:ada@example.test</D:href>
      </C:calendar-user-address-set>
      <D:getetag>"abc"</D:getetag>
      <D:getcontenttype>text/calendar</D:getcontenttype>
    </D:prop></D:propstat>
  </D:response>
</D:multistatus>"#;

        let entries = parse_propfind_events(xml).expect("valid XML");
        assert_eq!(entries[0].uri, "/cal/one.ics");
    }

    #[test]
    fn propfind_events_ignores_collections_even_when_calendar_typed() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/cal/archive.ics/</D:href>
    <D:propstat><D:prop>
      <D:resourcetype><D:collection/></D:resourcetype>
      <D:getetag>"abc"</D:getetag>
      <D:getcontenttype>text/calendar</D:getcontenttype>
    </D:prop></D:propstat>
  </D:response>
</D:multistatus>"#;

        let entries = parse_propfind_events(xml).expect("valid XML");
        assert!(entries.is_empty());
    }

    #[test]
    fn multiget_report_rejects_embedded_failures() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/missing.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data/></D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let error = parse_multiget_report(xml).expect_err("embedded 404 should fail");
        assert!(error.contains("404"));
        assert!(error.contains("/cal/missing.ics"));
    }

    #[test]
    fn multiget_report_accepts_embedded_success() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"abc"</D:getetag>
        <C:calendar-data>BEGIN:VCALENDAR
END:VCALENDAR</C:calendar-data>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let events = parse_multiget_report(xml).expect("embedded 200 should pass");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].etag.as_deref(), Some("abc"));
    }

    #[test]
    fn sync_collection_report_reads_token_and_changed_entries() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:sync-token>token-2</D:sync-token>
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat>
      <D:prop><D:getetag>"new"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/cal/two.ics</D:href>
    <D:status>HTTP/1.1 404 Not Found</D:status>
  </D:response>
</D:multistatus>"#;

        let report = parse_sync_collection_report(xml).expect("valid XML");

        assert_eq!(report.sync_token.as_deref(), Some("token-2"));
        assert_eq!(
            report.entries,
            vec![
                CalDavSyncEntry {
                    uri: "/cal/one.ics".to_string(),
                    etag: Some("new".to_string()),
                    status: Some(200),
                },
                CalDavSyncEntry {
                    uri: "/cal/two.ics".to_string(),
                    etag: None,
                    status: Some(404),
                },
            ]
        );
    }
}
