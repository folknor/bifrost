use bifrost_net::{status_line_code, status_line_is_success};
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::Event;
use reqwest::Url;

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

/// Outcome of a depth-1 event PROPFIND: the resources whose propstat
/// succeeded (`entries`) plus the hrefs the server reported *failed*
/// within the 207. A failed href is a transiently-failed resource, not
/// an absent one - the snapshot diff preserves the local copy rather
/// than emitting a Destroyed (brick 7).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CalDavEventListing {
    pub(crate) entries: Vec<CalDavEventEntry>,
    pub(crate) failed_hrefs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalDavFetchedEvent {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
    pub(crate) data: String,
}

/// One resource inside a 207 that yielded no usable `calendar-data`,
/// with the status the server gave it (when it gave one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalDavFailedResource {
    pub(crate) href: String,
    pub(crate) status: Option<u16>,
}

impl CalDavFailedResource {
    /// True when the status means "this particular resource is not
    /// there" - the benign, genuinely per-resource case that
    /// `Page::failed_ids` exists to carry. Anything else (auth,
    /// permission, server error, or no status at all) can just as
    /// easily be a condition affecting the whole request.
    pub(crate) fn is_missing_resource(&self) -> bool {
        matches!(self.status, Some(404 | 410))
    }
}

/// Outcome of a `calendar-multiget` / `calendar-query` REPORT: the
/// resources that came back with usable `calendar-data` (`events`) plus
/// the ones that did not (`failed`).
///
/// A 207 Multi-Status is per-resource by construction, so a single bad
/// propstat inside it is a per-resource failure and nothing more.
/// Failing the whole parse on one of them turned a one-event problem
/// into a dead pull. But the converse is equally wrong: RFC 4918 s13
/// says a 207 body may describe success, partial success, OR complete
/// failure, so a body in which every resource failed must not be
/// handed back as an empty success. See `classify`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CalDavMultigetReport {
    pub(crate) events: Vec<CalDavFetchedEvent>,
    pub(crate) failed: Vec<CalDavFailedResource>,
    /// Resources that returned a successful response but omitted the
    /// requested `calendar-data`. They are per-resource absences, not DAV
    /// failures, so they surface through `Page::failed_ids` but cannot turn
    /// a 207 into a synthetic server error.
    pub(crate) missing_data: Vec<String>,
}

/// What a parsed 207 body actually represents, per RFC 4918 s13.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MultigetOutcome {
    /// Nothing failed, or enough succeeded that the failures are
    /// per-resource news the caller should report but not fail on.
    Usable,
    /// Every resource in the body failed and at least one of them
    /// failed for a reason that is not "this resource is missing".
    /// Reporting this as an empty success lets a consumer record the
    /// collection as fully walked and drop the resources permanently.
    CompleteFailure { status: Option<u16> },
}

impl CalDavMultigetReport {
    /// Fold another chunk's report into this one. Multiget is chunked
    /// and text search runs one REPORT per property, so both lanes have
    /// to accumulate across calls.
    pub(crate) fn extend(&mut self, other: CalDavMultigetReport) {
        self.events.extend(other.events);
        self.failed.extend(other.failed);
        self.missing_data.extend(other.missing_data);
    }

    pub(crate) fn failed_hrefs(&self) -> Vec<String> {
        self.failed
            .iter()
            .map(|failure| failure.href.clone())
            .chain(self.missing_data.iter().cloned())
            .collect()
    }

    /// Classify the body rather than merely parsing it.
    ///
    /// An empty body is `Usable`: a query that matched nothing is a
    /// legitimate empty result, not a failure. A body with at least one
    /// usable resource is `Usable` too - the failures are real, and the
    /// caller still reports them, but the request as a whole worked.
    ///
    /// A body where EVERY resource failed is only benign if every
    /// failure was a missing resource (404/410), which is what happens
    /// when hrefs are deleted between the listing and the multiget.
    /// If any of them was an auth, permission, or server condition,
    /// the body is a complete failure and the caller must surface it as
    /// an error instead of an empty page.
    pub(crate) fn classify(&self) -> MultigetOutcome {
        if !self.events.is_empty() || self.failed.is_empty() {
            return MultigetOutcome::Usable;
        }
        let systemic = self
            .failed
            .iter()
            .find(|failure| !failure.is_missing_resource());
        match systemic {
            Some(failure) => MultigetOutcome::CompleteFailure {
                status: failure.status,
            },
            None => MultigetOutcome::Usable,
        }
    }
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

impl CalendarCollection {
    pub(crate) fn resolve_href(&mut self, base_url: &str) {
        self.href = resolve_href(base_url, &self.href);
    }
}

impl CalDavEventListing {
    pub(crate) fn resolve_hrefs(&mut self, base_url: &str) {
        for entry in &mut self.entries {
            entry.uri = resolve_href(base_url, &entry.uri);
        }
        for href in &mut self.failed_hrefs {
            *href = resolve_href(base_url, href);
        }
    }
}

impl CalDavMultigetReport {
    pub(crate) fn resolve_hrefs(&mut self, base_url: &str) {
        for event in &mut self.events {
            event.uri = resolve_href(base_url, &event.uri);
        }
        for failed in &mut self.failed {
            failed.href = resolve_href(base_url, &failed.href);
        }
        for href in &mut self.missing_data {
            *href = resolve_href(base_url, href);
        }
    }
}

impl CalDavSyncReport {
    pub(crate) fn resolve_hrefs(&mut self, base_url: &str) {
        for entry in &mut self.entries {
            entry.uri = resolve_href(base_url, &entry.uri);
        }
    }
}

/// Rebase a DAV response href at the XML decoding boundary. Client callers
/// never expose parsed relative hrefs to the account layer.
pub(crate) fn resolve_href(base_url: &str, href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        return href.to_string();
    }
    if let Ok(base) = Url::parse(base_url)
        && let Ok(resolved) = base.join(href)
    {
        return resolved.to_string();
    }
    if base_url.ends_with('/') || href.starts_with('/') {
        format!("{base_url}{href}")
    } else {
        format!("{base_url}/{href}")
    }
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
                if current.in_response
                    && name == "calendar"
                    && stack.iter().any(|item| item == "resourcetype")
                {
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
                if current.in_response
                    && name == "calendar"
                    && stack.iter().any(|item| item == "resourcetype")
                {
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
                            current.propstat_success = Some(status_line_is_success(&text));
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

pub(crate) fn parse_propfind_events(xml: &str) -> Result<CalDavEventListing, String> {
    let mut reader = Reader::from_str(xml);
    let mut listing = CalDavEventListing::default();
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
                if current.in_response && name == "propstat" {
                    current.begin_propstat();
                }
                if current.in_response
                    && name == "collection"
                    && stack.iter().any(|item| item == "resourcetype")
                {
                    current.is_collection = true;
                }
                stack.push(name);
                text.clear();
            }
            Ok(Event::Empty(element)) => {
                let name = local_name(element.name().as_ref());
                if current.in_response
                    && name == "collection"
                    && stack.iter().any(|item| item == "resourcetype")
                {
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
                        (Some("prop"), "getcontenttype") => current.content_type = trimmed(&text),
                        (Some("propstat"), "status") => {
                            current.propstat_success = Some(status_line_is_success(&text));
                        }
                        _ => {}
                    }
                }
                if name == "propstat" {
                    current.commit_propstat();
                }
                if name == "response" {
                    current.in_response = false;
                    if let Some(entry) = current.as_event_entry() {
                        listing.entries.push(entry);
                    } else if let Some(href) = current.as_failed_event_href() {
                        listing.failed_hrefs.push(href);
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

    Ok(listing)
}

/// Parse a `calendar-multiget` / `calendar-query` 207 response.
///
/// `Err` is reserved for a malformed document - the whole payload is
/// unusable and there is nothing per-resource to salvage. A well-formed
/// 207 whose individual responses carry non-2xx propstats (or 2xx with
/// no `calendar-data`) is NOT an error: those hrefs land in
/// `failed_hrefs` and every sibling that did come back is returned, so
/// the caller can report the casualties through `Page::failed_ids`
/// while the rest of the pull succeeds.
pub(crate) fn parse_multiget_report(xml: &str) -> Result<CalDavMultigetReport, String> {
    let mut reader = Reader::from_str(xml);
    let mut report = CalDavMultigetReport::default();
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
                if current.in_response && name == "propstat" {
                    current.begin_propstat();
                }
                if current.in_response
                    && name == "collection"
                    && stack.iter().any(|item| item == "resourcetype")
                {
                    current.is_collection = true;
                }
                stack.push(name);
                text.clear();
            }
            Ok(Event::Empty(element)) => {
                let name = local_name(element.name().as_ref());
                if current.in_response
                    && name == "collection"
                    && stack.iter().any(|item| item == "resourcetype")
                {
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
                    // Properties are collected PROPSTAT-scoped and only
                    // promoted by `commit_propstat` if that propstat's
                    // own status was 2xx. Writing the response-level
                    // fields here instead made the result depend on
                    // propstat order: data from a failed block could be
                    // adopted, and a trailing non-2xx block could
                    // discard data a successful one supplied.
                    match (parent, name.as_str()) {
                        (Some("response"), "href") => current.href = trimmed(&text),
                        (Some("prop"), "getetag") => {
                            current.propstat_etag = normalize_etag(&text);
                        }
                        (Some("prop"), "calendar-data") => {
                            current.propstat_calendar_data = trimmed(&text);
                        }
                        (Some("propstat"), "status") => {
                            current.propstat_status = trimmed(&text);
                            current.propstat_success = Some(status_line_is_success(&text));
                        }
                        (Some("response"), "status") => current.status = trimmed(&text),
                        _ => {}
                    }
                }
                if name == "propstat" {
                    current.commit_propstat();
                }
                if name == "response" {
                    current.in_response = false;
                    if let Some(event) = current.as_fetched_event() {
                        report.events.push(event);
                    } else if let Some(failed) = current.as_failed_multiget_resource() {
                        // Nothing usable came back for this one
                        // resource. That says nothing about its
                        // siblings, so the rest of the page survives;
                        // the carried status lets the caller tell a
                        // vanished resource apart from a refusal.
                        report.failed.push(failed);
                    } else if let Some(href) = current.as_missing_multiget_data() {
                        report.missing_data.push(href);
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

/// Extract a collection sync token from a depth-0 PROPFIND. The token is
/// committed only from a successful propstat so a stale value in a refused
/// property cannot revive an invalid cursor.
pub(crate) fn parse_collection_sync_token(xml: &str) -> Result<Option<String>, String> {
    let mut reader = Reader::from_str(xml);
    let mut stack: Vec<String> = Vec::new();
    let mut text = String::new();
    let mut propstat_token = None;
    let mut propstat_success = None;
    let mut committed = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_name(element.name().as_ref());
                if name == "propstat" {
                    propstat_token = None;
                    propstat_success = None;
                }
                stack.push(name);
                text.clear();
            }
            Ok(Event::Text(value)) => push_text(&mut text, value.as_ref())?,
            Ok(Event::CData(value)) => {
                let value = value.decode().map_err(|error| error.to_string())?;
                text.push_str(&value);
            }
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                match (parent, name.as_str()) {
                    (Some("prop"), "sync-token") => propstat_token = trimmed(&text),
                    (Some("propstat"), "status") => {
                        propstat_success = Some(status_line_is_success(&text));
                    }
                    _ => {}
                }
                if name == "propstat"
                    && propstat_success.unwrap_or(true)
                    && propstat_token.is_some()
                {
                    committed = propstat_token.take();
                }
                stack.pop();
                text.clear();
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("XML parse error: {error}")),
        }
    }

    Ok(committed)
}

pub(crate) fn extract_href_property(
    xml: &str,
    property_name: &str,
) -> Result<Option<String>, String> {
    Ok(extract_href_properties(xml, property_name)?
        .into_iter()
        .next())
}

pub(crate) fn extract_href_properties(
    xml: &str,
    property_name: &str,
) -> Result<Vec<String>, String> {
    let mut reader = Reader::from_str(xml);
    let mut stack = Vec::new();
    let mut text = String::new();
    let mut hrefs = Vec::new();
    let mut propstat_hrefs = Vec::new();
    let mut propstat_success = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_name(element.name().as_ref());
                stack.push(name);
                text.clear();
            }
            Ok(Event::Text(value)) => push_text(&mut text, value.as_ref())?,
            Ok(Event::CData(value)) => {
                let value = value.decode().map_err(|error| error.to_string())?;
                text.push_str(&value);
            }
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                if name == "href"
                    && stack.iter().any(|tag| tag == property_name)
                    && let Some(href) = trimmed(&text)
                {
                    if stack.iter().any(|tag| tag == "propstat") {
                        propstat_hrefs.push(href);
                    } else {
                        hrefs.push(href);
                    }
                }
                if name == "status" && stack.iter().any(|tag| tag == "propstat") {
                    propstat_success = Some(
                        trimmed(&text)
                            .as_deref()
                            .is_some_and(status_line_is_success),
                    );
                }
                if name == "propstat" {
                    if propstat_success.unwrap_or(true) {
                        hrefs.append(&mut propstat_hrefs);
                    } else {
                        propstat_hrefs.clear();
                    }
                    propstat_success = None;
                }
                stack.pop();
                text.clear();
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("XML parse error: {error}")),
        }
    }

    Ok(hrefs)
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
    trimmed(text).map(|value| {
        value
            .get(..2)
            .filter(|prefix| prefix.eq_ignore_ascii_case("W/"))
            .map_or_else(
                || value.trim_matches('"').to_string(),
                |_| format!("W/{}", value[2..].trim()),
            )
    })
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
    has_success_propstat: bool,
    saw_failed_propstat: bool,
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
    /// Propstat-scoped `calendar-data` / `getetag`, promoted to the
    /// response level by `commit_propstat` only when that propstat's
    /// own status was 2xx. Multiget uses these instead of writing the
    /// response-level fields directly, so a value can never be adopted
    /// from a propstat the server refused, and a later unrelated
    /// non-2xx propstat can never retract a value an earlier 2xx one
    /// legitimately supplied.
    propstat_calendar_data: Option<String>,
    propstat_etag: Option<String>,
    /// Status codes of every non-2xx propstat in this response, in
    /// document order. Drives failure classification.
    failed_statuses: Vec<u16>,
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
        self.propstat_calendar_data = None;
        self.propstat_etag = None;
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
        if self.propstat_success == Some(false) {
            self.saw_failed_propstat = true;
            if let Some(code) = self.propstat_status.as_deref().and_then(status_line_code) {
                self.failed_statuses.push(code);
            }
        }
        if self.propstat_success.unwrap_or(true) {
            self.has_success_propstat = true;
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
            if self.propstat_calendar_data.is_some() {
                self.calendar_data = self.propstat_calendar_data.take();
            }
            if self.propstat_etag.is_some() {
                self.etag = self.propstat_etag.take();
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
        self.propstat_calendar_data = None;
        self.propstat_etag = None;
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
        // A resource whose only propstat failed is not a committed
        // entry; it is surfaced via `as_failed_event_href` instead.
        if self.saw_failed_propstat && !self.has_success_propstat {
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

    /// The href of an event resource the server reported *failed* within
    /// the 207 (a non-2xx propstat, no success propstat). Only `.ics`
    /// resources surface - a failed collection is not a
    /// transiently-failed resource.
    fn as_failed_event_href(&self) -> Option<String> {
        if self.is_collection || self.has_success_propstat || !self.saw_failed_propstat {
            return None;
        }
        let href = self.href.as_ref()?;
        if !href.to_ascii_lowercase().ends_with(".ics") {
            return None;
        }
        Some(href.clone())
    }

    fn as_fetched_event(&self) -> Option<CalDavFetchedEvent> {
        Some(CalDavFetchedEvent {
            uri: self.href.as_ref()?.clone(),
            etag: self.etag.clone(),
            data: self.calendar_data.as_ref()?.clone(),
        })
    }

    /// A multiget response with an actual non-2xx status, using the first
    /// refused propstat status or a non-2xx response-level status. A
    /// collection response (some servers echo the collection itself alongside
    /// the requested resources) is not a failed resource and is dropped.
    fn as_failed_multiget_resource(&self) -> Option<CalDavFailedResource> {
        if self.is_collection {
            return None;
        }
        let href = self.href.clone()?;
        let status = self.failed_statuses.first().copied().or_else(|| {
            self.status
                .as_deref()
                .and_then(status_line_code)
                .filter(|status| !(200..=299).contains(status))
        })?;
        Some(CalDavFailedResource {
            href,
            status: Some(status),
        })
    }

    fn as_missing_multiget_data(&self) -> Option<String> {
        if self.is_collection || self.calendar_data.is_some() {
            return None;
        }
        self.href.clone()
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
                .and_then(status_line_code)
                .or_else(|| self.propstat_status.as_deref().and_then(status_line_code)),
        })
    }
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
    fn calendar_home_without_calendar_children_lists_empty() {
        // Depth-1 PROPFIND on a calendar-home that is a plain collection with
        // no calendar children: the home's own response carries no <calendar/>
        // and there are no other collections. `calendars_list` maps this
        // parse output 1:1, so an empty result here is what surfaces to the
        // consumer as an empty calendar list (no fabricated placeholder).
        let xml = r##"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/home/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/></D:resourcetype>
        <D:displayname>Home</D:displayname>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"##;

        let calendars = parse_calendar_collections(xml).expect("valid XML");
        assert!(calendars.is_empty());
    }

    #[test]
    fn calendar_home_that_is_itself_a_calendar_lists_one() {
        // A CalDAV server whose calendar-home is itself a calendar collection
        // (resourcetype includes <calendar/>) is returned by the same depth-1
        // parse. This is the genuine case the removed placeholder-fallback was
        // conflated with: it needs no fabrication because the home's own
        // response already maps to one calendar keyed on the home href.
        let xml = r##"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/home/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:calendar/></D:resourcetype>
        <D:displayname>Home</D:displayname>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"##;

        let calendars = parse_calendar_collections(xml).expect("valid XML");
        assert_eq!(
            calendars,
            vec![CalendarCollection {
                href: "/cal/home/".to_string(),
                display_name: Some("Home".to_string()),
                color: None,
                can_edit: None,
                sync_token: None,
            }]
        );
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

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert_eq!(
            listing.entries,
            vec![CalDavEventEntry {
                uri: "/cal/one.ics".to_string(),
                etag: Some("abc".to_string()),
            }]
        );
        assert!(listing.failed_hrefs.is_empty());
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

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert_eq!(listing.entries[0].uri, "/cal/one.ics");
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

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert!(listing.entries.is_empty());
    }

    #[test]
    fn propfind_events_surfaces_failed_propstat_as_failed_href() {
        // Brick 7: an event resource whose only propstat is a 404 is not
        // committed as an entry, but IS surfaced as a failed href so the
        // snapshot diff preserves the local copy.
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"missing"</D:getetag>
        <D:getcontenttype>text/calendar</D:getcontenttype>
      </D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert!(listing.entries.is_empty());
        assert_eq!(listing.failed_hrefs, vec!["/cal/one.ics".to_string()]);
    }

    #[test]
    fn multiget_report_reports_embedded_failures_per_resource() {
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

        let report = parse_multiget_report(xml).expect("a 207 is not a parse failure");
        assert!(report.events.is_empty());
        assert_eq!(report.failed_hrefs(), vec!["/cal/missing.ics".to_string()]);
        assert_eq!(report.failed[0].status, Some(404));
        // Every resource failed, but the only failure was a vanished
        // resource, so the body is still usable.
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn one_bad_propstat_does_not_abort_the_rest_of_the_multiget() {
        // The whole point of a 207: per-resource outcomes. A refused
        // resource must not cost the caller its siblings.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/missing.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data/></D:prop>
      <D:status>HTTP/1.1 403 Forbidden</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/cal/good.ics</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"g1"</D:getetag>
        <C:calendar-data>BEGIN:VCALENDAR
END:VCALENDAR</C:calendar-data>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("a 207 is not a parse failure");
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.events[0].uri, "/cal/good.ics");
        assert_eq!(report.failed_hrefs(), vec!["/cal/missing.ics".to_string()]);
        // Partial success: the 403 is reported, not fatal.
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn multiget_report_treats_a_2xx_without_calendar_data_as_failed() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/empty.ics</D:href>
    <D:propstat>
      <D:prop><D:getetag>"e1"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("a 207 is not a parse failure");
        assert!(report.events.is_empty());
        assert_eq!(report.failed_hrefs(), vec!["/cal/empty.ics".to_string()]);
        assert!(report.failed.is_empty());
        assert_eq!(report.missing_data, vec!["/cal/empty.ics"]);
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn multiget_report_ignores_an_echoed_collection_response() {
        // Some servers echo the collection alongside the requested
        // resources. That is not a failed event resource.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("a 207 is not a parse failure");
        assert!(report.events.is_empty());
        assert!(report.failed.is_empty());
        assert_eq!(report.classify(), MultigetOutcome::Usable);
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

        let report = parse_multiget_report(xml).expect("embedded 200 should pass");
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.events[0].etag.as_deref(), Some("abc"));
        assert!(report.failed.is_empty());
    }

    /// A response split across two propstat blocks - the RFC 4918
    /// shape where found and not-found properties are reported
    /// separately - in both orders. Equivalent input must give an
    /// equivalent answer.
    fn two_propstat_multiget(data_first: bool) -> String {
        let ok_block = "    <D:propstat>\n\
      <D:prop>\n\
        <D:getetag>\"ok\"</D:getetag>\n\
        <C:calendar-data>BEGIN:VCALENDAR\nEND:VCALENDAR</C:calendar-data>\n\
      </D:prop>\n\
      <D:status>HTTP/1.1 200 OK</D:status>\n\
    </D:propstat>\n";
        let missing_block = "    <D:propstat>\n\
      <D:prop><D:displayname/></D:prop>\n\
      <D:status>HTTP/1.1 404 Not Found</D:status>\n\
    </D:propstat>\n";
        let blocks = if data_first {
            format!("{ok_block}{missing_block}")
        } else {
            format!("{missing_block}{ok_block}")
        };
        format!(
            "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:response>\n\
    <D:href>/cal/split.ics</D:href>\n\
{blocks}  </D:response>\n\
</D:multistatus>"
        )
    }

    #[test]
    fn multiget_is_not_order_dependent_across_propstats() {
        // A trailing 404 propstat for an unrelated property must not
        // retract calendar-data a 200 propstat legitimately supplied,
        // and the answer must not depend on which block came first.
        for data_first in [true, false] {
            let xml = two_propstat_multiget(data_first);
            let report = parse_multiget_report(&xml).expect("valid 207");
            assert_eq!(
                report.events.len(),
                1,
                "data_first={data_first}: usable calendar-data was dropped"
            );
            assert_eq!(report.events[0].uri, "/cal/split.ics");
            assert_eq!(report.events[0].etag.as_deref(), Some("ok"));
            assert!(report.failed.is_empty());
        }
    }

    #[test]
    fn calendar_data_from_a_failed_propstat_is_never_adopted() {
        // The mirror image: data sitting inside a non-2xx propstat is
        // not usable, even when a later propstat succeeds.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/refused.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data>BEGIN:VCALENDAR
END:VCALENDAR</C:calendar-data></D:prop>
      <D:status>HTTP/1.1 403 Forbidden</D:status>
    </D:propstat>
    <D:propstat>
      <D:prop><D:getetag>"x"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(
            report.events.is_empty(),
            "calendar-data from a 403 propstat must not be served as an event"
        );
        assert_eq!(report.failed_hrefs(), vec!["/cal/refused.ics".to_string()]);
        assert_eq!(report.failed[0].status, Some(403));
    }

    #[test]
    fn a_wholly_refused_body_is_a_complete_failure_not_an_empty_success() {
        // RFC 4918 s13: a 207 can describe complete failure. Handing
        // this back as an empty page lets a consumer mark the
        // collection walked and drop both resources for good.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data/></D:prop>
      <D:status>HTTP/1.1 401 Unauthorized</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/cal/two.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data/></D:prop>
      <D:status>HTTP/1.1 401 Unauthorized</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert_eq!(
            report.classify(),
            MultigetOutcome::CompleteFailure { status: Some(401) }
        );
    }

    /// A `<D:status>` that is PRESENT but unreadable must not commit the
    /// properties beside it.
    ///
    /// `propstat_success.unwrap_or(true)` treats `None` as success, which
    /// is right for an ABSENT status (RFC 4918 s14.22 requires one, and a
    /// server omitting it is describing a success). It is wrong for a
    /// status that is there and cannot be parsed: that is not evidence
    /// the property was returned, so committing it accepts a value the
    /// server may have refused.
    ///
    /// This crate previously mapped unparseable to `None` (success) while
    /// bifrost-carddav mapped it to failure - a divergence that survived
    /// precisely because each crate spelled the check itself. Both now
    /// fail closed through `bifrost_net::status_line_is_success`.
    #[test]
    fn a_present_but_unparseable_propstat_status_does_not_commit() {
        // Real calendar-data, so the old behaviour genuinely committed an
        // event here - the assertion would pass vacuously against an
        // empty prop.
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
      <D:status>HTTP/1.1 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(
            report.events.is_empty(),
            "an unreadable status must not commit the property beside it"
        );
    }

    #[test]
    fn multiget_response_level_status_reports_failed_resource() {
        // Some servers report a vanished resource with a response-level
        // status and no propstat at all (the sync-collection shape). The
        // status still classifies the failure as a benign missing
        // resource.
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/cal/gone.ics</D:href>
    <D:status>HTTP/1.1 404 Not Found</D:status>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.events.is_empty());
        assert_eq!(report.failed_hrefs(), vec!["/cal/gone.ics".to_string()]);
        assert_eq!(report.failed[0].status, Some(404));
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    /// `status_line_code` reads the status POSITION, not the first
    /// in-range number anywhere in the line. That moves inputs between
    /// buckets here, so both directions are pinned: the protocol-less
    /// form servers really emit inside `<D:status>` still classifies,
    /// and free text a proxy dropped in no longer fabricates a code the
    /// server never sent. The failure lane keeps its `href` either way -
    /// this is about the reported status, not about losing the resource.
    #[test]
    fn a_prose_response_status_does_not_fabricate_a_resource_status_code() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/cal/terse.ics</D:href>
    <D:status>404 Not Found</D:status>
  </D:response>
  <D:response>
    <D:href>/cal/prose.ics</D:href>
    <D:propstat>
      <D:prop><D:getetag>"p"</D:getetag></D:prop>
      <D:status>Resource error 404 reported by upstream</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.events.is_empty());
        let terse = report
            .failed
            .iter()
            .find(|resource| resource.href == "/cal/terse.ics")
            .expect("a protocol-less status line still classifies");
        assert_eq!(terse.status, Some(404));
        // The refusal is still honoured - no property is committed - but
        // with no readable code the resource carries no status to report,
        // so it surfaces in the absent-data lane rather than being
        // labelled with a code the server never sent.
        assert!(
            !report
                .failed
                .iter()
                .any(|resource| resource.href == "/cal/prose.ics"),
            "prose must not be mined for a status the server never sent"
        );
        assert_eq!(report.missing_data, vec!["/cal/prose.ics"]);
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn absent_calendar_data_does_not_make_a_missing_resource_systemic() {
        // A 2xx propstat with no calendar-data is an absent-data outcome,
        // not a server failure. Combined with a benign 404 the body remains
        // usable and both hrefs stay in the per-resource failed-id lane.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/gone.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data/></D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/cal/empty.ics</D:href>
    <D:propstat>
      <D:prop><D:getetag>"e"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.events.is_empty());
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.missing_data, vec!["/cal/empty.ics"]);
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn an_empty_body_is_usable_not_a_failure() {
        // A query that matched nothing is a legitimate empty result.
        let xml = r#"<D:multistatus xmlns:D="DAV:"></D:multistatus>"#;
        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.events.is_empty());
        assert!(report.failed.is_empty());
        assert_eq!(report.classify(), MultigetOutcome::Usable);
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

    #[test]
    fn cdata_is_read_by_listing_sync_and_href_parsers() {
        let listing = parse_propfind_events(
            r#"<D:multistatus xmlns:D="DAV:"><D:response><D:href><![CDATA[/cal/one.ics]]></D:href><D:propstat><D:prop><D:getetag><![CDATA["one"]]></D:getetag><D:getcontenttype>text/calendar</D:getcontenttype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid listing");
        assert_eq!(listing.entries[0].uri, "/cal/one.ics");
        assert_eq!(listing.entries[0].etag.as_deref(), Some("one"));

        let sync = parse_sync_collection_report(
            r#"<D:multistatus xmlns:D="DAV:"><D:sync-token><![CDATA[token-2]]></D:sync-token><D:response><D:href><![CDATA[/cal/one.ics]]></D:href><D:propstat><D:prop><D:getetag><![CDATA["two"]]></D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid sync report");
        assert_eq!(sync.sync_token.as_deref(), Some("token-2"));
        assert_eq!(sync.entries[0].etag.as_deref(), Some("two"));

        let href = extract_href_property(
            r#"<D:current-user-principal xmlns:D="DAV:"><D:href><![CDATA[/principals/ada/]]></D:href></D:current-user-principal>"#,
            "current-user-principal",
        )
        .expect("valid property");
        assert_eq!(href.as_deref(), Some("/principals/ada/"));
    }

    #[test]
    fn address_set_extractor_returns_every_href() {
        let hrefs = extract_href_properties(
            r#"<C:calendar-user-address-set xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:href>/principals/ada/</D:href><D:href>Mailto:Ada@example.test</D:href></C:calendar-user-address-set>"#,
            "calendar-user-address-set",
        )
        .expect("valid property");

        assert_eq!(
            hrefs,
            vec![
                "/principals/ada/".to_string(),
                "Mailto:Ada@example.test".to_string()
            ]
        );
    }

    #[test]
    fn href_extractors_ignore_failed_propstats() {
        let hrefs = extract_href_properties(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:response><D:propstat><D:prop><C:calendar-user-address-set><D:href>mailto:wrong@example.test</D:href></C:calendar-user-address-set></D:prop><D:status>HTTP/1.1 404 Not Found</D:status></D:propstat><D:propstat><D:prop><C:calendar-user-address-set><D:href>mailto:right@example.test</D:href></C:calendar-user-address-set></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
            "calendar-user-address-set",
        )
        .expect("valid XML");

        assert_eq!(hrefs, vec!["mailto:right@example.test".to_string()]);
    }

    #[test]
    fn weak_etag_keeps_weakness_marker() {
        assert_eq!(normalize_etag("W/\"abc\"").as_deref(), Some("W/\"abc\""));
        assert_eq!(normalize_etag("\"abc\"").as_deref(), Some("abc"));
    }

    #[test]
    fn calendar_element_outside_resourcetype_does_not_mark_collection() {
        let collections = parse_calendar_collections(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:response><D:href>/not-a-calendar/</D:href><D:propstat><D:prop><D:owner><C:calendar/></D:owner></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid multistatus");

        assert!(collections.is_empty());
    }
}
