pub(crate) use bifrost_dav_core::resolve_href;
use bifrost_dav_core::{local_name, normalize_etag, push_text, trimmed};
use bifrost_net::{status_line_code, status_line_is_success};
use quick_xml::Reader;
use quick_xml::events::Event;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CardDavContactEntry {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
}

/// Outcome of a depth-1 contact PROPFIND: the resources whose propstat
/// succeeded (`entries`) plus the hrefs the server reported *failed*
/// within the 207 (a non-2xx propstat). A failed href is a
/// transiently-failed resource, not an absent one - the snapshot diff
/// preserves the local copy rather than emitting a Destroyed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CardDavContactListing {
    pub(crate) entries: Vec<CardDavContactEntry>,
    pub(crate) failed_hrefs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CardDavFetchedVCard {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
    pub(crate) data: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CardDavFailedResource {
    pub(crate) href: String,
    pub(crate) status: Option<u16>,
}

impl CardDavFailedResource {
    pub(crate) fn is_missing_resource(&self) -> bool {
        matches!(self.status, Some(404 | 410))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CardDavMultigetReport {
    pub(crate) cards: Vec<CardDavFetchedVCard>,
    pub(crate) failed: Vec<CardDavFailedResource>,
    /// Resources that returned a successful response but omitted the
    /// requested `address-data`. They are per-resource absences, not DAV
    /// failures, so they surface through `Page::failed_ids` but cannot turn
    /// a 207 into a synthetic server error.
    pub(crate) missing_data: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MultigetOutcome {
    Usable,
    CompleteFailure { status: Option<u16> },
}

impl CardDavMultigetReport {
    pub(crate) fn extend(&mut self, other: CardDavMultigetReport) {
        self.cards.extend(other.cards);
        self.failed.extend(other.failed);
        self.missing_data.extend(other.missing_data);
    }

    pub(crate) fn classify(&self) -> MultigetOutcome {
        if !self.cards.is_empty() || self.failed.is_empty() {
            return MultigetOutcome::Usable;
        }
        match self
            .failed
            .iter()
            .find(|failure| !failure.is_missing_resource())
        {
            Some(failure) => MultigetOutcome::CompleteFailure {
                status: failure.status,
            },
            None => MultigetOutcome::Usable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AddressBookCollection {
    pub(crate) href: String,
    pub(crate) display_name: Option<String>,
    pub(crate) ctag: Option<String>,
    /// Writability derived from `current-user-privilege-set`: `Some(false)`
    /// when the server answered the property and named no write privilege,
    /// `None` when it did not answer it at all (in which case the account
    /// assumes writable, as CalDAV does). A read-only shared address book
    /// used to advertise writable unconditionally, so a consumer's capability
    /// gate passed a PUT the server was always going to 403.
    pub(crate) can_edit: Option<bool>,
}

impl AddressBookCollection {
    pub(crate) fn resolve_href(&mut self, request_url: &str) {
        self.href = resolve_href(request_url, &self.href);
    }
}

impl CardDavContactListing {
    pub(crate) fn resolve_hrefs(&mut self, request_url: &str) {
        for entry in &mut self.entries {
            entry.uri = resolve_href(request_url, &entry.uri);
        }
        for href in &mut self.failed_hrefs {
            *href = resolve_href(request_url, href);
        }
    }
}

impl CardDavMultigetReport {
    pub(crate) fn resolve_hrefs(&mut self, request_url: &str) {
        for card in &mut self.cards {
            card.uri = resolve_href(request_url, &card.uri);
        }
        for failed in &mut self.failed {
            failed.href = resolve_href(request_url, &failed.href);
        }
        for href in &mut self.missing_data {
            *href = resolve_href(request_url, href);
        }
    }
}

pub(crate) fn parse_addressbook_collections(
    xml: &str,
) -> Result<Vec<AddressBookCollection>, String> {
    let mut reader = Reader::from_str(xml);
    let mut collections = Vec::new();
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
                if current.in_response && name == "addressbook" {
                    current.mark_addressbook();
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
            Ok(Event::Text(value)) => {
                push_text(&mut text, value.as_ref())?;
            }
            Ok(Event::CData(value)) => {
                let value = value.decode().map_err(|error| error.to_string())?;
                text.push_str(&value);
            }
            Ok(Event::Empty(element)) => {
                let name = local_name(element.name().as_ref());
                if current.in_response && name == "addressbook" {
                    current.mark_addressbook();
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
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                if current.in_response {
                    match (parent, name.as_str()) {
                        (Some("response"), "href") => current.href = trimmed(&text),
                        (Some("prop"), "displayname") => {
                            current.staged.display_name = trimmed(&text);
                        }
                        (Some("prop"), "getctag") => {
                            current.staged.ctag = trimmed(&text);
                        }
                        (Some("propstat"), "status") => {
                            current.staged.success = Some(status_line_is_success(&text));
                        }
                        _ => {}
                    }
                }
                if name == "propstat" {
                    current.commit_propstat();
                }
                if name == "response" {
                    current.in_response = false;
                    if let Some(collection) = current.as_addressbook_collection() {
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

pub(crate) fn parse_propfind_contacts(xml: &str) -> Result<CardDavContactListing, String> {
    let mut reader = Reader::from_str(xml);
    let mut listing = CardDavContactListing::default();
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
                    current.mark_collection();
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
                    current.mark_collection();
                }
            }
            Ok(Event::Text(value)) => {
                push_text(&mut text, value.as_ref())?;
            }
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
                        (Some("prop"), "getetag") => current.staged.etag = normalize_etag(&text),
                        (Some("prop"), "getcontenttype") => {
                            current.staged.content_type = trimmed(&text);
                        }
                        (Some("propstat"), "status") => {
                            current.staged.success = Some(status_line_is_success(&text));
                        }
                        _ => {}
                    }
                }
                if name == "propstat" {
                    current.commit_propstat();
                }
                if name == "response" {
                    current.in_response = false;
                    if let Some(entry) = current.as_contact_entry() {
                        listing.entries.push(entry);
                    } else if let Some(href) = current.as_failed_contact_href() {
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

pub(crate) fn parse_multiget_report(xml: &str) -> Result<CardDavMultigetReport, String> {
    let mut reader = Reader::from_str(xml);
    let mut report = CardDavMultigetReport::default();
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
                    current.mark_collection();
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
                    current.mark_collection();
                }
            }
            Ok(Event::Text(value)) => {
                push_text(&mut text, value.as_ref())?;
            }
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
                        (Some("prop"), "getetag") => current.staged.etag = normalize_etag(&text),
                        (Some("prop"), "address-data") => {
                            current.staged.address_data = trimmed(&text);
                        }
                        (Some("propstat"), "status") => {
                            current.staged.status = trimmed(&text);
                            current.staged.success = Some(status_line_is_success(&text));
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
                    if let Some(card) = current.as_fetched_vcard() {
                        report.cards.push(card);
                    } else if let Some(failed) = current.as_failed_multiget_resource() {
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

/// Extract the collection `getctag` value from a depth-0 PROPFIND
/// response. Returns `None` when the server omits `getctag` (the caller
/// then falls through to a full snapshot + diff).
pub(crate) fn parse_collection_ctag(xml: &str) -> Result<Option<String>, String> {
    let mut reader = Reader::from_str(xml);
    let mut stack: Vec<String> = Vec::new();
    let mut text = String::new();
    // Track the ctag and status of the propstat currently being read,
    // and commit the ctag only when its propstat reports success. A
    // stale `getctag` returned inside a failed (non-2xx) propstat must
    // not feed the short-circuit, or a server emitting an old ctag in a
    // failed block would suppress a real change (false-positive
    // short-circuit). Mirrors the depth-1 parser's success gating.
    let mut propstat_ctag: Option<String> = None;
    let mut propstat_success: Option<bool> = None;
    let mut committed: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_name(element.name().as_ref());
                if name == "propstat" {
                    propstat_ctag = None;
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
                    (Some("prop"), "getctag") => propstat_ctag = trimmed(&text),
                    (Some("propstat"), "status") => {
                        propstat_success = Some(status_line_is_success(&text));
                    }
                    _ => {}
                }
                if name == "propstat" {
                    // Absent status defaults to success, matching
                    // `ResponseParts::commit_propstat`.
                    if propstat_success.unwrap_or(true)
                        && let Some(ctag) = propstat_ctag.take()
                    {
                        committed = Some(ctag);
                    }
                    propstat_ctag = None;
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

    Ok(committed)
}

pub(crate) fn extract_href_property(
    xml: &str,
    property_name: &str,
) -> Result<Option<String>, String> {
    let mut reader = Reader::from_str(xml);
    let mut stack = Vec::new();
    let mut text = String::new();
    let mut propstat_href = None;
    let mut propstat_success = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_name(element.name().as_ref());
                stack.push(name);
                text.clear();
            }
            Ok(Event::Text(value)) => {
                push_text(&mut text, value.as_ref())?;
            }
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
                        propstat_href = Some(href);
                    } else {
                        return Ok(Some(href));
                    }
                }
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                if name == "status" && matches!(parent, Some("propstat")) {
                    propstat_success = Some(
                        trimmed(&text)
                            .as_deref()
                            .is_some_and(status_line_is_success),
                    );
                }
                if name == "propstat" {
                    if propstat_success.unwrap_or(true) && propstat_href.is_some() {
                        return Ok(propstat_href);
                    }
                    propstat_href = None;
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

    Ok(None)
}

/// Properties read from the `propstat` currently being parsed, held apart
/// from the committed values until its `status` is known.
///
/// One struct rather than a `propstat_`-prefixed twin of every committed
/// field: the two reset lists this replaces had to be kept in sync by hand
/// (a `begin` that forgot a field would leak the previous propstat's value
/// into this one), and each of the three parsers below reads only the subset
/// it asked the server for - the rest simply stay `None` and commit nothing.
#[derive(Default)]
struct PropStat {
    /// `None` when the propstat carried no `status` element at all, which
    /// `commit` reads as success.
    success: Option<bool>,
    status: Option<String>,
    is_addressbook: bool,
    is_collection: bool,
    privilege_seen: bool,
    write_seen: bool,
    etag: Option<String>,
    content_type: Option<String>,
    address_data: Option<String>,
    display_name: Option<String>,
    ctag: Option<String>,
}

#[derive(Default)]
struct ResponseParts {
    in_response: bool,
    in_propstat: bool,
    has_success_propstat: bool,
    saw_failed_propstat: bool,
    is_collection: bool,
    is_addressbook: bool,
    privilege_seen: bool,
    write_seen: bool,
    href: Option<String>,
    etag: Option<String>,
    content_type: Option<String>,
    address_data: Option<String>,
    status: Option<String>,
    failed_statuses: Vec<u16>,
    display_name: Option<String>,
    ctag: Option<String>,
    staged: PropStat,
}

/// Commit a staged property over its committed slot, leaving the committed
/// value alone when this propstat did not carry the property.
fn commit_if_present<T>(committed: &mut Option<T>, staged: Option<T>) {
    if staged.is_some() {
        *committed = staged;
    }
}

impl ResponseParts {
    fn begin_propstat(&mut self) {
        self.in_propstat = true;
        self.staged = PropStat::default();
    }

    /// A `resourcetype` the server refused is not evidence about the
    /// resource, so a `collection` marker seen inside a propstat is staged
    /// and only promoted when that propstat's own status was 2xx. Otherwise
    /// a response whose contact properties succeeded but whose `resourcetype`
    /// block was rejected (echoing back `<collection/>` in the 404 prop
    /// skeleton) would be discarded as a collection.
    fn mark_collection(&mut self) {
        if self.in_propstat {
            self.staged.is_collection = true;
        } else {
            self.is_collection = true;
        }
    }

    fn mark_addressbook(&mut self) {
        if self.in_propstat {
            self.staged.is_addressbook = true;
        } else {
            self.is_addressbook = true;
        }
    }

    /// A `current-user-privilege-set` the server REFUSED says nothing about
    /// what this principal may do, so both privilege markers are staged and
    /// promoted only from a 2xx propstat, exactly like the CalDAV twin.
    fn mark_privilege_seen(&mut self) {
        if self.in_propstat {
            self.staged.privilege_seen = true;
        } else {
            self.privilege_seen = true;
        }
    }

    fn mark_write_seen(&mut self) {
        if self.in_propstat {
            self.staged.write_seen = true;
        } else {
            self.write_seen = true;
        }
    }

    fn commit_propstat(&mut self) {
        let staged = std::mem::take(&mut self.staged);
        self.in_propstat = false;

        if staged.success == Some(false) {
            self.saw_failed_propstat = true;
            if let Some(code) = staged.status.as_deref().and_then(status_line_code) {
                self.failed_statuses.push(code);
            }
        }
        // An absent status is success (RFC 4918 Section 14.22 requires one,
        // but servers omit it and the properties are still there).
        if staged.success.unwrap_or(true) {
            self.has_success_propstat = true;
            self.is_addressbook |= staged.is_addressbook;
            self.is_collection |= staged.is_collection;
            self.privilege_seen |= staged.privilege_seen;
            self.write_seen |= staged.write_seen;
            commit_if_present(&mut self.etag, staged.etag);
            commit_if_present(&mut self.content_type, staged.content_type);
            commit_if_present(&mut self.address_data, staged.address_data);
            commit_if_present(&mut self.display_name, staged.display_name);
            commit_if_present(&mut self.ctag, staged.ctag);
        }
    }

    fn as_addressbook_collection(&self) -> Option<AddressBookCollection> {
        if !self.is_addressbook {
            return None;
        }
        let href = self.href.as_ref()?;
        Some(AddressBookCollection {
            href: href.clone(),
            display_name: self.display_name.clone(),
            ctag: self.ctag.clone(),
            can_edit: self.privilege_seen.then_some(self.write_seen),
        })
    }

    fn as_contact_entry(&self) -> Option<CardDavContactEntry> {
        let href = self.href.as_ref()?;
        if self.is_collection {
            return None;
        }
        // Only a response whose ONLY propstat failed is withheld here - it is
        // surfaced by `as_failed_contact_href` instead. A response carrying no
        // propstat at all is still a member of the collection, so it commits
        // as an entry (with whatever etag it managed to supply, usually none).
        //
        // Requiring a successful propstat instead DESTROYED DATA: a bare
        // `<response><href/></response>` for a resource that exists landed in
        // neither lane - not an entry, and not a failed href either, since
        // `as_failed_contact_href` wants a failed propstat - so the resource
        // vanished from the snapshot and the diff emitted a `Destroyed` for a
        // contact the server still holds, with the failed-href preservation
        // guard unable to help. The CalDAV twin already read it this way; this
        // is that divergence closed on the non-destructive side.
        if self.saw_failed_propstat && !self.has_success_propstat {
            return None;
        }
        Some(CardDavContactEntry {
            uri: href.clone(),
            etag: self.etag.clone(),
        })
    }

    /// The href of a resource the server reported *failed* within the
    /// 207 (a non-2xx propstat, no success propstat). A response known
    /// to be a collection is excluded; resource names need no `.vcf` suffix.
    fn as_failed_contact_href(&self) -> Option<String> {
        if self.is_collection || self.has_success_propstat || !self.saw_failed_propstat {
            return None;
        }
        self.href.clone()
    }

    fn as_fetched_vcard(&self) -> Option<CardDavFetchedVCard> {
        if self.is_collection {
            return None;
        }
        Some(CardDavFetchedVCard {
            uri: self.href.as_ref()?.clone(),
            etag: self.etag.clone(),
            data: self.address_data.as_ref()?.clone(),
        })
    }

    // Accepted edge: a failed propstat whose status line is absent or
    // unparseable yields no numeric code here, so the resource degrades to
    // the benign `missing_data` lane and cannot contribute to
    // `CompleteFailure`. A pathological server failing every resource with
    // garbage status text thus reads as an empty success; tolerated because
    // such a server violates RFC 4918's required status line and the honest
    // lanes still preserve the resource (mirrored in caldav).
    fn as_failed_multiget_resource(&self) -> Option<CardDavFailedResource> {
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
        Some(CardDavFailedResource {
            href,
            status: Some(status),
        })
    }

    fn as_missing_multiget_data(&self) -> Option<String> {
        if self.is_collection || self.address_data.is_some() {
            return None;
        }
        self.href.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hrefs_resolve_against_the_request_uri() {
        assert_eq!(
            resolve_href("https://books.example.test/homes/ada/", "team/one.vcf"),
            "https://books.example.test/homes/ada/team/one.vcf"
        );
    }

    /// The migration to request-URI resolution must not respell ids that
    /// were already correct. Most servers emit absolute-path hrefs, and the
    /// pre-migration base was `CardDavConfig::base_url` with its trailing
    /// slash trimmed. Pin both spellings against the post-migration request
    /// URI, and pin the literal so a regression cannot pass by changing both
    /// sides at once.
    #[test]
    fn absolute_path_href_keeps_the_common_deployment_id() {
        let href = "/addressbooks/ada/one.vcf";
        let previous = resolve_href("https://dav.example.test", href);
        assert_eq!(
            previous,
            "https://dav.example.test/addressbooks/ada/one.vcf"
        );
        assert_eq!(
            resolve_href("https://dav.example.test/addressbooks/ada/", href),
            previous
        );
        assert_eq!(
            resolve_href("https://dav.example.test/dav/users/ada/addressbook/", href),
            previous
        );
    }

    #[test]
    fn propfind_contacts_extracts_vcards_and_etags() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/contacts/card-1.vcf</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"abc"</D:getetag>
        <D:getcontenttype>text/vcard; charset=utf-8</D:getcontenttype>
      </D:prop>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/contacts/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/></D:resourcetype>
        <D:getetag>"collection"</D:getetag>
        <D:getcontenttype>httpd/unix-directory</D:getcontenttype>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(
            listing.entries,
            vec![CardDavContactEntry {
                uri: "/contacts/card-1.vcf".to_string(),
                etag: Some("abc".to_string()),
            }]
        );
        assert!(listing.failed_hrefs.is_empty());
    }

    #[test]
    fn propfind_contacts_accepts_extensionless_resources_and_skips_collections() {
        let xml = r#"<D:multistatus xmlns:D="DAV:">
          <D:response><D:href>/contacts/opaque-id</D:href><D:propstat><D:prop>
          <D:resourcetype/><D:getetag>"abc"</D:getetag></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
          <D:response><D:href>/contacts/child</D:href><D:propstat><D:prop>
          <D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
          </D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(listing.entries.len(), 1);
        assert_eq!(listing.entries[0].uri, "/contacts/opaque-id");
    }

    #[test]
    fn propfind_contacts_ignores_failed_propstat_values() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/contacts/card-1.vcf</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"missing"</D:getetag>
        <D:getcontenttype>text/vcard</D:getcontenttype>
      </D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        // A 404-propstat resource is not committed as an entry, but it
        // IS surfaced as a failed href so the snapshot diff can preserve
        // the local copy instead of destroying it.
        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert!(listing.entries.is_empty());
        assert_eq!(
            listing.failed_hrefs,
            vec!["/contacts/card-1.vcf".to_string()]
        );
    }

    #[test]
    fn propfind_contacts_preserves_failed_extensionless_resource_but_not_collection() {
        let xml = r#"<D:multistatus xmlns:D="DAV:">
          <D:response><D:href>/contacts/opaque-id</D:href><D:propstat><D:prop><D:getetag/></D:prop>
          <D:status>HTTP/1.1 503 Unavailable</D:status></D:propstat></D:response>
          <D:response><D:href>/contacts/child</D:href>
          <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:getetag/></D:prop>
          <D:status>HTTP/1.1 503 Unavailable</D:status></D:propstat></D:response>
          </D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(listing.failed_hrefs, vec!["/contacts/opaque-id"]);
    }

    /// A `resourcetype` block the server REFUSED says nothing about the
    /// resource. Servers routinely echo the requested prop skeleton back
    /// inside a 404 propstat, and some echo a `<collection/>` child with
    /// it; treating that as authoritative discarded a real contact whose
    /// own properties came back 200.
    #[test]
    fn propfind_contacts_ignores_collection_marker_in_a_failed_propstat() {
        let xml = r#"<D:multistatus xmlns:D="DAV:">
          <D:response><D:href>/contacts/opaque-id</D:href>
          <D:propstat><D:prop><D:getetag>"abc"</D:getetag></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat></D:response>
          </D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(listing.entries.len(), 1);
        assert_eq!(listing.entries[0].uri, "/contacts/opaque-id");
        assert_eq!(listing.entries[0].etag, Some("abc".to_string()));
    }

    /// A response carrying NO propstat at all names a resource the server
    /// still holds. It used to land in neither lane - not an entry (no
    /// success propstat) and not a failed href (no failed propstat) - so the
    /// contact vanished from the snapshot and the diff destroyed a row that
    /// exists. It commits as an etag-less entry, as the CalDAV twin already
    /// read it.
    #[test]
    fn a_propstat_less_response_is_an_entry_rather_than_a_vanished_contact() {
        let xml = r#"<D:multistatus xmlns:D="DAV:">
          <D:response><D:href>/contacts/opaque-id</D:href></D:response>
          </D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(listing.entries.len(), 1, "{listing:?}");
        assert_eq!(listing.entries[0].uri, "/contacts/opaque-id");
        assert_eq!(listing.entries[0].etag, None);
        assert!(listing.failed_hrefs.is_empty());
    }

    /// Same rule on the multiget lane: a refused `resourcetype` must not
    /// suppress address data a successful propstat supplied.
    #[test]
    fn multiget_ignores_collection_marker_in_a_failed_propstat() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
          <D:response><D:href>/contacts/opaque-id</D:href>
          <D:propstat><D:prop><D:getetag>"abc"</D:getetag>
          <C:address-data>BEGIN:VCARD
VERSION:4.0
FN:One
END:VCARD</C:address-data></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          </D:response></D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid XML");
        assert_eq!(report.cards.len(), 1);
        assert_eq!(report.cards[0].uri, "/contacts/opaque-id");
    }

    /// The CalDAV twin guards this; the two crates had drifted. A server
    /// echoing the collection itself alongside the requested resources must
    /// not surface the collection URL as a card id.
    #[test]
    fn multiget_does_not_fetch_an_echoed_collection() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
          <D:response><D:href>/contacts/</D:href><D:propstat><D:prop>
          <D:resourcetype><D:collection/></D:resourcetype>
          <C:address-data>not a card</C:address-data></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
          </D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid XML");
        assert!(report.cards.is_empty());
    }

    #[test]
    fn propfind_contacts_ignores_nested_href_properties() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/card-1.vcf</D:href>
    <D:propstat>
      <D:prop>
        <D:owner><D:href>/principals/ada/</D:href></D:owner>
        <D:getetag>"abc"</D:getetag>
        <D:getcontenttype>text/vcard</D:getcontenttype>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(listing.entries[0].uri, "/contacts/card-1.vcf");
    }

    #[test]
    fn multiget_extracts_href_etag_and_vcard() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/card-1.vcf</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"abc"</D:getetag>
        <C:address-data>BEGIN:VCARD
FN:Ada Lovelace
END:VCARD</C:address-data>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid XML");
        assert_eq!(report.cards.len(), 1);
        assert_eq!(report.cards[0].uri, "/contacts/card-1.vcf");
        assert_eq!(report.cards[0].etag.as_deref(), Some("abc"));
        assert!(report.cards[0].data.contains("FN:Ada Lovelace"));
        assert!(report.failed.is_empty());
    }

    #[test]
    fn multiget_ignores_failed_propstat_values() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/card-1.vcf</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"abc"</D:getetag>
        <C:address-data>BEGIN:VCARD
FN:Ada Lovelace
END:VCARD</C:address-data>
      </D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid XML");
        assert!(report.cards.is_empty());
        assert_eq!(report.failed[0].status, Some(404));
    }

    #[test]
    fn multiget_complete_failure_is_classified() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/one.vcf</D:href>
    <D:propstat>
      <D:prop><C:address-data/></D:prop>
      <D:status>HTTP/1.1 401 Unauthorized</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/contacts/two.vcf</D:href>
    <D:propstat>
      <D:prop><C:address-data/></D:prop>
      <D:status>HTTP/1.1 401 Unauthorized</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.cards.is_empty());
        assert_eq!(report.failed.len(), 2);
        assert_eq!(
            report.classify(),
            MultigetOutcome::CompleteFailure { status: Some(401) }
        );
    }

    #[test]
    fn multiget_200_without_address_data_is_not_a_complete_failure() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/empty.vcf</D:href>
    <D:propstat>
      <D:prop><D:getetag>"e1"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.failed.is_empty());
        assert_eq!(report.missing_data, vec!["/contacts/empty.vcf"]);
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    // The depth-0 getctag PROPFIND backing the brick-8 ctag
    // short-circuit. The short-circuit decision (current == previous)
    // is exercised end-to-end downstream against a real server, per the
    // no-mock-server testing rule; here the deterministic piece is that
    // the helper extracts the collection ctag the decision keys off.
    #[test]
    fn ctag_short_circuit_parses_depth0_getctag() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop>
        <CS:getctag>ctag-7</CS:getctag>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let ctag = parse_collection_ctag(xml).expect("valid XML");
        assert_eq!(ctag.as_deref(), Some("ctag-7"));
    }

    // The status-line parsing this used to pin now lives in
    // `bifrost_net::status_line`, tested there against both DAV crates'
    // cases. A copy here would be the drift this consolidation removed.

    #[test]
    fn parse_collection_ctag_ignores_failed_propstat() {
        // A getctag returned inside a non-2xx propstat is stale and must
        // not feed the short-circuit; only a success propstat commits.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop>
        <CS:getctag>stale-ctag</CS:getctag>
      </D:prop>
      <D:status>HTTP/1.1 403 Forbidden</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        assert!(parse_collection_ctag(xml).expect("valid XML").is_none());
    }

    #[test]
    fn parse_collection_ctag_prefers_success_propstat() {
        // Failed propstat carries a stale ctag; the success propstat
        // carries the live one - the live one wins.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop><CS:getctag>stale-ctag</CS:getctag></D:prop>
      <D:status>HTTP/1.1 403 Forbidden</D:status>
    </D:propstat>
    <D:propstat>
      <D:prop><CS:getctag>live-ctag</CS:getctag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        assert_eq!(
            parse_collection_ctag(xml).expect("valid XML").as_deref(),
            Some("live-ctag")
        );
    }

    #[test]
    fn parse_collection_ctag_absent_returns_none() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop><D:displayname>Personal</D:displayname></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        assert!(parse_collection_ctag(xml).expect("valid XML").is_none());
    }

    #[test]
    fn extract_href_property_finds_nested_href() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:propstat>
      <D:prop>
        <D:current-user-principal>
          <D:href>/principals/user/</D:href>
        </D:current-user-principal>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let href = extract_href_property(xml, "current-user-principal").expect("valid XML");
        assert_eq!(href.as_deref(), Some("/principals/user/"));
    }

    #[test]
    fn nested_property_status_does_not_refuse_href_property() {
        let href = extract_href_property(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:response><D:propstat><D:status>HTTP/1.1 200 OK</D:status><D:prop><C:addressbook-home-set><D:href>/right/</D:href><C:extension><D:status>HTTP/1.1 404 Not Found</D:status></C:extension></C:addressbook-home-set></D:prop></D:propstat></D:response></D:multistatus>"#,
            "addressbook-home-set",
        )
        .expect("valid property");

        assert_eq!(href.as_deref(), Some("/right/"));
    }

    #[test]
    fn href_extractor_ignores_failed_propstat() {
        let href = extract_href_property(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:response><D:propstat><D:prop><C:addressbook-home-set><D:href>/wrong/</D:href></C:addressbook-home-set></D:prop><D:status>HTTP/1.1 404 Not Found</D:status></D:propstat><D:propstat><D:prop><C:addressbook-home-set><D:href>/right/</D:href></C:addressbook-home-set></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
            "addressbook-home-set",
        )
        .expect("valid XML");

        assert_eq!(href.as_deref(), Some("/right/"));
    }

    #[test]
    fn addressbook_collections_read_display_names() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav" xmlns:CS="http://calendarserver.org/ns/">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:addressbook/></D:resourcetype>
        <D:displayname>Personal</D:displayname>
        <CS:getctag>42</CS:getctag>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let books = parse_addressbook_collections(xml).expect("valid XML");
        assert_eq!(
            books,
            vec![AddressBookCollection {
                href: "/contacts/personal/".to_string(),
                display_name: Some("Personal".to_string()),
                ctag: Some("42".to_string()),
                can_edit: None,
            }]
        );
    }

    /// Writability comes from `current-user-privilege-set`, as CalDAV's does.
    /// A read-only shared book that advertises writable makes a consumer's
    /// capability gate pass a PUT the server will 403.
    #[test]
    fn addressbook_collections_derive_writability_from_the_privilege_set() {
        let book = |privileges: &str| {
            let xml = format!(
                r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response><D:href>/contacts/shared/</D:href><D:propstat><D:prop>
    <D:resourcetype><D:collection/><C:addressbook/></D:resourcetype>
    <D:current-user-privilege-set>{privileges}</D:current-user-privilege-set>
  </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
</D:multistatus>"#
            );
            parse_addressbook_collections(&xml).expect("valid XML")[0].can_edit
        };

        assert_eq!(
            book("<D:privilege><D:read/></D:privilege>"),
            Some(false),
            "a read-only book must report read-only"
        );
        assert_eq!(
            book(
                "<D:privilege><D:read/></D:privilege><D:privilege><D:write-content/></D:privilege>"
            ),
            Some(true)
        );
    }

    /// A server that never answers the property leaves the answer unknown,
    /// and the account assumes writable rather than locking the user out.
    #[test]
    fn an_unanswered_privilege_set_leaves_writability_unknown() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response><D:href>/contacts/personal/</D:href><D:propstat><D:prop>
    <D:resourcetype><D:collection/><C:addressbook/></D:resourcetype>
  </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
</D:multistatus>"#;

        assert_eq!(
            parse_addressbook_collections(xml).expect("valid XML")[0].can_edit,
            None
        );
    }

    #[test]
    fn addressbook_collections_ignore_failed_propstat_values() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:addressbook/></D:resourcetype>
        <D:displayname>Personal</D:displayname>
      </D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let books = parse_addressbook_collections(xml).expect("valid XML");
        assert!(books.is_empty());
    }

    #[test]
    fn cdata_is_read_by_carddav_parsers() {
        let books = parse_addressbook_collections(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:response><D:href><![CDATA[/contacts/]]></D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:addressbook/></D:resourcetype><D:displayname><![CDATA[Personal]]></D:displayname></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid collections");
        assert_eq!(books[0].display_name.as_deref(), Some("Personal"));

        let listing = parse_propfind_contacts(
            r#"<D:multistatus xmlns:D="DAV:"><D:response><D:href><![CDATA[/contacts/one.vcf]]></D:href><D:propstat><D:prop><D:getetag><![CDATA["one"]]></D:getetag><D:getcontenttype>text/vcard</D:getcontenttype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid listing");
        assert_eq!(listing.entries[0].etag.as_deref(), Some("one"));

        let report = parse_multiget_report(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:response><D:href><![CDATA[/contacts/one.vcf]]></D:href><D:propstat><D:prop><C:address-data><![CDATA[BEGIN:VCARD
FN:Ada
END:VCARD]]></C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid multiget");
        assert!(report.cards[0].data.contains("FN:Ada"));

        let ctag = parse_collection_ctag(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/"><D:response><D:propstat><D:prop><CS:getctag><![CDATA[tag-2]]></CS:getctag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid ctag");
        assert_eq!(ctag.as_deref(), Some("tag-2"));

        let href = extract_href_property(
            r#"<D:current-user-principal xmlns:D="DAV:"><D:href><![CDATA[/principals/ada/]]></D:href></D:current-user-principal>"#,
            "current-user-principal",
        )
        .expect("valid property");
        assert_eq!(href.as_deref(), Some("/principals/ada/"));
    }

    #[test]
    fn weak_etag_keeps_weakness_marker() {
        assert_eq!(normalize_etag("W/\"abc\"").as_deref(), Some("W/\"abc\""));
        assert_eq!(normalize_etag("\"abc\"").as_deref(), Some("abc"));
    }
}
