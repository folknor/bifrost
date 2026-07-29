use quick_xml::Reader;
use quick_xml::escape::unescape;
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
pub(crate) struct AddressBookCollection {
    pub(crate) href: String,
    pub(crate) display_name: Option<String>,
    pub(crate) ctag: Option<String>,
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
                stack.push(name);
                text.clear();
            }
            Ok(Event::Text(value)) => {
                push_text(&mut text, value.as_ref())?;
            }
            Ok(Event::Empty(element)) => {
                let name = local_name(element.name().as_ref());
                if current.in_response && name == "addressbook" {
                    current.mark_addressbook();
                }
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
                        (Some("prop"), "getctag") => {
                            current.propstat_ctag = trimmed(&text);
                        }
                        (Some("propstat"), "status") => {
                            current.propstat_success = Some(is_success_status(&text));
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
                stack.push(name);
                text.clear();
            }
            Ok(Event::Text(value)) => {
                push_text(&mut text, value.as_ref())?;
            }
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                if current.in_response {
                    match (parent, name.as_str()) {
                        (Some("response"), "href") => current.href = trimmed(&text),
                        (Some("prop"), "getetag") => current.propstat_etag = normalize_etag(&text),
                        (Some("prop"), "getcontenttype") => {
                            current.propstat_content_type = trimmed(&text);
                        }
                        (Some("propstat"), "status") => {
                            current.propstat_success = Some(is_success_status(&text));
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

pub(crate) fn parse_multiget_report(xml: &str) -> Result<Vec<CardDavFetchedVCard>, String> {
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
                if current.in_response && name == "propstat" {
                    current.begin_propstat();
                }
                stack.push(name);
                text.clear();
            }
            Ok(Event::Text(value)) => {
                push_text(&mut text, value.as_ref())?;
            }
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                if current.in_response {
                    match (parent, name.as_str()) {
                        (Some("response"), "href") => current.href = trimmed(&text),
                        (Some("prop"), "getetag") => current.propstat_etag = normalize_etag(&text),
                        (Some("prop"), "address-data") => {
                            current.propstat_address_data = trimmed(&text);
                        }
                        (Some("propstat"), "status") => {
                            current.propstat_success = Some(is_success_status(&text));
                        }
                        _ => {}
                    }
                }
                if name == "propstat" {
                    current.commit_propstat();
                }
                if name == "response" {
                    current.in_response = false;
                    if let Some(card) = current.as_fetched_vcard() {
                        results.push(card);
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
            Ok(Event::End(element)) => {
                let name = local_name(element.name().as_ref());
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                match (parent, name.as_str()) {
                    (Some("prop"), "getctag") => propstat_ctag = trimmed(&text),
                    (Some("propstat"), "status") => {
                        propstat_success = Some(is_success_status(&text));
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
            Ok(Event::Text(value)) => {
                push_text(&mut text, value.as_ref())?;
            }
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

fn is_success_status(value: &str) -> bool {
    // Parse the first whitespace-delimited token that is a 3-digit-ish
    // numeric code and test the 2xx range, rather than positionally
    // assuming the leading `HTTP/x` token is present. A status line that
    // omits the protocol token (`200 OK`) would otherwise have its code
    // read as the word `OK` and be misclassified as a failed propstat.
    // Matches CalDAV's `status_code` + `200..=299` check.
    status_code(value).is_some_and(|code| matches!(code, 200..=299))
}

fn status_code(value: &str) -> Option<u16> {
    value
        .split_whitespace()
        .find_map(|part| part.parse::<u16>().ok())
}

fn normalize_etag(text: &str) -> Option<String> {
    trimmed(text).map(|value| value.trim_matches('"').to_string())
}

fn is_vcard_resource(href: &str, content_type: &Option<String>) -> bool {
    content_type
        .as_deref()
        .is_some_and(|ty| ty.to_ascii_lowercase().contains("text/vcard"))
        || href.to_ascii_lowercase().ends_with(".vcf")
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
    is_addressbook: bool,
    propstat_is_addressbook: bool,
    href: Option<String>,
    etag: Option<String>,
    propstat_etag: Option<String>,
    content_type: Option<String>,
    propstat_content_type: Option<String>,
    address_data: Option<String>,
    propstat_address_data: Option<String>,
    display_name: Option<String>,
    propstat_display_name: Option<String>,
    ctag: Option<String>,
    propstat_ctag: Option<String>,
}

impl ResponseParts {
    fn begin_propstat(&mut self) {
        self.in_propstat = true;
        self.propstat_success = None;
        self.propstat_is_addressbook = false;
        self.propstat_etag = None;
        self.propstat_content_type = None;
        self.propstat_address_data = None;
        self.propstat_display_name = None;
        self.propstat_ctag = None;
    }

    fn mark_addressbook(&mut self) {
        if self.in_propstat {
            self.propstat_is_addressbook = true;
        } else {
            self.is_addressbook = true;
        }
    }

    fn commit_propstat(&mut self) {
        if self.propstat_success == Some(false) {
            self.saw_failed_propstat = true;
        }
        if self.propstat_success.unwrap_or(true) {
            self.has_success_propstat = true;
            self.is_addressbook |= self.propstat_is_addressbook;
            if self.propstat_etag.is_some() {
                self.etag = self.propstat_etag.take();
            }
            if self.propstat_content_type.is_some() {
                self.content_type = self.propstat_content_type.take();
            }
            if self.propstat_address_data.is_some() {
                self.address_data = self.propstat_address_data.take();
            }
            if self.propstat_display_name.is_some() {
                self.display_name = self.propstat_display_name.take();
            }
            if self.propstat_ctag.is_some() {
                self.ctag = self.propstat_ctag.take();
            }
        }
        self.in_propstat = false;
        self.propstat_success = None;
        self.propstat_is_addressbook = false;
        self.propstat_etag = None;
        self.propstat_content_type = None;
        self.propstat_address_data = None;
        self.propstat_display_name = None;
        self.propstat_ctag = None;
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
        })
    }

    fn as_contact_entry(&self) -> Option<CardDavContactEntry> {
        let href = self.href.as_ref()?;
        if !self.has_success_propstat {
            return None;
        }
        if !is_vcard_resource(href, &self.content_type) {
            return None;
        }
        Some(CardDavContactEntry {
            uri: href.clone(),
            etag: self.etag.clone(),
        })
    }

    /// The href of a resource the server reported *failed* within the
    /// 207 (a non-2xx propstat, no success propstat). Only vcard
    /// resources are surfaced - a failed collection is not a
    /// transiently-failed resource and must not leak into the
    /// failed-href channel. Content-type is committed only on success,
    /// so detection keys on the `.vcf` href suffix.
    fn as_failed_contact_href(&self) -> Option<String> {
        if self.has_success_propstat || !self.saw_failed_propstat {
            return None;
        }
        let href = self.href.as_ref()?;
        if !href.to_ascii_lowercase().ends_with(".vcf") {
            return None;
        }
        Some(href.clone())
    }

    fn as_fetched_vcard(&self) -> Option<CardDavFetchedVCard> {
        Some(CardDavFetchedVCard {
            uri: self.href.as_ref()?.clone(),
            etag: self.etag.clone(),
            data: self.address_data.as_ref()?.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let cards = parse_multiget_report(xml).expect("valid XML");
        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0].uri, "/contacts/card-1.vcf");
        assert_eq!(cards[0].etag.as_deref(), Some("abc"));
        assert!(cards[0].data.contains("FN:Ada Lovelace"));
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

        let cards = parse_multiget_report(xml).expect("valid XML");
        assert!(cards.is_empty());
    }

    #[test]
    fn multiget_complete_failure_is_indistinguishable_from_empty() {
        // GAP (documented, not endorsed): unlike CalDAV's multiget parser,
        // which returns per-resource failures and classifies an all-failed
        // 207 as a CompleteFailure (RFC 4918 s13), the CardDAV multiget
        // returns only the successes. An all-401 body parses to an empty
        // Vec that the caller cannot tell apart from a legitimately empty
        // result, so it surfaces as an empty page a consumer records as a
        // completed walk. Pinned so a future fix flips this loudly.
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

        let cards = parse_multiget_report(xml).expect("valid 207");
        assert!(cards.is_empty());
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

    #[test]
    fn is_success_status_parses_code_without_protocol_token() {
        // A status line missing the leading `HTTP/x` token must still
        // classify on the numeric code, not positionally read the second
        // whitespace token (which would be `OK`).
        assert!(is_success_status("200 OK"));
        assert!(is_success_status("HTTP/1.1 207 Multi-Status"));
        assert!(!is_success_status("404 Not Found"));
        assert!(!is_success_status("HTTP/1.1 404 Not Found"));
    }

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
            }]
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
}
