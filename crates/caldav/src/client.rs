use std::fmt;
#[cfg(test)]
use std::sync::Arc;

pub(crate) use bifrost_dav_core::PutCondition;
#[cfg(test)]
use bifrost_dav_core::ReqwestDavTransport;
use bifrost_dav_core::{DavDispatch, DavProtocol, prepare_if_match, response_etag, worse_recovery};
#[cfg(test)]
pub(crate) use bifrost_dav_core::{DavResponse, DavTransport};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, CursorScope,
    DiagnosticText, ErrorScope, ObjectType, Protocol, ResourceKind, StateCause, SyncStateErrorKind,
};
#[cfg(test)]
use reqwest::header::{AUTHORIZATION, HeaderMap};
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use reqwest::{Method, StatusCode};

use crate::CalDavConfig;
use crate::parse::{
    CalDavFetchedEvent, CalDavMultigetReport, CalDavSyncReport, CalendarCollection,
    MultigetOutcome, extract_href_properties, extract_href_property, parse_calendar_collections,
    parse_multiget_report, parse_propfind_events, parse_sync_collection_report, resolve_href,
};

const MULTIGET_BATCH_SIZE: usize = 50;

/// In-flight REPORT legs a single multiget or text search may hold open.
///
/// The chunk count is driven by the caller's uri list, so an unbounded
/// `join_all` over it lets one large collection launch hundreds of
/// simultaneous REPORTs at a server that never agreed to that. `bifrost-net`
/// has no concurrency governor, so the bound belongs here, at the call site
/// that knows the fan-out is input-sized. Ordered (`buffered`, not
/// `buffer_unordered`) so the merged report and the surviving degraded error
/// stay deterministic regardless of completion order.
const MULTIGET_LEG_CONCURRENCY: usize = 4;

/// This crate's dialect binding for the shared DAV layer.
///
/// Every shared constructor takes it, so a CalDAV error can never be stamped
/// with a CardDAV `Protocol` or name a `ResourceKind::Contact`.
const DAV: DavProtocol = DavProtocol::CalDav;

#[derive(Clone)]
pub(crate) struct CalDavClient {
    /// Transport, credentials, origin gate and the generic WebDAV verbs, all
    /// shared with `bifrost-carddav` through `bifrost-dav-core`.
    dav: DavDispatch,
}

#[derive(Debug)]
pub(crate) struct CalDavDiscovery {
    pub(crate) calendar_home: String,
    pub(crate) calendar_user_email: Option<String>,
    pub(crate) schedule_outbox_url: Option<String>,
}

impl fmt::Debug for CalDavClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CalDavClient")
            .field("dav", &self.dav)
            .finish_non_exhaustive()
    }
}

impl CalDavClient {
    pub(crate) fn resolve_url(&self, href: &str) -> String {
        self.dav.resolve_url(href)
    }

    pub(crate) fn admit_discovered_urls(&mut self, urls: impl IntoIterator<Item = String>) {
        self.dav.admit_discovered_urls(urls);
    }

    pub(crate) async fn move_resource(
        &self,
        from: &str,
        to: &str,
        operation: AccountOperation,
    ) -> Result<bool, AccountError> {
        self.dav.move_resource(from, to, operation).await
    }

    pub(crate) fn new(config: &CalDavConfig) -> Result<Self, AccountError> {
        Ok(Self {
            dav: DavDispatch::new(&config.base_url, config.credentials.to_shared(), DAV)?,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_transport(base_url: &str, transport: Arc<dyn DavTransport>) -> Self {
        Self {
            dav: DavDispatch::with_transport(
                base_url,
                transport,
                crate::CalDavCredentials::bearer("token").to_shared(),
                DAV,
            ),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_base_url(base_url: &str) -> Self {
        Self::with_transport(base_url, Arc::new(ReqwestDavTransport))
    }

    pub(crate) async fn discover_account(&self) -> Result<CalDavDiscovery, AccountError> {
        // Well-known discovery lives at the ORIGIN root (RFC 6764), so a
        // configured base carrying a path must not have the well-known
        // suffix appended to it: the resulting URL is not a discovery
        // endpoint, and a deployment answering it with 401/403 rather
        // than 404 would fail the open before the configured base was
        // ever tried.
        let from_well_known = match bifrost_net::url::well_known_url(self.dav.base_url(), "caldav")
        {
            Some(well_known) => match self.discover_principal(&well_known).await {
                Ok(principal) => principal,
                Err(error) if should_fallback_discovery(&error) => None,
                Err(error) => return Err(error),
            },
            None => None,
        };
        let principal = match from_well_known {
            Some(principal) => principal,
            None => self.discover_principal_from_base().await?,
        };
        self.discover_account_for_principal(&principal).await
    }

    async fn discover_principal_from_base(&self) -> Result<String, AccountError> {
        self.discover_principal(self.dav.base_url())
            .await?
            .ok_or_else(|| {
                parse_error(AccountOperation::Discover, "missing current-user-principal")
            })
    }

    async fn discover_account_for_principal(
        &self,
        principal: &str,
    ) -> Result<CalDavDiscovery, AccountError> {
        let response = self
            .dav
            .propfind_raw(principal, "0", PROPFIND_ACCOUNT, AccountOperation::Discover)
            .await?;
        let body = response.text;
        let base = response.url;
        let calendar_home = extract_href_property(&body, "calendar-home-set")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?
            .map(|href| resolve_href(&base, &href))
            .ok_or_else(|| parse_error(AccountOperation::Discover, "missing calendar-home-set"))?;
        let hrefs = extract_href_properties(&body, "calendar-user-address-set")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?;
        let calendar_user_email = hrefs.iter().find_map(|href| mailto_email(href));
        let schedule_outbox_url = extract_href_property(&body, "schedule-outbox-URL")
            .map_err(|error| parse_error(AccountOperation::Discover, error))
            .map(|href| href.map(|href| resolve_href(&base, &href)))?;
        Ok(CalDavDiscovery {
            calendar_home,
            calendar_user_email,
            schedule_outbox_url,
        })
    }

    async fn discover_principal(&self, root: &str) -> Result<Option<String>, AccountError> {
        let response = self
            .dav
            .propfind_raw(root, "0", PROPFIND_PRINCIPAL, AccountOperation::Discover)
            .await?;
        Ok(
            extract_href_property(&response.text, "current-user-principal")
                .map_err(|error| parse_error(AccountOperation::Discover, error))?
                .map(|href| resolve_href(&response.url, &href)),
        )
    }

    pub(crate) async fn list_calendars(
        &self,
        home_url: &str,
    ) -> Result<Vec<CalendarCollection>, AccountError> {
        self.list_calendars_for_operation(home_url, AccountOperation::CalendarsList)
            .await
    }

    pub(crate) async fn list_calendars_for_operation(
        &self,
        home_url: &str,
        operation: AccountOperation,
    ) -> Result<Vec<CalendarCollection>, AccountError> {
        let response = self
            .dav
            .propfind_raw(home_url, "1", PROPFIND_CALENDARS, operation)
            .await?;
        let mut collections = parse_calendar_collections(&response.text)
            .map_err(|error| parse_error(operation, format!("calendar list: {error}")))?;
        for collection in &mut collections {
            collection.resolve_href(&response.url);
        }
        Ok(collections)
    }

    /// Depth-1 event PROPFIND returning both the committed entries and
    /// the hrefs the server reported *failed* within the 207, so the
    /// snapshot diff can preserve transiently-failed resources rather
    /// than destroying them (brick 7).
    pub(crate) async fn list_events_listing(
        &self,
        calendar_url: &str,
        operation: AccountOperation,
    ) -> Result<crate::parse::CalDavEventListing, AccountError> {
        let response = self
            .dav
            .propfind_raw(calendar_url, "1", PROPFIND_EVENTS, operation)
            .await?;
        let mut listing =
            parse_propfind_events(&response.text).map_err(|error| parse_error(operation, error))?;
        listing.resolve_hrefs(&response.url);
        Ok(listing)
    }

    /// Cheap depth-0 PROPFIND used by snapshot polling to refresh the
    /// collection sync token without re-listing every calendar collection.
    pub(crate) async fn collection_sync_token(
        &self,
        calendar_url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let response = self
            .dav
            .propfind_raw(calendar_url, "0", PROPFIND_SYNC_TOKEN, operation)
            .await?;
        crate::parse::parse_collection_sync_token(&response.text)
            .map_err(|error| parse_error(operation, format!("collection sync token: {error}")))
    }

    pub(crate) async fn query_events_in_range(
        &self,
        calendar_url: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<CalDavMultigetReport, AccountError> {
        let body = calendar_query_body(start, end);
        let response = self
            .dav
            .report_raw(calendar_url, "1", &body, AccountOperation::EventsInRange)
            .await?;
        let mut parsed = parse_multiget_report(&response.text).map_err(|error| {
            parse_error(AccountOperation::EventsInRange, format!("query: {error}"))
        })?;
        parsed.resolve_hrefs(&response.url);
        multiget_failure(&parsed, AccountOperation::EventsInRange).map_or(Ok(parsed), Err)
    }

    pub(crate) async fn query_events_text(
        &self,
        calendar_url: &str,
        query: &str,
    ) -> Result<MultigetFetch, AccountError> {
        let mut all_results = CalDavMultigetReport::default();
        let mut degraded = None;
        let bodies = ["SUMMARY", "DESCRIPTION", "LOCATION", "ATTENDEE"]
            .map(|property| calendar_text_query_body(property, query));
        let legs: Vec<_> = bodies
            .iter()
            .map(|body| {
                self.run_leg(MultigetLeg {
                    url: calendar_url,
                    depth: "1",
                    body,
                    operation: AccountOperation::EventSearch,
                    context: "query",
                })
            })
            .collect();
        let mut legs =
            futures::StreamExt::buffered(futures::stream::iter(legs), MULTIGET_LEG_CONCURRENCY);
        while let Some((report, error)) = futures::StreamExt::next(&mut legs).await {
            all_results.extend(report);
            if let Some(error) = error {
                degraded = worse_recovery(degraded, error);
            }
        }
        MultigetFetch::settle(all_results, degraded)
    }

    pub(crate) async fn fetch_events(
        &self,
        calendar_url: &str,
        uris: &[String],
        operation: AccountOperation,
    ) -> Result<MultigetFetch, AccountError> {
        let mut all_results = CalDavMultigetReport::default();
        let mut degraded = None;
        let bodies = uris
            .chunks(MULTIGET_BATCH_SIZE)
            .map(|chunk| {
                let mut href_elements = String::new();
                for uri in chunk {
                    href_elements.push_str("  <D:href>");
                    href_elements.push_str(&escape_xml(uri));
                    href_elements.push_str("</D:href>\n");
                }

                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<C:calendar-multiget xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
    <C:calendar-data/>\n\
  </D:prop>\n\
{href_elements}</C:calendar-multiget>"
                );
                body
            })
            .collect::<Vec<_>>();
        let legs: Vec<_> = bodies
            .iter()
            .map(|body| {
                self.run_leg(MultigetLeg {
                    url: calendar_url,
                    depth: "0",
                    body,
                    operation,
                    context: "multiget",
                })
            })
            .collect();
        let mut legs =
            futures::StreamExt::buffered(futures::stream::iter(legs), MULTIGET_LEG_CONCURRENCY);
        while let Some((report, error)) = futures::StreamExt::next(&mut legs).await {
            all_results.extend(report);
            if let Some(error) = error {
                degraded = worse_recovery(degraded, error);
            }
        }
        MultigetFetch::settle(all_results, degraded)
    }

    /// Run one REPORT leg of a multi-leg fetch and fold its outcome into the
    /// caller's accumulators.
    ///
    /// This is the only way a leg result enters `all_results`, and it is what
    /// makes the partial-result contract structural rather than a habit. A leg
    /// can fail four ways - transport, a non-2xx status, a body that will not
    /// parse, and a 207 that describes complete failure - and all four land in
    /// `degraded` here. Routing only some of them (the shape this replaced
    /// classified the HTTP failure but kept `?` on the parse) meant a malformed
    /// body on chunk 3 of 40 threw away chunks 1 and 2, which is exactly the
    /// loss the degraded lane exists to prevent. The function returns nothing,
    /// so a leg added later has no unrouted path available to it.
    ///
    /// A malformed body is account-authored data, so it is classified and
    /// survived, never asserted on. `MultigetFetch::settle` is what turns "every
    /// leg failed and nothing materialized anywhere" back into an `Err` carrying
    /// the worst recovery class seen.
    async fn accumulate_leg(
        &self,
        leg: MultigetLeg<'_>,
        all_results: &mut CalDavMultigetReport,
        degraded: &mut Option<AccountError>,
    ) {
        let MultigetLeg {
            url,
            depth,
            body,
            operation,
            context,
        } = leg;
        let response = match self.dav.report_raw(url, depth, body, operation).await {
            Ok(response) => response,
            Err(error) => {
                *degraded = worse_recovery(degraded.take(), error);
                return;
            }
        };
        let mut parsed = match parse_multiget_report(&response.text) {
            Ok(parsed) => parsed,
            Err(error) => {
                *degraded = worse_recovery(
                    degraded.take(),
                    parse_error(operation, format!("{context}: {error}")),
                );
                return;
            }
        };
        parsed.resolve_hrefs(&response.url);
        if let Some(error) = multiget_failure(&parsed, operation) {
            *degraded = worse_recovery(degraded.take(), error);
        }
        all_results.extend(parsed);
    }

    async fn run_leg(&self, leg: MultigetLeg<'_>) -> (CalDavMultigetReport, Option<AccountError>) {
        let mut report = CalDavMultigetReport::default();
        let mut degraded = None;
        self.accumulate_leg(leg, &mut report, &mut degraded).await;
        (report, degraded)
    }

    pub(crate) async fn sync_events(
        &self,
        calendar_url: &str,
        sync_token: &str,
    ) -> Result<CalDavSyncReport, AccountError> {
        let body = sync_collection_body(sync_token);
        let operation = AccountOperation::SyncChanges;
        let response = self
            .dav
            .report_raw_response(calendar_url, "0", &body, operation)
            .await?;
        let status = response.status;
        let effective_url = response.url;
        let body = response.body;
        if status == StatusCode::GONE
            || (status == StatusCode::FORBIDDEN
                && body.to_ascii_lowercase().contains("valid-sync-token"))
        {
            return Err(cursor_invalid_error(status, body));
        }
        if !status.is_success() {
            return Err(status_error(operation, status, body));
        }
        let mut report = parse_sync_collection_report(&body).map_err(|error| {
            parse_error(AccountOperation::SyncChanges, format!("sync: {error}"))
        })?;
        report.resolve_hrefs(&effective_url);
        Ok(report)
    }

    pub(crate) async fn get_event(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<CalDavFetchedEvent, AccountError> {
        let request = self
            .dav
            .request(Method::GET, url)
            .headers(self.dav.auth_headers(url, operation).await?);
        let response = self.dav.send_raw_request(request, operation).await?;
        let status = response.status;
        let etag = response_etag(&response.headers);
        let body = response.body;
        if status.is_success() {
            Ok(CalDavFetchedEvent {
                uri: url.to_string(),
                etag,
                data: body,
            })
        } else {
            Err(status_error(operation, status, body))
        }
    }

    pub(crate) async fn put_event(
        &self,
        url: &str,
        body: String,
        condition: PutCondition<'_>,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let mut request = self
            .dav
            .request(Method::PUT, url)
            .header(CONTENT_TYPE, "text/calendar; charset=utf-8")
            .headers(self.dav.auth_headers(url, operation).await?)
            .body(body);
        match condition {
            PutCondition::IfNoneMatch => {
                request = request.header("If-None-Match", "*");
            }
            PutCondition::IfMatch(etag) => {
                if let Some(etag) = prepare_if_match(etag) {
                    request = request.header("If-Match", etag);
                }
            }
            PutCondition::None => {}
        }
        let response = self.dav.send_raw_request(request, operation).await?;
        let status = response.status;
        let etag = response_etag(&response.headers);
        let body = response.body;
        if status.is_success() {
            Ok(etag)
        } else {
            Err(status_error(operation, status, body))
        }
    }

    pub(crate) async fn delete_event(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        let request = self
            .dav
            .request(Method::DELETE, url)
            .headers(self.dav.auth_headers(url, operation).await?);
        self.dav.send_status_request(request, operation).await
    }

    pub(crate) async fn post_schedule_reply(
        &self,
        outbox_url: &str,
        originator: &str,
        recipient: &str,
        body: String,
    ) -> Result<(), AccountError> {
        // RFC 6638 outbox POSTs route iTIP via the `Originator` (the
        // replying calendar user) and `Recipient` (the organizer) headers;
        // servers reject the POST without them.
        let mut request = self
            .dav
            .request(Method::POST, outbox_url)
            .header(CONTENT_TYPE, "text/calendar; charset=utf-8")
            .headers(
                self.dav
                    .auth_headers(outbox_url, AccountOperation::EventRsvp)
                    .await?,
            );
        if let Ok(value) = HeaderValue::from_str(&schedule_address(originator)) {
            request = request.header("Originator", value);
        }
        if let Ok(value) = HeaderValue::from_str(&schedule_address(recipient)) {
            request = request.header("Recipient", value);
        }
        let request = request.body(body);
        self.dav
            .send_status_request(request, AccountOperation::EventRsvp)
            .await
    }
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn mailto_email(href: &str) -> Option<String> {
    href.get(.."mailto:".len())
        .filter(|prefix| prefix.eq_ignore_ascii_case("mailto:"))
        .and_then(|_| href.get("mailto:".len()..))
        .filter(|email| email.contains('@'))
        .map(str::to_ascii_lowercase)
}

fn schedule_address(address: &str) -> String {
    // iTIP calendar-user addresses are URIs; bare emails (the form both
    // discovery and the organizer field carry) become `mailto:` URIs.
    if address.contains(':') {
        address.to_string()
    } else {
        format!("mailto:{address}")
    }
}

fn calendar_query_body(start: Option<&str>, end: Option<&str>) -> String {
    let time_range = match (start, end) {
        (Some(start), Some(end)) => format!(
            "      <C:time-range start=\"{}\" end=\"{}\"/>\n",
            escape_xml(start),
            escape_xml(end)
        ),
        (Some(start), None) => format!("      <C:time-range start=\"{}\"/>\n", escape_xml(start)),
        (None, Some(end)) => format!("      <C:time-range end=\"{}\"/>\n", escape_xml(end)),
        (None, None) => String::new(),
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<C:calendar-query xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
    <C:calendar-data/>\n\
  </D:prop>\n\
  <C:filter>\n\
    <C:comp-filter name=\"VCALENDAR\">\n\
      <C:comp-filter name=\"VEVENT\">\n\
{time_range}      </C:comp-filter>\n\
    </C:comp-filter>\n\
  </C:filter>\n\
</C:calendar-query>"
    )
}

fn calendar_text_query_body(property: &str, query: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<C:calendar-query xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
    <C:calendar-data/>\n\
  </D:prop>\n\
  <C:filter>\n\
    <C:comp-filter name=\"VCALENDAR\">\n\
      <C:comp-filter name=\"VEVENT\">\n\
        <C:prop-filter name=\"{}\">\n\
          <C:text-match collation=\"i;unicode-casemap\">{}</C:text-match>\n\
        </C:prop-filter>\n\
      </C:comp-filter>\n\
    </C:comp-filter>\n\
  </C:filter>\n\
</C:calendar-query>",
        escape_xml(property),
        escape_xml(query)
    )
}

fn sync_collection_body(sync_token: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:sync-collection xmlns:D=\"DAV:\">\n\
  <D:sync-token>{}</D:sync-token>\n\
  <D:sync-level>1</D:sync-level>\n\
  <D:prop>\n\
    <D:getetag/>\n\
  </D:prop>\n\
</D:sync-collection>",
        escape_xml(sync_token)
    )
}

fn cursor_invalid_error(status: StatusCode, body: String) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .protocol(Protocol::CalDav)
    .operation(AccountOperation::SyncChanges)
    .scope(ErrorScope::Cursor(CursorScope::Type(
        ObjectType::CalendarEvent,
    )))
    .status(Some(status.as_u16()));
    let body = body.trim();
    if !body.is_empty() {
        builder = builder.text(DiagnosticText::support_only(body.to_string()));
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

fn should_fallback_discovery(error: &AccountError) -> bool {
    matches!(
        error.kind(),
        AccountErrorKind::NotFound(ResourceKind::Calendar)
    )
}

/// A multi-REPORT fetch: everything that came back usable, plus the
/// recovery classification of any single REPORT that failed wholly.
///
/// Multiget is chunked and text search runs one REPORT per property, so a
/// walk can meet a 401, 403, or 503 on one leg after other legs already
/// returned events. Aborting the whole call throws those events away;
/// folding the refusal into anonymous `failed_ids` throws the RECOVERY
/// signal away, and the consumer can no longer tell "reauthorize" from
/// "retry later" from "this resource is gone". So each REPORT is
/// classified where it happens, and the worst class survives to the
/// caller in `degraded`, which the account layer publishes as a
/// `Page::skipped_scopes` entry: the walk did not finish this collection.
/// One REPORT leg of a multi-leg fetch, as handed to `accumulate_leg`.
struct MultigetLeg<'a> {
    url: &'a str,
    depth: &'a str,
    body: &'a str,
    operation: AccountOperation,
    /// Names the leg in a parse-failure message ("multiget", "query").
    context: &'a str,
}

pub(crate) struct MultigetFetch {
    pub(crate) report: CalDavMultigetReport,
    pub(crate) degraded: Option<AccountError>,
}

impl MultigetFetch {
    /// A wholly-failed leg with nothing usable anywhere is still a failed
    /// call: there is no partial result to preserve, so it keeps riding the
    /// `Err` arm with its original classification.
    fn settle(
        report: CalDavMultigetReport,
        degraded: Option<AccountError>,
    ) -> Result<Self, AccountError> {
        match degraded {
            Some(error) if report.events.is_empty() => Err(error),
            degraded => Ok(Self { report, degraded }),
        }
    }
}

/// Keep whichever failure demands the more drastic recovery, so a 503 on
/// one chunk cannot hide a 401 on another.
fn multiget_failure(
    report: &crate::parse::CalDavMultigetReport,
    operation: AccountOperation,
) -> Option<AccountError> {
    match report.classify() {
        MultigetOutcome::Usable => None,
        MultigetOutcome::CompleteFailure { status } => {
            let code = status
                .and_then(|code| StatusCode::from_u16(code).ok())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            Some(status_error(
                operation,
                code,
                format!(
                    "multi-status body reported failure for all {} resources",
                    report.failed.len()
                ),
            ))
        }
    }
}

pub(crate) fn unsupported_error(operation: AccountOperation) -> AccountError {
    bifrost_dav_core::unsupported_error(operation, DAV)
}

pub(crate) fn missing_event_error(operation: AccountOperation, id: String) -> AccountError {
    bifrost_dav_core::not_found_error(operation, id, DAV)
}

pub(crate) fn local_error(operation: AccountOperation, message: impl Into<String>) -> AccountError {
    bifrost_dav_core::local_error(operation, message, DAV)
}

pub(crate) fn parse_error(operation: AccountOperation, message: impl Into<String>) -> AccountError {
    bifrost_dav_core::parse_error(operation, message, DAV)
}

/// Only the account layer's tests mint one directly now; production transport
/// failures come back already classified from `DavDispatch`.
#[cfg(test)]
pub(crate) fn transport_error(
    operation: AccountOperation,
    message: impl Into<String>,
) -> AccountError {
    bifrost_dav_core::transport_error(operation, message, DAV)
}

pub(crate) fn status_error(
    operation: AccountOperation,
    status: StatusCode,
    body: String,
) -> AccountError {
    bifrost_dav_core::status_error(operation, status, body, DAV)
}

pub(crate) fn event_scope(id: impl Into<String>) -> ErrorScope {
    ErrorScope::Calendar {
        id: (id.into()).into(),
    }
}

const PROPFIND_PRINCIPAL: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\">\n\
  <D:prop>\n\
    <D:current-user-principal/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_ACCOUNT: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:prop>\n\
    <C:calendar-home-set/>\n\
    <C:calendar-user-address-set/>\n\
    <C:schedule-outbox-URL/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_CALENDARS: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\" xmlns:A=\"http://apple.com/ns/ical/\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:displayname/>\n\
    <A:calendar-color/>\n\
    <D:current-user-privilege-set/>\n\
    <D:sync-token/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_EVENTS: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
    <D:getcontenttype/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_SYNC_TOKEN: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\">\n\
  <D:prop>\n\
    <D:sync-token/>\n\
  </D:prop>\n\
</D:propfind>";

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use bifrost_types::{AccountFuture, ProtocolErrorKind, RecoveryClass, ServerErrorKind};

    /// Every error this crate mints is stamped CalDAV, and names calendars.
    ///
    /// Twin of `bifrost-carddav`'s `every_error_this_crate_mints_is_stamped_carddav`;
    /// keep them in step. The shared ladder in `bifrost-dav-core` is
    /// parameterized by `DAV`, so a wrong binding here would silently restamp
    /// the whole crate's error surface as CardDAV.
    #[test]
    fn every_error_this_crate_mints_is_stamped_caldav() {
        let errors = [
            status_error(
                AccountOperation::EventGet,
                StatusCode::NOT_FOUND,
                String::new(),
            ),
            local_error(AccountOperation::EventGet, "bad"),
            parse_error(AccountOperation::EventGet, "bad"),
            transport_error(AccountOperation::EventGet, "bad"),
            unsupported_error(AccountOperation::EventGet),
            missing_event_error(AccountOperation::EventGet, "one.ics".to_string()),
        ];
        for error in errors {
            assert_eq!(
                error.protocol(),
                Some(Protocol::CalDav),
                "a CalDAV error must not be stamped otherwise: {error:?}"
            );
        }
        let missing = status_error(
            AccountOperation::EventGet,
            StatusCode::NOT_FOUND,
            String::new(),
        );
        assert!(
            matches!(
                missing.kind(),
                AccountErrorKind::NotFound(ResourceKind::Calendar)
            ),
            "a CalDAV 404 names a calendar, not a contact: {missing:?}"
        );
    }

    #[derive(Debug, Clone)]
    struct RequestTranscript {
        method: Method,
        url: String,
        headers: HeaderMap,
        body: String,
    }

    struct ScriptedDavTransport {
        responses: Mutex<VecDeque<DavResponse>>,
        requests: Mutex<Vec<RequestTranscript>>,
    }

    impl ScriptedDavTransport {
        fn new(responses: impl IntoIterator<Item = DavResponse>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(responses.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
            })
        }

        fn requests(&self) -> Vec<RequestTranscript> {
            self.requests
                .lock()
                .expect("scripted DAV request lock poisoned")
                .clone()
        }
    }

    impl DavTransport for ScriptedDavTransport {
        fn send(
            &self,
            request: reqwest::RequestBuilder,
        ) -> AccountFuture<Result<DavResponse, String>> {
            let request = match request.build() {
                Ok(request) => request,
                Err(error) => return Box::pin(async move { Err(error.to_string()) }),
            };
            self.requests
                .lock()
                .expect("scripted DAV request lock poisoned")
                .push(RequestTranscript {
                    method: request.method().clone(),
                    url: request.url().to_string(),
                    headers: request.headers().clone(),
                    body: request
                        .body()
                        .and_then(reqwest::Body::as_bytes)
                        .map_or_else(String::new, |body| {
                            String::from_utf8_lossy(body).into_owned()
                        }),
                });
            let mut response = self
                .responses
                .lock()
                .expect("scripted DAV response lock poisoned")
                .pop_front()
                .expect("scripted DAV transport exhausted");
            // A scripted response with no effective URL models the ordinary
            // no-redirect case: reqwest reports the submitted URI back. A
            // script that sets one models a followed redirect.
            if response.url.is_empty() {
                response.url = request.url().to_string();
            }
            Box::pin(async move { Ok(response) })
        }
    }

    /// A recurrence-instance `EventId` is refused before anything is sent.
    ///
    /// `events_from_ical` mints `"{uri}#{recurrence_id}"` for override VEVENTs,
    /// and a URL fragment never goes on the wire - so unguarded, every one of
    /// these ids addressed the master resource. `event_delete` was the severe
    /// case: deleting one occurrence DELETEd the whole `.ics` and destroyed the
    /// entire series.
    ///
    /// The script is deliberately EMPTY. Any request at all starves it and
    /// panics, so this fails loudly if a guard is removed rather than quietly
    /// asserting on an error some other layer produced. All four write and read
    /// doors are covered, because all four resolved the same wrong URL.
    #[tokio::test]
    async fn recurrence_instance_ids_are_refused_before_reaching_the_wire() {
        use bifrost_types::account::Account as _;

        let script = ScriptedDavTransport::new([]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = Arc::new(CalDavClient::with_transport(
            "https://dav.example.test",
            transport,
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/calendar/");
        let instance = bifrost_types::EventId(
            "https://dav.example.test/calendar/series.ics#20260609T120000Z".to_string(),
        );

        account
            .event_delete(instance.clone())
            .await
            .expect_err("deleting one occurrence must not delete the series");
        account
            .event_get(instance.clone())
            .await
            .expect_err("reading one occurrence must not silently return the master");
        account
            .event_update(instance.clone(), bifrost_types::EventPatch::default())
            .await
            .expect_err("editing one occurrence must not rewrite the series");
        account
            .event_rsvp(instance, bifrost_types::RsvpStatus::Accepted)
            .await
            .expect_err("answering for one occurrence must not answer for the series");

        assert!(
            script.requests().is_empty(),
            "a refused instance id must reach no transport at all"
        );
    }

    /// A discovery that enumerates no calendars leaves the OPENED account with
    /// no default, rather than the calendar home standing in for one.
    ///
    /// Drives the real discovery-to-account path, so it bites where a pure test
    /// of the selection helper cannot: reintroducing the old
    /// `unwrap_or_else(|| resolve_url(&home))` at the `open` call site is
    /// invisible to a test that constructs the `None` itself.
    #[tokio::test]
    async fn an_empty_discovery_opens_an_account_with_no_default_calendar() {
        let script = discovery_script("/cal/ada/");
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        let account = crate::account::CalDavAccount::open_with_client(client, None)
            .await
            .expect("discovery succeeds against an empty backend");

        assert_eq!(
            account.default_calendar_url, None,
            "an empty calendar home must leave no default, not the home itself"
        );
        assert!(
            account.calendar_urls.is_empty(),
            "an empty calendar home advertises no collections"
        );
    }

    /// An empty backend refuses a collection-less call locally instead of
    /// addressing the calendar home.
    ///
    /// The home is not a collection when the walk came back empty
    /// (`list_calendars` returns the home itself when it genuinely is one), so
    /// the old `unwrap_or_else(|| resolve_url(&home))` default sent every one
    /// of these to a resource a spec-correct server 404s - reporting a local
    /// routing failure as a remote `NotFound`. The empty script is the bite:
    /// restore the fallback and these calls reach the transport and panic on
    /// exhaustion rather than failing quietly.
    #[tokio::test]
    async fn an_empty_backend_refuses_collection_less_calls_before_the_wire() {
        use bifrost_types::account::Account as _;

        let script = ScriptedDavTransport::new([]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = Arc::new(CalDavClient::with_transport(
            "https://dav.example.test",
            transport,
        ));
        let account = crate::account::CalDavAccount::for_tests_without_collections(
            client,
            "https://dav.example.test/cal/",
        );

        account
            .event_search(bifrost_types::EventSearchRequest::new("standup"))
            .await
            .expect_err("a search naming no calendar has nothing to search");
        // Only the doors taking an `Option<CalendarId>` route through the
        // default. `event_get` / `event_update` / `event_rsvp` derive the
        // collection from the resource's own URL, and
        // `bifrost_net::url::parent_collection_url` answers for every id that
        // resolves absolute, so their fallback is unreachable in practice.

        let mut scopes = account.discover_cursor_scopes();
        let mut discovered = Vec::new();
        while let Some(event) = futures::StreamExt::next(&mut scopes).await {
            if let bifrost_types::SyncEvent::Batch(batch) = event {
                discovered.extend(batch.items);
            }
        }
        assert!(
            discovered.is_empty(),
            "an empty backend advertises no cursor scope, so none can be established"
        );

        assert!(
            script.requests().is_empty(),
            "an unroutable call must reach no transport at all"
        );
    }

    /// Every discovered calendar gets an independent cursor scope.
    #[tokio::test]
    async fn every_calendar_is_discovered_as_a_cursor_scope() {
        use bifrost_types::account::Account as _;
        use futures::StreamExt as _;
        let script = ScriptedDavTransport::new([]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = Arc::new(CalDavClient::with_transport(
            "https://dav.example.test",
            transport,
        ));

        let many = crate::account::CalDavAccount::for_tests_with_collections(
            client,
            "https://dav.example.test/cal/work/",
            vec![
                "https://dav.example.test/cal/personal/".to_string(),
                "https://dav.example.test/cal/holidays/".to_string(),
            ],
        );
        let mut stream = many.discover_cursor_scopes();
        let bifrost_types::SyncEvent::Batch(batch) = stream.next().await.expect("scope batch")
        else {
            panic!("expected scope batch");
        };
        assert_eq!(batch.items.len(), 3);
        assert!(
            batch
                .items
                .iter()
                .all(|scope| matches!(scope, CursorScope::Folder(_)))
        );
    }

    #[tokio::test]
    async fn unavailable_rsvp_is_rejected_before_reaching_the_wire() {
        use bifrost_types::account::Account as _;

        let script = ScriptedDavTransport::new([]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = Arc::new(CalDavClient::with_transport(
            "https://dav.example.test",
            transport,
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/work/");

        account
            .event_rsvp(
                bifrost_types::EventId("https://dav.example.test/cal/work/one.ics".to_string()),
                bifrost_types::RsvpStatus::Accepted,
            )
            .await
            .expect_err("scheduling-less accounts cannot RSVP");
        assert!(script.requests().is_empty());
    }

    /// An empty calendar home lists NOTHING - no fabricated placeholder.
    ///
    /// This crate removed its phantom home-calendar long ago but never pinned
    /// the absence, which is exactly how bifrost-carddav kept an identical
    /// phantom for as long as it did. The twin is
    /// `an_empty_home_lists_no_address_books_rather_than_a_phantom`; keep the
    /// pair in step.
    #[tokio::test]
    async fn an_empty_home_lists_no_calendars_rather_than_a_phantom() {
        use bifrost_types::account::Account as _;

        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"/>".to_string(),
            url: String::new(),
        }]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = Arc::new(CalDavClient::with_transport(
            "https://dav.example.test",
            transport,
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/ada/");

        let calendars = account.calendars_list().await.expect("list");
        assert!(
            calendars.is_empty(),
            "an empty home must not fabricate a calendar: {calendars:?}"
        );
    }

    /// A server without MOVE falls back to copy-then-delete, and a failure of
    /// the delete leg reports the move as HALF applied.
    ///
    /// The order is the recoverable one: a failed copy leaves the event exactly
    /// where it was, while a failed delete leaves it readable in two places.
    /// The second is the one that needs the `Protocol(PartialResponse)` +
    /// acknowledged `Attempt` verdict, because replaying the whole update
    /// against a source that may already be gone is not the consumer's best
    /// move - reconciling is.
    #[tokio::test]
    async fn a_move_without_server_move_support_copies_then_deletes() {
        use bifrost_types::account::Account as _;

        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:One\r\n\
                   DTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\n\
                   END:VEVENT\r\nEND:VCALENDAR\r\n";
        let response = |status: StatusCode, body: &str| DavResponse {
            status,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let script = ScriptedDavTransport::new([
            response(StatusCode::OK, ics),
            // MOVE unimplemented.
            response(StatusCode::METHOD_NOT_ALLOWED, ""),
            // PUT to the destination succeeds.
            response(StatusCode::CREATED, ""),
            // DELETE of the original fails.
            response(StatusCode::INTERNAL_SERVER_ERROR, "boom"),
        ]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = Arc::new(CalDavClient::with_transport(
            "https://dav.example.test",
            transport,
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/work/");

        let error = account
            .event_update(
                bifrost_types::EventId("https://dav.example.test/cal/work/one.ics".to_string()),
                bifrost_types::EventPatch {
                    calendar_id: Some(bifrost_types::CalendarId(
                        "https://dav.example.test/cal/personal/".to_string(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a failed cleanup is not a success");

        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
            ),
            "a copied-but-not-removed event is a partial response: {error:?}"
        );
        let methods = script
            .requests()
            .into_iter()
            .map(|request| request.method.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(methods, vec!["GET", "MOVE", "PUT", "DELETE"]);
    }

    /// A cross-calendar `event_update` MOVES the resource, and a restated
    /// calendar still updates in place.
    ///
    /// History, because the assertion has now been inverted twice: this shape
    /// originally returned `Ok(())` having moved nothing, was then refused
    /// outright as better than a silent drop, and is now performed. The
    /// move-only patch is deliberately ONE request - no content changed, so the
    /// event must not be charged a second write or the partial-failure verdict
    /// that would come with it.
    ///
    /// The second half matters as much as the first: a patch that RESTATES the
    /// event's current calendar is not a move and must still take the ordinary
    /// GET-plus-PUT path. A guard that treated any `calendar_id` as a
    /// relocation would pass the first assertion and break every ordinary
    /// update.
    #[tokio::test]
    async fn event_update_moves_across_calendars_and_updates_in_place_otherwise() {
        use bifrost_types::account::Account as _;

        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:One\r\n\
                   DTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\n\
                   END:VEVENT\r\nEND:VCALENDAR\r\n";
        let event =
            || bifrost_types::EventId("https://dav.example.test/cal/work/one.ics".to_string());
        let patch_to = |calendar: &str| bifrost_types::EventPatch {
            calendar_id: Some(bifrost_types::CalendarId(calendar.to_string())),
            ..Default::default()
        };

        // A move: GET the current resource, then MOVE it. The destination keeps
        // the resource's own file name.
        let script = ScriptedDavTransport::new([
            DavResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: ics.to_string(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::CREATED,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            },
        ]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = Arc::new(CalDavClient::with_transport(
            "https://dav.example.test",
            transport,
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/work/");
        account
            .event_update(event(), patch_to("https://dav.example.test/cal/personal/"))
            .await
            .expect("a move between calendars is performed");
        let requests = script.requests();
        assert_eq!(
            requests.len(),
            2,
            "a move-only patch is a GET and a MOVE, with no content write"
        );
        assert_eq!(requests[1].method.as_str(), "MOVE");
        assert_eq!(
            requests[1].url, "https://dav.example.test/cal/work/one.ics",
            "MOVE addresses the source resource"
        );
        assert_eq!(
            requests[1]
                .headers
                .get("Destination")
                .and_then(|value| value.to_str().ok()),
            Some("https://dav.example.test/cal/personal/one.ics"),
            "the destination keeps the resource's own file name"
        );
        assert_eq!(
            requests[1]
                .headers
                .get("Overwrite")
                .and_then(|value| value.to_str().ok()),
            Some("F"),
            "a name collision at the destination must refuse, not overwrite"
        );

        // Restating the event's own calendar is not a move, and still updates.
        let script = ScriptedDavTransport::new([
            DavResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: ics.to_string(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::NO_CONTENT,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            },
        ]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = Arc::new(CalDavClient::with_transport(
            "https://dav.example.test",
            transport,
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/work/");
        account
            .event_update(event(), patch_to("https://dav.example.test/cal/work/"))
            .await
            .expect("restating the current calendar is not a move");
        assert_eq!(
            script.requests().len(),
            2,
            "an ordinary update is still a GET plus a PUT"
        );
    }

    #[tokio::test]
    async fn credentials_never_reach_a_resource_href_origin() {
        // Two canned responses, so a neutered guard reaches the transport and
        // fails on the destination assertion below rather than on a starved
        // script - the failure has to name the credential leak.
        let event = || DavResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:one\nEND:VEVENT\nEND:VCALENDAR".to_string(),
            url: String::new(),
        };
        let script = ScriptedDavTransport::new([event(), event()]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        client
            .get_event(
                "https://dav.example.test/calendar/one.ics",
                AccountOperation::EventGet,
            )
            .await
            .expect("trusted request succeeds");
        client
            .get_event("https://evil.test/stolen.ics", AccountOperation::EventGet)
            .await
            .expect_err("foreign resource origin is rejected");

        let requests = script.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://dav.example.test/calendar/one.ics");
        assert_eq!(
            requests[0]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }

    fn discovery_script(home_href: &str) -> Arc<ScriptedDavTransport> {
        let response = |body: String| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body,
            url: String::new(),
        };
        ScriptedDavTransport::new([
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>"
                    .to_string(),
            ),
            response(format!(
                "<C:calendar-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:href>{home_href}</D:href></C:calendar-home-set>"
            )),
            response("<D:multistatus xmlns:D=\"DAV:\"/>".to_string()),
        ])
    }

    #[tokio::test]
    async fn discovery_falls_back_to_base_after_empty_well_known_response() {
        let response = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let script = ScriptedDavTransport::new([
            response("<D:multistatus xmlns:D=\"DAV:\"/>"),
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>",
            ),
            response(
                "<C:calendar-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:href>/cal/ada/</D:href></C:calendar-home-set>",
            ),
        ]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        let discovery = client.discover_account().await.expect("fallback succeeds");

        assert_eq!(discovery.calendar_home, "https://dav.example.test/cal/ada/");
        let urls = script
            .requests()
            .into_iter()
            .map(|request| request.url)
            .collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec![
                "https://dav.example.test/.well-known/caldav".to_string(),
                "https://dav.example.test/".to_string(),
                "https://dav.example.test/principals/ada/".to_string(),
            ]
        );
    }

    /// RFC 6764 puts well-known discovery at the origin root. A base URL
    /// carrying a path is the only input that distinguishes an
    /// origin-rooted construction from suffix concatenation, and getting
    /// it wrong is not merely a wasted request: only the crate's
    /// not-found classification falls back, so a deployment answering
    /// the bogus path with 401/403 would fail the open outright.
    #[tokio::test]
    async fn well_known_probe_is_origin_rooted_for_a_path_bearing_base() {
        let response = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let script = ScriptedDavTransport::new([
            response("<D:multistatus xmlns:D=\"DAV:\"/>"),
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>",
            ),
            response(
                "<C:calendar-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:href>/cal/ada/</D:href></C:calendar-home-set>",
            ),
        ]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test/service", transport);

        let discovery = client.discover_account().await.expect("fallback succeeds");

        assert_eq!(discovery.calendar_home, "https://dav.example.test/cal/ada/");
        let urls = script
            .requests()
            .into_iter()
            .map(|request| request.url)
            .collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec![
                "https://dav.example.test/.well-known/caldav".to_string(),
                "https://dav.example.test/service".to_string(),
                "https://dav.example.test/principals/ada/".to_string(),
            ],
            "the well-known probe is rooted at the origin, and only the fallback uses the configured path"
        );
    }

    #[tokio::test]
    async fn principal_404_does_not_restart_discovery_at_base() {
        let script = ScriptedDavTransport::new([
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>".to_string(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::NOT_FOUND,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            },
        ]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        client
            .discover_account()
            .await
            .expect_err("principal failure is terminal");

        assert_eq!(script.requests().len(), 2);
    }

    /// The legitimate deployment the origin allowlist must not break: the
    /// calendar home lives on a different host than the principal. It is
    /// discovered over HTTPS, so it is credential-bearing.
    #[tokio::test]
    async fn discovered_cross_origin_https_home_receives_credentials() {
        let script = discovery_script("https://cal.example.test/homes/ada/");
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let mut client = CalDavClient::with_transport("https://dav.example.test", transport);

        let home = client
            .discover_account()
            .await
            .expect("cross-origin home is discovered")
            .calendar_home;
        assert_eq!(home, "https://cal.example.test/homes/ada/");
        client.admit_discovered_urls(std::iter::once(home.clone()));
        client
            .list_calendars(&home)
            .await
            .expect("cross-origin home is credential-bearing");

        let requests = script.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].url, "https://cal.example.test/homes/ada/");
        assert_eq!(
            requests[2]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }

    /// The reviewer-found hole this pins: a cross-origin hop followed inside
    /// reqwest arrives with `Authorization` stripped and cannot get it back.
    /// The manual re-dispatch must therefore carry fresh credentials to the
    /// admitted target - the assertion that bites is the auth header on the
    /// SECOND transcript entry, and the href assertion additionally keeps
    /// round 3's guarantee that resolution uses the post-redirect URI.
    #[tokio::test]
    async fn cross_origin_redirect_is_redispatched_with_credentials() {
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(
            reqwest::header::LOCATION,
            HeaderValue::from_static("https://cal.example.test/dav/homes/ada/"),
        );
        let script = ScriptedDavTransport::new([
            DavResponse {
                status: StatusCode::MOVED_PERMANENTLY,
                headers: redirect_headers,
                body: String::new(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:response><D:href>team/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:calendar/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
                url: String::new(),
            },
        ]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let mut client = CalDavClient::with_transport("https://dav.example.test", transport);
        client.admit_discovered_urls(std::iter::once(
            "https://cal.example.test/homes/ada/".to_string(),
        ));

        let calendars = client
            .list_calendars("https://dav.example.test/calendars/ada/")
            .await
            .expect("cross-origin redirect is followed with credentials");

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].url, "https://cal.example.test/dav/homes/ada/");
        assert_eq!(
            requests[1].method,
            Method::from_bytes(b"PROPFIND").expect("PROPFIND method")
        );
        assert_eq!(
            requests[1]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
        assert_eq!(
            calendars[0].href,
            "https://cal.example.test/dav/homes/ada/team/"
        );
    }

    /// A redirect is server-controlled input: an origin discovery never
    /// admitted must fail locally, and no request at all may reach it.
    #[tokio::test]
    async fn redirect_to_an_unadmitted_origin_is_refused_locally() {
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(
            reqwest::header::LOCATION,
            HeaderValue::from_static("https://evil.test/dav/"),
        );
        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::FOUND,
            headers: redirect_headers,
            body: String::new(),
            url: String::new(),
        }]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        client
            .list_calendars("https://dav.example.test/calendars/ada/")
            .await
            .expect_err("an unadmitted redirect target is refused");

        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test]
    async fn manual_redirect_walk_allows_the_configured_hop_count() {
        let max_hops = usize::from(bifrost_net::RedirectPolicy::default().max_hops);
        let mut responses = Vec::new();
        for _ in 0..max_hops {
            let mut headers = HeaderMap::new();
            headers.insert(reqwest::header::LOCATION, HeaderValue::from_static("/next"));
            responses.push(DavResponse {
                status: StatusCode::TEMPORARY_REDIRECT,
                headers,
                body: String::new(),
                url: String::new(),
            });
        }
        responses.push(DavResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: "ok".to_string(),
            url: String::new(),
        });
        let script = ScriptedDavTransport::new(responses);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        client
            .get_event("https://dav.example.test/start", AccountOperation::EventGet)
            .await
            .expect("the configured number of redirects is allowed");

        assert_eq!(script.requests().len(), max_hops + 1);
    }

    /// RFC 4918 resolves a relative href against the EFFECTIVE request URI.
    /// `dav_redirect_policy` follows same-host hops, so a PROPFIND submitted
    /// to `/calendar` can be served from `/dav/users/ada/calendar/`. Resolving
    /// `one.ics` against the submitted URI mints `/one.ics` - a native id that
    /// does not exist, and a follow-up GET that 404s.
    #[tokio::test]
    async fn event_hrefs_resolve_against_the_post_redirect_url() {
        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>one.ics</D:href><D:propstat><D:prop><D:getetag>\"e1\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/dav/users/ada/calendar/".to_string(),
        }]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        let listing = client
            .list_events_listing(
                "https://dav.example.test/calendar",
                AccountOperation::EventsInRange,
            )
            .await
            .expect("redirected listing succeeds");

        assert_eq!(
            listing.entries[0].uri,
            "https://dav.example.test/dav/users/ada/calendar/one.ics"
        );
    }

    /// Discovery is server-steered, so a discovered home must never weaken the
    /// transport guarantee the configured HTTPS base URL established. The
    /// assertion that matters is the destination: no request at all reaches the
    /// plaintext origin, credential-bearing or otherwise.
    #[tokio::test]
    async fn discovered_plaintext_home_never_receives_credentials() {
        let script = discovery_script("http://cal.example.test/homes/ada/");
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let mut client = CalDavClient::with_transport("https://dav.example.test", transport);

        let home = client
            .discover_account()
            .await
            .expect("home href is still reported")
            .calendar_home;
        assert_eq!(home, "http://cal.example.test/homes/ada/");
        client.admit_discovered_urls(std::iter::once(home.clone()));
        client
            .list_calendars(&home)
            .await
            .expect_err("a downgraded discovered origin is refused");

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.url.starts_with("https://dav.example.test/")),
            "no request reached the plaintext origin: {:?}",
            requests.iter().map(|r| &r.url).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn account_discovery_reads_all_principal_properties_in_two_requests() {
        let response = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let script = ScriptedDavTransport::new([
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>",
            ),
            response(
                "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><C:calendar-home-set><D:href>/cal/</D:href></C:calendar-home-set><C:calendar-user-address-set><D:href>mailto:ada@example.test</D:href></C:calendar-user-address-set><C:schedule-outbox-URL><D:href>/outbox/</D:href></C:schedule-outbox-URL></D:multistatus>",
            ),
        ]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        let discovery = client.discover_account().await.expect("discovery succeeds");

        assert_eq!(discovery.calendar_home, "https://dav.example.test/cal/");
        assert_eq!(
            discovery.calendar_user_email.as_deref(),
            Some("ada@example.test")
        );
        assert_eq!(
            discovery.schedule_outbox_url.as_deref(),
            Some("https://dav.example.test/outbox/")
        );
        assert_eq!(script.requests().len(), 2);
    }

    #[tokio::test]
    async fn scheduling_discovery_failure_is_not_downgraded_to_no_capability() {
        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            headers: HeaderMap::new(),
            body: "try later".to_string(),
            url: String::new(),
        }]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        let error = client
            .discover_account()
            .await
            .expect_err("a transient discovery failure must fail the open, not be swallowed");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Server(ServerErrorKind::Unavailable)
        );
    }

    #[tokio::test]
    async fn sync_events_uses_depth_zero_report_transcript() {
        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body:
                "<D:multistatus xmlns:D=\"DAV:\"><D:sync-token>next</D:sync-token><D:response><D:href>/calendar/one.ics</D:href><D:status>HTTP/1.1 200 OK</D:status></D:response></D:multistatus>"
                    .to_string(),
            url: String::new(),
        }]);
        let concrete_transport = Arc::clone(&script);
        let transport: Arc<dyn DavTransport> = concrete_transport;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        let report = client
            .sync_events("https://dav.example.test/calendar/", "previous")
            .await
            .expect("scripted sync succeeds");

        assert_eq!(report.sync_token.as_deref(), Some("next"));
        assert_eq!(
            report.entries[0].uri,
            "https://dav.example.test/calendar/one.ics"
        );
        let requests = script.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].method,
            Method::from_bytes(b"REPORT").expect("REPORT method")
        );
        assert_eq!(requests[0].url, "https://dav.example.test/calendar/");
        assert_eq!(
            requests[0]
                .headers
                .get("depth")
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
        assert_eq!(
            requests[0]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }

    /// A 401 on an ordinary REPORT must classify as reauthorization. Without
    /// the status check in `report_raw` the empty/HTML error body either parses
    /// as an authoritative empty multiget or degrades into a parse failure, and
    /// a consumer treats "no events" as truth.
    #[tokio::test]
    async fn unauthorized_report_classifies_as_reauthorization() {
        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::UNAUTHORIZED,
            headers: HeaderMap::new(),
            body: "<html><body>401 Unauthorized</body></html>".to_string(),
            url: String::new(),
        }]);
        let concrete_transport = Arc::clone(&script);
        let transport: Arc<dyn DavTransport> = concrete_transport;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        let error = client
            .query_events_in_range(
                "https://dav.example.test/calendar/",
                Some("20260101T000000Z"),
                Some("20260201T000000Z"),
            )
            .await
            .expect_err("401 REPORT must not be reported as an empty result");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Authentication(bifrost_types::AuthErrorKind::ReauthorizationRequired)
        ));
        assert_eq!(*error.recovery(), RecoveryClass::AuthLost);
    }

    #[test]
    fn resolve_url_fallback_preserves_separator() {
        let client = CalDavClient::for_base_url("not a url");

        assert_eq!(
            client.resolve_url("calendar/one.ics"),
            "not a url/calendar/one.ics"
        );
        assert_eq!(
            client.resolve_url("/calendar/one.ics"),
            "not a url/calendar/one.ics"
        );
    }

    #[test]
    fn discovery_fallback_only_allows_not_found() {
        let unauthorized = status_error(
            AccountOperation::Discover,
            StatusCode::UNAUTHORIZED,
            String::new(),
        );
        assert!(!should_fallback_discovery(&unauthorized));

        let not_found = status_error(
            AccountOperation::Discover,
            StatusCode::NOT_FOUND,
            String::new(),
        );
        assert!(should_fallback_discovery(&not_found));
    }

    #[test]
    fn calendar_query_body_uses_caldav_time_range() {
        let body = calendar_query_body(Some("20260602T000000Z"), Some("20260603T000000Z"));

        assert!(body.contains("<C:calendar-query"));
        assert!(body.contains("<D:resourcetype/>"));
        assert!(body.contains("<C:calendar-data/>"));
        assert!(
            body.contains("<C:time-range start=\"20260602T000000Z\" end=\"20260603T000000Z\"/>")
        );
    }

    #[test]
    fn calendar_query_body_escapes_time_range_attributes() {
        let body = calendar_query_body(Some("20260602T000000Z"), Some("bad\"&value"));

        assert!(body.contains("end=\"bad&quot;&amp;value\""));
    }

    #[test]
    fn calendar_query_body_preserves_one_sided_ranges() {
        let start_only = calendar_query_body(Some("20260602T000000Z"), None);
        let end_only = calendar_query_body(None, Some("20260603T000000Z"));

        assert!(start_only.contains("<C:time-range start=\"20260602T000000Z\"/>"));
        assert!(end_only.contains("<C:time-range end=\"20260603T000000Z\"/>"));
    }

    #[test]
    fn calendar_text_query_body_uses_property_text_match() {
        let body = calendar_text_query_body("SUMMARY", "plan & meet");

        assert!(body.contains("<C:prop-filter name=\"SUMMARY\">"));
        assert!(body.contains("<D:resourcetype/>"));
        assert!(body.contains("<C:text-match collation=\"i;unicode-casemap\">"));
        assert!(body.contains("plan &amp; meet"));
    }

    #[tokio::test]
    async fn calendar_multiget_uses_depth_zero() {
        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"/>".to_string(),
            url: String::new(),
        }]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        client
            .fetch_events(
                "https://dav.example.test/cal/",
                &["https://dav.example.test/cal/opaque-id".to_string()],
                AccountOperation::EventSearch,
            )
            .await
            .expect("empty multistatus is usable");

        let requests = script.requests();
        assert_eq!(requests[0].headers["Depth"], "0");
        assert!(requests[0].body.contains("<D:resourcetype/>"));
    }

    /// A transport that records the HIGH-WATER MARK of simultaneously
    /// in-flight requests.
    ///
    /// Each send yields once before answering, so every leg the caller has
    /// polled is genuinely in flight at the same time and the mark reflects
    /// the caller's fan-out policy rather than scheduling luck.
    struct ConcurrencyProbeTransport {
        body: String,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        peak: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl DavTransport for ConcurrencyProbeTransport {
        fn send(
            &self,
            _request: reqwest::RequestBuilder,
        ) -> AccountFuture<Result<DavResponse, String>> {
            let body = self.body.clone();
            let in_flight = Arc::clone(&self.in_flight);
            let peak = Arc::clone(&self.peak);
            Box::pin(async move {
                use std::sync::atomic::Ordering;
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(DavResponse {
                    status: StatusCode::MULTI_STATUS,
                    headers: HeaderMap::new(),
                    body,
                    url: "https://dav.example.test/cal/".to_string(),
                })
            })
        }
    }

    /// Multiget fan-out is bounded by `MULTIGET_LEG_CONCURRENCY`, not by the
    /// caller's uri list.
    ///
    /// The chunk count is input-sized, so an unbounded dispatch lets one large
    /// calendar open hundreds of simultaneous REPORTs against a server that
    /// never agreed to that, and `bifrost-net` has no concurrency governor to
    /// catch it downstream.
    #[tokio::test]
    async fn multiget_never_holds_more_legs_open_than_the_concurrency_bound() {
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let transport: Arc<dyn DavTransport> = Arc::new(ConcurrencyProbeTransport {
            body: "<D:multistatus xmlns:D=\"DAV:\"></D:multistatus>".to_string(),
            in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peak: Arc::clone(&peak),
        });
        let client = CalDavClient::with_transport("https://dav.example.test", transport);
        // Ten chunks: comfortably more than the bound, so an unbounded
        // dispatch is distinguishable from a bounded one.
        let uris = (0..MULTIGET_BATCH_SIZE * 10)
            .map(|index| format!("https://dav.example.test/cal/{index}.ics"))
            .collect::<Vec<_>>();

        client
            .fetch_events(
                "https://dav.example.test/cal/",
                &uris,
                AccountOperation::EventSearch,
            )
            .await
            .expect("empty multistatus legs are usable");

        assert_eq!(
            peak.load(std::sync::atomic::Ordering::SeqCst),
            MULTIGET_LEG_CONCURRENCY
        );
    }

    #[tokio::test]
    async fn chunked_multiget_keeps_prior_chunk_when_later_http_leg_fails() {
        let good = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:response><D:href>/cal/one.ics</D:href><D:propstat><D:prop><C:calendar-data>BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:one\nEND:VEVENT\nEND:VCALENDAR</C:calendar-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/cal/".to_string(),
        };
        let refused = DavResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            headers: HeaderMap::new(),
            body: String::new(),
            url: "https://dav.example.test/cal/".to_string(),
        };
        let script = ScriptedDavTransport::new([good, refused]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);
        let uris = (0..=MULTIGET_BATCH_SIZE)
            .map(|index| format!("https://dav.example.test/cal/{index}.ics"))
            .collect::<Vec<_>>();

        let fetched = client
            .fetch_events(
                "https://dav.example.test/cal/",
                &uris,
                AccountOperation::EventSearch,
            )
            .await
            .expect("the usable first chunk survives");

        assert_eq!(fetched.report.events.len(), 1);
        assert!(fetched.degraded.is_some());
    }

    /// The adjacent leg-failure path: a later chunk whose body will not parse
    /// must degrade like a later chunk that returned 503, not discard the
    /// chunks that already materialized. A malformed body is account-authored
    /// data, so it is classified and survived rather than asserted on.
    #[tokio::test]
    async fn chunked_multiget_keeps_prior_chunk_when_later_body_is_malformed() {
        let good = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:response><D:href>/cal/one.ics</D:href><D:propstat><D:prop><C:calendar-data>BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:one\nEND:VEVENT\nEND:VCALENDAR</C:calendar-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/cal/".to_string(),
        };
        let malformed = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"><D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/cal/".to_string(),
        };
        let script = ScriptedDavTransport::new([good, malformed]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);
        let uris = (0..=MULTIGET_BATCH_SIZE)
            .map(|index| format!("https://dav.example.test/cal/{index}.ics"))
            .collect::<Vec<_>>();

        let fetched = client
            .fetch_events(
                "https://dav.example.test/cal/",
                &uris,
                AccountOperation::EventSearch,
            )
            .await
            .expect("the usable first chunk survives a malformed later chunk");

        assert_eq!(fetched.report.events.len(), 1);
        assert!(fetched.degraded.is_some());
    }

    /// Nothing usable anywhere is still a failed call, malformed or not.
    #[tokio::test]
    async fn an_only_leg_that_will_not_parse_is_still_an_error() {
        let malformed = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"><D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/cal/".to_string(),
        };
        let script = ScriptedDavTransport::new([malformed]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CalDavClient::with_transport("https://dav.example.test", transport);

        let outcome = client
            .fetch_events(
                "https://dav.example.test/cal/",
                &["https://dav.example.test/cal/one.ics".to_string()],
                AccountOperation::EventSearch,
            )
            .await
            .map(|fetched| fetched.report.events.len());
        let Err(error) = outcome else {
            panic!("no usable result anywhere must stay an error");
        };
        assert_eq!(error.operation(), Some(AccountOperation::EventSearch));
    }

    #[test]
    fn schedule_outbox_propfind_requests_caldav_outbox_url() {
        assert!(PROPFIND_ACCOUNT.contains("<C:schedule-outbox-URL/>"));
    }

    #[test]
    fn sync_collection_body_posts_sync_token_and_getetag() {
        let body = sync_collection_body("token&1");

        assert!(body.contains("<D:sync-collection"));
        assert!(body.contains("<D:sync-token>token&amp;1</D:sync-token>"));
        assert!(body.contains("<D:sync-level>1</D:sync-level>"));
        assert!(body.contains("<D:getetag/>"));
    }

    #[test]
    fn mailto_email_extracts_case_insensitive_mailto_address() {
        assert_eq!(
            mailto_email("MAILTO:Ada@Example.Test").as_deref(),
            Some("ada@example.test")
        );
        assert_eq!(
            mailto_email("Mailto:Ada@Example.Test").as_deref(),
            Some("ada@example.test")
        );
        assert_eq!(mailto_email("/principals/ada"), None);
    }

    #[test]
    fn stale_sync_token_maps_to_scoped_cursor_invalid() {
        let error =
            cursor_invalid_error(StatusCode::FORBIDDEN, "<D:valid-sync-token/>".to_string());

        assert_eq!(
            error.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        );
        assert_eq!(
            error.scope(),
            Some(&ErrorScope::Cursor(CursorScope::Type(
                ObjectType::CalendarEvent
            )))
        );
    }

    #[test]
    fn schedule_address_wraps_bare_email_as_mailto_uri() {
        assert_eq!(
            schedule_address("ada@example.test"),
            "mailto:ada@example.test"
        );
        assert_eq!(
            schedule_address("mailto:ada@example.test"),
            "mailto:ada@example.test"
        );
        assert_eq!(schedule_address("urn:uuid:ada"), "urn:uuid:ada");
    }

    #[test]
    fn accumulated_multiget_success_keeps_a_later_refusal_per_resource() {
        let report = crate::parse::CalDavMultigetReport {
            events: vec![crate::parse::CalDavFetchedEvent {
                uri: "/cal/one.ics".to_string(),
                etag: None,
                data: "BEGIN:VCALENDAR\nEND:VCALENDAR".to_string(),
            }],
            failed: vec![crate::parse::CalDavFailedResource {
                href: "/cal/two.ics".to_string(),
                status: Some(401),
            }],
            missing_data: Vec::new(),
        };

        assert!(multiget_failure(&report, AccountOperation::EventsInRange).is_none());
    }

    fn usable_report() -> CalDavMultigetReport {
        CalDavMultigetReport {
            events: vec![crate::parse::CalDavFetchedEvent {
                uri: "/cal/one.ics".to_string(),
                etag: None,
                data: "BEGIN:VCALENDAR\nEND:VCALENDAR".to_string(),
            }],
            failed: Vec::new(),
            missing_data: Vec::new(),
        }
    }

    #[test]
    fn a_refused_leg_after_a_usable_one_keeps_the_events_and_the_recovery_class() {
        let refusal = status_error(
            AccountOperation::EventSearch,
            StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );

        let fetch = MultigetFetch::settle(usable_report(), Some(refusal))
            .expect("a partial result is not a failed call");

        assert_eq!(fetch.report.events.len(), 1);
        assert_eq!(
            fetch.degraded.expect("the refusal survives").recovery(),
            &RecoveryClass::AuthLost
        );
    }

    #[test]
    fn a_refused_leg_with_nothing_usable_stays_an_error() {
        let refusal = status_error(
            AccountOperation::EventSearch,
            StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );

        let error = MultigetFetch::settle(CalDavMultigetReport::default(), Some(refusal))
            .err()
            .expect("nothing usable came back");

        assert_eq!(error.recovery(), &RecoveryClass::AuthLost);
    }
}
