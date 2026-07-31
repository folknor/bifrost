use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFuture, AccountOperation, Cause,
    CursorScope, DiagnosticText, ErrorScope, ObjectType, Protocol, ProtocolErrorKind,
    RecoveryClass, RequestCause, RequestErrorKind, ResourceKind, ServerCause, ServerErrorKind,
    StateCause, SyncStateErrorKind, TransmissionState, TransportCause, TransportErrorKind,
    TransportKind, WireCause,
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Method, StatusCode, Url};

use crate::parse::{
    CalDavFetchedEvent, CalDavMultigetReport, CalDavSyncReport, CalendarCollection,
    MultigetOutcome, extract_href_properties, extract_href_property, parse_calendar_collections,
    parse_multiget_report, parse_propfind_events, parse_sync_collection_report, resolve_href,
};
use crate::{CalDavConfig, CalDavCredentials};

const DAV_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
const MULTIGET_BATCH_SIZE: usize = 50;

#[derive(Debug, Clone, Copy)]
pub(crate) enum PutCondition<'a> {
    IfNoneMatch,
    IfMatch(&'a str),
    None,
}

#[derive(Clone)]
pub(crate) struct CalDavClient {
    http: reqwest::Client,
    transport: Arc<dyn DavTransport>,
    base_url: String,
    credentials: CalDavCredentials,
}

impl fmt::Debug for CalDavClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CalDavClient")
            .field("base_url", &self.base_url)
            .field("credentials", &self.credentials)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
struct DavResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: String,
}

/// Local DAV transport boundary. `bifrost-net`'s dispatcher is intentionally
/// crate-private, while DAV keeps Basic auth and its own redirect policy, so
/// the seam belongs here until these clients move onto `AccountNet`.
trait DavTransport: Send + Sync {
    fn send(&self, request: reqwest::RequestBuilder) -> AccountFuture<Result<DavResponse, String>>;
}

struct ReqwestDavTransport;

impl DavTransport for ReqwestDavTransport {
    fn send(&self, request: reqwest::RequestBuilder) -> AccountFuture<Result<DavResponse, String>> {
        Box::pin(async move {
            let response = request.send().await.map_err(|error| error.to_string())?;
            let status = response.status();
            let headers = response.headers().clone();
            let body = response.text().await.map_err(|error| error.to_string())?;
            Ok(DavResponse {
                status,
                headers,
                body,
            })
        })
    }
}

impl CalDavClient {
    pub(crate) fn new(config: &CalDavConfig) -> Result<Self, AccountError> {
        let http = reqwest::Client::builder()
            .redirect(dav_redirect_policy(&config.base_url))
            .timeout(DAV_CLIENT_TIMEOUT)
            .build()
            .map_err(|error| local_error(AccountOperation::Discover, error.to_string()))?;

        Ok(Self {
            http,
            transport: Arc::new(ReqwestDavTransport),
            base_url: config.base_url.trim_end_matches('/').to_string(),
            credentials: config.credentials.clone(),
        })
    }

    #[cfg(test)]
    fn with_transport(base_url: &str, transport: Arc<dyn DavTransport>) -> Self {
        Self {
            http: reqwest::Client::new(),
            transport,
            base_url: base_url.trim_end_matches('/').to_string(),
            credentials: CalDavCredentials::bearer("token"),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_base_url(base_url: &str) -> Self {
        Self::with_transport(base_url, Arc::new(ReqwestDavTransport))
    }

    pub(crate) async fn discover_calendar_home(&self) -> Result<String, AccountError> {
        let base = self.base_url.clone();
        match self.discover_from_root(&base).await {
            Ok(home) => Ok(home),
            Err(error) if should_fallback_discovery(&error) => {
                let well_known = format!("{}/.well-known/caldav", self.base_url);
                self.discover_from_root(&well_known).await
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn discover_calendar_user_email(
        &self,
    ) -> Result<Option<String>, AccountError> {
        let base = self.base_url.clone();
        match self.discover_email_from_root(&base).await {
            Ok(email) => Ok(email),
            Err(error) if should_fallback_discovery(&error) => {
                let well_known = format!("{}/.well-known/caldav", self.base_url);
                self.discover_email_from_root(&well_known).await
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn discover_schedule_outbox_url(
        &self,
    ) -> Result<Option<String>, AccountError> {
        let base = self.base_url.clone();
        match self.discover_schedule_outbox_from_root(&base).await {
            Ok(outbox) => Ok(outbox),
            Err(error) if should_fallback_discovery(&error) => {
                let well_known = format!("{}/.well-known/caldav", self.base_url);
                self.discover_schedule_outbox_from_root(&well_known).await
            }
            Err(error) => Err(error),
        }
    }

    async fn discover_from_root(&self, root: &str) -> Result<String, AccountError> {
        let principal = self.discover_principal(root).await?;
        let body = self
            .propfind_raw(
                &principal,
                "0",
                PROPFIND_CALENDAR_HOME,
                AccountOperation::Discover,
            )
            .await?;
        extract_href_property(&body, "calendar-home-set")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?
            .map(|href| resolve_href(&self.base_url, &href))
            .ok_or_else(|| parse_error(AccountOperation::Discover, "missing calendar-home-set"))
    }

    async fn discover_email_from_root(&self, root: &str) -> Result<Option<String>, AccountError> {
        let principal = self.discover_principal(root).await?;
        let body = self
            .propfind_raw(
                &principal,
                "0",
                PROPFIND_CALENDAR_USER_ADDRESS,
                AccountOperation::Discover,
            )
            .await?;
        let hrefs = extract_href_properties(&body, "calendar-user-address-set")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?;
        Ok(hrefs.iter().find_map(|href| mailto_email(href)))
    }

    async fn discover_schedule_outbox_from_root(
        &self,
        root: &str,
    ) -> Result<Option<String>, AccountError> {
        let principal = self.discover_principal(root).await?;
        let body = self
            .propfind_raw(
                &principal,
                "0",
                PROPFIND_SCHEDULE_OUTBOX,
                AccountOperation::Discover,
            )
            .await?;
        extract_href_property(&body, "schedule-outbox-URL")
            .map_err(|error| parse_error(AccountOperation::Discover, error))
            .map(|href| href.map(|href| resolve_href(&self.base_url, &href)))
    }

    async fn discover_principal(&self, root: &str) -> Result<String, AccountError> {
        let body = self
            .propfind_raw(root, "0", PROPFIND_PRINCIPAL, AccountOperation::Discover)
            .await?;
        extract_href_property(&body, "current-user-principal")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?
            .map(|href| resolve_href(&self.base_url, &href))
            .ok_or_else(|| {
                parse_error(AccountOperation::Discover, "missing current-user-principal")
            })
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
        let body = self
            .propfind_raw(home_url, "1", PROPFIND_CALENDARS, operation)
            .await?;
        let mut collections = parse_calendar_collections(&body)
            .map_err(|error| parse_error(operation, format!("calendar list: {error}")))?;
        for collection in &mut collections {
            collection.resolve_href(&self.base_url);
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
        let body = self
            .propfind_raw(calendar_url, "1", PROPFIND_EVENTS, operation)
            .await?;
        let mut listing =
            parse_propfind_events(&body).map_err(|error| parse_error(operation, error))?;
        listing.resolve_hrefs(&self.base_url);
        Ok(listing)
    }

    /// Cheap depth-0 PROPFIND used by snapshot polling to refresh the
    /// collection sync token without re-listing every calendar collection.
    pub(crate) async fn collection_sync_token(
        &self,
        calendar_url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let body = self
            .propfind_raw(calendar_url, "0", PROPFIND_SYNC_TOKEN, operation)
            .await?;
        crate::parse::parse_collection_sync_token(&body)
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
            .report_raw(calendar_url, &body, AccountOperation::EventsInRange)
            .await?;
        let mut parsed = parse_multiget_report(&response).map_err(|error| {
            parse_error(AccountOperation::EventsInRange, format!("query: {error}"))
        })?;
        parsed.resolve_hrefs(&self.base_url);
        multiget_failure(&parsed, AccountOperation::EventsInRange).map_or(Ok(parsed), Err)
    }

    pub(crate) async fn query_events_text(
        &self,
        calendar_url: &str,
        query: &str,
    ) -> Result<MultigetFetch, AccountError> {
        let mut all_results = CalDavMultigetReport::default();
        let mut degraded = None;
        for property in ["SUMMARY", "DESCRIPTION", "LOCATION", "ATTENDEE"] {
            let body = calendar_text_query_body(property, query);
            let response = self
                .report_raw(calendar_url, &body, AccountOperation::EventSearch)
                .await?;
            let mut parsed = parse_multiget_report(&response).map_err(|error| {
                parse_error(AccountOperation::EventSearch, format!("query: {error}"))
            })?;
            parsed.resolve_hrefs(&self.base_url);
            if let Some(error) = multiget_failure(&parsed, AccountOperation::EventSearch) {
                degraded = worse_recovery(degraded, error);
            }
            all_results.extend(parsed);
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
        for chunk in uris.chunks(MULTIGET_BATCH_SIZE) {
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
    <D:getetag/>\n\
    <C:calendar-data/>\n\
  </D:prop>\n\
{href_elements}</C:calendar-multiget>"
            );
            let response = self.report_raw(calendar_url, &body, operation).await?;
            let mut parsed = parse_multiget_report(&response)
                .map_err(|error| parse_error(operation, format!("multiget: {error}")))?;
            parsed.resolve_hrefs(&self.base_url);
            if let Some(error) = multiget_failure(&parsed, operation) {
                degraded = worse_recovery(degraded, error);
            }
            all_results.extend(parsed);
        }
        MultigetFetch::settle(all_results, degraded)
    }

    pub(crate) async fn sync_events(
        &self,
        calendar_url: &str,
        sync_token: &str,
    ) -> Result<CalDavSyncReport, AccountError> {
        let body = sync_collection_body(sync_token);
        let operation = AccountOperation::SyncChanges;
        let response = self
            .report_raw_with_depth(calendar_url, "0", &body, operation)
            .await?;
        let status = response.status;
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
        report.resolve_hrefs(&self.base_url);
        Ok(report)
    }

    pub(crate) async fn get_event(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<CalDavFetchedEvent, AccountError> {
        let request = self
            .http
            .request(Method::GET, url)
            .headers(self.auth_headers(operation).await?);
        let response = self.send_raw_request(request, operation).await?;
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
            .http
            .request(Method::PUT, url)
            .header(CONTENT_TYPE, "text/calendar; charset=utf-8")
            .headers(self.auth_headers(operation).await?)
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
        let response = self.send_raw_request(request, operation).await?;
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
            .http
            .request(Method::DELETE, url)
            .headers(self.auth_headers(operation).await?);
        self.send_status_request(request, operation).await
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
            .http
            .request(Method::POST, outbox_url)
            .header(CONTENT_TYPE, "text/calendar; charset=utf-8")
            .headers(self.auth_headers(AccountOperation::EventRsvp).await?);
        if let Ok(value) = HeaderValue::from_str(&schedule_address(originator)) {
            request = request.header("Originator", value);
        }
        if let Ok(value) = HeaderValue::from_str(&schedule_address(recipient)) {
            request = request.header("Recipient", value);
        }
        let request = request.body(body);
        self.send_status_request(request, AccountOperation::EventRsvp)
            .await
    }

    pub(crate) fn resolve_url(&self, href: &str) -> String {
        if href.starts_with("http://") || href.starts_with("https://") {
            return href.to_string();
        }
        if let Ok(base) = Url::parse(&self.base_url)
            && let Ok(resolved) = base.join(href)
        {
            return resolved.to_string();
        }
        if self.base_url.ends_with('/') || href.starts_with('/') {
            format!("{}{href}", self.base_url)
        } else {
            format!("{}/{href}", self.base_url)
        }
    }

    async fn propfind_raw(
        &self,
        url: &str,
        depth: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<String, AccountError> {
        let method = Method::from_bytes(b"PROPFIND")
            .map_err(|error| local_error(operation, error.to_string()))?;
        let request = self
            .http
            .request(method, url)
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .header("Depth", depth)
            .headers(self.auth_headers(operation).await?)
            .body(body.to_string());
        self.send_body_request(request, operation).await
    }

    async fn report_raw(
        &self,
        url: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<String, AccountError> {
        // Ordinary REPORTs get the same status classification as any other
        // body request; only `sync_events` takes the raw-response path,
        // because it has to inspect 403/410 before they become errors.
        let response = self
            .report_raw_with_depth(url, "1", body, operation)
            .await?;
        if response.status.is_success() {
            Ok(response.body)
        } else {
            Err(status_error(operation, response.status, response.body))
        }
    }

    async fn report_raw_with_depth(
        &self,
        url: &str,
        depth: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<DavResponse, AccountError> {
        let method = Method::from_bytes(b"REPORT")
            .map_err(|error| local_error(operation, error.to_string()))?;
        let request = self
            .http
            .request(method, url)
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .header("Depth", depth)
            .headers(self.auth_headers(operation).await?)
            .body(body.to_string());
        self.send_raw_request(request, operation).await
    }

    async fn send_body_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: AccountOperation,
    ) -> Result<String, AccountError> {
        let response = self.send_raw_request(request, operation).await?;
        if response.status.is_success() {
            Ok(response.body)
        } else {
            Err(status_error(operation, response.status, response.body))
        }
    }

    async fn send_status_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        let response = self.send_raw_request(request, operation).await?;
        if response.status.is_success() {
            Ok(())
        } else {
            Err(status_error(operation, response.status, response.body))
        }
    }

    async fn send_raw_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: AccountOperation,
    ) -> Result<DavResponse, AccountError> {
        self.transport
            .send(request)
            .await
            .map_err(|error| transport_error(operation, error))
    }

    /// Build the per-request auth headers. The bearer token is read from
    /// the shared source on every call, so a token rotated mid-sync is
    /// honored on the next DAV request without reopening the account.
    async fn auth_headers(&self, operation: AccountOperation) -> Result<HeaderMap, AccountError> {
        let mut headers = HeaderMap::new();
        match &self.credentials {
            CalDavCredentials::Basic { username, password } => {
                let credentials = base64::engine::general_purpose::STANDARD
                    .encode(format!("{username}:{password}"));
                if let Ok(value) = HeaderValue::from_str(&format!("Basic {credentials}")) {
                    headers.insert(AUTHORIZATION, value);
                }
            }
            CalDavCredentials::Bearer { token_source } => {
                let token = token_source.current().await.map_err(|error| {
                    transport_error(
                        operation,
                        format!("failed to read OAuth access token: {error}"),
                    )
                })?;
                if let Ok(value) = HeaderValue::from_str(&format!("Bearer {}", token.as_str())) {
                    headers.insert(AUTHORIZATION, value);
                }
            }
        }
        Ok(headers)
    }
}

/// Hardened redirect policy for the DAV `reqwest::Client`, sourced from
/// `bifrost-net`'s single source of truth. The hop cap and the
/// case-insensitive host allowlist check both live in
/// `RedirectPolicy::reqwest_policy`; here we only seed the allowlist with
/// the configured base URL's host so cross-host redirects are stopped.
/// When the base URL has no parseable host the allowlist stays empty and
/// the policy degrades to a hop cap only - reqwest's own cross-origin
/// `Authorization` stripping still applies regardless.
fn dav_redirect_policy(base_url: &str) -> reqwest::redirect::Policy {
    let mut policy = bifrost_net::RedirectPolicy::default();
    if let Some(host) = Url::parse(base_url.trim_end_matches('/'))
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
    {
        policy = policy.trust_host(host);
    }
    policy.reqwest_policy()
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
        _ => String::new(),
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<C:calendar-query xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:prop>\n\
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

fn response_etag(headers: &HeaderMap) -> Option<String> {
    headers
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(normalize_http_etag)
}

fn normalize_http_etag(value: &str) -> String {
    let value = value.trim();
    if value
        .get(..2)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
    {
        format!("W/{}", value[2..].trim())
    } else {
        value.trim_matches('"').to_string()
    }
}

fn prepare_if_match(etag: &str) -> Option<String> {
    if etag
        .get(..2)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
    {
        None
    } else if etag.starts_with('"') {
        Some(etag.to_string())
    } else {
        Some(format!("\"{etag}\""))
    }
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
pub(crate) fn worse_recovery(
    current: Option<AccountError>,
    candidate: AccountError,
) -> Option<AccountError> {
    match current {
        Some(existing)
            if recovery_rank(existing.recovery()) >= recovery_rank(candidate.recovery()) =>
        {
            Some(existing)
        }
        _ => Some(candidate),
    }
}

fn recovery_rank(class: &RecoveryClass) -> u8 {
    match class {
        RecoveryClass::AuthLost => 4,
        RecoveryClass::NeedsAdminConsent { .. }
        | RecoveryClass::NeedsPolicyChange
        | RecoveryClass::NoPermission { .. } => 3,
        RecoveryClass::Retry(_) => 0,
        RecoveryClass::Reconcile(_) | RecoveryClass::Engine(_) => 1,
        _ => 2,
    }
}

/// Turn a wholly-failed 207 body into a real error.
///
/// RFC 4918 s13: a Multi-Status body can describe success, partial
/// success, or complete failure. The transport already returned 207, so
/// only the body says which. Handing a complete failure back as an
/// empty page lets a consumer record the collection as fully walked and
/// drop every resource in it permanently; routing it through
/// `status_error` instead gives the embedded status its normal
/// classification, so an all-401 body reauthorizes and an all-503 body
/// retries rather than silently truncating the calendar.
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
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .protocol(Protocol::CalDav)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

pub(crate) fn missing_event_error(operation: AccountOperation, id: String) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::NotFound(ResourceKind::Calendar),
        Cause::Request(RequestCause::NotFound {
            what: ResourceKind::Calendar,
            id: Some(id),
        }),
    )
    .protocol(Protocol::CalDav)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

pub(crate) fn local_error(operation: AccountOperation, message: impl Into<String>) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("caldav"),
            message: Some(DiagnosticText::support_only(message)),
        }),
    )
    .protocol(Protocol::CalDav)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

pub(crate) fn parse_error(operation: AccountOperation, message: impl Into<String>) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::CalDav,
            detail: Some(DiagnosticText::support_only(message)),
        }),
    )
    .protocol(Protocol::CalDav)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

pub(crate) fn transport_error(
    operation: AccountOperation,
    message: impl Into<String>,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(
            TransportKind::Network,
            Some(DiagnosticText::support_only(message)),
        )),
    )
    .push_cause(Cause::Attempt(bifrost_types::AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .protocol(Protocol::CalDav)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

pub(crate) fn status_error(
    operation: AccountOperation,
    status: StatusCode,
    body: String,
) -> AccountError {
    let kind = if status == StatusCode::UNAUTHORIZED {
        AccountErrorKind::Authentication(bifrost_types::AuthErrorKind::ReauthorizationRequired)
    } else if status == StatusCode::FORBIDDEN {
        AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied)
    } else if status == StatusCode::NOT_FOUND {
        AccountErrorKind::NotFound(ResourceKind::Calendar)
    } else if status == StatusCode::CONFLICT
        || status == StatusCode::PRECONDITION_FAILED
        || status == StatusCode::LOCKED
    {
        AccountErrorKind::ConcurrencyConflict
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        AccountErrorKind::Server(ServerErrorKind::RateLimited)
    } else if status == StatusCode::SERVICE_UNAVAILABLE {
        AccountErrorKind::Server(ServerErrorKind::Unavailable)
    } else if status == StatusCode::INSUFFICIENT_STORAGE {
        AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
    } else {
        AccountErrorKind::Server(ServerErrorKind::Error {
            status: Some(status.as_u16()),
        })
    };
    let cause = if status == StatusCode::UNAUTHORIZED {
        Cause::Auth(bifrost_types::AuthCause::ReauthorizationRequired)
    } else if status == StatusCode::FORBIDDEN {
        Cause::Access(bifrost_types::AccessCause::PermissionDenied {
            resource: Some(ResourceKind::Calendar),
        })
    } else if status == StatusCode::NOT_FOUND {
        Cause::Request(RequestCause::NotFound {
            what: ResourceKind::Calendar,
            id: None,
        })
    } else if status == StatusCode::CONFLICT
        || status == StatusCode::PRECONDITION_FAILED
        || status == StatusCode::LOCKED
    {
        Cause::State(StateCause::ConcurrencyConflict)
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        Cause::Server(ServerCause::RateLimited { retry_hint: None })
    } else if status == StatusCode::SERVICE_UNAVAILABLE {
        Cause::Server(ServerCause::Unavailable { retry_hint: None })
    } else if status == StatusCode::INSUFFICIENT_STORAGE {
        Cause::Server(ServerCause::QuotaExhausted { retry_hint: None })
    } else {
        Cause::Server(ServerCause::Error {
            status: Some(status.as_u16()),
        })
    };

    let mut builder = AccountErrorBuilder::new(kind, cause)
        .protocol(Protocol::CalDav)
        .operation(operation)
        .status(Some(status.as_u16()));
    let body = body.trim();
    if !body.is_empty() {
        builder = builder.text(DiagnosticText::support_only(body.to_string()));
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

pub(crate) fn event_scope(id: impl Into<String>) -> ErrorScope {
    ErrorScope::Calendar { id: id.into() }
}

const PROPFIND_PRINCIPAL: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\">\n\
  <D:prop>\n\
    <D:current-user-principal/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_CALENDAR_HOME: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:prop>\n\
    <C:calendar-home-set/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_CALENDAR_USER_ADDRESS: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:prop>\n\
    <C:calendar-user-address-set/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_SCHEDULE_OUTBOX: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:prop>\n\
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

    #[derive(Debug, Clone)]
    struct RequestTranscript {
        method: Method,
        url: String,
        headers: HeaderMap,
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
                });
            let response = self
                .responses
                .lock()
                .expect("scripted DAV response lock poisoned")
                .pop_front()
                .expect("scripted DAV transport exhausted");
            Box::pin(async move { Ok(response) })
        }
    }

    #[tokio::test]
    async fn sync_events_uses_depth_zero_report_transcript() {
        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body:
                "<D:multistatus xmlns:D=\"DAV:\"><D:sync-token>next</D:sync-token><D:response><D:href>/calendar/one.ics</D:href><D:status>HTTP/1.1 200 OK</D:status></D:response></D:multistatus>"
                    .to_string(),
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
    fn status_error_maps_write_conflicts() {
        for status in [
            StatusCode::CONFLICT,
            StatusCode::PRECONDITION_FAILED,
            StatusCode::LOCKED,
        ] {
            let error = status_error(AccountOperation::EventUpdate, status, String::new());
            assert_eq!(error.kind(), &AccountErrorKind::ConcurrencyConflict);
        }
    }

    #[test]
    fn status_error_maps_transient_and_quota_statuses() {
        let rate_limited = status_error(
            AccountOperation::EventUpdate,
            StatusCode::TOO_MANY_REQUESTS,
            String::new(),
        );
        assert_eq!(
            rate_limited.kind(),
            &AccountErrorKind::Server(ServerErrorKind::RateLimited)
        );

        let unavailable = status_error(
            AccountOperation::EventUpdate,
            StatusCode::SERVICE_UNAVAILABLE,
            String::new(),
        );
        assert_eq!(
            unavailable.kind(),
            &AccountErrorKind::Server(ServerErrorKind::Unavailable)
        );

        let quota = status_error(
            AccountOperation::EventUpdate,
            StatusCode::INSUFFICIENT_STORAGE,
            String::new(),
        );
        assert_eq!(
            quota.kind(),
            &AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
        );
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
    fn calendar_text_query_body_uses_property_text_match() {
        let body = calendar_text_query_body("SUMMARY", "plan & meet");

        assert!(body.contains("<C:prop-filter name=\"SUMMARY\">"));
        assert!(body.contains("<C:text-match collation=\"i;unicode-casemap\">"));
        assert!(body.contains("plan &amp; meet"));
    }

    #[test]
    fn schedule_outbox_propfind_requests_caldav_outbox_url() {
        assert!(PROPFIND_SCHEDULE_OUTBOX.contains("<C:schedule-outbox-URL/>"));
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
    fn weak_etag_is_never_sent_in_if_match() {
        assert_eq!(normalize_http_etag("W/\"abc\""), "W/\"abc\"");
        assert_eq!(prepare_if_match("W/\"abc\""), None);
        assert_eq!(prepare_if_match("abc").as_deref(), Some("\"abc\""));
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

    #[test]
    fn the_worst_recovery_class_wins_whatever_the_chunk_order() {
        let auth = || {
            status_error(
                AccountOperation::EventSearch,
                StatusCode::UNAUTHORIZED,
                "refused".to_string(),
            )
        };
        let transient = || {
            status_error(
                AccountOperation::EventSearch,
                StatusCode::SERVICE_UNAVAILABLE,
                "later".to_string(),
            )
        };

        let auth_first = worse_recovery(Some(auth()), transient());
        let transient_first = worse_recovery(Some(transient()), auth());

        assert_eq!(
            auth_first.expect("kept").recovery(),
            &RecoveryClass::AuthLost
        );
        assert_eq!(
            transient_first.expect("kept").recovery(),
            &RecoveryClass::AuthLost
        );
    }
}
