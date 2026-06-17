use std::time::Duration;

use base64::Engine;
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, DiagnosticText,
    ErrorScope, Protocol, ProtocolErrorKind, RequestCause, RequestErrorKind, ResourceKind,
    ServerCause, ServerErrorKind, StateCause, TransmissionState, TransportCause,
    TransportErrorKind, TransportKind, WireCause,
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Method, StatusCode, Url};

use crate::parse::{
    CalDavEventEntry, CalDavFetchedEvent, CalDavSyncReport, CalendarCollection,
    extract_href_property, parse_calendar_collections, parse_multiget_report,
    parse_propfind_events, parse_sync_collection_report,
};
use crate::{CalDavConfig, CalDavCredentials};

const DAV_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
const MULTIGET_BATCH_SIZE: usize = 50;

/// Redirect hop cap shared by both DAV crates. `bifrost-net` owns a
/// hardened method-aware redirect loop (trusted-host allowlist, hop
/// count, cross-host `Authorization` stripping), but the DAV clients run
/// their own bare `reqwest::Client` and do not route through it; routing
/// them through `bifrost-net` is a cross-crate migration tracked as a
/// follow-up. Until then both DAV crates agree on this hop count and both
/// enforce a base-host allowlist (see `dav_redirect_policy`) rather than
/// silently disagreeing (10 vs 5) and following arbitrary cross-host hops.
const DAV_MAX_REDIRECTS: usize = 5;

#[derive(Debug, Clone, Copy)]
pub(crate) enum PutCondition<'a> {
    IfNoneMatch,
    IfMatch(&'a str),
    None,
}

#[derive(Debug, Clone)]
pub(crate) struct CalDavClient {
    http: reqwest::Client,
    base_url: String,
    credentials: CalDavCredentials,
}

impl CalDavClient {
    pub(crate) fn new(config: &CalDavConfig) -> Result<Self, AccountError> {
        let trusted_host = Url::parse(config.base_url.trim_end_matches('/'))
            .ok()
            .and_then(|url| url.host_str().map(str::to_ascii_lowercase));
        let http = reqwest::Client::builder()
            .redirect(dav_redirect_policy(trusted_host))
            .timeout(DAV_CLIENT_TIMEOUT)
            .build()
            .map_err(|error| local_error(AccountOperation::Discover, error.to_string()))?;

        Ok(Self {
            http,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            credentials: config.credentials.clone(),
        })
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
            .map(|href| self.resolve_url(&href))
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
        let href = extract_href_property(&body, "calendar-user-address-set")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?;
        Ok(href.and_then(|href| mailto_email(&href)))
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
            .map(|href| href.map(|href| self.resolve_url(&href)))
    }

    async fn discover_principal(&self, root: &str) -> Result<String, AccountError> {
        let body = self
            .propfind_raw(root, "0", PROPFIND_PRINCIPAL, AccountOperation::Discover)
            .await?;
        extract_href_property(&body, "current-user-principal")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?
            .map(|href| self.resolve_url(&href))
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
        parse_calendar_collections(&body)
            .map_err(|error| parse_error(operation, format!("calendar list: {error}")))
    }

    pub(crate) async fn list_events(
        &self,
        calendar_url: &str,
    ) -> Result<Vec<CalDavEventEntry>, AccountError> {
        self.list_events_for_operation(calendar_url, AccountOperation::EventsInRange)
            .await
    }

    pub(crate) async fn list_events_for_operation(
        &self,
        calendar_url: &str,
        operation: AccountOperation,
    ) -> Result<Vec<CalDavEventEntry>, AccountError> {
        Ok(self
            .list_events_listing(calendar_url, operation)
            .await?
            .entries)
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
        parse_propfind_events(&body).map_err(|error| parse_error(operation, error))
    }

    pub(crate) async fn query_events_in_range(
        &self,
        calendar_url: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Vec<CalDavFetchedEvent>, AccountError> {
        let body = calendar_query_body(start, end);
        let response = self
            .report_raw(calendar_url, &body, AccountOperation::EventsInRange)
            .await?;
        parse_multiget_report(&response).map_err(|error| {
            parse_error(AccountOperation::EventsInRange, format!("query: {error}"))
        })
    }

    pub(crate) async fn query_events_text(
        &self,
        calendar_url: &str,
        query: &str,
    ) -> Result<Vec<CalDavFetchedEvent>, AccountError> {
        let mut all_results = Vec::new();
        for property in ["SUMMARY", "DESCRIPTION", "LOCATION", "ATTENDEE"] {
            let body = calendar_text_query_body(property, query);
            let response = self
                .report_raw(calendar_url, &body, AccountOperation::EventSearch)
                .await?;
            let parsed = parse_multiget_report(&response).map_err(|error| {
                parse_error(AccountOperation::EventSearch, format!("query: {error}"))
            })?;
            all_results.extend(parsed);
        }
        Ok(all_results)
    }

    pub(crate) async fn fetch_events(
        &self,
        calendar_url: &str,
        uris: &[String],
        operation: AccountOperation,
    ) -> Result<Vec<CalDavFetchedEvent>, AccountError> {
        let mut all_results = Vec::new();
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
            let parsed = parse_multiget_report(&response)
                .map_err(|error| parse_error(operation, format!("multiget: {error}")))?;
            all_results.extend(parsed);
        }
        Ok(all_results)
    }

    pub(crate) async fn sync_events(
        &self,
        calendar_url: &str,
        sync_token: &str,
    ) -> Result<CalDavSyncReport, AccountError> {
        let body = sync_collection_body(sync_token);
        let response = self
            .report_raw(calendar_url, &body, AccountOperation::SyncChanges)
            .await?;
        parse_sync_collection_report(&response)
            .map_err(|error| parse_error(AccountOperation::SyncChanges, format!("sync: {error}")))
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
        let response = request
            .send()
            .await
            .map_err(|error| transport_error(operation, error.to_string()))?;
        let status = response.status();
        let etag = response_etag(response.headers());
        let body = response
            .text()
            .await
            .map_err(|error| transport_error(operation, error.to_string()))?;
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
                request = request.header("If-Match", prepare_if_match(etag));
            }
            PutCondition::None => {}
        }
        let response = request
            .send()
            .await
            .map_err(|error| transport_error(operation, error.to_string()))?;
        let status = response.status();
        let etag = response_etag(response.headers());
        let body = response
            .text()
            .await
            .map_err(|error| transport_error(operation, error.to_string()))?;
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
        let method = Method::from_bytes(b"REPORT")
            .map_err(|error| local_error(operation, error.to_string()))?;
        let request = self
            .http
            .request(method, url)
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .header("Depth", "1")
            .headers(self.auth_headers(operation).await?)
            .body(body.to_string());
        self.send_body_request(request, operation).await
    }

    async fn send_body_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: AccountOperation,
    ) -> Result<String, AccountError> {
        let response = request
            .send()
            .await
            .map_err(|error| transport_error(operation, error.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| transport_error(operation, error.to_string()))?;
        if status.is_success() || status == StatusCode::MULTI_STATUS {
            Ok(body)
        } else {
            Err(status_error(operation, status, body))
        }
    }

    async fn send_status_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        let response = request
            .send()
            .await
            .map_err(|error| transport_error(operation, error.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| transport_error(operation, error.to_string()))?;
        if status.is_success() {
            Ok(())
        } else {
            Err(status_error(operation, status, body))
        }
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

/// Hardened redirect policy for the DAV `reqwest::Client`: cap at
/// `DAV_MAX_REDIRECTS` hops and refuse any cross-host redirect whose host
/// differs from the configured base URL's host (case-insensitive). When
/// the base URL has no parseable host the allowlist cannot be enforced, so
/// the policy degrades to a hop cap only - matching reqwest's own
/// cross-origin `Authorization` stripping, which still applies.
fn dav_redirect_policy(trusted_host: Option<String>) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() >= DAV_MAX_REDIRECTS {
            return attempt.error("too many redirects");
        }
        if let Some(host) = trusted_host.as_deref()
            && attempt.url().host_str().map(str::to_ascii_lowercase).as_deref() != Some(host)
        {
            return attempt.stop();
        }
        attempt.follow()
    })
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
    href.strip_prefix("mailto:")
        .or_else(|| href.strip_prefix("MAILTO:"))
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
        .map(|value| value.trim_matches('"').to_string())
}

fn prepare_if_match(etag: &str) -> String {
    if etag.starts_with('"') {
        etag.to_string()
    } else {
        format!("\"{etag}\"")
    }
}

fn should_fallback_discovery(error: &AccountError) -> bool {
    matches!(
        error.kind(),
        AccountErrorKind::NotFound(ResourceKind::Calendar)
    )
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

fn status_error(operation: AccountOperation, status: StatusCode, body: String) -> AccountError {
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let client = CalDavClient {
            http: reqwest::Client::new(),
            base_url: "not a url".to_string(),
            credentials: CalDavCredentials::bearer("token"),
        };

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
        assert_eq!(mailto_email("/principals/ada"), None);
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
}
