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
    trusted_origins: Vec<String>,
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
            .field("base_url", &self.base_url)
            .field("credentials", &self.credentials)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DavResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: String,
    /// Effective request URI, after any redirects the client followed.
    ///
    /// RFC 4918 relative hrefs in a Multi-Status resolve against the
    /// effective request URI, not the URI the caller submitted. The DAV
    /// redirect policy permits same-host hops, so a PROPFIND on
    /// `/calendar` that lands on `/dav/users/ada/calendar/` is a real
    /// deployment shape; resolving `one.ics` against the submitted URI
    /// there mints a wrong native id and a wrong follow-up request URL.
    url: String,
}

/// A DAV response body paired with the effective URI that produced it,
/// so href resolution has the base RFC 4918 requires.
struct DavBody {
    text: String,
    url: String,
}

/// Local DAV transport boundary. `bifrost-net`'s dispatcher is intentionally
/// crate-private, while DAV keeps Basic auth and its own redirect policy, so
/// the seam belongs here until these clients move onto `AccountNet`.
pub(crate) trait DavTransport: Send + Sync {
    fn send(&self, request: reqwest::RequestBuilder) -> AccountFuture<Result<DavResponse, String>>;
}

struct ReqwestDavTransport;

impl DavTransport for ReqwestDavTransport {
    fn send(&self, request: reqwest::RequestBuilder) -> AccountFuture<Result<DavResponse, String>> {
        Box::pin(async move {
            let response = request.send().await.map_err(|error| error.to_string())?;
            let status = response.status();
            let headers = response.headers().clone();
            let url = response.url().to_string();
            let body = read_capped_body(response).await?;
            Ok(DavResponse {
                status,
                headers,
                body,
                url,
            })
        })
    }
}

/// Read a DAV response body with a ceiling.
///
/// `response.text()` buffers without one, so a provider returning a
/// runaway 207, an error page, or a mis-routed blob URL OOMs the
/// process. A Multi-Status body for a large calendar is legitimately
/// big, hence a ceiling generous enough that only a pathological
/// response reaches it, matching the buffered ceiling `bifrost-net`
/// applies on its own `send` path.
const RESPONSE_BODY_TOO_LARGE: &str = "DAV response body exceeded the buffered ceiling";

async fn read_capped_body(response: reqwest::Response) -> Result<String, String> {
    use futures::StreamExt;

    let limit = bifrost_net::DEFAULT_MAX_BUFFERED_RESPONSE;
    let mut stream = response.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        if buf.len() + chunk.len() > limit {
            return Err(format!("{RESPONSE_BODY_TOO_LARGE} ({limit} bytes)"));
        }
        buf.extend_from_slice(&chunk);
    }
    // `.text()` decodes per the `charset` Content-Type parameter and
    // falls back to lossy UTF-8. This decodes lossily unconditionally,
    // which narrows behaviour for a server that declares a non-UTF-8
    // charset - RFC 4918 bodies are XML, whose declared default is
    // UTF-8, so that case was already outside what the parsers here
    // handle. Lossy rather than strict keeps a malformed byte behaving
    // as it did before (a replacement character, not a failed request).
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

impl CalDavClient {
    pub(crate) fn new(config: &CalDavConfig) -> Result<Self, AccountError> {
        let http = reqwest::Client::builder()
            .redirect(dav_redirect_policy())
            .timeout(DAV_CLIENT_TIMEOUT)
            .build()
            .map_err(|error| local_error(AccountOperation::Discover, error.to_string()))?;

        let base_url = config.base_url.trim_end_matches('/').to_string();
        let trusted_origins = url_origin(&base_url).into_iter().collect::<Vec<_>>();
        Ok(Self {
            http,
            transport: Arc::new(ReqwestDavTransport),
            trusted_origins,
            base_url,
            credentials: config.credentials.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_transport(base_url: &str, transport: Arc<dyn DavTransport>) -> Self {
        let trusted_origins = url_origin(base_url).into_iter().collect::<Vec<_>>();
        Self {
            http: reqwest::Client::new(),
            transport,
            base_url: base_url.trim_end_matches('/').to_string(),
            credentials: CalDavCredentials::bearer("token"),
            trusted_origins,
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
        let from_well_known = match bifrost_net::url::well_known_url(&self.base_url, "caldav") {
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
        self.discover_principal(&self.base_url)
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
        let response = match self.report_raw(url, depth, body, operation).await {
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
            .report_raw_with_depth(calendar_url, "0", &body, operation)
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
            .http
            .request(Method::GET, url)
            .headers(self.auth_headers(url, operation).await?);
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
            .headers(self.auth_headers(url, operation).await?)
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
            .headers(self.auth_headers(url, operation).await?);
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
            .headers(
                self.auth_headers(outbox_url, AccountOperation::EventRsvp)
                    .await?,
            );
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
    ) -> Result<DavBody, AccountError> {
        let method = Method::from_bytes(b"PROPFIND")
            .map_err(|error| local_error(operation, error.to_string()))?;
        let request = self
            .http
            .request(method, url)
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .header("Depth", depth)
            .headers(self.auth_headers(url, operation).await?)
            .body(body.to_string());
        self.send_body_request(request, operation).await
    }

    async fn report_raw(
        &self,
        url: &str,
        depth: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<DavBody, AccountError> {
        // Ordinary REPORTs get the same status classification as any other
        // body request; only `sync_events` takes the raw-response path,
        // because it has to inspect 403/410 before they become errors.
        let response = self
            .report_raw_with_depth(url, depth, body, operation)
            .await?;
        settle_body(response, operation)
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
            .headers(self.auth_headers(url, operation).await?)
            .body(body.to_string());
        self.send_raw_request(request, operation).await
    }

    async fn send_body_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: AccountOperation,
    ) -> Result<DavBody, AccountError> {
        let response = self.send_raw_request(request, operation).await?;
        settle_body(response, operation)
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

    /// Send a request, following cross-origin redirects by hand.
    ///
    /// Same-origin hops are followed inside reqwest, which preserves the
    /// `Authorization` header when scheme, host, and effective port are all
    /// unchanged. A cross-origin hop cannot ride that path: reqwest strips
    /// `Authorization` on any origin change and its redirect policy has no
    /// way to restore it, so a followed hop would reach the destination
    /// unauthenticated. Cross-origin 3xx responses are therefore stopped by
    /// the policy and re-dispatched here with fresh `auth_headers` for the
    /// target - and `auth_headers` refuses any origin discovery did not
    /// admit, so a server-controlled `Location` can never widen trust, only
    /// spend trust that authenticated discovery already granted. The method
    /// and body are preserved on 301/302/307/308; DAV verbs have no useful
    /// GET rewrite, and RFC 7231 permits preserving them. A 303 is not
    /// followed and classifies as a terminal status downstream.
    async fn send_raw_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: AccountOperation,
    ) -> Result<DavResponse, AccountError> {
        let max_hops = usize::from(bifrost_net::RedirectPolicy::default().max_hops);
        let mut request = request;
        let mut hops = 0usize;
        loop {
            let replay = request.try_clone();
            let response = self
                .transport
                .send(request)
                .await
                .map_err(|error| response_read_error(operation, error))?;
            let redirect = matches!(
                response.status,
                StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT
            );
            let location = response
                .headers
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok());
            let Some(location) = location.filter(|_| redirect) else {
                if !self.is_trusted_url(&response.url) {
                    return Err(local_error(
                        operation,
                        format!(
                            "response arrived from an untrusted DAV origin: {}",
                            response.url
                        ),
                    ));
                }
                return Ok(response);
            };
            let next = Url::parse(&response.url)
                .and_then(|base| base.join(location))
                .map_err(|error| {
                    local_error(operation, format!("unresolvable redirect target: {error}"))
                })?;
            hops += 1;
            if hops > max_hops {
                return Err(local_error(operation, "too many redirects"));
            }
            let Some(replay) = replay else {
                return Err(local_error(
                    operation,
                    "redirected DAV request cannot be replayed",
                ));
            };
            let previous = replay
                .build()
                .map_err(|error| local_error(operation, error.to_string()))?;
            // Fresh credentials for the target origin; refused locally when
            // the origin was never admitted by discovery.
            let auth = self.auth_headers(next.as_str(), operation).await?;
            let mut headers = previous.headers().clone();
            headers.remove(AUTHORIZATION);
            for (name, value) in &auth {
                headers.insert(name, value.clone());
            }
            let mut rebuilt = self
                .http
                .request(previous.method().clone(), next)
                .headers(headers);
            if let Some(body) = previous.body().and_then(reqwest::Body::as_bytes) {
                rebuilt = rebuilt.body(body.to_vec());
            }
            request = rebuilt;
        }
    }

    /// Build the per-request auth headers. The bearer token is read from
    /// the shared source on every call, so a token rotated mid-sync is
    /// honored on the next DAV request without reopening the account.
    async fn auth_headers(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<HeaderMap, AccountError> {
        if !self.is_trusted_url(url) {
            return Err(local_error(
                operation,
                format!("refusing to send DAV credentials to untrusted URL: {url}"),
            ));
        }
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

    /// Admit successfully discovered DAV origins to the credential gate.
    ///
    /// Discovery is server-steered: the calendar home and scheduling outbox
    /// come out of the principal's own PROPFIND response, so a compromised or
    /// hostile server picks these origins. Two rules bound what it can pick.
    /// A discovered origin must parse, and it must never weaken the transport
    /// guarantee the configured base URL already established - an
    /// HTTPS-configured account never trusts a plaintext discovered home,
    /// because that would turn discovery into a downgrade channel for the
    /// account credential. A cross-origin HTTPS home is still admitted; that
    /// is a real deployment shape, where the principal and the calendar home
    /// live on different hosts of the same service. Admission happens only
    /// after the complete authenticated discovery result is available,
    /// before the account is shared or a home request can be in flight.
    pub(crate) fn admit_discovered_urls(&mut self, urls: impl IntoIterator<Item = String>) {
        for url in urls {
            let Some(origin) = url_origin(&url) else {
                continue;
            };
            if origin_is_secure(&self.base_url) && !origin_is_secure(&url) {
                continue;
            }
            if !self.trusted_origins.contains(&origin) {
                self.trusted_origins.push(origin);
            }
        }
    }

    fn is_trusted_url(&self, url: &str) -> bool {
        url_origin(url).is_some_and(|origin| self.trusted_origins.contains(&origin))
    }
}

/// Whether a URL's scheme carries an authenticated, encrypted transport.
///
/// Only `https` qualifies; an unparseable URL is treated as insecure so the
/// downgrade check fails closed.
/// Classify a completed DAV response and keep the effective URI attached
/// to the body, so the caller resolves hrefs against the URI that actually
/// served the Multi-Status rather than the one it submitted.
fn settle_body(
    response: DavResponse,
    operation: AccountOperation,
) -> Result<DavBody, AccountError> {
    if response.status.is_success() {
        Ok(DavBody {
            text: response.body,
            url: response.url,
        })
    } else {
        Err(status_error(operation, response.status, response.body))
    }
}

fn origin_is_secure(value: &str) -> bool {
    Url::parse(value).is_ok_and(|url| url.scheme().eq_ignore_ascii_case("https"))
}

fn url_origin(value: &str) -> Option<String> {
    let url = Url::parse(value).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    Some(format!(
        "{}://{}:{}",
        url.scheme().to_ascii_lowercase(),
        host,
        url.port_or_known_default()?
    ))
}

/// Hardened redirect policy for the DAV `reqwest::Client`.
///
/// Follows a hop only when the next URL keeps the exact origin (scheme,
/// host, effective port) of the URL that issued the redirect; reqwest
/// preserves `Authorization` precisely under that condition, and strips it
/// on any origin change with no way for a policy to restore it. Every
/// cross-origin hop is stopped so the 3xx surfaces to `send_raw_request`,
/// which re-dispatches it with fresh credentials against the admitted
/// origin set. The hop cap comes from `bifrost-net`.
fn dav_redirect_policy() -> reqwest::redirect::Policy {
    let max_hops = usize::from(bifrost_net::RedirectPolicy::default().max_hops);
    reqwest::redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() >= max_hops {
            return attempt.error("too many redirects");
        }
        let same_origin = attempt
            .previous()
            .last()
            .and_then(|previous| url_origin(previous.as_str()))
            .zip(url_origin(attempt.url().as_str()))
            .is_some_and(|(previous, next)| previous == next);
        if same_origin {
            attempt.follow()
        } else {
            attempt.stop()
        }
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
        RecoveryClass::Unsupported(_)
        | RecoveryClass::ClientBug
        | RecoveryClass::ProviderContractViolation
        | RecoveryClass::ProviderRefused
        | RecoveryClass::UnknownPermanent => 2,
        // RecoveryClass is non-exhaustive. An unknown future class must win
        // rather than being silently ranked below a known terminal failure.
        _ => u8::MAX,
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

fn response_read_error(operation: AccountOperation, message: String) -> AccountError {
    if !message.starts_with(RESPONSE_BODY_TOO_LARGE) {
        return transport_error(operation, message);
    }
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::CalDav,
            detail: Some(DiagnosticText::support_only(message)),
        }),
    )
    .push_cause(Cause::Attempt(bifrost_types::AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .protocol(Protocol::CalDav)
    .operation(operation)
    .try_build()
    .expect("valid acknowledged response-overflow classification")
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

    /// A cross-calendar `event_update` is refused, and refused before any I/O.
    ///
    /// `patch.calendar_id` only ever chose which calendar to FETCH from - the
    /// PUT always went back to the event's original location - so a move
    /// request used to return `Ok(())` having done nothing, which is worse than
    /// either refusing or performing it. bifrost-carddav's `contact_update`
    /// already refuses the same shape; these two must not drift apart.
    ///
    /// The second half matters as much as the first: a patch that RESTATES the
    /// event's current calendar is legal and common, and must still go through.
    /// A guard that refuses any `calendar_id` at all would pass the first
    /// assertion and break every ordinary update.
    #[tokio::test]
    async fn event_update_refuses_a_cross_calendar_move_but_allows_a_restated_calendar() {
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

        // Refused, with an EMPTY script: a regression sends a request and
        // starves the transport rather than failing a soft assertion.
        let script = ScriptedDavTransport::new([]);
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
            .expect_err("a move between calendars must be refused, not dropped");
        assert!(
            script.requests().is_empty(),
            "a refused move must reach no transport at all"
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
    fn response_overflow_after_mutation_is_acknowledged_and_reconciles() {
        let error = response_read_error(
            AccountOperation::EventDelete,
            format!("{RESPONSE_BODY_TOO_LARGE} (1 bytes)"),
        );
        assert!(error.recovery().requires_reconciliation());
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            Cause::Attempt(attempt)
                if attempt.transmission_state == TransmissionState::Acknowledged
        )));
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

    /// Pins the whole known ladder, not just the 401-vs-503 pair the test
    /// below covers. The two DAV crates carry byte-identical copies of this
    /// function and have drifted before, so the ordering is asserted
    /// explicitly in each.
    ///
    /// The `_ =>` arm cannot be pinned hermetically: `RecoveryClass` is
    /// `#[non_exhaustive]` and lives in `bifrost-types`, so no test in this
    /// crate can name a variant this `match` does not already list. What is
    /// pinnable is that every variant we CAN name ranks strictly below the
    /// sentinel, which is what makes an unknown one win by construction.
    #[test]
    fn recovery_ranks_order_from_retryable_up_to_auth_lost() {
        use bifrost_types::{EngineDirective, RetryAdvice, RetryDisposition, RetryReason};

        let retry = RecoveryClass::Retry(RetryAdvice::new(
            RetryDisposition::SameRequest,
            None,
            RetryReason::Transport,
            None,
        ));
        let engine = RecoveryClass::Engine(EngineDirective::RestartAccount);
        let terminal = [
            RecoveryClass::Unsupported(AccountOperation::EventSearch),
            RecoveryClass::ClientBug,
            RecoveryClass::ProviderContractViolation,
            RecoveryClass::ProviderRefused,
            RecoveryClass::UnknownPermanent,
        ];
        let consent = [
            RecoveryClass::NeedsAdminConsent { needed: "scope" },
            RecoveryClass::NeedsPolicyChange,
            RecoveryClass::NoPermission { resource: None },
        ];

        assert!(recovery_rank(&retry) < recovery_rank(&engine));
        for class in &terminal {
            assert!(
                recovery_rank(&engine) < recovery_rank(class),
                "{class:?} must outrank an engine directive"
            );
            for stronger in &consent {
                assert!(
                    recovery_rank(class) < recovery_rank(stronger),
                    "{stronger:?} must outrank {class:?}"
                );
            }
        }
        for class in &consent {
            assert!(
                recovery_rank(class) < recovery_rank(&RecoveryClass::AuthLost),
                "AuthLost must outrank {class:?}"
            );
            // Every named variant sits below the catch-all sentinel, so an
            // unknown future class escalates rather than being buried.
            assert!(recovery_rank(class) < u8::MAX);
        }
        assert!(recovery_rank(&RecoveryClass::AuthLost) < u8::MAX);
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
