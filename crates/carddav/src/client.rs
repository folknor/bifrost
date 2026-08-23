use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use base64::Engine;
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFuture, AccountOperation, Cause,
    DiagnosticText, ErrorScope, Protocol, ProtocolErrorKind, RecoveryClass, RequestCause,
    RequestErrorKind, ResourceKind, ServerCause, ServerErrorKind, StateCause, TransmissionState,
    TransportCause, TransportErrorKind, TransportKind, WireCause,
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, ETAG, HeaderMap, HeaderValue};
use reqwest::{Method, StatusCode, Url};

use crate::parse::{
    AddressBookCollection, CardDavContactEntry, CardDavContactListing, CardDavMultigetReport,
    MultigetOutcome, extract_href_property, parse_addressbook_collections, parse_multiget_report,
    parse_propfind_contacts, resolve_href,
};
use crate::{CardDavConfig, CardDavCredentials};

const DAV_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
const MULTIGET_BATCH_SIZE: usize = 50;

#[derive(Debug, Clone, Copy)]
pub(crate) enum PutCondition<'a> {
    IfNoneMatch,
    IfMatch(&'a str),
    None,
}

#[derive(Clone)]
pub(crate) struct CardDavClient {
    http: reqwest::Client,
    transport: Arc<dyn DavTransport>,
    base_url: String,
    credentials: CardDavCredentials,
    trusted_origins: Arc<RwLock<Vec<String>>>,
}

impl fmt::Debug for CardDavClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardDavClient")
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

/// Local DAV transport boundary. `bifrost-net` keeps its dispatcher private,
/// while this client still owns Basic auth and DAV redirect policy.
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
            let body = read_capped_body(response).await?;
            Ok(DavResponse {
                status,
                headers,
                body,
            })
        })
    }
}

/// Read a DAV response body with a ceiling.
///
/// `response.text()` buffers without one, so a provider returning a
/// runaway 207, an error page, or a mis-routed blob URL OOMs the
/// process. A Multi-Status body for a large address book is
/// legitimately big, hence a ceiling generous enough that only a
/// pathological response reaches it, matching the buffered ceiling
/// `bifrost-net` applies on its own `send` path.
async fn read_capped_body(response: reqwest::Response) -> Result<String, String> {
    use futures::StreamExt;

    let limit = bifrost_net::DEFAULT_MAX_BUFFERED_RESPONSE;
    let mut stream = response.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        if buf.len() + chunk.len() > limit {
            return Err(format!(
                "DAV response body exceeded the {limit}-byte ceiling"
            ));
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

impl CardDavClient {
    pub(crate) fn new(config: &CardDavConfig) -> Result<Self, AccountError> {
        let http = reqwest::Client::builder()
            .redirect(dav_redirect_policy(&config.base_url))
            .timeout(DAV_CLIENT_TIMEOUT)
            .build()
            .map_err(|error| local_error(AccountOperation::Discover, error.to_string()))?;

        let base_url = config.base_url.trim_end_matches('/').to_string();
        Ok(Self {
            http,
            transport: Arc::new(ReqwestDavTransport),
            trusted_origins: Arc::new(RwLock::new(url_origin(&base_url).into_iter().collect())),
            base_url,
            credentials: config.credentials.clone(),
        })
    }

    pub(crate) async fn discover_addressbook_home(&self) -> Result<String, AccountError> {
        let well_known_url = format!("{}/.well-known/carddav", self.base_url);
        let dav_root = match self
            .propfind_raw(
                &well_known_url,
                "0",
                PROPFIND_PRINCIPAL,
                AccountOperation::Discover,
            )
            .await
        {
            Ok(body) => match extract_href_property(&body, "current-user-principal")
                .map_err(|error| parse_error(AccountOperation::Discover, error))?
                .map(|href| resolve_href(&self.base_url, &href))
            {
                Some(principal) => {
                    return self.addressbook_home_for_principal(principal).await;
                }
                None => self.base_url.clone(),
            },
            Err(error) if should_fallback_discovery(&error) => self.base_url.clone(),
            Err(error) => return Err(error),
        };

        let body = self
            .propfind_raw(
                &dav_root,
                "0",
                PROPFIND_PRINCIPAL,
                AccountOperation::Discover,
            )
            .await?;
        let principal = extract_href_property(&body, "current-user-principal")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?
            .map(|href| resolve_href(&self.base_url, &href))
            .ok_or_else(|| {
                parse_error(AccountOperation::Discover, "missing current-user-principal")
            })?;
        self.addressbook_home_for_principal(principal).await
    }

    async fn addressbook_home_for_principal(
        &self,
        principal: String,
    ) -> Result<String, AccountError> {
        let body = self
            .propfind_raw(
                &principal,
                "0",
                PROPFIND_ADDRESSBOOK_HOME,
                AccountOperation::Discover,
            )
            .await?;
        extract_href_property(&body, "addressbook-home-set")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?
            .map(|href| resolve_href(&self.base_url, &href))
            .inspect(|home| self.trust_discovered_url(home))
            .ok_or_else(|| parse_error(AccountOperation::Discover, "missing addressbook-home-set"))
    }

    pub(crate) async fn list_addressbooks(
        &self,
        home_url: &str,
    ) -> Result<Vec<AddressBookCollection>, AccountError> {
        self.list_addressbooks_for_operation(home_url, AccountOperation::AddressBooksList)
            .await
    }

    pub(crate) async fn list_addressbooks_for_operation(
        &self,
        home_url: &str,
        operation: AccountOperation,
    ) -> Result<Vec<AddressBookCollection>, AccountError> {
        let body = self
            .propfind_raw(home_url, "1", PROPFIND_ADDRESSBOOKS, operation)
            .await?;
        let mut collections = parse_addressbook_collections(&body)
            .map_err(|error| parse_error(operation, format!("addressbook list: {error}")))?;
        for collection in &mut collections {
            collection.resolve_href(&self.base_url);
        }
        Ok(collections)
    }

    pub(crate) async fn list_contacts(
        &self,
        addressbook_url: &str,
    ) -> Result<Vec<CardDavContactEntry>, AccountError> {
        self.list_contacts_for_operation(addressbook_url, AccountOperation::ContactsList)
            .await
    }

    /// Cheap depth-0 PROPFIND for the collection `getctag`. Returns
    /// `None` when the server omits it, so the caller falls through to a
    /// full snapshot + diff (brick 8 ctag short-circuit).
    pub(crate) async fn collection_ctag(
        &self,
        addressbook_url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let body = self
            .propfind_raw(addressbook_url, "0", PROPFIND_CTAG, operation)
            .await?;
        crate::parse::parse_collection_ctag(&body)
            .map_err(|error| parse_error(operation, format!("collection ctag: {error}")))
    }

    pub(crate) async fn list_contacts_for_operation(
        &self,
        addressbook_url: &str,
        operation: AccountOperation,
    ) -> Result<Vec<CardDavContactEntry>, AccountError> {
        Ok(self
            .list_contacts_listing(addressbook_url, operation)
            .await?
            .entries)
    }

    /// Depth-1 contact PROPFIND returning both the committed entries and
    /// the hrefs the server reported *failed* within the 207, so the
    /// snapshot diff can preserve transiently-failed resources rather
    /// than destroying them (brick 7).
    pub(crate) async fn list_contacts_listing(
        &self,
        addressbook_url: &str,
        operation: AccountOperation,
    ) -> Result<CardDavContactListing, AccountError> {
        let body = self
            .propfind_raw(addressbook_url, "1", PROPFIND_CONTACTS, operation)
            .await?;
        let mut listing = parse_propfind_contacts(&body)
            .map_err(|error| parse_error(operation, format!("contact list: {error}")))?;
        listing.resolve_hrefs(&self.base_url);
        Ok(listing)
    }

    pub(crate) async fn fetch_vcards(
        &self,
        addressbook_url: &str,
        uris: &[String],
        operation: AccountOperation,
    ) -> Result<MultigetFetch, AccountError> {
        let mut all_results = CardDavMultigetReport::default();
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
<C:addressbook-multiget xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">\n\
  <D:prop>\n\
    <D:getetag/>\n\
    <C:address-data/>\n\
  </D:prop>\n\
{href_elements}</C:addressbook-multiget>"
            );
            let response = self
                .report_raw(addressbook_url, "0", &body, operation)
                .await?;
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

    pub(crate) async fn query_vcards_text(
        &self,
        addressbook_url: &str,
        query: &str,
    ) -> Result<MultigetFetch, AccountError> {
        let mut all_results = CardDavMultigetReport::default();
        let mut degraded = None;
        for property in ["FN", "N", "EMAIL", "TEL", "ADR", "ORG", "TITLE", "NOTE"] {
            let body = addressbook_text_query_body(property, query);
            let response = self
                .report_raw(addressbook_url, "1", &body, AccountOperation::ContactSearch)
                .await?;
            let mut parsed = parse_multiget_report(&response).map_err(|error| {
                parse_error(AccountOperation::ContactSearch, format!("query: {error}"))
            })?;
            parsed.resolve_hrefs(&self.base_url);
            if let Some(error) = multiget_failure(&parsed, AccountOperation::ContactSearch) {
                degraded = worse_recovery(degraded, error);
            }
            all_results.extend(parsed);
        }
        MultigetFetch::settle(all_results, degraded)
    }

    pub(crate) async fn put_vcard(
        &self,
        url: &str,
        body: String,
        condition: PutCondition<'_>,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let mut request = self
            .http
            .request(Method::PUT, url)
            .header(CONTENT_TYPE, "text/vcard; charset=utf-8")
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
        self.send_status_request(request, operation).await
    }

    pub(crate) async fn delete_vcard(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let request = self
            .http
            .request(Method::DELETE, url)
            .headers(self.auth_headers(url, operation).await?);
        self.send_status_request(request, operation).await
    }

    /// A client that only knows its base URL, for tests that exercise
    /// href resolution without touching the network.
    #[cfg(test)]
    pub(crate) fn for_base_url(base_url: &str) -> Self {
        Self::with_transport(base_url, Arc::new(ReqwestDavTransport))
    }

    #[cfg(test)]
    fn with_transport(base_url: &str, transport: Arc<dyn DavTransport>) -> Self {
        Self {
            http: reqwest::Client::new(),
            transport,
            base_url: base_url.trim_end_matches('/').to_string(),
            credentials: CardDavCredentials::bearer("token"),
            trusted_origins: Arc::new(RwLock::new(url_origin(base_url).into_iter().collect())),
        }
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
    ) -> Result<String, AccountError> {
        let method = Method::from_bytes(b"REPORT")
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
    ) -> Result<Option<String>, AccountError> {
        let response = self.send_raw_request(request, operation).await?;
        let etag = response
            .headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(normalize_http_etag);
        if response.status.is_success() {
            Ok(etag)
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
            CardDavCredentials::Basic { username, password } => {
                let credentials = base64::engine::general_purpose::STANDARD
                    .encode(format!("{username}:{password}"));
                if let Ok(value) = HeaderValue::from_str(&format!("Basic {credentials}")) {
                    headers.insert(AUTHORIZATION, value);
                }
            }
            CardDavCredentials::Bearer { token_source } => {
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

    /// Admit a discovered DAV home to the credential-bearing origin set.
    ///
    /// Discovery is server-steered: the address book home comes out of the
    /// principal's own PROPFIND response, so a compromised or hostile server
    /// picks the origin. Two rules bound what it can pick. A discovered
    /// origin must parse, and it must never weaken the transport guarantee
    /// the configured base URL already established - an HTTPS-configured
    /// account never trusts a plaintext discovered home, because that would
    /// turn discovery into a downgrade channel for the account credential. A
    /// cross-origin HTTPS home is still admitted; that is a real deployment
    /// shape, where the principal and the address book home live on different
    /// hosts of the same service.
    fn trust_discovered_url(&self, url: &str) {
        let Some(origin) = url_origin(url) else {
            return;
        };
        if origin_is_secure(&self.base_url) && !origin_is_secure(url) {
            return;
        }
        let mut origins = self.trusted_origins.write().expect("trusted origins lock");
        if !origins.contains(&origin) {
            origins.push(origin);
        }
    }

    fn is_trusted_url(&self, url: &str) -> bool {
        url_origin(url).is_some_and(|origin| {
            self.trusted_origins
                .read()
                .expect("trusted origins lock")
                .contains(&origin)
        })
    }
}

/// Whether a URL's scheme carries an authenticated, encrypted transport.
///
/// Only `https` qualifies; an unparseable URL is treated as insecure so the
/// downgrade check fails closed.
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

fn addressbook_text_query_body(property: &str, query: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<C:addressbook-query xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">\n\
  <D:prop>\n\
    <D:getetag/>\n\
    <C:address-data/>\n\
  </D:prop>\n\
  <C:filter>\n\
    <C:prop-filter name=\"{}\">\n\
      <C:text-match collation=\"i;unicode-casemap\">{}</C:text-match>\n\
    </C:prop-filter>\n\
  </C:filter>\n\
</C:addressbook-query>",
        property,
        escape_xml(query)
    )
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

fn should_fallback_discovery(error: &AccountError) -> bool {
    matches!(
        error.kind(),
        AccountErrorKind::NotFound(ResourceKind::Contact)
    )
}

/// A multi-REPORT fetch: everything that came back usable, plus the
/// recovery classification of any single REPORT that failed wholly.
///
/// Multiget is chunked and text search runs one REPORT per property, so a
/// walk can meet a 401, 403, or 503 on one leg after other legs already
/// returned cards. Aborting the whole call throws those cards away;
/// folding the refusal into anonymous `failed_ids` throws the RECOVERY
/// signal away, and the consumer can no longer tell "reauthorize" from
/// "retry later" from "this resource is gone". So each REPORT is
/// classified where it happens, and the worst class survives to the
/// caller in `degraded`, which the account layer publishes as a
/// `Page::skipped_scopes` entry: the walk did not finish this collection.
pub(crate) struct MultigetFetch {
    pub(crate) report: CardDavMultigetReport,
    pub(crate) degraded: Option<AccountError>,
}

impl MultigetFetch {
    /// A wholly-failed leg with nothing usable anywhere is still a failed
    /// call: there is no partial result to preserve, so it keeps riding the
    /// `Err` arm with its original classification.
    fn settle(
        report: CardDavMultigetReport,
        degraded: Option<AccountError>,
    ) -> Result<Self, AccountError> {
        match degraded {
            Some(error) if report.cards.is_empty() => Err(error),
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

fn multiget_failure(
    report: &CardDavMultigetReport,
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
    .protocol(Protocol::CardDav)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

pub(crate) fn local_error(operation: AccountOperation, message: impl Into<String>) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("carddav"),
            message: Some(DiagnosticText::support_only(message)),
        }),
    )
    .protocol(Protocol::CardDav)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

pub(crate) fn parse_error(operation: AccountOperation, message: impl Into<String>) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::CardDav,
            detail: Some(DiagnosticText::support_only(message)),
        }),
    )
    .protocol(Protocol::CardDav)
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
    .protocol(Protocol::CardDav)
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
        AccountErrorKind::NotFound(ResourceKind::Contact)
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
            resource: Some(ResourceKind::Contact),
        })
    } else if status == StatusCode::NOT_FOUND {
        Cause::Request(RequestCause::NotFound {
            what: ResourceKind::Contact,
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
        .protocol(Protocol::CardDav)
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

pub(crate) fn contact_scope(id: impl Into<String>) -> ErrorScope {
    ErrorScope::Contact { id: id.into() }
}

pub(crate) fn not_found_error(operation: AccountOperation, id: impl Into<String>) -> AccountError {
    status_error(operation, StatusCode::NOT_FOUND, String::new())
        .into_builder()
        .scope(contact_scope(id))
        .try_build()
        .expect("valid account error classification")
}

const PROPFIND_PRINCIPAL: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\">\n\
  <D:prop>\n\
    <D:current-user-principal/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_ADDRESSBOOK_HOME: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">\n\
  <D:prop>\n\
    <C:addressbook-home-set/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_ADDRESSBOOKS: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\" xmlns:CS=\"http://calendarserver.org/ns/\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:displayname/>\n\
    <CS:getctag/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_CTAG: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:CS=\"http://calendarserver.org/ns/\">\n\
  <D:prop>\n\
    <CS:getctag/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_CONTACTS: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
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
    async fn credentials_never_reach_a_resource_href_origin() {
        // Two canned responses, so a neutered guard reaches the transport and
        // fails on the destination assertion below rather than on a starved
        // script - the failure has to name the credential leak.
        let deleted = || DavResponse {
            status: StatusCode::NO_CONTENT,
            headers: HeaderMap::new(),
            body: String::new(),
        };
        let script = ScriptedDavTransport::new([deleted(), deleted()]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CardDavClient::with_transport("https://dav.example.test", transport);

        client
            .delete_vcard(
                "https://dav.example.test/book/one.vcf",
                AccountOperation::ContactDelete,
            )
            .await
            .expect("trusted request succeeds");
        client
            .delete_vcard(
                "https://evil.test/stolen.vcf",
                AccountOperation::ContactDelete,
            )
            .await
            .expect_err("foreign resource origin is rejected");

        let requests = script.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://dav.example.test/book/one.vcf");
        assert_eq!(
            requests[0]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }

    #[tokio::test]
    async fn discovery_falls_back_to_base_after_empty_well_known_response() {
        let response = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
        };
        let script = ScriptedDavTransport::new([
            response("<D:multistatus xmlns:D=\"DAV:\"/>"),
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>",
            ),
            response(
                "<C:addressbook-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:href>/books/ada/</D:href></C:addressbook-home-set>",
            ),
        ]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CardDavClient::with_transport("https://dav.example.test", transport);

        let home = client
            .discover_addressbook_home()
            .await
            .expect("base fallback discovers home");

        assert_eq!(home, "https://dav.example.test/books/ada/");
        let urls = script
            .requests()
            .into_iter()
            .map(|request| request.url)
            .collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec![
                "https://dav.example.test/.well-known/carddav".to_string(),
                "https://dav.example.test/".to_string(),
                "https://dav.example.test/principals/ada/".to_string(),
            ]
        );
    }

    fn discovery_script(home_href: &str) -> Arc<ScriptedDavTransport> {
        let response = |body: String| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body,
        };
        ScriptedDavTransport::new([
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>"
                    .to_string(),
            ),
            response(format!(
                "<C:addressbook-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:href>{home_href}</D:href></C:addressbook-home-set>"
            )),
            response("<D:multistatus xmlns:D=\"DAV:\"/>".to_string()),
        ])
    }

    /// The legitimate deployment the origin allowlist must not break: the
    /// address book home lives on a different host than the principal. It is
    /// discovered over HTTPS, so it is credential-bearing.
    #[tokio::test]
    async fn discovered_cross_origin_https_home_receives_credentials() {
        let script = discovery_script("https://books.example.test/homes/ada/");
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CardDavClient::with_transport("https://dav.example.test", transport);

        let home = client
            .discover_addressbook_home()
            .await
            .expect("cross-origin home is discovered");
        assert_eq!(home, "https://books.example.test/homes/ada/");
        client
            .list_addressbooks(&home)
            .await
            .expect("cross-origin home is credential-bearing");

        let requests = script.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].url, "https://books.example.test/homes/ada/");
        assert_eq!(
            requests[2]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }

    /// Discovery is server-steered, so a discovered home must never weaken the
    /// transport guarantee the configured HTTPS base URL established. The
    /// assertion that matters is the destination: no request at all reaches the
    /// plaintext origin, credential-bearing or otherwise.
    #[tokio::test]
    async fn discovered_plaintext_home_never_receives_credentials() {
        let script = discovery_script("http://books.example.test/homes/ada/");
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CardDavClient::with_transport("https://dav.example.test", transport);

        let home = client
            .discover_addressbook_home()
            .await
            .expect("home href is still reported");
        assert_eq!(home, "http://books.example.test/homes/ada/");
        client
            .list_addressbooks(&home)
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
    async fn fetch_vcards_uses_scripted_report_transcript() {
        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>/book/one.vcf</D:href><D:propstat><D:prop><C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:One\nEND:VCARD</C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
        }]);
        let concrete_transport = Arc::clone(&script);
        let transport: Arc<dyn DavTransport> = concrete_transport;
        let client = CardDavClient::with_transport("https://dav.example.test", transport);

        let fetched = client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &["https://dav.example.test/book/one.vcf".to_string()],
                AccountOperation::ContactsList,
            )
            .await
            .expect("scripted multiget succeeds");

        assert_eq!(
            fetched.report.cards[0].uri,
            "https://dav.example.test/book/one.vcf"
        );
        let requests = script.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].method,
            Method::from_bytes(b"REPORT").expect("REPORT method")
        );
        assert_eq!(requests[0].url, "https://dav.example.test/book/");
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

    /// Shared-shape guard against the CalDAV regression: a non-2xx REPORT must
    /// classify, never decode into an authoritative empty multiget. An empty
    /// result treated as truth is a downstream deletion.
    #[tokio::test]
    async fn unauthorized_report_classifies_as_reauthorization() {
        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::UNAUTHORIZED,
            headers: HeaderMap::new(),
            body: "<html><body>401 Unauthorized</body></html>".to_string(),
        }]);
        let concrete_transport = Arc::clone(&script);
        let transport: Arc<dyn DavTransport> = concrete_transport;
        let client = CardDavClient::with_transport("https://dav.example.test", transport);

        let Err(error) = client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &["https://dav.example.test/book/one.vcf".to_string()],
                AccountOperation::ContactsList,
            )
            .await
        else {
            panic!("401 REPORT must not be reported as an empty result");
        };

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Authentication(
                bifrost_types::AuthErrorKind::ReauthorizationRequired
            )
        );
    }

    #[test]
    fn status_error_maps_write_conflicts() {
        for status in [
            StatusCode::CONFLICT,
            StatusCode::PRECONDITION_FAILED,
            StatusCode::LOCKED,
        ] {
            let error = status_error(AccountOperation::ContactUpdate, status, String::new());
            assert_eq!(error.kind(), &AccountErrorKind::ConcurrencyConflict);
        }
    }

    #[test]
    fn status_error_maps_transient_and_quota_statuses() {
        let rate_limited = status_error(
            AccountOperation::ContactUpdate,
            StatusCode::TOO_MANY_REQUESTS,
            String::new(),
        );
        assert_eq!(
            rate_limited.kind(),
            &AccountErrorKind::Server(ServerErrorKind::RateLimited)
        );

        let unavailable = status_error(
            AccountOperation::ContactUpdate,
            StatusCode::SERVICE_UNAVAILABLE,
            String::new(),
        );
        assert_eq!(
            unavailable.kind(),
            &AccountErrorKind::Server(ServerErrorKind::Unavailable)
        );

        let quota = status_error(
            AccountOperation::ContactUpdate,
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
        let client = CardDavClient::for_base_url("not a url");

        assert_eq!(
            client.resolve_url("addressbook/one.vcf"),
            "not a url/addressbook/one.vcf"
        );
        assert_eq!(
            client.resolve_url("/addressbook/one.vcf"),
            "not a url/addressbook/one.vcf"
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
    fn addressbook_text_query_body_uses_property_text_match() {
        let body = addressbook_text_query_body("EMAIL", "ada & team");

        assert!(body.contains("<C:addressbook-query"));
        assert!(body.contains("<C:address-data/>"));
        assert!(body.contains("<C:prop-filter name=\"EMAIL\">"));
        assert!(body.contains("ada &amp; team"));
    }

    #[test]
    fn addressbook_text_query_escapes_quotes_consistently() {
        let body = addressbook_text_query_body("EMAIL", "Ada's \"team\"");
        assert!(body.contains("Ada&apos;s &quot;team&quot;"));
    }

    #[tokio::test]
    async fn addressbook_multiget_uses_depth_zero() {
        let script = ScriptedDavTransport::new([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"/>".to_string(),
        }]);
        let transport: Arc<dyn DavTransport> = Arc::clone(&script) as Arc<dyn DavTransport>;
        let client = CardDavClient::with_transport("https://dav.example.test", transport);

        client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &["https://dav.example.test/book/opaque-id".to_string()],
                AccountOperation::ContactsList,
            )
            .await
            .expect("empty multistatus is usable");

        assert_eq!(script.requests()[0].headers["Depth"], "0");
    }

    #[test]
    fn weak_etag_is_never_sent_in_if_match() {
        assert_eq!(normalize_http_etag("W/\"abc\""), "W/\"abc\"");
        assert_eq!(prepare_if_match("W/\"abc\""), None);
        assert_eq!(prepare_if_match("abc").as_deref(), Some("\"abc\""));
    }

    #[test]
    fn complete_multiget_failure_uses_embedded_status_classification() {
        let report = CardDavMultigetReport {
            cards: Vec::new(),
            failed: vec![crate::parse::CardDavFailedResource {
                href: "/contacts/one.vcf".to_string(),
                status: Some(401),
            }],
            missing_data: Vec::new(),
        };

        let error = multiget_failure(&report, AccountOperation::ContactsList)
            .expect("all-401 report is a complete failure");
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Authentication(
                bifrost_types::AuthErrorKind::ReauthorizationRequired
            )
        );
    }

    #[test]
    fn accumulated_multiget_success_keeps_a_later_refusal_per_resource() {
        let report = CardDavMultigetReport {
            cards: vec![crate::parse::CardDavFetchedVCard {
                uri: "/contacts/one.vcf".to_string(),
                etag: None,
                data: "BEGIN:VCARD\nEND:VCARD".to_string(),
            }],
            failed: vec![crate::parse::CardDavFailedResource {
                href: "/contacts/two.vcf".to_string(),
                status: Some(401),
            }],
            missing_data: Vec::new(),
        };

        assert!(multiget_failure(&report, AccountOperation::ContactsList).is_none());
    }

    fn usable_report() -> CardDavMultigetReport {
        CardDavMultigetReport {
            cards: vec![crate::parse::CardDavFetchedVCard {
                uri: "/contacts/one.vcf".to_string(),
                etag: None,
                data: "BEGIN:VCARD\nEND:VCARD".to_string(),
            }],
            failed: Vec::new(),
            missing_data: Vec::new(),
        }
    }

    #[test]
    fn a_refused_leg_after_a_usable_one_keeps_the_cards_and_the_recovery_class() {
        let refusal = status_error(
            AccountOperation::ContactSearch,
            StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );

        let fetch = MultigetFetch::settle(usable_report(), Some(refusal))
            .expect("a partial result is not a failed call");

        assert_eq!(fetch.report.cards.len(), 1);
        assert_eq!(
            fetch.degraded.expect("the refusal survives").recovery(),
            &RecoveryClass::AuthLost
        );
    }

    #[test]
    fn a_refused_leg_with_nothing_usable_stays_an_error() {
        let refusal = status_error(
            AccountOperation::ContactSearch,
            StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );

        let error = MultigetFetch::settle(CardDavMultigetReport::default(), Some(refusal))
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
            RecoveryClass::Unsupported(AccountOperation::ContactSearch),
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
                AccountOperation::ContactSearch,
                StatusCode::UNAUTHORIZED,
                "refused".to_string(),
            )
        };
        let transient = || {
            status_error(
                AccountOperation::ContactSearch,
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
