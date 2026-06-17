use std::time::Duration;

use base64::Engine;
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, DiagnosticText,
    ErrorScope, Protocol, ProtocolErrorKind, RequestCause, RequestErrorKind, ResourceKind,
    ServerCause, ServerErrorKind, StateCause, TransmissionState, TransportCause,
    TransportErrorKind, TransportKind, WireCause,
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, ETAG, HeaderMap, HeaderValue};
use reqwest::{Method, StatusCode, Url};

use crate::parse::{
    AddressBookCollection, CardDavContactEntry, CardDavContactListing, CardDavFetchedVCard,
    extract_href_property, parse_addressbook_collections, parse_multiget_report,
    parse_propfind_contacts,
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

#[derive(Debug, Clone)]
pub(crate) struct CardDavClient {
    http: reqwest::Client,
    base_url: String,
    credentials: CardDavCredentials,
}

impl CardDavClient {
    pub(crate) fn new(config: &CardDavConfig) -> Result<Self, AccountError> {
        let http = reqwest::Client::builder()
            .redirect(dav_redirect_policy(&config.base_url))
            .timeout(DAV_CLIENT_TIMEOUT)
            .build()
            .map_err(|error| local_error(AccountOperation::Discover, error.to_string()))?;

        Ok(Self {
            http,
            base_url: config.base_url.trim_end_matches('/').to_string(),
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
                .map(|href| self.resolve_url(&href))
            {
                Some(principal) => {
                    return self.addressbook_home_for_principal(principal).await;
                }
                None => well_known_url,
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
            .map(|href| self.resolve_url(&href))
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
            .map(|href| self.resolve_url(&href))
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
        parse_addressbook_collections(&body)
            .map_err(|error| parse_error(operation, format!("addressbook list: {error}")))
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
        parse_propfind_contacts(&body)
            .map_err(|error| parse_error(operation, format!("contact list: {error}")))
    }

    pub(crate) async fn fetch_vcards(
        &self,
        addressbook_url: &str,
        uris: &[String],
        operation: AccountOperation,
    ) -> Result<Vec<CardDavFetchedVCard>, AccountError> {
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
<C:addressbook-multiget xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">\n\
  <D:prop>\n\
    <D:getetag/>\n\
    <C:address-data/>\n\
  </D:prop>\n\
{href_elements}</C:addressbook-multiget>"
            );
            let response = self.report_raw(addressbook_url, &body, operation).await?;
            let parsed = parse_multiget_report(&response)
                .map_err(|error| parse_error(operation, format!("multiget: {error}")))?;
            all_results.extend(parsed);
        }
        Ok(all_results)
    }

    pub(crate) async fn query_vcards_text(
        &self,
        addressbook_url: &str,
        query: &str,
    ) -> Result<Vec<CardDavFetchedVCard>, AccountError> {
        let mut all_results = Vec::new();
        for property in ["FN", "N", "EMAIL", "TEL", "ADR", "ORG", "TITLE", "NOTE"] {
            let body = addressbook_text_query_body(property, query);
            let response = self
                .report_raw(addressbook_url, &body, AccountOperation::ContactSearch)
                .await?;
            let parsed = parse_multiget_report(&response).map_err(|error| {
                parse_error(AccountOperation::ContactSearch, format!("query: {error}"))
            })?;
            all_results.extend(parsed);
        }
        Ok(all_results)
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
            .headers(self.auth_headers(operation).await?);
        self.send_status_request(request, operation).await
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
    ) -> Result<Option<String>, AccountError> {
        let response = request
            .send()
            .await
            .map_err(|error| transport_error(operation, error.to_string()))?;
        let status = response.status();
        let etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);
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

    /// Build the per-request auth headers. The bearer token is read from
    /// the shared source on every call, so a token rotated mid-sync is
    /// honored on the next DAV request without reopening the account.
    async fn auth_headers(&self, operation: AccountOperation) -> Result<HeaderMap, AccountError> {
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
        AccountErrorKind::NotFound(ResourceKind::Contact)
    )
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

fn status_error(operation: AccountOperation, status: StatusCode, body: String) -> AccountError {
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
        let client = CardDavClient {
            http: reqwest::Client::new(),
            base_url: "not a url".to_string(),
            credentials: CardDavCredentials::bearer("token"),
        };

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
}
