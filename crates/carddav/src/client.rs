use std::fmt;

pub(crate) use bifrost_dav_core::PutCondition;
use bifrost_dav_core::escape_xml;
use bifrost_dav_core::{
    DavDispatch, DavProtocol, DavRequest, normalize_http_etag, prepare_if_match, worse_recovery,
};
use bifrost_net::{AccountId, AccountNet};
use bifrost_types::{
    AccountError, AccountErrorKind, AccountOperation, ErrorScope, RequestErrorKind, ResourceKind,
    ServerErrorKind,
};
use reqwest::header::{CONTENT_TYPE, ETAG};
use reqwest::{Method, StatusCode};

use crate::CardDavConfig;
use crate::parse::{
    AddressBookCollection, CardDavContactListing, CardDavMultigetReport, MultigetOutcome,
    extract_href_property, parse_addressbook_collections, parse_multiget_report,
    parse_propfind_contacts, resolve_href,
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
/// Every shared constructor takes it, so a CardDAV error can never be stamped
/// with a CalDAV `Protocol` or name a `ResourceKind::Calendar`.
const DAV: DavProtocol = DavProtocol::CardDav;

#[derive(Clone)]
pub(crate) struct CardDavClient {
    /// Transport, credentials, origin gate and the generic WebDAV verbs, all
    /// shared with `bifrost-caldav` through `bifrost-dav-core`.
    dav: DavDispatch,
}

impl fmt::Debug for CardDavClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardDavClient")
            .field("dav", &self.dav)
            .finish_non_exhaustive()
    }
}

impl CardDavClient {
    pub(crate) fn new(account_id: AccountId, config: &CardDavConfig) -> Self {
        Self {
            dav: DavDispatch::new(
                account_id,
                &config.base_url,
                config.credentials.to_shared(),
                DAV,
            ),
        }
    }

    /// The account handle carrying this account's meter, priority and cap.
    pub(crate) fn net(&self) -> &AccountNet {
        self.dav.net()
    }

    #[cfg(test)]
    pub(crate) fn with_account_net(base_url: &str, net: AccountNet) -> Self {
        Self {
            dav: DavDispatch::with_account_net(
                net,
                base_url,
                crate::CardDavCredentials::bearer("token").to_shared(),
                DAV,
            ),
        }
    }

    pub(crate) async fn discover_addressbook_home(&self) -> Result<String, AccountError> {
        // Well-known discovery lives at the ORIGIN root (RFC 6764), so a
        // configured base carrying a path must not have the well-known
        // suffix appended to it: the resulting URL is not a discovery
        // endpoint, and a deployment answering it with 401/403 rather
        // than 404 would fail the open before the configured base was
        // ever tried.
        let well_known_url = bifrost_net::url::well_known_url(self.dav.base_url(), "carddav");
        let dav_root = match well_known_url {
            None => self.dav.base_url().to_string(),
            Some(well_known_url) => match self
                .dav
                .propfind_raw(
                    &well_known_url,
                    "0",
                    PROPFIND_PRINCIPAL,
                    AccountOperation::Discover,
                )
                .await
            {
                Ok(response) => {
                    match extract_href_property(&response.text, "current-user-principal")
                        .map_err(|error| parse_error(AccountOperation::Discover, error))?
                        .map(|href| resolve_href(&response.url, &href))
                    {
                        Some(principal) => {
                            return self.addressbook_home_for_principal(principal).await;
                        }
                        None => self.dav.base_url().to_string(),
                    }
                }
                Err(error) if should_fallback_discovery(&error) => self.dav.base_url().to_string(),
                Err(error) => return Err(error),
            },
        };

        let response = self
            .dav
            .propfind_raw(
                &dav_root,
                "0",
                PROPFIND_PRINCIPAL,
                AccountOperation::Discover,
            )
            .await?;
        let principal = extract_href_property(&response.text, "current-user-principal")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?
            .map(|href| resolve_href(&response.url, &href))
            .ok_or_else(|| {
                parse_error(AccountOperation::Discover, "missing current-user-principal")
            })?;
        self.addressbook_home_for_principal(principal).await
    }

    async fn addressbook_home_for_principal(
        &self,
        principal: String,
    ) -> Result<String, AccountError> {
        let response = self
            .dav
            .propfind_raw(
                &principal,
                "0",
                PROPFIND_ADDRESSBOOK_HOME,
                AccountOperation::Discover,
            )
            .await?;
        extract_href_property(&response.text, "addressbook-home-set")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?
            .map(|href| resolve_href(&response.url, &href))
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
        let response = self
            .dav
            .propfind_raw(home_url, "1", PROPFIND_ADDRESSBOOKS, operation)
            .await?;
        let mut collections = parse_addressbook_collections(&response.text)
            .map_err(|error| parse_error(operation, format!("addressbook list: {error}")))?;
        for collection in &mut collections {
            collection.resolve_href(&response.url);
        }
        Ok(collections)
    }

    /// Cheap depth-0 PROPFIND for the collection `getctag`. Returns
    /// `None` when the server omits it, so the caller falls through to a
    /// full snapshot + diff (brick 8 ctag short-circuit).
    pub(crate) async fn collection_ctag(
        &self,
        addressbook_url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let response = self
            .dav
            .propfind_raw(addressbook_url, "0", PROPFIND_CTAG, operation)
            .await?;
        crate::parse::parse_collection_ctag(&response.text)
            .map_err(|error| parse_error(operation, format!("collection ctag: {error}")))
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
        let response = self
            .dav
            .propfind_raw(addressbook_url, "1", PROPFIND_CONTACTS, operation)
            .await?;
        let mut listing = parse_propfind_contacts(&response.text)
            .map_err(|error| parse_error(operation, format!("contact list: {error}")))?;
        listing.resolve_hrefs(&response.url);
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
<C:addressbook-multiget xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
    <C:address-data/>\n\
  </D:prop>\n\
{href_elements}</C:addressbook-multiget>"
                );
                body
            })
            .collect::<Vec<_>>();
        let legs: Vec<_> = bodies
            .iter()
            .map(|body| {
                self.run_leg(MultigetLeg {
                    url: addressbook_url,
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
    ///
    /// The CalDAV twin of this function must stay in step with it.
    async fn accumulate_leg(
        &self,
        leg: MultigetLeg<'_>,
        all_results: &mut CardDavMultigetReport,
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

    async fn run_leg(&self, leg: MultigetLeg<'_>) -> (CardDavMultigetReport, Option<AccountError>) {
        let mut report = CardDavMultigetReport::default();
        let mut degraded = None;
        self.accumulate_leg(leg, &mut report, &mut degraded).await;
        (report, degraded)
    }

    pub(crate) async fn query_vcards_text(
        &self,
        addressbook_url: &str,
        query: &str,
    ) -> Result<MultigetFetch, AccountError> {
        let mut all_results = CardDavMultigetReport::default();
        let mut degraded = None;
        let bodies = ["FN", "N", "EMAIL", "TEL", "ADR", "ORG", "TITLE", "NOTE"]
            .map(|property| addressbook_text_query_body(property, query));
        let legs: Vec<_> = bodies
            .iter()
            .map(|body| {
                self.run_leg(MultigetLeg {
                    url: addressbook_url,
                    depth: "1",
                    body,
                    operation: AccountOperation::ContactSearch,
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

    pub(crate) async fn put_vcard(
        &self,
        url: &str,
        body: String,
        condition: PutCondition<'_>,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let mut request = self
            .dav
            .request(Method::PUT, url)
            .header(CONTENT_TYPE, "text/vcard; charset=utf-8")
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
        self.send_status_request(request, operation).await
    }

    /// WebDAV `MOVE` of one resource into another collection.
    ///
    /// Twin of `bifrost-caldav`'s `move_resource`; keep them in step. See there
    /// for why `Overwrite: F`, why the destination is credential-gated, and why
    /// only 405/501 are fallback triggers.
    pub(crate) async fn move_resource(
        &self,
        from: &str,
        to: &str,
        operation: AccountOperation,
    ) -> Result<bool, AccountError> {
        self.dav.move_resource(from, to, operation).await
    }

    pub(crate) async fn delete_vcard(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let request = self
            .dav
            .request(Method::DELETE, url)
            .headers(self.dav.auth_headers(url, operation).await?);
        self.send_status_request(request, operation).await
    }

    /// A status-only request that also hands back the response ETag.
    ///
    /// Deliberately NOT `DavDispatch::send_status_request`, which discards the
    /// response. This is a real divergence from `bifrost-caldav` rather than
    /// drift: CardDAV's `put_vcard` and `delete_vcard` return the new validator
    /// to their callers, and CalDAV's equivalents return `()`. Collapsing the
    /// two during the dav-core extraction would have silently dropped the etag
    /// every CardDAV write reports.
    async fn send_status_request(
        &self,
        request: DavRequest,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let response = self.dav.send_raw_request(request, operation).await?;
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

    pub(crate) fn resolve_url(&self, href: &str) -> String {
        self.dav.resolve_url(href)
    }

    pub(crate) fn admit_discovered_urls(&mut self, urls: impl IntoIterator<Item = String>) {
        self.dav.admit_discovered_urls(urls);
    }
}

/// Whether a URL's scheme carries an authenticated, encrypted transport.
///
/// Only `https` qualifies; an unparseable URL is treated as insecure so the
/// downgrade check fails closed.
fn addressbook_text_query_body(property: &str, query: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<C:addressbook-query xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
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

/// Does this well-known PROBE failure mean "this is not a discovery endpoint"?
///
/// Twin of `bifrost-caldav::should_fallback_discovery`, which carries the full
/// reasoning: applied to the `/.well-known/carddav` attempt only; 401/403 still
/// fail the open, while 404, a 405 from a static site or proxy sitting on the
/// origin root, and a locally-refused cross-origin redirect (RFC 6764's
/// canonical shape, which the credential-origin gate cannot admit before
/// discovery has authenticated anything) all mean the probe found no discovery
/// endpoint and the configured base URL should be tried.
fn should_fallback_discovery(error: &AccountError) -> bool {
    matches!(
        error.kind(),
        AccountErrorKind::NotFound(ResourceKind::Contact)
            | AccountErrorKind::Request(RequestErrorKind::Malformed)
            | AccountErrorKind::Server(ServerErrorKind::Error { status: Some(405) })
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
    pub(crate) report: CardDavMultigetReport,
    pub(crate) degraded: Option<AccountError>,
}

impl MultigetFetch {
    /// A wholly-failed fetch with nothing usable anywhere is still a failed
    /// call: there is no partial result to preserve, so it keeps riding the
    /// `Err` arm with its original classification. "Nothing usable" means no
    /// observations of ANY kind - a successful leg that reported all its
    /// resources 404 (genuine deletions in `failed`) or data-less
    /// (`missing_data`) has still answered for those resources, and a
    /// degraded sibling leg must not throw those verdicts away and force the
    /// caller to re-walk them.
    fn settle(
        report: CardDavMultigetReport,
        degraded: Option<AccountError>,
    ) -> Result<Self, AccountError> {
        let no_observations =
            report.cards.is_empty() && report.failed.is_empty() && report.missing_data.is_empty();
        match degraded {
            Some(error) if no_observations => Err(error),
            degraded => Ok(Self { report, degraded }),
        }
    }
}

/// Keep whichever failure demands the more drastic recovery, so a 503 on
/// one chunk cannot hide a 401 on another.
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
    bifrost_dav_core::unsupported_error(operation, DAV)
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
    status: reqwest::StatusCode,
    body: String,
) -> AccountError {
    bifrost_dav_core::status_error(operation, status, body, DAV)
}

pub(crate) fn contact_scope(id: impl Into<String>) -> ErrorScope {
    ErrorScope::Contact {
        id: (id.into()).into(),
    }
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
    <D:current-user-privilege-set/>\n\
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

    use bifrost_types::{Protocol, ProtocolErrorKind, RecoveryClass};

    /// Every error this crate mints is stamped CardDAV, and names contacts.
    ///
    /// The shared ladder in `bifrost-dav-core` is parameterized by `DAV`, so a
    /// wrong binding here would silently restamp the whole crate's error
    /// surface as CalDAV - a consumer routing on `Protocol` or `ResourceKind`
    /// would misfile every failure. Ablating `DAV` to `CalDav` before this
    /// existed failed exactly one unrelated test, which is not the coverage
    /// that mistake deserves.
    #[test]
    fn every_error_this_crate_mints_is_stamped_carddav() {
        let errors = [
            status_error(
                AccountOperation::ContactGet,
                StatusCode::NOT_FOUND,
                String::new(),
            ),
            local_error(AccountOperation::ContactGet, "bad"),
            parse_error(AccountOperation::ContactGet, "bad"),
            transport_error(AccountOperation::ContactGet, "bad"),
            unsupported_error(AccountOperation::ContactGet),
            not_found_error(AccountOperation::ContactGet, "one.vcf"),
        ];
        for error in errors {
            assert_eq!(
                error.protocol(),
                Some(Protocol::CardDav),
                "a CardDAV error must not be stamped otherwise: {error:?}"
            );
        }
        let missing = status_error(
            AccountOperation::ContactGet,
            StatusCode::NOT_FOUND,
            String::new(),
        );
        assert!(
            matches!(
                missing.kind(),
                AccountErrorKind::NotFound(ResourceKind::Contact)
            ),
            "a CardDAV 404 names a contact, not a calendar: {missing:?}"
        );
    }
    use bifrost_dav_core::DavResponse;
    use bifrost_dav_core::test_support::{
        dav_redirect, dav_retried, dav_script, dav_script_empty, dav_script_yielding,
        scripted_dav_net, transcripts,
    };
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use std::sync::Arc;

    use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

    #[tokio::test]
    async fn credentials_never_reach_a_resource_href_origin() {
        // Two canned responses, so a neutered guard reaches the transport and
        // fails on the destination assertion below rather than on a starved
        // script - the failure has to name the credential leak.
        let deleted = || DavResponse {
            status: StatusCode::NO_CONTENT,
            headers: HeaderMap::new(),
            body: String::new(),
            url: String::new(),
        };
        let script = dav_script([deleted(), deleted()]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

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

        let requests = transcripts(&script);
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

    /// A poll-path snapshot spends the depth-0 ctag request and the depth-1
    /// contact listing, and never re-lists the address book home.
    ///
    /// `CtagSource::Known` is what makes that true: the poll already asked the
    /// collection for its ctag in order to decide whether to short-circuit, so
    /// re-deriving the same value from a depth-1 PROPFIND over the home cost a
    /// third round trip - one that grows with the number of address books
    /// rather than staying a single collection wide. CalDAV's `event_snapshot`
    /// has taken the cheap path for a while; this is the CardDAV twin catching
    /// up, and the assertion is written against the transcript so the two
    /// cannot drift back apart silently.
    #[tokio::test]
    async fn poll_snapshot_never_relists_the_address_book_home() {
        let multistatus = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        // Only two responses are scripted. A regression that re-lists the home
        // starves the script and fails loudly here rather than silently
        // spending a third request.
        let script = dav_script([multistatus(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
  <D:response>
    <D:href>/books/ada/personal/</D:href>
    <D:propstat>
      <D:prop><CS:getctag>ctag-7</CS:getctag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#,
        )]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let ctag = client
            .collection_ctag(
                "https://dav.example.test/books/ada/personal/",
                AccountOperation::SyncChanges,
            )
            .await
            .expect("ctag resolves");
        assert_eq!(ctag.as_deref(), Some("ctag-7"));

        let script = dav_script([multistatus(
            r#"<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/books/ada/personal/one.vcf</D:href>
    <D:propstat>
      <D:prop><D:getetag>"etag-1"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#,
        )]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let snapshot = crate::account::CardDavAccount::contact_snapshot(
            &client,
            crate::account::CtagSource::Known(ctag),
            "https://dav.example.test/books/ada/personal/",
            AccountOperation::SyncChanges,
        )
        .await
        .expect("snapshot");

        // The known ctag survived into the snapshot without being refetched.
        assert_eq!(snapshot.ctag.as_deref(), Some("ctag-7"));

        let requests = transcripts(&script);
        assert_eq!(
            requests.len(),
            1,
            "a Known ctag must cost the listing request and nothing else"
        );
        assert_eq!(
            requests[0].url, "https://dav.example.test/books/ada/personal/",
            "the one request addresses the collection, never the home"
        );
        assert_eq!(
            requests[0]
                .headers
                .get("depth")
                .and_then(|value| value.to_str().ok()),
            Some("1"),
            "the listing is the depth-1 request"
        );
    }

    /// An empty address book home lists NOTHING - no fabricated placeholder.
    ///
    /// The phantom this pins the absence of pointed at the home itself and
    /// advertised `can_create_contacts: true`, so a consumer that trusted it and
    /// POSTed a vCard to the home URL got a 404 or 405 from a spec-correct
    /// server. An empty list also lets a consumer tell a genuinely empty backend
    /// from a real single book, and so reap stale ones.
    ///
    /// `bifrost-caldav::calendars_list` removed the identical shape for the
    /// identical reasons, and the two crates drifted on it for a long time
    /// precisely because neither side pinned it. Both are pinned now; keep them
    /// in step.
    #[tokio::test]
    async fn an_empty_home_lists_no_address_books_rather_than_a_phantom() {
        use bifrost_types::account::Account as _;

        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"/>".to_string(),
            url: String::new(),
        }]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/ada/",
        );

        let books = account.address_books_list().await.expect("list");
        assert!(
            books.is_empty(),
            "an empty home must not fabricate an address book: {books:?}"
        );
    }

    /// A discovery that enumerates no address books leaves the OPENED account
    /// with no default, rather than the addressbook home standing in for one.
    ///
    /// Drives the real discovery-to-account path, so it bites where a pure test
    /// of the selection helper cannot: reintroducing the old
    /// `unwrap_or_else(|| resolve_url(&home))` at the `open` call site is
    /// invisible to a test that constructs the `None` itself. Twin of the
    /// CalDAV assertion; keep them in step.
    #[tokio::test]
    async fn an_empty_discovery_opens_an_account_with_no_default_address_book() {
        let script = discovery_script("/books/ada/");
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let account = crate::account::CardDavAccount::open_with_client(client)
            .await
            .expect("discovery succeeds against an empty backend");

        assert_eq!(
            account.default_addressbook_url, None,
            "an empty addressbook home must leave no default, not the home itself"
        );
        assert!(
            account.addressbook_urls.is_empty(),
            "an empty addressbook home advertises no collections"
        );
    }

    /// An empty backend refuses a collection-less call locally instead of
    /// addressing the addressbook home.
    ///
    /// Twin of `bifrost-caldav`'s
    /// `an_empty_backend_refuses_collection_less_calls_before_the_wire`; keep
    /// them in step. The home is not a collection when the walk came back empty
    /// (`list_addressbooks` returns the home itself when it genuinely is one),
    /// so the old `unwrap_or_else(|| resolve_url(&home))` default sent every one
    /// of these to a resource a spec-correct server 404s - reporting a local
    /// routing failure as a remote `NotFound`. The empty script is the bite:
    /// restore the fallback and these calls reach the transport and panic on
    /// exhaustion rather than failing quietly.
    #[tokio::test]
    async fn an_empty_backend_refuses_collection_less_calls_before_the_wire() {
        use bifrost_types::account::Account as _;

        let script = dav_script_empty();
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests_without_collections(
            client,
            "https://dav.example.test/books/",
        );

        account
            .contacts_list(None, None)
            .await
            .expect_err("a listing naming no address book has nothing to list");
        account
            .contact_create(bifrost_types::ContactCreate::default())
            .await
            .expect_err("a create naming no address book has nowhere to write");
        // Only the doors taking an `Option<AddressBookId>` route through the
        // default. `contact_get` / `contact_update` derive the collection from
        // the resource's own URL, and `bifrost_net::url::parent_collection_url`
        // answers for every id that resolves absolute, so their fallback is
        // unreachable in practice.

        assert!(
            transcripts(&script).is_empty(),
            "an unroutable call must reach no transport at all"
        );
    }

    #[tokio::test]
    async fn discovery_falls_back_to_base_after_empty_well_known_response() {
        let response = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let script = dav_script([
            response("<D:multistatus xmlns:D=\"DAV:\"/>"),
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>",
            ),
            response(
                "<C:addressbook-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:href>/books/ada/</D:href></C:addressbook-home-set>",
            ),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let home = client
            .discover_addressbook_home()
            .await
            .expect("base fallback discovers home");

        assert_eq!(home, "https://dav.example.test/books/ada/");
        let urls = transcripts(&script)
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
        let script = dav_script([
            response("<D:multistatus xmlns:D=\"DAV:\"/>"),
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>",
            ),
            response(
                "<C:addressbook-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:href>/books/ada/</D:href></C:addressbook-home-set>",
            ),
        ]);
        let client = CardDavClient::with_account_net(
            "https://dav.example.test/service",
            scripted_dav_net(&script),
        );

        let home = client
            .discover_addressbook_home()
            .await
            .expect("base fallback discovers home");

        assert_eq!(home, "https://dav.example.test/books/ada/");
        let urls = transcripts(&script)
            .into_iter()
            .map(|request| request.url)
            .collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec![
                "https://dav.example.test/.well-known/carddav".to_string(),
                "https://dav.example.test/service".to_string(),
                "https://dav.example.test/principals/ada/".to_string(),
            ],
            "the well-known probe is rooted at the origin, and only the fallback uses the configured path"
        );
    }

    fn discovery_script(home_href: &str) -> Arc<ScriptedDispatch> {
        let response = |body: String| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body,
            url: String::new(),
        };
        dav_script([
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
        let mut client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let home = client
            .discover_addressbook_home()
            .await
            .expect("cross-origin home is discovered");
        assert_eq!(home, "https://books.example.test/homes/ada/");
        client.admit_discovered_urls(std::iter::once(home.clone()));
        client
            .list_addressbooks(&home)
            .await
            .expect("cross-origin home is credential-bearing");

        let requests = transcripts(&script);
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
            HeaderValue::from_static("https://books.example.test/dav/homes/ada/"),
        );
        let script = dav_script([
            DavResponse {
                status: StatusCode::MOVED_PERMANENTLY,
                headers: redirect_headers,
                body: String::new(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>team/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:addressbook/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
                url: String::new(),
            },
        ]);
        let mut client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        client.admit_discovered_urls(std::iter::once(
            "https://books.example.test/homes/ada/".to_string(),
        ));

        let books = client
            .list_addressbooks("https://dav.example.test/books/ada/")
            .await
            .expect("cross-origin redirect is followed with credentials");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].url, "https://books.example.test/dav/homes/ada/");
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
            books[0].href,
            "https://books.example.test/dav/homes/ada/team/"
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
        let script = dav_script([DavResponse {
            status: StatusCode::FOUND,
            headers: redirect_headers,
            body: String::new(),
            url: String::new(),
        }]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .list_addressbooks("https://dav.example.test/books/ada/")
            .await
            .expect_err("an unadmitted redirect target is refused");

        assert_eq!(transcripts(&script).len(), 1);
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
        let script = dav_script(responses);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .delete_vcard(
                "https://dav.example.test/start",
                AccountOperation::ContactDelete,
            )
            .await
            .expect("the configured number of redirects is allowed");

        assert_eq!(transcripts(&script).len(), max_hops + 1);
    }

    /// RFC 4918 resolves a relative href against the EFFECTIVE request URI.
    /// The dispatcher follows same-origin hops, so a PROPFIND submitted to
    /// `/addressbook` can be served from `/dav/users/ada/addressbook/`.
    /// Resolving `one.vcf` against the submitted URI mints `/one.vcf` - a
    /// native id that does not exist, and a follow-up GET that 404s.
    ///
    /// Twin of bifrost-caldav's. The hop is scripted as the 301 it is, so the
    /// walk that produces the effective URI is on the path under test.
    #[tokio::test]
    async fn contact_hrefs_resolve_against_the_post_redirect_url() {
        let script = dav_script([
            dav_redirect(
                StatusCode::MOVED_PERMANENTLY,
                "https://dav.example.test/dav/users/ada/addressbook/",
            ),
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>one.vcf</D:href><D:propstat><D:prop><D:getetag>\"e1\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
                url: String::new(),
            }
            .into(),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let listing = client
            .list_contacts_listing(
                "https://dav.example.test/addressbook",
                AccountOperation::ContactsList,
            )
            .await
            .expect("redirected listing succeeds");

        assert_eq!(
            listing.entries[0].uri,
            "https://dav.example.test/dav/users/ada/addressbook/one.vcf"
        );
    }

    /// Discovery is server-steered, so a discovered home must never weaken the
    /// transport guarantee the configured HTTPS base URL established. The
    /// assertion that matters is the destination: no request at all reaches the
    /// plaintext origin, credential-bearing or otherwise.
    #[tokio::test]
    async fn discovered_plaintext_home_never_receives_credentials() {
        let script = discovery_script("http://books.example.test/homes/ada/");
        let mut client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let home = client
            .discover_addressbook_home()
            .await
            .expect("home href is still reported");
        assert_eq!(home, "http://books.example.test/homes/ada/");
        client.admit_discovered_urls(std::iter::once(home.clone()));
        client
            .list_addressbooks(&home)
            .await
            .expect_err("a downgraded discovered origin is refused");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.url.starts_with("https://dav.example.test/")),
            "no request reached the plaintext origin: {:?}",
            requests.iter().map(|r| &r.url).collect::<Vec<_>>()
        );
    }

    /// A cross-address-book `contact_update` MOVES the resource, and a restated
    /// address book still updates in place.
    ///
    /// Twin of `bifrost-caldav`'s
    /// `event_update_moves_across_calendars_and_updates_in_place_otherwise`;
    /// keep them in step. This crate refused the relocation before dav-B11 and,
    /// unlike its CalDAV twin, never pinned the refusal - which is why the
    /// behaviour change here broke no test. The pair is pinned now.
    #[tokio::test]
    async fn contact_update_moves_across_address_books_and_updates_in_place_otherwise() {
        use bifrost_types::account::Account as _;

        let multiget = |href: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: format!(
                "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>{href}</D:href><D:propstat><D:prop><C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:One\nEND:VCARD</C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"
            ),
            url: String::new(),
        };
        let contact =
            || bifrost_types::ContactId("https://dav.example.test/books/work/one.vcf".to_string());
        let patch_to = |book: &str| bifrost_types::ContactPatch {
            address_book_id: Some(bifrost_types::AddressBookId(book.to_string())),
            ..Default::default()
        };

        // A move: multiget the current resource, then MOVE it.
        let script = dav_script([
            multiget("/books/work/one.vcf"),
            DavResponse {
                status: StatusCode::CREATED,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            },
        ]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/work/",
        );
        account
            .contact_update(contact(), patch_to("https://dav.example.test/books/home/"))
            .await
            .expect("a move between address books is performed");
        let requests = transcripts(&script);
        assert_eq!(
            requests.len(),
            2,
            "a move-only patch is a REPORT and a MOVE, with no content write"
        );
        assert_eq!(requests[1].method.as_str(), "MOVE");
        assert_eq!(
            requests[1]
                .headers
                .get("Destination")
                .and_then(|value| value.to_str().ok()),
            Some("https://dav.example.test/books/home/one.vcf"),
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

        // Restating the contact's own address book is not a move.
        let script = dav_script([
            multiget("/books/work/one.vcf"),
            DavResponse {
                status: StatusCode::NO_CONTENT,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            },
        ]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/work/",
        );
        account
            .contact_update(contact(), patch_to("https://dav.example.test/books/work/"))
            .await
            .expect("restating the current address book is not a move");
        let methods = transcripts(&script)
            .into_iter()
            .map(|request| request.method.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(methods, vec!["REPORT", "PUT"]);
    }

    /// A server without MOVE falls back to copy-then-delete, and a failure of
    /// the delete leg reports the move as HALF applied. Twin of the CalDAV
    /// assertion; keep them in step.
    #[tokio::test]
    async fn a_contact_move_without_server_move_support_copies_then_deletes() {
        use bifrost_types::account::Account as _;

        let response = |status: StatusCode, body: &str| DavResponse {
            status,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let script = dav_script([
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>/books/work/one.vcf</D:href><D:propstat><D:prop><C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:One\nEND:VCARD</C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
                url: String::new(),
            },
            response(StatusCode::METHOD_NOT_ALLOWED, ""),
            response(StatusCode::CREATED, ""),
        ]
        .into_iter()
        .map(Canned::from)
        // The DELETE of the original 500s, and a 500 is now retried to
        // exhaustion before it surfaces.
        .chain(dav_retried(response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "boom",
        )))
        .collect::<Vec<_>>());
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/work/",
        );

        let error = account
            .contact_update(
                bifrost_types::ContactId("https://dav.example.test/books/work/one.vcf".to_string()),
                bifrost_types::ContactPatch {
                    address_book_id: Some(bifrost_types::AddressBookId(
                        "https://dav.example.test/books/home/".to_string(),
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
            "a copied-but-not-removed contact is a partial response: {error:?}"
        );
        let methods = transcripts(&script)
            .into_iter()
            .map(|request| request.method.as_str().to_string())
            .collect::<Vec<_>>();
        // The trailing DELETEs are the retry budget being spent on the 500.
        assert_eq!(
            methods,
            vec!["REPORT", "MOVE", "PUT", "DELETE", "DELETE", "DELETE"]
        );
    }

    #[tokio::test]
    async fn fetch_vcards_uses_scripted_report_transcript() {
        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>/book/one.vcf</D:href><D:propstat><D:prop><C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:One\nEND:VCARD</C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: String::new(),
        }]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

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
        let requests = transcripts(&script);
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

    /// Multiget fan-out is bounded by `MULTIGET_LEG_CONCURRENCY`, not by the
    /// caller's uri list: one large address book must not open hundreds of
    /// simultaneous REPORTs, and no layer below this one bounds them.
    ///
    /// Twin of bifrost-caldav's bound. The probe sits at the wire: the yielding
    /// script raises its in-flight count for each outstanding dispatch, so the
    /// mark measures what the net pipeline holds open.
    #[tokio::test]
    async fn multiget_never_holds_more_legs_open_than_the_concurrency_bound() {
        let chunks = 10;
        let script = dav_script_yielding((0..chunks).map(|_| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"></D:multistatus>".to_string(),
            url: String::new(),
        }));
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let uris = (0..MULTIGET_BATCH_SIZE * chunks)
            .map(|index| format!("https://dav.example.test/book/{index}.vcf"))
            .collect::<Vec<_>>();

        client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &uris,
                AccountOperation::ContactsList,
            )
            .await
            .expect("empty multistatus legs are usable");

        assert_eq!(script.peak_in_flight(), MULTIGET_LEG_CONCURRENCY);
    }

    #[tokio::test]
    async fn chunked_multiget_keeps_prior_chunk_when_later_http_leg_fails() {
        let good = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>/book/one.vcf</D:href><D:propstat><D:prop><C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:One\nEND:VCARD</C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/book/".to_string(),
        };
        let refused = DavResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            headers: HeaderMap::new(),
            body: String::new(),
            url: String::new(),
        };
        // The refused leg burns the whole retry budget before it degrades.
        let script = dav_script(
            std::iter::once(Canned::from(good))
                .chain(dav_retried(refused))
                .collect::<Vec<_>>(),
        );
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let uris = (0..=MULTIGET_BATCH_SIZE)
            .map(|index| format!("https://dav.example.test/book/{index}.vcf"))
            .collect::<Vec<_>>();

        let fetched = client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &uris,
                AccountOperation::ContactsList,
            )
            .await
            .expect("the usable first chunk survives");

        assert_eq!(fetched.report.cards.len(), 1);
        assert!(fetched.degraded.is_some());
    }

    /// The adjacent leg-failure path, mirroring the CalDAV twin: a later chunk
    /// whose body will not parse must degrade like a later chunk that returned
    /// 503, not discard the chunks that already materialized.
    #[tokio::test]
    async fn chunked_multiget_keeps_prior_chunk_when_later_body_is_malformed() {
        let good = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>/book/one.vcf</D:href><D:propstat><D:prop><C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:One\nEND:VCARD</C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/book/".to_string(),
        };
        let malformed = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"><D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/book/".to_string(),
        };
        let script = dav_script([good, malformed]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let uris = (0..=MULTIGET_BATCH_SIZE)
            .map(|index| format!("https://dav.example.test/book/{index}.vcf"))
            .collect::<Vec<_>>();

        let fetched = client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &uris,
                AccountOperation::ContactsList,
            )
            .await
            .expect("the usable first chunk survives a malformed later chunk");

        assert_eq!(fetched.report.cards.len(), 1);
        assert!(fetched.degraded.is_some());
    }

    /// Nothing usable anywhere is still a failed call, malformed or not.
    #[tokio::test]
    async fn an_only_leg_that_will_not_parse_is_still_an_error() {
        let malformed = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"><D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/book/".to_string(),
        };
        let script = dav_script([malformed]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let outcome = client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &["https://dav.example.test/book/one.vcf".to_string()],
                AccountOperation::ContactsList,
            )
            .await
            .map(|fetched| fetched.report.cards.len());
        let Err(error) = outcome else {
            panic!("no usable result anywhere must stay an error");
        };
        assert_eq!(error.operation(), Some(AccountOperation::ContactsList));
    }

    /// Shared-shape guard against the CalDAV regression: a non-2xx REPORT must
    /// classify, never decode into an authoritative empty multiget. An empty
    /// result treated as truth is a downstream deletion.
    #[tokio::test]
    async fn unauthorized_report_classifies_as_reauthorization() {
        let script = dav_script([DavResponse {
            status: StatusCode::UNAUTHORIZED,
            headers: HeaderMap::new(),
            body: "<html><body>401 Unauthorized</body></html>".to_string(),
            url: String::new(),
        }]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

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
    fn resolve_url_fallback_preserves_separator() {
        let client =
            CardDavClient::with_account_net("not a url", scripted_dav_net(&dav_script_empty()));

        assert_eq!(
            client.resolve_url("addressbook/one.vcf"),
            "not a url/addressbook/one.vcf"
        );
        assert_eq!(
            client.resolve_url("/addressbook/one.vcf"),
            "not a url/addressbook/one.vcf"
        );
    }

    /// Twin of the CalDAV test: fall back on "not a discovery endpoint"
    /// answers, never on a credential refusal.
    #[test]
    fn discovery_falls_back_on_not_found_405_and_a_refused_redirect() {
        let status = |status| status_error(AccountOperation::Discover, status, String::new());

        assert!(!should_fallback_discovery(&status(
            StatusCode::UNAUTHORIZED
        )));
        assert!(!should_fallback_discovery(&status(StatusCode::FORBIDDEN)));
        assert!(!should_fallback_discovery(&status(
            StatusCode::INTERNAL_SERVER_ERROR
        )));

        assert!(should_fallback_discovery(&status(StatusCode::NOT_FOUND)));
        assert!(should_fallback_discovery(&status(
            StatusCode::METHOD_NOT_ALLOWED
        )));
        assert!(should_fallback_discovery(&local_error(
            AccountOperation::Discover,
            "redirect to an unadmitted origin",
        )));
    }

    #[test]
    fn addressbook_text_query_body_uses_property_text_match() {
        let body = addressbook_text_query_body("EMAIL", "ada & team");

        assert!(body.contains("<C:addressbook-query"));
        assert!(body.contains("<D:resourcetype/>"));
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
        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"/>".to_string(),
            url: String::new(),
        }]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &["https://dav.example.test/book/opaque-id".to_string()],
                AccountOperation::ContactsList,
            )
            .await
            .expect("empty multistatus is usable");

        let requests = transcripts(&script);
        assert_eq!(requests[0].headers["Depth"], "0");
        assert!(requests[0].body.contains("<D:resourcetype/>"));
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
}
