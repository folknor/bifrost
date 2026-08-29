//! The DAV request record, response shape, and origin comparison.
//!
//! DAV traffic rides `bifrost-net` like every other HTTP protocol crate, so the
//! retry budget, per-host rate limiting, bandwidth metering and observability
//! are the shared ones rather than a second implementation. What DAV keeps for
//! itself is the credential-origin gate and the redirect walk that enforces it:
//! `AccountNet` strips `Authorization` on a cross-origin hop with no way to
//! restore it, and its trusted-host allowlist compares hosts where the DAV gate
//! compares scheme, host and effective port. Redirects are therefore disabled on
//! the account spec and walked here, one hop at a time, each with credentials
//! minted for the origin actually being addressed.

use std::time::Duration;

use bifrost_types::{AccountError, AccountOperation};
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url};

use crate::error::DavProtocol;

/// Wall-clock ceiling on a single DAV request.
pub const DAV_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct DavResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: String,
    /// Effective request URI, after any redirects the walk followed.
    ///
    /// RFC 4918 relative hrefs in a Multi-Status resolve against the effective
    /// request URI, not the URI the caller submitted. The DAV redirect walk
    /// permits same-origin hops, so a PROPFIND on `/calendar` that lands on
    /// `/dav/users/ada/calendar/` is a real deployment shape; resolving
    /// `one.ics` against the submitted URI there mints a wrong native id and a
    /// wrong follow-up request URL.
    pub url: String,
}

/// A DAV response body paired with the effective URI that produced it, so href
/// resolution has the base RFC 4918 requires.
#[derive(Debug, Clone)]
pub struct DavBody {
    pub text: String,
    pub url: String,
}

/// A DAV request as an owned record rather than a builder.
///
/// The redirect walk re-dispatches the same logical request against a new
/// origin, with the credential for that origin swapped in. A `reqwest`-style
/// builder cannot express that: it is consumed on send and only conditionally
/// cloneable, so the previous walk replayed through `try_clone` and gave up
/// with a local error when a body made the request unclonable. An owned record
/// is rebuildable per hop by construction.
#[derive(Debug, Clone)]
pub struct DavRequest {
    pub(crate) method: Method,
    pub(crate) url: String,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Option<Bytes>,
    /// Whether replaying this request cannot change server state.
    ///
    /// `bifrost-net` derives replay safety from the method and treats every
    /// extension method as unsafe, which is right for `MOVE` and wrong for
    /// `PROPFIND` and `REPORT` - both are reads, and refusing to retry them
    /// after a dropped connection loses resilience for nothing.
    pub(crate) idempotent: Option<bool>,
}

impl DavRequest {
    #[must_use]
    pub fn new(method: Method, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            headers: HeaderMap::new(),
            body: None,
            idempotent: None,
        }
    }

    /// Set one header.
    ///
    /// A name or value the HTTP grammar rejects is dropped rather than raised.
    /// Every call site passes either a `HeaderName` constant or a value it has
    /// already validated, so a rejection here is unreachable in practice; the
    /// previous builder silently skipped the same cases, and preserving that
    /// keeps the migration behaviour-neutral.
    #[must_use]
    pub fn header<K, V>(mut self, name: K, value: V) -> Self
    where
        K: TryInto<HeaderName>,
        V: TryInto<HeaderValue>,
    {
        if let (Ok(name), Ok(value)) = (name.try_into(), value.try_into()) {
            self.headers.insert(name, value);
        }
        self
    }

    #[must_use]
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        for (name, value) in &headers {
            self.headers.insert(name, value.clone());
        }
        self
    }

    #[must_use]
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Override the method-derived replay-safety default.
    #[must_use]
    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent = Some(idempotent);
        self
    }

    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

/// Classify a completed DAV response and keep the effective URI attached to the
/// body, so the caller resolves hrefs against the URI that actually served the
/// Multi-Status rather than the one it submitted.
pub fn settle_body(
    response: DavResponse,
    operation: AccountOperation,
    protocol: DavProtocol,
) -> Result<DavBody, AccountError> {
    if response.status.is_success() {
        Ok(DavBody {
            text: response.body,
            url: response.url,
        })
    } else {
        Err(crate::error::status_error(
            operation,
            response.status,
            response.body,
            protocol,
        ))
    }
}

/// Whether a URL's scheme carries an authenticated, encrypted transport.
///
/// Only `https` qualifies; an unparseable URL is treated as insecure so the
/// downgrade check fails closed.
#[must_use]
pub fn origin_is_secure(value: &str) -> bool {
    Url::parse(value).is_ok_and(|url| url.scheme().eq_ignore_ascii_case("https"))
}

/// The scheme/host/effective-port triple a credential gate compares on.
#[must_use]
pub fn url_origin(value: &str) -> Option<String> {
    let url = Url::parse(value).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    Some(format!(
        "{}://{}:{}",
        url.scheme().to_ascii_lowercase(),
        host,
        url.port_or_known_default()?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_origin_normalizes_case_and_default_port() {
        assert_eq!(
            url_origin("HTTPS://DAV.Example.Test/cal/").as_deref(),
            Some("https://dav.example.test:443")
        );
        assert_eq!(
            url_origin("https://dav.example.test:8443/cal/").as_deref(),
            Some("https://dav.example.test:8443")
        );
        assert_eq!(url_origin("not a url"), None);
    }

    #[test]
    fn only_https_counts_as_secure() {
        assert!(origin_is_secure("https://dav.example.test"));
        assert!(!origin_is_secure("http://dav.example.test"));
        // Fails closed rather than open.
        assert!(!origin_is_secure("not a url"));
    }

    /// A body no longer makes a request unreplayable. The previous walk held a
    /// `reqwest::RequestBuilder` and called `try_clone`, which returns `None`
    /// for a streaming body, so a redirected PUT failed locally with
    /// "redirected DAV request cannot be replayed" rather than following.
    #[test]
    fn a_request_carrying_a_body_survives_being_rebuilt() {
        let request = DavRequest::new(Method::PUT, "https://dav.example.test/one.ics")
            .header(reqwest::header::CONTENT_TYPE, "text/calendar")
            .body("BEGIN:VCALENDAR");
        let replayed = request.clone();
        assert_eq!(replayed.body, request.body);
        assert_eq!(replayed.headers, request.headers);
        assert_eq!(replayed.method, Method::PUT);
    }
}
