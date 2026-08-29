//! The DAV transport seam, its redirect policy, and origin comparison.
//!
//! `bifrost-net`'s dispatcher is deliberately crate-private and the DAV clients
//! still own Basic auth and their own redirect policy, so the seam lives here
//! rather than riding an `AccountNet`. Moving these clients onto `AccountNet` is
//! tracked separately (dav-B9); this crate unifies the two copies without
//! prejudging that.

use std::time::Duration;

use bifrost_types::{AccountError, AccountFuture, AccountOperation};
use reqwest::header::HeaderMap;
use reqwest::{StatusCode, Url};

use crate::error::{DavProtocol, response_read_error};

/// Wall-clock ceiling on a single DAV request.
pub const DAV_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

const RESPONSE_BODY_TOO_LARGE: &str = "DAV response body exceeded the buffered ceiling";

#[derive(Debug, Clone)]
pub struct DavResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: String,
    /// Effective request URI, after any redirects the client followed.
    ///
    /// RFC 4918 relative hrefs in a Multi-Status resolve against the effective
    /// request URI, not the URI the caller submitted. The DAV redirect policy
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

/// Local DAV transport boundary.
///
/// Production dispatch is `ReqwestDavTransport`; tests install a scripted
/// double, which is what lets DAV flows be exercised byte for byte with no
/// listener and no socket.
pub trait DavTransport: Send + Sync {
    fn send(&self, request: reqwest::RequestBuilder) -> AccountFuture<Result<DavResponse, String>>;
}

pub struct ReqwestDavTransport;

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
/// `response.text()` buffers without one, so a provider returning a runaway
/// 207, an error page, or a mis-routed blob URL OOMs the process. A Multi-Status
/// body for a large collection is legitimately big, hence a ceiling generous
/// enough that only a pathological response reaches it, matching the buffered
/// ceiling `bifrost-net` applies on its own `send` path.
pub async fn read_capped_body(response: reqwest::Response) -> Result<String, String> {
    use futures::StreamExt as _;

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
    // `.text()` decodes per the `charset` Content-Type parameter and falls back
    // to lossy UTF-8. This decodes lossily unconditionally, which narrows
    // behaviour for a server that declares a non-UTF-8 charset - RFC 4918
    // bodies are XML, whose declared default is UTF-8, so that case was already
    // outside what the parsers handle. Lossy rather than strict keeps a
    // malformed byte behaving as it did before (a replacement character, not a
    // failed request).
    Ok(String::from_utf8_lossy(&buf).into_owned())
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
        Err(status_error_for(response, operation, protocol))
    }
}

fn status_error_for(
    response: DavResponse,
    operation: AccountOperation,
    protocol: DavProtocol,
) -> AccountError {
    crate::error::status_error(operation, response.status, response.body, protocol)
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

/// Hardened redirect policy for the DAV `reqwest::Client`.
///
/// Follows a hop only when the next URL keeps the exact origin (scheme, host,
/// effective port) of the URL that issued the redirect; reqwest preserves
/// `Authorization` precisely under that condition, and strips it on any origin
/// change with no way for a policy to restore it. Every cross-origin hop is
/// stopped so the 3xx surfaces to the caller's `send_raw_request`, which
/// re-dispatches it with fresh credentials against the admitted origin set. The
/// hop cap comes from `bifrost-net`.
#[must_use]
pub fn dav_redirect_policy() -> reqwest::redirect::Policy {
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

/// Wrap a transport-layer failure string, mapping the body-ceiling refusal onto
/// its own classification.
#[must_use]
pub fn transport_failure(
    operation: AccountOperation,
    message: String,
    protocol: DavProtocol,
) -> AccountError {
    if message.starts_with(RESPONSE_BODY_TOO_LARGE) {
        response_read_error(operation, message, protocol)
    } else {
        crate::error::transport_error(operation, message, protocol)
    }
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
}
