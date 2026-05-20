use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

const MAX_BODY_EXCERPT_CHARS: usize = 4096;

/// Result type for Gmail client operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Base64 alphabet used by a failed decoder.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum Base64Encoding {
    /// RFC 4648 standard base64 alphabet.
    Standard,
    /// RFC 4648 URL-safe alphabet without padding, as used by Gmail bodies.
    UrlSafeNoPad,
}

impl Display for Base64Encoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Standard => f.write_str("standard base64"),
            Self::UrlSafeNoPad => f.write_str("base64url without padding"),
        }
    }
}

/// Error type for Gmail and Google Drive API operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Network, TLS, timeout, or response-body read failure.
    Transport(reqwest::Error),
    /// Server returned an unsuccessful HTTP status.
    HttpStatus {
        /// Logical service being called.
        service: String,
        /// HTTP response status.
        status: reqwest::StatusCode,
        /// Truncated response body suitable for diagnostics.
        body: String,
    },
    /// Gmail or Drive refused the bearer token.
    Auth {
        /// Logical service being called.
        service: String,
        /// HTTP response status, when known.
        status: Option<reqwest::StatusCode>,
        /// Truncated response body suitable for diagnostics.
        body: String,
        /// True when callers should refresh the token before retrying.
        refresh_required: bool,
    },
    /// Quota, rate-limit, or upload bandwidth limit response.
    QuotaExhausted {
        /// Logical service being called.
        service: String,
        /// HTTP response status.
        status: reqwest::StatusCode,
        /// Truncated response body suitable for diagnostics.
        body: String,
    },
    /// JSON response could not be decoded into the requested type.
    Json(serde_json::Error),
    /// Gmail body or raw-message base64 data could not be decoded.
    Base64 {
        /// Alphabet that was attempted.
        encoding: Base64Encoding,
        /// Decoder failure.
        source: base64::DecodeError,
    },
    /// Gmail or Drive returned a response shape that is not usable.
    MalformedPayload(String),
    /// Caller supplied an invalid argument.
    InvalidInput(String),
}

impl Error {
    pub(crate) fn status(
        service: impl Into<String>,
        status: reqwest::StatusCode,
        body: String,
    ) -> Self {
        let service = service.into();
        let body = body_excerpt(body);

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Self::Auth {
                service,
                status: Some(status),
                body,
                refresh_required: true,
            };
        }

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || (status == reqwest::StatusCode::FORBIDDEN && looks_like_quota_error(&body))
        {
            return Self::QuotaExhausted {
                service,
                status,
                body,
            };
        }

        Self::HttpStatus {
            service,
            status,
            body,
        }
    }

    pub(crate) fn base64_standard(source: base64::DecodeError) -> Self {
        Self::Base64 {
            encoding: Base64Encoding::Standard,
            source,
        }
    }

    pub(crate) fn base64url(source: base64::DecodeError) -> Self {
        Self::Base64 {
            encoding: Base64Encoding::UrlSafeNoPad,
            source,
        }
    }

    /// Whether this failure is worth retrying without changing credentials.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(err) => err.is_connect() || err.is_timeout(),
            Self::HttpStatus { status, .. } | Self::QuotaExhausted { status, .. } => {
                *status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
            }
            Self::Auth { .. }
            | Self::Json(_)
            | Self::Base64 { .. }
            | Self::MalformedPayload(_)
            | Self::InvalidInput(_) => false,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(err) => write!(f, "transport error: {err}"),
            Self::HttpStatus {
                service,
                status,
                body,
            } => write!(f, "{service} returned HTTP {status}: {body}"),
            Self::Auth {
                service,
                status,
                body,
                refresh_required,
            } => match status {
                Some(status) if *refresh_required => {
                    write!(
                        f,
                        "{service} authentication failed with HTTP {status}: {body}"
                    )
                }
                Some(status) => write!(
                    f,
                    "{service} authentication failed with HTTP {status}: {body}"
                ),
                None => write!(f, "{service} authentication failed: {body}"),
            },
            Self::QuotaExhausted {
                service,
                status,
                body,
            } => write!(f, "{service} quota exhausted with HTTP {status}: {body}"),
            Self::Json(err) => write!(f, "JSON decode failed: {err}"),
            Self::Base64 { encoding, source } => {
                write!(f, "{encoding} decode failed: {source}")
            }
            Self::MalformedPayload(message) => write!(f, "malformed API payload: {message}"),
            Self::InvalidInput(message) => f.write_str(message),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Transport(err) => Some(err),
            Self::Json(err) => Some(err),
            Self::Base64 { source, .. } => Some(source),
            Self::HttpStatus { .. }
            | Self::Auth { .. }
            | Self::QuotaExhausted { .. }
            | Self::MalformedPayload(_)
            | Self::InvalidInput(_) => None,
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::Transport(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

fn body_excerpt(body: String) -> String {
    if body.chars().count() <= MAX_BODY_EXCERPT_CHARS {
        return body;
    }

    let mut excerpt = body
        .chars()
        .take(MAX_BODY_EXCERPT_CHARS)
        .collect::<String>();
    excerpt.push_str("...");
    excerpt
}

fn looks_like_quota_error(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.contains("quota")
        || lower.contains("rate limit")
        || lower.contains("ratelimit")
        || lower.contains("user-rate-limit")
}

#[cfg(test)]
mod tests {
    use super::{Error, MAX_BODY_EXCERPT_CHARS};

    #[test]
    fn status_truncates_http_body_excerpt() {
        let err = Error::status(
            "Gmail API",
            reqwest::StatusCode::BAD_REQUEST,
            "x".repeat(MAX_BODY_EXCERPT_CHARS + 10),
        );

        let Error::HttpStatus { body, .. } = err else {
            panic!("expected HTTP status error");
        };
        assert_eq!(body.chars().count(), MAX_BODY_EXCERPT_CHARS + 3);
        assert!(body.ends_with("..."));
    }

    #[test]
    fn status_classifies_unauthorized_as_auth_refresh() {
        let err = Error::status(
            "Gmail API",
            reqwest::StatusCode::UNAUTHORIZED,
            "invalid token".to_string(),
        );

        let Error::Auth {
            refresh_required, ..
        } = err
        else {
            panic!("expected auth error");
        };
        assert!(refresh_required);
    }

    #[test]
    fn status_classifies_rate_limit_as_retryable_quota() {
        let err = Error::status(
            "Gmail API",
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "slow down".to_string(),
        );

        assert!(matches!(err, Error::QuotaExhausted { .. }));
        assert!(err.is_retryable());
    }
}
