//! Authentication policy and credential types.

use std::fmt;

use super::SecretString;

/// Credentials accepted by `ImapAccountFactory`.
///
/// This models the common single-identity flows. Delegated access with a
/// distinct SASL authorization identity is not represented here yet.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    kind: CredentialsKind,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum CredentialsKind {
    /// Username and password credentials.
    Password {
        /// Authentication identity.
        username: String,
        /// Password or app password.
        password: SecretString,
    },
    /// OAuth 2.0 bearer token credentials.
    OAuth2 {
        /// Authorization identity, usually the email address being accessed.
        identity: String,
        /// OAuth 2.0 access token.
        access_token: SecretString,
    },
}

impl Credentials {
    /// Create username and password credentials.
    pub fn password(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            kind: CredentialsKind::Password {
                username: username.into(),
                password: SecretString::from(password.into()),
            },
        }
    }

    /// Create OAuth 2.0 bearer-token credentials.
    pub fn oauth2(identity: impl Into<String>, access_token: impl Into<String>) -> Self {
        Self {
            kind: CredentialsKind::OAuth2 {
                identity: identity.into(),
                access_token: SecretString::from(access_token.into()),
            },
        }
    }

    pub(crate) fn kind(&self) -> &CredentialsKind {
        &self.kind
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            CredentialsKind::Password { username, .. } => f
                .debug_struct("Credentials::Password")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            CredentialsKind::OAuth2 { identity, .. } => f
                .debug_struct("Credentials::OAuth2")
                .field("identity", identity)
                .field("access_token", &"<redacted>")
                .finish(),
        }
    }
}

/// SASL or legacy authentication mechanism selected by the client.
///
/// The `*-PLUS` variants are the SCRAM channel-binding mechanisms. They are
/// channel-bound via RFC 5929 `tls-server-end-point` and are selected only
/// when the peer certificate yields a binding value; `authenticate_best`
/// prefers them over their non-PLUS counterparts and enforces RFC 5802
/// Section 6 downgrade protection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AuthMechanism {
    /// SASL XOAUTH2 bearer-token authentication.
    XOAuth2,
    /// SASL SCRAM-SHA-256-PLUS (channel-bound).
    ScramSha256Plus,
    /// SASL SCRAM-SHA-1-PLUS (channel-bound).
    ScramSha1Plus,
    /// SASL SCRAM-SHA-256.
    ScramSha256,
    /// SASL SCRAM-SHA-1.
    ScramSha1,
    /// SASL PLAIN.
    Plain,
    /// SASL CRAM-MD5.
    CramMd5,
    /// Legacy IMAP LOGIN command.
    Login,
}

impl AuthMechanism {
    /// Return the advertised SASL mechanism name, or `LOGIN` for the legacy command.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::XOAuth2 => "XOAUTH2",
            Self::ScramSha256Plus => "SCRAM-SHA-256-PLUS",
            Self::ScramSha1Plus => "SCRAM-SHA-1-PLUS",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha1 => "SCRAM-SHA-1",
            Self::Plain => "PLAIN",
            Self::CramMd5 => "CRAM-MD5",
            Self::Login => "LOGIN",
        }
    }
}

/// Policy for automatic authentication mechanism selection.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthPolicy {
    /// Allow the legacy LOGIN command.
    ///
    /// Default: `false`. LOGIN is not SASL and is deprecated for IMAP4rev2.
    pub allow_login: bool,
    /// Allow credential-bearing mechanisms on a connection that is not encrypted.
    ///
    /// This includes PLAIN, XOAUTH2, CRAM-MD5, and LOGIN. SCRAM mechanisms
    /// remain allowed because they do not send reusable credentials or bearer
    /// tokens directly.
    ///
    /// Default: `false`.
    pub allow_cleartext_without_tls: bool,
    /// Allow CRAM-MD5 as a legacy fallback.
    ///
    /// Default: `false`. CRAM-MD5 is vulnerable to offline password
    /// guessing when the challenge is attacker-controlled, so automatic
    /// selection only uses it when callers opt in.
    pub allow_cram_md5: bool,
}

impl AuthPolicy {
    /// Permit the legacy LOGIN command as a last resort.
    pub fn with_login(mut self) -> Self {
        self.allow_login = true;
        self
    }

    /// Permit cleartext credential mechanisms without TLS.
    pub fn allow_cleartext_without_tls(mut self) -> Self {
        self.allow_cleartext_without_tls = true;
        self
    }

    /// Permit CRAM-MD5 as a legacy fallback.
    pub fn with_cram_md5(mut self) -> Self {
        self.allow_cram_md5 = true;
        self
    }

    /// Disable CRAM-MD5 fallback.
    pub fn without_cram_md5(mut self) -> Self {
        self.allow_cram_md5 = false;
        self
    }
}

/// Result of automatic authentication.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct AuthOutcome {
    /// Mechanism used for the successful authentication exchange.
    pub(crate) mechanism: AuthMechanism,
}
