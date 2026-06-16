//! SMTP submission transport owned by an IMAP account.
//!
//! When an `ImapAccountConfig` carries a `SmtpSubmissionConfig`, the
//! factory builds one of these and stores it on the account. The send
//! path (`pim::send_message` / `pim::draft_send`) drives it to submit
//! RFC 5322 bytes over `bifrost-smtp`, authenticating with the same
//! credentials the IMAP login uses (or an explicit override), so a
//! consumer reaches `send` through the uniform `Account` surface with no
//! IMAP-plus-SMTP composition leaking up.
//!
//! Address conversion is confined here: the send path stays in
//! `bifrost_types::Address` space, and the narrow, total
//! `Address -> bifrost_smtp::Address` reverse-path / recipient
//! conversion (bare addr-specs, no display name) happens only at
//! `send_rfc5322`.

use std::sync::Arc;
use std::time::Duration;

use bifrost_net::TokenSource;
use bifrost_smtp::transport::smtp::authentication::Credentials as SmtpCredentials;
use bifrost_smtp::transport::smtp::{PoolConfig, SendOptions};
use bifrost_smtp::{Address as SmtpAddress, AsyncSmtpTransport, TokioExecutor};
use bifrost_types::error::{AccountError, BatchItem, BatchItemId};
use bifrost_types::mime::SubmissionEnvelope;

use crate::types::{Credentials, CredentialsKind};

/// TLS mode for the submission transport. Mirrors `ImapConfig`'s TLS
/// modes; maps to SMTP's `relay` / `starttls_relay` / `builder_dangerous`
/// at build time.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmissionTls {
    /// Implicit TLS (SMTPS). Default port 465.
    Implicit,
    /// STARTTLS upgrade. Default port 587.
    StartTls,
    /// Plaintext. Default port 587. Requires opting into insecure auth.
    Plaintext,
}

impl SubmissionTls {
    const fn default_port(self) -> u16 {
        match self {
            Self::Implicit => 465,
            Self::StartTls | Self::Plaintext => 587,
        }
    }
}

/// Submission auth when it differs from the IMAP login. The common
/// single-sign-on case (`SmtpSubmissionConfig::credentials == None`)
/// reuses the IMAP `Credentials` and never constructs one of these.
#[non_exhaustive]
#[derive(Clone)]
pub enum SubmissionCredentials {
    /// Username + password submission auth.
    Password {
        /// Authentication identity.
        username: String,
        /// Password or app password.
        password: String,
    },
    /// OAuth 2.0 bearer submission auth, sharing a rotation source.
    OAuth2 {
        /// Authorization identity, usually the email address.
        identity: String,
        /// Live token source, read fresh at each connect.
        token_source: Arc<dyn TokenSource>,
    },
}

/// Submission transport configuration, independent of the IMAP dial
/// config because submission host/port differ from IMAP host/port.
#[non_exhaustive]
#[derive(Clone)]
pub struct SmtpSubmissionConfig {
    /// Submission host (e.g. "smtp.example.com").
    pub host: String,
    /// TLS mode for submission.
    pub tls: SubmissionTls,
    /// Override the mode-default port when set.
    pub port: Option<u16>,
    /// Per-command timeout for the submission transport.
    pub timeout: Option<Duration>,
    /// Override the SMTP credentials. `None` reuses the IMAP account
    /// credentials (same username + password, or same identity + A1
    /// token source).
    pub credentials: Option<SubmissionCredentials>,
    /// Pool sizing for the submission transport.
    pub pool: Option<PoolConfig>,
    /// Default From address, used when a `SendRequest` omits `from` and
    /// as the MAIL FROM reverse path default.
    pub default_from: bifrost_types::Address,
    /// Append sent messages to the Sent folder unless the caller says
    /// otherwise via `SendRequest::save_to_sent`.
    pub save_to_sent_default: bool,
}

impl SmtpSubmissionConfig {
    /// Construct a submission config that reuses the IMAP credentials.
    pub fn new(
        host: impl Into<String>,
        tls: SubmissionTls,
        default_from: bifrost_types::Address,
    ) -> Self {
        Self {
            host: host.into(),
            tls,
            port: None,
            timeout: None,
            credentials: None,
            pool: None,
            default_from,
            save_to_sent_default: true,
        }
    }

    /// Override the port (defaults follow the TLS mode: 465 / 587 / 587).
    #[must_use]
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Set the per-command submission timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Use explicit submission credentials instead of reusing the IMAP
    /// login.
    #[must_use]
    pub fn with_credentials(mut self, credentials: SubmissionCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Tune the submission connection pool.
    #[must_use]
    pub fn with_pool(mut self, pool: PoolConfig) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Set whether sends append to the Sent folder by default.
    #[must_use]
    pub fn with_save_to_sent_default(mut self, save: bool) -> Self {
        self.save_to_sent_default = save;
        self
    }
}

/// Thin crate-internal wrapper around the SMTP transport plus the
/// resolved default From / save-to-sent policy, so the send path can
/// default `from` and the MAIL FROM reverse path.
pub(crate) struct SubmissionTransport {
    transport: AsyncSmtpTransport<TokioExecutor>,
    default_from: bifrost_types::Address,
    save_to_sent_default: bool,
}

impl SubmissionTransport {
    pub(crate) fn default_from(&self) -> &bifrost_types::Address {
        &self.default_from
    }

    pub(crate) fn save_to_sent_default(&self) -> bool {
        self.save_to_sent_default
    }

    /// Build the submission transport from the config, deriving SMTP
    /// credentials from the IMAP login when the config does not override
    /// them.
    pub(crate) fn build(
        cfg: &SmtpSubmissionConfig,
        imap_credentials: &Credentials,
    ) -> Result<Self, crate::Error> {
        let credentials = resolve_credentials(cfg, imap_credentials);
        let port = cfg.port.unwrap_or_else(|| cfg.tls.default_port());

        // A builder error here is a local submission-config fault (bad TLS
        // SNI / host), not a wire failure: surface it as InvalidInput so the
        // account boundary classifies it as a client-side request error.
        let smtp_config_err =
            |e: bifrost_smtp::transport::smtp::Error| crate::Error::InvalidInput(e.to_string());
        let mut builder = match cfg.tls {
            SubmissionTls::Implicit => {
                AsyncSmtpTransport::<TokioExecutor>::relay(&cfg.host).map_err(smtp_config_err)?
            }
            SubmissionTls::StartTls => {
                AsyncSmtpTransport::<TokioExecutor>::starttls_relay(&cfg.host)
                    .map_err(smtp_config_err)?
            }
            SubmissionTls::Plaintext => {
                AsyncSmtpTransport::<TokioExecutor>::builder_dangerous(cfg.host.clone())
                    .dangerous_allow_insecure_auth(true)
            }
        };

        builder = builder.port(port).credentials(credentials);
        if let Some(timeout) = cfg.timeout {
            builder = builder.timeout(Some(timeout));
        }
        if let Some(pool) = &cfg.pool {
            builder = builder.pool_config(pool.clone());
        }

        Ok(Self {
            transport: builder.build(),
            default_from: cfg.default_from.clone(),
            save_to_sent_default: cfg.save_to_sent_default,
        })
    }

    /// Submit the RFC 5322 bytes over SMTP using `envelope` as the
    /// reverse path / recipient set. This is the only place the
    /// `bifrost_types::Address -> bifrost_smtp::Address` conversion
    /// happens. Returns the already-translated `AccountError` from
    /// `bifrost-smtp` on failure.
    pub(crate) async fn send_rfc5322(
        &self,
        envelope: &SubmissionEnvelope,
        raw: &[u8],
    ) -> Result<(), AccountError> {
        let from = to_smtp_address(&envelope.from);
        let recipients: Vec<BatchItem<SmtpAddress>> = envelope
            .recipients
            .iter()
            .enumerate()
            .map(|(index, address)| {
                BatchItem::new(BatchItemId(index.to_string()), to_smtp_address(address))
            })
            .collect();

        let outcome = self
            .transport
            .send_raw_batch_with_options(Some(from), recipients, raw, &SendOptions::default())
            .await?;

        // A single committed message either lands all recipients in the
        // succeeded lane or surfaces per-recipient failures. Treat any
        // non-succeeded recipient as the send error so the caller never
        // reports a partial drop as success. The first failed/uncertain
        // recipient's already-classified `AccountError` is authoritative.
        if let Some(failure) = outcome.failed().first() {
            return Err(failure.error.clone());
        }
        if let Some(uncertain) = outcome.uncertain().first() {
            return Err(uncertain.error.clone());
        }
        Ok(())
    }
}

/// Convert a `bifrost_types::Address` into an SMTP envelope address.
/// Envelope addresses are bare addr-specs (no display name), so this is
/// total: even a malformed user/domain split is accepted unchecked, with
/// the server rejecting an invalid RCPT - the same posture
/// `bifrost-smtp`'s own envelope builder takes.
fn to_smtp_address(address: &bifrost_types::Address) -> SmtpAddress {
    match address.address.rsplit_once('@') {
        Some((user, domain)) => SmtpAddress::new_unchecked(user, domain),
        None => SmtpAddress::new_unchecked(address.address.as_str(), ""),
    }
}

/// Derive SMTP credentials: an explicit override if present, else a
/// structural copy of the IMAP login (password or A1 token source). The
/// `Arc<dyn TokenSource>` threads straight across - the same source the
/// IMAP side reads, so a rotated token is presented on both legs.
fn resolve_credentials(
    cfg: &SmtpSubmissionConfig,
    imap_credentials: &Credentials,
) -> SmtpCredentials {
    match &cfg.credentials {
        Some(SubmissionCredentials::Password { username, password }) => {
            SmtpCredentials::password(username.clone(), password.clone())
        }
        Some(SubmissionCredentials::OAuth2 {
            identity,
            token_source,
        }) => SmtpCredentials::oauth2_source(identity.clone(), Arc::clone(token_source)),
        None => match imap_credentials.kind() {
            CredentialsKind::Password { username, password } => {
                SmtpCredentials::password(username.clone(), password.as_str().to_owned())
            }
            CredentialsKind::OAuth2 {
                identity,
                token_source,
            } => SmtpCredentials::oauth2_source(identity.clone(), Arc::clone(token_source)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_net::StaticTokenSource;

    #[test]
    fn submission_credentials_reuse_imap_oauth() {
        let source: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("tok", None));
        let imap = Credentials::oauth2_source("user@example.com", Arc::clone(&source));
        let cfg = SmtpSubmissionConfig::new(
            "smtp.example.com",
            SubmissionTls::Implicit,
            bifrost_types::Address::bare("user@example.com"),
        );

        let smtp = resolve_credentials(&cfg, &imap);
        match smtp {
            SmtpCredentials::OAuth2 {
                identity,
                token_source,
            } => {
                assert_eq!(identity, "user@example.com");
                // Same A1 token source threaded across the IMAP/SMTP boundary.
                assert!(Arc::ptr_eq(&token_source, &source));
            }
            SmtpCredentials::Password { .. } => panic!("expected OAuth2 credentials"),
        }
    }

    #[test]
    fn submission_tls_default_ports() {
        assert_eq!(SubmissionTls::Implicit.default_port(), 465);
        assert_eq!(SubmissionTls::StartTls.default_port(), 587);
        assert_eq!(SubmissionTls::Plaintext.default_port(), 587);
    }
}
