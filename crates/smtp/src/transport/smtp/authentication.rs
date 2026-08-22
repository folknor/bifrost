//! Provides limited SASL authentication mechanisms

use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use bifrost_net::{StaticTokenSource, TokenSource};
use bifrost_sasl::{ScramChannelBinding, ScramHash};

use crate::transport::smtp::error::{self, Error, SmtpCommandPhase};
use crate::transport::smtp::extension::ServerInfo;
// pub: Credentials exposes zeroizing fields and constructors accept this type.
pub use zeroize::Zeroizing;

/// Accepted password authentication mechanisms, strongest first.
///
/// SCRAM (channel-bound first, then unbound), then PLAIN. LOGIN is **not** in
/// the default set: it is an opt-in legacy mechanism (it sends reusable
/// cleartext credentials), so a caller that wants it must list it explicitly
/// via `authentication(...)`. This mirrors IMAP's `allow_login = false`
/// default. A server advertising only `AUTH LOGIN` therefore yields an empty
/// attempt order under the default and fails with "no compatible mechanism"
/// rather than silently downgrading to LOGIN.
pub(crate) const PASSWORD_MECHANISMS: &[Mechanism] = &[
    Mechanism::ScramSha256Plus,
    Mechanism::ScramSha1Plus,
    Mechanism::ScramSha256,
    Mechanism::ScramSha1,
    Mechanism::Plain,
];

/// Accepted OAuth 2.0 bearer-token authentication mechanisms.
///
/// `OAUTHBEARER` is the standard mechanism. `XOAUTH2` is kept for providers
/// that only expose the older non-standard mechanism.
pub(crate) const OAUTH2_MECHANISMS: &[Mechanism] = &[Mechanism::OAuthBearer, Mechanism::Xoauth2];

/// Default authentication mechanisms.
pub(crate) const DEFAULT_MECHANISMS: &[Mechanism] = PASSWORD_MECHANISMS;

/// Contains user credentials
//
// `Credentials` no longer derives `PartialEq`/`Eq` or the `serde`
// round-trip it used to: the OAuth variant now carries a live
// `Arc<dyn TokenSource>` instead of a frozen token string, and a token
// source is neither comparable nor serializable. Rotation material is
// the consumer's to persist; serializing a credential would freeze a
// token that is meant to rotate. Password credentials keep `Clone`.
#[derive(Clone)]
// pub: users configure password or OAuth bearer SASL credentials.
pub enum Credentials {
    /// Username and password credentials.
    Password {
        /// Authentication identity.
        username: String,
        /// Password or app password.
        password: Zeroizing<String>,
    },
    /// OAuth 2.0 bearer-token credentials.
    OAuth2 {
        /// Authorization identity, usually the email address being accessed.
        identity: String,
        /// Live source of the OAuth 2.0 access token. Read at each wire
        /// authentication so a token rotated on the shared source is
        /// presented without reconstructing the credential.
        token_source: Arc<dyn TokenSource>,
    },
}

/// Converts owned secret material into a zeroizing string.
// pub: Credentials constructors expose this bound for secret inputs.
pub trait IntoSecretString {
    /// Move the secret into zeroizing storage.
    fn into_secret_string(self) -> Zeroizing<String>;
}

impl IntoSecretString for String {
    fn into_secret_string(self) -> Zeroizing<String> {
        Zeroizing::new(self)
    }
}

impl IntoSecretString for &str {
    fn into_secret_string(self) -> Zeroizing<String> {
        Zeroizing::new(self.to_owned())
    }
}

impl IntoSecretString for Zeroizing<String> {
    fn into_secret_string(self) -> Zeroizing<String> {
        self
    }
}

impl Credentials {
    /// Create username and password credentials.
    pub fn password<U, P>(username: U, password: P) -> Credentials
    where
        U: Into<String>,
        P: IntoSecretString,
    {
        Credentials::Password {
            username: username.into(),
            password: password.into_secret_string(),
        }
    }

    /// Create OAuth 2.0 bearer-token credentials from a raw token string.
    /// Convenience wrapper: the token is held in a `StaticTokenSource` so
    /// existing call sites keep their string ergonomics.
    pub fn oauth2<I, T>(identity: I, access_token: T) -> Credentials
    where
        I: Into<String>,
        T: IntoSecretString,
    {
        let token = access_token.into_secret_string();
        Credentials::oauth2_source(
            identity,
            Arc::new(StaticTokenSource::new(token.to_string(), None)),
        )
    }

    /// Create OAuth 2.0 bearer-token credentials from a shared token
    /// source. ratatoskr supplies one `Arc<dyn TokenSource>` it also
    /// drives rotation on, so a refreshed token is read live at every
    /// SMTP authentication without reconstructing the credential.
    pub fn oauth2_source<I>(identity: I, token_source: Arc<dyn TokenSource>) -> Credentials
    where
        I: Into<String>,
    {
        Credentials::OAuth2 {
            identity: identity.into(),
            token_source,
        }
    }

    pub(crate) fn preferred_mechanisms(&self) -> &'static [Mechanism] {
        match self {
            Credentials::Password { .. } => PASSWORD_MECHANISMS,
            Credentials::OAuth2 { .. } => OAUTH2_MECHANISMS,
        }
    }

    pub(crate) fn password_parts(&self) -> Result<(&str, &str), Error> {
        match self {
            Credentials::Password { username, password } => Ok((username, password.as_str())),
            Credentials::OAuth2 { .. } => Err(error::invalid_input(
                "OAuth2 credentials cannot be used with password authentication mechanisms",
            )),
        }
    }

    /// Pair the credential's OAuth identity with a token already resolved
    /// by the connection driver. Rejects password credentials (wrong
    /// mechanism) and a missing token (driver bug: an OAuth mechanism was
    /// driven without resolving the token first).
    fn oauth_identity_and_token<'a>(
        &'a self,
        oauth_token: Option<&'a str>,
    ) -> Result<(&'a str, &'a str), Error> {
        match self {
            Credentials::OAuth2 { identity, .. } => {
                let token = oauth_token.ok_or_else(|| {
                    error::invalid_input("OAuth mechanism driven without a resolved access token")
                })?;
                Ok((identity, token))
            }
            Credentials::Password { .. } => Err(error::invalid_input(
                "password credentials cannot be used with OAuth2 authentication mechanisms",
            )),
        }
    }

    /// Resolve the OAuth identity and a freshly read access token from
    /// the shared source. Used by the async transport, which awaits a
    /// possible refresh.
    pub(crate) async fn oauth2_token(&self) -> Result<(String, Zeroizing<String>), Error> {
        match self {
            Credentials::OAuth2 {
                identity,
                token_source,
            } => {
                let token = token_source.current().await.map_err(oauth_token_error)?;
                Ok((identity.clone(), Zeroizing::new(token.as_str().to_owned())))
            }
            Credentials::Password { .. } => Err(error::invalid_input(
                "password credentials cannot be used with OAuth2 authentication mechanisms",
            )),
        }
    }

    /// Resolve the OAuth identity and current access token without an
    /// executor, by polling `current()` once. The blocking transport has
    /// no async context to await a refresh in; a `StaticTokenSource` (and
    /// an `OAuthRefresher` whose token is already fresh) resolves on the
    /// first poll. A source that would need a network refresh yields
    /// `Pending` and is rejected - live refresh requires the async
    /// transport.
    pub(crate) fn oauth2_token_blocking(&self) -> Result<(String, Zeroizing<String>), Error> {
        match self {
            Credentials::OAuth2 {
                identity,
                token_source,
            } => {
                let mut future = token_source.current();
                let waker = Waker::noop();
                let mut cx = Context::from_waker(waker);
                match future.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(token)) => {
                        Ok((identity.clone(), Zeroizing::new(token.as_str().to_owned())))
                    }
                    Poll::Ready(Err(e)) => Err(oauth_token_error(e)),
                    Poll::Pending => Err(error::invalid_input(
                        "OAuth token source requires a network refresh; use the async SMTP transport",
                    )
                    .with_phase(SmtpCommandPhase::Auth)),
                }
            }
            Credentials::Password { .. } => Err(error::invalid_input(
                "password credentials cannot be used with OAuth2 authentication mechanisms",
            )),
        }
    }
}

/// Map a token-source failure into the SMTP error model as an auth-phase
/// input fault carrying the source error's message. The `TokenSource`
/// failure projection (`AuthLost` vs `RefreshFailed`) is the error
/// model's concern downstream and is not refined here.
fn oauth_token_error(error: bifrost_net::Error) -> Error {
    error::invalid_input(format!("failed to read OAuth access token: {error}"))
        .with_phase(SmtpCommandPhase::Auth)
}

impl Debug for Credentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Credentials::Password { .. } => f.debug_struct("Credentials::Password").finish(),
            Credentials::OAuth2 { .. } => f.debug_struct("Credentials::OAuth2").finish(),
        }
    }
}

/// Represents authentication mechanisms
#[derive(PartialEq, Eq, Copy, Clone, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
// pub: users can constrain or inspect the SASL mechanism selection.
pub enum Mechanism {
    /// PLAIN authentication mechanism, defined in
    /// [RFC 4616](https://tools.ietf.org/html/rfc4616)
    Plain,
    /// LOGIN authentication mechanism
    /// Obsolete but needed for some providers (like Office 365)
    ///
    /// Defined in [draft-murchison-sasl-login-00](https://www.ietf.org/archive/id/draft-murchison-sasl-login-00.txt).
    Login,
    /// Non-standard XOAUTH2 mechanism, defined in
    /// [xoauth2-protocol](https://developers.google.com/gmail/imap/xoauth2-protocol)
    Xoauth2,
    /// OAUTHBEARER authentication mechanism, defined in
    /// [RFC 7628](https://www.rfc-editor.org/rfc/rfc7628.html)
    OAuthBearer,
    /// SCRAM-SHA-1, defined in [RFC 5802](https://www.rfc-editor.org/rfc/rfc5802).
    ScramSha1,
    /// SCRAM-SHA-256, defined in [RFC 7677](https://www.rfc-editor.org/rfc/rfc7677).
    ScramSha256,
    /// SCRAM-SHA-1-PLUS (channel-bound), defined in
    /// [RFC 5802](https://www.rfc-editor.org/rfc/rfc5802).
    ScramSha1Plus,
    /// SCRAM-SHA-256-PLUS (channel-bound), defined in
    /// [RFC 7677](https://www.rfc-editor.org/rfc/rfc7677).
    ScramSha256Plus,
}

impl Display for Mechanism {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // The `-PLUS` suffix and the SCRAM token spelling have a single
        // authority in `bifrost-sasl`, so the SMTP wire token can never drift
        // from the value used in the proof math.
        f.write_str(match *self {
            Mechanism::Plain => "PLAIN",
            Mechanism::Login => "LOGIN",
            Mechanism::Xoauth2 => "XOAUTH2",
            Mechanism::OAuthBearer => "OAUTHBEARER",
            Mechanism::ScramSha1 => {
                ScramHash::Sha1.mechanism_name(bifrost_sasl::ChannelBinding::None)
            }
            Mechanism::ScramSha256 => {
                ScramHash::Sha256.mechanism_name(bifrost_sasl::ChannelBinding::None)
            }
            Mechanism::ScramSha1Plus => {
                ScramHash::Sha1.mechanism_name(bifrost_sasl::ChannelBinding::TlsServerEndPoint)
            }
            Mechanism::ScramSha256Plus => {
                ScramHash::Sha256.mechanism_name(bifrost_sasl::ChannelBinding::TlsServerEndPoint)
            }
        })
    }
}

impl Mechanism {
    /// Does the mechanism support initial response?
    pub fn supports_initial_response(self) -> bool {
        match self {
            Mechanism::Plain | Mechanism::Xoauth2 | Mechanism::OAuthBearer => true,
            // SCRAM is driven as a no-IR challenge exchange so the multi-round
            // state lives entirely in `ScramExchange`, never in the stateless
            // `Auth` command `Display`.
            Mechanism::Login
            | Mechanism::ScramSha1
            | Mechanism::ScramSha256
            | Mechanism::ScramSha1Plus
            | Mechanism::ScramSha256Plus => false,
        }
    }

    /// Returns the string to send to the server, using the provided
    /// username, password and challenge in some cases.
    ///
    /// `oauth_token` is the access token already resolved from the
    /// credential's `TokenSource` by the connection's auth driver (which
    /// has the async/blocking context to read it). It is `None` for
    /// password mechanisms and ignored by them.
    pub(crate) fn response_with_token(
        self,
        credentials: &Credentials,
        challenge: Option<&str>,
        oauth_token: Option<&str>,
    ) -> Result<String, Error> {
        match self {
            Mechanism::ScramSha1
            | Mechanism::ScramSha256
            | Mechanism::ScramSha1Plus
            | Mechanism::ScramSha256Plus => Err(error::invalid_input(
                "SCRAM is driven by the SCRAM exchange, not the stateless mechanism encoder",
            )),
            Mechanism::Plain => match challenge {
                Some(_) => Err(error::invalid_input(
                    "This mechanism does not expect a challenge",
                )),
                None => {
                    let (username, password) = credentials.password_parts()?;
                    // RFC 4616 forbids NUL inside authzid/authcid/passwd: NUL is
                    // the field separator. A username carrying one splits into
                    // an extra field, so `authzid\0authcid` in the username slot
                    // is an authorization-identity injection, not merely a
                    // malformed message. Refuse rather than format it.
                    if username.contains('\u{0}') || password.contains('\u{0}') {
                        return Err(error::invalid_input(
                            "PLAIN credentials must not contain a NUL byte",
                        ));
                    }
                    Ok(format!("\u{0}{username}\u{0}{password}"))
                }
            },
            Mechanism::Login => {
                let (username, password) = credentials.password_parts()?;
                let decoded_challenge = challenge.ok_or_else(|| {
                    error::invalid_input("This mechanism does expect a challenge")
                })?;

                if contains_ignore_ascii_case(
                    decoded_challenge,
                    ["User Name", "Username:", "Username", "User Name\0"],
                ) {
                    return Ok(username.to_owned());
                }

                if contains_ignore_ascii_case(
                    decoded_challenge,
                    ["Password", "Password:", "Password\0"],
                ) {
                    return Ok(password.to_owned());
                }

                Err(error::invalid_input("Unrecognized challenge"))
            }
            Mechanism::Xoauth2 => match challenge {
                // A failed XOAUTH2 auth returns `334 <base64-json-error>`
                // (Google / Microsoft) rather than a tagged failure. RFC 6749
                // SASL XOAUTH2 requires the client to acknowledge with an empty
                // response so the server emits the final failure reply; we send
                // the `\x01` dummy-cancel line (`AQ==` on the wire), matching
                // OAUTHBEARER's RFC 7628 error-continuation. The connection
                // driver then classifies the negative reply on the Auth lane.
                Some(_) => Ok("\x01".to_owned()),
                None => {
                    let (identity, access_token) =
                        credentials.oauth_identity_and_token(oauth_token)?;
                    // Shared bifrost-sasl builder returns the raw payload as a
                    // zeroizing Secret; the AUTH command base64-frames it. The
                    // Secret drops and zeroizes after this owned copy.
                    Ok(bifrost_sasl::xoauth2_payload(identity, access_token)
                        .as_str()
                        .to_owned())
                }
            },
            Mechanism::OAuthBearer => match challenge {
                // RFC 7628 error-continuation response: protocol command flow,
                // not payload construction, so it stays in SMTP.
                Some(_) => Ok("\x01".to_owned()),
                None => {
                    let (identity, access_token) =
                        credentials.oauth_identity_and_token(oauth_token)?;
                    Ok(bifrost_sasl::oauthbearer_payload(identity, access_token)
                        .as_str()
                        .to_owned())
                }
            },
        }
    }
}

fn contains_ignore_ascii_case<'a>(
    haystack: &str,
    needles: impl IntoIterator<Item = &'a str>,
) -> bool {
    needles
        .into_iter()
        .any(|item| item.eq_ignore_ascii_case(haystack))
}

/// Map a `bifrost-sasl` computation failure into the SMTP error model.
///
/// A SCRAM `e=` server error or a signature/exchange auth failure
/// (`AuthFailed`) lands on the `SmtpCommandPhase::Auth` + `InvalidInput`
/// lane that `account_error.rs` routes to `Authorization(PolicyBlocked)` -
/// reauth/policy UX, not `ClientBug`. A malformed SASL message (`Protocol`)
/// is a protocol-class fault and stays on `ErrorKind::Parse`. This mirrors
/// IMAP's `From<SaslError>` split.
impl From<bifrost_sasl::SaslError> for Error {
    fn from(e: bifrost_sasl::SaslError) -> Self {
        match e {
            bifrost_sasl::SaslError::Protocol(m) => error::parse(m),
            bifrost_sasl::SaslError::AuthFailed(m) => {
                error::invalid_input(m).with_phase(SmtpCommandPhase::Auth)
            }
            // `SaslError` is `#[non_exhaustive]`; any future variant is an
            // unclassified auth failure until it is mapped explicitly.
            other => error::invalid_input(other.to_string()).with_phase(SmtpCommandPhase::Auth),
        }
    }
}

/// Fixed SCRAM-aware preference order, strongest first. Intersected with both
/// the caller's `allowed` set and the server's advertised set by
/// [`password_mechanism_order`].
const MECHANISM_PREFERENCE: &[Mechanism] = &[
    Mechanism::ScramSha256Plus,
    Mechanism::ScramSha1Plus,
    Mechanism::ScramSha256,
    Mechanism::ScramSha1,
    Mechanism::Plain,
    Mechanism::Login,
];

/// Ordered SCRAM-aware password mechanism selection over the
/// server-advertised set, with RFC 5802 Section 6 downgrade protection.
///
/// `allowed` is the configured/default preference set (the existing
/// `authentication` Vec). `advertised` is the server's EHLO `AUTH` set.
/// Returns the mechanisms to try, strongest first, with the unbound
/// `SCRAM-SHA-N` rung dropped when `SCRAM-SHA-N-PLUS` is advertised (the
/// downgrade skip is keyed on the advertised set, matching IMAP and the RFC).
pub(crate) fn password_mechanism_order(
    allowed: &[Mechanism],
    advertised: &ServerInfo,
) -> Vec<Mechanism> {
    let offers_sha256_plus = advertised.supports_auth_mechanism(Mechanism::ScramSha256Plus);
    let offers_sha1_plus = advertised.supports_auth_mechanism(Mechanism::ScramSha1Plus);

    let mut order = Vec::new();
    for &mechanism in MECHANISM_PREFERENCE {
        if !allowed.contains(&mechanism) {
            continue;
        }
        if !advertised.supports_auth_mechanism(mechanism) {
            continue;
        }
        // RFC 5802 Section 6: never offer the unbound SCRAM-SHA-N rung when
        // the matching PLUS variant is advertised, so a MITM cannot strip the
        // channel binding by forcing the unbound fallback.
        let downgrade_forbidden = (mechanism == Mechanism::ScramSha256 && offers_sha256_plus)
            || (mechanism == Mechanism::ScramSha1 && offers_sha1_plus);
        if downgrade_forbidden {
            continue;
        }
        order.push(mechanism);
    }
    order
}

/// First advertised OAuth mechanism in the caller's order, or `None`.
///
/// OAuth credentials never flow through the SCRAM/password ladder; they pick
/// the first advertised `OAUTHBEARER` / `XOAUTH2` rung in caller order and run
/// the legacy stateless encoder. Returns `None` for password credentials so
/// the caller falls through to [`password_mechanism_order`].
pub(crate) fn oauth_mechanism(
    mechanisms: &[Mechanism],
    advertised: &ServerInfo,
    credentials: &Credentials,
) -> Option<Mechanism> {
    if !matches!(credentials, Credentials::OAuth2 { .. }) {
        return None;
    }
    mechanisms.iter().copied().find(|&m| {
        matches!(m, Mechanism::OAuthBearer | Mechanism::Xoauth2)
            && advertised.supports_auth_mechanism(m)
    })
}

/// Walk `order` and return the first attemptable mechanism, skipping PLUS
/// rungs whose channel binding is unavailable (`binding_available(mech) ==
/// false`).
///
/// Only PLUS rungs consult `binding_available`; non-PLUS mechanisms are always
/// attemptable. Per RFC 5802 Section 6, a skipped PLUS rung never falls back
/// to its own unbound hash, but the *other* PLUS hash and PLAIN remain
/// attemptable. Errors with an `Auth`-phase `invalid_input` when every
/// candidate is a binding-unavailable PLUS rung (or `order` is empty).
pub(crate) fn first_attemptable(
    order: &[Mechanism],
    binding_available: impl Fn(Mechanism) -> bool,
) -> Result<Mechanism, Error> {
    let mut skipped_plus_for_binding = false;
    for &mechanism in order {
        let is_plus = matches!(
            mechanism,
            Mechanism::ScramSha1Plus | Mechanism::ScramSha256Plus
        );
        if is_plus && !binding_available(mechanism) {
            skipped_plus_for_binding = true;
            continue;
        }
        return Ok(mechanism);
    }
    // Two distinct exhaustion shapes share this routing (InvalidInput + Auth ->
    // PolicyBlocked) but warrant different diagnostics: every candidate was a
    // binding-unavailable PLUS rung, versus the empty order / no-compatible-
    // mechanism case (the default-set-vs-`AUTH LOGIN`-only outcome documented
    // in `reference/smtp.md`).
    let message = if skipped_plus_for_binding {
        "channel binding required but unavailable"
    } else {
        "no compatible authentication mechanism"
    };
    Err(error::invalid_input(message).with_phase(SmtpCommandPhase::Auth))
}

/// Resolve cached peer-certificate DER into SCRAM channel binding. Absence is
/// the only skip signal; a present but unusable certificate remains an error.
pub(crate) fn resolve_scram_binding(
    peer_certificate_der: Option<&[u8]>,
) -> Result<Option<ScramChannelBinding>, Error> {
    let Some(der) = peer_certificate_der else {
        return Ok(None);
    };
    let bytes = bifrost_sasl::tls_server_end_point(der)?;
    Ok(Some(ScramChannelBinding::TlsServerEndPoint(bytes)))
}

/// Next client line a [`ScramExchange::step`] produces.
#[derive(Debug)]
pub(crate) enum ScramStep {
    /// Base64 client line to write back to the server.
    Reply(String),
    /// Server-final verified; the caller sends an empty continuation line and
    /// expects the tagged success reply.
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScramExchangeState {
    /// Sent client-first, waiting for server-first.
    AwaitServerFirst,
    /// Sent client-final, waiting for server-final.
    AwaitServerFinal,
    Done,
}

/// Stateful SCRAM exchange computation for SMTP.
///
/// Mirrors IMAP's `AuthenticateScramConsumer`: it only computes the next
/// client line from a decoded server continuation; it never owns the socket.
/// The connection's `auth_scram` driver frames the wire `334` rounds and feeds
/// each decoded continuation into [`ScramExchange::step`].
pub(crate) struct ScramExchange {
    hash: ScramHash,
    binding: ScramChannelBinding,
    password: bifrost_sasl::Secret,
    client_nonce: String,
    client_first_bare: String,
    expected_server_signature: Option<Vec<u8>>,
    state: ScramExchangeState,
}

impl ScramExchange {
    pub(crate) fn new(
        hash: ScramHash,
        binding: ScramChannelBinding,
        username: &str,
        password: bifrost_sasl::Secret,
    ) -> Result<ScramExchange, Error> {
        let client_nonce = generate_scram_nonce()?;
        let client_first_bare = format!(
            "n={},r={client_nonce}",
            bifrost_sasl::prepare_scram_username(username)?
        );
        Ok(ScramExchange {
            hash,
            binding,
            password,
            client_nonce,
            client_first_bare,
            expected_server_signature: None,
            state: ScramExchangeState::AwaitServerFirst,
        })
    }

    /// The client-first message to send after the server's first `334`,
    /// already base64-encoded: `base64(gs2_header || client_first_bare)`. The
    /// GS2 header comes from the binding so it can never desync from the `c=`
    /// value computed in `scram_client_final`.
    pub(crate) fn client_first(&self) -> String {
        crate::base64::encode(format!(
            "{}{}",
            self.binding.gs2_header(),
            self.client_first_bare
        ))
    }

    /// Advance the exchange given a decoded (UTF-8) server continuation.
    pub(crate) fn step(&mut self, server_line: &str) -> Result<ScramStep, Error> {
        match self.state {
            ScramExchangeState::AwaitServerFirst => {
                let (client_final, server_signature) = bifrost_sasl::scram_client_final(
                    self.hash,
                    &self.password,
                    &self.client_nonce,
                    &self.client_first_bare,
                    server_line,
                    &self.binding,
                )?;
                self.expected_server_signature = Some(server_signature);
                self.state = ScramExchangeState::AwaitServerFinal;
                Ok(ScramStep::Reply(crate::base64::encode(
                    client_final.as_bytes(),
                )))
            }
            ScramExchangeState::AwaitServerFinal => {
                let expected = self.expected_server_signature.take().ok_or_else(|| {
                    error::parse("SCRAM server signature missing from client state")
                })?;
                bifrost_sasl::verify_server_final(server_line, &expected)?;
                self.state = ScramExchangeState::Done;
                Ok(ScramStep::Complete)
            }
            ScramExchangeState::Done => Err(error::parse(
                "unexpected SCRAM continuation after server-final",
            )),
        }
    }
}

/// Decode a `334` SASL continuation reply's payload to a UTF-8 string.
///
/// The single base64/UTF-8 decode path shared by the sync and async SCRAM
/// drivers (RFC 4954: server-first / server-final are base64 SASL messages).
/// Errors with a parse-class `Error` when the reply is not a `334`
/// continuation or its payload is not valid base64/UTF-8.
pub(crate) fn decode_auth_challenge(
    response: &crate::transport::smtp::response::Response,
) -> Result<String, Error> {
    if !response.has_code(334) {
        return Err(error::parse("expected a SCRAM 334 continuation"));
    }
    decode_scram_payload(response)
}

/// Decode the base64 SASL payload carried in a reply's first word, regardless
/// of the reply code.
///
/// Used for the SCRAM server-final, which RFC 4954 carries on a `334`
/// continuation but which some servers (field-observed) fold directly onto the
/// `235` success reply (`235 v=...`). The caller is responsible for validating
/// the reply code; this only extracts and decodes the payload.
pub(crate) fn decode_scram_payload(
    response: &crate::transport::smtp::response::Response,
) -> Result<String, Error> {
    let encoded = response
        .first_word()
        .ok_or_else(|| error::parse("could not read SCRAM challenge"))?;
    let decoded = crate::base64::decode(encoded).map_err(error::parse)?;
    String::from_utf8(decoded).map_err(error::parse)
}

/// `ScramHash` for a SCRAM `Mechanism` variant (bound or unbound).
pub(crate) fn scram_hash(mechanism: Mechanism) -> Option<ScramHash> {
    match mechanism {
        Mechanism::ScramSha1 | Mechanism::ScramSha1Plus => Some(ScramHash::Sha1),
        Mechanism::ScramSha256 | Mechanism::ScramSha256Plus => Some(ScramHash::Sha256),
        _ => None,
    }
}

/// 18-byte URL-safe-no-pad base64 client nonce (verbatim shape of IMAP's
/// `generate_scram_nonce`).
fn generate_scram_nonce() -> Result<String, Error> {
    use base64::Engine;

    let mut bytes = [0u8; 18];
    getrandom::fill(&mut bytes).map_err(|e| {
        error::invalid_input(format!("failed to generate SCRAM nonce: {e}"))
            .with_phase(SmtpCommandPhase::Auth)
    })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod test {
    use super::{Credentials, Mechanism, Zeroizing};

    #[test]
    fn scram_mechanism_wire_tokens() {
        use bifrost_sasl::{ChannelBinding, ScramHash};

        assert_eq!(Mechanism::ScramSha1.to_string(), "SCRAM-SHA-1");
        assert_eq!(Mechanism::ScramSha256.to_string(), "SCRAM-SHA-256");
        assert_eq!(Mechanism::ScramSha1Plus.to_string(), "SCRAM-SHA-1-PLUS");
        assert_eq!(Mechanism::ScramSha256Plus.to_string(), "SCRAM-SHA-256-PLUS");

        // Single-authority pin: each token equals the matching bifrost-sasl
        // mechanism name.
        assert_eq!(
            Mechanism::ScramSha1.to_string(),
            ScramHash::Sha1.mechanism_name(ChannelBinding::None)
        );
        assert_eq!(
            Mechanism::ScramSha256.to_string(),
            ScramHash::Sha256.mechanism_name(ChannelBinding::None)
        );
        assert_eq!(
            Mechanism::ScramSha1Plus.to_string(),
            ScramHash::Sha1.mechanism_name(ChannelBinding::TlsServerEndPoint)
        );
        assert_eq!(
            Mechanism::ScramSha256Plus.to_string(),
            ScramHash::Sha256.mechanism_name(ChannelBinding::TlsServerEndPoint)
        );
    }

    #[test]
    fn scram_variants_do_not_support_initial_response() {
        assert!(!Mechanism::ScramSha1.supports_initial_response());
        assert!(!Mechanism::ScramSha256.supports_initial_response());
        assert!(!Mechanism::ScramSha1Plus.supports_initial_response());
        assert!(!Mechanism::ScramSha256Plus.supports_initial_response());
    }

    #[test]
    fn scram_response_encoder_is_an_error() {
        let credentials = Credentials::password("u".to_owned(), "p".to_owned());
        assert!(
            Mechanism::ScramSha256
                .response_with_token(&credentials, None, None)
                .is_err()
        );
        assert!(
            Mechanism::ScramSha256Plus
                .response_with_token(&credentials, Some("anything"), None)
                .is_err()
        );
    }

    #[test]
    fn password_mechanism_order() {
        use super::{PASSWORD_MECHANISMS, password_mechanism_order};
        use crate::transport::smtp::extension::ServerInfo;

        // All SCRAM + PLAIN advertised and allowed.
        let advertised = ServerInfo::with_auth_mechanisms(&[
            Mechanism::ScramSha256Plus,
            Mechanism::ScramSha1Plus,
            Mechanism::ScramSha256,
            Mechanism::ScramSha1,
            Mechanism::Plain,
        ]);
        // With the PLUS rungs advertised, the matching non-PLUS rungs are
        // dropped (RFC 5802 Section 6).
        assert_eq!(
            password_mechanism_order(PASSWORD_MECHANISMS, &advertised),
            vec![
                Mechanism::ScramSha256Plus,
                Mechanism::ScramSha1Plus,
                Mechanism::Plain,
            ]
        );

        // SHA-256-PLUS + SHA-256 advertised: SHA-256 dropped, SHA-256-PLUS
        // first.
        let advertised =
            ServerInfo::with_auth_mechanisms(&[Mechanism::ScramSha256Plus, Mechanism::ScramSha256]);
        assert_eq!(
            password_mechanism_order(PASSWORD_MECHANISMS, &advertised),
            vec![Mechanism::ScramSha256Plus]
        );

        // No PLUS advertised: non-PLUS SCRAM kept.
        let advertised = ServerInfo::with_auth_mechanisms(&[
            Mechanism::ScramSha256,
            Mechanism::ScramSha1,
            Mechanism::Plain,
        ]);
        assert_eq!(
            password_mechanism_order(PASSWORD_MECHANISMS, &advertised),
            vec![
                Mechanism::ScramSha256,
                Mechanism::ScramSha1,
                Mechanism::Plain
            ]
        );

        // LOGIN only appears when in `allowed`, and always last.
        let allowed = [Mechanism::Plain, Mechanism::Login];
        let advertised = ServerInfo::with_auth_mechanisms(&[Mechanism::Login, Mechanism::Plain]);
        assert_eq!(
            password_mechanism_order(&allowed, &advertised),
            vec![Mechanism::Plain, Mechanism::Login]
        );

        // LOGIN-only server under the default `PASSWORD_MECHANISMS` (LOGIN not
        // allowed): empty order. Pins the migration-note regression.
        let advertised = ServerInfo::with_auth_mechanisms(&[Mechanism::Login]);
        assert!(password_mechanism_order(PASSWORD_MECHANISMS, &advertised).is_empty());
    }

    #[test]
    fn default_password_mechanisms() {
        use super::{DEFAULT_MECHANISMS, PASSWORD_MECHANISMS};

        assert_eq!(
            PASSWORD_MECHANISMS,
            &[
                Mechanism::ScramSha256Plus,
                Mechanism::ScramSha1Plus,
                Mechanism::ScramSha256,
                Mechanism::ScramSha1,
                Mechanism::Plain,
            ]
        );
        assert_eq!(DEFAULT_MECHANISMS, PASSWORD_MECHANISMS);
        // LOGIN is opt-in: not in the default set.
        assert!(!PASSWORD_MECHANISMS.contains(&Mechanism::Login));
    }

    #[test]
    fn scram_binding_skip_falls_through() {
        use super::first_attemptable;
        use crate::transport::smtp::error::SmtpCommandPhase;

        // PLUS rungs first, but binding unavailable -> fall through to PLAIN.
        let order = [
            Mechanism::ScramSha256Plus,
            Mechanism::ScramSha1Plus,
            Mechanism::Plain,
        ];
        assert_eq!(
            first_attemptable(&order, |_| false).unwrap(),
            Mechanism::Plain
        );

        // A PLUS rung whose binding *is* available is chosen.
        assert_eq!(
            first_attemptable(&order, |_| true).unwrap(),
            Mechanism::ScramSha256Plus
        );

        // All-PLUS order with no binding -> Auth-phase invalid_input error,
        // diagnosed as a channel-binding failure (every candidate was a
        // binding-unavailable PLUS rung).
        let all_plus = [Mechanism::ScramSha256Plus, Mechanism::ScramSha1Plus];
        let err = first_attemptable(&all_plus, |_| false).unwrap_err();
        assert!(err.is_invalid_input());
        assert_eq!(err.phase(), Some(SmtpCommandPhase::Auth));
        assert_eq!(
            err.diagnostic_text().as_deref(),
            Some("channel binding required but unavailable")
        );

        // Empty order -> same routing (Auth-phase invalid_input) but the
        // no-compatible-mechanism diagnostic, not the channel-binding one (no
        // PLUS rung was skipped).
        let err = first_attemptable(&[], |_| true).unwrap_err();
        assert!(err.is_invalid_input());
        assert_eq!(err.phase(), Some(SmtpCommandPhase::Auth));
        assert_eq!(
            err.diagnostic_text().as_deref(),
            Some("no compatible authentication mechanism")
        );
    }

    #[test]
    fn scram_binding_only_treats_absent_certificate_as_unavailable() {
        use super::resolve_scram_binding;

        assert!(resolve_scram_binding(None).unwrap().is_none());
        let err = resolve_scram_binding(Some(&[0x30, 0x01, 0x00])).unwrap_err();
        assert!(err.is_parse());
    }

    #[test]
    fn scram_sasl_error_mapping() {
        use crate::transport::smtp::error::{ErrorKind, SmtpCommandPhase};

        let protocol: super::Error =
            bifrost_sasl::SaslError::Protocol("bad server-first".into()).into();
        assert_eq!(protocol.kind(), &ErrorKind::Parse);

        let auth_failed: super::Error =
            bifrost_sasl::SaslError::AuthFailed("server rejected".into()).into();
        assert_eq!(auth_failed.kind(), &ErrorKind::InvalidInput);
        assert_eq!(auth_failed.phase(), Some(SmtpCommandPhase::Auth));
    }

    #[test]
    fn scram_exchange_none_binding_drives_to_complete() {
        use super::{ScramExchange, ScramStep, scram_hash};
        use bifrost_sasl::ScramChannelBinding;

        let hash = scram_hash(Mechanism::ScramSha256).unwrap();
        let mut exchange =
            ScramExchange::new(hash, ScramChannelBinding::None, "user", "pencil".into()).unwrap();

        // client-first decodes to `n,,n=user,r=<nonce>`.
        let decoded =
            String::from_utf8(crate::base64::decode(exchange.client_first()).unwrap()).unwrap();
        assert!(decoded.starts_with("n,,n=user,r="), "got: {decoded}");
        let nonce = decoded.rsplit("r=").next().unwrap().to_owned();

        // Hand-build an RFC-5802-shaped server-first reusing the client nonce.
        let server_first = format!("r={nonce}srvextra,s=QSXCR+Q6sek8bf92,i=4096");

        // Drive the exchange with the server-first; it must reply.
        let reply = exchange.step(&server_first).unwrap();
        let client_final_b64 = match reply {
            ScramStep::Reply(s) => s,
            ScramStep::Complete => panic!("expected Reply on server-first"),
        };
        assert!(!client_final_b64.is_empty());

        // Recompute the expected server-final from the exchange's own state by
        // re-deriving the signature through scram_client_final with the exact
        // client-first-bare the exchange used.
        let client_first_bare = format!("n=user,r={nonce}");
        let password: bifrost_sasl::Secret = "pencil".into();
        let (_cf, sig) = bifrost_sasl::scram_client_final(
            hash,
            &password,
            &nonce,
            &client_first_bare,
            &server_first,
            &ScramChannelBinding::None,
        )
        .unwrap();
        use base64::Engine;
        let server_final = format!(
            "v={}",
            base64::engine::general_purpose::STANDARD.encode(&sig)
        );
        assert!(matches!(
            exchange.step(&server_final).unwrap(),
            ScramStep::Complete
        ));

        // A continuation after Done is a parse-class error.
        assert!(exchange.step(&server_final).unwrap_err().is_parse());
    }

    #[test]
    fn scram_exchange_plus_binding_gs2_prefix() {
        use super::{ScramExchange, scram_hash};
        use bifrost_sasl::ScramChannelBinding;

        let hash = scram_hash(Mechanism::ScramSha256Plus).unwrap();
        let exchange = ScramExchange::new(
            hash,
            ScramChannelBinding::TlsServerEndPoint(vec![1, 2, 3, 4]),
            "us=,er",
            "pw".into(),
        )
        .unwrap();
        let decoded =
            String::from_utf8(crate::base64::decode(exchange.client_first()).unwrap()).unwrap();
        assert!(
            decoded.starts_with("p=tls-server-end-point,,n="),
            "got: {decoded}"
        );
        // saslname escaping: `=` -> `=3D`, `,` -> `=2C`.
        assert!(decoded.contains("n=us=3D=2Cer,r="), "got: {decoded}");
    }

    #[test]
    fn scram_exchange_malformed_server_first_is_parse() {
        use super::{ScramExchange, scram_hash};
        use bifrost_sasl::ScramChannelBinding;

        let hash = scram_hash(Mechanism::ScramSha256).unwrap();
        let mut exchange =
            ScramExchange::new(hash, ScramChannelBinding::None, "user", "pw".into()).unwrap();
        // Not a SCRAM server-first message at all.
        let err = exchange.step("garbage-without-r=").unwrap_err();
        assert!(err.is_parse(), "expected parse-class, got {err:?}");
    }

    #[test]
    fn scram_exchange_tampered_server_final_is_parse() {
        use super::{ScramExchange, ScramStep, scram_hash};
        use bifrost_sasl::ScramChannelBinding;

        let hash = scram_hash(Mechanism::ScramSha256).unwrap();
        let mut exchange =
            ScramExchange::new(hash, ScramChannelBinding::None, "user", "pencil".into()).unwrap();
        let decoded =
            String::from_utf8(crate::base64::decode(exchange.client_first()).unwrap()).unwrap();
        let nonce = decoded.rsplit("r=").next().unwrap().to_owned();
        let server_first = format!("r={nonce}srvextra,s=QSXCR+Q6sek8bf92,i=4096");
        assert!(matches!(
            exchange.step(&server_first).unwrap(),
            ScramStep::Reply(_)
        ));
        // A wrong v= signature is a protocol-class verification failure (the
        // bifrost-sasl contract: signature mismatch is `Protocol`, not
        // `AuthFailed`), so it maps to a parse-class SMTP error.
        let err = exchange.step("v=AAAA").unwrap_err();
        assert!(err.is_parse(), "got {err:?}");
    }

    #[test]
    fn scram_exchange_server_error_is_auth_failure() {
        use super::{ScramExchange, ScramStep, scram_hash};
        use crate::transport::smtp::error::SmtpCommandPhase;
        use bifrost_sasl::ScramChannelBinding;

        let hash = scram_hash(Mechanism::ScramSha256).unwrap();
        let mut exchange =
            ScramExchange::new(hash, ScramChannelBinding::None, "user", "pencil".into()).unwrap();
        let decoded =
            String::from_utf8(crate::base64::decode(exchange.client_first()).unwrap()).unwrap();
        let nonce = decoded.rsplit("r=").next().unwrap().to_owned();
        let server_first = format!("r={nonce}srvextra,s=QSXCR+Q6sek8bf92,i=4096");
        assert!(matches!(
            exchange.step(&server_first).unwrap(),
            ScramStep::Reply(_)
        ));
        // A SCRAM `e=` server error is a credential auth failure and must land
        // on the Auth-phase InvalidInput lane (Authorization(PolicyBlocked)).
        let err = exchange.step("e=invalid-proof").unwrap_err();
        assert!(err.is_invalid_input(), "got {err:?}");
        assert_eq!(err.phase(), Some(SmtpCommandPhase::Auth));
    }

    #[test]
    fn server_final_decodes_from_either_334_or_235() {
        use super::{decode_auth_challenge, decode_scram_payload};
        use crate::transport::smtp::response::{Category, Code, Detail, Response, Severity};

        let payload = crate::base64::encode("v=server-signature");

        // RFC 4954 shape: server-final on a 334 continuation.
        let continuation = Response::new(
            Code {
                severity: Severity::PositiveIntermediate,
                category: Category::Unspecified3,
                detail: Detail::Four,
            },
            vec![payload.clone()],
        );
        assert_eq!(
            decode_auth_challenge(&continuation).unwrap(),
            "v=server-signature"
        );
        assert_eq!(
            decode_scram_payload(&continuation).unwrap(),
            "v=server-signature"
        );

        // Field-observed shape: server-final folded onto the 235 success reply.
        // The 334-only decoder rejects it (the spurious parse error this fix
        // removes); the code-agnostic decoder accepts it.
        let success = Response::new(
            Code {
                severity: Severity::PositiveCompletion,
                category: Category::Unspecified3,
                detail: Detail::Five,
            },
            vec![payload],
        );
        assert!(decode_auth_challenge(&success).is_err());
        assert_eq!(
            decode_scram_payload(&success).unwrap(),
            "v=server-signature"
        );
    }

    #[test]
    fn test_plain() {
        let mechanism = Mechanism::Plain;

        let credentials = Credentials::password("username".to_owned(), "password".to_owned());

        assert_eq!(
            mechanism
                .response_with_token(&credentials, None, None)
                .unwrap(),
            "\u{0}username\u{0}password"
        );
        assert!(
            mechanism
                .response_with_token(&credentials, Some("test"), None)
                .is_err()
        );
    }

    #[test]
    fn plain_rejects_nul_in_credentials() {
        let mechanism = Mechanism::Plain;

        // authzid injection: the NUL would open a third PLAIN field.
        let injected =
            Credentials::password("admin\u{0}username".to_owned(), "password".to_owned());
        assert!(
            mechanism
                .response_with_token(&injected, None, None)
                .is_err()
        );

        let injected_password =
            Credentials::password("username".to_owned(), "pass\u{0}word".to_owned());
        assert!(
            mechanism
                .response_with_token(&injected_password, None, None)
                .is_err()
        );
    }

    #[test]
    fn test_login() {
        let mechanism = Mechanism::Login;

        let credentials = Credentials::password("alice".to_owned(), "wonderland".to_owned());

        assert_eq!(
            mechanism
                .response_with_token(&credentials, Some("Username"), None)
                .unwrap(),
            "alice"
        );
        assert_eq!(
            mechanism
                .response_with_token(&credentials, Some("Password"), None)
                .unwrap(),
            "wonderland"
        );
        assert!(
            mechanism
                .response_with_token(&credentials, None, None)
                .is_err()
        );
    }

    #[test]
    fn test_login_case_insensitive() {
        let mechanism = Mechanism::Login;

        let credentials = Credentials::password("alice".to_owned(), "wonderland".to_owned());

        assert_eq!(
            mechanism
                .response_with_token(&credentials, Some("username"), None)
                .unwrap(),
            "alice"
        );
        assert_eq!(
            mechanism
                .response_with_token(&credentials, Some("password"), None)
                .unwrap(),
            "wonderland"
        );
        assert!(
            mechanism
                .response_with_token(&credentials, None, None)
                .is_err()
        );
    }

    #[test]
    fn test_xoauth2() {
        let mechanism = Mechanism::Xoauth2;

        let credentials = Credentials::oauth2(
            "username".to_owned(),
            "vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==".to_owned(),
        );

        let token = "vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==";
        assert_eq!(
            mechanism
                .response_with_token(&credentials, None, Some(token))
                .unwrap(),
            "user=username\x01auth=Bearer vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==\x01\x01"
        );
        // A 334 challenge after the initial response is XOAUTH2's failed-auth
        // shape (`334 <base64-json-error>`); like OAUTHBEARER, the encoder
        // returns the `\x01` dummy-cancel so the server emits the tagged
        // failure reply on the next read.
        assert_eq!(
            mechanism
                .response_with_token(&credentials, Some(r#"{"status":"401"}"#), Some(token))
                .unwrap(),
            "\x01"
        );
    }

    #[test]
    fn test_oauthbearer() {
        let mechanism = Mechanism::OAuthBearer;

        let credentials = Credentials::oauth2(
            "user@example.com".to_owned(),
            "vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==".to_owned(),
        );

        let token = "vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==";
        let response = mechanism
            .response_with_token(&credentials, None, Some(token))
            .unwrap();
        assert_eq!(
            response,
            "n,a=user@example.com,\x01auth=Bearer vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==\x01\x01"
        );
        assert!(!response.contains("\x01host="));
        assert!(!response.contains("\x01port="));
        assert_eq!(
            mechanism
                .response_with_token(&credentials, Some("{}"), Some(token))
                .unwrap(),
            "\x01"
        );
        assert_eq!(
            mechanism
                .response_with_token(
                    &credentials,
                    Some(r#"{"status":"invalid_token"}"#),
                    Some(token)
                )
                .unwrap(),
            "\x01"
        );
    }

    #[test]
    fn test_oauthbearer_escapes_gs2_identity() {
        let mechanism = Mechanism::OAuthBearer;
        let credentials = Credentials::oauth2("a,b=c".to_owned(), "token".to_owned());

        assert_eq!(
            mechanism
                .response_with_token(&credentials, None, Some("token"))
                .unwrap(),
            "n,a=a=2Cb=3Dc,\x01auth=Bearer token\x01\x01"
        );
    }

    #[test]
    fn test_rejects_wrong_credential_kind() {
        assert!(
            Mechanism::Plain
                .response_with_token(
                    &Credentials::oauth2("alice".to_owned(), "token".to_owned()),
                    None,
                    Some("token"),
                )
                .is_err()
        );
        assert!(
            Mechanism::Xoauth2
                .response_with_token(
                    &Credentials::password("alice".to_owned(), "wonderland".to_owned()),
                    None,
                    None,
                )
                .is_err()
        );
    }

    #[test]
    fn test_accepts_zeroizing_secrets() {
        let credentials = Credentials::oauth2(
            "alice".to_owned(),
            Zeroizing::new("access-token".to_owned()),
        );

        // The token threads through the source convenience constructor and
        // is read back by the blocking resolver.
        let (_identity, token) = credentials.oauth2_token_blocking().unwrap();
        assert_eq!(
            Mechanism::Xoauth2
                .response_with_token(&credentials, None, Some(token.as_str()))
                .unwrap(),
            "user=alice\x01auth=Bearer access-token\x01\x01"
        );
    }

    #[tokio::test]
    async fn oauth2_payload_reads_current_token() {
        use bifrost_net::{AccessToken, StaticTokenSource};
        use std::sync::Arc;

        let source = StaticTokenSource::new("old-token", None);
        let credentials = Credentials::oauth2_source("user@example.com", Arc::new(source.clone()));

        // The SASL payload is built from the token the source currently
        // holds, both through the async read and the blocking read.
        let (_identity, token) = credentials.oauth2_token().await.unwrap();
        let payload = Mechanism::OAuthBearer
            .response_with_token(&credentials, None, Some(token.as_str()))
            .unwrap();
        assert!(payload.contains("auth=Bearer old-token"), "got: {payload}");

        // Rotate the token on the shared source; the next read presents the
        // new token with no reconstruction of the credential.
        source.set(AccessToken::new("new-token", None));
        let (_identity, token) = credentials.oauth2_token().await.unwrap();
        let payload = Mechanism::Xoauth2
            .response_with_token(&credentials, None, Some(token.as_str()))
            .unwrap();
        assert!(payload.contains("auth=Bearer new-token"), "got: {payload}");

        let (_identity, token) = credentials.oauth2_token_blocking().unwrap();
        assert_eq!(token.as_str(), "new-token");
    }
}
