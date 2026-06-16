#[cfg(unix)]
use std::path::Path;
use std::sync::Arc;
use std::{fmt::Debug, time::Duration};

use bifrost_types::error::{AccountError, BatchItem, BatchOutcome};

use super::PoolConfig;
use super::batch::{SmtpBatchRecipient, batch_input_invalid_error, batch_level_error};
use super::pool::sync_impl::Pool;
use super::{
    ClientId, Credentials, Error, Mechanism, Protocol, Response, SendOptions, SmtpConnection,
    SmtpInfo, error,
};
use super::{SUBMISSION_PORT, SUBMISSIONS_PORT, Tls, TlsParameters};
use crate::address::Address;
use crate::transport::smtp::account_error::SmtpErrorContext;
use crate::transport::smtp::authentication::IntoSecretString;
use crate::{Transport, address::Envelope};

/// Synchronously send emails using the SMTP protocol
///
/// `SmtpTransport` is the primary way for communicating
/// with SMTP relay servers to send email messages. It holds the
/// client connect configuration and creates new connections
/// as necessary.
///
/// # Connection pool
///
/// `SmtpTransport` maintains a connection pool to manage SMTP connections. The
/// pool:
///
/// - Establishes a new connection when sending a message.
/// - Recycles connections internally after a message is sent.
/// - Reuses connections for subsequent messages, reducing connection setup overhead.
///
/// The connection pool can grow to hold multiple SMTP connections if multiple
/// emails are sent concurrently, as SMTP does not support multiplexing within a
/// single connection.
///
/// However, **connection reuse is not possible** if the `SmtpTransport` instance
/// is dropped after every email send operation. You must reuse the instance
/// of this struct for the connection pool to be of any use.
///
/// To customize connection pool settings, use [`SmtpTransportBuilder::pool_config`].
#[derive(Clone)]
pub struct SmtpTransport {
    inner: Arc<Pool>,
}

/// Synchronously send emails using the LMTP protocol
///
/// `LmtpTransport` is the local-delivery counterpart to [`SmtpTransport`].
/// LMTP uses `LHLO` for capability discovery and returns one status per
/// envelope recipient. Rejected recipients carry their `RCPT` response;
/// accepted recipients carry their post-DATA delivery response.
#[derive(Clone)]
pub struct LmtpTransport {
    inner: Arc<Pool>,
}

impl Transport for SmtpTransport {
    type Ok = Response;
    type Error = Error;

    /// Sends an email
    fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send(envelope, email)?;

        Ok(result)
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }
}

impl Transport for LmtpTransport {
    type Ok = Vec<Response>;
    type Error = Error;

    /// Sends an email and returns one LMTP status per recipient.
    fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send_lmtp(envelope, email)?;

        Ok(result)
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }
}

impl Debug for SmtpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_struct("SmtpTransport");
        builder.field("inner", &self.inner);
        builder.finish()
    }
}

impl Debug for LmtpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_struct("LmtpTransport");
        builder.field("inner", &self.inner);
        builder.finish()
    }
}

impl SmtpTransport {
    /// Simple and secure transport, using TLS connections to communicate with the SMTP server
    ///
    /// The right option for most SMTP servers.
    ///
    /// Creates an encrypted transport over submissions port, using the provided domain
    /// to validate TLS certificates.
    pub fn relay(relay: &str) -> Result<SmtpTransportBuilder, Error> {
        let tls_parameters = TlsParameters::new(relay.into())?;

        Ok(Self::builder_dangerous(relay)
            .port(SUBMISSIONS_PORT)
            .tls(Tls::Wrapper(tls_parameters)))
    }

    /// Simple and secure transport, using STARTTLS to obtain encrypted connections
    ///
    /// Alternative to [`SmtpTransport::relay`](#method.relay), for SMTP servers
    /// that don't take SMTPS connections.
    ///
    /// Creates an encrypted transport over submissions port, by first connecting using
    /// an unencrypted connection and then upgrading it with STARTTLS. The provided
    /// domain is used to validate TLS certificates.
    ///
    /// An error is returned if the connection can't be upgraded. No credentials
    /// or emails will be sent to the server, protecting from downgrade attacks.
    pub fn starttls_relay(relay: &str) -> Result<SmtpTransportBuilder, Error> {
        let tls_parameters = TlsParameters::new(relay.into())?;

        Ok(Self::builder_dangerous(relay)
            .port(SUBMISSION_PORT)
            .tls(Tls::Required(tls_parameters)))
    }

    /// Creates a new local SMTP client to port 25
    ///
    /// Shortcut for local unencrypted relay (typical local email daemon that will handle relaying)
    pub fn unencrypted_localhost() -> SmtpTransport {
        Self::builder_dangerous("localhost").build()
    }

    /// Creates a new SMTP client
    ///
    /// Defaults are:
    ///
    /// * No authentication
    /// * No TLS
    /// * A 10-second timeout for SMTP commands
    /// * Port 25
    ///
    /// Consider using [`SmtpTransport::relay`](#method.relay) or
    /// [`SmtpTransport::starttls_relay`](#method.starttls_relay) instead,
    /// if possible.
    pub fn builder_dangerous<T: Into<String>>(server: T) -> SmtpTransportBuilder {
        SmtpTransportBuilder::new(server)
    }

    /// Creates a `SmtpTransportBuilder` from a connection URL
    ///
    /// The protocol, credentials, host, port and EHLO name can be provided
    /// in a single URL. This may be simpler than having to configure SMTP
    /// through multiple configuration parameters and then having to pass
    /// those options to Bifrost SMTP.
    ///
    /// The URL is created in the following way:
    /// `scheme://user:pass@hostname:port/ehlo-name?tls=TLS`.
    ///
    /// `user` (Username) and `pass` (Password) are optional in case the
    /// SMTP relay doesn't require authentication. When `port` is not
    /// configured it is automatically determined based on the `scheme`.
    /// `ehlo-name` optionally overwrites the hostname sent for the EHLO
    /// command. `TLS` controls whether STARTTLS is simply enabled
    /// (`opportunistic` - not enough to prevent man-in-the-middle attacks)
    /// or `required` (require the server to upgrade the connection to
    /// STARTTLS, otherwise fail on suspicion of main-in-the-middle attempt).
    ///
    /// Use the following table to construct your SMTP url:
    ///
    /// | scheme  | `tls` query parameter | example                                            | default port | remarks                                                                                                                               |
    /// | ------- | --------------------- | -------------------------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
    /// | `smtps` | unset                 | `smtps://user:pass@hostname:port`                  | 465          | SMTP over TLS, recommended method                                                                                                     |
    /// | `smtp`  | `required`            | `smtp://user:pass@hostname:port?tls=required`      | 587          | SMTP with STARTTLS required, when SMTP over TLS is not available                                                                      |
    /// | `smtp`  | `opportunistic`       | `smtp://user:pass@hostname:port?tls=opportunistic` | 587          | SMTP with optionally STARTTLS when supported by the server. Not suitable for production use: vulnerable to a man-in-the-middle attack |
    /// | `smtp`  | unset                 | `smtp://user:pass@hostname:port`                   | 587          | Always unencrypted SMTP. Credentials are refused by default; message data is still unencrypted                                        |
    ///
    /// IMPORTANT: some parameters like `user` and `pass` cannot simply
    /// be concatenated to construct the final URL because special characters
    /// contained within the parameter may confuse the URL decoder.
    /// Manually URL encode the parameters before concatenating them or use
    /// a proper URL encoder, like the following cargo script:
    ///
    /// ```rust
    /// # const TOML: &str = r#"
    /// #!/usr/bin/env cargo
    ///
    /// //! ```cargo
    /// //! [dependencies]
    /// //! url = "2"
    /// //! ```
    /// # "#;
    ///
    /// use url::Url;
    ///
    /// fn main() {
    ///     // don't touch this line
    ///     let mut url = Url::parse("foo://bar").unwrap();
    ///
    ///     // configure the scheme (`smtp` or `smtps`) here.
    ///     url.set_scheme("smtps").unwrap();
    ///     // configure the username and password.
    ///     // remove the following two lines if unauthenticated.
    ///     url.set_username("username").unwrap();
    ///     url.set_password(Some("password")).unwrap();
    ///     // configure the hostname
    ///     url.set_host(Some("smtp.example.com")).unwrap();
    ///     // configure the port - only necessary if using a non-default port
    ///     url.set_port(Some(465)).unwrap();
    ///     // configure the EHLO name
    ///     url.set_path("ehlo-name");
    ///
    ///     println!("{url}");
    /// }
    /// ```
    ///
    /// The connection URL can then be used in the following way:
    /// If a plaintext URL contains credentials, authentication is refused at
    /// connection time unless
    /// [`SmtpTransportBuilder::dangerous_allow_insecure_auth`] is enabled.
    ///
    /// ```rust,no_run
    /// use bifrost_smtp::{
    ///     Message, SmtpTransport, Transport, message::header::ContentType,
    /// };
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let email = Message::builder()
    ///     .from("NoBody <nobody@domain.tld>".parse().unwrap())
    ///     .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
    ///     .to("Hei <hei@domain.tld>".parse().unwrap())
    ///     .subject("Happy new year")
    ///     .header(ContentType::TEXT_PLAIN)
    ///     .body(String::from("Be happy!"))
    ///     .unwrap();
    ///
    /// // Open a remote connection to example
    /// let mailer = SmtpTransport::from_url("smtps://username:password@smtp.example.com")?.build();
    ///
    /// // Send the email
    /// mailer.send(&email)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_url(connection_url: &str) -> Result<SmtpTransportBuilder, Error> {
        super::connection_url::from_connection_url(connection_url)
    }

    /// Tests the SMTP connection
    ///
    /// `test_connection()` tests the connection by using the SMTP NOOP command.
    pub fn test_connection(&self) -> Result<bool, Error> {
        let mut conn = self.inner.connection()?;

        let is_connected = conn.test_connected();

        Ok(is_connected)
    }

    /// Sends `VRFY` and returns the server response.
    ///
    /// Many servers disable `VRFY` for privacy. Negative SMTP replies are
    /// returned as [`Response`] values so callers can inspect the exact status.
    pub fn verify(&self, argument: impl Into<String>) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;
        conn.verify(argument)
    }

    /// Sends `EXPN` and returns the server response.
    ///
    /// Many servers disable `EXPN` for privacy. Negative SMTP replies are
    /// returned as [`Response`] values so callers can inspect the exact status.
    pub fn expand(&self, argument: impl Into<String>) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;
        conn.expand(argument)
    }

    /// Sends an email with per-message SMTP options.
    ///
    /// This is the advanced counterpart to [`Transport::send_raw`]. It keeps
    /// the core transport trait simple while still allowing message-specific
    /// ESMTP parameters such as `REQUIRETLS`, `DELIVERBY`, `FUTURERELEASE`,
    /// `MT-PRIORITY`, and DSN options.
    pub fn send_raw_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send_with_options(envelope, email, options)?;

        Ok(result)
    }

    /// Sends an email with `BDAT ... LAST`.
    ///
    /// The server must advertise `CHUNKING`. This path avoids DATA
    /// dot-stuffing and is the required path for `BODY=BINARYMIME`.
    pub fn send_raw_bdat(&self, envelope: &Envelope, email: &[u8]) -> Result<Response, Error> {
        self.send_raw_bdat_with_options(envelope, email, &SendOptions::default())
    }

    /// Sends an email with `BDAT ... LAST` and per-message SMTP options.
    pub fn send_raw_bdat_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send_bdat_with_options(envelope, email, options)?;

        Ok(result)
    }

    /// Account-oriented multi-recipient send over SMTP.
    ///
    /// Validates batch input, runs MAIL FROM / RCPT TO / DATA body, and returns
    /// a `BatchOutcome<()>` with every submitted recipient accounted for in
    /// exactly one lane (`succeeded`, `failed`, or `uncertain`).
    ///
    /// Returns `Err(AccountError)` only for batch-level failures where no
    /// per-recipient outcome can be attributed: pool checkout failure, connection
    /// setup failure, MAIL FROM rejection, or a pre-MAIL transport drop.
    pub fn send_raw_batch_with_options(
        &self,
        from: Option<Address>,
        recipients: Vec<BatchItem<Address>>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<BatchOutcome<()>, AccountError> {
        let ctx = SmtpErrorContext::send(Protocol::Smtp);

        if let Err(invalid) = bifrost_types::error::validate_batch_input(&recipients) {
            return Err(batch_input_invalid_error(Protocol::Smtp, invalid));
        }

        let batch_recipients: Vec<SmtpBatchRecipient> = recipients
            .into_iter()
            .map(SmtpBatchRecipient::from)
            .collect();

        let mut conn = self
            .inner
            .connection()
            .map_err(|e| batch_level_error(e, ctx.clone()))?;

        match conn.send_smtp_batch(from, batch_recipients, email, options) {
            Ok(progress) => Ok(progress.resolve()),
            Err((e, _progress)) => Err(batch_level_error(e, ctx)),
        }
    }
}

impl LmtpTransport {
    /// Creates a new local LMTP client to port 24.
    ///
    /// RFC 2033 does not assign an LMTP TCP port. Port 24 is the common TCP
    /// convention. Use [`Self::unix_socket`] for the more common local socket
    /// deployment shape.
    pub fn unencrypted_localhost() -> LmtpTransport {
        Self::builder_dangerous("localhost").build()
    }

    /// Creates a new local LMTP client over a Unix-domain socket.
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    pub fn unix_socket(path: impl AsRef<Path>) -> LmtpTransportBuilder {
        Self::builder_dangerous("localhost").unix_socket(path)
    }

    /// Creates a new LMTP client.
    ///
    /// Defaults are:
    ///
    /// * No authentication
    /// * No TLS
    /// * A 10-second timeout for SMTP commands
    /// * Port 24, the common TCP LMTP convention
    pub fn builder_dangerous<T: Into<String>>(server: T) -> LmtpTransportBuilder {
        LmtpTransportBuilder::new(server)
    }

    /// Tests the LMTP connection.
    ///
    /// `test_connection()` tests the connection by using the SMTP NOOP command.
    pub fn test_connection(&self) -> Result<bool, Error> {
        let mut conn = self.inner.connection()?;

        let is_connected = conn.test_connected();

        Ok(is_connected)
    }

    /// Sends `VRFY` over LMTP and returns the server response.
    ///
    /// Negative replies are returned as [`Response`] values so callers can
    /// inspect the exact status.
    pub fn verify(&self, argument: impl Into<String>) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;
        conn.verify(argument)
    }

    /// Sends `EXPN` over LMTP and returns the server response.
    ///
    /// Negative replies are returned as [`Response`] values so callers can
    /// inspect the exact status.
    pub fn expand(&self, argument: impl Into<String>) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;
        conn.expand(argument)
    }

    /// Sends an email over LMTP with per-message SMTP options.
    pub fn send_raw_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send_lmtp_with_options(envelope, email, options)?;

        Ok(result)
    }

    /// Sends an email over LMTP with `BDAT ... LAST`.
    ///
    /// The server must advertise `CHUNKING`. Returned responses still preserve
    /// one status per input recipient.
    pub fn send_raw_bdat(&self, envelope: &Envelope, email: &[u8]) -> Result<Vec<Response>, Error> {
        self.send_raw_bdat_with_options(envelope, email, &SendOptions::default())
    }

    /// Sends an email over LMTP with `BDAT ... LAST` and per-message options.
    pub fn send_raw_bdat_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send_lmtp_bdat_with_options(envelope, email, options)?;

        Ok(result)
    }

    /// Account-oriented multi-recipient send over LMTP.
    ///
    /// Validates batch input, runs MAIL FROM / RCPT TO / DATA body, reads one
    /// final status per accepted recipient, and returns a `BatchOutcome<()>`
    /// with every submitted recipient accounted for exactly once.
    ///
    /// Returns `Err(AccountError)` only for batch-level failures where no
    /// per-recipient outcome can be attributed: pool checkout failure, connection
    /// setup failure, MAIL FROM rejection, or a pre-MAIL transport drop.
    pub fn send_raw_batch_with_options(
        &self,
        from: Option<Address>,
        recipients: Vec<BatchItem<Address>>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<BatchOutcome<()>, AccountError> {
        let ctx = SmtpErrorContext::send(Protocol::Lmtp);

        if let Err(invalid) = bifrost_types::error::validate_batch_input(&recipients) {
            return Err(batch_input_invalid_error(Protocol::Lmtp, invalid));
        }

        let batch_recipients: Vec<SmtpBatchRecipient> = recipients
            .into_iter()
            .map(SmtpBatchRecipient::from)
            .collect();

        let mut conn = self
            .inner
            .connection()
            .map_err(|e| batch_level_error(e, ctx.clone()))?;

        match conn.send_lmtp_batch(from, batch_recipients, email, options) {
            Ok(progress) => Ok(progress.resolve()),
            Err((e, _progress)) => Err(batch_level_error(e, ctx)),
        }
    }
}

/// Contains client configuration.
/// Instances of this struct can be created using functions of [`SmtpTransport`].
#[derive(Debug, Clone)]
pub struct SmtpTransportBuilder {
    info: SmtpInfo,
    pool_config: PoolConfig,
}

/// Contains LMTP client configuration.
/// Instances of this struct can be created using functions of [`LmtpTransport`].
#[derive(Debug, Clone)]
pub struct LmtpTransportBuilder {
    info: SmtpInfo,
    pool_config: PoolConfig,
}

/// Builder for the SMTP `SmtpTransport`
impl SmtpTransportBuilder {
    // Create new builder with default parameters
    pub(crate) fn new<T: Into<String>>(server: T) -> Self {
        Self {
            info: SmtpInfo::new(server, Protocol::Smtp),
            pool_config: PoolConfig::default(),
        }
    }

    /// Set the name used during EHLO
    pub fn hello_name(mut self, name: ClientId) -> Self {
        self.info.hello_name = name;
        self
    }

    /// Set the authentication credentials to use
    ///
    /// Unless [`Self::authentication`] was called explicitly, this also selects
    /// the default mechanisms for the credential kind.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.info.set_credentials(credentials);
        self
    }

    /// Set username and password authentication credentials.
    pub fn password<U, P>(self, username: U, password: P) -> Self
    where
        U: Into<String>,
        P: IntoSecretString,
    {
        self.credentials(Credentials::password(username, password))
    }

    /// Set OAuth 2.0 bearer-token authentication credentials.
    ///
    /// This configures `OAUTHBEARER` and `XOAUTH2`, in that preference order.
    pub fn oauth2<I, T>(self, identity: I, access_token: T) -> Self
    where
        I: Into<String>,
        T: IntoSecretString,
    {
        self.credentials(Credentials::oauth2(identity, access_token))
    }

    /// Set the authentication mechanism to use
    pub fn authentication(mut self, mechanisms: Vec<Mechanism>) -> Self {
        self.info.set_authentication(mechanisms);
        self
    }

    /// Allow credentials to be sent over an unencrypted SMTP connection.
    ///
    /// By default, Bifrost LMTP refuses to send passwords or bearer tokens
    /// unless the connection is already encrypted by TLS or has been upgraded
    /// with STARTTLS. Set this only for trusted local relays or test servers.
    pub fn dangerous_allow_insecure_auth(mut self, allow: bool) -> Self {
        self.info.allow_insecure_auth = allow;
        self
    }

    /// Set the timeout duration
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.info.timeout = timeout;
        self
    }

    /// Set the port to use
    ///
    /// # Warning
    ///
    /// You probably do not need to call this method.
    ///
    /// Bifrost SMTP usually picks the correct `port` when building
    /// [`SmtpTransport`] using [`SmtpTransport::relay`] or
    /// [`SmtpTransport::starttls_relay`].
    ///
    /// # Errors
    ///
    /// Using the incorrect `port` and [`Self::tls`] combination may
    /// lead to hard to debug IO errors coming from the TLS library.
    pub fn port(mut self, port: u16) -> Self {
        self.info.port = port;
        self
    }

    /// Set the TLS settings to use.
    ///
    /// LMTP-over-TLS and STARTTLS are supported through the same TLS modes as
    /// SMTP. After STARTTLS succeeds, the client refreshes capabilities with
    /// `LHLO`.
    ///
    /// # Warning
    ///
    /// You probably do not need to call this method.
    ///
    /// By default Bifrost SMTP chooses the correct `tls` configuration when
    /// building [`SmtpTransport`] using [`SmtpTransport::relay`] or
    /// [`SmtpTransport::starttls_relay`].
    ///
    /// # Errors
    ///
    /// Using the wrong [`Tls`] and [`Self::port`] combination may
    /// lead to hard to debug IO errors coming from the TLS library.
    pub fn tls(mut self, tls: Tls) -> Self {
        self.info.tls = tls;
        self.info.unix_socket = None;
        self
    }

    /// Use a custom configuration for the connection pool
    ///
    /// Defaults can be found at [`PoolConfig`]
    pub fn pool_config(mut self, pool_config: PoolConfig) -> Self {
        self.pool_config = pool_config;
        self
    }

    /// Build the transport
    ///
    /// Defaults can be found at [`PoolConfig`]
    pub fn build(self) -> SmtpTransport {
        let client = SmtpClient { info: self.info };

        let client = Pool::new(self.pool_config, client);

        SmtpTransport { inner: client }
    }
}

/// Builder for the LMTP `LmtpTransport`
impl LmtpTransportBuilder {
    // Create new builder with default parameters
    pub(crate) fn new<T: Into<String>>(server: T) -> Self {
        Self {
            info: SmtpInfo::new(server, Protocol::Lmtp),
            pool_config: PoolConfig::default(),
        }
    }

    /// Set the name used during LHLO
    pub fn hello_name(mut self, name: ClientId) -> Self {
        self.info.hello_name = name;
        self
    }

    /// Set the authentication credentials to use
    ///
    /// Unless [`Self::authentication`] was called explicitly, this also selects
    /// the default mechanisms for the credential kind.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.info.set_credentials(credentials);
        self
    }

    /// Set username and password authentication credentials.
    pub fn password<U, P>(self, username: U, password: P) -> Self
    where
        U: Into<String>,
        P: IntoSecretString,
    {
        self.credentials(Credentials::password(username, password))
    }

    /// Set OAuth 2.0 bearer-token authentication credentials.
    ///
    /// This configures `OAUTHBEARER` and `XOAUTH2`, in that preference order.
    pub fn oauth2<I, T>(self, identity: I, access_token: T) -> Self
    where
        I: Into<String>,
        T: IntoSecretString,
    {
        self.credentials(Credentials::oauth2(identity, access_token))
    }

    /// Set the authentication mechanism to use
    pub fn authentication(mut self, mechanisms: Vec<Mechanism>) -> Self {
        self.info.set_authentication(mechanisms);
        self
    }

    /// Allow credentials to be sent over an unencrypted LMTP connection.
    ///
    /// By default, Bifrost SMTP refuses to send passwords or bearer tokens
    /// unless the connection is already encrypted by TLS or has been upgraded
    /// with STARTTLS. Set this only for trusted local relays or test servers.
    pub fn dangerous_allow_insecure_auth(mut self, allow: bool) -> Self {
        self.info.allow_insecure_auth = allow;
        self
    }

    /// Set the timeout duration
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.info.timeout = timeout;
        self
    }

    /// Set the port to use
    pub fn port(mut self, port: u16) -> Self {
        self.info.port = port;
        self.info.unix_socket = None;
        self
    }

    /// Connect over a Unix-domain socket instead of TCP.
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    pub fn unix_socket(mut self, path: impl AsRef<Path>) -> Self {
        self.info.unix_socket = Some(path.as_ref().to_path_buf());
        self.info.tls = super::Tls::None;
        self
    }

    /// Set the TLS settings to use
    pub fn tls(mut self, tls: Tls) -> Self {
        self.info.tls = tls;
        self.info.unix_socket = None;
        self
    }

    /// Use a custom configuration for the connection pool
    ///
    /// Defaults can be found at [`PoolConfig`]
    pub fn pool_config(mut self, pool_config: PoolConfig) -> Self {
        self.pool_config = pool_config;
        self
    }

    /// Build the transport
    ///
    /// Defaults can be found at [`PoolConfig`]
    pub fn build(self) -> LmtpTransport {
        let client = SmtpClient { info: self.info };

        let client = Pool::new(self.pool_config, client);

        LmtpTransport { inner: client }
    }
}

/// Build client
#[derive(Debug, Clone)]
pub(super) struct SmtpClient {
    info: SmtpInfo,
}

impl SmtpClient {
    /// Creates a new connection directly usable to send emails
    ///
    /// Handles encryption and authentication
    pub(super) fn connection(&self) -> Result<SmtpConnection, Error> {
        if let Some(path) = &self.info.unix_socket {
            #[cfg(unix)]
            {
                if self.info.uses_tls() {
                    return Err(error::invalid_input(
                        "TLS is not supported over Unix-domain LMTP sockets",
                    ));
                }
                let mut conn = SmtpConnection::connect_unix_with_protocol(
                    path,
                    self.info.timeout,
                    &self.info.hello_name,
                    self.info.protocol,
                )?;

                if let Some(credentials) = &self.info.credentials {
                    self.info.ensure_can_authenticate(conn.is_encrypted())?;
                    conn.auth(&self.info.authentication, credentials)?;
                }
                return Ok(conn);
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                return Err(error::invalid_input(
                    "Unix-domain LMTP sockets are only supported on Unix platforms",
                ));
            }
        }

        let tls_parameters = match &self.info.tls {
            Tls::Wrapper(tls_parameters) => Some(tls_parameters),
            _ => None,
        };

        let mut conn = SmtpConnection::connect_with_protocol::<(&str, u16)>(
            (self.info.server.as_ref(), self.info.port),
            self.info.timeout,
            &self.info.hello_name,
            tls_parameters,
            None,
            self.info.protocol,
        )?;

        match &self.info.tls {
            Tls::Opportunistic(tls_parameters) if conn.can_starttls() => {
                conn.starttls(tls_parameters, &self.info.hello_name)?;
            }
            Tls::Required(tls_parameters) => {
                conn.starttls(tls_parameters, &self.info.hello_name)?;
            }
            _ => (),
        }

        if let Some(credentials) = &self.info.credentials {
            self.info.ensure_can_authenticate(conn.is_encrypted())?;
            conn.auth(&self.info.authentication, credentials)?;
        }
        Ok(conn)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Duration,
    };

    use crate::transport::smtp::Tls;
    #[cfg(unix)]
    use crate::transport::smtp::test_support::spawn_unix_lmtp_delivery_server;
    use crate::{
        LmtpTransport, SmtpTransport, Transport,
        address::Envelope,
        transport::smtp::{
            authentication::{
                Credentials, DEFAULT_MECHANISMS, Mechanism, OAUTH2_MECHANISMS, PASSWORD_MECHANISMS,
            },
            test_support::{assert_lmtp_delivery_commands, spawn_lmtp_delivery_server},
        },
    };

    use super::{Protocol, SmtpClient};

    #[test]
    fn transport_from_plaintext_url() {
        let builder = SmtpTransport::from_url("smtp://127.0.0.1:2525").unwrap();

        assert_eq!(builder.info.port, 2525);
        assert_eq!(builder.info.server, "127.0.0.1");
    }

    #[test]
    fn lmtp_builder_uses_lmtp_defaults() {
        let builder = LmtpTransport::builder_dangerous("localhost");

        assert_eq!(builder.info.port, super::super::LMTP_PORT);
        assert_eq!(builder.info.protocol, Protocol::Lmtp);
    }

    #[test]
    fn lmtp_transport_returns_per_recipient_statuses() {
        let server = spawn_lmtp_delivery_server();

        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                "first@example.com".parse().unwrap(),
                "second@example.com".parse().unwrap(),
                "third@example.com".parse().unwrap(),
            ],
        )
        .unwrap();
        let mailer = LmtpTransport::builder_dangerous("127.0.0.1")
            .port(server.address.port())
            .build();

        let responses = mailer
            .send_raw(&envelope, b"Subject: test\r\n\r\nHello")
            .unwrap();

        assert_eq!(responses.len(), 3);
        assert!(responses[0].has_code(250));
        assert!(responses[1].has_code(550));
        assert!(!responses[1].is_positive());
        assert!(responses[2].has_code(451));
        assert!(!responses[2].is_positive());

        let commands = server.commands();
        assert_lmtp_delivery_commands(&commands);
    }

    #[test]
    #[cfg(unix)]
    fn lmtp_transport_sends_over_unix_socket() {
        let server = spawn_unix_lmtp_delivery_server();

        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                "first@example.com".parse().unwrap(),
                "second@example.com".parse().unwrap(),
                "third@example.com".parse().unwrap(),
            ],
        )
        .unwrap();
        let mailer = LmtpTransport::unix_socket(server.path.clone()).build();

        let responses = mailer
            .send_raw(&envelope, b"Subject: test\r\n\r\nHello")
            .unwrap();

        assert_eq!(responses.len(), 3);
        assert!(responses[0].has_code(250));
        assert!(responses[1].has_code(550));
        assert!(responses[2].has_code(451));

        let commands = server.commands();
        assert_lmtp_delivery_commands(&commands);
    }

    #[test]
    fn transport_from_tls_url() {
        let builder = SmtpTransport::from_url("smtp://127.0.0.1:2525").unwrap();

        assert!(matches!(builder.info.tls, Tls::None));

        let builder =
            SmtpTransport::from_url("smtps://username:password@smtp.example.com:465").unwrap();

        assert_eq!(builder.info.port, 465);
        assert_eq!(
            builder.info.credentials,
            Some(Credentials::password(
                "username".to_owned(),
                "password".to_owned()
            ))
        );
        assert!(matches!(builder.info.tls, Tls::Wrapper(_)));
        assert_eq!(builder.info.server, "smtp.example.com");

        let builder = SmtpTransport::from_url(
            "smtps://user%40example.com:pa$$word%3F%22!@smtp.example.com:465",
        )
        .unwrap();

        assert_eq!(builder.info.port, 465);
        assert_eq!(
            builder.info.credentials,
            Some(Credentials::password(
                "user@example.com".to_owned(),
                "pa$$word?\"!".to_owned()
            ))
        );
        assert!(matches!(builder.info.tls, Tls::Wrapper(_)));
        assert_eq!(builder.info.server, "smtp.example.com");

        let builder =
            SmtpTransport::from_url("smtp://username:password@smtp.example.com:587?tls=required")
                .unwrap();

        assert_eq!(builder.info.port, 587);
        assert_eq!(
            builder.info.credentials,
            Some(Credentials::password(
                "username".to_owned(),
                "password".to_owned()
            ))
        );
        assert!(matches!(builder.info.tls, Tls::Required(_)));

        let builder = SmtpTransport::from_url(
            "smtp://username:password@smtp.example.com:587?tls=opportunistic",
        )
        .unwrap();

        assert_eq!(builder.info.port, 587);
        assert!(matches!(builder.info.tls, Tls::Opportunistic(_)));

        let builder = SmtpTransport::from_url("smtps://smtp.example.com").unwrap();

        assert_eq!(builder.info.port, 465);
        assert_eq!(builder.info.credentials, None);
        assert!(matches!(builder.info.tls, Tls::Wrapper(_)));
    }

    #[test]
    fn password_helper_uses_password_mechanisms() {
        let builder =
            SmtpTransport::builder_dangerous("smtp.example.com").password("username", "password");

        assert_eq!(
            builder.info.credentials,
            Some(Credentials::password(
                "username".to_owned(),
                "password".to_owned()
            ))
        );
        assert_eq!(builder.info.authentication, PASSWORD_MECHANISMS);
        // Pin the grown default-construction surface explicitly: SCRAM is now
        // offered out of the box (strongest first), LOGIN is opt-in only.
        assert_eq!(
            builder.info.authentication,
            vec![
                Mechanism::ScramSha256Plus,
                Mechanism::ScramSha1Plus,
                Mechanism::ScramSha256,
                Mechanism::ScramSha1,
                Mechanism::Plain,
            ]
        );
        assert_eq!(DEFAULT_MECHANISMS, PASSWORD_MECHANISMS);
    }

    #[test]
    fn oauth2_helper_prefers_standard_bearer_mechanism() {
        let builder = SmtpTransport::builder_dangerous("smtp.example.com")
            .oauth2("user@example.com", "token");

        assert_eq!(
            builder.info.credentials,
            Some(Credentials::oauth2(
                "user@example.com".to_owned(),
                "token".to_owned()
            ))
        );
        assert_eq!(builder.info.authentication, OAUTH2_MECHANISMS);
        assert_eq!(
            builder.info.authentication,
            [Mechanism::OAuthBearer, Mechanism::Xoauth2]
        );
    }

    #[test]
    fn credentials_preserve_explicit_authentication_mechanisms() {
        let builder = SmtpTransport::builder_dangerous("smtp.example.com")
            .authentication(vec![Mechanism::Xoauth2])
            .oauth2("user@example.com", "token");

        assert_eq!(builder.info.authentication, [Mechanism::Xoauth2]);

        let builder = SmtpTransport::builder_dangerous("smtp.example.com")
            .authentication(vec![Mechanism::Plain])
            .password("username", "password");

        assert_eq!(builder.info.authentication, [Mechanism::Plain]);
    }

    #[test]
    fn plaintext_auth_is_refused_before_auth_command() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (observed_tx, observed_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream.write_all(b"220 localhost\r\n").unwrap();

            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut ehlo = String::new();
            reader.read_line(&mut ehlo).unwrap();
            stream
                .write_all(b"250-localhost\r\n250 AUTH PLAIN\r\n")
                .unwrap();

            let mut after_ehlo = String::new();
            let read = reader.read_line(&mut after_ehlo).unwrap();
            observed_tx.send((read, after_ehlo)).unwrap();
        });

        let builder = SmtpTransport::builder_dangerous("127.0.0.1")
            .port(address.port())
            .password("user", "pass");
        let client = SmtpClient { info: builder.info };
        let Err(error) = client.connection() else {
            panic!("plaintext auth must be refused");
        };

        assert!(error.is_policy(), "expected policy error, got {error:?}");
        let (read, after_ehlo) = observed_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(read, 0, "client must close instead of sending AUTH");
        assert_eq!(after_ehlo, "");
        handle.join().unwrap();
    }

    #[test]
    fn dangerous_allow_insecure_auth_preserves_plaintext_auth_escape_hatch() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (auth_tx, auth_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream.write_all(b"220 localhost\r\n").unwrap();

            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut ehlo = String::new();
            reader.read_line(&mut ehlo).unwrap();
            stream
                .write_all(b"250-localhost\r\n250 AUTH PLAIN\r\n")
                .unwrap();

            let mut auth = String::new();
            reader.read_line(&mut auth).unwrap();
            stream.write_all(b"235 authenticated\r\n").unwrap();

            let mut post_auth_ehlo = String::new();
            reader.read_line(&mut post_auth_ehlo).unwrap();
            stream.write_all(b"250 localhost\r\n").unwrap();
            auth_tx.send(auth).unwrap();
        });

        let builder = SmtpTransport::builder_dangerous("127.0.0.1")
            .port(address.port())
            .password("user", "pass")
            .dangerous_allow_insecure_auth(true);
        let client = SmtpClient { info: builder.info };
        let mut connection = client.connection().unwrap();
        connection.abort();

        let auth = auth_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(auth.starts_with("AUTH PLAIN "), "got {auth:?}");
        handle.join().unwrap();
    }
}
