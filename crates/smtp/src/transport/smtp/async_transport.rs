#[cfg(unix)]
use std::path::Path;
use std::sync::Arc;
use std::{
    fmt::{self, Debug},
    marker::PhantomData,
    time::Duration,
};

use bifrost_types::error::{AccountError, BatchItem, BatchOutcome};

use super::PoolConfig;
#[cfg(feature = "tokio")]
use super::Tls;
use super::batch::{SmtpBatchRecipient, batch_input_invalid_error, batch_level_error};
use super::pool::async_impl::Pool;
use super::{
    AsyncSmtpConnection, ClientId, Credentials, Error, Mechanism, Protocol, Response, SendOptions,
    SmtpInfo,
};
use crate::AsyncTransport;
use crate::TokioExecutor;
use crate::address::Address;
use crate::executor::SmtpExecutor;
use crate::transport::smtp::account_error::SmtpErrorContext;
use crate::transport::smtp::authentication::IntoSecretString;
use crate::{Envelope, Executor};

/// Asynchronously sends emails using the SMTP protocol
///
/// `AsyncSmtpTransport` is the primary way for communicating
/// with SMTP relay servers to send email messages. It holds the
/// client connect configuration and creates new connections
/// as necessary.
///
/// # Connection pool
///
/// `AsyncSmtpTransport` maintains a connection pool to manage SMTP
/// connections. The pool:
///
/// - Establishes a new connection when sending a message.
/// - Recycles connections internally after a message is sent.
/// - Reuses connections for subsequent messages, reducing connection setup overhead.
///
/// The connection pool can grow to hold multiple SMTP connections if multiple
/// emails are sent concurrently, as SMTP does not support multiplexing within a
/// single connection.
///
/// However, **connection reuse is not possible** if the `SyncSmtpTransport` instance
/// is dropped after every email send operation. You must reuse the instance
/// of this struct for the connection pool to be of any use.
///
/// To customize connection pool settings, use [`AsyncSmtpTransportBuilder::pool_config`].
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub struct AsyncSmtpTransport<E: Executor> {
    inner: Arc<Pool<E>>,
}

/// Asynchronously sends emails using the LMTP protocol
///
/// `AsyncLmtpTransport` is the local-delivery counterpart to
/// [`AsyncSmtpTransport`]. LMTP uses `LHLO` for capability discovery and
/// returns one status per envelope recipient. Rejected recipients carry their
/// `RCPT` response; accepted recipients carry their post-DATA delivery response.
///
/// Direct sends return only the ordered statuses. Use
/// [`AsyncLmtpTransport::send_raw_batch_with_options`] when callers need the
/// RCPT-versus-final-status phase and per-recipient recovery classification.
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub struct AsyncLmtpTransport<E: Executor> {
    inner: Arc<Pool<E>>,
}

impl AsyncTransport for AsyncSmtpTransport<TokioExecutor> {
    type Ok = Response;
    type Error = Error;

    /// Sends an email
    async fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        let mut conn = self.inner.connection().await?;

        let result = conn.send(envelope, email).await?;

        Ok(result)
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

impl AsyncTransport for AsyncLmtpTransport<TokioExecutor> {
    type Ok = Vec<Response>;
    type Error = Error;

    /// Sends an email and returns one LMTP status per recipient.
    ///
    /// For per-recipient command-phase and recovery details, use
    /// [`AsyncLmtpTransport::send_raw_batch_with_options`].
    async fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        let mut conn = self.inner.connection().await?;

        let result = conn.send_lmtp(envelope, email).await?;

        Ok(result)
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

impl<E> AsyncSmtpTransport<E>
where
    E: Executor,
{
    /// Simple and secure transport, using TLS connections to communicate with the SMTP server
    ///
    /// The right option for most SMTP servers.
    ///
    /// Creates an encrypted transport over submissions port, using the provided domain
    /// to validate TLS certificates.
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    pub fn relay(relay: &str) -> Result<AsyncSmtpTransportBuilder, Error> {
        use super::{SUBMISSIONS_PORT, Tls, TlsParameters};

        let tls_parameters = TlsParameters::new(relay.into())?;

        Ok(Self::builder_dangerous(relay)
            .port(SUBMISSIONS_PORT)
            .tls(Tls::Wrapper(tls_parameters)))
    }

    /// Simple and secure transport, using STARTTLS to obtain encrypted connections
    ///
    /// Alternative to [`AsyncSmtpTransport::relay`](#method.relay), for SMTP servers
    /// that don't take SMTPS connections.
    ///
    /// Creates an encrypted transport over submissions port, by first connecting using
    /// an unencrypted connection and then upgrading it with STARTTLS. The provided
    /// domain is used to validate TLS certificates.
    ///
    /// An error is returned if the connection can't be upgraded. No credentials
    /// or emails will be sent to the server, protecting from downgrade attacks.
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    pub fn starttls_relay(relay: &str) -> Result<AsyncSmtpTransportBuilder, Error> {
        use super::{SUBMISSION_PORT, Tls, TlsParameters};

        let tls_parameters = TlsParameters::new(relay.into())?;

        Ok(Self::builder_dangerous(relay)
            .port(SUBMISSION_PORT)
            .tls(Tls::Required(tls_parameters)))
    }

    /// Creates a new local SMTP client to port 25
    ///
    /// Shortcut for local unencrypted relay (typical local email daemon that will handle relaying)
    #[allow(private_bounds)]
    pub fn unencrypted_localhost() -> AsyncSmtpTransport<E>
    where
        E: SmtpExecutor,
    {
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
    /// Consider using [`AsyncSmtpTransport::relay`](#method.relay) or
    /// [`AsyncSmtpTransport::starttls_relay`](#method.starttls_relay) instead,
    /// if possible.
    pub fn builder_dangerous<T: Into<String>>(server: T) -> AsyncSmtpTransportBuilder {
        AsyncSmtpTransportBuilder::new(server)
    }

    /// Creates a `AsyncSmtpTransportBuilder` from a connection URL
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
    ///
    /// ```rust,no_run
    /// use bifrost_smtp::{
    ///     AsyncSmtpTransport, AsyncTransport, Message, TokioExecutor, message::header::ContentType,
    /// };
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let email = Message::builder()
    ///     .from("NoBody <nobody@domain.tld>".parse().unwrap())
    ///     .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
    ///     .to("Hei <hei@domain.tld>".parse().unwrap())
    ///     .subject("Happy new year")
    ///     .header(ContentType::TEXT_PLAIN)
    ///     .body(String::from("Be happy!"))
    ///     .unwrap();
    ///
    /// // Open a remote connection to gmail
    /// let mailer: AsyncSmtpTransport<TokioExecutor> =
    ///     AsyncSmtpTransport::<TokioExecutor>::from_url(
    ///         "smtps://username:password@smtp.example.com:465",
    ///     )?
    ///     .build();
    ///
    /// // Send the email
    /// mailer.send(&email).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// If a plaintext URL contains credentials, authentication is refused at
    /// connection time unless
    /// [`AsyncSmtpTransportBuilder::dangerous_allow_insecure_auth`] is
    /// enabled.
    pub fn from_url(connection_url: &str) -> Result<AsyncSmtpTransportBuilder, Error> {
        super::connection_url::from_connection_url(connection_url)
    }

    /// Tests the SMTP connection
    ///
    /// `test_connection()` tests the connection by using the SMTP NOOP command.
    #[allow(private_bounds)]
    pub async fn test_connection(&self) -> Result<bool, Error>
    where
        E: SmtpExecutor,
    {
        let mut conn = self.inner.connection().await?;

        let is_connected = conn.test_connected().await;

        Ok(is_connected)
    }

    /// Sends `VRFY` and returns the server response.
    ///
    /// Many servers disable `VRFY` for privacy. Negative SMTP replies are
    /// returned as [`Response`] values so callers can inspect the exact status.
    #[allow(private_bounds)]
    pub async fn verify(&self, argument: impl Into<String>) -> Result<Response, Error>
    where
        E: SmtpExecutor,
    {
        let mut conn = self.inner.connection().await?;
        conn.verify(argument).await
    }

    /// Sends `EXPN` and returns the server response.
    ///
    /// Many servers disable `EXPN` for privacy. Negative SMTP replies are
    /// returned as [`Response`] values so callers can inspect the exact status.
    #[allow(private_bounds)]
    pub async fn expand(&self, argument: impl Into<String>) -> Result<Response, Error>
    where
        E: SmtpExecutor,
    {
        let mut conn = self.inner.connection().await?;
        conn.expand(argument).await
    }

    /// Sends an email with per-message SMTP options.
    ///
    /// This is the advanced counterpart to [`AsyncTransport::send_raw`]. It
    /// keeps the core transport trait simple while still allowing
    /// message-specific ESMTP parameters such as `REQUIRETLS`, `DELIVERBY`,
    /// `FUTURERELEASE`, `MT-PRIORITY`, and DSN options.
    #[allow(private_bounds)]
    pub async fn send_raw_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error>
    where
        E: SmtpExecutor,
    {
        let mut conn = self.inner.connection().await?;

        let result = conn.send_with_options(envelope, email, options).await?;

        Ok(result)
    }

    /// Sends an email with `BDAT ... LAST`.
    ///
    /// The server must advertise `CHUNKING`. This path avoids DATA
    /// dot-stuffing and is the required path for `BODY=BINARYMIME`.
    #[allow(private_bounds)]
    pub async fn send_raw_bdat(&self, envelope: &Envelope, email: &[u8]) -> Result<Response, Error>
    where
        E: SmtpExecutor,
    {
        self.send_raw_bdat_with_options(envelope, email, &SendOptions::default())
            .await
    }

    /// Sends an email with `BDAT ... LAST` and per-message SMTP options.
    #[allow(private_bounds)]
    pub async fn send_raw_bdat_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error>
    where
        E: SmtpExecutor,
    {
        let mut conn = self.inner.connection().await?;

        let result = conn
            .send_bdat_with_options(envelope, email, options)
            .await?;

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
    #[allow(private_bounds)]
    pub async fn send_raw_batch_with_options(
        &self,
        from: Option<Address>,
        recipients: Vec<BatchItem<Address>>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<BatchOutcome<()>, AccountError>
    where
        E: SmtpExecutor,
    {
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
            .await
            .map_err(|e| batch_level_error(e, ctx.clone()))?;

        match conn
            .send_smtp_batch(from, batch_recipients, email, options)
            .await
        {
            Ok(progress) => Ok(progress.resolve()),
            Err((e, _progress)) => Err(batch_level_error(e, ctx)),
        }
    }
}

impl<E> AsyncLmtpTransport<E>
where
    E: Executor,
{
    /// Creates a new local LMTP client to port 24.
    ///
    /// RFC 2033 does not assign an LMTP TCP port. Port 24 is the common TCP
    /// convention. Use [`Self::unix_socket`] for the more common local socket
    /// deployment shape.
    #[allow(private_bounds)]
    pub fn unencrypted_localhost() -> AsyncLmtpTransport<E>
    where
        E: SmtpExecutor,
    {
        Self::builder_dangerous("localhost").build()
    }

    /// Creates a new local LMTP client over a Unix-domain socket.
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    pub fn unix_socket(path: impl AsRef<Path>) -> AsyncLmtpTransportBuilder {
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
    pub fn builder_dangerous<T: Into<String>>(server: T) -> AsyncLmtpTransportBuilder {
        AsyncLmtpTransportBuilder::new(server)
    }

    /// Tests the LMTP connection.
    ///
    /// `test_connection()` tests the connection by using the SMTP NOOP command.
    #[allow(private_bounds)]
    pub async fn test_connection(&self) -> Result<bool, Error>
    where
        E: SmtpExecutor,
    {
        let mut conn = self.inner.connection().await?;

        let is_connected = conn.test_connected().await;

        Ok(is_connected)
    }

    /// Sends `VRFY` over LMTP and returns the server response.
    ///
    /// Negative replies are returned as [`Response`] values so callers can
    /// inspect the exact status.
    #[allow(private_bounds)]
    pub async fn verify(&self, argument: impl Into<String>) -> Result<Response, Error>
    where
        E: SmtpExecutor,
    {
        let mut conn = self.inner.connection().await?;
        conn.verify(argument).await
    }

    /// Sends `EXPN` over LMTP and returns the server response.
    ///
    /// Negative replies are returned as [`Response`] values so callers can
    /// inspect the exact status.
    #[allow(private_bounds)]
    pub async fn expand(&self, argument: impl Into<String>) -> Result<Response, Error>
    where
        E: SmtpExecutor,
    {
        let mut conn = self.inner.connection().await?;
        conn.expand(argument).await
    }

    /// Sends an email over LMTP with per-message SMTP options.
    ///
    /// For per-recipient command-phase and recovery details, use
    /// [`Self::send_raw_batch_with_options`].
    #[allow(private_bounds)]
    pub async fn send_raw_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error>
    where
        E: SmtpExecutor,
    {
        let mut conn = self.inner.connection().await?;

        let result = conn
            .send_lmtp_with_options(envelope, email, options)
            .await?;

        Ok(result)
    }

    /// Sends an email over LMTP with `BDAT ... LAST`.
    ///
    /// The server must advertise `CHUNKING`. Returned responses still preserve
    /// one status per input recipient.
    #[allow(private_bounds)]
    pub async fn send_raw_bdat(
        &self,
        envelope: &Envelope,
        email: &[u8],
    ) -> Result<Vec<Response>, Error>
    where
        E: SmtpExecutor,
    {
        self.send_raw_bdat_with_options(envelope, email, &SendOptions::default())
            .await
    }

    /// Sends an email over LMTP with `BDAT ... LAST` and per-message options.
    #[allow(private_bounds)]
    pub async fn send_raw_bdat_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error>
    where
        E: SmtpExecutor,
    {
        let mut conn = self.inner.connection().await?;

        let result = conn
            .send_lmtp_bdat_with_options(envelope, email, options)
            .await?;

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
    #[allow(private_bounds)]
    pub async fn send_raw_batch_with_options(
        &self,
        from: Option<Address>,
        recipients: Vec<BatchItem<Address>>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<BatchOutcome<()>, AccountError>
    where
        E: SmtpExecutor,
    {
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
            .await
            .map_err(|e| batch_level_error(e, ctx.clone()))?;

        match conn
            .send_lmtp_batch(from, batch_recipients, email, options)
            .await
        {
            Ok(progress) => Ok(progress.resolve()),
            Err((e, _progress)) => Err(batch_level_error(e, ctx)),
        }
    }
}

impl<E: Executor> Debug for AsyncSmtpTransport<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("AsyncSmtpTransport");
        builder.field("inner", &self.inner);
        builder.finish()
    }
}

impl<E: Executor> Debug for AsyncLmtpTransport<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("AsyncLmtpTransport");
        builder.field("inner", &self.inner);
        builder.finish()
    }
}

impl<E> Clone for AsyncSmtpTransport<E>
where
    E: Executor,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<E> Clone for AsyncLmtpTransport<E>
where
    E: Executor,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Contains client configuration.
/// Instances of this struct can be created using functions of [`AsyncSmtpTransport`].
#[derive(Debug, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub struct AsyncSmtpTransportBuilder {
    info: SmtpInfo,
    pool_config: PoolConfig,
}

/// Contains LMTP client configuration.
/// Instances of this struct can be created using functions of [`AsyncLmtpTransport`].
#[derive(Debug, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub struct AsyncLmtpTransportBuilder {
    info: SmtpInfo,
    pool_config: PoolConfig,
}

/// Builder for the SMTP `AsyncSmtpTransport`
impl AsyncSmtpTransportBuilder {
    // Create new builder with default parameters
    pub(crate) fn new<T: Into<String>>(server: T) -> Self {
        AsyncSmtpTransportBuilder {
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

    /// Set OAuth 2.0 bearer-token credentials from a shared token source.
    ///
    /// ratatoskr supplies one `Arc<dyn TokenSource>` it drives rotation
    /// on; the token is read live at each connect, so a refreshed token
    /// is presented on reconnect without rebuilding the transport.
    pub fn oauth2_source<I>(
        self,
        identity: I,
        token_source: Arc<dyn bifrost_net::TokenSource>,
    ) -> Self
    where
        I: Into<String>,
    {
        self.credentials(Credentials::oauth2_source(identity, token_source))
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

    /// Set the port to use
    ///
    /// # Warning
    ///
    /// You probably do not need to call this method.
    ///
    /// Bifrost SMTP usually picks the correct `port` when building
    /// [`AsyncSmtpTransport`] using [`AsyncSmtpTransport::relay`] or
    /// [`AsyncSmtpTransport::starttls_relay`].
    ///
    /// # Errors
    ///
    /// Using the incorrect `port` and [`Self::tls`] combination may
    /// lead to hard to debug IO errors coming from the TLS library.
    pub fn port(mut self, port: u16) -> Self {
        self.info.port = port;
        self
    }

    /// Set the timeout duration
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.info.timeout = timeout;
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
    /// building [`AsyncSmtpTransport`] using [`AsyncSmtpTransport::relay`] or
    /// [`AsyncSmtpTransport::starttls_relay`].
    ///
    /// # Errors
    ///
    /// Using the incorrect [`Tls`] and [`Self::port`] combination may
    /// lead to hard to debug IO errors coming from the TLS library.
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
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
    #[allow(private_bounds)]
    pub fn build<E>(self) -> AsyncSmtpTransport<E>
    where
        E: SmtpExecutor,
    {
        let client = AsyncSmtpClient {
            info: self.info,
            marker_: PhantomData,
        };

        let client = Pool::new(self.pool_config, client);

        AsyncSmtpTransport { inner: client }
    }
}

/// Builder for the LMTP `AsyncLmtpTransport`
impl AsyncLmtpTransportBuilder {
    // Create new builder with default parameters
    pub(crate) fn new<T: Into<String>>(server: T) -> Self {
        AsyncLmtpTransportBuilder {
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

    /// Set OAuth 2.0 bearer-token credentials from a shared token source.
    ///
    /// ratatoskr supplies one `Arc<dyn TokenSource>` it drives rotation
    /// on; the token is read live at each connect, so a refreshed token
    /// is presented on reconnect without rebuilding the transport.
    pub fn oauth2_source<I>(
        self,
        identity: I,
        token_source: Arc<dyn bifrost_net::TokenSource>,
    ) -> Self
    where
        I: Into<String>,
    {
        self.credentials(Credentials::oauth2_source(identity, token_source))
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

    /// Set the timeout duration
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.info.timeout = timeout;
        self
    }

    /// Set the TLS settings to use
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
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
    #[allow(private_bounds)]
    pub fn build<E>(self) -> AsyncLmtpTransport<E>
    where
        E: SmtpExecutor,
    {
        let client = AsyncSmtpClient {
            info: self.info,
            marker_: PhantomData,
        };

        let client = Pool::new(self.pool_config, client);

        AsyncLmtpTransport { inner: client }
    }
}

/// Build client
pub(super) struct AsyncSmtpClient<E> {
    info: SmtpInfo,
    marker_: PhantomData<E>,
}

impl<E> AsyncSmtpClient<E>
where
    E: SmtpExecutor,
{
    /// Creates a new connection directly usable to send emails
    ///
    /// Handles encryption and authentication
    pub(super) async fn connection(&self) -> Result<AsyncSmtpConnection, Error> {
        let mut conn = E::connect(
            &self.info.server,
            self.info.port,
            self.info.unix_socket.as_deref(),
            self.info.timeout,
            &self.info.hello_name,
            &self.info.tls,
            self.info.protocol,
        )
        .await?;

        if let Some(credentials) = &self.info.credentials {
            self.info.ensure_can_authenticate(conn.is_encrypted())?;
            conn.auth(&self.info.authentication, credentials).await?;
        }
        Ok(conn)
    }
}

impl<E> Debug for AsyncSmtpClient<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("AsyncSmtpClient");
        builder.field("info", &self.info);
        builder.finish()
    }
}

#[cfg(test)]
#[cfg(feature = "tokio")]
mod tests {
    use std::{
        io::{BufRead, BufReader, Write},
        marker::PhantomData,
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Duration,
    };

    #[cfg(unix)]
    use crate::transport::smtp::test_support::spawn_unix_lmtp_delivery_server;
    use crate::{
        AsyncLmtpTransport, AsyncSmtpTransport, AsyncTransport, TokioExecutor,
        address::Envelope,
        transport::smtp::test_support::{
            assert_lmtp_delivery_commands, spawn_lmtp_delivery_server,
        },
    };

    use super::{AsyncSmtpClient, Protocol};

    #[test]
    fn tokio_transport_from_plaintext_url() {
        let builder =
            AsyncSmtpTransport::<TokioExecutor>::from_url("smtp://127.0.0.1:2525").unwrap();

        assert_eq!(builder.info.port, 2525);
        assert_eq!(builder.info.server, "127.0.0.1");
    }

    #[test]
    fn tokio_lmtp_builder_uses_lmtp_defaults() {
        let builder = AsyncLmtpTransport::<TokioExecutor>::builder_dangerous("localhost");

        assert_eq!(builder.info.port, super::super::LMTP_PORT);
        assert_eq!(builder.info.protocol, Protocol::Lmtp);
    }

    #[tokio::test(crate = "tokio")]
    async fn tokio_lmtp_transport_returns_per_recipient_statuses() {
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
        let mailer: AsyncLmtpTransport<TokioExecutor> =
            AsyncLmtpTransport::<TokioExecutor>::builder_dangerous("127.0.0.1")
                .port(server.address.port())
                .build();

        let responses = mailer
            .send_raw(&envelope, b"Subject: test\r\n\r\nHello")
            .await
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

    #[tokio::test(crate = "tokio")]
    #[cfg(unix)]
    async fn tokio_lmtp_transport_sends_over_unix_socket() {
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
        let mailer: AsyncLmtpTransport<TokioExecutor> =
            AsyncLmtpTransport::<TokioExecutor>::unix_socket(server.path.clone()).build();

        let responses = mailer
            .send_raw(&envelope, b"Subject: test\r\n\r\nHello")
            .await
            .unwrap();

        assert_eq!(responses.len(), 3);
        assert!(responses[0].has_code(250));
        assert!(responses[1].has_code(550));
        assert!(responses[2].has_code(451));

        let commands = server.commands();
        assert_lmtp_delivery_commands(&commands);
    }

    #[tokio::test(crate = "tokio")]
    async fn tokio_plaintext_auth_is_refused_before_auth_command() {
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

        let builder = AsyncSmtpTransport::<TokioExecutor>::builder_dangerous("127.0.0.1")
            .port(address.port())
            .password("user", "pass");
        let client = AsyncSmtpClient::<TokioExecutor> {
            info: builder.info,
            marker_: PhantomData,
        };
        let Err(error) = client.connection().await else {
            panic!("plaintext auth must be refused");
        };

        assert!(error.is_policy(), "expected policy error, got {error:?}");
        let (read, after_ehlo) = observed_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(read, 0, "client must close instead of sending AUTH");
        assert_eq!(after_ehlo, "");
        handle.join().unwrap();
    }

    mod pooled_batch {
        use std::{marker::PhantomData, sync::Arc};

        use bifrost_types::error::{BatchItem, BatchItemId};

        use crate::transport::smtp::test_support::Transcript;

        use super::super::{
            AsyncLmtpTransport, AsyncSmtpClient, AsyncSmtpConnection, AsyncSmtpTransport, ClientId,
            Pool, PoolConfig, Protocol, SendOptions, SmtpInfo, TokioExecutor,
        };

        fn hello() -> ClientId {
            ClientId::Domain("client.example".to_owned())
        }

        /// A pool that will never dial: the only connection it can ever hand
        /// out is the scripted one parked here.
        async fn pool_with(
            conn: AsyncSmtpConnection,
            protocol: Protocol,
        ) -> Arc<Pool<TokioExecutor>> {
            let pool = Pool::new(
                // The checkout probe would need its own scripted NOOP step;
                // the probe itself is pinned at the connection level.
                PoolConfig::new().test_on_checkout(false),
                AsyncSmtpClient::<TokioExecutor> {
                    info: SmtpInfo::new("transcript.invalid", protocol),
                    marker_: PhantomData,
                },
            );
            pool.park_for_test(conn).await;
            pool
        }

        fn batch(addresses: &[&str]) -> Vec<BatchItem<crate::address::Address>> {
            addresses
                .iter()
                .enumerate()
                .map(|(index, address)| {
                    BatchItem::new(
                        BatchItemId(format!("item-{index}")),
                        address.parse().unwrap(),
                    )
                })
                .collect()
        }

        /// Async recycling runs in a spawned task, so drain the executor
        /// before observing the pool. `yield_now` is enough on the
        /// current-thread test runtime and keeps the test off the clock.
        async fn settle() {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
        }

        /// End-to-end counterpart to the connection-level `should_retire()`
        /// pin, on the async path: a completed LMTP delivery must leave the
        /// pool empty.
        #[tokio::test(crate = "tokio")]
        async fn tokio_lmtp_batch_send_leaves_no_pooled_connection_behind() {
            let transcript = Transcript::new("220 lmtp.example\r\n")
                .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
                .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
                .expect("DATA\r\n", "354 send body\r\n")
                .expect("body", "")
                .expect(
                    "\r\n.\r\n",
                    "250 first delivered\r\n550 second rejected\r\n",
                );
            let conn =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello(), Protocol::Lmtp)
                    .await
                    .unwrap();
            let pool = pool_with(conn, Protocol::Lmtp).await;
            let transport = AsyncLmtpTransport::<TokioExecutor> {
                inner: Arc::clone(&pool),
            };

            let outcome = transport
                .send_raw_batch_with_options(
                    Some("sender@example.com".parse().unwrap()),
                    batch(&["first@example.com", "second@example.com"]),
                    b"body",
                    &SendOptions::default(),
                )
                .await
                .expect("the delivery completes with per-recipient outcomes");
            settle().await;

            assert_eq!(outcome.succeeded().len(), 1);
            assert_eq!(outcome.failed().len(), 1);
            assert_eq!(
                pool.idle_count_for_test().await,
                0,
                "a drained LMTP connection must be retired, not parked"
            );
            transcript.assert_exhausted();
        }

        /// The SMTP contrast case on the async path: the connection goes back
        /// to the pool and the next batch reuses it without a reconnect.
        #[tokio::test(crate = "tokio")]
        async fn tokio_smtp_batch_send_recycles_and_reuses_its_pooled_connection() {
            let transcript = Transcript::new("220 smtp.example\r\n")
                .expect("EHLO client.example\r\n", "250 smtp.example\r\n")
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
                .expect("DATA\r\n", "354 send body\r\n")
                .expect("body", "")
                .expect("\r\n.\r\n", "250 queued\r\n")
                // Second transaction: no greeting, no EHLO. A reconnect would
                // have to write EHLO here and fail the transcript.
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
                .expect("DATA\r\n", "354 send body\r\n")
                .expect("body", "")
                .expect("\r\n.\r\n", "250 queued\r\n");
            let conn =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello(), Protocol::Smtp)
                    .await
                    .unwrap();
            let pool = pool_with(conn, Protocol::Smtp).await;
            let transport = AsyncSmtpTransport::<TokioExecutor> {
                inner: Arc::clone(&pool),
            };

            let first = transport
                .send_raw_batch_with_options(
                    Some("sender@example.com".parse().unwrap()),
                    batch(&["first@example.com"]),
                    b"body",
                    &SendOptions::default(),
                )
                .await
                .unwrap();
            settle().await;
            assert_eq!(first.succeeded().len(), 1);
            assert_eq!(
                pool.idle_count_for_test().await,
                1,
                "a healthy SMTP connection must be recycled"
            );

            let second = transport
                .send_raw_batch_with_options(
                    Some("sender@example.com".parse().unwrap()),
                    batch(&["second@example.com"]),
                    b"body",
                    &SendOptions::default(),
                )
                .await
                .unwrap();
            settle().await;
            assert_eq!(second.succeeded().len(), 1);
            assert_eq!(pool.idle_count_for_test().await, 1);
            transcript.assert_exhausted();
        }
    }
}
