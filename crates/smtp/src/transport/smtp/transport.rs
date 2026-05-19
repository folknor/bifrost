#[cfg(feature = "pool")]
use std::sync::Arc;
use std::{fmt::Debug, time::Duration};

#[cfg(feature = "pool")]
use super::PoolConfig;
#[cfg(feature = "pool")]
use super::pool::sync_impl::Pool;
use super::{ClientId, Credentials, Error, Mechanism, Response, SmtpConnection, SmtpInfo};
#[cfg(feature = "native-tls")]
use super::{SUBMISSION_PORT, SUBMISSIONS_PORT, Tls, TlsParameters};
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
/// When the `pool` feature is enabled (default), `SmtpTransport` maintains a
/// connection pool to manage SMTP connections. The pool:
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
#[cfg_attr(docsrs, doc(cfg(feature = "smtp-transport")))]
#[derive(Clone)]
pub struct SmtpTransport {
    #[cfg(feature = "pool")]
    inner: Arc<Pool>,
    #[cfg(not(feature = "pool"))]
    inner: SmtpClient,
}

impl Transport for SmtpTransport {
    type Ok = Response;
    type Error = Error;

    /// Sends an email
    fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send(envelope, email)?;

        #[cfg(not(feature = "pool"))]
        conn.abort();

        Ok(result)
    }

    fn shutdown(&self) {
        #[cfg(feature = "pool")]
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

impl SmtpTransport {
    /// Simple and secure transport, using TLS connections to communicate with the SMTP server
    ///
    /// The right option for most SMTP servers.
    ///
    /// Creates an encrypted transport over submissions port, using the provided domain
    /// to validate TLS certificates.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
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
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
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
    /// TLS URL forms such as `smtps://` and `?tls=required` require the
    /// `native-tls` feature. Without it, only plaintext `smtp://` URLs are
    /// accepted. If a plaintext URL contains credentials, authentication is
    /// refused at connection time unless
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
    /// The connection is closed afterward if a connection pool is not used.
    pub fn test_connection(&self) -> Result<bool, Error> {
        let mut conn = self.inner.connection()?;

        let is_connected = conn.test_connected();

        #[cfg(not(feature = "pool"))]
        conn.quit()?;

        Ok(is_connected)
    }
}

/// Contains client configuration.
/// Instances of this struct can be created using functions of [`SmtpTransport`].
#[derive(Debug, Clone)]
pub struct SmtpTransportBuilder {
    info: SmtpInfo,
    #[cfg(feature = "pool")]
    pool_config: PoolConfig,
}

/// Builder for the SMTP `SmtpTransport`
impl SmtpTransportBuilder {
    // Create new builder with default parameters
    pub(crate) fn new<T: Into<String>>(server: T) -> Self {
        let new = SmtpInfo {
            server: server.into(),
            ..Default::default()
        };

        Self {
            info: new,
            #[cfg(feature = "pool")]
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

    /// Set the TLS settings to use
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
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    pub fn tls(mut self, tls: Tls) -> Self {
        self.info.tls = tls;
        self
    }

    /// Use a custom configuration for the connection pool
    ///
    /// Defaults can be found at [`PoolConfig`]
    #[cfg(feature = "pool")]
    #[cfg_attr(docsrs, doc(cfg(feature = "pool")))]
    pub fn pool_config(mut self, pool_config: PoolConfig) -> Self {
        self.pool_config = pool_config;
        self
    }

    /// Build the transport
    ///
    /// If the `pool` feature is enabled, an `Arc` wrapped pool is created.
    /// Defaults can be found at [`PoolConfig`]
    pub fn build(self) -> SmtpTransport {
        let client = SmtpClient { info: self.info };

        #[cfg(feature = "pool")]
        let client = Pool::new(self.pool_config, client);

        SmtpTransport { inner: client }
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
        #[allow(clippy::match_single_binding)]
        let tls_parameters = match &self.info.tls {
            #[cfg(feature = "native-tls")]
            Tls::Wrapper(tls_parameters) => Some(tls_parameters),
            _ => None,
        };

        #[allow(unused_mut)]
        let mut conn = SmtpConnection::connect::<(&str, u16)>(
            (self.info.server.as_ref(), self.info.port),
            self.info.timeout,
            &self.info.hello_name,
            tls_parameters,
            None,
        )?;

        #[cfg(feature = "native-tls")]
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

    #[cfg(feature = "native-tls")]
    use crate::transport::smtp::Tls;
    use crate::{
        SmtpTransport,
        transport::smtp::authentication::{
            Credentials, Mechanism, OAUTH2_MECHANISMS, PASSWORD_MECHANISMS,
        },
    };

    use super::SmtpClient;

    #[test]
    fn transport_from_plaintext_url() {
        let builder = SmtpTransport::from_url("smtp://127.0.0.1:2525").unwrap();

        assert_eq!(builder.info.port, 2525);
        assert_eq!(builder.info.server, "127.0.0.1");
    }

    #[test]
    #[cfg(feature = "native-tls")]
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
