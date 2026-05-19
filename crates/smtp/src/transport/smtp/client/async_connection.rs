use std::{fmt::Display, future::Future, net::IpAddr, time::Duration};

use futures_util::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[cfg(feature = "tokio1")]
use super::async_net::AsyncTokioStream;
#[cfg(feature = "tracing")]
use super::escape_crlf;
#[allow(deprecated)]
use super::{
    ClientCodec, MAX_RESPONSE_BYTES, MAX_RESPONSE_LINE_BYTES, TlsParameters,
    async_net::AsyncNetworkStream,
};
use crate::{
    Envelope,
    transport::smtp::{
        authentication::{Credentials, Mechanism},
        commands::{Auth, Data, Ehlo, Mail, Noop, Quit, Rcpt, Starttls},
        error,
        error::Error,
        extension::{ClientId, Extension, MailBodyParameter, MailParameter, ServerInfo},
        response::{Response, parse_response},
    },
};

macro_rules! try_smtp (
    ($err: expr, $client: ident) => ({
        match $err {
            Ok(val) => val,
            Err(err) => {
                $client.abort().await;
                return Err(From::from(err))
            },
        }
    })
);

#[derive(Clone, Copy, Debug)]
enum TimeoutRuntime {
    #[cfg(feature = "tokio1")]
    Tokio1,
    #[cfg(feature = "async-std1")]
    AsyncStd1,
}

async fn with_timeout<T, F>(
    runtime: TimeoutRuntime,
    timeout: Option<Duration>,
    message: &'static str,
    future: F,
) -> Result<T, Error>
where
    F: Future<Output = T>,
{
    match timeout {
        None => Ok(future.await),
        Some(timeout) => match runtime {
            #[cfg(feature = "tokio1")]
            TimeoutRuntime::Tokio1 => tokio1_crate::time::timeout(timeout, future)
                .await
                .map_err(|_| error::timeout(message)),
            #[cfg(feature = "async-std1")]
            TimeoutRuntime::AsyncStd1 => async_std::future::timeout(timeout, future)
                .await
                .map_err(|_| error::timeout(message)),
        },
    }
}

/// Structure that implements the SMTP client
pub struct AsyncSmtpConnection {
    /// TCP stream between client and server
    /// Value is None before connection
    #[allow(deprecated)]
    stream: BufReader<AsyncNetworkStream>,
    /// Panic state
    panic: bool,
    /// Information about the server
    server_info: ServerInfo,
    /// Client identity used for EHLO.
    hello_name: ClientId,
    /// Timeout applied to each async SMTP I/O operation.
    timeout: Option<Duration>,
    timeout_runtime: TimeoutRuntime,
}

impl AsyncSmtpConnection {
    /// Get information about the server
    pub fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    /// Connects with existing async stream
    ///
    /// Sends EHLO and parses server information
    #[cfg(feature = "tokio1")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio1")))]
    pub async fn connect_with_transport(
        stream: Box<dyn AsyncTokioStream>,
        hello_name: &ClientId,
    ) -> Result<AsyncSmtpConnection, Error> {
        #[allow(deprecated)]
        let stream = AsyncNetworkStream::use_existing_tokio1(stream);
        Self::connect_impl(stream, hello_name, None, TimeoutRuntime::Tokio1).await
    }

    /// Connects to the configured server
    ///
    /// If `tls_parameters` is `Some`, then the connection will use Implicit TLS (sometimes
    /// referred to as `SMTPS`). See also [`AsyncSmtpConnection::starttls`].
    ///
    /// If `local_address` is `Some`, then the address provided shall be used to bind the
    /// connection to a specific local address using [`tokio1_crate::net::TcpSocket::bind`].
    ///
    /// Sends EHLO and parses server information
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use bifrost_smtp::transport::smtp::{client::{AsyncSmtpConnection, TlsParameters}, extension::ClientId};
    /// # use tokio1_crate::{self as tokio, net::ToSocketAddrs as _};
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let connection = AsyncSmtpConnection::connect_tokio1(
    ///     ("example.com", 465),
    ///     Some(Duration::from_secs(10)),
    ///     &ClientId::default(),
    ///     Some(TlsParameters::new("example.com".to_owned())?),
    ///     None,
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "tokio1")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio1")))]
    pub async fn connect_tokio1<T: tokio1_crate::net::ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<TlsParameters>,
        local_address: Option<IpAddr>,
    ) -> Result<AsyncSmtpConnection, Error> {
        #[allow(deprecated)]
        let stream =
            AsyncNetworkStream::connect_tokio1(server, timeout, tls_parameters, local_address)
                .await?;
        Self::connect_impl(stream, hello_name, timeout, TimeoutRuntime::Tokio1).await
    }

    /// Connects to the configured server
    ///
    /// Sends EHLO and parses server information
    #[cfg(feature = "async-std1")]
    #[cfg_attr(docsrs, doc(cfg(feature = "async-std1")))]
    pub async fn connect_asyncstd1<T: async_std::net::ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<TlsParameters>,
    ) -> Result<AsyncSmtpConnection, Error> {
        #[allow(deprecated)]
        let stream = AsyncNetworkStream::connect_asyncstd1(server, timeout, tls_parameters).await?;
        Self::connect_impl(stream, hello_name, timeout, TimeoutRuntime::AsyncStd1).await
    }

    #[allow(deprecated)]
    async fn connect_impl(
        stream: AsyncNetworkStream,
        hello_name: &ClientId,
        timeout: Option<Duration>,
        timeout_runtime: TimeoutRuntime,
    ) -> Result<AsyncSmtpConnection, Error> {
        let stream = BufReader::new(stream);
        let mut conn = AsyncSmtpConnection {
            stream,
            panic: false,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            timeout,
            timeout_runtime,
        };
        // TODO log
        let _response = conn.read_response().await?;

        conn.ehlo(hello_name).await?;

        // Print server information
        #[cfg(feature = "tracing")]
        tracing::debug!("server {}", conn.server_info);
        Ok(conn)
    }

    pub async fn send(&mut self, envelope: &Envelope, email: &[u8]) -> Result<Response, Error> {
        // Mail
        let mut mail_options = vec![];

        // Internationalization handling
        //
        // * 8BITMIME: https://tools.ietf.org/html/rfc6152
        // * SMTPUTF8: https://tools.ietf.org/html/rfc653

        // Check for non-ascii addresses and use the SMTPUTF8 option if any.
        if envelope.has_non_ascii_addresses() {
            if !self.server_info().supports_feature(Extension::SmtpUtfEight) {
                // don't try to send non-ascii addresses (per RFC)
                return Err(error::client(
                    "Envelope contains non-ascii chars but server does not support SMTPUTF8",
                ));
            }
            mail_options.push(MailParameter::SmtpUtfEight);
        }

        // Check for non-ascii content in the message
        if !email.is_ascii() {
            if !self.server_info().supports_feature(Extension::EightBitMime) {
                return Err(error::client(
                    "Message contains non-ascii chars but server does not support 8BITMIME",
                ));
            }
            mail_options.push(MailParameter::Body(MailBodyParameter::EightBitMime));
        }

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options))
                .await,
            self
        );

        // Recipient
        for to_address in envelope.to() {
            try_smtp!(
                self.command(Rcpt::new(to_address.clone(), vec![])).await,
                self
            );
        }

        // Data
        try_smtp!(self.command(Data).await, self);

        // Message content
        let result = try_smtp!(self.message(email).await, self);
        Ok(result)
    }

    pub fn has_broken(&self) -> bool {
        self.panic
    }

    pub fn can_starttls(&self) -> bool {
        !self.is_encrypted() && self.server_info.supports_feature(Extension::StartTls)
    }

    /// Upgrade the connection using `STARTTLS`.
    ///
    /// As described in [rfc3207]. Note that this mechanism has been deprecated in [rfc8314].
    ///
    /// [rfc3207]: https://www.rfc-editor.org/rfc/rfc3207
    /// [rfc8314]: https://www.rfc-editor.org/rfc/rfc8314
    #[allow(unused_variables)]
    pub async fn starttls(
        &mut self,
        tls_parameters: TlsParameters,
        hello_name: &ClientId,
    ) -> Result<(), Error> {
        if self.server_info.supports_feature(Extension::StartTls) {
            try_smtp!(self.command(Starttls).await, self);
            self.stream
                .get_mut()
                .upgrade_tls(tls_parameters, self.timeout)
                .await?;
            #[cfg(feature = "tracing")]
            tracing::debug!("connection encrypted");
            // Send EHLO again
            try_smtp!(self.ehlo(hello_name).await, self);
            self.hello_name = hello_name.clone();
            Ok(())
        } else {
            Err(error::client("STARTTLS is not supported on this server"))
        }
    }

    /// Send EHLO and update server info
    async fn ehlo(&mut self, hello_name: &ClientId) -> Result<(), Error> {
        let ehlo_response = try_smtp!(self.command(Ehlo::new(hello_name.clone())).await, self);
        self.server_info = try_smtp!(ServerInfo::from_response(&ehlo_response), self);
        Ok(())
    }

    pub async fn quit(&mut self) -> Result<Response, Error> {
        Ok(try_smtp!(self.command(Quit).await, self))
    }

    pub async fn abort(&mut self) {
        self.panic = true;
        let _ = self.stream.close().await;
    }

    /// Sets the underlying stream
    #[allow(deprecated)]
    pub fn set_stream(&mut self, stream: AsyncNetworkStream) {
        self.stream = BufReader::new(stream);
    }

    /// Tells if the underlying stream is currently encrypted
    pub fn is_encrypted(&self) -> bool {
        self.stream.get_ref().is_encrypted()
    }

    /// Checks if the server is connected using the NOOP SMTP command
    pub async fn test_connected(&mut self) -> bool {
        self.command(Noop).await.is_ok()
    }

    /// Sends an AUTH command with the given mechanism, and handles the challenge if needed
    pub async fn auth(
        &mut self,
        mechanisms: &[Mechanism],
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        let mechanism = self
            .server_info
            .get_auth_mechanism(mechanisms)
            .ok_or_else(|| error::client("No compatible authentication mechanism was found"))?;

        // Limit challenges to avoid blocking
        let mut challenges: u8 = 10;
        let mut response = self
            .command(Auth::new(mechanism, credentials.clone(), None)?)
            .await?;

        while challenges > 0 && response.has_code(334) {
            challenges -= 1;
            response = try_smtp!(
                self.command(Auth::new_from_response(
                    mechanism,
                    credentials.clone(),
                    &response,
                )?)
                .await,
                self
            );
        }

        if challenges == 0 {
            Err(error::response("Unexpected number of challenges"))
        } else {
            let hello_name = self.hello_name.clone();
            try_smtp!(self.ehlo(&hello_name).await, self);
            Ok(response)
        }
    }

    /// Sends the message content
    pub async fn message(&mut self, message: &[u8]) -> Result<Response, Error> {
        self.message_iter(std::iter::once(message)).await
    }

    /// Sends the message content by consuming an iterator that in its whole represents a message.
    pub async fn message_iter<I, B>(&mut self, message: I) -> Result<Response, Error>
    where
        I: Iterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut codec = ClientCodec::new();
        for message_part in message {
            let message_part = message_part.as_ref();
            let mut out_buf = Vec::with_capacity(message_part.len());
            codec.encode(message_part, &mut out_buf);
            self.write(out_buf.as_slice()).await?;
        }
        self.write(b"\r\n.\r\n").await?;

        self.read_response().await
    }

    /// Sends an SMTP command
    pub async fn command<C: Display>(&mut self, command: C) -> Result<Response, Error> {
        self.write(command.to_string().as_bytes()).await?;
        self.read_response().await
    }

    /// Writes a string to the server
    async fn write(&mut self, string: &[u8]) -> Result<(), Error> {
        with_timeout(
            self.timeout_runtime,
            self.timeout,
            "SMTP write timed out",
            self.stream.get_mut().write_all(string),
        )
        .await?
        .map_err(error::network)?;
        with_timeout(
            self.timeout_runtime,
            self.timeout,
            "SMTP flush timed out",
            self.stream.get_mut().flush(),
        )
        .await?
        .map_err(error::network)?;

        #[cfg(feature = "tracing")]
        tracing::debug!("Wrote: {}", escape_crlf(&String::from_utf8_lossy(string)));
        Ok(())
    }

    /// Gets the SMTP response
    pub async fn read_response(&mut self) -> Result<Response, Error> {
        let mut buffer = String::with_capacity(100);
        let mut pre = 0;

        while with_timeout(
            self.timeout_runtime,
            self.timeout,
            "SMTP read timed out",
            self.stream.read_line(&mut buffer),
        )
        .await?
        .map_err(error::network)?
            > 0
        {
            if buffer.len() - pre > MAX_RESPONSE_LINE_BYTES {
                return Err(error::response("SMTP response line too long"));
            }
            if buffer.len() > MAX_RESPONSE_BYTES {
                return Err(error::response("SMTP response too large"));
            }
            pre = buffer.len();

            #[cfg(feature = "tracing")]
            tracing::debug!("<< {}", escape_crlf(&buffer));
            match parse_response(&buffer) {
                Ok((_remaining, response)) => {
                    return if response.is_positive() {
                        Ok(response)
                    } else {
                        Err(error::code(
                            response.code(),
                            Some(response.message().collect()),
                        ))
                    };
                }
                Err(nom::Err::Failure(e)) => {
                    return Err(error::response(e.to_string()));
                }
                Err(nom::Err::Incomplete(_)) => { /* read more */ }
                Err(nom::Err::Error(e)) => {
                    return Err(error::response(e.to_string()));
                }
            }
        }

        Err(error::response("incomplete response"))
    }

    /// The X509 certificate of the server (DER encoded)
    #[cfg(feature = "tokio1-native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio1-native-tls")))]
    pub fn peer_certificate(&self) -> Result<Vec<u8>, Error> {
        self.stream.get_ref().peer_certificate()
    }
}

#[cfg(test)]
#[cfg(feature = "tokio1")]
mod test {
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Duration,
    };

    use crate::transport::smtp::{
        authentication::{Credentials, Mechanism},
        client::AsyncSmtpConnection,
        commands::Noop,
        extension::{ClientId, Extension},
    };

    #[tokio1_crate::test(crate = "tokio1_crate")]
    async fn abort_closes_without_quit_command() {
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
            stream.write_all(b"250 localhost\r\n").unwrap();

            let mut after_abort = String::new();
            let observed = match reader.read_line(&mut after_abort) {
                Ok(bytes) => format!("{bytes}:{after_abort}"),
                Err(error) => format!("error:{:?}", error.kind()),
            };
            observed_tx.send(observed).unwrap();
        });

        let mut connection =
            AsyncSmtpConnection::connect_tokio1(address, None, &ClientId::default(), None, None)
                .await
                .unwrap();
        connection.abort().await;

        let observed = observed_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(
            !observed.contains("QUIT"),
            "abort must close without sending QUIT, got {observed:?}"
        );
        handle.join().unwrap();
    }

    #[tokio1_crate::test(crate = "tokio1_crate")]
    async fn auth_refreshes_server_info_with_ehlo() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (commands_tx, commands_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream.write_all(b"220 localhost\r\n").unwrap();

            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut commands = Vec::new();

            let mut initial_ehlo = String::new();
            reader.read_line(&mut initial_ehlo).unwrap();
            commands.push(initial_ehlo);
            stream
                .write_all(b"250-localhost\r\n250-AUTH PLAIN\r\n250 SIZE 100\r\n")
                .unwrap();

            let mut auth = String::new();
            reader.read_line(&mut auth).unwrap();
            commands.push(auth);
            stream.write_all(b"235 authenticated\r\n").unwrap();

            let mut post_auth_ehlo = String::new();
            reader.read_line(&mut post_auth_ehlo).unwrap();
            commands.push(post_auth_ehlo);
            stream
                .write_all(b"250-localhost\r\n250 8BITMIME\r\n")
                .unwrap();

            commands_tx.send(commands).unwrap();
        });

        let mut connection =
            AsyncSmtpConnection::connect_tokio1(address, None, &ClientId::default(), None, None)
                .await
                .unwrap();
        assert!(
            connection
                .server_info()
                .supports_auth_mechanism(Mechanism::Plain)
        );

        let response = connection
            .auth(
                &[Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap();

        assert!(response.has_code(235));
        assert!(
            !connection
                .server_info()
                .supports_auth_mechanism(Mechanism::Plain)
        );
        assert!(
            connection
                .server_info()
                .supports_feature(Extension::EightBitMime)
        );

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(commands[0].starts_with("EHLO "));
        assert!(commands[1].starts_with("AUTH PLAIN "));
        assert!(commands[2].starts_with("EHLO "));
        handle.join().unwrap();
    }

    #[tokio1_crate::test(crate = "tokio1_crate")]
    async fn connect_times_out_waiting_for_banner() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(500));
        });

        let result = AsyncSmtpConnection::connect_tokio1(
            address,
            Some(Duration::from_millis(50)),
            &ClientId::default(),
            None,
            None,
        )
        .await;

        let Err(error) = result else {
            panic!("connect must time out while waiting for banner");
        };
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
        handle.join().unwrap();
    }

    #[tokio1_crate::test(crate = "tokio1_crate")]
    async fn command_times_out_waiting_for_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream.write_all(b"220 localhost\r\n").unwrap();

            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut ehlo = String::new();
            reader.read_line(&mut ehlo).unwrap();
            stream.write_all(b"250 localhost\r\n").unwrap();

            let mut noop = String::new();
            reader.read_line(&mut noop).unwrap();
            thread::sleep(Duration::from_millis(500));
        });

        let mut connection = AsyncSmtpConnection::connect_tokio1(
            address,
            Some(Duration::from_millis(50)),
            &ClientId::default(),
            None,
            None,
        )
        .await
        .unwrap();

        let Err(error) = connection.command(Noop).await else {
            panic!("NOOP must time out while waiting for response");
        };
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
        handle.join().unwrap();
    }
}
