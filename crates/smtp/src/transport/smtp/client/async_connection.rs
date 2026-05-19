#[cfg(feature = "tokio1")]
use std::net::IpAddr;
use std::{fmt::Display, future::Future, time::Duration};

use futures_util::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::async_net::AsyncDeadline;
#[cfg(feature = "tracing")]
use super::escape_crlf;
#[allow(deprecated)]
use super::{
    ClientCodec, ConnectionState, MAX_RESPONSE_BYTES, MAX_RESPONSE_LINE_BYTES, TlsParameters,
    async_net::AsyncNetworkStream,
};
use crate::{
    Envelope,
    transport::smtp::{
        Protocol,
        authentication::{Credentials, Mechanism},
        commands::{Auth, Data, Ehlo, Lhlo, Mail, Noop, Quit, Rcpt, Starttls},
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

#[derive(Clone, Copy, Debug)]
enum TimeoutBudget {
    PerOperation(Option<Duration>),
    SetupDeadline(AsyncDeadline),
}

async fn with_timeout<T, F>(
    runtime: TimeoutRuntime,
    budget: TimeoutBudget,
    message: &'static str,
    future: F,
) -> Result<T, Error>
where
    F: Future<Output = T>,
{
    let timeout = match budget {
        TimeoutBudget::PerOperation(timeout) => timeout,
        TimeoutBudget::SetupDeadline(deadline) => deadline.remaining(message)?,
    };

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
pub(crate) struct AsyncSmtpConnection {
    /// TCP stream between client and server
    /// Value is None before connection
    #[allow(deprecated)]
    stream: BufReader<AsyncNetworkStream>,
    /// Information about the server
    server_info: ServerInfo,
    /// Client identity used for EHLO.
    hello_name: ClientId,
    /// Wire protocol used for this connection.
    protocol: Protocol,
    /// Timeout applied to each async SMTP I/O operation.
    timeout: Option<Duration>,
    timeout_runtime: TimeoutRuntime,
}

impl AsyncSmtpConnection {
    /// Get information about the server
    pub(crate) fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    fn per_operation_budget(&self) -> TimeoutBudget {
        TimeoutBudget::PerOperation(self.timeout)
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
    /// # use bifrost_smtp::transport::smtp::{AsyncSmtpConnection, TlsParameters, extension::ClientId};
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
    #[cfg(test)]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio1")))]
    pub(crate) async fn connect_tokio1<T: tokio1_crate::net::ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<TlsParameters>,
        local_address: Option<IpAddr>,
    ) -> Result<AsyncSmtpConnection, Error> {
        Self::connect_tokio1_with_protocol(
            server,
            timeout,
            hello_name,
            tls_parameters,
            local_address,
            Protocol::Smtp,
        )
        .await
    }

    #[cfg(feature = "tokio1")]
    pub(crate) async fn connect_tokio1_with_protocol<T: tokio1_crate::net::ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<TlsParameters>,
        local_address: Option<IpAddr>,
        protocol: Protocol,
    ) -> Result<AsyncSmtpConnection, Error> {
        let deadline = AsyncDeadline::new(timeout);
        #[allow(deprecated)]
        let stream = AsyncNetworkStream::connect_tokio1_until(
            server,
            deadline,
            tls_parameters,
            local_address,
        )
        .await?;
        Self::connect_impl(
            stream,
            hello_name,
            timeout,
            TimeoutRuntime::Tokio1,
            TimeoutBudget::SetupDeadline(deadline),
            protocol,
        )
        .await
    }

    /// Connects to the configured server
    ///
    /// Sends EHLO and parses server information
    #[cfg(feature = "async-std1")]
    #[cfg(test)]
    #[cfg_attr(docsrs, doc(cfg(feature = "async-std1")))]
    pub(crate) async fn connect_asyncstd1<T: async_std::net::ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<TlsParameters>,
    ) -> Result<AsyncSmtpConnection, Error> {
        Self::connect_asyncstd1_with_protocol(
            server,
            timeout,
            hello_name,
            tls_parameters,
            Protocol::Smtp,
        )
        .await
    }

    #[cfg(feature = "async-std1")]
    pub(crate) async fn connect_asyncstd1_with_protocol<T: async_std::net::ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<TlsParameters>,
        protocol: Protocol,
    ) -> Result<AsyncSmtpConnection, Error> {
        let deadline = AsyncDeadline::new(timeout);
        #[allow(deprecated)]
        let stream =
            AsyncNetworkStream::connect_asyncstd1_until(server, deadline, tls_parameters).await?;
        Self::connect_impl(
            stream,
            hello_name,
            timeout,
            TimeoutRuntime::AsyncStd1,
            TimeoutBudget::SetupDeadline(deadline),
            protocol,
        )
        .await
    }

    #[allow(deprecated)]
    async fn connect_impl(
        stream: AsyncNetworkStream,
        hello_name: &ClientId,
        timeout: Option<Duration>,
        timeout_runtime: TimeoutRuntime,
        setup_budget: TimeoutBudget,
        protocol: Protocol,
    ) -> Result<AsyncSmtpConnection, Error> {
        let stream = BufReader::new(stream);
        let mut conn = AsyncSmtpConnection {
            stream,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
            timeout,
            timeout_runtime,
        };
        let _response = conn.read_response_with_budget(setup_budget).await?;

        conn.hello_with_budget(hello_name, setup_budget).await?;

        // Print server information
        #[cfg(feature = "tracing")]
        tracing::debug!("server {}", conn.server_info);
        Ok(conn)
    }

    pub(crate) async fn send(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
    ) -> Result<Response, Error> {
        let mail_options = self.mail_options(envelope, email)?;

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options))
                .await,
            self
        );

        for to_address in envelope.to() {
            try_smtp!(
                self.command(Rcpt::new(to_address.clone(), vec![])).await,
                self
            );
        }

        try_smtp!(self.command(Data).await, self);
        let result = try_smtp!(self.message(email).await, self);
        Ok(result)
    }

    pub(crate) async fn send_lmtp(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
    ) -> Result<Vec<Response>, Error> {
        let mail_options = self.mail_options(envelope, email)?;

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options))
                .await,
            self
        );

        let mut recipient_statuses = Vec::with_capacity(envelope.to().len());
        let mut accepted_recipients = 0;

        for to_address in envelope.to() {
            let response = try_smtp!(
                self.command_accepting_status(Rcpt::new(to_address.clone(), vec![]))
                    .await,
                self
            );
            if response.is_positive() {
                accepted_recipients += 1;
                recipient_statuses.push(None);
            } else {
                recipient_statuses.push(Some(response));
            }
        }

        if accepted_recipients == 0 {
            return Ok(recipient_statuses
                .into_iter()
                .map(|response| response.expect("all recipients were rejected"))
                .collect());
        }

        try_smtp!(self.command(Data).await, self);
        let mut delivery_statuses =
            try_smtp!(self.message_lmtp(email, accepted_recipients).await, self).into_iter();

        Ok(recipient_statuses
            .into_iter()
            .map(|response| {
                response.unwrap_or_else(|| {
                    delivery_statuses
                        .next()
                        .expect("server returned one status per accepted recipient")
                })
            })
            .collect())
    }

    fn mail_options(&self, envelope: &Envelope, email: &[u8]) -> Result<Vec<MailParameter>, Error> {
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

        Ok(mail_options)
    }

    pub(crate) fn has_broken(&self) -> bool {
        self.stream.get_ref().state() != ConnectionState::Ok
    }

    // Async STARTTLS is only wired for the tokio native-tls feature path.
    #[cfg_attr(not(feature = "tokio1-native-tls"), allow(dead_code))]
    pub(crate) fn can_starttls(&self) -> bool {
        !self.is_encrypted() && self.server_info.supports_feature(Extension::StartTls)
    }

    /// Upgrade the connection using `STARTTLS`.
    ///
    /// As described in [rfc3207]. Note that this mechanism has been deprecated in [rfc8314].
    ///
    /// [rfc3207]: https://www.rfc-editor.org/rfc/rfc3207
    /// [rfc8314]: https://www.rfc-editor.org/rfc/rfc8314
    // Async STARTTLS is only wired for the tokio native-tls feature path.
    #[allow(unused_variables)]
    #[cfg_attr(not(feature = "tokio1-native-tls"), allow(dead_code))]
    pub(crate) async fn starttls(
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
            // Send EHLO/LHLO again
            try_smtp!(self.hello(hello_name).await, self);
            self.hello_name = hello_name.clone();
            Ok(())
        } else {
            Err(error::client("STARTTLS is not supported on this server"))
        }
    }

    /// Send EHLO or LHLO and update server info
    async fn hello(&mut self, hello_name: &ClientId) -> Result<(), Error> {
        self.hello_with_budget(hello_name, self.per_operation_budget())
            .await
    }

    async fn hello_with_budget(
        &mut self,
        hello_name: &ClientId,
        budget: TimeoutBudget,
    ) -> Result<(), Error> {
        let response = match self.protocol {
            Protocol::Smtp => {
                try_smtp!(
                    self.command_with_budget(Ehlo::new(hello_name.clone()), budget)
                        .await,
                    self
                )
            }
            Protocol::Lmtp => {
                try_smtp!(
                    self.command_with_budget(Lhlo::new(hello_name.clone()), budget)
                        .await,
                    self
                )
            }
        };
        self.server_info = try_smtp!(ServerInfo::from_response(&response), self);
        Ok(())
    }

    #[cfg_attr(feature = "pool", allow(dead_code))]
    pub(crate) async fn quit(&mut self) -> Result<Response, Error> {
        Ok(try_smtp!(self.command(Quit).await, self))
    }

    pub(crate) async fn abort(&mut self) {
        let _ = self.stream.close().await;
    }

    /// Tells if the underlying stream is currently encrypted
    pub(crate) fn is_encrypted(&self) -> bool {
        self.stream.get_ref().is_encrypted()
    }

    /// Checks if the server is connected using the NOOP SMTP command.
    ///
    /// A failed check marks the connection broken and closes it. A connection
    /// that cannot answer NOOP is not safe to keep in the pool.
    pub(crate) async fn test_connected(&mut self) -> bool {
        match self.command(Noop).await {
            Ok(_) => true,
            Err(_) => {
                self.abort().await;
                false
            }
        }
    }

    /// Sends an AUTH command with the given mechanism, and handles the challenge if needed
    pub(crate) async fn auth(
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
        let auth = Auth::new(mechanism, credentials.clone(), None)?;
        let mut response = try_smtp!(self.command(auth).await, self);

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
            try_smtp!(self.hello(&hello_name).await, self);
            Ok(response)
        }
    }

    /// Sends the message content
    pub(crate) async fn message(&mut self, message: &[u8]) -> Result<Response, Error> {
        self.message_iter(std::iter::once(message)).await
    }

    pub(crate) async fn message_lmtp(
        &mut self,
        message: &[u8],
        recipients: usize,
    ) -> Result<Vec<Response>, Error> {
        self.message_lmtp_iter(std::iter::once(message), recipients)
            .await
    }

    /// Sends the message content by consuming an iterator that in its whole represents a message.
    pub(crate) async fn message_iter<I, B>(&mut self, message: I) -> Result<Response, Error>
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

    /// Sends the message content and reads one LMTP status per recipient.
    pub(crate) async fn message_lmtp_iter<I, B>(
        &mut self,
        message: I,
        recipients: usize,
    ) -> Result<Vec<Response>, Error>
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

        // A dropped LMTP send cannot safely reuse the stream until every
        // accepted recipient status has been consumed.
        self.stream.get_ref().state().verify()?;
        self.stream.get_mut().set_state(ConnectionState::Broken);

        let mut responses = Vec::with_capacity(recipients);
        for _ in 0..recipients {
            responses.push(
                self.read_response_with_budget_inner(self.per_operation_budget(), true, false)
                    .await?,
            );
        }

        self.stream.get_mut().set_state(ConnectionState::Ok);
        Ok(responses)
    }

    /// Sends an SMTP command
    pub(crate) async fn command<C: Display>(&mut self, command: C) -> Result<Response, Error> {
        self.command_with_budget(command, self.per_operation_budget())
            .await
    }

    async fn command_with_budget<C: Display>(
        &mut self,
        command: C,
        budget: TimeoutBudget,
    ) -> Result<Response, Error> {
        self.write_with_budget(command.to_string().as_bytes(), budget)
            .await?;
        self.read_response_with_budget(budget).await
    }

    async fn command_accepting_status<C: Display>(
        &mut self,
        command: C,
    ) -> Result<Response, Error> {
        self.write(command.to_string().as_bytes()).await?;
        self.read_response_accepting_status().await
    }

    /// Writes a string to the server
    async fn write(&mut self, string: &[u8]) -> Result<(), Error> {
        self.write_with_budget(string, self.per_operation_budget())
            .await
    }

    async fn write_with_budget(
        &mut self,
        string: &[u8],
        budget: TimeoutBudget,
    ) -> Result<(), Error> {
        self.stream.get_ref().state().verify()?;
        self.stream.get_mut().set_state(ConnectionState::Broken);

        with_timeout(
            self.timeout_runtime,
            budget,
            "SMTP write timed out",
            self.stream.get_mut().write_all(string),
        )
        .await?
        .map_err(error::network)?;
        with_timeout(
            self.timeout_runtime,
            budget,
            "SMTP flush timed out",
            self.stream.get_mut().flush(),
        )
        .await?
        .map_err(error::network)?;
        self.stream.get_mut().set_state(ConnectionState::Ok);

        #[cfg(feature = "tracing")]
        tracing::debug!("Wrote: {}", escape_crlf(&String::from_utf8_lossy(string)));
        Ok(())
    }

    /// Gets the SMTP response
    pub(crate) async fn read_response(&mut self) -> Result<Response, Error> {
        self.read_response_with_budget(self.per_operation_budget())
            .await
    }

    async fn read_response_accepting_status(&mut self) -> Result<Response, Error> {
        self.read_response_with_budget_inner(self.per_operation_budget(), true, true)
            .await
    }

    async fn read_response_with_budget(
        &mut self,
        budget: TimeoutBudget,
    ) -> Result<Response, Error> {
        self.read_response_with_budget_inner(budget, false, true)
            .await
    }

    async fn read_response_with_budget_inner(
        &mut self,
        budget: TimeoutBudget,
        accept_negative: bool,
        manage_state: bool,
    ) -> Result<Response, Error> {
        if manage_state {
            self.stream.get_ref().state().verify()?;
            self.stream.get_mut().set_state(ConnectionState::Broken);
        }

        let mut buffer = String::with_capacity(100);
        let mut pre = 0;

        while with_timeout(
            self.timeout_runtime,
            budget,
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
                    if manage_state {
                        self.stream.get_mut().set_state(ConnectionState::Ok);
                    }

                    return if accept_negative || response.is_positive() {
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
        AsyncSmtpConnection,
        authentication::{Credentials, Mechanism},
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
    async fn failed_test_connected_marks_connection_broken() {
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
        });

        let mut connection =
            AsyncSmtpConnection::connect_tokio1(address, None, &ClientId::default(), None, None)
                .await
                .unwrap();

        assert!(!connection.test_connected().await);
        assert!(connection.has_broken());
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
    async fn oauthbearer_auth_sends_initial_response_and_refreshes_ehlo() {
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
                .write_all(b"250-localhost\r\n250 AUTH OAUTHBEARER\r\n")
                .unwrap();

            let mut auth = String::new();
            reader.read_line(&mut auth).unwrap();
            commands.push(auth);
            stream.write_all(b"235 authenticated\r\n").unwrap();

            let mut post_auth_ehlo = String::new();
            reader.read_line(&mut post_auth_ehlo).unwrap();
            commands.push(post_auth_ehlo);
            stream.write_all(b"250 localhost\r\n").unwrap();

            commands_tx.send(commands).unwrap();
        });

        let mut connection =
            AsyncSmtpConnection::connect_tokio1(address, None, &ClientId::default(), None, None)
                .await
                .unwrap();
        let response = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("us,er=one", "token"),
            )
            .await
            .unwrap();

        assert!(response.has_code(235));

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(commands[0].starts_with("EHLO "));
        assert!(commands[1].starts_with("AUTH OAUTHBEARER "));
        assert!(commands[2].starts_with("EHLO "));

        let encoded_response = commands[1]
            .trim_end()
            .strip_prefix("AUTH OAUTHBEARER ")
            .unwrap();
        let decoded_response = crate::base64::decode(encoded_response).unwrap();
        assert_eq!(
            String::from_utf8(decoded_response).unwrap(),
            "n,a=us=2Cer=3Done,\x01auth=Bearer token\x01\x01"
        );
        handle.join().unwrap();
    }

    #[tokio1_crate::test(crate = "tokio1_crate")]
    async fn oauthbearer_immediate_rejection_marks_connection_broken() {
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
                .write_all(b"250-localhost\r\n250 AUTH OAUTHBEARER\r\n")
                .unwrap();

            let mut auth = String::new();
            reader.read_line(&mut auth).unwrap();
            commands.push(auth);
            stream.write_all(b"535 rejected\r\n").unwrap();

            commands_tx.send(commands).unwrap();
        });

        let mut connection =
            AsyncSmtpConnection::connect_tokio1(address, None, &ClientId::default(), None, None)
                .await
                .unwrap();
        let error = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("user", "token"),
            )
            .await
            .unwrap_err();

        assert!(
            error.is_permanent(),
            "expected permanent SMTP error: {error:?}"
        );
        assert!(connection.has_broken());

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(commands[0].starts_with("EHLO "));
        assert!(commands[1].starts_with("AUTH OAUTHBEARER "));
        handle.join().unwrap();
    }

    #[tokio1_crate::test(crate = "tokio1_crate")]
    async fn oauthbearer_failed_challenge_sends_cancel_response() {
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
                .write_all(b"250-localhost\r\n250 AUTH OAUTHBEARER\r\n")
                .unwrap();

            let mut auth = String::new();
            reader.read_line(&mut auth).unwrap();
            commands.push(auth);
            stream.write_all(b"334 e30=\r\n").unwrap();

            let mut cancel = String::new();
            reader.read_line(&mut cancel).unwrap();
            commands.push(cancel);
            stream.write_all(b"535 rejected\r\n").unwrap();

            commands_tx.send(commands).unwrap();
        });

        let mut connection =
            AsyncSmtpConnection::connect_tokio1(address, None, &ClientId::default(), None, None)
                .await
                .unwrap();
        let error = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("user", "token"),
            )
            .await
            .unwrap_err();

        assert!(
            error.is_permanent(),
            "expected permanent SMTP error: {error:?}"
        );

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(commands[0].starts_with("EHLO "));
        assert!(commands[1].starts_with("AUTH OAUTHBEARER "));
        assert_eq!(commands[2], "AQ==\r\n");
        assert!(connection.has_broken());
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
    async fn connect_setup_uses_single_deadline_for_banner_and_ehlo() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (ehlo_tx, ehlo_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            thread::sleep(Duration::from_millis(75));
            stream.write_all(b"220 localhost\r\n").unwrap();

            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut ehlo = String::new();
            reader.read_line(&mut ehlo).unwrap();
            ehlo_tx.send(ehlo.starts_with("EHLO ")).unwrap();

            thread::sleep(Duration::from_millis(75));
            let _ = stream.write_all(b"250 localhost\r\n");
        });

        let result = AsyncSmtpConnection::connect_tokio1(
            address,
            Some(Duration::from_millis(120)),
            &ClientId::default(),
            None,
            None,
        )
        .await;

        let Err(error) = result else {
            panic!("connect must use one setup deadline across banner and EHLO");
        };
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
        assert!(ehlo_rx.recv_timeout(Duration::from_secs(3)).unwrap());
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

    #[tokio1_crate::test(crate = "tokio1_crate")]
    async fn cancelled_command_marks_connection_broken() {
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
            thread::sleep(Duration::from_millis(250));
        });

        let mut connection = AsyncSmtpConnection::connect_tokio1(
            address,
            Some(Duration::from_secs(2)),
            &ClientId::default(),
            None,
            None,
        )
        .await
        .unwrap();

        let result =
            tokio1_crate::time::timeout(Duration::from_millis(50), connection.command(Noop)).await;

        assert!(result.is_err(), "command future must be cancelled");
        assert!(connection.has_broken());

        let error = connection.command(Noop).await.unwrap_err();
        assert!(
            error.is_connection(),
            "expected connection error: {error:?}"
        );
        handle.join().unwrap();
    }
}

#[cfg(test)]
#[cfg(feature = "async-std1")]
mod asyncstd_test {
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Duration,
    };

    use crate::{
        address::Envelope,
        transport::smtp::{AsyncSmtpConnection, Protocol, commands::Noop, extension::ClientId},
    };

    #[async_std::test]
    async fn asyncstd_connect_setup_uses_single_deadline_for_banner_and_ehlo() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (ehlo_tx, ehlo_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            thread::sleep(Duration::from_millis(75));
            stream.write_all(b"220 localhost\r\n").unwrap();

            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut ehlo = String::new();
            reader.read_line(&mut ehlo).unwrap();
            ehlo_tx.send(ehlo.starts_with("EHLO ")).unwrap();

            thread::sleep(Duration::from_millis(75));
            let _ = stream.write_all(b"250 localhost\r\n");
        });

        let result = AsyncSmtpConnection::connect_asyncstd1(
            address,
            Some(Duration::from_millis(120)),
            &ClientId::default(),
            None,
        )
        .await;

        let Err(error) = result else {
            panic!("connect must use one setup deadline across banner and EHLO");
        };
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
        assert!(ehlo_rx.recv_timeout(Duration::from_secs(3)).unwrap());
        handle.join().unwrap();
    }

    #[async_std::test]
    async fn asyncstd_cancelled_command_marks_connection_broken() {
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
            thread::sleep(Duration::from_millis(250));
        });

        let mut connection = AsyncSmtpConnection::connect_asyncstd1(
            address,
            Some(Duration::from_secs(2)),
            &ClientId::default(),
            None,
        )
        .await
        .unwrap();

        let result =
            async_std::future::timeout(Duration::from_millis(50), connection.command(Noop)).await;

        assert!(result.is_err(), "command future must be cancelled");
        assert!(connection.has_broken());

        let error = connection.command(Noop).await.unwrap_err();
        assert!(
            error.is_connection(),
            "expected connection error: {error:?}"
        );
        handle.join().unwrap();
    }

    #[async_std::test]
    async fn asyncstd_cancelled_lmtp_delivery_status_loop_marks_connection_broken() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream.write_all(b"220 localhost\r\n").unwrap();

            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut lhlo = String::new();
            reader.read_line(&mut lhlo).unwrap();
            stream
                .write_all(b"250-localhost\r\n250 8BITMIME\r\n")
                .unwrap();

            for response in [
                b"250 sender ok\r\n".as_slice(),
                b"250 first ok\r\n".as_slice(),
                b"250 second ok\r\n".as_slice(),
            ] {
                let mut command = String::new();
                reader.read_line(&mut command).unwrap();
                stream.write_all(response).unwrap();
            }

            let mut data = String::new();
            reader.read_line(&mut data).unwrap();
            stream.write_all(b"354 send message\r\n").unwrap();

            let mut line = String::new();
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == ".\r\n" {
                    break;
                }
            }

            stream.write_all(b"250 first delivered\r\n").unwrap();
            thread::sleep(Duration::from_millis(250));
            let _ = stream.write_all(b"250 second delivered\r\n");
        });

        let mut connection = AsyncSmtpConnection::connect_asyncstd1_with_protocol(
            address,
            Some(Duration::from_secs(2)),
            &ClientId::default(),
            None,
            Protocol::Lmtp,
        )
        .await
        .unwrap();
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                "first@example.com".parse().unwrap(),
                "second@example.com".parse().unwrap(),
            ],
        )
        .unwrap();

        let result = async_std::future::timeout(
            Duration::from_millis(50),
            connection.send_lmtp(&envelope, b"Subject: test\r\n\r\nHello"),
        )
        .await;

        assert!(result.is_err(), "LMTP send future must be cancelled");
        assert!(connection.has_broken());

        let error = connection.command(Noop).await.unwrap_err();
        assert!(
            error.is_connection(),
            "expected connection error: {error:?}"
        );
        handle.join().unwrap();
    }
}
