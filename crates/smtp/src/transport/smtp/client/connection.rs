use std::{
    fmt::Display,
    io::{self, BufRead, BufReader, Write},
    net::{IpAddr, ToSocketAddrs},
    time::Duration,
};

#[cfg(feature = "tracing")]
use super::escape_crlf;
use super::{
    ClientCodec, MAX_RESPONSE_BYTES, MAX_RESPONSE_LINE_BYTES, NetworkStream, TlsParameters,
};
#[cfg(feature = "native-tls")]
use crate::transport::smtp::commands::Starttls;
use crate::{
    address::Envelope,
    transport::smtp::{
        Protocol,
        authentication::{Credentials, Mechanism},
        commands::{Auth, Data, Ehlo, Lhlo, Mail, Noop, Quit, Rcpt},
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
                $client.abort();
                return Err(From::from(err))
            },
        }
    })
);

/// Structure that implements the SMTP client
pub(crate) struct SmtpConnection {
    /// TCP stream between client and server
    /// Value is None before connection
    stream: BufReader<NetworkStream>,
    /// Panic state
    panic: bool,
    /// Information about the server
    server_info: ServerInfo,
    /// Client identity used for EHLO.
    hello_name: ClientId,
    /// Wire protocol used for this connection.
    protocol: Protocol,
}

impl SmtpConnection {
    /// Get information about the server
    pub(crate) fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    // FIXME add simple connect and rename this one

    /// Connects to the configured server
    ///
    /// Sends EHLO and parses server information
    #[cfg(test)]
    pub(crate) fn connect<A: ToSocketAddrs>(
        server: A,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<&TlsParameters>,
        local_address: Option<IpAddr>,
    ) -> Result<SmtpConnection, Error> {
        Self::connect_with_protocol(
            server,
            timeout,
            hello_name,
            tls_parameters,
            local_address,
            Protocol::Smtp,
        )
    }

    pub(crate) fn connect_with_protocol<A: ToSocketAddrs>(
        server: A,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<&TlsParameters>,
        local_address: Option<IpAddr>,
        protocol: Protocol,
    ) -> Result<SmtpConnection, Error> {
        let stream = NetworkStream::connect(server, timeout, tls_parameters, local_address)?;
        let stream = BufReader::new(stream);
        let mut conn = SmtpConnection {
            stream,
            panic: false,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
        };
        conn.set_timeout(timeout).map_err(error::network)?;
        // TODO log
        let _response = conn.read_response()?;

        conn.hello(hello_name)?;

        // Print server information
        #[cfg(feature = "tracing")]
        tracing::debug!("server {}", conn.server_info);
        Ok(conn)
    }

    pub(crate) fn send(&mut self, envelope: &Envelope, email: &[u8]) -> Result<Response, Error> {
        let mail_options = self.mail_options(envelope, email)?;

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options)),
            self
        );

        for to_address in envelope.to() {
            try_smtp!(self.command(Rcpt::new(to_address.clone(), vec![])), self);
        }

        try_smtp!(self.command(Data), self);
        let result = try_smtp!(self.message(email), self);
        Ok(result)
    }

    pub(crate) fn send_lmtp(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
    ) -> Result<Vec<Response>, Error> {
        let mail_options = self.mail_options(envelope, email)?;

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options)),
            self
        );

        let mut recipient_statuses = Vec::with_capacity(envelope.to().len());
        let mut accepted_recipients = 0;

        for to_address in envelope.to() {
            let response = try_smtp!(
                self.command_accepting_status(Rcpt::new(to_address.clone(), vec![])),
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

        try_smtp!(self.command(Data), self);
        let mut delivery_statuses =
            try_smtp!(self.message_lmtp(email, accepted_recipients), self).into_iter();

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
        self.panic
    }

    // Without sync native-tls, no public transport path can perform STARTTLS.
    #[cfg_attr(not(feature = "native-tls"), allow(dead_code))]
    pub(crate) fn can_starttls(&self) -> bool {
        !self.is_encrypted() && self.server_info.supports_feature(Extension::StartTls)
    }

    // Without sync native-tls, no public transport path can perform STARTTLS.
    #[allow(unused_variables)]
    #[cfg_attr(not(feature = "native-tls"), allow(dead_code))]
    pub(crate) fn starttls(
        &mut self,
        tls_parameters: &TlsParameters,
        hello_name: &ClientId,
    ) -> Result<(), Error> {
        if self.server_info.supports_feature(Extension::StartTls) {
            #[cfg(feature = "native-tls")]
            {
                try_smtp!(self.command(Starttls), self);
                self.stream.get_mut().upgrade_tls(tls_parameters)?;
                #[cfg(feature = "tracing")]
                tracing::debug!("connection encrypted");
                // Send EHLO/LHLO again
                try_smtp!(self.hello(hello_name), self);
                self.hello_name = hello_name.clone();
                Ok(())
            }
            #[cfg(not(feature = "native-tls"))]
            // This should never happen as `Tls` can only be created
            // when a TLS library is enabled
            unreachable!("TLS support required but not supported");
        } else {
            Err(error::client("STARTTLS is not supported on this server"))
        }
    }

    /// Send EHLO or LHLO and update server info
    fn hello(&mut self, hello_name: &ClientId) -> Result<(), Error> {
        let response = match self.protocol {
            Protocol::Smtp => try_smtp!(self.command(Ehlo::new(hello_name.clone())), self),
            Protocol::Lmtp => try_smtp!(self.command(Lhlo::new(hello_name.clone())), self),
        };
        self.server_info = try_smtp!(ServerInfo::from_response(&response), self);
        Ok(())
    }

    #[cfg_attr(feature = "pool", allow(dead_code))]
    pub(crate) fn quit(&mut self) -> Result<Response, Error> {
        Ok(try_smtp!(self.command(Quit), self))
    }

    pub(crate) fn abort(&mut self) {
        self.panic = true;
        let _ = self.stream.get_mut().shutdown(std::net::Shutdown::Both);
    }

    /// Tells if the underlying stream is currently encrypted
    pub(crate) fn is_encrypted(&self) -> bool {
        self.stream.get_ref().is_encrypted()
    }

    /// Set timeout
    pub(crate) fn set_timeout(&mut self, duration: Option<Duration>) -> io::Result<()> {
        self.stream.get_mut().set_read_timeout(duration)?;
        self.stream.get_mut().set_write_timeout(duration)
    }

    /// Checks if the server is connected using the NOOP SMTP command.
    ///
    /// A failed check marks the connection broken and closes it. A connection
    /// that cannot answer NOOP is not safe to keep in the pool.
    pub(crate) fn test_connected(&mut self) -> bool {
        match self.command(Noop) {
            Ok(_) => true,
            Err(_) => {
                self.abort();
                false
            }
        }
    }

    /// Sends an AUTH command with the given mechanism, and handles the challenge if needed
    pub(crate) fn auth(
        &mut self,
        mechanisms: &[Mechanism],
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        let mechanism = self
            .server_info
            .get_auth_mechanism(mechanisms)
            .ok_or_else(|| error::client("No compatible authentication mechanism was found"))?;

        // Limit challenges to avoid blocking
        let mut challenges = 10;
        let mut response = self.command(Auth::new(mechanism, credentials.clone(), None)?)?;

        while challenges > 0 && response.has_code(334) {
            challenges -= 1;
            response = try_smtp!(
                self.command(Auth::new_from_response(
                    mechanism,
                    credentials.clone(),
                    &response,
                )?),
                self
            );
        }

        if challenges == 0 {
            Err(error::response("Unexpected number of challenges"))
        } else {
            let hello_name = self.hello_name.clone();
            try_smtp!(self.hello(&hello_name), self);
            Ok(response)
        }
    }

    /// Sends the message content
    pub(crate) fn message(&mut self, message: &[u8]) -> Result<Response, Error> {
        self.message_iter(std::iter::once(message))
    }

    pub(crate) fn message_lmtp(
        &mut self,
        message: &[u8],
        recipients: usize,
    ) -> Result<Vec<Response>, Error> {
        self.message_lmtp_iter(std::iter::once(message), recipients)
    }

    /// Sends the message content by consuming an iterator that in its whole represents a message.
    pub(crate) fn message_iter<I, B>(&mut self, message: I) -> Result<Response, Error>
    where
        I: Iterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut codec = ClientCodec::new();
        for message_part in message {
            let message_part = message_part.as_ref();
            let mut out_buf = Vec::with_capacity(message_part.len());
            codec.encode(message_part, &mut out_buf);
            self.write(out_buf.as_slice())?;
        }
        self.write(b"\r\n.\r\n")?;

        self.read_response()
    }

    /// Sends the message content and reads one LMTP status per recipient.
    pub(crate) fn message_lmtp_iter<I, B>(
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
            self.write(out_buf.as_slice())?;
        }
        self.write(b"\r\n.\r\n")?;

        let mut responses = Vec::with_capacity(recipients);
        for _ in 0..recipients {
            responses.push(self.read_response_accepting_status()?);
        }

        Ok(responses)
    }

    /// Sends an SMTP command
    pub(crate) fn command<C: Display>(&mut self, command: C) -> Result<Response, Error> {
        self.write(command.to_string().as_bytes())?;
        self.read_response()
    }

    fn command_accepting_status<C: Display>(&mut self, command: C) -> Result<Response, Error> {
        self.write(command.to_string().as_bytes())?;
        self.read_response_accepting_status()
    }

    /// Writes a string to the server
    fn write(&mut self, string: &[u8]) -> Result<(), Error> {
        self.stream
            .get_mut()
            .write_all(string)
            .map_err(error::network)?;
        self.stream.get_mut().flush().map_err(error::network)?;

        #[cfg(feature = "tracing")]
        tracing::debug!("Wrote: {}", escape_crlf(&String::from_utf8_lossy(string)));
        Ok(())
    }

    /// Gets the SMTP response
    pub(crate) fn read_response(&mut self) -> Result<Response, Error> {
        self.read_response_inner(false)
    }

    fn read_response_accepting_status(&mut self) -> Result<Response, Error> {
        self.read_response_inner(true)
    }

    fn read_response_inner(&mut self, accept_negative: bool) -> Result<Response, Error> {
        let mut buffer = String::with_capacity(100);
        let mut pre = 0;

        while self.stream.read_line(&mut buffer).map_err(error::network)? > 0 {
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
mod test {
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Duration,
    };

    use crate::transport::smtp::{
        SmtpConnection,
        authentication::{Credentials, Mechanism},
        extension::{ClientId, Extension},
    };

    #[test]
    fn abort_closes_without_quit_command() {
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
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();
        connection.abort();

        let observed = observed_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(
            !observed.contains("QUIT"),
            "abort must close without sending QUIT, got {observed:?}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn failed_test_connected_marks_connection_broken() {
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
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();

        assert!(!connection.test_connected());
        assert!(connection.has_broken());
        handle.join().unwrap();
    }

    #[test]
    fn auth_refreshes_server_info_with_ehlo() {
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
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();
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
}
