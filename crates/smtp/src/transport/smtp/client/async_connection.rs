use std::net::IpAddr;
#[cfg(unix)]
use std::path::Path;
use std::{fmt::Display, future::Future, time::Duration};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
        commands::{Auth, Bdat, Data, Ehlo, Expn, Lhlo, Mail, Noop, Rcpt, Rset, Starttls, Vrfy},
        error,
        error::Error,
        extension::{
            ClientId, DeliverByMode, Extension, FutureReleaseParameter, MailBodyParameter,
            MailParameter, RcptParameter, SendOptions, ServerInfo,
            addresses_match_for_recipient_options,
        },
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
enum TimeoutBudget {
    PerOperation(Option<Duration>),
    SetupDeadline(AsyncDeadline),
}

async fn with_timeout<T, F>(
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
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| error::timeout(message)),
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
    /// connection to a specific local address using [`tokio::net::TcpSocket::bind`].
    ///
    /// Sends EHLO and parses server information
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use bifrost_smtp::transport::smtp::{AsyncSmtpConnection, TlsParameters, extension::ClientId};
    /// # use tokio::net::ToSocketAddrs as _;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let connection = AsyncSmtpConnection::connect(
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
    #[cfg(test)]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    pub(crate) async fn connect<T: tokio::net::ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<TlsParameters>,
        local_address: Option<IpAddr>,
    ) -> Result<AsyncSmtpConnection, Error> {
        Self::connect_with_protocol(
            server,
            timeout,
            hello_name,
            tls_parameters,
            local_address,
            Protocol::Smtp,
        )
        .await
    }

    pub(crate) async fn connect_with_protocol<T: tokio::net::ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<TlsParameters>,
        local_address: Option<IpAddr>,
        protocol: Protocol,
    ) -> Result<AsyncSmtpConnection, Error> {
        let deadline = AsyncDeadline::new(timeout);
        #[allow(deprecated)]
        let stream =
            AsyncNetworkStream::connect_until(server, deadline, tls_parameters, local_address)
                .await?;
        Self::connect_impl(
            stream,
            hello_name,
            timeout,
            TimeoutBudget::SetupDeadline(deadline),
            protocol,
        )
        .await
    }

    #[cfg(unix)]
    pub(crate) async fn connect_unix_with_protocol(
        path: &Path,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        protocol: Protocol,
    ) -> Result<AsyncSmtpConnection, Error> {
        let deadline = AsyncDeadline::new(timeout);
        #[allow(deprecated)]
        let stream = AsyncNetworkStream::connect_unix_until(path, deadline).await?;
        Self::connect_impl(
            stream,
            hello_name,
            timeout,
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
        self.send_with_options(envelope, email, &SendOptions::default())
            .await
    }

    pub(crate) async fn send_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        let mail_options = self.mail_options(envelope, email, options, false)?;
        let rcpt_options = self.rcpt_options(envelope, options)?;

        if self.server_info().supports_pipelining() {
            return self
                .send_pipelined(envelope, email, mail_options, rcpt_options)
                .await;
        }

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options))
                .await,
            self
        );

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            try_smtp!(
                self.command(Rcpt::new(to_address.clone(), rcpt_options.clone()))
                    .await,
                self
            );
        }

        try_smtp!(self.command(Data).await, self);
        let result = try_smtp!(self.message(email).await, self);
        Ok(result)
    }

    pub(crate) async fn send_bdat_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        if !self.server_info().supports_chunking() {
            return Err(error::client("BDAT requires server CHUNKING support"));
        }

        let mail_options = self.mail_options(envelope, email, options, true)?;
        let rcpt_options = self.rcpt_options(envelope, options)?;

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options))
                .await,
            self
        );

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            try_smtp!(
                self.command(Rcpt::new(to_address.clone(), rcpt_options.clone()))
                    .await,
                self
            );
        }

        let result = try_smtp!(self.message_bdat(email).await, self);
        Ok(result)
    }

    async fn send_pipelined(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        mail_options: Vec<MailParameter>,
        rcpt_options: Vec<Vec<RcptParameter>>,
    ) -> Result<Response, Error> {
        let mut commands = Mail::new(envelope.from().cloned(), mail_options).to_string();
        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            commands.push_str(&Rcpt::new(to_address.clone(), rcpt_options.clone()).to_string());
        }
        commands.push_str(&Data.to_string());

        self.write(commands.as_bytes()).await?;

        // A dropped pipelined send cannot safely reuse the stream until every
        // queued MAIL/RCPT/DATA response has been consumed.
        // Keep verification and marking broken synchronous with no await gap.
        self.stream.get_ref().state().verify()?;
        self.stream.get_mut().set_state(ConnectionState::Broken);

        let mail_response = self
            .read_response_with_budget_inner(self.per_operation_budget(), true, false)
            .await?;
        let mut recipient_responses = Vec::with_capacity(envelope.to().len());
        for _ in envelope.to() {
            recipient_responses.push(
                self.read_response_with_budget_inner(self.per_operation_budget(), true, false)
                    .await?,
            );
        }
        let data_response = self
            .read_response_with_budget_inner(self.per_operation_budget(), true, false)
            .await?;
        let accepted_recipients = recipient_responses
            .iter()
            .filter(|response| response.is_positive())
            .count();

        self.stream.get_mut().set_state(ConnectionState::Ok);

        if !mail_response.is_positive() {
            self.reset_or_abort_pipelined_transaction(&data_response, accepted_recipients)
                .await;
            return Err(Self::error_from_status(mail_response));
        }

        if let Some(response) = recipient_responses
            .iter()
            .find(|response| !response.is_positive())
        {
            self.reset_or_abort_pipelined_transaction(&data_response, accepted_recipients)
                .await;
            return Err(Self::error_from_status(response.clone()));
        }

        if !data_response.is_positive() {
            self.reset_or_abort_pipelined_transaction(&data_response, accepted_recipients)
                .await;
            return Err(Self::error_from_status(data_response));
        }

        let result = try_smtp!(self.message(email).await, self);
        Ok(result)
    }

    async fn reset_or_abort_pipelined_transaction(
        &mut self,
        data_response: &Response,
        accepted_recipients: usize,
    ) {
        if data_response.is_positive() {
            if accepted_recipients == 0 {
                if self.write(b".\r\n").await.is_err() {
                    self.abort().await;
                    return;
                }
                if self.read_response_accepting_status().await.is_err() {
                    self.abort().await;
                }
            } else {
                self.abort().await;
            }
        } else if self.command_accepting_status(Rset).await.is_err() {
            self.abort().await;
        }
    }

    pub(crate) async fn send_lmtp(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
    ) -> Result<Vec<Response>, Error> {
        self.send_lmtp_with_options(envelope, email, &SendOptions::default())
            .await
    }

    pub(crate) async fn send_lmtp_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        let mail_options = self.mail_options(envelope, email, options, false)?;
        let rcpt_options = self.rcpt_options(envelope, options)?;

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options))
                .await,
            self
        );

        let mut recipient_statuses = Vec::with_capacity(envelope.to().len());
        let mut accepted_recipients = 0;

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            let response = try_smtp!(
                self.command_accepting_status(Rcpt::new(to_address.clone(), rcpt_options.clone()))
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
            let mut rejected = Vec::with_capacity(recipient_statuses.len());
            for response in recipient_statuses {
                let Some(response) = response else {
                    return Err(error::client(
                        "recipient status invariant failed after all recipients were rejected",
                    ));
                };
                rejected.push(response);
            }
            return Ok(rejected);
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

    pub(crate) async fn send_lmtp_bdat_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        if !self.server_info().supports_chunking() {
            return Err(error::client("BDAT requires server CHUNKING support"));
        }

        let mail_options = self.mail_options(envelope, email, options, true)?;
        let rcpt_options = self.rcpt_options(envelope, options)?;

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options))
                .await,
            self
        );

        let mut recipient_statuses = Vec::with_capacity(envelope.to().len());
        let mut accepted_recipients = 0;

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            let response = try_smtp!(
                self.command_accepting_status(Rcpt::new(to_address.clone(), rcpt_options.clone()))
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
            let mut rejected = Vec::with_capacity(recipient_statuses.len());
            for response in recipient_statuses {
                let Some(response) = response else {
                    return Err(error::client(
                        "recipient status invariant failed after all recipients were rejected",
                    ));
                };
                rejected.push(response);
            }
            return Ok(rejected);
        }

        let mut delivery_statuses = try_smtp!(
            self.message_lmtp_bdat(email, accepted_recipients).await,
            self
        )
        .into_iter();

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

    fn mail_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
        allow_binary_mime: bool,
    ) -> Result<Vec<MailParameter>, Error> {
        // Mail
        let mut mail_options = vec![];

        // Internationalization handling
        //
        // * 8BITMIME: https://tools.ietf.org/html/rfc6152
        // * SMTPUTF8: https://tools.ietf.org/html/rfc653

        // Check for non-ascii addresses and use the SMTPUTF8 option if any.
        let has_smtputf8 = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::SmtpUtfEight));
        let has_body_parameter = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::Body(_)));

        if envelope.has_non_ascii_addresses() && !has_smtputf8 {
            if !self.server_info().supports_feature(Extension::SmtpUtfEight) {
                // don't try to send non-ascii addresses (per RFC)
                return Err(error::client(
                    "Envelope contains non-ascii chars but server does not support SMTPUTF8",
                ));
            }
            mail_options.push(MailParameter::SmtpUtfEight);
        }

        // Check for non-ascii content in the message
        if !email.is_ascii() && !has_body_parameter {
            if !self.server_info().supports_feature(Extension::EightBitMime) {
                return Err(error::client(
                    "Message contains non-ascii chars but server does not support 8BITMIME",
                ));
            }
            mail_options.push(MailParameter::Body(MailBodyParameter::EightBitMime));
        }

        if self.server_info().supports_size()
            && !options
                .mail_parameters()
                .iter()
                .any(|parameter| matches!(parameter, MailParameter::Size(_)))
        {
            if self
                .server_info()
                .size_limit()
                .is_some_and(|limit| email.len() > limit)
            {
                return Err(error::client(
                    "Message is larger than the server-advertised SIZE limit",
                ));
            }
            mail_options.push(MailParameter::Size(email.len()));
        }

        for parameter in options.mail_parameters() {
            self.validate_mail_parameter(
                parameter,
                email.len(),
                email.is_ascii(),
                allow_binary_mime,
            )?;
            mail_options.push(parameter.clone());
        }

        Ok(mail_options)
    }

    fn rcpt_options(
        &self,
        envelope: &Envelope,
        options: &SendOptions,
    ) -> Result<Vec<Vec<RcptParameter>>, Error> {
        for (recipient, _) in options.recipient_parameters() {
            if !envelope.to().iter().any(|envelope_recipient| {
                addresses_match_for_recipient_options(recipient, envelope_recipient)
            }) {
                return Err(error::client(
                    "recipient-specific RCPT parameters do not match an envelope recipient",
                ));
            }
        }

        envelope
            .to()
            .iter()
            .map(|recipient| {
                let parameters = options.rcpt_parameters_for(recipient);
                for parameter in &parameters {
                    self.validate_rcpt_parameter(parameter)?;
                }
                Ok(parameters)
            })
            .collect()
    }

    fn validate_mail_parameter(
        &self,
        parameter: &MailParameter,
        message_size: usize,
        message_is_ascii: bool,
        allow_binary_mime: bool,
    ) -> Result<(), Error> {
        parameter.validate_syntax()?;

        match parameter {
            MailParameter::Body(MailBodyParameter::SevenBit) => {
                if message_is_ascii {
                    Ok(())
                } else {
                    Err(error::client(
                        "BODY=7BIT cannot be used with non-ASCII message content",
                    ))
                }
            }
            MailParameter::Body(MailBodyParameter::EightBitMime) => {
                if self.server_info().supports_feature(Extension::EightBitMime) {
                    Ok(())
                } else {
                    Err(error::client(
                        "BODY=8BITMIME requires server 8BITMIME support",
                    ))
                }
            }
            MailParameter::Body(MailBodyParameter::BinaryMime) => {
                if !allow_binary_mime {
                    return Err(error::client("BODY=BINARYMIME requires a BDAT send path"));
                }
                if self.server_info().supports_binary_mime() {
                    Ok(())
                } else {
                    Err(error::client(
                        "BODY=BINARYMIME requires server BINARYMIME support",
                    ))
                }
            }
            MailParameter::Size(size) => {
                if self
                    .server_info()
                    .size_limit()
                    .is_some_and(|limit| *size > limit || message_size > limit)
                {
                    Err(error::client(
                        "Message is larger than the server-advertised SIZE limit",
                    ))
                } else {
                    Ok(())
                }
            }
            MailParameter::SmtpUtfEight => {
                if self.server_info().supports_feature(Extension::SmtpUtfEight) {
                    Ok(())
                } else {
                    Err(error::client("SMTPUTF8 requires server SMTPUTF8 support"))
                }
            }
            MailParameter::RequireTls => {
                if !self.server_info().supports_require_tls() {
                    return Err(error::client(
                        "REQUIRETLS requires server REQUIRETLS support",
                    ));
                }
                if !self.is_encrypted() {
                    return Err(error::policy(
                        "REQUIRETLS requires an encrypted SMTP connection",
                    ));
                }
                Ok(())
            }
            MailParameter::FutureRelease(value) => {
                if !self.server_info().supports_future_release() {
                    return Err(error::client(
                        "FUTURERELEASE requires server FUTURERELEASE support",
                    ));
                }
                match value {
                    FutureReleaseParameter::HoldFor(seconds)
                        if self
                            .server_info()
                            .future_release_max_interval()
                            .is_some_and(|limit| *seconds > limit) =>
                    {
                        return Err(error::client(
                            "HOLDFOR exceeds the server-advertised FUTURERELEASE limit",
                        ));
                    }
                    _ => {}
                }
                Ok(())
            }
            MailParameter::DeliverBy(value) => {
                if !self.server_info().supports_deliver_by() {
                    return Err(error::client("BY requires server DELIVERBY support"));
                }
                if value.mode() == DeliverByMode::Return
                    && value.seconds() > 0
                    && self
                        .server_info()
                        .deliver_by_minimum()
                        .is_some_and(|minimum| value.seconds() < minimum)
                {
                    return Err(error::client(
                        "BY return deadline is below the server-advertised DELIVERBY minimum",
                    ));
                }
                Ok(())
            }
            MailParameter::MtPriority(_) => {
                if self.server_info().supports_mt_priority() {
                    Ok(())
                } else {
                    Err(error::client(
                        "MT-PRIORITY requires server MT-PRIORITY support",
                    ))
                }
            }
            MailParameter::DsnReturn(_) | MailParameter::EnvelopeId(_) => {
                if self.server_info().supports_dsn() {
                    Ok(())
                } else {
                    Err(error::client(
                        "DSN MAIL parameters require server DSN support",
                    ))
                }
            }
            MailParameter::Other { .. } | MailParameter::OtherRaw { .. } => Ok(()),
        }
    }

    fn validate_rcpt_parameter(&self, parameter: &RcptParameter) -> Result<(), Error> {
        parameter.validate_syntax()?;

        match parameter {
            RcptParameter::Notify(_) | RcptParameter::OriginalRecipient { .. } => {
                if self.server_info().supports_dsn() {
                    Ok(())
                } else {
                    Err(error::client(
                        "DSN RCPT parameters require server DSN support",
                    ))
                }
            }
            RcptParameter::Other { .. } => Ok(()),
        }
    }

    pub(crate) fn has_broken(&self) -> bool {
        self.stream.get_ref().state() != ConnectionState::Ok
    }

    // Async STARTTLS is only wired when the tokio backend is enabled.
    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
    pub(crate) fn can_starttls(&self) -> bool {
        !self.is_encrypted() && self.server_info.supports_feature(Extension::StartTls)
    }

    /// Upgrade the connection using `STARTTLS`.
    ///
    /// As described in [rfc3207]. Note that this mechanism has been deprecated in [rfc8314].
    ///
    /// [rfc3207]: https://www.rfc-editor.org/rfc/rfc3207
    /// [rfc8314]: https://www.rfc-editor.org/rfc/rfc8314
    // Async STARTTLS is only wired when the tokio backend is enabled.
    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
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

    pub(crate) async fn abort(&mut self) {
        let _ = self.stream.shutdown().await;
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

    /// Sends a VRFY command and returns the server response.
    pub(crate) async fn verify(&mut self, argument: impl Into<String>) -> Result<Response, Error> {
        self.command_accepting_status(Vrfy::new(argument.into())?)
            .await
    }

    /// Sends an EXPN command and returns the server response.
    pub(crate) async fn expand(&mut self, argument: impl Into<String>) -> Result<Response, Error> {
        self.command_accepting_status(Expn::new(argument.into())?)
            .await
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

    pub(crate) async fn message_bdat(&mut self, message: &[u8]) -> Result<Response, Error> {
        self.write(Bdat::last(message.len()).to_string().as_bytes())
            .await?;
        self.write(message).await?;
        self.read_response().await
    }

    pub(crate) async fn message_lmtp_bdat(
        &mut self,
        message: &[u8],
        recipients: usize,
    ) -> Result<Vec<Response>, Error> {
        self.write(Bdat::last(message.len()).to_string().as_bytes())
            .await?;
        self.write(message).await?;

        // A dropped LMTP BDAT send cannot safely reuse the stream until every
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

    fn error_from_status(response: Response) -> Error {
        error::code(response.code(), Some(response.message().collect()))
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
            budget,
            "SMTP write timed out",
            self.stream.get_mut().write_all(string),
        )
        .await?
        .map_err(error::network)?;
        with_timeout(
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
#[cfg(feature = "tokio")]
mod test {
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Duration,
    };

    use crate::{
        address::Envelope,
        transport::smtp::{
            AsyncSmtpConnection,
            authentication::{Credentials, Mechanism},
            commands::Noop,
            extension::{ClientId, Extension, MailBodyParameter, MailParameter, SendOptions},
        },
    };

    #[tokio::test(crate = "tokio")]
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
            AsyncSmtpConnection::connect(address, None, &ClientId::default(), None, None)
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

    #[tokio::test(crate = "tokio")]
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
            AsyncSmtpConnection::connect(address, None, &ClientId::default(), None, None)
                .await
                .unwrap();

        assert!(!connection.test_connected().await);
        assert!(connection.has_broken());
        handle.join().unwrap();
    }

    #[tokio::test(crate = "tokio")]
    async fn send_uses_pipelining_for_mail_and_recipients() {
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

            let mut ehlo = String::new();
            reader.read_line(&mut ehlo).unwrap();
            commands.push(ehlo);
            stream
                .write_all(b"250-localhost\r\n250-PIPELINING\r\n250 SIZE 1024\r\n")
                .unwrap();

            let expected_batch = concat!(
                "MAIL FROM:<sender@example.com> SIZE=22\r\n",
                "RCPT TO:<first@example.com>\r\n",
                "RCPT TO:<second@example.com>\r\n",
                "DATA\r\n",
            );
            let mut batch = vec![0; expected_batch.len()];
            reader.read_exact(&mut batch).unwrap();
            assert_eq!(batch, expected_batch.as_bytes());
            let batch = String::from_utf8(batch).unwrap();
            commands.extend(batch.split_inclusive('\n').map(str::to_owned));
            stream
                .write_all(b"250 sender ok\r\n250 first ok\r\n250 second ok\r\n354 send body\r\n")
                .unwrap();

            let mut line = String::new();
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == ".\r\n" {
                    break;
                }
            }
            stream.write_all(b"250 queued\r\n").unwrap();

            commands_tx.send(commands).unwrap();
        });

        let mut connection =
            AsyncSmtpConnection::connect(address, None, &ClientId::default(), None, None)
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

        let response = connection
            .send(&envelope, b"Subject: test\r\n\r\nHello")
            .await
            .unwrap();
        assert!(response.has_code(250));

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(commands[0].starts_with("EHLO "));
        assert_eq!(commands[1], "MAIL FROM:<sender@example.com> SIZE=22\r\n");
        assert_eq!(commands[2], "RCPT TO:<first@example.com>\r\n");
        assert_eq!(commands[3], "RCPT TO:<second@example.com>\r\n");
        assert_eq!(commands[4], "DATA\r\n");
        handle.join().unwrap();
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelined_send_rsets_when_data_is_rejected() {
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

            let mut ehlo = String::new();
            reader.read_line(&mut ehlo).unwrap();
            commands.push(ehlo);
            stream
                .write_all(b"250-localhost\r\n250-PIPELINING\r\n250 SIZE 1024\r\n")
                .unwrap();

            let expected_batch = concat!(
                "MAIL FROM:<sender@example.com> SIZE=22\r\n",
                "RCPT TO:<recipient@example.com>\r\n",
                "DATA\r\n",
            );
            let mut batch = vec![0; expected_batch.len()];
            reader.read_exact(&mut batch).unwrap();
            assert_eq!(batch, expected_batch.as_bytes());
            let batch = String::from_utf8(batch).unwrap();
            commands.extend(batch.split_inclusive('\n').map(str::to_owned));

            stream
                .write_all(b"250 sender ok\r\n550 recipient rejected\r\n554 no recipients\r\n")
                .unwrap();

            let mut rset = String::new();
            reader.read_line(&mut rset).unwrap();
            commands.push(rset);
            stream.write_all(b"250 reset ok\r\n").unwrap();

            let mut noop = String::new();
            reader.read_line(&mut noop).unwrap();
            commands.push(noop);
            stream.write_all(b"250 noop ok\r\n").unwrap();

            commands_tx.send(commands).unwrap();
        });

        let mut connection =
            AsyncSmtpConnection::connect(address, None, &ClientId::default(), None, None)
                .await
                .unwrap();
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();

        let result = connection
            .send(&envelope, b"Subject: test\r\n\r\nHello")
            .await;
        assert!(result.is_err());
        assert!(!connection.has_broken());
        assert!(connection.test_connected().await);

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(commands[0].starts_with("EHLO "));
        assert_eq!(commands[1], "MAIL FROM:<sender@example.com> SIZE=22\r\n");
        assert_eq!(commands[2], "RCPT TO:<recipient@example.com>\r\n");
        assert_eq!(commands[3], "DATA\r\n");
        assert_eq!(commands[4], "RSET\r\n");
        assert_eq!(commands[5], "NOOP\r\n");
        handle.join().unwrap();
    }

    #[tokio::test(crate = "tokio")]
    async fn explicit_mail_parameters_are_not_duplicated() {
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

            let mut ehlo = String::new();
            reader.read_line(&mut ehlo).unwrap();
            commands.push(ehlo);
            stream
                .write_all(
                    b"250-localhost\r\n250-PIPELINING\r\n250-SIZE 1024\r\n250-SMTPUTF8\r\n250 8BITMIME\r\n",
                )
                .unwrap();

            for _ in 0..3 {
                let mut command = String::new();
                reader.read_line(&mut command).unwrap();
                commands.push(command);
            }

            stream
                .write_all(b"250 sender ok\r\n250 recipient ok\r\n354 send body\r\n")
                .unwrap();

            let mut line = String::new();
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == ".\r\n" {
                    break;
                }
            }
            stream.write_all(b"250 queued\r\n").unwrap();

            commands_tx.send(commands).unwrap();
        });

        let mut connection =
            AsyncSmtpConnection::connect(address, None, &ClientId::default(), None, None)
                .await
                .unwrap();
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![crate::address::Address::new_dangerous(
                "recipient",
                "exämple.com",
            )],
        )
        .unwrap();
        let options = SendOptions::new()
            .mail_parameter(MailParameter::SmtpUtfEight)
            .mail_parameter(MailParameter::Body(MailBodyParameter::EightBitMime));

        let response = connection
            .send_with_options(&envelope, "Subject: test\r\n\r\nHéllo".as_bytes(), &options)
            .await
            .unwrap();
        assert!(response.has_code(250));

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(
            commands[1],
            "MAIL FROM:<sender@example.com> SIZE=23 SMTPUTF8 BODY=8BITMIME\r\n"
        );
        handle.join().unwrap();
    }

    #[tokio::test(crate = "tokio")]
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
            AsyncSmtpConnection::connect(address, None, &ClientId::default(), None, None)
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

    #[tokio::test(crate = "tokio")]
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
            AsyncSmtpConnection::connect(address, None, &ClientId::default(), None, None)
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

    #[tokio::test(crate = "tokio")]
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
            AsyncSmtpConnection::connect(address, None, &ClientId::default(), None, None)
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

    #[tokio::test(crate = "tokio")]
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
            AsyncSmtpConnection::connect(address, None, &ClientId::default(), None, None)
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

    #[tokio::test(crate = "tokio")]
    async fn connect_times_out_waiting_for_banner() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(500));
        });

        let result = AsyncSmtpConnection::connect(
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

    #[tokio::test(crate = "tokio")]
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

        let result = AsyncSmtpConnection::connect(
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

    #[tokio::test(crate = "tokio")]
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

        let mut connection = AsyncSmtpConnection::connect(
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

    #[tokio::test(crate = "tokio")]
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

        let mut connection = AsyncSmtpConnection::connect(
            address,
            Some(Duration::from_secs(2)),
            &ClientId::default(),
            None,
            None,
        )
        .await
        .unwrap();

        let result =
            tokio::time::timeout(Duration::from_millis(50), connection.command(Noop)).await;

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
