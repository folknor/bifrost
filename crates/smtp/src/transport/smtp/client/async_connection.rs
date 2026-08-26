use std::net::IpAddr;
#[cfg(unix)]
use std::path::Path;
use std::{
    fmt::{Display, Write as _},
    future::Future,
    time::Duration,
};

use bifrost_sasl::ScramChannelBinding;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use zeroize::{Zeroize, Zeroizing};

use super::async_net::AsyncDeadline;
#[cfg(feature = "tracing")]
use super::escape_crlf;
use super::metering::WireMetering;
use super::{
    ClientCodec, ConnectionState, MAX_RESPONSE_BYTES, MAX_RESPONSE_LINE_BYTES,
    PIPELINING_RECIPIENT_WINDOW, PhasedError, TlsParameters, async_net::AsyncNetworkStream,
    data_terminator, merge_lmtp_statuses, smtp_data_size,
};
use crate::{
    Envelope,
    address::Address,
    transport::smtp::{
        Protocol,
        authentication::{
            Credentials, Mechanism, ScramExchange, ScramStep, decode_auth_challenge,
            decode_scram_payload, oauth_mechanism, password_mechanism, resolve_scram_binding,
            scram_hash,
        },
        batch::{RecipientProgress, SendProgress, SmtpBatchRecipient},
        commands::{
            Auth, Bdat, Data, Ehlo, Expn, Lhlo, Mail, Noop, Rcpt, Rset, Starttls, Vrfy,
            build_recipient_commands, build_transaction_commands,
        },
        error,
        error::{Error, SmtpCommandPhase, SmtpTransmissionState},
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
    });
    // Phase-tagged variant (mirrors the sync sibling). Stamps the SMTP
    // error with the given SmtpCommandPhase so the translation
    // boundary in `account_error.rs` can route per-phase.
    ($err: expr, $client: ident, $phase: expr) => ({
        match $err {
            Ok(val) => val,
            Err(err) => {
                $client.abort().await;
                return Err(From::from(err.with_phase($phase)))
            },
        }
    });
);

/// `try_smtp!` for the phase-typed pipelined driver: aborts the connection and
/// returns a `PhasedError`, which is the only error this driver can produce.
macro_rules! try_phased (
    ($err: expr, $client: ident, $phase: expr) => ({
        match $err {
            Ok(val) => val,
            Err(err) => {
                $client.abort().await;
                return Err(PhasedError::new($phase, Error::from(err)))
            },
        }
    });
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
    stream: BufReader<AsyncNetworkStream>,
    /// Information about the server
    server_info: ServerInfo,
    /// Client identity used for EHLO.
    hello_name: ClientId,
    /// Wire protocol used for this connection.
    protocol: Protocol,
    /// Timeout applied to each async SMTP I/O operation.
    timeout: Option<Duration>,
    /// Reused storage for serializing individual SMTP commands.
    command_buffer: Zeroizing<String>,
    /// Set once an LMTP final-status drain has run: the connection must not
    /// go back into the pool because stream cleanliness cannot be proven.
    retire: bool,
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
    #[allow(dead_code)]
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
            WireMetering::disabled(),
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
        metering: WireMetering,
    ) -> Result<AsyncSmtpConnection, Error> {
        let deadline = AsyncDeadline::new(timeout);
        let mut stream =
            AsyncNetworkStream::connect_until(server, deadline, tls_parameters, local_address)
                .await?;
        // Installed on the dialed stream rather than threaded into the
        // dialer: the TCP/TLS handshake is not the account's traffic.
        stream.set_metering(metering);
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
        metering: WireMetering,
    ) -> Result<AsyncSmtpConnection, Error> {
        let deadline = AsyncDeadline::new(timeout);
        let mut stream = AsyncNetworkStream::connect_unix_until(path, deadline).await?;
        stream.set_metering(metering);
        Self::connect_impl(
            stream,
            hello_name,
            timeout,
            TimeoutBudget::SetupDeadline(deadline),
            protocol,
        )
        .await
    }

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
            command_buffer: Zeroizing::new(String::new()),
            retire: false,
        };
        let _response = conn.read_response_with_budget(setup_budget).await?;

        conn.hello_with_budget(hello_name, setup_budget).await?;

        // Print server information
        #[cfg(feature = "tracing")]
        tracing::debug!("server {}", conn.server_info);
        Ok(conn)
    }

    // Widened so the pool and transport tests can drive a scripted peer
    // through the public entry points instead of a socket.
    #[cfg(test)]
    pub(in crate::transport::smtp) async fn from_transcript(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Protocol,
    ) -> Result<Self, Error> {
        Self::connect_impl(
            AsyncNetworkStream::from_transcript(transcript),
            hello_name,
            None,
            TimeoutBudget::PerOperation(None),
            protocol,
        )
        .await
    }

    /// Transcript setup with byte accounting installed, so a test can
    /// observe what the real send path would have metered.
    #[cfg(test)]
    pub(in crate::transport::smtp) async fn from_transcript_metered(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Protocol,
        metering: WireMetering,
    ) -> Result<Self, Error> {
        let mut stream = AsyncNetworkStream::from_transcript(transcript);
        stream.set_metering(metering);
        Self::connect_impl(
            stream,
            hello_name,
            None,
            TimeoutBudget::PerOperation(None),
            protocol,
        )
        .await
    }

    /// Transcript setup with a peer-certificate DER injected on the stream,
    /// so the connection-level channel-binding gate in `auth` (resolve the
    /// binding only when an allowed PLUS mechanism is advertised, and treat a
    /// present-but-unusable certificate as a hard error) is testable without
    /// TLS.
    #[cfg(test)]
    pub(in crate::transport::smtp) async fn from_transcript_with_peer_certificate(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Protocol,
        peer_certificate_der: Vec<u8>,
    ) -> Result<Self, Error> {
        let mut stream = AsyncNetworkStream::from_transcript(transcript);
        stream.set_test_peer_certificate_der(peer_certificate_der);
        Self::connect_impl(
            stream,
            hello_name,
            None,
            TimeoutBudget::PerOperation(None),
            protocol,
        )
        .await
    }

    /// Transcript setup that goes through the same single setup deadline the
    /// real `connect` path uses, so banner and EHLO share one budget.
    #[cfg(test)]
    pub(in crate::transport::smtp) async fn from_transcript_with_timeout(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Protocol,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let deadline = AsyncDeadline::new(Some(timeout));
        Self::connect_impl(
            AsyncNetworkStream::from_transcript(transcript),
            hello_name,
            Some(timeout),
            TimeoutBudget::SetupDeadline(deadline),
            protocol,
        )
        .await
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

        let (mail, recipients) = build_transaction_commands(envelope, mail_options, &rcpt_options)?;

        if self.server_info().supports_pipelining() {
            return self.send_pipelined(email, mail, recipients).await;
        }

        try_smtp!(self.command(mail).await, self, SmtpCommandPhase::MailFrom);

        for recipient in recipients {
            try_smtp!(
                self.command(recipient).await,
                self,
                SmtpCommandPhase::RcptTo
            );
        }

        try_smtp!(
            self.command(Data).await,
            self,
            SmtpCommandPhase::DataCommand
        );
        let result = try_smtp!(self.message(email).await, self, SmtpCommandPhase::DataBody);
        Ok(result)
    }

    pub(crate) async fn send_bdat_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        if !self.server_info().supports_chunking() {
            return Err(error::invalid_input(
                "BDAT requires server CHUNKING support",
            ));
        }

        let mail_options = self.mail_options(envelope, email, options, true)?;
        let rcpt_options = self.rcpt_options(envelope, options)?;

        let (mail, recipients) = build_transaction_commands(envelope, mail_options, &rcpt_options)?;

        try_smtp!(self.command(mail).await, self, SmtpCommandPhase::MailFrom);

        for recipient in recipients {
            try_smtp!(
                self.command(recipient).await,
                self,
                SmtpCommandPhase::RcptTo
            );
        }

        let result = try_smtp!(
            self.message_bdat(email).await,
            self,
            SmtpCommandPhase::BdatBody
        );
        Ok(result)
    }

    /// Phase-stamping funnel for the pipelined driver.
    ///
    /// `send_pipelined_inner` cannot return an undecorated error: its error
    /// type is `PhasedError`, which has no `From<Error>` conversion, so `?`
    /// on a plain SMTP result does not compile there. This is the one place
    /// that turns a boundary failure back into an `Error`.
    async fn send_pipelined(
        &mut self,
        email: &[u8],
        mail: Mail,
        recipients: Vec<Rcpt>,
    ) -> Result<Response, Error> {
        self.send_pipelined_inner(email, mail, recipients)
            .await
            .map_err(PhasedError::into_error)
    }

    async fn send_pipelined_inner(
        &mut self,
        email: &[u8],
        mail: Mail,
        recipients: Vec<Rcpt>,
    ) -> Result<Response, PhasedError> {
        for (window_index, window) in recipients.chunks(PIPELINING_RECIPIENT_WINDOW).enumerate() {
            let mut commands = String::new();
            if window_index == 0 {
                commands.push_str(&mail.to_string());
            }
            for recipient in window {
                commands.push_str(&recipient.to_string());
            }
            // Keep the stream broken while this whole window is outstanding.
            // If this future is cancelled after only part of the replies have
            // been drained, the pool must not reuse a misaligned connection.
            let write_phase = if window_index == 0 {
                SmtpCommandPhase::MailFrom
            } else {
                SmtpCommandPhase::RcptTo
            };
            try_phased!(self.write(commands.as_bytes()).await, self, write_phase);
            try_phased!(self.stream.get_ref().state().verify(), self, write_phase);
            self.stream.get_mut().set_state(ConnectionState::Broken);

            if window_index == 0 {
                let mail_response = try_phased!(
                    self.read_response_with_budget_inner(self.per_operation_budget(), true, false)
                        .await,
                    self,
                    SmtpCommandPhase::MailFrom
                );
                if !mail_response.is_positive() {
                    for _ in window {
                        try_phased!(
                            self.read_response_with_budget_inner(
                                self.per_operation_budget(),
                                true,
                                false,
                            )
                            .await,
                            self,
                            SmtpCommandPhase::RcptTo
                        );
                    }
                    // No RSET: a rejected MAIL FROM opened no transaction, so
                    // there is nothing to reset and the connection stays
                    // reusable as it is.
                    try_phased!(self.finish_reply_group(), self, SmtpCommandPhase::RcptTo);
                    return Err(PhasedError::new(
                        SmtpCommandPhase::MailFrom,
                        error::status(mail_response),
                    ));
                }
            }

            let mut failure = None;
            for _ in window {
                let response = try_phased!(
                    self.read_response_with_budget_inner(self.per_operation_budget(), true, false)
                        .await,
                    self,
                    SmtpCommandPhase::RcptTo
                );
                if failure.is_none() && !response.is_positive() {
                    failure = Some(response);
                }
            }
            try_phased!(self.finish_reply_group(), self, SmtpCommandPhase::RcptTo);
            if let Some(response) = failure {
                self.reset_transaction().await;
                return Err(PhasedError::new(
                    SmtpCommandPhase::RcptTo,
                    error::status(response),
                ));
            }
        }

        let data_response = try_phased!(
            self.command_accepting_status(Data).await,
            self,
            SmtpCommandPhase::DataCommand
        );
        if !data_response.is_positive() {
            self.reset_transaction().await;
            return Err(PhasedError::new(
                SmtpCommandPhase::DataCommand,
                error::status(data_response),
            ));
        }

        let result = try_phased!(self.message(email).await, self, SmtpCommandPhase::DataBody);
        Ok(result)
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

        let (mail, recipients) = build_transaction_commands(envelope, mail_options, &rcpt_options)?;

        try_smtp!(self.command(mail).await, self, SmtpCommandPhase::MailFrom);

        let mut recipient_statuses = Vec::with_capacity(recipients.len());
        let mut accepted_recipients = 0;

        for recipient in recipients {
            let response = try_smtp!(
                self.command_accepting_status(recipient).await,
                self,
                SmtpCommandPhase::RcptTo
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
                    return Err(error::internal(
                        "recipient status invariant failed after all recipients were rejected",
                    ));
                };
                rejected.push(response);
            }
            self.reset_transaction().await;
            return Ok(rejected);
        }

        try_smtp!(
            self.command(Data).await,
            self,
            SmtpCommandPhase::DataCommand
        );
        let delivery_statuses = try_smtp!(
            self.message_lmtp(email, accepted_recipients).await,
            self,
            SmtpCommandPhase::LmtpFinalStatus
        );

        merge_lmtp_statuses(recipient_statuses, delivery_statuses)
    }

    pub(crate) async fn send_lmtp_bdat_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        if !self.server_info().supports_chunking() {
            return Err(error::invalid_input(
                "BDAT requires server CHUNKING support",
            ));
        }

        let mail_options = self.mail_options(envelope, email, options, true)?;
        let rcpt_options = self.rcpt_options(envelope, options)?;

        let (mail, recipients) = build_transaction_commands(envelope, mail_options, &rcpt_options)?;

        try_smtp!(self.command(mail).await, self, SmtpCommandPhase::MailFrom);

        let mut recipient_statuses = Vec::with_capacity(recipients.len());
        let mut accepted_recipients = 0;

        for recipient in recipients {
            let response = try_smtp!(
                self.command_accepting_status(recipient).await,
                self,
                SmtpCommandPhase::RcptTo
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
                    return Err(error::internal(
                        "recipient status invariant failed after all recipients were rejected",
                    ));
                };
                rejected.push(response);
            }
            self.reset_transaction().await;
            return Ok(rejected);
        }

        let delivery_statuses = try_smtp!(
            self.message_lmtp_bdat(email, accepted_recipients).await,
            self,
            SmtpCommandPhase::LmtpFinalStatus
        );

        merge_lmtp_statuses(recipient_statuses, delivery_statuses)
    }

    /// Async account-oriented SMTP multi-recipient send.
    ///
    /// Drives the SMTP command sequence (MAIL FROM, sequential RCPTs, DATA,
    /// body, final reply) and records per-recipient progress into a
    /// `SendProgress` tracker.
    pub(crate) async fn send_smtp_batch(
        &mut self,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        let mut progress = SendProgress::new(Protocol::Smtp, recipients);
        let rcpt_options_all = self
            .rcpt_options_for_batch(&progress.recipients, options)
            .map_err(|error| {
                (
                    error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::RcptTo),
                    progress.clone(),
                )
            })?;

        let mail_options = self
            .mail_options_for_batch(from.as_ref(), &progress.recipients, email, options, false)
            .map_err(|e| {
                (
                    e.with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress.clone(),
                )
            })?;

        // Every envelope address is validated here, before `MAIL FROM` opens a
        // transaction. Constructing an `Rcpt` further down would let a rejected
        // recipient return past an open transaction without aborting, leaving
        // the connection poolable but dirty.
        let mail_cmd = Mail::new(from, mail_options).map_err(|e| (e, progress.clone()))?;
        let rcpt_cmds = build_recipient_commands(
            progress.recipients.iter().map(|r| r.address.clone()),
            &rcpt_options_all,
        )
        .map_err(|e| (e, progress.clone()))?;

        if self.server_info().supports_pipelining() {
            return self
                .send_smtp_batch_pipelined(email, mail_cmd, rcpt_cmds, progress)
                .await;
        }

        // Before DATA, no message content can have reached the peer,
        // regardless of whether the failed operation was a write or a reply
        // drain. Negative replies remain `Acknowledged`.
        match self.command_accepting_status(mail_cmd).await {
            Ok(resp) if resp.is_positive() => {}
            Ok(resp) => {
                self.abort().await;
                return Err((
                    error::status(resp)
                        .with_attempt(SmtpTransmissionState::Acknowledged)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
            Err(e) => {
                self.abort().await;
                return Err((
                    e.with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
        }

        for (i, rcpt_cmd) in rcpt_cmds.into_iter().enumerate() {
            match self.command_accepting_status(rcpt_cmd).await {
                Ok(resp) if resp.is_positive() => progress.record_rcpt_accepted(i),
                Ok(resp) => progress.record_rcpt_rejected(i, resp),
                Err(e) => {
                    use crate::transport::smtp::account_error::{
                        SmtpErrorContext, into_account_error,
                    };
                    let ae = into_account_error(
                        e.with_attempt(SmtpTransmissionState::Unsent)
                            .with_phase(SmtpCommandPhase::RcptTo),
                        SmtpErrorContext::send(Protocol::Smtp).with_phase(SmtpCommandPhase::RcptTo),
                    );
                    progress.mark_unresolved_unsent(|| ae.clone());
                    self.abort().await;
                    return Ok(progress);
                }
            }
        }

        let accepted = progress.recipients.iter().any(|r| {
            matches!(
                r.rcpt,
                crate::transport::smtp::batch::RcptProgress::Accepted
            )
        });
        if !accepted {
            self.reset_transaction().await;
            return Ok(progress);
        }

        match self.command_accepting_status(Data).await {
            Ok(resp) if resp.is_positive() => {}
            Ok(resp) => {
                progress.mark_accepted_rejected_with_response(resp);
                self.reset_transaction().await;
                return Ok(progress);
            }
            Err(e) => {
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataCommand),
                    SmtpErrorContext::send(Protocol::Smtp)
                        .with_phase(SmtpCommandPhase::DataCommand),
                );
                let ae2 = ae.clone();
                progress.mark_accepted_uncertain(|| ae2.clone());
                self.abort().await;
                return Ok(progress);
            }
        }

        progress.set_body_started();
        match self.message(email).await {
            Ok(resp) => {
                progress.set_body_finished();
                progress.set_data_response(resp);
            }
            Err(e) => {
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataBody),
                    SmtpErrorContext::send(Protocol::Smtp).with_phase(SmtpCommandPhase::DataBody),
                );
                let ae2 = ae.clone();
                progress.mark_uncertain_unresolved(|| ae2.clone());
                self.abort().await;
                return Ok(progress);
            }
        }

        Ok(progress)
    }

    async fn send_smtp_batch_pipelined(
        &mut self,
        email: &[u8],
        mail_cmd: Mail,
        rcpt_cmds: Vec<Rcpt>,
        mut progress: SendProgress,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        for window_start in (0..progress.recipients.len()).step_by(PIPELINING_RECIPIENT_WINDOW) {
            let window_end =
                (window_start + PIPELINING_RECIPIENT_WINDOW).min(progress.recipients.len());
            let mut commands = String::new();
            if window_start == 0 {
                commands.push_str(&mail_cmd.to_string());
            }
            for rcpt in &rcpt_cmds[window_start..window_end] {
                commands.push_str(&rcpt.to_string());
            }
            if let Err(e) = self.write(commands.as_bytes()).await {
                if window_start == 0 {
                    self.abort().await;
                    return Err((
                        e.with_attempt(SmtpTransmissionState::Unsent)
                            .with_phase(SmtpCommandPhase::MailFrom),
                        progress,
                    ));
                }
                // A later recipient window failed to write. `DATA` is only
                // issued after every window, so no message content can have
                // reached the peer: the still-open recipients are `Unsent`,
                // and the RCPT replies already collected stay authoritative.
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::RcptTo),
                    SmtpErrorContext::send(Protocol::Smtp).with_phase(SmtpCommandPhase::RcptTo),
                );
                progress.mark_unresolved_unsent(|| ae.clone());
                self.abort().await;
                return Ok(progress);
            }

            // A batch keeps recipient indexes across windows, but the stream
            // is only reusable after every reply for this window is drained.
            self.stream.get_ref().state().verify().map_err(|error| {
                (
                    error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::RcptTo),
                    progress.clone(),
                )
            })?;
            self.stream.get_mut().set_state(ConnectionState::Broken);

            if window_start == 0 {
                let mail_response = match self
                    .read_response_with_budget_inner(self.per_operation_budget(), true, false)
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        self.abort().await;
                        return Err((
                            e.with_attempt(SmtpTransmissionState::Unsent)
                                .with_phase(SmtpCommandPhase::MailFrom),
                            progress,
                        ));
                    }
                };
                if !mail_response.is_positive() {
                    self.abort().await;
                    return Err((
                        error::status(mail_response)
                            .with_attempt(SmtpTransmissionState::Acknowledged)
                            .with_phase(SmtpCommandPhase::MailFrom),
                        progress,
                    ));
                }
            }

            for i in window_start..window_end {
                match self
                    .read_response_with_budget_inner(self.per_operation_budget(), true, false)
                    .await
                {
                    Ok(resp) if resp.is_positive() => progress.record_rcpt_accepted(i),
                    Ok(resp) => progress.record_rcpt_rejected(i, resp),
                    Err(e) => {
                        use crate::transport::smtp::account_error::{
                            SmtpErrorContext, into_account_error,
                        };
                        let ae = into_account_error(
                            e.with_attempt(SmtpTransmissionState::Unsent)
                                .with_phase(SmtpCommandPhase::RcptTo),
                            SmtpErrorContext::send(Protocol::Smtp)
                                .with_phase(SmtpCommandPhase::RcptTo),
                        );
                        progress.mark_unresolved_unsent(|| ae.clone());
                        self.abort().await;
                        return Ok(progress);
                    }
                }
            }
            self.finish_reply_group().map_err(|error| {
                (
                    error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::RcptTo),
                    progress.clone(),
                )
            })?;
        }

        let accepted = progress.recipients.iter().any(|r| {
            matches!(
                r.rcpt,
                crate::transport::smtp::batch::RcptProgress::Accepted
            )
        });
        if !accepted {
            self.reset_transaction().await;
            return Ok(progress);
        }

        let data_response = match self.command_accepting_status(Data).await {
            Ok(r) => r,
            Err(e) => {
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataCommand),
                    SmtpErrorContext::send(Protocol::Smtp)
                        .with_phase(SmtpCommandPhase::DataCommand),
                );
                let ae2 = ae.clone();
                progress.mark_uncertain_unresolved(|| ae2.clone());
                self.abort().await;
                return Ok(progress);
            }
        };

        if !data_response.is_positive() {
            progress.mark_accepted_rejected_with_response(data_response);
            self.reset_transaction().await;
            return Ok(progress);
        }

        progress.set_body_started();
        match self.message(email).await {
            Ok(resp) => {
                progress.set_body_finished();
                progress.set_data_response(resp);
            }
            Err(e) => {
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataBody),
                    SmtpErrorContext::send(Protocol::Smtp).with_phase(SmtpCommandPhase::DataBody),
                );
                let ae2 = ae.clone();
                progress.mark_uncertain_unresolved(|| ae2.clone());
                self.abort().await;
                return Ok(progress);
            }
        }

        Ok(progress)
    }

    /// Async account-oriented LMTP multi-recipient send.
    pub(crate) async fn send_lmtp_batch(
        &mut self,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        let mut progress = SendProgress::new(Protocol::Lmtp, recipients);
        let rcpt_options_all = self
            .rcpt_options_for_batch(&progress.recipients, options)
            .map_err(|error| {
                (
                    error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::RcptTo),
                    progress.clone(),
                )
            })?;

        let mail_options = self
            .mail_options_for_batch(from.as_ref(), &progress.recipients, email, options, false)
            .map_err(|e| {
                (
                    e.with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress.clone(),
                )
            })?;

        // Before DATA, a transport drop is `Unsent`; a server rejection is
        // still `Acknowledged`. The SMTP path applies the same split.
        //
        // Both commands are built before `MAIL FROM` goes out so that a
        // rejected recipient cannot unwind past an open transaction.
        let mail_cmd = Mail::new(from, mail_options).map_err(|e| (e, progress.clone()))?;
        let rcpt_cmds = build_recipient_commands(
            progress.recipients.iter().map(|r| r.address.clone()),
            &rcpt_options_all,
        )
        .map_err(|e| (e, progress.clone()))?;
        match self.command_accepting_status(mail_cmd).await {
            Ok(resp) if resp.is_positive() => {}
            Ok(resp) => {
                self.abort().await;
                return Err((
                    error::status(resp)
                        .with_attempt(SmtpTransmissionState::Acknowledged)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
            Err(e) => {
                self.abort().await;
                return Err((
                    e.with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
        }

        let mut accepted_count = 0usize;
        for (i, rcpt_cmd) in rcpt_cmds.into_iter().enumerate() {
            match self.command_accepting_status(rcpt_cmd).await {
                Ok(resp) if resp.is_positive() => {
                    progress.record_rcpt_accepted(i);
                    accepted_count += 1;
                }
                Ok(resp) => progress.record_rcpt_rejected(i, resp),
                Err(e) => {
                    use crate::transport::smtp::account_error::{
                        SmtpErrorContext, into_account_error,
                    };
                    let ae = into_account_error(
                        e.with_attempt(SmtpTransmissionState::Unsent)
                            .with_phase(SmtpCommandPhase::RcptTo),
                        SmtpErrorContext::send(Protocol::Lmtp).with_phase(SmtpCommandPhase::RcptTo),
                    );
                    progress.mark_unresolved_unsent(|| ae.clone());
                    self.abort().await;
                    return Ok(progress);
                }
            }
        }

        if accepted_count == 0 {
            self.reset_transaction().await;
            return Ok(progress);
        }

        // DATA command. Mirror the non-pipelined SMTP path: a negative DATA-
        // command reply after RCPT acceptances is per-recipient `Failed`,
        // never a batch-level Err. A batch-level Err here would collapse
        // RCPT acceptances and let the engine resend the entire non-
        // idempotent `Send` after the server already rejected it.
        match self.command_accepting_status(Data).await {
            Ok(resp) if resp.is_positive() => {}
            Ok(resp) => {
                progress.mark_accepted_rejected_with_response(resp);
                self.reset_transaction().await;
                return Ok(progress);
            }
            Err(e) => {
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataCommand),
                    SmtpErrorContext::send(Protocol::Lmtp)
                        .with_phase(SmtpCommandPhase::DataCommand),
                );
                let ae2 = ae.clone();
                progress.mark_accepted_uncertain(|| ae2.clone());
                self.abort().await;
                return Ok(progress);
            }
        }

        progress.set_body_started();
        if let Err(e) = self.write_body(email).await {
            use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
            let ae = into_account_error(
                e.with_attempt(SmtpTransmissionState::InFlight)
                    .with_phase(SmtpCommandPhase::DataBody),
                SmtpErrorContext::send(Protocol::Lmtp).with_phase(SmtpCommandPhase::DataBody),
            );
            let ae2 = ae.clone();
            progress.mark_uncertain_unresolved(|| ae2.clone());
            self.abort().await;
            return Ok(progress);
        }
        progress.set_body_finished();

        self.stream.get_ref().state().verify().map_err(|e| {
            (
                e.with_attempt(SmtpTransmissionState::InFlight)
                    .with_phase(SmtpCommandPhase::LmtpFinalStatus),
                progress.clone(),
            )
        })?;
        self.stream.get_mut().set_state(ConnectionState::Broken);
        for i in 0..progress.recipients.len() {
            if !matches!(
                progress.recipients[i].rcpt,
                crate::transport::smtp::batch::RcptProgress::Accepted
            ) {
                continue;
            }
            match self
                .read_response_with_budget_inner(self.per_operation_budget(), true, false)
                .await
            {
                Ok(resp) => {
                    progress.record_lmtp_final(i, resp);
                }
                Err(e) => {
                    use crate::transport::smtp::account_error::{
                        SmtpErrorContext, into_account_error,
                    };
                    let ae = into_account_error(
                        e.with_attempt(SmtpTransmissionState::InFlight)
                            .with_phase(SmtpCommandPhase::LmtpFinalStatus),
                        SmtpErrorContext::send(Protocol::Lmtp)
                            .with_phase(SmtpCommandPhase::LmtpFinalStatus),
                    );
                    let ae2 = ae.clone();
                    progress.mark_uncertain_unresolved(|| ae2.clone());
                    self.abort().await;
                    return Ok(progress);
                }
            }
        }

        // Every recipient outcome is already recorded, so a surplus final
        // status does not change the send result - it only means this stream
        // must never be reused. `finish_lmtp_final_drain` marks it broken and
        // retires it; the batch outcome stands.
        let _surplus = self.finish_lmtp_final_drain();

        Ok(progress)
    }

    /// Write the DATA body without reading the final reply.
    async fn write_body(&mut self, email: &[u8]) -> Result<(), Error> {
        let mut codec = crate::transport::smtp::client::ClientCodec::new();
        let mut out_buf = Vec::with_capacity(email.len());
        codec.encode(email, &mut out_buf);
        self.write(out_buf.as_slice()).await?;
        self.write(data_terminator(email.ends_with(b"\r\n"))).await
    }

    /// Compute RCPT TO parameters for a single recipient and the given options.
    fn rcpt_options_single(
        &self,
        addr: &Address,
        options: &SendOptions,
    ) -> Result<Vec<RcptParameter>, Error> {
        let parameters = options.rcpt_parameters_for(addr);
        for parameter in &parameters {
            self.validate_rcpt_parameter(parameter)?;
        }
        Ok(parameters)
    }

    fn rcpt_options_for_batch(
        &self,
        recipients: &[RecipientProgress],
        options: &SendOptions,
    ) -> Result<Vec<Vec<RcptParameter>>, Error> {
        for (recipient, _) in options.recipient_parameters() {
            if !recipients.iter().any(|batch_recipient| {
                addresses_match_for_recipient_options(recipient, &batch_recipient.address)
            }) {
                return Err(error::invalid_input(
                    "recipient-specific RCPT parameters do not match a batch recipient",
                ));
            }
        }

        recipients
            .iter()
            .map(|recipient| self.rcpt_options_single(&recipient.address, options))
            .collect()
    }

    /// Like `mail_options` but takes an explicit sender address instead of an `Envelope`.
    fn mail_options_for_batch(
        &self,
        from: Option<&Address>,
        recipients: &[RecipientProgress],
        email: &[u8],
        options: &SendOptions,
        allow_binary_mime: bool,
    ) -> Result<Vec<MailParameter>, Error> {
        let mut mail_options = vec![];
        let message_size = if allow_binary_mime {
            email.len()
        } else {
            smtp_data_size(email)
        };

        let has_smtputf8 = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::SmtpUtfEight));
        let has_body_parameter = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::Body(_)));

        let has_non_ascii = from.is_some_and(|a| !AsRef::<str>::as_ref(a).is_ascii())
            || recipients
                .iter()
                .any(|recipient| !AsRef::<str>::as_ref(&recipient.address).is_ascii());
        if has_non_ascii && !has_smtputf8 {
            if !self.server_info().supports_feature(Extension::SmtpUtfEight) {
                return Err(error::invalid_input(
                    "Envelope contains non-ascii chars but server does not support SMTPUTF8",
                ));
            }
            mail_options.push(MailParameter::SmtpUtfEight);
        }

        if !email.is_ascii() && !has_body_parameter {
            if !self.server_info().supports_feature(Extension::EightBitMime) {
                return Err(error::invalid_input(
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
                .is_some_and(|limit| message_size > limit)
            {
                return Err(error::invalid_input(
                    "Message is larger than the server-advertised SIZE limit",
                ));
            }
            mail_options.push(MailParameter::Size(message_size));
        }

        for parameter in options.mail_parameters() {
            self.validate_mail_parameter(
                parameter,
                message_size,
                email.is_ascii(),
                allow_binary_mime,
            )?;
            mail_options.push(parameter.clone());
        }

        Ok(mail_options)
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
        let message_size = if allow_binary_mime {
            email.len()
        } else {
            smtp_data_size(email)
        };

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
                return Err(error::invalid_input(
                    "Envelope contains non-ascii chars but server does not support SMTPUTF8",
                ));
            }
            mail_options.push(MailParameter::SmtpUtfEight);
        }

        // Check for non-ascii content in the message
        if !email.is_ascii() && !has_body_parameter {
            if !self.server_info().supports_feature(Extension::EightBitMime) {
                return Err(error::invalid_input(
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
                .is_some_and(|limit| message_size > limit)
            {
                return Err(error::invalid_input(
                    "Message is larger than the server-advertised SIZE limit",
                ));
            }
            mail_options.push(MailParameter::Size(message_size));
        }

        for parameter in options.mail_parameters() {
            self.validate_mail_parameter(
                parameter,
                message_size,
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
                return Err(error::invalid_input(
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
                    Err(error::invalid_input(
                        "BODY=7BIT cannot be used with non-ASCII message content",
                    ))
                }
            }
            MailParameter::Body(MailBodyParameter::EightBitMime) => {
                if self.server_info().supports_feature(Extension::EightBitMime) {
                    Ok(())
                } else {
                    Err(error::invalid_input(
                        "BODY=8BITMIME requires server 8BITMIME support",
                    ))
                }
            }
            MailParameter::Body(MailBodyParameter::BinaryMime) => {
                if !allow_binary_mime {
                    return Err(error::invalid_input(
                        "BODY=BINARYMIME requires a BDAT send path",
                    ));
                }
                if self.server_info().supports_binary_mime() {
                    Ok(())
                } else {
                    Err(error::invalid_input(
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
                    Err(error::invalid_input(
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
                    Err(error::invalid_input(
                        "SMTPUTF8 requires server SMTPUTF8 support",
                    ))
                }
            }
            MailParameter::RequireTls => {
                if !self.server_info().supports_require_tls() {
                    return Err(error::invalid_input(
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
                    return Err(error::feature_unsupported(
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
                        return Err(error::parameter_over_limit(
                            "HOLDFOR exceeds the server-advertised FUTURERELEASE limit",
                        ));
                    }
                    _ => {}
                }
                Ok(())
            }
            MailParameter::DeliverBy(value) => {
                if !self.server_info().supports_deliver_by() {
                    return Err(error::invalid_input("BY requires server DELIVERBY support"));
                }
                if value.mode() == DeliverByMode::Return
                    && value.seconds() > 0
                    && self
                        .server_info()
                        .deliver_by_minimum()
                        .is_some_and(|minimum| value.seconds() < minimum)
                {
                    return Err(error::invalid_input(
                        "BY return deadline is below the server-advertised DELIVERBY minimum",
                    ));
                }
                Ok(())
            }
            MailParameter::MtPriority(_) => {
                if self.server_info().supports_mt_priority() {
                    Ok(())
                } else {
                    Err(error::invalid_input(
                        "MT-PRIORITY requires server MT-PRIORITY support",
                    ))
                }
            }
            MailParameter::DsnReturn(_) | MailParameter::EnvelopeId(_) => {
                if self.server_info().supports_dsn() {
                    Ok(())
                } else {
                    Err(error::invalid_input(
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
                    Err(error::invalid_input(
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
    pub(crate) async fn starttls(
        &mut self,
        tls_parameters: TlsParameters,
        hello_name: &ClientId,
    ) -> Result<(), Error> {
        if self.server_info.supports_feature(Extension::StartTls) {
            try_smtp!(self.command(Starttls).await, self);
            // Belt and braces at the security boundary. The generic
            // surplus-bytes check inside the reply read normally fires first,
            // but `get_mut()` swaps only the INNER stream and leaves the
            // `BufReader` buffer intact, so any pre-TLS byte still sitting here
            // would be consumed as the first bytes of the TLS session - the
            // classic STARTTLS response-injection. This gate is independent of
            // whichever reply path produced the 220.
            if !self.stream.buffer().is_empty() {
                self.stream.get_mut().set_state(ConnectionState::Broken);
                self.abort().await;
                return Err(error::parse(
                    "SMTP server sent an unsolicited reply before the STARTTLS upgrade",
                ));
            }
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
            Err(error::invalid_input(
                "STARTTLS is not supported on this server",
            ))
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
            Protocol::Lmtp => {
                let command = Lhlo::new(hello_name.clone())?;
                try_smtp!(self.command_with_budget(command, budget).await, self)
            }
            _ => {
                let command = Ehlo::new(hello_name.clone())?;
                try_smtp!(self.command_with_budget(command, budget).await, self)
            }
        };
        self.server_info = try_smtp!(ServerInfo::from_response(&response), self);
        Ok(())
    }

    /// Close the current mail transaction, or make the connection
    /// unrecyclable if the server does not positively acknowledge the reset.
    async fn reset_transaction(&mut self) {
        match self.command_accepting_status(Rset).await {
            Ok(response) if response.is_positive() => {}
            Ok(_) | Err(_) => self.abort().await,
        }
    }

    pub(crate) async fn abort(&mut self) {
        self.stream.get_mut().set_state(ConnectionState::Broken);
        let _ = with_timeout(
            self.per_operation_budget(),
            "SMTP shutdown timed out",
            self.stream.shutdown(),
        )
        .await;
    }

    /// Tells if the underlying stream is currently encrypted
    pub(crate) fn is_encrypted(&self) -> bool {
        self.stream.get_ref().is_encrypted()
    }

    /// DER of the peer (server) certificate. See
    /// [`AsyncNetworkStream::peer_certificate_der`] for the contract.
    ///
    /// Consumed by `auth`'s `resolve_scram_binding` for SCRAM-PLUS channel
    /// binding.
    pub(crate) fn peer_certificate_der(&self) -> Option<Vec<u8>> {
        self.stream.get_ref().peer_certificate_der()
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

    /// Sends an AUTH command, selecting the strongest compatible mechanism.
    ///
    /// Awaited twin of `SmtpConnection::auth`: same SCRAM-aware order, same
    /// RFC 5802 Section 6 downgrade protection, same binding-skip fall-through.
    pub(crate) async fn auth(
        &mut self,
        mechanisms: &[Mechanism],
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        // OAuth credentials never use SCRAM: select the first advertised
        // mechanism from the caller's order and run the legacy encoder path.
        if let Some(mechanism) = oauth_mechanism(mechanisms, &self.server_info, credentials) {
            return self.auth_legacy(mechanism, credentials).await;
        }

        let plus_candidate = mechanisms.iter().any(|mechanism| {
            matches!(
                mechanism,
                Mechanism::ScramSha1Plus | Mechanism::ScramSha256Plus
            ) && self.server_info.supports_auth_mechanism(*mechanism)
        });
        let binding = if plus_candidate {
            self.resolve_scram_binding()?
        } else {
            None
        };
        let chosen = password_mechanism(mechanisms, &self.server_info, binding.is_some())?;
        match chosen {
            Mechanism::ScramSha1Plus | Mechanism::ScramSha256Plus => {
                let binding = binding.expect("PLUS binding resolved above");
                self.auth_scram(chosen, binding, credentials).await
            }
            Mechanism::ScramSha1 | Mechanism::ScramSha256 => {
                self.auth_scram(chosen, ScramChannelBinding::None, credentials)
                    .await
            }
            _ => self.auth_legacy(chosen, credentials).await,
        }
    }

    /// Resolve the `tls-server-end-point` channel binding for the live
    /// connection. The DER accessor is sync (cached on the TLS stream), so this
    /// is non-awaiting and the binding-skip decision stays network-free.
    fn resolve_scram_binding(&self) -> Result<Option<ScramChannelBinding>, Error> {
        let der = self.peer_certificate_der();
        resolve_scram_binding(der.as_deref())
    }

    /// Run a SCRAM exchange (bound or unbound) as a no-IR `334` challenge walk.
    async fn auth_scram(
        &mut self,
        mechanism: Mechanism,
        binding: ScramChannelBinding,
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        let hash = scram_hash(mechanism).expect("auth_scram only called for SCRAM mechanisms");
        let (username, password) = credentials.password_parts()?;
        let mut exchange = ScramExchange::new(hash, binding, username, password.into())?;

        let auth = Auth::new(mechanism, credentials.clone(), None)?;
        let response = try_smtp!(self.command(auth).await, self, SmtpCommandPhase::Auth);
        if !response.has_code(334) {
            self.abort().await;
            return Err(error::status(response).with_phase(SmtpCommandPhase::Auth));
        }
        let response = try_smtp!(
            self.write_auth_continuation(&exchange.client_first()).await,
            self,
            SmtpCommandPhase::Auth
        );

        // A malformed continuation is a protocol-class parse error (not an
        // auth failure), so it is not Auth-phase tagged, but it must still
        // abort the connection like every other error path in this exchange.
        let server_first = try_smtp!(decode_auth_challenge(&response), self);
        let response = match try_smtp!(exchange.step(&server_first), self, SmtpCommandPhase::Auth) {
            ScramStep::Reply(client_final) => try_smtp!(
                self.write_auth_continuation(&client_final).await,
                self,
                SmtpCommandPhase::Auth
            ),
            ScramStep::Complete => {
                self.abort().await;
                return Err(error::parse("SCRAM completed before server-final"));
            }
        };

        // Server-final reply. RFC 4954 carries it on a `334` continuation, with
        // the tagged `235` success following an empty client line. Some servers
        // (field-observed) fold the server-final straight onto the `235`
        // success reply (`235 v=...`). Both shapes are accepted; anything else
        // is an auth failure (or protocol error) and aborts. A `334` here must
        // not be mistaken for success - only a positive reply ends the walk.
        let success = if response.has_code(334) {
            let server_final = try_smtp!(decode_auth_challenge(&response), self);
            try_smtp!(
                Self::verify_scram_server_final(&mut exchange, &server_final),
                self,
                SmtpCommandPhase::Auth
            );
            let final_reply = try_smtp!(
                self.write_auth_continuation("").await,
                self,
                SmtpCommandPhase::Auth
            );
            if !final_reply.is_positive() {
                self.abort().await;
                return Err(error::status(final_reply).with_phase(SmtpCommandPhase::Auth));
            }
            final_reply
        } else if response.is_positive() {
            let server_final = try_smtp!(decode_scram_payload(&response), self);
            try_smtp!(
                Self::verify_scram_server_final(&mut exchange, &server_final),
                self,
                SmtpCommandPhase::Auth
            );
            response
        } else {
            self.abort().await;
            return Err(error::status(response).with_phase(SmtpCommandPhase::Auth));
        };

        let hello_name = self.hello_name.clone();
        try_smtp!(self.hello(&hello_name).await, self);
        Ok(success)
    }

    /// Verify a decoded SCRAM server-final, mapping a non-terminal step to a
    /// parse-class error. Shared by the `334`-continuation and `235`-folded
    /// server-final shapes.
    fn verify_scram_server_final(
        exchange: &mut ScramExchange,
        server_final: &str,
    ) -> Result<(), Error> {
        match exchange.step(server_final)? {
            ScramStep::Complete => Ok(()),
            ScramStep::Reply(_) => Err(error::parse("unexpected SCRAM reply after server-final")),
        }
    }

    /// Write a base64 SASL continuation line (already encoded) and read the
    /// reply. An empty `line` emits a bare `\r\n`.
    async fn write_auth_continuation(&mut self, line: &str) -> Result<Response, Error> {
        let framed = Zeroizing::new(format!("{line}\r\n"));
        self.write(framed.as_bytes()).await?;
        self.read_response().await
    }

    /// The legacy stateless `Auth::new` / `Auth::new_from_response` 334 loop,
    /// for PLAIN / LOGIN / XOAUTH2 / OAUTHBEARER.
    async fn auth_legacy(
        &mut self,
        mechanism: Mechanism,
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        // Resolve the OAuth access token from the shared source once, up
        // front, so the same token frames the initial response and any
        // OAUTHBEARER error-continuation round. Awaiting `current()` here
        // is the single rotation read point; a token refreshed on the
        // source is presented on this (re)connect.
        let oauth_token = if matches!(mechanism, Mechanism::Xoauth2 | Mechanism::OAuthBearer) {
            Some(try_smtp!(
                credentials.oauth2_token().await,
                self,
                SmtpCommandPhase::Auth
            ))
        } else {
            None
        };
        let oauth_token = oauth_token.as_ref().map(|(_, token)| token.as_str());

        // Limit challenges to avoid blocking
        let mut challenges: u8 = 10;
        // Position of the challenge within the exchange. LOGIN answers by
        // position (username, then password) rather than by prompt text, so
        // this counter is the only thing that selects which credential goes
        // out; it must not be derived from the remaining-challenge budget.
        let mut challenge_index: usize = 0;
        let auth = Auth::new(mechanism, credentials.clone(), oauth_token)?;
        let mut response = try_smtp!(self.command(auth).await, self, SmtpCommandPhase::Auth);

        while challenges > 0 && response.has_code(334) {
            challenges -= 1;
            // Build the continuation reply. For XOAUTH2/OAUTHBEARER a 334 here
            // is the server's base64 failure detail, and the encoder returns
            // the dummy-cancel line so the next read surfaces the tagged
            // negative reply on the Auth lane. Any construction error (bad
            // base64, an encoder rejecting an unexpected challenge) must abort
            // and carry the Auth phase, not leak through a bare `?` as an
            // untagged parse error.
            let continuation = try_smtp!(
                Auth::new_from_response_at_index(
                    mechanism,
                    credentials.clone(),
                    &response,
                    challenge_index,
                    oauth_token,
                ),
                self,
                SmtpCommandPhase::Auth
            );
            challenge_index += 1;
            response = try_smtp!(
                self.command(continuation).await,
                self,
                SmtpCommandPhase::Auth
            );
        }

        if challenges == 0 {
            // The server never completed (or rejected) the exchange within the
            // round-trip budget: an auth-exchange failure, routed on the Auth
            // lane (InvalidInput + Auth -> Authorization), not an untagged
            // Protocol(ParseFailed).
            self.abort().await;
            Err(error::invalid_input("Unexpected number of challenges")
                .with_phase(SmtpCommandPhase::Auth))
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
        self.write_command(Bdat::last(message.len())).await?;
        self.write(message).await?;
        self.read_response().await
    }

    pub(crate) async fn message_lmtp_bdat(
        &mut self,
        message: &[u8],
        recipients: usize,
    ) -> Result<Vec<Response>, Error> {
        self.write_command(Bdat::last(message.len())).await?;
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

        self.finish_lmtp_final_drain()?;

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
        let mut last_two = [0_u8; 2];
        let mut seen = 0_usize;
        for message_part in message {
            let message_part = message_part.as_ref();
            if message_part.len() >= 2 {
                last_two.copy_from_slice(&message_part[message_part.len() - 2..]);
                seen = 2;
            } else if let Some(&byte) = message_part.first() {
                last_two[0] = last_two[1];
                last_two[1] = byte;
                seen = (seen + 1).min(2);
            }
            let mut out_buf = Vec::with_capacity(message_part.len());
            codec.encode(message_part, &mut out_buf);
            self.write(out_buf.as_slice()).await?;
        }
        self.write(data_terminator(seen >= 2 && last_two == *b"\r\n"))
            .await?;

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
        let mut last_two = [0_u8; 2];
        let mut seen = 0_usize;
        for message_part in message {
            let message_part = message_part.as_ref();
            if message_part.len() >= 2 {
                last_two.copy_from_slice(&message_part[message_part.len() - 2..]);
                seen = 2;
            } else if let Some(&byte) = message_part.first() {
                last_two[0] = last_two[1];
                last_two[1] = byte;
                seen = (seen + 1).min(2);
            }
            let mut out_buf = Vec::with_capacity(message_part.len());
            codec.encode(message_part, &mut out_buf);
            self.write(out_buf.as_slice()).await?;
        }
        self.write(data_terminator(seen >= 2 && last_two == *b"\r\n"))
            .await?;

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

        self.finish_lmtp_final_drain()?;

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
        self.write_command_with_budget(command, budget).await?;
        self.read_response_with_budget(budget).await
    }

    async fn command_accepting_status<C: Display>(
        &mut self,
        command: C,
    ) -> Result<Response, Error> {
        self.write_command(command).await?;
        self.read_response_accepting_status().await
    }

    async fn write_command<C: Display>(&mut self, command: C) -> Result<(), Error> {
        self.write_command_with_budget(command, self.per_operation_budget())
            .await
    }

    async fn write_command_with_budget<C: Display>(
        &mut self,
        command: C,
        budget: TimeoutBudget,
    ) -> Result<(), Error> {
        self.command_buffer.zeroize();
        write!(&mut self.command_buffer, "{command}")
            .map_err(|_| error::internal("failed to serialize SMTP command"))?;
        let result = Self::write_stream_with_budget(
            &mut self.stream,
            self.command_buffer.as_bytes(),
            budget,
        )
        .await;
        self.command_buffer.zeroize();
        result
    }

    /// Closes out an LMTP final-status drain.
    ///
    /// A surplus final status that already reached the read buffer is a
    /// protocol violation we can prove: the stream is marked `Broken` and the
    /// caller gets an error. Bytes still below the buffer (in the socket or
    /// TLS record layer) cannot be observed without a read that would block on
    /// a well-behaved peer, so cleanliness is never positively established:
    /// every LMTP drain retires the connection instead of returning it to the
    /// pool. LMTP is local delivery, so a reconnect is cheap next to reusing a
    /// desynchronized stream.
    fn finish_lmtp_final_drain(&mut self) -> Result<(), Error> {
        self.retire = true;

        if self.stream.buffer().is_empty() {
            return Ok(());
        }

        self.stream.get_mut().set_state(ConnectionState::Broken);
        Err(error::parse(
            "LMTP server returned more final statuses than accepted recipients",
        ))
    }

    /// Whether this connection must not be returned to the pool.
    pub(crate) fn should_retire(&self) -> bool {
        self.retire
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
        Self::write_stream_with_budget(&mut self.stream, string, budget).await
    }

    async fn write_stream_with_budget(
        stream: &mut BufReader<AsyncNetworkStream>,
        string: &[u8],
        budget: TimeoutBudget,
    ) -> Result<(), Error> {
        stream.get_ref().state().verify()?;
        stream.get_mut().set_state(ConnectionState::Broken);

        with_timeout(
            budget,
            "SMTP write timed out",
            stream.get_mut().write_all(string),
        )
        .await?
        .map_err(error::network)?;
        with_timeout(budget, "SMTP flush timed out", stream.get_mut().flush())
            .await?
            .map_err(error::network)?;
        stream.get_mut().set_state(ConnectionState::Ok);

        #[cfg(feature = "tracing")]
        tracing::debug!("Wrote {} bytes", string.len());
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

        loop {
            let mut line = Vec::with_capacity(100);
            let bytes_read = {
                let mut limited = (&mut self.stream).take((MAX_RESPONSE_LINE_BYTES + 1) as u64);
                with_timeout(
                    budget,
                    "SMTP read timed out",
                    limited.read_until(b'\n', &mut line),
                )
                .await?
                .map_err(error::network)?
            };
            if bytes_read == 0 {
                break;
            }
            if line.len() > MAX_RESPONSE_LINE_BYTES {
                return Err(error::parse("SMTP response line too long"));
            }
            if buffer.len() + line.len() > MAX_RESPONSE_BYTES {
                return Err(error::parse("SMTP response too large"));
            }
            let line = std::str::from_utf8(&line)
                .map_err(|_| error::parse("SMTP response is not valid UTF-8"))?;
            buffer.push_str(line);

            #[cfg(feature = "tracing")]
            tracing::debug!("<< {}", escape_crlf(line));
            match parse_response(&buffer) {
                Ok((_remaining, response)) => {
                    if manage_state && !self.stream.buffer().is_empty() {
                        return Err(error::parse("SMTP server sent an unsolicited reply"));
                    }
                    if manage_state {
                        self.stream.get_mut().set_state(ConnectionState::Ok);
                    }

                    return if accept_negative || response.is_positive() {
                        Ok(response)
                    } else {
                        Err(error::status(response))
                    };
                }
                Err(nom::Err::Failure(e)) => {
                    return Err(error::parse(e.to_string()));
                }
                Err(nom::Err::Incomplete(_)) => { /* read more */ }
                Err(nom::Err::Error(e)) => {
                    return Err(error::parse(e.to_string()));
                }
            }
        }

        Err(error::parse("incomplete response"))
    }

    fn finish_reply_group(&mut self) -> Result<(), Error> {
        if !self.stream.buffer().is_empty() {
            return Err(error::parse("SMTP server sent an unsolicited reply"));
        }
        self.stream.get_mut().set_state(ConnectionState::Ok);
        Ok(())
    }
}

#[cfg(all(test, feature = "tokio"))]
mod transcript_tests {
    use std::time::Duration;

    use crate::{
        address::Envelope,
        transport::smtp::{
            Protocol,
            authentication::{Credentials, Mechanism},
            batch::SmtpBatchRecipient,
            commands::Noop,
            extension::{
                ClientId, DeliverByMode, DsnNotify, DsnReturn, Extension, MailBodyParameter,
                MailParameter,
            },
            test_support::Transcript,
        },
    };
    use bifrost_types::error::BatchItemId;

    use super::{AsyncSmtpConnection, SendOptions};

    const HELLO: &str = "EHLO client.example\r\n";

    #[tokio::test(crate = "tokio")]
    async fn all_recipient_rejection_resets_every_direct_and_batch_transaction() {
        let hello = ClientId::Domain("client.example".to_owned());
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();

        for chunking in [false, true] {
            let greeting = if chunking {
                "250-lmtp.example\r\n250 CHUNKING\r\n"
            } else {
                "250 lmtp.example\r\n"
            };
            let transcript = Transcript::new("220 lmtp.example\r\n")
                .expect("LHLO client.example\r\n", greeting)
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect(
                    "RCPT TO:<recipient@example.com>\r\n",
                    "550 recipient rejected\r\n",
                )
                .expect("RSET\r\n", "250 reset ok\r\n");
            let mut connection =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp)
                    .await
                    .unwrap();
            let statuses = if chunking {
                connection
                    .send_lmtp_bdat_with_options(&envelope, b"body", &Default::default())
                    .await
                    .unwrap()
            } else {
                connection.send_lmtp(&envelope, b"body").await.unwrap()
            };
            assert_eq!(statuses.len(), 1);
            assert!(!statuses[0].is_positive());
            assert!(!connection.has_broken());
            transcript.assert_exhausted();
        }

        for protocol in [Protocol::Smtp, Protocol::Lmtp] {
            let hello_command = if protocol == Protocol::Smtp {
                "EHLO client.example\r\n"
            } else {
                "LHLO client.example\r\n"
            };
            let transcript = Transcript::new("220 server.example\r\n")
                .expect(hello_command, "250 server.example\r\n")
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect(
                    "RCPT TO:<recipient@example.com>\r\n",
                    "550 recipient rejected\r\n",
                )
                .expect("RSET\r\n", "250 reset ok\r\n");
            let batch = vec![SmtpBatchRecipient {
                id: BatchItemId("item-0".to_owned()),
                address: "recipient@example.com".parse().unwrap(),
            }];
            let mut connection =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, protocol)
                    .await
                    .unwrap();
            let progress = if protocol == Protocol::Smtp {
                connection
                    .send_smtp_batch(
                        Some("sender@example.com".parse().unwrap()),
                        batch,
                        b"body",
                        &Default::default(),
                    )
                    .await
            } else {
                connection
                    .send_lmtp_batch(
                        Some("sender@example.com".parse().unwrap()),
                        batch,
                        b"body",
                        &Default::default(),
                    )
                    .await
            }
            .unwrap();
            assert_eq!(progress.resolve().failed().len(), 1);
            assert!(!connection.has_broken());
            transcript.assert_exhausted();
        }
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelined_server_rejections_carry_their_command_phase() {
        let hello = ClientId::Domain("client.example".to_owned());
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let window = "MAIL FROM:<sender@example.com>\r\nRCPT TO:<recipient@example.com>\r\n";

        // MAIL FROM rejected: no transaction was opened, so no RSET.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "550 sender rejected\r\n250 recipient ok\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::MailFrom));

        // RCPT TO rejected.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "250 sender ok\r\n550 recipient rejected\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::RcptTo));

        // DATA rejected before the body.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "250 sender ok\r\n250 recipient ok\r\n")
            .expect("DATA\r\n", "554 no data\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::DataCommand));
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelined_body_failure_carries_data_body_phase() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(
                "MAIL FROM:<sender@example.com>\r\nRCPT TO:<recipient@example.com>\r\n",
                "250 sender ok\r\n250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect_then_close("body", "");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection.send(&envelope, b"body").await.unwrap_err();

        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::DataBody));
    }

    #[tokio::test(crate = "tokio")]
    async fn data_terminator_preserves_exact_message_bytes() {
        let hello = ClientId::Domain("client.example".to_owned());
        let cases: &[(&[u8], &[u8])] = &[
            (b"body\r\n", b".\r\n"),
            (b"body", b"\r\n.\r\n"),
            (b"", b"\r\n.\r\n"),
            (b"body\r", b"\r\n.\r\n"),
            (b"body\n", b"\r\n.\r\n"),
        ];

        for &(body, terminator) in cases {
            let mut transcript =
                Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
            if !body.is_empty() {
                transcript = transcript.expect(body, "");
            }
            transcript = transcript.expect(terminator, "250 queued\r\n");
            let mut connection =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                    .await
                    .unwrap();

            connection.message(body).await.unwrap();
            transcript.assert_exhausted();
        }

        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("body\r", "")
            .expect("\n", "")
            .expect(".\r\n", "250 queued\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        connection
            .message_iter([b"body\r".as_slice(), b"\n".as_slice()].into_iter())
            .await
            .unwrap();
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelining_drains_each_recipient_window_before_writing_the_next() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients: Vec<crate::address::Address> = (0..33)
            .map(|index| format!("recipient-{index}@example.com").parse().unwrap())
            .collect();
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.extend(
            recipients[..32]
                .iter()
                .map(|recipient| format!("RCPT TO:<{recipient}>\r\n")),
        );
        let mut first_replies = "250 sender ok\r\n".to_owned();
        first_replies.push_str(&"250 recipient ok\r\n".repeat(32));
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                "EHLO client.example\r\n",
                "250-smtp.example\r\n250 PIPELINING\r\n",
            )
            .expect(first_window, first_replies)
            .expect(
                format!("RCPT TO:<{}>\r\n", recipients[32]),
                "250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "250 queued\r\n");
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        connection.send(&envelope, b"body").await.unwrap();
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelined_rcpt_reply_read_failure_reports_unsent_not_uncertain() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses: Vec<crate::address::Address> = (0..32)
            .map(|index| format!("recipient-{index}@example.com").parse().unwrap())
            .collect();
        let mut window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        window.extend(
            addresses
                .iter()
                .map(|recipient| format!("RCPT TO:<{recipient}>\r\n")),
        );
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(
                window,
                "250 sender ok\r\n250 first accepted\r\n550 second rejected\r\n",
            );
        let batch = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert!(outcome.uncertain().is_empty());
        assert_eq!(outcome.failed().len(), 32);
        assert_eq!(outcome.failed()[1].item.0, "item-1");
    }

    #[tokio::test(crate = "tokio")]
    async fn later_pipelining_window_write_failure_is_unsent() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses: Vec<crate::address::Address> = (0..33)
            .map(|index| format!("recipient-{index}@example.com").parse().unwrap())
            .collect();
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.extend(
            addresses[..32]
                .iter()
                .map(|recipient| format!("RCPT TO:<{recipient}>\r\n")),
        );
        let mut replies = "250 sender ok\r\n".to_owned();
        replies.push_str(&"250 recipient ok\r\n".repeat(31));
        replies.push_str("550 recipient rejected\r\n");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(first_window, replies);
        let batch = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert!(outcome.uncertain().is_empty());
        assert!(outcome.succeeded().is_empty());
        assert_eq!(outcome.failed().len(), 33);
        assert_eq!(outcome.failed()[31].item.0, "item-31");
    }

    #[tokio::test(crate = "tokio")]
    async fn starttls_downgrade_is_refused_without_a_wire_command() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect("EHLO client.example\r\n", "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        assert!(!connection.can_starttls());
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(tls, &hello)
            .await
            .expect_err("a server without the STARTTLS capability must not be upgraded");
        assert!(
            error.to_string().contains("STARTTLS is not supported"),
            "expected a capability refusal, got: {error}"
        );

        // The refusal happened before any byte hit the wire: the very next
        // scripted step is NOOP, so a stray STARTTLS write would be rejected.
        connection.command(Noop).await.unwrap();
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn starttls_refuses_plaintext_bytes_buffered_after_the_reply() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect_coalesced(
                "STARTTLS\r\n",
                "220 go ahead\r\n250 attacker-controlled capabilities\r\n",
            );
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();

        let error = connection
            .starttls(tls, &hello)
            .await
            .expect_err("pre-TLS injected bytes must refuse the upgrade");

        assert!(error.to_string().contains("unsolicited reply"));
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn advertised_starttls_writes_command_before_upgrade() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "220 ready to start tls\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();

        let error = connection.starttls(tls, &hello).await.unwrap_err();

        assert!(error.to_string().contains("only supported on TCP"));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn refused_starttls_reply_does_not_upgrade() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "454 TLS temporarily unavailable\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();

        let error = connection.starttls(tls, &hello).await.unwrap_err();

        assert!(
            error
                .smtp_response()
                .is_some_and(|response| response.has_code(454))
        );
        assert!(!connection.is_encrypted());
        transcript.assert_exhausted();
    }

    // The tests below replace the socket-listener tests this harness retired.
    // Same behaviors, same assertions, no listener or thread.

    #[tokio::test(crate = "tokio")]
    async fn abort_closes_without_quit_command() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        connection.abort().await;
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn peer_certificate_der_is_none_on_plaintext() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let connection = AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
            .await
            .unwrap();

        assert!(
            connection.peer_certificate_der().is_none(),
            "plaintext connection must have no peer certificate DER"
        );
    }

    #[tokio::test(crate = "tokio")]
    async fn failed_test_connected_marks_connection_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        assert!(!connection.test_connected().await);
        assert!(connection.has_broken());
    }

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn abort_is_bounded_when_tls_style_shutdown_never_completes() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .stall_shutdown();
        let mut connection = AsyncSmtpConnection::from_transcript_with_timeout(
            transcript,
            &hello,
            Protocol::Smtp,
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        let mut abort = Box::pin(connection.abort());
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(abort.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::timeout(Duration::ZERO, abort)
            .await
            .expect("abort must finish when its operation timeout expires");
    }

    #[tokio::test(crate = "tokio")]
    async fn explicit_mail_parameters_are_not_duplicated() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250-PIPELINING\r\n250-SIZE 1024\r\n250-SMTPUTF8\r\n250 8BITMIME\r\n",
            )
            .expect(
                "MAIL FROM:<sender@example.com> SIZE=25 SMTPUTF8 BODY=8BITMIME\r\nRCPT TO:<recipient@exämple.com>\r\n",
                "250 sender ok\r\n250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("Subject: test\r\n\r\nHéllo", "")
            .expect("\r\n.\r\n", "250 queued\r\n");
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

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let response = connection
            .send_with_options(&envelope, "Subject: test\r\n\r\nHéllo".as_bytes(), &options)
            .await
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn batch_eai_recipient_requires_smtputf8_before_mail_from() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: crate::address::Address::new_dangerous("üser", "example.com"),
        }];
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let (error, _) = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .await
            .expect_err("EAI recipient must be rejected before MAIL FROM");

        assert!(error.to_string().contains("SMTPUTF8"));
        assert!(connection.test_connected().await);
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn bdat_is_length_framed_without_dot_stuffing() {
        let hello = ClientId::Domain("client.example".to_owned());
        let message = b"Subject: test\r\n\r\n.Line\r\nBinary\0";
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250-CHUNKING\r\n250 BINARYMIME\r\n",
            )
            .expect(
                "MAIL FROM:<sender@example.com> BODY=BINARYMIME\r\n",
                "250 sender ok\r\n",
            )
            .expect(
                "RCPT TO:<recipient@example.com>\r\n",
                "250 recipient ok\r\n",
            )
            .expect(format!("BDAT {} LAST\r\n", message.len()), "")
            .expect(message, "250 queued\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let options =
            SendOptions::new().mail_parameter(MailParameter::Body(MailBodyParameter::BinaryMime));
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .send_bdat_with_options(&envelope, message, &options)
            .await
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn unsupported_dsn_is_refused_before_mail_from() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["first@example.com".parse().unwrap()],
        )
        .unwrap();
        let options = SendOptions::new().notify([DsnNotify::Failure]).unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection
            .send_with_options(&envelope, b"body", &options)
            .await
            .expect_err("DSN must be refused locally when it was not advertised");
        assert!(error.to_string().contains("require server DSN support"));
        assert!(connection.test_connected().await);
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn hostile_unchecked_sender_never_reaches_the_wire() {
        // `is_err()` alone would hold even without the guard, because the
        // transcript has no expectation for the smuggled `MAIL FROM` and would
        // fail the write instead. The load-bearing assertions are that nothing
        // was written and the connection is still clean.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 ok\r\n");
        let envelope = Envelope::new(
            Some(crate::address::Address::new_dangerous(
                "sender\r\nRSET",
                "example.com",
            )),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .await
                .is_err()
        );
        assert!(
            !connection.has_broken(),
            "a rejected sender must not break the connection"
        );
        assert!(
            connection.test_connected().await,
            "the connection stays reusable"
        );
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn hostile_unchecked_recipient_leaves_no_open_transaction() {
        // The sender is well-formed, so validation cannot short-circuit before
        // `MAIL FROM` for the reason the sender test covers. What must hold is
        // that the recipient is rejected while the connection is still clean:
        // no `MAIL FROM` on the wire, no abort needed, and the connection is
        // still usable afterwards rather than being pooled mid-transaction.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 ok\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                "good@example.com".parse().unwrap(),
                crate::address::Address::new_dangerous("hostile\r\nRSET", "example.com"),
            ],
        )
        .unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .await
                .is_err()
        );
        assert!(
            !connection.has_broken(),
            "a rejection before MAIL FROM must not break the connection"
        );
        assert!(
            connection.test_connected().await,
            "the connection must still be reusable, not stranded in a transaction"
        );
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelined_send_rsets_when_a_recipient_is_rejected() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250-PIPELINING\r\n250 SIZE 1024\r\n",
            )
            .expect(
                "MAIL FROM:<sender@example.com> SIZE=24\r\nRCPT TO:<recipient@example.com>\r\n",
                "250 sender ok\r\n550 recipient rejected\r\n",
            )
            .expect("RSET\r\n", "250 reset ok\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .await
                .is_err()
        );
        assert!(!connection.has_broken());
        assert!(connection.test_connected().await);
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn auth_refreshes_server_info_with_ehlo() {
        let hello = ClientId::Domain("client.example".to_owned());
        let plain = format!(
            "AUTH PLAIN {}\r\n",
            crate::base64::encode("\u{0}user\u{0}pass")
        );
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250-AUTH PLAIN\r\n250 SIZE 100\r\n",
            )
            .expect(plain, "235 authenticated\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 8BITMIME\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
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
        transcript.assert_exhausted();
    }

    /// A minimal but structurally valid DER `Certificate` whose
    /// `signatureAlgorithm` is sha256WithRSAEncryption, matching the fixture
    /// shape bifrost-sasl's own channel-binding tests use. The parser only
    /// walks to the signatureAlgorithm OID, so this is a usable certificate
    /// for `resolve_scram_binding`.
    fn usable_peer_certificate() -> Vec<u8> {
        vec![
            0x30, 0x12, // Certificate SEQUENCE
            0x30, 0x00, // tbsCertificate: empty SEQUENCE
            0x30, 0x0b, // signatureAlgorithm SEQUENCE
            0x06, 0x09, // OID, 9 bytes
            0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01,
            0x0b, // 1.2.840.113549.1.1.11 sha256WithRSAEncryption
            0x03, 0x01, 0x00, // signatureValue: empty BIT STRING
        ]
    }

    #[tokio::test(crate = "tokio")]
    async fn plus_advertising_server_with_certificate_is_answered_with_scram_plus() {
        // Pins the connection-level `plus_candidate` gate in the true
        // direction: with a usable peer certificate and SCRAM-SHA-256-PLUS
        // advertised, the AUTH command on the wire must be the PLUS mechanism,
        // never PLAIN. The SCRAM client-first is nonce-random and cannot be
        // scripted, so the exchange is cut short with a 535 on the AUTH
        // command itself; exhausting the transcript proves the PLUS command
        // was written.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250 AUTH SCRAM-SHA-256-PLUS PLAIN\r\n",
            )
            .expect("AUTH SCRAM-SHA-256-PLUS\r\n", "535 rejected\r\n");
        let mut connection = AsyncSmtpConnection::from_transcript_with_peer_certificate(
            transcript.clone(),
            &hello,
            Protocol::Smtp,
            usable_peer_certificate(),
        )
        .await
        .unwrap();

        let error = connection
            .auth(
                &[Mechanism::ScramSha256Plus, Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap_err();

        assert!(
            error.is_permanent(),
            "expected the scripted 535, got {error:?}"
        );
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn present_but_unusable_certificate_fails_auth_before_any_wire_write() {
        // A peer certificate that exists but cannot produce a channel binding
        // (truncated DER) must be a hard parse-class error, not a silent
        // downgrade to PLAIN-over-TLS. The transcript scripts no AUTH step, so
        // any attempt to write a fallback AUTH command would fail the
        // transcript instead of returning the typed error asserted here.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n").expect(
            HELLO,
            "250-smtp.example\r\n250 AUTH SCRAM-SHA-256-PLUS PLAIN\r\n",
        );
        let mut connection = AsyncSmtpConnection::from_transcript_with_peer_certificate(
            transcript.clone(),
            &hello,
            Protocol::Smtp,
            vec![0x30, 0x01, 0x00],
        )
        .await
        .unwrap();

        let error = connection
            .auth(
                &[Mechanism::ScramSha256Plus, Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap_err();

        assert!(
            error.is_parse(),
            "expected the typed binding parse error, got {error:?}"
        );
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn plus_advertised_without_certificate_falls_through_to_plain() {
        // No certificate at all (plaintext transcript, nothing injected):
        // the PLUS rung is skipped as binding-unavailable and PLAIN is the
        // answer. This is the only fall-through `resolve_scram_binding`
        // permits.
        let hello = ClientId::Domain("client.example".to_owned());
        let plain = format!(
            "AUTH PLAIN {}\r\n",
            crate::base64::encode("\u{0}user\u{0}pass")
        );
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250 AUTH SCRAM-SHA-256-PLUS PLAIN\r\n",
            )
            .expect(plain, "235 authenticated\r\n")
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .auth(
                &[Mechanism::ScramSha256Plus, Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap();

        assert!(response.has_code(235));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn login_answers_challenges_by_position_not_by_prompt_text() {
        // Both prompts are deliberately outside the English wordlist the driver
        // used to match on. A prompt-matching implementation rejects the
        // exchange with "Unrecognized challenge"; a positional one answers
        // username then password regardless of wording.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH LOGIN\r\n")
            .expect("AUTH LOGIN\r\n", {
                let prompt = crate::base64::encode("Nom d'utilisateur :");
                format!("334 {prompt}\r\n")
            })
            .expect(format!("{}\r\n", crate::base64::encode("user")), {
                let prompt = crate::base64::encode("Mot de passe :");
                format!("334 {prompt}\r\n")
            })
            .expect(
                format!("{}\r\n", crate::base64::encode("pass")),
                "235 authenticated\r\n",
            )
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .auth(
                &[Mechanism::Login],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap();

        assert!(response.has_code(235));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn oauthbearer_auth_sends_initial_response_and_refreshes_ehlo() {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=us=2Cer=3Done,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(
                format!("AUTH OAUTHBEARER {initial}\r\n"),
                "235 authenticated\r\n",
            )
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
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
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn oauthbearer_immediate_rejection_marks_connection_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=user,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(
                format!("AUTH OAUTHBEARER {initial}\r\n"),
                "535 rejected\r\n",
            );
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
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
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn oauthbearer_failed_challenge_sends_cancel_response() {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=user,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(format!("AUTH OAUTHBEARER {initial}\r\n"), "334 e30=\r\n")
            .expect("AQ==\r\n", "535 rejected\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
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
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn connect_times_out_waiting_for_banner() {
        let hello = ClientId::Domain("client.example".to_owned());
        // The peer accepts and then says nothing at all.
        let Err(error) = AsyncSmtpConnection::from_transcript_with_timeout(
            Transcript::silent(),
            &hello,
            Protocol::Smtp,
            Duration::from_millis(50),
        )
        .await
        else {
            panic!("connect must time out while waiting for banner");
        };

        assert!(error.is_timeout(), "expected timeout, got {error:?}");
    }

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn connect_setup_uses_single_deadline_for_banner_and_ehlo() {
        let hello = ClientId::Domain("client.example".to_owned());
        // The banner arrives, EHLO is written, and then the peer goes silent.
        // A per-operation timeout would restart the clock at EHLO; a single
        // setup deadline must still fire.
        let transcript = Transcript::new("220 smtp.example\r\n").expect_then_stall(HELLO);
        let Err(error) = AsyncSmtpConnection::from_transcript_with_timeout(
            transcript,
            &hello,
            Protocol::Smtp,
            Duration::from_millis(120),
        )
        .await
        else {
            panic!("connect must use one setup deadline across banner and EHLO");
        };

        assert!(error.is_timeout(), "expected timeout, got {error:?}");
    }

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn command_times_out_waiting_for_response() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect_then_stall("NOOP\r\n");
        let mut connection = AsyncSmtpConnection::from_transcript_with_timeout(
            transcript,
            &hello,
            Protocol::Smtp,
            Duration::from_millis(50),
        )
        .await
        .unwrap();

        let error = connection
            .command(Noop)
            .await
            .expect_err("NOOP must time out while waiting for response");
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
    }

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn cancelled_command_marks_connection_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect_then_stall("NOOP\r\n");
        let mut connection = AsyncSmtpConnection::from_transcript_with_timeout(
            transcript,
            &hello,
            Protocol::Smtp,
            Duration::from_secs(2),
        )
        .await
        .unwrap();

        // Dropping a read mid-response loses stream position, so the
        // connection must not be reused.
        let result =
            tokio::time::timeout(Duration::from_millis(50), connection.command(Noop)).await;
        assert!(result.is_err(), "command future must be cancelled");
        assert!(connection.has_broken());

        let error = connection.command(Noop).await.unwrap_err();
        assert!(
            error.is_connection(),
            "expected connection error: {error:?}"
        );
    }

    #[tokio::test(crate = "tokio")]
    async fn lmtp_drain_retires_the_connection_and_breaks_it_on_a_surplus_status() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = vec![
            "first@example.com".parse().unwrap(),
            "second@example.com".parse().unwrap(),
        ];
        let clean = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect(
                "\r\n.\r\n",
                "250 first delivered\r\n250 second delivered\r\n",
            );
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            recipients.clone(),
        )
        .unwrap();
        let mut connection = AsyncSmtpConnection::from_transcript(clean, &hello, Protocol::Lmtp)
            .await
            .unwrap();
        assert_eq!(
            connection
                .send_lmtp(&envelope, b"body")
                .await
                .unwrap()
                .len(),
            2
        );
        // Cleanliness below the read buffer is unprovable, so the connection
        // is retired rather than recycled - but it is not an error.
        assert!(connection.should_retire());
        assert!(!connection.has_broken());

        // Surplus bytes that arrive in the same segment do reach the read
        // buffer, and that case is a hard error.
        let surplus = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect_coalesced(
                "\r\n.\r\n",
                "250 first delivered\r\n250 second delivered\r\n250 surplus response\r\n",
            );
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();
        let mut connection = AsyncSmtpConnection::from_transcript(surplus, &hello, Protocol::Lmtp)
            .await
            .unwrap();
        let error = connection
            .send_lmtp(&envelope, b"body")
            .await
            .expect_err("a surplus final status desynchronizes the stream");
        assert!(
            error.to_string().contains("more final statuses"),
            "expected an honest LMTP final-status error, got: {error}"
        );
        assert!(connection.has_broken());
        assert!(connection.should_retire());
    }

    #[tokio::test(crate = "tokio")]
    async fn lmtp_too_few_final_statuses_breaks_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect_then_close("\r\n.\r\n", "250 first delivered\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                "first@example.com".parse().unwrap(),
                "second@example.com".parse().unwrap(),
            ],
        )
        .unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp)
                .await
                .unwrap();

        assert!(connection.send_lmtp(&envelope, b"body").await.is_err());
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn peer_closing_after_data_acceptance_leaves_recipient_uncertain() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect_then_close("DATA\r\n", "354 send body\r\n");
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: "first@example.com".parse().unwrap(),
        }];
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .await
            .expect("a body-write failure is represented per recipient")
            .resolve();

        assert!(outcome.succeeded().is_empty());
        assert_eq!(outcome.uncertain().len(), 1);
        assert_eq!(outcome.uncertain()[0].item.0, "item-0");
        assert!(connection.has_broken());
    }

    /// Every envelope-level parameter this crate can emit, on one pipelined
    /// peer, in one window. This is the widest wire-shape assertion in the
    /// crate: SIZE, HOLDFOR, BY, MT-PRIORITY, RET and the xtext-escaped ENVID
    /// on `MAIL FROM`, uniform NOTIFY plus a per-recipient ORCPT on the RCPT
    /// lines, and all of it written before any reply is read.
    #[tokio::test(crate = "tokio")]
    async fn send_with_options_pipelines_envelope_parameters_in_one_window() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250-PIPELINING\r\n250-SIZE 1024\r\n250-FUTURERELEASE 3600\r\n250-DELIVERBY 240\r\n250-MT-PRIORITY\r\n250 DSN\r\n",
            )
            .expect(
                concat!(
                    "MAIL FROM:<sender@example.com> SIZE=24 HOLDFOR=60 BY=300;R MT-PRIORITY=-1 RET=HDRS ENVID=env+3D1\r\n",
                    "RCPT TO:<first@example.com> NOTIFY=FAILURE,DELAY ORCPT=rfc822;alias+3Dfirst@example.com\r\n",
                    "RCPT TO:<second@example.com> NOTIFY=FAILURE,DELAY\r\n",
                ),
                "250 sender ok\r\n250 first ok\r\n250 second ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("Subject: test\r\n\r\nHello", "")
            .expect("\r\n.\r\n", "250 queued\r\n");
        let first_recipient: crate::address::Address = "first@example.com".parse().unwrap();
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                first_recipient.clone(),
                "second@example.com".parse().unwrap(),
            ],
        )
        .unwrap();
        let options = SendOptions::new()
            .hold_for(60)
            .deliver_by(300, DeliverByMode::Return, false)
            .unwrap()
            .mt_priority(-1)
            .unwrap()
            .dsn_return(DsnReturn::Headers)
            .envelope_id("env=1")
            .notify([DsnNotify::Failure, DsnNotify::Delay])
            .unwrap()
            .recipient_original_recipient(first_recipient, "rfc822", "alias=first@example.com");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .send_with_options(&envelope, b"Subject: test\r\n\r\nHello", &options)
            .await
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    /// Per-recipient DSN parameters override the uniform ones on the matching
    /// RCPT line only, and the batch path emits the same wire shape as the
    /// envelope path. Sequential (non-PIPELINING) peer, so each RCPT line is
    /// pinned as its own write.
    #[tokio::test(crate = "tokio")]
    async fn batch_rcpt_options_are_emitted_per_recipient_in_order() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 DSN\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect(
                "RCPT TO:<first@example.com> NOTIFY=NEVER ORCPT=rfc822;alias+3Dfirst@example.com\r\n",
                "250 first ok\r\n",
            )
            .expect(
                "RCPT TO:<second@example.com> NOTIFY=FAILURE,DELAY\r\n",
                "250 second ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "250 queued\r\n");
        let first: crate::address::Address = "first@example.com".parse().unwrap();
        let options = SendOptions::new()
            .notify([DsnNotify::Failure, DsnNotify::Delay])
            .unwrap()
            .recipient_never_notify(first.clone())
            .recipient_original_recipient(first.clone(), "rfc822", "alias=first@example.com");
        let batch = ["first@example.com", "second@example.com"]
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address: address.parse().unwrap(),
            })
            .collect();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &options,
            )
            .await
            .expect("the transaction completes")
            .resolve();

        assert_eq!(outcome.succeeded().len(), 2);
        transcript.assert_exhausted();
    }

    /// Recipient-specific parameters are keyed by address. A parameter naming
    /// an address that is not in the batch is a caller bug and must be caught
    /// before `MAIL FROM`, leaving the connection clean and reusable.
    #[tokio::test(crate = "tokio")]
    async fn batch_rcpt_parameters_for_an_unknown_recipient_fail_before_mail_from() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 DSN\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let options = SendOptions::new()
            .recipient_notify("stranger@example.com".parse().unwrap(), [DsnNotify::Never])
            .unwrap();
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: "first@example.com".parse().unwrap(),
        }];
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let (error, progress) = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &options,
            )
            .await
            .expect_err("unmatched recipient parameters are a pre-wire input error");
        assert!(
            error.to_string().contains("do not match a batch recipient"),
            "expected an unmatched-recipient refusal, got: {error}"
        );
        assert!(progress.resolve().succeeded().is_empty());

        // Nothing was written: the next scripted step is still NOOP.
        connection.command(Noop).await.unwrap();
        transcript.assert_exhausted();
    }

    /// A recipient rejected in the FIRST pipelining window must be reported
    /// against its own batch id while later-window recipients still succeed.
    /// The window boundary is where an implementation that re-indexes per
    /// window misattributes the rejection.
    #[tokio::test(crate = "tokio")]
    async fn pipelined_batch_keeps_original_indexes_across_a_window_boundary() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses: Vec<crate::address::Address> = (0..33)
            .map(|index| format!("recipient-{index}@example.com").parse().unwrap())
            .collect();
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.extend(
            addresses[..32]
                .iter()
                .map(|recipient| format!("RCPT TO:<{recipient}>\r\n")),
        );
        let mut first_replies = "250 sender ok\r\n".to_owned();
        first_replies.push_str(&"250 recipient ok\r\n".repeat(31));
        first_replies.push_str("550 recipient rejected\r\n");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(first_window, first_replies)
            .expect(
                format!("RCPT TO:<{}>\r\n", addresses[32]),
                "250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "250 queued\r\n");
        let batch = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, "item-31");
        assert_eq!(outcome.succeeded().last().unwrap().item.0, "item-32");
        transcript.assert_exhausted();
    }

    /// One final status per accepted recipient, mapped positionally: the
    /// second recipient's rejection must not be reported against the first.
    #[tokio::test(crate = "tokio")]
    async fn lmtp_drains_one_final_status_per_accepted_recipient() {
        let hello = ClientId::Domain("client.example".to_owned());
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
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                "first@example.com".parse().unwrap(),
                "second@example.com".parse().unwrap(),
            ],
        )
        .unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp)
                .await
                .unwrap();

        let statuses = connection.send_lmtp(&envelope, b"body").await.unwrap();

        assert_eq!(statuses.len(), 2);
        assert!(statuses[0].is_positive());
        assert!(!statuses[1].is_positive());
        // Surplus bytes below the read buffer are undetectable, so a completed
        // LMTP drain retires the connection rather than recycling it.
        assert!(connection.should_retire());
        transcript.assert_exhausted();
    }

    /// The batch LMTP path, unlike `send_lmtp`, must keep the per-recipient
    /// outcomes it already proved even when a surplus status desynchronizes
    /// the stream. Both deliveries are known; only the connection is lost.
    #[tokio::test(crate = "tokio")]
    async fn lmtp_batch_surplus_final_status_retires_the_connection_and_keeps_outcomes() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect_coalesced(
                "\r\n.\r\n",
                "250 first delivered\r\n250 second delivered\r\n250 surplus response\r\n",
            );
        let batch = ["first@example.com", "second@example.com"]
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address: address.parse().unwrap(),
            })
            .collect();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Lmtp)
                .await
                .unwrap();

        let outcome = connection
            .send_lmtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert_eq!(outcome.succeeded().len(), 2);
        assert!(outcome.uncertain().is_empty());
        assert!(connection.has_broken());
        assert!(connection.should_retire());
    }

    /// A reply line cut short by a close is a parse failure, not a usable
    /// connection carrying a half-read capability set.
    #[tokio::test(crate = "tokio")]
    async fn peer_closing_mid_reply_line_is_a_parse_failure() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect_then_close(HELLO, "250-smtp.example\r\n250 PIPELI");

        let Err(error) =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).await
        else {
            panic!("a truncated EHLO reply cannot yield a usable connection");
        };
        assert!(
            error.to_string().contains("incomplete response"),
            "expected an incomplete-response parse error, got: {error}"
        );
    }

    /// An unsolicited reply coalesced with a requested reply breaks the
    /// connection before it can be mistaken for the next command's answer.
    #[tokio::test(crate = "tokio")]
    async fn unsolicited_reply_coalesced_with_an_answer_breaks_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect_coalesced("NOOP\r\n", "250 noop ok\r\n421 service closing\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection
            .command(Noop)
            .await
            .expect_err("the surplus reply must break the connection immediately");
        assert!(
            error.to_string().contains("unsolicited reply"),
            "expected a desynchronization error, got: {error}"
        );
        assert!(connection.has_broken());
    }
}
