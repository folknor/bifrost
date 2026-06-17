use std::collections::HashMap;
use std::net::IpAddr;
#[cfg(unix)]
use std::path::Path;
use std::{fmt::Display, future::Future, time::Duration};

use bifrost_sasl::ScramChannelBinding;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::async_net::AsyncDeadline;
#[cfg(feature = "tracing")]
use super::escape_crlf;
use super::{
    ClientCodec, ConnectionState, MAX_RESPONSE_BYTES, MAX_RESPONSE_LINE_BYTES, TlsParameters,
    async_net::AsyncNetworkStream,
};
use crate::{
    Envelope,
    address::Address,
    transport::smtp::{
        Protocol,
        authentication::{
            Credentials, Mechanism, ScramExchange, ScramStep, decode_auth_challenge,
            decode_scram_payload, first_attemptable, oauth_mechanism, password_mechanism_order,
            scram_hash,
        },
        batch::{SendProgress, SmtpBatchRecipient},
        commands::{Auth, Bdat, Data, Ehlo, Expn, Lhlo, Mail, Noop, Rcpt, Rset, Starttls, Vrfy},
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
            self,
            SmtpCommandPhase::MailFrom
        );

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            try_smtp!(
                self.command(Rcpt::new(to_address.clone(), rcpt_options.clone()))
                    .await,
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

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options))
                .await,
            self,
            SmtpCommandPhase::MailFrom
        );

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            try_smtp!(
                self.command(Rcpt::new(to_address.clone(), rcpt_options.clone()))
                    .await,
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
            self,
            SmtpCommandPhase::MailFrom
        );

        let mut recipient_statuses = Vec::with_capacity(envelope.to().len());
        let mut accepted_recipients = 0;

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            let response = try_smtp!(
                self.command_accepting_status(Rcpt::new(to_address.clone(), rcpt_options.clone()))
                    .await,
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
            return Ok(rejected);
        }

        try_smtp!(
            self.command(Data).await,
            self,
            SmtpCommandPhase::DataCommand
        );
        let mut delivery_statuses = try_smtp!(
            self.message_lmtp(email, accepted_recipients).await,
            self,
            SmtpCommandPhase::LmtpFinalStatus
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

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options))
                .await,
            self,
            SmtpCommandPhase::MailFrom
        );

        let mut recipient_statuses = Vec::with_capacity(envelope.to().len());
        let mut accepted_recipients = 0;

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            let response = try_smtp!(
                self.command_accepting_status(Rcpt::new(to_address.clone(), rcpt_options.clone()))
                    .await,
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

        let mail_options = self
            .mail_options_for_batch(from.as_ref(), email, options, false)
            .map_err(|e| {
                (
                    e.with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress.clone(),
                )
            })?;

        if self.server_info().supports_pipelining() {
            return self
                .send_smtp_batch_pipelined(from, email, mail_options, options, progress)
                .await;
        }

        // Non-pipelined path. Split the wire event the same way the pipelined
        // sibling does so identical events yield identical transmission
        // evidence regardless of PIPELINING: a transport drop is `InFlight`,
        // an acknowledged negative reply is `Acknowledged` (not `Unsent`).
        let mail_cmd = Mail::new(from, mail_options);
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
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
        }

        let recipient_addresses: Vec<Address> = progress
            .recipients
            .iter()
            .map(|r| r.address.clone())
            .collect();
        for (i, addr) in recipient_addresses.into_iter().enumerate() {
            let rcpt_options = self.rcpt_options_single(&addr, options).unwrap_or_default();
            match self
                .command_accepting_status(Rcpt::new(addr, rcpt_options))
                .await
            {
                Ok(resp) if resp.is_positive() => progress.record_rcpt_accepted(i),
                Ok(resp) => progress.record_rcpt_rejected(i, resp),
                Err(e) => {
                    use crate::transport::smtp::account_error::{
                        SmtpErrorContext, into_account_error,
                    };
                    let ae = into_account_error(
                        e.with_attempt(SmtpTransmissionState::InFlight)
                            .with_phase(SmtpCommandPhase::RcptTo),
                        SmtpErrorContext::send(Protocol::Smtp).with_phase(SmtpCommandPhase::RcptTo),
                    );
                    let ae2 = ae.clone();
                    progress.mark_uncertain_unresolved(|| ae2.clone());
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
            return Ok(progress);
        }

        match self.command_accepting_status(Data).await {
            Ok(resp) if resp.is_positive() => {}
            Ok(resp) => {
                progress.mark_accepted_rejected_with_response(resp);
                if let Err(_e) = self.command_accepting_status(Rset).await {
                    self.abort().await;
                }
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
        from: Option<Address>,
        email: &[u8],
        mail_options: Vec<MailParameter>,
        options: &SendOptions,
        mut progress: SendProgress,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        let mut commands = Mail::new(from, mail_options).to_string();
        let rcpt_options_all: Vec<Vec<RcptParameter>> = progress
            .recipients
            .iter()
            .map(|r| {
                self.rcpt_options_single(&r.address, options)
                    .unwrap_or_default()
            })
            .collect();
        for (rec, rcpt_opts) in progress.recipients.iter().zip(&rcpt_options_all) {
            commands.push_str(&Rcpt::new(rec.address.clone(), rcpt_opts.clone()).to_string());
        }
        commands.push_str(&Data.to_string());

        if let Err(e) = self.write(commands.as_bytes()).await {
            self.abort().await;
            return Err((
                e.with_attempt(SmtpTransmissionState::Unsent)
                    .with_phase(SmtpCommandPhase::MailFrom),
                progress,
            ));
        }

        let mail_response = match self.read_response_accepting_status().await {
            Ok(r) => r,
            Err(e) => {
                self.abort().await;
                return Err((
                    e.with_attempt(SmtpTransmissionState::InFlight)
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

        let n_recipients = progress.recipients.len();
        for i in 0..n_recipients {
            match self.read_response_accepting_status().await {
                Ok(resp) if resp.is_positive() => progress.record_rcpt_accepted(i),
                Ok(resp) => progress.record_rcpt_rejected(i, resp),
                Err(e) => {
                    use crate::transport::smtp::account_error::{
                        SmtpErrorContext, into_account_error,
                    };
                    let ae = into_account_error(
                        e.with_attempt(SmtpTransmissionState::InFlight)
                            .with_phase(SmtpCommandPhase::RcptTo),
                        SmtpErrorContext::send(Protocol::Smtp).with_phase(SmtpCommandPhase::RcptTo),
                    );
                    let ae2 = ae.clone();
                    progress.mark_uncertain_unresolved(|| ae2.clone());
                    self.abort().await;
                    return Ok(progress);
                }
            }
        }

        let data_response = match self.read_response_accepting_status().await {
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

        let accepted = progress.recipients.iter().any(|r| {
            matches!(
                r.rcpt,
                crate::transport::smtp::batch::RcptProgress::Accepted
            )
        });

        if !data_response.is_positive() {
            progress.mark_accepted_rejected_with_response(data_response);
            if !accepted {
                if let Err(_e) = self.command_accepting_status(Rset).await {
                    self.abort().await;
                }
            } else {
                self.abort().await;
            }
            return Ok(progress);
        }

        if !accepted {
            if self.write(b".\r\n").await.is_ok() {
                let _ = self.read_response_accepting_status().await;
            } else {
                self.abort().await;
            }
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

        let mail_options = self
            .mail_options_for_batch(from.as_ref(), email, options, false)
            .map_err(|e| {
                (
                    e.with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress.clone(),
                )
            })?;

        // MAIL FROM: an acknowledged negative reply is `Acknowledged`, a
        // transport drop is `InFlight` - never `Unsent` for either. See the
        // SMTP non-pipelined path for the same split.
        let mail_cmd = Mail::new(from, mail_options);
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
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
        }

        let recipient_addresses: Vec<Address> = progress
            .recipients
            .iter()
            .map(|r| r.address.clone())
            .collect();
        let mut accepted_count = 0usize;
        for (i, addr) in recipient_addresses.into_iter().enumerate() {
            let rcpt_options = self.rcpt_options_single(&addr, options).unwrap_or_default();
            match self
                .command_accepting_status(Rcpt::new(addr, rcpt_options))
                .await
            {
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
                        e.with_attempt(SmtpTransmissionState::InFlight)
                            .with_phase(SmtpCommandPhase::RcptTo),
                        SmtpErrorContext::send(Protocol::Lmtp).with_phase(SmtpCommandPhase::RcptTo),
                    );
                    let ae2 = ae.clone();
                    progress.mark_uncertain_unresolved(|| ae2.clone());
                    self.abort().await;
                    return Ok(progress);
                }
            }
        }

        if accepted_count == 0 {
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
                if let Err(_e) = self.command_accepting_status(Rset).await {
                    self.abort().await;
                }
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

        for i in 0..progress.recipients.len() {
            if !matches!(
                progress.recipients[i].rcpt,
                crate::transport::smtp::batch::RcptProgress::Accepted
            ) {
                continue;
            }
            match self.read_response_accepting_status().await {
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

        Ok(progress)
    }

    /// Write the DATA body without reading the final reply.
    async fn write_body(&mut self, email: &[u8]) -> Result<(), Error> {
        let mut codec = crate::transport::smtp::client::ClientCodec::new();
        let mut out_buf = Vec::with_capacity(email.len());
        codec.encode(email, &mut out_buf);
        self.write(out_buf.as_slice()).await?;
        self.write(b"\r\n.\r\n").await
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

    /// Like `mail_options` but takes an explicit sender address instead of an `Envelope`.
    fn mail_options_for_batch(
        &self,
        from: Option<&Address>,
        email: &[u8],
        options: &SendOptions,
        allow_binary_mime: bool,
    ) -> Result<Vec<MailParameter>, Error> {
        let mut mail_options = vec![];

        let has_smtputf8 = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::SmtpUtfEight));
        let has_body_parameter = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::Body(_)));

        let has_non_ascii = from.is_some_and(|a| !AsRef::<str>::as_ref(a).is_ascii());
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
                .is_some_and(|limit| email.len() > limit)
            {
                return Err(error::invalid_input(
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
                .is_some_and(|limit| email.len() > limit)
            {
                return Err(error::invalid_input(
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
                try_smtp!(
                    self.command_with_budget(Lhlo::new(hello_name.clone()), budget)
                        .await,
                    self
                )
            }
            _ => {
                try_smtp!(
                    self.command_with_budget(Ehlo::new(hello_name.clone()), budget)
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

        let order = password_mechanism_order(mechanisms, &self.server_info);

        // The DER accessor is sync, so the PLUS binding resolution adds no
        // round trips: selection stays network-free.
        let mut bindings: HashMap<Mechanism, ScramChannelBinding> = HashMap::new();
        for &mech in &order {
            if matches!(mech, Mechanism::ScramSha1Plus | Mechanism::ScramSha256Plus)
                && let Some(b) = self.resolve_scram_binding()
            {
                bindings.insert(mech, b);
            }
        }

        let chosen = first_attemptable(&order, |m| bindings.contains_key(&m))?;
        match chosen {
            Mechanism::ScramSha1Plus | Mechanism::ScramSha256Plus => {
                let binding = bindings
                    .remove(&chosen)
                    .expect("PLUS binding resolved above");
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
    fn resolve_scram_binding(&self) -> Option<ScramChannelBinding> {
        let der = self.peer_certificate_der()?;
        let bytes = bifrost_sasl::tls_server_end_point(&der).ok()?;
        Some(ScramChannelBinding::TlsServerEndPoint(bytes))
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

        let auth = Auth::new(mechanism, credentials.clone(), None, None)?;
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
        self.write(format!("{line}\r\n").as_bytes()).await?;
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
        let auth = Auth::new(mechanism, credentials.clone(), None, oauth_token)?;
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
                Auth::new_from_response(mechanism, credentials.clone(), &response, oauth_token),
                self,
                SmtpCommandPhase::Auth
            );
            response = try_smtp!(self.command(continuation).await, self, SmtpCommandPhase::Auth);
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
        error::status(response)
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
                return Err(error::parse("SMTP response line too long"));
            }
            if buffer.len() > MAX_RESPONSE_BYTES {
                return Err(error::parse("SMTP response too large"));
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
    async fn async_peer_certificate_der_is_none_on_plaintext() {
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
        });

        let connection =
            AsyncSmtpConnection::connect(address, None, &ClientId::default(), None, None)
                .await
                .unwrap();
        assert!(
            connection.peer_certificate_der().is_none(),
            "plaintext connection must have no peer certificate DER"
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
