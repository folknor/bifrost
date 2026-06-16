#[cfg(unix)]
use std::path::Path;
use std::{
    fmt::Display,
    io::{self, BufRead, BufReader, Write},
    net::{IpAddr, ToSocketAddrs},
    time::Duration,
};

#[cfg(feature = "tracing")]
use super::escape_crlf;
use super::{
    ClientCodec, ConnectionState, MAX_RESPONSE_BYTES, MAX_RESPONSE_LINE_BYTES, NetworkStream,
    TlsParameters,
};
use crate::{
    address::{Address, Envelope},
    transport::smtp::{
        Protocol,
        authentication::{Credentials, Mechanism},
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
                $client.abort();
                return Err(From::from(err))
            },
        }
    });
    // Phase-tagged variant. Stamps the SMTP error with the given
    // SmtpCommandPhase so the translation boundary in `account_error.rs`
    // can route per-phase (e.g. AUTH-time failures -> PolicyBlocked,
    // MAIL FROM phase tagged on telemetry, etc.). Use this in the
    // legacy non-batch send paths and the AUTH command exchange.
    ($err: expr, $client: ident, $phase: expr) => ({
        match $err {
            Ok(val) => val,
            Err(err) => {
                $client.abort();
                return Err(From::from(err.with_phase($phase)))
            },
        }
    });
);

/// Structure that implements the SMTP client
pub(crate) struct SmtpConnection {
    /// TCP stream between client and server
    /// Value is None before connection
    stream: BufReader<NetworkStream>,
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
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
        };
        conn.set_timeout(timeout).map_err(error::network)?;
        let _response = conn.read_response()?;

        conn.hello(hello_name)?;

        // Print server information
        #[cfg(feature = "tracing")]
        tracing::debug!("server {}", conn.server_info);
        Ok(conn)
    }

    #[cfg(unix)]
    pub(crate) fn connect_unix_with_protocol(
        path: &Path,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        protocol: Protocol,
    ) -> Result<SmtpConnection, Error> {
        let stream = NetworkStream::connect_unix(path, timeout)?;
        let stream = BufReader::new(stream);
        let mut conn = SmtpConnection {
            stream,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
        };
        conn.set_timeout(timeout).map_err(error::network)?;
        let _response = conn.read_response()?;

        conn.hello(hello_name)?;

        #[cfg(feature = "tracing")]
        tracing::debug!("server {}", conn.server_info);
        Ok(conn)
    }

    pub(crate) fn send(&mut self, envelope: &Envelope, email: &[u8]) -> Result<Response, Error> {
        self.send_with_options(envelope, email, &SendOptions::default())
    }

    pub(crate) fn send_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        let mail_options = self.mail_options(envelope, email, options, false)?;
        let rcpt_options = self.rcpt_options(envelope, options)?;

        if self.server_info().supports_pipelining() {
            return self.send_pipelined(envelope, email, mail_options, rcpt_options);
        }

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options)),
            self,
            SmtpCommandPhase::MailFrom
        );

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            try_smtp!(
                self.command(Rcpt::new(to_address.clone(), rcpt_options.clone())),
                self,
                SmtpCommandPhase::RcptTo
            );
        }

        try_smtp!(self.command(Data), self, SmtpCommandPhase::DataCommand);
        let result = try_smtp!(self.message(email), self, SmtpCommandPhase::DataBody);
        Ok(result)
    }

    pub(crate) fn send_bdat_with_options(
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
            self.command(Mail::new(envelope.from().cloned(), mail_options)),
            self,
            SmtpCommandPhase::MailFrom
        );

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            try_smtp!(
                self.command(Rcpt::new(to_address.clone(), rcpt_options.clone())),
                self,
                SmtpCommandPhase::RcptTo
            );
        }

        let result = try_smtp!(self.message_bdat(email), self, SmtpCommandPhase::BdatBody);
        Ok(result)
    }

    fn send_pipelined(
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

        self.write(commands.as_bytes())?;

        let mail_response = self.read_response_accepting_status()?;
        let mut recipient_responses = Vec::with_capacity(envelope.to().len());
        for _ in envelope.to() {
            recipient_responses.push(self.read_response_accepting_status()?);
        }
        let data_response = self.read_response_accepting_status()?;
        let accepted_recipients = recipient_responses
            .iter()
            .filter(|response| response.is_positive())
            .count();

        if !mail_response.is_positive() {
            self.reset_or_abort_pipelined_transaction(&data_response, accepted_recipients);
            return Err(Self::error_from_status(mail_response));
        }

        if let Some(response) = recipient_responses
            .iter()
            .find(|response| !response.is_positive())
        {
            self.reset_or_abort_pipelined_transaction(&data_response, accepted_recipients);
            return Err(Self::error_from_status(response.clone()));
        }

        if !data_response.is_positive() {
            self.reset_or_abort_pipelined_transaction(&data_response, accepted_recipients);
            return Err(Self::error_from_status(data_response));
        }

        let result = try_smtp!(self.message(email), self);
        Ok(result)
    }

    fn reset_or_abort_pipelined_transaction(
        &mut self,
        data_response: &Response,
        accepted_recipients: usize,
    ) {
        if data_response.is_positive() {
            if accepted_recipients == 0 {
                if self
                    .write(b".\r\n")
                    .and_then(|_| self.read_response_accepting_status())
                    .is_err()
                {
                    self.abort();
                }
            } else {
                self.abort();
            }
        } else if self.command_accepting_status(Rset).is_err() {
            self.abort();
        }
    }

    pub(crate) fn send_lmtp(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
    ) -> Result<Vec<Response>, Error> {
        self.send_lmtp_with_options(envelope, email, &SendOptions::default())
    }

    pub(crate) fn send_lmtp_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        let mail_options = self.mail_options(envelope, email, options, false)?;
        let rcpt_options = self.rcpt_options(envelope, options)?;

        try_smtp!(
            self.command(Mail::new(envelope.from().cloned(), mail_options)),
            self,
            SmtpCommandPhase::MailFrom
        );

        let mut recipient_statuses = Vec::with_capacity(envelope.to().len());
        let mut accepted_recipients = 0;

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            let response = try_smtp!(
                self.command_accepting_status(Rcpt::new(to_address.clone(), rcpt_options.clone())),
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

        try_smtp!(self.command(Data), self, SmtpCommandPhase::DataCommand);
        let mut delivery_statuses = try_smtp!(
            self.message_lmtp(email, accepted_recipients),
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

    pub(crate) fn send_lmtp_bdat_with_options(
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
            self.command(Mail::new(envelope.from().cloned(), mail_options)),
            self,
            SmtpCommandPhase::MailFrom
        );

        let mut recipient_statuses = Vec::with_capacity(envelope.to().len());
        let mut accepted_recipients = 0;

        for (to_address, rcpt_options) in envelope.to().iter().zip(&rcpt_options) {
            let response = try_smtp!(
                self.command_accepting_status(Rcpt::new(to_address.clone(), rcpt_options.clone())),
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

        let mut delivery_statuses =
            try_smtp!(self.message_lmtp_bdat(email, accepted_recipients), self).into_iter();

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

    /// Account-oriented SMTP multi-recipient send.
    ///
    /// Drives the SMTP command sequence (MAIL FROM, sequential RCPTs, DATA,
    /// body, final reply) and records per-recipient progress into a
    /// `SendProgress` tracker.
    ///
    /// Returns `Ok(progress)` when the command sequence completes (even if
    /// some recipients were rejected). Returns `Err((error, progress))` for
    /// batch-level failures where no recipient-specific outcome can be
    /// attributed: MAIL FROM rejection, transport drop before any command,
    /// or pre-MAIL local validation errors. In the `Err` case, the progress
    /// tracker contains whatever state was accumulated before the abort.
    pub(crate) fn send_smtp_batch(
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
            return self.send_smtp_batch_pipelined(from, email, mail_options, options, progress);
        }

        // Non-pipelined path.
        let mail_cmd = Mail::new(from, mail_options);
        if let Err(e) = self.command(mail_cmd) {
            self.abort();
            return Err((
                e.with_attempt(SmtpTransmissionState::Unsent)
                    .with_phase(SmtpCommandPhase::MailFrom),
                progress,
            ));
        }

        let recipient_addresses: Vec<Address> = progress
            .recipients
            .iter()
            .map(|r| r.address.clone())
            .collect();
        for (i, addr) in recipient_addresses.into_iter().enumerate() {
            let rcpt_options = self.rcpt_options_single(&addr, options).unwrap_or_default();
            match self.command_accepting_status(Rcpt::new(addr, rcpt_options)) {
                Ok(resp) if resp.is_positive() => progress.record_rcpt_accepted(i),
                Ok(resp) => progress.record_rcpt_rejected(i, resp),
                Err(e) => {
                    // Transport drop during RCPT TO. Mark this and all
                    // subsequent recipients uncertain and return.
                    let err_clone = e
                        .with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::RcptTo);
                    {
                        let err_for_closure = err_clone.clone();
                        let addr_str = progress.recipients[i].address.clone();
                        use crate::transport::smtp::account_error::{
                            SmtpErrorContext, into_account_error,
                        };
                        progress.mark_uncertain_unresolved(|| {
                            into_account_error(
                                err_for_closure.clone(),
                                SmtpErrorContext::send(Protocol::Smtp)
                                    .with_attempt(SmtpTransmissionState::InFlight)
                                    .with_phase(SmtpCommandPhase::RcptTo)
                                    .with_scope(bifrost_types::error::ErrorScope::Account),
                            )
                        });
                        let _ = addr_str;
                    }
                    self.abort();
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

        // Send DATA command.
        match self.command_accepting_status(Data) {
            Ok(resp) if resp.is_positive() => {}
            Ok(resp) => {
                // DATA rejected before body: all accepted recipients failed with this response.
                progress.mark_accepted_rejected_with_response(resp);
                if let Err(_e) = self.command_accepting_status(Rset) {
                    self.abort();
                }
                return Ok(progress);
            }
            Err(e) => {
                // Transport drop during DATA command: outcome for accepted recipients uncertain.
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataCommand),
                    SmtpErrorContext::send(Protocol::Smtp)
                        .with_phase(SmtpCommandPhase::DataCommand),
                );
                let ae2 = ae.clone();
                progress.mark_accepted_uncertain(|| ae2.clone());
                self.abort();
                return Ok(progress);
            }
        }

        // Body starts here: side-effect boundary crossed.
        progress.set_body_started();
        match self.message(email) {
            Ok(resp) => {
                progress.set_body_finished();
                progress.set_data_response(resp);
            }
            Err(e) => {
                // Transport drop after body write started: accepted recipients uncertain.
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataBody),
                    SmtpErrorContext::send(Protocol::Smtp).with_phase(SmtpCommandPhase::DataBody),
                );
                let ae2 = ae.clone();
                progress.mark_uncertain_unresolved(|| ae2.clone());
                self.abort();
                return Ok(progress);
            }
        }

        Ok(progress)
    }

    fn send_smtp_batch_pipelined(
        &mut self,
        from: Option<Address>,
        email: &[u8],
        mail_options: Vec<MailParameter>,
        options: &SendOptions,
        mut progress: SendProgress,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        // Build and write the pipelined command batch (MAIL FROM + all RCPTs + DATA).
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

        if let Err(e) = self.write(commands.as_bytes()) {
            self.abort();
            return Err((
                e.with_attempt(SmtpTransmissionState::Unsent)
                    .with_phase(SmtpCommandPhase::MailFrom),
                progress,
            ));
        }

        // Drain MAIL FROM reply.
        let mail_response = match self.read_response_accepting_status() {
            Ok(r) => r,
            Err(e) => {
                self.abort();
                return Err((
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
        };
        if !mail_response.is_positive() {
            self.abort();
            return Err((
                error::status(mail_response)
                    .with_attempt(SmtpTransmissionState::Acknowledged)
                    .with_phase(SmtpCommandPhase::MailFrom),
                progress,
            ));
        }

        // Drain RCPT replies.
        let n_recipients = progress.recipients.len();
        for i in 0..n_recipients {
            match self.read_response_accepting_status() {
                Ok(resp) if resp.is_positive() => progress.record_rcpt_accepted(i),
                Ok(resp) => progress.record_rcpt_rejected(i, resp),
                Err(e) => {
                    // Transport drop during RCPT drain: remaining recipients uncertain.
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
                    self.abort();
                    return Ok(progress);
                }
            }
        }

        // Drain DATA reply.
        let data_response = match self.read_response_accepting_status() {
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
                self.abort();
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
            // DATA negative: all accepted recipients failed with this response.
            progress.mark_accepted_rejected_with_response(data_response);
            if !accepted {
                // No accepted recipients and DATA negative; clean up.
                if let Err(_e) = self.command_accepting_status(Rset) {
                    self.abort();
                }
            } else {
                self.abort();
            }
            return Ok(progress);
        }

        if !accepted {
            // No accepted recipients, DATA positive; send terminating dot to complete transaction.
            if self
                .write(b".\r\n")
                .and_then(|_| self.read_response_accepting_status())
                .is_err()
            {
                self.abort();
            }
            return Ok(progress);
        }

        // Body starts here.
        progress.set_body_started();
        match self.message(email) {
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
                self.abort();
                return Ok(progress);
            }
        }

        Ok(progress)
    }

    /// Account-oriented LMTP multi-recipient send.
    ///
    /// Like `send_smtp_batch` but reads one final status per accepted recipient
    /// after the DATA body. Returns `Err((error, progress))` for batch-level
    /// failures; returns `Ok(progress)` otherwise.
    pub(crate) fn send_lmtp_batch(
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

        let mail_cmd = Mail::new(from, mail_options);
        if let Err(e) = self.command(mail_cmd) {
            self.abort();
            return Err((
                e.with_attempt(SmtpTransmissionState::Unsent)
                    .with_phase(SmtpCommandPhase::MailFrom),
                progress,
            ));
        }

        let recipient_addresses: Vec<Address> = progress
            .recipients
            .iter()
            .map(|r| r.address.clone())
            .collect();
        let mut accepted_count = 0usize;
        for (i, addr) in recipient_addresses.into_iter().enumerate() {
            let rcpt_options = self.rcpt_options_single(&addr, options).unwrap_or_default();
            match self.command_accepting_status(Rcpt::new(addr, rcpt_options)) {
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
                    self.abort();
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
        match self.command_accepting_status(Data) {
            Ok(resp) if resp.is_positive() => {}
            Ok(resp) => {
                progress.mark_accepted_rejected_with_response(resp);
                if let Err(_e) = self.command_accepting_status(Rset) {
                    self.abort();
                }
                return Ok(progress);
            }
            Err(e) => {
                // Transport drop during DATA command write/read: outcome for
                // accepted recipients uncertain. Phase = DataCommand to
                // distinguish the command boundary from a body-write drop.
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataCommand),
                    SmtpErrorContext::send(Protocol::Lmtp)
                        .with_phase(SmtpCommandPhase::DataCommand),
                );
                let ae2 = ae.clone();
                progress.mark_accepted_uncertain(|| ae2.clone());
                self.abort();
                return Ok(progress);
            }
        }

        // Body starts here.
        progress.set_body_started();
        if let Err(e) = self.write_body(email) {
            use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
            let ae = into_account_error(
                e.with_attempt(SmtpTransmissionState::InFlight)
                    .with_phase(SmtpCommandPhase::DataBody),
                SmtpErrorContext::send(Protocol::Lmtp).with_phase(SmtpCommandPhase::DataBody),
            );
            let ae2 = ae.clone();
            progress.mark_uncertain_unresolved(|| ae2.clone());
            self.abort();
            return Ok(progress);
        }
        progress.set_body_finished();

        // Read one final LMTP status per accepted recipient.
        let mut lmtp_index = 0usize;
        for i in 0..progress.recipients.len() {
            if !matches!(
                progress.recipients[i].rcpt,
                crate::transport::smtp::batch::RcptProgress::Accepted
            ) {
                continue;
            }
            match self.read_response_accepting_status() {
                Ok(resp) => {
                    progress.record_lmtp_final(i, resp);
                    lmtp_index += 1;
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
                    self.abort();
                    return Ok(progress);
                }
            }
        }
        let _ = lmtp_index;

        Ok(progress)
    }

    /// Write the DATA body without reading the final reply.
    fn write_body(&mut self, email: &[u8]) -> Result<(), Error> {
        let mut codec = ClientCodec::new();
        let mut out_buf = Vec::with_capacity(email.len());
        codec.encode(email, &mut out_buf);
        self.write(out_buf.as_slice())?;
        self.write(b"\r\n.\r\n")
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
                    return Err(error::invalid_input(
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
                        return Err(error::invalid_input(
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

    pub(crate) fn can_starttls(&self) -> bool {
        !self.is_encrypted() && self.server_info.supports_feature(Extension::StartTls)
    }

    pub(crate) fn starttls(
        &mut self,
        tls_parameters: &TlsParameters,
        hello_name: &ClientId,
    ) -> Result<(), Error> {
        if self.server_info.supports_feature(Extension::StartTls) {
            try_smtp!(self.command(Starttls), self);
            self.stream.get_mut().upgrade_tls(tls_parameters)?;
            #[cfg(feature = "tracing")]
            tracing::debug!("connection encrypted");
            // Send EHLO/LHLO again
            try_smtp!(self.hello(hello_name), self);
            self.hello_name = hello_name.clone();
            Ok(())
        } else {
            Err(error::invalid_input(
                "STARTTLS is not supported on this server",
            ))
        }
    }

    /// Send EHLO or LHLO and update server info
    fn hello(&mut self, hello_name: &ClientId) -> Result<(), Error> {
        let response = match self.protocol {
            Protocol::Lmtp => try_smtp!(self.command(Lhlo::new(hello_name.clone())), self),
            _ => try_smtp!(self.command(Ehlo::new(hello_name.clone())), self),
        };
        self.server_info = try_smtp!(ServerInfo::from_response(&response), self);
        Ok(())
    }

    pub(crate) fn abort(&mut self) {
        let _ = self.stream.get_mut().shutdown(std::net::Shutdown::Both);
    }

    /// Tells if the underlying stream is currently encrypted
    pub(crate) fn is_encrypted(&self) -> bool {
        self.stream.get_ref().is_encrypted()
    }

    /// DER of the peer (server) certificate. See
    /// [`NetworkStream::peer_certificate_der`] for the contract.
    // Plumbing for SCRAM-PLUS channel binding; the first consumer is the
    // SASL layer, so there is no in-crate caller yet.
    #[allow(dead_code)]
    pub(crate) fn peer_certificate_der(&self) -> Option<Vec<u8>> {
        self.stream.get_ref().peer_certificate_der()
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

    /// Sends a VRFY command and returns the server response.
    pub(crate) fn verify(&mut self, argument: impl Into<String>) -> Result<Response, Error> {
        self.command_accepting_status(Vrfy::new(argument.into())?)
    }

    /// Sends an EXPN command and returns the server response.
    pub(crate) fn expand(&mut self, argument: impl Into<String>) -> Result<Response, Error> {
        self.command_accepting_status(Expn::new(argument.into())?)
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
            .ok_or_else(|| {
                // Tag the phase on the error so the account-error mapper can
                // route this to Authorization(PolicyBlocked). The generic
                // InvalidInput arm without phase would otherwise classify
                // this as Request(Malformed) -> ClientBug, which is the wrong
                // UX (the right one is reauth/policy-change, not "library
                // bug, see internal telemetry").
                error::invalid_input("No compatible authentication mechanism was found")
                    .with_phase(SmtpCommandPhase::Auth)
            })?;

        // Limit challenges to avoid blocking
        let mut challenges = 10;
        let auth = Auth::new(mechanism, credentials.clone(), None)?;
        let mut response = try_smtp!(self.command(auth), self, SmtpCommandPhase::Auth);

        while challenges > 0 && response.has_code(334) {
            challenges -= 1;
            response = try_smtp!(
                self.command(Auth::new_from_response(
                    mechanism,
                    credentials.clone(),
                    &response,
                )?),
                self,
                SmtpCommandPhase::Auth
            );
        }

        if challenges == 0 {
            Err(error::parse("Unexpected number of challenges"))
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

    pub(crate) fn message_bdat(&mut self, message: &[u8]) -> Result<Response, Error> {
        self.write(Bdat::last(message.len()).to_string().as_bytes())?;
        self.write(message)?;
        self.read_response()
    }

    pub(crate) fn message_lmtp_bdat(
        &mut self,
        message: &[u8],
        recipients: usize,
    ) -> Result<Vec<Response>, Error> {
        self.write(Bdat::last(message.len()).to_string().as_bytes())?;
        self.write(message)?;

        let mut responses = Vec::with_capacity(recipients);
        for _ in 0..recipients {
            responses.push(self.read_response_accepting_status()?);
        }

        Ok(responses)
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

    fn error_from_status(response: Response) -> Error {
        error::status(response)
    }

    /// Writes a string to the server
    fn write(&mut self, string: &[u8]) -> Result<(), Error> {
        self.stream.get_ref().state().verify()?;
        self.stream.get_mut().set_state(ConnectionState::Broken);

        self.stream
            .get_mut()
            .write_all(string)
            .map_err(error::network)?;
        self.stream.get_mut().flush().map_err(error::network)?;
        self.stream.get_mut().set_state(ConnectionState::Ok);

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
        self.stream.get_ref().state().verify()?;
        self.stream.get_mut().set_state(ConnectionState::Broken);

        let mut buffer = String::with_capacity(100);
        let mut pre = 0;

        while self.stream.read_line(&mut buffer).map_err(error::network)? > 0 {
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
                    self.stream.get_mut().set_state(ConnectionState::Ok);

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
            SmtpConnection,
            authentication::{Credentials, Mechanism},
            extension::{
                ClientId, DeliverByMode, DsnNotify, DsnReturn, Extension, MailBodyParameter,
                MailParameter, SendOptions,
            },
        },
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
    fn peer_certificate_der_is_none_on_plaintext() {
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
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();
        assert!(
            connection.peer_certificate_der().is_none(),
            "plaintext connection must have no peer certificate DER"
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
    fn send_uses_pipelining_for_mail_and_recipients() {
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
                    b"250-localhost\r\n250-PIPELINING\r\n250-SIZE 1024\r\n250-FUTURERELEASE 3600\r\n250-DELIVERBY 240\r\n250-MT-PRIORITY\r\n250 DSN\r\n",
                )
                .unwrap();

            let expected_batch = concat!(
                "MAIL FROM:<sender@example.com> SIZE=22 HOLDFOR=60 BY=300;R MT-PRIORITY=-1 RET=HDRS ENVID=env+3D1\r\n",
                "RCPT TO:<first@example.com> NOTIFY=FAILURE,DELAY ORCPT=rfc822;alias+3Dfirst@example.com\r\n",
                "RCPT TO:<second@example.com> NOTIFY=FAILURE,DELAY\r\n",
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
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();
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

        let response = connection
            .send_with_options(&envelope, b"Subject: test\r\n\r\nHello", &options)
            .unwrap();
        assert!(response.has_code(250));

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(commands[0].starts_with("EHLO "));
        assert_eq!(
            commands[1],
            "MAIL FROM:<sender@example.com> SIZE=22 HOLDFOR=60 BY=300;R MT-PRIORITY=-1 RET=HDRS ENVID=env+3D1\r\n"
        );
        assert_eq!(
            commands[2],
            "RCPT TO:<first@example.com> NOTIFY=FAILURE,DELAY ORCPT=rfc822;alias+3Dfirst@example.com\r\n"
        );
        assert_eq!(
            commands[3],
            "RCPT TO:<second@example.com> NOTIFY=FAILURE,DELAY\r\n"
        );
        assert_eq!(commands[4], "DATA\r\n");
        handle.join().unwrap();
    }

    #[test]
    fn pipelined_send_rsets_when_data_is_rejected() {
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
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();

        let result = connection.send(&envelope, b"Subject: test\r\n\r\nHello");
        assert!(result.is_err());
        assert!(!connection.has_broken());
        assert!(connection.test_connected());

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(commands[0].starts_with("EHLO "));
        assert_eq!(commands[1], "MAIL FROM:<sender@example.com> SIZE=22\r\n");
        assert_eq!(commands[2], "RCPT TO:<recipient@example.com>\r\n");
        assert_eq!(commands[3], "DATA\r\n");
        assert_eq!(commands[4], "RSET\r\n");
        assert_eq!(commands[5], "NOOP\r\n");
        handle.join().unwrap();
    }

    #[test]
    fn explicit_mail_parameters_are_not_duplicated() {
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
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();
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
            .unwrap();
        assert!(response.has_code(250));

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(
            commands[1],
            "MAIL FROM:<sender@example.com> SIZE=23 SMTPUTF8 BODY=8BITMIME\r\n"
        );
        handle.join().unwrap();
    }

    #[test]
    fn send_bdat_uses_chunking_without_dot_stuffing() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (commands_tx, commands_rx) = mpsc::channel();
        let message = b"Subject: test\r\n\r\n.Line\r\nBinary\0";
        let expected = message.to_vec();

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
                .write_all(b"250-localhost\r\n250-CHUNKING\r\n250 BINARYMIME\r\n")
                .unwrap();

            for response in [b"250 sender ok\r\n".as_slice(), b"250 recipient ok\r\n"] {
                let mut command = String::new();
                reader.read_line(&mut command).unwrap();
                commands.push(command);
                stream.write_all(response).unwrap();
            }

            let mut bdat = String::new();
            reader.read_line(&mut bdat).unwrap();
            commands.push(bdat);

            let mut body = vec![0; expected.len()];
            reader.read_exact(&mut body).unwrap();
            assert_eq!(body, expected);
            stream.write_all(b"250 queued\r\n").unwrap();

            commands_tx.send(commands).unwrap();
        });

        let mut connection =
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let options =
            SendOptions::new().mail_parameter(MailParameter::Body(MailBodyParameter::BinaryMime));

        let response = connection
            .send_bdat_with_options(&envelope, message, &options)
            .unwrap();
        assert!(response.has_code(250));

        let commands = commands_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(commands[0].starts_with("EHLO "));
        assert_eq!(
            commands[1],
            "MAIL FROM:<sender@example.com> BODY=BINARYMIME\r\n"
        );
        assert_eq!(commands[2], "RCPT TO:<recipient@example.com>\r\n");
        assert_eq!(commands[3], format!("BDAT {} LAST\r\n", message.len()));
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

    #[test]
    fn oauthbearer_auth_sends_initial_response_and_refreshes_ehlo() {
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
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();
        let response = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("us,er=one", "token"),
            )
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

    #[test]
    fn oauthbearer_immediate_rejection_marks_connection_broken() {
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
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();
        let error = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("user", "token"),
            )
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

    #[test]
    fn oauthbearer_failed_challenge_sends_cancel_response() {
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
            SmtpConnection::connect(address, None, &ClientId::default(), None, None).unwrap();
        let error = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("user", "token"),
            )
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
}
