#[cfg(unix)]
use std::path::Path;
use std::{
    fmt::{Display, Write as _},
    io::{self, BufRead, BufReader, Read, Write},
    net::{IpAddr, ToSocketAddrs},
    time::Duration,
};

use bifrost_sasl::ScramChannelBinding;
use zeroize::{Zeroize, Zeroizing};

#[cfg(feature = "tracing")]
use super::escape_crlf;
use super::metering::WireMetering;
use super::{
    ClientCodec, ConnectionState, MAX_RESPONSE_BYTES, MAX_RESPONSE_LINE_BYTES, NetworkStream,
    PIPELINING_RECIPIENT_WINDOW, PhasedError, TlsParameters, data_terminator, merge_lmtp_statuses,
    smtp_data_size,
};
use crate::{
    address::{Address, Envelope},
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

/// `try_smtp!` for the phase-typed pipelined driver: aborts the connection and
/// returns a `PhasedError`, which is the only error this driver can produce.
macro_rules! try_phased (
    ($err: expr, $client: ident, $phase: expr) => ({
        match $err {
            Ok(val) => val,
            Err(err) => {
                $client.abort();
                return Err(PhasedError::new($phase, Error::from(err)))
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
    /// Reused storage for serializing individual SMTP commands.
    command_buffer: Zeroizing<String>,
    /// Set once an LMTP final-status drain has run: the connection must not
    /// go back into the pool because stream cleanliness cannot be proven.
    retire: bool,
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
    #[allow(dead_code)]
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
            WireMetering::disabled(),
        )
    }

    // Widened so the pool and transport tests can drive a scripted peer
    // through the public entry points instead of a socket.
    #[cfg(test)]
    pub(in crate::transport::smtp) fn from_transcript(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Protocol,
    ) -> Result<Self, Error> {
        let stream = BufReader::new(NetworkStream::from_transcript(transcript));
        let mut conn = Self {
            stream,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
            command_buffer: Zeroizing::new(String::new()),
            retire: false,
        };
        let _response = conn.read_response()?;
        conn.hello(hello_name)?;
        Ok(conn)
    }

    pub(crate) fn connect_with_protocol<A: ToSocketAddrs>(
        server: A,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<&TlsParameters>,
        local_address: Option<IpAddr>,
        protocol: Protocol,
        metering: WireMetering,
    ) -> Result<SmtpConnection, Error> {
        let mut stream = NetworkStream::connect(server, timeout, tls_parameters, local_address)?;
        // Installed on the dialed stream rather than threaded into the
        // dialer: the TCP/TLS handshake is not the account's traffic.
        stream.set_metering(metering);
        let stream = BufReader::new(stream);
        let mut conn = SmtpConnection {
            stream,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
            command_buffer: Zeroizing::new(String::new()),
            retire: false,
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
        metering: WireMetering,
    ) -> Result<SmtpConnection, Error> {
        let mut stream = NetworkStream::connect_unix(path, timeout)?;
        stream.set_metering(metering);
        let stream = BufReader::new(stream);
        let mut conn = SmtpConnection {
            stream,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
            command_buffer: Zeroizing::new(String::new()),
            retire: false,
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

        let (mail, recipients) = build_transaction_commands(envelope, mail_options, &rcpt_options)?;

        if self.server_info().supports_pipelining() {
            return self.send_pipelined(email, mail, recipients);
        }

        try_smtp!(self.command(mail), self, SmtpCommandPhase::MailFrom);

        for recipient in recipients {
            try_smtp!(self.command(recipient), self, SmtpCommandPhase::RcptTo);
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

        let (mail, recipients) = build_transaction_commands(envelope, mail_options, &rcpt_options)?;

        try_smtp!(self.command(mail), self, SmtpCommandPhase::MailFrom);

        for recipient in recipients {
            try_smtp!(self.command(recipient), self, SmtpCommandPhase::RcptTo);
        }

        let result = try_smtp!(self.message_bdat(email), self, SmtpCommandPhase::BdatBody);
        Ok(result)
    }

    /// Phase-stamping funnel for the pipelined driver.
    ///
    /// `send_pipelined_inner` cannot return an undecorated error: its error
    /// type is `PhasedError`, which has no `From<Error>` conversion, so `?`
    /// on a plain SMTP result does not compile there. This is the one place
    /// that turns a boundary failure back into an `Error`.
    fn send_pipelined(
        &mut self,
        email: &[u8],
        mail: Mail,
        recipients: Vec<Rcpt>,
    ) -> Result<Response, Error> {
        self.send_pipelined_inner(email, mail, recipients)
            .map_err(PhasedError::into_error)
    }

    fn send_pipelined_inner(
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
            let write_phase = if window_index == 0 {
                SmtpCommandPhase::MailFrom
            } else {
                SmtpCommandPhase::RcptTo
            };
            try_phased!(self.write(commands.as_bytes()), self, write_phase);
            try_phased!(self.stream.get_ref().state().verify(), self, write_phase);
            self.stream.get_mut().set_state(ConnectionState::Broken);

            if window_index == 0 {
                let mail_response = try_phased!(
                    self.read_response_inner(true, false),
                    self,
                    SmtpCommandPhase::MailFrom
                );
                if !mail_response.is_positive() {
                    for _ in window {
                        try_phased!(
                            self.read_response_inner(true, false),
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
                    self.read_response_inner(true, false),
                    self,
                    SmtpCommandPhase::RcptTo
                );
                if failure.is_none() && !response.is_positive() {
                    failure = Some(response);
                }
            }
            try_phased!(self.finish_reply_group(), self, SmtpCommandPhase::RcptTo);
            if let Some(response) = failure {
                self.reset_transaction();
                return Err(PhasedError::new(
                    SmtpCommandPhase::RcptTo,
                    error::status(response),
                ));
            }
        }

        let data_response = try_phased!(
            self.command_accepting_status(Data),
            self,
            SmtpCommandPhase::DataCommand
        );
        if !data_response.is_positive() {
            self.reset_transaction();
            return Err(PhasedError::new(
                SmtpCommandPhase::DataCommand,
                error::status(data_response),
            ));
        }

        let result = try_phased!(self.message(email), self, SmtpCommandPhase::DataBody);
        Ok(result)
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

        let (mail, recipients) = build_transaction_commands(envelope, mail_options, &rcpt_options)?;

        try_smtp!(self.command(mail), self, SmtpCommandPhase::MailFrom);

        let mut recipient_statuses = Vec::with_capacity(recipients.len());
        let mut accepted_recipients = 0;

        for recipient in recipients {
            let response = try_smtp!(
                self.command_accepting_status(recipient),
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
            self.reset_transaction();
            return Ok(rejected);
        }

        try_smtp!(self.command(Data), self, SmtpCommandPhase::DataCommand);
        let delivery_statuses = try_smtp!(
            self.message_lmtp(email, accepted_recipients),
            self,
            SmtpCommandPhase::LmtpFinalStatus
        );

        merge_lmtp_statuses(recipient_statuses, delivery_statuses)
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

        let (mail, recipients) = build_transaction_commands(envelope, mail_options, &rcpt_options)?;

        try_smtp!(self.command(mail), self, SmtpCommandPhase::MailFrom);

        let mut recipient_statuses = Vec::with_capacity(recipients.len());
        let mut accepted_recipients = 0;

        for recipient in recipients {
            let response = try_smtp!(
                self.command_accepting_status(recipient),
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
            self.reset_transaction();
            return Ok(rejected);
        }

        let delivery_statuses = try_smtp!(
            self.message_lmtp_bdat(email, accepted_recipients),
            self,
            SmtpCommandPhase::LmtpFinalStatus
        );

        merge_lmtp_statuses(recipient_statuses, delivery_statuses)
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
            return self.send_smtp_batch_pipelined(email, mail_cmd, rcpt_cmds, progress);
        }

        // Before DATA, no message content can have reached the peer,
        // regardless of whether the failed operation was a write or a reply
        // drain. Negative replies remain `Acknowledged`.
        // `command_accepting_status` keeps a negative reply as an `Ok`
        // response instead of folding it into a transport-shaped `Err`.
        match self.command_accepting_status(mail_cmd) {
            Ok(resp) if resp.is_positive() => {}
            Ok(resp) => {
                self.abort();
                return Err((
                    error::status(resp)
                        .with_attempt(SmtpTransmissionState::Acknowledged)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
            Err(e) => {
                self.abort();
                return Err((
                    e.with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
        }

        for (i, rcpt) in rcpt_cmds.into_iter().enumerate() {
            match self.command_accepting_status(rcpt) {
                Ok(resp) if resp.is_positive() => progress.record_rcpt_accepted(i),
                Ok(resp) => progress.record_rcpt_rejected(i, resp),
                Err(e) => {
                    // DATA has not been issued, so preserve received RCPT
                    // answers and mark every remaining recipient `Unsent`.
                    let err_clone = e
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::RcptTo);
                    {
                        let err_for_closure = err_clone.clone();
                        let addr_str = progress.recipients[i].address.clone();
                        use crate::transport::smtp::account_error::{
                            SmtpErrorContext, into_account_error,
                        };
                        progress.mark_unresolved_unsent(|| {
                            into_account_error(
                                err_for_closure.clone(),
                                SmtpErrorContext::send(Protocol::Smtp)
                                    .with_attempt(SmtpTransmissionState::Unsent)
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
            self.reset_transaction();
            return Ok(progress);
        }

        // Send DATA command.
        match self.command_accepting_status(Data) {
            Ok(resp) if resp.is_positive() => {}
            Ok(resp) => {
                // DATA rejected before body: all accepted recipients failed with this response.
                progress.mark_accepted_rejected_with_response(resp);
                self.reset_transaction();
                return Ok(progress);
            }
            Err(e) => {
                // Transport drop during DATA command: outcome for accepted recipients uncertain.
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataCommand),
                    SmtpErrorContext::send(Protocol::Smtp),
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
                    SmtpErrorContext::send(Protocol::Smtp),
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
            if let Err(e) = self.write(commands.as_bytes()) {
                if window_start == 0 {
                    self.abort();
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
                    SmtpErrorContext::send(Protocol::Smtp),
                );
                progress.mark_unresolved_unsent(|| ae.clone());
                self.abort();
                return Ok(progress);
            }

            if window_start == 0 {
                let mail_response = match self.read_response_accepting_status() {
                    Ok(r) => r,
                    Err(e) => {
                        self.abort();
                        return Err((
                            e.with_attempt(SmtpTransmissionState::Unsent)
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
            }

            for i in window_start..window_end {
                match self.read_response_accepting_status() {
                    Ok(resp) if resp.is_positive() => progress.record_rcpt_accepted(i),
                    Ok(resp) => progress.record_rcpt_rejected(i, resp),
                    Err(e) => {
                        // DATA has not been issued, so remaining recipients
                        // are retryable `Unsent` failures.
                        use crate::transport::smtp::account_error::{
                            SmtpErrorContext, into_account_error,
                        };
                        let ae = into_account_error(
                            e.with_attempt(SmtpTransmissionState::Unsent)
                                .with_phase(SmtpCommandPhase::RcptTo),
                            SmtpErrorContext::send(Protocol::Smtp),
                        );
                        progress.mark_unresolved_unsent(|| ae.clone());
                        self.abort();
                        return Ok(progress);
                    }
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
            self.reset_transaction();
            return Ok(progress);
        }

        let data_response = match self.command_accepting_status(Data) {
            Ok(r) => r,
            Err(e) => {
                use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
                let ae = into_account_error(
                    e.with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataCommand),
                    SmtpErrorContext::send(Protocol::Smtp),
                );
                let ae2 = ae.clone();
                progress.mark_uncertain_unresolved(|| ae2.clone());
                self.abort();
                return Ok(progress);
            }
        };

        if !data_response.is_positive() {
            // DATA negative: all accepted recipients failed with this response.
            progress.mark_accepted_rejected_with_response(data_response);
            self.reset_transaction();
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
                    SmtpErrorContext::send(Protocol::Smtp),
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
        match self.command_accepting_status(mail_cmd) {
            Ok(resp) if resp.is_positive() => {}
            Ok(resp) => {
                self.abort();
                return Err((
                    error::status(resp)
                        .with_attempt(SmtpTransmissionState::Acknowledged)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
            Err(e) => {
                self.abort();
                return Err((
                    e.with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress,
                ));
            }
        }

        let mut accepted_count = 0usize;
        for (i, rcpt) in rcpt_cmds.into_iter().enumerate() {
            match self.command_accepting_status(rcpt) {
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
                        SmtpErrorContext::send(Protocol::Lmtp),
                    );
                    progress.mark_unresolved_unsent(|| ae.clone());
                    self.abort();
                    return Ok(progress);
                }
            }
        }

        if accepted_count == 0 {
            self.reset_transaction();
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
                self.reset_transaction();
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
                    SmtpErrorContext::send(Protocol::Lmtp),
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
                SmtpErrorContext::send(Protocol::Lmtp),
            );
            let ae2 = ae.clone();
            progress.mark_uncertain_unresolved(|| ae2.clone());
            self.abort();
            return Ok(progress);
        }
        progress.set_body_finished();

        // Read one final LMTP status per accepted recipient.
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
            match self.read_response_inner(true, false) {
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
                        SmtpErrorContext::send(Protocol::Lmtp),
                    );
                    let ae2 = ae.clone();
                    progress.mark_uncertain_unresolved(|| ae2.clone());
                    self.abort();
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
    fn write_body(&mut self, email: &[u8]) -> Result<(), Error> {
        let mut codec = ClientCodec::new();
        let mut out_buf = Vec::with_capacity(email.len());
        codec.encode(email, &mut out_buf);
        self.write(out_buf.as_slice())?;
        self.write(data_terminator(email.ends_with(b"\r\n")))
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
            // Belt and braces at the security boundary. The generic
            // surplus-bytes check inside the reply read normally fires first,
            // but `get_mut()` swaps only the INNER stream and leaves the
            // `BufReader` buffer intact, so any pre-TLS byte still sitting here
            // would be consumed as the first bytes of the TLS session - the
            // classic STARTTLS response-injection. This gate is independent of
            // whichever reply path produced the 220.
            if !self.stream.buffer().is_empty() {
                self.stream.get_mut().set_state(ConnectionState::Broken);
                self.abort();
                return Err(error::parse(
                    "SMTP server sent an unsolicited reply before the STARTTLS upgrade",
                ));
            }
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
            Protocol::Lmtp => {
                let command = Lhlo::new(hello_name.clone())?;
                try_smtp!(self.command(command), self)
            }
            _ => {
                let command = Ehlo::new(hello_name.clone())?;
                try_smtp!(self.command(command), self)
            }
        };
        self.server_info = try_smtp!(ServerInfo::from_response(&response), self);
        Ok(())
    }

    /// Close the current mail transaction, or make the connection
    /// unrecyclable if the server does not positively acknowledge the reset.
    fn reset_transaction(&mut self) {
        match self.command_accepting_status(Rset) {
            Ok(response) if response.is_positive() => {}
            Ok(_) | Err(_) => self.abort(),
        }
    }

    /// Close the connection.
    ///
    /// Unlike the async half this needs no timeout: `Shutdown::Both` on a
    /// blocking socket is a syscall that returns immediately, where the async
    /// half's `poll_shutdown` sends TLS `close_notify` and waits for the
    /// peer's. The state transition is mirrored on both sides so an aborted
    /// connection can never pass `state().verify()` again.
    pub(crate) fn abort(&mut self) {
        self.stream.get_mut().set_state(ConnectionState::Broken);
        let _ = self.stream.get_mut().shutdown(std::net::Shutdown::Both);
    }

    /// Tells if the underlying stream is currently encrypted
    pub(crate) fn is_encrypted(&self) -> bool {
        self.stream.get_ref().is_encrypted()
    }

    /// DER of the peer (server) certificate. See
    /// [`NetworkStream::peer_certificate_der`] for the contract.
    ///
    /// Consumed by `auth`'s `resolve_scram_binding` for SCRAM-PLUS channel
    /// binding.
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

    /// Sends an AUTH command, selecting the strongest compatible mechanism.
    ///
    /// [`password_mechanism`] is SCRAM-aware, applies RFC 5802 Section 6
    /// downgrade protection against the advertised set, and skips a PLUS rung
    /// whose channel binding cannot resolve, falling through to the next safe
    /// rung (the other PLUS hash, then PLAIN). Only binding-unavailability
    /// falls through; a wire-level rejection `?`-propagates and is final, since
    /// walking down to an unbound mechanism after a 535 is the silent downgrade
    /// RFC 5802 Section 6 exists to prevent.
    ///
    /// `tls-server-end-point` is a property of the certificate, not of the
    /// SCRAM hash, so the binding is resolved at most once per authentication
    /// and shared by either PLUS choice.
    pub(crate) fn auth(
        &mut self,
        mechanisms: &[Mechanism],
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        // OAuth credentials never use SCRAM: select the first advertised
        // mechanism from the caller's order and run the legacy encoder path.
        if let Some(mechanism) = oauth_mechanism(mechanisms, &self.server_info, credentials) {
            return self.auth_legacy(mechanism, credentials);
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
                self.auth_scram(chosen, binding, credentials)
            }
            Mechanism::ScramSha1 | Mechanism::ScramSha256 => {
                self.auth_scram(chosen, ScramChannelBinding::None, credentials)
            }
            _ => self.auth_legacy(chosen, credentials),
        }
    }

    /// Resolve the `tls-server-end-point` channel binding for the live
    /// connection: fetch the peer certificate DER (already cached on the TLS
    /// stream, no wire I/O) and hash it. `None` when the DER is absent
    /// (plaintext) or [`bifrost_sasl::tls_server_end_point`] errors (`EdDSA`
    /// leaf. Absence means there is no TLS certificate; malformed or
    /// unsupported certificate algorithms remain typed errors and cannot
    /// silently downgrade authentication.
    fn resolve_scram_binding(&self) -> Result<Option<ScramChannelBinding>, Error> {
        let der = self.peer_certificate_der();
        resolve_scram_binding(der.as_deref())
    }

    /// Run a SCRAM exchange (bound or unbound) as a no-IR `334` challenge walk.
    fn auth_scram(
        &mut self,
        mechanism: Mechanism,
        binding: ScramChannelBinding,
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        let hash = scram_hash(mechanism).expect("auth_scram only called for SCRAM mechanisms");
        let (username, password) = credentials.password_parts()?;
        let mut exchange = ScramExchange::new(hash, binding, username, password.into())?;

        // Bare `AUTH <mechanism>`: SCRAM has no initial response, so
        // `Mechanism::response` is never invoked and `Display` emits the bare
        // command.
        let auth = Auth::new(mechanism, credentials.clone(), None)?;
        let response = try_smtp!(self.command(auth), self, SmtpCommandPhase::Auth);
        // Server's first 334 (empty challenge): send client-first.
        if !response.has_code(334) {
            self.abort();
            return Err(error::status(response).with_phase(SmtpCommandPhase::Auth));
        }
        let response = try_smtp!(
            self.write_auth_continuation(&exchange.client_first()),
            self,
            SmtpCommandPhase::Auth
        );

        // 334 carrying server-first -> client-final. A malformed continuation
        // is a protocol-class parse error (not an auth failure), so it is not
        // Auth-phase tagged, but it must still abort the connection like every
        // other error path in this exchange.
        let server_first = try_smtp!(decode_auth_challenge(&response), self);
        let response = match try_smtp!(exchange.step(&server_first), self, SmtpCommandPhase::Auth) {
            ScramStep::Reply(client_final) => try_smtp!(
                self.write_auth_continuation(&client_final),
                self,
                SmtpCommandPhase::Auth
            ),
            ScramStep::Complete => {
                self.abort();
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
                self.write_auth_continuation(""),
                self,
                SmtpCommandPhase::Auth
            );
            if !final_reply.is_positive() {
                self.abort();
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
            self.abort();
            return Err(error::status(response).with_phase(SmtpCommandPhase::Auth));
        };

        let hello_name = self.hello_name.clone();
        try_smtp!(self.hello(&hello_name), self);
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
    fn write_auth_continuation(&mut self, line: &str) -> Result<Response, Error> {
        let framed = Zeroizing::new(format!("{line}\r\n"));
        self.write(framed.as_bytes())?;
        self.read_response()
    }

    /// The legacy stateless `Auth::new` / `Auth::new_from_response` 334 loop,
    /// for PLAIN / LOGIN / XOAUTH2 / OAUTHBEARER.
    fn auth_legacy(
        &mut self,
        mechanism: Mechanism,
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        // The blocking transport has no async context, so it resolves the
        // OAuth token by polling the source once (see
        // `oauth2_token_blocking`): a `StaticTokenSource` or an
        // already-fresh `OAuthRefresher` resolves immediately; a source
        // needing a network refresh is rejected with a clear error.
        let oauth_token = if matches!(mechanism, Mechanism::Xoauth2 | Mechanism::OAuthBearer) {
            Some(try_smtp!(
                credentials.oauth2_token_blocking(),
                self,
                SmtpCommandPhase::Auth
            ))
        } else {
            None
        };
        let oauth_token = oauth_token.as_ref().map(|(_, token)| token.as_str());

        // Limit challenges to avoid blocking
        let mut challenges = 10;
        // Position of the challenge within the exchange. LOGIN answers by
        // position (username, then password) rather than by prompt text, so
        // this counter is the only thing that selects which credential goes
        // out; it must not be derived from the remaining-challenge budget.
        let mut challenge_index: usize = 0;
        let auth = Auth::new(mechanism, credentials.clone(), oauth_token)?;
        let mut response = try_smtp!(self.command(auth), self, SmtpCommandPhase::Auth);

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
            response = try_smtp!(self.command(continuation), self, SmtpCommandPhase::Auth);
        }

        if challenges == 0 {
            // The server never completed (or rejected) the exchange within the
            // round-trip budget: an auth-exchange failure, routed on the Auth
            // lane (InvalidInput + Auth -> Authorization), not an untagged
            // Protocol(ParseFailed).
            self.abort();
            Err(error::invalid_input("Unexpected number of challenges")
                .with_phase(SmtpCommandPhase::Auth))
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

    // NB: SCRAM continuation decoding lives in the free `decode_auth_challenge`
    // helper below so the sync and async drivers share one base64/UTF-8 path.

    pub(crate) fn message_lmtp(
        &mut self,
        message: &[u8],
        recipients: usize,
    ) -> Result<Vec<Response>, Error> {
        self.message_lmtp_iter(std::iter::once(message), recipients)
    }

    pub(crate) fn message_bdat(&mut self, message: &[u8]) -> Result<Response, Error> {
        self.write_command(Bdat::last(message.len()))?;
        self.write(message)?;
        self.read_response()
    }

    pub(crate) fn message_lmtp_bdat(
        &mut self,
        message: &[u8],
        recipients: usize,
    ) -> Result<Vec<Response>, Error> {
        self.write_command(Bdat::last(message.len()))?;
        self.write(message)?;

        self.stream.get_ref().state().verify()?;
        self.stream.get_mut().set_state(ConnectionState::Broken);
        let mut responses = Vec::with_capacity(recipients);
        for _ in 0..recipients {
            responses.push(self.read_response_inner(true, false)?);
        }

        self.finish_lmtp_final_drain()?;

        self.stream.get_mut().set_state(ConnectionState::Ok);
        Ok(responses)
    }

    /// Sends the message content by consuming an iterator that in its whole represents a message.
    pub(crate) fn message_iter<I, B>(&mut self, message: I) -> Result<Response, Error>
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
            self.write(out_buf.as_slice())?;
        }
        self.write(data_terminator(seen >= 2 && last_two == *b"\r\n"))?;

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
            self.write(out_buf.as_slice())?;
        }
        self.write(data_terminator(seen >= 2 && last_two == *b"\r\n"))?;

        self.stream.get_ref().state().verify()?;
        self.stream.get_mut().set_state(ConnectionState::Broken);
        let mut responses = Vec::with_capacity(recipients);
        for _ in 0..recipients {
            responses.push(self.read_response_inner(true, false)?);
        }

        self.finish_lmtp_final_drain()?;

        self.stream.get_mut().set_state(ConnectionState::Ok);
        Ok(responses)
    }

    /// Sends an SMTP command
    pub(crate) fn command<C: Display>(&mut self, command: C) -> Result<Response, Error> {
        self.write_command(command)?;
        self.read_response()
    }

    fn command_accepting_status<C: Display>(&mut self, command: C) -> Result<Response, Error> {
        self.write_command(command)?;
        self.read_response_accepting_status()
    }

    fn write_command<C: Display>(&mut self, command: C) -> Result<(), Error> {
        self.command_buffer.zeroize();
        write!(&mut self.command_buffer, "{command}")
            .map_err(|_| error::internal("failed to serialize SMTP command"))?;
        let result = Self::write_stream(&mut self.stream, self.command_buffer.as_bytes());
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
    fn write(&mut self, string: &[u8]) -> Result<(), Error> {
        Self::write_stream(&mut self.stream, string)
    }

    fn write_stream(stream: &mut BufReader<NetworkStream>, string: &[u8]) -> Result<(), Error> {
        stream.get_ref().state().verify()?;
        stream.get_mut().set_state(ConnectionState::Broken);

        stream.get_mut().write_all(string).map_err(error::network)?;
        stream.get_mut().flush().map_err(error::network)?;
        stream.get_mut().set_state(ConnectionState::Ok);

        #[cfg(feature = "tracing")]
        tracing::debug!("Wrote {} bytes", string.len());
        Ok(())
    }

    /// Gets the SMTP response
    pub(crate) fn read_response(&mut self) -> Result<Response, Error> {
        self.read_response_inner(false, true)
    }

    fn read_response_accepting_status(&mut self) -> Result<Response, Error> {
        self.read_response_inner(true, true)
    }

    fn read_response_inner(
        &mut self,
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
                limited
                    .read_until(b'\n', &mut line)
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

#[cfg(test)]
mod transcript_tests {
    use crate::{
        address::Envelope,
        transport::smtp::{
            Protocol,
            authentication::{Credentials, Mechanism},
            batch::SmtpBatchRecipient,
            extension::{
                ClientId, DeliverByMode, DsnNotify, DsnReturn, Extension, MailBodyParameter,
                MailParameter,
            },
            test_support::Transcript,
        },
    };
    use bifrost_types::error::BatchItemId;

    use super::{SendOptions, SmtpConnection};

    const HELLO: &str = "EHLO client.example\r\n";

    #[test]
    fn all_recipient_rejection_resets_every_direct_and_batch_transaction() {
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
                SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp)
                    .unwrap();
            let statuses = if chunking {
                connection
                    .send_lmtp_bdat_with_options(&envelope, b"body", &Default::default())
                    .unwrap()
            } else {
                connection.send_lmtp(&envelope, b"body").unwrap()
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
                SmtpConnection::from_transcript(transcript.clone(), &hello, protocol).unwrap();
            let progress = if protocol == Protocol::Smtp {
                connection.send_smtp_batch(
                    Some("sender@example.com".parse().unwrap()),
                    batch,
                    b"body",
                    &Default::default(),
                )
            } else {
                connection.send_lmtp_batch(
                    Some("sender@example.com".parse().unwrap()),
                    batch,
                    b"body",
                    &Default::default(),
                )
            }
            .unwrap();
            assert_eq!(progress.resolve().failed().len(), 1);
            assert!(!connection.has_broken());
            transcript.assert_exhausted();
        }
    }

    #[test]
    fn pipelined_server_rejections_carry_their_command_phase() {
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
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let error = connection.send(&envelope, b"body").unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::MailFrom));

        // RCPT TO rejected.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "250 sender ok\r\n550 recipient rejected\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let error = connection.send(&envelope, b"body").unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::RcptTo));

        // DATA rejected before the body.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "250 sender ok\r\n250 recipient ok\r\n")
            .expect("DATA\r\n", "554 no data\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let error = connection.send(&envelope, b"body").unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::DataCommand));
    }

    #[test]
    fn pipelined_body_failure_carries_data_body_phase() {
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
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();

        let error = connection.send(&envelope, b"body").unwrap_err();

        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::DataBody));
    }

    fn recipients(count: usize) -> Vec<crate::address::Address> {
        (0..count)
            .map(|index| format!("recipient-{index}@example.com").parse().unwrap())
            .collect()
    }

    fn recipient_commands(recipients: &[crate::address::Address]) -> String {
        recipients
            .iter()
            .map(|recipient| format!("RCPT TO:<{recipient}>\r\n"))
            .collect()
    }

    /// RFC 5321 4.1.1.4: the terminator's leading CRLF is the message's own
    /// final CRLF. The blocking writers must reuse an existing one rather than
    /// appending an empty line the sender never wrote, and the chunked writer
    /// must carry the last two bytes across iterator items so a CRLF
    /// straddling a chunk boundary is still recognized.
    #[test]
    fn data_terminator_preserves_exact_message_bytes() {
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
                SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                    .unwrap();

            connection.message(body).unwrap();
            transcript.assert_exhausted();
        }

        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("body\r", "")
            .expect("\n", "")
            .expect(".\r\n", "250 queued\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        connection
            .message_iter([b"body\r".as_slice(), b"\n".as_slice()].into_iter())
            .unwrap();
        transcript.assert_exhausted();

        // `write_body` is the single-buffer writer the LMTP batch path uses,
        // and it must reuse the caller's final CRLF too. The SIZE declaration
        // is pinned alongside it so the RFC 1870 count stays honest.
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect(
                "LHLO client.example\r\n",
                "250-lmtp.example\r\n250 SIZE 1024\r\n",
            )
            .expect(
                "MAIL FROM:<sender@example.com> SIZE=6\r\n",
                "250 sender ok\r\n",
            )
            .expect(
                "RCPT TO:<recipient@example.com>\r\n",
                "250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body\r\n", "")
            .expect(".\r\n", "250 delivered\r\n");
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: "recipient@example.com".parse().unwrap(),
        }];
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();

        let outcome = connection
            .send_lmtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body\r\n",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        assert_eq!(outcome.succeeded().len(), 1);
        transcript.assert_exhausted();

        // `message_lmtp_iter` is the chunked LMTP writer behind `send_lmtp`.
        // It carries the same last-two-bytes state, so a CRLF ending the final
        // chunk is reused rather than duplicated.
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect(
                "RCPT TO:<recipient@example.com>\r\n",
                "250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body\r\n", "")
            .expect(".\r\n", "250 delivered\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();

        let statuses = connection.send_lmtp(&envelope, b"body\r\n").unwrap();

        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].is_positive());
        transcript.assert_exhausted();
    }

    #[test]
    fn pipelining_drains_each_recipient_window_before_writing_the_next() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = recipients(33);
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.push_str(&recipient_commands(&recipients[..32]));
        let mut first_replies = "250 sender ok\r\n".to_owned();
        first_replies.push_str(&"250 recipient ok\r\n".repeat(32));
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        connection.send(&envelope, b"body").unwrap();
        transcript.assert_exhausted();
    }

    #[test]
    fn pipelined_batch_keeps_original_indexes_across_a_window_boundary() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses = recipients(33);
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.push_str(&recipient_commands(&addresses[..32]));
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
        let recipients = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                recipients,
                b"body",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, "item-31");
        assert_eq!(outcome.succeeded().last().unwrap().item.0, "item-32");
        transcript.assert_exhausted();
    }

    /// A later recipient window fails to write. `DATA` was never issued, so no
    /// content can have reached the peer: every still-open recipient must land
    /// in the `failed` lane with `Unsent` evidence, and the RCPT rejection the
    /// server already gave must survive. `uncertain` here would be a false
    /// claim that the message might have been delivered.
    #[test]
    fn later_window_write_failure_reports_unsent_not_uncertain() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses = recipients(33);
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.push_str(&recipient_commands(&addresses[..32]));
        let mut first_replies = "250 sender ok\r\n".to_owned();
        first_replies.push_str(&"250 recipient ok\r\n".repeat(31));
        first_replies.push_str("550 recipient rejected\r\n");
        // No step for the second window: its write fails at the transport.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(first_window, first_replies);
        let batch = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();

        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        assert!(
            outcome.uncertain().is_empty(),
            "DATA was never issued, so nothing may be reported as uncertain"
        );
        assert_eq!(outcome.succeeded().len(), 0);
        assert_eq!(outcome.failed().len(), 33);
        assert_eq!(outcome.failed()[31].item.0, "item-31");
    }

    #[test]
    fn rcpt_reply_read_failure_reports_unsent_not_uncertain() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses: Vec<crate::address::Address> = [
            "accepted@example.com",
            "rejected@example.com",
            "unanswered@example.com",
        ]
        .into_iter()
        .map(str::parse)
        .collect::<Result<_, _>>()
        .unwrap();
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<accepted@example.com>\r\n", "250 accepted\r\n")
            .expect("RCPT TO:<rejected@example.com>\r\n", "550 rejected\r\n")
            .expect("RCPT TO:<unanswered@example.com>\r\n", "");
        let batch = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();

        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        assert!(outcome.uncertain().is_empty());
        assert_eq!(outcome.failed().len(), 3);
        assert_eq!(outcome.failed()[1].item.0, "item-1");
    }

    #[test]
    fn lmtp_drains_one_final_status_per_accepted_recipient() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = vec![
            "first@example.com".parse().unwrap(),
            "second@example.com".parse().unwrap(),
        ];
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
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();
        let statuses = connection.send_lmtp(&envelope, b"body").unwrap();

        assert!(statuses[0].is_positive());
        assert!(!statuses[1].is_positive());
        // Surplus bytes below the read buffer are undetectable without a
        // blocking read, so a completed LMTP drain retires the connection.
        assert!(connection.should_retire());
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }

    #[test]
    fn lmtp_too_few_final_statuses_breaks_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = vec![
            "first@example.com".parse().unwrap(),
            "second@example.com".parse().unwrap(),
        ];
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "250 first delivered\r\n");
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();
        assert!(connection.send_lmtp(&envelope, b"body").is_err());
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[test]
    fn lmtp_too_many_final_statuses_breaks_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = vec![
            "first@example.com".parse().unwrap(),
            "second@example.com".parse().unwrap(),
        ];
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            // One segment: the `BufReader` prefetches the surplus reply along
            // with the last expected one, which is what production sees.
            .expect_coalesced(
                "\r\n.\r\n",
                "250 first delivered\r\n250 second delivered\r\n250 surplus response\r\n",
            );
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();
        let error = connection
            .send_lmtp(&envelope, b"body")
            .expect_err("a surplus final status desynchronizes the stream");
        assert!(
            error.to_string().contains("more final statuses"),
            "expected an honest LMTP final-status error, got: {error}"
        );
        assert!(connection.has_broken());
        assert!(connection.should_retire());
    }

    #[test]
    fn lmtp_batch_surplus_final_status_retires_the_connection_and_keeps_outcomes() {
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
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Lmtp).unwrap();
        let outcome = connection
            .send_lmtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        // Both deliveries are known, so the batch result stands; the stream is
        // what cannot be reused.
        assert_eq!(outcome.succeeded().len(), 2);
        assert!(outcome.uncertain().is_empty());
        assert!(connection.has_broken());
        assert!(connection.should_retire());
    }

    // The tests below replace the socket-listener tests this harness retired.
    // Same behaviors, same assertions, no listener or thread.

    #[test]
    fn abort_closes_without_quit_command() {
        let hello = ClientId::Domain("client.example".to_owned());
        // No step after EHLO: any byte `abort` writes is a transcript failure.
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        connection.abort();
        transcript.assert_exhausted();
    }

    #[test]
    fn peer_certificate_der_is_none_on_plaintext() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();

        assert!(
            connection.peer_certificate_der().is_none(),
            "plaintext connection must have no peer certificate DER"
        );
    }

    #[test]
    fn failed_test_connected_marks_connection_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        // The NOOP probe is scripted, but the peer never answers it.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "");
        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();

        assert!(!connection.test_connected());
        assert!(connection.has_broken());
    }

    #[test]
    fn send_with_options_pipelines_envelope_parameters_in_one_window() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let response = connection
            .send_with_options(&envelope, b"Subject: test\r\n\r\nHello", &options)
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    #[test]
    fn pipelined_send_rsets_when_a_recipient_is_rejected() {
        let hello = ClientId::Domain("client.example".to_owned());
        // DATA is no longer pipelined, so a rejected RCPT is cleaned up with
        // RSET before any DATA command is issued, and the connection stays
        // reusable.
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .is_err()
        );
        assert!(!connection.has_broken());
        assert!(connection.test_connected());
        transcript.assert_exhausted();
    }

    #[test]
    fn explicit_mail_parameters_are_not_duplicated() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let response = connection
            .send_with_options(&envelope, "Subject: test\r\n\r\nHéllo".as_bytes(), &options)
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    #[test]
    fn batch_eai_recipient_requires_smtputf8_before_mail_from() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: crate::address::Address::new_dangerous("üser", "example.com"),
        }];
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let (error, _) = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .expect_err("EAI recipient must be rejected before MAIL FROM");

        assert!(error.to_string().contains("SMTPUTF8"));
        assert!(connection.test_connected());
        transcript.assert_exhausted();
    }

    #[test]
    fn hostile_unchecked_sender_never_reaches_the_wire() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .is_err()
        );
        assert!(
            !connection.has_broken(),
            "a rejected sender must not break the connection"
        );
        assert!(connection.test_connected(), "the connection stays reusable");
        transcript.assert_exhausted();
    }

    #[test]
    fn hostile_unchecked_recipient_leaves_no_open_transaction() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .is_err()
        );
        assert!(
            !connection.has_broken(),
            "a rejection before MAIL FROM must not break the connection"
        );
        assert!(
            connection.test_connected(),
            "the connection must still be reusable, not stranded in a transaction"
        );
        transcript.assert_exhausted();
    }

    #[test]
    fn send_bdat_uses_chunking_without_dot_stuffing() {
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
            // The leading dot is transmitted verbatim: BDAT is length-framed,
            // so dot-stuffing would corrupt the body.
            .expect(message, "250 queued\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let options =
            SendOptions::new().mail_parameter(MailParameter::Body(MailBodyParameter::BinaryMime));

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let response = connection
            .send_bdat_with_options(&envelope, message, &options)
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    #[test]
    fn auth_refreshes_server_info_with_ehlo() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

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
        // The post-AUTH EHLO replaced the capability set wholesale.
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

    #[test]
    fn oauthbearer_auth_sends_initial_response_and_refreshes_ehlo() {
        let hello = ClientId::Domain("client.example".to_owned());
        // GS2 header escaping of the authzid is part of the wire contract.
        let initial = crate::base64::encode("n,a=us=2Cer=3Done,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(
                format!("AUTH OAUTHBEARER {initial}\r\n"),
                "235 authenticated\r\n",
            )
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let response = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("us,er=one", "token"),
            )
            .unwrap();

        assert!(response.has_code(235));
        transcript.assert_exhausted();
    }

    #[test]
    fn oauthbearer_immediate_rejection_marks_connection_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=user,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(
                format!("AUTH OAUTHBEARER {initial}\r\n"),
                "535 rejected\r\n",
            );
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

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
        transcript.assert_exhausted();
    }

    #[test]
    fn oauthbearer_failed_challenge_sends_cancel_response() {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=user,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(format!("AUTH OAUTHBEARER {initial}\r\n"), "334 e30=\r\n")
            // RFC 7628 requires the client to answer a failure challenge with
            // the single `0x01` cancel byte before reading the final reply.
            .expect("AQ==\r\n", "535 rejected\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

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
        transcript.assert_exhausted();
    }

    #[test]
    fn starttls_downgrade_is_refused_without_a_wire_command() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        assert!(!connection.can_starttls());
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(&tls, &hello)
            .expect_err("a server without the STARTTLS capability must not be upgraded");
        assert!(
            error.to_string().contains("STARTTLS is not supported"),
            "expected a capability refusal, got: {error}"
        );

        // The refusal happened before any byte hit the wire: the very next
        // scripted step is NOOP, so a stray STARTTLS write would be rejected.
        connection
            .command(crate::transport::smtp::commands::Noop)
            .unwrap();
        transcript.assert_exhausted();
    }

    #[test]
    fn starttls_refuses_plaintext_bytes_buffered_after_the_reply() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect_coalesced(
                "STARTTLS\r\n",
                "220 go ahead\r\n250 attacker-controlled capabilities\r\n",
            );
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();

        let error = connection
            .starttls(&tls, &hello)
            .expect_err("pre-TLS injected bytes must refuse the upgrade");

        assert!(error.to_string().contains("unsolicited reply"));
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    /// The capability is advertised, so the driver must actually issue
    /// `STARTTLS` and only then attempt the upgrade. The transcript stream is
    /// not a TCP socket, so the upgrade stops at the handshake boundary: that
    /// is exactly as far as a hermetic test can follow, and it still pins the
    /// wire step and its ordering.
    #[test]
    fn advertised_starttls_writes_the_command_before_the_handshake() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "220 ready to start tls\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        assert!(connection.can_starttls());
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(&tls, &hello)
            .expect_err("the in-process transcript stream cannot complete a TLS handshake");
        assert!(
            error.to_string().contains("only supported on TCP"),
            "expected the upgrade to fail at the handshake boundary, got: {error}"
        );
        // The STARTTLS step was consumed, and no post-upgrade EHLO was sent.
        transcript.assert_exhausted();
    }

    /// A server that advertises STARTTLS may still refuse it. The refusal must
    /// surface as the server's reply, and no upgrade may be attempted.
    #[test]
    fn refused_starttls_reply_does_not_upgrade_the_stream() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "454 TLS temporarily unavailable\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(&tls, &hello)
            .expect_err("a refused STARTTLS must not be treated as an upgrade");
        assert!(
            error
                .smtp_response()
                .is_some_and(|response| response.has_code(454)),
            "expected the 454 reply to survive, got: {error}"
        );
        assert!(!connection.is_encrypted());
        transcript.assert_exhausted();
    }

    /// A peer that hangs up in the middle of a multiline reply must produce a
    /// parse failure, not a hang and not a half-parsed capability set.
    #[test]
    fn peer_closing_mid_reply_line_is_a_parse_failure() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect_then_close(HELLO, "250-smtp.example\r\n250 PIPELI");

        let Err(error) = SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp) else {
            panic!("a truncated EHLO reply cannot yield a usable connection");
        };
        assert!(
            error.to_string().contains("incomplete response"),
            "expected an incomplete-response parse error, got: {error}"
        );
    }

    /// The peer closes right after accepting `DATA`. The body write fails, so
    /// the message may or may not have been seen: every recipient must land in
    /// the `uncertain` lane, never `succeeded`.
    #[test]
    fn peer_closing_after_the_data_command_leaves_recipients_uncertain() {
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
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .expect("a mid-body drop is a per-recipient outcome, not a batch-level error")
            .resolve();

        assert!(outcome.succeeded().is_empty());
        assert_eq!(outcome.uncertain().len(), 1);
        assert_eq!(outcome.uncertain()[0].item.0, "item-0");
        assert!(connection.has_broken());
    }

    /// An unsolicited reply coalesced with a requested reply breaks the
    /// connection before it can be mistaken for the next command's answer.
    #[test]
    fn unsolicited_reply_coalesced_with_an_answer_breaks_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect_coalesced("NOOP\r\n", "250 noop ok\r\n421 service closing\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();

        let error = connection
            .command(crate::transport::smtp::commands::Noop)
            .expect_err("the surplus reply must break the connection immediately");
        assert!(
            error.to_string().contains("unsolicited reply"),
            "expected a desynchronization error, got: {error}"
        );
        assert!(connection.has_broken());
    }

    /// DSN RCPT parameters are refused before `MAIL FROM` when the server did
    /// not advertise DSN. The refusal must be local: no envelope byte may hit
    /// the wire, so the transaction can be retried on the same connection.
    #[test]
    fn dsn_rcpt_parameters_are_refused_before_any_envelope_byte() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let error = connection
            .send_with_options(&envelope, b"body", &options)
            .expect_err("DSN parameters must not be emitted to a server without DSN");
        assert!(
            error.to_string().contains("require server DSN support"),
            "expected a local DSN refusal, got: {error}"
        );

        // Nothing was written: the next scripted step is still NOOP.
        connection
            .command(crate::transport::smtp::commands::Noop)
            .unwrap();
        transcript.assert_exhausted();
    }

    /// Recipient-specific parameters are keyed by address. A parameter naming
    /// an address that is not in the batch is a caller bug and must be caught
    /// before `MAIL FROM`, never silently dropped.
    #[test]
    fn batch_rcpt_parameters_for_an_unknown_recipient_fail_before_mail_from() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let (error, progress) = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &options,
            )
            .expect_err("unmatched recipient parameters are a pre-wire input error");
        assert!(
            error.to_string().contains("do not match a batch recipient"),
            "expected an unmatched-recipient refusal, got: {error}"
        );
        let outcome = progress.resolve();
        assert!(outcome.succeeded().is_empty());

        connection
            .command(crate::transport::smtp::commands::Noop)
            .unwrap();
        transcript.assert_exhausted();
    }

    /// Per-recipient DSN parameters override the uniform ones on the matching
    /// RCPT line only, and the batch path emits the same wire shape as the
    /// envelope path. Sequential (non-PIPELINING) peer, so each RCPT line is
    /// pinned as its own write.
    #[test]
    fn batch_rcpt_options_are_emitted_per_recipient_in_order() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &options,
            )
            .expect("the transaction completes")
            .resolve();

        assert_eq!(outcome.succeeded().len(), 2);
        transcript.assert_exhausted();
    }
}
