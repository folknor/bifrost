//! Consumer trait and dispatcher for the new routing architecture.
//!
//! Consumers do NOT route. `classify` routes. Consumers only receive
//! responses that `classify` has already determined are theirs (either
//! solicited or `Either`) and they interpret them.
//!
//! The `Consumer` trait is typed (associated type `Output`).
//! `ConsumerErased` is a blanket-impl wrapper for the pipeline path,
//! which needs `Box<dyn ...>`.

use crate::connection::NotifyFlags;
use crate::connection::helpers::inbox_eq;
use crate::error::Error;
use crate::types::SecretString;
use crate::types::response::{
    Capability, ContinuationRequest, ResponseCode, TaggedResponse, UntaggedResponse, UntaggedStatus,
};
use crate::types::validated::MailboxName;

/// Typed consumer trait for a single command's response stream.
///
/// Not directly object-safe because `finalize` uses `Self::Output`
/// in its return type. `ConsumerErased` provides the object-safe
/// pipeline path via a blanket impl that erases `Output` to
/// `Box<dyn Any + Send>`.
pub(crate) trait Consumer: Send {
    type Output: Send + 'static;

    /// Called by the dispatcher for each untagged response that
    /// `classify` routed to this command. The response is either
    /// `OnlySolicited` or `Either`  -  the dispatcher never delivers
    /// `OnlyUnsolicited` responses here.
    ///
    /// Consumers accumulate. They do not route.
    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    );

    /// Called when the tagged response arrives. Produces the
    /// command's output and optionally returns responses that the
    /// consumer determined were not actually part of its result (for
    /// `Either` cases  -  the dispatcher re-emits them as events).
    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<Self::Output>, Error>;
}

/// Output of [`Consumer::finalize`].
pub(crate) struct Finalized<T> {
    /// The command's typed result.
    pub output: T,
    /// Responses the consumer decided were not actually part of its
    /// solicited result. Dispatcher re-emits these to the event sink.
    /// For most consumers this is empty; for consumers that receive
    /// `Either` responses it may contain the responses the consumer
    /// determined were asynchronous notifications.
    pub reclassified_as_events: Vec<UntaggedResponse>,
}

/// Consumer that handles `+` continuations (RFC 3501 Section7.5).
///
/// Used by AUTHENTICATE, APPEND, and future multi-round SASL.
/// The dispatcher routes continuations to `on_continuation` instead
/// of erroring on unexpected `+`.
pub(crate) trait ContinuationConsumer: Consumer {
    /// Handle a `+` continuation from the server.
    ///
    /// Returns either bytes to write back to the wire, or an abort
    /// signal with an error.
    fn on_continuation(
        &mut self,
        cont: ContinuationRequest,
        ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error>;
}

/// What to do after a `+` continuation is delivered to a
/// [`ContinuationConsumer`].
pub(crate) enum ContinuationReply {
    /// Write these bytes to the wire and continue reading.
    Write(Vec<u8>),
}

/// Read-only view of the connection state the consumer needs.
///
/// Exposes only the fields a consumer is allowed to observe; does
/// NOT expose a reference to `ProtocolState` itself (which would
/// leak the private state module's shape).
pub(crate) struct ConsumerContext<'a> {
    // All fields are pub(in crate::connection)  -  constructed by the
    // dispatcher inside the connection module, read by consumers
    // via the accessor methods below. No field is pub.
    pub(in crate::connection) capabilities: &'a [Capability],
    pub(in crate::connection) enabled: &'a [String],
    pub(in crate::connection) command_target: Option<&'a MailboxName>,
    /// The tag of the in-flight command. Used by consumers that need
    /// to correlate solicited responses (e.g., ESEARCH tag correlation
    /// per RFC 4466 search-correlator).
    pub(in crate::connection) command_tag: &'a str,
}

impl ConsumerContext<'_> {
    /// Cached server capabilities (RFC 3501 Section7.2.1).
    pub(crate) fn capabilities(&self) -> &[Capability] {
        self.capabilities
    }

    /// Successfully `ENABLE`d extensions (RFC 5161 Section3.2).
    pub(crate) fn enabled(&self) -> &[String] {
        self.enabled
    }

    /// The mailbox argument of the current command, if applicable.
    pub(crate) fn command_target(&self) -> Option<&MailboxName> {
        self.command_target
    }

    /// The tag of the in-flight command (RFC 3501 Section2.2.1).
    pub(crate) fn command_tag(&self) -> &str {
        self.command_tag
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  NOOP / CAPABILITY / CHECK / ENABLE / NAMESPACE / IDLE
// ---------------------------------------------------------------------------

/// Consumer for commands that expect no solicited untagged data.
///
/// Validates the tagged response is OK and reclassifies all untagged
/// responses it received as events (they were `Either`  -  ambiguous
/// between solicited and async, but this command has no use for them).
///
/// Used by NOOP (RFC 3501 Section6.1.2), DELETE (RFC 3501 Section6.3.4),
/// RENAME (RFC 3501 Section6.3.5), SUBSCRIBE (RFC 3501 Section6.3.6),
/// UNSUBSCRIBE (RFC 3501 Section6.3.7), and similar.
#[derive(Default)]
pub(crate) struct TaggedOkConsumer {
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for TaggedOkConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // Commands that use TaggedOkConsumer produce no untagged data
        // of their own. Every response `classify` routes here is Either
        // (async state changes). Buffer and reclassify in finalize.
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<()>, Error> {
        tagged.require_ok()?;
        Ok(Finalized {
            output: (),
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for CAPABILITY (RFC 3501 Section6.1.1).
///
/// Accumulates the untagged CAPABILITY response. If the server places
/// capabilities in the tagged OK response code instead (permitted by
/// RFC 3501 Section6.1.1), finalize extracts them from there.
#[derive(Default)]
pub(crate) struct CapabilityConsumer {
    caps: Option<Vec<Capability>>,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for CapabilityConsumer {
    type Output = Vec<Capability>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section6.1.1: the server MUST respond with a CAPABILITY
        // untagged response. Stash it; reclassify everything else.
        if let UntaggedResponse::Capability(ref c) = resp {
            self.caps = Some(c.clone());
        } else {
            self.buffered.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<Capability>>, Error> {
        let tagged = tagged.require_ok()?;

        // RFC 3501 Section6.1.1: capabilities may appear as an untagged
        // response or in the tagged OK response code.
        let caps = if let Some(c) = self.caps {
            c
        } else if let Some(ResponseCode::Capability(c)) = tagged.code {
            c
        } else {
            return Err(Error::Protocol(
                "CAPABILITY OK but no capability data in response \
                 (RFC 3501 Section 6.1.1)"
                    .into(),
            ));
        };

        Ok(Finalized {
            output: caps,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for LOGOUT (RFC 3501 Section6.1.3).
///
/// Tracks whether the mandatory `* BYE` response was received.
/// RFC 3501 Section6.1.3: the server MUST send `* BYE` before the tagged OK.
#[derive(Default)]
pub(crate) struct LogoutConsumer {
    saw_bye: bool,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for LogoutConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section6.1.3: the server MUST send `* BYE` before the
        // tagged OK response to LOGOUT.
        if matches!(
            &resp,
            UntaggedResponse::Status {
                status: UntaggedStatus::Bye,
                ..
            }
        ) {
            self.saw_bye = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<()>, Error> {
        // Check BYE first  -  if the server omitted it, that is a protocol
        // error even when the tagged status is OK.
        if !self.saw_bye {
            return Err(Error::Protocol(
                "LOGOUT: server did not send mandatory BYE \
                 (RFC 3501 Section 6.1.3)"
                    .into(),
            ));
        }
        tagged.require_ok()?;
        Ok(Finalized {
            output: (),
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for CREATE (RFC 3501 Section6.3.3) and CREATE-SPECIAL-USE (RFC 6154 Section3).
///
/// Extracts the optional `MAILBOXID` response code from the tagged OK
/// (RFC 8474 Section4.1). Servers advertising `OBJECTID` MUST include it;
/// others may omit it.
#[derive(Default)]
pub(crate) struct CreateConsumer {
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for CreateConsumer {
    type Output = Option<String>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // CREATE has no untagged responses of its own. Buffer
        // everything for reclassification.
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Option<String>>, Error> {
        let tagged = tagged.require_ok()?;
        // RFC 8474 Section4.1: MAILBOXID in the tagged OK response code.
        let mailbox_id = match tagged.code {
            Some(ResponseCode::MailboxId(id)) => Some(id),
            _ => None,
        };
        Ok(Finalized {
            output: mailbox_id,
            reclassified_as_events: self.buffered,
        })
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  LOGIN / AUTHENTICATE
// ---------------------------------------------------------------------------

use crate::types::response::StatusKind;

/// Validate a tagged response for an authentication command.
///
/// Returns the response on OK, or an [`Error::Auth`] for NO / [`Error::Bad`]
/// for BAD. AUTH commands need a more specific error than the generic
/// [`Error::No`] that [`TaggedResponse::require_ok`] produces.
fn require_ok_auth(tagged: TaggedResponse) -> Result<TaggedResponse, Error> {
    match tagged.status {
        StatusKind::Ok => Ok(tagged),
        StatusKind::No => Err(Error::auth_with_code(tagged.text, tagged.code)),
        StatusKind::Bad => Err(Error::bad_with_code(tagged.text, tagged.code)),
    }
}

/// Consumer for LOGIN (RFC 3501 Section6.2.3).
///
/// LOGIN has no solicited untagged responses of its own. Tracks whether
/// the server provided inline CAPABILITY data (either as an untagged
/// CAPABILITY response or in the tagged OK response code per
/// RFC 3501 Section6.2.3) so the caller knows whether to issue a follow-up
/// CAPABILITY command.
#[derive(Default)]
pub(crate) struct LoginConsumer {
    caps_seen: bool,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for LoginConsumer {
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section6.2.3 / Section7.2.1: the server SHOULD send updated
        // capabilities after authentication. Track it so the caller
        // can skip a follow-up CAPABILITY command.
        if matches!(&resp, UntaggedResponse::Capability(_)) {
            self.caps_seen = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<bool>, Error> {
        let tagged = require_ok_auth(tagged)?;
        let caps_in_tagged = matches!(&tagged.code, Some(ResponseCode::Capability(_)));
        Ok(Finalized {
            output: self.caps_seen || caps_in_tagged,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for AUTHENTICATE PLAIN (RFC 4616 / RFC 3501 Section6.2.2).
///
/// Implements [`ContinuationConsumer`]  -  when the server sends `+`,
/// the consumer replies with the base64-encoded SASL PLAIN credentials.
/// When SASL-IR (RFC 4959) was used, the payload was already part of
/// the AUTHENTICATE command and no continuation is expected.
pub(crate) struct AuthenticatePlainConsumer {
    /// Base64-encoded SASL PLAIN payload (RFC 4616 Section2).
    encoded: SecretString,
    /// Whether the initial response has already been sent (via SASL-IR
    /// in the command, or via a previous continuation reply).
    initial_sent: bool,
    /// Whether capability data arrived during the exchange.
    caps_seen: bool,
    buffered: Vec<UntaggedResponse>,
}

impl AuthenticatePlainConsumer {
    pub(crate) fn new(encoded: SecretString, sasl_ir_used: bool) -> Self {
        Self {
            encoded,
            initial_sent: sasl_ir_used,
            caps_seen: false,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for AuthenticatePlainConsumer {
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section7.2.1: capabilities may arrive as untagged response
        // after authentication state changes.
        if matches!(&resp, UntaggedResponse::Capability(_)) {
            self.caps_seen = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<bool>, Error> {
        let tagged = require_ok_auth(tagged)?;
        let caps_in_tagged = matches!(&tagged.code, Some(ResponseCode::Capability(_)));
        Ok(Finalized {
            output: self.caps_seen || caps_in_tagged,
            reclassified_as_events: self.buffered,
        })
    }
}

impl ContinuationConsumer for AuthenticatePlainConsumer {
    fn on_continuation(
        &mut self,
        _cont: ContinuationRequest,
        _ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        if self.initial_sent {
            // PLAIN is a single-round mechanism (RFC 4616 Section2). A second
            // continuation is a protocol error.
            return Err(Error::Protocol(
                "unexpected continuation after PLAIN initial response \
                 (RFC 4616 Section 2)"
                    .into(),
            ));
        }
        self.initial_sent = true;
        let mut bytes = Vec::with_capacity(self.encoded.len() + 2);
        bytes.extend_from_slice(self.encoded.as_bytes());
        bytes.extend_from_slice(b"\r\n");
        Ok(ContinuationReply::Write(bytes))
    }
}

/// Consumer for AUTHENTICATE XOAUTH2 (Google SASL mechanism).
///
/// Implements [`ContinuationConsumer`]. The first continuation triggers
/// the base64 credential payload. Subsequent continuations are XOAUTH2
/// error challenges  -  the client MUST reply with an empty `\r\n` to let
/// the server send the final tagged NO/BAD.
pub(crate) struct AuthenticateXoauth2Consumer {
    /// Base64-encoded XOAUTH2 payload.
    encoded: SecretString,
    /// Whether the initial credential payload has been sent.
    initial_sent: bool,
    /// Whether capability data arrived during the exchange.
    caps_seen: bool,
    buffered: Vec<UntaggedResponse>,
}

impl AuthenticateXoauth2Consumer {
    pub(crate) fn new(encoded: SecretString, sasl_ir_used: bool) -> Self {
        Self {
            encoded,
            initial_sent: sasl_ir_used,
            caps_seen: false,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for AuthenticateXoauth2Consumer {
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        if matches!(&resp, UntaggedResponse::Capability(_)) {
            self.caps_seen = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<bool>, Error> {
        let tagged = require_ok_auth(tagged)?;
        let caps_in_tagged = matches!(&tagged.code, Some(ResponseCode::Capability(_)));
        Ok(Finalized {
            output: self.caps_seen || caps_in_tagged,
            reclassified_as_events: self.buffered,
        })
    }
}

impl ContinuationConsumer for AuthenticateXoauth2Consumer {
    fn on_continuation(
        &mut self,
        _cont: ContinuationRequest,
        _ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        if self.initial_sent {
            // XOAUTH2 error continuation  -  the server sent a base64-encoded
            // error as `+ <error>`. The client MUST respond with an empty
            // `\r\n` to let the server finish the exchange with a tagged
            // NO/BAD (Google XOAUTH2 spec, non-IETF).
            Ok(ContinuationReply::Write(b"\r\n".to_vec()))
        } else {
            // First continuation  -  send the XOAUTH2 credential payload.
            self.initial_sent = true;
            let mut bytes = Vec::with_capacity(self.encoded.len() + 2);
            bytes.extend_from_slice(self.encoded.as_bytes());
            bytes.extend_from_slice(b"\r\n");
            Ok(ContinuationReply::Write(bytes))
        }
    }
}

/// Consumer for AUTHENTICATE CRAM-MD5.
pub(crate) struct AuthenticateCramMd5Consumer {
    user: String,
    pass: SecretString,
    response_sent: bool,
    caps_seen: bool,
    buffered: Vec<UntaggedResponse>,
}

impl AuthenticateCramMd5Consumer {
    pub(crate) fn new(user: String, pass: SecretString) -> Self {
        Self {
            user,
            pass,
            response_sent: false,
            caps_seen: false,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for AuthenticateCramMd5Consumer {
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        if matches!(&resp, UntaggedResponse::Capability(_)) {
            self.caps_seen = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<bool>, Error> {
        let tagged = require_ok_auth(tagged)?;
        let caps_in_tagged = matches!(&tagged.code, Some(ResponseCode::Capability(_)));
        Ok(Finalized {
            output: self.caps_seen || caps_in_tagged,
            reclassified_as_events: self.buffered,
        })
    }
}

impl ContinuationConsumer for AuthenticateCramMd5Consumer {
    fn on_continuation(
        &mut self,
        cont: ContinuationRequest,
        _ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        if self.response_sent {
            return Err(Error::Protocol(
                "unexpected continuation after CRAM-MD5 response".into(),
            ));
        }
        self.response_sent = true;
        let encoded = cram_md5_response(&self.user, &self.pass, &cont.data)?;
        let mut bytes = Vec::with_capacity(encoded.len() + 2);
        bytes.extend_from_slice(encoded.as_bytes());
        bytes.extend_from_slice(b"\r\n");
        Ok(ContinuationReply::Write(bytes))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScramMechanism {
    Sha1,
    Sha256,
}

impl ScramMechanism {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Sha1 => "SCRAM-SHA-1",
            Self::Sha256 => "SCRAM-SHA-256",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScramState {
    SendClientFirst,
    AwaitServerFirst,
    AwaitServerFinal,
    Done,
}

/// Consumer for SCRAM-SHA-1 and SCRAM-SHA-256.
pub(crate) struct AuthenticateScramConsumer {
    mechanism: ScramMechanism,
    pass: SecretString,
    client_nonce: String,
    client_first_bare: String,
    state: ScramState,
    expected_server_signature: Option<Vec<u8>>,
    caps_seen: bool,
    buffered: Vec<UntaggedResponse>,
}

impl AuthenticateScramConsumer {
    pub(crate) fn new(
        mechanism: ScramMechanism,
        user: String,
        pass: SecretString,
        nonce: String,
        sasl_ir_used: bool,
    ) -> Self {
        let client_first_bare = format!("n={},r={nonce}", scram_escape_username(&user));
        Self {
            mechanism,
            pass,
            client_nonce: nonce,
            client_first_bare,
            state: if sasl_ir_used {
                ScramState::AwaitServerFirst
            } else {
                ScramState::SendClientFirst
            },
            expected_server_signature: None,
            caps_seen: false,
            buffered: Vec::new(),
        }
    }

    pub(crate) fn initial_response(&self) -> SecretString {
        use base64::Engine;

        base64::engine::general_purpose::STANDARD
            .encode(format!("n,,{}", self.client_first_bare).as_bytes())
            .into()
    }
}

impl Consumer for AuthenticateScramConsumer {
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        if matches!(&resp, UntaggedResponse::Capability(_)) {
            self.caps_seen = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<bool>, Error> {
        if self.state != ScramState::Done {
            return Err(Error::Protocol(
                "SCRAM exchange ended before server-final verification".into(),
            ));
        }
        let tagged = require_ok_auth(tagged)?;
        let caps_in_tagged = matches!(&tagged.code, Some(ResponseCode::Capability(_)));
        Ok(Finalized {
            output: self.caps_seen || caps_in_tagged,
            reclassified_as_events: self.buffered,
        })
    }
}

impl ContinuationConsumer for AuthenticateScramConsumer {
    fn on_continuation(
        &mut self,
        cont: ContinuationRequest,
        _ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        match self.state {
            ScramState::SendClientFirst => {
                self.state = ScramState::AwaitServerFirst;
                let encoded = self.initial_response();
                let mut bytes = Vec::with_capacity(encoded.len() + 2);
                bytes.extend_from_slice(encoded.as_bytes());
                bytes.extend_from_slice(b"\r\n");
                Ok(ContinuationReply::Write(bytes))
            }
            ScramState::AwaitServerFirst => {
                let server_first = decode_sasl_continuation(&cont.data)?;
                let (client_final, server_signature) = scram_client_final(
                    self.mechanism,
                    &self.pass,
                    &self.client_nonce,
                    &self.client_first_bare,
                    &server_first,
                )?;
                self.expected_server_signature = Some(server_signature);
                self.state = ScramState::AwaitServerFinal;
                let mut bytes = Vec::with_capacity(client_final.len() + 2);
                bytes.extend_from_slice(client_final.as_bytes());
                bytes.extend_from_slice(b"\r\n");
                Ok(ContinuationReply::Write(bytes))
            }
            ScramState::AwaitServerFinal => {
                let server_final = decode_sasl_continuation(&cont.data)?;
                let expected = self.expected_server_signature.take().ok_or_else(|| {
                    Error::Protocol("SCRAM server signature missing from client state".into())
                })?;
                verify_scram_server_final(&server_final, &expected)?;
                self.state = ScramState::Done;
                Ok(ContinuationReply::Write(b"\r\n".to_vec()))
            }
            ScramState::Done => Err(Error::Protocol(
                "unexpected continuation after SCRAM server-final message".into(),
            )),
        }
    }
}

fn cram_md5_response(user: &str, pass: &str, challenge: &str) -> Result<SecretString, Error> {
    use base64::Engine;
    use hmac::Mac as _;
    use std::fmt::Write;

    let challenge = base64::engine::general_purpose::STANDARD
        .decode(challenge.trim())
        .map_err(|e| Error::Protocol(format!("invalid CRAM-MD5 challenge: {e}")))?;
    let mut mac = <hmac::Hmac<md5::Md5> as hmac::Mac>::new_from_slice(pass.as_bytes())
        .map_err(|e| Error::Protocol(format!("invalid CRAM-MD5 key: {e}")))?;
    mac.update(&challenge);
    let digest = mac.finalize().into_bytes();

    let mut response = String::with_capacity(user.len() + 1 + digest.len() * 2);
    response.push_str(user);
    response.push(' ');
    for byte in digest {
        let _ = write!(response, "{byte:02x}");
    }

    Ok(base64::engine::general_purpose::STANDARD
        .encode(response.as_bytes())
        .into())
}

fn decode_sasl_continuation(data: &str) -> Result<String, Error> {
    use base64::Engine;

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data.trim())
        .map_err(|e| Error::Protocol(format!("invalid base64 SASL continuation: {e}")))?;
    String::from_utf8(bytes)
        .map_err(|e| Error::Protocol(format!("SASL continuation was not UTF-8: {e}")))
}

fn scram_escape_username(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

fn scram_client_final(
    mechanism: ScramMechanism,
    pass: &str,
    client_nonce: &str,
    client_first_bare: &str,
    server_first: &str,
) -> Result<(SecretString, Vec<u8>), Error> {
    use base64::Engine;

    if scram_field(server_first, 'm').is_some() {
        return Err(Error::Protocol(
            "SCRAM mandatory extension field is not supported".into(),
        ));
    }
    let server_nonce = scram_field(server_first, 'r')
        .ok_or_else(|| Error::Protocol("SCRAM server-first message missing nonce".into()))?;
    if !server_nonce.starts_with(client_nonce) {
        return Err(Error::Protocol(
            "SCRAM server nonce does not extend client nonce".into(),
        ));
    }
    let salt_b64 = scram_field(server_first, 's')
        .ok_or_else(|| Error::Protocol("SCRAM server-first message missing salt".into()))?;
    let salt = base64::engine::general_purpose::STANDARD
        .decode(salt_b64)
        .map_err(|e| Error::Protocol(format!("invalid SCRAM salt: {e}")))?;
    let iterations = scram_field(server_first, 'i')
        .ok_or_else(|| {
            Error::Protocol("SCRAM server-first message missing iteration count".into())
        })?
        .parse::<u32>()
        .map_err(|e| Error::Protocol(format!("invalid SCRAM iteration count: {e}")))?;
    if iterations == 0 {
        return Err(Error::Protocol(
            "SCRAM iteration count must be greater than zero".into(),
        ));
    }

    let client_final_without_proof = format!("c=biws,r={server_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");
    let (proof, server_signature) = scram_proof_and_server_signature(
        mechanism,
        pass.as_bytes(),
        &salt,
        iterations,
        &auth_message,
    )?;
    let proof = zeroize::Zeroizing::new(base64::engine::general_purpose::STANDARD.encode(proof));
    let client_final =
        zeroize::Zeroizing::new(format!("{client_final_without_proof},p={}", proof.as_str()));
    Ok((
        base64::engine::general_purpose::STANDARD
            .encode(client_final.as_bytes())
            .into(),
        server_signature,
    ))
}

fn scram_field(message: &str, key: char) -> Option<&str> {
    message
        .split(',')
        .find_map(|field| field.strip_prefix(&format!("{key}=")))
}

fn verify_scram_server_final(server_final: &str, expected: &[u8]) -> Result<(), Error> {
    use base64::Engine;

    if let Some(error) = scram_field(server_final, 'e') {
        return Err(Error::auth_with_code(
            format!("SCRAM server error: {error}"),
            None,
        ));
    }
    let verifier = scram_field(server_final, 'v')
        .ok_or_else(|| Error::Protocol("SCRAM server-final message missing verifier".into()))?;
    let actual = base64::engine::general_purpose::STANDARD
        .decode(verifier)
        .map_err(|e| Error::Protocol(format!("invalid SCRAM server verifier: {e}")))?;
    if actual != expected {
        return Err(Error::Protocol(
            "SCRAM server signature verification failed".into(),
        ));
    }
    Ok(())
}

fn scram_proof_and_server_signature(
    mechanism: ScramMechanism,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Vec<u8>, Vec<u8>), Error> {
    match mechanism {
        ScramMechanism::Sha1 => scram_proof_sha1(password, salt, iterations, auth_message),
        ScramMechanism::Sha256 => scram_proof_sha256(password, salt, iterations, auth_message),
    }
}

fn scram_proof_sha1(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Vec<u8>, Vec<u8>), Error> {
    use sha1::Digest;

    let mut salted = [0u8; 20];
    pbkdf2::pbkdf2_hmac::<sha1::Sha1>(password, salt, iterations, &mut salted);
    let client_key = hmac_digest::<hmac::Hmac<sha1::Sha1>>(&salted, b"Client Key")?;
    let stored_key = sha1::Sha1::digest(&client_key);
    let client_signature =
        hmac_digest::<hmac::Hmac<sha1::Sha1>>(&stored_key, auth_message.as_bytes())?;
    let proof = xor_bytes(&client_key, &client_signature);
    let server_key = hmac_digest::<hmac::Hmac<sha1::Sha1>>(&salted, b"Server Key")?;
    let server_signature =
        hmac_digest::<hmac::Hmac<sha1::Sha1>>(&server_key, auth_message.as_bytes())?;
    Ok((proof, server_signature))
}

fn scram_proof_sha256(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Vec<u8>, Vec<u8>), Error> {
    use sha2::Digest;

    let mut salted = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password, salt, iterations, &mut salted);
    let client_key = hmac_digest::<hmac::Hmac<sha2::Sha256>>(&salted, b"Client Key")?;
    let stored_key = sha2::Sha256::digest(&client_key);
    let client_signature =
        hmac_digest::<hmac::Hmac<sha2::Sha256>>(&stored_key, auth_message.as_bytes())?;
    let proof = xor_bytes(&client_key, &client_signature);
    let server_key = hmac_digest::<hmac::Hmac<sha2::Sha256>>(&salted, b"Server Key")?;
    let server_signature =
        hmac_digest::<hmac::Hmac<sha2::Sha256>>(&server_key, auth_message.as_bytes())?;
    Ok((proof, server_signature))
}

fn hmac_digest<M>(key: &[u8], data: &[u8]) -> Result<Vec<u8>, Error>
where
    M: hmac::Mac + hmac::digest::KeyInit,
{
    let mut mac = <M as hmac::Mac>::new_from_slice(key)
        .map_err(|e| Error::Protocol(format!("invalid SCRAM HMAC key: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(a, b)| a ^ b).collect()
}

// ---------------------------------------------------------------------------
// Consumers  -  SELECT / EXAMINE / CLOSE / UNSELECT
// ---------------------------------------------------------------------------

use crate::types::SelectedMailbox;

/// Consumer for SELECT (RFC 3501 Section6.3.1) and EXAMINE (RFC 3501 Section6.3.2).
///
/// Accumulates the mandatory untagged response sequence (EXISTS, RECENT,
/// FLAGS) plus optional response codes (UIDVALIDITY, UIDNEXT,
/// PERMANENTFLAGS, HIGHESTMODSEQ, NOMODSEQ, UNSEEN, MAILBOXID,
/// UIDNOTSTICKY) and QRESYNC data (VANISHED EARLIER, FETCH with changed
/// flags  -  RFC 7162 Section3.2.5.2).
///
/// Unlike [`FetchVanishedConsumer`], this consumer does **not** filter
/// `VANISHED (EARLIER)` responses against the `known-uids` set.
/// RFC 7162 Section 3.2.5.2: during SELECT/EXAMINE with QRESYNC,
/// `known-uids` is a server hint for optimization, not a scoping
/// constraint  -  the server may legitimately return expunged UIDs outside
/// the known set based on its own `seq-match-data` computation.
///
/// `Output` is `Result<SelectedMailbox, Error>` rather than
/// `SelectedMailbox` so that NO / BAD / validation-failure paths can
/// still reclassify accumulated responses as events (the outer
/// `Finalized` always succeeds). The connection method unwraps the
/// inner `Result` for the caller.
pub(crate) struct SelectConsumer {
    /// Whether this is EXAMINE (always read-only) or SELECT.
    is_examine: bool,
    /// All responses delivered by the dispatcher. Partitioned in `finalize`
    /// on the `[CLOSED]` boundary (RFC 7162 Section3.2.11).
    responses: Vec<UntaggedResponse>,
}

impl SelectConsumer {
    pub(crate) fn new(is_examine: bool) -> Self {
        Self {
            is_examine,
            responses: Vec::new(),
        }
    }
}

impl Consumer for SelectConsumer {
    type Output = Result<SelectedMailbox, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // Accumulate everything. Partitioning on [CLOSED] and filtering
        // non-SELECT types happens in finalize.
        self.responses.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<SelectedMailbox, Error>>, Error> {
        match tagged.status {
            // ---- NO / BAD: reclassify all accumulated responses as events.
            // They may be legitimate unsolicited updates for the previously
            // selected mailbox (RFC 3501 Section7).
            StatusKind::No => Ok(Finalized {
                output: Err(Error::no_with_code(tagged.text, tagged.code)),
                reclassified_as_events: self.responses,
            }),
            StatusKind::Bad => Ok(Finalized {
                output: Err(Error::bad_with_code(tagged.text, tagged.code)),
                reclassified_as_events: self.responses,
            }),
            StatusKind::Ok => {
                let read_only = if self.is_examine {
                    true
                } else {
                    // RFC 3501 Section6.3.1: [READ-ONLY] in the tagged OK means the
                    // mailbox was opened read-only despite being SELECT'd.
                    tagged.code.as_ref() == Some(&ResponseCode::ReadOnly)
                };

                // Validate mandatory responses (RFC 3501 Section6.3.1-6.3.2).
                // Only post-[CLOSED] responses count  -  pre-CLOSED belong to
                // the previously selected mailbox (RFC 7162 Section3.2.11).
                let effective = super::selected_mailbox_effective_responses(&self.responses);
                if let Err(e) = validate_select_responses(effective, self.is_examine, ctx) {
                    // Validation failed  -  reclassify everything as events so
                    // legitimate unsolicited updates are not lost.
                    return Ok(Finalized {
                        output: Err(e),
                        reclassified_as_events: self.responses,
                    });
                }

                // Build the SelectedMailbox from accumulated responses. The
                // helper internally handles the [CLOSED] boundary.
                let result = super::build_selected_mailbox(&self.responses, &tagged, read_only);

                // Partition for reclassification. Pre-CLOSED responses are
                // old-mailbox data -> events. Post-CLOSED non-SELECT types
                // and NOTIFY-marked LIST are async notifications -> events.
                let reclassified = reclassify_select_responses(self.responses, ctx);

                Ok(Finalized {
                    output: Ok(result),
                    reclassified_as_events: reclassified,
                })
            }
        }
    }
}

/// Partition responses into events after a successful SELECT/EXAMINE.
///
/// Pre-`[CLOSED]` responses are old-mailbox data and always reclassified.
/// Post-`[CLOSED]` responses are split: SELECT-solicited types (EXISTS,
/// RECENT, FLAGS, VANISHED, FETCH, status codes, and the solicited rev2
/// LIST) are consumed by [`build_selected_mailbox`]; everything else
/// (EXPUNGE, NOTIFY-marked LIST, etc.) is reclassified as events.
fn reclassify_select_responses(
    responses: Vec<UntaggedResponse>,
    ctx: &ConsumerContext,
) -> Vec<UntaggedResponse> {
    let closed_idx = responses.iter().rposition(|r| {
        matches!(
            r,
            UntaggedResponse::Status {
                code: Some(ResponseCode::Closed),
                ..
            }
        )
    });

    let mut reclassified = Vec::new();

    // Track whether the solicited rev2 LIST has been consumed (at most one).
    let mut consumed_select_list = false;

    match closed_idx {
        Some(idx) => {
            let mut owned = responses;
            let post = owned.split_off(idx + 1);
            // Drop the CLOSED marker itself (last element of pre-split).
            owned.pop();
            // Pre-CLOSED: all go to events (old-mailbox notifications).
            reclassified = owned;
            // Post-CLOSED: non-SELECT types go to events.
            for r in post {
                if !is_select_solicited_response(&r, ctx, &mut consumed_select_list) {
                    reclassified.push(r);
                }
            }
        }
        None => {
            for r in responses {
                if !is_select_solicited_response(&r, ctx, &mut consumed_select_list) {
                    reclassified.push(r);
                }
            }
        }
    }

    reclassified
}

/// Check whether a response is one of the types solicited by SELECT/EXAMINE.
///
/// Used to partition post-`[CLOSED]` responses: SELECT types are consumed
/// by [`build_selected_mailbox`]; everything else is reclassified as an
/// event.
///
/// For LIST: only the first unmarked LIST matching the command target is
/// consumed as the mandatory rev2 response (RFC 9051 Section6.3.2). NOTIFY-
/// marked LIST responses (OLDNAME, `\NonExistent`, `\NoAccess`) are
/// always reclassified as events.
///
/// Note: `Vanished { earlier: false }` is consumed here even though
/// `build_selected_mailbox` only extracts `earlier: true`. Non-earlier
/// VANISHED during SELECT is rare (an asynchronous expunge for the new
/// mailbox arriving before the tagged OK) and is silently consumed,
/// consistent with the pre-dispatcher implementation.
fn is_select_solicited_response(
    resp: &UntaggedResponse,
    ctx: &ConsumerContext,
    consumed_select_list: &mut bool,
) -> bool {
    match resp {
        UntaggedResponse::Exists(_)
        | UntaggedResponse::Recent(_)
        | UntaggedResponse::Flags(_)
        | UntaggedResponse::Vanished { .. }
        | UntaggedResponse::Fetch(_)
        | UntaggedResponse::Status { code: Some(_), .. } => true,
        // RFC 9051 Section6.3.2: rev2 SELECT solicits exactly one LIST for
        // the selected mailbox. Consume the first unmarked LIST
        // matching the command target; reclassify NOTIFY-marked LIST.
        UntaggedResponse::List(info) => {
            if *consumed_select_list {
                return false;
            }
            if let Some(target) = ctx.command_target()
                && inbox_eq(target.as_str(), info.name.as_str())
                && !super::is_notify_list_event(info, true)
            {
                *consumed_select_list = true;
                return true;
            }
            false
        }
        _ => false,
    }
}

/// Validate that the mandatory SELECT/EXAMINE responses are present
/// (RFC 3501 Section6.3.1-6.3.2, RFC 9051 Section6.3.2-6.3.3).
///
/// For `IMAP4rev1`: FLAGS, EXISTS, and RECENT are required.
/// For `IMAP4rev2`: FLAGS, EXISTS, and a matching LIST are required.
fn validate_select_responses(
    effective: &[UntaggedResponse],
    is_examine: bool,
    ctx: &ConsumerContext,
) -> Result<(), Error> {
    let is_rev2 = {
        let has_rev2 = ctx.capabilities().contains(&Capability::Imap4Rev2);
        let has_rev1 = ctx.capabilities().contains(&Capability::Imap4Rev1);
        if has_rev2 && has_rev1 {
            // RFC 9051 Section6.3.1: dual-mode requires ENABLE IMAP4REV2.
            ctx.enabled()
                .iter()
                .any(|e| e.eq_ignore_ascii_case("IMAP4REV2"))
        } else {
            has_rev2
        }
    };

    let command_name = if is_examine { "EXAMINE" } else { "SELECT" };
    let section = match (is_rev2, is_examine) {
        (true, false) => "RFC 9051 Section 6.3.2",
        (true, true) => "RFC 9051 Section 6.3.3",
        (false, false) => "RFC 3501 Section 6.3.1",
        (false, true) => "RFC 3501 Section 6.3.2",
    };

    let mut saw_flags = false;
    let mut saw_exists = false;
    let mut saw_recent = false;
    let mut saw_list = false;

    for resp in effective {
        match resp {
            UntaggedResponse::Flags(_) => saw_flags = true,
            UntaggedResponse::Exists(_) => saw_exists = true,
            UntaggedResponse::Recent(_) => saw_recent = true,
            // RFC 9051 Section6.3.2: the solicited SELECT LIST has no NOTIFY
            // markers. Exclude marker-bearing LIST (OLDNAME,
            // \NonExistent, \NoAccess)  -  those are NOTIFY events, not
            // the mandatory solicited response.
            UntaggedResponse::List(info) => {
                if let Some(target) = ctx.command_target()
                    && inbox_eq(target.as_str(), info.name.as_str())
                    && !super::is_notify_list_event(info, true)
                {
                    saw_list = true;
                }
            }
            _ => {}
        }
    }

    if !saw_flags {
        return Err(Error::Protocol(format!(
            "{command_name} completed without the required FLAGS response ({section})"
        )));
    }
    if !saw_exists {
        return Err(Error::Protocol(format!(
            "{command_name} completed without the required EXISTS response ({section})"
        )));
    }
    if is_rev2 {
        if !saw_list {
            return Err(Error::Protocol(format!(
                "{command_name} completed without the required LIST response ({section})"
            )));
        }
    } else if !saw_recent {
        return Err(Error::Protocol(format!(
            "{command_name} completed without the required RECENT response ({section})"
        )));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Consumers  -  APPEND / MULTIAPPEND
// ---------------------------------------------------------------------------

/// Consumer for APPEND (RFC 3501 Section6.3.11).
///
/// APPEND has no solicited untagged responses of its own  -  all
/// untagged data during APPEND is async state changes (EXISTS,
/// EXPUNGE, FETCH, etc.). The result is extracted from the tagged
/// OK response code: `[APPENDUID uidvalidity uid]` (RFC 4315 Section3).
#[derive(Default)]
pub(crate) struct AppendConsumer {
    buffered: Vec<UntaggedResponse>,
    /// APPENDUID response code extracted from an untagged `* OK [APPENDUID ...]`.
    /// Some servers send APPENDUID in an untagged OK rather than in the
    /// tagged OK (RFC 4315 Section3).
    code: Option<ResponseCode>,
}

impl Consumer for AppendConsumer {
    type Output = Option<(u32, u32)>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // APPEND produces no untagged responses of its own
        // (RFC 3501 Section6.3.11). Buffer everything for reclassification.
        match resp {
            // RFC 4315 Section3: some servers send APPENDUID in an untagged OK.
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: code_opt @ Some(ResponseCode::AppendUid { .. }),
                ..
            } if self.code.is_none() => {
                self.code = code_opt;
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Option<(u32, u32)>>, Error> {
        let tagged = tagged.require_ok()?;
        // RFC 4315 Section3: extract APPENDUID from the tagged OK response code.
        // Servers without UIDPLUS may omit it.
        let code = tagged.code.or(self.code);
        let append_uid = match code {
            Some(ResponseCode::AppendUid { uid_validity, uids }) => {
                // Single APPEND  -  extract the first UID from the set.
                uids.first().map(|r| (uid_validity, r.start))
            }
            _ => None,
        };
        Ok(Finalized {
            output: append_uid,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for MULTIAPPEND (RFC 3502).
///
/// Same as [`AppendConsumer`] but extracts multiple UIDs from the
/// `[APPENDUID]` response code. Each UID range is expanded into
/// individual `(uid_validity, uid)` pairs.
#[derive(Default)]
pub(crate) struct MultiAppendConsumer {
    buffered: Vec<UntaggedResponse>,
    /// APPENDUID response code extracted from an untagged `* OK [APPENDUID ...]`.
    /// Some servers send APPENDUID in an untagged OK rather than in the
    /// tagged OK (RFC 4315 Section3).
    code: Option<ResponseCode>,
}

impl Consumer for MultiAppendConsumer {
    type Output = Vec<(u32, u32)>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // MULTIAPPEND produces no untagged responses of its own
        // (RFC 3502 Section3). Buffer everything for reclassification.
        match resp {
            // RFC 4315 Section3: some servers send APPENDUID in an untagged OK.
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: code_opt @ Some(ResponseCode::AppendUid { .. }),
                ..
            } if self.code.is_none() => {
                self.code = code_opt;
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<(u32, u32)>>, Error> {
        let tagged = tagged.require_ok()?;
        // RFC 4315 Section3: for MULTIAPPEND, the uid-set contains one
        // UID per appended message, possibly as ranges.
        let mut results = Vec::new();
        let code = tagged.code.or(self.code);
        if let Some(ResponseCode::AppendUid { uid_validity, uids }) = code {
            for range in &uids {
                if let Some(end) = range.end {
                    // Expand range into individual (uid_validity, uid) pairs.
                    for uid in range.start..=end {
                        results.push((uid_validity, uid));
                    }
                } else {
                    results.push((uid_validity, range.start));
                }
            }
        }
        Ok(Finalized {
            output: results,
            reclassified_as_events: self.buffered,
        })
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  FETCH / STORE
// ---------------------------------------------------------------------------

use crate::types::FetchResponse;

/// Default warn-on-large threshold in bytes (10 MB).
///
/// When the estimated accumulated size of buffered `FetchResponse`s
/// exceeds this limit, a `tracing::warn!` is emitted pointing the
/// caller towards `uid_fetch_streaming`.
pub(crate) const DEFAULT_FETCH_WARN_BYTES: usize = 10 * 1024 * 1024;

/// Rough byte-size estimate for a single [`FetchResponse`].
///
/// Sums the data lengths of body sections and binary sections (the
/// dominant contributors to memory), plus a flat overhead per response
/// for the fixed fields and heap-allocated strings.
pub(crate) fn estimate_fetch_response_bytes(fr: &FetchResponse) -> usize {
    // Flat overhead: seq/uid/flags/envelope/bodystructure/dates/ids etc.
    // Conservative estimate  -  covers the struct itself plus typical
    // small-string heap allocations.
    let mut size: usize = 256;
    for bs in &fr.body_sections {
        size += bs.data.as_ref().map_or(0, Vec::len);
    }
    for bin in &fr.binary_sections {
        size += bin.data.as_ref().map_or(0, Vec::len);
    }
    size
}

/// Consumer for FETCH / UID FETCH (RFC 3501 Section6.4.5, buffering form).
///
/// Accumulates `FETCH` untagged responses into a `Vec<FetchResponse>`.
/// Logs a warning when the accumulated byte estimate exceeds a
/// configurable threshold (default 10 MB) to nudge callers toward the
/// streaming variant (`uid_fetch_streaming`).
pub(crate) struct FetchConsumer {
    fetches: Vec<FetchResponse>,
    /// Non-FETCH responses routed here (classified as `Either`).
    buffered: Vec<UntaggedResponse>,
    /// Running byte-size estimate of accumulated FETCH data.
    estimated_bytes: usize,
    /// Threshold at which to emit a warn-on-large log.
    warn_threshold: usize,
    /// Hard caller-supplied memory budget.
    hard_limit: Option<usize>,
    /// Estimated size observed when the hard limit was first crossed.
    limit_exceeded_at: Option<usize>,
    /// Whether the warning has already been emitted (log once).
    warned: bool,
}

impl FetchConsumer {
    pub(crate) fn new() -> Self {
        Self {
            fetches: Vec::new(),
            buffered: Vec::new(),
            estimated_bytes: 0,
            warn_threshold: DEFAULT_FETCH_WARN_BYTES,
            hard_limit: None,
            limit_exceeded_at: None,
            warned: false,
        }
    }

    pub(crate) fn with_limit(limit: usize) -> Self {
        Self {
            hard_limit: Some(limit),
            ..Self::new()
        }
    }
}

impl Consumer for FetchConsumer {
    type Output = Vec<FetchResponse>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section7.4.2: FETCH responses are the solicited data
        // for FETCH/UID FETCH commands.
        if let UntaggedResponse::Fetch(fr) = resp {
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_add(estimate_fetch_response_bytes(&fr));
            if let Some(limit) = self.hard_limit
                && self.estimated_bytes > limit
            {
                self.limit_exceeded_at.get_or_insert(self.estimated_bytes);
                return;
            }
            if !self.warned && self.estimated_bytes > self.warn_threshold {
                tracing::warn!(
                    estimated_bytes = self.estimated_bytes,
                    threshold = self.warn_threshold,
                    "FETCH response buffer exceeds {} MB  -  consider \
                     uid_fetch_streaming for large result sets",
                    self.warn_threshold / (1024 * 1024),
                );
                self.warned = true;
            }
            self.fetches.push(*fr);
        } else {
            // Non-FETCH responses (EXISTS, EXPUNGE, FLAGS, etc.)
            // classified as Either  -  reclassify as events.
            self.buffered.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<FetchResponse>>, Error> {
        tagged.require_ok()?;
        if let Some(estimated) = self.limit_exceeded_at {
            return Err(Error::FetchLimit {
                estimated,
                limit: self.hard_limit.expect("limit exceeded requires hard limit"),
            });
        }
        Ok(Finalized {
            output: self.fetches,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Streaming consumer for FETCH / UID FETCH (RFC 3501 Section6.4.5).
///
/// Instead of buffering all `FETCH` responses into a `Vec`, pushes each
/// one through an `mpsc::UnboundedSender` as it arrives. The dispatcher keeps
/// reading until the tagged OK regardless of whether the receiver is
/// still alive  -  this keeps the IMAP stream consistent.
///
/// Non-FETCH responses classified as `Either` are buffered and returned
/// in `finalize` for the dispatcher to re-emit as events.
pub(crate) struct StreamingFetchConsumer {
    tx: tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>,
    /// Buffer for ambiguous responses the dispatcher routed here but
    /// that finalize will re-emit as events.
    ambiguous_buffer: Vec<UntaggedResponse>,
}

impl StreamingFetchConsumer {
    pub(crate) fn new(
        tx: tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>,
    ) -> Self {
        Self {
            tx,
            ambiguous_buffer: Vec::new(),
        }
    }
}

impl Consumer for StreamingFetchConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section7.4.2: FETCH responses are the solicited data
        // for FETCH/UID FETCH commands.
        if let UntaggedResponse::Fetch(fr) = resp {
            // If the receiver is dropped, discard the response but keep
            // reading until the tagged OK to preserve stream consistency.
            let _ = self.tx.send(Ok(*fr));
        } else {
            // Non-FETCH response routed to us  -  ambiguous.
            self.ambiguous_buffer.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<()>, Error> {
        tagged.require_ok()?;
        // Drop self.tx by consuming self  -  signals end of stream.
        Ok(Finalized {
            output: (),
            reclassified_as_events: self.ambiguous_buffer,
        })
    }
}

use crate::types::StoreResult;

/// Consumer for STORE / UID STORE (RFC 3501 Section6.4.6, RFC 7162 Section3.1.3).
///
/// Accumulates the implicit FETCH responses that non-`.SILENT` STORE
/// operations produce (RFC 3501 Section6.4.6: "the server SHOULD send an
/// untagged FETCH response for each message whose flags were updated").
/// Also extracts the tagged OK response code, which may contain
/// `[MODIFIED ...]` when UNCHANGEDSINCE was used (RFC 7162 Section3.1.3).
pub(crate) struct StoreConsumer {
    fetches: Vec<FetchResponse>,
    /// Non-FETCH responses routed here (classified as `Either`).
    buffered: Vec<UntaggedResponse>,
}

impl StoreConsumer {
    pub(crate) fn new() -> Self {
        Self {
            fetches: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for StoreConsumer {
    type Output = StoreResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section6.4.6: STORE returns implicit FETCH responses
        // with updated flags for each message whose flags were changed.
        // `.SILENT` operations suppress these.
        if let UntaggedResponse::Fetch(fr) = resp {
            self.fetches.push(*fr);
        } else {
            self.buffered.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<StoreResult>, Error> {
        let tagged = tagged.require_ok()?;
        // RFC 7162 Section3.1.3: preserve [MODIFIED sequence-set] from
        // tagged OK when UNCHANGEDSINCE was used.
        Ok(Finalized {
            output: StoreResult {
                fetches: self.fetches,
                code: tagged.code,
            },
            reclassified_as_events: self.buffered,
        })
    }
}

use crate::types::UidRange;
use crate::types::validated::ParsedUidSet;

/// Consumer for UID FETCH with VANISHED modifier
/// (RFC 7162 Section3.2.6).
///
/// Accumulates both `FETCH` responses and `VANISHED (EARLIER)`
/// responses. Plain `VANISHED` (earlier: false) are unsolicited
/// real-time expunge notifications and are reclassified as events.
///
/// When `requested_set` is `Some`, `VANISHED (EARLIER)` UIDs are
/// defensively filtered to only include UIDs within the requested
/// set. RFC 7162 Section 3.2.6 says the server SHOULD limit these
/// responses, but non-conformant servers (e.g. Stalwart) may return
/// UIDs outside the requested set. When `requested_set` is `None`
/// (because the sequence set contained `$`, an unresolvable search
/// result reference per RFC 5182), filtering is skipped.
pub(crate) struct FetchVanishedConsumer {
    fetches: Vec<FetchResponse>,
    vanished_uids: Vec<UidRange>,
    /// Parsed requested UID set for defensive filtering of
    /// `VANISHED (EARLIER)` responses (RFC 7162 Section 3.2.6).
    /// `None` when the sequence set contains `$` (RFC 5182).
    requested_set: Option<ParsedUidSet>,
    /// Count of individual UIDs dropped by filtering.
    dropped_vanished_count: usize,
    /// Non-solicited responses (classified as `Either`).
    buffered: Vec<UntaggedResponse>,
    /// Running byte-size estimate for the warn-on-large check.
    estimated_bytes: usize,
    warn_threshold: usize,
    warned: bool,
}

impl FetchVanishedConsumer {
    /// Create a new consumer with an optional parsed UID set for
    /// defensive filtering of `VANISHED (EARLIER)` responses.
    ///
    /// Pass `Some(set)` to filter out-of-set UIDs per RFC 7162
    /// Section 3.2.6. Pass `None` when the sequence set contains `$`
    /// (RFC 5182 search result reference) and cannot be parsed.
    pub(crate) fn new(requested_set: Option<ParsedUidSet>) -> Self {
        Self {
            fetches: Vec::new(),
            vanished_uids: Vec::new(),
            requested_set,
            dropped_vanished_count: 0,
            buffered: Vec::new(),
            estimated_bytes: 0,
            warn_threshold: DEFAULT_FETCH_WARN_BYTES,
            warned: false,
        }
    }
}

impl Consumer for FetchVanishedConsumer {
    type Output = (Vec<FetchResponse>, Vec<UidRange>);

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 7162 Section3.2.6: FETCH responses for messages whose
            // flags changed since the given mod-sequence.
            UntaggedResponse::Fetch(fr) => {
                self.estimated_bytes += estimate_fetch_response_bytes(&fr);
                if !self.warned && self.estimated_bytes > self.warn_threshold {
                    tracing::warn!(
                        estimated_bytes = self.estimated_bytes,
                        threshold = self.warn_threshold,
                        "FETCH response buffer exceeds {} MB  -  consider \
                         uid_fetch_streaming for large result sets",
                        self.warn_threshold / (1024 * 1024),
                    );
                    self.warned = true;
                }
                self.fetches.push(*fr);
            }
            // RFC 7162 Section3.2.6: VANISHED (EARLIER) lists UIDs expunged
            // since the given mod-sequence. Defensively filter to only
            // include UIDs within the requested set  -  non-conformant
            // servers may return UIDs outside it.
            UntaggedResponse::Vanished {
                earlier: true,
                uids,
            } => {
                if let Some(ref set) = self.requested_set {
                    let (filtered, dropped) = set.intersect_uid_ranges(&uids);
                    self.dropped_vanished_count += dropped;
                    self.vanished_uids.extend(filtered);
                } else {
                    // No parsed set ($ in sequence set)  -  accept all.
                    self.vanished_uids.extend(uids);
                }
            }
            // Plain VANISHED (earlier: false) and other responses are
            // unsolicited  -  reclassify as events.
            _ => {
                self.buffered.push(resp);
            }
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<(Vec<FetchResponse>, Vec<UidRange>)>, Error> {
        tagged.require_ok()?;
        if self.dropped_vanished_count > 0 {
            tracing::debug!(
                dropped = self.dropped_vanished_count,
                "filtered out-of-set VANISHED (EARLIER) UIDs per RFC 7162 Section 3.2.6",
            );
        }
        Ok(Finalized {
            output: (self.fetches, self.vanished_uids),
            reclassified_as_events: self.buffered,
        })
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  LIST / LSUB / LIST-EXTENDED / LIST-STATUS / STATUS
// ---------------------------------------------------------------------------

use crate::types::{MailboxInfo, StatusItem, StatusResult};

/// Consumer for LIST (RFC 3501 Section6.3.8).
///
/// Accumulates solicited LIST responses and classifies NOTIFY marker-
/// bearing LIST responses (OLDNAME, `\NonExistent`, `\NoAccess`) as
/// events to be re-emitted by the dispatcher (RFC 5465 Section5.4).
///
/// The `notify_snapshot` parameter on each `on_response` call provides
/// the per-response NOTIFY state  -  after a mid-stream
/// `[NOTIFICATIONOVERFLOW]`, `apply_side_effects` clears the notify
/// flags, so subsequent snapshots have `list = false`. This replaces
/// the manual `first_notification_overflow_index` approach used by the
/// old hand-rolled loop.
///
/// `Output` is `Result<Vec<MailboxInfo>, Error>` so that NOTIFY marker
/// events can be reclassified even when the tagged response is NO/BAD.
pub(crate) struct ListConsumer {
    /// Solicited LIST entries (marker-less, accumulated on success).
    mailboxes: Vec<MailboxInfo>,
    /// NOTIFY marker-bearing LIST entries  -  reclassified as events in
    /// `finalize` regardless of tagged status.
    marker_events: Vec<UntaggedResponse>,
    /// Non-LIST responses routed here via `Either` classification.
    buffered: Vec<UntaggedResponse>,
}

impl ListConsumer {
    pub(crate) fn new() -> Self {
        Self {
            mailboxes: Vec::new(),
            marker_events: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ListConsumer {
    type Output = Result<Vec<MailboxInfo>, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::List(info) => {
                // RFC 5465 Section5.4: when NOTIFY LIST events are registered,
                // marker-bearing LIST responses are NOTIFY events. The
                // per-response notify_snapshot handles mid-stream overflow
                // (RFC 5465 Section5.8)  -  after overflow, snapshot.list is false.
                if notify_snapshot.list && super::is_notify_list_event(&info, true) {
                    self.marker_events.push(UntaggedResponse::List(info));
                } else {
                    self.mailboxes.push(info);
                }
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<Vec<MailboxInfo>, Error>>, Error> {
        // Marker events are reclassified as events on both success and
        // failure paths. Non-LIST buffered responses are also reclassified.
        let mut reclassified = self.marker_events;
        reclassified.extend(self.buffered);

        match tagged.require_ok() {
            Ok(_) => Ok(Finalized {
                output: Ok(self.mailboxes),
                reclassified_as_events: reclassified,
            }),
            Err(e) => {
                // On failure: marker-less LIST may be the failed solicited
                // result  -  drop it rather than leaking as a notification
                // (RFC 5465 Section5.4). Marker events are still emitted.
                Ok(Finalized {
                    output: Err(e),
                    reclassified_as_events: reclassified,
                })
            }
        }
    }
}

/// Consumer for LSUB (RFC 3501 Section6.3.9).
///
/// Simple accumulator  -  LSUB has no NOTIFY ambiguity (deprecated in
/// `IMAP4rev2`; RFC 9051 Appendix F item 19). All LSUB responses
/// classified as `OnlySolicited` are accumulated; any `Either` responses
/// are reclassified as events.
#[derive(Default)]
pub(crate) struct LsubConsumer {
    mailboxes: Vec<MailboxInfo>,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for LsubConsumer {
    type Output = Vec<MailboxInfo>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Lsub(info) => {
                self.mailboxes.push(info);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<MailboxInfo>>, Error> {
        tagged.require_ok()?;
        Ok(Finalized {
            output: self.mailboxes,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for LIST-EXTENDED (RFC 5258 Section3 / RFC 9051 Section6.3.9).
///
/// Like [`ListConsumer`] but additionally filters selection-mismatch
/// NOTIFY events (RFC 5258 Section3: responses that lack the required
/// `\Subscribed`, `\Remote`, or special-use attributes).
///
/// `filter_extended` controls whether `\NonExistent` / `\NoAccess` are
/// treated as NOTIFY markers. When `SUBSCRIBED` is in the selection
/// options, these attributes are legitimate solicited data (RFC 5258 Section3)
/// and must NOT be filtered.
pub(crate) struct ListExtendedConsumer {
    /// Whether to treat `\NonExistent` / `\NoAccess` as NOTIFY markers.
    /// `true` when SUBSCRIBED is NOT in selection options.
    filter_extended: bool,
    /// Selection options for mismatch detection (owned copies).
    selection_options: Vec<String>,
    /// Solicited LIST entries.
    mailboxes: Vec<MailboxInfo>,
    /// NOTIFY marker-bearing LIST entries.
    marker_events: Vec<UntaggedResponse>,
    /// Selection-mismatch NOTIFY events (already decoded  -  pushed
    /// directly to reclassified, not through `buffer_remaining`).
    mismatch_events: Vec<UntaggedResponse>,
    /// Non-LIST responses routed here via `Either`.
    buffered: Vec<UntaggedResponse>,
}

impl ListExtendedConsumer {
    pub(crate) fn new(filter_extended: bool, selection_options: Vec<String>) -> Self {
        Self {
            filter_extended,
            selection_options,
            mailboxes: Vec::new(),
            marker_events: Vec::new(),
            mismatch_events: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ListExtendedConsumer {
    type Output = Result<Vec<MailboxInfo>, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::List(info) => {
                if notify_snapshot.list {
                    // RFC 5465 Section5.4: check for NOTIFY marker events.
                    if super::is_notify_list_event(&info, self.filter_extended) {
                        self.marker_events.push(UntaggedResponse::List(info));
                        return;
                    }
                    // RFC 5258 Section3: check selection-option mismatch. Build
                    // a temporary &[&str] view for the helper function.
                    let opts: Vec<&str> =
                        self.selection_options.iter().map(String::as_str).collect();
                    if super::is_notify_selection_mismatch(&info, &opts) {
                        self.mismatch_events.push(UntaggedResponse::List(info));
                        return;
                    }
                }
                self.mailboxes.push(info);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<Vec<MailboxInfo>, Error>>, Error> {
        // Marker events and mismatch events are reclassified on both
        // success and failure paths  -  they are provably NOTIFY events
        // and must not be dropped.
        let mut reclassified = self.marker_events;
        reclassified.extend(self.mismatch_events);
        reclassified.extend(self.buffered);

        match tagged.require_ok() {
            Ok(_) => Ok(Finalized {
                output: Ok(self.mailboxes),
                reclassified_as_events: reclassified,
            }),
            Err(e) => {
                // On failure: marker-less, non-mismatch LIST may be
                // the failed solicited result  -  drop it.
                Ok(Finalized {
                    output: Err(e),
                    reclassified_as_events: reclassified,
                })
            }
        }
    }
}

/// Consumer for LIST with STATUS return option (RFC 5819 Section2).
///
/// Correlates interleaved LIST and STATUS responses by mailbox name.
/// Both LIST and STATUS are classified as `OnlySolicited` during
/// LIST-STATUS (see `classify`). NOTIFY marker-bearing LIST entries
/// are identified via `is_notify_list_event` and reclassified.
///
/// On failure: marker-bearing LIST -> reclassified as events; all STATUS
/// and marker-less LIST -> dropped (STATUS is wire-identical to NOTIFY,
/// RFC 5465 Section4 / RFC 5819 Section2).
pub(crate) struct ListStatusConsumer {
    /// Accumulated solicited LIST entries paired with their STATUS data.
    /// STATUS slot is `None` until the correlated STATUS arrives.
    results: Vec<(MailboxInfo, Option<Vec<StatusItem>>)>,
    /// STATUS responses that arrived before their LIST (valid per
    /// RFC 5819  -  ordering is not mandated).
    pending_status: Vec<(MailboxName, Vec<StatusItem>)>,
    /// NOTIFY marker-bearing LIST entries.
    marker_events: Vec<UntaggedResponse>,
    /// Non-LIST/non-STATUS responses routed here via `Either`.
    buffered: Vec<UntaggedResponse>,
}

impl ListStatusConsumer {
    pub(crate) fn new() -> Self {
        Self {
            results: Vec::new(),
            pending_status: Vec::new(),
            marker_events: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ListStatusConsumer {
    type Output = Result<Vec<(MailboxInfo, Vec<StatusItem>)>, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::List(info) => {
                // RFC 5465 Section5.4: NOTIFY marker detection.
                if notify_snapshot.list && super::is_notify_list_event(&info, true) {
                    self.marker_events.push(UntaggedResponse::List(info));
                    return;
                }
                // Solicited LIST entry  -  waiting for correlated STATUS.
                self.results.push((info, None));
            }
            UntaggedResponse::MailboxStatus { mailbox, items } => {
                // Positional correlation: pair with the first unpaired
                // LIST for this mailbox. Use `inbox_eq` for
                // case-insensitive INBOX matching (RFC 3501 Section5.1).
                if let Some((_, status)) = self
                    .results
                    .iter_mut()
                    .find(|(mb, s)| s.is_none() && inbox_eq(mb.name.as_str(), mailbox.as_str()))
                {
                    *status = Some(items);
                } else {
                    // STATUS arrived before its LIST  -  save for second
                    // pass in finalize (valid LIST-STATUS ordering per
                    // RFC 5819).
                    self.pending_status.push((mailbox, items));
                }
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<Vec<(MailboxInfo, Vec<StatusItem>)>, Error>>, Error> {
        // Marker events are reclassified regardless of success/failure.
        let mut reclassified = self.marker_events;
        reclassified.extend(self.buffered);

        if let Err(e) = tagged.require_ok() {
            // On failure: drop all accumulated LIST and STATUS
            // (STATUS is wire-identical to NOTIFY  -  RFC 5465 Section4).
            // Only NOTIFY marker events survive.
            return Ok(Finalized {
                output: Err(e),
                reclassified_as_events: reclassified,
            });
        }

        // Second pass: pair STATUS that arrived before their LIST.
        let mut results = self.results;
        for (decoded, items) in self.pending_status {
            if let Some((_, status)) = results
                .iter_mut()
                .find(|(mb, s)| s.is_none() && inbox_eq(mb.name.as_str(), decoded.as_str()))
            {
                *status = Some(items);
            }
            // Orphaned STATUS inside LIST-STATUS is ambiguous:
            // could be malformed solicited output or NOTIFY
            // delivery. Drop rather than manufacturing a fake
            // notification (RFC 5465 Section4; RFC 5819 Section2).
        }

        // Replace None with empty vec for any LIST without a
        // matching STATUS (non-conformant server or STATUS not
        // yet arrived). See RFC 5465 Section5.5 / RFC 5819 Section2 for
        // the ambiguity reasoning.
        let paired: Vec<(MailboxInfo, Vec<StatusItem>)> = results
            .into_iter()
            .map(|(mb, status)| {
                let items = status.unwrap_or_default();
                (mb, items)
            })
            .collect();

        Ok(Finalized {
            output: Ok(paired),
            reclassified_as_events: reclassified,
        })
    }
}

/// Consumer for STATUS (RFC 3501 Section6.3.10).
///
/// Accumulates same-mailbox STATUS responses (classified as
/// `OnlySolicited` by `classify`). When NOTIFY STATUS is active
/// (RFC 5465 Section4), additional same-mailbox STATUS responses are
/// ambiguous  -  the protocol provides no marker to distinguish
/// solicited from NOTIFY. These are surfaced in
/// [`StatusResult::ambiguous`] rather than silently reclassified.
///
/// On failure: all same-mailbox STATUS is dropped  -  buffering as
/// unsolicited would leak potentially-solicited data into the event
/// channel (RFC 5465 Section4, RFC 3501 Section6.3.10).
pub(crate) struct StatusConsumer {
    /// Same-mailbox STATUS responses with their per-response notify
    /// snapshot flag. The last entry becomes the primary result;
    /// earlier entries with `had_notify == true` become ambiguous.
    matching: Vec<(UntaggedResponse, bool)>,
    /// Non-STATUS responses routed here via `Either`.
    buffered: Vec<UntaggedResponse>,
}

impl StatusConsumer {
    pub(crate) fn new() -> Self {
        Self {
            matching: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for StatusConsumer {
    type Output = StatusResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::MailboxStatus { .. } => {
                // Record whether NOTIFY STATUS was active when this
                // response was generated. Used in finalize to classify
                // extras as ambiguous vs unsolicited.
                self.matching.push((resp, notify_snapshot.status));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<StatusResult>, Error> {
        // On failure: drop all matching STATUS AND any Either-classified
        // responses (e.g., * OK [ALERT])  -  same pattern as other consumers.
        // Leaking potentially-solicited STATUS as unsolicited events is
        // worse than losing a transient alert (RFC 5465 Section4).
        tagged.require_ok()?;

        let mut matching = self.matching;

        // RFC 3501 Section6.3.10: an OK response MUST include an untagged
        // STATUS for the requested mailbox.
        let Some((last_resp, _)) = matching.pop() else {
            let target = ctx
                .command_target()
                .map_or_else(|| "<unknown>".to_owned(), |t| t.as_str().to_owned());
            return Err(Error::Protocol(format!(
                "STATUS OK but no matching untagged STATUS response \
                 for mailbox '{target}' (RFC 3501 Sections 5.2, 6.3.10)"
            )));
        };

        // Extract the primary items from the last response.
        let UntaggedResponse::MailboxStatus {
            items: primary_items,
            ..
        } = last_resp
        else {
            return Err(Error::Protocol(
                "internal: matching predicate returned non-MailboxStatus \
                 variant"
                    .into(),
            ));
        };

        // RFC 5465 Section4 / Section5.8: classify remaining extras.
        // NOTIFY active -> ambiguous. No NOTIFY -> server anomaly,
        // reclassify as unsolicited per RFC 3501 Section5.2.
        let mut ambiguous = Vec::new();
        let mut non_notify_extras: Vec<UntaggedResponse> = Vec::new();
        for (resp, had_notify) in matching {
            if had_notify {
                if let UntaggedResponse::MailboxStatus { items, .. } = resp {
                    ambiguous.push(items);
                }
            } else {
                non_notify_extras.push(resp);
            }
        }

        let mut reclassified = self.buffered;
        reclassified.extend(non_notify_extras);

        Ok(Finalized {
            output: StatusResult {
                items: primary_items,
                ambiguous,
            },
            reclassified_as_events: reclassified,
        })
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  QUOTA / ACL / METADATA / THREAD / SORT / NOTIFY
// ---------------------------------------------------------------------------

use crate::connection::SearchResult;
use crate::types::response::{
    AclEntry, EsearchResponse, ListRightsResponse, MetadataResult, QuotaResource,
    QuotaRootResponse, ThreadNode,
};
use crate::types::{CopyResult, ExpungeResult, MoveResult};

/// Consumer for GETQUOTA (RFC 2087 Section4.2) and SETQUOTA (RFC 2087 Section4.1).
///
/// Both commands solicit a single untagged QUOTA response for the
/// requested root. Accumulates the first matching QUOTA response;
/// non-matching QUOTA responses are reclassified as events.
pub(crate) struct QuotaConsumer {
    /// The quota root we are looking for.
    root: String,
    /// The matching QUOTA response, if received.
    result: Option<Vec<QuotaResource>>,
    /// Non-matching or non-QUOTA responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl QuotaConsumer {
    pub(crate) fn new(root: String) -> Self {
        Self {
            root,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for QuotaConsumer {
    type Output = Vec<QuotaResource>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 2087 Section4.2 / Section4.1: accept only the QUOTA for the requested root.
        // RFC 3501 Section5.2: unrelated untagged responses may be interleaved.
        match resp {
            UntaggedResponse::Quota { root, resources }
                if root == self.root && self.result.is_none() =>
            {
                self.result = Some(resources);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<QuotaResource>>, Error> {
        tagged.require_ok()?;
        let resources = self.result.ok_or_else(|| {
            Error::Protocol(format!(
                "server sent OK but no QUOTA response for root '{}' \
                 (RFC 2087 Section 4.2)",
                self.root,
            ))
        })?;
        Ok(Finalized {
            output: resources,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for GETQUOTAROOT (RFC 2087 Section4.3 / RFC 9208 Section4.1.2).
///
/// Accumulates the QUOTAROOT response (root names) and all QUOTA
/// responses (resource triplets). Correlates QUOTA responses to the
/// roots listed in the QUOTAROOT response in `finalize`.
pub(crate) struct QuotaRootConsumer {
    /// The mailbox argument, for correlation.
    mailbox: String,
    /// The QUOTAROOT response roots, if received.
    roots: Option<Vec<String>>,
    /// All QUOTA responses received.
    quotas: Vec<(String, Vec<QuotaResource>)>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl QuotaRootConsumer {
    pub(crate) fn new(mailbox: String) -> Self {
        Self {
            mailbox,
            roots: None,
            quotas: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for QuotaRootConsumer {
    type Output = QuotaRootResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 2087 Section4.3: QUOTAROOT response correlates by mailbox
            // via inbox_eq for INBOX case-insensitivity (RFC 3501 Section5.1).
            UntaggedResponse::QuotaRoot { mailbox, roots }
                if inbox_eq(&self.mailbox, mailbox.as_str()) && self.roots.is_none() =>
            {
                self.roots = Some(roots);
            }
            // RFC 2087 Section4.3: QUOTA responses for the returned roots.
            // We consume all QUOTA here  -  filtering against the root
            // list happens in finalize, where non-matching ones are
            // reclassified as events.
            UntaggedResponse::Quota { root, resources } => {
                self.quotas.push((root, resources));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<QuotaRootResponse>, Error> {
        tagged.require_ok()?;

        let roots = self.roots.ok_or_else(|| {
            Error::Protocol(format!(
                "server sent OK but no QUOTAROOT response for mailbox '{}' \
                 (RFC 2087 Section 4.3)",
                self.mailbox,
            ))
        })?;

        // Partition QUOTA responses: matching roots are the result,
        // non-matching are reclassified as events.
        let mut resources: Vec<(String, Vec<QuotaResource>)> = Vec::new();
        let mut buffered = self.buffered;
        for (root, res) in self.quotas {
            if roots.iter().any(|expected| expected == &root) {
                resources.push((root, res));
            } else {
                buffered.push(UntaggedResponse::Quota {
                    root,
                    resources: res,
                });
            }
        }

        if roots.is_empty() {
            return Ok(Finalized {
                output: QuotaRootResponse { roots, resources },
                reclassified_as_events: buffered,
            });
        }
        if resources.is_empty() {
            return Err(Error::Protocol(format!(
                "server sent OK but no QUOTA response for QUOTAROOT mailbox \
                 '{}' (RFC 2087 Section 4.3)",
                self.mailbox,
            )));
        }

        Ok(Finalized {
            output: QuotaRootResponse { roots, resources },
            reclassified_as_events: buffered,
        })
    }
}

/// Consumer for GETACL (RFC 4314 Section3.3).
///
/// Accumulates the ACL response for the requested mailbox.
pub(crate) struct AclConsumer {
    /// The mailbox argument, for correlation.
    mailbox: String,
    /// The matching ACL entries, if received.
    result: Option<Vec<AclEntry>>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl AclConsumer {
    pub(crate) fn new(mailbox: String) -> Self {
        Self {
            mailbox,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for AclConsumer {
    type Output = Vec<AclEntry>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 4314 Section3.3 / RFC 3501 Section5.2: correlate by mailbox via
        // inbox_eq for INBOX case-insensitivity.
        match resp {
            UntaggedResponse::Acl { mailbox, entries }
                if inbox_eq(&self.mailbox, mailbox.as_str()) && self.result.is_none() =>
            {
                self.result = Some(entries);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<AclEntry>>, Error> {
        tagged.require_ok()?;
        let entries = self.result.ok_or_else(|| {
            Error::Protocol(format!(
                "server sent OK but no ACL response for mailbox '{}' \
                 (RFC 4314 Section 3.3)",
                self.mailbox,
            ))
        })?;
        Ok(Finalized {
            output: entries,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for LISTRIGHTS (RFC 4314 Section3.4).
///
/// Accumulates the LISTRIGHTS response for the requested mailbox and
/// identifier.
pub(crate) struct ListRightsConsumer {
    /// The mailbox argument, for correlation.
    mailbox: String,
    /// The identifier argument, for correlation.
    identifier: String,
    /// The matching LISTRIGHTS response, if received.
    result: Option<ListRightsResponse>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl ListRightsConsumer {
    pub(crate) fn new(mailbox: String, identifier: String) -> Self {
        Self {
            mailbox,
            identifier,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ListRightsConsumer {
    type Output = ListRightsResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 4314 Section3.4: correlate by mailbox AND identifier.
        match resp {
            UntaggedResponse::ListRights {
                mailbox,
                identifier,
                required,
                optional,
            } if inbox_eq(&self.mailbox, mailbox.as_str())
                && identifier == self.identifier
                && self.result.is_none() =>
            {
                self.result = Some(ListRightsResponse { required, optional });
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<ListRightsResponse>, Error> {
        tagged.require_ok()?;
        let result = self.result.ok_or_else(|| {
            Error::Protocol(format!(
                "server sent OK but no LISTRIGHTS response for mailbox '{}' \
                 and identifier '{}' (RFC 4314 Section 3.4)",
                self.mailbox, self.identifier,
            ))
        })?;
        Ok(Finalized {
            output: result,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for MYRIGHTS (RFC 4314 Section3.5).
///
/// Accumulates the MYRIGHTS response for the requested mailbox.
pub(crate) struct MyRightsConsumer {
    /// The mailbox argument, for correlation.
    mailbox: String,
    /// The matching MYRIGHTS rights string, if received.
    result: Option<String>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl MyRightsConsumer {
    pub(crate) fn new(mailbox: String) -> Self {
        Self {
            mailbox,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for MyRightsConsumer {
    type Output = String;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 4314 Section3.5: correlate by mailbox.
        match resp {
            UntaggedResponse::MyRights { mailbox, rights }
                if inbox_eq(&self.mailbox, mailbox.as_str()) && self.result.is_none() =>
            {
                self.result = Some(rights);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<String>, Error> {
        tagged.require_ok()?;
        let rights = self.result.ok_or_else(|| {
            Error::Protocol(format!(
                "server sent OK but no MYRIGHTS response for mailbox '{}' \
                 (RFC 4314 Section 3.5)",
                self.mailbox,
            ))
        })?;
        Ok(Finalized {
            output: rights,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for GETMETADATA (RFC 5464 Section4.2).
///
/// Accumulates same-mailbox METADATA responses and tracks NOTIFY
/// ambiguity via per-response `notify_snapshot`. Different-mailbox
/// METADATA responses are reclassified as events.
///
/// RFC 5465 Section5.6-5.8: when NOTIFY metadata is active, the protocol
/// provides no marker to distinguish solicited METADATA from unsolicited
/// NOTIFY METADATA for the same mailbox. The consumer exposes this via
/// `MetadataResult::notify_ambiguity`.
pub(crate) struct MetadataConsumer {
    /// The mailbox argument, for correlation.
    mailbox: String,
    /// Accumulated metadata entries from same-mailbox responses.
    entries: Vec<crate::types::response::MetadataEntry>,
    /// Whether any same-mailbox response arrived while NOTIFY metadata
    /// was active, making the result potentially ambiguous.
    notify_ambiguity: bool,
    /// Whether we saw at least one matching METADATA response.
    saw_matching: bool,
    /// Different-mailbox METADATA and non-METADATA responses.
    buffered: Vec<UntaggedResponse>,
}

impl MetadataConsumer {
    pub(crate) fn new(mailbox: String) -> Self {
        Self {
            mailbox,
            entries: Vec::new(),
            notify_ambiguity: false,
            saw_matching: false,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for MetadataConsumer {
    type Output = MetadataResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // Same-mailbox METADATA: accumulate entries.
            // RFC 5464 Section4.2: GETMETADATA can produce multiple METADATA
            // response lines for the same mailbox.
            UntaggedResponse::Metadata { mailbox, entries }
                if inbox_eq(&self.mailbox, mailbox.as_str()) =>
            {
                // RFC 5465 Section5.6-5.8: if NOTIFY metadata was active when
                // this response was generated, the result is ambiguous  -
                // some entries may be from interleaved NOTIFY events.
                // Post-NOTIFICATIONOVERFLOW, apply_side_effects clears the
                // metadata flag, so notify_snapshot.metadata will be false
                // for post-overflow responses (they are unambiguously
                // solicited per RFC 5465 Section5.8).
                if notify_snapshot.metadata {
                    self.notify_ambiguity = true;
                }
                self.saw_matching = true;
                self.entries.extend(entries);
            }
            // Different-mailbox METADATA or non-METADATA: reclassify.
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<MetadataResult>, Error> {
        // On failure: drop all matching same-mailbox METADATA rather than
        // buffering as unsolicited. Same-mailbox METADATA is wire-identical
        // between the solicited reply and a NOTIFY event (RFC 5465
        // Section5.6-5.7, RFC 5464 Section4.2)  -  buffering as unsolicited would leak
        // potentially-solicited data into the NOTIFY event channel.
        tagged.require_ok()?;

        if !self.saw_matching {
            return Err(Error::Protocol(
                "server completed GETMETADATA without the required METADATA \
                 response for the requested mailbox (RFC 5464 Section 4.2)"
                    .into(),
            ));
        }

        Ok(Finalized {
            output: MetadataResult {
                entries: self.entries,
                notify_ambiguity: self.notify_ambiguity,
            },
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for THREAD and UID THREAD (RFC 5256 Section3).
///
/// Accumulates the single THREAD response.
#[derive(Default)]
pub(crate) struct ThreadConsumer {
    /// The THREAD response, if received.
    result: Option<Vec<ThreadNode>>,
    /// Non-THREAD responses routed here.
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for ThreadConsumer {
    type Output = Vec<ThreadNode>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Thread(threads) if self.result.is_none() => {
                self.result = Some(threads);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<ThreadNode>>, Error> {
        tagged.require_ok()?;
        // RFC 5256 Section 4: an empty THREAD result (no matching
        // messages) may be represented by the server omitting the
        // untagged THREAD response entirely and sending only tagged OK.
        let threads = self.result.unwrap_or_default();
        Ok(Finalized {
            output: threads,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for SORT and UID SORT (RFC 5256 Section2).
///
/// Accumulates the single SORT response with optional MODSEQ
/// (RFC 7162 Section3.1.6).
#[derive(Default)]
pub(crate) struct SortConsumer {
    /// The SORT response, if received.
    result: Option<(Vec<u32>, Option<u64>)>,
    /// Non-SORT responses routed here.
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for SortConsumer {
    type Output = SearchResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Sort { nums, mod_seq } if self.result.is_none() => {
                self.result = Some((nums, mod_seq));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<SearchResult>, Error> {
        tagged.require_ok()?;
        // RFC 5256 Section 4: an empty SORT result (no matching
        // messages) may be represented by the server omitting the
        // untagged SORT response entirely and sending only tagged OK.
        let (ids, mod_seq) = self.result.unwrap_or_default();
        Ok(Finalized {
            output: SearchResult {
                ids,
                mod_seq,
                truncated: false,
            },
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for NOTIFY SET (RFC 5465 Section3).
///
/// NOTIFY SET has no solicited untagged data of its own, but the
/// implicit NOOP effect means the server may flush STATUS/LIST/METADATA
/// before the tagged OK. All untagged responses are reclassified as
/// events. The consumer detects NOTIFICATIONOVERFLOW in both untagged
/// responses and the tagged response code.
#[derive(Default)]
pub(crate) struct NotifySetConsumer {
    /// Whether NOTIFICATIONOVERFLOW was seen in untagged responses.
    saw_overflow: bool,
    /// All untagged responses  -  reclassified as events.
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for NotifySetConsumer {
    /// `Ok(true)` when NOTIFICATIONOVERFLOW was detected (RFC 5465 Section5.8).
    /// `Err(...)` when the server rejected the command (NO/BAD).
    /// Wrapping the error in `Output` instead of `finalize`'s `Result`
    /// ensures that `reclassified_as_events` is always emitted  -  even
    /// on the failure path (EXISTS/RECENT classified as `Either` during
    /// NOTIFY SET must not be silently dropped).
    type Output = Result<bool, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 5465 Section5.8: detect NOTIFICATIONOVERFLOW in untagged
        // responses (status with the overflow response code).
        if matches!(
            &resp,
            UntaggedResponse::Status {
                code: Some(ResponseCode::NotificationOverflow(_)),
                ..
            }
        ) {
            self.saw_overflow = true;
        }
        // All responses from the implicit NOOP are unsolicited.
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<bool, Error>>, Error> {
        match tagged.status {
            StatusKind::Ok => {
                // RFC 5465 Section5.8: NOTIFICATIONOVERFLOW can also appear in
                // the tagged response code.
                let overflow = self.saw_overflow
                    || matches!(tagged.code, Some(ResponseCode::NotificationOverflow(_)));
                Ok(Finalized {
                    output: Ok(overflow),
                    reclassified_as_events: self.buffered,
                })
            }
            StatusKind::No => Ok(Finalized {
                output: Err(Error::no_with_code(tagged.text, tagged.code)),
                reclassified_as_events: self.buffered,
            }),
            StatusKind::Bad => Ok(Finalized {
                output: Err(Error::bad_with_code(tagged.text, tagged.code)),
                reclassified_as_events: self.buffered,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  SEARCH / ESEARCH / COPY / MOVE / EXPUNGE
// ---------------------------------------------------------------------------

/// Consumer for SEARCH and UID SEARCH (RFC 3501 Section6.4.4 / Section6.4.8).
///
/// Accumulates solicited SEARCH and ESEARCH responses. In `finalize`,
/// picks the best match with the same three-pass priority ordering
/// as the old `parse_search_result`:
/// 1. Tag-correlated ESEARCH (highest  -  unambiguous match)
/// 2. Tagless ESEARCH (servers that omit the correlator)
/// 3. Legacy SEARCH (`IMAP4rev1` fallback)
///
/// ESEARCH UID ranges are expanded into individual IDs. The `truncated`
/// flag on [`SearchResult`] signals when the expansion was capped at the
/// internal safety limit (RFC 4731 Section3, RFC 3501 Section6.4.4).
pub(crate) struct SearchConsumer {
    /// Tag-correlated ESEARCH responses (highest priority).
    tag_correlated: Vec<EsearchResponse>,
    /// Tagless ESEARCH responses (second priority).
    tagless_esearch: Vec<EsearchResponse>,
    /// Collected legacy SEARCH responses (lowest priority).
    search_responses: Vec<(Vec<u32>, Option<u64>)>,
    /// Non-SEARCH/ESEARCH responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl SearchConsumer {
    pub(crate) fn new() -> Self {
        Self {
            tag_correlated: Vec::new(),
            tagless_esearch: Vec::new(),
            search_responses: Vec::new(),
            buffered: Vec::new(),
        }
    }

    /// Drain all accumulated responses into `buffered` for reclassification.
    fn drain_all_into_buffered(&mut self) {
        for e in self.tag_correlated.drain(..) {
            self.buffered.push(UntaggedResponse::Esearch(e));
        }
        for e in self.tagless_esearch.drain(..) {
            self.buffered.push(UntaggedResponse::Esearch(e));
        }
        for (uids, mod_seq) in self.search_responses.drain(..) {
            self.buffered
                .push(UntaggedResponse::Search { uids, mod_seq });
        }
    }
}

impl Consumer for SearchConsumer {
    /// `Result` wrapper ensures `reclassified_as_events` is always
    /// processed even when the command-level outcome is an error
    /// (e.g., no solicited response found). Callers flatten with `??`.
    type Output = Result<SearchResult, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 4466 search-correlator: tag-correlated ESEARCH.
            UntaggedResponse::Esearch(e) if e.tag.as_deref() == Some(ctx.command_tag()) => {
                self.tag_correlated.push(e);
            }
            // Tagless ESEARCH  -  some servers omit the correlator.
            UntaggedResponse::Esearch(e) if e.tag.is_none() => {
                self.tagless_esearch.push(e);
            }
            UntaggedResponse::Search { uids, mod_seq } => {
                self.search_responses.push((uids, mod_seq));
            }
            // Foreign-tagged ESEARCH and other response types.
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        mut self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<SearchResult, Error>>, Error> {
        if let Err(e) = tagged.require_ok() {
            self.drain_all_into_buffered();
            return Ok(Finalized {
                output: Err(e),
                reclassified_as_events: self.buffered,
            });
        }

        // Priority: tag-correlated ESEARCH > tagless ESEARCH > legacy
        // SEARCH (RFC 4731 Section3.1, same as the old three-pass ordering).

        // Pass 1: tag-correlated ESEARCH.
        if let Some(esearch) = self.tag_correlated.first() {
            let (ids, truncated) = super::expand_uid_ranges(&esearch.all);
            let result = SearchResult {
                ids,
                mod_seq: esearch.mod_seq,
                truncated,
            };
            // Consumed the first tag-correlated; reclassify the rest.
            let mut buffered = self.buffered;
            for e in self.tag_correlated.into_iter().skip(1) {
                buffered.push(UntaggedResponse::Esearch(e));
            }
            for e in self.tagless_esearch {
                buffered.push(UntaggedResponse::Esearch(e));
            }
            for (uids, mod_seq) in self.search_responses {
                buffered.push(UntaggedResponse::Search { uids, mod_seq });
            }
            return Ok(Finalized {
                output: Ok(result),
                reclassified_as_events: buffered,
            });
        }

        // Pass 2: tagless ESEARCH.
        if let Some(esearch) = self.tagless_esearch.first() {
            let (ids, truncated) = super::expand_uid_ranges(&esearch.all);
            let result = SearchResult {
                ids,
                mod_seq: esearch.mod_seq,
                truncated,
            };
            let mut buffered = self.buffered;
            for e in self.tagless_esearch.into_iter().skip(1) {
                buffered.push(UntaggedResponse::Esearch(e));
            }
            for (uids, mod_seq) in self.search_responses {
                buffered.push(UntaggedResponse::Search { uids, mod_seq });
            }
            return Ok(Finalized {
                output: Ok(result),
                reclassified_as_events: buffered,
            });
        }

        // Pass 3: legacy SEARCH.
        let mut search_iter = self.search_responses.into_iter();
        if let Some((uids, mod_seq)) = search_iter.next() {
            let mut buffered = self.buffered;
            for (uids, mod_seq) in search_iter {
                buffered.push(UntaggedResponse::Search { uids, mod_seq });
            }
            return Ok(Finalized {
                output: Ok(SearchResult {
                    ids: uids,
                    mod_seq,
                    truncated: false,
                }),
                reclassified_as_events: buffered,
            });
        }

        Ok(Finalized {
            output: Err(Error::Protocol(
                "SEARCH OK but no untagged SEARCH/ESEARCH response \
                 (RFC 3501 Section 6.4.4)"
                    .into(),
            )),
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for SEARCH RETURN and UID SEARCH RETURN (RFC 4731 Section3.2).
///
/// Accumulates the solicited ESEARCH response and returns the full
/// [`EsearchResponse`] with MIN, MAX, COUNT, ALL, and MODSEQ fields.
pub(crate) struct EsearchConsumer {
    /// The first matching ESEARCH response.
    result: Option<EsearchResponse>,
    /// Non-ESEARCH responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl EsearchConsumer {
    pub(crate) fn new() -> Self {
        Self {
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for EsearchConsumer {
    /// `Result` wrapper ensures `reclassified_as_events` is always
    /// processed even when the command-level outcome is an error.
    type Output = Result<EsearchResponse, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 4731 Section3.1: server MUST return a single ESEARCH response.
            // RFC 4466 search-correlator: accept tag-correlated or tagless
            // ESEARCH. Foreign-tagged ESEARCH belongs to another context.
            // Take the first matching one; extras are reclassified.
            UntaggedResponse::Esearch(e)
                if self.result.is_none()
                    && (e.tag.is_none() || e.tag.as_deref() == Some(ctx.command_tag())) =>
            {
                self.result = Some(e);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<EsearchResponse, Error>>, Error> {
        let buffered = self.buffered;

        if let Err(e) = tagged.require_ok() {
            return Ok(Finalized {
                output: Err(e),
                reclassified_as_events: buffered,
            });
        }

        let output = self.result.ok_or_else(|| {
            Error::Protocol(
                "SEARCH RETURN OK but no ESEARCH response \
                 (RFC 4731 Section 3.1)"
                    .into(),
            )
        });

        Ok(Finalized {
            output,
            reclassified_as_events: buffered,
        })
    }
}

/// Consumer for SEARCH RETURN (SAVE) and UID SEARCH RETURN (SAVE)
/// (RFC 5182 Section2).
///
/// The server saves results server-side. RFC 5182 Section2 requires a
/// solicited SEARCH or ESEARCH echo, but some servers (e.g. Dovecot)
/// omit it. Per Postel's law we tolerate the omission  -  the consumer
/// discards any SEARCH/ESEARCH data and succeeds on tagged OK.
pub(crate) struct SearchSaveConsumer {
    /// Non-SEARCH/ESEARCH responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl SearchSaveConsumer {
    pub(crate) fn new() -> Self {
        Self {
            buffered: Vec::new(),
        }
    }
}

impl Consumer for SearchSaveConsumer {
    /// `Result` wrapper ensures `reclassified_as_events` is always
    /// processed even when the command-level outcome is an error.
    type Output = Result<(), Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 5182 Section2: the server MUST send a solicited SEARCH or
            // ESEARCH even for SAVE-only requests. We accept and discard
            // the data  -  the caller only needs tagged OK.
            UntaggedResponse::Search { .. } => {}
            // RFC 4466 search-correlator: only accept tag-correlated or
            // tagless ESEARCH. Foreign-tagged ESEARCH is not solicited.
            UntaggedResponse::Esearch(e)
                if e.tag.is_none() || e.tag.as_deref() == Some(ctx.command_tag()) => {}
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<(), Error>>, Error> {
        Ok(Finalized {
            output: tagged.require_ok().map(|_| ()),
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for COPY and UID COPY (RFC 3501 Section6.4.7, RFC 4315 Section3).
///
/// COPY has no solicited untagged responses. The result is extracted
/// from the tagged OK response code, which SHOULD be `[COPYUID ...]`
/// per RFC 4315 Section3. Any untagged responses routed here (classified as
/// `Either`) are reclassified as events.
pub(crate) struct CopyConsumer {
    /// All responses routed here  -  COPY has no solicited untagged
    /// responses, so everything is reclassified as events.
    buffered: Vec<UntaggedResponse>,
    /// COPYUID response code extracted from an untagged `* OK [COPYUID ...]`.
    /// Some servers (e.g. Dovecot) send COPYUID in an untagged OK rather
    /// than in the tagged OK (RFC 4315 Section3).
    code: Option<ResponseCode>,
}

impl CopyConsumer {
    pub(crate) fn new() -> Self {
        Self {
            buffered: Vec::new(),
            code: None,
        }
    }
}

impl Consumer for CopyConsumer {
    type Output = CopyResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // COPY has no solicited untagged responses (RFC 3501 Section6.4.7).
        // Buffer everything for reclassification as events.
        match resp {
            // RFC 4315 Section3: some servers send COPYUID in an untagged OK.
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: code_opt @ Some(ResponseCode::CopyUid { .. }),
                ..
            } if self.code.is_none() => {
                self.code = code_opt;
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<CopyResult>, Error> {
        // RFC 4315 Section3: server SHOULD return COPYUID response code.
        let tagged = tagged.require_ok()?;
        Ok(Finalized {
            output: CopyResult {
                code: tagged.code.or(self.code),
            },
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for MOVE and UID MOVE (RFC 6851 Section3).
///
/// Accumulates EXPUNGE (RFC 3501 Section7.4.1) and VANISHED (RFC 7162
/// Section3.2.10) responses sent by the server before the tagged OK.
/// Returns a [`MoveResult`] with the COPYUID response code and the
/// expunged sequence numbers or UID ranges.
///
/// When QRESYNC is enabled the server sends VANISHED instead of
/// EXPUNGE (RFC 7162 Section3.2.10). The consumer accumulates both
/// variants and selects the appropriate [`ExpungeResult`] variant
/// based on the QRESYNC enabled state in `finalize`.
pub(crate) struct MoveConsumer {
    /// Expunged sequence numbers from `* N EXPUNGE` responses.
    expunged: Vec<u32>,
    /// Vanished UID ranges from `* VANISHED ...` responses.
    vanished: Vec<UidRange>,
    /// Non-EXPUNGE/VANISHED responses for reclassification.
    buffered: Vec<UntaggedResponse>,
    /// COPYUID response code extracted from an untagged `* OK [COPYUID ...]`.
    /// Some servers (e.g. Dovecot) send COPYUID in an untagged OK rather
    /// than in the tagged OK (RFC 4315 Section3).
    code: Option<ResponseCode>,
}

impl MoveConsumer {
    pub(crate) fn new() -> Self {
        Self {
            expunged: Vec::new(),
            vanished: Vec::new(),
            buffered: Vec::new(),
            code: None,
        }
    }
}

impl Consumer for MoveConsumer {
    type Output = MoveResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 6851 Section3: EXPUNGE responses for moved messages.
            UntaggedResponse::Expunge(n) => {
                self.expunged.push(n);
            }
            // RFC 7162 Section3.2.10: VANISHED responses when QRESYNC is enabled.
            UntaggedResponse::Vanished { uids, .. } => {
                self.vanished.extend(uids);
            }
            // RFC 4315 Section3: some servers send COPYUID in an untagged OK.
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: code_opt @ Some(ResponseCode::CopyUid { .. }),
                ..
            } if self.code.is_none() => {
                self.code = code_opt;
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<MoveResult>, Error> {
        let tagged = tagged.require_ok()?;
        // RFC 7162 Section3.2.10: when QRESYNC is enabled, the server sends
        // VANISHED instead of EXPUNGE.
        let expunged = if ctx.enabled().iter().any(|e| e == "QRESYNC") {
            ExpungeResult::Vanished(self.vanished)
        } else {
            ExpungeResult::Expunged(self.expunged)
        };
        Ok(Finalized {
            output: MoveResult {
                // RFC 6851 Section4.3: MOVE SHOULD return COPYUID response code.
                code: tagged.code.or(self.code),
                expunged,
            },
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for EXPUNGE (RFC 3501 Section6.4.3) and UID EXPUNGE (RFC 4315 Section2).
///
/// Accumulates EXPUNGE sequence numbers and VANISHED UID ranges.
/// When QRESYNC is enabled the server sends VANISHED instead of
/// EXPUNGE (RFC 7162 Section3.2.10).
pub(crate) struct ExpungeConsumer {
    /// Expunged sequence numbers from `* N EXPUNGE` responses.
    expunged: Vec<u32>,
    /// Vanished UID ranges from `* VANISHED (EARLIER) ...` responses.
    vanished: Vec<UidRange>,
    /// Non-EXPUNGE/VANISHED responses for reclassification.
    buffered: Vec<UntaggedResponse>,
}

impl ExpungeConsumer {
    pub(crate) fn new() -> Self {
        Self {
            expunged: Vec::new(),
            vanished: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ExpungeConsumer {
    type Output = ExpungeResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 3501 Section7.4.1: EXPUNGE responses with sequence numbers.
            UntaggedResponse::Expunge(n) => {
                self.expunged.push(n);
            }
            // RFC 7162 Section3.2.10: VANISHED (EARLIER) with UID ranges when
            // QRESYNC is enabled.
            UntaggedResponse::Vanished { uids, .. } => {
                self.vanished.extend(uids);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<ExpungeResult>, Error> {
        tagged.require_ok()?;
        // RFC 7162 Section3.2.10: when QRESYNC is enabled, the server sends
        // VANISHED instead of EXPUNGE.
        let result = if ctx.enabled().iter().any(|e| e == "QRESYNC") {
            ExpungeResult::Vanished(self.vanished)
        } else {
            ExpungeResult::Expunged(self.expunged)
        };
        Ok(Finalized {
            output: result,
            reclassified_as_events: self.buffered,
        })
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  ID / COMPRESS / STARTTLS / LOGOUT
// ---------------------------------------------------------------------------

/// Consumer for ID (RFC 2971 Section3.1).
///
/// Extracts the server's identity key-value pairs from the untagged
/// ID response. RFC 2971 Section3.2: the server MUST respond with an ID response.
#[derive(Default)]
pub(crate) struct IdConsumer {
    pairs: Option<Vec<(String, Option<String>)>>,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for IdConsumer {
    type Output = Vec<(String, Option<String>)>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 2971 Section3.2: take the first ID response.
        match resp {
            UntaggedResponse::Id(pairs) if self.pairs.is_none() => {
                self.pairs = Some(pairs);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<(String, Option<String>)>>, Error> {
        tagged.require_ok()?;
        let pairs = self.pairs.ok_or_else(|| {
            Error::Protocol("ID OK but no untagged ID response (RFC 2971 Section 3.2)".into())
        })?;
        Ok(Finalized {
            output: pairs,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for NAMESPACE (RFC 2342 Section5).
///
/// Extracts the personal, other-users, and shared namespace
/// descriptors from the untagged NAMESPACE response.
#[derive(Default)]
pub(crate) struct NamespaceConsumer {
    namespace: Option<(
        Vec<crate::types::NamespaceDescriptor>,
        Vec<crate::types::NamespaceDescriptor>,
        Vec<crate::types::NamespaceDescriptor>,
    )>,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for NamespaceConsumer {
    type Output = crate::types::NamespaceResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 2342 Section5: take the first NAMESPACE response.
        match resp {
            UntaggedResponse::Namespace {
                personal,
                other,
                shared,
            } if self.namespace.is_none() => {
                self.namespace = Some((personal, other, shared));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<crate::types::NamespaceResponse>, Error> {
        tagged.require_ok()?;
        let (personal, other, shared) = self.namespace.ok_or_else(|| {
            Error::Protocol(
                "NAMESPACE OK but no untagged NAMESPACE response (RFC 2342 Section 5)".into(),
            )
        })?;
        Ok(Finalized {
            output: crate::types::NamespaceResponse {
                personal,
                other,
                shared,
            },
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for the ENABLE command (RFC 5161 Section 3).
///
/// Captures the `* ENABLED` untagged response and returns the list
/// of extensions the server actually enabled for this request.
#[derive(Default)]
pub(crate) struct EnableConsumer {
    /// The enabled extensions from `* ENABLED`.
    caps: Option<Vec<String>>,
    /// Non-matching untagged responses to reclassify as events.
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for EnableConsumer {
    type Output = Vec<String>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Enabled(exts) if self.caps.is_none() => {
                self.caps = Some(exts);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<String>>, Error> {
        tagged.require_ok()?;
        // RFC 5161 Section 3.2: the server MUST send an ENABLED response.
        // Tolerate omission per Postel's law  -  warn and return empty.
        let exts = self.caps.unwrap_or_else(|| {
            tracing::warn!(
                "server omitted ENABLED response (RFC 5161 Section 3.2) \
                  -  treating as empty"
            );
            Vec::new()
        });
        Ok(Finalized {
            output: exts,
            reclassified_as_events: self.buffered,
        })
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
