use bifrost_sasl::{
    ScramHash, cram_md5_response, decode_continuation, escape_username, scram_client_final,
    verify_server_final,
};

use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::SecretString;
use crate::types::response::{
    ContinuationRequest, ResponseCode, StatusKind, TaggedResponse, UntaggedResponse,
};

use super::{Consumer, ConsumerContext, ContinuationConsumer, ContinuationReply, Finalized};

/// Map a `bifrost-sasl` computation failure back into the IMAP error model.
///
/// Protocol-class messages stay `Error::Protocol`; a SCRAM `e=` server error
/// (the auth-failure lane) stays `Error::auth_with_code(.., None)`. This
/// preserves the exact pre-move error classification.
impl From<bifrost_sasl::SaslError> for Error {
    fn from(e: bifrost_sasl::SaslError) -> Self {
        match e {
            bifrost_sasl::SaslError::Protocol(m) => Error::Protocol(m),
            bifrost_sasl::SaslError::AuthFailed(m) => Error::auth_with_code(m, None),
            // `SaslError` is `#[non_exhaustive]`; any future variant is an
            // unclassified auth failure until it is mapped explicitly.
            other => Error::auth_with_code(other.to_string(), None),
        }
    }
}

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
        // RFC 3501 Section7.2.1: the server SHOULD send updated capabilities
        // after authentication. Track it so the caller can skip a follow-up
        // CAPABILITY command.
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
/// Implements [`ContinuationConsumer`] when the server sends `+`,
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
/// error challenges; the client MUST reply with an empty `\r\n` to let
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
            // XOAUTH2 error continuation: the server sent a base64-encoded
            // error as `+ <error>`. The client MUST respond with an empty
            // `\r\n` to let the server finish the exchange with a tagged
            // NO/BAD (Google XOAUTH2 spec, non-IETF).
            Ok(ContinuationReply::Write(b"\r\n".to_vec()))
        } else {
            // First continuation: send the XOAUTH2 credential payload.
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
        let mut bytes = Vec::with_capacity(encoded.as_str().len() + 2);
        bytes.extend_from_slice(encoded.as_bytes());
        bytes.extend_from_slice(b"\r\n");
        Ok(ContinuationReply::Write(bytes))
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
    mechanism: ScramHash,
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
        mechanism: ScramHash,
        user: String,
        pass: SecretString,
        nonce: String,
        sasl_ir_used: bool,
    ) -> Self {
        let client_first_bare = format!("n={},r={nonce}", escape_username(&user));
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
                let server_first = decode_continuation(&cont.data)?;
                let (client_final, server_signature) = scram_client_final(
                    self.mechanism,
                    &self.pass,
                    &self.client_nonce,
                    &self.client_first_bare,
                    &server_first,
                )?;
                self.expected_server_signature = Some(server_signature);
                self.state = ScramState::AwaitServerFinal;
                let mut bytes = Vec::with_capacity(client_final.as_str().len() + 2);
                bytes.extend_from_slice(client_final.as_bytes());
                bytes.extend_from_slice(b"\r\n");
                Ok(ContinuationReply::Write(bytes))
            }
            ScramState::AwaitServerFinal => {
                let server_final = decode_continuation(&cont.data)?;
                let expected = self.expected_server_signature.take().ok_or_else(|| {
                    Error::Protocol("SCRAM server signature missing from client state".into())
                })?;
                verify_server_final(&server_final, &expected)?;
                self.state = ScramState::Done;
                Ok(ContinuationReply::Write(b"\r\n".to_vec()))
            }
            ScramState::Done => Err(Error::Protocol(
                "unexpected continuation after SCRAM server-final message".into(),
            )),
        }
    }
}
