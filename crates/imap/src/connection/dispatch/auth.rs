use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::SecretString;
use crate::types::response::{
    ContinuationRequest, ResponseCode, StatusKind, TaggedResponse, UntaggedResponse,
};

use super::{Consumer, ConsumerContext, ContinuationConsumer, ContinuationReply, Finalized};

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

pub(super) fn cram_md5_response(
    user: &str,
    pass: &str,
    challenge: &str,
) -> Result<SecretString, Error> {
    use base64::Engine;
    use hmac::Mac as _;
    use std::fmt::Write;

    let challenge = base64::engine::general_purpose::STANDARD
        .decode(challenge.trim())
        .map_err(|e| Error::Protocol(format!("invalid CRAM-MD5 challenge: {e}")))?;
    let mut mac = <hmac::Hmac<md5::Md5> as hmac::digest::KeyInit>::new_from_slice(pass.as_bytes())
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

pub(super) fn scram_client_final(
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
    let mut mac = <M as hmac::digest::KeyInit>::new_from_slice(key)
        .map_err(|e| Error::Protocol(format!("invalid SCRAM HMAC key: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(a, b)| a ^ b).collect()
}
