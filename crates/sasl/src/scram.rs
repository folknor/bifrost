//! Pure SCRAM (RFC 5802 / RFC 7677) computation.
//!
//! No I/O, no protocol command flow. The protocol crates drive the `+`
//! continuation sequencing and call these transition functions.

use crate::error::SaslError;
use crate::secret::Secret;

/// The hash function backing a SCRAM mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScramHash {
    /// SCRAM-SHA-1 (RFC 5802).
    Sha1,
    /// SCRAM-SHA-256 (RFC 7677).
    Sha256,
}

/// Whether a SCRAM exchange binds to the TLS channel.
///
/// Names the channel-binding dimension, kept orthogonal to [`ScramHash`] (the
/// hash) so the proof math stays hash-parameterized and the `-PLUS` mechanism
/// name is derived in one place ([`ScramHash::mechanism_name`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChannelBinding {
    /// No channel binding (GS2 header `n,,`). Server advertises the non-PLUS
    /// mechanism, or the client declines binding.
    None,
    /// `tls-server-end-point` binding (GS2 header
    /// `p=tls-server-end-point,,`). Server advertises the `-PLUS` mechanism.
    TlsServerEndPoint,
}

impl ScramHash {
    /// The SASL mechanism name advertised on the wire, including the `-PLUS`
    /// suffix when the exchange binds to the channel.
    pub fn mechanism_name(self, binding: ChannelBinding) -> &'static str {
        match (self, binding) {
            (Self::Sha1, ChannelBinding::None) => "SCRAM-SHA-1",
            (Self::Sha256, ChannelBinding::None) => "SCRAM-SHA-256",
            (Self::Sha1, ChannelBinding::TlsServerEndPoint) => "SCRAM-SHA-1-PLUS",
            (Self::Sha256, ChannelBinding::TlsServerEndPoint) => "SCRAM-SHA-256-PLUS",
        }
    }
}

/// The GS2 channel-binding input to a SCRAM client-final message.
///
/// Determines the GS2 header the client sends in client-first and the `c=`
/// attribute (base64 of `gs2_header || cbind_data`). A data-carrying enum, not
/// a struct with a discriminant plus a separate data field, so the two never
/// disagree: both the header and the `c=` value derive from the same variant,
/// and the invalid "discriminant says None but data is non-empty" state is
/// unrepresentable.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ScramChannelBinding {
    /// No binding: GS2 header `n,,`, `c=biws`.
    None,
    /// `tls-server-end-point` binding: GS2 header
    /// `p=tls-server-end-point,,`, `c=` over that header plus the raw
    /// [`crate::tls_server_end_point`] bytes.
    TlsServerEndPoint(Vec<u8>),
}

impl ScramChannelBinding {
    /// The GS2 header string the consumer must send in client-first (so the
    /// consumer and the `c=` computation never disagree). A PLUS consumer MUST
    /// build client-first from this, not from a literal, or the desync this
    /// type prevents re-enters at the consumer boundary.
    pub fn gs2_header(&self) -> &'static str {
        match self {
            Self::None => "n,,",
            Self::TlsServerEndPoint(_) => "p=tls-server-end-point,,",
        }
    }

    /// The channel-binding discriminant, for callers that select the mechanism
    /// name via [`ScramHash::mechanism_name`].
    pub fn binding(&self) -> ChannelBinding {
        match self {
            Self::None => ChannelBinding::None,
            Self::TlsServerEndPoint(_) => ChannelBinding::TlsServerEndPoint,
        }
    }
}

/// base64-decode a SASL continuation payload to its UTF-8 text form.
pub fn decode_continuation(data: &str) -> Result<String, SaslError> {
    use base64::Engine;

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data.trim())
        .map_err(|e| SaslError::Protocol(format!("invalid base64 SASL continuation: {e}")))?;
    String::from_utf8(bytes)
        .map_err(|e| SaslError::Protocol(format!("SASL continuation was not UTF-8: {e}")))
}

/// Escape `=` and `,` in a SCRAM username (RFC 5802 saslname rule).
pub fn escape_username(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

/// Build the base64 client-final message and the expected server signature
/// from the server-first message. Pure; no I/O.
pub fn scram_client_final(
    hash: ScramHash,
    password: &str,
    client_nonce: &str,
    client_first_bare: &str,
    server_first: &str,
    binding: &ScramChannelBinding,
) -> Result<(Secret, Vec<u8>), SaslError> {
    use base64::Engine;

    if scram_field(server_first, 'm').is_some() {
        return Err(SaslError::Protocol(
            "SCRAM mandatory extension field is not supported".into(),
        ));
    }
    let server_nonce = scram_field(server_first, 'r')
        .ok_or_else(|| SaslError::Protocol("SCRAM server-first message missing nonce".into()))?;
    if !server_nonce.starts_with(client_nonce) {
        return Err(SaslError::Protocol(
            "SCRAM server nonce does not extend client nonce".into(),
        ));
    }
    let salt_b64 = scram_field(server_first, 's')
        .ok_or_else(|| SaslError::Protocol("SCRAM server-first message missing salt".into()))?;
    let salt = base64::engine::general_purpose::STANDARD
        .decode(salt_b64)
        .map_err(|e| SaslError::Protocol(format!("invalid SCRAM salt: {e}")))?;
    let iterations = scram_field(server_first, 'i')
        .ok_or_else(|| {
            SaslError::Protocol("SCRAM server-first message missing iteration count".into())
        })?
        .parse::<u32>()
        .map_err(|e| SaslError::Protocol(format!("invalid SCRAM iteration count: {e}")))?;
    if iterations == 0 {
        return Err(SaslError::Protocol(
            "SCRAM iteration count must be greater than zero".into(),
        ));
    }

    // RFC 5802 Section 6: the `c=` attribute is base64(gs2-header || cbind-data).
    // For no binding the GS2 header is `n,,` and there is no cbind-data, so the
    // value is the well-known `biws`. For tls-server-end-point the header is
    // `p=tls-server-end-point,,` followed by the raw binding bytes.
    let mut cbind = binding.gs2_header().as_bytes().to_vec();
    if let ScramChannelBinding::TlsServerEndPoint(data) = binding {
        cbind.extend_from_slice(data);
    }
    let c_value = base64::engine::general_purpose::STANDARD.encode(&cbind);
    let client_final_without_proof = format!("c={c_value},r={server_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");
    let (proof, server_signature) = scram_proof_and_server_signature(
        hash,
        password.as_bytes(),
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

/// Verify the server-final message against the expected signature.
pub fn verify_server_final(server_final: &str, expected_signature: &[u8]) -> Result<(), SaslError> {
    use base64::Engine;

    if let Some(error) = scram_field(server_final, 'e') {
        return Err(SaslError::AuthFailed(format!(
            "SCRAM server error: {error}"
        )));
    }
    let verifier = scram_field(server_final, 'v')
        .ok_or_else(|| SaslError::Protocol("SCRAM server-final message missing verifier".into()))?;
    let actual = base64::engine::general_purpose::STANDARD
        .decode(verifier)
        .map_err(|e| SaslError::Protocol(format!("invalid SCRAM server verifier: {e}")))?;
    if actual != expected_signature {
        return Err(SaslError::Protocol(
            "SCRAM server signature verification failed".into(),
        ));
    }
    Ok(())
}

fn scram_field(message: &str, key: char) -> Option<&str> {
    message
        .split(',')
        .find_map(|field| field.strip_prefix(&format!("{key}=")))
}

fn scram_proof_and_server_signature(
    hash: ScramHash,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Vec<u8>, Vec<u8>), SaslError> {
    match hash {
        ScramHash::Sha1 => scram_proof_sha1(password, salt, iterations, auth_message),
        ScramHash::Sha256 => scram_proof_sha256(password, salt, iterations, auth_message),
    }
}

fn scram_proof_sha1(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &str,
) -> Result<(Vec<u8>, Vec<u8>), SaslError> {
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
) -> Result<(Vec<u8>, Vec<u8>), SaslError> {
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

fn hmac_digest<M>(key: &[u8], data: &[u8]) -> Result<Vec<u8>, SaslError>
where
    M: hmac::Mac + hmac::digest::KeyInit,
{
    let mut mac = <M as hmac::digest::KeyInit>::new_from_slice(key)
        .map_err(|e| SaslError::Protocol(format!("invalid SCRAM HMAC key: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(a, b)| a ^ b).collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn scram_sha1_client_final_matches_rfc_5802_vector() {
        use base64::Engine;

        let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
        let client_first_bare = "n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        let server_first = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
        let (client_final, server_signature) = scram_client_final(
            ScramHash::Sha1,
            "pencil",
            client_nonce,
            client_first_bare,
            server_first,
            &ScramChannelBinding::None,
        )
        .unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(client_final.as_str())
            .unwrap();
        let decoded = String::from_utf8(decoded).unwrap();
        assert_eq!(
            decoded,
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
        );
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(server_signature),
            "rmF9pqV8S7suAoZWja4dJRkFsKQ="
        );
    }

    /// Decode the base64 client-final and return its `c=` field.
    fn client_final_c_field(client_final: &Secret) -> String {
        use base64::Engine;

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(client_final.as_str())
            .unwrap();
        let decoded = String::from_utf8(decoded).unwrap();
        decoded
            .split(',')
            .find_map(|f| f.strip_prefix("c="))
            .unwrap()
            .to_owned()
    }

    #[test]
    fn scram_sha256_plus_c_field_is_base64_of_header_and_cbind() {
        use base64::Engine;

        let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
        let client_first_bare = "n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        let server_first = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
        let cbind = vec![0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04];

        let (plus_final, plus_sig) = scram_client_final(
            ScramHash::Sha256,
            "pencil",
            client_nonce,
            client_first_bare,
            server_first,
            &ScramChannelBinding::TlsServerEndPoint(cbind.clone()),
        )
        .unwrap();

        // RFC 5802 Section 6: c= is base64(gs2-header || cbind-data).
        let mut expected_cbind = b"p=tls-server-end-point,,".to_vec();
        expected_cbind.extend_from_slice(&cbind);
        let expected_c = base64::engine::general_purpose::STANDARD.encode(&expected_cbind);
        assert_eq!(client_final_c_field(&plus_final), expected_c);

        // PLUS and non-PLUS must diverge: different c= flows into auth_message,
        // hence a different proof and server signature for the same inputs.
        let (none_final, none_sig) = scram_client_final(
            ScramHash::Sha256,
            "pencil",
            client_nonce,
            client_first_bare,
            server_first,
            &ScramChannelBinding::None,
        )
        .unwrap();
        assert_eq!(client_final_c_field(&none_final), "biws");
        assert_ne!(plus_sig, none_sig);
        assert_ne!(plus_final.as_str(), none_final.as_str());
    }

    #[test]
    fn scram_channel_binding_gs2_header_and_discriminant() {
        assert_eq!(ScramChannelBinding::None.gs2_header(), "n,,");
        assert_eq!(
            ScramChannelBinding::TlsServerEndPoint(vec![0x01]).gs2_header(),
            "p=tls-server-end-point,,"
        );
        assert_eq!(ScramChannelBinding::None.binding(), ChannelBinding::None);
        assert_eq!(
            ScramChannelBinding::TlsServerEndPoint(vec![0x01]).binding(),
            ChannelBinding::TlsServerEndPoint
        );
    }

    #[test]
    fn mechanism_name_includes_plus_suffix_for_bound_variant() {
        assert_eq!(
            ScramHash::Sha256.mechanism_name(ChannelBinding::None),
            "SCRAM-SHA-256"
        );
        assert_eq!(
            ScramHash::Sha1.mechanism_name(ChannelBinding::None),
            "SCRAM-SHA-1"
        );
        assert_eq!(
            ScramHash::Sha256.mechanism_name(ChannelBinding::TlsServerEndPoint),
            "SCRAM-SHA-256-PLUS"
        );
        assert_eq!(
            ScramHash::Sha1.mechanism_name(ChannelBinding::TlsServerEndPoint),
            "SCRAM-SHA-1-PLUS"
        );
    }
}
