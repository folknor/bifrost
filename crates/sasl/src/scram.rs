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

impl ScramHash {
    /// The SASL mechanism name advertised on the wire.
    pub fn mechanism_name(self) -> &'static str {
        match self {
            Self::Sha1 => "SCRAM-SHA-1",
            Self::Sha256 => "SCRAM-SHA-256",
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

    let client_final_without_proof = format!("c=biws,r={server_nonce}");
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
}
