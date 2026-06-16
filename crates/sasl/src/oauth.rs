//! OAuth SASL payload construction (XOAUTH2, OAUTHBEARER).
//!
//! XOAUTH2 is the Google-defined non-IETF mechanism; OAUTHBEARER is RFC 7628.
//! Both wrap a bearer access token. Payloads are returned as a zeroizing
//! `Secret` of the *raw* (un-base64'd) bytes; the protocol crate frames them
//! for the wire (IMAP base64-encodes for AUTHENTICATE; SMTP's AUTH command
//! base64-encodes downstream).

use crate::Secret;

/// XOAUTH2 client payload: `user=<user>\x01auth=Bearer <token>\x01\x01`.
///
/// XOAUTH2 has no GS2 framing, so neither field is GS2-escaped. The
/// `\x01`-delimited frame is byte-literal.
pub fn xoauth2_payload(user: &str, access_token: &str) -> Secret {
    Secret::from(format!("user={user}\x01auth=Bearer {access_token}\x01\x01"))
}

/// OAUTHBEARER (RFC 7628) client payload:
/// `n,a=<identity>,\x01auth=Bearer <token>\x01\x01`.
///
/// The `a=` authorization identity is a GS2 attribute and is GS2-escaped
/// (`,` -> `=2C`, `=` -> `=3D`). The optional RFC 7628 `host=` / `port=`
/// attributes are deliberately omitted: bearer auth does not require them and
/// omitting them keeps the builder transport-agnostic.
pub fn oauthbearer_payload(identity: &str, access_token: &str) -> Secret {
    let identity = crate::escape_username(identity);
    Secret::from(format!(
        "n,a={identity},\x01auth=Bearer {access_token}\x01\x01"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xoauth2_payload_pins_raw_bytes() {
        assert_eq!(
            xoauth2_payload("user", "token").as_str(),
            "user=user\x01auth=Bearer token\x01\x01"
        );
    }

    #[test]
    fn xoauth2_payload_alice_access_token() {
        // Mirrors SMTP test_accepts_zeroizing_secrets.
        assert_eq!(
            xoauth2_payload("alice", "access-token").as_str(),
            "user=alice\x01auth=Bearer access-token\x01\x01"
        );
    }

    #[test]
    fn oauthbearer_payload_omits_host_and_port() {
        // Mirrors SMTP test_oauthbearer.
        let payload = oauthbearer_payload(
            "user@example.com",
            "vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg==",
        );
        assert_eq!(
            payload.as_str(),
            "n,a=user@example.com,\x01auth=Bearer vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg==\x01\x01"
        );
        assert!(!payload.as_str().contains("\x01host="));
        assert!(!payload.as_str().contains("\x01port="));
    }

    #[test]
    fn oauthbearer_payload_escapes_gs2_identity() {
        // Mirrors SMTP test_oauthbearer_escapes_gs2_identity.
        assert_eq!(
            oauthbearer_payload("a,b=c", "token").as_str(),
            "n,a=a=2Cb=3Dc,\x01auth=Bearer token\x01\x01"
        );
    }
}
