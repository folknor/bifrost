use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::{self, Display},
    time::SystemTime,
};

use ed25519_compact::{KeyPair, Seed};
use rsa::{PrivateKey as RsaPrivateKey, pkcs1::DecodeRsaPrivateKey, pkcs1v15::Pkcs1v15Sign};
use sha2::{Digest, Sha256};

use crate::{
    error::Error,
    message::{
        Message,
        header::{HeaderName, HeaderValue},
    },
};

/// Describe Dkim Canonicalization to apply to either body or headers
#[derive(Copy, Clone, Debug)]
#[non_exhaustive]
pub enum DkimCanonicalizationType {
    Simple,
    Relaxed,
}

impl Display for DkimCanonicalizationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DkimCanonicalizationType::Simple => "simple",
            DkimCanonicalizationType::Relaxed => "relaxed",
        })
    }
}

/// Describe Canonicalization to be applied before signing
#[derive(Copy, Clone, Debug)]
pub struct DkimCanonicalization {
    pub header: DkimCanonicalizationType,
    pub body: DkimCanonicalizationType,
}

impl Default for DkimCanonicalization {
    fn default() -> Self {
        DkimCanonicalization {
            header: DkimCanonicalizationType::Relaxed,
            body: DkimCanonicalizationType::Relaxed,
        }
    }
}

/// Format canonicalization to be shown in Dkim header
impl Display for DkimCanonicalization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.header, self.body)
    }
}

/// Describe the algorithm used for signing the message
#[derive(Copy, Clone, Debug)]
#[non_exhaustive]
pub enum DkimSigningAlgorithm {
    Rsa,
    Ed25519,
}

impl Display for DkimSigningAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DkimSigningAlgorithm::Rsa => "rsa",
            DkimSigningAlgorithm::Ed25519 => "ed25519",
        })
    }
}

/// Describe [`DkimSigningKey`] key error
#[derive(Debug)]
pub struct DkimSigningKeyError(InnerDkimSigningKeyError);

#[derive(Debug)]
enum InnerDkimSigningKeyError {
    Base64(base64::DecodeError),
    Rsa(rsa::pkcs1::Error),
    Ed25519(ed25519_compact::Error),
}

impl Display for DkimSigningKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match &self.0 {
            InnerDkimSigningKeyError::Base64(_err) => "base64 decode error",
            InnerDkimSigningKeyError::Rsa(_err) => "rsa decode error",
            InnerDkimSigningKeyError::Ed25519(_err) => "ed25519 decode error",
        })
    }
}

impl StdError for DkimSigningKeyError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(match &self.0 {
            InnerDkimSigningKeyError::Base64(err) => err,
            InnerDkimSigningKeyError::Rsa(err) => err,
            InnerDkimSigningKeyError::Ed25519(err) => err,
        })
    }
}

/// Signing key carried by [`DkimConfig`].
///
/// Existing RSA private keys and Ed25519 key pairs convert with `From`;
/// [`DkimSigningKey::new`] parses a PKCS#1 PEM or a base64-encoded seed.
#[derive(Debug)]
pub struct DkimSigningKey(InnerDkimSigningKey);

#[derive(Debug)]
enum InnerDkimSigningKey {
    Rsa(RsaPrivateKey),
    Ed25519(KeyPair),
}

impl From<RsaPrivateKey> for DkimSigningKey {
    fn from(value: RsaPrivateKey) -> Self {
        Self(InnerDkimSigningKey::Rsa(value))
    }
}

impl From<KeyPair> for DkimSigningKey {
    fn from(value: KeyPair) -> Self {
        Self(InnerDkimSigningKey::Ed25519(value))
    }
}

impl DkimSigningKey {
    /// Parse a PKCS#1 PEM RSA key or a base64-encoded 32-byte Ed25519 seed.
    pub fn new(
        private_key: &str,
        algorithm: DkimSigningAlgorithm,
    ) -> Result<DkimSigningKey, DkimSigningKeyError> {
        Ok(Self(match algorithm {
            DkimSigningAlgorithm::Rsa => InnerDkimSigningKey::Rsa(
                RsaPrivateKey::from_pkcs1_pem(private_key)
                    .map_err(|err| DkimSigningKeyError(InnerDkimSigningKeyError::Rsa(err)))?,
            ),
            DkimSigningAlgorithm::Ed25519 => {
                let seed_bytes: [u8; 32] = crate::base64::decode(private_key)
                    .map_err(|err| DkimSigningKeyError(InnerDkimSigningKeyError::Base64(err)))?
                    .try_into()
                    .map_err(|_| {
                        DkimSigningKeyError(InnerDkimSigningKeyError::Ed25519(
                            ed25519_compact::Error::InvalidSeed,
                        ))
                    })?;
                InnerDkimSigningKey::Ed25519(KeyPair::from_seed(Seed::new(seed_bytes)))
            }
        }))
    }
    fn get_signing_algorithm(&self) -> DkimSigningAlgorithm {
        match self.0 {
            InnerDkimSigningKey::Rsa(_) => DkimSigningAlgorithm::Rsa,
            InnerDkimSigningKey::Ed25519(_) => DkimSigningAlgorithm::Ed25519,
        }
    }
}

/// A struct to describe Dkim configuration applied when signing a message
#[derive(Debug)]
pub struct DkimConfig {
    /// The name of the key published in DNS
    selector: String,
    /// The domain for which we sign the message
    domain: String,
    /// Signing key for the selected RSA or Ed25519 algorithm.
    private_key: DkimSigningKey,
    /// A list of header names to be included in the signature. Signing of more than one
    /// header with the same name is not supported
    headers: Vec<HeaderName>,
    /// The signing algorithm to be used when signing
    canonicalization: DkimCanonicalization,
}

impl DkimConfig {
    /// Create a default signature configuration with a set of headers and "relaxed/relaxed"
    /// canonicalization
    pub fn default_config(
        selector: String,
        domain: String,
        private_key: DkimSigningKey,
    ) -> DkimConfig {
        DkimConfig {
            selector,
            domain,
            private_key,
            headers: vec![
                HeaderName::new_from_ascii_str("From"),
                HeaderName::new_from_ascii_str("Subject"),
                HeaderName::new_from_ascii_str("To"),
                HeaderName::new_from_ascii_str("Date"),
            ],
            canonicalization: DkimCanonicalization::default(),
        }
    }

    /// Create a [`DkimConfig`]
    pub fn new(
        selector: String,
        domain: String,
        private_key: DkimSigningKey,
        headers: Vec<HeaderName>,
        canonicalization: DkimCanonicalization,
    ) -> DkimConfig {
        DkimConfig {
            selector,
            domain,
            private_key,
            headers,
            canonicalization,
        }
    }
}

fn invalid(message: &str) -> Error {
    Error::InvalidInput(format!("DKIM: {message}"))
}

/// Refuse a configuration whose signature no verifier could accept.
fn validate_config(config: &DkimConfig) -> Result<(), Error> {
    let labels_ok = |name: &str, minimum: usize| {
        let labels: Vec<&str> = name.split('.').collect();
        labels.len() >= minimum
            && labels.iter().all(|label| {
                (1..=63).contains(&label.len())
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                    && !label.starts_with('-')
                    && !label.ends_with('-')
            })
    };
    // RFC 6376 3.5: d= is a domain name (two labels or more), s= one or
    // more labels, and the key lookup name must be a usable DNS name.
    if !labels_ok(&config.domain, 2) {
        return Err(invalid("signing domain is not a multi-label domain name"));
    }
    if !labels_ok(&config.selector, 1) {
        return Err(invalid("selector is not a sequence of DNS labels"));
    }
    if config.selector.len() + "._domainkey.".len() + config.domain.len() > 253 {
        return Err(invalid("selector._domainkey.domain exceeds 253 octets"));
    }
    // A `;` would end the h= tag and smuggle a tag into the signature.
    if config.headers.iter().any(|name| name.contains(';')) {
        return Err(invalid("signed header name contains ';'"));
    }
    // RFC 6376 5.4: From must be signed.
    if !config
        .headers
        .iter()
        .any(|name| name.eq_ignore_ascii_case("From"))
    {
        return Err(invalid("signed header list must include From"));
    }
    Ok(())
}

/// The unsigned DKIM-Signature field: every tag folded at fixed points and
/// `b=` always opening its own line, so the signature appended later never
/// moves anything before it. Stripping the `b=` value from the final field
/// yields exactly this template, which is what was hashed.
fn dkim_template(
    config: &DkimConfig,
    timestamp: u64,
    headers_list: &[String],
    body_hash: &str,
) -> String {
    let mut field = format!(
        "DKIM-Signature: v=1; a={algorithm}-sha256; d={domain}; s={selector};\r\n c={canon}; q=dns/txt; t={timestamp};\r\n h=",
        algorithm = config.private_key.get_signing_algorithm(),
        domain = config.domain,
        selector = config.selector,
        canon = config.canonicalization,
    );
    // FWS is legal around the colons of h= (RFC 6376 3.5).
    let mut column = " h=".len();
    for (index, name) in headers_list.iter().enumerate() {
        if index > 0 {
            if column + 1 + name.len() > 76 {
                field.push_str("\r\n :");
                column = 2;
            } else {
                field.push(':');
                column += 1;
            }
        }
        field.push_str(name);
        column += name.len();
    }
    field.push_str(&format!(";\r\n bh={body_hash};\r\n b="));
    field
}

/// Append the signature to the template, folded in 72-octet chunks; FWS
/// inside the b= value is ignored by verifiers (RFC 6376 3.5).
fn dkim_signed_field(template: &str, signature: &str) -> String {
    let chunks: Vec<&str> = signature
        .as_bytes()
        .chunks(72)
        .map(|chunk| std::str::from_utf8(chunk).expect("base64 is ASCII"))
        .collect();
    format!("{template}{}", chunks.join("\r\n "))
}

/// Split serialized header bytes into physical fields, each with its CRLF.
fn header_fields(block: &str) -> Vec<(&str, &str)> {
    let mut fields: Vec<(&str, &str)> = Vec::new();
    let mut start = 0;
    let bytes = block.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let end = block[index..]
            .find("\r\n")
            .map_or(block.len(), |offset| index + offset + 2);
        let continuation = matches!(bytes.get(end), Some(b' ' | b'\t'));
        if !continuation {
            let field = &block[start..end];
            let name = field.split(':').next().unwrap_or_default().trim_end();
            fields.push((name, field));
            start = end;
        }
        index = end;
    }
    fields
}

/// Canonicalize the fields named in `headers_list`, selected from the
/// emitted header block bottom-up per RFC 6376 5.4.2: a repeated name takes
/// successive occurrences upward, an absent occurrence contributes nothing.
/// Simple canonicalization keeps the emitted bytes, name spelling included.
fn dkim_canonicalize_headers<'a>(
    headers_list: impl IntoIterator<Item = &'a str>,
    header_block: &str,
    canonicalization: DkimCanonicalizationType,
) -> String {
    let fields = header_fields(header_block);
    let mut used = vec![false; fields.len()];
    let mut out = String::new();
    for name in headers_list {
        let Some(position) = (0..fields.len())
            .rev()
            .find(|&i| !used[i] && fields[i].0.eq_ignore_ascii_case(name))
        else {
            continue;
        };
        used[position] = true;
        let field = fields[position].1;
        match canonicalization {
            DkimCanonicalizationType::Simple => out.push_str(field),
            DkimCanonicalizationType::Relaxed => {
                out.push_str(&dkim_canonicalize_headers_relaxed(field));
            }
        }
    }
    out
}

/// Canonicalize the body of an email
fn dkim_canonicalize_body(
    mut body: &[u8],
    canonicalization: DkimCanonicalizationType,
) -> Cow<'_, [u8]> {
    match canonicalization {
        DkimCanonicalizationType::Simple => {
            // Remove empty lines at end
            while body.ends_with(b"\r\n\r\n") {
                body = &body[..body.len() - 2];
            }
            if body.ends_with(b"\r\n") {
                Cow::Borrowed(body)
            } else {
                let mut out = Vec::with_capacity(body.len() + 2);
                out.extend_from_slice(body);
                out.extend_from_slice(b"\r\n");
                Cow::Owned(out)
            }
        }
        DkimCanonicalizationType::Relaxed => {
            let mut out = Vec::with_capacity(body.len());
            loop {
                match body {
                    [b' ' | b'\t', b'\r', b'\n', ..] => {}
                    [b' ' | b'\t', b' ' | b'\t', ..] => {}
                    [b' ' | b'\t', ..] => out.push(b' '),
                    [c, ..] => out.push(*c),
                    [] => break,
                }
                body = &body[1..];
            }
            // Remove empty lines at end
            while out.ends_with(b"\r\n") {
                out.truncate(out.len() - 2);
            }
            if !out.is_empty() {
                out.extend_from_slice(b"\r\n");
            }
            Cow::Owned(out)
        }
    }
}

fn dkim_canonicalize_headers_relaxed(headers: &str) -> String {
    let input = headers.as_bytes();
    let mut out = Vec::with_capacity(input.len());
    let mut offset = 0;

    while offset < input.len() {
        let Some(relative_colon) = input[offset..].iter().position(|byte| *byte == b':') else {
            out.extend_from_slice(&input[offset..]);
            break;
        };
        let colon = offset + relative_colon;
        let mut name_end = colon;
        while name_end > offset && matches!(input[name_end - 1], b' ' | b'\t') {
            name_end -= 1;
        }
        out.extend(input[offset..name_end].iter().map(u8::to_ascii_lowercase));
        out.push(b':');

        offset = colon + 1;
        let value_start = out.len();
        let mut pending_space = false;
        loop {
            match input.get(offset..) {
                Some([b'\r', b'\n', b' ' | b'\t', ..]) => {
                    pending_space = out.len() > value_start;
                    offset += 2;
                    while matches!(input.get(offset), Some(b' ' | b'\t')) {
                        offset += 1;
                    }
                }
                Some([b'\r', b'\n', ..]) => {
                    offset += 2;
                    out.extend_from_slice(b"\r\n");
                    break;
                }
                Some([b' ' | b'\t', ..]) => {
                    pending_space = out.len() > value_start;
                    offset += 1;
                }
                Some([byte, ..]) => {
                    if pending_space {
                        out.push(b' ');
                        pending_space = false;
                    }
                    out.push(*byte);
                    offset += 1;
                }
                Some([]) | None => break,
            }
        }
    }

    String::from_utf8(out).expect("serialized email headers are valid UTF-8")
}

/// Sign with Dkim a message by adding Dkim-Signature header created with configuration expressed by
/// `dkim_config`
///
/// The signer reads the final serialized bytes, not the header model: the
/// signed fields are selected from the emitted header block (message
/// headers followed by the root MIME part's headers, as they go on the
/// wire) and the body hash covers the emitted body. An existing message
/// level DKIM-Signature, which this insert replaces, is left out of the
/// snapshot so the signed set is exactly the delivered one.
///
/// # Errors
///
/// `InvalidInput` when the signing domain or selector is not a DNS name,
/// the key lookup name exceeds 253 octets, a signed header name holds `;`,
/// From is not signed, or the signature field cannot be encoded. The
/// message is unchanged on error.
pub fn dkim_sign(message: &mut Message, dkim_config: &DkimConfig) -> Result<(), Error> {
    dkim_sign_fixed_time(message, dkim_config, crate::time::now())
}

fn dkim_sign_fixed_time(
    message: &mut Message,
    dkim_config: &DkimConfig,
    timestamp: SystemTime,
) -> Result<(), Error> {
    validate_config(dkim_config)?;
    // A pre-epoch clock yields t=0 rather than panicking the signer; DKIM
    // timestamps are advisory and a verifier treats a bad one as stale.
    let timestamp = timestamp
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());

    let mut snapshot = message.clone();
    snapshot.headers.remove_raw("DKIM-Signature");
    let bytes = snapshot.formatted();
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| invalid("message has no header/body separator"))?;
    let header_block = std::str::from_utf8(&bytes[..split + 2])
        .map_err(|_| invalid("header block is not UTF-8"))?;
    let body = &bytes[split + 4..];

    let body_hash = Sha256::digest(dkim_canonicalize_body(
        body,
        dkim_config.canonicalization.body,
    ));
    let bh = crate::base64::encode(body_hash);
    let headers_list: Vec<String> = dkim_config
        .headers
        .iter()
        .map(|name| match dkim_config.canonicalization.header {
            DkimCanonicalizationType::Relaxed => name.to_ascii_lowercase(),
            DkimCanonicalizationType::Simple => name.to_string(),
        })
        .collect();
    let template = dkim_template(dkim_config, timestamp, &headers_list, &bh);
    let signed_headers = dkim_canonicalize_headers(
        dkim_config.headers.iter().map(AsRef::as_ref),
        header_block,
        dkim_config.canonicalization.header,
    );
    let canonicalized_template = match dkim_config.canonicalization.header {
        DkimCanonicalizationType::Simple => template.clone(),
        DkimCanonicalizationType::Relaxed => dkim_canonicalize_headers_relaxed(&template),
    };
    let mut hashed_headers = Sha256::new();
    hashed_headers.update(signed_headers.as_bytes());
    hashed_headers.update(canonicalized_template.as_bytes());
    let hashed_headers = hashed_headers.finalize();
    let signature = match &dkim_config.private_key.0 {
        InnerDkimSigningKey::Rsa(private_key) => crate::base64::encode(
            private_key
                .sign(Pkcs1v15Sign::new::<Sha256>(), &hashed_headers)
                .map_err(|_| invalid("RSA signing failed"))?,
        ),
        InnerDkimSigningKey::Ed25519(keypair) => {
            crate::base64::encode(keypair.sk.sign(hashed_headers, None).as_ref())
        }
    };
    let field = dkim_signed_field(&template, &signature);
    let value = field
        .strip_prefix("DKIM-Signature: ")
        .expect("the template starts with its field name");
    // Inserted byte-for-byte: the pre-encoded path validates framing only
    // and never refolds, so the emitted field is the hashed template plus
    // the b= value.
    message
        .headers
        .insert_raw(HeaderValue::dangerous_new_pre_encoded(
            HeaderName::new_from_ascii_str("DKIM-Signature"),
            value.replace("\r\n", ""),
            value.to_owned(),
        ))
}

#[cfg(test)]
mod test {
    use rsa::{PrivateKey as RsaPrivateKey, pkcs1::DecodeRsaPrivateKey};
    use sha2::{Digest, Sha256};

    use super::{
        super::{
            Header, Message,
            header::{HeaderName, HeaderValue, Headers},
        },
        DkimCanonicalization, DkimCanonicalizationType, DkimConfig, DkimSigningAlgorithm,
        DkimSigningKey, KeyPair, Seed, dkim_canonicalize_body, dkim_canonicalize_headers,
        dkim_canonicalize_headers_relaxed, dkim_sign_fixed_time,
    };
    use crate::StdError;

    const KEY_RSA: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAwOsW7UFcWn1ch3UM8Mll5qZH5hVHKJQ8Z0tUlebUECq0vjw6
VcsIucZ/B70VpCN63whyi7oApdCIS1o0zad7f0UaW/BfxXADqdcFL36uMaG0RHer
uSASjQGnsl9Kozt/dXiDZX5ngjr/arLJhNZSNR4/9VSwqbE2OPXaSaQ9BsqneD0P
8dCVSfkkDZCcfC2864z7hvC01lFzWQKF36ZAoGBERHScHtFMAzUOgGuqqPiP5khw
DQB3Ffccf+BsWLU2OOteshUwTGjpoangbPCYj6kckwNm440lQwuqTinpC92yyIE5
Ol8psNMW49DLowAeZb6JrjLhD+wY9bghTaOkcwIDAQABAoIBAHTZ8LkkrdvhsvoZ
XA088AwVC9fBa6iYoT2v0zw45JomQ/Q2Zt8wa8ibAradQU56byJI65jWwS2ucd+y
c+ldWOBt6tllb50XjCCDrRBnmvtVBuux0MIBOztNlVXlgj/8+ecdZ/lB51Bqi+sF
ACsF5iVmfTcMZTVjsYQu5llUseI6Lwgqpx6ktaXD2PVsVo9Gf01ssZ4GCy69wB/3
20CsOz4LEpSYkq1oE98lMMGCfD7py3L9kWHYNNisam78GM+1ynRxRGwEDUbz6pxs
fGPIAwHLaZsOmibPkBB0PJTW742w86qQ8KAqC6ZbRYOF19rSMj3oTfRnPMHn9Uu5
N8eQcoECgYEA97SMUrz2hqII5i8igKylO9kV8pjcIWKI0rdt8MKj4FXTNYjjO9I+
41ONOjhUOpFci/G3YRKi8UiwbKxIRTvIxNMh2xj6Ws3iO9gQHK1j8xTWxJdjEBEz
EuZI59Mi5H7fxSL1W+n8nS8JVsaH93rvQErngqTUAsihAzjxHWdFwm0CgYEAx2Dh
claESJP2cOKgYp+SUNwc26qMaqnl1f37Yn+AflrQOfgQqJe5TRbicEC+nFlm6XUt
3st1Nj29H0uOMmMZDmDCO+cOs5Qv5A9pG6jSC6wM+2KNHQDtrxlakBFygePEPVVy
GXaY9DRa9Q4/4ataxDR2/VvIAWfEEtMTJIBDtl8CgYAIXEuwLziS6r0qJ8UeWrVp
A7a97XLgnZbIpfBMBAXL+JmcYPZqenos6hEGOgh9wZJCFvJ9kEd3pWBvCpGV5KKu
IgIuhvVMQ06zfmNs1F1fQwDMud9aF3qF1Mf5KyMuWynqWXe2lns0QvYpu6GzNK8G
mICf5DhTr7nfhfh9aZLtMQKBgCxKsmqzG5n//MxhHB4sstVxwJtwDNeZPKzISnM8
PfBT/lQSbqj1Y73japRjXbTgC4Ore3A2JKjTGFN+dm1tJGDUT/H8x4BPWEBCyCfT
3i2noA6sewrJbQPsDvlYVubSEYNKmxlbBmmhw98StlBMv9I8kX6BSDI/uggwid0e
/WvjAoGBAKpZ0UOKQyrl9reBiUfrpRCvIMakBMd79kNiH+5y0Soq/wCAnAuABayj
XEIBhFv+HxeLEnT7YV+Zzqp5L9kKw/EU4ik3JX/XsEihdSxEuGX00ZYOw05FEfpW
cJ5Ku0OTwRtSMaseRPX+T4EfG1Caa/eunPPN4rh+CSup2BVVarOT
-----END RSA PRIVATE KEY-----";

    #[derive(Clone)]
    struct TestHeader(String);

    impl Header for TestHeader {
        fn name() -> HeaderName {
            HeaderName::new_from_ascii_str("Test")
        }

        fn parse(s: &str) -> Result<Self, Box<dyn StdError + Send + Sync>> {
            Ok(Self(s.into()))
        }

        fn display(&self) -> HeaderValue {
            HeaderValue::new(Self::name(), self.0.clone())
        }
    }

    /// The emitted header block of a message, as the signer reads it.
    fn block(message: &Message) -> String {
        let bytes = String::from_utf8(message.formatted()).unwrap();
        let end = bytes.find("\r\n\r\n").unwrap();
        bytes[..end + 2].to_owned()
    }

    fn test_message() -> Message {
        Message::builder()
            .from("Test O'Leary <test+ezrz@example.net>".parse().unwrap())
            .to("Test2 <test2@example.org>".parse().unwrap())
            .date(std::time::UNIX_EPOCH)
            .header(TestHeader("test  test very very long with spaces and extra spaces   \twill be folded to several lines ".to_owned()))
            .subject("Test with utf-8 ë")
            .body("test\r\n\r\ntest   \ttest\r\n\r\n\r\n".to_owned()).unwrap()
    }

    #[test]
    fn test_headers_simple_canonicalize() {
        let message = test_message();
        assert_eq!(
            dkim_canonicalize_headers(
                ["From", "Test"],
                &block(&message),
                DkimCanonicalizationType::Simple
            ),
            "From: Test O'Leary <test+ezrz@example.net>\r\nTest: test  test very very long with spaces and extra spaces   \twill be folded to several lines \r\n"
        );
    }

    #[test]
    fn test_headers_relaxed_canonicalize() {
        let message = test_message();
        assert_eq!(
            dkim_canonicalize_headers(
                ["From", "Test"],
                &block(&message),
                DkimCanonicalizationType::Relaxed
            ),
            "from:Test O'Leary <test+ezrz@example.net>\r\ntest:test test very very long with spaces and extra spaces will be folded to several lines\r\n"
        );
    }

    #[test]
    fn test_body_simple_canonicalize() {
        let body = b" C \r\nD \t E\r\n\r\n\r\n";
        assert_eq!(
            dkim_canonicalize_body(body, DkimCanonicalizationType::Simple).into_owned(),
            b" C \r\nD \t E\r\n"
        );
    }

    #[test]
    fn test_body_relaxed_canonicalize() {
        let body = b" C \r\nD \t E\r\n\tF\r\n\t\r\n\r\n\r\n";
        assert_eq!(
            dkim_canonicalize_body(body, DkimCanonicalizationType::Relaxed).into_owned(),
            b" C\r\nD E\r\n F\r\n"
        );
    }

    #[test]
    fn relaxed_header_canonicalization_matches_the_rfc_6376_example() {
        // RFC 6376 3.4.5, header example (bracketed descriptors expanded):
        //   A: X<CRLF>
        //   B <SP> : <SP> Y <HTAB><CRLF>
        //   <HTAB> Z <SP><SP><CRLF>
        // whose "relaxed" output the RFC gives as:
        //   a:X<CRLF>
        //   b:Y<SP>Z<CRLF>
        //
        assert_eq!(
            dkim_canonicalize_headers_relaxed("A: X\r\nB : Y\t\r\n\tZ  \r\n"),
            "a:X\r\nb:Y Z\r\n"
        );
    }

    #[test]
    fn simple_header_canonicalization_is_a_byte_identity() {
        let mut headers = Headers::new();
        headers
            .insert_raw(HeaderValue::new(
                HeaderName::new_from_ascii_str("Subject"),
                "hello".to_owned(),
            ))
            .unwrap();

        assert_eq!(
            dkim_canonicalize_headers(
                ["Subject"],
                &headers.to_string(),
                DkimCanonicalizationType::Simple
            ),
            "Subject: hello\r\n"
        );
    }

    #[test]
    fn a_signed_header_absent_from_the_message_contributes_nothing() {
        // RFC 6376 5.4.2 allows naming a non-existent header in `h=`; the
        // verifier treats it as an empty value. `dkim_sign_fixed_time` puts
        // every configured name into `h=` but only hashes the ones that exist,
        // which is the matching behavior.
        let message = test_message();

        assert_eq!(
            dkim_canonicalize_headers(
                ["Reply-To"],
                &block(&message),
                DkimCanonicalizationType::Relaxed
            ),
            ""
        );
    }

    #[test]
    fn only_one_instance_of_a_repeated_header_is_signed() {
        // `DkimConfig` documents that signing more than one header with the
        // same name is unsupported. `Headers::insert_raw` overwrites in place,
        // so the last write wins and the earlier value is not hashed at all.
        let mut headers = Headers::new();
        headers
            .insert_raw(HeaderValue::new(
                HeaderName::new_from_ascii_str("Received"),
                "one".to_owned(),
            ))
            .unwrap();
        headers
            .insert_raw(HeaderValue::new(
                HeaderName::new_from_ascii_str("Received"),
                "two".to_owned(),
            ))
            .unwrap();

        assert_eq!(
            dkim_canonicalize_headers(
                ["Received"],
                &headers.to_string(),
                DkimCanonicalizationType::Relaxed
            ),
            "received:two\r\n"
        );
    }

    #[test]
    fn empty_body_canonicalization() {
        assert_eq!(
            dkim_canonicalize_body(b"", DkimCanonicalizationType::Simple).into_owned(),
            b"\r\n"
        );

        // RFC 6376 3.4.4: relaxed canonicalization of an empty body is the
        // null input. This half is correct.
        assert_eq!(
            dkim_canonicalize_body(b"", DkimCanonicalizationType::Relaxed).into_owned(),
            b""
        );
    }

    #[test]
    fn body_of_only_empty_lines_canonicalization() {
        assert_eq!(
            dkim_canonicalize_body(b"\r\n", DkimCanonicalizationType::Relaxed).into_owned(),
            b""
        );
        assert_eq!(
            dkim_canonicalize_body(b"\r\n\r\n\r\n", DkimCanonicalizationType::Relaxed).into_owned(),
            b""
        );

        // Simple is correct here: RFC 6376 3.4.3 collapses a trailing "*CRLF"
        // to exactly one CRLF.
        assert_eq!(
            dkim_canonicalize_body(b"\r\n\r\n\r\n", DkimCanonicalizationType::Simple).into_owned(),
            b"\r\n"
        );
    }

    #[test]
    fn body_canonicalization_appends_a_missing_final_crlf() {
        assert_eq!(
            dkim_canonicalize_body(b"abc", DkimCanonicalizationType::Simple).into_owned(),
            b"abc\r\n"
        );
        assert_eq!(
            dkim_canonicalize_body(b"abc", DkimCanonicalizationType::Relaxed).into_owned(),
            b"abc\r\n"
        );
    }

    #[test]
    fn dkim_signature_fold_is_stable_when_the_signature_is_inserted() {
        let signing_key = DkimSigningKey::new(KEY_RSA, DkimSigningAlgorithm::Rsa).unwrap();
        let config = DkimConfig::new(
            "dkimtest".to_owned(),
            "example.org".to_owned(),
            signing_key,
            vec![HeaderName::new_from_ascii_str("Date")],
            DkimCanonicalization {
                header: DkimCanonicalizationType::Simple,
                body: DkimCanonicalizationType::Simple,
            },
        );
        // An 8192-bit RSA signature is 1368 base64 octets.
        let template = super::dkim_template(&config, 0, &["Date".to_owned()], &"B".repeat(44));
        let signed = super::dkim_signed_field(&template, &"A".repeat(1368));
        assert!(
            signed.split("\r\n").all(|line| line.len() <= 78),
            "{signed}"
        );
        let (before, after) = signed.rsplit_once("\r\n b=").unwrap();
        assert_eq!(format!("{before}\r\n b="), template);
        assert_eq!(after.replace("\r\n ", ""), "A".repeat(1368));
    }

    #[test]
    fn relaxed_body_canonicalization_reduces_whitespace_per_rfc_6376_3_4_4() {
        // Step a: trailing WSP before a CRLF is dropped, and any run of WSP
        // inside a line collapses to one SP.
        assert_eq!(
            dkim_canonicalize_body(b"a \t \r\nb\t\r\n", DkimCanonicalizationType::Relaxed)
                .into_owned(),
            b"a\r\nb\r\n"
        );
        assert_eq!(
            dkim_canonicalize_body(b"x \t \t y\r\n", DkimCanonicalizationType::Relaxed)
                .into_owned(),
            b"x y\r\n"
        );
        // Leading whitespace on a line is whitespace *inside* the line and
        // collapses rather than disappearing.
        assert_eq!(
            dkim_canonicalize_body(b"  a\r\n", DkimCanonicalizationType::Relaxed).into_owned(),
            b" a\r\n"
        );
    }

    #[test]
    fn simple_body_canonicalization_preserves_everything_but_trailing_blank_lines() {
        assert_eq!(
            dkim_canonicalize_body(b"  a \t b  \r\n\r\n\r\n", DkimCanonicalizationType::Simple)
                .into_owned(),
            b"  a \t b  \r\n"
        );
    }

    #[test]
    fn default_config_uses_relaxed_relaxed() {
        let signing_key = DkimSigningKey::new(KEY_RSA, DkimSigningAlgorithm::Rsa).unwrap();
        let config = DkimConfig::default_config(
            "dkimtest".to_owned(),
            "example.org".to_owned(),
            signing_key,
        );

        assert_eq!(config.canonicalization.to_string(), "relaxed/relaxed");
    }

    #[test]
    fn signing_key_can_wrap_rsa_key_object() {
        let key = RsaPrivateKey::from_pkcs1_pem(KEY_RSA).unwrap();
        let signing_key = DkimSigningKey::from(key);

        assert!(matches!(
            signing_key.get_signing_algorithm(),
            DkimSigningAlgorithm::Rsa
        ));
    }

    #[test]
    fn signing_key_can_wrap_ed25519_key_object() {
        let key = KeyPair::from_seed(Seed::new([42; 32]));
        let signing_key = DkimSigningKey::from(key);

        assert!(matches!(
            signing_key.get_signing_algorithm(),
            DkimSigningAlgorithm::Ed25519
        ));
    }

    #[test]
    fn test_signature_rsa_simple() {
        let mut message = test_message();
        let signing_key = DkimSigningKey::new(KEY_RSA, DkimSigningAlgorithm::Rsa).unwrap();
        dkim_sign_fixed_time(
            &mut message,
            &DkimConfig::new(
                "dkimtest".to_owned(),
                "example.org".to_owned(),
                signing_key,
                vec![
                    HeaderName::new_from_ascii_str("Date"),
                    HeaderName::new_from_ascii_str("From"),
                    HeaderName::new_from_ascii_str("Subject"),
                    HeaderName::new_from_ascii_str("To"),
                ],
                DkimCanonicalization {
                    header: DkimCanonicalizationType::Simple,
                    body: DkimCanonicalizationType::Simple,
                },
            ),
            std::time::UNIX_EPOCH,
        )
        .unwrap();
        let signed = message.formatted();
        let signed = std::str::from_utf8(&signed).unwrap();
        assert!(signed.starts_with(TEST_MESSAGE_HEADERS), "{signed}");
        assert!(signed.ends_with(TEST_MESSAGE_BODY), "{signed}");
        assert_dkim_signature(
            signed,
            "simple/simple",
            "Date:From:Subject:To",
            "f3Zksdcjqa/xRBwdyFzIXWCcgP7XTgxjCgYsXOMKQl4=",
        );
    }

    #[test]
    fn test_signature_rsa_relaxed() {
        let mut message = test_message();
        let signing_key = DkimSigningKey::new(KEY_RSA, DkimSigningAlgorithm::Rsa).unwrap();
        dkim_sign_fixed_time(
            &mut message,
            &DkimConfig::new(
                "dkimtest".to_owned(),
                "example.org".to_owned(),
                signing_key,
                vec![
                    HeaderName::new_from_ascii_str("Date"),
                    HeaderName::new_from_ascii_str("From"),
                    HeaderName::new_from_ascii_str("Subject"),
                    HeaderName::new_from_ascii_str("To"),
                ],
                DkimCanonicalization {
                    header: DkimCanonicalizationType::Relaxed,
                    body: DkimCanonicalizationType::Relaxed,
                },
            ),
            std::time::UNIX_EPOCH,
        )
        .unwrap();
        let signed = message.formatted();
        let signed = std::str::from_utf8(&signed).unwrap();
        assert!(signed.starts_with(TEST_MESSAGE_HEADERS), "{signed}");
        assert!(signed.ends_with(TEST_MESSAGE_BODY), "{signed}");
        assert_dkim_signature(
            signed,
            "relaxed/relaxed",
            "date:from:subject:to",
            "qN8je6qJgWFGSnN2MycC/XKPbN6BOrMJyAX2h4m19Ss=",
        );
    }

    const TEST_MESSAGE_HEADERS: &str = std::concat!(
        "From: Test O'Leary <test+ezrz@example.net>\r\n",
        "To: Test2 <test2@example.org>\r\n",
        // The shared formatter writes the day without padding (RFC 5322
        // `day = 1*2DIGIT`).
        "Date: Thu, 1 Jan 1970 00:00:00 +0000\r\n",
        "Test: test  test very very long with spaces and extra spaces   \twill be folded to several lines \r\n",
        "Subject: =?UTF-8?B?VGVzdCB3aXRoIHV0Zi04IMOr?=\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "Content-Transfer-Encoding: 7bit\r\n",
    );

    const TEST_MESSAGE_BODY: &str = std::concat!(
        "\r\n",
        "test\r\n",
        "\r\n",
        "test   \ttest\r\n",
        "\r\n",
        "\r\n",
    );

    fn assert_dkim_signature(
        signed: &str,
        canonicalization: &str,
        headers_list: &str,
        body_hash: &str,
    ) {
        assert!(
            signed.contains("DKIM-Signature: v=1; a=rsa-sha256; d=example.org; s=dkimtest;\r\n"),
            "{signed}"
        );
        assert!(
            signed.contains(&format!(
                " c={canonicalization}; q=dns/txt; t=0;\r\n h={headers_list};\r\n"
            )),
            "{signed}"
        );
        assert!(
            signed.contains(&format!(" bh={body_hash};\r\n")),
            "{signed}"
        );

        let signature_line = signed
            .lines()
            .find(|line| line.starts_with(" b="))
            .expect("signed message should contain a DKIM signature value");
        assert!(signature_line.trim_end().len() > " b=".len());
    }

    /// A verifier written against the delivered bytes alone, sharing nothing
    /// with the signer but the RFC-pinned relaxed canonicalizer and the body
    /// canonicalizer: it unfolds the DKIM-Signature itself, selects the
    /// h= fields bottom-up from the bytes, and checks bh= and b=.
    #[allow(clippy::unwrap_in_result)]
    fn verify(signed: &[u8], key: &KeyPair) -> Result<(), String> {
        let signed = std::str::from_utf8(signed).unwrap();
        let (head, body) = signed.split_once("\r\n\r\n").unwrap();
        let head = format!("{head}\r\n");
        let mut fields: Vec<String> = Vec::new();
        for line in head.split_inclusive("\r\n") {
            if line.starts_with([' ', '\t']) {
                fields.last_mut().unwrap().push_str(line);
            } else {
                fields.push(line.to_owned());
            }
        }
        let name_of = |field: &str| field.split(':').next().unwrap().trim().to_ascii_lowercase();
        let signature = fields
            .iter()
            .position(|field| name_of(field) == "dkim-signature")
            .ok_or("no signature")?;
        let dkim = fields.remove(signature);
        let tags: std::collections::HashMap<String, String> = dkim
            .split_once(':')
            .unwrap()
            .1
            .split(';')
            .filter_map(|tag| tag.split_once('='))
            .map(|(k, v)| {
                (
                    k.split_whitespace().collect(),
                    v.split_whitespace().collect::<String>(),
                )
            })
            .collect();
        let (header_c, body_c) = tags["c"].split_once('/').unwrap();
        let canon = |c: &str| match c {
            "simple" => DkimCanonicalizationType::Simple,
            _ => DkimCanonicalizationType::Relaxed,
        };
        let body_hash = crate::base64::encode(Sha256::digest(dkim_canonicalize_body(
            body.as_bytes(),
            canon(body_c),
        )));
        if body_hash != tags["bh"] {
            return Err(format!("bh mismatch: {body_hash} != {}", tags["bh"]));
        }
        let mut used = vec![false; fields.len()];
        let mut input = String::new();
        for name in tags["h"].split(':') {
            if let Some(i) = (0..fields.len())
                .rev()
                .find(|&i| !used[i] && name_of(&fields[i]) == name.to_ascii_lowercase())
            {
                used[i] = true;
                input.push_str(&match canon(header_c) {
                    DkimCanonicalizationType::Simple => fields[i].clone(),
                    DkimCanonicalizationType::Relaxed => {
                        dkim_canonicalize_headers_relaxed(&fields[i])
                    }
                });
            }
        }
        // The signature field with the b= value removed, no trailing CRLF.
        let b_start = dkim
            .match_indices("b=")
            .map(|(i, _)| i)
            .find(|&i| {
                dkim[..i]
                    .trim_end_matches([' ', '\t', '\r', '\n'])
                    .ends_with(';')
            })
            .unwrap()
            + 2;
        let b_end = dkim[b_start..]
            .find(';')
            .map_or(dkim.len(), |o| b_start + o);
        let unsigned = format!("{}{}", &dkim[..b_start], &dkim[b_end..]);
        let unsigned = unsigned.trim_end_matches("\r\n");
        input.push_str(&match canon(header_c) {
            DkimCanonicalizationType::Simple => unsigned.to_owned(),
            DkimCanonicalizationType::Relaxed => dkim_canonicalize_headers_relaxed(unsigned),
        });
        let digest = Sha256::digest(input.as_bytes());
        let sig = crate::base64::decode(&tags["b"]).unwrap();
        key.pk
            .verify(
                digest,
                &ed25519_compact::Signature::from_slice(&sig).unwrap(),
            )
            .map_err(|error| format!("signature: {error}"))
    }

    fn multipart_message() -> Message {
        Message::builder()
            .from("Alice <alice@example.org>".parse().unwrap())
            .to("Bob <bob@example.net>".parse().unwrap())
            .subject("Quarterly report")
            .date(std::time::UNIX_EPOCH)
            .multipart(super::super::MultiPart::alternative_plain_html(
                "plain".to_owned(),
                "<p>html</p>".to_owned(),
            ))
            .unwrap()
    }

    fn ed25519_config(
        names: &[&'static str],
        canonicalization: DkimCanonicalizationType,
    ) -> (DkimConfig, KeyPair) {
        let key = KeyPair::from_seed(Seed::new([7; 32]));
        let config = DkimConfig::new(
            "sel".to_owned(),
            "example.org".to_owned(),
            DkimSigningKey::from(key.clone()),
            names
                .iter()
                .map(|name| HeaderName::new_from_ascii_str(name))
                .collect(),
            DkimCanonicalization {
                header: canonicalization,
                body: canonicalization,
            },
        );
        (config, key)
    }

    /// Final-byte verification: lowercase configured names under simple
    /// canonicalization (the emitted spelling must be hashed) and the root
    /// part's Content-Type (emitted after the message headers) both sign
    /// what a verifier reads.
    #[test]
    fn signature_verifies_against_the_delivered_bytes() {
        for canonicalization in [
            DkimCanonicalizationType::Simple,
            DkimCanonicalizationType::Relaxed,
        ] {
            let mut message = multipart_message();
            let (config, key) = ed25519_config(
                &[
                    "from",
                    "to",
                    "subject",
                    "date",
                    "content-type",
                    "mime-version",
                ],
                canonicalization,
            );
            dkim_sign_fixed_time(&mut message, &config, std::time::UNIX_EPOCH).unwrap();
            verify(&message.formatted(), &key).unwrap();
            // Re-signing replaces the field and still verifies.
            dkim_sign_fixed_time(&mut message, &config, std::time::UNIX_EPOCH).unwrap();
            verify(&message.formatted(), &key).unwrap();
        }
    }

    #[test]
    fn unverifiable_configurations_are_refused_without_mutation() {
        let message = multipart_message();
        let key = KeyPair::from_seed(Seed::new([7; 32]));
        let config = |domain: &str, selector: &str, names: Vec<HeaderName>| {
            DkimConfig::new(
                selector.to_owned(),
                domain.to_owned(),
                DkimSigningKey::from(key.clone()),
                names,
                DkimCanonicalization::default(),
            )
        };
        let from = || vec![HeaderName::new_from_ascii_str("From")];
        for bad in [
            config("localhost", "sel", from()),
            config("example.org", "sel; t=0", from()),
            config(
                "example.org",
                "sel",
                vec![HeaderName::new_from_ascii_str("To")],
            ),
            config(
                "example.org",
                "sel",
                vec![
                    HeaderName::new_from_ascii_str("From"),
                    HeaderName::new_from_ascii_str("X;z=1"),
                ],
            ),
            config(&format!("{}.example.org", "a".repeat(64)), "sel", from()),
        ] {
            let mut signed = message.clone();
            assert!(dkim_sign_fixed_time(&mut signed, &bad, std::time::UNIX_EPOCH).is_err());
            assert_eq!(signed.formatted(), message.formatted());
        }
    }
}
