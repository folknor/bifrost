use super::charset::hex_digit;
use super::limits::Defect;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum TransferEncoding {
    #[default]
    SevenBit,
    EightBit,
    Binary,
    Base64,
    QuotedPrintable,
    Unknown,
}

impl TransferEncoding {
    pub fn from_token(token: &str) -> Self {
        match token.trim().to_ascii_lowercase().as_str() {
            "7bit" => Self::SevenBit,
            "8bit" => Self::EightBit,
            "binary" => Self::Binary,
            "base64" => Self::Base64,
            "quoted-printable" => Self::QuotedPrintable,
            _ => Self::Unknown,
        }
    }
}

pub(super) fn decode_transfer(
    encoding: TransferEncoding,
    body: &[u8],
    defects: &mut Vec<Defect>,
) -> Vec<u8> {
    match encoding {
        TransferEncoding::SevenBit
        | TransferEncoding::EightBit
        | TransferEncoding::Binary
        | TransferEncoding::Unknown => {
            if matches!(encoding, TransferEncoding::Unknown) {
                defects.push(Defect::UnknownTransferEncoding);
            }
            body.to_vec()
        }
        TransferEncoding::Base64 => decode_base64(body, defects),
        TransferEncoding::QuotedPrintable => decode_quoted_printable(body, defects),
    }
}

/// Decode base64 leniently: any octet outside the alphabet is skipped,
/// padding is optional, and a trailing group of a single character (which
/// encodes no whole octet) is dropped. A malformed run never costs the rest
/// of the payload, which a strict decoder would discard wholesale.
fn decode_base64(body: &[u8], defects: &mut Vec<Defect>) -> Vec<u8> {
    use base64::Engine;
    let mut clean = Vec::with_capacity(body.len());
    let mut malformed = false;
    for byte in body {
        match *byte {
            b'=' => {}
            byte if byte.is_ascii_whitespace() => {}
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/') => clean.push(byte),
            _ => malformed = true,
        }
    }
    if clean.len() % 4 != 0 {
        malformed = true;
        if clean.len() % 4 == 1 {
            clean.pop();
        }
    }
    let decoded = FORGIVING_BASE64.decode(&clean).unwrap_or_else(|_| {
        malformed = true;
        Vec::new()
    });
    if malformed {
        defects.push(Defect::MalformedBase64);
    }
    decoded
}

/// Unpadded standard alphabet, tolerating the non-zero trailing bits real
/// encoders emit on a final partial group.
const FORGIVING_BASE64: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    base64::engine::GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent)
        .with_decode_allow_trailing_bits(true),
);

/// Decode quoted-printable per RFC 2045 section 6.7. Whitespace immediately
/// before a line break is transport padding and is dropped; a malformed `=XY`
/// is emitted literally.
fn decode_quoted_printable(body: &[u8], defects: &mut Vec<Defect>) -> Vec<u8> {
    let mut output = Vec::with_capacity(body.len());
    let mut pending = Vec::new();
    let mut i = 0;
    let mut malformed = false;
    while i < body.len() {
        match body[i] {
            b' ' | b'\t' => {
                pending.push(body[i]);
                i += 1;
            }
            b'\r' | b'\n' => {
                pending.clear();
                output.push(body[i]);
                i += 1;
            }
            b'=' if i + 1 < body.len() && body[i + 1] == b'\n' => {
                pending.clear();
                i += 2;
            }
            b'=' if i + 2 < body.len() && body[i + 1] == b'\r' && body[i + 2] == b'\n' => {
                pending.clear();
                i += 3;
            }
            b'=' => {
                output.append(&mut pending);
                if i + 2 < body.len()
                    && let (Some(high), Some(low)) =
                        (hex_digit(body[i + 1]), hex_digit(body[i + 2]))
                {
                    output.push(high << 4 | low);
                    i += 3;
                } else {
                    malformed = true;
                    output.push(body[i]);
                    i += 1;
                }
            }
            byte => {
                output.append(&mut pending);
                output.push(byte);
                i += 1;
            }
        }
    }
    output.append(&mut pending);
    if malformed {
        defects.push(Defect::MalformedQuotedPrintable);
    }
    output
}

#[cfg(test)]
#[path = "transfer_tests.rs"]
mod tests;
