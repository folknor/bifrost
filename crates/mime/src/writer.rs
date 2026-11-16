//! Structured outbound header encoding shared by composition interfaces.

use crate::render::malformed;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bifrost_types::compose::Address;
use bifrost_types::error::AccountError;

/// The grammar of an outbound header value. Inputs are unfolded values;
/// CR and LF in caller text are replaced with spaces, never trusted as folds.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum HeaderValue<'a> {
    /// Subject-like text; RFC 2047 is legal here.
    Unstructured(&'a str),
    /// Comma-separated phrases, with quoted strings and quoted-pairs.
    PhraseList(&'a str),
    /// An optional display phrase followed by an ASCII angle-bracketed list id.
    ListId(&'a str),
    /// Validated mailbox values, with optional display names.
    Addresses(&'a [Address]),
    /// Space-separated message identifiers, already bracketed when required.
    MessageIds(&'a str),
    /// A MIME token and its parameter name/value pairs.
    Parameterized {
        value: &'a str,
        parameters: &'a [(&'a str, &'a str)],
    },
    /// An ASCII structured value (Content-Type, a URL list, a date) emitted
    /// verbatim and folded only at whitespace outside quoted strings and
    /// angle brackets. Never encoded: controls and non-ASCII are refused.
    Structured(&'a str),
    /// A value whose grammar the caller did not declare: the caller's own
    /// wire spelling. Legal wire text (printable ASCII and WSP, CRLF only as
    /// a fold before WSP, every physical line within 998 octets) is emitted
    /// byte-for-byte, with no trim and no inserted fold, so a caller Date or
    /// any hand-folded value survives exactly. Otherwise words carrying
    /// non-ASCII or control octets become RFC 2047 encoded words and the
    /// result folds at 78; an ASCII word is never encoded because the
    /// grammar may forbid an encoded word there, so one that cannot fit 998
    /// octets is refused.
    Raw(&'a str),
    /// An already folded value. Only framing is validated (printable ASCII
    /// and WSP, CRLF only before WSP, lines within 998 octets); it is
    /// refused rather than normalized, and emitted byte-for-byte.
    PreEncoded(&'a str),
}

/// A control octet that may never appear raw in a header value: C0 other
/// than HTAB, and DEL.
fn is_control(b: u8) -> bool {
    (b < 0x20 && b != b'\t') || b == 0x7f
}

/// Whether `value` is legal wire text for a field named `name`.
fn is_wire_text(name: &str, value: &str) -> bool {
    let mut lines = value.split("\r\n");
    let first = lines.next().unwrap_or_default();
    let line_ok = |line: &str| {
        line.bytes()
            .all(|b| b == b' ' || b == b'\t' || (0x21..=0x7e).contains(&b))
    };
    if !line_ok(first) || name.len() + 2 + first.len() > 998 {
        return false;
    }
    lines.all(|line| {
        line_ok(line)
            && line.starts_with([' ', '\t'])
            && !line.trim().is_empty()
            && line.len() <= 998
    })
}

/// Word-wise RFC 2047 for an undeclared grammar: only words that hold
/// octets outside printable ASCII are encoded, ASCII words stay verbatim.
fn encode_raw_words(value: &str) -> String {
    let mut out = String::new();
    let mut pending = String::new();
    let flush = |out: &mut String, pending: &mut String| {
        if pending.is_empty() {
            return;
        }
        let prefix = pending.trim_end_matches(' ');
        out.push_str(&encode_words(prefix));
        out.push_str(&pending[prefix.len()..]);
        pending.clear();
    };
    for word in value.split_inclusive(' ') {
        let plain = word
            .trim_end_matches(' ')
            .bytes()
            .all(|b| b == b'\t' || (0x21..=0x7e).contains(&b));
        if plain {
            flush(&mut out, &mut pending);
            out.push_str(word);
        } else {
            pending.push_str(word);
        }
    }
    flush(&mut out, &mut pending);
    out
}

fn refuse_controls(value: &str, ascii_only: bool) -> Result<(), AccountError> {
    if value.bytes().any(is_control) || (ascii_only && !value.is_ascii()) {
        return Err(malformed(
            "Structured header value holds control or non-ASCII octets",
        ));
    }
    Ok(())
}

/// Encode a complete header without its terminating CRLF.
///
/// Folds at grammar boundaries with a preferred width of 78 octets. An
/// indivisible structured token that cannot fit a 998-octet physical line
/// fails as Request(Malformed). Enterprise send queues must report an
/// unrepresentable header rather than submit bytes a relay can truncate.
/// The SMTP message builder writes every header through this function too.
pub fn write_header(name: &str, value: HeaderValue<'_>) -> Result<String, AccountError> {
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_graphic() && b != b':') {
        return Err(malformed("Invalid header field name"));
    }
    let structured = !matches!(value, HeaderValue::Unstructured(_) | HeaderValue::Raw(_));
    let value = match value {
        HeaderValue::Unstructured(text) => encode_header_value(text),
        HeaderValue::PhraseList(text) => phrase_list(text)?,
        HeaderValue::ListId(text) => {
            let text = sanitize_header(text);
            let (phrase, id) = text
                .rsplit_once('<')
                .ok_or_else(|| malformed("List-ID requires an angle identifier"))?;
            let id = id
                .strip_suffix('>')
                .ok_or_else(|| malformed("Unfinished List-ID"))?;
            if id
                .split('.')
                .any(|atom| atom.is_empty() || !atom.bytes().all(is_atext))
            {
                return Err(malformed("Invalid List-ID identifier"));
            }
            let phrase = phrase.trim();
            if phrase.is_empty() {
                format!("<{id}>")
            } else {
                format!("{} <{id}>", phrase_list_item(phrase)?)
            }
        }
        HeaderValue::Addresses(addresses) => {
            for address in addresses {
                refuse_controls(&address.address, false)?;
            }
            addresses
                .iter()
                .map(format_address)
                .collect::<Vec<_>>()
                .join(", ")
        }
        HeaderValue::MessageIds(ids) => {
            // RFC 6532 permits UTF-8 identifiers; controls are never legal.
            let ids = sanitize_header(ids);
            refuse_controls(&ids, false)?;
            ids
        }
        HeaderValue::Structured(text) => {
            let text = sanitize_header(text);
            refuse_controls(&text, true)?;
            text
        }
        HeaderValue::Raw(text) => {
            if is_wire_text(name, text) {
                return Ok(format!("{name}: {text}"));
            }
            encode_raw_words(text)
        }
        HeaderValue::PreEncoded(text) => {
            if is_wire_text(name, text) {
                return Ok(format!("{name}: {text}"));
            }
            return Err(malformed("Pre-encoded header value is not legal wire text"));
        }
        HeaderValue::Parameterized { value, parameters } => {
            let mut text = sanitize_header(value);
            refuse_controls(&text, true)?;
            for &(key, value) in parameters {
                if key.is_empty()
                    || !key
                        .bytes()
                        .all(|b| b.is_ascii_graphic() && !b"()<>@,;:\\\"/[]?=*".contains(&b))
                {
                    return Err(malformed("Invalid MIME parameter name"));
                }
                let value = sanitize_header(value);
                let quoted = value.replace('\\', "\\\\").replace('"', "\\\"");
                // A control octet is only representable percent-encoded.
                let plain = value.is_ascii() && !value.bytes().any(is_control);
                if value.is_empty() || (plain && key.len() + quoted.len() < 60) {
                    text.push_str(&format!("; {key}=\"{quoted}\""));
                } else {
                    // Percent triplets are indivisible; continuations may split
                    // UTF-8 octets because decoding follows concatenation.
                    let encoded: Vec<String> = value.bytes().map(|b| format!("%{b:02X}")).collect();
                    for (index, chunk) in encoded.chunks(15).enumerate() {
                        let charset = if index == 0 { "utf-8''" } else { "" };
                        text.push_str(&format!("; {key}*{index}*={charset}{}", chunk.concat()));
                    }
                }
            }
            text
        }
    };
    fold(name, &value.replace("\r\n ", " "), structured)
}

// Decode quoted-pairs before encoding a phrase; syntax must never become
// part of an encoded display name. Comments retain their own grammar.
fn phrase_list_item(text: &str) -> Result<String, AccountError> {
    let mut plain = String::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut depth = 0usize;
    let mut comment = String::new();
    let mut pieces = Vec::new();
    for ch in text.chars() {
        if depth > 0 {
            comment.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth == 0 {
                    refuse_controls(&comment, true)?;
                    pieces.push(std::mem::take(&mut comment));
                }
            }
            continue;
        }
        if !quoted && ch == '(' {
            if !plain.trim().is_empty() {
                pieces.push(encode_phrase(plain.trim()));
                plain.clear();
            }
            depth = 1;
            comment.push(ch);
            continue;
        }
        if escaped {
            plain.push(ch);
            escaped = false;
        } else if quoted && ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            quoted = !quoted;
        } else if !quoted
            && ch.is_ascii()
            && !is_atext(ch as u8)
            && ch != ' '
            && ch != '\t'
            && ch != '.'
            && !is_control(ch as u8)
        {
            return Err(malformed("Invalid phrase syntax"));
        } else {
            plain.push(ch);
        }
    }
    if quoted || escaped || depth != 0 || (plain.trim().is_empty() && pieces.is_empty()) {
        return Err(malformed("Empty or unfinished phrase"));
    }
    if !plain.trim().is_empty() {
        pieces.push(encode_phrase(plain.trim()));
    }
    Ok(pieces.join(" "))
}

fn phrase_list(text: &str) -> Result<String, AccountError> {
    let text = sanitize_header(text);
    let mut quoted = false;
    let mut escaped = false;
    let mut start = 0;
    let mut items = Vec::new();
    let mut depth = 0usize;
    for (index, ch) in text.char_indices() {
        if escaped {
            escaped = false;
        } else if (quoted || depth > 0) && ch == '\\' {
            escaped = true;
        } else if !quoted && ch == '(' {
            depth += 1;
        } else if depth > 0 && ch == ')' {
            depth -= 1;
        } else if depth == 0 && ch == '"' {
            quoted = !quoted;
        } else if !quoted && depth == 0 && ch == ',' {
            items.push(phrase_list_item(text[start..index].trim())?);
            start = index + 1;
        }
    }
    items.push(phrase_list_item(text[start..].trim())?);
    Ok(items.join(", "))
}

fn fold(name: &str, value: &str, structured: bool) -> Result<String, AccountError> {
    let mut out = format!("{name}:");
    let mut column = out.len();
    let mut start = 0;
    let mut gap_start = 0;
    let width = if value.contains("=?UTF-8?B?") { 76 } else { 78 };
    let mut quoted = false;
    let mut escaped = false;
    let mut angle = false;
    // Only whitespace outside quoted strings and angle addresses is a
    // structural folding boundary. Never split a quoted local part.
    let mut tokens = Vec::new();
    for (index, ch) in value.char_indices() {
        if structured && escaped {
            escaped = false;
            continue;
        }
        if structured && quoted && ch == '\\' {
            escaped = true;
            continue;
        }
        if structured && ch == '"' {
            quoted = !quoted;
        }
        if structured && !quoted {
            if ch == '<' {
                angle = true;
            }
            if ch == '>' {
                angle = false;
            }
        }
        if !quoted && !angle && ch.is_ascii_whitespace() {
            if start < index {
                tokens.push((&value[gap_start..start], &value[start..index]));
                gap_start = index;
            }
            start = index + ch.len_utf8();
        }
    }
    if start < value.len() {
        tokens.push((&value[gap_start..start], &value[start..]));
    }
    for (gap, token) in tokens {
        let gap = if gap.is_empty() { " " } else { gap };
        if token.len() + 1 > 998 {
            return Err(malformed("Indivisible header token exceeds 998 octets"));
        }
        if column + gap.len() + token.len() > width {
            out.push_str("\r\n");
            out.push_str(gap);
            column = gap.len();
        } else {
            out.push_str(gap);
            column += gap.len();
        }
        out.push_str(token);
        column += token.len();
        if column > 998 {
            return Err(malformed("Header line exceeds 998 octets"));
        }
    }
    if out
        .lines()
        .any(|line| line.trim_end_matches('\r').len() > 998)
    {
        return Err(malformed("Header field name exceeds 998 octets"));
    }
    Ok(out)
}

/// Format an [`Address`] as an RFC 5322 header value: `Name <addr>` with
/// the display name RFC 2047 encoded when non-ASCII or long, or the bare
/// addr-spec when there is no name.
#[must_use]
pub fn format_address(address: &Address) -> String {
    let email = sanitize_header(&address.address);
    match address
        .name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        Some(name) => format!("{} <{email}>", encode_phrase(name)),
        None => email,
    }
}

/// Encode an address display name as an RFC 5322 phrase.
///
/// Matches the bifrost-smtp builder (and `reference/smtp.md`): an
/// short atom-shaped ASCII name is emitted as bare phrase text (no quoting),
/// which avoids the DKIM-breaking quoted-string rewrites some relays
/// perform; an ASCII name carrying specials that an atom may not hold is
/// quoted; a long or non-ASCII name becomes RFC 2047 encoded-words. Producing the
/// bare form when possible keeps the two send paths (IMAP and Google)
/// emitting identical From/To phrases.
pub(crate) fn encode_phrase(value: &str) -> String {
    let sanitized = sanitize_header(value);
    if !sanitized.is_ascii() || sanitized.len() > 70 || sanitized.bytes().any(is_control) {
        return encode_words(&sanitized);
    }
    if is_atom_phrase(&sanitized) {
        sanitized
    } else {
        format!(
            "\"{}\"",
            sanitized.replace('\\', "\\\\").replace('"', "\\\"")
        )
    }
}

/// An RFC 5322 phrase that needs no quoting: at least one `atext`
/// character and nothing outside `atext` plus the inter-atom whitespace
/// (space / tab). Mirrors `bifrost-smtp`'s `is_valid_phrase`.
fn is_atom_phrase(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| is_atext(b) || b == b' ' || b == b'\t')
        && value.bytes().any(is_atext)
}

/// RFC 5322 `atext` (the unquoted-atom character set).
pub(crate) fn is_atext(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'/'
                | b'='
                | b'?'
                | b'^'
                | b'_'
                | b'`'
                | b'{'
                | b'|'
                | b'}'
                | b'~'
        )
}

pub(crate) fn encode_header_value(value: &str) -> String {
    let sanitized = sanitize_header(value);
    if sanitized.is_ascii()
        && !sanitized.bytes().any(is_control)
        && !sanitized
            .split_ascii_whitespace()
            .any(|word| word.len() > 70)
    {
        sanitized
    } else {
        encode_words(&sanitized)
    }
}

/// Maximum length of a single RFC 2047 encoded-word, including the
/// `=?UTF-8?B?` prefix and `?=` suffix. RFC 2047 caps an encoded-word at
/// 75 octets so a folded header line of `space + encoded-word` stays
/// within the 76-octet practical line budget (well under the 998-octet
/// hard limit).
pub(crate) const ENCODED_WORD_MAX: usize = 75;

/// Length of the `=?UTF-8?B?` ... `?=` framing around the base64 payload.
const ENCODED_WORD_OVERHEAD: usize = "=?UTF-8?B?".len() + "?=".len();

/// Encode a non-ASCII value as one or more RFC 2047 base64 encoded-words,
/// each capped at [`ENCODED_WORD_MAX`] octets and split on UTF-8
/// character boundaries (a multibyte scalar is never split across two
/// words, per RFC 2047 §5). Multiple words are joined by CRLF + a single
/// space (folding white space), so the assembled header obeys both the
/// per-word cap and the 998-octet line limit.
fn encode_words(value: &str) -> String {
    // base64 expands 3 input octets to 4 output octets; the payload
    // budget is the per-word cap minus the framing, rounded down to a
    // multiple of 3 so each chunk encodes without padding ambiguity.
    let payload_budget = (ENCODED_WORD_MAX - ENCODED_WORD_OVERHEAD) / 4 * 3;

    let mut words: Vec<String> = Vec::new();
    let mut chunk_start = 0;
    let mut chunk_len = 0;
    let bytes = value.as_bytes();
    let mut idx = 0;
    while idx < value.len() {
        // Advance to the next char boundary so we never split a scalar.
        let char_len = utf8_char_len(bytes[idx]);
        if chunk_len + char_len > payload_budget && chunk_len > 0 {
            words.push(encode_one_word(&value[chunk_start..idx]));
            chunk_start = idx;
            chunk_len = 0;
        }
        chunk_len += char_len;
        idx += char_len;
    }
    if chunk_start < value.len() {
        words.push(encode_one_word(&value[chunk_start..]));
    }
    // Folding white space between encoded-words: CRLF + a single space.
    // Encoded-words carry no internal whitespace, so the linear-white-
    // space between them is semantically elided by the parser per §6.2.
    words.join("\r\n ")
}

fn encode_one_word(segment: &str) -> String {
    format!("=?UTF-8?B?{}?=", STANDARD.encode(segment.as_bytes()))
}

/// Length in bytes of the UTF-8 scalar that starts at this lead byte.
fn utf8_char_len(lead: u8) -> usize {
    match lead {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

pub(crate) fn sanitize_header(value: &str) -> String {
    value.replace(['\r', '\n'], " ").trim().to_string()
}
