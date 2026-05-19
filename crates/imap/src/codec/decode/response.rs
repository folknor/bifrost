//! Top-level response parsing (RFC 3501 Section 7 / RFC 9051 Section 7).

use super::envelope_fetch::fetch_response_inner;
use super::extensions::{
    parse_untagged_acl, parse_untagged_listrights, parse_untagged_metadata,
    parse_untagged_myrights, parse_untagged_quota, parse_untagged_quotaroot, parse_untagged_thread,
    parse_untagged_unknown,
};
use super::flags_caps::{capability_list, flag_list, resp_text, sequence_set, uid_set};
#[allow(clippy::wildcard_imports)]
use super::primitives::*;
#[allow(clippy::wildcard_imports)]
use super::*;

/// Parse a continuation request: `+ text\r\n` (RFC 3501 Section 7.5 / RFC 9051 Section 7.6).
///
/// RFC 3501 defines `continue-req = "+" SP (resp-text / base64) CRLF`
/// where `resp-text = ["[" resp-text-code "]" SP] text`.
///
/// If the data after `+ ` starts with `[`, we attempt to parse as `[response-code] text`.
/// Base64 SASL challenges never start with `[`, so this is unambiguous.
/// If the bracket parse fails, we fall back to treating everything as plain data.
///
/// RFC 9051 makes resp-text optional (`resp-text = ["[" resp-text-code "]" SP] [text]`),
/// so bare `+\r\n` with no space or text is valid. Many servers also send this form
/// for literal synchronization.
pub(super) fn parse_continuation(input: &[u8]) -> IResult<&[u8], ContinuationRequest> {
    let (input, _) = tag(&b"+"[..]).parse(input)?;
    // RFC 3501/9051 Section 9: continue-req = "+" SP (resp-text / base64) CRLF
    // We intentionally accept bare "+" without SP for compatibility with servers
    // that send "+\r\n" for literal synchronization (common in practice).
    let (input, _) = opt(char(' ')).parse(input)?;

    // RFC 3501 Section 7.5: try to parse resp-text with optional [response-code].
    // If data starts with '[', attempt structured resp-text parse; otherwise plain data.
    let (input, code, data) = if input.first() == Some(&b'[') {
        // Try parsing as resp-text: "[response-code] text"
        if let Ok((rest, (code, text))) = resp_text(input) {
            (rest, code, text)
        } else {
            // Bracket parse failed  -  treat entire remainder as plain data.
            let (rest, data_bytes) = take_while(|b: u8| b != b'\r' && b != b'\n').parse(input)?;
            (rest, None, String::from_utf8_lossy(data_bytes).into_owned())
        }
    } else {
        let (rest, data_bytes) = take_while(|b: u8| b != b'\r' && b != b'\n').parse(input)?;
        (rest, None, String::from_utf8_lossy(data_bytes).into_owned())
    };

    let (input, _) = crlf(input)?;
    Ok((input, ContinuationRequest { code, data }))
}

/// Parse a tagged response: `tag SP status [SP resp-text] CRLF`
/// (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
///
/// The formal grammar requires SP after the status keyword, but many real servers
/// send bare `"tag OK\r\n"` with no trailing text. We tolerate the missing SP
/// per Postel's law (RFC 1122 Section 1.2.2).
pub(super) fn parse_tagged(input: &[u8]) -> IResult<&[u8], TaggedResponse> {
    let (input, tag_bytes) = tag_str(input)?;
    let (input, _) = sp(input)?;
    let (input, status) = alt((
        value(StatusKind::Ok, tag_no_case(&b"OK"[..])),
        value(StatusKind::No, tag_no_case(&b"NO"[..])),
        value(StatusKind::Bad, tag_no_case(&b"BAD"[..])),
    ))
    .parse(input)?;
    // SP is formally required (RFC 3501 Section 7.1) but tolerated as absent
    // for servers that send bare status with no resp-text.
    let (input, maybe_sp) = opt(sp).parse(input)?;
    if maybe_sp.is_some() {
        let (input, (code, text)) = resp_text(input)?;
        let (input, _) = crlf(input)?;
        Ok((
            input,
            TaggedResponse {
                tag: String::from_utf8_lossy(tag_bytes).into_owned(),
                status,
                code,
                text,
            },
        ))
    } else {
        let (input, _) = crlf(input)?;
        Ok((
            input,
            TaggedResponse {
                tag: String::from_utf8_lossy(tag_bytes).into_owned(),
                status,
                code: None,
                text: String::new(),
            },
        ))
    }
}

/// Parse an untagged response: `* SP ...` (RFC 3501 Section 2.2.2 / RFC 9051 Section 2.2.2).
///
/// When `utf8_mode` is true, ENVELOPE fields use raw UTF-8 per RFC 6532 Section 3 / RFC 6855 Section 3.
pub(super) fn parse_untagged(input: &[u8], utf8_mode: bool) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag(&b"* "[..]).parse(input)?;
    alt((
        alt((
            parse_untagged_status,
            |i| parse_untagged_numbered(i, utf8_mode),
            parse_untagged_capability,
            parse_untagged_flags,
            |i| parse_untagged_list(i, utf8_mode),
            |i| parse_untagged_lsub(i, utf8_mode),
            parse_untagged_esearch,
            parse_untagged_search,
            parse_untagged_sort,
            |i| parse_untagged_status_mailbox(i, utf8_mode),
        )),
        alt((
            parse_untagged_enabled,
            parse_untagged_vanished,
            parse_untagged_id,
            |i| parse_untagged_namespace(i, utf8_mode),
            |i| parse_untagged_quotaroot(i, utf8_mode),
            parse_untagged_quota,
            |i| parse_untagged_acl(i, utf8_mode),
            |i| parse_untagged_myrights(i, utf8_mode),
            |i| parse_untagged_listrights(i, utf8_mode),
            |i| parse_untagged_metadata(i, utf8_mode),
        )),
        alt((parse_untagged_thread, parse_untagged_unknown)),
    ))
    .parse(input)
}

/// Parse untagged status: `OK/NO/BAD/BYE [code] text` (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
///
/// SP is formally required (RFC 3501 Section 7.1) but tolerated as absent
/// for servers that send bare status with no resp-text (Postel's law),
/// matching the tolerance already present in `parse_tagged`.
pub(super) fn parse_untagged_status(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, status) = alt((
        value(UntaggedStatus::Ok, tag_no_case(&b"OK"[..])),
        value(UntaggedStatus::No, tag_no_case(&b"NO"[..])),
        value(UntaggedStatus::Bad, tag_no_case(&b"BAD"[..])),
        value(UntaggedStatus::Bye, tag_no_case(&b"BYE"[..])),
    ))
    .parse(input)?;
    let (input, maybe_sp) = opt(sp).parse(input)?;
    if maybe_sp.is_some() {
        let (input, (code, text)) = resp_text(input)?;
        let (input, _) = crlf(input)?;
        Ok((input, UntaggedResponse::Status { status, code, text }))
    } else {
        let (input, _) = crlf(input)?;
        Ok((
            input,
            UntaggedResponse::Status {
                status,
                code: None,
                text: String::new(),
            },
        ))
    }
}

/// Parse `* <n> EXISTS/RECENT/EXPUNGE/FETCH` (RFC 3501 Section 7.3 / RFC 9051 Section 7.3).
///
/// EXISTS and RECENT use `number` (zero is valid for empty mailboxes).
/// EXPUNGE and FETCH use `nz-number` per `message-data` (RFC 3501 Section 9).
///
/// When `utf8_mode` is true, FETCH ENVELOPE fields use raw UTF-8 per RFC 6532 Section 3 / RFC 6855 Section 3.
pub(super) fn parse_untagged_numbered(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    let num_start = input; // Save position for leading-zero check
    let (input, n) = number(input)?;
    let (input, _) = sp(input)?;

    // Peek at the keyword to decide which response type we have.
    if input.len() >= 6 && input[..6].eq_ignore_ascii_case(b"EXISTS") {
        // mailbox-data: number SP "EXISTS"  -  zero is valid (RFC 3501 Section 7.3.1)
        let (input, _) = tag_no_case(&b"EXISTS"[..]).parse(input)?;
        // Tolerate trailing whitespace before CRLF (Postel's law).
        let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
        let (input, _) = crlf(input)?;
        return Ok((input, UntaggedResponse::Exists(n)));
    }
    if input.len() >= 6 && input[..6].eq_ignore_ascii_case(b"RECENT") {
        // mailbox-data: number SP "RECENT"  -  zero is valid (RFC 3501 Section 7.3.2)
        let (input, _) = tag_no_case(&b"RECENT"[..]).parse(input)?;
        // Tolerate trailing whitespace before CRLF (Postel's law).
        let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
        let (input, _) = crlf(input)?;
        return Ok((input, UntaggedResponse::Recent(n)));
    }

    // message-data uses nz-number (RFC 3501 Section 9).
    if n == 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    // nz-number = digit-nz *DIGIT  -  the first digit must be 1-9 (no leading zeros).
    // Use Failure (not Error) so that `alt` does not fall through to the unknown-response
    // catch-all  -  a leading-zero sequence number like `01` is a hard protocol violation.
    if num_start.first() == Some(&b'0') {
        return Err(nom::Err::Failure(nom::error::Error::new(
            num_start,
            nom::error::ErrorKind::Verify,
        )));
    }

    if input.len() >= 7 && input[..7].eq_ignore_ascii_case(b"EXPUNGE") {
        // message-data: nz-number SP "EXPUNGE" (RFC 3501 Section 7.4.1)
        let (input, _) = tag_no_case(&b"EXPUNGE"[..]).parse(input)?;
        // Tolerate trailing whitespace before CRLF (Postel's law).
        let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
        let (input, _) = crlf(input)?;
        return Ok((input, UntaggedResponse::Expunge(n)));
    }

    // message-data: nz-number SP msg-att (RFC 3501 Section 7.4.2)
    let (input, _) = tag_no_case(&b"FETCH"[..]).parse(input)?;
    let (input, _) = sp(input)?;
    let (input, mut fr) = fetch_response_inner(input, utf8_mode)?;
    let (input, _) = crlf(input)?;
    fr.seq = n;
    Ok((input, UntaggedResponse::Fetch(Box::new(fr))))
}

/// Parse `* CAPABILITY ...` (RFC 3501 Section 7.2.1 / RFC 9051 Section 7.2.1).
pub(super) fn parse_untagged_capability(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"CAPABILITY"[..]).parse(input)?;
    let (input, _) = sp(input)?;
    // RFC 3501 Section 7.2.1: capability-data = "CAPABILITY" *(SP capability)
    let (input, caps) = capability_list(input)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Capability(caps)))
}

/// Parse `* FLAGS (...)` (RFC 3501 Section 7.2.6 / RFC 9051 Section 7.2.6).
pub(super) fn parse_untagged_flags(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"FLAGS"[..]).parse(input)?;
    let (input, _) = sp(input)?;
    // RFC 3501 Section 7.2.6: "FLAGS" SP flag-list
    let (input, flags) = flag_list(input)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Flags(flags)))
}

/// Parse LIST-EXTENDED data block: `"(" extended-item *(SP extended-item) ")"`.
///
/// Returns `(old_name, child_info)` extracted from known extended items.
/// Unknown items are silently skipped (forward compatibility).
///
/// Known items:
/// - `"OLDNAME" "(" mailbox-name ")"` (RFC 9051 Section 6.3.9.7)
/// - `"CHILDINFO" "(" astring *(SP astring) ")"` (RFC 5258 Section 4)
fn parse_list_extended_data(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], (Option<MailboxName>, Vec<String>)> {
    let (mut input, _) = char('(').parse(input)?;
    let mut old_name = None;
    let mut child_info = Vec::new();

    loop {
        // Skip optional whitespace between items.
        let (rest, _) = take_while(|b: u8| b == b' ').parse(input)?;
        input = rest;

        // End of extended data block.
        if input.first() == Some(&b')') {
            input = &input[1..];
            break;
        }

        // Parse extended item tag (an atom like "OLDNAME" or "CHILDINFO").
        let (rest, tag_bytes) = astring_utf8(input)?;
        let (rest, _) = sp(rest)?;
        let tag_upper = tag_bytes.to_ascii_uppercase();

        match tag_upper.as_str() {
            "OLDNAME" => {
                // RFC 9051 Section 6.3.9.7: OLDNAME "(" mailbox-name ")"
                let (rest2, _) = char('(').parse(rest)?;
                let (rest2, name_bytes) = astring(rest2)?;
                let (rest2, _) = char(')').parse(rest2)?;
                // Decode wire-form mailbox name (MUTF-7 or UTF-8) at parse time
                // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
                old_name = Some(decode_mailbox_from_wire(&name_bytes, utf8_mode));
                input = rest2;
            }
            "CHILDINFO" => {
                // RFC 5258 Section 6: childinfo-extended-item =
                //   "CHILDINFO" SP "(" list-select-base-opt-quoted
                //                      *(SP list-select-base-opt-quoted) ")"
                // Formally requires at least one item (1*), but accept empty `()`
                // per Postel's law (RFC 1122 Section 1.2.2)  -  a CHILDINFO with
                // no selection options is semantically equivalent to absent CHILDINFO.
                // Postel's law (RFC 1122 Section 1.2.2): accept one or more
                // spaces between items  -  some non-conformant servers insert
                // extra whitespace.
                let (rest2, _) = char('(').parse(rest)?;
                let (rest2, items) =
                    separated_list0(take_while1(|b: u8| b == b' '), astring_utf8).parse(rest2)?;
                let (rest2, _) = char(')').parse(rest2)?;
                child_info = items;
                input = rest2;
            }
            _ => {
                // Unknown extended item  -  skip its value.
                // Per RFC 5258 Section 6 and RFC 9051 Section 9,
                // tagged-ext-val = tagged-ext-simple / "(" [tagged-ext-comp] ")"
                // tagged-ext-simple = sequence-set / number64
                // So the value may be parenthesized OR a simple token.
                if rest.first() == Some(&b'(') {
                    let (rest2, ()) = skip_parenthesized_block(rest)?;
                    input = rest2;
                } else {
                    // Simple value (number64 or sequence-set): consume until SP or ')'.
                    let (rest2, _) = take_while1(|b: u8| b != b' ' && b != b')').parse(rest)?;
                    input = rest2;
                }
            }
        }
    }

    Ok((input, (old_name, child_info)))
}

/// Parse a `* LIST` or `* LSUB` mailbox response (RFC 3501 Section 7.2.2 / 7.2.3).
///
/// LIST and LSUB share identical syntax; only the keyword and the
/// [`UntaggedResponse`] variant differ.
fn parse_mailbox_list_response<'a, F>(
    input: &'a [u8],
    keyword: &'static [u8],
    utf8_mode: bool,
    wrap: F,
) -> IResult<&'a [u8], UntaggedResponse>
where
    F: FnOnce(MailboxInfo) -> UntaggedResponse,
{
    let (input, _) = tag_no_case(keyword).parse(input)?;
    let (input, _) = sp(input)?;
    // Attributes
    let (input, attrs) = delimited(
        char('('),
        // Postel's law (RFC 1122 Section 1.2.2): accept one or more spaces
        // between attributes  -  some non-conformant servers insert extra whitespace.
        separated_list0(
            take_while1(|b: u8| b == b' '),
            map(
                alt((
                    map((char('\\'), atom), |(_, a)| {
                        format!("\\{}", String::from_utf8_lossy(a))
                    }),
                    map(atom, |a| String::from_utf8_lossy(a).into_owned()),
                )),
                |s| parse_mailbox_attribute(&s),
            ),
        ),
        char(')'),
    )
    .parse(input)?;
    let (input, _) = sp(input)?;
    // Delimiter: NIL or single QUOTED-CHAR  -  RFC 3501 Section 9.
    let (input, delimiter) = mailbox_delimiter(input)?;
    let (input, _) = sp(input)?;
    // Mailbox name  -  decode from wire form (MUTF-7 or UTF-8) at parse time
    // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
    let (input, name_bytes) = astring(input)?;
    let name = decode_mailbox_from_wire(&name_bytes, utf8_mode);
    // Optional LIST-EXTENDED data: SP "(" ... ")" (RFC 5258 Section 6 / RFC 9051).
    // Examples: ("OLDNAME" ("OldMailbox")), ("CHILDINFO" ("SUBSCRIBED"))
    let (input, ext_data) =
        opt(preceded(sp, |i| parse_list_extended_data(i, utf8_mode))).parse(input)?;
    let (old_name, child_info) = ext_data.unwrap_or_default();
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((
        input,
        wrap(MailboxInfo {
            name,
            delimiter,
            attributes: attrs,
            old_name,
            child_info,
        }),
    ))
}

/// Parse `* LIST (\attrs) "delimiter" "name" [SP mbox-list-extended]`
/// (RFC 3501 Section 7.2.2, RFC 5258 Section 6, RFC 9051 Section 7.2.2).
///
/// The optional `mbox-list-extended` block carries data like OLDNAME (RFC 9051
/// Section 6.3.9.7) or CHILDINFO (RFC 5258 Section 4). It is parsed and
/// discarded for now, but its presence must not cause a parse failure.
pub(super) fn parse_untagged_list(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    parse_mailbox_list_response(input, b"LIST", utf8_mode, UntaggedResponse::List)
}

/// Parse `* LSUB (\attrs) "delimiter" "name"` (RFC 3501 Section 7.2.3).
///
/// Identical format to LIST. LSUB is obsoleted by LIST-EXTENDED (RFC 5258)
/// but still sent by some servers.
pub(super) fn parse_untagged_lsub(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    parse_mailbox_list_response(input, b"LSUB", utf8_mode, UntaggedResponse::Lsub)
}

/// Parse a mailbox-list delimiter: NIL or exactly one QUOTED-CHAR (RFC 3501 Section 9).
///
/// ```text
/// mailbox-list = "(" [mbx-list-flags] ")" SP
///                (DQUOTE QUOTED-CHAR DQUOTE / nil) SP mailbox
/// ```
///
/// Per Postel's law (RFC 3501 Section 9): if a non-conformant server sends a
/// multi-character quoted string, the first character is taken and a warning
/// is logged instead of returning a hard parse error.
fn mailbox_delimiter(input: &[u8]) -> IResult<&[u8], Option<char>> {
    let (rest, delimiter_bytes) =
        alt((value(None, nil_token), map(quoted_string, Some))).parse(input)?;
    match delimiter_bytes {
        None => Ok((rest, None)),
        Some(v) if v.is_empty() => Ok((rest, None)),
        Some(v) => {
            let s = String::from_utf8_lossy(&v);
            let mut chars = s.chars();
            let ch = chars.next();
            if chars.next().is_some() {
                // RFC 3501 Section 9: delimiter should be exactly one QUOTED-CHAR.
                // Postel's law: tolerate multi-character delimiters from
                // non-conformant servers by taking only the first character.
                tracing::warn!(
                    delimiter = %s,
                    "LIST delimiter has multiple characters (RFC 3501 Section 9 \
                     requires exactly one QUOTED-CHAR); taking only the first character"
                );
            }
            Ok((rest, ch))
        }
    }
}

/// Map a string to a `MailboxAttribute` (RFC 3501 Section 7.2.2, RFC 6154).
///
/// Delegates to [`MailboxAttribute::from_imap_str`] for the string-to-variant mapping.
pub(super) fn parse_mailbox_attribute(s: &str) -> MailboxAttribute {
    MailboxAttribute::from_imap_str(s)
}

/// Parse `* SEARCH` response (RFC 3501 Section 7.2.5, RFC 7162 Section 3.1.5).
///
/// RFC 7162 Section 3.1.5: when a MODSEQ search criterion is used and the
/// result is non-empty, the server appends `(MODSEQ <n>)`.
/// Example: `* SEARCH 2 5 6 7 11 12 18 19 20 23 (MODSEQ 917162500)`
pub(super) fn parse_untagged_search(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, (nums, mod_seq)) = parse_number_list_with_modseq(input, b"SEARCH")?;
    Ok((
        input,
        UntaggedResponse::Search {
            uids: nums,
            mod_seq,
        },
    ))
}

/// Parse `* SORT` response (RFC 5256 Section 4, RFC 7162 Section 3.1.6).
///
/// `sort-data = "SORT" *(SP nz-number) [SP search-sort-mod-seq]`  -  the
/// result is a list of message numbers or UIDs in sorted order. An empty
/// result (no numbers) is valid and means no messages matched.
///
/// RFC 7162 Section 3.1.6: when a MODSEQ search criterion is used, the
/// server appends `(MODSEQ <mod-sequence-value>)` to the SORT response.
pub(super) fn parse_untagged_sort(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, (nums, mod_seq)) = parse_number_list_with_modseq(input, b"SORT")?;
    Ok((input, UntaggedResponse::Sort { nums, mod_seq }))
}

/// Shared parser for SEARCH and SORT responses (RFC 3501 Section 7.2.5,
/// RFC 5256 Section 4, RFC 7162 Section 3.1.5-6).
///
/// Both responses share the same wire format:
/// `keyword *(SP nz-number) [SP "(" "MODSEQ" SP mod-sequence-value ")"] CRLF`
///
/// Some servers (e.g., Zoho) send trailing spaces with no numbers; these
/// are tolerated via `parse_optional_modseq_and_crlf` (Postel's law).
fn parse_number_list_with_modseq<'a>(
    input: &'a [u8],
    keyword: &'static [u8],
) -> IResult<&'a [u8], (Vec<u32>, Option<u64>)> {
    let (input, _) = tag_no_case(keyword).parse(input)?;
    // RFC 3501 Section 7.2.5 / RFC 5256 Section 4 ABNF specifies
    // `nz-number`, but we use `number` (which accepts 0) per Postel's
    // law (RFC 1122 Section 1.2.2): some non-conformant servers include
    // 0 in SEARCH/SORT results, and using `nz_number` with `many0`
    // silently drops 0 and all subsequent results  -  causing data loss.
    // We accept 0 during parsing to avoid truncating results, then filter
    // it out below since UID/sequence number 0 is semantically invalid
    // (RFC 3501 Section 9: `nz-number`).
    let (input, nums) = many0(preceded(sp, number)).parse(input)?;
    // Discard 0 values  -  they are not valid UIDs or sequence numbers.
    let nums: Vec<u32> = nums.into_iter().filter(|&n| n != 0).collect();
    let (input, mod_seq) = parse_optional_modseq_and_crlf(input)?;
    Ok((input, (nums, mod_seq)))
}

/// Parse optional `search-sort-mod-seq` suffix and consume trailing content
/// through CRLF (RFC 7162 Section 3.1.6).
///
/// Shared by SEARCH and SORT response parsers. Handles:
/// - Optional `(MODSEQ <mod-sequence-value>)` per RFC 7162 Section 3.1.6.
///   `mod-sequence-value` must be >= 1 (only `mod-sequence-valzer` allows 0).
///   RFC 5234 Section2.3: ABNF literal strings are case-insensitive.
/// - Trailing content before CRLF (Postel's law / RFC 1122 Section 1.2.2):
///   trailing whitespace from non-conformant servers and malformed MODSEQ
///   suffixes (e.g. `(MODSEQ 0)` or overflowing values) are discarded so
///   already-parsed results are preserved.
fn parse_optional_modseq_and_crlf(input: &[u8]) -> IResult<&[u8], Option<u64>> {
    let (input, mod_seq) = opt(preceded(
        sp,
        delimited(
            (char('('), tag_no_case(&b"MODSEQ"[..]), sp),
            nz_number64,
            char(')'),
        ),
    ))
    .parse(input)?;
    let (input, _) = take_while(|b: u8| b != b'\r' && b != b'\n').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, mod_seq))
}

/// Parse `* ESEARCH (TAG "tag") [UID] result-data` (RFC 4731 Section 3.1).
///
/// RFC 4731 Section 3.1 ABNF:
/// `search-return-data = "MIN" SP nz-number / "MAX" SP nz-number /
///                        "ALL" SP sequence-set / "COUNT" SP number`
///
/// Produces `UntaggedResponse::Esearch(EsearchResponse)` with all fields preserved.
pub(super) fn parse_untagged_esearch(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"ESEARCH"[..]).parse(input)?;

    // Optional search-correlator: (TAG "tagstring")
    // RFC 4466 Section 2.6.2: `search-correlator = SP "(" "TAG" SP tag-string ")"`
    // tag-string = astring (RFC 4466 Section 2.6.2)
    let (input, tag_val) = opt(preceded(
        sp,
        delimited(
            char('('),
            preceded((tag_no_case(&b"TAG"[..]), sp), astring),
            char(')'),
        ),
    ))
    .parse(input)?;

    // Optional UID indicator (RFC 4731 Section 3.1).
    // Like `nil_token`, we must verify a token boundary after "UID" to
    // prevent greedy prefix matches against atoms like "UIDFOO".
    let (input, uid_indicator) = opt(preceded(
        sp,
        terminated(
            tag_no_case(&b"UID"[..]),
            peek(alt((
                value((), verify(take(1u8), |b: &[u8]| !is_atom_char(b[0]))),
                value((), eof),
            ))),
        ),
    ))
    .parse(input)?;

    let mut esearch = EsearchResponse {
        tag: tag_val.map(|v| String::from_utf8_lossy(&v).into_owned()),
        uid: uid_indicator.is_some(),
        ..EsearchResponse::default()
    };

    // Result data: space-separated key-value pairs until CRLF.
    let mut input = input;

    while let Ok((rest, _)) = sp(input) {
        if rest.first() == Some(&b'\r') || rest.first() == Some(&b'\n') {
            // Trailing whitespace before CRLF  -  advance past it and stop.
            // Consistent with every other parser (FLAGS, CAPABILITY, LIST,
            // VANISHED, ENABLED, ID, SEARCH, SORT, QUOTA, ACL) per
            // Postel's law (RFC 1122 Section 1.2.2).
            input = rest;
            break;
        }

        let (rest, key) = atom(rest)?;
        let key_upper = String::from_utf8_lossy(key).to_ascii_uppercase();

        match key_upper.as_str() {
            "ALL" => {
                // RFC 4731 Section 3.1: "ALL SP sequence-set"
                // "If the SEARCH results in no matches, the server MUST NOT
                // include the ALL result option in the ESEARCH response."
                let (rest2, _) = sp(rest)?;
                // RFC 4731 Section 3.1: sequence-set allows `*` as a valid seq-number.
                let (rest2, set) = sequence_set(rest2)?;
                esearch.all = set;
                input = rest2;
            }
            "MIN" => {
                // RFC 4731 Section 3.1: "MIN SP nz-number"
                // "Return the lowest message number/UID that satisfies the
                // SEARCH criteria."
                let (rest2, _) = sp(rest)?;
                let (rest2, val) = nz_number(rest2)?;
                esearch.min = Some(val);
                input = rest2;
            }
            "MAX" => {
                // RFC 4731 Section 3.1: "MAX SP nz-number"
                // "Return the highest message number/UID that satisfies the
                // SEARCH criteria."
                let (rest2, _) = sp(rest)?;
                let (rest2, val) = nz_number(rest2)?;
                esearch.max = Some(val);
                input = rest2;
            }
            "COUNT" => {
                // RFC 4731 Section 3.1: "COUNT SP number"
                // "This result option MUST always be included in the ESEARCH
                // response."
                let (rest2, _) = sp(rest)?;
                let (rest2, val) = number(rest2)?;
                esearch.count = Some(val);
                input = rest2;
            }
            "MODSEQ" => {
                // RFC 7162 Section 3.1.10: "MODSEQ SP mod-sequence-value"
                // Extended SEARCH/ESEARCH responses MUST return the MODSEQ
                // result option when a MODSEQ criterion was used.
                // mod-sequence-value must be >= 1 (nz-number64) per
                // RFC 7162 Section 3.1.3.
                // If the space or value is malformed (e.g., missing space,
                // 0, or overflow), skip gracefully rather than losing the
                // entire ESEARCH response
                // (Postel's law  -  RFC 1122 Section 1.2.2).
                if let Ok((rest2, _)) = sp(rest) {
                    if let Ok((rest2, val)) = nz_number64(rest2) {
                        esearch.mod_seq = Some(val);
                        input = rest2;
                    } else {
                        // Malformed MODSEQ value  -  skip to next SP or CRLF.
                        // This preserves all other ESEARCH results (MIN, MAX,
                        // COUNT, ALL) that were successfully parsed before
                        // this point, rather than discarding the entire
                        // response.
                        let (rest2, _) =
                            take_while(|b: u8| b != b' ' && b != b'\r').parse(rest2)?;
                        input = rest2;
                    }
                } else {
                    // Malformed MODSEQ (no space after keyword)  -  skip to
                    // next SP or CRLF. Preserves already-parsed ESEARCH
                    // results (Postel's law  -  RFC 1122 Section 1.2.2).
                    let (rest2, _) = take_while(|b: u8| b != b' ' && b != b'\r').parse(rest)?;
                    input = rest2;
                }
            }
            _ => {
                // Unknown key  -  skip its value gracefully.
                // RFC 4731 Section 3.1: servers may extend ESEARCH with new
                // result options. Unknown keys may or may not have a value;
                // if there is no SP following the key (i.e. the key is at the
                // end of the line), skip it without consuming a value.
                let Ok((rest2, _)) = sp(rest) else {
                    // Unknown key with no value  -  skip it.
                    input = rest;
                    continue;
                };
                // RFC 9051 Section 9: tagged-ext-val = tagged-ext-simple /
                //   "(" [tagged-ext-comp] ")"
                // tagged-ext-simple includes astring (which covers quoted
                // strings and literals), so we must handle all value forms.
                if rest2.first() == Some(&b'(') {
                    // Skip balanced parenthesized group
                    let (rest2, ()) = skip_parenthesized_block(rest2)?;
                    input = rest2;
                } else {
                    // ESEARCH values are terminated by SP or CRLF.
                    let (rest2, ()) =
                        super::envelope_fetch::skip_tagged_ext_simple(|b| b == b' ' || b == b'\r')(
                            rest2,
                        )?;
                    input = rest2;
                }
            }
        }
    }

    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Esearch(esearch)))
}

/// Parse `* STATUS "mailbox" (...)` (RFC 3501 Section 7.2.4).
pub(super) fn parse_untagged_status_mailbox(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"STATUS"[..]).parse(input)?;
    let (input, _) = sp(input)?;
    // Decode wire-form mailbox name (MUTF-7 or UTF-8) at parse time
    // per RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1.
    let (input, mailbox_bytes) = astring(input)?;
    let mailbox = decode_mailbox_from_wire(&mailbox_bytes, utf8_mode);
    let (input, _) = sp(input)?;
    let (input, items) = delimited(char('('), status_items, char(')')).parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::MailboxStatus { mailbox, items }))
}

/// Parse STATUS items inside parentheses (RFC 3501 Section 6.3.10).
///
/// Uses a loop instead of `separated_list0` so that unknown attributes from
/// future extensions can be gracefully skipped without failing the parse.
#[allow(clippy::too_many_lines)]
pub(super) fn status_items(input: &[u8]) -> IResult<&[u8], Vec<StatusItem>> {
    let mut items = Vec::new();
    let mut input_loop = input;

    loop {
        // Skip optional leading whitespace
        let (rest, _) = take_while(|b: u8| b == b' ').parse(input_loop)?;
        input_loop = rest;

        // Check for end of parenthesized group
        if input_loop.first() == Some(&b')') || input_loop.is_empty() {
            break;
        }

        let (rest, name) = atom(input_loop)?;
        let upper = String::from_utf8_lossy(name).to_ascii_uppercase();

        match upper.as_str() {
            "MESSAGES" => {
                let (rest, _) = sp(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::Messages(val));
                }
                input_loop = rest;
            }
            "RECENT" => {
                let (rest, _) = sp(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::Recent(val));
                }
                input_loop = rest;
            }
            "UNSEEN" => {
                let (rest, _) = sp(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::Unseen(val));
                }
                input_loop = rest;
            }
            // RFC 9051 Section 9 says nz-number for UIDNEXT and UIDVALIDITY,
            // but we accept 0 per Postel's law (RFC 1122 Section 1.2.2) because
            // non-conformant servers (e.g. some Dovecot configurations) send 0
            // for empty mailboxes.
            "UIDNEXT" => {
                let (rest, _) = sp(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::UidNext(val));
                }
                input_loop = rest;
            }
            "UIDVALIDITY" => {
                let (rest, _) = sp(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::UidValidity(val));
                }
                input_loop = rest;
            }
            // RFC 9051 Section 6.3.11: DELETED is a standard rev2 STATUS item.
            "DELETED" => {
                let (rest, _) = sp(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::Deleted(val));
                }
                input_loop = rest;
            }
            "HIGHESTMODSEQ" => {
                let (rest, _) = sp(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number64_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::HighestModSeq(val));
                }
                input_loop = rest;
            }
            "SIZE" => {
                let (rest, _) = sp(rest)?;
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, val) = number64_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::Size(val));
                }
                input_loop = rest;
            }
            "MAILBOXID" => {
                // RFC 8474 Section 5.1: MAILBOXID (objectid-val)
                // objectid = 1*255(ALPHA / DIGIT / "_" / "-") per RFC 8474 Section 7.
                let (rest, _) = sp(rest)?;
                let (rest, _) = char('(').parse(rest)?;
                let (rest, val) =
                    map(objectid, |a| String::from_utf8_lossy(a).into_owned()).parse(rest)?;
                let (rest, _) = char(')').parse(rest)?;
                items.push(StatusItem::MailboxId(val));
                input_loop = rest;
            }
            "APPENDLIMIT" => {
                // RFC 7889: APPENDLIMIT in STATUS can be NIL (no limit) or a number.
                // Overflow is silently skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, _) = sp(rest)?;
                let (rest, val) = alt((
                    map(nil_token, |_| Some(None::<u64>)),
                    map(number64_tolerant, |opt| opt.map(Some)),
                ))
                .parse(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::AppendLimit(val));
                }
                input_loop = rest;
            }
            "DELETED-STORAGE" => {
                // RFC 9208 Section 3: disk space consumed by deleted messages.
                // Tolerant: overflow skipped per Postel's law (RFC 1122 Section 1.2.2).
                let (rest, _) = sp(rest)?;
                let (rest, val) = number64_tolerant(rest)?;
                if let Some(val) = val {
                    items.push(StatusItem::DeletedStorage(val));
                }
                input_loop = rest;
            }
            _ => {
                // RFC 9051 Section9: future extensions may use tagged-ext-val which
                // includes parenthesized forms, quoted strings, and literals.
                // tagged-ext-val = tagged-ext-simple /
                //   "(" [tagged-ext-comp] ")"
                let (rest, _) = sp(rest)?;
                if rest.first() == Some(&b'(') {
                    // Parenthesized value  -  skip balanced group.
                    let (rest, ()) = skip_parenthesized_block(rest)?;
                    input_loop = rest;
                } else {
                    // STATUS values are terminated by SP or `)`.
                    let (rest, ()) =
                        super::envelope_fetch::skip_tagged_ext_simple(|b| b == b' ' || b == b')')(
                            rest,
                        )?;
                    input_loop = rest;
                }
            }
        }
    }

    Ok((input_loop, items))
}

/// Parse `* ENABLED cap1 cap2 ...` (RFC 5161 Section 3.2).
pub(super) fn parse_untagged_enabled(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"ENABLED"[..]).parse(input)?;
    // RFC 5161 Section 3.2: "ENABLED" *(SP capability)
    let (input, caps) = many0(preceded(
        sp,
        map(atom, |a| String::from_utf8_lossy(a).into_owned()),
    ))
    .parse(input)?;
    // Tolerate trailing whitespace before CRLF (same trailing-SP issue
    // as SEARCH/SORT  -  non-conformant per RFC 5161 Section 3.2 formal
    // syntax, but accepted per Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Enabled(caps)))
}

/// Parse `* VANISHED (EARLIER) known-uids` (RFC 7162 Section 3.2.10).
///
/// ABNF: `expunged-resp = "VANISHED" [SP "(EARLIER)"] SP known-uids`
/// where `known-uids = sequence-set` (RFC 3501 Section 9).
pub(super) fn parse_untagged_vanished(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"VANISHED"[..]).parse(input)?;
    let (input, _) = sp(input)?;
    let (input, earlier) = opt(delimited(
        char('('),
        tag_no_case(&b"EARLIER"[..]),
        char(')'),
    ))
    .parse(input)?;
    let (input, _) = if earlier.is_some() {
        sp(input)?
    } else {
        (input, b' ')
    };
    // RFC 7162 Section 6 ABNF: known-uids = sequence-set
    //   ;; Sequence of UIDs; "*" is not allowed.
    // Despite referencing `sequence-set`, the ABNF comment explicitly prohibits
    // `*`. We therefore use `uid_set` which only accepts `nz-number`.
    let (input, uids) = uid_set(input)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    // RFC 7162 Section 3.2.10: "VANISHED" [SP "(EARLIER)"] SP known-uids
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((
        input,
        UntaggedResponse::Vanished {
            earlier: earlier.is_some(),
            uids,
        },
    ))
}

/// Parse `* ID (key val ...)` (RFC 2971 Section 3.2).
pub(super) fn parse_untagged_id(input: &[u8]) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"ID"[..]).parse(input)?;
    let (input, _) = sp(input)?;
    let (input, params) = alt((
        value(vec![], nil_token),
        delimited(
            char('('),
            many0(map(
                (
                    preceded(take_while(|b: u8| b == b' '), string_utf8),
                    preceded(sp, nstring_utf8),
                ),
                |(k, v)| (k, v),
            )),
            char(')'),
        ),
    ))
    .parse(input)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, UntaggedResponse::Id(params)))
}

/// Parse `* NAMESPACE personal other shared` (RFC 2342 Section 5).
///
/// When `utf8_mode` is false, namespace prefixes are decoded from Modified
/// UTF-7 (RFC 3501 Section 5.1.3). When true, they are raw UTF-8
/// (RFC 9051 Section 5.1 / RFC 6855 Section 3).
pub(super) fn parse_untagged_namespace(
    input: &[u8],
    utf8_mode: bool,
) -> IResult<&[u8], UntaggedResponse> {
    let (input, _) = tag_no_case(&b"NAMESPACE"[..]).parse(input)?;
    let (input, _) = sp(input)?;
    let (input, personal) = namespace_list(input, utf8_mode)?;
    let (input, _) = sp(input)?;
    let (input, other) = namespace_list(input, utf8_mode)?;
    let (input, _) = sp(input)?;
    let (input, shared) = namespace_list(input, utf8_mode)?;
    // Tolerate trailing whitespace before CRLF (Postel's law).
    let (input, _) = take_while(|b: u8| b == b' ').parse(input)?;
    let (input, _) = crlf(input)?;
    Ok((
        input,
        UntaggedResponse::Namespace {
            personal,
            other,
            shared,
        },
    ))
}

/// Parse a namespace descriptor list: NIL or `((prefix delimiter) ...)` (RFC 2342 Section 5).
fn namespace_list(input: &[u8], utf8_mode: bool) -> IResult<&[u8], Vec<NamespaceDescriptor>> {
    alt((
        value(vec![], nil_token),
        // RFC 2342 Section 6: Namespace = nil / "(" 1*(...) ")"
        // Strictly requires at least one descriptor in the parenthesized form.
        // However, non-conformant servers may send "()" for an empty namespace
        // category instead of NIL. Accept per Postel's law (RFC 1122 Section 1.2.2)
        // to avoid losing the entire NAMESPACE response.
        delimited(
            char('('),
            many0(|i| namespace_descriptor(i, utf8_mode)),
            char(')'),
        ),
    ))
    .parse(input)
}

/// Parse a single namespace descriptor: `(prefix delimiter [extensions])` (RFC 2342 Section 6).
///
/// RFC 2342 Section 6 ABNF:
/// ```text
/// Namespace = nil / "(" 1*( "(" string SP  (<"> QUOTED_CHAR <"> / nil)
///                    *(Namespace_Response_Extension) ")" ) ")"
/// Namespace_Response_Extension = SP string SP "(" string *(SP string) ")"
/// ```
fn namespace_descriptor(input: &[u8], utf8_mode: bool) -> IResult<&[u8], NamespaceDescriptor> {
    let (input, _) = char('(').parse(input)?;
    let (input, raw_prefix) = string_utf8(input)?;
    // RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1: decode namespace prefix
    // from Modified UTF-7 when not in UTF-8 mode, matching the decoding
    // applied to mailbox names elsewhere in the parser.
    let prefix = if utf8_mode {
        raw_prefix
    } else {
        crate::codec::utf7::decode_utf7(raw_prefix.as_bytes())
    };
    let (input, _) = sp(input)?;
    // RFC 2342 Section 6 / RFC 9051 Section 9: the delimiter is
    // `DQUOTE QUOTED-CHAR DQUOTE / nil`  -  exactly one character.
    // QUOTED-CHAR allows UTF8-2/UTF8-3/UTF8-4, so the delimiter may
    // be a multibyte UTF-8 character. Decode as UTF-8 first, then
    // verify exactly one Unicode scalar value (matching the LIST/LSUB
    // delimiter handling in `mailbox_delimiter`).
    let (input, delim_bytes) =
        alt((value(None, nil_token), map(quoted_string, Some))).parse(input)?;
    let delim = match delim_bytes {
        None => None,
        Some(v) if v.is_empty() => None,
        Some(v) => {
            let s = String::from_utf8_lossy(&v);
            let mut chars = s.chars();
            let ch = chars.next();
            if chars.next().is_some() {
                // RFC 2342 Section 6: delimiter should be exactly one QUOTED-CHAR.
                // Postel's law: tolerate multi-character delimiters from
                // non-conformant servers by taking only the first character.
                tracing::warn!(
                    delimiter = %s,
                    "NAMESPACE delimiter has multiple characters (RFC 2342 Section 6 \
                     requires exactly one QUOTED-CHAR); taking only the first character"
                );
            }
            ch
        }
    };
    // Parse zero or more Namespace_Response_Extension (RFC 2342 Section 6).
    let (input, extensions) = many0(namespace_response_extension).parse(input)?;
    let (input, _) = char(')').parse(input)?;
    Ok((
        input,
        NamespaceDescriptor {
            prefix,
            delimiter: delim,
            extensions,
        },
    ))
}

/// Parse a single `Namespace_Response_Extension` (RFC 2342 Section 6).
///
/// ABNF: `SP string SP "(" string *(SP string) ")"`
fn namespace_response_extension(input: &[u8]) -> IResult<&[u8], (String, Vec<String>)> {
    // SP before the extension key
    let (input, _) = sp(input)?;
    // Extension key (string)
    let (input, key) = string_utf8(input)?;
    let (input, _) = sp(input)?;
    // "(" string *(SP string) ")"
    let (input, _) = char('(').parse(input)?;
    let (input, first_val) = string_utf8(input)?;
    let (input, rest_vals) = many0(preceded(sp, string_utf8)).parse(input)?;
    let (input, _) = char(')').parse(input)?;
    let mut values = Vec::with_capacity(1 + rest_vals.len());
    values.push(first_val);
    values.extend(rest_vals);
    Ok((input, (key, values)))
}
