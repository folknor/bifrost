//! THREAD and SORT command encoder (RFC 5256).

use super::{
    BytesMut, LiteralMode, validate_atom, validate_non_empty_search_criteria,
    validate_search_criteria_crlf, validate_sort_thread_charset,
};

/// Encode a THREAD or SORT command (RFC 5256 Sections 2-3).
///
/// Shared by THREAD, UID THREAD, SORT, and UID SORT.
/// THREAD format: `<cmd> <algorithm> <charset> <criteria>`.
/// SORT format:   `<cmd> (<algorithm>) <charset> <criteria>`.
/// `parenthesize_algo` controls whether the algorithm is wrapped in `()`.
#[allow(clippy::too_many_arguments)]
pub(in crate::codec::encode) fn encode_thread_or_sort_cmd(
    buf: &mut BytesMut,
    tag: &str,
    cmd: &str,
    algorithm: &str,
    charset: &str,
    criteria: &str,
    parenthesize_algo: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    if parenthesize_algo {
        // RFC 5256 Section 2: sort-criteria = "(" sort-key *(SP sort-key) ")"
        // Each sort-key is an atom (RFC 3501 Section 9).
        for key in algorithm.split(' ') {
            validate_atom(key, "SORT sort-key")?;
        }
    } else {
        // RFC 5256 Section 3: thread-alg = atom (RFC 3501 Section 9).
        validate_atom(algorithm, "THREAD algorithm")?;
    }
    // RFC 5256 Section 5: charset = atom / quoted.
    validate_sort_thread_charset(charset)?;
    validate_search_criteria_crlf(criteria, &format!("{cmd} criteria"), literal_mode)?;
    validate_non_empty_search_criteria(criteria, cmd)?;

    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(cmd.as_bytes());
    if parenthesize_algo {
        // RFC 5256 Section 2: sort-criteria = "(" sort-key *(SP sort-key) ")"
        buf.extend_from_slice(b" (");
        buf.extend_from_slice(algorithm.as_bytes());
        buf.extend_from_slice(b") ");
    } else {
        // RFC 5256 Section 3: thread-alg is unparenthesized
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(algorithm.as_bytes());
        buf.extend_from_slice(b" ");
    }
    buf.extend_from_slice(charset.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(criteria.as_bytes());
    buf.extend_from_slice(b"\r\n");
    Ok(())
}
