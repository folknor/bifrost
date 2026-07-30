use bytes::BytesMut;

use crate::types::response::Capability;

/// Controls how the encoder produces literal markers.
///
/// RFC 3501 Section 4.3 defines synchronizing literals (`{N}\r\n`) which
/// require the client to wait for a `+` continuation response. RFC 7888
/// introduces two extensions that allow non-synchronizing literals (`{N+}\r\n`):
///
/// - **LITERAL+** (RFC 7888 Section 4): non-synchronizing literals of any size.
/// - **LITERAL-** (RFC 7888 Section 5): non-synchronizing literals up to 4096
///   bytes; larger literals MUST use synchronizing form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralMode {
    /// No literal extension  -  all literals use synchronizing form `{N}\r\n`
    /// (RFC 3501 Section 4.3).
    Synchronizing,
    /// LITERAL+ (RFC 7888 Section 4)  -  non-synchronizing `{N+}\r\n` for all sizes.
    LiteralPlus,
    /// LITERAL- (RFC 7888 Section 5)  -  non-synchronizing `{N+}\r\n` only when
    /// the literal is <= 4096 bytes; larger literals use synchronizing `{N}\r\n`.
    LiteralMinus,
}

/// Encoding context derived from the current connection state.
///
/// Carries the capability set, enabled extensions, and negotiated literal
/// mode so the encoder can:
/// - reject commands whose prerequisite capability is not advertised (I6),
/// - select the correct wire encoding for mailbox names and literals.
///
/// Constructed by `ImapConnection::encode_options()`.
#[derive(Debug, Clone)]
pub(crate) struct EncodeOptions {
    /// RFC 6855 / RFC 9051 `UTF8=ACCEPT` mode.
    ///
    /// When `true`, non-ASCII UTF-8 bytes are allowed in quoted strings
    /// (RFC 6855 Section 3 / RFC 9051 Section 9) and mailbox names are
    /// sent as raw UTF-8 instead of modified UTF-7.
    pub(crate) utf8_mode: bool,
    /// RFC 7888 LITERAL+ vs synchronizing literal (RFC 3501 Section 4.3).
    pub(crate) literal_mode: LiteralMode,
    /// Server-advertised capabilities (RFC 3501 Section 7.2.1).
    pub(crate) capabilities: Vec<Capability>,
    /// Extensions successfully enabled with ENABLE.
    pub(crate) enabled: Vec<String>,
}

impl EncodeOptions {
    /// Check whether the server advertises a specific capability.
    pub(super) fn has_capability(&self, cap: &Capability) -> bool {
        self.capabilities.contains(cap) || self.rev2_implies(cap)
    }

    /// Check whether CONDSTORE is available (explicitly or via QRESYNC).
    ///
    /// RFC 7162 Section 3.2.3: a server that advertises QRESYNC implicitly
    /// supports CONDSTORE. The encoder checks this whenever a CONDSTORE
    /// modifier (CHANGEDSINCE, UNCHANGEDSINCE, VANISHED) is present.
    pub(super) fn has_condstore(&self) -> bool {
        self.has_capability(&Capability::Condstore) || self.has_capability(&Capability::QResync)
    }

    fn imap4rev2_active(&self) -> bool {
        let has_rev2 = self.capabilities.contains(&Capability::Imap4Rev2);
        let has_rev1 = self.capabilities.contains(&Capability::Imap4Rev1);
        if has_rev2 && has_rev1 {
            self.enabled
                .iter()
                .any(|extension| extension.eq_ignore_ascii_case("IMAP4rev2"))
        } else {
            has_rev2
        }
    }

    fn rev2_implies(&self, capability: &Capability) -> bool {
        self.imap4rev2_active()
            && matches!(
                capability,
                Capability::Binary
                    | Capability::Enable
                    | Capability::Esearch
                    | Capability::Idle
                    | Capability::ListExtended
                    | Capability::ListStatus
                    | Capability::LiteralMinus
                    | Capability::LiteralPlus
                    | Capability::Move
                    | Capability::Namespace
                    | Capability::ObjectId
                    | Capability::SaslIr
                    | Capability::SaveDate
                    | Capability::SearchRes
                    | Capability::SpecialUse
                    | Capability::StatusDeleted
                    | Capability::StatusSize
                    | Capability::UidPlus
                    | Capability::Unselect
            )
    }
}

/// Error produced when the encoder cannot produce a valid wire command.
///
/// Separate from [`crate::Error`] so codec-level callers can distinguish
/// encoding failures from I/O and protocol errors. The connection layer
/// converts this to [`crate::Error`] at the call site.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum EncodeError {
    /// The command requires a capability that the server has not advertised
    /// or the client has not `ENABLE`d (RFC 3501 Section 6.1.1).
    #[error("command {cmd} requires capability {cap} which is not available")]
    MissingCapability {
        /// The command that requires the capability.
        cmd: &'static str,
        /// The wire name of the missing capability.
        cap: String,
    },
    /// A protocol-level validation failure detected during encoding
    /// (e.g., CRLF in a parameter, invalid atom, etc.).
    #[error("encode validation error: {0}")]
    Validation(String),
}

impl From<crate::Error> for EncodeError {
    /// Convert legacy `crate::Error::Protocol` validation errors into
    /// `EncodeError::Validation`. This bridge exists so existing per-command
    /// encoders that return `Result<(), crate::Error>` can be composed with
    /// the new `EncodeError` return path without rewriting every encoder at
    /// once.
    fn from(e: crate::Error) -> Self {
        Self::Validation(e.to_string())
    }
}

/// RFC 7888 Section 5: maximum non-synchronizing literal size for LITERAL-.
pub(crate) const LITERAL_MINUS_MAX: usize = 4096;

/// Encoded IMAP command split into segments at synchronizing literal boundaries.
///
/// RFC 3501 Section 4.3: when a command contains a synchronizing literal
/// (`{N}\r\n`), the client must send the bytes up to and including the literal
/// marker, then wait for a `+` continuation response before sending the literal
/// body. This struct represents that split so callers can implement the
/// send-wait-send cycle without rescanning the wire bytes.
///
/// - When `literal_mode` is [`LiteralMode::LiteralPlus`] (RFC 7888 Section 4) or
///   the command contains no literals, `segments` has exactly one element.
/// - When `literal_mode` is [`LiteralMode::Synchronizing`] and the command
///   contains synchronizing literals, `segments` has N+1 elements (one for each
///   literal boundary plus the trailing data). Between consecutive segments the
///   caller must wait for a server `+` continuation response.
/// - When `literal_mode` is [`LiteralMode::LiteralMinus`] (RFC 7888 Section 5),
///   literals <= 4096 bytes are non-synchronizing; only literals > 4096 bytes
///   produce segment splits.
#[derive(Debug, Clone)]
pub(crate) struct EncodedCommand {
    /// Sequential wire segments. The caller sends `segments[0]`, waits for `+`,
    /// sends `segments[1]`, waits for `+`, ..., sends `segments[N]` (no wait
    /// after the last segment). Each segment is never empty.
    segments: Vec<BytesMut>,
}

impl EncodedCommand {
    /// Return the wire segments. Between consecutive segments, the connection
    /// must wait for a `+` continuation response (RFC 3501 Section 4.3).
    pub(crate) fn segments(&self) -> &[BytesMut] {
        &self.segments
    }

    /// Concatenate all segments into a single buffer.
    ///
    /// Useful for non-synchronizing paths (LITERAL+) where the entire command
    /// can be sent in one shot, or for tests that verify total wire output.
    pub(crate) fn into_buf(mut self) -> BytesMut {
        if self.segments.len() == 1 {
            // Single-segment fast path  -  avoid reallocation.
            return self.segments.swap_remove(0);
        }
        let total: usize = self.segments.iter().map(BytesMut::len).sum();
        let mut buf = BytesMut::with_capacity(total);
        for seg in &self.segments {
            buf.extend_from_slice(seg);
        }
        buf
    }

    /// Build an `EncodedCommand` from a flat buffer by scanning literal
    /// markers and splitting at synchronizing boundaries.
    ///
    /// A synchronizing literal marker is `{digits}\r\n` where digits parse as
    /// a valid RFC 9051 `number64` (counts above `i64::MAX` are text, not
    /// framing), fit in this process's `usize`, and there
    /// is no `+` before `}`. The buffer is split so
    /// that the marker ends the current segment and the literal body begins
    /// the next segment. The caller sends each segment and waits for a `+`
    /// continuation response between consecutive segments.
    ///
    /// RFC 3501 Section 4.3: "The client MUST wait for a continuation request
    /// before sending the octets of a synchronizing literal."
    pub(super) fn from_flat_buffer(buf: &[u8]) -> Self {
        // Every caller has first encoded a complete tagged command. Keeping
        // this as a release assertion makes the non-empty segment contract
        // structural instead of merely documenting a property of current
        // command encoders.
        assert!(
            !buf.is_empty(),
            "EncodedCommand requires a non-empty command buffer"
        );
        let mut segments = Vec::new();
        let mut seg_start = 0;
        // `scan_pos` tracks our scanning position; it may jump ahead past
        // literal bodies to avoid matching `{N}\r\n` patterns inside literal
        // data.
        let mut scan_pos = 0;

        while scan_pos < buf.len() {
            let Some((marker_end_rel, literal_size, synchronizing)) =
                find_literal_marker(&buf[scan_pos..])
            else {
                break;
            };
            // Absolute offset of the byte just past `{N}\r\n` or
            // `{N+}\r\n`.
            let abs_marker_end = scan_pos + marker_end_rel;
            // A malformed marker cannot delimit a literal body. In particular,
            // do not allow an attacker-controlled size to wrap scan_pos or make
            // us rescan bytes that a valid preceding literal owns.
            let Some(payload_end) = u64::try_from(abs_marker_end)
                .ok()
                .and_then(|marker_end| marker_end.checked_add(literal_size))
                .and_then(|payload_end| usize::try_from(payload_end).ok())
                .filter(|&end| end <= buf.len())
            else {
                break;
            };
            if synchronizing {
                // Current segment: from seg_start through the marker (inclusive
                // of `{N}\r\n`).
                segments.push(BytesMut::from(&buf[seg_start..abs_marker_end]));
                // Next segment begins at the literal body.
                seg_start = abs_marker_end;
            }
            // Skip every literal body, including LITERAL+ / small LITERAL-
            // bodies. Their contents are opaque and may look like framing.
            scan_pos = payload_end;
        }

        // Remaining bytes (literal body + any trailing command text) form the
        // final segment.
        if seg_start < buf.len() {
            segments.push(BytesMut::from(&buf[seg_start..]));
        }

        // If no synchronizing literals were found, the whole buffer is one
        // segment (already pushed above when seg_start == 0).
        Self { segments }
    }
}

/// Find the first literal marker in `buf`.
///
/// Returns `(marker_end, literal_size, synchronizing)` where `marker_end` is the offset
/// past the `\r\n` of the marker, and `literal_size` is the parsed digit
/// count (the number of octets in the literal body).
///
/// Matches synchronizing literals (`{N}\r\n`), non-synchronizing literals
/// (`{N+}\r\n`), and literal8
/// markers (`~{N}\r\n`). RFC 9051 Section 9 defines
/// `literal8 = "~{" number64 "}" CRLF *OCTET` with no `["+"]` modifier,
/// so literal8 is unconditionally synchronizing (RFC 3516 Section 4).
fn find_literal_marker(buf: &[u8]) -> Option<(usize, u64, bool)> {
    use crate::connection::literals::{LiteralMarker, literal_marker_at};

    let mut i = 0;
    while i < buf.len() {
        match literal_marker_at(buf, i) {
            LiteralMarker::Counted {
                data_start,
                size,
                synchronizing,
            } => {
                // A legal number64 is a marker even on a narrow target;
                // `from_flat_buffer` then rejects it as an unavailable body
                // boundary uniformly on every pointer width.
                return Some((data_start, size, synchronizing));
            }
            // Above the RFC 9051 ceiling this cannot be a literal we emitted,
            // so on the send path it is ordinary text: keep scanning for the
            // real boundary instead of stopping at a private parse failure.
            LiteralMarker::CountOutOfRange { .. } | LiteralMarker::NotAMarker => i += 1,
        }
    }
    None
}
