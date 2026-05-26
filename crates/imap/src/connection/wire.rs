//! Single wire-reader primitive (RFC 3501 Section2.2.2).
//!
//! Private to `crate::connection`. `WireReader::new` and the internal
//! fields are unreachable outside this module. The ONLY construction
//! site outside this file is `ImapConnection::new_wire_reader`, which
//! itself is only called by the connection constructor and the stream
//! upgrade handler.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bifrost_net::MeterSinkHandle;
use bytes::BytesMut;
use tracing::{trace, warn};

use crate::codec::decode::parse_response_utf8;
use crate::error::Error;
use crate::types::Response;

use super::ImapStream;

/// Low-level wire reader that owns an IMAP stream and a parse buffer.
///
/// All wire reads flow through this type. The buffer and stream are
/// private  -  no external code can pull bytes or reset state. The only
/// way to "reset" the reader is to consume it via [`into_stream`](Self::into_stream)
/// and construct a fresh reader on the (possibly upgraded) stream.
pub(crate) struct WireReader {
    stream: ImapStream,
    buf: BytesMut,
    metering: WireMetering,
}

impl WireReader {
    /// Create a new reader over the given stream with an 8 KiB initial
    /// parse buffer (RFC 3501 Section2.2.2  -  responses are line-oriented, so
    /// 8 KiB covers the vast majority of single-response reads without
    /// reallocation).
    #[cfg(test)]
    pub(super) fn new(stream: ImapStream) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(8 * 1024),
            metering: WireMetering::disabled(),
        }
    }

    pub(super) fn new_metered(
        stream: ImapStream,
        sink: Option<MeterSinkHandle>,
        bandwidth_cap: Option<Arc<AtomicU64>>,
    ) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(8 * 1024),
            metering: WireMetering::new(sink, bandwidth_cap),
        }
    }

    pub(super) fn with_metering(stream: ImapStream, metering: WireMetering) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(8 * 1024),
            metering,
        }
    }

    pub(super) fn metering(&self) -> WireMetering {
        self.metering.clone()
    }

    /// Read and parse a single IMAP response from the wire.
    ///
    /// Mirrors the existing `helpers::read_parsed_response` behavior:
    /// accumulate bytes into the internal buffer, run the nom parser,
    /// and return the first complete response.
    ///
    /// * On parse error -> `Err(Error::Parse)`
    /// * On I/O error -> `Err(Error::Io)`
    /// * On EOF (zero-byte read) -> `Err(Error::Closed)`
    ///
    /// `utf8_mode` should be `true` when `UTF8=ACCEPT` (RFC 6855
    /// Section 3) has been enabled or `IMAP4rev2` is active (RFC 9051
    /// Section 7).
    pub(crate) async fn read_one(&mut self, utf8_mode: bool) -> Result<Response, Error> {
        loop {
            // Try to parse a complete response from the buffer.
            if let Some(resp) = self.try_parse_response_inner(utf8_mode)? {
                return Ok(resp);
            }
            // Need more data from the wire.
            let n = self.stream.read_buf(&mut self.buf).await?;
            if n == 0 {
                return Err(Error::closed());
            }
            self.metering.record_in(n).await;
        }
    }

    /// Read and parse a server greeting from the wire (RFC 3501 Section7.1).
    ///
    /// Like [`read_one`](Self::read_one), but uses the greeting parser
    /// instead of the general response parser. The greeting is the very
    /// first message from the server  -  `* OK`, `* PREAUTH`, or `* BYE`.
    pub(crate) async fn read_greeting(&mut self) -> Result<Response, Error> {
        loop {
            if let Some(resp) = self.try_parse_greeting_inner()? {
                return Ok(resp);
            }
            let n = self.stream.read_buf(&mut self.buf).await?;
            if n == 0 {
                return Err(Error::closed());
            }
            self.metering.record_in(n).await;
        }
    }

    /// Write raw bytes to the wire. Used by command encoders.
    /// Does not read. Does not parse. Does not mutate buf.
    pub(crate) async fn write_all(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.metering.throttle_out(bytes.len()).await;
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;
        self.metering.record_out(bytes.len());
        Ok(())
    }

    /// Whether the internal parse buffer is empty.
    pub(crate) fn buffer_is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Set TCP keepalive on the underlying socket (RFC 1122 Section 4.2.3.6).
    ///
    /// Delegates to [`ImapStream::set_keepalive`]. Safe to call at any time  -
    /// does not touch the read buffer or affect wire protocol state.
    pub(in crate::connection) fn set_keepalive(
        &self,
        ka: &super::TcpKeepalive,
    ) -> Result<(), Error> {
        self.stream.set_keepalive(ka)
    }

    /// Consume the reader and return the owned stream.
    ///
    /// Used by stream upgrades (STARTTLS, COMPRESS): drop this reader
    /// (and its buffer), transform the stream, and construct a fresh
    /// reader on the new stream. This enforces I10  -  buffer bytes
    /// cannot straddle a stream upgrade.
    pub(super) fn into_stream(self) -> ImapStream {
        self.stream
    }

    /// Take all remaining bytes from the internal parse buffer.
    ///
    /// Used by COMPRESS (RFC 4978 Section3): after the tagged OK, any bytes
    /// already buffered are compressed data that must be preserved in
    /// the new `CompressedStream`'s raw read buffer.
    pub(super) fn take_buffer(&mut self) -> bytes::BytesMut {
        self.buf.split_off(0)
    }

    // -----------------------------------------------------------------------
    // Private helpers  -  ported from ImapConnection::try_parse_response,
    // buffer_may_contain_complete_response, and try_parse_literal_marker
    // in helpers.rs. The logic is identical; only field accesses changed
    // from `self.read_buf` to `self.buf`.
    // -----------------------------------------------------------------------

    /// Try to parse a greeting from the internal buffer (RFC 3501 Section7.1).
    fn try_parse_greeting_inner(&mut self) -> Result<Option<Response>, Error> {
        use crate::codec::decode::parse_greeting;
        if self.buf.is_empty() {
            return Ok(Option::None);
        }
        if !buffer_may_contain_complete_response(&self.buf) {
            return Ok(Option::None);
        }
        match parse_greeting(&self.buf) {
            Ok((remaining, resp)) => {
                let consumed = self.buf.len() - remaining.len();
                let _ = self.buf.split_to(consumed);
                Ok(Some(resp))
            }
            Err(nom::Err::Incomplete(_)) => Ok(Option::None),
            Err(e) => {
                if !self.buf.ends_with(b"\r\n") {
                    trace!(
                        "greeting parse Error on buffer not ending with CRLF  -  \
                         treating as incomplete ({} bytes buffered)",
                        self.buf.len()
                    );
                    return Ok(Option::None);
                }
                warn!("greeting parse error: {e}");
                Err(Error::Parse(format!("greeting parse error: {e}")))
            }
        }
    }

    /// Try to parse a response from the internal buffer.
    ///
    /// Uses `parse_response_utf8` in UTF-8 mode when the caller indicates
    /// `UTF8=ACCEPT` (RFC 6855 Section 3) or `IMAP4rev2` is active
    /// (RFC 9051 Section 7: quoted-strings carry raw UTF-8).
    fn try_parse_response_inner(&mut self, utf8_mode: bool) -> Result<Option<Response>, Error> {
        if self.buf.is_empty() {
            return Ok(Option::None);
        }

        // The parser uses `nom::bytes::complete` mode, which returns `Error`
        // (not `Incomplete`) when input is truncated mid-response. We must
        // detect incomplete data ourselves to avoid treating partial TCP
        // reads as hard parse failures.
        //
        // Every IMAP response ends with \r\n. If the buffer doesn't contain
        // \r\n at all, the first response is definitely incomplete.
        if !buffer_may_contain_complete_response(&self.buf) {
            return Ok(Option::None);
        }

        match parse_response_utf8(&self.buf, utf8_mode) {
            Ok((remaining, resp)) => {
                let consumed = self.buf.len() - remaining.len();
                let _ = self.buf.split_to(consumed);
                Ok(Some(resp))
            }
            Err(nom::Err::Incomplete(_)) => Ok(Option::None),
            Err(e) => {
                // The parser failed even though we thought the data was
                // complete. If the buffer doesn't end with \r\n, it's
                // likely a partial read that our heuristic didn't catch
                // (e.g., literal data spanning TCP segments).
                if !self.buf.ends_with(b"\r\n") {
                    trace!(
                        "parse Error on buffer not ending with CRLF  -  treating as incomplete \
                         ({} bytes buffered)",
                        self.buf.len()
                    );
                    return Ok(Option::None);
                }
                warn!("response parse error: {e}");
                Err(Error::Parse(format!("response parse error: {e}")))
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct WireMetering {
    sink: Option<MeterSinkHandle>,
    bandwidth_cap: Option<Arc<AtomicU64>>,
    in_bucket: ByteBucket,
    out_bucket: ByteBucket,
}

impl WireMetering {
    #[cfg(test)]
    fn disabled() -> Self {
        Self::new(None, None)
    }

    fn new(sink: Option<MeterSinkHandle>, bandwidth_cap: Option<Arc<AtomicU64>>) -> Self {
        let initial_cap = bandwidth_cap
            .as_ref()
            .and_then(|cap| bandwidth_cap_from_raw(cap.load(Ordering::Relaxed)));
        Self {
            sink,
            bandwidth_cap,
            in_bucket: ByteBucket::new(initial_cap),
            out_bucket: ByteBucket::new(initial_cap),
        }
    }

    async fn record_in(&self, n: usize) {
        let n = u64::try_from(n).unwrap_or(u64::MAX);
        if let Some(sink) = &self.sink {
            sink.record_bytes_in(n);
        }
        if self.bandwidth_cap.is_some() {
            self.in_bucket.consume(n, self.cap_now()).await;
        }
    }

    async fn throttle_out(&self, n: usize) {
        if self.bandwidth_cap.is_some() {
            let n = u64::try_from(n).unwrap_or(u64::MAX);
            self.out_bucket.consume(n, self.cap_now()).await;
        }
    }

    fn record_out(&self, n: usize) {
        if let Some(sink) = &self.sink {
            sink.record_bytes_out(u64::try_from(n).unwrap_or(u64::MAX));
        }
    }

    fn cap_now(&self) -> Option<u64> {
        self.bandwidth_cap
            .as_ref()
            .and_then(|cap| bandwidth_cap_from_raw(cap.load(Ordering::Relaxed)))
    }
}

fn bandwidth_cap_from_raw(raw: u64) -> Option<u64> {
    (raw != u64::MAX).then_some(raw.max(1))
}

#[derive(Clone)]
struct ByteBucket {
    state: Arc<Mutex<ByteBucketState>>,
}

struct ByteBucketState {
    tokens: f64,
    last_refill: Instant,
}

impl ByteBucket {
    fn new(cap: Option<u64>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ByteBucketState {
                tokens: cap.map_or(0.0, |cap| cap as f64),
                last_refill: Instant::now(),
            })),
        }
    }

    async fn consume(&self, n: u64, cap: Option<u64>) {
        let Some(cap) = cap else { return };
        if cap == 0 || n == 0 {
            return;
        }
        let cap_f = cap as f64;
        let want = n as f64;
        if n > cap {
            let secs = (want / cap_f).min(60.0);
            tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            let mut state = self.state.lock().expect("byte bucket lock poisoned");
            state.tokens = 0.0;
            state.last_refill = Instant::now();
            return;
        }
        loop {
            let wait = {
                let mut state = self.state.lock().expect("byte bucket lock poisoned");
                let now = Instant::now();
                let elapsed = now.duration_since(state.last_refill).as_secs_f64();
                state.tokens = (state.tokens + elapsed * cap_f).min(cap_f);
                state.last_refill = now;
                if state.tokens >= want {
                    state.tokens -= want;
                    return;
                }
                let deficit = want - state.tokens;
                Duration::from_secs_f64((deficit / cap_f).min(60.0))
            };
            tokio::time::sleep(wait).await;
        }
    }
}

// DELIBERATELY ABSENT  -  do NOT add:
// - fn reset_buffer(&mut self)
// - fn set_stream(&mut self, s: ImapStream)
// - fn buffer_mut(&mut self) -> &mut BytesMut
// - any Deref/DerefMut/AsMut impl that exposes buf or stream
// These absences are the enforcement of I10.

// ---------------------------------------------------------------------------
// Module-private standalone helpers  -  ported verbatim from
// ImapConnection::buffer_may_contain_complete_response and
// ImapConnection::try_parse_literal_marker in helpers.rs.
// ---------------------------------------------------------------------------

/// Check whether `buf` likely contains at least one complete IMAP response.
///
/// A complete response always ends with `\r\n`. If the buffer contains
/// literal markers `{N}\r\n`, we verify that N bytes of literal data
/// follow each one, iterating through all literals in the response
/// (e.g., multi-body FETCH responses).
pub(super) fn buffer_may_contain_complete_response(buf: &[u8]) -> bool {
    // Fast path: no \r\n means definitely incomplete.
    let Some(first_crlf) = buf.windows(2).position(|w| w == b"\r\n") else {
        return false;
    };

    // Check for a literal marker `{digits[+]}\r\n` ending at first_crlf.
    // If present, verify we have the literal body + more data after it.
    // Also handles `~{digits}\r\n` (literal8, RFC 3516).
    //
    // Status responses (OK/NO/BAD/BYE) and tagged responses never contain
    // literals  -  their text is `1*TEXT-CHAR` (RFC 3501 Section 9), not
    // `string`. Skip the literal check for these to avoid misclassifying
    // `{digits}` in status text (e.g. `* OK [ALERT] limit {42}\r\n`).
    let may_contain_literal = if buf.starts_with(b"* ") && buf.len() >= 5 {
        // Untagged status responses: `* OK ...`, `* NO ...`, `* BAD ...`, `* BYE ...`
        // These use `resp-text` which has no literal production.
        // RFC 3501 Section 9: all alphabetic characters are case-insensitive,
        // so compare case-insensitively to handle non-conformant servers.
        let after_star = &buf[2..];
        let is_status = after_star
            .get(..3)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"OK ") || s.eq_ignore_ascii_case(b"NO "))
            || after_star.get(..4).is_some_and(|s| {
                s.eq_ignore_ascii_case(b"BAD ") || s.eq_ignore_ascii_case(b"BYE ")
            });
        !is_status
    } else if buf.starts_with(b"+") {
        // Continuation responses never contain literals.
        false
    } else {
        // Tagged responses (`TAG OK/NO/BAD ...`) use `resp-cond-state`
        // which also has no literal production (RFC 3501 Section 7.1).
        // Detect the tag boundary and check for a status keyword.
        //
        // RFC 3501 Section 9: `tag = 1*<any ASTRING-CHAR except "+">`.
        // A tagged response is `tag SP resp-cond-state CRLF` where
        // resp-cond-state = ("OK" / "NO" / "BAD") SP resp-text.
        // resp-text is `1*TEXT-CHAR` (no literal production), so
        // `{digits}` in the text is NOT a literal marker.
        if let Some(sp) = buf.iter().position(|&b| b == b' ') {
            let after_tag = &buf[sp + 1..];
            let is_tagged_status = after_tag
                .get(..3)
                .is_some_and(|s| s.eq_ignore_ascii_case(b"OK ") || s.eq_ignore_ascii_case(b"NO "))
                || after_tag
                    .get(..4)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"BAD "));
            // Also handle the edge case where the status keyword is
            // followed directly by CRLF (e.g., `TAG OK\r\n` with no
            // resp-text).
            let is_tagged_status = is_tagged_status
                || after_tag
                    .get(..4)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"OK\r\n"))
                || after_tag
                    .get(..4)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"NO\r\n"))
                || after_tag
                    .get(..5)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"BAD\r\n"));
            !is_tagged_status
        } else {
            // No space found  -  cannot be a valid tagged response.
            // Conservatively check for literals.
            true
        }
    };

    if first_crlf >= 2
        && may_contain_literal
        && let Some(literal_len) = try_parse_literal_marker(buf, first_crlf)
    {
        // Skip past the first literal body and iteratively check
        // for additional literals (e.g., multi-body FETCH).
        let mut pos = first_crlf + 2 + literal_len;
        loop {
            if pos > buf.len() {
                return false;
            }
            // Find the next \r\n after the current literal body.
            let remaining = &buf[pos..];
            let Some(next_crlf) = remaining.windows(2).position(|w| w == b"\r\n") else {
                return false;
            };
            // Check if there's another literal marker at this CRLF.
            if let Some(next_literal_len) = try_parse_literal_marker(remaining, next_crlf) {
                // Another literal  -  skip past it and continue.
                pos += next_crlf + 2 + next_literal_len;
                continue;
            }
            // No more literals  -  this CRLF terminates the response.
            return true;
        }
    }

    // No literal  -  the first \r\n likely terminates a complete response.
    true
}

/// Try to parse a literal marker `{digits[+]}` ending just before `crlf_pos`
/// in `buf`. Returns the literal byte count if a valid marker is found.
fn try_parse_literal_marker(buf: &[u8], crlf_pos: usize) -> Option<usize> {
    let before_crlf = &buf[..crlf_pos];
    let brace_pos = before_crlf.iter().rposition(|&b| b == b'{')?;
    let close_offset = buf[brace_pos + 1..crlf_pos]
        .iter()
        .position(|&b| b == b'}')?;
    let between = &buf[brace_pos + 1..brace_pos + 1 + close_offset];
    // Strip optional trailing '+' for LITERAL+ (RFC 7888) or
    // LITERAL- (RFC 7888 Section 5).
    let digits = if between.last() == Some(&b'+') {
        &between[..between.len() - 1]
    } else {
        between
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
}
