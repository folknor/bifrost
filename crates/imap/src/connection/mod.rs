//! IMAP client connection.
//!
//! `ImapConnection` is a plain struct (no typestate generics). Callers manage
//! session lifecycle themselves.
//!
//! Connection and authentication are defined in RFC 3501 Sections 6.1-6.2 /
//! RFC 9051 Sections 6.1-6.2.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use flate2::{Compress, Decompress, FlushCompress, FlushDecompress, Status};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;

use tracing::{debug, warn};

use crate::codec::encode::{
    LiteralMode, encode_multi_append_header_with_literal8, encode_quoted_or_literal,
    encode_quoted_or_literal_utf8,
};
use crate::error::Error;
use crate::types::{
    AclEntry, AppendMessage, Capability, Command, CopyResult, EsearchResponse, ExpungeResult,
    FetchAttr, FetchResponse, Flag, ListRightsResponse, MailboxAttribute, MailboxFilter,
    MailboxInfo, MailboxName, MetadataEntry, MetadataResult, MoveResult, NamespaceResponse,
    NotifyEvent, NotifySetParams, QresyncParams, QuotaResource, QuotaRootResponse, Response,
    ResponseCode, SelectOptions, SelectedMailbox, SequenceSet, StatusItem, StatusResult,
    StoreOperation, StoreResult, TaggedResponse, ThreadNode, UidRange, UntaggedResponse,
    UntaggedStatus, format_fetch_attrs,
};

mod append;
mod auth;
pub(super) mod dispatch;
pub(super) mod driver;
mod extensions;
mod helpers;
mod idle;
mod lifecycle;
mod mailbox;
pub(super) mod pipeline;
mod seq_ops;
mod sort_thread;
pub(super) mod state;
mod tag;
/// Typed event enum for asynchronous server notifications.
///
/// See [`TypedEvent`](typed_event::TypedEvent) for the event variants
/// observable via [`drain_events`](ImapConnection::drain_events) and
/// [`next_event`](ImapConnection::next_event).
pub mod typed_event;
mod uid_ops;
pub(super) mod wire;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

/// TLS policy for an IMAP connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TlsMode {
    /// Connect directly over TLS.
    Implicit,
    /// Connect in cleartext, then upgrade via STARTTLS.
    StartTls,
    /// Connect without TLS.
    None,
}

impl TlsMode {
    fn uses_implicit_tls(self) -> bool {
        matches!(self, Self::Implicit)
    }

    fn uses_starttls(self) -> bool {
        matches!(self, Self::StartTls)
    }
}

/// TCP keepalive configuration for the underlying socket.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TcpKeepalive {
    /// Time before the first keepalive probe.
    pub time: Duration,
    /// Interval between subsequent probes.
    pub interval: Duration,
}

impl TcpKeepalive {
    /// Create a TCP keepalive configuration with the given time and interval.
    pub fn new(time: Duration, interval: Duration) -> Self {
        Self { time, interval }
    }
}

/// IMAP session state (RFC 3501 Section 3 / RFC 9051 Section 3).
///
/// Tracks the current protocol state of the connection. State transitions
/// are managed automatically by `ImapConnection` methods.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SessionState {
    /// Not Authenticated  -  client must authenticate (RFC 3501 Section 3.1).
    NotAuthenticated,
    /// Authenticated  -  client may select a mailbox (RFC 3501 Section 3.2).
    Authenticated,
    /// Selected  -  a mailbox is open (RFC 3501 Section 3.3).
    Selected,
    /// Logout  -  connection is being closed (RFC 3501 Section 3.4).
    Logout,
}

/// Event received during an IDLE session (RFC 2177).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IdleEvent {
    /// New message(s) arrived  -  `* <n> EXISTS` (RFC 3501 Section 7.3.1).
    Exists(u32),
    /// Message expunged  -  `* <n> EXPUNGE` (RFC 3501 Section 7.4.1).
    Expunge(u32),
    /// Messages vanished  -  `* VANISHED [EARLIER] uid-set` (RFC 7162 Section 3.2.10.2).
    ///
    /// After `ENABLE QRESYNC`, servers send VANISHED instead of EXPUNGE.
    Vanished {
        /// `true` if this was a `VANISHED (EARLIER)` response (initial sync).
        earlier: bool,
        /// UIDs of vanished messages.
        uids: Vec<UidRange>,
    },
    /// Message data changed  -  `* n FETCH ...` (RFC 3501 Section 7.4.2, RFC 2177 Section 3).
    ///
    /// During IDLE, the server may send unsolicited FETCH responses when message
    /// attributes change (e.g., flags updated by another session). The full
    /// `FetchResponse` is preserved so callers can inspect the sequence number,
    /// UID, flags, and any other returned data items.
    Fetch(Box<crate::types::FetchResponse>),
    /// Recent message count changed  -  `* <n> RECENT` (RFC 3501 Section 7.3.2).
    ///
    /// RFC 2177 allows the server to send mailbox size messages during IDLE;
    /// `* n RECENT` is one such message.
    Recent(u32),
    /// Server sent an ALERT that MUST be presented to the user
    /// (RFC 3501 Section 7.1).
    ///
    /// RFC 3501 Section 7.1 mandates: "The human-readable text contains a
    /// special alert that MUST be presented to the user in a fashion that
    /// calls the user's attention to the message." RFC 2177 (IDLE) does not
    /// exempt this requirement, so alerts received during IDLE are surfaced
    /// immediately rather than buffered.
    Alert(String),
    /// The idle timed out (caller-supplied timeout elapsed).
    Timeout,
    /// The idle was cancelled via the `CancellationToken`.
    Cancelled,
    /// Mailbox created, deleted, renamed, or access changed  -  `* LIST ...`
    /// (RFC 5465 Sections 5.4-5.5).
    ///
    /// When NOTIFY is active with `MailboxName` (Section5.4) or
    /// `SubscriptionChange` (Section5.5) events, the server delivers
    /// mailbox-level notifications as LIST responses during IDLE.
    MailboxEvent(MailboxInfo),
    /// Non-selected mailbox status changed  -  `* STATUS "mailbox" (...)`
    /// (RFC 5465 Section 4, Sections 5.1-5.3).
    ///
    /// When NOTIFY is active with message events (Section5.1-5.3) on
    /// non-selected mailboxes, the server delivers status changes (new
    /// messages, expunges, flag changes) as STATUS responses during IDLE.
    /// The initial snapshot is delivered per the STATUS indicator (Section4).
    MailboxStatus {
        /// The mailbox whose status changed.
        mailbox: MailboxName,
        /// The status items that changed.
        items: Vec<StatusItem>,
    },
    /// Mailbox or server metadata changed  -  `* METADATA "mailbox" (...)`
    /// (RFC 5465 Sections 5.6-5.7).
    ///
    /// When NOTIFY is active with `MailboxMetadataChange` (Section5.6) or
    /// `ServerMetadataChange` (Section5.7) events, the server delivers metadata
    /// notifications as METADATA responses during IDLE.
    MetadataChange {
        /// The mailbox whose metadata changed (empty string for server-level).
        mailbox: MailboxName,
        /// The metadata entries that changed.
        entries: Vec<MetadataEntry>,
    },
    /// Search context update  -  `* ESEARCH ...` or `* SEARCH ...`
    /// (RFC 5267 Sections 2.4 / RFC 5465 Sections 5.1-5.3).
    ///
    /// When a search context is active (RFC 5267) or NOTIFY triggers
    /// search-related notifications, the server may deliver ESEARCH
    /// updates during IDLE.
    ///
    /// **Note on legacy `* SEARCH` conversion:** When the server sends a
    /// legacy `* SEARCH n1 n2 ...` update (instead of ESEARCH), the numbers
    /// are wrapped in an `EsearchResponse` with `uid: false`. However, IMAP
    /// uses the same wire form for both sequence-number and UID results  -
    /// the `uid` field may be inaccurate for legacy SEARCH. Callers should
    /// use the `tag` field (if present) to correlate with the original search
    /// context and determine the number semantics.
    SearchUpdate(Box<crate::types::EsearchResponse>),
    /// Extension-defined untagged response not recognized by the parser
    /// (RFC 9051 Section 2.2.2).
    ///
    /// When `NotifyEvent::Other(...)` is registered, the server may deliver
    /// extension-defined notifications using response types this client does
    /// not implement. The raw response line is preserved so callers can
    /// parse extension data themselves.
    ExtensionEvent(String),
    /// Unsolicited status update with a response code  -  `* OK [code]` or
    /// `* NO [code]` (RFC 3501 Section 7.1).
    ///
    /// Covers `[PERMANENTFLAGS]`, `[UIDVALIDITY]`, and any other response
    /// code not handled by a more specific variant. Surfaced during IDLE
    /// so the caller can react to state changes (e.g., updated writable
    /// flags, NOTIFY-driven metadata updates).
    StatusUpdate {
        /// The original response condition (RFC 3501 Section 7.1).
        /// OK, NO, and BAD are semantically distinct and must be preserved.
        status: UntaggedStatus,
        /// The response code.
        code: ResponseCode,
        /// Human-readable text.
        text: String,
    },
    /// The server discarded the NOTIFY registration due to overflow
    /// (RFC 5465 Section 5.8).
    ///
    /// The client MUST behave as if `NOTIFY NONE` was received  -  the
    /// registration is gone and no further notifications will be delivered.
    /// Callers should re-issue `notify_set()` to re-establish monitoring.
    NotificationOverflow {
        /// The response-code payload from `[NOTIFICATIONOVERFLOW ...]`
        /// (RFC 5465 Section 5.8). `None` when the server omits the
        /// optional argument.
        code_text: Option<String>,
        /// Human-readable text from the untagged status line
        /// (RFC 3501 Section 7.1).
        resp_text: String,
    },
    /// The server sent `* BYE`  -  the connection is closing
    /// (RFC 3501 Section 7.1.5).
    ///
    /// Unlike [`Alert`], this signals that the server is terminating
    /// the connection. After receiving this event, further commands
    /// will fail.
    Bye {
        /// Optional response code from the BYE response.
        code: Option<ResponseCode>,
        /// Human-readable reason for disconnection.
        text: String,
    },
    /// The server terminated the IDLE session by sending the tagged OK
    /// response (RFC 2177 Section 3).
    ///
    /// Some servers (Exchange, Zimbra) have short IDLE limits and terminate
    /// IDLE from the server side by sending the tagged OK rather than
    /// waiting for the client's DONE. When this happens, the client MUST NOT
    /// send DONE because the IDLE command is already complete.
    ServerTerminated,
}

/// Result of a SEARCH or UID SEARCH command.
///
/// Contains both the matching sequence numbers/UIDs and the optional
/// highest mod-sequence value (RFC 7162 Section 3.1.5).
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SearchResult {
    /// Matching message sequence numbers (SEARCH) or UIDs (UID SEARCH).
    pub ids: Vec<u32>,
    /// Highest mod-sequence of matching messages, if MODSEQ was used
    /// in the search criteria (RFC 7162 Section 3.1.5).
    pub mod_seq: Option<u64>,
    /// `true` when ESEARCH UID range expansion was capped at the internal
    /// safety limit and the returned `ids` do not faithfully represent the
    /// full server response (RFC 4731 Section 3, RFC 3501 Section 6.4.4).
    pub truncated: bool,
}

// ---------------------------------------------------------------------------
// Stream abstraction
// ---------------------------------------------------------------------------

/// The inner transport stream  -  either plain TCP or TLS over TCP.
///
/// Used as the underlying I/O transport for both uncompressed and compressed
/// connections. The `Tls` variant is large due to TLS session state  -  same
/// rationale as `ImapStream` for not boxing.
#[allow(clippy::large_enum_variant)]
enum InnerStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl InnerStream {
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read_buf(buf).await,
            Self::Tls(s) => s.read_buf(buf).await,
        }
    }

    async fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.write_all(data).await,
            Self::Tls(s) => s.write_all(data).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush().await,
            Self::Tls(s) => s.flush().await,
        }
    }
}

/// Compressed stream wrapper implementing COMPRESS=DEFLATE (RFC 4978).
///
/// Wraps an `InnerStream` with raw deflate compression/decompression.
/// Per RFC 4978 Section 3, uses raw deflate (RFC 1951)  -  not gzip, not zlib  -
/// with `SyncFlush` after each write to ensure the peer can decode immediately.
struct CompressedStream {
    /// The underlying TCP or TLS stream.
    inner: InnerStream,
    /// Decompressor for inflating data received from the server.
    decompress: Decompress,
    /// Compressor for deflating data sent to the server.
    compress: Compress,
    /// Buffer holding raw (compressed) bytes read from the network that have
    /// not yet been inflated.
    raw_read_buf: BytesMut,
    /// Scratch buffer used as destination during inflate operations.
    inflate_buf: Vec<u8>,
}

/// Initial size of the raw-read buffer for compressed streams.
const COMPRESSED_RAW_BUF_SIZE: usize = 8192;
/// Size of the scratch buffer used during inflate.
const INFLATE_BUF_SIZE: usize = 16384;
/// Size of the output buffer used during deflate.
const DEFLATE_BUF_SIZE: usize = 16384;

impl CompressedStream {
    /// Create a new compressed stream wrapping the given inner stream.
    ///
    /// Initialises raw deflate compressor/decompressor per RFC 4978 Section 3.
    fn new(inner: InnerStream) -> Self {
        Self {
            inner,
            // RFC 4978 Section 3: raw deflate (no zlib/gzip header).
            decompress: Decompress::new(false),
            compress: Compress::new(flate2::Compression::default(), false),
            raw_read_buf: BytesMut::with_capacity(COMPRESSED_RAW_BUF_SIZE),
            inflate_buf: vec![0u8; INFLATE_BUF_SIZE],
        }
    }

    /// Read decompressed data into `buf`.
    ///
    /// Reads compressed bytes from the inner stream, then inflates them into
    /// the caller's buffer. Returns the number of decompressed bytes appended.
    //
    // The u64->usize casts on total_in/total_out deltas are safe: each delta
    // is bounded by the buffer size (at most INFLATE_BUF_SIZE = 16 KiB).
    #[allow(clippy::cast_possible_truncation)]
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        loop {
            // Try to inflate any data already in the raw buffer.
            if !self.raw_read_buf.is_empty() {
                let before_in = self.decompress.total_in();
                let before_out = self.decompress.total_out();

                let status = self
                    .decompress
                    .decompress(
                        &self.raw_read_buf,
                        &mut self.inflate_buf,
                        FlushDecompress::Sync,
                    )
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("deflate decompression error: {e}"),
                        )
                    })?;

                let consumed = (self.decompress.total_in() - before_in) as usize;
                let produced = (self.decompress.total_out() - before_out) as usize;

                // Advance past consumed compressed bytes.
                if consumed > 0 {
                    let _ = self.raw_read_buf.split_to(consumed);
                }

                if produced > 0 {
                    buf.extend_from_slice(&self.inflate_buf[..produced]);
                    return Ok(produced);
                }

                // If the stream has ended, signal EOF.
                if status == Status::StreamEnd {
                    return Ok(0);
                }
            }

            // Need more compressed data from the network.
            let n = self.inner.read_buf(&mut self.raw_read_buf).await?;
            if n == 0 {
                return Ok(0); // EOF on underlying stream.
            }
        }
    }

    /// Compress `data` and write it to the inner stream.
    ///
    /// Per RFC 4978 Section 3, each IMAP command/response is terminated with
    /// `SyncFlush` so the peer can decompress without waiting for more data.
    //
    // The u64->usize casts on total_in/total_out deltas are safe: each delta
    // is bounded by the buffer size (at most DEFLATE_BUF_SIZE = 16 KiB).
    #[allow(clippy::cast_possible_truncation)]
    async fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        let mut deflate_buf = vec![0u8; DEFLATE_BUF_SIZE];
        let mut input_offset = 0;

        // Compress the input data in chunks.
        while input_offset < data.len() {
            let before_in = self.compress.total_in();
            let before_out = self.compress.total_out();

            self.compress
                .compress(&data[input_offset..], &mut deflate_buf, FlushCompress::None)
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("deflate compression error: {e}"),
                    )
                })?;

            let consumed = (self.compress.total_in() - before_in) as usize;
            let produced = (self.compress.total_out() - before_out) as usize;

            input_offset += consumed;

            if produced > 0 {
                self.inner.write_all(&deflate_buf[..produced]).await?;
            }
        }

        // SyncFlush to ensure the server can decode immediately
        // (RFC 4978 Section 3).
        //
        // Issue the Sync flush exactly once, then switch to None for any
        // remaining overflow. Calling Sync repeatedly would emit a new
        // sync marker each time (producing output indefinitely).
        let mut flush = FlushCompress::Sync;
        loop {
            let before_out = self.compress.total_out();

            self.compress
                .compress(&[], &mut deflate_buf, flush)
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("deflate sync-flush error: {e}"),
                    )
                })?;

            let produced = (self.compress.total_out() - before_out) as usize;

            if produced > 0 {
                self.inner.write_all(&deflate_buf[..produced]).await?;
            }

            if produced == 0 {
                break;
            }

            // After the initial Sync, switch to None to drain any remaining
            // buffered output without emitting additional sync markers.
            flush = FlushCompress::None;
        }

        Ok(())
    }

    /// Flush the underlying stream.
    async fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush().await
    }
}

/// Wraps either a plain TCP, TLS, or compressed stream.
///
/// Delegates `AsyncRead`/`AsyncWrite` via match  -  no `unsafe` code.
/// The `Tls` variant is large due to the TLS session state  -  boxing it
/// would add indirection on every I/O call, which is not worth it.
#[allow(clippy::large_enum_variant)]
enum ImapStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
    /// Compressed stream per RFC 4978 (COMPRESS=DEFLATE).
    Compressed(CompressedStream),
    /// Sentinel used during in-progress stream upgrades (STARTTLS,
    /// COMPRESS). All I/O operations return an error immediately.
    /// If the upgrade fails or the future is cancelled, the stream
    /// stays `Poisoned` forever and the connection is dead  -  this is
    /// the enforcement of I9 (atomic upgrades).
    /// RFC 3501 Section6.2.1 / RFC 4978.
    Poisoned,
    /// In-memory transport used by the test harness. Backed by
    /// [`tokio::io::DuplexStream`] so unit tests do not need to bind a real
    /// loopback socket  -  restricted environments (sandboxes, hardened CI
    /// runners) can disallow `TcpListener::bind("127.0.0.1:0")`. Gated to
    /// `cfg(test)` so production builds carry zero overhead.
    #[cfg(test)]
    Memory(tokio::io::DuplexStream),
}

impl ImapStream {
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read_buf(buf).await,
            Self::Tls(s) => s.read_buf(buf).await,
            Self::Compressed(s) => s.read_buf(buf).await,
            Self::Poisoned => Err(std::io::Error::other("stream poisoned during upgrade")),
            #[cfg(test)]
            Self::Memory(s) => s.read_buf(buf).await,
        }
    }

    async fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.write_all(data).await,
            Self::Tls(s) => s.write_all(data).await,
            Self::Compressed(s) => s.write_all(data).await,
            Self::Poisoned => Err(std::io::Error::other("stream poisoned during upgrade")),
            #[cfg(test)]
            Self::Memory(s) => s.write_all(data).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush().await,
            Self::Tls(s) => s.flush().await,
            Self::Compressed(s) => s.flush().await,
            Self::Poisoned => Err(std::io::Error::other("stream poisoned during upgrade")),
            #[cfg(test)]
            Self::Memory(s) => s.flush().await,
        }
    }

    /// Set TCP keepalive on the underlying socket (RFC 1122 Section 4.2.3.6).
    ///
    /// Configures the operating system's TCP keepalive probes via
    /// `setsockopt(2)`. Works on plain TCP, TLS (reaches through to the
    /// inner `TcpStream`), and compressed streams. Returns an error for
    /// `Poisoned` (upgrade in progress) and `Memory` (test-only) streams.
    fn set_keepalive(&self, ka: &TcpKeepalive) -> Result<(), Error> {
        use socket2::SockRef;

        let sock_ka = socket2::TcpKeepalive::new()
            .with_time(ka.time)
            .with_interval(ka.interval);

        let result = match self {
            Self::Plain(tcp) => SockRef::from(tcp).set_tcp_keepalive(&sock_ka),
            Self::Tls(tls) => {
                SockRef::from(tls.get_ref().get_ref().get_ref()).set_tcp_keepalive(&sock_ka)
            }
            Self::Compressed(c) => {
                let inner_result = match &c.inner {
                    InnerStream::Plain(tcp) => SockRef::from(tcp).set_tcp_keepalive(&sock_ka),
                    InnerStream::Tls(tls) => {
                        SockRef::from(tls.get_ref().get_ref().get_ref()).set_tcp_keepalive(&sock_ka)
                    }
                };
                inner_result
            }
            Self::Poisoned => {
                return Err(Error::Io(std::sync::Arc::new(std::io::Error::other(
                    "cannot set keepalive: stream is in upgrade transition",
                ))));
            }
            #[cfg(test)]
            Self::Memory(_) => {
                return Err(Error::Io(std::sync::Arc::new(std::io::Error::other(
                    "keepalive not supported on memory streams",
                ))));
            }
        };
        result.map_err(|e| Error::Io(std::sync::Arc::new(e)))
    }

    /// Extract the underlying `TcpStream` for STARTTLS upgrade.
    fn into_tcp(self) -> Option<TcpStream> {
        match self {
            Self::Plain(s) => Some(s),
            Self::Tls(_) | Self::Compressed(_) | Self::Poisoned => Option::None,
            #[cfg(test)]
            // Memory streams cannot upgrade to TLS  -  STARTTLS tests must
            // use the real transport.
            Self::Memory(_) => Option::None,
        }
    }
}

// ---------------------------------------------------------------------------
// ImapConnection
// ---------------------------------------------------------------------------

/// An IMAP client connection (RFC 3501 Section 2 / RFC 9051 Section 2).
///
/// Manages a single TCP (or TLS) connection to an IMAP server. All operations
/// are async and require a caller-supplied timeout  -  there are no hardcoded
/// defaults and no infinite waits.
///
/// # Connection state
///
/// RFC 3501 Section 3 defines four session states: Not Authenticated,
/// Authenticated, Selected, and Logout. Each command method validates that
/// the connection is in an allowed state before sending, returning
/// [`Error::Protocol`] if not. For example, [`uid_fetch()`](Self::uid_fetch)
/// requires the Selected state and will fail if called before
/// [`select()`](Self::select).
pub struct ImapConnection {
    /// Channel for submitting commands to the driver task.
    cmd_tx: tokio::sync::mpsc::Sender<driver::DriverCommand>,
    /// Watch receiver for observing connection state snapshots.
    state_rx: tokio::sync::watch::Receiver<driver::ConnectionStateSnapshot>,
    /// Receiver for asynchronous server events (ALERTs, EXISTS, etc.).
    /// Wrapped in `Mutex` so event consumption does not require `&mut self`.
    events_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<typed_event::TypedEvent>>,
    /// Handle to the driver task. Observed on shutdown or on submit
    /// failure to surface panics as `Error::DriverPanicked`.
    /// Wrapped in `Mutex<Option<...>>` so that `observe_driver_panic`
    /// can take the handle when shutting down, without needing `&mut self`.
    driver_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Tag counter for pre-built commands (APPEND / MULTIAPPEND).
    ///
    /// Uses prefix `P` to avoid collision with the driver's hex-format
    /// tags. `AtomicU32` enables `&self` access without interior mutability.
    prebuilt_tag_counter: std::sync::atomic::AtomicU32,
    /// Server hostname, retained for STARTTLS upgrade (RFC 3501 Section6.2.1).
    ///
    /// Needed to construct the `ServerName` for TLS SNI and certificate
    /// verification when the caller invokes `starttls()`.
    host: String,
}

/// Per-type NOTIFY flags (RFC 5465 Sections 5.1-5.8).
///
/// Tracks which response types the current NOTIFY registration can produce
/// so that IDLE event classification and LIST filtering during LIST-family
/// commands know which types to expect.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NotifyFlags {
    /// `MailboxName` or `SubscriptionChange` events registered
    /// (RFC 5465 Sections 5.4-5.5  -  delivered as LIST).
    pub(crate) list: bool,
    /// `MessageNew`/`MessageExpunge` on non-selected mailboxes or STATUS
    /// indicator (RFC 5465 Sections 4, 5.1-5.2  -  delivered as STATUS).
    pub(crate) status: bool,
    /// `MailboxMetadataChange` or `ServerMetadataChange` events registered
    /// (RFC 5465 Sections 5.6-5.7  -  delivered as METADATA).
    pub(crate) metadata: bool,
}

// Compile-time proof that ImapConnection is Send  -  required for
// holding the connection across `.await` points in async tasks.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<ImapConnection>();
};

/// Wire literal syntax for APPEND message data.
///
/// RFC 3501 Section 4.3 / RFC 9051 Section 4.3 define classic `literal`
/// syntax for `CHAR8` data (no NUL octets). RFC 3516 Section 4.4 extends
/// APPEND with `literal8` for binary data, and RFC 6855 Section 4 wraps
/// `literal8` in `UTF8 (...)` when UTF8=ACCEPT is enabled for UTF-8 headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendLiteralKind {
    /// Classic `{N}` / `{N+}` APPEND literal for `CHAR8` data.
    /// RFC 3501 Section 4.3 / RFC 9051 Section 4.3.
    Literal,
    /// Binary `~{N}` APPEND literal for data containing NUL octets.
    /// RFC 3516 Section 4.4.
    Literal8,
    /// UTF8 APPEND wrapper using `UTF8 (~{N})`.
    /// RFC 6855 Section 4.
    Utf8Literal8,
}

impl ImapConnection {
    /// Generate the next tag for a pre-built command (APPEND/MULTIAPPEND).
    ///
    /// Uses `P` prefix to avoid collision with the driver's hex-format
    /// tags (RFC 3501 Section2.2.1). Safe to call from `&self` via atomic
    /// increment.
    pub(super) fn next_prebuilt_tag(&self) -> String {
        let n = self
            .prebuilt_tag_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .wrapping_add(1);
        format!("P{n:03}")
    }

    /// Drain all pending events from the typed event queue.
    ///
    /// Returns every [`TypedEvent`] that has accumulated since the last
    /// call to `drain_events` or `next_event`. Non-blocking  -  returns an
    /// empty `Vec` when no events are pending.
    ///
    /// This is the primary way to observe asynchronous server data
    /// (ALERTs, EXISTS/EXPUNGE, NOTIFY events, BYE, etc.) outside of an
    /// active command or IDLE session.
    pub async fn drain_events(&self) -> Vec<typed_event::TypedEvent> {
        let mut rx = self.events_rx.lock().await;
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            out.push(ev);
        }
        out
    }

    /// Wait for the next event from the typed event queue, with a
    /// timeout.
    ///
    /// Returns `Ok(Some(event))` when an event arrives, `Ok(None)` when
    /// `timeout` elapses without an event, or `Err(Error::DriverGone)`
    /// when the driver task has exited (channel closed).
    ///
    /// RFC 3501 Section5.3: servers may send untagged data at any time. This
    /// method surfaces that data as [`TypedEvent`]s, enabling callers to
    /// react to mailbox state changes, ALERTs, and NOTIFY events.
    pub async fn next_event(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<typed_event::TypedEvent>, crate::error::Error> {
        let mut rx = self.events_rx.lock().await;
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(ev)) => Ok(Some(ev)),
            Ok(None) => Err(crate::error::Error::DriverGone),
            Err(_) => Ok(None), // timeout
        }
    }
}

impl std::fmt::Debug for ImapConnection {
    /// Prints connection metadata useful for logging and diagnostics,
    /// without exposing internal stream state or buffers.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.state_rx.borrow();
        f.debug_struct("ImapConnection")
            .field("state", &snapshot.session_state)
            .field("capabilities_count", &snapshot.capabilities.len())
            .field("cmd_tx_closed", &self.cmd_tx.is_closed())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the default native-tls connector.
fn build_default_tls_connector() -> Result<native_tls::TlsConnector, Error> {
    native_tls::TlsConnector::new().map_err(|e| Error::Io(Arc::new(std::io::Error::other(e))))
}

fn validate_tls_server_name(host: &str) -> Result<(), Error> {
    if host.is_empty() {
        return Err(Error::Protocol("TLS server name must not be empty".into()));
    }
    if host.bytes().any(|b| b == 0 || b.is_ascii_whitespace()) {
        return Err(Error::Protocol(format!(
            "invalid TLS server name: {host:?}"
        )));
    }
    Ok(())
}

/// Find the next synchronizing literal boundary (`{digits}\r\n`) in `buf`.
///
/// Returns `Some((offset, size))` where `offset` is past the `\r\n` (i.e., the
/// literal data starts at `buf[offset..]`), and `size` is the literal byte count.
/// Returns `None` if no literal is found.
///
/// Only matches `{digits}\r\n` (synchronizing), NOT `{digits+}\r\n` (LITERAL+).
fn find_literal_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'{' {
            let start = i + 1;
            // Scan for digits
            let mut j = start;
            while j < buf.len() && buf[j].is_ascii_digit() {
                j += 1;
            }
            // Must have at least one digit, then `}\r\n`
            if j > start
                && j + 2 < buf.len()
                && buf[j] == b'}'
                && buf[j + 1] == b'\r'
                && buf[j + 2] == b'\n'
            {
                // Parse the literal size so callers can skip the body.
                // If the digit sequence is not valid UTF-8 or overflows usize,
                // skip this candidate and keep scanning  -  returning a 0-byte
                // literal would desynchronize the caller (RFC 3501 Section 4.3).
                let Ok(size_str) = std::str::from_utf8(&buf[start..j]) else {
                    i += 1;
                    continue;
                };
                let Ok(size) = size_str.parse::<usize>() else {
                    i += 1;
                    continue;
                };
                // This is a synchronizing literal (no `+` before `}`)
                return Some((j + 3, size));
            }
        }
        i += 1;
    }
    None
}

/// Patch all synchronizing literal markers in `buf` to non-synchronizing (LITERAL+).
///
/// Replaces every `{digits}\r\n` with `{digits+}\r\n` (RFC 7888 Section 4).
/// Length-aware: after patching a marker, skips the literal body so that
/// `{digits}\r\n` patterns inside literal data are not modified.
///
/// Literal8 markers (`~{digits}\r\n`, RFC 3516) are only converted when the
/// caller indicates that both BINARY and the relevant literal extension are
/// active (RFC 7888 Section 6).
fn patch_literals_to_plus_with_binary(buf: &[u8], allow_literal8: bool) -> BytesMut {
    let mut result = BytesMut::with_capacity(buf.len() + 16);
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'{' {
            let start = i + 1;
            let mut j = start;
            while j < buf.len() && buf[j].is_ascii_digit() {
                j += 1;
            }
            if j > start
                && j + 2 < buf.len()
                && buf[j] == b'}'
                && buf[j + 1] == b'\r'
                && buf[j + 2] == b'\n'
            {
                // Parse the literal size to skip the body.
                // If the digit sequence overflows usize, treat the `{...}` as
                // plain text  -  do not patch it (RFC 3501 Section 4.3).
                let Ok(size_str) = std::str::from_utf8(&buf[start..j]) else {
                    result.extend_from_slice(&buf[i..=i]);
                    i += 1;
                    continue;
                };
                let Ok(size) = size_str.parse::<usize>() else {
                    result.extend_from_slice(&buf[i..=i]);
                    i += 1;
                    continue;
                };

                // RFC 7888 Section 6 / RFC 3516: literal8 markers (`~{N}\r\n`)
                // may only use the non-synchronizing form when BINARY is also
                // advertised alongside LITERAL+.
                let is_literal8 = i > 0 && buf[i - 1] == b'~';

                // Copy `{digits` then insert `+}\r\n` for non-synchronizing
                // literals that the server advertised support for.
                result.extend_from_slice(&buf[i..j]);
                if !is_literal8 || allow_literal8 {
                    result.extend_from_slice(b"+}\r\n");
                } else {
                    result.extend_from_slice(b"}\r\n");
                }
                let body_start = j + 3;
                // Copy the literal body verbatim (RFC 3501 Section 4.3).
                // Use checked arithmetic to prevent overflow when `size`
                // is near `usize::MAX` (e.g., from a crafted command).
                let body_end = body_start
                    .checked_add(size)
                    .map_or(buf.len(), |end| end.min(buf.len()));
                result.extend_from_slice(&buf[body_start..body_end]);
                i = body_end;
                continue;
            }
        }
        result.extend_from_slice(&buf[i..=i]);
        i += 1;
    }
    result
}

/// Patch synchronizing literals up to 4096 bytes to non-synchronizing (LITERAL-).
///
/// Converts `{digits}\r\n` to `{digits+}\r\n` only when `digits` (the
/// literal octet count) is <= 4096.
/// Larger literals are left as synchronizing, per RFC 7888 Section 5.
///
/// Literal8 markers (`~{digits}\r\n`, RFC 3516) are only converted when the
/// caller indicates that both BINARY and the relevant literal extension are
/// active (RFC 7888 Section 6).
///
/// Length-aware: after patching (or skipping) a marker, skips the literal
/// body so that `{digits}\r\n` patterns inside literal data are not modified
/// (RFC 3501 Section 4.3).
fn patch_small_literals_to_plus_with_binary(buf: &[u8], allow_literal8: bool) -> BytesMut {
    /// RFC 7888 Section 5: LITERAL- limit.
    const LITERAL_MINUS_MAX: usize = 4096;

    let mut result = BytesMut::with_capacity(buf.len() + 16);
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'{' {
            let start = i + 1;
            let mut j = start;
            while j < buf.len() && buf[j].is_ascii_digit() {
                j += 1;
            }
            if j > start
                && j + 2 < buf.len()
                && buf[j] == b'}'
                && buf[j + 1] == b'\r'
                && buf[j + 2] == b'\n'
            {
                // Parse the literal size to decide whether to patch and to skip the body.
                // If the digit sequence is not valid UTF-8 or overflows usize,
                // treat the `{` as plain text  -  do not patch it (RFC 3501 Section 4.3).
                let Ok(size_str) = std::str::from_utf8(&buf[start..j]) else {
                    result.extend_from_slice(&buf[i..=i]);
                    i += 1;
                    continue;
                };
                let Ok(size) = size_str.parse::<usize>() else {
                    result.extend_from_slice(&buf[i..=i]);
                    i += 1;
                    continue;
                };

                // RFC 7888 Section 6 / RFC 3516: literal8 markers (`~{N}\r\n`)
                // may only use the non-synchronizing form when BINARY is also
                // advertised alongside the literal extension.
                let is_literal8 = i > 0 && buf[i - 1] == b'~';

                // Copy `{digits`
                result.extend_from_slice(&buf[i..j]);
                if size <= LITERAL_MINUS_MAX && (!is_literal8 || allow_literal8) {
                    // RFC 7888 Section 5: small literal, upgrade to non-synchronizing.
                    result.extend_from_slice(b"+}\r\n");
                } else {
                    // Large literal or literal8: leave as synchronizing.
                    result.extend_from_slice(b"}\r\n");
                }
                let body_start = j + 3;
                // Copy the literal body verbatim (RFC 3501 Section 4.3).
                // Use checked arithmetic to prevent overflow when `size`
                // is near `usize::MAX` (e.g., from a crafted command).
                let body_end = body_start
                    .checked_add(size)
                    .map_or(buf.len(), |end| end.min(buf.len()));
                result.extend_from_slice(&buf[body_start..body_end]);
                i = body_end;
                continue;
            }
        }
        result.extend_from_slice(&buf[i..=i]);
        i += 1;
    }
    result
}

/// Filter flags not valid in STORE flag lists (RFC 3501 Section 2.3.2).
///
/// `\Recent` is server-only and `\*` (wildcard) is not valid in STORE.
fn filter_store_flags(flags: &[Flag]) -> Vec<Flag> {
    flags
        .iter()
        .filter(|f| !matches!(f, Flag::Recent | Flag::Wildcard))
        .cloned()
        .collect()
}

/// Expand a slice of [`UidRange`] into individual UIDs.
///
/// Used for backward-compatible conversion of ESEARCH ALL uid-set results
/// into a flat `Vec<u32>` matching the legacy SEARCH response format.
///
/// Expansion is capped at [`MAX_EXPANDED_UIDS`] to prevent OOM when a
/// server returns extremely large ranges (e.g. `1:4294967295`).
///
/// Ranges whose end is `u32::MAX` (the sentinel for `*` in sequence-sets,
/// per RFC 4731 Section 3.1) are NOT expanded because `*` means "the
/// highest UID in the mailbox"  -  a value unknown from the ESEARCH response
/// alone.  Only the `start` UID is emitted and `truncated` is set to `true`.
fn expand_uid_ranges(ranges: &[UidRange]) -> (Vec<u32>, bool) {
    /// Safety cap to prevent OOM from malicious or buggy server responses.
    const MAX_EXPANDED_UIDS: usize = 1_000_000;

    /// Sentinel value the parser uses for `*` in sequence-sets
    /// (RFC 4731 Section 3.1 / RFC 3501 Section 9).
    const STAR_SENTINEL: u32 = u32::MAX;

    let mut uids = Vec::new();
    let mut truncated = false;
    for range in ranges {
        if let Some(end) = range.end {
            // RFC 4731 Section 3.1: `*` in a uid-set means "the highest UID
            // in the mailbox."  The parser maps `*` to u32::MAX as a sentinel.
            // Since the actual highest UID is unknown from the ESEARCH response
            // alone, we cannot expand this range.  Emit only the start UID and
            // signal truncation so callers know the result is incomplete.
            if end == STAR_SENTINEL {
                uids.push(range.start);
                truncated = true;
                continue;
            }
            let count = (end.saturating_sub(range.start).saturating_add(1)) as usize;
            if uids.len().saturating_add(count) > MAX_EXPANDED_UIDS {
                warn!(
                    start = range.start,
                    end = end,
                    "UID range too large to expand ({count} UIDs), \
                     truncating to {MAX_EXPANDED_UIDS} total"
                );
                let remaining = MAX_EXPANDED_UIDS.saturating_sub(uids.len());
                if remaining > 0 {
                    // remaining > 0 is guaranteed by the guard above.
                    // Use saturating_add + min(end) to avoid u32 overflow
                    // when range.start is near u32::MAX (RFC 3501 Section 9:
                    // nz-number can be up to 4294967295).
                    #[allow(clippy::cast_possible_truncation)]
                    let last = end.min(range.start.saturating_add((remaining - 1) as u32));
                    for uid in range.start..=last {
                        uids.push(uid);
                    }
                }
                // RFC 4731 Section 3 / RFC 3501 Section 6.4.4: signal that
                // the expanded result does not faithfully represent the full
                // server response.
                truncated = true;
                break;
            }
            for uid in range.start..=end {
                uids.push(uid);
            }
        } else {
            uids.push(range.start);
        }
    }
    (uids, truncated)
}

/// Build a `SelectedMailbox` from collected untagged and tagged responses.
///
/// Extracts EXISTS, RECENT, FLAGS, UIDVALIDITY, UIDNEXT, PERMANENTFLAGS,
/// HIGHESTMODSEQ, and UIDNOTSTICKY from the responses per RFC 3501 Section 7.
fn build_selected_mailbox(
    untagged: &[UntaggedResponse],
    tagged: &TaggedResponse,
    read_only: bool,
) -> SelectedMailbox {
    let mut exists = 0;
    let mut recent = 0;
    let mut uid_validity: Option<u32> = None;
    let mut uid_next = Option::None;
    let mut flags = Vec::new();
    let mut permanent_flags = Vec::new();
    let mut highest_mod_seq = Option::None;
    let mut no_mod_seq = false;
    let mut unseen: Option<u32> = None;
    // RFC 8474 Section 5.1: unique mailbox identifier from [MAILBOXID (<id>)].
    let mut mailbox_id: Option<String> = None;
    // RFC 4315 Section 2 / RFC 9051 Section 7.1: [UIDNOTSTICKY]  -  UIDs are not persistent.
    let mut uid_not_sticky = false;
    // RFC 7162 Section 3.2.5.2: QRESYNC SELECT responses include
    // VANISHED (EARLIER) with UIDs removed since last sync, and
    // FETCH responses with changed flags.
    let mut vanished = Vec::new();
    let mut changed_messages = Vec::new();

    let effective_responses = selected_mailbox_effective_responses(untagged);

    for resp in effective_responses {
        match resp {
            UntaggedResponse::Exists(n) => exists = *n,
            UntaggedResponse::Recent(n) => recent = *n,
            UntaggedResponse::Flags(f) => flags.clone_from(f),
            UntaggedResponse::Status {
                code: Some(code), ..
            } => {
                extract_selected_code(
                    code,
                    &mut uid_validity,
                    &mut uid_next,
                    &mut permanent_flags,
                    &mut highest_mod_seq,
                    &mut no_mod_seq,
                    &mut unseen,
                    &mut mailbox_id,
                    &mut uid_not_sticky,
                );
            }
            // RFC 7162 Section 3.2.5.2: capture VANISHED (EARLIER) responses
            // sent during QRESYNC SELECT/EXAMINE. Only `earlier: true` responses
            // belong to the initial sync; `earlier: false` are unsolicited and
            // should not be included in the SelectedMailbox.
            UntaggedResponse::Vanished {
                earlier: true,
                uids,
            } => {
                vanished.extend_from_slice(uids);
            }
            // RFC 7162 Section 3.2.5.2: capture FETCH responses with
            // updated flags sent during QRESYNC SELECT/EXAMINE.
            UntaggedResponse::Fetch(fetch) => {
                changed_messages.push((**fetch).clone());
            }
            _ => {}
        }
    }

    // Also check the tagged response code
    if let Some(code) = &tagged.code {
        extract_selected_code(
            code,
            &mut uid_validity,
            &mut uid_next,
            &mut permanent_flags,
            &mut highest_mod_seq,
            &mut no_mod_seq,
            &mut unseen,
            &mut mailbox_id,
            &mut uid_not_sticky,
        );
    }

    SelectedMailbox {
        exists,
        recent,
        uid_validity,
        uid_next,
        flags,
        permanent_flags,
        highest_mod_seq,
        no_mod_seq,
        unseen,
        mailbox_id,
        read_only,
        uid_not_sticky,
        vanished,
        changed_messages,
    }
}

/// Restrict SELECT/EXAMINE processing to responses belonging to the newly
/// selected mailbox.
///
/// RFC 7162 Section 3.2.11: when switching mailboxes, `* OK [CLOSED]`
/// separates responses for the previously selected mailbox from the new one.
fn selected_mailbox_effective_responses(untagged: &[UntaggedResponse]) -> &[UntaggedResponse] {
    match untagged.iter().rposition(|r| {
        matches!(
            r,
            UntaggedResponse::Status {
                code: Some(ResponseCode::Closed),
                ..
            }
        )
    }) {
        Some(closed_idx) => &untagged[closed_idx + 1..],
        None => untagged,
    }
}

/// Extract mailbox metadata from a response code (RFC 3501 Section 7.1).
#[allow(clippy::too_many_arguments)]
fn extract_selected_code(
    code: &ResponseCode,
    uid_validity: &mut Option<u32>,
    uid_next: &mut Option<u32>,
    permanent_flags: &mut Vec<Flag>,
    highest_mod_seq: &mut Option<u64>,
    no_mod_seq: &mut bool,
    unseen: &mut Option<u32>,
    mailbox_id: &mut Option<String>,
    uid_not_sticky: &mut bool,
) {
    match code {
        ResponseCode::UidValidity(v) => *uid_validity = Some(*v),
        ResponseCode::UidNext(v) => *uid_next = Some(*v),
        ResponseCode::PermanentFlags(f) => permanent_flags.clone_from(f),
        ResponseCode::HighestModSeq(v) => {
            // RFC 7162 Section 3.1.2.1: mod-sequence-value >= 1.
            // HIGHESTMODSEQ 0 is semantically invalid  -  the server should
            // have sent [NOMODSEQ] instead. Treat it equivalently per
            // Postel's law (RFC 1122 Section 1.2.2).
            if *v == 0 {
                *no_mod_seq = true;
            } else {
                *highest_mod_seq = Some(*v);
            }
        }
        // RFC 7162 Section 3.1.2: [NOMODSEQ]  -  mailbox does not support
        // mod-sequences. Distinct from the server simply not sending
        // HIGHESTMODSEQ.
        ResponseCode::NoModSeq => *no_mod_seq = true,
        // RFC 3501 Section 7.1: [UNSEEN n]  -  first unseen message sequence number.
        // Dropped in IMAP4rev2 (RFC 9051), but commonly sent by rev1 servers.
        ResponseCode::Unseen(v) => *unseen = Some(*v),
        // RFC 8474 Section 5.1: [MAILBOXID (<id>)]  -  unique mailbox identifier.
        ResponseCode::MailboxId(id) => *mailbox_id = Some(id.clone()),
        // RFC 4315 Section 2 / RFC 9051 Section 7.1: [UIDNOTSTICKY]  -
        // UIDs assigned to messages in this mailbox are not persistent.
        ResponseCode::UidNotSticky => *uid_not_sticky = true,
        _ => {}
    }
}

/// Check whether a LIST response's attributes conflict with the selection
/// options of a LIST-EXTENDED command (RFC 5258 Section 3).
///
/// When NOTIFY is active, a concurrent create event may match the wildcard
/// but lack the attributes required by the selection options (SUBSCRIBED,
/// REMOTE, SPECIAL-USE). Returns `true` if the response does NOT satisfy
/// the selection criteria and should be treated as a NOTIFY event.
pub(super) fn is_notify_selection_mismatch(info: &MailboxInfo, selection_options: &[&str]) -> bool {
    let has_recursivematch = selection_options
        .iter()
        .any(|o| o.eq_ignore_ascii_case("RECURSIVEMATCH"));

    for opt in selection_options {
        if opt.eq_ignore_ascii_case("SUBSCRIBED") {
            let has_subscribed = info
                .attributes
                .iter()
                .any(|a| matches!(a, MailboxAttribute::Subscribed));
            // RFC 5258 Section 3.5: with RECURSIVEMATCH, a mailbox can be
            // returned without \Subscribed if it has children with matching
            // subscriptions (indicated by non-empty CHILDINFO extended data).
            let has_childinfo = has_recursivematch && !info.child_info.is_empty();
            if !has_subscribed && !has_childinfo {
                return true;
            }
        }
        if opt.eq_ignore_ascii_case("REMOTE")
            && !info
                .attributes
                .iter()
                .any(|a| matches!(a, MailboxAttribute::Remote))
        {
            return true;
        }
        // RFC 6154 Section 3: SPECIAL-USE requests only mailboxes with
        // a special-use attribute.
        if opt.eq_ignore_ascii_case("SPECIAL-USE")
            && !info.attributes.iter().any(MailboxAttribute::is_special_use)
        {
            return true;
        }
    }
    false
}

/// Find the index of the first `[NOTIFICATIONOVERFLOW]` response code in a
/// stream of untagged responses, or `responses.len()` if there is none.
///
/// Used by LIST/LIST-EXTENDED/LIST-STATUS handlers to classify each
/// Collect solicited FETCH responses from untagged data
/// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
/// Check whether a LIST response carries markers that identify it as a
/// NOTIFY event rather than a solicited LIST result (RFC 5465 Section 5.4).
///
/// NOTIFY delivers `MailboxName` events (rename, delete, ACL change,
/// subscription change) as LIST responses. Reliable markers:
///
/// - **OLDNAME** (RFC 9051 Section 6.3.9.7): present on rename events.
///   A solicited LIST lists current mailbox state and never includes OLDNAME.
///   Always treated as a NOTIFY marker.
/// - **`\NonExistent`** (RFC 9051 Section 7.2.2): present on delete events.
///   A solicited plain LIST only returns existing mailboxes so `\NonExistent`
///   is a reliable NOTIFY marker in that context. However, LIST-EXTENDED with
///   SUBSCRIBED can legitimately return subscribed-but-deleted mailboxes with
///   `\NonExistent` (RFC 5258 Section 3).
/// - **`\NoAccess`** (RFC 5465 Section 5.9): present when the logged-in
///   user loses the `l` ACL right on a monitored mailbox. A solicited plain
///   LIST typically does not return inaccessible mailboxes. However,
///   LIST-EXTENDED with SUBSCRIBED may return subscribed-but-inaccessible
///   mailboxes with `\NoAccess`.
///
/// Callers must set `filter_extended_markers` to `false` when
/// `\NonExistent` / `\NoAccess` can legitimately appear on solicited
/// results (i.e., LIST-EXTENDED with SUBSCRIBED).
///
/// Create events carry no distinguishing marker and are not filtered.
pub(super) fn is_notify_list_event(info: &MailboxInfo, filter_extended_markers: bool) -> bool {
    // Rename event: OLDNAME extended data item  -  always a NOTIFY marker.
    if info.old_name.is_some() {
        return true;
    }
    // Delete (\NonExistent) and ACL-loss (\NoAccess) events. Only
    // treated as NOTIFY markers when the caller confirms that these
    // attributes cannot appear on solicited results (i.e., plain LIST
    // or LIST-EXTENDED without SUBSCRIBED).
    if filter_extended_markers
        && info.attributes.iter().any(|a| {
            matches!(
                a,
                MailboxAttribute::NonExistent | MailboxAttribute::NoAccess
            )
        })
    {
        return true;
    }
    false
}
