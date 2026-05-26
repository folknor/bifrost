use bytes::BytesMut;
use flate2::{Compress, Decompress, FlushCompress, FlushDecompress, Status};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;

use crate::error::Error;

use super::TcpKeepalive;

/// The inner transport stream: either plain TCP or TLS over TCP.
///
/// Used as the underlying I/O transport for both uncompressed and compressed
/// connections. The `Tls` variant is large due to TLS session state, same
/// rationale as `ImapStream` for not boxing.
#[allow(clippy::large_enum_variant)]
pub(super) enum InnerStream {
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
/// Per RFC 4978 Section 3, uses raw deflate (RFC 1951): not gzip, not zlib,
/// with `SyncFlush` after each write to ensure the peer can decode immediately.
pub(super) struct CompressedStream {
    /// The underlying TCP or TLS stream.
    pub(super) inner: InnerStream,
    /// Decompressor for inflating data received from the server.
    decompress: Decompress,
    /// Compressor for deflating data sent to the server.
    compress: Compress,
    /// Buffer holding raw (compressed) bytes read from the network that have
    /// not yet been inflated.
    pub(super) raw_read_buf: BytesMut,
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
    pub(super) fn new(inner: InnerStream) -> Self {
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
/// Delegates `AsyncRead`/`AsyncWrite` via match: no `unsafe` code.
/// The `Tls` variant is large due to the TLS session state. Boxing it
/// would add indirection on every I/O call, which is not worth it.
#[allow(clippy::large_enum_variant)]
pub(super) enum ImapStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
    /// Compressed stream per RFC 4978 (COMPRESS=DEFLATE).
    Compressed(CompressedStream),
    /// Sentinel used during in-progress stream upgrades (STARTTLS,
    /// COMPRESS). All I/O operations return an error immediately.
    /// If the upgrade fails or the future is cancelled, the stream
    /// stays `Poisoned` forever and the connection is dead: this is
    /// the enforcement of I9 (atomic upgrades).
    /// RFC 3501 Section6.2.1 / RFC 4978.
    Poisoned,
    /// In-memory transport used by the test harness. Backed by
    /// [`tokio::io::DuplexStream`] so unit tests do not need to bind a real
    /// loopback socket. Restricted environments (sandboxes, hardened CI
    /// runners) can disallow `TcpListener::bind("127.0.0.1:0")`. Gated to
    /// `cfg(test)` so production builds carry zero overhead.
    #[cfg(test)]
    Memory(tokio::io::DuplexStream),
}

impl ImapStream {
    pub(super) async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read_buf(buf).await,
            Self::Tls(s) => s.read_buf(buf).await,
            Self::Compressed(s) => s.read_buf(buf).await,
            Self::Poisoned => Err(std::io::Error::other("stream poisoned during upgrade")),
            #[cfg(test)]
            Self::Memory(s) => s.read_buf(buf).await,
        }
    }

    pub(super) async fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.write_all(data).await,
            Self::Tls(s) => s.write_all(data).await,
            Self::Compressed(s) => s.write_all(data).await,
            Self::Poisoned => Err(std::io::Error::other("stream poisoned during upgrade")),
            #[cfg(test)]
            Self::Memory(s) => s.write_all(data).await,
        }
    }

    pub(super) async fn flush(&mut self) -> std::io::Result<()> {
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
    pub(super) fn set_keepalive(&self, ka: &TcpKeepalive) -> Result<(), Error> {
        use socket2::SockRef;

        let sock_ka = socket2::TcpKeepalive::new()
            .with_time(ka.time)
            .with_interval(ka.interval);

        let result = match self {
            Self::Plain(tcp) => SockRef::from(tcp).set_tcp_keepalive(&sock_ka),
            Self::Tls(tls) => {
                SockRef::from(tls.get_ref().get_ref().get_ref()).set_tcp_keepalive(&sock_ka)
            }
            Self::Compressed(c) => match &c.inner {
                InnerStream::Plain(tcp) => SockRef::from(tcp).set_tcp_keepalive(&sock_ka),
                InnerStream::Tls(tls) => {
                    SockRef::from(tls.get_ref().get_ref().get_ref()).set_tcp_keepalive(&sock_ka)
                }
            },
            Self::Poisoned => {
                return Err(Error::Io {
                    source: std::sync::Arc::new(std::io::Error::other(
                        "cannot set keepalive: stream is in upgrade transition",
                    )),
                    attempt: None,
                });
            }
            #[cfg(test)]
            Self::Memory(_) => {
                return Err(Error::Io {
                    source: std::sync::Arc::new(std::io::Error::other(
                        "keepalive not supported on memory streams",
                    )),
                    attempt: None,
                });
            }
        };
        result.map_err(|e| Error::Io {
            source: std::sync::Arc::new(e),
            attempt: None,
        })
    }

    /// Extract the underlying `TcpStream` for STARTTLS upgrade.
    pub(super) fn into_tcp(self) -> Option<TcpStream> {
        match self {
            Self::Plain(s) => Some(s),
            Self::Tls(_) | Self::Compressed(_) | Self::Poisoned => Option::None,
            #[cfg(test)]
            // Memory streams cannot upgrade to TLS: STARTTLS tests must
            // use the real transport.
            Self::Memory(_) => Option::None,
        }
    }
}
