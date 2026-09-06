//! In-memory transcript harness. Everything the drivers, transports and pool
//! are tested against is scripted here: there are deliberately no listeners,
//! no threads and no sleeps in this module, so a test can never fail because
//! of a port, a socket path or the scheduler.

use std::{collections::VecDeque, time::Duration};

#[derive(Clone, Debug)]
pub(super) struct Transcript {
    shared: std::sync::Arc<std::sync::Mutex<TranscriptState>>,
}

#[derive(Debug)]
struct TranscriptState {
    pending_server_bytes: VecDeque<u8>,
    steps: VecDeque<TranscriptStep>,
    /// The peer accepted the command and then went silent forever. Reads park
    /// instead of reporting EOF, which is what a timeout or a cancellation
    /// test needs to observe.
    stalled: bool,
    /// The pending server bytes arrived as one TCP segment, so a single read
    /// hands out all of them rather than one reply line.
    coalesced: bool,
    /// The peer closed the connection once the scripted bytes were handed out.
    /// Reads then report EOF and writes fail the way a real closed socket
    /// does, so a driver cannot keep talking to a hung-up peer.
    closed: bool,
    shutdown_stalled: bool,
    /// Every read timeout the blocking driver armed on this stream, in order.
    /// The transcript never enforces them - it has no clock - but recording
    /// them is how the per-reply deadline (as opposed to a per-line one) is
    /// observable hermetically.
    read_timeouts: Vec<Option<Duration>>,
}

#[derive(Debug)]
struct TranscriptStep {
    client: Vec<u8>,
    server: Vec<u8>,
    stall: bool,
    coalesced: bool,
    close: bool,
}

impl Transcript {
    pub(super) fn new(greeting: impl AsRef<[u8]>) -> Self {
        Self::with_state(greeting.as_ref().iter().copied().collect(), false)
    }

    /// A peer that accepts the connection and never sends its banner.
    #[cfg(feature = "tokio")]
    pub(super) fn silent() -> Self {
        Self::with_state(VecDeque::new(), true)
    }

    fn with_state(pending_server_bytes: VecDeque<u8>, stalled: bool) -> Self {
        Self {
            shared: std::sync::Arc::new(std::sync::Mutex::new(TranscriptState {
                pending_server_bytes,
                steps: VecDeque::new(),
                stalled,
                coalesced: false,
                closed: false,
                shutdown_stalled: false,
                read_timeouts: Vec::new(),
            })),
        }
    }

    pub(super) fn expect(self, client: impl AsRef<[u8]>, server: impl AsRef<[u8]>) -> Self {
        self.push(client, server, false, false, false)
    }

    #[cfg(feature = "tokio")]
    pub(super) fn stall_shutdown(self) -> Self {
        self.shared
            .lock()
            .expect("transcript lock")
            .shutdown_stalled = true;
        self
    }

    /// A peer that answers with `server` and then hangs up.
    ///
    /// `server` may be a half-written reply line (or empty): the scripted
    /// bytes are handed out, then reads report EOF and any further client
    /// write fails the way a write to a closed socket does. That is the
    /// difference from `expect_then_stall`, where the peer stays
    /// connected and simply never answers.
    pub(super) fn expect_then_close(
        self,
        client: impl AsRef<[u8]>,
        server: impl AsRef<[u8]>,
    ) -> Self {
        self.push(client, server, false, false, true)
    }

    /// A step whose reply bytes arrive as one segment, the way a real peer's
    /// TCP stack coalesces adjacent replies. A `BufReader` over the stream
    /// therefore prefetches all of them, which is how the driver can observe
    /// bytes it did not ask for without a blocking read.
    pub(super) fn expect_coalesced(
        self,
        client: impl AsRef<[u8]>,
        server: impl AsRef<[u8]>,
    ) -> Self {
        self.push(client, server, false, true, false)
    }

    /// Accept the client bytes, then never answer.
    #[cfg(feature = "tokio")]
    pub(super) fn expect_then_stall(self, client: impl AsRef<[u8]>) -> Self {
        self.push(client, b"", true, false, false)
    }

    fn push(
        self,
        client: impl AsRef<[u8]>,
        server: impl AsRef<[u8]>,
        stall: bool,
        coalesced: bool,
        close: bool,
    ) -> Self {
        self.shared
            .lock()
            .expect("transcript lock")
            .steps
            .push_back(TranscriptStep {
                client: client.as_ref().to_vec(),
                server: server.as_ref().to_vec(),
                stall,
                coalesced,
                close,
            });
        self
    }

    /// Every read timeout armed on this transcript's stream since the last
    /// call, in order, clearing the record.
    pub(super) fn take_read_timeouts(&self) -> Vec<Option<Duration>> {
        std::mem::take(&mut self.shared.lock().expect("transcript lock").read_timeouts)
    }

    pub(super) fn stream(&self) -> TranscriptStream {
        TranscriptStream {
            transcript: self.clone(),
        }
    }

    pub(super) fn assert_exhausted(&self) {
        let state = self.shared.lock().expect("transcript lock");
        assert!(
            state.steps.is_empty(),
            "SMTP transcript has {} unconsumed client step(s)",
            state.steps.len()
        );
        assert!(
            state.pending_server_bytes.is_empty(),
            "SMTP transcript has {} unconsumed server byte(s)",
            state.pending_server_bytes.len()
        );
    }
}

/// A synchronous side of an in-process SMTP transcript.
///
/// Each client write exactly matches one scripted step. Its paired server
/// bytes become readable only after that write, so transcripts preserve the
/// request-response sequence a real peer can produce.
#[derive(Clone, Debug)]
pub(super) struct TranscriptStream {
    transcript: Transcript,
}

impl TranscriptStream {
    /// Records the timeout the driver armed. The transcript has no clock, so
    /// nothing is enforced; the record is the observable.
    pub(super) fn record_read_timeout(&self, duration: Option<Duration>) {
        self.transcript
            .shared
            .lock()
            .expect("transcript lock")
            .read_timeouts
            .push(duration);
    }
}

impl std::io::Read for TranscriptStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut state = self
            .transcript
            .shared
            .lock()
            .map_err(|_| std::io::Error::other("SMTP transcript lock poisoned"))?;
        if state.pending_server_bytes.is_empty() {
            if state.closed {
                // A closed peer reports EOF, never a stall: the driver must
                // classify the truncated response instead of waiting.
                return Ok(0);
            }
            if state.stalled {
                return Err(std::io::ErrorKind::WouldBlock.into());
            }
            return Ok(0);
        }
        // Hand out at most one reply line per read. A `BufReader` over this
        // stream would otherwise prefetch a whole pipelined reply group, empty
        // `pending_server_bytes` after the first parsed response, and let the
        // driver write the next command without draining the rest - which is
        // exactly the sequencing bug these transcripts exist to catch.
        let line_end = if state.coalesced {
            state.pending_server_bytes.len()
        } else {
            state
                .pending_server_bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(state.pending_server_bytes.len(), |index| index + 1)
        };
        let count = buf.len().min(line_end);
        for destination in &mut buf[..count] {
            *destination = state
                .pending_server_bytes
                .pop_front()
                .ok_or_else(|| std::io::Error::other("SMTP transcript server bytes disappeared"))?;
        }
        Ok(count)
    }
}

impl std::io::Write for TranscriptStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut state = self
            .transcript
            .shared
            .lock()
            .map_err(|_| std::io::Error::other("SMTP transcript lock poisoned"))?;
        if state.closed {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        if !state.pending_server_bytes.is_empty() {
            return Err(std::io::Error::other(
                "SMTP client wrote before draining the scripted server replies",
            ));
        }
        let Some(step) = state.steps.pop_front() else {
            return Err(std::io::Error::other(
                "SMTP transcript exhausted by client write",
            ));
        };
        if step.client != buf {
            return Err(std::io::Error::other(format!(
                "unexpected SMTP client bytes: expected {:?}, got {:?}",
                String::from_utf8_lossy(&step.client),
                String::from_utf8_lossy(buf)
            )));
        }
        state.pending_server_bytes.extend(step.server);
        state.stalled = step.stall;
        state.coalesced = step.coalesced;
        state.closed = step.close;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A peer that trickles one reply line per `gap`, swallowing every write.
///
/// The in-memory stand-in for a pathological server that answers, just very
/// slowly. Each line is preceded by a real (paused-clock) sleep, so under
/// `start_paused` tokio time a reply of N lines costs N gaps of virtual time
/// with no wall-clock cost. That is what distinguishes a per-reply read
/// deadline from a per-line one: the per-line arm gives every line a fresh
/// full timeout and never fires, while one deadline across the reply does.
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub(super) struct SlowLinePeer {
    lines: VecDeque<Vec<u8>>,
    gap: Duration,
    next: std::pin::Pin<Box<tokio::time::Sleep>>,
}

#[cfg(feature = "tokio")]
impl SlowLinePeer {
    pub(super) fn new<I: IntoIterator<Item = &'static str>>(lines: I, gap: Duration) -> Self {
        Self {
            lines: lines
                .into_iter()
                .map(|line| line.as_bytes().to_vec())
                .collect(),
            gap,
            next: Box::pin(tokio::time::sleep(gap)),
        }
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncRead for SlowLinePeer {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::ready!(std::future::Future::poll(self.next.as_mut(), cx));
        let Some(line) = self.lines.pop_front() else {
            // Out of lines: park forever, the way a peer that simply stops
            // talking does. Only the caller's own deadline resumes this.
            return std::task::Poll::Pending;
        };
        let gap = self.gap;
        self.next = Box::pin(tokio::time::sleep(gap));
        buf.put_slice(&line);
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncWrite for SlowLinePeer {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// A peer that drains an upload slowly but steadily, or not at all.
///
/// `new(chunk, gap)` accepts `chunk` bytes every `gap`, which is a slow link:
/// the transfer takes `len / chunk` gaps of virtual time while every
/// individual write makes progress. `stalled(...)` never accepts a byte and
/// registers no waker, which is a wedged peer. Together they separate the two
/// things a write timeout has to tell apart - the async half must survive the
/// first and still fail the second, which is what `SO_SNDTIMEO` gives the
/// blocking half for free by being per `write(2)`.
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub(super) struct SlowSinkPeer {
    /// Bytes accepted per `gap`. `None` means the peer never accepts anything.
    chunk: Option<usize>,
    gap: Duration,
    next: std::pin::Pin<Box<tokio::time::Sleep>>,
}

#[cfg(feature = "tokio")]
impl SlowSinkPeer {
    pub(super) fn new(chunk: usize, gap: Duration) -> Self {
        Self {
            chunk: Some(chunk),
            gap,
            next: Box::pin(tokio::time::sleep(gap)),
        }
    }

    /// A peer that accepts nothing, ever.
    pub(super) fn stalled() -> Self {
        Self {
            chunk: None,
            gap: Duration::ZERO,
            next: Box::pin(tokio::time::sleep(Duration::ZERO)),
        }
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncRead for SlowSinkPeer {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Pending
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncWrite for SlowSinkPeer {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let Some(chunk) = self.chunk else {
            // No waker: only the caller's own timeout may resume this.
            return std::task::Poll::Pending;
        };
        std::task::ready!(std::future::Future::poll(self.next.as_mut(), cx));
        let gap = self.gap;
        self.next = Box::pin(tokio::time::sleep(gap));
        std::task::Poll::Ready(Ok(chunk.min(buf.len())))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// A peer that accepts every byte written to it and never answers.
///
/// The in-memory stand-in for "a listener that accepts the TCP connection and
/// then sleeps": it is what a setup-deadline test needs, because the handshake
/// bytes it swallows are opaque (a TLS `ClientHello` is not scriptable) and the
/// only thing that may resume the caller is the caller's own deadline. Reads
/// park with no waker registered, exactly as `AsyncTranscriptStream` does for
/// a stalled step, so nothing but a timeout or a cancellation can wake it.
#[cfg(feature = "tokio")]
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct StalledPeer;

#[cfg(feature = "tokio")]
impl tokio::io::AsyncRead for StalledPeer {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Pending
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncWrite for StalledPeer {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
#[derive(Clone, Debug)]
pub(super) struct AsyncTranscriptStream {
    inner: TranscriptStream,
}

#[cfg(feature = "tokio")]
impl AsyncTranscriptStream {
    pub(super) fn new(transcript: Transcript) -> Self {
        Self {
            inner: transcript.stream(),
        }
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncRead for AsyncTranscriptStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let unfilled = buf.initialize_unfilled();
        let read = match std::io::Read::read(&mut self.inner, unfilled) {
            Ok(read) => read,
            // A stalled peer parks the read. No waker is registered on
            // purpose: the only thing that may resume this task is the
            // caller's own timeout or cancellation, which is exactly the
            // behavior these transcripts pin.
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return std::task::Poll::Pending;
            }
            Err(error) => return std::task::Poll::Ready(Err(error)),
        };
        buf.advance(read);
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncWrite for AsyncTranscriptStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(std::io::Write::write(&mut self.inner, buf))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(std::io::Write::flush(&mut self.inner))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self
            .inner
            .transcript
            .shared
            .lock()
            .expect("transcript lock")
            .shutdown_stalled
        {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(Ok(()))
        }
    }
}
