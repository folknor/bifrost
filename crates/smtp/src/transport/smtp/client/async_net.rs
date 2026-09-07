#[cfg(unix)]
use std::path::Path;
use std::{
    future::Future,
    io::Result as IoResult,
    mem,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

// Tokio's clock, not `std`'s: every timeout this deadline arms is a
// `tokio::time::timeout`, so measuring the deadline on a different clock
// makes the two disagree wherever tokio's clock is not the system one -
// under `start_paused` test time most visibly, but the principle is that a
// deadline and the timers it hands out must read the same clock.
use tokio::time::Instant;

use std::fmt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
#[cfg(unix)]
use tokio::net::UnixStream as TokioUnixStream;
use tokio::net::{TcpSocket, TcpStream, ToSocketAddrs};
use tokio::time::Sleep;
use tokio_native_tls::TlsStream as TokioTlsStream;

use super::metering::WireMetering;
use super::net::resolved_address_filter;
use super::{ConnectionState, TlsParameters};
use crate::transport::smtp::{Error, error};

#[derive(Clone, Copy, Debug)]
pub(super) struct AsyncDeadline {
    expires_at: Option<Instant>,
}

impl AsyncDeadline {
    pub(super) fn new(timeout: Option<Duration>) -> Self {
        Self {
            expires_at: timeout.map(|timeout| Instant::now() + timeout),
        }
    }

    /// Returns `Ok(None)` for an unbounded operation, `Ok(Some(_))` while
    /// slack remains, and a timeout error once the shared deadline is spent.
    pub(super) fn remaining(self, message: &'static str) -> Result<Option<Duration>, Error> {
        let Some(expires_at) = self.expires_at else {
            return Ok(None);
        };

        expires_at
            .checked_duration_since(Instant::now())
            .ok_or_else(|| error::timeout(message))
            .map(Some)
    }

    async fn timeout<T, F>(self, message: &'static str, future: F) -> Result<T, Error>
    where
        F: Future<Output = T>,
    {
        match self.remaining(message)? {
            None => Ok(future.await),
            Some(timeout) => tokio::time::timeout(timeout, future)
                .await
                .map_err(|_| error::timeout(message)),
        }
    }
}

async fn resolve_lookup_until<I, F>(
    deadline: AsyncDeadline,
    local_addr: Option<IpAddr>,
    lookup: F,
) -> Result<Vec<SocketAddr>, Error>
where
    F: Future<Output = std::io::Result<I>>,
    I: IntoIterator<Item = SocketAddr>,
{
    let addrs = deadline
        .timeout("DNS lookup timed out", lookup)
        .await?
        .map_err(error::connection_io)?;

    Ok(addrs
        .into_iter()
        .filter(|resolved_addr| resolved_address_filter(resolved_addr, local_addr))
        .collect())
}

/// A network stream
#[derive(Debug)]
pub(crate) struct AsyncNetworkStream {
    inner: InnerAsyncNetworkStream,
    state: ConnectionState,
    /// Byte accounting and cap for this connection. `disabled()` unless
    /// an account wired one in, so the non-account path is unchanged.
    metering: WireMetering,
    /// Outstanding inbound throttle debt. Polled to completion before the
    /// next read touches the socket, and never consulted by a write.
    ///
    /// Held here rather than awaited inline because both the read and
    /// write funnels are `poll_` methods that cannot await. Parking the
    /// sleep in the stream means a throttled connection yields to the
    /// runtime like any other pending IO instead of blocking the task.
    ///
    /// Inbound and outbound debt are separate slots on purpose: the meter
    /// already keeps separate buckets, and a single shared slot silently
    /// undid that, because the debt parked by the last DATA-body write
    /// gated the read of the server's reply to it. That spends the caller's
    /// per-operation read timeout on outbound debt. See "Bandwidth metering
    /// and throttling" in `reference/smtp.md`.
    throttle_in: Option<Pin<Box<Sleep>>>,
    /// Outstanding outbound throttle debt. Polled to completion before the
    /// next write touches the socket, and never consulted by a read.
    throttle_out: Option<Pin<Box<Sleep>>>,
    /// Test-injected peer-certificate DER, returned by
    /// [`Self::peer_certificate_der`] ahead of the TLS session's. The
    /// transcript harness is an in-memory duplex with no TLS, so without this
    /// seam no hermetic test can make a certificate exist, and the
    /// connection-level channel-binding gate (`plus_candidate` in `auth`)
    /// would be untestable in both directions.
    #[cfg(test)]
    test_peer_certificate_der: Option<Vec<u8>>,
}

pub(crate) trait AsyncTokioStream:
    AsyncRead + AsyncWrite + Send + Sync + Unpin + fmt::Debug
{
}

impl AsyncTokioStream for TcpStream {}
#[cfg(unix)]
impl AsyncTokioStream for TokioUnixStream {}
#[cfg(test)]
impl AsyncTokioStream for crate::transport::smtp::test_support::AsyncTranscriptStream {}
#[cfg(test)]
impl AsyncTokioStream for crate::transport::smtp::test_support::StalledPeer {}
#[cfg(test)]
impl AsyncTokioStream for crate::transport::smtp::test_support::SlowLinePeer {}
#[cfg(test)]
impl AsyncTokioStream for crate::transport::smtp::test_support::SlowSinkPeer {}

/// Represents the different types of underlying network streams
// usually only one TLS backend at a time is going to be enabled,
// so clippy::large_enum_variant doesn't make sense here
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum InnerAsyncNetworkStream {
    /// Plain Tokio 1.x TCP stream
    TokioTcp(Box<dyn AsyncTokioStream>),
    /// Plain Tokio 1.x Unix-domain stream
    #[cfg(unix)]
    TokioUnix(Box<dyn AsyncTokioStream>),
    /// Encrypted Tokio 1.x TCP stream
    TokioNativeTls(TokioTlsStream<Box<dyn AsyncTokioStream>>),
    /// Test-only scripted in-process peer.
    #[cfg(test)]
    Transcript(Box<dyn AsyncTokioStream>),
    /// Can't be built
    None,
}

impl AsyncNetworkStream {
    fn new(inner: InnerAsyncNetworkStream) -> Self {
        if let InnerAsyncNetworkStream::None = inner {
            debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
        }

        AsyncNetworkStream {
            inner,
            state: ConnectionState::Ok,
            metering: WireMetering::disabled(),
            throttle_in: None,
            throttle_out: None,
            #[cfg(test)]
            test_peer_certificate_der: None,
        }
    }

    /// Install byte accounting / capping for this connection. Called by
    /// the transport when an account supplied a meter sink or cap.
    pub(super) fn set_metering(&mut self, metering: WireMetering) {
        self.metering = metering;
    }

    /// Wait out any throttle debt owed from the previous transfer in this
    /// direction. Debt in the other direction is not consulted.
    ///
    /// Returns `Pending` while the debt is outstanding, which parks the
    /// caller the same way a not-yet-readable socket would.
    fn poll_throttle(&mut self, cx: &mut Context<'_>, inbound: bool) -> Poll<()> {
        let slot = if inbound {
            &mut self.throttle_in
        } else {
            &mut self.throttle_out
        };
        if let Some(delay) = slot {
            std::task::ready!(delay.as_mut().poll(cx));
            *slot = None;
        }
        Poll::Ready(())
    }

    /// Await any outbound throttle debt parked by an earlier write.
    ///
    /// The debt is this crate's OWN doing - the cap the consumer asked for -
    /// and the caller's per-operation write timeout is a statement about the
    /// PEER. Left to `poll_write`'s own `poll_throttle`, the two are spent from
    /// one budget: the write parks up to a second of debt (the offer clamp
    /// bounds it there), the next write waits that debt out inside
    /// `with_timeout`, and any operation timeout at or below a second turns a
    /// perfectly healthy capped upload into "SMTP write timed out" with every
    /// recipient uncertain. Draining it here, before the timeout is armed,
    /// means the timeout is only ever spent on the socket - which is what the
    /// blocking half gets for free, since `NetworkStream::charge` sleeps the
    /// thread outside `SO_SNDTIMEO`.
    ///
    /// Unbounded on purpose: the CALLER decides what the drain may run
    /// outside. A per-operation write timeout may be left entirely (see
    /// above); an absolute setup deadline may not, since waiting past a
    /// ceiling is not the same as spending it. `write_stream_with_budget`
    /// makes that choice per budget variant.
    ///
    /// Cancel-safe: a dropped future leaves the `Sleep` parked in its slot, so
    /// the debt is neither lost nor double-counted. That is also what makes it
    /// safe to bound with a `tokio::time::timeout`.
    pub(super) async fn drain_outbound_throttle(&mut self) {
        std::future::poll_fn(|cx| self.poll_throttle(cx, false)).await;
    }

    /// Record `n` transferred bytes and park the resulting debt, if any.
    fn charge(&mut self, n: usize, inbound: bool) {
        if n == 0 || !self.metering.is_enabled() {
            return;
        }
        let (debt, slot) = if inbound {
            (self.metering.record_in(n), &mut self.throttle_in)
        } else {
            (self.metering.record_out(n), &mut self.throttle_out)
        };
        if let Some(debt) = debt {
            *slot = Some(Box::pin(tokio::time::sleep(debt)));
        }
    }

    /// Inject a peer-certificate DER for tests. See the field's comment.
    #[cfg(test)]
    pub(crate) fn set_test_peer_certificate_der(&mut self, der: Vec<u8>) {
        self.test_peer_certificate_der = Some(der);
    }

    #[cfg(test)]
    pub(crate) fn from_transcript(
        transcript: crate::transport::smtp::test_support::Transcript,
    ) -> Self {
        Self::new(InnerAsyncNetworkStream::Transcript(Box::new(
            crate::transport::smtp::test_support::AsyncTranscriptStream::new(transcript),
        )))
    }

    /// Wrap an arbitrary scripted stream. For peers a `Transcript` cannot
    /// express; see `AsyncSmtpConnection::from_raw_stream_for_test`.
    #[cfg(test)]
    pub(crate) fn from_raw_stream_for_test(stream: Box<dyn AsyncTokioStream>) -> Self {
        Self::new(InnerAsyncNetworkStream::Transcript(stream))
    }

    pub(super) fn state(&self) -> ConnectionState {
        self.state
    }

    pub(super) fn set_state(&mut self, state: ConnectionState) {
        self.state = state;
    }

    /// Accepted coverage gap: this dial path has no hermetic test, for the
    /// same reason as its blocking twin - reaching it needs a peer on a real
    /// socket, which the crate's testing rules put out of scope, and the
    /// in-process `Transcript` harness enters below the dial. The parts that
    /// can be separated from the socket are pinned without one: the setup
    /// deadline is driven against `StalledPeer` under paused tokio time,
    /// including the TLS handshake leg through `upgrade_tls_stream`, and the
    /// STARTTLS exchange is pinned up to the handshake boundary. A real TLS
    /// handshake stays out of scope; closing that would need a production seam
    /// substituting the connector, not another test.
    pub(super) async fn connect_until<T: ToSocketAddrs>(
        server: T,
        deadline: AsyncDeadline,
        tls_parameters: Option<TlsParameters>,
        local_addr: Option<IpAddr>,
    ) -> Result<AsyncNetworkStream, Error> {
        async fn try_connect<T: ToSocketAddrs>(
            server: T,
            deadline: AsyncDeadline,
            local_addr: Option<IpAddr>,
        ) -> Result<TcpStream, Error> {
            let addrs =
                resolve_lookup_until(deadline, local_addr, tokio::net::lookup_host(server)).await?;

            let mut last_err = None;

            for addr in addrs {
                let socket = match addr.ip() {
                    IpAddr::V4(_) => TcpSocket::new_v4(),
                    IpAddr::V6(_) => TcpSocket::new_v6(),
                }
                .map_err(error::connection_io)?;
                if let Some(local_addr) = local_addr {
                    socket
                        .bind(SocketAddr::new(local_addr, 0))
                        .map_err(error::connection_io)?;
                }

                let connect_future = socket.connect(addr);
                match deadline
                    .timeout("connection timed out", connect_future)
                    .await?
                {
                    Ok(stream) => return Ok(stream),
                    Err(err) => last_err = Some(err),
                }
            }

            Err(match last_err {
                Some(last_err) => error::connection_io(last_err),
                None => error::connection("could not resolve to any supported address"),
            })
        }

        let tcp_stream = try_connect(server, deadline, local_addr).await?;
        let mut stream =
            AsyncNetworkStream::new(InnerAsyncNetworkStream::TokioTcp(Box::new(tcp_stream)));
        if let Some(tls_parameters) = tls_parameters {
            stream.upgrade_tls_until(tls_parameters, deadline).await?;
        }
        Ok(stream)
    }

    #[cfg(unix)]
    pub(super) async fn connect_unix_until(
        path: &Path,
        deadline: AsyncDeadline,
    ) -> Result<AsyncNetworkStream, Error> {
        let stream = deadline
            .timeout(
                "Unix socket connection timed out",
                TokioUnixStream::connect(path),
            )
            .await?
            .map_err(error::connection_io)?;
        Ok(AsyncNetworkStream::new(InnerAsyncNetworkStream::TokioUnix(
            Box::new(stream),
        )))
    }

    pub(crate) async fn upgrade_tls(
        &mut self,
        tls_parameters: TlsParameters,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        self.upgrade_tls_until(tls_parameters, AsyncDeadline::new(timeout))
            .await
    }

    pub(super) async fn upgrade_tls_until(
        &mut self,
        tls_parameters: TlsParameters,
        deadline: AsyncDeadline,
    ) -> Result<(), Error> {
        self.state.verify()?;

        match &self.inner {
            InnerAsyncNetworkStream::TokioTcp(_) => {
                self.state = ConnectionState::Broken;

                // get owned TcpStream
                let tcp_stream = mem::replace(&mut self.inner, InnerAsyncNetworkStream::None);
                let InnerAsyncNetworkStream::TokioTcp(tcp_stream) = tcp_stream else {
                    unreachable!()
                };

                self.inner = Self::upgrade_tls_stream(tcp_stream, tls_parameters, deadline).await?;
                self.state = ConnectionState::Ok;
                Ok(())
            }
            _ => Err(error::invalid_input(
                "STARTTLS is only supported on TCP connections",
            )),
        }
    }

    #[allow(unused_variables)]
    async fn upgrade_tls_stream(
        tcp_stream: Box<dyn AsyncTokioStream>,
        tls_parameters: TlsParameters,
        deadline: AsyncDeadline,
    ) -> Result<InnerAsyncNetworkStream, Error> {
        let domain = tls_parameters.domain().to_owned();

        use tokio_native_tls::TlsConnector;

        let connector = TlsConnector::from(tls_parameters.connector);
        let handshake = connector.connect(&domain, tcp_stream);
        let stream = deadline
            .timeout("TLS handshake timed out", handshake)
            .await?;
        Ok(InnerAsyncNetworkStream::TokioNativeTls(
            stream.map_err(error::tls)?,
        ))
    }

    pub(crate) fn is_encrypted(&self) -> bool {
        match &self.inner {
            InnerAsyncNetworkStream::TokioTcp(_) => false,
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(_) => false,
            InnerAsyncNetworkStream::TokioNativeTls(_) => true,
            #[cfg(test)]
            InnerAsyncNetworkStream::Transcript(_) => false,
            InnerAsyncNetworkStream::None => false,
        }
    }

    /// DER of the peer (server) certificate when the connection is TLS.
    ///
    /// See `NetworkStream::peer_certificate_der` for the contract; this is
    /// the Tokio sibling reaching the same native-tls session through
    /// `tokio_native_tls::TlsStream::get_ref`.
    // Plumbing for SCRAM-PLUS channel binding; the first consumer is the
    // SASL layer, so there is no in-crate caller yet.
    #[allow(dead_code)]
    pub(crate) fn peer_certificate_der(&self) -> Option<Vec<u8>> {
        #[cfg(test)]
        if let Some(der) = &self.test_peer_certificate_der {
            return Some(der.clone());
        }
        match &self.inner {
            InnerAsyncNetworkStream::TokioNativeTls(s) => s
                .get_ref()
                .peer_certificate()
                .ok()
                .flatten()
                .and_then(|c| c.to_der().ok()),
            _ => None,
        }
    }
}

impl AsyncRead for AsyncNetworkStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let this = self.as_mut().get_mut();
        std::task::ready!(this.poll_throttle(cx, true));
        let before = buf.filled().len();
        let result = match &mut this.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_read(cx, buf),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(test)]
            InnerAsyncNetworkStream::Transcript(s) => Pin::new(s).poll_read(cx, buf),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(()))
            }
        };
        if matches!(result, Poll::Ready(Ok(()))) {
            this.charge(buf.filled().len().saturating_sub(before), true);
        }
        result
    }
}

impl AsyncWrite for AsyncNetworkStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        let this = self.as_mut().get_mut();
        std::task::ready!(this.poll_throttle(cx, false));
        // Offer the socket at most one second of the cap. A TCP socket with
        // an autotuned send buffer accepts megabytes in one call, and every
        // accepted byte is charged, so an unclamped offer parks many seconds
        // of debt in one go. The clamp bounds a single charge - and so a
        // single uninterruptible throttle wait - at about a second; it does
        // not clamp the debt a sub-cap write owes, so a low cap still holds.
        // Keeping that debt out of the caller's write timeout is
        // `drain_outbound_throttle`'s job, not this clamp's.
        let buf = match this.metering.write_chunk_limit() {
            Some(limit) if buf.len() > limit => &buf[..limit],
            _ => buf,
        };
        let result = match &mut this.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_write(cx, buf),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(test)]
            InnerAsyncNetworkStream::Transcript(s) => Pin::new(s).poll_write(cx, buf),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(0))
            }
        };
        // Charge what the socket ACCEPTED, not what was offered: a short
        // write means the rest has not crossed the wire yet and will be
        // charged on the retry.
        if let Poll::Ready(Ok(n)) = &result {
            this.charge(*n, false);
        }
        result
    }

    /// Deliberately does NOT drain outbound throttle debt.
    ///
    /// The bytes were charged to the shared bucket the moment the socket
    /// accepted them, and the parked `Sleep` only holds the next transfer on
    /// THIS stream until the bucket has refilled. Draining here would make a
    /// finished send wait for bytes it has already paid for, without slowing
    /// the rate at which they left.
    ///
    /// What that does NOT buy, stated because an earlier revision of this
    /// comment claimed it and was wrong: it does not make a dropped connection
    /// pay its debt somewhere else. `throttle_out` is per-stream while the
    /// bucket is shared, and `poll_write` consults the bucket only AFTER the
    /// socket has accepted bytes, so a FRESH connection's first write is
    /// ungated no matter what the bucket owes. A caller that writes one
    /// cap-sized chunk, drops the connection and dials again therefore never
    /// waits. That evasion predates the drain and is not closed by it; closing
    /// it needs shared ADMISSION - permission one connection holds against the
    /// balance that another cannot simultaneously obtain - which is a
    /// mechanism this crate does not have. Do not restate the stronger claim
    /// here without building it.
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match &mut self.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_flush(cx),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(test)]
            InnerAsyncNetworkStream::Transcript(s) => Pin::new(s).poll_flush(cx),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.state = ConnectionState::Closed;

        match &mut self.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_shutdown(cx),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(test)]
            InnerAsyncNetworkStream::Transcript(s) => Pin::new(s).poll_shutdown(cx),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[cfg(test)]
mod tokio_test {
    use std::{future::pending, time::Duration};

    use crate::transport::smtp::test_support::StalledPeer;

    use super::*;

    #[tokio::test(crate = "tokio")]
    async fn tokio_dns_lookup_uses_deadline() {
        let result = resolve_lookup_until(
            AsyncDeadline::new(Some(Duration::from_millis(25))),
            None,
            pending::<std::io::Result<Vec<SocketAddr>>>(),
        )
        .await;

        let error = result.unwrap_err();
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
    }

    /// Finding 6: the meter keeps separate inbound and outbound buckets, and
    /// the stream must keep the resulting debt separate too. A single shared
    /// sleep slot made the debt parked by the final DATA-body write gate the
    /// read of the server's reply to it, spending the caller's read timeout on
    /// outbound debt. The read below must complete on its first poll even
    /// though the preceding write is still in debt.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn outbound_throttle_debt_does_not_gate_the_next_read() {
        use std::sync::{Arc, atomic::AtomicU64};

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        use crate::transport::smtp::client::metering::WireMetering;
        use crate::transport::smtp::test_support::Transcript;

        // Six bytes per second, which is exactly one `PING` line. The bucket
        // starts with one second of tokens and a write is now clamped to that
        // same second (see `write_chunk_limit`), so the FIRST write can never
        // owe anything - it burns the budget, and the second is the one that
        // parks debt.
        let cap = Arc::new(AtomicU64::new(6));
        let transcript = Transcript::new("")
            .expect("PING\r\n", "")
            .expect("PING\r\n", "250 pong\r\n");
        let mut stream = AsyncNetworkStream::from_transcript(transcript);
        stream.set_metering(WireMetering::new(None, Some(cap)));

        stream.write_all(b"PING\r\n").await.unwrap();
        stream.write_all(b"PING\r\n").await.unwrap();
        assert!(
            stream.throttle_out.is_some(),
            "the write must have parked outbound debt for this test to mean anything"
        );
        assert!(
            stream.throttle_in.is_none(),
            "an outbound charge must not park inbound debt"
        );

        // Poll the read exactly once. A read gated on outbound debt returns
        // Pending here; a read gated only on inbound debt returns the reply.
        let mut buf = [0_u8; 10];
        let mut read = Box::pin(stream.read(&mut buf));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let polled = read.as_mut().poll(&mut cx);
        let Poll::Ready(Ok(n)) = polled else {
            panic!("the reply read was gated on outbound throttle debt: {polled:?}");
        };
        assert_eq!(&buf[..n], b"250 pong\r\n");
    }

    /// What outbound debt actually guarantees: it gates the NEXT write, and
    /// nothing more.
    ///
    /// A cap is a RATE, and the rate is enforced by the shared `ByteBucket` -
    /// the bytes are debited the moment the socket accepts them, and the
    /// bucket outlives this stream (every connection a transport dials clones
    /// the same one). The parked `Sleep` is not the accounting; it is only the
    /// mechanism that holds the next transfer until the bucket has refilled.
    /// So a send that ends in debt returns with that debt still parked, and
    /// `poll_flush` deliberately does not drain it: flushing pays nothing back
    /// - the bytes are already charged - and making a caller wait it out would
    /// add latency to a send that has finished without changing the rate the
    /// cap enforces.
    ///
    /// The scope of this test is one stream, and so is the scope of the
    /// guarantee. A caller that sends one message and drops the connection
    /// escapes the wait, and the charge it leaves in the shared bucket does
    /// NOT gate the next connection's first write - see `poll_flush` for why
    /// that is a real evasion rather than a wording problem. A pooled
    /// connection recycled with its debt still parked is the case this does
    /// cover: the next checkout writes through this same `throttle_out`.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn outbound_throttle_debt_gates_the_next_write() {
        use std::sync::{Arc, atomic::AtomicU64};

        use tokio::io::AsyncWriteExt;

        use crate::transport::smtp::client::metering::WireMetering;
        use crate::transport::smtp::test_support::SlowSinkPeer;

        let cap = Arc::new(AtomicU64::new(1000));
        let mut stream = AsyncNetworkStream::from_raw_stream_for_test(Box::new(SlowSinkPeer::new(
            usize::MAX,
            Duration::ZERO,
        )));
        stream.set_metering(WireMetering::new(None, Some(cap)));

        // Burn the initial second of tokens, then overspend by exactly one
        // more second: the bucket owes 1000 ms.
        let body = vec![b'x'; 1000];
        assert_eq!(stream.write(&body).await.unwrap(), 1000);
        assert!(stream.throttle_out.is_none(), "the first write is free");
        assert_eq!(stream.write(&body).await.unwrap(), 1000);
        assert!(
            stream.throttle_out.is_some(),
            "the second write must owe time, or this pins nothing"
        );

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut next = Box::pin(stream.write(b"x"));

        assert!(
            next.as_mut().poll(&mut cx).is_pending(),
            "outstanding outbound debt must hold the next write"
        );
        // Just short of the debt: still nothing may reach the socket. A
        // `Pending` here is proof no byte was accepted, since the funnel
        // consults the debt before it ever offers the slice.
        tokio::time::advance(Duration::from_millis(999)).await;
        assert!(
            next.as_mut().poll(&mut cx).is_pending(),
            "the wait must last the whole debt, not merely yield once"
        );
        // And through it: the bucket has refilled and the write proceeds.
        // Driven under a 1 ms timeout rather than a single poll, so the peer
        // is allowed its own wakeup without the assertion depending on how
        // many polls a zero-length delay takes.
        tokio::time::advance(Duration::from_millis(2)).await;
        let written = tokio::time::timeout(Duration::from_millis(1), next)
            .await
            .expect("past the debt the write must make progress")
            .expect("the peer accepts");
        assert_eq!(written, 1);
    }

    /// Under a cap, a single write must never park more than about a second
    /// of throttle debt.
    ///
    /// The write funnel charges what the socket ACCEPTED, and a real socket
    /// accepts megabytes in one call, so an unclamped offer of a whole body
    /// debits the bucket by megabytes and parks a sleep of many seconds. The
    /// wait is uninterruptible once parked, and the amount of traffic a retune
    /// arrives too late to govern is whatever one charge covered. Clamping what
    /// is offered to one second of the cap bounds both.
    ///
    /// What this does NOT pin is that the caller's write timeout is spent only
    /// on the peer. That is `drain_outbound_throttle`'s doing, pinned by
    /// `a_capped_upload_outlives_a_write_timeout_shorter_than_its_throttle_debt`
    /// in `async_connection`; a one-second bound on debt is not a guarantee
    /// against a timeout that may itself be shorter than a second.
    ///
    /// Paused time is honest here: `ByteBucket` refills on tokio's clock, the
    /// same one the parked `Sleep` and this assertion's `Instant::now()` read.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_capped_write_offers_at_most_one_second_of_budget() {
        use std::sync::{Arc, atomic::AtomicU64};

        use tokio::io::AsyncWriteExt;

        use crate::transport::smtp::client::metering::WireMetering;
        use crate::transport::smtp::test_support::SlowSinkPeer;

        // A sink that accepts everything offered, immediately.
        let cap = Arc::new(AtomicU64::new(100));
        let mut stream = AsyncNetworkStream::from_raw_stream_for_test(Box::new(SlowSinkPeer::new(
            usize::MAX,
            Duration::ZERO,
        )));
        stream.set_metering(WireMetering::new(None, Some(cap)));

        let body = vec![b'x'; 100_000];

        // No single write may park more than about a second, at any point.
        // (The first burns the bucket's initial second of tokens and parks
        // nothing; the second is the one that owes time.)
        let mut offset = 0;
        for _ in 0..2 {
            let written = stream.write(&body[offset..]).await.unwrap();
            assert_eq!(
                written, 100,
                "the offer must be clamped to one second of cap"
            );
            offset += written;
            let debt = stream
                .throttle_out
                .as_ref()
                .map_or(Duration::ZERO, |sleep| {
                    sleep
                        .deadline()
                        .saturating_duration_since(tokio::time::Instant::now())
                });
            assert!(
                debt <= Duration::from_secs(2),
                "parked debt must stay well inside a write timeout, got {debt:?}"
            );
        }
        assert!(
            stream.throttle_out.is_some(),
            "the second write must owe time, or this pins nothing"
        );
    }

    /// Finding 4b: this used to bind a `TcpListener` and park a thread in a
    /// 250ms `thread::sleep` so the handshake would outlive a 50ms deadline.
    /// `StalledPeer` is the in-memory equivalent - it swallows the opaque
    /// `ClientHello` and never answers - so the deadline is the only thing that
    /// can resolve the handshake, with no socket, no thread and no sleep.
    #[tokio::test(crate = "tokio")]
    async fn tokio_tls_handshake_uses_deadline() {
        let tls_parameters = TlsParameters::new("localhost".to_owned()).unwrap();
        let result = AsyncNetworkStream::upgrade_tls_stream(
            Box::new(StalledPeer),
            tls_parameters,
            AsyncDeadline::new(Some(Duration::from_millis(25))),
        )
        .await;

        let error = result.unwrap_err();
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
    }
}
