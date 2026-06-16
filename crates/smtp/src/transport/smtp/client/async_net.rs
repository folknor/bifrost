#[cfg(unix)]
use std::path::Path;
use std::{
    future::Future,
    io::Result as IoResult,
    mem,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use std::fmt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
#[cfg(unix)]
use tokio::net::UnixStream as TokioUnixStream;
use tokio::net::{TcpSocket, TcpStream, ToSocketAddrs};
use tokio_native_tls::TlsStream as TokioTlsStream;

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
}

pub(crate) trait AsyncTokioStream:
    AsyncRead + AsyncWrite + Send + Sync + Unpin + fmt::Debug
{
}

impl AsyncTokioStream for TcpStream {}
#[cfg(unix)]
impl AsyncTokioStream for TokioUnixStream {}

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
        }
    }

    pub(super) fn state(&self) -> ConnectionState {
        self.state
    }

    pub(super) fn set_state(&mut self, state: ConnectionState) {
        self.state = state;
    }

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
        match &mut self.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_read(cx, buf),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_read(cx, buf),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl AsyncWrite for AsyncNetworkStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        match &mut self.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_write(cx, buf),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_write(cx, buf),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(0))
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match &mut self.inner {
            InnerAsyncNetworkStream::TokioTcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            InnerAsyncNetworkStream::TokioUnix(s) => Pin::new(s).poll_flush(cx),
            InnerAsyncNetworkStream::TokioNativeTls(s) => Pin::new(s).poll_flush(cx),
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
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[cfg(test)]
mod tokio_test {
    use std::{future::pending, net::TcpListener, thread, time::Duration};

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

    #[tokio::test(crate = "tokio")]
    async fn tokio_tls_handshake_uses_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(250));
        });

        let tls_parameters = TlsParameters::new("localhost".to_owned()).unwrap();
        let result = AsyncNetworkStream::connect_until(
            address,
            AsyncDeadline::new(Some(Duration::from_millis(50))),
            Some(tls_parameters),
            None,
        )
        .await;

        let error = result.unwrap_err();
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
        handle.join().unwrap();
    }
}
