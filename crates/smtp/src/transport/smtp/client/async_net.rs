use std::{
    fmt, io, mem,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[cfg(feature = "async-std1")]
use async_std::net::{TcpStream as AsyncStd1TcpStream, ToSocketAddrs as AsyncStd1ToSocketAddrs};
use futures_io::{
    AsyncRead as FuturesAsyncRead, AsyncWrite as FuturesAsyncWrite, Error as IoError,
    Result as IoResult,
};
#[cfg(feature = "tokio1")]
use tokio1_crate::io::{AsyncRead, AsyncWrite, ReadBuf as Tokio1ReadBuf};
#[cfg(feature = "tokio1")]
use tokio1_crate::net::{
    TcpSocket as Tokio1TcpSocket, TcpStream as Tokio1TcpStream,
    ToSocketAddrs as Tokio1ToSocketAddrs,
};
#[cfg(feature = "tokio1-native-tls")]
use tokio1_native_tls_crate::TlsStream as Tokio1TlsStream;

#[cfg(feature = "tokio1-native-tls")]
use super::InnerTlsParameters;
use super::TlsParameters;
#[cfg(feature = "tokio1")]
use crate::transport::smtp::client::net::resolved_address_filter;
use crate::transport::smtp::{Error, error};

/// A network stream
#[derive(Debug)]
#[deprecated(
    since = "0.11.14",
    note = "This struct was not meant to be made public"
)]
pub struct AsyncNetworkStream {
    inner: InnerAsyncNetworkStream,
}

#[cfg(feature = "tokio1")]
pub trait AsyncTokioStream: AsyncRead + AsyncWrite + Send + Sync + Unpin + fmt::Debug {
    fn peer_addr(&self) -> io::Result<SocketAddr>;
}

#[cfg(feature = "tokio1")]
impl AsyncTokioStream for Tokio1TcpStream {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer_addr()
    }
}

/// Represents the different types of underlying network streams
// usually only one TLS backend at a time is going to be enabled,
// so clippy::large_enum_variant doesn't make sense here
#[allow(clippy::large_enum_variant)]
#[allow(dead_code)]
#[derive(Debug)]
enum InnerAsyncNetworkStream {
    /// Plain Tokio 1.x TCP stream
    #[cfg(feature = "tokio1")]
    Tokio1Tcp(Box<dyn AsyncTokioStream>),
    /// Encrypted Tokio 1.x TCP stream
    #[cfg(feature = "tokio1-native-tls")]
    Tokio1NativeTls(Tokio1TlsStream<Box<dyn AsyncTokioStream>>),
    /// Plain Tokio 1.x TCP stream
    #[cfg(feature = "async-std1")]
    AsyncStd1Tcp(AsyncStd1TcpStream),
    /// Can't be built
    None,
}

#[allow(deprecated)]
impl AsyncNetworkStream {
    fn new(inner: InnerAsyncNetworkStream) -> Self {
        if let InnerAsyncNetworkStream::None = inner {
            debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
        }

        AsyncNetworkStream { inner }
    }

    /// Returns peer's address
    pub fn peer_addr(&self) -> IoResult<SocketAddr> {
        match &self.inner {
            #[cfg(feature = "tokio1")]
            InnerAsyncNetworkStream::Tokio1Tcp(s) => s.peer_addr(),
            #[cfg(feature = "tokio1-native-tls")]
            InnerAsyncNetworkStream::Tokio1NativeTls(s) => {
                s.get_ref().get_ref().get_ref().peer_addr()
            }
            #[cfg(feature = "async-std1")]
            InnerAsyncNetworkStream::AsyncStd1Tcp(s) => s.peer_addr(),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Err(IoError::other(
                    "InnerAsyncNetworkStream::None must never be built",
                ))
            }
        }
    }

    #[cfg(feature = "tokio1")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio1")))]
    pub fn use_existing_tokio1(stream: Box<dyn AsyncTokioStream>) -> AsyncNetworkStream {
        AsyncNetworkStream::new(InnerAsyncNetworkStream::Tokio1Tcp(stream))
    }

    #[cfg(feature = "tokio1")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio1")))]
    pub async fn connect_tokio1<T: Tokio1ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        tls_parameters: Option<TlsParameters>,
        local_addr: Option<IpAddr>,
    ) -> Result<AsyncNetworkStream, Error> {
        async fn try_connect<T: Tokio1ToSocketAddrs>(
            server: T,
            timeout: Option<Duration>,
            local_addr: Option<IpAddr>,
        ) -> Result<Tokio1TcpStream, Error> {
            // Timeout is currently a per-phase budget: DNS, each address
            // connection attempt, and TLS handshake each get their own window.
            let lookup = tokio1_crate::net::lookup_host(server);
            let addrs = if let Some(timeout) = timeout {
                tokio1_crate::time::timeout(timeout, lookup)
                    .await
                    .map_err(|_| error::timeout("DNS lookup timed out"))?
            } else {
                lookup.await
            }
            .map_err(error::connection)?
            .filter(|resolved_addr| resolved_address_filter(resolved_addr, local_addr));

            let mut last_err = None;

            for addr in addrs {
                let socket = match addr.ip() {
                    IpAddr::V4(_) => Tokio1TcpSocket::new_v4(),
                    IpAddr::V6(_) => Tokio1TcpSocket::new_v6(),
                }
                .map_err(error::connection)?;
                if let Some(local_addr) = local_addr {
                    socket
                        .bind(SocketAddr::new(local_addr, 0))
                        .map_err(error::connection)?;
                }

                let connect_future = socket.connect(addr);
                if let Some(timeout) = timeout {
                    match tokio1_crate::time::timeout(timeout, connect_future).await {
                        Ok(Ok(stream)) => return Ok(stream),
                        Ok(Err(err)) => last_err = Some(err),
                        Err(_) => {
                            last_err = Some(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "connection timed out",
                            ));
                        }
                    }
                } else {
                    match connect_future.await {
                        Ok(stream) => return Ok(stream),
                        Err(err) => last_err = Some(err),
                    }
                }
            }

            Err(match last_err {
                Some(last_err) => error::connection(last_err),
                None => error::connection("could not resolve to any supported address"),
            })
        }

        let tcp_stream = try_connect(server, timeout, local_addr).await?;
        let mut stream =
            AsyncNetworkStream::new(InnerAsyncNetworkStream::Tokio1Tcp(Box::new(tcp_stream)));
        if let Some(tls_parameters) = tls_parameters {
            stream.upgrade_tls(tls_parameters, timeout).await?;
        }
        Ok(stream)
    }

    #[cfg(feature = "async-std1")]
    #[cfg_attr(docsrs, doc(cfg(feature = "async-std1")))]
    pub async fn connect_asyncstd1<T: AsyncStd1ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        tls_parameters: Option<TlsParameters>,
    ) -> Result<AsyncNetworkStream, Error> {
        // Unfortunately, there doesn't currently seem to be a way to set the local address.
        // Whilst we can create a AsyncStd1TcpStream from an existing socket, it needs to first have
        // been connected, which is a blocking operation.
        async fn try_connect_timeout<T: AsyncStd1ToSocketAddrs>(
            server: T,
            timeout: Duration,
        ) -> Result<AsyncStd1TcpStream, Error> {
            // Timeout is currently a per-phase budget: DNS and each address
            // connection attempt each get their own window.
            let addrs = async_std::future::timeout(timeout, server.to_socket_addrs())
                .await
                .map_err(|_| error::timeout("DNS lookup timed out"))?
                .map_err(error::connection)?;

            let mut last_err = None;

            for addr in addrs {
                let connect_future = AsyncStd1TcpStream::connect(&addr);
                match async_std::future::timeout(timeout, connect_future).await {
                    Ok(Ok(stream)) => return Ok(stream),
                    Ok(Err(err)) => last_err = Some(err),
                    Err(_) => {
                        last_err = Some(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "connection timed out",
                        ));
                    }
                }
            }

            Err(match last_err {
                Some(last_err) => error::connection(last_err),
                None => error::connection("could not resolve to any address"),
            })
        }

        let tcp_stream = match timeout {
            Some(t) => try_connect_timeout(server, t).await?,
            None => AsyncStd1TcpStream::connect(server)
                .await
                .map_err(error::connection)?,
        };

        let mut stream = AsyncNetworkStream::new(InnerAsyncNetworkStream::AsyncStd1Tcp(tcp_stream));
        if let Some(tls_parameters) = tls_parameters {
            stream.upgrade_tls(tls_parameters, timeout).await?;
        }
        Ok(stream)
    }

    pub async fn upgrade_tls(
        &mut self,
        tls_parameters: TlsParameters,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        match &self.inner {
            #[cfg(all(feature = "tokio1", not(feature = "tokio1-native-tls")))]
            InnerAsyncNetworkStream::Tokio1Tcp(_) => {
                let _ = tls_parameters;
                let _ = timeout;
                unreachable!(
                    "Trying to upgrade an AsyncNetworkStream without having enabled the tokio1-native-tls feature"
                );
            }

            #[cfg(feature = "tokio1-native-tls")]
            InnerAsyncNetworkStream::Tokio1Tcp(_) => {
                // get owned TcpStream
                let tcp_stream = mem::replace(&mut self.inner, InnerAsyncNetworkStream::None);
                let InnerAsyncNetworkStream::Tokio1Tcp(tcp_stream) = tcp_stream else {
                    unreachable!()
                };

                self.inner = Self::upgrade_tokio1_tls(tcp_stream, tls_parameters, timeout)
                    .await
                    .map_err(error::connection)?;
                Ok(())
            }
            #[cfg(feature = "async-std1")]
            InnerAsyncNetworkStream::AsyncStd1Tcp(_) => {
                let _ = tls_parameters;
                let _ = timeout;
                unreachable!(
                    "Trying to upgrade an AsyncNetworkStream with async-std, which does not support native-tls"
                );
            }
            _ => Ok(()),
        }
    }

    #[allow(unused_variables)]
    #[cfg(feature = "tokio1-native-tls")]
    async fn upgrade_tokio1_tls(
        tcp_stream: Box<dyn AsyncTokioStream>,
        tls_parameters: TlsParameters,
        timeout: Option<Duration>,
    ) -> Result<InnerAsyncNetworkStream, Error> {
        let domain = tls_parameters.domain().to_owned();

        match tls_parameters.connector {
            InnerTlsParameters::NativeTls { connector } => {
                use tokio1_native_tls_crate::TlsConnector;

                let connector = TlsConnector::from(connector);
                let handshake = connector.connect(&domain, tcp_stream);
                let stream = if let Some(timeout) = timeout {
                    tokio1_crate::time::timeout(timeout, handshake)
                        .await
                        .map_err(|_| error::timeout("TLS handshake timed out"))?
                } else {
                    handshake.await
                };
                Ok(InnerAsyncNetworkStream::Tokio1NativeTls(
                    stream.map_err(error::connection)?,
                ))
            }
        }
    }

    pub fn is_encrypted(&self) -> bool {
        match &self.inner {
            #[cfg(feature = "tokio1")]
            InnerAsyncNetworkStream::Tokio1Tcp(_) => false,
            #[cfg(feature = "tokio1-native-tls")]
            InnerAsyncNetworkStream::Tokio1NativeTls(_) => true,
            #[cfg(feature = "async-std1")]
            InnerAsyncNetworkStream::AsyncStd1Tcp(_) => false,
            InnerAsyncNetworkStream::None => false,
        }
    }

    #[cfg(feature = "tokio1-native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio1-native-tls")))]
    pub fn peer_certificate(&self) -> Result<Vec<u8>, Error> {
        match &self.inner {
            #[cfg(feature = "tokio1")]
            InnerAsyncNetworkStream::Tokio1Tcp(_) => {
                Err(error::client("Connection is not encrypted"))
            }
            #[cfg(feature = "tokio1-native-tls")]
            InnerAsyncNetworkStream::Tokio1NativeTls(stream) => Ok(stream
                .get_ref()
                .peer_certificate()
                .map_err(error::tls)?
                .unwrap()
                .to_der()
                .map_err(error::tls)?),
            #[cfg(feature = "async-std1")]
            InnerAsyncNetworkStream::AsyncStd1Tcp(_) => {
                Err(error::client("Connection is not encrypted"))
            }
            InnerAsyncNetworkStream::None => panic!("InnerNetworkStream::None must never be built"),
        }
    }
}

#[allow(deprecated)]
impl FuturesAsyncRead for AsyncNetworkStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        match &mut self.inner {
            #[cfg(feature = "tokio1")]
            InnerAsyncNetworkStream::Tokio1Tcp(s) => {
                let mut b = Tokio1ReadBuf::new(buf);
                match Pin::new(s).poll_read(cx, &mut b) {
                    Poll::Ready(Ok(())) => Poll::Ready(Ok(b.filled().len())),
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                }
            }
            #[cfg(feature = "tokio1-native-tls")]
            InnerAsyncNetworkStream::Tokio1NativeTls(s) => {
                let mut b = Tokio1ReadBuf::new(buf);
                match Pin::new(s).poll_read(cx, &mut b) {
                    Poll::Ready(Ok(())) => Poll::Ready(Ok(b.filled().len())),
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                }
            }
            #[cfg(feature = "async-std1")]
            InnerAsyncNetworkStream::AsyncStd1Tcp(s) => Pin::new(s).poll_read(cx, buf),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(0))
            }
        }
    }
}

#[allow(deprecated)]
impl FuturesAsyncWrite for AsyncNetworkStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        match &mut self.inner {
            #[cfg(feature = "tokio1")]
            InnerAsyncNetworkStream::Tokio1Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tokio1-native-tls")]
            InnerAsyncNetworkStream::Tokio1NativeTls(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "async-std1")]
            InnerAsyncNetworkStream::AsyncStd1Tcp(s) => Pin::new(s).poll_write(cx, buf),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(0))
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match &mut self.inner {
            #[cfg(feature = "tokio1")]
            InnerAsyncNetworkStream::Tokio1Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tokio1-native-tls")]
            InnerAsyncNetworkStream::Tokio1NativeTls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "async-std1")]
            InnerAsyncNetworkStream::AsyncStd1Tcp(s) => Pin::new(s).poll_flush(cx),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match &mut self.inner {
            #[cfg(feature = "tokio1")]
            InnerAsyncNetworkStream::Tokio1Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tokio1-native-tls")]
            InnerAsyncNetworkStream::Tokio1NativeTls(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "async-std1")]
            InnerAsyncNetworkStream::AsyncStd1Tcp(s) => Pin::new(s).poll_close(cx),
            InnerAsyncNetworkStream::None => {
                debug_assert!(false, "InnerAsyncNetworkStream::None must never be built");
                Poll::Ready(Ok(()))
            }
        }
    }
}
