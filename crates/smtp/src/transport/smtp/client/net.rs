use std::{
    io::{self, Read, Write},
    net::{IpAddr, Shutdown, SocketAddr, TcpStream, ToSocketAddrs},
    time::Duration,
};

#[cfg(feature = "native-tls")]
use std::mem;

#[cfg(feature = "native-tls")]
use native_tls::TlsStream;
use socket2::{Domain, Protocol, Type};

#[cfg(feature = "native-tls")]
use super::InnerTlsParameters;
use super::TlsParameters;
use crate::transport::smtp::{Error, error};

/// A network stream
pub(crate) struct NetworkStream {
    inner: InnerNetworkStream,
}

/// Represents the different types of underlying network streams
// usually only one TLS backend at a time is going to be enabled,
// so clippy::large_enum_variant doesn't make sense here
#[allow(clippy::large_enum_variant)]
enum InnerNetworkStream {
    /// Plain TCP stream
    Tcp(TcpStream),
    /// Encrypted TCP stream
    #[cfg(feature = "native-tls")]
    NativeTls(TlsStream<TcpStream>),
    /// Can't be built
    #[cfg(feature = "native-tls")]
    None,
}

impl NetworkStream {
    fn new(inner: InnerNetworkStream) -> Self {
        #[cfg(feature = "native-tls")]
        if let InnerNetworkStream::None = inner {
            debug_assert!(false, "InnerNetworkStream::None must never be built");
        }

        NetworkStream { inner }
    }

    /// Shutdowns the connection
    pub(crate) fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match &self.inner {
            InnerNetworkStream::Tcp(s) => s.shutdown(how),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::NativeTls(s) => s.get_ref().shutdown(how),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::None => {
                debug_assert!(false, "InnerNetworkStream::None must never be built");
                Ok(())
            }
        }
    }

    pub(crate) fn connect<T: ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        tls_parameters: Option<&TlsParameters>,
        local_addr: Option<IpAddr>,
    ) -> Result<NetworkStream, Error> {
        fn try_connect<T: ToSocketAddrs>(
            server: T,
            timeout: Option<Duration>,
            local_addr: Option<IpAddr>,
        ) -> Result<TcpStream, Error> {
            let addrs = server
                .to_socket_addrs()
                .map_err(error::connection)?
                .filter(|resolved_addr| resolved_address_filter(resolved_addr, local_addr));

            let mut last_err = None;

            for addr in addrs {
                let socket = socket2::Socket::new(
                    Domain::for_address(addr),
                    Type::STREAM,
                    Some(Protocol::TCP),
                )
                .map_err(error::connection)?;
                bind_local_address(&socket, &addr, local_addr)?;

                if let Some(timeout) = timeout {
                    match socket.connect_timeout(&addr.into(), timeout) {
                        Ok(()) => return Ok(socket.into()),
                        Err(err) => last_err = Some(err),
                    }
                } else {
                    match socket.connect(&addr.into()) {
                        Ok(()) => return Ok(socket.into()),
                        Err(err) => last_err = Some(err),
                    }
                }
            }

            Err(match last_err {
                Some(last_err) => error::connection(last_err),
                None => error::connection("could not resolve to any address"),
            })
        }

        let tcp_stream = try_connect(server, timeout, local_addr)?;
        let mut stream = NetworkStream::new(InnerNetworkStream::Tcp(tcp_stream));
        if let Some(tls_parameters) = tls_parameters {
            stream.upgrade_tls(tls_parameters)?;
        }
        Ok(stream)
    }

    #[cfg(not(feature = "native-tls"))]
    pub(crate) fn upgrade_tls(&mut self, tls_parameters: &TlsParameters) -> Result<(), Error> {
        let _ = self;
        let _ = tls_parameters;
        unreachable!("Trying to upgrade a NetworkStream without having enabled native-tls");
    }

    #[cfg(feature = "native-tls")]
    pub(crate) fn upgrade_tls(&mut self, tls_parameters: &TlsParameters) -> Result<(), Error> {
        match &self.inner {
            InnerNetworkStream::Tcp(_) => {
                // get owned TcpStream
                let tcp_stream = mem::replace(&mut self.inner, InnerNetworkStream::None);
                let InnerNetworkStream::Tcp(tcp_stream) = tcp_stream else {
                    unreachable!()
                };

                self.inner = Self::upgrade_tls_impl(tcp_stream, tls_parameters)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    #[cfg(feature = "native-tls")]
    fn upgrade_tls_impl(
        tcp_stream: TcpStream,
        tls_parameters: &TlsParameters,
    ) -> Result<InnerNetworkStream, Error> {
        Ok(match &tls_parameters.connector {
            InnerTlsParameters::NativeTls { connector } => {
                let stream = connector
                    .connect(tls_parameters.domain(), tcp_stream)
                    .map_err(error::connection)?;
                InnerNetworkStream::NativeTls(stream)
            }
        })
    }

    pub(crate) fn is_encrypted(&self) -> bool {
        match &self.inner {
            InnerNetworkStream::Tcp(_) => false,
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::NativeTls(_) => true,
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::None => {
                debug_assert!(false, "InnerNetworkStream::None must never be built");
                false
            }
        }
    }

    pub(crate) fn set_read_timeout(&mut self, duration: Option<Duration>) -> io::Result<()> {
        match &mut self.inner {
            InnerNetworkStream::Tcp(stream) => stream.set_read_timeout(duration),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::NativeTls(stream) => stream.get_ref().set_read_timeout(duration),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::None => {
                debug_assert!(false, "InnerNetworkStream::None must never be built");
                Ok(())
            }
        }
    }

    /// Set write timeout for IO calls
    pub(crate) fn set_write_timeout(&mut self, duration: Option<Duration>) -> io::Result<()> {
        match &mut self.inner {
            InnerNetworkStream::Tcp(stream) => stream.set_write_timeout(duration),

            #[cfg(feature = "native-tls")]
            InnerNetworkStream::NativeTls(stream) => stream.get_ref().set_write_timeout(duration),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::None => {
                debug_assert!(false, "InnerNetworkStream::None must never be built");
                Ok(())
            }
        }
    }
}

impl Read for NetworkStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            InnerNetworkStream::Tcp(s) => s.read(buf),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::NativeTls(s) => s.read(buf),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::None => {
                debug_assert!(false, "InnerNetworkStream::None must never be built");
                Ok(0)
            }
        }
    }
}

impl Write for NetworkStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.inner {
            InnerNetworkStream::Tcp(s) => s.write(buf),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::NativeTls(s) => s.write(buf),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::None => {
                debug_assert!(false, "InnerNetworkStream::None must never be built");
                Ok(0)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            InnerNetworkStream::Tcp(s) => s.flush(),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::NativeTls(s) => s.flush(),
            #[cfg(feature = "native-tls")]
            InnerNetworkStream::None => {
                debug_assert!(false, "InnerNetworkStream::None must never be built");
                Ok(())
            }
        }
    }
}

/// If the local address is set, binds the socket to this address.
/// If local address is not set, then destination address is required to determine the default
/// local address on some platforms.
/// See: <https://github.com/hyperium/hyper/blob/faf24c6ad8eee1c3d5ccc9a4d4835717b8e2903f/src/client/connect/http.rs#L560>
fn bind_local_address(
    socket: &socket2::Socket,
    dst_addr: &SocketAddr,
    local_addr: Option<IpAddr>,
) -> Result<(), Error> {
    match local_addr {
        Some(local_addr) => {
            socket
                .bind(&SocketAddr::new(local_addr, 0).into())
                .map_err(error::connection)?;
        }
        _ => {
            if cfg!(windows) {
                // Windows requires a socket be bound before calling connect
                let any: SocketAddr = match dst_addr {
                    SocketAddr::V4(_) => ([0, 0, 0, 0], 0).into(),
                    SocketAddr::V6(_) => ([0, 0, 0, 0, 0, 0, 0, 0], 0).into(),
                };
                socket.bind(&any.into()).map_err(error::connection)?;
            }
        }
    }
    Ok(())
}

/// When we have an iterator of resolved remote addresses, we must filter them to be the same
/// protocol as the local address binding. If no local address is set, then all will be matched.
pub(crate) fn resolved_address_filter(
    resolved_addr: &SocketAddr,
    local_addr: Option<IpAddr>,
) -> bool {
    match local_addr {
        Some(local_addr) => match resolved_addr.ip() {
            IpAddr::V4(_) => local_addr.is_ipv4(),
            IpAddr::V6(_) => local_addr.is_ipv6(),
        },
        None => true,
    }
}
