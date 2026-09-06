use std::{
    io::{self, Read, Write},
    net::{IpAddr, Shutdown, SocketAddr, TcpStream, ToSocketAddrs},
    time::Duration,
};

#[cfg(unix)]
use std::os::fd::OwnedFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(unix)]
use std::path::Path;

use native_tls::TlsStream;
#[cfg(unix)]
use socket2::SockAddr;
use socket2::{Domain, Protocol, Type};

use super::metering::WireMetering;
use super::{ConnectionState, TlsParameters};
use crate::transport::smtp::{Error, error};

/// A network stream
pub(crate) struct NetworkStream {
    inner: Option<InnerNetworkStream>,
    state: ConnectionState,
    /// Byte accounting and cap for this connection. `disabled()` unless
    /// an account wired one in.
    ///
    /// This transport is blocking by construction, so the cap is honoured
    /// by sleeping the calling thread - the same semantic its caller has
    /// already accepted by choosing the blocking API. The async transport
    /// parks a timer instead.
    metering: WireMetering,
}

/// Represents the different types of underlying network streams
// usually only one TLS backend at a time is going to be enabled,
// so clippy::large_enum_variant doesn't make sense here
#[allow(clippy::large_enum_variant)]
enum InnerNetworkStream {
    /// Plain TCP stream
    Tcp(TcpStream),
    /// Plain Unix-domain stream
    #[cfg(unix)]
    Unix(UnixStream),
    /// Encrypted TCP stream
    NativeTls(TlsStream<TcpStream>),
    /// Test-only scripted in-process peer.
    #[cfg(test)]
    Transcript(crate::transport::smtp::test_support::TranscriptStream),
}

impl NetworkStream {
    fn new(inner: InnerNetworkStream) -> Self {
        NetworkStream {
            inner: Some(inner),
            state: ConnectionState::Ok,
            metering: WireMetering::disabled(),
        }
    }

    /// Install byte accounting / capping for this connection.
    pub(super) fn set_metering(&mut self, metering: WireMetering) {
        self.metering = metering;
    }

    /// Record `n` transferred bytes and sleep off any debt the cap
    /// imposes. Blocking, matching this transport's contract.
    fn charge(&mut self, n: usize, inbound: bool) {
        if n == 0 || !self.metering.is_enabled() {
            return;
        }
        let debt = if inbound {
            self.metering.record_in(n)
        } else {
            self.metering.record_out(n)
        };
        if let Some(debt) = debt {
            std::thread::sleep(debt);
        }
    }

    #[cfg(test)]
    pub(crate) fn from_transcript(
        transcript: crate::transport::smtp::test_support::Transcript,
    ) -> Self {
        Self::new(InnerNetworkStream::Transcript(transcript.stream()))
    }

    pub(super) fn state(&self) -> ConnectionState {
        self.state
    }

    pub(super) fn set_state(&mut self, state: ConnectionState) {
        self.state = state;
    }

    /// Shutdowns the connection
    pub(crate) fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        self.state = ConnectionState::Closed;

        match self.inner.as_ref() {
            Some(InnerNetworkStream::Tcp(s)) => s.shutdown(how),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(s)) => s.shutdown(how),
            Some(InnerNetworkStream::NativeTls(s)) => s.get_ref().shutdown(how),
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(_)) => Ok(()),
            None => Ok(()),
        }
    }

    /// Accepted coverage gap: this dial path, and the TLS handshake
    /// `upgrade_tls` performs on it, have no hermetic test. Reaching either
    /// needs a peer on a real socket - a listener and a port - which the
    /// crate's testing rules put out of scope, and the in-process `Transcript`
    /// harness enters below the dial by construction. Everything above the
    /// socket is covered instead: address filtering is pinned directly through
    /// `resolved_address_filter`, and the STARTTLS command exchange is pinned
    /// up to the handshake boundary. Closing the gap would require a
    /// production seam that lets a test substitute the connector, not a new
    /// test.
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
                .map_err(error::connection_io)?
                .filter(|resolved_addr| resolved_address_filter(resolved_addr, local_addr));

            let mut last_err = None;

            for addr in addrs {
                let socket = socket2::Socket::new(
                    Domain::for_address(addr),
                    Type::STREAM,
                    Some(Protocol::TCP),
                )
                .map_err(error::connection_io)?;
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
                Some(last_err) => error::connection_io(last_err),
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

    #[cfg(unix)]
    pub(crate) fn connect_unix(
        path: &Path,
        timeout: Option<Duration>,
    ) -> Result<NetworkStream, Error> {
        let addr = SockAddr::unix(path).map_err(error::connection_io)?;
        let socket =
            socket2::Socket::new(Domain::UNIX, Type::STREAM, None).map_err(error::connection_io)?;
        if let Some(timeout) = timeout {
            socket
                .connect_timeout(&addr, timeout)
                .map_err(error::connection_io)?;
        } else {
            socket.connect(&addr).map_err(error::connection_io)?;
        }
        let stream = UnixStream::from(OwnedFd::from(socket));
        Ok(NetworkStream::new(InnerNetworkStream::Unix(stream)))
    }

    pub(crate) fn upgrade_tls(&mut self, tls_parameters: &TlsParameters) -> Result<(), Error> {
        self.state.verify()?;

        match self.inner.as_ref() {
            Some(InnerNetworkStream::Tcp(_)) => {
                self.state = ConnectionState::Broken;

                let Some(InnerNetworkStream::Tcp(tcp_stream)) = self.inner.take() else {
                    unreachable!()
                };

                self.inner = Some(Self::upgrade_tls_impl(tcp_stream, tls_parameters)?);
                self.state = ConnectionState::Ok;
                Ok(())
            }
            _ => Err(error::invalid_input(
                "STARTTLS is only supported on TCP connections",
            )),
        }
    }

    fn upgrade_tls_impl(
        tcp_stream: TcpStream,
        tls_parameters: &TlsParameters,
    ) -> Result<InnerNetworkStream, Error> {
        let stream = tls_parameters
            .connector
            .connect(tls_parameters.domain(), tcp_stream)
            .map_err(error::tls)?;
        Ok(InnerNetworkStream::NativeTls(stream))
    }

    pub(crate) fn is_encrypted(&self) -> bool {
        match self.inner.as_ref() {
            Some(InnerNetworkStream::Tcp(_)) => false,
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(_)) => false,
            Some(InnerNetworkStream::NativeTls(_)) => true,
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(_)) => false,
            None => false,
        }
    }

    /// DER of the peer (server) certificate when the connection is TLS.
    ///
    /// Returns `None` on plaintext, Unix-domain, and disconnected
    /// streams, and when native-tls cannot produce the certificate DER.
    /// The DER is the input to RFC 5929 `tls-server-end-point` channel
    /// binding; computing the binding is the SASL layer's job, not this
    /// accessor's.
    // Plumbing for SCRAM-PLUS channel binding; the first consumer is the
    // SASL layer, so there is no in-crate caller yet.
    #[allow(dead_code)]
    pub(crate) fn peer_certificate_der(&self) -> Option<Vec<u8>> {
        match self.inner.as_ref() {
            Some(InnerNetworkStream::NativeTls(s)) => s
                .peer_certificate()
                .ok()
                .flatten()
                .and_then(|c| c.to_der().ok()),
            _ => None,
        }
    }

    pub(crate) fn set_read_timeout(&mut self, duration: Option<Duration>) -> io::Result<()> {
        match self.inner.as_mut() {
            Some(InnerNetworkStream::Tcp(stream)) => stream.set_read_timeout(duration),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(stream)) => stream.set_read_timeout(duration),
            Some(InnerNetworkStream::NativeTls(stream)) => {
                stream.get_ref().set_read_timeout(duration)
            }
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(s)) => {
                // No clock to enforce it against, but recording what the
                // driver armed is what makes the per-reply read deadline
                // observable in a hermetic test.
                s.record_read_timeout(duration);
                Ok(())
            }
            None => Err(not_connected()),
        }
    }

    /// Set write timeout for IO calls
    pub(crate) fn set_write_timeout(&mut self, duration: Option<Duration>) -> io::Result<()> {
        match self.inner.as_mut() {
            Some(InnerNetworkStream::Tcp(stream)) => stream.set_write_timeout(duration),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(stream)) => stream.set_write_timeout(duration),

            Some(InnerNetworkStream::NativeTls(stream)) => {
                stream.get_ref().set_write_timeout(duration)
            }
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(_)) => Ok(()),
            None => Err(not_connected()),
        }
    }
}

impl Read for NetworkStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = match self.inner.as_mut() {
            Some(InnerNetworkStream::Tcp(s)) => s.read(buf),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(s)) => s.read(buf),
            Some(InnerNetworkStream::NativeTls(s)) => s.read(buf),
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(s)) => s.read(buf),
            None => Err(not_connected()),
        }?;
        self.charge(read, true);
        Ok(read)
    }
}

impl Write for NetworkStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = match self.inner.as_mut() {
            Some(InnerNetworkStream::Tcp(s)) => s.write(buf),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(s)) => s.write(buf),
            Some(InnerNetworkStream::NativeTls(s)) => s.write(buf),
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(s)) => s.write(buf),
            None => Err(not_connected()),
        }?;
        // Charge what the socket accepted, not what was offered.
        self.charge(written, false);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.inner.as_mut() {
            Some(InnerNetworkStream::Tcp(s)) => s.flush(),
            #[cfg(unix)]
            Some(InnerNetworkStream::Unix(s)) => s.flush(),
            Some(InnerNetworkStream::NativeTls(s)) => s.flush(),
            #[cfg(test)]
            Some(InnerNetworkStream::Transcript(s)) => s.flush(),
            None => Err(not_connected()),
        }
    }
}

fn not_connected() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "network stream is not connected",
    )
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
                .map_err(error::connection_io)?;
        }
        _ => {
            if cfg!(windows) {
                // Windows requires a socket be bound before calling connect
                let any: SocketAddr = match dst_addr {
                    SocketAddr::V4(_) => ([0, 0, 0, 0], 0).into(),
                    SocketAddr::V6(_) => ([0, 0, 0, 0, 0, 0, 0, 0], 0).into(),
                };
                socket.bind(&any.into()).map_err(error::connection_io)?;
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
