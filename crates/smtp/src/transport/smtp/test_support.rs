use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener},
    sync::mpsc,
    thread,
    time::Duration,
};

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
}

#[derive(Debug)]
struct TranscriptStep {
    client: Vec<u8>,
    server: Vec<u8>,
    stall: bool,
    coalesced: bool,
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
            })),
        }
    }

    pub(super) fn expect(self, client: impl AsRef<[u8]>, server: impl AsRef<[u8]>) -> Self {
        self.push(client, server, false, false)
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
        self.push(client, server, false, true)
    }

    /// Accept the client bytes, then never answer.
    #[cfg(feature = "tokio")]
    pub(super) fn expect_then_stall(self, client: impl AsRef<[u8]>) -> Self {
        self.push(client, b"", true, false)
    }

    fn push(
        self,
        client: impl AsRef<[u8]>,
        server: impl AsRef<[u8]>,
        stall: bool,
        coalesced: bool,
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
            });
        self
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

impl std::io::Read for TranscriptStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut state = self
            .transcript
            .shared
            .lock()
            .map_err(|_| std::io::Error::other("SMTP transcript lock poisoned"))?;
        if state.pending_server_bytes.is_empty() {
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
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(unix)]
use std::{
    fs,
    path::PathBuf,
    process,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(unix)]
use std::os::unix::net::UnixListener;

#[cfg(unix)]
static NEXT_UNIX_SOCKET: AtomicU64 = AtomicU64::new(0);

pub(super) struct LmtpServer {
    pub(super) address: SocketAddr,
    commands_rx: mpsc::Receiver<Vec<String>>,
    handle: thread::JoinHandle<()>,
}

#[cfg(unix)]
pub(super) struct UnixLmtpServer {
    pub(super) path: PathBuf,
    commands_rx: mpsc::Receiver<Vec<String>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl LmtpServer {
    pub(super) fn commands(self) -> Vec<String> {
        let commands = self
            .commands_rx
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        self.handle.join().unwrap();
        commands
    }
}

#[cfg(unix)]
impl UnixLmtpServer {
    pub(super) fn commands(mut self) -> Vec<String> {
        let commands = self
            .commands_rx
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        self.handle.take().unwrap().join().unwrap();
        commands
    }
}

#[cfg(unix)]
impl Drop for UnixLmtpServer {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(super) fn spawn_lmtp_delivery_server() -> LmtpServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (commands_tx, commands_rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream.write_all(b"220 localhost\r\n").unwrap();

        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut commands = Vec::new();

        let mut lhlo = String::new();
        reader.read_line(&mut lhlo).unwrap();
        commands.push(lhlo);
        stream
            .write_all(b"250-localhost\r\n250 8BITMIME\r\n")
            .unwrap();

        for response in [
            b"250 sender ok\r\n".as_slice(),
            b"250 rcpt ok\r\n".as_slice(),
            b"550 rcpt rejected\r\n".as_slice(),
            b"250 rcpt ok\r\n".as_slice(),
        ] {
            let mut command = String::new();
            reader.read_line(&mut command).unwrap();
            commands.push(command);
            stream.write_all(response).unwrap();
        }

        let mut data = String::new();
        reader.read_line(&mut data).unwrap();
        commands.push(data);
        stream.write_all(b"354 send message\r\n").unwrap();

        let mut line = String::new();
        loop {
            line.clear();
            reader.read_line(&mut line).unwrap();
            if line == ".\r\n" {
                break;
            }
        }

        stream
            .write_all(b"250 first recipient ok\r\n451 third recipient deferred\r\n")
            .unwrap();
        commands_tx.send(commands).unwrap();
    });

    LmtpServer {
        address,
        commands_rx,
        handle,
    }
}

#[cfg(unix)]
pub(super) fn spawn_unix_lmtp_delivery_server() -> UnixLmtpServer {
    let socket_id = NEXT_UNIX_SOCKET.fetch_add(1, Ordering::Relaxed);
    let socket_dir = std::env::var_os("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/t"));
    fs::create_dir_all(&socket_dir).unwrap();
    let path = socket_dir.join(format!("lmtp-{}-{socket_id}.sock", process::id()));
    let _ = fs::remove_file(&path);

    let listener = UnixListener::bind(&path).unwrap();
    let (commands_tx, commands_rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream.write_all(b"220 localhost\r\n").unwrap();

        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut commands = Vec::new();

        let mut lhlo = String::new();
        reader.read_line(&mut lhlo).unwrap();
        commands.push(lhlo);
        stream
            .write_all(b"250-localhost\r\n250 8BITMIME\r\n")
            .unwrap();

        for response in [
            b"250 sender ok\r\n".as_slice(),
            b"250 rcpt ok\r\n".as_slice(),
            b"550 rcpt rejected\r\n".as_slice(),
            b"250 rcpt ok\r\n".as_slice(),
        ] {
            let mut command = String::new();
            reader.read_line(&mut command).unwrap();
            commands.push(command);
            stream.write_all(response).unwrap();
        }

        let mut data = String::new();
        reader.read_line(&mut data).unwrap();
        commands.push(data);
        stream.write_all(b"354 send message\r\n").unwrap();

        let mut line = String::new();
        loop {
            line.clear();
            reader.read_line(&mut line).unwrap();
            if line == ".\r\n" {
                break;
            }
        }

        stream
            .write_all(b"250 first recipient ok\r\n451 third recipient deferred\r\n")
            .unwrap();
        commands_tx.send(commands).unwrap();
    });

    UnixLmtpServer {
        path,
        commands_rx,
        handle: Some(handle),
    }
}

pub(super) fn assert_lmtp_delivery_commands(commands: &[String]) {
    assert!(commands[0].starts_with("LHLO "));
    assert!(commands[1].starts_with("MAIL FROM:<sender@example.com>"));
    assert!(commands[2].starts_with("RCPT TO:<first@example.com>"));
    assert!(commands[3].starts_with("RCPT TO:<second@example.com>"));
    assert!(commands[4].starts_with("RCPT TO:<third@example.com>"));
    assert_eq!(commands[5], "DATA\r\n");
}
