#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Connection lifecycle
    // -----------------------------------------------------------------------

    /// Connect to an IMAP server.
    ///
    /// Performs the TCP connection (with optional TLS), reads the server greeting,
    /// and parses initial capabilities per RFC 3501 Section 6.1.1.
    pub async fn connect(
        host: &str,
        port: u16,
        tls_mode: TlsMode,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let tls_connector = build_default_tls_connector()?;
        Self::connect_with_tls_connector(host, port, tls_mode, tls_connector, timeout).await
    }

    /// Connect with a custom TLS connector (RFC 3501 Section 6.1.1 /
    /// RFC 9051 Section 6.1.1).
    ///
    /// Establishes the TCP/TLS connection, reads the server greeting,
    /// fetches initial capabilities, then spawns the driver task and
    /// returns a handle-based `ImapConnection`.
    #[allow(clippy::too_many_lines)]
    pub async fn connect_with_tls_connector(
        host: &str,
        port: u16,
        tls_mode: TlsMode,
        tls_connector: native_tls::TlsConnector,
        timeout: Duration,
    ) -> Result<Self, Error> {
        use super::dispatch::CapabilityConsumer;
        use super::driver;

        debug!(host, port, ?tls_mode, "connecting to IMAP server");

        let tcp = tokio::time::timeout(timeout, TcpStream::connect((host, port)))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|e| Error::Io(std::sync::Arc::new(e)))?;

        let stream = if tls_mode.uses_implicit_tls() {
            validate_tls_server_name(host)?;
            let connector = tokio_native_tls::TlsConnector::from(tls_connector.clone());
            let tls_stream = tokio::time::timeout(timeout, connector.connect(host, tcp))
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|e| Error::Io(std::sync::Arc::new(std::io::Error::other(e))))?;
            ImapStream::Tls(tls_stream)
        } else {
            ImapStream::Plain(tcp)
        };

        // --- Pre-driver phase: read greeting and initial capabilities ---
        //
        // The WireReader and ProtocolState live on the stack here. After
        // the greeting and capability fetch they are moved into the
        // driver task, which owns them for the rest of the connection.
        let mut wire_reader = wire::WireReader::new(stream);
        let mut proto_state = state::ProtocolState::new();
        let mut tag_gen = tag::TagGenerator::new();

        // Set up the event channel.
        let (events_tx, events_rx) = tokio::sync::mpsc::channel::<typed_event::TypedEvent>(256);

        // Read and parse the server greeting (RFC 3501 Section 7.1).
        // This MUST happen before wrapping events_tx in DriverEventSink
        // so we can send a greeting ALERT on the raw channel (emit() is
        // pub(in crate::connection::driver) and inaccessible from here).
        let greeting = tokio::time::timeout(timeout, wire_reader.read_greeting())
            .await
            .map_err(|_| Error::Timeout)??;

        let Response::Greeting(g) = &greeting else {
            return Err(Error::Protocol(
                "expected greeting from server (RFC 3501 Section 7.1)".into(),
            ));
        };

        // Apply greeting to protocol state (sets session state, caches
        // capabilities, returns Err on BYE).
        let greeting_alert = proto_state.apply_greeting(g)?;

        // RFC 3501 Section7.1: if the greeting carried an [ALERT], deliver it
        // on the raw channel before wrapping in DriverEventSink.
        if let Some(alert_text) = greeting_alert {
            // Best-effort: if the channel is full we drop the alert.
            let _ = events_tx.try_send(typed_event::TypedEvent::Alert(alert_text));
        }

        // Wrap the channel in DriverEventSink  -  events_tx moves here.
        let mut event_sink = driver::event_sink::DriverEventSink::new(events_tx, None);

        // If we didn't get capabilities from the greeting, request them
        // explicitly using the driver's run_one_command (RFC 3501 Section6.1.1).
        if proto_state.capabilities().is_empty() {
            let consumer = driver::DriverConsumer::Regular(
                Box::new(CapabilityConsumer::default()) as Box<dyn driver::ConsumerErased>
            );
            let result = tokio::time::timeout(
                timeout,
                driver::run_one_command(
                    &mut wire_reader,
                    &mut proto_state,
                    &mut tag_gen,
                    &mut event_sink,
                    Command::Capability,
                    consumer,
                ),
            )
            .await
            .map_err(|_| Error::Timeout)??;

            // Downcast the erased output back to Vec<Capability>.
            // CapabilityConsumer::Output is Vec<Capability>, so the downcast
            // is provably correct. An Internal error here would indicate a
            // library bug in the consumer/erased-output machinery.
            let caps = result
                .downcast::<Vec<Capability>>()
                .map_err(|_| Error::Internal("CapabilityConsumer output downcast failed".into()))?;
            proto_state.apply_capability_fetch(*caps);
        }

        // --- STARTTLS upgrade (RFC 3501 Section6.2.1) ---
        //
        // When the caller requests `TlsMode::StartTls`, negotiate the
        // upgrade before spawning the driver task.  All required state
        // (`wire_reader`, `proto_state`, `tag_gen`, `event_sink`) is
        // still on the stack.  `run_starttls_upgrade` sends the STARTTLS
        // command, verifies the buffer is empty, performs the TLS
        // handshake, installs a fresh `WireReader`, and re-fetches
        // capabilities  -  so `proto_state` reflects post-TLS caps by the
        // time we snapshot it into `state_tx` below.
        if tls_mode.uses_starttls() {
            // The server must advertise STARTTLS (RFC 3501 Section6.2.1).
            if !proto_state
                .capabilities()
                .iter()
                .any(|c| matches!(c, Capability::StartTls))
            {
                return Err(Error::StartTlsUnavailable);
            }

            validate_tls_server_name(host)?;

            tokio::time::timeout(
                timeout,
                driver::run_starttls_upgrade(
                    &mut wire_reader,
                    &mut proto_state,
                    &mut tag_gen,
                    &mut event_sink,
                    tls_connector,
                    host.to_owned(),
                ),
            )
            .await
            .map_err(|_| Error::Timeout)??;
        }

        // --- Spawn the driver task ---
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(16);
        let (state_tx, state_rx) = tokio::sync::watch::channel(proto_state.snapshot());

        let handle = tokio::spawn(driver::driver_task(
            wire_reader,
            proto_state,
            tag_gen,
            cmd_rx,
            state_tx,
            event_sink,
        ));

        Ok(Self {
            cmd_tx,
            state_rx,
            events_rx: tokio::sync::Mutex::new(events_rx),
            driver_handle: tokio::sync::Mutex::new(Some(handle)),
            prebuilt_tag_counter: std::sync::atomic::AtomicU32::new(0),
            host: host.to_owned(),
            tls_active: std::sync::atomic::AtomicBool::new(
                tls_mode.uses_implicit_tls() || tls_mode.uses_starttls(),
            ),
        })
    }

    /// Set TCP keepalive on the underlying socket (RFC 1122 Section 4.2.3.6).
    ///
    /// Configures the operating system's TCP keepalive probes via the
    /// driver task. This does not send any data on the IMAP wire  -  it
    /// sets socket options on the underlying file descriptor via
    /// `setsockopt(2)`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the `setsockopt` call fails.
    /// Returns [`Error::DriverGone`] if the driver task has exited.
    pub async fn set_keepalive(&self, keepalive: TcpKeepalive) -> Result<(), Error> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = super::driver::DriverCommand::SetKeepalive {
            keepalive,
            result_tx,
        };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic().await);
        }
        match result_rx.await {
            Ok(result) => result,
            Err(_) => Err(self.observe_driver_panic().await),
        }
    }

    /// If the driver task has terminated, return an error describing
    /// why (panic message if panicked, or `DriverGone` if exited
    /// cleanly). Called from `submit` when `cmd_tx.send` fails so the
    /// caller sees the real reason instead of a generic `Disconnected`.
    pub(super) async fn observe_driver_panic(&self) -> Error {
        let mut guard = self.driver_handle.lock().await;
        let Some(handle) = guard.take() else {
            return Error::DriverGone;
        };
        // `handle.is_finished()` avoids blocking if still running.
        if !handle.is_finished() {
            // Put it back  -  still running. The cmd_tx failure may be
            // a TOCTOU race with the driver exiting.
            *guard = Some(handle);
            drop(guard);
            return Error::DriverGone;
        }
        match handle.await {
            Err(join_err) if join_err.is_panic() => {
                let panic_msg = join_err
                    .into_panic()
                    .downcast::<String>()
                    .map(|s| *s)
                    .or_else(|p| p.downcast::<&'static str>().map(|s| s.to_string()))
                    .unwrap_or_else(|_| "driver panicked (payload not a String)".to_string());
                Error::DriverPanicked(panic_msg)
            }
            Ok(()) | Err(_) => Error::DriverGone,
        }
    }

    /// Upgrade to TLS via STARTTLS (RFC 3501 Section 6.2.1).
    ///
    /// Only valid if `TlsMode::StartTls` was used and `connect` did not already
    /// perform the upgrade. Errors if the server doesn't advertise STARTTLS.
    ///
    /// The upgrade is atomic via the `Poisoned` sentinel pattern (I9, I10)
    ///  -  handled entirely by the driver task.
    pub async fn starttls(&self, timeout: Duration) -> Result<(), Error> {
        self.starttls_with_connector(build_default_tls_connector()?, timeout)
            .await
    }

    /// STARTTLS with a custom TLS connector (RFC 3501 Section 6.2.1 /
    /// RFC 9051 Section 6.2.1).
    ///
    /// The upgrade is atomic via the `Poisoned` sentinel pattern (I9, I10):
    ///
    /// 1. Send STARTTLS, await tagged OK.
    /// 2. Verify the wire buffer is empty (no injected bytes  -  B10 fix).
    /// 3. `mem::replace` the reader with a `Poisoned`-stream reader.
    /// 4. TLS handshake (may suspend). If it fails or the future is
    ///    cancelled, the reader stays wrapping `Poisoned` forever.
    /// 5. Install a fresh `WireReader` on the new TLS stream.
    /// 6. Re-fetch capabilities (RFC 3501 Section6.2.1).
    ///
    /// All steps are executed by the driver task. The caller submits
    /// the upgrade command and awaits the result.
    pub async fn starttls_with_connector(
        &self,
        tls_connector: native_tls::TlsConnector,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.require_state(&[SessionState::NotAuthenticated])?;

        // Check STARTTLS capability from the snapshot.
        {
            let snap = self.state_rx.borrow();
            if !snap.capabilities.is_empty()
                && !snap
                    .capabilities
                    .iter()
                    .any(|c| matches!(c, Capability::StartTls))
            {
                return Err(Error::StartTlsUnavailable);
            }
        }

        // Validate server name before submitting so malformed input fails
        // without involving the driver.
        validate_tls_server_name(&self.host)?;

        debug!("upgrading to TLS via STARTTLS");
        tokio::time::timeout(
            timeout,
            self.submit_upgrade(driver::UpgradePayload::StartTls {
                tls_connector,
                server_name: self.host.clone(),
            }),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        self.tls_active
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}
