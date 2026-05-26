#![allow(clippy::wildcard_imports)]
use super::*;
use crate::types::SecretString;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Authentication
    // -----------------------------------------------------------------------

    /// Authenticate using a policy-selected mechanism.
    ///
    /// Password credentials prefer SCRAM-SHA-256, SCRAM-SHA-1, then PLAIN on
    /// encrypted connections, then CRAM-MD5 only when explicitly allowed by
    /// policy and TLS is active. LOGIN is never used unless explicitly
    /// allowed in [`AuthPolicy`].
    ///
    /// OAuth credentials currently use XOAUTH2 and require TLS unless the
    /// policy explicitly permits cleartext credential mechanisms.
    pub async fn authenticate_best(
        &self,
        credentials: &crate::types::Credentials,
        policy: &crate::types::AuthPolicy,
        timeout: Duration,
    ) -> Result<crate::types::AuthOutcome, Error> {
        use crate::error::{
            AuthMechanismRejection, AuthMechanismRejectionReason, AuthPolicyFailure,
        };
        use crate::types::{AuthMechanism, AuthOutcome, CredentialsKind};

        let profile = self.server_profile();
        match credentials.kind() {
            CredentialsKind::OAuth2 {
                identity,
                access_token,
            } => {
                if !profile.supports_sasl_auth(AuthMechanism::XOAuth2) {
                    return Err(Error::MissingCapability("AUTH=XOAUTH2".into()));
                }
                if !self.is_encrypted() && !policy.allow_cleartext_without_tls {
                    return Err(Error::AuthPolicy(AuthPolicyFailure::new(
                        offered_authentication(&profile, false),
                        vec![AuthMechanismRejection::new(
                            AuthMechanism::XOAuth2,
                            AuthMechanismRejectionReason::CleartextWithoutTls,
                        )],
                    )));
                }
                self.authenticate_xoauth2(identity, access_token.as_str(), timeout)
                    .await?;
                Ok(AuthOutcome {
                    mechanism: AuthMechanism::XOAuth2,
                })
            }
            CredentialsKind::Password { username, password } => {
                let mut rejected = Vec::new();
                for mechanism in [
                    AuthMechanism::ScramSha256,
                    AuthMechanism::ScramSha1,
                    AuthMechanism::Plain,
                    AuthMechanism::CramMd5,
                    AuthMechanism::Login,
                ] {
                    let supported = if mechanism == AuthMechanism::Login {
                        profile.supports_login_command()
                    } else {
                        profile.supports_sasl_auth(mechanism)
                    };
                    if !supported {
                        continue;
                    }
                    let mut rejection = None;
                    match mechanism {
                        AuthMechanism::ScramSha256 => {
                            self.authenticate_scram_sha256(username, password.as_str(), timeout)
                                .await?;
                        }
                        AuthMechanism::ScramSha1 => {
                            self.authenticate_scram_sha1(username, password.as_str(), timeout)
                                .await?;
                        }
                        AuthMechanism::Plain => {
                            if !self.is_encrypted() && !policy.allow_cleartext_without_tls {
                                rejection = Some(AuthMechanismRejectionReason::CleartextWithoutTls);
                            }
                            if let Some(reason) = rejection {
                                rejected.push(AuthMechanismRejection::new(mechanism, reason));
                                continue;
                            }
                            self.authenticate_plain(username, password.as_str(), timeout)
                                .await?;
                        }
                        AuthMechanism::CramMd5 => {
                            if !policy.allow_cram_md5 {
                                rejection = Some(AuthMechanismRejectionReason::DisabledByPolicy);
                            }
                            if !self.is_encrypted() && !policy.allow_cleartext_without_tls {
                                rejection = Some(AuthMechanismRejectionReason::CleartextWithoutTls);
                            }
                            if let Some(reason) = rejection {
                                rejected.push(AuthMechanismRejection::new(mechanism, reason));
                                continue;
                            }
                            self.authenticate_cram_md5(username, password.as_str(), timeout)
                                .await?;
                        }
                        AuthMechanism::Login => {
                            if !policy.allow_login {
                                rejection = Some(AuthMechanismRejectionReason::DisabledByPolicy);
                            }
                            if !self.is_encrypted() && !policy.allow_cleartext_without_tls {
                                rejection = Some(AuthMechanismRejectionReason::CleartextWithoutTls);
                            }
                            if let Some(reason) = rejection {
                                rejected.push(AuthMechanismRejection::new(mechanism, reason));
                                continue;
                            }
                            self.login(username, password.as_str(), timeout).await?;
                        }
                        AuthMechanism::XOAuth2 => unreachable!("password path skips XOAUTH2"),
                    }
                    return Ok(AuthOutcome { mechanism });
                }
                Err(Error::AuthPolicy(AuthPolicyFailure::new(
                    offered_authentication(&profile, true),
                    rejected,
                )))
            }
        }
    }

    /// Authenticate with LOGIN command (RFC 3501 Section 6.2.3).
    ///
    /// **Note:** LOGIN is deprecated in `IMAP4rev2` (RFC 9051 Section 2.2).
    /// Prefer [`authenticate_plain`](Self::authenticate_plain) when the server
    /// advertises `AUTH=PLAIN`, which is the standard SASL mechanism and works
    /// with all servers including those that don't support LOGIN (e.g. Stalwart).
    pub async fn login(&self, user: &str, pass: &str, timeout: Duration) -> Result<(), Error> {
        use super::dispatch::LoginConsumer;

        // Validate state and capabilities from the driver's snapshot.
        {
            let snap = self.state_rx.borrow();
            // RFC 3501 Section 6.2.3: LOGIN is only valid in
            // NotAuthenticated state.
            if snap.session_state != SessionState::NotAuthenticated {
                return Err(Error::Protocol(format!(
                    "command not valid in {:?} state (expected one of \
                     [{:?}])",
                    snap.session_state,
                    SessionState::NotAuthenticated,
                )));
            }
            // RFC 3501 Section 6.2.3: "If the server advertises the
            // LOGINDISABLED capability [...] the LOGIN command MUST NOT
            // be used."
            if snap
                .capabilities
                .iter()
                .any(|c| matches!(c, Capability::LoginDisabled))
            {
                return Err(Error::Protocol(
                    "LOGIN disabled by server (LOGINDISABLED capability \
                     advertised, RFC 3501 Section 6.2.3)"
                        .into(),
                ));
            }

            // RFC 9051 Section 2.2: warn when a better alternative
            // exists. Use the broader check (raw capability presence)
            // rather than is_rev2 so that dual-mode servers that
            // advertise rev2 but haven't had ENABLE issued yet still
            // trigger the warning.
            let has_rev2 = is_rev2_from_snapshot(&snap)
                || snap
                    .capabilities
                    .iter()
                    .any(|c| matches!(c, Capability::Imap4Rev2));
            let has_auth_plain = snap
                .capabilities
                .iter()
                .any(|c| matches!(c, Capability::Auth(m) if m.eq_ignore_ascii_case("PLAIN")));
            drop(snap);
            if has_rev2 && has_auth_plain {
                warn!(
                    "LOGIN is deprecated on IMAP4rev2 servers \
                     (RFC 9051 Section 2.2); prefer \
                     authenticate_plain()  -  the server advertises \
                     AUTH=PLAIN"
                );
            }
        }

        // RFC 6855 Section 5: LOGIN must not be used for non-ASCII
        // credentials. Clients must use AUTHENTICATE instead.
        if !user.is_ascii() || !pass.is_ascii() {
            return Err(Error::Protocol(
                "LOGIN does not support non-ASCII credentials; \
                 use AUTHENTICATE (RFC 6855 Section 5)"
                    .into(),
            ));
        }

        let deadline = tokio::time::Instant::now() + timeout;
        let cmd = Command::Login {
            user: user.to_owned(),
            pass: pass.to_owned().into(),
        };

        let caps_provided =
            tokio::time::timeout(timeout, self.submit_regular(cmd, LoginConsumer::default()))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Authenticate with SASL PLAIN mechanism (RFC 4616).
    ///
    /// Constructs the PLAIN payload (`\0user\0pass`) and sends it via
    /// AUTHENTICATE PLAIN, using SASL-IR (RFC 4959) when available.
    pub async fn authenticate_plain(
        &self,
        user: &str,
        pass: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::AuthenticatePlainConsumer;
        use base64::Engine;

        // Validate state and build SASL payload from the snapshot.
        let (encoded, has_sasl_ir) = {
            let snap = self.state_rx.borrow();
            if snap.session_state != SessionState::NotAuthenticated {
                return Err(Error::Protocol(format!(
                    "command not valid in {:?} state (expected one of \
                     [{:?}])",
                    snap.session_state,
                    SessionState::NotAuthenticated,
                )));
            }
            // RFC 3501 Section 6.2.2: verify the server advertises
            // AUTH=PLAIN before sending credentials.
            if !snap
                .capabilities
                .contains(&Capability::Auth("PLAIN".into()))
            {
                return Err(Error::MissingCapability("AUTH=PLAIN".into()));
            }

            // PLAIN payload per RFC 4616 Section 2:
            // [authzid] NUL authcid NUL passwd  -  authzid is empty.
            let mut payload =
                zeroize::Zeroizing::new(Vec::with_capacity(1 + user.len() + 1 + pass.len()));
            payload.push(b'\0');
            payload.extend_from_slice(user.as_bytes());
            payload.push(b'\0');
            payload.extend_from_slice(pass.as_bytes());
            let encoded: SecretString = base64::engine::general_purpose::STANDARD
                .encode(payload.as_slice())
                .into();

            let has_sasl_ir =
                snap.capabilities.contains(&Capability::SaslIr) || is_rev2_from_snapshot(&snap);
            drop(snap);
            (encoded, has_sasl_ir)
        };

        let cmd = Command::Authenticate {
            mechanism: "PLAIN".to_owned(),
            initial_response: if has_sasl_ir {
                Some(encoded.clone())
            } else {
                None
            },
        };

        let consumer = AuthenticatePlainConsumer::new(encoded, has_sasl_ir);
        let deadline = tokio::time::Instant::now() + timeout;
        let caps_provided =
            tokio::time::timeout(timeout, self.submit_with_continuations(cmd, consumer))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Authenticate with XOAUTH2 SASL mechanism (Google-defined, not an
    /// IETF RFC).
    ///
    /// Uses SASL-IR (RFC 4959 Section 3) if the server advertises it.
    pub async fn authenticate_xoauth2(
        &self,
        user: &str,
        token: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::AuthenticateXoauth2Consumer;
        use base64::Engine;

        // Validate state and build XOAUTH2 payload from the snapshot.
        let (encoded, has_sasl_ir) = {
            let snap = self.state_rx.borrow();
            if snap.session_state != SessionState::NotAuthenticated {
                return Err(Error::Protocol(format!(
                    "command not valid in {:?} state (expected one of \
                     [{:?}])",
                    snap.session_state,
                    SessionState::NotAuthenticated,
                )));
            }
            // RFC 3501 Section 6.2.2: verify the server advertises
            // AUTH=XOAUTH2 before sending credentials.
            if !snap
                .capabilities
                .contains(&Capability::Auth("XOAUTH2".into()))
            {
                return Err(Error::MissingCapability("AUTH=XOAUTH2".into()));
            }

            // Build XOAUTH2 payload:
            // "user=<user>\x01auth=Bearer <token>\x01\x01"
            let payload =
                zeroize::Zeroizing::new(format!("user={user}\x01auth=Bearer {token}\x01\x01"));
            let encoded: SecretString = base64::engine::general_purpose::STANDARD
                .encode(payload.as_bytes())
                .into();

            let has_sasl_ir =
                snap.capabilities.contains(&Capability::SaslIr) || is_rev2_from_snapshot(&snap);
            drop(snap);
            (encoded, has_sasl_ir)
        };

        let cmd = Command::Authenticate {
            mechanism: "XOAUTH2".to_owned(),
            initial_response: if has_sasl_ir {
                Some(encoded.clone())
            } else {
                None
            },
        };

        let consumer = AuthenticateXoauth2Consumer::new(encoded, has_sasl_ir);
        let deadline = tokio::time::Instant::now() + timeout;
        let caps_provided =
            tokio::time::timeout(timeout, self.submit_with_continuations(cmd, consumer))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Authenticate with SASL CRAM-MD5.
    ///
    /// CRAM-MD5 is a challenge-response mechanism. The password is never sent
    /// directly on the wire, but the mechanism is still legacy and should only
    /// be used when stronger SASL mechanisms are unavailable.
    pub async fn authenticate_cram_md5(
        &self,
        user: &str,
        pass: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::AuthenticateCramMd5Consumer;

        self.require_auth_mechanism("CRAM-MD5")?;

        let cmd = Command::Authenticate {
            mechanism: "CRAM-MD5".to_owned(),
            initial_response: None,
        };

        let consumer = AuthenticateCramMd5Consumer::new(user.to_owned(), pass.to_owned().into());
        let deadline = tokio::time::Instant::now() + timeout;
        let caps_provided =
            tokio::time::timeout(timeout, self.submit_with_continuations(cmd, consumer))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Authenticate with SASL SCRAM-SHA-1 (RFC 5802).
    pub async fn authenticate_scram_sha1(
        &self,
        user: &str,
        pass: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.authenticate_scram(user, pass, super::dispatch::ScramMechanism::Sha1, timeout)
            .await
    }

    /// Authenticate with SASL SCRAM-SHA-256 (RFC 7677).
    pub async fn authenticate_scram_sha256(
        &self,
        user: &str,
        pass: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.authenticate_scram(user, pass, super::dispatch::ScramMechanism::Sha256, timeout)
            .await
    }

    async fn authenticate_scram(
        &self,
        user: &str,
        pass: &str,
        mechanism: super::dispatch::ScramMechanism,
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::AuthenticateScramConsumer;

        self.require_auth_mechanism(mechanism.name())?;
        let has_sasl_ir = {
            let snap = self.state_rx.borrow();
            snap.capabilities.contains(&Capability::SaslIr) || is_rev2_from_snapshot(&snap)
        };

        let nonce = generate_scram_nonce()?;
        let consumer = AuthenticateScramConsumer::new(
            mechanism,
            user.to_owned(),
            pass.to_owned().into(),
            nonce,
            has_sasl_ir,
        );
        let initial_response = has_sasl_ir.then(|| consumer.initial_response());
        let cmd = Command::Authenticate {
            mechanism: mechanism.name().to_owned(),
            initial_response,
        };

        let deadline = tokio::time::Instant::now() + timeout;
        let caps_provided =
            tokio::time::timeout(timeout, self.submit_with_continuations(cmd, consumer))
                .await
                .map_err(|_| Error::timeout_inflight())??;

        self.complete_auth(caps_provided, deadline).await
    }

    /// Finalize authentication: refresh capabilities if the server did
    /// not provide them inline.
    ///
    /// State transition to `Authenticated` is handled by the driver task
    /// via the `in_auth` flag in `ProtocolState::apply_tagged`  -  the
    /// driver sets `in_auth` when it sees `Command::Login` or
    /// `Command::Authenticate`, and `apply_tagged` transitions on tagged
    /// OK (RFC 3501 Section6.2.2, Section6.2.3).
    ///
    /// If the server did not include updated capabilities in its response,
    /// issue an explicit CAPABILITY command (RFC 3501 Section6.2.2 / Section6.2.3).
    async fn complete_auth(
        &self,
        caps_provided: bool,
        deadline: tokio::time::Instant,
    ) -> Result<(), Error> {
        if !caps_provided {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::timeout_inflight());
            }
            // Send CAPABILITY via the driver. apply_side_effects inside
            // the driver updates the cached capabilities automatically
            // when it processes the untagged CAPABILITY or tagged
            // [CAPABILITY] response code (RFC 3501 Section7.2.1).
            tokio::time::timeout(remaining, self.fetch_capabilities_via_driver())
                .await
                .map_err(|_| Error::timeout_inflight())??;
        }
        Ok(())
    }

    fn require_auth_mechanism(&self, mechanism: &str) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        if snap.session_state != SessionState::NotAuthenticated {
            return Err(Error::Protocol(format!(
                "command not valid in {:?} state (expected one of [{:?}])",
                snap.session_state,
                SessionState::NotAuthenticated,
            )));
        }
        if !snap
            .capabilities
            .contains(&Capability::Auth(mechanism.to_owned()))
        {
            return Err(Error::MissingCapability(format!("AUTH={mechanism}")));
        }
        Ok(())
    }

    /// Send CAPABILITY via the driver task and wait for completion.
    ///
    /// The driver's `apply_side_effects` updates the cached capability set
    /// automatically. The consumer output is ignored  -  the canonical state
    /// lives in `ProtocolState` inside the driver.
    async fn fetch_capabilities_via_driver(&self) -> Result<(), Error> {
        use super::dispatch::CapabilityConsumer;

        let _caps: Vec<Capability> = self
            .submit_regular(Command::Capability, CapabilityConsumer::default())
            .await?;
        Ok(())
    }

    /// UNAUTHENTICATE  -  reset to unauthenticated state (RFC 8437 Section 2).
    ///
    /// Resets the IMAP session to Not Authenticated without closing the
    /// TLS connection. After success, the client can LOGIN or AUTHENTICATE
    /// as a different user  -  enabling connection pooling across users.
    ///
    /// Clears all per-user state: selected mailbox, NOTIFY registrations,
    /// and ENABLE'd extensions. The TLS layer remains intact.
    ///
    /// After unauthentication, issues an explicit CAPABILITY command to
    /// refresh the cached capabilities, since the server's advertised
    /// capabilities may differ between authenticated and unauthenticated
    /// states (RFC 8437 Section 2).
    ///
    /// # Stale events
    ///
    /// Events buffered before this call may contain stale data from the
    /// previous session (e.g., EXISTS, EXPUNGE for a previously selected
    /// mailbox). Callers should drain the event channel before
    /// re-authenticating.
    ///
    /// # Errors
    ///
    /// - [`Error::MissingCapability`] if `UNAUTHENTICATE` is not advertised.
    /// - [`Error::Protocol`] if called in Not Authenticated state.
    pub async fn unauthenticate(&self, timeout: Duration) -> Result<(), Error> {
        // RFC 8437 Section2: valid in Authenticated or Selected state.
        self.require_state(&[SessionState::Authenticated, SessionState::Selected])?;

        // Connection-level capability check.
        {
            let snap = self.state_rx.borrow();
            if !snap.capabilities.contains(&Capability::Unauthenticate) {
                return Err(Error::MissingCapability("UNAUTHENTICATE".into()));
            }
        }

        let deadline = tokio::time::Instant::now() + timeout;

        tokio::time::timeout(
            timeout,
            self.submit_regular(
                Command::Unauthenticate,
                super::dispatch::TaggedOkConsumer::default(),
            ),
        )
        .await
        .map_err(|_| Error::timeout_inflight())??;

        // RFC 8437 Section2: capabilities may change after unauthentication.
        // Unconditional refresh  -  the server SHOULD send caps but is not
        // required to.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(Error::timeout_inflight());
        }
        tokio::time::timeout(remaining, self.fetch_capabilities_via_driver())
            .await
            .map_err(|_| Error::timeout_inflight())??;

        Ok(())
    }

    /// Graceful logout (RFC 3501 Section 6.1.3 / RFC 9051 Section 6.1.3).
    ///
    /// Sends LOGOUT via the driver task and validates the required
    /// response sequence: the server MUST send `* BYE` followed by a
    /// tagged OK.
    ///
    /// State transitions are handled by `apply_side_effects` inside the
    /// driver task:
    /// - `* BYE` -> `Logout` (via `apply_untagged`)
    /// - Tagged OK with `in_logout` -> `Logout` (via `apply_tagged`)
    ///
    /// The driver sets `in_logout` automatically when it sees
    /// `Command::Logout`.
    pub async fn logout(&self) -> Result<(), Error> {
        use super::dispatch::LogoutConsumer;

        // RFC 3501 Section 3.4: already in Logout state  -  no-op.
        if self.state_rx.borrow().session_state == SessionState::Logout {
            return Ok(());
        }

        let _: () = self
            .submit_regular(Command::Logout, LogoutConsumer::default())
            .await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Submit helpers
    // -----------------------------------------------------------------------

    /// Submit a regular (non-continuation) command to the driver task
    /// and await the typed result.
    pub(super) async fn submit_regular<C: super::dispatch::Consumer + 'static>(
        &self,
        cmd: Command,
        consumer: C,
    ) -> Result<C::Output, Error>
    where
        C::Output: 'static,
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = driver::DriverCommand::Run {
            payload: driver::DriverCommandPayload::Standard(cmd),
            consumer: driver::DriverConsumer::Regular(
                Box::new(consumer) as Box<dyn driver::ConsumerErased>
            ),
            result_tx,
        };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic().await);
        }
        let result = match result_rx.await {
            Ok(inner) => inner?,
            Err(_) => return Err(self.observe_driver_panic().await),
        };
        let output = *result
            .downcast::<C::Output>()
            .map_err(|_| Error::Internal("type mismatch in driver result".into()))?;
        Ok(output)
    }

    /// Submit a streaming regular command to the driver task and await the
    /// typed result.
    pub(super) async fn submit_streaming<C: super::dispatch::StreamingConsumer + 'static>(
        &self,
        cmd: Command,
        consumer: C,
    ) -> Result<C::Output, Error>
    where
        C::Output: 'static,
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = driver::DriverCommand::Run {
            payload: driver::DriverCommandPayload::Standard(cmd),
            consumer: driver::DriverConsumer::StreamingRegular(
                Box::new(consumer) as Box<dyn driver::StreamingConsumerErased>
            ),
            result_tx,
        };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic().await);
        }
        let result = match result_rx.await {
            Ok(inner) => inner?,
            Err(_) => return Err(self.observe_driver_panic().await),
        };
        let output = *result
            .downcast::<C::Output>()
            .map_err(|_| Error::Internal("type mismatch in driver result".into()))?;
        Ok(output)
    }

    /// Submit a continuation-aware command to the driver task and await
    /// the typed result.
    ///
    /// Used by AUTHENTICATE (SASL PLAIN, XOAUTH2) which expect `+`
    /// continuations during the response loop (RFC 3501 Section7.5).
    pub(super) async fn submit_with_continuations<
        C: super::dispatch::ContinuationConsumer + 'static,
    >(
        &self,
        cmd: Command,
        consumer: C,
    ) -> Result<C::Output, Error>
    where
        C::Output: 'static,
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = driver::DriverCommand::Run {
            payload: driver::DriverCommandPayload::Standard(cmd),
            consumer: driver::DriverConsumer::WithContinuations(
                Box::new(consumer) as Box<dyn driver::ContinuationConsumerErased>
            ),
            result_tx,
        };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic().await);
        }
        let result = match result_rx.await {
            Ok(inner) => inner?,
            Err(_) => return Err(self.observe_driver_panic().await),
        };
        let output = *result
            .downcast::<C::Output>()
            .map_err(|_| Error::Internal("type mismatch in driver result".into()))?;
        Ok(output)
    }

    /// Submit a pre-built command (APPEND/MULTIAPPEND) to the driver task.
    ///
    /// The handle builds the complete wire bytes (including tag) and
    /// provides them along with the classification metadata. The driver
    /// sends the bytes with literal synchronization and runs the response
    /// classification loop.
    pub(super) async fn submit_prebuilt<C: super::dispatch::Consumer + 'static>(
        &self,
        wire_bytes: bytes::BytesMut,
        tag: String,
        cmd_kind: crate::types::CommandKind,
        cmd_target: Option<crate::types::validated::MailboxName>,
        consumer: C,
    ) -> Result<C::Output, Error>
    where
        C::Output: 'static,
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = driver::DriverCommand::Run {
            payload: driver::DriverCommandPayload::PreBuilt {
                wire_bytes,
                tag,
                cmd_kind,
                cmd_target,
            },
            consumer: driver::DriverConsumer::Regular(
                Box::new(consumer) as Box<dyn driver::ConsumerErased>
            ),
            result_tx,
        };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic().await);
        }
        let result = match result_rx.await {
            Ok(inner) => inner?,
            Err(_) => return Err(self.observe_driver_panic().await),
        };
        let output = *result
            .downcast::<C::Output>()
            .map_err(|_| Error::Internal("type mismatch in driver result".into()))?;
        Ok(output)
    }

    /// Submit a stream upgrade (STARTTLS / COMPRESS) to the driver task.
    ///
    /// The driver handles the entire upgrade sequence atomically: sends
    /// the protocol command, awaits the tagged OK, then swaps the stream
    /// using the `Poisoned` sentinel (I9, I10). No consumer is needed.
    pub(super) async fn submit_upgrade(
        &self,
        payload: driver::UpgradePayload,
    ) -> Result<(), Error> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = driver::DriverCommand::Upgrade { payload, result_tx };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic().await);
        }
        match result_rx.await {
            Ok(inner) => {
                inner?;
                Ok(())
            }
            Err(_) => Err(self.observe_driver_panic().await),
        }
    }
}

fn offered_authentication(
    profile: &crate::types::ServerProfile,
    include_login: bool,
) -> Vec<String> {
    let mut offered = profile
        .auth_mechanisms
        .iter()
        .map(|mechanism| format!("AUTH={mechanism}"))
        .collect::<Vec<_>>();
    if include_login && profile.supports_login_command() {
        offered.push("LOGIN".to_owned());
    }
    offered
}

/// Check if `IMAP4rev2` behavior is active from a
/// [`ConnectionStateSnapshot`](driver::ConnectionStateSnapshot).
///
/// Mirrors `ImapConnection::is_rev2` but operates on the snapshot so it
/// can be used while the borrow is held (RFC 9051 Section6.3.1).
pub(super) fn is_rev2_from_snapshot(snap: &driver::ConnectionStateSnapshot) -> bool {
    let has_rev2 = snap.capabilities.contains(&Capability::Imap4Rev2);
    let has_rev1 = snap.capabilities.contains(&Capability::Imap4Rev1);
    if has_rev2 && has_rev1 {
        // Dual-mode server: rev2 requires explicit ENABLE
        // (RFC 9051 Section 6.3.1).
        snap.enabled
            .iter()
            .any(|e| e.eq_ignore_ascii_case("IMAP4rev2"))
    } else {
        has_rev2
    }
}

fn generate_scram_nonce() -> Result<String, Error> {
    use base64::Engine;

    let mut bytes = [0u8; 18];
    getrandom::fill(&mut bytes)
        .map_err(|e| Error::Protocol(format!("failed to generate SCRAM nonce: {e}")))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}
