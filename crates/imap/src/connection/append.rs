#![allow(clippy::wildcard_imports)]
use bytes::BytesMut;

use super::*;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct AdvertisedAppendLimits {
    global: Option<u64>,
    mailbox_specific: bool,
}

impl AdvertisedAppendLimits {
    fn from_capabilities(capabilities: &[Capability]) -> Self {
        let mut limits = Self::default();
        for capability in capabilities {
            match capability {
                Capability::AppendLimit(Some(limit)) => {
                    limits.global =
                        Some(limits.global.map_or(*limit, |current| current.min(*limit)));
                }
                Capability::AppendLimit(None) => limits.mailbox_specific = true,
                _ => {}
            }
        }
        limits
    }
}

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Append
    // -----------------------------------------------------------------------

    /// APPEND a message to a mailbox (RFC 3501 Section 6.3.11).
    ///
    /// Handles literal synchronization: sends header with `{count}\r\n`,
    /// waits for `+` continuation, then sends literal data.
    /// Uses LITERAL+ (RFC 7888 Section 4) `{count+}` when the
    /// server advertises it.
    ///
    /// Returns `Some((uid_validity, uid))` when the server supports UIDPLUS
    /// (RFC 4315) and includes an `[APPENDUID]` response code, otherwise `None`.
    pub async fn append(
        &self,
        mailbox: &str,
        flags: &[Flag],
        date: Option<&str>,
        message: &[u8],
        timeout: Duration,
    ) -> Result<Option<(u32, u32)>, Error> {
        use super::dispatch::AppendConsumer;

        self.check_utf8_only_enforced()?;
        // RFC 3501 Section 6.3.11: APPEND is valid in Authenticated and Selected states.
        self.require_state(&[SessionState::Authenticated, SessionState::Selected])?;

        let deadline = tokio::time::Instant::now() + timeout;
        if let Some(limit) = self.append_limit_for_mailbox(mailbox, timeout).await? {
            self.check_append_limit(message.len(), limit)?;
        }

        let utf8_enabled = self.utf8_enabled();
        let literal_kind = self.append_literal_kind(message)?;
        let effective_non_sync = self.append_literal_is_non_sync(literal_kind, message.len());
        // RFC 7888 Sections 4-5: determine the literal mode for the encoder.
        let mode = self.literal_mode();

        // Build the complete wire bytes as a single buffer.
        // The driver will send them with literal synchronization handling.
        let tag = self.next_prebuilt_tag();
        // RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1: encode mailbox name
        // with INBOX normalization and MUTF-7 when not in UTF-8 mode.
        let wire_mailbox = crate::codec::encode::encode_mailbox_str(mailbox, utf8_enabled);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(tag.as_bytes());
        buf.extend_from_slice(b" APPEND ");
        // RFC 6855 Section 3: when UTF8=ACCEPT is active, the server MUST accept
        // UTF-8 in quoted strings, so non-ASCII mailbox names can use quoted form
        // instead of falling back to a synchronizing literal.
        // RFC 7888 Sections 4-5: use non-synchronizing literal when available.
        encode_quoted_or_literal_utf8(&mut buf, wire_mailbox.as_bytes(), utf8_enabled, mode);

        // RFC 3501 Section 6.3.11 / RFC 9051 Section 6.3.12: \Recent is
        // server-only and \* is not valid in APPEND flag lists. Filter them
        // out just like encode_multi_append_header does.
        let filtered_flags: Vec<&Flag> = flags
            .iter()
            .filter(|f| !matches!(f, Flag::Recent | Flag::Wildcard))
            .collect();
        // Validate custom flag keywords contain only ATOM-CHARs (RFC 3501 Section 9).
        for flag in &filtered_flags {
            if let Flag::Custom(s) = flag {
                crate::codec::encode::validate_flag_keyword(s)?;
            }
        }
        if !filtered_flags.is_empty() {
            buf.extend_from_slice(b" (");
            for (i, flag) in filtered_flags.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b" ");
                }
                buf.extend_from_slice(flag.as_imap_str().as_bytes());
            }
            buf.extend_from_slice(b")");
        }

        if let Some(d) = date {
            // Validate against the date-time production (RFC 3501 Section 9).
            crate::codec::encode::validate_append_datetime(d)?;
            // Date-time is a quoted string (RFC 3501 Section 9).
            buf.extend_from_slice(b" ");
            // RFC 7888 Sections 4-5: use non-synchronizing literal when available.
            encode_quoted_or_literal(&mut buf, d.as_bytes(), mode);
        }

        // Literal header.
        // RFC 6855 Section 4: when UTF8=ACCEPT is enabled, use the UTF8
        // APPEND data extension: `UTF8 (~{size}\r\n<message>)`.
        // RFC 3516 Section 4.4: APPEND data containing NUL octets must use
        // the `literal8` prefix `~`, not classic `literal` syntax.
        // RFC 7888 Section 6: non-synchronizing literal8 (`~{N+}\r\n`) is
        // only valid when BOTH BINARY and LITERAL+/LITERAL- permit it.
        match literal_kind {
            AppendLiteralKind::Utf8Literal8 => buf.extend_from_slice(b" UTF8 (~{"),
            AppendLiteralKind::Literal8 => buf.extend_from_slice(b" ~{"),
            AppendLiteralKind::Literal => buf.extend_from_slice(b" {"),
        }
        buf.extend_from_slice(message.len().to_string().as_bytes());
        if effective_non_sync {
            buf.extend_from_slice(b"+");
        }
        buf.extend_from_slice(b"}\r\n");

        // Literal data + closing delimiter.
        buf.extend_from_slice(message);
        if utf8_enabled {
            // RFC 6855 Section 4: close the UTF8 data extension group.
            buf.extend_from_slice(b")\r\n");
        } else {
            buf.extend_from_slice(b"\r\n");
        }

        // Submit the pre-built bytes to the driver task.
        tokio::time::timeout(
            remaining_timeout(deadline)?,
            self.submit_prebuilt(
                buf,
                tag,
                crate::types::CommandKind::Append,
                None,
                AppendConsumer::default(),
            ),
        )
        .await
        .map_err(|_| Error::timeout_inflight())?
    }

    /// MULTIAPPEND  -  append multiple messages in a single APPEND command (RFC 3502).
    ///
    /// Sends all messages as consecutive literals in one APPEND command.
    /// Each message carries its own flags and optional internal date.
    /// The first message includes the mailbox name; subsequent messages
    /// follow immediately with their own flag/date/literal (RFC 3502 Section 3).
    ///
    /// Checks APPENDLIMIT (RFC 7889) per message and uses LITERAL+
    /// (RFC 7888 Section 4) when the server advertises it.
    ///
    /// Returns a `Vec<(uid_validity, uid)>` extracted from `[APPENDUID]` response
    /// codes (RFC 4315 UIDPLUS). The vec may be empty if the server does not
    /// support UIDPLUS.
    #[allow(clippy::too_many_lines)]
    pub async fn multi_append(
        &self,
        mailbox: &str,
        messages: &[AppendMessage],
        timeout: Duration,
    ) -> Result<Vec<(u32, u32)>, Error> {
        use super::dispatch::MultiAppendConsumer;

        self.check_utf8_only_enforced()?;
        // RFC 3502 Section 3: MULTIAPPEND is valid in Authenticated and Selected states.
        self.require_state(&[SessionState::Authenticated, SessionState::Selected])?;

        // Require MULTIAPPEND capability (RFC 3502 Section 3).
        // Also snapshot APPENDLIMIT and BINARY behavior before any STATUS
        // lookup for a mailbox-specific limit.
        let (has_multiappend, advertised_limits, allow_literal8) = {
            let snap = self.state_rx.borrow();
            let has_multiappend = snap.capabilities.contains(&Capability::MultiAppend);
            let advertised_limits = AdvertisedAppendLimits::from_capabilities(&snap.capabilities);
            // RFC 7888 Section 6 / RFC 9051 Section 9: literal8 may use
            // non-synchronizing `+` only when BINARY is advertised AND the
            // connection is NOT pure IMAP4rev2 (rev2 literal8 is always
            // synchronizing).
            let allow_literal8 = snap.capabilities.contains(&Capability::Binary)
                && !super::auth::is_rev2_from_snapshot(&snap);
            drop(snap);
            (has_multiappend, advertised_limits, allow_literal8)
        };

        if !has_multiappend {
            return Err(Error::MissingCapability("MULTIAPPEND".into()));
        }

        if messages.is_empty() {
            return Err(Error::Protocol(
                "MULTIAPPEND requires at least one message".into(),
            ));
        }

        let deadline = tokio::time::Instant::now() + timeout;
        if let Some(limit) = self
            .append_limit_from_advertisement(mailbox, advertised_limits, timeout)
            .await?
        {
            for msg in messages {
                self.check_append_limit(msg.data.len(), limit)?;
            }
        }

        let literal_kinds: Vec<AppendLiteralKind> = messages
            .iter()
            .map(|msg| self.append_literal_kind(&msg.data))
            .collect::<Result<_, _>>()?;

        let utf8_enabled = self.utf8_enabled();
        let tag = self.next_prebuilt_tag();
        // RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1: encode mailbox name
        // with INBOX normalization and MUTF-7 when not in UTF-8 mode.
        let wire_mailbox = crate::codec::encode::encode_mailbox_str(mailbox, utf8_enabled);

        // RFC 7888 Sections 4-5: determine the literal mode for the encoder.
        let mode = self.literal_mode();

        // Build the complete wire bytes for all messages.
        let mut buf = BytesMut::new();

        for (i, (msg, literal_kind)) in messages.iter().zip(literal_kinds.iter()).enumerate() {
            // Build the header for this message (RFC 3502 Section 3).
            let header_start = buf.len();
            encode_multi_append_header_with_literal8(
                &mut buf,
                &tag,
                &wire_mailbox,
                &msg.flags,
                msg.date.as_deref(),
                msg.data.len(),
                i == 0,
                mode,
                matches!(literal_kind, AppendLiteralKind::Utf8Literal8),
                matches!(
                    literal_kind,
                    AppendLiteralKind::Literal8 | AppendLiteralKind::Utf8Literal8
                ),
            )?;

            // RFC 7888 Section 6: when both BINARY and a literal extension are
            // active, literal8 may use the non-synchronizing `+` modifier. The
            // encoder conservatively emits synchronizing literal8, so patch the
            // header before appending the literal data.
            let header_bytes = buf.split_off(header_start);
            let patched_header = match mode {
                LiteralMode::LiteralPlus => {
                    patch_literals_to_plus_with_binary(&header_bytes, allow_literal8)
                }
                LiteralMode::LiteralMinus => {
                    patch_small_literals_to_plus_with_binary(&header_bytes, allow_literal8)
                }
                LiteralMode::Synchronizing => header_bytes,
            };
            buf.extend_from_slice(&patched_header);

            // If the literal is synchronizing, the driver's
            // send_with_literal_sync will detect the {N}\r\n boundary
            // and wait for the server's `+` continuation before sending
            // the literal data. Non-sync markers ({N+}\r\n) are sent
            // without waiting.

            // Literal data.
            buf.extend_from_slice(&msg.data);
            if utf8_enabled {
                // RFC 6855 Section 4: close the UTF8 data extension group.
                buf.extend_from_slice(b")");
            }
            if i == messages.len() - 1 {
                // Final message  -  terminate the command with CRLF.
                buf.extend_from_slice(b"\r\n");
            }
        }

        // Submit the pre-built bytes to the driver task.
        tokio::time::timeout(
            remaining_timeout(deadline)?,
            self.submit_prebuilt(
                buf,
                tag,
                crate::types::CommandKind::Append,
                None,
                MultiAppendConsumer::default(),
            ),
        )
        .await
        .map_err(|_| Error::timeout_inflight())?
    }

    /// Resolve the effective APPENDLIMIT for one destination mailbox.
    ///
    /// RFC 7889 defines `APPENDLIMIT=<n>` as a global limit and bare
    /// `APPENDLIMIT` as a request to obtain the mailbox value with STATUS.
    /// A conforming server advertises one form, but a contradictory capability
    /// list is handled conservatively: obtain a mailbox value when requested
    /// and apply the smallest numeric value available.
    async fn append_limit_for_mailbox(
        &self,
        mailbox: &str,
        timeout: Duration,
    ) -> Result<Option<u64>, Error> {
        let advertised = {
            let snapshot = self.state_rx.borrow();
            AdvertisedAppendLimits::from_capabilities(&snapshot.capabilities)
        };
        self.append_limit_from_advertisement(mailbox, advertised, timeout)
            .await
    }

    async fn append_limit_from_advertisement(
        &self,
        mailbox: &str,
        advertised: AdvertisedAppendLimits,
        timeout: Duration,
    ) -> Result<Option<u64>, Error> {
        let mailbox_limit = if advertised.mailbox_specific {
            // This STATUS is a preflight: the APPEND itself has not been
            // written, so every failure here is Unsent evidence relative to
            // the APPEND. Leaving the STATUS attempt state in place would make
            // a non-idempotent APPEND look uncertain and send recovery down
            // the reconcile path instead of simply retrying.
            let status = self
                .status(mailbox, "APPENDLIMIT", timeout)
                .await
                .map_err(unsent_preflight)?;
            mailbox_append_limit(&status)?
        } else {
            None
        };

        Ok(match (advertised.global, mailbox_limit) {
            (Some(global), Some(mailbox)) => Some(global.min(mailbox)),
            (Some(global), None) => Some(global),
            (None, Some(mailbox)) => Some(mailbox),
            (None, None) => None,
        })
    }

    fn check_append_limit(&self, size: usize, limit: u64) -> Result<(), Error> {
        let size = u64::try_from(size).unwrap_or(u64::MAX);
        if size > limit {
            return Err(Error::AppendLimit { size, limit });
        }
        Ok(())
    }
}

/// The most restrictive APPENDLIMIT any plausible reply to this STATUS named.
///
/// RFC 5465 Section 4: with NOTIFY STATUS active the solicited reply is
/// wire-identical to a notification, so the heuristic that picked
/// [`StatusResult::items`] may have picked a stale notification and left the
/// real answer in [`StatusResult::ambiguous`]. Ignoring `ambiguous` therefore
/// either invents a protocol error (the answer is there, just not where we
/// looked) or applies a higher, stale limit. RFC 7889 Section 3.1: an APPEND
/// over the limit is refused, so the smallest named value is the safe choice.
///
/// `None` means "no limit for this mailbox" (an explicit `APPENDLIMIT NIL`),
/// which is different from the absence of the item altogether: the latter is a
/// server contradicting its own advertised capability.
pub(super) fn mailbox_append_limit(status: &StatusResult) -> Result<Option<u64>, Error> {
    let mut saw_append_limit = false;
    let mut limit = None;
    for item in status.items.iter().chain(status.ambiguous.iter().flatten()) {
        if let StatusItem::AppendLimit(value) = item {
            saw_append_limit = true;
            if let Some(value) = *value {
                limit = Some(limit.map_or(value, |current: u64| current.min(value)));
            }
        }
    }
    if !saw_append_limit {
        return Err(Error::Protocol(
            "APPENDLIMIT capability but STATUS response omitted APPENDLIMIT".into(),
        ));
    }
    Ok(limit)
}

/// Restamp a preflight failure as `Unsent` relative to the APPEND.
///
/// The helper command carries its own transmission evidence, which is about
/// the helper, not about the APPEND. APPEND is non-idempotent, so publishing
/// the helper's `InFlight` / `Acknowledged` evidence would make recovery
/// reconcile a message that was never written.
fn unsent_preflight(error: Error) -> Error {
    error.with_attempt(bifrost_types::TransmissionState::Unsent)
}

/// Time left before the caller's deadline.
///
/// Expiry here is still before `submit_prebuilt`, so it is `Unsent`: no APPEND
/// octet has reached the driver.
fn remaining_timeout(deadline: tokio::time::Instant) -> Result<Duration, Error> {
    deadline
        .checked_duration_since(tokio::time::Instant::now())
        .ok_or_else(|| Error::timeout().with_attempt(bifrost_types::TransmissionState::Unsent))
}
