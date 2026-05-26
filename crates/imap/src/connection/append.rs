#![allow(clippy::wildcard_imports)]
use bytes::BytesMut;

use super::*;

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

        // Check APPENDLIMIT (RFC 7889)  -  reject early if the message is too large.
        // Compare in u64 space to avoid truncating the limit on 32-bit
        // platforms where usize is 32 bits (RFC 7889 Section5: number64).
        {
            let snap = self.state_rx.borrow();
            for cap in &snap.capabilities {
                if let Capability::AppendLimit(Some(limit)) = cap {
                    if (message.len() as u64) > *limit {
                        return Err(Error::AppendLimit {
                            size: message.len() as u64,
                            limit: *limit,
                        });
                    }
                    break;
                }
            }
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
            timeout,
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
        // Also snapshot capabilities for APPENDLIMIT + BINARY checks.
        let (has_multiappend, append_limit, allow_literal8) = {
            let snap = self.state_rx.borrow();
            let has_multiappend = snap.capabilities.contains(&Capability::MultiAppend);
            let append_limit: Option<u64> = snap.capabilities.iter().find_map(|cap| {
                if let Capability::AppendLimit(Some(limit)) = cap {
                    Some(*limit)
                } else {
                    None
                }
            });
            // RFC 7888 Section 6 / RFC 9051 Section 9: literal8 may use
            // non-synchronizing `+` only when BINARY is advertised AND the
            // connection is NOT pure IMAP4rev2 (rev2 literal8 is always
            // synchronizing).
            let allow_literal8 = snap.capabilities.contains(&Capability::Binary)
                && !super::auth::is_rev2_from_snapshot(&snap);
            drop(snap);
            (has_multiappend, append_limit, allow_literal8)
        };

        if !has_multiappend {
            return Err(Error::MissingCapability("MULTIAPPEND".into()));
        }

        if messages.is_empty() {
            return Err(Error::Protocol(
                "MULTIAPPEND requires at least one message".into(),
            ));
        }

        // Validate all message sizes up front.
        // Compare in u64 space to avoid truncating the limit on 32-bit
        // platforms where usize is 32 bits (RFC 7889 Section5: number64).
        if let Some(limit) = append_limit {
            for msg in messages {
                if (msg.data.len() as u64) > limit {
                    return Err(Error::AppendLimit {
                        size: msg.data.len() as u64,
                        limit,
                    });
                }
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
            timeout,
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
}
