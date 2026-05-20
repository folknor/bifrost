#![allow(clippy::wildcard_imports)]
use super::*;
use crate::types::validated::ParsedUidSet;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Message operations (UID variants)
    // -----------------------------------------------------------------------

    /// UID FETCH (RFC 3501 Section 6.4.5).
    ///
    /// Collects all responses into a `Vec`. Logs a warning when the buffered
    /// data exceeds 10 MB to nudge callers toward the streaming variant for
    /// large result sets.
    pub async fn uid_fetch(
        &self,
        sequence_set: &SequenceSet,
        items: &[FetchAttr],
        timeout: Duration,
    ) -> Result<Vec<FetchResponse>, Error> {
        self.validate_requested_fetch_items(items)?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        self.fetch_impl(
            Command::UidFetch {
                sequence_set: sequence_set.clone(),
                items: format_fetch_attrs(items),
                changed_since: None,
                vanished: false,
            },
            None,
            timeout,
        )
        .await
    }

    /// UID FETCH with CHANGEDSINCE modifier (RFC 7162 Section 3.1.4).
    ///
    /// Returns only messages whose mod-sequence is greater than `mod_seq`.
    /// Requires the server to support CONDSTORE (RFC 7162).
    pub async fn uid_fetch_changed_since(
        &self,
        sequence_set: &SequenceSet,
        items: &[FetchAttr],
        mod_seq: u64,
        timeout: Duration,
    ) -> Result<Vec<FetchResponse>, Error> {
        self.validate_requested_fetch_items(items)?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        self.fetch_impl(
            Command::UidFetch {
                sequence_set: sequence_set.clone(),
                items: format_fetch_attrs(items),
                changed_since: Some(mod_seq),
                vanished: false,
            },
            Some(mod_seq),
            timeout,
        )
        .await
    }

    /// Shared implementation for FETCH and UID FETCH (RFC 3501 Section 6.4.5).
    ///
    /// When `changed_since` is `Some`, validates that the server supports
    /// CONDSTORE (RFC 7162 Section 3.1.4) before issuing the command.
    pub(super) async fn fetch_impl(
        &self,
        cmd: Command,
        changed_since: Option<u64>,
        timeout: Duration,
    ) -> Result<Vec<FetchResponse>, Error> {
        self.require_state(&[SessionState::Selected])?;
        if changed_since.is_some() {
            self.require_condstore()?;
        }
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, dispatch::FetchConsumer::new()),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// UID FETCH with CHANGEDSINCE and VANISHED modifiers (RFC 7162 Section 3.2.6).
    ///
    /// Issues `UID FETCH <set> (<items>) (CHANGEDSINCE <mod_seq> VANISHED)`.
    /// The server returns `VANISHED (EARLIER)` responses for UIDs in the
    /// requested set that have been expunged since `mod_seq`, plus regular
    /// FETCH responses for messages whose flags changed.
    ///
    /// `VANISHED (EARLIER)` UIDs are defensively filtered to only include
    /// UIDs within the requested `sequence_set`  -  non-conformant servers
    /// may return UIDs outside the set (RFC 7162 Section 3.2.6). When the
    /// sequence set contains `$` (RFC 5182 search result reference),
    /// filtering is best-effort (skipped, since `$` cannot be resolved
    /// client-side).
    ///
    /// Requires:
    /// - QRESYNC must have been `ENABLE`d (RFC 7162 Section 3.2.6).
    /// - The mailbox must be selected.
    pub async fn uid_fetch_vanished(
        &self,
        sequence_set: &SequenceSet,
        items: &[FetchAttr],
        mod_seq: u64,
        timeout: Duration,
    ) -> Result<(Vec<FetchResponse>, Vec<UidRange>), Error> {
        self.require_state(&[SessionState::Selected])?;
        self.validate_requested_fetch_items(items)?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        // RFC 7162 Section 3.2.6: VANISHED modifier requires QRESYNC to be ENABLEd.
        {
            let snap = self.state_rx.borrow();
            if !snap.enabled.iter().any(|e| e == "QRESYNC") {
                return Err(Error::MissingCapability("QRESYNC (not ENABLEd)".into()));
            }
        }
        // Parse the requested set for defensive filtering of VANISHED (EARLIER)
        // UIDs. Returns None when the set contains `$` (RFC 5182), in which case
        // filtering is skipped inside the consumer.
        let parsed_set = ParsedUidSet::new(sequence_set);
        let cmd = Command::UidFetch {
            sequence_set: sequence_set.clone(),
            items: format_fetch_attrs(items),
            changed_since: Some(mod_seq),
            vanished: true,
        };
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, dispatch::FetchVanishedConsumer::new(parsed_set)),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// UID FETCH streaming (RFC 3501 Section 6.4.5).
    ///
    /// Pushes each [`FetchResponse`] through the provided unbounded channel as
    /// it arrives from the server, rather than buffering the entire result set
    /// in memory. The channel is closed when the tagged OK is received.
    ///
    /// If the receiver is dropped, the driver still drains the command to the
    /// tagged completion so the IMAP stream remains synchronized.
    ///
    /// Prefer this over [`uid_fetch`](Self::uid_fetch) when the result set
    /// may be large enough to cause memory pressure.
    pub async fn uid_fetch_streaming(
        &self,
        sequence_set: &SequenceSet,
        items: &[FetchAttr],
        tx: tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.validate_requested_fetch_items(items)?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        self.fetch_streaming_impl(
            Command::UidFetch {
                sequence_set: sequence_set.clone(),
                items: format_fetch_attrs(items),
                changed_since: None,
                vanished: false,
            },
            tx,
            timeout,
        )
        .await
    }

    /// Shared implementation for streaming FETCH and UID FETCH
    /// (RFC 3501 Section 6.4.5).
    ///
    /// Validates session state and dispatches the command with a
    /// [`StreamingFetchConsumer`](dispatch::StreamingFetchConsumer) that
    /// pushes each response through `tx`.
    pub(super) async fn fetch_streaming_impl(
        &self,
        cmd: Command,
        tx: tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.require_state(&[SessionState::Selected])?;
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, dispatch::StreamingFetchConsumer::new(tx)),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// UID SEARCH (RFC 3501 Section 6.4.4).
    ///
    /// `criteria` is the raw IMAP search criteria string, e.g. `"UNSEEN"` or
    /// `"FROM \"alice\" SINCE 1-Mar-2026"`.
    /// Returns a [`SearchResult`] containing matching UIDs and an optional
    /// MODSEQ value (RFC 7162 Section 3.1.5).
    /// Handles both SEARCH and ESEARCH responses. For ESEARCH, the ALL uid-set
    /// is expanded into individual UIDs for backward compatibility.
    ///
    /// Accepts anything that implements `AsRef<str>`, including `&str`,
    /// `String`, and [`SearchCriteria`](crate::types::SearchCriteria).
    pub async fn uid_search(
        &self,
        criteria: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<SearchResult, Error> {
        let criteria = criteria.as_ref();
        self.search_impl(
            criteria,
            Command::UidSearch {
                criteria: criteria.to_owned(),
            },
            "UID SEARCH",
            timeout,
        )
        .await
    }

    /// Shared implementation for SEARCH and UID SEARCH (RFC 3501 Section 6.4.4).
    pub(super) async fn search_impl(
        &self,
        criteria: &str,
        cmd: Command,
        _label: &str,
        timeout: Duration,
    ) -> Result<SearchResult, Error> {
        self.require_state(&[SessionState::Selected])?;
        // RFC 6855 Section 6: SEARCH criteria are free-form strings, so a
        // UTF8=ONLY server requires ENABLE UTF8=ACCEPT before SEARCH.
        self.check_utf8_only_enforced()?;
        self.validate_search_criteria_capabilities(criteria)?;
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, dispatch::SearchConsumer::new()),
        )
        .await
        .map_err(|_| Error::Timeout)??
    }

    /// UID SEARCH with RETURN options (RFC 4731 Section 3.2).
    ///
    /// Returns the full [`EsearchResponse`] with MIN, MAX, COUNT, and ALL fields.
    /// Requires the server to advertise the `ESEARCH` capability.
    ///
    /// RFC 4731 Section 3.2: an extended SEARCH with RETURN options causes the
    /// server to return a single ESEARCH response instead of a SEARCH response.
    ///
    /// Accepts anything that implements `AsRef<str>`, including `&str`,
    /// `String`, and [`SearchCriteria`](crate::types::SearchCriteria).
    pub async fn uid_search_esearch(
        &self,
        criteria: impl AsRef<str>,
        return_opts: &[&str],
        timeout: Duration,
    ) -> Result<EsearchResponse, Error> {
        let criteria = criteria.as_ref();
        self.search_esearch_impl(
            criteria,
            Command::UidSearchReturn {
                criteria: criteria.to_owned(),
                return_opts: return_opts.iter().map(|s| (*s).to_owned()).collect(),
            },
            "UID SEARCH RETURN",
            timeout,
        )
        .await
    }

    /// Shared implementation for SEARCH RETURN and UID SEARCH RETURN
    /// (RFC 4731 Section 3.2).
    ///
    /// Requires the server to advertise the `ESEARCH` capability, or to be
    /// running `IMAP4rev2` (RFC 9051 Section 6.4.4).
    pub(super) async fn search_esearch_impl(
        &self,
        criteria: &str,
        cmd: Command,
        _label: &str,
        timeout: Duration,
    ) -> Result<EsearchResponse, Error> {
        self.require_state(&[SessionState::Selected])?;
        // RFC 6855 Section 6: ESEARCH is an extended SEARCH form and uses the
        // same free-form criteria strings, so it must honor UTF8=ONLY too.
        self.check_utf8_only_enforced()?;
        self.validate_search_criteria_capabilities(criteria)?;
        {
            let snap = self.state_rx.borrow();
            // ESEARCH is a base feature in IMAP4rev2 (RFC 9051 Section 6.4.4).
            if !snap.capabilities.contains(&Capability::Esearch)
                && !super::auth::is_rev2_from_snapshot(&snap)
            {
                return Err(Error::MissingCapability("ESEARCH".into()));
            }
        }
        // RFC 5182 Section 2 extends SEARCH RETURN with the SAVE result option.
        // Generic ESEARCH callers can request SAVE through return_opts, so
        // SEARCHRES remains mandatory unless IMAP4rev2 folds it into the base
        // protocol (RFC 9051 Appendix E).
        if Self::search_return_requests_save(&cmd) {
            self.require_searchres()?;
        }
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, dispatch::EsearchConsumer::new()),
        )
        .await
        .map_err(|_| Error::Timeout)??
    }

    /// UID STORE (RFC 3501 Section 6.4.6).
    ///
    /// If `unchanged_since` is `Some`, uses CONDSTORE (RFC 7162 Section 3.1.3)
    /// to avoid overwriting concurrent flag changes.
    ///
    /// `\Recent` and `\*` are silently filtered per RFC 3501 Section 2.3.2
    /// (they are not valid in STORE flag lists).
    pub async fn uid_store(
        &self,
        sequence_set: &SequenceSet,
        operation: StoreOperation,
        flags: &[Flag],
        unchanged_since: Option<u64>,
        timeout: Duration,
    ) -> Result<StoreResult, Error> {
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        // RFC 3501 Section 2.3.2 / Section 9: \Recent is server-only and \*
        // is not valid in STORE flag lists. Filter them out before encoding,
        // consistent with append() (RFC 3501 Section 6.3.11).
        let filtered_flags = filter_store_flags(flags);
        self.store_impl(
            Command::UidStore {
                sequence_set: sequence_set.clone(),
                operation,
                flags: filtered_flags,
                unchanged_since,
            },
            unchanged_since,
            timeout,
        )
        .await
    }

    /// Shared implementation for STORE and UID STORE (RFC 3501 Section 6.4.6).
    ///
    /// Validates session state, optionally checks CONDSTORE, executes the
    /// command, and collects the resulting FETCH responses and response code.
    pub(super) async fn store_impl(
        &self,
        cmd: Command,
        unchanged_since: Option<u64>,
        timeout: Duration,
    ) -> Result<StoreResult, Error> {
        self.require_state(&[SessionState::Selected])?;
        if unchanged_since.is_some() {
            self.require_condstore()?;
        }
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, dispatch::StoreConsumer::new()),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// UID MOVE (RFC 6851 Section 3, RFC 9051 Appendix E item 2).
    ///
    /// Falls back to COPY + Store `\Deleted` + UID EXPUNGE (RFC 6851 Section 4)
    /// when the server doesn't advertise the MOVE capability and isn't `IMAP4rev2`.
    /// The fallback requires UIDPLUS (RFC 4315)  -  plain EXPUNGE is never used
    /// because it can delete unrelated `\Deleted` messages (data-loss risk).
    ///
    /// Returns a [`MoveResult`] containing:
    /// - `code`: the server's response code, typically `[COPYUID ...]` per
    ///   RFC 6851 Section 4.3 / RFC 4315 Section 3.
    /// - `expunged`: the EXPUNGE or VANISHED responses (RFC 6851 Section 3).
    ///   When QRESYNC is enabled (RFC 7162 Section 3.2.10), the server sends
    ///   VANISHED instead of EXPUNGE.
    pub async fn uid_move_messages(
        &self,
        sequence_set: &SequenceSet,
        mailbox: &str,
        timeout: Duration,
    ) -> Result<MoveResult, Error> {
        self.require_state(&[SessionState::Selected])?;
        self.check_utf8_only_enforced()?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }

        // Read capabilities from snapshot to decide which path to take.
        let (has_move, has_uidplus, is_rev2) = {
            let snap = self.state_rx.borrow();
            (
                snap.capabilities.contains(&Capability::Move),
                snap.capabilities.contains(&Capability::UidPlus),
                super::auth::is_rev2_from_snapshot(&snap),
            )
        };

        // MOVE is a base feature in IMAP4rev2 (RFC 9051 Appendix E item 2).
        if has_move || is_rev2 {
            let mbox = MailboxName::new(mailbox)?;
            self.move_native_impl(
                Command::UidMove {
                    sequence_set: sequence_set.clone(),
                    mailbox: mbox,
                },
                timeout,
            )
            .await
        } else if has_uidplus {
            // Fallback: COPY + STORE +FLAGS.SILENT \Deleted + UID EXPUNGE
            // per RFC 6851 Section 3.3.  The `.SILENT` modifier is required
            // to suppress the implicit untagged FETCH responses that a plain
            // `+FLAGS` would trigger (RFC 3501 Section 6.4.6).  Only safe
            // with UID EXPUNGE which targets specific UIDs.
            // Capture the COPYUID response code from uid_copy
            // (RFC 6851 Section 4.3, RFC 4315 Section 3).
            let copy_result = self.uid_copy(sequence_set, mailbox, timeout).await?;
            self.uid_store(
                sequence_set,
                StoreOperation::AddSilent,
                &[Flag::Deleted],
                Option::None,
                timeout,
            )
            .await?;
            let expunged = self.uid_expunge(sequence_set, timeout).await?;
            Ok(MoveResult {
                code: copy_result.code,
                expunged,
            })
        } else {
            // No MOVE, no UIDPLUS  -  cannot safely move messages.
            // Plain EXPUNGE would delete ALL \Deleted messages, not just the
            // requested set (RFC 9051 Section 6.4.3).
            Err(Error::MissingCapability(
                "MOVE or UIDPLUS (plain EXPUNGE is unsafe for move fallback)".into(),
            ))
        }
    }

    /// Shared implementation for the native MOVE path used by both
    /// `move_messages()` and `uid_move_messages()` (RFC 6851 Section 3).
    ///
    /// Executes the MOVE command and collects EXPUNGE/VANISHED responses.
    pub(super) async fn move_native_impl(
        &self,
        cmd: Command,
        timeout: Duration,
    ) -> Result<MoveResult, Error> {
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, dispatch::MoveConsumer::new()),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// UID COPY (RFC 3501 Section 6.4.7, RFC 4315 Section 3).
    ///
    /// On success, returns the server's response code, which SHOULD be
    /// `[COPYUID uid-validity source-uids dest-uids]` per RFC 4315 Section 3.
    pub async fn uid_copy(
        &self,
        sequence_set: &SequenceSet,
        mailbox: &str,
        timeout: Duration,
    ) -> Result<CopyResult, Error> {
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        let mbox = MailboxName::new(mailbox)?;
        self.copy_impl(
            Command::UidCopy {
                sequence_set: sequence_set.clone(),
                mailbox: mbox,
            },
            timeout,
        )
        .await
    }

    /// Shared implementation for COPY and UID COPY (RFC 3501 Section 6.4.7,
    /// RFC 4315 Section 3).
    ///
    /// On success, returns a [`CopyResult`] containing the server's response
    /// code, which SHOULD be `[COPYUID uid-validity source-uids dest-uids]`
    /// per RFC 4315 Section 3.
    pub(super) async fn copy_impl(
        &self,
        cmd: Command,
        timeout: Duration,
    ) -> Result<CopyResult, Error> {
        self.require_state(&[SessionState::Selected])?;
        self.check_utf8_only_enforced()?;
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, dispatch::CopyConsumer::new()),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// UID EXPUNGE (RFC 4315 UIDPLUS / RFC 9051 Section 6.4.9).
    pub async fn uid_expunge(
        &self,
        sequence_set: &SequenceSet,
        timeout: Duration,
    ) -> Result<ExpungeResult, Error> {
        self.require_state(&[SessionState::Selected])?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        {
            let snap = self.state_rx.borrow();
            // RFC 4315 Section 2: UID EXPUNGE requires the UIDPLUS extension.
            // RFC 9051 Appendix E item 3: IMAP4rev2 incorporates UIDPLUS into
            // the base command set, so rev2 servers implicitly support it.
            if !snap.capabilities.contains(&Capability::UidPlus)
                && !super::auth::is_rev2_from_snapshot(&snap)
            {
                return Err(Error::MissingCapability("UIDPLUS".into()));
            }
        }
        let cmd = Command::UidExpunge {
            sequence_set: sequence_set.clone(),
        };
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, dispatch::ExpungeConsumer::new()),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// EXPUNGE (RFC 3501 Section 6.4.3 / RFC 7162 Section 3.2.10).
    ///
    /// Without QRESYNC, returns `ExpungeResult::Expunged` with the sequence
    /// numbers of removed messages (`* n EXPUNGE`, RFC 3501 Section 7.4.1).
    ///
    /// After `ENABLE QRESYNC` (RFC 7162 Section 3.2.10), the server sends
    /// `VANISHED` responses instead of `EXPUNGE`, and this method returns
    /// `ExpungeResult::Vanished` with UID ranges.
    pub async fn expunge(&self, timeout: Duration) -> Result<ExpungeResult, Error> {
        self.require_state(&[SessionState::Selected])?;
        tokio::time::timeout(
            timeout,
            self.submit_regular(Command::Expunge, dispatch::ExpungeConsumer::new()),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }
}
