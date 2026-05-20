#![allow(clippy::wildcard_imports)]
use super::*;

/// Compare two decoded mailbox names for command-response correlation.
///
/// RFC 3501 Section 5.1: `INBOX` is case-insensitive; all other mailbox
/// names are compared byte-for-byte. Both arguments must be decoded
/// (user-facing UTF-8), not wire-form.
pub(crate) fn inbox_eq(a: &str, b: &str) -> bool {
    if a.eq_ignore_ascii_case("INBOX") && b.eq_ignore_ascii_case("INBOX") {
        return true;
    }
    a == b
}

impl ImapConnection {
    /// Current session state (RFC 3501 Section3 / RFC 9051 Section3).
    ///
    /// Returns a snapshot of the session state as last observed by the
    /// driver task. State transitions are driven by command responses
    /// (e.g., `SELECT` moves to `Selected`, `LOGOUT` moves to `Logout`).
    pub fn session_state(&self) -> SessionState {
        self.state_rx.borrow().session_state
    }

    /// Cached server capabilities (RFC 3501 Section7.2.1 / RFC 9051 Section7.2.1).
    ///
    /// Returns a clone of the capability list as last observed by the
    /// driver task. Updated automatically when the server advertises
    /// new capabilities (e.g., post-STARTTLS, post-LOGIN).
    pub fn capabilities(&self) -> Vec<Capability> {
        self.state_rx.borrow().capabilities.clone()
    }

    /// Caller-friendly server capability profile snapshot.
    ///
    /// This clones the driver's current capability and ENABLE state. The
    /// result is stale after STARTTLS, authentication, or ENABLE; call this
    /// method again after those transitions before making feature decisions.
    pub fn server_profile(&self) -> crate::types::ServerProfile {
        let snap = self.state_rx.borrow();
        crate::types::ServerProfile::new(snap.capabilities.clone(), snap.enabled.clone())
    }

    /// Whether the current transport is encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.tls_active.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Check if `IMAP4rev2` behavior is active (RFC 9051).
    ///
    /// Many extension capabilities (`ESEARCH`, `IDLE`, `MOVE`, etc.) are part of the
    /// `IMAP4rev2` base set and don't need separate capability tokens.
    ///
    /// RFC 9051 Section 6.3.1: when both `IMAP4rev1` and `IMAP4rev2` are
    /// advertised, a client MUST issue `ENABLE IMAP4rev2` before assuming
    /// rev2 behavior. If the server advertises only `IMAP4rev2` (no rev1),
    /// rev2 is implicitly active.
    pub(super) fn is_rev2(&self) -> bool {
        let snap = self.state_rx.borrow();
        super::auth::is_rev2_from_snapshot(&snap)
    }

    /// Check if the server requires UTF8=ACCEPT to be enabled (RFC 6855 Section 3).
    ///
    /// When a server advertises `UTF8=ONLY`, clients MUST issue
    /// `ENABLE UTF8=ACCEPT` before sending commands that use mailbox names
    /// or string arguments.
    ///
    /// On dual-mode servers that advertise both `IMAP4rev1` and `IMAP4rev2`,
    /// RFC 9051 Appendix A requires the client to issue `ENABLE IMAP4rev2`
    /// before rev2 UTF-8 quoted-string behavior becomes active. Once that
    /// happens, the connection is already in the UTF-8-capable mode that
    /// `UTF8=ONLY` requires.
    pub(super) fn check_utf8_only_enforced(&self) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        let utf8_enabled = snap
            .enabled
            .iter()
            .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT"));
        let needs_utf8 = snap.capabilities.contains(&Capability::Utf8Only)
            && !utf8_enabled
            && !super::auth::is_rev2_from_snapshot(&snap);
        drop(snap);
        if needs_utf8 {
            return Err(Error::Protocol(
                "server requires ENABLE UTF8=ACCEPT before use \
                 (UTF8=ONLY advertised, RFC 6855 Section 3)"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Ensure the session is in one of the allowed states (RFC 3501 Section 6).
    pub(super) fn require_state(&self, allowed: &[SessionState]) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        let session_state = snap.session_state;
        drop(snap);
        if allowed.contains(&session_state) {
            Ok(())
        } else {
            Err(Error::Protocol(format!(
                "command not valid in {session_state:?} state (expected one of {allowed:?})"
            )))
        }
    }

    /// Verify that the server supports CONDSTORE (RFC 7162 Section 3.1).
    ///
    /// QRESYNC implies CONDSTORE (RFC 7162 Section 3.2.3), so either
    /// capability satisfies the requirement.
    pub(super) fn require_condstore(&self) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        let has_condstore = snap.capabilities.contains(&Capability::Condstore)
            || snap.capabilities.contains(&Capability::QResync);
        drop(snap);
        if !has_condstore {
            return Err(Error::MissingCapability("CONDSTORE".into()));
        }
        Ok(())
    }

    /// Verify that the server advertises the SEARCHRES capability (RFC 5182 Section 2).
    pub(super) fn require_searchres(&self) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        // RFC 9051 Appendix E: IMAP4rev2 folds SEARCHRES into the base protocol.
        let has_searchres = snap.capabilities.contains(&Capability::SearchRes)
            || super::auth::is_rev2_from_snapshot(&snap);
        drop(snap);
        if !has_searchres {
            return Err(Error::MissingCapability("SEARCHRES".into()));
        }
        Ok(())
    }

    /// Returns `true` when a generic SEARCH RETURN command requests the SAVE
    /// result option from the SEARCHRES extension (RFC 5182 Section 2).
    pub(super) fn search_return_requests_save(cmd: &Command) -> bool {
        match cmd {
            Command::SearchReturn { return_opts, .. }
            | Command::UidSearchReturn { return_opts, .. } => return_opts
                .iter()
                .any(|opt| opt.trim().eq_ignore_ascii_case("SAVE")),
            _ => false,
        }
    }

    /// RFC 7162 Section 3.1.5 defines `MODSEQ` as a SEARCH extension key, so
    /// SEARCH-family commands must not use it until CONDSTORE or QRESYNC is
    /// available. SORT and THREAD inherit the same search criteria grammar.
    pub(super) fn require_condstore_for_modseq_criterion(
        &self,
        criteria: &str,
    ) -> Result<(), Error> {
        if Self::search_criteria_contains_atom(criteria, "MODSEQ") {
            self.require_condstore()?;
        }
        Ok(())
    }

    /// Validate SEARCH-family extension criteria against negotiated
    /// capabilities before sending the command.
    ///
    /// RFC 7162 Section 3.1.5 defines `MODSEQ` as a SEARCH extension key,
    /// while RFC 5032 Sections 2 and 3 add `OLDER` and `YOUNGER` only for
    /// servers that advertise the WITHIN capability. RFC 8514 Sections 4.1,
    /// 4.3, and 5 likewise add `SAVEDBEFORE`, `SAVEDON`, `SAVEDSINCE`, and
    /// `SAVEDATESUPPORTED` only for servers that advertise `SAVEDATE`.
    /// RFC 8474 Sections 6 and 7 likewise add the `EMAILID` and `THREADID`
    /// SEARCH keys only for servers that advertise `OBJECTID`.
    /// RFC 5182 Sections 2.1 and 3 extend SEARCH-family criteria with the
    /// `$` marker only when SEARCHRES (or `IMAP4rev2`, RFC 9051 Appendix E)
    /// is available.
    /// SEARCH RETURN, SORT, and THREAD all reuse the SEARCH criteria grammar,
    /// so the same gates apply to every SEARCH-family command.
    pub(super) fn validate_search_criteria_capabilities(
        &self,
        criteria: &str,
    ) -> Result<(), Error> {
        self.require_condstore_for_modseq_criterion(criteria)?;

        let snap = self.state_rx.borrow();

        if (Self::search_criteria_contains_atom(criteria, "OLDER")
            || Self::search_criteria_contains_atom(criteria, "YOUNGER"))
            && !snap.capabilities.contains(&Capability::Within)
        {
            return Err(Error::MissingCapability("WITHIN".into()));
        }

        let is_rev2 = super::auth::is_rev2_from_snapshot(&snap);

        if ["SAVEDBEFORE", "SAVEDON", "SAVEDSINCE", "SAVEDATESUPPORTED"]
            .into_iter()
            .any(|atom| Self::search_criteria_contains_atom(criteria, atom))
            && !snap.capabilities.contains(&Capability::SaveDate)
            && !is_rev2
        {
            return Err(Error::MissingCapability("SAVEDATE".into()));
        }

        if ["EMAILID", "THREADID"]
            .into_iter()
            .any(|atom| Self::search_criteria_contains_atom(criteria, atom))
            && !snap.capabilities.contains(&Capability::ObjectId)
            && !is_rev2
        {
            return Err(Error::MissingCapability("OBJECTID".into()));
        }

        // Drop the borrow before calling require_searchres (which borrows again).
        drop(snap);

        if Self::search_criteria_contains_atom(criteria, "$") {
            self.require_searchres()?;
        }

        Ok(())
    }

    /// Returns `true` when `criteria` contains the given search-key atom
    /// outside quoted strings.
    ///
    /// SEARCH, SORT, and THREAD criteria are free-form IMAP search syntax, so
    /// extension keys such as `MODSEQ` can appear nested inside parenthesized
    /// groups. RFC 3501 Section 6.4.4 and RFC 9051 Section 6.4.4 also define
    /// many standard keys whose operands are `astring`, dates, numbers, or
    /// sequence-sets. The scanner therefore walks the SEARCH grammar and skips
    /// operands for known keys, so payloads like `HEADER Subject "MODSEQ"`,
    /// `BODY {12}\r\nhello MODSEQ`, and `BODY MODSEQ` do not trigger extension
    /// capability checks just because their data happens to equal a gated
    /// search-key atom.
    pub(super) fn search_criteria_contains_atom(criteria: &str, atom: &str) -> bool {
        let bytes = criteria.as_bytes();
        let mut i = 0usize;

        Self::search_criteria_skip_whitespace(bytes, &mut i);

        // RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4: SEARCH criteria
        // may start with an optional `CHARSET <astring>` prefix.
        let mut lookahead = i;
        if let Some(SearchCriteriaItem::Bare(token)) =
            Self::search_criteria_consume_item(criteria, bytes, &mut lookahead)
            && token.eq_ignore_ascii_case("CHARSET")
        {
            i = lookahead;
            let _ = Self::search_criteria_consume_item(criteria, bytes, &mut i);
        }

        while i < bytes.len() {
            if Self::search_criteria_contains_atom_in_key(criteria, bytes, &mut i, atom) {
                return true;
            }
            Self::search_criteria_skip_whitespace(bytes, &mut i);
        }

        false
    }

    /// RFC 3501 Section 4.3: a client literal is `{number}` or `{number+}`
    /// followed by CRLF and exactly `number` octets of data.
    ///
    /// SEARCH-family criteria can embed such literals because many search keys
    /// take `astring` operands (RFC 3501 Section 6.4.4, RFC 5256 Section 5).
    /// When scanning criteria for extension atoms, the literal payload must be
    /// skipped verbatim so its contents are not mistaken for syntax.
    pub(super) fn search_criteria_literal_end(bytes: &[u8], pos: usize) -> Option<usize> {
        if bytes.get(pos) != Some(&b'{') {
            return None;
        }

        let mut j = pos + 1;
        let digits_start = j;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        if j == digits_start {
            return None;
        }

        let digits_end = j;
        if j < bytes.len() && bytes[j] == b'+' {
            j += 1;
        }

        if j + 2 >= bytes.len()
            || bytes[j] != b'}'
            || bytes[j + 1] != b'\r'
            || bytes[j + 2] != b'\n'
        {
            return None;
        }

        let size = std::str::from_utf8(&bytes[digits_start..digits_end])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())?;
        let data_start = j + 3;
        let data_end = data_start.checked_add(size)?;
        (data_end <= bytes.len()).then_some(data_end)
    }

    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4: SEARCH keys are
    /// separated by linear whitespace.
    pub(super) fn search_criteria_skip_whitespace(bytes: &[u8], pos: &mut usize) {
        while *pos < bytes.len() && matches!(bytes[*pos], b' ' | b'\t' | b'\r' | b'\n') {
            *pos += 1;
        }
    }

    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4: a SEARCH program is a
    /// sequence of search keys, each of which can be a parenthesized group or
    /// an atom with zero or more operands.
    pub(super) fn search_criteria_contains_atom_in_key(
        criteria: &str,
        bytes: &[u8],
        pos: &mut usize,
        atom: &str,
    ) -> bool {
        Self::search_criteria_skip_whitespace(bytes, pos);

        if *pos >= bytes.len() {
            return false;
        }

        if bytes[*pos] == b'(' {
            *pos += 1;
            loop {
                Self::search_criteria_skip_whitespace(bytes, pos);
                if *pos >= bytes.len() {
                    return false;
                }
                if bytes[*pos] == b')' {
                    *pos += 1;
                    return false;
                }
                if Self::search_criteria_contains_atom_in_key(criteria, bytes, pos, atom) {
                    return true;
                }
            }
        }

        let item = Self::search_criteria_consume_item(criteria, bytes, pos);

        let Some(SearchCriteriaItem::Bare(token)) = item else {
            return false;
        };

        if token.eq_ignore_ascii_case(atom) {
            return true;
        }

        let upper = token.to_ascii_uppercase();
        match upper.as_str() {
            // 1-operand keys (astring or number/date)
            "BCC" | "BODY" | "CC" | "FROM" | "KEYWORD" | "SUBJECT" | "TEXT" | "TO"
            | "UNKEYWORD" | "EMAILID" | "THREADID" | "LARGER" | "SMALLER" | "BEFORE" | "ON"
            | "SINCE" | "SENTBEFORE" | "SENTON" | "SENTSINCE" | "OLDER" | "YOUNGER"
            | "SAVEDBEFORE" | "SAVEDON" | "SAVEDSINCE" | "UID" => {
                let _ = Self::search_criteria_consume_item(criteria, bytes, pos);
            }

            // HEADER takes two astring operands
            "HEADER" => {
                let _ = Self::search_criteria_consume_item(criteria, bytes, pos);
                let _ = Self::search_criteria_consume_item(criteria, bytes, pos);
            }

            // NOT takes one search-key operand (handled recursively)
            "NOT" => {
                return Self::search_criteria_contains_atom_in_key(criteria, bytes, pos, atom);
            }

            // OR takes two search-key operands (handled recursively)
            "OR" => {
                if Self::search_criteria_contains_atom_in_key(criteria, bytes, pos, atom) {
                    return true;
                }
                return Self::search_criteria_contains_atom_in_key(criteria, bytes, pos, atom);
            }

            // MODSEQ has a variable number of operands
            "MODSEQ" => {
                Self::search_criteria_consume_modseq_operands(criteria, bytes, pos);
            }

            // Sequence set or unknown key  -  treat as 0-operand
            _ => {}
        }

        false
    }

    /// Consume the next item from the search criteria stream.
    ///
    /// Returns `Some(SearchCriteriaItem::Bare(token))` for a bare atom,
    /// `Some(SearchCriteriaItem::Quoted)` for a quoted string,
    /// `Some(SearchCriteriaItem::Literal)` for a literal `{N}\r\n...`,
    /// or `None` at end-of-input.
    pub(super) fn search_criteria_consume_item<'a>(
        criteria: &'a str,
        bytes: &[u8],
        pos: &mut usize,
    ) -> Option<SearchCriteriaItem<'a>> {
        Self::search_criteria_skip_whitespace(bytes, pos);

        if *pos >= bytes.len() {
            return None;
        }

        if bytes[*pos] == b'"' {
            *pos += 1;
            while *pos < bytes.len() && bytes[*pos] != b'"' {
                if bytes[*pos] == b'\\' {
                    *pos += 1;
                }
                *pos += 1;
            }
            if *pos < bytes.len() {
                *pos += 1;
            }
            return Some(SearchCriteriaItem::Quoted);
        }

        if let Some(end) = Self::search_criteria_literal_end(bytes, *pos) {
            *pos = end;
            return Some(SearchCriteriaItem::Literal);
        }

        let start = *pos;
        while *pos < bytes.len()
            && !matches!(bytes[*pos], b' ' | b'\t' | b'\r' | b'\n' | b'(' | b')')
        {
            *pos += 1;
        }

        if *pos > start {
            Some(SearchCriteriaItem::Bare(&criteria[start..*pos]))
        } else {
            None
        }
    }

    /// RFC 7162 Section 3.1.5: `MODSEQ` is followed by either just
    /// `mod-sequence-valzer` or by `entry-name SP entry-type-req SP
    /// mod-sequence-valzer`. The gate only needs to skip those operands so
    /// later search keys continue to be parsed in the right position.
    pub(super) fn search_criteria_consume_modseq_operands(
        criteria: &str,
        bytes: &[u8],
        pos: &mut usize,
    ) {
        let Some(first) = Self::search_criteria_consume_item(criteria, bytes, pos) else {
            return;
        };

        match first {
            SearchCriteriaItem::Bare(value) if value.bytes().all(|b| b.is_ascii_digit()) => {}
            _ => {
                let _ = Self::search_criteria_consume_item(criteria, bytes, pos);
                let _ = Self::search_criteria_consume_item(criteria, bytes, pos);
            }
        }
    }

    /// `true` when the server advertises support for the named quota resource
    /// via `QUOTA=RES-<name>` (RFC 9208 Section 3.1.1).
    pub(super) fn has_quota_resource(&self, resource: &str) -> bool {
        let snap = self.state_rx.borrow();
        snap.capabilities.iter().any(|cap| {
            Self::quota_resource_name(cap).is_some_and(|name| name.eq_ignore_ascii_case(resource))
        })
    }

    /// Returns the quota resource name from `QUOTA=RES-<name>`
    /// capabilities (RFC 9208 Section 3.1.1).
    pub(super) fn quota_resource_name(cap: &Capability) -> Option<&str> {
        match cap {
            Capability::QuotaResource(name) => Some(name.as_str()),
            Capability::Other(s)
                if s.len() > "QUOTA=RES-".len()
                    && s[.."QUOTA=RES-".len()].eq_ignore_ascii_case("QUOTA=RES-") =>
            {
                Some(&s["QUOTA=RES-".len()..])
            }
            _ => None,
        }
    }

    /// Validate LIST-EXTENDED request syntax and capability requirements
    /// before sending a LIST command with selection options, multiple
    /// patterns, or return options (RFC 5258 Section 3 /
    /// RFC 9051 Section 6.3.9 / Appendix C).
    pub(super) fn validate_list_extended_request(
        &self,
        patterns: &[&str],
        selection_options: &[&str],
        return_options: &[&str],
    ) -> Result<(), Error> {
        if patterns.is_empty() {
            return Err(Error::Protocol(
                "LIST-EXTENDED requires at least one mailbox pattern \
                 (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                    .into(),
            ));
        }

        {
            let snap = self.state_rx.borrow();

            let is_rev2 = super::auth::is_rev2_from_snapshot(&snap);

            if patterns.len() > 1
                && !snap.capabilities.contains(&Capability::ListExtended)
                && !is_rev2
            {
                return Err(Error::MissingCapability("LIST-EXTENDED".into()));
            }

            // RFC 5258 Section 3 defines selection options and return options as
            // LIST-EXTENDED syntax on IMAP4rev1, even when the individual option
            // token comes from another extension such as RFC 6154 SPECIAL-USE or
            // RFC 5819 LIST-STATUS.
            let needs_list_extended = !selection_options.is_empty() || !return_options.is_empty();

            if needs_list_extended
                && !snap.capabilities.contains(&Capability::ListExtended)
                && !is_rev2
            {
                return Err(Error::MissingCapability("LIST-EXTENDED".into()));
            }

            for option in selection_options {
                let trimmed = option.trim();
                if trimmed.is_empty() {
                    return Err(Error::Protocol(
                        "LIST-EXTENDED selection options must not be empty \
                         (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                            .into(),
                    ));
                }
                if trimmed.eq_ignore_ascii_case("SPECIAL-USE")
                    && !snap.capabilities.contains(&Capability::SpecialUse)
                    && !is_rev2
                {
                    return Err(Error::MissingCapability("SPECIAL-USE".into()));
                }
            }
        }

        let has_recursivematch = selection_options
            .iter()
            .any(|option| option.trim().eq_ignore_ascii_case("RECURSIVEMATCH"));
        for option in return_options {
            let trimmed = option.trim();
            if trimmed.is_empty() {
                return Err(Error::Protocol(
                    "LIST-EXTENDED return options must not be empty \
                     (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                        .into(),
                ));
            }

            {
                let snap = self.state_rx.borrow();
                let is_rev2 = super::auth::is_rev2_from_snapshot(&snap);
                if trimmed.eq_ignore_ascii_case("SPECIAL-USE")
                    && !snap.capabilities.contains(&Capability::SpecialUse)
                    && !is_rev2
                {
                    return Err(Error::MissingCapability("SPECIAL-USE".into()));
                }
            }

            if let Some(status_items) =
                Self::list_status_return_option_items(trimmed).transpose()?
            {
                {
                    let snap = self.state_rx.borrow();
                    if !snap.capabilities.contains(&Capability::ListStatus)
                        && !super::auth::is_rev2_from_snapshot(&snap)
                    {
                        return Err(Error::MissingCapability("LIST-STATUS".into()));
                    }
                }
                self.validate_requested_status_items(status_items)?;
            }
        }

        if has_recursivematch
            && !selection_options.iter().any(|option| {
                let trimmed = option.trim();
                !trimmed.is_empty()
                    && !trimmed.eq_ignore_ascii_case("RECURSIVEMATCH")
                    && !trimmed.eq_ignore_ascii_case("REMOTE")
            })
        {
            return Err(Error::Protocol(
                "LIST-EXTENDED selection option RECURSIVEMATCH requires another \
                 non-REMOTE selection option (RFC 5258 Section 3 / \
                 RFC 9051 Section 6.3.9)"
                    .into(),
            ));
        }

        Ok(())
    }

    /// RFC 5819 Section 2 adds the reserved `STATUS (<items>)` return option
    /// to RFC 5258's `option-extension` grammar, so tokens that merely start
    /// with `STATUS` remain generic extension names rather than STATUS itself.
    pub(super) fn list_status_return_option_items(option: &str) -> Option<Result<&str, Error>> {
        let trimmed = option.trim();
        if !trimmed
            .get(..6)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("STATUS"))
        {
            return None;
        }

        // Only the exact STATUS keyword, followed by the reserved `SP "("`
        // sequence from RFC 5819 Section 2, is LIST-STATUS. A longer atom
        // such as STATUSX is still an RFC 5258 option-extension.
        match trimmed.as_bytes().get(6).copied() {
            Some(next) if next != b' ' && !next.is_ascii_whitespace() && next != b'(' => {
                return None;
            }
            _ => {}
        }

        Some(if let Some(suffix) = trimmed[6..].strip_prefix(" (") {
            if suffix.ends_with(')') && suffix.len() >= 2 {
                Ok(&suffix[1..suffix.len() - 1])
            } else {
                Err(Error::Protocol(
                    "LIST-EXTENDED STATUS return option must be STATUS (<items>) \
                 per RFC 5819 Section 2 / RFC 9051 Section 6.3.9"
                        .into(),
                ))
            }
        } else {
            Err(Error::Protocol(
                "LIST-EXTENDED STATUS return option must be STATUS (<items>) \
                 per RFC 5819 Section 2 / RFC 9051 Section 6.3.9"
                    .into(),
            ))
        })
    }

    /// Validate requested STATUS data items against the negotiated protocol
    /// version and advertised extensions before sending the command.
    ///
    /// RFC 3501 Section 6.3.10 defines the `IMAP4rev1` base items
    /// `MESSAGES`, `RECENT`, `UIDNEXT`, `UIDVALIDITY`, and `UNSEEN`.
    /// RFC 9051 Section 6.3.11 updates the `IMAP4rev2` base set to
    /// `MESSAGES`, `UIDNEXT`, `UIDVALIDITY`, `UNSEEN`, `DELETED`, and `SIZE`.
    /// RFC 9208 Section 4.1.4 additionally allows `DELETED` on
    /// `IMAP4rev1` when `QUOTA=RES-MESSAGE` is advertised and
    /// `DELETED-STORAGE` when `QUOTA=RES-STORAGE` is advertised.
    /// Additional items are gated by their respective extensions:
    /// `HIGHESTMODSEQ` (RFC 7162 Section 3.1.7), `APPENDLIMIT`
    /// (RFC 7889 Section 3), and `MAILBOXID` (RFC 8474 Section 5.1).
    pub(super) fn validate_requested_status_items(&self, items: &str) -> Result<(), Error> {
        for item in Self::status_item_tokens(items)? {
            match item.to_ascii_uppercase().as_str() {
                "RECENT" => {
                    if self.is_rev2() {
                        return Err(Error::Protocol(
                            "STATUS item RECENT was removed in IMAP4rev2 \
                             (RFC 9051 Section 6.3.11)"
                                .into(),
                        ));
                    }
                }
                "DELETED" => {
                    if !self.is_rev2() && !self.has_quota_resource("MESSAGE") {
                        return Err(Error::Protocol(
                            "STATUS item DELETED requires IMAP4rev2 or \
                             QUOTA=RES-MESSAGE (RFC 9051 Section 6.3.11 / \
                             RFC 9208 Section 4.1.4)"
                                .into(),
                        ));
                    }
                }
                "DELETED-STORAGE" => {
                    if !self.has_quota_resource("STORAGE") {
                        return Err(Error::MissingCapability("QUOTA=RES-STORAGE".into()));
                    }
                }
                "SIZE" => {
                    let snap = self.state_rx.borrow();
                    if !super::auth::is_rev2_from_snapshot(&snap)
                        && !snap.capabilities.contains(&Capability::StatusSize)
                    {
                        return Err(Error::MissingCapability("STATUS=SIZE".into()));
                    }
                }
                "HIGHESTMODSEQ" => self.require_condstore()?,
                "APPENDLIMIT" => {
                    let has_appendlimit = self
                        .state_rx
                        .borrow()
                        .capabilities
                        .iter()
                        .any(|cap| matches!(cap, Capability::AppendLimit(_)));
                    if !has_appendlimit {
                        return Err(Error::MissingCapability("APPENDLIMIT".into()));
                    }
                }
                "MAILBOXID" => {
                    let snap = self.state_rx.borrow();
                    if !snap.capabilities.contains(&Capability::ObjectId)
                        && !super::auth::is_rev2_from_snapshot(&snap)
                    {
                        return Err(Error::MissingCapability("OBJECTID".into()));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Validate requested FETCH data items against negotiated extensions
    /// before sending the command.
    ///
    /// RFC 3501 Section 6.4.5 allows extension data items in FETCH requests,
    /// but clients must not request them unless the corresponding extension
    /// has been advertised:
    /// - `MODSEQ` requires CONDSTORE/QRESYNC (RFC 7162 Section 3.1.5).
    /// - `PREVIEW` requires PREVIEW (RFC 8970 Section 4).
    /// - On `IMAP4rev1`, `BINARY[...]`, `BINARY.PEEK[...]`, and `BINARY.SIZE[...]`
    ///   require BINARY (RFC 3516 Sections 4.5.1-4.5.2).
    /// - On `IMAP4rev2`, those FETCH items are part of the base protocol
    ///   (RFC 9051 Appendix B).
    /// - `SAVEDATE` requires SAVEDATE (RFC 8514 Section 3).
    /// - `EMAILID` / `THREADID` require OBJECTID (RFC 8474 Sections 4 and 7).
    pub(super) fn validate_requested_fetch_items(&self, items: &[FetchAttr]) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        let is_rev2 = super::auth::is_rev2_from_snapshot(&snap);
        for item in items {
            match item {
                // RFC 7162 Section 3.1.5: MODSEQ requires CONDSTORE/QRESYNC.
                FetchAttr::ModSeq => {
                    if !snap.capabilities.contains(&Capability::Condstore)
                        && !snap.capabilities.contains(&Capability::QResync)
                    {
                        return Err(Error::MissingCapability("CONDSTORE".into()));
                    }
                }
                // RFC 8970 Section 4: PREVIEW requires PREVIEW capability.
                FetchAttr::Preview | FetchAttr::PreviewLazy => {
                    if !snap.capabilities.contains(&Capability::Preview) {
                        return Err(Error::MissingCapability("PREVIEW".into()));
                    }
                }
                // RFC 8514 Section 3: SAVEDATE requires SAVEDATE capability.
                FetchAttr::SaveDate => {
                    if !snap.capabilities.contains(&Capability::SaveDate) && !is_rev2 {
                        return Err(Error::MissingCapability("SAVEDATE".into()));
                    }
                }
                // RFC 8474 Sections 4 and 7: EMAILID/THREADID require OBJECTID.
                FetchAttr::EmailId | FetchAttr::ThreadId => {
                    if !snap.capabilities.contains(&Capability::ObjectId) && !is_rev2 {
                        return Err(Error::MissingCapability("OBJECTID".into()));
                    }
                }
                FetchAttr::GmailMsgId | FetchAttr::GmailThreadId => {
                    if !snap.capabilities.contains(&Capability::XGmExt1) {
                        return Err(Error::MissingCapability("X-GM-EXT-1".into()));
                    }
                }
                // RFC 9051 Appendix B: IMAP4rev2 folds the FETCH side of
                // RFC 3516 into the base protocol, so explicit BINARY is
                // only required on IMAP4rev1.
                FetchAttr::Binary { .. } | FetchAttr::BinarySize { .. }
                    if !snap.capabilities.contains(&Capability::Binary) && !is_rev2 =>
                {
                    return Err(Error::MissingCapability("BINARY".into()));
                }
                FetchAttr::Binary { .. } | FetchAttr::BinarySize { .. } => {}
                _ => {}
            }
        }
        Ok(())
    }

    /// Parse a STATUS data item list into individual item atoms.
    ///
    /// Accepts either a raw space-separated list (`"MESSAGES UNSEEN"`) or a
    /// parenthesized `status-att-list` (`"(MESSAGES UNSEEN)"`). The list must
    /// contain at least one item, and nested or unbalanced parentheses are
    /// rejected per RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11.
    pub(super) fn status_item_tokens(items: &str) -> Result<Vec<&str>, Error> {
        let trimmed = items.trim();
        if trimmed.is_empty() {
            return Err(Error::Protocol(
                "STATUS item list must contain at least one data item \
                 (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    .into(),
            ));
        }

        let body = match (trimmed.strip_prefix('('), trimmed.strip_suffix(')')) {
            (Some(without_open), Some(_)) => {
                let inner = without_open
                    .strip_suffix(')')
                    .ok_or_else(|| {
                        Error::Protocol(
                            "STATUS item list must use balanced parentheses \
                             (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                                .into(),
                        )
                    })?
                    .trim();
                if inner.is_empty() {
                    return Err(Error::Protocol(
                        "STATUS item list must contain at least one data item \
                         (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                            .into(),
                    ));
                }
                inner
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(Error::Protocol(
                    "STATUS item list must use balanced parentheses \
                     (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                        .into(),
                ));
            }
            (None, None) => trimmed,
        };

        if body.contains('(') || body.contains(')') {
            return Err(Error::Protocol(
                "STATUS item list must be flat and must not contain nested parentheses \
                 (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    .into(),
            ));
        }

        let tokens: Vec<&str> = body.split_ascii_whitespace().collect();
        if tokens.is_empty() {
            return Err(Error::Protocol(
                "STATUS item list must contain at least one data item \
                 (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    .into(),
            ));
        }
        Ok(tokens)
    }

    /// Check if the server supports non-synchronizing literals of the given size.
    ///
    /// RFC 7888 Section 4: LITERAL+ allows any size.
    /// RFC 7888 Section 5 / RFC 9051 Section 4.3: LITERAL- style
    /// non-synchronizing literals are limited to 4096 octets.
    /// RFC 9051 Appendix E item 2 folds LITERAL- into pure `IMAP4rev2`.
    pub(super) fn supports_non_sync_literal(&self, size: usize) -> bool {
        let snap = self.state_rx.borrow();
        snap.capabilities.contains(&Capability::LiteralPlus)
            || ((snap.capabilities.contains(&Capability::LiteralMinus)
                || super::auth::is_rev2_from_snapshot(&snap))
                && size <= 4096)
    }

    /// Determine the APPEND literal syntax required for `message`.
    ///
    /// RFC 3501 Section 4.3 / RFC 9051 Section 4.3: classic literals carry
    /// `CHAR8`, which excludes NUL octets.
    /// RFC 3516 Section 4.4: APPEND data containing NULs requires `literal8`
    /// and the `BINARY` capability.
    /// RFC 6855 Section 4: after ENABLE UTF8=ACCEPT, APPEND message data with
    /// UTF-8 headers must use the `UTF8 (literal8)` wrapper.
    pub(super) fn append_literal_kind(&self, message: &[u8]) -> Result<AppendLiteralKind, Error> {
        if self.utf8_enabled() {
            return Ok(AppendLiteralKind::Utf8Literal8);
        }

        if message.contains(&0) {
            let snap = self.state_rx.borrow();
            if snap.capabilities.contains(&Capability::Binary) {
                Ok(AppendLiteralKind::Literal8)
            } else {
                Err(Error::Protocol(
                    "APPEND data containing NUL requires BINARY literal8 support \
                     (RFC 3516 Section 4.4)"
                        .into(),
                ))
            }
        } else {
            Ok(AppendLiteralKind::Literal)
        }
    }

    /// Check if the server supports non-synchronizing literal8 of the given size.
    ///
    /// RFC 7888 Section 6: on `IMAP4rev1`, literal8 may use the
    /// non-synchronizing form only when BOTH BINARY and a compatible
    /// literal extension are advertised.
    ///
    /// RFC 9051 Section 9 redefines `literal8` for pure `IMAP4rev2` as
    /// `~{" number64 "}" CRLF *OCTET`, with no `+` modifier, so rev2
    /// literal8 is always synchronizing.
    pub(super) fn supports_non_sync_literal8(&self, size: usize) -> bool {
        let snap = self.state_rx.borrow();
        if !snap.capabilities.contains(&Capability::Binary)
            || super::auth::is_rev2_from_snapshot(&snap)
        {
            return false;
        }

        snap.capabilities.contains(&Capability::LiteralPlus)
            || (snap.capabilities.contains(&Capability::LiteralMinus) && size <= 4096)
    }

    /// Check if this APPEND literal form may use the non-synchronizing marker.
    ///
    /// RFC 7888 Sections 4-6: classic literals follow LITERAL+/LITERAL- rules,
    /// while `literal8` requires both `BINARY` and a compatible literal
    /// extension for the `+` suffix.
    pub(super) fn append_literal_is_non_sync(&self, kind: AppendLiteralKind, size: usize) -> bool {
        match kind {
            AppendLiteralKind::Literal => self.supports_non_sync_literal(size),
            AppendLiteralKind::Literal8 | AppendLiteralKind::Utf8Literal8 => {
                self.supports_non_sync_literal8(size)
            }
        }
    }

    /// Whether `UTF8=ACCEPT` has been enabled (RFC 6855 Section 3).
    ///
    /// Derived from the connection state snapshot. Does NOT include
    /// `IMAP4rev2`  -  use `utf8_mode()` for the combined check.
    pub(super) fn utf8_enabled(&self) -> bool {
        self.state_rx
            .borrow()
            .enabled
            .iter()
            .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT"))
    }

    /// Determine the [`LiteralMode`] based on the server's advertised capabilities.
    ///
    /// RFC 7888 Section 4: LITERAL+  -  non-synchronizing literals of any size.
    /// RFC 7888 Section 5: LITERAL-  -  non-synchronizing literals up to 4096 bytes.
    /// RFC 9051 Appendix E item 2 / Section 4.3: pure `IMAP4rev2` includes the
    /// same 4096-octet non-synchronizing literal behavior as LITERAL-.
    /// RFC 3501 Section 4.3: otherwise, literals are synchronizing.
    pub(super) fn literal_mode(&self) -> LiteralMode {
        let snap = self.state_rx.borrow();
        if snap.capabilities.contains(&Capability::LiteralPlus) {
            LiteralMode::LiteralPlus
        } else if snap.capabilities.contains(&Capability::LiteralMinus)
            || super::auth::is_rev2_from_snapshot(&snap)
        {
            LiteralMode::LiteralMinus
        } else {
            LiteralMode::Synchronizing
        }
    }

    // -----------------------------------------------------------------------
    // NOOP (RFC 3501 Section 6.1.2 / RFC 9051 Section 6.1.2)
    // -----------------------------------------------------------------------

    /// NOOP  -  no operation (RFC 3501 Section 6.1.2 / RFC 9051 Section 6.1.2).
    ///
    /// Sends a NOOP command to the server. Since the NOOP command can
    /// include unsolicited responses from the server, this is the
    /// recommended method for polling for new messages or status updates.
    ///
    /// Valid in any state (RFC 3501 Section 6.1.2). Returns an error only
    /// if the connection is closed (e.g. after LOGOUT).
    pub async fn noop(&self, timeout: Duration) -> Result<(), Error> {
        tokio::time::timeout(
            timeout,
            self.submit_regular(Command::Noop, super::dispatch::TaggedOkConsumer::default()),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }

    // -----------------------------------------------------------------------
    // CHECK (RFC 3501 Section 6.4.1)
    // -----------------------------------------------------------------------

    /// CHECK  -  request a checkpoint of the currently selected mailbox
    /// (RFC 3501 Section 6.4.1).
    ///
    /// Implementation-defined server action; typically flushes internal
    /// state to persistent storage.
    ///
    /// **`IMAP4rev1` only.** RFC 9051 removed the CHECK command from
    /// `IMAP4rev2`. On a pure rev2 connection, use [`noop`](Self::noop)
    /// instead  -  this method returns [`Error::Protocol`].
    pub async fn check(&self, timeout: Duration) -> Result<(), Error> {
        self.require_state(&[SessionState::Selected])?;
        // RFC 9051 removed CHECK; reject when rev2 behavior is active.
        // `is_rev2_from_snapshot` already handles dual-mode servers: it
        // returns true only after `ENABLE IMAP4rev2` (or on pure-rev2
        // servers). Once rev2 is active, CHECK is undefined.
        if self.is_rev2() {
            return Err(Error::Protocol(
                "CHECK is not defined in IMAP4rev2 (RFC 9051); use NOOP instead".into(),
            ));
        }
        tokio::time::timeout(
            timeout,
            self.submit_regular(Command::Check, super::dispatch::TaggedOkConsumer::default()),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }

    // -----------------------------------------------------------------------
    // CAPABILITY (RFC 3501 Section 6.1.1 / RFC 9051 Section 6.1.1)
    // -----------------------------------------------------------------------

    /// CAPABILITY  -  force a capability round-trip (RFC 3501 Section 6.1.1 /
    /// RFC 9051 Section 6.1.1).
    ///
    /// Sends an explicit CAPABILITY command and returns the server's
    /// response. Unlike [`capabilities`](Self::capabilities), which
    /// returns the cached capability list, this method always performs a
    /// network round-trip and updates the cached state.
    ///
    /// Valid in any state (RFC 3501 Section 6.1.1).
    pub async fn capability(&self, timeout: Duration) -> Result<Vec<Capability>, Error> {
        tokio::time::timeout(
            timeout,
            self.submit_regular(
                Command::Capability,
                super::dispatch::CapabilityConsumer::default(),
            ),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }
}

/// Item from a SEARCH criteria scan.
pub(super) enum SearchCriteriaItem<'a> {
    Bare(&'a str),
    Quoted,
    Literal,
}
