# bifrost-imap `codec/**` - bug hunt and test sweep

Scope: `crates/imap/src/codec/**`, exclusively (decode, encode, UTF-7,
response classification). Written against the tree as of this sweep;
nothing outside the scope was edited.

Two halves, kept separate on purpose:

- **Tests landed.** They pin behavior *as it exists today*. Where a test
  pins behavior I believe is wrong, its doc comment says
  `DOCUMENTS A BUG, NOT AN ENDORSEMENT` and names the finding ID below.
- **Bugs reported, not fixed.** Each has a proposed fix; triage is the
  orchestrator's.

Finding IDs (`C1`..`C7`) are stable references, not a severity rank; the
document is ordered by severity and the IDs run out of numeric order at
the end.

## 0. Coverage survey (verified, not assumed)

This subtree is the best-covered in the crate, and the previous sweep
(`plans/bug-hunt-2026-06-17.md`) clearly worked it hard: overflow,
truncation, nesting-depth, `checked_add`, and token-boundary cases are
already pinned throughout.

| file | tests | notes |
|---|---|---|
| `decode/tests.rs` | 867 | incl. a `prop_invariants` module |
| `encode/tests.rs` | 382 | incl. a `prop_roundtrip` module |
| `utf7_tests.rs` | 34 | incl. a `prop_invariants` module |
| `classification_tests.rs` | 27 | table-driven over the truth table |

**The brief's claim that "`proptest` is currently unused across the
crate" is stale.** It is used in all three of `decode/tests.rs`,
`encode/tests.rs`, and `utf7_tests.rs`. Existing properties: parser
never panics / always consumes / never over-consumes, SEARCH UIDs
non-zero, whitespace-injection tolerance, quoted-vs-unquoted media
types, output-case normalization, command well-formedness, LITERAL+
single-segment, tagged/greeting round trips, and MUTF-7 round-trip for
names without NUL/CR/LF. I extended rather than duplicated these.

I therefore weighted almost everything toward the bug hunt, as asked.

## 0.1 Sibling check against the `connection/**` findings

The brief asked me to look for siblings of two `connection/**` framing
defects. Results:

- **`}` must abut the CRLF** (connection B4). **Not present here.**
  `encode/core.rs::find_sync_literal_boundary` explicitly requires
  `buf[j] == b'}'` immediately after the digit run and `\r\n`
  immediately after that. `decode`'s five literal scanners
  (`primitives::literal`, `skip_balanced_parens`,
  `envelope_fetch::scan_section_spec`, `skip_paren_group`,
  `extensions::try_skip_literal`) all require the same abutment.
  Pinned by the new
  `brace_digits_not_abutting_crlf_is_not_a_literal_marker`.

- **Unchecked `pos + literal_len`** (connection B5). **Present**, in
  `encode/core.rs::from_flat_buffer` - see **C7** below. Every *decode*
  site already uses `checked_add` plus a bounds check; that half was
  fixed by the previous sweep and has regression tests
  (`skip_balanced_parens_literal_checked_add_overflow`,
  `try_skip_literal_overflow_returns_none`,
  `scan_section_spec_literal_overflow_does_not_panic`,
  `skip_paren_group_literal_overflow_no_wrap`).

A third sibling turned up unprompted: `connection/helpers.rs`'s
`list_status_return_option_items` (connection B6) is **duplicated
verbatim, with the identical off-by-one**, in
`codec/encode/commands/list.rs` - see **C3**.

---

# Bugs

## C1 - `from_flat_buffer` rescans non-synchronizing literal payloads, producing a bogus segment split that deadlocks the send path

**Severity: high.** **Where:** `codec/encode/core.rs`,
`EncodedCommand::from_flat_buffer` + `find_sync_literal_boundary`.
Consumed by `connection/driver/wire_send.rs::send_encoded_segments` and
`connection/driver/pipeline.rs`.

**Path to failure.**

1. `find_sync_literal_boundary` matches only *synchronizing* markers:
   the digit run must be followed directly by `}`. A LITERAL+ marker
   `{N+}\r\n` (RFC 7888 Section 4) has `+` there, so it does not match.
2. Because it did not match, `from_flat_buffer` never learns the
   payload length and so never advances `scan_pos` past the payload -
   it just does `i += 1` and keeps scanning **into the literal body**.
3. Any `{digits}\r\n` occurring inside that body is then read as a
   synchronizing literal marker, and the command is split there.
4. `send_encoded_segments` writes segment 0 and calls
   `wait_for_continuation`. Under LITERAL+ the server is counting down
   N octets of a non-synchronizing literal and has no reason to emit
   `+`, so the wait never completes.

The same applies to `LiteralMode::LiteralMinus` for any literal <= 4096
octets (RFC 7888 Section 5), which is the common case.

**Concrete input.** `SETMETADATA` of an opaque value containing the byte
sequence `{5}\r\n`:

```rust
Command::SetMetadata {
    mailbox: MailboxName::new("INBOX").unwrap(),
    entries: vec![("/private/x".into(), Some(b"a{5}\r\nbbbbb".to_vec()))],
}
```

encodes to

```
A001 SETMETADATA "INBOX" ("/private/x" {11+}\r\na{5}\r\nbbbbb)\r\n
```

and `from_flat_buffer` returns two segments split after `a{5}\r\n`.

**Reachability.** Every payload that reaches the literal encoder and is
not CR/LF-free:

- `SETMETADATA` values - arbitrary application bytes
  (`string_helpers::encode_metadata_value`).
- SEARCH / SORT / THREAD criteria - `validate_search_criteria_crlf`
  deliberately permits caller-written literals, and `account/pim.rs`
  `search_plan()` appends `SearchRequest::provider_query` verbatim.
- LOGIN password - `validate_login_credential_ascii` only rejects
  non-ASCII, so CR and LF pass and force the literal form.
- `ID` values (RFC 2971).

Mailbox names are *not* reachable: `MailboxName::new` rejects CR/LF, so
a mailbox literal cannot contain a marker.

**Result.** The caller's `command_timeout` fires; per
`plans/bugs-imap-connection.md` B3/B2 the driver task itself has no
timeout, so the connection is wedged with a half-written command.

**What should happen instead.** Under `LiteralMode::LiteralPlus` every
literal is non-synchronizing, so the command must be exactly one
segment regardless of payload content. Under `LiteralMinus` only
literals > 4096 octets may split.

**Note:** the property `prop_roundtrip::literal_plus_single_segment`
already states exactly this invariant, but its generator
(`arb_simple_command`) never produces a command carrying a literal, so
it is vacuously true today and did not catch this.

**Proposed fix.** Teach the scanner about both marker forms and always
skip the payload. `codec/encode/mod.rs::validate_search_criteria_crlf`
already does exactly this (`has_plus` -> `i = data_end`); the two
should share one scanner.

```rust
/// Returns (offset past the marker, payload octets, is_synchronizing).
fn find_literal_marker(buf: &[u8]) -> Option<(usize, usize, bool)> {
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'{' {
            let start = i + 1;
            let mut j = start;
            while j < buf.len() && buf[j].is_ascii_digit() {
                j += 1;
            }
            if j > start {
                let non_sync = buf.get(j) == Some(&b'+');
                let close = if non_sync { j + 1 } else { j };
                if close + 2 < buf.len()
                    && buf[close] == b'}'
                    && buf[close + 1] == b'\r'
                    && buf[close + 2] == b'\n'
                    && let Ok(s) = std::str::from_utf8(&buf[start..j])
                    && let Ok(size) = s.parse::<usize>()
                {
                    return Some((close + 3, size, !non_sync));
                }
            }
        }
        i += 1;
    }
    None
}

pub(super) fn from_flat_buffer(buf: &[u8]) -> Self {
    let mut segments = Vec::new();
    let mut seg_start = 0;
    let mut scan_pos = 0;

    while scan_pos < buf.len() {
        let Some((marker_end_rel, size, sync)) =
            find_literal_marker(&buf[scan_pos..])
        else {
            break;
        };
        let abs_marker_end = scan_pos + marker_end_rel;
        // C7: a marker whose payload overruns the buffer, or whose
        // declared size overflows usize, is not a boundary.
        let Some(payload_end) = abs_marker_end
            .checked_add(size)
            .filter(|&end| end <= buf.len())
        else {
            break;
        };
        if sync {
            segments.push(BytesMut::from(&buf[seg_start..abs_marker_end]));
            seg_start = abs_marker_end;
        }
        scan_pos = payload_end;
    }

    if seg_start < buf.len() {
        segments.push(BytesMut::from(&buf[seg_start..]));
    }
    Self { segments }
}
```

`scan_pos = payload_end` also guarantees strict progress, which closes
C7's infinite-loop variant.

**Tests landed.**
`encode/tests.rs::literal_plus_body_containing_sync_marker_is_wrongly_split`
pins the wrong 2-segment result;
`sync_literal_body_containing_sync_marker_is_skipped` is the control
case showing the synchronizing form handles the same bytes correctly.

---

## C2 - `decode_rfc2047` is quadratic in the header length on a crafted Subject

**Severity: high** (remote, unauthenticated, attacker-controlled).
**Where:** `codec/decode/encoded_words.rs::parse_encoded_word_inner`.

```rust
let q1 = remaining.find('?')?;      // unbounded
let q2 = remaining.find('?')?;      // unbounded
let end = remaining.find("?=")?;    // unbounded, and usually fails
```

**Path to failure.** RFC 2047 Section 2 defines

```text
encoded-word = "=?" charset "?" encoding "?" encoded-text "?="
token        = 1*<Any CHAR except SPACE, CTLs, and especials>
encoded-text = 1*<printable ASCII other than "?" or SPACE>
```

so an encoded word cannot contain SPACE, a control character, or a
non-ASCII octet. The parser nevertheless searches for the closing `?=`
across the **entire remaining header value**. The outer loop in
`decode_rfc2047_str` restarts at every `=?`, so a value built from many
`=?` candidates that never close costs O(n) per candidate.

**Concrete input.** `"=? ".repeat(k)`:

- Iteration i finds `=?` at offset 1 of the remaining slice, `before`
  is `" "`, which ends in whitespace, so `candidate_has_valid_prefix`
  is true and `parse_encoded_word` is entered.
- `find('?')` succeeds twice (charset `" ="`, encoding `" ="`).
- `find("?=")` then scans the whole remainder and fails - the string
  contains `"? "` and `"=?"` but never `"?="`.
- The candidate is rejected, `=?` is emitted verbatim, and the loop
  advances 3 bytes.

Total work is `k` full scans over `n = 3k` bytes, i.e. O(n^2).

**Reachability.** `decode_rfc2047` runs on:

- `ENVELOPE` subject (`envelope_fetch.rs::envelope`),
- every `ENVELOPE` address display name (`envelope_fetch.rs::address`),
- every `BODYSTRUCTURE` Content-Description
  (`bodystructure.rs::body_type_single`).

All three are written by the *message sender*, not by the server
operator. Anyone who can send the user an email controls them. Postfix's
default `header_size_limit` is 102 400 octets, so a single Subject can
carry ~34 000 candidates over ~100 KB: ~3.5e9 byte comparisons per
`FETCH ENVELOPE` of that message, repeated on every re-hydration.
A server with no header cap makes it arbitrarily worse.

**What should happen instead.** The scan must be bounded by the length
of the candidate, not the length of the header value.

**Proposed fix.** Clamp the search window to the first byte that cannot
appear inside an encoded word. Bytes outside `33..=126` include SPACE,
all CTLs, DEL, and every UTF-8 lead/continuation byte, so the position
of the first such byte is always a `char` boundary and the slice is
safe:

```rust
fn parse_encoded_word_inner(remaining: &mut &str) -> Option<String> {
    // RFC 2047 Section 2: charset, encoding and encoded-text are all
    // built from printable non-space ASCII, so an encoded word cannot
    // extend past the first SPACE / CTL / non-ASCII octet. Bounding the
    // search keeps it linear in the candidate rather than in the whole
    // header value.
    let window_len = remaining
        .bytes()
        .position(|b| !(33..=126).contains(&b))
        .unwrap_or(remaining.len());
    let window = &remaining[..window_len];

    let q1 = window.find('?')?;
    // ... all three searches now run against `window`; on success
    //     advance `*remaining` by the consumed byte count.
}
```

RFC 2047 Section 2 also caps an encoded word at 75 characters; the
decoder deliberately tolerates overlong words (Postel), and the window
bound is sufficient without reintroducing that limit.

**Tests landed.**
`decode/tests.rs::rfc2047_repeated_shift_prefix_is_passed_through_verbatim`
(the quadratic shape, at a deliberately small repetition count) and
`rfc2047_adjacent_shift_prefixes_are_passed_through_verbatim` (the
`"=?x"` shape, which takes the cheap branch because
`candidate_has_valid_prefix` is false from the second candidate onward -
included so a future fix does not accidentally "fix" only the cheap
case). Both assert verbatim passthrough per RFC 2047 Section 6.3.

I did not land a timing assertion: it would be flaky and the policy is
hermeticity.

---

## C7 - `from_flat_buffer`'s `scan_pos = abs_marker_end + literal_size` is unchecked

**Severity: high.** **Where:** `codec/encode/core.rs:221`.

```rust
scan_pos = abs_marker_end + literal_size;
```

`find_sync_literal_boundary` returns whatever `str::parse::<usize>()`
accepts, so `literal_size` can be `usize::MAX`.

**Path to failure.** This is only reachable *because of C1*: the byte
sequence has to be inside a literal payload the scanner should have
skipped. Combine the two and:

- **Debug / `brokkr check`:** `attempt to add with overflow` panic
  inside the driver task -> `Error::DriverPanicked`.
- **Release:** silent wraparound. Choose the count so the wrap lands
  exactly back on the `{`:

  `{18446744073709551592}\r\n` is 24 octets, and
  `18446744073709551592 == 2^64 - 24`, so
  `scan_pos = abs_marker_end + (2^64 - 24) == marker_start`.
  The next iteration re-finds the same marker at relative offset 0,
  pushes another 24-byte segment, and sets `scan_pos` back to
  `marker_start` again - **an unbounded loop that allocates a
  `BytesMut` per iteration until the process is OOM-killed.**

**Concrete input.** Under LITERAL+ or LITERAL-, any payload containing
the 24 ASCII bytes `{18446744073709551592}\r\n`. Reachable through the
same four payload sources listed in C1; the SEARCH-criteria path is the
sharpest, because `validate_search_criteria_crlf` **correctly** skips
non-synchronizing payloads and therefore never inspects those bytes:

```text
SUBJECT {24+}\r\n{18446744073709551592}\r\n
```

passes validation and reaches `from_flat_buffer` intact.

**What should happen instead.** An unrepresentable or over-long literal
length means "not a boundary", not "wrap".

**Proposed fix.** Included in the C1 patch above:
`abs_marker_end.checked_add(size).filter(|&e| e <= buf.len())`, plus
the `scan_pos = payload_end` progress guarantee.

**No test landed.** The behavior is profile-dependent (panic in debug,
loop in release), so there is nothing stable to pin. Once fixed, the
natural test is that
`Command::SetMetadata` with value `b"{18446744073709551592}\r\n"` under
`LiteralMode::LiteralPlus` yields exactly one segment.

---

## C3 - `list_status_return_option_items` eats the first octet of the STATUS item list

**Severity: medium.** **Where:**
`codec/encode/commands/list.rs::list_status_return_option_items`.

```rust
Some(if let Some(suffix) = option[6..].strip_prefix(" (") {
    if suffix.ends_with(')') && suffix.len() >= 2 {
        Ok(&suffix[1..suffix.len() - 1])
```

`strip_prefix(" (")` has already removed the opening paren, and then the
remainder is sliced `[1..len-1]` as if it had not been. For
`option = "STATUS (" + X + ")"` the function returns `X[1..]` - the
item list minus its first character.

This is the same defect as `plans/bugs-imap-connection.md` B6, in a
second, independent copy of the helper. Fixing one and not the other
just re-establishes the drift.

**Path to failure, two symptoms.**

1. **False reject.** A one-character item list degenerates to `""`:
   `list_extended(.., &[], &["STATUS (X)"])` is rejected with
   "must contain at least one status data item" even though the list is
   not empty.

2. **False accept.** The only check that survives the truncation is
   `normalize_status_items_body`'s "no parentheses in the item list",
   and hiding the first character hides a *leading* `(`. For
   `option = "STATUS ((MESSAGES)"` the item list is `X = "(MESSAGES"`;
   the helper returns `X[1..] == "MESSAGES"`, the paren check passes,
   and the option is then emitted **verbatim** (the encoder writes
   `option.trim()`, not the parsed value), producing

   ```
   A001 LIST "" "*" RETURN (STATUS ((MESSAGES))\r\n
   ```

   which is unbalanced per RFC 5819 Section 4 / RFC 9051 Section 7.

**Severity note.** The codec copy is milder than the connection copy:
`normalize_status_items_body` only checks emptiness and nested parens,
whereas `connection/helpers.rs::validate_requested_status_items` gates
capabilities per item, so the truncation there kills a real guard on the
first item (RECENT on rev2, HIGHESTMODSEQ without CONDSTORE, etc.).

**Proposed fix.** Drop the redundant slice and the `len() >= 2` special
case - `strip_suffix` handles both:

```rust
Some(if let Some(suffix) = option[6..].strip_prefix(" (") {
    match suffix.strip_suffix(')') {
        Some(items) => Ok(items),
        None => Err(crate::Error::Protocol(
            "LIST-EXTENDED STATUS return option must be STATUS (<items>) \
             per RFC 5819 Section 4 / RFC 9051 Section 7"
                .into(),
        )),
    }
} else {
    Err(/* same */)
})
```

`"STATUS ()"` still errors correctly (empty item list). Better still,
lift one copy of this helper (and `normalize_status_items_body`) into a
shared module so `connection` and `codec` cannot drift again.

**Tests landed.**
`encode/tests.rs::list_status_return_option_rejects_one_character_item_list`
and `list_status_return_option_accepts_unbalanced_leading_paren`.

---

## C4 - Any parse failure inside a recognized untagged response is laundered into `Unknown`, with no diagnostic

**Severity: medium.** **Where:** `codec/decode/response.rs::parse_untagged`
(the final `alt((parse_untagged_thread, parse_untagged_unknown))`) plus
`codec/classification.rs:290`.

**Path to failure.** `parse_untagged_unknown` is an unconditional
catch-all that consumes to the response-terminating CRLF and always
succeeds. Every keyword-specific parser returns `nom::Err::Error`
(not `Failure`) on any internal problem, so `alt` falls through to the
catch-all. `classify(_, UR::Unknown(_))` then returns `OnlyUnsolicited`,
so the response is routed away from the in-flight command's consumer.

There is **no `tracing` event on this path at all** - not warn, not
debug. A response the codec could not parse is indistinguishable from a
genuine extension response the codec does not know.

**Concrete inputs.**

- `* STATUS "INBOX" (MESSAGES)\r\n` (RFC 3501 Section 7.2.4 requires
  `status-att SP number`) becomes
  `Unknown("STATUS \"INBOX\" (MESSAGES)")`. The STATUS command completes
  with no status data and no error.
- `* 1 FETCH (UID 9 ENVELOPE (NIL NIL) FLAGS (\Seen))\r\n` (short
  ENVELOPE) becomes `Unknown(..)`, discarding the UID and FLAGS that
  parsed cleanly.

**Why it matters.** In a QRESYNC/CONDSTORE sync a silently-dropped FETCH
is a message that never gets hydrated, and the cursor still advances -
so the loss is permanent until UIDVALIDITY changes. A hard parse error
would at least be retryable.

Three parsers *do* use `nom::Err::Failure` to defeat the catch-all
(leading-zero sequence numbers, BODYSTRUCTURE depth, empty multipart),
which shows the mechanism is understood; it is just not applied to the
general case.

**What should happen instead.** At minimum, the degradation must be
observable. Ideally, a response whose *keyword* is recognized but whose
body did not parse should be distinguishable from a genuinely unknown
extension response.

**Proposed fix (minimal).** In `parse_untagged_unknown`, emit a
`tracing::warn!` when the captured raw text begins with a keyword the
codec claims to implement:

```rust
const KNOWN_KEYWORDS: &[&str] = &[
    "STATUS", "LIST", "LSUB", "FETCH", "SEARCH", "SORT", "THREAD",
    "ESEARCH", "NAMESPACE", "QUOTA", "QUOTAROOT", "ACL", "MYRIGHTS",
    "LISTRIGHTS", "METADATA", "ENABLED", "VANISHED", "ID", "CAPABILITY",
    "FLAGS",
];
```

(the numbered forms need a `<digits> SP <keyword>` check). A larger fix
adds an `UntaggedResponse::Malformed { keyword, raw }` variant that
`classify` routes as `Impossible`, so the dispatcher's existing
non-conformance accounting sees it - but that is a `types` change,
outside this scope.

**Tests landed.**
`decode/tests.rs::malformed_status_silently_degrades_to_unknown_response`
and `malformed_fetch_envelope_silently_degrades_to_unknown_response`.

---

## C5 - A response-code numeric overflow discards the entire response code, not just the value

**Severity: low.** **Where:**
`codec/decode/flags_caps.rs::response_code_inner`
(`UIDNEXT`, `UIDVALIDITY`, `UNSEEN`, `HIGHESTMODSEQ`, `APPENDUID`,
`COPYUID`, `MODIFIED`, `METADATA LONGENTRIES/MAXSIZE`), reached through
`resp_text`'s `opt(response_code)`.

**Path to failure.** `number` / `number64` return `Err` on overflow.
`opt` backtracks the *whole bracket group*, so
`* OK [UIDNEXT 4294967296] Predicted next UID` yields
`code: None, text: "[UIDNEXT 4294967296] Predicted next UID"` - the
bracket text is silently reclassified as human-readable prose.

**Why it is a defect and not a policy.** The STATUS parser
(`response.rs::status_items`) hit the identical problem and was fixed
the other way: it uses `number_tolerant` / `number64_tolerant`, drops
only the offending item, and keeps every sibling item. Twelve comments
in that function cite Postel's law for exactly this. The two paths now
disagree about what "tolerate an oversized number" means.

**Practical impact is small** - a `UIDVALIDITY` above `u32::MAX` is a
broken server - but the failure is invisible: nothing logs, and a
consumer looking for `[UIDNEXT ...]` just does not find it.

**Proposed fix.** On numeric-parse failure, fall back to preserving the
code name rather than losing the whole group:

```rust
"UIDNEXT" => {
    let (rest, _) = sp(input)?;
    let (rest, val) = number_tolerant(rest)?;
    match val {
        Some(n) => Ok((rest, ResponseCode::UidNext(n))),
        // Postel: keep the code name, surface the raw text.
        None => Ok((rest, ResponseCode::Other {
            name: code_str.to_owned(),
            value: None,
        })),
    }
}
```

**Test landed.**
`decode/tests.rs::response_code_uidnext_overflow_falls_into_text_not_code`
pins the current shape.

---

## C6 - `MailboxName::from_decoded` lets a server smuggle NUL / CR / LF into a `MailboxName`, and the type's doc says it cannot

**Severity: low** (an invariant leak, not an injection). **Where:**
`codec/decode/mailbox.rs::decode_mailbox_from_wire`,
`codec/utf7.rs::decode_utf7`, and the doc block on
`types/validated.rs::MailboxName`.

**Path to failure.** `decode_utf7` replaces *raw* control octets with
U+FFFD, but a control character carried inside a modified-Base64 shift
segment is decoded literally - `&AAoALQ-` is UTF-16BE
`U+000A U+002D`, i.e. `"\n-"`. `decode_mailbox_from_wire` hands the
result to `MailboxName::from_decoded`, which performs no validation, so

```
* LIST () "/" "&AAoALQ-"\r\n
```

produces a `MailboxName` containing a raw LF that `MailboxName::new`
rejects. The UTF8=ACCEPT path is worse: it is a bare
`String::from_utf8_lossy` with no filtering at all.

**Is it exploitable?** No, as far as I can trace. The re-encode path
contains it: `encode_mailbox_str` with `utf8 == false` runs
`encode_utf7`, which pushes any non-printable character back into a
Base64 shift segment (pinned by the new
`encode_emits_only_printable_ascii` property); with `utf8 == true` the
raw LF fails the `quotable` check in
`encode_quoted_or_literal_utf8` and the name is sent as a literal,
where LF is legal. So this is an invariant leak, not CRLF injection.

**The doc, however, is wrong.** `types/validated.rs` says:

> `MailboxName` has exactly two construction paths: `MailboxName::new`
> (public, validating) and `from_decoded` (codec-private). There is no
> `From<String>` or `From<&str>` - smuggling unvalidated data through
> the type is a compile error

The second sentence contradicts the first: `from_decoded` *is* the
smuggling path, it is `pub(crate)`, and it runs on every parsed
response. Anyone reading that block will assume a `MailboxName` in hand
satisfies the "no NUL, CR, or LF" invariant listed three lines above.

**Proposed fix.** Either (a) filter in `decode_mailbox_from_wire` -
map NUL/CR/LF to U+FFFD, matching what `decode_utf7`'s raw-octet branch
already does for exactly these octets - or (b) correct the doc block to
say that names originating from the wire are *not* validated and that
consumers must not assume the invariant. (a) is cheap and makes the
type honest; the `codec` half is in my scope, the `types` half is not.

**Tests landed.**
`utf7_tests.rs::base64_encoded_control_characters_are_decoded_literally`
(shows the two decode paths disagreeing) and
`decode/tests.rs::list_mailbox_name_can_carry_control_characters_from_the_wire`
(shows the resulting `MailboxName` is one `MailboxName::new` rejects).

---

# Non-bug findings

## N1 - `X-GM-LABELS` is silently dropped

`FetchResponse` models `gmail_msg_id` (`X-GM-MSGID`) and
`gmail_thread_id` (`X-GM-THRID`) but not `X-GM-LABELS`, so labels fall
into `fetch_response_inner`'s unknown-attribute arm and are skipped.
Since Gmail's IMAP label model *is* its folder model, and the other two
Gmail extensions are modelled, this reads as an oversight rather than a
decision. `bifrost-google` uses the Gmail API rather than IMAP, so the
gap only bites a Gmail account configured as a generic IMAP account.
The fix needs a `FetchResponse` field, which is `types`, outside my
scope. Pinned by
`decode/tests.rs::fetch_x_gm_labels_is_skipped_without_losing_other_attributes`.

## N2 - THREAD attaches a nested group to the wrong chain node when the group precedes a bare UID

`extensions.rs::parse_thread_node` pushes a fresh branch bucket after
each bare UID and attaches nested groups to `branch_groups.last_mut()`.
For `(1 (2) 3)` the group `(2)` lands in bucket 0, which
`build_thread_tree` then treats as the children of chain UID `3` - so
`2` becomes a child of `3` rather than a sibling branch off `1`.

RFC 5256 Section 5 (`thread-members = nz-number *(SP nz-number)
[SP thread-nested]`) only allows nested groups at the *end*, so this
input is non-conformant and the handling is arbitrary-but-not-crashing.
I did not land a test: pinning arbitrary handling of invalid input
mostly creates work for whoever changes it later. Noting it because the
in-function comment claims `branch_groups` is `[[], [...]]` for the
worked example, which does not match what the code builds
(`[[...]]`) - the comment is stale even though the result is right.

## N3 - Three paren-skippers can scan past a response-terminating CRLF

`skip_paren_group`, `skip_balanced_parens`, and `scan_section_spec` all
break out of their quoted-string loop when a backslash precedes CR/LF
(so the escape cannot swallow the CRLF), but the *outer* loop then
treats CR and LF as ordinary bytes and keeps looking for the closing
delimiter - potentially in the next response in the buffer.
`scan_unknown_response`, by contrast, correctly stops at CR/LF.

I could not construct an input where this produces a *successful*
over-consuming parse: in every case I tried, `fetch_response_inner`
subsequently fails on the CR and the whole response degrades to
`Unknown` with the buffer position intact (which is C4, not
over-consumption). The existing
`skip_paren_group_backslash_crlf_in_quoted_string` test already
acknowledges the concern and asserts the weaker "must not consume past
CRLF *if* it parses". Recording this as PLAUSIBLE, not CONFIRMED: the
structural asymmetry with `scan_unknown_response` is real and cheap to
close (add `b'\r' | b'\n' => break` at depth > 0), but I have no
failing input.

## N4 - `body_ext_1part` / `body_ext_mpart` tolerate one space but not two

`at_body_ext_end` skips leading spaces only when the next non-space byte
is `)`; otherwise it returns the *original* position and the caller does
a strict `sp(input)?`. So `... NIL  NIL)` (two spaces between extension
fields) fails, while `... NIL NIL )` (space before the close paren)
succeeds. Every other list position in the BODYSTRUCTURE parser accepts
`take_while1(|b| b == b' ')`; this one does not. Low impact - I have not
seen a server do it - but it is the one remaining hole in an otherwise
uniform whitespace-tolerance story, and `extra_whitespace_still_parses`
does not reach it (its generator only doubles existing SP positions
inside the sampled responses, and the BODYSTRUCTURE sample has no
extension fields).

## N5 - `parse_untagged_quota` depends on `alt` ordering for correctness

`parse_untagged_quota` matches `tag_no_case(b"QUOTA ")`, which cannot
match `QUOTAROOT` (no space), so it is correct - but the in-function
comment says "If we got here, it's `QUOTA ` followed by a root name",
which reads as if it were relying on `parse_untagged_quotaroot` running
first in the `alt`. It does run first, but the ordering is not what
makes this safe. Worth rewording so nobody "fixes" the ordering.

## N6 - `EncodedCommand::segments`'s "never empty" claim is unenforced

The doc on `EncodedCommand::segments` says "Each segment is never
empty". It holds today (every marker is at least 4 octets, so each
split advances), but nothing checks it, and `from_flat_buffer` on an
empty buffer returns *zero* segments, which `send_encoded_segments`
would treat as "nothing to send". Unreachable today because every
encoder writes at least a tag; a `debug_assert!(!segments.is_empty())`
would make the contract self-checking.

## N7 - `validate_login_credential_ascii` permits CR/LF in credentials

It rejects only non-ASCII (`!value.is_ascii()`), and CR/LF are ASCII.
A password containing CRLF therefore reaches
`encode_quoted_or_literal_utf8`, fails the `quotable` check, and is sent
as a literal - which is legal and safe (RFC 3501 Section 4.3 literals
carry CHAR8). It is also the trigger condition for C1 and C7 on the
LOGIN path. The behavior is correct; it is just surprising that the
function named "validate credential" does not reject the one byte class
everything else in `encode/mod.rs` rejects. A comment would do.

## N8 - `decode_utf7` accepts an unterminated shift segment without complaint

`&AOk` (no closing `-`) decodes to `é` with no `tracing` event, while
`&` alone is preserved as a literal ampersand and malformed Base64 falls
back to emitting `&<raw>-`. Three different recovery strategies, only
one of them logged. Deliberate Postel behavior per the existing
`unterminated_base64_segment` test; noting the inconsistency only.

## N9 - `classification.rs` `mailbox_names_eq` duplicates `connection::helpers::inbox_eq`

The comment already says so and explains why (module privacy). Both are
four lines. If a shared helper module is created for C3, this belongs in
it.

---

# Tests landed

All hermetic, in-process, deterministic. No `Cargo.toml` touched;
`proptest` and `pretty_assertions` were already dev-dependencies and
`proptest` was already in use in all three codec test files. No existing
test was modified.

| file | test | what it pins |
|---|---|---|
| `encode/tests.rs` | `literal_plus_body_containing_sync_marker_is_wrongly_split` | **C1.** LITERAL+ payload containing `{5}\r\n` is split into 2 segments |
| | `sync_literal_body_containing_sync_marker_is_skipped` | control case: the synchronizing form skips the same payload correctly |
| | `brace_digits_not_abutting_crlf_is_not_a_literal_marker` | the encoder does *not* share `connection/wire.rs`'s B4 abutment defect |
| | `list_status_return_option_rejects_one_character_item_list` | **C3**, false-reject symptom |
| | `list_status_return_option_accepts_unbalanced_leading_paren` | **C3**, false-accept symptom, byte-exact wire output |
| `decode/tests.rs` | `rfc2047_repeated_shift_prefix_is_passed_through_verbatim` | **C2**, the quadratic shape; verbatim passthrough per RFC 2047 Section 6.3 |
| | `rfc2047_adjacent_shift_prefixes_are_passed_through_verbatim` | the cheap sibling branch, so a partial fix is visible |
| | `malformed_status_silently_degrades_to_unknown_response` | **C4**, STATUS with a valueless item |
| | `malformed_fetch_envelope_silently_degrades_to_unknown_response` | **C4**, short ENVELOPE discards a whole FETCH |
| | `fetch_x_gm_labels_is_skipped_without_losing_other_attributes` | **N1**, labels dropped, neighbours survive |
| | `response_code_uidnext_overflow_falls_into_text_not_code` | **C5**, whole bracket group demoted to prose |
| | `list_mailbox_name_can_carry_control_characters_from_the_wire` | **C6**, `MailboxName` that `MailboxName::new` rejects |
| `utf7_tests.rs` | `base64_encoded_control_characters_are_decoded_literally` | **C6**, the two decode paths disagree on control octets |
| | `prop_decode_invariants::decode_utf7_never_panics` | arbitrary wire bytes |
| | `prop_decode_invariants::roundtrip_identity_including_control_characters` | MUTF-7 identity over *all* strings, not only NUL/CR/LF-free ones |
| | `prop_decode_invariants::decode_is_stable_under_reencode` | decode -> encode -> decode is a fixed point (this is the invariant that keeps the client addressing the mailbox the server named) |
| | `prop_decode_invariants::encode_emits_only_printable_ascii` | RFC 3501 Section 5.1.3: everything else goes through the shift; this is what makes C6 non-exploitable |

Six of these carry a `DOCUMENTS A BUG, NOT AN ENDORSEMENT` doc comment
naming this file: the two C1 / C3 pairs, the two C4 tests, and the C5 /
C6 tests carry a softer "see `plans/bugs-imap-codec.md`" pointer because
they document contract tensions rather than outright defects.

---

# Not reached, and why

- **A byte-level duplex transcript through the codec.** The brief
  offered this as the new capability, and I did not use it: the codec
  is a pure function over `&[u8]` with no I/O in it at all
  (`parse_response_utf8` / `encode_command` take and return buffers),
  so a duplex adds a framing layer that belongs to `connection/**` and
  is already covered by that agent's `wire_tests.rs`. Every finding
  above is reachable with a byte slice. The one place a transcript
  *would* pay for itself is proving that C1 actually deadlocks
  `send_encoded_segments` end to end - but `send_encoded_segments` is
  in `connection/driver/`, outside my edit scope, and the segment count
  is the honest codec-side assertion.

- **`classification.rs` beyond reading it.** 27 tests already walk the
  truth table, and the one interesting interaction I found (Unknown ->
  `OnlyUnsolicited` as C4's landing zone) is a decode finding, not a
  classification one. The table is dense but each arm is a direct RFC
  transcription; I read all of them and found no misclassification.

- **`decode_rfc2231_params`** (called from `body_params`). It lives in
  `types/rfc2231.rs`, outside my scope. It is worth someone's attention
  for the same reason as C2: it is fed attacker-controlled
  `Content-Type` parameters and does continuation reassembly, which is
  the classic quadratic-and/or-unbounded-allocation shape. I did not
  read it closely enough to make a claim.

- **`encoding_rs` charset fan-out.** `parse_encoded_word_inner` calls
  `Encoding::for_label` with an arbitrary attacker-supplied charset
  label and then decodes an arbitrary payload. `encoding_rs` is
  well-audited and its decoders are linear, so I did not pursue it, but
  the expansion factor is worth knowing: a 100 KB Base64 payload in a
  legacy multi-byte charset can expand several-fold into the resulting
  `String`, and nothing caps the subject length.

- **Fuzzing `body_structure` shape-space.** The existing whitespace and
  quoting properties cover the tolerance axis, and depth is capped at 64
  with a regression test. A structure-aware generator (well-formed
  BODYSTRUCTURE trees, then targeted mutations) would be the next
  increment, but it is a day of generator work and the previous sweep
  has already been through this file with the same lens.

- **`skip_tagged_ext_simple` terminator matrix.** Two callers pass
  different terminator sets (`{SP, )}` for STATUS, `{SP, CR}` for
  ESEARCH). I convinced myself both terminate and neither can
  over-consume, but I did not enumerate the cross product of
  NIL / literal / quoted / atom against both terminator sets. That is a
  cheap, mechanical test matrix if someone wants it.
