# bifrost-imap `codec/**` - bug hunt and test sweep

Scope: `crates/imap/src/codec/**` (decode, encode, UTF-7, response
classification), plus the `MailboxName` invariant boundary required by C6.
Written against the tree as of this sweep.

Two halves, kept separate on purpose:

- **Tests landed.** They pin behavior *as it exists today*. Where a test
  pins behavior I believe is wrong, its doc comment says
  `DOCUMENTS A BUG, NOT AN ENDORSEMENT` and names the finding ID below.
- **Open bugs.** Each remaining report has a proposed fix.

Finding IDs are stable references, not a severity rank.

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

- **Unchecked `pos + literal_len`** (connection B5). **Was present** in
  `encode/core.rs::from_flat_buffer` (C7, now fixed: the marker end plus the
  literal size goes through `checked_add` and a buffer-length filter, and
  every literal body is skipped rather than rescanned, which was C1). Every
  *decode*
  site already uses `checked_add` plus a bounds check; that half was
  fixed by the previous sweep and has regression tests
  (`skip_balanced_parens_literal_checked_add_overflow`,
  `try_skip_literal_overflow_returns_none`,
  `scan_section_spec_literal_overflow_does_not_panic`,
  `skip_paren_group_literal_overflow_no_wrap`).

A third sibling turned up unprompted: `connection/helpers.rs`'s
`list_status_return_option_items` (connection B6) was **duplicated verbatim,
with the identical off-by-one**, in `codec/encode/commands/list.rs` (C3). Both
are fixed, and the duplication is gone rather than fixed twice: the encoder
owns the single implementation (`pub(crate)`) and the connection layer's
pre-encode capability check calls it.

---

# Bugs

No open bugs. C4, the last open finding, is closed; its ledger follows.

## C4 - closed: malformed BODYSTRUCTURE shapes can no longer be laundered into `Unknown`

**Where:** `codec/decode/envelope_fetch.rs::has_closed_grammar`.

**What is closed.** `parse_untagged_unknown` refuses to swallow a response
opening with a keyword this codec claims to parse (`OK`, `STATUS`, `LIST`,
`ESEARCH`, `VANISHED`, `THREAD`, ... and the numbered `EXISTS` / `RECENT` /
`EXPUNGE`), and inside `FETCH` the same rule now applies per attribute rather
than per keyword: `fetch_attr_value` is gated by `has_closed_grammar`, so a
failure parsing `UID`, `FLAGS`, `RFC822.SIZE`, `RFC822`, `RFC822.HEADER`,
`RFC822.TEXT`, `INTERNALDATE`, `MODSEQ`, `SAVEDATE`, `PREVIEW`, `EMAILID`,
`THREADID`, `X-GM-MSGID`, `X-GM-THRID`, or the sectioned `BODY[...]` /
`BINARY[...]` / `BINARY.SIZE[...]` forms becomes `nom::Err::Failure`. `alt`
propagates it, the connection surfaces `Error::Parse`, and the account
boundary maps to `Protocol(ParseFailed)` / `ProviderContractViolation` per
`reference/error-model.md`. Every attribute separator went to multi-space
tolerance (`attr_sp`) at the same time, so a server padding with two spaces
does not pay for the new strictness.

**What round 3 closed.** `has_closed_grammar`, now applied within
`fetch_attr_value`, promotes malformed ENVELOPE ten-field shapes, malformed
single-part BODYSTRUCTURE / bare-BODY prefixes (media type/subtype, params,
id, description, encoding, size), and malformed multipart outer prefixes
(one or more child groups plus subtype). Both body prefixes additionally
require the outer structure to *close*: an extension tail is open-ended in its
contents, never in its framing, so a truncated
`BODYSTRUCTURE ("IMAGE" "PNG" NIL NIL NIL "BASE64" 5000` is a contract
violation and no longer degrades to `Unknown`. This preserves multi-space
attribute separators and keeps the contents of extension tails tolerant.

**What round 3 also had to widen.** Gating `X-GM-LABELS` strict made the
label grammar load-bearing, and `astring` cannot express Gmail's actual wire
form: system labels arrive as bare backslash-prefixed atoms
(`(\Inbox \Sent Important)`), and `\` is a `quoted-special` excluded from
`ATOM-CHAR`. Parsing labels as plain `astring` therefore turned a routine
Gmail FETCH into a connection-fatal failure. The grammar is now
`"\" atom / astring`, and `X-GM-MSGID` / `X-GM-THRID` also accept the quoted
decimal some Gmail proxies emit. This is the general hazard of the strict
lane: every attribute promoted into it must first be checked against what
servers actually send, not only against the ABNF.

**What the close pass closed (final disposition).** The residual - a
malformed nested multipart child such as
`* 1 FETCH (BODYSTRUCTURE (("TEXT") "MIXED"))` degrading to `Unknown` - was
recorded as a permanent tolerance on the premise that a strict *recursive
parser* would also reject a balanced child carrying an unmodelled-but-
conformant extension tail. That premise does not hold for a recursive
*prefix* check: `body_group_is_malformed` applies the same fixed-arity rule
the top level already used (six fields plus size for a single part; children
plus subtype for a multipart) to every child, recursively, depth-capped at
the typed decoder's own limit of 64. An arity violation is caught at any
depth; a balanced child whose *prefix* is conformant keeps its extension
tail tolerant, exactly as at the top level, so no conformant server shape
became connection-fatal. C4 is closed in full.

**Also closed at the same time.** `* 0 FETCH (...)` was still laundered into
`Unknown` (silently dropping the FETCH) while `* 0 EXPUNGE` was already
connection-fatal. FETCH stays out of the numbered known-response guard's
keyword table because its `msg-att` body is open-ended, but the sequence
number itself is `nz-number`, a grammar this codec fully implements - so the
guard now recognizes a zero (or all-zero) sequence number followed by
`FETCH` as a known-response violation. Zero-numbered unmodelled keywords
(`* 0 XSOMETHING`) remain extensions.

**Why it mattered.** In a QRESYNC/CONDSTORE sync a silently-dropped FETCH can
leave a message unhydrated while the cursor advances, making the loss persist
until UIDVALIDITY changes.

**Tests landed.**
`decode/tests.rs::malformed_closed_grammar_fetch_attributes_are_parse_failures`
pins the closed half over every gated attribute;
`malformed_open_ended_fetch_fixed_prefixes_are_parse_failures` pins the
ENVELOPE / BODYSTRUCTURE / bare-BODY prefix gate, including the recursive
nested-child arity check; `fetch_seq_zero_rejected` pins the zero-sequence
FETCH closure with its unmodelled-keyword control;
`unmodelled_and_open_ended_fetch_attributes_stay_tolerated` pins the
unmodelled-attribute tolerance and the extension-tail tolerance at both the
top level and inside a multipart child;
`connection/wire_tests.rs::read_one_rejects_a_malformed_recognized_fetch_attribute`
pins the outcome on the path the driver actually takes.
`malformed_status_is_a_parse_failure` and `expunge_zero_rejected` are the
controls for the direct and numbered keyword halves.

---

# Tests landed

All hermetic, in-process, deterministic. No `Cargo.toml` touched;
`proptest` and `pretty_assertions` were already dev-dependencies and
`proptest` was already in use in all three codec test files. Fixed-behavior
tests supersede the former bug-documenting expectations.

| file | test | what it pins |
|---|---|---|
| `encode/tests.rs` | `brace_digits_not_abutting_crlf_is_not_a_literal_marker` | the encoder does not share `connection/wire.rs`'s B4 abutment defect |
| `decode/tests.rs` | `malformed_status_is_a_parse_failure` | direct known-keyword parse failures do not route as unsolicited data |
| | `expunge_zero_rejected` | the closed numbered half of **C4**: `EXISTS` / `RECENT` / `EXPUNGE` fail, unmodelled numbered keywords stay `Unknown` |
| | `malformed_fetch_uid_zero_is_parse_failure` | **C4**, `uniqueid = nz-number` violations now hard-fail instead of routing as `Unknown` |
| | `malformed_closed_grammar_fetch_attributes_are_parse_failures` | **C4**, the same for every FETCH attribute with a closed grammar |
| | `malformed_open_ended_fetch_fixed_prefixes_are_parse_failures` | **C4**, malformed ENVELOPE and BODYSTRUCTURE / bare-BODY fixed prefixes are connection-fatal |
| | `unmodelled_and_open_ended_fetch_attributes_stay_tolerated` | **C4**'s other edge: unmodelled attributes and body extension tails (top-level and inside multipart children) tolerate rather than drop the connection |
| | `scan_section_spec_incomplete_is_a_parse_failure_not_incomplete` | an unterminated `BODY[` section is an error, never `Incomplete` |
| | `paren_skippers_do_not_scan_past_backslash_crlf` | **N3**, a backslash-escaped CR/LF cannot bridge two responses |
| | `paren_skippers_do_not_scan_past_raw_crlf_in_a_quoted_value` | **N3**, raw CR/LF inside a quoted value ends the scan in all three paren-skippers |
| | `a_raw_crlf_in_a_quoted_fetch_value_does_not_swallow_the_next_response` | **N3** end to end: the following `* 2 EXISTS` survives in the buffer |
| | `fetch_bodystructure_extension_double_spaces_parse` | **N4**, repeated spaces between `body-ext-*` fields |
| | `rfc2047_candidate_window_is_capped_for_unbroken_printable_runs` | C2's scan bound is a constant, so a whitespace-free hostile header cannot go quadratic; an overlong real word still fits |
| | `response_code_overflow_recovery_stops_at_the_closing_bracket` | C5's recovery reads only the code's own value, not the status text after `]` |
| | `list_mailbox_name_preserves_control_characters_from_the_wire` | C6's resolution: wire names keep the server's identity, and `MailboxName::new`'s invariant is documented as not applying to them |
| `connection/helpers_tests.rs` | `list_status_option_extracts_the_whole_item_list` | C3, on the path `ImapConnection` actually calls |
| `connection/wire_tests.rs` | `read_one_rejects_a_malformed_recognized_fetch_attribute` | **C4**, decoder hard failure becomes the driver-facing `Error::Parse` for both closed attributes and BODYSTRUCTURE prefixes |
| `decode/tests.rs` | `fetch_x_gm_labels_are_exposed_without_losing_other_attributes` | **N1**, Gmail's real wire form (unquoted `\Inbox`, unquoted atom, quoted user label, modified UTF-7) decodes into `FetchResponse` without changing account-layer semantics |
| `connection/dispatch_tests.rs` | `fetch_byte_estimate_counts_gmail_labels` | labels are heap data the server controls, so they count toward `uid_fetch_limited` and the buffered-fetch warning |
| `connection/helpers_tests.rs` | `gmail_labels_fetch_requires_x_gm_ext_1` | `FetchAttr::GmailLabels` is capability-gated like the other Gmail attributes |
| `encode/tests.rs` | `encoded_command_rejects_an_empty_buffer` | **N6**, the non-empty segment invariant is enforced in release builds |
| | `encode_login_crlf_credential_stays_inside_its_literal` | **N7**, a CR/LF credential is counted-literal framed verbatim and cannot reach a command boundary |
| `utf7_tests.rs` | `unterminated_base64_segment` | **N8**, invalid unterminated shifts preserve raw identity rather than decode a prefix |
| `connection/helpers_tests.rs` | `inbox_compare_is_case_insensitive_for_inbox_only` | **N9**, connection consumers use the codec-owned mailbox comparison |
| `utf7_tests.rs` | `prop_decode_invariants::decode_utf7_never_panics` | arbitrary wire bytes |
| | `prop_decode_invariants::roundtrip_identity_including_control_characters` | MUTF-7 identity over all strings, not only NUL/CR/LF-free ones |
| | `prop_decode_invariants::decode_is_stable_under_reencode` | decode -> encode -> decode is a fixed point |
| | `prop_decode_invariants::encode_emits_only_printable_ascii` | RFC 3501 Section 5.1.3 printable-wire invariant |

C4 is closed in full; there is no remaining gap. The tolerance boundary now
sits exactly on the fixed-arity prefix rule, applied recursively.
`malformed_status_is_a_parse_failure` and `expunge_zero_rejected` are the
controls for the keyword halves that were closed earlier.

## N7, settled: not a bug on the literal path

Round 3 first rejected CR/LF in LOGIN credentials outright. That was wrong and
has been reverted. `login = "LOGIN" SP userid SP password` takes two
`astring`s, and RFC 9051 Section 4.3 allows a literal to carry any CHAR8.
A credential containing a line break fails the `quotable` check in
`encode_quoted_or_literal_utf8` and goes out as a counted literal whose octet
count is computed from exactly the bytes written, so the line break is payload
the server consumes inside the literal. The injection the reject was defending
against would require credential bytes to *escape* that framing, which the
count makes impossible;
`encode_login_crlf_credential_stays_inside_its_literal` proves it by encoding
a password containing a full `A002 DELETE INBOX` line and asserting the exact
framing. Rejecting would have locked out accounts whose valid password
contains a line break on servers offering only LOGIN. The ASCII-only check
(RFC 6855 Section 5) stands, and now runs once in the encoder with the
connection layer calling the same function before submitting to the driver
(`login_rejects_non_ascii_before_submitting_to_the_driver`).

# Not reached, and why

- **A byte-level duplex transcript through the codec.** The brief
  offered this as the new capability, and I did not use it: the codec
  is a pure function over `&[u8]` with no I/O in it at all
  (`parse_response_utf8` / `encode_command` take and return buffers),
  so a duplex adds a framing layer that belongs to `connection/**` and
  is already covered by that agent's `wire_tests.rs`. Every finding
  above is reachable with a byte slice. The one place a transcript
  *would* pay for itself is proving C4's dispatch outcome end to end;
  `connection/wire_tests.rs::read_one_rejects_a_malformed_recognized_fetch_attribute`
  now does that half over an in-memory duplex, which is as far as the
  reader goes - what the *driver* then does with `Error::Parse`
  (connection-fatal, per `reference/imap.md`) is still pinned only by the
  driver's own tests.

- **`classification.rs` beyond reading it.** 27 tests already walk the
  truth table, and the one interesting interaction I found (Unknown ->
  `OnlyUnsolicited` as C4's landing zone) is a decode finding, not a
  classification one. The table is dense but each arm is a direct RFC
  transcription; I read all of them and found no misclassification.

- **`decode_rfc2231_params`** (called from `body_params`). It lives in
  `types/rfc2231.rs`, outside my scope. It is worth someone's attention
  because it is fed attacker-controlled
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
