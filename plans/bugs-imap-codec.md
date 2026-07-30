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
pre-encode capability check calls it. N9 below is the remaining copy of this
shape.

---

# Bugs

## C4 - Malformed open-ended `FETCH` attributes are still laundered into `Unknown`

**Severity: low** (was medium; the closed-grammar half is now fixed).
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

**What is still open.** `ENVELOPE`, `BODYSTRUCTURE`, and bare `BODY` (a
`BODYSTRUCTURE` synonym) stay outside the gate, so `* 1 FETCH (BODYSTRUCTURE
("TEXT"))` still lands in `UntaggedResponse::Unknown`, `classify` routes it as
`OnlyUnsolicited`, and a FETCH consumer receives neither the response nor an
error.

**Why those three are excluded rather than forgotten.** They are the
open-ended bodies of `msg-att`: extensions append fields to `body-ext-*`, and
real servers vary inside `ENVELOPE` addresses. A failure there is at least as
likely to be a modelling gap of ours as a server defect, and parse failure is
connection-fatal (`reference/imap.md`), so hard-failing would drop connections
against conformant servers. N4 was exactly such a gap of ours and is now
closed, which shrinks but does not eliminate the class.

**Why it matters.** In a QRESYNC/CONDSTORE sync a silently-dropped FETCH can
leave a message unhydrated while the cursor advances, making the loss persist
until UIDVALIDITY changes.

**Proposed fix.** Distinguish "failed inside a recognized structure" from
"unmodelled extension data" *inside* the BODYSTRUCTURE and ENVELOPE parsers -
the fixed-arity prefix of each is closed (media type / subtype / params /
id / description / encoding / size; the ten ENVELOPE fields), only the
extension tail is open. Gating on the prefix would close the rest of C4
without touching the tolerance that protects the tail.

**Tests landed.**
`decode/tests.rs::malformed_closed_grammar_fetch_attributes_are_parse_failures`
pins the closed half over every gated attribute;
`unmodelled_and_open_ended_fetch_attributes_stay_tolerated` pins both the
remaining `Unknown` degradation and the unmodelled-attribute tolerance;
`connection/wire_tests.rs::read_one_rejects_a_malformed_recognized_fetch_attribute`
pins the outcome on the path the driver actually takes.
`malformed_status_is_a_parse_failure` and `expunge_zero_rejected` are the
controls for the direct and numbered keyword halves.

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
carry CHAR8). It was also the trigger condition for C1 and C7 on the
LOGIN path, both now fixed. The behavior is correct; it is just surprising that the
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
four lines. C3's fix showed the cheap way out - move the single
implementation into the codec and re-export it `pub(crate)` - and this is the
next candidate for the same treatment.

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
| | `unmodelled_and_open_ended_fetch_attributes_stay_tolerated` | **C4**'s other edge: unmodelled attributes and `ENVELOPE`/`BODYSTRUCTURE`/bare `BODY` still degrade rather than drop the connection |
| | `scan_section_spec_incomplete_is_a_parse_failure_not_incomplete` | an unterminated `BODY[` section is an error, never `Incomplete` |
| | `paren_skippers_do_not_scan_past_backslash_crlf` | **N3**, a backslash-escaped CR/LF cannot bridge two responses |
| | `paren_skippers_do_not_scan_past_raw_crlf_in_a_quoted_value` | **N3**, raw CR/LF inside a quoted value ends the scan in all three paren-skippers |
| | `a_raw_crlf_in_a_quoted_fetch_value_does_not_swallow_the_next_response` | **N3** end to end: the following `* 2 EXISTS` survives in the buffer |
| | `fetch_bodystructure_extension_double_spaces_parse` | **N4**, repeated spaces between `body-ext-*` fields |
| | `rfc2047_candidate_window_is_capped_for_unbroken_printable_runs` | C2's scan bound is a constant, so a whitespace-free hostile header cannot go quadratic; an overlong real word still fits |
| | `response_code_overflow_recovery_stops_at_the_closing_bracket` | C5's recovery reads only the code's own value, not the status text after `]` |
| | `list_mailbox_name_preserves_control_characters_from_the_wire` | C6's resolution: wire names keep the server's identity, and `MailboxName::new`'s invariant is documented as not applying to them |
| `connection/helpers_tests.rs` | `list_status_option_extracts_the_whole_item_list` | C3, on the path `ImapConnection` actually calls |
| | `fetch_x_gm_labels_is_skipped_without_losing_other_attributes` | **N1**, labels dropped, neighbours survive |
| `utf7_tests.rs` | `prop_decode_invariants::decode_utf7_never_panics` | arbitrary wire bytes |
| | `prop_decode_invariants::roundtrip_identity_including_control_characters` | MUTF-7 identity over all strings, not only NUL/CR/LF-free ones |
| | `prop_decode_invariants::decode_is_stable_under_reencode` | decode -> encode -> decode is a fixed point |
| | `prop_decode_invariants::encode_emits_only_printable_ascii` | RFC 3501 Section 5.1.3 printable-wire invariant |

C4 remains documented as a current gap, now narrowed from "numbered `FETCH`"
to the three open-ended attribute bodies (`ENVELOPE`, `BODYSTRUCTURE`, bare
`BODY`); `malformed_status_is_a_parse_failure` and `expunge_zero_rejected` are
the controls for the keyword halves that were already closed.

## C6, and why it is closed without sanitizing

C6 reported that `MailboxName`'s doc block promises a "no NUL / CR / LF"
invariant that `from_decoded` does not enforce. Two ways to make that true;
we took the one that does not change data.

Sanitizing (mapping those code points to U+FFFD) makes the type honest at the
cost of the server's own identifier: distinct mailboxes collide in the folder
registry and in sync scope identity, and every subsequent command names a
mailbox the server never advertised in its LIST reply. RFC 3501 Section 5.1 /
RFC 9051 Section 5.1 make the name the server's opaque handle, and modified
UTF-7 can represent control characters, so that rewrite is a correctness
regression, not a hardening step.

The invariant is therefore documented as belonging to `MailboxName::new`
alone, and the CRLF-safety claim is discharged where it actually holds - the
encoder. `encode_utf7` folds every non-printable character back into a Base64
shift segment (pinned by `encode_emits_only_printable_ascii`), and in
UTF8=ACCEPT mode a name with CR or LF fails `quotable` in
`encode_quoted_or_literal_utf8` and goes out as a literal, where CHAR8 is
legal (RFC 3501 Section 4.3). Consumers that embed a mailbox name in a
line-oriented sink must escape it themselves; the constructor doc says so.

---

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
