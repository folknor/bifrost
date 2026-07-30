# bifrost-imap `connection/**` - bug hunt and test sweep

Scope: `crates/imap/src/connection/**`, with narrow codec, error-model, and
account-boundary plumbing where a connection result crosses that boundary.
Written against the tree as of this sweep.

Tests pin behavior as it exists today.

## 0. Coverage survey (verified, not assumed)

The claim was that auth / tag / state / idle / dispatch / pipeline /
driver / typed_event had tests and the rest did not. Confirmed by
`grep -rn "cfg(test)"` over the whole subtree. Before this sweep:

| file | test module |
|---|---|
| `auth.rs` | yes (`ladder_tests`, in-file) |
| `dispatch.rs` + `dispatch/{auth,fetch,select}.rs` | yes |
| `driver/mod.rs`, `driver/event_sink.rs` | yes |
| `idle.rs` | yes (`idle_tests.rs`) |
| `pipeline/mod.rs` | yes (`pipeline/tests.rs`) |
| `state.rs` | yes (`state_tests.rs`) |
| `tag.rs` | yes (in-file) |
| `typed_event.rs` | yes (in-file) |
| `mod.rs` (`tests.rs`) | 3 driver-panic tests only |
| **`append.rs`** | none |
| **`config.rs`** | none |
| **`ergonomics.rs`** | none |
| **`extensions.rs`** | none |
| **`helpers.rs`** | none |
| **`lifecycle.rs`** | none |
| **`literals.rs`** | none |
| **`mailbox.rs`** | none |
| **`search_validation.rs`** | none |
| **`seq_ops.rs`** | none |
| **`sort_thread.rs`** | none |
| **`stream.rs`** | none (only `cfg(test)` `Memory` variant) |
| **`uid_ops.rs`** | none |
| **`wire.rs`** | none (only `cfg(test)` `WireReader::new`) |

The `cfg(test)` hits in `wire.rs`, `stream.rs`, and `driver/upgrade.rs`
are *instrumentation* (the in-memory `ImapStream::Memory` variant and an
unmetered `WireReader::new`), not tests. The duplex harness the task
description asks for was already half-built and used by exactly three
tests in `tests.rs`.

# Tests landed

All hermetic: in-process, deterministic, no listener, no port, no daemon.
Two doubles, both in the new `connection/test_support.rs`:

- **`detached(session_state, capabilities, enabled)`** - an
  `ImapConnection` whose command receiver is dropped and whose state
  snapshot is fixed at construction. Every capability / session-state
  gate is exercisable with zero I/O, and anything that *would* have hit
  the wire fails fast and unambiguously with `DriverGone` - which is how
  these tests distinguish "rejected locally" from "would have been sent".
- **`driver_pair(greeting)`** - the real driver task over
  `tokio::io::duplex`, fed a canned greeting, returning the server end
  for byte-level transcript scripting. Generalizes the
  `make_driver_test_pair` helper that already existed in `tests.rs`
  (parameterized greeting, plus `read_line` / `read_exact` / `respond` /
  `tag_of` helpers; `read_line` reads a byte at a time so a test can
  interleave line reads with exact-length literal reads).

| file | wired into | what it covers |
|---|---|---|
| `test_support.rs` | `mod.rs` (`#[cfg(test)] mod test_support;`) | the two doubles above |
| `literals_tests.rs` | `literals.rs` | `find_literal_boundary`, both `patch_*` functions: LITERAL+ vs LITERAL-, the 4096 boundary, literal8-needs-BINARY gating, payload skipping (including inside an already non-synchronizing body), oversized declared bodies, non-marker braces |
| `wire_tests.rs` | `wire.rs` | `buffer_may_contain_complete_response` (status-text braces, tagged-status forms with and without resp-text, continuations, single and multi literal FETCH, `{N+}`, quoted `{digits}` that is not a marker, and the fatal lane: an undeliverable literal count is `Error::Parse`, not an indefinite wait, on any target width), `try_parse_literal_marker`, the structural `ByteBucket::consume` Send check, plus `WireReader` duplex transcripts: greeting parse, split-response reassembly, split-literal reassembly, two responses from one segment, EOF -> `Closed`, hard parse error, `write_all` reaching the peer, `take_buffer` handover, keepalive/peer-cert unavailability on memory streams |
| `stream_tests.rs` | `stream.rs` | raw-DEFLATE write/decompress round trip over the same duplex byte-stream shape as production transports, and explicit no-progress failure |
| `helpers_tests.rs` | `helpers.rs` | `inbox_eq`, `status_item_tokens` (bare/parenthesized/empty/unbalanced/nested), `list_status_return_option_items`, `quota_resource_name` + `has_quota_resource`, `search_return_requests_save`, `require_condstore` / `require_searchres` / `require_state` / `check_utf8_only_enforced`, dual-mode rev2 ENABLE gating, the full STATUS and FETCH item validation matrices, `literal_mode` / `supports_non_sync_literal` / `supports_non_sync_literal8` / `append_literal_kind` / `append_literal_is_non_sync`, `validate_list_extended_request` |
| `search_validation_tests.rs` | `search_validation.rs` | `search_criteria_contains_atom`: bare keys, nested groups, `NOT` / `OR` recursion, the `CHARSET` prefix, one- and two-operand key skipping, MODSEQ's variable operand forms, quoted and literal operand skipping, unknown-key zero-operand handling, non-ASCII byte-boundary safety, termination on unterminated quotes/literals and unclosed groups; then the five capability gates `validate_search_criteria_capabilities` drives |
| `extensions_tests.rs` | `extensions.rs` | `compute_notify_flags` across selected / selected-delayed / non-selected filters, `MailboxName`, metadata events, empty event lists, `Other(_)` fan-out, multi-group union; plus every extension command's capability gate and ENABLE's authenticated-state-only rule |
| `mailbox_tests.rs` | `mailbox.rs` | `validate_qresync_params` (ENABLE requirement, seq-match-data ABNF rule), SELECT CONDSTORE gating, state gates for SELECT/CLOSE/UNSELECT, UNSELECT capability-or-rev2, LSUB rejected on rev2, CREATE-SPECIAL-USE capability and use-attr validation, LIST-STATUS needing both capabilities on rev1, STATUS item pre-validation, the single-pattern LIST-EXTENDED fallback, CRLF injection rejection across six mailbox commands |
| `uid_ops_tests.rs` | `uid_ops.rs` | `filter_store_flags`, UID EXPUNGE's UIDPLUS-or-rev2 gate, UID MOVE's refusal without MOVE or UIDPLUS, sequence MOVE's stricter gate, VANISHED needing QRESYNC *enabled*, the `$` SEARCHRES gate across fetch/copy/store/expunge, CHANGEDSINCE/UNCHANGEDSINCE needing CONDSTORE, Selected-state gates, ESEARCH capability, the SAVE return option's SEARCHRES gate, SEARCH RETURN (SAVE), SORT and THREAD capability gating including the algorithm upper-casing, and SORT inheriting the SEARCH criteria gates |
| `config_tests.rs` | `config.rs` | `ImapConfig` constructors (ports and modes), defaults, builder overrides, `TlsMode` predicates, `Debug` output |
| `ergonomics_tests.rs` | `ergonomics.rs` | bounded FETCH shutdown after a callback stops early: the receiver is released, buffered items are discarded, the callback error is preserved, and the driver still reaches tagged completion; plus the stalled-server case (no tagged completion, paused clock) proving the drain cannot outlive the command timeout while the driver holds a reserved permit |
| `tests.rs` (extended) | already wired | `validate_tls_server_name`, `filter_store_flags`, `expand_uid_ranges` (singles, ranges, the `*` sentinel, the 1e6 cap reported as an error with a known omitted count, inverted ranges), `selected_mailbox_effective_responses` (the `[CLOSED]` split, last-marker-wins), `build_selected_mailbox` (full code extraction, tagged-code extraction, missing UIDVALIDITY staying `None`, `HIGHESTMODSEQ 0` -> `NOMODSEQ`, VANISHED EARLIER filtering, pre-`[CLOSED]` state ignored), `is_notify_list_event` / `is_notify_selection_mismatch`, `next_prebuilt_tag`; then byte-level transcripts: SELECT round trip with state transition, SELECT NO leaving the session Authenticated, UID FETCH with a literal body section, unsolicited EXISTS/EXPUNGE during NOOP becoming typed events, CAPABILITY updating the cached snapshot, BYE mid-command preserving its response code without waiting for close, `* BYE [CAPABILITY ...]` still ending the command (the capability side-effect arm must not shadow the BYE tag), APPEND waiting for `+` on a synchronizing literal, APPEND skipping the wait under LITERAL+, a LITERAL+ APPEND whose body carries a marker-shaped line, and global and mailbox-specific APPENDLIMIT rejection before the wire |

The bug-documenting tests for B1 through B9 were rewritten into invariant
tests when those bugs were fixed, and the fixes added their own
regressions:

| former bug | test now pinning the fix |
|---|---|
| B1 (scanner spin on unbalanced `)`) | `search_validation_tests.rs::scanner_terminates_on_unmatched_closing_parentheses` |
| B2 (`find_literal_boundary` not length-aware) | `literals_tests.rs::boundary_skips_markers_inside_a_non_synchronizing_literal_payload`, `literals_tests.rs::literal_plus_skips_payload_of_an_already_non_synchronizing_literal`, and the driver transcript `tests.rs::literal_plus_append_does_not_wait_on_a_marker_shaped_body_line` |
| B4 (quoted `{digits}` read as a marker) | `wire_tests.rs::framing_quoted_brace_digits_are_not_a_literal_marker` |
| B5 (framing overflow on a hostile literal size) | `wire_tests.rs::framing_unreachable_literal_size_is_a_parse_error_not_a_stall`, `wire_tests.rs::framing_addressable_literal_size_is_merely_incomplete`, `wire_tests.rs::read_one_fails_fast_on_an_undeliverable_literal_count` |
| B6 (`list_status_return_option_items` ate an octet) | `helpers_tests.rs::list_status_option_extracts_the_whole_item_list` (the helper now delegates to the codec's single implementation) |
| B8 (`UID $` past the SEARCHRES gate) | `search_validation_tests.rs::saved_search_marker_is_detected_after_uid_key` |
| B3 (mid-command BYE swallowed) | `tests.rs::bye_mid_command_preserves_the_response_code_without_waiting_for_close`, `tests.rs::bye_carrying_a_capability_code_still_ends_the_command`, `state_tests.rs::bye_carrying_a_capability_code_still_reports_shutdown`; all driver read loops route the `SideEffectDigest` through `short_circuit_on_bye`, and BYE is now detected from the status tag independently of the response code |
| B7 (state published after the command result) | `driver/mod_tests.rs::command_answer_follows_state_publication` (every completion arm goes through `publish_then_answer`) |
| B9 (streaming FETCH stall on an early consumer stop) | `ergonomics_tests.rs::uid_fetch_each_finishes_after_the_callback_stops_a_full_bounded_stream`, `ergonomics_tests.rs::uid_fetch_each_gives_up_when_the_server_stalls_after_an_early_consumer_stop` (the drain drops the receiver instead of awaiting drainage past an outstanding permit) |
| N4 (`ByteBucket` lock/await proximity, `expect` on a poisoned lock) | locking and token arithmetic moved into synchronous helpers so a guard *cannot* reach the sleep, poisoning recovers with a warning instead of panicking in the driver, and `wire_tests.rs::byte_bucket_consume_future_is_send` pins the future's `Send`-ness structurally |
| N5 (deflate no-progress spin) | `stream_tests.rs` no-progress case; a compress call that consumes and produces nothing is `io::ErrorKind::WriteZero`, not an unbounded loop that never awaits |
| N6 (no test seam for `CompressedStream`) | `#[cfg(test)] InnerStream::Memory(DuplexStream)` plus `stream_tests.rs`'s raw-DEFLATE round trip. It is a real ordered byte stream, not a mock that could invent impossible I/O results |
| N7 (UID-range expansion returned a partial success) | `tests.rs::expand_uid_ranges_caps_at_one_million`, `tests.rs::expand_uid_ranges_handles_bare_star_and_overlap`, `dispatch_tests.rs::search_consumer_keeps_the_solicited_reply_when_uid_expansion_is_incomplete`, `dispatch_tests.rs::search_consumer_rejects_a_bare_star_and_counts_overlap_once`, `dispatch_tests.rs::search_consumer_reclassifies_only_the_extra_esearch`, and the account error translation. The set is normalized (star-anywhere refused, overlap merged) before both the cap check and the expansion, and the solicited ESEARCH stays on the command lane even when its expansion fails |
| N8 (APPENDLIMIT selected an ordering-dependent capability) | `tests.rs::append_uses_the_most_restrictive_global_appendlimit`, `tests.rs::append_checks_the_mailbox_specific_appendlimit_before_writing`, `tests.rs::mailbox_append_limit_reads_every_plausible_status_reply` (NOTIFY can leave the solicited STATUS in `ambiguous`), `tests.rs::append_preflight_failure_is_unsent_evidence_for_the_append` (the preflight's own transmission evidence must not be published as the non-idempotent APPEND's) |
| queued literal-marker duplicate | `encode/tests.rs::literal_marker_scanner_uses_the_shared_number64_parser`, `encode/tests.rs::number64_marker_has_the_same_boundary_on_every_pointer_width`, `encode/tests.rs::out_of_range_literal_count_is_not_a_marker_for_the_shared_parser`. The one shared parser now enforces the RFC 9051 `number64` ceiling and answers three ways, so each call site states whether an above-ceiling count is fatal framing or ordinary text |

Fixing those bugs did modify tests landed by this sweep (the six B1-B8
codec/validation entries above were rewritten or renamed). No test outside this sweep's own additions
was changed. No `Cargo.toml` was touched; the manifest already carries
tokio `io-util` plus dev `test-util`, `proptest` and `pretty_assertions`.

**`proptest` and `pretty_assertions` are still unused in this crate.**
I did not reach for either: the properties worth stating here
(`patch_literals_to_plus_with_binary` is a no-op on input with no
markers; a round trip of "encode a literal, then find its boundary"
always agrees) are cheap as example tests and much clearer as such. If
the orchestrator wants proptest coverage, the strongest candidate is now
unblocked: `buffer_may_contain_complete_response` against a generated
stream of well-formed responses, asserting the generator never lands on
the fatal lane.

---

# Still open

No known bug in scope remains: every B and N finding above is fixed and
pinned by a test that was bite-audited (reverted, observed failing,
restored). What is left is test reach, not defects.

- **The metering cap's 60 s sleep ceiling.** `ByteBucket::consume` clamps
  any single sleep to 60 s, so a cap low enough that one read needs more
  than 60 s of budget is silently exceeded: a 1 B/s cap with 16 KiB reads
  behaves like ~273 B/s. Intended or not, nothing states it, and nothing
  measures it - see the metering item below.

- **`lifecycle.rs` connect / STARTTLS.** `connect_with_tls_connector_metered`
  starts with `TcpStream::connect`, which is precisely the "can fail
  because a network did" shape the policy excludes. `ImapStream::into_tcp`
  returns `None` for `Memory`, so the STARTTLS upgrade path cannot be
  driven over the duplex either (deliberately - the comment says "STARTTLS
  tests must use the real transport"). What *is* testable there and now
  is: `validate_tls_server_name` (done), and `observe_driver_panic`'s
  three arms (partially covered by the pre-existing driver-panic tests).
  A hermetic STARTTLS test still needs a fake TLS handshake, which is a
  larger design question than a byte-stream seam.

- **Bandwidth metering (`WireMetering` / `ByteBucket`).** Testable with
  `tokio::time::pause()` and the duplex harness, and worth doing: the cap
  arithmetic is the kind of thing that is wrong by a factor of
  60 and nobody notices (see the ceiling note above). The shape is
  `WireReader::new_metered(ImapStream::Memory(client), None, Some(cap))`
  plus paused time, asserting the elapsed virtual duration.

- **`idle.rs` / `driver/idle.rs` transcripts.** `idle_tests.rs` covers
  the event-mapping half; the DONE handshake and the server-terminated
  path (`IdleEvent::ServerTerminated`) have no transcript test. Out of my
  survey gap (idle is listed as covered), and IDLE's `done_rx` plumbing
  makes the script non-trivial.
