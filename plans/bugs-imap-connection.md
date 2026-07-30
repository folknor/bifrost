# bifrost-imap `connection/**` - bug hunt and test sweep

Scope: `crates/imap/src/connection/**`, exclusively. Written against the
tree as of this sweep; nothing outside the scope was edited.

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

---

# Non-bug findings

## N4 - `ByteBucket::consume` holds a `std::sync::Mutex` guard across a computed sleep, but never across an `.await`

`connection/wire.rs::ByteBucket::consume` is careful: the guard is scoped
inside a block whose value is the `Duration`, and the `sleep().await` is
outside. Good. Two smaller notes:

- `state.tokens = (state.tokens + elapsed * cap_f).min(cap_f)` means the
  bucket capacity equals one second of budget. A cap of 1 B/s with a
  16 KiB read therefore takes the `n > cap` fast path and sleeps
  `min(n/cap, 60)` = 60 s per read. That is the documented "clamps to
  1 B/s with a warning" behavior, but the 60 s ceiling means the
  *effective* floor is ~273 B/s for 16 KiB reads, not 1 B/s. Nobody is
  hurt; just noting the ceiling silently overrides the cap.
- `.expect("byte bucket lock poisoned")` is a panic path inside the
  driver task. A poisoned lock there is unreachable in practice (nothing
  panics while holding it), but the crate elsewhere avoids `expect` in
  the driver.

## N5 - `CompressedStream::write_all` can spin if deflate makes no progress

`connection/stream.rs`:

```rust
while input_offset < data.len() {
    ...compress(&data[input_offset..], &mut deflate_buf, FlushCompress::None)...
    input_offset += consumed;
    if produced > 0 { self.inner.write_all(&deflate_buf[..produced]).await?; }
}
```

If a call ever returns `consumed == 0` and `produced == 0` the loop never
terminates and never awaits. flate2 in practice always consumes into its
internal window, so this is theoretical - but the loop has no progress
guard, unlike the flush loop below it, which does (`if produced == 0 { break; }`).
A `if consumed == 0 && produced == 0 { return Err(...) }` would close it.

## N6 - `stream.rs` has no test hook for `CompressedStream`

`ImapStream::Memory` is the only `cfg(test)` affordance, and
`CompressedStream::new` takes an `InnerStream`, which is `Plain(TcpStream)`
or `Tls(...)` only. So the deflate round trip - the one piece of
`stream.rs` with real logic - is untestable without a socket. Adding a
`#[cfg(test)] InnerStream::Memory(DuplexStream)` variant would make a
compress/decompress round-trip test hermetic. I did not add it: it means
touching four `match` arms in `set_keepalive` / `peer_certificate_der` /
the three I/O methods, and `InnerStream` is on the STARTTLS/COMPRESS
upgrade path that another agent may be in. Flagging as the highest-value
remaining test hook in my scope.

## N7 - `expand_uid_ranges` cap is 1e6 but callers cannot tell how much was lost

`truncated: bool` says "incomplete" but not "how incomplete". The one
consumer that matters (`SearchResult.truncated`) then propagates a
boolean upward. For a UID SEARCH over a large mailbox this silently
discards results with no count. Not a bug against the current contract,
but if a caller ever needs "did I get everything", a `Option<usize>`
would be strictly more useful than the bool. Left alone; the contract is
documented.

## N8 - `append()`'s APPENDLIMIT scan stops at the first `AppendLimit` capability

```rust
for cap in &snap.capabilities {
    if let Capability::AppendLimit(Some(limit)) = cap { ...; break; }
}
```

The `break` is inside the `if let`, so `AppendLimit(None)` (bare
`APPENDLIMIT`, meaning per-mailbox limits reported via
`STATUS APPENDLIMIT`) does not stop the scan - correct. But if a
non-conformant server advertises both `APPENDLIMIT` and
`APPENDLIMIT=<n>`, the winner depends on capability ordering. Harmless;
noted only because the loop reads as if it were an `iter().find_map`,
which is what `multi_append` actually uses two functions later. The two
should be the same helper.

---

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
| `wire_tests.rs` | `wire.rs` | `buffer_may_contain_complete_response` (status-text braces, tagged-status forms with and without resp-text, continuations, single and multi literal FETCH, `{N+}`, quoted `{digits}` that is not a marker, and the fatal lane: an undeliverable literal count is `Error::Parse`, not an indefinite wait, on any target width), `try_parse_literal_marker`, plus `WireReader` duplex transcripts: greeting parse, split-response reassembly, split-literal reassembly, two responses from one segment, EOF -> `Closed`, hard parse error, `write_all` reaching the peer, `take_buffer` handover, keepalive/peer-cert unavailability on memory streams |
| `helpers_tests.rs` | `helpers.rs` | `inbox_eq`, `status_item_tokens` (bare/parenthesized/empty/unbalanced/nested), `list_status_return_option_items`, `quota_resource_name` + `has_quota_resource`, `search_return_requests_save`, `require_condstore` / `require_searchres` / `require_state` / `check_utf8_only_enforced`, dual-mode rev2 ENABLE gating, the full STATUS and FETCH item validation matrices, `literal_mode` / `supports_non_sync_literal` / `supports_non_sync_literal8` / `append_literal_kind` / `append_literal_is_non_sync`, `validate_list_extended_request` |
| `search_validation_tests.rs` | `search_validation.rs` | `search_criteria_contains_atom`: bare keys, nested groups, `NOT` / `OR` recursion, the `CHARSET` prefix, one- and two-operand key skipping, MODSEQ's variable operand forms, quoted and literal operand skipping, unknown-key zero-operand handling, non-ASCII byte-boundary safety, termination on unterminated quotes/literals and unclosed groups; then the five capability gates `validate_search_criteria_capabilities` drives |
| `extensions_tests.rs` | `extensions.rs` | `compute_notify_flags` across selected / selected-delayed / non-selected filters, `MailboxName`, metadata events, empty event lists, `Other(_)` fan-out, multi-group union; plus every extension command's capability gate and ENABLE's authenticated-state-only rule |
| `mailbox_tests.rs` | `mailbox.rs` | `validate_qresync_params` (ENABLE requirement, seq-match-data ABNF rule), SELECT CONDSTORE gating, state gates for SELECT/CLOSE/UNSELECT, UNSELECT capability-or-rev2, LSUB rejected on rev2, CREATE-SPECIAL-USE capability and use-attr validation, LIST-STATUS needing both capabilities on rev1, STATUS item pre-validation, the single-pattern LIST-EXTENDED fallback, CRLF injection rejection across six mailbox commands |
| `uid_ops_tests.rs` | `uid_ops.rs` | `filter_store_flags`, UID EXPUNGE's UIDPLUS-or-rev2 gate, UID MOVE's refusal without MOVE or UIDPLUS, sequence MOVE's stricter gate, VANISHED needing QRESYNC *enabled*, the `$` SEARCHRES gate across fetch/copy/store/expunge, CHANGEDSINCE/UNCHANGEDSINCE needing CONDSTORE, Selected-state gates, ESEARCH capability, the SAVE return option's SEARCHRES gate, SEARCH RETURN (SAVE), SORT and THREAD capability gating including the algorithm upper-casing, and SORT inheriting the SEARCH criteria gates |
| `config_tests.rs` | `config.rs` | `ImapConfig` constructors (ports and modes), defaults, builder overrides, `TlsMode` predicates, `Debug` output |
| `ergonomics_tests.rs` | `ergonomics.rs` | bounded FETCH shutdown after a callback stops early: the receiver is released, buffered items are discarded, the callback error is preserved, and the driver still reaches tagged completion; plus the stalled-server case (no tagged completion, paused clock) proving the drain cannot outlive the command timeout while the driver holds a reserved permit |
| `tests.rs` (extended) | already wired | `validate_tls_server_name`, `filter_store_flags`, `expand_uid_ranges` (singles, ranges, the `*` sentinel, the 1e6 cap, inverted ranges), `selected_mailbox_effective_responses` (the `[CLOSED]` split, last-marker-wins), `build_selected_mailbox` (full code extraction, tagged-code extraction, missing UIDVALIDITY staying `None`, `HIGHESTMODSEQ 0` -> `NOMODSEQ`, VANISHED EARLIER filtering, pre-`[CLOSED]` state ignored), `is_notify_list_event` / `is_notify_selection_mismatch`, `next_prebuilt_tag`; then byte-level transcripts: SELECT round trip with state transition, SELECT NO leaving the session Authenticated, UID FETCH with a literal body section, unsolicited EXISTS/EXPUNGE during NOOP becoming typed events, CAPABILITY updating the cached snapshot, BYE mid-command preserving its response code without waiting for close, `* BYE [CAPABILITY ...]` still ending the command (the capability side-effect arm must not shadow the BYE tag), APPEND waiting for `+` on a synchronizing literal, APPEND skipping the wait under LITERAL+, a LITERAL+ APPEND whose body carries a marker-shaped line, and APPENDLIMIT rejection before the wire |

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

# Not reached, and why

- **`lifecycle.rs` connect / STARTTLS.** `connect_with_tls_connector_metered`
  starts with `TcpStream::connect`, which is precisely the "can fail
  because a network did" shape the policy excludes. `ImapStream::into_tcp`
  returns `None` for `Memory`, so the STARTTLS upgrade path cannot be
  driven over the duplex either (deliberately - the comment says "STARTTLS
  tests must use the real transport"). What *is* testable there and now
  is: `validate_tls_server_name` (done), and `observe_driver_panic`'s
  three arms (partially covered by the pre-existing driver-panic tests).
  A hermetic STARTTLS test needs the `InnerStream::Memory` variant from
  N6 plus a way to inject a fake TLS handshake, which is a larger design
  question than a test.

- **`extensions.rs` COMPRESS round trip.** Blocked on N6 for the same
  reason. Only the capability and state gates are covered.

- **Bandwidth metering (`WireMetering` / `ByteBucket`).** Testable with
  `tokio::time::pause()` and the duplex harness, and worth doing: the cap
  arithmetic in N4 is the kind of thing that is wrong by a factor of
  60 and nobody notices. I ran out of budget; the shape is
  `WireReader::new_metered(ImapStream::Memory(client), None, Some(cap))`
  plus paused time, asserting the elapsed virtual duration.

- **`idle.rs` / `driver/idle.rs` transcripts.** `idle_tests.rs` covers
  the event-mapping half; the DONE handshake and the server-terminated
  path (`IdleEvent::ServerTerminated`) have no transcript test. Out of my
  survey gap (idle is listed as covered), and IDLE's `done_rx` plumbing
  makes the script non-trivial.
