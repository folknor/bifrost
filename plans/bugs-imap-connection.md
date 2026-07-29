# bifrost-imap `connection/**` - bug hunt and test sweep

Scope: `crates/imap/src/connection/**`, exclusively. Written against the
tree as of this sweep; nothing outside the scope was edited.

Two halves, kept separate on purpose:

- **Tests landed.** They pin behavior *as it exists today*. Where a test
  pins behavior I believe is wrong, the test carries a
  `DOCUMENTS A BUG, NOT AN ENDORSEMENT` doc comment naming this file.
- **Bugs reported, not fixed.** Each has a proposed fix; triage is the
  orchestrator's.

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

# Bugs

Ordered by severity.

## B1 - `search_criteria_contains_atom` spins forever on an unbalanced `)`

**Where:** `connection/search_validation.rs`, `search_criteria_contains_atom`
(the top-level `while i < bytes.len()` loop) plus
`search_criteria_contains_atom_in_key` / `search_criteria_consume_item`.

**Path to failure.** `search_criteria_consume_item` treats `(` and `)` as
token delimiters, so when the scan cursor sits on a top-level `)` it
consumes nothing and returns `None`. `search_criteria_contains_atom_in_key`
then returns `false` **without advancing `pos`**, and
`search_criteria_skip_whitespace` does not advance either (`)` is not
whitespace). The outer `while i < bytes.len()` re-enters with the same `i`
forever.

Concrete input: `validate_search_criteria_capabilities(")")`. Also
`"OR (SEEN) (FLAGGED))"`, `"SEEN)"`, and any criteria with a trailing or
stray close-paren at depth 0.

**Reachability.** This is consumer-controlled, unvalidated input, not just
a hand-written criteria string. `crates/imap/src/account/pim.rs`
`search_plan()` appends `SearchRequest::provider_query` verbatim
(`plan.criteria.push_str(raw.trim())`) with no syntax check, and
`search_messages` hands the result to `uid_search`, which calls
`validate_search_criteria_capabilities` first thing. A provider query of
`")"` wedges the calling task in a tight, non-yielding CPU loop. It never
awaits, so a `tokio::time::timeout` around the call does not help and, on
a current-thread runtime, the whole runtime is starved.

**What should happen instead:** unbalanced criteria should terminate the
scan (returning `false`, since the gate is advisory and the server is the
real syntax authority) or be rejected as `Error::Protocol`.

**Proposed fix.** Make the outer loop guarantee progress:

```rust
while i < bytes.len() {
    let before = i;
    if Self::search_criteria_contains_atom_in_key(criteria, bytes, &mut i, atom) {
        return true;
    }
    Self::search_criteria_skip_whitespace(bytes, &mut i);
    if i == before {
        // Unbalanced ')' (or any byte the item scanner refuses to
        // consume at depth 0). Skip it; the server validates syntax.
        i += 1;
    }
}
```

A belt-and-braces variant additionally makes the `(`-group loop in
`search_criteria_contains_atom_in_key` bail on a non-advancing recursion.

**No test landed** - a test would hang the suite. The scanner tests that
did land (`search_validation_tests.rs`) cover the terminating cases only,
including `"(SEEN"` (unclosed *open* paren, which does terminate).

---

## B2 - `find_literal_boundary` is not length-aware, so a LITERAL+ APPEND whose body contains `{digits}` at end-of-line deadlocks the driver

**Where:** `connection/literals.rs::find_literal_boundary`, consumed by
`connection/driver/wire_send.rs::send_with_literal_sync`, which is the
send path for *every* command and for `run_prebuilt_command`
(APPEND / MULTIAPPEND).

**Path to failure.**

1. `ImapConnection::append` builds the whole command - header **plus the
   raw message octets** - into one `BytesMut` and submits it as a
   pre-built command.
2. On a LITERAL+ server `effective_non_sync` is true, so the APPEND
   marker is written as `{N+}`.
3. `send_with_literal_sync` calls `find_literal_boundary(&buf[pos..])`,
   which scans for the *first* `{digits}\r\n` in the slice. `{N+}` does
   not match (the `+` breaks the `}` check), so the scan runs on into the
   message body.
4. Any body line ending in `{<digits>}` matches: `find_literal_boundary`
   requires `{`, one or more ASCII digits, `}`, CRLF - it does **not**
   require the `{` to start the line.
5. The driver writes the header plus the leading part of the message,
   then calls `wait_for_continuation` and blocks on `read_one`. The
   server is counting down N literal octets and will never send `+`.

**Concrete input.** APPEND (draft save, Sent copy after `send_message`,
`draft_create`) of a message whose body contains a line ending in a brace
quantifier or a template placeholder, e.g.

```
Try the regex /^\d{3}$/
```
or
```
Hello {0}
```

Both end a line with `}` immediately preceded by digits and `{`.

**Result.** The caller's `tokio::time::timeout` fires with
`Error::Timeout`, but the **driver task stays blocked in `read_one`
forever**: it has no timeout of its own. The pooled connection is wedged
and the socket half-written, so the connection is unusable but never
observed as broken.

A second, sharper variant: if the false marker's declared size exceeds
the bytes remaining in `buf`, `send_with_literal_sync` panics on
`&buf[send_end..send_end + literal_size]` (slice index out of range).
That surfaces as `Error::DriverPanicked`, which is at least visible.

**What should happen instead.** The send path must skip literal payloads
the same way `patch_literals_to_plus_with_binary` already does.

**Proposed fix.** Teach the boundary scanner about non-synchronizing
markers so it can skip their payloads, and bound the body slice:

- Change `find_literal_boundary` to also recognize `{digits+}\r\n` and
  return a third value ("non-synchronizing, payload length N") so
  `send_with_literal_sync` can advance `pos` past that payload instead of
  scanning into it. Symmetrically handle `~{N}` / `~{N+}`.
- Clamp the second write: `&buf[send_end..(send_end + literal_size).min(buf.len())]`,
  and treat a marker whose payload runs past the buffer as a bug
  (`Error::Internal`) rather than a panic.

**Test landed:** `literals_tests.rs::boundary_matches_inside_literal_payload`
pins the current (wrong) return value and points here.

---

## B3 - `run_one_command` swallows `* BYE`: the reason is lost and a non-closing server wedges the driver

**Where:** `connection/driver/mod.rs:589` (`run_one_command`) and
`:709` (`run_prebuilt_command`); same shape in
`connection/driver/pipeline.rs:312` and `connection/driver/upgrade.rs:266`.
All four write `let _digest = state.apply_side_effects(&resp);` and drop
the digest. `connection/driver/idle.rs:52,143` and
`connection/driver/wire_send.rs:153` *do* consult `digest.had_bye` and
return `Error::bye_with_code`.

**Path to failure.** Server sends `* BYE [UNAVAILABLE] too many
connections` in the middle of, say, a NOOP or a SELECT.

1. `apply_side_effects` sets the session state to `Logout` and sets
   `digest.had_bye` - which nobody reads.
2. `classify(_, UntaggedResponse::Status { .. })` returns `Either`
   (`codec/classification.rs:212`), so the BYE is handed to the
   *consumer*, which buffers it for `finalize`.
3. `finalize` never runs, because the tagged response never arrives.
4. The read loop keeps going. If the server closes, `read_one` returns
   `Error::Closed { attempt: InFlight }`. If it does not close, the
   driver blocks forever.

**Result.**

- The BYE text and response code are lost **entirely** - not in the
  error, not in the typed-event queue (the consumer that buffered it is
  dropped un-finalized).
- The account boundary sees a bare transport drop instead of a
  server-acknowledged shutdown. `error.rs` distinguishes these: `Bye`
  with a code routes through `response_code()` into a specific
  `RecoveryClass`; `Closed` collapses to a generic retryable drop. So an
  `[UNAVAILABLE]` / `[ALERT]` shutdown gets the wrong recovery class and
  the operator-visible alert never fires.
- A BYE-without-close permanently wedges the driver task (no per-read
  timeout in the driver; the caller's timeout only drops the caller's
  future).

**What should happen instead:** identical to `wait_for_continuation` -
return `Err(Error::bye_with_code(text, code))` as soon as
`digest.had_bye` is set, after emitting the response-code events.

**Proposed fix.** In all four loops, replace `let _digest = ...` with

```rust
let digest = state.apply_side_effects(&resp);
```

and, in the `Response::Untagged(u)` arm (after
`emit_untagged_response_code_events`), add the same `if digest.had_bye`
early return `wire_send.rs` already has. Factor it into one helper so the
six call sites cannot drift again.

**Test landed:** `tests.rs::bye_mid_command_is_swallowed_and_surfaces_as_closed`
pins the current behavior (a `Closed` error and an empty event queue) and
points here.

---

## B4 - `buffer_may_contain_complete_response` mistakes `{digits}` in a quoted string for a literal declaration

**Where:** `connection/wire.rs::try_parse_literal_marker`.

```rust
let brace_pos = before_crlf.iter().rposition(|&b| b == b'{')?;
let close_offset = buf[brace_pos + 1..crlf_pos].iter().position(|&b| b == b'}')?;
```

It never checks that the `}` actually *abuts* the CRLF. Any `{digits}`
before the CRLF, at any position, is read as a literal declaration.

**Path to failure.** Server sends a complete, self-contained response
whose last brace group before the CRLF is `{digits}`:

```
* LIST (\HasNoChildren) "/" "Order {12}"\r\n
```

`try_parse_literal_marker` returns `Some(12)`; the caller sets
`pos = first_crlf + 2 + 12`, which is past the end of the buffer, and
`buffer_may_contain_complete_response` returns `false`.
`WireReader::read_one` therefore does **not** parse the response it
already holds and blocks in `read_buf` waiting for octets that are not
coming.

**Result.** The command stalls until the next server write unblocks it,
or until the caller's command timeout if nothing else is in flight (IDLE,
a final untagged response before a delayed tagged line, a
`NOTIFY`-delivered LIST). Realistic triggers: a mailbox name or a FETCH
ENVELOPE `Subject` ending in a brace quantifier or a template
placeholder - the same family as B2.

**What should happen instead:** the marker must terminate the line.

**Proposed fix.** Add the abutment check:

```rust
let close_pos = brace_pos + 1 + close_offset;
if close_pos + 1 != crlf_pos {
    return None; // `}` does not abut the CRLF, so this is not a marker
}
```

**Test landed:**
`wire_tests.rs::framing_quoted_brace_digits_misread_as_literal` pins the
false `false`, plus
`framing_brace_digits_not_before_crlf_still_completes_when_more_follows`
showing why the defect usually only bites on the last buffered response.

---

## B5 - `buffer_may_contain_complete_response` overflows `usize` on a hostile literal size

**Where:** `connection/wire.rs::buffer_may_contain_complete_response`:

```rust
let mut pos = first_crlf + 2 + literal_len;
...
pos += next_crlf + 2 + next_literal_len;
```

`try_parse_literal_marker` returns whatever `str::parse::<usize>()`
accepts, so `literal_len` can be `usize::MAX`.

**Path to failure.** A malicious or broken server sends

```
* 1 FETCH (BODY[] {18446744073709551615}\r\n
```

`first_crlf + 2 + usize::MAX` overflows. The workspace has no
`overflow-checks` override in the root `Cargo.toml`, so:

- **debug / `brokkr check` test builds:** panic inside the driver task ->
  `Error::DriverPanicked`. Remotely triggerable panic.
- **release:** silent wraparound to a small `pos`, after which the scan
  reads from an arbitrary offset and returns a meaningless verdict.

**What should happen instead:** an unrepresentable literal length means
"cannot be complete" -> `false`.

**Proposed fix.** Use checked arithmetic, mirroring what `literals.rs`
already does (`body_start.checked_add(size)`):

```rust
let Some(mut pos) = first_crlf.checked_add(2).and_then(|p| p.checked_add(literal_len))
else { return false };
...
let Some(next) = pos.checked_add(next_crlf).and_then(|p| p.checked_add(2))
    .and_then(|p| p.checked_add(next_literal_len)) else { return false };
pos = next;
```

Optionally also cap `try_parse_literal_marker` at a sane ceiling
(the crate already caps UID-range expansion at 1e6).

**No test landed** - the current behavior is profile-dependent (panic vs.
wrap), so there is nothing stable to pin. Once fixed, the natural test is
`assert!(!buffer_may_contain_complete_response(b"* 1 FETCH (BODY[] {18446744073709551615}\r\n"))`.

---

## B6 - `list_status_return_option_items` eats the first octet of the STATUS item list, silently disabling item validation

**Where:** `connection/helpers.rs::list_status_return_option_items`.

```rust
Some(if let Some(suffix) = trimmed[6..].strip_prefix(" (") {
    if suffix.ends_with(')') && suffix.len() >= 2 {
        Ok(&suffix[1..suffix.len() - 1])
```

`strip_prefix(" (")` has already removed the opening paren, and then the
body is sliced `[1..len-1]` as if it had not been. The leading `(` is
removed twice.

**Path to failure.** `list_extended(reference, patterns, &[], &["STATUS (MESSAGES UNSEEN)"], t)`.

- `list_status_return_option_items` returns `"ESSAGES UNSEEN"`.
- `validate_requested_status_items("ESSAGES UNSEEN")` tokenizes to
  `["ESSAGES", "UNSEEN"]`. `"ESSAGES"` matches no gated item name, so it
  falls into the catch-all `_ => {}` arm.

**Result.** Every gate on the *first* STATUS item in a LIST-STATUS return
option is dead:

- `STATUS (RECENT)` is accepted on an IMAP4rev2 connection, where RECENT
  was removed (RFC 9051 Section 6.3.11).
- `STATUS (HIGHESTMODSEQ ...)` is accepted without CONDSTORE.
- `STATUS (DELETED-STORAGE ...)` is accepted without `QUOTA=RES-STORAGE`.
- `STATUS (SIZE)` is accepted on rev1 without `STATUS=SIZE`.
- Single-item options with a one-character item degenerate to `""` and
  fail with the "must contain at least one data item" error instead.

The malformed items still go to the server verbatim (the raw option
string, not the parsed one, is what gets encoded), so this is a lost
client-side guard rather than a corrupted wire command. Still, the whole
point of the gate is to fail before the round trip.

**Proposed fix.** Drop the redundant slice:

```rust
if let Some(suffix) = trimmed[6..].strip_prefix(" (") {
    match suffix.strip_suffix(')') {
        Some(items) => Ok(items),
        None => Err(Error::Protocol(...)),
    }
}
```

which also fixes the `len() >= 2` special case (`STATUS (X)` currently
errors).

**Test landed:** `helpers_tests.rs::list_status_option_drops_the_first_item_octet`
pins `"ESSAGES UNSEEN"` and points here.

---

## B7 - The state snapshot is published *after* the command result, so `session_state()` / `capabilities()` can be stale right after a command returns

**Where:** `connection/driver/mod.rs`, driver loop:

```rust
let _ = result_tx.send(result);      // caller can wake here
...
let _ = state_tx.send_replace(state.snapshot());   // ...but state lands here
```

**Path to failure.** On a multi-thread runtime (which is what
`bifrost-sync` runs on - `bifrost-imap` enables `rt-multi-thread`), the
oneshot `send` wakes the caller's task on another worker. If that worker
runs to the caller's next `state_rx.borrow()` before the driver executes
one more statement, the caller reads the pre-command snapshot.

Concrete: `select("INBOX").await` returns `Ok`, the account layer
immediately calls `uid_fetch(...)`, `require_state(&[Selected])` reads a
snapshot that still says `Authenticated`, and the fetch fails with
`Error::Protocol("command not valid in Authenticated state (expected one
of [Selected])")` - a spurious, unretryable-looking client bug.

The same window applies to `capabilities()` / `server_profile()` after
`ENABLE` or `CAPABILITY`, and to `is_rev2()` after `ENABLE IMAP4rev2`.

**Why it has not been seen:** the window is a couple of instructions
wide, and `#[tokio::test]` uses the current-thread runtime, where the
driver always reaches `send_replace` before the caller is polled. That is
exactly why the tests in this sweep are stable and would not catch a
regression here.

**Proposed fix.** Publish first, answer second:

```rust
let _ = state_tx.send_replace(state.snapshot());
let _ = result_tx.send(result);
```

This requires hoisting `state_tx.send_replace` into each command arm (or
computing `result` into a local and moving the send below the publish),
and it also removes the `continue` special-casing for `SetKeepalive` /
`PeerCertificate` - those arms genuinely do not mutate state and can keep
skipping the publish.

**No test landed** - reproducing this needs a multi-thread runtime and a
timing race, which is not a hermetic test.

---

## B8 - `UID $` slips past the SEARCHRES gate

**Where:** `connection/search_validation.rs::search_criteria_contains_atom_in_key`.
`"UID"` is on the one-operand key list, so the scanner consumes the token
after it as an operand and never inspects it as a key.

**Path to failure.** `uid_search("UID $")` (RFC 5182 Section 2.1: `$` as a
sequence-set operand). `search_criteria_contains_atom(criteria, "$")`
returns `false`, `require_searchres()` is never called, and the command
goes out to a server that may not implement SEARCHRES - producing a
server `BAD` instead of a local `MissingCapability`.

**Severity:** low. The failure is a worse error message, not data loss,
and the parallel `sequence_set.as_str().contains('$')` checks in
`uid_ops.rs` / `seq_ops.rs` catch the typed-`SequenceSet` paths. Only the
free-form criteria path is affected.

**Proposed fix.** Special-case the `$` atom: when `atom == "$"`, also
match it in operand position for the sequence-set-shaped keys (`UID`, and
the bare sequence-set key which already works). Or, more simply, treat
`$` with a whole-string token scan since `$` cannot legally appear inside
an unquoted operand for any other purpose.

**Test landed:** `search_validation_tests.rs::saved_search_marker_missed_after_uid_key`.

---

## B9 - `uid_fetch_each` / `uid_fetch_streaming` stall for the full command timeout when the consumer stops early

**Where:** `connection/ergonomics.rs::uid_fetch_each`,
`connection/uid_ops.rs::uid_fetch_streaming`,
`connection/seq_ops.rs::fetch_streaming`.

```rust
let drain_fut = async {
    while let Some(fetch) = rx.recv().await {
        on_fetch(fetch?)?;          // early return leaves `rx` alive
    }
    Ok::<(), Error>(())
};
let (fetch_result, drain_result) = tokio::join!(fetch_fut, drain_fut);
```

`rx` is borrowed by the async block, not moved into it. When `on_fetch`
returns `Err` (or `tx.send` fails in the `uid_fetch_streaming` variant),
the drain future returns but `rx` stays alive and un-drained. The
bounded streaming consumer's `prepare_to_read` then blocks on channel
capacity, so `fetch_fut` cannot make progress and `tokio::join!` parks
until `fetch_stream_bounded_impl`'s `tokio::time::timeout` fires.

**Result.** A callback that aborts on the first item still costs the full
command timeout (60 s by default via `ImapConfig::command_timeout`)
before `uid_fetch_each` returns the callback's error. Account-layer
hydration uses `uid_fetch_each`, so a hydration budget breach or a
serialization error on message 1 of 5000 blocks that folder's task for a
minute.

**Proposed fix.** Close the receiver on early exit so the driver's
`prepare_to_read` observes a dropped consumer and drains to the tagged
response promptly:

```rust
let drain_fut = async {
    let mut result = Ok(());
    while let Some(fetch) = rx.recv().await {
        match fetch.and_then(&mut on_fetch) {
            Ok(()) => {}
            Err(e) => { result = Err(e); break; }
        }
    }
    rx.close();          // <- unblocks the sender side
    while rx.recv().await.is_some() {}   // drain what is already queued
    result
};
```

(`Receiver::close` stops new sends and lets queued items be drained; the
driver keeps reading to the tagged OK, which is the documented contract.)

**No test landed** - proving it needs either a real timeout or paused
time plus a bounded-channel fill, which is more machinery than the
finding warrants right now.

---

# Non-bug findings

## N1 - Doc rot in `connection/mod.rs`

The doc comment on `is_notify_list_event` (`mod.rs` ~line 743) begins with
two orphaned paragraphs left over from deleted functions:

```
/// Find the index of the first `[NOTIFICATIONOVERFLOW]` response code in a
/// stream of untagged responses, or `responses.len()` if there is none.
///
/// Used by LIST/LIST-EXTENDED/LIST-STATUS handlers to classify each
/// Collect solicited FETCH responses from untagged data
/// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
/// Check whether a LIST response carries markers that identify it as a
```

Note the sentence that just stops mid-clause ("to classify each"). The
actual doc for `is_notify_list_event` starts at "Check whether a LIST
response...". Delete the first six lines.

## N2 - The `SideEffectDigest` contract is enforced by convention only

Six call sites take `apply_side_effects`'s digest; two use it, four bind
it to `_digest`. `SideEffectDigest` is not `#[must_use]`, so nothing
flags the drop. Marking the struct `#[must_use]` would not help (the
binding satisfies it), but making the BYE handling a shared helper -
`fn short_circuit_on_bye(digest, resp) -> Option<Error>` - would collapse
the four divergent copies into one. Same argument applies to
`had_notification_overflow`, which no driver loop consumes at all (the
state mutation happens inside `apply_untagged`, so nothing is currently
broken, but the digest field is dead weight advertising a contract that
is not honoured).

## N3 - `TlsMode::None` is reachable but `starttls()` still guards on capabilities being non-empty

`starttls_with_connector` only rejects when the capability list is
*non-empty and* lacks STARTTLS:

```rust
if !snap.capabilities.is_empty() && !snap.capabilities.iter().any(...) {
    return Err(Error::StartTlsUnavailable);
}
```

An empty capability list (server never sent `* CAPABILITY` and the
greeting carried none) therefore permits the upgrade attempt. That is
deliberate Postel behavior, but it is the one place in the crate where a
missing capability is treated as "maybe" rather than "no", and it is a
downgrade-adjacent decision. Worth a comment at minimum. Contrast
`connect_with_tls_connector_metered`, which *does* hard-fail on a missing
STARTTLS capability - the two paths to the same upgrade disagree.

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
| `literals_tests.rs` | `literals.rs` | `find_literal_boundary`, both `patch_*` functions: LITERAL+ vs LITERAL-, the 4096 boundary, literal8-needs-BINARY gating, payload skipping, oversized declared bodies, non-marker braces |
| `wire_tests.rs` | `wire.rs` | `buffer_may_contain_complete_response` (status-text braces, tagged-status forms with and without resp-text, continuations, single and multi literal FETCH, `{N+}`), `try_parse_literal_marker`, plus `WireReader` duplex transcripts: greeting parse, split-response reassembly, split-literal reassembly, two responses from one segment, EOF -> `Closed`, hard parse error, `write_all` reaching the peer, `take_buffer` handover, keepalive/peer-cert unavailability on memory streams |
| `helpers_tests.rs` | `helpers.rs` | `inbox_eq`, `status_item_tokens` (bare/parenthesized/empty/unbalanced/nested), `list_status_return_option_items`, `quota_resource_name` + `has_quota_resource`, `search_return_requests_save`, `require_condstore` / `require_searchres` / `require_state` / `check_utf8_only_enforced`, dual-mode rev2 ENABLE gating, the full STATUS and FETCH item validation matrices, `literal_mode` / `supports_non_sync_literal` / `supports_non_sync_literal8` / `append_literal_kind` / `append_literal_is_non_sync`, `validate_list_extended_request` |
| `search_validation_tests.rs` | `search_validation.rs` | `search_criteria_contains_atom`: bare keys, nested groups, `NOT` / `OR` recursion, the `CHARSET` prefix, one- and two-operand key skipping, MODSEQ's variable operand forms, quoted and literal operand skipping, unknown-key zero-operand handling, non-ASCII byte-boundary safety, termination on unterminated quotes/literals and unclosed groups; then the five capability gates `validate_search_criteria_capabilities` drives |
| `extensions_tests.rs` | `extensions.rs` | `compute_notify_flags` across selected / selected-delayed / non-selected filters, `MailboxName`, metadata events, empty event lists, `Other(_)` fan-out, multi-group union; plus every extension command's capability gate and ENABLE's authenticated-state-only rule |
| `mailbox_tests.rs` | `mailbox.rs` | `validate_qresync_params` (ENABLE requirement, seq-match-data ABNF rule), SELECT CONDSTORE gating, state gates for SELECT/CLOSE/UNSELECT, UNSELECT capability-or-rev2, LSUB rejected on rev2, CREATE-SPECIAL-USE capability and use-attr validation, LIST-STATUS needing both capabilities on rev1, STATUS item pre-validation, the single-pattern LIST-EXTENDED fallback, CRLF injection rejection across six mailbox commands |
| `uid_ops_tests.rs` | `uid_ops.rs` | `filter_store_flags`, UID EXPUNGE's UIDPLUS-or-rev2 gate, UID MOVE's refusal without MOVE or UIDPLUS, sequence MOVE's stricter gate, VANISHED needing QRESYNC *enabled*, the `$` SEARCHRES gate across fetch/copy/store/expunge, CHANGEDSINCE/UNCHANGEDSINCE needing CONDSTORE, Selected-state gates, ESEARCH capability, the SAVE return option's SEARCHRES gate, SEARCH RETURN (SAVE), SORT and THREAD capability gating including the algorithm upper-casing, and SORT inheriting the SEARCH criteria gates |
| `config_tests.rs` | `config.rs` | `ImapConfig` constructors (ports and modes), defaults, builder overrides, `TlsMode` predicates, `Debug` output |
| `tests.rs` (extended) | already wired | `validate_tls_server_name`, `filter_store_flags`, `expand_uid_ranges` (singles, ranges, the `*` sentinel, the 1e6 cap, inverted ranges), `selected_mailbox_effective_responses` (the `[CLOSED]` split, last-marker-wins), `build_selected_mailbox` (full code extraction, tagged-code extraction, missing UIDVALIDITY staying `None`, `HIGHESTMODSEQ 0` -> `NOMODSEQ`, VANISHED EARLIER filtering, pre-`[CLOSED]` state ignored), `is_notify_list_event` / `is_notify_selection_mismatch`, `next_prebuilt_tag`; then byte-level transcripts: SELECT round trip with state transition, SELECT NO leaving the session Authenticated, UID FETCH with a literal body section, unsolicited EXISTS/EXPUNGE during NOOP becoming typed events, CAPABILITY updating the cached snapshot, BYE mid-command (B3), APPEND waiting for `+` on a synchronizing literal, APPEND skipping the wait under LITERAL+, and APPENDLIMIT rejection before the wire |

Three of those tests carry an explicit
`DOCUMENTS A BUG, NOT AN ENDORSEMENT` header and name this file:
`boundary_matches_inside_literal_payload` (B2),
`framing_quoted_brace_digits_misread_as_literal` (B4),
`list_status_option_drops_the_first_item_octet` (B6),
`bye_mid_command_is_swallowed_and_surfaces_as_closed` (B3), and
`saved_search_marker_missed_after_uid_key` (B8). (Five, not three.)

No existing test was modified. No `Cargo.toml` was touched; the manifest
already carries tokio `io-util` plus dev `test-util`, `proptest` and
`pretty_assertions`.

**`proptest` and `pretty_assertions` are still unused in this crate.**
I did not reach for either: the properties worth stating here
(`patch_literals_to_plus_with_binary` is a no-op on input with no
markers; a round trip of "encode a literal, then find its boundary"
always agrees) are cheap as example tests and much clearer as such. If
the orchestrator wants proptest coverage, the strongest candidate is
`buffer_may_contain_complete_response` against a generated stream of
well-formed responses - but that is only worth writing *after* B4 and B5
are fixed, since the generator would otherwise be tuned around the bugs.

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

- **`driver/pipeline.rs` and `driver/upgrade.rs` BYE handling.** Both
  share B3's defect, and both are inside another agent's likely blast
  radius (`driver/**` has existing tests, so it was outside my survey
  gap). I read them to confirm the defect but wrote no tests there.

- **`ergonomics.rs`.** `select_for_sync` / `sync_fetch` / `uid_fetch_each`
  / `uid_fetch_limited` are thin compositions over the surfaces above.
  They are worth transcript tests, but each needs a multi-command script
  (ENABLE -> SELECT -> UID FETCH) and the payoff is mostly re-testing the
  pieces. B9 is the finding that came out of reading them.

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
