# Bug hunt: bifrost-imap + bifrost-sasl

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/imap/` (driver, wire framing, pool, push/IDLE, auth, change
strategies, mutations, inventory, hydration, blob, cursor envelope, folder
registry) plus `crates/sasl/`.

Coverage note from the hunter: `pim.rs`'s long tail, `sieve.rs`, `factory.rs`,
`scopes.rs`, codec internals, and pipeline were not read in depth; findings
note where confidence is bounded by that.

## Confident defects

### 1. `decode_object_id` accepts `uid: 0`, and downstream `uid_set_from_u32` silently filters it - producing false successes and silently dropped items

- `crates/imap/src/account/envelope.rs` - `parse_u32` accepts `"0"` for the uid
  (and uidvalidity) field, so `imap1:5:INBOX:7:0` decodes cleanly. UID 0 is not
  an `nz-number` and the crate never mints it, but nothing rejects it on input.
- `crates/imap/src/account/mutate.rs` - in `run_folder_mutation` (Move arm) and
  `run_flag_mutation_groups`/`run_destroy_mutation_groups`, `uid_set_from_u32`
  drops the 0 before building the wire operand. Mixed batch: the STORE/MOVE
  never targets uid 0, yet `mutation_results(ids, Applied, ...)` and
  `applied_uids_after_store` (which returns *requested* uids on `Applied`)
  report **`Succeeded(Applied)` for the uid-0 id** - a fabricated success the
  engine will trust. All-zero group: `let Some(uid_set) = ... else { continue; }`
  skips the group and those ids get **no outcome at all**, violating the
  one-outcome-per-id contract.
- `crates/imap/src/account/get.rs` - `run_folder_get`: an all-zero request
  `return Ok(())`s with no outcome; a mixed one lands uid 0 in
  `Failed(NotFound(Message))` (misclassified but at least answered).
  `crates/imap/src/account/blob.rs` `run_fetch` turns a uid-0 id into a
  successful *empty* stream (`Done`, zero bytes).
- Fix shape: reject `uid == 0` (and arguably `uidvalidity == 0`) in
  `decode_object_id`/`decode_thread_id` as `Request(Malformed)`; every
  downstream hole closes at once.

### 2. `logout_best_effort` is unbounded, and dropping an `ImapConnection` leaks the driver task against a silent peer

- `crates/imap/src/connection/driver/upgrade.rs::logout_best_effort` writes
  LOGOUT and then `read_one()`s in a loop with **no timeout**. It runs at the
  end of `driver_task` (`driver/mod.rs`) when the last handle drops.
- `crates/imap/src/connection/mod.rs` - dropping `ImapConnection` does not
  abort the driver (only `terminate()` does, and `Pool` holds only `Weak`
  refs, which are dead by the time the Arc is dropped). So every path that
  just drops a connection - the push loop redial (`account/push.rs`, on
  `Disconnected`/`Bye`/resubscribe break), `PooledConn::discard`, dead members
  retained out of `idle` - hands the socket to a detached task that will sit
  in the LOGOUT read loop until the peer closes TCP or OS keepalive gives up.
  Against a stalled/half-open peer this accumulates leaked tasks and sockets
  for the account's lifetime; `Account::close` cannot reach them (the weak
  registry entry is gone). The DONE-handshake timeout added for IDLE
  (`connection/idle.rs`) shows the exact hazard was recognized one layer up;
  the terminal LOGOUT drain has the same shape and no bound. Fix: bound
  `logout_best_effort` with the command timeout (or a short fixed one) inside
  the driver.

### 3. Push IDLE loop has no backoff when dial+SELECT succeed but the session dies immediately

`crates/imap/src/account/push.rs::idle_loop` - `backoff` resets to 5s after
every successful SELECT, and the redial path after `event_closes_connection`
(BYE/ServerTerminated) or `idle()` error does **not** sleep. A server that
accepts connect/auth/SELECT but kills IDLE at once (aggressive per-user IDLE
policy, broken middlebox) produces a hot loop of full dial+TLS+auth+SELECT
cycles with zero delay, hammering the server and burning CPU/battery. The
backoff should also gate the redial after an in-session termination, not just
dial/SELECT failures.

## Latent defects / suspected

### 4. `open_raw_rfc822` does not stream and has no byte budget

`crates/imap/src/account/blob.rs::run_fetch` uses buffered `uid_fetch` with no
limit, materializing the entire message (arbitrarily large; adversarial server
unbounded) in one `Vec<FetchResponse>` before emitting it as a single `Bytes`
chunk. Both the doc comment ("streams the whole message") and
`reference/imap.md` ("streams the whole message via BODY.PEEK[]") claim
streaming. Every other body path has a budget (`HYDRATION_FETCH_BUDGET` 256
MiB, `DRAFT_FETCH_BUDGET` 64 MiB); this one has none. Should route through
`uid_fetch_streaming`/`uid_fetch_limited`.

### 5. Error-scope shape inconsistency the crate's own reference forbids

`reference/imap.md` (~line 411): "Folder producers build
`ErrorScope::Cursor(Folder(id))` ... not `ErrorScope::Mailbox { id }` -
`with_mailbox` is reached only from tests," precisely because a scope reader
matching only one shape degrades silently. But
`account/get.rs::uidvalidity_changed_error` and `account/blob.rs::run_fetch`'s
UIDVALIDITY error both build `ErrorScope::Mailbox { id }` for real
folder-scoped failures, while `account/mutate.rs::uidvalidity_changed_error`
builds `Cursor(Folder(..))` for the identical condition. Either the doc rule or
the two producers are wrong; the asymmetry is exactly the drift the doc warns
recreates the `ThrottleScope::Mailbox` bug.

### 6. `Pool::close` vs `PooledConn::drop` race leaves a member parked after close

`crates/imap/src/account/pool.rs` - `Drop` reads `is_closed()` and then pushes
to `idle` without holding the `sessions` linearization lock. Sequence: drop
reads `closed == false` -> `close()` sets flag, drains `idle` and `sessions`,
LOGOUTs/terminates the connection -> drop pushes the (now-terminated) member
into `idle`. Consequence is only a retained dead entry on a closed pool
(checkouts refuse anyway), so severity is low - but it falsifies the
`close_gates_every_way_into_the_pool` test's `idle_len == 0` postcondition
under the race, and the file's own comments claim "nothing can land a live
session on the far side of a completed close" via a lock the drop path doesn't
take.

### 7. `FolderRegistry::replace_personal` rebuilds every personal entry, discarding cursor and MODSEQ caches

`crates/imap/src/account/folder_registry.rs` - mid-session `refresh_folders`
(after any folder CRUD) retains only shared entries and creates fresh
`FolderEntry`s for all personal folders, zeroing their cursor cache and
50k-entry MODSEQ caches, and orphaning any `Arc<FolderEntry>` a concurrent
task holds (its subsequent per-entry writes land on the orphan). This is the
exact lost-update/in-place-refresh hazard `apply_mailbox_event` and
`refresh_listing` were carefully built to avoid, reintroduced one function
over. Cost today is a cold MODSEQ cache (unprotected STOREs) rather than
corruption, since cursor commits go through name lookup - but it deserves the
same preserve-in-place treatment: keep existing entries whose name survives,
remove the missing, add the new.

### 8. Dead code / unreachable fallback in `choose_idle_folder`

`account/push.rs` - the INBOX fallback for slot 0 is unreachable:
`push_subscribe` only inserts non-empty accepted folder-scope sets, and those
all pass `MailboxName::new`, so whenever the scopes map is non-empty
`subscribed_idle_folders` yields at least one entry and slot 0 always takes
index 0. If the fallback is meant to cover "subscriptions exist but none is
watchable," that state cannot occur; if it's meant as default-INBOX-push with
no subscriptions, the early `scopes.is_empty() -> None` defeats it. Either
intent is currently not served.

### 9. Resubscribe interrupt always redials even when the chosen folder is unchanged

`account/push.rs` - any subscribe/unsubscribe cancels the in-flight IDLE round
and the inner-loop `break` discards the connection; the outer loop dials a
brand-new session even if `choose_idle_folder` returns the same mailbox, and
emits a coarse `Unknown` invalidation for a window that a same-folder re-IDLE
on the same connection would not have lost. Costly on providers with strict
connection-rate limits when subscriptions churn. (The lost-events part is
documented as accepted; the unconditional redial is not.)

## Contract / documentation mismatches

### 10. `reference/imap.md` overstates `bulk_move` validation

"validates the destination mailbox once" - `mutate.rs::validated_move_destination`
validates only name syntax (`MailboxName::new`), not existence/selectability. A
well-formed nonexistent destination flows into the per-folder path and comes
back as per-item outcomes from the server's `NO [TRYCREATE]`, not the
documented up-front `Request(Malformed)` for every target.

### 11. Unaudited reference claims

`reference/imap.md` claims about APPENDLIMIT/STATUS handling, `X-GM-LABELS`,
ESEARCH normalization etc. were not independently verified in this pass (codec
and `pim.rs`'s long tail unread); flagged as unaudited rather than clean.

## bifrost-sasl: clean, with two notes

The crate is in excellent shape - SCRAM (RFC 5802/7677 vectors,
duplicate-attribute and nonce-extension guards, `i=` floor/ceiling, SASLprep on
both username and password), CRAM-MD5, RFC 5929 channel binding with a careful
DER walk and PSS parameter validation, OAuth payload framing with `\x01`
stripping. Two minor observations:

- `channel_binding.rs::tls_server_end_point` hashes the **entire input buffer**
  (`family.digest(cert_der)`), while `read_tlv` on the outer SEQUENCE tolerates
  trailing bytes (`_rest` ignored). If a transport ever hands DER with trailing
  garbage, the binding hash silently includes it and mismatches the server's.
  Rejecting non-empty `_rest` on the outer certificate would make it airtight;
  with native-tls as the only source today it's theoretical.
- `secret.rs::PartialEq` via `ct_eq` short-circuits on length mismatch
  (inherent to `subtle` slices) - acceptable, just noting the length leak is
  known-shape.

The IMAP-side consumption (`connection/auth.rs`, `connection/dispatch/auth.rs`)
is correct: SASLprep is applied via `prepare_scram_username` at consumer
construction, the GS2 header always comes from
`ScramChannelBinding::gs2_header()`, PLUS downgrade protection matches the
documented policy (`Ok(None)` only for absent cert; unusable cert aborts the
ladder), and `finalize` refuses a tagged OK that arrives before server-final
verification.

## Verified-solid areas (for the record)

Driver BYE ordering (`process_untagged_prefix` single application point),
cancellation safety of `read_one` (buffer-accumulating, `read_buf`
cancel-safe), the `(fetch_rx, fetch_fut)` future-is-authoritative pattern in
inventory/QRESYNC/CONDSTORE, `StoreConsumer`'s conditional-vs-unconditional
tagged-NO split, patch-STORE per-UID accounting, the QRESYNC mid-stream
downgrade discipline (only before a page escapes), cursor envelope versioning
and its legacy shapes, `CompactUidSet` normalization/diff, STARTTLS/COMPRESS
poisoned-sentinel upgrades, and pool close linearization (modulo finding 6)
all check out against both the RFCs and `reference/imap.md`.

## Architectural note (pre-1.0, aggressive-rewrite lens)

The one structural weakness worth real investment is that **the
one-outcome-per-id contract is enforced by convention across four hand-rolled
loops** (mutate flags/destroy/move, get). Finding 1's downstream blast radius
exists because "requested set -> wire operand -> outcome attribution" is
re-derived per path with no type making them agree. A small `TargetBatch` type
that owns the decoded ids, produces the wire `UidSet`, and is the *only* way to
mint outcomes (consuming each id exactly once, with an explicit lane for ids
excluded from the wire set) would make finding 1 and its whole class
unrepresentable, the same way `StoreConsumer::new(unchanged_since)` and
`SideEffectDigest` already killed their classes. That is where the hunter would
spend a rewrite, ahead of any strategy-trait consolidation in `changes.rs`
(which the code correctly argues against).
