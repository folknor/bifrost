# Bug hunt: bifrost-imap + bifrost-sasl

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/imap/` (driver, wire framing, pool, push/IDLE, auth, change
strategies, mutations, inventory, hydration, blob, cursor envelope, folder
registry) plus `crates/sasl/`.

Coverage note from the hunter: `pim.rs`'s long tail, `sieve.rs`, `factory.rs`,
`scopes.rs`, codec internals, and pipeline were not read in depth; findings
note where confidence is bounded by that.

## Confident defects

(Finding 1 - `decode_object_id` accepting uid 0 and the downstream
zero-filtering fabricating successes / dropping outcomes - is fixed at the
decode boundary: `decode_object_id` and `decode_thread_id` now reject a 0 uid
or uidvalidity as `Request(Malformed)`, closing every downstream hole at once.
Pinned by `object_and_thread_ids_reject_uid_zero_and_uidvalidity_zero` with
revert-and-confirm; documented in `reference/imap.md`.)

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

### 9. Resubscribe interrupt always redials even when the chosen folder is unchanged

`account/push.rs` - any subscribe/unsubscribe cancels the in-flight IDLE round
and the inner-loop `break` discards the connection; the outer loop dials a
brand-new session even if `choose_idle_folder` returns the same mailbox, and
emits a coarse `Unknown` invalidation for a window that a same-folder re-IDLE
on the same connection would not have lost. Costly on providers with strict
connection-rate limits when subscriptions churn. (The lost-events part is
documented as accepted; the unconditional redial is not.)

## Contract / documentation mismatches

### 11. Unaudited reference claims

`reference/imap.md` claims about APPENDLIMIT/STATUS handling, `X-GM-LABELS`,
ESEARCH normalization etc. were not independently verified in this pass (codec
and `pim.rs`'s long tail unread); flagged as unaudited rather than clean.

## bifrost-sasl: clean

The crate is in excellent shape - SCRAM (RFC 5802/7677 vectors,
duplicate-attribute and nonce-extension guards, `i=` floor/ceiling, SASLprep on
both username and password), CRAM-MD5, RFC 5929 channel binding with a careful
DER walk and PSS parameter validation, OAuth payload framing with `\x01`
stripping. (Its two minor observations - trailing DER bytes after the outer
certificate SEQUENCE, and the ct_eq length short-circuit - are closed: the
former is now rejected, the latter documented as accepted.)

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
