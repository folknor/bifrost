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

(Finding 2 - the unbounded terminal LOGOUT drain leaking detached driver
tasks and sockets against a silent peer - is fixed: the drain is wrapped in a
5s `LOGOUT_DRAIN_TIMEOUT` inside `driver_task`. Pinned by the paused-time
test `driver_logout_drain_is_bounded_against_a_silent_peer` with
revert-and-confirm; documented in `reference/imap.md`.)

## Latent defects / suspected

(Finding 4 - `open_raw_rfc822` buffering the whole message with no byte
budget while both the doc comment and the reference claimed streaming - is
fixed: `run_fetch` now drives `uid_fetch_stream`, emits each body section as
it arrives, and takes an explicit budget (`RAW_FETCH_BUDGET`, 256 MiB) whose
crossing is `Error::FetchLimit`; the command future stays authoritative.
Pinned by `open_raw_rfc822_emits_chunks_before_the_tagged_completion` (a
transcript gated on the first chunk reaching the consumer) and
`a_raw_message_read_stops_at_its_byte_budget`, both revert-and-confirmed;
documented in `reference/imap.md`.)

(Finding 6 - `PooledConn::drop` reading `is_closed()` outside the lock and
parking a terminated member after `close` had drained - is fixed: the drop
path now re-checks `closed` while holding the `idle` mutex, in the same
critical section as the push, which is the linearization `close`'s
store-then-drain order already relies on. Pinned by
`a_drop_that_finishes_after_the_close_drain_parks_nothing`, which forces the
interleaving by holding the idle lock across close's linearization rather
than racing it; revert-and-confirmed 3/3. Documented in
`reference/imap.md`.)

(Finding 7 - `replace_personal` rebuilding every personal entry and so
zeroing cursor and MODSEQ caches on each folder CRUD - is fixed: surviving
names keep their existing `Arc<FolderEntry>` with `refresh_listing` applied
in place, missing names are removed, new names are added; a retained shared
entry keeps its own MYRIGHTS-derived listing. Pinned by
`replace_personal_preserves_surviving_entries_in_place` (`Arc::ptr_eq`,
cache survival, write-through-the-held-Arc, attribute refresh, removal)
with revert-and-confirm; documented in `reference/imap.md`.)

(Finding 9 - the resubscribe interrupt redialing unconditionally and
emitting a coarse `Unknown` even when `choose_idle_folder` returns the same
mailbox - is fixed: the interrupt re-chooses first and, on an unchanged
assignment, re-IDLEs on the same connection with the round's own event
published normally and `NOTIFY SET` re-issued for the new watched set; only
a changed or absent assignment breaks out to redial and signals the coarse
loss. The decision is the pure `resubscribe_action`, pinned by
`a_resubscribe_keeps_the_connection_when_the_folder_is_unchanged`.
Caveat: only the decision function is pinned. The loop around it is not
hermetically testable - an IDLE worker's connection comes from
`pool.dial_idle()`, i.e. a real dial - so the wiring rests on review.
Documented in `reference/imap.md`.)

## Contract / documentation mismatches

(Finding 11 - the three named unaudited reference claims - is now verified
against the code, with no discrepancies found:

- APPENDLIMIT/STATUS (`connection/append.rs`): numeric `APPENDLIMIT=<n>` is
  the global limit, bare `APPENDLIMIT` triggers the preflight
  `STATUS <mailbox> (APPENDLIMIT)`, both advertised takes `global.min(mailbox)`,
  `mailbox_append_limit` reads `items` chained with `ambiguous` and takes the
  smallest named value, an omitted item is `Error::Protocol`, and both the
  STATUS failure (`unsent_preflight`) and a deadline that expires before
  `submit_prebuilt` (`remaining_timeout`) are stamped `Unsent`.
- `X-GM-LABELS` (`codec/decode/envelope_fetch.rs`): the label grammar is
  `"\" atom / astring`, astring labels are MUTF-7 decoded and system labels
  kept verbatim, `X-GM-MSGID`/`X-GM-THRID` accept the quoted decimal, all
  three are in `has_closed_grammar`, `X-GM-EXT-1` gates the request
  (`connection/helpers.rs`), and labels count toward the buffered-FETCH byte
  estimate (`dispatch/fetch.rs`).
- ESEARCH (`connection/mod.rs::expand_uid_ranges`,
  `dispatch/search.rs`): `*` is refused wherever it appears (not only as an
  endpoint), ranges are sorted and merged - adjacent ones too - before both
  the 1e6 cap check and the expansion, an unexpandable set is
  `Error::SearchResultTruncated` rather than a partial list, and
  `reclassified_extras` keeps the chosen solicited ESEARCH consumed by the
  command instead of republishing it as an event.

The rest of `pim.rs`'s long tail and the codec internals remain unread and so
still unaudited.)

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

## Architectural note (pre-1.0, aggressive-rewrite lens) - CLOSED

(Built as ruling 3 of the 2026-09-04 structural rulings. `TargetBatch` lives
in `crates/imap/src/account/targets.rs`: it takes the decoded ids, owns the
wire `UidSet`, keeps an explicit excluded lane, and is the only way to mint
the batch's outcomes - `settle` debug-asserts exactly-once coverage and an
unsettled batch panics on drop. All four hand-rolled loops now route through
it: flag mutation, its two-sided patch group, destroy, move, and hydration in
`get.rs`. Pinned by the `account::targets` tests, each revert-and-confirmed;
documented in `reference/imap.md` under "One outcome per id". The original
note follows.)

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
