# bifrost-imap and bifrost-sasl: hunt findings

Scope: `crates/imap/` - driver-task model, cancellation safety, streaming FETCH,
typed IDs, auth, and the account layer (QRESYNC / CONDSTORE / Basic cursor
strategy, per-folder modseq cache, opportunistic `STORE UNCHANGEDSINCE`). Plus
`crates/sasl/` in full.

Hunter note: both primary findings verified against the code (grep confirms
`tagged_ok: false` appears only inside `mod tests`; SCRAM `finalize` checks state
before the tagged status).

## 1. A SCRAM auth rejection is reported as a protocol error, not an auth failure

**High confidence.** `crates/imap/src/connection/dispatch/auth.rs`, `impl Consumer
for AuthenticateScramConsumer::finalize`. The state guard runs *before*
`require_ok_auth(tagged)`:

```rust
if self.state != ScramState::Done {
    return Err(Error::Protocol("SCRAM exchange ended before server-final verification".into()));
}
let tagged = require_ok_auth(tagged)?;
```

RFC 5802 lets a server reject client-final by simply returning a tagged `NO` with
no server-final `+`. When it does, the consumer is in `AwaitServerFinal`, so a wrong
password produces `Error::Protocol` instead of `Error::auth_with_code`. Per
`reference/error-model.md` that lands on `Protocol(ParseFailed)` /
`ProviderContractViolation` rather than the authentication lane, so the engine gets
a contract violation instead of `ReauthorizationRequired` and never prompts for
re-auth. `AuthenticatePlainConsumer` / `AuthenticateCramMd5Consumer` do not have
this bug - they call `require_ok_auth` first. Fix that keeps the security property:
match on `tagged.status` first, return the auth/bad error for `No`/`Bad`, and
enforce `state == Done` only on the `Ok` arm (a server still cannot skip
verification and claim success).

## 2. The tagged-NO `[MODIFIED ...]` STORE lane is dead code, and the real path loses per-UID conflict attribution

**High confidence.** `crates/imap/src/account/mutate.rs`. Both production call sites
of `StoreWireOutcome::from_response_code` hardcode `tagged_ok = true` (line 360,
line 742); `false` appears only in `mod tests`. `StoreConsumer::finalize`
(`connection/dispatch/fetch.rs`) does `tagged.require_ok()?`, so a tagged NO never
reaches `from_response_code` at all - it surfaces as `Err(Error::No)` and the group
falls into `failed_all(account_error_with(err, ..))`.

Consequences:

- `StoreWireOutcome::PendingRetry` and `StoreWireOutcome::Failed` are unreachable in
  production. The `pending_retry_conflict_is_failed_not_uncertain` test and the long
  comment documenting that lane pass against code that never runs - the "audit new
  tests for bite" failure mode in the standing lessons.
- A server that rejects `STORE UNCHANGEDSINCE` with `NO [MODIFIED 1,3]` (legal under
  RFC 7162 section 3.1.3) gets every UID in the group condemned with one generic
  classification. The conflicting UIDs never get `ConcurrencyConflict` /
  `Retry::AfterStateRefresh`, and the non-conflicting ones are indistinguishable from
  them.

Fix: `StoreConsumer` should return the tagged status alongside the code rather than
erroring on `NO`, or (smaller) the mutate layer should inspect `err.response_code()`
on `Error::No` before falling through to `failed_all`. Either restores the lane the
code already documents.

## 3. Seeded CONDSTORE/QRESYNC cycles announce new arrivals as `Updated`, never `Added`

**Medium-high confidence.** `account/changes.rs`, `run_condstore_with_baseline` and
`run_qresync`. When `known_uids_complete == false`, the baseline is seeded from a
live `UID SEARCH ALL` - which already contains every message that arrived since the
cursor's MODSEQ. The CHANGEDSINCE loop then guards with `if
known_uids.contains(uid)`, which is now true for those arrivals, so they emit
`updated_change` (an `ObjectChange`); and since `live_set = known_uids.clone()` on
the seeded path, the baseline diff is empty and no `Added` `ScopeChange` is ever
produced for them. The same shape holds in `run_qresync`: `live_uids.insert(uid)`
returns false for a seeded arrival, so `record_fetch_change` takes the `Updated`
branch.

The consumer therefore receives an update for an object it has no membership record
of. Whether that is lost data depends on how `bifrost-sync` handles
`ObjectChange::Updated` for an unknown id - worth confirming against the sync
findings - but the IMAP side is the producer that dropped the `Added`. The honest fix
is that a cursor with no complete baseline cannot be diffed at all: emit
`RestartScope` / force an inventory re-establish instead of manufacturing a baseline
that is by construction indistinguishable from the consumer's state.

## 4. The CONDSTORE and Basic paths buffer without bound; only QRESYNC pages

**Medium confidence, structural.** `run_condstore_with_baseline` calls
`uid_fetch_changed_since(ALL, .., modseq)` - the buffered, unbudgeted path - and
collects every result into a `Vec`, then accumulates all changes into a single
`Vec<Change>` that `finish_changes` emits as one `PageBoundary::Final` batch.
`run_basic` and `run_basic_from_selected` do the same. A folder whose cursor MODSEQ
is far behind (or a `Basic` folder of any size) materializes the whole mailbox twice
- once as `FetchResponse`s, once as `Change`s - with no `BATCH_ITEMS` flush and no
`FetchLimit` guard. `run_qresync` gets this right, streaming through
`uid_fetch_vanished_stream` and flushing at `BATCH_ITEMS`. The three strategies
should share one paging harness rather than each re-deriving the loop; that is also
where finding 3 would be fixed once instead of twice.

## 5. A per-group STORE wire error is reported `Failed`, not `Uncertain`

**Medium confidence.** `run_destroy_mutation_groups` (line 361) and
`run_flag_mutation_groups` (line 461) both route `Err(err)` from `uid_store` into
`failed_all`. A timeout or connection drop mid-STORE leaves the server state
genuinely unknown, which is exactly what `ItemOutcome::Uncertain` exists for - and
`flush_mutation_groups` already uses `uncertain_all` for the folder-level analogue.
Reporting `Failed` tells the engine the mutation definitively did not happen. For
flag ops the retry is idempotent so impact is low; for `bulk_destroy` the `\Deleted`
mark may have landed and the engine will not reconcile it. The `Failed`/`Uncertain`
split should be driven by the classified error's transmission state, not by which
loop caught it.

## Smaller / lower confidence

- **`ErrorScope::Mailbox` from folder producers.** `concurrency_conflict_error`,
  `store_failed_error`, and `uidvalidity_changed_error` (`mutate.rs`) build
  `.scope(ErrorScope::Mailbox { id })` directly. `reference/imap.md` states folder
  producers use `with_folder_scope` -> `ErrorScope::Cursor(Folder(id))` and that
  `with_mailbox` "is reached only from tests". Readers are documented to accept both
  shapes, so this is currently benign, but it is the exact asymmetry that previously
  made `ThrottleScope::Mailbox` unreachable. Low confidence of live impact, high
  confidence it is a latent trap.
- **`store_failed_error` is classified `Request(Malformed)`.** A STORE the server
  refused is far more often an ACL denial or a server-side failure than a malformed
  request, and `Request(Malformed)` derives a terminal, non-retryable class.
  Currently unreachable (finding 2), so this only matters once that lane is revived -
  but it should be fixed in the same change.
- **`FolderCursor::Basic::uidnext` is write-only.** Encoded, decoded,
  round-trip-tested, and never read: `run_basic` diffs `known_uids` against a fresh
  `SEARCH ALL` and ignores `uidnext` entirely. Either use it (a cheap
  `uidnext`-unchanged short-circuit that skips the full SEARCH would be a real win on
  the Basic path) or drop the field.
- **`run_basic` emits `updated_change` for `selected.mailbox.changed_messages` with
  no `known_uids.contains` guard**, unlike the CONDSTORE path which added exactly that
  guard for exactly this hazard. On a Basic cursor `changed_messages` should be empty,
  so this is probably unreachable - but if a server ever populates it, a new arrival is
  reported as both `Updated` and `Added`.
- **`CompactUidSet::diff` expands both sides to individual UIDs** despite the type
  existing to avoid exactly that; the doc comment claims it avoids expansion, but
  `iter()` yields one `u32` per UID and `added`/`removed` are `Vec<u32>`. Cosmetic for
  correctness, real for a 500k-UID mailbox.

## bifrost-sasl

No computation defect found. SCRAM matches the RFC 5802 and RFC 7677 vectors, nonce
extension is strictly enforced, `i=` is bounded on both sides, duplicate attributes
are rejected by both `scram_field` and `validate_scram_attributes`, verifier
comparison is constant-time, and SASLprep runs on both username and password.

The DER walk in `channel_binding.rs` is lenient about non-minimal long-form lengths
and does not reject trailing bytes after the outer `Certificate` SEQUENCE - but since
`family.digest(cert_der)` hashes the whole input buffer regardless, parse leniency
cannot change the binding value, only the family selection, and every
family-selection path that is ambiguous is already a hard error.

Two cosmetic notes: `cram_md5_response` does not zeroize the raw HMAC digest bytes
(it does zeroize the assembled response), and `xor_bytes` silently truncates on a
length mismatch (unreachable today, since both inputs are always the same digest
width).

## Structural read

The account layer's three change strategies are three hand-written loops that
re-derive the same five decisions - select, validate UIDVALIDITY/MODSEQ,
seed-or-diff the baseline, page the output, checkpoint. Every finding above except 1
and 2 is a place where one of the three got a decision that the others did not
(QRESYNC pages, the others do not; CONDSTORE guards arrivals, Basic does not; QRESYNC
tracks baseline completeness, CONDSTORE's cursor variant has no field for it). The
right shape is one strategy-parameterized runner with a `Strategy` trait supplying
only the parts that genuinely differ - the change-source (VANISHED stream /
CHANGEDSINCE fetch / nothing) and the next-cursor constructor - with paging, dedup,
baseline handling, and checkpointing owned once. That is a rewrite of `changes.rs`,
and given pre-1.0 and the correctness holes it would close, the hunter's call is that
it is the right move rather than patching findings 3 and 4 in place.
