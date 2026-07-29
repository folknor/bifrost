# bifrost-imap: `account/**` + `types/**` bug hunt

Scope: `crates/imap/src/account/**`, `crates/imap/src/types/**`,
`crates/imap/src/error.rs`, `error_tests.rs`, `lib.rs`.

Findings are ordered by severity within each section. Every bug is
reported with a proposed fix; **no fix in this list has been applied**.
The only edits landed alongside this document are tests (listed at the
end), which pin behavior as it exists today.

---

## Bugs

### B1 - `search_plan` splices `provider_query` verbatim, and one `)` hangs the task forever

`account/pim.rs:1332-1359`.

`search_plan` performs exactly three operations on `SearchRequest::provider_query`:
`trim()`, "replace ALL or append with a space", and "empty means ALL".
There is no syntactic validation of any kind. The result becomes
`SearchPlan::criteria`, which both public entry points hand straight to
the connection layer:

- `pim.rs:835` - `search_messages` -> `conn.uid_search(&plan.criteria, ..)`
- `pim.rs:790-797` - `search` -> `conn.uid_thread("REFERENCES", "UTF-8", &plan.criteria, ..)`

Both call `validate_search_criteria_capabilities(criteria)`
(`connection/uid_ops.rs:347`, `connection/sort_thread.rs:152`) and both
call it **outside** the `tokio::time::timeout` wrapper. The sibling
agent's finding is that `search_criteria_contains_atom` never advances
past a top-level `)`:

- `search_criteria_contains_atom_in_key` sees a byte that is not `(`, so
  it calls `search_criteria_consume_item`.
- `consume_item` treats `)` as a token terminator, so the bare-atom loop
  exits immediately with `*pos == start`, returns `None`, and does not
  advance `pos`.
- `contains_atom_in_key` returns `false` with `pos` unchanged.
- The outer `while i < bytes.len()` loop re-enters at the same offset.

**Path to failure.** `account.search_messages(SearchRequest::provider(")"))`
on a folder-bearing account. `search_plan` yields `criteria == ")"`.
`uid_search` -> `validate_search_criteria_capabilities(")")` ->
`require_condstore_for_modseq_criterion` -> `search_criteria_contains_atom(")", "MODSEQ")`
spins on a synchronous, non-yielding loop inside an async fn. The tokio
worker thread is pinned at 100% and never returns. The `timeout` at
`uid_ops.rs:348` is downstream of the call, so it cannot fire; and even
if it were upstream, the loop never reaches an await point, so the
timeout future would never be polled. On a single-threaded runtime the
entire account is dead; on a multi-threaded one a worker is burned
permanently and the caller's future never resolves.

Shorter reproductions that are plausible from a real UI: any raw query
where a `)` outlives its `(`. `"FROM alice)"`, `"(UNSEEN))"`,
`"OR (A) (B))"`. Note the *balanced* forms are fine - the paren-group
arm of `contains_atom_in_key` consumes its own `)`. It is only a
top-level unmatched `)` that traps.

**Two independent defects here, and both should be fixed.**

1. The scanner must never fail to advance (connection agent's half).
2. `search_plan` should not be handing arbitrary consumer text to a
   scanner that assumes well-formed input. `provider_query` is a
   consumer-supplied string reaching an IMAP command; the CRLF-injection
   guard at `codec/encode/commands.rs:143` (`validate_search_criteria_crlf`)
   is currently the *only* thing standing between it and the socket.

**Proposed fix (my half).** Add a pure `validate_provider_query(raw) -> Result<(), AccountError>`
in `pim.rs`, called from `search_plan` before the splice, rejecting with
`pim_malformed`:

- unbalanced parentheses (single pass, depth counter, reject on depth
  going negative and on a non-zero depth at end);
- unbalanced double quotes (odd count of unescaped `"` outside a literal);
- an embedded `{n}` literal (the account layer never has a reason to emit
  one, and it is the one construct that makes the criteria string
  non-self-describing).

That is cheap, pure, unit-testable, and it closes the class rather than
the instance. It also converts an infinite hang into a
`Request(Malformed)` the engine can classify.

**Everything else in `account/` that builds criteria is already safe** -
I checked each arm of `criteria_from_filter` (`pim.rs:1366-1407`):

| filter | path | guard |
| --- | --- | --- |
| `From` / `To` / `Subject` / `Body` / `Has` | `SearchCriteria::push_string` | `quote_imap_string` rejects NUL/CR/LF, escapes `\` and `"` |
| `Labeled(LabelId)` | `SearchCriteria::keyword` | `validate_atom_bytes` |
| `DateRange` | `imap_date` -> `push_date` | value is generated, then `validate_imap_date` |
| `In(ContainerId)` | `folder_from_container` | goes to `plan.folder`, never into `criteria` |
| `And` / `Or` / `Not` | recursive | inherits the above |

So `provider_query` is the single unvalidated ingress. `sieve.rs` is
clean too - every wire string goes through `quote_string` /
`script_name_arg` / `literal_command`, all of which reject NUL and CRLF.

### B2 - a corrupt cursor with a zero UID start panics the process in debug builds

`account/envelope.rs:241-264` (`CursorBytes::take_uid_set`), reaching
`types/uid_range.rs:17` and `:32`.

`take_uid_set` reads `start` and `end` from untrusted cursor bytes and
calls the **unchecked** constructors:

```rust
let range = if end == 0 {
    crate::types::UidRange::single(start)      // debug_assert!(uid != 0)
} else if end < start {
    return Err(schema_incompatible(..));
} else {
    crate::types::UidRange::range(start, end)  // debug_assert!(start != 0)
};
```

Both constructors carry `debug_assert!(.. != 0, "UID must be non-zero
(RFC 3501 Section 9: uniqueid = nz-number)")`.

**Path to failure.** A persisted `ChangeCursor` whose payload contains a
range with `start == 0` (bit-flip in the checkpoint store, a truncated
write that leaves a zeroed tail inside a declared range count, a
checkpoint restored from a different build). `decode_cursor` is called
from `changes::run_changes` and `changes::describe_cursor`. In a debug
build - which is what `brokkr check` runs the whole suite under - this
**panics** instead of returning the `SyncState(SchemaIncompatible)` the
function is explicitly designed to return for structurally-bad bytes. In
release the assert is compiled out and a `UidRange { start: 0, end: None }`
is minted; `CompactUidSet::from_ranges` then filters the 0 out, so
release silently recovers. A behavior difference between profiles on
untrusted input is exactly the shape that gets found in production first.

Note the asymmetry: the author already thought about malformed ranges
here (`end < start` is rejected explicitly with a comment) but reached
for the panicking constructors for the zero case.

**Proposed fix.** Use the checked constructors that already exist:

```rust
let range = if end == 0 {
    crate::types::UidRange::try_single(start)
} else if end < start {
    return Err(schema_incompatible("IMAP cursor UID range has end before start"));
} else {
    crate::types::UidRange::try_range(start, end)
}
.ok_or_else(|| schema_incompatible("IMAP cursor UID range contains a zero UID"))?;
```

I did not write a test for this: a `#[should_panic]` test would pass in
debug and fail in release, and `brokkr test` runs release.

### B3 - `take_uid_set` allocates from an untrusted length before reading the data

`account/envelope.rs:242-243`.

```rust
let count = self.take_u32()? as usize;
let mut ranges = Vec::with_capacity(count);
```

`count` is four bytes of untrusted cursor payload. A corrupt cursor
declaring `0xFFFFFFFF` ranges makes the process request
`4_294_967_295 * size_of::<UidRange>()` = roughly 34 GB in one
allocation before a single range byte is validated. That is an
allocation failure -> `abort()`, i.e. the whole process, not a
recoverable `SchemaIncompatible`.

**Path to failure.** Same trigger surface as B2: any corruption of the
first four bytes of the uid-set section of a stored cursor. The decode
loop *would* have failed on the very next `take_u32` (there are no bytes
left), so the allocation is pure waste even in the benign case.

**Proposed fix.** Bound the reservation by what the remaining input can
possibly contain - each range costs 8 bytes:

```rust
let count = self.take_u32()? as usize;
let mut ranges = Vec::with_capacity(count.min(self.remaining() / 8));
```

`remaining()` already exists on `CursorBytes`. The subsequent
`take_u32` calls still produce the correct `SchemaIncompatible` on
truncation; only the pre-allocation changes.

### B4 - the pool never evicts a dead connection, so one network blip poisons a slot for the account's lifetime

`account/pool.rs:156-172`.

```rust
impl Drop for PooledConn {
    fn drop(&mut self) {
        if let Some(member) = self.member.take()
            && !matches!(member.conn.session_state(), SessionState::Logout)
            && !self.pool.closed.load(Ordering::Acquire)
        { self.pool.idle.lock()...push(member); }
    }
}
```

The only health signal consulted is `SessionState`, which is the RFC 3501
*protocol* state machine (`NotAuthenticated` / `Authenticated` /
`Selected` / `Logout`). It says nothing about whether the socket or the
driver task is alive. Searching the crate, the only writers of
`SessionState::Logout` are the BYE greeting, an explicit LOGOUT, and
`apply_infrastructure_failure` - and `apply_infrastructure_failure` is
called from exactly one place, `connection/driver/upgrade.rs` (STARTTLS /
COMPRESS stream upgrades). A plain read/write failure, a TCP reset, a
server-side idle timeout, or a driver-task panic never moves the session
state.

**Path to failure.** A `changes_stream` for INBOX checks out a pooled
connection. Mid-`UID FETCH` the server drops the TCP connection. The
driver task exits; `cmd_tx` closes. `run_changes` returns an error; the
`PooledConn` is dropped; `session_state()` still reports `Selected`, the
pool is not closed, so the dead member is **pushed back onto the idle
list**. The engine retries. `checkout_for_folder` pops the same dead
member (`idle.pop()` at `pool.rs:69`). Every subsequent command on it
fails immediately. Because each failure returns the same member to the
pool, the slot never heals: the account is stuck failing until the engine
gives up and reopens. With `pool_cap` at its default 4, three dead
members will starve the account entirely.

`PooledConn::discard()` exists and does the right thing, but it is called
from exactly one place in the whole crate - the QRESYNC parse-error path
in `changes.rs:260,431`. No error path calls it.

Note also that `reference/imap.md` lines 15-22 claim a connection state
machine of `Ok` / `Broken` / `Closed` with "A dropped future leaves the
connection `Broken`; the pool discards it." The string `Broken` does not
appear anywhere in `crates/imap/src/` outside `types/envelope.rs` (an
unrelated use). That documented invariant is not implemented (see G5).

**Proposed fix.** Two parts, one of which is outside my scope:

1. (connection/, not mine) expose liveness on `ImapConnection`:
   `pub(crate) fn is_alive(&self) -> bool { !self.cmd_tx.is_closed() }`.
   The driver's `JoinHandle` is already retained for panic observation,
   so this is a one-line accessor over state that already exists.
2. (pool.rs, mine) consult it in both directions:
   - in `Drop`, add `&& member.conn.is_alive()` to the re-pool condition;
   - in `checkout_for_folder`, filter dead members out of the idle list
     before the `position` / `pop`, so a member that died while parked
     is discarded rather than handed out.

Until (1) lands, a weaker but still useful in-scope mitigation is for
`ImapAccount::select_folder` to `conn.discard()` on any error, since a
failed SELECT is already a strong signal that the connection is not
reusable.

### B5 - a partially-conflicting `FlagOp::Patch` reports full success for the messages it only half-applied

`account/mutate.rs:467-529` (`apply_patch`), consumed at `:341-347`.

`FlagOp::Patch { add, remove }` is two STORE commands, because IMAP has
no combined add-and-remove. The first (`+FLAGS.SILENT`) carries the
`UNCHANGEDSINCE` guard; the second (`-FLAGS.SILENT`) runs unguarded, by
design. The early-return is:

```rust
if !matches!(first, StoreWireOutcome::Applied) {
    return Ok(first);
}
```

`StoreWireOutcome::Modified(uids)` is a **tagged-OK** STORE that named
some conflicting UIDs: the non-conflicting UIDs in that same command
*did* get the add. `apply_patch` returns early, so the remove half never
runs for them. `mutation_results` (`:602-616`) then maps
`Modified(modified)` to `Succeeded(BatchSuccess::new(item, MutationSuccess::Applied))`
for every id **not** in `modified`.

**Path to failure.** CONDSTORE server, warm MODSEQ cache (so
`partition_by_modseq` produces a guarded group), three messages
UID 1/2/3 at the same cached MODSEQ, and a
`FlagOp::Patch { add: {"$flagged"}, remove: {"\\Seen"} }`. A concurrent
client touches UID 2. The server answers
`OK [MODIFIED 2] STORE completed`. Result:

- UID 1, 3: `\Flagged` set, `\Seen` **still set**, reported
  `Succeeded(Applied)`.
- UID 2: reported `Failed(ConcurrencyConflict)`, correct.

The engine's read-back-after-retry safety net is only engaged for the
failed and uncertain lanes, so UID 1 and 3 keep a wrong `\Seen` locally
and remotely with no signal that anything is outstanding. The comment
above the early return says "the patch did not fully land; surface it
rather than charging ahead" - the intent is right, the surfacing is not.

**Proposed fix.** `apply_patch` needs to distinguish "nothing landed"
from "part landed". Minimal change: on `Modified(conflicts)` from the
first STORE, run the second STORE unguarded against the non-conflicting
subset and return an outcome that reflects both halves. That requires
threading the requested UID set into `apply_patch` (it currently only
receives the encoded `SequenceSet`), so the cleaner shape is to move the
patch expansion up into `run_flag_mutation_groups`, where `uids` is
already in hand:

- first STORE guarded against the whole group;
- compute `applied = applied_uids_after_store(&uids, &first)` - that
  helper already exists and already does this exact subtraction;
- second STORE unguarded against `applied` only;
- report `Modified(conflicts)` merged with any second-command failure.

Failing that, the conservative fix is to report `Modified` from a
two-sided patch as `Uncertain` for the non-conflicting UIDs rather than
`Succeeded`, which at least routes them into the engine's read-back.

### B6 - `folder_role` hardcodes `/` as the hierarchy delimiter, so the Sent / Drafts / Trash roles vanish on a `.`-delimited server

`account/pim.rs:1694-1718`.

```rust
let lower = name.rsplit('/').next().unwrap_or(name).to_ascii_lowercase();
match lower.as_str() { "inbox" => .., "sent" | .. }
```

The SPECIAL-USE arm above it is correct. The name-based fallback - which
is the entire point of the function on a server without SPECIAL-USE -
assumes `/`. The IMAP hierarchy delimiter is per-server and reported by
LIST; Courier and several stock Dovecot layouts use `.` with an `INBOX.`
prefix, and Cyrus historically used `.` too. The delimiter is *already
available*: `FolderEntry::delimiter` is populated from LIST, and
`parent_id` (`pim.rs:1720-1725`) takes it as a parameter and uses it
correctly. `folder_role` just does not receive it.

**Path to failure.** A Dovecot/Courier account with `INBOX`,
`INBOX.Sent`, `INBOX.Drafts`, `INBOX.Trash` and no SPECIAL-USE
advertisement:

- `folder_role(&[], "INBOX.Sent")` -> leaf is the whole string
  `"inbox.sent"` -> no match -> `None`.
- `role_folder(account, FolderRole::Sent)` -> `None`.
- `append_to_sent_or_fallback` (`pim.rs:364`) logs
  "save_to_sent requested but no Sent folder resolved" and every sent
  message is missing from the Sent folder, silently, forever.
- `draft_create` (`pim.rs:198`) -> `Unsupported(DraftCreate)`.
- `delete_thread` (`pim.rs:1088,1098`) -> `Unsupported(BulkMove)`, so
  deleting a thread fails outright.
- `containers_list` reports every folder with `role: None`, so the
  consumer cannot render a mailbox tree with roles either.

The same assumption is duplicated in `capabilities.rs:26-32`
(`has_drafts` matches the *whole* folder name against `"drafts"`), so
`draft_create` is additionally advertised `false` - at least that pair is
self-consistent.

**Proposed fix.** Thread the delimiter through. `folder_role` is called
from `container_from_folder_entry` (which has the entry, and therefore
`entry.delimiter`), `role_folder` (same), and `delete_thread` (which
looks the entry up). Change the signature to
`folder_role(attributes: &[MailboxAttribute], name: &str, delimiter: Option<char>)`
and split on the supplied delimiter, falling back to the current `/`
behavior when `delimiter` is `None`. `has_drafts` in `capabilities.rs`
should use the same leaf extraction rather than a whole-name compare.

### B7 - `remove_from_container` and `draft_discard` are advertised on servers that cannot perform them

`account/capabilities.rs:67,79`.

```rust
remove_from_container: true,
draft_discard: true,
```

Both are implemented by `pim::delete_messages` (`pim.rs:1143-1180`),
which is `UID STORE +FLAGS.SILENT (\Deleted)` followed by
`UID EXPUNGE`. `ImapConnection::uid_expunge` (`connection/uid_ops.rs:637-647`)
requires the UIDPLUS capability, or IMAP4rev2 which folds it in:

```rust
if !snap.capabilities.contains(&Capability::UidPlus)
    && !super::auth::is_rev2_from_snapshot(&snap)
{ return Err(Error::MissingCapability("UIDPLUS".into())); }
```

The sibling flag `draft_create` is correctly gated on exactly this
(`draft_create: has_drafts && profile.supports(Capability::UidPlus)`),
which is what makes these two look like an oversight rather than a
deliberate rule.

**Path to failure.** A plain RFC 3501 server without UIDPLUS. Engine
reads `pim_methods.remove_from_container == true`, calls it, and gets a
runtime `Unsupported(RemoveFromContainer)` from the capability mapping -
after the `\Deleted` STORE has already committed. The message is now
flagged deleted and not expunged, in a folder the consumer believes it
was removed from. Same for `draft_discard` on a draft.

`bulk_destroy` (`mutate.rs:243-317`) has the same wire shape and the same
partial-side-effect problem, though it has no `pim_methods` flag to
mis-advertise; see G12.

**Proposed fix.** Gate both on the same condition `draft_create` uses:

```rust
let can_expunge_by_uid = profile.supports(Capability::UidPlus) || profile.imap4rev2;
remove_from_container: can_expunge_by_uid,
draft_discard: can_expunge_by_uid,
```

`ServerProfile` already carries `imap4rev2` (used at
`factory.rs:505,621`), so no new plumbing is needed. Note `draft_create`
itself is *missing* the rev2 half of the gate today - an IMAP4rev2 server
that does not separately advertise UIDPLUS gets `draft_create: false`
even though it supports APPENDUID. Same one-line fix covers it.

### B8 - the `Preview` projection returns headers and drops the preview text

`account/get.rs:224-232` and `:273-280`.

`attrs_for_projection(Projection::Preview(count))` requests two data
items:

```rust
Projection::Preview(count) => vec![
    FetchAttr::Uid,
    FetchAttr::Rfc822Header,
    FetchAttr::BodySection { peek: true, section: Some("TEXT"), partial: Some((0, count)) },
],
```

`RFC822.HEADER` is decoded into `body_sections` with `section: "HEADER"`
(`codec/decode/envelope_fetch.rs:211-222`), so *both* items land in the
same `Vec<BodySection>`. `fetch_to_hydrated` then does:

```rust
let bytes = fetch.body_sections.into_iter()
    .find_map(|section| section.data)
    .unwrap_or_default();
HydratedObjectKind::RawMime(bytes)
```

`find_map` takes the **first** section carrying data. Servers answer
FETCH data items in request order, so that is the HEADER block; the
preview text is discarded.

**Path to failure.** `get_stream(ids, Projection::Preview(512))` returns
`RawMime(<header block>)` for every message. A consumer parsing that as
MIME sees a message with headers and an empty body - the list view shows
no snippet at all, for every message, on every server.

**Proposed fix.** Concatenate rather than pick. The projection's
contract is "Headers + N bytes of decoded text/plain", so the natural
`RawMime` payload is `HEADER` bytes + a blank line + `TEXT` bytes, which
is a parseable MIME document. `TextOnly` and `Full` request one section
each and are unaffected either way, so the concatenation can be applied
unconditionally:

```rust
let mut bytes = Vec::new();
for section in fetch.body_sections { if let Some(data) = section.data { bytes.extend_from_slice(&data); } }
```

...with the caveat that the HEADER block already ends in CRLF CRLF from
the server, so no separator needs synthesizing. Worth confirming against
one real server before landing.

`account/get.rs::preview_hydration_keeps_only_the_first_returned_section`
pins the current behavior and is explicitly labelled as documenting a bug.

### B9 - `HydrationProjection::Full` puts the entire raw message, headers and all, into `Message::body_text`

`account/pim.rs:1811-1835` and `:1845-1890`.

`attrs_for_hydration(Full | FullWithBlobs)` requests
`BODY.PEEK[]` - the whole RFC 5322 message. `fetch_to_message` then does:

```rust
let body = fetch.body_sections.iter()
    .find_map(|section| section.data.as_ref())
    .map(|bytes| String::from_utf8_lossy(bytes).into_owned());
...
body_text: match projection { HydrationProjection::Headers => None, _ => body.clone() },
body_html: None,
attachments: Vec::new(),
```

So `message_hydrate(id, Full)` returns a `Message` whose `body_text` is
`"Subject: ...\r\nFrom: ...\r\nContent-Type: multipart/alternative; boundary=...\r\n\r\n--...\r\nContent-Transfer-Encoding: base64\r\n\r\nU3Vi..."`.
No MIME parsing happens anywhere on this path: `body_html` is
unconditionally `None` and `attachments` unconditionally empty, so a
multipart message's actual text is never extracted and a
quoted-printable or base64 body is handed over still encoded.

**Path to failure.** Any consumer rendering `Message::body_text` from
`message_hydrate(.., Full)` or `thread_hydrate` (which passes
`HydrationProjection::Full` at `pim.rs:1016`) shows raw MIME source to
the user.

**Proposed fix.** Two defensible options, pick one deliberately:

- *Narrow*: `Full` should request `BODY.PEEK[TEXT]` rather than
  `BODY.PEEK[]`, matching what `body_text` claims to hold. Still wrong
  for multipart and still un-decoded, but no longer contains headers.
- *Correct*: run the fetched octets through the shared
  `bifrost-types::mime` machinery the send path already uses, filling
  `body_text` / `body_html` / `attachments` properly. That is a real
  feature, not a bug fix, and should be scoped as one.

Either way the current state is a trap: the field name promises
something the value is not. `account/pim.rs::full_hydration_puts_the_whole_raw_message_in_body_text`
pins it, labelled.

### B10 - three folder-selection paths iterate a `HashMap`, so the target is nondeterministic run to run

`account/pim.rs:1727-1745` (`role_folder`, `quota_probe_folder`),
`account/mutate.rs:120` (`mutation_stream`).

`FolderRegistry::entries()` collects `HashMap::values()`. Rust's default
hasher is randomly seeded per process, so the iteration order differs on
every run.

- `role_folder(account, role)` does `.find(|entry| folder_role(..) == Some(role))`
  and takes the **first** match in that arbitrary order.
- `quota_probe_folder` falls back to `.find(|entry| entry.selectable)`.
- `mutation_stream` does `for (_name, (folder, ids)) in grouped` over a
  `HashMap`, so multi-folder mutation batches process folders in an
  arbitrary order.

**Path to failure for `role_folder`.** Any account where two folders map
to the same role. This is common, not exotic:

- Apple Mail creates `Sent Messages` alongside a server's `Sent`; both
  match the name fallback (`pim.rs:1711`).
- A server advertising `\Trash` on `Corbeille` *and* carrying a literal
  `Trash` folder: both match.
- Gmail: `[Gmail]/Sent Mail` (leaf `sent mail`) plus a user-created
  `Sent`.

Then `append_to_sent_or_fallback` writes the Sent copy into a folder
chosen at random per process. Restart the app and sent mail starts
landing in a different folder. `draft_create` picks a random Drafts;
`delete_thread` moves to a random Trash.

**Path to failure for `mutation_stream`.** A `bulk_set_flags` batch
spanning INBOX and Archive hits an auth-lost error in the second folder.
`stream_terminating` fires and the stream returns immediately - so
*which* folder got mutated before the abort is a coin flip. The
consumer's `Terminated` carries no information about which half landed.
`get.rs:53-58` sorts its groups for exactly this reason and says so in a
comment; `mutate.rs` does not.

**Proposed fix.**

- `role_folder`: make the choice total and deterministic - prefer an
  entry whose *attribute* set names the role over one matched by name,
  and break remaining ties by sorted mailbox path. A tiny helper
  `fn role_rank(entry) -> Option<(u8, &str)>` with `0` for
  attribute-derived and `1` for name-derived, then `min_by_key`.
- `quota_probe_folder`: sort by name before `.find`.
- `mutation_stream`: sort the groups by folder name, copying the
  `get.rs` pattern and its comment.

### B11 - `get_stream` drops targets into no lane at all when their UIDVALIDITY is stale

`account/get.rs:154-161`.

```rust
let valid: Vec<u32> = ids.into_iter()
    .filter(|id| id.uidvalidity == uidvalidity)
    .map(|id| id.uid).collect();
let Some(uid_set) = uid_set_from_u32(&valid) else { return Ok(()); };
```

Ids whose UIDVALIDITY no longer matches the freshly-SELECTed mailbox are
silently dropped. No `Failed`, no `Uncertain`, no `Warning` - they simply
never appear in the output stream. If *every* id in the folder is stale,
the function returns `Ok(())` and the folder contributes nothing at all.

The same hole exists for ids that pass the filter but that the server
does not return (expunged between the caller's read and the FETCH):
`run_folder_get` only emits `Succeeded` for the fetches it got back.

`mutate.rs` gets this right - `split_by_uidvalidity` keeps the stale half
and `failed_all` turns it into per-item `Failed(uidvalidity_changed_error)`
(there is even a test, `stale_uidvalidity_targets_are_kept_for_failed_results`).
`get.rs` has the parallel situation and no parallel handling.

**Path to failure.** Engine hydrates 100 ids after a mailbox was
recreated server-side (fresh UIDVALIDITY). `get_stream` yields
`Done(None)` with zero items. The engine cannot distinguish "the account
returned nothing for these" from "these do not exist"; the three-lane
`BatchOutcome` contract in `reference/error-model.md` exists precisely so
every submitted item lands in exactly one lane.

**Proposed fix.** Mirror `mutate.rs`: partition on UIDVALIDITY, emit
`ItemOutcome::Failed` carrying a `Request(Malformed)` /
"UIDVALIDITY changed before hydration" error for the stale half, and
proceed with the valid half. Optionally also reconcile the returned
fetches against the requested UIDs and emit `Failed(NotFound)` for the
difference - that one is a judgement call, since a hydration miss for an
expunged message is arguably normal, but silently dropping it is not.

### B12 - `blob_attr` can emit `<start.0>`, which is not a legal IMAP partial

`account/blob.rs:284-297`.

```rust
let partial = range.map(|range| {
    let length = range.length
        .or_else(|| size.map(|size| size.saturating_sub(range.start)))
        .unwrap_or(u32::MAX as u64);
    (range.start, length)
});
```

RFC 3501 Section 6.4.5 defines the partial as `"<" number "." nz-number ">"` -
the count is non-zero. Two inputs produce a zero count:

- `ByteRange { start, length: Some(0) }` - an explicit empty read.
- `ByteRange { start, length: None }` with `handle.size == Some(total)`
  and `start == total` - the `saturating_sub` yields 0. Note
  `run_blob`'s guard is `range.start > total`, so `start == total` is
  explicitly allowed through.

The command is then encoded and sent, and the server answers `BAD`.

**Path to failure.** A consumer requesting a zero-length range (a
progress-bar probe, a resumed download whose remaining length is 0) gets
a protocol error instead of an empty stream. Minor severity - the read
fails cleanly rather than corrupting anything - but it is a wire-level
protocol violation we emit ourselves.

**Proposed fix.** Short-circuit in `run_blob`: when the computed length
is 0, send a single empty `Bytes` batch (or none) and `Done`, without a
FETCH. Alternatively have `blob_attr` return `Option<FetchAttr>` and let
the caller treat `None` as "nothing to read".
`account/blob.rs::blob_attr_encodes_a_zero_length_range_verbatim` pins
the current behavior, labelled.

### B13 - an invalid bulk-move destination surfaces as `Uncertain`, claiming a mutation might have landed when nothing was sent

`account/mutate.rs:204-212`, escalating at `:125-145`.

```rust
MutationKind::Move(destination) => {
    let folder = if let MembershipScope::Folder(id) = destination {
        MailboxName::new(id.0.clone()).map_err(crate::Error::from)?   // <- folder-level Err
    } else { return Ok(failed_all(valid, unsupported(BulkMove))); }
```

The `MembershipScope` variant check correctly produces per-item `Failed`.
The mailbox-name validation right next to it uses `?`, which escapes to
`run_folder_mutation`'s caller, where `mutation_stream` converts any
folder-level error into `ItemOutcome::Uncertain` for every id in the
folder.

`Uncertain` means "a request may have reached the server and we cannot
tell" - it queues the item for read-back reconciliation. Here nothing was
transmitted at all: `MailboxName::new` rejected a destination containing
NUL or CRLF before any command was built.

**Path to failure.** `bulk_move(ids, MembershipScope::Folder(FolderId("IN\r\nBOX")), key)`.
Every id comes back `Uncertain`, so the engine schedules a read-back
reconcile pass for a mutation that provably never happened. Wasted round
trips and a lingering "unknown state" in the engine's mutation ledger.

The same over-conservatism applies to the earlier `?`s in
`run_folder_mutation` (checkout failure, SELECT failure, missing
UIDVALIDITY) - none of those can have applied the mutation either, so
`Failed` would be the honest lane. Those are more defensible (a SELECT
failure could in principle follow a partially-processed pipeline) but the
name-validation one is not.

**Proposed fix.** Validate the destination once, before the per-folder
loop in `mutation_stream` (it is the same destination for every folder,
so validating it per folder is wasteful too), and emit `Failed` for the
whole batch on rejection.

---

## Gaps, smells, and things worth knowing

### G1 - the blob surface is advertised but no production code ever mints an IMAP `BlobId`

`capabilities.rs:38` sets `blob_range: BlobRangeSupport::Yes`, and
`blob.rs` implements `open_blob` / `open_blob_range` against
`decode_blob_id`. But `encode_blob_id` (`envelope.rs:314`) is
`#[cfg(test)]`, `fetch_to_inventory` sets `blob_id: None`
(`inventory.rs:254`), and `fetch_to_hydrated` sets `blobs: Vec::new()`
(`get.rs:285`). Nothing in the crate ever produces a `BlobHandle` a
consumer could pass back.

So the entire blob path is reachable only if a consumer synthesizes an
`imapblob1:` id by hand - which means the wire format is de-facto public
API without being documented as such, and `blob_range: Yes` is a
capability claim nothing can exercise. Either wire `blob_id` into the
inventory/hydration projections (the BODYSTRUCTURE is already parsed, so
per-part blob ids are derivable), or drop the claim to
`BlobRangeSupport::No` until it is. Right now `reference/imap.md` does
not mention this gap at all.

### G2 - `close()` does not close the composed DAV sub-accounts

`account/close.rs`. Composition is otherwise thorough - `route_scope`,
the four sync entry points, the discovery fan-in, and the capability
merge all delegate - but `close` cancels the IMAP shutdown token, stops
push, and closes the pool without touching `self.contacts` /
`self.calendars`.

Zero impact today: both `bifrost-caldav` and `bifrost-carddav` implement
`close` as `Box::pin(async { Ok(()) })`. It is a latent asymmetry: the
day either DAV crate acquires a real close (draining an HTTP pool,
cancelling a renewal task), IMAP will silently not call it. One-line fix:
await both subs' `close()` before `pool.close()`, folding any error into
a log rather than failing the IMAP close.

### G3 - the CONDSTORE baseline-seeding path issues `UID SEARCH ALL` twice

`account/changes.rs:506-514` then `:549`.

```rust
if !known_uids_complete {
    send_warning(..).await?;
    known_uids = CompactUidSet::from_uids(search_all(&account, conn.connection()).await?);
}
...
let live = search_all(&account, conn.connection()).await?;
let live_set = CompactUidSet::from_uids(live);
let diff = known_uids.diff(&live_set);
```

Two full `UID SEARCH ALL` round trips against the same selected mailbox,
back to back, and the diff between them is necessarily empty (modulo a
race, which would produce a *spurious* change). On a 200k-message
mailbox that is two full UID list transfers where one would do.

The semantics are intentional - a seeded baseline cannot yield adds or
removals on its first pass - but they are achievable with one call:
assign the single `search_all` result to both `known_uids` and
`live_set`, or skip the diff entirely when seeding. Worth noting this
path is not rare: it fires on every QRESYNC->CONDSTORE downgrade and on
every cursor restored without a complete UID baseline.

### G4 - `dial_idle()` opens a fresh authenticated connection per call, never pools it, never logs out, and bypasses the pool's concurrency cap

`account/pool.rs:98-114`.

`dial_idle` runs a full TCP connect + TLS handshake + SASL exchange and
hands back a bare `ImapConnection`. It does not take a semaphore permit,
so it is unbounded with respect to `pool_cap`. The returned connection is
never returned to `idle`, and callers just drop it - no `LOGOUT`, so the
socket is torn down abruptly and the server logs an unclean disconnect.

Call sites: `container_create`, `container_rename`, `container_move`,
`container_delete`, `quota_get`, `draft_create`,
`append_to_sent_or_fallback`, `refresh_folders`, and the push IDLE loop.
Note that four of those (`container_*`) each call `refresh_folders`
afterwards, which dials *again* - so a single "create folder" action
costs two complete TLS handshakes and two SASL exchanges. Renaming ten
folders is twenty.

The IDLE loop is the one legitimate user (it wants a dedicated
connection). Everything else wants a pooled connection that happens not
to need a SELECT. The natural fix is a `checkout_any()` that takes a
permit and reuses an idle member without re-selecting, leaving
`dial_idle` to push only.

### G5 - `reference/imap.md` documents a connection state machine that does not exist

Lines 15-22 of `reference/imap.md`:

> ## Connection state machine
> State is `Ok` / `Broken` / `Closed`. [...] Every async write, flush, and
> read sets state to `Broken` before the await; only success restores `Ok`.
> `abort()` transitions to `Closed` without sending LOGOUT. A dropped
> future leaves the connection `Broken`; the pool discards it.

None of that is in the code. `grep -rl Broken crates/imap/src/` matches
exactly one file, `types/envelope.rs`, for an unrelated purpose. There is
no `abort()`. The driver-task model replaced this and the doc section was
not updated. This matters beyond tidiness because `pool.rs` was written
against that contract - B4 is the consequence. The section should be
rewritten to describe what the driver actually guarantees (cancel-safety
via the driver owning the socket; command channel closure as the liveness
signal), or deleted.

### G6 - `SearchCriteria::header` does not validate the header field name

`types/search.rs:357-364`.

```rust
pub fn header(mut self, name: &str, value: &str) -> Result<Self, crate::Error> {
    self.sep();
    self.buf.push_str("HEADER ");
    self.buf.push_str(name);      // unvalidated, unquoted
    self.buf.push(' ');
    quote_imap_string(&mut self.buf, value)?;
```

The *value* is properly quoted. The *name* is pushed raw. RFC 3501's
`header-fld-name` is an `astring`, so a name containing a space, a paren,
or a quote produces a malformed SEARCH program; a name containing CRLF is
caught downstream by `validate_search_criteria_crlf` in the encoder, but
only by luck of that later guard.

Not reachable from `account/` (`criteria_from_filter` never uses
`header`), but `SearchCriteria` is the crate's documented typed-builder
surface and every other operand-taking method validates. Fix: run `name`
through `validate_atom_bytes`, or quote it like the value.

### G7 - `mutation_results` carries a dead parameter

`account/mutate.rs:586-592` takes `_requested_uids: &[u32]` and never
reads it; all five call sites compute and pass a `uids` vector for it.
It looks like a leftover from an earlier shape where the "not in
`modified` therefore succeeded" set was derived from the request rather
than from `ids`. Removing it would delete a `.map(|id| id.uid).collect()`
at two call sites too.

### G8 - push subscription lifecycle has two small races

`account/push.rs`.

- `push_unsubscribe` removes the handle and, if the scope map is now
  empty, calls `stop()`. An *unknown* handle passed when the map is
  already empty also stops the IDLE task. Harmless-ish (a later subscribe
  restarts it) but it means an idempotent-looking unsubscribe has a side
  effect.
- `stop()` cancels the token and takes it out of `task_cancel`, but the
  `idle_loop` may be parked inside `conn.idle(..)` and take up to
  `idle_timeout` to notice. A `push_subscribe` in that window sees
  `task_cancel == None`, spawns a second `idle_loop`, and the account
  briefly holds two IDLE connections. Both eventually converge, so this
  is a resource blip rather than a correctness bug.
- `choose_idle_folder` picks the first `CursorScope::Folder` out of a
  `HashMap<String, HashSet<CursorScope>>` flat-map - arbitrary order, so
  which folder the account IDLEs on changes between runs even with an
  identical scope set. For a consumer that expects INBOX to be the
  low-latency folder, that is a surprise. Sorting, or preferring the
  INBOX-role folder when it is among the subscribed scopes, would make it
  predictable.

### G9 - the LIST-STATUS double-strip is not reachable from `account/`

Answering the orchestrator's second question directly. The defect in
`connection/helpers.rs:295-320` -

```rust
Some(if let Some(suffix) = trimmed[6..].strip_prefix(" (") {   // strips "("
    if suffix.ends_with(')') && suffix.len() >= 2 {
        Ok(&suffix[1..suffix.len() - 1])                        // strips it again
```

so `STATUS (MESSAGES UNSEEN)` yields `ESSAGES UNSEEN` - is real, and it
does mean `validate_requested_status_items` inspects a corrupted first
item and the LIST-STATUS capability gate no-ops. But the account layer
never exercises it:

- `factory::list_folders` (`factory.rs:616-632`) is the only
  `list_extended` caller in `account/`, and it passes return options
  `&["SPECIAL-USE"]` - no `STATUS (...)` option, so
  `list_status_return_option_items` returns `None` at the prefix check.
- `discover_shared_folders` uses plain `conn.list("", pattern, ..)`.
- `pim::container_delete` uses the standalone `conn.status(folder, "MESSAGES", ..)`
  command, which is a different code path entirely.

So no account-side STATUS or LIST-STATUS path is affected, and no fix is
needed in my scope. It is worth noting for the future though: the reason
the account layer never uses LIST-STATUS is that `list_folders` does one
LIST and then STATUSes lazily, which is the more expensive shape on a
large mailbox tree. If that is ever optimized into a LIST-STATUS, this
bug becomes live.

### G10 - `encode_blob_id` is test-only while `decode_blob_id` is production

`envelope.rs:314-330` is `#[cfg(test)]`; `decode_blob_id` at `:332` is
not. A decoder without a matching encoder in the same visibility tier is
a smell that outlives the reason for it - see G1. If blob ids are wired
up, the `#[cfg(test)]` comes off; if they are not, `decode_blob_id` and
`open_blob*` are dead weight.

### G11 - `discover_memberships` emits one `Mailbox(owner)` per shared folder, not per owner

`scopes.rs:34-39` flat-maps `memberships_for_entry` over every entry, and
each shared entry contributes its owner. An account with 40 folders
shared by `alice` emits `MembershipScope::Mailbox("alice")` 40 times in
the discovery batch. The engine presumably dedupes (membership scopes are
a set downstream), but emitting 40 copies of the same scope through the
batch machinery is wasteful and makes the batch item count meaningless as
a signal. Dedupe before emitting.

### G12 - a failed `UID EXPUNGE` in `bulk_destroy` leaves `\Deleted` set with no signal

`mutate.rs:280-307`. The STORE `+FLAGS.SILENT (\Deleted)` is issued
first; if the subsequent `UID EXPUNGE` fails, the affected ids are
reported `Failed`. That is the right lane, but the `\Deleted` flag stays
set on the server. The consumer sees "destroy failed", retries, and gets
the same result on a server that cannot expunge by UID at all (B7). The
messages accumulate as flagged-deleted-but-present, invisible to the
consumer's model.

This is arguably unavoidable given IMAP's two-step destroy, but the
failure message should say so - `store_failed_error`'s detail
("STORE command failed with no per-item response code") is actively
misleading here, since the STORE succeeded and the EXPUNGE is what
failed. A dedicated `expunge_failed_error` with an honest detail string
would help whoever reads the support diagnostics.

### G13 - `apply_mailbox_event` only refreshes on rename or delete-then-create

`folder_registry.rs:436-443`:

```rust
if !map.contains_key(&name) || info.old_name.is_some() { .. install fresh entry .. }
```

A LIST/IDLE event for a folder already in the map, with no `OLDNAME` and
no `\NonExistent`, is a no-op - the attributes are not updated. So a
folder that gains or loses a SPECIAL-USE attribute mid-session (an admin
sets `\Archive` on it, or `\NoAccess` appears per RFC 5465 Section 5.9)
keeps its stale attribute set until the next account reopen. `selectable`
is likewise never re-evaluated, so a folder that becomes `\Noselect`
mid-session stays a live cursor scope.

The documented behavior in `reference/imap.md` ("delete, rename, and
delete-then-recreate") matches the code, so this is a scope decision
rather than a defect - but the `\NoAccess` case in particular is a real
revocation signal being dropped, and it is adjacent to the A5c shared
folder work. Worth a decision rather than silence.
`folder_registry.rs::duplicate_create_event_leaves_a_known_folder_untouched`
pins the current behavior.

### G14 - `CompactUidSet::len` is a UID count, not a range count, and has no `is_empty`

`folder_registry.rs:61-72`. `len()` sums the expanded cardinality of the
ranges, which is what `warn_if_uid_count_mismatch` wants, but the name
invites `set.ranges().len()` confusion at a glance and there is no
`is_empty` companion. `count()` or `uid_count()` would read better. Minor.

---

## Tests landed

All pin current behavior. Three are explicitly labelled as documenting
behavior I believe is wrong (B7, B8, B9 above) with a `NOTE:` comment
naming this file; they are not endorsements.

`account/envelope.rs` (+7)
: foreign-protocol cursor tag rejected; bad magic / unknown tag / missing
  tag; truncated u64 field and truncated uid-set; nine malformed
  object-id shapes; multibyte folder name round-trip through the
  byte-length prefix; thread-id empty / non-numeric / missing uid set;
  blob-id section containing colons, and empty section meaning `None`.

`account/folder_registry.rs` (+6)
: `CompactUidSet` drops zero UIDs, sorts, dedupes, and coalesces;
  `from_ranges` normalizes out-of-order and overlapping ranges;
  `diff` in both directions and against itself; `expand_range`;
  `clear_modseqs` is a no-op across a UIDVALIDITY epoch; `mark_seen`;
  a duplicate create event leaves a known folder's MODSEQ cache alone.

`account/inventory.rs` (+6)
: `flags_hash` is order-, case-, and duplicate-insensitive; distinct flag
  sets hash differently, including the `$a`+`$b` vs `$a$b` collision;
  `flags_set` lowercases; the shared-owner `Mailbox` membership appears
  only for shared folders; `ServerVersion::Unavailable` without CONDSTORE;
  `thread_id` preferred over `gmail_thread_id`; angle brackets stripped
  from Message-ID / In-Reply-To.

`account/get.rs` (+7)
: every projection requests UID; every body-bearing projection PEEKs;
  metadata projections never fetch a body; a UID-less FETCH is dropped;
  `FlagsOnly` lowercases; `Metadata` reuses the inventory projection and
  keeps the owner tag; a missing section yields empty `RawMime`;
  **[documents B8]** `Preview` keeps only the first returned section.

`account/blob.rs` (+5, file previously had no tests)
: `blob_attr` peeks and emits no partial without a range; explicit
  length honored; open-ended length derived from the known total,
  saturating at the end; the u32 fallback when the total is unknown;
  **[documents B12]** a zero-length range is encoded verbatim.

`account/capabilities.rs` (+4)
: `draft_create` needs both a Drafts folder (by name or SPECIAL-USE) and
  UIDPLUS; `search` / `thread_hydrate` need `THREAD=REFERENCES`
  case-insensitively while `search_messages` does not; push and quota
  track IDLE and QUOTA; **[documents B7]** the EXPUNGE-backed methods are
  advertised without UIDPLUS.

`account/mutate.rs` (+5)
: only the `MODIFIED` response code yields conflicting UIDs (including
  the empty-list case); `Modified` splits per-UID success from conflict;
  `Applied` succeeds everything; `Failed` fails everything and carries
  the caller's operation; `split_ids_by_uid`; `failed_all` emits one
  outcome per target.

`account/changes.rs` (+5)
: an equal MODSEQ is not a reset and an absent one is; a matching UID
  count emits no warning; the downgrade warning is
  `StrategyDowngraded`-kinded; a FETCH after an already-flushed removal
  reports `Added` while a still-buffered removal is retracted into an
  `Updated`; a UID-less FETCH is ignored.

`account/pim.rs` (+18)
: SPECIAL-USE beats the name and the name fallback covers the known
  aliases; **[documents B6]** the fallback misses a `.`-delimited leaf;
  `parent_id` uses the server's delimiter; convenience keywords map onto
  system flags and everything else stays a keyword; page cursor walks to
  exhaustion and tolerates an over-large offset; a page cursor this crate
  did not mint is rejected; `imap_date` emits `date-text` with an
  unpadded day;
  `search_plan` defaults, folder restriction, operand quoting,
  provider-query substitution vs conjunction, whitespace-only query;
  **[documents B1]** an unbalanced provider query passes straight
  through; AND/OR criteria shapes including three-way OR nesting;
  two-folder AND rejected, same-folder AND accepted; NOT wrapping;
  `header_name_is_bcc` trimming and case; folded `Bcc:` continuation
  lines stripped; `unfold_headers` fold-joining and repeat ordering;
  angle-bracket commas in address lists; hydration attrs per projection;
  `$important` -> `Importance::High`; **[documents B9]** `Full` puts the
  raw message in `body_text`.

`account/mod.rs` (+4)
: `CursorScope::Account` is `Unsupported`; folder and membership scopes
  agree on the native path; a CRLF-bearing mailbox name is
  `Request(Malformed)`; `uid_set_from_u32` drops zeros, refuses to build
  an empty operand, and coalesces.

`types/uid_range.rs` (+4, file previously had no tests)
: single vs one-element range are distinct values; the checked
  constructors reject every zero-UID combination while the unchecked ones
  only `debug_assert`; `try_range` does not police direction; `Default`
  is the degenerate zero.

No `Cargo.toml` was touched and no new dev-dependency is needed. Nothing
outside my assigned files was edited.

---

## Not reached

- **Byte-level duplex transcripts.** The new capability
  (`connection/test_support.rs::driver_pair`) is the right tool for the
  `account/` streaming paths - `run_inventory`, `run_changes`,
  `run_folder_get`, `mutation_stream` - which are the least-covered code
  in my scope and the only place the QRESYNC/CONDSTORE/Basic strategy
  dispatch can actually be exercised. I did not build on it because
  every one of those paths goes through `ImapAccount`, which needs an
  `ImapAccountParts` with a live `Pool`, and `Pool::new` takes a primed
  `ImapConnection` plus an `ImapAccountConfig` carrying real
  `Credentials`. Constructing that from a duplex needs a
  `pub(crate)` seam that does not exist yet - either
  `Pool::from_connection(conn)` for an already-authenticated handle, or
  an `ImapAccountParts` test constructor. Both are one-file additions
  inside my scope, but they are *source* changes rather than tests, and
  the split I was asked to keep says a source change is a fix, not a
  test. Flagging it as the highest-value next step: with that seam,
  `run_changes` under a canned QRESYNC transcript would cover the
  strategy dispatch, the VANISHED/FETCH dedup against real wire ordering,
  the downgrade paths, and the checkpoint emission - none of which any
  test touches today.
- **`account/error.rs` and `account/error_tests.rs`.** 42 KB of mapping
  code with 31 existing tests. I read the `InvalidInput` /
  `MissingCapability` arms to confirm classifications used elsewhere in
  this report, but did not audit the full `RecoveryClass` derivation
  against `reference/error-model.md`. It is the best-covered file in my
  scope, so I deprioritized it; a dedicated pass against the contract doc
  would still be worth doing.
- **`crates/imap/src/error.rs` / `error_tests.rs`.** 44 existing tests.
  Not audited beyond the variants I needed.
- **`types/` breadth.** I verified the coverage claim rather than
  assuming it: `capability.rs` has no sibling `_tests.rs`, but its
  parsing, cross-representation equality, and Hash/Eq consistency are all
  covered from `response_tests.rs` (~108 tests), including the
  `Other("IDLE") == Idle` case and `SORT=DISPLAY` hash consistency. I
  read the `PartialEq`/`Hash` impls looking for a transitivity or
  consistency break and did not find one. `uid_range.rs` was the one
  genuine hole and is now covered. `secret.rs`, `notify.rs`, `sync.rs`,
  `events.rs`, `address.rs` are thin wrappers whose behavior is exercised
  through the parser tests; I did not add duplicate coverage.
- **`sieve.rs` and `submission.rs`.** Read for the untrusted-text
  question (both clean - see B1) but not audited for their own logic.
- **`account/scopes.rs` fan-in under sub-account failure modes beyond the
  single existing warning test**, and `factory.rs`'s `discover_shared_folders`
  wire path (its pure helpers are already well covered).
