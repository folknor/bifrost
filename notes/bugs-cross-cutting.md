# Cross-cutting hunt findings

Findings that belong to no single scope, and facts about the report itself. Filed
per the hunt procedure: an unfiled finding is a finding you paid for and threw away.

Per-scope documents: `bugs-sync.md`, `bugs-types.md`, `bugs-jmap.md`,
`bugs-google.md`, `bugs-graph.md`, `bugs-imap-sasl.md`, `bugs-dav.md`,
`bugs-net.md`, `bugs-smtp.md`.

## Defects reported independently from three or more scopes

Stated as facts about the report, not as judgements about the code.

### `bytes_in: 0` across protocol crates - settled by the net hunter

Reported by the jmap hunter (eleven `Batch` constructions), the google hunter
(changes, mutation, inventory, get_stream, scopes), and visible in graph. The net
hunter was asked to settle it from the transport side and did: `AccountMeter` exposes
only process-lifetime cumulative totals and a 10-second sliding rate, so "bytes this
batch consumed" is not derivable without a cross-request race. The consumers are not
being lazy; the seam does not exist. Fix belongs in `bifrost-net` first (see
`bugs-net.md` N-10), and a naive before/after delta would be wrong anyway because of
N-3. The only lanes with honest byte accounting today are the blob paths, which
measure the decoded blob themselves.

### A completeness or coverage claim asserted wider than the walk that produced it

Three independent instances, three different crates:

- `types`: `unsupported_inventory_stream` and `InventoryCompletion::complete` mint a
  full-scope `Complete` from a refusal (`bugs-types.md` 1).
- `caldav`/`carddav`: `inventory_stream` claims `CoverageDomain::full(Type(...))`
  while walking exactly one collection (`bugs-dav.md` 2).
- `sync`: the ledger accepts these and `completion_permitted` lets a sentinel land
  over discarded debt (`bugs-sync.md` A1, A3).

The coverage machinery was designed to prevent precisely this, and the escape hatch
is a scope-only constructor with no `CoverageDomain` argument.

### A "shared guard" that call sites are free not to call

- `graph`: `PageWalk` bounds six incidental traversals and neither of the two delta
  sync loops (`bugs-graph.md` 1); `reference/graph.md` asserts it bounds every one.
- `sync`: "one writer per account owns every durable mutation" is enforced by nothing;
  three recovery paths write the store directly (`bugs-sync.md` A5).
- `net`: no end-to-end deadline exists; per-attempt timeouts multiply across retries
  and redirect hops (`bugs-net.md` N-1).

Same shape each time: the invariant lives in prose and in whichever call sites
remembered.

### Hand-cloned code that has since drifted

- `smtp`: `connection.rs` / `async_connection.rs` are a manual clone; five of eight
  findings are divergences between the halves (`bugs-smtp.md`).
- `imap`: three hand-written change strategies re-derive the same five decisions and
  disagree on three of them (`bugs-imap-sasl.md`, structural read).
- `graph`: three copies of one search paging loop, all carrying the same
  result-dropping bug (`bugs-graph.md` 2).
- `carddav`: `contact_addressbook_url` is the bug CalDAV already fixed and documented
  (`bugs-dav.md` 5).
- `sync`: `BackfillRunner` and `InventoryFusion` are two implementations of one walk,
  diverged on the safety-critical barrier rule (`bugs-sync.md` A2, F3).

### Tests that pass against the bug they were written for

Three, matching the standing lesson exactly:

- `sync`: `a_recovery_discharges_only_after_the_consumer_acknowledges` never acks
  anything (`bugs-sync.md` A4).
- `imap`: `pending_retry_conflict_is_failed_not_uncertain` pins a lane that is
  unreachable in production (`bugs-imap-sasl.md` 2).
- `types`: `telemetry_has_no_free_form_text` checks nothing about free-form text
  (`bugs-types.md` 11).

## Cross-scope claims tested this round

- **Refuted.** `Fingerprint::flags_hash` computed two ways inside `graph` producing
  divergent fingerprints for identical state - the two producers mint disjoint id
  namespaces, so no consumer can compare them. The graph hunter found a different and
  worse defect at that site (public-folder change detection cannot see read-state
  changes at all). See `bugs-graph.md`.
- **Confirmed false.** `reference/google.md:751` claims the crate has "no
  scripted-transport seam". `bifrost_net::test_support` is used by eight google
  modules; the net hunter enumerated what the seam offers. The line should be deleted.
  See `bugs-net.md`, cross-scope answers.

## Durable docs that contradict the code

`reference/` is the citable-as-truth tier, so these need correcting whether or not the
corresponding code changes:

- `reference/graph.md` - `PageWalk` bounds "every `@odata.nextLink` traversal".
- `reference/caldav.md` and `reference/carddav.md` - "a leg that fails wholly after
  other legs returned events keeps those events".
- `reference/smtp.md` (lines 18-32, 46-52) - the two halves are held in step; a missed
  phase decoration cannot degrade the classifier.
- `reference/google.md:751` - no scripted-transport seam.
- `reference/sync.md` - contradicts itself on `retry_queue_cap` (both "bounds this"
  and "is inert"); `IdempotencyVendor`'s doc claims a `CheckpointStore` campaign key
  that does not exist.
- `reference/types.md` - file map omits `coverage.rs` and `repair.rs`; the "94
  methods" lane table is behind the trait; `error-model.md` lists `EmptyChain` among
  "the five enforced invariants" and it is enforced nowhere.
- `reference/net.md` - calls `RequestBuilder::timeout` "the explicit total request
  deadline"; duplicates a paragraph at lines 363-371.
- Three doc comments in `types` still describe `RecoveryClass::CapabilityChanged`,
  which no longer exists.

## Security findings

One this round, in `bugs-smtp.md` 1: STARTTLS keeps the pre-upgrade `BufReader` buffer
across `upgrade_tls` on both halves, so plaintext appended to the `220` segment is
served as the first post-TLS server output - which is the EHLO reply that populates
`ServerInfo`, and therefore the AUTH mechanism list. CVE-2011-0411 lineage. The crate
already has the primitive to close it (`stream.buffer().is_empty()`, used in the LMTP
drain).

## Process note

The smtp hunter's first return was not a report - it launched a background
`brokkr check -p smtp` and stopped with "I'll report once it lands". Asked directly, it
returned a report substantially fuller than its own second-pass summary: 13 findings
rather than 8, plus a verified-correct section (dot-stuffing, header injection on both
the name and value sides, `smtp_data_size`, the `Unsent`/`Uncertain` split) worth
keeping so a later pass does not re-derive it. `bugs-smtp.md` holds the fuller version.
Two other hunters also ran builds or test suites unprompted. The template does not ask
for a baseline and the hunt phase does not need one; if this recurs, the template may
want a sentence saying so.
