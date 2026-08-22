# Bug hunt 2026-08-05: cross-scope observations

Orchestrator notes on the eight-scope hunt filed in `notes/bugs-*.md`. This document records
findings that belong to no single scope, and repetitions across scopes. It states what the reports
say; it does not rank or verify them.

## How to read this document (triage pass 2026-08-23)

**This document holds no findings of its own and must never be run as a work queue.** It is an
index over the eight per-scope ledgers: every item below is a restatement of a finding that lives,
in full, in another `bugs-*.md`, and the fix belongs there. Working an entry from here means
working a summary rather than the finding, with the owning document's context stripped off - which
is precisely how a proposal gets mistaken for a defect.

Categories used across the `bugs-*.md` set, for reference when following a link out of here:
**C1 live defect**, **C2 latent defect**, **C3 refactor opinion**, **C4 product decision** (the
owner's call, never the loop's), **STALE**. **PUBLISHED SURFACE** marks any remedy that would
remove, rename, or reshape a published item.

One warning about the last section, "Large hand-mirrored duplication proposed for collapse":
**every entry in it is C3 or C4, and two of the six have already been acted on and reverted.** The
smtp entry ("~5000 lines of blocking transport mirroring the async half, with no in-workspace
consumer, recommending deletion") is the exact text that produced `d20816c`, restored in
`e632ab9`; the sync entry's "~1000 lines of unwired machinery" is the reasoning behind `603d146`,
restored in `7e7184d`. That section reads as a list of tasks and is a list of opinions, and its
"no in-workspace consumer" premise is meaningless for library crates whose consumers are outside
this workspace by definition. See the standing-lessons section of `notes/carry-forward.md`. Nothing
in that section may be acted on without the repository owner.

Reports: `bugs-jmap.md`, `bugs-imap.md`, `bugs-sync.md`, `bugs-google.md`, `bugs-graph.md`,
`bugs-dav.md`, `bugs-smtp-sasl.md`, `bugs-net-types.md`. All eight scopes returned findings.

## Reported from two scopes with different diagnoses (not deduplicated)

**The DAV transport bypass.** The DAV hunter reports that `DavTransport` exists "solely because
bifrost-net's dispatcher is crate-private", with the consequence that all DAV traffic skips retry,
rate limiting, bandwidth metering, and observability, and that `set_priority` /
`set_bandwidth_cap` are silent no-ops. The net hunter reports the same consequence but rejects the
cause: `Dispatch`'s privacy is worth keeping, and the actual blockers are that `AccountNet` has no
`request(Method, &str)` for PROPFIND/REPORT/MKCALENDAR and that `AccountSpec::token_source` is
mandatory even for Basic auth. Both writeups are retained in full in their own documents.

**Uncapped response buffering.** `bugs-net-types.md` reports `send()` buffering response bodies with
no ceiling, and separately notes `ReqwestDavTransport::send` calling `response.text()` with the same
exposure. The DAV report does not raise it. (Fixed 2026-08-07 in all three places, against one
shared `DEFAULT_MAX_BUFFERED_RESPONSE`.)

## The same defect shape in three or more scopes

**A no-op or downgraded mutation reported as `Applied`.** google (`bulk_destroy` falls back to a
TRASH label patch and returns `MutationSuccess::Applied` for messages that still exist), graph (a
`FlagOp` with no Graph-recognized flag builds an empty PATCH, gets a 200, and files `Applied` -
fixed 2026-08-18 by testing the built body rather than the token namespace).
Both reports note the crate already has the right instinct nearby and scoped the guard too narrowly.

**`close()` / teardown that is unbounded, uncancellable, or does not actually tear down.** jmap
(`close()` awaited an unbounded WebSocket write, and the detached reader `JoinHandle` was dropped so
`close()` returning proved nothing - both fixed 2026-08-22: the write is bounded by
`connect_timeout` and the retained handle is joined under the same bound), imap (`idle()`'s DONE handshake has no deadline - fixed
2026-08-18 - and the IDLE connection is outside the pool's drain; `Pool::close` cannot log out an
outstanding checkout), graph
(`close()` aborted the EWS worker rather than letting it release, and never deleted webhook
subscriptions - both fixed 2026-08-18: `close()` retires webhook subscriptions first, then joins
the EWS worker under a bounded timeout so its `Shutdown` arm can Unsubscribe).

**Broadcast `Lagged` treated as nothing.** sync (the engine never detects lag on the changes
channel, and the in-memory cursor has already advanced past the dropped batches), graph
(`push_stream` did `Err(RecvError::Lagged(_)) => continue`, losing invalidations permanently -
fixed 2026-08-18: it now yields a `PushSource::Coalesced` invalidation). Both
reports independently propose synthesizing a full-reconcile signal instead; the sync half stands.

**Reference-doc invariants that the code does not enforce.** Reported in sync (four named claims,
three with no test), imap (the `get.rs` preview fix the doc describes landed only in `pim.rs`; the
IDLE drain "discards" claim - both corrected 2026-08-18), jmap (`close()` "awaits teardown" - made
true 2026-08-22 rather than corrected), google (`open_blob_range`'s
"range-supporting branch" does not exist), graph, dav (the CardDAV phantom collection that the
CalDAV doc explains was removed as a bug), net (a `Drop` the doc asserts and the code does not have;
a `Cancelled` variant nothing constructs; a stale "lands in S1-W2" planning note).

**Large hand-mirrored duplication proposed for collapse. [C3/C4 throughout - see the warning at
the top of this document. Do not act on any of this here.]** dav (~1500 lines across the two crates,
with four already-drifted copies identified, recommending a shared `bifrost-dav`), smtp (~5000 lines
of blocking transport mirroring the async half, with no in-workspace consumer, recommending
deletion), sync (a 230-line verbatim copy of the mutation campaign loop, plus ~1000 lines of unwired
machinery), google (four hand-rolled `stream::unfold` drivers whose divergence directly caused two
of that report's findings), jmap (three near-identical query/get/advance loops), imap (four copies
of the untagged-response dispatch loop), graph (two worker-lifecycle state machines, one hardened
and one absent).

## Process notes

- The jmap hunter did not audit `calendar_ops.rs` or `contacts.rs` in depth (~3k lines of
  JSCalendar/JSContact mapping); the RRULE/`UNTIL`, all-day exclusive-end, and RSVP claims in
  `reference/jmap.md` are unverified by this hunt and were flagged as wanting a dedicated pass.
- `notes/` did not exist in the tree before this hunt.
