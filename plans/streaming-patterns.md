# Streaming patterns across crates

Bifrost is being designed for ratatoskr users with multiple hundred GB
accounts and millions of messages. Ratatoskr will be rewritten to
consume bifrost once the API is ready; nothing in the current ratatoskr
code constrains the bifrost-side design. The question is what bifrost
should expose so a memory-bounded, resumable, cancellable consumer can
be written cleanly against it.

## The five bifrost-side properties

1. **Every list / query / sync is `impl Stream<Item = Result<T, Error>>`.**
   Pagination, multi-fetch, changes-since - all of it. JMAP `Email/query`,
   Gmail `messages.list`, Graph `messages` collection, IMAP FETCH - same
   consumer-facing shape, per-protocol implementation underneath. The
   consumer never sees `pageToken`, `$skiptoken`, `position/limit`, or
   manual paging loops.

2. **Typed opaque cursors per change-tracked type.** `Cursor<Email>`,
   `Cursor<Mailbox>`, etc. The consumer persists the cursor and hands it
   back on resume. JMAP's `State` is already this shape; Gmail's
   `historyId` and Graph's delta tokens need wrapping; IMAP's
   `UIDNEXT`/`UIDVALIDITY` pair needs a typed cursor. A hundred-GB
   initial sync that fails at 90% resumes from the cursor, not from zero.

3. **Drop-is-cancel.** Dropping a stream cleanly tears down the
   underlying primitive - IMAP driver drains the wire, HTTP request
   aborts, WebSocket sends disable + close. The consumer never needs a
   "point-check between RPCs" workaround. Mid-RPC cancellation is a
   non-event from the consumer's perspective.

4. **Bounded internal buffers, documented.** Each stream documents its
   capacity: "buffers up to N items / N bytes". IMAP's `DriverEventSink`
   already does this with explicit caps and overflow buffering for
   critical events. HTTP-paginated streams adopt the same pattern -
   buffer one page, await pull, fetch the next.

5. **Body / attachment bytes as `impl Stream<Item = Result<Bytes, Error>>`
   for anything large.** Downloading a 200 MB attachment must not
   materialize 200 MB in memory. JMAP blob download and Gmail / Graph
   attachment GET are HTTP and stream naturally; IMAP uses partial fetch
   with byte ranges. The consumer writes to disk as bytes arrive.

## Not a `bifrost-stream` module

What sounds shareable across crates turns out not to be:

- **Cursor shapes are protocol-specific.** JMAP `State` is a server-issued
  string per type. Gmail `historyId` is a monotonic integer per account.
  Graph delta tokens are URLs embedded in collection responses. IMAP
  `UIDNEXT` + `UIDVALIDITY` is a per-folder pair. Wrapping each in a
  typed marker is per-crate work; the cursors themselves cannot share
  an implementation.
- **Cancellation mechanisms are protocol-specific.** IMAP driver-task
  drain, HTTP request abort, WebSocket disable + close - same contract
  to the consumer (drop the stream), different work under the hood.
- **Push event payloads are protocol-specific.** JMAP `StateChange`, IMAP
  IDLE / NOTIFY untagged responses, Graph subscription notifications,
  Gmail history-changed events - different content, different
  granularity.

What IS shareable is the convention, not the code:

- The `impl Stream<Item = Result<T, Error>>` boundary typing.
- The "drop is cancel; the impl cleans up" contract.
- The "cursors are typed and opaque" pattern.
- Push exposed as `impl Stream<Item = Notification>` with a per-protocol
  payload.

These belong in a workspace `reference/` doc when implementation lands,
not in a shared crate.

## Per-crate scope (sketch)

These are scaffolds, not designs. Concrete plans get written per crate
when implementation starts. Each crate's section lists the surfaces to
add, the cursor shape, the push shape (if any), and the open questions
worth pinning down before designing.

### bifrost-jmap

Already has: `*/query`, `*/get`, `*/changes` builders returning
materialized lists; WebSocket support in `client_ws.rs`; SSE in
`event_source/`; blob download returning materialized `Bytes`.

Surfaces to add:

- `email_query_stream(filter)` - auto-pages internally via
  `position` + `limit`, yields `Email` per item. Same shape for
  `mailbox_query_stream`, `calendar_event_query_stream`,
  `contact_card_query_stream`, etc.
- `email_get_stream(ids)` - takes an arbitrary-sized ID set, batches
  into `Email/get` calls of safe size (capped by
  `urn:ietf:params:jmap:mail` capability `maxObjectsInGet`), yields
  `Email` per item. Same for other typed objects.
- `email_changes_stream(cursor)` - follows `hasMoreChanges` until
  exhausted, yields `EmailChange { id, kind: Created|Updated|Destroyed }`
  events. Final item carries the new cursor. Same for other change-
  tracked types.
- `query_changes_stream(cursor, filter)` - combined `*/queryChanges` for
  incremental sync of filtered views (e.g. inbox-only).
- `download_blob_stream(blob_ref)` - returns `impl Stream<Bytes>` over
  reqwest's chunked body (`bytes_stream`).
- `push_stream(types)` - unifies WebSocket and SSE under one
  `impl Stream<Item = Notification>` with internal auth-resolver +
  reconnect + backoff + persisted `push_state`. Server capability
  determines transport; consumer just sees the stream. This is the
  surface that makes ratatoskr's hand-rolled push loop redundant.

Cursor shapes:

- `Cursor<Email>` wraps JMAP `State` for the Email type, per account.
  Same per-type for every change-tracked object.
- `PushCursor` wraps `pushState` from `WebSocketPushEnable` / SSE - a
  separate cursor from the per-type `State`, persisted alongside the
  push subscription.

Open questions:

- Resume-mid-query: JMAP `position` is a count, not opaque, so a paging
  consumer can resume from a specific offset. Does the stream API
  expose that, or is mid-stream resume only via change cursors?
- Push reconnect policy: backed-in defaults vs. consumer-configurable
  (`MAX_CONSECUTIVE_FAILURES`, backoff)? Lean configurable; defaults
  match RFC 8887 guidance.
- Raw-response escape hatch for batching consumers (multiple methods in
  one HTTP request). Likely keep the current builder API alongside the
  streams for this case.

### bifrost-imap

Already has: streaming FETCH (`StreamingFetchConsumer`); IDLE machinery;
driver event sink for NOTIFY / EXISTS / EXPUNGE.

Surfaces to add:

- `folder_new_messages_stream(folder, cursor)` - returns
  `impl Stream<Item = Uid>` of UIDs newer than the cursor. Driven by
  `SEARCH UID <next>:*` with QRESYNC fallback to `SEARCH NEW` for
  servers without QRESYNC.
- `folder_changes_stream(folder, cursor)` - QRESYNC `SELECT (QRESYNC ...)`
  yields a stream of `MessageChange { uid, kind: Created|FlagsChanged
  |Expunged }` events. Without QRESYNC, falls back to full UID list diff
  (more expensive; document the cost).
- `search_stream(folder, criteria)` - paginates very large SEARCH
  results in UID ranges, yields `Uid` per item.
- `idle_stream(folder)` - exposes IDLE / NOTIFY events as
  `impl Stream<Item = Notification>` over the driver event sink. Drop
  the stream → driver leaves IDLE cleanly.
- `fetch_body_stream(uid, section, byte_range)` - partial fetch
  (`BODY[<section>]<offset.length>`) yielded as `impl Stream<Bytes>`,
  chunked to fit RFC 3501's literal size handling.

Cursor shapes:

- `Cursor<Folder>` = `{ uidvalidity: u32, uidnext: u32, highest_modseq:
  Option<u64> }`. `highest_modseq` only meaningful with CONDSTORE.
- UIDVALIDITY change invalidates the cursor; consumer must resync the
  folder. Surfaced as an error variant on the stream, not a panic.

Open questions:

- Per-folder cursors only - no account-level "everything that changed"
  primitive in IMAP. Consumer multiplexes per-folder streams. Document
  the multiplexing pattern in `reference/imap.md` when the surface
  lands.
- IDLE server-side timeout (typically 29 minutes) - does the stream
  auto-reissue, or surface the timeout and let the consumer decide?
  Auto-reissue is the obvious default.

CONDSTORE support is fully shipped in ratatoskr (capability negotiation
with iCloud workaround, CONDSTORE fast-path skip when `HIGHESTMODSEQ`
matches, modseq reset detection, `CHANGEDSINCE` FETCH, modseq
persistence, non-CONDSTORE flag-sync fallback, UID-based deletion
detection with throttling). QRESYNC capability negotiation is also
shipped, but the `VANISHED` consumption path is not - `SELECT (QRESYNC
(...))` and `UID FETCH ... (CHANGEDSINCE ... VANISHED)` are
unimplemented, and `Response::Vanished` is currently unconsumed. See
`ratatoskr/docs/roadmap/imap-condstore-qresync.md` for the full state.

The port is conceptual, not literal. Ratatoskr is built on `async-imap`
and uses raw commands plus typed-response matching; bifrost-imap has its
own driver-based codec and can model CONDSTORE/QRESYNC as first-class
commands and responses. What transfers cleanly:

- Capability negotiation flow with iCloud-style "advertised but did not
  ENABLE" detection.
- The three-state cursor lifecycle (below).
- Server quirk catalog (Gmail CONDSTORE-only, iCloud broken QRESYNC,
  Dovecot reference implementation, Exchange IMAP no support, Courier /
  hMailServer no support).
- Footguns documented under "Phase 3" (drain untagged `VANISHED` on
  every command in a QRESYNC session; cross-check UID count vs.
  `Mailbox.exists` after delta application; treat modseq reset at same
  UIDVALIDITY as a forced resync; tolerate iCloud's malformed FETCH
  responses by disabling QRESYNC for the session on parse failure).
- Throttle intervals already validated in production
  (`FLAG_SYNC_INTERVAL_SECS = 300`, `DELETION_CHECK_INTERVAL_SECS =
  600`).
- The "CONDSTORE + UID-deletion-detection is a viable end state" exit
  ramp - if QRESYNC VANISHED keeps regressing, the cursor degrades and
  the consumer is unaffected.

Cursor lifecycle has three states:

- QRESYNC-negotiated: `MODSEQ` diff + `VANISHED` for expunges. Cheap.
  Not yet shipped in ratatoskr; bifrost-imap implements fresh.
- CONDSTORE-only (advertised QRESYNC didn't enable, or only CONDSTORE
  advertised): `MODSEQ` diff for flag changes, UID-list diff required
  for expunges. Medium cost. Shipped path in ratatoskr; port the
  semantics.
- Neither: full UID-list diff for every change category. Expensive.
  Shipped path in ratatoskr; port the semantics.

The cursor knows which path it was created under; the streams pick the
matching diff strategy.

### bifrost-gmail

Today: HTTP CRUD; `Result<_, String>` everywhere; no streaming surfaces.

Surfaces to add:

- `messages_list_stream(query)` - auto-pages via `pageToken`, yields
  `MessageStub { id, thread_id }` per item.
- `threads_list_stream(query)` - same shape, yields `ThreadStub`.
- `messages_get_stream(ids, format)` - composes list + per-message GET
  internally, parallelized under a concurrency limit (Gmail rate-limits
  aggressively). Yields full `Message` per item.
- `history_list_stream(cursor)` - auto-pages, yields `HistoryRecord`
  events. Final item carries the new cursor.
- `attachment_get_stream(message_id, attachment_id)` - returns
  `impl Stream<Bytes>`. Gmail attachments are base64url-encoded in JSON
  by default; this surface fetches and decodes, yielding raw bytes.
- `message_raw_stream(message_id)` - same shape for the full RFC822
  blob (mailing-list digests can hit 25 MB).

Cursor shapes:

- `Cursor<History>` wraps `historyId: u64`. Monotonic per account.
- A stale `historyId` (older than ~7 days) returns 404 from Gmail;
  surface as a typed error so the consumer can fall back to full
  resync.

Push: Gmail uses Cloud Pub/Sub `watch` for push. Bifrost can manage the
watch lifecycle (CRUD via `users.watch` / `users.stop`); ingestion is a
Pub/Sub listener concern outside bifrost. Push, when delivered, is just
a wake-up - the consumer calls `history_list_stream(cursor)` to find
out what changed.

Open questions:

- Per-message-GET concurrency limit: hardcoded vs. configurable? Gmail's
  per-user rate limit is 250 quota units/sec; a `messages.get` is 5
  units. Conservative default of 8 in-flight; expose as config.
- Batch endpoint (`/batch`) - Gmail supports multipart batching of GET
  requests. Worth using internally? Lower latency but more complex.
  Probably defer; revisit if the per-message stream is too slow.

### bifrost-graph

Today: HTTP CRUD; webhook subscription CRUD (`webhooks.rs`); some EWS
support; no streaming surfaces.

Surfaces to add:

- `messages_stream(filter)` - auto-pages via OData `@odata.nextLink`,
  yields `Message` per item. Same shape for `events_stream`,
  `contacts_stream`, `folders_stream`.
- `messages_delta_stream(cursor)` - delta query auto-pages via
  `@odata.nextLink`; final response carries `@odata.deltaLink` as the
  new cursor. Yields `MessageChange` events.
- `attachment_get_stream(message_id, attachment_id)` - returns
  `impl Stream<Bytes>` over the chunked body.
- `ews_streaming_subscription_stream(folders, events)` - opens an EWS
  long-poll connection, parses notification XML chunks, yields
  `impl Stream<Item = Notification>`. Internal reconnect on the
  30-minute server-side lifetime cap. This is the one place EWS gives
  bifrost an actual long-lived connection worth wrapping.

Cursor shapes:

- `Cursor<Message>` wraps the `@odata.deltaLink` URL, opaque to
  consumer. Same per-type for events / contacts / folders.

Push: Graph subscriptions push to a public HTTPS endpoint; bifrost
manages CRUD (already there), but ingestion is an HTTP listener
concern outside the crate. EWS StreamingSubscription is the in-process
push alternative for Exchange-on-prem and tenants that prefer
long-polling over webhooks.

Open questions:

- Throttling: Graph returns `429 Too Many Requests` with `Retry-After`.
  The stream honors this internally - already a concern in the existing
  client. Cap retries at some bound and surface as an error rather than
  looping forever.
- Concurrency semaphore (existing client has limit 3) interacts with
  parallel paging. Single in-flight page is the safe default; consumer-
  configurable for higher tiers.
- EWS XML parsing - the streaming notification path needs an XML stream
  parser. Worth bringing in a crate (quick-xml) vs. extending the
  existing EWS code? Likely the former.

### bifrost-smtp

No streaming surfaces. Send is request / response. Connection pooling
stays internal. The crate's job is done at SMTP-reply granularity.

## Decision

No shared `bifrost-stream` crate or module. Per-crate streams, shared
conventions only. Per-crate implementation plans get written when each
crate's work starts; this document remains the central reference until
then.
