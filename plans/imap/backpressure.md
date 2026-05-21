# bifrost-imap streaming FETCH backpressure rework

A planning document for converting `bifrost-imap`'s streaming FETCH
dispatch from `UnboundedSender` to a bounded, cooperatively-paced
read pump. The sync engine (`bifrost-sync`) requires bounded
buffering on every protocol stream (see `plans/sync-engine.md` ->
Backpressure). This is not a type swap; it is a driver-loop redesign
that has to compose with the IMAP connection pool, IDLE, the
QRESYNC `VANISHED` drain rule, and the existing cancellation-safety
contract.

The document is the brief a phase-2 implementer follows. The aim is
no architectural surprise during implementation.

## Current state characterization

The streaming FETCH path:

- Public handle API: `ImapConnection::uid_fetch_streaming`
  (`crates/imap/src/connection/uid_ops.rs:156-179`) and
  `ImapConnection::fetch_streaming`
  (`crates/imap/src/connection/seq_ops.rs:76-98`). Both call
  `fetch_streaming_impl`
  (`crates/imap/src/connection/uid_ops.rs:187-200`), which submits
  the command to the driver with a
  `dispatch::StreamingFetchConsumer`.
- Channel type at the boundary: the caller hands in
  `tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>`.
  See the type signatures at
  `crates/imap/src/connection/uid_ops.rs:160`, `190` and
  `crates/imap/src/connection/seq_ops.rs:80`.
- Consumer hook (synchronous): `StreamingFetchConsumer::on_response`
  in `crates/imap/src/connection/dispatch/fetch.rs:147-198`. The
  hook is a plain `fn on_response(&mut self, ...)`; it calls
  `self.tx.send(Ok(*fr))` on each `UntaggedResponse::Fetch`. The
  send is non-async because the trait method is non-async, which is
  the entire reason the channel must be unbounded today.
- Ergonomic adapter: `uid_fetch_each`
  (`crates/imap/src/connection/ergonomics.rs:83-107`) constructs
  the unbounded channel internally, joins the fetch future and a
  drain future, and runs both to completion. The caller passes only
  a `FnMut(FetchResponse)` callback.
- Driver loop body: `run_one_command` in
  `crates/imap/src/connection/driver/mod.rs:455-584`. The loop:
  1. Encodes and sends the command via `send_command_on_wire`.
  2. Repeatedly calls `wire_reader.read_one(utf8).await?`
     (line 519) to pull the next parsed `Response`.
  3. Applies side effects to `ProtocolState`.
  4. On a matching tagged response, finalizes the consumer and
     returns.
  5. On an untagged response, runs `classification::classify` and
     either calls `consumer.on_response(...)` (synchronously, line
     560) or forwards the response to `event_sink`.
  6. On a continuation, calls `consumer.on_continuation(...)` and
     writes the consumer's reply bytes back to the wire (line 577).

The driver loop has exactly one `.await` per iteration that yields
to the runtime: `wire_reader.read_one`. Consumer dispatch is
synchronous. There is no point in the loop where the driver can be
made to wait on downstream capacity without redesigning the
`Consumer` trait.

The result: when the receiver in `uid_fetch_each` (or any other
caller) is slow, `StreamingFetchConsumer` happily keeps stuffing
into the unbounded channel as fast as the driver reads from the
socket. Memory grows unbounded. The driver never applies TCP-level
backpressure to the server.

Other supporting points:

- The driver task is a single tokio task. Concurrent commands are
  serialized through `cmd_rx` (`driver/mod.rs:349-433`). One
  in-flight command at a time per connection. Pipelining
  (`DriverCommand::Pipeline`) bundles multiple commands into one
  driver invocation; per-command consumers still receive responses
  synchronously.
- `WireReader::read_one` is the single read point. The driver does
  not implement a separate read pump.
- `event_sink.emit(...)` is non-async and uses an internal queue;
  it is not a source of backpressure today.

## The constraint

`plans/sync-engine.md` (Backpressure) states the bounded-buffering
invariant: "Backpressure flows: consumer poll on the outer
`Stream` slows the batch producer, which slows the wire fetcher,
which yields the HTTP/2 stream window or IMAP read pump. No layer
accumulates unboundedly." `plans/account-trait.md` makes this
binding: the IMAP `Account` impl returns `AccountStream<...>` from
`inventory_stream`, `get_stream`, `changes_stream`, and
`bulk_set_flags`, and the engine drives those streams via
`Stream::poll_next`. If the engine stops polling, every layer
upstream must stop producing.

Naive bounded swap fails. Replace `UnboundedSender` with
`mpsc::Sender<...>` and the synchronous `on_response` hook can no
longer send when the buffer is full. The options at that point are:

- `try_send` and drop on `Full`. Data loss. Violates "no FETCH
  responses dropped" - the engine's checkpointing relies on every
  batch being delivered.
- `blocking_send` from inside the async driver. Deadlocks the
  runtime worker.
- Park a "pending FETCH" in the consumer and re-deliver from the
  driver loop. Requires the driver to know which consumer is
  saturated, which means `on_response` becomes async or the driver
  reaches into consumer state. Either is a redesign.

Worse: even if dropping were acceptable, an unmodified read pump
keeps draining the socket. The OS TCP read buffer empties, the
server's send window opens, and the server keeps streaming FETCH
lines that have nowhere to go. The IMAP server is happy because
the client keeps acking TCP segments; the client is unhappy
because memory is full. There is no protocol-level
"please-pause-FETCH" signal in IMAP - flow control is implicit in
read-side TCP windowing. The driver must stop reading from the
wire to stop the server from sending.

Continuation-request hazard: APPEND, AUTHENTICATE, and SASL flows
require the driver to write back a continuation reply when a `+`
arrives mid-command. If the driver is parked waiting on downstream
capacity at that moment, the server is waiting for the
continuation reply and the client is waiting for the consumer to
drain. Classic deadlock. Backpressure must be applied only where
no continuation-request is in flight.

## The new shape

The cooperative slow-pump model. Three pieces.

### Where the bounded channel sits

The bounded channel goes between the driver and the consumer's
downstream, owned by a new consumer variant
(`BoundedStreamingFetchConsumer`). The channel capacity is
documented; the default is 64 `FetchResponse` records, configured
per command, with override via `ImapConnection` builder methods
that the `Account` impl uses.

Why 64: matches the engine's stated default "64 batches in flight"
from `plans/sync-engine.md`. A FETCH response is one item, not a
batch; the engine adapts by batching items downstream of this
channel. The driver-side channel is intentionally smaller than the
engine's batch buffer so the wire pump pauses sooner.

The consumer no longer holds the sender alone. It holds a
`BoundedFetchPipe`:

```text
BoundedFetchPipe {
    tx: mpsc::Sender<Result<FetchResponse, Error>>,
    permit_slot: Option<mpsc::Permit<...>>,
    ambiguous_buffer: Vec<UntaggedResponse>,
}
```

The `permit_slot` pre-reserves capacity. Before the driver reads
the next wire response, it asks the consumer "can you take one
more?" The consumer answers by awaiting
`tx.reserve()`. If a permit is granted, the consumer stores it and
the next `on_response` synchronously consumes it via
`permit.send(...)`. If `reserve()` returns `Closed`, the consumer
records that the downstream went away (treated identically to the
existing dropped-receiver case: keep draining the wire to tagged
completion, but discard).

This split is the crux: capacity reservation is async and happens
in the driver loop; data placement remains synchronous and happens
in `on_response`. The `Consumer` trait does not gain an async
method.

### Driver decides when to read

The driver loop in `run_one_command` gains a pre-read backpressure
hook. Before each `wire_reader.read_one(...).await`:

1. Ask the consumer for its current backpressure state via a new
   trait method `Consumer::backpressure_state(&mut self) ->
   BackpressureState`. Variants:
   - `Ready` - the consumer has no buffered backpressure (current
     behavior; default impl for all non-streaming consumers).
   - `NeedsCapacity` - the consumer has an outgoing channel and
     does not currently hold a permit. The driver must await the
     consumer's reservation before reading further.
   - `Drained` - the receiver is closed; the driver can read at
     full speed and the consumer will discard.
2. If `NeedsCapacity`, the driver calls
   `consumer.reserve_capacity().await` (a new method on a streaming
   sub-trait, see below). This `.await` yields the runtime and,
   while the future is pending, the OS read buffer fills, the TCP
   window closes, and the server stops sending. Backpressure
   propagates to the wire by the absence of further reads.
3. Once `reserve_capacity` resolves (permit obtained, channel
   closed, or error), the driver returns to its normal read loop
   and calls `read_one`.

The new sub-trait:

```text
trait StreamingConsumer: Consumer {
    fn backpressure_state(&self) -> BackpressureState;
    async fn reserve_capacity(&mut self) -> Result<(), Error>;
}
```

Object-safe variant lives next to `ConsumerErased` in
`driver/mod.rs`. `DriverConsumer` gains a third variant
`StreamingRegular(Box<dyn StreamingConsumerErased>)` so the loop
can pattern-match the streaming case without paying the cost for
non-streaming commands.

### Partial FETCH responses

One command can produce many untagged FETCH lines (one per
message, plus EXISTS / FLAGS / EXPUNGE / VANISHED interleaved).
The bounded channel applies to *delivered* `FetchResponse` items,
not to the bytes on the wire. A single FETCH response is parsed by
`WireReader::read_one` before the consumer ever sees it; the
literal payload (body data) is fully resident in the parser's
buffer by the time `on_response` runs.

This is fine: one `FetchResponse` is the unit of bounded buffering
at this layer. Per-response memory pressure (huge body literals)
is the existing `FetchLimit` story and stays where it is in the
buffering `FetchConsumer`. The streaming variant's contract is "no
more than N parsed FetchResponses in flight downstream," not "no
more than M bytes."

For VANISHED, FLAGS, EXISTS, EXPUNGE interleaved with FETCH: those
are classified by `classify` as `OnlyUnsolicited` and routed to
`event_sink`, not to the streaming consumer. They do not consume
permits. The event sink has its own bounded capacity (existing
mechanism in `event_sink.rs`) and the driver already does an
opportunistic drain at the top of each iteration
(`driver/mod.rs:347`). Keep that.

### QRESYNC VANISHED drain

`plans/imap/condstore-qresync.md` ("Implementation footguns"):
"Drain untagged VANISHED on every command in a QRESYNC session."
The streaming FETCH path must surface those VANISHED events
without losing them and without contending for the downstream
permit.

Two-channel design for the streaming consumer:

- Primary `FetchResponse` channel (bounded, the one above).
- VANISHED side-channel routed through the event sink. The
  existing `event_sink.emit(...)` path is already where unsolicited
  VANISHED responses go for non-streaming commands; the streaming
  consumer reuses it. The `BoundedStreamingFetchConsumer` matches
  on `UntaggedResponse::Vanished { earlier: false, .. }` and
  forwards to the event sink rather than the data channel. (Plain
  VANISHED with `earlier: true` arriving outside of a SELECT or a
  CHANGEDSINCE FETCH is a protocol violation; emit to events with a
  warning.)

The `FetchVanishedConsumer`
(`crates/imap/src/connection/dispatch/fetch.rs:273-379`) handles
the `UID FETCH CHANGEDSINCE ... VANISHED` case where VANISHED is
solicited. That consumer is buffering, not streaming; the
backpressure rework does not change its shape. A new streaming
variant `BoundedStreamingFetchVanishedConsumer` is added that
emits both `FetchResponse` items and `VanishedRange` items through
*one* channel of `Result<FetchStreamItem, Error>` where:

```text
enum FetchStreamItem {
    Fetch(FetchResponse),
    VanishedEarlier(Vec<UidRange>),
}
```

This keeps the per-command VANISHED data correlated with the FETCH
batch the engine is consuming. Unsolicited (`earlier: false`)
VANISHED still goes through `event_sink`.

## Composition with the pool

Per `plans/account-trait-shape.md` (Q2 verdict, IMAP is
pool-shaped), the `Account` impl owns a pool of `ImapConnection`s.
Each pool member is one driver + one connection. The bounded
streaming design composes naturally:

- Pool checkout returns an exclusive `PooledConnection<'pool>`
  guard wrapping `&ImapConnection`. While the guard is held, no
  other task uses that connection.
- Streaming methods on `ImapConnection` (e.g. `uid_fetch_stream`,
  the new return type) yield an `AccountStream<...>` that captures
  the pooled connection inside its state. Dropping the stream
  drops the guard and returns the connection to the pool. Polling
  the stream forwards to the bounded receiver.
- The streaming handle exposed by the `Account` impl looks like:

```text
fn inventory_stream(&self, scope: CursorScope) -> AccountStream<...> {
    Box::pin(async_stream::try_stream! {
        let conn = self.pool.checkout().await?;
        let (rx, fetch_fut) = conn.uid_fetch_stream(...);
        let driver_handle = tokio::spawn(fetch_fut);
        let mut rx = rx;
        while let Some(item) = rx.recv().await {
            yield item?;
        }
        driver_handle.await??;
        drop(conn); // returns to pool
    })
}
```

Two notes:

- The driver-side future (`fetch_fut`) is the
  `fetch_streaming_impl` await. The consumer-side
  `mpsc::Receiver` is the bounded channel. The two are joined by
  the spawned task, exactly as `uid_fetch_each` joins them today
  (`crates/imap/src/connection/ergonomics.rs:101`), but with a
  bounded channel between them.
- Cancellation of the outer stream (drop) closes the receiver,
  which is observed by `reserve_capacity` returning `Closed`,
  which lets the driver continue draining to tagged completion
  with discard. The connection is then returned to the pool
  cleanly. This matches the existing dropped-receiver behavior in
  `StreamingFetchConsumer::on_response`
  (`dispatch/fetch.rs:177-179`).

The pool itself is out of scope for this plan (it is its own work,
tracked separately) but the contract above is what the pool must
support: `checkout().await -> PooledConnection<'pool>` with one
driver per connection, no shared driver-loop state between
connections.

Concurrent commands on the same checkout: not supported. The
existing `cmd_rx` mpsc serializes commands; a streaming FETCH
holds the driver for the duration of the command. Engine
multiplexing happens across connections, not within one.

## IDLE coexistence

IDLE has its own driver-loop state today (`run_idle` in
`crates/imap/src/connection/driver/idle.rs`). It does not use the
command dispatch path; it has its own select between `done_rx` and
`wire_reader.read_one`. Events are emitted through `event_sink`.

For the backpressure rework: IDLE stays exactly as it is.
Specifically:

- The bounded channel applies only to `DriverCommand::Run` with a
  streaming consumer. `DriverCommand::Idle` does not have a
  consumer; it emits through `event_sink` which already has its
  own bounded queue (`event_sink::DriverEventSink`).
- IDLE cannot run concurrently with a streaming FETCH on the same
  connection (the driver is one task processing one
  `DriverCommand` at a time, see `driver/mod.rs:349-433`). The
  pool keeps separate connections for IDLE and FETCH; the engine's
  multi-folder multiplexer
  (`plans/sync-engine.md` -> Multi-folder multiplexing) is
  expected to dedicate one pool member to IDLE on the most-active
  folder and use other pool members for backfill / hydrate / diff.
- The event-sink bounded queue does apply backpressure to the
  IDLE driver: when the queue is full, `event_sink.emit()` either
  blocks or drops with a warning (the existing
  `drain_pending_nonblocking` semantics decide this; that
  decision lives in `event_sink.rs` and is outside this plan).
  The IDLE path's behavior under event-sink saturation is a
  separate decision; this plan does not regress it.

What this plan does require of IDLE: nothing. The IDLE state
machine is independent of the streaming FETCH state machine.
Documenting that fact, and pinning IDLE-and-FETCH-on-one-
connection as "do not do this; the pool is the answer," is the
extent of the cross-cutting work.

## Cancellation safety

Current contract (per `reference/imap.md` -> "Connection state
machine"): dropping the future returned by a command method leaves
the connection `Broken`; the driver continues to drain the
response and abandons the reply (the `oneshot::Sender` for
`result_tx` is dropped, no panic). The new design must preserve
this.

Where cancellation lands safely:

- Caller drops the outer `AccountStream`. The bounded receiver is
  dropped; the streaming consumer's next `reserve_capacity` call
  returns `Closed`. Driver transitions to "drain to tagged
  completion with discard" - identical to today's dropped-receiver
  path. Connection ends `Ok`, returns to the pool. Safe.
- Caller drops the future returned by `uid_fetch_stream` (the
  driver-side future) while it is awaiting `reserve_capacity`.
  The driver task is owned by the connection, not by the caller;
  the `result_tx` oneshot is dropped, the driver continues. Same
  as today.

Where cancellation cannot land safely (unchanged from today):

- Mid-command, mid-continuation-request. If the driver has read a
  `+` and is in the middle of writing the consumer's reply back to
  the wire, dropping does not stop the in-flight write; the
  underlying `WireReader::write_all` is set up so that the
  connection is marked `Broken` before the await and only restored
  on success (`reference/imap.md` -> "Connection state machine").
  A drop here leaves `Broken`, pool reclamation discards.
- Mid-literal. `WireReader::read_one` reads a complete response
  including any literal payload. A drop in the middle of a long
  literal leaves `Broken`. Pool reclaims by discarding.

The new method `reserve_capacity` must be cancel-safe. Its
implementation is `tx.reserve().await`, which is cancel-safe by
documentation (tokio mpsc `Sender::reserve`): dropping the
returned future before it resolves does not reserve a permit. Good.

Pin this in the driver: place the `.await` for
`reserve_capacity` such that a drop at that point cleanly
transitions the connection state. Mirror the
"set-broken-before-await" pattern used by the wire ops, except the
reserve call does not touch the wire so there is no wire-state
flag to flip; the consumer state is the only mutated state and
it does not produce externally observable inconsistency.

## Continuation-request protocol

APPEND, AUTHENTICATE, and SASL commands receive `+` continuations
from the server. The driver loop's continuation branch
(`driver/mod.rs:572-578`) writes the consumer's reply back to the
wire synchronously.

None of these commands use the streaming consumer. APPEND uses
`AppendConsumer` or `MultiAppendConsumer`; AUTHENTICATE uses
`AuthenticatePlainConsumer` etc. (see
`crates/imap/src/connection/dispatch/auth.rs`). All return
non-streaming output types. Therefore the bounded-channel logic
never interacts with continuation handling.

The constraint that must hold: streaming consumers must not gain
continuation-request handling. If a future streaming command needs
continuations, that is a separate design problem; the present
contract is "streaming = no continuations." Encode this by having
`BoundedStreamingFetchConsumer` implement `Consumer` and
`StreamingConsumer` but not `ContinuationConsumer`. The driver's
continuation branch already errors on a `Regular` (non-
continuation) consumer; the new `StreamingRegular` variant is
treated identically by `on_continuation` (return
`Error::Protocol("unexpected continuation during streaming
command")`).

## Migration plan

Files that change. Phases are independent; phase 1 introduces the
new types alongside the old; phase 2 swaps callers; phase 3
removes the unbounded path.

### Phase 1: add bounded streaming alongside unbounded

- `crates/imap/src/connection/dispatch.rs`
  Add `StreamingConsumer` sub-trait, `BackpressureState` enum,
  `StreamingConsumerErased` object-safe wrapper. Re-export new
  types from `dispatch` module root.
- `crates/imap/src/connection/dispatch/fetch.rs`
  Add `BoundedStreamingFetchConsumer` and
  `BoundedStreamingFetchVanishedConsumer`. Both hold an
  `mpsc::Sender` with optional pre-reserved `Permit`. Add the
  `FetchStreamItem` enum for the VANISHED-aware variant. Keep
  `StreamingFetchConsumer` for now (deprecation pass in phase 3).
- `crates/imap/src/connection/driver/mod.rs`
  Add `DriverConsumer::StreamingRegular` variant. In
  `run_one_command` and `run_prebuilt_command`, before each
  `wire_reader.read_one`, call into the new
  `consumer.backpressure_state()` / `reserve_capacity()` flow on
  the streaming variant only. Other variants take the existing
  path with no observable change.
- `crates/imap/src/connection/uid_ops.rs`,
  `crates/imap/src/connection/seq_ops.rs`
  Add new public methods `uid_fetch_stream` and `fetch_stream`
  that return a `(mpsc::Receiver<...>, impl Future)` tuple, where
  the receiver is bounded. Capacity is a parameter on these
  methods with a default of 64. Keep `uid_fetch_streaming` and
  `fetch_streaming` for now.
- `crates/imap/src/connection/ergonomics.rs`
  Update `uid_fetch_each` to use the bounded path internally. The
  callback contract does not change; backpressure becomes implicit
  because the callback now slows the driver via the bounded
  channel.

### Phase 2: callers move to the new API

- Any internal callers (search the crate for
  `uid_fetch_streaming` / `fetch_streaming`) move to the bounded
  methods. The downstream `bifrost-sync` `Account` impl (not yet
  present) is written against the bounded methods from the start.

### Phase 3: remove the unbounded path

- Delete `StreamingFetchConsumer`, `uid_fetch_streaming`,
  `fetch_streaming`. This step happens only after every internal
  and downstream caller is on the bounded API.

### Tests

Per `AGENTS.md` testing rules, no end-to-end or live-server tests.
The deterministic unit tests for this rework:

- `dispatch/fetch.rs` (existing test module): construct a
  `BoundedStreamingFetchConsumer` over a small-capacity channel
  (size 2), feed it three `UntaggedResponse::Fetch` directly via
  the synchronous `on_response`, observe that the consumer
  transitions to `NeedsCapacity` after two and does not lose the
  third. (The third is held in the consumer's pending slot until
  the next `reserve_capacity` succeeds.)
- A second test that drops the receiver and feeds further
  responses: the consumer's `on_response` discards, the consumer
  reports `Drained` from `backpressure_state`, the driver's
  reserve path becomes a no-op.
- A test that the VANISHED-aware variant routes
  `Vanished { earlier: true, .. }` through the data channel and
  `Vanished { earlier: false, .. }` through `reclassified_as_events`.
- A test that the streaming consumer paired with a continuation
  request causes the driver dispatch to error with
  `Error::Protocol`.
- A `Consumer` trait test that the default
  `StreamingConsumer::backpressure_state` impl on non-streaming
  consumers reports `Ready` (i.e., the new sub-trait does not
  introduce behavior changes on the existing buffering paths).

None of these tests need a real server or a mock wire. They
exercise the consumer in isolation against synthesized
`UntaggedResponse` values, which is the testing scope this repo
already uses for `FetchConsumer`,
`FetchVanishedConsumer`, etc.

## Risks and known unknowns

Where the cooperative slow-pump is straightforward:

- Adding `reserve_capacity` and pre-reserving permits before each
  read is a small, local change to the driver loop. `tokio`'s
  `Sender::reserve` is the right primitive, well-documented as
  cancel-safe, and matches the lock-then-write pattern.
- The split of solicited FETCH (data channel) vs. unsolicited
  events (event sink) already exists; backpressure applies only to
  the data side. Untagged VANISHED, EXISTS, EXPUNGE go to the
  event sink today and continue to do so.
- The pool boundary is mechanical. The streaming methods return
  `(Receiver, Future)`; the `Account` impl spawns the future and
  yields from the receiver. This is the same shape `uid_fetch_each`
  already uses, just bounded.

Where it is genuinely tricky:

- **Event-sink backpressure interaction.** The event sink is the
  sink for unsolicited responses, including the VANISHED stream
  during a QRESYNC session. If the event sink fills (downstream
  not draining), the driver currently has a non-blocking emit
  path (`emit_with_overflow_policy`, whatever that turns out to
  be). Under the bounded-FETCH model, slow downstreams will be
  the *common* case, and a slow event-drain will couple
  unpredictably with a slow data-drain. The decision is whether
  the event sink also becomes a hard-bounded await point (couples
  data and events at the wire) or stays a drop-on-overflow path
  (data backpressure works but events are lossy when the consumer
  is slow). This plan does not pre-decide; it pins it as a
  follow-up in the same milestone. Lossy events plus the engine's
  push-is-invalidation model
  (`plans/sync-engine.md` -> Push is invalidation) is probably
  acceptable - the engine reconciles on next changes_stream
  poll - but the loss case must be observable
  (`Warning::StrategyDowngraded` or a similar
  `Warning::EventDropped`).
- **Continuation deadlock during literal sending in APPEND.**
  APPEND sends large message literals upstream; the server
  responds with `+` continuations. The driver's continuation
  branch writes the reply synchronously. Backpressure is on the
  read side and on the consumer's output channel, neither of
  which APPEND uses. But: if APPEND ever gains a streaming
  variant (multi-message APPEND with per-message progress), the
  current design forbids streaming + continuation. That is a
  documented limitation, not a hidden hazard. Phase-2 implementer
  must keep this restriction.
- **Per-batch latency under heavy backpressure.** Pre-reserving a
  permit adds one `.await` per wire read in the streaming path.
  Under no contention, `tokio::mpsc::Sender::reserve` resolves
  immediately and the cost is one task scheduling. Under
  contention, the driver parks - which is the intent. The
  worry is the no-contention overhead on small fetches (one or
  two messages). Mitigation: the streaming consumer's
  `backpressure_state` returns `Ready` when it already holds a
  permit, skipping `reserve_capacity` entirely. The permit
  reservation cost is paid at most once per message, not per
  wire byte.
- **Pool interaction with cursor advance.** The streaming FETCH
  for `inventory_stream` and `changes_stream` returns one cursor
  advance per page. The pool holds the connection for the
  duration of the stream. If the engine pauses a stream
  (`Control::pause()` per `plans/sync-engine.md`), the
  connection sits idle in the pool with `SELECTED` state pinned
  to a folder. Long pauses risk server-side IDLE timeouts (some
  servers terminate inactive connections after 5-30 minutes).
  Either the pool's idle timer reclaims paused-stream
  connections, or the engine's pause semantics tear down the
  stream and re-establish on resume. The latter is simpler; pin
  it. (Re-establishment is cheap on QRESYNC: the cursor's
  `advanced_through` lets the protocol resume.) This is a sync-
  engine-side decision; flag it for the engine implementer.
- **Mid-page resumption inside a single FETCH command.** A FETCH
  command for a range of UIDs cannot be paused mid-response. The
  driver either reads the next response or it does not; there is
  no "stop at UID N, resume at UID N+1 next time" within one
  command. The pause boundary is between commands. The engine's
  `PageBoundary` model (`plans/sync-engine.md` -> Stream
  contract) already pins cursor advances to page boundaries; pin
  the IMAP protocol crate's "page" to "one FETCH command's worth
  of responses" so pause / checkpoint / resume align. Document
  this in the IMAP Account impl when it lands.
- **The `event_sink` drain at the top of each loop iteration**
  (`driver/mod.rs:347`) interacts with backpressure: if the event
  sink is at capacity and `drain_pending_nonblocking` becomes
  not-non-blocking (i.e. the drain itself starts awaiting), the
  loop iteration order matters. Pre-reserve permit first, drain
  events second, read wire third - or drain events first,
  pre-reserve second, read third? The right answer is probably
  "drain events first" because event-sink overflow is the more
  catastrophic case (lost EXISTS could miss an arrival
  notification), but this needs verifying with the event-sink
  implementer.
- **Stalwart's malformed VANISHED behavior.** Per
  `condstore-qresync.md`, Stalwart returns VANISHED UIDs outside
  the requested set. The bounded streaming consumer must apply
  the same defensive filtering that `FetchVanishedConsumer` does
  (`dispatch/fetch.rs:341-353`). Easy to forget; pin it.
- **Drop-with-pending-permit.** If the consumer holds a permit and
  the connection's driver task is dropped (e.g. pool shutdown),
  the permit is returned to the channel by tokio's destructor
  automatically. No leak. Verify in the bounded-consumer drop
  test.

Where the design is genuinely uncertain and needs implementer
judgment:

- Whether `reserve_capacity` should be on `Consumer` itself (with
  a default impl that returns `Ready`) or on a separate
  `StreamingConsumer` sub-trait. This plan assumes the sub-trait
  for cleaner pattern-matching in `DriverConsumer`. The
  implementer may discover the default-on-base-trait is simpler.
  Either is acceptable; both preserve the contract.
- Whether the streaming methods return `(Receiver, Future)` or a
  single combined `Stream` that owns both. The tuple is easier to
  test in isolation; the combined `Stream` is friendlier to the
  `Account` impl. The implementer picks based on what the
  emerging `Account` IMAP code wants. The bounded-channel
  contract holds either way.
