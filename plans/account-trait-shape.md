# Account trait shape: resolved decisions

Three shape questions about the `Account` trait. Q1 and Q2 are
resolved by ratatoskr's empirical experience plus a third-party
review and a focused read of `crates/imap/` as it stands today;
Q3 is newly raised by the ratatoskr review and needs sign-off
before phase 1.

## Q1: Dispatch - is `dyn Account` reachable?

### How we want it to look

The engine holds N heterogeneous accounts in one collection and
multiplexes them uniformly:

```rust
struct SyncEngine {
    accounts: Vec<Arc<dyn Account>>,
}
```

Adding a sixth protocol means editing one protocol crate and
registering the new `Account` impl. The engine has no generic
parameter that grows with protocol count.

This matches the trait doc's existing language: cursor state is
opaque to the engine, batches carry opaque `advanced_through`
bytes, capabilities discriminate behavior at runtime. The engine
already treats `Account` as type-erased in spirit.

### Problem statement

The trait as drafted in `plans/account-trait.md` has four object-
safety blockers, not the three first identified:

- `impl Stream<...>` return positions (RPITIT) are not dyn-safe.
- `async fn` methods (`push_subscribe`, `push_unsubscribe`,
  `close`) desugar to return-position `impl Future`, same RPITIT
  problem.
- `type ChangeState: Send + Sync` associated type forces a binding
  on every dyn handle (`dyn Account<ChangeState = X>`), which
  defeats heterogeneous collection.
- `impl Stream<...>` input parameters on the bulk-mutation methods
  are not dyn-safe either.

Plus a shape problem in `close(self)`: a self-consuming method
does not compose with `Arc<dyn Account>`, where the handle is
shared across tasks and no single caller owns it outright.

### Verdict

YES, `dyn Account` is reachable. Final erasure set:

```rust
type AccountStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;
type AccountFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

// Concrete opaque cursor state, replaces `type ChangeState`.
// Protocol tag and version prevent silent misrouting when an
// account-level dispatch hands the wrong cursor to the wrong
// protocol impl.
struct OpaqueChangeState {
    protocol: ProtocolKind,
    envelope_version: u32,
    bytes: Vec<u8>,
}
```

Applied to the trait:

1. All `impl Stream<...>` return positions become `AccountStream<T>`.
2. All `async fn` methods become `fn ... -> AccountFuture<T>`.
3. `type ChangeState` becomes the concrete `OpaqueChangeState`.
   Protocol crates serialize their native cursor shape through it
   and validate `protocol` + `envelope_version` on read.
4. `impl Stream<...>` input parameters become `AccountStream<T>`.
5. `close(self)` becomes `close(&self) -> AccountFuture<Result<(),
   Error>>`, idempotent. Internal shutdown state plus an
   idempotent close composes cleanly with `Arc<dyn Account>`;
   self-consuming shutdown does not.

Ratatoskr has been running `Box<dyn ProviderSyncOps>` across four
providers for three years with no documented regret. The boxing
cost is irrelevant at network speed; I/O, parsing, batching, and
persistence dominate.

The real implementation risk is not dispatch but authoring
cancellation-safe streams that honor the per-batch checkpoint
contract. That is stream design, not dispatch.

## Q2: Send + Sync - is concurrent shared access reachable for IMAP?

### How we want it to look

The engine spawns concurrent tasks over one `Arc<dyn Account>`:

```rust
let account: Arc<dyn Account> = ...;

tokio::spawn({ let a = account.clone(); async move { push_loop(a).await } });
tokio::spawn({ let a = account.clone(); async move { backfill(a).await } });
tokio::spawn({ let a = account.clone(); async move { reconciler(a).await } });
```

Three tasks share one handle, running IDLE / backfill / reconcile
concurrently on the same account.

### Problem statement

JMAP / Gmail / Graph are HTTP-based with `Send + Sync` connection
pools underneath. Trivially yes.

IMAP is the question. The real constraint is not "one in-flight
tagged command at a time" - bifrost-imap supports command
pipelining via `DriverCommand::Pipeline` for safe-to-pipeline
commands. The real constraints are:

- `SELECTED` state is per-connection. A connection pinned to Inbox
  cannot serve an Archive request without UNSELECT/SELECT.
- IDLE ownership: once a connection enters IDLE, the driver loop
  is exclusive to IDLE until DONE. Other commands cannot run on
  that connection.
- Mailbox affinity: cursor advances are per-folder; the session
  that built a modseq baseline is the cheapest to advance it from.

What bifrost-imap looks like today (`crates/imap/src/connection/
mod.rs` and `crates/imap/src/connection/driver/`):

- `ImapConnection` is `Send` but not `Sync`. The events
  `mpsc::Receiver` is wrapped in `tokio::sync::Mutex` for `&mut
  self` drain access, which makes the public type non-`Sync`.
- Single session per handle; no built-in pool.
- A dedicated tokio task owns the TCP/TLS stream exclusively. All
  public methods are `&self` over channels and oneshot replies -
  structurally pool-friendly even though no pool exists yet.
- Driver supports `DriverCommand::Pipeline` for safe-to-pipeline
  commands plus `DriverCommand::Idle` that holds the driver loop
  exclusively until DONE.

Pool refactor cost (from the read of `crates/imap/src/`):

- ~500-600 LOC of new pooling infrastructure: pool struct,
  checkout/return, reuse heuristics that prefer already-selected
  sessions, idle timeout, min/max bounds, pool-level auth.
- No codec or parser changes.
- New tests for concurrent checkout, session reuse, IDLE
  coexistence.

The existing driver model is a good pool substrate: each driver
is independent, drivers share no state besides auth config, and
they can be spawned and reaped cleanly.

### Verdict

YES, pool-shaped from day one. The trait bound `Account: Send +
Sync` is reachable for IMAP; the architectural commitment is to a
session pool inside `bifrost-imap`, not a mutex around a single
session.

The pool justification is the concurrency model, not the trait
bound itself. Without IDLE + concurrent backfill/reconciliation,
connect-per-call would satisfy `Send + Sync` with no pool at all.
But bifrost ships IDLE in v1 and the engine multiplexes IDLE +
backfill + reconciler tasks against one account, which forces N >=
2 sessions, which forces a pool.

The pool buys push and amortized handshake. It does not buy
correctness - correctness is in the driver and codec, which are
already done. Existing `ImapConnection` becomes the pool's
checkout unit; the public Account type is the pool itself.

## Q3: Account lifecycle and ownership

[Raised in the ratatoskr review. Resolved as phase-1 shape.]

### How we want it to look

The consumer registers a long-lived account factory; the engine
owns the current open `Arc<dyn Account>` and calls the factory
when it needs a fresh handle (capability change, transport
reset):

```rust
trait AccountFactory: Send + Sync + 'static {
    fn open(&self) -> AccountFuture<Result<Arc<dyn Account>, Error>>;
}

let factory: Arc<dyn AccountFactory> =
    Arc::new(ImapAccountFactory::new(config));

engine.attach(account_id, factory);
// engine calls factory.open() to get the first handle, drives
// streams against it, calls factory.open() again on reopen cycles.
engine.detach(account_id).await;  // closes current handle, drops factory.
```

Construction is consumer-side via the factory's per-protocol
builder (config + transport + auth). The engine never sees
protocol-specific config; it only sees the factory trait and the
opened `Arc<dyn Account>`.

### Problem statement

Ratatoskr today constructs a `Box<dyn ProviderSyncOps>` per
action, per sync run, and during prefetch. Construction is cheap
because the structs hold no live state: Gmail/Graph/JMAP clients
clone `Arc<reqwest::Client>`, IMAP holds no session.

Bifrost `Account` is heavier:

- Owns a connection pool (IMAP) or HTTP client + token refresher
  (JMAP/Gmail/Graph).
- Owns push subscription handles, IDLE-bound sessions, in-flight
  cursor advances.
- Holds open observability spans, retry budget, rate-limit
  governor.

Construct-per-call would tear down IDLE between calls and waste
the pool. The engine wants one handle per attachment, lived
across push / reconcile / backfill scheduling.

Plain `attach(account_id, Arc<dyn Account>)` conflicts with the
existing `RecoveryClass::CapabilityChanged` story in
`plans/account-trait.md`, which says "the engine re-opens the
account." If the engine receives only a ready handle and no way
to reconstruct one, reopen cannot live inside the engine.

### Verdict

PHASE-1 SHAPE. Three details pinned:

1. **Sync session = engine attachment lifetime, not one sync run.**
   The handle lives across push, reconcile, and backfill scheduling
   until `detach`. Cursor advances, push subscriptions, and
   observability spans persist across stream restarts within the
   attachment.

2. **`close(&self)` is local handle teardown only.** It stops
   streams, releases IMAP sessions, closes the WebSocket or IDLE,
   stops local workers. It does NOT destroy durable server-side
   push subscriptions. Server-side subscription lifecycle stays
   explicit through `push_subscribe` / `push_unsubscribe`; the
   engine calls `push_unsubscribe` on account removal or when the
   consumer disables push, never as a side effect of process
   shutdown. Otherwise an ordinary restart silently wipes
   subscriptions the consumer still wants.

3. **`attach` takes a factory, not a handle.** The engine owns
   the current open `Arc<dyn Account>` and the factory; reopen
   cycles call `factory.open()` without the engine knowing
   protocol config. This resolves the `RecoveryClass::
   CapabilityChanged` reopen story: "the engine re-opens the
   account" means "the engine calls the factory."

`close(&self)` stays idempotent so detach/shutdown races are
safe, and so consumers that call close before detach (defensive
ordering) do not corrupt engine state.
