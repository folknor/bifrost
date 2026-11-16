# Closed-account gate: one admission boundary for every Account method

Design chain for the closed-account bullet of the wave 10 to 16 residue in
`notes/bugs.md`. Sparred and agreed; not yet built.

## Problem

After `Account::close()`, providers disagree about what a later call does:

- Google wraps every `AccountFuture` method in `shutdown_future` (refuse with
  `AccountClosed` before the request, `Transport(Network)` plus `InFlight` if
  close lands mid-flight); streams end silently.
- IMAP refuses explicitly only in send, draft send, raw blob fetch and polling
  EXAMINE. Everything else reaches the pool, which answers
  `TransportFailure::Closed` -> `Transport(Network)` "IMAP connection closed":
  a retryable network error for an account the user removed.
- Graph and JMAP gate only push registration. CalDAV and CardDAV gate nothing
  (`close()` only ends `scope_lifecycle_stream`).
- The `Account::close` trait doc states no post-close contract at all.

The yardstick: a user who removed an account expects its work to stop. Every
new call refuses uniformly with `AccountClosed` (terminal `Cancelled`) before
I/O, never a transport error, never a silent success.

## Shapes considered

1. Per-provider gates in every method. Six crates times about ninety methods,
   each a place to forget; this is how the current gaps arose.
2. Chokepoint gating (net request builder, IMAP pool checkout, DAV
   `dispatch_once`). Rejected in the spar on concrete bypasses: CalDAV
   `discover_cursor_scopes` yields from cached URLs with no dispatch; the IMAP
   ManageSieve leg opens its own connection outside the pool; local validation
   and inherited `Unsupported` defaults complete before any transport; DAV
   inventory materialises batches up front so closing mid-stream stops no
   output; and a request that passed a boolean check can still dispatch after
   close while it waits on rate admission, a connection or a token. Net's
   builder is also too early: retries re-attempt without a new builder.
3. **Chosen: a shared wrapper in bifrost-types with generated forwarding,**
   backed by one lifecycle/admission implementation. Provider transport gates
   stay as a second boundary.

## Design

- `bifrost-types` owns `ClosingAccount<A: Account>` (name open) wrapping any
  provider. Its `Account` impl is **generated from the trait declaration**,
  including every method that has a default body, so adding a trait method
  generates its forwarding and the wrapper can never silently inherit a
  default where the provider overrides it. A hand-written forwarding impl is
  not acceptable for that reason.
- Admission is checked when a lazy future or stream is **first polled**, not
  when it is constructed: an `AccountFuture` built before close and polled
  after it is refused.
- Three distinct behaviours, never conflated:
  1. Refuse new work: a call first polled after close returns
     `AccountError::account_closed(operation, protocol)` before any I/O.
  2. Settle admitted work: an operation already admitted keeps its own result
     and transmission evidence. Replacing an in-flight mutation's result with
     `AccountClosed` could hide a committed write. Multi-leg operations keep
     their partial-commit records. Google's existing "shutdown after poll is
     `Transport(Network)` + `InFlight`" is the model.
  3. Terminate streams: decide per stream kind whether an open stream drains
     or ends; never fabricate a successful inventory completion when ending.
- Explicit exemptions, because their signatures cannot carry an error:
  `protocol`, borrowed `capabilities`, `set_priority`, `set_bandwidth_cap`,
  `throttle_keys`, `describe_cursor`, `flag_target`, `push_lanes`, and
  repeated `close` (idempotent, `Ok`). The design must list every exempt
  method by name, generated from the same trait walk.
- Composed accounts: the wrapper sits at the outermost account the factory
  returns. Composed legs (IMAP's CalDAV/CardDAV/SMTP/ManageSieve) are reached
  only through it, so one gate covers them; their own `close` still runs from
  the provider's `close`.
- Factories return the wrapped account, so the engine and direct consumers get
  the same contract. The `Account::close` trait doc gains the post-close
  contract; `reference/error-model.md` `AccountClosed` and each provider
  reference replace their partial lists with a pointer to it.

## Link plan

Each link is one commit with its own gate.

1. bifrost-types: the generator (macro or build-time walk of the trait),
   `ClosingAccount`, admission at first poll, the exemption list, the trait
   doc contract, and hermetic tests with a `StubAccount`: refuse-after-close
   for a future built before close, a default-bodied method, a stream, and
   an exempt accessor; a method added to the trait without regenerating fails
   to compile.
2. Wire every factory (IMAP, JMAP, Graph, Google, CalDAV, CardDAV) to return
   the wrapped account. Delete the per-crate refusals the wrapper now owns
   only where the wrapper provably covers them (keep transport gates).
   Update provider references.
3. Stream termination policy per stream kind, with tests that a closed
   inventory stream never reports completion.
4. IMAP: the pool's post-close `TransportFailure::Closed` stays as the second
   boundary but is no longer reachable from a gated method; pin that with a
   test through the wrapper.
