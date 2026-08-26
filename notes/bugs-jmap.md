# bifrost-jmap: hunt findings

Scope: `crates/jmap/` - dispatch, transport, per-RFC module pattern, capability
negotiation, error model, and the `Account` impl under `crates/jmap/src/sync/`.

Hunter note: baseline was green (522 tests pass, clippy/gremlins clean), so
everything below is a live gap the suite does not cover.

## J7. Duplicate submitted ids collapse in hydration's closed accounting

**MEDIUM confidence, low severity.** `hydrate::reconcile_hydration` builds
`by_native: HashMap<&str, &ObjectId>` from the submitted slice. If the caller
submits the same id twice in one batch, one entry is silently lost and the batch
returns fewer outcomes than inputs - a hole in the "every submitted id leaves on
exactly one lane" contract the module otherwise defends rigorously. Dedupe on
entry (and document it) or key by index.

## J8. `maxCallsInRequest` is enforced at exactly one call site

**MEDIUM confidence.** `Request::call` auto-appends to `using` and pushes calls
with no count check. Only `batched_open_probes` consults the advertised call
count; `send_methods_within` checks encoded *size* only. Multi-call flows built
elsewhere (`send_message` with its result-referenced Email/set +
EmailSubmission/set + onSuccessUpdateEmail, the tuple batches up to `M8`) can
exceed a conservative server's `maxCallsInRequest` and take a whole-request
`limit` rejection. The size guard was deliberately made a *required* argument so a
call site "cannot forget the question" - the call-count guard deserves the same
treatment, in `Request` itself rather than per-caller.

## J9. `bytes_in: 0` on all eleven `Batch` constructions in `sync/`

**MEDIUM confidence, systematic.** Every stream reports zero bytes received.
`set_bandwidth_cap` delegates to `bifrost-net::AccountNet`, so caps may still
function, but any engine-side metering, cost accounting, or per-scope bandwidth
attribution reading `Batch::bytes_in` sees zero from this protocol. Either
populate it (the transport returns `Bytes`; the length is right there) or the
field is misleading.

## J10. Push reconnect emits `Reconnected` with no invalidation

**LOW-MEDIUM confidence, contract question.** After a disconnect/reconnect cycle
the reader emits `WatchEvent::Reconnected` and nothing else. If the engine does
not treat `Reconnected` as an implicit full invalidation, everything that changed
during the outage waits for the next poll. Given J2 (no `pushState` replay), the
reconnect path has *no* mechanism at all for catching up. Emitting a
`Coalesced`/`Unknown` invalidation alongside `Reconnected` would close it cheaply;
the engine contract needs confirming against `crates/sync/`.

## J11. Minor: the reader's first `reenable_current_push_set` sends a *disable* frame

**LOW confidence, cosmetic.** `enabled` starts empty at `open()`, and
`apply_push_set` maps an empty set to `client.disable_push_ws()`. So the very
first pass of the reader disables push on a connection nobody had enabled, then
announces `Reconnected`. Harmless in practice but it makes the "Reconnected means
a live connection whose subscription applied" invariant read oddly - an applied
*empty* subscription is not push.

## The structural story

**The inventory walk should not be a position-paged loop at all.** Three
parameterizations (primary / foreign / bounded page) share one loop, which is
right, but the loop's contract with the server is the wrong one. JMAP gives you
two stable mechanisms - anchored paging, and `Email/queryChanges` - and the crate
uses neither, then hand-rolls a defensive overshoot rule (the `Some(0)` arm) to
paper over one symptom of the instability while leaving the underlying skip open.
The rewrite argued for: make the walk anchor-based, freeze the result set with a
`before:` bound derived from open time, and delete the overshoot special-case
entirely, because with a frozen anchored window a zero-entry partition genuinely
does mean end-of-inventory. That is a real simplification, not just a fix.

**Push state belongs in one place, and it currently lives in three.**
`WsState.enabled`, the `subscriptions` registry, and the (missing) `pushState` are
three pieces of one connection-level subscription object, mutated by two different
actors (the `subscribe`/`unsubscribe` callers and the reader task) under two
different locks with no transactional boundary. J2 and J3 are both symptoms of
that. Collapse them into a single `PushSubscriptionState` behind one mutex with
`apply(&self, client) -> Result` as the *only* mutator: register-then-apply
becomes apply-then-commit by construction, and `pushState` has an obvious home.
Small rewrite, real payoff.

**The session-staleness signal is detected in the transport and consumed 300
seconds later in a discovery poller.** That is a long wire for a one-bit fact. The
detector should either drive the reopen directly (a watch channel the lifecycle
stream selects on, so the reopen is prompt rather than paced by an unrelated poll
interval) or `refresh_session` should be wired in for the cases where in-place
refresh is genuinely sufficient. Right now the crate has both mechanisms and uses
neither properly.

**On what to keep:** the error translation boundary (`sync/error.rs`), the cursor
envelope (`state.rs`), the foreign-account object-id codec, and
`reconcile_hydration` are genuinely good - closed accounting, right recovery
classes, well-pinned. Nothing there argues for restructuring.

## Out-of-scope observations

- `crates/types/`: `ScopeLifecycle::Renamed` carries two `MembershipScope`s and no
  name, which makes a rename event uninformative for any protocol whose container
  ids are stable across rename (JMAP, Graph, Gmail).
- `crates/types/`: `Batch::bytes_in` - if no consumer reads it, it should go; if
  one does, four protocol crates may be feeding it zeros (only JMAP verified).
