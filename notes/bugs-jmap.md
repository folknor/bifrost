# bifrost-jmap: hunt findings

Scope: `crates/jmap/` - dispatch, transport, per-RFC module pattern, capability
negotiation, error model, and the `Account` impl under `crates/jmap/src/sync/`.

Hunter note: baseline was green (522 tests pass, clippy/gremlins clean), so
everything below is a live gap the suite does not cover.

## J1. The inventory walk pages `Email/query` by integer `position` over an unstable order, and can silently skip messages

**HIGH confidence, data loss.** `sync/inventory.rs::email_inventory_loop` walks
the whole account with `Email/query` `sort: receivedAt desc`, `position: N`,
`limit: L`, advancing `position` by ids consumed. There is no anchor, no
`before:` bound, and no tiebreak sort key.

`reference/jmap.md` makes exactly this argument for the search path - *"RFC 8621
gives an unsorted `Email/query` a server-defined order with no cross-call
stability guarantee, and paging an unstable order by integer position duplicates
and skips results"* - and then the inventory walk does the unstable thing anyway.
Pinning the sort key is not sufficient: `receivedAt` is not a total order (bulk
imported mail routinely shares a second, and RFC 8621 defines no tiebreak), and
the result set is live.

Concretely: any message **deleted or expunged during a full backfill** shifts
every later message down one index, so the message that moves from position P to
P-1 is never returned by the walk. It is also not in the change stream -
`establish_initial_cursor` seeds from the **open-time** state, and the skipped
message was created *before* that state, so `Email/changes` will never mention
it. It is permanently absent until an unrelated full re-inventory. Ties resolved
differently between two calls produce the same skip with no deletion at all.

The loop already reasons carefully about the *query->get* deletion race (the
`Some(0)` overshoot arm, which is good and load-bearing) - but not about the
*query-position* race one layer up. Same defect shape the standing lessons
describe: the fix closed the hole one layer down.

The remedy is already in the codebase and unused: `core/query.rs` implements
`anchor` / `anchorOffset`. Page by anchoring on the last id of the previous page
with `anchorOffset: 1`, or add a server-side `before: <open time>` filter so the
result set is frozen for the duration of the walk (that also removes the
duplicate-on-new-arrival behavior). This affects the primary Email walk, the
foreign walk, and the bounded `Page` partition equally.

## J2. RFC 8887 `pushState` is never parsed and never replayed, so state changes during a WebSocket outage are not pushed

**HIGH confidence.** `PushObject::StateChange` in `lib.rs` decodes only `changed`;
the `pushState` property RFC 8887 adds to the WebSocket StateChange is dropped on
the floor. `client_ws.rs` *does* have a `push_state` field on the enable frame,
and `push.rs::apply_push_set` hardcodes `None::<String>`:

```rust
client.enable_push_ws(Some(values), None::<String>).await
```

The entire point of `pushState` is that on re-enable the server replays the state
changes that happened while the connection was down. Because it is never captured
or sent, every reconnect starts cold: changes across the outage window are
invisible to push and surface only when the engine's own poll interval next
fires. On a flapping link, push degrades to polling without any signal that it
has. The fix is small and structural: thread the last-seen `pushState` through
`WsState` alongside `enabled`, and hand it to `set_push_data_types`.

## J3. A failed `push_subscribe` permanently widens the subscription union with a handle the caller can never remove

**HIGH confidence, leak.** `sync/push.rs::subscribe`:

```rust
let union = { let mut guard = subscriptions.lock().await;
              guard.insert(handle.clone(), data_types);      // committed first
              union_data_types(&guard) };
set_enabled_data_types(&enabled, union.clone()).await;        // committed second
apply_push_set(&client, &union).await?;                       // may fail here
Ok(handle)
```

On the `?`, the handle stays in `subscriptions` and `enabled` retains the wider
union - but the handle was minted *inside* `push_subscribe`
(`next_subscription_handle()`) and is never returned to the caller on the error
path. There is no way to unsubscribe it. For the life of the account,
`reenable_current_push_set` re-applies data types nobody is subscribed to on every
reconnect, and a later `unsubscribe` of a real handle will not narrow the set.
Commit the registry and `enabled` only after `apply_push_set` succeeds, or roll
both back on failure.

## J4. `push_subscribe` reports scopes as subscribed that were silently dropped

**HIGH confidence.** `account.rs` wraps the result as
`PushSubscription::all_succeeded(handle, &scopes)` over the *caller's* scope list,
but `push::subscribe` filters through `data_type_for_scope`, which returns `None`
for `CursorScope::Query(_)` and anything else unmapped. Subscribing to
`[Type(Email), Query(q)]` succeeds and claims both are covered; `q` gets no push
forever. Either report per-scope success honestly (the shared type clearly
supports it - `all_succeeded` implies a partial constructor exists) or reject
unmappable scopes.

## J5. `refresh_session` is dead code, and a `sessionState` change leaves the client using stale derived URLs for up to 300s

**MEDIUM-HIGH confidence.** `client.rs::send_request` latches `session_updated =
false` when the echoed `sessionState` diverges. The only consumer is the top of
`discover::scope_lifecycle`'s poll loop, which fires **at most once per 300s
`POLL_INTERVAL`**. In that window every request continues against the stale
`apiUrl` / `uploadUrl` / `downloadUrl` / default account id. RFC 8620 section 2
lets any of those change, which is precisely why `refresh_session` was written to
republish them atomically - and nothing calls it. Two separable problems: (a) the
detection-to-action latency is a whole poll interval on a signal that is available
synchronously per response, and (b) the in-place refresh path exists, is
documented as correct, and is unreachable. At minimum the account-level reopen
should be triggered from the detection point rather than polled for.

## J6. `ScopeLifecycle::Renamed { old, new }` always carries two identical scopes

**MEDIUM confidence, boundary issue.** `discover.rs` emits `Renamed { old:
scope.clone(), new: scope }` where `scope = MembershipScope::Mailbox(id)`. A JMAP
mailbox id does not change on rename, so both halves are byte-identical and the
consumer learns *that* something was renamed but never *what to*. The name is
fetched (`fetch_mailboxes`, compared against `mailbox_names`) and then discarded.
Either the shared `ScopeLifecycle` type needs a name channel - a `crates/types/`
question - or this event should not be emitted as `Renamed` at all, because as it
stands it is indistinguishable from a no-op.

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
