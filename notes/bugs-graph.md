# bifrost-graph bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/graph/`. Findings are unverified work
material. Line numbers are as of the hunt and will drift.

Fixed 2026-08-18 and removed from this document: the calendar webhook
subscriptions colliding on `/me/events`, `push_stream` swallowing broadcast
`Lagged`, the infallible `expirationDateTime` parser (and the hand-rolled
civil-date arithmetic beside it), and the empty flag PATCH reported as
`Applied`, `close()` stranding server-side subscriptions in both push modes,
the `decode_cursor` progress asymmetry, the missing scope attribution on a
terminal renewal failure, and the `with_push_endpoint` constructor whose
`clientState` no receiver could validate. `reference/graph.md` now states each
new rule.

Fixed 2026-08-22 and removed from this document: EWS streaming delivering in
thirty-minute batches (there is now a chunked response seam and an incremental
frame decoder), the frozen calendarView window (cursor envelope v3 records the
horizon and reseeds ahead of it), the unrejected contradictory `FlagOp::Patch`,
the EWS worker that parked forever after its last handle went away, and
`inventory_stream` never checkpointing mid-walk (envelope v4 plus the
`Account::inventory_resume_stream` hook). Three defects that pass introduced
were caught in review and fixed in the same commit: the EWS worker retiring
without clearing its slot (now one shared `worker_slot` discipline across both
push modes), the frame decoder assuming the literal `m:` namespace prefix, and
a classified SOAP response error inside the stream being downgraded to a
malformed-XML reconnect loop. A fourth was found while auditing the consumers:
the reopen path `run_establish` put a stored mid-inventory cursor straight into
the registry, where `changes_stream` would have walked an empty delta link.

## Smaller observations

- **Move sends `If-Match` on `POST /messages/{id}/move`** (`mutate.rs`). Graph likely does not honor
  a precondition on the move action; if so, the `Move` etag preflight (`refresh_missing_etags`, one
  GET per uncached id) is buying nothing while `mutation.concurrency: StateBased` implies it is.
  Worth verifying against the live service; hunter was not certain either way.
- **`translate_ews_scopes` fails the whole subscription on one refused id** (`push.rs`).
  `reconcile_translated_ews_scopes` collects into `Result`, so a single stale folder id kills
  `push_subscribe` for every other folder. Everywhere else in this crate a per-item failure on a
  multi-item surface is filed per item and the rest proceeds, but here `push_subscribe` answers per
  request, so the design is at least self-consistent. Still, the practical effect is that one
  deleted folder in the engine's scope list disables push entirely.
