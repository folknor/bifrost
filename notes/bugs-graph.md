# bifrost-graph: hunt findings

Scope: `crates/graph/` - Microsoft Graph delta-token sync, webhook push with
renewal health worker plus EWS streaming fallback, cursor envelope and validation,
`If-Match` etag mutations, error mapping, public-folder path.

Hunter note: read `reference/graph.md` in full plus `paging.rs`, `inventory.rs`,
`changes.rs`, `mutate.rs`, `webhooks.rs`, `push.rs`, `ews_stream.rs`, `contacts.rs`,
`calendar.rs`, `pim.rs` (write paths), and `public_folder.rs`. Each finding
verified by reading the code, not inferred from the reference.

## The cross-scope claim this hunter was asked to test: refuted as stated, real defect underneath

The types hunter reported that `Fingerprint::flags_hash` is computed two different
ways inside this crate, so two producers emit different fingerprints for identical
state.

`flags_hash` really is computed two ways - an FNV-1a over sorted
`isread=/flag=/category=` tokens in `inventory.rs:550`, and a bare
`u64::from(item.is_read)` (so only ever 0 or 1) in `public_folder.rs:517`. But the
two producers **never describe the same object**. Public-folder items are minted
with RS-separated `encode_public_item_id("<folderId>\u{1e}<itemId>")` under
`CursorScope::Folder`, and Graph REST items with `encode_message_id` under
`CursorScope::FolderType`. The id namespaces are disjoint by construction, so no
consumer can ever compare a hash from one producer against the other. "Two
producers for the same account emit different fingerprints for identical state"
does not happen.

What *is* wrong is narrower and worse than a hash mismatch (**high confidence**):
the public-folder fingerprint tracks **only read state**, and the public-folder
change stream cannot detect a read-state change at all. The incremental poll is a
`DateTimeReceived >= watermark` restriction, and `public_folder.rs`'s own module
docs concede that `DateTimeReceived` is not a change watermark - an item edited in
place sits below the watermark forever. The throttled full scan is IdOnly and only
diffs deletions. So flag/category/read changes in a public folder are
**permanently invisible**, and the one bit the fingerprint does carry is the one
nothing will ever re-deliver. The fix is to make the poll's item shape carry
`change_key` into an `Updated` emission (the change key *is* already in the
fingerprint's `server_version`) and to hash the same token set `inventory.rs` does,
via one shared function.

## 1. The two delta walks - the crate's primary sync loops - have no `nextLink` bound at all

**High confidence.** `paging.rs` exists precisely for this, and `reference/graph.md`
describes it as "the shared bound on **every** `@odata.nextLink` traversal". It is
not. `PageWalk` appears at six sites (`api.rs` x3, `contacts.rs:37`,
`calendar.rs:51`, `pim.rs:1368`). It appears at **neither**
`inventory.rs::inventory_stream_from` (loop at line 116) nor
`changes.rs::changes_stream` (loop at line 107). Those are the highest-volume,
longest-lived, most adversarially-exposed traversals in the crate - a server
echoing a `nextLink` spins them forever, and each iteration also grows
`obligations` unbounded and takes the `etag_index` write lock. The bounded walks
are the incidental ones; the unbounded walks are the sync core. Both loops need
`PageWalk::enter(&current_url)` before each `fetch_delta_page`, with the refusal
projected into `Terminated`/`InventoryEvent::Terminated` like every other fetch
failure.

## 2. Three copies of one paging loop, all of which silently discard matched results

**High confidence.** `contacts.rs::contact_search` (199-218),
`contacts.rs::directory_search` (268-282), and `calendar.rs::event_search`
(306-325) are the same eleven lines three times. All three do:

```rust
if items.len() >= limit {
    items.truncate(limit);
    next_cursor = page.next_link.map(String::into_bytes);
    break;
}
```

The truncated overflow came from **the current page**, but `next_cursor` resumes at
the **next** page. Every match beyond `limit` on the final page read is dropped and
never returned to the caller on any subsequent call. A client paging a 250-limit
contact search through a folder loses results at every page boundary where the page
over-delivers. Correct behavior is to carry the overflow into the cursor (or cap
`$top`/stop extending once `limit` is reached and resume from the same page). None
of the three has a `PageWalk` either, so they are also finding 1's shape - and
`contact_search`/`directory_search` filter client-side, so they walk the whole
corpus looking for matches, which is exactly when an endless-link server bites.

The structural answer: one `paged_walk(client, first_url, limit, project_fn)` helper
carrying the guard, the limit, and the overflow. Three hand-written copies is how
the same bug got written three times.

## 3. Bulk mutations send an etag they have just invalidated themselves - a 412 livelock

**High confidence.** `mutate.rs::submit_batch` reads `If-Match` from
`account.etag_index`. `refresh_missing_etags` (348) refreshes only ids **absent**
from the cache. On a successful 2xx, `mutation_item_outcome`
(`graph_error.rs:1227`) discards the response entirely - no header, no body - and
`submit_batch` evicts the key **only for `MutationKind::Destroy`** (228-235). So
after any successful `bulk_set_flags` or `bulk_move`, the cache retains the
**pre-mutation** `changeKey`.

Consequence: a second bulk flag write on the same message before the next delta page
re-caches it sends the stale etag, gets 412, classifies `ConcurrencyConflict` ->
`Retry::AfterStateRefresh` - and the retry re-reads the same stale cached etag,
because the refresh path skips ids that are present. It 412s again, indefinitely.
`bulk_move` is worse: Graph's `/move` mints a **new** message id, so the old key is
not merely stale, it names a message that no longer exists.

The asymmetry is the tell: `pim.rs`'s single-message twins of these exact operations
(`set_is_read`, `set_importance`) do a `fetch_message_value` -> `cache_etag_for` on
every write, so they are immune. The bulk path is the odd one out. Fix: on a 2xx
non-destroy subresponse, take the new `ETag`/`@odata.etag` from the subresponse and
overwrite the entry (Graph returns it), and evict on 412 so the retry actually
refreshes. `mutation_item_outcome` already receives `headers` and `body` and throws
both away on the success arm.

## 4. Webhook renewal records an expiry the server never granted - coverage can lapse silently

**High confidence.** `webhooks.rs::renew_subscription` (64-84) PATCHes, ignores the
response body, and returns its own locally-computed `new_expiry` string.
`push.rs:521` writes that into `state.expires_at`. Graph clamps
`expirationDateTime` server-side and returns the value it actually granted.
Whenever the grant is shorter than the request, local state believes the
subscription lives longer than it does, `is_expiring_soon` answers false through the
real expiry, and the subscription dies with the renewal worker seeing nothing due.
That is precisely a renewal that lapses.

`create_subscription` gets this right - it deserializes `SubscriptionResponse` and
uses `response.expiration_date_time`. `renew_subscription` should do the same:
`client.patch` into a `SubscriptionResponse` and return the server's value.
Low-cost fix, and the sibling already shows the shape.

## 5. `close()` strands webhook subscriptions after one failed DELETE

**High confidence.** `unsubscribe_graph` (359-396) uses `?` on
`delete_subscription`, so the first failure abandons every remaining `server_id` in
the snapshot. Under `push_unsubscribe` that is defensible - the doc comment argues
the siblings stay reachable for a retry. Under `retire_all_graph_subscriptions`
(411) it is not: `close()` is the last thing that runs, nothing retries, and the
reference's own stated goal ("without the walk, each reopen stranded one live
webhook subscription per resource still POSTing to the consumer's receiver") is
defeated by one 500 on the first DELETE. Every later subscription in that group
keeps delivering to the consumer's receiver for up to 24h. The close path should be
best-effort per subscription - log and continue - matching how the enclosing loop
already treats a failed handle.

## 6. The EWS streaming worker hot-spins on a flapping connection

**Medium-high confidence.** `ews_stream.rs` run loop, 136-186. The `Err` arm of
`subscribe` sleeps a fixed 5s - no backoff, no cap - so a persistently 503-ing EWS
endpoint is hammered every five seconds for the account's whole lifetime. Worse,
`StreamLoopExit::Disconnected` (141-144) sets a flag, releases the subscription, and
falls straight back to `subscribe` with **no delay whatsoever**. A
`GetStreamingEvents` that dies mid-body immediately after a successful Subscribe - a
broken proxy, an idle-timeout appliance - produces an unthrottled
Subscribe/Unsubscribe/Subscribe loop against Exchange, each iteration consuming and
returning a per-mailbox streaming subscription slot. Compare the webhook worker,
which is disciplined by a 10-minute tick. This wants one exponential backoff with
jitter shared by both failure exits, reset on a successful long-poll read.

## Structural story

Two shapes are actively working against this crate.

**The guard is a library nobody is obliged to call.** `PageWalk` is correct and
well-argued, and the two loops that most need it do not use it because using it is
opt-in. Same for the paging loop in finding 2 and the etag lifecycle in finding 3:
the invariant lives in prose and in whichever call sites remembered. The fix is not
more discipline - it is making the walk itself the only way to page. One
`GraphPageWalk` stream type that owns the URL, the guard, the fetch, and the
`nextLink`/`deltaLink` termination, which `inventory_stream`, `changes_stream`, all
three searches, and every `fetch_paged_values` consume. Pre-1.0, this is a
straightforward internal rewrite and it deletes more code than it adds.

**The etag cache is a write-through cache with no write path.** Every producer
(inventory, changes, hydration, pim reads) fills it; only Destroy invalidates it. A
cache whose entries are invalidated by the very operations that consume them, but
which observes only one of three such operations, is going to be wrong. It should be
a small owned type with `record_read(id, etag)` / `record_write(id,
Option<new_etag>)` / `evict(id)`, and `mutation_item_outcome` should hand it the
subresponse rather than dropping it. That is also the natural home for the 412
eviction.

**Sizes worth flagging as a smell** (low confidence as defects, high as maintenance
risk): `pim.rs` is 4,795 lines and `push.rs` 2,600. `pim.rs` in particular is
holding message writes, drafts, send-as, search + its cursor codec, folder CRUD,
identities, vacation, and typed hydration in one file; the search cursor logic alone
(1,984-2,150) is a self-contained subsystem with its own versioned wire format.

## Out of scope, flagged

- `bifrost-google`'s `calendars_list` is cited in `paging.rs` as having learned the
  same lesson independently. Worth checking whether Google's *delta/history* walks
  got the guard, or only its list walks - the miss here was exactly that split.
- `reference/graph.md` states `PageWalk` bounds "every `@odata.nextLink` traversal"
  and enumerates six loops. That sentence is false against the code (findings 1 and
  2), and `reference/` is the citable-as-truth folder. It needs correcting whether or
  not the code is fixed.
