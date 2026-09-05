# Bug hunt: bifrost-jmap sync (Account impl)

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/jmap/src/sync/` - cursor envelope, inventory/changes/hydration, push,
mutation pipeline, recovery taxonomy.

## Confident defects

(Finding 1 - `calendar_ops.rs` failing an entire page on one unrepresentable
event and never reconciling submitted ids - is fixed: `get_events` now routes
its answer through `reconcile_events`, which mirrors `reconcile_cards`. An id
answered in neither `list` nor `notFound`, and an event `event_from_jmap`
refuses, both ride `Page::failed_ids` while the rest of the page returns. The
single-event `get` door keeps returning the conversion error itself, since
there is no neighbour there to protect. Pinned by
`unconvertible_and_unanswered_event_ids_ride_failed_ids` and
`a_not_found_event_id_is_reported_once`, the first ablated against a
drop-the-unconvertible-id variant and confirmed failing.)

(Finding 10's calendar half is fixed: `search` suppresses the server `total`
when `EventSearchRequest.calendar_id` is set, because that filter is applied
client-side and the server counted the unfiltered set. The cursor stays live -
it addresses the server-side result set, so an empty page with more behind it
is correct. Pinned by `a_calendar_filtered_search_reports_no_server_total`
with revert-and-confirm.)

(Finding 2's changes-loop half is fixed: `email_changes` / `mailbox_changes`
now terminate as `Protocol(ContractViolation)` when `hasMoreChanges: true`
arrives with an unmoved `newState`, before that page's batch. Pinned by
`a_stuck_changes_state_terminates_instead_of_looping`, whose ablation run
confirmed the unguarded loop spins forever. Both remaining halves are now
fixed too: the change loops keep a per-walk set of served states and
terminate as `ContractViolation` when one repeats, which is what catches an
oscillating state pair (every single step "moves"); and the inventory walk
refuses a page that re-serves the previous anchor id, impossible under
`anchorOffset: 1` with an unmoved `queryState`, terminating without a `Done`
so the engine restarts the scope. Pinned by
`an_oscillating_changes_state_terminates_instead_of_looping` and
`a_re_served_inventory_anchor_terminates_instead_of_looping`; both ablations
hung the test binary until brokkr's 20s per-test timeout killed it, which is
exactly the unbounded loop they describe.)

## Suspected / lower confidence

(Finding 6 - the wrong error KIND for a zero-mappable-scope
`push_subscribe` - is fixed: the all-rejected case now returns
`Request(Malformed)` via `error::no_mappable_push_scopes`, matching
`cross_account_destination`'s precedent for "the caller asked for something
this endpoint cannot express", instead of `Unsupported(PushSubscribe)`, which
claimed the account has no push at all. The `Err`-means-nothing-subscribed
contract is unchanged. Pinned by
`zero_mappable_scopes_is_a_malformed_request_not_absent_push` with
revert-and-confirm.)

(Finding 7 - a `contact_update` book move silently degrading into an ADD when
the read did not materialize the card - is fixed: the update now fails as a
retryable `Protocol(PartialResponse)` (`get_id_unanswered`) instead of
inferring "no old book" from a failed read. Pinned by
`a_book_move_fails_when_the_read_did_not_materialize_the_card`, which also
asserts no `ContactCard/set` reaches the wire; ablated against the previous
`and_then` and confirmed failing.)

(Finding 8 - `scope_lifecycle`'s spurious rename and lost create - is fixed:
both halves now key on what the CONSUMER has been told rather than on which
change collection the server used. An id the names map has never seen emits
`Created`, not `Renamed { old_name: "" }` - `updated` is not evidence of a
prior announcement, since the poller's window opens at the state seeded at
`open`. And an id named in `created`/`updated` that the follow-up
`Mailbox/get` never answers was destroyed in between: the state commit
cannot be withheld for it (nothing will ever mention that id again, so
there is no replay), so the create is surfaced together with the deletion
that overtook it, `Created` then `Deleted`, which leaves the engine with a
scope it established and then tore down instead of a delete for a scope it
never had. Ids the same response already reports as `destroyed` are left to
that loop. Pinned by `an_updated_mailbox_with_no_known_name_is_a_discovery`
and `a_create_that_vanished_before_the_read_is_created_then_deleted`, both
revert-and-confirmed - the second's ablation hangs on the 300s poll pause
until brokkr's per-test timeout kills it, which is the "the event never
arrives" it describes.)

(Finding 9 - `move_thread` / `delete_thread` re-resolving the thread between
the two legs - is fixed: both doors resolve the thread once and run both legs
over that id set via `patch_mailbox_membership_of`, with the cross-account
container check hoisted ahead of the resolve for both containers. The two-leg
non-atomicity itself is unchanged, as documented. Pinned by
`a_thread_move_resolves_the_thread_once_for_both_legs`, whose transport grows
the thread between resolves; ablated by restoring the second resolve and
confirmed failing.)

(Finding 10's `pim::search` half is fixed too: the thread projection now
reconciles every submitted email id through `reconcile_search_threads`, so a
declared `notFound`, an unanswered id, and an email returned without the
mandatory `threadId` all reach `Page::failed_ids`; thread ids are also
deduplicated. Pinned by `search_emails_without_a_thread_ride_failed_ids` with
revert-and-confirm.)

### 11. Mail search never covers shares (CLOSED)

Ruled on as todo ruling 4 and fixed. The defect half - a `SearchFilter::In`
naming a shared container sent the owner-qualified id as `inMailbox` to the
PRIMARY account, matching nothing, so the consumer got an empty page and no
error - is gone: `route_search` decides the owning account from the filter's
`In` containers, `Email/query` and the follow-up `Email/get` run against that
share with the native mailbox id, and the returned message and thread ids are
re-qualified into the foreign object namespace. An `In` naming an unreachable
share and a filter naming two different owners are both `Request(Malformed)`
before the wire, and the page cursor now carries the account it was minted
against so a page 2 cannot cross accounts. The implicit cross-account union
was explicitly not approved and is not implemented. Pinned by the six
`search_handles` transport tests in `sync/pim.rs`, each confirmed to bite.

### 11b. Search paging read fullness and pinned no `queryState` (CLOSED)

Surfaced while auditing 11's cursor. `search_email_ids` derived `next_cursor`
from "the page came back full", so a server answering a non-final page with
fewer ids than `limit` (which RFC 8620 permits) ended the walk in Done-shaped
silence with most hits unreported; and nothing pinned `queryState`, so page 2
taken under a moved order silently duplicated one hit and dropped another.
Both closed: the echoed `position` plus `total` decide whether more remains
(fullness survives only as the fallback for a server omitting `total`), and
the cursor payload is now versioned `2:<position>:<queryState>`,
owner-qualification intact. A moved state is `ConcurrencyConflict` ->
`Retry(AfterStateRefresh)`; a v1 bare-integer cursor is refused
`SchemaIncompatible` rather than resumed unpinned. Pinned by the
`PagingTransport` tests plus the codec round-trip and refusal tests in
`sync/pim.rs`, confirmed to bite by ablating each half.

(Finding 12 - `filters_list` downloading script blobs serially, N+2 round
trips for N scripts - is fixed: the downloads run concurrently through the
SAME clamp the open-time foreign probes use, which was renamed
`foreign_probe_concurrency` -> `factory::api_request_concurrency` and made
`pub(super)` rather than duplicated, so the crate keeps one answer to how
wide a fan-out may go. It is `buffered`, not `buffer_unordered`:
submission-ordered yielding is what preserves the per-script error
accounting exactly, since the serial loop's `?` reported the first failing
script in list order. Pinned by
`script_bodies_download_concurrently_within_the_advertised_limit` (six
scripts, an advertised limit of 4, peak in-flight asserted at exactly 4;
ablated to `buffered(1)` and confirmed failing at peak 1) and
`the_first_failing_script_in_order_is_the_reported_error` (a fast transport
failure behind a slower undecodable body; ablated to `buffer_unordered` and
confirmed reporting the wrong one).

## Lateral findings from the 2026-09-05 fix pass

Three defects surfaced while fixing findings 8 and 12, all in code the
findings walked past rather than in the findings themselves.

(Lateral 1 - `discover.rs::scope_lifecycle` paginating `Mailbox/changes`
with no forward-progress guard of its own - is fixed: the two guards
finding 2 put on the change walks are now ONE mechanism,
`changes::ChangeWalkGuard` / `WalkFault`, and the lifecycle poller shares
it. It matters more here than there: the engine drives this stream for
the life of the account, so the unguarded spin was permanent rather than
one dead walk. The guard covers the pagination burst and is dropped at
every poll pause, since a state recurring across two polls five minutes
apart is not a spin; on a fault the stream yields
`Terminated(Protocol(ContractViolation))` and ends, which is final for
the account's lifecycle channel and is the point - a non-conformant
provider is reported rather than polled forever. Pinned by
`a_stuck_lifecycle_state_terminates_instead_of_looping` and
`an_oscillating_lifecycle_state_terminates_instead_of_looping`, both
revert-and-confirmed. Note the ablation shape: asserting only the error
KIND does NOT bite, because the scripted transport's exhaustion reply
also classifies `Protocol(ContractViolation)` - both tests assert the
recorded REQUEST COUNT, which is what actually distinguishes a guarded
walk from a spinning one.)

(Lateral 2 - the lifecycle poller's follow-up `Mailbox/get` ignoring
`notFound` and never capping its id list - is fixed: `fetch_mailboxes`
batches at `maxObjectsInGet` (it was bounded only incidentally, by the
`maxChanges` fed from the same limit) and returns `notFound` explicitly.
The two lanes take one path, consistent with the Created-then-Deleted
rule finding 8 introduced: reconciliation drives from the SUBMITTED ids,
so a `notFound` id and a silently omitted one both land in the vanished
loop, which is right because RFC 8620 s5.1 gives them the same meaning
here. Pinned by `the_lifecycle_mailbox_read_batches_at_max_objects_in_get`
- five ids under a limit of two are three gets - ablated back to a single
unbounded call and confirmed failing; the `notFound` lane keeps its
existing pin, `a_create_that_vanished_before_the_read_is_created_then_deleted`.)

(Lateral 3 - `filters_list` hydrating every `SieveScript/query` id in one
unbatched `SieveScript/get` and never reconciling the answer - is fixed:
it batches at `maxObjectsInGet` via the new
`factory::max_objects_in_get` (the reader for doors holding a bare
`Account` rather than open-time `CoreLimits`), and reconciles each answer
against the ids that batch submitted. A script the server omitted used to
vanish from the returned `Vec`, which the consumer reads as "that filter
does not exist" - a deletion nothing claimed, and one an absent
`notFound` cannot disprove. The door has no per-item lane, so it is a
page-level failure: `error::get_id_unresolved_after_query` gives both an
unanswered id and a declared `notFound` the retryable
`Protocol(PartialResponse)` + `Attempt(Acknowledged)` that
`get_id_unanswered` and `contact_update`'s failed read already use,
rather than the terminal `get_id_not_found` lane, because a script the
query named and the get disclaimed is a delete that raced the two calls
and the retry's fresh query will not name it again. Pinned by
`the_script_hydration_batches_at_max_objects_in_get` and
`a_script_the_get_never_answers_fails_the_list` (both lanes, table-driven),
each revert-and-confirmed.)

## Checked and found sound

The load-bearing machinery holds up well against its documentation: the cursor
envelope (v1/v2 refusal split, mis-keyed-row vs schema-drift classification),
state-cache CAS discipline, foreign object/folder namespace qualification
(inventory, changes, hydration, blob, thread doors all agree,
unregistered-foreign ids stay literal everywhere), `reconcile_hydration`'s
closed per-item accounting, the mutation pipeline's per-owner batching and
starvation bound, the push reader's bounded setup awaits / keepalive /
backoff-reset-on-traffic logic, `close()` teardown bounds, the open-time
foreign probe concurrency clamp, and the error-translation table all match
`reference/jmap.md` claim for claim.

(The one spec nit found here - the contact projections reading `pref == 1`
only, versus RFC 9553's 1-100 ranking in which lower is more preferred - is
fixed: `pref_rank` accepts a rank only inside 1-100, so `pref: 0` cannot
outrank every legal value, and `apply_preferred` marks the single lowest
rank present, ties by position, none when no entry carries one. An absent
`pref` stays least preferred. Pinned by
`the_lowest_pref_rank_is_the_primary_entry` - a best rank of 10, a tie at 5,
and a `pref: 0` losing to a `pref: 100` - and
`an_unranked_contact_has_no_primary_entry`; the first was ablated back to
the `pref == 1` reading and confirmed failing.)
