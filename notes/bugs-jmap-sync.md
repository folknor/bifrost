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

### 8. `scope_lifecycle` spurious rename and lost create

Emits a spurious `Renamed { old_name: "" }` for an updated mailbox absent from
the names map, and drops a `Created` event when the mailbox vanishes between
`Mailbox/changes` and the follow-up `Mailbox/get` while still committing the
new state (the create is lost forever to the lifecycle stream). Both benign
under the current engine (account-wide cursor shapes create no per-folder
cursor), but worth knowing they're load-bearing on that engine policy.

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

### 11. Mail search never covers shares (now documented; product gap stays open)

`search`/`search_messages` run only against the primary account; foreign
accounts, which sync and hydrate fully, are invisible to search. The doc gap
is closed (`reference/jmap.md` Known limitations now states it); whether
shared mail SHOULD be searchable is a product decision for the owner.

### 12. `filters_list` downloads script blobs serially

N+2 round trips for N scripts; the only other concurrency-capable spot in the
crate (foreign probes) bounds by `maxConcurrentRequests`, and the same
treatment would apply here.

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
`reference/jmap.md` claim for claim. `contact_search`'s pref reading (`pref ==
1` only) versus RFC 9553 pref semantics (1-100 ranking) is the only spec nit
there.
