# bugs-google-net - bifrost-google + bifrost-net sweep

Bug hunt, fixes, and landed tests over `crates/google/src/**`,
`crates/net/src/**`, and `crates/net/tests/**`. This document contains
only findings that remain open. Resolved findings are removed rather
than retained as historical discrepancies. Tests that still pin
behavior believed to be wrong say so in their own doc comment
(`DOCUMENTS A BUG, NOT AN ENDORSEMENT`).

Initial coverage survey result: the handed-down read was accurate.
`request.rs`, `bandwidth.rs`, `rate.rs`, `retry.rs`, `config.rs` and
`url.rs` had no in-file tests (`rate.rs` had partial coverage from
`tests/rate_governor.rs`); `net.rs`, `redirect.rs`, `auth.rs` and
`account_error.rs` did. On the Google side `blobs.rs`, `changes.rs`,
`scopes.rs`, `inventory.rs`, `push.rs`, `api.rs` and `types.rs` had
none.

---

## Bugs

Ordered most-severe first.

### B7 - `max_hops: 255` is an infinite redirect chain

`crates/net/src/request.rs:368` and `562-567`

```rust
let mut redirect_hops: u8 = 0;
...
redirect_hops = redirect_hops.saturating_add(1);
if redirect_hops > policy.max_hops {
    return Err(Error::RedirectLoop { hops: redirect_hops });
}
```

`redirect_hops` and `max_hops` are both `u8`. With
`RedirectPolicy::with_hops(255)`, the counter saturates at 255 and
`255 > 255` is never true. The `attempt = 0` reset at `request.rs:608`
means the retry budget cannot terminate the walk either.

Path to failure: `NetConfig::new().with_redirect_policy(
RedirectPolicy::with_hops(255))` against a server serving an endless
`302 -> /next` chain. The request never returns and never errors; the
task hangs holding a connection.

Proposed fix: use `>=`, widen the counter to `u16`, or clamp `max_hops`
to 254 in `with_hops`. `>=` is the smallest change and is what the
error message already implies ("exceeded the configured maximum").

### B10 - `bulk_move` to a non-`Label` scope reports success-`Skipped` for every id

`crates/google/src/account/mutation.rs:283-289` with `203-205`

`move_patch` on a `MembershipScope::Folder` (or any non-label variant)
returns `LabelPatch { unsupported_flags: vec![...], .. }`, and
`apply_label_patch` maps non-empty `unsupported_flags` to
`skipped_outcomes(ids)` - the **success** lane
(`ItemOutcome::Succeeded(MutationSuccess::Skipped)`).

Path to failure: a consumer that composes a folder-shaped destination
(reasonable, since `MembershipScope` is shared across protocols and
IMAP/Graph use `Folder`) gets a clean success report for a move that
never happened.

Proposed fix: distinguish "the operation was a no-op for this id"
(`Skipped`, legitimate for an empty patch) from "the request was
malformed" (`ItemOutcome::Failed` with a `Request(InvalidArgument)`
error, or a stream-level `Terminated(Unsupported(BulkMove))`). The
existing `unsupported_flags` field conflates both.

### B11 - `content_range_matches` rejects a legal shortened closed range

`crates/net/src/net.rs:696-698`

```rust
match req_end_n {
    Some(end) => resp_end_n == end,
    ...
}
```

RFC 9110 section 14.1.2 lets a server answer a range whose last-pos
exceeds the resource with the smaller satisfiable range. A request of
`bytes=0-99` against a 50-byte resource legally returns
`206` + `Content-Range: bytes 0-49/50`.

Path to failure: `download_stream(url, Some(ByteRange { start: 0,
length: Some(100) }))` on a 50-byte Gmail/Drive blob returns
`Error::RangeNotHonored { kind: ContentRangeMismatch }` instead of the
50 correct bytes. Any caller that walks a blob in fixed-size chunks
hits this on the final chunk unless it already knows the exact size.

Proposed fix: when the total is known, also accept
`resp_end_n == total - 1 && resp_end_n <= end` - i.e. the server
returned the whole tail from the requested start. The exact-match rule
stays for the case where the server returned a shorter window *and*
the resource is longer, which is the corruption the check exists to
catch.

Pinned by the new test
`net::tests::content_range_rejects_a_legally_shortened_closed_window`.

### B12 - Nothing in the workspace calls `Net::detach_account`; every `GmailClient` construction leaks a registration

`crates/google/src/client.rs:65-85, 104-120, 287-323` and
`crates/google/src/account/mod.rs:837-847`

`grep -rn detach_account crates/` outside `crates/net/src` returns
nothing. Consequences:

- `GmailClient::with_api_base_and_source` (reached by every
  `GoogleAccountFactory::from_*` constructor) calls
  `default_account_net` with `AccountId("gmail-direct")`, which
  `uniquify_account_id` rewrites to `gmail-direct-N` from a
  monotonically increasing static counter. That attaches a
  `BandwidthMeter` entry and bumps the governor's attach count for
  `www.googleapis.com` and `people.googleapis.com` on the process-wide
  `Net::shared_default()`. It is never detached. Building N factories
  leaks N meter entries and 2N attach counts.
- `AccountFactory::open` then calls `for_account`, attaching *again*
  under the real engine id, so the `gmail-direct-N` registration is
  pure waste from the moment it is created.
- `GoogleAccount::close()` cancels the shutdown token and aborts the
  renewer but never detaches, so a closed account's meter entry and
  host attach counts persist.

Because the counts never reach zero, `RateLimitGovernor::unregister`'s
bucket-reclamation branch and `Net::detach_account`'s whole symmetric
teardown are dead code in practice, despite `reference/net.md:37-42`
describing them as the live contract.

Proposed fix: have `GoogleAccount::close()` call
`net.detach_account(id)`, and avoid attaching a throwaway account in
the client constructor - build the `AccountNet` lazily in
`for_account`, or reuse a single shared placeholder id instead of a
fresh one per construction. (Same shape almost certainly applies to
bifrost-graph; out of my scope to confirm.)

---

## Hazards, smells, and untested behaviour that matters

### N1 - `RateLimit::quota_per_second` is an unvalidated `f64`

`crates/net/src/rate.rs:117-156` and `230-271`. Three distinct
failure modes, none of which produce an error:

- **`0.0`**: `wait_secs` computes to `f64::INFINITY`, clamps to the
  250 ms poll cap, and the bucket never refills. Once the initial burst
  is spent, `acquire` parks forever - no deadline, no error. Only an
  explicit `refund` can release it. The crate already treats
  `cost > burst` as a typed configuration bug
  (`Error::CostExceedsBurst`); a zero refill rate is the same class of
  mistake and gets a silent hang.
- **negative**: the refill line
  `(tokens + elapsed * refill_rate).min(burst)` *drains* the bucket,
  `deficit / refill_rate` goes negative, and `clamp(0.005, 0.25)`
  returns the 5 ms floor. The acquire loop becomes a 200 Hz spin
  instead of a park.
- **NaN**: `bucket.tokens` becomes NaN, `tokens >= cost_f` is false
  forever, `wait_secs` is NaN, `f64::clamp` propagates NaN, and
  **`Duration::from_secs_f64(NaN)` panics** (`rate.rs:262` - the std
  docs specify a panic on non-finite input). A NaN in a caller's
  config panics the acquire future rather than erroring.

Suggested: validate in `register` - reject non-finite and non-positive
rates with a `tracing::warn!` and either skip the registration (leaving
the host unmetered, matching the "unregistered hosts are unmetered"
rule) or store a documented floor.

Pinned by the new tests
`zero_quota_never_refills_and_parks_forever` and
`negative_quota_spins_at_the_minimum_poll_interval` in
`crates/net/tests/rate_governor.rs`. The NaN panic is deliberately not
tested, because the test would be the panic.

### N2 - `bifrost-net` has no transport seam, so the retry loop cannot be tested at all

This is the single biggest structural coverage gap in the crate.
`send_streaming_inner` (`request.rs:318-699`) reaches
`account.net().client()` and builds a `reqwest::RequestBuilder`
directly. There is no trait boundary to substitute. That leaves
untestable, in-process:

- the retry decision matrix (which statuses retry, which are terminal)
- the 401 forced-refresh path and its **separate** auth budget,
  including the `attempt.saturating_sub(1)` compensation
- `Retry-After` honouring and the `retry_after_history` accumulation,
  including the exhausted-attempt final parse
- the rate-limit refund-on-every-retried-failure rule (the fix that
  stopped a plain 503 burning three tokens)
- the whole redirect walk: method rewriting applied to a real request,
  `Authorization` stripping across a host boundary, hop counting,
  cost recomputation on a cross-host hop
- `RangeNotHonored` classification in `download_stream`
- `Error::AuthLost` final-response preservation

Every one of those is a documented behavioural contract in
`reference/net.md`, and none of them has a test. What I could land is
only the pure-function layer around the loop.

Suggested: introduce a crate-private dispatch trait, e.g.

```rust
pub(crate) trait Dispatch: Send + Sync + 'static {
    fn send(&self, req: reqwest::Request) -> BoxFuture<'static, Result<reqwest::Response, reqwest::Error>>;
}
```

with the reqwest client as the production impl and a scripted
`Vec<CannedResponse>` double in tests. `reqwest::Response` is
constructible from `http::Response<Body>` via `From`, so a canned
status + headers + body can be built without a socket. That is the
same shape the JMAP agent proved out with its stub transport
(`crates/jmap/src/core/tests.rs`), and it would take this crate from
"pure helpers only" to full loop coverage.

### N3 - `acquire` and `ByteBucket::consume` mix `std::time::Instant` with `tokio::time::sleep`

`rate.rs:239-270` and `net.rs:809-828` compute refills from
`std::time::Instant::now()` but wait on `tokio::time::sleep`. Under
`tokio::time::pause()` the sleeps complete instantly in virtual time
while the real clock barely moves, so both loops turn into hot spins
that make no progress. That is exactly why
`crates/net/tests/bandwidth_window.rs` opens with a comment explaining
it cannot pin the window maths, and why every new "this acquire
blocks" test I wrote has to be wrapped in `tokio::time::timeout`.

Switching the refill clock to `tokio::time::Instant` is behaviourally
identical in production (it is the std clock when time is not paused)
and makes both throttles fully deterministic under test.

### N4 - Cancellation between `acquire` and the response leaks a rate-limit slot

`request.rs:377-450`. Every explicit failure arm refunds the debited
cost, but if the caller drops the `send()` future after `acquire`
succeeded and before a response arrives - which the engine does
routinely on scope teardown and reopen - the tokens are never
refunded. Natural refill recovers them eventually, so this is a
throughput dent rather than a deadlock, but on People's
`quota_per_second: 1.5` bucket (`client.rs:17`) a burst of cancelled
contact reads costs neighbours real seconds.

Suggested: an RAII guard holding `(host, cost)` that refunds on
`Drop` unless explicitly disarmed on the success path.

### N5 - Outbound HTTP metering silently drops bytes for an unregistered account, while `MeterSink` auto-registers

`request.rs:416-419` records outbound bytes through
`account.meter()`, which goes via `BandwidthMeter::account` ->
`lookup` (`bandwidth.rs:205-208`) - a pure lookup that yields
`counters: None` for an unregistered account, making every subsequent
`record_*` a no-op. The `MeterSink` impl on the same struct
(`bandwidth.rs:225-232`) takes `lookup_or_register` instead. Two entry
points into one meter with opposite behaviour on the same input.

`attach_account` always registers, so this is latent today. It stops
being latent the moment anything calls `detach_account` while a
request is in flight (B12 says nothing does yet, but the API exists and
`reference/net.md` documents it as the teardown path), or with
`retag`, which registers the new id and leaves the old one registered
too (`net.rs:539-561`).

Pinned by the new test
`bandwidth::tests::unregistered_account_meter_is_inert_but_meter_sink_auto_registers`.

### N6 - `encode_component` does not escape what its doc comment claims

`crates/net/src/url.rs:5-32`. The doc says it preserves RFC 3986
unreserved characters "and escap[es] everything else". The `COMPONENT`
set omits `<`, `>`, `\`, `^`, `` ` ``, `{`, `|`, `}` and `!`.

`\` is the load-bearing omission: the WHATWG URL parser that
`reqwest::Url` implements treats a backslash as a path separator for
`http`/`https` schemes, so a component "encoded" by this helper can
still split a path segment.

Separately - and not fixable inside the escape set, since `.` is
unreserved - a component that is exactly `..` passes through intact and
the URL parser resolves it as a parent-segment traversal. The new test
`url::tests::dot_dot_component_passes_through_and_is_resolved_by_the_parser`
demonstrates `https://h.example/a/b/` + `encode_component("..")`
resolving to `/a/`. Every dynamic path segment the Gmail and Graph
crates splice runs through this helper, so an unsanitised
provider-supplied id can retarget a request path.

Suggested: add the missing characters to `COMPONENT` (free, no
behaviour change for well-formed ids), and either reject or explicitly
escape a component that is `.` or `..` - or document loudly that the
caller owns that check.

### N7 - Gmail id splicing is inconsistent about encoding

`crates/google/src/api.rs`. `delete_filter` (line 74) and
`patch_send_as` (line 279) call `encode_component`. Every other path
splices raw: `label_id` (57, 61), `thread_id` (139, 150, 160),
`message_id` (164, 187, 202), `attachment_id` (202), `draft_id` (240,
254, 262). Gmail ids are hex-ish in practice so this is latent, but the
file disagrees with itself, and per N6 the helper would not fully
protect those sites even if they used it.

### N8 - `users.messages.list` never sets `includeSpamTrash`, so Spam and Trash are invisible to inventory and search

`crates/google/src/account/inventory.rs:245` and
`crates/google/src/api.rs:104-136`. Gmail's default is
`includeSpamTrash=false`.

Consequences: cold-start inventory never enumerates Spam or Trash
contents; `search` and `search_messages` never return them. Meanwhile
`changes_stream` reads `users.history.list`, which is **not** filtered,
so the engine can receive `ScopeChange` rows for `SPAM`/`TRASH`
messages it never inventoried and has no object record for.
`reference/google.md` does not mention the exclusion anywhere, and the
crate does model both as first-class containers
(`flags.rs:22-25` treats them as exclusive display containers, and
`containers_list` maps them to `FolderRole`).

Suggested: add `&includeSpamTrash=true` to the inventory list call at
minimum, and decide explicitly whether search should follow. Either way
the choice belongs in `reference/google.md`.

### N9 - `scope_lifecycle_stream`'s first poll is scheduling-order dependent

`crates/google/src/account/scopes.rs:112-156`. The first iteration
diffs `snapshot(&cache)` - which is `ScopeSnapshot::empty()` at open -
against the freshly fetched full label list. If the lifecycle stream
starts before anything else populated the cache, it announces
`ScopeLifecycle::Created` for *every* label in the account. If
`discover_memberships` or `containers_list` ran first, it announces
nothing. Same account, same server state, different event stream
depending on task scheduling.

Suggested: make the first poll explicitly a seeding pass that emits
nothing (or emits everything, deliberately), rather than leaving it to
whoever touched the cache first.

### N10 - `ScopeLifecycle::Renamed` carries the same scope on both sides

`crates/google/src/account/scopes.rs:223-227` builds
`Renamed { old: Label(id), new: Label(id) }` - Gmail's label id is
stable across a rename, so both halves are identical. A consumer
learns that *something* was renamed but not what, and must re-read
`containers_list` to find out. Either carry the display names or
document `Renamed` as a pure "re-read me" signal.

Pinned by the new test
`scopes::tests::rename_events_carry_the_same_scope_on_both_sides`.

### N11 - The mutation stream never emits `PageBoundary::Final`

`crates/google/src/account/mutation.rs:142-151` always uses
`PageBoundary::Page`, then a separate `SyncEvent::Done(None)`. Every
other stream in the crate (`inventory_stream`, `changes_stream`,
`open_blob`, `discover_memberships`, `discover_cursor_scopes`) marks
its last batch `Final`. A consumer that keys "batch complete" off
`Final` sees none for mutations.

### N12 - `OAuthRefresher` throws away a still-valid token when a proactive refresh fails

`crates/net/src/auth.rs:252-259` transitions `Fresh -> Refreshing`,
dropping the cached token before the network call. On failure
`drive_refresh` (`auth.rs:312-322`) leaves the state `Empty`.

A token 59 minutes into a 60-minute TTL is still usable for another
minute, but a transient network failure during the proactive refresh
discards it, and every request afterwards fails with `RefreshFailed`
until the network recovers. Keeping the old token inside `Refreshing`
and restoring it on a transient (non-`AuthLost`) failure would be
strictly better - the proactive window exists precisely because the
token is still valid.

### N13 - `push_unsubscribe` with an unknown handle can stop somebody else's watch

`crates/google/src/account/push.rs:92-96` and `192-200`.
`remove_handle` returns `handles.is_empty()`, which is true both for
"you removed the last handle" and for "the handle was never in the set
and the set was already empty". A caller replaying a stale handle after
everyone else unsubscribed - or passing a fabricated but
JSON-well-formed handle - reaches `stop_watch` and tears down the
account's watch.

Suggested: return an enum distinguishing "removed, set now empty",
"removed, others remain", and "not present", and treat the last as a
no-op.

### N14 - `apply_label` with the synthetic `archive` id performs a full relocation

`crates/google/src/account/pim.rs:646-654`. The shared convenience
dispatch (`crates/types/src/account.rs:1080-1082`) routes
`(ContainerKind::Label, ProtocolKind::Gmail)` to
`set_label_membership(target, label.id, value)`, which reaches
`add_container_patch`. If `label.id` is `archive`, that becomes
`move_container_patch` - stripping `INBOX`, `SPAM` and `TRASH`.

Correct per the documented archive rule, and `containers_list` does
present `archive` with `ContainerKind::Label`, so a consumer that
round-trips a container into a `Label` and calls `apply_label` gets a
relocation from a call that reads as additive. Worth one line in
`reference/google.md` under the conveniences section.

### N15 - Documented-dead error variants

`Error::NetSetup` (`crates/net/src/error.rs:248-258`) and
`MalformedRedirectKind::MissingLocation`
(`crates/net/src/error.rs:57-61`) are both retained "for
representability" and constructed by no current path
(`reference/net.md:437-445` says so explicitly). They cost every
downstream matcher an arm for a case that cannot occur. Either remove
them (both enums are `#[non_exhaustive]`, so downstream keeps
compiling) or note the deprecation in the variant doc.

### N16 - `bifrost-google` dev-dependencies lack `tokio/test-util`

`crates/google/Cargo.toml` dev-deps are `tokio = { features = ["macros",
"rt"] }`. No `test-util`, so `tokio::time::pause()` is unavailable in
this crate. That leaves the renewer loop, the 30-second lifecycle
poll, and the 300-second scope-cache staleness window untestable
in-crate - all three are wall-clock driven. Adding `test-util` (and
`rt-multi-thread` if a concurrency test is ever wanted) would unlock
them. I did not make the edit: `Cargo.toml` is outside my scope.

### N17 - Minor: per-attempt `AccountMeter` construction allocates

`request.rs:418` calls `account.meter()` inside the retry loop, and
`AccountMeter` clones the `AccountId` `String` per call
(`net.rs:418-424`). Three retries of a large upload cost three
allocations on the hot path. Caching the `AccountMeter` on
`AccountNetInner` at attach time would remove it. Trivial in
isolation; noted because it is on the per-request path.

---

## Tests landed

All tests are hermetic: pure functions, synthetic clocks, and JSON
fixtures deserialized through the crates' own DTOs. No sockets, no
listeners, no wall-clock sleeps outside `tokio::time`'s virtual clock.

### bifrost-net

- `src/request.rs` (new `mod tests`, 15 tests) - `backoff_for` window
  bounds, growth clamping, `u32::MAX` attempt saturation, all-zero and
  `Duration::MAX` policies, `max_backoff` below `initial_backoff`;
  `parse_retry_after` for delta-seconds, whitespace, negatives,
  garbage, past and future HTTP-dates, and the deliberate absence of
  capping; `recompute_cost_units` precedence; `host_from_url`;
  `strip_body_headers`; the header rejections the deferred-error arms
  depend on.
- `src/bandwidth.rs` (new `mod tests`, 11 tests) - `RateWindow`
  driven with synthetic `Instant`s: sub-second bucketing, second
  boundary rolls, sub-second remainder carry-forward, ten-second and
  hour-long gap wipes, warm-up divisor, ten-second divisor cap,
  saturating byte counts; meter registration asymmetry, idempotent
  register, `forget_account`, `MeterSinkHandle` routing and `Debug`.
- `src/net.rs` (extended existing `mod tests`, +12 tests) -
  `encode_range` inclusive-closed, open-ended, zero-length rejection,
  overflow rejection, largest representable window;
  `content_range_matches` exact match, the legally-shortened-window
  rejection (B11), shifted starts, seven malformed-syntax shapes,
  unknown-total handling, zero-total resource.
- `src/error.rs` (new `mod tests`, 6 tests) - `cap_status_body` under,
  at, and over the cap, one byte over, multi-byte character splitting,
  empty body; `Error::Status` `Display` not leaking the body.
- `src/url.rs` (new `mod tests`, 6 tests) - unreserved passthrough,
  structural delimiters, controls and non-ASCII, the currently
  unescaped characters (N6), the `..` traversal (N6),
  non-idempotence.
- `src/config.rs` (new `mod tests`, 4 tests) - production-safe
  defaults, default redirect policy, both builder helpers, the
  token-max-age margin.
- `src/retry.rs` (new `mod tests`, 4 tests) - default policy shape,
  429 as the only non-5xx member, `disabled()` semantics, the
  single-capping-knob invariant.
- `tests/rate_governor.rs` (+8 tests) - unregistered hosts unmetered,
  `cost == burst` boundary, zero-cost bypass, refund clamping at
  burst, attach-count refcounting and over-unregister, first-
  registration-wins, and the two configuration hazards from N1.

### bifrost-google

- `src/account/changes.rs` (new `mod tests`, 8 tests) - final-page-only
  durable checkpointing plus first coverage
  of `changes_from_history`: empty history, `messagesAdded` ordering
  and per-label scope rows, label-less adds, `messagesDeleted`,
  label add/remove rows, delta-vs-full-label-set, multi-record order
  preservation, all-four-buckets.
- `src/account/push.rs` (new `mod tests`, 9 tests) - first coverage of
  `parse_expiration` and `renewal_delay`, including the five-minute
  renewal floor; subscription-handle envelope round trip and
  rejection; watch-response decoding; `PubSubConfig` builder.
- `src/account/scopes.rs` (new `mod tests`, 11 tests) - first coverage
  of `diff_snapshots`: no-op, create/delete, user rename, system-label
  locale flip suppression, untyped label, colour-only edit,
  empty-old-snapshot; the same-scope rename shape (N10);
  never-populated caches being born stale, a successfully fetched
  empty label list being fresh; staleness window.
- `src/account/blobs.rs` (new `mod tests`, 7 tests) - first coverage
  of `blob_handles_for_message`: no payload, inline-only, attachment
  capabilities, recursive multipart walk, negative size; blob-id round
  trip, malformed rejection, JSON metacharacter survival.
- `src/account/inventory.rs` (new `mod tests`, 13 tests) - pre-page
  profile checkpoint anchoring plus first
  coverage of `inventory_entry_from_message`: id/thread/size/
  membership projection, `historyId` -> `ServerVersion`,
  case-insensitive threading headers, `References` splitting,
  absent headers, `flags_hash` stability, and why canonicalization
  requires a populated label vocabulary; `raw_bytes` decode / missing / undecodable;
  `non_negative_u64`; paging constants.
- `src/account/flags.rs` (+17 tests on the existing module) -
  user-label translation both directions, stable-id resolution across
  renames, empty vocabulary, exact `Set` handling for user labels,
  the `CATEGORY_*` round trip and its exemption from `Set`
  re-derivation, `Set` removal lists scaling with the user label
  vocabulary, unknown-flag poisoning, case-insensitive flag matching,
  contradictory patch, empty ops, order independence and dedup,
  folder-label dropout, unknown-id name fallback, the `UNREAD`
  case-sensitivity asymmetry, FNV boundary separation.
- `src/api.rs` (new `mod tests`, 1 test) - opaque Gmail page-token
  percent-encoding.

Tests whose doc comment marks them as documenting a defect rather than
endorsing it: `url::dot_dot_component_passes_through...`,
`url::characters_the_component_set_currently_leaves_unescaped`,
`net::content_range_rejects_a_legally_shortened_closed_window`,
`rate_governor::zero_quota_never_refills_and_parks_forever`,
`rate_governor::negative_quota_spins_at_the_minimum_poll_interval`,
`scopes::rename_events_carry_the_same_scope_on_both_sides`,
`flags::an_unknown_label_id_renders_its_id_in_the_name_slot`,
`flags::the_unread_projection_is_case_sensitive_unlike_the_move_rule`.

---

## Not reached

- **The `bifrost-net` retry / redirect / auth loop itself.** No
  transport seam exists (N2), so `send_streaming_inner` is untestable
  without a socket. This is by far the largest remaining gap and the
  reason the net half of this pass is all pure-function coverage.
  Landing the `Dispatch` trait is a prerequisite, and it is a source
  change rather than a test change, so it belongs in a fix slice.
- **`crates/google/src/account/pim.rs`** (1540 lines, 14 existing
  tests). I read the container-patch and `modify_target` paths for the
  `apply_label` / `set_starred` contract question (answer: Gmail
  honours it - `set_label_membership` reaches `add_container_patch`,
  which is a plain additive label add for any non-`archive` id, and
  `set_starred`'s `ContainerId("STARRED")` takes the same path). I did
  not audit MIME rendering, draft patching, search translation, or
  identity/vacation mapping.
- **`crates/google/src/account/contacts.rs`** (1872 lines, 29 tests),
  **`calendar.rs`** (1379 lines, 21 tests), **`account/error.rs`**
  (2063 lines, 60 tests), **`filters.rs`**, **`cloud.rs`**. All
  already have the densest coverage in the crate; I deprioritised them
  in favour of the six files with zero tests.
- **`crates/net/src/account_error.rs`** and **`trace.rs`**.
  `account_error.rs` has a 625-line integration suite already;
  `trace.rs` is 88 lines of traceparent formatting whose only
  interesting property (the trace id is minted from the UUID RNG, not
  the clock) is already asserted by construction.
- **The NaN panic in `rate.rs`** is reported (N1) but not tested,
  because the test would be the panic. It needs
  `#[should_panic]` or - better - the validation fix, at which point
  it becomes an ordinary error-path test.
- **`Cargo.toml` edits.** N16 needs `tokio/test-util` in
  `bifrost-google`'s dev-dependencies; out of scope for me to add.
- **Confirming B12 in `bifrost-graph`.** The
  attach-without-detach shape almost certainly repeats there
  (`bifrost-graph` uses the same `Net::shared_default()` +
  `attach_account` pattern), but that crate is another agent's
  section.
