# Bug hunt: bifrost-net + bifrost-types (error model)

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/net/`, `crates/types/`, and the cross-cutting error-model contract
(`reference/error-model.md`) as implemented in shared code. Also checked two
lateral claims routed from the google and DAV hunts; the resolved versions
live here only.

## The two routed claims

### (a) URL encoder dot-segment behavior: tests exist, but the double-escape has an undocumented consequence

The google hunt suspected bifrost-net's encoders lacked a test pinning the
`.`/`..` double-escape behavior that `reference/google.md` leans on for
`events.move`. That suspicion is wrong - `crates/net/src/url.rs` pins the
behavior directly:
`complete_dot_segments_cannot_navigate_the_parsed_url` asserts
`encode_path_component(".") == "%252E"` and `".." == "%252E%252E"` and proves
via a WHATWG `Url::join` that the encoded component stays in path position;
`complete_dot_query_values_are_not_double_escaped` pins the query-side
negative. No gap.

However, a real consequence the reference doesn't state: **the double-escape
renames the resource.** A server percent-decodes once, so it receives the
literal three bytes `%2E`, not `.`. A provider id that is literally `.` or
`..` is therefore permanently unaddressable through this encoder - the request
goes to a different resource name. There is no correct spelling under WHATWG
parsing (single-escaped `%2E` is still treated as navigation), so
safety-over-addressability is the right call, but `reference/net.md` presents
this as pure hardening; if bifrost-google's reference "relies on"
round-tripping such an id, it relies on something impossible. Worth one
sentence in the docs.

### (b) `RedirectPolicy::default().max_hops` in dav-core - latent coupling, not a live trap

The DAV hunt flagged that the DAV redirect walk derives its hop cap from
bifrost-net's type default rather than from any attached spec.
`crates/dav-core/src/dispatch.rs:350` reads
`RedirectPolicy::default().max_hops` for its self-walked hop cap, while line
61 sets `FollowRedirects::Disabled` on the spec. There is no attached
per-account redirect policy to consult - DAV exposes no hop-count config - so
nothing misbehaves today. But the cap is a magic constant laundered through a
type `Default`: changing `RedirectPolicy::default()` silently changes DAV
behavior, and if DAV ever grows a redirect config it will be ignored here.
Suggest a named `pub const DEFAULT_MAX_HOPS` in bifrost-net that both
`Default` and dav-core cite.

## Confident findings

### 1. `RedirectPolicy::reqwest_policy()` is dead published API with a false doc comment and divergent semantics

`crates/net/src/redirect.rs:125-160`. No non-test caller exists anywhere in
the workspace; its doc says it exists for "the CalDAV / CardDAV clients",
which attach with `Disabled` and walk redirects themselves in `DavDispatch`
(and `reference/net.md` line ~816 says exactly that). Worse, its semantics
diverge from the pipeline it claims to unify: the pipeline consults the
allowlist only on *cross-origin* hops (same-origin hops always follow);
`reqwest_policy` checks **every** hop's host, so with a populated allowlist
that omits the origin host, a plain same-host redirect is stopped under the
bare-client path but followed under the pipeline. "The rule lives in exactly
one place" is not true of the cross-host precondition. Proposal for the owner:
delete `reqwest_policy` (published-API deletion, so owner's call), or fix its
precondition and its doc.

### 2. A redirect chain A->B->A produces a terminal `AuthLost` masquerading as credential loss

`crates/net/src/request.rs:1083`, `account_error.rs:307` in net.
`auth_for_next_hop = step.keep_auth && auth_for_next_hop` is a monotone AND -
correct security posture, auth can never come back after a foreign hop. But
the hop back to the *original* origin then arrives unauthenticated; the
401-recovery branch is skipped (`auth_for_next_hop` is false), the 401 falls
to the terminal-4xx branch as `Error::Status{401}`, and `into_account_error`
maps a bare 401 to `Authentication(ReauthorizationRequired)` -> terminal
`RecoveryClass::AuthLost`. A provider that bounces requests through a CDN and
back tells the engine the user must re-authorize, when the credential is fine
and the transport stripped it itself. The transport knows it stripped the
header; that provenance should survive - e.g. classify a 401 on a hop where
auth was self-stripped as `RedirectRejected`-like or
`Protocol(ContractViolation)` rather than letting it read as credential death.
Latent (needs a provider that redirects home cross-origin), but the
misclassification is terminal-severity when it fires.

### 3. Doc-vs-code mismatch on `FilterUpdate` idempotency

`reference/error-model.md` (Scope section): "Absolute-state writes against a
known id, including the `*Update` family and the singleton settings writers,
are idempotent." But `AccountOperation::is_idempotent`
(`crates/types/src/error/scope.rs:265`) puts `FilterUpdate` in the
non-idempotent exclusion set, while
`DraftUpdate`/`ContactUpdate`/`EventUpdate`/`IdentityUpdate` are idempotent
and the long comment never mentions filters. Probably deliberate (Gmail filter
"update" is delete+create; ManageSieve keys on name), but either the code
comment or the reference must say so - this is exactly the "authoritative
source" the whole derive table reads, and the binding doc currently asserts
the opposite of the code. One of the two is wrong; the hunter's read is the
doc.

### 4. Doc imprecision on headers-timeout evidence

`reference/net.md`: "Expiry before the response is `Timeout { Unsent }`; the
replacement attempt had not been dispatched, so replay is safe for any
method." The `response_headers_timeout` expiry in `send_streaming_inner`
(request.rs, `dispatch_limit` arm) is `Timeout { InFlight }` - correctly,
since bytes may have gone out - so a headers-timeout on a POST is *not*
replayed and reconciles instead. The code is right; the doc sentence
over-promises "replay is safe" for a class of pre-response expiry that is not
replay-safe. Docs fix.

## Suspected / smells (lower confidence or low severity)

### 5. `Error::Cancelled` classifies as `Transport(Network) + InFlight`

In `into_account_error` - for a non-idempotent op that yields
`Reconcile(TransportDropAfterSend)` and a read-back even when cancellation
happened before dispatch. Nearly unreachable today (`Cancelled` is only minted
inside `RefreshFailed.source`), so cosmetic - but the arm exists and will fire
the day someone surfaces `Cancelled` directly.

### 6. `ByteBucket::consume` oversized-chunk path

`crates/net/src/net.rs:1111`: the sleep is capped at 60 s, so a single chunk
larger than 60x the cap is under-throttled (documented as smoothing - fine),
and it ignores the tokens already in the (initially full) bucket,
over-throttling the first oversized chunk. Both minor and defensible; noted
for completeness.

### 7. Mixed clocks in `OAuthRefresher`

`crates/net/src/auth.rs`: `Backoff.retry_at` uses `tokio::time::Instant`
(pausable), while `refreshed_at`, `expires_at`, and `refresh_not_before` use
`std::time::Instant`. Paused-time tests can drive the failure backoff but not
the max-age/expiry windows; migrating the lot to `tokio::time::Instant` would
make the proactive-refresh window testable the way the rate governor and byte
bucket already are.

### 8. A `RangeNotHonored` / `ResponseTooLarge` return drops the response body undrained

`net.rs::download_stream`, `request.rs::send`. Deliberate for
`ResponseTooLarge` (stop reading gigabytes), but the dropped bytes are also
unmetered and the connection is likely torn down rather than pooled. For the
range-mismatch case the body is usually the full resource, so dropping is
right - just noting the metering asymmetry the reference's "every body the
transport reads is metered" phrasing glosses.

### 9. Per-request `.cost(n)` override sticks across redirect hops

`request.rs::recompute_cost_units`: a cost chosen for the original host's
quota units is applied verbatim to a cross-host hop's bucket, where the units
may mean something different. The no-override path correctly re-resolves the
host default per hop. Edge-case semantics; probably fine in practice since
cross-host hops rarely land on a governed host.

## Verified and found sound

- The rate governor's generation-keyed tickets, FIFO handoff, cancellation
  guard, and detach-wakes-all logic are tight - the hunter could not construct
  a strand-forever or wrong-bucket-refund interleaving.
- The `RateDebit` drop-guard, disarm-on-response, and explicit refund rules
  are consistent; refunds are generation-checked.
- The 401/auth budget, redirect budget, and attempt budget are genuinely
  independent (separate types, per-hop resets as documented).
- `OAuthRefresher` single-flight is cancellation-safe (driver is
  `tokio::spawn`ed; state transition happens under the lock with no
  intervening await).
- `try_build` invariants, `recovery::derive`, `kind_matches_cause`,
  `finalize`'s multiset accounting, telemetry-token replacement semantics, and
  the consent-tier exports all match `reference/error-model.md`, and the tests
  pinning them have real bite (mutual-exclusion proof,
  producible-reconcile-space pin, duplicate-expectation multiset test).
- `same_origin` correctly compares scheme, case-insensitive host, and
  default-collapsed port, with the `http://h:443` downgrade pinned.
