# Bug hunt: bifrost-net + bifrost-types (error model)

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/net/`, `crates/types/`, and the cross-cutting error-model contract
(`reference/error-model.md`) as implemented in shared code. Also checked two
lateral claims routed from the google and DAV hunts; both are resolved and
closed (the dot-segment tests exist and the double-escape consequence is now
documented in `reference/net.md`; dav-core cites the named
`DEFAULT_MAX_HOPS` constant).

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

## Suspected / smells (lower confidence or low severity)

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
