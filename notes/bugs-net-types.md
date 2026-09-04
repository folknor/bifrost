# Bug hunt: bifrost-net + bifrost-types (error model)

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/net/`, `crates/types/`, and the cross-cutting error-model contract
(`reference/error-model.md`) as implemented in shared code. Also checked two
lateral claims routed from the google and DAV hunts; both are resolved and
closed (the dot-segment tests exist and the double-escape consequence is now
documented in `reference/net.md`; dav-core cites the named
`DEFAULT_MAX_HOPS` constant).

## Confident findings

(Finding 1 - `reqwest_policy`'s divergent cross-origin precondition and its
false doc comment - is fixed, without deleting the published method: the
follow/stop decision moved into `RedirectPolicy::admits_hop`, which admits any
same-origin hop (RFC 6454 scheme+host+port, the same `same_origin` the pipeline
uses) and consults the allowlist only on a hop that leaves the origin, exactly
as `classify_redirect` does. The doc comment now states that precondition and
no longer claims the CalDAV/CardDAV clients as its callers. Pinned by
`a_same_origin_hop_is_followed_even_when_the_allowlist_omits_its_host` - which
also asserts the pipeline's agreeing answer - and
`a_cross_origin_hop_is_checked_against_the_allowlist`, both revert-and-confirmed.
Whether the method should exist at all remains the owner's call.)

(Finding 2 - a redirect chain A->B->A producing a terminal `AuthLost` that
masquerades as credential loss - is fixed: the pipeline now tracks whether the
redirect walker itself removed a credential the request was carrying
(`auth_self_stripped`), and a 401 on such a hop returns
`Error::UnauthenticatedRedirectHop { message, final_response }` instead of
falling into the terminal-4xx branch. It classifies as
`Protocol(ContractViolation)` -> `RecoveryClass::ProviderContractViolation`,
carrying the rejecting hop's response evidence, so a CDN bounce no longer tells
the engine the user must re-authorize. Pinned by
`a_401_after_the_walker_stripped_auth_is_not_auth_lost` (revert-and-confirmed:
without the branch it fails with `Status { code: 401 }`, the reported shape) and
`a_401_on_an_authenticated_request_still_reports_auth_lost` for the untouched
ordinary path. Documented in `reference/net.md`.)

## Suspected / smells (lower confidence or low severity)

(Finding 7 - mixed clocks in `OAuthRefresher` - is fixed:
`RefreshState::Fresh::refreshed_at`, `refresh_not_before` and the `Refreshing`
fallback instant are now `tokio::time::Instant` like `Backoff::retry_at`, and
the issuer expiry is lifted with `tokio::time::Instant::from_std` at each
comparison (`token_expiry`). `AccessToken::expires_at` stays a published
`std::time::Instant`; the clocks share a timeline so production behavior is
unchanged. Pinned by `paused_time_drives_the_proactive_refresh_window` and
`paused_time_drives_the_max_age_window_for_opaque_tokens`, both
revert-and-confirmed against a std-clock `now`. Stated in `reference/net.md`.)

(Finding 8 - the metering asymmetry a `RangeNotHonored` / `ResponseTooLarge`
return creates - is stated: `reference/net.md` now carries a "Bodies abandoned
before they are read are not metered" section naming both returns, why each
drops its body deliberately, that the connection is likely torn down rather than
pooled, and that this is an accepted asymmetry rather than missing coverage. No
code change.)

(Finding 9 - a per-request `.cost(n)` sticking across a cross-host redirect hop
- is fixed: `cost_override_for_hop` drops the override when the hop changes host
(case-insensitively) so the new host's registered `cost_default` is re-resolved,
and keeps it on a same-host hop where the units mean the same thing. An explicit
`quota_scope` override is deliberately unchanged - that one IS a cross-hop
instruction. Pinned by `cost_override_is_kept_per_host_and_dropped_across_hosts`
and the end-to-end `a_cost_override_does_not_follow_a_cross_host_hop`, whose
ablation fails with `CostExceedsBurst { cost: 20, burst: 5 }` - the concrete
consequence the finding only guessed at. Documented in `reference/net.md`.)

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
