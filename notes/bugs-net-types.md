# bifrost-net / bifrost-types bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/net/` and `crates/types/`, the shared
foundation. Read-only review. Findings are unverified work material. Line numbers are as of the hunt
and will drift.

2026-08-07 (second pass): three more fixed - the missing request deadline (as
`NetConfig::read_timeout`, an inactivity bound; a TOTAL default deadline was tried and then
dropped, because consumers program against `Account` and cannot override it, so it could have
failed a slow-but-healthy large fetch with no recourse), uncapped buffered
response bodies in both `send()` and the two DAV transports, and the `AccountNet` registration
leak (a registration-keyed `Drop`, which also forced same-id `retag` to stop minting a second
owner of one token).

2026-08-07: five findings verified and fixed, each with a regression test - the non-idempotent
retry replay, `same_origin` ignoring scheme, `status_line_code`'s missing range constraint,
`finalize`'s O(n*m) membership scan, and the dropped refresh driver reported as terminal auth
loss. `Error::Cancelled` now has a producer (the dropped-driver source), closing that item too.
Their entries are removed below; the behaviour lives in `reference/net.md`. Everything still
listed is unverified.

2026-08-22 (round 1 of the bug-hunt arc): the remaining smaller findings fixed - the no-fallback
token-refresh backoff (now escalating 1s -> 60s on transient failure, straight to 60s on an
authoritative issuer refusal, reset by any success), FIFO admission in the rate governor,
`status_line_code` reading the status POSITION rather than the first in-range number anywhere in
the line, and the two DAV blockers (`AccountNet::request(Method, &str)`, optional
`AccountSpec::token_source`). `Dispatch` stays private - assessed as correct. The FIFO change
introduced and this round fixed a permanent-strand bug: a waiter woken by the final `unregister`
could be recaptured by a bucket another account registered for the same host. Behaviour lives in
`reference/net.md`. The two entries below remain unverified.

## NetConfig conflates process-wide and per-account settings, destroying the sharing the crate exists for

`Net` owns the reqwest client, the governor, and the meter, all genuinely process-wide. But
`NetConfig` also carries `connect_timeout`, `user_agent`, `root_certs`,
`dangerous_accept_invalid_certs`, `token_max_age`, and `follow_redirects` (whose `trusted_hosts`
allowlist is inherently per-account: it is seeded from the account's own base host). The consequence
is visible: `crates/jmap/src/transport_reqwest.rs` calls `Net::new(config)` per transport, i.e. per
account, because it needs a per-account trusted-host allowlist and cert settings. Every JMAP account
gets its own reqwest client, its own connection pool, its own empty governor, and its own meter. The
"process-wide, multi-account quota coordination out of the box" story in `reference/net.md` is false
for JMAP. Conversely google/graph use `Net::shared_default()` and therefore cannot configure TLS
roots or timeouts at all.

This wants a split: `NetConfig` keeps only what the shared client owns (pool, keepalive, TLS trust),
and everything per-account (timeouts, UA, redirect policy, token max-age) moves onto
`AccountSpec`/`AccountNet`. A breaking change worth taking pre-1.0; it is the single change that
would make the crate deliver what it advertises.

2026-08-07 amendment: the second sentence of the motivation above is wrong and should not drive
the design. Consumers program against `Account` / `AccountFactory`; `bifrost-net` is an internal
shared layer like `bifrost-sasl`, so google/graph being unable to configure TLS roots or timeouts
is not a defect - it is the abstraction working. Where a per-deployment value is genuinely needed
it belongs on the protocol crate's own config struct, which is already the pattern.

The rest of the finding stands and is the reason to keep it: JMAP minting one `Net` per account
means one reqwest client, connection pool, governor, and meter per account, which is a real
resource and correctness problem and does make `reference/net.md`'s "multi-account quota
coordination out of the box" false. The split is still the fix; the goal is sharing the client,
not exposing configuration.

## The 93-method Account trait

Object-safe with default impls, so it does not force duplication mechanically, but every new lane on
it is a workspace-wide edit. The hunter did not audit this deeply enough to propose a specific split;
flagging it as the thing most likely to be the next structural cost.
