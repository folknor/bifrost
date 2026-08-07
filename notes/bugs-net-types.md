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

## No backoff on the token-endpoint failure path that has no fallback token

`drive_refresh`'s 30-second `refresh_not_before` deferral only applies when `can_fallback`, i.e. when
there was a cached token with a known `expires_at` that is still valid. For the `Empty` state, or for
opaque tokens with no `expires_at` (exactly the case `DEFAULT_TOKEN_MAX_AGE` exists to handle), a
failure resets state to `Empty` and the very next request drives another refresh. Concurrent callers
are single-flighted, but a serial request stream hammers the token endpoint at request rate during an
issuer outage. The doc's claim that the deferral prevents "one refresh call per request" is only true
for the narrow fallback case.

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

## The DAV bypass: Dispatch's privacy is not what is blocking them

The `test_support` feature already publishes `ScriptedDispatch` / `scripted_account`, which is the
only thing consumers actually needed from that seam. Keeping the `Dispatch` trait itself private is
buying real value (reqwest stays out of the public API), and the hunter would not change it. The
actual blockers for CalDAV/CardDAV are two much smaller gaps:

- `AccountNet` exposes only `get/post/put/patch/delete`. DAV needs `PROPFIND`, `REPORT`,
  `MKCALENDAR` (`crates/caldav/src/client.rs` builds them via `Method::from_bytes`). There is no
  `AccountNet::request(Method, &str)`.
- `AccountSpec::token_source` is mandatory `Arc<dyn TokenSource>`, but DAV is commonly Basic-auth.
  `without_bearer_auth()` exists on the builder, but the spec still demands a token source, so a
  Basic account must fabricate a dummy `StaticTokenSource`.

Fix those two and the DAV crates can drop `ReqwestDavTransport`, their own `reqwest::Client`, their
`DavTransport` double, and their `String`-typed transport errors, and gain retry, rate limiting,
bandwidth metering, traceparent, 401 refresh, and method-aware redirects (today they get follow/stop
only, since `reqwest::redirect::Policy` cannot rewrite methods or strip headers).

## The 93-method Account trait

Object-safe with default impls, so it does not force duplication mechanically, but every new lane on
it is a workspace-wide edit. The hunter did not audit this deeply enough to propose a specific split;
flagging it as the thing most likely to be the next structural cost.

## Smaller / lower-confidence

- **`status_line_code` (dav-F5) landed correctly.** Both DAV crates consume it, the caldav
  `propstat_success.unwrap_or(true)` hole is closed (unparseable is now `Some(false)`, absent stays
  `None` into success), and `commit_propstat`'s two branches are mutually exclusive. Two hardening
  nits: the parser accepts any `u16` token anywhere in the line, so `"Error 42 occurred"` parses as
  42, and it accepts codes outside 100-599. Constraining to the first token in `100..=599` would cost
  nothing.
- **Rate governor has no fairness.** Waiters race after each `notify_one`, with a 250 ms poll floor.
  Under sustained contention a waiter can starve; the code's own comment concedes "if a caller adds
  higher-volume refund paths, reconsider", and the retry loop now refunds on every retried failure (a
  change since that comment), so that condition is arguably already met.
- **Outbound metering is counted before dispatch** (`request.rs`), so bytes are recorded for attempts
  that fail `Unsent` (DNS/connect) and never touched the wire. Headers are excluded by design. Minor
  accounting skew, documented only in a code comment.
- **Doc drift in `reference/net.md`**: the Drop claim and the `MeterSinkHandle` "wiring lands in
  S1-W2" note, which reads as stale planning text in a durable reference doc.
