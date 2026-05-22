# Phase 3.1: Dependency and shared-crate-wiring audit

`Account` / `AccountFactory`, `bifrost-net`, and `bifrost-types` were
shoehorned into pre-existing protocol crates. This audit catalogues
what the shoehorn left behind at the dependency and shared-crate-
wiring level. The companion **Phase 3.5** (in `orchestration.md`)
covers the visibility and two-ways-to-do-X audit at the trait-surface
level.

This phase runs *before* Phase 3.5: the structural fix in finding 1
(routing the HTTP-based protocol crates through `bifrost-net`)
dissolves most of the other findings, and doing the small items
before the big one creates rebase churn.

## Status

Phase 3.1 is **complete**. Items 1-6 in the order of operations
below landed in a single workstream; the exit-criteria checklist
at the bottom is satisfied.

What actually landed:

- **Routing through `bifrost-net`.** `bifrost-jmap`, `bifrost-gmail`,
  and `bifrost-graph` no longer own a `reqwest::Client`. Gmail and
  Graph default constructors register against `Net::shared_default()`
  (a `OnceLock<Net>` process-wide singleton) with unique
  `AccountId` suffixes (`gmail-1`, `gmail-2`, ...) so multiple default
  clients in the same process share the connection pool, the per-host
  `RateLimitGovernor`, and the `BandwidthMeter`. Consumers that want
  their own `Net` keep calling `with_account_net(...)`.
- **JMAP redirect policy preserved.** `NetConfig.follow_redirects`
  was added and wired through to `reqwest::redirect::Policy::none()`.
  `bifrost-jmap` sets it to `false` and runs a manual 5-redirect
  loop that consults `ClientBuilder::follow_redirects(trusted_hosts)`
  and strips `Authorization` on cross-host hops by comparing the
  next-hop host against the original.
- **`bifrost-net::AccessToken` everywhere.** Plain `String` bearer
  storage in the three clients is gone. JMAP wraps the credential
  in a `StaticTokenSource` (sync-mutable via `std::sync::RwLock`),
  and `JmapCredentials::set_access_token` plus the WS-feature-gated
  `Client::set_access_token` update the same `Arc<RwLock<AccessToken>>`
  that the `AccountNet`'s token source observes.
- **`urlencoding` removed.** Duplicate per-crate encoders were
  hoisted into `bifrost_net::url::encode_component`. Every call
  site goes through that helper.
- **`log` removed.** Gmail and Graph log via `tracing` exclusively.
- **Workspace pin housekeeping.** `async-stream`, `thiserror`,
  `getrandom`'s `std` feature, and the `futures` umbrella are
  declared once at the workspace level; per-crate manifests use
  `{ workspace = true }`. `futures-util` is gone.

Known limitations carried into Phase 3.5:

- **JMAP manual redirect loop is not RFC-7231 method-aware.** It
  replays method and body across every hop instead of converting
  `POST` to `GET` and dropping the body on `301`/`302`/`303`.
  Harmless for JMAP today (the server endpoints we hit are POST for
  `api_request` and GET for session/blob/SSE), but worth tightening
  if redirect handling grows into a shared primitive.
- **Per-host rate buckets serve a process-wide pool.** Multiple
  Gmail accounts sharing `Net::shared_default()` all share one
  `www.googleapis.com` token bucket. Gmail's actual quota is
  per-user, so a multi-account ratatoskr workload may under-provision
  itself relative to the API ceiling. Consumers that need per-account
  isolation can construct their own `Net` and pass it through
  `with_account_net`. Revisit if multi-account throughput becomes
  an issue.

## Findings

### 1. bifrost-net is not wired into jmap / gmail / graph (structural)

`crates/net/Cargo.toml:3` describes itself as "shared HTTP transport
for the bifrost JMAP, Gmail, and Graph clients." The only crate that
actually depends on `bifrost-net` is `bifrost-sync`
(`crates/sync/Cargo.toml:19`). `bifrost-jmap`, `bifrost-gmail`, and
`bifrost-graph` each spin up their own `reqwest::Client` and their
own OAuth path.

Consequences this audit keeps tripping over:

- Three different bearer-token storage approaches; only
  `bifrost-net::AccessToken` zeroizes.
- Two different logging facades (see finding 3).
- Direct `urlencoding` / `reqwest` duplication of work
  `bifrost-net` already does.

Routing jmap / gmail / graph through `bifrost-net` would dissolve
most of the findings below. The smaller items are not worth touching
until this decision is made.

### 2. Workspace-policy holes (deps that should be workspace-pinned but aren't)

These crates are declared per-package, so the workspace cannot bump
versions in one place:

- `async-stream = "0.3"` in `crates/gmail/Cargo.toml:21`,
  `crates/graph/Cargo.toml:21`, `crates/jmap/Cargo.toml:26`.
- `futures-util = "0.3"` in `crates/graph/Cargo.toml:28` and
  `crates/jmap/Cargo.toml:25`, while every other crate uses workspace
  `futures` (`crates/imap`, `gmail`, `net`, `sync`, `types`). Pick
  one (see finding 4).
- `thiserror = "2.0.18"` in `crates/imap/Cargo.toml:40` and
  `crates/smtp/Cargo.toml` instead of `{ workspace = true }`.
- `quick-xml = "0.40.1"`, `mail-parser = "0.11.3"`,
  `calcard = "0.3.3"` in `crates/graph/Cargo.toml:26-32`. Fine to
  keep package-local since only graph uses them, but version pins
  live in source rather than workspace.

### 3. log vs tracing: gmail / graph drop events on the floor

- `crates/gmail/Cargo.toml:27` and `crates/graph/Cargo.toml:30`
  depend on `log`.
- `imap`, `smtp`, `net`, `sync`, `types` use `tracing`.
- Nothing in the workspace installs `tracing_log::LogTracer`. So if a
  downstream (ratatoskr) sets up `tracing-subscriber`, every
  `log::info!` / `log::warn!` in gmail / graph (24+ call sites in
  `gmail/src/account/*`, `graph/src/webhooks.rs`, etc.) silently
  disappears.

Migrate gmail / graph to `tracing` and remove `log` from the
workspace.

### 4. futures: consolidate the import path

Across the workspace we use `StreamExt`, `TryStreamExt`, `stream`,
`Stream`, `Future`. That is `futures-core + futures-util` only -
nothing from `futures-channel` / `-executor` / `-io` / `-sink`.

Two options, pick one:

- Everyone imports `futures::*` (drop the per-crate `futures-util`
  in jmap / graph).
- Everyone imports `futures_util::*` (workspace-pin `futures-util`,
  drop the `futures` umbrella).

Today we have both styles, so "uses futures" lies depending on
which crate you are in.

### 5. async-stream is justified

Used by `crates/{gmail,graph,jmap}/src/**/*.rs` for `try_stream!`
blocks that turn imperative paginated-fetch code into a `Stream`.
Replacing them with hand-written `poll_next` impls or
`futures::stream::unfold` would be uglier and is not worth it.
Promote to workspace dep and move on.

### 6. urlencoding vs percent-encoding: pick one

The workspace currently declares both:

- `urlencoding = "2"` (workspace), used in `crates/gmail/src/**`
  and `crates/graph/src/**` (form-style encoding).
- `percent-encoding = "2.3"` (workspace), used in
  `crates/jmap/src/blob/{download,upload}.rs` and
  `crates/smtp/src/transport/smtp/connection_url.rs` (with explicit
  `AsciiSet`).

`urlencoding` (2.1.3) was last updated July 2023 and is essentially
abandoned. `percent-encoding` is part of the rust-url family,
actively maintained, and lets you pick the encode set explicitly,
which is what we should be doing for URL path components anyway.
Migrate gmail / graph to
`percent_encoding::utf8_percent_encode(..., NON_ALPHANUMERIC)` (or
a tighter set), drop `urlencoding` from the workspace.

### 7. Unmaintained-looking crates: actual status

| Crate             | Pin                 | Last release                           | Verdict                                                                                  |
|-------------------|---------------------|----------------------------------------|------------------------------------------------------------------------------------------|
| urlencoding       | workspace `"2"`     | Jul 2023, stagnant                     | Replace with `percent-encoding` (see finding 6).                                         |
| httpdate          | workspace `"1"`     | 2022, complete                         | Keep. Just parses RFC 7231 IMF-fixdate; nothing to maintain. Used in `net/request.rs:616` and `smtp/message/header/date.rs`. |
| mime              | smtp-local `"0.3.4"`| 0.3.17 (2023), hyperium owns it        | Keep. Surface used is `Mime` / `FromStrError` plus `TEXT_PLAIN_UTF_8` / `TEXT_HTML_UTF_8`. Stable. |
| email_address     | smtp-local `"0.2.1"`| 0.2.9, original repo stagnant, Arcjet fork keeps it alive | Keep. Small, correct.                                                                    |
| hostname          | smtp-local `"0.4"`  | 0.4.2                                  | Keep. Used twice for EHLO fallback.                                                      |
| fastrand          | smtp-local `"2.0"`  | 2.x active                             | Keep. Used for MIME boundary generation.                                                 |
| calcard           | graph-local `"0.3.3"`| active (Stalwart Labs)                | Keep.                                                                                    |
| mail-parser       | graph-local `"0.11.3"`| active (Stalwart)                    | Keep.                                                                                    |
| quick-xml         | graph-local `"0.40.1"`| active                               | Keep.                                                                                    |
| tokio-tungstenite | jmap-local `"0.29.0"`| latest is 0.29.0 (Mar 2026)           | Up-to-date.                                                                              |

Only `urlencoding` is the real unmaintained-and-replaceable case.
The others are either complete-and-frozen (`httpdate`) or
stagnant-but-correct.

### 8. zeroize coverage gap

`bifrost-net::AccessToken` (`crates/net/src/auth.rs:46`) wraps the
bearer token in `Zeroizing<String>`. `bifrost-imap` does the same for
SCRAM proofs and XOAUTH2 strings
(`crates/imap/src/connection/auth.rs:253,323`,
`crates/imap/src/types/secret.rs:6`). `bifrost-smtp` re-exports it.

OAuth tokens in the API clients are plain `String`:

- `crates/gmail/src/client.rs:22`: `access_token: RwLock<String>`.
- `crates/graph/src/client.rs`: same pattern.
- `crates/jmap/src/client.rs:31`: `enum Authorization { Bearer(String), ... }`.
- `crates/graph/src/ews/client.rs`,
  `crates/graph/src/account/ews_stream.rs`,
  `crates/gmail/src/account/*`: tokens passed around as plain
  `&str`.

This goes away automatically if the clients move onto `bifrost-net`
(finding 1). If we delay that, the floor is wrapping the three
`Client` fields in `Zeroizing<String>` directly.

### 9. Cargo.lock duplicate versions

Three notable splits, all transitive:

- RustCrypto 0.10 vs 0.11 chain: `sha2 0.10.9 + 0.11.0`,
  `sha1 0.10.6 + 0.11.0`, `digest 0.10.7 + 0.11.3`, `block-buffer`,
  `crypto-common`, `cpufeatures`, `der 0.7 + 0.8`,
  `pkcs8 0.10 + 0.11`, `spki`, `signature`, `const-oid`. The 0.10
  chain is pulled exclusively by `ed25519-dalek`, which sits behind
  the smtp `dkim` feature. Builds without `--features dkim` will
  not have it.
- `hashbrown 0.14 + 0.15 + 0.17`: 0.14 comes from `ahash` (pulled
  by calcard); 0.15 / 0.17 are downstream of std / serde-json /
  etc. Nothing to do.
- `getrandom 0.2.17 + 0.3.4 + 0.4.2`: 0.4 is ours (workspace). 0.3
  is what e.g. `rand` and a couple of others pin. 0.2 is dragged in
  by the RustCrypto 0.10 chain via `ed25519-dalek`. Same root
  cause.
- `rand_core 0.6 + 0.9 + 0.10`: same RustCrypto / ed25519-dalek
  story.

None of this is something we can fix from inside bifrost;
`ed25519-dalek` and `ahash` have to publish updates. Documented
here so future audits do not re-discover it.

### 10. Small odds and ends

- `crates/graph/Cargo.toml:29` enables `getrandom`'s `std` feature;
  no other crate does. Promote `std` to the workspace pin or remove
  it from graph.
- `crates/jmap/Cargo.toml` has deps in arbitrary order (`reqwest`,
  then `tokio-tungstenite`, then `tokio`, then `native-tls`, then
  `futures-util`, ...). Cosmetic.
- `bifrost-smtp` directly depends on `url = "2.4"`, `idna = "1"`,
  `percent-encoding`. With `urlencoding` removed, the workspace
  url family becomes cohesive: `url` + `percent-encoding` + `idna`
  everywhere they are needed.

## Suggested order of operations

1. Move jmap / gmail / graph onto `bifrost-net` (fixes findings 1,
   3, and 8 in one shot).
2. Drop `urlencoding` in favour of `percent-encoding` (finding 6).
3. Promote `async-stream`, `thiserror`, and the rest of the
   per-crate pins to `[workspace.dependencies]` (finding 2).
4. Standardize on one of `futures` or `futures-util` workspace-wide
   (finding 4).

Items 2-4 are mechanical once 1 is done; doing them before 1 just
creates rebase churn.

## Sequencing

Step 1 is the load-bearing decision and the largest piece of work.
It touches `bifrost-jmap`, `bifrost-gmail`, and `bifrost-graph`
HTTP plumbing plus their `Cargo.toml`s, and it lifts secret-handling
into `bifrost-net::AccessToken`. Run it as a single workstream with
one agent per crate working against a shared `bifrost-net` surface
(if `bifrost-net` exposes everything needed) or as a serial pass if
the agent has to extend `bifrost-net` along the way.

Steps 2, 3, and 4 are safe to parallelize across crates because each
agent owns its own `Cargo.toml` and a small set of import sites.

## File ownership

- **P3.1-A0 (jmap on bifrost-net)**: `crates/jmap/src/client.rs`,
  `crates/jmap/src/blob/*`, `crates/jmap/Cargo.toml`. May add a
  `bifrost-net::AccessToken`-backed `Authorization` field.
- **P3.1-A1 (gmail on bifrost-net)**: `crates/gmail/src/client.rs`,
  `crates/gmail/src/api/*`, `crates/gmail/Cargo.toml`.
- **P3.1-A2 (graph on bifrost-net)**: `crates/graph/src/client.rs`,
  `crates/graph/src/api.rs`, `crates/graph/src/ews/client.rs`,
  `crates/graph/Cargo.toml`.
- **P3.1-A3 (urlencoding to percent-encoding)**:
  `crates/gmail/src/**`, `crates/graph/src/**`, root `Cargo.toml`.
  Runs after A0-A2 to avoid rebase churn.
- **P3.1-A4 (workspace pin housekeeping)**: root `Cargo.toml`,
  per-crate `Cargo.toml` edits to switch `async-stream`,
  `thiserror`, `futures-util` to `{ workspace = true }`. Runs after
  A3.
- **P3.1-A5 (log to tracing in gmail / graph)**:
  `crates/gmail/src/**` and `crates/graph/src/**` for log macro
  call sites, plus `Cargo.toml`. Can run in parallel with A3.

Cross-crate findings (e.g. `bifrost-net` needs a new method)
surface as a follow-up bullet for an A0-A2 agent, not a direct edit.

## Exit criteria

All satisfied (see **Status** at the top for the landing summary).

- [x] `bifrost-jmap`, `bifrost-gmail`, and `bifrost-graph` route every
  HTTP call through `bifrost-net`. No `reqwest::Client` lives
  outside `crates/net/src/`.
- [x] All OAuth bearer tokens use `bifrost-net::AccessToken` (or
  another `Zeroizing<String>`-backed wrapper). No plain `String`
  bearer tokens in any client struct.
- [x] `urlencoding` is gone from the workspace. `gmail` and `graph`
  URL-encode through `bifrost_net::url::encode_component`
  (percent-encoding under the hood).
- [x] `log` is gone from the workspace. `gmail` and `graph` log
  through `tracing`.
- [x] `async-stream`, `thiserror`, and `futures` are declared in
  `[workspace.dependencies]`. Per-crate `Cargo.toml`s reference them
  as `{ workspace = true }`.
- [x] `futures` chosen workspace-wide; `futures-util` removed.
- [x] `brokkr check` clean under `--all-features` (orchestrator
  validated).

## Sources

- urlencoding: https://crates.io/crates/urlencoding
- mime: https://crates.io/crates/mime
- httpdate: https://crates.io/crates/httpdate
- email_address (Arcjet fork): https://github.com/arcjet/email_address
- tokio-tungstenite: https://crates.io/crates/tokio-tungstenite
- calcard: https://crates.io/crates/calcard
