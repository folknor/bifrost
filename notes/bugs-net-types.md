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
`reference/net.md`. The two larger findings were left for the final round.

2026-08-22 (round 2, closing the document): `NetConfig` split into the
process-wide half it always should have been (client pool, keepalive, TLS trust)
and a per-account half on `AccountSpec` (timeouts, User-Agent, redirect policy,
response ceiling, token max-age). JMAP now attaches to a shared `Net` instead of
minting one per transport, so its accounts genuinely share one client, connection
pool, governor and meter - which is what makes `reference/net.md`'s
multi-account quota coordination claim true for JMAP rather than false. Redirects
stay disabled in reqwest and are followed inside bifrost-net, which is the only
way a shared client can carry a per-account trusted-host allowlist and the only
place method rewriting and `Authorization` stripping exist. The 94-method
`Account` trait was audited lane by lane and deliberately KEPT: a split is
object-safe but adds impl and accessor churn without removing any dependency, and
the grouping evidence is recorded in `reference/types.md` so the next person does
not have to redo the audit.

Three defects the split introduced, all found in review and fixed in the same
commit. Two terminal-status drains still called `response.bytes()`, which had
been safe only because of the client-level read timeout the split removed - a
server sending 4xx headers and then stalling mid-body would have blocked forever,
and google/graph accounts carry no total request deadline to rescue them; those
drains also buffered an unbounded body before the cap applied. JMAP's
`accept_invalid_certs` was accepted and discarded, so a self-signed deployment
would have failed every HTTP request while its WebSocket succeeded, the option
appearing to work and not; TLS trust now selects between two shared clients
rather than being silently dropped, preserving sharing within each trust class.
And the split deleted three tests that pinned `NetConfig` defaults which had
merely MOVED to `AccountSpec`, leaving those defaults unpinned; they are
re-pinned on their new home.

2026-08-22 (close pass): the arc-level review both earlier rounds lacked was
run. The never-cold-reviewed half of round 2's fix-and-commit stage was read in
full - `Net::shared_for_tls`, both rewritten terminal-status drains, the
`Canned::StreamThenStall` seam variant, and the tests around them - and found
sound; the stall test is deterministic under paused time because the drain's
read-timeout is the only pending timer. `crates/net/src/rate.rs`, which round 1
lost and reconstructed from context, was re-read as new code: the generation
check holds at all four sites, only a ticket's owner can remove it from the
queue (so the front cannot change between the is_front check and the debit),
and `Notify`'s stored-permit semantics cover the wake-before-park race. The
FIFO admission and permanent-strand properties hold, and the governor tests
observe them with uneven costs. auth.rs backoff escalation, terminal shortcut,
fallback restore and reset-on-success were verified against the prose, and the
escalation counter is shared across refresher handle clones. The consumer
sharing claim was checked crate by crate: google and graph attach to
`Net::shared_default`, JMAP routes through `shared_for_tls`; caldav and carddav
still run `ReqwestDavTransport` and are documented as outside the claim rather
than silently inside it. `reference/types.md`'s 94-method count was re-counted
against the trait and is exact. Three doc-only drifts in `reference/net.md`
were fixed by the close pass: the sharing paragraph named `shared_default`
where JMAP actually uses `shared_for_tls`, the retry-decision bullet still said
reqwest follows redirects (contradicting the `Policy::none()` design stated
later in the same document), and the file map omitted `status_line.rs` and
`test_support.rs`. No code defect was found.

Accepted residuals, on the record:

- **No concurrency governor in bifrost-net.** `TODO.md` carries the full
  cross-crate writeup (symptom in JMAP's foreign probing, clamped `[1, 8]` at
  the single overlapping call site, no cross-account global bound). The close
  pass ruled it stays a recorded deferral: it is a new permit-pool feature with
  its own API and test-bite obligations, not a defect in these two commits.
- **The DAV crates have not migrated onto `AccountNet`.** Round 1 shipped the
  enablers (`request(Method, ..)`, optional token source) deliberately without
  the migration; `reference/net.md` now scopes the sharing claim accordingly.
- `read_capped_response_body` applies the read timeout per chunk, so a server
  trickling one byte per interval can stretch a terminal-status drain to
  roughly `STATUS_BODY_CAP` intervals. Bounded by the 4 KB cap; noted, not
  worth a second deadline.
