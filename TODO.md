# TODO

Cross-crate work items surfaced by per-crate work but not fixable inside one
crate. Each entry stands alone: symptom on the discovering side, what the other
side would have to ship, what was done locally instead, and what remains wrong.

## bifrost-sync removed public configuration that never controlled runtime work

**Symptom (discovered in bifrost-sync).** `SchedulerConfig`,
`ConcurrencyBudget`, `MutationConfig::{fanout_buffer, retry_queue_cap}`, and
`BackfillConfig::clock_skew_warn` were public configuration surfaces documented
as runtime tuning, but no production path consulted them. In particular,
`retry_queue_cap` read as a bound on mutation retries while the campaign retains
retry candidates in an unbounded `Vec<ObjectId>`.

**What an external consumer would have to ship.** Consumers constructing
`EngineConfig` must remove these fields and stop calling
`SyncEngineBuilder::budget`. A consumer that depended on a retry-memory bound
must bound the target list before starting each campaign; the engine does not
currently provide that guarantee.

**What was done here instead.** The unwired scheduler and budget gate, their
public exports and tests, the unused mutation fanout helper, the unused
checkpoint writer, and the three inert configuration fields were deleted. The
durable sync reference now describes only running machinery.

**What remains wrong.** Mutation retry storage is still unbounded and grows
with the number of unresolved targets in one campaign. This is disclosed rather
than disguised as a working cap. Reintroducing a bound requires defined overflow
semantics that preserve every target's outcome; silently dropping retry ids
would turn memory protection into mutation-accounting loss. These removals are
pre-1.0 breaking API changes and belong in the next release notes.

## bifrost-smtp's `account-error` feature gates nothing and cannot be turned off

**Symptom (discovered in bifrost-smtp).** `bifrost-smtp` declares
`account-error = ["dep:bifrost-types"]` and puts it in `default`, but no source
file anywhere in the crate carries `#[cfg(feature = "account-error")]`.
`bifrost_types` is referenced unconditionally by `transport/smtp/mod.rs`,
`account_error.rs`, `async_transport.rs`, `batch.rs`, and the async connection's
test module. `brokkr check -p bifrost-smtp --no-default-features` therefore
fails with 18 unresolved-crate errors, and has done so since before the
async-only deletion round - it is not a regression from it. The configuration
the feature name advertises has never existed.

This is the same defect shape as the `tokio` feature that the async-only round
removed: a cargo feature that reads like an option but describes a build that
does not compile. It was left standing only because that round was scoped to
the blocking transport, and removing a second public feature name was not part
of the agreed change.

**What the other side would have to ship.** Nothing external. This is entirely
inside `bifrost-smtp`.

**What was done here instead.** Disclosed only. The default build and the
`--no-default-features --features account-error` build are both green, so no
supported configuration is broken today.

**What remains wrong.** The honest fix is to delete the `account-error` feature
and make `bifrost-types` an unconditional dependency, exactly as was done for
`tokio`. The alternative - actually gating the `AccountError` surface behind
the feature - means cfg-gating the batch API, the error mapping and a large
part of `async_transport.rs`, to support a build that no consumer in this
workspace wants. Whichever is chosen, `default` should stop advertising an
option that is not one. This is a pre-1.0 public feature removal and belongs in
release notes alongside the `tokio` one.

## bifrost-smtp no longer exposes a blocking transport API

**Symptom (discovered in bifrost-smtp).** The crate exposed blocking SMTP and
LMTP transports alongside its Tokio transports even though this workspace is
async-only. The blocking driver mirrored the protocol state machine, pooling,
PIPELINING, LMTP final-status draining, SCRAM, connection state, and metering.
It also resolved OAuth tokens by polling `TokenSource::current()` once with a
noop waker and dropping any pending future.

**What an external consumer would have to ship.** An out-of-workspace caller
that still requires a blocking API must provide its own runtime boundary around
the async transport, or remain on an older bifrost-smtp release. The crate does
not provide a `block_on` compatibility shim.

**What was done here instead.** The blocking transports, connection driver,
pool, socket funnel, transport trait and erasure, blocking stub, examples, and
blocking-only tests were removed. Tests that uniquely pinned protocol
invariants were moved to the async transcript harness. OAuth token resolution
now has only the awaited path.

The `tokio` cargo feature was removed with them. It had stopped gating an
option and started gating the crate: with the blocking half gone, a build
without Tokio offered no transport at all, and the crate's own `#![deny(...)]`
turned that configuration into 144 dead-code errors under the DEFAULT feature
set (`default = ["account-error"]`). Tokio and tokio-native-tls are now
unconditional dependencies, so `--no-default-features` builds again.

**What remains wrong.** Two things.

First, `AsyncLmtpTransportBuilder` has no `bandwidth_metering`. The blocking
LMTP builder carried one and the async LMTP builder never did, so deleting the
blocking half removed the only way to meter an LMTP transport. An LMTP
transport is now unconditionally unmetered. This is disclosed, not fixed: LMTP
is local delivery, where a bytes-per-second ceiling protects nothing that a
metered link would. If an LMTP consumer ever needs byte accounting rather than
throttling, the fix is a two-line delegation on the LMTP builder mirroring the
SMTP one, plus a transcript test through `a_metered_transport_reports_its_
socket_bytes_to_the_sink`.

Second, this is a pre-1.0 public API removal and must be called out in release
notes so external consumers are not surprised by the missing `SmtpTransport`,
`LmtpTransport`, `Transport`, `BoxedTransport`, and `StubTransport` symbols, or
by the `tokio` feature no longer existing. An out-of-workspace caller that
passed `features = ["tokio"]` will now fail to resolve that feature; the fix on
their side is to drop it.

## bifrost-smtp now delivers different bytes for every message, and consumers need telling

**Symptom (discovered in bifrost-smtp).** Every DATA writer terminated with
`\r\n.\r\n` unconditionally, so a message that already ended in CRLF - which is
every well-formed message - gained a trailing empty line the sender never wrote.
RFC 5321 section 4.1.1.4 defines the terminator as `<CRLF>.<CRLF>` where the
leading CRLF *is* the message's final CRLF. `Message::formatted()` was therefore
not what the recipient received, and `smtp_data_size` had to declare
`len() + 2` to keep the RFC 1870 `SIZE` honest about the inflated body.

**What an external consumer would have to ship.** Nothing, in the ordinary case:
the new bytes are the correct ones and the old trailing blank line was the
defect. But anything downstream that pinned the old shape needs revisiting -
golden-file tests of delivered content, stored message digests computed over the
old terminated form, or a caller that deliberately omitted a final CRLF knowing
one would be supplied (still supplied, so unaffected).

**What was done here instead.** Fixed, not disclosed. The writer reuses an
existing final CRLF and adds one only when the buffer does not end in CRLF; the
chunked writers track the last two bytes across iterator boundaries so a
straddling CRLF is recognized. `smtp_data_size` matches the transmitted data
section in every case. DKIM is unaffected: `body_raw()` still appends an
unconditional CRLF, and RFC 6376 simple and relaxed body canonicalization both
strip trailing empty lines, so signing and delivery continue to agree.

**What remains wrong.** Nothing in the code. This is a pre-1.0 observable wire
change and belongs in release notes next to the blocking-transport removal and
the `tokio` feature removal already recorded above - a consumer reading the
changelog should not have to infer it from a `SIZE` arithmetic change.

## An inventory partition that yields zero entries is the engine's exhaustion signal, and nothing enforces it

**Symptom (discovered in bifrost-jmap).** `bifrost-sync`'s `BackfillPlan::OpenPages`
walker asks an account for `InventoryPartition::Page { from, to }` windows and
stops the whole scope the first time a partition reports `seen == 0`. That is
the only termination signal it has: there is no separate "the listing is
exhausted" flag on the partition result. The JMAP `Page` stream could produce a
zero-entry partition while the account still had messages - `Email/query`
returned a full window of ids, every one of them was deleted before the
following `Email/get`, the loop filled its window with nothing, and the stream
ended. A single concurrent deletion landing in the first window silently
truncated the backfill and dropped every later message in the scope. The bug is
data loss, not a stall, and nothing observable reports it: the scope is marked
`Completed`.

**What the other side would have to ship.** The partition result should carry
exhaustion explicitly rather than inferring it from an entry count - either a
`reached_end` flag on the partition outcome, or a `Done` payload that
distinguishes "this window produced nothing" from "there is nothing past this
window". Then the engine stops on the account's own statement instead of on a
count that means two different things, and an implementation that gets it wrong
is a type error rather than a silent truncation.

**What was done here instead.** The fix is entirely on the JMAP side: the
consolidated `email_inventory_loop` now tracks whether it has emitted any entry,
and when a bounded window is filled without emitting anything it keeps walking
past the window until it either produces an entry or `Email/query` returns an
empty page. That makes zero entries mean only "no more results", which is what
the engine already assumed. Overshooting re-reads positions the next partition
also covers; inventory entries are idempotent, so duplication is the safe side
of the trade. The engine comment that previously asserted an empty window was
unambiguous has been corrected to state the requirement it actually relies on.
The engine's resume path carried the same inference one layer up -
`open_pages_resume` read a short acked page (`items_done < to - from`) as
exhaustion and skipped the whole scope on re-attach, though a partition
legitimately emits fewer entries than its width (deletion races, id-less
objects dropped). It now resumes at `to` on any non-completion page; only the
consumer-acked completion marker means exhausted.

**What remains wrong.** The requirement is prose in a comment, not a type. JMAP
is currently the only crate that implements `InventoryPartition::Page` - every
other account crate falls through to `Full` - so there is exactly one
implementation to be right today, and the next one to implement bounded
partitions gets no compiler help and no test that fails. The overshoot is also
unbounded in principle: a scope in which a very large contiguous run of ids
vanishes mid-walk makes one partition read far past its window. In practice the
run ends at the first surviving message, but nothing caps it.

## bifrost-net has no concurrency governor, so per-protocol concurrency limits are unenforced

**Symptom (discovered in bifrost-jmap).** JMAP servers advertise
`maxConcurrentRequests` in the session's core capability (RFC 8620 §2). A client
that exceeds it is answered with a request-level `limit` error, which the JMAP
sync layer classifies as a failed call - so overshooting the limit does not
merely waste sockets, it manufactures spurious failures out of healthy requests.
`Session::max_concurrent_requests()` has been parsed and exposed for as long as
the session type has existed, and until now nothing read it, because until now
`bifrost-jmap` never issued two API requests at once.

That changed when `JmapAccountFactory::open` began probing shared/delegate
accounts concurrently instead of serially. With N granted shares, the first
version of that change put N requests in flight simultaneously with no bound at
all.

**What the other side would have to ship.** `bifrost-net` owns the shared HTTP
transport, including retry and rate limiting, and is the natural home for a
per-host (or per-account) in-flight request governor - a permit pool the
protocol crates configure from whatever their protocol advertises, acquired
around each request rather than around each call site. Today `bifrost-net`
limits *rate* but has no notion of *concurrency*, so there is nowhere for a
protocol crate to say "at most K of my requests may be outstanding" and have it
hold across every call site in that crate.

**What was done here instead.** `bifrost-jmap` bounds only the one call site it
introduced: `foreign_probe_concurrency` reads the session's advertised
`maxConcurrentRequests`, clamps it to `[1, 8]`, and feeds that to the
`buffer_unordered` over the foreign probes. A session with no readable core
capability probes serially. This is correct because that call site is currently
the *only* place in `bifrost-jmap` that puts overlapping API requests on the
wire; the reference doc says so explicitly, which is what makes the local bound
sufficient rather than merely convenient.

**What remains wrong.** The invariant is enforced by a comment and a
single-call-site bound, not by the transport. The next call site in any protocol
crate that issues concurrent requests - JMAP or otherwise - gets no help and
will have to rediscover this. There is also no global bound across accounts: ten
JMAP accounts each probing at their own advertised limit can still put eighty
requests in flight against ten different hosts, which is fine per-host and
possibly not fine for the process. A `bifrost-net` governor would address both;
the local fix addresses neither.
