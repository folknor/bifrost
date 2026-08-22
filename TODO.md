# TODO

Cross-crate work items surfaced by per-crate work but not fixable inside one
crate. Each entry stands alone: symptom on the discovering side, what the other
side would have to ship, what was done locally instead, and what remains wrong.

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
