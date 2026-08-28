# TODO

Cross-crate work items surfaced by per-crate work but not fixable inside one
crate. Each entry stands alone: symptom on the discovering side, what the other
side would have to ship, what was done locally instead, and what remains wrong.

## CalDAV and CardDAV sync batches do not report inbound bytes

**Symptom (discovered while closing the bifrost-jmap byte-accounting audit).**
`Batch::bytes_in` is consumed by the sync engine when it fuses inventory and
change batches, and the field is part of the published protocol-neutral stream
contract. JMAP, Google, and Graph now aggregate request-local transport counts,
but every request-producing batch in `bifrost-caldav` and `bifrost-carddav`
still sets `bytes_in: 0`. IMAP also has zero-valued account batches and needs a
protocol-specific audit because its byte stream is not the shared HTTP seam.
Those zeros make provider traffic invisible to engine-side per-scope accounting
even though requests were performed.

**What the other side would have to ship.** The DAV transport funnel needs a
request-local inbound byte count covering success and any error path that later
emits a batch, aggregated and cleared at each batch boundary. IMAP needs the
equivalent count at its driver transcript boundary. Counts must describe bytes
actually read, including retry and redirect traffic where applicable, rather
than merely the decoded entity length.

**What was done here instead.** JMAP was verified site by site. Its API batches
use the existing `ByteTally`, blob batches report their downloaded body size,
and its only zero-valued batches are local paths that perform no request. No
other protocol crate was changed from inside the JMAP round.

**What remains wrong.** CalDAV, CardDAV, and possibly IMAP still under-report
real inbound traffic as zero. The published field must remain; the repair is to
make it truthful, not remove it.

## bifrost-sync mutation retry storage is unbounded

**Symptom (discovered in bifrost-sync).** A bulk mutation campaign retains its
retry candidates in an unbounded `Vec<ObjectId>`, so campaign memory grows with
the number of unresolved targets in a single run.
`MutationConfig::retry_queue_cap` reads as a bound on that memory but is inert:
no production path consults it.

**What an external consumer would have to ship.** A consumer that needs a
retry-memory bound must bound the target list before starting each campaign.
`retry_queue_cap` must not be relied on for it; the engine provides no such
guarantee today.

**What was done here instead.** Nothing beyond disclosure. The field stays, and
the durable sync reference states plainly that it does not bound the vector.

**What remains wrong.** Reintroducing a real bound requires defined overflow
semantics that preserve every target's outcome; silently dropping retry ids
would turn memory protection into mutation-accounting loss.

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

The `tokio` feature is a different shape and is not affected: it gates the
async half against a blocking half that really does build without it.
`account-error` gates nothing at all.

**What the other side would have to ship.** Nothing external. This is entirely
inside `bifrost-smtp`.

**What was done here instead.** Disclosed only. The default build and the
`--no-default-features --features account-error` build are both green, so no
supported configuration is broken today.

**What remains wrong.** The honest fix is to delete the `account-error` feature
and make `bifrost-types` an unconditional dependency. The alternative - actually
gating the `AccountError` surface behind the feature - means cfg-gating the
batch API, the error mapping and a large part of `async_transport.rs`, to
support a build that no consumer in this workspace wants. Whichever is chosen,
`default` should stop advertising an option that is not one. This would be a
pre-1.0 public feature removal and belongs in release notes.

## bifrost-smtp's async OAuth resolver has no in-crate caller without `tokio`

**Symptom (discovered in bifrost-smtp).** `Credentials::oauth2_token` is used in
production only by the Tokio-gated async connection driver, so
`--no-default-features --features account-error` compiles it with no in-crate
caller and the crate's own `deny` turns that into a dead-code error. This is
pre-existing, not a consequence of restoring the blocking half: the same shape
held before the blocking transport was deleted.

**What the other side would have to ship.** Nothing external.

**What was done here instead.** The method carries
`#[cfg_attr(not(feature = "tokio"), allow(dead_code))]`. It stays present in
every build rather than being feature-gated away, because its blocking sibling
`oauth2_token_blocking` is ungated and the pair reads as one API.

**What remains wrong.** The `allow` is a suppression, not a structural fix. If
the crate ever grows a non-Tokio production caller for the awaited path, or the
`tokio` feature stops gating the async connection, the attribute should go.

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
change and belongs in release notes - a consumer reading the changelog should
not have to infer it from a `SIZE` arithmetic change. Note the blocking writers
carry the same fix, so the change applies to both transports.

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

## JMAP builds its well-known session URL by suffix concatenation, and pins that as correct

**Symptom (surfaced while fixing the DAV crates).** RFC 6764 well-known
discovery is rooted at the origin: for a configured base of
`https://dav.example.test/service`, the probe belongs at
`https://dav.example.test/.well-known/caldav`, not at
`https://dav.example.test/service/.well-known/caldav`. Both DAV crates built the
URL by appending the suffix to the whole configured base, so a path-bearing base
produced a bogus endpoint. In CardDAV, which probes well-known first, that was
the request deciding whether the account opened at all - a server answering 401
or 403 for the nonexistent path failed the open without the valid base ever
being tried. Both crates were fixed to route through
`bifrost_net::url::well_known_url`, which parses the base, clears query and
fragment, and replaces the path.

`crates/jmap/src/client.rs:346` builds `well_known_session_url` by the same
suffix concatenation, and its test at line 468 deliberately pins
`https://example.test/jmap/.well-known/jmap` - the path-preserving spelling - as
the expected result.

**Why this is filed rather than fixed.** JMAP session-resource discovery
(RFC 8620 §2.2) is a different contract from RFC 6764, and the pinned test reads
as an intentional decision rather than the same oversight. Changing it would
alter which endpoint every JMAP account with a path-bearing base probes at open,
which is not a change to make as a side effect of a DAV round.

**What remains to decide.** Whether that pin encodes a real reading of RFC 8620
or is a copy of the same mistake the DAV crates just paid for. If the former, the
test deserves a comment saying so, because it now sits beside a shared helper
that does the opposite and will read as drift to the next person. If the latter,
JMAP should adopt `well_known_url` too. Either way the current state is one
crate doing the opposite of its sibling with nothing explaining why.
