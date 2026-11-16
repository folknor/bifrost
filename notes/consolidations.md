# Consolidations - design hunt, November 2026

One decision answered in more than one place. The unit of an entry is the
question, not the site: every site that answers it belongs to the one entry,
along with where its single owner should live. Each entry is the hunters'
account, unverified.

1. An entry is removed entirely when completely resolved. No historical record
   stays here.
2. Stable IDs never change and are never reused; removal leaves a gap.
3. An entry adjudicated against, verified incorrect, or whose outcome is that no
   action is taken owes comments at the code sites it names - and, where the
   claim touches a documented contract, the relevant `reference/` or `docs/`
   page - before the entry is removed, so the finding is not hunted again.
4. Once all findings are resolved, the file gets deleted.
5. Finding IDs are never written into the code or other documents. They are
   stable only until this document is drained; the next hunt writes new ones,
   and they are never deduplicated through git history. Carry the context
   inline instead.

Each entry names the hunt scope(s) that reported it. Where hunters disagreed,
both readings are recorded. The raw reports are in the commit that precedes
this file's.

## CON-047 - May this URL carry the account credential?

**Reported by:** graph.

Partly landed in wave 15: `crates/net/src/url.rs` has `TrustedOrigins` and a
net-minted `AdmittedUrl`, Graph and DAV admit through them, and an optional
`AccountSpec::trusted_origins` is checked before automatic bearer lookup.
Residue: an `AccountSpec` without `trusted_origins` still relies on each
protocol's own admission, because composed handles and JMAP session endpoints
have no complete account-wide authority set yet, so bearer-carrying requests
are not required to take an `AdmittedUrl`. The original account:

net attaches the bearer to the first request whatever its host (stated in Graph's
`origin.rs` module doc) and leaves admission to each crate: Graph (`origin.rs`)
uses an exact-origin `AdmittedUrl` type and refuses everything off the base;
dav-core (`DavDispatch::admit_discovered_urls` / `is_trusted_url`) uses a string
allowlist consulted per request, admits a cross-origin HTTPS discovered home, and
`resolve_url` decides "absolute" by the case-sensitive literal prefixes `http://`
and `https://`; JMAP relies on the redirect policy's `same_origin()`. The
transport attaches the credential, so it should own admission: `AccountSpec`
carries trusted origins, bearer-carrying requests take an `AdmittedUrl` minted by
net, and each crate contributes only its policy for widening the set (DAV's
discovered homes, Graph's upload sessions). Related: BUG-075.

## CON-068 - What support detail does a proof of irrelevance carry?

**Reported by:** spar (Gmail cursor-bridge absence).

`crates/sync/src/repair.rs` flattens `DefinitiveIrrelevance` to its `detail`
string with an `_ => String::new()` arm (the enum is `#[non_exhaustive]` in
bifrost-types, so the wildcard is forced). A future variant would discharge
debt with an empty support detail and nothing would notice. The owner of the
answer is the type: a `DefinitiveIrrelevance::detail()` accessor in
bifrost-types, exhaustive at home, removes the wildcard. Also unverified:
`OutOfScope` appears to have no producer in the workspace.

