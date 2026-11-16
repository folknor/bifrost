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

Graph and Google now set `AccountSpec::trusted_origins` at attach (Graph api
origin; the four Google bases), DAV carries no account bearer, so JMAP is the
only bearer-carrying protocol whose account handle is unrestricted
(`crates/jmap/src/transport_reqwest.rs`, `AccountSpec` construction). Its
authority is the authenticated session document: `apiUrl`, upload, download,
event-source and WebSocket URLs on possibly independent origins, as templates
whose variables may fall in the host (`client.rs` accepts that). A set fixed at
attach cannot describe it, and one widened per session fetch keeps origins of
superseded sessions. Open question: a session-scoped, template-aware
admission (replaced, not widened, on each session fetch; WebSocket door
included) owned by net or by JMAP. Not a live leak: every JMAP URL comes from
configuration or the session.

Refused, with reasons in `reference/net.md` "Credential destination
admission": requiring a bare `AdmittedUrl` on every bearer door. Anyone can
build a `TrustedOrigins`, so the proof shows membership in some policy, not
authority over this account's credential; the account set is what binds.

