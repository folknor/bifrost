# Structural - design hunt, November 2026

Shape and placement: the axes that should be types, and the moves, splits and
rewrites, each with the payoff it buys. Each entry is the hunters' account,
unverified. Renaming, moving and reshaping published items is ruled fine; an
entry that would remove a capability entirely (something a consumer could do
that would no longer be possible) needs the repository owner's ruling before
anyone builds it, whatever the entry argues.

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

## STR-014 - There are two RFC 5322 writers (bifrost-mime and bifrost-smtp's builder)

**Reported by:** types, smtp-sasl. Narrowed 2026-11-16: MIME moved out of
`bifrost-types` into a new `bifrost-mime` crate above it (types no longer
depends on `base64` or `encoding_rs`; JMAP, Graph, DAV, sync and net no
longer compile MIME).

Residue: `bifrost-mime`'s renderer and `bifrost-smtp`'s lettre-derived
`message` builder still decide header encoding, folding, Message-ID
(sender-domain vs `hostname::get()`), Date and boundaries independently; only
envelope derivation is shared. The renderer also has wire defects the
unified writer would fix: address headers and References are never folded
(998-octet overrun on long lists), non-ASCII display-name folds are
flattened, non-ASCII attachment filenames go out as raw UTF-8. The chain
(one structured header writer below both composers, then the builder taking
it plus Message-ID and Date policy, then one entity serializer) is in
`notes/mime-writer-unification.md`. The builder is published and stays.

## STR-071 - The IMAP pipeline has one production caller; the rest of discovery is still sequential

**Reported by:** imap-connection. Narrowed 2026-11-16: the gate bypass and the
unbounded wait were already fixed (pipeline execution admits through
`admit_commands` and takes one deadline), the second NOTIFY pre-effect copy is
gone, routing keys are derived from the classifier, and shared-folder discovery
now pipelines each prefix's MYRIGHTS probes through
`ImapConnection::my_rights_batch` (one exchange per prefix, mailbox-correlated
in the router). Removal is off the table (owner).

Residue: the other discovery round trips are still one command each. Personal
LIST (or LIST-EXTENDED, with its MissingCapability fallback) and NAMESPACE have
distinct routing keys and could share one write at open; GETMETADATA and
GETQUOTAROOT have no discovery caller at all (quota is read lazily in
`settings.rs::quota_get`). `pipeline/mod.rs` still carries its "not
line-audited" note and a module-wide `allow(dead_code)` over the typed builder
methods nobody in `account/` calls. Each further caller is a product question
(what discovery should fetch at open), not a driver change.

## STR-147 - Server clock-skew advisory is unbuilt (`clock_skew_warn` unread, `WarningKind::ClockSkew` unproduced)

**Reported by:** the STR-046 collapse (2026-11-16).

Docs corrected 2026-11-16: the knob and `WarningKind::ClockSkew` now say
plainly they have no reader and no producer (no producer ever existed in
history). The feature itself is unbuilt and needs its own ruling before
implementation. Accepted design direction (sparred, not authorized):

- bifrost-net captures per-attempt observations: response `Date`, send and
  header-receipt instants (excluding governor waits and backoff), cache
  evidence, source origin. It declares no account-wide offset. Cached
  responses are not clock samples, and `Date + Age` is not independent.
- The account assigns meaning: which service responses are eligible, per
  service, published as a retained observable (initial snapshot plus
  updates), not a bare getter.
- The engine owns policy and delivery: healthy / skewed / unknown, warn when
  the estimate's interval lies wholly past the threshold, hysteresis to
  rearm, expiry to unknown; delivered on the account change stream outside
  backfill. `ClockSkew` gains signed offset and uncertainty; the knob moves
  out of `BackfillConfig` to that owner.
- Backfill planning shifts its instant only when an account binds an estimate
  to its inventory's time semantics, and a backward shift must be shown not to
  open a gap at the newest window's upper bound. No production account
  advertises `TimeWindowed` today.

Open decisions: endpoint eligibility per provider, the policy constants, and
whether IMAP has any usable server-time source.



