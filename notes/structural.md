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

## STR-062 - Protocol-value behaviour lives in IMAP `account/`, and account identity lives in `types/`

**Reported by:** imap-protocol, imap-account.

The set algebra, the FETCH merge and the `~imap-wire:` mailbox identity moved
to their owners. What remains is the registry side: `FolderEntry` bundles LIST
facts, ownership, MYRIGHTS, a cursor hint, a MODSEQ cache and a last-seen
instant behind four locks, and is the implicit owner of folder roles; making
roles, ownership and the shared-error mapping explicit registry
responsibilities (BUG-028, BUG-046) gives those decisions a home.

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

## STR-082 - IMAP does not partition inventory, so large folders get no bounded backfill

**Reported by:** imap-account (not a bug; a gap between vocabulary and its intended producer).

Still true: every IMAP folder plans `Full`, and both the first (fusion-owned)
inventory and every later backfill-orchestrator pass (re-incarnations, attaches
that resume a stored cursor) read the folder as one oldest-first `UID FETCH
1:*`. The engine side is ready: `backfill::uid_windows` plans newest-first
epoch-scoped windows from `InventoryPartitioning::UidRange`. Design and link
plan, sparred 2026-11-16: `notes/imap-partitioned-inventory.md` (bounded
descending bands inside the fusion walk, plus a `UidRange` producer for
orchestrator passes whose ceiling must cover the resumed cursor's baseline;
early `Ready` establishment rejected). Two premises of the original entry were
wrong: `reference/types.md` never tied the UID vocabulary to IMAP, and the
bounded lane counts unread pages, not partitions, so it already applies to a
`Full` plan.

## STR-147 - `BackfillConfig::clock_skew_warn` has no reader and `Warning::ClockSkew` no producer

**Reported by:** the STR-046 collapse (2026-11-16).

`BackfillConfig::clock_skew_warn` (crates/sync/src/types.rs) is documented as
the threshold above which a `Warning::ClockSkew` is emitted, but nothing reads
it and nothing constructs `Warning::ClockSkew`. The planners take skew by
shifting the planning instant, and the engine has no measured server skew to
shift by (the original design read it from a STATUS timestamp at account open).
Either measure skew and emit the warning, or say plainly the knob is reserved.

## STR-138 - Smaller SMTP axes and placement notes

**Reported by:** smtp-sasl.

The reply-mode enum, `MechanismChoice`, the credentials and challenge-framing
split, and the shared message-error strings landed. One residue needs a
bifrost-net API first: `client/metering.rs`'s blocking admission loop belongs
beside `BandwidthBudget` in bifrost-net as a blocking budget API, which does not
exist yet. (The rest of `authentication.rs` moving to bifrost-sasl is STR-135.)


