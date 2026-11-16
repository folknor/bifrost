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

## STR-014 - MIME lives in `bifrost-types`, and there are two RFC 5322 renderers

**Reported by:** types.

`src/mime/` (parser, header decoder, transfer decoding, body selection,
renderer, about 4,000 lines with tests) is why the contract crate depends on
`base64`, `encoding_rs` and `jiff` for MIME. Its consumers are essentially IMAP
and Google. `bifrost-smtp` carries a second, lettre-derived builder
(`smtp/src/message/`), so header encoding, address formatting and boundary
generation are decided by two published renderers. Proposal: a `bifrost-mime`
crate (depended on by types for `Address`, or the reverse) and an explicit
ruling on how `smtp::message` relates to it.

The smtp-sasl hunter (needs owner on direction; no deletion implied) shows the two
composers already hold opposite Message-ID policies: bifrost-smtp's
`MessageBuilder::message_id(None)` generates `<36 alphanumerics@HOSTNAME>` from
`hostname::get()`, while bifrost-types' `mime::render` documents "a deterministic,
controlled-domain Message-ID (the sender's domain, never a leaked machine
hostname)". The workspace also has two address-list parsers (bifrost-smtp
`Mailbox`/`Mailboxes` FromStr with `message/mailbox/parsers/rfc2822.rs` vs
`bifrost_types::Address::parse_list`), and two Date formatters; `account_error.rs`
notes nothing in the workspace builds a bifrost-smtp `Message`. bifrost-types'
renderer is what every account crate's send path uses (IMAP, Google drafts);
bifrost-smtp's `message/` is a richer published builder (DKIM, attachments, typed
headers) used by nobody in the workspace. A plausible direction: bifrost-smtp's
builder becomes a typed facade over bifrost-types' header and body writers (it
already depends on bifrost-types), with the DKIM signer staying in bifrost-smtp, so
header encoding, folding, Message-ID, Date and Bcc stripping are decided once.

## STR-046 - A partitioning framework with no producer imposes its costs on every consumer

**Reported by:** sync-lanes (published; not a deletion recommendation).

`BackfillPolicy`, `BackfillStrategy`, `PartitionPlan`, `PartitionBounds`,
`plan_time` / `plan_uid` / `plan_page`, `OpenPages`, `parse_page_partition`,
the page tie-break in `get_backfill`, and `open_pages_resume` all serve
`TimeWindowed`, `UidRange` and `PageCount`; no in-tree provider returns any of
them. `BackfillPolicy` is documented as "configurable per account" but never
configured (`backfill_plan_for` builds `BackfillPolicy::default()` or a literal
with `clock_skew: Duration::ZERO`). `BackfillStrategy` duplicates
`InventoryPartitioning` and `PartitionBounds` duplicates `InventoryPartition`
(using a `Timestamp::MIN` sentinel for "open" where `InventoryPartition::Time`
uses `Option`). The hunter's narrower points: the chain
`InventoryPartitioning -> BackfillPolicy -> plan -> PartitionBounds ->
InventoryPartition` has two redundant middle layers, and collapsing it to
`InventoryPartitioning -> Vec<InventoryPartition>` (or `OpenPages`) in
`backfill/plan.rs` keeps every behaviour (owner ruling for the middle types);
and the durable cost of the unexercised machinery (the selection rule every
store implements, the time-key compaction contract) is paid now by every backend
for producers that do not exist yet.

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

`inventory_partitioning` is not overridden, so IMAP folders inventory as one `UID
FETCH 1:*` that also accumulates every UID in a `Vec<u32>` before minting the
cursor. The half-open `u64`-ended `InventoryPartition::Uid` vocabulary was
designed with IMAP in mind (`types.md` says as much), and the bounded backfill
lane depends on partitions, but no production crate mints a `Uid` partition.
Related: STR-046.

## STR-138 - Smaller SMTP axes and placement notes

**Reported by:** smtp-sasl.

The reply-mode enum, `MechanismChoice`, the credentials and challenge-framing
split, and the shared message-error strings landed. One residue needs a
bifrost-net API first: `client/metering.rs`'s blocking admission loop belongs
beside `BandwidthBudget` in bifrost-net as a blocking budget API, which does not
exist yet. (The rest of `authentication.rs` moving to bifrost-sasl is STR-135.)
