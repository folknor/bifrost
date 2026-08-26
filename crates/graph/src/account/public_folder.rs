//! No-delta-token public-folder sync strategy.
//!
//! Public folders have no `@odata.deltaLink`. The opaque cursor IS the
//! sync state: a `DateTimeReceived` watermark drives the incremental
//! timestamp poll, and a throttled full-id scan reconciles deletions the
//! timestamp poll structurally cannot see (a deleted item simply stops
//! appearing; old items sit below the watermark) and also picks up
//! appearing items with no `DateTimeReceived` (some calendar/contact
//! items), which the restricted poll can never surface. The authoritative
//! live-id set rides inside the cursor so a cold resume reconstructs the
//! deletion baseline with no engine-side side table - the one
//! substantive departure from ratatoskr, which diffed against a SQLite
//! table. The whole algorithm decomposes into the pure helpers below so
//! every branch is unit-pinnable without a live EWS server.
//!
//! # Emission and paging contract
//!
//! This is the written contract the emission/paging code and its tests
//! conform to. It is the lightweight substitute for a spec: when
//! `Added` / `Updated` / `Destroyed` emit, and when the watermark /
//! deletion baseline / cursor checkpoint may advance, across the five
//! conditions below. EWS has no created/updated split, so an appearing
//! item is emitted as `Updated` plus the two `Added` `ScopeChange`
//! memberships (`emit_item_added`); "emit Added" below means exactly
//! that triple.
//!
//! Two structural invariants gate everything:
//!
//! - **A walk drives state only if it COMPLETED.** `fetch_all_items`
//!   returns `complete = false` when the page walk stalled (the server
//!   never advances its offset / `IncludesLastItemInRange` stays false)
//!   or hit `PUBLIC_FOLDER_PAGE_CAP` before the last page. A partial set
//!   must never advance the watermark, never diff deletions, never update
//!   the baseline, and never checkpoint. Both streams treat an incomplete
//!   walk as a retryable transient and re-poll next cycle (condition e).
//! - **Deletion diffing needs the FULL baseline vs. the FULL scan.**
//!   Diffing a complete baseline against a partial scan would falsely
//!   emit the unvisited tail as `Destroyed`; diffing a degraded (empty)
//!   baseline emits nothing.
//!
//! Conditions:
//!
//! - **(a) Timestamped incremental poll** (`watermark = Some`): the
//!   `DateTimeReceived >= watermark` restriction returns items at/after
//!   the watermark second. Each such item emits `Added`, except ids on
//!   the prior boundary second (`boundary_ids` - the inclusive `>=`
//!   re-fetches them, so they were already emitted last poll). On a
//!   complete poll the watermark advances to the newest received time
//!   seen and `boundary_ids` is recomputed at the new boundary.
//! - **(b) None-watermark folder** (`watermark = None`): the poll is
//!   unrestricted and returns the whole set every time. An id NEW to the
//!   `live_ids` baseline emits `Added` exactly once - whether it surfaces
//!   in the incremental poll or the full scan, and REGARDLESS of whether it
//!   carries a timestamp (an unrestricted None-watermark scan is the only
//!   entry point both for a contact/appointment with no `DateTimeReceived`
//!   and for a timestamped item that first appears in the scan rather than
//!   the poll). To stop it re-emitting next cycle it is folded into the
//!   persisted baseline the SAME cycle: the untimestamped emissions of the
//!   incremental poll are capped-and-folded (`extend_live_ids` + `apply_cap`)
//!   when no scan runs, and the full scan folds its own authoritative set
//!   otherwise; either way an established id does not re-emit. The
//!   same-cycle `emitted` set prevents the poll and the scan double-emitting
//!   one id. Timestamped emissions are NOT folded (the advanced watermark /
//!   boundary already stop them re-emitting, and folding them would bypass
//!   the cap). The watermark stays `None` until a timestamped item appears
//!   in an incremental poll; deletion reconcile still runs via the full scan.
//! - **(c) Degraded (over-cap) folder** (`degraded = true`, `live_ids`
//!   empty): the deletion diff is skipped (no `Destroyed`) and the over-cap
//!   `Warning` fires once, only on the transition into degraded mode. That
//!   transition applies UNIFORMLY to whichever path grows the persisted
//!   baseline past `PUBLIC_FOLDER_LIVE_IDS_CAP` - the full-scan reassignment
//!   or the None-watermark incremental fold - through the shared `apply_cap`.
//!   Additions still flow while degraded: timestamped items via the
//!   incremental poll, and (None watermark) new-to-baseline items via the
//!   full scan. With an empty baseline the scan cannot dedupe, so it may
//!   re-emit the same ones each scan - idempotent `Added`, the best a
//!   baseline-free mode can do; the incremental fold is skipped while
//!   degraded (the baseline stays empty by contract). A recovery scan that
//!   drops back below the cap re-installs the baseline and clears
//!   `degraded`; new additions emit BEFORE the baseline is reassigned, so
//!   recovery never silently absorbs them.
//! - **(d) Page carrying unhandled item classes** (`Task`, `PostItem`,
//!   ...): those rows are dropped from the item set (paging still advances
//!   off the server's wire offset, never the parsed count), and their
//!   class names surface as a single operator `Warning`. A class is
//!   warned about at most once per folder lifetime (`warned_classes` in
//!   the cursor); both the incremental poll and the full scan contribute
//!   observed classes, so a class only the unrestricted scan sees still
//!   warns.
//! - **(e) Incomplete page walk** (stalled offset, or page cap hit before
//!   `IncludesLastItemInRange`): the collected set is PARTIAL. The walk
//!   returns `complete = false`; the caller emits NO `Added`/`Destroyed`
//!   off it, does NOT advance the watermark or baseline, and does NOT
//!   checkpoint - it terminates retryably so the engine re-polls next
//!   cycle. This prevents permanently skipping later pages and prevents
//!   diffing the full baseline against a partial scan.
//!
//! Out of scope (NOT blessed away by this contract): `DateTimeReceived`
//! is not a change watermark, so an in-place edit that bumps the
//! change-key without moving the received time sits below the watermark
//! and is never re-emitted. This affects every class (Message included)
//! and is tracked separately.

use std::time::{SystemTime, UNIX_EPOCH};

use bifrost_types::{
    AccountOperation, AccountStream, Change, ChangeCursor, Checkpoint, CursorScope, ErrorScope,
    Fingerprint, FolderId, InventoryEntry, MailboxId, MembershipScope, ObjectChange,
    ObjectChangeKind, PageBoundary, ScopeChange, ScopeChangeKind, ServerVersion, SyncEvent,
    Warning, WarningKind,
};

use super::GraphAccount;
use super::cursor::{
    FULL_SCAN_INTERVAL_SECS, GraphCursorKind, GraphCursorPayload, PUBLIC_FOLDER_LIVE_IDS_CAP,
    PublicFolderCursor, PublicFolderRouting, cap_live_ids, decode_cursor, encode_cursor,
};
use super::graph_error::{
    GraphErrorContext, cursor_error_to_account_error, ews_shared_scope_error,
};
use super::inventory::batch;
use crate::ews::{EwsClient, EwsError, EwsFolder, EwsItem};

/// Max entries per `FindItem` page.
const PUBLIC_FOLDER_PAGE_SIZE: u32 = 100;

/// Defensive cap on the number of `FindItem` pages walked for one
/// folder. At `PUBLIC_FOLDER_PAGE_SIZE` items/page this covers well
/// beyond the `PUBLIC_FOLDER_LIVE_IDS_CAP` (10k) ceiling; it only bites a
/// misbehaving server that reports `IncludesLastItemInRange=false`
/// forever (or saturates the offset at `u32::MAX`), which would
/// otherwise spin the page loop indefinitely.
const PUBLIC_FOLDER_PAGE_CAP: usize = 1_000;

/// Which public folders this account actually SYNCS.
///
/// An organization can carry thousands of public folders holding millions
/// of items, so "discovered" and "synced" must be separate decisions:
/// discovery always browses and seeds the full readable hierarchy (that is
/// what makes the folders visible in `containers_list`), but only the
/// folders named here get a `CursorScope::Folder` and therefore a cursor,
/// an inventory pass, and a poll.
///
/// `#[non_exhaustive]`, so construct through [`PublicFolderScope::hierarchy_only`]
/// / [`PublicFolderScope::pinned`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PublicFolderScope {
    /// Browse and project the hierarchy, sync nothing. The consumer sees
    /// every readable public folder and can pin one later.
    HierarchyOnly,
    /// Sync exactly these folders (by native EWS folder id). A folder not
    /// in the list is still discovered and projected, but never synced.
    Pinned(Vec<bifrost_types::FolderId>),
}

impl PublicFolderScope {
    /// Project the hierarchy without syncing any folder's items.
    // pub: the default-safe public-folder opt-in.
    #[must_use]
    pub fn hierarchy_only() -> Self {
        Self::HierarchyOnly
    }

    /// Sync only the named folders.
    // pub: consumers pin the public folders they want synced.
    #[must_use]
    pub fn pinned(folders: impl IntoIterator<Item = bifrost_types::FolderId>) -> Self {
        Self::Pinned(folders.into_iter().collect())
    }

    /// Whether this folder may emit a cursor scope (i.e. be synced).
    fn syncs(&self, folder: &FolderId) -> bool {
        match self {
            Self::HierarchyOnly => false,
            Self::Pinned(pinned) => pinned.contains(folder),
        }
    }
}

/// Projection metadata for a discovered public folder, seeded alongside the
/// routing map so `containers_list` can render the hierarchy without
/// re-browsing EWS.
///
/// Deliberately separate from `PublicFolderRouting`: routing is serialized
/// into the opaque cursor payload, so adding display fields to it would tick
/// the cursor schema for data the sync loop never reads.
#[derive(Debug, Clone)]
pub(crate) struct PublicFolderMeta {
    pub(crate) display_name: String,
    /// The EWS `FolderClass` (`IPF.Note`, `IPF.Appointment`, ...), when the
    /// server reported one.
    pub(crate) folder_class: Option<String>,
    /// The parent folder in the public hierarchy, or `None` directly under
    /// the public-folders root.
    pub(crate) parent: Option<FolderId>,
    pub(crate) effective_rights: crate::ews::EwsEffectiveRights,
}

/// Defensive cap on the number of `FindFolder` browse calls during
/// hierarchy discovery. A public-folder hierarchy is a tree, so this
/// only bites a pathological (cyclic or enormous) hierarchy; the dedup
/// `visited` set already prevents re-browsing a folder. Bounds the
/// discovery round-trip count.
const PUBLIC_FOLDER_BROWSE_STEP_CAP: usize = 1_000;

// ── Pure helpers ────────────────────────────────────────────

/// Advance a watermark to the newest `received_at` seen in a batch.
///
/// EWS `DateTimeReceived` is RFC-3339 UTC, but the fractional-second
/// precision is inconsistent across items (`...00Z` vs `...00.5Z`), so a
/// raw string comparison mis-orders them: `'Z'` (0x5A) sorts after `'.'`
/// (0x2E), making `...00Z` look NEWER than `...00.5Z`. That would move
/// the watermark backward (re-fetching items) or forward past an item
/// (skipping it forever). Parse both sides to a real instant and compare
/// chronologically; the stored watermark is the original string of the
/// chronologically-newest value (preserving whatever precision the
/// server emitted so the `>=` restriction round-trips). An unparseable
/// timestamp is conservatively ignored for the comparison.
///
/// Calendar and contact items may not carry `DateTimeReceived`; such an
/// item contributes nothing here, so a mixed folder's watermark tracks
/// only its timestamped items, and a folder holding no timestamped items
/// keeps a `None` watermark. No-timestamp items are therefore invisible to
/// the `>=` timestamp poll: once any timestamped item sets a watermark,
/// the restriction excludes them. They are handled entirely by the
/// throttled full scan instead - which runs regardless of the watermark
/// (`scan_due`), emits appearing no-timestamp items as additions
/// (`scan_additions`), and reconciles their deletions against `live_ids`.
/// A `None`-watermark folder's unrestricted poll emits each id new to the
/// baseline once, then folds it into the persisted baseline so it does not
/// re-emit next poll (see the module-level contract, condition b).
///
/// Known poll-model limitation (all classes, Message included, NOT fixed
/// here): `DateTimeReceived` is not a change watermark, so an in-place
/// edit that bumps the change-key without moving the received time sits
/// below the watermark and is never re-emitted. Closing that would mean
/// switching the whole model off received-time; a separate change.
pub(crate) fn advance_watermark(prior: Option<String>, items: &[EwsItem]) -> Option<String> {
    let mut best = prior;
    let mut best_instant = best.as_deref().and_then(parse_received);
    for item in items {
        let Some(received) = item.received_at.as_ref() else {
            continue;
        };
        let Some(instant) = parse_received(received) else {
            continue;
        };
        if best_instant.is_none_or(|cur| instant > cur) {
            best = Some(received.clone());
            best_instant = Some(instant);
        }
    }
    best
}

/// Parse an EWS `DateTimeReceived` (RFC-3339 / ISO-8601 UTC) into a
/// comparable instant. Returns `None` for an unparseable value so the
/// caller can fall back conservatively rather than mis-order.
fn parse_received(raw: &str) -> Option<jiff::Timestamp> {
    raw.parse::<jiff::Timestamp>().ok()
}

/// Collect the ids of items whose `received_at` equals `watermark`.
/// These sit on the inclusive (`>=`) restriction boundary and would be
/// re-fetched on every subsequent poll; the cursor remembers them so the
/// next poll can skip re-emitting the ones it already saw.
pub(crate) fn boundary_ids_at(watermark: Option<&str>, items: &[EwsItem]) -> Vec<String> {
    let Some(watermark) = watermark else {
        return Vec::new();
    };
    items
        .iter()
        .filter(|item| item.received_at.as_deref() == Some(watermark))
        .map(|item| item.item_id.clone())
        .collect()
}

/// Diff a fresh full-scan id set against the prior live-id snapshot.
/// Returns the ids that were in the snapshot but are absent from the
/// scan (i.e. deleted). The caller emits a `Destroyed` change for each.
pub(crate) fn deleted_ids(prior_snapshot: &[String], scan_set: &[String]) -> Vec<String> {
    use std::collections::HashSet;
    let live: HashSet<&str> = scan_set.iter().map(String::as_str).collect();
    prior_snapshot
        .iter()
        .filter(|id| !live.contains(id.as_str()))
        .cloned()
        .collect()
}

/// Decide whether a full-id deletion scan is due. A scan runs on any
/// changes poll (the folder is always established by the time
/// `changes_stream` calls this - the inventory pass mints the first
/// cursor) that has either never scanned or is past
/// `FULL_SCAN_INTERVAL_SECS` since the last one.
///
/// The scan is NOT gated on a watermark. A folder whose items carry no
/// `DateTimeReceived` (some calendar/contact items) keeps a `None`
/// watermark forever; gating on the watermark would mean such a folder
/// never reconciles deletions and never picks up no-timestamp additions
/// (the timestamp poll cannot see them). The full scan is those items'
/// only sync path, so it must run regardless of the watermark.
pub(crate) fn scan_due(cursor: &PublicFolderCursor, now: u64) -> bool {
    match cursor.last_full_scan_at {
        None => true,
        Some(last) => now.saturating_sub(last) >= FULL_SCAN_INTERVAL_SECS,
    }
}

/// The next step of a `FindItem` page walk. Distinguishes a clean
/// completion from a stall so the caller can tell a fully-walked folder
/// (safe to checkpoint / diff deletions) from a partial one (must not).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageStep {
    /// Fetch the next page at this offset.
    Continue(u32),
    /// The server marked the last item in range: the walk is COMPLETE.
    Complete,
    /// The walk cannot make progress - the server never advances its
    /// offset (`IncludesLastItemInRange` stays false and the reported /
    /// derived offset does not move), or the page cap was reached before
    /// the last page. The collected set is PARTIAL: the caller must not
    /// advance the watermark, diff deletions, update the baseline, or
    /// checkpoint off it.
    Incomplete,
}

/// Decide the next step of a `FindItem` page walk. Completes when the
/// server marks the last item in range; otherwise advances to the
/// server's own `IndexedPagingOffset` (wire rows counting EVERY class),
/// falling back to `offset + page_size` when the server omits it. Never
/// derive the next offset from the count of PARSED items: the parser drops
/// unhandled classes, so a mixed page's parsed count under-advances
/// (re-fetching rows -> duplicates) and an unsupported-only page parses to
/// zero (a false early termination).
///
/// A non-advancing offset is `Incomplete`, NOT a completion: a server that
/// reports `IncludesLastItemInRange=false` forever (or saturates the
/// offset) has more pages we cannot reach, so treating it as done would
/// permanently skip the tail (and, on a full scan, falsely emit that tail
/// as `Destroyed`). The page cap reached before the last page is the same:
/// `Incomplete`, retry next cycle.
pub(crate) fn page_walk_step(
    includes_last: bool,
    next_offset: Option<u32>,
    offset: u32,
    page_size: u32,
    pages_walked: usize,
    page_cap: usize,
) -> PageStep {
    if includes_last {
        return PageStep::Complete;
    }
    let candidate = next_offset.unwrap_or_else(|| offset.saturating_add(page_size));
    if candidate <= offset {
        return PageStep::Incomplete;
    }
    if pages_walked >= page_cap {
        return PageStep::Incomplete;
    }
    PageStep::Continue(candidate)
}

/// Ids a full scan must emit as additions because the incremental poll did
/// not (or could not) surface them. Emit an id genuinely new to the prior
/// live-id baseline and not already emitted this cycle.
///
/// The class filter is watermark-dependent (condition b):
///
/// - `watermark_none == true`: the incremental poll is UNRESTRICTED, so the
///   scan is a peer entry point. Emit ANY new-to-baseline id regardless of
///   timestamp. This is the only path for a contact/appointment with no
///   `DateTimeReceived`, AND for a timestamped item that first appears in
///   the scan rather than the (same-cycle, earlier-fetched) poll - without
///   it that item would be installed into `live_ids` silently and then
///   filtered as "known" on the next poll, never emitting (bug fix).
/// - `watermark_none == false`: the restricted poll already bounds the
///   timestamped emissions, so the scan only needs to cover items the `>=`
///   restriction structurally cannot see - those lacking `received_at`.
///   Timestamped items are the poll's responsibility and are left untouched
///   (an in-place edit keeping `DateTimeReceived` is still NOT re-emitted -
///   a known poll-model limitation shared by every class, Message included).
///
/// The same-cycle `emitted` set stops the scan double-emitting an id the
/// incremental poll already emitted.
pub(crate) fn scan_additions(
    scan_items: &[EwsItem],
    prior_live: &[String],
    emitted: &std::collections::HashSet<String>,
    watermark_none: bool,
) -> Vec<String> {
    use std::collections::HashSet;
    let live: HashSet<&str> = prior_live.iter().map(String::as_str).collect();
    scan_items
        .iter()
        .filter(|item| watermark_none || item.received_at.is_none())
        .filter(|item| !live.contains(item.item_id.as_str()))
        .filter(|item| !emitted.contains(&item.item_id))
        .map(|item| item.item_id.clone())
        .collect()
}

/// Ids the incremental timestamp poll must emit as `Added`. Always skips
/// ids on the prior boundary second (`prior_boundary`): the inclusive
/// `>=` restriction re-fetches those, and they were already emitted last
/// poll. Additionally, for a **None-watermark** folder the poll is
/// unrestricted and returns the whole established set every cycle, so ids
/// already in the `live_ids` baseline are skipped too - otherwise the
/// folder re-emits its entire inventory on every poll (condition b). Once
/// a watermark is set the restriction bounds the set to genuinely-new
/// items, so the baseline filter is a no-op there and is not applied.
pub(crate) fn incremental_added_ids(
    items: &[EwsItem],
    watermark: Option<&str>,
    prior_boundary: &[String],
    live_ids: &[String],
) -> Vec<String> {
    use std::collections::HashSet;
    let boundary: HashSet<&str> = prior_boundary.iter().map(String::as_str).collect();
    let known: HashSet<&str> = live_ids.iter().map(String::as_str).collect();
    let filter_known = watermark.is_none();
    items
        .iter()
        .filter(|item| !boundary.contains(item.item_id.as_str()))
        .filter(|item| !(filter_known && known.contains(item.item_id.as_str())))
        .map(|item| item.item_id.clone())
        .collect()
}

/// The item classes seen unhandled this poll that have NOT yet been warned
/// about (source order, deduped). The caller emits one `Warning` for these
/// and folds them into the cursor's `warned_classes`, so a class warns at
/// most once per folder lifetime (condition d). `seen` is the union of the
/// incremental poll's and the full scan's unhandled classes.
pub(crate) fn newly_unhandled(seen: &[String], already_warned: &[String]) -> Vec<String> {
    use std::collections::HashSet;
    let known: HashSet<&str> = already_warned.iter().map(String::as_str).collect();
    let mut out: Vec<String> = Vec::new();
    for class in seen {
        if !known.contains(class.as_str()) && !out.contains(class) {
            out.push(class.clone());
        }
    }
    out
}

/// Fold the ids emitted this poll into the live-id deletion baseline,
/// deduped, prior ids first then the newly-added ones in emission order.
///
/// Without this, a None-watermark folder's unrestricted poll re-emits a
/// new untimestamped item on every poll until the next hourly full scan
/// finally installs it (the `live_ids` filter in `incremental_added_ids`
/// only bites once the id is in the baseline). Persisting emitted ids the
/// same cycle closes that (condition b). It is also correct for deletion
/// reconcile: an emitted id is genuinely live, so a later full scan that
/// no longer sees it still diffs it out as `Destroyed`.
///
/// The result is passed through `apply_cap` by the caller, so a fold that
/// crosses `PUBLIC_FOLDER_LIVE_IDS_CAP` degrades exactly like the full-scan
/// path rather than shipping an over-cap baseline with `degraded == false`.
pub(crate) fn extend_live_ids(mut live: Vec<String>, added: &[String]) -> Vec<String> {
    use std::collections::HashSet;
    let mut seen: HashSet<String> = live.iter().cloned().collect();
    for id in added {
        if seen.insert(id.clone()) {
            live.push(id.clone());
        }
    }
    live
}

/// Apply the live-id hard cap to a candidate baseline, returning the
/// baseline to store, the new `degraded` flag, and whether THIS call
/// transitioned the folder into degraded mode (so the caller warns exactly
/// once). At/under the cap the candidate is kept and `degraded` clears; over
/// the cap the baseline empties, `degraded` sets, and the transition flag is
/// true only if the folder was not already degraded. Shared by the full-scan
/// reassignment and the None-watermark incremental fold so both honor the
/// cap and the one-warning-on-transition rule identically.
fn apply_cap(candidate: Vec<String>, was_degraded: bool) -> (Vec<String>, bool, bool) {
    match cap_live_ids(candidate, PUBLIC_FOLDER_LIVE_IDS_CAP) {
        Some(set) => (set, false, false),
        None => (Vec::new(), true, !was_degraded),
    }
}

/// Project an `EwsItem` to an inventory entry, owner-tagged with both
/// the public folder and its content mailbox (the A5a owner-tag
/// pattern, so the consumer maps the scope to its public owner).
///
/// The id is folder-qualified (`encode_public_item_id`): a raw EWS `ItemId`
/// carries no hint of where it lives, so an unqualified id would reach
/// `get_stream` / `open_blob` with nothing to route on and fall through to
/// the Graph REST arm, which cannot address an EWS item at all.
pub(crate) fn item_to_inventory_entry(
    item: &EwsItem,
    folder: &FolderId,
    content_mailbox: &str,
) -> InventoryEntry {
    InventoryEntry {
        id: super::foreign::encode_public_item_id(folder, &item.item_id),
        memberships: vec![
            MembershipScope::Folder(folder.clone()),
            MembershipScope::Mailbox(MailboxId(content_mailbox.to_string())),
        ],
        size: None,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: item
                .change_key
                .clone()
                .map(ServerVersion::ETag)
                .unwrap_or(ServerVersion::Unavailable),
            size: None,
            flags_hash: bifrost_types::canonical_flags_hash([format!("isread={}", item.is_read)]),
        },
        thread_id: None,
        message_id: None,
        references: Vec::new(),
        in_reply_to: None,
    }
}

/// The hierarchy routing to browse with when Autodiscover did not answer
/// `PublicFolderInformation`.
///
/// Anchoring on the caller's own mailbox is what Exchange does when no
/// hierarchy hint is published, and it keeps `anchor_mailbox` a real,
/// non-empty identity - the value that later becomes the owner
/// `MailboxId` on every item this leg emits. An empty anchor would tag
/// every public item with `Mailbox("")`.
///
/// Pure so the degradation decision is unit-pinnable without Autodiscover.
pub(crate) fn hierarchy_routing_fallback(user_email: &str) -> PublicFolderRouting {
    PublicFolderRouting {
        anchor_mailbox: user_email.to_string(),
        // No `InternalRpcClientServer` hint: send no `X-PublicFolderMailbox`
        // rather than inventing a server name.
        public_folder_mailbox: None,
    }
}

/// The routing a discovered public folder is seeded with: its own
/// content-mailbox routing when Autodiscover resolved one, else the
/// hierarchy routing the browse already succeeded with.
///
/// Split out (and pure) because it is the decision that separates "this
/// folder is projected but routes suboptimally" from the prior behavior,
/// "this folder does not exist as far as the consumer is concerned".
pub(crate) fn content_routing_or_hierarchy(
    content: Option<PublicFolderRouting>,
    hierarchy: &PublicFolderRouting,
) -> PublicFolderRouting {
    content.unwrap_or_else(|| hierarchy.clone())
}

/// Read-gate a discovered folder list: keep only folders the caller can
/// read. Advisory at discovery (don't emit an unreadable scope); the
/// authoritative per-operation gate is the live `ErrorAccessDenied`.
/// Pure so the read decision is unit-pinnable over a folder list.
pub(crate) fn readable_folders(folders: Vec<EwsFolder>) -> Vec<EwsFolder> {
    folders
        .into_iter()
        .filter(|folder| folder.effective_rights.read)
        .collect()
}

/// Build a scoped warning for item classes a poll returned that the parser
/// does not yet collect (`Task`, `MeetingRequest`, `PostItem`, ...), so the
/// silent omission is visible to an operator. `None` when nothing was
/// dropped.
fn unhandled_classes_warning(folder_id: &str, unhandled: &[String]) -> Option<Warning> {
    if unhandled.is_empty() {
        return None;
    }
    Some(Warning::support_only(
        WarningKind::OperatorAttentionNeeded,
        format!(
            "public folder {folder_id} contains item classes not yet synced: {}",
            unhandled.join(", "),
        ),
    ))
}

/// Push the change triple for an appearing item: an `Updated`
/// `ObjectChange` (EWS gives no created/updated split, matching Graph
/// delta's convention) plus the two `ScopeChange` `Added` memberships the
/// inventory pass assigns - the `Folder` scope and the content-mailbox
/// owner tag (the A5a pattern the engine's covering rule cannot
/// synthesize).
///
/// The emitted id is folder-qualified, identical to what the inventory
/// projection mints for the same item, so a changed item hydrates through
/// the EWS arm exactly like a backfilled one.
fn emit_item_added(changes: &mut Vec<Change>, item_id: &str, folder: &FolderId, owner: &MailboxId) {
    let id = super::foreign::encode_public_item_id(folder, item_id);
    changes.push(Change::ObjectChange(ObjectChange {
        id: id.clone(),
        kind: ObjectChangeKind::Updated,
    }));
    changes.push(Change::ScopeChange(ScopeChange {
        id: id.clone(),
        membership: MembershipScope::Folder(folder.clone()),
        kind: ScopeChangeKind::Added,
    }));
    changes.push(Change::ScopeChange(ScopeChange {
        id,
        membership: MembershipScope::Mailbox(owner.clone()),
        kind: ScopeChangeKind::Added,
    }));
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn ews_client(account: &GraphAccount) -> Option<EwsClient> {
    account
        .client
        .account_net()
        .map(|net| EwsClient::new(net, account.client.outlook_base()))
}

/// The result of walking a folder's `FindItem` pages.
struct ItemWalk {
    items: Vec<EwsItem>,
    /// Deduped item classes the parser does not collect, in source order.
    unhandled_classes: Vec<String>,
    /// Whether the walk reached the last page. `false` means the walk
    /// stalled or hit the page cap: `items` is PARTIAL and must not drive
    /// the watermark, deletion diff, baseline, or checkpoint. See the
    /// module-level emission/paging contract, condition (e).
    complete: bool,
}

/// Walk every `FindItem` page for a folder, optionally restricted to
/// `since`, collecting all items in received-descending order. Also
/// returns the deduped set of item classes the parser does not collect
/// (surfaced by the caller as a scoped warning) and whether the walk
/// COMPLETED.
///
/// Paging advances off the server's `IndexedPagingOffset` (`page_walk_step`),
/// never off the parsed-item count - the parser omits unhandled classes, so
/// a parsed count would under-advance a mixed page (duplicates) or falsely
/// terminate an unsupported-only page. A stalled offset or a page-cap hit
/// before the last page yields `complete = false`: an incomplete walk the
/// caller must reject rather than treat as a full result.
async fn fetch_all_items(
    ews: &EwsClient,
    folder_id: &str,
    since: Option<&str>,
    routing: &PublicFolderRouting,
) -> Result<ItemWalk, EwsError> {
    let headers = routing.headers();
    let mut all = Vec::new();
    let mut unhandled: Vec<String> = Vec::new();
    let mut offset = 0u32;
    let mut pages = 0usize;
    loop {
        let page = ews
            .find_items(folder_id, since, offset, PUBLIC_FOLDER_PAGE_SIZE, &headers)
            .await?;
        for class in page.unhandled_classes {
            if !unhandled.contains(&class) {
                unhandled.push(class);
            }
        }
        all.extend(page.items);
        pages += 1;
        match page_walk_step(
            page.includes_last,
            page.next_offset,
            offset,
            PUBLIC_FOLDER_PAGE_SIZE,
            pages,
            PUBLIC_FOLDER_PAGE_CAP,
        ) {
            PageStep::Complete => {
                return Ok(ItemWalk {
                    items: all,
                    unhandled_classes: unhandled,
                    complete: true,
                });
            }
            PageStep::Incomplete => {
                // Stalled offset or page cap before the last page. Report
                // the partial set as INCOMPLETE; the caller refuses to
                // advance state off it and retries next cycle.
                return Ok(ItemWalk {
                    items: all,
                    unhandled_classes: unhandled,
                    complete: false,
                });
            }
            PageStep::Continue(next) => offset = next,
        }
    }
}

/// A page walk that could not complete (stalled offset or page cap before
/// the last page). Surfaced as a retryable transient (`Transport(Unsent)`)
/// so the engine re-polls next cycle rather than checkpointing partial
/// state - the walk advanced nothing, so nothing is lost by retrying. Not
/// a wire error: the server answered, we just could not reach the tail.
fn incomplete_walk_error(folder_id: &str) -> EwsError {
    EwsError::Transport(bifrost_net::Error::Network {
        message: format!(
            "public folder {folder_id} page walk incomplete (offset stalled or \
             page cap reached before the last page); retrying next poll"
        ),
        transmission_state: bifrost_types::TransmissionState::Unsent,
        source: None,
    })
}

// ── Discovery ───────────────────────────────────────────────

/// Browse the public-folder hierarchy, seeding `routing_map` (content-mailbox
/// routing) and `public_folder_meta` (display name, folder class, parent,
/// effective rights) for every readable folder, and emit a
/// `CursorScope::Folder` ONLY for the folders the configured
/// [`PublicFolderScope`] allows.
///
/// The browse/seed half is deliberately unconditional: `containers_list`
/// projects the full readable hierarchy off these two maps, so a consumer can
/// see (and later pin) a folder it is not yet syncing. The scope half is
/// allowlisted, because an organization can carry thousands of public folders
/// holding millions of items and syncing all of them uninvited is exactly the
/// defect a silently-permissive default produces.
///
/// Both maps are filled here, synchronously under `SyncEngine::attach`'s
/// discovery call, which is what makes them populated by the time
/// `containers_list` runs. Do not defer either seeding to a lazier point.
///
/// Opt-in (`with_public_folders`); a per-folder Autodiscover/permission
/// failure is skipped with a scoped warning, never a discovery-wide failure
/// (A5a per-mailbox robustness). On a routing-resolution failure (no
/// public-folder hierarchy at all) the whole public-folder leg is skipped with
/// a single warning - the primary and shared mailboxes still discover.
pub(crate) async fn discover_public_folder_scopes(
    account: &GraphAccount,
    scope_policy: &PublicFolderScope,
) -> (Vec<CursorScope>, Vec<Warning>) {
    let mut scopes = Vec::new();
    let mut warnings = Vec::new();

    let Some(user_email) = account.user_email.clone() else {
        warnings.push(Warning::support_only(
            WarningKind::OperatorAttentionNeeded,
            "public-folder discovery skipped: no account email available".to_string(),
        ));
        return (scopes, warnings);
    };
    // Split the domain off the right of `@`; a malformed (no-`@`) email
    // has no usable domain (the prior `rsplit('@').next()` returned the
    // whole string, producing a bogus `guid@whole-email` replica SMTP).
    let Some(domain) = user_email.rsplit_once('@').map(|(_, d)| d.to_string()) else {
        warnings.push(Warning::support_only(
            WarningKind::OperatorAttentionNeeded,
            "public-folder discovery skipped: account email has no domain".to_string(),
        ));
        return (scopes, warnings);
    };

    // A missing Autodiscover hierarchy hint is a DEGRADATION, not a reason to
    // abandon the leg. `PublicFolderInformation` only names a preferred
    // hierarchy mailbox; the caller's own mailbox is a valid `X-AnchorMailbox`
    // for `publicfoldersroot`, and a deployment (or a harness) that does not
    // answer `GetUserSettings` still serves the hierarchy. Returning here
    // meant a single unanswered Autodiscover setting produced zero public
    // containers with nothing but a support-only warning to show for it.
    let routing = match account.discover_public_folder_routing(&user_email).await {
        Ok(routing) => routing,
        Err(_) => {
            warnings.push(Warning::support_only(
                WarningKind::OperatorAttentionNeeded,
                "public-folder hierarchy routing unavailable; browsing anchored on the \
                 account mailbox"
                    .to_string(),
            ));
            hierarchy_routing_fallback(&user_email)
        }
    };

    let Some(ews) = ews_client(account) else {
        warnings.push(Warning::support_only(
            WarningKind::OperatorAttentionNeeded,
            "public-folder discovery skipped: EWS account net not attached".to_string(),
        ));
        return (scopes, warnings);
    };

    // Browse the hierarchy from the root using hierarchy headers (the
    // routing's anchor/server pair). FindFolder is a hierarchy op. The
    // hierarchy is a tree, so walk it breadth-first: each folder with
    // children re-enters the worklist as a parent to browse. The depth
    // bound is a defensive cap against a pathological (cyclic) hierarchy.
    let hierarchy_headers = routing.headers();
    let mut to_visit: Vec<String> = vec!["publicfoldersroot".to_string()];
    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut steps = 0usize;

    while let Some(parent_id) = to_visit.pop() {
        if !visited.insert(parent_id.clone()) {
            continue;
        }
        // The root is a distinguished id, not a real folder, so a child of
        // it has no projectable parent container.
        let parent_container =
            (parent_id != "publicfoldersroot").then(|| FolderId(parent_id.clone()));
        steps += 1;
        if steps > PUBLIC_FOLDER_BROWSE_STEP_CAP {
            warnings.push(Warning::support_only(
                WarningKind::OperatorAttentionNeeded,
                "public-folder discovery truncated: hierarchy browse step cap reached".to_string(),
            ));
            break;
        }

        let children = match ews.find_folder(&parent_id, &hierarchy_headers).await {
            Ok(children) => children,
            Err(_) => {
                // The root failing is fatal to the whole leg (no
                // hierarchy at all); a sub-folder failing is skipped.
                let msg = if parent_id == "publicfoldersroot" {
                    "public-folder discovery skipped: hierarchy browse failed".to_string()
                } else {
                    format!("public folder subtree {parent_id} skipped: browse failed")
                };
                warnings.push(Warning::support_only(
                    WarningKind::OperatorAttentionNeeded,
                    msg,
                ));
                if parent_id == "publicfoldersroot" {
                    return (scopes, warnings);
                }
                continue;
            }
        };

        for folder in readable_folders(children) {
            // Recurse into branches before resolving routing, so a
            // routing failure on one folder never prunes its subtree.
            if folder.child_folder_count > 0 {
                to_visit.push(folder.folder_id.clone());
            }
            // Seeding is UNCONDITIONAL (that is the documented contract:
            // `containers_list` projects the full readable hierarchy off these
            // maps). Content-mailbox routing is a per-folder optimization -
            // it needs `PR_REPLICA_LIST` plus one Autodiscover round-trip per
            // replica GUID - so gating the seed on it, as this previously did,
            // silently produced an EMPTY routing map (and therefore zero public
            // containers and zero pinned scopes) on any deployment that does
            // not serve that chain. Fall back to the hierarchy routing, which
            // is the routing the browse itself just succeeded with.
            let content_routing =
                resolve_content_routing(account, &ews, &folder, &hierarchy_headers, &domain)
                    .await
                    .ok();
            if content_routing.is_none() {
                warnings.push(Warning::support_only(
                    WarningKind::OperatorAttentionNeeded,
                    format!(
                        "public folder {} content-mailbox routing unresolved; \
                         routing content ops on the hierarchy mailbox",
                        folder.display_name
                    ),
                ));
            }
            if let Some(scope) = seed_and_scope(
                account,
                scope_policy,
                &folder,
                parent_container.clone(),
                content_routing_or_hierarchy(content_routing, &routing),
            )
            .await
            {
                scopes.push(scope);
            }
        }
    }

    (scopes, warnings)
}

/// Seed one discovered public folder into BOTH discovery maps and decide
/// whether it becomes a cursor scope.
///
/// The seeding is unconditional (that is what makes the folder visible in
/// `containers_list`); only the scope is allowlisted. Split out of the browse
/// loop so the seed-vs-sync split is unit-pinnable without a live EWS server.
async fn seed_and_scope(
    account: &GraphAccount,
    scope_policy: &PublicFolderScope,
    folder: &EwsFolder,
    parent: Option<FolderId>,
    routing: PublicFolderRouting,
) -> Option<CursorScope> {
    let folder_id = FolderId(folder.folder_id.clone());
    account
        .routing_map
        .write()
        .await
        .insert(folder_id.clone(), routing);
    account.public_folder_meta.write().await.insert(
        folder_id.clone(),
        PublicFolderMeta {
            display_name: folder.display_name.clone(),
            folder_class: folder.folder_class.clone(),
            parent,
            effective_rights: folder.effective_rights.clone(),
        },
    );
    scope_policy
        .syncs(&folder_id)
        .then_some(CursorScope::Folder(folder_id))
}

/// Resolve a single public folder's content-mailbox routing: fetch its
/// `PR_REPLICA_LIST`, build the synthetic replica SMTP, and Autodiscover
/// the real content mailbox. The content ops route by that mailbox on
/// BOTH `X-AnchorMailbox` and `X-PublicFolderMailbox`.
async fn resolve_content_routing(
    account: &GraphAccount,
    ews: &EwsClient,
    folder: &EwsFolder,
    hierarchy_headers: &crate::ews::EwsHeaders,
    domain: &str,
) -> Result<PublicFolderRouting, ()> {
    // The replica list may already be on the FindFolder result, but
    // FindFolder does not request 0x6698; GetFolder does.
    let detailed = ews
        .get_folder(&folder.folder_id, hierarchy_headers)
        .await
        .map_err(|_| ())?;
    let replica = detailed.replica_list.ok_or(())?;
    // The bytes are already base64-decoded by the GetFolder parser; parse
    // them straight rather than re-encode-then-decode.
    let guids = crate::ews::decode_replica_bytes(&replica);
    if guids.is_empty() {
        return Err(());
    }
    // A public folder can be replicated across several content mailboxes;
    // try each replica GUID's Autodiscover lookup until one resolves
    // rather than committing to an arbitrary first entry.
    for guid in &guids {
        let stripped = guid.trim_matches(['{', '}']);
        let replica_smtp = super::autodiscover::construct_replica_smtp(stripped, domain);
        if let Ok(content_mailbox) = account.discover_content_mailbox(&replica_smtp).await {
            return Ok(PublicFolderRouting {
                anchor_mailbox: content_mailbox.clone(),
                public_folder_mailbox: Some(content_mailbox),
            });
        }
    }
    Err(())
}

// ── Streams ─────────────────────────────────────────────────

pub(crate) fn public_folder_inventory_stream(
    account: GraphAccount,
    scope: CursorScope,
) -> AccountStream<bifrost_types::InventoryEvent> {
    Box::pin(async_stream::stream! {
        let CursorScope::Folder(folder) = scope.clone() else {
            // Routed here only for `CursorScope::Folder` in the routing
            // map; any other shape is a wiring bug.
            let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                .with_scope(ErrorScope::Cursor(scope.clone()));
            yield bifrost_types::InventoryEvent::Terminated(cursor_error_to_account_error(
                super::cursor::CursorError::Unsupported, ctx));
            return;
        };

        let Some(routing) = account.public_folder_routing(&folder).await else {
            let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                .with_scope(ErrorScope::Cursor(scope.clone()));
            yield bifrost_types::InventoryEvent::Terminated(cursor_error_to_account_error(
                super::cursor::CursorError::Unsupported, ctx));
            return;
        };
        let owner = MailboxId(routing.anchor_mailbox.clone());

        let Some(ews) = ews_client(&account) else {
            let ctx = GraphErrorContext::ews(AccountOperation::SyncInventory)
                .with_scope(ErrorScope::Cursor(scope.clone()));
            yield bifrost_types::InventoryEvent::Terminated(ews_shared_scope_error(
                // Not-yet-attached is a transient lifecycle condition
                // (the engine reopens), not a malformed wire response;
                // route it as a retryable Transport(Unsent) rather than a
                // terminal Protocol(ParseFailed) that kills the scope.
                EwsError::Transport(bifrost_net::Error::Network {
                    message: "EWS account net not attached".to_string(),
                    transmission_state: bifrost_types::TransmissionState::Unsent,
                    source: None,
                }),
                &scope,
                None,
                ctx,
            ));
            return;
        };

        let walk = match fetch_all_items(&ews, &folder.0, None, &routing).await {
            Ok(walk) => walk,
            Err(error) => {
                let ctx = GraphErrorContext::ews(AccountOperation::SyncInventory)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield bifrost_types::InventoryEvent::Terminated(ews_shared_scope_error(
                    error, &scope, Some(&owner), ctx));
                return;
            }
        };
        // An incomplete establish walk would seed a PARTIAL deletion
        // baseline, so the first incremental's reconcile would diff the
        // real inventory against a partial scan and falsely emit the tail
        // as Destroyed. Refuse to establish off it; retry next cycle.
        if !walk.complete {
            let ctx = GraphErrorContext::ews(AccountOperation::SyncInventory)
                .with_scope(ErrorScope::Cursor(scope.clone()));
            yield bifrost_types::InventoryEvent::Terminated(ews_shared_scope_error(
                incomplete_walk_error(&folder.0), &scope, Some(&owner), ctx));
            return;
        }
        let ItemWalk { items, unhandled_classes: unhandled, .. } = walk;
        if let Some(warning) = unhandled_classes_warning(&folder.0, &unhandled) {
            yield bifrost_types::InventoryEvent::Warning(warning);
        }

        let watermark = advance_watermark(None, &items);
        let scan_set: Vec<String> = items.iter().map(|i| i.item_id.clone()).collect();
        let entries: Vec<InventoryEntry> = items
            .iter()
            .map(|item| item_to_inventory_entry(item, &folder, &routing.anchor_mailbox))
            .collect();

        // The first establish carries the full scanned id set as the
        // deletion baseline (capped) but sets `last_full_scan_at = None`
        // so the FIRST incremental does its inaugural reconcile against
        // this snapshot. `live_ids` empty (over cap) degrades to
        // additions-only - emit the same one-warning-on-degrade the
        // changes path emits, rather than silently entering the degraded
        // mode (the documented one-warning-on-degrade contract).
        let (live_ids, degraded) = match cap_live_ids(scan_set, PUBLIC_FOLDER_LIVE_IDS_CAP) {
            Some(set) => (set, false),
            None => (Vec::new(), true),
        };
        if degraded {
            yield bifrost_types::InventoryEvent::Warning(Warning::support_only(
                WarningKind::StrategyDowngraded,
                format!(
                    "public folder {} exceeds {} items; deletion reconcile \
                     disabled, syncing additions only",
                    folder.0, PUBLIC_FOLDER_LIVE_IDS_CAP,
                ),
            ));
        }
        let boundary_ids = boundary_ids_at(watermark.as_deref(), &items);
        // Seed the warned-class set with whatever this establish walk
        // already warned about, so the first changes poll does not
        // re-warn the same classes.
        let first = PublicFolderCursor {
            folder_id: folder.0.clone(),
            routing: routing.clone(),
            watermark,
            last_full_scan_at: None,
            live_ids,
            boundary_ids,
            degraded,
            warned_classes: unhandled.clone(),
        };
        let cursor = match encode_cursor(scope.clone(), GraphCursorPayload::public_folder(first)) {
            Ok(cursor) => cursor,
            Err(error) => {
                let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield bifrost_types::InventoryEvent::Terminated(cursor_error_to_account_error(error, ctx));
                return;
            }
        };

        let checkpoint = Checkpoint::Change(cursor.clone());
        yield super::inventory::inventory_batch(
            entries,
            PageBoundary::Final,
            Some(cursor),
            bifrost_types::InventoryCoverageReport::complete(
                bifrost_types::CoverageDomain::full(scope.clone()),
            ),
            // The public-folder arm reads over EWS, and `EwsClient`
            // composes `AccountNet` directly rather than routing through
            // `GraphClient`'s wire funnel, so no `GraphClient`-level
            // accumulator can observe its traffic. Reported as zero
            // rather than guessed.
            0,
        );
        yield bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion::complete(
            bifrost_types::CoverageDomain::full(scope.clone()),
            Some(checkpoint),
        ));
    })
}

/// The outcome of reducing one public-folder changes poll. Keeps the whole
/// state machine (conditions a-e) pure and unit-pinnable: the async stream
/// only fetches the page walks and enacts this decision.
enum PollOutcome {
    /// An incomplete walk (the incremental poll or the full scan). Advance
    /// nothing, checkpoint nothing: the caller terminates retryably so the
    /// engine re-polls next cycle (condition e). No `Added`, no `Destroyed`,
    /// no watermark/baseline move.
    Retry,
    /// A completed poll: emit `changes` and any `warnings`, then checkpoint
    /// `cursor` (the advanced state).
    Apply {
        changes: Vec<Change>,
        warnings: Vec<Warning>,
        cursor: Box<PublicFolderCursor>,
    },
}

/// Reduce one changes poll to a `PollOutcome`. Pure: the incremental page
/// walk (`poll`) and the optional full-scan walk (`scan`, `Some` only when
/// a deletion reconcile was due AND fetched) are already-fetched data, so
/// every branch of the emission/paging contract is testable without a live
/// EWS server. `now` is the reconcile clock stamped into `last_full_scan_at`.
fn reduce_public_folder_poll(
    mut pf: PublicFolderCursor,
    poll: &ItemWalk,
    scan: Option<&ItemWalk>,
    now: u64,
) -> PollOutcome {
    use std::collections::HashSet;

    // (e) An incomplete incremental walk skips the tail it did not reach;
    // advancing off it would drop those pages forever. Retry next cycle.
    if !poll.complete {
        return PollOutcome::Retry;
    }

    let folder = FolderId(pf.folder_id.clone());
    let owner = MailboxId(pf.routing.anchor_mailbox.clone());
    let mut changes: Vec<Change> = Vec::new();
    let mut warnings: Vec<Warning> = Vec::new();
    // Ids emitted this poll, so the deletion-scan additions pass does not
    // double-emit them.
    let mut emitted: HashSet<String> = HashSet::new();
    // Unhandled classes observed across BOTH the incremental poll and the
    // full scan; warned about once at the end (condition d).
    let mut unhandled_seen: Vec<String> = Vec::new();
    for class in &poll.unhandled_classes {
        if !unhandled_seen.contains(class) {
            unhandled_seen.push(class.clone());
        }
    }

    // 1. Incremental emissions. Skip the prior boundary second (re-fetched
    // by the inclusive `>=`) and, for a None-watermark folder, ids already
    // in the baseline (condition b).
    let incremental_ids = incremental_added_ids(
        &poll.items,
        pf.watermark.as_deref(),
        &pf.boundary_ids,
        &pf.live_ids,
    );
    for id in &incremental_ids {
        emitted.insert(id.clone());
        emit_item_added(&mut changes, id, &folder, &owner);
    }
    // The UNTIMESTAMPED ids emitted this cycle (only ever non-empty in
    // None-watermark mode, where the poll is unrestricted - a restricted
    // `>=` poll never returns an item lacking `DateTimeReceived`). These
    // are the ones the next unrestricted poll would re-emit, so they are
    // folded into the persisted baseline below. Timestamped emissions are
    // deliberately NOT folded: the advanced watermark/boundary already stop
    // them re-emitting, and folding them would bypass the live-id cap.
    let untimestamped_emitted: Vec<String> = {
        let untimestamped: HashSet<&str> = poll
            .items
            .iter()
            .filter(|i| i.received_at.is_none())
            .map(|i| i.item_id.as_str())
            .collect();
        incremental_ids
            .iter()
            .filter(|id| untimestamped.contains(id.as_str()))
            .cloned()
            .collect()
    };
    pf.watermark = advance_watermark(pf.watermark.take(), &poll.items);
    pf.boundary_ids = boundary_ids_at(pf.watermark.as_deref(), &poll.items);

    // 2. Throttled deletion reconcile (only when a scan was due + fetched).
    let mut degrade_transition = false;
    if let Some(scan) = scan {
        // (e) A PARTIAL scan would falsely emit the unvisited tail as
        // Destroyed and drop it from the baseline. Reject the whole poll.
        if !scan.complete {
            return PollOutcome::Retry;
        }
        for class in &scan.unhandled_classes {
            if !unhandled_seen.contains(class) {
                unhandled_seen.push(class.clone());
            }
        }
        let scan_set: Vec<String> = scan.items.iter().map(|i| i.item_id.clone()).collect();
        // A degraded (empty snapshot, was over cap) folder skips the
        // deletion diff and emits no Destroyed.
        if !pf.degraded {
            for gone in deleted_ids(&pf.live_ids, &scan_set) {
                // Same folder-qualified id form the additions use, so the
                // consumer's destroy matches the id it stored.
                changes.push(Change::ObjectChange(ObjectChange {
                    id: super::foreign::encode_public_item_id(&folder, &gone),
                    kind: ObjectChangeKind::Destroyed,
                }));
            }
        }
        // Additions the incremental poll did not cover. While the watermark
        // is None the scan is a peer entry point and emits ANY new-to-baseline
        // id regardless of timestamp (condition b - the fix for a timestamped
        // item that first surfaces in the scan); once a watermark is set the
        // restricted poll owns timestamped items, so the scan only covers the
        // untimestamped ones. Runs REGARDLESS of `degraded` (condition c), and
        // before the baseline is reassigned below, so a recovery scan never
        // silently absorbs a new item. `pf.live_ids` here is still the prior
        // baseline (the incremental fold happens only when no scan runs).
        for added in scan_additions(&scan.items, &pf.live_ids, &emitted, pf.watermark.is_none()) {
            emitted.insert(added.clone());
            emit_item_added(&mut changes, &added, &folder, &owner);
        }
        // The full scan is authoritative for the baseline (it already
        // includes this cycle's incremental emissions), so reassign from the
        // scan set and cap/degrade uniformly.
        let (live, degraded, warn) = apply_cap(scan_set, pf.degraded);
        pf.live_ids = live;
        pf.degraded = degraded;
        degrade_transition = warn;
        pf.last_full_scan_at = Some(now);
    } else if !pf.degraded && !untimestamped_emitted.is_empty() {
        // No scan this cycle: persist the untimestamped emissions into the
        // baseline so the next unrestricted poll does not re-emit them
        // (condition b), applying the SAME cap/degrade transition the scan
        // path uses. Skipped while degraded (the baseline stays empty by
        // contract; only a full scan recovers from degraded).
        let candidate = extend_live_ids(std::mem::take(&mut pf.live_ids), &untimestamped_emitted);
        let (live, degraded, warn) = apply_cap(candidate, pf.degraded);
        pf.live_ids = live;
        pf.degraded = degraded;
        degrade_transition = warn;
    }

    // (d) Warn once per folder lifetime on newly-seen unhandled classes,
    // pooling the incremental poll's and the full scan's observations.
    let newly = newly_unhandled(&unhandled_seen, &pf.warned_classes);
    if let Some(warning) = unhandled_classes_warning(&pf.folder_id, &newly) {
        pf.warned_classes.extend(newly);
        warnings.push(warning);
    }
    if degrade_transition {
        warnings.push(Warning::support_only(
            WarningKind::StrategyDowngraded,
            format!(
                "public folder {} exceeds {} items; deletion reconcile \
                 disabled, syncing additions only",
                pf.folder_id, PUBLIC_FOLDER_LIVE_IDS_CAP,
            ),
        ));
    }

    PollOutcome::Apply {
        changes,
        warnings,
        cursor: Box::new(pf),
    }
}

pub(crate) fn public_folder_changes_stream(
    account: GraphAccount,
    cursor: ChangeCursor,
) -> AccountStream<SyncEvent<Change>> {
    Box::pin(async_stream::stream! {
        let scope = cursor.scope.clone();
        let payload = match decode_cursor(&cursor) {
            Ok(payload) => payload,
            Err(error) => {
                let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };
        // Guard that the cursor's scope names the folder the payload polls,
        // mirroring the delta `changes_stream` (changes.rs). Without it a
        // mismatched cursor would poll the payload's folder while
        // checkpointing under the request's scope. `scope_for_kind` maps a
        // `PublicFolder` payload back to its `CursorScope::Folder`.
        if !super::inventory::scope_matches_payload(&scope, &payload) {
            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(scope.clone()));
            yield SyncEvent::Terminated(cursor_error_to_account_error(
                super::cursor::CursorError::SchemaIncompatible, ctx));
            yield SyncEvent::Done(None);
            return;
        }
        let GraphCursorPayload { kind: GraphCursorKind::PublicFolder(pf), .. } = payload else {
            // changes_stream routes only `PublicFolder` payloads here.
            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(scope.clone()));
            yield SyncEvent::Terminated(cursor_error_to_account_error(
                super::cursor::CursorError::SchemaIncompatible, ctx));
            yield SyncEvent::Done(None);
            return;
        };

        let owner = MailboxId(pf.routing.anchor_mailbox.clone());
        let folder = FolderId(pf.folder_id.clone());

        let Some(ews) = ews_client(&account) else {
            let ctx = GraphErrorContext::ews(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(scope.clone()));
            yield SyncEvent::Terminated(ews_shared_scope_error(
                // Not-yet-attached is a transient lifecycle condition
                // (the engine reopens), not a malformed wire response;
                // route it as a retryable Transport(Unsent) rather than a
                // terminal Protocol(ParseFailed) that kills the scope.
                EwsError::Transport(bifrost_net::Error::Network {
                    message: "EWS account net not attached".to_string(),
                    transmission_state: bifrost_types::TransmissionState::Unsent,
                    source: None,
                }),
                &scope,
                None,
                ctx,
            ));
            yield SyncEvent::Done(None);
            return;
        };

        // 1. Incremental timestamp poll: items at/after the watermark. An
        // incomplete walk (or an incomplete scan below) drives no state -
        // terminate retryably and re-poll next cycle (condition e). The
        // async stream only fetches; `reduce_public_folder_poll` owns the
        // state machine.
        let poll = match fetch_all_items(
            &ews, &pf.folder_id, pf.watermark.as_deref(), &pf.routing,
        ).await {
            Ok(walk) => walk,
            Err(error) => {
                let ctx = GraphErrorContext::ews(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield SyncEvent::Terminated(ews_shared_scope_error(
                    error, &scope, Some(&owner), ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };

        // 2. Throttled deletion reconcile: fetch the full scan only when it
        // is due AND the incremental poll completed (a stalled poll retries
        // before spending a full scan).
        let now = now_unix_secs();
        let scan = if poll.complete && scan_due(&pf, now) {
            match fetch_all_items(&ews, &pf.folder_id, None, &pf.routing).await {
                Ok(walk) => Some(walk),
                Err(error) => {
                    let ctx = GraphErrorContext::ews(AccountOperation::SyncChanges)
                        .with_scope(ErrorScope::Cursor(scope.clone()));
                    yield SyncEvent::Terminated(ews_shared_scope_error(
                        error, &scope, Some(&owner), ctx));
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        } else {
            None
        };

        match reduce_public_folder_poll(pf, &poll, scan.as_ref(), now) {
            PollOutcome::Retry => {
                // An incomplete walk: no batch, no checkpoint. Retry next
                // cycle (the folder id is stable across the moved cursor).
                let ctx = GraphErrorContext::ews(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield SyncEvent::Terminated(ews_shared_scope_error(
                    incomplete_walk_error(&folder.0), &scope, Some(&owner), ctx));
                yield SyncEvent::Done(None);
                return;
            }
            PollOutcome::Apply { changes, warnings, cursor: advanced_pf } => {
                for warning in warnings {
                    yield SyncEvent::Warning(warning);
                }
                // Checkpoint the advanced cursor. A no-change poll re-emits
                // the same cursor; the engine's adaptive cadence backs off.
                let advanced = match encode_cursor(
                    scope.clone(), GraphCursorPayload::public_folder(*advanced_pf),
                ) {
                    Ok(cursor) => cursor,
                    Err(error) => {
                        let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                            .with_scope(ErrorScope::Cursor(scope.clone()));
                        yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                        yield SyncEvent::Done(None);
                        return;
                    }
                };
                let checkpoint = Checkpoint::Change(advanced.clone());
                // EWS-served: see the inventory arm above - the byte
                // accounting seam is on `GraphClient`, which this path
                // does not use.
                yield batch(changes, PageBoundary::Final, Some(advanced), 0);
                yield SyncEvent::Done(Some(checkpoint));
            }
        }
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // Discovery must project a folder it browsed successfully even when the
    // per-folder content-mailbox chain (PR_REPLICA_LIST -> Autodiscover) does
    // not resolve. The prior behavior dropped such a folder entirely, so a
    // deployment without that chain saw an empty routing map, zero public
    // containers, and zero pinned scopes.
    #[test]
    fn unresolved_content_routing_falls_back_to_the_hierarchy_routing() {
        let hierarchy = PublicFolderRouting {
            anchor_mailbox: "hierarchy@contoso.com".to_string(),
            public_folder_mailbox: Some("server01.contoso.com".to_string()),
        };
        let fallback = content_routing_or_hierarchy(None, &hierarchy);
        assert_eq!(fallback.anchor_mailbox, "hierarchy@contoso.com");
        assert_eq!(
            fallback.public_folder_mailbox.as_deref(),
            Some("server01.contoso.com")
        );
        // Resolved content routing still wins - the fallback never overrides
        // a real answer.
        let content = PublicFolderRouting {
            anchor_mailbox: "content@contoso.com".to_string(),
            public_folder_mailbox: Some("content@contoso.com".to_string()),
        };
        assert_eq!(
            content_routing_or_hierarchy(Some(content), &hierarchy).anchor_mailbox,
            "content@contoso.com"
        );
    }

    // An unanswered `PublicFolderInformation` degrades the anchor to the
    // account's own mailbox rather than abandoning the whole leg. The anchor
    // must stay a real identity: it becomes the owner `MailboxId` on every
    // item this leg emits.
    #[test]
    fn missing_hierarchy_hint_anchors_on_the_account_mailbox() {
        let routing = hierarchy_routing_fallback("user@contoso.com");
        assert_eq!(routing.anchor_mailbox, "user@contoso.com");
        assert!(routing.public_folder_mailbox.is_none());
        assert!(!routing.anchor_mailbox.is_empty());
        // The headers it materializes carry the anchor and omit the
        // public-folder mailbox rather than sending an invented server.
        let pairs = routing.headers().pairs();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, "X-AnchorMailbox");
    }

    fn item(id: &str, received: Option<&str>, is_read: bool) -> EwsItem {
        EwsItem {
            item_id: id.to_string(),
            change_key: Some(format!("ck-{id}")),
            subject: None,
            sender_email: None,
            sender_name: None,
            received_at: received.map(str::to_string),
            body_preview: None,
            body_html: None,
            is_read,
            item_class: "IPM.Note".to_string(),
            to_recipients: Vec::new(),
            cc_recipients: Vec::new(),
            attachments: Vec::new(),
        }
    }

    /// The folder-qualified `ObjectId` the emission path mints for a native
    /// EWS item id in the test folder. Emissions carry the folder so the
    /// hydration path can route the item onto EWS `GetItem`; the cursor's
    /// own `live_ids` / `boundary_ids` stay native.
    fn emitted_id(native: &str) -> bifrost_types::ObjectId {
        super::super::foreign::encode_public_item_id(&FolderId("AAMkPF=".to_string()), native)
    }

    fn cursor(
        watermark: Option<&str>,
        last_scan: Option<u64>,
        live: &[&str],
    ) -> PublicFolderCursor {
        PublicFolderCursor {
            folder_id: "AAMkPF=".to_string(),
            routing: PublicFolderRouting {
                anchor_mailbox: "content@contoso.com".to_string(),
                public_folder_mailbox: Some("pf@contoso.com".to_string()),
            },
            watermark: watermark.map(str::to_string),
            last_full_scan_at: last_scan,
            live_ids: live.iter().map(|s| (*s).to_string()).collect(),
            boundary_ids: Vec::new(),
            degraded: false,
            warned_classes: Vec::new(),
        }
    }

    // A None-watermark folder already in the over-cap degraded mode: empty
    // baseline, `degraded = true`, previously scanned.
    fn cursor_degraded() -> PublicFolderCursor {
        PublicFolderCursor {
            degraded: true,
            ..cursor(None, Some(2_000_000), &[])
        }
    }

    #[test]
    fn watermark_advances_to_newest_received() {
        let items = vec![
            item("a", Some("2026-02-01T00:00:00Z"), false),
            item("b", Some("2026-03-05T12:00:00Z"), false),
            item("c", Some("2026-01-10T00:00:00Z"), false),
        ];
        assert_eq!(
            advance_watermark(None, &items).as_deref(),
            Some("2026-03-05T12:00:00Z")
        );
        // A prior watermark newer than the batch holds.
        assert_eq!(
            advance_watermark(Some("2026-12-01T00:00:00Z".to_string()), &items).as_deref(),
            Some("2026-12-01T00:00:00Z")
        );
        // Empty batch leaves the prior watermark untouched.
        assert_eq!(advance_watermark(None, &[]), None);
    }

    #[test]
    fn watermark_orders_by_instant_not_lexically() {
        // Mixed fractional-second precision: `...00.5Z` is chronologically
        // NEWER than `...00Z`, but lexically `'Z'`(0x5A) > `'.'`(0x2E)
        // would pick `...00Z`. The instant comparison must pick the
        // fractional one.
        let items = vec![
            item("a", Some("2026-03-01T10:00:00Z"), false),
            item("b", Some("2026-03-01T10:00:00.5Z"), false),
        ];
        assert_eq!(
            advance_watermark(None, &items).as_deref(),
            Some("2026-03-01T10:00:00.5Z")
        );
        // A prior fractional watermark must not regress to a whole-second
        // value that is chronologically older.
        assert_eq!(
            advance_watermark(
                Some("2026-03-01T10:00:00.9Z".to_string()),
                &[item("c", Some("2026-03-01T10:00:00Z"), false)],
            )
            .as_deref(),
            Some("2026-03-01T10:00:00.9Z")
        );
    }

    #[test]
    fn boundary_ids_collects_items_at_watermark() {
        let items = vec![
            item("old", Some("2026-03-01T09:00:00Z"), false),
            item("edge1", Some("2026-03-05T12:00:00Z"), false),
            item("edge2", Some("2026-03-05T12:00:00Z"), false),
        ];
        let wm = advance_watermark(None, &items);
        let boundary = boundary_ids_at(wm.as_deref(), &items);
        assert_eq!(boundary, vec!["edge1".to_string(), "edge2".to_string()]);
        // No watermark -> no boundary set.
        assert!(boundary_ids_at(None, &items).is_empty());
    }

    #[test]
    fn deletion_scan_diffs_against_snapshot() {
        let prior = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let scan = vec!["a".to_string(), "c".to_string()];
        assert_eq!(deleted_ids(&prior, &scan), vec!["b".to_string()]);
        // Nothing deleted -> empty.
        assert!(deleted_ids(&prior, &prior).is_empty());
    }

    #[test]
    fn never_scanned_cursor_is_due_even_without_watermark() {
        // `scan_due` is only ever called from `changes_stream`, where the
        // folder is already established. A never-scanned cursor is due even
        // with a `None` watermark - a no-timestamp folder keeps `None`
        // forever, and the scan is its only deletion-reconcile and
        // no-timestamp-addition path, so it MUST run.
        assert!(scan_due(&cursor(None, None, &[]), 10_000));
        assert!(scan_due(&cursor(None, None, &["a"]), 10_000));
    }

    #[test]
    fn none_watermark_folder_still_reconciles_after_interval() {
        // A no-timestamp folder (watermark None) that scanned recently is
        // throttled, and becomes due again past the interval - the
        // watermark never gates the scan.
        let now = 1_700_000_000;
        assert!(!scan_due(&cursor(None, Some(now - 600), &["a"]), now));
        assert!(scan_due(&cursor(None, Some(now - 3700), &["a"]), now));
    }

    #[test]
    fn page_walk_step_drives_off_server_rows() {
        // Last page -> COMPLETE.
        assert_eq!(
            page_walk_step(true, Some(200), 100, 100, 1, 1_000),
            PageStep::Complete
        );
        // Not last, server offset present -> continue at it (server wire
        // rows, NOT the parsed-item count).
        assert_eq!(
            page_walk_step(false, Some(250), 100, 100, 1, 1_000),
            PageStep::Continue(250)
        );
        // Not last, server omitted the offset -> advance by a full page.
        assert_eq!(
            page_walk_step(false, None, 100, 100, 1, 1_000),
            PageStep::Continue(200)
        );
    }

    #[test]
    fn page_walk_step_stall_and_cap_are_incomplete() {
        // A non-advancing server offset is INCOMPLETE, not a completion:
        // the tail is unreachable, so the caller must not checkpoint/diff.
        assert_eq!(
            page_walk_step(false, Some(100), 100, 100, 1, 1_000),
            PageStep::Incomplete
        );
        assert_eq!(
            page_walk_step(false, Some(50), 100, 100, 1, 1_000),
            PageStep::Incomplete
        );
        // Page cap reached before the last page -> INCOMPLETE, even though
        // the offset would otherwise advance.
        assert_eq!(
            page_walk_step(false, Some(1_000), 900, 100, 3, 3),
            PageStep::Incomplete
        );
        // Last page on the final allowed page -> COMPLETE (cap not a stall).
        assert_eq!(
            page_walk_step(true, None, 900, 100, 3, 3),
            PageStep::Complete
        );
    }

    #[test]
    fn incremental_added_ids_skips_boundary_and_baseline() {
        // Timestamped (watermark = Some): the baseline filter is a no-op;
        // only the prior boundary second is skipped.
        let items = vec![
            item("new1", Some("2026-03-05T12:00:00Z"), false),
            item("edge", Some("2026-03-05T12:00:00Z"), false),
        ];
        let boundary = vec!["edge".to_string()];
        let live = vec!["new1".to_string()]; // present, but must NOT filter here
        assert_eq!(
            incremental_added_ids(&items, Some("2026-03-05T12:00:00Z"), &boundary, &live),
            vec!["new1".to_string()]
        );

        // None-watermark folder: the unrestricted poll returns the whole
        // set; established ids (in the baseline) are filtered so only the
        // genuinely-new one emits. The next baseline is produced by the
        // REAL production merge (`extend_live_ids`), not hand-built, so the
        // second poll proves the id does not recur (condition b).
        let full = vec![
            item("known1", None, false),
            item("known2", None, false),
            item("fresh", None, false),
        ];
        let live = vec!["known1".to_string(), "known2".to_string()];
        let emitted = incremental_added_ids(&full, None, &[], &live);
        assert_eq!(emitted, vec!["fresh".to_string()]);
        // Fold the emitted ids into the baseline exactly as the reducer does.
        let live = extend_live_ids(live, &emitted);
        assert!(incremental_added_ids(&full, None, &[], &live).is_empty());
    }

    // Build an `ItemWalk` result the pure reducer consumes, so the caller
    // contract is testable without a live EWS server.
    fn walk(items: Vec<EwsItem>, unhandled: &[&str], complete: bool) -> ItemWalk {
        ItemWalk {
            items,
            unhandled_classes: unhandled.iter().map(|s| (*s).to_string()).collect(),
            complete,
        }
    }

    #[test]
    fn incomplete_incremental_walk_makes_caller_retry() {
        // (e) caller contract: an incomplete incremental walk yields
        // `Retry` - the stream then emits NO batch and NO checkpoint, and
        // the watermark/baseline never move (the cursor is not returned).
        let pf = cursor(Some("2026-03-01T10:00:00Z"), Some(1_000_000), &["a", "b"]);
        let poll = walk(
            vec![item("c", Some("2026-04-01T00:00:00Z"), false)],
            &[],
            false, // did not reach the last page
        );
        assert!(
            matches!(
                reduce_public_folder_poll(pf, &poll, None, 2_000_000),
                PollOutcome::Retry
            ),
            "an incomplete poll must retry, not advance/checkpoint"
        );
    }

    #[test]
    fn incomplete_full_scan_makes_caller_retry_no_false_destroyed() {
        // (e) caller contract: the incremental poll completed, but the
        // deletion scan came back partial. Diffing the full baseline
        // {a,b,c} against a partial scan {a} would falsely emit b,c as
        // Destroyed; instead the whole poll retries (no changes, no
        // checkpoint, baseline untouched).
        let pf = cursor(None, Some(1_000_000), &["a", "b", "c"]);
        let poll = walk(Vec::new(), &[], true);
        let scan = walk(vec![item("a", None, false)], &[], false);
        assert!(
            matches!(
                reduce_public_folder_poll(pf, &poll, Some(&scan), 6_000_000),
                PollOutcome::Retry
            ),
            "an incomplete scan must retry, never emit false Destroyed"
        );
    }

    #[test]
    fn none_watermark_addition_persists_and_does_not_reemit() {
        // (b) real production path: poll -> checkpoint -> poll again. A
        // None-watermark folder emits the genuinely-new untimestamped item
        // once; the reducer folds it into the checkpointed baseline, so the
        // next identical unrestricted poll re-emits nothing. Scan not due
        // (recently scanned) so this exercises the incremental path alone.
        let pf = cursor(None, Some(2_000_000), &["known"]);
        let set = || vec![item("known", None, false), item("fresh", None, false)];
        let out1 = reduce_public_folder_poll(pf, &walk(set(), &[], true), None, 2_000_050);
        let cursor1 = match out1 {
            PollOutcome::Apply {
                changes, cursor, ..
            } => {
                // "fresh" emits (Updated + 2 Added); "known" does not.
                assert!(changes.iter().any(|c| matches!(
                    c,
                    Change::ObjectChange(o)
                        if o.id == emitted_id("fresh")
                            && matches!(o.kind, ObjectChangeKind::Updated)
                )));
                assert!(!changes.iter().any(|c| matches!(
                    c, Change::ObjectChange(o) if o.id == emitted_id("known")
                )));
                assert!(cursor.live_ids.contains(&"fresh".to_string()));
                *cursor
            }
            PollOutcome::Retry => panic!("complete poll must Apply"),
        };
        // Second poll, same set, updated baseline -> nothing re-emits.
        let out2 = reduce_public_folder_poll(cursor1, &walk(set(), &[], true), None, 2_000_100);
        match out2 {
            PollOutcome::Apply { changes, .. } => assert!(
                changes.is_empty(),
                "the persisted baseline stops the second-poll re-emission"
            ),
            PollOutcome::Retry => panic!("complete poll must Apply"),
        }
    }

    #[test]
    fn full_scan_only_unhandled_class_warns_exactly_once() {
        // (d) caller contract, combined sources: the incremental poll sees
        // no unhandled class; the full scan surfaces a `Task`. It must warn
        // once, record the class, and never re-warn on a later scan.
        let pf = cursor(Some("2026-01-01T00:00:00Z"), None, &["a"]); // never scanned -> due
        let poll = walk(Vec::new(), &[], true);
        let scan = walk(vec![item("a", None, false)], &["Task"], true);
        let out1 = reduce_public_folder_poll(pf, &poll, Some(&scan), 3_000_000);
        let cursor1 = match out1 {
            PollOutcome::Apply {
                warnings, cursor, ..
            } => {
                assert_eq!(warnings.len(), 1, "a scan-only unhandled class warns once");
                assert!(cursor.warned_classes.contains(&"Task".to_string()));
                *cursor
            }
            PollOutcome::Retry => panic!("complete poll must Apply"),
        };
        // A later due scan still seeing `Task` does not re-warn.
        let later = 3_000_000 + FULL_SCAN_INTERVAL_SECS + 1;
        let scan2 = walk(vec![item("a", None, false)], &["Task"], true);
        match reduce_public_folder_poll(cursor1, &poll, Some(&scan2), later) {
            PollOutcome::Apply { warnings, .. } => assert!(
                warnings.is_empty(),
                "an already-warned class does not re-warn across polls"
            ),
            PollOutcome::Retry => panic!("complete poll must Apply"),
        }
    }

    // Count `ObjectChange`s of a given kind for a specific id.
    fn count_object_change(changes: &[Change], id: &str, destroyed: bool) -> usize {
        changes
            .iter()
            .filter(|c| match c {
                Change::ObjectChange(o) => {
                    o.id == emitted_id(id)
                        && matches!(o.kind, ObjectChangeKind::Destroyed) == destroyed
                }
                _ => false,
            })
            .count()
    }

    fn unbox_apply(out: PollOutcome) -> (Vec<Change>, Vec<Warning>, PublicFolderCursor) {
        match out {
            PollOutcome::Apply {
                changes,
                warnings,
                cursor,
            } => (changes, warnings, *cursor),
            PollOutcome::Retry => panic!("complete poll must Apply"),
        }
    }

    #[test]
    fn persisted_fresh_id_deleted_emits_one_destroyed() {
        // (b) persistence + deletion: cycle 1 (None wm, no scan) emits a
        // fresh untimestamped id and folds it into the baseline; cycle 2's
        // complete scan no longer sees it, so exactly one Destroyed fires.
        let pf = cursor(None, Some(2_000_000), &["known"]);
        let poll1 = walk(
            vec![item("known", None, false), item("fresh", None, false)],
            &[],
            true,
        );
        let (_c1, _w1, cursor1) =
            unbox_apply(reduce_public_folder_poll(pf, &poll1, None, 2_000_050));
        assert!(
            cursor1.live_ids.contains(&"fresh".to_string()),
            "fresh must be folded into the persisted baseline"
        );

        // Cycle 2: fresh gone from both the poll and the (due) scan.
        let poll2 = walk(vec![item("known", None, false)], &[], true);
        let scan2 = walk(vec![item("known", None, false)], &[], true);
        let (changes, _w2, cursor2) = unbox_apply(reduce_public_folder_poll(
            cursor1,
            &poll2,
            Some(&scan2),
            9_000_000,
        ));
        assert_eq!(
            count_object_change(&changes, "fresh", true),
            1,
            "exactly one Destroyed for the vanished persisted id"
        );
        assert!(!cursor2.live_ids.contains(&"fresh".to_string()));
    }

    #[test]
    fn same_cycle_scan_containing_fresh_neither_destroys_nor_double_adds() {
        // (b) no double-handling: the incremental poll emits `fresh`; the
        // same-cycle scan also contains it. No Destroyed (it is live) and no
        // second Added (the same-cycle `emitted` set suppresses it).
        let pf = cursor(None, None, &["known"]);
        let poll = walk(
            vec![item("known", None, false), item("fresh", None, false)],
            &[],
            true,
        );
        let scan = walk(
            vec![item("known", None, false), item("fresh", None, false)],
            &[],
            true,
        );
        let (changes, _w, cursor1) =
            unbox_apply(reduce_public_folder_poll(pf, &poll, Some(&scan), 5_000_000));
        assert_eq!(
            count_object_change(&changes, "fresh", false),
            1,
            "one Added, not two"
        );
        assert_eq!(
            count_object_change(&changes, "fresh", true),
            0,
            "no false Destroyed"
        );
        assert!(cursor1.live_ids.contains(&"fresh".to_string()));
    }

    #[test]
    fn fold_crossing_cap_degrades_with_empty_baseline() {
        // (c) bug-1 fix: folding an incremental emission that crosses the
        // hard cap degrades exactly like the scan path - empty baseline,
        // degraded set, one over-cap warning - instead of shipping a
        // 10_001-id checkpoint with degraded == false.
        let full: Vec<String> = (0..PUBLIC_FOLDER_LIVE_IDS_CAP)
            .map(|i| format!("id{i}"))
            .collect();
        let pf = PublicFolderCursor {
            folder_id: "AAMkPF=".to_string(),
            routing: PublicFolderRouting {
                anchor_mailbox: "content@contoso.com".to_string(),
                public_folder_mailbox: Some("pf@contoso.com".to_string()),
            },
            watermark: None,
            last_full_scan_at: Some(2_000_000),
            live_ids: full,
            boundary_ids: Vec::new(),
            degraded: false,
            warned_classes: Vec::new(),
        };
        // No scan this cycle; one fresh untimestamped id pushes to cap + 1.
        let poll = walk(vec![item("fresh", None, false)], &[], true);
        let (changes, warnings, cursor1) =
            unbox_apply(reduce_public_folder_poll(pf, &poll, None, 2_000_050));
        assert!(cursor1.degraded, "crossing the cap must set degraded");
        assert!(cursor1.live_ids.is_empty(), "degraded baseline is empty");
        assert_eq!(
            warnings.len(),
            1,
            "exactly one over-cap warning on transition"
        );
        assert!(
            matches!(warnings[0].kind, WarningKind::StrategyDowngraded),
            "the over-cap warning is the downgrade warning"
        );
        // The addition still emitted before the fold degraded the folder.
        assert_eq!(count_object_change(&changes, "fresh", false), 1);
    }

    #[test]
    fn degraded_apply_stays_empty_then_recovers_via_scan() {
        // (c) degraded stays empty on a no-scan cycle; a later under-cap scan
        // recovers the baseline and clears degraded.
        let degraded = cursor_degraded();
        let poll = walk(vec![item("newC", None, false)], &[], true);
        let (changes, _w, still_degraded) =
            unbox_apply(reduce_public_folder_poll(degraded, &poll, None, 5_000_000));
        assert!(still_degraded.degraded, "no scan -> stays degraded");
        assert!(
            still_degraded.live_ids.is_empty(),
            "baseline stays empty while degraded"
        );
        assert_eq!(
            count_object_change(&changes, "newC", false),
            1,
            "best-effort Added while degraded"
        );

        // Recovery: a complete under-cap scan re-installs the baseline.
        let recover_from = cursor_degraded();
        let empty_poll = walk(Vec::new(), &[], true);
        let scan = walk(
            vec![item("a", None, false), item("b", None, false)],
            &[],
            true,
        );
        let (recovery_changes, _w2, recovered) = unbox_apply(reduce_public_folder_poll(
            recover_from,
            &empty_poll,
            Some(&scan),
            6_000_000,
        ));
        assert!(!recovered.degraded, "under-cap scan clears degraded");
        assert_eq!(
            recovered.live_ids,
            vec!["a".to_string(), "b".to_string()],
            "the scan set becomes the recovered baseline"
        );
        // Both recovered ids must emit exactly one Added each: additions
        // still flow before the baseline is reassigned (condition c), so a
        // future reorder that reassigns the baseline before emitting would
        // silently absorb them instead of failing loudly here.
        assert_eq!(count_object_change(&recovery_changes, "a", false), 1);
        assert_eq!(count_object_change(&recovery_changes, "b", false), 1);
    }

    #[test]
    fn scan_only_timestamped_arrival_none_watermark_is_emitted() {
        // Bug 2: a None-watermark folder whose incremental poll saw nothing,
        // but whose full scan surfaces a NEW TIMESTAMPED item. It must emit
        // (once) rather than be silently installed into the baseline and
        // then filtered as known next poll.
        let pf = cursor(None, None, &[]);
        let poll = walk(Vec::new(), &[], true);
        let scan = walk(
            vec![item("T", Some("2026-05-01T00:00:00Z"), false)],
            &[],
            true,
        );
        let (changes, _w, cursor1) =
            unbox_apply(reduce_public_folder_poll(pf, &poll, Some(&scan), 3_000_000));
        assert_eq!(
            count_object_change(&changes, "T", false),
            1,
            "the scan-only timestamped arrival emits, not absorbed"
        );
        assert!(cursor1.live_ids.contains(&"T".to_string()));
        assert!(
            cursor1.watermark.is_none(),
            "the scan does not advance the watermark"
        );
    }

    #[test]
    fn some_watermark_scan_only_timestamped_id_stays_eligible_for_poll() {
        // Reducer-level pin of condition (b)'s asymmetry: with a Some
        // watermark the full scan is NOT a peer entry point for timestamped
        // items (that is the incremental poll's job - the `>=` restriction
        // will pick it up once its received time reaches the watermark).
        // Only a genuinely-untimestamped scan-only id emits here; a
        // timestamped scan-only id must NOT emit off the scan alone.
        let pf = cursor(Some("2026-03-05T12:00:00Z"), None, &[]);
        // Empty poll leaves the watermark unchanged (still Some), so
        // `scan_additions` runs with `watermark_none = false`.
        let poll = walk(Vec::new(), &[], true);
        let scan = walk(
            vec![
                item("ts_only", Some("2026-04-01T00:00:00Z"), false),
                item("plain_only", None, false),
            ],
            &[],
            true,
        );
        let (changes, _w, cursor1) =
            unbox_apply(reduce_public_folder_poll(pf, &poll, Some(&scan), 3_000_000));
        assert_eq!(
            count_object_change(&changes, "ts_only", false),
            0,
            "a timestamped scan-only id under a Some watermark must not emit off the scan"
        );
        assert_eq!(
            count_object_change(&changes, "plain_only", false),
            1,
            "the untimestamped scan-only id is the scan's job and must emit"
        );
        assert_eq!(cursor1.watermark.as_deref(), Some("2026-03-05T12:00:00Z"));
    }

    #[test]
    fn condition_a_boundary_and_timestamp_through_reducer() {
        // (a) full reducer: a watermarked folder emits new timestamped items,
        // skips the prior boundary second, advances the watermark and boundary,
        // and (bug-1) does NOT fold timestamped ids into the baseline.
        let pf = PublicFolderCursor {
            folder_id: "AAMkPF=".to_string(),
            routing: PublicFolderRouting {
                anchor_mailbox: "content@contoso.com".to_string(),
                public_folder_mailbox: Some("pf@contoso.com".to_string()),
            },
            watermark: Some("2026-03-05T12:00:00Z".to_string()),
            last_full_scan_at: Some(2_000_000),
            live_ids: vec!["old".to_string()],
            boundary_ids: vec!["edge".to_string()],
            degraded: false,
            warned_classes: Vec::new(),
        };
        let poll = walk(
            vec![
                item("edge", Some("2026-03-05T12:00:00Z"), false),
                item("new", Some("2026-03-06T00:00:00Z"), false),
            ],
            &[],
            true,
        );
        let (changes, _w, cursor1) =
            unbox_apply(reduce_public_folder_poll(pf, &poll, None, 2_000_050));
        assert_eq!(
            count_object_change(&changes, "new", false),
            1,
            "new timestamped item emits"
        );
        assert_eq!(
            count_object_change(&changes, "edge", false),
            0,
            "prior boundary second is skipped"
        );
        assert_eq!(cursor1.watermark.as_deref(), Some("2026-03-06T00:00:00Z"));
        assert_eq!(cursor1.boundary_ids, vec!["new".to_string()]);
        assert_eq!(
            cursor1.live_ids,
            vec!["old".to_string()],
            "timestamped ids are NOT folded into the baseline (bug 1)"
        );
    }

    #[test]
    fn newly_unhandled_suppresses_already_warned() {
        // Only classes not already warned about surface, deduped in order.
        assert_eq!(
            newly_unhandled(
                &[
                    "Task".to_string(),
                    "PostItem".to_string(),
                    "Task".to_string()
                ],
                &["Task".to_string()],
            ),
            vec!["PostItem".to_string()]
        );
        // All already warned -> nothing (no repeat warning across polls).
        assert!(newly_unhandled(&["Task".to_string()], &["Task".to_string()]).is_empty());
        // A class only the full scan saw (absent from the prior set) warns.
        assert_eq!(
            newly_unhandled(&["DistributionList".to_string()], &[]),
            vec!["DistributionList".to_string()]
        );
    }

    #[test]
    fn scan_additions_watermarked_emits_only_new_untimestamped() {
        use std::collections::HashSet;
        // Watermarked folder (`watermark_none = false`): the restricted poll
        // owns timestamped items, so the scan emits only new UNTIMESTAMPED
        // ones. `keep` was already in the baseline; `ts` carries a received
        // time (the poll's job); `alreadyEmitted` was emitted this cycle;
        // `newContact` is a brand-new no-timestamp item -> the only emit.
        let scan = vec![
            item("keep", None, false),
            item("ts", Some("2026-03-01T00:00:00Z"), false),
            item("alreadyEmitted", None, false),
            item("newContact", None, false),
        ];
        let prior_live = vec!["keep".to_string()];
        let emitted: HashSet<String> = ["alreadyEmitted".to_string()].into_iter().collect();
        assert_eq!(
            scan_additions(&scan, &prior_live, &emitted, false),
            vec!["newContact".to_string()]
        );
    }

    #[test]
    fn scan_additions_none_watermark_emits_new_regardless_of_timestamp() {
        use std::collections::HashSet;
        // None-watermark folder (`watermark_none = true`): the scan is a peer
        // entry point, so a new-to-baseline item emits whether or not it
        // carries a timestamp. `ts` is a brand-new TIMESTAMPED item that first
        // appears in the scan - it MUST emit (bug 2), unlike the watermarked
        // case above. `known` is filtered by the baseline; `alreadyEmitted`
        // by the same-cycle set.
        let scan = vec![
            item("known", None, false),
            item("ts", Some("2026-05-01T00:00:00Z"), false),
            item("alreadyEmitted", Some("2026-05-02T00:00:00Z"), false),
            item("newContact", None, false),
        ];
        let prior_live = vec!["known".to_string()];
        let emitted: HashSet<String> = ["alreadyEmitted".to_string()].into_iter().collect();
        assert_eq!(
            scan_additions(&scan, &prior_live, &emitted, true),
            vec!["ts".to_string(), "newContact".to_string()]
        );
    }

    #[test]
    fn scan_additions_empty_when_nothing_new() {
        use std::collections::HashSet;
        let scan = vec![item("a", None, false), item("b", None, false)];
        let prior_live = vec!["a".to_string(), "b".to_string()];
        assert!(
            scan_additions(&scan, &prior_live, &HashSet::new(), false).is_empty(),
            "all scan ids already in the baseline -> no additions"
        );
    }

    #[test]
    fn degraded_folder_still_emits_untimestamped_additions() {
        use std::collections::HashSet;
        // A degraded (over-cap) folder stores an EMPTY baseline. The full
        // scan still runs (the reducer calls this regardless of `degraded`),
        // so with an empty prior baseline every untimestamped item not
        // already emitted this poll surfaces as an addition (condition c) -
        // best-effort additions-only, since there is no baseline to dedupe.
        // (Watermarked case, so timestamped `ts` is the poll's job.)
        let scan = vec![
            item("contactA", None, false),
            item("contactB", None, false),
            item("ts", Some("2026-03-01T00:00:00Z"), false),
        ];
        let degraded_baseline: Vec<String> = Vec::new();
        assert_eq!(
            scan_additions(&scan, &degraded_baseline, &HashSet::new(), false),
            vec!["contactA".to_string(), "contactB".to_string()],
            "empty (degraded) baseline -> all untimestamped items emit"
        );
        // An item already emitted by the incremental poll this cycle is not
        // double-emitted even when degraded.
        let emitted: HashSet<String> = ["contactA".to_string()].into_iter().collect();
        assert_eq!(
            scan_additions(&scan, &degraded_baseline, &emitted, false),
            vec!["contactB".to_string()]
        );
    }

    #[test]
    fn scan_throttled_to_interval() {
        // Has polled (watermark present), never scanned -> due.
        assert!(scan_due(
            &cursor(Some("2026-01-01T00:00:00Z"), None, &["a"]),
            10_000
        ));
        // Scanned 600s ago, interval 3600 -> not due.
        let now = 1_700_000_000;
        assert!(!scan_due(
            &cursor(Some("2026-01-01T00:00:00Z"), Some(now - 600), &["a"]),
            now
        ));
        // Scanned 3700s ago -> due.
        assert!(scan_due(
            &cursor(Some("2026-01-01T00:00:00Z"), Some(now - 3700), &["a"]),
            now
        ));
    }

    fn folder(id: &str, read: bool) -> EwsFolder {
        EwsFolder {
            folder_id: id.to_string(),
            display_name: id.to_string(),
            folder_class: Some("IPF.Note".to_string()),
            total_count: 0,
            unread_count: 0,
            child_folder_count: 0,
            effective_rights: crate::ews::EwsEffectiveRights {
                read,
                ..Default::default()
            },
            replica_list: None,
        }
    }

    /// `HierarchyOnly` emits zero cursor scopes and a one-element `Pinned`
    /// list emits exactly one - but BOTH seed the routing map (and the
    /// projection metadata) for every discovered folder, so
    /// `containers_list` shows the whole readable hierarchy either way. An
    /// organization can carry thousands of public folders; discovering one
    /// must never imply syncing it.
    #[tokio::test]
    async fn scope_policy_gates_cursor_scopes_but_never_the_routing_map() {
        use super::super::{GraphAccount, PushMode};
        use crate::client::GraphClient;

        let routing = PublicFolderRouting {
            anchor_mailbox: "content@contoso.com".to_string(),
            public_folder_mailbox: Some("pf@contoso.com".to_string()),
        };
        let discovered = [folder("AAMkPF1=", true), folder("AAMkPF2=", true)];

        for (policy, expected_scopes) in [
            (PublicFolderScope::hierarchy_only(), Vec::new()),
            (
                PublicFolderScope::pinned([FolderId("AAMkPF2=".to_string())]),
                vec![CursorScope::Folder(FolderId("AAMkPF2=".to_string()))],
            ),
        ] {
            let account =
                GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
            let mut scopes = Vec::new();
            for entry in &discovered {
                if let Some(scope) =
                    seed_and_scope(&account, &policy, entry, None, routing.clone()).await
                {
                    scopes.push(scope);
                }
            }
            assert_eq!(scopes, expected_scopes, "policy {policy:?}");
            // Both folders are routable and projectable under either policy.
            assert_eq!(
                account.routing_map.read().await.len(),
                2,
                "policy {policy:?}"
            );
            assert_eq!(
                account.public_folder_meta.read().await.len(),
                2,
                "policy {policy:?}"
            );
            assert!(
                account
                    .public_folder_routing(&FolderId("AAMkPF1=".to_string()))
                    .await
                    .is_some(),
                "an unpinned folder is still routable so it can project",
            );
        }
    }

    #[test]
    fn discovery_skips_unreadable_public_folder() {
        let folders = vec![folder("readable", true), folder("denied", false)];
        let kept = readable_folders(folders);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].folder_id, "readable");
    }

    #[test]
    fn ews_item_projects_to_inventory_entry_with_owner_tag() {
        let folder = FolderId("AAMkPF=".to_string());
        let entry = item_to_inventory_entry(
            &item("x", Some("2026-03-01T00:00:00Z"), true),
            &folder,
            "content@contoso.com",
        );
        // The entry id is folder-qualified so hydration can route the item
        // onto EWS `GetItem` (Graph REST cannot address an EWS ItemId).
        assert_eq!(entry.id, emitted_id("x"));
        let parsed = super::super::foreign::parse_message_id(&entry.id);
        assert_eq!(parsed.public_folder(), Some("AAMkPF="));
        assert_eq!(parsed.native_id(), "x");
        assert!(
            entry
                .memberships
                .contains(&MembershipScope::Folder(folder.clone()))
        );
        assert!(
            entry
                .memberships
                .contains(&MembershipScope::Mailbox(MailboxId(
                    "content@contoso.com".to_string()
                )))
        );
        assert!(matches!(
            entry.fingerprint.server_version,
            ServerVersion::ETag(_)
        ));
    }
}
