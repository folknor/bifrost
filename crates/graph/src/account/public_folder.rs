//! No-delta-token public-folder sync strategy.
//!
//! Public folders have no `@odata.deltaLink`. The opaque cursor IS the
//! sync state: a `DateTimeReceived` watermark drives the incremental
//! timestamp poll, and a throttled full-id scan reconciles deletions the
//! timestamp poll structurally cannot see (a deleted item simply stops
//! appearing; old items sit below the watermark). The authoritative
//! live-id set rides inside the cursor so a cold resume reconstructs the
//! deletion baseline with no engine-side side table - the one
//! substantive departure from ratatoskr, which diffed against a SQLite
//! table. The whole algorithm decomposes into the pure helpers below so
//! every branch is unit-pinnable without a live EWS server.

use std::time::{SystemTime, UNIX_EPOCH};

use bifrost_types::{
    AccountOperation, AccountStream, Change, ChangeCursor, Checkpoint, CursorScope, ErrorScope,
    Fingerprint, FolderId, InventoryEntry, MailboxId, MembershipScope, ObjectChange,
    ObjectChangeKind, ObjectId, PageBoundary, ScopeChange, ScopeChangeKind, ServerVersion,
    SyncEvent, Warning, WarningKind,
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

/// Defensive cap on the number of `FindFolder` browse calls during
/// hierarchy discovery. A public-folder hierarchy is a tree, so this
/// only bites a pathological (cyclic or enormous) hierarchy; the dedup
/// `visited` set already prevents re-browsing a folder. Bounds the
/// discovery round-trip count.
const PUBLIC_FOLDER_BROWSE_STEP_CAP: usize = 1_000;

// ── Pure helpers ────────────────────────────────────────────

/// Advance a watermark to the newest `received_at` seen in a batch.
/// RFC-3339 UTC timestamps compare correctly lexicographically, so the
/// new watermark is the string max of the prior watermark and every
/// item's `received_at`.
pub(crate) fn advance_watermark(prior: Option<String>, items: &[EwsItem]) -> Option<String> {
    let mut best = prior;
    for item in items {
        if let Some(received) = item.received_at.as_ref()
            && best.as_ref().is_none_or(|cur| received > cur)
        {
            best = Some(received.clone());
        }
    }
    best
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

/// Decide whether a full-id deletion scan is due. A scan runs when the
/// folder has polled at least once (`watermark.is_some()`) and either it
/// has never scanned, or `FULL_SCAN_INTERVAL_SECS` has elapsed since the
/// last scan. The very first establish (no watermark yet) never scans -
/// there is nothing local to reconcile.
pub(crate) fn scan_due(cursor: &PublicFolderCursor, now: u64) -> bool {
    if cursor.watermark.is_none() {
        return false;
    }
    match cursor.last_full_scan_at {
        None => true,
        Some(last) => now.saturating_sub(last) >= FULL_SCAN_INTERVAL_SECS,
    }
}

/// Project an `EwsItem` to an inventory entry, owner-tagged with both
/// the public folder and its content mailbox (the A5a owner-tag
/// pattern, so the consumer maps the scope to its public owner).
pub(crate) fn item_to_inventory_entry(
    item: &EwsItem,
    folder: &FolderId,
    content_mailbox: &str,
) -> InventoryEntry {
    InventoryEntry {
        id: ObjectId(item.item_id.clone()),
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
            flags_hash: u64::from(item.is_read),
        },
        thread_id: None,
        message_id: None,
        references: Vec::new(),
        in_reply_to: None,
    }
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

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn ews_client(account: &GraphAccount) -> Option<EwsClient> {
    account.client.account_net().map(EwsClient::new)
}

/// Walk every `FindItem` page for a folder, optionally restricted to
/// `since`, collecting all items in received-descending order.
async fn fetch_all_items(
    ews: &EwsClient,
    folder_id: &str,
    since: Option<&str>,
    routing: &PublicFolderRouting,
) -> Result<Vec<EwsItem>, EwsError> {
    let headers = routing.headers();
    let mut all = Vec::new();
    let mut offset = 0u32;
    loop {
        let page = ews
            .find_items(folder_id, since, offset, PUBLIC_FOLDER_PAGE_SIZE, &headers)
            .await?;
        let count = u32::try_from(page.items.len()).unwrap_or(u32::MAX);
        all.extend(page.items);
        if page.includes_last || count == 0 {
            break;
        }
        offset = offset.saturating_add(count);
    }
    Ok(all)
}

// ── Discovery ───────────────────────────────────────────────

/// Browse the public-folder hierarchy and emit a `CursorScope::Folder`
/// per readable leaf-and-branch folder, seeding `routing_map` with each
/// folder's content-mailbox routing. Opt-in (`with_public_folders`); a
/// per-folder Autodiscover/permission failure is skipped with a scoped
/// warning, never a discovery-wide failure (A5a per-mailbox robustness).
///
/// Returns the discovered scopes plus any warnings. On a routing-
/// resolution failure (no public-folder hierarchy at all) the whole
/// public-folder leg is skipped with a single warning - the primary and
/// shared mailboxes still discover.
pub(crate) async fn discover_public_folder_scopes(
    account: &GraphAccount,
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
    let domain = user_email.rsplit('@').next().unwrap_or("").to_string();

    let routing = match account.discover_public_folder_routing(&user_email).await {
        Ok(routing) => routing,
        Err(_) => {
            warnings.push(Warning::support_only(
                WarningKind::OperatorAttentionNeeded,
                "public-folder discovery skipped: hierarchy routing unavailable".to_string(),
            ));
            return (scopes, warnings);
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
            match resolve_content_routing(account, &ews, &folder, &hierarchy_headers, &domain).await
            {
                Ok(content_routing) => {
                    let folder_id = FolderId(folder.folder_id.clone());
                    account
                        .routing_map
                        .write()
                        .await
                        .insert(folder_id.clone(), content_routing);
                    scopes.push(CursorScope::Folder(folder_id));
                }
                Err(()) => {
                    warnings.push(Warning::support_only(
                        WarningKind::OperatorAttentionNeeded,
                        format!(
                            "public folder {} skipped: content-mailbox routing unresolved",
                            folder.display_name
                        ),
                    ));
                }
            }
        }
    }

    (scopes, warnings)
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
    let guids = crate::ews::decode_replica_list(&base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        &replica,
    ))
    .map_err(|_| ())?;
    let guid = guids.into_iter().next().ok_or(())?;
    let stripped = guid.trim_matches(['{', '}']);
    let replica_smtp = super::autodiscover::construct_replica_smtp(stripped, domain);
    let content_mailbox = account
        .discover_content_mailbox(&replica_smtp)
        .await
        .map_err(|_| ())?;
    Ok(PublicFolderRouting {
        anchor_mailbox: content_mailbox.clone(),
        public_folder_mailbox: Some(content_mailbox),
    })
}

// ── Streams ─────────────────────────────────────────────────

pub(crate) fn public_folder_inventory_stream(
    account: GraphAccount,
    scope: CursorScope,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    Box::pin(async_stream::stream! {
        let CursorScope::Folder(folder) = scope.clone() else {
            // Routed here only for `CursorScope::Folder` in the routing
            // map; any other shape is a wiring bug.
            let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                .with_scope(ErrorScope::Cursor(scope.clone()));
            yield SyncEvent::Terminated(cursor_error_to_account_error(
                super::cursor::CursorError::Unsupported, ctx));
            yield SyncEvent::Done(None);
            return;
        };

        let Some(routing) = account.public_folder_routing(&folder).await else {
            let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                .with_scope(ErrorScope::Cursor(scope.clone()));
            yield SyncEvent::Terminated(cursor_error_to_account_error(
                super::cursor::CursorError::Unsupported, ctx));
            yield SyncEvent::Done(None);
            return;
        };
        let owner = MailboxId(routing.anchor_mailbox.clone());

        let Some(ews) = ews_client(&account) else {
            let ctx = GraphErrorContext::ews(AccountOperation::SyncInventory)
                .with_scope(ErrorScope::Cursor(scope.clone()));
            yield SyncEvent::Terminated(ews_shared_scope_error(
                EwsError::MalformedXml(bifrost_types::DiagnosticText::support_only(
                    "EWS account net not attached".to_string(),
                )),
                &scope,
                None,
                ctx,
            ));
            yield SyncEvent::Done(None);
            return;
        };

        let items = match fetch_all_items(&ews, &folder.0, None, &routing).await {
            Ok(items) => items,
            Err(error) => {
                let ctx = GraphErrorContext::ews(AccountOperation::SyncInventory)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield SyncEvent::Terminated(ews_shared_scope_error(
                    error, &scope, Some(&owner), ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };

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
        // additions-only.
        let live_ids = cap_live_ids(scan_set, PUBLIC_FOLDER_LIVE_IDS_CAP).unwrap_or_default();
        let first = PublicFolderCursor {
            folder_id: folder.0.clone(),
            routing: routing.clone(),
            watermark,
            last_full_scan_at: None,
            live_ids,
        };
        let cursor = match encode_cursor(scope.clone(), GraphCursorPayload::public_folder(first)) {
            Ok(cursor) => cursor,
            Err(error) => {
                let ctx = GraphErrorContext::graph(AccountOperation::SyncInventory)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };

        let checkpoint = Checkpoint::Change(cursor.clone());
        yield batch(entries, PageBoundary::Final, Some(cursor));
        yield SyncEvent::Done(Some(checkpoint));
    })
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
        let GraphCursorPayload { kind: GraphCursorKind::PublicFolder(mut pf), .. } = payload else {
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
                EwsError::MalformedXml(bifrost_types::DiagnosticText::support_only(
                    "EWS account net not attached".to_string(),
                )),
                &scope,
                None,
                ctx,
            ));
            yield SyncEvent::Done(None);
            return;
        };

        let mut changes = Vec::new();

        // 1. Incremental timestamp poll: items at/after the watermark.
        let new_items = match fetch_all_items(
            &ews, &pf.folder_id, pf.watermark.as_deref(), &pf.routing,
        ).await {
            Ok(items) => items,
            Err(error) => {
                let ctx = GraphErrorContext::ews(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(scope.clone()));
                yield SyncEvent::Terminated(ews_shared_scope_error(
                    error, &scope, Some(&owner), ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };
        for item in &new_items {
            // EWS gives no created/updated split; emit Updated to match
            // Graph delta's convention.
            changes.push(Change::ObjectChange(ObjectChange {
                id: ObjectId(item.item_id.clone()),
                kind: ObjectChangeKind::Updated,
            }));
            // Folder membership: a NEWLY-appearing item must get the same
            // `Folder` scope tag the inventory pass assigns, or new items
            // would be missing the folder membership their inventory peers
            // carry. The existing Graph delta path tags items by `Folder`
            // here too (membership_from_value).
            changes.push(Change::ScopeChange(ScopeChange {
                id: ObjectId(item.item_id.clone()),
                membership: MembershipScope::Folder(folder.clone()),
                kind: ScopeChangeKind::Added,
            }));
            // Owner tag: the content-mailbox identity, the A5a pattern the
            // engine's covering rule cannot synthesize.
            changes.push(Change::ScopeChange(ScopeChange {
                id: ObjectId(item.item_id.clone()),
                membership: MembershipScope::Mailbox(owner.clone()),
                kind: ScopeChangeKind::Added,
            }));
        }
        pf.watermark = advance_watermark(pf.watermark.take(), &new_items);

        // 2. Throttled deletion reconcile.
        let now = now_unix_secs();
        let mut warning: Option<Warning> = None;
        if scan_due(&pf, now) {
            match fetch_all_items(&ews, &pf.folder_id, None, &pf.routing).await {
                Ok(scan_items) => {
                    let scan_set: Vec<String> =
                        scan_items.iter().map(|i| i.item_id.clone()).collect();
                    // A degraded (empty snapshot, was over cap) folder
                    // skips the diff and emits no Destroyed.
                    if !pf.live_ids.is_empty() {
                        for gone in deleted_ids(&pf.live_ids, &scan_set) {
                            changes.push(Change::ObjectChange(ObjectChange {
                                id: ObjectId(gone),
                                kind: ObjectChangeKind::Destroyed,
                            }));
                        }
                    }
                    match cap_live_ids(scan_set, PUBLIC_FOLDER_LIVE_IDS_CAP) {
                        Some(set) => pf.live_ids = set,
                        None => {
                            pf.live_ids = Vec::new();
                            warning = Some(Warning::support_only(
                                WarningKind::StrategyDowngraded,
                                format!(
                                    "public folder {} exceeds {} items; deletion reconcile \
                                     disabled, syncing additions only",
                                    folder.0, PUBLIC_FOLDER_LIVE_IDS_CAP,
                                ),
                            ));
                        }
                    }
                    pf.last_full_scan_at = Some(now);
                }
                Err(error) => {
                    let ctx = GraphErrorContext::ews(AccountOperation::SyncChanges)
                        .with_scope(ErrorScope::Cursor(scope.clone()));
                    yield SyncEvent::Terminated(ews_shared_scope_error(
                        error, &scope, Some(&owner), ctx));
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        }

        if let Some(warning) = warning {
            yield SyncEvent::Warning(warning);
        }

        // 3. Checkpoint the advanced cursor. A no-change poll re-emits
        // the same cursor; the engine's adaptive cadence backs off.
        let advanced = match encode_cursor(
            scope.clone(), GraphCursorPayload::public_folder(pf),
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
        yield batch(changes, PageBoundary::Final, Some(advanced));
        yield SyncEvent::Done(Some(checkpoint));
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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
        }
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
    fn deletion_scan_diffs_against_snapshot() {
        let prior = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let scan = vec!["a".to_string(), "c".to_string()];
        assert_eq!(deleted_ids(&prior, &scan), vec!["b".to_string()]);
        // Nothing deleted -> empty.
        assert!(deleted_ids(&prior, &prior).is_empty());
    }

    #[test]
    fn first_establish_skips_deletion_scan() {
        // No watermark yet (the inventory-minted first cursor) -> no
        // scan, regardless of the (None) scan clock.
        let c = cursor(None, None, &[]);
        assert!(!scan_due(&c, 10_000));
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
        assert_eq!(entry.id, ObjectId("x".to_string()));
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
