//! Message search: the KQL and OData filter builders, the shared
//! mailbox walk, and the versioned opaque search cursor codec.

use crate::account::GraphAccount;
use crate::account::graph_error::{
    GraphErrorContext, into_account_error, invalid_account_error, unsupported_account_error,
};
use crate::client::Resource;
use crate::types::ODataCollection;
use bifrost_types::{
    AccessErrorKind, AccountError, AccountErrorKind, AccountOperation, AttachmentFilter,
    ContainerId, ErrorScope, LabelId, ObjectId, Page, SearchFilter, SearchRequest, SkippedScope,
    ThreadId, balanced_raw_query,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

use super::common::*;

pub(crate) async fn search(
    account: GraphAccount,
    request: SearchRequest,
) -> Result<Page<ThreadId>, AccountError> {
    let page = search_message_rows(&account, request, AccountOperation::Search).await?;
    let mut seen = HashSet::new();
    let mut threads = Vec::new();
    for row in page.items {
        let thread = row.thread_id.unwrap_or(ThreadId(row.id.0));
        if seen.insert(thread.0.clone()) {
            threads.push(thread);
        }
    }
    Ok(Page {
        truncated: page.truncated,
        items: threads,
        next_cursor: page.next_cursor,
        estimated_total: page.estimated_total,
        failed_ids: page.failed_ids,
        skipped_scopes: page.skipped_scopes,
    })
}

pub(crate) async fn search_messages(
    account: GraphAccount,
    request: SearchRequest,
) -> Result<Page<ObjectId>, AccountError> {
    let page = search_message_rows(&account, request, AccountOperation::SearchMessages).await?;
    Ok(Page {
        truncated: page.truncated,
        items: page.items.into_iter().map(|row| row.id).collect(),
        next_cursor: page.next_cursor,
        estimated_total: page.estimated_total,
        failed_ids: page.failed_ids,
        skipped_scopes: page.skipped_scopes,
    })
}

#[derive(Debug)]
pub(super) struct SearchRow {
    pub(super) id: ObjectId,
    pub(super) thread_id: Option<ThreadId>,
}

/// Graph's `nextLink` is only meaningful in the mailbox that minted it.
/// Wrap it with that mailbox so one `Page` cursor can walk the primary and
/// every configured shared mailbox in deterministic order.
///
/// The cursor also carries the shared-mailbox set it was minted against.
/// A search cursor is a POSITION IN A WALK, not a position in one result
/// set: `owner` says which mailbox to resume and `next_search_owner` says
/// which mailbox follows it, and both answers are only meaningful relative
/// to the mailbox list that produced them. Resuming a cursor against a
/// different list silently skips a mailbox added before the current
/// position, or hands the remainder of the walk to mailboxes the caller
/// never asked about - a wrong answer that reports success. So the set is
/// pinned in the cursor and a mismatch is rejected, which costs the caller
/// a restart from page one and nothing else (unlike a sync cursor, a
/// search cursor holds no durable state). The versioned envelope covers the
/// same ground for a cursor whose SHAPE predates this encoding.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct SearchCursor {
    /// Sorted routing keys of every shared mailbox configured when this
    /// cursor was minted, primary excluded (it is always walked first).
    pub(super) mailboxes: Vec<String>,
    pub(super) owner: Option<String>,
    pub(super) next_link: Option<String>,
    /// The request this walk answers (`search_binding`). A `nextLink`
    /// carries its endpoint and query with it, and a mailbox's first page is
    /// built from the resuming request, so a cursor resumed under another
    /// filter, folder or raw query would answer the old search (or half of
    /// each) as the new one; the mismatch is refused. Required: a cursor
    /// minted before the binding existed is refused rather than trusted.
    pub(super) query: String,
    /// Rows already returned from `owner` by this walk, saturating at the
    /// `$search` ceiling. Required, not defaulted: a cursor minted before
    /// the count existed is refused rather than resumed at zero, which would
    /// report a capped mailbox as exhausted.
    pub(super) returned: u32,
}

/// Graph answers a `$search` request with at most this many results in
/// total, across all of its pages; a walk that reaches it with no further
/// `nextLink` was (or may have been) cut short.
pub(super) const SEARCH_RESULT_CEILING: u32 = 1_000;

/// One mailbox's share of a search: the folder lifted into its endpoint and
/// the filter left to compile into `$filter` or `$search`.
pub(super) struct MailboxPlan {
    /// Native folder id for `/mailFolders/{id}/messages`.
    pub(super) folder: Option<String>,
    /// The reduced filter with any request-level `provider_query` folded in
    /// as a `ProviderQuery` conjunct.
    pub(super) filter: Option<SearchFilter>,
}

/// A filter reduced against one mailbox. Folder identity is (owner, native
/// id), so an `In` naming another mailbox's folder is constant false there,
/// and an `In` naming this mailbox's folder keeps only its native id.
enum Reduced {
    True,
    False,
    Filter(SearchFilter),
}

fn reduce_for_owner(filter: &SearchFilter, owner: Option<&str>) -> Reduced {
    match filter {
        SearchFilter::In(container) => {
            let parsed = crate::account::foreign::parse_folder(&bifrost_types::FolderId(
                container.0.clone(),
            ));
            let folder_owner = parsed.foreign().map(|foreign| foreign.mailbox.as_str());
            if crate::account::foreign::same_owner(folder_owner, owner) {
                Reduced::Filter(SearchFilter::In(ContainerId(
                    parsed.native_id().to_string(),
                )))
            } else {
                Reduced::False
            }
        }
        SearchFilter::And(filters) => {
            let mut kept = Vec::new();
            for filter in filters {
                match reduce_for_owner(filter, owner) {
                    Reduced::False => return Reduced::False,
                    Reduced::True => {}
                    Reduced::Filter(SearchFilter::And(inner)) => kept.extend(inner),
                    Reduced::Filter(filter) => kept.push(filter),
                }
            }
            match kept.len() {
                0 => Reduced::True,
                1 => Reduced::Filter(kept.remove(0)),
                _ => Reduced::Filter(SearchFilter::And(kept)),
            }
        }
        SearchFilter::Or(filters) => {
            let mut kept = Vec::new();
            for filter in filters {
                match reduce_for_owner(filter, owner) {
                    Reduced::True => return Reduced::True,
                    Reduced::False => {}
                    Reduced::Filter(SearchFilter::Or(inner)) => kept.extend(inner),
                    Reduced::Filter(filter) => kept.push(filter),
                }
            }
            match kept.len() {
                0 => Reduced::False,
                1 => Reduced::Filter(kept.remove(0)),
                _ => Reduced::Filter(SearchFilter::Or(kept)),
            }
        }
        SearchFilter::Not(inner) => match reduce_for_owner(inner, owner) {
            Reduced::True => Reduced::False,
            Reduced::False => Reduced::True,
            Reduced::Filter(filter) => Reduced::Filter(SearchFilter::Not(Box::new(filter))),
        },
        // An unbounded range matches every message. Neither KQL nor `$filter`
        // has a literal for that, so it folds here like any other constant
        // (a true disjunct makes its `Or` true, a negation makes it false),
        // and the compilers never see a fragment that renders as nothing.
        SearchFilter::DateRange {
            after: None,
            before: None,
        } => Reduced::True,
        other => Reduced::Filter(other.clone()),
    }
}

/// Plan the search of one mailbox, or `None` when the request's folder
/// restrictions rule the whole mailbox out (it is then not requested).
///
/// The first positive top-level `In` becomes the endpoint: KQL has no folder
/// property, but Graph searches a folder's own message collection, so the
/// folder restriction is carried by the URL instead of the query. Every exact
/// repeat of that conjunct is redundant and dropped. Any other `In` stays in
/// the filter: `parentFolderId` on the `$filter` path, refused on the KQL
/// path. Two different folder strings are NOT read as an empty intersection,
/// because a well-known name (`inbox`) and the native id of the same folder
/// are different strings for one folder.
pub(super) fn plan_for_owner(request: &SearchRequest, owner: Option<&str>) -> Option<MailboxPlan> {
    let filter = match request.filter.as_ref().map(|f| reduce_for_owner(f, owner)) {
        None | Some(Reduced::True) => None,
        Some(Reduced::False) => return None,
        Some(Reduced::Filter(filter)) => Some(filter),
    };
    let (folder, filter) = match filter {
        Some(SearchFilter::In(container)) => (Some(container.0), None),
        Some(SearchFilter::And(conjuncts)) => {
            let lifted = conjuncts.iter().find_map(|conjunct| match conjunct {
                SearchFilter::In(container) => Some(container.0.clone()),
                _ => None,
            });
            match lifted {
                Some(folder) => {
                    let mut rest: Vec<SearchFilter> = conjuncts
                        .into_iter()
                        .filter(|c| !matches!(c, SearchFilter::In(other) if other.0 == folder))
                        .collect();
                    let rest = match rest.len() {
                        0 => None,
                        1 => Some(rest.remove(0)),
                        _ => Some(SearchFilter::And(rest)),
                    };
                    (Some(folder), rest)
                }
                None => (None, Some(SearchFilter::And(conjuncts))),
            }
        }
        other => (None, other),
    };
    // The request-level raw query is the same KQL a `ProviderQuery` leaf
    // carries, so it joins the filter as one: Graph cannot send `$search`
    // beside `$filter` (400), and the leaf forces the whole filter onto KQL,
    // checked for blankness and balance like every spliced raw string.
    // Folded after folder reduction and lifting, which it takes no part in.
    let filter = match (filter, normalized_provider_query(request)) {
        (filter, None) => filter,
        (None, Some(raw)) => Some(SearchFilter::ProviderQuery(raw.to_string())),
        (Some(SearchFilter::And(mut conjuncts)), Some(raw)) => {
            conjuncts.push(SearchFilter::ProviderQuery(raw.to_string()));
            Some(SearchFilter::And(conjuncts))
        }
        (Some(filter), Some(raw)) => Some(SearchFilter::And(vec![
            filter,
            SearchFilter::ProviderQuery(raw.to_string()),
        ])),
    };
    Some(MailboxPlan { folder, filter })
}

/// The request-level raw query, trimmed; blank means none, as on every
/// other provider.
fn normalized_provider_query(request: &SearchRequest) -> Option<&str> {
    request
        .provider_query
        .as_deref()
        .map(str::trim)
        .filter(|raw| !raw.is_empty())
}

/// What a search cursor is bound to: the structured filter and the
/// normalized raw query, everything that decides which messages match and
/// in which folder (the lifted endpoint folder is a function of the filter
/// and the cursor's owner). Not `limit`, which is presentation only and rides
/// in the `nextLink`'s `$top`. `Debug` is a private structural rendering, not
/// wire syntax; a change to it only refuses cursors minted by an older build,
/// which costs a search restart and nothing else.
fn search_binding(request: &SearchRequest) -> String {
    format!(
        "{:?}",
        (&request.filter, normalized_provider_query(request))
    )
}

/// The mailbox the walk visits after `owner` (`None` = after the primary),
/// skipping every mailbox the request's folder restrictions rule out.
fn next_planned_owner(
    account: &GraphAccount,
    request: &SearchRequest,
    owner: Option<&str>,
) -> Option<String> {
    let mut current = owner.map(str::to_string);
    loop {
        let next = next_search_owner(account, current.as_deref())?;
        if plan_for_owner(request, Some(&next)).is_some() {
            return Some(next);
        }
        current = Some(next);
    }
}

pub(super) async fn search_message_rows(
    account: &GraphAccount,
    request: SearchRequest,
    operation: AccountOperation,
) -> Result<Page<SearchRow>, AccountError> {
    let cursor = decode_search_cursor(account, request.page_cursor.as_deref(), operation)?;
    let (mut owner, mut next_link, mut returned) = match cursor {
        // Validated against THIS request before the walk moves anywhere: a
        // cursor minted for another search would otherwise resume (or
        // restart) it and report it as this one. An enterprise user who
        // changes a query expects fresh results, never a continuation of
        // the old ones. (A cursor positioned in a mailbox the request rules
        // out is refused by the loop below.)
        Some(cursor) => {
            if cursor.query != search_binding(&request) {
                return Err(AccountError::invalid_page_cursor(
                    operation,
                    bifrost_types::Protocol::Graph,
                    "Graph search cursor was minted for a different search",
                ));
            }
            (cursor.owner, cursor.next_link, cursor.returned)
        }
        None if plan_for_owner(&request, None).is_some() => (None, None, 0),
        None => match next_planned_owner(account, &request, None) {
            Some(first) => (Some(first), None, 0),
            // Every mailbox is ruled out by the request's folder
            // restrictions (a folder of a mailbox this account does not
            // configure): no message of this account can match.
            None => {
                return Ok(Page {
                    truncated: false,
                    items: Vec::new(),
                    next_cursor: None,
                    estimated_total: None,
                    failed_ids: Vec::new(),
                    skipped_scopes: Vec::new(),
                });
            }
        },
    };
    // Shared mailboxes the walk quarantined instead of searching, reported
    // on the returned page. See the skip arm below for why they are not the
    // call's `Err`.
    let mut skipped_scopes: Vec<SkippedScope> = Vec::new();
    let (client, page, plan) = loop {
        // Every owner the walk reaches was chosen (or its cursor validated)
        // because its plan exists.
        let Some(plan) = plan_for_owner(&request, owner.as_deref()) else {
            return Err(AccountError::invalid_page_cursor(
                operation,
                bifrost_types::Protocol::Graph,
                "Graph search cursor names a mailbox this search excludes",
            ));
        };
        let client = account
            .mailboxes
            .client_for_owner(owner.as_deref())
            .map_err(|error| into_account_error(error, GraphErrorContext::graph(operation)))?;
        let ctx = GraphErrorContext::graph(operation);
        let url = match next_link.take() {
            // The caller's cursor bytes: admitted onto the owning mailbox
            // client's origin before anything is sent, or refused as the
            // caller's malformed input.
            Some(next_link) => client.admit_target(&next_link).map_err(|refusal| {
                AccountError::invalid_page_cursor(
                    operation,
                    bifrost_types::Protocol::Graph,
                    format!("Graph search cursor {refusal}"),
                )
            })?,
            // No `next_link` means "start this mailbox at its first page":
            // either the whole search, or the mailbox after one whose own
            // pagination ran out.
            None => client
                .api_url(&mailbox_search_url(
                    &client.api_path_prefix(),
                    &request,
                    &plan,
                    operation,
                )?)
                .map_err(|error| into_account_error(error, ctx.clone()))?,
        };
        let result: Result<ODataCollection<Value>, _> = client.get(Resource::Outlook, &url).await;
        match result {
            Ok(page) => break (client, page, plan),
            Err(error) => {
                // A shared mailbox this account has lost delegate access to
                // must not end the walk for the mailboxes behind it. The
                // dead mailbox stays configured, so retrying this cursor -
                // or restarting the search from page one - would 403 at the
                // same position forever, leaving the whole search surface
                // dead over one revoked share. Quarantine it the way every
                // other shared-mailbox door does, but report the skip on the
                // page (`Page::skipped_scopes`) instead of raising an engine
                // directive: a search walk has no cursor scope to disable,
                // and skipping SILENTLY would leave "no matches there"
                // indistinguishable from "never searched there". A primary
                // failure (`owner == None`) is a genuine account-level
                // signal, and a non-permission failure is transient enough
                // that retrying this same cursor can succeed; both still
                // fail the call.
                let Some(mailbox) = owner.as_deref() else {
                    return Err(into_account_error(error, ctx));
                };
                let scope = ErrorScope::Owner {
                    id: (mailbox.to_string()).into(),
                };
                let error = into_account_error(error, ctx.with_scope(scope.clone()));
                if !matches!(
                    error.kind(),
                    AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
                ) {
                    return Err(error);
                }
                let next_owner = next_planned_owner(account, &request, Some(mailbox));
                skipped_scopes.push(SkippedScope { scope, error });
                match next_owner {
                    Some(next_owner) => {
                        owner = Some(next_owner);
                        returned = 0;
                    }
                    // The dead mailbox was the walk's last: the search is
                    // complete, minus the quarantined scopes it reports.
                    None => {
                        return Ok(Page {
                            truncated: false,
                            items: Vec::new(),
                            next_cursor: None,
                            estimated_total: None,
                            failed_ids: Vec::new(),
                            skipped_scopes,
                        });
                    }
                }
            }
        }
    };
    let mut rows = Vec::new();
    for value in page.value {
        let id = object_id_from_value(&value, operation, None, acknowledged())?;
        let id = ObjectId(crate::account::foreign::qualify_with_owner(
            owner.as_deref(),
            &id.0,
        ));
        let thread_id = value
            .get("conversationId")
            .and_then(Value::as_str)
            .map(|id| {
                ThreadId(crate::account::foreign::qualify_with_owner(
                    owner.as_deref(),
                    id,
                ))
            });
        rows.push(SearchRow { id, thread_id });
    }
    // Admitted at receipt by the client that fetched the page, before it
    // can be minted into the caller's cursor.
    let next_link = client
        .admit_next(page.next_link.as_ref())
        .map_err(|error| into_account_error(error, GraphErrorContext::graph(operation)))?;
    // Raw rows, counted before any thread deduplication, because the
    // ceiling is Graph's count of messages. Cursor bytes are caller input,
    // so the sum saturates rather than wrapping below the ceiling.
    let returned = returned
        .saturating_add(u32::try_from(rows.len()).unwrap_or(u32::MAX))
        .min(SEARCH_RESULT_CEILING);
    // A `$search` mailbox that runs out of pages AT the ceiling was cut
    // short by Graph, not exhausted (exactly 1,000 genuine matches cannot be
    // told apart, and reads as possibly incomplete, as Outlook's "there may
    // be more results" does). Reported on THIS page even when the walk
    // continues into the next shared mailbox: the cut part has no
    // continuation, so a consumer ORs `truncated` across the walk's pages.
    let truncated =
        next_link.is_none() && plan_uses_search(&plan) && returned >= SEARCH_RESULT_CEILING;
    let query = search_binding(&request);
    let next_cursor = match next_link {
        Some(next_link) => Some(encode_search_cursor(
            account,
            operation,
            SearchPosition {
                owner: owner.as_deref(),
                next_link: Some(next_link.as_str().to_string()),
                query,
                returned,
            },
        )),
        None => next_planned_owner(account, &request, owner.as_deref()).map(|next| {
            encode_search_cursor(
                account,
                operation,
                SearchPosition {
                    owner: Some(&next),
                    next_link: None,
                    query,
                    returned: 0,
                },
            )
        }),
    };
    Ok(Page {
        truncated,
        items: rows,
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes,
    })
}

/// Decode a caller-supplied search page cursor.
///
/// Every rejection here is `Request(Malformed)`, not `Protocol(...)`: these
/// bytes are request INPUT the caller handed back, so corrupt or stale
/// client state must recommend fixing the request (`ClientBug` ->
/// `FixClientRequest`) rather than accusing Graph of a contract violation.
pub(super) fn decode_search_cursor(
    account: &GraphAccount,
    cursor: Option<&[u8]>,
    operation: AccountOperation,
) -> Result<Option<SearchCursor>, AccountError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let cursor: SearchCursor =
        crate::paging::decode_cursor(cursor, operation).map_err(|detail| {
            AccountError::invalid_page_cursor(operation, bifrost_types::Protocol::Graph, detail)
        })?;
    if cursor.mailboxes != search_mailboxes(account) {
        return Err(AccountError::invalid_page_cursor(
            operation,
            bifrost_types::Protocol::Graph,
            "Graph search cursor was minted against a different shared-mailbox set",
        ));
    }
    // Every link this crate mints into a search cursor is an admitted,
    // absolute server link. Anything else is not a cursor it produced; the
    // origin itself is checked against the owning mailbox's client when the
    // walk resumes.
    if cursor
        .next_link
        .as_deref()
        .is_some_and(|link| reqwest::Url::parse(link).is_err())
    {
        return Err(AccountError::invalid_page_cursor(
            operation,
            bifrost_types::Protocol::Graph,
            "Graph search cursor link is not an absolute URL",
        ));
    }
    Ok(Some(cursor))
}

/// Where a search walk resumes; see [`SearchCursor`] for each field.
pub(super) struct SearchPosition<'a> {
    pub(super) owner: Option<&'a str>,
    pub(super) next_link: Option<String>,
    pub(super) query: String,
    pub(super) returned: u32,
}

pub(super) fn encode_search_cursor(
    account: &GraphAccount,
    operation: AccountOperation,
    position: SearchPosition<'_>,
) -> bifrost_types::PageCursor {
    let cursor = SearchCursor {
        mailboxes: search_mailboxes(account),
        owner: position.owner.map(str::to_string),
        next_link: position.next_link,
        query: position.query,
        returned: position.returned,
    };
    crate::paging::encode_cursor(operation, &cursor)
}

/// The shared mailboxes the search walk visits after the primary, in the
/// order it visits them. Sorted, so the walk order does not ride on
/// `HashMap` iteration order and two runs of the same account agree.
pub(super) fn search_mailboxes(account: &GraphAccount) -> Vec<String> {
    let mut mailboxes: Vec<String> = account.mailboxes.shared().keys().cloned().collect();
    mailboxes.sort_unstable();
    mailboxes
}

pub(super) fn next_search_owner(account: &GraphAccount, owner: Option<&str>) -> Option<String> {
    let mailboxes = search_mailboxes(account);
    match owner {
        None => mailboxes.into_iter().next(),
        Some(owner) => mailboxes
            .into_iter()
            .skip_while(|mailbox| !mailbox.eq_ignore_ascii_case(owner))
            .nth(1),
    }
}

/// Whether this mailbox's request goes through `$search`, and so is subject
/// to [`SEARCH_RESULT_CEILING`]. A request-level `provider_query` is already
/// a `ProviderQuery` conjunct of the plan's filter.
pub(super) fn plan_uses_search(plan: &MailboxPlan) -> bool {
    plan.filter.as_ref().is_some_and(filter_requires_search)
}

/// The first-page URL for the primary mailbox. Test convenience over
/// [`mailbox_search_url`]; a request whose folder restrictions rule the
/// primary out has no URL there.
#[cfg(test)]
pub(super) fn search_url(prefix: &str, request: &SearchRequest) -> Result<String, AccountError> {
    search_url_for(prefix, request, AccountOperation::Search)
}

/// [`search_url`] for a named operation.
#[cfg(test)]
pub(super) fn search_url_for(
    prefix: &str,
    request: &SearchRequest,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    let plan = plan_for_owner(request, None).expect("the request searches the primary mailbox");
    mailbox_search_url(prefix, request, &plan, operation)
}

/// Build the FIRST-page URL for one mailbox. Continuations never come
/// through here: `request.page_cursor` is decoded once, by
/// `decode_search_cursor`, and a decoded cursor either carries the
/// mailbox's own `nextLink` or asks for this mailbox's first page.
pub(super) fn mailbox_search_url(
    prefix: &str,
    request: &SearchRequest,
    plan: &MailboxPlan,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    let collection = match &plan.folder {
        Some(folder) => format!(
            "{prefix}/mailFolders/{}/messages",
            bifrost_net::url::encode_path_component(folder)
        ),
        None => format!("{prefix}/messages"),
    };
    let mut params = vec![
        "$select=id,conversationId".to_string(),
        format!("$top={}", request.limit.unwrap_or(50).clamp(1, 250)),
    ];
    // Graph `/messages` forbids combining `$search` with `$filter` in one
    // request (it answers 400), so a request is one or the other, never
    // both. A filter with any KQL-only leaf (From/To, since `$filter`
    // `contains()` on the sender/recipient navigation properties is
    // rejected; an attachment name; raw KQL, including the request-level
    // `provider_query` that `plan_for_owner` folded in as a leaf) goes
    // *whole* onto `$search`/KQL. Everything else stays on `$filter`.
    // The plan's filter, not the request's: folder restrictions are already
    // reduced against this mailbox and the endpoint folder lifted out.
    if let Some(filter) = &plan.filter {
        if filter_requires_search(filter) {
            let search = kql_filter(filter, operation)?;
            params.push(format!(
                "$search={}",
                bifrost_net::url::encode_query_value(&format!("\"{search}\""))
            ));
        } else {
            // Never empty: constants were folded out of the plan.
            let filter = odata_filter(filter, operation)?;
            params.push(format!(
                "$filter={}",
                bifrost_net::url::encode_query_value(&filter)
            ));
        }
    }
    Ok(format!("{collection}?{}", params.join("&")))
}

/// True if any leaf of the filter tree exists only in KQL: a `From`/`To`
/// substring match (Graph `$filter` rejects `contains()` on the
/// sender/recipient navigation properties), an attachment-name search
/// (`attachments` is not filterable on messages; KQL has `attachment:`), or a
/// raw KQL leaf. The whole request then routes through `$search`/KQL when one
/// of these is present anywhere in the boolean composition.
pub(super) fn filter_requires_search(filter: &SearchFilter) -> bool {
    match filter {
        SearchFilter::From(_)
        | SearchFilter::To(_)
        | SearchFilter::Attachment(AttachmentFilter::Name(_))
        | SearchFilter::ProviderQuery(_) => true,
        SearchFilter::And(filters) | SearchFilter::Or(filters) => {
            filters.iter().any(filter_requires_search)
        }
        SearchFilter::Not(inner) => filter_requires_search(inner),
        _ => false,
    }
}

/// Express the whole filter tree as a KQL `$search` string.
///
/// Used only when `filter_requires_search` holds, because Graph cannot mix
/// `$search` with `$filter`. KQL property restrictions cover sender,
/// recipient, subject, body, attachment presence and name, category, and a
/// date range, plus raw KQL leaves; KQL terms are AND/OR/NOT-composed. KQL
/// has no folder property: a positive top-level `In` never reaches here
/// (`plan_for_owner` lifts it into the `/mailFolders/{id}/messages`
/// endpoint), and any other `In` - under `Or`/`Not`, or a second different
/// top-level folder - is a clean `Request(Malformed)` rather than silently
/// dropped (returning matches from other folders) or sent as an invalid
/// mixed request.
///
/// `operation` stamps every refusal, so a `SearchMessages` request refused
/// here reports `SearchMessages`, not `Search`.
pub(super) fn kql_filter(
    filter: &SearchFilter,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    kql_compile(filter, true, operation)
}

/// `positive` is the leaf's Boolean polarity: true at the root and through
/// `And`/`Or`, flipped by each `Not`. Only date rounding reads it.
fn kql_compile(
    filter: &SearchFilter,
    positive: bool,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    match filter {
        SearchFilter::From(value) => Ok(format!("from:{}", kql_quoted(value))),
        // KQL `to:` and `cc:` cover the recipient set; there is no KQL
        // bcc property, matching Graph's search surface.
        SearchFilter::To(value) => Ok(format!("(to:{0} OR cc:{0})", kql_quoted(value))),
        SearchFilter::Subject(value) => Ok(format!("subject:{}", kql_quoted(value))),
        SearchFilter::Body(value) => Ok(format!("body:{}", kql_quoted(value))),
        SearchFilter::Attachment(AttachmentFilter::Any) => Ok("hasattachment:true".to_string()),
        // Exchange's own attachment-name search (word matching), which is
        // what the shared Name form promises.
        SearchFilter::Attachment(AttachmentFilter::Name(name)) => {
            if name.trim().is_empty() {
                return Err(invalid_account_error(
                    operation,
                    "attachment name search needs a name",
                ));
            }
            Ok(format!("attachment:{}", kql_quoted(name)))
        }
        // No KQL or OData property carries an attachment's MIME type.
        SearchFilter::Attachment(_) => Err(unsupported_account_error(operation)),
        // Raw KQL (a leaf, or the request-level `provider_query` that
        // `plan_for_owner` folded in as one), escaped and grouped by `kql_join`/`NOT (...)` like every other leaf; the balance
        // check stops an unmatched `)` or quote from escaping that group.
        SearchFilter::ProviderQuery(raw) => {
            let raw = raw.trim();
            if raw.is_empty() {
                return Err(invalid_account_error(
                    operation,
                    "provider query leaf is empty",
                ));
            }
            if !balanced_raw_query(raw) {
                return Err(invalid_account_error(
                    operation,
                    "provider query leaf has unbalanced parentheses or quotes",
                ));
            }
            Ok(format!("({})", graph_search_escape(raw)))
        }
        // Unverified: `$filter` matches a label as exact category membership,
        // but Graph's message `$search` property list does not name
        // `category`, and KQL text properties are usually phrase-matched, so
        // `category:"Red"` may also match "Dark Red" (and then, beneath
        // `NOT`, drop real matches) or miss some. Only a live probe can say.
        // Kept rather than refused because refusing removes the Outlook
        // search "from X with category Y". If the probe shows it inexact, the
        // remedies are refusal on this path or a superset query (drop the
        // label restriction), fetching `categories` and evaluating the
        // original tree exactly, with raw rows counted against the 1,000 cap.
        SearchFilter::Labeled(label) => {
            Ok(format!("category:{}", kql_quoted(&label_id_native(label))))
        }
        // The shared range is against send time, as the `$filter` path's
        // `sentDateTime` is, so KQL uses `sent`, not `received`: a sibling
        // leaf forcing KQL must not move the clock. KQL takes whole dates, so
        // each bound is rounded by polarity: outward where the range is
        // asserted (after floors, before ceils), inward beneath an odd number
        // of `Not`s (after ceils, before floors), so the negation of the
        // narrowed range still covers the exact complement. Either way the
        // date approximation never drops a matching message; each bound moves
        // by under a day.
        SearchFilter::DateRange { after, before } => {
            let mut parts = Vec::new();
            if let Some(after) = after {
                parts.push(format!("sent>={}", kql_day(*after, !positive, operation)?));
            }
            if let Some(before) = before {
                parts.push(format!("sent<{}", kql_day(*before, positive, operation)?));
            }
            nonempty_fragment(parts.join(" AND "), operation)
        }
        SearchFilter::And(filters) => kql_join(filters, "AND", positive, operation),
        SearchFilter::Or(filters) => kql_join(filters, "OR", positive, operation),
        SearchFilter::Not(filter) => Ok(format!(
            "NOT ({})",
            kql_compile(filter, !positive, operation)?
        )),
        // A folder restriction the endpoint cannot carry has no KQL
        // property; failing cleanly beats shipping a request that would
        // silently search every folder.
        SearchFilter::In(_) => Err(invalid_account_error(
            operation,
            "Graph search can scope a sender/recipient, attachment-name or \
             raw KQL term to one folder only as a top-level AND condition \
             (KQL has no folder property); a folder under OR or NOT, or a \
             second folder, cannot be combined with such a term",
        )),
        _ => Err(unsupported_account_error(operation)),
    }
}

/// The UTC date of `time`, rounded up to the next date when `ceil` is set
/// and `time` is not exactly midnight (sub-second precision included). A
/// bound outside the representable range, or a ceiling past its last date,
/// is refused: substituting another date would move the bound the wrong
/// way and drop matches.
fn kql_day(
    time: std::time::SystemTime,
    ceil: bool,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    let civil = utc_civil(time, operation)?;
    let mut date = civil.date();
    if ceil && civil.time() != jiff::civil::Time::midnight() {
        date = date.tomorrow().map_err(|_| {
            invalid_account_error(
                operation,
                "date bound rounds past the last date KQL can represent",
            )
        })?;
    }
    Ok(date.strftime("%Y-%m-%d").to_string())
}

/// Every compiled fragment is a real predicate. Constants (an unbounded
/// range, an empty `And`/`Or`) are folded by `reduce_for_owner` before
/// either compiler runs; an empty rendering here would silently vanish from
/// a join (narrowing an `Or`) or produce `NOT ()`, so it is refused.
fn nonempty_fragment(
    fragment: String,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    if fragment.is_empty() {
        Err(invalid_account_error(
            operation,
            "search filter holds a constant (an unbounded date range or an \
             empty AND/OR) that was not folded before compiling",
        ))
    } else {
        Ok(fragment)
    }
}

fn kql_join(
    filters: &[SearchFilter],
    op: &str,
    positive: bool,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    let parts = filters
        .iter()
        .map(|filter| kql_compile(filter, positive, operation).map(|part| format!("({part})")))
        .collect::<Result<Vec<_>, _>>()?;
    nonempty_fragment(parts.join(&format!(" {op} ")), operation)
}

/// KQL-quoted string value: wrap in double quotes (so multi-word values are
/// one phrase, not OR-ed tokens) and escape embedded double quotes.
pub(super) fn kql_quoted(value: &str) -> String {
    format!("\"{}\"", graph_search_escape(value))
}

/// `operation` stamps every refusal, so a `SearchMessages` request refused
/// here reports `SearchMessages`, not `Search`.
pub(super) fn odata_filter(
    filter: &SearchFilter,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    // `From`/`To` are never reached here: any filter tree containing a
    // sender/recipient substring leaf is detected by `filter_requires_search`
    // in `mailbox_search_url` and routed onto `$search`/KQL instead (Graph
    // `$filter` rejects `contains()` on those navigation properties with a
    // 400). The arms below are kept as a defensive fallback for a direct
    // `odata_filter` call and use the rejected `contains()` shape, but the
    // live path never emits them.
    match filter {
        SearchFilter::From(value) => Ok(format!(
            "(contains(from/emailAddress/address,{0}) or contains(from/emailAddress/name,{0}))",
            odata_quoted(value)
        )),
        SearchFilter::To(value) => Ok(format!(
            "(toRecipients/any(r:contains(r/emailAddress/address,{0}) or contains(r/emailAddress/name,{0})) or ccRecipients/any(r:contains(r/emailAddress/address,{0}) or contains(r/emailAddress/name,{0})) or bccRecipients/any(r:contains(r/emailAddress/address,{0}) or contains(r/emailAddress/name,{0})))",
            odata_quoted(value)
        )),
        SearchFilter::Subject(value) => Ok(format!("contains(subject,{})", odata_quoted(value))),
        SearchFilter::Body(value) => Ok(format!("contains(body/content,{})", odata_quoted(value))),
        SearchFilter::Attachment(AttachmentFilter::Any) => Ok("hasAttachments eq true".to_string()),
        // `Name` and `ProviderQuery` never reach here (`filter_requires_search`
        // routes them to KQL), and no OData property carries a MIME type.
        SearchFilter::Attachment(_) | SearchFilter::ProviderQuery(_) => {
            Err(unsupported_account_error(operation))
        }
        // Already reduced against the searched mailbox by `plan_for_owner`,
        // so the id is native (owner qualification stripped) and belongs to
        // this mailbox; another mailbox's folder became constant false.
        SearchFilter::In(container) => {
            Ok(format!("parentFolderId eq {}", odata_quoted(&container.0)))
        }
        SearchFilter::Labeled(label) => Ok(format!(
            "categories/any(c:c eq {})",
            odata_quoted(&label_id_native(label))
        )),
        SearchFilter::DateRange { after, before } => {
            let mut parts = Vec::new();
            if let Some(after) = after {
                parts.push(format!(
                    "sentDateTime ge {}",
                    system_time_rfc3339(*after, operation)?
                ));
            }
            if let Some(before) = before {
                parts.push(format!(
                    "sentDateTime lt {}",
                    system_time_rfc3339(*before, operation)?
                ));
            }
            nonempty_fragment(parts.join(" and "), operation)
        }
        SearchFilter::And(filters) => join_filters(filters, "and", operation),
        SearchFilter::Or(filters) => join_filters(filters, "or", operation),
        SearchFilter::Not(filter) => Ok(format!("not ({})", odata_filter(filter, operation)?)),
        _ => Err(unsupported_account_error(operation)),
    }
}

pub(super) fn join_filters(
    filters: &[SearchFilter],
    op: &str,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    let parts = filters
        .iter()
        .map(|filter| odata_filter(filter, operation).map(|part| format!("({part})")))
        .collect::<Result<Vec<_>, _>>()?;
    nonempty_fragment(parts.join(&format!(" {op} ")), operation)
}

pub(super) fn label_id_native(label: &LabelId) -> String {
    label.0.clone()
}

pub(super) fn odata_quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(super) fn graph_search_escape(value: &str) -> String {
    value.replace('"', "\\\"")
}
