//! Account search compilation and position-based paging.

use super::containers::folder_from_container;
use super::error::{folder_err, or_folder_restriction_error, pim_malformed, search_cursor_voided};
use super::{
    ImapAccount, decode_object_id, encode_object_id, encode_thread_id, selected_uidvalidity,
};
use crate::types::{MailboxName, SearchCriteria, ThreadNode};
use bifrost_types::ids::{ObjectId, ThreadId};
use bifrost_types::page::{Page, SkippedScope};
use bifrost_types::search::{AttachmentFilter, SearchFilter, SearchRequest};
use bifrost_types::{AccountError, AccountFuture, AccountOperation, LabelId, Protocol};
use jiff::Timestamp;
use jiff::tz::Offset;
use std::time::SystemTime;

pub(crate) fn search(
    account: ImapAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        account
            .capabilities
            .pim_methods
            .gate(AccountOperation::Search, Protocol::Imap)?;
        let plan = search_plan(&account.dav_scopes, &request, AccountOperation::Search)?;
        let mut pager = SearchPager::new(&request, plan.folder.as_ref(), AccountOperation::Search)?;
        pager.include_discovery_skips(&account.discovery_skips, plan.folder.as_ref());
        for folder in pager.folders(search_folders(&account, plan.folder.as_ref())) {
            if pager.is_full() {
                break;
            }
            let walk = account.begin_walk(&folder);
            let err = folder_err(&walk, AccountOperation::Search);
            // Checkout can authenticate a fresh session: its failures are
            // account-level even when a provider reports permission denial.
            let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
            let outcome = async {
                let selected = account
                    .select_walk(&mut conn, &walk, super::SelectPurpose::Read)
                    .await
                    .map_err(err)?;
                let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(err)?;
                pager.check_epoch(&folder, uidvalidity)?;
                let timeout = account.command_timeout();
                let (roots, matching) = match &plan.query {
                    SearchQuery::Server(criteria) => (
                        conn.connection()
                            .uid_thread("REFERENCES", "UTF-8", criteria, timeout)
                            .await
                            .map_err(err)?,
                        None,
                    ),
                    // Attachment-aware thread search groups the matching
                    // messages by the folder's conversations: the whole
                    // folder is threaded and each thread keeps only its
                    // matching members. Threading the matching set itself
                    // would need a `UID <set>` operand of unbounded length,
                    // and threading the over-approximation would make the
                    // grouping depend on how the filter was compiled (RFC
                    // 5256 dummy pruning and subject grouping change with
                    // the input set). Against a Has-free search, which
                    // threads only the matching messages, grouping can
                    // differ only in those RFC 5256 edge cases.
                    SearchQuery::Attachment { tree, universe } => {
                        let matching = attachment::matching_uids(
                            conn.connection(),
                            attachment::Surface::Threads,
                            tree,
                            universe,
                            timeout,
                        )
                        .await
                        .map_err(err)?;
                        let roots = if matching.is_empty() {
                            Vec::new()
                        } else {
                            conn.connection()
                                .uid_thread(
                                    "REFERENCES",
                                    "UTF-8",
                                    SearchCriteria::new().all(),
                                    timeout,
                                )
                                .await
                                .map_err(err)?
                        };
                        (roots, Some(matching))
                    }
                };
                // A thread's position is its lowest member UID: member sets are
                // disjoint, so the minima order the folder's threads totally,
                // and an expunge elsewhere cannot move an untouched thread.
                // The key is taken after pruning to matching members, so a
                // matching message below the cursor still anchors its thread.
                let mut threads = Vec::new();
                for root in roots {
                    let mut uids = Vec::new();
                    flatten_thread(&root, &mut uids);
                    if let Some(matching) = &matching {
                        uids.retain(|uid| matching.contains(uid));
                    }
                    if let Some(&key) = uids.iter().min() {
                        threads.push((key, uids));
                    }
                }
                pager.accept(&folder, uidvalidity, threads, |uids| {
                    encode_thread_id(&folder, uidvalidity, &super::UidSet::from_numbers(uids))
                })?;
                Ok(())
            }
            .await;
            if let Err(error) = outcome {
                pager.skip_or_fail(&folder, error)?;
            }
        }
        Ok(pager.finish())
    })
}

pub(crate) fn search_messages(
    account: ImapAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
    Box::pin(async move {
        account
            .capabilities
            .pim_methods
            .gate(AccountOperation::SearchMessages, Protocol::Imap)?;
        let plan = search_plan(
            &account.dav_scopes,
            &request,
            AccountOperation::SearchMessages,
        )?;
        let mut pager = SearchPager::new(
            &request,
            plan.folder.as_ref(),
            AccountOperation::SearchMessages,
        )?;
        pager.include_discovery_skips(&account.discovery_skips, plan.folder.as_ref());
        for folder in pager.folders(search_folders(&account, plan.folder.as_ref())) {
            if pager.is_full() {
                break;
            }
            let walk = account.begin_walk(&folder);
            let err = folder_err(&walk, AccountOperation::SearchMessages);
            // Checkout can authenticate a fresh session: its failures are
            // account-level even when a provider reports permission denial.
            let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
            let outcome = async {
                let selected = account
                    .select_walk(&mut conn, &walk, super::SelectPurpose::Read)
                    .await
                    .map_err(err)?;
                let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(err)?;
                pager.check_epoch(&folder, uidvalidity)?;
                let ids: Vec<u32> = match &plan.query {
                    SearchQuery::Server(criteria) => {
                        conn.connection()
                            .uid_search(criteria, account.command_timeout())
                            .await
                            .map_err(err)?
                            .ids
                    }
                    SearchQuery::Attachment { tree, universe } => attachment::matching_uids(
                        conn.connection(),
                        attachment::Surface::Messages,
                        tree,
                        universe,
                        account.command_timeout(),
                    )
                    .await
                    .map_err(err)?
                    .into_iter()
                    .collect(),
                };
                let uids = ids.into_iter().map(|uid| (uid, uid)).collect();
                pager.accept(&folder, uidvalidity, uids, |uid| {
                    encode_object_id(
                        &folder,
                        uidvalidity,
                        crate::types::Uid::new(uid).expect("wire UID is nonzero"),
                    )
                })?;
                Ok(())
            }
            .await;
            if let Err(error) = outcome {
                pager.skip_or_fail(&folder, error)?;
            }
        }
        Ok(pager.finish())
    })
}

/// A compiled filter. Every maximal attachment-free subtree collapses into
/// one `Server` SEARCH criteria; only `Has` and the boolean nodes above it
/// survive as structure, because IMAP SEARCH cannot evaluate them.
#[derive(Debug, Clone, PartialEq)]
enum FilterNode {
    Server(SearchCriteria),
    Has(AttachmentFilter),
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    Not(Box<FilterNode>),
}

impl FilterNode {
    fn all() -> Self {
        Self::Server(SearchCriteria::new().all())
    }

    fn is_all(&self) -> bool {
        matches!(self, Self::Server(criteria) if *criteria == "ALL")
    }

    /// A SEARCH criteria every match of this node satisfies: `Has` widens to
    /// `ALL`, and so does any negation over an attachment subtree, since
    /// the complement of an over-approximation is not one.
    fn over_approximation(&self) -> SearchCriteria {
        match self {
            Self::Server(criteria) => criteria.clone(),
            Self::Has(_) | Self::Not(_) => SearchCriteria::new().all(),
            Self::And(nodes) => {
                let parts: Vec<_> = nodes
                    .iter()
                    .map(Self::over_approximation)
                    .filter(|criteria| *criteria != "ALL")
                    .collect();
                if parts.is_empty() {
                    SearchCriteria::new().all()
                } else {
                    parts
                        .into_iter()
                        .fold(SearchCriteria::new(), SearchCriteria::and)
                }
            }
            Self::Or(nodes) => {
                let parts: Vec<_> = nodes.iter().map(Self::over_approximation).collect();
                if parts.is_empty() || parts.iter().any(|criteria| *criteria == "ALL") {
                    return SearchCriteria::new().all();
                }
                or_chain(parts)
            }
        }
    }
}

/// How a request runs against one folder.
#[derive(Debug)]
enum SearchQuery {
    /// One server command, as for any filter without `Has`.
    Server(SearchCriteria),
    /// Attachment-aware set evaluation (see `search/attachment.rs`):
    /// `universe` is the over-approximation conjoined with the provider
    /// query, and `tree` is evaluated exactly inside it.
    Attachment {
        tree: FilterNode,
        universe: SearchCriteria,
    },
}

#[derive(Debug)]
struct SearchPlan {
    query: SearchQuery,
    folder: Option<MailboxName>,
}

#[cfg(test)]
impl SearchPlan {
    fn criteria(&self) -> SearchCriteria {
        match &self.query {
            SearchQuery::Server(criteria) => criteria.clone(),
            SearchQuery::Attachment { .. } => panic!("attachment plan has no single criteria"),
        }
    }
}

fn search_plan(
    scopes: &super::DavScopeIndex,
    request: &SearchRequest,
    op: AccountOperation,
) -> Result<SearchPlan, AccountError> {
    let plan = match &request.filter {
        Some(filter) => criteria_from_filter(scopes, filter, op)?,
        None => CriteriaPart {
            node: FilterNode::all(),
            folder: None,
        },
    };
    let (mut criteria, tree) = match plan.node {
        FilterNode::Server(criteria) => (criteria, None),
        tree => (tree.over_approximation(), Some(tree)),
    };
    if let Some(raw) = request
        .provider_query
        .as_deref()
        .filter(|raw| !raw.trim().is_empty())
    {
        validate_provider_query(raw, op)?;
        if criteria == "ALL" {
            criteria = SearchCriteria::raw(raw.trim().to_owned(), Vec::new());
        } else {
            criteria = criteria.and(SearchCriteria::raw(raw.trim().to_owned(), Vec::new()));
        }
    }
    if criteria.trim().is_empty() {
        criteria = SearchCriteria::new().all();
    }
    let query = match tree {
        None => SearchQuery::Server(criteria),
        Some(tree) => SearchQuery::Attachment {
            tree,
            universe: criteria,
        },
    };
    Ok(SearchPlan {
        query,
        folder: plan.folder,
    })
}

/// Structural guard for caller-written raw criteria at the account surface:
/// unbalanced parentheses, an unterminated quote, or an IMAP literal marker
/// are `Request(Malformed)`. Refusing literals here is deliberate, not a gap
/// in the connection-level literal support: a literal is the only context in
/// which the encoder lets a CRLF through inside criteria, so refusing it
/// keeps every account caller's CRLF off the wire. (Connection-level raw
/// `SearchCriteria` keeps caller-written literals; typed operands still go
/// out as literals when the writer chooses one.)
fn validate_provider_query(raw: &str, op: AccountOperation) -> Result<(), AccountError> {
    let bytes = raw.as_bytes();
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    let mut pos = 0usize;

    while pos < bytes.len() {
        let byte = bytes[pos];
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            pos += 1;
            continue;
        }

        match byte {
            b'"' => quoted = true,
            b'(' => depth += 1,
            b')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| pim_malformed(op, "provider query has an unmatched ')'"))?;
            }
            b'{' => {
                let mut end = pos + 1;
                let digits_start = end;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if end > digits_start {
                    if bytes.get(end) == Some(&b'+') {
                        end += 1;
                    }
                    if bytes.get(end) == Some(&b'}') {
                        return Err(pim_malformed(
                            op,
                            "provider query must not contain an IMAP literal",
                        ));
                    }
                }
            }
            _ => {}
        }
        pos += 1;
    }

    if quoted {
        return Err(pim_malformed(
            op,
            "provider query has an unterminated quoted string",
        ));
    }
    if depth != 0 {
        return Err(pim_malformed(op, "provider query has an unmatched '('"));
    }
    Ok(())
}

#[derive(Debug)]
struct CriteriaPart {
    node: FilterNode,
    folder: Option<MailboxName>,
}

#[cfg(test)]
impl CriteriaPart {
    fn criteria(&self) -> SearchCriteria {
        match &self.node {
            FilterNode::Server(criteria) => criteria.clone(),
            _ => panic!("attachment part has no single criteria"),
        }
    }
}

fn criteria_from_filter(
    scopes: &super::DavScopeIndex,

    filter: &SearchFilter,
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    match filter {
        SearchFilter::From(value) => leaf(SearchCriteria::new().from(value), op),
        SearchFilter::To(value) => leaf(SearchCriteria::new().to(value), op),
        SearchFilter::Subject(value) => leaf(SearchCriteria::new().subject(value), op),
        SearchFilter::Body(value) => leaf(SearchCriteria::new().body(value), op),
        // No IMAP SEARCH key sees MIME part headers or parameters, and BODY
        // matches prose, so attachments are judged from BODYSTRUCTURE by
        // `search/attachment.rs`, composed with the rest of the filter as set
        // algebra over UIDs.
        SearchFilter::Attachment(attachment) => {
            match attachment {
                AttachmentFilter::Name(name) if name.trim().is_empty() => {
                    return Err(pim_malformed(op, "attachment name search needs a name"));
                }
                AttachmentFilter::Any
                | AttachmentFilter::Name(_)
                | AttachmentFilter::MediaType(_) => {}
                _ => return Err(super::error::unsupported(op)),
            }
            Ok(CriteriaPart {
                node: FilterNode::Has(attachment.clone()),
                folder: None,
            })
        }
        // The same raw SEARCH criteria `provider_query` takes, under the same
        // validation, grouped so a multi-key operand stays one operand of
        // the surrounding OR / NOT.
        SearchFilter::ProviderQuery(raw) => {
            if raw.trim().is_empty() {
                return Err(pim_malformed(op, "provider query leaf is empty"));
            }
            validate_provider_query(raw, op)?;
            Ok(CriteriaPart {
                node: FilterNode::Server(
                    SearchCriteria::new()
                        .group(SearchCriteria::raw(raw.trim().to_owned(), Vec::new())),
                ),
                folder: None,
            })
        }
        SearchFilter::In(container) => Ok(CriteriaPart {
            node: FilterNode::all(),
            folder: Some(folder_from_container(scopes, container, op)?),
        }),
        SearchFilter::Labeled(LabelId(label)) => leaf(SearchCriteria::new().keyword(label), op),
        SearchFilter::DateRange { after, before } => {
            let mut criteria = SearchCriteria::new();
            if let Some(after) = after {
                criteria = criteria
                    .sent_since(&imap_date(*after))
                    .map_err(|e| pim_malformed(op, e.to_string()))?;
            }
            if let Some(before) = before {
                criteria = criteria
                    .sent_before(&imap_date(*before))
                    .map_err(|e| pim_malformed(op, e.to_string()))?;
            }
            Ok(CriteriaPart {
                node: FilterNode::Server(empty_to_all(criteria)),
                folder: None,
            })
        }
        SearchFilter::And(filters) => combine_and(scopes, filters, op),
        SearchFilter::Or(filters) => combine_or(scopes, filters, op),
        SearchFilter::Not(filter) => {
            // The folder restriction is metadata on the whole search, not a
            // set the negation complements: it is carried out unchanged.
            let inner = criteria_from_filter(scopes, filter, op)?;
            let node = match inner.node {
                FilterNode::Server(criteria) => FilterNode::Server(
                    SearchCriteria::new().not(&SearchCriteria::new().group(criteria)),
                ),
                tree => FilterNode::Not(Box::new(tree)),
            };
            Ok(CriteriaPart {
                node,
                folder: inner.folder,
            })
        }
        _ => Err(super::error::unsupported(op)),
    }
}

fn leaf(
    result: Result<SearchCriteria, crate::Error>,
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    let criteria = result.map_err(|e| pim_malformed(op, e.to_string()))?;
    Ok(CriteriaPart {
        node: FilterNode::Server(empty_to_all(criteria)),
        folder: None,
    })
}

fn combine_and(
    scopes: &super::DavScopeIndex,

    filters: &[SearchFilter],
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    let mut nodes = Vec::new();
    let mut folder = None;
    for filter in filters {
        let part = criteria_from_filter(scopes, filter, op)?;
        folder = merge_folder(folder, part.folder, op)?;
        if !part.node.is_all() {
            nodes.push(part.node);
        }
    }
    let node = if nodes
        .iter()
        .all(|node| matches!(node, FilterNode::Server(_)))
    {
        let criteria: Vec<SearchCriteria> = nodes
            .into_iter()
            .filter_map(|node| match node {
                FilterNode::Server(criteria) => Some(criteria),
                _ => None,
            })
            .collect();
        FilterNode::Server(if criteria.is_empty() {
            SearchCriteria::new().all()
        } else {
            criteria
                .into_iter()
                .fold(SearchCriteria::new(), SearchCriteria::and)
        })
    } else if nodes.len() == 1 {
        nodes.remove(0)
    } else {
        FilterNode::And(nodes)
    };
    Ok(CriteriaPart { node, folder })
}

/// RFC 3501 OR takes exactly two search keys, so a longer OR nests.
fn or_chain(parts: Vec<SearchCriteria>) -> SearchCriteria {
    let mut iter = parts.into_iter();
    let mut criteria = iter.next().unwrap_or_else(|| SearchCriteria::new().all());
    for next in iter {
        criteria = SearchCriteria::new().or(
            &SearchCriteria::new().group(criteria),
            &SearchCriteria::new().group(next),
        );
    }
    criteria
}

fn combine_or(
    scopes: &super::DavScopeIndex,

    filters: &[SearchFilter],
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    if filters.is_empty() {
        return Ok(CriteriaPart {
            node: FilterNode::all(),
            folder: None,
        });
    }
    let mut parts = Vec::new();
    for filter in filters {
        let part = criteria_from_filter(scopes, filter, op)?;
        // A folder restriction (`In`) nested inside an `Or` cannot be
        // expressed in a single-mailbox IMAP SEARCH: SEARCH runs against
        // the currently-selected mailbox, so hoisting the `In` folder to
        // the whole OR group would silently restrict the *other* OR
        // branches to that folder too, changing the query's meaning. The
        // AND case is defensible (the folder narrows the whole
        // conjunction); the OR case is not, so reject it explicitly
        // rather than leak the restriction.
        if part.folder.is_some() {
            return Err(or_folder_restriction_error(op));
        }
        parts.push(part.node);
    }
    let node = if parts
        .iter()
        .all(|node| matches!(node, FilterNode::Server(_)))
    {
        FilterNode::Server(or_chain(
            parts
                .into_iter()
                .filter_map(|node| match node {
                    FilterNode::Server(criteria) => Some(criteria),
                    _ => None,
                })
                .collect(),
        ))
    } else {
        FilterNode::Or(parts)
    };
    Ok(CriteriaPart { node, folder: None })
}

#[allow(clippy::unwrap_in_result)]
fn merge_folder(
    current: Option<MailboxName>,
    next: Option<MailboxName>,
    op: AccountOperation,
) -> Result<Option<MailboxName>, bifrost_types::AccountError> {
    match (current, next) {
        (Some(a), Some(b)) if a != b => Err(super::error::search_folders_conflict(&a, &b, op)),
        (Some(a), _) => Ok(Some(a)),
        (_, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

fn empty_to_all(criteria: SearchCriteria) -> SearchCriteria {
    if criteria.trim().is_empty() {
        SearchCriteria::new().all()
    } else {
        criteria
    }
}

fn imap_date(time: SystemTime) -> String {
    let datetime = Offset::UTC
        .to_datetime(Timestamp::try_from(time).unwrap_or(Timestamp::UNIX_EPOCH))
        .date();
    let month = match datetime.month() {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        _ => "Dec",
    };
    format!("{}-{month}-{}", datetime.day(), datetime.year())
}

fn search_folders(account: &ImapAccount, restriction: Option<&MailboxName>) -> Vec<MailboxName> {
    if let Some(folder) = restriction {
        return vec![folder.clone()];
    }
    // `entries()` iterates a `HashMap`, so its order is not stable
    // run-to-run. The search page cursor is a position in a walk ordered
    // by folder and then UID, and `SearchPager` resumes by skipping every
    // folder ordered before the cursor's; a nondeterministic folder order
    // would skip folders the walk never visited. Sort by display name and
    // wire identity, the same total order the pager compares by.
    sorted_search_folders(&account.folders)
}

fn sorted_search_folders(registry: &super::folder_registry::FolderRegistry) -> Vec<MailboxName> {
    let mut folders: Vec<MailboxName> = registry
        .entries()
        .into_iter()
        .filter(|entry| entry.selectable())
        .map(|entry| entry.name.clone())
        .collect();
    folders.sort_by(MailboxName::cmp_account_order);
    folders
}

/// Prefix of the search page cursor. The payload after it is the canonical
/// `imap2` object id of the walk position, so the folder rides in the same
/// length-prefixed wire-identity encoding object ids use.
const SEARCH_CURSOR_PREFIX: &str = "imapsearch1:";

/// Where a search walk stands: the folder, its UIDVALIDITY, and the key UID
/// of the last item handed back (the message UID, or a thread's lowest
/// member UID).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchPosition {
    folder: MailboxName,
    uidvalidity: crate::types::UidValidity,
    key: crate::types::Uid,
}

impl SearchPosition {
    fn encode(&self) -> Vec<u8> {
        let id = encode_object_id(&self.folder, self.uidvalidity, self.key);
        format!("{SEARCH_CURSOR_PREFIX}{}", id.0).into_bytes()
    }

    fn decode(bytes: &[u8], op: AccountOperation) -> Result<Self, AccountError> {
        let invalid = || search_page_cursor_error(op, "invalid IMAP page cursor");
        let payload = std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.strip_prefix(SEARCH_CURSOR_PREFIX))
            .filter(|payload| payload.starts_with("imap2:"))
            .ok_or_else(invalid)?;
        let decoded = decode_object_id(&ObjectId(payload.to_owned()), op).map_err(|_| invalid())?;
        Ok(Self {
            folder: decoded.folder.clone(),
            uidvalidity: decoded.uidvalidity,
            key: decoded.uid,
        })
    }
}

// Version 3 retains runtime coverage gaps as well as the query and position.
// Only folder identity and classification travel in the cursor; diagnostics
// remain on the page that observed the failure.
fn cursor_field(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(format!("{}:", value.len()).as_bytes());
    bytes.extend_from_slice(value);
}

fn take_cursor_field<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
    let colon = bytes.iter().position(|byte| *byte == b':')?;
    let len = std::str::from_utf8(&bytes[..colon])
        .ok()?
        .parse::<usize>()
        .ok()?;
    let rest = bytes.get(colon + 1..)?;
    let field = rest.get(..len)?;
    *bytes = rest.get(len..)?;
    Some(field)
}

fn encode_bound_position(
    position: &SearchPosition,
    binding: &[u8],
    skips: &[(MailboxName, u8)],
) -> Vec<u8> {
    let mut bytes = b"imapsearch3:".to_vec();
    cursor_field(&mut bytes, binding);
    cursor_field(&mut bytes, &position.encode());
    for (folder, kind) in skips {
        let mut identity = position.clone();
        identity.folder = folder.clone();
        cursor_field(&mut bytes, &identity.encode());
        bytes.push(*kind);
    }
    bytes
}

fn decode_bound_position(
    bytes: &[u8],
    binding: &[u8],
    op: AccountOperation,
) -> Result<(SearchPosition, Vec<(MailboxName, u8)>), AccountError> {
    let invalid = || search_page_cursor_error(op, "IMAP page cursor does not match this search");
    let mut rest = bytes.strip_prefix(b"imapsearch3:").ok_or_else(invalid)?;
    if take_cursor_field(&mut rest) != Some(binding) {
        return Err(invalid());
    }
    let position = SearchPosition::decode(take_cursor_field(&mut rest).ok_or_else(invalid)?, op)?;
    let mut skips = Vec::new();
    while !rest.is_empty() {
        let folder =
            SearchPosition::decode(take_cursor_field(&mut rest).ok_or_else(invalid)?, op)?.folder;
        let kind = *rest
            .first()
            .filter(|kind| **kind <= 2)
            .ok_or_else(invalid)?;
        rest = &rest[1..];
        if !skips.iter().any(|(known, _)| known == &folder) {
            skips.push((folder, kind));
        }
    }
    Ok((position, skips))
}

fn restored_runtime_skip(folder: &MailboxName, kind: u8, op: AccountOperation) -> SkippedScope {
    use bifrost_types::{
        AccessCause, AccountErrorBuilder, PrimaryCause, RequestCause, ResourceKind, StateCause,
    };
    let scope = super::folder_scope(folder);
    let cause = match kind {
        0 => PrimaryCause::Access(AccessCause::PermissionDenied { resource: None }),
        1 => PrimaryCause::Request(RequestCause::NotFound {
            what: ResourceKind::Mailbox,
            id: Some(folder.account_id()),
        }),
        _ => PrimaryCause::State(StateCause::ScopeRevoked {
            scope: scope.clone(),
        }),
    };
    SkippedScope {
        scope: bifrost_types::ErrorScope::Cursor(scope.clone()),
        error: AccountErrorBuilder::new(cause)
            .operation(op)
            .protocol(Protocol::Imap)
            .scope(bifrost_types::ErrorScope::Cursor(scope))
            .try_build()
            .expect("valid retained search skip"),
    }
}

/// Pages a multi-folder search by position, not by offset.
///
/// IMAP has no server-side result state a page cursor could pin, so every
/// page call re-runs the search. An offset into that fresh result is not
/// stable: expunging an item on an earlier page shifts every later item down
/// one place and the item at the boundary is never returned. The cursor is
/// instead the position of the last item handed back, in a walk ordered by
/// folder (`MailboxName::cmp_account_order`) and then key UID ascending. UIDs
/// within one UIDVALIDITY never move, so "key greater than the cursor's" names
/// the same survivors whatever was expunged meanwhile, and folders ordered
/// before the cursor's are not searched again.
///
/// What a position walk does not promise: an item moved into, or a folder
/// created or renamed into, a position the walk already passed is not
/// returned; a thread whose membership changed between pages can come back
/// under a new id or be passed. A changed UIDVALIDITY on the cursor's folder
/// voids every UID in the cursor. Request folder re-inventory and have the
/// caller repeat the walk against the fresh generation.
struct SearchPager<T> {
    resume: Option<SearchPosition>,
    limit: usize,
    collected: Vec<(SearchPosition, T)>,
    op: AccountOperation,
    binding: Vec<u8>,
    skipped_scopes: Vec<SkippedScope>,
    runtime_skips: Vec<(MailboxName, u8)>,
}

impl<T> SearchPager<T> {
    fn new(
        request: &SearchRequest,
        restriction: Option<&MailboxName>,
        op: AccountOperation,
    ) -> Result<Self, AccountError> {
        // Enterprise users expect changing a query to start a new walk, never
        // to inherit a position from unrelated results. Limit is presentation
        // only; filter, provider query and result surface define the request.
        // Debug here is a private, versioned structural representation, not
        // wire syntax. A representation change safely refuses older cursors.
        let binding =
            format!("{:?}|{:?}|{:?}", request.filter, request.provider_query, op).into_bytes();
        let continuation = request
            .page_cursor
            .as_deref()
            .map(|bytes| decode_bound_position(bytes, &binding, op))
            .transpose()?;
        let (resume, runtime_skips) = continuation.map_or_else(
            || (None, Vec::new()),
            |(position, skips)| (Some(position), skips),
        );
        let skipped_scopes = runtime_skips
            .iter()
            .map(|(folder, kind)| restored_runtime_skip(folder, *kind, op))
            .collect();
        if let (Some(resume), Some(restriction)) = (&resume, restriction)
            && resume.folder.cmp_account_order(restriction) != std::cmp::Ordering::Equal
        {
            return Err(search_page_cursor_error(
                op,
                "IMAP page cursor names a folder outside the search restriction",
            ));
        }
        // A zero-item page has no last item to mint a position from: reusing
        // the incoming cursor would loop, and advancing would skip.
        let limit = request.limit.unwrap_or(500);
        if limit == 0 {
            return Err(pim_malformed(op, "IMAP search page limit must be nonzero"));
        }
        Ok(Self {
            resume,
            limit: usize::try_from(limit).unwrap_or(usize::MAX),
            collected: Vec::new(),
            op,
            binding,
            skipped_scopes,
            runtime_skips,
        })
    }

    // Every page must disclose incomplete mail coverage, even when the gap
    // precedes its cursor. DAV collection gaps do not affect mail search.
    fn include_discovery_skips(
        &mut self,
        skips: &[SkippedScope],
        restriction: Option<&MailboxName>,
    ) {
        use bifrost_types::ErrorScope;
        for skip in skips {
            let relevant = match &skip.scope {
                ErrorScope::Account => true,
                ErrorScope::Namespace { prefix } => {
                    restriction.is_none_or(|folder| folder.as_str().starts_with(prefix))
                }
                ErrorScope::Cursor(bifrost_types::CursorScope::Folder(folder)) => {
                    restriction.is_none_or(|name| name.account_id() == folder.0)
                }
                _ => false,
            };
            if relevant {
                self.record_skip(skip.clone());
            }
        }
    }

    fn record_skip(&mut self, skip: SkippedScope) {
        if !self
            .skipped_scopes
            .iter()
            .any(|known| known.scope == skip.scope)
        {
            self.skipped_scopes.push(skip);
        }
    }

    /// The folders still ahead of the cursor, in walk order. `all` must
    /// already be sorted by `cmp_account_order`.
    fn folders(&self, all: Vec<MailboxName>) -> Vec<MailboxName> {
        match &self.resume {
            None => all,
            Some(resume) => all
                .into_iter()
                .filter(|folder| folder.cmp_account_order(&resume.folder).is_ge())
                .collect(),
        }
    }

    /// One item past the page is known, so the page is settled and later
    /// folders need not be searched.
    fn is_full(&self) -> bool {
        self.collected.len() > self.limit
    }

    /// The UID floor this folder resumes above, or a `ConcurrencyConflict`
    /// when it is the cursor's folder under a different UIDVALIDITY. Call it
    /// straight after SELECT, so a changed epoch is reported as itself rather
    /// than as whatever the following search command did.
    fn check_epoch(
        &self,
        folder: &MailboxName,
        uidvalidity: crate::types::UidValidity,
    ) -> Result<Option<crate::types::Uid>, AccountError> {
        match &self.resume {
            Some(resume) if folder.cmp_account_order(&resume.folder).is_eq() => {
                if resume.uidvalidity != uidvalidity {
                    return Err(search_cursor_voided(self.op, folder));
                }
                Ok(Some(resume.key))
            }
            _ => Ok(None),
        }
    }

    /// Take one folder's results, `(key, item)` in any order.
    fn accept<K>(
        &mut self,
        folder: &MailboxName,
        uidvalidity: crate::types::UidValidity,
        mut entries: Vec<(u32, K)>,
        mint: impl Fn(K) -> T,
    ) -> Result<(), AccountError> {
        let floor = self.check_epoch(folder, uidvalidity)?;
        entries.sort_by_key(|(key, _)| *key);
        entries.dedup_by_key(|(key, _)| *key);
        for (key, entry) in entries {
            if self.is_full() {
                break;
            }
            if floor.is_some_and(|floor| key <= floor.get()) {
                continue;
            }
            let position = SearchPosition {
                folder: folder.clone(),
                uidvalidity,
                key: crate::types::Uid::new(key)
                    .ok_or_else(|| pim_malformed(self.op, "zero search position"))?,
            };
            self.collected.push((position, mint(entry)));
        }
        Ok(())
    }

    fn skip_or_fail(
        &mut self,
        folder: &MailboxName,
        error: AccountError,
    ) -> Result<(), AccountError> {
        use bifrost_types::{AccessErrorKind, AccountErrorKind, ResourceKind, SyncStateErrorKind};
        // A revoked or vanished folder must not hide healthy mail from an
        // enterprise user. Account-wide and ambiguous failures still abort so
        // loss of connectivity never appears as an empty search result.
        if !matches!(
            error.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
                | AccountErrorKind::NotFound(ResourceKind::Mailbox)
                | AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
        ) {
            return Err(error);
        }
        // A later page may resume beyond this folder. Enterprise users reading
        // only that page must still see that the walk omitted some mail.
        let kind = match error.kind() {
            AccountErrorKind::Authorization(_) => 0,
            AccountErrorKind::NotFound(_) => 1,
            _ => 2,
        };
        if !self.runtime_skips.iter().any(|(known, _)| known == folder) {
            self.runtime_skips.push((folder.clone(), kind));
        }
        self.record_skip(SkippedScope {
            scope: bifrost_types::ErrorScope::Cursor(super::folder_scope(folder)),
            error,
        });
        Ok(())
    }

    fn finish(mut self) -> Page<T> {
        let (next_cursor, estimated_total) = if self.collected.len() > self.limit {
            // The lookahead item stays ahead of the cursor for the next call.
            self.collected.truncate(self.limit);
            let last = self.collected.last().map(|(position, _)| {
                encode_bound_position(position, &self.binding, &self.runtime_skips).into()
            });
            (last, None)
        } else if self.resume.is_none() && self.skipped_scopes.is_empty() {
            // This call enumerated the whole result from the start.
            (
                None,
                Some(u64::try_from(self.collected.len()).unwrap_or(u64::MAX)),
            )
        } else {
            (None, None)
        };
        Page {
            truncated: false,
            items: self.collected.into_iter().map(|(_, item)| item).collect(),
            next_cursor,
            estimated_total,
            failed_ids: Vec::new(),
            skipped_scopes: self.skipped_scopes,
        }
    }
}

/// A page cursor this account cannot use (undecodable, or naming a folder
/// outside the search restriction) requires restarting the walk, not
/// retrying the same page. The shared page_cursor field is the signal JMAP
/// uses too, so consumers restart paging the same way across providers.
fn search_page_cursor_error(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    AccountError::invalid_page_cursor(op, Protocol::Imap, detail)
}

fn flatten_thread(node: &ThreadNode, out: &mut Vec<u32>) {
    // Reply depth is not bounded by the wire grammar's parenthesis cap.
    let mut pending = vec![node];
    while let Some(node) = pending.pop() {
        if let Some(uid) = node.id {
            out.push(uid);
        }
        pending.extend(node.children.iter().rev());
    }
}

mod attachment;

#[cfg(test)]
mod tests;
